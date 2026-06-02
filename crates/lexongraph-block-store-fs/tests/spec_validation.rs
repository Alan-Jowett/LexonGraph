// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};

use lexongraph_block::{
    Block, BlockError, BlockHash, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_leaf_block,
    compute_block_hash, serialize_block,
};
use lexongraph_block_store::conformance::{
    BlockStoreConformanceHarness, BlockStoreFactory, run_full_suite,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_block_store_fs::FilesystemBlockStore;
use tempfile::TempDir;

#[test]
fn val_fs_store_001_002_009_constructor_and_publish_path_are_deterministic() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let block = sample_leaf_block("path");
    let serialized = serialize_block(&block).unwrap();
    let canonical_root = temp_dir.path().canonicalize().unwrap();

    let block_id = store.put(&block).unwrap();
    let expected_path = expected_block_path(temp_dir.path(), &block_id);

    assert_eq!(block_id, serialized.hash);
    assert!(canonical_root.is_dir());
    assert!(expected_path.starts_with(&canonical_root));
    assert_eq!(std::fs::read(&expected_path).unwrap(), serialized.bytes);
    assert_eq!(
        shard_filenames(expected_path.parent().unwrap()),
        vec![
            expected_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        ]
    );
}

#[test]
fn val_fs_store_003_missing_blocks_return_ok_none() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();

    assert_eq!(store.get(&BlockHash::from_bytes([0x44; 32])).unwrap(), None);
}

#[test]
fn val_fs_store_004_and_005_get_reports_integrity_and_malformed_content_explicitly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();

    let first = serialize_block(&sample_leaf_block("first")).unwrap();
    let second = serialize_block(&sample_leaf_block("second")).unwrap();
    let mismatched_path = expected_block_path(temp_dir.path(), &second.hash);
    std::fs::create_dir_all(mismatched_path.parent().unwrap()).unwrap();
    std::fs::write(&mismatched_path, &first.bytes).unwrap();

    assert_eq!(
        store.get(&second.hash).unwrap_err(),
        BlockStoreError::IntegrityMismatch {
            expected: second.hash,
            actual: first.hash,
        }
    );

    let malformed_bytes = [0xff, 0xff, 0x00];
    let malformed_hash = compute_block_hash(&malformed_bytes);
    let malformed_path = expected_block_path(temp_dir.path(), &malformed_hash);
    std::fs::create_dir_all(malformed_path.parent().unwrap()).unwrap();
    std::fs::write(&malformed_path, malformed_bytes).unwrap();

    assert!(matches!(
        store.get(&malformed_hash).unwrap_err(),
        BlockStoreError::MalformedContent(BlockError::MalformedCbor(_))
    ));
}

#[test]
fn val_fs_store_006_conflicting_existing_bytes_fail_without_overwrite() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let block = sample_leaf_block("conflict");
    let serialized = serialize_block(&block).unwrap();
    let published_path = expected_block_path(temp_dir.path(), &serialized.hash);
    let conflicting_bytes = b"not canonical bytes".to_vec();

    std::fs::create_dir_all(published_path.parent().unwrap()).unwrap();
    std::fs::write(&published_path, &conflicting_bytes).unwrap();

    let error = store.put(&block).unwrap_err();

    assert!(matches!(error, BlockStoreError::BackendFailure(_)));
    assert_eq!(std::fs::read(&published_path).unwrap(), conflicting_bytes);
}

#[test]
fn val_fs_store_007_publish_only_exposes_complete_target_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let body = "atomic-visibility-".repeat(512 * 8);
    let block = sample_leaf_block(&body);
    let serialized = Arc::new(serialize_block(&block).unwrap());
    let published_path = expected_block_path(temp_dir.path(), &serialized.hash);
    let stop = Arc::new(AtomicBool::new(false));
    let saw_partial = Arc::new(AtomicBool::new(false));

    let watcher = {
        let published_path = published_path.clone();
        let serialized = Arc::clone(&serialized);
        let stop = Arc::clone(&stop);
        let saw_partial = Arc::clone(&saw_partial);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match std::fs::read(&published_path) {
                    Ok(bytes) if bytes != serialized.bytes => {
                        saw_partial.store(true, Ordering::Release);
                        break;
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => {
                        saw_partial.store(true, Ordering::Release);
                        break;
                    }
                }
                std::thread::yield_now();
            }
        })
    };

    let put_result = store.put(&block);
    stop.store(true, Ordering::Release);
    watcher.join().unwrap();

    assert_eq!(put_result.unwrap(), serialized.hash);
    assert!(!saw_partial.load(Ordering::Acquire));
    assert_eq!(
        std::fs::read(&published_path).unwrap(),
        serialized.bytes.as_slice()
    );
}

#[test]
fn val_fs_store_008_concurrent_same_block_publishers_converge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let block = Arc::new(sample_leaf_block("shared"));
    let expected_hash = serialize_block(block.as_ref()).unwrap().hash;
    let barrier = Arc::new(Barrier::new(6));
    let mut threads = Vec::new();

    for _ in 0..6 {
        let store = store.clone();
        let block = Arc::clone(&block);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            store.put(block.as_ref())
        }));
    }

    for result in threads {
        assert_eq!(result.join().unwrap().unwrap(), expected_hash);
    }

    let loaded = store.get(&expected_hash).unwrap().unwrap();
    assert_eq!(loaded.hash, expected_hash);
    assert_eq!(loaded.block, *block);
}

#[test]
fn val_fs_store_010_parent_conformance_requirements_are_realized_by_tests() {
    run_full_suite(&FilesystemHarness::default()).unwrap();
}

#[test]
fn val_fs_store_011_repository_includes_filesystem_store_verification_artifacts() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));

    assert!(
        manifest_dir
            .join("tests")
            .join("spec_validation.rs")
            .is_file()
    );
    assert!(
        manifest_dir
            .join("tests")
            .join("conformance_feature.rs")
            .is_file()
    );
}

fn sample_leaf_block(body: &str) -> Block {
    Block::Leaf(
        build_leaf_block(
            VERSION_1,
            EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            vec![LeafEntry {
                embedding: vec![0xaa, 0xbb],
                metadata: vec![],
                content: Content {
                    media_type: "text/plain".into(),
                    body: body.as_bytes().to_vec(),
                },
            }],
            None,
        )
        .unwrap(),
    )
}

fn expected_block_path(root: &Path, block_id: &BlockHash) -> PathBuf {
    let canonical_root = root.canonicalize().unwrap();
    let hex = block_id.to_string();
    canonical_root
        .join(&hex[..2])
        .join(&hex[2..4])
        .join(format!("{hex}.cbor"))
}

fn shard_filenames(path: &Path) -> Vec<String> {
    let mut names = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[derive(Default)]
struct FilesystemHarness {
    roots: Mutex<Vec<TempDir>>,
}

#[derive(Clone, Debug)]
struct HarnessStore {
    inner: FilesystemBlockStore,
    root: PathBuf,
}

impl BlockStore for HarnessStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        self.inner.put(block)
    }

    fn get(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        self.inner.get(block_id)
    }
}

impl BlockStoreFactory for FilesystemHarness {
    type Store = HarnessStore;

    fn fresh_store(&self) -> Self::Store {
        let root = tempfile::tempdir().unwrap();
        let store = HarnessStore {
            inner: FilesystemBlockStore::new(root.path()).unwrap(),
            root: root.path().canonicalize().unwrap(),
        };
        self.roots.lock().unwrap().push(root);
        store
    }
}

impl BlockStoreConformanceHarness for FilesystemHarness {
    fn inject_raw_bytes(
        &self,
        store: &Self::Store,
        block_id: &BlockHash,
        bytes: &[u8],
    ) -> Result<(), String> {
        let path = expected_block_path(&store.root, block_id);
        std::fs::create_dir_all(path.parent().unwrap()).map_err(|error| error.to_string())?;
        std::fs::write(path, bytes).map_err(|error| error.to_string())
    }
}
