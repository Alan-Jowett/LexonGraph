// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::collections::HashSet;
use std::fs::File;
use std::future::Future;
#[cfg(feature = "inject")]
use std::io;
#[cfg(feature = "inject")]
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::SystemTime;

use async_trait::async_trait;
use futures::TryStreamExt;
use lexongraph_block::{
    Block, BlockError, BlockHash, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_leaf_block,
    compute_block_hash, serialize_block,
};
use lexongraph_block_store::conformance::{
    BlockStoreConformanceHarness, BlockStoreFactory, run_full_suite,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
#[cfg(feature = "inject")]
use lexongraph_block_store_fs::StoredFileMetadata;
#[cfg(feature = "inject")]
use lexongraph_block_store_fs::inject::{FsOps, StagedFile};
use lexongraph_block_store_fs::{FilesystemBlockStore, FilesystemBlockStoreBuildError};
#[cfg(feature = "inject")]
use tempfile::NamedTempFile;
use tempfile::TempDir;

trait BlockingResultFutureExt<T, E>: Future<Output = Result<T, E>> + Sized {
    fn unwrap(self) -> T
    where
        E: std::fmt::Debug,
    {
        pollster::block_on(self).unwrap()
    }

    fn unwrap_err(self) -> E
    where
        T: std::fmt::Debug,
        E: std::fmt::Debug,
    {
        pollster::block_on(self).unwrap_err()
    }
}

impl<F, T, E> BlockingResultFutureExt<T, E> for F where F: Future<Output = Result<T, E>> {}

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
        BlockStoreError::DecodeFailure(BlockError::HashMismatch {
            expected: second.hash,
            actual: first.hash,
        })
    );

    let malformed_bytes = [0xff, 0xff, 0x00];
    let malformed_hash = compute_block_hash(&malformed_bytes);
    let malformed_path = expected_block_path(temp_dir.path(), &malformed_hash);
    std::fs::create_dir_all(malformed_path.parent().unwrap()).unwrap();
    std::fs::write(&malformed_path, malformed_bytes).unwrap();

    assert!(matches!(
        store.get(&malformed_hash).unwrap_err(),
        BlockStoreError::DecodeFailure(BlockError::MalformedCbor(_))
    ));
}

#[test]
fn val_fs_store_006_and_016_conflicting_existing_bytes_fail_without_overwrite() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let block = sample_leaf_block("conflict");
    let serialized = serialize_block(&block).unwrap();
    let published_path = expected_block_path(temp_dir.path(), &serialized.hash);
    let conflicting_bytes = b"not canonical bytes".to_vec();

    std::fs::create_dir_all(published_path.parent().unwrap()).unwrap();
    std::fs::write(&published_path, &conflicting_bytes).unwrap();

    let error = store.put(&block).unwrap_err();

    expect_backend_failure_contains(error, "integrity conflict");
    assert_eq!(std::fs::read(&published_path).unwrap(), conflicting_bytes);
}

#[test]
fn val_fs_store_015_publish_failure_with_matching_bytes_reports_success() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let block = sample_leaf_block("matching-publish-recovery");
    let serialized = serialize_block(&block).unwrap();
    let published_path = expected_block_path(temp_dir.path(), &serialized.hash);

    std::fs::create_dir_all(published_path.parent().unwrap()).unwrap();
    std::fs::write(&published_path, &serialized.bytes).unwrap();

    assert_eq!(store.put(&block).unwrap(), serialized.hash);
    assert_eq!(std::fs::read(&published_path).unwrap(), serialized.bytes);
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
            pollster::block_on(store.put(block.as_ref()))
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
    pollster::block_on(run_full_suite(&FilesystemHarness::default())).unwrap();
}

#[test]
fn val_fs_store_019_enumeration_yields_published_block_ids() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let first = sample_leaf_block("first");
    let second = sample_leaf_block("second");

    let expected = HashSet::from([store.put(&first).unwrap(), store.put(&second).unwrap()]);
    let enumerated = collect_block_ids(store.iter_block_ids().unwrap()).unwrap();

    assert_eq!(enumerated, expected);
}

#[test]
fn val_fs_store_020_enumeration_excludes_staging_and_other_non_published_artifacts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let block = sample_leaf_block("published");
    let block_id = store.put(&block).unwrap();
    let published_path = expected_block_path(temp_dir.path(), &block_id);
    let published_dir = published_path.parent().unwrap();

    std::fs::write(temp_dir.path().join("root-note.txt"), b"ignore me").unwrap();
    std::fs::write(published_dir.join(".tmp-junk.part"), b"transient").unwrap();
    std::fs::create_dir_all(published_dir.join("nested-dir")).unwrap();

    let enumerated = collect_block_ids(store.iter_block_ids().unwrap()).unwrap();

    assert_eq!(enumerated, HashSet::from([block_id]));
}

#[test]
fn val_fs_store_021_path_decoding_failures_are_explicit_during_enumeration() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let malformed_path = temp_dir
        .path()
        .join("aa")
        .join("bb")
        .join("not-a-block-id.cbor");

    std::fs::create_dir_all(malformed_path.parent().unwrap()).unwrap();
    std::fs::write(&malformed_path, b"malformed candidate").unwrap();

    let error = store.iter_block_ids().unwrap().try_collect::<Vec<_>>();
    let error = pollster::block_on(error).unwrap_err();

    expect_backend_failure_contains(error, "failed to decode an enumerated block ID candidate");
}

#[test]
fn val_fs_store_022_cache_mode_constructor_enforces_positive_mb_capacity() {
    let temp_dir = tempfile::tempdir().unwrap();

    assert_eq!(
        FilesystemBlockStore::new_cache_mb(temp_dir.path(), 0).unwrap_err(),
        FilesystemBlockStoreBuildError::ZeroCacheCapacity
    );

    FilesystemBlockStore::new_cache_mb(temp_dir.path(), 1).unwrap();
}

#[test]
fn val_fs_store_023_cache_mode_eviction_uses_payload_bytes_and_lru_recency() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new_cache_mb(temp_dir.path(), 1).unwrap();
    let first = sample_leaf_block(&"a".repeat(700_000));
    let second = sample_leaf_block(&"b".repeat(300_000));
    let third = sample_leaf_block(&"c".repeat(300_000));

    let first_id = store.put(&first).unwrap();
    let second_id = store.put(&second).unwrap();

    assert_eq!(store.get(&first_id).unwrap().unwrap().hash, first_id);
    let third_id = store.put(&third).unwrap();

    assert_eq!(
        collect_block_ids(store.iter_block_ids().unwrap()).unwrap(),
        HashSet::from([first_id, third_id]),
    );
    assert_eq!(store.get(&second_id).unwrap(), None);
    assert!(!expected_block_path(temp_dir.path(), &second_id).exists());
}

#[test]
fn val_fs_store_024_cache_mode_rejects_blocks_larger_than_total_budget_without_publish() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new_cache_mb(temp_dir.path(), 1).unwrap();
    let oversized = sample_leaf_block(&"x".repeat(1_100_000));
    let oversized_hash = serialize_block(&oversized).unwrap().hash;
    let oversized_path = expected_block_path(temp_dir.path(), &oversized_hash);

    let error = store.put(&oversized).unwrap_err();

    expect_backend_failure_contains(error, "exceeds cache capacity");
    assert!(!oversized_path.exists());
}

#[test]
fn val_fs_store_025_cache_mode_constructor_evicts_oldest_existing_blocks_until_budget_fits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let priming_store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let first = sample_leaf_block(&"a".repeat(700_000));
    let second = sample_leaf_block(&"b".repeat(700_000));

    let first_id = priming_store.put(&first).unwrap();
    let second_id = priming_store.put(&second).unwrap();
    set_last_modified(
        &expected_block_path(temp_dir.path(), &first_id),
        SystemTime::UNIX_EPOCH,
    );
    set_last_modified(
        &expected_block_path(temp_dir.path(), &second_id),
        SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10),
    );

    let bounded_store = FilesystemBlockStore::new_cache_mb(temp_dir.path(), 1).unwrap();

    assert_eq!(
        collect_block_ids(bounded_store.iter_block_ids().unwrap()).unwrap(),
        HashSet::from([second_id]),
    );
    assert!(!expected_block_path(temp_dir.path(), &first_id).exists());
    assert!(expected_block_path(temp_dir.path(), &second_id).exists());
}

#[cfg(feature = "inject")]
#[test]
fn val_fs_store_026_cache_mode_constructor_surfaces_injected_metadata_failures() {
    let temp_dir = tempfile::tempdir().unwrap();
    let priming_store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let block = sample_leaf_block("metadata-failure");

    priming_store.put(&block).unwrap();

    let error = FilesystemBlockStore::new_cache_mb_with_ops(
        temp_dir.path(),
        Arc::new(ScriptedFsOps::with_metadata_failure(
            1,
            error_spec("metadata failure"),
        )),
        1,
    )
    .unwrap_err();

    match error {
        FilesystemBlockStoreBuildError::CacheInitialization(error) => {
            expect_backend_failure_contains(error, "failed to inspect existing cached block");
        }
        other => panic!("expected cache initialization failure, got {other:?}"),
    }
}

#[cfg(feature = "inject")]
#[test]
fn val_fs_store_027_cache_mode_constructor_surfaces_injected_eviction_failures() {
    let temp_dir = tempfile::tempdir().unwrap();
    let priming_store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let first = sample_leaf_block(&"a".repeat(700_000));
    let second = sample_leaf_block(&"b".repeat(700_000));

    let first_id = priming_store.put(&first).unwrap();
    let second_id = priming_store.put(&second).unwrap();
    set_last_modified(
        &expected_block_path(temp_dir.path(), &first_id),
        SystemTime::UNIX_EPOCH,
    );
    set_last_modified(
        &expected_block_path(temp_dir.path(), &second_id),
        SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10),
    );

    let error = FilesystemBlockStore::new_cache_mb_with_ops(
        temp_dir.path(),
        Arc::new(ScriptedFsOps::with_remove_file_failure(
            1,
            error_spec("remove failure"),
        )),
        1,
    )
    .unwrap_err();

    match error {
        FilesystemBlockStoreBuildError::CacheInitialization(error) => {
            expect_backend_failure_contains(error, "failed to evict cached block");
        }
        other => panic!("expected cache initialization failure, got {other:?}"),
    }
    assert!(expected_block_path(temp_dir.path(), &first_id).exists());
    assert!(expected_block_path(temp_dir.path(), &second_id).exists());
}

#[cfg(feature = "inject")]
#[test]
fn val_fs_store_021_directory_traversal_failures_are_explicit_during_enumeration() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new_with_ops(
        temp_dir.path(),
        Arc::new(ScriptedFsOps::with_read_dir_failure(
            1,
            error_spec("enumerate root"),
        )),
    )
    .unwrap();

    let error = match store.iter_block_ids() {
        Ok(_) => panic!("expected root enumeration to fail explicitly"),
        Err(error) => error,
    };

    expect_backend_failure_contains(error, "failed to enumerate the block store root");
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

#[cfg(feature = "inject")]
#[test]
fn val_fs_store_012_constructor_failures_are_explicit_backend_failures() {
    let temp_dir = tempfile::tempdir().unwrap();

    let create_error = FilesystemBlockStore::new_with_ops(
        temp_dir.path().join("create"),
        Arc::new(ScriptedFsOps::with_create_dir_all_failure(
            1,
            error_spec("create root"),
        )),
    )
    .unwrap_err();
    expect_backend_failure_contains(create_error, "failed to create store root");

    let canonicalize_error = FilesystemBlockStore::new_with_ops(
        temp_dir.path().join("canonicalize"),
        Arc::new(ScriptedFsOps::with_canonicalize_failure(error_spec(
            "canonicalize root",
        ))),
    )
    .unwrap_err();
    expect_backend_failure_contains(canonicalize_error, "failed to canonicalize store root");

    let stat_error = FilesystemBlockStore::new_with_ops(
        temp_dir.path().join("stat"),
        Arc::new(ScriptedFsOps::with_is_dir_result(IsDirResult::Error(
            error_spec("stat root"),
        ))),
    )
    .unwrap_err();
    expect_backend_failure_contains(stat_error, "failed to stat store root");

    let non_directory_error = FilesystemBlockStore::new_with_ops(
        temp_dir.path().join("not-a-directory"),
        Arc::new(ScriptedFsOps::with_is_dir_result(IsDirResult::Value(false))),
    )
    .unwrap_err();
    expect_backend_failure_contains(non_directory_error, "is not a directory");
}

#[cfg(feature = "inject")]
#[test]
fn val_fs_store_013_present_but_unreadable_files_during_get_fail_explicitly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let ops = ScriptedFsOps::with_read_failure(1, error_spec("get unreadable"));
    let store = FilesystemBlockStore::new_with_ops(temp_dir.path(), Arc::new(ops)).unwrap();
    let serialized = serialize_block(&sample_leaf_block("get-unreadable")).unwrap();
    let published_path = expected_block_path(temp_dir.path(), &serialized.hash);

    std::fs::create_dir_all(published_path.parent().unwrap()).unwrap();
    std::fs::write(&published_path, &serialized.bytes).unwrap();

    let error = store.get(&serialized.hash).unwrap_err();
    expect_backend_failure_contains(error, "failed to read block");
}

#[cfg(feature = "inject")]
#[test]
fn val_fs_store_014_pre_publication_failures_leave_no_published_target() {
    assert_put_pre_publication_failure(
        ScriptedFsOps::with_create_dir_all_failure(2, error_spec("create shard dir")),
        "failed to create block directory",
    );
    assert_put_pre_publication_failure(
        ScriptedFsOps::with_create_staged_file_failure(error_spec("create staging")),
        "failed to create staging file",
    );
    assert_put_pre_publication_failure(
        ScriptedFsOps::with_write_failure(error_spec("write staged bytes")),
        "failed to stage block",
    );
    assert_put_pre_publication_failure(
        ScriptedFsOps::with_flush_failure(error_spec("flush staged bytes")),
        "failed to flush staged block",
    );
}

#[cfg(feature = "inject")]
#[test]
fn val_fs_store_017_publish_failure_followed_by_missing_target_is_backend_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let block = sample_leaf_block("publish-missing");
    let serialized = serialize_block(&block).unwrap();
    let published_path = expected_block_path(temp_dir.path(), &serialized.hash);
    let ops = ScriptedFsOps::with_persist_failure(error_spec("persist missing target"));
    let store = FilesystemBlockStore::new_with_ops(temp_dir.path(), Arc::new(ops)).unwrap();

    let error = store.put(&block).unwrap_err();

    expect_backend_failure_contains(error, "failed to publish block");
    assert!(!published_path.exists());
}

#[cfg(feature = "inject")]
#[test]
fn val_fs_store_018_publish_failure_followed_by_uninspectable_target_is_backend_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let block = sample_leaf_block("publish-uninspectable");
    let serialized = serialize_block(&block).unwrap();
    let published_path = expected_block_path(temp_dir.path(), &serialized.hash);
    let ops = ScriptedFsOps::with_persist_and_read_failures(
        error_spec("persist unreadable target"),
        1,
        error_spec("inspect unreadable target"),
    );
    let store = FilesystemBlockStore::new_with_ops(temp_dir.path(), Arc::new(ops)).unwrap();

    std::fs::create_dir_all(published_path.parent().unwrap()).unwrap();
    std::fs::write(&published_path, &serialized.bytes).unwrap();

    let error = store.put(&block).unwrap_err();

    expect_backend_failure_contains(error, "failed to inspect published block");
    assert_eq!(std::fs::read(&published_path).unwrap(), serialized.bytes);
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

fn collect_block_ids(
    iter: lexongraph_block_store::BlockIdStream<'_>,
) -> Result<HashSet<BlockHash>, BlockStoreError> {
    pollster::block_on(iter.try_collect::<HashSet<_>>())
}

fn set_last_modified(path: &Path, modified: SystemTime) {
    let file = File::options().write(true).open(path).unwrap();
    file.set_modified(modified).unwrap();
}

#[cfg(feature = "inject")]
fn assert_put_pre_publication_failure(ops: ScriptedFsOps, expected_message: &str) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new_with_ops(temp_dir.path(), Arc::new(ops)).unwrap();
    let block = sample_leaf_block(expected_message);
    let serialized = serialize_block(&block).unwrap();
    let published_path = expected_block_path(temp_dir.path(), &serialized.hash);

    let error = store.put(&block).unwrap_err();

    expect_backend_failure_contains(error, expected_message);
    assert!(!published_path.exists());
}

fn expect_backend_failure_contains(error: BlockStoreError, expected_fragment: &str) {
    match error {
        BlockStoreError::BackendFailure(message) => {
            assert!(
                message.contains(expected_fragment),
                "expected backend failure containing {expected_fragment:?}, got {message:?}"
            );
        }
        other => panic!("expected backend failure, got {other:?}"),
    }
}

#[cfg(feature = "inject")]
fn error_spec(message: &'static str) -> ErrorSpec {
    ErrorSpec {
        kind: io::ErrorKind::PermissionDenied,
        message,
    }
}

#[cfg(feature = "inject")]
#[derive(Clone)]
struct ErrorSpec {
    kind: io::ErrorKind,
    message: &'static str,
}

#[cfg(feature = "inject")]
impl ErrorSpec {
    fn to_io_error(&self) -> io::Error {
        io::Error::new(self.kind, self.message)
    }
}

#[cfg(feature = "inject")]
enum IsDirResult {
    Value(bool),
    Error(ErrorSpec),
}

#[cfg(feature = "inject")]
#[derive(Clone, Default)]
struct ScriptedFsOps {
    state: Arc<Mutex<ScriptState>>,
}

#[cfg(feature = "inject")]
#[derive(Default)]
struct ScriptState {
    create_dir_all_calls: usize,
    create_dir_all_failure: Option<IndexedFailure>,
    canonicalize_failure: Option<ErrorSpec>,
    is_dir_result: Option<IsDirResult>,
    read_dir_calls: usize,
    read_dir_failure: Option<IndexedFailure>,
    metadata_calls: usize,
    metadata_failure: Option<IndexedFailure>,
    read_calls: usize,
    read_failure: Option<IndexedFailure>,
    remove_file_calls: usize,
    remove_file_failure: Option<IndexedFailure>,
    create_staged_file_failure: Option<ErrorSpec>,
    write_failure: Option<ErrorSpec>,
    flush_failure: Option<ErrorSpec>,
    persist_failure: Option<ErrorSpec>,
}

#[cfg(feature = "inject")]
struct IndexedFailure {
    call: usize,
    error: ErrorSpec,
}

#[cfg(feature = "inject")]
impl ScriptedFsOps {
    fn with_create_dir_all_failure(call: usize, error: ErrorSpec) -> Self {
        let ops = Self::default();
        ops.state.lock().unwrap().create_dir_all_failure = Some(IndexedFailure { call, error });
        ops
    }

    fn with_canonicalize_failure(error: ErrorSpec) -> Self {
        let ops = Self::default();
        ops.state.lock().unwrap().canonicalize_failure = Some(error);
        ops
    }

    fn with_is_dir_result(result: IsDirResult) -> Self {
        let ops = Self::default();
        ops.state.lock().unwrap().is_dir_result = Some(result);
        ops
    }

    fn with_read_failure(call: usize, error: ErrorSpec) -> Self {
        let ops = Self::default();
        ops.state.lock().unwrap().read_failure = Some(IndexedFailure { call, error });
        ops
    }

    fn with_read_dir_failure(call: usize, error: ErrorSpec) -> Self {
        let ops = Self::default();
        ops.state.lock().unwrap().read_dir_failure = Some(IndexedFailure { call, error });
        ops
    }

    fn with_metadata_failure(call: usize, error: ErrorSpec) -> Self {
        let ops = Self::default();
        ops.state.lock().unwrap().metadata_failure = Some(IndexedFailure { call, error });
        ops
    }

    fn with_create_staged_file_failure(error: ErrorSpec) -> Self {
        let ops = Self::default();
        ops.state.lock().unwrap().create_staged_file_failure = Some(error);
        ops
    }

    fn with_write_failure(error: ErrorSpec) -> Self {
        let ops = Self::default();
        ops.state.lock().unwrap().write_failure = Some(error);
        ops
    }

    fn with_flush_failure(error: ErrorSpec) -> Self {
        let ops = Self::default();
        ops.state.lock().unwrap().flush_failure = Some(error);
        ops
    }

    fn with_persist_failure(error: ErrorSpec) -> Self {
        let ops = Self::default();
        ops.state.lock().unwrap().persist_failure = Some(error);
        ops
    }

    fn with_remove_file_failure(call: usize, error: ErrorSpec) -> Self {
        let ops = Self::default();
        ops.state.lock().unwrap().remove_file_failure = Some(IndexedFailure { call, error });
        ops
    }

    fn with_persist_and_read_failures(
        persist_error: ErrorSpec,
        read_call: usize,
        read_error: ErrorSpec,
    ) -> Self {
        let ops = Self::with_persist_failure(persist_error);
        ops.state.lock().unwrap().read_failure = Some(IndexedFailure {
            call: read_call,
            error: read_error,
        });
        ops
    }
}

#[cfg(feature = "inject")]
impl FsOps for ScriptedFsOps {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.create_dir_all_calls += 1;
        if let Some(failure) = state.create_dir_all_failure.as_ref()
            && failure.call == state.create_dir_all_calls
        {
            return Err(failure.error.to_io_error());
        }
        drop(state);
        std::fs::create_dir_all(path)
    }

    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        let state = self.state.lock().unwrap();
        if let Some(error) = state.canonicalize_failure.as_ref() {
            return Err(error.to_io_error());
        }
        drop(state);
        path.canonicalize()
    }

    fn is_dir(&self, path: &Path) -> io::Result<bool> {
        let state = self.state.lock().unwrap();
        match state.is_dir_result.as_ref() {
            Some(IsDirResult::Value(value)) => Ok(*value),
            Some(IsDirResult::Error(error)) => Err(error.to_io_error()),
            None => {
                drop(state);
                std::fs::symlink_metadata(path).map(|metadata| metadata.is_dir())
            }
        }
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut state = self.state.lock().unwrap();
        state.read_dir_calls += 1;
        if let Some(failure) = state.read_dir_failure.as_ref()
            && failure.call == state.read_dir_calls
        {
            return Err(failure.error.to_io_error());
        }
        drop(state);
        std::fs::read_dir(path)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect()
    }

    fn metadata(&self, path: &Path) -> io::Result<StoredFileMetadata> {
        let mut state = self.state.lock().unwrap();
        state.metadata_calls += 1;
        if let Some(failure) = state.metadata_failure.as_ref()
            && failure.call == state.metadata_calls
        {
            return Err(failure.error.to_io_error());
        }
        drop(state);
        let metadata = std::fs::metadata(path)?;
        Ok(StoredFileMetadata {
            len: metadata.len(),
            modified: metadata.modified()?,
        })
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let mut state = self.state.lock().unwrap();
        state.read_calls += 1;
        if let Some(failure) = state.read_failure.as_ref()
            && failure.call == state.read_calls
        {
            return Err(failure.error.to_io_error());
        }
        drop(state);
        std::fs::read(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.remove_file_calls += 1;
        if let Some(failure) = state.remove_file_failure.as_ref()
            && failure.call == state.remove_file_calls
        {
            return Err(failure.error.to_io_error());
        }
        drop(state);
        std::fs::remove_file(path)
    }

    fn create_staged_file(&self, dir: &Path) -> io::Result<Box<dyn StagedFile>> {
        let mut state = self.state.lock().unwrap();
        if let Some(error) = state.create_staged_file_failure.take() {
            return Err(error.to_io_error());
        }
        drop(state);
        Ok(Box::new(ScriptedStagedFile {
            file: tempfile::Builder::new()
                .prefix(".tmp-")
                .suffix(".part")
                .tempfile_in(dir)?,
            state: Arc::clone(&self.state),
        }))
    }
}

#[cfg(feature = "inject")]
struct ScriptedStagedFile {
    file: NamedTempFile,
    state: Arc<Mutex<ScriptState>>,
}

#[cfg(feature = "inject")]
impl StagedFile for ScriptedStagedFile {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(error) = state.write_failure.take() {
            return Err(error.to_io_error());
        }
        drop(state);
        self.file.write_all(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(error) = state.flush_failure.take() {
            return Err(error.to_io_error());
        }
        drop(state);
        self.file.flush()
    }

    fn persist_noclobber(self: Box<Self>, target: &Path) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(error) = state.persist_failure.take() {
            return Err(error.to_io_error());
        }
        drop(state);
        self.file
            .persist_noclobber(target)
            .map(|_| ())
            .map_err(|error| error.error)
    }
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

#[async_trait]
impl BlockStore for HarnessStore {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        self.inner.put_block_bytes(block_id, block_bytes).await
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        self.inner.get_block_bytes(block_id).await
    }

    fn iter_block_ids(&self) -> Result<lexongraph_block_store::BlockIdStream<'_>, BlockStoreError> {
        self.inner.iter_block_ids()
    }
}

#[async_trait(?Send)]
impl BlockStoreFactory for FilesystemHarness {
    type Store = HarnessStore;

    async fn fresh_store(&self) -> Self::Store {
        let root = tempfile::tempdir().unwrap();
        let store = HarnessStore {
            inner: FilesystemBlockStore::new(root.path()).unwrap(),
            root: root.path().canonicalize().unwrap(),
        };
        self.roots.lock().unwrap().push(root);
        store
    }
}

#[async_trait(?Send)]
impl BlockStoreConformanceHarness for FilesystemHarness {
    async fn inject_raw_bytes(
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
