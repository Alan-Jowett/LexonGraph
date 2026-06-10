// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use lexongraph_block::{
    Block, BlockError, BlockHash, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_leaf_block,
    compute_block_hash, serialize_block,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_block_store_zip::ZipBlockStore;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

#[test]
fn val_zip_store_001_constructor_accepts_an_accessible_zip_archive() {
    let temp_dir = tempfile::tempdir().unwrap();
    let archive_path = temp_dir.path().join("blocks.zip");
    write_zip_archive(&archive_path, &[]);

    let store = ZipBlockStore::new(&archive_path).unwrap();

    assert_eq!(store.iter_block_ids().unwrap().count(), 0);
}

#[test]
fn val_zip_store_002_constructor_rejects_missing_non_file_and_invalid_zip_inputs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let missing = temp_dir.path().join("missing.zip");
    expect_backend_failure_contains(
        ZipBlockStore::new(&missing).unwrap_err(),
        "failed to canonicalize zip archive",
    );

    expect_backend_failure_contains(
        ZipBlockStore::new(temp_dir.path()).unwrap_err(),
        "is not a file",
    );

    let invalid = temp_dir.path().join("invalid.zip");
    std::fs::write(&invalid, b"not a zip archive").unwrap();
    expect_backend_failure_contains(
        ZipBlockStore::new(&invalid).unwrap_err(),
        "failed to read zip archive",
    );
}

#[test]
fn val_zip_store_003_and_004_get_supports_round_trip_and_absence() {
    let temp_dir = tempfile::tempdir().unwrap();
    let block = sample_leaf_block("zip-round-trip");
    let serialized = serialize_block(&block).unwrap();
    let archive_path = temp_dir.path().join("blocks.zip");
    write_zip_archive(
        &archive_path,
        &[(
            expected_entry_name(&serialized.hash),
            serialized.bytes.clone(),
        )],
    );

    let store = ZipBlockStore::new(&archive_path).unwrap();
    let loaded = store.get(&serialized.hash).unwrap().unwrap();

    assert_eq!(loaded.hash, serialized.hash);
    assert_eq!(loaded.block, block);
    assert_eq!(store.get(&BlockHash::from_bytes([0x77; 32])).unwrap(), None);
}

#[test]
fn val_zip_store_005_and_006_get_reports_malformed_and_integrity_failures() {
    let temp_dir = tempfile::tempdir().unwrap();
    let archive_path = temp_dir.path().join("blocks.zip");
    let first = serialize_block(&sample_leaf_block("first")).unwrap();
    let second = serialize_block(&sample_leaf_block("second")).unwrap();
    let malformed_bytes = vec![0xff, 0xff, 0x00];
    let malformed_hash = compute_block_hash(&malformed_bytes);

    write_zip_archive(
        &archive_path,
        &[
            (expected_entry_name(&second.hash), first.bytes.clone()),
            (expected_entry_name(&malformed_hash), malformed_bytes),
        ],
    );

    let store = ZipBlockStore::new(&archive_path).unwrap();

    assert_eq!(
        store.get(&second.hash).unwrap_err(),
        BlockStoreError::IntegrityMismatch {
            expected: second.hash,
            actual: first.hash,
        }
    );
    assert!(matches!(
        store.get(&malformed_hash).unwrap_err(),
        BlockStoreError::MalformedContent(BlockError::MalformedCbor(_))
    ));
}

#[test]
fn val_zip_store_007_duplicate_recognized_entries_fail_explicitly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let archive_path = temp_dir.path().join("blocks.zip");
    let block = serialize_block(&sample_leaf_block("duplicate")).unwrap();
    let entry_name = expected_entry_name(&block.hash);

    write_zip_archive_with_duplicate_entry(&archive_path, &entry_name, &block.bytes);

    let store = ZipBlockStore::new(&archive_path).unwrap();

    expect_backend_failure_contains(
        store.get(&block.hash).unwrap_err(),
        "duplicate recognized block entry",
    );
    expect_backend_failure_contains(
        match store.iter_block_ids() {
            Ok(_) => panic!("expected duplicate recognized entries to fail enumeration"),
            Err(error) => error,
        },
        "duplicate recognized block entry",
    );
}

#[test]
fn val_zip_store_008_and_009_unrelated_entries_are_ignored_and_enumeration_yields_only_blocks() {
    let temp_dir = tempfile::tempdir().unwrap();
    let archive_path = temp_dir.path().join("blocks.zip");
    let first = serialize_block(&sample_leaf_block("first")).unwrap();
    let second = serialize_block(&sample_leaf_block("second")).unwrap();

    write_zip_archive(
        &archive_path,
        &[
            (expected_entry_name(&first.hash), first.bytes),
            (expected_entry_name(&second.hash), second.bytes),
            ("notes/readme.txt".into(), b"ignore me".to_vec()),
            (
                "aa/bb/not-a-block-id.cbor".into(),
                b"also ignore me".to_vec(),
            ),
            ("aa/bad-level.txt".into(), b"ignore me too".to_vec()),
        ],
    );

    let store = ZipBlockStore::new(&archive_path).unwrap();
    let enumerated = collect_block_ids(store.iter_block_ids().unwrap()).unwrap();

    assert_eq!(enumerated, HashSet::from([first.hash, second.hash]));
}

#[test]
fn val_zip_store_011_put_fails_explicitly_without_mutating_the_archive() {
    let temp_dir = tempfile::tempdir().unwrap();
    let archive_path = temp_dir.path().join("blocks.zip");
    let block = sample_leaf_block("read-only");
    write_zip_archive(&archive_path, &[]);
    let before = std::fs::read(&archive_path).unwrap();

    let store = ZipBlockStore::new(&archive_path).unwrap();
    let error = store.put(&block).unwrap_err();
    let after = std::fs::read(&archive_path).unwrap();

    expect_backend_failure_contains(error, "read-only");
    assert_eq!(before, after);
}

#[test]
fn val_zip_store_010_repository_includes_zip_store_verification_artifacts() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));

    assert!(
        manifest_dir
            .join("tests")
            .join("spec_validation.rs")
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

fn expected_entry_name(block_id: &BlockHash) -> String {
    let hex = block_id.to_string();
    format!("{}/{}/{}.cbor", &hex[..2], &hex[2..4], hex)
}

fn write_zip_archive(path: &Path, entries: &[(String, Vec<u8>)]) {
    let file = File::create(path).unwrap();
    let mut archive = ZipWriter::new(file);
    let options = SimpleFileOptions::default();

    for (name, bytes) in entries {
        archive.start_file(name, options).unwrap();
        archive.write_all(bytes).unwrap();
    }

    archive.finish().unwrap();
}

fn write_zip_archive_with_duplicate_entry(path: &Path, entry_name: &str, bytes: &[u8]) {
    let duplicate_placeholder = entry_name.replacen(".cbor", ".dbor", 1);
    write_zip_archive(
        path,
        &[
            (entry_name.to_string(), bytes.to_vec()),
            (duplicate_placeholder.clone(), bytes.to_vec()),
        ],
    );

    let mut archive_bytes = std::fs::read(path).unwrap();
    replace_all_bytes(
        &mut archive_bytes,
        duplicate_placeholder.as_bytes(),
        entry_name.as_bytes(),
    );
    std::fs::write(path, archive_bytes).unwrap();
}

fn collect_block_ids(
    iter: lexongraph_block_store::BlockIdIterator<'_>,
) -> Result<HashSet<BlockHash>, BlockStoreError> {
    iter.collect::<Result<HashSet<_>, _>>()
}

fn replace_all_bytes(buffer: &mut [u8], needle: &[u8], replacement: &[u8]) {
    assert_eq!(needle.len(), replacement.len());
    let mut offset = 0;
    while let Some(found) = buffer[offset..]
        .windows(needle.len())
        .position(|window| window == needle)
    {
        let start = offset + found;
        let end = start + needle.len();
        buffer[start..end].copy_from_slice(replacement);
        offset = end;
    }
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
