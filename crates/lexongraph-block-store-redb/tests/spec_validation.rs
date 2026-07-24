// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::collections::HashSet;
use std::future::Future;

use futures::TryStreamExt;
use lexongraph_block::BlockHash;
#[cfg(feature = "inject")]
use lexongraph_block::{BlockError, compute_block_hash, serialize_block};
#[cfg(feature = "inject")]
use lexongraph_block_store::conformance::run_full_suite;
use lexongraph_block_store::{BlockStore, BlockStoreError, BlockStoreExt};
use lexongraph_block_store_redb::{RedbBlockStore, RedbBlockStoreDurabilityMode};

mod support;

#[cfg(feature = "inject")]
use support::RedbHarness;
use support::{sample_leaf_block, validated_block};

const DATABASE_FILE_NAME: &str = "blocks.redb";

trait BlockingResultFutureExt<T, E>: Future<Output = Result<T, E>> + Sized {
    fn unwrap(self) -> T
    where
        E: std::fmt::Debug,
    {
        pollster::block_on(self).unwrap()
    }
}

impl<F, T, E> BlockingResultFutureExt<T, E> for F where F: Future<Output = Result<T, E>> {}

#[test]
fn val_redb_store_001_constructor_initializes_store_root_and_backend_private_database() {
    let temp_dir = tempfile::tempdir().unwrap();
    let requested_root = temp_dir.path().join("nested").join("store");

    let store = RedbBlockStore::new(&requested_root).unwrap();

    assert!(format!("{store:?}").contains("RedbBlockStore"));
    assert!(requested_root.join(DATABASE_FILE_NAME).is_file());
}

#[test]
fn val_redb_store_002_constructor_fails_for_non_directory_root_and_unopenable_database_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_root = temp_dir.path().join("not-a-directory");
    std::fs::write(&file_root, b"root").unwrap();

    expect_backend_failure_contains(
        RedbBlockStore::new(&file_root).unwrap_err(),
        "create store root",
    );

    let blocked_root = temp_dir.path().join("blocked-db");
    std::fs::create_dir_all(&blocked_root).unwrap();
    std::fs::create_dir(blocked_root.join(DATABASE_FILE_NAME)).unwrap();

    expect_backend_failure_contains(
        RedbBlockStore::new(&blocked_root).unwrap_err(),
        "failed to open redb database",
    );
}

#[test]
fn val_redb_store_003_put_and_get_round_trip_through_the_parent_contract() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = RedbBlockStore::new(temp_dir.path()).unwrap();
    let expected = validated_block("round-trip");

    let block_id = store.put(&expected.block).unwrap();

    assert_eq!(block_id, expected.hash);
    assert_eq!(store.get(&block_id).unwrap(), Some(expected));
}

#[test]
fn val_redb_store_003a_default_mode_retains_immediate_reopen_visibility() {
    let temp_dir = tempfile::tempdir().unwrap();
    let expected = validated_block("default-durable");

    let block_id = {
        let store = RedbBlockStore::new_with_durability(
            temp_dir.path(),
            RedbBlockStoreDurabilityMode::Durable,
        )
        .unwrap();
        store.put(&expected.block).unwrap()
    };
    let reopened = RedbBlockStore::new(temp_dir.path()).unwrap();

    assert_eq!(block_id, expected.hash);
    assert_eq!(reopened.get(&block_id).unwrap(), Some(expected));
}

#[test]
#[cfg(feature = "inject")]
fn val_redb_store_003b_fast_mode_put_skips_immediate_flush_but_remains_readable_in_process() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store =
        RedbBlockStore::new_with_durability(temp_dir.path(), RedbBlockStoreDurabilityMode::Fast)
            .unwrap();
    let expected = validated_block("fast-mode");

    let block_id = store.put(&expected.block).unwrap();

    assert_eq!(block_id, expected.hash);
    assert!(store.pending_fast_mode_flush());
    assert_eq!(store.get(&block_id).unwrap(), Some(expected));
}

#[test]
fn val_redb_store_004_missing_blocks_return_ok_none() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = RedbBlockStore::new(temp_dir.path()).unwrap();

    assert_eq!(store.get(&BlockHash::from_bytes([0x44; 32])).unwrap(), None);
}

#[test]
#[cfg(feature = "inject")]
fn val_redb_store_005_and_006_get_reports_malformed_content_and_integrity_mismatch_explicitly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = RedbBlockStore::new(temp_dir.path()).unwrap();

    let first = serialize_block(&sample_leaf_block("first")).unwrap();
    let second = serialize_block(&sample_leaf_block("second")).unwrap();
    store.raw_insert(second.hash, first.bytes.clone()).unwrap();

    assert_eq!(
        pollster::block_on(store.get(&second.hash)).unwrap_err(),
        BlockStoreError::DecodeFailure(BlockError::HashMismatch {
            expected: second.hash,
            actual: first.hash,
        })
    );

    let malformed_bytes = vec![0xff, 0x00, 0xaa];
    let malformed_hash = compute_block_hash(&malformed_bytes);
    store.raw_insert(malformed_hash, malformed_bytes).unwrap();

    assert!(matches!(
        pollster::block_on(store.get(&malformed_hash)).unwrap_err(),
        BlockStoreError::DecodeFailure(BlockError::MalformedCbor(_))
    ));
}

#[test]
#[cfg(feature = "inject")]
fn val_redb_store_007_conflicting_existing_bytes_fail_without_overwrite() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = RedbBlockStore::new(temp_dir.path()).unwrap();
    let block = sample_leaf_block("conflict");
    let serialized = serialize_block(&block).unwrap();
    let conflicting_bytes = b"not canonical bytes".to_vec();
    store
        .raw_insert(serialized.hash, conflicting_bytes.clone())
        .unwrap();

    let error = pollster::block_on(store.put(&block)).unwrap_err();

    expect_backend_failure_contains(error, "integrity conflict");
    assert_eq!(
        store.get_block_bytes(&serialized.hash).unwrap().unwrap(),
        conflicting_bytes
    );
}

#[test]
fn val_redb_store_008_successful_commits_are_visible_after_reopening_the_same_root() {
    let temp_dir = tempfile::tempdir().unwrap();
    let block = sample_leaf_block("persisted");
    let expected = validated_block("persisted");

    {
        let store = RedbBlockStore::new(temp_dir.path()).unwrap();
        assert_eq!(store.put(&block).unwrap(), expected.hash);
    }

    let reopened = RedbBlockStore::new(temp_dir.path()).unwrap();
    assert_eq!(reopened.get(&expected.hash).unwrap(), Some(expected));
}

#[test]
#[cfg(feature = "inject")]
fn val_redb_store_008a_fast_mode_flushes_on_final_handle_drop() {
    let temp_dir = tempfile::tempdir().unwrap();
    let expected = validated_block("fast-persisted");

    {
        let store = RedbBlockStore::new_with_durability(
            temp_dir.path(),
            RedbBlockStoreDurabilityMode::Fast,
        )
        .unwrap();
        let clone = store.clone();

        assert_eq!(store.put(&expected.block).unwrap(), expected.hash);
        assert!(store.pending_fast_mode_flush());

        drop(clone);
        assert!(store.pending_fast_mode_flush());
    }

    let reopened = RedbBlockStore::new(temp_dir.path()).unwrap();
    assert_eq!(reopened.get(&expected.hash).unwrap(), Some(expected));
}

#[test]
#[cfg(feature = "inject")]
fn val_redb_store_008b_fast_mode_crash_durability_boundary_remains_pending_until_shutdown() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store =
        RedbBlockStore::new_with_durability(temp_dir.path(), RedbBlockStoreDurabilityMode::Fast)
            .unwrap();

    store.put(&sample_leaf_block("crash-boundary")).unwrap();

    assert!(store.pending_fast_mode_flush());
}

#[test]
fn val_redb_store_009_enumeration_yields_persisted_block_ids_only() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = RedbBlockStore::new(temp_dir.path()).unwrap();
    let first = sample_leaf_block("first");
    let second = sample_leaf_block("second");

    let expected = HashSet::from([store.put(&first).unwrap(), store.put(&second).unwrap()]);

    assert_eq!(persisted_ids(&store), expected);
}

#[test]
#[cfg(feature = "inject")]
fn val_redb_store_010_enumeration_reports_malformed_persisted_keys_explicitly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = RedbBlockStore::new(temp_dir.path()).unwrap();

    store
        .raw_insert_key_value(vec![0x11; 31], b"value".to_vec())
        .unwrap();

    match store.iter_block_ids() {
        Ok(stream) => {
            let error = pollster::block_on(stream.try_collect::<Vec<_>>()).unwrap_err();
            expect_backend_failure_contains(error, "failed to decode an enumerated redb block key")
        }
        Err(error) => {
            expect_backend_failure_contains(error, "failed to decode an enumerated redb block key")
        }
    }
}

#[test]
#[cfg(feature = "inject")]
fn val_redb_store_011_parent_conformance_requirements_are_realized_by_tests() {
    pollster::block_on(run_full_suite(&RedbHarness::default())).unwrap();
}

#[test]
fn val_redb_store_012_public_surface_remains_backend_neutral() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = RedbBlockStore::new(temp_dir.path()).unwrap();
    let block = sample_leaf_block("neutral");
    let block_id = store.put(&block).unwrap();

    assert_eq!(store.list_block_ids().unwrap(), vec![block_id]);
    assert_eq!(store.get(&block_id).unwrap().unwrap().hash, block_id);
}

#[test]
fn val_redb_store_013_concrete_store_exposes_compact_now() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut store = RedbBlockStore::new(temp_dir.path()).unwrap();

    assert!(store.compact_now().is_ok());
}

#[test]
#[cfg(feature = "inject")]
fn val_redb_store_014_compact_now_preserves_visibility_and_flushes_fast_mode_writes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let expected = validated_block("compact-now");
    let mut store =
        RedbBlockStore::new_with_durability(temp_dir.path(), RedbBlockStoreDurabilityMode::Fast)
            .unwrap();

    let block_id = store.put(&expected.block).unwrap();
    assert!(store.pending_fast_mode_flush());

    store.compact_now().unwrap();
    assert!(!store.pending_fast_mode_flush());

    drop(store);

    let reopened = RedbBlockStore::new(temp_dir.path()).unwrap();
    assert_eq!(block_id, expected.hash);
    assert_eq!(reopened.get(&block_id).unwrap(), Some(expected));
}

#[test]
fn val_redb_store_015_compact_now_fails_without_exclusive_ownership() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut store = RedbBlockStore::new(temp_dir.path()).unwrap();
    let clone = store.clone();

    let error = store.compact_now().unwrap_err();

    expect_backend_failure_contains(error, "exclusive store ownership is required");
    drop(clone);
}

fn persisted_ids(store: &RedbBlockStore) -> HashSet<BlockHash> {
    pollster::block_on(store.iter_block_ids().unwrap().try_collect()).unwrap()
}

fn expect_backend_failure_contains(error: BlockStoreError, needle: &str) {
    match error {
        BlockStoreError::BackendFailure(message) => assert!(
            message.contains(needle),
            "expected backend failure containing {needle:?}, got {message:?}"
        ),
        other => panic!("expected backend failure containing {needle:?}, got {other:?}"),
    }
}
