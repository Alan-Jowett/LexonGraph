// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::collections::HashSet;
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use futures::TryStreamExt;
#[cfg(feature = "inject")]
use lexongraph_block::{BlockError, compute_block_hash};
use lexongraph_block::{BlockHash, serialize_block};
#[cfg(feature = "inject")]
use lexongraph_block_store::conformance::run_full_suite;
use lexongraph_block_store::{
    BlockBytesBatchEntry, BlockStore, BlockStoreError, BlockStoreExt, BlockStoreTelemetryCallback,
    BlockStoreTelemetryEvent,
};
use lexongraph_block_store_redb::{RedbBlockStore, RedbBlockStoreDurabilityMode};

mod support;

#[cfg(feature = "inject")]
use support::RedbHarness;
use support::{sample_leaf_block, validated_block};

const DATABASE_FILE_NAME: &str = "blocks.redb";
// These header values mirror the redb internal page-store header layout used by
// the version currently resolved in Cargo.lock (at the time of writing, redb
// 2.6.3), even though Cargo.toml declares 2.1.0 as the semver minimum:
// - GOD_BYTE_OFFSET is immediately after the 9-byte magic number
// - RECOVERY_REQUIRED marks the file as needing repair on next open
// - TWO_PHASE_COMMIT is cleared here to match redb's own repair-path tests
// See redb's local source under:
// src/tree_store/page_store/header.rs
const GOD_BYTE_OFFSET: u64 = 9;
const RECOVERY_REQUIRED: u8 = 2;
const TWO_PHASE_COMMIT: u8 = 4;

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
fn redb_store_shared_telemetry_callback_can_be_updated_after_construction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = RedbBlockStore::new(temp_dir.path()).unwrap();
    let events = Arc::new(Mutex::new(Vec::<BlockStoreTelemetryEvent>::new()));

    store
        .set_telemetry_callback(Some(telemetry_collector(events.clone())))
        .unwrap();

    assert_eq!(events.lock().unwrap().len(), 0);
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

    store.compact_now().unwrap();
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

#[test]
fn val_redb_store_016_batch_writes_commit_atomically_in_durable_mode() {
    let temp_dir = tempfile::tempdir().unwrap();
    let first = serialize_block(&sample_leaf_block("first")).unwrap();
    let second = serialize_block(&sample_leaf_block("second")).unwrap();
    {
        let store = RedbBlockStore::new(temp_dir.path()).unwrap();
        let entries = [
            BlockBytesBatchEntry {
                block_id: &first.hash,
                block_bytes: &first.bytes,
            },
            BlockBytesBatchEntry {
                block_id: &second.hash,
                block_bytes: &second.bytes,
            },
        ];

        store.put_block_bytes_batch(&entries).unwrap();
    }

    let reopened = RedbBlockStore::new(temp_dir.path()).unwrap();
    assert_eq!(reopened.get(&first.hash).unwrap().unwrap().hash, first.hash);
    assert_eq!(
        reopened.get(&second.hash).unwrap().unwrap().hash,
        second.hash
    );
}

#[test]
#[cfg(feature = "inject")]
fn val_redb_store_017_batch_conflicts_abort_the_full_operation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = RedbBlockStore::new(temp_dir.path()).unwrap();
    let first = serialize_block(&sample_leaf_block("first")).unwrap();
    let second = serialize_block(&sample_leaf_block("second")).unwrap();
    store
        .raw_insert(second.hash, b"conflicting bytes".to_vec())
        .unwrap();
    let entries = [
        BlockBytesBatchEntry {
            block_id: &first.hash,
            block_bytes: &first.bytes,
        },
        BlockBytesBatchEntry {
            block_id: &second.hash,
            block_bytes: &second.bytes,
        },
    ];

    let error = pollster::block_on(store.put_block_bytes_batch(&entries)).unwrap_err();

    expect_backend_failure_contains(error, "integrity conflict");
    assert_eq!(store.get_block_bytes(&first.hash).unwrap(), None);
    assert_eq!(
        store.get_block_bytes(&second.hash).unwrap().unwrap(),
        b"conflicting bytes".to_vec()
    );
}

#[test]
#[cfg(feature = "inject")]
fn val_redb_store_018_fast_mode_batch_writes_flush_after_graceful_shutdown() {
    let temp_dir = tempfile::tempdir().unwrap();
    let first = serialize_block(&sample_leaf_block("first")).unwrap();
    let second = serialize_block(&sample_leaf_block("second")).unwrap();

    {
        let store = RedbBlockStore::new_with_durability(
            temp_dir.path(),
            RedbBlockStoreDurabilityMode::Fast,
        )
        .unwrap();
        let entries = [
            BlockBytesBatchEntry {
                block_id: &first.hash,
                block_bytes: &first.bytes,
            },
            BlockBytesBatchEntry {
                block_id: &second.hash,
                block_bytes: &second.bytes,
            },
        ];

        store.put_block_bytes_batch(&entries).unwrap();

        assert!(store.pending_fast_mode_flush());
        assert_eq!(store.get(&first.hash).unwrap().unwrap().hash, first.hash);
        assert_eq!(store.get(&second.hash).unwrap().unwrap().hash, second.hash);
    }

    let reopened = RedbBlockStore::new(temp_dir.path()).unwrap();
    assert_eq!(reopened.get(&first.hash).unwrap().unwrap().hash, first.hash);
    assert_eq!(
        reopened.get(&second.hash).unwrap().unwrap().hash,
        second.hash
    );
}

#[test]
fn val_redb_store_019_repair_status_updates_emit_shared_telemetry_events() {
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let store = RedbBlockStore::new(temp_dir.path()).unwrap();
        store.put(&sample_leaf_block("repair-a")).unwrap();
        store.put(&sample_leaf_block("repair-b")).unwrap();
    }
    mark_database_as_repair_required(&temp_dir.path().join(DATABASE_FILE_NAME));
    let events = Arc::new(Mutex::new(Vec::<BlockStoreTelemetryEvent>::new()));

    let store =
        RedbBlockStore::new_with_telemetry(temp_dir.path(), telemetry_collector(events.clone()))
            .unwrap();

    let captured = events.lock().unwrap();
    assert!(!captured.is_empty());
    assert!(captured.iter().all(|event| event.name == "repair_status"));
    assert!(captured.iter().all(|event| {
        event.attributes.get("backend").map(String::as_str) == Some("redb")
            && event.attributes.contains_key("database_path")
            && event.attributes.contains_key("progress")
    }));
    drop(captured);
    assert!(store.iter_block_ids().is_ok());
}

#[test]
fn val_redb_store_020_repair_events_use_generic_name_message_and_attributes() {
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let store = RedbBlockStore::new(temp_dir.path()).unwrap();
        store.put(&sample_leaf_block("repair-shape")).unwrap();
    }
    mark_database_as_repair_required(&temp_dir.path().join(DATABASE_FILE_NAME));
    let events = Arc::new(Mutex::new(Vec::<BlockStoreTelemetryEvent>::new()));

    let _store =
        RedbBlockStore::new_with_telemetry(temp_dir.path(), telemetry_collector(events.clone()))
            .unwrap();

    let captured = events.lock().unwrap();
    let first = captured
        .first()
        .expect("expected at least one repair event");
    assert_eq!(first.name, "repair_status");
    assert_eq!(
        first.message.as_deref(),
        Some("redb reported database repair progress")
    );
    assert_eq!(
        first.attributes.get("backend").map(String::as_str),
        Some("redb")
    );
}

#[test]
fn val_redb_store_021_repair_without_shared_telemetry_preserves_existing_behavior() {
    let temp_dir = tempfile::tempdir().unwrap();
    let expected = validated_block("repair-without-telemetry");
    {
        let store = RedbBlockStore::new(temp_dir.path()).unwrap();
        store.put(&expected.block).unwrap();
    }
    mark_database_as_repair_required(&temp_dir.path().join(DATABASE_FILE_NAME));

    let reopened = RedbBlockStore::new(temp_dir.path()).unwrap();

    assert_eq!(reopened.get(&expected.hash).unwrap(), Some(expected));
}

#[test]
fn val_redb_store_022_shared_telemetry_observers_cannot_control_repair() {
    fn observe(callback: &BlockStoreTelemetryCallback) {
        let _: () = callback(
            BlockStoreTelemetryEvent::new("repair_status")
                .with_message("observers receive events only")
                .with_attribute("backend", "redb"),
        );
    }

    let callback: BlockStoreTelemetryCallback = Arc::new(|_| {});
    observe(&callback);
}

#[test]
fn val_redb_store_023_read_only_open_reads_without_mutating_the_database_header() {
    let temp_dir = tempfile::tempdir().unwrap();
    let expected = validated_block("read-only");
    {
        let store = RedbBlockStore::new(temp_dir.path()).unwrap();
        store.put(&expected.block).unwrap();
    }
    let database_path = temp_dir.path().join(DATABASE_FILE_NAME);
    let header_before = read_god_byte(&database_path);

    let store = RedbBlockStore::new_read_only(temp_dir.path()).unwrap();

    assert_eq!(store.get(&expected.hash).unwrap(), Some(expected));
    drop(store);
    assert_eq!(read_god_byte(&database_path), header_before);
}

#[cfg(any(unix, windows))]
#[test]
fn val_redb_store_028_read_only_open_preserves_native_writer_locking() {
    let temp_dir = tempfile::tempdir().unwrap();
    let writable = RedbBlockStore::new(temp_dir.path()).unwrap();

    let error = RedbBlockStore::new_read_only(temp_dir.path()).unwrap_err();

    match error {
        BlockStoreError::BackendFailure(message) => assert!(
            message.contains("already open") || message.contains("locked a portion"),
            "expected native writer-lock failure, got {message:?}"
        ),
        other => panic!("expected native writer-lock failure, got {other:?}"),
    }
    drop(writable);
}

#[test]
fn val_redb_store_024_read_only_enumeration_yields_persisted_block_ids_only() {
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let store = RedbBlockStore::new(temp_dir.path()).unwrap();
        store.put(&sample_leaf_block("first")).unwrap();
        store.put(&sample_leaf_block("second")).unwrap();
    }

    let store = RedbBlockStore::new_read_only(temp_dir.path()).unwrap();

    assert_eq!(
        persisted_ids(&store),
        HashSet::from([
            validated_block("first").hash,
            validated_block("second").hash,
        ])
    );
}

#[test]
fn val_redb_store_025_read_only_write_paths_fail_explicitly() {
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let store = RedbBlockStore::new(temp_dir.path()).unwrap();
        store.put(&sample_leaf_block("existing")).unwrap();
    }
    let store = RedbBlockStore::new_read_only(temp_dir.path()).unwrap();
    let block = sample_leaf_block("blocked-write");
    let serialized = serialize_block(&block).unwrap();
    let entries = [BlockBytesBatchEntry {
        block_id: &serialized.hash,
        block_bytes: &serialized.bytes,
    }];

    expect_backend_failure_contains(
        pollster::block_on(store.put(&block)).unwrap_err(),
        "read-only mode",
    );
    expect_backend_failure_contains(
        pollster::block_on(store.put_block_bytes_batch(&entries)).unwrap_err(),
        "read-only mode",
    );
    assert_eq!(store.get(&serialized.hash).unwrap(), None);
}

#[test]
fn val_redb_store_026_read_only_compact_now_fails_explicitly() {
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let store = RedbBlockStore::new(temp_dir.path()).unwrap();
        store.put(&sample_leaf_block("existing")).unwrap();
    }
    let mut store = RedbBlockStore::new_read_only(temp_dir.path()).unwrap();

    let error = store.compact_now().unwrap_err();

    expect_backend_failure_contains(error, "read-only mode");
}

#[test]
fn val_redb_store_027_read_only_open_fails_when_recovery_is_required() {
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let store = RedbBlockStore::new(temp_dir.path()).unwrap();
        store.put(&sample_leaf_block("repair")).unwrap();
    }
    mark_database_as_repair_required(&temp_dir.path().join(DATABASE_FILE_NAME));

    let error = RedbBlockStore::new_read_only(temp_dir.path()).unwrap_err();

    expect_backend_failure_contains(error, "recovery is required");
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

fn mark_database_as_repair_required(database_path: &Path) {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(database_path)
        .unwrap();
    let file_len = file.metadata().unwrap().len();
    assert!(
        file_len > GOD_BYTE_OFFSET,
        "expected redb database header in {} to be longer than byte offset {}, got {} bytes",
        database_path.display(),
        GOD_BYTE_OFFSET,
        file_len
    );
    file.seek(SeekFrom::Start(GOD_BYTE_OFFSET)).unwrap();
    let mut buffer = [0u8; 1];
    file.read_exact(&mut buffer).unwrap();
    file.seek(SeekFrom::Start(GOD_BYTE_OFFSET)).unwrap();
    buffer[0] |= RECOVERY_REQUIRED;
    buffer[0] &= !TWO_PHASE_COMMIT;
    file.write_all(&buffer).unwrap();
    file.flush().unwrap();
    file.sync_all().unwrap();
}

fn read_god_byte(database_path: &Path) -> u8 {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .open(database_path)
        .unwrap();
    file.seek(SeekFrom::Start(GOD_BYTE_OFFSET)).unwrap();
    let mut buffer = [0u8; 1];
    file.read_exact(&mut buffer).unwrap();
    buffer[0]
}

fn telemetry_collector(
    events: Arc<Mutex<Vec<BlockStoreTelemetryEvent>>>,
) -> BlockStoreTelemetryCallback {
    Arc::new(move |event| {
        events.lock().unwrap().push(event);
    })
}
