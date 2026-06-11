// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::collections::HashSet;

use lexongraph_block::serialize_block;
#[cfg(feature = "inject")]
use lexongraph_block_store::conformance::run_full_suite;
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_block_store_memory::{MemoryBlockStore, MemoryBlockStoreBuildError};
use lexongraph_block_store_overlay::{OverlayGetOutcome, OverlayLayerNotifier, OverlayPutOutcome};

mod support;

#[cfg(feature = "inject")]
use support::MemoryHarness;
use support::{sample_leaf_block, validated_block};

#[test]
fn val_mem_store_001_and_002_constructor_enforces_positive_capacity() {
    assert_eq!(
        MemoryBlockStore::new(0).unwrap_err(),
        MemoryBlockStoreBuildError::ZeroCapacity
    );

    let store = MemoryBlockStore::new(2).unwrap();
    assert_eq!(store.max_resident_blocks(), 2);
}

#[test]
fn val_mem_store_003_and_004_put_round_trips_and_keeps_one_resident_entry_per_block_id() {
    let store = MemoryBlockStore::new(2).unwrap();
    let block = sample_leaf_block("resident");

    let first = store.put(&block).unwrap();
    let second = store.put(&block).unwrap();

    assert_eq!(first, second);
    assert_eq!(store.get(&first).unwrap().unwrap().hash, first);
    assert_eq!(resident_ids(&store), HashSet::from_iter([first]),);
}

#[test]
fn val_mem_store_005_missing_blocks_are_absent() {
    let store = MemoryBlockStore::new(1).unwrap();

    assert_eq!(
        store
            .get(&lexongraph_block::BlockHash::from_bytes([0x44; 32]))
            .unwrap(),
        None
    );
}

#[test]
fn val_mem_store_006_enumeration_yields_current_resident_ids_only() {
    let store = MemoryBlockStore::new(3).unwrap();
    let first = sample_leaf_block("first");
    let second = sample_leaf_block("second");

    let first_id = store.put(&first).unwrap();
    let second_id = store.put(&second).unwrap();

    assert_eq!(
        resident_ids(&store),
        HashSet::from_iter([first_id, second_id]),
    );
}

#[test]
fn val_mem_store_007_successful_get_refreshes_recency_before_eviction() {
    let store = MemoryBlockStore::new(2).unwrap();
    let first = sample_leaf_block("first");
    let second = sample_leaf_block("second");
    let third = sample_leaf_block("third");

    let first_id = store.put(&first).unwrap();
    let second_id = store.put(&second).unwrap();

    assert_eq!(store.get(&first_id).unwrap().unwrap().hash, first_id);
    let third_id = store.put(&third).unwrap();

    assert_eq!(
        resident_ids(&store),
        HashSet::from_iter([first_id, third_id]),
    );
    assert_eq!(store.get(&second_id).unwrap(), None);
}

#[test]
fn val_mem_store_008_least_recently_used_entry_is_evicted_on_capacity_pressure() {
    let store = MemoryBlockStore::new(2).unwrap();
    let first = sample_leaf_block("first");
    let second = sample_leaf_block("second");
    let third = sample_leaf_block("third");

    let first_id = store.put(&first).unwrap();
    let second_id = store.put(&second).unwrap();
    let third_id = store.put(&third).unwrap();

    assert_eq!(
        resident_ids(&store),
        HashSet::from_iter([second_id, third_id]),
    );
    assert_eq!(store.get(&first_id).unwrap(), None);
}

#[test]
fn val_mem_store_009_completed_get_hit_notification_populates_residency() {
    let store = MemoryBlockStore::new(1).unwrap();
    let block = validated_block("notified");

    store.on_get_result(&block.hash, OverlayGetOutcome::Hit(&block));

    assert_eq!(store.get(&block.hash).unwrap().unwrap(), block);
}

#[test]
fn val_mem_store_010_completed_get_hit_notification_refreshes_recency() {
    let store = MemoryBlockStore::new(2).unwrap();
    let first = validated_block("first");
    let second = validated_block("second");
    let third = sample_leaf_block("third");

    store.on_get_result(&first.hash, OverlayGetOutcome::Hit(&first));
    store.on_get_result(&second.hash, OverlayGetOutcome::Hit(&second));
    store.on_get_result(&first.hash, OverlayGetOutcome::Hit(&first));
    let third_id = store.put(&third).unwrap();

    assert_eq!(
        resident_ids(&store),
        HashSet::from_iter([first.hash, third_id]),
    );
    assert_eq!(store.get(&second.hash).unwrap(), None);
}

#[test]
fn val_mem_store_011_get_miss_and_error_notifications_do_not_populate() {
    let store = MemoryBlockStore::new(2).unwrap();
    let block = validated_block("notified");

    store.on_get_result(&block.hash, OverlayGetOutcome::Miss);
    store.on_get_result(
        &block.hash,
        OverlayGetOutcome::Error(&BlockStoreError::BackendFailure("failed".into())),
    );

    assert_eq!(resident_ids(&store), HashSet::new());
}

#[test]
fn val_mem_store_012_put_notifications_are_ignored() {
    let store = MemoryBlockStore::new(2).unwrap();
    let block = sample_leaf_block("ignored");
    let block_id = serialize_block(&block).unwrap().hash;
    let success = OverlayPutOutcome::Stored {
        block: &block,
        block_id,
    };
    let failure_error = BlockStoreError::BackendFailure("failed".into());
    let failure = OverlayPutOutcome::Error {
        block: &block,
        error: &failure_error,
    };

    store.on_put_result(&block, success);
    store.on_put_result(&block, failure);

    assert_eq!(resident_ids(&store), HashSet::new());
}

#[test]
#[cfg(feature = "inject")]
fn val_mem_store_013_parent_conformance_requirements_are_realized_by_tests() {
    run_full_suite(&MemoryHarness).unwrap();
}

#[test]
fn val_mem_store_014_public_surface_keeps_backend_volatile_and_read_populated() {
    let store = MemoryBlockStore::new(2).unwrap();
    let block = sample_leaf_block("volatile");
    let block_id = store.put(&block).unwrap();

    assert_eq!(store.max_resident_blocks(), 2);
    assert_eq!(store.get(&block_id).unwrap().unwrap().hash, block_id);
}

fn resident_ids(store: &MemoryBlockStore) -> HashSet<lexongraph_block::BlockHash> {
    store
        .iter_block_ids()
        .unwrap()
        .collect::<Result<HashSet<_>, _>>()
        .unwrap()
}
