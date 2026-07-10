// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::collections::HashSet;
use std::future::Future;

use lexongraph_block::BlockHash;
#[cfg(feature = "inject")]
use lexongraph_block_store::conformance::run_full_suite;
use lexongraph_block_store::{BlockStore, BlockStoreExt};
use lexongraph_block_store_memory::{MemoryBlockStore, MemoryBlockStoreBuildError};

mod support;

#[cfg(feature = "inject")]
use support::MemoryHarness;
use support::sample_leaf_block;

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
fn val_mem_store_001_and_002_constructor_enforces_positive_capacity() {
    assert_eq!(
        MemoryBlockStore::new(0).unwrap_err(),
        MemoryBlockStoreBuildError::ZeroCapacity
    );
    assert_eq!(
        MemoryBlockStore::new_cache_mb(0).unwrap_err(),
        MemoryBlockStoreBuildError::ZeroCapacity
    );

    let store = MemoryBlockStore::new(2).unwrap();
    assert_eq!(store.max_resident_blocks(), 2);
    MemoryBlockStore::new_cache_mb(1).unwrap();
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
#[cfg(feature = "inject")]
fn val_mem_store_009_parent_conformance_requirements_are_realized_by_tests() {
    pollster::block_on(run_full_suite(&MemoryHarness)).unwrap();
}

#[test]
fn val_mem_store_010_public_surface_keeps_backend_volatile_and_bounded() {
    let store = MemoryBlockStore::new(2).unwrap();
    let block = sample_leaf_block("volatile");
    let block_id = store.put(&block).unwrap();

    assert_eq!(store.max_resident_blocks(), 2);
    assert_eq!(store.get(&block_id).unwrap().unwrap().hash, block_id);
}

#[test]
fn val_mem_store_011_cache_mode_evicts_least_recently_used_entries_by_payload_bytes() {
    let store = MemoryBlockStore::new_cache_mb(1).unwrap();
    let first = sample_leaf_block(&"a".repeat(700_000));
    let second = sample_leaf_block(&"b".repeat(300_000));
    let third = sample_leaf_block(&"c".repeat(300_000));

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
fn val_mem_store_012_cache_mode_rejects_blocks_larger_than_total_budget() {
    let store = MemoryBlockStore::new_cache_mb(1).unwrap();
    let oversized = sample_leaf_block(&"x".repeat(1_100_000));

    let error = store.put(&oversized).unwrap_err();

    expect_backend_failure_contains(error, "exceeds cache capacity");
    assert!(resident_ids(&store).is_empty());
}

#[test]
fn val_mem_store_013_cache_mode_replacement_evicts_other_entries_before_growing_bytes() {
    let store = MemoryBlockStore::new_cache_mb(1).unwrap();
    let first_id = BlockHash::from_bytes([0x11; 32]);
    let second_id = BlockHash::from_bytes([0x22; 32]);

    store
        .put_block_bytes(&first_id, &vec![0xaa; 200_000])
        .unwrap();
    store
        .put_block_bytes(&second_id, &vec![0xbb; 700_000])
        .unwrap();

    store
        .put_block_bytes(&first_id, &vec![0xcc; 400_000])
        .unwrap();

    assert_eq!(
        store.get_block_bytes(&first_id).unwrap().unwrap().len(),
        400_000
    );
    assert_eq!(store.get_block_bytes(&second_id).unwrap(), None);
}

fn resident_ids(store: &MemoryBlockStore) -> HashSet<lexongraph_block::BlockHash> {
    store
        .list_block_ids()
        .unwrap()
        .into_iter()
        .collect::<HashSet<_>>()
}

fn expect_backend_failure_contains(
    error: lexongraph_block_store::BlockStoreError,
    expected_fragment: &str,
) {
    match error {
        lexongraph_block_store::BlockStoreError::BackendFailure(message) => {
            assert!(
                message.contains(expected_fragment),
                "expected backend failure containing {expected_fragment:?}, got {message:?}"
            );
        }
        other => panic!("expected backend failure, got {other:?}"),
    }
}
