// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use lexongraph_block::{Block, BlockHash, ValidatedBlock, serialize_block};
use lexongraph_block_store::conformance::{
    BlockStoreConformanceHarness, BlockStoreFactory, run_full_suite,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_block_store_azure::AzureBlobBlockStore;
use lexongraph_block_store_fs::FilesystemBlockStore;
use lexongraph_block_store_memory::MemoryBlockStore;
use lexongraph_block_store_overlay::{
    OverlayBlockStore, OverlayBuildError, OverlayLayerRole, OverlayStoreLayer, PassiveLayer,
};
use tempfile::TempDir;

mod support;

use support::{SharedMemoryBlockStore, sample_leaf_block};

#[test]
fn val_overlay_store_001_constructor_requires_at_least_two_layers() {
    assert_eq!(
        OverlayBlockStore::new(vec![]).unwrap_err(),
        OverlayBuildError::InsufficientLayers { count: 0 }
    );
    assert_eq!(
        OverlayBlockStore::new(vec![Box::new(PassiveLayer::new(MockStore::default()))])
            .unwrap_err(),
        OverlayBuildError::InsufficientLayers { count: 1 }
    );
}

#[test]
fn val_overlay_store_002_and_003_get_uses_priority_order_and_stops_at_first_hit() {
    let higher_block = validated_block("higher");
    let lower_block = validated_block("lower");
    let high = MockStore::for_get(Ok(Some(higher_block.clone())));
    let low = MockStore::for_get(Ok(Some(lower_block)));
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::new(high.clone())),
        Box::new(PassiveLayer::new(low.clone())),
    ])
    .unwrap();

    let loaded = overlay.get(&higher_block.hash).unwrap().unwrap();

    assert_eq!(loaded, higher_block);
    assert_eq!(high.get_calls(), 1);
    assert_eq!(low.get_calls(), 0);
}

#[test]
fn val_overlay_store_004_get_falls_through_errors_to_lower_success() {
    let expected = validated_block("fallback");
    let high = MockStore::for_get(Err(backend_failure("higher read failed")));
    let low = MockStore::for_get(Ok(Some(expected.clone())));
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::new(high.clone())),
        Box::new(PassiveLayer::new(low.clone())),
    ])
    .unwrap();

    let loaded = overlay.get(&expected.hash).unwrap().unwrap();

    assert_eq!(loaded, expected);
    assert_eq!(high.get_calls(), 1);
    assert_eq!(low.get_calls(), 1);
}

#[test]
fn val_overlay_store_005_get_returns_ok_none_only_when_all_layers_are_absent_or_last_error() {
    let missing = BlockHash::from_bytes([0x44; 32]);
    let absent_overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::new(MockStore::for_get(Ok(None)))),
        Box::new(PassiveLayer::new(MockStore::for_get(Ok(None)))),
    ])
    .unwrap();

    assert_eq!(absent_overlay.get(&missing).unwrap(), None);

    let expected_error = backend_failure("lowest read failed");
    let mixed_overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::new(MockStore::for_get(Err(backend_failure(
            "highest read failed",
        ))))),
        Box::new(PassiveLayer::new(MockStore::for_get(Ok(None)))),
        Box::new(PassiveLayer::new(MockStore::for_get(Err(
            expected_error.clone()
        )))),
    ])
    .unwrap();

    assert_eq!(mixed_overlay.get(&missing).unwrap_err(), expected_error);
}

#[test]
fn val_overlay_store_006_put_writes_all_writable_layers_and_skips_cache_and_read_only_layers() {
    let block = sample_leaf_block("stored");
    let block_id = serialize_block(&block).unwrap().hash;
    let cache = MockStore::for_put(Ok(block_id));
    let writable_one = MockStore::for_put(Ok(block_id));
    let read_only = MockStore::for_put(Ok(block_id));
    let writable_two = MockStore::for_put(Ok(block_id));
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::cache(cache.clone())),
        Box::new(PassiveLayer::writable(writable_one.clone())),
        Box::new(PassiveLayer::read_only(read_only.clone())),
        Box::new(PassiveLayer::writable(writable_two.clone())),
    ])
    .unwrap();

    let stored = overlay.put(&block).unwrap();

    assert_eq!(stored, block_id);
    assert_eq!(cache.put_calls(), 0);
    assert_eq!(writable_one.put_calls(), 1);
    assert_eq!(read_only.put_calls(), 0);
    assert_eq!(writable_two.put_calls(), 1);
}

#[test]
fn val_overlay_store_007_put_reports_failure_if_any_writable_layer_fails_after_attempting_all_writes()
 {
    let block = sample_leaf_block("failure");
    let block_id = serialize_block(&block).unwrap().hash;
    let first = MockStore::for_put(Err(backend_failure("first write failed")));
    let second = MockStore::for_put(Ok(block_id));
    let third = MockStore::for_put(Err(backend_failure("third write failed")));
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::writable(first.clone())),
        Box::new(PassiveLayer::writable(second.clone())),
        Box::new(PassiveLayer::writable(third.clone())),
    ])
    .unwrap();

    assert_eq!(
        overlay.put(&block).unwrap_err(),
        backend_failure("third write failed")
    );
    assert_eq!(first.put_calls(), 1);
    assert_eq!(second.put_calls(), 1);
    assert_eq!(third.put_calls(), 1);
}

#[test]
fn val_overlay_store_008_put_fails_when_no_layer_accepts_direct_writes() {
    let block = sample_leaf_block("nowhere");
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::cache(MockStore::default())),
        Box::new(PassiveLayer::read_only(MockStore::default())),
    ])
    .unwrap();

    assert_eq!(
        overlay.put(&block).unwrap_err(),
        backend_failure("overlay block store has no layers that accept direct writes")
    );
}

#[test]
fn val_overlay_store_009_parent_conformance_requirements_are_realized_by_tests() {
    run_full_suite(&OverlayHarness).unwrap();
}

#[test]
fn val_overlay_store_010_enumeration_yields_the_deduplicated_union_in_priority_order() {
    let first = validated_block("first").hash;
    let second = validated_block("second").hash;
    let third = validated_block("third").hash;
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::new(MockStore::for_iter(Ok(vec![
            Ok(first),
            Ok(second),
        ])))),
        Box::new(PassiveLayer::new(MockStore::for_iter(Ok(vec![
            Ok(second),
            Ok(third),
        ])))),
    ])
    .unwrap();

    let enumerated = overlay
        .iter_block_ids()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(enumerated, vec![first, second, third]);
}

#[test]
fn val_overlay_store_011_enumeration_failures_are_explicit() {
    let first = validated_block("first").hash;
    let startup_error = backend_failure("highest enumeration failed before startup");
    let startup_overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::new(MockStore::for_iter(Err(
            startup_error.clone()
        )))),
        Box::new(PassiveLayer::new(MockStore::for_iter(Ok(vec![Ok(first)])))),
    ])
    .unwrap();

    match startup_overlay.iter_block_ids() {
        Ok(_) => panic!("expected highest-priority enumeration startup to fail explicitly"),
        Err(error) => assert_eq!(error, startup_error),
    }

    let expected_error = backend_failure("lower enumeration failed");
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::new(MockStore::for_iter(Ok(vec![Ok(first)])))),
        Box::new(PassiveLayer::new(MockStore::for_iter(Err(
            expected_error.clone()
        )))),
    ])
    .unwrap();
    let mut iter = overlay.iter_block_ids().unwrap();

    assert_eq!(iter.next().unwrap().unwrap(), first);
    assert_eq!(iter.next().unwrap().unwrap_err(), expected_error);
    assert!(iter.next().is_none());
}

#[test]
fn val_overlay_store_012_lower_layer_hits_refill_higher_cache_layers() {
    let cache = SharedMemoryBlockStore::default();
    let source = SharedMemoryBlockStore::default();
    let block = sample_leaf_block("cached");
    let block_id = source.put(&block).unwrap();
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::cache(cache.clone())),
        Box::new(PassiveLayer::read_only(source.clone())),
    ])
    .unwrap();

    assert_eq!(cache.get(&block_id).unwrap(), None);
    assert_eq!(overlay.get(&block_id).unwrap().unwrap().hash, block_id);
    assert_eq!(cache.get(&block_id).unwrap().unwrap().hash, block_id);
}

#[test]
fn val_overlay_store_013_cache_refill_failures_are_non_fatal() {
    let block = validated_block("cached");
    let cache = MockStore::for_put(Err(backend_failure("cache write-back failed")));
    let source = MockStore::for_get(Ok(Some(block.clone())));
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::cache(cache.clone())),
        Box::new(PassiveLayer::read_only(source)),
    ])
    .unwrap();

    let loaded = overlay.get(&block.hash).unwrap().unwrap();

    assert_eq!(loaded, block);
    assert_eq!(cache.put_calls(), 1);
}

#[test]
fn val_overlay_store_014_public_surface_keeps_role_based_layering_additive() {
    let passive_overlay = OverlayBlockStore::from_layers([
        PassiveLayer::cache(MockStore::default()),
        PassiveLayer::writable(MockStore::default()),
    ])
    .unwrap();

    assert_eq!(passive_overlay.layer_count(), 2);

    let custom_overlay = OverlayBlockStore::new(vec![
        Box::new(CustomLayer::default()) as Box<dyn OverlayStoreLayer>,
        Box::new(PassiveLayer::read_only(MockStore::default())) as Box<dyn OverlayStoreLayer>,
    ])
    .unwrap();

    assert_eq!(custom_overlay.layer_count(), 2);
}

#[test]
fn val_overlay_store_015_put_fails_if_a_writable_layer_returns_a_non_canonical_block_id() {
    let block = sample_leaf_block("mismatch");
    let block_id = serialize_block(&block).unwrap().hash;
    let wrong_id = validated_block("wrong").hash;
    let first = MockStore::for_put(Ok(wrong_id));
    let second = MockStore::for_put(Ok(block_id));
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::writable(first.clone())),
        Box::new(PassiveLayer::writable(second.clone())),
    ])
    .unwrap();

    assert_eq!(
        overlay.put(&block).unwrap_err(),
        BlockStoreError::ContractViolation(lexongraph_block::BlockError::HashMismatch {
            expected: block_id,
            actual: wrong_id,
        })
    );
    assert_eq!(first.put_calls(), 1);
    assert_eq!(second.put_calls(), 1);
}

#[test]
fn val_overlay_store_016_overlay_public_composition_surface_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<OverlayBlockStore>();
    assert_send_sync::<PassiveLayer<MockStore>>();
    assert_send_sync::<Box<dyn OverlayStoreLayer>>();
}

#[test]
fn val_overlay_store_017_memory_filesystem_and_azure_backends_compose_without_duplicate_logic() {
    let cache = MemoryBlockStore::new(4).unwrap();
    let temp_dir = TempDir::new().unwrap();
    let filesystem = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let azure = AzureBlobBlockStore::new("https://example.invalid/archive?sig=test").unwrap();
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::cache(cache.clone())),
        Box::new(PassiveLayer::writable(filesystem)),
        Box::new(PassiveLayer::read_only(azure)),
    ])
    .unwrap();
    let block = sample_leaf_block("heterogeneous");
    let block_id = serialize_block(&block).unwrap().hash;

    assert_eq!(overlay.put(&block).unwrap(), block_id);
    assert_eq!(overlay.get(&block_id).unwrap().unwrap().hash, block_id);
    assert_eq!(cache.get(&block_id).unwrap().unwrap().hash, block_id);
}

struct OverlayHarness;

struct HarnessStore {
    overlay: OverlayBlockStore,
    lower: SharedMemoryBlockStore,
}

impl BlockStore for HarnessStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        self.overlay.put(block)
    }

    fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        self.overlay.get(block_id)
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        self.overlay.iter_block_ids()
    }
}

impl BlockStoreFactory for OverlayHarness {
    type Store = HarnessStore;

    fn fresh_store(&self) -> Self::Store {
        let high = SharedMemoryBlockStore::default();
        let low = SharedMemoryBlockStore::default();
        let overlay = OverlayBlockStore::new(vec![
            Box::new(PassiveLayer::cache(high)),
            Box::new(PassiveLayer::writable(low.clone())),
        ])
        .unwrap();

        HarnessStore {
            overlay,
            lower: low,
        }
    }
}

impl BlockStoreConformanceHarness for OverlayHarness {
    fn inject_raw_bytes(
        &self,
        store: &Self::Store,
        block_id: &BlockHash,
        bytes: &[u8],
    ) -> Result<(), String> {
        store.lower.raw_insert(*block_id, bytes.to_vec());
        Ok(())
    }
}

#[derive(Clone, Default)]
struct MockStore {
    state: Arc<MockStoreState>,
}

struct MockStoreState {
    get_result: Mutex<Result<Option<ValidatedBlock>, BlockStoreError>>,
    put_result: Mutex<Result<BlockHash, BlockStoreError>>,
    iter_result: Mutex<Result<Vec<Result<BlockHash, BlockStoreError>>, BlockStoreError>>,
    get_calls: AtomicUsize,
    put_calls: AtomicUsize,
}

impl Default for MockStoreState {
    fn default() -> Self {
        Self {
            get_result: Mutex::new(Ok(None)),
            put_result: Mutex::new(Err(backend_failure("mock put result not configured"))),
            iter_result: Mutex::new(Ok(vec![])),
            get_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
        }
    }
}

impl MockStore {
    fn for_get(result: Result<Option<ValidatedBlock>, BlockStoreError>) -> Self {
        Self {
            state: Arc::new(MockStoreState {
                get_result: Mutex::new(result),
                ..MockStoreState::default()
            }),
        }
    }

    fn for_put(result: Result<BlockHash, BlockStoreError>) -> Self {
        Self {
            state: Arc::new(MockStoreState {
                put_result: Mutex::new(result),
                ..MockStoreState::default()
            }),
        }
    }

    fn for_iter(result: Result<Vec<Result<BlockHash, BlockStoreError>>, BlockStoreError>) -> Self {
        Self {
            state: Arc::new(MockStoreState {
                iter_result: Mutex::new(result),
                ..MockStoreState::default()
            }),
        }
    }

    fn get_calls(&self) -> usize {
        self.state.get_calls.load(Ordering::SeqCst)
    }

    fn put_calls(&self) -> usize {
        self.state.put_calls.load(Ordering::SeqCst)
    }
}

impl BlockStore for MockStore {
    fn put(&self, _block: &Block) -> Result<BlockHash, BlockStoreError> {
        self.state.put_calls.fetch_add(1, Ordering::SeqCst);
        self.state.put_result.lock().unwrap().clone()
    }

    fn get(&self, _block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        self.state.get_calls.fetch_add(1, Ordering::SeqCst);
        self.state.get_result.lock().unwrap().clone()
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        self.state
            .iter_result
            .lock()
            .unwrap()
            .clone()
            .map(|items| Box::new(items.into_iter()) as lexongraph_block_store::BlockIdIterator<'_>)
    }
}

#[derive(Default)]
struct CustomLayer {
    inner: MockStore,
}

impl BlockStore for CustomLayer {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        self.inner.put(block)
    }

    fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        self.inner.get(block_id)
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        self.inner.iter_block_ids()
    }
}

impl OverlayStoreLayer for CustomLayer {
    fn role(&self) -> OverlayLayerRole {
        OverlayLayerRole::ReadOnly
    }
}

fn validated_block(body: &str) -> ValidatedBlock {
    let block = sample_leaf_block(body);
    let serialized = serialize_block(&block).unwrap();
    ValidatedBlock {
        block,
        hash: serialized.hash,
    }
}

fn backend_failure(message: &str) -> BlockStoreError {
    BlockStoreError::BackendFailure(message.into())
}
