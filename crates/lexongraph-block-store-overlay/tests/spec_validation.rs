// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use lexongraph_block::{Block, BlockHash, ValidatedBlock, serialize_block};
use lexongraph_block_store::conformance::{
    BlockStoreConformanceHarness, BlockStoreFactory, run_full_suite,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_block_store_overlay::{
    ObservedLayer, OverlayBlockStore, OverlayBuildError, OverlayGetOutcome, OverlayLayerNotifier,
    OverlayPutOutcome, OverlayStoreLayer, PassiveLayer,
};

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
fn val_overlay_store_006_put_uses_the_first_accepting_layer() {
    let block = sample_leaf_block("stored");
    let block_id = serialize_block(&block).unwrap().hash;
    let high = MockStore::for_put(Err(backend_failure("cache layer rejected write")));
    let middle = MockStore::for_put(Ok(block_id));
    let low = MockStore::for_put(Ok(BlockHash::from_bytes([0x99; 32])));
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::new(high.clone())),
        Box::new(PassiveLayer::new(middle.clone())),
        Box::new(PassiveLayer::new(low.clone())),
    ])
    .unwrap();

    let stored = overlay.put(&block).unwrap();

    assert_eq!(stored, block_id);
    assert_eq!(high.put_calls(), 1);
    assert_eq!(middle.put_calls(), 1);
    assert_eq!(low.put_calls(), 0);
}

#[test]
fn val_overlay_store_007_put_returns_the_last_error_when_all_layers_fail() {
    let block = sample_leaf_block("failure");
    let expected_error = backend_failure("lowest write failed");
    let overlay = OverlayBlockStore::new(vec![
        Box::new(PassiveLayer::new(MockStore::for_put(Err(backend_failure(
            "highest write failed",
        ))))),
        Box::new(PassiveLayer::new(MockStore::for_put(Err(
            expected_error.clone()
        )))),
    ])
    .unwrap();

    assert_eq!(overlay.put(&block).unwrap_err(), expected_error);
}

#[test]
fn val_overlay_store_008_parent_conformance_requirements_are_realized_by_tests() {
    run_full_suite(&OverlayHarness).unwrap();
}

#[test]
fn val_overlay_store_009_enumeration_yields_the_deduplicated_union_in_priority_order() {
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
fn val_overlay_store_010_enumeration_failures_are_explicit() {
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
fn val_overlay_store_011_and_012_notifications_only_target_opted_in_layers_and_run_low_to_high_for_get()
 {
    let log = Arc::new(Mutex::new(Vec::new()));
    let block = validated_block("cached");
    let overlay = OverlayBlockStore::new(vec![
        Box::new(ObservedLayer::new(
            MockStore::for_get(Ok(None)),
            RecordingObserver::new("high", Arc::clone(&log)),
        )),
        Box::new(PassiveLayer::new(MockStore::for_get(Ok(None)))),
        Box::new(ObservedLayer::new(
            MockStore::for_get(Ok(Some(block.clone()))),
            RecordingObserver::new("low", Arc::clone(&log)),
        )),
    ])
    .unwrap();

    let loaded = overlay.get(&block.hash).unwrap().unwrap();

    assert_eq!(loaded, block);
    assert_eq!(
        log.lock().unwrap().as_slice(),
        ["low:get:hit", "high:get:hit"]
    );
}

#[test]
fn val_overlay_store_011_and_013_notifications_only_target_opted_in_layers_and_run_low_to_high_for_put()
 {
    let log = Arc::new(Mutex::new(Vec::new()));
    let block = sample_leaf_block("write-through-notification");
    let block_id = serialize_block(&block).unwrap().hash;
    let overlay = OverlayBlockStore::new(vec![
        Box::new(ObservedLayer::new(
            MockStore::for_put(Err(backend_failure("higher write failed"))),
            RecordingObserver::new("high", Arc::clone(&log)),
        )),
        Box::new(PassiveLayer::new(MockStore::for_put(Err(backend_failure(
            "middle write failed",
        ))))),
        Box::new(ObservedLayer::new(
            MockStore::for_put(Ok(block_id)),
            RecordingObserver::new("low", Arc::clone(&log)),
        )),
    ])
    .unwrap();

    assert_eq!(overlay.put(&block).unwrap(), block_id);
    assert_eq!(
        log.lock().unwrap().as_slice(),
        ["low:put:stored", "high:put:stored"]
    );
}

#[test]
fn val_overlay_store_014_public_surface_keeps_notifications_additive_and_optional() {
    let passive_overlay = OverlayBlockStore::from_layers([
        PassiveLayer::new(MockStore::default()),
        PassiveLayer::new(MockStore::default()),
    ])
    .unwrap();

    assert_eq!(passive_overlay.layer_count(), 2);

    let custom_overlay = OverlayBlockStore::new(vec![
        Box::new(CustomLayer::default()) as Box<dyn OverlayStoreLayer>,
        Box::new(PassiveLayer::new(MockStore::default())) as Box<dyn OverlayStoreLayer>,
    ])
    .unwrap();

    assert_eq!(custom_overlay.layer_count(), 2);
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
            Box::new(PassiveLayer::new(high)),
            Box::new(PassiveLayer::new(low.clone())),
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
    iter_calls: AtomicUsize,
}

impl Default for MockStoreState {
    fn default() -> Self {
        Self {
            get_result: Mutex::new(Ok(None)),
            put_result: Mutex::new(Err(backend_failure("mock put result not configured"))),
            iter_result: Mutex::new(Ok(vec![])),
            get_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            iter_calls: AtomicUsize::new(0),
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
        self.state.iter_calls.fetch_add(1, Ordering::SeqCst);
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
    fn notifier(&self) -> Option<&dyn OverlayLayerNotifier> {
        Some(self)
    }
}

impl OverlayLayerNotifier for CustomLayer {
    fn on_get_result(&self, _block_id: &BlockHash, _outcome: OverlayGetOutcome<'_>) {}

    fn on_put_result(&self, _block: &Block, _outcome: OverlayPutOutcome<'_>) {}
}

struct RecordingObserver {
    name: &'static str,
    log: Arc<Mutex<Vec<String>>>,
}

impl RecordingObserver {
    fn new(name: &'static str, log: Arc<Mutex<Vec<String>>>) -> Self {
        Self { name, log }
    }
}

impl OverlayLayerNotifier for RecordingObserver {
    fn on_get_result(&self, _block_id: &BlockHash, outcome: OverlayGetOutcome<'_>) {
        let suffix = match outcome {
            OverlayGetOutcome::Hit(_) => "hit",
            OverlayGetOutcome::Miss => "miss",
            OverlayGetOutcome::Error(_) => "error",
        };
        self.log
            .lock()
            .unwrap()
            .push(format!("{}:get:{suffix}", self.name));
    }

    fn on_put_result(&self, _block: &Block, outcome: OverlayPutOutcome<'_>) {
        let suffix = match outcome {
            OverlayPutOutcome::Stored { .. } => "stored",
            OverlayPutOutcome::Error { .. } => "error",
        };
        self.log
            .lock()
            .unwrap()
            .push(format!("{}:put:{suffix}", self.name));
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
