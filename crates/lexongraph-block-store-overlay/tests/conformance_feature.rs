// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use lexongraph_block::BlockHash;
use lexongraph_block_store::conformance::{
    BlockStoreConformanceHarness, BlockStoreFactory, run_contract_suite, run_full_suite,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_block_store_overlay::{OverlayBlockStore, PassiveLayer};

mod support;

use support::SharedMemoryBlockStore;

struct OverlayHarness;

struct HarnessStore {
    overlay: OverlayBlockStore,
    lower: SharedMemoryBlockStore,
}

impl BlockStore for HarnessStore {
    fn put(&self, block: &lexongraph_block::Block) -> Result<BlockHash, BlockStoreError> {
        self.overlay.put(block)
    }

    fn get(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
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

#[test]
fn downstream_crates_can_run_the_contract_suite() {
    run_contract_suite(&OverlayHarness).unwrap();
}

#[test]
fn downstream_crates_can_run_the_full_suite() {
    run_full_suite(&OverlayHarness).unwrap();
}
