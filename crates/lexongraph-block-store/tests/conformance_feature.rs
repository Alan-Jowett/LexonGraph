// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![cfg(feature = "conformance")]

use std::cell::RefCell;
use std::collections::HashMap;

use lexongraph_block::BlockHash;
use lexongraph_block_store::conformance::{
    BlockStoreConformanceHarness, BlockStoreFactory, run_contract_suite, run_full_suite,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};

#[derive(Default)]
struct MemoryBlockStore {
    blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
}

impl MemoryBlockStore {
    fn raw_insert(&self, hash: BlockHash, bytes: Vec<u8>) {
        self.blocks.borrow_mut().insert(hash, bytes);
    }
}

impl BlockStore for MemoryBlockStore {
    fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        self.blocks.borrow_mut().insert(*block_id, block_bytes.to_vec());
        Ok(())
    }

    fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        Ok(self.blocks.borrow().get(block_id).cloned())
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        let block_ids = self.blocks.borrow().keys().copied().collect::<Vec<_>>();
        Ok(Box::new(block_ids.into_iter().map(Ok)))
    }
}

struct MemoryHarness;

impl BlockStoreFactory for MemoryHarness {
    type Store = MemoryBlockStore;

    fn fresh_store(&self) -> Self::Store {
        MemoryBlockStore::default()
    }
}

impl BlockStoreConformanceHarness for MemoryHarness {
    fn inject_raw_bytes(
        &self,
        store: &Self::Store,
        block_id: &BlockHash,
        bytes: &[u8],
    ) -> Result<(), String> {
        store.raw_insert(*block_id, bytes.to_vec());
        Ok(())
    }
}

#[test]
fn downstream_crates_can_run_the_contract_suite() {
    run_contract_suite(&MemoryHarness).unwrap();
}

#[test]
fn downstream_crates_can_run_the_full_suite() {
    run_full_suite(&MemoryHarness).unwrap();
}
