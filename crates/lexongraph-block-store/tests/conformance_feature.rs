// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![cfg(feature = "conformance")]

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use futures::{executor::block_on, stream};
use lexongraph_block::BlockHash;
use lexongraph_block_store::conformance::{
    BlockStoreConformanceHarness, BlockStoreFactory, run_contract_suite, run_full_suite,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};

#[derive(Default)]
struct MemoryBlockStore {
    blocks: Mutex<HashMap<BlockHash, Vec<u8>>>,
}

impl MemoryBlockStore {
    fn raw_insert(&self, hash: BlockHash, bytes: Vec<u8>) {
        self.blocks.lock().unwrap().insert(hash, bytes);
    }
}

#[async_trait]
impl BlockStore for MemoryBlockStore {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        self.blocks
            .lock()
            .unwrap()
            .insert(*block_id, block_bytes.to_vec());
        Ok(())
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        Ok(self.blocks.lock().unwrap().get(block_id).cloned())
    }

    fn iter_block_ids(&self) -> Result<lexongraph_block_store::BlockIdStream<'_>, BlockStoreError> {
        let block_ids = self
            .blocks
            .lock()
            .unwrap()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        Ok(Box::pin(stream::iter(block_ids.into_iter().map(Ok))))
    }
}

struct MemoryHarness;

#[async_trait(?Send)]
impl BlockStoreFactory for MemoryHarness {
    type Store = MemoryBlockStore;

    async fn fresh_store(&self) -> Self::Store {
        MemoryBlockStore::default()
    }
}

#[async_trait(?Send)]
impl BlockStoreConformanceHarness for MemoryHarness {
    async fn inject_raw_bytes(
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
    block_on(run_contract_suite(&MemoryHarness)).unwrap();
}

#[test]
fn downstream_crates_can_run_the_full_suite() {
    block_on(run_full_suite(&MemoryHarness)).unwrap();
}
