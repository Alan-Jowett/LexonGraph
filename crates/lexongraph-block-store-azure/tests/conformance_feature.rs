// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
mod support;

use std::sync::Mutex;

use lexongraph_block::BlockHash;
use lexongraph_block_store::BlockStore;
use lexongraph_block_store::conformance::{
    BlockStoreConformanceHarness, BlockStoreFactory, run_contract_suite, run_full_suite,
};
use lexongraph_block_store_azure::AzureBlobBlockStore;

use support::MockAzureServer;

#[derive(Default)]
struct AzureHarness {
    servers: Mutex<Vec<MockAzureServer>>,
}

#[derive(Clone, Debug)]
struct HarnessStore {
    inner: AzureBlobBlockStore,
    server: MockAzureServer,
}

impl BlockStore for HarnessStore {
    fn put(
        &self,
        block: &lexongraph_block::Block,
    ) -> Result<BlockHash, lexongraph_block_store::BlockStoreError> {
        self.inner.put(block)
    }

    fn get(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, lexongraph_block_store::BlockStoreError>
    {
        self.inner.get(block_id)
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, lexongraph_block_store::BlockStoreError>
    {
        self.inner.iter_block_ids()
    }
}

impl BlockStoreFactory for AzureHarness {
    type Store = HarnessStore;

    fn fresh_store(&self) -> Self::Store {
        let server = MockAzureServer::start();
        let store = HarnessStore {
            inner: server.store(),
            server: server.clone(),
        };
        self.servers.lock().unwrap().push(server);
        store
    }
}

impl BlockStoreConformanceHarness for AzureHarness {
    fn inject_raw_bytes(
        &self,
        store: &Self::Store,
        block_id: &BlockHash,
        bytes: &[u8],
    ) -> Result<(), String> {
        store
            .server
            .insert_blob(store.server.blob_name(block_id), bytes.to_vec());
        Ok(())
    }
}

#[test]
fn downstream_crates_can_run_the_contract_suite() {
    run_contract_suite(&AzureHarness::default()).unwrap();
}

#[test]
fn downstream_crates_can_run_the_full_suite() {
    run_full_suite(&AzureHarness::default()).unwrap();
}
