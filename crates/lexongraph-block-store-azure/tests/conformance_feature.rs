// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
mod support;

use std::sync::Mutex;

use async_trait::async_trait;
use futures::executor::block_on;
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

#[async_trait(?Send)]
impl BlockStore for HarnessStore {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), lexongraph_block_store::BlockStoreError> {
        self.inner.put_block_bytes(block_id, block_bytes).await
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, lexongraph_block_store::BlockStoreError> {
        self.inner.get_block_bytes(block_id).await
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdStream<'_>, lexongraph_block_store::BlockStoreError>
    {
        self.inner.iter_block_ids()
    }
}

#[async_trait(?Send)]
impl BlockStoreFactory for AzureHarness {
    type Store = HarnessStore;

    async fn fresh_store(&self) -> Self::Store {
        let server = MockAzureServer::start();
        let store = HarnessStore {
            inner: server.store(),
            server: server.clone(),
        };
        self.servers.lock().unwrap().push(server);
        store
    }
}

#[async_trait(?Send)]
impl BlockStoreConformanceHarness for AzureHarness {
    async fn inject_raw_bytes(
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
    block_on(run_contract_suite(&AzureHarness::default())).unwrap();
}

#[test]
fn downstream_crates_can_run_the_full_suite() {
    block_on(run_full_suite(&AzureHarness::default())).unwrap();
}
