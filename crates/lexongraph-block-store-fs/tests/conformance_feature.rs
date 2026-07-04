// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;
use futures::executor::block_on;
use lexongraph_block::BlockHash;
use lexongraph_block_store::conformance::{
    BlockStoreConformanceHarness, BlockStoreFactory, run_contract_suite, run_full_suite,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_block_store_fs::FilesystemBlockStore;
use tempfile::TempDir;

#[derive(Default)]
struct FilesystemHarness {
    roots: Mutex<Vec<TempDir>>,
}

#[derive(Clone, Debug)]
struct HarnessStore {
    inner: FilesystemBlockStore,
    root: PathBuf,
}

#[async_trait(?Send)]
impl BlockStore for HarnessStore {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        self.inner.put_block_bytes(block_id, block_bytes).await
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        self.inner.get_block_bytes(block_id).await
    }

    fn iter_block_ids(&self) -> Result<lexongraph_block_store::BlockIdStream<'_>, BlockStoreError> {
        self.inner.iter_block_ids()
    }
}

#[async_trait(?Send)]
impl BlockStoreFactory for FilesystemHarness {
    type Store = HarnessStore;

    async fn fresh_store(&self) -> Self::Store {
        let root = tempfile::tempdir().unwrap();
        let store = HarnessStore {
            inner: FilesystemBlockStore::new(root.path()).unwrap(),
            root: root.path().canonicalize().unwrap(),
        };
        self.roots.lock().unwrap().push(root);
        store
    }
}

#[async_trait(?Send)]
impl BlockStoreConformanceHarness for FilesystemHarness {
    async fn inject_raw_bytes(
        &self,
        store: &Self::Store,
        block_id: &BlockHash,
        bytes: &[u8],
    ) -> Result<(), String> {
        let path = expected_block_path(&store.root, block_id);
        std::fs::create_dir_all(path.parent().unwrap()).map_err(|error| error.to_string())?;
        std::fs::write(path, bytes).map_err(|error| error.to_string())
    }
}

#[test]
fn downstream_crates_can_run_the_contract_suite() {
    block_on(run_contract_suite(&FilesystemHarness::default())).unwrap();
}

#[test]
fn downstream_crates_can_run_the_full_suite() {
    block_on(run_full_suite(&FilesystemHarness::default())).unwrap();
}

fn expected_block_path(root: &std::path::Path, block_id: &BlockHash) -> std::path::PathBuf {
    let hex = block_id.to_string();
    root.join(&hex[..2])
        .join(&hex[2..4])
        .join(format!("{hex}.cbor"))
}
