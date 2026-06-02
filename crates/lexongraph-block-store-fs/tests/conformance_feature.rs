// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::path::PathBuf;
use std::sync::Mutex;

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

impl BlockStore for HarnessStore {
    fn put(&self, block: &lexongraph_block::Block) -> Result<BlockHash, BlockStoreError> {
        self.inner.put(block)
    }

    fn get(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        self.inner.get(block_id)
    }
}

impl BlockStoreFactory for FilesystemHarness {
    type Store = HarnessStore;

    fn fresh_store(&self) -> Self::Store {
        let root = tempfile::tempdir().unwrap();
        let store = HarnessStore {
            inner: FilesystemBlockStore::new(root.path()).unwrap(),
            root: root.path().canonicalize().unwrap(),
        };
        self.roots.lock().unwrap().push(root);
        store
    }
}

impl BlockStoreConformanceHarness for FilesystemHarness {
    fn inject_raw_bytes(
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
    run_contract_suite(&FilesystemHarness::default()).unwrap();
}

#[test]
fn downstream_crates_can_run_the_full_suite() {
    run_full_suite(&FilesystemHarness::default()).unwrap();
}

fn expected_block_path(root: &std::path::Path, block_id: &BlockHash) -> std::path::PathBuf {
    let hex = block_id.to_string();
    root.join(&hex[..2])
        .join(&hex[2..4])
        .join(format!("{hex}.cbor"))
}
