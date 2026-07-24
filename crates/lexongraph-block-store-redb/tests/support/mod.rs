// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#[cfg(feature = "inject")]
use async_trait::async_trait;
#[cfg(feature = "inject")]
use lexongraph_block::BlockHash;
use lexongraph_block::{
    Block, Content, EmbeddingSpec, LeafEntry, VERSION_1, ValidatedBlock, build_leaf_block,
    serialize_block,
};
#[cfg(feature = "inject")]
use lexongraph_block_store::conformance::{BlockStoreConformanceHarness, BlockStoreFactory};
#[cfg(feature = "inject")]
use lexongraph_block_store_redb::RedbBlockStore;
#[cfg(feature = "inject")]
use std::sync::Mutex;
#[cfg(feature = "inject")]
use tempfile::TempDir;

#[allow(dead_code)]
pub fn sample_leaf_block(body: &str) -> Block {
    Block::Leaf(
        build_leaf_block(
            VERSION_1,
            EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            vec![LeafEntry {
                embedding: vec![0xaa, 0xbb],
                metadata: vec![],
                content: Content {
                    media_type: "text/plain".into(),
                    body: body.as_bytes().to_vec(),
                },
            }],
            None,
        )
        .unwrap(),
    )
}

#[allow(dead_code)]
pub fn validated_block(body: &str) -> ValidatedBlock {
    let block = sample_leaf_block(body);
    let serialized = serialize_block(&block).unwrap();
    ValidatedBlock {
        block,
        hash: serialized.hash,
    }
}

#[cfg(feature = "inject")]
#[derive(Default)]
pub struct RedbHarness {
    roots: Mutex<Vec<TempDir>>,
}

#[cfg(feature = "inject")]
#[async_trait(?Send)]
impl BlockStoreFactory for RedbHarness {
    type Store = RedbBlockStore;

    async fn fresh_store(&self) -> Self::Store {
        let root = tempfile::tempdir().unwrap();
        let store = RedbBlockStore::new(root.path()).unwrap();
        self.roots.lock().unwrap().push(root);
        store
    }
}

#[cfg(feature = "inject")]
#[async_trait(?Send)]
impl BlockStoreConformanceHarness for RedbHarness {
    async fn inject_raw_bytes(
        &self,
        store: &Self::Store,
        block_id: &BlockHash,
        bytes: &[u8],
    ) -> Result<(), String> {
        store
            .raw_insert(*block_id, bytes.to_vec())
            .map_err(|error| error.to_string())
    }
}
