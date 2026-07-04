// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use lexongraph_block::{
    Block, BlockHash, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_leaf_block,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};

#[derive(Clone, Default)]
pub struct SharedMemoryBlockStore {
    blocks: Arc<Mutex<HashMap<BlockHash, Vec<u8>>>>,
}

impl SharedMemoryBlockStore {
    pub fn raw_insert(&self, hash: BlockHash, bytes: Vec<u8>) {
        self.blocks.lock().unwrap().insert(hash, bytes);
    }
}

#[async_trait(?Send)]
impl BlockStore for SharedMemoryBlockStore {
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
