// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lexongraph_block::{
    Block, BlockError, BlockHash, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_leaf_block,
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

impl BlockStore for SharedMemoryBlockStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        let serialized =
            lexongraph_block::serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
        self.blocks
            .lock()
            .unwrap()
            .insert(serialized.hash, serialized.bytes);
        Ok(serialized.hash)
    }

    fn get(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        let Some(bytes) = self.blocks.lock().unwrap().get(block_id).cloned() else {
            return Ok(None);
        };

        lexongraph_block::deserialize_block(&bytes, block_id)
            .map(Some)
            .map_err(map_get_error)
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        let block_ids = self
            .blocks
            .lock()
            .unwrap()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        Ok(Box::new(block_ids.into_iter().map(Ok)))
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

fn map_get_error(error: BlockError) -> BlockStoreError {
    match error {
        BlockError::HashMismatch { expected, actual } => {
            BlockStoreError::DecodeFailure(BlockError::HashMismatch { expected, actual })
        }
        other => BlockStoreError::DecodeFailure(other),
    }
}
