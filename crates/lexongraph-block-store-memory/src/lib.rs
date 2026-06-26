// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Volatile in-memory `BlockStore` implementation for LexonGraph blocks.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use lexongraph_block::BlockHash;
use lexongraph_block_store::{BlockIdIterator, BlockStore, BlockStoreError};

#[derive(Clone)]
pub struct MemoryBlockStore {
    state: Arc<Mutex<State>>,
}

struct State {
    max_resident_blocks: usize,
    next_recency: u64,
    entries: HashMap<BlockHash, ResidentEntry>,
}

struct ResidentEntry {
    bytes: Vec<u8>,
    recency: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemoryBlockStoreBuildError {
    ZeroCapacity,
}

impl fmt::Display for MemoryBlockStoreBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => write!(f, "memory block store capacity must be at least 1"),
        }
    }
}

impl std::error::Error for MemoryBlockStoreBuildError {}

impl fmt::Debug for MemoryBlockStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock().unwrap();
        f.debug_struct("MemoryBlockStore")
            .field("max_resident_blocks", &state.max_resident_blocks)
            .field("resident_len", &state.entries.len())
            .finish()
    }
}

impl MemoryBlockStore {
    pub fn new(max_resident_blocks: usize) -> Result<Self, MemoryBlockStoreBuildError> {
        if max_resident_blocks == 0 {
            return Err(MemoryBlockStoreBuildError::ZeroCapacity);
        }

        Ok(Self {
            state: Arc::new(Mutex::new(State {
                max_resident_blocks,
                next_recency: 0,
                entries: HashMap::new(),
            })),
        })
    }

    pub fn max_resident_blocks(&self) -> usize {
        self.state.lock().unwrap().max_resident_blocks
    }

    #[cfg(feature = "inject")]
    pub fn raw_insert(&self, block_id: BlockHash, bytes: Vec<u8>) {
        let mut state = self.state.lock().unwrap();
        state.insert_or_refresh(block_id, bytes);
    }

    fn insert_block_bytes(&self, block_id: &BlockHash, block_bytes: &[u8]) {
        let mut state = self.state.lock().unwrap();
        state.insert_or_refresh(*block_id, block_bytes.to_vec());
    }
}

impl State {
    fn next_recency(&mut self) -> u64 {
        let recency = self.next_recency;
        self.next_recency = self.next_recency.wrapping_add(1);
        recency
    }

    fn insert_or_refresh(&mut self, block_id: BlockHash, bytes: Vec<u8>) {
        let recency = self.next_recency();
        if let Some(entry) = self.entries.get_mut(&block_id) {
            entry.bytes = bytes;
            entry.recency = recency;
            return;
        }

        if self.entries.len() == self.max_resident_blocks {
            self.evict_lru();
        }

        self.entries
            .insert(block_id, ResidentEntry { bytes, recency });
    }

    fn refresh(&mut self, block_id: &BlockHash) {
        let recency = self.next_recency();
        if let Some(entry) = self.entries.get_mut(block_id) {
            entry.recency = recency;
        }
    }

    fn evict_lru(&mut self) {
        let lru_block_id = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.recency)
            .map(|(block_id, _)| *block_id)
            .expect("memory block store capacity is always positive");
        self.entries.remove(&lru_block_id);
    }
}

impl BlockStore for MemoryBlockStore {
    fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        self.insert_block_bytes(block_id, block_bytes);
        Ok(())
    }

    fn get_block_bytes(&self, block_id: &BlockHash) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let bytes = {
            let state = self.state.lock().unwrap();
            let Some(entry) = state.entries.get(block_id) else {
                return Ok(None);
            };
            entry.bytes.clone()
        };

        self.state.lock().unwrap().refresh(block_id);
        Ok(Some(bytes))
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        let block_ids = self
            .state
            .lock()
            .unwrap()
            .entries
            .keys()
            .copied()
            .collect::<Vec<_>>();
        Ok(Box::new(block_ids.into_iter().map(Ok)))
    }
}
