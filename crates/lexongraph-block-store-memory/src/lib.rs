// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Volatile in-memory `BlockStore` implementation for LexonGraph blocks.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use lexongraph_block::BlockHash;
use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};

#[derive(Clone)]
pub struct MemoryBlockStore {
    state: Arc<Mutex<State>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryBlockStoreCapacity {
    ResidentBlocks(usize),
    ResidentBytes(usize),
}

struct State {
    capacity: Capacity,
    resident_bytes: usize,
    next_recency: u64,
    entries: HashMap<BlockHash, ResidentEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Capacity {
    ResidentBlocks(usize),
    ResidentBytes(usize),
}

struct ResidentEntry {
    bytes: Vec<u8>,
    recency: u64,
}

pub const BYTES_PER_MB: usize = 1_048_576;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemoryBlockStoreBuildError {
    ZeroCapacity,
    CapacityOverflow,
}

impl fmt::Display for MemoryBlockStoreBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => write!(f, "memory block store capacity must be at least 1"),
            Self::CapacityOverflow => {
                write!(
                    f,
                    "memory block store capacity overflowed when converting MB to bytes"
                )
            }
        }
    }
}

impl std::error::Error for MemoryBlockStoreBuildError {}

impl fmt::Debug for MemoryBlockStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock().unwrap();
        let mut debug = f.debug_struct("MemoryBlockStore");
        match state.capacity {
            Capacity::ResidentBlocks(max_resident_blocks) => {
                debug.field("max_resident_blocks", &max_resident_blocks);
            }
            Capacity::ResidentBytes(max_resident_bytes) => {
                debug.field("max_resident_bytes", &max_resident_bytes);
            }
        }
        debug
            .field("resident_len", &state.entries.len())
            .field("resident_bytes", &state.resident_bytes)
            .finish()
    }
}

impl MemoryBlockStore {
    pub fn new(max_resident_blocks: usize) -> Result<Self, MemoryBlockStoreBuildError> {
        Self::new_with_capacity(Capacity::ResidentBlocks(max_resident_blocks))
    }

    pub fn new_cache_mb(max_resident_mb: usize) -> Result<Self, MemoryBlockStoreBuildError> {
        let max_resident_bytes = max_resident_mb
            .checked_mul(BYTES_PER_MB)
            .ok_or(MemoryBlockStoreBuildError::CapacityOverflow)?;
        Self::new_with_capacity(Capacity::ResidentBytes(max_resident_bytes))
    }

    fn new_with_capacity(capacity: Capacity) -> Result<Self, MemoryBlockStoreBuildError> {
        if capacity.limit() == 0 {
            return Err(MemoryBlockStoreBuildError::ZeroCapacity);
        }

        Ok(Self {
            state: Arc::new(Mutex::new(State {
                capacity,
                resident_bytes: 0,
                next_recency: 0,
                entries: HashMap::new(),
            })),
        })
    }

    pub fn max_resident_blocks(&self) -> usize {
        match self.state.lock().unwrap().capacity {
            Capacity::ResidentBlocks(max_resident_blocks) => max_resident_blocks,
            Capacity::ResidentBytes(_) => 0,
        }
    }

    pub fn capacity(&self) -> MemoryBlockStoreCapacity {
        match self.state.lock().unwrap().capacity {
            Capacity::ResidentBlocks(max_resident_blocks) => {
                MemoryBlockStoreCapacity::ResidentBlocks(max_resident_blocks)
            }
            Capacity::ResidentBytes(max_resident_bytes) => {
                MemoryBlockStoreCapacity::ResidentBytes(max_resident_bytes)
            }
        }
    }

    #[cfg(feature = "inject")]
    pub fn raw_insert(&self, block_id: BlockHash, bytes: Vec<u8>) {
        let mut state = self.state.lock().unwrap();
        state
            .insert_or_refresh(block_id, bytes)
            .expect("injected raw insert must fit configured capacity");
    }

    fn insert_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        let mut state = self.state.lock().unwrap();
        state
            .insert_or_refresh(*block_id, block_bytes.to_vec())
            .map_err(backend_failure)
    }
}

impl State {
    fn next_recency(&mut self) -> u64 {
        let recency = self.next_recency;
        self.next_recency = self.next_recency.wrapping_add(1);
        recency
    }

    fn insert_or_refresh(&mut self, block_id: BlockHash, bytes: Vec<u8>) -> Result<(), String> {
        let new_len = bytes.len();
        if let Capacity::ResidentBytes(max_resident_bytes) = self.capacity
            && new_len > max_resident_bytes
        {
            return Err(format!(
                "block payload of {new_len} bytes exceeds cache capacity of {} bytes",
                max_resident_bytes
            ));
        }

        let recency = self.next_recency();
        let replacing_existing = self.entries.contains_key(&block_id);
        let old_len = self
            .entries
            .get(&block_id)
            .map(|entry| entry.bytes.len())
            .unwrap_or(0);
        while !self.capacity.allows_insert(
            self.entries.len(),
            self.resident_bytes.saturating_sub(old_len),
            new_len,
            replacing_existing,
        ) {
            self.evict_lru_excluding(Some(block_id));
        }
        if let Some(entry) = self.entries.get_mut(&block_id) {
            entry.bytes = bytes;
            entry.recency = recency;
        } else {
            self.entries
                .insert(block_id, ResidentEntry { bytes, recency });
        }

        self.resident_bytes = self
            .resident_bytes
            .saturating_sub(old_len)
            .saturating_add(new_len);
        Ok(())
    }

    fn refresh(&mut self, block_id: &BlockHash) {
        let recency = self.next_recency();
        if let Some(entry) = self.entries.get_mut(block_id) {
            entry.recency = recency;
        }
    }

    fn evict_lru_excluding(&mut self, excluded_block_id: Option<BlockHash>) {
        let lru_block_id = self
            .entries
            .iter()
            .filter(|(block_id, _)| Some(**block_id) != excluded_block_id)
            .min_by_key(|(_, entry)| entry.recency)
            .map(|(block_id, _)| *block_id)
            .expect("memory block store capacity is always positive");
        let removed = self.entries.remove(&lru_block_id).unwrap();
        self.resident_bytes = self.resident_bytes.saturating_sub(removed.bytes.len());
    }
}

impl Capacity {
    fn limit(self) -> usize {
        match self {
            Self::ResidentBlocks(limit) | Self::ResidentBytes(limit) => limit,
        }
    }

    fn allows_insert(
        self,
        resident_len: usize,
        resident_bytes: usize,
        new_len: usize,
        replacing_existing: bool,
    ) -> bool {
        match self {
            Self::ResidentBlocks(max_resident_blocks) => {
                replacing_existing || resident_len < max_resident_blocks
            }
            Self::ResidentBytes(max_resident_bytes) => {
                resident_bytes.saturating_add(new_len) <= max_resident_bytes
            }
        }
    }
}

#[async_trait]
impl BlockStore for MemoryBlockStore {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        self.insert_block_bytes(block_id, block_bytes)
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
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

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
        let block_ids = self
            .state
            .lock()
            .unwrap()
            .entries
            .keys()
            .copied()
            .collect::<Vec<_>>();
        Ok(Box::pin(stream::iter(block_ids.into_iter().map(Ok))))
    }
}

fn backend_failure(message: String) -> BlockStoreError {
    BlockStoreError::BackendFailure(message)
}
