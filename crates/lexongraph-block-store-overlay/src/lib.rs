// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Overlay `BlockStore` implementation for LexonGraph blocks.
//!
//! The overlay presents a single `BlockStore` over an ordered stack of layers:
//! reads walk layers from highest priority to lowest priority, direct writes
//! target only writable layers, and successful lower-layer reads may refill
//! higher-priority cache layers.

use std::collections::HashSet;
use std::fmt;

use lexongraph_block::BlockHash;
use lexongraph_block_store::{BlockIdIterator, BlockStore, BlockStoreError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OverlayLayerRole {
    Cache,
    Writable,
    ReadOnly,
}

impl OverlayLayerRole {
    fn accepts_direct_writes(self) -> bool {
        matches!(self, Self::Writable)
    }

    fn accepts_refill(self) -> bool {
        matches!(self, Self::Cache)
    }
}

impl fmt::Display for OverlayLayerRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cache => f.write_str("cache"),
            Self::Writable => f.write_str("writable"),
            Self::ReadOnly => f.write_str("read-only"),
        }
    }
}

pub trait OverlayStoreLayer: BlockStore + Send + Sync {
    fn role(&self) -> OverlayLayerRole {
        OverlayLayerRole::Writable
    }
}

pub struct PassiveLayer<S> {
    store: S,
    role: OverlayLayerRole,
}

impl<S> PassiveLayer<S> {
    pub fn new(store: S) -> Self {
        Self::writable(store)
    }

    pub fn cache(store: S) -> Self {
        Self {
            store,
            role: OverlayLayerRole::Cache,
        }
    }

    pub fn writable(store: S) -> Self {
        Self {
            store,
            role: OverlayLayerRole::Writable,
        }
    }

    pub fn read_only(store: S) -> Self {
        Self {
            store,
            role: OverlayLayerRole::ReadOnly,
        }
    }

    pub fn into_inner(self) -> S {
        self.store
    }

    pub fn role(&self) -> OverlayLayerRole {
        self.role
    }
}

impl<S: BlockStore> BlockStore for PassiveLayer<S> {
    fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        self.store.put_block_bytes(block_id, block_bytes)
    }

    fn get_block_bytes(&self, block_id: &BlockHash) -> Result<Option<Vec<u8>>, BlockStoreError> {
        self.store.get_block_bytes(block_id)
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        self.store.iter_block_ids()
    }
}

impl<S: BlockStore + Send + Sync> OverlayStoreLayer for PassiveLayer<S> {
    fn role(&self) -> OverlayLayerRole {
        self.role
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OverlayBuildError {
    InsufficientLayers { count: usize },
}

impl fmt::Display for OverlayBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientLayers { count } => {
                write!(
                    f,
                    "overlay block store requires at least 2 layers, got {count}"
                )
            }
        }
    }
}

impl std::error::Error for OverlayBuildError {}

pub struct OverlayBlockStore {
    layers: Vec<Box<dyn OverlayStoreLayer>>,
}

impl OverlayBlockStore {
    pub const MIN_LAYERS: usize = 2;

    pub fn new(layers: Vec<Box<dyn OverlayStoreLayer>>) -> Result<Self, OverlayBuildError> {
        if layers.len() < Self::MIN_LAYERS {
            return Err(OverlayBuildError::InsufficientLayers {
                count: layers.len(),
            });
        }

        Ok(Self { layers })
    }

    pub fn from_layers<I, L>(layers: I) -> Result<Self, OverlayBuildError>
    where
        I: IntoIterator<Item = L>,
        L: OverlayStoreLayer + 'static,
    {
        Self::new(
            layers
                .into_iter()
                .map(|layer| Box::new(layer) as Box<dyn OverlayStoreLayer>)
                .collect(),
        )
    }

    pub fn layer_count(&self) -> usize {
        self.layers.len()
    }

    fn refill_cache_layers(&self, hit_index: usize, block_id: &BlockHash, block_bytes: &[u8]) {
        for layer in &self.layers[..hit_index] {
            if layer.role().accepts_refill() {
                let _ = layer.put_block_bytes(block_id, block_bytes);
            }
        }
    }
}

impl fmt::Debug for OverlayBlockStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OverlayBlockStore")
            .field("layer_count", &self.layers.len())
            .finish()
    }
}

impl BlockStore for OverlayBlockStore {
    fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        let mut attempted_write = false;
        let mut last_error = None;

        for layer in &self.layers {
            if !layer.role().accepts_direct_writes() {
                continue;
            }
            attempted_write = true;
            if let Err(error) = layer.put_block_bytes(block_id, block_bytes) {
                last_error = Some(error);
            }
        }

        if !attempted_write {
            return Err(backend_failure(
                "overlay block store has no layers that accept direct writes".into(),
            ));
        }

        match last_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn get_block_bytes(&self, block_id: &BlockHash) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let mut last_error = None;

        for (index, layer) in self.layers.iter().enumerate() {
            match layer.get_block_bytes(block_id) {
                Ok(Some(bytes)) => {
                    self.refill_cache_layers(index, block_id, &bytes);
                    return Ok(Some(bytes));
                }
                Ok(None) => {}
                Err(error) => last_error = Some(error),
            }
        }

        match last_error {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        Ok(Box::new(OverlayBlockIdIterator::new(&self.layers)?))
    }
}

struct OverlayBlockIdIterator<'a> {
    layers: &'a [Box<dyn OverlayStoreLayer>],
    next_layer: usize,
    current: Option<BlockIdIterator<'a>>,
    seen: HashSet<BlockHash>,
    failed: bool,
}

impl<'a> OverlayBlockIdIterator<'a> {
    fn new(layers: &'a [Box<dyn OverlayStoreLayer>]) -> Result<Self, BlockStoreError> {
        let mut iter = Self {
            layers,
            next_layer: 0,
            current: None,
            seen: HashSet::new(),
            failed: false,
        };
        iter.advance_to_next_layer()?;
        Ok(iter)
    }

    fn advance_to_next_layer(&mut self) -> Result<(), BlockStoreError> {
        if self.current.is_some() || self.next_layer == self.layers.len() {
            return Ok(());
        }

        let iter = self.layers[self.next_layer].iter_block_ids()?;
        self.current = Some(iter);
        self.next_layer += 1;
        Ok(())
    }
}

impl Iterator for OverlayBlockIdIterator<'_> {
    type Item = Result<BlockHash, BlockStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }

        loop {
            if let Some(iter) = self.current.as_mut() {
                match iter.next() {
                    Some(Ok(block_id)) => {
                        if self.seen.insert(block_id) {
                            return Some(Ok(block_id));
                        }
                    }
                    Some(Err(error)) => {
                        self.failed = true;
                        return Some(Err(error));
                    }
                    None => {
                        self.current = None;
                    }
                }
                continue;
            }

            if self.next_layer == self.layers.len() {
                return None;
            }

            if let Err(error) = self.advance_to_next_layer() {
                self.failed = true;
                return Some(Err(error));
            }
        }
    }
}

fn backend_failure(message: String) -> BlockStoreError {
    BlockStoreError::BackendFailure(message)
}
