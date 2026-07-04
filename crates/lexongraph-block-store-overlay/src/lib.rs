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

use async_trait::async_trait;
use futures::{StreamExt, stream};
use lexongraph_block::BlockHash;
use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};

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

#[async_trait(?Send)]
impl<S: BlockStore> BlockStore for PassiveLayer<S> {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        self.store.put_block_bytes(block_id, block_bytes).await
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        self.store.get_block_bytes(block_id).await
    }

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
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

    async fn refill_cache_layers(
        &self,
        hit_index: usize,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) {
        for layer in &self.layers[..hit_index] {
            if layer.role().accepts_refill() {
                let _ = layer.put_block_bytes(block_id, block_bytes).await;
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

#[async_trait(?Send)]
impl BlockStore for OverlayBlockStore {
    async fn put_block_bytes(
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
            if let Err(error) = layer.put_block_bytes(block_id, block_bytes).await {
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

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let mut last_error = None;

        for (index, layer) in self.layers.iter().enumerate() {
            match layer.get_block_bytes(block_id).await {
                Ok(Some(bytes)) => {
                    self.refill_cache_layers(index, block_id, &bytes).await;
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

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
        Ok(Box::pin(overlay_block_id_stream(&self.layers)))
    }
}

struct OverlayEnumerationState<'a> {
    layers: &'a [Box<dyn OverlayStoreLayer>],
    next_layer: usize,
    current: Option<BlockIdStream<'a>>,
    seen: HashSet<BlockHash>,
}

fn overlay_block_id_stream(
    layers: &[Box<dyn OverlayStoreLayer>],
) -> impl futures::Stream<Item = Result<BlockHash, BlockStoreError>> + '_ {
    stream::try_unfold(
        OverlayEnumerationState {
            layers,
            next_layer: 0,
            current: None,
            seen: HashSet::new(),
        },
        |mut state| async move {
            loop {
                if let Some(iter) = state.current.as_mut() {
                    match iter.next().await {
                        Some(Ok(block_id)) => {
                            if state.seen.insert(block_id) {
                                return Ok(Some((block_id, state)));
                            }
                        }
                        Some(Err(error)) => return Err(error),
                        None => state.current = None,
                    }
                    continue;
                }

                if state.next_layer == state.layers.len() {
                    return Ok(None);
                }

                state.current = Some(state.layers[state.next_layer].iter_block_ids()?);
                state.next_layer += 1;
            }
        },
    )
}

fn backend_failure(message: String) -> BlockStoreError {
    BlockStoreError::BackendFailure(message)
}
