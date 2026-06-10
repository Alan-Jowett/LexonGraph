// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Overlay `BlockStore` implementation for LexonGraph blocks.
//!
//! The overlay presents a single `BlockStore` over an ordered stack of layers:
//! reads and writes walk layers from highest priority to lowest priority, while
//! optional outcome notifications flow from lowest priority to highest priority.

use std::collections::HashSet;
use std::fmt;

use lexongraph_block::{Block, BlockHash, ValidatedBlock};
use lexongraph_block_store::{BlockIdIterator, BlockStore, BlockStoreError};

pub trait OverlayLayerNotifier {
    fn on_get_result(&self, block_id: &BlockHash, outcome: OverlayGetOutcome<'_>);

    fn on_put_result(&self, block: &Block, outcome: OverlayPutOutcome<'_>);
}

#[derive(Clone, Copy, Debug)]
pub enum OverlayGetOutcome<'a> {
    Hit(&'a ValidatedBlock),
    Miss,
    Error(&'a BlockStoreError),
}

#[derive(Clone, Copy, Debug)]
pub enum OverlayPutOutcome<'a> {
    Stored {
        block: &'a Block,
        block_id: BlockHash,
    },
    Error {
        block: &'a Block,
        error: &'a BlockStoreError,
    },
}

pub trait OverlayStoreLayer: BlockStore {
    fn notifier(&self) -> Option<&dyn OverlayLayerNotifier> {
        None
    }
}

pub struct PassiveLayer<S> {
    store: S,
}

impl<S> PassiveLayer<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn into_inner(self) -> S {
        self.store
    }
}

impl<S: BlockStore> BlockStore for PassiveLayer<S> {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        self.store.put(block)
    }

    fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        self.store.get(block_id)
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        self.store.iter_block_ids()
    }
}

impl<S: BlockStore> OverlayStoreLayer for PassiveLayer<S> {}

pub struct ObservedLayer<S, O> {
    store: S,
    observer: O,
}

impl<S, O> ObservedLayer<S, O> {
    pub fn new(store: S, observer: O) -> Self {
        Self { store, observer }
    }

    pub fn into_parts(self) -> (S, O) {
        (self.store, self.observer)
    }
}

impl<S: BlockStore, O> BlockStore for ObservedLayer<S, O> {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        self.store.put(block)
    }

    fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        self.store.get(block_id)
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        self.store.iter_block_ids()
    }
}

impl<S, O> OverlayStoreLayer for ObservedLayer<S, O>
where
    S: BlockStore,
    O: OverlayLayerNotifier,
{
    fn notifier(&self) -> Option<&dyn OverlayLayerNotifier> {
        Some(&self.observer)
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

    fn notify_get(
        &self,
        block_id: &BlockHash,
        result: &Result<Option<ValidatedBlock>, BlockStoreError>,
    ) {
        let outcome = match result {
            Ok(Some(block)) => OverlayGetOutcome::Hit(block),
            Ok(None) => OverlayGetOutcome::Miss,
            Err(error) => OverlayGetOutcome::Error(error),
        };

        for layer in self.layers.iter().rev() {
            if let Some(notifier) = layer.notifier() {
                notifier.on_get_result(block_id, outcome);
            }
        }
    }

    fn notify_put(&self, block: &Block, result: &Result<BlockHash, BlockStoreError>) {
        let outcome = match result {
            Ok(block_id) => OverlayPutOutcome::Stored {
                block,
                block_id: *block_id,
            },
            Err(error) => OverlayPutOutcome::Error { block, error },
        };

        for layer in self.layers.iter().rev() {
            if let Some(notifier) = layer.notifier() {
                notifier.on_put_result(block, outcome);
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
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        let mut last_error = None;

        for layer in &self.layers {
            match layer.put(block) {
                Ok(block_id) => {
                    let result = Ok(block_id);
                    self.notify_put(block, &result);
                    return result;
                }
                Err(error) => last_error = Some(error),
            }
        }

        let result =
            Err(last_error
                .expect("overlay block store construction guarantees at least two layers"));
        self.notify_put(block, &result);
        result
    }

    fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        let mut last_error = None;

        for layer in &self.layers {
            match layer.get(block_id) {
                Ok(Some(block)) => {
                    let result = Ok(Some(block));
                    self.notify_get(block_id, &result);
                    return result;
                }
                Ok(None) => {}
                Err(error) => last_error = Some(error),
            }
        }

        let result = match last_error {
            Some(error) => Err(error),
            None => Ok(None),
        };
        self.notify_get(block_id, &result);
        result
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        Ok(Box::new(OverlayBlockIdIterator::new(&self.layers)))
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
    fn new(layers: &'a [Box<dyn OverlayStoreLayer>]) -> Self {
        Self {
            layers,
            next_layer: 0,
            current: None,
            seen: HashSet::new(),
            failed: false,
        }
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

            match self.layers[self.next_layer].iter_block_ids() {
                Ok(iter) => {
                    self.current = Some(iter);
                    self.next_layer += 1;
                }
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
    }
}
