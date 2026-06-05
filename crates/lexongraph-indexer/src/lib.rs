// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Protocol-conforming LexonGraph indexing orchestration.
//!
//! ```compile_fail
//! #[cfg(feature = "conformance")]
//! compile_error!("the conformance module is intentionally enabled in this doctest configuration");
//!
//! use lexongraph_indexer::conformance;
//!
//! let _ = std::any::type_name::<conformance::ConformanceError>();
//! ```

use std::collections::HashMap;
use std::fmt;

use half::f16;
pub use lexongraph_block::{
    BlockHash, BranchBlock, Content, EmbeddingSpec, Metadata, SerializedBlock,
};

use lexongraph_block::{
    Block, BlockError, BranchEntry, LeafEntry, VERSION_1, build_branch_block, build_leaf_block,
    serialize_block,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_dcbc::{DcbcInput, run_dcbc};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};

#[derive(Clone, Debug, PartialEq)]
pub struct IndexItem<R> {
    pub metadata: Metadata,
    pub content_ref: R,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexingResult {
    pub root_id: BlockHash,
    pub block_ids: Vec<BlockHash>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConstructedBlocks {
    pub block_ids: Vec<BlockHash>,
    pub blocks: Vec<SerializedBlock>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedChild {
    pub embedding: Vec<u8>,
    pub child: BlockHash,
    pub level: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexerError {
    EmptyInput,
    InvalidStagedInput(String),
    ContentResolution(String),
    UnusableContent(String),
    EmbeddingFailure(String),
    CanonicalEmbeddingFailure(String),
    NodePackingFailure(String),
    IntermediateNodeTooLarge {
        min_serialized_bytes: usize,
        size_target: usize,
    },
    InvalidInputBlock(BlockError),
    BlockConstruction(BlockError),
    Storage(BlockStoreError),
}

impl fmt::Display for IndexerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyInput => write!(f, "indexing requires at least one item"),
            Self::InvalidStagedInput(message) => write!(f, "invalid staged input: {message}"),
            Self::ContentResolution(message) => {
                write!(f, "content resolution failed: {message}")
            }
            Self::UnusableContent(message) => write!(f, "resolved content is unusable: {message}"),
            Self::EmbeddingFailure(message) => write!(f, "embedding generation failed: {message}"),
            Self::CanonicalEmbeddingFailure(message) => {
                write!(f, "canonical embedding selection failed: {message}")
            }
            Self::NodePackingFailure(message) => write!(f, "node packing failed: {message}"),
            Self::IntermediateNodeTooLarge {
                min_serialized_bytes,
                size_target,
            } => write!(
                f,
                "smallest intermediate node needs {min_serialized_bytes} bytes, exceeding block size target {size_target}"
            ),
            Self::InvalidInputBlock(error) => write!(f, "invalid staged input block: {error}"),
            Self::BlockConstruction(error) => write!(f, "block construction failed: {error}"),
            Self::Storage(error) => write!(f, "block storage failed: {error}"),
        }
    }
}

impl std::error::Error for IndexerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidInputBlock(error) => Some(error),
            Self::BlockConstruction(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::EmptyInput
            | Self::InvalidStagedInput(_)
            | Self::ContentResolution(_)
            | Self::UnusableContent(_)
            | Self::EmbeddingFailure(_)
            | Self::CanonicalEmbeddingFailure(_)
            | Self::NodePackingFailure(_)
            | Self::IntermediateNodeTooLarge { .. } => None,
        }
    }
}

pub trait ContentResolver<R> {
    type Error: std::error::Error;

    fn resolve(&self, content_ref: &R) -> Result<Content, Self::Error>;
}

pub trait CanonicalEmbeddingPolicy {
    type Error: std::error::Error;

    fn canonical_embedding(&self, block: &BranchBlock) -> Result<Vec<u8>, Self::Error>;
}

pub trait NodePackingPolicy {
    type Error: std::error::Error;

    fn pack(
        &self,
        children: &[IndexedChild],
        spec: &EmbeddingSpec,
        block_size_target: usize,
    ) -> Result<Vec<Vec<usize>>, Self::Error>;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ArithmeticMeanCanonicalEmbeddingPolicy;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArithmeticMeanCanonicalEmbeddingError(String);

impl fmt::Display for ArithmeticMeanCanonicalEmbeddingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ArithmeticMeanCanonicalEmbeddingError {}

impl CanonicalEmbeddingPolicy for ArithmeticMeanCanonicalEmbeddingPolicy {
    type Error = ArithmeticMeanCanonicalEmbeddingError;

    fn canonical_embedding(&self, block: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
        arithmetic_mean_canonical_embedding(block).map_err(ArithmeticMeanCanonicalEmbeddingError)
    }
}

const DCBC_DEFAULT_ITERATION_COUNT: usize = 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DcbcNodePackingPolicy;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DcbcNodePackingError(String);

impl fmt::Display for DcbcNodePackingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for DcbcNodePackingError {}

impl NodePackingPolicy for DcbcNodePackingPolicy {
    type Error = DcbcNodePackingError;

    fn pack(
        &self,
        children: &[IndexedChild],
        spec: &EmbeddingSpec,
        block_size_target: usize,
    ) -> Result<Vec<Vec<usize>>, Self::Error> {
        if children.len() < 2 {
            return Err(DcbcNodePackingError(
                "default DCBC node packing requires at least two children".into(),
            ));
        }
        validate_dcbc_embedding_encoding(spec).map_err(DcbcNodePackingError)?;
        if children.len() == 2 {
            return Ok(vec![vec![0, 1]]);
        }

        let max_group_size = max_children_per_branch(spec, block_size_target, children.len())
            .map_err(DcbcNodePackingError)?;
        if children.len() <= max_group_size {
            return Ok(vec![(0..children.len()).collect()]);
        }

        let min_cluster_count = children.len().div_ceil(max_group_size.max(1));
        let max_cluster_count = children.len() / 2;
        if min_cluster_count > max_cluster_count {
            return Ok(vec![(0..children.len()).collect()]);
        }

        let vectors = decode_embeddings_as_f64(children, spec).map_err(DcbcNodePackingError)?;
        let cluster_count = min_cluster_count;
        let min_cluster_size = children.len() / cluster_count;
        let max_cluster_size = children.len().div_ceil(cluster_count);
        let input = DcbcInput {
            x: vectors,
            cluster_count,
            min_cluster_size,
            max_cluster_size,
            iteration_count: DCBC_DEFAULT_ITERATION_COUNT,
        };
        let result = run_dcbc(&input)
            .map_err(|error| DcbcNodePackingError(format!("dcbc clustering failed: {error}")))?;
        assignment_to_groups(&result.assignment, cluster_count).map_err(DcbcNodePackingError)
    }
}

#[derive(Clone, Debug)]
pub struct Indexer<
    CR,
    EP,
    CEP = ArithmeticMeanCanonicalEmbeddingPolicy,
    NPP = DcbcNodePackingPolicy,
> {
    resolver: CR,
    embedding_provider: EP,
    canonical_embedding_policy: CEP,
    node_packing_policy: NPP,
}

impl<CR, EP> Indexer<CR, EP, ArithmeticMeanCanonicalEmbeddingPolicy, DcbcNodePackingPolicy> {
    pub fn with_defaults(resolver: CR, embedding_provider: EP) -> Self {
        Self::with_node_packing_policy(
            resolver,
            embedding_provider,
            ArithmeticMeanCanonicalEmbeddingPolicy,
            DcbcNodePackingPolicy,
        )
    }
}

impl<CR, EP, CEP> Indexer<CR, EP, CEP, DcbcNodePackingPolicy> {
    pub fn new(resolver: CR, embedding_provider: EP, canonical_embedding_policy: CEP) -> Self {
        Self::with_node_packing_policy(
            resolver,
            embedding_provider,
            canonical_embedding_policy,
            DcbcNodePackingPolicy,
        )
    }

    pub fn with_canonical_embedding_policy(
        resolver: CR,
        embedding_provider: EP,
        canonical_embedding_policy: CEP,
    ) -> Self {
        Self::new(resolver, embedding_provider, canonical_embedding_policy)
    }
}

impl<CR, EP, CEP, NPP> Indexer<CR, EP, CEP, NPP> {
    pub fn with_node_packing_policy(
        resolver: CR,
        embedding_provider: EP,
        canonical_embedding_policy: CEP,
        node_packing_policy: NPP,
    ) -> Self {
        Self {
            resolver,
            embedding_provider,
            canonical_embedding_policy,
            node_packing_policy,
        }
    }
}

impl<CR, EP, CEP, NPP> Indexer<CR, EP, CEP, NPP>
where
    EP: EmbeddingProvider,
    CEP: CanonicalEmbeddingPolicy,
    NPP: NodePackingPolicy,
{
    pub async fn build_leaf_blocks<R>(
        &self,
        items: &[IndexItem<R>],
        embedding_spec: EmbeddingSpec,
    ) -> Result<ConstructedBlocks, IndexerError>
    where
        CR: ContentResolver<R>,
    {
        let layer = self.build_leaf_layer(items, &embedding_spec).await?;
        Ok(ConstructedBlocks::from_constructed_blocks(layer.blocks))
    }

    pub fn build_parent_blocks(
        &self,
        child_blocks: &[SerializedBlock],
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Result<ConstructedBlocks, IndexerError> {
        let layer =
            self.build_parent_layer_from_blocks(child_blocks, &embedding_spec, block_size_target)?;
        Ok(ConstructedBlocks::from_constructed_blocks(layer.blocks))
    }

    pub async fn index<R>(
        &self,
        items: &[IndexItem<R>],
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
        store: &dyn BlockStore,
    ) -> Result<IndexingResult, IndexerError>
    where
        CR: ContentResolver<R>,
    {
        let (mut persisted_block_ids, current_children) = self
            .build_leaf_layer_and_persist(items, &embedding_spec, store)
            .await?;
        let mut current_layer = IndexedLayer {
            blocks: Vec::new(),
            children: current_children,
        };

        while current_layer.children.len() > 1 {
            current_layer = self.build_parent_layer_from_children(
                &current_layer.children,
                &embedding_spec,
                block_size_target,
            )?;
            persisted_block_ids
                .extend(self.persist_constructed_blocks(&current_layer.blocks, store)?);
        }

        persisted_block_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        persisted_block_ids.dedup_by(|left, right| left.as_bytes() == right.as_bytes());

        Ok(IndexingResult {
            root_id: current_layer.children[0].child,
            block_ids: persisted_block_ids,
        })
    }
}

#[derive(Clone, Debug)]
struct IndexedLayer {
    blocks: Vec<ConstructedBlock>,
    children: Vec<IndexedChild>,
}

impl ConstructedBlocks {
    fn from_constructed_blocks(blocks: Vec<ConstructedBlock>) -> Self {
        let block_ids = blocks.iter().map(|block| block.serialized.hash).collect();
        let blocks = blocks.into_iter().map(|block| block.serialized).collect();
        Self { block_ids, blocks }
    }
}

#[derive(Clone, Debug)]
struct ConstructedBlock {
    block: Block,
    serialized: SerializedBlock,
}

impl<CR, EP, CEP, NPP> Indexer<CR, EP, CEP, NPP>
where
    EP: EmbeddingProvider,
    CEP: CanonicalEmbeddingPolicy,
    NPP: NodePackingPolicy,
{
    async fn resolve_and_embed_items<R>(
        &self,
        items: &[IndexItem<R>],
        embedding_spec: &EmbeddingSpec,
    ) -> Result<(Vec<Content>, Vec<Vec<u8>>), IndexerError>
    where
        CR: ContentResolver<R>,
    {
        if items.is_empty() {
            return Err(IndexerError::EmptyInput);
        }

        let mut contents = Vec::with_capacity(items.len());
        let mut inputs = Vec::with_capacity(items.len());
        for item in items {
            let content = self
                .resolver
                .resolve(&item.content_ref)
                .map_err(|error| IndexerError::ContentResolution(error.to_string()))?;
            if content.media_type.is_empty() {
                return Err(IndexerError::UnusableContent(
                    "resolved content must include a media type".into(),
                ));
            }

            inputs.push(EmbeddingInput {
                media_type: content.media_type.clone(),
                body: content.body.clone(),
            });
            contents.push(content);
        }

        let embeddings = self
            .embedding_provider
            .embed_batch(&inputs, embedding_spec)
            .await
            .map_err(|error| IndexerError::EmbeddingFailure(error.to_string()))?;
        if embeddings.len() != items.len() {
            return Err(IndexerError::EmbeddingFailure(format!(
                "embedding provider returned {} embeddings for {} inputs",
                embeddings.len(),
                items.len()
            )));
        }
        for embedding in &embeddings {
            validate_embedding_bytes(embedding, embedding_spec, "item")
                .map_err(IndexerError::EmbeddingFailure)?;
        }

        Ok((contents, embeddings))
    }

    async fn build_leaf_layer<R>(
        &self,
        items: &[IndexItem<R>],
        embedding_spec: &EmbeddingSpec,
    ) -> Result<IndexedLayer, IndexerError>
    where
        CR: ContentResolver<R>,
    {
        if items.is_empty() {
            return Err(IndexerError::EmptyInput);
        }

        let (contents, embeddings) = self.resolve_and_embed_items(items, embedding_spec).await?;
        let mut blocks = Vec::with_capacity(items.len());
        let mut current_layer = Vec::with_capacity(items.len());

        for ((item, content), embedding) in items.iter().zip(contents).zip(embeddings) {
            let leaf = build_leaf_block(
                VERSION_1,
                embedding_spec.clone(),
                vec![LeafEntry {
                    embedding: embedding.clone(),
                    metadata: item.metadata.clone(),
                    content,
                }],
                None,
            )
            .map_err(IndexerError::BlockConstruction)?;
            let leaf_block = Block::Leaf(leaf);
            let serialized =
                serialize_block(&leaf_block).map_err(IndexerError::BlockConstruction)?;

            current_layer.push(IndexedChild {
                embedding,
                child: serialized.hash,
                level: 0,
            });
            blocks.push(ConstructedBlock {
                block: leaf_block,
                serialized,
            });
        }

        Ok(IndexedLayer {
            blocks,
            children: normalize_current_layer(current_layer),
        })
    }

    async fn build_leaf_layer_and_persist<R>(
        &self,
        items: &[IndexItem<R>],
        embedding_spec: &EmbeddingSpec,
        store: &dyn BlockStore,
    ) -> Result<(Vec<BlockHash>, Vec<IndexedChild>), IndexerError>
    where
        CR: ContentResolver<R>,
    {
        if items.is_empty() {
            return Err(IndexerError::EmptyInput);
        }

        let (contents, embeddings) = self.resolve_and_embed_items(items, embedding_spec).await?;
        let mut persisted_block_ids = Vec::with_capacity(items.len());
        let mut current_layer = Vec::with_capacity(items.len());

        for ((item, content), embedding) in items.iter().zip(contents).zip(embeddings) {
            let leaf = build_leaf_block(
                VERSION_1,
                embedding_spec.clone(),
                vec![LeafEntry {
                    embedding: embedding.clone(),
                    metadata: item.metadata.clone(),
                    content,
                }],
                None,
            )
            .map_err(IndexerError::BlockConstruction)?;
            let leaf_block = Block::Leaf(leaf);
            let block_id = store.put(&leaf_block).map_err(IndexerError::Storage)?;

            persisted_block_ids.push(block_id);
            current_layer.push(IndexedChild {
                embedding,
                child: block_id,
                level: 0,
            });
        }

        Ok((persisted_block_ids, normalize_current_layer(current_layer)))
    }

    fn build_parent_layer_from_blocks(
        &self,
        child_blocks: &[SerializedBlock],
        embedding_spec: &EmbeddingSpec,
        block_size_target: usize,
    ) -> Result<IndexedLayer, IndexerError> {
        if child_blocks.is_empty() {
            return Err(IndexerError::InvalidStagedInput(
                "parent construction requires at least one child block".into(),
            ));
        }

        let current_layer = child_blocks
            .iter()
            .map(|block| self.indexed_child_from_serialized_block(block, embedding_spec))
            .collect::<Result<Vec<_>, _>>()?;
        self.build_parent_layer_from_children(&current_layer, embedding_spec, block_size_target)
    }

    fn indexed_child_from_serialized_block(
        &self,
        serialized: &SerializedBlock,
        embedding_spec: &EmbeddingSpec,
    ) -> Result<IndexedChild, IndexerError> {
        let validated = lexongraph_block::deserialize_block(&serialized.bytes, &serialized.hash)
            .map_err(IndexerError::InvalidInputBlock)?;
        let child = validated.hash;
        match validated.block {
            Block::Leaf(block) => {
                ensure_matching_embedding_spec(
                    "leaf child block",
                    &block.embedding_spec,
                    embedding_spec,
                )?;
                let Some(entry) = block.entries.into_iter().next() else {
                    return Err(IndexerError::InvalidInputBlock(BlockError::NonConforming(
                        "leaf blocks must contain exactly one entry",
                    )));
                };
                validate_embedding_bytes(&entry.embedding, embedding_spec, "child")
                    .map_err(IndexerError::InvalidStagedInput)?;
                Ok(IndexedChild {
                    embedding: entry.embedding,
                    child,
                    level: block.level,
                })
            }
            Block::Branch(block) => {
                ensure_matching_embedding_spec(
                    "branch child block",
                    &block.embedding_spec,
                    embedding_spec,
                )?;
                let embedding = self
                    .canonical_embedding_policy
                    .canonical_embedding(&block)
                    .map_err(|error| IndexerError::CanonicalEmbeddingFailure(error.to_string()))?;
                validate_embedding_bytes(&embedding, embedding_spec, "canonical")
                    .map_err(IndexerError::CanonicalEmbeddingFailure)?;
                Ok(IndexedChild {
                    embedding,
                    child,
                    level: block.level,
                })
            }
        }
    }

    fn build_parent_layer_from_children(
        &self,
        current_layer: &[IndexedChild],
        embedding_spec: &EmbeddingSpec,
        block_size_target: usize,
    ) -> Result<IndexedLayer, IndexerError> {
        let current_layer = normalize_current_layer(current_layer.to_vec());
        if current_layer.len() < 2 {
            return Err(IndexerError::InvalidStagedInput(
                "parent construction requires at least two unique child blocks".into(),
            ));
        }
        let child_level = shared_child_level(&current_layer)?;
        let parent_level = child_level.checked_add(1).ok_or_else(|| {
            IndexerError::InvalidStagedInput(format!(
                "child level {child_level} is too large to derive a parent level"
            ))
        })?;

        let groups = self
            .node_packing_policy
            .pack(&current_layer, embedding_spec, block_size_target)
            .map_err(|error| IndexerError::NodePackingFailure(error.to_string()))?;
        validate_group_partition(&groups, &current_layer)
            .map_err(IndexerError::NodePackingFailure)?;

        let mut blocks = Vec::with_capacity(groups.len());
        let mut next_layer = Vec::with_capacity(groups.len());
        for group in groups {
            let entries = normalize_branch_entries(
                group
                    .into_iter()
                    .map(|index| BranchEntry {
                        embedding: current_layer[index].embedding.clone(),
                        child: current_layer[index].child,
                    })
                    .collect(),
            );
            if entries.len() < 2 {
                return Err(IndexerError::NodePackingFailure(
                    "node packing candidate normalized to fewer than two unique children".into(),
                ));
            }

            let branch = build_branch_block(
                VERSION_1,
                parent_level,
                embedding_spec.clone(),
                entries,
                None,
            )
            .map_err(IndexerError::BlockConstruction)?;
            let branch_block = Block::Branch(branch.clone());
            let serialized =
                serialize_block(&branch_block).map_err(IndexerError::BlockConstruction)?;
            if serialized.bytes.len() > block_size_target {
                if branch.entries.len() == 2 {
                    return Err(IndexerError::IntermediateNodeTooLarge {
                        min_serialized_bytes: serialized.bytes.len(),
                        size_target: block_size_target,
                    });
                }
                return Err(IndexerError::NodePackingFailure(format!(
                    "candidate branch block serialized to {} bytes, exceeding block size target {}",
                    serialized.bytes.len(),
                    block_size_target
                )));
            }

            let canonical_embedding = self
                .canonical_embedding_policy
                .canonical_embedding(&branch)
                .map_err(|error| IndexerError::CanonicalEmbeddingFailure(error.to_string()))?;
            validate_embedding_bytes(&canonical_embedding, embedding_spec, "canonical")
                .map_err(IndexerError::CanonicalEmbeddingFailure)?;

            next_layer.push(IndexedChild {
                embedding: canonical_embedding,
                child: serialized.hash,
                level: parent_level,
            });
            blocks.push(ConstructedBlock {
                block: branch_block,
                serialized,
            });
        }

        Ok(IndexedLayer {
            blocks,
            children: normalize_current_layer(next_layer),
        })
    }

    fn persist_constructed_blocks(
        &self,
        blocks: &[ConstructedBlock],
        store: &dyn BlockStore,
    ) -> Result<Vec<BlockHash>, IndexerError> {
        let mut persisted_block_ids = Vec::with_capacity(blocks.len());
        for block in blocks {
            let block_id = store.put(&block.block).map_err(IndexerError::Storage)?;
            if block_id != block.serialized.hash {
                return Err(IndexerError::Storage(BlockStoreError::IntegrityMismatch {
                    expected: block.serialized.hash,
                    actual: block_id,
                }));
            }
            persisted_block_ids.push(block_id);
        }
        Ok(persisted_block_ids)
    }
}

fn normalize_current_layer(mut layer: Vec<IndexedChild>) -> Vec<IndexedChild> {
    layer.sort_by(compare_indexed_children);
    deduplicate_layer_by_child(layer)
}

fn ensure_matching_embedding_spec(
    context: &str,
    actual: &EmbeddingSpec,
    expected: &EmbeddingSpec,
) -> Result<(), IndexerError> {
    if actual != expected {
        return Err(IndexerError::InvalidStagedInput(format!(
            "{context} uses embedding spec {} dims under {}, expected {} dims under {}",
            actual.dims, actual.encoding, expected.dims, expected.encoding
        )));
    }
    Ok(())
}

fn shared_child_level(children: &[IndexedChild]) -> Result<u64, IndexerError> {
    let Some(first) = children.first() else {
        return Err(IndexerError::InvalidStagedInput(
            "parent construction requires at least one child".into(),
        ));
    };
    if children.iter().all(|child| child.level == first.level) {
        Ok(first.level)
    } else {
        let levels = children
            .iter()
            .map(|child| child.level.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        Err(IndexerError::InvalidStagedInput(format!(
            "parent construction requires children from one shared level, got levels [{levels}]"
        )))
    }
}

fn validate_group_partition(
    groups: &[Vec<usize>],
    children: &[IndexedChild],
) -> Result<(), String> {
    if groups.is_empty() {
        return Err("node packing returned no groups".into());
    }

    let child_count = children.len();
    let mut seen = vec![0_u8; child_count];
    let mut child_group_owners = HashMap::with_capacity(child_count);
    for (group_index, group) in groups.iter().enumerate() {
        if group.is_empty() {
            return Err("node packing returned an empty group".into());
        }
        let mut group_children = Vec::with_capacity(group.len());
        for &index in group {
            let Some(slot) = seen.get_mut(index) else {
                return Err(format!(
                    "node packing referenced child index {index}, but only {child_count} children were available"
                ));
            };
            *slot = slot.saturating_add(1);
            group_children.push(children[index].child);
        }

        group_children.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        group_children.dedup_by(|left, right| left.as_bytes() == right.as_bytes());
        for child in group_children {
            if let Some(owner_group) = child_group_owners.insert(child, group_index) {
                return Err(format!(
                    "node packing assigned child {} to groups {} and {}, which would create a DAG",
                    child, owner_group, group_index
                ));
            }
        }
    }

    for (index, count) in seen.into_iter().enumerate() {
        match count {
            1 => {}
            0 => {
                return Err(format!(
                    "node packing omitted child index {index} from the candidate partition"
                ));
            }
            _ => {
                return Err(format!(
                    "node packing used child index {index} more than once in the candidate partition"
                ));
            }
        }
    }

    Ok(())
}

fn normalize_branch_entries(mut entries: Vec<BranchEntry>) -> Vec<BranchEntry> {
    entries.sort_by(|left, right| {
        left.child
            .as_bytes()
            .cmp(right.child.as_bytes())
            .then_with(|| left.embedding.cmp(&right.embedding))
    });

    let mut deduplicated = Vec::with_capacity(entries.len());
    for entry in entries {
        if deduplicated
            .last()
            .is_some_and(|previous: &BranchEntry| previous.child == entry.child)
        {
            continue;
        }
        deduplicated.push(entry);
    }

    deduplicated.sort_by(compare_branch_entries);
    deduplicated
}

fn deduplicate_layer_by_child(mut layer: Vec<IndexedChild>) -> Vec<IndexedChild> {
    layer.sort_by(|left, right| {
        left.child
            .as_bytes()
            .cmp(right.child.as_bytes())
            .then_with(|| left.embedding.cmp(&right.embedding))
    });
    layer.dedup_by(|left, right| left.child == right.child);
    layer.sort_by(compare_indexed_children);
    layer
}

fn compare_indexed_children(left: &IndexedChild, right: &IndexedChild) -> std::cmp::Ordering {
    left.embedding
        .cmp(&right.embedding)
        .then_with(|| left.child.as_bytes().cmp(right.child.as_bytes()))
}

fn compare_branch_entries(left: &BranchEntry, right: &BranchEntry) -> std::cmp::Ordering {
    left.embedding
        .cmp(&right.embedding)
        .then_with(|| left.child.as_bytes().cmp(right.child.as_bytes()))
}

fn validate_embedding_bytes(
    embedding: &[u8],
    spec: &EmbeddingSpec,
    context: &str,
) -> Result<(), String> {
    let expected = expected_embedding_len(spec).ok_or_else(|| {
        format!(
            "unsupported embedding encoding {:?} for {context} embedding validation",
            spec.encoding
        )
    })?;
    if embedding.len() != expected {
        return Err(format!(
            "{context} embedding length {} does not match expected length {expected} for {} dims under {}",
            embedding.len(),
            spec.dims,
            spec.encoding
        ));
    }
    Ok(())
}

fn expected_embedding_len(spec: &EmbeddingSpec) -> Option<usize> {
    let dims = usize::try_from(spec.dims).ok()?;
    match spec.encoding.as_str() {
        "f32le" => dims.checked_mul(4),
        "f16le" => dims.checked_mul(2),
        "i8" => Some(dims),
        "pq4" => dims.checked_add(1).map(|value| value / 2),
        _ => None,
    }
}

fn decode_embeddings_as_f64(
    children: &[IndexedChild],
    spec: &EmbeddingSpec,
) -> Result<Vec<Vec<f64>>, String> {
    validate_dcbc_embedding_encoding(spec)?;
    children
        .iter()
        .map(|child| decode_embedding_as_f64(&child.embedding, spec, "node-packing input"))
        .collect()
}

fn validate_dcbc_embedding_encoding(spec: &EmbeddingSpec) -> Result<(), String> {
    match spec.encoding.as_str() {
        "i8" | "f32le" | "f16le" => Ok(()),
        "pq4" => {
            Err("pq4 embeddings cannot be used with the default DCBC node-packing policy".into())
        }
        other => Err(format!(
            "unsupported embedding encoding {other:?} for default DCBC node packing"
        )),
    }
}

fn decode_embedding_as_f64(
    embedding: &[u8],
    spec: &EmbeddingSpec,
    context: &str,
) -> Result<Vec<f64>, String> {
    validate_embedding_bytes(embedding, spec, context)?;
    match spec.encoding.as_str() {
        "i8" => Ok(embedding
            .iter()
            .map(|byte| i8::from_le_bytes([*byte]) as f64)
            .collect()),
        "f32le" => embedding
            .chunks_exact(4)
            .map(|chunk| {
                let bytes: [u8; 4] = chunk
                    .try_into()
                    .map_err(|_| "invalid f32le embedding chunk length".to_string())?;
                Ok(f32::from_le_bytes(bytes) as f64)
            })
            .collect(),
        "f16le" => embedding
            .chunks_exact(2)
            .map(|chunk| {
                let bytes: [u8; 2] = chunk
                    .try_into()
                    .map_err(|_| "invalid f16le embedding chunk length".to_string())?;
                Ok(f16::from_le_bytes(bytes).to_f64())
            })
            .collect(),
        "pq4" => Err("pq4 embeddings cannot be decoded as arithmetic vectors".into()),
        other => Err(format!(
            "unsupported embedding encoding {other:?} for arithmetic decoding"
        )),
    }
}

fn arithmetic_mean_canonical_embedding(block: &BranchBlock) -> Result<Vec<u8>, String> {
    if block.entries.is_empty() {
        return Err(
            "built-in arithmetic-mean canonical policy requires at least one branch entry".into(),
        );
    }

    let dims = usize::try_from(block.embedding_spec.dims).map_err(|_| {
        format!(
            "branch embedding dims {} do not fit in usize",
            block.embedding_spec.dims
        )
    })?;
    let mut sums = vec![0.0_f64; dims];

    for (entry_index, entry) in block.entries.iter().enumerate() {
        let decoded = decode_embedding_as_f64(&entry.embedding, &block.embedding_spec, "canonical")
            .map_err(|error| format!("failed to decode branch entry {entry_index}: {error}"))?;
        for (dimension, (sum, value)) in sums.iter_mut().zip(decoded).enumerate() {
            if !value.is_finite() {
                return Err(format!(
                    "branch entry {entry_index} contains a non-finite value at dimension {dimension}"
                ));
            }
            *sum += value;
            if !sum.is_finite() {
                return Err(format!(
                    "arithmetic-mean sum overflowed or became non-finite at dimension {dimension}"
                ));
            }
        }
    }

    let divisor = block.entries.len() as f64;
    for (dimension, sum) in sums.iter_mut().enumerate() {
        *sum /= divisor;
        if !sum.is_finite() {
            return Err(format!(
                "arithmetic-mean result became non-finite at dimension {dimension}"
            ));
        }
    }

    encode_embedding_from_f64(&sums, &block.embedding_spec)
}

fn encode_embedding_from_f64(values: &[f64], spec: &EmbeddingSpec) -> Result<Vec<u8>, String> {
    let dims = usize::try_from(spec.dims)
        .map_err(|_| format!("embedding dims {} do not fit in usize", spec.dims))?;
    if values.len() != dims {
        return Err(format!(
            "mean embedding dimension {} does not match expected dimension {dims}",
            values.len()
        ));
    }

    match spec.encoding.as_str() {
        "f32le" => {
            let mut bytes = Vec::with_capacity(dims * std::mem::size_of::<f32>());
            for (dimension, value) in values.iter().copied().enumerate() {
                if !value.is_finite() {
                    return Err(format!(
                        "cannot encode non-finite arithmetic mean at dimension {dimension}"
                    ));
                }
                let encoded = value as f32;
                if !encoded.is_finite() {
                    return Err(format!(
                        "arithmetic mean overflowed f32 encoding at dimension {dimension}"
                    ));
                }
                bytes.extend_from_slice(&encoded.to_le_bytes());
            }
            Ok(bytes)
        }
        "f16le" => {
            let mut bytes = Vec::with_capacity(dims * std::mem::size_of::<u16>());
            for (dimension, value) in values.iter().copied().enumerate() {
                if !value.is_finite() {
                    return Err(format!(
                        "cannot encode non-finite arithmetic mean at dimension {dimension}"
                    ));
                }
                let encoded = f16::from_f64(value);
                if !encoded.to_f64().is_finite() {
                    return Err(format!(
                        "arithmetic mean overflowed f16 encoding at dimension {dimension}"
                    ));
                }
                bytes.extend_from_slice(&encoded.to_le_bytes());
            }
            Ok(bytes)
        }
        "i8" => values
            .iter()
            .copied()
            .enumerate()
            .map(|(dimension, value)| {
                if !value.is_finite() {
                    return Err(format!(
                        "cannot encode non-finite arithmetic mean at dimension {dimension}"
                    ));
                }
                let rounded = value.round();
                if rounded < f64::from(i8::MIN) || rounded > f64::from(i8::MAX) {
                    return Err(format!(
                        "arithmetic mean {rounded} exceeds i8 range at dimension {dimension}"
                    ));
                }
                Ok((rounded as i8).to_le_bytes()[0])
            })
            .collect(),
        "pq4" => Err(
            "pq4 embeddings are not supported by the built-in arithmetic-mean canonical policy"
                .into(),
        ),
        other => Err(format!(
            "unsupported embedding encoding {other:?} for arithmetic-mean canonical policy"
        )),
    }
}

fn assignment_to_groups(
    assignment: &[usize],
    cluster_count: usize,
) -> Result<Vec<Vec<usize>>, String> {
    let mut groups = vec![Vec::new(); cluster_count];
    for (child_index, &cluster_index) in assignment.iter().enumerate() {
        let Some(group) = groups.get_mut(cluster_index) else {
            return Err(format!(
                "dcbc assignment referenced cluster {cluster_index}, but only {cluster_count} clusters were configured"
            ));
        };
        group.push(child_index);
    }
    groups.retain(|group| !group.is_empty());
    Ok(groups)
}

fn max_children_per_branch(
    spec: &EmbeddingSpec,
    block_size_target: usize,
    child_count: usize,
) -> Result<usize, String> {
    if child_count < 2 {
        return Ok(child_count);
    }

    let min_size = serialized_branch_size(spec, 2)?;
    if min_size > block_size_target {
        return Ok(1);
    }

    let mut low = 2;
    let mut high = 2;
    while high < child_count {
        let candidate = (high.saturating_mul(2)).min(child_count);
        if serialized_branch_size(spec, candidate)? <= block_size_target {
            low = candidate;
            high = candidate;
        } else {
            high = candidate;
            break;
        }
    }

    if low == child_count {
        return Ok(child_count);
    }

    while low + 1 < high {
        let mid = low + (high - low) / 2;
        if serialized_branch_size(spec, mid)? <= block_size_target {
            low = mid;
        } else {
            high = mid;
        }
    }
    Ok(low)
}

fn serialized_branch_size(spec: &EmbeddingSpec, entry_count: usize) -> Result<usize, String> {
    let embedding_len = expected_embedding_len(spec).ok_or_else(|| {
        format!(
            "unsupported embedding encoding {:?} for branch-size estimation",
            spec.encoding
        )
    })?;
    let entries = (0..entry_count)
        .map(|index| BranchEntry {
            embedding: vec![0; embedding_len],
            child: synthetic_block_hash(index),
        })
        .collect();
    let branch = build_branch_block(VERSION_1, 1, spec.clone(), entries, None)
        .map_err(|error| format!("failed to build synthetic branch block: {error}"))?;
    let block = Block::Branch(branch);
    serialize_block(&block)
        .map(|serialized| serialized.bytes.len())
        .map_err(|error| format!("failed to serialize synthetic branch block: {error}"))
}

fn synthetic_block_hash(index: usize) -> BlockHash {
    let mut bytes = [0_u8; BlockHash::LEN];
    bytes[..std::mem::size_of::<usize>()].copy_from_slice(&index.to_le_bytes());
    BlockHash::from_bytes(bytes)
}

#[cfg(feature = "conformance")]
mod conformance_support {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fmt;

    use lexongraph_block::{TypedEntries, into_entries};

    use super::*;

    #[derive(Default)]
    struct MemoryBlockStore {
        blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
    }

    impl BlockStore for MemoryBlockStore {
        fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
            let serialized = serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
            self.blocks
                .borrow_mut()
                .insert(serialized.hash, serialized.bytes);
            Ok(serialized.hash)
        }

        fn get(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
            let Some(bytes) = self.blocks.borrow().get(block_id).cloned() else {
                return Ok(None);
            };

            lexongraph_block::deserialize_block(&bytes, block_id)
                .map(Some)
                .map_err(map_get_error)
        }
    }

    pub type ConformanceResult = Result<(), ConformanceError>;

    #[derive(Debug)]
    pub enum ConformanceError {
        Indexer(IndexerError),
        Expectation(String),
    }

    impl fmt::Display for ConformanceError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Indexer(error) => write!(f, "{error}"),
                Self::Expectation(message) => {
                    write!(f, "conformance expectation failed: {message}")
                }
            }
        }
    }

    impl std::error::Error for ConformanceError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Indexer(error) => Some(error),
                Self::Expectation(_) => None,
            }
        }
    }

    impl From<IndexerError> for ConformanceError {
        fn from(value: IndexerError) -> Self {
            Self::Indexer(value)
        }
    }

    pub trait ContentResolverConformanceHarness {
        type Ref;
        type Resolver: ContentResolver<Self::Ref>;

        fn sample_item(&self) -> IndexItem<Self::Ref>;
        fn expected_content(&self) -> Content;
        fn conforming_resolver(&self) -> Self::Resolver;
        fn failing_resolver(&self) -> Self::Resolver;
        fn unusable_resolver(&self) -> Self::Resolver;
    }

    pub trait CanonicalEmbeddingPolicyConformanceHarness {
        type Policy: CanonicalEmbeddingPolicy;

        fn conforming_policy(&self) -> Self::Policy;
        fn failing_policy(&self) -> Self::Policy;
        fn invalid_length_policy(&self) -> Self::Policy;
    }

    pub trait NodePackingPolicyConformanceHarness {
        type Policy: NodePackingPolicy;

        fn conforming_policy(&self) -> Self::Policy;
        fn failing_policy(&self) -> Self::Policy;
        fn singleton_group_policy(&self) -> Self::Policy;
        fn out_of_bounds_policy(&self) -> Self::Policy;
        fn missing_child_policy(&self) -> Self::Policy;
    }

    pub fn run_content_resolver_suite<H>(harness: &H) -> ConformanceResult
    where
        H: ContentResolverConformanceHarness,
    {
        pollster::block_on(async {
            let store = MemoryBlockStore::default();
            let item = harness.sample_item();
            let indexer = Indexer::with_node_packing_policy(
                harness.conforming_resolver(),
                FixedEmbeddingProvider,
                FixedCanonicalEmbeddingPolicy,
                PairPackingPolicy,
            );
            let result = indexer
                .index(
                    &[item],
                    fixture_embedding_spec(),
                    fixture_block_size_target(),
                    &store,
                )
                .await?;
            let loaded = store
                .get(&result.root_id)
                .map_err(IndexerError::Storage)?
                .ok_or_else(|| {
                    ConformanceError::Expectation(
                        "expected indexed root block to be present".into(),
                    )
                })?;
            match into_entries(loaded) {
                TypedEntries::Leaf(_, entries)
                    if entries[0].content == harness.expected_content() => {}
                TypedEntries::Leaf(_, entries) => {
                    return Err(ConformanceError::Expectation(format!(
                        "expected resolved content {:?}, got {:?}",
                        harness.expected_content(),
                        entries[0].content
                    )));
                }
                TypedEntries::Branch(_, _) => {
                    return Err(ConformanceError::Expectation(
                        "expected a leaf root for a single indexed item".into(),
                    ));
                }
            }

            let store = MemoryBlockStore::default();
            let item = harness.sample_item();
            let indexer = Indexer::with_node_packing_policy(
                harness.failing_resolver(),
                FixedEmbeddingProvider,
                FixedCanonicalEmbeddingPolicy,
                PairPackingPolicy,
            );
            expect_indexer_error(
                indexer
                    .index(
                        &[item],
                        fixture_embedding_spec(),
                        fixture_block_size_target(),
                        &store,
                    )
                    .await,
                |error| matches!(error, IndexerError::ContentResolution(_)),
                "expected content-resolution failure",
            )?;

            let store = MemoryBlockStore::default();
            let item = harness.sample_item();
            let indexer = Indexer::with_node_packing_policy(
                harness.unusable_resolver(),
                FixedEmbeddingProvider,
                FixedCanonicalEmbeddingPolicy,
                PairPackingPolicy,
            );
            expect_indexer_error(
                indexer
                    .index(
                        &[item],
                        fixture_embedding_spec(),
                        fixture_block_size_target(),
                        &store,
                    )
                    .await,
                |error| matches!(error, IndexerError::UnusableContent(_)),
                "expected unusable-content failure",
            )
        })
    }

    pub fn run_canonical_embedding_policy_suite<H>(harness: &H) -> ConformanceResult
    where
        H: CanonicalEmbeddingPolicyConformanceHarness,
    {
        pollster::block_on(async {
            let store = MemoryBlockStore::default();
            let indexer = Indexer::with_node_packing_policy(
                FixedResolver,
                FixedMultiEmbeddingProvider,
                harness.conforming_policy(),
                PairPackingPolicy,
            );
            let result = indexer
                .index(
                    &fixture_multi_items(),
                    fixture_embedding_spec(),
                    fixture_block_size_target(),
                    &store,
                )
                .await?;
            let loaded = store
                .get(&result.root_id)
                .map_err(IndexerError::Storage)?
                .ok_or_else(|| {
                    ConformanceError::Expectation(
                        "expected indexed root block to be present".into(),
                    )
                })?;
            match into_entries(loaded) {
                TypedEntries::Branch(_, entries) if entries.len() >= 2 => {}
                TypedEntries::Branch(_, entries) => {
                    return Err(ConformanceError::Expectation(format!(
                        "expected a branch root with at least two entries, got {}",
                        entries.len()
                    )));
                }
                TypedEntries::Leaf(_, _) => {
                    return Err(ConformanceError::Expectation(
                        "expected multi-item indexing to produce a branch root".into(),
                    ));
                }
            }

            let store = MemoryBlockStore::default();
            let indexer = Indexer::with_node_packing_policy(
                FixedResolver,
                FixedMultiEmbeddingProvider,
                harness.failing_policy(),
                PairPackingPolicy,
            );
            expect_indexer_error(
                indexer
                    .index(
                        &fixture_multi_items(),
                        fixture_embedding_spec(),
                        fixture_block_size_target(),
                        &store,
                    )
                    .await,
                |error| matches!(error, IndexerError::CanonicalEmbeddingFailure(_)),
                "expected canonical-embedding failure",
            )?;

            let store = MemoryBlockStore::default();
            let indexer = Indexer::with_node_packing_policy(
                FixedResolver,
                FixedMultiEmbeddingProvider,
                harness.invalid_length_policy(),
                PairPackingPolicy,
            );
            expect_indexer_error(
                indexer
                    .index(
                        &fixture_multi_items(),
                        fixture_embedding_spec(),
                        fixture_block_size_target(),
                        &store,
                    )
                    .await,
                |error| matches!(error, IndexerError::CanonicalEmbeddingFailure(_)),
                "expected invalid canonical-embedding length failure",
            )
        })
    }

    pub fn run_node_packing_policy_suite<H>(harness: &H) -> ConformanceResult
    where
        H: NodePackingPolicyConformanceHarness,
    {
        pollster::block_on(async {
            let store = MemoryBlockStore::default();
            let indexer = Indexer::with_node_packing_policy(
                FixedResolver,
                FixedMultiEmbeddingProvider,
                FixedCanonicalEmbeddingPolicy,
                harness.conforming_policy(),
            );
            indexer
                .index(
                    &fixture_multi_items(),
                    fixture_embedding_spec(),
                    fixture_block_size_target(),
                    &store,
                )
                .await?;

            let store = MemoryBlockStore::default();
            let indexer = Indexer::with_node_packing_policy(
                FixedResolver,
                FixedMultiEmbeddingProvider,
                FixedCanonicalEmbeddingPolicy,
                harness.failing_policy(),
            );
            expect_indexer_error(
                indexer
                    .index(
                        &fixture_multi_items(),
                        fixture_embedding_spec(),
                        fixture_block_size_target(),
                        &store,
                    )
                    .await,
                |error| matches!(error, IndexerError::NodePackingFailure(_)),
                "expected node-packing failure",
            )?;

            let store = MemoryBlockStore::default();
            let indexer = Indexer::with_node_packing_policy(
                FixedResolver,
                FixedMultiEmbeddingProvider,
                FixedCanonicalEmbeddingPolicy,
                harness.singleton_group_policy(),
            );
            expect_indexer_error(
                indexer
                    .index(
                        &fixture_multi_items(),
                        fixture_embedding_spec(),
                        fixture_block_size_target(),
                        &store,
                    )
                    .await,
                |error| matches!(error, IndexerError::NodePackingFailure(_)),
                "expected singleton-group failure",
            )?;

            let store = MemoryBlockStore::default();
            let indexer = Indexer::with_node_packing_policy(
                FixedResolver,
                FixedMultiEmbeddingProvider,
                FixedCanonicalEmbeddingPolicy,
                harness.out_of_bounds_policy(),
            );
            expect_indexer_error(
                indexer
                    .index(
                        &fixture_multi_items(),
                        fixture_embedding_spec(),
                        fixture_block_size_target(),
                        &store,
                    )
                    .await,
                |error| matches!(error, IndexerError::NodePackingFailure(_)),
                "expected out-of-bounds packing failure",
            )?;

            let store = MemoryBlockStore::default();
            let indexer = Indexer::with_node_packing_policy(
                FixedResolver,
                FixedMultiEmbeddingProvider,
                FixedCanonicalEmbeddingPolicy,
                harness.missing_child_policy(),
            );
            expect_indexer_error(
                indexer
                    .index(
                        &fixture_multi_items(),
                        fixture_embedding_spec(),
                        fixture_block_size_target(),
                        &store,
                    )
                    .await,
                |error| matches!(error, IndexerError::NodePackingFailure(_)),
                "expected missing-child packing failure",
            )
        })
    }

    pub fn run_full_trait_suite<CR, CEP, NPP>(
        content_harness: &CR,
        canonical_harness: &CEP,
        packing_harness: &NPP,
    ) -> ConformanceResult
    where
        CR: ContentResolverConformanceHarness,
        CEP: CanonicalEmbeddingPolicyConformanceHarness,
        NPP: NodePackingPolicyConformanceHarness,
    {
        run_content_resolver_suite(content_harness)?;
        run_canonical_embedding_policy_suite(canonical_harness)?;
        run_node_packing_policy_suite(packing_harness)
    }

    #[derive(Clone, Copy)]
    struct FixedResolver;

    impl ContentResolver<u8> for FixedResolver {
        type Error = FixtureError;

        fn resolve(&self, content_ref: &u8) -> Result<Content, Self::Error> {
            Ok(Content {
                media_type: "text/plain".into(),
                body: vec![*content_ref],
            })
        }
    }

    #[derive(Clone, Copy)]
    struct FixedEmbeddingProvider;

    impl EmbeddingProvider for FixedEmbeddingProvider {
        type Error = FixtureError;

        async fn embed(
            &self,
            _: &EmbeddingInput,
            _: &EmbeddingSpec,
        ) -> Result<Vec<u8>, Self::Error> {
            Ok(vec![0x10, 0x20])
        }
    }

    #[derive(Clone, Copy)]
    struct FixedMultiEmbeddingProvider;

    impl EmbeddingProvider for FixedMultiEmbeddingProvider {
        type Error = FixtureError;

        async fn embed(
            &self,
            input: &EmbeddingInput,
            _: &EmbeddingSpec,
        ) -> Result<Vec<u8>, Self::Error> {
            let first = *input
                .body
                .first()
                .ok_or_else(|| FixtureError("expected non-empty fixture content".into()))?;
            Ok(vec![first, first.wrapping_add(1)])
        }
    }

    #[derive(Clone, Copy)]
    struct FixedCanonicalEmbeddingPolicy;

    impl CanonicalEmbeddingPolicy for FixedCanonicalEmbeddingPolicy {
        type Error = FixtureError;

        fn canonical_embedding(&self, block: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
            block
                .entries
                .first()
                .map(|entry| entry.embedding.clone())
                .ok_or_else(|| FixtureError("expected branch block to contain entries".into()))
        }
    }

    #[derive(Clone, Copy)]
    struct PairPackingPolicy;

    impl NodePackingPolicy for PairPackingPolicy {
        type Error = FixtureError;

        fn pack(
            &self,
            children: &[IndexedChild],
            _: &EmbeddingSpec,
            _: usize,
        ) -> Result<Vec<Vec<usize>>, Self::Error> {
            if children.len() < 2 {
                return Err(FixtureError("expected at least two children".into()));
            }

            let mut groups = Vec::new();
            let mut index = 0;
            while index < children.len() {
                let remaining = children.len() - index;
                let width = if remaining == 3 { 3 } else { 2 };
                groups.push((index..index + width).collect());
                index += width;
            }
            Ok(groups)
        }
    }

    #[derive(Clone, Debug)]
    pub struct FixtureError(pub String);

    impl fmt::Display for FixtureError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for FixtureError {}

    fn fixture_embedding_spec() -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: "i8".into(),
        }
    }

    fn fixture_block_size_target() -> usize {
        256
    }

    fn fixture_multi_items() -> Vec<IndexItem<u8>> {
        vec![
            IndexItem {
                metadata: vec![],
                content_ref: b'a',
            },
            IndexItem {
                metadata: vec![],
                content_ref: b'b',
            },
            IndexItem {
                metadata: vec![],
                content_ref: b'c',
            },
            IndexItem {
                metadata: vec![],
                content_ref: b'd',
            },
        ]
    }

    fn expect_indexer_error(
        result: Result<IndexingResult, IndexerError>,
        matcher: impl FnOnce(&IndexerError) -> bool,
        message: &str,
    ) -> ConformanceResult {
        match result {
            Err(error) if matcher(&error) => Ok(()),
            Err(error) => Err(ConformanceError::Expectation(format!(
                "{message}, got {error}"
            ))),
            Ok(result) => Err(ConformanceError::Expectation(format!(
                "{message}, got successful result {result:?}"
            ))),
        }
    }

    fn map_get_error(error: BlockError) -> BlockStoreError {
        match error {
            BlockError::HashMismatch { expected, actual } => {
                BlockStoreError::IntegrityMismatch { expected, actual }
            }
            other => BlockStoreError::MalformedContent(other),
        }
    }
}

#[cfg(feature = "conformance")]
pub mod conformance {
    //! Opt-in helper APIs for validating downstream implementations of the
    //! indexer-owned policy traits.
    //!
    //! Enable this module from test code with a dev-dependency such as:
    //!
    //! ```toml
    //! [dev-dependencies]
    //! lexongraph-indexer = { version = "*", features = ["conformance"] }
    //! ```

    pub use super::conformance_support::{
        CanonicalEmbeddingPolicyConformanceHarness, ConformanceError, ConformanceResult,
        ContentResolverConformanceHarness, FixtureError, NodePackingPolicyConformanceHarness,
        run_canonical_embedding_policy_suite, run_content_resolver_suite, run_full_trait_suite,
        run_node_packing_policy_suite,
    };
}
