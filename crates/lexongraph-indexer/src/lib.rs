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

pub use lexongraph_block::{BlockHash, BranchBlock, Content, EmbeddingSpec, Metadata};

use lexongraph_block::{
    Block, BlockError, BranchEntry, LeafEntry, VERSION_1, build_branch_block, build_leaf_block,
    serialize_block,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedChild {
    pub embedding: Vec<u8>,
    pub child: BlockHash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexerError {
    EmptyInput,
    ContentResolution(String),
    UnusableContent(String),
    EmbeddingFailure(String),
    CanonicalEmbeddingFailure(String),
    NodePackingFailure(String),
    IntermediateNodeTooLarge {
        min_serialized_bytes: usize,
        size_target: usize,
    },
    BlockConstruction(BlockError),
    Storage(BlockStoreError),
}

impl fmt::Display for IndexerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyInput => write!(f, "indexing requires at least one item"),
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
            Self::BlockConstruction(error) => write!(f, "block construction failed: {error}"),
            Self::Storage(error) => write!(f, "block storage failed: {error}"),
        }
    }
}

impl std::error::Error for IndexerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BlockConstruction(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::EmptyInput
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

pub trait EmbeddingProvider {
    type Error: std::error::Error;

    fn embed(&self, content: &Content, spec: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error>;
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

#[derive(Clone, Debug)]
pub struct Indexer<CR, EP, CEP, NPP> {
    resolver: CR,
    embedding_provider: EP,
    canonical_embedding_policy: CEP,
    node_packing_policy: NPP,
}

impl<CR, EP, CEP, NPP> Indexer<CR, EP, CEP, NPP> {
    pub fn new(
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
    pub fn index<R>(
        &self,
        items: &[IndexItem<R>],
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
        store: &dyn BlockStore,
    ) -> Result<IndexingResult, IndexerError>
    where
        CR: ContentResolver<R>,
    {
        if items.is_empty() {
            return Err(IndexerError::EmptyInput);
        }

        let mut persisted_block_ids = Vec::with_capacity(items.len());
        let mut current_layer = Vec::with_capacity(items.len());

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

            let embedding = self
                .embedding_provider
                .embed(&content, &embedding_spec)
                .map_err(|error| IndexerError::EmbeddingFailure(error.to_string()))?;
            validate_embedding_bytes(&embedding, &embedding_spec, "item")
                .map_err(IndexerError::EmbeddingFailure)?;

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
            });
        }

        current_layer.sort_by(compare_indexed_children);
        current_layer = deduplicate_layer_by_child(current_layer);

        while current_layer.len() > 1 {
            let groups = self
                .node_packing_policy
                .pack(&current_layer, &embedding_spec, block_size_target)
                .map_err(|error| IndexerError::NodePackingFailure(error.to_string()))?;
            validate_group_partition(&groups, &current_layer)
                .map_err(IndexerError::NodePackingFailure)?;

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
                        "node packing candidate normalized to fewer than two unique children"
                            .into(),
                    ));
                }

                let branch = build_branch_block(VERSION_1, embedding_spec.clone(), entries, None)
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
                validate_embedding_bytes(&canonical_embedding, &embedding_spec, "canonical")
                    .map_err(IndexerError::CanonicalEmbeddingFailure)?;

                let block_id = store.put(&branch_block).map_err(IndexerError::Storage)?;
                persisted_block_ids.push(block_id);

                next_layer.push(IndexedChild {
                    embedding: canonical_embedding,
                    child: block_id,
                });
            }

            next_layer.sort_by(compare_indexed_children);
            current_layer = deduplicate_layer_by_child(next_layer);
        }

        persisted_block_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        persisted_block_ids.dedup_by(|left, right| left.as_bytes() == right.as_bytes());

        Ok(IndexingResult {
            root_id: current_layer[0].child,
            block_ids: persisted_block_ids,
        })
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

    pub trait EmbeddingProviderConformanceHarness {
        type Provider: EmbeddingProvider;

        fn expected_embedding(&self) -> Vec<u8>;
        fn conforming_provider(&self) -> Self::Provider;
        fn failing_provider(&self) -> Self::Provider;
        fn invalid_length_provider(&self) -> Self::Provider;
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
        let store = MemoryBlockStore::default();
        let item = harness.sample_item();
        let indexer = Indexer::new(
            harness.conforming_resolver(),
            FixedEmbeddingProvider,
            FixedCanonicalEmbeddingPolicy,
            PairPackingPolicy,
        );
        let result = indexer.index(
            &[item],
            fixture_embedding_spec(),
            fixture_block_size_target(),
            &store,
        )?;
        let loaded = store
            .get(&result.root_id)
            .map_err(IndexerError::Storage)?
            .ok_or_else(|| {
                ConformanceError::Expectation("expected indexed root block to be present".into())
            })?;
        match into_entries(loaded) {
            TypedEntries::Leaf(_, entries) if entries[0].content == harness.expected_content() => {}
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
        let indexer = Indexer::new(
            harness.failing_resolver(),
            FixedEmbeddingProvider,
            FixedCanonicalEmbeddingPolicy,
            PairPackingPolicy,
        );
        expect_indexer_error(
            indexer.index(
                &[item],
                fixture_embedding_spec(),
                fixture_block_size_target(),
                &store,
            ),
            |error| matches!(error, IndexerError::ContentResolution(_)),
            "expected content-resolution failure",
        )?;

        let store = MemoryBlockStore::default();
        let item = harness.sample_item();
        let indexer = Indexer::new(
            harness.unusable_resolver(),
            FixedEmbeddingProvider,
            FixedCanonicalEmbeddingPolicy,
            PairPackingPolicy,
        );
        expect_indexer_error(
            indexer.index(
                &[item],
                fixture_embedding_spec(),
                fixture_block_size_target(),
                &store,
            ),
            |error| matches!(error, IndexerError::UnusableContent(_)),
            "expected unusable-content failure",
        )
    }

    pub fn run_embedding_provider_suite<H>(harness: &H) -> ConformanceResult
    where
        H: EmbeddingProviderConformanceHarness,
    {
        let store = MemoryBlockStore::default();
        let indexer = Indexer::new(
            FixedResolver,
            harness.conforming_provider(),
            FixedCanonicalEmbeddingPolicy,
            PairPackingPolicy,
        );
        let result = indexer.index(
            &[fixture_single_item()],
            fixture_embedding_spec(),
            fixture_block_size_target(),
            &store,
        )?;
        let loaded = store
            .get(&result.root_id)
            .map_err(IndexerError::Storage)?
            .ok_or_else(|| {
                ConformanceError::Expectation("expected indexed root block to be present".into())
            })?;
        match into_entries(loaded) {
            TypedEntries::Leaf(_, entries)
                if entries[0].embedding == harness.expected_embedding() => {}
            TypedEntries::Leaf(_, entries) => {
                return Err(ConformanceError::Expectation(format!(
                    "expected embedding {:?}, got {:?}",
                    harness.expected_embedding(),
                    entries[0].embedding
                )));
            }
            TypedEntries::Branch(_, _) => {
                return Err(ConformanceError::Expectation(
                    "expected a leaf root for a single indexed item".into(),
                ));
            }
        }

        let store = MemoryBlockStore::default();
        let indexer = Indexer::new(
            FixedResolver,
            harness.failing_provider(),
            FixedCanonicalEmbeddingPolicy,
            PairPackingPolicy,
        );
        expect_indexer_error(
            indexer.index(
                &[fixture_single_item()],
                fixture_embedding_spec(),
                fixture_block_size_target(),
                &store,
            ),
            |error| matches!(error, IndexerError::EmbeddingFailure(_)),
            "expected embedding-generation failure",
        )?;

        let store = MemoryBlockStore::default();
        let indexer = Indexer::new(
            FixedResolver,
            harness.invalid_length_provider(),
            FixedCanonicalEmbeddingPolicy,
            PairPackingPolicy,
        );
        expect_indexer_error(
            indexer.index(
                &[fixture_single_item()],
                fixture_embedding_spec(),
                fixture_block_size_target(),
                &store,
            ),
            |error| matches!(error, IndexerError::EmbeddingFailure(_)),
            "expected invalid embedding length failure",
        )
    }

    pub fn run_canonical_embedding_policy_suite<H>(harness: &H) -> ConformanceResult
    where
        H: CanonicalEmbeddingPolicyConformanceHarness,
    {
        let store = MemoryBlockStore::default();
        let indexer = Indexer::new(
            FixedResolver,
            FixedMultiEmbeddingProvider,
            harness.conforming_policy(),
            PairPackingPolicy,
        );
        let result = indexer.index(
            &fixture_multi_items(),
            fixture_embedding_spec(),
            fixture_block_size_target(),
            &store,
        )?;
        let loaded = store
            .get(&result.root_id)
            .map_err(IndexerError::Storage)?
            .ok_or_else(|| {
                ConformanceError::Expectation("expected indexed root block to be present".into())
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
        let indexer = Indexer::new(
            FixedResolver,
            FixedMultiEmbeddingProvider,
            harness.failing_policy(),
            PairPackingPolicy,
        );
        expect_indexer_error(
            indexer.index(
                &fixture_multi_items(),
                fixture_embedding_spec(),
                fixture_block_size_target(),
                &store,
            ),
            |error| matches!(error, IndexerError::CanonicalEmbeddingFailure(_)),
            "expected canonical-embedding failure",
        )?;

        let store = MemoryBlockStore::default();
        let indexer = Indexer::new(
            FixedResolver,
            FixedMultiEmbeddingProvider,
            harness.invalid_length_policy(),
            PairPackingPolicy,
        );
        expect_indexer_error(
            indexer.index(
                &fixture_multi_items(),
                fixture_embedding_spec(),
                fixture_block_size_target(),
                &store,
            ),
            |error| matches!(error, IndexerError::CanonicalEmbeddingFailure(_)),
            "expected invalid canonical-embedding length failure",
        )
    }

    pub fn run_node_packing_policy_suite<H>(harness: &H) -> ConformanceResult
    where
        H: NodePackingPolicyConformanceHarness,
    {
        let store = MemoryBlockStore::default();
        let indexer = Indexer::new(
            FixedResolver,
            FixedMultiEmbeddingProvider,
            FixedCanonicalEmbeddingPolicy,
            harness.conforming_policy(),
        );
        indexer.index(
            &fixture_multi_items(),
            fixture_embedding_spec(),
            fixture_block_size_target(),
            &store,
        )?;

        let store = MemoryBlockStore::default();
        let indexer = Indexer::new(
            FixedResolver,
            FixedMultiEmbeddingProvider,
            FixedCanonicalEmbeddingPolicy,
            harness.failing_policy(),
        );
        expect_indexer_error(
            indexer.index(
                &fixture_multi_items(),
                fixture_embedding_spec(),
                fixture_block_size_target(),
                &store,
            ),
            |error| matches!(error, IndexerError::NodePackingFailure(_)),
            "expected node-packing failure",
        )?;

        let store = MemoryBlockStore::default();
        let indexer = Indexer::new(
            FixedResolver,
            FixedMultiEmbeddingProvider,
            FixedCanonicalEmbeddingPolicy,
            harness.singleton_group_policy(),
        );
        expect_indexer_error(
            indexer.index(
                &fixture_multi_items(),
                fixture_embedding_spec(),
                fixture_block_size_target(),
                &store,
            ),
            |error| matches!(error, IndexerError::NodePackingFailure(_)),
            "expected singleton-group failure",
        )?;

        let store = MemoryBlockStore::default();
        let indexer = Indexer::new(
            FixedResolver,
            FixedMultiEmbeddingProvider,
            FixedCanonicalEmbeddingPolicy,
            harness.out_of_bounds_policy(),
        );
        expect_indexer_error(
            indexer.index(
                &fixture_multi_items(),
                fixture_embedding_spec(),
                fixture_block_size_target(),
                &store,
            ),
            |error| matches!(error, IndexerError::NodePackingFailure(_)),
            "expected out-of-bounds packing failure",
        )?;

        let store = MemoryBlockStore::default();
        let indexer = Indexer::new(
            FixedResolver,
            FixedMultiEmbeddingProvider,
            FixedCanonicalEmbeddingPolicy,
            harness.missing_child_policy(),
        );
        expect_indexer_error(
            indexer.index(
                &fixture_multi_items(),
                fixture_embedding_spec(),
                fixture_block_size_target(),
                &store,
            ),
            |error| matches!(error, IndexerError::NodePackingFailure(_)),
            "expected missing-child packing failure",
        )
    }

    pub fn run_full_trait_suite<CR, EP, CEP, NPP>(
        content_harness: &CR,
        embedding_harness: &EP,
        canonical_harness: &CEP,
        packing_harness: &NPP,
    ) -> ConformanceResult
    where
        CR: ContentResolverConformanceHarness,
        EP: EmbeddingProviderConformanceHarness,
        CEP: CanonicalEmbeddingPolicyConformanceHarness,
        NPP: NodePackingPolicyConformanceHarness,
    {
        run_content_resolver_suite(content_harness)?;
        run_embedding_provider_suite(embedding_harness)?;
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

        fn embed(&self, _: &Content, _: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
            Ok(vec![0x10, 0x20])
        }
    }

    #[derive(Clone, Copy)]
    struct FixedMultiEmbeddingProvider;

    impl EmbeddingProvider for FixedMultiEmbeddingProvider {
        type Error = FixtureError;

        fn embed(&self, content: &Content, _: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
            let first = *content
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

    fn fixture_single_item() -> IndexItem<u8> {
        IndexItem {
            metadata: vec![],
            content_ref: b'f',
        }
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
        ContentResolverConformanceHarness, EmbeddingProviderConformanceHarness, FixtureError,
        NodePackingPolicyConformanceHarness, run_canonical_embedding_policy_suite,
        run_content_resolver_suite, run_embedding_provider_suite, run_full_trait_suite,
        run_node_packing_policy_suite,
    };
}
