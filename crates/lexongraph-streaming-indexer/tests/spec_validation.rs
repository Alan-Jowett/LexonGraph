// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Executable verification for docs/specs/rust-streaming-indexer-crate/validation.md

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex};

use lexongraph_block::{
    BlockError, BlockHash, BranchBlock, Content, EmbeddingSpec, TypedEntries, into_entries,
    serialize_block,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_directional_pca::DirectionalPcaParams;
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_streaming_clustering::{
    ClusterId, MetricDirection, PassReport, StreamingClusterClassifier, StreamingClusterTrainer,
    StreamingClusteringConfig, StreamingClusteringError, TrainerState,
};
use lexongraph_streaming_indexer::{
    ArithmeticMeanCanonicalEmbeddingPolicy, BuiltInClustering, BuiltInClusteringFactory,
    CanonicalEmbeddingPolicy, ContentResolver, DcbcBuiltInClusteringSettings,
    DcbcStreamingClusteringFactory, DirectionalPcaBuiltInClusteringSettings, IndexItem,
    StreamingClusteringFactory, StreamingIndexerError, StreamingIndexingResult,
    StreamingIndexingRun, StreamingIndexingStatus, StreamingIndexingStatusObserver,
};
use sha2::{Digest, Sha256};

// ─── Shared test infrastructure ───────────────────────────────────────────────

#[derive(Debug, Default)]
struct MemoryBlockStore {
    blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
}

impl MemoryBlockStore {
    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.blocks.borrow().len()
    }
}

fn markdown_section<'a>(document: &'a str, heading: &str) -> &'a str {
    let marker = format!("### {heading}");
    let start = document
        .find(&marker)
        .unwrap_or_else(|| panic!("document must contain section heading `{heading}`"));
    let tail = &document[start..];
    let end = tail.find("\n### ").unwrap_or(tail.len());
    &tail[..end]
}

impl BlockStore for MemoryBlockStore {
    fn put(&self, block: &lexongraph_block::Block) -> Result<BlockHash, BlockStoreError> {
        let serialized =
            lexongraph_block::serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
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
            .map_err(|e| match e {
                BlockError::HashMismatch { expected, actual } => {
                    BlockStoreError::IntegrityMismatch { expected, actual }
                }
                other => BlockStoreError::MalformedContent(other),
            })
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        let ids = self.blocks.borrow().keys().copied().collect::<Vec<_>>();
        Ok(Box::new(ids.into_iter().map(Ok)))
    }
}

#[derive(Default)]
struct FaultyIdStore {
    blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
    corrupt_leaf_ids: bool,
    corrupt_branch_ids: bool,
}

impl FaultyIdStore {
    fn corrupt_leaf_ids() -> Self {
        Self {
            blocks: RefCell::default(),
            corrupt_leaf_ids: true,
            corrupt_branch_ids: false,
        }
    }

    fn corrupt_branch_ids() -> Self {
        Self {
            blocks: RefCell::default(),
            corrupt_leaf_ids: false,
            corrupt_branch_ids: true,
        }
    }
}

impl BlockStore for FaultyIdStore {
    fn put(&self, block: &lexongraph_block::Block) -> Result<BlockHash, BlockStoreError> {
        let serialized =
            lexongraph_block::serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
        self.blocks
            .borrow_mut()
            .insert(serialized.hash, serialized.bytes);

        let should_corrupt = match block {
            lexongraph_block::Block::Leaf(_) => self.corrupt_leaf_ids,
            lexongraph_block::Block::Branch(_) => self.corrupt_branch_ids,
        };

        if should_corrupt {
            let mut bytes = serialized.hash.into_bytes();
            bytes[0] ^= 0xFF;
            Ok(BlockHash::from_bytes(bytes))
        } else {
            Ok(serialized.hash)
        }
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
            .map_err(|e| match e {
                BlockError::HashMismatch { expected, actual } => {
                    BlockStoreError::IntegrityMismatch { expected, actual }
                }
                other => BlockStoreError::MalformedContent(other),
            })
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        let ids = self.blocks.borrow().keys().copied().collect::<Vec<_>>();
        Ok(Box::new(ids.into_iter().map(Ok)))
    }
}

#[derive(Default)]
struct FailOnceStore {
    blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
    should_fail: Cell<bool>,
}

impl FailOnceStore {
    fn new() -> Self {
        Self {
            blocks: RefCell::default(),
            should_fail: Cell::new(true),
        }
    }
}

impl BlockStore for FailOnceStore {
    fn put(&self, block: &lexongraph_block::Block) -> Result<BlockHash, BlockStoreError> {
        if self.should_fail.replace(false) {
            return Err(BlockStoreError::BackendFailure("transient failure".into()));
        }
        let serialized =
            lexongraph_block::serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
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
            .map_err(|e| match e {
                BlockError::HashMismatch { expected, actual } => {
                    BlockStoreError::IntegrityMismatch { expected, actual }
                }
                other => BlockStoreError::MalformedContent(other),
            })
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        let ids = self.blocks.borrow().keys().copied().collect::<Vec<_>>();
        Ok(Box::new(ids.into_iter().map(Ok)))
    }
}

// ─── Fixture types ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct FixtureError(String);

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for FixtureError {}

/// Maps content_ref directly as body bytes with media_type = "text/plain".
#[derive(Clone, Copy)]
struct MapResolver;

impl ContentResolver<&'static str> for MapResolver {
    type Error = FixtureError;
    fn resolve(&self, r: &&'static str) -> Result<Content, Self::Error> {
        Ok(Content {
            media_type: "text/plain".into(),
            body: r.as_bytes().to_vec(),
        })
    }
    fn fingerprint(&self, r: &&'static str) -> Result<BlockHash, Self::Error> {
        Ok(hash_bytes(r.as_bytes()))
    }
}

/// Equivalent resolver with a distinct implementation type.
#[derive(Clone, Copy)]
struct MirrorResolver;

impl ContentResolver<&'static str> for MirrorResolver {
    type Error = FixtureError;
    fn resolve(&self, r: &&'static str) -> Result<Content, Self::Error> {
        Ok(Content {
            media_type: "text/plain".into(),
            body: r.as_bytes().to_vec(),
        })
    }
    fn fingerprint(&self, r: &&'static str) -> Result<BlockHash, Self::Error> {
        Ok(hash_bytes(r.as_bytes()))
    }
}

/// Always fails to resolve.
#[derive(Clone, Copy)]
struct FailingResolver;

impl ContentResolver<&'static str> for FailingResolver {
    type Error = FixtureError;
    fn resolve(&self, _: &&'static str) -> Result<Content, Self::Error> {
        Err(FixtureError("resolver unavailable".into()))
    }
    fn fingerprint(&self, _: &&'static str) -> Result<BlockHash, Self::Error> {
        Err(FixtureError("resolver unavailable".into()))
    }
}

/// Resolves but returns empty media type.
#[derive(Clone, Copy)]
struct UnusableResolver;

impl ContentResolver<&'static str> for UnusableResolver {
    type Error = FixtureError;
    fn resolve(&self, r: &&'static str) -> Result<Content, Self::Error> {
        Ok(Content {
            media_type: String::new(),
            body: r.as_bytes().to_vec(),
        })
    }
    fn fingerprint(&self, r: &&'static str) -> Result<BlockHash, Self::Error> {
        Ok(hash_bytes(r.as_bytes()))
    }
}

#[derive(Clone, Copy)]
struct AliasResolver;

impl ContentResolver<&'static str> for AliasResolver {
    type Error = FixtureError;
    fn resolve(&self, r: &&'static str) -> Result<Content, Self::Error> {
        let body = match *r {
            "alpha-alias-1" | "alpha-alias-2" => b"alpha".to_vec(),
            other => other.as_bytes().to_vec(),
        };
        Ok(Content {
            media_type: "text/plain".into(),
            body,
        })
    }
    fn fingerprint(&self, r: &&'static str) -> Result<BlockHash, Self::Error> {
        Ok(hash_bytes(r.as_bytes()))
    }
}

/// Derives a 2-byte i8 embedding from content: (first_byte, len_byte).
#[derive(Clone, Copy)]
struct AsciiEmbeddingProvider;

impl EmbeddingProvider for AsciiEmbeddingProvider {
    type Error = FixtureError;
    async fn embed(
        &self,
        input: &EmbeddingInput,
        spec: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        if spec.encoding != "i8" || spec.dims != 2 {
            return Err(FixtureError("unexpected embedding spec".into()));
        }
        let first = *input
            .body
            .first()
            .ok_or_else(|| FixtureError("empty content body".into()))?;
        Ok(vec![first, input.body.len() as u8])
    }
}

/// Always fails to embed.
#[derive(Clone, Copy)]
struct FailingEmbeddingProvider;

impl EmbeddingProvider for FailingEmbeddingProvider {
    type Error = FixtureError;
    async fn embed(&self, _: &EmbeddingInput, _: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
        Err(FixtureError("embedding model offline".into()))
    }
}

/// Returns wrong-length embeddings.
#[derive(Clone, Copy)]
struct WrongLengthEmbeddingProvider;

impl EmbeddingProvider for WrongLengthEmbeddingProvider {
    type Error = FixtureError;
    async fn embed(&self, _: &EmbeddingInput, _: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![0x01])
    }
}

/// Returns the same all-zero 2-byte embedding (triggers non-zero-norm failure
/// in DCBC when used for multi-item clustering).
#[derive(Clone, Copy)]
struct ZeroEmbeddingProvider;

impl EmbeddingProvider for ZeroEmbeddingProvider {
    type Error = FixtureError;
    async fn embed(
        &self,
        _: &EmbeddingInput,
        spec: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        if spec.encoding == "i8" && spec.dims == 2 {
            Ok(vec![0x00, 0x00])
        } else {
            Err(FixtureError("unexpected embedding spec".into()))
        }
    }
}

/// Returns the first entry's embedding as the canonical embedding.
#[derive(Clone, Copy)]
struct FirstChildCanonicalPolicy;

impl CanonicalEmbeddingPolicy for FirstChildCanonicalPolicy {
    type Error = FixtureError;
    fn canonical_embedding(&self, block: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
        Ok(block.entries[0].embedding.clone())
    }
}

/// Always fails canonical embedding.
#[derive(Clone, Copy)]
struct FailingCanonicalPolicy;

impl CanonicalEmbeddingPolicy for FailingCanonicalPolicy {
    type Error = FixtureError;
    fn canonical_embedding(&self, _: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
        Err(FixtureError("canonical policy failed".into()))
    }
}

/// Returns a single-byte embedding (wrong length).
#[derive(Clone, Copy)]
#[allow(dead_code)]
struct WrongLengthCanonicalPolicy;

impl CanonicalEmbeddingPolicy for WrongLengthCanonicalPolicy {
    type Error = FixtureError;
    fn canonical_embedding(&self, _: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![0x01])
    }
}

// ─── A custom clustering factory that always groups into pairs/triples ────────

use lexongraph_dcbc_streaming::DcbcStreamingTrainer;

/// Wraps DCBC streaming but forces cluster_count = 2 regardless of hints.
#[derive(Clone, Copy)]
struct PairClusteringFactory;

impl StreamingClusteringFactory for PairClusteringFactory {
    type Trainer = DcbcStreamingTrainer;
    type Error = StreamingClusteringError;

    fn create_trainer(
        &self,
        dimensions: usize,
        _: usize,
        _: usize,
        _: &EmbeddingSpec,
    ) -> Result<DcbcStreamingTrainer, StreamingClusteringError> {
        DcbcStreamingTrainer::new(StreamingClusteringConfig {
            cluster_count: 2,
            dimensions,
            balance_constraints: None,
            random_seed: None,
        })
    }
}

#[derive(Clone, Copy)]
struct SparseIdClusteringFactory;

#[derive(Clone)]
struct SparseIdTrainer {
    config: StreamingClusteringConfig,
    observed_count: usize,
}

#[derive(Clone)]
struct SparseIdClassifier {
    config: StreamingClusteringConfig,
}

#[derive(Clone, Copy)]
struct ShortAssignClusteringFactory;

#[derive(Clone)]
struct ShortAssignTrainer {
    config: StreamingClusteringConfig,
    observed_count: usize,
}

#[derive(Clone)]
struct ShortAssignClassifier {
    config: StreamingClusteringConfig,
}

#[derive(Clone, Copy)]
struct BadObservedCountFactory;

#[derive(Clone)]
struct BadObservedCountTrainer {
    config: StreamingClusteringConfig,
    observed_count: usize,
}

#[derive(Clone)]
struct BadObservedCountClassifier {
    config: StreamingClusteringConfig,
}

impl StreamingClusterClassifier for SparseIdClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        Ok(if embedding[0] < 99.0 { 0 } else { u32::MAX })
    }
}

impl StreamingClusterClassifier for ShortAssignClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn assign(&self, _: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        Ok(0)
    }

    fn assign_batch(
        &self,
        embeddings: &[Vec<f32>],
    ) -> Result<Vec<ClusterId>, StreamingClusteringError> {
        Ok(vec![0; embeddings.len().saturating_sub(1)])
    }
}

impl StreamingClusterClassifier for BadObservedCountClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn assign(&self, _: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        Ok(0)
    }
}

impl StreamingClusterTrainer for SparseIdTrainer {
    type Classifier = SparseIdClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn state(&self) -> TrainerState {
        TrainerState::Ingesting
    }

    fn ingest_batch(&mut self, embeddings: &[Vec<f32>]) -> Result<(), StreamingClusteringError> {
        self.observed_count += embeddings.len();
        Ok(())
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        Ok(PassReport {
            observed_count: self.observed_count,
            quality_metric: 0.0,
            balance_metric: 0.0,
            quality_direction: MetricDirection::LargerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0, u32::MAX],
        })
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        Ok(())
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        Ok(SparseIdClassifier {
            config: self.config,
        })
    }
}

impl StreamingClusterTrainer for ShortAssignTrainer {
    type Classifier = ShortAssignClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn state(&self) -> TrainerState {
        TrainerState::Ingesting
    }

    fn ingest_batch(&mut self, embeddings: &[Vec<f32>]) -> Result<(), StreamingClusteringError> {
        self.observed_count += embeddings.len();
        Ok(())
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        Ok(PassReport {
            observed_count: self.observed_count,
            quality_metric: 0.0,
            balance_metric: 0.0,
            quality_direction: MetricDirection::LargerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0],
        })
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        Ok(())
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        Ok(ShortAssignClassifier {
            config: self.config,
        })
    }
}

impl StreamingClusterTrainer for BadObservedCountTrainer {
    type Classifier = BadObservedCountClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn state(&self) -> TrainerState {
        TrainerState::Ingesting
    }

    fn ingest_batch(&mut self, embeddings: &[Vec<f32>]) -> Result<(), StreamingClusteringError> {
        self.observed_count += embeddings.len();
        Ok(())
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        Ok(PassReport {
            observed_count: self.observed_count + 1,
            quality_metric: 0.0,
            balance_metric: 0.0,
            quality_direction: MetricDirection::LargerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0],
        })
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        Ok(())
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        Ok(BadObservedCountClassifier {
            config: self.config,
        })
    }
}

impl StreamingClusteringFactory for SparseIdClusteringFactory {
    type Trainer = SparseIdTrainer;
    type Error = StreamingClusteringError;

    fn create_trainer(
        &self,
        dimensions: usize,
        _: usize,
        _: usize,
        _: &EmbeddingSpec,
    ) -> Result<Self::Trainer, StreamingClusteringError> {
        Ok(SparseIdTrainer {
            config: StreamingClusteringConfig {
                cluster_count: 2,
                dimensions,
                balance_constraints: None,
                random_seed: None,
            },
            observed_count: 0,
        })
    }
}

impl StreamingClusteringFactory for ShortAssignClusteringFactory {
    type Trainer = ShortAssignTrainer;
    type Error = StreamingClusteringError;

    fn create_trainer(
        &self,
        dimensions: usize,
        _: usize,
        _: usize,
        _: &EmbeddingSpec,
    ) -> Result<Self::Trainer, StreamingClusteringError> {
        Ok(ShortAssignTrainer {
            config: StreamingClusteringConfig {
                cluster_count: 2,
                dimensions,
                balance_constraints: None,
                random_seed: None,
            },
            observed_count: 0,
        })
    }
}

impl StreamingClusteringFactory for BadObservedCountFactory {
    type Trainer = BadObservedCountTrainer;
    type Error = StreamingClusteringError;

    fn create_trainer(
        &self,
        dimensions: usize,
        _: usize,
        _: usize,
        _: &EmbeddingSpec,
    ) -> Result<Self::Trainer, StreamingClusteringError> {
        Ok(BadObservedCountTrainer {
            config: StreamingClusteringConfig {
                cluster_count: 1,
                dimensions,
                balance_constraints: None,
                random_seed: None,
            },
            observed_count: 0,
        })
    }
}

// ─── Helper functions ─────────────────────────────────────────────────────────

fn hash_bytes(bytes: &[u8]) -> BlockHash {
    let digest = Sha256::digest(bytes);
    let mut hash = [0_u8; BlockHash::LEN];
    hash.copy_from_slice(&digest);
    BlockHash::from_bytes(hash)
}

fn embedding_spec() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: "i8".into(),
    }
}

fn item(s: &'static str) -> IndexItem<&'static str> {
    IndexItem {
        metadata: vec![],
        content_ref: s,
    }
}

fn dcbc_builtin_clustering() -> BuiltInClustering {
    BuiltInClustering::Dcbc(DcbcBuiltInClusteringSettings {
        cluster_count: 2,
        balance_constraints: None,
        random_seed: None,
    })
}

fn directional_pca_builtin_clustering() -> BuiltInClustering {
    BuiltInClustering::DirectionalPca(DirectionalPcaBuiltInClusteringSettings {
        cluster_count: 1,
        balance_constraints: None,
        random_seed: None,
        params: DirectionalPcaParams {
            retained_dimension_count: 1,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    })
}

#[derive(Clone)]
struct BuiltInAlgorithmCase {
    name: &'static str,
    clustering: BuiltInClustering,
}

fn built_in_algorithm_cases() -> [BuiltInAlgorithmCase; 2] {
    [
        BuiltInAlgorithmCase {
            name: "dcbc",
            clustering: dcbc_builtin_clustering(),
        },
        BuiltInAlgorithmCase {
            name: "directional-pca",
            clustering: directional_pca_builtin_clustering(),
        },
    ]
}

fn run_with_builtin<RS, EP>(
    resolver: RS,
    embedding_provider: EP,
    clustering: BuiltInClustering,
    embedding_spec: EmbeddingSpec,
    block_size_target: usize,
) -> StreamingIndexingRun<
    &'static str,
    RS,
    EP,
    ArithmeticMeanCanonicalEmbeddingPolicy,
    BuiltInClusteringFactory,
> {
    StreamingIndexingRun::with_builtin_clustering(
        resolver,
        embedding_provider,
        clustering,
        embedding_spec,
        block_size_target,
    )
}

fn run_with_builtin_dcbc<RS, EP>(
    resolver: RS,
    embedding_provider: EP,
    embedding_spec: EmbeddingSpec,
    block_size_target: usize,
) -> StreamingIndexingRun<
    &'static str,
    RS,
    EP,
    ArithmeticMeanCanonicalEmbeddingPolicy,
    BuiltInClusteringFactory,
> {
    StreamingIndexingRun::with_builtin_clustering(
        resolver,
        embedding_provider,
        dcbc_builtin_clustering(),
        embedding_spec,
        block_size_target,
    )
}

fn run_with_builtin_dcbc_and_canonical<RS, EP, CEP>(
    resolver: RS,
    embedding_provider: EP,
    canonical_embedding_policy: CEP,
    embedding_spec: EmbeddingSpec,
    block_size_target: usize,
) -> StreamingIndexingRun<&'static str, RS, EP, CEP, BuiltInClusteringFactory> {
    StreamingIndexingRun::with_canonical_policy(
        resolver,
        embedding_provider,
        canonical_embedding_policy,
        dcbc_builtin_clustering(),
        embedding_spec,
        block_size_target,
    )
}

async fn one_shot_index_with_clustering(
    clustering: BuiltInClustering,
    items: &[IndexItem<&'static str>],
    block_size_target: usize,
) -> Result<(StreamingIndexingResult, MemoryBlockStore), StreamingIndexerError> {
    let store = MemoryBlockStore::default();
    let mut run = run_with_builtin(
        MapResolver,
        AsciiEmbeddingProvider,
        clustering,
        embedding_spec(),
        block_size_target,
    );
    run.ingest_batch(items).await?;
    run.finish_pass()?;
    run.mark_training_complete()?;
    let result = run.finalize(std::iter::once(items), &store).await?;
    Ok((result, store))
}

/// Run one training pass + mark_complete + finalize for a set of items.
async fn one_shot_index(
    items: &[IndexItem<&'static str>],
    block_size_target: usize,
) -> Result<(StreamingIndexingResult, MemoryBlockStore), StreamingIndexerError> {
    let store = MemoryBlockStore::default();
    let mut run = run_with_builtin_dcbc(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        block_size_target,
    );
    run.ingest_batch(items).await?;
    run.finish_pass()?;
    run.mark_training_complete()?;
    let result = run.finalize(std::iter::once(items), &store).await?;
    Ok((result, store))
}

async fn for_each_builtin_algorithm_case<F, Fut>(mut f: F)
where
    F: FnMut(BuiltInAlgorithmCase) -> Fut,
    Fut: Future<Output = ()>,
{
    for case in built_in_algorithm_cases() {
        f(case).await;
    }
}

// ─── VAL-STREAM-INDEXER-001 ───────────────────────────────────────────────────
// Crate and spec exist; the streaming spec defines its own normative boundary.

#[test]
fn val_stream_indexer_001_crate_and_spec_define_direct_boundary() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("crate should be nested under <repo>/crates/<crate>");
    let requirements_path = repo_root
        .join("docs")
        .join("specs")
        .join("rust-streaming-indexer-crate")
        .join("requirements.md");
    let validation_path = repo_root
        .join("docs")
        .join("specs")
        .join("rust-streaming-indexer-crate")
        .join("validation.md");

    assert!(
        repo_root
            .join("crates")
            .join("lexongraph-streaming-indexer")
            .join("Cargo.toml")
            .exists(),
        "streaming-indexer crate should remain present"
    );
    assert!(
        requirements_path.exists(),
        "streaming-indexer spec package should remain present"
    );
    assert!(
        validation_path.exists(),
        "streaming-indexer validation spec should remain present"
    );

    let requirements = std::fs::read_to_string(&requirements_path)
        .expect("streaming-indexer requirements should be readable");
    let validation = std::fs::read_to_string(&validation_path)
        .expect("streaming-indexer validation should be readable");
    let req_stream_indexer_003 = markdown_section(&requirements, "REQ-STREAM-INDEXER-003");
    let val_stream_indexer_001 = markdown_section(&validation, "VAL-STREAM-INDEXER-001");

    assert!(
        req_stream_indexer_003.contains("docs/protocol/indexing.md")
            && req_stream_indexer_003.contains("docs/protocol/blocks.md"),
        "REQ-STREAM-INDEXER-003 must anchor the streaming line to the indexing and block protocols"
    );
    assert!(
        req_stream_indexer_003.contains("legacy batch-oriented")
            && req_stream_indexer_003.contains("normative conformance boundary"),
        "REQ-STREAM-INDEXER-003 must exclude the legacy batch indexer line from the normative boundary"
    );
    assert!(
        val_stream_indexer_001.contains("retired legacy")
            && val_stream_indexer_001.contains("batch-oriented indexing")
            && val_stream_indexer_001.contains("artifacts")
            && val_stream_indexer_001.contains("does not depend on")
            && val_stream_indexer_001.contains("remaining present"),
        "VAL-STREAM-INDEXER-001 must not require legacy batch artifacts to remain present"
    );
    let _ = include_str!("../src/lib.rs");
}

// ─── VAL-STREAM-INDEXER-002 ───────────────────────────────────────────────────
// Public surface exposes the replay lifecycle and consumes shared clustering.

#[test]
fn val_stream_indexer_002_public_surface_inspection() {
    // Verify that the shared streaming contract and both built-in clustering
    // dependencies are declared.
    let manifest = include_str!("../Cargo.toml");
    assert!(
        manifest.contains("lexongraph-dcbc-streaming"),
        "Cargo.toml must depend on lexongraph-dcbc-streaming"
    );
    assert!(
        manifest.contains("lexongraph-directional-pca"),
        "Cargo.toml must depend on lexongraph-directional-pca"
    );
    assert!(
        manifest.contains("lexongraph-streaming-clustering"),
        "Cargo.toml must depend on lexongraph-streaming-clustering"
    );
}

// ─── VAL-STREAM-INDEXER-003 ───────────────────────────────────────────────────
// Empty pass and empty logical run fail explicitly.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_003_empty_pass_fails_explicitly() {
    for_each_builtin_algorithm_case(|case| async move {
        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        let err = run.finish_pass().unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::EmptyPass(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_003_empty_item_list_in_finalize_fails_explicitly() {
    for_each_builtin_algorithm_case(|case| async move {
        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        run.ingest_batch(&[item("alpha"), item("bravo")])
            .await
            .unwrap();
        run.finish_pass().unwrap();
        run.mark_training_complete().unwrap();
        let err = run
            .finalize(
                std::iter::empty::<&[IndexItem<&'static str>]>(),
                &MemoryBlockStore::default(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                StreamingIndexerError::ReplayMismatch(_) | StreamingIndexerError::EmptyInput
            ),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-004 ───────────────────────────────────────────────────
// IndexItem carries metadata + content_ref; no inline bytes.

#[test]
fn val_stream_indexer_004_index_item_shape() {
    let it: IndexItem<&str> = IndexItem {
        metadata: vec![],
        content_ref: "hello",
    };
    assert_eq!(it.content_ref, "hello");
}

// ─── VAL-STREAM-INDEXER-005 ───────────────────────────────────────────────────
// Different resolver types share the same indexer contract.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_005_distinct_resolver_types_share_the_same_contract() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo")];
        let (result_map, _) = one_shot_index_with_clustering(case.clustering.clone(), &items, 256)
            .await
            .unwrap();

        let store2 = MemoryBlockStore::default();
        let mut run2 = run_with_builtin(
            MirrorResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        run2.ingest_batch(&items).await.unwrap();
        run2.finish_pass().unwrap();
        run2.mark_training_complete().unwrap();
        let result2 = run2
            .finalize(std::iter::once(items.as_slice()), &store2)
            .await
            .unwrap();

        assert_eq!(result_map.root_id, result2.root_id, "{}", case.name);
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-006 ───────────────────────────────────────────────────
// Embedding provider consumed through shared embeddings-trait contract.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_006_uses_shared_embeddings_trait_contract() {
    for_each_builtin_algorithm_case(|case| async move {
        let mut run = run_with_builtin(
            MapResolver,
            FailingEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        let err = run.ingest_batch(&[item("alpha")]).await.unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::EmbeddingFailure(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-007 ───────────────────────────────────────────────────
// Built-in directional-PCA selection requires explicit caller-provided settings.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_007_directional_pca_builtin_selection_requires_explicit_settings() {
    let store = MemoryBlockStore::default();
    let mut run = StreamingIndexingRun::with_builtin_clustering(
        MapResolver,
        AsciiEmbeddingProvider,
        directional_pca_builtin_clustering(),
        embedding_spec(),
        256,
    );
    let items = [item("alpha"), item("bravo")];
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    let result = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();
    assert!(!result.block_ids.is_empty());
    assert!(store.get(&result.root_id).unwrap().is_some());
}

// ─── VAL-STREAM-INDEXER-008 ───────────────────────────────────────────────────
// Override path accepts caller-supplied canonical and clustering policies.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_008_override_path_accepts_custom_policies() {
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let store = MemoryBlockStore::default();

    // with_canonical_policy: override canonical while still selecting a built-in
    // clustering realization explicitly.
    let mut run = run_with_builtin_dcbc_and_canonical(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    let result = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();
    assert!(!result.block_ids.is_empty());

    // with_new: fully explicit override.
    let store2 = MemoryBlockStore::default();
    let mut run2 = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairClusteringFactory,
        embedding_spec(),
        256,
    );
    run2.ingest_batch(&items).await.unwrap();
    run2.finish_pass().unwrap();
    run2.mark_training_complete().unwrap();
    let result2 = run2
        .finalize(std::iter::once(items.as_slice()), &store2)
        .await
        .unwrap();
    assert!(!result2.block_ids.is_empty());
}

// ─── VAL-STREAM-INDEXER-009 ───────────────────────────────────────────────────
// Pass report is deterministic and contains item count + fitness info.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_009_pass_report_deterministic_with_multiple_batches() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering.clone(),
            embedding_spec(),
            256,
        );
        run.ingest_batch(&items[..2]).await.unwrap();
        run.ingest_batch(&items[2..]).await.unwrap();
        let report = run.finish_pass().unwrap();

        assert_eq!(report.observed_item_count, 4, "{}", case.name);
        assert_eq!(report.completed_pass_count, 1, "{}", case.name);

        let mut run2 = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        run2.ingest_batch(&items).await.unwrap();
        let report2 = run2.finish_pass().unwrap();

        assert_eq!(
            report.observed_item_count, report2.observed_item_count,
            "{}",
            case.name
        );
        assert_eq!(
            report.clustering_quality_metric, report2.clustering_quality_metric,
            "{}",
            case.name
        );
        assert_eq!(
            report.clustering_quality_direction, report2.clustering_quality_direction,
            "{}",
            case.name
        );
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-010 ───────────────────────────────────────────────────
// Two identical passes are accepted.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_010_two_identical_passes_accepted() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo")];
        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        run.ingest_batch(&items).await.unwrap();
        let r1 = run.finish_pass().unwrap();

        run.ingest_batch(&items).await.unwrap();
        let r2 = run.finish_pass().unwrap();

        assert_eq!(
            r1.observed_item_count, r2.observed_item_count,
            "{}",
            case.name
        );
        assert_eq!(r2.completed_pass_count, 2, "{}", case.name);
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_011_later_pass_with_different_content_reference_fails() {
    for_each_builtin_algorithm_case(|case| async move {
        let first = IndexItem {
            metadata: vec![],
            content_ref: "alpha-alias-1",
        };
        let second = IndexItem {
            metadata: vec![],
            content_ref: "alpha-alias-2",
        };
        let mut run = run_with_builtin(
            AliasResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );

        run.ingest_batch(&[first, item("bravo")]).await.unwrap();
        run.finish_pass().unwrap();
        let err = run.ingest_batch(&[second]).await.unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::ReplayMismatch(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn metadata_order_differences_do_not_break_replay_equivalence() {
    use ciborium::Value;

    let first = IndexItem {
        metadata: vec![
            (Value::Text("b".into()), Value::Integer(2.into())),
            (Value::Text("a".into()), Value::Integer(1.into())),
        ],
        content_ref: "alpha",
    };
    let second = IndexItem {
        metadata: vec![
            (Value::Text("a".into()), Value::Integer(1.into())),
            (Value::Text("b".into()), Value::Integer(2.into())),
        ],
        content_ref: "alpha",
    };

    for_each_builtin_algorithm_case(|case| {
        let first = first.clone();
        let second = second.clone();
        async move {
            let mut run = run_with_builtin(
                MapResolver,
                AsciiEmbeddingProvider,
                case.clustering,
                embedding_spec(),
                256,
            );
            run.ingest_batch(&[first.clone(), item("bravo")])
                .await
                .unwrap();
            run.finish_pass().unwrap();
            run.ingest_batch(&[second.clone(), item("bravo")])
                .await
                .unwrap();
            run.finish_pass().unwrap();
        }
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-011 ───────────────────────────────────────────────────
// Later pass with different items fails.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_011_later_pass_with_different_items_fails() {
    for_each_builtin_algorithm_case(|case| async move {
        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        run.ingest_batch(&[item("alpha"), item("bravo")])
            .await
            .unwrap();
        run.finish_pass().unwrap();

        let err = run
            .ingest_batch(&[item("alpha"), item("DIFFERENT")])
            .await
            .unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::ReplayMismatch(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_011_later_pass_with_more_items_fails() {
    for_each_builtin_algorithm_case(|case| async move {
        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        run.ingest_batch(&[item("alpha"), item("bravo")])
            .await
            .unwrap();
        run.finish_pass().unwrap();

        run.ingest_batch(&[item("alpha"), item("bravo")])
            .await
            .unwrap();
        let err = run.ingest_batch(&[item("extra")]).await.unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::ReplayMismatch(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-012 ───────────────────────────────────────────────────
// Finalize before training completion fails.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_012_finalize_before_training_complete_fails() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo")];
        let store = MemoryBlockStore::default();

        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering.clone(),
            embedding_spec(),
            256,
        );
        let err = run
            .finalize(std::iter::once(items.as_slice()), &store)
            .await
            .unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::InvalidLifecycleTransition(_)),
            "{}: {err}",
            case.name
        );

        let mut run2 = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        run2.ingest_batch(&items).await.unwrap();
        run2.finish_pass().unwrap();
        let err2 = run2
            .finalize(std::iter::once(items.as_slice()), &store)
            .await
            .unwrap_err();
        assert!(
            matches!(err2, StreamingIndexerError::InvalidLifecycleTransition(_)),
            "{}: {err2}",
            case.name
        );
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-013 ───────────────────────────────────────────────────
// Successful final materialization after training completion.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_013_successful_finalize_after_training_complete() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo")];
        let store = MemoryBlockStore::default();

        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap();
        run.mark_training_complete().unwrap();
        let result = run
            .finalize(std::iter::once(items.as_slice()), &store)
            .await
            .unwrap();

        assert!(!result.block_ids.is_empty(), "{}", case.name);
        assert!(
            store.get(&result.root_id).unwrap().is_some(),
            "{}",
            case.name
        );
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-014 ───────────────────────────────────────────────────
// Finalization with different item order or count fails.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_014_finalize_with_different_items_fails() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo")];
        let store = MemoryBlockStore::default();

        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap();
        run.mark_training_complete().unwrap();

        let err = run
            .finalize(std::iter::once([item("alpha")].as_slice()), &store)
            .await
            .unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::ReplayMismatch(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_014_finalize_with_wrong_order_fails() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo")];
        let store = MemoryBlockStore::default();

        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap();
        run.mark_training_complete().unwrap();

        let err = run
            .finalize(
                std::iter::once([item("bravo"), item("alpha")].as_slice()),
                &store,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::ReplayMismatch(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-015 ───────────────────────────────────────────────────
// Leaf entry stores resolved media type and bytes inline.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_015_resolved_content_stored_inline_in_leaf() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo")];
        let (result, store) = one_shot_index_with_clustering(case.clustering, &items, 256)
            .await
            .unwrap();

        let mut found_alpha_leaf = false;
        for id in &result.block_ids {
            let block = store.get(id).unwrap().unwrap();
            if let TypedEntries::Leaf(_, entries) = into_entries(block)
                && entries[0].content.body == b"alpha".to_vec()
            {
                assert_eq!(entries[0].content.media_type, "text/plain", "{}", case.name);
                found_alpha_leaf = true;
                break;
            }
        }
        assert!(found_alpha_leaf, "{}: missing alpha leaf block", case.name);
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-016 ───────────────────────────────────────────────────
// Single item → one leaf block which is the root.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_016_single_item_produces_leaf_root() {
    let (result, store) = one_shot_index(&[item("alpha")], 256).await.unwrap();

    assert_eq!(result.block_ids.len(), 1);
    let root = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(root) {
        TypedEntries::Leaf(meta, entries) => {
            assert_eq!(meta.level, 0);
            assert_eq!(entries.len(), 1);
        }
        TypedEntries::Branch(_, _) => panic!("expected leaf root"),
    }
}

// ─── VAL-STREAM-INDEXER-017 ───────────────────────────────────────────────────
// Multiple items build parent layers until one root remains.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_017_multiple_items_produce_single_root() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
        let (result, store) = one_shot_index_with_clustering(case.clustering, &items, 256)
            .await
            .unwrap();

        assert!(
            store.get(&result.root_id).unwrap().is_some(),
            "{}",
            case.name
        );
        for id in &result.block_ids {
            assert!(
                store.get(id).unwrap().is_some(),
                "{}: missing block {id}",
                case.name
            );
        }
        assert!(result.block_ids.len() >= items.len(), "{}", case.name);
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-018 ───────────────────────────────────────────────────
// Branch entries are sorted by embedding bytes and deduplicated by child ID.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_018_branch_entries_sorted_and_deduplicated() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("alpha"), item("bravo"), item("charlie")];
        let (result, store) = one_shot_index_with_clustering(case.clustering, &items, 256)
            .await
            .unwrap();

        for id in &result.block_ids {
            let validated = store.get(id).unwrap().unwrap();
            if let TypedEntries::Branch(_, entries) = into_entries(validated) {
                for window in entries.windows(2) {
                    assert!(
                        window[0].embedding <= window[1].embedding,
                        "{}: entries not sorted by embedding",
                        case.name
                    );
                }
                let unique_children: HashSet<_> = entries.iter().map(|entry| entry.child).collect();
                assert_eq!(
                    unique_children.len(),
                    entries.len(),
                    "{}: duplicate child IDs in branch",
                    case.name
                );
            }
        }
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-019 ───────────────────────────────────────────────────
// Block size target is enforced; failing when no conforming block can be built.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_019_intermediate_nodes_respect_size_target() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo"), item("charlie")];
        let (result, store) = one_shot_index_with_clustering(case.clustering.clone(), &items, 256)
            .await
            .unwrap();

        for id in &result.block_ids {
            let validated = store.get(id).unwrap().unwrap();
            let bytes = serialize_block(&validated.block).unwrap().bytes;
            if matches!(validated.block, lexongraph_block::Block::Branch(_)) {
                assert!(
                    bytes.len() <= 256,
                    "{}: branch block {} serializes to {} bytes, exceeds 256",
                    case.name,
                    id,
                    bytes.len()
                );
            }
        }

        let err =
            one_shot_index_with_clustering(case.clustering, &[item("alpha"), item("bravo")], 24)
                .await
                .unwrap_err();
        assert!(
            matches!(
                err,
                StreamingIndexerError::IntermediateNodeTooLarge { .. }
                    | StreamingIndexerError::ClusteringFailure(_)
            ),
            "{}: expected size-target failure, got: {err}",
            case.name
        );
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-020 ───────────────────────────────────────────────────
// Higher-layer construction uses streaming clustering contract (inspect).

#[test]
fn val_stream_indexer_020_higher_layers_use_streaming_clustering() {
    // Static: the lib.rs higher-layer path calls factory.create_trainer,
    // which returns a StreamingClusterTrainer.  Verified by reading the source.
    let src = include_str!("../src/lib.rs");
    assert!(
        src.contains("HigherLayerClustering"),
        "lib.rs must reference HigherLayerClustering phase for higher layers"
    );
    assert!(
        src.contains("create_trainer"),
        "lib.rs must call factory.create_trainer for higher layers"
    );
}

// ─── VAL-STREAM-INDEXER-021 ───────────────────────────────────────────────────
// Same logical set produces the same result twice.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_021_deterministic_across_two_runs() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
        let (r1, _) = one_shot_index_with_clustering(case.clustering.clone(), &items, 256)
            .await
            .unwrap();
        let (r2, _) = one_shot_index_with_clustering(case.clustering, &items, 256)
            .await
            .unwrap();

        assert_eq!(
            r1.root_id, r2.root_id,
            "{}: root IDs must be equal",
            case.name
        );
        assert_eq!(
            r1.block_ids, r2.block_ids,
            "{}: block ID sets must be equal",
            case.name
        );
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-022 ───────────────────────────────────────────────────
// Various explicit failure modes.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_content_resolution_failure_is_explicit() {
    for_each_builtin_algorithm_case(|case| async move {
        let mut run = run_with_builtin(
            FailingResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        let err = run.ingest_batch(&[item("alpha")]).await.unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::ContentResolution(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_unusable_content_failure_is_explicit() {
    for_each_builtin_algorithm_case(|case| async move {
        let mut run = run_with_builtin(
            UnusableResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        let err = run.ingest_batch(&[item("alpha")]).await.unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::UnusableContent(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_invalid_metadata_failure_is_explicit() {
    use ciborium::Value;

    for_each_builtin_algorithm_case(|case| async move {
        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        let err = run
            .ingest_batch(&[IndexItem {
                metadata: vec![
                    (Value::Text("dup".into()), Value::Integer(1.into())),
                    (Value::Text("dup".into()), Value::Integer(2.into())),
                ],
                content_ref: "alpha",
            }])
            .await
            .unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::InvalidMetadata(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_embedding_failure_is_explicit() {
    for_each_builtin_algorithm_case(|case| async move {
        let mut run = run_with_builtin(
            MapResolver,
            FailingEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        let err = run.ingest_batch(&[item("alpha")]).await.unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::EmbeddingFailure(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_wrong_length_embedding_failure_is_explicit() {
    for_each_builtin_algorithm_case(|case| async move {
        let mut run = run_with_builtin(
            MapResolver,
            WrongLengthEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        let err = run.ingest_batch(&[item("alpha")]).await.unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::EmbeddingFailure(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_canonical_embedding_failure_is_explicit() {
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let store = MemoryBlockStore::default();

    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FailingCanonicalPolicy,
        PairClusteringFactory,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    let err = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::CanonicalEmbeddingFailure(_)),
        "{err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_clustering_failure_on_zero_norm_is_explicit() {
    // Zero-norm embeddings are rejected by DCBC with MalformedInput.
    // With the buffered design, the trainer is fed in finish_pass.
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let mut run = run_with_builtin_dcbc(MapResolver, ZeroEmbeddingProvider, embedding_spec(), 96);
    // ingest_batch succeeds (buffers embeddings, no trainer yet)
    run.ingest_batch(&items).await.unwrap();
    // finish_pass feeds to trainer → zero-norm detected
    let err = run.finish_pass().unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::ClusteringFailure(_)),
        "{err}"
    );
    let err_again = run.finish_pass().unwrap_err();
    assert!(
        matches!(err_again, StreamingIndexerError::ClusteringFailure(_)),
        "{err_again}"
    );
}

// ─── VAL-STREAM-INDEXER-023 ───────────────────────────────────────────────────
// Status observer receives structured start/progress/completion updates.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_023_observer_receives_structured_status_updates() {
    use lexongraph_streaming_indexer::StreamingIndexingPhase;
    use lexongraph_streaming_indexer::StreamingIndexingStatusState;

    let log: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let log_clone = Arc::clone(&log);
    let observer: StreamingIndexingStatusObserver =
        Arc::new(move |s| log_clone.lock().unwrap().push(s));

    for_each_builtin_algorithm_case(|case| {
        let observer = Arc::clone(&observer);
        async move {
            let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
            let store = MemoryBlockStore::default();

            let mut run = run_with_builtin(
                MapResolver,
                AsciiEmbeddingProvider,
                case.clustering,
                embedding_spec(),
                256,
            )
            .with_observer(observer);

            run.ingest_batch(&items).await.unwrap();
            run.finish_pass().unwrap();
            run.mark_training_complete().unwrap();
            run.finalize(std::iter::once(items.as_slice()), &store)
                .await
                .unwrap();
        }
    })
    .await;

    let captured = log.lock().unwrap();
    let has_pass_complete = captured.iter().any(|s| {
        matches!(s.phase, StreamingIndexingPhase::TrainingPass { .. })
            && s.state == StreamingIndexingStatusState::Completed
    });
    let has_leaf_start = captured.iter().any(|s| {
        s.phase == StreamingIndexingPhase::LeafMaterialization
            && s.state == StreamingIndexingStatusState::Started
    });
    let has_leaf_complete = captured.iter().any(|s| {
        s.phase == StreamingIndexingPhase::LeafMaterialization
            && s.state == StreamingIndexingStatusState::Completed
    });
    let has_in_progress = captured
        .iter()
        .any(|s| s.state == StreamingIndexingStatusState::InProgress);
    let all_started_are_zero = captured
        .iter()
        .filter(|s| s.state == StreamingIndexingStatusState::Started)
        .all(|s| s.elapsed.is_zero());

    assert!(
        has_pass_complete,
        "no TrainingPass Completed event recorded"
    );
    assert!(
        has_leaf_start,
        "no LeafMaterialization Started event recorded"
    );
    assert!(
        has_leaf_complete,
        "no LeafMaterialization Completed event recorded"
    );
    assert!(has_in_progress, "no InProgress event recorded");
    assert!(
        all_started_are_zero,
        "Started events should report zero elapsed time"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn leaf_store_integrity_mismatch_is_explicit() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo")];
        let store = FaultyIdStore::corrupt_leaf_ids();
        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );

        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap();
        run.mark_training_complete().unwrap();
        let error = run
            .finalize(std::iter::once(items.as_slice()), &store)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                StreamingIndexerError::Storage(BlockStoreError::IntegrityMismatch { .. })
            ),
            "{}: expected explicit leaf integrity mismatch, got: {error}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn branch_store_integrity_mismatch_is_explicit() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo"), item("charlie")];
        let store = FaultyIdStore::corrupt_branch_ids();
        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );

        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap();
        run.mark_training_complete().unwrap();
        let error = run
            .finalize(std::iter::once(items.as_slice()), &store)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                StreamingIndexerError::Storage(BlockStoreError::IntegrityMismatch { .. })
            ),
            "{}: expected explicit branch integrity mismatch, got: {error}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_failure_does_not_consume_training_complete_state() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo")];
        let store = FailOnceStore::new();
        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );

        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap();
        run.mark_training_complete().unwrap();

        let error = run
            .finalize(std::iter::once(items.as_slice()), &store)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                StreamingIndexerError::Storage(BlockStoreError::BackendFailure(_))
            ),
            "{}: expected transient storage failure, got: {error}",
            case.name
        );

        let result = run
            .finalize(std::iter::once(items.as_slice()), &store)
            .await
            .unwrap();
        assert!(
            !result.block_ids.is_empty(),
            "{}: retry should succeed",
            case.name
        );
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-024 ───────────────────────────────────────────────────
// Conformance helpers are behind the non-default "conformance" feature.

#[test]
fn val_stream_indexer_024_conformance_helpers_are_feature_gated() {
    let manifest = include_str!("../Cargo.toml");
    assert!(
        manifest.contains("conformance"),
        "Cargo.toml must declare a conformance feature"
    );
    // The conformance module must NOT be compiled without the feature —
    // the compile_fail doctest in lib.rs guards against that.
}

#[test]
fn derived_cluster_count_overflow_is_explicit() {
    let factory = DcbcStreamingClusteringFactory::new(8);
    let spec = embedding_spec();
    let error = factory
        .create_trainer(spec.dims as usize, usize::MAX, 256, &spec)
        .unwrap_err();

    assert!(
        matches!(error, StreamingClusteringError::InvalidConfiguration { .. }),
        "expected explicit invalid configuration, got: {error}"
    );
}

#[test]
fn impossible_min_two_children_grouping_fails_early() {
    let factory = DcbcStreamingClusteringFactory::new(8);
    let spec = embedding_spec();
    let error = factory
        .create_trainer(spec.dims as usize, 3, 24, &spec)
        .unwrap_err();

    assert!(
        matches!(error, StreamingClusteringError::InvalidConfiguration { .. }),
        "expected explicit invalid configuration, got: {error}"
    );
}

#[test]
fn two_child_branch_capacity_failure_is_explicit() {
    let factory = DcbcStreamingClusteringFactory::new(8);
    let spec = embedding_spec();
    let error = factory
        .create_trainer(spec.dims as usize, 2, 24, &spec)
        .unwrap_err();

    assert!(
        matches!(error, StreamingClusteringError::InvalidConfiguration { .. }),
        "expected explicit invalid configuration, got: {error}"
    );
}

#[test]
fn unsupported_embedding_encoding_fails_explicitly_in_default_factory() {
    let factory = DcbcStreamingClusteringFactory::new(8);
    let spec = EmbeddingSpec {
        dims: 2,
        encoding: "bogus".into(),
    };
    let error = factory
        .create_trainer(spec.dims as usize, 8, 256, &spec)
        .unwrap_err();

    assert!(
        matches!(error, StreamingClusteringError::InvalidConfiguration { .. }),
        "expected explicit invalid configuration, got: {error}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn sparse_cluster_ids_do_not_trigger_large_group_allocation() {
    let items = [item("alpha"), item("zulu"), item("bravo"), item("yankee")];
    let store = MemoryBlockStore::default();
    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        SparseIdClusteringFactory,
        embedding_spec(),
        256,
    );

    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    let result = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await;

    assert!(result.is_ok(), "sparse cluster IDs should still finalize");
}

#[tokio::test(flavor = "current_thread")]
async fn short_assignment_batch_fails_explicitly() {
    let items = [item("alpha"), item("bravo"), item("charlie")];
    let store = MemoryBlockStore::default();
    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        ShortAssignClusteringFactory,
        embedding_spec(),
        256,
    );

    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    let error = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap_err();

    assert!(
        matches!(error, StreamingIndexerError::ClusteringFailure(_)),
        "expected explicit clustering failure, got: {error}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn mismatched_observed_count_fails_explicitly() {
    let items = [item("alpha"), item("bravo")];
    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        BadObservedCountFactory,
        embedding_spec(),
        256,
    );

    run.ingest_batch(&items).await.unwrap();
    let error = run.finish_pass().unwrap_err();

    assert!(
        matches!(error, StreamingIndexerError::ClusteringFailure(_)),
        "expected explicit clustering failure, got: {error}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn mismatched_observed_count_emits_failed_status() {
    use lexongraph_streaming_indexer::StreamingIndexingPhase;
    use lexongraph_streaming_indexer::StreamingIndexingStatusState;

    let log: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let log_clone = Arc::clone(&log);
    let observer: StreamingIndexingStatusObserver =
        Arc::new(move |s| log_clone.lock().unwrap().push(s));

    let items = [item("alpha"), item("bravo")];
    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        BadObservedCountFactory,
        embedding_spec(),
        256,
    )
    .with_observer(observer);

    run.ingest_batch(&items).await.unwrap();
    let _ = run.finish_pass().unwrap_err();

    let captured = log.lock().unwrap();
    assert!(captured.iter().any(|s| {
        matches!(s.phase, StreamingIndexingPhase::TrainingPass { .. })
            && s.state == StreamingIndexingStatusState::Failed
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn short_assignment_batch_emits_failed_status() {
    use lexongraph_streaming_indexer::StreamingIndexingPhase;
    use lexongraph_streaming_indexer::StreamingIndexingStatusState;

    let log: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let log_clone = Arc::clone(&log);
    let observer: StreamingIndexingStatusObserver =
        Arc::new(move |s| log_clone.lock().unwrap().push(s));

    let items = [item("alpha"), item("bravo"), item("charlie")];
    let store = MemoryBlockStore::default();
    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        ShortAssignClusteringFactory,
        embedding_spec(),
        256,
    )
    .with_observer(observer);

    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    let _ = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap_err();

    let captured = log.lock().unwrap();
    assert!(captured.iter().any(|s| {
        s.phase == StreamingIndexingPhase::FirstLayerClustering
            && s.state == StreamingIndexingStatusState::Failed
    }));
}

// ─── VAL-STREAM-INDEXER-025 ───────────────────────────────────────────────────
// Repeated passes require caller replay; the crate does not rematerialize data.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_025_repeated_passes_require_caller_replay() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo")];

        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap();
        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap();
        run.mark_training_complete().unwrap();
        let result = run
            .finalize(
                std::iter::once(items.as_slice()),
                &MemoryBlockStore::default(),
            )
            .await
            .unwrap();
        assert!(!result.block_ids.is_empty(), "{}", case.name);
    })
    .await;
}

// ─── VAL-STREAM-INDEXER-026 ───────────────────────────────────────────────────
// Depends on both built-in clustering crates; built-in paths delegate through contract.

#[test]
fn val_stream_indexer_026_depends_on_both_builtin_clustering_crates() {
    let manifest = include_str!("../Cargo.toml");
    assert!(
        manifest.contains("lexongraph-dcbc-streaming"),
        "Cargo.toml must depend on lexongraph-dcbc-streaming"
    );
    assert!(
        manifest.contains("lexongraph-directional-pca"),
        "Cargo.toml must depend on lexongraph-directional-pca"
    );
    // The explicit DCBC factory remains available, and the built-in selection
    // wrapper must be constructible for both built-in choices.
    let _factory = DcbcStreamingClusteringFactory::new(2);
    let _built_in_dcbc = BuiltInClusteringFactory::new(dcbc_builtin_clustering());
    let _built_in_dpca = BuiltInClusteringFactory::new(directional_pca_builtin_clustering());
}

// ─── VAL-STREAM-INDEXER-027 ───────────────────────────────────────────────────
// Automated verification artifacts exist (this file IS the artifact).

#[test]
fn val_stream_indexer_027_verification_artifacts_exist() {
    // This test itself is the artifact.  Additionally verify the test file name.
    let src = include_str!("../tests/spec_validation.rs");
    assert!(
        src.contains("val_stream_indexer_001"),
        "spec_validation.rs must contain the first VAL entry test"
    );
}

// ─── Additional invariant tests ───────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn mark_training_complete_without_any_pass_fails() {
    for_each_builtin_algorithm_case(|case| async move {
        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        let err = run.mark_training_complete().unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::InvalidLifecycleTransition(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn mark_training_complete_with_open_pass_fails() {
    for_each_builtin_algorithm_case(|case| async move {
        let mut run = run_with_builtin(
            MapResolver,
            AsciiEmbeddingProvider,
            case.clustering,
            embedding_spec(),
            256,
        );
        run.ingest_batch(&[item("alpha")]).await.unwrap();
        let err = run.mark_training_complete().unwrap_err();
        assert!(
            matches!(err, StreamingIndexerError::InvalidLifecycleTransition(_)),
            "{}: {err}",
            case.name
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_items_collapse_to_single_leaf_root() {
    let items = [item("same"), item("same"), item("same")];
    let (result, store) = one_shot_index(&items, 256).await.unwrap();

    let root = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(root) {
        TypedEntries::Leaf(_, entries) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].content.body, b"same".to_vec());
        }
        TypedEntries::Branch(_, _) => panic!("duplicate items should collapse to leaf root"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn built_in_dcbc_selection_and_explicit_dcbc_factory_produce_same_result() {
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];

    let (r1, _) = one_shot_index(&items, 256).await.unwrap();

    let store2 = MemoryBlockStore::default();
    let mut run2 = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        DcbcStreamingClusteringFactory { cluster_count: 2 },
        embedding_spec(),
        256,
    );
    run2.ingest_batch(&items).await.unwrap();
    run2.finish_pass().unwrap();
    run2.mark_training_complete().unwrap();
    let r2 = run2
        .finalize(std::iter::once(items.as_slice()), &store2)
        .await
        .unwrap();

    assert_eq!(r1.root_id, r2.root_id);
    assert_eq!(r1.block_ids, r2.block_ids);
}

// ─── VAL-STREAM-INDEXER-028 ───────────────────────────────────────────────────
// Built-in selection supports both algorithms and requires caller-owned settings.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_028_builtin_selection_supports_dcbc_and_directional_pca() {
    let items = [item("alpha"), item("bravo")];

    let dcbc_store = MemoryBlockStore::default();
    let mut dcbc_run = StreamingIndexingRun::with_builtin_clustering(
        MapResolver,
        AsciiEmbeddingProvider,
        dcbc_builtin_clustering(),
        embedding_spec(),
        256,
    );
    dcbc_run.ingest_batch(&items).await.unwrap();
    dcbc_run.finish_pass().unwrap();
    dcbc_run.mark_training_complete().unwrap();
    let dcbc_result = dcbc_run
        .finalize(std::iter::once(items.as_slice()), &dcbc_store)
        .await
        .unwrap();
    assert!(!dcbc_result.block_ids.is_empty());

    let dpca_store = MemoryBlockStore::default();
    let mut dpca_run = StreamingIndexingRun::with_builtin_clustering(
        MapResolver,
        AsciiEmbeddingProvider,
        directional_pca_builtin_clustering(),
        embedding_spec(),
        256,
    );
    dpca_run.ingest_batch(&items).await.unwrap();
    dpca_run.finish_pass().unwrap();
    dpca_run.mark_training_complete().unwrap();
    let dpca_result = dpca_run
        .finalize(std::iter::once(items.as_slice()), &dpca_store)
        .await
        .unwrap();
    assert!(!dpca_result.block_ids.is_empty());
}

// ─── VAL-STREAM-INDEXER-029 ───────────────────────────────────────────────────
// Algorithm-agnostic built-in-path behavior is realized as a two-algorithm matrix.

#[test]
fn val_stream_indexer_029_algorithm_agnostic_built_in_behavior_is_matrizized() {
    let src = include_str!("../tests/spec_validation.rs");
    let matrix_loop_count = src.matches("for_each_builtin_algorithm_case(").count();
    assert!(
        src.contains("built_in_algorithm_cases()"),
        "spec_validation.rs must define a built-in algorithm matrix helper"
    );
    assert!(
        matrix_loop_count >= 10,
        "spec_validation.rs should use the built-in algorithm matrix across many algorithm-agnostic cases; found {matrix_loop_count}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn two_pass_training_accepted_and_deterministic() {
    for_each_builtin_algorithm_case(|case| async move {
        let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];

        let r1 = {
            let store = MemoryBlockStore::default();
            let mut run = run_with_builtin(
                MapResolver,
                AsciiEmbeddingProvider,
                case.clustering.clone(),
                embedding_spec(),
                256,
            );
            run.ingest_batch(&items).await.unwrap();
            run.finish_pass().unwrap();
            run.ingest_batch(&items).await.unwrap();
            run.finish_pass().unwrap();
            run.mark_training_complete().unwrap();
            run.finalize(std::iter::once(items.as_slice()), &store)
                .await
                .unwrap()
        };
        let r2 = {
            let store = MemoryBlockStore::default();
            let mut run = run_with_builtin(
                MapResolver,
                AsciiEmbeddingProvider,
                case.clustering,
                embedding_spec(),
                256,
            );
            run.ingest_batch(&items).await.unwrap();
            run.finish_pass().unwrap();
            run.ingest_batch(&items).await.unwrap();
            run.finish_pass().unwrap();
            run.mark_training_complete().unwrap();
            run.finalize(std::iter::once(items.as_slice()), &store)
                .await
                .unwrap()
        };
        assert_eq!(r1.root_id, r2.root_id, "{}", case.name);
        assert_eq!(r1.block_ids, r2.block_ids, "{}", case.name);
    })
    .await;
}
