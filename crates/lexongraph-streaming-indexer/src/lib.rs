// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Protocol-conforming LexonGraph streaming indexing orchestration.
//!
//! This crate exposes a caller-visible, replay-based streaming lifecycle for
//! indexing large datasets.  Callers drive one or more training passes (each
//! a full replay of the item set in batches), then signal training completion,
//! then supply a final materialization replay.  The crate uses the shared
//! [`StreamingClusterTrainer`] /
//! [`lexongraph_streaming_clustering::StreamingClusterClassifier`] contract from
//! `lexongraph-streaming-clustering` for the first parent-producing layer and
//! for every higher layer.  The built-in default clustering realization is
//! backed by `lexongraph-dcbc-streaming`.
//!
//! ```compile_fail
//! #[cfg(feature = "conformance")]
//! compile_error!("the conformance module is intentionally enabled in this doctest configuration");
//!
//! use lexongraph_streaming_indexer::conformance;
//!
//! let _ = std::any::type_name::<conformance::ConformanceError>();
//! ```

use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use ciborium::ser::into_writer;
use half::f16;
use lexongraph_block::{
    Block, BlockError, BranchEntry, LeafEntry, VERSION_1, build_branch_block, build_leaf_block,
    canonicalize_metadata, serialize_block,
};
pub use lexongraph_block::{
    BlockHash, BranchBlock, Content, EmbeddingSpec, Metadata, SerializedBlock,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_dcbc_streaming::DcbcStreamingTrainer;
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
pub use lexongraph_streaming_clustering::MetricDirection;
use lexongraph_streaming_clustering::{
    ClusterId, StreamingClusterClassifier, StreamingClusterTrainer, StreamingClusteringConfig,
    StreamingClusteringError,
};
use sha2::{Digest, Sha256};

// ─────────────────────────────────────────────────────────────
// Public input / output types
// ─────────────────────────────────────────────────────────────

/// One caller-supplied indexing unit carrying application metadata and a
/// content reference.  Raw content bytes are intentionally absent; they are
/// resolved on demand by the caller-supplied [`ContentResolver`].
#[derive(Clone, Debug, PartialEq)]
pub struct IndexItem<R> {
    pub metadata: Metadata,
    pub content_ref: R,
}

/// The result of a successful final materialization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamingIndexingResult {
    pub root_id: BlockHash,
    pub block_ids: Vec<BlockHash>,
}

/// Report returned after each completed training pass.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexingPassReport {
    /// Number of items observed in this pass.
    pub observed_item_count: usize,
    /// Clustering quality metric from the streaming pass report.
    pub clustering_quality_metric: f64,
    /// Clustering balance metric from the streaming pass report.
    pub clustering_balance_metric: f64,
    pub clustering_quality_direction: MetricDirection,
    pub clustering_balance_direction: MetricDirection,
    /// Total number of successfully completed passes so far.
    pub completed_pass_count: usize,
}

// ─────────────────────────────────────────────────────────────
// Indexer-owned trait definitions
// ─────────────────────────────────────────────────────────────

/// Resolves a content reference into the concrete [`Content`] used for leaf
/// construction and embedding generation.
pub trait ContentResolver<R> {
    type Error: std::error::Error;
    fn resolve(&self, content_ref: &R) -> Result<Content, Self::Error>;
}

/// Derives a canonical (representative) embedding from the finalized entries
/// of a child-bearing branch block.
pub trait CanonicalEmbeddingPolicy {
    type Error: std::error::Error;
    fn canonical_embedding(&self, block: &BranchBlock) -> Result<Vec<u8>, Self::Error>;
}

/// Creates a fresh [`StreamingClusterTrainer`] for a single clustering layer.
/// The factory is consulted once per layer: for the caller-visible first layer
/// it is created lazily during `finish_pass()` once the first pass's item count
/// is known; for each higher layer it is called during final materialization
/// with the known child count.
pub trait StreamingClusteringFactory {
    type Trainer: StreamingClusterTrainer;
    type Error: std::error::Error;

    fn create_trainer(
        &self,
        dimensions: usize,
        estimated_child_count: usize,
        block_size_target: usize,
        embedding_spec: &EmbeddingSpec,
    ) -> Result<Self::Trainer, Self::Error>;
}

// ─────────────────────────────────────────────────────────────
// Error type
// ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamingIndexerError {
    EmptyInput,
    EmptyPass(String),
    ReplayMismatch(String),
    ReplayFingerprint(String),
    ContentResolution(String),
    UnusableContent(String),
    EmbeddingFailure(String),
    CanonicalEmbeddingFailure(String),
    ClusteringFailure(String),
    IntermediateNodeTooLarge {
        min_serialized_bytes: usize,
        size_target: usize,
    },
    BlockConstruction(BlockError),
    Storage(BlockStoreError),
    InvalidLifecycleTransition(String),
}

impl fmt::Display for StreamingIndexerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyInput => write!(f, "streaming indexing requires at least one item"),
            Self::EmptyPass(m) => write!(f, "pass is empty: {m}"),
            Self::ReplayMismatch(m) => write!(f, "replay mismatch: {m}"),
            Self::ReplayFingerprint(m) => write!(f, "replay fingerprinting failed: {m}"),
            Self::ContentResolution(m) => write!(f, "content resolution failed: {m}"),
            Self::UnusableContent(m) => write!(f, "resolved content is unusable: {m}"),
            Self::EmbeddingFailure(m) => write!(f, "embedding generation failed: {m}"),
            Self::CanonicalEmbeddingFailure(m) => {
                write!(f, "canonical embedding selection failed: {m}")
            }
            Self::ClusteringFailure(m) => write!(f, "clustering failed: {m}"),
            Self::IntermediateNodeTooLarge {
                min_serialized_bytes,
                size_target,
            } => write!(
                f,
                "smallest intermediate node needs {min_serialized_bytes} bytes, \
                 exceeding block size target {size_target}"
            ),
            Self::BlockConstruction(e) => write!(f, "block construction failed: {e}"),
            Self::Storage(e) => write!(f, "block storage failed: {e}"),
            Self::InvalidLifecycleTransition(m) => {
                write!(f, "invalid lifecycle transition: {m}")
            }
        }
    }
}

impl std::error::Error for StreamingIndexerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BlockConstruction(e) => Some(e),
            Self::Storage(e) => Some(e),
            _ => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Status observer
// ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamingIndexingPhase {
    TrainingPass { pass_number: usize },
    LeafMaterialization,
    FirstLayerClustering,
    HigherLayerClustering { layer_index: usize },
    LayerMaterialization { layer_index: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamingIndexingStatusState {
    Started,
    InProgress,
    Completed,
    Failed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StreamingIndexingStatus {
    pub phase: StreamingIndexingPhase,
    pub state: StreamingIndexingStatusState,
    pub item_count: usize,
    pub elapsed: Duration,
    pub error: Option<String>,
}

pub type StreamingIndexingStatusObserver =
    Arc<dyn Fn(StreamingIndexingStatus) + Send + Sync + 'static>;

const STATUS_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

// ─────────────────────────────────────────────────────────────
// Built-in canonical-embedding policy
// ─────────────────────────────────────────────────────────────

/// Computes the component-wise arithmetic mean of all branch entry embeddings.
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

// ─────────────────────────────────────────────────────────────
// Built-in streaming clustering factory (DCBC-backed)
// ─────────────────────────────────────────────────────────────

/// Default [`StreamingClusteringFactory`] backed by `lexongraph-dcbc-streaming`.
///
/// `cluster_count` is the fallback only when `create_trainer` is invoked with
/// an `estimated_child_count` of zero. When the child count is known, the
/// factory derives an appropriate cluster count from that count and
/// `block_size_target`.
#[derive(Clone, Debug)]
pub struct DcbcStreamingClusteringFactory {
    pub cluster_count: u32,
}

impl DcbcStreamingClusteringFactory {
    pub fn new(cluster_count: u32) -> Self {
        Self { cluster_count }
    }
}

impl StreamingClusteringFactory for DcbcStreamingClusteringFactory {
    type Trainer = DcbcStreamingTrainer;
    type Error = StreamingClusteringError;

    fn create_trainer(
        &self,
        dimensions: usize,
        estimated_child_count: usize,
        block_size_target: usize,
        embedding_spec: &EmbeddingSpec,
    ) -> Result<DcbcStreamingTrainer, StreamingClusteringError> {
        let cluster_count = if estimated_child_count == 0 {
            // Unknown count: use the configured default.
            self.cluster_count.max(1)
        } else {
            let max_per = max_children_per_branch(
                embedding_spec,
                block_size_target,
                estimated_child_count,
            )
            .map_err(|message| StreamingClusteringError::InvalidConfiguration {
                message: format!(
                    "cannot derive branch capacity for embedding spec {} dims under {}: {message}",
                    embedding_spec.dims, embedding_spec.encoding
                ),
            })?;
            if estimated_child_count <= max_per.max(1) {
                1 // All items fit in a single block.
            } else {
                let needed = estimated_child_count.div_ceil(max_per.max(2));
                let max_sensible = estimated_child_count / 2;
                if needed > max_sensible {
                    return Err(StreamingClusteringError::InvalidConfiguration {
                        message: format!(
                            "cannot satisfy minimum two-children-per-branch constraint for {estimated_child_count} children with block size target {block_size_target}"
                        ),
                    });
                } else {
                    u32::try_from(needed).map_err(|_| {
                        StreamingClusteringError::InvalidConfiguration {
                            message: format!(
                                "derived cluster count {needed} exceeds u32::MAX for estimated child count {estimated_child_count}"
                            ),
                        }
                    })?
                }
            }
        };

        DcbcStreamingTrainer::new(StreamingClusteringConfig {
            cluster_count,
            dimensions,
            balance_constraints: None,
            random_seed: None,
        })
    }
}

// ─────────────────────────────────────────────────────────────
// Internal state helpers
// ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunPhase {
    Training,
    TrainingComplete,
    Finalized,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct BaselineItem {
    metadata_hash: BlockHash,
    content_hash: BlockHash,
    embedding_hash: BlockHash,
}

struct IndexedChild {
    embedding: Vec<u8>,
    child: BlockHash,
    level: u64,
}

// ─────────────────────────────────────────────────────────────
// StreamingIndexingRun — the public orchestration type
// ─────────────────────────────────────────────────────────────

/// Orchestrates one streaming indexing run.
///
/// **Lifecycle**
/// 1. Create via [`with_defaults`], [`with_canonical_policy`], or [`new`].
/// 2. Replay the item set in one or more passes:  
///    `ingest_batch` (one or more times) → `finish_pass` → repeat.
/// 3. Call `mark_training_complete` once satisfied.
/// 4. Call `finalize` with the same item set to produce the finished index.
///
/// [`with_defaults`]: StreamingIndexingRun::with_defaults
/// [`with_canonical_policy`]: StreamingIndexingRun::with_canonical_policy
/// [`new`]: StreamingIndexingRun::new
pub struct StreamingIndexingRun<R, CR, EP, CEP, SCF>
where
    SCF: StreamingClusteringFactory,
{
    resolver: CR,
    embedding_provider: EP,
    canonical_embedding_policy: CEP,
    factory: SCF,
    embedding_spec: EmbeddingSpec,
    block_size_target: usize,
    observer: Option<StreamingIndexingStatusObserver>,

    phase: RunPhase,
    trainer: Option<SCF::Trainer>,
    classifier: Option<<SCF::Trainer as StreamingClusterTrainer>::Classifier>,
    completed_passes: usize,
    baseline: Option<Vec<BaselineItem>>,
    current_pass_items: Vec<BaselineItem>,
    current_pass_f32_embeddings: Vec<Vec<f32>>,
    items_seen_in_current_pass: usize,
    _item_ref: PhantomData<R>,
}

// ─── Constructors ────────────────────────────────────────────

impl<R, CR, EP>
    StreamingIndexingRun<
        R,
        CR,
        EP,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        DcbcStreamingClusteringFactory,
    >
{
    /// Primary default constructor: uses the built-in arithmetic-mean
    /// canonical-embedding policy and built-in DCBC streaming clustering.
    pub fn with_defaults(
        resolver: CR,
        embedding_provider: EP,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Self {
        Self::new(
            resolver,
            embedding_provider,
            ArithmeticMeanCanonicalEmbeddingPolicy,
            DcbcStreamingClusteringFactory { cluster_count: 2 },
            embedding_spec,
            block_size_target,
        )
    }
}

impl<R, CR, EP, CEP> StreamingIndexingRun<R, CR, EP, CEP, DcbcStreamingClusteringFactory> {
    /// Explicit canonical-embedding policy override; keeps the built-in DCBC
    /// streaming clustering.
    pub fn with_canonical_policy(
        resolver: CR,
        embedding_provider: EP,
        canonical_embedding_policy: CEP,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Self {
        Self::new(
            resolver,
            embedding_provider,
            canonical_embedding_policy,
            DcbcStreamingClusteringFactory { cluster_count: 2 },
            embedding_spec,
            block_size_target,
        )
    }
}

impl<R, CR, EP, CEP, SCF> StreamingIndexingRun<R, CR, EP, CEP, SCF>
where
    SCF: StreamingClusteringFactory,
{
    /// Fully explicit constructor accepting caller-supplied canonical-embedding
    /// policy and clustering factory.
    pub fn new(
        resolver: CR,
        embedding_provider: EP,
        canonical_embedding_policy: CEP,
        factory: SCF,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Self {
        Self {
            resolver,
            embedding_provider,
            canonical_embedding_policy,
            factory,
            embedding_spec,
            block_size_target,
            observer: None,
            phase: RunPhase::Training,
            trainer: None,
            classifier: None,
            completed_passes: 0,
            baseline: None,
            current_pass_items: Vec::new(),
            current_pass_f32_embeddings: Vec::new(),
            items_seen_in_current_pass: 0,
            _item_ref: PhantomData,
        }
    }

    /// Attach an optional status observer; returns `self` for chaining.
    pub fn with_observer(mut self, observer: StreamingIndexingStatusObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Number of successfully completed training passes so far.
    pub fn completed_passes(&self) -> usize {
        self.completed_passes
    }
}

// ─── Lifecycle methods ────────────────────────────────────────

impl<R, CR, EP, CEP, SCF> StreamingIndexingRun<R, CR, EP, CEP, SCF>
where
    CR: ContentResolver<R>,
    EP: EmbeddingProvider,
    CEP: CanonicalEmbeddingPolicy,
    SCF: StreamingClusteringFactory,
{
    /// Ingest one batch of items for the current training pass.
    ///
    /// Empty batches are accepted as a no-op.  Content is resolved and
    /// embeddings are generated; the f32 vectors are buffered until
    /// `finish_pass` is called.  Replay continuity is verified against
    /// the baseline established by the first completed pass.
    pub async fn ingest_batch(
        &mut self,
        batch: &[IndexItem<R>],
    ) -> Result<(), StreamingIndexerError> {
        if !matches!(self.phase, RunPhase::Training) {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                "ingest_batch requires the training phase (currently {:?})",
                self.phase
            )));
        }

        if batch.is_empty() {
            return Ok(());
        }

        // Resolve content for all items in the batch
        let mut contents = Vec::with_capacity(batch.len());
        let mut inputs = Vec::with_capacity(batch.len());
        for item in batch {
            let content = self
                .resolver
                .resolve(&item.content_ref)
                .map_err(|e| StreamingIndexerError::ContentResolution(e.to_string()))?;
            if content.media_type.is_empty() {
                return Err(StreamingIndexerError::UnusableContent(
                    "resolved content must include a media type".into(),
                ));
            }
            inputs.push(EmbeddingInput {
                media_type: content.media_type.clone(),
                body: content.body.clone(),
            });
            contents.push(content);
        }

        // Generate embeddings
        let embeddings = self
            .embedding_provider
            .embed_batch(&inputs, &self.embedding_spec)
            .await
            .map_err(|e| StreamingIndexerError::EmbeddingFailure(e.to_string()))?;
        if embeddings.len() != batch.len() {
            return Err(StreamingIndexerError::EmbeddingFailure(format!(
                "embedding provider returned {} embeddings for {} inputs",
                embeddings.len(),
                batch.len()
            )));
        }
        for embedding in &embeddings {
            validate_embedding_bytes(embedding, &self.embedding_spec, "item")
                .map_err(StreamingIndexerError::EmbeddingFailure)?;
        }

        let offset = self.items_seen_in_current_pass;
        for (index, ((item, content), embedding)) in batch
            .iter()
            .zip(contents.iter())
            .zip(embeddings.iter())
            .enumerate()
        {
            let replay_item = BaselineItem {
                metadata_hash: hash_metadata(&item.metadata)
                    .map_err(StreamingIndexerError::ReplayFingerprint)?,
                content_hash: hash_content(content),
                embedding_hash: hash_bytes(embedding),
            };
            if let Some(baseline) = &self.baseline {
                let Some(expected) = baseline.get(offset + index) else {
                    return Err(StreamingIndexerError::ReplayMismatch(format!(
                        "current pass has more items than the {} items in the established baseline",
                        baseline.len()
                    )));
                };
                if expected != &replay_item {
                    return Err(StreamingIndexerError::ReplayMismatch(format!(
                        "item {} in current pass differs from established baseline",
                        offset + index
                    )));
                }
            } else {
                self.current_pass_items.push(replay_item);
            }
        }

        // Convert to f32 and buffer for batch-ingestion in finish_pass
        for embedding in &embeddings {
            let f32_emb = decode_embedding_as_f32(embedding, &self.embedding_spec)?;
            self.current_pass_f32_embeddings.push(f32_emb);
        }
        self.items_seen_in_current_pass += batch.len();

        Ok(())
    }

    /// Complete the current training pass and return a deterministic pass
    /// report.  Fails if no items were ingested since the last completed pass.
    /// The trainer is created here (lazily, with the actual item count known)
    /// and the buffered embeddings are fed to it in one call.
    pub fn finish_pass(&mut self) -> Result<IndexingPassReport, StreamingIndexerError> {
        if !matches!(self.phase, RunPhase::Training) {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                "finish_pass requires the training phase (currently {:?})",
                self.phase
            )));
        }

        if self.items_seen_in_current_pass == 0 {
            return Err(StreamingIndexerError::EmptyPass(
                "at least one item must be ingested before completing a pass".into(),
            ));
        }

        // Validate baseline completeness for passes after the first
        if let Some(baseline) = &self.baseline
            && self.items_seen_in_current_pass != baseline.len()
        {
            return Err(StreamingIndexerError::ReplayMismatch(format!(
                "pass had {} items but baseline has {}",
                self.items_seen_in_current_pass,
                baseline.len()
            )));
        }

        // Create the trainer now that we know the exact item count.
        // For subsequent passes the trainer is already present (reused).
        if self.trainer.is_none() {
            let dims = self.embedding_spec.dims as usize;
            let item_count = self.items_seen_in_current_pass;
            let new_trainer = self
                .factory
                .create_trainer(
                    dims,
                    item_count,
                    self.block_size_target,
                    &self.embedding_spec,
                )
                .map_err(|e| StreamingIndexerError::ClusteringFailure(e.to_string()))?;
            self.trainer = Some(new_trainer);
        }

        let trainer = self.trainer.as_mut().unwrap();

        let pass_number = self.completed_passes + 1;
        let pass_started = Instant::now();
        emit_status(
            &self.observer,
            StreamingIndexingStatus {
                phase: StreamingIndexingPhase::TrainingPass { pass_number },
                state: StreamingIndexingStatusState::Started,
                item_count: self.items_seen_in_current_pass,
                elapsed: Duration::ZERO,
                error: None,
            },
        );
        let mut heartbeat = StatusHeartbeatGuard::new(start_status_heartbeat(
            &self.observer,
            StreamingIndexingPhase::TrainingPass { pass_number },
            self.items_seen_in_current_pass,
            pass_started,
        ));

        // Feed all buffered embeddings as one batch
        let buffered = std::mem::take(&mut self.current_pass_f32_embeddings);
        let ingest_result = trainer.ingest_batch(&buffered);
        if let Err(error) = ingest_result {
            heartbeat.stop();
            self.current_pass_f32_embeddings = buffered;
            emit_status(
                &self.observer,
                StreamingIndexingStatus {
                    phase: StreamingIndexingPhase::TrainingPass { pass_number },
                    state: StreamingIndexingStatusState::Failed,
                    item_count: self.items_seen_in_current_pass,
                    elapsed: pass_started.elapsed(),
                    error: Some(error.to_string()),
                },
            );
            return Err(StreamingIndexerError::ClusteringFailure(error.to_string()));
        }

        let pass_report = match trainer.finish_pass() {
            Ok(report) => report,
            Err(error) => {
                heartbeat.stop();
                self.current_pass_f32_embeddings = buffered;
                emit_status(
                    &self.observer,
                    StreamingIndexingStatus {
                        phase: StreamingIndexingPhase::TrainingPass { pass_number },
                        state: StreamingIndexingStatusState::Failed,
                        item_count: self.items_seen_in_current_pass,
                        elapsed: pass_started.elapsed(),
                        error: Some(error.to_string()),
                    },
                );
                return Err(StreamingIndexerError::ClusteringFailure(error.to_string()));
            }
        };
        heartbeat.stop();

        // Establish baseline after first completed pass
        if self.baseline.is_none() {
            self.baseline = Some(std::mem::take(&mut self.current_pass_items));
        }

        self.completed_passes += 1;
        self.items_seen_in_current_pass = 0;

        emit_status(
            &self.observer,
            StreamingIndexingStatus {
                phase: StreamingIndexingPhase::TrainingPass { pass_number },
                state: StreamingIndexingStatusState::Completed,
                item_count: pass_report.observed_count,
                elapsed: pass_started.elapsed(),
                error: None,
            },
        );

        Ok(IndexingPassReport {
            observed_item_count: pass_report.observed_count,
            clustering_quality_metric: pass_report.quality_metric,
            clustering_balance_metric: pass_report.balance_metric,
            clustering_quality_direction: pass_report.quality_direction,
            clustering_balance_direction: pass_report.balance_direction,
            completed_pass_count: self.completed_passes,
        })
    }

    /// Signal that training is complete.  Requires at least one completed pass
    /// and no open (incomplete) pass.  Converts the trainer into a classifier
    /// ready for final materialization.
    pub fn mark_training_complete(&mut self) -> Result<(), StreamingIndexerError> {
        if !matches!(self.phase, RunPhase::Training) {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                "mark_training_complete requires the training phase (currently {:?})",
                self.phase
            )));
        }
        if self.completed_passes == 0 {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(
                "training completion requires at least one completed pass".into(),
            ));
        }
        if self.items_seen_in_current_pass > 0 {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(
                "cannot complete training with an open (unfinished) pass".into(),
            ));
        }

        let mut trainer = self.trainer.take().ok_or_else(|| {
            StreamingIndexerError::InvalidLifecycleTransition(
                "no trainer available to complete".into(),
            )
        })?;

        trainer
            .complete_training()
            .map_err(|e| StreamingIndexerError::ClusteringFailure(e.to_string()))?;

        let classifier = trainer
            .into_classifier()
            .map_err(|e| StreamingIndexerError::ClusteringFailure(e.to_string()))?;

        self.classifier = Some(classifier);
        self.phase = RunPhase::TrainingComplete;
        Ok(())
    }

    /// Final materialization replay.  The caller must supply the same logical
    /// item set in the same replay order as the training passes.  Resolves
    /// content, constructs leaf blocks, persists the full block tree, and
    /// returns the root block ID plus all persisted block IDs.
    pub async fn finalize<I, B>(
        &mut self,
        replay_batches: I,
        store: &dyn BlockStore,
    ) -> Result<StreamingIndexingResult, StreamingIndexerError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[IndexItem<R>]>,
    {
        if !matches!(self.phase, RunPhase::TrainingComplete) {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                "finalize requires the training-complete phase (currently {:?})",
                self.phase
            )));
        }

        let baseline = self.baseline.as_ref().ok_or_else(|| {
            StreamingIndexerError::InvalidLifecycleTransition("no baseline established".into())
        })?;
        let classifier = self
            .classifier
            .as_ref()
            .expect("classifier must be present in TrainingComplete phase");

        let result = self
            .do_finalize(replay_batches, baseline.as_slice(), store, classifier)
            .await;

        if result.is_ok() {
            self.phase = RunPhase::Finalized;
        }
        result
    }

    // ── Private: perform the actual materialization ────────────

    async fn do_finalize<I, B>(
        &self,
        replay_batches: I,
        baseline: &[BaselineItem],
        store: &dyn BlockStore,
        classifier: &<SCF::Trainer as StreamingClusterTrainer>::Classifier,
    ) -> Result<StreamingIndexingResult, StreamingIndexerError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[IndexItem<R>]>,
    {
        let leaf_started = Instant::now();

        emit_status(
            &self.observer,
            StreamingIndexingStatus {
                phase: StreamingIndexingPhase::LeafMaterialization,
                state: StreamingIndexingStatusState::Started,
                item_count: baseline.len(),
                elapsed: Duration::ZERO,
                error: None,
            },
        );
        emit_status(
            &self.observer,
            StreamingIndexingStatus {
                phase: StreamingIndexingPhase::LeafMaterialization,
                state: StreamingIndexingStatusState::InProgress,
                item_count: baseline.len(),
                elapsed: leaf_started.elapsed(),
                error: None,
            },
        );

        let mut replay_count = 0usize;
        let mut leaf_children: Vec<IndexedChild> = Vec::with_capacity(baseline.len());
        let mut persisted_ids: Vec<BlockHash> = Vec::new();
        for batch in replay_batches {
            let items = batch.as_ref();
            if items.is_empty() {
                continue;
            }
            for (offset, _) in items.iter().enumerate() {
                let Some(_) = baseline.get(replay_count + offset) else {
                    return Err(StreamingIndexerError::ReplayMismatch(format!(
                        "finalization replay has more items than the {} items in the established baseline",
                        baseline.len()
                    )));
                };
            }

            let mut inputs: Vec<EmbeddingInput> = Vec::with_capacity(items.len());
            let mut contents: Vec<Content> = Vec::with_capacity(items.len());
            let mut metadatas: Vec<Metadata> = Vec::with_capacity(items.len());
            for item in items {
                let content = self
                    .resolver
                    .resolve(&item.content_ref)
                    .map_err(|e| StreamingIndexerError::ContentResolution(e.to_string()))?;
                if content.media_type.is_empty() {
                    return Err(StreamingIndexerError::UnusableContent(
                        "resolved content must include a media type".into(),
                    ));
                }
                inputs.push(EmbeddingInput {
                    media_type: content.media_type.clone(),
                    body: content.body.clone(),
                });
                contents.push(content);
                metadatas.push(item.metadata.clone());
            }

            let embeddings = self
                .embedding_provider
                .embed_batch(&inputs, &self.embedding_spec)
                .await
                .map_err(|e| StreamingIndexerError::EmbeddingFailure(e.to_string()))?;
            if embeddings.len() != items.len() {
                return Err(StreamingIndexerError::EmbeddingFailure(format!(
                    "expected {} embeddings, got {}",
                    items.len(),
                    embeddings.len()
                )));
            }

            for (offset, ((item, content), embedding)) in items
                .iter()
                .zip(contents.iter())
                .zip(embeddings.iter())
                .enumerate()
            {
                let expected = &baseline[replay_count + offset];
                let replay_item = BaselineItem {
                    metadata_hash: hash_metadata(&item.metadata)
                        .map_err(StreamingIndexerError::ReplayFingerprint)?,
                    content_hash: hash_content(content),
                    embedding_hash: hash_bytes(embedding),
                };
                if expected != &replay_item {
                    return Err(StreamingIndexerError::ReplayMismatch(format!(
                        "finalization item {} differs from baseline",
                        replay_count + offset
                    )));
                }
            }

            for ((content, metadata), embedding) in
                contents.into_iter().zip(metadatas).zip(embeddings.iter())
            {
                validate_embedding_bytes(embedding, &self.embedding_spec, "item")
                    .map_err(StreamingIndexerError::EmbeddingFailure)?;

                let leaf = build_leaf_block(
                    VERSION_1,
                    self.embedding_spec.clone(),
                    vec![LeafEntry {
                        embedding: embedding.clone(),
                        metadata,
                        content,
                    }],
                    None,
                )
                .map_err(StreamingIndexerError::BlockConstruction)?;

                let leaf_block = Block::Leaf(leaf);
                let serialized = serialize_block(&leaf_block)
                    .map_err(StreamingIndexerError::BlockConstruction)?;
                let block_id = store
                    .put(&leaf_block)
                    .map_err(StreamingIndexerError::Storage)?;
                if block_id != serialized.hash {
                    return Err(StreamingIndexerError::Storage(
                        BlockStoreError::IntegrityMismatch {
                            expected: serialized.hash,
                            actual: block_id,
                        },
                    ));
                }
                persisted_ids.push(block_id);
                leaf_children.push(IndexedChild {
                    embedding: embedding.clone(),
                    child: block_id,
                    level: 0,
                });
            }
            replay_count += items.len();
        }
        if replay_count == 0 {
            return Err(StreamingIndexerError::EmptyInput);
        }
        if replay_count != baseline.len() {
            return Err(StreamingIndexerError::ReplayMismatch(format!(
                "finalization replay had {replay_count} items but baseline has {}",
                baseline.len()
            )));
        }

        emit_status(
            &self.observer,
            StreamingIndexingStatus {
                phase: StreamingIndexingPhase::LeafMaterialization,
                state: StreamingIndexingStatusState::Completed,
                item_count: replay_count,
                elapsed: leaf_started.elapsed(),
                error: None,
            },
        );

        // Normalize leaf layer
        let unique_leaves = normalize_current_layer(leaf_children);
        if unique_leaves.is_empty() {
            return Err(StreamingIndexerError::EmptyInput);
        }

        // Single unique leaf → it is the root
        if unique_leaves.len() == 1 {
            let root_id = unique_leaves[0].child;
            dedup_sort_ids(&mut persisted_ids);
            return Ok(StreamingIndexingResult {
                root_id,
                block_ids: persisted_ids,
            });
        }

        // ── First parent layer: use the trained classifier ────────

        let first_layer_started = Instant::now();
        emit_status(
            &self.observer,
            StreamingIndexingStatus {
                phase: StreamingIndexingPhase::FirstLayerClustering,
                state: StreamingIndexingStatusState::Started,
                item_count: unique_leaves.len(),
                elapsed: Duration::ZERO,
                error: None,
            },
        );
        emit_status(
            &self.observer,
            StreamingIndexingStatus {
                phase: StreamingIndexingPhase::FirstLayerClustering,
                state: StreamingIndexingStatusState::InProgress,
                item_count: unique_leaves.len(),
                elapsed: first_layer_started.elapsed(),
                error: None,
            },
        );
        let mut heartbeat = StatusHeartbeatGuard::new(start_status_heartbeat(
            &self.observer,
            StreamingIndexingPhase::FirstLayerClustering,
            unique_leaves.len(),
            first_layer_started,
        ));

        let leaf_f32: Vec<Vec<f32>> = unique_leaves
            .iter()
            .map(|c| decode_embedding_as_f32(&c.embedding, &self.embedding_spec))
            .collect::<Result<_, _>>()?;

        let assignments = classifier
            .assign_batch(&leaf_f32)
            .map_err(|e| StreamingIndexerError::ClusteringFailure(e.to_string()))?;
        let groups = ensure_min_two_per_group(assignments_to_groups(&assignments));
        heartbeat.stop();

        emit_status(
            &self.observer,
            StreamingIndexingStatus {
                phase: StreamingIndexingPhase::FirstLayerClustering,
                state: StreamingIndexingStatusState::Completed,
                item_count: unique_leaves.len(),
                elapsed: first_layer_started.elapsed(),
                error: None,
            },
        );

        let layer_zero_started = Instant::now();
        emit_status(
            &self.observer,
            StreamingIndexingStatus {
                phase: StreamingIndexingPhase::LayerMaterialization { layer_index: 0 },
                state: StreamingIndexingStatusState::Started,
                item_count: groups.len(),
                elapsed: Duration::ZERO,
                error: None,
            },
        );

        let mut current_layer =
            self.build_branch_layer(&unique_leaves, &groups, 1, store, &mut persisted_ids)?;

        emit_status(
            &self.observer,
            StreamingIndexingStatus {
                phase: StreamingIndexingPhase::LayerMaterialization { layer_index: 0 },
                state: StreamingIndexingStatusState::Completed,
                item_count: current_layer.len(),
                elapsed: layer_zero_started.elapsed(),
                error: None,
            },
        );

        current_layer = normalize_current_layer(current_layer);

        // ── Higher layers: internal streaming replay ──────────────

        let mut layer_index = 1usize;
        while current_layer.len() > 1 {
            let child_count = current_layer.len();

            let groups = if child_count == 2 {
                // Two children always merge into one root without clustering.
                vec![vec![0usize, 1]]
            } else {
                let dims = self.embedding_spec.dims as usize;

                let higher_layer_started = Instant::now();
                emit_status(
                    &self.observer,
                    StreamingIndexingStatus {
                        phase: StreamingIndexingPhase::HigherLayerClustering { layer_index },
                        state: StreamingIndexingStatusState::Started,
                        item_count: child_count,
                        elapsed: Duration::ZERO,
                        error: None,
                    },
                );
                emit_status(
                    &self.observer,
                    StreamingIndexingStatus {
                        phase: StreamingIndexingPhase::HigherLayerClustering { layer_index },
                        state: StreamingIndexingStatusState::InProgress,
                        item_count: child_count,
                        elapsed: higher_layer_started.elapsed(),
                        error: None,
                    },
                );
                let mut heartbeat = StatusHeartbeatGuard::new(start_status_heartbeat(
                    &self.observer,
                    StreamingIndexingPhase::HigherLayerClustering { layer_index },
                    child_count,
                    higher_layer_started,
                ));

                let f32_embs: Vec<Vec<f32>> = current_layer
                    .iter()
                    .map(|c| decode_embedding_as_f32(&c.embedding, &self.embedding_spec))
                    .collect::<Result<_, _>>()?;

                let mut trainer = self
                    .factory
                    .create_trainer(
                        dims,
                        child_count,
                        self.block_size_target,
                        &self.embedding_spec,
                    )
                    .map_err(|e| StreamingIndexerError::ClusteringFailure(e.to_string()))?;

                trainer
                    .ingest_batch(&f32_embs)
                    .map_err(|e| StreamingIndexerError::ClusteringFailure(e.to_string()))?;
                trainer
                    .finish_pass()
                    .map_err(|e| StreamingIndexerError::ClusteringFailure(e.to_string()))?;
                trainer
                    .complete_training()
                    .map_err(|e| StreamingIndexerError::ClusteringFailure(e.to_string()))?;
                let layer_cls = trainer
                    .into_classifier()
                    .map_err(|e| StreamingIndexerError::ClusteringFailure(e.to_string()))?;

                let asgn = layer_cls
                    .assign_batch(&f32_embs)
                    .map_err(|e| StreamingIndexerError::ClusteringFailure(e.to_string()))?;
                heartbeat.stop();

                emit_status(
                    &self.observer,
                    StreamingIndexingStatus {
                        phase: StreamingIndexingPhase::HigherLayerClustering { layer_index },
                        state: StreamingIndexingStatusState::Completed,
                        item_count: child_count,
                        elapsed: higher_layer_started.elapsed(),
                        error: None,
                    },
                );

                ensure_min_two_per_group(assignments_to_groups(&asgn))
            };

            let next_level = current_layer[0].level + 1;

            let layer_started = Instant::now();
            emit_status(
                &self.observer,
                StreamingIndexingStatus {
                    phase: StreamingIndexingPhase::LayerMaterialization { layer_index },
                    state: StreamingIndexingStatusState::Started,
                    item_count: groups.len(),
                    elapsed: Duration::ZERO,
                    error: None,
                },
            );
            emit_status(
                &self.observer,
                StreamingIndexingStatus {
                    phase: StreamingIndexingPhase::LayerMaterialization { layer_index },
                    state: StreamingIndexingStatusState::InProgress,
                    item_count: groups.len(),
                    elapsed: layer_started.elapsed(),
                    error: None,
                },
            );

            let next_layer = self.build_branch_layer(
                &current_layer,
                &groups,
                next_level,
                store,
                &mut persisted_ids,
            )?;

            emit_status(
                &self.observer,
                StreamingIndexingStatus {
                    phase: StreamingIndexingPhase::LayerMaterialization { layer_index },
                    state: StreamingIndexingStatusState::Completed,
                    item_count: next_layer.len(),
                    elapsed: layer_started.elapsed(),
                    error: None,
                },
            );

            current_layer = normalize_current_layer(next_layer);
            layer_index += 1;
        }

        let root_id = current_layer[0].child;
        dedup_sort_ids(&mut persisted_ids);

        Ok(StreamingIndexingResult {
            root_id,
            block_ids: persisted_ids,
        })
    }

    /// Build one layer of branch blocks from `children` grouped by `groups`.
    fn build_branch_layer(
        &self,
        children: &[IndexedChild],
        groups: &[Vec<usize>],
        parent_level: u64,
        store: &dyn BlockStore,
        persisted_ids: &mut Vec<BlockHash>,
    ) -> Result<Vec<IndexedChild>, StreamingIndexerError> {
        let mut next_layer = Vec::with_capacity(groups.len());

        for group in groups {
            let raw_entries: Vec<BranchEntry> = group
                .iter()
                .map(|&i| BranchEntry {
                    embedding: children[i].embedding.clone(),
                    child: children[i].child,
                })
                .collect();
            let entries = normalize_branch_entries(raw_entries);
            if entries.len() < 2 {
                return Err(StreamingIndexerError::ClusteringFailure(
                    "cluster group normalized to fewer than two unique children".into(),
                ));
            }

            let branch = build_branch_block(
                VERSION_1,
                parent_level,
                self.embedding_spec.clone(),
                entries,
                None,
            )
            .map_err(StreamingIndexerError::BlockConstruction)?;

            let branch_block = Block::Branch(branch.clone());
            let serialized =
                serialize_block(&branch_block).map_err(StreamingIndexerError::BlockConstruction)?;

            if serialized.bytes.len() > self.block_size_target {
                if branch.entries.len() == 2 {
                    return Err(StreamingIndexerError::IntermediateNodeTooLarge {
                        min_serialized_bytes: serialized.bytes.len(),
                        size_target: self.block_size_target,
                    });
                }
                return Err(StreamingIndexerError::ClusteringFailure(format!(
                    "branch block serialized to {} bytes, exceeding block size target {}",
                    serialized.bytes.len(),
                    self.block_size_target
                )));
            }

            let block_id = store
                .put(&branch_block)
                .map_err(StreamingIndexerError::Storage)?;
            if block_id != serialized.hash {
                return Err(StreamingIndexerError::Storage(
                    BlockStoreError::IntegrityMismatch {
                        expected: serialized.hash,
                        actual: block_id,
                    },
                ));
            }
            persisted_ids.push(block_id);

            let canonical = self
                .canonical_embedding_policy
                .canonical_embedding(&branch)
                .map_err(|e| StreamingIndexerError::CanonicalEmbeddingFailure(e.to_string()))?;
            validate_embedding_bytes(&canonical, &self.embedding_spec, "canonical")
                .map_err(StreamingIndexerError::CanonicalEmbeddingFailure)?;

            next_layer.push(IndexedChild {
                embedding: canonical,
                child: block_id,
                level: parent_level,
            });
        }

        Ok(next_layer)
    }
}

// ─────────────────────────────────────────────────────────────
// Private helpers
// ─────────────────────────────────────────────────────────────

fn emit_status(
    observer: &Option<StreamingIndexingStatusObserver>,
    status: StreamingIndexingStatus,
) {
    if let Some(obs) = observer {
        let _ = catch_unwind(AssertUnwindSafe(|| obs(status)));
    }
}

fn start_status_heartbeat(
    observer: &Option<StreamingIndexingStatusObserver>,
    phase: StreamingIndexingPhase,
    item_count: usize,
    started: Instant,
) -> Option<(mpsc::Sender<()>, thread::JoinHandle<()>)> {
    let observer = observer.as_ref().map(Arc::clone)?;
    let (stop_tx, stop_rx) = mpsc::channel::<()>();
    let handle = thread::spawn(move || {
        while matches!(
            stop_rx.recv_timeout(STATUS_HEARTBEAT_INTERVAL),
            Err(mpsc::RecvTimeoutError::Timeout)
        ) {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                observer(StreamingIndexingStatus {
                    phase: phase.clone(),
                    state: StreamingIndexingStatusState::InProgress,
                    item_count,
                    elapsed: started.elapsed(),
                    error: None,
                })
            }));
        }
    });
    Some((stop_tx, handle))
}

fn stop_status_heartbeat(heartbeat: Option<(mpsc::Sender<()>, thread::JoinHandle<()>)>) {
    if let Some((stop_tx, handle)) = heartbeat {
        let _ = stop_tx.send(());
        let _ = handle.join();
    }
}

struct StatusHeartbeatGuard(Option<(mpsc::Sender<()>, thread::JoinHandle<()>)>);

impl StatusHeartbeatGuard {
    fn new(heartbeat: Option<(mpsc::Sender<()>, thread::JoinHandle<()>)>) -> Self {
        Self(heartbeat)
    }

    fn stop(&mut self) {
        stop_status_heartbeat(self.0.take());
    }
}

impl Drop for StatusHeartbeatGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

fn hash_bytes(bytes: &[u8]) -> BlockHash {
    let digest = Sha256::digest(bytes);
    let mut hash = [0_u8; BlockHash::LEN];
    hash.copy_from_slice(&digest);
    BlockHash::from_bytes(hash)
}

fn hash_content(content: &Content) -> BlockHash {
    let mut hasher = Sha256::new();
    hasher.update((content.media_type.len() as u64).to_le_bytes());
    hasher.update(content.media_type.as_bytes());
    hasher.update((content.body.len() as u64).to_le_bytes());
    hasher.update(&content.body);
    let digest = hasher.finalize();
    let mut hash = [0_u8; BlockHash::LEN];
    hash.copy_from_slice(&digest);
    BlockHash::from_bytes(hash)
}

fn hash_metadata(metadata: &Metadata) -> Result<BlockHash, String> {
    let canonical = canonicalize_metadata(metadata.clone())
        .map_err(|error| format!("failed to canonicalize metadata for replay hashing: {error}"))?;
    let mut encoded = Vec::new();
    into_writer(&canonical, &mut encoded)
        .map_err(|error| format!("failed to encode metadata for replay hashing: {error}"))?;
    Ok(hash_bytes(&encoded))
}

fn assignments_to_groups(assignments: &[ClusterId]) -> Vec<Vec<usize>> {
    if assignments.is_empty() {
        return Vec::new();
    }
    let mut groups: BTreeMap<ClusterId, Vec<usize>> = BTreeMap::new();
    for (i, &id) in assignments.iter().enumerate() {
        groups.entry(id).or_default().push(i);
    }
    groups.into_values().collect()
}

/// Ensure no group has fewer than 2 items by merging singletons into the
/// largest group.  This preserves protocol correctness (every branch block
/// requires at least two entries) regardless of how the classifier distributes
/// items at inference time.
fn ensure_min_two_per_group(mut groups: Vec<Vec<usize>>) -> Vec<Vec<usize>> {
    // Partition into groups that satisfy the minimum and those that don't.
    let (mut ok, singletons): (Vec<Vec<usize>>, Vec<Vec<usize>>) =
        groups.drain(..).partition(|g| g.len() >= 2);

    if singletons.is_empty() {
        return ok;
    }

    if ok.is_empty() {
        // Everything is a singleton: merge all into one group.
        let merged: Vec<usize> = singletons.into_iter().flatten().collect();
        return if merged.is_empty() {
            vec![]
        } else {
            vec![merged]
        };
    }

    // Append singletons to the largest group (deterministic: latest largest group wins ties).
    let target = ok.iter_mut().max_by_key(|g| g.len()).unwrap();
    for singleton in singletons {
        target.extend(singleton);
    }
    ok
}

fn decode_embedding_as_f32(
    bytes: &[u8],
    spec: &EmbeddingSpec,
) -> Result<Vec<f32>, StreamingIndexerError> {
    validate_embedding_bytes(bytes, spec, "f32-decode")
        .map_err(StreamingIndexerError::ClusteringFailure)?;
    match spec.encoding.as_str() {
        "i8" => Ok(bytes
            .iter()
            .map(|&b| i8::from_le_bytes([b]) as f32)
            .collect()),
        "f32le" => bytes
            .chunks_exact(4)
            .map(|c| Ok(f32::from_le_bytes(c.try_into().unwrap())))
            .collect(),
        "f16le" => bytes
            .chunks_exact(2)
            .map(|c| Ok(f16::from_le_bytes(c.try_into().unwrap()).to_f32()))
            .collect(),
        "pq4" => Err(StreamingIndexerError::ClusteringFailure(
            "pq4 embeddings are not supported by the streaming clustering path".into(),
        )),
        other => Err(StreamingIndexerError::ClusteringFailure(format!(
            "unsupported embedding encoding {other:?} for streaming clustering"
        ))),
    }
}

fn dedup_sort_ids(ids: &mut Vec<BlockHash>) {
    ids.sort_by(|l, r| l.as_bytes().cmp(r.as_bytes()));
    ids.dedup_by(|l, r| l.as_bytes() == r.as_bytes());
}

// ── Copied normalization / encoding helpers (no public re-export) ──────────

fn normalize_current_layer(mut layer: Vec<IndexedChild>) -> Vec<IndexedChild> {
    layer.sort_by(compare_indexed_children);
    deduplicate_layer_by_child(layer)
}

fn deduplicate_layer_by_child(mut layer: Vec<IndexedChild>) -> Vec<IndexedChild> {
    layer.sort_by(|l, r| {
        l.child
            .as_bytes()
            .cmp(r.child.as_bytes())
            .then_with(|| l.embedding.cmp(&r.embedding))
    });
    layer.dedup_by(|l, r| l.child == r.child);
    layer.sort_by(compare_indexed_children);
    layer
}

fn compare_indexed_children(l: &IndexedChild, r: &IndexedChild) -> std::cmp::Ordering {
    l.embedding
        .cmp(&r.embedding)
        .then_with(|| l.child.as_bytes().cmp(r.child.as_bytes()))
}

fn normalize_branch_entries(mut entries: Vec<BranchEntry>) -> Vec<BranchEntry> {
    entries.sort_by(|l, r| {
        l.child
            .as_bytes()
            .cmp(r.child.as_bytes())
            .then_with(|| l.embedding.cmp(&r.embedding))
    });
    let mut deduped: Vec<BranchEntry> = Vec::with_capacity(entries.len());
    for entry in entries {
        if deduped
            .last()
            .is_some_and(|prev: &BranchEntry| prev.child == entry.child)
        {
            continue;
        }
        deduped.push(entry);
    }
    deduped.sort_by(|l, r| {
        l.embedding
            .cmp(&r.embedding)
            .then_with(|| l.child.as_bytes().cmp(r.child.as_bytes()))
    });
    deduped
}

fn validate_embedding_bytes(
    embedding: &[u8],
    spec: &EmbeddingSpec,
    context: &str,
) -> Result<(), String> {
    let expected = expected_embedding_len(spec).ok_or_else(|| {
        format!(
            "unsupported embedding encoding {:?} for {context}",
            spec.encoding
        )
    })?;
    if embedding.len() != expected {
        return Err(format!(
            "{context} embedding length {} does not match expected {expected} \
             for {} dims under {}",
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
        "pq4" => dims.checked_add(1).map(|v| v / 2),
        _ => None,
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
            .map(|&b| i8::from_le_bytes([b]) as f64)
            .collect()),
        "f32le" => embedding
            .chunks_exact(4)
            .map(|c| Ok(f32::from_le_bytes(c.try_into().unwrap()) as f64))
            .collect(),
        "f16le" => embedding
            .chunks_exact(2)
            .map(|c| Ok(f16::from_le_bytes(c.try_into().unwrap()).to_f64()))
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
    let mut sums = vec![0.0f64; dims];
    for (idx, entry) in block.entries.iter().enumerate() {
        let decoded = decode_embedding_as_f64(&entry.embedding, &block.embedding_spec, "canonical")
            .map_err(|e| format!("failed to decode branch entry {idx}: {e}"))?;
        for (dim, (sum, value)) in sums.iter_mut().zip(decoded).enumerate() {
            if !value.is_finite() {
                return Err(format!(
                    "branch entry {idx} contains non-finite value at dimension {dim}"
                ));
            }
            *sum += value;
            if !sum.is_finite() {
                return Err(format!("arithmetic-mean sum overflowed at dimension {dim}"));
            }
        }
    }
    let divisor = block.entries.len() as f64;
    for (dim, sum) in sums.iter_mut().enumerate() {
        *sum /= divisor;
        if !sum.is_finite() {
            return Err(format!(
                "arithmetic-mean result became non-finite at dimension {dim}"
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
            "mean embedding dimension {} does not match expected {dims}",
            values.len()
        ));
    }
    match spec.encoding.as_str() {
        "f32le" => {
            let mut bytes = Vec::with_capacity(dims * 4);
            for (dim, &v) in values.iter().enumerate() {
                if !v.is_finite() {
                    return Err(format!(
                        "cannot encode non-finite arithmetic mean at dimension {dim}"
                    ));
                }
                let encoded = v as f32;
                if !encoded.is_finite() {
                    return Err(format!(
                        "arithmetic mean overflowed f32 encoding at dimension {dim}"
                    ));
                }
                bytes.extend_from_slice(&encoded.to_le_bytes());
            }
            Ok(bytes)
        }
        "f16le" => {
            let mut bytes = Vec::with_capacity(dims * 2);
            for (dim, &v) in values.iter().enumerate() {
                if !v.is_finite() {
                    return Err(format!(
                        "cannot encode non-finite arithmetic mean at dimension {dim}"
                    ));
                }
                let encoded = f16::from_f64(v);
                if !encoded.to_f64().is_finite() {
                    return Err(format!(
                        "arithmetic mean overflowed f16 encoding at dimension {dim}"
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
            .map(|(dim, v)| {
                if !v.is_finite() {
                    return Err(format!(
                        "cannot encode non-finite arithmetic mean at dimension {dim}"
                    ));
                }
                let rounded = v.round();
                if rounded < f64::from(i8::MIN) || rounded > f64::from(i8::MAX) {
                    return Err(format!(
                        "arithmetic mean {rounded} exceeds i8 range at dimension {dim}"
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
        .map(|i| BranchEntry {
            embedding: vec![0; embedding_len],
            child: synthetic_block_hash(i),
        })
        .collect();
    let branch = build_branch_block(VERSION_1, 1, spec.clone(), entries, None)
        .map_err(|e| format!("failed to build synthetic branch block: {e}"))?;
    let block = Block::Branch(branch);
    serialize_block(&block)
        .map(|s| s.bytes.len())
        .map_err(|e| format!("failed to serialize synthetic branch block: {e}"))
}

fn synthetic_block_hash(index: usize) -> BlockHash {
    let mut bytes = [0u8; BlockHash::LEN];
    bytes[..std::mem::size_of::<usize>()].copy_from_slice(&index.to_le_bytes());
    BlockHash::from_bytes(bytes)
}

// ─────────────────────────────────────────────────────────────
// Opt-in conformance helpers (feature = "conformance")
// ─────────────────────────────────────────────────────────────

#[cfg(feature = "conformance")]
mod conformance_support {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fmt;

    use lexongraph_block::{TypedEntries, into_entries};

    use super::*;

    // ── In-memory block store for test use ────────────────────

    #[derive(Default)]
    pub(crate) struct MemoryBlockStore {
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

        fn iter_block_ids(
            &self,
        ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
            let ids = self.blocks.borrow().keys().copied().collect::<Vec<_>>();
            Ok(Box::new(ids.into_iter().map(Ok)))
        }
    }

    fn map_get_error(e: BlockError) -> BlockStoreError {
        match e {
            BlockError::HashMismatch { expected, actual } => {
                BlockStoreError::IntegrityMismatch { expected, actual }
            }
            other => BlockStoreError::MalformedContent(other),
        }
    }

    // ── Public conformance surface ────────────────────────────

    pub type ConformanceResult = Result<(), ConformanceError>;

    #[derive(Debug)]
    pub enum ConformanceError {
        Indexer(StreamingIndexerError),
        Expectation(String),
    }

    impl fmt::Display for ConformanceError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Indexer(e) => write!(f, "{e}"),
                Self::Expectation(m) => write!(f, "conformance expectation failed: {m}"),
            }
        }
    }

    impl std::error::Error for ConformanceError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Indexer(e) => Some(e),
                Self::Expectation(_) => None,
            }
        }
    }

    impl From<StreamingIndexerError> for ConformanceError {
        fn from(e: StreamingIndexerError) -> Self {
            Self::Indexer(e)
        }
    }

    /// Shareable test-fixture error type.
    #[derive(Clone, Debug)]
    pub struct FixtureError(pub String);

    impl fmt::Display for FixtureError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl std::error::Error for FixtureError {}

    // ── Harness traits ────────────────────────────────────────

    pub trait ContentResolverConformanceHarness {
        type Ref: Clone + PartialEq;
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

    pub trait StreamingClusteringFactoryConformanceHarness {
        type Factory: StreamingClusteringFactory;

        fn conforming_factory(&self) -> Self::Factory;
    }

    // ── Suite runners ─────────────────────────────────────────

    pub fn run_content_resolver_suite<H>(harness: &H) -> ConformanceResult
    where
        H: ContentResolverConformanceHarness,
    {
        pollster::block_on(async {
            let store = MemoryBlockStore::default();
            let item = harness.sample_item();

            // Conforming resolver should produce the expected leaf
            let mut run = StreamingIndexingRun::<H::Ref, _, _, _, _>::new(
                harness.conforming_resolver(),
                FixedEmbeddingProvider,
                FixedCanonicalEmbeddingPolicy,
                FixedClusteringFactory,
                conformance_embedding_spec(),
                conformance_block_size_target(),
            );
            run.ingest_batch(std::slice::from_ref(&item)).await?;
            run.finish_pass()?;
            run.mark_training_complete()?;
            let result = run
                .finalize(std::iter::once(std::slice::from_ref(&item)), &store)
                .await?;

            let root = store
                .get(&result.root_id)
                .map_err(StreamingIndexerError::Storage)?
                .ok_or_else(|| {
                    ConformanceError::Expectation("root block must be present in store".into())
                })?;
            match into_entries(root) {
                TypedEntries::Leaf(_, entries)
                    if entries[0].content == harness.expected_content() => {}
                TypedEntries::Leaf(_, entries) => {
                    return Err(ConformanceError::Expectation(format!(
                        "expected content {:?}, got {:?}",
                        harness.expected_content(),
                        entries[0].content
                    )));
                }
                TypedEntries::Branch(_, _) => {
                    return Err(ConformanceError::Expectation(
                        "expected leaf root for a single indexed item".into(),
                    ));
                }
            }

            // Failing resolver
            let mut run2 = StreamingIndexingRun::<H::Ref, _, _, _, _>::new(
                harness.failing_resolver(),
                FixedEmbeddingProvider,
                FixedCanonicalEmbeddingPolicy,
                FixedClusteringFactory,
                conformance_embedding_spec(),
                conformance_block_size_target(),
            );
            match run2.ingest_batch(std::slice::from_ref(&item)).await {
                Err(StreamingIndexerError::ContentResolution(_)) => {}
                other => {
                    return Err(ConformanceError::Expectation(format!(
                        "expected ContentResolution failure, got {other:?}"
                    )));
                }
            }

            // Unusable resolver (empty media type)
            let mut run3 = StreamingIndexingRun::<H::Ref, _, _, _, _>::new(
                harness.unusable_resolver(),
                FixedEmbeddingProvider,
                FixedCanonicalEmbeddingPolicy,
                FixedClusteringFactory,
                conformance_embedding_spec(),
                conformance_block_size_target(),
            );
            match run3.ingest_batch(&[item]).await {
                Err(StreamingIndexerError::UnusableContent(_)) => {}
                other => {
                    return Err(ConformanceError::Expectation(format!(
                        "expected UnusableContent failure, got {other:?}"
                    )));
                }
            }

            Ok(())
        })
    }

    pub fn run_canonical_embedding_policy_suite<H>(harness: &H) -> ConformanceResult
    where
        H: CanonicalEmbeddingPolicyConformanceHarness,
    {
        pollster::block_on(async {
            // Multi-item items → needs a parent layer → canonical policy is invoked
            let items = conformance_multi_items();
            let store = MemoryBlockStore::default();

            let mut run = StreamingIndexingRun::<u8, _, _, _, _>::new(
                FixedResolver,
                FixedMultiEmbeddingProvider,
                harness.conforming_policy(),
                FixedClusteringFactory,
                conformance_embedding_spec(),
                conformance_block_size_target(),
            );
            for item in &items {
                run.ingest_batch(std::slice::from_ref(item)).await?;
            }
            run.finish_pass()?;
            run.mark_training_complete()?;
            let result = run
                .finalize(std::iter::once(items.as_slice()), &store)
                .await?;

            let root = store
                .get(&result.root_id)
                .map_err(StreamingIndexerError::Storage)?
                .ok_or_else(|| {
                    ConformanceError::Expectation("root block must be present".into())
                })?;
            match into_entries(root) {
                TypedEntries::Branch(_, entries) if entries.len() >= 2 => {}
                TypedEntries::Branch(_, entries) => {
                    return Err(ConformanceError::Expectation(format!(
                        "expected ≥2 branch entries, got {}",
                        entries.len()
                    )));
                }
                TypedEntries::Leaf(_, _) => {
                    return Err(ConformanceError::Expectation(
                        "expected branch root for multi-item indexing".into(),
                    ));
                }
            }

            // Failing policy
            let mut run_fail = StreamingIndexingRun::<u8, _, _, _, _>::new(
                FixedResolver,
                FixedMultiEmbeddingProvider,
                harness.failing_policy(),
                FixedClusteringFactory,
                conformance_embedding_spec(),
                conformance_block_size_target(),
            );
            for item in &items {
                run_fail.ingest_batch(std::slice::from_ref(item)).await?;
            }
            run_fail.finish_pass()?;
            run_fail.mark_training_complete()?;
            match run_fail
                .finalize(
                    std::iter::once(items.as_slice()),
                    &MemoryBlockStore::default(),
                )
                .await
            {
                Err(StreamingIndexerError::CanonicalEmbeddingFailure(_)) => {}
                other => {
                    return Err(ConformanceError::Expectation(format!(
                        "expected CanonicalEmbeddingFailure, got {other:?}"
                    )));
                }
            }

            // Invalid-length policy
            let mut run_inv = StreamingIndexingRun::<u8, _, _, _, _>::new(
                FixedResolver,
                FixedMultiEmbeddingProvider,
                harness.invalid_length_policy(),
                FixedClusteringFactory,
                conformance_embedding_spec(),
                conformance_block_size_target(),
            );
            for item in &items {
                run_inv.ingest_batch(std::slice::from_ref(item)).await?;
            }
            run_inv.finish_pass()?;
            run_inv.mark_training_complete()?;
            match run_inv
                .finalize(
                    std::iter::once(items.as_slice()),
                    &MemoryBlockStore::default(),
                )
                .await
            {
                Err(StreamingIndexerError::CanonicalEmbeddingFailure(_)) => {}
                other => {
                    return Err(ConformanceError::Expectation(format!(
                        "expected CanonicalEmbeddingFailure for invalid length, got {other:?}"
                    )));
                }
            }

            Ok(())
        })
    }

    pub fn run_streaming_factory_suite<H>(harness: &H) -> ConformanceResult
    where
        H: StreamingClusteringFactoryConformanceHarness,
    {
        pollster::block_on(async {
            let items = conformance_multi_items();
            let store = MemoryBlockStore::default();

            let mut run = StreamingIndexingRun::<u8, _, _, _, _>::new(
                FixedResolver,
                FixedMultiEmbeddingProvider,
                FixedCanonicalEmbeddingPolicy,
                harness.conforming_factory(),
                conformance_embedding_spec(),
                conformance_block_size_target(),
            );
            for item in &items {
                run.ingest_batch(std::slice::from_ref(item)).await?;
            }
            run.finish_pass()?;
            run.mark_training_complete()?;
            let result = run
                .finalize(std::iter::once(items.as_slice()), &store)
                .await?;

            if result.block_ids.is_empty() {
                return Err(ConformanceError::Expectation(
                    "expected non-empty block set".into(),
                ));
            }

            Ok(())
        })
    }

    pub fn run_full_trait_suite<CR, CEP, SCF>(
        content_harness: &CR,
        canonical_harness: &CEP,
        factory_harness: &SCF,
    ) -> ConformanceResult
    where
        CR: ContentResolverConformanceHarness,
        CEP: CanonicalEmbeddingPolicyConformanceHarness,
        SCF: StreamingClusteringFactoryConformanceHarness,
    {
        run_content_resolver_suite(content_harness)?;
        run_canonical_embedding_policy_suite(canonical_harness)?;
        run_streaming_factory_suite(factory_harness)
    }

    // ── Internal fixture types used by suite runners ──────────

    #[derive(Clone, Copy)]
    struct FixedResolver;

    impl ContentResolver<u8> for FixedResolver {
        type Error = FixtureError;
        fn resolve(&self, r: &u8) -> Result<Content, Self::Error> {
            Ok(Content {
                media_type: "text/plain".into(),
                body: vec![*r],
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
                .ok_or_else(|| FixtureError("expected non-empty content".into()))?;
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
                .map(|e| e.embedding.clone())
                .ok_or_else(|| FixtureError("expected branch block to have entries".into()))
        }
    }

    /// Single-cluster factory: puts everything into one group, used for
    /// conformance tests where we control the tree shape.
    #[derive(Clone, Copy)]
    struct FixedClusteringFactory;

    impl StreamingClusteringFactory for FixedClusteringFactory {
        type Trainer = DcbcStreamingTrainer;
        type Error = StreamingClusteringError;

        fn create_trainer(
            &self,
            dimensions: usize,
            _estimated_child_count: usize,
            _block_size_target: usize,
            _embedding_spec: &EmbeddingSpec,
        ) -> Result<DcbcStreamingTrainer, StreamingClusteringError> {
            // Use cluster_count=1 when count is known so N≥K is guaranteed.
            let cluster_count = 1;
            DcbcStreamingTrainer::new(StreamingClusteringConfig {
                cluster_count,
                dimensions,
                balance_constraints: None,
                random_seed: None,
            })
        }
    }

    fn conformance_embedding_spec() -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: "i8".into(),
        }
    }

    fn conformance_block_size_target() -> usize {
        256
    }

    fn conformance_multi_items() -> Vec<IndexItem<u8>> {
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
    //! lexongraph-streaming-indexer = { version = "*", features = ["conformance"] }
    //! ```

    pub use super::conformance_support::{
        CanonicalEmbeddingPolicyConformanceHarness, ConformanceError, ConformanceResult,
        ContentResolverConformanceHarness, FixtureError,
        StreamingClusteringFactoryConformanceHarness, run_canonical_embedding_policy_suite,
        run_content_resolver_suite, run_full_trait_suite, run_streaming_factory_suite,
    };
}
