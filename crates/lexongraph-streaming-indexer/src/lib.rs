// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Protocol-conforming LexonGraph streaming indexing orchestration.
//!
//! This crate exposes a caller-visible, replay-based streaming lifecycle for
//! planning and materializing large datasets. Callers drive one or more
//! planning passes (each a full replay of the item set in batches), then mark
//! planning complete, then supply a final materialization replay. Hierarchical
//! planning is derived over replayed original-item embeddings, and final block
//! assembly proceeds bottom-up from the finalized partition hierarchy.
//!
//! ```compile_fail
//! #[cfg(feature = "conformance")]
//! compile_error!("the conformance module is intentionally enabled in this doctest configuration");
//!
//! use lexongraph_streaming_indexer::conformance;
//!
//! let _ = std::any::type_name::<conformance::ConformanceError>();
//! ```

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use ciborium::{Value, ser::into_writer};
use half::f16;
use lexongraph_adaptive_planning_policy::AdaptivePlanningSelector;
pub use lexongraph_adaptive_planning_policy::{
    ActivePlanningAlgorithm, AdaptiveDcbcSettings, AdaptiveDirectionalPcaSettings,
    AdaptiveDivisiveSwitchSettings, AdaptivePlanningDecisionReason, AdaptivePlanningDiagnostics,
    AdaptivePlanningDirection, AdaptivePlanningError, AdaptivePlanningSettings,
    AdaptiveSwitchDecisionRecord, DEFAULT_DCBC_MAX_EMBEDDING_COUNT, DEFAULT_EMBEDDING_COUNT_CUTOFF,
    DEFAULT_PC1_EXPLAINED_VARIANCE_RATIO_THRESHOLD,
};
use lexongraph_block::{
    Block, BlockError, BranchEntry, LeafEntry, VERSION_1, build_branch_block, build_leaf_block,
    canonicalize_metadata, serialize_block,
};
pub use lexongraph_block::{
    BlockHash, BranchBlock, Content, EmbeddingSpec, Metadata, SerializedBlock,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_dcbc_streaming::DcbcStreamingTrainer;
use lexongraph_directional_pca::{DirectionalPcaParams, DirectionalPcaStreamingTrainer};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
pub use lexongraph_streaming_clustering::{BalanceConstraints, MetricDirection};
use lexongraph_streaming_clustering::{
    ClusterId, PassReport, StreamingClusterClassifier, StreamingClusterTrainer,
    StreamingClusteringConfig, StreamingClusteringError,
};
use sha2::{Digest, Sha256};

// ─────────────────────────────────────────────────────────────
// Public input / output types
// ─────────────────────────────────────────────────────────────

/// One caller-supplied indexing unit carrying application metadata and a
/// content reference. Raw content bytes are intentionally absent; they are
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

/// Report returned after each completed planning pass.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexingPassReport {
    pub observed_item_count: usize,
    pub completed_pass_count: usize,
    pub planning_quality_metric: f64,
    pub planning_balance_metric: f64,
    pub planning_quality_direction: MetricDirection,
    pub planning_balance_direction: MetricDirection,
    pub planned_partition_count: usize,
    pub terminal_partition_count: usize,
    pub hierarchy_depth: usize,
    pub adaptive_planning: Option<AdaptivePlanningPassTelemetry>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AdaptivePlanningDecisionTelemetry {
    pub boundary_position: usize,
    pub active_algorithm: ActivePlanningAlgorithm,
    pub switch_boundary_occurred: bool,
    pub embedding_count: Option<usize>,
    pub pc1_explained_variance_ratio: Option<f32>,
    pub pc1_explained_variance_ratio_threshold: Option<f32>,
    pub dcbc_max_embedding_count: Option<usize>,
    pub reason: AdaptivePlanningDecisionReason,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AdaptivePlanningPassTelemetry {
    pub pass_number: usize,
    pub switch_occurred: bool,
    pub latest_decision: AdaptivePlanningDecisionTelemetry,
    pub first_switch_boundary_position: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AdaptivePlanningStatusTelemetry {
    pub pass_number: usize,
    pub decision: AdaptivePlanningDecisionTelemetry,
    pub active_subproblem: Option<AdaptivePlanningActiveSubproblemTelemetry>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AdaptivePlanningActiveSubproblemTelemetry {
    pub active_algorithm: ActivePlanningAlgorithm,
    pub active_subproblem_position: Option<usize>,
    pub completed_subproblem_count: usize,
    pub total_subproblem_count: Option<usize>,
    pub active_dcbc_progress: Option<AdaptivePlanningNestedProgressTelemetry>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AdaptivePlanningNestedProgressTelemetry {
    pub completed_unit_count: Option<usize>,
    pub total_unit_count: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PlanningStage {
    Single,
    Coarse,
    Fine,
    Custom,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizedPartition {
    pub id: String,
    pub parent_id: Option<String>,
    pub child_ids: Vec<String>,
    pub item_indices: Vec<usize>,
    pub terminal: bool,
    pub planning_stage: PlanningStage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizedPartitionHierarchy {
    pub root_partition_id: String,
    pub partitions: Vec<FinalizedPartition>,
}

pub struct PlanningPassOutcome {
    pub hierarchy: FinalizedPartitionHierarchy,
    pub planning_quality_metric: f64,
    pub planning_balance_metric: f64,
    pub planning_quality_direction: MetricDirection,
    pub planning_balance_direction: MetricDirection,
    pub stages_used: BTreeSet<PlanningStage>,
}

// ─────────────────────────────────────────────────────────────
// Indexer-owned trait definitions
// ─────────────────────────────────────────────────────────────

pub trait ContentResolver<R> {
    type Error: std::error::Error;
    fn resolve(&self, content_ref: &R) -> Result<Content, Self::Error>;
    fn fingerprint(&self, content_ref: &R) -> Result<BlockHash, Self::Error>;
}

pub trait CanonicalEmbeddingPolicy {
    type Error: std::error::Error;
    fn canonical_embedding(&self, block: &BranchBlock) -> Result<Vec<u8>, Self::Error>;
}

/// Lower-level shared clustering seam used by the built-in and adapter-based
/// planning paths.
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

/// Caller-supplied hierarchical planning seam.
pub trait HierarchicalPlanningPolicy {
    type Error: std::error::Error;

    fn declared_stages(&self) -> BTreeSet<PlanningStage> {
        BTreeSet::from([PlanningStage::Custom])
    }

    fn adaptive_decision_records(&self) -> &[AdaptiveSwitchDecisionRecord] {
        &[]
    }

    fn finish_planning_pass(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
    ) -> Result<PlanningPassOutcome, Self::Error>;

    fn finish_planning_pass_with_stage_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
        mut observe_stage: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(
            PlanningStage,
            usize,
            StreamingIndexingStatusState,
            Option<AdaptivePlanningDecisionTelemetry>,
        ),
    {
        for stage in self.declared_stages() {
            observe_stage(
                stage,
                embeddings.len(),
                StreamingIndexingStatusState::Started,
                None,
            );
            observe_stage(
                stage,
                embeddings.len(),
                StreamingIndexingStatusState::InProgress,
                None,
            );
        }
        self.finish_planning_pass(
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
        )
    }

    fn finish_planning_pass_with_detailed_stage_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
        mut observe_stage: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(
            PlanningStage,
            usize,
            StreamingIndexingStatusState,
            Option<AdaptivePlanningDecisionTelemetry>,
            Option<AdaptivePlanningActiveSubproblemTelemetry>,
            bool,
        ),
    {
        self.finish_planning_pass_with_stage_observer(
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
            |stage, item_count, state, adaptive_decision| {
                observe_stage(stage, item_count, state, adaptive_decision, None, true);
            },
        )
    }
}

// ─────────────────────────────────────────────────────────────
// Error type
// ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamingIndexerError {
    EmptyInput,
    EmptyPass(String),
    ReplayMismatch(String),
    InvalidMetadata(String),
    ContentResolution(String),
    UnusableContent(String),
    EmbeddingFailure(String),
    ClusteringFailure(String),
    InvalidHybridPlanningConfiguration(String),
    InvalidAdaptivePlanningConfiguration(String),
    HierarchyValidation(String),
    CanonicalEmbeddingFailure(String),
    IntermediateNodeTooLarge {
        min_serialized_bytes: usize,
        size_target: usize,
    },
    TerminalPartitionMaterialization(String),
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
            Self::InvalidMetadata(m) => write!(f, "metadata is invalid: {m}"),
            Self::ContentResolution(m) => write!(f, "content resolution failed: {m}"),
            Self::UnusableContent(m) => write!(f, "resolved content is unusable: {m}"),
            Self::EmbeddingFailure(m) => write!(f, "embedding generation failed: {m}"),
            Self::ClusteringFailure(m) => write!(f, "clustering failed: {m}"),
            Self::InvalidHybridPlanningConfiguration(m) => {
                write!(f, "hybrid planning configuration is invalid: {m}")
            }
            Self::InvalidAdaptivePlanningConfiguration(m) => {
                write!(f, "adaptive planning configuration is invalid: {m}")
            }
            Self::HierarchyValidation(m) => write!(f, "partition hierarchy is invalid: {m}"),
            Self::CanonicalEmbeddingFailure(m) => {
                write!(f, "canonical embedding selection failed: {m}")
            }
            Self::IntermediateNodeTooLarge {
                min_serialized_bytes,
                size_target,
            } => write!(
                f,
                "smallest intermediate node needs {min_serialized_bytes} bytes, \
                 exceeding block size target {size_target}"
            ),
            Self::TerminalPartitionMaterialization(m) => {
                write!(f, "terminal partition could not be materialized: {m}")
            }
            Self::BlockConstruction(e) => write!(f, "block construction failed: {e}"),
            Self::Storage(e) => write!(f, "block storage failed: {e}"),
            Self::InvalidLifecycleTransition(m) => write!(f, "invalid lifecycle transition: {m}"),
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

impl From<StreamingClusteringError> for StreamingIndexerError {
    fn from(error: StreamingClusteringError) -> Self {
        Self::ClusteringFailure(error.to_string())
    }
}

// ─────────────────────────────────────────────────────────────
// Status observer
// ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamingIndexingPhase {
    PlanningPass { pass_number: usize },
    HierarchyPlanning { stage: PlanningStage },
    FinalMaterializationReplay,
    BottomUpAssembly { layer_index: usize },
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
    pub phase_total_unit_count: Option<usize>,
    pub completed_unit_count: usize,
    pub remaining_unit_count: Option<usize>,
    pub elapsed: Duration,
    pub error: Option<String>,
    pub adaptive_planning: Option<AdaptivePlanningStatusTelemetry>,
}

pub type StreamingIndexingStatusObserver =
    Arc<dyn Fn(StreamingIndexingStatus) + Send + Sync + 'static>;

const STATUS_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

// ─────────────────────────────────────────────────────────────
// Built-in canonical-embedding policy
// ─────────────────────────────────────────────────────────────

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
// Built-in planning configuration
// ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuiltInPlanningDirection {
    Divisive,
    Agglomerative,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DcbcBuiltInPlanningSettings {
    pub direction: BuiltInPlanningDirection,
    pub cluster_count: u32,
    pub balance_constraints: Option<BalanceConstraints>,
    pub random_seed: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectionalPcaBuiltInPlanningSettings {
    pub direction: BuiltInPlanningDirection,
    pub cluster_count: u32,
    pub random_seed: Option<u64>,
    pub params: DirectionalPcaParams,
}

#[derive(Clone, Debug, PartialEq)]
pub enum BuiltInPlanningPhase {
    Dcbc(DcbcBuiltInPlanningSettings),
    DirectionalPca(DirectionalPcaBuiltInPlanningSettings),
}

impl BuiltInPlanningPhase {
    fn direction(&self) -> BuiltInPlanningDirection {
        match self {
            Self::Dcbc(settings) => settings.direction,
            Self::DirectionalPca(settings) => settings.direction,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct HybridBuiltInPlanningSettings {
    pub coarse: BuiltInPlanningPhase,
    pub fine: BuiltInPlanningPhase,
    pub fine_partition_max_items: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub enum BuiltInPlanning {
    Dcbc(DcbcBuiltInPlanningSettings),
    DirectionalPca(DirectionalPcaBuiltInPlanningSettings),
    Hybrid(HybridBuiltInPlanningSettings),
    Adaptive(AdaptivePlanningSettings),
}

#[derive(Clone, Debug, PartialEq)]
pub struct BuiltInPlanningPolicy {
    planning: BuiltInPlanning,
    last_adaptive_decision_records: Vec<AdaptiveSwitchDecisionRecord>,
}

impl BuiltInPlanningPolicy {
    pub fn new(planning: BuiltInPlanning) -> Self {
        Self {
            planning,
            last_adaptive_decision_records: Vec::new(),
        }
    }

    pub fn adaptive_decision_records(&self) -> &[AdaptiveSwitchDecisionRecord] {
        &self.last_adaptive_decision_records
    }
}

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
    ) -> Result<Self::Trainer, Self::Error> {
        let materializability_bound =
            materializability_bound(embedding_spec, block_size_target).map_err(invalid_config)?;
        let cluster_count = effective_cluster_count(
            self.cluster_count,
            estimated_child_count,
            materializability_bound,
        )
        .map_err(invalid_config)?;

        DcbcStreamingTrainer::new(StreamingClusteringConfig {
            cluster_count,
            dimensions,
            balance_constraints: None,
            random_seed: None,
        })
    }
}

enum BuiltInStreamingClusterTrainer {
    Dcbc(DcbcStreamingTrainer),
    DirectionalPca(DirectionalPcaStreamingTrainer),
}

enum BuiltInStreamingClusterClassifier {
    Dcbc(<DcbcStreamingTrainer as StreamingClusterTrainer>::Classifier),
    DirectionalPca(<DirectionalPcaStreamingTrainer as StreamingClusterTrainer>::Classifier),
}

impl StreamingClusterClassifier for BuiltInStreamingClusterClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        match self {
            Self::Dcbc(classifier) => classifier.config(),
            Self::DirectionalPca(classifier) => classifier.config(),
        }
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        match self {
            Self::Dcbc(classifier) => classifier.assign(embedding),
            Self::DirectionalPca(classifier) => classifier.assign(embedding),
        }
    }
}

impl StreamingClusterTrainer for BuiltInStreamingClusterTrainer {
    type Classifier = BuiltInStreamingClusterClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        match self {
            Self::Dcbc(trainer) => trainer.config(),
            Self::DirectionalPca(trainer) => trainer.config(),
        }
    }

    fn state(&self) -> lexongraph_streaming_clustering::TrainerState {
        match self {
            Self::Dcbc(trainer) => trainer.state(),
            Self::DirectionalPca(trainer) => trainer.state(),
        }
    }

    fn ingest_batch(&mut self, embeddings: &[Vec<f32>]) -> Result<(), StreamingClusteringError> {
        match self {
            Self::Dcbc(trainer) => trainer.ingest_batch(embeddings),
            Self::DirectionalPca(trainer) => trainer.ingest_batch(embeddings),
        }
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        match self {
            Self::Dcbc(trainer) => trainer.finish_pass(),
            Self::DirectionalPca(trainer) => trainer.finish_pass(),
        }
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        match self {
            Self::Dcbc(trainer) => trainer.complete_training(),
            Self::DirectionalPca(trainer) => trainer.complete_training(),
        }
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        match self {
            Self::Dcbc(trainer) => trainer
                .into_classifier()
                .map(BuiltInStreamingClusterClassifier::Dcbc),
            Self::DirectionalPca(trainer) => trainer
                .into_classifier()
                .map(BuiltInStreamingClusterClassifier::DirectionalPca),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FactoryHierarchicalPlanningPolicy<F> {
    factory: F,
}

pub type StreamingClusteringPlanningPolicy<F> = FactoryHierarchicalPlanningPolicy<F>;

impl<F> FactoryHierarchicalPlanningPolicy<F> {
    pub fn new(factory: F) -> Self {
        Self { factory }
    }
}

impl<F> HierarchicalPlanningPolicy for FactoryHierarchicalPlanningPolicy<F>
where
    F: StreamingClusteringFactory,
{
    type Error = StreamingClusteringError;

    fn finish_planning_pass(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
    ) -> Result<PlanningPassOutcome, Self::Error> {
        let mut noop = |_, _, _, _, _, _| {};
        derive_hierarchy_from_factory(
            &self.factory,
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
            &mut noop,
        )
    }

    fn finish_planning_pass_with_stage_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
        mut observe_stage: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(
            PlanningStage,
            usize,
            StreamingIndexingStatusState,
            Option<AdaptivePlanningDecisionTelemetry>,
        ),
    {
        self.finish_planning_pass_with_detailed_stage_observer(
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
            |stage, item_count, state, adaptive_decision, _, _| {
                observe_stage(stage, item_count, state, adaptive_decision);
            },
        )
    }

    fn finish_planning_pass_with_detailed_stage_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
        mut observe_stage: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(
            PlanningStage,
            usize,
            StreamingIndexingStatusState,
            Option<AdaptivePlanningDecisionTelemetry>,
            Option<AdaptivePlanningActiveSubproblemTelemetry>,
            bool,
        ),
    {
        derive_hierarchy_from_factory(
            &self.factory,
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
            &mut observe_stage,
        )
    }
}

impl HierarchicalPlanningPolicy for BuiltInPlanningPolicy {
    type Error = StreamingIndexerError;

    fn declared_stages(&self) -> BTreeSet<PlanningStage> {
        match &self.planning {
            BuiltInPlanning::Dcbc(_)
            | BuiltInPlanning::DirectionalPca(_)
            | BuiltInPlanning::Adaptive(_) => BTreeSet::from([PlanningStage::Single]),
            BuiltInPlanning::Hybrid(_) => {
                BTreeSet::from([PlanningStage::Coarse, PlanningStage::Fine])
            }
        }
    }

    fn finish_planning_pass(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
    ) -> Result<PlanningPassOutcome, Self::Error> {
        let mut noop = |_, _, _, _, _, _| {};
        let (outcome, decision_records) = derive_hierarchy_from_built_in(
            &self.planning,
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
            &mut noop,
        )?;
        self.last_adaptive_decision_records = decision_records;
        Ok(outcome)
    }

    fn finish_planning_pass_with_stage_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
        mut observe_stage: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(
            PlanningStage,
            usize,
            StreamingIndexingStatusState,
            Option<AdaptivePlanningDecisionTelemetry>,
        ),
    {
        self.finish_planning_pass_with_detailed_stage_observer(
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
            |stage, item_count, state, adaptive_decision, _, _| {
                observe_stage(stage, item_count, state, adaptive_decision);
            },
        )
    }

    fn finish_planning_pass_with_detailed_stage_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
        mut observe_stage: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(
            PlanningStage,
            usize,
            StreamingIndexingStatusState,
            Option<AdaptivePlanningDecisionTelemetry>,
            Option<AdaptivePlanningActiveSubproblemTelemetry>,
            bool,
        ),
    {
        let (outcome, decision_records) = derive_hierarchy_from_built_in(
            &self.planning,
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
            &mut observe_stage,
        )?;
        self.last_adaptive_decision_records = decision_records;
        Ok(outcome)
    }

    fn adaptive_decision_records(&self) -> &[AdaptiveSwitchDecisionRecord] {
        &self.last_adaptive_decision_records
    }
}

// ─────────────────────────────────────────────────────────────
// Internal state helpers
// ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunPhase {
    Planning,
    PlanningComplete,
    Finalized,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct BaselineItem {
    content_ref_hash: BlockHash,
    metadata_hash: BlockHash,
    content_hash: BlockHash,
    embedding_hash: BlockHash,
}

#[derive(Clone)]
struct IndexedChild {
    embedding: Vec<u8>,
    child: BlockHash,
    level: u64,
}

struct LayerBuildStatus<'a> {
    phase: StreamingIndexingPhase,
    started: Instant,
    progress: &'a Arc<AtomicUsize>,
    legacy_item_count: usize,
}

// ─────────────────────────────────────────────────────────────
// StreamingIndexingRun — the public orchestration type
// ─────────────────────────────────────────────────────────────

pub struct StreamingIndexingRun<R, CR, EP, CEP, HPP> {
    resolver: CR,
    embedding_provider: EP,
    canonical_embedding_policy: CEP,
    planning_policy: HPP,
    embedding_spec: EmbeddingSpec,
    block_size_target: usize,
    observer: Option<StreamingIndexingStatusObserver>,

    phase: RunPhase,
    completed_passes: usize,
    baseline: Option<Vec<BaselineItem>>,
    finalized_hierarchy: Option<FinalizedPartitionHierarchy>,
    current_pass_items: Vec<BaselineItem>,
    current_pass_f32_embeddings: Vec<Vec<f32>>,
    items_seen_in_current_pass: usize,
    _item_ref: PhantomData<R>,
}

impl<R, CR, EP>
    StreamingIndexingRun<R, CR, EP, ArithmeticMeanCanonicalEmbeddingPolicy, BuiltInPlanningPolicy>
{
    pub fn with_builtin_planning(
        resolver: CR,
        embedding_provider: EP,
        planning: BuiltInPlanning,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Self {
        Self::new(
            resolver,
            embedding_provider,
            ArithmeticMeanCanonicalEmbeddingPolicy,
            BuiltInPlanningPolicy::new(planning),
            embedding_spec,
            block_size_target,
        )
    }
}

impl<R, CR, EP, CEP> StreamingIndexingRun<R, CR, EP, CEP, BuiltInPlanningPolicy> {
    pub fn with_canonical_policy(
        resolver: CR,
        embedding_provider: EP,
        canonical_embedding_policy: CEP,
        planning: BuiltInPlanning,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Self {
        Self::new(
            resolver,
            embedding_provider,
            canonical_embedding_policy,
            BuiltInPlanningPolicy::new(planning),
            embedding_spec,
            block_size_target,
        )
    }

    pub fn adaptive_decision_records(&self) -> &[AdaptiveSwitchDecisionRecord] {
        self.planning_policy.adaptive_decision_records()
    }
}

impl<R, CR, EP, CEP, F> StreamingIndexingRun<R, CR, EP, CEP, FactoryHierarchicalPlanningPolicy<F>>
where
    F: StreamingClusteringFactory,
{
    pub fn with_streaming_clustering_factory(
        resolver: CR,
        embedding_provider: EP,
        canonical_embedding_policy: CEP,
        factory: F,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Self {
        Self::new(
            resolver,
            embedding_provider,
            canonical_embedding_policy,
            FactoryHierarchicalPlanningPolicy::new(factory),
            embedding_spec,
            block_size_target,
        )
    }
}

impl<R, CR, EP, CEP, HPP> StreamingIndexingRun<R, CR, EP, CEP, HPP> {
    pub fn new(
        resolver: CR,
        embedding_provider: EP,
        canonical_embedding_policy: CEP,
        planning_policy: HPP,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Self {
        Self {
            resolver,
            embedding_provider,
            canonical_embedding_policy,
            planning_policy,
            embedding_spec,
            block_size_target,
            observer: None,
            phase: RunPhase::Planning,
            completed_passes: 0,
            baseline: None,
            finalized_hierarchy: None,
            current_pass_items: Vec::new(),
            current_pass_f32_embeddings: Vec::new(),
            items_seen_in_current_pass: 0,
            _item_ref: PhantomData,
        }
    }

    pub fn with_observer(mut self, observer: StreamingIndexingStatusObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    pub fn completed_passes(&self) -> usize {
        self.completed_passes
    }

    pub fn finalized_partition_hierarchy(&self) -> Option<&FinalizedPartitionHierarchy> {
        self.finalized_hierarchy.as_ref()
    }
}

impl<R, CR, EP, CEP, HPP> StreamingIndexingRun<R, CR, EP, CEP, HPP>
where
    CR: ContentResolver<R>,
    EP: EmbeddingProvider,
    CEP: CanonicalEmbeddingPolicy,
    HPP: HierarchicalPlanningPolicy,
    HPP::Error: 'static,
{
    pub async fn ingest_batch(
        &mut self,
        batch: &[IndexItem<R>],
    ) -> Result<(), StreamingIndexerError> {
        if !matches!(self.phase, RunPhase::Planning) {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                "ingest_batch requires the planning phase (currently {:?})",
                self.phase
            )));
        }

        if batch.is_empty() {
            return Ok(());
        }

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
            let content_ref_hash = self
                .resolver
                .fingerprint(&item.content_ref)
                .map_err(|e| StreamingIndexerError::ContentResolution(e.to_string()))?;
            let replay_item = BaselineItem {
                content_ref_hash,
                metadata_hash: hash_metadata(&item.metadata)
                    .map_err(StreamingIndexerError::InvalidMetadata)?,
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

        for embedding in &embeddings {
            self.current_pass_f32_embeddings
                .push(decode_embedding_as_f32(embedding, &self.embedding_spec)?);
        }
        self.items_seen_in_current_pass += batch.len();
        Ok(())
    }

    pub fn finish_pass(&mut self) -> Result<IndexingPassReport, StreamingIndexerError> {
        if !matches!(self.phase, RunPhase::Planning) {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                "finish_pass requires the planning phase (currently {:?})",
                self.phase
            )));
        }

        if self.items_seen_in_current_pass == 0 {
            return Err(StreamingIndexerError::EmptyPass(
                "at least one item must be ingested before completing a pass".into(),
            ));
        }

        if let Some(baseline) = &self.baseline
            && self.items_seen_in_current_pass != baseline.len()
        {
            return Err(StreamingIndexerError::ReplayMismatch(format!(
                "pass had {} items but baseline has {}",
                self.items_seen_in_current_pass,
                baseline.len()
            )));
        }

        let materializability_bound =
            materializability_bound(&self.embedding_spec, self.block_size_target)
                .map_err(StreamingIndexerError::TerminalPartitionMaterialization)?;

        let pass_number = self.completed_passes + 1;
        let pass_started = Instant::now();
        let pass_total = self.items_seen_in_current_pass;
        let pass_progress = Arc::new(AtomicUsize::new(0));
        emit_status(
            &self.observer,
            status_with_known_total(
                StreamingIndexingPhase::PlanningPass { pass_number },
                StreamingIndexingStatusState::Started,
                pass_total,
                0,
                Duration::ZERO,
                None,
            ),
        );
        emit_status(
            &self.observer,
            status_with_known_total(
                StreamingIndexingPhase::PlanningPass { pass_number },
                StreamingIndexingStatusState::InProgress,
                pass_total,
                0,
                pass_started.elapsed(),
                None,
            ),
        );
        let mut heartbeat = StatusHeartbeatGuard::new(start_status_heartbeat(
            &self.observer,
            StreamingIndexingPhase::PlanningPass { pass_number },
            Some(pass_total),
            Arc::clone(&pass_progress),
            None,
            pass_started,
        ));

        let mut stage_statuses = PlanningStageStatusTracker::new(&self.observer, pass_started);
        let buffered = std::mem::take(&mut self.current_pass_f32_embeddings);
        let outcome = self
            .planning_policy
            .finish_planning_pass_with_detailed_stage_observer(
                &buffered,
                &self.embedding_spec,
                materializability_bound,
                self.block_size_target,
                |stage,
                 item_count,
                 state,
                 adaptive_decision,
                 active_subproblem,
                 advance_completed_work| {
                    stage_statuses.observe(PlanningStageObservation {
                        pass_number,
                        stage,
                        state,
                        item_count,
                        adaptive_decision,
                        active_subproblem,
                        advance_completed_work,
                    });
                },
            )
            .map_err(map_planning_policy_error);

        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                self.current_pass_f32_embeddings = buffered;
                heartbeat.stop();
                stage_statuses.fail_all(pass_started.elapsed(), &error.to_string());
                emit_status(
                    &self.observer,
                    status_with_known_total(
                        StreamingIndexingPhase::PlanningPass { pass_number },
                        StreamingIndexingStatusState::Failed,
                        pass_total,
                        pass_progress.load(AtomicOrdering::Relaxed),
                        pass_started.elapsed(),
                        Some(error.to_string()),
                    ),
                );
                return Err(error);
            }
        };

        heartbeat.stop();
        if let Err(error) =
            validate_partition_hierarchy(&outcome.hierarchy, self.items_seen_in_current_pass)
                .map_err(StreamingIndexerError::HierarchyValidation)
        {
            self.current_pass_f32_embeddings = buffered;
            stage_statuses.fail_all(pass_started.elapsed(), &error.to_string());
            emit_status(
                &self.observer,
                status_with_known_total(
                    StreamingIndexingPhase::PlanningPass { pass_number },
                    StreamingIndexingStatusState::Failed,
                    pass_total,
                    pass_progress.load(AtomicOrdering::Relaxed),
                    pass_started.elapsed(),
                    Some(error.to_string()),
                ),
            );
            return Err(error);
        }
        stage_statuses.complete_all(pass_started.elapsed());

        if self.baseline.is_none() {
            self.baseline = Some(std::mem::take(&mut self.current_pass_items));
        }
        self.finalized_hierarchy = Some(outcome.hierarchy.clone());
        self.completed_passes += 1;
        self.items_seen_in_current_pass = 0;

        emit_status(
            &self.observer,
            status_with_known_total(
                StreamingIndexingPhase::PlanningPass { pass_number },
                StreamingIndexingStatusState::Completed,
                pass_total,
                pass_total,
                pass_started.elapsed(),
                None,
            ),
        );

        let hierarchy_stats = hierarchy_stats(&outcome.hierarchy);
        let adaptive_planning = adaptive_pass_telemetry(
            pass_number,
            self.planning_policy.adaptive_decision_records(),
        );
        Ok(IndexingPassReport {
            observed_item_count: self.baseline.as_ref().map_or(0, std::vec::Vec::len),
            completed_pass_count: self.completed_passes,
            planning_quality_metric: outcome.planning_quality_metric,
            planning_balance_metric: outcome.planning_balance_metric,
            planning_quality_direction: outcome.planning_quality_direction,
            planning_balance_direction: outcome.planning_balance_direction,
            planned_partition_count: hierarchy_stats.partition_count,
            terminal_partition_count: hierarchy_stats.terminal_partition_count,
            hierarchy_depth: hierarchy_stats.depth,
            adaptive_planning,
        })
    }

    pub fn mark_planning_complete(&mut self) -> Result<(), StreamingIndexerError> {
        if !matches!(self.phase, RunPhase::Planning) {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                "mark_planning_complete requires the planning phase (currently {:?})",
                self.phase
            )));
        }
        if self.completed_passes == 0 {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(
                "planning completion requires at least one completed pass".into(),
            ));
        }
        if self.items_seen_in_current_pass > 0 {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(
                "cannot complete planning with an open (unfinished) pass".into(),
            ));
        }
        let hierarchy = self.finalized_hierarchy.as_ref().ok_or_else(|| {
            StreamingIndexerError::InvalidLifecycleTransition(
                "no finalized partition hierarchy is available".into(),
            )
        })?;
        let baseline_len = self.baseline.as_ref().map_or(0, std::vec::Vec::len);
        validate_partition_hierarchy(hierarchy, baseline_len)
            .map_err(StreamingIndexerError::HierarchyValidation)?;
        self.phase = RunPhase::PlanningComplete;
        Ok(())
    }

    pub async fn finalize<I, B>(
        &mut self,
        replay_batches: I,
        store: &dyn BlockStore,
    ) -> Result<StreamingIndexingResult, StreamingIndexerError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[IndexItem<R>]>,
    {
        if !matches!(self.phase, RunPhase::PlanningComplete) {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                "finalize requires the planning-complete phase (currently {:?})",
                self.phase
            )));
        }

        let baseline = self.baseline.as_ref().ok_or_else(|| {
            StreamingIndexerError::InvalidLifecycleTransition("no baseline established".into())
        })?;
        let hierarchy = self.finalized_hierarchy.as_ref().ok_or_else(|| {
            StreamingIndexerError::InvalidLifecycleTransition(
                "no finalized partition hierarchy is available".into(),
            )
        })?;
        validate_partition_hierarchy(hierarchy, baseline.len())
            .map_err(StreamingIndexerError::HierarchyValidation)?;

        let result = self
            .do_finalize(replay_batches, baseline.as_slice(), hierarchy, store)
            .await;

        if result.is_ok() {
            self.phase = RunPhase::Finalized;
        }
        result
    }

    async fn do_finalize<I, B>(
        &self,
        replay_batches: I,
        baseline: &[BaselineItem],
        hierarchy: &FinalizedPartitionHierarchy,
        store: &dyn BlockStore,
    ) -> Result<StreamingIndexingResult, StreamingIndexerError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[IndexItem<R>]>,
    {
        let materializability_bound =
            materializability_bound(&self.embedding_spec, self.block_size_target)
                .map_err(StreamingIndexerError::TerminalPartitionMaterialization)?;

        let replay_started = Instant::now();
        let replay_total = baseline.len();
        let replay_progress = Arc::new(AtomicUsize::new(0));
        emit_status(
            &self.observer,
            status_with_known_total(
                StreamingIndexingPhase::FinalMaterializationReplay,
                StreamingIndexingStatusState::Started,
                replay_total,
                0,
                Duration::ZERO,
                None,
            ),
        );
        emit_status(
            &self.observer,
            status_with_known_total(
                StreamingIndexingPhase::FinalMaterializationReplay,
                StreamingIndexingStatusState::InProgress,
                replay_total,
                0,
                replay_started.elapsed(),
                None,
            ),
        );
        let mut heartbeat = StatusHeartbeatGuard::new(start_status_heartbeat(
            &self.observer,
            StreamingIndexingPhase::FinalMaterializationReplay,
            Some(replay_total),
            Arc::clone(&replay_progress),
            None,
            replay_started,
        ));

        let replay_result = async {
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

                let mut inputs = Vec::with_capacity(items.len());
                let mut contents = Vec::with_capacity(items.len());
                let mut metadatas = Vec::with_capacity(items.len());
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
                    let content_ref_hash = self
                        .resolver
                        .fingerprint(&item.content_ref)
                        .map_err(|e| StreamingIndexerError::ContentResolution(e.to_string()))?;
                    let expected = &baseline[replay_count + offset];
                    let replay_item = BaselineItem {
                        content_ref_hash,
                        metadata_hash: hash_metadata(&item.metadata)
                            .map_err(StreamingIndexerError::InvalidMetadata)?,
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
                    verify_persisted_block_id(block_id, serialized.hash)?;
                    persisted_ids.push(block_id);
                    leaf_children.push(IndexedChild {
                        embedding: embedding.clone(),
                        child: block_id,
                        level: 0,
                    });
                    replay_progress.fetch_add(1, AtomicOrdering::Relaxed);
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

            Ok((leaf_children, persisted_ids))
        }
        .await;

        heartbeat.stop();
        let (leaf_children, mut persisted_ids) = match replay_result {
            Ok(replay_materialization) => {
                emit_status(
                    &self.observer,
                    status_with_known_total(
                        StreamingIndexingPhase::FinalMaterializationReplay,
                        StreamingIndexingStatusState::Completed,
                        replay_total,
                        replay_total,
                        replay_started.elapsed(),
                        None,
                    ),
                );
                replay_materialization
            }
            Err(error) => {
                emit_status(
                    &self.observer,
                    status_with_known_total(
                        StreamingIndexingPhase::FinalMaterializationReplay,
                        StreamingIndexingStatusState::Failed,
                        replay_total,
                        replay_progress.load(AtomicOrdering::Relaxed),
                        replay_started.elapsed(),
                        Some(error.to_string()),
                    ),
                );
                return Err(error);
            }
        };

        if leaf_children.len() == 1 {
            let root_id = leaf_children[0].child;
            dedup_sort_ids(&mut persisted_ids);
            return Ok(StreamingIndexingResult {
                root_id,
                block_ids: persisted_ids,
            });
        }

        let root_child = self.materialize_hierarchy_bottom_up(
            hierarchy,
            &leaf_children,
            materializability_bound,
            store,
            &mut persisted_ids,
        )?;
        dedup_sort_ids(&mut persisted_ids);
        Ok(StreamingIndexingResult {
            root_id: root_child.child,
            block_ids: persisted_ids,
        })
    }

    fn materialize_hierarchy_bottom_up(
        &self,
        hierarchy: &FinalizedPartitionHierarchy,
        leaf_children: &[IndexedChild],
        materializability_bound: usize,
        store: &dyn BlockStore,
        persisted_ids: &mut Vec<BlockHash>,
    ) -> Result<IndexedChild, StreamingIndexerError> {
        let partitions = hierarchy
            .partitions
            .iter()
            .cloned()
            .map(|partition| (partition.id.clone(), partition))
            .collect::<HashMap<_, _>>();
        self.materialize_partition(
            &hierarchy.root_partition_id,
            &partitions,
            leaf_children,
            materializability_bound,
            store,
            persisted_ids,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn materialize_partition(
        &self,
        partition_id: &str,
        partitions: &HashMap<String, FinalizedPartition>,
        leaf_children: &[IndexedChild],
        materializability_bound: usize,
        store: &dyn BlockStore,
        persisted_ids: &mut Vec<BlockHash>,
    ) -> Result<IndexedChild, StreamingIndexerError> {
        let partition = partitions.get(partition_id).ok_or_else(|| {
            StreamingIndexerError::HierarchyValidation(format!(
                "partition {partition_id:?} is missing during final assembly"
            ))
        })?;

        let children = if partition.terminal {
            partition
                .item_indices
                .iter()
                .map(|&index| {
                    leaf_children.get(index).cloned().ok_or_else(|| {
                        StreamingIndexerError::HierarchyValidation(format!(
                            "terminal partition {partition_id:?} references missing item index {index}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            partition
                .child_ids
                .iter()
                .map(|child_id| {
                    self.materialize_partition(
                        child_id,
                        partitions,
                        leaf_children,
                        materializability_bound,
                        store,
                        persisted_ids,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        self.assemble_child_set(children, materializability_bound, store, persisted_ids)
    }

    fn assemble_child_set(
        &self,
        children: Vec<IndexedChild>,
        materializability_bound: usize,
        store: &dyn BlockStore,
        persisted_ids: &mut Vec<BlockHash>,
    ) -> Result<IndexedChild, StreamingIndexerError> {
        let mut current = normalize_current_layer(children);
        if current.is_empty() {
            return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                "child set normalized to zero entries".into(),
            ));
        }
        if current.len() == 1 {
            return Ok(current.remove(0));
        }

        loop {
            if current.len() == 1 {
                return Ok(current.remove(0));
            }

            let groups = balanced_groups(current.len(), materializability_bound)
                .map_err(StreamingIndexerError::TerminalPartitionMaterialization)?;
            let layer_index =
                usize::try_from(current.iter().map(|child| child.level).max().unwrap_or(0))
                    .map_err(|_| {
                        StreamingIndexerError::TerminalPartitionMaterialization(
                            "semantic bottom-up layer index does not fit usize".into(),
                        )
                    })?;
            let phase = StreamingIndexingPhase::BottomUpAssembly { layer_index };
            let started = Instant::now();
            let legacy_item_count = current.len();
            let phase_total = groups.len();
            let phase_progress = Arc::new(AtomicUsize::new(0));
            emit_status(
                &self.observer,
                with_legacy_item_count(
                    status_with_known_total(
                        phase.clone(),
                        StreamingIndexingStatusState::Started,
                        phase_total,
                        0,
                        Duration::ZERO,
                        None,
                    ),
                    legacy_item_count,
                ),
            );
            emit_status(
                &self.observer,
                with_legacy_item_count(
                    status_with_known_total(
                        phase.clone(),
                        StreamingIndexingStatusState::InProgress,
                        phase_total,
                        0,
                        started.elapsed(),
                        None,
                    ),
                    legacy_item_count,
                ),
            );
            let mut heartbeat = StatusHeartbeatGuard::new(start_status_heartbeat(
                &self.observer,
                phase.clone(),
                Some(phase_total),
                Arc::clone(&phase_progress),
                Some(legacy_item_count),
                started,
            ));

            let next_level = current.iter().map(|child| child.level).max().unwrap_or(0) + 1;
            let next_layer = match self.build_branch_layer(
                &current,
                &groups,
                next_level,
                LayerBuildStatus {
                    phase: phase.clone(),
                    started,
                    progress: &phase_progress,
                    legacy_item_count,
                },
                store,
                persisted_ids,
            ) {
                Ok(next_layer) => next_layer,
                Err(error) => {
                    heartbeat.stop();
                    emit_status(
                        &self.observer,
                        with_legacy_item_count(
                            status_with_known_total(
                                phase,
                                StreamingIndexingStatusState::Failed,
                                phase_total,
                                phase_progress.load(AtomicOrdering::Relaxed),
                                started.elapsed(),
                                Some(error.to_string()),
                            ),
                            legacy_item_count,
                        ),
                    );
                    return Err(error);
                }
            };
            current = normalize_current_layer(next_layer);

            heartbeat.stop();
            emit_status(
                &self.observer,
                with_legacy_item_count(
                    status_with_known_total(
                        phase,
                        StreamingIndexingStatusState::Completed,
                        phase_total,
                        phase_total,
                        started.elapsed(),
                        None,
                    ),
                    legacy_item_count,
                ),
            );
        }
    }

    fn build_branch_layer(
        &self,
        children: &[IndexedChild],
        groups: &[Vec<usize>],
        parent_level: u64,
        status: LayerBuildStatus<'_>,
        store: &dyn BlockStore,
        persisted_ids: &mut Vec<BlockHash>,
    ) -> Result<Vec<IndexedChild>, StreamingIndexerError> {
        let mut next_layer = Vec::with_capacity(groups.len());

        for group in groups {
            let raw_entries = group
                .iter()
                .map(|&index| BranchEntry {
                    embedding: children[index].embedding.clone(),
                    child: children[index].child,
                })
                .collect::<Vec<_>>();
            let entries = normalize_branch_entries(raw_entries);
            if entries.len() < 2 {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    "normalized child-bearing entry set has fewer than two unique children".into(),
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
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    format!(
                        "branch block serialized to {} bytes, exceeding block size target {}",
                        serialized.bytes.len(),
                        self.block_size_target
                    ),
                ));
            }

            let block_id = store
                .put(&branch_block)
                .map_err(StreamingIndexerError::Storage)?;
            verify_persisted_block_id(block_id, serialized.hash)?;
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
            status.progress.fetch_add(1, AtomicOrdering::Relaxed);
        }

        emit_status(
            &self.observer,
            with_legacy_item_count(
                status_with_known_total(
                    status.phase,
                    StreamingIndexingStatusState::InProgress,
                    groups.len(),
                    status.progress.load(AtomicOrdering::Relaxed),
                    status.started.elapsed(),
                    None,
                ),
                status.legacy_item_count,
            ),
        );

        Ok(next_layer)
    }
}

// ─────────────────────────────────────────────────────────────
// Built-in / factory-based planning
// ─────────────────────────────────────────────────────────────

fn derive_hierarchy_from_built_in(
    planning: &BuiltInPlanning,
    embeddings: &[Vec<f32>],
    embedding_spec: &EmbeddingSpec,
    materializability_bound: usize,
    _block_size_target: usize,
    stage_observer: &mut impl FnMut(
        PlanningStage,
        usize,
        StreamingIndexingStatusState,
        Option<AdaptivePlanningDecisionTelemetry>,
        Option<AdaptivePlanningActiveSubproblemTelemetry>,
        bool,
    ),
) -> Result<(PlanningPassOutcome, Vec<AdaptiveSwitchDecisionRecord>), StreamingIndexerError> {
    match planning {
        BuiltInPlanning::Dcbc(settings) => derive_hierarchy_for_single_built_in_phase(
            BuiltInPlanningPhase::Dcbc(settings.clone()),
            embeddings,
            embedding_spec,
            materializability_bound,
            stage_observer,
        )
        .map(|outcome| (outcome, Vec::new())),
        BuiltInPlanning::DirectionalPca(settings) => derive_hierarchy_for_single_built_in_phase(
            BuiltInPlanningPhase::DirectionalPca(settings.clone()),
            embeddings,
            embedding_spec,
            materializability_bound,
            stage_observer,
        )
        .map(|outcome| (outcome, Vec::new())),
        BuiltInPlanning::Hybrid(settings) => {
            if settings.fine_partition_max_items < 2 {
                return Err(StreamingIndexerError::InvalidHybridPlanningConfiguration(
                    "fine_partition_max_items must be at least 2".into(),
                ));
            }
            let Some(direction) = hybrid_direction(settings) else {
                return Err(StreamingIndexerError::InvalidHybridPlanningConfiguration(
                    "hybrid built-in planning requires coarse and fine phases to use the same direction".into(),
                ));
            };
            match direction {
                BuiltInPlanningDirection::Divisive => derive_hierarchy_with_builder(
                    embeddings,
                    materializability_bound,
                    stage_observer,
                    |partition_embeddings| {
                        let (stage, phase) =
                            select_hybrid_phase(settings, partition_embeddings.len());
                        Ok((
                            PartitionPlanner::new(
                                stage,
                                create_built_in_trainer(
                                    &phase,
                                    partition_embeddings.len(),
                                    partition_embeddings.first().map_or(0, std::vec::Vec::len),
                                    embedding_spec,
                                    materializability_bound,
                                )?,
                            ),
                            None,
                        ))
                    },
                )
                .map(|outcome| (outcome, Vec::new())),
                BuiltInPlanningDirection::Agglomerative => {
                    derive_hierarchy_agglomeratively_with_builder(
                        embeddings,
                        materializability_bound,
                        stage_observer,
                        |layer_embeddings, _represented_item_count, max_unit_item_count| {
                            let (stage, phase) = select_hybrid_phase(settings, max_unit_item_count);
                            Ok((
                                PartitionPlanner::new(
                                    stage,
                                    create_built_in_trainer(
                                        &phase,
                                        layer_embeddings.len(),
                                        layer_embeddings.first().map_or(0, std::vec::Vec::len),
                                        embedding_spec,
                                        materializability_bound,
                                    )?,
                                ),
                                None,
                            ))
                        },
                    )
                    .map(|outcome| (outcome, Vec::new()))
                }
            }
        }
        BuiltInPlanning::Adaptive(settings) => derive_hierarchy_for_adaptive_built_in(
            settings,
            embeddings,
            embedding_spec,
            materializability_bound,
            stage_observer,
        ),
    }
}

fn derive_hierarchy_for_single_built_in_phase(
    phase: BuiltInPlanningPhase,
    embeddings: &[Vec<f32>],
    embedding_spec: &EmbeddingSpec,
    materializability_bound: usize,
    stage_observer: &mut impl FnMut(
        PlanningStage,
        usize,
        StreamingIndexingStatusState,
        Option<AdaptivePlanningDecisionTelemetry>,
        Option<AdaptivePlanningActiveSubproblemTelemetry>,
        bool,
    ),
) -> Result<PlanningPassOutcome, StreamingIndexerError> {
    match phase.direction() {
        BuiltInPlanningDirection::Divisive => derive_hierarchy_with_builder(
            embeddings,
            materializability_bound,
            stage_observer,
            |partition_embeddings| {
                Ok((
                    PartitionPlanner::new(
                        PlanningStage::Single,
                        create_built_in_trainer(
                            &phase,
                            partition_embeddings.len(),
                            partition_embeddings.first().map_or(0, std::vec::Vec::len),
                            embedding_spec,
                            materializability_bound,
                        )?,
                    ),
                    None,
                ))
            },
        ),
        BuiltInPlanningDirection::Agglomerative => derive_hierarchy_agglomeratively_with_builder(
            embeddings,
            materializability_bound,
            stage_observer,
            |layer_embeddings, _represented_item_count, _max_unit_item_count| {
                Ok((
                    PartitionPlanner::new(
                        PlanningStage::Single,
                        create_built_in_trainer(
                            &phase,
                            layer_embeddings.len(),
                            layer_embeddings.first().map_or(0, std::vec::Vec::len),
                            embedding_spec,
                            materializability_bound,
                        )?,
                    ),
                    None,
                ))
            },
        ),
    }
}

fn select_hybrid_phase(
    settings: &HybridBuiltInPlanningSettings,
    represented_item_count: usize,
) -> (PlanningStage, BuiltInPlanningPhase) {
    if represented_item_count <= settings.fine_partition_max_items {
        (PlanningStage::Fine, settings.fine.clone())
    } else {
        (PlanningStage::Coarse, settings.coarse.clone())
    }
}

fn hybrid_direction(settings: &HybridBuiltInPlanningSettings) -> Option<BuiltInPlanningDirection> {
    let coarse = settings.coarse.direction();
    let fine = settings.fine.direction();
    (coarse == fine).then_some(coarse)
}

fn create_built_in_trainer(
    phase: &BuiltInPlanningPhase,
    partition_len: usize,
    dimensions: usize,
    _embedding_spec: &EmbeddingSpec,
    materializability_bound: usize,
) -> Result<BuiltInStreamingClusterTrainer, StreamingIndexerError> {
    match phase {
        BuiltInPlanningPhase::Dcbc(settings) => {
            let cluster_count = effective_cluster_count(
                settings.cluster_count,
                partition_len,
                materializability_bound,
            )
            .map_err(map_clustering_configuration_error)?;
            DcbcStreamingTrainer::new(StreamingClusteringConfig {
                cluster_count,
                dimensions,
                balance_constraints: settings.balance_constraints.clone(),
                random_seed: settings.random_seed,
            })
            .map(BuiltInStreamingClusterTrainer::Dcbc)
            .map_err(map_clustering_error)
        }
        BuiltInPlanningPhase::DirectionalPca(settings) => {
            let cluster_count = effective_cluster_count(
                settings.cluster_count,
                partition_len,
                materializability_bound,
            )
            .map_err(map_clustering_configuration_error)?;
            DirectionalPcaStreamingTrainer::new(
                StreamingClusteringConfig {
                    cluster_count,
                    dimensions,
                    balance_constraints: None,
                    random_seed: settings.random_seed,
                },
                settings.params.clone(),
            )
            .map(BuiltInStreamingClusterTrainer::DirectionalPca)
            .map_err(map_clustering_error)
        }
    }
}

fn derive_hierarchy_for_adaptive_built_in(
    settings: &AdaptivePlanningSettings,
    embeddings: &[Vec<f32>],
    embedding_spec: &EmbeddingSpec,
    materializability_bound: usize,
    stage_observer: &mut impl FnMut(
        PlanningStage,
        usize,
        StreamingIndexingStatusState,
        Option<AdaptivePlanningDecisionTelemetry>,
        Option<AdaptivePlanningActiveSubproblemTelemetry>,
        bool,
    ),
) -> Result<(PlanningPassOutcome, Vec<AdaptiveSwitchDecisionRecord>), StreamingIndexerError> {
    let mut selector =
        AdaptivePlanningSelector::new(settings.clone()).map_err(map_adaptive_planning_error)?;
    let outcome = match settings.direction {
        AdaptivePlanningDirection::Divisive => derive_adaptive_divisive_hierarchy(
            embeddings,
            materializability_bound,
            &mut selector,
            settings,
            embedding_spec,
            stage_observer,
        ),
        AdaptivePlanningDirection::Agglomerative => derive_hierarchy_agglomeratively_with_builder(
            embeddings,
            materializability_bound,
            stage_observer,
            |layer_embeddings, _represented_item_count, _max_unit_item_count| {
                let algorithm = selector
                    .select_algorithm(layer_embeddings)
                    .map_err(map_adaptive_planning_error)?;
                let adaptive_decision = selector
                    .decision_records()
                    .last()
                    .map(adaptive_decision_telemetry);
                let phase = adaptive_phase(settings, algorithm);
                Ok::<_, StreamingIndexerError>((
                    PartitionPlanner::new(
                        PlanningStage::Single,
                        create_built_in_trainer(
                            &phase,
                            layer_embeddings.len(),
                            layer_embeddings.first().map_or(0, std::vec::Vec::len),
                            embedding_spec,
                            materializability_bound,
                        )?,
                    ),
                    adaptive_decision,
                ))
            },
        ),
    }?;
    Ok((outcome, selector.decision_records().to_vec()))
}

fn adaptive_phase(
    settings: &AdaptivePlanningSettings,
    algorithm: ActivePlanningAlgorithm,
) -> BuiltInPlanningPhase {
    let direction = built_in_direction(settings.direction);
    match algorithm {
        ActivePlanningAlgorithm::DirectionalPca => {
            BuiltInPlanningPhase::DirectionalPca(DirectionalPcaBuiltInPlanningSettings {
                direction,
                cluster_count: settings.directional_pca.cluster_count,
                random_seed: settings.directional_pca.random_seed,
                params: settings.directional_pca.params.clone(),
            })
        }
        ActivePlanningAlgorithm::Dcbc => BuiltInPlanningPhase::Dcbc(DcbcBuiltInPlanningSettings {
            direction,
            cluster_count: settings.dcbc.cluster_count,
            balance_constraints: settings.dcbc.balance_constraints.clone(),
            random_seed: settings.dcbc.random_seed,
        }),
    }
}

fn built_in_direction(direction: AdaptivePlanningDirection) -> BuiltInPlanningDirection {
    match direction {
        AdaptivePlanningDirection::Divisive => BuiltInPlanningDirection::Divisive,
        AdaptivePlanningDirection::Agglomerative => BuiltInPlanningDirection::Agglomerative,
    }
}

fn map_adaptive_planning_error(error: AdaptivePlanningError) -> StreamingIndexerError {
    match error {
        AdaptivePlanningError::InvalidConfiguration(message) => {
            StreamingIndexerError::InvalidAdaptivePlanningConfiguration(message)
        }
        AdaptivePlanningError::DiagnosticComputation(message) => {
            StreamingIndexerError::ClusteringFailure(format!(
                "adaptive planning diagnostics failed: {message}"
            ))
        }
    }
}

fn derive_hierarchy_from_factory<F>(
    factory: &F,
    embeddings: &[Vec<f32>],
    embedding_spec: &EmbeddingSpec,
    materializability_bound: usize,
    block_size_target: usize,
    stage_observer: &mut impl FnMut(
        PlanningStage,
        usize,
        StreamingIndexingStatusState,
        Option<AdaptivePlanningDecisionTelemetry>,
        Option<AdaptivePlanningActiveSubproblemTelemetry>,
        bool,
    ),
) -> Result<PlanningPassOutcome, StreamingClusteringError>
where
    F: StreamingClusteringFactory,
{
    derive_hierarchy_with_builder(
        embeddings,
        materializability_bound,
        stage_observer,
        |partition_embeddings| {
            let trainer = factory
                .create_trainer(
                    partition_embeddings.first().map_or(0, std::vec::Vec::len),
                    partition_embeddings.len(),
                    block_size_target,
                    embedding_spec,
                )
                .map_err(|error| invalid_config(error.to_string()))?;
            Ok((PartitionPlanner::new(PlanningStage::Custom, trainer), None))
        },
    )
}

struct PartitionPlanner<T> {
    stage: PlanningStage,
    trainer: T,
}

impl<T> PartitionPlanner<T> {
    fn new(stage: PlanningStage, trainer: T) -> Self {
        Self { stage, trainer }
    }
}

trait PartitionPlannerRunner {
    fn stage(&self) -> PlanningStage;
    fn run(
        self,
        embeddings: &[Vec<f32>],
    ) -> Result<(PassReport, Vec<ClusterId>), StreamingClusteringError>;
}

impl<T> PartitionPlannerRunner for PartitionPlanner<T>
where
    T: StreamingClusterTrainer,
{
    fn stage(&self) -> PlanningStage {
        self.stage
    }

    fn run(
        mut self,
        embeddings: &[Vec<f32>],
    ) -> Result<(PassReport, Vec<ClusterId>), StreamingClusteringError> {
        self.trainer.ingest_batch(embeddings)?;
        let pass_report = self.trainer.finish_pass()?;
        self.trainer.complete_training()?;
        let classifier = self.trainer.into_classifier()?;
        let assignments = classifier.assign_batch(embeddings)?;
        Ok((pass_report, assignments))
    }
}

#[derive(Clone, Debug)]
struct AgglomerativeHierarchyNode {
    child_node_indices: Vec<usize>,
    item_indices: Vec<usize>,
    representative_embedding: Option<Vec<f32>>,
    planning_stage: PlanningStage,
}

#[derive(Default)]
struct AdaptiveDivisiveProgressTracker {
    completed_subproblem_count: usize,
}

impl AdaptiveDivisiveProgressTracker {
    fn active_subproblem_position(&self) -> usize {
        self.completed_subproblem_count
    }

    fn mark_subproblem_completed(&mut self) {
        self.completed_subproblem_count += 1;
    }
}

fn adaptive_active_subproblem_telemetry(
    active_algorithm: ActivePlanningAlgorithm,
    active_subproblem_position: usize,
    completed_subproblem_count: usize,
    active_dcbc_progress: Option<AdaptivePlanningNestedProgressTelemetry>,
) -> AdaptivePlanningActiveSubproblemTelemetry {
    AdaptivePlanningActiveSubproblemTelemetry {
        active_algorithm,
        active_subproblem_position: Some(active_subproblem_position),
        completed_subproblem_count,
        total_subproblem_count: None,
        active_dcbc_progress,
    }
}

fn derive_adaptive_divisive_hierarchy(
    embeddings: &[Vec<f32>],
    materializability_bound: usize,
    selector: &mut AdaptivePlanningSelector,
    settings: &AdaptivePlanningSettings,
    embedding_spec: &EmbeddingSpec,
    stage_observer: &mut impl FnMut(
        PlanningStage,
        usize,
        StreamingIndexingStatusState,
        Option<AdaptivePlanningDecisionTelemetry>,
        Option<AdaptivePlanningActiveSubproblemTelemetry>,
        bool,
    ),
) -> Result<PlanningPassOutcome, StreamingIndexerError> {
    if embeddings.is_empty() {
        return Ok(PlanningPassOutcome {
            hierarchy: FinalizedPartitionHierarchy {
                root_partition_id: "p0".into(),
                partitions: Vec::new(),
            },
            planning_quality_metric: 0.0,
            planning_balance_metric: 0.0,
            planning_quality_direction: MetricDirection::LargerIsBetter,
            planning_balance_direction: MetricDirection::SmallerIsBetter,
            stages_used: BTreeSet::new(),
        });
    }

    let mut accumulator = PlanningMetricAccumulator::default();
    let root_indices = (0..embeddings.len()).collect::<Vec<_>>();
    let mut partitions = Vec::new();
    let mut progress = AdaptiveDivisiveProgressTracker::default();
    derive_adaptive_divisive_partition_recursive(
        &root_indices,
        "p0".into(),
        None,
        embeddings,
        materializability_bound,
        selector,
        settings,
        embedding_spec,
        stage_observer,
        &mut accumulator,
        &mut partitions,
        &mut progress,
    )?;
    partitions.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(PlanningPassOutcome {
        hierarchy: FinalizedPartitionHierarchy {
            root_partition_id: "p0".into(),
            partitions,
        },
        planning_quality_metric: accumulator.average_quality(),
        planning_balance_metric: accumulator.average_balance(),
        planning_quality_direction: accumulator.quality_direction,
        planning_balance_direction: accumulator.balance_direction,
        stages_used: accumulator.stages_used,
    })
}

#[allow(clippy::too_many_arguments)]
fn derive_adaptive_divisive_partition_recursive(
    indices: &[usize],
    partition_id: String,
    parent_id: Option<String>,
    embeddings: &[Vec<f32>],
    materializability_bound: usize,
    selector: &mut AdaptivePlanningSelector,
    settings: &AdaptivePlanningSettings,
    embedding_spec: &EmbeddingSpec,
    stage_observer: &mut impl FnMut(
        PlanningStage,
        usize,
        StreamingIndexingStatusState,
        Option<AdaptivePlanningDecisionTelemetry>,
        Option<AdaptivePlanningActiveSubproblemTelemetry>,
        bool,
    ),
    accumulator: &mut PlanningMetricAccumulator,
    partitions: &mut Vec<FinalizedPartition>,
    progress: &mut AdaptiveDivisiveProgressTracker,
) -> Result<(), StreamingIndexerError> {
    let terminal = indices.len() <= materializability_bound || indices.len() <= 1;
    if terminal {
        partitions.push(FinalizedPartition {
            id: partition_id,
            parent_id,
            child_ids: Vec::new(),
            item_indices: indices.to_vec(),
            terminal: true,
            planning_stage: PlanningStage::Single,
        });
        return Ok(());
    }

    let partition_embeddings = indices
        .iter()
        .map(|&index| embeddings[index].clone())
        .collect::<Vec<_>>();
    let active_subproblem_position = progress.active_subproblem_position();
    let completed_subproblem_count = progress.completed_subproblem_count;
    let algorithm = selector
        .select_algorithm(&partition_embeddings)
        .map_err(map_adaptive_planning_error)?;
    let adaptive_decision = selector
        .decision_records()
        .last()
        .map(adaptive_decision_telemetry);
    let active_subproblem = adaptive_active_subproblem_telemetry(
        algorithm,
        active_subproblem_position,
        completed_subproblem_count,
        None,
    );
    let phase = adaptive_phase(settings, algorithm);
    let trainer = create_built_in_trainer(
        &phase,
        partition_embeddings.len(),
        partition_embeddings.first().map_or(0, std::vec::Vec::len),
        embedding_spec,
        materializability_bound,
    )?;
    let stage = PlanningStage::Single;
    stage_observer(
        stage,
        indices.len(),
        StreamingIndexingStatusState::Started,
        adaptive_decision,
        Some(active_subproblem),
        false,
    );
    stage_observer(
        stage,
        indices.len(),
        StreamingIndexingStatusState::InProgress,
        adaptive_decision,
        Some(active_subproblem),
        true,
    );
    let (pass_report, assignments) =
        run_built_in_planner_with_progress(trainer, &partition_embeddings, |nested_progress| {
            stage_observer(
                stage,
                indices.len(),
                StreamingIndexingStatusState::InProgress,
                adaptive_decision,
                Some(adaptive_active_subproblem_telemetry(
                    algorithm,
                    active_subproblem_position,
                    completed_subproblem_count,
                    nested_progress,
                )),
                false,
            );
        })?;
    if assignments.len() != partition_embeddings.len() {
        return Err(StreamingIndexerError::ClusteringFailure(format!(
            "planner returned {} cluster ids for {} embeddings",
            assignments.len(),
            partition_embeddings.len()
        )));
    }

    accumulator.observe(stage, &pass_report);
    progress.mark_subproblem_completed();

    let mut groups = assignments_to_groups(&assignments);
    groups = ensure_min_two_per_group(groups);
    for group in &mut groups {
        group.sort_unstable();
    }
    groups.sort_by_key(|group| group[0]);
    if groups.len() <= 1 {
        groups = balanced_groups(indices.len(), materializability_bound)
            .map_err(invalid_config)
            .map_err(|error| StreamingIndexerError::ClusteringFailure(error.to_string()))?;
    }

    let child_ids = (0..groups.len())
        .map(|child_index| format!("{partition_id}.{child_index}"))
        .collect::<Vec<_>>();
    partitions.push(FinalizedPartition {
        id: partition_id.clone(),
        parent_id: parent_id.clone(),
        child_ids: child_ids.clone(),
        item_indices: indices.to_vec(),
        terminal: false,
        planning_stage: stage,
    });

    for (child_index, group) in groups.into_iter().enumerate() {
        let child_indices = group
            .into_iter()
            .map(|local_index| indices[local_index])
            .collect::<Vec<_>>();
        derive_adaptive_divisive_partition_recursive(
            &child_indices,
            child_ids[child_index].clone(),
            Some(partition_id.clone()),
            embeddings,
            materializability_bound,
            selector,
            settings,
            embedding_spec,
            stage_observer,
            accumulator,
            partitions,
            progress,
        )?;
    }

    Ok(())
}

fn run_built_in_planner_with_progress(
    mut trainer: BuiltInStreamingClusterTrainer,
    embeddings: &[Vec<f32>],
    mut observe_nested_progress: impl FnMut(Option<AdaptivePlanningNestedProgressTelemetry>),
) -> Result<(PassReport, Vec<ClusterId>), StreamingClusteringError> {
    trainer.ingest_batch(embeddings)?;
    let pass_report = match &mut trainer {
        BuiltInStreamingClusterTrainer::Dcbc(dcbc) => {
            dcbc.finish_pass_with_progress_observer(|completed_unit_count, total_unit_count| {
                observe_nested_progress(Some(AdaptivePlanningNestedProgressTelemetry {
                    completed_unit_count: Some(completed_unit_count),
                    total_unit_count: Some(total_unit_count),
                }));
            })?
        }
        BuiltInStreamingClusterTrainer::DirectionalPca(trainer) => {
            observe_nested_progress(None);
            trainer.finish_pass()?
        }
    };
    trainer.complete_training()?;
    let classifier = trainer.into_classifier()?;
    let assignments = classifier.assign_batch(embeddings)?;
    Ok((pass_report, assignments))
}

fn derive_hierarchy_with_builder<E, B, P, SO>(
    embeddings: &[Vec<f32>],
    materializability_bound: usize,
    stage_observer: &mut SO,
    mut planner_builder: B,
) -> Result<PlanningPassOutcome, E>
where
    E: From<StreamingClusteringError>,
    B: FnMut(&[Vec<f32>]) -> Result<(P, Option<AdaptivePlanningDecisionTelemetry>), E>,
    P: PartitionPlannerRunner,
    SO: FnMut(
        PlanningStage,
        usize,
        StreamingIndexingStatusState,
        Option<AdaptivePlanningDecisionTelemetry>,
        Option<AdaptivePlanningActiveSubproblemTelemetry>,
        bool,
    ),
{
    if embeddings.is_empty() {
        return Ok(PlanningPassOutcome {
            hierarchy: FinalizedPartitionHierarchy {
                root_partition_id: "p0".into(),
                partitions: Vec::new(),
            },
            planning_quality_metric: 0.0,
            planning_balance_metric: 0.0,
            planning_quality_direction: MetricDirection::LargerIsBetter,
            planning_balance_direction: MetricDirection::SmallerIsBetter,
            stages_used: BTreeSet::new(),
        });
    }

    let mut accumulator = PlanningMetricAccumulator::default();
    let root_indices = (0..embeddings.len()).collect::<Vec<_>>();
    let mut partitions = Vec::new();
    derive_partition_recursive(
        &root_indices,
        "p0".into(),
        None,
        embeddings,
        materializability_bound,
        stage_observer,
        &mut planner_builder,
        &mut accumulator,
        &mut partitions,
    )?;
    partitions.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(PlanningPassOutcome {
        hierarchy: FinalizedPartitionHierarchy {
            root_partition_id: "p0".into(),
            partitions,
        },
        planning_quality_metric: accumulator.average_quality(),
        planning_balance_metric: accumulator.average_balance(),
        planning_quality_direction: accumulator.quality_direction,
        planning_balance_direction: accumulator.balance_direction,
        stages_used: accumulator.stages_used,
    })
}

fn derive_hierarchy_agglomeratively_with_builder<E, B, P, SO>(
    embeddings: &[Vec<f32>],
    materializability_bound: usize,
    stage_observer: &mut SO,
    mut planner_builder: B,
) -> Result<PlanningPassOutcome, E>
where
    E: From<StreamingClusteringError>,
    B: FnMut(
        &[Vec<f32>],
        usize,
        usize,
    ) -> Result<(P, Option<AdaptivePlanningDecisionTelemetry>), E>,
    P: PartitionPlannerRunner,
    SO: FnMut(
        PlanningStage,
        usize,
        StreamingIndexingStatusState,
        Option<AdaptivePlanningDecisionTelemetry>,
        Option<AdaptivePlanningActiveSubproblemTelemetry>,
        bool,
    ),
{
    if embeddings.is_empty() {
        return Ok(PlanningPassOutcome {
            hierarchy: FinalizedPartitionHierarchy {
                root_partition_id: "p0".into(),
                partitions: Vec::new(),
            },
            planning_quality_metric: 0.0,
            planning_balance_metric: 0.0,
            planning_quality_direction: MetricDirection::LargerIsBetter,
            planning_balance_direction: MetricDirection::SmallerIsBetter,
            stages_used: BTreeSet::new(),
        });
    }

    let mut accumulator = PlanningMetricAccumulator::default();
    let mut nodes = embeddings
        .iter()
        .enumerate()
        .map(|(index, embedding)| AgglomerativeHierarchyNode {
            child_node_indices: Vec::new(),
            item_indices: vec![index],
            representative_embedding: Some(embedding.clone()),
            planning_stage: PlanningStage::Single,
        })
        .collect::<Vec<_>>();
    let mut current_layer = (0..nodes.len()).collect::<Vec<_>>();

    while current_layer.len() > 1 {
        let represented_item_count = current_layer
            .iter()
            .map(|&node_index| nodes[node_index].item_indices.len())
            .sum();
        let max_unit_item_count = current_layer
            .iter()
            .map(|&node_index| nodes[node_index].item_indices.len())
            .max()
            .unwrap_or(0);
        let layer_embeddings = current_layer
            .iter()
            .map(|&node_index| {
                nodes[node_index]
                    .representative_embedding
                    .clone()
                    .ok_or_else(|| {
                        invalid_config(format!(
                            "planning unit {node_index} is missing its representative embedding"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(E::from)?;
        let (planner, adaptive_decision) = planner_builder(
            &layer_embeddings,
            represented_item_count,
            max_unit_item_count,
        )?;
        let stage = planner.stage();
        stage_observer(
            stage,
            represented_item_count,
            StreamingIndexingStatusState::Started,
            adaptive_decision,
            None,
            false,
        );
        stage_observer(
            stage,
            represented_item_count,
            StreamingIndexingStatusState::InProgress,
            adaptive_decision,
            None,
            true,
        );
        let (pass_report, assignments) = planner.run(&layer_embeddings).map_err(E::from)?;
        if assignments.len() != layer_embeddings.len() {
            return Err(E::from(invalid_config(format!(
                "planner returned {} cluster ids for {} planning units",
                assignments.len(),
                layer_embeddings.len()
            ))));
        }

        accumulator.observe(stage, &pass_report);

        let mut groups = assignments_to_groups(&assignments);
        groups = ensure_min_two_per_group(groups);
        for group in &mut groups {
            group.sort_unstable();
        }
        groups.sort_by_key(|group| group[0]);
        if groups.len() <= 1 || groups.len() >= current_layer.len() {
            groups = balanced_groups(current_layer.len(), materializability_bound)
                .map_err(invalid_config)
                .map_err(E::from)?;
        }

        let mut next_layer = Vec::with_capacity(groups.len());
        for group in groups {
            let child_node_indices = group
                .into_iter()
                .map(|local_index| current_layer[local_index])
                .collect::<Vec<_>>();
            let mut item_indices = child_node_indices
                .iter()
                .flat_map(|&node_index| nodes[node_index].item_indices.iter().copied())
                .collect::<Vec<_>>();
            item_indices.sort_unstable();
            let child_representative_embeddings = child_node_indices
                .iter()
                .map(|&node_index| {
                    nodes[node_index]
                        .representative_embedding
                        .take()
                        .ok_or_else(|| {
                            invalid_config(format!(
                                "planning unit {node_index} is missing its representative embedding"
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(E::from)?;
            let representative_embedding = weighted_mean_f32_embeddings(
                child_node_indices
                    .iter()
                    .zip(child_representative_embeddings.iter())
                    .map(|(&node_index, embedding)| {
                        (embedding.as_slice(), nodes[node_index].item_indices.len())
                    }),
            )
            .map_err(E::from)?;
            let next_index = nodes.len();
            nodes.push(AgglomerativeHierarchyNode {
                child_node_indices,
                item_indices,
                representative_embedding: Some(representative_embedding),
                planning_stage: stage,
            });
            next_layer.push(next_index);
        }
        current_layer = next_layer;
    }

    let mut partitions = Vec::new();
    build_agglomerative_partitions(&nodes, current_layer[0], "p0".into(), None, &mut partitions);
    partitions.sort_by(|left, right| left.id.cmp(&right.id));

    Ok(PlanningPassOutcome {
        hierarchy: FinalizedPartitionHierarchy {
            root_partition_id: "p0".into(),
            partitions,
        },
        planning_quality_metric: accumulator.average_quality(),
        planning_balance_metric: accumulator.average_balance(),
        planning_quality_direction: accumulator.quality_direction,
        planning_balance_direction: accumulator.balance_direction,
        stages_used: accumulator.stages_used,
    })
}

fn build_agglomerative_partitions(
    nodes: &[AgglomerativeHierarchyNode],
    node_index: usize,
    partition_id: String,
    parent_id: Option<String>,
    partitions: &mut Vec<FinalizedPartition>,
) {
    let node = &nodes[node_index];
    let terminal = node.child_node_indices.is_empty();
    let child_ids = (0..node.child_node_indices.len())
        .map(|child_index| format!("{partition_id}.{child_index}"))
        .collect::<Vec<_>>();

    partitions.push(FinalizedPartition {
        id: partition_id.clone(),
        parent_id: parent_id.clone(),
        child_ids: child_ids.clone(),
        item_indices: node.item_indices.clone(),
        terminal,
        planning_stage: node.planning_stage,
    });

    for (child_index, &child_node_index) in node.child_node_indices.iter().enumerate() {
        build_agglomerative_partitions(
            nodes,
            child_node_index,
            child_ids[child_index].clone(),
            Some(partition_id.clone()),
            partitions,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn derive_partition_recursive<E, B, P, SO>(
    indices: &[usize],
    partition_id: String,
    parent_id: Option<String>,
    embeddings: &[Vec<f32>],
    materializability_bound: usize,
    stage_observer: &mut SO,
    planner_builder: &mut B,
    accumulator: &mut PlanningMetricAccumulator,
    partitions: &mut Vec<FinalizedPartition>,
) -> Result<(), E>
where
    E: From<StreamingClusteringError>,
    B: FnMut(&[Vec<f32>]) -> Result<(P, Option<AdaptivePlanningDecisionTelemetry>), E>,
    P: PartitionPlannerRunner,
    SO: FnMut(
        PlanningStage,
        usize,
        StreamingIndexingStatusState,
        Option<AdaptivePlanningDecisionTelemetry>,
        Option<AdaptivePlanningActiveSubproblemTelemetry>,
        bool,
    ),
{
    let terminal = indices.len() <= materializability_bound || indices.len() <= 1;
    if terminal {
        partitions.push(FinalizedPartition {
            id: partition_id,
            parent_id,
            child_ids: Vec::new(),
            item_indices: indices.to_vec(),
            terminal: true,
            planning_stage: PlanningStage::Single,
        });
        return Ok(());
    }

    let partition_embeddings = indices
        .iter()
        .map(|&index| embeddings[index].clone())
        .collect::<Vec<_>>();
    let (planner, adaptive_decision) = planner_builder(&partition_embeddings)?;
    let stage = planner.stage();
    stage_observer(
        stage,
        indices.len(),
        StreamingIndexingStatusState::Started,
        adaptive_decision,
        None,
        false,
    );
    stage_observer(
        stage,
        indices.len(),
        StreamingIndexingStatusState::InProgress,
        adaptive_decision,
        None,
        true,
    );
    let (pass_report, assignments) = planner.run(&partition_embeddings).map_err(E::from)?;
    if assignments.len() != partition_embeddings.len() {
        return Err(E::from(invalid_config(format!(
            "planner returned {} cluster ids for {} embeddings",
            assignments.len(),
            partition_embeddings.len()
        ))));
    }

    accumulator.observe(stage, &pass_report);

    let mut groups = assignments_to_groups(&assignments);
    groups = ensure_min_two_per_group(groups);
    for group in &mut groups {
        group.sort_unstable();
    }
    groups.sort_by_key(|group| group[0]);
    if groups.len() <= 1 {
        groups = balanced_groups(indices.len(), materializability_bound)
            .map_err(invalid_config)
            .map_err(E::from)?;
    }

    let child_ids = (0..groups.len())
        .map(|child_index| format!("{partition_id}.{child_index}"))
        .collect::<Vec<_>>();
    partitions.push(FinalizedPartition {
        id: partition_id.clone(),
        parent_id: parent_id.clone(),
        child_ids: child_ids.clone(),
        item_indices: indices.to_vec(),
        terminal: false,
        planning_stage: stage,
    });

    for (child_index, group) in groups.into_iter().enumerate() {
        let child_indices = group
            .into_iter()
            .map(|local_index| indices[local_index])
            .collect::<Vec<_>>();
        derive_partition_recursive(
            &child_indices,
            child_ids[child_index].clone(),
            Some(partition_id.clone()),
            embeddings,
            materializability_bound,
            stage_observer,
            planner_builder,
            accumulator,
            partitions,
        )?;
    }

    Ok(())
}

struct PlanningMetricAccumulator {
    quality_sum: f64,
    balance_sum: f64,
    cluster_runs: usize,
    quality_direction: MetricDirection,
    balance_direction: MetricDirection,
    stages_used: BTreeSet<PlanningStage>,
}

impl Default for PlanningMetricAccumulator {
    fn default() -> Self {
        Self {
            quality_sum: 0.0,
            balance_sum: 0.0,
            cluster_runs: 0,
            quality_direction: MetricDirection::LargerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            stages_used: BTreeSet::new(),
        }
    }
}

impl PlanningMetricAccumulator {
    fn observe(&mut self, stage: PlanningStage, report: &PassReport) {
        if self.cluster_runs == 0 {
            self.quality_direction = report.quality_direction;
            self.balance_direction = report.balance_direction;
        }
        self.quality_sum += report.quality_metric;
        self.balance_sum += report.balance_metric;
        self.cluster_runs += 1;
        self.stages_used.insert(stage);
    }

    fn average_quality(&self) -> f64 {
        if self.cluster_runs == 0 {
            0.0
        } else {
            self.quality_sum / self.cluster_runs as f64
        }
    }

    fn average_balance(&self) -> f64 {
        if self.cluster_runs == 0 {
            0.0
        } else {
            self.balance_sum / self.cluster_runs as f64
        }
    }
}

struct PlanningStageStatusTracker<'a> {
    observer: &'a Option<StreamingIndexingStatusObserver>,
    pass_started: Instant,
    stage_item_counts: BTreeMap<PlanningStage, usize>,
}

#[derive(Clone, Copy)]
struct PlanningStageObservation {
    pass_number: usize,
    stage: PlanningStage,
    state: StreamingIndexingStatusState,
    item_count: usize,
    adaptive_decision: Option<AdaptivePlanningDecisionTelemetry>,
    active_subproblem: Option<AdaptivePlanningActiveSubproblemTelemetry>,
    advance_completed_work: bool,
}

impl<'a> PlanningStageStatusTracker<'a> {
    fn new(observer: &'a Option<StreamingIndexingStatusObserver>, pass_started: Instant) -> Self {
        Self {
            observer,
            pass_started,
            stage_item_counts: BTreeMap::new(),
        }
    }

    fn observe(&mut self, observation: PlanningStageObservation) {
        match observation.state {
            StreamingIndexingStatusState::Started => self.ensure_started(
                observation.pass_number,
                observation.stage,
                observation.item_count,
                observation.adaptive_decision,
                observation.active_subproblem,
            ),
            StreamingIndexingStatusState::InProgress => {
                self.ensure_started(
                    observation.pass_number,
                    observation.stage,
                    observation.item_count,
                    observation.adaptive_decision,
                    observation.active_subproblem,
                );
                let total = self.stage_item_counts.entry(observation.stage).or_insert(0);
                if observation.advance_completed_work {
                    *total += observation.item_count;
                }
                emit_status(
                    self.observer,
                    with_adaptive_planning(
                        status_with_progress(
                            StreamingIndexingPhase::HierarchyPlanning {
                                stage: observation.stage,
                            },
                            observation.state,
                            None,
                            *total,
                            self.pass_started.elapsed(),
                            None,
                        ),
                        observation.adaptive_decision.map(|decision| {
                            AdaptivePlanningStatusTelemetry {
                                pass_number: observation.pass_number,
                                decision,
                                active_subproblem: observation.active_subproblem,
                            }
                        }),
                    ),
                );
            }
            StreamingIndexingStatusState::Completed | StreamingIndexingStatusState::Failed => {}
        }
    }

    fn complete_all(&self, elapsed: Duration) {
        for (stage, item_count) in &self.stage_item_counts {
            emit_status(
                self.observer,
                status_with_progress(
                    StreamingIndexingPhase::HierarchyPlanning { stage: *stage },
                    StreamingIndexingStatusState::Completed,
                    None,
                    *item_count,
                    elapsed,
                    None,
                ),
            );
        }
    }

    fn fail_all(&self, elapsed: Duration, error: &str) {
        for (stage, item_count) in &self.stage_item_counts {
            emit_status(
                self.observer,
                status_with_progress(
                    StreamingIndexingPhase::HierarchyPlanning { stage: *stage },
                    StreamingIndexingStatusState::Failed,
                    None,
                    *item_count,
                    elapsed,
                    Some(error.to_owned()),
                ),
            );
        }
    }

    fn ensure_started(
        &mut self,
        pass_number: usize,
        stage: PlanningStage,
        item_count: usize,
        adaptive_decision: Option<AdaptivePlanningDecisionTelemetry>,
        active_subproblem: Option<AdaptivePlanningActiveSubproblemTelemetry>,
    ) {
        if self.stage_item_counts.contains_key(&stage) {
            return;
        }
        self.stage_item_counts.insert(stage, 0);
        emit_status(
            self.observer,
            with_legacy_item_count(
                with_adaptive_planning(
                    status_with_progress(
                        StreamingIndexingPhase::HierarchyPlanning { stage },
                        StreamingIndexingStatusState::Started,
                        None,
                        0,
                        Duration::ZERO,
                        None,
                    ),
                    adaptive_decision.map(|decision| AdaptivePlanningStatusTelemetry {
                        pass_number,
                        decision,
                        active_subproblem,
                    }),
                ),
                item_count,
            ),
        );
    }
}

// ─────────────────────────────────────────────────────────────
// Private helpers
// ─────────────────────────────────────────────────────────────

fn remaining_units(total: Option<usize>, completed: usize) -> Option<usize> {
    total.and_then(|total| total.checked_sub(completed))
}

fn adaptive_decision_telemetry(
    record: &AdaptiveSwitchDecisionRecord,
) -> AdaptivePlanningDecisionTelemetry {
    AdaptivePlanningDecisionTelemetry {
        boundary_position: record.boundary_position,
        active_algorithm: record.active_algorithm,
        switch_boundary_occurred: record.switch_boundary_occurred,
        embedding_count: record
            .collapse_diagnostics
            .as_ref()
            .map(|diagnostics| diagnostics.embedding_count),
        pc1_explained_variance_ratio: record
            .collapse_diagnostics
            .as_ref()
            .and_then(|diagnostics| diagnostics.pc1_explained_variance_ratio),
        pc1_explained_variance_ratio_threshold: record.pc1_explained_variance_ratio_threshold,
        dcbc_max_embedding_count: record.dcbc_max_embedding_count,
        reason: record.reason,
    }
}

fn adaptive_pass_telemetry(
    pass_number: usize,
    decision_records: &[AdaptiveSwitchDecisionRecord],
) -> Option<AdaptivePlanningPassTelemetry> {
    let latest_decision = adaptive_decision_telemetry(
        decision_records
            .iter()
            .rev()
            .find(|record| record.collapse_diagnostics.is_some())
            .unwrap_or(decision_records.last()?),
    );
    Some(AdaptivePlanningPassTelemetry {
        pass_number,
        switch_occurred: decision_records
            .iter()
            .any(|record| record.switch_boundary_occurred),
        latest_decision,
        first_switch_boundary_position: decision_records
            .iter()
            .find(|record| record.switch_boundary_occurred)
            .map(|record| record.boundary_position),
    })
}

fn status_with_progress(
    phase: StreamingIndexingPhase,
    state: StreamingIndexingStatusState,
    phase_total_unit_count: Option<usize>,
    completed_unit_count: usize,
    elapsed: Duration,
    error: Option<String>,
) -> StreamingIndexingStatus {
    StreamingIndexingStatus {
        phase,
        state,
        item_count: phase_total_unit_count.unwrap_or(completed_unit_count),
        phase_total_unit_count,
        completed_unit_count,
        remaining_unit_count: remaining_units(phase_total_unit_count, completed_unit_count),
        elapsed,
        error,
        adaptive_planning: None,
    }
}

fn status_with_known_total(
    phase: StreamingIndexingPhase,
    state: StreamingIndexingStatusState,
    phase_total_unit_count: usize,
    completed_unit_count: usize,
    elapsed: Duration,
    error: Option<String>,
) -> StreamingIndexingStatus {
    status_with_progress(
        phase,
        state,
        Some(phase_total_unit_count),
        completed_unit_count,
        elapsed,
        error,
    )
}

fn with_legacy_item_count(
    mut status: StreamingIndexingStatus,
    legacy_item_count: usize,
) -> StreamingIndexingStatus {
    status.item_count = legacy_item_count;
    status
}

fn with_adaptive_planning(
    mut status: StreamingIndexingStatus,
    adaptive_planning: Option<AdaptivePlanningStatusTelemetry>,
) -> StreamingIndexingStatus {
    status.adaptive_planning = adaptive_planning;
    status
}

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
    phase_total_unit_count: Option<usize>,
    progress: Arc<AtomicUsize>,
    legacy_item_count: Option<usize>,
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
                let status = status_with_progress(
                    phase.clone(),
                    StreamingIndexingStatusState::InProgress,
                    phase_total_unit_count,
                    progress.load(AtomicOrdering::Relaxed),
                    started.elapsed(),
                    None,
                );
                observer(if let Some(legacy_item_count) = legacy_item_count {
                    with_legacy_item_count(status, legacy_item_count)
                } else {
                    status
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
    into_writer(&Value::Map(canonical), &mut encoded)
        .map_err(|error| format!("failed to encode metadata for replay hashing: {error}"))?;
    Ok(hash_bytes(&encoded))
}

fn assignments_to_groups(assignments: &[ClusterId]) -> Vec<Vec<usize>> {
    if assignments.is_empty() {
        return Vec::new();
    }
    let mut groups: BTreeMap<ClusterId, Vec<usize>> = BTreeMap::new();
    for (index, &cluster_id) in assignments.iter().enumerate() {
        groups.entry(cluster_id).or_default().push(index);
    }
    groups.into_values().collect()
}

fn ensure_min_two_per_group(mut groups: Vec<Vec<usize>>) -> Vec<Vec<usize>> {
    let (mut ok, singletons): (Vec<Vec<usize>>, Vec<Vec<usize>>) =
        groups.drain(..).partition(|group| group.len() >= 2);

    if singletons.is_empty() {
        return ok;
    }
    if ok.is_empty() {
        let merged = singletons.into_iter().flatten().collect::<Vec<_>>();
        return if merged.is_empty() {
            vec![]
        } else {
            vec![merged]
        };
    }

    let target = ok.iter_mut().max_by_key(|group| group.len()).unwrap();
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
            .map(|chunk| Ok(f32::from_le_bytes(chunk.try_into().unwrap())))
            .collect(),
        "f16le" => bytes
            .chunks_exact(2)
            .map(|chunk| Ok(f16::from_le_bytes(chunk.try_into().unwrap()).to_f32()))
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
    ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    ids.dedup_by(|left, right| left.as_bytes() == right.as_bytes());
}

fn normalize_current_layer(mut layer: Vec<IndexedChild>) -> Vec<IndexedChild> {
    layer.sort_by(compare_indexed_children);
    deduplicate_layer_by_child(layer)
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

fn compare_indexed_children(left: &IndexedChild, right: &IndexedChild) -> Ordering {
    left.embedding
        .cmp(&right.embedding)
        .then_with(|| left.child.as_bytes().cmp(right.child.as_bytes()))
}

fn normalize_branch_entries(mut entries: Vec<BranchEntry>) -> Vec<BranchEntry> {
    entries.sort_by(|left, right| {
        left.child
            .as_bytes()
            .cmp(right.child.as_bytes())
            .then_with(|| left.embedding.cmp(&right.embedding))
    });
    let mut deduped = Vec::with_capacity(entries.len());
    for entry in entries {
        if deduped
            .last()
            .is_some_and(|prev: &BranchEntry| prev.child == entry.child)
        {
            continue;
        }
        deduped.push(entry);
    }
    deduped.sort_by(|left, right| {
        left.embedding
            .cmp(&right.embedding)
            .then_with(|| left.child.as_bytes().cmp(right.child.as_bytes()))
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
        "pq4" => dims.checked_add(1).map(|value| value / 2),
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
            .map(|&byte| i8::from_le_bytes([byte]) as f64)
            .collect()),
        "f32le" => embedding
            .chunks_exact(4)
            .map(|chunk| Ok(f32::from_le_bytes(chunk.try_into().unwrap()) as f64))
            .collect(),
        "f16le" => embedding
            .chunks_exact(2)
            .map(|chunk| Ok(f16::from_le_bytes(chunk.try_into().unwrap()).to_f64()))
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
    for (index, entry) in block.entries.iter().enumerate() {
        let decoded = decode_embedding_as_f64(&entry.embedding, &block.embedding_spec, "canonical")
            .map_err(|error| format!("failed to decode branch entry {index}: {error}"))?;
        for (dimension, (sum, value)) in sums.iter_mut().zip(decoded).enumerate() {
            if !value.is_finite() {
                return Err(format!(
                    "branch entry {index} contains non-finite value at dimension {dimension}"
                ));
            }
            *sum += value;
            if !sum.is_finite() {
                return Err(format!(
                    "arithmetic-mean sum overflowed at dimension {dimension}"
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

fn weighted_mean_f32_embeddings<'a>(
    embeddings: impl IntoIterator<Item = (&'a [f32], usize)>,
) -> Result<Vec<f32>, StreamingClusteringError> {
    let mut embeddings = embeddings.into_iter();
    let Some((first, first_weight)) = embeddings.next() else {
        return Err(invalid_config(
            "cannot compute representative embedding from an empty planning group".into(),
        ));
    };
    if first_weight == 0 {
        return Err(invalid_config(
            "representative embedding weight must be positive".into(),
        ));
    }
    let mut sums = first
        .iter()
        .map(|&value| f64::from(value) * first_weight as f64)
        .collect::<Vec<_>>();
    for (dimension, value) in sums.iter().enumerate() {
        if !value.is_finite() {
            return Err(invalid_config(format!(
                "non-finite representative embedding value at dimension {dimension}"
            )));
        }
    }
    let mut total_weight = first_weight;
    for (embedding, weight) in embeddings {
        if weight == 0 {
            return Err(invalid_config(
                "representative embedding weight must be positive".into(),
            ));
        }
        if embedding.len() != sums.len() {
            return Err(invalid_config(format!(
                "representative embedding dimension {} does not match expected {}",
                embedding.len(),
                sums.len()
            )));
        }
        for (dimension, (sum, &value)) in sums.iter_mut().zip(embedding.iter()).enumerate() {
            if !value.is_finite() {
                return Err(invalid_config(format!(
                    "non-finite representative embedding value at dimension {dimension}"
                )));
            }
            *sum += f64::from(value) * weight as f64;
            if !sum.is_finite() {
                return Err(invalid_config(format!(
                    "representative embedding sum overflowed at dimension {dimension}"
                )));
            }
        }
        total_weight += weight;
    }

    sums.into_iter()
        .enumerate()
        .map(|(dimension, sum)| {
            let mean = sum / total_weight as f64;
            if !mean.is_finite() {
                return Err(invalid_config(format!(
                    "representative embedding mean became non-finite at dimension {dimension}"
                )));
            }
            let encoded = mean as f32;
            if !encoded.is_finite() {
                return Err(invalid_config(format!(
                    "representative embedding mean overflowed f32 at dimension {dimension}"
                )));
            }
            Ok(encoded)
        })
        .collect()
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
            for (dimension, &value) in values.iter().enumerate() {
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
            let mut bytes = Vec::with_capacity(dims * 2);
            for (dimension, &value) in values.iter().enumerate() {
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

fn effective_cluster_count(
    requested_cluster_count: u32,
    estimated_child_count: usize,
    materializability_bound: usize,
) -> Result<u32, String> {
    if estimated_child_count <= 1 {
        return Ok(1);
    }
    let bound = materializability_bound.max(2);
    let requested = usize::try_from(requested_cluster_count.max(2))
        .map_err(|_| "requested cluster count does not fit in usize".to_string())?;
    let max_groups = estimated_child_count / 2;
    let effective = requested.min(bound).min(max_groups.max(1)).max(2);
    u32::try_from(effective).map_err(|_| "effective cluster count exceeds u32::MAX".into())
}

fn materializability_bound(
    spec: &EmbeddingSpec,
    block_size_target: usize,
) -> Result<usize, String> {
    let min_size = serialized_branch_size(spec, 2)?;
    if min_size > block_size_target {
        return Err(format!(
            "minimum 2-child branch serializes to {min_size} bytes, exceeding block size target {block_size_target}"
        ));
    }

    let mut low = 2usize;
    let mut high = 2usize;
    loop {
        let next = high.saturating_mul(2);
        if next <= high {
            break;
        }
        if serialized_branch_size(spec, next)? <= block_size_target {
            low = next;
            high = next;
        } else {
            high = next;
            break;
        }
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
        .collect::<Vec<_>>();
    let branch = build_branch_block(VERSION_1, 1, spec.clone(), entries, None)
        .map_err(|error| format!("failed to build synthetic branch block: {error}"))?;
    let block = Block::Branch(branch);
    serialize_block(&block)
        .map(|serialized| serialized.bytes.len())
        .map_err(|error| format!("failed to serialize synthetic branch block: {error}"))
}

fn synthetic_block_hash(index: usize) -> BlockHash {
    let mut bytes = [0u8; BlockHash::LEN];
    bytes[..std::mem::size_of::<usize>()].copy_from_slice(&index.to_le_bytes());
    BlockHash::from_bytes(bytes)
}

fn balanced_groups(len: usize, materializability_bound: usize) -> Result<Vec<Vec<usize>>, String> {
    if len == 0 {
        return Err("cannot materialize an empty child set".into());
    }
    if len == 1 {
        return Ok(vec![vec![0]]);
    }
    if len <= materializability_bound {
        return Ok(vec![(0..len).collect()]);
    }

    let group_count = len.div_ceil(materializability_bound);
    if group_count > len / 2 {
        return Err(format!(
            "cannot split {len} children into conforming groups under materializability bound {materializability_bound}"
        ));
    }

    let base = len / group_count;
    let remainder = len % group_count;
    let mut groups = Vec::with_capacity(group_count);
    let mut next = 0usize;
    for group_index in 0..group_count {
        let size = base + usize::from(group_index < remainder);
        groups.push((next..next + size).collect());
        next += size;
    }
    Ok(groups)
}

fn verify_persisted_block_id(
    actual: BlockHash,
    expected: BlockHash,
) -> Result<(), StreamingIndexerError> {
    if actual == expected {
        Ok(())
    } else {
        Err(StreamingIndexerError::Storage(
            BlockStoreError::IntegrityMismatch { expected, actual },
        ))
    }
}

fn invalid_config(message: String) -> StreamingClusteringError {
    StreamingClusteringError::InvalidConfiguration { message }
}

fn map_clustering_configuration_error(message: String) -> StreamingIndexerError {
    StreamingIndexerError::ClusteringFailure(message)
}

fn map_clustering_error(error: StreamingClusteringError) -> StreamingIndexerError {
    StreamingIndexerError::ClusteringFailure(error.to_string())
}

fn map_planning_policy_error<E>(error: E) -> StreamingIndexerError
where
    E: std::error::Error + 'static,
{
    if let Some(error) = (&error as &dyn std::error::Error).downcast_ref::<StreamingIndexerError>()
    {
        return error.clone();
    }

    if let Some(StreamingClusteringError::InvalidConfiguration { message }) =
        (&error as &dyn std::error::Error).downcast_ref::<StreamingClusteringError>()
        && message.contains("fine_partition_max_items")
    {
        return StreamingIndexerError::InvalidHybridPlanningConfiguration(message.clone());
    }

    StreamingIndexerError::ClusteringFailure(error.to_string())
}

fn validate_partition_hierarchy(
    hierarchy: &FinalizedPartitionHierarchy,
    item_count: usize,
) -> Result<(), String> {
    let partitions = hierarchy
        .partitions
        .iter()
        .map(|partition| (partition.id.clone(), partition))
        .collect::<HashMap<_, _>>();
    let root = partitions
        .get(&hierarchy.root_partition_id)
        .ok_or_else(|| "root partition is missing".to_string())?;
    if root.parent_id.is_some() {
        return Err("root partition must not have a parent".into());
    }

    for partition in hierarchy.partitions.iter() {
        let mut sorted_items = partition.item_indices.clone();
        sorted_items.sort_unstable();
        sorted_items.dedup();
        if sorted_items != partition.item_indices {
            return Err(format!(
                "partition {:?} must store sorted unique item indices",
                partition.id
            ));
        }
        if partition
            .item_indices
            .iter()
            .any(|&index| index >= item_count)
        {
            return Err(format!(
                "partition {:?} references an out-of-range item index",
                partition.id
            ));
        }
        if partition.terminal && !partition.child_ids.is_empty() {
            return Err(format!(
                "terminal partition {:?} must not declare children",
                partition.id
            ));
        }
        if !partition.terminal && partition.child_ids.is_empty() {
            return Err(format!(
                "non-terminal partition {:?} must declare children",
                partition.id
            ));
        }
        for child_id in &partition.child_ids {
            let child = partitions.get(child_id).ok_or_else(|| {
                format!(
                    "partition {:?} references missing child {:?}",
                    partition.id, child_id
                )
            })?;
            if child.parent_id.as_deref() != Some(partition.id.as_str()) {
                return Err(format!(
                    "partition {:?} has ancestry mismatch for child {:?}",
                    partition.id, child_id
                ));
            }
        }
    }

    fn walk(
        partition_id: &str,
        partitions: &HashMap<String, &FinalizedPartition>,
        visited: &mut BTreeSet<String>,
    ) -> Result<Vec<usize>, String> {
        let partition = partitions
            .get(partition_id)
            .ok_or_else(|| format!("partition {partition_id:?} is missing"))?;
        if !visited.insert(partition_id.to_string()) {
            return Err(format!(
                "partition hierarchy contains a cycle at {partition_id:?}"
            ));
        }
        if partition.terminal {
            return Ok(partition.item_indices.clone());
        }
        let mut union = Vec::new();
        for child_id in &partition.child_ids {
            let child_items = walk(child_id, partitions, visited)?;
            union.extend(child_items);
        }
        union.sort_unstable();
        if union != partition.item_indices {
            return Err(format!(
                "partition {:?} does not match the exact union of its children",
                partition.id
            ));
        }
        Ok(union)
    }

    let mut visited = BTreeSet::new();
    let root_items = walk(&hierarchy.root_partition_id, &partitions, &mut visited)?;
    if root_items != (0..item_count).collect::<Vec<_>>() {
        return Err("root partition must cover the complete logical item set".into());
    }
    if visited.len() != hierarchy.partitions.len() {
        return Err("partition hierarchy contains unreachable partitions".into());
    }
    Ok(())
}

struct HierarchyStats {
    partition_count: usize,
    terminal_partition_count: usize,
    depth: usize,
}

fn hierarchy_stats(hierarchy: &FinalizedPartitionHierarchy) -> HierarchyStats {
    let partitions = hierarchy
        .partitions
        .iter()
        .map(|partition| (partition.id.clone(), partition))
        .collect::<HashMap<_, _>>();

    fn depth_of(partition_id: &str, partitions: &HashMap<String, &FinalizedPartition>) -> usize {
        let partition = partitions.get(partition_id).unwrap();
        if partition.child_ids.is_empty() {
            1
        } else {
            1 + partition
                .child_ids
                .iter()
                .map(|child_id| depth_of(child_id, partitions))
                .max()
                .unwrap_or(0)
        }
    }

    HierarchyStats {
        partition_count: hierarchy.partitions.len(),
        terminal_partition_count: hierarchy.partitions.iter().filter(|p| p.terminal).count(),
        depth: if hierarchy.partitions.is_empty() {
            0
        } else {
            depth_of(&hierarchy.root_partition_id, &partitions)
        },
    }
}

// ─────────────────────────────────────────────────────────────
// Opt-in conformance helpers (feature = "conformance")
// ─────────────────────────────────────────────────────────────

#[cfg(feature = "conformance")]
mod conformance_support {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fmt;

    use super::*;

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
                .map_err(|error| match error {
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

    pub type ConformanceResult = Result<(), ConformanceError>;

    #[derive(Debug)]
    pub enum ConformanceError {
        Indexer(StreamingIndexerError),
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

    impl std::error::Error for ConformanceError {}

    impl From<StreamingIndexerError> for ConformanceError {
        fn from(error: StreamingIndexerError) -> Self {
            Self::Indexer(error)
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

    #[derive(Clone, Copy)]
    struct FixedEmbeddingProvider;

    impl EmbeddingProvider for FixedEmbeddingProvider {
        type Error = FixtureError;

        async fn embed(
            &self,
            _input: &EmbeddingInput,
            spec: &EmbeddingSpec,
        ) -> Result<Vec<u8>, Self::Error> {
            if spec.encoding != "i8" || spec.dims != 2 {
                return Err(FixtureError("unexpected embedding spec".into()));
            }
            Ok(vec![1, 2])
        }
    }

    fn embedding_spec() -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: "i8".into(),
        }
    }

    pub fn run_content_resolver_suite<H>(harness: &H) -> ConformanceResult
    where
        H: ContentResolverConformanceHarness,
    {
        pollster::block_on(async {
            let store = MemoryBlockStore::default();
            let item = harness.sample_item();

            let mut ok_run = StreamingIndexingRun::with_builtin_planning(
                harness.conforming_resolver(),
                FixedEmbeddingProvider,
                BuiltInPlanning::Dcbc(DcbcBuiltInPlanningSettings {
                    direction: BuiltInPlanningDirection::Divisive,
                    cluster_count: 2,
                    balance_constraints: None,
                    random_seed: None,
                }),
                embedding_spec(),
                256,
            );
            ok_run.ingest_batch(std::slice::from_ref(&item)).await?;
            ok_run.finish_pass()?;
            ok_run.mark_planning_complete()?;
            let result = ok_run
                .finalize(std::iter::once([item.clone()]), &store)
                .await?;
            if store
                .get(&result.root_id)
                .map_err(StreamingIndexerError::Storage)?
                .is_none()
            {
                return Err(ConformanceError::Expectation(
                    "conforming resolver should materialize a root block".into(),
                ));
            }

            let mut failing_run = StreamingIndexingRun::with_builtin_planning(
                harness.failing_resolver(),
                FixedEmbeddingProvider,
                BuiltInPlanning::Dcbc(DcbcBuiltInPlanningSettings {
                    direction: BuiltInPlanningDirection::Divisive,
                    cluster_count: 2,
                    balance_constraints: None,
                    random_seed: None,
                }),
                embedding_spec(),
                256,
            );
            match failing_run.ingest_batch(std::slice::from_ref(&item)).await {
                Err(StreamingIndexerError::ContentResolution(_)) => {}
                other => {
                    return Err(ConformanceError::Expectation(format!(
                        "expected content-resolution failure, got {other:?}"
                    )));
                }
            }

            let mut unusable_run = StreamingIndexingRun::with_builtin_planning(
                harness.unusable_resolver(),
                FixedEmbeddingProvider,
                BuiltInPlanning::Dcbc(DcbcBuiltInPlanningSettings {
                    direction: BuiltInPlanningDirection::Divisive,
                    cluster_count: 2,
                    balance_constraints: None,
                    random_seed: None,
                }),
                embedding_spec(),
                256,
            );
            match unusable_run.ingest_batch(&[item]).await {
                Err(StreamingIndexerError::UnusableContent(_)) => Ok(()),
                other => Err(ConformanceError::Expectation(format!(
                    "expected unusable-content failure, got {other:?}"
                ))),
            }
        })
    }

    pub fn run_full_trait_suite<CH, AH, FH>(
        content_harness: &CH,
        canonical_harness: &AH,
        factory_harness: &FH,
    ) -> ConformanceResult
    where
        CH: ContentResolverConformanceHarness,
        AH: CanonicalEmbeddingPolicyConformanceHarness,
        FH: StreamingClusteringFactoryConformanceHarness,
    {
        run_content_resolver_suite(content_harness)?;
        pollster::block_on(async {
            let store = MemoryBlockStore::default();
            let item = content_harness.sample_item();
            let mut distinct_item = item.clone();
            distinct_item.metadata = vec![(
                ciborium::Value::Text("variant".into()),
                ciborium::Value::Integer(1.into()),
            )];

            let mut ok_run = StreamingIndexingRun::with_streaming_clustering_factory(
                content_harness.conforming_resolver(),
                FixedEmbeddingProvider,
                canonical_harness.conforming_policy(),
                factory_harness.conforming_factory(),
                embedding_spec(),
                256,
            );
            ok_run
                .ingest_batch(&[item.clone(), distinct_item.clone()])
                .await?;
            ok_run.finish_pass()?;
            ok_run.mark_planning_complete()?;
            ok_run
                .finalize(
                    std::iter::once([item.clone(), distinct_item.clone()]),
                    &store,
                )
                .await?;

            let mut failing_canonical = StreamingIndexingRun::with_streaming_clustering_factory(
                content_harness.conforming_resolver(),
                FixedEmbeddingProvider,
                canonical_harness.failing_policy(),
                factory_harness.conforming_factory(),
                embedding_spec(),
                256,
            );
            failing_canonical
                .ingest_batch(&[item.clone(), distinct_item.clone()])
                .await?;
            failing_canonical.finish_pass()?;
            failing_canonical.mark_planning_complete()?;
            match failing_canonical
                .finalize(
                    std::iter::once([item.clone(), distinct_item.clone()]),
                    &store,
                )
                .await
            {
                Err(StreamingIndexerError::CanonicalEmbeddingFailure(_)) => {}
                other => {
                    return Err(ConformanceError::Expectation(format!(
                        "expected canonical-embedding failure, got {other:?}"
                    )));
                }
            }

            Ok(())
        })
    }
}

#[cfg(feature = "conformance")]
pub mod conformance {
    pub use super::conformance_support::{
        CanonicalEmbeddingPolicyConformanceHarness, ConformanceError, ConformanceResult,
        ContentResolverConformanceHarness, FixtureError,
        StreamingClusteringFactoryConformanceHarness, run_content_resolver_suite,
        run_full_trait_suite,
    };
}

#[cfg(test)]
mod tests {
    use super::weighted_mean_f32_embeddings;

    #[test]
    fn weighted_representative_embedding_uses_item_counts() {
        let mean = weighted_mean_f32_embeddings([(&[0.0f32, 2.0][..], 1), (&[6.0f32, 8.0][..], 3)])
            .expect("weighted mean should succeed");
        assert_eq!(mean, vec![4.5, 6.5]);
    }
}
