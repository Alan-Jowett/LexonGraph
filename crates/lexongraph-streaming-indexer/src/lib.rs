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
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use async_recursion::async_recursion;
use ciborium::{Value, ser::into_writer};
use half::f16;
use lexongraph_adaptive_planning_policy::AdaptivePlanningSelector;
pub use lexongraph_adaptive_planning_policy::{
    ActivePlanningAlgorithm, AdaptiveDcbcSettings, AdaptiveDirectionalPcaSettings,
    AdaptivePlanningDecisionReason, AdaptivePlanningDiagnostics, AdaptivePlanningDirection,
    AdaptivePlanningError, AdaptivePlanningSettings, AdaptiveSwitchCriteria,
    AdaptiveSwitchDecisionRecord, DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
};
use lexongraph_block::{
    Block, BlockError, BranchEntry, EbcpDescriptor, EbcpQuantization, EbcpRotation, LeafEntry,
    VERSION_1, build_branch_block, build_leaf_block, canonicalize_metadata, ebcp_extension_map,
    serialize_block,
};
pub use lexongraph_block::{
    BlockHash, BranchBlock, Content, EmbeddingSpec, Metadata, SerializedBlock,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_dcbc_streaming::DcbcStreamingTrainer;
use lexongraph_directional_pca::{
    DirectionalPcaAllocationPolicy, DirectionalPcaBinningPolicy,
    DirectionalPcaClusterCardinalityMode, DirectionalPcaOutOfCorePlannerState,
    DirectionalPcaParams, DirectionalPcaRetainedAxisPolicy, DirectionalPcaStreamingClassifier,
    DirectionalPcaStreamingTrainer, DirectionalPcaTrainerSubphase,
};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_pca::fit;
use lexongraph_spherical_kmeans::{SphericalKmeansParams, SphericalKmeansStreamingTrainer};
pub use lexongraph_streaming_clustering::{BalanceConstraints, MetricDirection};
use lexongraph_streaming_clustering::{
    ClusterId, PassReadiness, PassReport, StreamingClusterClassifier, StreamingClusterTrainer,
    StreamingClusteringConfig, StreamingClusteringError, TrainerState,
};
use memmap2::{Mmap, MmapOptions};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

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

impl Default for StreamingV2PassMetricAccumulator {
    fn default() -> Self {
        Self {
            quality_sum: 0.0,
            balance_sum: 0.0,
            cluster_runs: 0,
            requested_cluster_count: None,
            realized_cluster_count: None,
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
        }
    }
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
    pub requested_planning_cluster_count: Option<u32>,
    pub realized_planning_cluster_count: Option<u32>,
    pub planning_quality_metric: f64,
    pub planning_balance_metric: f64,
    pub planning_quality_direction: MetricDirection,
    pub planning_balance_direction: MetricDirection,
    pub planned_partition_count: usize,
    pub terminal_partition_count: usize,
    pub hierarchy_depth: usize,
    pub v2_completed_pass_summary: Option<StreamingV2CompletedPassSummary>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamingV2Partition {
    pub id: String,
    pub parent_id: Option<String>,
    pub child_ids: Vec<String>,
    pub item_count: usize,
    pub terminal: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamingV2PartitionTopology {
    pub root_partition_id: String,
    pub partitions: Vec<StreamingV2Partition>,
}

pub struct PlanningPassOutcome {
    pub hierarchy: FinalizedPartitionHierarchy,
    pub requested_cluster_count: Option<u32>,
    pub realized_cluster_count: Option<u32>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildSummaryInput {
    pub embedding: Vec<u8>,
    pub child: BlockHash,
    pub level: u64,
    pub descendant_count: usize,
}

pub trait ChildSummaryPolicy {
    type Error: std::error::Error;

    fn summarize_children(
        &self,
        embedding_spec: &EmbeddingSpec,
        children: &[ChildSummaryInput],
    ) -> Result<Vec<u8>, Self::Error>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CanonicalChildSummaryAdapterError<E> {
    InvalidBranchBlock(BlockError),
    CanonicalEmbedding(E),
}

impl<E> fmt::Display for CanonicalChildSummaryAdapterError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBranchBlock(error) => write!(f, "{error}"),
            Self::CanonicalEmbedding(error) => write!(f, "{error}"),
        }
    }
}

impl<E> std::error::Error for CanonicalChildSummaryAdapterError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidBranchBlock(error) => Some(error),
            Self::CanonicalEmbedding(error) => Some(error),
        }
    }
}

impl<T> ChildSummaryPolicy for T
where
    T: CanonicalEmbeddingPolicy,
    T::Error: 'static,
{
    type Error = CanonicalChildSummaryAdapterError<T::Error>;

    fn summarize_children(
        &self,
        embedding_spec: &EmbeddingSpec,
        children: &[ChildSummaryInput],
    ) -> Result<Vec<u8>, Self::Error> {
        let branch = build_branch_block(
            VERSION_1,
            children.iter().map(|child| child.level).max().unwrap_or(0) + 1,
            embedding_spec.clone(),
            children
                .iter()
                .map(|child| BranchEntry {
                    embedding: child.embedding.clone(),
                    child: child.child,
                })
                .collect(),
            None,
        )
        .map_err(CanonicalChildSummaryAdapterError::InvalidBranchBlock)?;
        self.canonical_embedding(&branch)
            .map_err(CanonicalChildSummaryAdapterError::CanonicalEmbedding)
    }
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
        SO: FnMut(PlanningStage, usize, StreamingIndexingStatusState),
    {
        for stage in self.declared_stages() {
            observe_stage(
                stage,
                embeddings.len(),
                StreamingIndexingStatusState::Started,
            );
            observe_stage(
                stage,
                embeddings.len(),
                StreamingIndexingStatusState::InProgress,
            );
        }
        self.finish_planning_pass(
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
        )
    }

    fn finish_planning_pass_with_status_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
        mut observe_status: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(HierarchyPlanningStatusEvent),
    {
        self.finish_planning_pass_with_stage_observer(
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
            |stage, item_count, state| {
                observe_status(HierarchyPlanningStatusEvent::legacy(
                    stage, item_count, state,
                ));
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
    UnsupportedPublishedProfileVersion(PublishedProfileVersion),
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
    LocalSpill(String),
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
            Self::UnsupportedPublishedProfileVersion(version) => {
                write!(
                    f,
                    "unsupported published indexing profile version {version}"
                )
            }
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
            Self::LocalSpill(m) => write!(f, "local spill failed: {m}"),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamingIndexingProgressUnitKind {
    PassItem,
    HierarchyPlanningItem,
    PartitionPlanningInvocation,
    ReplayItem,
    AssemblyGroup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamingIndexingTrainerSubphase {
    AnalyzePca,
    PlanCuts,
    CountCells,
    RealizePartition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamingIndexingSuspectedStallReason {
    UnchangedPassObservedCount,
    UnchangedPendingPartitionProgress,
    UnchangedRoutingBucketFill,
    UnchangedTrainerSubphase,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamingIndexingSuspectedStall {
    pub reason: StreamingIndexingSuspectedStallReason,
    pub duration_without_progress: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamingV2PendingPartitionStatus {
    pub partition_path: String,
    pub expected_item_count: usize,
    pub observed_replay_progress: Option<usize>,
    pub routing_bucket_fill_counts: Option<Vec<usize>>,
    pub trainer_subphase: Option<StreamingIndexingTrainerSubphase>,
    pub ready_axis_plan_count: Option<usize>,
    pub total_axis_plan_count: Option<usize>,
    pub populated_cell_count: Option<usize>,
    pub realized_cell_count: Option<usize>,
    pub planner_state_fingerprint_hex: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamingV2ConvergenceState {
    InitialPass,
    UnresolvedWorkShrank,
    UnresolvedWorkChanged,
    NoVisibleChange,
    RepeatedPriorState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamingV2BlockerKind {
    ReplayIncomplete,
    AnalyzePcaPending,
    PlanCutsPending,
    CountCellsPending,
    RealizePartitionPending,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamingV2PendingPartitionDelta {
    pub partition_path: String,
    pub previous_trainer_subphase: Option<StreamingIndexingTrainerSubphase>,
    pub trainer_subphase: Option<StreamingIndexingTrainerSubphase>,
    pub previous_observed_replay_progress: Option<usize>,
    pub observed_replay_progress: Option<usize>,
    pub previous_routing_bucket_fill_counts: Option<Vec<usize>>,
    pub routing_bucket_fill_counts: Option<Vec<usize>>,
    pub previous_ready_axis_plan_count: Option<usize>,
    pub ready_axis_plan_count: Option<usize>,
    pub previous_total_axis_plan_count: Option<usize>,
    pub total_axis_plan_count: Option<usize>,
    pub previous_populated_cell_count: Option<usize>,
    pub populated_cell_count: Option<usize>,
    pub previous_realized_cell_count: Option<usize>,
    pub realized_cell_count: Option<usize>,
    pub previous_planner_state_fingerprint_hex: Option<String>,
    pub planner_state_fingerprint_hex: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamingV2PartitionBlockerSummary {
    pub partition_path: String,
    pub expected_item_count: usize,
    pub trainer_subphase: Option<StreamingIndexingTrainerSubphase>,
    pub blocker_kind: StreamingV2BlockerKind,
    pub blocker_detail: String,
    pub observed_replay_progress: Option<usize>,
    pub routing_bucket_fill_counts: Option<Vec<usize>>,
    pub ready_axis_plan_count: Option<usize>,
    pub total_axis_plan_count: Option<usize>,
    pub populated_cell_count: Option<usize>,
    pub realized_cell_count: Option<usize>,
    pub planner_state_fingerprint_hex: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamingV2CompletedPassDelta {
    pub previous_completed_pass_number: Option<usize>,
    pub previous_topology_fingerprint_hex: Option<String>,
    pub topology_fingerprint_hex: String,
    pub previous_pending_partition_fingerprint_hex: Option<String>,
    pub pending_partition_fingerprint_hex: String,
    pub pending_partition_count_delta: Option<isize>,
    pub terminal_partition_count_delta: Option<isize>,
    pub routed_partition_count_delta: Option<isize>,
    pub planned_partition_count_delta: Option<isize>,
    pub hierarchy_depth_delta: Option<isize>,
    pub topology_changed: Option<bool>,
    pub pending_partitions_changed: Option<bool>,
    pub repeated_prior_completed_pass_number: Option<usize>,
    pub current_pending_partition_paths: Vec<String>,
    pub added_pending_partition_paths: Vec<String>,
    pub removed_pending_partition_paths: Vec<String>,
    pub unchanged_pending_partition_paths: Vec<String>,
    pub changed_pending_partitions: Vec<StreamingV2PendingPartitionDelta>,
    pub newly_terminal_partition_paths: Vec<String>,
    pub newly_routed_partition_paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamingV2CompletedPassSummary {
    pub convergence_state: StreamingV2ConvergenceState,
    pub delta: StreamingV2CompletedPassDelta,
    pub blockers: Vec<StreamingV2PartitionBlockerSummary>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HierarchyPlanningStatusEvent {
    pub stage: PlanningStage,
    pub state: StreamingIndexingStatusState,
    pub legacy_item_count: usize,
    pub progress_unit_kind: Option<StreamingIndexingProgressUnitKind>,
    pub completed_unit_count: Option<usize>,
    pub discovered_unit_count: Option<usize>,
    pub current_partition_path: Option<String>,
    pub current_partition_size: Option<usize>,
    pub current_recursion_depth: Option<usize>,
    pub started_subproblem_count: Option<usize>,
    pub completed_subproblem_count: Option<usize>,
    pub visited_partition_count: Option<usize>,
    pub finalized_partition_count: Option<usize>,
    pub terminal_partition_count: Option<usize>,
    pub completed_planner_invocation_count: Option<usize>,
    pub fallback_count: Option<usize>,
}

impl HierarchyPlanningStatusEvent {
    pub fn legacy(
        stage: PlanningStage,
        legacy_item_count: usize,
        state: StreamingIndexingStatusState,
    ) -> Self {
        Self {
            stage,
            state,
            legacy_item_count,
            progress_unit_kind: Some(StreamingIndexingProgressUnitKind::HierarchyPlanningItem),
            completed_unit_count: None,
            discovered_unit_count: None,
            current_partition_path: None,
            current_partition_size: None,
            current_recursion_depth: None,
            started_subproblem_count: None,
            completed_subproblem_count: None,
            visited_partition_count: None,
            finalized_partition_count: None,
            terminal_partition_count: None,
            completed_planner_invocation_count: None,
            fallback_count: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StreamingIndexingStatus {
    pub phase: StreamingIndexingPhase,
    pub state: StreamingIndexingStatusState,
    pub item_count: usize,
    pub phase_total_unit_count: Option<usize>,
    pub completed_unit_count: usize,
    pub remaining_unit_count: Option<usize>,
    pub progress_unit_kind: Option<StreamingIndexingProgressUnitKind>,
    pub discovered_unit_count: Option<usize>,
    pub current_unit_elapsed: Option<Duration>,
    pub current_partition_path: Option<String>,
    pub current_partition_size: Option<usize>,
    pub current_recursion_depth: Option<usize>,
    pub started_subproblem_count: Option<usize>,
    pub completed_subproblem_count: Option<usize>,
    pub visited_partition_count: Option<usize>,
    pub finalized_partition_count: Option<usize>,
    pub terminal_partition_count: Option<usize>,
    pub completed_planner_invocation_count: Option<usize>,
    pub fallback_count: Option<usize>,
    pub pending_partition_count: Option<usize>,
    pub v2_pending_partitions: Option<Vec<StreamingV2PendingPartitionStatus>>,
    pub v2_completed_pass_summary: Option<StreamingV2CompletedPassSummary>,
    pub suspected_stall: Option<StreamingIndexingSuspectedStall>,
    pub elapsed: Duration,
    pub last_progress_at: Option<Duration>,
    pub error: Option<String>,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExactCentroidChildSummaryPolicy;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExactCentroidChildSummaryError(String);

impl fmt::Display for ExactCentroidChildSummaryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ExactCentroidChildSummaryError {}

impl ChildSummaryPolicy for ExactCentroidChildSummaryPolicy {
    type Error = ExactCentroidChildSummaryError;

    fn summarize_children(
        &self,
        embedding_spec: &EmbeddingSpec,
        children: &[ChildSummaryInput],
    ) -> Result<Vec<u8>, Self::Error> {
        exact_centroid_child_summary(children, embedding_spec)
            .map_err(ExactCentroidChildSummaryError)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublishedProfileVersion {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

impl PublishedProfileVersion {
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

impl fmt::Display for PublishedProfileVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

pub const PUBLISHED_PROFILE_V0_1_0: PublishedProfileVersion = PublishedProfileVersion::new(0, 1, 0);
pub const PUBLISHED_PROFILE_V0_2_0: PublishedProfileVersion = PublishedProfileVersion::new(0, 2, 0);
pub const PUBLISHED_PROFILE_V0_3_0: PublishedProfileVersion = PublishedProfileVersion::new(0, 3, 0);
pub const PUBLISHED_PROFILE_V0_3_1: PublishedProfileVersion = PublishedProfileVersion::new(0, 3, 1);
pub const PUBLISHED_PROFILE_V0_3_2: PublishedProfileVersion = PublishedProfileVersion::new(0, 3, 2);
pub const PUBLISHED_PROFILE_V0_3_3: PublishedProfileVersion = PublishedProfileVersion::new(0, 3, 3);
pub const PUBLISHED_PROFILE_V0_3_4: PublishedProfileVersion = PublishedProfileVersion::new(0, 3, 4);
pub const PUBLISHED_PROFILE_V0_3_5: PublishedProfileVersion = PublishedProfileVersion::new(0, 3, 5);
pub const PUBLISHED_PROFILE_V0_3_6: PublishedProfileVersion = PublishedProfileVersion::new(0, 3, 6);
pub const PUBLISHED_PROFILE_V0_3_7: PublishedProfileVersion = PublishedProfileVersion::new(0, 3, 7);
pub const PUBLISHED_PROFILE_V0_3_8: PublishedProfileVersion = PublishedProfileVersion::new(0, 3, 8);
pub const PUBLISHED_PROFILE_V0_3_9: PublishedProfileVersion = PublishedProfileVersion::new(0, 3, 9);
pub const PUBLISHED_PROFILE_V0_3_10: PublishedProfileVersion =
    PublishedProfileVersion::new(0, 3, 10);
pub const PUBLISHED_PROFILE_V0_4_0: PublishedProfileVersion = PublishedProfileVersion::new(0, 4, 0);
pub const PUBLISHED_PROFILE_V0_4_1: PublishedProfileVersion = PublishedProfileVersion::new(0, 4, 1);
pub const PUBLISHED_PROFILE_V0_4_2: PublishedProfileVersion = PublishedProfileVersion::new(0, 4, 2);
pub const PUBLISHED_PROFILE_V0_4_3: PublishedProfileVersion = PublishedProfileVersion::new(0, 4, 3);
pub const PUBLISHED_PROFILE_V0_4_4: PublishedProfileVersion = PublishedProfileVersion::new(0, 4, 4);
pub const PUBLISHED_PROFILE_V0_4_5: PublishedProfileVersion = PublishedProfileVersion::new(0, 4, 5);
pub const PUBLISHED_PROFILE_V0_4_6: PublishedProfileVersion = PublishedProfileVersion::new(0, 4, 6);
pub const PUBLISHED_PROFILE_V0_4_7: PublishedProfileVersion = PublishedProfileVersion::new(0, 4, 7);
pub const PUBLISHED_PROFILE_V0_4_8: PublishedProfileVersion = PublishedProfileVersion::new(0, 4, 8);
pub const PUBLISHED_PROFILE_V0_4_9: PublishedProfileVersion = PublishedProfileVersion::new(0, 4, 9);
pub const PUBLISHED_PROFILE_V0_5_0: PublishedProfileVersion = PublishedProfileVersion::new(0, 5, 0);
pub const PUBLISHED_PROFILE_V0_5_1: PublishedProfileVersion = PublishedProfileVersion::new(0, 5, 1);
pub const PUBLISHED_PROFILE_V0_5_2: PublishedProfileVersion = PublishedProfileVersion::new(0, 5, 2);
pub const PUBLISHED_PROFILE_V0_5_3: PublishedProfileVersion = PublishedProfileVersion::new(0, 5, 3);
pub const PUBLISHED_PROFILE_V0_5_4: PublishedProfileVersion = PublishedProfileVersion::new(0, 5, 4);
pub const PUBLISHED_PROFILE_V0_5_5: PublishedProfileVersion = PublishedProfileVersion::new(0, 5, 5);
pub const PUBLISHED_PROFILE_V0_6_0: PublishedProfileVersion = PublishedProfileVersion::new(0, 6, 0);
pub const PUBLISHED_PROFILE_V0_6_1: PublishedProfileVersion = PublishedProfileVersion::new(0, 6, 1);
pub const PUBLISHED_PROFILE_V0_6_2: PublishedProfileVersion = PublishedProfileVersion::new(0, 6, 2);
pub const PUBLISHED_PROFILE_V0_6_3: PublishedProfileVersion = PublishedProfileVersion::new(0, 6, 3);
pub const PUBLISHED_PROFILE_V0_6_4: PublishedProfileVersion = PublishedProfileVersion::new(0, 6, 4);
pub const PUBLISHED_PROFILE_V0_6_5: PublishedProfileVersion = PublishedProfileVersion::new(0, 6, 5);
pub const PUBLISHED_PROFILE_V0_7_0: PublishedProfileVersion = PublishedProfileVersion::new(0, 7, 0);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishedHierarchyMetric {
    Euclidean,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PublishedSphericalKmeansProfileSettings {
    pub cluster_count: u32,
    pub random_seed: Option<u64>,
    pub params: SphericalKmeansParams,
    pub hierarchy_metric: PublishedHierarchyMetric,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PublishedDirectionalPcaProfileSettings {
    pub cluster_count: u32,
    pub random_seed: Option<u64>,
    pub params: DirectionalPcaParams,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PublishedPlanningStrategy {
    SphericalKmeansGreedyPack(PublishedSphericalKmeansProfileSettings),
    DirectionalPcaDivisive(PublishedDirectionalPcaProfileSettings),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishedBranchEncodingPolicy {
    Ordinary,
    PcaRotF32Le,
    PcaRotDeltaF32Le,
    PcaRotDeltaUniform {
        root_bits: u8,
        interior_bits: u8,
        lowest_routing_bits: u8,
    },
    PcaRotDeltaVariable {
        root_bits: u8,
        interior_bits: u8,
        lowest_routing_bits: u8,
    },
    AmbientDeltaUniform {
        root_bits: u8,
        interior_bits: u8,
        lowest_routing_bits: u8,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct PublishedIndexingProfile {
    pub version: PublishedProfileVersion,
    pub planning_algorithm_id: &'static str,
    pub planning_direction: Option<BuiltInPlanningDirection>,
    pub packing_strategy_id: Option<&'static str>,
    pub hierarchy_strategy_id: &'static str,
    pub summary_policy_id: &'static str,
    pub planning_strategy: PublishedPlanningStrategy,
    pub branch_encoding_policy: PublishedBranchEncodingPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BranchEncodingPolicy {
    Ordinary,
    PcaRotF32Le,
    PcaRotDeltaF32Le,
    PcaRotDeltaUniform {
        root_bits: u8,
        interior_bits: u8,
        lowest_routing_bits: u8,
    },
    PcaRotDeltaVariable {
        root_bits: u8,
        interior_bits: u8,
        lowest_routing_bits: u8,
    },
    AmbientDeltaUniform {
        root_bits: u8,
        interior_bits: u8,
        lowest_routing_bits: u8,
    },
}

fn branch_encoding_policy_for_profile(profile: &PublishedIndexingProfile) -> BranchEncodingPolicy {
    match profile.branch_encoding_policy {
        PublishedBranchEncodingPolicy::Ordinary => BranchEncodingPolicy::Ordinary,
        PublishedBranchEncodingPolicy::PcaRotF32Le => BranchEncodingPolicy::PcaRotF32Le,
        PublishedBranchEncodingPolicy::PcaRotDeltaF32Le => BranchEncodingPolicy::PcaRotDeltaF32Le,
        PublishedBranchEncodingPolicy::PcaRotDeltaUniform {
            root_bits,
            interior_bits,
            lowest_routing_bits,
        } => BranchEncodingPolicy::PcaRotDeltaUniform {
            root_bits,
            interior_bits,
            lowest_routing_bits,
        },
        PublishedBranchEncodingPolicy::PcaRotDeltaVariable {
            root_bits,
            interior_bits,
            lowest_routing_bits,
        } => BranchEncodingPolicy::PcaRotDeltaVariable {
            root_bits,
            interior_bits,
            lowest_routing_bits,
        },
        PublishedBranchEncodingPolicy::AmbientDeltaUniform {
            root_bits,
            interior_bits,
            lowest_routing_bits,
        } => BranchEncodingPolicy::AmbientDeltaUniform {
            root_bits,
            interior_bits,
            lowest_routing_bits,
        },
    }
}

fn directional_pca_published_profile(
    version: PublishedProfileVersion,
    cluster_count: u32,
    params: DirectionalPcaParams,
    branch_encoding_policy: PublishedBranchEncodingPolicy,
) -> PublishedIndexingProfile {
    PublishedIndexingProfile {
        version,
        planning_algorithm_id: "directional-pca",
        planning_direction: Some(BuiltInPlanningDirection::Divisive),
        packing_strategy_id: None,
        hierarchy_strategy_id: "built-in-divisive",
        summary_policy_id: "exact-centroid",
        planning_strategy: PublishedPlanningStrategy::DirectionalPcaDivisive(
            PublishedDirectionalPcaProfileSettings {
                cluster_count,
                random_seed: Some(7),
                params,
            },
        ),
        branch_encoding_policy,
    }
}

fn directional_pca_published_profile_params(
    retained_axis_policy: DirectionalPcaRetainedAxisPolicy,
    allocation_policy: DirectionalPcaAllocationPolicy,
    binning_policy: DirectionalPcaBinningPolicy,
    cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode,
    min_effective_rank: usize,
    min_cumulative_variance: f32,
) -> DirectionalPcaParams {
    DirectionalPcaParams {
        retained_axis_policy,
        allocation_policy,
        binning_policy,
        cluster_cardinality_mode,
        variance_exponent: 1.0,
        temperature: 1.0,
        min_input_count: 2,
        min_effective_rank,
        min_cumulative_variance,
    }
}

pub fn published_indexing_profile(
    version: PublishedProfileVersion,
) -> Result<PublishedIndexingProfile, StreamingIndexerError> {
    match version {
        PUBLISHED_PROFILE_V0_1_0 => Ok(PublishedIndexingProfile {
            version,
            planning_algorithm_id: "spherical-kmeans",
            planning_direction: None,
            packing_strategy_id: Some("cluster-order-balanced-range-packer-v1"),
            hierarchy_strategy_id: "greedy-pack",
            summary_policy_id: "exact-centroid",
            branch_encoding_policy: PublishedBranchEncodingPolicy::Ordinary,
            planning_strategy: PublishedPlanningStrategy::SphericalKmeansGreedyPack(
                PublishedSphericalKmeansProfileSettings {
                    cluster_count: 157,
                    random_seed: Some(11),
                    params: SphericalKmeansParams {
                        initialization_policy:
                            lexongraph_spherical_kmeans::SphericalInitializationPolicy::SeededDeterministicFarthestPoint,
                        max_iteration_count: 32,
                        convergence_tolerance: 1.0e-4,
                    },
                    hierarchy_metric: PublishedHierarchyMetric::Euclidean,
                },
            ),
        }),
        PUBLISHED_PROFILE_V0_2_0 => Ok(directional_pca_published_profile(
            version,
            2,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::FixedCount(1),
                DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::Exact,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_3_0 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::DensityValley,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_3_1 => Ok(directional_pca_published_profile(
            version,
            128,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::DensityValley,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_3_2 => Ok(directional_pca_published_profile(
            version,
            32,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::DensityValley,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_3_3 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_3_4 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::FixedCount(1),
                DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_3_5 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                DirectionalPcaBinningPolicy::DensityValley,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_3_6 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::FixedCount(2),
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::DensityValley,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_3_7 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::FixedCount(3),
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::DensityValley,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_3_8 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::DensityValley,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.5,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_3_9 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::DensityValley,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                2,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_3_10 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::DensityValley,
                DirectionalPcaClusterCardinalityMode::Exact,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_4_0 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_4_1 => Ok(directional_pca_published_profile(
            version,
            128,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_4_2 => Ok(directional_pca_published_profile(
            version,
            32,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_4_3 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::FixedCount(1),
                DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_4_4 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_4_5 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::FixedCount(2),
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_4_6 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::FixedCount(3),
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_4_7 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.5,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_4_8 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                2,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_4_9 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::Exact,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_5_0 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_5_1 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::PcaRotF32Le,
        )),
        PUBLISHED_PROFILE_V0_5_2 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::PcaRotDeltaF32Le,
        )),
        PUBLISHED_PROFILE_V0_5_3 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::PcaRotDeltaUniform {
                root_bits: 12,
                interior_bits: 8,
                lowest_routing_bits: 6,
            },
        )),
        PUBLISHED_PROFILE_V0_5_4 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::PcaRotDeltaVariable {
                root_bits: 12,
                interior_bits: 8,
                lowest_routing_bits: 6,
            },
        )),
        PUBLISHED_PROFILE_V0_5_5 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::AmbientDeltaUniform {
                root_bits: 12,
                interior_bits: 8,
                lowest_routing_bits: 6,
            },
        )),
        PUBLISHED_PROFILE_V0_6_0 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::Ordinary,
        )),
        PUBLISHED_PROFILE_V0_6_1 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::PcaRotF32Le,
        )),
        PUBLISHED_PROFILE_V0_6_2 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::PcaRotDeltaF32Le,
        )),
        PUBLISHED_PROFILE_V0_6_3 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::PcaRotDeltaUniform {
                root_bits: 12,
                interior_bits: 8,
                lowest_routing_bits: 6,
            },
        )),
        PUBLISHED_PROFILE_V0_6_4 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::PcaRotDeltaVariable {
                root_bits: 12,
                interior_bits: 8,
                lowest_routing_bits: 6,
            },
        )),
        PUBLISHED_PROFILE_V0_6_5 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::AmbientDeltaUniform {
                root_bits: 12,
                interior_bits: 8,
                lowest_routing_bits: 6,
            },
        )),
        PUBLISHED_PROFILE_V0_7_0 => Ok(directional_pca_published_profile(
            version,
            64,
            directional_pca_published_profile_params(
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                DirectionalPcaBinningPolicy::Quantile,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                1,
                0.0,
            ),
            PublishedBranchEncodingPolicy::AmbientDeltaUniform {
                root_bits: 12,
                interior_bits: 8,
                lowest_routing_bits: 6,
            },
        )),
        _ => Err(StreamingIndexerError::UnsupportedPublishedProfileVersion(version)),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PublishedProfilePlanningPolicy {
    profile: PublishedIndexingProfile,
}

impl PublishedProfilePlanningPolicy {
    pub fn new(profile: PublishedIndexingProfile) -> Self {
        Self { profile }
    }

    pub fn profile(&self) -> &PublishedIndexingProfile {
        &self.profile
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
pub struct SphericalKmeansBuiltInPlanningSettings {
    pub direction: BuiltInPlanningDirection,
    pub cluster_count: u32,
    pub random_seed: Option<u64>,
    pub params: SphericalKmeansParams,
}

#[derive(Clone, Debug, PartialEq)]
pub enum BuiltInPlanningPhase {
    Dcbc(DcbcBuiltInPlanningSettings),
    DirectionalPca(DirectionalPcaBuiltInPlanningSettings),
    SphericalKmeans(SphericalKmeansBuiltInPlanningSettings),
}

impl BuiltInPlanningPhase {
    fn direction(&self) -> BuiltInPlanningDirection {
        match self {
            Self::Dcbc(settings) => settings.direction,
            Self::DirectionalPca(settings) => settings.direction,
            Self::SphericalKmeans(settings) => settings.direction,
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
    SphericalKmeans(SphericalKmeansBuiltInPlanningSettings),
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
    DirectionalPca(Box<DirectionalPcaStreamingTrainer>),
    SphericalKmeans(SphericalKmeansStreamingTrainer),
}

enum BuiltInStreamingClusterClassifier {
    Dcbc(<DcbcStreamingTrainer as StreamingClusterTrainer>::Classifier),
    DirectionalPca(<DirectionalPcaStreamingTrainer as StreamingClusterTrainer>::Classifier),
    SphericalKmeans(<SphericalKmeansStreamingTrainer as StreamingClusterTrainer>::Classifier),
}

impl StreamingClusterClassifier for BuiltInStreamingClusterClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        match self {
            Self::Dcbc(classifier) => classifier.config(),
            Self::DirectionalPca(classifier) => classifier.config(),
            Self::SphericalKmeans(classifier) => classifier.config(),
        }
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        match self {
            Self::Dcbc(classifier) => classifier.assign(embedding),
            Self::DirectionalPca(classifier) => classifier.assign(embedding),
            Self::SphericalKmeans(classifier) => classifier.assign(embedding),
        }
    }
}

impl StreamingClusterTrainer for BuiltInStreamingClusterTrainer {
    type Classifier = BuiltInStreamingClusterClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        match self {
            Self::Dcbc(trainer) => trainer.config(),
            Self::DirectionalPca(trainer) => trainer.config(),
            Self::SphericalKmeans(trainer) => trainer.config(),
        }
    }

    fn state(&self) -> lexongraph_streaming_clustering::TrainerState {
        match self {
            Self::Dcbc(trainer) => trainer.state(),
            Self::DirectionalPca(trainer) => trainer.state(),
            Self::SphericalKmeans(trainer) => trainer.state(),
        }
    }

    fn ingest_batch(&mut self, embeddings: &[Vec<f32>]) -> Result<(), StreamingClusteringError> {
        match self {
            Self::Dcbc(trainer) => trainer.ingest_batch(embeddings),
            Self::DirectionalPca(trainer) => trainer.ingest_batch(embeddings),
            Self::SphericalKmeans(trainer) => trainer.ingest_batch(embeddings),
        }
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        match self {
            Self::Dcbc(trainer) => trainer.finish_pass(),
            Self::DirectionalPca(trainer) => trainer.finish_pass(),
            Self::SphericalKmeans(trainer) => trainer.finish_pass(),
        }
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        match self {
            Self::Dcbc(trainer) => trainer.complete_training(),
            Self::DirectionalPca(trainer) => trainer.complete_training(),
            Self::SphericalKmeans(trainer) => trainer.complete_training(),
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
            Self::SphericalKmeans(trainer) => trainer
                .into_classifier()
                .map(BuiltInStreamingClusterClassifier::SphericalKmeans),
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
        let mut noop = |_| {};
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
        SO: FnMut(PlanningStage, usize, StreamingIndexingStatusState),
    {
        derive_hierarchy_from_factory(
            &self.factory,
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
            &mut |event| observe_stage(event.stage, event.legacy_item_count, event.state),
        )
    }

    fn finish_planning_pass_with_status_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
        mut observe_status: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(HierarchyPlanningStatusEvent),
    {
        derive_hierarchy_from_factory(
            &self.factory,
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
            &mut observe_status,
        )
    }
}

impl HierarchicalPlanningPolicy for BuiltInPlanningPolicy {
    type Error = StreamingIndexerError;

    fn declared_stages(&self) -> BTreeSet<PlanningStage> {
        match &self.planning {
            BuiltInPlanning::Dcbc(_)
            | BuiltInPlanning::DirectionalPca(_)
            | BuiltInPlanning::SphericalKmeans(_)
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
        let mut noop = |_| {};
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
        SO: FnMut(PlanningStage, usize, StreamingIndexingStatusState),
    {
        let (outcome, decision_records) = derive_hierarchy_from_built_in(
            &self.planning,
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
            &mut |event| observe_stage(event.stage, event.legacy_item_count, event.state),
        )?;
        self.last_adaptive_decision_records = decision_records;
        Ok(outcome)
    }

    fn finish_planning_pass_with_status_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
        mut observe_status: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(HierarchyPlanningStatusEvent),
    {
        let (outcome, decision_records) = derive_hierarchy_from_built_in(
            &self.planning,
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
            &mut observe_status,
        )?;
        self.last_adaptive_decision_records = decision_records;
        Ok(outcome)
    }
}

impl HierarchicalPlanningPolicy for PublishedProfilePlanningPolicy {
    type Error = StreamingIndexerError;

    fn declared_stages(&self) -> BTreeSet<PlanningStage> {
        match &self.profile.planning_strategy {
            PublishedPlanningStrategy::SphericalKmeansGreedyPack(_) => {
                BTreeSet::from([PlanningStage::Fine, PlanningStage::Coarse])
            }
            PublishedPlanningStrategy::DirectionalPcaDivisive(_) => {
                BTreeSet::from([PlanningStage::Single])
            }
        }
    }

    fn finish_planning_pass(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        _block_size_target: usize,
    ) -> Result<PlanningPassOutcome, Self::Error> {
        let mut noop = |_| {};
        derive_hierarchy_from_published_profile(
            &self.profile,
            embeddings,
            embedding_spec,
            materializability_bound,
            &mut noop,
        )
    }

    fn finish_planning_pass_with_stage_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        _block_size_target: usize,
        mut observe_stage: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(PlanningStage, usize, StreamingIndexingStatusState),
    {
        derive_hierarchy_from_published_profile(
            &self.profile,
            embeddings,
            embedding_spec,
            materializability_bound,
            &mut |event| observe_stage(event.stage, event.legacy_item_count, event.state),
        )
    }

    fn finish_planning_pass_with_status_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        _block_size_target: usize,
        mut observe_status: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(HierarchyPlanningStatusEvent),
    {
        derive_hierarchy_from_published_profile(
            &self.profile,
            embeddings,
            embedding_spec,
            materializability_bound,
            &mut observe_status,
        )
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

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct BaselineItem {
    content_ref_hash: BlockHash,
    metadata_hash: BlockHash,
    content_hash: BlockHash,
    embedding_hash: BlockHash,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PartitionId(usize);

const ROOT_PARTITION_ID: PartitionId = PartitionId(0);

struct PartitionRoutingPlan {
    terminal_partition_ids: Vec<String>,
    terminal_partition_item_counts: Vec<usize>,
    _route_dir: TempDir,
    route_path: PathBuf,
}

struct PartitionSpillDirectory {
    dir: Option<TempDir>,
    paths: Vec<PathBuf>,
    writers: Vec<Option<BufWriter<File>>>,
}

const V2_PLANNER_STATE_WINDOW_BYTES: usize = 1024 * 1024;
const V2_MMAP_ALLOCATION_GRANULARITY: u64 = 64 * 1024;
const V2_PLANNER_STATE_VALUE_BYTES: usize = std::mem::size_of::<f32>();

struct StreamingV2QuantilePlannerState {
    quantile_pass: Option<StreamingV2QuantilePassFiles>,
    dir: TempDir,
}

struct StreamingV2QuantilePassFiles {
    expected_value_count: usize,
    paths: Vec<PathBuf>,
    writers: Vec<BufferedF32Writer>,
}

struct BufferedF32Writer {
    writer: BufWriter<File>,
    total_values: usize,
    written_values: usize,
}

struct WindowedMmapF32Reader {
    file: File,
    total_values: usize,
    read_values: usize,
    map: Option<Mmap>,
    map_start: u64,
    map_len: usize,
}

#[derive(Clone)]
struct StreamingV2ReplayFingerprint {
    observed_count: usize,
    digest: [u8; 32],
}

struct StreamingV2ReplayFingerprintTracker {
    observed_count: usize,
    hasher: Sha256,
}

struct StreamingV2PassState {
    fingerprint: StreamingV2ReplayFingerprintTracker,
    replay_order_offsets: Vec<usize>,
    classifier_assignment_counts: Vec<Option<Vec<usize>>>,
    started: Instant,
    last_progress_at: Option<Duration>,
    planning_started_emitted: bool,
    hierarchy_started_emitted: bool,
    partition_unit_started: Vec<Option<Instant>>,
}

#[derive(Clone)]
struct StreamingV2ReplayOrderPlan {
    child_counts: Vec<usize>,
}

struct StreamingV2PartitionNode {
    parent_id: Option<PartitionId>,
    child_ids: Vec<PartitionId>,
    item_count: usize,
    terminal: bool,
    pending_trainer: Option<DirectionalPcaStreamingTrainer>,
    routing: Option<StreamingV2RoutingStrategy>,
}

#[derive(Clone)]
struct StreamingV2CompletedPassSnapshot {
    pass_number: usize,
    planned_partition_count: usize,
    terminal_partition_count: usize,
    routed_partition_paths: Vec<String>,
    terminal_partition_paths: Vec<String>,
    hierarchy_depth: usize,
    topology_fingerprint_hex: String,
    pending_partition_fingerprint_hex: String,
    combined_fingerprint_hex: String,
    pending_partitions: Vec<StreamingV2PendingPartitionStatus>,
}

#[derive(Clone)]
struct StreamingV2CompletedPassHistoryEntry {
    pass_number: usize,
    combined_fingerprint_hex: String,
}

enum StreamingV2RoutingStrategy {
    Classifier(DirectionalPcaStreamingClassifier),
    ReplayOrder(StreamingV2ReplayOrderPlan),
}

#[derive(Clone)]
struct IndexedChild {
    embedding: Vec<u8>,
    child: BlockHash,
    level: u64,
    descendant_count: usize,
}

struct LayerBuildStatus<'a> {
    phase: StreamingIndexingPhase,
    started: Instant,
    progress: &'a Arc<AtomicUsize>,
    legacy_item_count: usize,
    is_global_root_partition: bool,
}

// ─────────────────────────────────────────────────────────────
// StreamingIndexingRun — the public orchestration type
// ─────────────────────────────────────────────────────────────

pub struct StreamingIndexingRun<R, CR, EP, CEP, HPP> {
    resolver: CR,
    embedding_provider: EP,
    canonical_embedding_policy: CEP,
    planning_policy: HPP,
    branch_encoding_policy: BranchEncodingPolicy,
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

    pub fn with_summary_policy(
        resolver: CR,
        embedding_provider: EP,
        summary_policy: CEP,
        planning: BuiltInPlanning,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Self {
        Self::new(
            resolver,
            embedding_provider,
            summary_policy,
            BuiltInPlanningPolicy::new(planning),
            embedding_spec,
            block_size_target,
        )
    }

    pub fn adaptive_decision_records(&self) -> &[AdaptiveSwitchDecisionRecord] {
        self.planning_policy.adaptive_decision_records()
    }
}

impl<R, CR, EP>
    StreamingIndexingRun<R, CR, EP, ExactCentroidChildSummaryPolicy, PublishedProfilePlanningPolicy>
{
    pub fn with_published_profile(
        resolver: CR,
        embedding_provider: EP,
        profile_version: PublishedProfileVersion,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Result<Self, StreamingIndexerError> {
        let profile = published_indexing_profile(profile_version)?;
        Self::new_with_validated_published_profile(
            resolver,
            embedding_provider,
            profile,
            embedding_spec,
            block_size_target,
        )
    }

    pub fn with_resolved_published_profile(
        resolver: CR,
        embedding_provider: EP,
        profile: PublishedIndexingProfile,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Result<Self, StreamingIndexerError> {
        let _ = published_indexing_profile(profile.version)?;
        Self::new_with_validated_published_profile(
            resolver,
            embedding_provider,
            profile,
            embedding_spec,
            block_size_target,
        )
    }

    fn new_with_validated_published_profile(
        resolver: CR,
        embedding_provider: EP,
        profile: PublishedIndexingProfile,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Result<Self, StreamingIndexerError> {
        validate_published_profile_configuration(&profile, &embedding_spec, block_size_target)?;
        let branch_encoding_policy = branch_encoding_policy_for_profile(&profile);
        Ok(Self::new_with_branch_encoding(
            resolver,
            embedding_provider,
            ExactCentroidChildSummaryPolicy,
            PublishedProfilePlanningPolicy::new(profile),
            branch_encoding_policy,
            embedding_spec,
            block_size_target,
        ))
    }
}

fn validate_published_profile_configuration(
    profile: &PublishedIndexingProfile,
    embedding_spec: &EmbeddingSpec,
    block_size_target: usize,
) -> Result<(), StreamingIndexerError> {
    let PublishedProfileVersion { major, minor, .. } = profile.version;
    if published_branch_policy_requires_f32le(&profile.branch_encoding_policy)
        && embedding_spec.encoding != "f32le"
    {
        return Err(map_clustering_configuration_error(format!(
            "published profile {} requires embedding_spec.encoding f32le so branch EBCP logical and leaf encodings remain compatible",
            profile.version
        )));
    }
    if !matches!((major, minor), (0, 4) | (0, 5) | (0, 6) | (0, 7)) {
        return Ok(());
    }

    let PublishedPlanningStrategy::DirectionalPcaDivisive(settings) = &profile.planning_strategy
    else {
        return Ok(());
    };
    let materializability_bound =
        materializability_bound(embedding_spec, block_size_target).map_err(invalid_config)?;
    if settings.cluster_count as usize > materializability_bound {
        return Err(map_clustering_configuration_error(format!(
            "published profile {} requires cluster_count {} but block-size/materializability bound is {} for the supplied embedding spec and block size target {}",
            profile.version, settings.cluster_count, materializability_bound, block_size_target
        )));
    }
    Ok(())
}

fn published_branch_policy_requires_f32le(policy: &PublishedBranchEncodingPolicy) -> bool {
    !matches!(policy, PublishedBranchEncodingPolicy::Ordinary)
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
        Self::new_with_branch_encoding(
            resolver,
            embedding_provider,
            canonical_embedding_policy,
            planning_policy,
            BranchEncodingPolicy::Ordinary,
            embedding_spec,
            block_size_target,
        )
    }

    fn new_with_branch_encoding(
        resolver: CR,
        embedding_provider: EP,
        canonical_embedding_policy: CEP,
        planning_policy: HPP,
        branch_encoding_policy: BranchEncodingPolicy,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
    ) -> Self {
        Self {
            resolver,
            embedding_provider,
            canonical_embedding_policy,
            planning_policy,
            branch_encoding_policy,
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
    CEP: ChildSummaryPolicy,
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
            .finish_planning_pass_with_status_observer(
                &buffered,
                &self.embedding_spec,
                materializability_bound,
                self.block_size_target,
                |event| {
                    stage_statuses.observe(event);
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
        Ok(IndexingPassReport {
            observed_item_count: self.baseline.as_ref().map_or(0, std::vec::Vec::len),
            completed_pass_count: self.completed_passes,
            requested_planning_cluster_count: outcome.requested_cluster_count,
            realized_planning_cluster_count: outcome.realized_cluster_count,
            planning_quality_metric: outcome.planning_quality_metric,
            planning_balance_metric: outcome.planning_balance_metric,
            planning_quality_direction: outcome.planning_quality_direction,
            planning_balance_direction: outcome.planning_balance_direction,
            planned_partition_count: hierarchy_stats.partition_count,
            terminal_partition_count: hierarchy_stats.terminal_partition_count,
            hierarchy_depth: hierarchy_stats.depth,
            v2_completed_pass_summary: None,
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
        let routing_plan = build_partition_routing_plan(hierarchy, baseline.len())?;
        let partitions = hierarchy
            .partitions
            .iter()
            .cloned()
            .map(|partition| (partition.id.clone(), partition))
            .collect::<HashMap<_, _>>();

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
            let mut persisted_ids: Vec<BlockHash> = Vec::new();
            let mut routing_reader = routing_plan.open_reader()?;
            let mut spill = PartitionSpillDirectory::new(routing_plan.terminal_partition_ids.len())?;

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

                for (offset, (((item, content), metadata), embedding)) in items
                    .iter()
                    .zip(contents)
                    .zip(metadatas)
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
                        content_hash: hash_content(&content),
                        embedding_hash: hash_bytes(embedding),
                    };
                    if expected != &replay_item {
                        return Err(StreamingIndexerError::ReplayMismatch(format!(
                            "finalization item {} differs from baseline",
                            replay_count + offset
                        )));
                    }
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
                        .await
                        .map_err(StreamingIndexerError::Storage)?;
                    verify_persisted_block_id(block_id, serialized.hash)?;
                    persisted_ids.push(block_id);
                    let partition_ordinal =
                        routing_reader.read_partition_ordinal()?.ok_or_else(|| {
                            StreamingIndexerError::ReplayMismatch(
                                "finalization replay routing ended before the baseline".into(),
                            )
                        })?;
                    spill.append_leaf_child(
                        partition_ordinal,
                        &IndexedChild {
                            embedding: embedding.clone(),
                            child: block_id,
                            level: 0,
                            descendant_count: 1,
                        },
                    )?;
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
            if routing_reader.read_partition_ordinal()?.is_some() {
                return Err(StreamingIndexerError::ReplayMismatch(
                    "finalization replay routing contained more items than the baseline".into(),
                ));
            }

            Ok((spill.finish()?, persisted_ids))
        }
        .await;

        heartbeat.stop();
        let (spill, mut persisted_ids) = match replay_result {
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

        let mut materialized_terminal_children = HashMap::<String, IndexedChild>::new();
        for (partition_ordinal, partition_id) in
            routing_plan.terminal_partition_ids.iter().enumerate()
        {
            let expected_count = routing_plan.terminal_partition_item_counts[partition_ordinal];
            let leaf_children = spill.read_partition_children(partition_ordinal)?;
            if leaf_children.len() != expected_count {
                return Err(StreamingIndexerError::ReplayMismatch(format!(
                    "terminal partition {partition_id:?} spill contained {} items but expected {expected_count}",
                    leaf_children.len()
                )));
            }
            if leaf_children.is_empty() {
                return Err(StreamingIndexerError::ReplayMismatch(format!(
                    "terminal partition {partition_id:?} spill is empty"
                )));
            }
            let partition_child = if leaf_children.len() == 1 {
                leaf_children.into_iter().next().unwrap()
            } else {
                self.assemble_child_set(
                    leaf_children,
                    partition_id == &hierarchy.root_partition_id,
                    materializability_bound,
                    store,
                    &mut persisted_ids,
                )
                .await?
            };
            materialized_terminal_children.insert(partition_id.clone(), partition_child);
        }

        let root_child = self
            .materialize_partition_from_terminal_children(
                hierarchy.root_partition_id.as_str(),
                &partitions,
                &materialized_terminal_children,
                materializability_bound,
                store,
                &mut persisted_ids,
                true,
            )
            .await?;
        dedup_sort_ids(&mut persisted_ids);
        Ok(StreamingIndexingResult {
            root_id: root_child.child,
            block_ids: persisted_ids,
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[async_recursion(?Send)]
    async fn materialize_partition_from_terminal_children(
        &self,
        partition_id: &str,
        partitions: &HashMap<String, FinalizedPartition>,
        materialized_terminal_children: &HashMap<String, IndexedChild>,
        materializability_bound: usize,
        store: &dyn BlockStore,
        persisted_ids: &mut Vec<BlockHash>,
        is_global_root_partition: bool,
    ) -> Result<IndexedChild, StreamingIndexerError> {
        let partition = partitions.get(partition_id).ok_or_else(|| {
            StreamingIndexerError::HierarchyValidation(format!(
                "partition {partition_id:?} is missing during partition-ordered assembly"
            ))
        })?;

        if partition.terminal {
            return materialized_terminal_children
                .get(partition_id)
                .cloned()
                .ok_or_else(|| {
                    StreamingIndexerError::ReplayMismatch(format!(
                        "terminal partition {partition_id:?} was not materialized"
                    ))
                });
        }

        let mut children = Vec::with_capacity(partition.child_ids.len());
        for child_id in &partition.child_ids {
            children.push(
                self.materialize_partition_from_terminal_children(
                    child_id,
                    partitions,
                    materialized_terminal_children,
                    materializability_bound,
                    store,
                    persisted_ids,
                    false,
                )
                .await?,
            );
        }

        self.assemble_child_set(
            children,
            is_global_root_partition,
            materializability_bound,
            store,
            persisted_ids,
        )
        .await
    }

    async fn assemble_child_set(
        &self,
        children: Vec<IndexedChild>,
        is_global_root_partition: bool,
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
            let next_layer = match self
                .build_branch_layer(
                    &current,
                    &groups,
                    next_level,
                    LayerBuildStatus {
                        phase: phase.clone(),
                        started,
                        progress: &phase_progress,
                        legacy_item_count,
                        is_global_root_partition,
                    },
                    store,
                    persisted_ids,
                )
                .await
            {
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

    async fn build_branch_layer(
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
            let raw_child_summaries = group
                .iter()
                .map(|&index| ChildSummaryInput {
                    embedding: children[index].embedding.clone(),
                    child: children[index].child,
                    level: children[index].level,
                    descendant_count: children[index].descendant_count,
                })
                .collect::<Vec<_>>();
            let entries = normalize_branch_entries(raw_entries);
            let child_summaries = normalize_child_summary_inputs(raw_child_summaries);
            if entries.len() < 2 {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    "normalized child-bearing entry set has fewer than two unique children".into(),
                ));
            }
            if child_summaries.len() < 2 {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    "normalized child summary set has fewer than two unique children".into(),
                ));
            }

            let encoded_branch = encode_branch_entries(
                self.branch_encoding_policy,
                &self.embedding_spec,
                entries.as_slice(),
                parent_level,
                uses_root_branch_budget(status.is_global_root_partition, groups.len()),
            )?;

            let branch = build_branch_block(
                VERSION_1,
                parent_level,
                encoded_branch.embedding_spec,
                encoded_branch.entries,
                encoded_branch.ext,
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
                .await
                .map_err(StreamingIndexerError::Storage)?;
            verify_persisted_block_id(block_id, serialized.hash)?;
            persisted_ids.push(block_id);

            let canonical = self
                .canonical_embedding_policy
                .summarize_children(&self.embedding_spec, &child_summaries)
                .map_err(|e| StreamingIndexerError::CanonicalEmbeddingFailure(e.to_string()))?;
            validate_embedding_bytes(&canonical, &self.embedding_spec, "canonical")
                .map_err(StreamingIndexerError::CanonicalEmbeddingFailure)?;

            next_layer.push(IndexedChild {
                embedding: canonical,
                child: block_id,
                level: parent_level,
                descendant_count: child_summaries
                    .iter()
                    .map(|child| child.descendant_count)
                    .sum(),
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
// StreamingIndexingRunV2 — additive true-streaming published-profile surface
// ─────────────────────────────────────────────────────────────

pub struct StreamingIndexingRunV2<R, CR, EP> {
    resolver: CR,
    embedding_provider: EP,
    observer: Option<StreamingIndexingStatusObserver>,
    profile: PublishedIndexingProfile,
    branch_encoding_policy: BranchEncodingPolicy,
    embedding_spec: EmbeddingSpec,
    block_size_target: usize,
    phase: RunPhase,
    completed_passes: usize,
    baseline_fingerprint: Option<StreamingV2ReplayFingerprint>,
    current_pass: Option<StreamingV2PassState>,
    latest_completed_pass_snapshot: Option<StreamingV2CompletedPassSnapshot>,
    completed_pass_history: Vec<StreamingV2CompletedPassHistoryEntry>,
    partitions: Vec<StreamingV2PartitionNode>,
    next_partition_id: usize,
    planner_state_root: TempDir,
    _item_ref: PhantomData<R>,
}

struct StreamingV2PassMetricAccumulator {
    quality_sum: f64,
    balance_sum: f64,
    cluster_runs: usize,
    requested_cluster_count: Option<u32>,
    realized_cluster_count: Option<u32>,
    quality_direction: MetricDirection,
    balance_direction: MetricDirection,
}

struct StreamingV2CompletedPartition {
    partition_id: PartitionId,
    routing: StreamingV2RoutingStrategy,
    children: Vec<StreamingV2PartitionNode>,
}

impl<R, CR, EP> StreamingIndexingRunV2<R, CR, EP> {
    pub fn with_published_profile(
        resolver: CR,
        embedding_provider: EP,
        profile_version: PublishedProfileVersion,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
        planner_state_root: impl AsRef<Path>,
    ) -> Result<Self, StreamingIndexerError> {
        if profile_version != PUBLISHED_PROFILE_V0_7_0 {
            return Err(StreamingIndexerError::UnsupportedPublishedProfileVersion(
                profile_version,
            ));
        }
        let profile = published_indexing_profile(profile_version)?;
        validate_published_profile_configuration(&profile, &embedding_spec, block_size_target)?;
        let PublishedPlanningStrategy::DirectionalPcaDivisive(_) = &profile.planning_strategy
        else {
            return Err(StreamingIndexerError::ClusteringFailure(
                "streaming v2 currently requires a directional-PCA divisive published profile"
                    .into(),
            ));
        };
        if profile.branch_encoding_policy
            != (PublishedBranchEncodingPolicy::AmbientDeltaUniform {
                root_bits: 12,
                interior_bits: 8,
                lowest_routing_bits: 6,
            })
        {
            return Err(StreamingIndexerError::ClusteringFailure(
                "streaming v2 currently supports only the exact 0.7.0 ambient-delta-uq branch encoding contract".into(),
            ));
        }
        let planner_state_root = tempfile::Builder::new()
            .prefix("streaming-v2-")
            .tempdir_in(planner_state_root.as_ref())
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not initialize planner state root {}: {error}",
                    planner_state_root.as_ref().display()
                ))
            })?;
        Ok(Self {
            resolver,
            embedding_provider,
            observer: None,
            branch_encoding_policy: branch_encoding_policy_for_profile(&profile),
            profile,
            embedding_spec,
            block_size_target,
            phase: RunPhase::Planning,
            completed_passes: 0,
            baseline_fingerprint: None,
            current_pass: None,
            latest_completed_pass_snapshot: None,
            completed_pass_history: Vec::new(),
            partitions: Vec::new(),
            next_partition_id: 0,
            planner_state_root,
            _item_ref: PhantomData,
        })
    }

    pub fn current_partition_topology(&self) -> Option<StreamingV2PartitionTopology> {
        (!self.partitions.is_empty()).then(|| self.current_topology())
    }

    pub fn finalized_partition_topology(&self) -> Option<StreamingV2PartitionTopology> {
        matches!(self.phase, RunPhase::PlanningComplete | RunPhase::Finalized)
            .then(|| self.current_topology())
    }

    pub fn with_observer(mut self, observer: StreamingIndexingStatusObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    fn v2_planning_pass_total(&self) -> Option<usize> {
        self.baseline_fingerprint
            .as_ref()
            .map(|fingerprint| fingerprint.observed_count)
    }

    fn partition_depth(&self, partition_id: PartitionId) -> Result<usize, StreamingIndexerError> {
        let mut depth = 0;
        let mut current = self.partition(partition_id)?.parent_id;
        while let Some(parent_id) = current {
            depth += 1;
            current = self.partition(parent_id)?.parent_id;
        }
        Ok(depth)
    }

    fn v2_pending_partition_statuses(
        &self,
        classifier_assignment_counts: &[Option<Vec<usize>>],
    ) -> Result<Vec<StreamingV2PendingPartitionStatus>, StreamingIndexerError> {
        let mut statuses = Vec::new();
        for (index, partition) in self.partitions.iter().enumerate() {
            let partition_id = PartitionId(index);
            let partition_path = self.partition_label(partition_id);
            if let Some(trainer) = partition.pending_trainer.as_ref() {
                let telemetry = trainer.telemetry();
                statuses.push(StreamingV2PendingPartitionStatus {
                    partition_path,
                    expected_item_count: partition.item_count,
                    observed_replay_progress: telemetry.observed_count,
                    routing_bucket_fill_counts: None,
                    trainer_subphase: Some(map_v2_trainer_subphase(telemetry.subphase)),
                    ready_axis_plan_count: telemetry.ready_axis_plan_count,
                    total_axis_plan_count: telemetry.total_axis_plan_count,
                    populated_cell_count: telemetry.populated_cell_count,
                    realized_cell_count: telemetry.realized_cell_count,
                    planner_state_fingerprint_hex: encode_digest_hex(telemetry.state_fingerprint),
                });
                continue;
            }
            let Some(bucket_fill_counts) = classifier_assignment_counts
                .get(index)
                .and_then(|counts| counts.as_ref())
            else {
                continue;
            };
            if !partition.child_ids.is_empty() {
                continue;
            }
            statuses.push(StreamingV2PendingPartitionStatus {
                partition_path,
                expected_item_count: partition.item_count,
                observed_replay_progress: Some(bucket_fill_counts.iter().sum()),
                routing_bucket_fill_counts: Some(bucket_fill_counts.clone()),
                trainer_subphase: None,
                ready_axis_plan_count: None,
                total_axis_plan_count: None,
                populated_cell_count: None,
                realized_cell_count: None,
                planner_state_fingerprint_hex: hash_streaming_v2_bucket_fill_counts_hex(
                    bucket_fill_counts,
                ),
            });
        }
        statuses.sort_by(|left, right| left.partition_path.cmp(&right.partition_path));
        Ok(statuses)
    }

    fn maybe_v2_pending_partition_statuses(
        &self,
        classifier_assignment_counts: &[Option<Vec<usize>>],
    ) -> Result<Option<Vec<StreamingV2PendingPartitionStatus>>, StreamingIndexerError> {
        if self.observer.is_none() {
            return Ok(None);
        }
        self.v2_pending_partition_statuses(classifier_assignment_counts)
            .map(Some)
    }

    fn build_v2_completed_pass_summary(
        &self,
        pass_number: usize,
        stats: &HierarchyStats,
        topology: &StreamingV2PartitionTopology,
        pending_partitions: Vec<StreamingV2PendingPartitionStatus>,
    ) -> (
        StreamingV2CompletedPassSummary,
        StreamingV2CompletedPassSnapshot,
    ) {
        let topology_fingerprint_hex = hash_streaming_v2_topology_hex(topology);
        let pending_partition_fingerprint_hex =
            hash_streaming_v2_pending_partitions_hex(&pending_partitions);
        let combined_fingerprint_hex = hash_streaming_v2_completed_pass_state_hex(
            &topology_fingerprint_hex,
            &pending_partition_fingerprint_hex,
        );
        let terminal_partition_paths = topology
            .partitions
            .iter()
            .filter(|partition| partition.terminal)
            .map(|partition| partition.id.clone())
            .collect::<Vec<_>>();
        let routed_partition_paths = topology
            .partitions
            .iter()
            .filter(|partition| !partition.terminal && !partition.child_ids.is_empty())
            .map(|partition| partition.id.clone())
            .collect::<Vec<_>>();
        let snapshot = StreamingV2CompletedPassSnapshot {
            pass_number,
            planned_partition_count: stats.partition_count,
            terminal_partition_count: stats.terminal_partition_count,
            routed_partition_paths: routed_partition_paths.clone(),
            terminal_partition_paths: terminal_partition_paths.clone(),
            hierarchy_depth: stats.depth,
            topology_fingerprint_hex: topology_fingerprint_hex.clone(),
            pending_partition_fingerprint_hex: pending_partition_fingerprint_hex.clone(),
            combined_fingerprint_hex: combined_fingerprint_hex.clone(),
            pending_partitions: pending_partitions.clone(),
        };
        let previous = self.latest_completed_pass_snapshot.as_ref();
        let repeated_prior_completed_pass_number = self
            .completed_pass_history
            .iter()
            .rev()
            .find(|prior| prior.combined_fingerprint_hex == combined_fingerprint_hex)
            .map(|prior| prior.pass_number);
        let convergence_state = match previous {
            None => StreamingV2ConvergenceState::InitialPass,
            Some(previous) if previous.combined_fingerprint_hex == combined_fingerprint_hex => {
                StreamingV2ConvergenceState::NoVisibleChange
            }
            Some(_) if repeated_prior_completed_pass_number.is_some() => {
                StreamingV2ConvergenceState::RepeatedPriorState
            }
            Some(previous) if unresolved_work_shrank(previous, &snapshot) => {
                StreamingV2ConvergenceState::UnresolvedWorkShrank
            }
            Some(_) => StreamingV2ConvergenceState::UnresolvedWorkChanged,
        };
        let delta = summarize_streaming_v2_completed_pass_delta(
            previous,
            &snapshot,
            repeated_prior_completed_pass_number,
        );
        let blockers = pending_partitions
            .iter()
            .map(summarize_streaming_v2_partition_blocker)
            .collect();
        (
            StreamingV2CompletedPassSummary {
                convergence_state,
                delta,
                blockers,
            },
            snapshot,
        )
    }

    fn retain_v2_completed_pass_snapshot(&mut self, snapshot: StreamingV2CompletedPassSnapshot) {
        const MAX_V2_COMPLETED_PASS_HISTORY: usize = 16;
        self.completed_pass_history
            .push(StreamingV2CompletedPassHistoryEntry {
                pass_number: snapshot.pass_number,
                combined_fingerprint_hex: snapshot.combined_fingerprint_hex.clone(),
            });
        if self.completed_pass_history.len() > MAX_V2_COMPLETED_PASS_HISTORY {
            let remove = self.completed_pass_history.len() - MAX_V2_COMPLETED_PASS_HISTORY;
            self.completed_pass_history.drain(0..remove);
        }
        self.latest_completed_pass_snapshot = Some(snapshot);
    }

    fn emit_v2_planning_pass_status(
        &self,
        current_pass: &mut StreamingV2PassState,
        state: StreamingIndexingStatusState,
        error: Option<String>,
    ) -> Result<StreamingIndexingStatus, StreamingIndexerError> {
        let elapsed = current_pass.started.elapsed();
        let completed_unit_count = match state {
            StreamingIndexingStatusState::Started => 0,
            _ => current_pass.fingerprint.observed_count,
        };
        let pending =
            self.maybe_v2_pending_partition_statuses(&current_pass.classifier_assignment_counts)?;
        let status = build_v2_planning_pass_status(
            self.completed_passes + 1,
            state,
            self.v2_planning_pass_total(),
            completed_unit_count,
            elapsed,
            match state {
                StreamingIndexingStatusState::Started => Some(Duration::ZERO),
                _ => current_pass.last_progress_at.or(Some(elapsed)),
            },
            error,
            pending,
        );
        emit_status(&self.observer, status.clone());
        if state == StreamingIndexingStatusState::Started {
            current_pass.planning_started_emitted = true;
        }
        Ok(status)
    }

    fn emit_v2_hierarchy_status(
        &self,
        current_pass: &mut StreamingV2PassState,
        partition_id: PartitionId,
        state: StreamingIndexingStatusState,
        completed_unit_count: usize,
        error: Option<String>,
    ) -> Result<StreamingIndexingStatus, StreamingIndexerError> {
        let unit_started = current_pass
            .partition_unit_started
            .get_mut(partition_id.0)
            .ok_or_else(|| {
                StreamingIndexerError::HierarchyValidation(format!(
                    "partition {:?} is missing unit-start state",
                    self.partition_label(partition_id)
                ))
            })?;
        if unit_started.is_none() {
            *unit_started = Some(Instant::now());
        }
        let current_unit_started = *unit_started;
        let elapsed = current_pass.started.elapsed();
        let pending =
            self.maybe_v2_pending_partition_statuses(&current_pass.classifier_assignment_counts)?;
        let partition = self.partition(partition_id)?;
        let mut status = status_with_hierarchy_details(
            StreamingIndexingPhase::HierarchyPlanning {
                stage: PlanningStage::Custom,
            },
            state,
            None,
            completed_unit_count,
            elapsed,
            error,
            HierarchyPlanningDetailFields {
                legacy_item_count: Some(partition.item_count),
                progress_unit_kind: Some(
                    StreamingIndexingProgressUnitKind::PartitionPlanningInvocation,
                ),
                discovered_unit_count: pending.as_ref().map(Vec::len),
                current_unit_elapsed: match state {
                    StreamingIndexingStatusState::Started => Some(Duration::ZERO),
                    _ => current_unit_started.map(|started| started.elapsed()),
                },
                current_partition_path: Some(self.partition_label(partition_id)),
                current_partition_size: Some(partition.item_count),
                current_recursion_depth: Some(self.partition_depth(partition_id)?),
                started_subproblem_count: None,
                completed_subproblem_count: None,
                visited_partition_count: None,
                finalized_partition_count: None,
                terminal_partition_count: None,
                completed_planner_invocation_count: Some(completed_unit_count),
                fallback_count: None,
                last_progress_at: match state {
                    StreamingIndexingStatusState::Started => Some(Duration::ZERO),
                    _ => current_pass.last_progress_at.or(Some(elapsed)),
                },
            },
        );
        apply_v2_pending_partition_detail(&mut status, pending);
        emit_status(&self.observer, status.clone());
        if state == StreamingIndexingStatusState::Started {
            current_pass.hierarchy_started_emitted = true;
        }
        Ok(status)
    }

    async fn resolve_batch(
        &self,
        batch: &[IndexItem<R>],
    ) -> Result<(Vec<Content>, Vec<Vec<u8>>), StreamingIndexerError>
    where
        CR: ContentResolver<R>,
        EP: EmbeddingProvider,
    {
        let mut inputs = Vec::with_capacity(batch.len());
        let mut contents = Vec::with_capacity(batch.len());
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
        Ok((contents, embeddings))
    }

    pub async fn ingest_batch(
        &mut self,
        batch: &[IndexItem<R>],
    ) -> Result<(), StreamingIndexerError>
    where
        CR: ContentResolver<R>,
        EP: EmbeddingProvider,
    {
        if !matches!(self.phase, RunPhase::Planning) {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                "ingest_batch requires the planning phase (currently {:?})",
                self.phase
            )));
        }
        if batch.is_empty() {
            return Ok(());
        }

        let (contents, embeddings) = self.resolve_batch(batch).await?;
        let mut current_pass = self
            .current_pass
            .take()
            .unwrap_or_else(StreamingV2PassState::new);
        if !current_pass.planning_started_emitted {
            let _ = self.emit_v2_planning_pass_status(
                &mut current_pass,
                StreamingIndexingStatusState::Started,
                None,
            )?;
            if !self.partitions.is_empty() {
                current_pass.ensure_partition_capacity(self.partitions.len());
                let pending_ids = self.pending_partition_ids();
                if let Some(&partition_id) = pending_ids.first() {
                    let _ = self.emit_v2_hierarchy_status(
                        &mut current_pass,
                        partition_id,
                        StreamingIndexingStatusState::Started,
                        0,
                        None,
                    )?;
                }
            }
        }
        let outcome = (|| -> Result<Vec<PartitionId>, StreamingIndexerError> {
            let mut grouped = HashMap::<PartitionId, Vec<Vec<f32>>>::new();

            for ((item, content), embedding) in
                batch.iter().zip(contents.iter()).zip(embeddings.iter())
            {
                let content_ref_hash = self
                    .resolver
                    .fingerprint(&item.content_ref)
                    .map_err(|e| StreamingIndexerError::ContentResolution(e.to_string()))?;
                let metadata_hash = hash_metadata(&item.metadata)
                    .map_err(StreamingIndexerError::InvalidMetadata)?;
                let content_hash = hash_content(content);
                let embedding_hash = hash_bytes(embedding);
                if !self.partitions.is_empty() {
                    let decoded = decode_embedding_as_f32(embedding, &self.embedding_spec)?;
                    let partition_id = {
                        current_pass.ensure_partition_capacity(self.partitions.len());
                        Self::route_planning_target_in_partitions(
                            &self.partitions,
                            decoded.as_slice(),
                            current_pass.replay_order_offsets.as_mut_slice(),
                            current_pass.classifier_assignment_counts.as_mut_slice(),
                        )?
                    };
                    if let Some(partition_id) = partition_id {
                        grouped.entry(partition_id).or_default().push(decoded);
                    }
                }
                current_pass.fingerprint.observe(
                    content_ref_hash,
                    metadata_hash,
                    content_hash,
                    embedding_hash,
                );
            }

            let touched_partition_ids = grouped.keys().copied().collect::<Vec<_>>();
            for (partition_id, partition_embeddings) in grouped {
                let label = self.partition_label(partition_id);
                let node = self.partition_mut(partition_id).map_err(|_| {
                    StreamingIndexerError::HierarchyValidation(format!(
                        "planning partition {label:?} is missing"
                    ))
                })?;
                let trainer = node.pending_trainer.as_mut().ok_or_else(|| {
                    StreamingIndexerError::HierarchyValidation(format!(
                        "planning partition {label:?} is not awaiting replay input"
                    ))
                })?;
                trainer
                    .ingest_batch(partition_embeddings.as_slice())
                    .map_err(map_clustering_error)?;
            }
            Ok(touched_partition_ids)
        })();
        let touched_partition_ids = match outcome {
            Ok(touched_partition_ids) => touched_partition_ids,
            Err(error) => {
                self.current_pass = Some(current_pass);
                return Err(error);
            }
        };
        current_pass.last_progress_at = Some(current_pass.started.elapsed());
        let _ = self.emit_v2_planning_pass_status(
            &mut current_pass,
            StreamingIndexingStatusState::InProgress,
            None,
        )?;
        for partition_id in touched_partition_ids {
            let _ = self.emit_v2_hierarchy_status(
                &mut current_pass,
                partition_id,
                StreamingIndexingStatusState::InProgress,
                0,
                None,
            )?;
        }
        self.current_pass = Some(current_pass);
        Ok(())
    }

    pub fn finish_pass(&mut self) -> Result<IndexingPassReport, StreamingIndexerError> {
        if !matches!(self.phase, RunPhase::Planning) {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                "finish_pass requires the planning phase (currently {:?})",
                self.phase
            )));
        }
        let Some(mut current_pass) = self.current_pass.take() else {
            return Err(StreamingIndexerError::EmptyPass(
                "at least one item must be ingested before completing a pass".into(),
            ));
        };
        if !current_pass.planning_started_emitted {
            let _ = self.emit_v2_planning_pass_status(
                &mut current_pass,
                StreamingIndexingStatusState::Started,
                None,
            )?;
        }
        current_pass.last_progress_at = current_pass
            .last_progress_at
            .or(Some(current_pass.started.elapsed()));
        let planning_in_progress = self.emit_v2_planning_pass_status(
            &mut current_pass,
            StreamingIndexingStatusState::InProgress,
            None,
        )?;
        let mut planning_heartbeat = StatusHeartbeatGuard::new(start_snapshot_status_heartbeat(
            &self.observer,
            planning_in_progress,
            current_pass.started,
            None,
        ));
        let pass_observed_count = current_pass.fingerprint.observed_count;
        let fingerprint = std::mem::replace(
            &mut current_pass.fingerprint,
            StreamingV2ReplayFingerprintTracker::new(),
        )
        .finish();
        let expandable_ids = current_pass
            .classifier_assignment_counts
            .iter()
            .enumerate()
            .filter_map(|(index, counts)| counts.as_ref().map(|_| PartitionId(index)))
            .collect::<Vec<_>>();
        if fingerprint.observed_count == 0 {
            planning_heartbeat.stop();
            return Err(StreamingIndexerError::EmptyPass(
                "at least one item must be ingested before completing a pass".into(),
            ));
        }

        if let Some(baseline) = &self.baseline_fingerprint {
            if baseline != &fingerprint {
                planning_heartbeat.stop();
                let status = build_v2_planning_pass_status(
                    self.completed_passes + 1,
                    StreamingIndexingStatusState::Failed,
                    self.v2_planning_pass_total(),
                    pass_observed_count,
                    current_pass.started.elapsed(),
                    current_pass.last_progress_at,
                    Some("planning replay differs from the established v2 baseline".into()),
                    self.maybe_v2_pending_partition_statuses(
                        &current_pass.classifier_assignment_counts,
                    )?,
                );
                emit_status(&self.observer, status);
                return Err(StreamingIndexerError::ReplayMismatch(
                    "planning replay differs from the established v2 baseline".into(),
                ));
            }
        } else {
            self.baseline_fingerprint = Some(fingerprint.clone());
        }

        let materializability_bound =
            materializability_bound(&self.embedding_spec, self.block_size_target)
                .map_err(StreamingIndexerError::TerminalPartitionMaterialization)?;
        let mut metrics = StreamingV2PassMetricAccumulator::default();

        if self.partitions.is_empty() {
            let root = self.create_partition_node(
                None,
                fingerprint.observed_count,
                materializability_bound,
            )?;
            let root_id = self
                .upcoming_partition_ids(1)
                .into_iter()
                .next()
                .expect("single root partition id");
            debug_assert_eq!(root_id, ROOT_PARTITION_ID);
            self.append_partition_nodes(vec![root]);
        } else {
            let pending_ids = self.pending_partition_ids();
            let mut completed = Vec::new();
            let mut completed_unit_count = 0;
            for partition_id in pending_ids {
                current_pass.last_progress_at = Some(current_pass.started.elapsed());
                if !current_pass.hierarchy_started_emitted {
                    let _ = self.emit_v2_hierarchy_status(
                        &mut current_pass,
                        partition_id,
                        StreamingIndexingStatusState::Started,
                        completed_unit_count,
                        None,
                    )?;
                }
                let in_progress = self.emit_v2_hierarchy_status(
                    &mut current_pass,
                    partition_id,
                    StreamingIndexingStatusState::InProgress,
                    completed_unit_count,
                    None,
                )?;
                let unit_started = current_pass
                    .partition_unit_started
                    .get(partition_id.0)
                    .copied()
                    .flatten();
                let mut unit_heartbeat =
                    StatusHeartbeatGuard::new(start_snapshot_status_heartbeat(
                        &self.observer,
                        in_progress,
                        current_pass.started,
                        unit_started,
                    ));
                let label = self.partition_label(partition_id);
                let expected_item_count = self.partition(partition_id)?.item_count;
                let completed_training = {
                    let node = self.partition_mut(partition_id).map_err(|_| {
                        StreamingIndexerError::HierarchyValidation(format!(
                            "pending partition {label:?} is missing"
                        ))
                    })?;
                    let trainer = node.pending_trainer.as_mut().ok_or_else(|| {
                        StreamingIndexerError::HierarchyValidation(format!(
                            "pending partition {label:?} has no trainer"
                        ))
                    })?;
                    let report = trainer.finish_pass().map_err(map_clustering_error)?;
                    metrics.observe(&report);
                    if report.observed_count != expected_item_count {
                        return Err(StreamingIndexerError::HierarchyValidation(format!(
                            "pending partition {label:?} observed {} items but expected {}",
                            report.observed_count, expected_item_count
                        )));
                    }
                    match trainer.complete_training() {
                        Ok(()) => true,
                        Err(StreamingClusteringError::InvalidTransition { state, operation })
                            if state == TrainerState::PassComplete
                                && operation == "complete_training" =>
                        {
                            false
                        }
                        Err(error) => return Err(map_clustering_error(error)),
                    }
                };
                unit_heartbeat.stop();
                if completed_training {
                    completed_unit_count += 1;
                    let trainer = self
                        .partition_mut(partition_id)?
                        .pending_trainer
                        .take()
                        .unwrap();
                    let item_count = self.partition(partition_id)?.item_count;
                    let classifier = trainer.into_classifier().map_err(map_clustering_error)?;
                    let realized_cluster_count =
                        usize::try_from(classifier.realized_cluster_count()).map_err(|_| {
                            StreamingIndexerError::HierarchyValidation(
                                "classifier realized cluster count does not fit into usize".into(),
                            )
                        })?;
                    let (routing, children) =
                        if realized_cluster_count <= 1 && item_count > materializability_bound {
                            let child_counts = balanced_groups(item_count, materializability_bound)
                                .map_err(StreamingIndexerError::HierarchyValidation)?
                                .into_iter()
                                .map(|group| group.len())
                                .collect::<Vec<_>>();
                            if child_counts.is_empty() {
                                return Err(StreamingIndexerError::HierarchyValidation(format!(
                                    "partition {label:?} produced no child routing plan"
                                )));
                            }
                            let routing = StreamingV2RoutingStrategy::ReplayOrder(
                                StreamingV2ReplayOrderPlan::new(child_counts.clone()),
                            );
                            let children = self.create_child_nodes(
                                partition_id,
                                child_counts,
                                materializability_bound,
                            )?;
                            (routing, children)
                        } else {
                            (
                                StreamingV2RoutingStrategy::Classifier(classifier),
                                Vec::new(),
                            )
                        };
                    completed.push(StreamingV2CompletedPartition {
                        partition_id,
                        routing,
                        children,
                    });
                    current_pass.last_progress_at = Some(current_pass.started.elapsed());
                    let _ = self.emit_v2_hierarchy_status(
                        &mut current_pass,
                        partition_id,
                        StreamingIndexingStatusState::Completed,
                        completed_unit_count,
                        None,
                    )?;
                } else {
                    current_pass.last_progress_at = Some(current_pass.started.elapsed());
                    let _ = self.emit_v2_hierarchy_status(
                        &mut current_pass,
                        partition_id,
                        StreamingIndexingStatusState::InProgress,
                        completed_unit_count,
                        None,
                    )?;
                }
            }

            for partition in completed {
                let child_ids = self.upcoming_partition_ids(partition.children.len());
                {
                    let node = self.partition_mut(partition.partition_id)?;
                    node.pending_trainer = None;
                    node.routing = Some(partition.routing);
                    node.child_ids = child_ids;
                    node.terminal = false;
                }
                self.append_partition_nodes(partition.children);
            }

            for partition_id in expandable_ids {
                let label = self.partition_label(partition_id);
                let node = self.partition(partition_id).map_err(|_| {
                    StreamingIndexerError::HierarchyValidation(format!(
                        "expandable partition {label:?} is missing"
                    ))
                })?;
                let routing = node.routing.as_ref().ok_or_else(|| {
                    StreamingIndexerError::HierarchyValidation(format!(
                        "expandable partition {label:?} is missing routing"
                    ))
                })?;
                let expected_child_count = match routing {
                    StreamingV2RoutingStrategy::Classifier(classifier) => {
                        usize::try_from(classifier.realized_cluster_count()).map_err(|_| {
                            StreamingIndexerError::HierarchyValidation(
                                "classifier realized cluster count does not fit into usize".into(),
                            )
                        })?
                    }
                    StreamingV2RoutingStrategy::ReplayOrder(_) => {
                        return Err(StreamingIndexerError::HierarchyValidation(format!(
                            "expandable partition {label:?} unexpectedly uses replay-order routing"
                        )));
                    }
                };
                let child_counts = current_pass
                    .classifier_assignment_counts
                    .get(partition_id.0)
                    .and_then(|counts| counts.as_ref())
                    .ok_or_else(|| {
                        StreamingIndexerError::HierarchyValidation(format!(
                            "partition {label:?} did not observe a full classifier replay before child sizing"
                        ))
                    })?
                    .clone();
                if child_counts.len() != expected_child_count {
                    return Err(StreamingIndexerError::HierarchyValidation(format!(
                        "partition {label:?} observed {} classifier child buckets but expected {}",
                        child_counts.len(),
                        expected_child_count
                    )));
                }
                if child_counts.contains(&0) {
                    return Err(StreamingIndexerError::HierarchyValidation(format!(
                        "partition {label:?} classifier replay left at least one child empty"
                    )));
                }
                let observed_child_total = child_counts.iter().sum::<usize>();
                if observed_child_total != node.item_count {
                    return Err(StreamingIndexerError::HierarchyValidation(format!(
                        "partition {label:?} classifier replay observed {} items but expected {}",
                        observed_child_total, node.item_count
                    )));
                }
                let children =
                    self.create_child_nodes(partition_id, child_counts, materializability_bound)?;
                let child_ids = self.upcoming_partition_ids(children.len());
                {
                    let node = self.partition_mut(partition_id).map_err(|_| {
                        StreamingIndexerError::HierarchyValidation(format!(
                            "expandable partition {label:?} disappeared before child installation"
                        ))
                    })?;
                    node.child_ids = child_ids;
                }
                self.append_partition_nodes(children);
            }
        }

        planning_heartbeat.stop();
        self.completed_passes += 1;
        let topology = self.current_topology();
        let stats = streaming_v2_topology_stats(&topology)
            .map_err(StreamingIndexerError::HierarchyValidation)?;
        let pending_partitions =
            self.v2_pending_partition_statuses(&current_pass.classifier_assignment_counts)?;
        let (completed_pass_summary, completed_pass_snapshot) = self
            .build_v2_completed_pass_summary(
                self.completed_passes,
                &stats,
                &topology,
                pending_partitions.clone(),
            );
        self.retain_v2_completed_pass_snapshot(completed_pass_snapshot);
        let mut completed_status = build_v2_planning_pass_status(
            self.completed_passes,
            StreamingIndexingStatusState::Completed,
            Some(pass_observed_count),
            pass_observed_count,
            current_pass.started.elapsed(),
            Some(current_pass.started.elapsed()),
            None,
            Some(pending_partitions.clone()),
        );
        completed_status.v2_completed_pass_summary = Some(completed_pass_summary.clone());
        emit_status(&self.observer, completed_status);
        Ok(IndexingPassReport {
            observed_item_count: fingerprint.observed_count,
            completed_pass_count: self.completed_passes,
            requested_planning_cluster_count: metrics.requested_cluster_count,
            realized_planning_cluster_count: metrics.realized_cluster_count,
            planning_quality_metric: metrics.average_quality(),
            planning_balance_metric: metrics.average_balance(),
            planning_quality_direction: metrics.quality_direction,
            planning_balance_direction: metrics.balance_direction,
            planned_partition_count: stats.partition_count,
            terminal_partition_count: stats.terminal_partition_count,
            hierarchy_depth: stats.depth,
            v2_completed_pass_summary: Some(completed_pass_summary),
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
        if self.current_pass.is_some() {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(
                "cannot complete planning with an open (unfinished) pass".into(),
            ));
        }
        if self.pending_partition_ids().is_empty() {
            if self.partitions.iter().any(|partition| {
                !partition.terminal
                    && partition.pending_trainer.is_none()
                    && partition.routing.is_some()
                    && partition.child_ids.is_empty()
            }) {
                return Err(StreamingIndexerError::InvalidLifecycleTransition(
                    "planning completion requires every routed v2 partition to install child partitions".into(),
                ));
            }
            validate_streaming_v2_topology(&self.current_topology())
                .map_err(StreamingIndexerError::HierarchyValidation)?;
            self.phase = RunPhase::PlanningComplete;
            return Ok(());
        }
        Err(StreamingIndexerError::InvalidLifecycleTransition(
            "planning completion requires every v2 partition to be terminal or routed".into(),
        ))
    }

    pub async fn finalize<I, B>(
        &mut self,
        replay_batches: I,
        store: &dyn BlockStore,
    ) -> Result<StreamingIndexingResult, StreamingIndexerError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[IndexItem<R>]>,
        CR: ContentResolver<R>,
        EP: EmbeddingProvider,
    {
        if !matches!(self.phase, RunPhase::PlanningComplete) {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                "finalize requires the planning-complete phase (currently {:?})",
                self.phase
            )));
        }
        let baseline = self.baseline_fingerprint.as_ref().ok_or_else(|| {
            StreamingIndexerError::InvalidLifecycleTransition("no v2 baseline established".into())
        })?;
        let topology = self.current_topology();
        validate_streaming_v2_topology(&topology)
            .map_err(StreamingIndexerError::HierarchyValidation)?;
        let materializability_bound =
            materializability_bound(&self.embedding_spec, self.block_size_target)
                .map_err(StreamingIndexerError::TerminalPartitionMaterialization)?;

        let mut terminal_partition_ids = self
            .partitions
            .iter()
            .enumerate()
            .filter(|(_, partition)| partition.terminal)
            .map(|(index, _)| PartitionId(index))
            .collect::<Vec<_>>();
        terminal_partition_ids
            .sort_by_cached_key(|&partition_id| self.partition_label(partition_id));
        let mut terminal_ordinals = vec![None; self.partitions.len()];
        for (ordinal, partition_id) in terminal_partition_ids.iter().copied().enumerate() {
            terminal_ordinals[partition_id.0] = Some(ordinal);
        }

        let mut fingerprint = StreamingV2ReplayFingerprintTracker::new();
        let mut replay_order_offsets = vec![0; self.partitions.len()];
        let mut replay_count = 0usize;
        let mut persisted_ids = Vec::new();
        let mut spill = PartitionSpillDirectory::new(terminal_partition_ids.len())?;

        for batch in replay_batches {
            let items = batch.as_ref();
            if items.is_empty() {
                continue;
            }
            let (contents, embeddings) = self.resolve_batch(items).await?;
            for ((item, content), embedding) in items.iter().zip(contents).zip(embeddings) {
                let content_ref_hash = self
                    .resolver
                    .fingerprint(&item.content_ref)
                    .map_err(|e| StreamingIndexerError::ContentResolution(e.to_string()))?;
                let metadata_hash = hash_metadata(&item.metadata)
                    .map_err(StreamingIndexerError::InvalidMetadata)?;
                let content_hash = hash_content(&content);
                let embedding_hash = hash_bytes(embedding.as_slice());
                fingerprint.observe(
                    content_ref_hash,
                    metadata_hash,
                    content_hash,
                    embedding_hash,
                );
                let decoded = decode_embedding_as_f32(embedding.as_slice(), &self.embedding_spec)?;
                let terminal_id = self.route_terminal_partition(
                    decoded.as_slice(),
                    replay_order_offsets.as_mut_slice(),
                )?;
                let partition_ordinal = terminal_ordinals
                    .get(terminal_id.0)
                    .and_then(|ordinal| *ordinal)
                    .ok_or_else(|| {
                        StreamingIndexerError::HierarchyValidation(format!(
                            "terminal partition {:?} is missing from the v2 topology",
                            self.partition_label(terminal_id)
                        ))
                    })?;

                let leaf = build_leaf_block(
                    VERSION_1,
                    self.embedding_spec.clone(),
                    vec![LeafEntry {
                        embedding: embedding.clone(),
                        metadata: item.metadata.clone(),
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
                    .await
                    .map_err(StreamingIndexerError::Storage)?;
                verify_persisted_block_id(block_id, serialized.hash)?;
                persisted_ids.push(block_id);
                spill.append_leaf_child(
                    u32::try_from(partition_ordinal).map_err(|_| {
                        StreamingIndexerError::HierarchyValidation(
                            "terminal partition ordinal does not fit into u32".into(),
                        )
                    })?,
                    &IndexedChild {
                        embedding,
                        child: block_id,
                        level: 0,
                        descendant_count: 1,
                    },
                )?;
                replay_count += 1;
            }
        }

        if replay_count == 0 {
            return Err(StreamingIndexerError::EmptyInput);
        }
        let finalized_fingerprint = fingerprint.finish();
        if &finalized_fingerprint != baseline {
            return Err(StreamingIndexerError::ReplayMismatch(
                "finalization replay differs from the established v2 baseline".into(),
            ));
        }

        let spill = spill.finish()?;
        let mut materialized_terminal_children = vec![None; self.partitions.len()];
        for (partition_ordinal, partition_id) in terminal_partition_ids.iter().copied().enumerate()
        {
            let partition = self.partition(partition_id)?;
            let partition_label = self.partition_label(partition_id);
            let leaf_children = spill.read_partition_children(partition_ordinal)?;
            if leaf_children.len() != partition.item_count {
                return Err(StreamingIndexerError::ReplayMismatch(format!(
                    "terminal partition {:?} spill contained {} items but expected {}",
                    partition_label,
                    leaf_children.len(),
                    partition.item_count
                )));
            }
            if leaf_children.is_empty() {
                return Err(StreamingIndexerError::ReplayMismatch(format!(
                    "terminal partition {:?} spill is empty",
                    partition_label
                )));
            }
            let child = if leaf_children.len() == 1 {
                leaf_children.into_iter().next().unwrap()
            } else {
                self.v2_assemble_child_set(
                    leaf_children,
                    partition_id == ROOT_PARTITION_ID,
                    materializability_bound,
                    store,
                    &mut persisted_ids,
                )
                .await?
            };
            materialized_terminal_children[partition_id.0] = Some(child);
        }

        let root_child = self
            .materialize_v2_partition_from_terminal_children(
                ROOT_PARTITION_ID,
                &materialized_terminal_children,
                materializability_bound,
                store,
                &mut persisted_ids,
            )
            .await?;
        dedup_sort_ids(&mut persisted_ids);
        self.phase = RunPhase::Finalized;
        Ok(StreamingIndexingResult {
            root_id: root_child.child,
            block_ids: persisted_ids,
        })
    }

    fn current_topology(&self) -> StreamingV2PartitionTopology {
        let partitions = self
            .partitions
            .iter()
            .enumerate()
            .map(|(index, partition)| {
                let partition_id = PartitionId(index);
                StreamingV2Partition {
                    id: self.partition_label(partition_id),
                    parent_id: partition
                        .parent_id
                        .map(|parent_id| self.partition_label(parent_id)),
                    child_ids: partition
                        .child_ids
                        .iter()
                        .map(|&child_id| self.partition_label(child_id))
                        .collect(),
                    item_count: partition.item_count,
                    terminal: partition.terminal,
                }
            })
            .collect::<Vec<_>>();
        StreamingV2PartitionTopology {
            root_partition_id: self.partition_label(ROOT_PARTITION_ID),
            partitions,
        }
    }

    fn upcoming_partition_ids(&self, count: usize) -> Vec<PartitionId> {
        debug_assert_eq!(self.next_partition_id, self.partitions.len());
        let end = self
            .next_partition_id
            .checked_add(count)
            .expect("partition id allocation overflowed usize");
        (self.next_partition_id..end).map(PartitionId).collect()
    }

    fn append_partition_nodes(&mut self, nodes: Vec<StreamingV2PartitionNode>) {
        debug_assert_eq!(self.next_partition_id, self.partitions.len());
        self.partitions.extend(nodes);
        self.next_partition_id = self.partitions.len();
    }

    fn partition(
        &self,
        partition_id: PartitionId,
    ) -> Result<&StreamingV2PartitionNode, StreamingIndexerError> {
        self.partitions.get(partition_id.0).ok_or_else(|| {
            StreamingIndexerError::HierarchyValidation(format!(
                "partition {:?} is missing from the v2 topology",
                compact_partition_label(partition_id)
            ))
        })
    }

    fn partition_mut(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<&mut StreamingV2PartitionNode, StreamingIndexerError> {
        self.partitions.get_mut(partition_id.0).ok_or_else(|| {
            StreamingIndexerError::HierarchyValidation(format!(
                "partition {:?} is missing from the v2 topology",
                compact_partition_label(partition_id)
            ))
        })
    }

    fn partition_label(&self, partition_id: PartitionId) -> String {
        format_partition_label(&self.partitions, partition_id)
    }

    fn profile_settings(
        &self,
    ) -> Result<&PublishedDirectionalPcaProfileSettings, StreamingIndexerError> {
        match &self.profile.planning_strategy {
            PublishedPlanningStrategy::DirectionalPcaDivisive(settings) => Ok(settings),
            _ => Err(StreamingIndexerError::ClusteringFailure(
                "streaming v2 currently requires directional-PCA divisive planning".into(),
            )),
        }
    }

    fn dimensions(&self) -> Result<usize, StreamingIndexerError> {
        usize::try_from(self.embedding_spec.dims).map_err(|_| {
            StreamingIndexerError::ClusteringFailure(format!(
                "embedding dims {} do not fit into usize",
                self.embedding_spec.dims
            ))
        })
    }

    fn create_partition_node(
        &self,
        parent_id: Option<PartitionId>,
        item_count: usize,
        materializability_bound: usize,
    ) -> Result<StreamingV2PartitionNode, StreamingIndexerError> {
        if item_count <= materializability_bound || item_count <= 1 {
            return Ok(StreamingV2PartitionNode {
                parent_id,
                child_ids: Vec::new(),
                item_count,
                terminal: true,
                pending_trainer: None,
                routing: None,
            });
        }
        let settings = self.profile_settings()?;
        let cluster_count = effective_directional_pca_cluster_count(
            settings.cluster_count,
            item_count,
            materializability_bound,
            settings.params.allocation_policy,
        )
        .map_err(map_clustering_configuration_error)?;
        let planner_state = StreamingV2QuantilePlannerState::new(&self.planner_state_root)?;
        let trainer = DirectionalPcaStreamingTrainer::new(
            StreamingClusteringConfig {
                cluster_count,
                dimensions: self.dimensions()?,
                balance_constraints: None,
                random_seed: settings.random_seed,
            },
            settings.params.clone(),
        )
        .map(|trainer| trainer.with_out_of_core_planner_state(Box::new(planner_state)))
        .map_err(map_clustering_error)?;
        Ok(StreamingV2PartitionNode {
            parent_id,
            child_ids: Vec::new(),
            item_count,
            terminal: false,
            pending_trainer: Some(trainer),
            routing: None,
        })
    }

    fn create_child_nodes(
        &self,
        parent_id: PartitionId,
        child_counts: Vec<usize>,
        materializability_bound: usize,
    ) -> Result<Vec<StreamingV2PartitionNode>, StreamingIndexerError> {
        let mut children = Vec::with_capacity(child_counts.len());
        for child_count in child_counts {
            children.push(self.create_partition_node(
                Some(parent_id),
                child_count,
                materializability_bound,
            )?);
        }
        Ok(children)
    }

    fn pending_partition_ids(&self) -> Vec<PartitionId> {
        self.partitions
            .iter()
            .enumerate()
            .filter(|(_, partition)| partition.pending_trainer.is_some())
            .map(|(index, _)| PartitionId(index))
            .collect()
    }

    fn route_planning_target_in_partitions(
        partitions: &[StreamingV2PartitionNode],
        embedding: &[f32],
        replay_order_offsets: &mut [usize],
        classifier_assignment_counts: &mut [Option<Vec<usize>>],
    ) -> Result<Option<PartitionId>, StreamingIndexerError> {
        if partitions.is_empty() {
            return Ok(None);
        }
        let mut current = ROOT_PARTITION_ID;
        loop {
            let partition = partitions.get(current.0).ok_or_else(|| {
                StreamingIndexerError::HierarchyValidation(format!(
                    "partition {:?} is missing from the v2 planning topology",
                    compact_partition_label(current)
                ))
            })?;
            if partition.terminal {
                return Ok(None);
            }
            if partition.pending_trainer.is_some() {
                return Ok(Some(current));
            }
            let routing = partition.routing.as_ref().ok_or_else(|| {
                StreamingIndexerError::HierarchyValidation(format!(
                    "partition {:?} is neither terminal, pending, nor routed",
                    format_partition_label(partitions, current)
                ))
            })?;
            let child_index = match routing {
                StreamingV2RoutingStrategy::Classifier(classifier) => {
                    usize::try_from(classifier.assign(embedding).map_err(map_clustering_error)?)
                        .map_err(|_| {
                            StreamingIndexerError::HierarchyValidation(
                                "classifier cluster id does not fit into usize".into(),
                            )
                        })?
                }
                StreamingV2RoutingStrategy::ReplayOrder(plan) => {
                    let seen = replay_order_offsets.get_mut(current.0).ok_or_else(|| {
                        StreamingIndexerError::HierarchyValidation(format!(
                            "partition {:?} is missing replay-order state",
                            format_partition_label(partitions, current)
                        ))
                    })?;
                    let index = plan.child_index_for_seen(*seen).map_err(|error| {
                        StreamingIndexerError::HierarchyValidation(error.to_string())
                    })?;
                    *seen += 1;
                    index
                }
            };
            if matches!(routing, StreamingV2RoutingStrategy::Classifier(_))
                && partition.child_ids.is_empty()
            {
                let expected_child_count = match routing {
                    StreamingV2RoutingStrategy::Classifier(classifier) => {
                        usize::try_from(classifier.realized_cluster_count()).map_err(|_| {
                            StreamingIndexerError::HierarchyValidation(
                                "classifier realized cluster count does not fit into usize".into(),
                            )
                        })?
                    }
                    StreamingV2RoutingStrategy::ReplayOrder(_) => unreachable!(),
                };
                let counts = classifier_assignment_counts
                    .get_mut(current.0)
                    .ok_or_else(|| {
                        StreamingIndexerError::HierarchyValidation(format!(
                            "partition {:?} is missing classifier-assignment state",
                            format_partition_label(partitions, current)
                        ))
                    })?
                    .get_or_insert_with(|| vec![0; expected_child_count]);
                let slot = counts.get_mut(child_index).ok_or_else(|| {
                    StreamingIndexerError::HierarchyValidation(format!(
                        "classifier for partition {:?} routed to missing child count slot {}",
                        format_partition_label(partitions, current),
                        child_index
                    ))
                })?;
                *slot += 1;
                return Ok(None);
            }
            current = partition
                .child_ids
                .get(child_index)
                .copied()
                .ok_or_else(|| {
                    StreamingIndexerError::HierarchyValidation(format!(
                        "partition {:?} routed to missing child index {}",
                        format_partition_label(partitions, current),
                        child_index
                    ))
                })?;
        }
    }

    fn route_terminal_partition(
        &self,
        embedding: &[f32],
        replay_order_offsets: &mut [usize],
    ) -> Result<PartitionId, StreamingIndexerError> {
        let mut current = ROOT_PARTITION_ID;
        loop {
            let partition = self.partition(current)?;
            if partition.terminal {
                return Ok(current);
            }
            if partition.pending_trainer.is_some() {
                return Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                    "partition {:?} is still awaiting planning completion",
                    self.partition_label(current)
                )));
            }
            let routing = partition.routing.as_ref().ok_or_else(|| {
                StreamingIndexerError::HierarchyValidation(format!(
                    "partition {:?} is missing a routing strategy",
                    self.partition_label(current)
                ))
            })?;
            let child_index = match routing {
                StreamingV2RoutingStrategy::Classifier(classifier) => {
                    usize::try_from(classifier.assign(embedding).map_err(map_clustering_error)?)
                        .map_err(|_| {
                            StreamingIndexerError::HierarchyValidation(
                                "classifier cluster id does not fit into usize".into(),
                            )
                        })?
                }
                StreamingV2RoutingStrategy::ReplayOrder(plan) => {
                    let seen = replay_order_offsets.get_mut(current.0).ok_or_else(|| {
                        StreamingIndexerError::HierarchyValidation(format!(
                            "partition {:?} is missing replay-order state",
                            self.partition_label(current)
                        ))
                    })?;
                    let index = plan.child_index_for_seen(*seen).map_err(|error| {
                        StreamingIndexerError::HierarchyValidation(error.to_string())
                    })?;
                    *seen += 1;
                    index
                }
            };
            current = partition
                .child_ids
                .get(child_index)
                .copied()
                .ok_or_else(|| {
                    StreamingIndexerError::HierarchyValidation(format!(
                        "partition {:?} routed to missing child index {}",
                        self.partition_label(current),
                        child_index
                    ))
                })?;
        }
    }

    #[async_recursion(?Send)]
    async fn materialize_v2_partition_from_terminal_children(
        &self,
        partition_id: PartitionId,
        materialized_terminal_children: &[Option<IndexedChild>],
        materializability_bound: usize,
        store: &dyn BlockStore,
        persisted_ids: &mut Vec<BlockHash>,
    ) -> Result<IndexedChild, StreamingIndexerError> {
        let partition = self.partition(partition_id)?;
        if partition.terminal {
            return materialized_terminal_children
                .get(partition_id.0)
                .and_then(|child| child.clone())
                .ok_or_else(|| {
                    StreamingIndexerError::ReplayMismatch(format!(
                        "terminal v2 partition {:?} was not materialized",
                        self.partition_label(partition_id)
                    ))
                });
        }
        let mut children = Vec::with_capacity(partition.child_ids.len());
        for child_id in &partition.child_ids {
            children.push(
                self.materialize_v2_partition_from_terminal_children(
                    *child_id,
                    materialized_terminal_children,
                    materializability_bound,
                    store,
                    persisted_ids,
                )
                .await?,
            );
        }
        self.v2_assemble_child_set(
            children,
            partition_id == ROOT_PARTITION_ID,
            materializability_bound,
            store,
            persisted_ids,
        )
        .await
    }

    async fn v2_assemble_child_set(
        &self,
        children: Vec<IndexedChild>,
        is_global_root_partition: bool,
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
            let next_level = current.iter().map(|child| child.level).max().unwrap_or(0) + 1;
            current = normalize_current_layer(
                self.v2_build_branch_layer(
                    &current,
                    &groups,
                    next_level,
                    is_global_root_partition,
                    store,
                    persisted_ids,
                )
                .await?,
            );
        }
    }

    async fn v2_build_branch_layer(
        &self,
        children: &[IndexedChild],
        groups: &[Vec<usize>],
        parent_level: u64,
        is_global_root_partition: bool,
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
            let raw_child_summaries = group
                .iter()
                .map(|&index| ChildSummaryInput {
                    embedding: children[index].embedding.clone(),
                    child: children[index].child,
                    level: children[index].level,
                    descendant_count: children[index].descendant_count,
                })
                .collect::<Vec<_>>();
            let entries = normalize_branch_entries(raw_entries);
            let child_summaries = normalize_child_summary_inputs(raw_child_summaries);
            if entries.len() < 2 {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    "normalized child-bearing entry set has fewer than two unique children".into(),
                ));
            }
            if child_summaries.len() < 2 {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    "normalized child summary set has fewer than two unique children".into(),
                ));
            }
            let encoded_branch = encode_branch_entries(
                self.branch_encoding_policy,
                &self.embedding_spec,
                entries.as_slice(),
                parent_level,
                uses_root_branch_budget(is_global_root_partition, groups.len()),
            )?;
            let branch = build_branch_block(
                VERSION_1,
                parent_level,
                encoded_branch.embedding_spec,
                encoded_branch.entries,
                encoded_branch.ext,
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
                .await
                .map_err(StreamingIndexerError::Storage)?;
            verify_persisted_block_id(block_id, serialized.hash)?;
            persisted_ids.push(block_id);
            let canonical = ExactCentroidChildSummaryPolicy
                .summarize_children(&self.embedding_spec, &child_summaries)
                .map_err(|e| StreamingIndexerError::CanonicalEmbeddingFailure(e.to_string()))?;
            validate_embedding_bytes(&canonical, &self.embedding_spec, "canonical")
                .map_err(StreamingIndexerError::CanonicalEmbeddingFailure)?;
            next_layer.push(IndexedChild {
                embedding: canonical,
                child: block_id,
                level: parent_level,
                descendant_count: child_summaries
                    .iter()
                    .map(|child| child.descendant_count)
                    .sum(),
            });
        }
        Ok(next_layer)
    }
}

impl StreamingV2PassMetricAccumulator {
    fn observe(&mut self, report: &PassReport) {
        if self.cluster_runs == 0 {
            self.quality_direction = report.quality_direction;
            self.balance_direction = report.balance_direction;
            self.requested_cluster_count = Some(report.requested_cluster_count);
            self.realized_cluster_count = report.realized_cluster_count;
        } else {
            if self.requested_cluster_count != Some(report.requested_cluster_count) {
                self.requested_cluster_count = None;
            }
            if self.realized_cluster_count != report.realized_cluster_count {
                self.realized_cluster_count = None;
            }
        }
        self.quality_sum += report.quality_metric;
        self.balance_sum += report.balance_metric;
        self.cluster_runs += 1;
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

impl StreamingV2ReplayFingerprintTracker {
    fn new() -> Self {
        Self {
            observed_count: 0,
            hasher: Sha256::new(),
        }
    }

    fn observe(
        &mut self,
        content_ref_hash: BlockHash,
        metadata_hash: BlockHash,
        content_hash: BlockHash,
        embedding_hash: BlockHash,
    ) {
        self.observed_count += 1;
        self.hasher.update(content_ref_hash.as_bytes());
        self.hasher.update(metadata_hash.as_bytes());
        self.hasher.update(content_hash.as_bytes());
        self.hasher.update(embedding_hash.as_bytes());
    }

    fn finish(self) -> StreamingV2ReplayFingerprint {
        StreamingV2ReplayFingerprint {
            observed_count: self.observed_count,
            digest: self.hasher.finalize().into(),
        }
    }
}

impl PartialEq for StreamingV2ReplayFingerprint {
    fn eq(&self, other: &Self) -> bool {
        self.observed_count == other.observed_count && self.digest == other.digest
    }
}

impl Eq for StreamingV2ReplayFingerprint {}

impl StreamingV2PassState {
    fn new() -> Self {
        Self {
            fingerprint: StreamingV2ReplayFingerprintTracker::new(),
            replay_order_offsets: Vec::new(),
            classifier_assignment_counts: Vec::new(),
            started: Instant::now(),
            last_progress_at: None,
            planning_started_emitted: false,
            hierarchy_started_emitted: false,
            partition_unit_started: Vec::new(),
        }
    }

    fn ensure_partition_capacity(&mut self, partition_count: usize) {
        if self.replay_order_offsets.len() < partition_count {
            self.replay_order_offsets.resize(partition_count, 0);
        }
        if self.classifier_assignment_counts.len() < partition_count {
            self.classifier_assignment_counts
                .resize_with(partition_count, || None);
        }
        if self.partition_unit_started.len() < partition_count {
            self.partition_unit_started.resize(partition_count, None);
        }
    }
}

impl StreamingV2ReplayOrderPlan {
    fn new(child_counts: Vec<usize>) -> Self {
        Self { child_counts }
    }

    fn child_index_for_seen(&self, seen: usize) -> Result<usize, &'static str> {
        let mut offset = 0usize;
        for (index, count) in self.child_counts.iter().copied().enumerate() {
            let limit = offset.saturating_add(count);
            if seen < limit {
                return Ok(index);
            }
            offset = limit;
        }
        Err("replay-order routing consumed more items than the partition expected")
    }
}

fn compact_partition_label(partition_id: PartitionId) -> String {
    format!("p{}", partition_id.0)
}

fn format_partition_label(
    partitions: &[StreamingV2PartitionNode],
    partition_id: PartitionId,
) -> String {
    if partition_id == ROOT_PARTITION_ID {
        return "p0".into();
    }
    let mut path = Vec::new();
    let mut current = partition_id;
    loop {
        let Some(partition) = partitions.get(current.0) else {
            return compact_partition_label(partition_id);
        };
        let Some(parent_id) = partition.parent_id else {
            if current != ROOT_PARTITION_ID {
                return compact_partition_label(partition_id);
            }
            break;
        };
        let Some(parent) = partitions.get(parent_id.0) else {
            return compact_partition_label(partition_id);
        };
        let Some(child_index) = parent
            .child_ids
            .iter()
            .position(|&child_id| child_id == current)
        else {
            return compact_partition_label(partition_id);
        };
        path.push(child_index.to_string());
        current = parent_id;
    }
    path.reverse();
    if path.is_empty() {
        "p0".into()
    } else {
        format!("p0.{}", path.join("."))
    }
}

fn validate_streaming_v2_topology(topology: &StreamingV2PartitionTopology) -> Result<(), String> {
    let partitions = topology
        .partitions
        .iter()
        .map(|partition| (partition.id.clone(), partition))
        .collect::<HashMap<_, _>>();
    let root = partitions
        .get(&topology.root_partition_id)
        .ok_or_else(|| "v2 root partition is missing".to_string())?;
    if root.parent_id.is_some() {
        return Err("v2 root partition must not have a parent".into());
    }

    fn walk(
        partition_id: &str,
        partitions: &HashMap<String, &StreamingV2Partition>,
        visited: &mut BTreeSet<String>,
    ) -> Result<usize, String> {
        let partition = partitions
            .get(partition_id)
            .ok_or_else(|| format!("v2 partition {partition_id:?} is missing"))?;
        if !visited.insert(partition_id.to_string()) {
            return Err(format!(
                "v2 partition topology contains a cycle at {partition_id:?}"
            ));
        }
        if partition.item_count == 0 {
            return Err(format!(
                "v2 partition {:?} must contain at least one logical item",
                partition.id
            ));
        }
        if partition.terminal {
            if !partition.child_ids.is_empty() {
                return Err(format!(
                    "terminal v2 partition {:?} must not declare children",
                    partition.id
                ));
            }
            return Ok(partition.item_count);
        }
        if partition.child_ids.is_empty() {
            return Err(format!(
                "non-terminal v2 partition {:?} must declare children",
                partition.id
            ));
        }
        let mut total = 0usize;
        for child_id in &partition.child_ids {
            let child = partitions.get(child_id).ok_or_else(|| {
                format!(
                    "v2 partition {:?} references missing child {:?}",
                    partition.id, child_id
                )
            })?;
            if child.parent_id.as_deref() != Some(partition.id.as_str()) {
                return Err(format!(
                    "v2 partition {:?} has ancestry mismatch for child {:?}",
                    partition.id, child_id
                ));
            }
            total = total
                .checked_add(walk(child_id, partitions, visited)?)
                .ok_or_else(|| "v2 partition item counts overflowed usize".to_string())?;
        }
        if total != partition.item_count {
            return Err(format!(
                "v2 partition {:?} item_count {} does not match child total {}",
                partition.id, partition.item_count, total
            ));
        }
        Ok(total)
    }

    let mut visited = BTreeSet::new();
    let root_count = walk(&topology.root_partition_id, &partitions, &mut visited)?;
    if root_count != root.item_count {
        return Err("v2 root partition count must match its recursive coverage".into());
    }
    if visited.len() != topology.partitions.len() {
        return Err("v2 partition topology contains unreachable partitions".into());
    }
    Ok(())
}

fn streaming_v2_topology_stats(
    topology: &StreamingV2PartitionTopology,
) -> Result<HierarchyStats, String> {
    let partitions = topology
        .partitions
        .iter()
        .map(|partition| (partition.id.clone(), partition))
        .collect::<HashMap<_, _>>();

    fn depth_of(
        partition_id: &str,
        partitions: &HashMap<String, &StreamingV2Partition>,
        visiting: &mut BTreeSet<String>,
        memo: &mut HashMap<String, usize>,
    ) -> Result<usize, String> {
        if let Some(depth) = memo.get(partition_id) {
            return Ok(*depth);
        }
        let partition = partitions.get(partition_id).ok_or_else(|| {
            format!("v2 partition topology stats referenced missing partition {partition_id:?}")
        })?;
        if !visiting.insert(partition_id.to_string()) {
            return Err(format!(
                "v2 partition topology stats detected a cycle at partition {partition_id:?}"
            ));
        }
        let depth = if partition.child_ids.is_empty() {
            1
        } else {
            1 + partition
                .child_ids
                .iter()
                .map(|child_id| depth_of(child_id, partitions, visiting, memo))
                .max()
                .transpose()?
                .unwrap_or(0)
        };
        visiting.remove(partition_id);
        memo.insert(partition_id.to_string(), depth);
        Ok(depth)
    }

    Ok(HierarchyStats {
        partition_count: topology.partitions.len(),
        terminal_partition_count: topology.partitions.iter().filter(|p| p.terminal).count(),
        depth: if topology.partitions.is_empty() {
            0
        } else {
            depth_of(
                &topology.root_partition_id,
                &partitions,
                &mut BTreeSet::new(),
                &mut HashMap::new(),
            )?
        },
    })
}

fn summarize_streaming_v2_completed_pass_delta(
    previous: Option<&StreamingV2CompletedPassSnapshot>,
    current: &StreamingV2CompletedPassSnapshot,
    repeated_prior_completed_pass_number: Option<usize>,
) -> StreamingV2CompletedPassDelta {
    let current_pending_partition_paths = current
        .pending_partitions
        .iter()
        .map(|status| status.partition_path.clone())
        .collect::<Vec<_>>();
    let Some(previous) = previous else {
        return StreamingV2CompletedPassDelta {
            previous_completed_pass_number: None,
            previous_topology_fingerprint_hex: None,
            topology_fingerprint_hex: current.topology_fingerprint_hex.clone(),
            previous_pending_partition_fingerprint_hex: None,
            pending_partition_fingerprint_hex: current.pending_partition_fingerprint_hex.clone(),
            pending_partition_count_delta: None,
            terminal_partition_count_delta: None,
            routed_partition_count_delta: None,
            planned_partition_count_delta: None,
            hierarchy_depth_delta: None,
            topology_changed: None,
            pending_partitions_changed: None,
            repeated_prior_completed_pass_number,
            current_pending_partition_paths,
            added_pending_partition_paths: Vec::new(),
            removed_pending_partition_paths: Vec::new(),
            unchanged_pending_partition_paths: Vec::new(),
            changed_pending_partitions: Vec::new(),
            newly_terminal_partition_paths: current.terminal_partition_paths.clone(),
            newly_routed_partition_paths: current.routed_partition_paths.clone(),
        };
    };

    let previous_pending = previous
        .pending_partitions
        .iter()
        .map(|status| (status.partition_path.as_str(), status))
        .collect::<HashMap<_, _>>();
    let current_pending = current
        .pending_partitions
        .iter()
        .map(|status| (status.partition_path.as_str(), status))
        .collect::<HashMap<_, _>>();

    let mut added_pending_partition_paths = current
        .pending_partitions
        .iter()
        .filter(|status| !previous_pending.contains_key(status.partition_path.as_str()))
        .map(|status| status.partition_path.clone())
        .collect::<Vec<_>>();
    let mut removed_pending_partition_paths = previous
        .pending_partitions
        .iter()
        .filter(|status| !current_pending.contains_key(status.partition_path.as_str()))
        .map(|status| status.partition_path.clone())
        .collect::<Vec<_>>();
    let mut unchanged_pending_partition_paths = Vec::new();
    let mut changed_pending_partitions = Vec::new();
    for status in &current.pending_partitions {
        let Some(previous_status) = previous_pending.get(status.partition_path.as_str()) else {
            continue;
        };
        if *previous_status == status {
            unchanged_pending_partition_paths.push(status.partition_path.clone());
            continue;
        }
        changed_pending_partitions.push(StreamingV2PendingPartitionDelta {
            partition_path: status.partition_path.clone(),
            previous_trainer_subphase: previous_status.trainer_subphase,
            trainer_subphase: status.trainer_subphase,
            previous_observed_replay_progress: previous_status.observed_replay_progress,
            observed_replay_progress: status.observed_replay_progress,
            previous_routing_bucket_fill_counts: previous_status.routing_bucket_fill_counts.clone(),
            routing_bucket_fill_counts: status.routing_bucket_fill_counts.clone(),
            previous_ready_axis_plan_count: previous_status.ready_axis_plan_count,
            ready_axis_plan_count: status.ready_axis_plan_count,
            previous_total_axis_plan_count: previous_status.total_axis_plan_count,
            total_axis_plan_count: status.total_axis_plan_count,
            previous_populated_cell_count: previous_status.populated_cell_count,
            populated_cell_count: status.populated_cell_count,
            previous_realized_cell_count: previous_status.realized_cell_count,
            realized_cell_count: status.realized_cell_count,
            previous_planner_state_fingerprint_hex: Some(
                previous_status.planner_state_fingerprint_hex.clone(),
            ),
            planner_state_fingerprint_hex: status.planner_state_fingerprint_hex.clone(),
        });
    }
    added_pending_partition_paths.sort();
    removed_pending_partition_paths.sort();
    unchanged_pending_partition_paths.sort();
    changed_pending_partitions
        .sort_by(|left, right| left.partition_path.cmp(&right.partition_path));

    let previous_terminal = previous
        .terminal_partition_paths
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let current_terminal = current
        .terminal_partition_paths
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let previous_routed = previous
        .routed_partition_paths
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let current_routed = current
        .routed_partition_paths
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();

    StreamingV2CompletedPassDelta {
        previous_completed_pass_number: Some(previous.pass_number),
        previous_topology_fingerprint_hex: Some(previous.topology_fingerprint_hex.clone()),
        topology_fingerprint_hex: current.topology_fingerprint_hex.clone(),
        previous_pending_partition_fingerprint_hex: Some(
            previous.pending_partition_fingerprint_hex.clone(),
        ),
        pending_partition_fingerprint_hex: current.pending_partition_fingerprint_hex.clone(),
        pending_partition_count_delta: usize_delta(
            current.pending_partitions.len(),
            previous.pending_partitions.len(),
        ),
        terminal_partition_count_delta: usize_delta(
            current.terminal_partition_count,
            previous.terminal_partition_count,
        ),
        routed_partition_count_delta: usize_delta(
            current.routed_partition_paths.len(),
            previous.routed_partition_paths.len(),
        ),
        planned_partition_count_delta: usize_delta(
            current.planned_partition_count,
            previous.planned_partition_count,
        ),
        hierarchy_depth_delta: usize_delta(current.hierarchy_depth, previous.hierarchy_depth),
        topology_changed: Some(
            current.topology_fingerprint_hex != previous.topology_fingerprint_hex,
        ),
        pending_partitions_changed: Some(
            current.pending_partition_fingerprint_hex != previous.pending_partition_fingerprint_hex,
        ),
        repeated_prior_completed_pass_number,
        current_pending_partition_paths,
        added_pending_partition_paths,
        removed_pending_partition_paths,
        unchanged_pending_partition_paths,
        changed_pending_partitions,
        newly_terminal_partition_paths: current_terminal
            .difference(&previous_terminal)
            .cloned()
            .collect(),
        newly_routed_partition_paths: current_routed
            .difference(&previous_routed)
            .cloned()
            .collect(),
    }
}

fn summarize_streaming_v2_partition_blocker(
    status: &StreamingV2PendingPartitionStatus,
) -> StreamingV2PartitionBlockerSummary {
    let (blocker_kind, blocker_detail) = match status.trainer_subphase {
        Some(StreamingIndexingTrainerSubphase::AnalyzePca) => (
            StreamingV2BlockerKind::AnalyzePcaPending,
            format!(
                "partition still requires PCA analysis replay; observed {} of expected {} items",
                format_optional_usize(status.observed_replay_progress),
                status.expected_item_count
            ),
        ),
        Some(StreamingIndexingTrainerSubphase::PlanCuts) => (
            StreamingV2BlockerKind::PlanCutsPending,
            format!(
                "partition still requires cut planning; ready axes {} of {}",
                format_optional_usize(status.ready_axis_plan_count),
                format_optional_usize(status.total_axis_plan_count)
            ),
        ),
        Some(StreamingIndexingTrainerSubphase::CountCells) => (
            StreamingV2BlockerKind::CountCellsPending,
            format!(
                "partition still requires cell counting; populated cells observed {}",
                format_optional_usize(status.populated_cell_count)
            ),
        ),
        Some(StreamingIndexingTrainerSubphase::RealizePartition) => (
            StreamingV2BlockerKind::RealizePartitionPending,
            format!(
                "partition still requires partition realization; realized cells {}",
                format_optional_usize(status.realized_cell_count)
            ),
        ),
        None => {
            match status.observed_replay_progress {
                Some(observed) if observed < status.expected_item_count => (
                    StreamingV2BlockerKind::ReplayIncomplete,
                    format!(
                        "partition replay is incomplete; observed {observed} of expected {} items",
                        status.expected_item_count
                    ),
                ),
                Some(_) => (
                    StreamingV2BlockerKind::Unknown,
                    "partition remains unresolved but retained state does not expose a stronger blocker"
                        .into(),
                ),
                None => (
                    StreamingV2BlockerKind::Unknown,
                    format!(
                        "partition remains unresolved and replay progress is unknown; retained state does not expose a stronger blocker for expected {} items",
                        status.expected_item_count
                    ),
                ),
            }
        }
    };
    StreamingV2PartitionBlockerSummary {
        partition_path: status.partition_path.clone(),
        expected_item_count: status.expected_item_count,
        trainer_subphase: status.trainer_subphase,
        blocker_kind,
        blocker_detail,
        observed_replay_progress: status.observed_replay_progress,
        routing_bucket_fill_counts: status.routing_bucket_fill_counts.clone(),
        ready_axis_plan_count: status.ready_axis_plan_count,
        total_axis_plan_count: status.total_axis_plan_count,
        populated_cell_count: status.populated_cell_count,
        realized_cell_count: status.realized_cell_count,
        planner_state_fingerprint_hex: status.planner_state_fingerprint_hex.clone(),
    }
}

fn unresolved_work_shrank(
    previous: &StreamingV2CompletedPassSnapshot,
    current: &StreamingV2CompletedPassSnapshot,
) -> bool {
    let pending_count_decreased =
        current.pending_partitions.len() < previous.pending_partitions.len();
    let pending_count_unchanged =
        current.pending_partitions.len() == previous.pending_partitions.len();
    let terminal_or_routed_grew = current.terminal_partition_count
        > previous.terminal_partition_count
        || current.routed_partition_paths.len() > previous.routed_partition_paths.len();
    pending_count_decreased || (pending_count_unchanged && terminal_or_routed_grew)
}

fn usize_delta(current: usize, previous: usize) -> Option<isize> {
    let current = i128::try_from(current).ok()?;
    let previous = i128::try_from(previous).ok()?;
    isize::try_from(current - previous).ok()
}

fn hash_streaming_v2_topology_hex(topology: &StreamingV2PartitionTopology) -> String {
    encode_digest_hex(hash_with_sha256(|hasher| {
        hash_string(hasher, &topology.root_partition_id);
        hash_usize_sha256(hasher, topology.partitions.len());
        for partition in &topology.partitions {
            hash_string(hasher, &partition.id);
            hash_optional_string(hasher, partition.parent_id.as_deref());
            hash_strings(hasher, &partition.child_ids);
            hash_usize_sha256(hasher, partition.item_count);
            hasher.update([u8::from(partition.terminal)]);
        }
    }))
}

fn hash_streaming_v2_pending_partitions_hex(
    pending_partitions: &[StreamingV2PendingPartitionStatus],
) -> String {
    encode_digest_hex(hash_with_sha256(|hasher| {
        hash_usize_sha256(hasher, pending_partitions.len());
        for partition in pending_partitions {
            hash_string(hasher, &partition.partition_path);
            hash_usize_sha256(hasher, partition.expected_item_count);
            hash_optional_usize(hasher, partition.observed_replay_progress);
            hash_optional_usizes(hasher, partition.routing_bucket_fill_counts.as_deref());
            hash_optional_usize(
                hasher,
                partition
                    .trainer_subphase
                    .map(streaming_v2_trainer_subphase_code),
            );
            hash_optional_usize(hasher, partition.ready_axis_plan_count);
            hash_optional_usize(hasher, partition.total_axis_plan_count);
            hash_optional_usize(hasher, partition.populated_cell_count);
            hash_optional_usize(hasher, partition.realized_cell_count);
            hash_string(hasher, &partition.planner_state_fingerprint_hex);
        }
    }))
}

fn hash_streaming_v2_completed_pass_state_hex(
    topology_fingerprint_hex: &str,
    pending_partition_fingerprint_hex: &str,
) -> String {
    encode_digest_hex(hash_with_sha256(|hasher| {
        hash_string(hasher, topology_fingerprint_hex);
        hash_string(hasher, pending_partition_fingerprint_hex);
    }))
}

fn hash_streaming_v2_bucket_fill_counts_hex(bucket_fill_counts: &[usize]) -> String {
    encode_digest_hex(hash_with_sha256(|hasher| {
        hash_usizes_sha256(hasher, bucket_fill_counts);
    }))
}

fn format_optional_usize(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn hash_with_sha256(update: impl FnOnce(&mut Sha256)) -> [u8; 32] {
    let mut hasher = Sha256::new();
    update(&mut hasher);
    hasher.finalize().into()
}

fn encode_digest_hex(bytes: [u8; 32]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(hex_nibble(byte >> 4));
        text.push(hex_nibble(byte & 0x0f));
    }
    text
}

fn hex_nibble(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'a' + (value - 10)),
        _ => unreachable!("hex nibble out of range"),
    }
}

fn hash_usize_sha256(hasher: &mut Sha256, value: usize) {
    hasher.update((value as u64).to_le_bytes());
}

fn hash_optional_usize(hasher: &mut Sha256, value: Option<usize>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hash_usize_sha256(hasher, value);
        }
        None => hasher.update([0]),
    }
}

fn hash_usizes_sha256(hasher: &mut Sha256, values: &[usize]) {
    hash_usize_sha256(hasher, values.len());
    for value in values {
        hash_usize_sha256(hasher, *value);
    }
}

fn hash_optional_usizes(hasher: &mut Sha256, values: Option<&[usize]>) {
    match values {
        Some(values) => {
            hasher.update([1]);
            hash_usizes_sha256(hasher, values);
        }
        None => hasher.update([0]),
    }
}

fn hash_string(hasher: &mut Sha256, value: &str) {
    hash_usize_sha256(hasher, value.len());
    hasher.update(value.as_bytes());
}

fn hash_optional_string(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hash_string(hasher, value);
        }
        None => hasher.update([0]),
    }
}

fn hash_strings(hasher: &mut Sha256, values: &[String]) {
    hash_usize_sha256(hasher, values.len());
    for value in values {
        hash_string(hasher, value);
    }
}

fn streaming_v2_trainer_subphase_code(subphase: StreamingIndexingTrainerSubphase) -> usize {
    match subphase {
        StreamingIndexingTrainerSubphase::AnalyzePca => 0,
        StreamingIndexingTrainerSubphase::PlanCuts => 1,
        StreamingIndexingTrainerSubphase::CountCells => 2,
        StreamingIndexingTrainerSubphase::RealizePartition => 3,
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
    stage_observer: &mut impl FnMut(HierarchyPlanningStatusEvent),
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
        BuiltInPlanning::SphericalKmeans(settings) => derive_hierarchy_for_single_built_in_phase(
            BuiltInPlanningPhase::SphericalKmeans(settings.clone()),
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
                        Ok(PartitionPlanner::new(
                            stage,
                            create_built_in_trainer(
                                &phase,
                                partition_embeddings.len(),
                                partition_embeddings.first().map_or(0, std::vec::Vec::len),
                                embedding_spec,
                                materializability_bound,
                            )?,
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
                            Ok(PartitionPlanner::new(
                                stage,
                                create_built_in_trainer(
                                    &phase,
                                    layer_embeddings.len(),
                                    layer_embeddings.first().map_or(0, std::vec::Vec::len),
                                    embedding_spec,
                                    materializability_bound,
                                )?,
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
    stage_observer: &mut impl FnMut(HierarchyPlanningStatusEvent),
) -> Result<PlanningPassOutcome, StreamingIndexerError> {
    derive_hierarchy_for_single_built_in_phase_with_fallback_group_cap(
        phase,
        embeddings,
        embedding_spec,
        materializability_bound,
        None,
        stage_observer,
    )
}

fn derive_hierarchy_for_single_built_in_phase_with_fallback_group_cap(
    phase: BuiltInPlanningPhase,
    embeddings: &[Vec<f32>],
    embedding_spec: &EmbeddingSpec,
    materializability_bound: usize,
    fallback_group_cap: Option<usize>,
    stage_observer: &mut impl FnMut(HierarchyPlanningStatusEvent),
) -> Result<PlanningPassOutcome, StreamingIndexerError> {
    match phase.direction() {
        BuiltInPlanningDirection::Divisive => derive_hierarchy_with_builder_and_fallback_group_cap(
            embeddings,
            materializability_bound,
            fallback_group_cap,
            stage_observer,
            |partition_embeddings| {
                Ok(PartitionPlanner::new(
                    PlanningStage::Single,
                    create_built_in_trainer(
                        &phase,
                        partition_embeddings.len(),
                        partition_embeddings.first().map_or(0, std::vec::Vec::len),
                        embedding_spec,
                        materializability_bound,
                    )?,
                ))
            },
        ),
        BuiltInPlanningDirection::Agglomerative => derive_hierarchy_agglomeratively_with_builder(
            embeddings,
            materializability_bound,
            stage_observer,
            |layer_embeddings, _represented_item_count, _max_unit_item_count| {
                Ok(PartitionPlanner::new(
                    PlanningStage::Single,
                    create_built_in_trainer(
                        &phase,
                        layer_embeddings.len(),
                        layer_embeddings.first().map_or(0, std::vec::Vec::len),
                        embedding_spec,
                        materializability_bound,
                    )?,
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
            let cluster_count = effective_directional_pca_cluster_count(
                settings.cluster_count,
                partition_len,
                materializability_bound,
                settings.params.allocation_policy,
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
            .map(Box::new)
            .map(BuiltInStreamingClusterTrainer::DirectionalPca)
            .map_err(map_clustering_error)
        }
        BuiltInPlanningPhase::SphericalKmeans(settings) => {
            let cluster_count = effective_cluster_count(
                settings.cluster_count,
                partition_len,
                materializability_bound,
            )
            .map_err(map_clustering_configuration_error)?;
            SphericalKmeansStreamingTrainer::new(
                StreamingClusteringConfig {
                    cluster_count,
                    dimensions,
                    balance_constraints: None,
                    random_seed: settings.random_seed,
                },
                settings.params.clone(),
            )
            .map(BuiltInStreamingClusterTrainer::SphericalKmeans)
            .map_err(map_clustering_error)
        }
    }
}

fn derive_hierarchy_for_adaptive_built_in(
    settings: &AdaptivePlanningSettings,
    embeddings: &[Vec<f32>],
    embedding_spec: &EmbeddingSpec,
    materializability_bound: usize,
    stage_observer: &mut impl FnMut(HierarchyPlanningStatusEvent),
) -> Result<(PlanningPassOutcome, Vec<AdaptiveSwitchDecisionRecord>), StreamingIndexerError> {
    let mut selector =
        AdaptivePlanningSelector::new(settings.clone()).map_err(map_adaptive_planning_error)?;
    let outcome = match settings.direction {
        AdaptivePlanningDirection::Divisive => derive_hierarchy_with_builder(
            embeddings,
            materializability_bound,
            stage_observer,
            |partition_embeddings| {
                let algorithm = select_adaptive_algorithm_for_boundary(
                    &mut selector,
                    partition_embeddings.len(),
                    partition_embeddings,
                )?;
                let phase = adaptive_phase(settings, algorithm);
                Ok::<PartitionPlanner<BuiltInStreamingClusterTrainer>, StreamingIndexerError>(
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
                )
            },
        ),
        AdaptivePlanningDirection::Agglomerative => derive_hierarchy_agglomeratively_with_builder(
            embeddings,
            materializability_bound,
            stage_observer,
            |layer_embeddings, represented_item_count, _max_unit_item_count| {
                let algorithm = select_adaptive_algorithm_for_boundary(
                    &mut selector,
                    represented_item_count,
                    layer_embeddings,
                )?;
                let phase = adaptive_phase(settings, algorithm);
                Ok::<PartitionPlanner<BuiltInStreamingClusterTrainer>, StreamingIndexerError>(
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
                )
            },
        ),
    }?;
    Ok((outcome, selector.decision_records().to_vec()))
}

fn select_adaptive_algorithm_for_boundary(
    selector: &mut AdaptivePlanningSelector,
    represented_item_count: usize,
    embeddings: &[Vec<f32>],
) -> Result<ActivePlanningAlgorithm, StreamingIndexerError> {
    let replay_pass_limit = embeddings.len().saturating_add(4).max(1);
    let mut progress = selector
        .begin_selection_boundary(
            represented_item_count,
            embeddings.len(),
            embeddings.first().map_or(0, std::vec::Vec::len),
        )
        .map_err(map_adaptive_planning_error)?;
    let mut replay_passes = 0usize;
    loop {
        match progress {
            lexongraph_adaptive_planning_policy::AdaptiveSelectionProgress::Selected(algorithm) => {
                return Ok(algorithm);
            }
            lexongraph_adaptive_planning_policy::AdaptiveSelectionProgress::ReplayRequired(_) => {
                replay_passes += 1;
                if replay_passes > replay_pass_limit {
                    return Err(StreamingIndexerError::ClusteringFailure(format!(
                        "adaptive selection exceeded the maximum replay pass count of {replay_pass_limit}"
                    )));
                }
                selector
                    .ingest_selection_batch(embeddings)
                    .map_err(map_adaptive_planning_error)?;
                progress = selector
                    .finish_selection_pass()
                    .map_err(map_adaptive_planning_error)?;
            }
        }
    }
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

fn derive_hierarchy_from_published_profile(
    profile: &PublishedIndexingProfile,
    embeddings: &[Vec<f32>],
    embedding_spec: &EmbeddingSpec,
    materializability_bound: usize,
    stage_observer: &mut impl FnMut(HierarchyPlanningStatusEvent),
) -> Result<PlanningPassOutcome, StreamingIndexerError> {
    let effective_partition_bound =
        published_profile_partition_bound(profile, materializability_bound)?;
    match &profile.planning_strategy {
        PublishedPlanningStrategy::SphericalKmeansGreedyPack(settings) => {
            derive_hierarchy_from_published_spherical_kmeans_profile(
                settings,
                embeddings,
                effective_partition_bound,
                stage_observer,
            )
        }
        PublishedPlanningStrategy::DirectionalPcaDivisive(settings) => {
            derive_hierarchy_for_single_built_in_phase_with_fallback_group_cap(
                BuiltInPlanningPhase::DirectionalPca(DirectionalPcaBuiltInPlanningSettings {
                    direction: BuiltInPlanningDirection::Divisive,
                    cluster_count: settings.cluster_count,
                    random_seed: settings.random_seed,
                    params: settings.params.clone(),
                }),
                embeddings,
                embedding_spec,
                effective_partition_bound,
                published_profile_fallback_group_cap(profile, effective_partition_bound),
                stage_observer,
            )
        }
    }
}

fn published_profile_partition_bound(
    profile: &PublishedIndexingProfile,
    materializability_bound: usize,
) -> Result<usize, StreamingIndexerError> {
    let PublishedProfileVersion { major, minor, .. } = profile.version;
    if !matches!((major, minor), (0, 6)) {
        return Ok(materializability_bound);
    }

    let PublishedPlanningStrategy::DirectionalPcaDivisive(settings) = &profile.planning_strategy
    else {
        return Ok(materializability_bound);
    };
    let cluster_count = usize::try_from(settings.cluster_count).map_err(|_| {
        map_clustering_configuration_error("published profile cluster_count exceeds usize".into())
    })?;
    Ok(materializability_bound.min(cluster_count))
}

fn published_profile_fallback_group_cap(
    profile: &PublishedIndexingProfile,
    effective_partition_bound: usize,
) -> Option<usize> {
    let PublishedProfileVersion { major, minor, .. } = profile.version;
    matches!((major, minor), (0, 6)).then_some(effective_partition_bound)
}

fn derive_hierarchy_from_published_spherical_kmeans_profile(
    settings: &PublishedSphericalKmeansProfileSettings,
    embeddings: &[Vec<f32>],
    materializability_bound: usize,
    stage_observer: &mut impl FnMut(HierarchyPlanningStatusEvent),
) -> Result<PlanningPassOutcome, StreamingIndexerError> {
    if embeddings.is_empty() {
        return Ok(PlanningPassOutcome {
            hierarchy: FinalizedPartitionHierarchy {
                root_partition_id: "p0".into(),
                partitions: Vec::new(),
            },
            requested_cluster_count: None,
            realized_cluster_count: None,
            planning_quality_metric: 0.0,
            planning_balance_metric: 0.0,
            planning_quality_direction: MetricDirection::LargerIsBetter,
            planning_balance_direction: MetricDirection::SmallerIsBetter,
            stages_used: BTreeSet::new(),
        });
    }

    if embeddings.len() <= materializability_bound {
        return Ok(PlanningPassOutcome {
            hierarchy: FinalizedPartitionHierarchy {
                root_partition_id: "p0".into(),
                partitions: vec![FinalizedPartition {
                    id: "p0".into(),
                    parent_id: None,
                    child_ids: Vec::new(),
                    item_indices: (0..embeddings.len()).collect(),
                    terminal: true,
                    planning_stage: PlanningStage::Single,
                }],
            },
            requested_cluster_count: Some(1),
            realized_cluster_count: Some(1),
            planning_quality_metric: 0.0,
            planning_balance_metric: 0.0,
            planning_quality_direction: MetricDirection::LargerIsBetter,
            planning_balance_direction: MetricDirection::SmallerIsBetter,
            stages_used: BTreeSet::new(),
        });
    }

    stage_observer(HierarchyPlanningStatusEvent::legacy(
        PlanningStage::Fine,
        embeddings.len(),
        StreamingIndexingStatusState::Started,
    ));
    stage_observer(HierarchyPlanningStatusEvent::legacy(
        PlanningStage::Fine,
        embeddings.len(),
        StreamingIndexingStatusState::InProgress,
    ));

    let requested_cluster_count = effective_cluster_count(
        settings.cluster_count,
        embeddings.len(),
        materializability_bound,
    )
    .map_err(map_clustering_configuration_error)?;
    let mut trainer = SphericalKmeansStreamingTrainer::new(
        StreamingClusteringConfig {
            cluster_count: requested_cluster_count,
            dimensions: embeddings.first().map_or(0, std::vec::Vec::len),
            balance_constraints: None,
            random_seed: settings.random_seed,
        },
        settings.params.clone(),
    )
    .map_err(map_clustering_error)?;
    trainer
        .ingest_batch(embeddings)
        .map_err(map_clustering_error)?;
    let pass_report = trainer.finish_pass().map_err(map_clustering_error)?;
    trainer.complete_training().map_err(map_clustering_error)?;
    let classifier = trainer.into_classifier().map_err(map_clustering_error)?;
    let assignments = classifier
        .assign_batch(embeddings)
        .map_err(map_clustering_error)?;

    let terminal_groups = build_profile_terminal_groups(&assignments, materializability_bound)
        .map_err(StreamingIndexerError::HierarchyValidation)?;

    let mut nodes = terminal_groups
        .into_iter()
        .map(|item_indices| {
            profile_terminal_node(&item_indices, embeddings).map(|representative_embedding| {
                AgglomerativeHierarchyNode {
                    child_node_indices: Vec::new(),
                    item_indices,
                    representative_embedding: Some(representative_embedding),
                    planning_stage: PlanningStage::Fine,
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(StreamingIndexerError::HierarchyValidation)?;

    let mut accumulator = PlanningMetricAccumulator::default();
    accumulator.observe(PlanningStage::Fine, &pass_report);
    let mut current_layer = (0..nodes.len()).collect::<Vec<_>>();

    if current_layer.len() > 1 {
        stage_observer(HierarchyPlanningStatusEvent::legacy(
            PlanningStage::Coarse,
            current_layer.len(),
            StreamingIndexingStatusState::Started,
        ));
        stage_observer(HierarchyPlanningStatusEvent::legacy(
            PlanningStage::Coarse,
            current_layer.len(),
            StreamingIndexingStatusState::InProgress,
        ));
    }

    while current_layer.len() > 1 {
        let groups = greedy_pack_node_groups(
            &current_layer,
            &nodes,
            materializability_bound,
            settings.hierarchy_metric,
        )
        .map_err(StreamingIndexerError::HierarchyValidation)?;
        let mut next_layer = Vec::with_capacity(groups.len());
        for group in groups {
            let mut item_indices = group
                .iter()
                .flat_map(|&node_index| nodes[node_index].item_indices.iter().copied())
                .collect::<Vec<_>>();
            item_indices.sort_unstable();
            let weighted_embeddings = group
                .iter()
                .map(|&node_index| {
                    let embedding = nodes[node_index]
                        .representative_embedding
                        .as_ref()
                        .ok_or_else(|| {
                            "published profile hierarchy node is missing its representative embedding"
                                .to_string()
                        })?;
                    Ok::<(&[f32], usize), String>((
                        embedding.as_slice(),
                        nodes[node_index].item_indices.len(),
                    ))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(StreamingIndexerError::HierarchyValidation)?;
            let representative_embedding = weighted_mean_f32_embeddings(weighted_embeddings)
                .map_err(|error| StreamingIndexerError::HierarchyValidation(error.to_string()))?;
            let next_index = nodes.len();
            nodes.push(AgglomerativeHierarchyNode {
                child_node_indices: group,
                item_indices,
                representative_embedding: Some(representative_embedding),
                planning_stage: PlanningStage::Coarse,
            });
            next_layer.push(next_index);
        }
        current_layer = next_layer;
    }

    let mut partitions = Vec::new();
    build_agglomerative_partitions(&nodes, current_layer[0], "p0".into(), None, &mut partitions);
    partitions.sort_by(|left, right| left.id.cmp(&right.id));
    accumulator.stages_used.insert(PlanningStage::Fine);
    if nodes.len() > 1 {
        accumulator.stages_used.insert(PlanningStage::Coarse);
    }

    Ok(PlanningPassOutcome {
        hierarchy: FinalizedPartitionHierarchy {
            root_partition_id: "p0".into(),
            partitions,
        },
        requested_cluster_count: Some(requested_cluster_count),
        realized_cluster_count: pass_report.realized_cluster_count,
        planning_quality_metric: accumulator.average_quality(),
        planning_balance_metric: accumulator.average_balance(),
        planning_quality_direction: accumulator.quality_direction,
        planning_balance_direction: accumulator.balance_direction,
        stages_used: accumulator.stages_used,
    })
}

fn derive_hierarchy_from_factory<F>(
    factory: &F,
    embeddings: &[Vec<f32>],
    embedding_spec: &EmbeddingSpec,
    materializability_bound: usize,
    block_size_target: usize,
    stage_observer: &mut impl FnMut(HierarchyPlanningStatusEvent),
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
            Ok(PartitionPlanner::new(PlanningStage::Custom, trainer))
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
    fn run<OP>(
        self,
        embeddings: &[Vec<f32>],
        observe_progress: &mut OP,
    ) -> Result<(PassReport, Vec<ClusterId>), StreamingClusteringError>
    where
        OP: FnMut();
}

impl<T> PartitionPlannerRunner for PartitionPlanner<T>
where
    T: StreamingClusterTrainer,
{
    fn stage(&self) -> PlanningStage {
        self.stage
    }

    fn run<OP>(
        mut self,
        embeddings: &[Vec<f32>],
        observe_progress: &mut OP,
    ) -> Result<(PassReport, Vec<ClusterId>), StreamingClusteringError>
    where
        OP: FnMut(),
    {
        let replay_pass_limit = embeddings.len().saturating_add(4).max(1);
        let mut replay_passes = 0usize;
        let pass_report = loop {
            replay_passes += 1;
            if replay_passes > replay_pass_limit {
                return Err(StreamingClusteringError::MalformedInput {
                    message: format!(
                        "planner exceeded the maximum replay pass count of {replay_pass_limit}"
                    ),
                });
            }
            self.trainer.ingest_batch(embeddings)?;
            observe_progress();
            let pass_report = self.trainer.finish_pass()?;
            observe_progress();
            if pass_report.readiness == PassReadiness::PartitionReady {
                match self.trainer.complete_training() {
                    Ok(()) => {
                        observe_progress();
                        break pass_report;
                    }
                    Err(StreamingClusteringError::InvalidTransition {
                        state: TrainerState::PassComplete,
                        operation,
                    }) if operation == "complete_training" => continue,
                    Err(error) => return Err(error),
                }
            }
        };
        let classifier = self.trainer.into_classifier()?;
        let assignments = classifier.assign_batch(embeddings)?;
        observe_progress();
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

fn derive_hierarchy_with_builder<E, B, P, SO>(
    embeddings: &[Vec<f32>],
    materializability_bound: usize,
    stage_observer: &mut SO,
    planner_builder: B,
) -> Result<PlanningPassOutcome, E>
where
    E: From<StreamingClusteringError>,
    B: FnMut(&[Vec<f32>]) -> Result<P, E>,
    P: PartitionPlannerRunner,
    SO: FnMut(HierarchyPlanningStatusEvent),
{
    derive_hierarchy_with_builder_and_fallback_group_cap(
        embeddings,
        materializability_bound,
        None,
        stage_observer,
        planner_builder,
    )
}

fn derive_hierarchy_with_builder_and_fallback_group_cap<E, B, P, SO>(
    embeddings: &[Vec<f32>],
    materializability_bound: usize,
    fallback_group_cap: Option<usize>,
    stage_observer: &mut SO,
    mut planner_builder: B,
) -> Result<PlanningPassOutcome, E>
where
    E: From<StreamingClusteringError>,
    B: FnMut(&[Vec<f32>]) -> Result<P, E>,
    P: PartitionPlannerRunner,
    SO: FnMut(HierarchyPlanningStatusEvent),
{
    if embeddings.is_empty() {
        return Ok(PlanningPassOutcome {
            hierarchy: FinalizedPartitionHierarchy {
                root_partition_id: "p0".into(),
                partitions: Vec::new(),
            },
            requested_cluster_count: None,
            realized_cluster_count: None,
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
    let mut telemetry = RecursivePlanningTelemetry::default();
    derive_partition_recursive(
        &root_indices,
        "p0".into(),
        None,
        None,
        embeddings,
        materializability_bound,
        fallback_group_cap,
        stage_observer,
        &mut telemetry,
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
        requested_cluster_count: accumulator.requested_cluster_count,
        realized_cluster_count: accumulator.realized_cluster_count,
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
    B: FnMut(&[Vec<f32>], usize, usize) -> Result<P, E>,
    P: PartitionPlannerRunner,
    SO: FnMut(HierarchyPlanningStatusEvent),
{
    if embeddings.is_empty() {
        return Ok(PlanningPassOutcome {
            hierarchy: FinalizedPartitionHierarchy {
                root_partition_id: "p0".into(),
                partitions: Vec::new(),
            },
            requested_cluster_count: None,
            realized_cluster_count: None,
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
        let planner = planner_builder(
            &layer_embeddings,
            represented_item_count,
            max_unit_item_count,
        )?;
        let stage = planner.stage();
        stage_observer(HierarchyPlanningStatusEvent::legacy(
            stage,
            represented_item_count,
            StreamingIndexingStatusState::Started,
        ));
        stage_observer(HierarchyPlanningStatusEvent::legacy(
            stage,
            represented_item_count,
            StreamingIndexingStatusState::InProgress,
        ));
        let (pass_report, assignments) = planner
            .run(&layer_embeddings, &mut || {})
            .map_err(E::from)?;
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
        requested_cluster_count: accumulator.requested_cluster_count,
        realized_cluster_count: accumulator.realized_cluster_count,
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
    stage_hint: Option<PlanningStage>,
    embeddings: &[Vec<f32>],
    materializability_bound: usize,
    fallback_group_cap: Option<usize>,
    stage_observer: &mut SO,
    telemetry: &mut RecursivePlanningTelemetry,
    planner_builder: &mut B,
    accumulator: &mut PlanningMetricAccumulator,
    partitions: &mut Vec<FinalizedPartition>,
) -> Result<(), E>
where
    E: From<StreamingClusteringError>,
    B: FnMut(&[Vec<f32>]) -> Result<P, E>,
    P: PartitionPlannerRunner,
    SO: FnMut(HierarchyPlanningStatusEvent),
{
    telemetry.started_subproblem_count += 1;
    telemetry.visited_partition_count += 1;
    let terminal = indices.len() <= materializability_bound || indices.len() <= 1;
    if terminal {
        telemetry.finalized_partition_count += 1;
        telemetry.terminal_partition_count += 1;
        telemetry.completed_subproblem_count += 1;
        partitions.push(FinalizedPartition {
            id: partition_id,
            parent_id,
            child_ids: Vec::new(),
            item_indices: indices.to_vec(),
            terminal: true,
            planning_stage: stage_hint.unwrap_or(PlanningStage::Single),
        });
        return Ok(());
    }

    let partition_embeddings = indices
        .iter()
        .map(|&index| embeddings[index].clone())
        .collect::<Vec<_>>();
    let planner = planner_builder(&partition_embeddings)?;
    let stage = planner.stage();
    telemetry.started_planner_invocation_count += 1;
    stage_observer(telemetry.unit_event(
        stage,
        indices.len(),
        &partition_id,
        StreamingIndexingStatusState::Started,
    ));
    stage_observer(telemetry.unit_event(
        stage,
        indices.len(),
        &partition_id,
        StreamingIndexingStatusState::InProgress,
    ));
    let (pass_report, assignments) = planner
        .run(&partition_embeddings, &mut || {
            stage_observer(telemetry.unit_event(
                stage,
                indices.len(),
                &partition_id,
                StreamingIndexingStatusState::InProgress,
            ));
        })
        .map_err(|error| {
            telemetry.fail_unit(stage, indices.len(), &partition_id, stage_observer);
            E::from(error)
        })?;
    if assignments.len() != partition_embeddings.len() {
        telemetry.fail_unit(stage, indices.len(), &partition_id, stage_observer);
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
        telemetry.fallback_count += 1;
        stage_observer(telemetry.unit_event(
            stage,
            indices.len(),
            &partition_id,
            StreamingIndexingStatusState::InProgress,
        ));
        groups =
            fallback_partition_groups(indices.len(), materializability_bound, fallback_group_cap)
                .map_err(|error| {
                telemetry.fail_unit(stage, indices.len(), &partition_id, stage_observer);
                E::from(invalid_config(error))
            })?;
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
    telemetry.finalized_partition_count += 1;
    telemetry.completed_subproblem_count += 1;
    telemetry.completed_planner_invocation_count += 1;
    stage_observer(telemetry.unit_event(
        stage,
        indices.len(),
        &partition_id,
        StreamingIndexingStatusState::Completed,
    ));

    for (child_index, group) in groups.into_iter().enumerate() {
        let child_indices = group
            .into_iter()
            .map(|local_index| indices[local_index])
            .collect::<Vec<_>>();
        derive_partition_recursive(
            &child_indices,
            child_ids[child_index].clone(),
            Some(partition_id.clone()),
            Some(stage),
            embeddings,
            materializability_bound,
            fallback_group_cap,
            stage_observer,
            telemetry,
            planner_builder,
            accumulator,
            partitions,
        )?;
    }

    Ok(())
}

#[derive(Default)]
struct RecursivePlanningTelemetry {
    started_subproblem_count: usize,
    completed_subproblem_count: usize,
    started_planner_invocation_count: usize,
    visited_partition_count: usize,
    finalized_partition_count: usize,
    terminal_partition_count: usize,
    completed_planner_invocation_count: usize,
    fallback_count: usize,
}

impl RecursivePlanningTelemetry {
    fn fail_unit(
        &mut self,
        stage: PlanningStage,
        legacy_item_count: usize,
        partition_path: &str,
        stage_observer: &mut impl FnMut(HierarchyPlanningStatusEvent),
    ) {
        self.completed_subproblem_count += 1;
        self.completed_planner_invocation_count += 1;
        stage_observer(self.unit_event(
            stage,
            legacy_item_count,
            partition_path,
            StreamingIndexingStatusState::Failed,
        ));
    }

    fn unit_event(
        &self,
        stage: PlanningStage,
        legacy_item_count: usize,
        partition_path: &str,
        state: StreamingIndexingStatusState,
    ) -> HierarchyPlanningStatusEvent {
        HierarchyPlanningStatusEvent {
            stage,
            state,
            legacy_item_count,
            progress_unit_kind: Some(
                StreamingIndexingProgressUnitKind::PartitionPlanningInvocation,
            ),
            completed_unit_count: Some(self.completed_planner_invocation_count),
            discovered_unit_count: Some(self.started_planner_invocation_count),
            current_partition_path: Some(partition_path.to_owned()),
            current_partition_size: Some(legacy_item_count),
            current_recursion_depth: Some(partition_depth(partition_path)),
            started_subproblem_count: Some(self.started_subproblem_count),
            completed_subproblem_count: Some(self.completed_subproblem_count),
            visited_partition_count: Some(self.visited_partition_count),
            finalized_partition_count: Some(self.finalized_partition_count),
            terminal_partition_count: Some(self.terminal_partition_count),
            completed_planner_invocation_count: Some(self.completed_planner_invocation_count),
            fallback_count: Some(self.fallback_count),
        }
    }
}

fn partition_depth(partition_path: &str) -> usize {
    partition_path.split('.').count().saturating_sub(1)
}

struct PlanningMetricAccumulator {
    quality_sum: f64,
    balance_sum: f64,
    cluster_runs: usize,
    requested_cluster_count: Option<u32>,
    realized_cluster_count: Option<u32>,
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
            requested_cluster_count: None,
            realized_cluster_count: None,
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
            self.requested_cluster_count = Some(report.requested_cluster_count);
            self.realized_cluster_count = report.realized_cluster_count;
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
    stage_states: BTreeMap<PlanningStage, PlanningStageProgressState>,
    active_snapshot: Arc<Mutex<Option<HierarchyPlanningHeartbeatSnapshot>>>,
    active_snapshot_generation: Arc<AtomicUsize>,
    heartbeat: StatusHeartbeatGuard,
}

impl<'a> PlanningStageStatusTracker<'a> {
    fn new(observer: &'a Option<StreamingIndexingStatusObserver>, pass_started: Instant) -> Self {
        let active_snapshot = Arc::new(Mutex::new(None));
        let active_snapshot_generation = Arc::new(AtomicUsize::new(0));
        Self {
            observer,
            pass_started,
            stage_states: BTreeMap::new(),
            heartbeat: StatusHeartbeatGuard::new(start_hierarchy_status_heartbeat(
                observer,
                Arc::clone(&active_snapshot),
                Arc::clone(&active_snapshot_generation),
                pass_started,
            )),
            active_snapshot,
            active_snapshot_generation,
        }
    }

    fn observe(&mut self, event: HierarchyPlanningStatusEvent) {
        if event.state != StreamingIndexingStatusState::Started {
            self.ensure_started(&event);
        }
        let (
            completed_unit_count,
            progress_unit_kind,
            discovered_unit_count,
            started_subproblem_count,
            completed_subproblem_count,
            visited_partition_count,
            finalized_partition_count,
            terminal_partition_count,
            completed_planner_invocation_count,
            fallback_count,
        ) = {
            let stage_state = self
                .stage_states
                .entry(event.stage)
                .or_insert_with(|| PlanningStageProgressState::new(event.legacy_item_count));
            stage_state.observe(&event);
            (
                stage_state.completed_unit_count,
                stage_state.progress_unit_kind,
                stage_state.discovered_unit_count,
                stage_state.started_subproblem_count,
                stage_state.completed_subproblem_count,
                stage_state.visited_partition_count,
                stage_state.finalized_partition_count,
                stage_state.terminal_partition_count,
                stage_state.completed_planner_invocation_count,
                stage_state.fallback_count,
            )
        };
        let unit_started = self.update_current_unit_started(&event);
        let last_progress_at = self.pass_started.elapsed();
        let status = status_with_hierarchy_details(
            StreamingIndexingPhase::HierarchyPlanning { stage: event.stage },
            event.state,
            None,
            completed_unit_count,
            last_progress_at,
            None,
            HierarchyPlanningDetailFields {
                legacy_item_count: Some(event.legacy_item_count),
                progress_unit_kind,
                discovered_unit_count,
                current_unit_elapsed: hierarchy_event_current_unit_elapsed(
                    event.state,
                    unit_started,
                ),
                current_partition_path: event.current_partition_path.clone(),
                current_partition_size: event.current_partition_size,
                current_recursion_depth: event.current_recursion_depth,
                started_subproblem_count,
                completed_subproblem_count,
                visited_partition_count,
                finalized_partition_count,
                terminal_partition_count,
                completed_planner_invocation_count,
                fallback_count,
                last_progress_at: Some(last_progress_at),
            },
        );
        match event.state {
            StreamingIndexingStatusState::Started | StreamingIndexingStatusState::InProgress => {
                emit_status(self.observer, status.clone());
                self.replace_active_snapshot(HierarchyPlanningHeartbeatSnapshot {
                    snapshot_generation: 0,
                    status,
                    current_unit_started: unit_started,
                });
            }
            StreamingIndexingStatusState::Completed | StreamingIndexingStatusState::Failed => {
                self.clear_active_snapshot();
                emit_status(self.observer, status);
            }
        }
    }

    fn complete_all(&mut self, elapsed: Duration) {
        self.clear_active_snapshot();
        self.heartbeat.stop();
        for (stage, state) in &self.stage_states {
            emit_status(
                self.observer,
                status_with_hierarchy_details(
                    StreamingIndexingPhase::HierarchyPlanning { stage: *stage },
                    StreamingIndexingStatusState::Completed,
                    None,
                    state.completed_unit_count,
                    elapsed,
                    None,
                    HierarchyPlanningDetailFields {
                        legacy_item_count: None,
                        progress_unit_kind: state.progress_unit_kind,
                        discovered_unit_count: state.discovered_unit_count,
                        current_unit_elapsed: None,
                        current_partition_path: None,
                        current_partition_size: None,
                        current_recursion_depth: None,
                        started_subproblem_count: state.started_subproblem_count,
                        completed_subproblem_count: state.completed_subproblem_count,
                        visited_partition_count: state.visited_partition_count,
                        finalized_partition_count: state.finalized_partition_count,
                        terminal_partition_count: state.terminal_partition_count,
                        completed_planner_invocation_count: state
                            .completed_planner_invocation_count,
                        fallback_count: state.fallback_count,
                        last_progress_at: Some(elapsed),
                    },
                ),
            );
        }
    }

    fn fail_all(&mut self, elapsed: Duration, error: &str) {
        self.clear_active_snapshot();
        self.heartbeat.stop();
        for (stage, state) in &self.stage_states {
            emit_status(
                self.observer,
                status_with_hierarchy_details(
                    StreamingIndexingPhase::HierarchyPlanning { stage: *stage },
                    StreamingIndexingStatusState::Failed,
                    None,
                    state.completed_unit_count,
                    elapsed,
                    Some(error.to_owned()),
                    HierarchyPlanningDetailFields {
                        legacy_item_count: None,
                        progress_unit_kind: state.progress_unit_kind,
                        discovered_unit_count: state.discovered_unit_count,
                        current_unit_elapsed: None,
                        current_partition_path: None,
                        current_partition_size: None,
                        current_recursion_depth: None,
                        started_subproblem_count: state.started_subproblem_count,
                        completed_subproblem_count: state.completed_subproblem_count,
                        visited_partition_count: state.visited_partition_count,
                        finalized_partition_count: state.finalized_partition_count,
                        terminal_partition_count: state.terminal_partition_count,
                        completed_planner_invocation_count: state
                            .completed_planner_invocation_count,
                        fallback_count: state.fallback_count,
                        last_progress_at: Some(elapsed),
                    },
                ),
            );
        }
    }

    fn ensure_started(&mut self, event: &HierarchyPlanningStatusEvent) {
        if self.stage_states.contains_key(&event.stage) {
            return;
        }
        self.stage_states.insert(
            event.stage,
            PlanningStageProgressState::new(event.legacy_item_count),
        );
        emit_status(
            self.observer,
            status_with_hierarchy_details(
                StreamingIndexingPhase::HierarchyPlanning { stage: event.stage },
                StreamingIndexingStatusState::Started,
                None,
                0,
                Duration::ZERO,
                None,
                HierarchyPlanningDetailFields {
                    legacy_item_count: Some(event.legacy_item_count),
                    progress_unit_kind: event.progress_unit_kind,
                    discovered_unit_count: event.discovered_unit_count,
                    current_unit_elapsed: hierarchy_event_has_unit_descriptor(event)
                        .then_some(Duration::ZERO),
                    current_partition_path: event.current_partition_path.clone(),
                    current_partition_size: event.current_partition_size,
                    current_recursion_depth: event.current_recursion_depth,
                    started_subproblem_count: event.started_subproblem_count,
                    completed_subproblem_count: event.completed_subproblem_count,
                    visited_partition_count: event.visited_partition_count,
                    finalized_partition_count: event.finalized_partition_count,
                    terminal_partition_count: event.terminal_partition_count,
                    completed_planner_invocation_count: event.completed_planner_invocation_count,
                    fallback_count: event.fallback_count,
                    last_progress_at: Some(Duration::ZERO),
                },
            ),
        );
    }

    fn replace_active_snapshot(&self, snapshot: HierarchyPlanningHeartbeatSnapshot) {
        let snapshot_generation = self
            .active_snapshot_generation
            .fetch_add(1, AtomicOrdering::SeqCst)
            + 1;
        if let Ok(mut active) = self.active_snapshot.lock() {
            *active = Some(HierarchyPlanningHeartbeatSnapshot {
                snapshot_generation,
                ..snapshot
            });
        }
    }

    fn clear_active_snapshot(&self) {
        self.active_snapshot_generation
            .fetch_add(1, AtomicOrdering::SeqCst);
        if let Ok(mut active) = self.active_snapshot.lock() {
            *active = None;
        }
    }

    fn update_current_unit_started(&self, event: &HierarchyPlanningStatusEvent) -> Option<Instant> {
        if !hierarchy_event_has_unit_descriptor(event) {
            return None;
        }
        let now = Instant::now();
        let active = self.active_snapshot.lock().ok()?;
        match active.as_ref() {
            Some(existing)
                if existing.status.phase
                    == (StreamingIndexingPhase::HierarchyPlanning { stage: event.stage })
                    && existing.status.current_partition_path == event.current_partition_path
                    && existing.status.current_partition_size == event.current_partition_size
                    && existing.status.current_recursion_depth == event.current_recursion_depth =>
            {
                existing.current_unit_started
            }
            _ => Some(now),
        }
    }
}

fn hierarchy_event_has_unit_descriptor(event: &HierarchyPlanningStatusEvent) -> bool {
    event.current_partition_path.is_some()
        || event.current_partition_size.is_some()
        || event.current_recursion_depth.is_some()
}

fn hierarchy_event_current_unit_elapsed(
    state: StreamingIndexingStatusState,
    unit_started: Option<Instant>,
) -> Option<Duration> {
    match (state, unit_started) {
        (StreamingIndexingStatusState::Started, Some(_)) => Some(Duration::ZERO),
        (_, Some(started)) => Some(started.elapsed()),
        (_, None) => None,
    }
}

#[derive(Clone)]
struct HierarchyPlanningHeartbeatSnapshot {
    snapshot_generation: usize,
    status: StreamingIndexingStatus,
    current_unit_started: Option<Instant>,
}

struct PlanningStageProgressState {
    legacy_item_count: usize,
    progress_unit_kind: Option<StreamingIndexingProgressUnitKind>,
    completed_unit_count: usize,
    discovered_unit_count: Option<usize>,
    started_subproblem_count: Option<usize>,
    completed_subproblem_count: Option<usize>,
    visited_partition_count: Option<usize>,
    finalized_partition_count: Option<usize>,
    terminal_partition_count: Option<usize>,
    completed_planner_invocation_count: Option<usize>,
    fallback_count: Option<usize>,
}

impl PlanningStageProgressState {
    fn new(legacy_item_count: usize) -> Self {
        Self {
            legacy_item_count,
            progress_unit_kind: None,
            completed_unit_count: 0,
            discovered_unit_count: None,
            started_subproblem_count: None,
            completed_subproblem_count: None,
            visited_partition_count: None,
            finalized_partition_count: None,
            terminal_partition_count: None,
            completed_planner_invocation_count: None,
            fallback_count: None,
        }
    }

    fn observe(&mut self, event: &HierarchyPlanningStatusEvent) {
        self.legacy_item_count = event.legacy_item_count;
        self.progress_unit_kind = event.progress_unit_kind.or(self.progress_unit_kind);
        if let Some(completed_unit_count) = event.completed_unit_count {
            self.completed_unit_count = completed_unit_count;
        } else if event.state == StreamingIndexingStatusState::InProgress {
            self.completed_unit_count += event.legacy_item_count;
        }
        self.discovered_unit_count = event.discovered_unit_count.or(self.discovered_unit_count);
        self.started_subproblem_count = event
            .started_subproblem_count
            .or(self.started_subproblem_count);
        self.completed_subproblem_count = event
            .completed_subproblem_count
            .or(self.completed_subproblem_count);
        self.visited_partition_count = event
            .visited_partition_count
            .or(self.visited_partition_count);
        self.finalized_partition_count = event
            .finalized_partition_count
            .or(self.finalized_partition_count);
        self.terminal_partition_count = event
            .terminal_partition_count
            .or(self.terminal_partition_count);
        self.completed_planner_invocation_count = event
            .completed_planner_invocation_count
            .or(self.completed_planner_invocation_count);
        self.fallback_count = event.fallback_count.or(self.fallback_count);
    }
}

// ─────────────────────────────────────────────────────────────
// Private helpers
// ─────────────────────────────────────────────────────────────

fn remaining_units(total: Option<usize>, completed: usize) -> Option<usize> {
    total.and_then(|total| total.checked_sub(completed))
}

fn status_with_progress(
    phase: StreamingIndexingPhase,
    state: StreamingIndexingStatusState,
    phase_total_unit_count: Option<usize>,
    completed_unit_count: usize,
    elapsed: Duration,
    error: Option<String>,
) -> StreamingIndexingStatus {
    let progress_unit_kind = match &phase {
        StreamingIndexingPhase::PlanningPass { .. } => {
            Some(StreamingIndexingProgressUnitKind::PassItem)
        }
        StreamingIndexingPhase::HierarchyPlanning { .. } => None,
        StreamingIndexingPhase::FinalMaterializationReplay => {
            Some(StreamingIndexingProgressUnitKind::ReplayItem)
        }
        StreamingIndexingPhase::BottomUpAssembly { .. } => {
            Some(StreamingIndexingProgressUnitKind::AssemblyGroup)
        }
    };
    StreamingIndexingStatus {
        phase,
        state,
        item_count: phase_total_unit_count.unwrap_or(completed_unit_count),
        phase_total_unit_count,
        completed_unit_count,
        remaining_unit_count: remaining_units(phase_total_unit_count, completed_unit_count),
        progress_unit_kind,
        discovered_unit_count: None,
        current_unit_elapsed: None,
        current_partition_path: None,
        current_partition_size: None,
        current_recursion_depth: None,
        started_subproblem_count: None,
        completed_subproblem_count: None,
        visited_partition_count: None,
        finalized_partition_count: None,
        terminal_partition_count: None,
        completed_planner_invocation_count: None,
        fallback_count: None,
        pending_partition_count: None,
        v2_pending_partitions: None,
        v2_completed_pass_summary: None,
        suspected_stall: None,
        elapsed,
        last_progress_at: None,
        error,
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

struct HierarchyPlanningDetailFields {
    legacy_item_count: Option<usize>,
    progress_unit_kind: Option<StreamingIndexingProgressUnitKind>,
    discovered_unit_count: Option<usize>,
    current_unit_elapsed: Option<Duration>,
    current_partition_path: Option<String>,
    current_partition_size: Option<usize>,
    current_recursion_depth: Option<usize>,
    started_subproblem_count: Option<usize>,
    completed_subproblem_count: Option<usize>,
    visited_partition_count: Option<usize>,
    finalized_partition_count: Option<usize>,
    terminal_partition_count: Option<usize>,
    completed_planner_invocation_count: Option<usize>,
    fallback_count: Option<usize>,
    last_progress_at: Option<Duration>,
}

fn status_with_hierarchy_details(
    phase: StreamingIndexingPhase,
    state: StreamingIndexingStatusState,
    phase_total_unit_count: Option<usize>,
    completed_unit_count: usize,
    elapsed: Duration,
    error: Option<String>,
    detail: HierarchyPlanningDetailFields,
) -> StreamingIndexingStatus {
    let legacy_item_count = detail.legacy_item_count;
    let mut status = status_with_progress(
        phase,
        state,
        phase_total_unit_count,
        completed_unit_count,
        elapsed,
        error,
    );
    status.progress_unit_kind = detail.progress_unit_kind;
    status.discovered_unit_count = detail.discovered_unit_count;
    status.current_unit_elapsed = detail.current_unit_elapsed;
    status.current_partition_path = detail.current_partition_path;
    status.current_partition_size = detail.current_partition_size;
    status.current_recursion_depth = detail.current_recursion_depth;
    status.started_subproblem_count = detail.started_subproblem_count;
    status.completed_subproblem_count = detail.completed_subproblem_count;
    status.visited_partition_count = detail.visited_partition_count;
    status.finalized_partition_count = detail.finalized_partition_count;
    status.terminal_partition_count = detail.terminal_partition_count;
    status.completed_planner_invocation_count = detail.completed_planner_invocation_count;
    status.fallback_count = detail.fallback_count;
    status.last_progress_at = detail.last_progress_at;
    if let Some(legacy_item_count) = legacy_item_count {
        status.item_count = legacy_item_count;
    }
    status
}

fn with_legacy_item_count(
    mut status: StreamingIndexingStatus,
    legacy_item_count: usize,
) -> StreamingIndexingStatus {
    status.item_count = legacy_item_count;
    status
}

fn map_v2_trainer_subphase(
    subphase: DirectionalPcaTrainerSubphase,
) -> StreamingIndexingTrainerSubphase {
    match subphase {
        DirectionalPcaTrainerSubphase::AnalyzePca => StreamingIndexingTrainerSubphase::AnalyzePca,
        DirectionalPcaTrainerSubphase::PlanCuts => StreamingIndexingTrainerSubphase::PlanCuts,
        DirectionalPcaTrainerSubphase::CountCells => StreamingIndexingTrainerSubphase::CountCells,
        DirectionalPcaTrainerSubphase::RealizePartition => {
            StreamingIndexingTrainerSubphase::RealizePartition
        }
    }
}

fn apply_v2_pending_partition_detail(
    status: &mut StreamingIndexingStatus,
    pending_partitions: Option<Vec<StreamingV2PendingPartitionStatus>>,
) {
    let Some(pending_partitions) = pending_partitions else {
        return;
    };
    status.pending_partition_count = Some(pending_partitions.len());
    status.v2_pending_partitions = Some(pending_partitions);
    status.suspected_stall = None;
}

#[allow(clippy::too_many_arguments)]
fn build_v2_planning_pass_status(
    pass_number: usize,
    state: StreamingIndexingStatusState,
    phase_total_unit_count: Option<usize>,
    observed_count: usize,
    elapsed: Duration,
    last_progress_at: Option<Duration>,
    error: Option<String>,
    pending_partitions: Option<Vec<StreamingV2PendingPartitionStatus>>,
) -> StreamingIndexingStatus {
    let mut status = match phase_total_unit_count {
        Some(total) => status_with_known_total(
            StreamingIndexingPhase::PlanningPass { pass_number },
            state,
            total,
            observed_count,
            elapsed,
            error,
        ),
        None => status_with_progress(
            StreamingIndexingPhase::PlanningPass { pass_number },
            state,
            None,
            observed_count,
            elapsed,
            error,
        ),
    };
    status.last_progress_at = last_progress_at;
    apply_v2_pending_partition_detail(&mut status, pending_partitions);
    status
}

fn maybe_mark_v2_suspected_stall(
    status: &mut StreamingIndexingStatus,
    heartbeat_covers_current_unit: bool,
) {
    let Some(last_progress_at) = status.last_progress_at else {
        return;
    };
    let Some(duration_without_progress) = status.elapsed.checked_sub(last_progress_at) else {
        return;
    };
    if duration_without_progress.is_zero() || status.pending_partition_count.is_none() {
        return;
    }
    let reason = if matches!(status.phase, StreamingIndexingPhase::PlanningPass { .. }) {
        StreamingIndexingSuspectedStallReason::UnchangedPassObservedCount
    } else if status
        .v2_pending_partitions
        .as_ref()
        .is_some_and(|partitions| {
            partitions
                .iter()
                .any(|partition| partition.routing_bucket_fill_counts.is_some())
        })
    {
        StreamingIndexingSuspectedStallReason::UnchangedRoutingBucketFill
    } else if heartbeat_covers_current_unit
        && status
            .v2_pending_partitions
            .as_ref()
            .is_some_and(|partitions| {
                partitions
                    .iter()
                    .any(|partition| partition.trainer_subphase.is_some())
            })
    {
        StreamingIndexingSuspectedStallReason::UnchangedTrainerSubphase
    } else {
        StreamingIndexingSuspectedStallReason::UnchangedPendingPartitionProgress
    };
    status.suspected_stall = Some(StreamingIndexingSuspectedStall {
        reason,
        duration_without_progress,
    });
}

fn emit_status(
    observer: &Option<StreamingIndexingStatusObserver>,
    status: StreamingIndexingStatus,
) {
    if let Some(obs) = observer {
        let _ = catch_unwind(AssertUnwindSafe(|| obs(status)));
    }
}

fn start_hierarchy_status_heartbeat(
    observer: &Option<StreamingIndexingStatusObserver>,
    active_snapshot: Arc<Mutex<Option<HierarchyPlanningHeartbeatSnapshot>>>,
    active_snapshot_generation: Arc<AtomicUsize>,
    pass_started: Instant,
) -> Option<(mpsc::Sender<()>, thread::JoinHandle<()>)> {
    let observer = observer.as_ref().map(Arc::clone)?;
    let (stop_tx, stop_rx) = mpsc::channel::<()>();
    let handle = thread::spawn(move || {
        while matches!(
            stop_rx.recv_timeout(STATUS_HEARTBEAT_INTERVAL),
            Err(mpsc::RecvTimeoutError::Timeout)
        ) {
            let snapshot = active_snapshot.lock().ok().and_then(|guard| guard.clone());
            let Some(snapshot) = snapshot else {
                continue;
            };
            let _ = catch_unwind(AssertUnwindSafe(|| {
                let mut status = snapshot.status.clone();
                status.state = StreamingIndexingStatusState::InProgress;
                status.elapsed = pass_started.elapsed();
                status.current_unit_elapsed = snapshot
                    .current_unit_started
                    .map(|started| started.elapsed());
                maybe_mark_v2_suspected_stall(&mut status, snapshot.current_unit_started.is_some());
                if active_snapshot_generation.load(AtomicOrdering::SeqCst)
                    != snapshot.snapshot_generation
                {
                    return;
                }
                observer(status);
            }));
        }
    });
    Some((stop_tx, handle))
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

fn start_snapshot_status_heartbeat(
    observer: &Option<StreamingIndexingStatusObserver>,
    status: StreamingIndexingStatus,
    started: Instant,
    current_unit_started: Option<Instant>,
) -> Option<(mpsc::Sender<()>, thread::JoinHandle<()>)> {
    let observer = observer.as_ref().map(Arc::clone)?;
    let (stop_tx, stop_rx) = mpsc::channel::<()>();
    let handle = thread::spawn(move || {
        while matches!(
            stop_rx.recv_timeout(STATUS_HEARTBEAT_INTERVAL),
            Err(mpsc::RecvTimeoutError::Timeout)
        ) {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                let mut heartbeat_status = status.clone();
                heartbeat_status.state = StreamingIndexingStatusState::InProgress;
                heartbeat_status.elapsed = started.elapsed();
                heartbeat_status.current_unit_elapsed =
                    match (current_unit_started, heartbeat_status.current_unit_elapsed) {
                        (Some(unit_started), _) => Some(unit_started.elapsed()),
                        (None, current_unit_elapsed) => current_unit_elapsed,
                    };
                maybe_mark_v2_suspected_stall(
                    &mut heartbeat_status,
                    current_unit_started.is_some(),
                );
                observer(heartbeat_status);
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

fn build_profile_terminal_groups(
    assignments: &[ClusterId],
    materializability_bound: usize,
) -> Result<Vec<Vec<usize>>, String> {
    let mut groups = assignments_to_groups(assignments);
    groups = ensure_min_two_per_group(groups);
    for group in &mut groups {
        group.sort_unstable();
    }
    groups.sort_by_key(|group| group[0]);
    let ordered_indices = groups.into_iter().flatten().collect::<Vec<_>>();
    if ordered_indices.is_empty() {
        return Ok(Vec::new());
    }
    let packed_ranges = balanced_groups(ordered_indices.len(), materializability_bound)?;
    Ok(packed_ranges
        .into_iter()
        .map(|range| {
            let mut packed = range
                .into_iter()
                .map(|ordered_index| ordered_indices[ordered_index])
                .collect::<Vec<_>>();
            packed.sort_unstable();
            packed
        })
        .collect())
}

fn profile_terminal_node(
    item_indices: &[usize],
    embeddings: &[Vec<f32>],
) -> Result<Vec<f32>, String> {
    weighted_mean_f32_embeddings(
        item_indices
            .iter()
            .map(|&item_index| {
                let embedding = embeddings.get(item_index).ok_or_else(|| {
                    format!("terminal partition references missing embedding index {item_index}")
                })?;
                Ok::<(&[f32], usize), String>((embedding.as_slice(), 1))
            })
            .collect::<Result<Vec<_>, _>>()?,
    )
    .map_err(|error| error.to_string())
}

fn greedy_pack_node_groups(
    current_layer: &[usize],
    nodes: &[AgglomerativeHierarchyNode],
    materializability_bound: usize,
    metric: PublishedHierarchyMetric,
) -> Result<Vec<Vec<usize>>, String> {
    let sizes = balanced_groups(current_layer.len(), materializability_bound)?
        .into_iter()
        .map(|group| group.len())
        .collect::<Vec<_>>();
    let mut remaining = current_layer.iter().copied().map(Some).collect::<Vec<_>>();
    remaining.sort_by(|left, right| match (left, right) {
        (Some(left), Some(right)) => compare_profile_node_order(*left, *right, nodes),
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (None, None) => Ordering::Equal,
    });

    let mut groups = Vec::with_capacity(sizes.len());
    for target_size in sizes {
        let Some(seed) = take_first_active_profile_node(&mut remaining) else {
            return Err(
                "published profile hierarchy packing exhausted its remaining nodes prematurely"
                    .into(),
            );
        };
        let seed_embedding = nodes[seed]
            .representative_embedding
            .as_ref()
            .ok_or_else(|| {
                "published profile hierarchy node is missing its representative embedding"
                    .to_string()
            })?;
        let mut group = vec![seed];
        while group.len() < target_size {
            let Some((next_offset, candidate)) =
                best_profile_group_extension(&remaining, nodes, seed_embedding, metric)?
            else {
                return Err(
                    "published profile hierarchy packing could not satisfy its target group sizes"
                        .into(),
                );
            };
            remaining[next_offset] = None;
            group.push(candidate);
        }
        groups.push(group);
    }

    Ok(groups)
}

fn take_first_active_profile_node(remaining: &mut [Option<usize>]) -> Option<usize> {
    let seed_slot = remaining.iter_mut().find(|slot| slot.is_some())?;
    seed_slot.take()
}

fn best_profile_group_extension(
    remaining: &[Option<usize>],
    nodes: &[AgglomerativeHierarchyNode],
    seed_embedding: &[f32],
    metric: PublishedHierarchyMetric,
) -> Result<Option<(usize, usize)>, String> {
    let mut best = None::<(usize, f64, usize)>;
    for (offset, candidate) in remaining.iter().enumerate() {
        let Some(candidate) = candidate else {
            continue;
        };
        let candidate_embedding = nodes[*candidate]
            .representative_embedding
            .as_ref()
            .ok_or_else(|| {
                "published profile hierarchy node is missing its representative embedding"
                    .to_string()
            })?;
        let distance = representative_distance(seed_embedding, candidate_embedding, metric)?;
        match best {
            Some((_, best_distance, best_candidate))
                if distance
                    .partial_cmp(&best_distance)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| {
                        compare_profile_node_order(*candidate, best_candidate, nodes)
                    })
                    != Ordering::Less => {}
            _ => best = Some((offset, distance, *candidate)),
        }
    }
    Ok(best.map(|(offset, _, candidate)| (offset, candidate)))
}

fn compare_profile_node_order(
    left_node_index: usize,
    right_node_index: usize,
    nodes: &[AgglomerativeHierarchyNode],
) -> Ordering {
    let left = &nodes[left_node_index];
    let right = &nodes[right_node_index];
    compare_f32_embeddings_lexicographically(
        left.representative_embedding.as_deref().unwrap_or(&[]),
        right.representative_embedding.as_deref().unwrap_or(&[]),
    )
    .then_with(|| left.item_indices.cmp(&right.item_indices))
}

fn compare_f32_embeddings_lexicographically(left: &[f32], right: &[f32]) -> Ordering {
    left.iter()
        .zip(right.iter())
        .find_map(|(left_value, right_value)| {
            let ordering = left_value.total_cmp(right_value);
            (ordering != Ordering::Equal).then_some(ordering)
        })
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

fn representative_distance(
    left: &[f32],
    right: &[f32],
    metric: PublishedHierarchyMetric,
) -> Result<f64, String> {
    if left.len() != right.len() {
        return Err(format!(
            "representative embedding dimension {} does not match expected {}",
            right.len(),
            left.len()
        ));
    }
    match metric {
        PublishedHierarchyMetric::Euclidean => Ok(left
            .iter()
            .zip(right.iter())
            .map(|(left_value, right_value)| {
                let delta = f64::from(*left_value) - f64::from(*right_value);
                delta * delta
            })
            .sum::<f64>()
            .sqrt()),
    }
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

struct EncodedBranchEntries {
    embedding_spec: EmbeddingSpec,
    entries: Vec<BranchEntry>,
    ext: Option<Metadata>,
}

fn encode_branch_entries(
    policy: BranchEncodingPolicy,
    logical_embedding_spec: &EmbeddingSpec,
    entries: &[BranchEntry],
    parent_level: u64,
    is_root: bool,
) -> Result<EncodedBranchEntries, StreamingIndexerError> {
    match policy {
        BranchEncodingPolicy::Ordinary => Ok(EncodedBranchEntries {
            embedding_spec: logical_embedding_spec.clone(),
            entries: entries.to_vec(),
            ext: None,
        }),
        BranchEncodingPolicy::AmbientDeltaUniform {
            root_bits,
            interior_bits,
            lowest_routing_bits,
        } => {
            if logical_embedding_spec.encoding != "f32le" {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    "ambient delta branch authoring currently requires f32le logical embeddings"
                        .into(),
                ));
            }
            let decoded = entries
                .iter()
                .enumerate()
                .map(|(index, entry)| {
                    decode_f32_embedding_exact(
                        &entry.embedding,
                        logical_embedding_spec,
                        "ambient delta branch authoring",
                    )
                    .map_err(|error| {
                        StreamingIndexerError::TerminalPartitionMaterialization(format!(
                            "failed to decode logical branch entry {index}: {error}"
                        ))
                    })
                })
                .collect::<Result<Vec<Vec<f32>>, StreamingIndexerError>>()?;
            let centroid = exact_f32_centroid(decoded.as_slice())?;
            encode_ambient_delta_quantized_entries(
                entries,
                decoded.as_slice(),
                logical_embedding_spec,
                centroid.as_slice(),
                resolve_branch_bit_budget(
                    parent_level,
                    is_root,
                    root_bits,
                    interior_bits,
                    lowest_routing_bits,
                ),
            )
        }
        BranchEncodingPolicy::PcaRotF32Le
        | BranchEncodingPolicy::PcaRotDeltaF32Le
        | BranchEncodingPolicy::PcaRotDeltaUniform { .. }
        | BranchEncodingPolicy::PcaRotDeltaVariable { .. } => {
            if logical_embedding_spec.encoding != "f32le" {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    "EBCP branch authoring currently requires f32le logical embeddings".into(),
                ));
            }
            let decoded = entries
                .iter()
                .enumerate()
                .map(|(index, entry)| {
                    decode_f32_embedding_exact(
                        &entry.embedding,
                        logical_embedding_spec,
                        "EBCP branch authoring",
                    )
                    .map_err(|error| {
                        StreamingIndexerError::TerminalPartitionMaterialization(format!(
                            "failed to decode logical branch entry {index}: {error}"
                        ))
                    })
                })
                .collect::<Result<Vec<Vec<f32>>, StreamingIndexerError>>()?;
            let (rotation_matrix, explained_variance) = fit_ebcp_rotation(decoded.as_slice())?;
            let encoded = match policy {
                BranchEncodingPolicy::PcaRotF32Le => encode_pca_rot_f32le_entries(
                    entries,
                    decoded.as_slice(),
                    logical_embedding_spec,
                    rotation_matrix.as_slice(),
                ),
                BranchEncodingPolicy::PcaRotDeltaF32Le => {
                    let centroid = exact_f32_centroid(decoded.as_slice())?;
                    encode_pca_rot_delta_f32le_entries(
                        entries,
                        decoded.as_slice(),
                        logical_embedding_spec,
                        rotation_matrix.as_slice(),
                        centroid.as_slice(),
                    )
                }
                BranchEncodingPolicy::PcaRotDeltaUniform {
                    root_bits,
                    interior_bits,
                    lowest_routing_bits,
                } => {
                    let centroid = exact_f32_centroid(decoded.as_slice())?;
                    encode_pca_rot_delta_quantized_entries(
                        entries,
                        decoded.as_slice(),
                        logical_embedding_spec,
                        rotation_matrix.as_slice(),
                        centroid.as_slice(),
                        QuantizedBranchEncoding::Uniform(resolve_branch_bit_budget(
                            parent_level,
                            is_root,
                            root_bits,
                            interior_bits,
                            lowest_routing_bits,
                        )),
                        Some(explained_variance.as_slice()),
                    )
                }
                BranchEncodingPolicy::PcaRotDeltaVariable {
                    root_bits,
                    interior_bits,
                    lowest_routing_bits,
                } => {
                    let centroid = exact_f32_centroid(decoded.as_slice())?;
                    encode_pca_rot_delta_quantized_entries(
                        entries,
                        decoded.as_slice(),
                        logical_embedding_spec,
                        rotation_matrix.as_slice(),
                        centroid.as_slice(),
                        QuantizedBranchEncoding::Variable(resolve_branch_bit_budget(
                            parent_level,
                            is_root,
                            root_bits,
                            interior_bits,
                            lowest_routing_bits,
                        )),
                        Some(explained_variance.as_slice()),
                    )
                }
                BranchEncodingPolicy::AmbientDeltaUniform { .. } => unreachable!(),
                BranchEncodingPolicy::Ordinary => unreachable!(),
            }?;
            Ok(encoded)
        }
    }
}

fn resolve_branch_bit_budget(
    parent_level: u64,
    is_root: bool,
    root_bits: u8,
    interior_bits: u8,
    lowest_routing_bits: u8,
) -> u8 {
    if is_root {
        root_bits
    } else if parent_level == 1 {
        lowest_routing_bits
    } else {
        interior_bits
    }
}

fn uses_root_branch_budget(is_global_root_partition: bool, group_count: usize) -> bool {
    is_global_root_partition && group_count == 1
}

fn fit_ebcp_rotation(
    embeddings: &[Vec<f32>],
) -> Result<(Vec<f32>, Vec<f32>), StreamingIndexerError> {
    let first =
        embeddings
            .first()
            .ok_or(StreamingIndexerError::TerminalPartitionMaterialization(
                "cannot fit block-local PCA rotation from an empty branch-entry set".into(),
            ))?;
    let dims = first.len();
    for embedding in embeddings {
        if embedding.len() != dims {
            return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                "EBCP branch embeddings disagree on dimensionality".into(),
            ));
        }
    }
    if embeddings.len() < 2 {
        let mut rotation = vec![0.0f32; dims * dims];
        for index in 0..dims {
            rotation[index * dims + index] = 1.0;
        }
        return Ok((rotation, vec![0.0; dims]));
    }
    let transform = fit(embeddings).map_err(|error| {
        StreamingIndexerError::TerminalPartitionMaterialization(format!(
            "failed to fit block-local PCA rotation: {error}"
        ))
    })?;
    let explained_variance = transform
        .explained_variance()
        .map(|values| values.to_vec())
        .unwrap_or_else(|| vec![0.0; transform.output_dim]);
    let mut rotation = Vec::with_capacity(transform.input_dim * transform.output_dim);
    for rotated_index in 0..transform.output_dim {
        for ambient_index in 0..transform.input_dim {
            rotation.push(transform.basis[ambient_index + rotated_index * transform.input_dim]);
        }
    }
    Ok((rotation, explained_variance))
}

fn exact_f32_centroid(embeddings: &[Vec<f32>]) -> Result<Vec<f32>, StreamingIndexerError> {
    let first =
        embeddings
            .first()
            .ok_or(StreamingIndexerError::TerminalPartitionMaterialization(
                "cannot compute EBCP centroid from an empty branch-entry set".into(),
            ))?;
    let mut sums = vec![0.0f64; first.len()];
    for embedding in embeddings {
        if embedding.len() != sums.len() {
            return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                "EBCP branch embeddings disagree on dimensionality".into(),
            ));
        }
        for (index, value) in embedding.iter().copied().enumerate() {
            sums[index] += f64::from(value);
        }
    }
    sums.into_iter()
        .enumerate()
        .map(|(index, sum)| {
            let mean = (sum / embeddings.len() as f64) as f32;
            if !mean.is_finite() {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    format!("EBCP centroid became non-finite at dimension {index}"),
                ));
            }
            Ok(mean)
        })
        .collect()
}

fn decode_f32_embedding_exact(
    embedding: &[u8],
    spec: &EmbeddingSpec,
    context: &str,
) -> Result<Vec<f32>, String> {
    validate_embedding_bytes(embedding, spec, context)?;
    if spec.encoding != "f32le" {
        return Err(format!(
            "{context} requires f32le embeddings, got {:?}",
            spec.encoding
        ));
    }
    embedding
        .chunks_exact(4)
        .map(|chunk| {
            let value = f32::from_le_bytes(chunk.try_into().unwrap());
            if !value.is_finite() {
                return Err("embedding contains non-finite f32 values".into());
            }
            Ok(value)
        })
        .collect()
}

fn encode_pca_rot_f32le_entries(
    original_entries: &[BranchEntry],
    decoded: &[Vec<f32>],
    logical_embedding_spec: &EmbeddingSpec,
    rotation_matrix: &[f32],
) -> Result<EncodedBranchEntries, StreamingIndexerError> {
    let encoded_entries = original_entries
        .iter()
        .zip(decoded.iter())
        .map(|(entry, vector)| {
            let rotated = apply_row_major_rotation(rotation_matrix, vector)?;
            Ok(BranchEntry {
                embedding: encode_f32_vec(rotated.as_slice()),
                child: entry.child,
            })
        })
        .collect::<Result<Vec<BranchEntry>, StreamingIndexerError>>()?;
    Ok(EncodedBranchEntries {
        embedding_spec: EmbeddingSpec {
            dims: logical_embedding_spec.dims,
            encoding: "pca-rot-f32le".into(),
        },
        entries: encoded_entries,
        ext: Some(ebcp_extension_map(&EbcpDescriptor {
            version: 1,
            logical_embedding_spec: logical_embedding_spec.clone(),
            base_centroid: None,
            rotation: Some(EbcpRotation {
                matrix_format: "f32le-row-major".into(),
                matrix: rotation_matrix.to_vec(),
            }),
            quantization: None,
        })),
    })
}

fn encode_pca_rot_delta_f32le_entries(
    original_entries: &[BranchEntry],
    decoded: &[Vec<f32>],
    logical_embedding_spec: &EmbeddingSpec,
    rotation_matrix: &[f32],
    centroid: &[f32],
) -> Result<EncodedBranchEntries, StreamingIndexerError> {
    let encoded_entries = original_entries
        .iter()
        .zip(decoded.iter())
        .map(|(entry, vector)| {
            let delta = subtract_f32_vectors(vector, centroid)?;
            let rotated = apply_row_major_rotation(rotation_matrix, delta.as_slice())?;
            Ok(BranchEntry {
                embedding: encode_f32_vec(rotated.as_slice()),
                child: entry.child,
            })
        })
        .collect::<Result<Vec<BranchEntry>, StreamingIndexerError>>()?;
    Ok(EncodedBranchEntries {
        embedding_spec: EmbeddingSpec {
            dims: logical_embedding_spec.dims,
            encoding: "pca-rot-delta-f32le".into(),
        },
        entries: encoded_entries,
        ext: Some(ebcp_extension_map(&EbcpDescriptor {
            version: 1,
            logical_embedding_spec: logical_embedding_spec.clone(),
            base_centroid: Some(centroid.to_vec()),
            rotation: Some(EbcpRotation {
                matrix_format: "f32le-row-major".into(),
                matrix: rotation_matrix.to_vec(),
            }),
            quantization: None,
        })),
    })
}

enum QuantizedBranchEncoding {
    Uniform(u8),
    Variable(u8),
}

fn encode_pca_rot_delta_quantized_entries(
    original_entries: &[BranchEntry],
    decoded: &[Vec<f32>],
    logical_embedding_spec: &EmbeddingSpec,
    rotation_matrix: &[f32],
    centroid: &[f32],
    encoding: QuantizedBranchEncoding,
    explained_variance: Option<&[f32]>,
) -> Result<EncodedBranchEntries, StreamingIndexerError> {
    let rotated_deltas = decoded
        .iter()
        .map(|vector| {
            let delta = subtract_f32_vectors(vector, centroid)?;
            apply_row_major_rotation(rotation_matrix, delta.as_slice())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let bit_widths = match encoding {
        QuantizedBranchEncoding::Uniform(bit_width) => vec![bit_width; centroid.len()],
        QuantizedBranchEncoding::Variable(total_uniform_bits) => allocate_variable_bit_widths(
            explained_variance.ok_or(StreamingIndexerError::TerminalPartitionMaterialization(
                "missing explained variance for variable-bit EBCP encoding".into(),
            ))?,
            total_uniform_bits,
        )?,
    };
    let scale_factors =
        compute_quantization_scales(rotated_deltas.as_slice(), bit_widths.as_slice())?;
    let encoded_entries = original_entries
        .iter()
        .zip(rotated_deltas.iter())
        .map(|(entry, rotated)| {
            let embedding = pack_quantized_delta_vector(
                rotated,
                bit_widths.as_slice(),
                scale_factors.as_slice(),
            )?;
            Ok(BranchEntry {
                embedding,
                child: entry.child,
            })
        })
        .collect::<Result<Vec<BranchEntry>, StreamingIndexerError>>()?;
    let (encoding_name, quantization) = match encoding {
        QuantizedBranchEncoding::Uniform(bit_width) => (
            "pca-rot-delta-uq",
            EbcpQuantization::Uniform {
                bit_width,
                scale_factors,
            },
        ),
        QuantizedBranchEncoding::Variable(_) => (
            "pca-rot-delta-vbq",
            EbcpQuantization::Variable {
                bit_widths,
                scale_factors,
            },
        ),
    };
    Ok(EncodedBranchEntries {
        embedding_spec: EmbeddingSpec {
            dims: logical_embedding_spec.dims,
            encoding: encoding_name.into(),
        },
        entries: encoded_entries,
        ext: Some(ebcp_extension_map(&EbcpDescriptor {
            version: 1,
            logical_embedding_spec: logical_embedding_spec.clone(),
            base_centroid: Some(centroid.to_vec()),
            rotation: Some(EbcpRotation {
                matrix_format: "f32le-row-major".into(),
                matrix: rotation_matrix.to_vec(),
            }),
            quantization: Some(quantization),
        })),
    })
}

fn encode_ambient_delta_quantized_entries(
    original_entries: &[BranchEntry],
    decoded: &[Vec<f32>],
    logical_embedding_spec: &EmbeddingSpec,
    centroid: &[f32],
    bit_width: u8,
) -> Result<EncodedBranchEntries, StreamingIndexerError> {
    let ambient_deltas = decoded
        .iter()
        .map(|vector| subtract_f32_vectors(vector, centroid))
        .collect::<Result<Vec<_>, _>>()?;
    let bit_widths = vec![bit_width; centroid.len()];
    let scale_factors =
        compute_quantization_scales(ambient_deltas.as_slice(), bit_widths.as_slice())?;
    let encoded_entries = original_entries
        .iter()
        .zip(ambient_deltas.iter())
        .map(|(entry, ambient_delta)| {
            let embedding = pack_quantized_delta_vector(
                ambient_delta,
                bit_widths.as_slice(),
                scale_factors.as_slice(),
            )?;
            Ok(BranchEntry {
                embedding,
                child: entry.child,
            })
        })
        .collect::<Result<Vec<BranchEntry>, StreamingIndexerError>>()?;
    Ok(EncodedBranchEntries {
        embedding_spec: EmbeddingSpec {
            dims: logical_embedding_spec.dims,
            encoding: "ambient-delta-uq".into(),
        },
        entries: encoded_entries,
        ext: Some(ebcp_extension_map(&EbcpDescriptor {
            version: 1,
            logical_embedding_spec: logical_embedding_spec.clone(),
            base_centroid: Some(centroid.to_vec()),
            rotation: None,
            quantization: Some(EbcpQuantization::Uniform {
                bit_width,
                scale_factors,
            }),
        })),
    })
}

fn apply_row_major_rotation(
    rotation_matrix: &[f32],
    vector: &[f32],
) -> Result<Vec<f32>, StreamingIndexerError> {
    if rotation_matrix.len() != vector.len() * vector.len() {
        return Err(StreamingIndexerError::TerminalPartitionMaterialization(
            "EBCP rotation matrix shape does not match logical dimensions".into(),
        ));
    }
    let dims = vector.len();
    let mut rotated = vec![0.0f32; dims];
    for rotated_index in 0..dims {
        let mut acc = 0.0f32;
        for ambient_index in 0..dims {
            acc += rotation_matrix[rotated_index * dims + ambient_index] * vector[ambient_index];
        }
        if !acc.is_finite() {
            return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                "EBCP rotation produced a non-finite value".into(),
            ));
        }
        rotated[rotated_index] = acc;
    }
    Ok(rotated)
}

fn subtract_f32_vectors(left: &[f32], right: &[f32]) -> Result<Vec<f32>, StreamingIndexerError> {
    if left.len() != right.len() {
        return Err(StreamingIndexerError::TerminalPartitionMaterialization(
            "EBCP vector subtraction requires matching dimensions".into(),
        ));
    }
    left.iter()
        .zip(right.iter())
        .map(|(&lhs, &rhs)| {
            let value = lhs - rhs;
            if !value.is_finite() {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    "EBCP delta vector produced a non-finite value".into(),
                ));
            }
            Ok(value)
        })
        .collect()
}

fn encode_f32_vec(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(values.len() * 4);
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn compute_quantization_scales(
    delta_vectors: &[Vec<f32>],
    bit_widths: &[u8],
) -> Result<Vec<f32>, StreamingIndexerError> {
    let dims = bit_widths.len();
    let mut max_abs = vec![0.0f32; dims];
    for vector in delta_vectors {
        if vector.len() != dims {
            return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                "EBCP delta vectors disagree on dimensionality".into(),
            ));
        }
        for (index, value) in vector.iter().copied().enumerate() {
            max_abs[index] = max_abs[index].max(value.abs());
        }
    }
    max_abs
        .into_iter()
        .zip(bit_widths.iter().copied())
        .map(|(max_abs, bit_width)| {
            if bit_width == 0 {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    "EBCP quantization bit widths must be nonzero".into(),
                ));
            }
            let qmax = ((1_u32 << (bit_width - 1)) - 1) as f32;
            let scale = if max_abs == 0.0 || qmax == 0.0 {
                1.0
            } else {
                max_abs / qmax
            };
            if !scale.is_finite() || scale <= 0.0 {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    "EBCP quantization scale must be finite and positive".into(),
                ));
            }
            Ok(scale)
        })
        .collect()
}

fn pack_quantized_delta_vector(
    values: &[f32],
    bit_widths: &[u8],
    scale_factors: &[f32],
) -> Result<Vec<u8>, StreamingIndexerError> {
    if values.len() != bit_widths.len() || values.len() != scale_factors.len() {
        return Err(StreamingIndexerError::TerminalPartitionMaterialization(
            "EBCP quantization metadata dimension mismatch".into(),
        ));
    }
    let total_bits = bit_widths.iter().try_fold(0usize, |sum, width| {
        sum.checked_add(usize::from(*width)).ok_or(
            StreamingIndexerError::TerminalPartitionMaterialization(
                "EBCP quantized payload bit length overflowed".into(),
            ),
        )
    })?;
    let mut bytes = vec![0u8; total_bits.div_ceil(8)];
    let mut bit_offset = 0usize;
    for ((&value, &bit_width), &scale) in values
        .iter()
        .zip(bit_widths.iter())
        .zip(scale_factors.iter())
    {
        let qmax = ((1_i32 << (bit_width - 1)) - 1) as f64;
        let centered = if scale == 0.0 {
            0.0
        } else {
            f64::from(value) / f64::from(scale)
        };
        let quantized = centered.round_ties_even().clamp(-qmax, qmax) as i32;
        let stored = u32::try_from(quantized + (1_i32 << (bit_width - 1))).map_err(|_| {
            StreamingIndexerError::TerminalPartitionMaterialization(
                "EBCP quantized code did not fit the declared bit width".into(),
            )
        })?;
        write_lsb_first_bits(bytes.as_mut_slice(), bit_offset, bit_width, stored)?;
        bit_offset += usize::from(bit_width);
    }
    Ok(bytes)
}

fn write_lsb_first_bits(
    bytes: &mut [u8],
    start_bit: usize,
    bit_width: u8,
    value: u32,
) -> Result<(), StreamingIndexerError> {
    for bit_index in 0..usize::from(bit_width) {
        let absolute_bit = start_bit + bit_index;
        let byte_index = absolute_bit / 8;
        let intra_byte = absolute_bit % 8;
        let bit = ((value >> bit_index) & 1) as u8;
        bytes[byte_index] |= bit << intra_byte;
    }
    Ok(())
}

fn allocate_variable_bit_widths(
    explained_variance: &[f32],
    uniform_bit_width: u8,
) -> Result<Vec<u8>, StreamingIndexerError> {
    const MAX_EBCP_BIT_WIDTH: u8 = 31;
    if explained_variance.is_empty() {
        return Err(StreamingIndexerError::TerminalPartitionMaterialization(
            "variable-bit EBCP encoding requires at least one explained-variance entry".into(),
        ));
    }
    let total_bits = usize::from(uniform_bit_width)
        .checked_mul(explained_variance.len())
        .ok_or(StreamingIndexerError::TerminalPartitionMaterialization(
            "variable-bit EBCP bit budget overflowed".into(),
        ))?;
    let max_total_bits = usize::from(MAX_EBCP_BIT_WIDTH)
        .checked_mul(explained_variance.len())
        .ok_or(StreamingIndexerError::TerminalPartitionMaterialization(
            "variable-bit EBCP maximum bit budget overflowed".into(),
        ))?;
    if total_bits > max_total_bits {
        return Err(StreamingIndexerError::TerminalPartitionMaterialization(
            "variable-bit EBCP budget exceeds the 31-bit per-dimension limit".into(),
        ));
    }
    let mut widths = vec![1u8; explained_variance.len()];
    let remaining = total_bits.saturating_sub(explained_variance.len());
    if remaining == 0 {
        return Ok(widths);
    }

    let weights = explained_variance
        .iter()
        .map(|value| (1.0 + f64::from((*value).max(0.0))).ln())
        .collect::<Vec<_>>();
    let weight_sum = weights.iter().sum::<f64>();
    let desired = if weight_sum > 0.0 {
        weights
            .iter()
            .map(|weight| (weight / weight_sum) * remaining as f64)
            .collect::<Vec<_>>()
    } else {
        vec![remaining as f64 / widths.len() as f64; widths.len()]
    };
    let base = desired
        .iter()
        .map(|value| value.floor() as usize)
        .collect::<Vec<_>>();
    for (width, addend) in widths.iter_mut().zip(base.iter().copied()) {
        let capped_addend = addend.min(usize::from(MAX_EBCP_BIT_WIDTH - 1));
        *width = width.saturating_add(u8::try_from(capped_addend).map_err(|_| {
            StreamingIndexerError::TerminalPartitionMaterialization(
                "variable-bit EBCP bit width exceeded u8".into(),
            )
        })?);
    }
    let used = widths
        .iter()
        .map(|width| usize::from(*width) - 1)
        .sum::<usize>();
    let mut leftovers = remaining - used;
    let mut remainders = desired
        .iter()
        .enumerate()
        .map(|(index, value)| (index, value - value.floor()))
        .collect::<Vec<_>>();
    remainders.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    while leftovers > 0 {
        let mut progressed = false;
        for (index, _) in remainders.iter().copied() {
            if leftovers == 0 {
                break;
            }
            if widths[index] >= MAX_EBCP_BIT_WIDTH {
                continue;
            }
            widths[index] = widths[index].checked_add(1).ok_or(
                StreamingIndexerError::TerminalPartitionMaterialization(
                    "variable-bit EBCP bit width exceeded u8".into(),
                ),
            )?;
            leftovers -= 1;
            progressed = true;
        }
        if !progressed {
            return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                "variable-bit EBCP allocation could not satisfy the 31-bit per-dimension limit"
                    .into(),
            ));
        }
    }
    Ok(widths)
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

fn normalize_child_summary_inputs(mut children: Vec<ChildSummaryInput>) -> Vec<ChildSummaryInput> {
    children.sort_by(|left, right| {
        left.child
            .as_bytes()
            .cmp(right.child.as_bytes())
            .then_with(|| left.embedding.cmp(&right.embedding))
    });
    let mut deduped = Vec::with_capacity(children.len());
    for child in children {
        if deduped
            .last()
            .is_some_and(|prev: &ChildSummaryInput| prev.child == child.child)
        {
            continue;
        }
        deduped.push(child);
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

fn exact_centroid_child_summary(
    children: &[ChildSummaryInput],
    embedding_spec: &EmbeddingSpec,
) -> Result<Vec<u8>, String> {
    if children.is_empty() {
        return Err("exact-centroid child summary requires at least one child".into());
    }
    let dims = usize::try_from(embedding_spec.dims)
        .map_err(|_| format!("embedding dims {} do not fit in usize", embedding_spec.dims))?;
    let mut sums = vec![0.0f64; dims];
    let mut total_weight = 0usize;
    for (index, child) in children.iter().enumerate() {
        if child.descendant_count == 0 {
            return Err(format!(
                "child summary {index} has zero descendant count and cannot contribute to an exact centroid"
            ));
        }
        let decoded =
            decode_embedding_as_f64(&child.embedding, embedding_spec, "exact-centroid")
                .map_err(|error| format!("failed to decode child summary {index}: {error}"))?;
        for (dimension, (sum, value)) in sums.iter_mut().zip(decoded).enumerate() {
            if !value.is_finite() {
                return Err(format!(
                    "child summary {index} contains non-finite value at dimension {dimension}"
                ));
            }
            *sum += value * child.descendant_count as f64;
            if !sum.is_finite() {
                return Err(format!(
                    "exact-centroid sum overflowed at dimension {dimension}"
                ));
            }
        }
        total_weight = total_weight
            .checked_add(child.descendant_count)
            .ok_or_else(|| "exact-centroid total descendant count overflowed usize".to_string())?;
    }
    for (dimension, sum) in sums.iter_mut().enumerate() {
        *sum /= total_weight as f64;
        if !sum.is_finite() {
            return Err(format!(
                "exact-centroid result became non-finite at dimension {dimension}"
            ));
        }
    }
    encode_embedding_from_f64(&sums, embedding_spec)
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

fn effective_directional_pca_cluster_count(
    requested_cluster_count: u32,
    estimated_child_count: usize,
    materializability_bound: usize,
    allocation_policy: DirectionalPcaAllocationPolicy,
) -> Result<u32, String> {
    let effective = effective_cluster_count(
        requested_cluster_count,
        estimated_child_count,
        materializability_bound,
    )?;
    if allocation_policy != DirectionalPcaAllocationPolicy::EigenvalueLogBits || effective <= 1 {
        return Ok(effective);
    }
    let adjusted = 1u32 << (u32::BITS - 1 - effective.leading_zeros());
    Ok(adjusted.max(2))
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

fn fallback_partition_groups(
    len: usize,
    materializability_bound: usize,
    fallback_group_cap: Option<usize>,
) -> Result<Vec<Vec<usize>>, String> {
    let groups = balanced_groups(len, materializability_bound)?;
    let Some(group_cap) = fallback_group_cap else {
        return Ok(groups);
    };
    if groups.len() <= group_cap {
        return Ok(groups);
    }
    if group_cap < 2 {
        return Err(format!(
            "cannot split {len} items under fallback fanout cap {group_cap}"
        ));
    }

    let group_count = group_cap.min(len);
    let base = len / group_count;
    let remainder = len % group_count;
    let mut capped_groups = Vec::with_capacity(group_count);
    let mut next = 0usize;
    for group_index in 0..group_count {
        let size = base + usize::from(group_index < remainder);
        capped_groups.push((next..next + size).collect());
        next += size;
    }
    Ok(capped_groups)
}

fn verify_persisted_block_id(
    actual: BlockHash,
    expected: BlockHash,
) -> Result<(), StreamingIndexerError> {
    if actual == expected {
        Ok(())
    } else {
        Err(StreamingIndexerError::Storage(
            BlockStoreError::ContractViolation(BlockError::HashMismatch { expected, actual }),
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
        if partition.terminal && partition.item_indices.is_empty() {
            return Err(format!(
                "terminal partition {:?} must contain at least one item",
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

fn build_partition_routing_plan(
    hierarchy: &FinalizedPartitionHierarchy,
    item_count: usize,
) -> Result<PartitionRoutingPlan, StreamingIndexerError> {
    validate_partition_hierarchy(hierarchy, item_count)
        .map_err(StreamingIndexerError::HierarchyValidation)?;

    let mut terminal_partitions = hierarchy
        .partitions
        .iter()
        .filter(|partition| partition.terminal)
        .collect::<Vec<_>>();
    terminal_partitions.sort_by(|left, right| left.id.cmp(&right.id));

    let mut terminal_partition_ids = Vec::with_capacity(terminal_partitions.len());
    let mut terminal_partition_item_counts = Vec::with_capacity(terminal_partitions.len());
    let route_dir = tempfile::tempdir().map_err(|error| {
        StreamingIndexerError::LocalSpill(format!(
            "could not create temporary routing directory: {error}"
        ))
    })?;
    let route_path = route_dir.path().join("partition-routing.bin");
    {
        let route_file = File::create(&route_path).map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not create partition routing file {}: {error}",
                route_path.display()
            ))
        })?;
        let mut writer = BufWriter::new(route_file);
        for _ in 0..item_count {
            writer
                .write_all(&u32::MAX.to_le_bytes())
                .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
        }
        writer
            .flush()
            .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
    }
    let mut route_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&route_path)
        .map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not reopen partition routing file {}: {error}",
                route_path.display()
            ))
        })?;
    let mut assigned_item_count = 0usize;
    for (partition_ordinal, partition) in terminal_partitions.into_iter().enumerate() {
        if partition.item_indices.is_empty() {
            return Err(StreamingIndexerError::HierarchyValidation(format!(
                "terminal partition {:?} must contain at least one item",
                partition.id
            )));
        }
        let partition_ordinal = u32::try_from(partition_ordinal).map_err(|_| {
            StreamingIndexerError::HierarchyValidation(
                "terminal partition ordinal does not fit into u32".into(),
            )
        })?;
        terminal_partition_ids.push(partition.id.clone());
        terminal_partition_item_counts.push(partition.item_indices.len());
        for &item_index in &partition.item_indices {
            let offset = u64::try_from(item_index)
                .map_err(|_| {
                    StreamingIndexerError::HierarchyValidation(
                        "item index does not fit into u64".into(),
                    )
                })?
                .checked_mul(4)
                .ok_or_else(|| {
                    StreamingIndexerError::HierarchyValidation(
                        "partition routing offset overflowed".into(),
                    )
                })?;
            route_file
                .seek(SeekFrom::Start(offset))
                .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
            let mut existing = [0u8; 4];
            route_file
                .read_exact(&mut existing)
                .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
            if u32::from_le_bytes(existing) != u32::MAX {
                return Err(StreamingIndexerError::HierarchyValidation(format!(
                    "item index {item_index} is assigned to more than one terminal partition"
                )));
            }
            route_file
                .seek(SeekFrom::Start(offset))
                .and_then(|_| route_file.write_all(&partition_ordinal.to_le_bytes()))
                .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
            assigned_item_count += 1;
        }
    }
    if assigned_item_count != item_count {
        return Err(StreamingIndexerError::HierarchyValidation(
            "every logical item must resolve to a terminal partition".into(),
        ));
    }

    Ok(PartitionRoutingPlan {
        terminal_partition_ids,
        terminal_partition_item_counts,
        _route_dir: route_dir,
        route_path,
    })
}

impl PartitionRoutingPlan {
    fn open_reader(&self) -> Result<PartitionRoutingReader, StreamingIndexerError> {
        let route_file = File::open(&self.route_path).map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not open partition routing file {}: {error}",
                self.route_path.display()
            ))
        })?;
        Ok(PartitionRoutingReader {
            reader: BufReader::new(route_file),
        })
    }
}

struct PartitionRoutingReader {
    reader: BufReader<File>,
}

impl PartitionRoutingReader {
    fn read_partition_ordinal(&mut self) -> Result<Option<u32>, StreamingIndexerError> {
        let mut ordinal = [0u8; 4];
        match self.reader.read_exact(&mut ordinal) {
            Ok(()) => Ok(Some(u32::from_le_bytes(ordinal))),
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(error) => Err(StreamingIndexerError::LocalSpill(error.to_string())),
        }
    }
}

impl StreamingV2QuantilePlannerState {
    fn new(parent: &TempDir) -> Result<Self, StreamingIndexerError> {
        let dir = tempfile::Builder::new()
            .prefix("partition-")
            .tempdir_in(parent.path())
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not create v2 planner state directory under {}: {error}",
                    parent.path().display()
                ))
            })?;
        Ok(Self {
            quantile_pass: None,
            dir,
        })
    }
}

impl DirectionalPcaOutOfCorePlannerState for StreamingV2QuantilePlannerState {
    fn begin_quantile_pass(
        &mut self,
        axis_count: usize,
        expected_value_count: usize,
    ) -> Result<(), String> {
        self.clear_quantile_pass()?;
        let window_bytes = per_axis_planner_state_window_bytes(axis_count)?;
        let mut paths = Vec::with_capacity(axis_count);
        let mut writers = Vec::with_capacity(axis_count);
        for axis_index in 0..axis_count {
            let path = self.dir.path().join(format!("axis-{axis_index:04}.bin"));
            match BufferedF32Writer::create(&path, expected_value_count, window_bytes) {
                Ok(writer) => {
                    paths.push(path);
                    writers.push(writer);
                }
                Err(error) => {
                    drop(writers);
                    if path.is_file()
                        && let Err(remove_error) = std::fs::remove_file(&path)
                        && remove_error.kind() != std::io::ErrorKind::NotFound
                    {
                        return Err(format!(
                            "{error}; additionally could not remove planner state file {}: {remove_error}",
                            path.display()
                        ));
                    }
                    for stale_path in &paths {
                        if let Err(remove_error) = std::fs::remove_file(stale_path)
                            && remove_error.kind() != std::io::ErrorKind::NotFound
                        {
                            return Err(format!(
                                "{error}; additionally could not remove planner state file {}: {remove_error}",
                                stale_path.display()
                            ));
                        }
                    }
                    return Err(error);
                }
            }
        }
        self.quantile_pass = Some(StreamingV2QuantilePassFiles {
            expected_value_count,
            paths,
            writers,
        });
        Ok(())
    }

    fn append_quantile_values(&mut self, values: &[f32]) -> Result<(), String> {
        let quantile_pass = self
            .quantile_pass
            .as_mut()
            .ok_or_else(|| "quantile planner state is not initialized".to_string())?;
        if values.len() != quantile_pass.writers.len() {
            return Err(format!(
                "observed {} axis values but expected {}",
                values.len(),
                quantile_pass.writers.len()
            ));
        }
        for (writer, value) in quantile_pass.writers.iter_mut().zip(values.iter().copied()) {
            writer.write_f32(value)?;
        }
        Ok(())
    }

    fn finish_quantile_pass(&mut self) -> Result<(), String> {
        let quantile_pass = self
            .quantile_pass
            .as_mut()
            .ok_or_else(|| "quantile planner state is not initialized".to_string())?;
        for writer in &mut quantile_pass.writers {
            writer.finish()?;
        }
        Ok(())
    }

    fn scan_quantile_axis(
        &self,
        axis_index: usize,
        observe: &mut dyn FnMut(f32) -> Result<(), String>,
    ) -> Result<(), String> {
        let quantile_pass = self
            .quantile_pass
            .as_ref()
            .ok_or_else(|| "quantile planner state is not initialized".to_string())?;
        let path = quantile_pass
            .paths
            .get(axis_index)
            .ok_or_else(|| format!("missing quantile planner axis file {axis_index}"))?;
        let mut reader = WindowedMmapF32Reader::open(path, quantile_pass.expected_value_count)?;
        reader.scan(observe)
    }

    fn clear_quantile_pass(&mut self) -> Result<(), String> {
        if let Some(quantile_pass) = self.quantile_pass.take() {
            let StreamingV2QuantilePassFiles {
                expected_value_count: _,
                paths,
                writers,
            } = quantile_pass;
            drop(writers);
            for path in paths {
                std::fs::remove_file(&path).map_err(|error| {
                    format!(
                        "could not remove planner state file {}: {error}",
                        path.display()
                    )
                })?;
            }
        }
        Ok(())
    }
}

impl BufferedF32Writer {
    fn create(path: &Path, total_values: usize, window_bytes: usize) -> Result<Self, String> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|error| {
                format!(
                    "could not create planner state file {}: {error}",
                    path.display()
                )
            })?;
        let total_bytes = u64::try_from(total_byte_len(total_values)?)
            .map_err(|_| "planner state file length overflowed".to_string())?;
        file.set_len(total_bytes).map_err(|error| {
            format!(
                "could not size planner state file {}: {error}",
                path.display()
            )
        })?;
        Ok(Self {
            writer: BufWriter::with_capacity(window_bytes.max(V2_PLANNER_STATE_VALUE_BYTES), file),
            total_values,
            written_values: 0,
        })
    }

    fn write_f32(&mut self, value: f32) -> Result<(), String> {
        if self.written_values >= self.total_values {
            return Err("planner state received more values than expected".into());
        }
        self.writer
            .write_all(&value.to_le_bytes())
            .map_err(|error| format!("could not write planner state value: {error}"))?;
        self.written_values += 1;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), String> {
        if self.written_values != self.total_values {
            return Err(format!(
                "planner state captured {} values but expected {}",
                self.written_values, self.total_values
            ));
        }
        self.writer
            .flush()
            .map_err(|error| format!("could not flush planner state writer: {error}"))
    }
}

fn per_axis_planner_state_window_bytes(axis_count: usize) -> Result<usize, String> {
    if axis_count == 0 {
        return Err("quantile planner state requires at least one axis".into());
    }
    let per_axis = V2_PLANNER_STATE_WINDOW_BYTES
        .checked_div(axis_count)
        .unwrap_or(0)
        .max(V2_PLANNER_STATE_VALUE_BYTES);
    Ok(per_axis)
}

impl WindowedMmapF32Reader {
    fn open(path: &Path, total_values: usize) -> Result<Self, String> {
        let file = File::open(path).map_err(|error| {
            format!(
                "could not open planner state file {}: {error}",
                path.display()
            )
        })?;
        let expected_len = u64::try_from(total_byte_len(total_values)?)
            .map_err(|_| "planner state file length overflowed".to_string())?;
        let actual_len = file
            .metadata()
            .map_err(|error| {
                format!(
                    "could not inspect planner state file {}: {error}",
                    path.display()
                )
            })?
            .len();
        if actual_len < expected_len {
            return Err(format!(
                "planner state file {} is truncated: expected at least {expected_len} bytes but found {actual_len}",
                path.display()
            ));
        }
        Ok(Self {
            file,
            total_values,
            read_values: 0,
            map: None,
            map_start: 0,
            map_len: 0,
        })
    }

    fn scan(&mut self, observe: &mut dyn FnMut(f32) -> Result<(), String>) -> Result<(), String> {
        while self.read_values < self.total_values {
            let byte_offset = self
                .read_values
                .checked_mul(V2_PLANNER_STATE_VALUE_BYTES)
                .ok_or_else(|| "planner state read offset overflowed".to_string())?;
            self.ensure_window(byte_offset)?;
            let local_offset = byte_offset
                .checked_sub(
                    usize::try_from(self.map_start)
                        .map_err(|_| "planner map start overflowed".to_string())?,
                )
                .ok_or_else(|| "planner state local offset underflowed".to_string())?;
            let map = self
                .map
                .as_ref()
                .ok_or_else(|| "planner state read window is not mapped".to_string())?;
            let value_end = local_offset
                .checked_add(V2_PLANNER_STATE_VALUE_BYTES)
                .ok_or_else(|| "planner state local value range overflowed".to_string())?;
            let value =
                f32::from_le_bytes(map[local_offset..value_end].try_into().map_err(|_| {
                    "planner state window returned an invalid value size".to_string()
                })?);
            observe(value)?;
            self.read_values += 1;
        }
        self.map = None;
        self.map_len = 0;
        Ok(())
    }

    fn ensure_window(&mut self, byte_offset: usize) -> Result<(), String> {
        if self.map.is_some() && self.window_contains(byte_offset) {
            return Ok(());
        }
        self.map = None;
        let aligned_start = align_down(
            u64::try_from(byte_offset)
                .map_err(|_| "planner state byte offset overflowed".to_string())?,
            V2_MMAP_ALLOCATION_GRANULARITY,
        );
        let total_bytes = total_byte_len(self.total_values)?;
        let remaining = total_bytes
            .checked_sub(
                usize::try_from(aligned_start)
                    .map_err(|_| "planner map start overflowed".to_string())?,
            )
            .ok_or_else(|| "planner state remaining window underflowed".to_string())?;
        let map_len =
            remaining.min(V2_PLANNER_STATE_WINDOW_BYTES.max(V2_PLANNER_STATE_VALUE_BYTES));
        // SAFETY: open() validated the file length from total_values; aligned_start is
        // allocation-granularity aligned and checked against total_bytes above; map_len is
        // non-zero and the mapping stays within the file range for the lifetime of self.file.
        let map = unsafe {
            MmapOptions::new()
                .offset(aligned_start)
                .len(map_len)
                .map(&self.file)
        }
        .map_err(|error| format!("could not map planner state window: {error}"))?;
        self.map_start = aligned_start;
        self.map_len = map_len;
        self.map = Some(map);
        Ok(())
    }

    fn window_contains(&self, byte_offset: usize) -> bool {
        let start = usize::try_from(self.map_start).unwrap_or(usize::MAX);
        let Some(end) = byte_offset.checked_add(V2_PLANNER_STATE_VALUE_BYTES) else {
            return false;
        };
        byte_offset >= start && end <= start.saturating_add(self.map_len)
    }
}

fn align_down(value: u64, alignment: u64) -> u64 {
    value / alignment * alignment
}

fn total_byte_len(total_values: usize) -> Result<usize, String> {
    total_values
        .checked_mul(V2_PLANNER_STATE_VALUE_BYTES)
        .ok_or_else(|| "planner state file length overflowed".to_string())
}

impl PartitionSpillDirectory {
    fn new(partition_count: usize) -> Result<Self, StreamingIndexerError> {
        let dir = tempfile::tempdir().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not create temporary spill directory: {error}"
            ))
        })?;

        let mut paths = Vec::with_capacity(partition_count);
        for index in 0..partition_count {
            let path = dir.path().join(format!("partition-{index:08}.spill"));
            File::create(&path).map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not create partition spill {}: {error}",
                    path.display()
                ))
            })?;
            paths.push(path);
        }

        Ok(Self {
            dir: Some(dir),
            paths,
            writers: (0..partition_count).map(|_| None).collect(),
        })
    }

    fn append_leaf_child(
        &mut self,
        partition_ordinal: u32,
        child: &IndexedChild,
    ) -> Result<(), StreamingIndexerError> {
        let partition_ordinal = usize::try_from(partition_ordinal).map_err(|_| {
            StreamingIndexerError::LocalSpill("partition ordinal does not fit into usize".into())
        })?;
        let writer_slot = self.writers.get_mut(partition_ordinal).ok_or_else(|| {
            StreamingIndexerError::LocalSpill(format!(
                "partition ordinal {partition_ordinal} is out of range for spill routing"
            ))
        })?;
        if writer_slot.is_none() {
            let path = self.paths.get(partition_ordinal).ok_or_else(|| {
                StreamingIndexerError::LocalSpill(format!(
                    "partition ordinal {partition_ordinal} is missing a spill path"
                ))
            })?;
            let file = File::create(path).map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not create partition spill {}: {error}",
                    path.display()
                ))
            })?;
            *writer_slot = Some(BufWriter::new(file));
        }

        let writer = writer_slot.as_mut().ok_or_else(|| {
            StreamingIndexerError::LocalSpill(format!(
                "partition ordinal {partition_ordinal} spill writer is unavailable"
            ))
        })?;
        write_spilled_indexed_child(writer, child)
    }

    fn finish(mut self) -> Result<Self, StreamingIndexerError> {
        for writer in &mut self.writers {
            if let Some(writer) = writer.as_mut() {
                writer
                    .flush()
                    .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
            }
        }
        self.writers.iter_mut().for_each(|writer| *writer = None);
        Ok(self)
    }

    fn read_partition_children(
        &self,
        partition_ordinal: usize,
    ) -> Result<Vec<IndexedChild>, StreamingIndexerError> {
        let path = self.paths.get(partition_ordinal).ok_or_else(|| {
            StreamingIndexerError::LocalSpill(format!(
                "partition ordinal {partition_ordinal} is missing a spill path"
            ))
        })?;
        let file = File::open(path).map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not open partition spill {}: {error}",
                path.display()
            ))
        })?;
        let mut reader = BufReader::new(file);
        let mut children = Vec::new();
        while let Some(child) = read_spilled_indexed_child(&mut reader)? {
            children.push(child);
        }
        Ok(children)
    }
}

impl Drop for PartitionSpillDirectory {
    fn drop(&mut self) {
        self.writers.clear();
        if let Some(dir) = self.dir.take() {
            let _ = dir.close();
        }
    }
}

fn write_spilled_indexed_child(
    writer: &mut BufWriter<File>,
    child: &IndexedChild,
) -> Result<(), StreamingIndexerError> {
    let embedding_len = u32::try_from(child.embedding.len()).map_err(|_| {
        StreamingIndexerError::LocalSpill(
            "embedding length does not fit the spill file format".into(),
        )
    })?;
    let descendant_count = u64::try_from(child.descendant_count).map_err(|_| {
        StreamingIndexerError::LocalSpill(
            "descendant count does not fit the spill file format".into(),
        )
    })?;
    writer
        .write_all(&embedding_len.to_le_bytes())
        .and_then(|_| writer.write_all(&child.embedding))
        .and_then(|_| writer.write_all(child.child.as_bytes()))
        .and_then(|_| writer.write_all(&child.level.to_le_bytes()))
        .and_then(|_| writer.write_all(&descendant_count.to_le_bytes()))
        .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))
}

fn read_spilled_indexed_child(
    reader: &mut BufReader<File>,
) -> Result<Option<IndexedChild>, StreamingIndexerError> {
    let mut embedding_len_bytes = [0u8; 4];
    match reader.read_exact(&mut embedding_len_bytes) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(StreamingIndexerError::LocalSpill(error.to_string())),
    }
    let embedding_len = usize::try_from(u32::from_le_bytes(embedding_len_bytes)).map_err(|_| {
        StreamingIndexerError::LocalSpill("spilled embedding length does not fit usize".into())
    })?;
    let mut embedding = vec![0u8; embedding_len];
    reader
        .read_exact(&mut embedding)
        .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;

    let mut child_bytes = [0u8; BlockHash::LEN];
    reader
        .read_exact(&mut child_bytes)
        .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;

    let mut level_bytes = [0u8; 8];
    reader
        .read_exact(&mut level_bytes)
        .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;

    let mut descendant_count_bytes = [0u8; 8];
    reader
        .read_exact(&mut descendant_count_bytes)
        .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;

    Ok(Some(IndexedChild {
        embedding,
        child: BlockHash::from_bytes(child_bytes),
        level: u64::from_le_bytes(level_bytes),
        descendant_count: usize::try_from(u64::from_le_bytes(descendant_count_bytes)).map_err(
            |_| {
                StreamingIndexerError::LocalSpill(
                    "spilled descendant count does not fit usize".into(),
                )
            },
        )?,
    }))
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
    use std::collections::HashMap;
    use std::fmt;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::stream;

    use super::*;

    #[derive(Default)]
    pub(crate) struct MemoryBlockStore {
        blocks: Mutex<HashMap<BlockHash, Vec<u8>>>,
    }

    #[async_trait]
    impl BlockStore for MemoryBlockStore {
        async fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            self.blocks
                .lock()
                .unwrap()
                .insert(*block_id, block_bytes.to_vec());
            Ok(())
        }

        async fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            Ok(self.blocks.lock().unwrap().get(block_id).cloned())
        }

        fn iter_block_ids(
            &self,
        ) -> Result<lexongraph_block_store::BlockIdStream<'_>, BlockStoreError> {
            let ids = self
                .blocks
                .lock()
                .unwrap()
                .keys()
                .copied()
                .collect::<Vec<_>>();
            Ok(Box::pin(stream::iter(ids.into_iter().map(Ok))))
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
                .await
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
        <AH::Policy as CanonicalEmbeddingPolicy>::Error: 'static,
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
    use super::{
        BlockHash, ChildSummaryInput, DirectionalPcaAllocationPolicy,
        DirectionalPcaOutOfCorePlannerState, EmbeddingSpec, PUBLISHED_PROFILE_V0_1_0, RunPhase,
        StreamingIndexingRunV2, StreamingIndexingTrainerSubphase, StreamingV2CompletedPassSnapshot,
        StreamingV2Partition, StreamingV2PartitionNode, StreamingV2PartitionTopology,
        StreamingV2PassState, StreamingV2PendingPartitionStatus, StreamingV2QuantilePlannerState,
        allocate_variable_bit_widths, branch_encoding_policy_for_profile,
        effective_directional_pca_cluster_count, exact_centroid_child_summary,
        fallback_partition_groups, fit_ebcp_rotation, format_partition_label,
        published_indexing_profile, streaming_v2_topology_stats,
        summarize_streaming_v2_partition_blocker, unresolved_work_shrank, uses_root_branch_budget,
        weighted_mean_f32_embeddings,
    };
    use std::marker::PhantomData;

    #[test]
    fn weighted_representative_embedding_uses_item_counts() {
        let mean = weighted_mean_f32_embeddings([(&[0.0f32, 2.0][..], 1), (&[6.0f32, 8.0][..], 3)])
            .expect("weighted mean should succeed");
        assert_eq!(mean, vec![4.5, 6.5]);
    }

    #[test]
    fn fallback_partition_groups_caps_fanout_for_large_v0_6_degenerate_partitions() {
        let groups = fallback_partition_groups(5_000, 64, Some(64))
            .expect("capped fallback groups should succeed");
        assert_eq!(groups.len(), 64);
        assert_eq!(groups.iter().map(Vec::len).sum::<usize>(), 5_000);
        assert!(groups.iter().all(|group| !group.is_empty()));
    }

    #[test]
    fn exact_centroid_child_summary_rejects_descendant_count_overflow() {
        let embedding_spec = EmbeddingSpec {
            dims: 1,
            encoding: "f32le".into(),
        };
        let make_hash = |byte: u8| {
            let mut bytes = [0u8; BlockHash::LEN];
            bytes[0] = byte;
            BlockHash::from_bytes(bytes)
        };
        let children = vec![
            ChildSummaryInput {
                embedding: 1.0f32.to_le_bytes().to_vec(),
                child: make_hash(1),
                level: 0,
                descendant_count: usize::MAX,
            },
            ChildSummaryInput {
                embedding: 1.0f32.to_le_bytes().to_vec(),
                child: make_hash(2),
                level: 0,
                descendant_count: 1,
            },
        ];

        let error = exact_centroid_child_summary(&children, &embedding_spec).unwrap_err();
        assert_eq!(
            error,
            "exact-centroid total descendant count overflowed usize"
        );
    }

    #[test]
    fn eigenvalue_log_bit_profiles_keep_effective_cluster_count_power_of_two() {
        let effective = effective_directional_pca_cluster_count(
            64,
            13,
            7,
            DirectionalPcaAllocationPolicy::EigenvalueLogBits,
        )
        .expect("effective cluster count should succeed");
        assert_eq!(effective, 4);
    }

    #[test]
    fn single_entry_ebcp_rotation_uses_identity_fallback() {
        let (rotation, explained_variance) = fit_ebcp_rotation(&[vec![3.0f32, -2.0f32]])
            .expect("single-entry fallback should succeed");
        assert_eq!(rotation, vec![1.0, 0.0, 0.0, 1.0]);
        assert_eq!(explained_variance, vec![0.0, 0.0]);
    }

    #[test]
    fn root_branch_budget_applies_only_to_single_group_global_root() {
        assert!(uses_root_branch_budget(true, 1));
        assert!(!uses_root_branch_budget(false, 1));
        assert!(!uses_root_branch_budget(true, 2));
    }

    #[test]
    fn variable_bit_widths_respect_protocol_cap_and_preserve_budget() {
        let widths = allocate_variable_bit_widths(&[1_000_000_000.0, 0.0, 0.0, 0.0], 12)
            .expect("allocation should succeed");
        assert_eq!(
            widths
                .iter()
                .map(|width| usize::from(*width))
                .sum::<usize>(),
            48
        );
        assert!(widths.iter().all(|width| (1..=31).contains(width)));
        assert_eq!(widths[0], 31);
    }

    #[test]
    fn streaming_v2_topology_stats_reports_missing_children_instead_of_panicking() {
        let result = streaming_v2_topology_stats(&StreamingV2PartitionTopology {
            root_partition_id: "root".into(),
            partitions: vec![StreamingV2Partition {
                id: "root".into(),
                parent_id: None,
                child_ids: vec!["missing".into()],
                item_count: 1,
                terminal: false,
            }],
        });
        let Err(error) = result else {
            panic!("invalid topology should return an error");
        };
        assert_eq!(
            error,
            "v2 partition topology stats referenced missing partition \"missing\""
        );
    }

    #[test]
    fn quantile_planner_state_clear_quantile_pass_removes_axis_files() {
        let root = tempfile::tempdir().expect("test planner state root should exist");
        let mut planner_state =
            StreamingV2QuantilePlannerState::new(&root).expect("planner state should initialize");
        DirectionalPcaOutOfCorePlannerState::begin_quantile_pass(&mut planner_state, 2, 3)
            .expect("quantile pass should initialize");
        for values in [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]] {
            DirectionalPcaOutOfCorePlannerState::append_quantile_values(
                &mut planner_state,
                &values,
            )
            .expect("quantile pass should capture values");
        }
        DirectionalPcaOutOfCorePlannerState::finish_quantile_pass(&mut planner_state)
            .expect("quantile pass should finish");

        let axis_paths = planner_state
            .quantile_pass
            .as_ref()
            .expect("quantile pass should remain available")
            .paths
            .clone();
        assert!(axis_paths.iter().all(|path| path.exists()));

        DirectionalPcaOutOfCorePlannerState::clear_quantile_pass(&mut planner_state)
            .expect("quantile pass should clear");

        assert!(planner_state.quantile_pass.is_none());
        assert!(axis_paths.iter().all(|path| !path.exists()));
    }

    #[test]
    fn quantile_planner_state_begin_quantile_pass_clears_prior_axis_files() {
        let root = tempfile::tempdir().expect("test planner state root should exist");
        let mut planner_state =
            StreamingV2QuantilePlannerState::new(&root).expect("planner state should initialize");
        DirectionalPcaOutOfCorePlannerState::begin_quantile_pass(&mut planner_state, 3, 2)
            .expect("initial quantile pass should initialize");
        let stale_axis_path = planner_state
            .quantile_pass
            .as_ref()
            .expect("quantile pass should remain available")
            .paths[2]
            .clone();
        assert!(stale_axis_path.exists());

        DirectionalPcaOutOfCorePlannerState::begin_quantile_pass(&mut planner_state, 2, 2)
            .expect("replacement quantile pass should initialize");

        assert!(!stale_axis_path.exists());
        let quantile_pass = planner_state
            .quantile_pass
            .as_ref()
            .expect("replacement quantile pass should remain available");
        assert_eq!(quantile_pass.paths.len(), 2);
    }

    #[test]
    fn quantile_planner_state_drop_cleans_up_open_axis_files() {
        let root = tempfile::tempdir().expect("test planner state root should exist");
        let planner_dir_path = {
            let mut planner_state = StreamingV2QuantilePlannerState::new(&root)
                .expect("planner state should initialize");
            DirectionalPcaOutOfCorePlannerState::begin_quantile_pass(&mut planner_state, 2, 3)
                .expect("quantile pass should initialize");
            for values in [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]] {
                DirectionalPcaOutOfCorePlannerState::append_quantile_values(
                    &mut planner_state,
                    &values,
                )
                .expect("quantile pass should capture values");
            }
            planner_state.dir.path().to_path_buf()
        };
        assert!(!planner_dir_path.exists());
    }

    #[test]
    fn quantile_planner_state_begin_quantile_pass_cleans_up_partial_files_on_error() {
        let root = tempfile::tempdir().expect("test planner state root should exist");
        let mut planner_state =
            StreamingV2QuantilePlannerState::new(&root).expect("planner state should initialize");
        let blocked_path = planner_state.dir.path().join("axis-0001.bin");
        std::fs::create_dir(&blocked_path)
            .expect("test should be able to block second axis file creation");

        let error =
            DirectionalPcaOutOfCorePlannerState::begin_quantile_pass(&mut planner_state, 2, 3)
                .unwrap_err();

        assert!(
            error.contains("could not create planner state file"),
            "unexpected error: {error}"
        );
        assert!(planner_state.quantile_pass.is_none());
        assert!(!planner_state.dir.path().join("axis-0000.bin").exists());
        assert!(blocked_path.is_dir());
    }

    #[test]
    fn quantile_planner_state_splits_writer_window_budget_across_axes() {
        let root = tempfile::tempdir().expect("test planner state root should exist");
        let mut planner_state =
            StreamingV2QuantilePlannerState::new(&root).expect("planner state should initialize");
        DirectionalPcaOutOfCorePlannerState::begin_quantile_pass(&mut planner_state, 8, 3)
            .expect("quantile pass should initialize");

        let quantile_pass = planner_state
            .quantile_pass
            .as_ref()
            .expect("quantile pass should remain available");
        assert_eq!(quantile_pass.writers.len(), 8);
        assert!(
            quantile_pass
                .writers
                .iter()
                .all(|writer| writer.writer.capacity() == super::V2_PLANNER_STATE_WINDOW_BYTES / 8)
        );
        assert_eq!(
            quantile_pass
                .writers
                .iter()
                .map(|writer| writer.writer.capacity())
                .sum::<usize>(),
            super::V2_PLANNER_STATE_WINDOW_BYTES
        );
    }

    #[test]
    fn quantile_planner_state_writer_windows_preserve_total_budget() {
        let root = tempfile::tempdir().expect("test planner state root should exist");
        let mut planner_state =
            StreamingV2QuantilePlannerState::new(&root).expect("planner state should initialize");
        let axis_count = 32;
        DirectionalPcaOutOfCorePlannerState::begin_quantile_pass(&mut planner_state, axis_count, 3)
            .expect("quantile pass should initialize");

        let quantile_pass = planner_state
            .quantile_pass
            .as_ref()
            .expect("quantile pass should remain available");
        assert_eq!(quantile_pass.writers.len(), axis_count);
        assert!(
            quantile_pass
                .writers
                .iter()
                .all(|writer| writer.writer.capacity()
                    == super::V2_PLANNER_STATE_WINDOW_BYTES / axis_count)
        );
        assert_eq!(
            quantile_pass
                .writers
                .iter()
                .map(|writer| writer.writer.capacity())
                .sum::<usize>(),
            super::V2_PLANNER_STATE_WINDOW_BYTES
        );
    }

    #[test]
    fn windowed_mmap_reader_rejects_truncated_files() {
        let root = tempfile::tempdir().expect("test planner state root should exist");
        let path = root.path().join("truncated-axis.bin");
        std::fs::write(&path, [0u8; 4]).expect("test should create truncated planner state file");

        let error = match super::WindowedMmapF32Reader::open(&path, 2) {
            Ok(_) => panic!("truncated planner state file should be rejected"),
            Err(error) => error,
        };
        assert!(error.contains("is truncated"), "unexpected error: {error}");
    }

    fn make_streaming_v2_run(
        profile_version: super::PublishedProfileVersion,
    ) -> StreamingIndexingRunV2<(), (), ()> {
        let profile = published_indexing_profile(profile_version).expect("profile should exist");
        let branch_encoding_policy = branch_encoding_policy_for_profile(&profile);
        let planner_state_root = tempfile::tempdir().expect("test planner state root should exist");
        StreamingIndexingRunV2 {
            resolver: (),
            embedding_provider: (),
            observer: None,
            profile,
            branch_encoding_policy,
            embedding_spec: EmbeddingSpec {
                dims: 1,
                encoding: "f32le".into(),
            },
            block_size_target: 256,
            phase: RunPhase::Planning,
            completed_passes: 0,
            baseline_fingerprint: None,
            current_pass: None,
            latest_completed_pass_snapshot: None,
            completed_pass_history: Vec::new(),
            partitions: Vec::new(),
            next_partition_id: 0,
            planner_state_root,
            _item_ref: PhantomData,
        }
    }

    #[test]
    fn unresolved_work_shrank_requires_pending_reduction_or_equal_pending_with_more_resolution() {
        let previous = StreamingV2CompletedPassSnapshot {
            pass_number: 1,
            planned_partition_count: 3,
            terminal_partition_count: 1,
            routed_partition_paths: vec!["p0".into()],
            terminal_partition_paths: vec!["p0.0".into()],
            hierarchy_depth: 2,
            topology_fingerprint_hex: "a".repeat(64),
            pending_partition_fingerprint_hex: "b".repeat(64),
            combined_fingerprint_hex: "c".repeat(64),
            pending_partitions: vec![pending_partition("p0.1"), pending_partition("p0.2")],
        };
        let pending_grew_but_routed_grew = StreamingV2CompletedPassSnapshot {
            pass_number: 2,
            planned_partition_count: 5,
            terminal_partition_count: 1,
            routed_partition_paths: vec!["p0".into(), "p0.1".into()],
            terminal_partition_paths: vec!["p0.0".into()],
            hierarchy_depth: 3,
            topology_fingerprint_hex: "d".repeat(64),
            pending_partition_fingerprint_hex: "e".repeat(64),
            combined_fingerprint_hex: "f".repeat(64),
            pending_partitions: vec![
                pending_partition("p0.1.0"),
                pending_partition("p0.1.1"),
                pending_partition("p0.2"),
            ],
        };
        let equal_pending_and_more_terminal = StreamingV2CompletedPassSnapshot {
            pass_number: 2,
            planned_partition_count: 4,
            terminal_partition_count: 2,
            routed_partition_paths: vec!["p0".into()],
            terminal_partition_paths: vec!["p0.0".into(), "p0.1".into()],
            hierarchy_depth: 2,
            topology_fingerprint_hex: "g".repeat(64),
            pending_partition_fingerprint_hex: "h".repeat(64),
            combined_fingerprint_hex: "i".repeat(64),
            pending_partitions: vec![pending_partition("p0.2"), pending_partition("p0.3")],
        };
        assert!(!unresolved_work_shrank(
            &previous,
            &pending_grew_but_routed_grew
        ));
        assert!(unresolved_work_shrank(
            &previous,
            &equal_pending_and_more_terminal
        ));
    }

    fn pending_partition(path: &str) -> StreamingV2PendingPartitionStatus {
        StreamingV2PendingPartitionStatus {
            partition_path: path.into(),
            expected_item_count: 1,
            observed_replay_progress: None,
            routing_bucket_fill_counts: None,
            trainer_subphase: Some(StreamingIndexingTrainerSubphase::AnalyzePca),
            ready_axis_plan_count: None,
            total_axis_plan_count: None,
            populated_cell_count: None,
            realized_cell_count: None,
            planner_state_fingerprint_hex: "0".repeat(64),
        }
    }

    #[test]
    fn unresolved_blocker_without_progress_stays_unknown() {
        let blocker =
            summarize_streaming_v2_partition_blocker(&StreamingV2PendingPartitionStatus {
                partition_path: "p0".into(),
                expected_item_count: 10,
                observed_replay_progress: None,
                routing_bucket_fill_counts: None,
                trainer_subphase: None,
                ready_axis_plan_count: None,
                total_axis_plan_count: None,
                populated_cell_count: None,
                realized_cell_count: None,
                planner_state_fingerprint_hex: "0".repeat(64),
            });
        assert_eq!(blocker.blocker_kind, super::StreamingV2BlockerKind::Unknown);
        assert!(blocker.blocker_detail.contains("unknown"));
        assert!(!blocker.blocker_detail.contains("observed 0"));
    }

    #[test]
    fn format_partition_label_uses_compact_label_for_orphaned_non_root_partition() {
        let partitions = vec![
            StreamingV2PartitionNode {
                parent_id: None,
                child_ids: Vec::new(),
                item_count: 3,
                terminal: false,
                pending_trainer: None,
                routing: None,
            },
            StreamingV2PartitionNode {
                parent_id: None,
                child_ids: Vec::new(),
                item_count: 1,
                terminal: true,
                pending_trainer: None,
                routing: None,
            },
        ];

        assert_eq!(
            format_partition_label(&partitions, super::PartitionId(1)),
            "p1"
        );
    }

    #[test]
    fn finish_pass_keeps_root_partition_id_counter_stable_on_root_creation_error() {
        let mut run = make_streaming_v2_run(PUBLISHED_PROFILE_V0_1_0);
        run.block_size_target = super::serialized_branch_size(&run.embedding_spec, 2)
            .expect("minimum branch size should serialize");
        let mut current_pass = StreamingV2PassState::new();
        current_pass.fingerprint.observed_count = 3;
        run.current_pass = Some(current_pass);

        let error = run.finish_pass().expect_err("root creation should fail");

        assert!(matches!(
            error,
            super::StreamingIndexerError::ClusteringFailure(_)
        ));
        assert!(run.partitions.is_empty());
        assert_eq!(run.next_partition_id, 0);
    }

    #[test]
    fn create_child_nodes_keeps_partition_id_counter_stable_on_child_creation_error() {
        let mut run = make_streaming_v2_run(PUBLISHED_PROFILE_V0_1_0);
        run.partitions.push(StreamingV2PartitionNode {
            parent_id: None,
            child_ids: Vec::new(),
            item_count: 3,
            terminal: false,
            pending_trainer: None,
            routing: None,
        });
        run.next_partition_id = 1;

        let result = run.create_child_nodes(super::PartitionId(0), vec![3], 2);

        assert!(matches!(
            result,
            Err(super::StreamingIndexerError::ClusteringFailure(_))
        ));
        assert_eq!(run.partitions.len(), 1);
        assert_eq!(run.next_partition_id, 1);
    }
}
