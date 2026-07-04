// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Evaluator-owned streaming clustering benchmark harness layered on:
//!
//! - `docs/research/clustering.md`
//! - `docs/research/clustering_plan.md`
//! - `docs/specs/rust-streaming-clustering-crate/`
//!
//! The evaluator owns comparative benchmark profiles, candidate adapters,
//! provenance, leaf-membership scoring, and scorecard generation without
//! broadening the shared streaming clustering trainer/classifier contract.

mod acceleration;
mod section4;
mod section5;
mod section6;
mod section7;

#[cfg(test)]
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Instant;

use async_trait::async_trait;
use ciborium::value::Value as CborValue;
use half::f16;
use lexongraph_block::{BlockHash, EmbeddingSpec, LeafEntry, Metadata, TypedEntries, into_entries};
use lexongraph_block_store::BlockStore;
use lexongraph_block_store_fs::FilesystemBlockStore;
use lexongraph_block_store_overlay::{OverlayBlockStore, OverlayStoreLayer, PassiveLayer};
use lexongraph_block_store_zip::{ZipBlockStore, ZipBlockStoreInitError};
use lexongraph_dcbc_streaming::{DCBC_STREAMING_SOFTWARE_IDENTITY, DcbcStreamingTrainer};
use lexongraph_directional_pca::{
    DIRECTIONAL_PCA_SOFTWARE_IDENTITY, DirectionalPcaParams, DirectionalPcaStreamingTrainer,
};
use lexongraph_pca_chunking::{
    PCA_CHUNKING_SOFTWARE_IDENTITY, PcaChunkingParams, PcaChunkingStreamingTrainer,
};
use lexongraph_spherical_kmeans::{
    SPHERICAL_KMEANS_SOFTWARE_IDENTITY, SphericalInitializationPolicy, SphericalKmeansParams,
    SphericalKmeansStreamingTrainer,
};
use lexongraph_streaming_clustering::{
    ClusterId, Embedding, MetricDirection, PassReport, StreamingClusterClassifier,
    StreamingClusterTrainer, StreamingClusteringConfig, StreamingClusteringError, TrainerState,
    validate_config, validate_embedding,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

pub use acceleration::{
    ExecutionBackendRequest, ExecutionBackendResolution, ExecutionBackendSelection,
    execution_backend_request, with_execution_backend_request,
};
pub use section4::{
    Section4CorpusFamily, Section4DimensionalityContract, Section4ExperimentTrackContract,
    Section4FrozenContractItem, Section4GeneratedProfile, Section4HarvestEmbeddingAdmissibility,
    Section4HarvestPolicy, Section4HarvestSubsetSelection, Section4MetricContract,
    Section4ProfileSourceSpec, Section4ProfileSpec, Section4ProofSurface,
    Section4QualificationSurface, Section4ScaleTierKind, Section4SuiteManifest,
    Section4SuiteRunArtifacts, Section4SuiteRunCandidateReport, Section4SuiteRunProfileReport,
    Section4SuiteRunReport, Section4SuiteSpec, generate_section4_suite_assets,
    materialize_section4_archive_from_json, render_section4_suite_scorecard,
    render_section4_survivor_decision, resolve_profile_block_store_paths,
    resolve_registered_candidates, resolve_section4_suite_manifest_paths,
    resolve_section4_suite_spec_paths, run_section4_suite, write_section4_suite_artifacts,
};
pub use section5::{
    RegisteredHierarchyStrategy, Section5CampaignArtifacts, Section5CampaignReport,
    Section5DeferredGoalRecord, Section5DepthBoundPolicy, Section5EpsilonPolicy, Section5GateKind,
    Section5GateResult, Section5GateStatus, Section5HierarchyContract, Section5HierarchyEdgeReport,
    Section5HierarchyNodeKind, Section5HierarchyNodeReport, Section5HierarchyStrategyIdentity,
    Section5HierarchyStrategyKind, Section5MetricSemanticsConsistencyResult, Section5PairReport,
    Section5PairRunStatus, Section5RankedPair, emit_section5_campaign_artifacts,
    registered_hierarchy_strategy_names, render_section5_carry_forward_summary,
    render_section5_scorecard, resolve_registered_hierarchy_strategies, run_section5_campaign,
    write_section5_campaign_artifacts,
};
pub use section6::{
    RegisteredSection6SummaryCandidate, Section6CampaignArtifacts, Section6CampaignReport,
    Section6DeferredGoalRecord, Section6GateKind, Section6GateResult, Section6GateStatus,
    Section6MetricSemanticsConsistencyResult, Section6NodeSummaryReport, Section6RankedSummary,
    Section6SummaryCandidateIdentity, Section6SummaryCandidateKind, Section6SummaryContract,
    Section6SummaryReport, Section6SummaryRunStatus, emit_section6_campaign_artifacts,
    registered_section6_summary_candidate_names, render_section6_carry_forward_summary,
    render_section6_scorecard, resolve_registered_section6_summary_candidates,
    run_section6_campaign, write_section6_campaign_artifacts,
};
pub use section7::{
    Section7BeamReport, Section7CampaignArtifacts, Section7CampaignReport,
    Section7DeferredGoalRecord, Section7DesignReport, Section7HeldOutQuery, Section7QueryReport,
    Section7RankedDesign, Section7RunStatus, emit_section7_campaign_artifacts,
    render_section7_carry_forward_summary, render_section7_scorecard, run_section7_campaign,
    write_section7_campaign_artifacts,
};

pub type PassPlan = Vec<Vec<Embedding>>;

pub const DEFAULT_DEFERRED_HIERARCHY_ROUTING_REASON: &str = "full hierarchy, sibling structure, and persisted search routing remain outside the leaf-stage evaluator boundary; this evaluator provides staged leaf-stage evidence toward docs/research/clustering.md rather than narrowing the parent end-state requirements; the future end-to-end evaluator layered on the indexer and search specifications remains a separate later line";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateIdentity {
    pub candidate_id: String,
    pub implementation_label: String,
    pub software_identity: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SharedCandidateConfig {
    pub cluster_count: u32,
    pub dimensions: usize,
    pub balance_constraints: Option<SharedBalanceConstraints>,
    pub random_seed: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SharedBalanceConstraints {
    pub min_cluster_occupancy: Option<u32>,
    pub max_cluster_occupancy: Option<u32>,
    pub max_cluster_size_ratio: Option<f64>,
    pub soft_balance_penalty: Option<f64>,
}

impl SharedCandidateConfig {
    pub fn to_streaming_config(&self) -> StreamingClusteringConfig {
        StreamingClusteringConfig {
            cluster_count: self.cluster_count,
            dimensions: self.dimensions,
            balance_constraints: self.balance_constraints.as_ref().map(|constraints| {
                lexongraph_streaming_clustering::BalanceConstraints {
                    min_cluster_occupancy: constraints.min_cluster_occupancy,
                    max_cluster_occupancy: constraints.max_cluster_occupancy,
                    max_cluster_size_ratio: constraints.max_cluster_size_ratio,
                    soft_balance_penalty: constraints.soft_balance_penalty,
                }
            }),
            random_seed: self.random_seed,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResearchCoverage {
    Direct,
    Proxy,
    Deferred,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBudget {
    pub wall_clock_limit_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObservableMetricDirection {
    LargerIsBetter,
    SmallerIsBetter,
}

impl From<MetricDirection> for ObservableMetricDirection {
    fn from(value: MetricDirection) -> Self {
        match value {
            MetricDirection::LargerIsBetter => Self::LargerIsBetter,
            MetricDirection::SmallerIsBetter => Self::SmallerIsBetter,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ObservablePassReport {
    pub observed_count: usize,
    pub requested_cluster_count: u32,
    pub realized_cluster_count: u32,
    pub quality_metric: f64,
    pub balance_metric: f64,
    pub quality_direction: ObservableMetricDirection,
    pub balance_direction: ObservableMetricDirection,
    pub cluster_ids: Vec<ClusterId>,
}

impl From<PassReport> for ObservablePassReport {
    fn from(value: PassReport) -> Self {
        Self {
            observed_count: value.observed_count,
            requested_cluster_count: value.requested_cluster_count,
            realized_cluster_count: value.realized_cluster_count,
            quality_metric: value.quality_metric,
            balance_metric: value.balance_metric,
            quality_direction: value.quality_direction.into(),
            balance_direction: value.balance_direction.into(),
            cluster_ids: value.cluster_ids,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LaterPhaseIdentityKind {
    HeldOutQuerySet,
    RoutingWorkload,
    HierarchyArtifact,
    SummaryArtifact,
    PersistenceArtifact,
    ServiceLevelArtifact,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaterPhaseIdentity {
    pub identity_id: String,
    pub label: String,
    pub kind: LaterPhaseIdentityKind,
    pub corpus_id: Option<String>,
    pub scale_tier_id: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_portable_pathbuf",
        deserialize_with = "deserialize_optional_cross_platform_pathbuf"
    )]
    pub asset_path: Option<PathBuf>,
    pub later_evaluation_line: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkProfile {
    pub profile_id: String,
    pub corpus_ids: Vec<String>,
    pub shared_candidate_config: SharedCandidateConfig,
    pub training_passes: Vec<TrainingPassSource>,
    pub probe_workloads: Vec<ProbeWorkload>,
    pub evaluation_entities: EvaluationEntitySource,
    pub leaf_model: LeafModel,
    pub locality_ground_truth: Vec<GroundTruthNeighborhood>,
    pub compression_benchmark: CompressionBenchmark,
    pub metric_declarations: Vec<MetricDeclaration>,
    pub gate_declarations: Vec<GateDeclaration>,
    #[serde(default = "default_packing_strategy_ids")]
    pub packing_strategy_ids: Vec<String>,
    pub deferred_research_goals: Vec<DeferredResearchGoal>,
    #[serde(default)]
    pub later_phase_identities: Vec<LaterPhaseIdentity>,
    pub reproducibility: ReproducibilityMetadata,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BlockStoreCorpusReference {
    pub source_id: String,
    pub root_block_id: String,
    #[serde(flatten)]
    pub store: BlockStoreReferenceStore,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "store_kind", rename_all = "kebab-case")]
pub enum BlockStoreReferenceStore {
    Filesystem {
        #[serde(serialize_with = "serialize_portable_pathbuf")]
        store_root: PathBuf,
    },
    ZipArchive {
        #[serde(serialize_with = "serialize_portable_pathbuf")]
        archive_path: PathBuf,
    },
}

pub(crate) fn normalize_cross_platform_path(path: impl AsRef<str>) -> PathBuf {
    let raw = path.as_ref();
    if cfg!(windows) || !raw.contains('\\') || raw.contains('/') || has_windows_drive_prefix(raw) {
        return PathBuf::from(raw);
    }
    PathBuf::from(raw.replace('\\', std::path::MAIN_SEPARATOR_STR))
}

pub(crate) fn deserialize_cross_platform_pathbuf<'de, D>(
    deserializer: D,
) -> Result<PathBuf, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    Ok(normalize_cross_platform_path(raw))
}

pub(crate) fn serialize_portable_pathbuf<S>(path: &Path, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&path.to_string_lossy().replace('\\', "/"))
}

pub(crate) fn deserialize_optional_cross_platform_pathbuf<'de, D>(
    deserializer: D,
) -> Result<Option<PathBuf>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    Ok(raw.map(normalize_cross_platform_path))
}

pub(crate) fn serialize_optional_portable_pathbuf<S>(
    path: &Option<PathBuf>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match path {
        Some(path) => serializer.serialize_some(&path.to_string_lossy().replace('\\', "/")),
        None => serializer.serialize_none(),
    }
}

fn has_windows_drive_prefix(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3 && bytes[1] == b':' && bytes[2] == b'\\' && bytes[0].is_ascii_alphabetic()
}

impl<'de> Deserialize<'de> for BlockStoreReferenceStore {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "kebab-case")]
        enum FilesystemTag {
            Filesystem,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "kebab-case")]
        enum ZipArchiveTag {
            ZipArchive,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LegacyFilesystem {
            #[serde(deserialize_with = "deserialize_cross_platform_pathbuf")]
            store_root: PathBuf,
        }

        #[derive(Deserialize)]
        struct TaggedFilesystem {
            #[serde(rename = "store_kind")]
            _store_kind: FilesystemTag,
            #[serde(deserialize_with = "deserialize_cross_platform_pathbuf")]
            store_root: PathBuf,
        }

        #[derive(Deserialize)]
        struct TaggedZipArchive {
            #[serde(rename = "store_kind")]
            _store_kind: ZipArchiveTag,
            #[serde(deserialize_with = "deserialize_cross_platform_pathbuf")]
            archive_path: PathBuf,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            TaggedFilesystem(TaggedFilesystem),
            TaggedZipArchive(TaggedZipArchive),
            LegacyFilesystem(LegacyFilesystem),
        }

        match Repr::deserialize(deserializer)? {
            Repr::TaggedFilesystem(TaggedFilesystem {
                _store_kind: _,
                store_root,
            })
            | Repr::LegacyFilesystem(LegacyFilesystem { store_root }) => {
                Ok(Self::Filesystem { store_root })
            }
            Repr::TaggedZipArchive(TaggedZipArchive {
                _store_kind: _,
                archive_path,
            }) => Ok(Self::ZipArchive { archive_path }),
        }
    }
}

pub struct FsOverlayZipBlockStore {
    cache_layer: TempDir,
    store: OverlayBlockStore,
}

#[cfg(test)]
thread_local! {
    static TEST_FORCE_TEMP_LAYER_FAILURE: Cell<bool> = const { Cell::new(false) };
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArchiveOverlayStoreError {
    TemporaryLayer(String),
    ArchiveOpen(String),
    ArchiveRead(String),
}

impl fmt::Display for ArchiveOverlayStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TemporaryLayer(message)
            | Self::ArchiveOpen(message)
            | Self::ArchiveRead(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ArchiveOverlayStoreError {}

impl FsOverlayZipBlockStore {
    pub fn new(archive_path: impl AsRef<Path>) -> Result<Self, ArchiveOverlayStoreError> {
        if should_force_temp_layer_failure_for_tests() {
            return Err(ArchiveOverlayStoreError::TemporaryLayer(
                "forced temporary cache-layer failure for tests".into(),
            ));
        }

        let cache_layer = tempfile::tempdir().map_err(|error| {
            ArchiveOverlayStoreError::TemporaryLayer(format!(
                "failed to create temporary filesystem cache layer for archive {}: {error}",
                archive_path.as_ref().display()
            ))
        })?;
        let cache_store = FilesystemBlockStore::new(cache_layer.path()).map_err(|error| {
            ArchiveOverlayStoreError::TemporaryLayer(format!(
                "failed to open temporary filesystem cache layer for archive {}: {error}",
                archive_path.as_ref().display()
            ))
        })?;
        let zip_store =
            ZipBlockStore::new_classified(archive_path.as_ref()).map_err(|error| match error {
                ZipBlockStoreInitError::Open(message) => {
                    ArchiveOverlayStoreError::ArchiveOpen(message)
                }
                ZipBlockStoreInitError::Read(message) => {
                    ArchiveOverlayStoreError::ArchiveRead(message)
                }
            })?;
        let store = OverlayBlockStore::new(vec![
            Box::new(PassiveLayer::cache(cache_store)) as Box<dyn OverlayStoreLayer>,
            Box::new(PassiveLayer::read_only(zip_store)) as Box<dyn OverlayStoreLayer>,
        ])
        .map_err(|error| {
            ArchiveOverlayStoreError::TemporaryLayer(format!(
                "failed to build temporary filesystem-over-zip overlay: {error}"
            ))
        })?;

        Ok(Self { cache_layer, store })
    }

    pub fn cache_layer_path(&self) -> &Path {
        self.cache_layer.path()
    }
}

#[cfg(test)]
fn should_force_temp_layer_failure_for_tests() -> bool {
    TEST_FORCE_TEMP_LAYER_FAILURE.with(|flag| flag.get())
}

#[cfg(not(test))]
fn should_force_temp_layer_failure_for_tests() -> bool {
    false
}

#[async_trait(?Send)]
impl BlockStore for FsOverlayZipBlockStore {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), lexongraph_block_store::BlockStoreError> {
        self.store.put_block_bytes(block_id, block_bytes).await
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, lexongraph_block_store::BlockStoreError> {
        self.store.get_block_bytes(block_id).await
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdStream<'_>, lexongraph_block_store::BlockStoreError>
    {
        self.store.iter_block_ids()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source_kind", rename_all = "kebab-case")]
pub enum TrainingPassSource {
    Inline {
        batches: PassPlan,
    },
    BlockStore {
        corpus: BlockStoreCorpusReference,
        batch_size: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source_kind", rename_all = "kebab-case")]
pub enum EmbeddingWorkloadSource {
    Inline { embeddings: Vec<Embedding> },
    BlockStore { corpus: BlockStoreCorpusReference },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProbeWorkload {
    pub workload_id: String,
    #[serde(flatten)]
    pub source: EmbeddingWorkloadSource,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EvaluationEntity {
    pub entity_id: String,
    pub corpus_id: String,
    pub embedding: Embedding,
    pub synthetic: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BlockStoreEvaluationCorpus {
    pub corpus_id: String,
    pub corpus: BlockStoreCorpusReference,
    pub entity_id_metadata_key: String,
    pub synthetic_metadata_key: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source_kind", rename_all = "kebab-case")]
pub enum EvaluationEntitySource {
    Inline {
        entities: Vec<EvaluationEntity>,
    },
    BlockStore {
        corpora: Vec<BlockStoreEvaluationCorpus>,
    },
}

impl BenchmarkProfile {
    pub fn inline_evaluation_entities(&self) -> Option<&[EvaluationEntity]> {
        match &self.evaluation_entities {
            EvaluationEntitySource::Inline { entities } => Some(entities),
            EvaluationEntitySource::BlockStore { .. } => None,
        }
    }

    pub fn inline_evaluation_entities_mut(&mut self) -> Option<&mut Vec<EvaluationEntity>> {
        match &mut self.evaluation_entities {
            EvaluationEntitySource::Inline { entities } => Some(entities),
            EvaluationEntitySource::BlockStore { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeafModel {
    pub leaf_size: usize,
    pub declared_final_cluster_count: u32,
    pub alignment_policy: AlignmentPolicy,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AlignmentPolicy {
    StrictAlignment,
    DeterministicSyntheticPadding,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GroundTruthNeighborhood {
    pub entity_id: String,
    pub neighbor_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompressionBenchmark {
    pub method: CompressionMethod,
    pub global_baseline_label: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionMethod {
    ScalarQuantization8Bit,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricDeclaration {
    pub metric_id: String,
    pub label: String,
    pub kind: MetricKind,
    pub coverage: ResearchCoverage,
    pub research_goal_ids: Vec<String>,
    pub ranking_weight: f64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricKind {
    SameLeafNeighborhoodCoherence,
    LocalCompressionGain,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GateDeclaration {
    pub gate_id: String,
    pub label: String,
    pub kind: GateKind,
    pub coverage: ResearchCoverage,
    pub research_goal_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum GateKind {
    ExactLeafOccupancy,
    LeafSizeAtLeast { minimum: usize },
    LeafSizeAtMost { maximum: usize },
    CompleteCoverage,
    OneClusterPerEntity,
    NoEmptyDeclaredClusters,
    DeterministicObservableResults,
    ExecutionBudget,
    MetricAtLeast { metric_id: String, minimum: f64 },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeferredResearchGoal {
    pub deferred_id: String,
    pub label: String,
    pub reason: String,
    pub research_goal_ids: Vec<String>,
    pub coverage: ResearchCoverage,
    #[serde(default)]
    pub later_evaluation_line: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReproducibilityMetadata {
    pub seed_policy: String,
    pub software_identity: String,
    pub floating_point_profile: String,
    pub hardware_profile: String,
    #[serde(default = "default_candidate_threading_model")]
    pub candidate_threading_model: String,
    #[serde(default = "default_reduction_order_strategy")]
    pub reduction_order_strategy: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateThreadingProvenance {
    pub declared_model: String,
    pub reduction_order_strategy: String,
    pub effective_mode: String,
    pub effective_thread_count: usize,
}

impl Default for CandidateThreadingProvenance {
    fn default() -> Self {
        Self {
            declared_model: default_candidate_threading_model(),
            reduction_order_strategy: default_reduction_order_strategy(),
            effective_mode: "single-threaded".into(),
            effective_thread_count: 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProvenanceManifest {
    pub profile_id: String,
    pub corpus_ids: Vec<String>,
    pub source_reference_ids: Vec<String>,
    pub candidate_identity: CandidateIdentity,
    pub shared_candidate_config: SharedCandidateConfig,
    pub seed_policy: String,
    pub software_identity: String,
    pub floating_point_profile: String,
    pub hardware_profile: String,
    #[serde(default)]
    pub candidate_threading: CandidateThreadingProvenance,
    #[serde(default)]
    pub execution_backend: ExecutionBackendSelection,
}

fn default_candidate_threading_model() -> String {
    "single-threaded section-4 screening".into()
}

fn default_reduction_order_strategy() -> String {
    "single-thread sequential reduction order".into()
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProbeAssignmentResult {
    pub workload_id: String,
    pub assignments: Vec<ClusterId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeafMembershipRecord {
    pub entity_id: String,
    pub cluster_id: ClusterId,
    pub synthetic: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterOccupancy {
    pub cluster_id: ClusterId,
    pub total_count: usize,
    pub real_count: usize,
    pub synthetic_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyntheticPaddingConcentrationReport {
    pub synthetic_entity_count: usize,
    pub clusters_with_synthetic_entities: usize,
    pub minimum_possible_cluster_count: usize,
    pub satisfies_minimum_concentration: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterOccupancyStats {
    pub mean_total_count: f64,
    pub stddev_total_count: f64,
    pub min_total_count: usize,
    pub max_total_count: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PackingEvaluationReport {
    pub pipeline_id: String,
    pub packer_id: String,
    pub lower_bound: usize,
    pub upper_bound: usize,
    pub packing_elapsed_nanos: u128,
    pub leaf_membership: Vec<LeafMembershipRecord>,
    pub cluster_occupancies: Vec<ClusterOccupancy>,
    pub cluster_occupancy_stats: ClusterOccupancyStats,
    pub metric_results: Vec<MetricResult>,
    pub gate_results: Vec<GateResult>,
    pub survived_required_gates: bool,
    pub ranking_score: Option<f64>,
    pub terminal_failure_code: Option<String>,
    pub terminal_failure_message: Option<String>,
    pub terminal_failure: Option<StructuredFailure>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PrerequisiteCheckResult {
    pub check_id: String,
    pub label: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeterminismReport {
    pub deterministic: bool,
    pub compared_fields: Vec<String>,
    pub mismatch_details: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricResult {
    pub metric_id: String,
    pub label: String,
    pub kind: MetricKind,
    pub coverage: ResearchCoverage,
    pub research_goal_ids: Vec<String>,
    pub ranking_weight: f64,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GateStatus {
    Passed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GateResult {
    pub gate_id: String,
    pub label: String,
    pub coverage: ResearchCoverage,
    pub research_goal_ids: Vec<String>,
    pub status: GateStatus,
    pub observed_value: Option<f64>,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeferredMeasurementStatus {
    Deferred,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeferredResearchGoalResult {
    pub deferred_id: String,
    pub label: String,
    pub reason: String,
    pub research_goal_ids: Vec<String>,
    pub coverage: ResearchCoverage,
    pub status: DeferredMeasurementStatus,
    pub later_evaluation_line: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompressionBucketReport {
    pub cluster_id: ClusterId,
    pub real_entity_count: usize,
    pub reconstruction_error: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompressionAnalysis {
    pub baseline_label: String,
    pub global_real_entity_count: usize,
    pub global_reconstruction_error: f64,
    pub local_reconstruction_error_sum: f64,
    pub reported_gain: f64,
    pub delta_semantics: String,
    pub bucket_reports: Vec<CompressionBucketReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactHygieneEvidence {
    pub comparative_metrics_emitted: bool,
    pub success_shaped_completion_artifacts_emitted: bool,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CandidateRunStatus {
    Succeeded,
    GateFailed,
    CandidateSharedContractFailure,
    CorpusSourceFailure,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StructuredFailure {
    InvalidConfiguration {
        message: String,
    },
    InvalidCorpusSourceReference {
        source_id: String,
        message: String,
    },
    CorpusSourceLoadFailure {
        source_id: String,
        message: String,
    },
    ArchiveSourceOpenFailure {
        source_id: String,
        message: String,
    },
    ArchiveSourceReadFailure {
        source_id: String,
        message: String,
    },
    ArchiveSourceTemporaryLayerFailure {
        source_id: String,
        message: String,
    },
    CandidateSharedContractFailure {
        candidate_id: String,
        message: String,
    },
    GateFailure {
        candidate_id: String,
        gate_id: String,
        message: String,
    },
    DeferredMeasurement {
        candidate_id: String,
        deferred_id: String,
        message: String,
    },
}

impl StructuredFailure {
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::InvalidConfiguration { .. } => "invalid-configuration",
            Self::InvalidCorpusSourceReference { .. } => "invalid-corpus-source-reference",
            Self::CorpusSourceLoadFailure { .. } => "corpus-source-load-failure",
            Self::ArchiveSourceOpenFailure { .. } => "archive-source-open-failure",
            Self::ArchiveSourceReadFailure { .. } => "archive-source-read-failure",
            Self::ArchiveSourceTemporaryLayerFailure { .. } => {
                "archive-source-temporary-layer-failure"
            }
            Self::CandidateSharedContractFailure { .. } => "candidate-shared-contract-failure",
            Self::GateFailure { .. } => "gate-failure",
            Self::DeferredMeasurement { .. } => "deferred-measurement",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CandidateRunReport {
    pub candidate_identity: CandidateIdentity,
    pub provenance: ProvenanceManifest,
    pub prerequisite_checks: Vec<PrerequisiteCheckResult>,
    pub pass_reports: Vec<ObservablePassReport>,
    pub probe_results: Vec<ProbeAssignmentResult>,
    pub leaf_membership: Vec<LeafMembershipRecord>,
    pub cluster_occupancies: Vec<ClusterOccupancy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_occupancy_stats: Option<ClusterOccupancyStats>,
    #[serde(default)]
    pub packing_evaluations: Vec<PackingEvaluationReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packing_evaluation: Option<PackingEvaluationReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_packing_strategy_id: Option<String>,
    pub synthetic_padding_concentration: Option<SyntheticPaddingConcentrationReport>,
    pub determinism: DeterminismReport,
    pub compression_analysis: Option<CompressionAnalysis>,
    pub metric_results: Vec<MetricResult>,
    pub gate_results: Vec<GateResult>,
    pub deferred_research_goals: Vec<DeferredResearchGoalResult>,
    pub artifact_hygiene: ArtifactHygieneEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_budget_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_elapsed_nanos: Option<u128>,
    pub run_status: CandidateRunStatus,
    #[serde(default)]
    pub raw_survived_required_gates: bool,
    pub survived_required_gates: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_ranking_score: Option<f64>,
    pub ranking_score: Option<f64>,
    pub terminal_failure_code: Option<String>,
    pub terminal_failure_message: Option<String>,
    pub terminal_failure: Option<StructuredFailure>,
}

impl CandidateRunReport {
    pub fn effective_leaf_membership(&self) -> &[LeafMembershipRecord] {
        self.packing_evaluation
            .as_ref()
            .map(|packing| packing.leaf_membership.as_slice())
            .unwrap_or(self.leaf_membership.as_slice())
    }

    pub fn effective_cluster_occupancies(&self) -> &[ClusterOccupancy] {
        self.packing_evaluation
            .as_ref()
            .map(|packing| packing.cluster_occupancies.as_slice())
            .unwrap_or(self.cluster_occupancies.as_slice())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RankedCandidate {
    pub candidate_id: String,
    pub ranking_score: f64,
    pub rank: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RankedPackingPipeline {
    pub pipeline_id: String,
    pub candidate_id: String,
    pub packing_strategy_id: String,
    pub ranking_score: f64,
    pub rank: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CampaignReport {
    pub profile_id: String,
    pub run_reports: Vec<CandidateRunReport>,
    pub ranking: Vec<RankedCandidate>,
    #[serde(default)]
    pub raw_ranking: Vec<RankedCandidate>,
    #[serde(default)]
    pub packing_pipeline_ranking: Vec<RankedPackingPipeline>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmittedArtifact {
    pub file_name: String,
    pub contents: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CampaignArtifacts {
    pub per_candidate_reports: Vec<EmittedArtifact>,
    pub campaign_report: EmittedArtifact,
    pub scorecard: EmittedArtifact,
}

#[derive(Debug, PartialEq, Eq)]
pub enum EvaluatorError {
    InvalidConfiguration(String),
    Io(String),
    Json(String),
}

impl fmt::Display for EvaluatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => {
                write!(f, "invalid evaluator configuration: {message}")
            }
            Self::Io(message) => write!(f, "io failure: {message}"),
            Self::Json(message) => write!(f, "json failure: {message}"),
        }
    }
}

impl std::error::Error for EvaluatorError {}

pub struct RegisteredCandidate {
    pub identity: CandidateIdentity,
    factory: Box<dyn CandidateFactory>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackingStrategyIdentity {
    pub strategy_id: String,
    pub label: String,
    pub algorithm_family: String,
}

struct RegisteredPackingStrategy {
    pub identity: PackingStrategyIdentity,
    pack: fn(&SingleExecution, &BenchmarkProfile) -> Result<SingleExecution, String>,
}

pub fn candidate_adapter<F, T>(identity: CandidateIdentity, factory: F) -> RegisteredCandidate
where
    F: Fn(&StreamingClusteringConfig) -> Result<T, StreamingClusteringError>
        + Send
        + Sync
        + 'static,
    T: StreamingClusterTrainer + 'static,
    T::Classifier: Send + Sync + 'static,
{
    RegisteredCandidate {
        identity,
        factory: Box::new(factory),
    }
}

fn default_packing_strategy_ids() -> Vec<String> {
    vec![BALANCED_RANGE_PACKER_ID.into()]
}

pub fn registered_packing_strategy_names() -> Vec<String> {
    vec![
        BALANCED_RANGE_PACKER_ID.into(),
        CONTIGUOUS_SEGMENTATION_PACKER_ID.into(),
        GEOMETRY_SPLIT_MERGE_PACKER_ID.into(),
    ]
}

fn registered_packing_strategy(strategy_id: &str) -> Option<RegisteredPackingStrategy> {
    match strategy_id {
        BALANCED_RANGE_PACKER_ID => Some(RegisteredPackingStrategy {
            identity: PackingStrategyIdentity {
                strategy_id: BALANCED_RANGE_PACKER_ID.into(),
                label: "Cluster-order balanced range packer".into(),
                algorithm_family: "order-preserving-balanced-range".into(),
            },
            pack: build_balanced_range_packed_execution,
        }),
        CONTIGUOUS_SEGMENTATION_PACKER_ID => Some(RegisteredPackingStrategy {
            identity: PackingStrategyIdentity {
                strategy_id: CONTIGUOUS_SEGMENTATION_PACKER_ID.into(),
                label: "Contiguous bounded segmentation packer".into(),
                algorithm_family: "contiguous-bounded-segmentation".into(),
            },
            pack: build_contiguous_segmented_packed_execution,
        }),
        GEOMETRY_SPLIT_MERGE_PACKER_ID => Some(RegisteredPackingStrategy {
            identity: PackingStrategyIdentity {
                strategy_id: GEOMETRY_SPLIT_MERGE_PACKER_ID.into(),
                label: "Geometry-aware split/merge packer".into(),
                algorithm_family: "geometry-aware-split-merge".into(),
            },
            pack: build_geometry_split_merge_packed_execution,
        }),
        #[cfg(test)]
        FAILING_TEST_PACKER_ID => Some(RegisteredPackingStrategy {
            identity: PackingStrategyIdentity {
                strategy_id: FAILING_TEST_PACKER_ID.into(),
                label: "Failing test packer".into(),
                algorithm_family: "test-only".into(),
            },
            pack: failing_test_packed_execution,
        }),
        _ => None,
    }
}

pub fn run_evaluation_campaign(
    profile: &BenchmarkProfile,
    candidates: &[RegisteredCandidate],
) -> Result<CampaignReport, EvaluatorError> {
    validate_profile(profile)?;
    validate_candidates(candidates)?;

    let mut run_reports = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        run_reports.push(run_candidate(profile, candidate));
    }

    let ranking = rank_candidates(&run_reports);
    let raw_ranking = rank_raw_candidates(&run_reports);
    let packing_pipeline_ranking = rank_packing_pipelines(&run_reports);

    Ok(CampaignReport {
        profile_id: profile.profile_id.clone(),
        run_reports,
        ranking,
        raw_ranking,
        packing_pipeline_ranking,
    })
}

pub fn emit_campaign_artifacts(
    report: &CampaignReport,
) -> Result<CampaignArtifacts, EvaluatorError> {
    let mut per_candidate_reports = Vec::with_capacity(report.run_reports.len());
    let mut used_file_names = HashSet::new();
    for run_report in &report.run_reports {
        let safe_candidate_id = sanitize_artifact_stem(&run_report.candidate_identity.candidate_id);
        let file_name =
            unique_artifact_file_name(&mut used_file_names, &safe_candidate_id, "-run-report.json");
        per_candidate_reports.push(EmittedArtifact {
            file_name,
            contents: serde_json::to_string_pretty(run_report)
                .map_err(|error| EvaluatorError::Json(error.to_string()))?,
        });
    }

    let campaign_report = EmittedArtifact {
        file_name: "campaign-report.json".into(),
        contents: serde_json::to_string_pretty(report)
            .map_err(|error| EvaluatorError::Json(error.to_string()))?,
    };

    let scorecard = EmittedArtifact {
        file_name: "scorecard.txt".into(),
        contents: render_scorecard(report),
    };

    Ok(CampaignArtifacts {
        per_candidate_reports,
        campaign_report,
        scorecard,
    })
}

pub fn write_campaign_artifacts(
    output_dir: &Path,
    artifacts: &CampaignArtifacts,
) -> Result<Vec<PathBuf>, EvaluatorError> {
    std::fs::create_dir_all(output_dir).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to create output directory {}: {error}",
            output_dir.display()
        ))
    })?;

    let mut written = Vec::with_capacity(artifacts.per_candidate_reports.len() + 2);
    for artifact in artifacts
        .per_candidate_reports
        .iter()
        .chain([&artifacts.campaign_report, &artifacts.scorecard])
    {
        let path = output_dir.join(&artifact.file_name);
        std::fs::write(&path, &artifact.contents).map_err(|error| {
            EvaluatorError::Io(format!(
                "failed to write artifact {}: {error}",
                path.display()
            ))
        })?;
        written.push(path);
    }

    Ok(written)
}

pub fn render_scorecard(report: &CampaignReport) -> String {
    let mut lines = vec![format!("Campaign scorecard for {}", report.profile_id)];
    if !report.raw_ranking.is_empty() {
        lines.push("Raw clustering-stage ranking:".into());
        for ranked in &report.raw_ranking {
            lines.push(format!(
                "  - rank {}: {} ({:.6})",
                ranked.rank, ranked.candidate_id, ranked.ranking_score
            ));
        }
    }
    if !report.packing_pipeline_ranking.is_empty() {
        lines.push("Packed pipeline ranking:".into());
        for ranked in &report.packing_pipeline_ranking {
            lines.push(format!(
                "  - rank {}: {} via {} ({:.6})",
                ranked.rank, ranked.candidate_id, ranked.packing_strategy_id, ranked.ranking_score
            ));
        }
    }
    for run_report in &report.run_reports {
        let status = match run_report.run_status {
            CandidateRunStatus::Succeeded => "PASS",
            CandidateRunStatus::GateFailed => "GATE-FAILED",
            CandidateRunStatus::CandidateSharedContractFailure => "CONTRACT-FAILED",
            CandidateRunStatus::CorpusSourceFailure => "SOURCE-FAILED",
        };
        let ranking = report
            .ranking
            .iter()
            .find(|ranked| ranked.candidate_id == run_report.candidate_identity.candidate_id)
            .map(|ranked| format!("rank {}", ranked.rank))
            .unwrap_or_else(|| "not ranked".into());
        lines.push(format!(
            "- {} [{}; {}]",
            run_report.candidate_identity.candidate_id, status, ranking
        ));
        lines.push(format!(
            "  execution-backend: {} ({})",
            acceleration::backend_resolution_label(&run_report.provenance.execution_backend),
            run_report.provenance.execution_backend.detail
        ));
        lines.push(format!(
            "  candidate-threading: {} [{} thread(s); {}]",
            run_report.provenance.candidate_threading.effective_mode,
            run_report
                .provenance
                .candidate_threading
                .effective_thread_count,
            run_report
                .provenance
                .candidate_threading
                .reduction_order_strategy
        ));
        for gate in &run_report.gate_results {
            lines.push(format!(
                "  gate {}: {:?} ({})",
                gate.gate_id, gate.status, gate.detail
            ));
        }
        for metric in &run_report.metric_results {
            lines.push(format!(
                "  metric {}: {:.6}",
                metric.metric_id, metric.value
            ));
        }
        if let Some(padding) = &run_report.synthetic_padding_concentration {
            lines.push(format!(
                "  synthetic-padding-concentration: {} synthetic entities across {} cluster(s); minimum possible {} [{}]",
                padding.synthetic_entity_count,
                padding.clusters_with_synthetic_entities,
                padding.minimum_possible_cluster_count,
                if padding.satisfies_minimum_concentration {
                    "PASS"
                } else {
                    "FAIL"
                }
            ));
        }
        if let Some(stats) = &run_report.cluster_occupancy_stats {
            lines.push(format!(
                "  clustering-stage cluster-size-stats: mean={:.3}, stddev={:.3}, min={}, max={}",
                stats.mean_total_count,
                stats.stddev_total_count,
                stats.min_total_count,
                stats.max_total_count
            ));
        }
        if let Some(raw_ranking_score) = run_report.raw_ranking_score {
            lines.push(format!(
                "  clustering-stage ranking_score: {:.6} [{}]",
                raw_ranking_score,
                if run_report.raw_survived_required_gates {
                    "PASS"
                } else {
                    "FAIL"
                }
            ));
        }
        for packing in &run_report.packing_evaluations {
            lines.push(format!(
                "  packing-stage: {} [pipeline={}; bounds={},{}; packing_elapsed_nanos={}{}]",
                packing.packer_id,
                packing.pipeline_id,
                packing.lower_bound,
                packing.upper_bound,
                packing.packing_elapsed_nanos,
                if run_report.selected_packing_strategy_id.as_deref()
                    == Some(packing.packer_id.as_str())
                {
                    "; selected-for-carry-forward"
                } else {
                    ""
                }
            ));
            for gate in &packing.gate_results {
                lines.push(format!(
                    "  packed gate {}: {:?} ({})",
                    gate.gate_id, gate.status, gate.detail
                ));
            }
            for metric in &packing.metric_results {
                lines.push(format!(
                    "  packed metric {}: {:.6}",
                    metric.metric_id, metric.value
                ));
            }
            lines.push(format!(
                "  packed cluster-size-stats: mean={:.3}, stddev={:.3}, min={}, max={}",
                packing.cluster_occupancy_stats.mean_total_count,
                packing.cluster_occupancy_stats.stddev_total_count,
                packing.cluster_occupancy_stats.min_total_count,
                packing.cluster_occupancy_stats.max_total_count
            ));
        }
        for deferred in &run_report.deferred_research_goals {
            lines.push(format!(
                "  deferred {}: {}",
                deferred.deferred_id, deferred.reason
            ));
        }
    }

    lines.join("\n")
}

fn validate_profile(profile: &BenchmarkProfile) -> Result<(), EvaluatorError> {
    if profile.profile_id.trim().is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "profile_id must not be empty".into(),
        ));
    }
    if profile.corpus_ids.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "benchmark profile must declare at least one corpus id".into(),
        ));
    }
    if profile.training_passes.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "benchmark profile must declare at least one training pass".into(),
        ));
    }
    if profile.probe_workloads.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "benchmark profile must declare at least one probe workload".into(),
        ));
    }
    if profile.metric_declarations.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "benchmark profile must declare at least one metric".into(),
        ));
    }
    if profile.gate_declarations.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "benchmark profile must declare at least one gate".into(),
        ));
    }
    if profile.leaf_model.leaf_size == 0 {
        return Err(EvaluatorError::InvalidConfiguration(
            "leaf_size must be positive".into(),
        ));
    }
    if profile.leaf_model.declared_final_cluster_count == 0 {
        return Err(EvaluatorError::InvalidConfiguration(
            "declared_final_cluster_count must be positive".into(),
        ));
    }
    if profile.shared_candidate_config.cluster_count
        != profile.leaf_model.declared_final_cluster_count
    {
        return Err(EvaluatorError::InvalidConfiguration(
            "shared candidate config cluster_count must match leaf model declared_final_cluster_count"
                .into(),
        ));
    }

    validate_config(&profile.shared_candidate_config.to_streaming_config())
        .map_err(|error| EvaluatorError::InvalidConfiguration(error.to_string()))?;

    assert_unique(profile.corpus_ids.iter().map(String::as_str), "corpus ids")?;
    assert_unique(
        profile
            .metric_declarations
            .iter()
            .map(|metric| metric.metric_id.as_str()),
        "metric ids",
    )?;
    assert_unique(
        profile
            .gate_declarations
            .iter()
            .map(|gate| gate.gate_id.as_str()),
        "gate ids",
    )?;
    assert_unique(
        profile
            .deferred_research_goals
            .iter()
            .map(|goal| goal.deferred_id.as_str()),
        "deferred ids",
    )?;
    assert_unique(
        profile
            .later_phase_identities
            .iter()
            .map(|identity| identity.identity_id.as_str()),
        "later-phase identity ids",
    )?;
    assert_unique(
        profile
            .probe_workloads
            .iter()
            .map(|workload| workload.workload_id.as_str()),
        "probe workload ids",
    )?;
    assert_unique(
        profile.packing_strategy_ids.iter().map(String::as_str),
        "packing strategy ids",
    )?;
    assert_unique(
        iter_declared_source_reference_ids(profile),
        "corpus source ids",
    )?;
    if profile.packing_strategy_ids.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "benchmark profile must declare at least one packing strategy id".into(),
        ));
    }
    for strategy_id in &profile.packing_strategy_ids {
        if registered_packing_strategy(strategy_id).is_none() {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "unknown packing strategy {strategy_id}; available packing strategies: {}",
                registered_packing_strategy_names().join(", ")
            )));
        }
    }

    let dimensions = profile.shared_candidate_config.dimensions;
    let corpus_ids = profile
        .corpus_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    for pass in &profile.training_passes {
        match pass {
            TrainingPassSource::Inline { batches } => validate_inline_batches(
                batches,
                dimensions,
                "training embedding",
                "each training pass must contain at least one batch",
                "each training batch must contain at least one embedding",
            )?,
            TrainingPassSource::BlockStore { corpus, batch_size } => {
                validate_corpus_reference(corpus)?;
                if *batch_size == 0 {
                    return Err(EvaluatorError::InvalidConfiguration(format!(
                        "block-store training pass {} must declare a positive batch_size",
                        corpus.source_id
                    )));
                }
            }
        }
    }

    for workload in &profile.probe_workloads {
        match &workload.source {
            EmbeddingWorkloadSource::Inline { embeddings } => {
                for embedding in embeddings {
                    validate_embedding(embedding, dimensions).map_err(|error| {
                        EvaluatorError::InvalidConfiguration(format!(
                            "invalid probe embedding in {}: {error}",
                            workload.workload_id
                        ))
                    })?;
                }
            }
            EmbeddingWorkloadSource::BlockStore { corpus } => validate_corpus_reference(corpus)?,
        }
    }

    match &profile.evaluation_entities {
        EvaluationEntitySource::Inline { entities } => {
            assert_unique(
                entities.iter().map(|entity| entity.entity_id.as_str()),
                "evaluation entity ids",
            )?;
            validate_materialized_evaluation_entities(profile, entities)?;
        }
        EvaluationEntitySource::BlockStore { corpora } => {
            if corpora.is_empty() {
                return Err(EvaluatorError::InvalidConfiguration(
                    "block-store evaluation sources must declare at least one corpus".into(),
                ));
            }
            for corpus in corpora {
                validate_corpus_reference(&corpus.corpus)?;
                if corpus.entity_id_metadata_key.trim().is_empty() {
                    return Err(EvaluatorError::InvalidConfiguration(format!(
                        "block-store evaluation corpus {} must declare entity_id_metadata_key",
                        corpus.corpus.source_id
                    )));
                }
                if let Some(key) = &corpus.synthetic_metadata_key
                    && key.trim().is_empty()
                {
                    return Err(EvaluatorError::InvalidConfiguration(format!(
                        "block-store evaluation corpus {} must not declare an empty synthetic_metadata_key",
                        corpus.corpus.source_id
                    )));
                }
                if matches!(
                    profile.leaf_model.alignment_policy,
                    AlignmentPolicy::DeterministicSyntheticPadding
                ) && corpus.synthetic_metadata_key.is_none()
                {
                    return Err(EvaluatorError::InvalidConfiguration(format!(
                        "block-store evaluation corpus {} must declare synthetic_metadata_key when using deterministic synthetic padding",
                        corpus.corpus.source_id
                    )));
                }
                if !corpus_ids.contains(corpus.corpus_id.as_str()) {
                    return Err(EvaluatorError::InvalidConfiguration(format!(
                        "block-store evaluation corpus {} references unknown corpus {}",
                        corpus.corpus.source_id, corpus.corpus_id
                    )));
                }
            }
        }
    }

    let declared_metric_ids = profile
        .metric_declarations
        .iter()
        .map(|metric| metric.metric_id.as_str())
        .collect::<HashSet<_>>();
    for declaration in &profile.metric_declarations {
        if declaration.research_goal_ids.is_empty() {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "metric {} must trace to at least one research goal",
                declaration.metric_id
            )));
        }
        if !declaration.ranking_weight.is_finite() || declaration.ranking_weight < 0.0 {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "metric {} ranking_weight must be finite and non-negative",
                declaration.metric_id
            )));
        }
    }
    for declaration in &profile.gate_declarations {
        if declaration.research_goal_ids.is_empty() {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "gate {} must trace to at least one research goal",
                declaration.gate_id
            )));
        }
        if let GateKind::MetricAtLeast { metric_id, minimum } = &declaration.kind {
            if !declared_metric_ids.contains(metric_id.as_str()) {
                return Err(EvaluatorError::InvalidConfiguration(format!(
                    "gate {} references unknown metric {}",
                    declaration.gate_id, metric_id
                )));
            }
            if !minimum.is_finite() {
                return Err(EvaluatorError::InvalidConfiguration(format!(
                    "gate {} minimum must be finite",
                    declaration.gate_id
                )));
            }
        }
    }
    for declaration in &profile.deferred_research_goals {
        if declaration.research_goal_ids.is_empty() {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "deferred goal {} must trace to at least one research goal",
                declaration.deferred_id
            )));
        }
        if declaration.later_evaluation_line.trim().is_empty() {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "deferred goal {} must declare later_evaluation_line",
                declaration.deferred_id
            )));
        }
    }
    for identity in &profile.later_phase_identities {
        if identity.identity_id.trim().is_empty() {
            return Err(EvaluatorError::InvalidConfiguration(
                "later-phase identities must declare a non-empty identity_id".into(),
            ));
        }
        if identity.label.trim().is_empty() {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "later-phase identity {} must declare a non-empty label",
                identity.identity_id
            )));
        }
        if identity.later_evaluation_line.trim().is_empty() {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "later-phase identity {} must declare later_evaluation_line",
                identity.identity_id
            )));
        }
        if matches!(identity.kind, LaterPhaseIdentityKind::HeldOutQuerySet)
            && identity.asset_path.is_none()
        {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "held-out query-set identity {} must declare asset_path",
                identity.identity_id
            )));
        }
        if let Some(corpus_id) = &identity.corpus_id
            && !corpus_ids.contains(corpus_id.as_str())
        {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "later-phase identity {} references unknown corpus {}",
                identity.identity_id, corpus_id
            )));
        }
    }

    if profile
        .compression_benchmark
        .global_baseline_label
        .trim()
        .is_empty()
    {
        return Err(EvaluatorError::InvalidConfiguration(
            "compression benchmark global_baseline_label must not be empty".into(),
        ));
    }
    if profile.reproducibility.seed_policy.trim().is_empty()
        || profile.reproducibility.software_identity.trim().is_empty()
        || profile
            .reproducibility
            .floating_point_profile
            .trim()
            .is_empty()
        || profile.reproducibility.hardware_profile.trim().is_empty()
    {
        return Err(EvaluatorError::InvalidConfiguration(
            "reproducibility metadata fields must not be empty".into(),
        ));
    }

    Ok(())
}

fn assert_unique<'a>(
    items: impl Iterator<Item = &'a str>,
    label: &str,
) -> Result<(), EvaluatorError> {
    let mut seen = HashSet::new();
    for item in items {
        if !seen.insert(item) {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "duplicate value in {label}: {item}"
            )));
        }
    }
    Ok(())
}

fn validate_corpus_reference(reference: &BlockStoreCorpusReference) -> Result<(), EvaluatorError> {
    if reference.source_id.trim().is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "block-store corpus references must declare a non-empty source_id".into(),
        ));
    }
    match &reference.store {
        BlockStoreReferenceStore::Filesystem { store_root } => {
            if store_root.as_os_str().is_empty() {
                return Err(EvaluatorError::InvalidConfiguration(format!(
                    "block-store corpus reference {} must declare a non-empty store_root",
                    reference.source_id
                )));
            }
        }
        BlockStoreReferenceStore::ZipArchive { archive_path } => {
            if archive_path.as_os_str().is_empty() {
                return Err(EvaluatorError::InvalidConfiguration(format!(
                    "block-store corpus reference {} must declare a non-empty archive_path",
                    reference.source_id
                )));
            }
        }
    }
    parse_block_hash_hex(&reference.root_block_id).map_err(|message| {
        EvaluatorError::InvalidConfiguration(format!(
            "block-store corpus reference {} has an invalid root_block_id: {message}",
            reference.source_id
        ))
    })?;
    Ok(())
}

fn validate_inline_batches(
    batches: &PassPlan,
    dimensions: usize,
    embedding_label: &str,
    empty_pass_message: &str,
    empty_batch_message: &str,
) -> Result<(), EvaluatorError> {
    if batches.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            empty_pass_message.into(),
        ));
    }
    for batch in batches {
        if batch.is_empty() {
            return Err(EvaluatorError::InvalidConfiguration(
                empty_batch_message.into(),
            ));
        }
        for embedding in batch {
            validate_embedding(embedding, dimensions).map_err(|error| {
                EvaluatorError::InvalidConfiguration(format!("invalid {embedding_label}: {error}"))
            })?;
        }
    }
    Ok(())
}

fn validate_materialized_evaluation_entities(
    profile: &BenchmarkProfile,
    entities: &[EvaluationEntity],
) -> Result<(), EvaluatorError> {
    if entities.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "benchmark profile must declare at least one evaluation entity".into(),
        ));
    }

    let mut synthetic_count = 0usize;
    let corpus_ids = profile
        .corpus_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    for entity in entities {
        validate_embedding(
            &entity.embedding,
            profile.shared_candidate_config.dimensions,
        )
        .map_err(|error| {
            EvaluatorError::InvalidConfiguration(format!(
                "invalid evaluation entity {}: {error}",
                entity.entity_id
            ))
        })?;
        if !corpus_ids.contains(entity.corpus_id.as_str()) {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "evaluation entity {} references unknown corpus {}",
                entity.entity_id, entity.corpus_id
            )));
        }
        if entity.synthetic {
            synthetic_count += 1;
        }
    }

    let expected_total_count = profile
        .leaf_model
        .leaf_size
        .checked_mul(profile.leaf_model.declared_final_cluster_count as usize)
        .ok_or_else(|| {
            EvaluatorError::InvalidConfiguration(
                "leaf_size * declared_final_cluster_count overflowed usize".into(),
            )
        })?;
    if entities.len() != expected_total_count {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "evaluation entity count {} must equal leaf_size * cluster_count {}",
            entities.len(),
            expected_total_count
        )));
    }

    match profile.leaf_model.alignment_policy {
        AlignmentPolicy::StrictAlignment => {
            if synthetic_count != 0 {
                return Err(EvaluatorError::InvalidConfiguration(
                    "strict alignment profiles must not contain synthetic entities".into(),
                ));
            }
        }
        AlignmentPolicy::DeterministicSyntheticPadding => {
            if synthetic_count == 0 {
                return Err(EvaluatorError::InvalidConfiguration(
                    "deterministic synthetic padding profiles must contain synthetic entities"
                        .into(),
                ));
            }
        }
    }

    let real_entity_lookup = entities
        .iter()
        .filter(|entity| !entity.synthetic)
        .map(|entity| entity.entity_id.as_str())
        .collect::<HashSet<_>>();
    for truth in &profile.locality_ground_truth {
        if !real_entity_lookup.contains(truth.entity_id.as_str()) {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "ground truth entity {} must refer to a real evaluation entity",
                truth.entity_id
            )));
        }
        if truth.neighbor_ids.is_empty() {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "ground truth entry {} must list at least one neighbor",
                truth.entity_id
            )));
        }
        for neighbor_id in &truth.neighbor_ids {
            if !real_entity_lookup.contains(neighbor_id.as_str()) {
                return Err(EvaluatorError::InvalidConfiguration(format!(
                    "ground truth neighbor {} must refer to a real evaluation entity",
                    neighbor_id
                )));
            }
        }
    }

    Ok(())
}

fn validate_candidates(candidates: &[RegisteredCandidate]) -> Result<(), EvaluatorError> {
    if candidates.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "at least one candidate must be registered".into(),
        ));
    }

    for candidate in candidates {
        if candidate.identity.candidate_id.trim().is_empty() {
            return Err(EvaluatorError::InvalidConfiguration(
                "registered candidate_id must not be empty".into(),
            ));
        }
    }

    assert_unique(
        candidates
            .iter()
            .map(|candidate| candidate.identity.candidate_id.as_str()),
        "candidate ids",
    )
}

fn sanitize_artifact_stem(input: &str) -> String {
    let sanitized = input
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('.')
        .trim_matches('_')
        .to_string();

    if sanitized.is_empty() {
        "candidate".into()
    } else {
        sanitized
    }
}

fn unique_artifact_file_name(
    used_file_names: &mut HashSet<String>,
    stem: &str,
    suffix: &str,
) -> String {
    let mut index = 0usize;
    loop {
        let candidate = if index == 0 {
            format!("{stem}{suffix}")
        } else {
            format!("{stem}-{index}{suffix}")
        };
        if used_file_names.insert(candidate.clone()) {
            return candidate;
        }
        index += 1;
    }
}

fn run_candidate(
    profile: &BenchmarkProfile,
    candidate: &RegisteredCandidate,
) -> CandidateRunReport {
    let resolved = match resolve_profile_inputs(profile) {
        Ok(resolved) => resolved,
        Err(CandidateExecutionError::Candidate(error)) => {
            return failed_candidate_run(profile, &candidate.identity, error);
        }
        Err(CandidateExecutionError::CorpusSource(failure)) => {
            return failed_corpus_source_run(profile, &candidate.identity, failure);
        }
    };
    match (
        execute_candidate_once(profile, candidate, &resolved),
        execute_candidate_once(profile, candidate, &resolved),
    ) {
        (Ok(primary), Ok(repeated)) => {
            finalize_successful_run(profile, &candidate.identity, primary, repeated)
        }
        (Err(CandidateExecutionError::Candidate(error)), _)
        | (_, Err(CandidateExecutionError::Candidate(error))) => {
            failed_candidate_run(profile, &candidate.identity, error)
        }
        (Err(CandidateExecutionError::CorpusSource(failure)), _)
        | (_, Err(CandidateExecutionError::CorpusSource(failure))) => {
            failed_corpus_source_run(profile, &candidate.identity, failure)
        }
    }
}

fn failed_candidate_run(
    profile: &BenchmarkProfile,
    identity: &CandidateIdentity,
    error: StreamingClusteringError,
) -> CandidateRunReport {
    let provenance = build_provenance(profile, identity, declared_source_reference_ids(profile));
    let terminal_failure = StructuredFailure::CandidateSharedContractFailure {
        candidate_id: identity.candidate_id.clone(),
        message: error.to_string(),
    };
    CandidateRunReport {
        candidate_identity: identity.clone(),
        provenance,
        prerequisite_checks: vec![PrerequisiteCheckResult {
            check_id: "shared-contract-execution".into(),
            label: "Shared contract execution".into(),
            passed: false,
            detail: error.to_string(),
        }],
        pass_reports: Vec::new(),
        probe_results: Vec::new(),
        leaf_membership: Vec::new(),
        cluster_occupancies: Vec::new(),
        cluster_occupancy_stats: None,
        packing_evaluations: Vec::new(),
        packing_evaluation: None,
        selected_packing_strategy_id: None,
        synthetic_padding_concentration: None,
        determinism: DeterminismReport {
            deterministic: false,
            compared_fields: determinism_compared_fields(),
            mismatch_details: vec!["candidate execution did not complete".into()],
        },
        compression_analysis: None,
        metric_results: Vec::new(),
        gate_results: Vec::new(),
        deferred_research_goals: profile
            .deferred_research_goals
            .iter()
            .map(|goal| DeferredResearchGoalResult {
                deferred_id: goal.deferred_id.clone(),
                label: goal.label.clone(),
                reason: goal.reason.clone(),
                research_goal_ids: goal.research_goal_ids.clone(),
                coverage: goal.coverage.clone(),
                status: DeferredMeasurementStatus::Deferred,
                later_evaluation_line: goal.later_evaluation_line.clone(),
            })
            .collect(),
        artifact_hygiene: ArtifactHygieneEvidence {
            comparative_metrics_emitted: false,
            success_shaped_completion_artifacts_emitted: false,
            detail:
                "candidate execution failed before comparative metrics or success-shaped completion artifacts could be emitted"
                    .into(),
        },
        execution_budget_millis: None,
        observed_elapsed_nanos: None,
        run_status: CandidateRunStatus::CandidateSharedContractFailure,
        raw_survived_required_gates: false,
        survived_required_gates: false,
        raw_ranking_score: None,
        ranking_score: None,
        terminal_failure_code: Some(terminal_failure.error_code().into()),
        terminal_failure_message: Some(structured_failure_detail(&terminal_failure)),
        terminal_failure: Some(terminal_failure),
    }
}

fn failed_corpus_source_run(
    profile: &BenchmarkProfile,
    identity: &CandidateIdentity,
    failure: StructuredFailure,
) -> CandidateRunReport {
    let provenance = build_provenance(profile, identity, declared_source_reference_ids(profile));
    let terminal_failure_code = failure.error_code().to_string();
    let terminal_failure_message = structured_failure_detail(&failure);
    CandidateRunReport {
        candidate_identity: identity.clone(),
        provenance,
        prerequisite_checks: vec![PrerequisiteCheckResult {
            check_id: "corpus-source-resolution".into(),
            label: "Corpus source resolution".into(),
            passed: false,
            detail: structured_failure_detail(&failure),
        }],
        pass_reports: Vec::new(),
        probe_results: Vec::new(),
        leaf_membership: Vec::new(),
        cluster_occupancies: Vec::new(),
        cluster_occupancy_stats: None,
        packing_evaluations: Vec::new(),
        packing_evaluation: None,
        selected_packing_strategy_id: None,
        synthetic_padding_concentration: None,
        determinism: DeterminismReport {
            deterministic: false,
            compared_fields: determinism_compared_fields(),
            mismatch_details: vec!["corpus source resolution did not complete".into()],
        },
        compression_analysis: None,
        metric_results: Vec::new(),
        gate_results: Vec::new(),
        deferred_research_goals: profile
            .deferred_research_goals
            .iter()
            .map(|goal| DeferredResearchGoalResult {
                deferred_id: goal.deferred_id.clone(),
                label: goal.label.clone(),
                reason: goal.reason.clone(),
                research_goal_ids: goal.research_goal_ids.clone(),
                coverage: goal.coverage.clone(),
                status: DeferredMeasurementStatus::Deferred,
                later_evaluation_line: goal.later_evaluation_line.clone(),
            })
            .collect(),
        artifact_hygiene: ArtifactHygieneEvidence {
            comparative_metrics_emitted: false,
            success_shaped_completion_artifacts_emitted: false,
            detail:
                "corpus-source resolution failed before comparative metrics or success-shaped completion artifacts could be emitted"
                    .into(),
        },
        execution_budget_millis: None,
        observed_elapsed_nanos: None,
        run_status: CandidateRunStatus::CorpusSourceFailure,
        raw_survived_required_gates: false,
        survived_required_gates: false,
        raw_ranking_score: None,
        ranking_score: None,
        terminal_failure_code: Some(terminal_failure_code),
        terminal_failure_message: Some(terminal_failure_message),
        terminal_failure: Some(failure),
    }
}

fn finalize_successful_run(
    profile: &BenchmarkProfile,
    identity: &CandidateIdentity,
    primary: SingleExecution,
    repeated: SingleExecution,
) -> CandidateRunReport {
    let determinism = compare_executions(&primary, &repeated);
    let cluster_occupancy_stats = compute_cluster_occupancy_stats(&primary.cluster_occupancies);
    let synthetic_padding_concentration =
        compute_synthetic_padding_concentration(&primary.cluster_occupancies, profile);
    let raw_hard_gate_results = compute_gate_results_with_filter(
        profile,
        &primary,
        &[],
        &determinism,
        is_clustering_stage_hard_gate_kind,
    );
    let failed_raw_hard_gate = raw_hard_gate_results
        .iter()
        .find(|gate| gate.status == GateStatus::Failed)
        .cloned();
    let raw_hard_gate_failed = failed_raw_hard_gate.is_some();
    let compression_analysis = if raw_hard_gate_failed {
        None
    } else {
        compute_compression_analysis(
            &primary.leaf_membership,
            &primary.evaluation_entities,
            &profile.compression_benchmark,
        )
    };
    let (metric_results, gate_results) = if raw_hard_gate_failed {
        (Vec::new(), raw_hard_gate_results)
    } else {
        let metric_results =
            compute_metric_results(&primary, profile, compression_analysis.as_ref());
        let gate_results = compute_gate_results_with_filter(
            profile,
            &primary,
            &metric_results,
            &determinism,
            is_clustering_stage_visible_gate_kind,
        );
        (metric_results, gate_results)
    };
    let raw_required_gate_status =
        evaluate_required_gate_status(&gate_results, CLUSTERING_STAGE_REQUIRED_GATE_IDS);
    let raw_survived_required_gates = raw_required_gate_status == Some(true);
    let ranking_score = if raw_survived_required_gates {
        Some(
            metric_results
                .iter()
                .map(|metric| metric.value * metric.ranking_weight)
                .sum(),
        )
    } else {
        None
    };
    let raw_terminal_failure = if raw_survived_required_gates {
        None
    } else {
        match failed_raw_hard_gate {
            Some(failed_gate) => Some(StructuredFailure::GateFailure {
                candidate_id: identity.candidate_id.clone(),
                gate_id: failed_gate.gate_id,
                message: failed_gate.detail,
            }),
            None => gate_results
                .iter()
                .find(|gate| {
                    is_clustering_stage_required_gate_id(gate.gate_id.as_str())
                        && gate.status == GateStatus::Failed
                })
                .cloned()
                .map(|failed_gate| StructuredFailure::GateFailure {
                    candidate_id: identity.candidate_id.clone(),
                    gate_id: failed_gate.gate_id,
                    message: failed_gate.detail,
                })
                .or_else(|| {
                    Some(missing_required_gate_failure(
                        identity.candidate_id.as_str(),
                        "clustering-stage",
                        &gate_results,
                        CLUSTERING_STAGE_REQUIRED_GATE_IDS,
                    ))
                }),
        }
    };
    let packing_evaluations = if raw_hard_gate_failed {
        Vec::new()
    } else {
        profile
            .packing_strategy_ids
            .iter()
            .filter_map(|strategy_id| registered_packing_strategy(strategy_id))
            .map(|strategy| {
                evaluate_packing_stage(
                    profile,
                    identity,
                    &primary,
                    &repeated,
                    identity.candidate_id.as_str(),
                    &strategy,
                )
            })
            .collect::<Vec<_>>()
    };
    let packing_evaluation = select_best_packing_evaluation(&packing_evaluations);
    let survived_required_gates = if raw_hard_gate_failed {
        raw_survived_required_gates
    } else {
        packing_evaluation.is_some()
    };
    let effective_ranking_score = packing_evaluation
        .as_ref()
        .and_then(|packing| packing.ranking_score)
        .or(ranking_score);
    let terminal_failure = if survived_required_gates {
        None
    } else {
        packing_evaluation
            .as_ref()
            .and_then(|packing| packing.terminal_failure.clone())
            .or_else(|| {
                first_failed_packing_evaluation(&packing_evaluations)
                    .and_then(|packing| packing.terminal_failure.clone())
            })
            .or(raw_terminal_failure)
    };
    let artifact_hygiene = if raw_hard_gate_failed {
        ArtifactHygieneEvidence {
            comparative_metrics_emitted: false,
            success_shaped_completion_artifacts_emitted: false,
            detail:
                "a hard invariant gate failed, so later comparative metrics and success-shaped completion artifacts were not emitted"
                    .into(),
        }
    } else if survived_required_gates {
        ArtifactHygieneEvidence {
            comparative_metrics_emitted: !metric_results.is_empty(),
            success_shaped_completion_artifacts_emitted: true,
            detail:
                "the candidate satisfied the required clustering and packing-stage gates and emitted the full comparative artifact surface"
                    .into(),
        }
    } else {
        ArtifactHygieneEvidence {
            comparative_metrics_emitted: !metric_results.is_empty(),
            success_shaped_completion_artifacts_emitted: false,
            detail:
                "comparative metrics were emitted, but the candidate did not survive the full clustering-plus-packing evaluation surface"
                    .into(),
        }
    };
    let terminal_failure_code = terminal_failure
        .as_ref()
        .map(|failure| failure.error_code().to_string());
    let terminal_failure_message = terminal_failure.as_ref().map(structured_failure_detail);

    CandidateRunReport {
        candidate_identity: identity.clone(),
        provenance: primary.provenance,
        prerequisite_checks: vec![PrerequisiteCheckResult {
            check_id: "shared-contract-execution".into(),
            label: "Shared contract execution".into(),
            passed: true,
            detail: "candidate completed the shared trainer/classifier lifecycle".into(),
        }],
        pass_reports: primary.pass_reports,
        probe_results: primary.probe_results,
        leaf_membership: primary.leaf_membership,
        cluster_occupancies: primary.cluster_occupancies,
        cluster_occupancy_stats: Some(cluster_occupancy_stats),
        packing_evaluations,
        packing_evaluation: packing_evaluation.clone(),
        selected_packing_strategy_id: packing_evaluation
            .as_ref()
            .map(|packing| packing.packer_id.clone()),
        synthetic_padding_concentration,
        determinism,
        compression_analysis,
        metric_results,
        gate_results,
        deferred_research_goals: profile
            .deferred_research_goals
            .iter()
            .map(|goal| DeferredResearchGoalResult {
                deferred_id: goal.deferred_id.clone(),
                label: goal.label.clone(),
                reason: goal.reason.clone(),
                research_goal_ids: goal.research_goal_ids.clone(),
                coverage: goal.coverage.clone(),
                status: DeferredMeasurementStatus::Deferred,
                later_evaluation_line: goal.later_evaluation_line.clone(),
            })
            .collect(),
        artifact_hygiene,
        execution_budget_millis: None,
        observed_elapsed_nanos: None,
        run_status: if survived_required_gates {
            CandidateRunStatus::Succeeded
        } else {
            CandidateRunStatus::GateFailed
        },
        raw_survived_required_gates,
        survived_required_gates,
        raw_ranking_score: ranking_score,
        ranking_score: effective_ranking_score,
        terminal_failure_code,
        terminal_failure_message,
        terminal_failure,
    }
}

fn packing_bounds(profile: &BenchmarkProfile) -> (usize, usize) {
    (
        profile.leaf_model.leaf_size / 2,
        profile.leaf_model.leaf_size,
    )
}

fn is_clustering_stage_hard_gate_kind(kind: &GateKind) -> bool {
    matches!(
        kind,
        GateKind::CompleteCoverage
            | GateKind::OneClusterPerEntity
            | GateKind::DeterministicObservableResults
    )
}

fn is_clustering_stage_visible_gate_kind(kind: &GateKind) -> bool {
    !matches!(
        kind,
        GateKind::LeafSizeAtLeast { .. }
            | GateKind::LeafSizeAtMost { .. }
            | GateKind::ExecutionBudget
    )
}

const CLUSTERING_STAGE_REQUIRED_GATE_IDS: &[&str] = &[
    "complete-coverage",
    "one-cluster-per-entity",
    "deterministic-observable-results",
];

const PACKING_STAGE_REQUIRED_GATE_IDS: &[&str] = &[
    "leaf-size-lower-bound",
    "leaf-size-upper-bound",
    "complete-coverage",
    "one-cluster-per-entity",
    "deterministic-observable-results",
];

fn is_clustering_stage_required_gate_id(gate_id: &str) -> bool {
    CLUSTERING_STAGE_REQUIRED_GATE_IDS.contains(&gate_id)
}

fn is_packing_stage_visible_gate_kind(kind: &GateKind) -> bool {
    !matches!(
        kind,
        GateKind::ExactLeafOccupancy
            | GateKind::NoEmptyDeclaredClusters
            | GateKind::ExecutionBudget
    )
}

fn is_packing_stage_required_gate_id(gate_id: &str) -> bool {
    PACKING_STAGE_REQUIRED_GATE_IDS.contains(&gate_id)
}

fn evaluate_required_gate_status(
    gate_results: &[GateResult],
    required_gate_ids: &[&str],
) -> Option<bool> {
    let mut observed_required_gate_ids = BTreeMap::<&str, ()>::new();
    for gate in gate_results {
        if required_gate_ids.contains(&gate.gate_id.as_str()) {
            if gate.status != GateStatus::Passed {
                return Some(false);
            }
            observed_required_gate_ids.insert(gate.gate_id.as_str(), ());
        }
    }
    (observed_required_gate_ids.len() == required_gate_ids.len()).then_some(true)
}

fn missing_required_gate_failure(
    candidate_id: &str,
    stage_label: &str,
    gate_results: &[GateResult],
    required_gate_ids: &[&str],
) -> StructuredFailure {
    let missing_gate_ids = required_gate_ids
        .iter()
        .copied()
        .filter(|required_gate_id| {
            !gate_results
                .iter()
                .any(|gate| gate.gate_id.as_str() == *required_gate_id)
        })
        .collect::<Vec<_>>();
    StructuredFailure::InvalidConfiguration {
        message: format!(
            "candidate {candidate_id} did not emit the full required {stage_label} gate set; missing [{}] from required [{}]",
            missing_gate_ids.join(", "),
            required_gate_ids.join(", ")
        ),
    }
}

const BALANCED_RANGE_PACKER_ID: &str = "cluster-order-balanced-range-packer-v1";
const CONTIGUOUS_SEGMENTATION_PACKER_ID: &str = "contiguous-bounded-segmentation-packer-v1";
const GEOMETRY_SPLIT_MERGE_PACKER_ID: &str = "geometry-aware-split-merge-packer-v1";
#[cfg(test)]
const FAILING_TEST_PACKER_ID: &str = "failing-test-packer";

fn ordered_members_by_cluster(execution: &SingleExecution) -> Vec<LeafMembershipRecord> {
    let mut members_by_cluster = execution.leaf_membership.iter().cloned().fold(
        BTreeMap::<ClusterId, Vec<LeafMembershipRecord>>::new(),
        |mut acc, member| {
            acc.entry(member.cluster_id).or_default().push(member);
            acc
        },
    );
    for members in members_by_cluster.values_mut() {
        members.sort_by(|left, right| {
            left.entity_id
                .cmp(&right.entity_id)
                .then_with(|| left.synthetic.cmp(&right.synthetic))
        });
    }
    members_by_cluster.into_values().flatten().collect()
}

fn materialize_packed_execution(
    execution: &SingleExecution,
    packed_groups: Vec<Vec<LeafMembershipRecord>>,
) -> SingleExecution {
    let cluster_count =
        ClusterId::try_from(packed_groups.len()).expect("packed group count exceeds ClusterId");
    let packed_membership = packed_groups
        .into_iter()
        .enumerate()
        .flat_map(|(cluster_id, members)| {
            members.into_iter().map(move |member| LeafMembershipRecord {
                entity_id: member.entity_id,
                cluster_id: cluster_id as ClusterId,
                synthetic: member.synthetic,
            })
        })
        .collect::<Vec<_>>();
    let cluster_occupancies = compute_cluster_occupancies(cluster_count, &packed_membership);
    SingleExecution {
        provenance: execution.provenance.clone(),
        pass_reports: execution.pass_reports.clone(),
        probe_results: execution.probe_results.clone(),
        leaf_membership: packed_membership,
        cluster_occupancies,
        evaluation_entities: execution.evaluation_entities.clone(),
    }
}

fn build_balanced_range_packed_execution(
    execution: &SingleExecution,
    profile: &BenchmarkProfile,
) -> Result<SingleExecution, String> {
    let (_, upper_bound) = packing_bounds(profile);
    let ordered_members = ordered_members_by_cluster(execution);
    if ordered_members.is_empty() {
        return Ok(materialize_packed_execution(execution, Vec::new()));
    }
    let packed_cluster_count = ordered_members.len().div_ceil(upper_bound);
    let packed_cluster_sizes = balanced_cluster_counts(ordered_members.len(), packed_cluster_count);
    let mut offset = 0usize;
    let mut packed_groups = Vec::with_capacity(packed_cluster_sizes.len());
    for cluster_size in packed_cluster_sizes {
        packed_groups.push(ordered_members[offset..offset + cluster_size].to_vec());
        offset += cluster_size;
    }
    Ok(materialize_packed_execution(execution, packed_groups))
}

fn build_contiguous_segmented_packed_execution(
    execution: &SingleExecution,
    profile: &BenchmarkProfile,
) -> Result<SingleExecution, String> {
    let (lower_bound, upper_bound) = packing_bounds(profile);
    let ordered_members = ordered_members_by_cluster(execution);
    let raw_cluster_ids = ordered_members
        .iter()
        .map(|member| member.cluster_id)
        .collect::<Vec<_>>();
    let cuts = partition_contiguous_member_sequence(&raw_cluster_ids, lower_bound, upper_bound)?;
    let mut packed_groups = Vec::with_capacity(cuts.len());
    let mut start = 0usize;
    for end in cuts {
        packed_groups.push(ordered_members[start..end].to_vec());
        start = end;
    }
    Ok(materialize_packed_execution(execution, packed_groups))
}

#[derive(Clone)]
struct PackingMicroCluster {
    original_cluster_id: ClusterId,
    members: Vec<LeafMembershipRecord>,
    centroid: Vec<f32>,
}

fn build_geometry_split_merge_packed_execution(
    execution: &SingleExecution,
    profile: &BenchmarkProfile,
) -> Result<SingleExecution, String> {
    let (lower_bound, upper_bound) = packing_bounds(profile);
    let entity_embeddings = execution
        .evaluation_entities
        .iter()
        .map(|entity| (entity.entity_id.as_str(), entity.embedding.as_slice()))
        .collect::<HashMap<_, _>>();
    let mut raw_clusters = execution.leaf_membership.iter().cloned().fold(
        BTreeMap::<ClusterId, Vec<LeafMembershipRecord>>::new(),
        |mut acc, member| {
            acc.entry(member.cluster_id).or_default().push(member);
            acc
        },
    );
    let mut micro_clusters = Vec::new();
    for (cluster_id, members) in &mut raw_clusters {
        members.sort_by(|left, right| left.entity_id.cmp(&right.entity_id));
        if members.len() <= upper_bound {
            micro_clusters.push(PackingMicroCluster {
                original_cluster_id: *cluster_id,
                centroid: centroid_for_members(members, &entity_embeddings)?,
                members: members.clone(),
            });
            continue;
        }
        let dimension = dominant_embedding_dimension(members, &entity_embeddings)?;
        members.sort_by(|left, right| {
            entity_embeddings[left.entity_id.as_str()][dimension]
                .partial_cmp(&entity_embeddings[right.entity_id.as_str()][dimension])
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.entity_id.cmp(&right.entity_id))
        });
        let chunk_count = members.len().div_ceil(upper_bound);
        let chunk_sizes = balanced_cluster_counts(members.len(), chunk_count);
        let mut offset = 0usize;
        for chunk_size in chunk_sizes {
            let chunk_members = members[offset..offset + chunk_size].to_vec();
            micro_clusters.push(PackingMicroCluster {
                original_cluster_id: *cluster_id,
                centroid: centroid_for_members(&chunk_members, &entity_embeddings)?,
                members: chunk_members,
            });
            offset += chunk_size;
        }
    }
    let global_dimension = dominant_centroid_dimension(&micro_clusters);
    micro_clusters.sort_by(|left, right| {
        left.centroid[global_dimension]
            .partial_cmp(&right.centroid[global_dimension])
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.original_cluster_id.cmp(&right.original_cluster_id))
            .then_with(|| left.members[0].entity_id.cmp(&right.members[0].entity_id))
    });
    let weights = micro_clusters
        .iter()
        .map(|cluster| cluster.members.len())
        .collect::<Vec<_>>();
    let segments = partition_weighted_microclusters(&weights, lower_bound, upper_bound)?;
    let mut packed_groups = Vec::with_capacity(segments.len());
    let mut start = 0usize;
    for end in segments {
        let members = micro_clusters[start..end]
            .iter()
            .flat_map(|cluster| cluster.members.iter().cloned())
            .collect::<Vec<_>>();
        packed_groups.push(members);
        start = end;
    }
    Ok(materialize_packed_execution(execution, packed_groups))
}

#[cfg(test)]
fn failing_test_packed_execution(
    _execution: &SingleExecution,
    _profile: &BenchmarkProfile,
) -> Result<SingleExecution, String> {
    Err("intentional test-only packing failure".into())
}

fn evaluate_packing_stage(
    profile: &BenchmarkProfile,
    _identity: &CandidateIdentity,
    primary: &SingleExecution,
    repeated: &SingleExecution,
    candidate_id: &str,
    strategy: &RegisteredPackingStrategy,
) -> PackingEvaluationReport {
    let (lower_bound, upper_bound) = packing_bounds(profile);
    let started = Instant::now();
    let packed_primary = (strategy.pack)(primary, profile);
    let packed_repeated = if packed_primary.is_ok() {
        (strategy.pack)(repeated, profile)
    } else {
        Err("primary packing execution failed".into())
    };
    let packing_elapsed_nanos = started.elapsed().as_nanos();
    let pipeline_id = packing_pipeline_id(candidate_id, strategy.identity.strategy_id.as_str());
    let (packed_primary, packed_repeated) = match (packed_primary, packed_repeated) {
        (Ok(primary), Ok(repeated)) => (primary, repeated),
        (Err(message), _) | (_, Err(message)) => {
            let failure = StructuredFailure::InvalidConfiguration {
                message: format!(
                    "packing strategy {} failed for candidate {candidate_id}: {message}",
                    strategy.identity.strategy_id
                ),
            };
            return PackingEvaluationReport {
                pipeline_id,
                packer_id: strategy.identity.strategy_id.clone(),
                lower_bound,
                upper_bound,
                packing_elapsed_nanos,
                leaf_membership: Vec::new(),
                cluster_occupancies: Vec::new(),
                cluster_occupancy_stats: ClusterOccupancyStats {
                    mean_total_count: 0.0,
                    stddev_total_count: 0.0,
                    min_total_count: 0,
                    max_total_count: 0,
                },
                metric_results: Vec::new(),
                gate_results: Vec::new(),
                survived_required_gates: false,
                ranking_score: None,
                terminal_failure_code: Some(failure.error_code().to_string()),
                terminal_failure_message: Some(structured_failure_detail(&failure)),
                terminal_failure: Some(failure),
            };
        }
    };
    let determinism = compare_executions(&packed_primary, &packed_repeated);
    let compression_analysis = compute_compression_analysis(
        &packed_primary.leaf_membership,
        &packed_primary.evaluation_entities,
        &profile.compression_benchmark,
    );
    let metric_results =
        compute_metric_results(&packed_primary, profile, compression_analysis.as_ref());
    let gate_results = compute_gate_results_with_filter(
        profile,
        &packed_primary,
        &metric_results,
        &determinism,
        is_packing_stage_visible_gate_kind,
    );
    let required_gate_status =
        evaluate_required_gate_status(&gate_results, PACKING_STAGE_REQUIRED_GATE_IDS);
    let survived_required_gates = required_gate_status == Some(true);
    let ranking_score = if survived_required_gates {
        Some(
            metric_results
                .iter()
                .map(|metric| metric.value * metric.ranking_weight)
                .sum(),
        )
    } else {
        None
    };
    let terminal_failure = if survived_required_gates {
        None
    } else {
        gate_results
            .iter()
            .find(|gate| {
                is_packing_stage_required_gate_id(gate.gate_id.as_str())
                    && gate.status == GateStatus::Failed
            })
            .cloned()
            .map(|failed_gate| StructuredFailure::GateFailure {
                candidate_id: candidate_id.into(),
                gate_id: failed_gate.gate_id,
                message: failed_gate.detail,
            })
            .or_else(|| {
                Some(missing_required_gate_failure(
                    candidate_id,
                    "packing-stage",
                    &gate_results,
                    PACKING_STAGE_REQUIRED_GATE_IDS,
                ))
            })
    };
    let terminal_failure_code = terminal_failure
        .as_ref()
        .map(|failure| failure.error_code().to_string());
    let terminal_failure_message = terminal_failure.as_ref().map(structured_failure_detail);
    PackingEvaluationReport {
        pipeline_id,
        packer_id: strategy.identity.strategy_id.clone(),
        lower_bound,
        upper_bound,
        packing_elapsed_nanos,
        leaf_membership: packed_primary.leaf_membership.clone(),
        cluster_occupancies: packed_primary.cluster_occupancies.clone(),
        cluster_occupancy_stats: compute_cluster_occupancy_stats(
            &packed_primary.cluster_occupancies,
        ),
        metric_results,
        gate_results,
        survived_required_gates,
        ranking_score,
        terminal_failure_code,
        terminal_failure_message,
        terminal_failure,
    }
}

fn partition_contiguous_member_sequence(
    raw_cluster_ids: &[ClusterId],
    lower_bound: usize,
    upper_bound: usize,
) -> Result<Vec<usize>, String> {
    let item_count = raw_cluster_ids.len();
    let mut dp = vec![None::<(usize, usize, usize)>; item_count + 1];
    dp[0] = Some((0, 0, 0));
    for end in 1..=item_count {
        for size in lower_bound..=upper_bound.min(end) {
            let start = end - size;
            let Some((cost, imbalance, _)) = dp[start] else {
                continue;
            };
            let split_penalty =
                usize::from(end < item_count && raw_cluster_ids[end - 1] == raw_cluster_ids[end]);
            let candidate = (
                cost + split_penalty,
                imbalance + upper_bound.abs_diff(size),
                start,
            );
            if dp[end]
                .as_ref()
                .map(|best| candidate.0 < best.0 || (candidate.0 == best.0 && candidate.1 < best.1))
                .unwrap_or(true)
            {
                dp[end] = Some(candidate);
            }
        }
    }
    reconstruct_partition_predecessors(&dp)
}

fn partition_weighted_microclusters(
    weights: &[usize],
    lower_bound: usize,
    upper_bound: usize,
) -> Result<Vec<usize>, String> {
    let item_count = weights.len();
    let mut dp = vec![None::<(usize, usize, usize)>; item_count + 1];
    dp[0] = Some((0, 0, 0));
    for end in 1..=item_count {
        let mut total_weight = 0usize;
        for start in (0..end).rev() {
            total_weight += weights[start];
            if total_weight > upper_bound {
                break;
            }
            if total_weight < lower_bound {
                continue;
            }
            let Some((cost, imbalance, _)) = dp[start] else {
                continue;
            };
            let microcluster_count = end - start;
            let candidate = (
                cost + microcluster_count.saturating_sub(1),
                imbalance + upper_bound.abs_diff(total_weight),
                start,
            );
            if dp[end]
                .as_ref()
                .map(|best| candidate.0 < best.0 || (candidate.0 == best.0 && candidate.1 < best.1))
                .unwrap_or(true)
            {
                dp[end] = Some(candidate);
            }
        }
    }
    reconstruct_partition_predecessors(&dp)
}

fn reconstruct_partition_predecessors(
    predecessors: &[Option<(usize, usize, usize)>],
) -> Result<Vec<usize>, String> {
    if predecessors.is_empty() || predecessors[predecessors.len() - 1].is_none() {
        return Err("no deterministic bounded partition satisfied the requested size range".into());
    }
    let mut cuts = Vec::new();
    let mut cursor = predecessors.len() - 1;
    while cursor > 0 {
        let (_, _, start) = predecessors[cursor].ok_or_else(|| {
            "missing partition predecessor while reconstructing packing".to_string()
        })?;
        cuts.push(cursor);
        cursor = start;
    }
    cuts.reverse();
    Ok(cuts)
}

fn dominant_embedding_dimension(
    members: &[LeafMembershipRecord],
    entity_embeddings: &HashMap<&str, &[f32]>,
) -> Result<usize, String> {
    let first = members
        .first()
        .ok_or_else(|| "cannot derive an embedding dimension from an empty cluster".to_string())?;
    let dimensions = entity_embeddings[first.entity_id.as_str()].len();
    let mut mins = vec![f32::INFINITY; dimensions];
    let mut maxs = vec![f32::NEG_INFINITY; dimensions];
    for member in members {
        for (index, value) in entity_embeddings[member.entity_id.as_str()]
            .iter()
            .enumerate()
        {
            mins[index] = mins[index].min(*value);
            maxs[index] = maxs[index].max(*value);
        }
    }
    Ok((0..dimensions)
        .max_by(|left, right| {
            (maxs[*left] - mins[*left])
                .partial_cmp(&(maxs[*right] - mins[*right]))
                .unwrap_or(Ordering::Equal)
        })
        .unwrap_or(0))
}

fn centroid_for_members(
    members: &[LeafMembershipRecord],
    entity_embeddings: &HashMap<&str, &[f32]>,
) -> Result<Vec<f32>, String> {
    let first = members
        .first()
        .ok_or_else(|| "cannot compute a centroid for an empty cluster".to_string())?;
    let dimensions = entity_embeddings[first.entity_id.as_str()].len();
    let mut centroid = vec![0.0f32; dimensions];
    for member in members {
        for (index, value) in entity_embeddings[member.entity_id.as_str()]
            .iter()
            .enumerate()
        {
            centroid[index] += *value;
        }
    }
    for value in &mut centroid {
        *value /= members.len() as f32;
    }
    Ok(centroid)
}

fn dominant_centroid_dimension(micro_clusters: &[PackingMicroCluster]) -> usize {
    let Some(first) = micro_clusters.first() else {
        return 0;
    };
    let dimensions = first.centroid.len();
    let mut mins = vec![f32::INFINITY; dimensions];
    let mut maxs = vec![f32::NEG_INFINITY; dimensions];
    for cluster in micro_clusters {
        for (index, value) in cluster.centroid.iter().enumerate() {
            mins[index] = mins[index].min(*value);
            maxs[index] = maxs[index].max(*value);
        }
    }
    (0..dimensions)
        .max_by(|left, right| {
            (maxs[*left] - mins[*left])
                .partial_cmp(&(maxs[*right] - mins[*right]))
                .unwrap_or(Ordering::Equal)
        })
        .unwrap_or(0)
}

fn compute_metric_results(
    execution: &SingleExecution,
    profile: &BenchmarkProfile,
    compression_analysis: Option<&CompressionAnalysis>,
) -> Vec<MetricResult> {
    profile
        .metric_declarations
        .iter()
        .map(|declaration| MetricResult {
            metric_id: declaration.metric_id.clone(),
            label: declaration.label.clone(),
            kind: declaration.kind.clone(),
            coverage: declaration.coverage.clone(),
            research_goal_ids: declaration.research_goal_ids.clone(),
            ranking_weight: declaration.ranking_weight,
            value: match &declaration.kind {
                MetricKind::SameLeafNeighborhoodCoherence => same_leaf_neighborhood_coherence(
                    &execution.leaf_membership,
                    &profile.locality_ground_truth,
                ),
                MetricKind::LocalCompressionGain => compression_analysis
                    .map(|analysis| analysis.reported_gain)
                    .unwrap_or(0.0),
            },
        })
        .collect()
}

fn compute_gate_results_with_filter(
    profile: &BenchmarkProfile,
    execution: &SingleExecution,
    metric_results: &[MetricResult],
    determinism: &DeterminismReport,
    mut include_gate: impl FnMut(&GateKind) -> bool,
) -> Vec<GateResult> {
    let metric_lookup = metric_results
        .iter()
        .map(|metric| (metric.metric_id.as_str(), metric.value))
        .collect::<HashMap<_, _>>();

    let total_entity_count = execution.leaf_membership.len();
    let unique_entity_count = execution
        .leaf_membership
        .iter()
        .map(|member| member.entity_id.as_str())
        .collect::<HashSet<_>>()
        .len();
    let exact_occupancy = execution
        .cluster_occupancies
        .iter()
        .all(|occupancy| occupancy.total_count == profile.leaf_model.leaf_size);
    let no_empty_clusters = execution
        .cluster_occupancies
        .iter()
        .all(|occupancy| occupancy.total_count > 0);

    profile
        .gate_declarations
        .iter()
        .filter(|gate| include_gate(&gate.kind) && !matches!(gate.kind, GateKind::ExecutionBudget))
        .map(|gate| match &gate.kind {
            GateKind::ExactLeafOccupancy => GateResult {
                gate_id: gate.gate_id.clone(),
                label: gate.label.clone(),
                coverage: gate.coverage.clone(),
                research_goal_ids: gate.research_goal_ids.clone(),
                status: bool_to_status(exact_occupancy),
                observed_value: Some(
                    execution
                        .cluster_occupancies
                        .iter()
                        .filter(|occupancy| occupancy.total_count == profile.leaf_model.leaf_size)
                        .count() as f64,
                ),
                detail: format!(
                    "expected every cluster to contain exactly {} entities",
                    profile.leaf_model.leaf_size
                ),
            },
            GateKind::LeafSizeAtLeast { minimum } => GateResult {
                gate_id: gate.gate_id.clone(),
                label: gate.label.clone(),
                coverage: gate.coverage.clone(),
                research_goal_ids: gate.research_goal_ids.clone(),
                status: bool_to_status(
                    execution
                        .cluster_occupancies
                        .iter()
                        .all(|occupancy| occupancy.total_count >= *minimum),
                ),
                observed_value: Some(
                    execution
                        .cluster_occupancies
                        .iter()
                        .map(|occupancy| occupancy.total_count)
                        .min()
                        .unwrap_or_default() as f64,
                ),
                detail: format!(
                    "expected every packed cluster to contain at least {} entities",
                    minimum
                ),
            },
            GateKind::LeafSizeAtMost { maximum } => GateResult {
                gate_id: gate.gate_id.clone(),
                label: gate.label.clone(),
                coverage: gate.coverage.clone(),
                research_goal_ids: gate.research_goal_ids.clone(),
                status: bool_to_status(
                    execution
                        .cluster_occupancies
                        .iter()
                        .all(|occupancy| occupancy.total_count <= *maximum),
                ),
                observed_value: Some(
                    execution
                        .cluster_occupancies
                        .iter()
                        .map(|occupancy| occupancy.total_count)
                        .max()
                        .unwrap_or_default() as f64,
                ),
                detail: format!(
                    "expected every packed cluster to contain at most {} entities",
                    maximum
                ),
            },
            GateKind::CompleteCoverage => GateResult {
                gate_id: gate.gate_id.clone(),
                label: gate.label.clone(),
                coverage: gate.coverage.clone(),
                research_goal_ids: gate.research_goal_ids.clone(),
                status: bool_to_status(total_entity_count == execution.evaluation_entities.len()),
                observed_value: Some(total_entity_count as f64),
                detail: format!(
                    "observed {} assigned entities for {} declared entities",
                    total_entity_count,
                    execution.evaluation_entities.len()
                ),
            },
            GateKind::OneClusterPerEntity => GateResult {
                gate_id: gate.gate_id.clone(),
                label: gate.label.clone(),
                coverage: gate.coverage.clone(),
                research_goal_ids: gate.research_goal_ids.clone(),
                status: bool_to_status(unique_entity_count == execution.evaluation_entities.len()),
                observed_value: Some(unique_entity_count as f64),
                detail: "each evaluated entity must appear once in the leaf membership artifact"
                    .into(),
            },
            GateKind::NoEmptyDeclaredClusters => GateResult {
                gate_id: gate.gate_id.clone(),
                label: gate.label.clone(),
                coverage: gate.coverage.clone(),
                research_goal_ids: gate.research_goal_ids.clone(),
                status: bool_to_status(no_empty_clusters),
                observed_value: Some(
                    execution
                        .cluster_occupancies
                        .iter()
                        .filter(|occupancy| occupancy.total_count > 0)
                        .count() as f64,
                ),
                detail: "every declared final cluster must contain at least one entity".into(),
            },
            GateKind::DeterministicObservableResults => GateResult {
                gate_id: gate.gate_id.clone(),
                label: gate.label.clone(),
                coverage: gate.coverage.clone(),
                research_goal_ids: gate.research_goal_ids.clone(),
                status: bool_to_status(determinism.deterministic),
                observed_value: Some(if determinism.deterministic { 1.0 } else { 0.0 }),
                detail: if determinism.deterministic {
                    "repeated observable results matched".into()
                } else {
                    determinism.mismatch_details.join("; ")
                },
            },
            GateKind::MetricAtLeast { metric_id, minimum } => {
                let observed = metric_lookup
                    .get(metric_id.as_str())
                    .copied()
                    .unwrap_or(f64::NEG_INFINITY);
                GateResult {
                    gate_id: gate.gate_id.clone(),
                    label: gate.label.clone(),
                    coverage: gate.coverage.clone(),
                    research_goal_ids: gate.research_goal_ids.clone(),
                    status: bool_to_status(observed >= *minimum),
                    observed_value: Some(observed),
                    detail: format!(
                        "required metric {} to be at least {:.6}, observed {:.6}",
                        metric_id, minimum, observed
                    ),
                }
            }
            GateKind::ExecutionBudget => {
                unreachable!(
                    "execution-budget gates are applied by section-4/section-5 suite workflows"
                )
            }
        })
        .collect()
}

fn bool_to_status(value: bool) -> GateStatus {
    if value {
        GateStatus::Passed
    } else {
        GateStatus::Failed
    }
}

fn rank_candidates(run_reports: &[CandidateRunReport]) -> Vec<RankedCandidate> {
    let mut ranked = run_reports
        .iter()
        .filter(|run_report| {
            run_report.run_status == CandidateRunStatus::Succeeded
                && run_report.survived_required_gates
        })
        .filter_map(|run_report| {
            run_report
                .packing_evaluation
                .as_ref()
                .and_then(|packing| packing.ranking_score)
                .or(run_report.ranking_score)
                .map(|ranking_score| RankedCandidate {
                    candidate_id: run_report.candidate_identity.candidate_id.clone(),
                    ranking_score,
                    rank: 0,
                })
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|left, right| {
        right
            .ranking_score
            .partial_cmp(&left.ranking_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.candidate_id.cmp(&right.candidate_id))
    });

    for (index, candidate) in ranked.iter_mut().enumerate() {
        candidate.rank = index + 1;
    }

    ranked
}

fn rank_raw_candidates(run_reports: &[CandidateRunReport]) -> Vec<RankedCandidate> {
    let mut ranked = run_reports
        .iter()
        .filter(|run_report| run_report.raw_survived_required_gates)
        .filter_map(|run_report| {
            run_report
                .raw_ranking_score
                .map(|ranking_score| RankedCandidate {
                    candidate_id: run_report.candidate_identity.candidate_id.clone(),
                    ranking_score,
                    rank: 0,
                })
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|left, right| {
        right
            .ranking_score
            .partial_cmp(&left.ranking_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.candidate_id.cmp(&right.candidate_id))
    });
    for (index, candidate) in ranked.iter_mut().enumerate() {
        candidate.rank = index + 1;
    }
    ranked
}

fn rank_packing_pipelines(run_reports: &[CandidateRunReport]) -> Vec<RankedPackingPipeline> {
    let mut ranked = run_reports
        .iter()
        .flat_map(|run_report| {
            run_report
                .packing_evaluations
                .iter()
                .filter(|packing| packing.survived_required_gates)
                .filter_map(|packing| {
                    packing
                        .ranking_score
                        .map(|ranking_score| RankedPackingPipeline {
                            pipeline_id: packing.pipeline_id.clone(),
                            candidate_id: run_report.candidate_identity.candidate_id.clone(),
                            packing_strategy_id: packing.packer_id.clone(),
                            ranking_score,
                            rank: 0,
                        })
                })
        })
        .collect::<Vec<_>>();
    ranked.sort_by(compare_ranked_packing_pipelines);
    for (index, pipeline) in ranked.iter_mut().enumerate() {
        pipeline.rank = index + 1;
    }
    ranked
}

fn compare_ranked_packing_pipelines(
    left: &RankedPackingPipeline,
    right: &RankedPackingPipeline,
) -> Ordering {
    right
        .ranking_score
        .partial_cmp(&left.ranking_score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left.candidate_id.cmp(&right.candidate_id))
        .then_with(|| left.packing_strategy_id.cmp(&right.packing_strategy_id))
}

fn select_best_packing_evaluation(
    packing_evaluations: &[PackingEvaluationReport],
) -> Option<PackingEvaluationReport> {
    let mut ranked = packing_evaluations
        .iter()
        .filter(|packing| packing.survived_required_gates)
        .map(|packing| RankedPackingPipeline {
            pipeline_id: packing.pipeline_id.clone(),
            candidate_id: String::new(),
            packing_strategy_id: packing.packer_id.clone(),
            ranking_score: packing.ranking_score.unwrap_or(f64::NEG_INFINITY),
            rank: 0,
        })
        .collect::<Vec<_>>();
    ranked.sort_by(compare_ranked_packing_pipelines);
    let best_strategy_id = ranked.first()?.packing_strategy_id.as_str();
    packing_evaluations
        .iter()
        .find(|packing| {
            packing.survived_required_gates && packing.packer_id.as_str() == best_strategy_id
        })
        .cloned()
}

fn first_failed_packing_evaluation(
    packing_evaluations: &[PackingEvaluationReport],
) -> Option<&PackingEvaluationReport> {
    packing_evaluations
        .iter()
        .filter(|packing| !packing.survived_required_gates)
        .min_by(|left, right| left.packer_id.cmp(&right.packer_id))
}

pub(crate) fn packing_pipeline_id(candidate_id: &str, packing_strategy_id: &str) -> String {
    format!("{candidate_id}::{packing_strategy_id}")
}

#[derive(Clone, Debug, PartialEq)]
struct SingleExecution {
    provenance: ProvenanceManifest,
    pass_reports: Vec<ObservablePassReport>,
    probe_results: Vec<ProbeAssignmentResult>,
    leaf_membership: Vec<LeafMembershipRecord>,
    cluster_occupancies: Vec<ClusterOccupancy>,
    evaluation_entities: Vec<EvaluationEntity>,
}

#[derive(Clone, Debug, PartialEq)]
struct ResolvedProbeWorkload {
    workload_id: String,
    embeddings: Vec<Embedding>,
}

#[derive(Clone, Debug, PartialEq)]
struct ResolvedProfileInputs {
    training_passes: Vec<PassPlan>,
    probe_workloads: Vec<ResolvedProbeWorkload>,
    evaluation_entities: Vec<EvaluationEntity>,
    source_reference_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
enum CandidateExecutionError {
    Candidate(StreamingClusteringError),
    CorpusSource(StructuredFailure),
}

impl From<StreamingClusteringError> for CandidateExecutionError {
    fn from(value: StreamingClusteringError) -> Self {
        Self::Candidate(value)
    }
}

fn execute_candidate_once(
    profile: &BenchmarkProfile,
    candidate: &RegisteredCandidate,
    resolved: &ResolvedProfileInputs,
) -> Result<SingleExecution, CandidateExecutionError> {
    let streaming_config = profile.shared_candidate_config.to_streaming_config();
    let mut trainer = candidate.factory.create(&streaming_config)?;
    eprintln!(
        "[TIMING] Starting training phase for {}",
        candidate.identity.candidate_id
    );
    let train_start = std::time::Instant::now();
    let mut pass_reports = Vec::with_capacity(resolved.training_passes.len());
    for pass in &resolved.training_passes {
        for batch in pass {
            trainer.ingest_batch(batch)?;
        }
        pass_reports.push(trainer.finish_pass()?.into());
    }
    trainer.complete_training()?;
    let classifier = trainer.into_classifier()?;
    eprintln!(
        "[TIMING] Training phase completed in {:.2}s",
        train_start.elapsed().as_secs_f64()
    );
    let candidate_threading = candidate_threading_provenance(profile);
    let host_scaled_candidate_execution = candidate_threading.effective_mode == "host-scaled"
        && candidate_threading.effective_thread_count > 1;

    eprintln!(
        "[TIMING] Starting probe workload phase ({} workloads)",
        resolved.probe_workloads.len()
    );
    let probe_start = std::time::Instant::now();
    let probe_results = if host_scaled_candidate_execution {
        resolved
            .probe_workloads
            .par_iter()
            .map(|workload| assign_probe_workload(&*classifier, profile, workload))
            .collect::<Result<Vec<_>, StreamingClusteringError>>()?
    } else {
        resolved
            .probe_workloads
            .iter()
            .map(|workload| assign_probe_workload(&*classifier, profile, workload))
            .collect::<Result<Vec<_>, StreamingClusteringError>>()?
    };
    eprintln!(
        "[TIMING] Probe workload phase completed in {:.2}s",
        probe_start.elapsed().as_secs_f64()
    );

    eprintln!(
        "[TIMING] Starting leaf membership phase ({} entities)",
        resolved.evaluation_entities.len()
    );
    let membership_start = std::time::Instant::now();
    let leaf_membership = if host_scaled_candidate_execution {
        resolved
            .evaluation_entities
            .par_iter()
            .map(|entity| assign_evaluation_entity(&*classifier, profile, entity))
            .collect::<Result<Vec<_>, StreamingClusteringError>>()?
    } else {
        resolved
            .evaluation_entities
            .iter()
            .map(|entity| assign_evaluation_entity(&*classifier, profile, entity))
            .collect::<Result<Vec<_>, StreamingClusteringError>>()?
    };
    eprintln!(
        "[TIMING] Leaf membership phase completed in {:.2}s",
        membership_start.elapsed().as_secs_f64()
    );

    let cluster_occupancies = compute_cluster_occupancies(
        profile.leaf_model.declared_final_cluster_count,
        &leaf_membership,
    );

    Ok(SingleExecution {
        provenance: build_provenance(
            profile,
            &candidate.identity,
            resolved.source_reference_ids.clone(),
        ),
        pass_reports,
        probe_results,
        leaf_membership,
        cluster_occupancies,
        evaluation_entities: resolved.evaluation_entities.clone(),
    })
}

fn assign_probe_workload(
    classifier: &dyn DynClassifier,
    profile: &BenchmarkProfile,
    workload: &ResolvedProbeWorkload,
) -> Result<ProbeAssignmentResult, StreamingClusteringError> {
    let assignments = classifier.assign_batch(&workload.embeddings)?;
    Ok(ProbeAssignmentResult {
        workload_id: workload.workload_id.clone(),
        assignments: validate_cluster_assignments(
            assignments,
            profile.leaf_model.declared_final_cluster_count,
            &format!("probe workload {}", workload.workload_id),
        )?,
    })
}

fn assign_evaluation_entity(
    classifier: &dyn DynClassifier,
    profile: &BenchmarkProfile,
    entity: &EvaluationEntity,
) -> Result<LeafMembershipRecord, StreamingClusteringError> {
    Ok(LeafMembershipRecord {
        entity_id: entity.entity_id.clone(),
        cluster_id: validate_cluster_id(
            classifier.assign(&entity.embedding)?,
            profile.leaf_model.declared_final_cluster_count,
            &format!("evaluation entity {}", entity.entity_id),
        )?,
        synthetic: entity.synthetic,
    })
}

fn build_provenance(
    profile: &BenchmarkProfile,
    identity: &CandidateIdentity,
    source_reference_ids: Vec<String>,
) -> ProvenanceManifest {
    build_provenance_with_backend(
        profile,
        identity,
        source_reference_ids,
        acceleration::detected_execution_backend_selection().clone(),
    )
}

fn build_provenance_with_backend(
    profile: &BenchmarkProfile,
    identity: &CandidateIdentity,
    source_reference_ids: Vec<String>,
    execution_backend: ExecutionBackendSelection,
) -> ProvenanceManifest {
    ProvenanceManifest {
        profile_id: profile.profile_id.clone(),
        corpus_ids: profile.corpus_ids.clone(),
        source_reference_ids,
        candidate_identity: identity.clone(),
        shared_candidate_config: profile.shared_candidate_config.clone(),
        seed_policy: profile.reproducibility.seed_policy.clone(),
        software_identity: profile.reproducibility.software_identity.clone(),
        floating_point_profile: profile.reproducibility.floating_point_profile.clone(),
        hardware_profile: profile.reproducibility.hardware_profile.clone(),
        candidate_threading: candidate_threading_provenance(profile),
        execution_backend,
    }
}

fn candidate_threading_provenance(profile: &BenchmarkProfile) -> CandidateThreadingProvenance {
    let declared_model = normalized_threading_field(
        &profile.reproducibility.candidate_threading_model,
        default_candidate_threading_model(),
    );
    let reduction_order_strategy = normalized_threading_field(
        &profile.reproducibility.reduction_order_strategy,
        default_reduction_order_strategy(),
    );
    let effective_mode = if declared_candidate_threading_is_host_scaled(&declared_model) {
        "host-scaled".into()
    } else {
        "single-threaded".into()
    };
    let effective_thread_count = if effective_mode == "host-scaled" {
        rayon::current_num_threads()
    } else {
        1
    };
    CandidateThreadingProvenance {
        declared_model,
        reduction_order_strategy,
        effective_mode,
        effective_thread_count,
    }
}

fn normalized_threading_field(value: &str, fallback: String) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback
    } else {
        trimmed.to_owned()
    }
}

pub(crate) fn declared_candidate_threading_is_host_scaled(model: &str) -> bool {
    model.to_ascii_lowercase().contains("host-scaled")
}

fn declared_source_reference_ids(profile: &BenchmarkProfile) -> Vec<String> {
    let mut ids = BTreeMap::<String, ()>::new();
    for source_id in iter_declared_source_reference_ids(profile) {
        ids.insert(source_id.to_owned(), ());
    }
    ids.into_keys().collect()
}

fn iter_declared_source_reference_ids(profile: &BenchmarkProfile) -> impl Iterator<Item = &str> {
    let training = profile
        .training_passes
        .iter()
        .filter_map(|pass| match pass {
            TrainingPassSource::BlockStore { corpus, .. } => Some(corpus.source_id.as_str()),
            TrainingPassSource::Inline { .. } => None,
        });
    let probes = profile
        .probe_workloads
        .iter()
        .filter_map(|workload| match &workload.source {
            EmbeddingWorkloadSource::BlockStore { corpus } => Some(corpus.source_id.as_str()),
            EmbeddingWorkloadSource::Inline { .. } => None,
        });
    let evaluation = match &profile.evaluation_entities {
        EvaluationEntitySource::BlockStore { corpora } => Some(
            corpora
                .iter()
                .map(|corpus| corpus.corpus.source_id.as_str())
                .collect::<Vec<_>>(),
        ),
        EvaluationEntitySource::Inline { .. } => None,
    }
    .into_iter()
    .flatten();

    training.chain(probes).chain(evaluation)
}

fn structured_failure_detail(failure: &StructuredFailure) -> String {
    match failure {
        StructuredFailure::InvalidConfiguration { message } => message.clone(),
        StructuredFailure::InvalidCorpusSourceReference { source_id, message } => {
            format!("invalid corpus source {source_id}: {message}")
        }
        StructuredFailure::CorpusSourceLoadFailure { source_id, message } => {
            format!("failed to load corpus source {source_id}: {message}")
        }
        StructuredFailure::ArchiveSourceOpenFailure { source_id, message } => {
            format!("failed to open archive-backed corpus source {source_id}: {message}")
        }
        StructuredFailure::ArchiveSourceReadFailure { source_id, message } => {
            format!("failed to read archive-backed corpus source {source_id}: {message}")
        }
        StructuredFailure::ArchiveSourceTemporaryLayerFailure { source_id, message } => {
            format!(
                "failed to create the temporary writable layer for archive-backed corpus source {source_id}: {message}"
            )
        }
        StructuredFailure::CandidateSharedContractFailure { message, .. } => message.clone(),
        StructuredFailure::GateFailure { message, .. } => message.clone(),
        StructuredFailure::DeferredMeasurement { message, .. } => message.clone(),
    }
}

fn determinism_compared_fields() -> Vec<String> {
    vec![
        "pass_reports".into(),
        "probe_results".into(),
        "leaf_membership".into(),
        "evaluation_entities".into(),
        "provenance".into(),
    ]
}

fn resolve_profile_inputs(
    profile: &BenchmarkProfile,
) -> Result<ResolvedProfileInputs, CandidateExecutionError> {
    eprintln!("[TIMING-RESOLVE] Starting profile input resolution");
    let resolve_start = std::time::Instant::now();
    let mut source_reference_ids = BTreeMap::<String, ()>::new();

    eprintln!("[TIMING-RESOLVE] Loading training passes");
    let training_passes_start = std::time::Instant::now();
    let training_passes = profile
        .training_passes
        .iter()
        .map(|pass| match pass {
            TrainingPassSource::Inline { batches } => Ok(batches.clone()),
            TrainingPassSource::BlockStore { corpus, batch_size } => {
                source_reference_ids.insert(corpus.source_id.clone(), ());
                let embeddings = load_embeddings_from_reference(
                    corpus,
                    profile.shared_candidate_config.dimensions,
                )?;
                if embeddings.is_empty() {
                    return Err(invalid_corpus_source_reference(
                        &corpus.source_id,
                        "resolved to zero embeddings".into(),
                    ));
                }
                Ok(embeddings_into_batches(embeddings, *batch_size))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    eprintln!(
        "[TIMING-RESOLVE] Training passes loaded in {:.2}s",
        training_passes_start.elapsed().as_secs_f64()
    );

    eprintln!("[TIMING-RESOLVE] Loading probe workloads");
    let probe_workloads_start = std::time::Instant::now();
    let probe_workloads = profile
        .probe_workloads
        .iter()
        .map(|workload| {
            let embeddings = match &workload.source {
                EmbeddingWorkloadSource::Inline { embeddings } => embeddings.clone(),
                EmbeddingWorkloadSource::BlockStore { corpus } => {
                    source_reference_ids.insert(corpus.source_id.clone(), ());
                    load_embeddings_from_reference(
                        corpus,
                        profile.shared_candidate_config.dimensions,
                    )?
                }
            };
            Ok(ResolvedProbeWorkload {
                workload_id: workload.workload_id.clone(),
                embeddings,
            })
        })
        .collect::<Result<Vec<_>, CandidateExecutionError>>()?;
    eprintln!(
        "[TIMING-RESOLVE] Probe workloads loaded in {:.2}s",
        probe_workloads_start.elapsed().as_secs_f64()
    );

    eprintln!("[TIMING-RESOLVE] Loading evaluation entities");
    let evaluation_entities_start = std::time::Instant::now();
    let evaluation_entities = match &profile.evaluation_entities {
        EvaluationEntitySource::Inline { entities } => entities.clone(),
        EvaluationEntitySource::BlockStore { corpora } => {
            let mut entities = Vec::new();
            for corpus in corpora {
                source_reference_ids.insert(corpus.corpus.source_id.clone(), ());
                entities.extend(load_evaluation_entities_from_reference(corpus)?);
            }
            entities
        }
    };
    eprintln!(
        "[TIMING-RESOLVE] Evaluation entities loaded in {:.2}s",
        evaluation_entities_start.elapsed().as_secs_f64()
    );

    assert_unique(
        evaluation_entities
            .iter()
            .map(|entity| entity.entity_id.as_str()),
        "evaluation entity ids",
    )
    .map_err(|error| {
        corpus_source_load_failure(
            &evaluation_source_label(&profile.evaluation_entities),
            error.to_string(),
        )
    })?;
    validate_materialized_evaluation_entities(profile, &evaluation_entities).map_err(|error| {
        corpus_source_load_failure(
            &evaluation_source_label(&profile.evaluation_entities),
            error.to_string(),
        )
    })?;

    eprintln!(
        "[TIMING-RESOLVE] Total resolution time: {:.2}s",
        resolve_start.elapsed().as_secs_f64()
    );
    Ok(ResolvedProfileInputs {
        training_passes,
        probe_workloads,
        evaluation_entities,
        source_reference_ids: source_reference_ids.into_keys().collect(),
    })
}

pub(crate) fn resolved_profile_evaluation_entities(
    profile: &BenchmarkProfile,
) -> Result<Vec<EvaluationEntity>, EvaluatorError> {
    match resolve_profile_inputs(profile) {
        Ok(resolved) => Ok(resolved.evaluation_entities),
        Err(CandidateExecutionError::Candidate(error)) => {
            Err(EvaluatorError::InvalidConfiguration(error.to_string()))
        }
        Err(CandidateExecutionError::CorpusSource(failure)) => Err(
            EvaluatorError::InvalidConfiguration(structured_failure_detail(&failure)),
        ),
    }
}

fn evaluation_source_label(source: &EvaluationEntitySource) -> String {
    match source {
        EvaluationEntitySource::Inline { .. } => "inline-evaluation-entities".into(),
        EvaluationEntitySource::BlockStore { corpora } => corpora
            .iter()
            .map(|corpus| corpus.corpus.source_id.clone())
            .collect::<Vec<_>>()
            .join(","),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LoadedLeafRecord {
    pub(crate) block_id: BlockHash,
    pub(crate) embedding_spec: EmbeddingSpec,
    pub(crate) entry: LeafEntry,
}

enum ResolvedCorpusStore {
    Filesystem(FilesystemBlockStore),
    ZipOverlay(FsOverlayZipBlockStore),
}

impl ResolvedCorpusStore {
    fn as_block_store(&self) -> &dyn BlockStore {
        match self {
            Self::Filesystem(store) => store,
            Self::ZipOverlay(store) => store,
        }
    }
}

fn load_embeddings_from_reference(
    reference: &BlockStoreCorpusReference,
    expected_dimensions: usize,
) -> Result<Vec<Embedding>, CandidateExecutionError> {
    let records = load_leaf_records(reference)?;
    records
        .iter()
        .map(|record| {
            let embedding = decode_embedding_to_f32(
                &record.entry.embedding,
                &record.embedding_spec,
                &format!("block {} in source {}", record.block_id, reference.source_id),
            )
            .map_err(|message| corpus_source_load_failure(&reference.source_id, message))?;
            validate_embedding(&embedding, expected_dimensions).map_err(|error| {
                corpus_source_load_failure(
                    &reference.source_id,
                    format!(
                        "decoded embedding from block {} did not match expected dimensions: {error}",
                        record.block_id
                    ),
                )
            })?;
            Ok(embedding)
        })
        .collect()
}

fn embeddings_into_batches(embeddings: Vec<Embedding>, batch_size: usize) -> PassPlan {
    let mut batches = Vec::with_capacity(embeddings.len().div_ceil(batch_size));
    let mut next_batch = Vec::with_capacity(batch_size);
    for embedding in embeddings {
        next_batch.push(embedding);
        if next_batch.len() == batch_size {
            batches.push(next_batch);
            next_batch = Vec::with_capacity(batch_size);
        }
    }
    if !next_batch.is_empty() {
        batches.push(next_batch);
    }
    batches
}

fn load_evaluation_entities_from_reference(
    source: &BlockStoreEvaluationCorpus,
) -> Result<Vec<EvaluationEntity>, CandidateExecutionError> {
    let records = load_leaf_records(&source.corpus)?;
    records
        .iter()
        .map(|record| {
            let entity_id = required_metadata_text(
                &record.entry.metadata,
                &source.entity_id_metadata_key,
                &source.corpus.source_id,
                record.block_id,
            )?;
            let synthetic = match &source.synthetic_metadata_key {
                Some(key) => required_metadata_bool(
                    &record.entry.metadata,
                    key,
                    &source.corpus.source_id,
                    record.block_id,
                )?,
                None => false,
            };
            Ok(EvaluationEntity {
                entity_id,
                corpus_id: source.corpus_id.clone(),
                embedding: decode_embedding_to_f32(
                    &record.entry.embedding,
                    &record.embedding_spec,
                    &format!(
                        "block {} in source {}",
                        record.block_id, source.corpus.source_id
                    ),
                )
                .map_err(|message| corpus_source_load_failure(&source.corpus.source_id, message))?,
                synthetic,
            })
        })
        .collect()
}

pub(crate) fn load_leaf_records(
    reference: &BlockStoreCorpusReference,
) -> Result<Vec<LoadedLeafRecord>, CandidateExecutionError> {
    let root_block_id = parse_block_hash_hex(&reference.root_block_id)
        .map_err(|message| invalid_corpus_source_reference(&reference.source_id, message))?;
    let store = open_corpus_store(reference)?;
    let mut records = Vec::new();
    let mut visited = HashSet::new();
    collect_leaf_records(
        store.as_block_store(),
        reference,
        root_block_id,
        &mut visited,
        &mut records,
    )?;
    Ok(records)
}

fn open_corpus_store(
    reference: &BlockStoreCorpusReference,
) -> Result<ResolvedCorpusStore, CandidateExecutionError> {
    match &reference.store {
        BlockStoreReferenceStore::Filesystem { store_root } => {
            FilesystemBlockStore::new(store_root)
                .map(ResolvedCorpusStore::Filesystem)
                .map_err(|error| {
                    corpus_source_load_failure(
                        &reference.source_id,
                        format!(
                            "failed to open block store {}: {error}",
                            store_root.display()
                        ),
                    )
                })
        }
        BlockStoreReferenceStore::ZipArchive { archive_path } => {
            FsOverlayZipBlockStore::new(archive_path)
                .map(ResolvedCorpusStore::ZipOverlay)
                .map_err(|error| match error {
                    ArchiveOverlayStoreError::TemporaryLayer(message) => {
                        archive_source_temporary_layer_failure(&reference.source_id, message)
                    }
                    ArchiveOverlayStoreError::ArchiveOpen(message) => {
                        archive_source_open_failure(&reference.source_id, message)
                    }
                    ArchiveOverlayStoreError::ArchiveRead(message) => {
                        archive_source_read_failure(&reference.source_id, message)
                    }
                })
        }
    }
}

fn collect_leaf_records(
    store: &dyn BlockStore,
    reference: &BlockStoreCorpusReference,
    block_id: BlockHash,
    visited: &mut HashSet<BlockHash>,
    records: &mut Vec<LoadedLeafRecord>,
) -> Result<(), CandidateExecutionError> {
    if !visited.insert(block_id) {
        return Err(corpus_source_load_failure(
            &reference.source_id,
            format!("encountered block {block_id} more than once while traversing the source"),
        ));
    }

    let validated = pollster::block_on(store.get(&block_id))
        .map_err(|error| {
            source_store_read_failure(
                reference,
                format!("failed to load block {block_id}: {error}"),
            )
        })?
        .ok_or_else(|| {
            corpus_source_load_failure(
                &reference.source_id,
                format!("referenced block {block_id} was not present in the store"),
            )
        })?;

    match into_entries(validated) {
        TypedEntries::Branch(_, entries) => {
            for entry in entries {
                collect_leaf_records(store, reference, entry.child, visited, records)?;
            }
        }
        TypedEntries::Leaf(metadata, entries) => {
            for entry in entries {
                records.push(LoadedLeafRecord {
                    block_id,
                    embedding_spec: metadata.embedding_spec.clone(),
                    entry,
                });
            }
        }
    }

    Ok(())
}

fn required_metadata_text(
    metadata: &Metadata,
    key: &str,
    source_id: &str,
    block_id: BlockHash,
) -> Result<String, CandidateExecutionError> {
    match metadata_value(metadata, key) {
        Some(CborValue::Text(text)) => Ok(text.clone()),
        Some(_) => Err(corpus_source_load_failure(
            source_id,
            format!("metadata key {key:?} in block {block_id} must be text"),
        )),
        None => Err(corpus_source_load_failure(
            source_id,
            format!("metadata key {key:?} was missing in block {block_id}"),
        )),
    }
}

pub(crate) fn required_metadata_bool(
    metadata: &Metadata,
    key: &str,
    source_id: &str,
    block_id: BlockHash,
) -> Result<bool, CandidateExecutionError> {
    match metadata_value(metadata, key) {
        Some(CborValue::Bool(boolean)) => Ok(*boolean),
        Some(_) => Err(corpus_source_load_failure(
            source_id,
            format!("metadata key {key:?} in block {block_id} must be boolean"),
        )),
        None => Err(corpus_source_load_failure(
            source_id,
            format!("metadata key {key:?} was missing in block {block_id}"),
        )),
    }
}

pub(crate) fn metadata_value<'a>(metadata: &'a Metadata, key: &str) -> Option<&'a CborValue> {
    metadata
        .iter()
        .find_map(|(candidate, value)| match candidate {
            CborValue::Text(text) if text == key => Some(value),
            _ => None,
        })
}

pub(crate) fn decode_embedding_to_f32(
    bytes: &[u8],
    spec: &EmbeddingSpec,
    context: &str,
) -> Result<Vec<f32>, String> {
    let dims = usize::try_from(spec.dims).map_err(|_| {
        format!(
            "{context} declares dimensions {} that do not fit usize",
            spec.dims
        )
    })?;
    match spec.encoding.as_str() {
        "f32le" => {
            let expected_len =
                checked_embedding_byte_len(dims, 4, context, spec.encoding.as_str())?;
            if bytes.len() != expected_len {
                return Err(format!(
                    "{context} expected {} f32 bytes, found {}",
                    expected_len,
                    bytes.len()
                ));
            }
            Ok(bytes
                .chunks_exact(4)
                .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect())
        }
        "f16le" => {
            let expected_len =
                checked_embedding_byte_len(dims, 2, context, spec.encoding.as_str())?;
            if bytes.len() != expected_len {
                return Err(format!(
                    "{context} expected {} f16 bytes, found {}",
                    expected_len,
                    bytes.len()
                ));
            }
            Ok(bytes
                .chunks_exact(2)
                .map(|chunk| f16::from_le_bytes([chunk[0], chunk[1]]).to_f32())
                .collect())
        }
        "i8" => {
            if bytes.len() != dims {
                return Err(format!(
                    "{context} expected {} i8 bytes, found {}",
                    dims,
                    bytes.len()
                ));
            }
            Ok(bytes.iter().map(|byte| (*byte as i8) as f32).collect())
        }
        other => Err(format!(
            "{context} uses unsupported embedding encoding {other:?}; evaluator corpus sources currently require f32le, f16le, or i8"
        )),
    }
}

fn checked_embedding_byte_len(
    dims: usize,
    bytes_per_dimension: usize,
    context: &str,
    encoding: &str,
) -> Result<usize, String> {
    dims.checked_mul(bytes_per_dimension).ok_or_else(|| {
        format!(
            "{context} byte length overflowed usize for {dims}-dimensional {encoding} embedding"
        )
    })
}

pub(crate) fn parse_block_hash_hex(value: &str) -> Result<BlockHash, String> {
    if value.len() != BlockHash::LEN * 2 {
        return Err(format!(
            "expected a {}-character hex block id, found {} characters",
            BlockHash::LEN * 2,
            value.len()
        ));
    }
    let mut bytes = [0u8; BlockHash::LEN];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(chunk[0]).ok_or_else(|| {
            format!(
                "block id contains a non-hex character at byte offset {}",
                index * 2
            )
        })?;
        let low = hex_nibble(chunk[1]).ok_or_else(|| {
            format!(
                "block id contains a non-hex character at byte offset {}",
                index * 2 + 1
            )
        })?;
        bytes[index] = (high << 4) | low;
    }
    Ok(BlockHash::from_bytes(bytes))
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn invalid_corpus_source_reference(source_id: &str, message: String) -> CandidateExecutionError {
    CandidateExecutionError::CorpusSource(StructuredFailure::InvalidCorpusSourceReference {
        source_id: source_id.into(),
        message,
    })
}

fn corpus_source_load_failure(source_id: &str, message: String) -> CandidateExecutionError {
    CandidateExecutionError::CorpusSource(StructuredFailure::CorpusSourceLoadFailure {
        source_id: source_id.into(),
        message,
    })
}

fn archive_source_open_failure(source_id: &str, message: String) -> CandidateExecutionError {
    CandidateExecutionError::CorpusSource(StructuredFailure::ArchiveSourceOpenFailure {
        source_id: source_id.into(),
        message,
    })
}

fn archive_source_read_failure(source_id: &str, message: String) -> CandidateExecutionError {
    CandidateExecutionError::CorpusSource(StructuredFailure::ArchiveSourceReadFailure {
        source_id: source_id.into(),
        message,
    })
}

fn archive_source_temporary_layer_failure(
    source_id: &str,
    message: String,
) -> CandidateExecutionError {
    CandidateExecutionError::CorpusSource(StructuredFailure::ArchiveSourceTemporaryLayerFailure {
        source_id: source_id.into(),
        message,
    })
}

fn source_store_read_failure(
    reference: &BlockStoreCorpusReference,
    message: String,
) -> CandidateExecutionError {
    match &reference.store {
        BlockStoreReferenceStore::Filesystem { .. } => {
            corpus_source_load_failure(&reference.source_id, message)
        }
        BlockStoreReferenceStore::ZipArchive { .. } => {
            archive_source_read_failure(&reference.source_id, message)
        }
    }
}

fn validate_cluster_assignments(
    assignments: Vec<ClusterId>,
    cluster_count: u32,
    context: &str,
) -> Result<Vec<ClusterId>, StreamingClusteringError> {
    assignments
        .into_iter()
        .map(|cluster_id| validate_cluster_id(cluster_id, cluster_count, context))
        .collect()
}

fn validate_cluster_id(
    cluster_id: ClusterId,
    cluster_count: u32,
    context: &str,
) -> Result<ClusterId, StreamingClusteringError> {
    if cluster_id < cluster_count {
        Ok(cluster_id)
    } else {
        Err(StreamingClusteringError::UnsatisfiableConstraint {
            message: format!(
                "{context} returned cluster id {cluster_id} outside [0, {cluster_count})"
            ),
        })
    }
}

fn compute_cluster_occupancies(
    cluster_count: u32,
    leaf_membership: &[LeafMembershipRecord],
) -> Vec<ClusterOccupancy> {
    let mut by_cluster = (0..cluster_count)
        .map(|cluster_id| {
            (
                cluster_id,
                ClusterOccupancy {
                    cluster_id,
                    total_count: 0,
                    real_count: 0,
                    synthetic_count: 0,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    for member in leaf_membership {
        if let Some(occupancy) = by_cluster.get_mut(&member.cluster_id) {
            occupancy.total_count += 1;
            if member.synthetic {
                occupancy.synthetic_count += 1;
            } else {
                occupancy.real_count += 1;
            }
        }
    }

    by_cluster.into_values().collect()
}

fn compute_cluster_occupancy_stats(
    cluster_occupancies: &[ClusterOccupancy],
) -> ClusterOccupancyStats {
    let counts = cluster_occupancies
        .iter()
        .map(|occupancy| occupancy.total_count as f64)
        .collect::<Vec<_>>();
    let mean_total_count = counts.iter().sum::<f64>() / counts.len() as f64;
    let variance = counts
        .iter()
        .map(|count| {
            let delta = count - mean_total_count;
            delta * delta
        })
        .sum::<f64>()
        / counts.len() as f64;
    ClusterOccupancyStats {
        mean_total_count,
        stddev_total_count: variance.sqrt(),
        min_total_count: cluster_occupancies
            .iter()
            .map(|occupancy| occupancy.total_count)
            .min()
            .expect("cluster occupancies are always materialized for declared clusters"),
        max_total_count: cluster_occupancies
            .iter()
            .map(|occupancy| occupancy.total_count)
            .max()
            .expect("cluster occupancies are always materialized for declared clusters"),
    }
}

fn compute_synthetic_padding_concentration(
    cluster_occupancies: &[ClusterOccupancy],
    profile: &BenchmarkProfile,
) -> Option<SyntheticPaddingConcentrationReport> {
    if profile.leaf_model.alignment_policy != AlignmentPolicy::DeterministicSyntheticPadding {
        return None;
    }

    let synthetic_entity_count = cluster_occupancies
        .iter()
        .map(|occupancy| occupancy.synthetic_count)
        .sum::<usize>();
    if synthetic_entity_count == 0 {
        return None;
    }

    let clusters_with_synthetic_entities = cluster_occupancies
        .iter()
        .filter(|occupancy| occupancy.synthetic_count > 0)
        .count();
    let minimum_possible_cluster_count =
        synthetic_entity_count.div_ceil(profile.leaf_model.leaf_size);

    Some(SyntheticPaddingConcentrationReport {
        synthetic_entity_count,
        clusters_with_synthetic_entities,
        minimum_possible_cluster_count,
        satisfies_minimum_concentration: clusters_with_synthetic_entities
            == minimum_possible_cluster_count,
    })
}

fn compare_executions(left: &SingleExecution, right: &SingleExecution) -> DeterminismReport {
    let mut mismatch_details = Vec::new();
    if left.pass_reports != right.pass_reports {
        mismatch_details.push("pass reports differed between repeated executions".into());
    }
    if left.probe_results != right.probe_results {
        mismatch_details.push("probe assignments differed between repeated executions".into());
    }
    if left.leaf_membership != right.leaf_membership {
        mismatch_details.push("leaf membership differed between repeated executions".into());
    }
    if left.evaluation_entities != right.evaluation_entities {
        mismatch_details
            .push("materialized evaluation entities differed between repeated executions".into());
    }
    if left.provenance != right.provenance {
        mismatch_details.push("provenance manifest differed between repeated executions".into());
    }

    DeterminismReport {
        deterministic: mismatch_details.is_empty(),
        compared_fields: determinism_compared_fields(),
        mismatch_details,
    }
}

fn same_leaf_neighborhood_coherence(
    leaf_membership: &[LeafMembershipRecord],
    ground_truth: &[GroundTruthNeighborhood],
) -> f64 {
    let assignment_by_entity = leaf_membership
        .iter()
        .filter(|member| !member.synthetic)
        .map(|member| (member.entity_id.as_str(), member.cluster_id))
        .collect::<HashMap<_, _>>();

    let mut same_leaf_hits = 0usize;
    let mut total_neighbors = 0usize;
    for truth in ground_truth {
        let Some(entity_cluster) = assignment_by_entity.get(truth.entity_id.as_str()) else {
            continue;
        };
        for neighbor_id in &truth.neighbor_ids {
            if let Some(neighbor_cluster) = assignment_by_entity.get(neighbor_id.as_str()) {
                total_neighbors += 1;
                if entity_cluster == neighbor_cluster {
                    same_leaf_hits += 1;
                }
            }
        }
    }

    if total_neighbors == 0 {
        0.0
    } else {
        same_leaf_hits as f64 / total_neighbors as f64
    }
}

fn compute_compression_analysis(
    leaf_membership: &[LeafMembershipRecord],
    evaluation_entities: &[EvaluationEntity],
    compression_benchmark: &CompressionBenchmark,
) -> Option<CompressionAnalysis> {
    match compression_benchmark.method {
        CompressionMethod::ScalarQuantization8Bit => {
            let real_entities = evaluation_entities
                .iter()
                .filter(|entity| !entity.synthetic)
                .collect::<Vec<_>>();
            if real_entities.is_empty() {
                return None;
            }

            let entity_lookup = evaluation_entities
                .iter()
                .map(|entity| (entity.entity_id.as_str(), entity))
                .collect::<HashMap<_, _>>();
            let mut entities_by_cluster = BTreeMap::<ClusterId, Vec<&EvaluationEntity>>::new();
            for member in leaf_membership {
                if member.synthetic {
                    continue;
                }
                if let Some(entity) = entity_lookup.get(member.entity_id.as_str()) {
                    entities_by_cluster
                        .entry(member.cluster_id)
                        .or_default()
                        .push(*entity);
                }
            }
            let bucket_reports = entities_by_cluster
                .iter()
                .map(|(cluster_id, entities)| CompressionBucketReport {
                    cluster_id: *cluster_id,
                    real_entity_count: entities.len(),
                    reconstruction_error: scalar_quantization_error(entities),
                })
                .collect::<Vec<_>>();
            let local_error_sum = bucket_reports
                .iter()
                .map(|bucket| bucket.reconstruction_error)
                .sum::<f64>();

            let global_error = scalar_quantization_error(&real_entities);
            if global_error == 0.0 {
                return Some(CompressionAnalysis {
                    baseline_label: compression_benchmark.global_baseline_label.clone(),
                    global_real_entity_count: real_entities.len(),
                    global_reconstruction_error: global_error,
                    local_reconstruction_error_sum: local_error_sum,
                    reported_gain: 0.0,
                    delta_semantics:
                        "reported_gain = 0 when global_reconstruction_error == 0; local_reconstruction_error_sum is reported directly"
                            .into(),
                    bucket_reports,
                });
            }

            Some(CompressionAnalysis {
                baseline_label: compression_benchmark.global_baseline_label.clone(),
                global_real_entity_count: real_entities.len(),
                global_reconstruction_error: global_error,
                local_reconstruction_error_sum: local_error_sum,
                reported_gain: 1.0 - (local_error_sum / global_error),
                delta_semantics:
                    "reported_gain = 1 - local_reconstruction_error_sum / global_reconstruction_error"
                        .into(),
                bucket_reports,
            })
        }
    }
}

fn scalar_quantization_error(entities: &[&EvaluationEntity]) -> f64 {
    if entities.is_empty() {
        return 0.0;
    }

    let dimensions = entities[0].embedding.len();
    let mut mins = vec![f32::INFINITY; dimensions];
    let mut maxs = vec![f32::NEG_INFINITY; dimensions];
    for entity in entities {
        for (index, value) in entity.embedding.iter().enumerate() {
            mins[index] = mins[index].min(*value);
            maxs[index] = maxs[index].max(*value);
        }
    }

    entities
        .iter()
        .flat_map(|entity| {
            entity
                .embedding
                .iter()
                .enumerate()
                .map(|(index, value)| quantization_error(*value, mins[index], maxs[index]))
        })
        .sum::<f64>()
}

fn quantization_error(value: f32, min_value: f32, max_value: f32) -> f64 {
    let range = max_value - min_value;
    if range == 0.0 {
        return 0.0;
    }
    let normalized = ((value - min_value) / range).clamp(0.0, 1.0);
    let quantized = (normalized * 255.0).round() / 255.0;
    let reconstructed = min_value + quantized * range;
    let delta = value - reconstructed;
    f64::from(delta * delta)
}

trait DynClassifier: Send + Sync {
    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError>;
    fn assign_batch(
        &self,
        embeddings: &[Embedding],
    ) -> Result<Vec<ClusterId>, StreamingClusteringError> {
        embeddings
            .iter()
            .map(|embedding| self.assign(embedding))
            .collect()
    }
}

trait DynTrainer {
    fn ingest_batch(&mut self, embeddings: &[Embedding]) -> Result<(), StreamingClusteringError>;
    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError>;
    fn complete_training(&mut self) -> Result<(), StreamingClusteringError>;
    fn into_classifier(self: Box<Self>)
    -> Result<Box<dyn DynClassifier>, StreamingClusteringError>;
}

trait CandidateFactory: Send + Sync {
    fn create(
        &self,
        config: &StreamingClusteringConfig,
    ) -> Result<Box<dyn DynTrainer>, StreamingClusteringError>;
}

impl<F, T> CandidateFactory for F
where
    F: Fn(&StreamingClusteringConfig) -> Result<T, StreamingClusteringError>
        + Send
        + Sync
        + 'static,
    T: StreamingClusterTrainer + 'static,
    T::Classifier: Send + Sync + 'static,
{
    fn create(
        &self,
        config: &StreamingClusteringConfig,
    ) -> Result<Box<dyn DynTrainer>, StreamingClusteringError> {
        Ok(Box::new(TrainerAdapter(self(config)?)))
    }
}

struct TrainerAdapter<T>(T);

impl<T> DynTrainer for TrainerAdapter<T>
where
    T: StreamingClusterTrainer + 'static,
    T::Classifier: Send + Sync + 'static,
{
    fn ingest_batch(&mut self, embeddings: &[Embedding]) -> Result<(), StreamingClusteringError> {
        self.0.ingest_batch(embeddings)
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        self.0.finish_pass()
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        self.0.complete_training()
    }

    fn into_classifier(
        self: Box<Self>,
    ) -> Result<Box<dyn DynClassifier>, StreamingClusteringError> {
        let classifier = self.0.into_classifier()?;
        Ok(Box::new(ClassifierAdapter(classifier)))
    }
}

struct ClassifierAdapter<C>(C);

impl<C> DynClassifier for ClassifierAdapter<C>
where
    C: StreamingClusterClassifier + Send + Sync + 'static,
{
    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        self.0.assign(embedding)
    }
}

pub fn built_in_fixture_candidate_names() -> Vec<&'static str> {
    vec![
        "balanced-threshold",
        "skewed-gate-fail",
        "shared-contract-failure",
        "nondeterministic-probe",
    ]
}

pub fn section4_family_candidate_names() -> Vec<&'static str> {
    vec![
        "recursive-balanced-kmeans",
        "pca-sort-exact-chunking",
        "space-filling-curve-exact-chunking",
        "graph-neighborhood-balance",
        "hybrid-coarse-rebalance",
        "random-shuffle-exact-chunking",
    ]
}

pub fn registered_candidate_names() -> Vec<&'static str> {
    let mut names = built_in_fixture_candidate_names();
    names.extend(section4_family_candidate_names());
    names.push("directional-pca");
    names.push("dcbc-streaming");
    names.push("spherical-kmeans");
    names
}

fn default_directional_pca_params() -> DirectionalPcaParams {
    DirectionalPcaParams {
        retained_axis_policy:
            lexongraph_directional_pca::DirectionalPcaRetainedAxisPolicy::FixedCount(1),
        allocation_policy:
            lexongraph_directional_pca::DirectionalPcaAllocationPolicy::CentroidWeightedBins,
        binning_policy: lexongraph_directional_pca::DirectionalPcaBinningPolicy::Quantile,
        cluster_cardinality_mode:
            lexongraph_directional_pca::DirectionalPcaClusterCardinalityMode::Exact,
        variance_exponent: 1.0,
        temperature: 1.0,
        min_input_count: 2,
        min_effective_rank: 1,
        min_cumulative_variance: 0.0,
    }
}

fn default_spherical_kmeans_params() -> SphericalKmeansParams {
    SphericalKmeansParams {
        initialization_policy: SphericalInitializationPolicy::SeededDeterministicFarthestPoint,
        max_iteration_count: 32,
        convergence_tolerance: 1e-4,
    }
}

#[derive(Clone, Copy)]
enum Section4FamilyStrategyMode {
    RecursiveBalancedKmeans,
    SpaceFillingCurveExactChunking,
    GraphNeighborhoodBalance,
    HybridCoarseRebalance,
    RandomShuffleExactChunking,
}

#[derive(Clone)]
struct Section4FamilyStrategyTrainer {
    config: StreamingClusteringConfig,
    state: TrainerState,
    mode: Section4FamilyStrategyMode,
    pass_observed_count: usize,
    ingested_embeddings: Vec<Embedding>,
}

#[derive(Clone)]
struct Section4FamilyStrategyClassifier {
    config: StreamingClusteringConfig,
    exact_assignments: HashMap<Vec<u32>, ClusterId>,
    cluster_centroids: Vec<Embedding>,
}

impl Section4FamilyStrategyTrainer {
    fn new(
        config: &StreamingClusteringConfig,
        mode: Section4FamilyStrategyMode,
    ) -> Result<Self, StreamingClusteringError> {
        validate_config(config)?;
        Ok(Self {
            config: config.clone(),
            state: TrainerState::Idle,
            mode,
            pass_observed_count: 0,
            ingested_embeddings: Vec::new(),
        })
    }
}

impl StreamingClusterTrainer for Section4FamilyStrategyTrainer {
    type Classifier = Section4FamilyStrategyClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn state(&self) -> TrainerState {
        self.state
    }

    fn ingest_batch(&mut self, embeddings: &[Embedding]) -> Result<(), StreamingClusteringError> {
        if !matches!(
            self.state,
            TrainerState::Idle | TrainerState::Ingesting | TrainerState::PassComplete
        ) {
            let invalid_state = self.state;
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::InvalidTransition {
                state: invalid_state,
                operation: "ingest_batch".into(),
            });
        }
        for embedding in embeddings {
            validate_embedding(embedding, self.config.dimensions)?;
            self.ingested_embeddings.push(embedding.clone());
        }
        self.pass_observed_count += embeddings.len();
        self.state = TrainerState::Ingesting;
        Ok(())
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        if self.state != TrainerState::Ingesting {
            let invalid_state = self.state;
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::InvalidTransition {
                state: invalid_state,
                operation: "finish_pass".into(),
            });
        }
        let report = PassReport {
            observed_count: self.pass_observed_count,
            requested_cluster_count: self.config.cluster_count,
            realized_cluster_count: self.config.cluster_count,
            quality_metric: 0.0,
            balance_metric: 0.0,
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: (0..self.config.cluster_count).collect(),
        };
        self.pass_observed_count = 0;
        self.state = TrainerState::PassComplete;
        Ok(report)
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        if self.state != TrainerState::PassComplete {
            let invalid_state = self.state;
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::InvalidTransition {
                state: invalid_state,
                operation: "complete_training".into(),
            });
        }
        self.state = TrainerState::TrainingComplete;
        Ok(())
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        if self.state != TrainerState::TrainingComplete {
            return Err(StreamingClusteringError::InvalidTransition {
                state: self.state,
                operation: "into_classifier".into(),
            });
        }
        let partitions = build_section4_family_partitions(
            &self.ingested_embeddings,
            self.config.cluster_count as usize,
            self.mode,
            self.config.random_seed.unwrap_or(0),
        )?;
        let mut exact_assignments = HashMap::with_capacity(self.ingested_embeddings.len());
        let mut cluster_centroids = Vec::with_capacity(partitions.len());
        for (cluster_id, members) in partitions.iter().enumerate() {
            let centroid = compute_cluster_centroid(&self.ingested_embeddings, members);
            for &member_index in members {
                exact_assignments.insert(
                    embedding_key(&self.ingested_embeddings[member_index]),
                    cluster_id as ClusterId,
                );
            }
            cluster_centroids.push(centroid);
        }
        Ok(Section4FamilyStrategyClassifier {
            config: self.config,
            exact_assignments,
            cluster_centroids,
        })
    }
}

impl StreamingClusterClassifier for Section4FamilyStrategyClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        validate_embedding(embedding, self.config.dimensions)?;
        if let Some(cluster_id) = self.exact_assignments.get(&embedding_key(embedding)) {
            return Ok(*cluster_id);
        }
        let (cluster_id, _) = self
            .cluster_centroids
            .iter()
            .enumerate()
            .map(|(cluster_id, centroid)| {
                (
                    cluster_id as ClusterId,
                    squared_euclidean_distance(embedding, centroid),
                )
            })
            .min_by(|left, right| {
                left.1
                    .partial_cmp(&right.1)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| left.0.cmp(&right.0))
            })
            .expect("section-4 family candidates always produce at least one centroid");
        Ok(cluster_id)
    }
}

fn build_section4_family_partitions(
    embeddings: &[Embedding],
    cluster_count: usize,
    mode: Section4FamilyStrategyMode,
    random_seed: u64,
) -> Result<Vec<Vec<usize>>, StreamingClusteringError> {
    if embeddings.is_empty() {
        return Err(StreamingClusteringError::UnsatisfiableConstraint {
            message: "section-4 family candidates require at least one embedding".into(),
        });
    }
    if cluster_count == 0 || !embeddings.len().is_multiple_of(cluster_count) {
        return Err(StreamingClusteringError::UnsatisfiableConstraint {
            message: format!(
                "section-4 family candidates require an evaluated entity count divisible by cluster_count; observed {} and {}",
                embeddings.len(),
                cluster_count
            ),
        });
    }
    let leaf_size = embeddings.len() / cluster_count;
    let partitions = match mode {
        Section4FamilyStrategyMode::RecursiveBalancedKmeans => {
            recursive_balanced_partitions(embeddings, cluster_count, leaf_size)
        }
        Section4FamilyStrategyMode::SpaceFillingCurveExactChunking => {
            if embeddings[0].len() > 2 {
                return Err(StreamingClusteringError::UnsatisfiableConstraint {
                    message: format!(
                        "space-filling-curve-exact-chunking supports at most 2 dimensions; observed {}",
                        embeddings[0].len()
                    ),
                });
            }
            contiguous_chunks(sorted_indices_by_space_filling_curve(embeddings), leaf_size)
        }
        Section4FamilyStrategyMode::GraphNeighborhoodBalance => {
            graph_neighborhood_partitions(embeddings, cluster_count, leaf_size)
        }
        Section4FamilyStrategyMode::HybridCoarseRebalance => {
            hybrid_coarse_rebalance_partitions(embeddings, cluster_count, leaf_size)
        }
        Section4FamilyStrategyMode::RandomShuffleExactChunking => contiguous_chunks(
            sorted_indices_by_deterministic_hash(embeddings, random_seed),
            leaf_size,
        ),
    };
    if partitions.len() != cluster_count
        || partitions
            .iter()
            .any(|partition| partition.len() != leaf_size)
    {
        return Err(StreamingClusteringError::UnsatisfiableConstraint {
            message: "section-4 family candidates failed to materialize exact-size partitions"
                .into(),
        });
    }
    Ok(partitions)
}

fn recursive_balanced_partitions(
    embeddings: &[Embedding],
    cluster_count: usize,
    leaf_size: usize,
) -> Vec<Vec<usize>> {
    fn recurse(
        embeddings: &[Embedding],
        indices: &[usize],
        cluster_count: usize,
        leaf_size: usize,
        partitions: &mut Vec<Vec<usize>>,
    ) {
        if cluster_count == 1 {
            partitions.push(indices.to_vec());
            return;
        }
        let left_clusters = cluster_count / 2;
        let right_clusters = cluster_count - left_clusters;
        let split_point = left_clusters * leaf_size;
        let axis = widest_variance_dimension(embeddings, indices);
        let mut sorted = indices.to_vec();
        sorted.sort_by(|left, right| {
            embeddings[*left][axis]
                .total_cmp(&embeddings[*right][axis])
                .then_with(|| left.cmp(right))
        });
        let (left, right) = sorted.split_at(split_point);
        recurse(embeddings, left, left_clusters, leaf_size, partitions);
        recurse(embeddings, right, right_clusters, leaf_size, partitions);
    }

    let indices = (0..embeddings.len()).collect::<Vec<_>>();
    let mut partitions = Vec::with_capacity(cluster_count);
    recurse(
        embeddings,
        &indices,
        cluster_count,
        leaf_size,
        &mut partitions,
    );
    partitions
}

fn widest_variance_dimension(embeddings: &[Embedding], indices: &[usize]) -> usize {
    let first_embedding = &embeddings[indices[0]];
    let mut widest_dimension = 0;
    let mut widest_variance = f32::NEG_INFINITY;
    for (dimension, _) in first_embedding.iter().enumerate() {
        let mean = indices
            .iter()
            .map(|index| embeddings[*index][dimension])
            .sum::<f32>()
            / indices.len() as f32;
        let variance = indices
            .iter()
            .map(|index| {
                let delta = embeddings[*index][dimension] - mean;
                delta * delta
            })
            .sum::<f32>();
        if variance > widest_variance {
            widest_variance = variance;
            widest_dimension = dimension;
        }
    }
    widest_dimension
}

fn sorted_indices_by_space_filling_curve(embeddings: &[Embedding]) -> Vec<usize> {
    let dimensions = embeddings[0].len();
    debug_assert!(dimensions > 0);
    let x_dimension = 0;
    let y_dimension = if dimensions > 1 { 1 } else { 0 };
    let (min_x, max_x) = min_max_dimension(embeddings, x_dimension);
    let (min_y, max_y) = min_max_dimension(embeddings, y_dimension);
    let mut indices = (0..embeddings.len()).collect::<Vec<_>>();
    indices.sort_by(|left, right| {
        morton_code(
            embeddings[*left][x_dimension],
            embeddings[*left][y_dimension],
            min_x,
            max_x,
            min_y,
            max_y,
        )
        .cmp(&morton_code(
            embeddings[*right][x_dimension],
            embeddings[*right][y_dimension],
            min_x,
            max_x,
            min_y,
            max_y,
        ))
        .then_with(|| left.cmp(right))
    });
    indices
}

fn min_max_dimension(embeddings: &[Embedding], dimension: usize) -> (f32, f32) {
    embeddings.iter().fold(
        (f32::INFINITY, f32::NEG_INFINITY),
        |(min_value, max_value), embedding| {
            (
                min_value.min(embedding[dimension]),
                max_value.max(embedding[dimension]),
            )
        },
    )
}

fn quantize_to_u16(value: f32, min_value: f32, max_value: f32) -> u16 {
    if max_value <= min_value {
        return 0;
    }
    let normalized = ((value - min_value) / (max_value - min_value)).clamp(0.0, 1.0);
    (normalized * u16::MAX as f32).round() as u16
}

fn morton_code(x: f32, y: f32, min_x: f32, max_x: f32, min_y: f32, max_y: f32) -> u32 {
    let x = quantize_to_u16(x, min_x, max_x);
    let y = quantize_to_u16(y, min_y, max_y);
    let mut code = 0u32;
    for bit in 0..16 {
        code |= (((x >> bit) & 1) as u32) << (bit * 2);
        code |= (((y >> bit) & 1) as u32) << (bit * 2 + 1);
    }
    code
}

fn graph_neighborhood_partitions(
    embeddings: &[Embedding],
    cluster_count: usize,
    leaf_size: usize,
) -> Vec<Vec<usize>> {
    let mut unassigned = (0..embeddings.len()).collect::<HashSet<_>>();
    let mut partitions = Vec::with_capacity(cluster_count);
    while !unassigned.is_empty() {
        let seed = *unassigned
            .iter()
            .min()
            .expect("unassigned set is non-empty");
        unassigned.remove(&seed);
        let mut partition = vec![seed];
        while partition.len() < leaf_size {
            let next = unassigned
                .iter()
                .copied()
                .min_by(|left, right| {
                    average_distance_to_partition(embeddings, *left, &partition)
                        .partial_cmp(&average_distance_to_partition(
                            embeddings, *right, &partition,
                        ))
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| left.cmp(right))
                })
                .expect("leaf_size is guaranteed to be achievable");
            unassigned.remove(&next);
            partition.push(next);
        }
        partitions.push(partition);
    }
    partitions
}

fn average_distance_to_partition(
    embeddings: &[Embedding],
    candidate_index: usize,
    partition: &[usize],
) -> f32 {
    partition
        .iter()
        .map(|member| {
            squared_euclidean_distance(&embeddings[candidate_index], &embeddings[*member])
        })
        .sum::<f32>()
        / partition.len() as f32
}

fn hybrid_coarse_rebalance_partitions(
    embeddings: &[Embedding],
    cluster_count: usize,
    leaf_size: usize,
) -> Vec<Vec<usize>> {
    let coarse_group_count = (cluster_count as f64).sqrt().floor().max(1.0) as usize;
    let coarse_cluster_counts = balanced_cluster_counts(cluster_count, coarse_group_count);
    let mut sorted = (0..embeddings.len()).collect::<Vec<_>>();
    sorted.sort_by(|left, right| {
        embeddings[*left][0]
            .total_cmp(&embeddings[*right][0])
            .then_with(|| left.cmp(right))
    });
    let mut partitions = Vec::with_capacity(cluster_count);
    let mut offset = 0usize;
    for coarse_clusters in coarse_cluster_counts {
        let coarse_size = coarse_clusters * leaf_size;
        let mut coarse_indices = sorted[offset..offset + coarse_size].to_vec();
        offset += coarse_size;
        let secondary_dimension = if embeddings[0].len() > 1 { 1 } else { 0 };
        coarse_indices.sort_by(|left, right| {
            embeddings[*left][secondary_dimension]
                .total_cmp(&embeddings[*right][secondary_dimension])
                .then_with(|| left.cmp(right))
        });
        partitions.extend(contiguous_chunks(coarse_indices, leaf_size));
    }
    partitions
}

fn balanced_cluster_counts(total_clusters: usize, group_count: usize) -> Vec<usize> {
    let base = total_clusters / group_count;
    let remainder = total_clusters % group_count;
    (0..group_count)
        .map(|group_index| base + usize::from(group_index < remainder))
        .collect()
}

fn sorted_indices_by_deterministic_hash(embeddings: &[Embedding], seed: u64) -> Vec<usize> {
    let mut indices = (0..embeddings.len()).collect::<Vec<_>>();
    indices.sort_by(|left, right| {
        deterministic_embedding_hash(&embeddings[*left], seed)
            .cmp(&deterministic_embedding_hash(&embeddings[*right], seed))
            .then_with(|| left.cmp(right))
    });
    indices
}

fn deterministic_embedding_hash(embedding: &[f32], seed: u64) -> u64 {
    let mut hash = 0x517c_c1b7_2722_0a95u64 ^ seed;
    for value in embedding {
        hash ^= u64::from(value.to_bits());
        hash = hash.rotate_left(17).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    }
    hash
}

fn contiguous_chunks(indices: Vec<usize>, leaf_size: usize) -> Vec<Vec<usize>> {
    indices
        .chunks(leaf_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn compute_cluster_centroid(embeddings: &[Embedding], members: &[usize]) -> Embedding {
    let dimensions = embeddings[0].len();
    let mut centroid = vec![0.0; dimensions];
    for &member in members {
        for (dimension, value) in embeddings[member].iter().enumerate() {
            centroid[dimension] += *value;
        }
    }
    for value in &mut centroid {
        *value /= members.len() as f32;
    }
    centroid
}

fn embedding_key(embedding: &[f32]) -> Vec<u32> {
    embedding.iter().map(|value| value.to_bits()).collect()
}

fn squared_euclidean_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left_value, right_value)| {
            let delta = left_value - right_value;
            delta * delta
        })
        .sum()
}

pub fn built_in_fixture_candidate(name: &str) -> Option<RegisteredCandidate> {
    match name {
        "balanced-threshold" => Some(candidate_adapter(
            CandidateIdentity {
                candidate_id: "balanced-threshold".into(),
                implementation_label: "Deterministic threshold fixture".into(),
                software_identity: "fixture-balanced-v1".into(),
            },
            FixtureTrainer::balanced,
        )),
        "skewed-gate-fail" => Some(candidate_adapter(
            CandidateIdentity {
                candidate_id: "skewed-gate-fail".into(),
                implementation_label: "Skewed gate-failing fixture".into(),
                software_identity: "fixture-skewed-v1".into(),
            },
            FixtureTrainer::skewed,
        )),
        "shared-contract-failure" => Some(candidate_adapter(
            CandidateIdentity {
                candidate_id: "shared-contract-failure".into(),
                implementation_label: "Shared-contract failing fixture".into(),
                software_identity: "fixture-failure-v1".into(),
            },
            FixtureTrainer::shared_contract_failure,
        )),
        "nondeterministic-probe" => {
            let creation_counter = Arc::new(AtomicUsize::new(0));
            Some(candidate_adapter(
                CandidateIdentity {
                    candidate_id: "nondeterministic-probe".into(),
                    implementation_label: "Observable nondeterministic fixture".into(),
                    software_identity: "fixture-nondeterministic-v1".into(),
                },
                move |config| FixtureTrainer::nondeterministic(config, creation_counter.clone()),
            ))
        }
        _ => None,
    }
}

pub fn registered_candidate(name: &str) -> Option<RegisteredCandidate> {
    built_in_fixture_candidate(name).or_else(|| match name {
        "recursive-balanced-kmeans" => Some(candidate_adapter(
            CandidateIdentity {
                candidate_id: "recursive-balanced-kmeans".into(),
                implementation_label:
                    "Evaluator-local recursive balanced k-means family representative".into(),
                software_identity: "evaluator-recursive-balanced-kmeans-v1".into(),
            },
            |config| {
                Section4FamilyStrategyTrainer::new(
                    config,
                    Section4FamilyStrategyMode::RecursiveBalancedKmeans,
                )
            },
        )),
        "pca-sort-exact-chunking" => Some(candidate_adapter(
            CandidateIdentity {
                candidate_id: "pca-sort-exact-chunking".into(),
                implementation_label: "Repository-owned PCA sort + exact chunking".into(),
                software_identity: PCA_CHUNKING_SOFTWARE_IDENTITY.into(),
            },
            |config| {
                PcaChunkingStreamingTrainer::new(
                    config.clone(),
                    PcaChunkingParams {
                        retained_dimension_count: 1,
                        variance_exponent: 1.0,
                    },
                )
            },
        )),
        "space-filling-curve-exact-chunking" => Some(candidate_adapter(
            CandidateIdentity {
                candidate_id: "space-filling-curve-exact-chunking".into(),
                implementation_label:
                    "Evaluator-local space-filling-curve ordering + exact chunking baseline".into(),
                software_identity: "evaluator-space-filling-curve-v1".into(),
            },
            |config| {
                Section4FamilyStrategyTrainer::new(
                    config,
                    Section4FamilyStrategyMode::SpaceFillingCurveExactChunking,
                )
            },
        )),
        "directional-pca" => Some(candidate_adapter(
            CandidateIdentity {
                candidate_id: "directional-pca".into(),
                implementation_label: "Repository-owned directional PCA clustering".into(),
                software_identity: DIRECTIONAL_PCA_SOFTWARE_IDENTITY.into(),
            },
            |config| {
                DirectionalPcaStreamingTrainer::new(
                    config.clone(),
                    default_directional_pca_params(),
                )
            },
        )),
        "graph-neighborhood-balance" => Some(candidate_adapter(
            CandidateIdentity {
                candidate_id: "graph-neighborhood-balance".into(),
                implementation_label:
                    "Evaluator-local graph-neighborhood partitioning with exact-size balancing"
                        .into(),
                software_identity: "evaluator-graph-neighborhood-v1".into(),
            },
            |config| {
                Section4FamilyStrategyTrainer::new(
                    config,
                    Section4FamilyStrategyMode::GraphNeighborhoodBalance,
                )
            },
        )),
        "hybrid-coarse-rebalance" => Some(candidate_adapter(
            CandidateIdentity {
                candidate_id: "hybrid-coarse-rebalance".into(),
                implementation_label:
                    "Evaluator-local hybrid coarse partitioning plus local rebalance".into(),
                software_identity: "evaluator-hybrid-coarse-rebalance-v1".into(),
            },
            |config| {
                Section4FamilyStrategyTrainer::new(
                    config,
                    Section4FamilyStrategyMode::HybridCoarseRebalance,
                )
            },
        )),
        "dcbc-streaming" => Some(candidate_adapter(
            CandidateIdentity {
                candidate_id: "dcbc-streaming".into(),
                implementation_label: "Repository-owned streaming DCBC clustering".into(),
                software_identity: DCBC_STREAMING_SOFTWARE_IDENTITY.into(),
            },
            |config| DcbcStreamingTrainer::new(config.clone()),
        )),
        "spherical-kmeans" => Some(candidate_adapter(
            CandidateIdentity {
                candidate_id: "spherical-kmeans".into(),
                implementation_label: "Repository-owned spherical k-means clustering".into(),
                software_identity: SPHERICAL_KMEANS_SOFTWARE_IDENTITY.into(),
            },
            |config| {
                SphericalKmeansStreamingTrainer::new(
                    config.clone(),
                    default_spherical_kmeans_params(),
                )
            },
        )),
        "random-shuffle-exact-chunking" => Some(candidate_adapter(
            CandidateIdentity {
                candidate_id: "random-shuffle-exact-chunking".into(),
                implementation_label: "Evaluator-local deterministic random-shuffle null baseline"
                    .into(),
                software_identity: "evaluator-random-shuffle-v1".into(),
            },
            |config| {
                Section4FamilyStrategyTrainer::new(
                    config,
                    Section4FamilyStrategyMode::RandomShuffleExactChunking,
                )
            },
        )),
        _ => None,
    })
}

#[derive(Clone)]
struct FixtureTrainer {
    config: StreamingClusteringConfig,
    state: TrainerState,
    mode: FixtureMode,
    pass_observed_count: usize,
    pass_index: usize,
    assignment_variant: usize,
}

#[derive(Clone)]
enum FixtureMode {
    BalancedThreshold,
    SkewedGateFail,
    SharedContractFailure,
    NondeterministicProbe,
}

impl FixtureTrainer {
    fn balanced(config: &StreamingClusteringConfig) -> Result<Self, StreamingClusteringError> {
        validate_fixture_config(config)?;
        Ok(Self {
            config: config.clone(),
            state: TrainerState::Idle,
            mode: FixtureMode::BalancedThreshold,
            pass_observed_count: 0,
            pass_index: 0,
            assignment_variant: 0,
        })
    }

    fn skewed(config: &StreamingClusteringConfig) -> Result<Self, StreamingClusteringError> {
        validate_fixture_config(config)?;
        Ok(Self {
            config: config.clone(),
            state: TrainerState::Idle,
            mode: FixtureMode::SkewedGateFail,
            pass_observed_count: 0,
            pass_index: 0,
            assignment_variant: 0,
        })
    }

    fn shared_contract_failure(
        config: &StreamingClusteringConfig,
    ) -> Result<Self, StreamingClusteringError> {
        validate_fixture_config(config)?;
        Ok(Self {
            config: config.clone(),
            state: TrainerState::Idle,
            mode: FixtureMode::SharedContractFailure,
            pass_observed_count: 0,
            pass_index: 0,
            assignment_variant: 0,
        })
    }

    fn nondeterministic(
        config: &StreamingClusteringConfig,
        creation_counter: Arc<AtomicUsize>,
    ) -> Result<Self, StreamingClusteringError> {
        validate_fixture_config(config)?;
        Ok(Self {
            config: config.clone(),
            state: TrainerState::Idle,
            mode: FixtureMode::NondeterministicProbe,
            pass_observed_count: 0,
            pass_index: 0,
            assignment_variant: creation_counter.fetch_add(1, AtomicOrdering::SeqCst) % 2,
        })
    }
}

impl StreamingClusterTrainer for FixtureTrainer {
    type Classifier = FixtureClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn state(&self) -> TrainerState {
        self.state
    }

    fn ingest_batch(&mut self, embeddings: &[Embedding]) -> Result<(), StreamingClusteringError> {
        if !matches!(
            self.state,
            TrainerState::Idle | TrainerState::Ingesting | TrainerState::PassComplete
        ) {
            let invalid_state = self.state;
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::InvalidTransition {
                state: invalid_state,
                operation: "ingest_batch".into(),
            });
        }
        for embedding in embeddings {
            validate_embedding(embedding, self.config.dimensions)?;
        }
        self.pass_observed_count += embeddings.len();
        self.state = TrainerState::Ingesting;
        Ok(())
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        if self.state != TrainerState::Ingesting {
            let invalid_state = self.state;
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::InvalidTransition {
                state: invalid_state,
                operation: "finish_pass".into(),
            });
        }
        if matches!(self.mode, FixtureMode::SharedContractFailure) {
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::UnsatisfiableConstraint {
                message: "fixture requested a shared-contract failure".into(),
            });
        }
        if self.pass_index == 0 && self.pass_observed_count < self.config.cluster_count as usize {
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::UnsatisfiableConstraint {
                message: "fixture observed fewer entities than K on the first pass".into(),
            });
        }

        let report = PassReport {
            observed_count: self.pass_observed_count,
            requested_cluster_count: self.config.cluster_count,
            realized_cluster_count: self.config.cluster_count,
            quality_metric: if matches!(self.mode, FixtureMode::SkewedGateFail) {
                1.0
            } else {
                0.0
            },
            balance_metric: if matches!(self.mode, FixtureMode::SkewedGateFail) {
                1.0
            } else {
                0.0
            },
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: (0..self.config.cluster_count).collect(),
        };
        self.pass_observed_count = 0;
        self.pass_index += 1;
        self.state = TrainerState::PassComplete;
        Ok(report)
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        if self.state != TrainerState::PassComplete {
            let invalid_state = self.state;
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::InvalidTransition {
                state: invalid_state,
                operation: "complete_training".into(),
            });
        }
        self.state = TrainerState::TrainingComplete;
        Ok(())
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        if self.state != TrainerState::TrainingComplete {
            return Err(StreamingClusteringError::InvalidTransition {
                state: self.state,
                operation: "into_classifier".into(),
            });
        }
        Ok(FixtureClassifier {
            config: self.config,
            mode: self.mode,
            assignment_variant: self.assignment_variant,
        })
    }
}

struct FixtureClassifier {
    config: StreamingClusteringConfig,
    mode: FixtureMode,
    assignment_variant: usize,
}

impl StreamingClusterClassifier for FixtureClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        validate_embedding(embedding, self.config.dimensions)?;
        match self.mode {
            FixtureMode::BalancedThreshold => Ok(if self.config.cluster_count == 2 {
                if embedding[0] < 5.0 { 0 } else { 1 }
            } else {
                hashed_fixture_assignment(embedding, self.config.cluster_count, 0)
            }),
            FixtureMode::SkewedGateFail => Ok(0),
            FixtureMode::SharedContractFailure => {
                Err(StreamingClusteringError::InvalidTransition {
                    state: TrainerState::Error,
                    operation: "assign".into(),
                })
            }
            FixtureMode::NondeterministicProbe => {
                if self.config.cluster_count == 2 {
                    let threshold = if self.assignment_variant == 0 {
                        5.0
                    } else {
                        0.15
                    };
                    Ok(if embedding[0] < threshold { 0 } else { 1 })
                } else {
                    Ok(hashed_fixture_assignment(
                        embedding,
                        self.config.cluster_count,
                        self.assignment_variant as u64 + 1,
                    ))
                }
            }
        }
    }
}

fn validate_fixture_config(
    config: &StreamingClusteringConfig,
) -> Result<(), StreamingClusteringError> {
    validate_config(config)?;
    if config.dimensions != 2 {
        return Err(StreamingClusteringError::InvalidConfiguration {
            message: "fixture candidates require dimensions = 2".into(),
        });
    }
    Ok(())
}

fn hashed_fixture_assignment(embedding: &[f32], cluster_count: u32, seed: u64) -> ClusterId {
    let mut hash = 0x9e37_79b9_7f4a_7c15u64 ^ seed;
    for value in embedding {
        hash = hash.rotate_left(13) ^ u64::from(value.to_bits());
        hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    }
    (hash % u64::from(cluster_count)) as ClusterId
}

#[cfg(test)]
mod tests {
    use super::{
        AlignmentPolicy, BenchmarkProfile, BlockStoreCorpusReference, BlockStoreReferenceStore,
        CandidateRunStatus, CompressionBenchmark, CompressionMethod,
        DEFAULT_DEFERRED_HIERARCHY_ROUTING_REASON, DeferredMeasurementStatus, DeferredResearchGoal,
        EmbeddingWorkloadSource, EvaluationEntity, EvaluationEntitySource, GateDeclaration,
        GateKind, GroundTruthNeighborhood, LaterPhaseIdentity, LaterPhaseIdentityKind,
        MetricDeclaration, MetricKind, ProbeWorkload, ReproducibilityMetadata, ResearchCoverage,
        Section4FamilyStrategyMode, SharedCandidateConfig, TEST_FORCE_TEMP_LAYER_FAILURE,
        TrainingPassSource, build_provenance_with_backend, build_section4_family_partitions,
        built_in_fixture_candidate, decode_embedding_to_f32, embeddings_into_batches,
        run_evaluation_campaign,
    };
    use lexongraph_block::EmbeddingSpec;
    use serde_json::json;
    use std::path::PathBuf;

    fn fixture_gate_result(gate_id: &str, status: super::GateStatus) -> super::GateResult {
        super::GateResult {
            gate_id: gate_id.into(),
            label: gate_id.into(),
            coverage: ResearchCoverage::Direct,
            research_goal_ids: Vec::new(),
            status,
            observed_value: None,
            detail: "fixture".into(),
        }
    }

    fn fixture_run_report(
        candidate_id: &str,
        run_status: CandidateRunStatus,
        survived_required_gates: bool,
        ranking_score: Option<f64>,
        packing_ranking_score: Option<f64>,
    ) -> super::CandidateRunReport {
        let identity = super::CandidateIdentity {
            candidate_id: candidate_id.into(),
            implementation_label: format!("{candidate_id} fixture"),
            software_identity: format!("{candidate_id}-fixture-v1"),
        };
        let packing_evaluation =
            packing_ranking_score.map(|score| super::PackingEvaluationReport {
                pipeline_id: super::packing_pipeline_id(candidate_id, "fixture-packer"),
                packer_id: "fixture-packer".into(),
                lower_bound: 1,
                upper_bound: 2,
                packing_elapsed_nanos: 1,
                leaf_membership: Vec::new(),
                cluster_occupancies: Vec::new(),
                cluster_occupancy_stats: super::ClusterOccupancyStats {
                    mean_total_count: 0.0,
                    stddev_total_count: 0.0,
                    min_total_count: 0,
                    max_total_count: 0,
                },
                metric_results: Vec::new(),
                gate_results: Vec::new(),
                survived_required_gates,
                ranking_score: Some(score),
                terminal_failure_code: None,
                terminal_failure_message: None,
                terminal_failure: None,
            });
        super::CandidateRunReport {
            candidate_identity: identity.clone(),
            provenance: build_provenance_with_backend(
                &archive_training_profile_for_tests(),
                &identity,
                vec!["archive-training-pass".into()],
                super::acceleration::fixture_cpu_execution_backend_selection(),
            ),
            prerequisite_checks: Vec::new(),
            pass_reports: Vec::new(),
            probe_results: Vec::new(),
            leaf_membership: Vec::new(),
            cluster_occupancies: Vec::new(),
            cluster_occupancy_stats: None,
            packing_evaluations: packing_evaluation.clone().into_iter().collect(),
            packing_evaluation: packing_evaluation.clone(),
            selected_packing_strategy_id: packing_evaluation
                .as_ref()
                .map(|packing| packing.packer_id.clone()),
            synthetic_padding_concentration: None,
            determinism: super::DeterminismReport {
                deterministic: true,
                compared_fields: Vec::new(),
                mismatch_details: Vec::new(),
            },
            compression_analysis: None,
            metric_results: Vec::new(),
            gate_results: Vec::new(),
            deferred_research_goals: Vec::new(),
            artifact_hygiene: super::ArtifactHygieneEvidence {
                comparative_metrics_emitted: false,
                success_shaped_completion_artifacts_emitted: false,
                detail: "fixture".into(),
            },
            execution_budget_millis: None,
            observed_elapsed_nanos: None,
            run_status,
            raw_survived_required_gates: survived_required_gates,
            survived_required_gates,
            raw_ranking_score: ranking_score,
            ranking_score,
            terminal_failure_code: None,
            terminal_failure_message: None,
            terminal_failure: None,
        }
    }

    fn inline_strict_alignment_profile() -> BenchmarkProfile {
        BenchmarkProfile {
            profile_id: "inline-strict-alignment".into(),
            corpus_ids: vec!["fixture-corpus-a".into()],
            shared_candidate_config: SharedCandidateConfig {
                cluster_count: 2,
                dimensions: 2,
                balance_constraints: None,
                random_seed: Some(7),
            },
            training_passes: vec![
                TrainingPassSource::Inline {
                    batches: vec![
                        vec![vec![0.0, 0.0], vec![0.3, 0.0]],
                        vec![vec![9.9, 0.0], vec![10.2, 0.0]],
                    ],
                },
                TrainingPassSource::Inline {
                    batches: vec![
                        vec![vec![0.0, 0.0], vec![0.3, 0.0]],
                        vec![vec![9.9, 0.0], vec![10.2, 0.0]],
                    ],
                },
            ],
            probe_workloads: vec![ProbeWorkload {
                workload_id: "heldout-probes".into(),
                source: EmbeddingWorkloadSource::Inline {
                    embeddings: vec![vec![0.15, 0.0], vec![10.05, 0.0]],
                },
            }],
            evaluation_entities: EvaluationEntitySource::Inline {
                entities: vec![
                    EvaluationEntity {
                        entity_id: "a".into(),
                        corpus_id: "fixture-corpus-a".into(),
                        embedding: vec![0.0, 0.0],
                        synthetic: false,
                    },
                    EvaluationEntity {
                        entity_id: "b".into(),
                        corpus_id: "fixture-corpus-a".into(),
                        embedding: vec![0.3, 0.0],
                        synthetic: false,
                    },
                    EvaluationEntity {
                        entity_id: "c".into(),
                        corpus_id: "fixture-corpus-a".into(),
                        embedding: vec![9.9, 0.0],
                        synthetic: false,
                    },
                    EvaluationEntity {
                        entity_id: "d".into(),
                        corpus_id: "fixture-corpus-a".into(),
                        embedding: vec![10.2, 0.0],
                        synthetic: false,
                    },
                ],
            },
            leaf_model: super::LeafModel {
                leaf_size: 2,
                declared_final_cluster_count: 2,
                alignment_policy: AlignmentPolicy::StrictAlignment,
            },
            locality_ground_truth: vec![
                GroundTruthNeighborhood {
                    entity_id: "a".into(),
                    neighbor_ids: vec!["b".into()],
                },
                GroundTruthNeighborhood {
                    entity_id: "b".into(),
                    neighbor_ids: vec!["a".into()],
                },
                GroundTruthNeighborhood {
                    entity_id: "c".into(),
                    neighbor_ids: vec!["d".into()],
                },
                GroundTruthNeighborhood {
                    entity_id: "d".into(),
                    neighbor_ids: vec!["c".into()],
                },
            ],
            compression_benchmark: CompressionBenchmark {
                method: CompressionMethod::ScalarQuantization8Bit,
                global_baseline_label: "global-real-dataset-8bit".into(),
            },
            metric_declarations: vec![
                MetricDeclaration {
                    metric_id: "same-leaf-neighborhood-coherence".into(),
                    label: "Same-leaf neighborhood coherence".into(),
                    kind: MetricKind::SameLeafNeighborhoodCoherence,
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-LOCALITY".into()],
                    ranking_weight: 1.0,
                },
                MetricDeclaration {
                    metric_id: "local-compression-gain".into(),
                    label: "Local compression gain".into(),
                    kind: MetricKind::LocalCompressionGain,
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-COMPRESSION".into()],
                    ranking_weight: 0.25,
                },
            ],
            gate_declarations: vec![
                GateDeclaration {
                    gate_id: "exact-leaf-occupancy".into(),
                    label: "Exact leaf occupancy".into(),
                    kind: GateKind::ExactLeafOccupancy,
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
                },
                GateDeclaration {
                    gate_id: "leaf-size-lower-bound".into(),
                    label: "Leaf size lower bound".into(),
                    kind: GateKind::LeafSizeAtLeast { minimum: 1 },
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
                },
                GateDeclaration {
                    gate_id: "leaf-size-upper-bound".into(),
                    label: "Leaf size upper bound".into(),
                    kind: GateKind::LeafSizeAtMost { maximum: 2 },
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
                },
                GateDeclaration {
                    gate_id: "complete-coverage".into(),
                    label: "Complete coverage".into(),
                    kind: GateKind::CompleteCoverage,
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-COVERAGE".into()],
                },
                GateDeclaration {
                    gate_id: "one-cluster-per-entity".into(),
                    label: "One cluster per entity".into(),
                    kind: GateKind::OneClusterPerEntity,
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-COVERAGE".into()],
                },
                GateDeclaration {
                    gate_id: "deterministic-observable-results".into(),
                    label: "Deterministic observable results".into(),
                    kind: GateKind::DeterministicObservableResults,
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-DETERMINISM".into()],
                },
            ],
            packing_strategy_ids: super::registered_packing_strategy_names(),
            deferred_research_goals: vec![DeferredResearchGoal {
                deferred_id: "deferred-hierarchy-routing".into(),
                label: "Hierarchy routing proof".into(),
                reason: DEFAULT_DEFERRED_HIERARCHY_ROUTING_REASON.into(),
                research_goal_ids: vec!["RG-HIERARCHY".into(), "RG-ROUTING".into()],
                coverage: ResearchCoverage::Deferred,
                later_evaluation_line: "future hierarchy-routing evaluator".into(),
            }],
            later_phase_identities: vec![LaterPhaseIdentity {
                identity_id: "fixture-heldout-query-set".into(),
                label: "Fixture held-out query set".into(),
                kind: LaterPhaseIdentityKind::HeldOutQuerySet,
                corpus_id: Some("fixture-corpus-a".into()),
                scale_tier_id: None,
                asset_path: Some(PathBuf::from("fixtures/heldout-queries.zip")),
                later_evaluation_line: "future hierarchy-routing evaluator".into(),
            }],
            reproducibility: ReproducibilityMetadata {
                seed_policy: "fixed-seed-7".into(),
                software_identity: "fixture-campaign-builder".into(),
                floating_point_profile: "ieee754-deterministic-no-fma".into(),
                hardware_profile: "fixture-cpu".into(),
                candidate_threading_model: "host-scaled deterministic candidate execution".into(),
                reduction_order_strategy: "deterministic stable input-order reduction".into(),
            },
        }
    }

    #[test]
    fn embeddings_into_batches_preserves_order_without_dropping_tail_items() {
        let batches = embeddings_into_batches(
            vec![vec![1.0], vec![2.0], vec![3.0], vec![4.0], vec![5.0]],
            2,
        );

        assert_eq!(
            batches,
            vec![
                vec![vec![1.0], vec![2.0]],
                vec![vec![3.0], vec![4.0]],
                vec![vec![5.0]]
            ]
        );
    }

    #[test]
    fn decode_embedding_to_f32_rejects_f32_byte_length_overflow() {
        let spec = EmbeddingSpec {
            dims: (usize::MAX / 4 + 1) as u64,
            encoding: "f32le".into(),
        };

        let error = decode_embedding_to_f32(&[], &spec, "overflowing f32 corpus")
            .expect_err("overflowing f32 dimensions should be rejected");

        assert!(error.contains("overflowed usize"));
    }

    #[test]
    fn decode_embedding_to_f32_rejects_f16_byte_length_overflow() {
        let spec = EmbeddingSpec {
            dims: (usize::MAX / 2 + 1) as u64,
            encoding: "f16le".into(),
        };

        let error = decode_embedding_to_f32(&[], &spec, "overflowing f16 corpus")
            .expect_err("overflowing f16 dimensions should be rejected");

        assert!(error.contains("overflowed usize"));
    }

    #[test]
    fn archive_backed_resolution_reports_temporary_layer_failures_without_public_hooks() {
        struct TempLayerFailureReset;
        impl Drop for TempLayerFailureReset {
            fn drop(&mut self) {
                TEST_FORCE_TEMP_LAYER_FAILURE.with(|flag| flag.set(false));
            }
        }

        TEST_FORCE_TEMP_LAYER_FAILURE.with(|flag| flag.set(true));
        let _reset = TempLayerFailureReset;

        let report = run_evaluation_campaign(
            &archive_training_profile_for_tests(),
            &[built_in_fixture_candidate("balanced-threshold").unwrap()],
        )
        .unwrap();

        assert_eq!(
            report.run_reports[0].run_status,
            CandidateRunStatus::CorpusSourceFailure
        );
        assert!(matches!(
            report.run_reports[0].terminal_failure,
            Some(super::StructuredFailure::ArchiveSourceTemporaryLayerFailure { .. })
        ));
        assert_eq!(
            report.run_reports[0].deferred_research_goals[0].status,
            DeferredMeasurementStatus::Deferred
        );
    }

    #[test]
    fn block_store_reference_store_prefers_tagged_forms_over_legacy_shape() {
        let parsed: super::BlockStoreReferenceStore = serde_json::from_value(json!({
            "store_kind": "zip-archive",
            "archive_path": r"C:\archive.zip",
            "store_root": r"C:\ignored-if-legacy-wins"
        }))
        .expect("tagged zip-archive shape should deserialize as a zip-backed reference");

        assert_eq!(
            parsed,
            super::BlockStoreReferenceStore::ZipArchive {
                archive_path: super::normalize_cross_platform_path(r"C:\archive.zip"),
            }
        );
    }

    #[test]
    fn space_filling_curve_exact_chunking_rejects_embeddings_above_two_dimensions() {
        let error = build_section4_family_partitions(
            &[vec![0.0, 0.0, 0.0], vec![1.0, 1.0, 1.0]],
            1,
            Section4FamilyStrategyMode::SpaceFillingCurveExactChunking,
            7,
        )
        .expect_err("space-filling curve candidate should reject dimensions above two");

        assert!(matches!(
            error,
            lexongraph_streaming_clustering::StreamingClusteringError::UnsatisfiableConstraint {
                message
            } if message.contains("at most 2 dimensions")
        ));
    }

    #[test]
    fn build_provenance_records_execution_backend_selection() {
        let profile = archive_training_profile_for_tests();
        let identity = super::CandidateIdentity {
            candidate_id: "balanced".into(),
            implementation_label: "Balanced fixture".into(),
            software_identity: "balanced-fixture-v1".into(),
        };

        let provenance = build_provenance_with_backend(
            &profile,
            &identity,
            vec!["archive-training-pass".into()],
            super::acceleration::fixture_cpu_execution_backend_selection(),
        );

        assert_eq!(
            provenance.execution_backend,
            super::acceleration::fixture_cpu_execution_backend_selection()
        );
    }

    #[test]
    fn render_scorecard_reports_execution_backend_resolution() {
        let report = run_evaluation_campaign(
            &archive_training_profile_for_tests(),
            &[built_in_fixture_candidate("balanced-threshold").unwrap()],
        )
        .unwrap();

        let scorecard = super::render_scorecard(&report);

        assert!(scorecard.contains("execution-backend:"));
        assert!(
            scorecard.contains("wgpu-unsupported-fallback")
                || scorecard.contains("wgpu-declined")
                || scorecard.contains("wgpu")
                || scorecard.contains("cpu")
        );
    }

    #[test]
    fn render_scorecard_reports_cluster_size_stats() {
        let report = super::CampaignReport {
            profile_id: "fixture".into(),
            run_reports: vec![super::CandidateRunReport {
                candidate_identity: super::CandidateIdentity {
                    candidate_id: "balanced".into(),
                    implementation_label: "Balanced fixture".into(),
                    software_identity: "balanced-fixture-v1".into(),
                },
                provenance: build_provenance_with_backend(
                    &archive_training_profile_for_tests(),
                    &super::CandidateIdentity {
                        candidate_id: "balanced".into(),
                        implementation_label: "Balanced fixture".into(),
                        software_identity: "balanced-fixture-v1".into(),
                    },
                    vec!["archive-training-pass".into()],
                    super::acceleration::fixture_cpu_execution_backend_selection(),
                ),
                prerequisite_checks: Vec::new(),
                pass_reports: Vec::new(),
                probe_results: Vec::new(),
                leaf_membership: Vec::new(),
                cluster_occupancies: vec![
                    super::ClusterOccupancy {
                        cluster_id: 0,
                        total_count: 63,
                        real_count: 63,
                        synthetic_count: 0,
                    },
                    super::ClusterOccupancy {
                        cluster_id: 1,
                        total_count: 65,
                        real_count: 65,
                        synthetic_count: 0,
                    },
                ],
                cluster_occupancy_stats: Some(super::ClusterOccupancyStats {
                    mean_total_count: 64.0,
                    stddev_total_count: 1.0,
                    min_total_count: 63,
                    max_total_count: 65,
                }),
                packing_evaluations: Vec::new(),
                packing_evaluation: None,
                selected_packing_strategy_id: None,
                synthetic_padding_concentration: None,
                determinism: super::DeterminismReport {
                    deterministic: true,
                    compared_fields: Vec::new(),
                    mismatch_details: Vec::new(),
                },
                compression_analysis: None,
                metric_results: Vec::new(),
                gate_results: Vec::new(),
                deferred_research_goals: Vec::new(),
                artifact_hygiene: super::ArtifactHygieneEvidence {
                    comparative_metrics_emitted: false,
                    success_shaped_completion_artifacts_emitted: false,
                    detail: "fixture".into(),
                },
                execution_budget_millis: None,
                observed_elapsed_nanos: None,
                run_status: super::CandidateRunStatus::GateFailed,
                raw_survived_required_gates: false,
                survived_required_gates: false,
                raw_ranking_score: None,
                ranking_score: None,
                terminal_failure_code: None,
                terminal_failure_message: None,
                terminal_failure: None,
            }],
            ranking: Vec::new(),
            raw_ranking: Vec::new(),
            packing_pipeline_ranking: Vec::new(),
        };

        let scorecard = super::render_scorecard(&report);

        assert!(
            scorecard.contains("cluster-size-stats: mean=64.000, stddev=1.000, min=63, max=65")
        );
    }

    #[test]
    fn evaluation_campaign_reports_multiple_packing_strategies() {
        let report =
            super::with_execution_backend_request(super::ExecutionBackendRequest::Cpu, || {
                run_evaluation_campaign(
                    &inline_strict_alignment_profile(),
                    &[built_in_fixture_candidate("balanced-threshold").unwrap()],
                )
            })
            .expect("inline fixture profile should evaluate successfully");

        assert_eq!(report.run_reports.len(), 1);
        assert_eq!(report.run_reports[0].packing_evaluations.len(), 3);
        assert!(!report.packing_pipeline_ranking.is_empty());
        assert!(
            report.packing_pipeline_ranking.len()
                <= report.run_reports[0].packing_evaluations.len()
        );
        assert!(report.run_reports[0].selected_packing_strategy_id.is_some());
        assert!(
            report.run_reports[0]
                .packing_evaluations
                .iter()
                .any(|packing| packing.survived_required_gates)
        );
        assert!(
            report
                .packing_pipeline_ranking
                .iter()
                .all(|pipeline| pipeline.candidate_id == "balanced-threshold")
        );
    }

    #[test]
    fn failing_packing_strategy_does_not_disqualify_successful_candidate() {
        let mut profile = inline_strict_alignment_profile();
        profile.packing_strategy_ids = vec![
            super::BALANCED_RANGE_PACKER_ID.into(),
            super::FAILING_TEST_PACKER_ID.into(),
        ];

        let report =
            super::with_execution_backend_request(super::ExecutionBackendRequest::Cpu, || {
                run_evaluation_campaign(
                    &profile,
                    &[built_in_fixture_candidate("balanced-threshold").unwrap()],
                )
            })
            .expect("test-only failing packer should be isolated inside the campaign report");

        let run_report = &report.run_reports[0];
        assert_eq!(run_report.run_status, CandidateRunStatus::Succeeded);
        assert_eq!(
            run_report.selected_packing_strategy_id.as_deref(),
            Some(super::BALANCED_RANGE_PACKER_ID)
        );
        assert_eq!(run_report.packing_evaluations.len(), 2);
        assert_eq!(report.packing_pipeline_ranking.len(), 1);
        assert!(run_report.terminal_failure.is_none());
        assert!(
            run_report
                .packing_evaluations
                .iter()
                .any(|packing| packing.packer_id == super::FAILING_TEST_PACKER_ID
                    && !packing.survived_required_gates)
        );
    }

    #[test]
    fn all_failing_packing_strategies_disqualify_candidate() {
        let mut profile = inline_strict_alignment_profile();
        profile.packing_strategy_ids = vec![super::FAILING_TEST_PACKER_ID.into()];

        let report =
            super::with_execution_backend_request(super::ExecutionBackendRequest::Cpu, || {
                run_evaluation_campaign(
                    &profile,
                    &[built_in_fixture_candidate("balanced-threshold").unwrap()],
                )
            })
            .expect("all-failing packers should still produce a deterministic campaign report");

        let run_report = &report.run_reports[0];
        assert_eq!(run_report.run_status, CandidateRunStatus::GateFailed);
        assert!(!run_report.survived_required_gates);
        assert!(run_report.selected_packing_strategy_id.is_none());
        assert_eq!(report.packing_pipeline_ranking.len(), 0);
        assert!(run_report.terminal_failure.is_some());
    }

    #[test]
    fn balanced_range_packer_accepts_empty_leaf_membership() {
        let profile = inline_strict_alignment_profile();
        let empty_execution = super::SingleExecution {
            provenance: build_provenance_with_backend(
                &profile,
                &super::CandidateIdentity {
                    candidate_id: "empty".into(),
                    implementation_label: "Empty fixture".into(),
                    software_identity: "empty-fixture-v1".into(),
                },
                Vec::new(),
                super::acceleration::fixture_cpu_execution_backend_selection(),
            ),
            pass_reports: Vec::new(),
            probe_results: Vec::new(),
            leaf_membership: Vec::new(),
            cluster_occupancies: Vec::new(),
            evaluation_entities: Vec::new(),
        };

        let packed = super::build_balanced_range_packed_execution(&empty_execution, &profile)
            .expect("empty leaf membership should pack to an empty result");

        assert!(packed.leaf_membership.is_empty());
        assert!(packed.cluster_occupancies.is_empty());
    }

    #[test]
    fn select_best_packing_evaluation_matches_pipeline_ranking_tie_break() {
        let left = super::PackingEvaluationReport {
            pipeline_id: super::packing_pipeline_id("fixture", "alpha-packer"),
            packer_id: "alpha-packer".into(),
            lower_bound: 1,
            upper_bound: 2,
            packing_elapsed_nanos: 1,
            leaf_membership: Vec::new(),
            cluster_occupancies: Vec::new(),
            cluster_occupancy_stats: super::ClusterOccupancyStats {
                mean_total_count: 0.0,
                stddev_total_count: 0.0,
                min_total_count: 0,
                max_total_count: 0,
            },
            metric_results: Vec::new(),
            gate_results: Vec::new(),
            survived_required_gates: true,
            ranking_score: Some(1.0),
            terminal_failure_code: None,
            terminal_failure_message: None,
            terminal_failure: None,
        };
        let right = super::PackingEvaluationReport {
            pipeline_id: super::packing_pipeline_id("fixture", "beta-packer"),
            packer_id: "beta-packer".into(),
            ..left.clone()
        };

        let selected = super::select_best_packing_evaluation(&[right.clone(), left.clone()])
            .expect("one tied winner should be selected deterministically");

        let ranked = super::rank_packing_pipelines(&[super::CandidateRunReport {
            candidate_identity: super::CandidateIdentity {
                candidate_id: "fixture".into(),
                implementation_label: "Fixture".into(),
                software_identity: "fixture-v1".into(),
            },
            provenance: build_provenance_with_backend(
                &archive_training_profile_for_tests(),
                &super::CandidateIdentity {
                    candidate_id: "fixture".into(),
                    implementation_label: "Fixture".into(),
                    software_identity: "fixture-v1".into(),
                },
                vec!["archive-training-pass".into()],
                super::acceleration::fixture_cpu_execution_backend_selection(),
            ),
            prerequisite_checks: Vec::new(),
            pass_reports: Vec::new(),
            probe_results: Vec::new(),
            leaf_membership: Vec::new(),
            cluster_occupancies: Vec::new(),
            cluster_occupancy_stats: None,
            packing_evaluations: vec![right, left.clone()],
            packing_evaluation: Some(left.clone()),
            selected_packing_strategy_id: Some(left.packer_id.clone()),
            synthetic_padding_concentration: None,
            determinism: super::DeterminismReport {
                deterministic: true,
                compared_fields: Vec::new(),
                mismatch_details: Vec::new(),
            },
            compression_analysis: None,
            metric_results: Vec::new(),
            gate_results: Vec::new(),
            deferred_research_goals: Vec::new(),
            artifact_hygiene: super::ArtifactHygieneEvidence {
                comparative_metrics_emitted: true,
                success_shaped_completion_artifacts_emitted: true,
                detail: "fixture".into(),
            },
            execution_budget_millis: None,
            observed_elapsed_nanos: None,
            run_status: super::CandidateRunStatus::Succeeded,
            raw_survived_required_gates: true,
            survived_required_gates: true,
            raw_ranking_score: Some(1.0),
            ranking_score: Some(1.0),
            terminal_failure_code: None,
            terminal_failure_message: None,
            terminal_failure: None,
        }]);

        assert_eq!(selected.packer_id, ranked[0].packing_strategy_id);
    }

    #[test]
    fn required_gate_status_distinguishes_missing_passed_and_failed_required_gates() {
        assert_eq!(
            super::evaluate_required_gate_status(&[], super::CLUSTERING_STAGE_REQUIRED_GATE_IDS,),
            None
        );
        assert_eq!(
            super::evaluate_required_gate_status(
                &[fixture_gate_result(
                    "complete-coverage",
                    super::GateStatus::Passed,
                )],
                super::CLUSTERING_STAGE_REQUIRED_GATE_IDS,
            ),
            None
        );
        assert_eq!(
            super::evaluate_required_gate_status(
                &[
                    fixture_gate_result("complete-coverage", super::GateStatus::Passed),
                    fixture_gate_result("one-cluster-per-entity", super::GateStatus::Passed),
                    fixture_gate_result(
                        "deterministic-observable-results",
                        super::GateStatus::Passed,
                    ),
                ],
                super::CLUSTERING_STAGE_REQUIRED_GATE_IDS,
            ),
            Some(true)
        );
        assert_eq!(
            super::evaluate_required_gate_status(
                &[fixture_gate_result(
                    "complete-coverage",
                    super::GateStatus::Failed,
                )],
                super::CLUSTERING_STAGE_REQUIRED_GATE_IDS,
            ),
            Some(false)
        );
    }

    #[test]
    fn rank_candidates_ignores_non_survivors_even_when_they_have_scores() {
        let ranked = super::rank_candidates(&[
            fixture_run_report(
                "disqualified",
                CandidateRunStatus::GateFailed,
                false,
                Some(99.0),
                Some(99.0),
            ),
            fixture_run_report(
                "packed-survivor",
                CandidateRunStatus::Succeeded,
                true,
                Some(0.1),
                Some(2.0),
            ),
            fixture_run_report(
                "raw-survivor",
                CandidateRunStatus::Succeeded,
                true,
                Some(1.0),
                None,
            ),
        ]);

        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].candidate_id, "packed-survivor");
        assert_eq!(ranked[1].candidate_id, "raw-survivor");
    }

    #[test]
    fn host_scaled_threading_provenance_uses_rayon_thread_count() {
        let provenance =
            super::candidate_threading_provenance(&archive_training_profile_for_tests());
        assert_eq!(provenance.effective_mode, "host-scaled");
        assert_eq!(
            provenance.effective_thread_count,
            rayon::current_num_threads()
        );
    }

    #[test]
    fn provenance_manifest_deserializes_without_candidate_threading_or_backend() {
        let parsed: super::ProvenanceManifest = serde_json::from_value(json!({
            "profile_id": "fixture-profile",
            "corpus_ids": ["fixture-corpus-a"],
            "source_reference_ids": ["fixture-source"],
            "candidate_identity": {
                "candidate_id": "balanced",
                "implementation_label": "Balanced fixture",
                "software_identity": "balanced-fixture-v1"
            },
            "shared_candidate_config": {
                "cluster_count": 2,
                "dimensions": 2,
                "balance_constraints": null,
                "random_seed": 7
            },
            "seed_policy": "fixed-seed-7",
            "software_identity": "fixture-campaign-builder",
            "floating_point_profile": "ieee754-deterministic-no-fma",
            "hardware_profile": "fixture-cpu"
        }))
        .expect("older artifacts should deserialize with default threading/backend provenance");

        assert_eq!(
            parsed.candidate_threading,
            super::CandidateThreadingProvenance::default()
        );
        assert_eq!(
            parsed.execution_backend,
            super::ExecutionBackendSelection::default()
        );
    }

    fn archive_training_profile_for_tests() -> BenchmarkProfile {
        BenchmarkProfile {
            profile_id: "archive-temp-layer-failure".into(),
            corpus_ids: vec!["fixture-corpus-a".into()],
            shared_candidate_config: SharedCandidateConfig {
                cluster_count: 2,
                dimensions: 2,
                balance_constraints: None,
                random_seed: Some(7),
            },
            training_passes: vec![TrainingPassSource::BlockStore {
                corpus: BlockStoreCorpusReference {
                    source_id: "archive-training-pass".into(),
                    root_block_id:
                        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".into(),
                    store: BlockStoreReferenceStore::ZipArchive {
                        archive_path: PathBuf::from(r"C:\temp\unused-for-forced-failure.zip"),
                    },
                },
                batch_size: 2,
            }],
            probe_workloads: vec![ProbeWorkload {
                workload_id: "heldout-probes".into(),
                source: EmbeddingWorkloadSource::Inline {
                    embeddings: vec![vec![0.15, 0.0], vec![10.05, 0.0]],
                },
            }],
            evaluation_entities: EvaluationEntitySource::Inline {
                entities: vec![
                    EvaluationEntity {
                        entity_id: "a".into(),
                        corpus_id: "fixture-corpus-a".into(),
                        embedding: vec![0.0, 0.0],
                        synthetic: false,
                    },
                    EvaluationEntity {
                        entity_id: "b".into(),
                        corpus_id: "fixture-corpus-a".into(),
                        embedding: vec![0.3, 0.0],
                        synthetic: false,
                    },
                    EvaluationEntity {
                        entity_id: "c".into(),
                        corpus_id: "fixture-corpus-a".into(),
                        embedding: vec![9.9, 0.0],
                        synthetic: false,
                    },
                    EvaluationEntity {
                        entity_id: "d".into(),
                        corpus_id: "fixture-corpus-a".into(),
                        embedding: vec![10.2, 0.0],
                        synthetic: false,
                    },
                ],
            },
            leaf_model: super::LeafModel {
                leaf_size: 2,
                declared_final_cluster_count: 2,
                alignment_policy: AlignmentPolicy::StrictAlignment,
            },
            locality_ground_truth: vec![
                GroundTruthNeighborhood {
                    entity_id: "a".into(),
                    neighbor_ids: vec!["b".into()],
                },
                GroundTruthNeighborhood {
                    entity_id: "b".into(),
                    neighbor_ids: vec!["a".into()],
                },
                GroundTruthNeighborhood {
                    entity_id: "c".into(),
                    neighbor_ids: vec!["d".into()],
                },
                GroundTruthNeighborhood {
                    entity_id: "d".into(),
                    neighbor_ids: vec!["c".into()],
                },
            ],
            compression_benchmark: CompressionBenchmark {
                method: CompressionMethod::ScalarQuantization8Bit,
                global_baseline_label: "global-real-dataset-8bit".into(),
            },
            metric_declarations: vec![
                MetricDeclaration {
                    metric_id: "same-leaf-neighborhood-coherence".into(),
                    label: "Same-leaf neighborhood coherence".into(),
                    kind: MetricKind::SameLeafNeighborhoodCoherence,
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-LOCALITY".into()],
                    ranking_weight: 1.0,
                },
                MetricDeclaration {
                    metric_id: "local-compression-gain".into(),
                    label: "Local compression gain".into(),
                    kind: MetricKind::LocalCompressionGain,
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-COMPRESSION".into()],
                    ranking_weight: 0.25,
                },
            ],
            gate_declarations: vec![
                GateDeclaration {
                    gate_id: "exact-leaf-occupancy".into(),
                    label: "Exact leaf occupancy".into(),
                    kind: GateKind::ExactLeafOccupancy,
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
                },
                GateDeclaration {
                    gate_id: "leaf-size-lower-bound".into(),
                    label: "Leaf size lower bound".into(),
                    kind: GateKind::LeafSizeAtLeast { minimum: 1 },
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
                },
                GateDeclaration {
                    gate_id: "leaf-size-upper-bound".into(),
                    label: "Leaf size upper bound".into(),
                    kind: GateKind::LeafSizeAtMost { maximum: 2 },
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
                },
                GateDeclaration {
                    gate_id: "complete-coverage".into(),
                    label: "Complete coverage".into(),
                    kind: GateKind::CompleteCoverage,
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-COVERAGE".into()],
                },
                GateDeclaration {
                    gate_id: "one-cluster-per-entity".into(),
                    label: "One cluster per entity".into(),
                    kind: GateKind::OneClusterPerEntity,
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-COVERAGE".into()],
                },
                GateDeclaration {
                    gate_id: "deterministic-observable-results".into(),
                    label: "Deterministic observable results".into(),
                    kind: GateKind::DeterministicObservableResults,
                    coverage: ResearchCoverage::Direct,
                    research_goal_ids: vec!["RG-DETERMINISM".into()],
                },
            ],
            packing_strategy_ids: super::registered_packing_strategy_names(),
            deferred_research_goals: vec![DeferredResearchGoal {
                deferred_id: "deferred-hierarchy-routing".into(),
                label: "Hierarchy routing proof".into(),
                reason: DEFAULT_DEFERRED_HIERARCHY_ROUTING_REASON.into(),
                research_goal_ids: vec!["RG-HIERARCHY".into(), "RG-ROUTING".into()],
                coverage: ResearchCoverage::Deferred,
                later_evaluation_line: "future hierarchy-routing evaluator".into(),
            }],
            later_phase_identities: vec![LaterPhaseIdentity {
                identity_id: "fixture-heldout-query-set".into(),
                label: "Fixture held-out query set".into(),
                kind: LaterPhaseIdentityKind::HeldOutQuerySet,
                corpus_id: Some("fixture-corpus-a".into()),
                scale_tier_id: None,
                asset_path: Some(PathBuf::from("fixtures/heldout-queries.zip")),
                later_evaluation_line: "future hierarchy-routing evaluator".into(),
            }],
            reproducibility: ReproducibilityMetadata {
                seed_policy: "fixed-seed-7".into(),
                software_identity: "fixture-campaign-builder".into(),
                floating_point_profile: "ieee754-deterministic-no-fma".into(),
                hardware_profile: "fixture-cpu".into(),
                candidate_threading_model: "host-scaled deterministic candidate execution".into(),
                reduction_order_strategy: "deterministic stable input-order reduction".into(),
            },
        }
    }
}
