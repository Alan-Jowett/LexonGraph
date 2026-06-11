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

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use ciborium::value::Value as CborValue;
use half::f16;
use lexongraph_block::{BlockHash, EmbeddingSpec, LeafEntry, Metadata, TypedEntries, into_entries};
use lexongraph_block_store::BlockStore;
use lexongraph_block_store_fs::FilesystemBlockStore;
use lexongraph_streaming_clustering::{
    ClusterId, Embedding, MetricDirection, PassReport, StreamingClusterClassifier,
    StreamingClusterTrainer, StreamingClusteringConfig, StreamingClusteringError, TrainerState,
    validate_config, validate_embedding,
};
use serde::{Deserialize, Serialize};

pub type PassPlan = Vec<Vec<Embedding>>;

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
            quality_metric: value.quality_metric,
            balance_metric: value.balance_metric,
            quality_direction: value.quality_direction.into(),
            balance_direction: value.balance_direction.into(),
            cluster_ids: value.cluster_ids,
        }
    }
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
    pub deferred_research_goals: Vec<DeferredResearchGoal>,
    pub reproducibility: ReproducibilityMetadata,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BlockStoreCorpusReference {
    pub source_id: String,
    pub store_root: PathBuf,
    pub root_block_id: String,
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
    CompleteCoverage,
    OneClusterPerEntity,
    NoEmptyDeclaredClusters,
    DeterministicObservableResults,
    MetricAtLeast { metric_id: String, minimum: f64 },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeferredResearchGoal {
    pub deferred_id: String,
    pub label: String,
    pub reason: String,
    pub research_goal_ids: Vec<String>,
    pub coverage: ResearchCoverage,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReproducibilityMetadata {
    pub seed_policy: String,
    pub software_identity: String,
    pub floating_point_profile: String,
    pub hardware_profile: String,
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CandidateRunReport {
    pub candidate_identity: CandidateIdentity,
    pub provenance: ProvenanceManifest,
    pub prerequisite_checks: Vec<PrerequisiteCheckResult>,
    pub pass_reports: Vec<ObservablePassReport>,
    pub probe_results: Vec<ProbeAssignmentResult>,
    pub leaf_membership: Vec<LeafMembershipRecord>,
    pub cluster_occupancies: Vec<ClusterOccupancy>,
    pub determinism: DeterminismReport,
    pub metric_results: Vec<MetricResult>,
    pub gate_results: Vec<GateResult>,
    pub deferred_research_goals: Vec<DeferredResearchGoalResult>,
    pub run_status: CandidateRunStatus,
    pub survived_required_gates: bool,
    pub ranking_score: Option<f64>,
    pub terminal_failure: Option<StructuredFailure>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RankedCandidate {
    pub candidate_id: String,
    pub ranking_score: f64,
    pub rank: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CampaignReport {
    pub profile_id: String,
    pub run_reports: Vec<CandidateRunReport>,
    pub ranking: Vec<RankedCandidate>,
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

pub fn candidate_adapter<F, T>(identity: CandidateIdentity, factory: F) -> RegisteredCandidate
where
    F: Fn(&StreamingClusteringConfig) -> Result<T, StreamingClusteringError>
        + Send
        + Sync
        + 'static,
    T: StreamingClusterTrainer + 'static,
    T::Classifier: 'static,
{
    RegisteredCandidate {
        identity,
        factory: Box::new(factory),
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

    Ok(CampaignReport {
        profile_id: profile.profile_id.clone(),
        run_reports,
        ranking,
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
            .probe_workloads
            .iter()
            .map(|workload| workload.workload_id.as_str()),
        "probe workload ids",
    )?;
    assert_unique(
        iter_declared_source_reference_ids(profile),
        "corpus source ids",
    )?;

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
    if reference.store_root.as_os_str().is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "block-store corpus reference {} must declare a non-empty store_root",
            reference.source_id
        )));
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
        determinism: DeterminismReport {
            deterministic: false,
            compared_fields: determinism_compared_fields(),
            mismatch_details: vec!["candidate execution did not complete".into()],
        },
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
            })
            .collect(),
        run_status: CandidateRunStatus::CandidateSharedContractFailure,
        survived_required_gates: false,
        ranking_score: None,
        terminal_failure: Some(StructuredFailure::CandidateSharedContractFailure {
            candidate_id: identity.candidate_id.clone(),
            message: error.to_string(),
        }),
    }
}

fn failed_corpus_source_run(
    profile: &BenchmarkProfile,
    identity: &CandidateIdentity,
    failure: StructuredFailure,
) -> CandidateRunReport {
    let provenance = build_provenance(profile, identity, declared_source_reference_ids(profile));
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
        determinism: DeterminismReport {
            deterministic: false,
            compared_fields: determinism_compared_fields(),
            mismatch_details: vec!["corpus source resolution did not complete".into()],
        },
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
            })
            .collect(),
        run_status: CandidateRunStatus::CorpusSourceFailure,
        survived_required_gates: false,
        ranking_score: None,
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
    let metric_results = compute_metric_results(&primary, profile);
    let gate_results = compute_gate_results(profile, &primary, &metric_results, &determinism);
    let survived_required_gates = gate_results
        .iter()
        .all(|gate| gate.status == GateStatus::Passed);
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
        let failed_gate = gate_results
            .iter()
            .find(|gate| gate.status == GateStatus::Failed)
            .expect("a non-surviving candidate must have a failed gate");
        Some(StructuredFailure::GateFailure {
            candidate_id: identity.candidate_id.clone(),
            gate_id: failed_gate.gate_id.clone(),
            message: failed_gate.detail.clone(),
        })
    };

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
        determinism,
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
            })
            .collect(),
        run_status: if survived_required_gates {
            CandidateRunStatus::Succeeded
        } else {
            CandidateRunStatus::GateFailed
        },
        survived_required_gates,
        ranking_score,
        terminal_failure,
    }
}

fn compute_metric_results(
    execution: &SingleExecution,
    profile: &BenchmarkProfile,
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
                MetricKind::LocalCompressionGain => local_compression_gain(
                    &execution.leaf_membership,
                    &execution.evaluation_entities,
                    &profile.compression_benchmark,
                ),
            },
        })
        .collect()
}

fn compute_gate_results(
    profile: &BenchmarkProfile,
    execution: &SingleExecution,
    metric_results: &[MetricResult],
    determinism: &DeterminismReport,
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
        .filter_map(|run_report| {
            run_report
                .ranking_score
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
    let mut pass_reports = Vec::with_capacity(resolved.training_passes.len());
    for pass in &resolved.training_passes {
        for batch in pass {
            trainer.ingest_batch(batch)?;
        }
        pass_reports.push(trainer.finish_pass()?.into());
    }
    trainer.complete_training()?;
    let classifier = trainer.into_classifier()?;

    let probe_results = resolved
        .probe_workloads
        .iter()
        .map(|workload| {
            let assignments = classifier.assign_batch(&workload.embeddings)?;
            Ok(ProbeAssignmentResult {
                workload_id: workload.workload_id.clone(),
                assignments: validate_cluster_assignments(
                    assignments,
                    profile.leaf_model.declared_final_cluster_count,
                    &format!("probe workload {}", workload.workload_id),
                )?,
            })
        })
        .collect::<Result<Vec<_>, StreamingClusteringError>>()?;

    let leaf_membership = resolved
        .evaluation_entities
        .iter()
        .map(|entity| {
            Ok(LeafMembershipRecord {
                entity_id: entity.entity_id.clone(),
                cluster_id: validate_cluster_id(
                    classifier.assign(&entity.embedding)?,
                    profile.leaf_model.declared_final_cluster_count,
                    &format!("evaluation entity {}", entity.entity_id),
                )?,
                synthetic: entity.synthetic,
            })
        })
        .collect::<Result<Vec<_>, StreamingClusteringError>>()?;

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

fn build_provenance(
    profile: &BenchmarkProfile,
    identity: &CandidateIdentity,
    source_reference_ids: Vec<String>,
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
    }
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
    let mut source_reference_ids = BTreeMap::<String, ()>::new();

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

    Ok(ResolvedProfileInputs {
        training_passes,
        probe_workloads,
        evaluation_entities,
        source_reference_ids: source_reference_ids.into_keys().collect(),
    })
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
struct LoadedLeafRecord {
    block_id: BlockHash,
    embedding_spec: EmbeddingSpec,
    entry: LeafEntry,
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

fn load_leaf_records(
    reference: &BlockStoreCorpusReference,
) -> Result<Vec<LoadedLeafRecord>, CandidateExecutionError> {
    let root_block_id = parse_block_hash_hex(&reference.root_block_id)
        .map_err(|message| invalid_corpus_source_reference(&reference.source_id, message))?;
    let store = FilesystemBlockStore::new(&reference.store_root).map_err(|error| {
        corpus_source_load_failure(
            &reference.source_id,
            format!(
                "failed to open block store {}: {error}",
                reference.store_root.display()
            ),
        )
    })?;
    let mut records = Vec::new();
    let mut visited = HashSet::new();
    collect_leaf_records(&store, reference, root_block_id, &mut visited, &mut records)?;
    Ok(records)
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

    let validated = store
        .get(&block_id)
        .map_err(|error| {
            corpus_source_load_failure(
                &reference.source_id,
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

fn required_metadata_bool(
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

fn metadata_value<'a>(metadata: &'a Metadata, key: &str) -> Option<&'a CborValue> {
    metadata
        .iter()
        .find_map(|(candidate, value)| match candidate {
            CborValue::Text(text) if text == key => Some(value),
            _ => None,
        })
}

fn decode_embedding_to_f32(
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

fn parse_block_hash_hex(value: &str) -> Result<BlockHash, String> {
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

fn local_compression_gain(
    leaf_membership: &[LeafMembershipRecord],
    evaluation_entities: &[EvaluationEntity],
    compression_benchmark: &CompressionBenchmark,
) -> f64 {
    match compression_benchmark.method {
        CompressionMethod::ScalarQuantization8Bit => {
            let real_entities = evaluation_entities
                .iter()
                .filter(|entity| !entity.synthetic)
                .collect::<Vec<_>>();
            if real_entities.is_empty() {
                return 0.0;
            }

            let global_error = scalar_quantization_error(&real_entities);
            if global_error == 0.0 {
                return 0.0;
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

            let local_error_sum = entities_by_cluster
                .values()
                .map(|entities| scalar_quantization_error(entities))
                .sum::<f64>();

            1.0 - (local_error_sum / global_error)
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

trait DynClassifier {
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
    T::Classifier: 'static,
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
    T::Classifier: 'static,
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
    C: StreamingClusterClassifier + 'static,
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
            FixtureMode::BalancedThreshold => Ok(if embedding[0] < 5.0 { 0 } else { 1 }),
            FixtureMode::SkewedGateFail => Ok(0),
            FixtureMode::SharedContractFailure => {
                Err(StreamingClusteringError::InvalidTransition {
                    state: TrainerState::Error,
                    operation: "assign".into(),
                })
            }
            FixtureMode::NondeterministicProbe => {
                let threshold = if self.assignment_variant == 0 {
                    5.0
                } else {
                    0.15
                };
                Ok(if embedding[0] < threshold { 0 } else { 1 })
            }
        }
    }
}

fn validate_fixture_config(
    config: &StreamingClusteringConfig,
) -> Result<(), StreamingClusteringError> {
    validate_config(config)?;
    if config.cluster_count != 2 || config.dimensions != 2 {
        return Err(StreamingClusteringError::InvalidConfiguration {
            message: "fixture candidates require cluster_count = 2 and dimensions = 2".into(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{decode_embedding_to_f32, embeddings_into_batches};
    use lexongraph_block::EmbeddingSpec;

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
}
