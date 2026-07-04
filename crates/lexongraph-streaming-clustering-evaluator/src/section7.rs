// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use ciborium::value::Value as CborValue;
use lexongraph_block::{
    BlockHash, Content, EmbeddingSpec, LeafEntry, Metadata, TypedEntries, into_entries,
};
use lexongraph_block_store::BlockStore;
use lexongraph_block_store_fs::FilesystemBlockStore;
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_search::{
    CandidateScorer, DefaultEmbeddingCompatibility, EmbeddingCompatibility, EncodedTargetEmbedding,
    SearchTelemetrySummary, SearchTerminationKind,
};
use lexongraph_streaming_indexer::{
    ChildSummaryPolicy, ExactCentroidChildSummaryPolicy, FinalizedPartition,
    FinalizedPartitionHierarchy, HierarchicalPlanningPolicy, IndexItem, PlanningPassOutcome,
    PlanningStage, StreamingIndexerError, StreamingIndexingRun,
};
use serde::{Deserialize, Serialize};

use crate::section5::{Section5CampaignReport, Section5HierarchyNodeKind, Section5PairReport};
use crate::section6::{
    Section6CampaignReport, Section6DeferredGoalRecord, Section6SummaryCandidateKind,
    Section6SummaryReport, Section6SummaryRunStatus,
};
use crate::{
    BenchmarkProfile, CandidateIdentity, EvaluationEntity, EvaluatorError,
    Section5HierarchyStrategyIdentity, decode_embedding_to_f32, metadata_value,
    resolved_profile_evaluation_entities,
};

const SECTION7_LATENCY_REASON: &str = "service-level latency and QPS benchmarking remain deferred beyond the first executable section-7 routing slice and must be discharged by a later service-level evaluation line";
const SECTION7_PERSISTENCE_REASON: &str = "serialization identity, persisted-artifact durability, and broader robustness checks remain deferred beyond the first executable section-7 routing slice and must be discharged by the later persistence and robustness evaluation line";
const SECTION7_ENTITY_ID_METADATA_KEY: &str = "entity_id";
const SECTION7_SYNTHETIC_METADATA_KEY: &str = "synthetic";
const SECTION7_BEAM_WIDTHS: [usize; 5] = [1, 2, 4, 8, 16];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section7RunStatus {
    Succeeded,
    DeferredUnsupportedSummary,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section7HeldOutQuery {
    pub query_id: String,
    pub corpus_id: String,
    pub embedding: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section7QueryReport {
    pub query_id: String,
    pub beam_width: usize,
    pub returned_neighbor_ids: Vec<String>,
    pub exact_neighbor_ids: Vec<String>,
    pub tnn_at_1: f64,
    pub tnn_at_5: f64,
    pub tnn_at_10: f64,
    pub nodes_visited: usize,
    pub routing_depth: usize,
    pub termination: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section7BeamReport {
    pub beam_width: usize,
    pub query_count: usize,
    pub mean_tnn_at_1: f64,
    pub mean_tnn_at_5: f64,
    pub mean_tnn_at_10: f64,
    pub mean_nodes_visited: f64,
    pub mean_routing_depth: f64,
    pub termination_counts: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section7DesignReport {
    pub leaf_candidate_identity: CandidateIdentity,
    pub hierarchy_strategy_identity: Section5HierarchyStrategyIdentity,
    pub summary_candidate_identity: crate::Section6SummaryCandidateIdentity,
    pub originating_section4_profile_id: String,
    pub originating_section5_contract_id: String,
    pub originating_section6_contract_id: String,
    pub held_out_query_set_ids: Vec<String>,
    pub query_count: usize,
    pub indexed_entity_count: usize,
    pub beam_reports: Vec<Section7BeamReport>,
    pub query_reports: Vec<Section7QueryReport>,
    pub run_status: Section7RunStatus,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ranking_score: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section7RankedDesign {
    pub leaf_candidate_id: String,
    pub hierarchy_strategy_id: String,
    pub summary_candidate_id: String,
    pub ranking_score: f64,
    pub rank: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section7DeferredGoalRecord {
    pub deferred_id: String,
    pub label: String,
    pub reason: String,
    pub later_evaluation_line: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section7CampaignReport {
    pub section4_profile_id: String,
    pub section5_contract_id: String,
    pub section6_contract_id: String,
    pub design_reports: Vec<Section7DesignReport>,
    pub ranking: Vec<Section7RankedDesign>,
    pub remaining_deferred_goals: Vec<Section7DeferredGoalRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Section7CampaignArtifacts {
    pub per_design_reports: Vec<crate::EmittedArtifact>,
    pub campaign_report: crate::EmittedArtifact,
    pub scorecard: crate::EmittedArtifact,
    pub carry_forward_summary: crate::EmittedArtifact,
}

#[derive(Clone, Debug)]
struct MaterializedEntity {
    entity_id: String,
    encoded_embedding: Vec<u8>,
    metadata: Metadata,
}

#[derive(Clone)]
struct MaterializedEntityResolver {
    entities: Vec<MaterializedEntity>,
}

impl lexongraph_streaming_indexer::ContentResolver<usize> for MaterializedEntityResolver {
    type Error = EvaluatorError;

    fn resolve(&self, content_ref: &usize) -> Result<Content, Self::Error> {
        let entity = self.entities.get(*content_ref).ok_or_else(|| {
            EvaluatorError::InvalidConfiguration(format!(
                "section-7 materialization referenced missing entity index {}",
                content_ref
            ))
        })?;
        Ok(Content {
            media_type: "application/lexongraph-section7-entity-id".into(),
            body: entity.entity_id.as_bytes().to_vec(),
        })
    }

    fn fingerprint(&self, content_ref: &usize) -> Result<BlockHash, Self::Error> {
        let entity = self.entities.get(*content_ref).ok_or_else(|| {
            EvaluatorError::InvalidConfiguration(format!(
                "section-7 materialization referenced missing entity index {}",
                content_ref
            ))
        })?;
        Ok(hash_entity_id(entity.entity_id.as_bytes()))
    }
}

#[derive(Clone)]
struct MaterializedEntityEmbeddingProvider {
    embeddings_by_entity_id: HashMap<Vec<u8>, Vec<u8>>,
}

impl EmbeddingProvider for MaterializedEntityEmbeddingProvider {
    type Error = EvaluatorError;

    async fn embed(
        &self,
        input: &EmbeddingInput,
        _: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        self.embeddings_by_entity_id
            .get(&input.body)
            .cloned()
            .ok_or_else(|| {
                EvaluatorError::InvalidConfiguration(format!(
                    "section-7 materialization could not resolve embedding for entity payload {:?}",
                    String::from_utf8_lossy(&input.body)
                ))
            })
    }
}

#[derive(Clone)]
struct FixedHierarchyPlanningPolicy {
    hierarchy: FinalizedPartitionHierarchy,
}

struct MaterializationInputs<'a> {
    hierarchy: &'a FinalizedPartitionHierarchy,
    embedding_spec: EmbeddingSpec,
    block_size_target: usize,
    items: &'a [IndexItem<usize>],
    store: &'a FilesystemBlockStore,
}

impl HierarchicalPlanningPolicy for FixedHierarchyPlanningPolicy {
    type Error = EvaluatorError;

    fn finish_planning_pass(
        &mut self,
        _: &[Vec<f32>],
        _: &EmbeddingSpec,
        _: usize,
        _: usize,
    ) -> Result<PlanningPassOutcome, Self::Error> {
        Ok(PlanningPassOutcome {
            hierarchy: self.hierarchy.clone(),
            requested_cluster_count: None,
            realized_cluster_count: None,
            planning_quality_metric: 1.0,
            planning_balance_metric: 0.0,
            planning_quality_direction:
                lexongraph_streaming_indexer::MetricDirection::LargerIsBetter,
            planning_balance_direction:
                lexongraph_streaming_indexer::MetricDirection::SmallerIsBetter,
            stages_used: [PlanningStage::Custom].into_iter().collect(),
        })
    }
}

#[derive(Clone, Copy)]
struct EuclideanCandidateScorer;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EuclideanScore(u64);

impl CandidateScorer<EncodedTargetEmbedding> for EuclideanCandidateScorer {
    type Error = std::io::Error;
    type Score = EuclideanScore;

    fn score(
        &self,
        target: &EncodedTargetEmbedding,
        candidate_embedding: &[u8],
        embedding_spec: &EmbeddingSpec,
    ) -> Result<Self::Score, Self::Error> {
        let target_values = decode_embedding_to_f32(
            target.bytes.as_slice(),
            embedding_spec,
            "section-7 euclidean target",
        )
        .map_err(std::io::Error::other)?;
        let candidate_values = decode_embedding_to_f32(
            candidate_embedding,
            embedding_spec,
            "section-7 euclidean candidate",
        )
        .map_err(std::io::Error::other)?;
        let distance = squared_euclidean_distance(&target_values, &candidate_values)
            .map_err(std::io::Error::other)?;
        Ok(EuclideanScore(total_order_key_f64(-distance)))
    }
}

#[derive(Deserialize)]
struct QuerySetDocument {
    #[serde(default)]
    corpus_id: Option<String>,
    #[serde(default)]
    queries: Vec<QueryDocument>,
    #[serde(default)]
    query_entity_ids: Vec<String>,
}

#[derive(Deserialize)]
struct QueryDocument {
    entity_id: String,
    embedding: Vec<f32>,
}

pub fn run_section7_campaign(
    profile: &BenchmarkProfile,
    section5_campaign: &Section5CampaignReport,
    section6_campaign: &Section6CampaignReport,
) -> Result<Section7CampaignReport, EvaluatorError> {
    if section5_campaign.section4_profile_id != profile.profile_id {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "section-7 profile id {} does not match section-5 profile id {}",
            profile.profile_id, section5_campaign.section4_profile_id
        )));
    }
    if section6_campaign.section4_profile_id != profile.profile_id {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "section-7 profile id {} does not match section-6 profile id {}",
            profile.profile_id, section6_campaign.section4_profile_id
        )));
    }
    if section6_campaign.section5_contract_id != section5_campaign.hierarchy_contract.contract_id {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "section-7 section-6 report expects section-5 contract {}, observed {}",
            section6_campaign.section5_contract_id,
            section5_campaign.hierarchy_contract.contract_id
        )));
    }

    let evaluation_entities = resolved_profile_evaluation_entities(profile)?;
    let real_entities = evaluation_entities
        .iter()
        .filter(|entity| !entity.synthetic)
        .cloned()
        .collect::<Vec<_>>();
    if real_entities.len() < 2 {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-7 execution requires at least two real evaluation entities".into(),
        ));
    }
    let held_out_queries = resolve_held_out_queries(profile, &real_entities)?;
    let held_out_query_set_ids = held_out_query_set_ids(profile);

    let pair_lookup = section5_campaign
        .pair_reports
        .iter()
        .map(|pair| {
            (
                (
                    pair.leaf_candidate_identity.candidate_id.as_str(),
                    pair.hierarchy_strategy_identity.strategy_id.as_str(),
                ),
                pair,
            )
        })
        .collect::<HashMap<_, _>>();
    let survivor_lookup = section5_campaign
        .section4_campaign
        .run_reports
        .iter()
        .map(|run| (run.candidate_identity.candidate_id.as_str(), run))
        .collect::<HashMap<_, _>>();

    let mut design_reports = Vec::new();
    for summary_report in &section6_campaign.summary_reports {
        let pair = pair_lookup
            .get(&(
                summary_report.leaf_candidate_identity.candidate_id.as_str(),
                summary_report
                    .hierarchy_strategy_identity
                    .strategy_id
                    .as_str(),
            ))
            .copied()
            .ok_or_else(|| {
                EvaluatorError::InvalidConfiguration(format!(
                    "section-7 could not find section-5 pair for candidate {} and strategy {}",
                    summary_report.leaf_candidate_identity.candidate_id,
                    summary_report.hierarchy_strategy_identity.strategy_id
                ))
            })?;
        let survivor = survivor_lookup
            .get(summary_report.leaf_candidate_identity.candidate_id.as_str())
            .copied()
            .ok_or_else(|| {
                EvaluatorError::InvalidConfiguration(format!(
                    "section-7 could not find section-4 survivor report for candidate {}",
                    summary_report.leaf_candidate_identity.candidate_id
                ))
            })?;

        design_reports.push(run_section7_design(
            summary_report,
            &section6_campaign.summary_contract.contract_id,
            pair,
            survivor,
            &real_entities,
            &held_out_query_set_ids,
            &held_out_queries,
        )?);
    }

    let ranking = rank_designs(&mut design_reports);
    Ok(Section7CampaignReport {
        section4_profile_id: profile.profile_id.clone(),
        section5_contract_id: section5_campaign.hierarchy_contract.contract_id.clone(),
        section6_contract_id: section6_campaign.summary_contract.contract_id.clone(),
        design_reports,
        ranking,
        remaining_deferred_goals: remaining_deferred_goals(
            &section6_campaign.remaining_deferred_goals,
        ),
    })
}

pub fn emit_section7_campaign_artifacts(
    report: &Section7CampaignReport,
) -> Result<Section7CampaignArtifacts, EvaluatorError> {
    let per_design_reports = report
        .design_reports
        .iter()
        .map(|design| {
            Ok(crate::EmittedArtifact {
                file_name: format!(
                    "section7-{}-{}-{}.json",
                    design.leaf_candidate_identity.candidate_id,
                    design.hierarchy_strategy_identity.strategy_id,
                    design.summary_candidate_identity.summary_candidate_id
                ),
                contents: serde_json::to_string_pretty(design)
                    .map_err(|error| EvaluatorError::Json(error.to_string()))?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let campaign_report = crate::EmittedArtifact {
        file_name: "section7-campaign-report.json".into(),
        contents: serde_json::to_string_pretty(report)
            .map_err(|error| EvaluatorError::Json(error.to_string()))?,
    };
    let scorecard = crate::EmittedArtifact {
        file_name: "section7-scorecard.txt".into(),
        contents: render_section7_scorecard(report),
    };
    let carry_forward_summary = crate::EmittedArtifact {
        file_name: "section7-carry-forward-summary.txt".into(),
        contents: render_section7_carry_forward_summary(report),
    };
    Ok(Section7CampaignArtifacts {
        per_design_reports,
        campaign_report,
        scorecard,
        carry_forward_summary,
    })
}

pub fn write_section7_campaign_artifacts(
    output_dir: &Path,
    artifacts: &Section7CampaignArtifacts,
) -> Result<Vec<std::path::PathBuf>, EvaluatorError> {
    std::fs::create_dir_all(output_dir).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to create section-7 output directory {}: {error}",
            output_dir.display()
        ))
    })?;
    let mut paths = Vec::new();
    for artifact in artifacts.per_design_reports.iter().chain([
        &artifacts.campaign_report,
        &artifacts.scorecard,
        &artifacts.carry_forward_summary,
    ]) {
        let path = output_dir.join(&artifact.file_name);
        std::fs::write(&path, &artifact.contents).map_err(|error| {
            EvaluatorError::Io(format!("failed to write {}: {error}", path.display()))
        })?;
        paths.push(path);
    }
    Ok(paths)
}

pub fn render_section7_scorecard(report: &Section7CampaignReport) -> String {
    let mut lines = vec![format!(
        "Section 7 routing scorecard for profile {}",
        report.section4_profile_id
    )];
    for design in &report.design_reports {
        lines.push(format!(
            "- {} × {} × {} [{}]",
            design.leaf_candidate_identity.candidate_id,
            design.hierarchy_strategy_identity.strategy_id,
            design.summary_candidate_identity.summary_candidate_id,
            design_status_label(design)
        ));
        for beam in &design.beam_reports {
            lines.push(format!(
                "  beam {:>2}: TNN@1 {:.3}, TNN@5 {:.3}, TNN@10 {:.3}, nodes {:.2}, depth {:.2}",
                beam.beam_width,
                beam.mean_tnn_at_1,
                beam.mean_tnn_at_5,
                beam.mean_tnn_at_10,
                beam.mean_nodes_visited,
                beam.mean_routing_depth
            ));
        }
        if !design.detail.is_empty() {
            lines.push(format!("  detail: {}", design.detail));
        }
    }
    lines.join("\n")
}

pub fn render_section7_carry_forward_summary(report: &Section7CampaignReport) -> String {
    let mut lines = vec![format!(
        "Section 7 carry-forward summary for profile {}",
        report.section4_profile_id
    )];
    for ranked in &report.ranking {
        lines.push(format!(
            "{}. {} × {} × {} => {:.6}",
            ranked.rank,
            ranked.leaf_candidate_id,
            ranked.hierarchy_strategy_id,
            ranked.summary_candidate_id,
            ranked.ranking_score
        ));
    }
    if !report.remaining_deferred_goals.is_empty() {
        lines.push(String::new());
        lines.push("Remaining deferred goals:".into());
        for deferred in &report.remaining_deferred_goals {
            lines.push(format!("- {}: {}", deferred.deferred_id, deferred.label));
        }
    }
    lines.join("\n")
}

fn run_section7_design(
    summary_report: &Section6SummaryReport,
    section6_contract_id: &str,
    pair_report: &Section5PairReport,
    survivor_report: &crate::CandidateRunReport,
    real_entities: &[EvaluationEntity],
    held_out_query_set_ids: &[String],
    held_out_queries: &[Section7HeldOutQuery],
) -> Result<Section7DesignReport, EvaluatorError> {
    let supported_summary_policy = match summary_report.summary_candidate_identity.kind {
        Section6SummaryCandidateKind::ExactCentroid => Some(SummaryPolicyKind::ExactCentroid),
        Section6SummaryCandidateKind::ComposedCentroid => Some(SummaryPolicyKind::ComposedCentroid),
        Section6SummaryCandidateKind::CentroidPlusVarianceScalar
        | Section6SummaryCandidateKind::LowRankCentroidPrincipalDirection => None,
    };
    if summary_report.run_status != Section6SummaryRunStatus::Succeeded {
        return Ok(Section7DesignReport {
            leaf_candidate_identity: summary_report.leaf_candidate_identity.clone(),
            hierarchy_strategy_identity: summary_report.hierarchy_strategy_identity.clone(),
            summary_candidate_identity: summary_report.summary_candidate_identity.clone(),
            originating_section4_profile_id: summary_report.originating_section4_profile_id.clone(),
            originating_section5_contract_id: summary_report
                .originating_section5_contract_id
                .clone(),
            originating_section6_contract_id: section6_contract_id.into(),
            held_out_query_set_ids: held_out_query_set_ids.to_vec(),
            query_count: held_out_queries.len(),
            indexed_entity_count: real_entities.len(),
            beam_reports: Vec::new(),
            query_reports: Vec::new(),
            run_status: Section7RunStatus::Failed,
            detail: "section-6 design did not survive required gates".into(),
            ranking_score: None,
        });
    }
    let Some(summary_policy) = supported_summary_policy else {
        return Ok(Section7DesignReport {
            leaf_candidate_identity: summary_report.leaf_candidate_identity.clone(),
            hierarchy_strategy_identity: summary_report.hierarchy_strategy_identity.clone(),
            summary_candidate_identity: summary_report.summary_candidate_identity.clone(),
            originating_section4_profile_id: summary_report.originating_section4_profile_id.clone(),
            originating_section5_contract_id: summary_report.originating_section5_contract_id.clone(),
            originating_section6_contract_id: section6_contract_id.into(),
            held_out_query_set_ids: held_out_query_set_ids.to_vec(),
            query_count: held_out_queries.len(),
            indexed_entity_count: real_entities.len(),
            beam_reports: Vec::new(),
            query_reports: Vec::new(),
            run_status: Section7RunStatus::DeferredUnsupportedSummary,
            detail: "summary family is not centroid-compatible with the current single-embedding branch-entry model".into(),
            ranking_score: None,
        });
    };

    let materialized_entities = build_materialized_entities(real_entities)?;
    let hierarchy = build_real_only_hierarchy(pair_report, survivor_report, real_entities)?;
    let block_size_target = recommended_block_size_target(
        materialized_entities.len(),
        materialized_entities[0].encoded_embedding.len(),
    );
    let design_tree = materialize_design_tree(
        &materialized_entities,
        &hierarchy,
        summary_policy,
        block_size_target,
    )?;
    let exact_neighbor_ids_by_query = held_out_queries
        .iter()
        .map(|query| {
            exact_top_neighbors(
                query,
                real_entities,
                &summary_report.metric_semantics_profile,
            )
        })
        .collect::<Result<Vec<_>, EvaluatorError>>()?;

    let mut query_reports = Vec::new();
    for &beam_width in &SECTION7_BEAM_WIDTHS {
        for (query, exact_neighbor_ids) in held_out_queries.iter().zip(&exact_neighbor_ids_by_query)
        {
            let predicted_neighbor_ids = pollster::block_on(search_neighbors(
                &design_tree.store,
                design_tree.root_id,
                query,
                exact_neighbor_ids.len(),
                materialized_entities.len(),
                beam_width,
                &summary_report.metric_semantics_profile,
            ))?;
            query_reports.push(build_query_report(
                query,
                beam_width,
                exact_neighbor_ids.clone(),
                predicted_neighbor_ids.0,
                &predicted_neighbor_ids.1,
            ));
        }
    }
    let beam_reports = build_beam_reports(&query_reports);

    Ok(Section7DesignReport {
        leaf_candidate_identity: summary_report.leaf_candidate_identity.clone(),
        hierarchy_strategy_identity: summary_report.hierarchy_strategy_identity.clone(),
        summary_candidate_identity: summary_report.summary_candidate_identity.clone(),
        originating_section4_profile_id: summary_report.originating_section4_profile_id.clone(),
        originating_section5_contract_id: summary_report.originating_section5_contract_id.clone(),
        originating_section6_contract_id: section6_contract_id.into(),
        held_out_query_set_ids: held_out_query_set_ids.to_vec(),
        query_count: held_out_queries.len(),
        indexed_entity_count: materialized_entities.len(),
        beam_reports,
        query_reports,
        run_status: Section7RunStatus::Succeeded,
        detail: String::new(),
        ranking_score: None,
    })
}

#[derive(Clone, Copy)]
enum SummaryPolicyKind {
    ExactCentroid,
    ComposedCentroid,
}

fn build_materialized_entities(
    real_entities: &[EvaluationEntity],
) -> Result<Vec<MaterializedEntity>, EvaluatorError> {
    if real_entities.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-7 requires at least one real evaluation entity".into(),
        ));
    }
    let dimensions = real_entities[0].embedding.len();
    for entity in real_entities {
        if entity.embedding.len() != dimensions {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "section-7 real entity {} had dimension {}, expected {}",
                entity.entity_id,
                entity.embedding.len(),
                dimensions
            )));
        }
    }
    Ok(real_entities
        .iter()
        .map(|entity| MaterializedEntity {
            entity_id: entity.entity_id.clone(),
            encoded_embedding: encode_f32_embedding(&entity.embedding),
            metadata: vec![
                (
                    CborValue::Text(SECTION7_ENTITY_ID_METADATA_KEY.into()),
                    CborValue::Text(entity.entity_id.clone()),
                ),
                (
                    CborValue::Text(SECTION7_SYNTHETIC_METADATA_KEY.into()),
                    CborValue::Bool(false),
                ),
            ],
        })
        .collect())
}

fn build_real_only_hierarchy(
    pair_report: &Section5PairReport,
    survivor_report: &crate::CandidateRunReport,
    real_entities: &[EvaluationEntity],
) -> Result<FinalizedPartitionHierarchy, EvaluatorError> {
    let real_entity_lookup = real_entities
        .iter()
        .enumerate()
        .map(|(index, entity)| (entity.entity_id.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut leaf_items = BTreeMap::<String, Vec<usize>>::new();
    for membership in survivor_report.effective_leaf_membership() {
        if let Some(&index) = real_entity_lookup.get(membership.entity_id.as_str()) {
            leaf_items
                .entry(format!("leaf-{}", membership.cluster_id))
                .or_default()
                .push(index);
        }
    }
    for (leaf_id, item_indices) in &mut leaf_items {
        normalize_item_indices(leaf_id, item_indices)?;
    }

    let node_kinds = pair_report
        .hierarchy_nodes
        .iter()
        .map(|node| (node.node_id.as_str(), node.kind.clone()))
        .collect::<HashMap<_, _>>();
    let mut child_map = BTreeMap::<String, Vec<String>>::new();
    for edge in &pair_report.hierarchy_edges {
        child_map
            .entry(edge.parent_node_id.clone())
            .or_default()
            .push(edge.child_node_id.clone());
    }
    let root_ids = pair_report
        .hierarchy_nodes
        .iter()
        .filter(|node| node.depth_from_root == 0)
        .map(|node| node.node_id.clone())
        .collect::<Vec<_>>();
    let [root_id] = root_ids.as_slice() else {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-7 expected exactly one hierarchy root at depth 0".into(),
        ));
    };

    let mut partitions = Vec::new();
    let Some(root_partition) = prune_and_collect_partitions(
        root_id,
        None,
        &node_kinds,
        &child_map,
        &leaf_items,
        &mut partitions,
    )?
    else {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-7 real-only pruning removed every partition".into(),
        ));
    };
    Ok(FinalizedPartitionHierarchy {
        root_partition_id: root_partition.id,
        partitions,
    })
}

#[derive(Clone, Debug)]
struct PrunedPartitionSummary {
    id: String,
    item_indices: Vec<usize>,
}

fn prune_and_collect_partitions(
    node_id: &str,
    parent_id: Option<&str>,
    node_kinds: &HashMap<&str, Section5HierarchyNodeKind>,
    child_map: &BTreeMap<String, Vec<String>>,
    leaf_items: &BTreeMap<String, Vec<usize>>,
    partitions: &mut Vec<FinalizedPartition>,
) -> Result<Option<PrunedPartitionSummary>, EvaluatorError> {
    let kind = node_kinds.get(node_id).ok_or_else(|| {
        EvaluatorError::InvalidConfiguration(format!(
            "section-7 hierarchy node {} was missing from the section-5 node set",
            node_id
        ))
    })?;
    if matches!(kind, Section5HierarchyNodeKind::LeafCluster) {
        let Some(item_indices) = leaf_items.get(node_id).cloned() else {
            return Ok(None);
        };
        if item_indices.is_empty() {
            return Ok(None);
        }
        partitions.push(FinalizedPartition {
            id: node_id.into(),
            parent_id: parent_id.map(str::to_owned),
            child_ids: Vec::new(),
            item_indices: item_indices.clone(),
            terminal: true,
            planning_stage: PlanningStage::Custom,
        });
        return Ok(Some(PrunedPartitionSummary {
            id: node_id.into(),
            item_indices,
        }));
    }

    let mut child_summaries = Vec::new();
    for child_id in child_map.get(node_id).into_iter().flatten() {
        if let Some(pruned_child) = prune_and_collect_partitions(
            child_id,
            Some(node_id),
            node_kinds,
            child_map,
            leaf_items,
            partitions,
        )? {
            child_summaries.push(pruned_child);
        }
    }
    if child_summaries.is_empty() {
        return Ok(None);
    }
    if child_summaries.len() == 1 {
        let single_child = child_summaries.remove(0);
        if let Some(partition) = partitions
            .iter_mut()
            .find(|partition| partition.id == single_child.id)
        {
            partition.parent_id = parent_id.map(str::to_owned);
        }
        return Ok(Some(single_child));
    }
    let child_ids = child_summaries
        .iter()
        .map(|child| child.id.clone())
        .collect::<Vec<_>>();
    let mut item_indices = child_summaries
        .into_iter()
        .flat_map(|child| child.item_indices)
        .collect::<Vec<_>>();
    normalize_item_indices(node_id, &mut item_indices)?;
    partitions.push(FinalizedPartition {
        id: node_id.into(),
        parent_id: parent_id.map(str::to_owned),
        child_ids,
        item_indices: item_indices.clone(),
        terminal: false,
        planning_stage: PlanningStage::Custom,
    });
    Ok(Some(PrunedPartitionSummary {
        id: node_id.into(),
        item_indices,
    }))
}

fn normalize_item_indices(node_id: &str, item_indices: &mut [usize]) -> Result<(), EvaluatorError> {
    item_indices.sort_unstable();
    for pair in item_indices.windows(2) {
        if pair[0] == pair[1] {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "section-7 real-only hierarchy contains duplicate item index {} in partition {}",
                pair[0], node_id
            )));
        }
    }
    Ok(())
}

struct MaterializedDesignTree {
    _store_root: tempfile::TempDir,
    store: FilesystemBlockStore,
    root_id: BlockHash,
}

fn materialize_design_tree(
    materialized_entities: &[MaterializedEntity],
    hierarchy: &FinalizedPartitionHierarchy,
    summary_policy: SummaryPolicyKind,
    block_size_target: usize,
) -> Result<MaterializedDesignTree, EvaluatorError> {
    let embedding_spec = EmbeddingSpec {
        dims: u64::try_from(materialized_entities[0].encoded_embedding.len() / 4).map_err(
            |_| EvaluatorError::InvalidConfiguration("section-7 dimensions overflowed u64".into()),
        )?,
        encoding: "f32le".into(),
    };
    let resolver = MaterializedEntityResolver {
        entities: materialized_entities.to_vec(),
    };
    let embedding_provider = MaterializedEntityEmbeddingProvider {
        embeddings_by_entity_id: materialized_entities
            .iter()
            .map(|entity| {
                (
                    entity.entity_id.as_bytes().to_vec(),
                    entity.encoded_embedding.clone(),
                )
            })
            .collect(),
    };
    let items = materialized_entities
        .iter()
        .enumerate()
        .map(|(index, entity)| IndexItem {
            metadata: entity.metadata.clone(),
            content_ref: index,
        })
        .collect::<Vec<_>>();
    let store_root = tempfile::tempdir().map_err(|error| {
        EvaluatorError::Io(format!("failed to create section-7 tempdir: {error}"))
    })?;
    let store = FilesystemBlockStore::new(store_root.path())
        .map_err(|error| EvaluatorError::InvalidConfiguration(error.to_string()))?;
    let root_id = match summary_policy {
        SummaryPolicyKind::ExactCentroid => materialize_with_policy(
            resolver.clone(),
            embedding_provider.clone(),
            ExactCentroidChildSummaryPolicy,
            MaterializationInputs {
                hierarchy,
                embedding_spec: embedding_spec.clone(),
                block_size_target,
                items: &items,
                store: &store,
            },
        )?,
        SummaryPolicyKind::ComposedCentroid => materialize_with_policy(
            resolver,
            embedding_provider,
            lexongraph_streaming_indexer::ArithmeticMeanCanonicalEmbeddingPolicy,
            MaterializationInputs {
                hierarchy,
                embedding_spec,
                block_size_target,
                items: &items,
                store: &store,
            },
        )?,
    };
    Ok(MaterializedDesignTree {
        _store_root: store_root,
        store,
        root_id,
    })
}

fn materialize_with_policy<P>(
    resolver: MaterializedEntityResolver,
    embedding_provider: MaterializedEntityEmbeddingProvider,
    summary_policy: P,
    inputs: MaterializationInputs<'_>,
) -> Result<BlockHash, EvaluatorError>
where
    P: ChildSummaryPolicy,
{
    let mut run = StreamingIndexingRun::new(
        resolver,
        embedding_provider,
        summary_policy,
        FixedHierarchyPlanningPolicy {
            hierarchy: inputs.hierarchy.clone(),
        },
        inputs.embedding_spec,
        inputs.block_size_target,
    );
    pollster::block_on(async {
        run.ingest_batch(inputs.items)
            .await
            .map_err(|error: StreamingIndexerError| {
                EvaluatorError::InvalidConfiguration(error.to_string())
            })?;
        run.finish_pass().map_err(|error: StreamingIndexerError| {
            EvaluatorError::InvalidConfiguration(error.to_string())
        })?;
        run.mark_planning_complete()
            .map_err(|error: StreamingIndexerError| {
                EvaluatorError::InvalidConfiguration(error.to_string())
            })?;
        let result = run
            .finalize(std::iter::once(inputs.items), inputs.store)
            .await
            .map_err(|error: StreamingIndexerError| {
                EvaluatorError::InvalidConfiguration(error.to_string())
            })?;
        Ok(result.root_id)
    })
}

fn resolve_held_out_queries(
    profile: &BenchmarkProfile,
    real_entities: &[EvaluationEntity],
) -> Result<Vec<Section7HeldOutQuery>, EvaluatorError> {
    let real_entity_lookup = real_entities
        .iter()
        .map(|entity| (entity.entity_id.as_str(), entity))
        .collect::<HashMap<_, _>>();
    let mut queries = Vec::new();
    for identity in profile.later_phase_identities.iter().filter(|identity| {
        matches!(
            identity.kind,
            crate::LaterPhaseIdentityKind::HeldOutQuerySet
        )
    }) {
        let asset_path = identity.asset_path.as_ref().ok_or_else(|| {
            EvaluatorError::InvalidConfiguration(format!(
                "held-out query identity {} is missing asset_path",
                identity.identity_id
            ))
        })?;
        let document = std::fs::read_to_string(asset_path).map_err(|error| {
            EvaluatorError::Io(format!(
                "failed to read held-out query set {}: {error}",
                asset_path.display()
            ))
        })?;
        let document: QuerySetDocument = serde_json::from_str(&document)
            .map_err(|error| EvaluatorError::Json(error.to_string()))?;
        if !document.queries.is_empty() {
            queries.extend(document.queries.into_iter().map(|query| {
                Section7HeldOutQuery {
                    query_id: query.entity_id,
                    corpus_id: document
                        .corpus_id
                        .clone()
                        .or_else(|| identity.corpus_id.clone())
                        .unwrap_or_default(),
                    embedding: query.embedding,
                }
            }));
            continue;
        }
        for query_entity_id in document.query_entity_ids {
            let query_entity = real_entity_lookup
                .get(query_entity_id.as_str())
                .ok_or_else(|| {
                    EvaluatorError::InvalidConfiguration(format!(
                        "held-out query entity {} was not present in the real evaluation set",
                        query_entity_id
                    ))
                })?;
            queries.push(Section7HeldOutQuery {
                query_id: query_entity.entity_id.clone(),
                corpus_id: query_entity.corpus_id.clone(),
                embedding: query_entity.embedding.clone(),
            });
        }
    }
    if queries.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-7 execution requires at least one held-out query".into(),
        ));
    }
    Ok(queries)
}

fn held_out_query_set_ids(profile: &BenchmarkProfile) -> Vec<String> {
    profile
        .later_phase_identities
        .iter()
        .filter(|identity| {
            matches!(
                identity.kind,
                crate::LaterPhaseIdentityKind::HeldOutQuerySet
            )
        })
        .map(|identity| identity.identity_id.clone())
        .collect()
}

async fn search_neighbors(
    store: &dyn BlockStore,
    root_id: BlockHash,
    query: &Section7HeldOutQuery,
    neighbor_count: usize,
    indexed_entity_count: usize,
    beam_width: usize,
    metric_semantics_profile: &str,
) -> Result<(Vec<String>, SearchTelemetrySummary), EvaluatorError> {
    let target = EncodedTargetEmbedding::new(
        encode_f32_embedding(&query.embedding),
        EmbeddingSpec {
            dims: u64::try_from(query.embedding.len()).map_err(|_| {
                EvaluatorError::InvalidConfiguration("query dims overflowed u64".into())
            })?,
            encoding: "f32le".into(),
        },
    );
    let requested = if neighbor_count == 0 {
        0
    } else {
        neighbor_count.saturating_add(1).min(indexed_entity_count)
    };
    let (neighbor_ids, telemetry) = match metric_semantics_profile {
        "cosine" => {
            greedy_route_with_telemetry(
                &root_id,
                &target,
                beam_width,
                requested,
                store,
                DefaultEmbeddingCompatibility,
                lexongraph_search::DefaultCandidateScorer,
            )
            .await?
        }
        "euclidean" => {
            greedy_route_with_telemetry(
                &root_id,
                &target,
                beam_width,
                requested,
                store,
                DefaultEmbeddingCompatibility,
                EuclideanCandidateScorer,
            )
            .await?
        }
        other => {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "section-7 metric semantics profile {other} is unsupported; supported profiles: euclidean, cosine"
            )));
        }
    };
    Ok((
        neighbor_ids
            .into_iter()
            .filter(|entity_id| entity_id != &query.query_id)
            .take(neighbor_count)
            .collect(),
        telemetry,
    ))
}

enum GreedyCandidate<Score> {
    Branch {
        child: BlockHash,
        depth: usize,
        score: Score,
    },
    Leaf {
        block_id: BlockHash,
        entry: LeafEntry,
        score: Score,
    },
}

struct GreedyRoutingContext<'a, Target, EC, CS> {
    target: &'a Target,
    store: &'a dyn BlockStore,
    compatibility: &'a EC,
    scorer: &'a CS,
    telemetry: &'a mut SearchTelemetrySummary,
    visited_blocks: &'a mut HashSet<BlockHash>,
}

async fn greedy_route_with_telemetry<Target, EC, CS>(
    root_id: &BlockHash,
    target: &Target,
    beam_width: usize,
    neighbor_count: usize,
    store: &dyn BlockStore,
    compatibility: EC,
    scorer: CS,
) -> Result<(Vec<String>, SearchTelemetrySummary), EvaluatorError>
where
    EC: EmbeddingCompatibility<Target>,
    CS: CandidateScorer<Target>,
{
    if beam_width == 0 {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-7 greedy routing requires beam_width >= 1".into(),
        ));
    }

    let mut telemetry = SearchTelemetrySummary {
        beam_width,
        distinct_blocks_visited: 0,
        max_routing_depth: 0,
        termination: SearchTerminationKind::Success,
    };
    let mut visited_blocks = HashSet::new();
    let mut active_blocks = vec![(*root_id, 0usize)];
    let mut terminal_candidates = Vec::<GreedyCandidate<CS::Score>>::new();

    while !active_blocks.is_empty() {
        let mut branch_candidates = Vec::<GreedyCandidate<CS::Score>>::new();
        for (block_id, depth) in active_blocks.drain(..) {
            let scored = load_greedy_candidates(
                block_id,
                depth,
                &mut GreedyRoutingContext {
                    target,
                    store,
                    compatibility: &compatibility,
                    scorer: &scorer,
                    telemetry: &mut telemetry,
                    visited_blocks: &mut visited_blocks,
                },
            )
            .await?;
            for candidate in scored {
                match candidate {
                    GreedyCandidate::Branch { .. } => branch_candidates.push(candidate),
                    GreedyCandidate::Leaf { .. } => terminal_candidates.push(candidate),
                }
            }
        }

        if branch_candidates.is_empty() {
            terminal_candidates.sort_by(compare_greedy_leaf_candidates::<CS::Score>);
            let mut neighbor_ids = Vec::new();
            for candidate in terminal_candidates {
                let GreedyCandidate::Leaf { entry, .. } = candidate else {
                    continue;
                };
                let Some(CborValue::Text(entity_id)) =
                    metadata_value(&entry.metadata, SECTION7_ENTITY_ID_METADATA_KEY)
                else {
                    return Err(EvaluatorError::InvalidConfiguration(
                        "section-7 greedy routing leaf entry was missing entity_id metadata".into(),
                    ));
                };
                neighbor_ids.push(entity_id.clone());
                if neighbor_ids.len() == neighbor_count {
                    break;
                }
            }
            telemetry.termination = if neighbor_ids.len() == neighbor_count {
                SearchTerminationKind::Success
            } else {
                SearchTerminationKind::Exhausted
            };
            return Ok((neighbor_ids, telemetry));
        }

        branch_candidates.sort_by(compare_greedy_branch_candidates::<CS::Score>);
        let mut seen_children = HashSet::new();
        active_blocks = branch_candidates
            .into_iter()
            .filter_map(|candidate| match candidate {
                GreedyCandidate::Branch { child, depth, .. } if seen_children.insert(child) => {
                    Some((child, depth))
                }
                _ => None,
            })
            .take(beam_width)
            .collect();
    }

    telemetry.termination = SearchTerminationKind::Exhausted;
    Ok((Vec::new(), telemetry))
}

async fn load_greedy_candidates<Target, EC, CS>(
    block_id: BlockHash,
    depth: usize,
    context: &mut GreedyRoutingContext<'_, Target, EC, CS>,
) -> Result<Vec<GreedyCandidate<CS::Score>>, EvaluatorError>
where
    EC: EmbeddingCompatibility<Target>,
    CS: CandidateScorer<Target>,
{
    let Some(validated) = context.store.get(&block_id).await.map_err(|error| {
        EvaluatorError::InvalidConfiguration(format!(
            "section-7 greedy routing failed to load block {block_id}: {error}"
        ))
    })?
    else {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "section-7 greedy routing missing block {block_id}"
        )));
    };

    context.visited_blocks.insert(block_id);
    context.telemetry.distinct_blocks_visited = context.visited_blocks.len();
    context.telemetry.max_routing_depth = context.telemetry.max_routing_depth.max(depth);

    match into_entries(validated) {
        TypedEntries::Branch(metadata, entries) => {
            context
                .compatibility
                .ensure_compatible(context.target, &metadata.embedding_spec)
                .map_err(|error| {
                    EvaluatorError::InvalidConfiguration(format!(
                        "section-7 greedy routing incompatible embedding in block {block_id}: {error}"
                    ))
                })?;
            entries
                .into_iter()
                .map(|entry| {
                    context
                        .scorer
                        .score(context.target, &entry.embedding, &metadata.embedding_spec)
                        .map(|score| GreedyCandidate::Branch {
                            child: entry.child,
                            depth: depth + 1,
                            score,
                        })
                        .map_err(|error| {
                            EvaluatorError::InvalidConfiguration(format!(
                                "section-7 greedy routing failed to score branch block {block_id}: {error}"
                            ))
                        })
                })
                .collect()
        }
        TypedEntries::Leaf(metadata, entries) => {
            context
                .compatibility
                .ensure_compatible(context.target, &metadata.embedding_spec)
                .map_err(|error| {
                    EvaluatorError::InvalidConfiguration(format!(
                        "section-7 greedy routing incompatible embedding in block {block_id}: {error}"
                    ))
                })?;
            entries
                .into_iter()
                .map(|entry| {
                    context
                        .scorer
                        .score(context.target, &entry.embedding, &metadata.embedding_spec)
                        .map(|score| GreedyCandidate::Leaf {
                            block_id,
                            entry,
                            score,
                        })
                        .map_err(|error| {
                            EvaluatorError::InvalidConfiguration(format!(
                                "section-7 greedy routing failed to score leaf block {block_id}: {error}"
                            ))
                        })
                })
                .collect()
        }
    }
}

fn compare_greedy_branch_candidates<Score: Ord>(
    left: &GreedyCandidate<Score>,
    right: &GreedyCandidate<Score>,
) -> Ordering {
    match (left, right) {
        (
            GreedyCandidate::Branch {
                child: left_child,
                score: left_score,
                ..
            },
            GreedyCandidate::Branch {
                child: right_child,
                score: right_score,
                ..
            },
        ) => right_score
            .cmp(left_score)
            .then_with(|| left_child.as_bytes().cmp(right_child.as_bytes())),
        _ => Ordering::Equal,
    }
}

fn compare_greedy_leaf_candidates<Score: Ord>(
    left: &GreedyCandidate<Score>,
    right: &GreedyCandidate<Score>,
) -> Ordering {
    match (left, right) {
        (
            GreedyCandidate::Leaf {
                block_id: left_block,
                score: left_score,
                ..
            },
            GreedyCandidate::Leaf {
                block_id: right_block,
                score: right_score,
                ..
            },
        ) => right_score
            .cmp(left_score)
            .then_with(|| left_block.as_bytes().cmp(right_block.as_bytes())),
        _ => Ordering::Equal,
    }
}

fn exact_top_neighbors(
    query: &Section7HeldOutQuery,
    real_entities: &[EvaluationEntity],
    metric_semantics_profile: &str,
) -> Result<Vec<String>, EvaluatorError> {
    let mut distances = real_entities
        .iter()
        .filter(|entity| entity.entity_id != query.query_id)
        .map(|entity| {
            let distance = match metric_semantics_profile {
                "cosine" => cosine_distance(&query.embedding, &entity.embedding),
                "euclidean" => squared_euclidean_distance(&query.embedding, &entity.embedding),
                other => {
                    return Err(EvaluatorError::InvalidConfiguration(format!(
                        "section-7 metric semantics profile {other} is unsupported; supported profiles: euclidean, cosine"
                    )))
                }
            }?;
            Ok((distance, entity.entity_id.clone()))
        })
        .collect::<Result<Vec<_>, EvaluatorError>>()?;
    distances.sort_by(|left, right| {
        left.0
            .partial_cmp(&right.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.1.cmp(&right.1))
    });
    Ok(distances
        .into_iter()
        .take(10)
        .map(|(_, entity_id)| entity_id)
        .collect())
}

fn build_query_report(
    query: &Section7HeldOutQuery,
    beam_width: usize,
    exact_neighbor_ids: Vec<String>,
    returned_neighbor_ids: Vec<String>,
    telemetry: &SearchTelemetrySummary,
) -> Section7QueryReport {
    Section7QueryReport {
        query_id: query.query_id.clone(),
        beam_width,
        tnn_at_1: tnn_at_k(&returned_neighbor_ids, &exact_neighbor_ids, 1),
        tnn_at_5: tnn_at_k(&returned_neighbor_ids, &exact_neighbor_ids, 5),
        tnn_at_10: tnn_at_k(&returned_neighbor_ids, &exact_neighbor_ids, 10),
        returned_neighbor_ids,
        exact_neighbor_ids,
        nodes_visited: telemetry.distinct_blocks_visited,
        routing_depth: telemetry.max_routing_depth,
        termination: termination_label(telemetry.termination.clone()),
    }
}

fn build_beam_reports(query_reports: &[Section7QueryReport]) -> Vec<Section7BeamReport> {
    let mut reports = Vec::new();
    for &beam_width in &SECTION7_BEAM_WIDTHS {
        let matching = query_reports
            .iter()
            .filter(|query| query.beam_width == beam_width)
            .collect::<Vec<_>>();
        if matching.is_empty() {
            continue;
        }
        let query_count = matching.len();
        let mut termination_counts = BTreeMap::<String, usize>::new();
        for report in &matching {
            *termination_counts
                .entry(report.termination.clone())
                .or_default() += 1;
        }
        reports.push(Section7BeamReport {
            beam_width,
            query_count,
            mean_tnn_at_1: matching.iter().map(|report| report.tnn_at_1).sum::<f64>()
                / query_count as f64,
            mean_tnn_at_5: matching.iter().map(|report| report.tnn_at_5).sum::<f64>()
                / query_count as f64,
            mean_tnn_at_10: matching.iter().map(|report| report.tnn_at_10).sum::<f64>()
                / query_count as f64,
            mean_nodes_visited: matching
                .iter()
                .map(|report| report.nodes_visited as f64)
                .sum::<f64>()
                / query_count as f64,
            mean_routing_depth: matching
                .iter()
                .map(|report| report.routing_depth as f64)
                .sum::<f64>()
                / query_count as f64,
            termination_counts,
        });
    }
    reports
}

fn rank_designs(design_reports: &mut [Section7DesignReport]) -> Vec<Section7RankedDesign> {
    let mut ranked = design_reports
        .iter_mut()
        .filter_map(|design| {
            let best_beam = design
                .beam_reports
                .iter()
                .max_by(|left, right| compare_beam_reports(left, right))?;
            let ranking_score = best_beam.mean_tnn_at_10
                + 0.01 * best_beam.mean_tnn_at_5
                + 0.0001 * best_beam.mean_tnn_at_1
                - 0.000001 * best_beam.mean_nodes_visited
                - 0.000000001 * best_beam.mean_routing_depth
                - best_beam.beam_width as f64 * 1.0e-12;
            design.ranking_score = Some(ranking_score);
            Some(Section7RankedDesign {
                leaf_candidate_id: design.leaf_candidate_identity.candidate_id.clone(),
                hierarchy_strategy_id: design.hierarchy_strategy_identity.strategy_id.clone(),
                summary_candidate_id: design
                    .summary_candidate_identity
                    .summary_candidate_id
                    .clone(),
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
            .then_with(|| left.leaf_candidate_id.cmp(&right.leaf_candidate_id))
            .then_with(|| left.hierarchy_strategy_id.cmp(&right.hierarchy_strategy_id))
            .then_with(|| left.summary_candidate_id.cmp(&right.summary_candidate_id))
    });
    for (rank, design) in ranked.iter_mut().enumerate() {
        design.rank = rank + 1;
    }
    ranked
}

fn compare_beam_reports(left: &Section7BeamReport, right: &Section7BeamReport) -> Ordering {
    left.mean_tnn_at_10
        .partial_cmp(&right.mean_tnn_at_10)
        .unwrap_or(Ordering::Equal)
        .then_with(|| {
            left.mean_tnn_at_5
                .partial_cmp(&right.mean_tnn_at_5)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| {
            left.mean_tnn_at_1
                .partial_cmp(&right.mean_tnn_at_1)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| {
            right
                .mean_nodes_visited
                .partial_cmp(&left.mean_nodes_visited)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| {
            right
                .mean_routing_depth
                .partial_cmp(&left.mean_routing_depth)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| right.beam_width.cmp(&left.beam_width))
}

fn remaining_deferred_goals(
    previous: &[Section6DeferredGoalRecord],
) -> Vec<Section7DeferredGoalRecord> {
    let mut deferred = BTreeMap::<String, Section7DeferredGoalRecord>::new();
    for goal in previous {
        if goal.deferred_id == "section6-deferred-routing" {
            continue;
        }
        deferred.insert(
            goal.deferred_id.clone(),
            Section7DeferredGoalRecord {
                deferred_id: goal.deferred_id.clone(),
                label: goal.label.clone(),
                reason: goal.reason.clone(),
                later_evaluation_line: goal.later_evaluation_line.clone(),
            },
        );
    }
    deferred.insert(
        "section7-deferred-latency-qps".into(),
        Section7DeferredGoalRecord {
            deferred_id: "section7-deferred-latency-qps".into(),
            label: "Latency and QPS service-level evaluation".into(),
            reason: SECTION7_LATENCY_REASON.into(),
            later_evaluation_line: "later service-level evaluation line".into(),
        },
    );
    deferred
        .entry("section7-deferred-persistence".into())
        .or_insert_with(|| Section7DeferredGoalRecord {
            deferred_id: "section7-deferred-persistence".into(),
            label: "Serialization, persistence, and robustness evaluation".into(),
            reason: SECTION7_PERSISTENCE_REASON.into(),
            later_evaluation_line: "later persistence and robustness evaluation line".into(),
        });
    deferred.into_values().collect()
}

fn design_status_label(report: &Section7DesignReport) -> &'static str {
    match report.run_status {
        Section7RunStatus::Succeeded => "succeeded",
        Section7RunStatus::DeferredUnsupportedSummary => "deferred-unsupported-summary",
        Section7RunStatus::Failed => "failed",
    }
}

fn termination_label(kind: SearchTerminationKind) -> String {
    match kind {
        SearchTerminationKind::Success => "success",
        SearchTerminationKind::Exhausted => "exhausted",
        SearchTerminationKind::InvalidTraversalWidth => "invalid-traversal-width",
        SearchTerminationKind::MissingRootBlock => "missing-root-block",
        SearchTerminationKind::RootLoadFailure => "root-load-failure",
        SearchTerminationKind::MissingChildBlock => "missing-child-block",
        SearchTerminationKind::ChildLoadFailure => "child-load-failure",
        SearchTerminationKind::MalformedBlock => "malformed-block",
        SearchTerminationKind::IncompatibleEmbedding => "incompatible-embedding",
        SearchTerminationKind::ScoringFailure => "scoring-failure",
        SearchTerminationKind::FrontierSelectionFailure => "frontier-selection-failure",
    }
    .into()
}

fn tnn_at_k(returned_neighbor_ids: &[String], exact_neighbor_ids: &[String], k: usize) -> f64 {
    let actual_k = k.min(exact_neighbor_ids.len());
    if actual_k == 0 {
        return 1.0;
    }
    let returned = returned_neighbor_ids
        .iter()
        .take(actual_k)
        .cloned()
        .collect::<HashSet<_>>();
    let hits = exact_neighbor_ids
        .iter()
        .take(actual_k)
        .filter(|neighbor_id| returned.contains(*neighbor_id))
        .count();
    hits as f64 / actual_k as f64
}

fn recommended_block_size_target(entity_count: usize, encoded_embedding_len: usize) -> usize {
    let max_children = entity_count.clamp(2, 128);
    4096.max(max_children * (encoded_embedding_len + 64))
}

fn encode_f32_embedding(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(values.len() * 4);
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn squared_euclidean_distance(left: &[f32], right: &[f32]) -> Result<f64, EvaluatorError> {
    if left.len() != right.len() {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "mismatched embedding dimensions: {} vs {}",
            left.len(),
            right.len()
        )));
    }
    let mut sum = 0.0f64;
    for (index, (&left, &right)) in left.iter().zip(right.iter()).enumerate() {
        if !left.is_finite() || !right.is_finite() {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "non-finite euclidean distance input at dimension {index}"
            )));
        }
        let delta = f64::from(left) - f64::from(right);
        sum += delta * delta;
    }
    Ok(sum)
}

fn cosine_distance(left: &[f32], right: &[f32]) -> Result<f64, EvaluatorError> {
    if left.len() != right.len() {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "mismatched embedding dimensions: {} vs {}",
            left.len(),
            right.len()
        )));
    }
    let mut dot = 0.0f64;
    let mut left_norm = 0.0f64;
    let mut right_norm = 0.0f64;
    for (index, (&left, &right)) in left.iter().zip(right.iter()).enumerate() {
        if !left.is_finite() || !right.is_finite() {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "non-finite cosine distance input at dimension {index}"
            )));
        }
        dot += f64::from(left) * f64::from(right);
        left_norm += f64::from(left) * f64::from(left);
        right_norm += f64::from(right) * f64::from(right);
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return Err(EvaluatorError::InvalidConfiguration(
            "cosine distance requires non-zero embeddings".into(),
        ));
    }
    let similarity = dot / (left_norm.sqrt() * right_norm.sqrt());
    Ok(1.0 - similarity)
}

fn total_order_key_f64(value: f64) -> u64 {
    let bits = value.to_bits();
    if (bits >> 63) == 0 {
        bits | (1 << 63)
    } else {
        !bits
    }
}

fn hash_entity_id(bytes: &[u8]) -> BlockHash {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(bytes);
    let mut out = [0u8; BlockHash::LEN];
    out.copy_from_slice(&digest);
    BlockHash::from_bytes(out)
}
