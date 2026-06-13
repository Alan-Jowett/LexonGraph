// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::section4::measure_peak_build_memory;
use crate::{
    BenchmarkProfile, CampaignReport, CandidateIdentity, CandidateRunReport, CandidateRunStatus,
    EvaluationEntity, EvaluatorError, ProvenanceManifest, RegisteredCandidate, ResearchCoverage,
    resolved_profile_evaluation_entities, run_evaluation_campaign,
};

const SECTION5_PARENT_SUMMARY_REASON: &str = "parent-summary accuracy and stability remain deferred beyond section-5 hierarchy construction and must be discharged by the later summary-comparison evaluation line";
const SECTION5_ROUTING_REASON: &str = "routing targets, recall, latency, and beam-width behavior remain deferred beyond section-5 hierarchy construction and must be discharged by the later routing evaluation line";
const SECTION5_PERSISTENCE_REASON: &str = "serialization identity, persisted-artifact durability, and broader robustness checks remain deferred beyond section-5 hierarchy construction and must be discharged by the later persistence and robustness evaluation line";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section5HierarchyStrategyKind {
    BottomUpAgglomeration,
    RecursiveTopDownPartitioning,
    GreedyPackByCentroidNearestGrouping,
    HybridTopDownBottomUp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Section5HierarchyStrategyIdentity {
    pub strategy_id: String,
    pub label: String,
    pub kind: Section5HierarchyStrategyKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredHierarchyStrategy {
    pub identity: Section5HierarchyStrategyIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section5DepthBoundPolicy {
    CeilLogByMinFanout,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section5EpsilonPolicy {
    pub parent_to_root_dispersion_ratio_max: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section5HierarchyContract {
    pub contract_id: String,
    pub fanout_min: usize,
    pub fanout_max: usize,
    pub depth_bound_policy: Section5DepthBoundPolicy,
    pub dispersion_functional: String,
    pub beta_threshold: f64,
    pub epsilon_policy: Section5EpsilonPolicy,
    pub section4_source_label: String,
    pub later_evaluation_line: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section5HierarchyNodeKind {
    LeafCluster,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section5HierarchyNodeReport {
    pub node_id: String,
    pub kind: Section5HierarchyNodeKind,
    pub depth_from_root: usize,
    pub fanout: usize,
    pub leaf_descendant_count: usize,
    pub member_count: usize,
    pub centroid: Vec<f32>,
    pub dispersion: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section5HierarchyEdgeReport {
    pub parent_node_id: String,
    pub child_node_id: String,
    pub beta: f64,
    pub child_is_leaf: bool,
    pub epsilon_exception_applied: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section5GateKind {
    FanoutBounds,
    NoSingleChildInternalNodes,
    DepthBound,
    RefinementBetaThreshold,
    EpsilonExceptionScope,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section5GateStatus {
    Passed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section5GateResult {
    pub gate_id: String,
    pub label: String,
    pub kind: Section5GateKind,
    pub coverage: ResearchCoverage,
    pub research_goal_ids: Vec<String>,
    pub status: Section5GateStatus,
    pub observed_value: Option<f64>,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section5PairRunStatus {
    Succeeded,
    GateFailed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section5PairReport {
    pub leaf_candidate_identity: CandidateIdentity,
    pub hierarchy_strategy_identity: Section5HierarchyStrategyIdentity,
    pub originating_section4_profile_id: String,
    pub originating_section4_source_label: String,
    pub originating_section4_ranking_score: Option<f64>,
    pub originating_section4_provenance: ProvenanceManifest,
    pub leaf_cluster_count: usize,
    pub internal_node_count: usize,
    pub max_depth: usize,
    pub theoretical_depth_bound: usize,
    pub minimum_observed_fanout: usize,
    pub maximum_observed_fanout: usize,
    pub refinement_edge_count: usize,
    pub maximum_observed_beta: f64,
    pub epsilon_exception_use_count: usize,
    pub build_elapsed_nanos: u128,
    pub build_throughput_leaf_nodes_per_second: f64,
    pub peak_build_memory_bytes: u64,
    pub gate_results: Vec<Section5GateResult>,
    pub hierarchy_nodes: Vec<Section5HierarchyNodeReport>,
    pub hierarchy_edges: Vec<Section5HierarchyEdgeReport>,
    pub run_status: Section5PairRunStatus,
    pub survived_required_gates: bool,
    pub ranking_score: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section5RankedPair {
    pub leaf_candidate_id: String,
    pub hierarchy_strategy_id: String,
    pub ranking_score: f64,
    pub rank: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section5DeferredGoalRecord {
    pub deferred_id: String,
    pub label: String,
    pub reason: String,
    pub later_evaluation_line: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section5CampaignReport {
    pub section4_profile_id: String,
    pub hierarchy_contract: Section5HierarchyContract,
    pub section4_campaign: CampaignReport,
    pub survivor_candidate_ids: Vec<String>,
    pub remaining_deferred_goals: Vec<Section5DeferredGoalRecord>,
    pub pair_reports: Vec<Section5PairReport>,
    pub ranking: Vec<Section5RankedPair>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Section5CampaignArtifacts {
    pub per_pair_reports: Vec<crate::EmittedArtifact>,
    pub campaign_report: crate::EmittedArtifact,
    pub scorecard: crate::EmittedArtifact,
    pub carry_forward_summary: crate::EmittedArtifact,
}

#[derive(Clone)]
struct LeafClusterSummary {
    cluster_id: u32,
    member_count: usize,
    leaf_descendant_count: usize,
    sum: Vec<f64>,
    sum_sq_norm: f64,
    centroid: Vec<f32>,
    dispersion: f64,
}

#[derive(Clone)]
struct BuiltNode {
    node_id: String,
    kind: Section5HierarchyNodeKind,
    member_count: usize,
    leaf_descendant_count: usize,
    sum: Vec<f64>,
    sum_sq_norm: f64,
    centroid: Vec<f32>,
    dispersion: f64,
    child_ids: Vec<String>,
}

#[derive(Clone)]
struct HierarchyBuild {
    root_id: String,
    nodes: Vec<BuiltNode>,
}

struct PairReportContext<'a> {
    section4_profile_id: &'a str,
    section4_source_label: &'a str,
    survivor: &'a CandidateRunReport,
    strategy: &'a RegisteredHierarchyStrategy,
    contract: &'a Section5HierarchyContract,
    elapsed: u128,
    peak_build_memory_bytes: u64,
}

pub fn registered_hierarchy_strategy_names() -> Vec<&'static str> {
    vec![
        "bottom-up-agglomeration",
        "recursive-top-down",
        "greedy-pack",
        "hybrid-top-down-bottom-up",
    ]
}

pub fn resolve_registered_hierarchy_strategies(
    strategy_names: &[String],
) -> Result<Vec<RegisteredHierarchyStrategy>, EvaluatorError> {
    if strategy_names.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "at least one hierarchy strategy must be registered".into(),
        ));
    }

    let mut strategies = Vec::with_capacity(strategy_names.len());
    let mut seen = HashSet::new();
    for strategy_name in strategy_names {
        let Some(strategy) = registered_hierarchy_strategy(strategy_name) else {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "unknown registered hierarchy strategy {strategy_name}; available hierarchy strategies: {}",
                registered_hierarchy_strategy_names().join(", ")
            )));
        };
        if !seen.insert(strategy.identity.strategy_id.clone()) {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "duplicate hierarchy strategy id {}",
                strategy.identity.strategy_id
            )));
        }
        strategies.push(strategy);
    }
    Ok(strategies)
}

pub fn run_section5_campaign(
    profile: &BenchmarkProfile,
    candidates: &[RegisteredCandidate],
    contract: &Section5HierarchyContract,
    strategies: &[RegisteredHierarchyStrategy],
) -> Result<Section5CampaignReport, EvaluatorError> {
    validate_section5_contract(contract)?;
    validate_section5_strategies(strategies)?;

    let section4_campaign = run_evaluation_campaign(profile, candidates)?;
    let survivor_reports = section4_campaign
        .run_reports
        .iter()
        .filter(|run_report| {
            run_report.survived_required_gates
                && matches!(run_report.run_status, CandidateRunStatus::Succeeded)
        })
        .cloned()
        .collect::<Vec<_>>();
    let survivor_candidate_ids = survivor_reports
        .iter()
        .map(|run_report| run_report.candidate_identity.candidate_id.clone())
        .collect::<Vec<_>>();
    let remaining_deferred_goals = remaining_deferred_goals(
        &section4_campaign.run_reports,
        &contract.later_evaluation_line,
    );

    let pair_reports = if survivor_reports.is_empty() {
        Vec::new()
    } else {
        let evaluation_entities = resolved_profile_evaluation_entities(profile)?;
        build_pair_reports(
            &section4_campaign,
            &survivor_reports,
            &evaluation_entities,
            contract,
            strategies,
        )?
    };
    let ranking = rank_pair_reports(&pair_reports);

    Ok(Section5CampaignReport {
        section4_profile_id: profile.profile_id.clone(),
        hierarchy_contract: contract.clone(),
        section4_campaign,
        survivor_candidate_ids,
        remaining_deferred_goals,
        pair_reports,
        ranking,
    })
}

pub fn emit_section5_campaign_artifacts(
    report: &Section5CampaignReport,
) -> Result<Section5CampaignArtifacts, EvaluatorError> {
    let mut per_pair_reports = Vec::with_capacity(report.pair_reports.len());
    let mut used_file_names = HashSet::new();
    for pair_report in &report.pair_reports {
        let stem = format!(
            "{}-{}",
            sanitize_artifact_stem(&pair_report.leaf_candidate_identity.candidate_id),
            sanitize_artifact_stem(&pair_report.hierarchy_strategy_identity.strategy_id)
        );
        let file_name = unique_artifact_file_name(&mut used_file_names, &stem, "-pair-report.json");
        per_pair_reports.push(crate::EmittedArtifact {
            file_name,
            contents: serde_json::to_string_pretty(pair_report)
                .map_err(|error| EvaluatorError::Json(error.to_string()))?,
        });
    }

    Ok(Section5CampaignArtifacts {
        per_pair_reports,
        campaign_report: crate::EmittedArtifact {
            file_name: "section5-campaign-report.json".into(),
            contents: serde_json::to_string_pretty(report)
                .map_err(|error| EvaluatorError::Json(error.to_string()))?,
        },
        scorecard: crate::EmittedArtifact {
            file_name: "section5-scorecard.txt".into(),
            contents: render_section5_scorecard(report),
        },
        carry_forward_summary: crate::EmittedArtifact {
            file_name: "section5-carry-forward-summary.txt".into(),
            contents: render_section5_carry_forward_summary(report),
        },
    })
}

pub fn write_section5_campaign_artifacts(
    output_dir: &Path,
    artifacts: &Section5CampaignArtifacts,
) -> Result<Vec<PathBuf>, EvaluatorError> {
    std::fs::create_dir_all(output_dir).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to create section-5 artifact directory {}: {error}",
            output_dir.display()
        ))
    })?;

    let mut written = Vec::with_capacity(artifacts.per_pair_reports.len() + 3);
    for artifact in artifacts.per_pair_reports.iter().chain([
        &artifacts.campaign_report,
        &artifacts.scorecard,
        &artifacts.carry_forward_summary,
    ]) {
        let path = output_dir.join(&artifact.file_name);
        std::fs::write(&path, &artifact.contents).map_err(|error| {
            EvaluatorError::Io(format!(
                "failed to write section-5 artifact {}: {error}",
                path.display()
            ))
        })?;
        written.push(path);
    }

    Ok(written)
}

pub fn render_section5_scorecard(report: &Section5CampaignReport) -> String {
    let mut lines = vec![format!(
        "Section-5 scorecard for {} [{}]",
        report.section4_profile_id, report.hierarchy_contract.contract_id
    )];
    lines.push(format!(
        "Section-4 survivors: {}",
        if report.survivor_candidate_ids.is_empty() {
            "none".into()
        } else {
            report.survivor_candidate_ids.join(", ")
        }
    ));
    for pair_report in &report.pair_reports {
        lines.push(format!(
            "- {} x {}: {:?}, depth={}/{}, max_beta={:.6}, epsilon_uses={}, throughput={:.3}, peak_build_memory_bytes={}",
            pair_report.leaf_candidate_identity.candidate_id,
            pair_report.hierarchy_strategy_identity.strategy_id,
            pair_report.run_status,
            pair_report.max_depth,
            pair_report.theoretical_depth_bound,
            pair_report.maximum_observed_beta,
            pair_report.epsilon_exception_use_count,
            pair_report.build_throughput_leaf_nodes_per_second,
            pair_report.peak_build_memory_bytes
        ));
    }
    if !report.remaining_deferred_goals.is_empty() {
        lines.push("Remaining deferred obligations:".into());
        for deferred in &report.remaining_deferred_goals {
            lines.push(format!(
                "  - {} [{}]: {}",
                deferred.deferred_id, deferred.later_evaluation_line, deferred.reason
            ));
        }
    }
    lines.join("\n")
}

pub fn render_section5_carry_forward_summary(report: &Section5CampaignReport) -> String {
    let carried_forward = report
        .ranking
        .iter()
        .map(|pair| {
            format!(
                "{} x {}",
                pair.leaf_candidate_id, pair.hierarchy_strategy_id
            )
        })
        .collect::<Vec<_>>();
    let mut lines = vec![format!(
        "Section-5 carry-forward summary for {} [{}]",
        report.section4_profile_id, report.hierarchy_contract.contract_id
    )];
    lines.push(format!(
        "Originating section-4 source: {}",
        report.hierarchy_contract.section4_source_label
    ));
    lines.push(format!(
        "Carried forward pairs: {}",
        if carried_forward.is_empty() {
            "none".into()
        } else {
            carried_forward.join(", ")
        }
    ));
    for pair in &report.ranking {
        lines.push(format!(
            "- rank {}: {} x {} (ranking_score={:.6})",
            pair.rank, pair.leaf_candidate_id, pair.hierarchy_strategy_id, pair.ranking_score
        ));
    }
    let mut rejected = report
        .pair_reports
        .iter()
        .filter(|pair_report| !pair_report.survived_required_gates)
        .collect::<Vec<_>>();
    rejected.sort_by(|left, right| {
        left.leaf_candidate_identity
            .candidate_id
            .cmp(&right.leaf_candidate_identity.candidate_id)
            .then_with(|| {
                left.hierarchy_strategy_identity
                    .strategy_id
                    .cmp(&right.hierarchy_strategy_identity.strategy_id)
            })
    });
    if !rejected.is_empty() {
        lines.push("Rejected pairs:".into());
        for pair_report in rejected {
            lines.push(format!(
                "- {} x {}",
                pair_report.leaf_candidate_identity.candidate_id,
                pair_report.hierarchy_strategy_identity.strategy_id
            ));
        }
    }
    lines.join("\n")
}

fn registered_hierarchy_strategy(name: &str) -> Option<RegisteredHierarchyStrategy> {
    let identity = match name {
        "bottom-up-agglomeration" => Section5HierarchyStrategyIdentity {
            strategy_id: "bottom-up-agglomeration".into(),
            label: "Bottom-up agglomeration with bounded fanout".into(),
            kind: Section5HierarchyStrategyKind::BottomUpAgglomeration,
        },
        "recursive-top-down" => Section5HierarchyStrategyIdentity {
            strategy_id: "recursive-top-down".into(),
            label: "Recursive top-down partitioning over leaf summaries".into(),
            kind: Section5HierarchyStrategyKind::RecursiveTopDownPartitioning,
        },
        "greedy-pack" => Section5HierarchyStrategyIdentity {
            strategy_id: "greedy-pack".into(),
            label: "Greedy pack-by-centroid nearest grouping".into(),
            kind: Section5HierarchyStrategyKind::GreedyPackByCentroidNearestGrouping,
        },
        "hybrid-top-down-bottom-up" => Section5HierarchyStrategyIdentity {
            strategy_id: "hybrid-top-down-bottom-up".into(),
            label: "Hybrid top-down coarse partitioning with lower-level bottom-up grouping".into(),
            kind: Section5HierarchyStrategyKind::HybridTopDownBottomUp,
        },
        _ => return None,
    };
    Some(RegisteredHierarchyStrategy { identity })
}

fn validate_section5_contract(contract: &Section5HierarchyContract) -> Result<(), EvaluatorError> {
    if contract.contract_id.trim().is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-5 hierarchy contract must declare a non-empty contract_id".into(),
        ));
    }
    if contract.fanout_min < 2 {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-5 hierarchy contract fanout_min must be at least 2".into(),
        ));
    }
    if contract.fanout_max < contract.fanout_min {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-5 hierarchy contract fanout_max must be greater than or equal to fanout_min"
                .into(),
        ));
    }
    if contract.dispersion_functional.trim().is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-5 hierarchy contract must declare a non-empty dispersion_functional".into(),
        ));
    }
    if !contract.beta_threshold.is_finite() || contract.beta_threshold <= 0.0 {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-5 hierarchy contract beta_threshold must be finite and positive".into(),
        ));
    }
    if !contract
        .epsilon_policy
        .parent_to_root_dispersion_ratio_max
        .is_finite()
        || contract.epsilon_policy.parent_to_root_dispersion_ratio_max < 0.0
    {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-5 epsilon parent_to_root_dispersion_ratio_max must be finite and non-negative"
                .into(),
        ));
    }
    if contract.section4_source_label.trim().is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-5 hierarchy contract must declare a non-empty section4_source_label".into(),
        ));
    }
    if contract.later_evaluation_line.trim().is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-5 hierarchy contract must declare a non-empty later_evaluation_line".into(),
        ));
    }
    Ok(())
}

fn validate_section5_strategies(
    strategies: &[RegisteredHierarchyStrategy],
) -> Result<(), EvaluatorError> {
    if strategies.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "at least one hierarchy strategy must be registered".into(),
        ));
    }
    let mut seen = HashSet::new();
    for strategy in strategies {
        if strategy.identity.strategy_id.trim().is_empty() {
            return Err(EvaluatorError::InvalidConfiguration(
                "registered hierarchy strategy id must not be empty".into(),
            ));
        }
        if !seen.insert(strategy.identity.strategy_id.as_str()) {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "duplicate hierarchy strategy id {}",
                strategy.identity.strategy_id
            )));
        }
    }
    Ok(())
}

fn remaining_deferred_goals(
    section4_run_reports: &[CandidateRunReport],
    later_evaluation_line: &str,
) -> Vec<Section5DeferredGoalRecord> {
    let mut deferred = BTreeMap::<String, Section5DeferredGoalRecord>::new();
    for run_report in section4_run_reports {
        for goal in &run_report.deferred_research_goals {
            deferred.entry(goal.deferred_id.clone()).or_insert_with(|| {
                Section5DeferredGoalRecord {
                    deferred_id: goal.deferred_id.clone(),
                    label: goal.label.clone(),
                    reason: goal.reason.clone(),
                    later_evaluation_line: goal.later_evaluation_line.clone(),
                }
            });
        }
    }
    for (deferred_id, label, reason) in [
        (
            "section5-deferred-parent-summary",
            "Parent-summary comparison",
            SECTION5_PARENT_SUMMARY_REASON,
        ),
        (
            "section5-deferred-routing",
            "Routing and beam-width evaluation",
            SECTION5_ROUTING_REASON,
        ),
        (
            "section5-deferred-persistence",
            "Serialization, persistence, and robustness evaluation",
            SECTION5_PERSISTENCE_REASON,
        ),
    ] {
        deferred
            .entry(deferred_id.into())
            .or_insert_with(|| Section5DeferredGoalRecord {
                deferred_id: deferred_id.into(),
                label: label.into(),
                reason: reason.into(),
                later_evaluation_line: later_evaluation_line.into(),
            });
    }
    deferred.into_values().collect()
}

fn build_pair_reports(
    section4_campaign: &CampaignReport,
    survivor_reports: &[CandidateRunReport],
    evaluation_entities: &[EvaluationEntity],
    contract: &Section5HierarchyContract,
    strategies: &[RegisteredHierarchyStrategy],
) -> Result<Vec<Section5PairReport>, EvaluatorError> {
    let mut pair_reports = Vec::with_capacity(survivor_reports.len() * strategies.len());
    for survivor in survivor_reports {
        let leaf_summaries = build_leaf_cluster_summaries(survivor, evaluation_entities)?;
        for strategy in strategies {
            let ((build, elapsed), peak_build_memory_bytes) = measure_peak_build_memory(|| {
                let started = Instant::now();
                let build = build_hierarchy(&leaf_summaries, strategy, contract);
                (build, started.elapsed().as_nanos())
            });
            pair_reports.push(build_pair_report(
                PairReportContext {
                    section4_profile_id: &section4_campaign.profile_id,
                    section4_source_label: &contract.section4_source_label,
                    survivor,
                    strategy,
                    contract,
                    elapsed,
                    peak_build_memory_bytes,
                },
                build,
            ));
        }
    }
    Ok(pair_reports)
}

fn build_leaf_cluster_summaries(
    survivor: &CandidateRunReport,
    evaluation_entities: &[EvaluationEntity],
) -> Result<Vec<LeafClusterSummary>, EvaluatorError> {
    let embedding_lookup = evaluation_entities
        .iter()
        .map(|entity| (entity.entity_id.as_str(), entity.embedding.as_slice()))
        .collect::<HashMap<_, _>>();
    let mut grouped = BTreeMap::<u32, Vec<&str>>::new();
    for membership in &survivor.leaf_membership {
        grouped
            .entry(membership.cluster_id)
            .or_default()
            .push(membership.entity_id.as_str());
    }
    if grouped.len() < 2 {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "section-5 hierarchy construction requires at least two surviving leaf clusters, observed {} for candidate {}",
            grouped.len(),
            survivor.candidate_identity.candidate_id
        )));
    }

    let mut summaries = Vec::with_capacity(grouped.len());
    for (cluster_id, entity_ids) in grouped {
        let mut sum = vec![
            0.0f64;
            embedding_lookup
                .values()
                .next()
                .map(|embedding| embedding.len())
                .unwrap_or_default()
        ];
        let mut sum_sq_norm = 0.0f64;
        for entity_id in &entity_ids {
            let Some(embedding) = embedding_lookup.get(*entity_id) else {
                return Err(EvaluatorError::InvalidConfiguration(format!(
                    "section-5 hierarchy construction could not resolve embedding for entity {}",
                    entity_id
                )));
            };
            for (index, value) in embedding.iter().enumerate() {
                sum[index] += *value as f64;
            }
            sum_sq_norm += embedding
                .iter()
                .map(|value| f64::from(*value) * f64::from(*value))
                .sum::<f64>();
        }
        summaries.push(LeafClusterSummary {
            cluster_id,
            member_count: entity_ids.len(),
            leaf_descendant_count: 1,
            centroid: centroid_from_sum(&sum, entity_ids.len()),
            dispersion: dispersion_from_stats(&sum, sum_sq_norm, entity_ids.len()),
            sum,
            sum_sq_norm,
        });
    }
    Ok(summaries)
}

fn build_hierarchy(
    leaf_summaries: &[LeafClusterSummary],
    strategy: &RegisteredHierarchyStrategy,
    contract: &Section5HierarchyContract,
) -> Result<HierarchyBuild, String> {
    let mut nodes = leaf_summaries
        .iter()
        .map(|summary| BuiltNode {
            node_id: format!("leaf-{}", summary.cluster_id),
            kind: Section5HierarchyNodeKind::LeafCluster,
            member_count: summary.member_count,
            leaf_descendant_count: summary.leaf_descendant_count,
            sum: summary.sum.clone(),
            sum_sq_norm: summary.sum_sq_norm,
            centroid: summary.centroid.clone(),
            dispersion: summary.dispersion,
            child_ids: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mut current = nodes.clone();
    let mut next_internal_index = 0usize;
    let mut layer_index = 0usize;
    while current.len() > 1 {
        let groups = group_current_nodes(&current, strategy, contract, layer_index)?;
        current = groups
            .into_iter()
            .map(|group| {
                next_internal_index += 1;
                let node_id = format!("internal-{next_internal_index}");
                let member_count = group.iter().map(|child| child.member_count).sum::<usize>();
                let leaf_descendant_count = group
                    .iter()
                    .map(|child| child.leaf_descendant_count)
                    .sum::<usize>();
                let dimension_count = group
                    .first()
                    .map(|child| child.sum.len())
                    .unwrap_or_default();
                let mut sum = vec![0.0f64; dimension_count];
                let mut sum_sq_norm = 0.0f64;
                let mut child_ids = Vec::with_capacity(group.len());
                for child in &group {
                    for (index, value) in child.sum.iter().enumerate() {
                        sum[index] += *value;
                    }
                    sum_sq_norm += child.sum_sq_norm;
                    child_ids.push(child.node_id.clone());
                }
                BuiltNode {
                    node_id,
                    kind: Section5HierarchyNodeKind::Internal,
                    member_count,
                    leaf_descendant_count,
                    centroid: centroid_from_sum(&sum, member_count),
                    dispersion: dispersion_from_stats(&sum, sum_sq_norm, member_count),
                    sum,
                    sum_sq_norm,
                    child_ids,
                }
            })
            .collect::<Vec<_>>();
        nodes.extend(current.iter().cloned());
        layer_index += 1;
    }

    let Some(root) = current.first() else {
        return Err("section-5 hierarchy construction requires at least one node".into());
    };
    Ok(HierarchyBuild {
        root_id: root.node_id.clone(),
        nodes,
    })
}

fn group_current_nodes(
    current: &[BuiltNode],
    strategy: &RegisteredHierarchyStrategy,
    contract: &Section5HierarchyContract,
    layer_index: usize,
) -> Result<Vec<Vec<BuiltNode>>, String> {
    let sizes = group_sizes(current.len(), contract.fanout_min, contract.fanout_max)?;
    match strategy.identity.kind {
        Section5HierarchyStrategyKind::BottomUpAgglomeration => {
            let ordered = sort_nodes_lexicographically(current);
            Ok(chunk_by_sizes(&ordered, &sizes))
        }
        Section5HierarchyStrategyKind::RecursiveTopDownPartitioning => {
            let ordered = sort_nodes_by_dominant_axis(current, false);
            Ok(chunk_by_sizes(&ordered, &sizes))
        }
        Section5HierarchyStrategyKind::GreedyPackByCentroidNearestGrouping => {
            greedy_pack_groups(current, &sizes)
        }
        Section5HierarchyStrategyKind::HybridTopDownBottomUp => {
            let ordered = if layer_index.is_multiple_of(2) {
                sort_nodes_by_dominant_axis(current, true)
            } else {
                sort_nodes_lexicographically(current)
            };
            Ok(chunk_by_sizes(&ordered, &sizes))
        }
    }
}

fn group_sizes(count: usize, fanout_min: usize, fanout_max: usize) -> Result<Vec<usize>, String> {
    if count < fanout_min {
        return Err(format!(
            "cannot build a valid hierarchy layer with {} child nodes under fanout_min={}",
            count, fanout_min
        ));
    }
    let min_group_count = count.div_ceil(fanout_max);
    let max_group_count = count / fanout_min;
    if min_group_count == 0 || min_group_count > max_group_count {
        return Err(format!(
            "cannot satisfy fanout bounds [{}, {}] for {} child nodes",
            fanout_min, fanout_max, count
        ));
    }
    let group_count = min_group_count;
    let base = count / group_count;
    let remainder = count % group_count;
    let mut sizes = Vec::with_capacity(group_count);
    for index in 0..group_count {
        let size = base + usize::from(index < remainder);
        if size < fanout_min || size > fanout_max {
            return Err(format!(
                "computed invalid fanout {} while partitioning {} child nodes under bounds [{}, {}]",
                size, count, fanout_min, fanout_max
            ));
        }
        sizes.push(size);
    }
    Ok(sizes)
}

fn sort_nodes_lexicographically(current: &[BuiltNode]) -> Vec<BuiltNode> {
    let mut ordered = current.to_vec();
    ordered.sort_by(|left, right| {
        compare_centroids(&left.centroid, &right.centroid)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    ordered
}

fn sort_nodes_by_dominant_axis(current: &[BuiltNode], descending: bool) -> Vec<BuiltNode> {
    let dominant_axis = dominant_axis(current);
    let mut ordered = current.to_vec();
    ordered.sort_by(|left, right| {
        let comparison = left.centroid[dominant_axis]
            .partial_cmp(&right.centroid[dominant_axis])
            .unwrap_or(Ordering::Equal)
            .then_with(|| compare_centroids(&left.centroid, &right.centroid))
            .then_with(|| left.node_id.cmp(&right.node_id));
        if descending {
            comparison.reverse()
        } else {
            comparison
        }
    });
    ordered
}

fn greedy_pack_groups(
    current: &[BuiltNode],
    sizes: &[usize],
) -> Result<Vec<Vec<BuiltNode>>, String> {
    let mut remaining = sort_nodes_lexicographically(current);
    let mut groups = Vec::with_capacity(sizes.len());
    for target_size in sizes {
        let Some(seed) = remaining.first().cloned() else {
            return Err(
                "greedy hierarchy packing exhausted its remaining nodes prematurely".into(),
            );
        };
        remaining.remove(0);
        let mut group = vec![seed];
        while group.len() < *target_size {
            let Some((next_index, _)) = remaining
                .iter()
                .enumerate()
                .map(|(index, candidate)| {
                    let distance = euclidean_distance(&group[0].centroid, &candidate.centroid);
                    (index, distance)
                })
                .min_by(|left, right| {
                    left.1
                        .partial_cmp(&right.1)
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| remaining[left.0].node_id.cmp(&remaining[right.0].node_id))
                })
            else {
                return Err(
                    "greedy hierarchy packing could not satisfy its target group sizes".into(),
                );
            };
            group.push(remaining.remove(next_index));
        }
        groups.push(group);
    }
    Ok(groups)
}

fn chunk_by_sizes(nodes: &[BuiltNode], sizes: &[usize]) -> Vec<Vec<BuiltNode>> {
    let mut groups = Vec::with_capacity(sizes.len());
    let mut start = 0usize;
    for size in sizes {
        let end = start + size;
        groups.push(nodes[start..end].to_vec());
        start = end;
    }
    groups
}

fn dominant_axis(current: &[BuiltNode]) -> usize {
    let dimensions = current
        .first()
        .map(|node| node.centroid.len())
        .unwrap_or_default();
    let mut best_axis = 0usize;
    let mut best_spread = f32::NEG_INFINITY;
    for axis in 0..dimensions {
        let mut min_value = f32::INFINITY;
        let mut max_value = f32::NEG_INFINITY;
        for node in current {
            min_value = min_value.min(node.centroid[axis]);
            max_value = max_value.max(node.centroid[axis]);
        }
        let spread = max_value - min_value;
        if spread > best_spread {
            best_spread = spread;
            best_axis = axis;
        }
    }
    best_axis
}

fn build_pair_report(
    context: PairReportContext<'_>,
    build: Result<HierarchyBuild, String>,
) -> Section5PairReport {
    match build {
        Ok(build) => {
            let analysis = analyze_hierarchy(&build, context.contract);
            let gate_results = gate_results_from_analysis(&analysis, context.contract);
            let survived_required_gates = gate_results
                .iter()
                .all(|gate| matches!(gate.status, Section5GateStatus::Passed));
            let leaf_cluster_count = build
                .nodes
                .iter()
                .filter(|node| matches!(node.kind, Section5HierarchyNodeKind::LeafCluster))
                .count();
            let build_throughput_leaf_nodes_per_second = if context.elapsed == 0 {
                leaf_cluster_count as f64
            } else {
                leaf_cluster_count as f64 / (context.elapsed as f64 / 1_000_000_000.0)
            };
            let ranking_score = if survived_required_gates {
                Some(compute_pair_ranking_score(
                    analysis.max_observed_beta,
                    analysis.max_depth,
                    analysis.theoretical_depth_bound,
                    analysis.epsilon_exception_use_count,
                    build_throughput_leaf_nodes_per_second,
                    context.peak_build_memory_bytes,
                ))
            } else {
                None
            };
            Section5PairReport {
                leaf_candidate_identity: context.survivor.candidate_identity.clone(),
                hierarchy_strategy_identity: context.strategy.identity.clone(),
                originating_section4_profile_id: context.section4_profile_id.into(),
                originating_section4_source_label: context.section4_source_label.into(),
                originating_section4_ranking_score: context.survivor.ranking_score,
                originating_section4_provenance: context.survivor.provenance.clone(),
                leaf_cluster_count,
                internal_node_count: analysis.internal_node_count,
                max_depth: analysis.max_depth,
                theoretical_depth_bound: analysis.theoretical_depth_bound,
                minimum_observed_fanout: analysis.minimum_observed_fanout,
                maximum_observed_fanout: analysis.maximum_observed_fanout,
                refinement_edge_count: analysis.refinement_edge_count,
                maximum_observed_beta: analysis.max_observed_beta,
                epsilon_exception_use_count: analysis.epsilon_exception_use_count,
                build_elapsed_nanos: context.elapsed,
                build_throughput_leaf_nodes_per_second,
                peak_build_memory_bytes: context.peak_build_memory_bytes,
                gate_results,
                hierarchy_nodes: analysis.node_reports,
                hierarchy_edges: analysis.edge_reports,
                run_status: if survived_required_gates {
                    Section5PairRunStatus::Succeeded
                } else {
                    Section5PairRunStatus::GateFailed
                },
                survived_required_gates,
                ranking_score,
            }
        }
        Err(message) => Section5PairReport {
            leaf_candidate_identity: context.survivor.candidate_identity.clone(),
            hierarchy_strategy_identity: context.strategy.identity.clone(),
            originating_section4_profile_id: context.section4_profile_id.into(),
            originating_section4_source_label: context.section4_source_label.into(),
            originating_section4_ranking_score: context.survivor.ranking_score,
            originating_section4_provenance: context.survivor.provenance.clone(),
            leaf_cluster_count: context.survivor.cluster_occupancies.len(),
            internal_node_count: 0,
            max_depth: 0,
            theoretical_depth_bound: theoretical_depth_bound(
                context.survivor.cluster_occupancies.len(),
                context.contract.fanout_min,
            ),
            minimum_observed_fanout: 0,
            maximum_observed_fanout: 0,
            refinement_edge_count: 0,
            maximum_observed_beta: f64::INFINITY,
            epsilon_exception_use_count: 0,
            build_elapsed_nanos: context.elapsed,
            build_throughput_leaf_nodes_per_second: 0.0,
            peak_build_memory_bytes: context.peak_build_memory_bytes,
            gate_results: vec![Section5GateResult {
                gate_id: "hierarchy-build".into(),
                label: "Hierarchy build".into(),
                kind: Section5GateKind::FanoutBounds,
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-HIERARCHY".into()],
                status: Section5GateStatus::Failed,
                observed_value: None,
                detail: message,
            }],
            hierarchy_nodes: Vec::new(),
            hierarchy_edges: Vec::new(),
            run_status: Section5PairRunStatus::GateFailed,
            survived_required_gates: false,
            ranking_score: None,
        },
    }
}

struct HierarchyAnalysis {
    internal_node_count: usize,
    max_depth: usize,
    theoretical_depth_bound: usize,
    minimum_observed_fanout: usize,
    maximum_observed_fanout: usize,
    refinement_edge_count: usize,
    max_observed_beta: f64,
    epsilon_exception_use_count: usize,
    fanout_bounds_passed: bool,
    no_single_child_internal_nodes: bool,
    depth_bound_passed: bool,
    beta_threshold_passed: bool,
    epsilon_scope_passed: bool,
    node_reports: Vec<Section5HierarchyNodeReport>,
    edge_reports: Vec<Section5HierarchyEdgeReport>,
}

fn analyze_hierarchy(
    build: &HierarchyBuild,
    contract: &Section5HierarchyContract,
) -> HierarchyAnalysis {
    let node_lookup = build
        .nodes
        .iter()
        .map(|node| (node.node_id.as_str(), node))
        .collect::<HashMap<_, _>>();
    let mut depth_by_node = HashMap::<String, usize>::new();
    assign_depths(&node_lookup, &build.root_id, 0, &mut depth_by_node);
    let root_dispersion = node_lookup
        .get(build.root_id.as_str())
        .map(|node| node.dispersion)
        .unwrap_or(0.0);
    let overall_max_depth = depth_by_node.values().copied().max().unwrap_or(0);

    let mut node_reports = Vec::with_capacity(build.nodes.len());
    let mut edge_reports = Vec::new();
    let mut internal_node_count = 0usize;
    let mut minimum_observed_fanout = usize::MAX;
    let mut maximum_observed_fanout = 0usize;
    let mut fanout_bounds_passed = true;
    let mut no_single_child_internal_nodes = true;
    let mut beta_threshold_passed = true;
    let mut epsilon_scope_passed = true;
    let mut refinement_edge_count = 0usize;
    let mut max_observed_beta = 0.0f64;
    let mut epsilon_exception_use_count = 0usize;

    for node in &build.nodes {
        let depth_from_root = *depth_by_node.get(&node.node_id).unwrap_or(&0);
        let fanout = node.child_ids.len();
        if matches!(node.kind, Section5HierarchyNodeKind::Internal) {
            internal_node_count += 1;
            minimum_observed_fanout = minimum_observed_fanout.min(fanout);
            maximum_observed_fanout = maximum_observed_fanout.max(fanout);
            if fanout < contract.fanout_min || fanout > contract.fanout_max {
                fanout_bounds_passed = false;
            }
            if fanout == 1 {
                no_single_child_internal_nodes = false;
            }
            let all_children_are_leaves = node.child_ids.iter().all(|child_id| {
                node_lookup
                    .get(child_id.as_str())
                    .map(|child| matches!(child.kind, Section5HierarchyNodeKind::LeafCluster))
                    .unwrap_or(false)
            });
            let epsilon_scope_allowed = all_children_are_leaves
                && depth_from_root + 1 == overall_max_depth
                && if root_dispersion == 0.0 {
                    node.dispersion == 0.0
                } else {
                    node.dispersion
                        <= contract.epsilon_policy.parent_to_root_dispersion_ratio_max
                            * root_dispersion
                };
            for child_id in &node.child_ids {
                if let Some(child) = node_lookup.get(child_id.as_str()) {
                    let beta = beta_for_edge(child.dispersion, node.dispersion);
                    let epsilon_exception_applied =
                        beta > contract.beta_threshold && epsilon_scope_allowed;
                    if beta > contract.beta_threshold && !epsilon_exception_applied {
                        beta_threshold_passed = false;
                    }
                    if epsilon_exception_applied {
                        epsilon_exception_use_count += 1;
                    }
                    if beta > contract.beta_threshold && !epsilon_scope_allowed {
                        epsilon_scope_passed = false;
                    }
                    refinement_edge_count += 1;
                    max_observed_beta = max_observed_beta.max(beta);
                    edge_reports.push(Section5HierarchyEdgeReport {
                        parent_node_id: node.node_id.clone(),
                        child_node_id: child.node_id.clone(),
                        beta,
                        child_is_leaf: matches!(child.kind, Section5HierarchyNodeKind::LeafCluster),
                        epsilon_exception_applied,
                    });
                }
            }
        }
        node_reports.push(Section5HierarchyNodeReport {
            node_id: node.node_id.clone(),
            kind: node.kind.clone(),
            depth_from_root,
            fanout,
            leaf_descendant_count: node.leaf_descendant_count,
            member_count: node.member_count,
            centroid: node.centroid.clone(),
            dispersion: node.dispersion,
        });
    }
    node_reports.sort_by(|left, right| {
        left.depth_from_root
            .cmp(&right.depth_from_root)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    edge_reports.sort_by(|left, right| {
        left.parent_node_id
            .cmp(&right.parent_node_id)
            .then_with(|| left.child_node_id.cmp(&right.child_node_id))
    });
    let leaf_cluster_count = build
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, Section5HierarchyNodeKind::LeafCluster))
        .count();
    let theoretical_depth_bound = theoretical_depth_bound(leaf_cluster_count, contract.fanout_min);
    let depth_bound_passed = overall_max_depth <= theoretical_depth_bound;

    HierarchyAnalysis {
        internal_node_count,
        max_depth: overall_max_depth,
        theoretical_depth_bound,
        minimum_observed_fanout: if minimum_observed_fanout == usize::MAX {
            0
        } else {
            minimum_observed_fanout
        },
        maximum_observed_fanout,
        refinement_edge_count,
        max_observed_beta,
        epsilon_exception_use_count,
        fanout_bounds_passed,
        no_single_child_internal_nodes,
        depth_bound_passed,
        beta_threshold_passed,
        epsilon_scope_passed,
        node_reports,
        edge_reports,
    }
}

fn gate_results_from_analysis(
    analysis: &HierarchyAnalysis,
    contract: &Section5HierarchyContract,
) -> Vec<Section5GateResult> {
    vec![
        Section5GateResult {
            gate_id: "fanout-bounds".into(),
            label: "Fanout bounds".into(),
            kind: Section5GateKind::FanoutBounds,
            coverage: ResearchCoverage::Direct,
            research_goal_ids: vec!["RG-HIERARCHY".into()],
            status: bool_to_section5_gate_status(analysis.fanout_bounds_passed),
            observed_value: Some(analysis.maximum_observed_fanout as f64),
            detail: format!(
                "observed fanout range [{} , {}] under required bounds [{}, {}]",
                analysis.minimum_observed_fanout,
                analysis.maximum_observed_fanout,
                contract.fanout_min,
                contract.fanout_max
            ),
        },
        Section5GateResult {
            gate_id: "no-single-child-internal-nodes".into(),
            label: "No single-child internal nodes".into(),
            kind: Section5GateKind::NoSingleChildInternalNodes,
            coverage: ResearchCoverage::Direct,
            research_goal_ids: vec!["RG-HIERARCHY".into()],
            status: bool_to_section5_gate_status(analysis.no_single_child_internal_nodes),
            observed_value: Some(if analysis.no_single_child_internal_nodes {
                1.0
            } else {
                0.0
            }),
            detail: "every internal node must have at least two children".into(),
        },
        Section5GateResult {
            gate_id: "depth-bound".into(),
            label: "Depth bound".into(),
            kind: Section5GateKind::DepthBound,
            coverage: ResearchCoverage::Direct,
            research_goal_ids: vec!["RG-HIERARCHY".into()],
            status: bool_to_section5_gate_status(analysis.depth_bound_passed),
            observed_value: Some(analysis.max_depth as f64),
            detail: format!(
                "observed depth {} against theoretical bound {}",
                analysis.max_depth, analysis.theoretical_depth_bound
            ),
        },
        Section5GateResult {
            gate_id: "refinement-beta-threshold".into(),
            label: "Refinement beta threshold".into(),
            kind: Section5GateKind::RefinementBetaThreshold,
            coverage: ResearchCoverage::Direct,
            research_goal_ids: vec!["RG-HIERARCHY".into(), "RG-REFINEMENT".into()],
            status: bool_to_section5_gate_status(analysis.beta_threshold_passed),
            observed_value: Some(analysis.max_observed_beta),
            detail: format!(
                "required beta <= {:.6}, observed maximum beta {:.6}",
                contract.beta_threshold, analysis.max_observed_beta
            ),
        },
        Section5GateResult {
            gate_id: "epsilon-exception-scope".into(),
            label: "Epsilon exception scope".into(),
            kind: Section5GateKind::EpsilonExceptionScope,
            coverage: ResearchCoverage::Direct,
            research_goal_ids: vec!["RG-HIERARCHY".into(), "RG-REFINEMENT".into()],
            status: bool_to_section5_gate_status(analysis.epsilon_scope_passed),
            observed_value: Some(analysis.epsilon_exception_use_count as f64),
            detail: format!(
                "epsilon exceptions must remain within penultimate-layer leaf groups and parent/root dispersion ratio <= {:.6}",
                contract.epsilon_policy.parent_to_root_dispersion_ratio_max
            ),
        },
    ]
}

fn rank_pair_reports(pair_reports: &[Section5PairReport]) -> Vec<Section5RankedPair> {
    let mut ranked = pair_reports
        .iter()
        .filter_map(|pair_report| {
            pair_report
                .ranking_score
                .map(|ranking_score| Section5RankedPair {
                    leaf_candidate_id: pair_report.leaf_candidate_identity.candidate_id.clone(),
                    hierarchy_strategy_id: pair_report
                        .hierarchy_strategy_identity
                        .strategy_id
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
    });
    for (index, pair) in ranked.iter_mut().enumerate() {
        pair.rank = index + 1;
    }
    ranked
}

fn compute_pair_ranking_score(
    max_beta: f64,
    max_depth: usize,
    theoretical_depth_bound: usize,
    epsilon_exception_use_count: usize,
    throughput: f64,
    peak_build_memory_bytes: u64,
) -> f64 {
    let depth_penalty = if theoretical_depth_bound == 0 {
        0.0
    } else {
        max_depth as f64 / theoretical_depth_bound as f64
    };
    1_000.0
        - (max_beta * 100.0)
        - (depth_penalty * 25.0)
        - (epsilon_exception_use_count as f64 * 5.0)
        - (peak_build_memory_bytes as f64 / 1_000_000_000.0)
        + (throughput / 1_000_000.0)
}

fn assign_depths(
    node_lookup: &HashMap<&str, &BuiltNode>,
    node_id: &str,
    depth: usize,
    depth_by_node: &mut HashMap<String, usize>,
) {
    depth_by_node.insert(node_id.to_string(), depth);
    if let Some(node) = node_lookup.get(node_id) {
        for child_id in &node.child_ids {
            assign_depths(node_lookup, child_id, depth + 1, depth_by_node);
        }
    }
}

fn theoretical_depth_bound(leaf_cluster_count: usize, fanout_min: usize) -> usize {
    if leaf_cluster_count <= 1 {
        return 0;
    }
    let mut covered = 1usize;
    let mut depth = 0usize;
    while covered < leaf_cluster_count {
        covered = covered.saturating_mul(fanout_min);
        depth += 1;
    }
    depth
}

fn centroid_from_sum(sum: &[f64], count: usize) -> Vec<f32> {
    sum.iter()
        .map(|value| (*value / count as f64) as f32)
        .collect()
}

fn dispersion_from_stats(sum: &[f64], sum_sq_norm: f64, count: usize) -> f64 {
    if count == 0 {
        return 0.0;
    }
    let centroid_norm_sq = sum
        .iter()
        .map(|value| {
            let mean = *value / count as f64;
            mean * mean
        })
        .sum::<f64>();
    ((sum_sq_norm / count as f64) - centroid_norm_sq).max(0.0)
}

fn beta_for_edge(child_dispersion: f64, parent_dispersion: f64) -> f64 {
    if parent_dispersion == 0.0 {
        if child_dispersion == 0.0 {
            0.0
        } else {
            f64::INFINITY
        }
    } else {
        child_dispersion / parent_dispersion
    }
}

fn bool_to_section5_gate_status(value: bool) -> Section5GateStatus {
    if value {
        Section5GateStatus::Passed
    } else {
        Section5GateStatus::Failed
    }
}

fn compare_centroids(left: &[f32], right: &[f32]) -> Ordering {
    left.iter()
        .zip(right)
        .find_map(|(left, right)| {
            let comparison = left.partial_cmp(right).unwrap_or(Ordering::Equal);
            (comparison != Ordering::Equal).then_some(comparison)
        })
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

fn euclidean_distance(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = f64::from(*left) - f64::from(*right);
            delta * delta
        })
        .sum::<f64>()
        .sqrt()
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
        "pair".into()
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
