// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::section5::Section5CampaignReport;
use crate::{
    BenchmarkProfile, CandidateRunReport, EvaluationEntity, EvaluatorError, ExecutionBudget,
    ResearchCoverage, resolved_profile_evaluation_entities,
};
use crate::{CandidateIdentity, EmittedArtifact};

const SECTION6_ROUTING_REASON: &str = "routing targets, recall, latency, and beam-width behavior remain deferred beyond section-6 parent-summary comparison and must be discharged by the later routing evaluation line";
const SECTION6_PERSISTENCE_REASON: &str = "serialization identity, persisted-artifact durability, and broader robustness checks remain deferred beyond section-6 parent-summary comparison and must be discharged by the later persistence and robustness evaluation line";
const SUPPORTED_SECTION6_METRIC_COMPATIBILITY_RULE: &str = "closed-profile-v1";
const SUPPORTED_SECTION6_EXACT_REFERENCE: &str = "descendant-exact-summary-v1";
const SUPPORTED_SECTION6_STORAGE_MEASUREMENT: &str = "f32-slot-count-v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section6SummaryCandidateKind {
    ExactCentroid,
    ComposedCentroid,
    CentroidPlusVarianceScalar,
    LowRankCentroidPrincipalDirection,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Section6SummaryCandidateIdentity {
    pub summary_candidate_id: String,
    pub label: String,
    pub kind: Section6SummaryCandidateKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredSection6SummaryCandidate {
    pub identity: Section6SummaryCandidateIdentity,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section6SummaryContract {
    pub contract_id: String,
    pub section5_source_label: String,
    pub exact_reference_semantics: String,
    pub delta_floor: f64,
    pub perturbation_scale: f64,
    pub storage_measurement_semantics: String,
    pub metric_compatibility_rule: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relative_error_bound_max: Option<f64>,
    pub later_evaluation_line: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_budget: Option<ExecutionBudget>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section6MetricSemanticsConsistencyResult {
    Consistent,
    UnsupportedDeclaration,
    InconsistentDeclaration,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section6GateKind {
    MetricSemanticsCompatibility,
    RelativeErrorBound,
    ExecutionBudget,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section6GateStatus {
    Passed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section6GateResult {
    pub gate_id: String,
    pub label: String,
    pub kind: Section6GateKind,
    pub coverage: ResearchCoverage,
    pub research_goal_ids: Vec<String>,
    pub status: Section6GateStatus,
    pub observed_value: Option<f64>,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section6NodeSummaryReport {
    pub node_id: String,
    pub member_count: usize,
    pub relative_l2_error: f64,
    pub perturbation_sensitivity: f64,
    pub storage_f32_slot_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section6SummaryRunStatus {
    Succeeded,
    GateFailed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section6SummaryReport {
    pub leaf_candidate_identity: CandidateIdentity,
    pub hierarchy_strategy_identity: crate::Section5HierarchyStrategyIdentity,
    pub summary_candidate_identity: Section6SummaryCandidateIdentity,
    pub originating_section4_profile_id: String,
    pub originating_section5_contract_id: String,
    pub originating_section5_source_label: String,
    pub metric_semantics_profile: String,
    pub exact_reference_semantics: String,
    pub storage_measurement_semantics: String,
    pub metric_semantics_consistency_result: Section6MetricSemanticsConsistencyResult,
    pub metric_semantics_consistency_detail: String,
    pub delta_floor: f64,
    pub internal_node_count: usize,
    pub max_relative_l2_error: f64,
    pub mean_relative_l2_error: f64,
    pub max_perturbation_sensitivity: f64,
    pub mean_perturbation_sensitivity: f64,
    pub mean_storage_f32_slot_count: f64,
    pub total_storage_f32_slot_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_budget_millis: Option<u64>,
    pub build_elapsed_nanos: u128,
    pub gate_results: Vec<Section6GateResult>,
    pub node_reports: Vec<Section6NodeSummaryReport>,
    pub run_status: Section6SummaryRunStatus,
    pub survived_required_gates: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ranking_score: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section6RankedSummary {
    pub leaf_candidate_id: String,
    pub hierarchy_strategy_id: String,
    pub summary_candidate_id: String,
    pub ranking_score: f64,
    pub rank: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section6DeferredGoalRecord {
    pub deferred_id: String,
    pub label: String,
    pub reason: String,
    pub later_evaluation_line: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section6CampaignReport {
    pub section4_profile_id: String,
    pub section5_contract_id: String,
    pub summary_contract: Section6SummaryContract,
    pub section5_campaign: Section5CampaignReport,
    pub carried_forward_pair_ids: Vec<String>,
    pub remaining_deferred_goals: Vec<Section6DeferredGoalRecord>,
    pub summary_reports: Vec<Section6SummaryReport>,
    pub ranking: Vec<Section6RankedSummary>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Section6CampaignArtifacts {
    pub per_summary_reports: Vec<EmittedArtifact>,
    pub campaign_report: EmittedArtifact,
    pub scorecard: EmittedArtifact,
    pub carry_forward_summary: EmittedArtifact,
}

#[derive(Clone)]
struct ExactNodeSummary {
    member_count: usize,
    centroid: Vec<f64>,
    variance: f64,
    principal_direction: Vec<f64>,
}

#[derive(Clone)]
enum SummaryState {
    ExactCentroid {
        count: usize,
        centroid: Vec<f64>,
    },
    ComposedCentroid {
        count: usize,
        centroid: Vec<f64>,
    },
    CentroidPlusVariance {
        count: usize,
        centroid: Vec<f64>,
        variance: f64,
    },
    LowRankCentroidDirection {
        count: usize,
        centroid: Vec<f64>,
        direction: Vec<f64>,
    },
}

pub fn registered_section6_summary_candidate_names() -> Vec<String> {
    vec![
        "exact-centroid".into(),
        "composed-centroid".into(),
        "centroid-plus-variance".into(),
        "low-rank-centroid-direction".into(),
    ]
}

pub fn resolve_registered_section6_summary_candidates(
    summary_candidate_ids: &[String],
) -> Result<Vec<RegisteredSection6SummaryCandidate>, EvaluatorError> {
    if summary_candidate_ids.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-6 execution requires at least one registered summary candidate".into(),
        ));
    }
    let mut registered = Vec::with_capacity(summary_candidate_ids.len());
    let mut seen = HashSet::new();
    for summary_candidate_id in summary_candidate_ids {
        if !seen.insert(summary_candidate_id.clone()) {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "duplicate section-6 summary candidate id {}",
                summary_candidate_id
            )));
        }
        let identity = match summary_candidate_id.as_str() {
            "exact-centroid" => Section6SummaryCandidateIdentity {
                summary_candidate_id: summary_candidate_id.clone(),
                label: "Exact centroid from descendant leaves".into(),
                kind: Section6SummaryCandidateKind::ExactCentroid,
            },
            "composed-centroid" => Section6SummaryCandidateIdentity {
                summary_candidate_id: summary_candidate_id.clone(),
                label: "Composed centroid from child summaries".into(),
                kind: Section6SummaryCandidateKind::ComposedCentroid,
            },
            "centroid-plus-variance" => Section6SummaryCandidateIdentity {
                summary_candidate_id: summary_candidate_id.clone(),
                label: "Centroid plus variance scalar".into(),
                kind: Section6SummaryCandidateKind::CentroidPlusVarianceScalar,
            },
            "low-rank-centroid-direction" => Section6SummaryCandidateIdentity {
                summary_candidate_id: summary_candidate_id.clone(),
                label: "Low-rank centroid plus first principal direction".into(),
                kind: Section6SummaryCandidateKind::LowRankCentroidPrincipalDirection,
            },
            _ => {
                return Err(EvaluatorError::InvalidConfiguration(format!(
                    "unknown section-6 summary candidate {}; expected one of: {}",
                    summary_candidate_id,
                    registered_section6_summary_candidate_names().join(", ")
                )));
            }
        };
        registered.push(RegisteredSection6SummaryCandidate { identity });
    }
    Ok(registered)
}

pub fn run_section6_campaign(
    profile: &BenchmarkProfile,
    section5_campaign: &Section5CampaignReport,
    contract: &Section6SummaryContract,
    summary_candidates: &[RegisteredSection6SummaryCandidate],
) -> Result<Section6CampaignReport, EvaluatorError> {
    validate_section6_contract(contract)?;
    validate_section6_summary_candidates(summary_candidates)?;

    let evaluation_entities = resolved_profile_evaluation_entities(profile)?;
    let carried_forward_pairs = section5_campaign
        .pair_reports
        .iter()
        .filter(|pair| pair.survived_required_gates)
        .cloned()
        .collect::<Vec<_>>();
    let carried_forward_pair_ids = carried_forward_pairs
        .iter()
        .map(|pair| {
            format!(
                "{}::{}",
                pair.leaf_candidate_identity.candidate_id,
                pair.hierarchy_strategy_identity.strategy_id
            )
        })
        .collect::<Vec<_>>();

    let mut summary_reports = Vec::new();
    for pair_report in &carried_forward_pairs {
        let survivor = section5_campaign
            .section4_campaign
            .run_reports
            .iter()
            .find(|run_report| {
                run_report.candidate_identity.candidate_id
                    == pair_report.leaf_candidate_identity.candidate_id
            })
            .ok_or_else(|| {
                EvaluatorError::InvalidConfiguration(format!(
                    "section-6 could not locate section-4 survivor report for candidate {}",
                    pair_report.leaf_candidate_identity.candidate_id
                ))
            })?;
        let cluster_memberships = build_cluster_memberships(survivor, &evaluation_entities)?;
        let tree = build_pair_tree(pair_report)?;
        for summary_candidate in summary_candidates {
            let summary_report = build_section6_summary_report(
                pair_report,
                &evaluation_entities,
                &cluster_memberships,
                &tree,
                &section5_campaign.hierarchy_contract.contract_id,
                contract,
                summary_candidate,
            )?;
            summary_reports.push(summary_report);
        }
    }
    let ranking = rank_section6_summary_reports(&summary_reports);
    let remaining_deferred_goals = remaining_section6_deferred_goals(
        &section5_campaign.remaining_deferred_goals,
        &contract.later_evaluation_line,
    );

    Ok(Section6CampaignReport {
        section4_profile_id: profile.profile_id.clone(),
        section5_contract_id: section5_campaign.hierarchy_contract.contract_id.clone(),
        summary_contract: contract.clone(),
        section5_campaign: section5_campaign.clone(),
        carried_forward_pair_ids,
        remaining_deferred_goals,
        summary_reports,
        ranking,
    })
}

pub fn emit_section6_campaign_artifacts(
    report: &Section6CampaignReport,
) -> Result<Section6CampaignArtifacts, EvaluatorError> {
    let mut per_summary_reports = Vec::with_capacity(report.summary_reports.len());
    let mut used_file_names = HashSet::new();
    for summary_report in &report.summary_reports {
        let stem = format!(
            "{}-{}-{}",
            sanitize_artifact_stem(&summary_report.leaf_candidate_identity.candidate_id),
            sanitize_artifact_stem(&summary_report.hierarchy_strategy_identity.strategy_id),
            sanitize_artifact_stem(
                &summary_report
                    .summary_candidate_identity
                    .summary_candidate_id
            ),
        );
        let file_name =
            unique_artifact_file_name(&mut used_file_names, &stem, "-summary-report.json");
        per_summary_reports.push(EmittedArtifact {
            file_name,
            contents: serde_json::to_string_pretty(summary_report)
                .map_err(|error| EvaluatorError::Json(error.to_string()))?,
        });
    }
    Ok(Section6CampaignArtifacts {
        per_summary_reports,
        campaign_report: EmittedArtifact {
            file_name: "section6-campaign-report.json".into(),
            contents: serde_json::to_string_pretty(report)
                .map_err(|error| EvaluatorError::Json(error.to_string()))?,
        },
        scorecard: EmittedArtifact {
            file_name: "section6-scorecard.txt".into(),
            contents: render_section6_scorecard(report),
        },
        carry_forward_summary: EmittedArtifact {
            file_name: "section6-carry-forward-summary.txt".into(),
            contents: render_section6_carry_forward_summary(report),
        },
    })
}

pub fn write_section6_campaign_artifacts(
    output_dir: &Path,
    artifacts: &Section6CampaignArtifacts,
) -> Result<Vec<PathBuf>, EvaluatorError> {
    std::fs::create_dir_all(output_dir).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to create section-6 artifact directory {}: {error}",
            output_dir.display()
        ))
    })?;

    let mut written = Vec::with_capacity(artifacts.per_summary_reports.len() + 3);
    for artifact in artifacts.per_summary_reports.iter().chain([
        &artifacts.campaign_report,
        &artifacts.scorecard,
        &artifacts.carry_forward_summary,
    ]) {
        let path = output_dir.join(&artifact.file_name);
        std::fs::write(&path, &artifact.contents).map_err(|error| {
            EvaluatorError::Io(format!(
                "failed to write section-6 artifact {}: {error}",
                path.display()
            ))
        })?;
        written.push(path);
    }
    Ok(written)
}

pub fn render_section6_scorecard(report: &Section6CampaignReport) -> String {
    let mut lines = vec![format!(
        "Section-6 scorecard for {} [{}]",
        report.section4_profile_id, report.summary_contract.contract_id
    )];
    lines.push(format!(
        "Carried-forward section-5 pairs: {}",
        if report.carried_forward_pair_ids.is_empty() {
            "none".into()
        } else {
            report.carried_forward_pair_ids.join(", ")
        }
    ));
    for summary_report in &report.summary_reports {
        let execution_budget = summary_report
            .execution_budget_millis
            .map(|budget| format!(", execution_budget_millis={budget}"))
            .unwrap_or_default();
        lines.push(format!(
            "- {} x {} x {}: {:?}, metric_semantics={:?}, max_error={:.6}, mean_error={:.6}, max_sensitivity={:.6}, mean_storage_slots={:.3}, total_storage_slots={}{}",
            summary_report.leaf_candidate_identity.candidate_id,
            summary_report.hierarchy_strategy_identity.strategy_id,
            summary_report.summary_candidate_identity.summary_candidate_id,
            summary_report.run_status,
            summary_report.metric_semantics_consistency_result,
            summary_report.max_relative_l2_error,
            summary_report.mean_relative_l2_error,
            summary_report.max_perturbation_sensitivity,
            summary_report.mean_storage_f32_slot_count,
            summary_report.total_storage_f32_slot_count,
            execution_budget
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

pub fn render_section6_carry_forward_summary(report: &Section6CampaignReport) -> String {
    let carried_forward = report
        .ranking
        .iter()
        .map(|summary| {
            format!(
                "{} x {} x {}",
                summary.leaf_candidate_id,
                summary.hierarchy_strategy_id,
                summary.summary_candidate_id
            )
        })
        .collect::<Vec<_>>();
    let mut lines = vec![format!(
        "Section-6 carry-forward summary for {} [{}]",
        report.section4_profile_id, report.summary_contract.contract_id
    )];
    lines.push(format!(
        "Originating section-5 source: {}",
        report.summary_contract.section5_source_label
    ));
    lines.push(format!(
        "Carried forward summary candidates: {}",
        if carried_forward.is_empty() {
            "none".into()
        } else {
            carried_forward.join(", ")
        }
    ));
    for ranked in &report.ranking {
        lines.push(format!(
            "- rank {}: {} x {} x {} (ranking_score={:.6})",
            ranked.rank,
            ranked.leaf_candidate_id,
            ranked.hierarchy_strategy_id,
            ranked.summary_candidate_id,
            ranked.ranking_score
        ));
    }
    let mut rejected = report
        .summary_reports
        .iter()
        .filter(|summary_report| !summary_report.survived_required_gates)
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
            .then_with(|| {
                left.summary_candidate_identity
                    .summary_candidate_id
                    .cmp(&right.summary_candidate_identity.summary_candidate_id)
            })
    });
    if !rejected.is_empty() {
        lines.push("Rejected summary candidates:".into());
        for summary_report in rejected {
            lines.push(format!(
                "- {} x {} x {}",
                summary_report.leaf_candidate_identity.candidate_id,
                summary_report.hierarchy_strategy_identity.strategy_id,
                summary_report
                    .summary_candidate_identity
                    .summary_candidate_id
            ));
        }
    }
    lines.join("\n")
}

fn validate_section6_contract(contract: &Section6SummaryContract) -> Result<(), EvaluatorError> {
    if contract.contract_id.trim().is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-6 contract_id must not be empty".into(),
        ));
    }
    if contract.section5_source_label.trim().is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-6 section5_source_label must not be empty".into(),
        ));
    }
    if contract.exact_reference_semantics != SUPPORTED_SECTION6_EXACT_REFERENCE {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "unsupported section-6 exact_reference_semantics {}; expected {}",
            contract.exact_reference_semantics, SUPPORTED_SECTION6_EXACT_REFERENCE
        )));
    }
    if contract.delta_floor <= 0.0 || !contract.delta_floor.is_finite() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-6 delta_floor must be finite and strictly positive".into(),
        ));
    }
    if contract.perturbation_scale < 0.0 || !contract.perturbation_scale.is_finite() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-6 perturbation_scale must be finite and non-negative".into(),
        ));
    }
    if contract.storage_measurement_semantics != SUPPORTED_SECTION6_STORAGE_MEASUREMENT {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "unsupported section-6 storage_measurement_semantics {}; expected {}",
            contract.storage_measurement_semantics, SUPPORTED_SECTION6_STORAGE_MEASUREMENT
        )));
    }
    if let Some(bound) = contract.relative_error_bound_max
        && (!bound.is_finite() || bound < 0.0)
    {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-6 relative_error_bound_max must be finite and non-negative".into(),
        ));
    }
    if contract.later_evaluation_line.trim().is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-6 later_evaluation_line must not be empty".into(),
        ));
    }
    if let Some(execution_budget) = &contract.execution_budget
        && execution_budget.wall_clock_limit_millis == 0
    {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-6 execution budget must be positive when provided".into(),
        ));
    }
    Ok(())
}

fn validate_section6_summary_candidates(
    summary_candidates: &[RegisteredSection6SummaryCandidate],
) -> Result<(), EvaluatorError> {
    if summary_candidates.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-6 execution requires at least one summary candidate".into(),
        ));
    }
    Ok(())
}

fn build_section6_summary_report(
    pair_report: &crate::Section5PairReport,
    evaluation_entities: &[EvaluationEntity],
    cluster_memberships: &HashMap<u32, Vec<usize>>,
    tree: &PairTree,
    section5_contract_id: &str,
    contract: &Section6SummaryContract,
    summary_candidate: &RegisteredSection6SummaryCandidate,
) -> Result<Section6SummaryReport, EvaluatorError> {
    let started = Instant::now();
    let metric_semantics = validate_summary_metric_semantics(
        &contract.metric_compatibility_rule,
        &pair_report.metric_semantics_profile,
    );

    let mut node_reports = Vec::new();
    let mut max_relative_l2_error = 0.0f64;
    let mut max_perturbation_sensitivity = 0.0f64;
    let mut total_relative_l2_error = 0.0f64;
    let mut total_perturbation_sensitivity = 0.0f64;
    let mut total_storage_f32_slot_count = 0usize;
    let mut exact_cache = HashMap::<String, ExactNodeSummary>::new();
    let mut state_cache = HashMap::<String, SummaryState>::new();

    for node in tree
        .nodes
        .values()
        .filter(|node| matches!(node.kind, crate::Section5HierarchyNodeKind::Internal))
    {
        let exact = exact_summary_for_node(
            node.node_id.as_str(),
            tree,
            cluster_memberships,
            evaluation_entities,
            &mut exact_cache,
        )?;
        let candidate_state = summary_state_for_node(
            node.node_id.as_str(),
            tree,
            cluster_memberships,
            evaluation_entities,
            summary_candidate.identity.kind.clone(),
            &mut exact_cache,
            &mut state_cache,
        )?;
        let exact_vector = exact_reference_vector(&exact, &summary_candidate.identity.kind);
        let candidate_vector = summary_state_vector(&candidate_state);
        let node_relative_l2_error =
            relative_l2_error(&candidate_vector, &exact_vector, contract.delta_floor);
        let perturbation_sensitivity = if matches!(
            summary_candidate.identity.kind,
            Section6SummaryCandidateKind::ExactCentroid
        ) {
            0.0
        } else {
            let child_states = child_states_for_node(
                node.node_id.as_str(),
                tree,
                cluster_memberships,
                evaluation_entities,
                summary_candidate.identity.kind.clone(),
                &mut exact_cache,
                &mut state_cache,
            )?;
            let perturbed_state = compose_state_from_children(
                &summary_candidate.identity.kind,
                perturb_children(child_states, contract.perturbation_scale),
            )?;
            relative_l2_error(
                &summary_state_vector(&candidate_state),
                &summary_state_vector(&perturbed_state),
                contract.delta_floor,
            )
        };
        let storage_f32_slot_count =
            storage_slots_for_summary_kind(&summary_candidate.identity.kind, exact.centroid.len());
        max_relative_l2_error = max_relative_l2_error.max(node_relative_l2_error);
        max_perturbation_sensitivity = max_perturbation_sensitivity.max(perturbation_sensitivity);
        total_relative_l2_error += node_relative_l2_error;
        total_perturbation_sensitivity += perturbation_sensitivity;
        total_storage_f32_slot_count += storage_f32_slot_count;
        node_reports.push(Section6NodeSummaryReport {
            node_id: node.node_id.clone(),
            member_count: exact.member_count,
            relative_l2_error: node_relative_l2_error,
            perturbation_sensitivity,
            storage_f32_slot_count,
        });
    }
    node_reports.sort_by(|left, right| left.node_id.cmp(&right.node_id));

    let internal_node_count = node_reports.len();
    let mean_relative_l2_error = if internal_node_count == 0 {
        0.0
    } else {
        total_relative_l2_error / internal_node_count as f64
    };
    let mean_perturbation_sensitivity = if internal_node_count == 0 {
        0.0
    } else {
        total_perturbation_sensitivity / internal_node_count as f64
    };
    let mean_storage_f32_slot_count = if internal_node_count == 0 {
        0.0
    } else {
        total_storage_f32_slot_count as f64 / internal_node_count as f64
    };

    let mut gate_results = vec![metric_semantics_gate_result(&metric_semantics)];
    gate_results.push(relative_error_gate_result(
        contract.relative_error_bound_max,
        max_relative_l2_error,
    ));
    let build_elapsed_nanos = started.elapsed().as_nanos();
    gate_results.push(execution_budget_gate_result(
        contract.execution_budget.as_ref(),
        build_elapsed_nanos,
    ));
    let survived_required_gates = gate_results
        .iter()
        .all(|gate| matches!(gate.status, Section6GateStatus::Passed));
    let run_status = if survived_required_gates {
        Section6SummaryRunStatus::Succeeded
    } else {
        Section6SummaryRunStatus::GateFailed
    };
    let ranking_score = if survived_required_gates {
        Some(
            1.0 / (1.0
                + max_relative_l2_error
                + mean_perturbation_sensitivity
                + mean_storage_f32_slot_count / 1000.0),
        )
    } else {
        None
    };

    Ok(Section6SummaryReport {
        leaf_candidate_identity: pair_report.leaf_candidate_identity.clone(),
        hierarchy_strategy_identity: pair_report.hierarchy_strategy_identity.clone(),
        summary_candidate_identity: summary_candidate.identity.clone(),
        originating_section4_profile_id: pair_report.originating_section4_profile_id.clone(),
        originating_section5_contract_id: section5_contract_id.into(),
        originating_section5_source_label: contract.section5_source_label.clone(),
        metric_semantics_profile: pair_report.metric_semantics_profile.clone(),
        exact_reference_semantics: contract.exact_reference_semantics.clone(),
        storage_measurement_semantics: contract.storage_measurement_semantics.clone(),
        metric_semantics_consistency_result: metric_semantics.0,
        metric_semantics_consistency_detail: metric_semantics.1,
        delta_floor: contract.delta_floor,
        internal_node_count,
        max_relative_l2_error,
        mean_relative_l2_error,
        max_perturbation_sensitivity,
        mean_perturbation_sensitivity,
        mean_storage_f32_slot_count,
        total_storage_f32_slot_count,
        execution_budget_millis: contract
            .execution_budget
            .as_ref()
            .map(|budget| budget.wall_clock_limit_millis),
        build_elapsed_nanos,
        gate_results,
        node_reports,
        run_status,
        survived_required_gates,
        ranking_score,
    })
}

fn rank_section6_summary_reports(
    summary_reports: &[Section6SummaryReport],
) -> Vec<Section6RankedSummary> {
    let mut surviving = summary_reports
        .iter()
        .filter_map(|summary_report| {
            summary_report.ranking_score.map(|ranking_score| {
                (
                    summary_report.leaf_candidate_identity.candidate_id.clone(),
                    summary_report
                        .hierarchy_strategy_identity
                        .strategy_id
                        .clone(),
                    summary_report
                        .summary_candidate_identity
                        .summary_candidate_id
                        .clone(),
                    ranking_score,
                )
            })
        })
        .collect::<Vec<_>>();
    surviving.sort_by(|left, right| {
        right
            .3
            .partial_cmp(&left.3)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    surviving
        .into_iter()
        .enumerate()
        .map(
            |(
                index,
                (leaf_candidate_id, hierarchy_strategy_id, summary_candidate_id, ranking_score),
            )| {
                Section6RankedSummary {
                    leaf_candidate_id,
                    hierarchy_strategy_id,
                    summary_candidate_id,
                    ranking_score,
                    rank: index + 1,
                }
            },
        )
        .collect()
}

fn remaining_section6_deferred_goals(
    section5_goals: &[crate::Section5DeferredGoalRecord],
    later_evaluation_line: &str,
) -> Vec<Section6DeferredGoalRecord> {
    let mut deferred = BTreeMap::<String, Section6DeferredGoalRecord>::new();
    for goal in section5_goals {
        if goal.deferred_id == "section5-deferred-parent-summary" {
            continue;
        }
        deferred.insert(
            goal.deferred_id.clone(),
            Section6DeferredGoalRecord {
                deferred_id: goal.deferred_id.clone(),
                label: goal.label.clone(),
                reason: goal.reason.clone(),
                later_evaluation_line: goal.later_evaluation_line.clone(),
            },
        );
    }
    for (deferred_id, label, reason) in [
        (
            "section6-deferred-routing",
            "Routing and beam-width evaluation",
            SECTION6_ROUTING_REASON,
        ),
        (
            "section6-deferred-persistence",
            "Serialization, persistence, and robustness evaluation",
            SECTION6_PERSISTENCE_REASON,
        ),
    ] {
        deferred
            .entry(deferred_id.into())
            .or_insert_with(|| Section6DeferredGoalRecord {
                deferred_id: deferred_id.into(),
                label: label.into(),
                reason: reason.into(),
                later_evaluation_line: later_evaluation_line.into(),
            });
    }
    deferred.into_values().collect()
}

fn validate_summary_metric_semantics(
    rule: &str,
    metric_semantics_profile: &str,
) -> (Section6MetricSemanticsConsistencyResult, String) {
    if rule != SUPPORTED_SECTION6_METRIC_COMPATIBILITY_RULE {
        return (
            Section6MetricSemanticsConsistencyResult::InconsistentDeclaration,
            format!(
                "unsupported section-6 metric_compatibility_rule {}; expected {}",
                rule, SUPPORTED_SECTION6_METRIC_COMPATIBILITY_RULE
            ),
        );
    }
    match metric_semantics_profile {
        "euclidean" | "cosine" => (
            Section6MetricSemanticsConsistencyResult::Consistent,
            format!(
                "section-6 summary comparison supports metric semantics profile {} under {}",
                metric_semantics_profile, rule
            ),
        ),
        _ => (
            Section6MetricSemanticsConsistencyResult::UnsupportedDeclaration,
            format!(
                "section-6 summary comparison does not support metric semantics profile {}",
                metric_semantics_profile
            ),
        ),
    }
}

fn metric_semantics_gate_result(
    metric_semantics: &(Section6MetricSemanticsConsistencyResult, String),
) -> Section6GateResult {
    Section6GateResult {
        gate_id: "metric-semantics-compatibility".into(),
        label: "Metric semantics compatibility".into(),
        kind: Section6GateKind::MetricSemanticsCompatibility,
        coverage: ResearchCoverage::Direct,
        research_goal_ids: vec!["RG-SUMMARY".into()],
        status: match metric_semantics.0 {
            Section6MetricSemanticsConsistencyResult::Consistent => Section6GateStatus::Passed,
            Section6MetricSemanticsConsistencyResult::UnsupportedDeclaration
            | Section6MetricSemanticsConsistencyResult::InconsistentDeclaration => {
                Section6GateStatus::Failed
            }
        },
        observed_value: None,
        detail: metric_semantics.1.clone(),
    }
}

fn relative_error_gate_result(
    relative_error_bound_max: Option<f64>,
    max_relative_l2_error: f64,
) -> Section6GateResult {
    let (status, detail) = match relative_error_bound_max {
        Some(bound) if max_relative_l2_error <= bound => (
            Section6GateStatus::Passed,
            format!(
                "maximum observed relative L2 error {:.6} is within the configured bound {:.6}",
                max_relative_l2_error, bound
            ),
        ),
        Some(bound) => (
            Section6GateStatus::Failed,
            format!(
                "maximum observed relative L2 error {:.6} exceeds the configured bound {:.6}",
                max_relative_l2_error, bound
            ),
        ),
        None => (
            Section6GateStatus::Passed,
            "no section-6 relative-error hard bound was configured".into(),
        ),
    };
    Section6GateResult {
        gate_id: "relative-l2-error-bound".into(),
        label: "Relative L2 error bound".into(),
        kind: Section6GateKind::RelativeErrorBound,
        coverage: ResearchCoverage::Direct,
        research_goal_ids: vec!["RG-SUMMARY".into()],
        status,
        observed_value: Some(max_relative_l2_error),
        detail,
    }
}

fn execution_budget_gate_result(
    execution_budget: Option<&ExecutionBudget>,
    build_elapsed_nanos: u128,
) -> Section6GateResult {
    let observed_millis = build_elapsed_nanos as f64 / 1_000_000.0;
    let (status, detail) = match execution_budget {
        Some(budget) if observed_millis <= budget.wall_clock_limit_millis as f64 => (
            Section6GateStatus::Passed,
            format!(
                "observed section-6 elapsed time {:.3}ms is within the configured budget {}ms",
                observed_millis, budget.wall_clock_limit_millis
            ),
        ),
        Some(budget) => (
            Section6GateStatus::Failed,
            format!(
                "observed section-6 elapsed time {:.3}ms exceeds the configured budget {}ms",
                observed_millis, budget.wall_clock_limit_millis
            ),
        ),
        None => (
            Section6GateStatus::Passed,
            "no section-6 execution budget was configured".into(),
        ),
    };
    Section6GateResult {
        gate_id: "execution-budget".into(),
        label: "Execution budget".into(),
        kind: Section6GateKind::ExecutionBudget,
        coverage: ResearchCoverage::Direct,
        research_goal_ids: vec!["RG-SUMMARY".into()],
        status,
        observed_value: Some(observed_millis),
        detail,
    }
}

fn build_cluster_memberships(
    survivor: &CandidateRunReport,
    evaluation_entities: &[EvaluationEntity],
) -> Result<HashMap<u32, Vec<usize>>, EvaluatorError> {
    let entity_index = evaluation_entities
        .iter()
        .enumerate()
        .map(|(index, entity)| (entity.entity_id.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut memberships = HashMap::<u32, Vec<usize>>::new();
    for member in survivor.effective_leaf_membership() {
        let index = entity_index.get(member.entity_id.as_str()).ok_or_else(|| {
            EvaluatorError::InvalidConfiguration(format!(
                "section-6 could not resolve evaluation entity {}",
                member.entity_id
            ))
        })?;
        memberships
            .entry(member.cluster_id)
            .or_default()
            .push(*index);
    }
    Ok(memberships)
}

#[derive(Clone)]
struct PairTreeNode {
    node_id: String,
    kind: crate::Section5HierarchyNodeKind,
}

struct PairTree {
    nodes: HashMap<String, PairTreeNode>,
    children: HashMap<String, Vec<String>>,
}

fn build_pair_tree(pair_report: &crate::Section5PairReport) -> Result<PairTree, EvaluatorError> {
    let nodes = pair_report
        .hierarchy_nodes
        .iter()
        .map(|node| {
            (
                node.node_id.clone(),
                PairTreeNode {
                    node_id: node.node_id.clone(),
                    kind: node.kind.clone(),
                },
            )
        })
        .collect::<HashMap<_, _>>();
    let mut children = HashMap::<String, Vec<String>>::new();
    for edge in &pair_report.hierarchy_edges {
        children
            .entry(edge.parent_node_id.clone())
            .or_default()
            .push(edge.child_node_id.clone());
    }
    let _root_id = pair_report
        .hierarchy_nodes
        .iter()
        .find(|node| node.depth_from_root == 0)
        .map(|node| node.node_id.clone())
        .ok_or_else(|| {
            EvaluatorError::InvalidConfiguration(
                "section-6 requires a hierarchy root in the carried-forward section-5 report"
                    .into(),
            )
        })?;
    Ok(PairTree { nodes, children })
}

fn exact_summary_for_node(
    node_id: &str,
    tree: &PairTree,
    cluster_memberships: &HashMap<u32, Vec<usize>>,
    evaluation_entities: &[EvaluationEntity],
    cache: &mut HashMap<String, ExactNodeSummary>,
) -> Result<ExactNodeSummary, EvaluatorError> {
    if let Some(existing) = cache.get(node_id) {
        return Ok(existing.clone());
    }
    let mut descendant_cache = HashMap::<String, Vec<usize>>::new();
    let entity_indices = descendant_entity_indices_for_node(
        node_id,
        tree,
        cluster_memberships,
        &mut descendant_cache,
    )?;
    let exact = exact_summary_from_entity_indices(&entity_indices, evaluation_entities)?;
    cache.insert(node_id.into(), exact.clone());
    Ok(exact)
}

fn descendant_entity_indices_for_node(
    node_id: &str,
    tree: &PairTree,
    cluster_memberships: &HashMap<u32, Vec<usize>>,
    cache: &mut HashMap<String, Vec<usize>>,
) -> Result<Vec<usize>, EvaluatorError> {
    if let Some(existing) = cache.get(node_id) {
        return Ok(existing.clone());
    }
    let node = tree.nodes.get(node_id).ok_or_else(|| {
        EvaluatorError::InvalidConfiguration(format!(
            "section-6 could not resolve hierarchy node {}",
            node_id
        ))
    })?;
    let indices = match node.kind {
        crate::Section5HierarchyNodeKind::LeafCluster => {
            let cluster_id = parse_leaf_cluster_id(node_id)?;
            cluster_memberships
                .get(&cluster_id)
                .cloned()
                .ok_or_else(|| {
                    EvaluatorError::InvalidConfiguration(format!(
                        "section-6 could not resolve leaf cluster {}",
                        cluster_id
                    ))
                })?
        }
        crate::Section5HierarchyNodeKind::Internal => {
            let mut indices = Vec::new();
            for child_id in tree.children.get(node_id).cloned().unwrap_or_default() {
                indices.extend(descendant_entity_indices_for_node(
                    child_id.as_str(),
                    tree,
                    cluster_memberships,
                    cache,
                )?);
            }
            indices
        }
    };
    cache.insert(node_id.into(), indices.clone());
    Ok(indices)
}

fn exact_summary_from_entity_indices(
    entity_indices: &[usize],
    evaluation_entities: &[EvaluationEntity],
) -> Result<ExactNodeSummary, EvaluatorError> {
    if entity_indices.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-6 exact summary requires at least one descendant entity".into(),
        ));
    }
    let dimension_count = evaluation_entities[entity_indices[0]].embedding.len();
    let mut centroid = vec![0.0f64; dimension_count];
    for entity_index in entity_indices {
        let embedding = &evaluation_entities[*entity_index].embedding;
        for (index, value) in embedding.iter().enumerate() {
            centroid[index] += *value as f64;
        }
    }
    for value in &mut centroid {
        *value /= entity_indices.len() as f64;
    }
    let mut variance_sum = 0.0f64;
    let mut centered = Vec::with_capacity(entity_indices.len());
    for entity_index in entity_indices {
        let embedding = &evaluation_entities[*entity_index].embedding;
        let centered_embedding = embedding
            .iter()
            .enumerate()
            .map(|(index, value)| *value as f64 - centroid[index])
            .collect::<Vec<_>>();
        variance_sum += centered_embedding
            .iter()
            .map(|value| value * value)
            .sum::<f64>();
        centered.push(centered_embedding);
    }
    let principal_direction = principal_direction(&centered);
    Ok(ExactNodeSummary {
        member_count: entity_indices.len(),
        centroid,
        variance: variance_sum / entity_indices.len() as f64,
        principal_direction,
    })
}

fn summary_state_for_node(
    node_id: &str,
    tree: &PairTree,
    cluster_memberships: &HashMap<u32, Vec<usize>>,
    evaluation_entities: &[EvaluationEntity],
    kind: Section6SummaryCandidateKind,
    exact_cache: &mut HashMap<String, ExactNodeSummary>,
    state_cache: &mut HashMap<String, SummaryState>,
) -> Result<SummaryState, EvaluatorError> {
    if let Some(existing) = state_cache.get(node_id) {
        return Ok(existing.clone());
    }
    let node = tree.nodes.get(node_id).ok_or_else(|| {
        EvaluatorError::InvalidConfiguration(format!(
            "section-6 could not resolve hierarchy node {}",
            node_id
        ))
    })?;
    let state = match node.kind {
        crate::Section5HierarchyNodeKind::LeafCluster => {
            let exact = exact_summary_for_node(
                node_id,
                tree,
                cluster_memberships,
                evaluation_entities,
                exact_cache,
            )?;
            match kind {
                Section6SummaryCandidateKind::ExactCentroid => SummaryState::ExactCentroid {
                    count: exact.member_count,
                    centroid: exact.centroid,
                },
                Section6SummaryCandidateKind::ComposedCentroid => SummaryState::ComposedCentroid {
                    count: exact.member_count,
                    centroid: exact.centroid,
                },
                Section6SummaryCandidateKind::CentroidPlusVarianceScalar => {
                    SummaryState::CentroidPlusVariance {
                        count: exact.member_count,
                        centroid: exact.centroid,
                        variance: exact.variance,
                    }
                }
                Section6SummaryCandidateKind::LowRankCentroidPrincipalDirection => {
                    SummaryState::LowRankCentroidDirection {
                        count: exact.member_count,
                        centroid: exact.centroid,
                        direction: exact.principal_direction,
                    }
                }
            }
        }
        crate::Section5HierarchyNodeKind::Internal => {
            if matches!(kind, Section6SummaryCandidateKind::ExactCentroid) {
                let exact = exact_summary_for_node(
                    node_id,
                    tree,
                    cluster_memberships,
                    evaluation_entities,
                    exact_cache,
                )?;
                SummaryState::ExactCentroid {
                    count: exact.member_count,
                    centroid: exact.centroid,
                }
            } else {
                let child_states = child_states_for_node(
                    node_id,
                    tree,
                    cluster_memberships,
                    evaluation_entities,
                    kind.clone(),
                    exact_cache,
                    state_cache,
                )?;
                compose_state_from_children(&kind, child_states)?
            }
        }
    };
    state_cache.insert(node_id.into(), state.clone());
    Ok(state)
}

fn child_states_for_node(
    node_id: &str,
    tree: &PairTree,
    cluster_memberships: &HashMap<u32, Vec<usize>>,
    evaluation_entities: &[EvaluationEntity],
    kind: Section6SummaryCandidateKind,
    exact_cache: &mut HashMap<String, ExactNodeSummary>,
    state_cache: &mut HashMap<String, SummaryState>,
) -> Result<Vec<SummaryState>, EvaluatorError> {
    tree.children
        .get(node_id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|child_id| {
            summary_state_for_node(
                child_id.as_str(),
                tree,
                cluster_memberships,
                evaluation_entities,
                kind.clone(),
                exact_cache,
                state_cache,
            )
        })
        .collect()
}

fn compose_state_from_children(
    kind: &Section6SummaryCandidateKind,
    child_states: Vec<SummaryState>,
) -> Result<SummaryState, EvaluatorError> {
    match kind {
        Section6SummaryCandidateKind::ExactCentroid => Err(EvaluatorError::InvalidConfiguration(
            "section-6 exact-centroid summaries should not be composed from child states".into(),
        )),
        Section6SummaryCandidateKind::ComposedCentroid => {
            let mut total_count = 0usize;
            let mut centroid = Vec::<f64>::new();
            for child_state in child_states {
                let (count, child_centroid) = match child_state {
                    SummaryState::ComposedCentroid { count, centroid }
                    | SummaryState::ExactCentroid { count, centroid } => (count, centroid),
                    _ => {
                        return Err(EvaluatorError::InvalidConfiguration(
                            "section-6 composed-centroid expected centroid child states".into(),
                        ));
                    }
                };
                if centroid.is_empty() {
                    centroid = vec![0.0; child_centroid.len()];
                }
                total_count += count;
                for (index, value) in child_centroid.iter().enumerate() {
                    centroid[index] += *value * count as f64;
                }
            }
            for value in &mut centroid {
                *value /= total_count as f64;
            }
            Ok(SummaryState::ComposedCentroid {
                count: total_count,
                centroid,
            })
        }
        Section6SummaryCandidateKind::CentroidPlusVarianceScalar => {
            let mut total_count = 0usize;
            let mut centroid = Vec::<f64>::new();
            let mut children = Vec::new();
            for child_state in child_states {
                let (count, child_centroid, variance) = match child_state {
                    SummaryState::CentroidPlusVariance {
                        count,
                        centroid,
                        variance,
                    } => (count, centroid, variance),
                    _ => {
                        return Err(EvaluatorError::InvalidConfiguration(
                            "section-6 centroid-plus-variance expected variance child states"
                                .into(),
                        ));
                    }
                };
                if centroid.is_empty() {
                    centroid = vec![0.0; child_centroid.len()];
                }
                total_count += count;
                for (index, value) in child_centroid.iter().enumerate() {
                    centroid[index] += *value * count as f64;
                }
                children.push((count, child_centroid, variance));
            }
            for value in &mut centroid {
                *value /= total_count as f64;
            }
            let mut second_moment = 0.0f64;
            for (count, child_centroid, variance) in children {
                let centroid_delta = child_centroid
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        let delta = *value - centroid[index];
                        delta * delta
                    })
                    .sum::<f64>();
                second_moment += count as f64 * (variance + centroid_delta);
            }
            Ok(SummaryState::CentroidPlusVariance {
                count: total_count,
                centroid,
                variance: second_moment / total_count as f64,
            })
        }
        Section6SummaryCandidateKind::LowRankCentroidPrincipalDirection => {
            let mut total_count = 0usize;
            let mut centroid = Vec::<f64>::new();
            let mut weighted_direction = Vec::<f64>::new();
            for child_state in child_states {
                let (count, child_centroid, child_direction) = match child_state {
                    SummaryState::LowRankCentroidDirection {
                        count,
                        centroid,
                        direction,
                    } => (count, centroid, direction),
                    _ => {
                        return Err(EvaluatorError::InvalidConfiguration(
                            "section-6 low-rank summaries expected low-rank child states".into(),
                        ));
                    }
                };
                if centroid.is_empty() {
                    centroid = vec![0.0; child_centroid.len()];
                    weighted_direction = vec![0.0; child_direction.len()];
                }
                total_count += count;
                for (index, value) in child_centroid.iter().enumerate() {
                    centroid[index] += *value * count as f64;
                }
                for (index, value) in child_direction.iter().enumerate() {
                    weighted_direction[index] += *value * count as f64;
                }
            }
            for value in &mut centroid {
                *value /= total_count as f64;
            }
            Ok(SummaryState::LowRankCentroidDirection {
                count: total_count,
                centroid,
                direction: normalize_vector(weighted_direction),
            })
        }
    }
}

fn exact_reference_vector(
    exact: &ExactNodeSummary,
    kind: &Section6SummaryCandidateKind,
) -> Vec<f64> {
    match kind {
        Section6SummaryCandidateKind::ExactCentroid
        | Section6SummaryCandidateKind::ComposedCentroid => exact.centroid.clone(),
        Section6SummaryCandidateKind::CentroidPlusVarianceScalar => {
            let mut vector = exact.centroid.clone();
            vector.push(exact.variance);
            vector
        }
        Section6SummaryCandidateKind::LowRankCentroidPrincipalDirection => {
            let mut vector = exact.centroid.clone();
            vector.extend(exact.principal_direction.clone());
            vector
        }
    }
}

fn summary_state_vector(state: &SummaryState) -> Vec<f64> {
    match state {
        SummaryState::ExactCentroid { centroid, .. }
        | SummaryState::ComposedCentroid { centroid, .. } => centroid.clone(),
        SummaryState::CentroidPlusVariance {
            centroid, variance, ..
        } => {
            let mut vector = centroid.clone();
            vector.push(*variance);
            vector
        }
        SummaryState::LowRankCentroidDirection {
            centroid,
            direction,
            ..
        } => {
            let mut vector = centroid.clone();
            vector.extend(direction.clone());
            vector
        }
    }
}

fn perturb_children(child_states: Vec<SummaryState>, perturbation_scale: f64) -> Vec<SummaryState> {
    child_states
        .into_iter()
        .enumerate()
        .map(|(child_index, child_state)| {
            let factor = perturbation_scale * (child_index + 1) as f64;
            match child_state {
                SummaryState::ExactCentroid { count, centroid } => SummaryState::ExactCentroid {
                    count,
                    centroid: perturb_vector(centroid, factor),
                },
                SummaryState::ComposedCentroid { count, centroid } => {
                    SummaryState::ComposedCentroid {
                        count,
                        centroid: perturb_vector(centroid, factor),
                    }
                }
                SummaryState::CentroidPlusVariance {
                    count,
                    centroid,
                    variance,
                } => SummaryState::CentroidPlusVariance {
                    count,
                    centroid: perturb_vector(centroid, factor),
                    variance: variance + factor,
                },
                SummaryState::LowRankCentroidDirection {
                    count,
                    centroid,
                    direction,
                } => SummaryState::LowRankCentroidDirection {
                    count,
                    centroid: perturb_vector(centroid, factor),
                    direction: normalize_vector(perturb_vector(direction, factor)),
                },
            }
        })
        .collect()
}

fn perturb_vector(mut vector: Vec<f64>, factor: f64) -> Vec<f64> {
    for (index, value) in vector.iter_mut().enumerate() {
        *value += factor * (index + 1) as f64 * 1.0e-3;
    }
    vector
}

fn principal_direction(centered: &[Vec<f64>]) -> Vec<f64> {
    if centered.is_empty() {
        return Vec::new();
    }
    let dimension_count = centered[0].len();
    let mut vector = vec![1.0f64; dimension_count];
    vector = normalize_vector(vector);
    for _ in 0..12 {
        let mut next = vec![0.0f64; dimension_count];
        for point in centered {
            let projection = point
                .iter()
                .zip(vector.iter())
                .map(|(left, right)| left * right)
                .sum::<f64>();
            for (index, value) in point.iter().enumerate() {
                next[index] += projection * value;
            }
        }
        if next.iter().all(|value| value.abs() < 1.0e-12) {
            break;
        }
        vector = normalize_vector(next);
    }
    vector
}

fn normalize_vector(mut vector: Vec<f64>) -> Vec<f64> {
    let norm = vector.iter().map(|value| value * value).sum::<f64>().sqrt();
    if norm <= 0.0 {
        return vector;
    }
    for value in &mut vector {
        *value /= norm;
    }
    vector
}

fn relative_l2_error(candidate: &[f64], exact: &[f64], delta_floor: f64) -> f64 {
    let numerator = candidate
        .iter()
        .zip(exact.iter())
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum::<f64>()
        .sqrt();
    let exact_norm = exact.iter().map(|value| value * value).sum::<f64>().sqrt();
    numerator / exact_norm.max(delta_floor)
}

fn storage_slots_for_summary_kind(kind: &Section6SummaryCandidateKind, dimensions: usize) -> usize {
    match kind {
        Section6SummaryCandidateKind::ExactCentroid
        | Section6SummaryCandidateKind::ComposedCentroid => dimensions,
        Section6SummaryCandidateKind::CentroidPlusVarianceScalar => dimensions + 1,
        Section6SummaryCandidateKind::LowRankCentroidPrincipalDirection => dimensions * 2,
    }
}

fn parse_leaf_cluster_id(node_id: &str) -> Result<u32, EvaluatorError> {
    node_id
        .strip_prefix("leaf-")
        .ok_or_else(|| {
            EvaluatorError::InvalidConfiguration(format!(
                "section-6 expected leaf node id with leaf- prefix, observed {}",
                node_id
            ))
        })?
        .parse::<u32>()
        .map_err(|error| {
            EvaluatorError::InvalidConfiguration(format!(
                "section-6 could not parse leaf cluster id from {}: {error}",
                node_id
            ))
        })
}

fn sanitize_artifact_stem(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

fn unique_artifact_file_name(
    used_file_names: &mut HashSet<String>,
    stem: &str,
    suffix: &str,
) -> String {
    let base = format!("{stem}{suffix}");
    if used_file_names.insert(base.clone()) {
        return base;
    }
    let mut counter = 2usize;
    loop {
        let candidate = format!("{stem}-{counter}{suffix}");
        if used_file_names.insert(candidate.clone()) {
            return candidate;
        }
        counter += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SUPPORTED_SECTION6_EXACT_REFERENCE, SUPPORTED_SECTION6_STORAGE_MEASUREMENT,
        Section6SummaryContract, normalize_vector, relative_l2_error, validate_section6_contract,
    };
    use crate::{EvaluatorError, ExecutionBudget};

    #[test]
    fn section6_contract_rejects_zero_delta_floor() {
        let result = validate_section6_contract(&Section6SummaryContract {
            contract_id: "section6-invalid".into(),
            section5_source_label: "fixture".into(),
            exact_reference_semantics: SUPPORTED_SECTION6_EXACT_REFERENCE.into(),
            delta_floor: 0.0,
            perturbation_scale: 0.001,
            storage_measurement_semantics: SUPPORTED_SECTION6_STORAGE_MEASUREMENT.into(),
            metric_compatibility_rule: "closed-profile-v1".into(),
            relative_error_bound_max: Some(0.01),
            later_evaluation_line: "future routing evaluator".into(),
            execution_budget: Some(ExecutionBudget {
                wall_clock_limit_millis: 1_000,
            }),
        });

        assert!(matches!(
            result,
            Err(EvaluatorError::InvalidConfiguration(message))
                if message.contains("delta_floor")
        ));
    }

    #[test]
    fn relative_error_uses_delta_floor() {
        let error = relative_l2_error(&[1.0e-12], &[0.0], 1.0e-6);
        assert!(error.is_finite());
        assert!(error > 0.0);
    }

    #[test]
    fn normalize_vector_preserves_zero_vector() {
        assert_eq!(normalize_vector(vec![0.0, 0.0]), vec![0.0, 0.0]);
    }
}
