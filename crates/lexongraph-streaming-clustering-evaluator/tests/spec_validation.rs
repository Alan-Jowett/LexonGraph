// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

mod support;

use std::path::Path;

use lexongraph_streaming_clustering::{
    MetricDirection, PassReport, StreamingClusterClassifier, StreamingClusterTrainer,
    StreamingClusteringConfig, StreamingClusteringError, TrainerState, validate_embedding,
};
use lexongraph_streaming_clustering_evaluator::{
    CandidateIdentity, CandidateRunStatus, DeferredMeasurementStatus, EvaluatorError, GateStatus,
    built_in_fixture_candidate_names, candidate_adapter, emit_campaign_artifacts,
    run_evaluation_campaign,
};
use support::{
    balanced_and_skewed_candidates, invalid_profile, lib_source, nondeterministic_candidate,
    shared_contract_failure_candidate, strict_alignment_profile, synthetic_padding_profile,
};

#[derive(Clone, Copy)]
enum InvalidRangeMode {
    Probe,
    LeafMembership,
}

struct InvalidRangeTrainer {
    config: StreamingClusteringConfig,
    state: TrainerState,
    mode: InvalidRangeMode,
}

impl InvalidRangeTrainer {
    fn new(config: &StreamingClusteringConfig, mode: InvalidRangeMode) -> Self {
        Self {
            config: config.clone(),
            state: TrainerState::Idle,
            mode,
        }
    }
}

impl StreamingClusterTrainer for InvalidRangeTrainer {
    type Classifier = InvalidRangeClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn state(&self) -> TrainerState {
        self.state
    }

    fn ingest_batch(&mut self, embeddings: &[Vec<f32>]) -> Result<(), StreamingClusteringError> {
        for embedding in embeddings {
            validate_embedding(embedding, self.config.dimensions)?;
        }
        self.state = TrainerState::Ingesting;
        Ok(())
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        if self.state != TrainerState::Ingesting {
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::InvalidTransition {
                state: self.state,
                operation: "finish_pass".into(),
            });
        }
        self.state = TrainerState::PassComplete;
        Ok(PassReport {
            observed_count: 4,
            quality_metric: 0.0,
            balance_metric: 0.0,
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0, 1],
        })
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        if self.state != TrainerState::PassComplete {
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::InvalidTransition {
                state: self.state,
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
        Ok(InvalidRangeClassifier {
            config: self.config,
            mode: self.mode,
        })
    }
}

struct InvalidRangeClassifier {
    config: StreamingClusteringConfig,
    mode: InvalidRangeMode,
}

impl StreamingClusterClassifier for InvalidRangeClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn assign(&self, embedding: &[f32]) -> Result<u32, StreamingClusteringError> {
        validate_embedding(embedding, self.config.dimensions)?;
        let first = embedding[0];
        match self.mode {
            InvalidRangeMode::Probe
                if (0.1..0.2).contains(&first) || (10.0..10.1).contains(&first) =>
            {
                Ok(self.config.cluster_count)
            }
            InvalidRangeMode::LeafMembership if first < 0.05 => Ok(self.config.cluster_count),
            _ => Ok(if first < 5.0 { 0 } else { 1 }),
        }
    }
}

#[test]
fn val_stream_eval_001_repository_includes_crate_and_spec_package() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(manifest_dir.join("src").join("lib.rs").exists());
    assert!(manifest_dir.join("src").join("main.rs").exists());
    assert!(
        manifest_dir
            .join("..")
            .join("..")
            .join("docs")
            .join("specs")
            .join("rust-streaming-clustering-evaluator-crate")
            .join("requirements.md")
            .exists()
    );
}

#[test]
fn val_stream_eval_002_public_surface_remains_subordinate_to_the_shared_contract() {
    let source = lib_source();
    assert!(source.contains("docs/specs/rust-streaming-clustering-crate/"));
    assert!(source.contains("candidate_adapter"));
    assert!(source.contains("StreamingClusterTrainer"));
    assert!(!source.contains("algorithm-specific evaluation hooks"));
}

#[test]
fn val_stream_eval_003_campaign_runs_two_registered_candidates_under_one_profile() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &balanced_and_skewed_candidates(),
    )
    .unwrap();

    assert_eq!(report.run_reports.len(), 2);
    assert_eq!(
        report.run_reports[0].provenance.profile_id,
        "strict-alignment-campaign"
    );
}

#[test]
fn val_stream_eval_004_candidate_registration_uses_adapter_or_factory_to_construct_trainers() {
    let source = lib_source();
    assert!(source.contains("pub fn candidate_adapter"));
    assert!(source.contains("Fn(&StreamingClusteringConfig)"));
    assert!(source.contains("T: StreamingClusterTrainer"));
}

#[test]
fn val_stream_eval_005_benchmark_profile_declares_the_required_campaign_fields() {
    let profile = strict_alignment_profile();

    assert_eq!(profile.corpus_ids, vec!["fixture-corpus-a"]);
    assert_eq!(profile.leaf_model.leaf_size, 2);
    assert_eq!(profile.metric_declarations.len(), 2);
    assert!(!profile.gate_declarations.is_empty());
    assert!(!profile.deferred_research_goals.is_empty());
    assert_eq!(profile.reproducibility.seed_policy, "fixed-seed-7");
}

#[test]
fn val_stream_eval_006_repeated_execution_reports_determinism() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();

    assert!(report.run_reports[0].determinism.deterministic);
    assert!(
        report.run_reports[0]
            .determinism
            .mismatch_details
            .is_empty()
    );
}

#[test]
fn val_stream_eval_007_provenance_manifest_records_reproducibility_metadata() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let provenance = &report.run_reports[0].provenance;

    assert_eq!(
        provenance.candidate_identity.candidate_id,
        "balanced-threshold"
    );
    assert_eq!(provenance.seed_policy, "fixed-seed-7");
    assert_eq!(provenance.software_identity, "fixture-campaign-builder");
    assert_eq!(
        provenance.floating_point_profile,
        "ieee754-deterministic-no-fma"
    );
    assert_eq!(provenance.hardware_profile, "fixture-cpu");
}

#[test]
fn val_stream_eval_008_run_report_includes_lifecycle_outputs_and_leaf_membership_materialization() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let run_report = &report.run_reports[0];

    assert_eq!(run_report.pass_reports.len(), 2);
    assert_eq!(run_report.probe_results.len(), 1);
    assert_eq!(run_report.leaf_membership.len(), 4);
}

#[test]
fn val_stream_eval_009_strict_alignment_profile_verifies_fixed_capacity_invariants() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let run_report = &report.run_reports[0];

    assert!(run_report
        .gate_results
        .iter()
        .any(|gate| gate.gate_id == "exact-leaf-occupancy" && gate.status == GateStatus::Passed));
    assert!(
        run_report
            .gate_results
            .iter()
            .any(|gate| gate.gate_id == "complete-coverage" && gate.status == GateStatus::Passed)
    );
    assert!(
        run_report
            .gate_results
            .iter()
            .any(|gate| gate.gate_id == "one-cluster-per-entity"
                && gate.status == GateStatus::Passed)
    );
}

#[test]
fn val_stream_eval_010_synthetic_padding_is_distinguished_and_excluded_from_external_metrics() {
    let report = run_evaluation_campaign(
        &synthetic_padding_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let run_report = &report.run_reports[0];

    assert!(
        run_report
            .leaf_membership
            .iter()
            .any(|member| member.entity_id == "pad-1" && member.synthetic)
    );
    assert!(
        run_report
            .cluster_occupancies
            .iter()
            .any(|occupancy| occupancy.synthetic_count == 1)
    );
    assert!(
        run_report
            .metric_results
            .iter()
            .find(|metric| metric.metric_id == "same-leaf-neighborhood-coherence")
            .unwrap()
            .value
            > 0.0
    );
}

#[test]
fn val_stream_eval_011_same_leaf_locality_metric_uses_ground_truth_over_real_entities() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let metric = report.run_reports[0]
        .metric_results
        .iter()
        .find(|metric| metric.metric_id == "same-leaf-neighborhood-coherence")
        .unwrap();

    assert_eq!(metric.value, 1.0);
}

#[test]
fn val_stream_eval_012_local_compression_metric_compares_local_and_global_baselines() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let metric = report.run_reports[0]
        .metric_results
        .iter()
        .find(|metric| metric.metric_id == "local-compression-gain")
        .unwrap();

    assert!(metric.value > 0.05);
}

#[test]
fn val_stream_eval_013_report_distinguishes_prerequisites_gates_and_metrics_and_excludes_gate_failures_from_ranking()
 {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &balanced_and_skewed_candidates(),
    )
    .unwrap();
    let balanced = report
        .run_reports
        .iter()
        .find(|run| run.candidate_identity.candidate_id == "balanced-threshold")
        .unwrap();
    let skewed = report
        .run_reports
        .iter()
        .find(|run| run.candidate_identity.candidate_id == "skewed-gate-fail")
        .unwrap();

    assert!(balanced.prerequisite_checks[0].passed);
    assert!(!balanced.metric_results.is_empty());
    assert!(!balanced.gate_results.is_empty());
    assert_eq!(balanced.run_status, CandidateRunStatus::Succeeded);
    assert_eq!(skewed.run_status, CandidateRunStatus::GateFailed);
    assert!(skewed.ranking_score.is_none());
    assert_eq!(report.ranking.len(), 1);
}

#[test]
fn val_stream_eval_014_metric_gate_and_deferred_records_trace_to_research_goals() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let run_report = &report.run_reports[0];

    assert!(
        run_report
            .metric_results
            .iter()
            .all(|metric| !metric.research_goal_ids.is_empty())
    );
    assert!(
        run_report
            .gate_results
            .iter()
            .all(|gate| !gate.research_goal_ids.is_empty())
    );
    assert!(run_report.deferred_research_goals.iter().all(|goal| {
        !goal.research_goal_ids.is_empty() && goal.status == DeferredMeasurementStatus::Deferred
    }));
}

#[test]
fn val_stream_eval_015_emits_machine_readable_reports_and_a_human_scorecard() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &balanced_and_skewed_candidates(),
    )
    .unwrap();
    let artifacts = emit_campaign_artifacts(&report).unwrap();

    assert_eq!(artifacts.per_candidate_reports.len(), 2);
    assert!(
        artifacts
            .campaign_report
            .contents
            .contains("\"profile_id\": \"strict-alignment-campaign\"")
    );
    assert!(artifacts.scorecard.contents.contains("Campaign scorecard"));
}

#[test]
fn val_stream_eval_016_invalid_profiles_and_shared_contract_failures_are_distinguished() {
    let invalid = run_evaluation_campaign(
        &invalid_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    );
    assert!(matches!(
        invalid,
        Err(EvaluatorError::InvalidConfiguration(_))
    ));

    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[shared_contract_failure_candidate()],
    )
    .unwrap();
    assert_eq!(
        report.run_reports[0].run_status,
        CandidateRunStatus::CandidateSharedContractFailure
    );
}

#[test]
fn val_stream_eval_017_gate_failures_are_reported_separately_from_deferred_goals() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &balanced_and_skewed_candidates(),
    )
    .unwrap();
    let skewed = report
        .run_reports
        .iter()
        .find(|run| run.candidate_identity.candidate_id == "skewed-gate-fail")
        .unwrap();

    assert!(
        skewed
            .gate_results
            .iter()
            .any(|gate| gate.status == GateStatus::Failed)
    );
    assert!(!skewed.deferred_research_goals.is_empty());
}

#[test]
fn val_stream_eval_018_deferred_hierarchy_and_search_goals_remain_explicitly_deferred() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();

    assert!(
        report.run_reports[0].deferred_research_goals[0]
            .reason
            .contains("outside the leaf-stage evaluator boundary")
    );
}

#[test]
fn val_stream_eval_019_repository_verification_artifacts_cover_the_evaluator_surface() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(
        manifest_dir
            .join("tests")
            .join("spec_validation.rs")
            .exists()
    );
    assert!(
        manifest_dir
            .join("tests")
            .join("support")
            .join("mod.rs")
            .exists()
    );
    assert!(built_in_fixture_candidate_names().contains(&"nondeterministic-probe"));
}

#[test]
fn val_stream_eval_determinism_gate_detects_observable_nondeterminism() {
    let report =
        run_evaluation_campaign(&strict_alignment_profile(), &[nondeterministic_candidate()])
            .unwrap();

    assert!(!report.run_reports[0].determinism.deterministic);
    assert_eq!(
        report.run_reports[0].run_status,
        CandidateRunStatus::GateFailed
    );
}

#[test]
fn regression_probe_assignments_outside_k_are_reported_as_shared_contract_failures() {
    let candidate = candidate_adapter(
        CandidateIdentity {
            candidate_id: "invalid-probe-cluster-id".into(),
            implementation_label: "Invalid probe cluster-id fixture".into(),
            software_identity: "invalid-probe-cluster-id-v1".into(),
        },
        |config| Ok(InvalidRangeTrainer::new(config, InvalidRangeMode::Probe)),
    );
    let report = run_evaluation_campaign(&strict_alignment_profile(), &[candidate]).unwrap();

    assert_eq!(
        report.run_reports[0].run_status,
        CandidateRunStatus::CandidateSharedContractFailure
    );
    assert!(
        report.run_reports[0].prerequisite_checks[0]
            .detail
            .contains("outside [0, 2)")
    );
}

#[test]
fn regression_leaf_membership_assignments_outside_k_are_reported_as_shared_contract_failures() {
    let candidate = candidate_adapter(
        CandidateIdentity {
            candidate_id: "invalid-leaf-cluster-id".into(),
            implementation_label: "Invalid leaf cluster-id fixture".into(),
            software_identity: "invalid-leaf-cluster-id-v1".into(),
        },
        |config| {
            Ok(InvalidRangeTrainer::new(
                config,
                InvalidRangeMode::LeafMembership,
            ))
        },
    );
    let report = run_evaluation_campaign(&strict_alignment_profile(), &[candidate]).unwrap();

    assert_eq!(
        report.run_reports[0].run_status,
        CandidateRunStatus::CandidateSharedContractFailure
    );
    assert!(
        report.run_reports[0].prerequisite_checks[0]
            .detail
            .contains("outside [0, 2)")
    );
}
