// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use lexongraph_streaming_clustering_evaluator::{
    AlignmentPolicy, BenchmarkProfile, CompressionBenchmark, CompressionMethod,
    DeferredResearchGoal, EvaluationEntity, GateDeclaration, GateKind, MetricDeclaration,
    MetricKind, ProbeWorkload, RegisteredCandidate, ReproducibilityMetadata, ResearchCoverage,
    SharedCandidateConfig, built_in_fixture_candidate,
};

pub fn strict_alignment_profile() -> BenchmarkProfile {
    BenchmarkProfile {
        profile_id: "strict-alignment-campaign".into(),
        corpus_ids: vec!["fixture-corpus-a".into()],
        shared_candidate_config: SharedCandidateConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: None,
            random_seed: Some(7),
        },
        training_passes: vec![
            vec![vec![vec![0.0, 0.0], vec![0.3, 0.0]], vec![vec![9.9, 0.0], vec![10.2, 0.0]]],
            vec![vec![vec![0.0, 0.0], vec![0.3, 0.0]], vec![vec![9.9, 0.0], vec![10.2, 0.0]]],
        ],
        probe_workloads: vec![ProbeWorkload {
            workload_id: "heldout-probes".into(),
            embeddings: vec![vec![0.15, 0.0], vec![10.05, 0.0]],
        }],
        evaluation_entities: vec![
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
        leaf_model: lexongraph_streaming_clustering_evaluator::LeafModel {
            leaf_size: 2,
            declared_final_cluster_count: 2,
            alignment_policy: AlignmentPolicy::StrictAlignment,
        },
        locality_ground_truth: vec![
            lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
                entity_id: "a".into(),
                neighbor_ids: vec!["b".into()],
            },
            lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
                entity_id: "b".into(),
                neighbor_ids: vec!["a".into()],
            },
            lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
                entity_id: "c".into(),
                neighbor_ids: vec!["d".into()],
            },
            lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
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
                gate_id: "no-empty-declared-clusters".into(),
                label: "No empty declared clusters".into(),
                kind: GateKind::NoEmptyDeclaredClusters,
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
            },
            GateDeclaration {
                gate_id: "deterministic-observable-results".into(),
                label: "Deterministic observable results".into(),
                kind: GateKind::DeterministicObservableResults,
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-DETERMINISM".into()],
            },
            GateDeclaration {
                gate_id: "same-leaf-coherence-threshold".into(),
                label: "Same-leaf coherence threshold".into(),
                kind: GateKind::MetricAtLeast {
                    metric_id: "same-leaf-neighborhood-coherence".into(),
                    minimum: 0.75,
                },
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-LOCALITY".into()],
            },
            GateDeclaration {
                gate_id: "compression-gain-threshold".into(),
                label: "Compression gain threshold".into(),
                kind: GateKind::MetricAtLeast {
                    metric_id: "local-compression-gain".into(),
                    minimum: 0.05,
                },
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-COMPRESSION".into()],
            },
        ],
        deferred_research_goals: vec![DeferredResearchGoal {
            deferred_id: "deferred-hierarchy-routing".into(),
            label: "Hierarchy routing proof".into(),
            reason: "full hierarchy, sibling structure, and persisted search routing remain outside the leaf-stage evaluator boundary".into(),
            research_goal_ids: vec!["RG-HIERARCHY".into(), "RG-ROUTING".into()],
            coverage: ResearchCoverage::Deferred,
        }],
        reproducibility: ReproducibilityMetadata {
            seed_policy: "fixed-seed-7".into(),
            software_identity: "fixture-campaign-builder".into(),
            floating_point_profile: "ieee754-deterministic-no-fma".into(),
            hardware_profile: "fixture-cpu".into(),
        },
    }
}

pub fn synthetic_padding_profile() -> BenchmarkProfile {
    let mut profile = strict_alignment_profile();
    profile.profile_id = "synthetic-padding-campaign".into();
    profile.evaluation_entities = vec![
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
            entity_id: "pad-1".into(),
            corpus_id: "fixture-corpus-a".into(),
            embedding: vec![10.2, 0.0],
            synthetic: true,
        },
    ];
    profile.leaf_model.alignment_policy = AlignmentPolicy::DeterministicSyntheticPadding;
    profile.locality_ground_truth = vec![
        lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
            entity_id: "a".into(),
            neighbor_ids: vec!["b".into()],
        },
        lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
            entity_id: "b".into(),
            neighbor_ids: vec!["a".into()],
        },
    ];
    profile
}

pub fn invalid_profile() -> BenchmarkProfile {
    let mut profile = strict_alignment_profile();
    profile.evaluation_entities.pop();
    profile
}

pub fn balanced_and_skewed_candidates() -> Vec<RegisteredCandidate> {
    vec![
        built_in_fixture_candidate("balanced-threshold").unwrap(),
        built_in_fixture_candidate("skewed-gate-fail").unwrap(),
    ]
}

pub fn shared_contract_failure_candidate() -> RegisteredCandidate {
    built_in_fixture_candidate("shared-contract-failure").unwrap()
}

pub fn nondeterministic_candidate() -> RegisteredCandidate {
    built_in_fixture_candidate("nondeterministic-probe").unwrap()
}

pub fn crate_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

pub fn lib_source() -> String {
    std::fs::read_to_string(crate_root().join("src").join("lib.rs")).unwrap()
}
