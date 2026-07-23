// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

mod support;

use std::fs;
use std::path::Path;

use lexongraph_directional_pca::{
    DirectionalPcaAllocationPolicy, DirectionalPcaBinningPolicy,
    DirectionalPcaClusterCardinalityMode, DirectionalPcaParams, DirectionalPcaRetainedAxisPolicy,
    DirectionalPcaStreamingTrainer,
};
use lexongraph_streaming_clustering::{
    PassReadiness, StreamingClusterClassifier, StreamingClusterTrainer, StreamingClusteringError,
    TrainerState,
};
use support::{
    config, conforming_trainer, expected_assignments, expected_pass_reports,
    identical_embedding_passes, nan_embedding, non_duplicate_underfull_passes, params,
    partially_collapsed_duplicate_passes, sample_passes, underfull_first_pass,
    underfull_success_config, underfull_success_passes, unsupported_balance_config,
    wrong_dimension_embedding,
};

#[test]
fn val_dpca_stream_001_and_002_repository_and_public_surface_exist() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(manifest_dir.join("src/lib.rs").exists());
    assert!(manifest_dir.join("tests/spec_validation.rs").exists());
    assert!(
        manifest_dir
            .join("..")
            .join("..")
            .join("docs")
            .join("specs")
            .join("rust-directional-pca-crate")
            .join("requirements.md")
            .exists()
    );

    let trainer = conforming_trainer();
    assert_eq!(trainer.state(), TrainerState::Idle);
}

#[test]
fn val_dpca_stream_003_trainer_construction_preserves_config_and_params() {
    let trainer = DirectionalPcaStreamingTrainer::new(config(), params()).unwrap();
    assert_eq!(trainer.config(), &config());
}

#[test]
fn val_dpca_stream_004_balance_constraints_are_rejected() {
    assert!(matches!(
        DirectionalPcaStreamingTrainer::new(unsupported_balance_config(), params()),
        Err(StreamingClusteringError::InvalidConfiguration { .. })
    ));
}

#[test]
fn val_dpca_stream_005_and_013_pass_reports_progress_from_analysis_to_partition_ready() {
    let mut trainer = conforming_trainer();
    let mut reports = Vec::new();
    for pass in sample_passes() {
        for batch in pass {
            trainer.ingest_batch(batch.as_slice()).unwrap();
        }
        reports.push(trainer.finish_pass().unwrap());
    }

    assert_eq!(reports, expected_pass_reports());
    assert_eq!(reports[0].readiness, PassReadiness::AnalysisOnly);
    assert_eq!(reports[1].readiness, PassReadiness::AnalysisOnly);
    assert_eq!(reports[2].readiness, PassReadiness::PartitionReady);
    assert_eq!(reports[3].readiness, PassReadiness::PartitionReady);
}

#[test]
fn val_dpca_stream_006_cross_pass_continuity_is_enforced() {
    let mut trainer = conforming_trainer();
    for batch in &sample_passes()[0] {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }
    trainer.finish_pass().unwrap();
    trainer
        .ingest_batch(&[vec![0.0, 0.0], vec![1.0, 0.0]])
        .unwrap();
    trainer
        .ingest_batch(&[vec![10.0, 0.0], vec![12.0, 0.0]])
        .unwrap();

    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));
}

#[test]
fn val_dpca_stream_007_and_016_malformed_input_and_invalid_transitions_are_explicit() {
    let mut trainer = conforming_trainer();
    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::InvalidTransition { .. })
    ));
    assert_eq!(trainer.state(), TrainerState::Error);

    let mut trainer = conforming_trainer();
    assert!(matches!(
        trainer.ingest_batch(&[wrong_dimension_embedding()]),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));

    let mut trainer = conforming_trainer();
    assert!(matches!(
        trainer.ingest_batch(&[nan_embedding()]),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));

    let mut trainer = conforming_trainer();
    trainer.ingest_batch(&[]).unwrap();
    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));
}

#[test]
fn val_dpca_stream_008_uses_streaming_pca_accumulation_without_full_pass_buffers() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("lib.rs"),
    )
    .unwrap();
    assert!(source.contains("PcaAccumulator"));
    assert!(source.contains(".update(embedding)"));
    assert!(!source.contains("current_pass: Vec<Embedding>"));
    assert!(!source.contains("fit("));
}

#[test]
fn val_dpca_stream_009_and_010_policy_surfaces_remain_available() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("lib.rs"),
    )
    .unwrap();
    assert!(source.contains("CentroidWeightedBins"));
    assert!(source.contains("EigenvalueLogBits"));
    assert!(source.contains("Quantile"));
    assert!(source.contains("DensityValley"));
}

#[test]
fn val_dpca_stream_011_caller_visible_replay_is_required_before_partition_ready() {
    let mut trainer = conforming_trainer();
    for batch in &sample_passes()[0] {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }
    let first = trainer.finish_pass().unwrap();
    assert_eq!(first.readiness, PassReadiness::AnalysisOnly);
    assert!(matches!(
        trainer.complete_training(),
        Err(StreamingClusteringError::InvalidTransition { .. })
    ));
}

#[test]
fn val_dpca_stream_012_exact_mode_rejects_underfull_first_passes() {
    let mut trainer =
        DirectionalPcaStreamingTrainer::new(underfull_success_config(), params()).unwrap();
    for batch in underfull_first_pass() {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }
    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::UnsatisfiableConstraint { .. })
    ));
}

#[test]
fn val_dpca_stream_014_and_015_classifier_assigns_deterministically() {
    let mut trainer = conforming_trainer();
    for pass in sample_passes() {
        for batch in pass {
            trainer.ingest_batch(batch.as_slice()).unwrap();
        }
        trainer.finish_pass().unwrap();
    }
    trainer.complete_training().unwrap();
    let classifier = trainer.into_classifier().unwrap();

    for (embedding, expected_cluster_id) in expected_assignments() {
        assert_eq!(
            classifier.assign(embedding.as_slice()).unwrap(),
            expected_cluster_id
        );
    }
    assert!(matches!(
        classifier.assign(wrong_dimension_embedding().as_slice()),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));
    assert!(matches!(
        classifier.assign(nan_embedding().as_slice()),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));
}

#[test]
fn val_dpca_stream_017_dead_block_store_surface_is_removed() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("lib.rs"),
    )
    .unwrap();
    let manifest =
        fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")).unwrap();

    assert!(!source.contains("BlockStore"));
    assert!(!source.contains("BlockHash"));
    assert!(!manifest.contains("lexongraph-block-store"));
    assert!(!manifest.contains("lexongraph-block ="));
}

#[test]
fn val_dpca_stream_018_shared_conformance_helpers_pass() {
    let harness = support::Harness;
    lexongraph_streaming_clustering::conformance::run_streaming_clustering_suite(&harness).unwrap();
}

#[test]
fn val_dpca_stream_020_and_022_identical_embeddings_export_replay_faithful_support() {
    let mut trainer = DirectionalPcaStreamingTrainer::new(
        underfull_success_config(),
        DirectionalPcaParams {
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
            ..params()
        },
    )
    .unwrap();
    let passes = identical_embedding_passes();
    let mut reports = Vec::new();
    for pass in passes {
        for batch in pass {
            trainer.ingest_batch(batch.as_slice()).unwrap();
        }
        reports.push(trainer.finish_pass().unwrap());
    }
    assert_eq!(reports[2].cluster_ids, Some(vec![0, 1, 2]));
    assert_eq!(reports[3].cluster_ids, Some(vec![0, 1, 2]));
    trainer.complete_training().unwrap();
    let classifier = trainer.into_classifier().unwrap();
    let mut replay_state = classifier
        .new_replay_state()
        .expect("duplicate refinement should export replay state");
    let replayed = std::iter::repeat_n([5.0, 5.0], 4)
        .map(|embedding| {
            classifier
                .replay_assign(embedding.as_slice(), &mut replay_state)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(replayed, vec![0, 0, 1, 2]);
}

#[test]
fn val_dpca_stream_021_duplicate_refinement_recovers_a_partially_collapsed_fixture() {
    let mut trainer = DirectionalPcaStreamingTrainer::new(
        underfull_success_config(),
        DirectionalPcaParams {
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
            ..params()
        },
    )
    .unwrap();
    let mut reports = Vec::new();
    for pass in partially_collapsed_duplicate_passes() {
        for batch in pass {
            trainer.ingest_batch(batch.as_slice()).unwrap();
        }
        reports.push(trainer.finish_pass().unwrap());
    }
    assert_eq!(reports[2].cluster_ids, Some(vec![0, 1, 2]));
    assert_eq!(reports[3].cluster_ids, Some(vec![0, 1, 2]));

    trainer.complete_training().unwrap();
    let classifier = trainer.into_classifier().unwrap();
    assert_eq!(classifier.realized_cluster_count(), 3);
    let mut replay_state = classifier
        .new_replay_state()
        .expect("duplicate refinement should export replay state");
    let replayed = [
        vec![0.0, 0.0],
        vec![0.0, 0.0],
        vec![0.0, 0.0],
        vec![10.0, 0.0],
    ]
    .into_iter()
    .map(|embedding| {
        classifier
            .replay_assign(embedding.as_slice(), &mut replay_state)
            .unwrap()
    })
    .collect::<Vec<_>>();
    assert_eq!(replayed, vec![0, 0, 1, 2]);
}

#[test]
fn val_dpca_stream_022a_duplicate_refinement_exports_explicit_child_support() {
    let mut trainer = DirectionalPcaStreamingTrainer::new(
        underfull_success_config(),
        DirectionalPcaParams {
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
            ..params()
        },
    )
    .unwrap();
    for pass in identical_embedding_passes() {
        for batch in pass {
            trainer.ingest_batch(batch.as_slice()).unwrap();
        }
        trainer.finish_pass().unwrap();
    }

    trainer.complete_training().unwrap();
    let classifier = trainer.into_classifier().unwrap();
    let mut replay_state = classifier
        .new_replay_state()
        .expect("duplicate refinement should export replay-order support");
    let replayed = std::iter::repeat_n([5.0, 5.0], 4)
        .map(|embedding| {
            classifier
                .replay_assign(embedding.as_slice(), &mut replay_state)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(replayed, vec![0, 0, 1, 2]);
}

#[test]
fn val_dpca_stream_023b_underfull_mode_succeeds_and_reports_realized_cluster_count() {
    let mut underfull_params = params();
    underfull_params.cluster_cardinality_mode =
        DirectionalPcaClusterCardinalityMode::UnderfullSuccess;
    let mut trainer =
        DirectionalPcaStreamingTrainer::new(underfull_success_config(), underfull_params).unwrap();
    let mut final_report = None;
    for pass in underfull_success_passes() {
        for batch in pass {
            trainer.ingest_batch(batch.as_slice()).unwrap();
        }
        final_report = Some(trainer.finish_pass().unwrap());
    }

    let report = final_report.unwrap();
    assert_eq!(report.requested_cluster_count, 3);
    assert_eq!(report.realized_cluster_count, Some(2));
    assert_eq!(report.cluster_ids, Some(vec![0, 1]));

    trainer.complete_training().unwrap();
    let classifier = trainer.into_classifier().unwrap();
    assert_eq!(classifier.realized_cluster_count(), 2);
}

#[test]
fn val_dpca_stream_023_non_duplicate_exact_k_failure_still_fails() {
    let mut trainer = DirectionalPcaStreamingTrainer::new(
        underfull_success_config(),
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(2),
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
            ..params()
        },
    )
    .unwrap();
    let mut final_result = None;
    for pass in non_duplicate_underfull_passes() {
        for batch in pass {
            trainer.ingest_batch(batch.as_slice()).unwrap();
        }
        final_result = Some(trainer.finish_pass());
        if final_result.as_ref().is_some_and(|result| result.is_err()) {
            break;
        }
    }

    assert!(matches!(
        final_result.unwrap(),
        Err(StreamingClusteringError::UnsatisfiableConstraint { .. })
    ));
}

#[test]
fn val_dpca_stream_027_mixed_policy_combinations_construct_successfully() {
    let mixed_params = [
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            ..params()
        },
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
            binning_policy: DirectionalPcaBinningPolicy::DensityValley,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            ..params()
        },
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(2),
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::DensityValley,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            ..params()
        },
    ];

    for params in mixed_params {
        DirectionalPcaStreamingTrainer::new(config(), params).unwrap();
    }
}
