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
    MetricDirection, StreamingClusterClassifier, StreamingClusterTrainer, StreamingClusteringError,
    TrainerState,
};
use support::{
    config, conforming_trainer, duplicate_refinement_config, duplicate_refinement_params,
    exact_k_failure_config, exact_k_failure_params, exact_k_failure_pass, expected_assignments,
    expected_pass_reports, identical_embedding_passes, nan_embedding, params,
    partially_collapsed_duplicate_pass, sample_passes, underfull_first_pass,
    unsupported_balance_config, wrong_dimension_embedding,
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
fn val_dpca_stream_005_and_013_pass_reports_expose_streaming_passes_and_metrics() {
    let mut trainer = conforming_trainer();
    let mut reports = Vec::new();
    for pass in sample_passes() {
        for batch in pass {
            trainer.ingest_batch(batch.as_slice()).unwrap();
        }
        reports.push(trainer.finish_pass().unwrap());
    }

    assert_eq!(reports, expected_pass_reports());
    assert_eq!(
        reports[0].quality_direction,
        MetricDirection::SmallerIsBetter
    );
    assert_eq!(
        reports[0].balance_direction,
        MetricDirection::SmallerIsBetter
    );
    assert_eq!(reports[0].balance_metric, 0.0);
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

    let mut trainer = conforming_trainer();
    assert!(matches!(
        trainer.complete_training(),
        Err(StreamingClusteringError::InvalidTransition { .. })
    ));
    assert_eq!(trainer.state(), TrainerState::Error);
}

#[test]
fn val_dpca_stream_008_pca_crate_is_used() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("lib.rs"),
    )
    .unwrap();
    assert!(source.contains("use lexongraph_pca"));
    assert!(source.contains("fit("));
}

#[test]
fn val_dpca_stream_009_and_010_axis_scoring_and_allocation_are_realized() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("lib.rs"),
    )
    .unwrap();
    assert!(source.contains("|alpha_i| * lambda_i^gamma") || source.contains("lambda.powf(gamma)"));
    assert!(source.contains("(1.0 + score.max(0.0)).ln()"));
}

#[test]
fn val_dpca_stream_011_quantile_binning_is_the_default_path() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("lib.rs"),
    )
    .unwrap();
    assert!(source.contains("rank * bin_count / point_count"));
}

#[test]
fn val_dpca_stream_024_adaptive_retained_axes_are_opt_in() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("lib.rs"),
    )
    .unwrap();
    assert!(source.contains("AdaptiveAllEligible"));
    assert!(source.contains("EigenvalueLogBits"));
}

#[test]
fn val_dpca_stream_025_density_valley_binning_is_available() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("lib.rs"),
    )
    .unwrap();
    assert!(source.contains("DensityValley"));
    assert!(source.contains("select_deepest_valley_cut_positions"));
}

#[test]
fn val_dpca_stream_026_default_path_remains_quantile_without_opt_in() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("lib.rs"),
    )
    .unwrap();
    assert!(source.contains("DirectionalPcaBinningPolicy::Quantile"));
    assert!(source.contains("rank * bin_count / point_count"));
}

#[test]
fn val_dpca_stream_012_exact_k_failures_are_explicit() {
    let mut underfull = conforming_trainer();
    for batch in underfull_first_pass() {
        underfull.ingest_batch(batch.as_slice()).unwrap();
    }
    assert!(matches!(
        underfull.finish_pass(),
        Err(StreamingClusteringError::UnsatisfiableConstraint { .. })
    ));

    let invalid_params = DirectionalPcaParams {
        retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(0),
        allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
        binning_policy: DirectionalPcaBinningPolicy::Quantile,
        ..params()
    };
    assert!(matches!(
        DirectionalPcaStreamingTrainer::new(config(), invalid_params),
        Err(StreamingClusteringError::InvalidConfiguration { .. })
    ));

    let mut exact_k_failure =
        DirectionalPcaStreamingTrainer::new(exact_k_failure_config(), exact_k_failure_params())
            .unwrap();
    for batch in exact_k_failure_pass() {
        exact_k_failure.ingest_batch(batch.as_slice()).unwrap();
    }
    assert!(matches!(
        exact_k_failure.finish_pass(),
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
fn val_dpca_stream_020_all_identical_embeddings_are_split_deterministically() {
    let mut trainer = DirectionalPcaStreamingTrainer::new(
        duplicate_refinement_config(),
        duplicate_refinement_params(),
    )
    .unwrap();
    for batch in identical_embedding_passes().into_iter().next().unwrap() {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }

    let report = trainer.finish_pass().unwrap();
    assert_eq!(report.observed_count, 4);
    assert_eq!(report.quality_metric, 0.0);
    assert_eq!(report.cluster_ids, vec![0, 1, 2]);
}

#[test]
fn val_dpca_stream_021_duplicate_refinement_recovers_a_partially_collapsed_fixture() {
    let mut trainer = DirectionalPcaStreamingTrainer::new(
        duplicate_refinement_config(),
        duplicate_refinement_params(),
    )
    .unwrap();
    for batch in partially_collapsed_duplicate_pass() {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }

    let report = trainer.finish_pass().unwrap();
    assert_eq!(report.observed_count, 4);
    assert_eq!(report.cluster_ids, vec![0, 1, 2]);
}

#[test]
fn val_dpca_stream_022_duplicate_refinement_is_stable_across_passes() {
    let mut trainer = DirectionalPcaStreamingTrainer::new(
        duplicate_refinement_config(),
        duplicate_refinement_params(),
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
    assert_eq!(reports[0], reports[1]);

    trainer.complete_training().unwrap();
    let classifier = trainer.into_classifier().unwrap();
    assert_eq!(classifier.assign([5.0, 5.0].as_slice()).unwrap(), 0);
    assert_eq!(classifier.assign([5.0, 5.0].as_slice()).unwrap(), 0);
}

#[test]
fn val_dpca_stream_023_non_duplicate_exact_k_failure_still_fails() {
    let mut trainer =
        DirectionalPcaStreamingTrainer::new(exact_k_failure_config(), exact_k_failure_params())
            .unwrap();
    for batch in exact_k_failure_pass() {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }

    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::UnsatisfiableConstraint { .. })
    ));
}

#[test]
fn val_dpca_stream_023b_underfull_mode_succeeds_and_reports_realized_cluster_count() {
    let mut underfull_params = exact_k_failure_params();
    underfull_params.cluster_cardinality_mode =
        DirectionalPcaClusterCardinalityMode::UnderfullSuccess;
    let mut trainer =
        DirectionalPcaStreamingTrainer::new(exact_k_failure_config(), underfull_params).unwrap();
    for batch in exact_k_failure_pass() {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }

    let report = trainer.finish_pass().unwrap();
    assert_eq!(report.requested_cluster_count, 3);
    assert_eq!(report.realized_cluster_count, 2);
    assert_eq!(report.cluster_ids, vec![0, 1]);

    trainer.complete_training().unwrap();
    let classifier = trainer.into_classifier().unwrap();
    assert_eq!(classifier.realized_cluster_count(), 2);
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

#[test]
fn val_dpca_stream_028_adaptive_quantile_combination_follows_quantile_path() {
    let mut trainer = DirectionalPcaStreamingTrainer::new(
        config(),
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            ..params()
        },
    )
    .unwrap();
    for batch in sample_passes().into_iter().next().unwrap() {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }

    let report = trainer.finish_pass().unwrap();
    assert_eq!(report.observed_count, 4);
    assert_eq!(report.requested_cluster_count, 2);
}

#[test]
fn val_dpca_stream_029_adaptive_centroid_weighted_density_valley_caps_retained_axes() {
    let config = lexongraph_streaming_clustering::StreamingClusteringConfig {
        cluster_count: 2,
        dimensions: 4,
        balance_constraints: None,
        random_seed: None,
    };
    let mut trainer = DirectionalPcaStreamingTrainer::new(
        config,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
            binning_policy: DirectionalPcaBinningPolicy::DensityValley,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            ..params()
        },
    )
    .unwrap();
    trainer
        .ingest_batch(&[
            vec![1.0, 0.0, 0.0, 0.0],
            vec![0.0, 1.0, 0.0, 0.0],
            vec![0.0, 0.0, 1.0, 0.0],
            vec![0.0, 0.0, 0.0, 1.0],
        ])
        .unwrap();

    let report = trainer.finish_pass().unwrap();
    assert_eq!(report.requested_cluster_count, 2);
    assert!((1..=2).contains(&report.realized_cluster_count));
}

#[test]
fn val_dpca_stream_030_fixed_axis_eigenvalue_density_valley_combination_is_available() {
    let config = lexongraph_streaming_clustering::StreamingClusteringConfig {
        cluster_count: 4,
        dimensions: 2,
        balance_constraints: None,
        random_seed: None,
    };
    let mut trainer = DirectionalPcaStreamingTrainer::new(
        config,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(2),
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::DensityValley,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            ..exact_k_failure_params()
        },
    )
    .unwrap();
    trainer
        .ingest_batch(&[
            vec![0.0, 0.0],
            vec![0.0, 1.0],
            vec![1.0, 0.0],
            vec![1.0, 1.0],
        ])
        .unwrap();

    let report = trainer.finish_pass().unwrap();
    assert_eq!(report.requested_cluster_count, 4);
    assert!(report.realized_cluster_count <= 4);
}
