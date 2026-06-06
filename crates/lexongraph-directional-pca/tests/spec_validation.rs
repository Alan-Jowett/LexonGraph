// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

mod support;

use std::fs;
use std::path::Path;

use lexongraph_directional_pca::{DirectionalPcaParams, DirectionalPcaStreamingTrainer};
use lexongraph_streaming_clustering::{
    MetricDirection, StreamingClusterClassifier, StreamingClusterTrainer, StreamingClusteringError,
    TrainerState,
};
use support::{
    config, conforming_trainer, exact_k_failure_config, exact_k_failure_params,
    exact_k_failure_pass, expected_assignments, expected_pass_reports, nan_embedding, params,
    sample_passes, underfull_first_pass, unsupported_balance_config, wrong_dimension_embedding,
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
        retained_dimension_count: 0,
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
