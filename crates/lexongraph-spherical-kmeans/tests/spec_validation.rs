// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

mod support;

use std::fs;
use std::path::Path;

use lexongraph_spherical_kmeans::{
    SPHERICAL_KMEANS_SOFTWARE_IDENTITY, SphericalKmeansStreamingTrainer,
};
use lexongraph_streaming_clustering::{
    MetricDirection, StreamingClusterClassifier, StreamingClusterTrainer,
    StreamingClusteringConfig, StreamingClusteringError, TrainerState,
};
use support::{
    config, conforming_trainer, expected_assignments, expected_pass_reports, invalid_params,
    nan_embedding, params, sample_passes, underfull_first_pass, unsupported_balance_config,
    wrong_dimension_embedding, zero_norm_embedding,
};

#[test]
fn val_sphkm_001_and_002_repository_and_public_surface_exist() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(manifest_dir.join("src/lib.rs").exists());
    assert!(manifest_dir.join("tests/spec_validation.rs").exists());
    assert!(
        manifest_dir
            .join("..")
            .join("..")
            .join("docs")
            .join("specs")
            .join("rust-spherical-kmeans-crate")
            .join("requirements.md")
            .exists()
    );
    assert!(
        manifest_dir
            .join("..")
            .join("..")
            .join("docs")
            .join("specs")
            .join("rust-linear-algebra-acceleration-crate")
            .join("requirements.md")
            .exists()
    );

    let trainer = conforming_trainer();
    assert_eq!(trainer.state(), TrainerState::Idle);
    assert!(SPHERICAL_KMEANS_SOFTWARE_IDENTITY.starts_with("lexongraph-spherical-kmeans-v"));
}

#[test]
fn val_sphkm_003_trainer_construction_preserves_config_and_params() {
    let trainer = SphericalKmeansStreamingTrainer::new(config(), params()).unwrap();
    assert_eq!(trainer.config(), &config());
}

#[test]
fn val_sphkm_004_and_010_params_and_balance_constraints_are_validated() {
    assert!(matches!(
        SphericalKmeansStreamingTrainer::new(config(), invalid_params()),
        Err(StreamingClusteringError::InvalidConfiguration { .. })
    ));
    assert!(matches!(
        SphericalKmeansStreamingTrainer::new(unsupported_balance_config(), params()),
        Err(StreamingClusteringError::InvalidConfiguration { .. })
    ));
}

#[test]
fn val_sphkm_005_006_008_and_009_pass_reports_are_deterministic() {
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
fn val_sphkm_007_and_013_malformed_input_and_invalid_transitions_are_explicit() {
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
    assert!(matches!(
        trainer.ingest_batch(&[zero_norm_embedding()]),
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
fn val_sphkm_008_cross_pass_continuity_is_enforced() {
    let mut trainer = conforming_trainer();
    for batch in &sample_passes()[0] {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }
    trainer.finish_pass().unwrap();
    trainer.ingest_batch(&[vec![1.0, 0.0]]).unwrap();
    trainer.ingest_batch(&[vec![0.0, 1.0]]).unwrap();

    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));
}

#[test]
fn val_sphkm_005_first_pass_rejects_underfull_input() {
    let mut trainer = conforming_trainer();
    for batch in underfull_first_pass() {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }
    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::UnsatisfiableConstraint { .. })
    ));
}

#[test]
fn regression_zero_norm_recomputed_centroid_is_unsatisfiable_not_malformed() {
    let mut trainer = SphericalKmeansStreamingTrainer::new(
        StreamingClusteringConfig {
            cluster_count: 1,
            ..config()
        },
        params(),
    )
    .unwrap();
    trainer
        .ingest_batch(&[vec![1.0, 0.0], vec![-1.0, 0.0]])
        .unwrap();

    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::UnsatisfiableConstraint { .. })
    ));
}

#[test]
fn val_sphkm_005_and_006_normalized_space_behavior_is_present_in_source() {
    let source =
        fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs")).unwrap();
    assert!(source.contains("normalize_pass("));
    assert!(source.contains("normalize_embedding("));
    assert!(source.contains("cosine_distance("));
    assert!(source.contains("SeededDeterministicFarthestPoint"));
    assert!(source.contains("chunked_dense_distance_matrix("));
    assert!(source.contains("detected_execution_backend_selection("));
}

#[test]
fn val_sphkm_011_classifier_assigns_deterministically() {
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
    assert!(matches!(
        classifier.assign(zero_norm_embedding().as_slice()),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));
}

#[test]
fn val_sphkm_014_shared_conformance_helpers_pass() {
    let harness = support::Harness;
    lexongraph_streaming_clustering::conformance::run_streaming_clustering_suite(&harness).unwrap();
}

#[test]
fn val_sphkm_012_014_015_016_and_017_acceleration_artifacts_exist() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(
        manifest_dir
            .join("tests/acceleration_validation.rs")
            .exists()
    );
    assert!(
        manifest_dir
            .join("src/bin/acceleration_benchmark.rs")
            .exists()
    );

    let manifest = fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
    assert!(manifest.contains("wgpu-accel"));
    assert!(manifest.contains("lexongraph-linear-algebra-acceleration"));
}
