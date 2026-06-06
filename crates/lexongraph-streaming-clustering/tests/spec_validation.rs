// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

mod support;

use std::path::Path;

use lexongraph_streaming_clustering::{
    BalanceConstraints, MetricDirection, StreamingClusterClassifier, StreamingClusterTrainer,
    StreamingClusteringConfig, StreamingClusteringError, TrainerState, validate_config,
};
use support::{
    FixtureTrainer, expected_assignments, expected_pass_reports, nan_embedding, sample_passes,
    underfull_first_pass, wrong_dimension_embedding,
};

#[test]
fn val_stream_trait_001_and_013_public_surface_exposes_the_shared_contract_only() {
    let config = StreamingClusteringConfig {
        cluster_count: 2,
        dimensions: 2,
        balance_constraints: None,
        random_seed: None,
    };
    validate_config(&config).unwrap();

    let source =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs")).unwrap();
    assert!(source.contains("pub trait StreamingClusterTrainer"));
    assert!(source.contains("pub trait StreamingClusterClassifier"));
    assert!(!source.contains("pub fn run_reference_streaming_clusterer"));
}

#[test]
fn val_stream_trait_002_caller_controls_finish_pass_and_training_completion() {
    let mut trainer = FixtureTrainer::default_deterministic();
    assert_eq!(trainer.state(), TrainerState::Idle);

    for batch in &sample_passes()[0] {
        trainer.ingest_batch(batch).unwrap();
        assert_eq!(trainer.state(), TrainerState::Ingesting);
    }
    let report = trainer.finish_pass().unwrap();
    assert_eq!(report, expected_pass_reports()[0]);
    assert_eq!(trainer.state(), TrainerState::PassComplete);

    trainer.complete_training().unwrap();
    assert_eq!(trainer.state(), TrainerState::TrainingComplete);
}

#[test]
fn val_stream_trait_003_first_pass_rejects_k_greater_than_n() {
    let mut trainer = FixtureTrainer::default_deterministic();
    for batch in underfull_first_pass() {
        trainer.ingest_batch(&batch).unwrap();
    }

    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::UnsatisfiableConstraint { .. })
    ));
    assert_eq!(trainer.state(), TrainerState::Error);
}

#[test]
fn val_stream_trait_004_pass_reports_expose_deterministic_metrics() {
    let mut trainer = FixtureTrainer::default_deterministic();
    for batch in &sample_passes()[0] {
        trainer.ingest_batch(batch).unwrap();
    }

    let report = trainer.finish_pass().unwrap();
    assert_eq!(report.quality_metric, 10.0);
    assert_eq!(report.balance_metric, 0.0);
    assert_eq!(report.quality_direction, MetricDirection::SmallerIsBetter);
    assert_eq!(report.balance_direction, MetricDirection::SmallerIsBetter);
}

#[test]
fn val_stream_trait_005_classifier_assigns_valid_embeddings_to_cluster_ids() {
    let mut trainer = FixtureTrainer::default_deterministic();
    for pass in sample_passes() {
        for batch in pass {
            trainer.ingest_batch(&batch).unwrap();
        }
        trainer.finish_pass().unwrap();
    }
    trainer.complete_training().unwrap();
    let classifier = trainer.into_classifier().unwrap();

    for (embedding, expected_cluster_id) in expected_assignments() {
        assert_eq!(classifier.assign(&embedding).unwrap(), expected_cluster_id);
    }
}

#[test]
fn val_stream_trait_006_invalid_transitions_and_malformed_inputs_fail_explicitly() {
    let mut trainer = FixtureTrainer::default_deterministic();
    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::InvalidTransition { .. })
    ));
    assert_eq!(trainer.state(), TrainerState::Error);

    let mut trainer = FixtureTrainer::default_deterministic();
    for pass in sample_passes() {
        for batch in pass {
            trainer.ingest_batch(&batch).unwrap();
        }
        trainer.finish_pass().unwrap();
    }
    trainer.complete_training().unwrap();
    let classifier = trainer.into_classifier().unwrap();

    assert!(matches!(
        classifier.assign(&wrong_dimension_embedding()),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));
    assert!(matches!(
        classifier.assign(&nan_embedding()),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));
}

#[test]
fn val_stream_trait_007_public_api_avoids_full_dataset_materialization() {
    let source =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs")).unwrap();
    assert!(source.contains("fn ingest_batch"));
    assert!(source.contains("fn assign_batch"));
    assert!(!source.contains("fn fit_dataset"));
}

#[test]
fn val_stream_trait_008_seeded_and_default_runs_are_deterministic() {
    let default_first = run_reports_and_assignments(FixtureTrainer::default_deterministic());
    let default_second = run_reports_and_assignments(FixtureTrainer::default_deterministic());
    assert_eq!(default_first, default_second);

    let seeded_first = run_reports_and_assignments(FixtureTrainer::explicit_seed(7));
    let seeded_second = run_reports_and_assignments(FixtureTrainer::explicit_seed(7));
    assert_eq!(seeded_first, seeded_second);
}

#[test]
fn val_stream_trait_009_conformance_helpers_are_feature_gated() {
    let manifest =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")).unwrap();
    assert!(manifest.contains("[features]"));
    assert!(manifest.contains("conformance = []"));
}

#[test]
fn val_stream_trait_010_repository_includes_spec_and_test_artifacts() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(manifest_dir.join("tests/spec_validation.rs").exists());
    assert!(manifest_dir.join("tests/conformance_feature.rs").exists());
    assert!(
        manifest_dir
            .join("..")
            .join("..")
            .join("docs")
            .join("specs")
            .join("rust-streaming-clustering-crate")
            .join("requirements.md")
            .exists()
    );
}

#[test]
fn val_stream_trait_014_classifier_contract_does_not_standardize_serialization() {
    let source =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs")).unwrap();
    assert!(!source.contains("fn serialize_classifier"));
}

#[test]
fn val_stream_trait_015_invalid_base_configuration_is_rejected_explicitly() {
    let zero_cluster_count = StreamingClusteringConfig {
        cluster_count: 0,
        dimensions: 2,
        balance_constraints: None,
        random_seed: None,
    };
    assert!(matches!(
        validate_config(&zero_cluster_count),
        Err(StreamingClusteringError::InvalidConfiguration { .. })
    ));

    let zero_dimensions = StreamingClusteringConfig {
        cluster_count: 2,
        dimensions: 0,
        balance_constraints: None,
        random_seed: None,
    };
    assert!(matches!(
        validate_config(&zero_dimensions),
        Err(StreamingClusteringError::InvalidConfiguration { .. })
    ));
}

#[test]
fn val_stream_trait_016_invalid_balance_constraints_are_rejected_explicitly() {
    let invalid_configs = vec![
        StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: Some(BalanceConstraints {
                min_cluster_occupancy: Some(0),
                max_cluster_occupancy: None,
                max_cluster_size_ratio: None,
                soft_balance_penalty: None,
            }),
            random_seed: None,
        },
        StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: Some(BalanceConstraints {
                min_cluster_occupancy: None,
                max_cluster_occupancy: Some(0),
                max_cluster_size_ratio: None,
                soft_balance_penalty: None,
            }),
            random_seed: None,
        },
        StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: Some(BalanceConstraints {
                min_cluster_occupancy: Some(3),
                max_cluster_occupancy: Some(2),
                max_cluster_size_ratio: None,
                soft_balance_penalty: None,
            }),
            random_seed: None,
        },
        StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: Some(BalanceConstraints {
                min_cluster_occupancy: None,
                max_cluster_occupancy: None,
                max_cluster_size_ratio: Some(0.0),
                soft_balance_penalty: None,
            }),
            random_seed: None,
        },
        StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: Some(BalanceConstraints {
                min_cluster_occupancy: None,
                max_cluster_occupancy: None,
                max_cluster_size_ratio: Some(-1.0),
                soft_balance_penalty: None,
            }),
            random_seed: None,
        },
        StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: Some(BalanceConstraints {
                min_cluster_occupancy: None,
                max_cluster_occupancy: None,
                max_cluster_size_ratio: Some(f64::NAN),
                soft_balance_penalty: None,
            }),
            random_seed: None,
        },
        StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: Some(BalanceConstraints {
                min_cluster_occupancy: None,
                max_cluster_occupancy: None,
                max_cluster_size_ratio: Some(f64::INFINITY),
                soft_balance_penalty: None,
            }),
            random_seed: None,
        },
        StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: Some(BalanceConstraints {
                min_cluster_occupancy: None,
                max_cluster_occupancy: None,
                max_cluster_size_ratio: None,
                soft_balance_penalty: Some(-1.0),
            }),
            random_seed: None,
        },
        StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: Some(BalanceConstraints {
                min_cluster_occupancy: None,
                max_cluster_occupancy: None,
                max_cluster_size_ratio: None,
                soft_balance_penalty: Some(f64::NAN),
            }),
            random_seed: None,
        },
        StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: Some(BalanceConstraints {
                min_cluster_occupancy: None,
                max_cluster_occupancy: None,
                max_cluster_size_ratio: None,
                soft_balance_penalty: Some(f64::INFINITY),
            }),
            random_seed: None,
        },
    ];

    for config in invalid_configs {
        assert!(matches!(
            validate_config(&config),
            Err(StreamingClusteringError::InvalidConfiguration { .. })
        ));
    }
}

fn run_reports_and_assignments(
    mut trainer: FixtureTrainer,
) -> (Vec<lexongraph_streaming_clustering::PassReport>, Vec<u32>) {
    let mut reports = Vec::new();
    for pass in sample_passes() {
        for batch in pass {
            trainer.ingest_batch(&batch).unwrap();
        }
        reports.push(trainer.finish_pass().unwrap());
    }
    trainer.complete_training().unwrap();
    let classifier = trainer.into_classifier().unwrap();
    let assignments = expected_assignments()
        .into_iter()
        .map(|(embedding, _)| classifier.assign(&embedding).unwrap())
        .collect();
    (reports, assignments)
}
