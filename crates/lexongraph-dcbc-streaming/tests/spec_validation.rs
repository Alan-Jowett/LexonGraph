// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

mod support;

use std::path::Path;

use lexongraph_dcbc_streaming::DcbcStreamingTrainer;
use lexongraph_streaming_clustering::{
    MetricDirection, StreamingClusterClassifier, StreamingClusterTrainer, StreamingClusteringError,
    TrainerState,
};
use support::{
    config, expected_assignments, expected_pass_reports, infeasible_occupancy_config,
    nan_embedding, sample_passes, supported_balance_config, underfull_first_pass,
    wrong_dimension_embedding, zero_norm_embedding,
};

#[test]
fn val_dcbc_stream_001_and_002_repository_and_public_surface_exist() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(manifest_dir.join("src/lib.rs").exists());
    assert!(manifest_dir.join("tests/spec_validation.rs").exists());
    assert!(
        manifest_dir
            .join("..")
            .join("..")
            .join("docs")
            .join("specs")
            .join("rust-dcbc-streaming-crate")
            .join("requirements.md")
            .exists()
    );

    let _trainer = DcbcStreamingTrainer::new(config()).unwrap();
}

#[test]
fn val_dcbc_stream_003_and_019_supported_occupancy_balance_constraints_are_accepted() {
    let config = supported_balance_config();
    let trainer = DcbcStreamingTrainer::new(config.clone()).unwrap();

    assert_eq!(trainer.config(), &config);
}

#[test]
fn val_dcbc_stream_004_and_012_pass_reports_expose_protocol_passes_directly() {
    let mut trainer = DcbcStreamingTrainer::new(config()).unwrap();
    let mut reports = Vec::new();
    for pass in sample_passes() {
        for batch in pass {
            trainer.ingest_batch(batch.as_slice()).unwrap();
        }
        reports.push(trainer.finish_pass().unwrap());
    }

    assert_eq!(reports, expected_pass_reports());
    assert_eq!(reports.len(), 2);
    assert_eq!(
        reports[0].quality_direction,
        MetricDirection::SmallerIsBetter
    );
    assert_eq!(
        reports[0].balance_direction,
        MetricDirection::SmallerIsBetter
    );
}

#[test]
fn val_dcbc_stream_005_and_006_first_pass_rejections_are_explicit() {
    let mut trainer = DcbcStreamingTrainer::new(config()).unwrap();
    for batch in underfull_first_pass() {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }
    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::UnsatisfiableConstraint { .. })
    ));

    let mut trainer = DcbcStreamingTrainer::new(config()).unwrap();
    let passes = sample_passes();
    for batch in &passes[0] {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }
    trainer.finish_pass().unwrap();
    trainer.ingest_batch(&[vec![1.0, 0.0]]).unwrap();
    trainer.ingest_batch(&[vec![0.0, 1.0]]).unwrap();
    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));

    let mut trainer = DcbcStreamingTrainer::new(config()).unwrap();
    for batch in &passes[0] {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }
    trainer.finish_pass().unwrap();
    trainer.ingest_batch(&[vec![1.0, 0.0]]).unwrap();
    trainer
        .ingest_batch(&[vec![-1.0, 0.0], vec![1.0, 0.0]])
        .unwrap();
    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));
}

#[test]
fn val_dcbc_stream_021_first_pass_rejects_infeasible_derived_occupancy_bounds() {
    let mut trainer = DcbcStreamingTrainer::new(infeasible_occupancy_config()).unwrap();
    trainer
        .ingest_batch(&[vec![1.0, 0.0], vec![-1.0, 0.0], vec![0.0, 1.0]])
        .unwrap();

    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::UnsatisfiableConstraint { .. })
    ));
}

#[test]
fn val_dcbc_stream_013_classifier_assigns_and_rejects_malformed_embeddings() {
    let mut trainer = DcbcStreamingTrainer::new(config()).unwrap();
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
fn val_dcbc_stream_022_trainer_rejects_zero_norm_embeddings() {
    let mut trainer = DcbcStreamingTrainer::new(config()).unwrap();

    assert!(matches!(
        trainer.ingest_batch(&[zero_norm_embedding()]),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));
}

#[test]
fn val_dcbc_stream_015_invalid_transitions_are_terminal() {
    let mut trainer = DcbcStreamingTrainer::new(config()).unwrap();
    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::InvalidTransition { .. })
    ));
    assert!(matches!(
        trainer.ingest_batch(&[vec![1.0, 0.0]]),
        Err(StreamingClusteringError::InvalidTransition { .. })
    ));

    let mut trainer = DcbcStreamingTrainer::new(config()).unwrap();
    assert!(matches!(
        trainer.complete_training(),
        Err(StreamingClusteringError::InvalidTransition { .. })
    ));
    assert_eq!(trainer.state(), TrainerState::Error);

    let mut trainer = DcbcStreamingTrainer::new(config()).unwrap();
    for batch in &sample_passes()[0] {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }
    trainer.finish_pass().unwrap();
    assert!(matches!(
        trainer.into_classifier(),
        Err(StreamingClusteringError::InvalidTransition {
            state: TrainerState::PassComplete,
            ..
        })
    ));
}
