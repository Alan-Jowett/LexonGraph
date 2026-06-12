// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::fs;
use std::path::Path;

use lexongraph_pca_chunking::{
    PCA_CHUNKING_SOFTWARE_IDENTITY, PcaChunkingParams, PcaChunkingStreamingTrainer,
};
use lexongraph_streaming_clustering::{
    MetricDirection, PassReport, StreamingClusterClassifier, StreamingClusterTrainer,
    StreamingClusteringConfig, StreamingClusteringError, TrainerState,
};

fn config() -> StreamingClusteringConfig {
    StreamingClusteringConfig {
        cluster_count: 2,
        dimensions: 2,
        balance_constraints: None,
        random_seed: Some(7),
    }
}

fn params() -> PcaChunkingParams {
    PcaChunkingParams {
        retained_dimension_count: 1,
        variance_exponent: 1.0,
    }
}

fn sample_passes() -> Vec<Vec<Vec<Vec<f32>>>> {
    vec![
        vec![
            vec![vec![0.0, 0.0], vec![0.25, 0.0]],
            vec![vec![10.0, 0.0], vec![10.25, 0.0]],
        ],
        vec![
            vec![vec![0.0, 0.0], vec![0.25, 0.0]],
            vec![vec![10.0, 0.0], vec![10.25, 0.0]],
        ],
    ]
}

fn expected_pass_reports() -> Vec<PassReport> {
    vec![
        PassReport {
            observed_count: 4,
            quality_metric: 0.0625,
            balance_metric: 0.0,
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0, 1],
        },
        PassReport {
            observed_count: 4,
            quality_metric: 0.0625,
            balance_metric: 0.0,
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0, 1],
        },
    ]
}

fn expected_assignments() -> Vec<(Vec<f32>, u32)> {
    vec![
        (vec![0.0, 0.0], 0),
        (vec![0.25, 0.0], 0),
        (vec![10.0, 0.0], 1),
        (vec![10.25, 0.0], 1),
    ]
}

fn wrong_dimension_embedding() -> Vec<f32> {
    vec![0.0]
}

fn nan_embedding() -> Vec<f32> {
    vec![f32::NAN, 0.0]
}

#[derive(Clone, Copy)]
enum HarnessMode {
    Conforming,
    UnstableClusterIds,
    MalformedInputAccepting,
}

struct HarnessTrainer {
    inner: PcaChunkingStreamingTrainer,
    mode: HarnessMode,
    completed_passes: usize,
}

impl StreamingClusterTrainer for HarnessTrainer {
    type Classifier = HarnessClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        self.inner.config()
    }

    fn state(&self) -> TrainerState {
        self.inner.state()
    }

    fn ingest_batch(&mut self, embeddings: &[Vec<f32>]) -> Result<(), StreamingClusteringError> {
        self.inner.ingest_batch(embeddings)
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        let mut report = self.inner.finish_pass()?;
        if matches!(self.mode, HarnessMode::UnstableClusterIds) && self.completed_passes % 2 == 1 {
            report.cluster_ids.reverse();
        }
        self.completed_passes += 1;
        Ok(report)
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        self.inner.complete_training()
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        Ok(HarnessClassifier {
            inner: self.inner.into_classifier()?,
            mode: self.mode,
        })
    }
}

struct HarnessClassifier {
    inner: lexongraph_pca_chunking::PcaChunkingStreamingClassifier,
    mode: HarnessMode,
}

impl StreamingClusterClassifier for HarnessClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        self.inner.config()
    }

    fn assign(&self, embedding: &[f32]) -> Result<u32, StreamingClusteringError> {
        match self.mode {
            HarnessMode::MalformedInputAccepting if embedding.len() != self.config().dimensions => {
                Ok(0)
            }
            HarnessMode::MalformedInputAccepting
                if embedding.iter().any(|value| !value.is_finite()) =>
            {
                Ok(0)
            }
            _ => self.inner.assign(embedding),
        }
    }
}

#[test]
fn val_pca_chunk_001_and_002_repository_and_public_surface_exist() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(manifest_dir.join("src/lib.rs").exists());
    assert!(manifest_dir.join("tests/spec_validation.rs").exists());
    assert!(
        manifest_dir
            .join("..")
            .join("..")
            .join("docs")
            .join("specs")
            .join("rust-pca-chunking-crate")
            .join("requirements.md")
            .exists()
    );

    let trainer = PcaChunkingStreamingTrainer::new(config(), params()).unwrap();
    assert_eq!(trainer.state(), TrainerState::Idle);
    assert!(PCA_CHUNKING_SOFTWARE_IDENTITY.contains("lexongraph-pca-chunking-v"));
}

#[test]
fn val_pca_chunk_003_trainer_construction_preserves_config_and_params() {
    let trainer = PcaChunkingStreamingTrainer::new(config(), params()).unwrap();
    assert_eq!(trainer.config(), &config());
}

#[test]
fn val_pca_chunk_004_and_010_pass_reports_are_deterministic() {
    let mut trainer = PcaChunkingStreamingTrainer::new(config(), params()).unwrap();
    let mut reports = Vec::new();
    for pass in sample_passes() {
        for batch in pass {
            trainer.ingest_batch(batch.as_slice()).unwrap();
        }
        reports.push(trainer.finish_pass().unwrap());
    }
    assert_eq!(reports, expected_pass_reports());
}

#[test]
fn val_pca_chunk_005_cross_pass_continuity_is_enforced() {
    let mut trainer = PcaChunkingStreamingTrainer::new(config(), params()).unwrap();
    for batch in &sample_passes()[0] {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }
    trainer.finish_pass().unwrap();
    trainer
        .ingest_batch(&[vec![0.0, 0.0], vec![0.25, 0.0]])
        .unwrap();
    trainer
        .ingest_batch(&[vec![10.0, 0.0], vec![10.5, 0.0]])
        .unwrap();

    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::MalformedInput { .. })
    ));
}

#[test]
fn val_pca_chunk_006_execution_path_uses_pca_and_contiguous_sort_chunking() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("lib.rs"),
    )
    .unwrap();
    assert!(source.contains("use lexongraph_pca"));
    assert!(source.contains("fit("));
    assert!(source.contains("compare_sort_key_parts"));
    assert!(source.contains("partition_point"));
}

#[test]
fn val_pca_chunk_007_equal_chunk_sizes_are_realized() {
    let mut trainer = PcaChunkingStreamingTrainer::new(config(), params()).unwrap();
    for batch in sample_passes().into_iter().next().unwrap() {
        trainer.ingest_batch(batch.as_slice()).unwrap();
    }
    trainer.finish_pass().unwrap();
    trainer.complete_training().unwrap();
    let classifier = trainer.into_classifier().unwrap();
    let assignments = expected_assignments()
        .into_iter()
        .map(|(embedding, _)| classifier.assign(embedding.as_slice()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(assignments, vec![0, 0, 1, 1]);
}

#[test]
fn val_pca_chunk_008_remainder_is_assigned_to_earliest_chunks() {
    let config = StreamingClusteringConfig {
        cluster_count: 2,
        dimensions: 2,
        balance_constraints: None,
        random_seed: Some(7),
    };
    let mut trainer = PcaChunkingStreamingTrainer::new(config, params()).unwrap();
    trainer
        .ingest_batch(&[
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![2.0, 0.0],
            vec![10.0, 0.0],
            vec![11.0, 0.0],
        ])
        .unwrap();
    trainer.finish_pass().unwrap();
    trainer.complete_training().unwrap();
    let classifier = trainer.into_classifier().unwrap();
    let assignments = [0.0, 1.0, 2.0, 10.0, 11.0]
        .into_iter()
        .map(|value| classifier.assign(&[value, 0.0]).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(assignments, vec![0, 0, 0, 1, 1]);
}

#[test]
fn val_pca_chunk_009_duplicate_heavy_inputs_remain_deterministic() {
    let mut run_a = PcaChunkingStreamingTrainer::new(config(), params()).unwrap();
    let mut run_b = PcaChunkingStreamingTrainer::new(config(), params()).unwrap();
    let duplicate_batch = vec![
        vec![1.0, 1.0],
        vec![1.0, 1.05],
        vec![1.05, 1.0],
        vec![1.05, 1.05],
    ];
    run_a.ingest_batch(duplicate_batch.as_slice()).unwrap();
    run_b.ingest_batch(duplicate_batch.as_slice()).unwrap();
    let report_a = run_a.finish_pass().unwrap();
    let report_b = run_b.finish_pass().unwrap();
    assert_eq!(report_a, report_b);
}

#[test]
fn regression_exact_identical_embeddings_that_cross_a_boundary_fail_explicitly() {
    let mut trainer = PcaChunkingStreamingTrainer::new(config(), params()).unwrap();
    trainer
        .ingest_batch(&[
            vec![1.0, 1.0],
            vec![1.0, 1.0],
            vec![1.0, 1.0],
            vec![1.0, 1.0],
        ])
        .unwrap();

    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::UnsatisfiableConstraint { message })
            if message.contains("identical classifier sort keys")
    ));
}

#[test]
fn val_pca_chunk_011_classifier_assigns_and_rejects_malformed_input() {
    let mut trainer = PcaChunkingStreamingTrainer::new(config(), params()).unwrap();
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
fn val_pca_chunk_012_invalid_configuration_and_transitions_are_explicit() {
    let mut unsupported_balance = config();
    unsupported_balance.balance_constraints =
        Some(lexongraph_streaming_clustering::BalanceConstraints {
            min_cluster_occupancy: Some(1),
            max_cluster_occupancy: None,
            max_cluster_size_ratio: None,
            soft_balance_penalty: None,
        });
    assert!(matches!(
        PcaChunkingStreamingTrainer::new(unsupported_balance, params()),
        Err(StreamingClusteringError::InvalidConfiguration { .. })
    ));

    let invalid_params = PcaChunkingParams {
        retained_dimension_count: 0,
        ..params()
    };
    assert!(matches!(
        PcaChunkingStreamingTrainer::new(config(), invalid_params),
        Err(StreamingClusteringError::InvalidConfiguration { .. })
    ));

    let mut trainer = PcaChunkingStreamingTrainer::new(config(), params()).unwrap();
    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::InvalidTransition { .. })
    ));
    assert_eq!(trainer.state(), TrainerState::Error);
}

struct Harness;

impl lexongraph_streaming_clustering::conformance::StreamingClusteringConformanceHarness
    for Harness
{
    type Trainer = HarnessTrainer;

    fn conforming_trainer(&self) -> Self::Trainer {
        HarnessTrainer {
            inner: PcaChunkingStreamingTrainer::new(config(), params()).unwrap(),
            mode: HarnessMode::Conforming,
            completed_passes: 0,
        }
    }

    fn unstable_cluster_ids_trainer(&self) -> Self::Trainer {
        HarnessTrainer {
            inner: PcaChunkingStreamingTrainer::new(config(), params()).unwrap(),
            mode: HarnessMode::UnstableClusterIds,
            completed_passes: 0,
        }
    }

    fn malformed_input_accepting_trainer(&self) -> Self::Trainer {
        HarnessTrainer {
            inner: PcaChunkingStreamingTrainer::new(config(), params()).unwrap(),
            mode: HarnessMode::MalformedInputAccepting,
            completed_passes: 0,
        }
    }

    fn sample_passes(&self) -> Vec<lexongraph_streaming_clustering::PassInput> {
        sample_passes()
    }

    fn expected_pass_reports(&self) -> Vec<PassReport> {
        expected_pass_reports()
    }

    fn expected_assignments(&self) -> Vec<(Vec<f32>, u32)> {
        expected_assignments()
    }

    fn underfull_first_pass(&self) -> lexongraph_streaming_clustering::PassInput {
        vec![vec![vec![0.0, 0.0]]]
    }

    fn wrong_dimension_embedding(&self) -> Vec<f32> {
        wrong_dimension_embedding()
    }

    fn nan_embedding(&self) -> Vec<f32> {
        nan_embedding()
    }
}

#[test]
fn val_pca_chunk_013_shared_conformance_helpers_pass() {
    let harness = Harness;
    lexongraph_streaming_clustering::conformance::run_streaming_clustering_suite(&harness).unwrap();
}
