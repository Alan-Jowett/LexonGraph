// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

#![allow(dead_code)]

use lexongraph_spherical_kmeans::{
    SphericalInitializationPolicy, SphericalKmeansParams, SphericalKmeansStreamingClassifier,
    SphericalKmeansStreamingTrainer,
};
use lexongraph_streaming_clustering::{
    BalanceConstraints, ClusterId, Embedding, PassInput, PassReport, StreamingClusterClassifier,
    StreamingClusterTrainer, StreamingClusteringConfig, StreamingClusteringError, TrainerState,
};

pub fn config() -> StreamingClusteringConfig {
    StreamingClusteringConfig {
        cluster_count: 2,
        dimensions: 2,
        balance_constraints: None,
        random_seed: Some(7),
    }
}

pub fn params() -> SphericalKmeansParams {
    SphericalKmeansParams {
        initialization_policy: SphericalInitializationPolicy::SeededDeterministicFarthestPoint,
        max_iteration_count: 8,
        convergence_tolerance: 0.0,
    }
}

pub fn unsupported_balance_config() -> StreamingClusteringConfig {
    StreamingClusteringConfig {
        balance_constraints: Some(BalanceConstraints {
            min_cluster_occupancy: Some(1),
            max_cluster_occupancy: None,
            max_cluster_size_ratio: None,
            soft_balance_penalty: None,
        }),
        ..config()
    }
}

pub fn invalid_params() -> SphericalKmeansParams {
    SphericalKmeansParams {
        max_iteration_count: 0,
        ..params()
    }
}

pub fn sample_passes() -> Vec<PassInput> {
    vec![
        vec![vec![vec![1.0, 0.0]], vec![vec![-1.0, 0.0]]],
        vec![vec![vec![1.0, 0.0]], vec![vec![-1.0, 0.0]]],
    ]
}

pub fn expected_pass_reports() -> Vec<PassReport> {
    vec![
        PassReport {
            observed_count: 2,
            quality_metric: 0.0,
            balance_metric: 0.0,
            quality_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            balance_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0, 1],
        },
        PassReport {
            observed_count: 2,
            quality_metric: 0.0,
            balance_metric: 0.0,
            quality_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            balance_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0, 1],
        },
    ]
}

pub fn expected_assignments() -> Vec<(Embedding, ClusterId)> {
    vec![(vec![1.0, 0.0], 0), (vec![-1.0, 0.0], 1)]
}

pub fn underfull_first_pass() -> PassInput {
    vec![vec![vec![1.0, 0.0]]]
}

pub fn wrong_dimension_embedding() -> Embedding {
    vec![1.0]
}

pub fn nan_embedding() -> Embedding {
    vec![f32::NAN, 0.0]
}

pub fn zero_norm_embedding() -> Embedding {
    vec![0.0, 0.0]
}

pub fn conforming_trainer() -> SphericalKmeansStreamingTrainer {
    SphericalKmeansStreamingTrainer::new(config(), params()).unwrap()
}

pub struct Harness;

pub struct HarnessTrainer {
    inner: SphericalKmeansStreamingTrainer,
    mode: HarnessMode,
    completed_passes: usize,
}

#[derive(Clone, Copy)]
enum HarnessMode {
    Conforming,
    UnstableClusterIds,
    MalformedInputAccepting,
}

impl HarnessTrainer {
    fn new(mode: HarnessMode) -> Self {
        Self {
            inner: conforming_trainer(),
            mode,
            completed_passes: 0,
        }
    }

    fn conforming() -> Self {
        Self::new(HarnessMode::Conforming)
    }

    fn unstable_cluster_ids() -> Self {
        Self::new(HarnessMode::UnstableClusterIds)
    }

    fn malformed_input_accepting() -> Self {
        Self::new(HarnessMode::MalformedInputAccepting)
    }
}

impl StreamingClusterTrainer for HarnessTrainer {
    type Classifier = HarnessClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        self.inner.config()
    }

    fn state(&self) -> TrainerState {
        self.inner.state()
    }

    fn ingest_batch(&mut self, embeddings: &[Embedding]) -> Result<(), StreamingClusteringError> {
        self.inner.ingest_batch(embeddings)
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        let mut report = self.inner.finish_pass()?;
        if matches!(self.mode, HarnessMode::UnstableClusterIds) && self.completed_passes == 1 {
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

pub struct HarnessClassifier {
    inner: SphericalKmeansStreamingClassifier,
    mode: HarnessMode,
}

impl StreamingClusterClassifier for HarnessClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        self.inner.config()
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        if matches!(self.mode, HarnessMode::MalformedInputAccepting) {
            let first = embedding.first().copied().unwrap_or(0.0);
            return Ok(if first >= 0.0 { 0 } else { 1 });
        }
        self.inner.assign(embedding)
    }
}

impl lexongraph_streaming_clustering::conformance::StreamingClusteringConformanceHarness
    for Harness
{
    type Trainer = HarnessTrainer;

    fn conforming_trainer(&self) -> Self::Trainer {
        HarnessTrainer::conforming()
    }

    fn unstable_cluster_ids_trainer(&self) -> Self::Trainer {
        HarnessTrainer::unstable_cluster_ids()
    }

    fn malformed_input_accepting_trainer(&self) -> Self::Trainer {
        HarnessTrainer::malformed_input_accepting()
    }

    fn sample_passes(&self) -> Vec<PassInput> {
        sample_passes()
    }

    fn expected_pass_reports(&self) -> Vec<PassReport> {
        expected_pass_reports()
    }

    fn expected_assignments(&self) -> Vec<(Embedding, ClusterId)> {
        expected_assignments()
    }

    fn underfull_first_pass(&self) -> PassInput {
        underfull_first_pass()
    }

    fn wrong_dimension_embedding(&self) -> Embedding {
        wrong_dimension_embedding()
    }

    fn nan_embedding(&self) -> Embedding {
        nan_embedding()
    }
}
