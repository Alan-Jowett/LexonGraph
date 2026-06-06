// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

#![allow(dead_code)]

use lexongraph_dcbc_streaming::{DcbcStreamingClassifier, DcbcStreamingTrainer};
use lexongraph_streaming_clustering::{
    ClusterId, PassInput, PassReport, StreamingClusterTrainer, StreamingClusteringConfig,
    StreamingClusteringError, TrainerState,
};

pub fn config() -> StreamingClusteringConfig {
    StreamingClusteringConfig {
        cluster_count: 2,
        dimensions: 2,
        balance_constraints: None,
        random_seed: None,
    }
}

pub fn supported_balance_config() -> StreamingClusteringConfig {
    StreamingClusteringConfig {
        cluster_count: 2,
        dimensions: 2,
        balance_constraints: Some(lexongraph_streaming_clustering::BalanceConstraints {
            min_cluster_occupancy: Some(1),
            max_cluster_occupancy: Some(1),
            max_cluster_size_ratio: None,
            soft_balance_penalty: None,
        }),
        random_seed: None,
    }
}

pub fn infeasible_occupancy_config() -> StreamingClusteringConfig {
    StreamingClusteringConfig {
        cluster_count: 2,
        dimensions: 2,
        balance_constraints: Some(lexongraph_streaming_clustering::BalanceConstraints {
            min_cluster_occupancy: Some(2),
            max_cluster_occupancy: Some(2),
            max_cluster_size_ratio: None,
            soft_balance_penalty: None,
        }),
        random_seed: None,
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

pub fn expected_assignments() -> Vec<(Vec<f32>, ClusterId)> {
    vec![(vec![1.0, 0.0], 0), (vec![-1.0, 0.0], 1)]
}

pub fn underfull_first_pass() -> PassInput {
    vec![vec![vec![1.0, 0.0]]]
}

pub fn wrong_dimension_embedding() -> Vec<f32> {
    vec![1.0]
}

pub fn nan_embedding() -> Vec<f32> {
    vec![f32::NAN, 0.0]
}

pub fn zero_norm_embedding() -> Vec<f32> {
    vec![0.0, 0.0]
}

pub struct Harness;

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

    fn sample_passes(&self) -> Vec<PassInput> {
        sample_passes()
    }

    fn expected_pass_reports(&self) -> Vec<PassReport> {
        expected_pass_reports()
    }

    fn expected_assignments(&self) -> Vec<(Vec<f32>, ClusterId)> {
        expected_assignments()
    }

    fn underfull_first_pass(&self) -> PassInput {
        underfull_first_pass()
    }

    fn wrong_dimension_embedding(&self) -> Vec<f32> {
        wrong_dimension_embedding()
    }

    fn nan_embedding(&self) -> Vec<f32> {
        nan_embedding()
    }
}

pub struct HarnessTrainer {
    inner: DcbcStreamingTrainer,
    mode: HarnessMode,
    completed_passes: usize,
}

#[derive(Clone, Copy)]
enum HarnessMode {
    Conforming,
    UnstableClusterIds,
}

impl HarnessTrainer {
    fn new(mode: HarnessMode) -> Self {
        Self {
            inner: DcbcStreamingTrainer::new(config()).unwrap(),
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
}

impl StreamingClusterTrainer for HarnessTrainer {
    type Classifier = DcbcStreamingClassifier;

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
        self.inner.into_classifier()
    }
}
