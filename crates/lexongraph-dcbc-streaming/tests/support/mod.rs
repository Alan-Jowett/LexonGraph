// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

#![allow(dead_code)]

use lexongraph_dcbc_streaming::{DcbcStreamingClassifier, DcbcStreamingTrainer};
use lexongraph_streaming_clustering::conformance::SamplePassEvent;
use lexongraph_streaming_clustering::{
    ClusterId, PassReadiness, PassReport, StreamingClusterTrainer, StreamingClusteringConfig,
    StreamingClusteringError, TrainerState,
};

type SamplePass = Vec<Vec<Vec<f32>>>;

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

pub fn sample_passes() -> Vec<SamplePass> {
    vec![
        vec![vec![vec![1.0, 0.0]], vec![vec![-1.0, 0.0]]],
        vec![vec![vec![1.0, 0.0]], vec![vec![-1.0, 0.0]]],
    ]
}

pub fn expected_pass_reports() -> Vec<PassReport> {
    vec![
        PassReport {
            observed_count: 2,
            requested_cluster_count: 2,
            readiness: PassReadiness::PartitionReady,
            realized_cluster_count: Some(2),
            quality_metric: 0.0,
            balance_metric: 0.0,
            quality_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            balance_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            cluster_ids: Some(vec![0, 1]),
        },
        PassReport {
            observed_count: 2,
            requested_cluster_count: 2,
            readiness: PassReadiness::PartitionReady,
            realized_cluster_count: Some(2),
            quality_metric: 0.0,
            balance_metric: 0.0,
            quality_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            balance_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            cluster_ids: Some(vec![0, 1]),
        },
    ]
}

pub fn expected_assignments() -> Vec<(Vec<f32>, ClusterId)> {
    vec![(vec![1.0, 0.0], 0), (vec![-1.0, 0.0], 1)]
}

pub fn underfull_first_pass() -> SamplePass {
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

    fn malformed_input_accepting_trainer(&self) -> Self::Trainer {
        HarnessTrainer::malformed_input_accepting()
    }

    fn for_each_sample_pass_event<E, F>(&self, mut on_event: F) -> Result<(), E>
    where
        F: FnMut(SamplePassEvent<'_>) -> Result<(), E>,
    {
        for pass in sample_passes() {
            for batch in pass {
                on_event(SamplePassEvent::Batch(batch.as_slice()))?;
            }
            on_event(SamplePassEvent::EndPass)?;
        }
        Ok(())
    }

    fn for_each_expected_pass_report<E, F>(&self, mut on_report: F) -> Result<(), E>
    where
        F: FnMut(&PassReport) -> Result<(), E>,
    {
        for report in expected_pass_reports() {
            on_report(&report)?;
        }
        Ok(())
    }

    fn for_each_expected_assignment<E, F>(&self, mut on_assignment: F) -> Result<(), E>
    where
        F: FnMut(&[f32], ClusterId) -> Result<(), E>,
    {
        for (embedding, cluster_id) in expected_assignments() {
            on_assignment(embedding.as_slice(), cluster_id)?;
        }
        Ok(())
    }

    fn for_each_underfull_first_pass_batch<E, F>(&self, mut on_batch: F) -> Result<(), E>
    where
        F: FnMut(&[Vec<f32>]) -> Result<(), E>,
    {
        for batch in underfull_first_pass() {
            on_batch(batch.as_slice())?;
        }
        Ok(())
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
    MalformedInputAccepting,
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

    fn ingest_batch(&mut self, embeddings: &[Vec<f32>]) -> Result<(), StreamingClusteringError> {
        self.inner.ingest_batch(embeddings)
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        let mut report = self.inner.finish_pass()?;
        if matches!(self.mode, HarnessMode::UnstableClusterIds)
            && self.completed_passes == 1
            && let Some(cluster_ids) = report.cluster_ids.as_mut()
        {
            cluster_ids.reverse();
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
    inner: DcbcStreamingClassifier,
    mode: HarnessMode,
}

impl lexongraph_streaming_clustering::StreamingClusterClassifier for HarnessClassifier {
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
