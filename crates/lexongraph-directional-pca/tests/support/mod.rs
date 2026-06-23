// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use lexongraph_directional_pca::{
    DirectionalPcaAllocationPolicy, DirectionalPcaBinningPolicy,
    DirectionalPcaClusterCardinalityMode, DirectionalPcaParams, DirectionalPcaRetainedAxisPolicy,
    DirectionalPcaStreamingTrainer,
};
use lexongraph_streaming_clustering::{
    BalanceConstraints, ClusterId, Embedding, PassInput, PassReport, StreamingClusteringConfig,
};

pub fn config() -> StreamingClusteringConfig {
    StreamingClusteringConfig {
        cluster_count: 2,
        dimensions: 2,
        balance_constraints: None,
        random_seed: None,
    }
}

pub fn params() -> DirectionalPcaParams {
    DirectionalPcaParams {
        retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
        allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
        binning_policy: DirectionalPcaBinningPolicy::Quantile,
        cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
        variance_exponent: 1.0,
        temperature: 1.0,
        min_input_count: 2,
        min_effective_rank: 1,
        min_cumulative_variance: 0.0,
    }
}

pub fn exact_k_failure_config() -> StreamingClusteringConfig {
    StreamingClusteringConfig {
        cluster_count: 3,
        dimensions: 2,
        balance_constraints: None,
        random_seed: None,
    }
}

pub fn exact_k_failure_params() -> DirectionalPcaParams {
    DirectionalPcaParams {
        retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(2),
        allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
        binning_policy: DirectionalPcaBinningPolicy::Quantile,
        cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
        variance_exponent: 1.0,
        temperature: 1.0,
        min_input_count: 2,
        min_effective_rank: 1,
        min_cumulative_variance: 0.0,
    }
}

pub fn duplicate_refinement_config() -> StreamingClusteringConfig {
    StreamingClusteringConfig {
        cluster_count: 3,
        dimensions: 2,
        balance_constraints: None,
        random_seed: None,
    }
}

pub fn duplicate_refinement_params() -> DirectionalPcaParams {
    DirectionalPcaParams {
        retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(2),
        allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
        binning_policy: DirectionalPcaBinningPolicy::Quantile,
        cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
        variance_exponent: 1.0,
        temperature: 1.0,
        min_input_count: 2,
        min_effective_rank: 1,
        min_cumulative_variance: 0.0,
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

pub fn sample_passes() -> Vec<PassInput> {
    let pass = vec![
        vec![vec![0.0, 0.0], vec![1.0, 0.0]],
        vec![vec![10.0, 0.0], vec![11.0, 0.0]],
    ];
    vec![pass.clone(), pass]
}

pub fn expected_pass_reports() -> Vec<PassReport> {
    vec![
        PassReport {
            observed_count: 4,
            requested_cluster_count: 2,
            realized_cluster_count: 2,
            quality_metric: 1.0,
            balance_metric: 0.0,
            quality_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            balance_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0, 1],
        },
        PassReport {
            observed_count: 4,
            requested_cluster_count: 2,
            realized_cluster_count: 2,
            quality_metric: 1.0,
            balance_metric: 0.0,
            quality_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            balance_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0, 1],
        },
    ]
}

pub fn expected_assignments() -> Vec<(Embedding, ClusterId)> {
    vec![(vec![0.25, 0.0], 0), (vec![10.75, 0.0], 1)]
}

pub fn underfull_first_pass() -> PassInput {
    vec![vec![vec![0.0, 0.0]]]
}

pub fn wrong_dimension_embedding() -> Embedding {
    vec![1.0]
}

pub fn nan_embedding() -> Embedding {
    vec![f32::NAN, 0.0]
}

pub fn exact_k_failure_pass() -> PassInput {
    vec![vec![
        vec![0.0, 0.0],
        vec![1.0, 0.0],
        vec![0.0, 1.0],
        vec![1.0, 1.0],
    ]]
}

pub fn identical_embedding_passes() -> Vec<PassInput> {
    let pass = vec![
        vec![vec![5.0, 5.0], vec![5.0, 5.0]],
        vec![vec![5.0, 5.0], vec![5.0, 5.0]],
    ];
    vec![pass.clone(), pass]
}

pub fn partially_collapsed_duplicate_pass() -> PassInput {
    vec![vec![
        vec![0.0, 0.0],
        vec![0.0, 0.0],
        vec![0.0, 0.0],
        vec![10.0, 0.0],
    ]]
}

pub fn conforming_trainer() -> DirectionalPcaStreamingTrainer {
    DirectionalPcaStreamingTrainer::new(config(), params()).unwrap()
}

pub struct Harness;

pub struct HarnessTrainer {
    inner: DirectionalPcaStreamingTrainer,
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
    pub fn conforming() -> Self {
        Self {
            inner: conforming_trainer(),
            mode: HarnessMode::Conforming,
            completed_passes: 0,
        }
    }

    pub fn unstable_cluster_ids() -> Self {
        Self {
            inner: conforming_trainer(),
            mode: HarnessMode::UnstableClusterIds,
            completed_passes: 0,
        }
    }

    pub fn malformed_input_accepting() -> Self {
        Self {
            inner: conforming_trainer(),
            mode: HarnessMode::MalformedInputAccepting,
            completed_passes: 0,
        }
    }
}

impl lexongraph_streaming_clustering::StreamingClusterTrainer for HarnessTrainer {
    type Classifier = HarnessClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        self.inner.config()
    }

    fn state(&self) -> lexongraph_streaming_clustering::TrainerState {
        self.inner.state()
    }

    fn ingest_batch(
        &mut self,
        embeddings: &[Embedding],
    ) -> Result<(), lexongraph_streaming_clustering::StreamingClusteringError> {
        self.inner.ingest_batch(embeddings)
    }

    fn finish_pass(
        &mut self,
    ) -> Result<PassReport, lexongraph_streaming_clustering::StreamingClusteringError> {
        let mut report = self.inner.finish_pass()?;
        if matches!(self.mode, HarnessMode::UnstableClusterIds) && self.completed_passes > 0 {
            report.cluster_ids.reverse();
        }
        self.completed_passes += 1;
        Ok(report)
    }

    fn complete_training(
        &mut self,
    ) -> Result<(), lexongraph_streaming_clustering::StreamingClusteringError> {
        self.inner.complete_training()
    }

    fn into_classifier(
        self,
    ) -> Result<Self::Classifier, lexongraph_streaming_clustering::StreamingClusteringError> {
        Ok(HarnessClassifier {
            inner: self.inner.into_classifier()?,
            mode: self.mode,
        })
    }
}

pub struct HarnessClassifier {
    inner: <DirectionalPcaStreamingTrainer as lexongraph_streaming_clustering::StreamingClusterTrainer>::Classifier,
    mode: HarnessMode,
}

impl lexongraph_streaming_clustering::StreamingClusterClassifier for HarnessClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        self.inner.config()
    }

    fn assign(
        &self,
        embedding: &[f32],
    ) -> Result<ClusterId, lexongraph_streaming_clustering::StreamingClusteringError> {
        if matches!(self.mode, HarnessMode::MalformedInputAccepting) {
            let first = embedding.first().copied().unwrap_or(0.0);
            return Ok(if first < 5.0 { 0 } else { 1 });
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
