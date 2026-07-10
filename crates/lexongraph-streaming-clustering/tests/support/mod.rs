// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![allow(dead_code)]

use lexongraph_streaming_clustering::{
    ClusterId, Embedding, PassReport, StreamingClusterClassifier, StreamingClusterTrainer,
    StreamingClusteringConfig, StreamingClusteringError, TrainerState, validate_config,
    validate_embedding,
};

#[derive(Clone, Copy)]
pub enum FixtureMode {
    Conforming,
    UnstableClusterIds,
    MalformedInputAccepting,
}

pub struct FixtureTrainer {
    config: StreamingClusteringConfig,
    mode: FixtureMode,
    state: TrainerState,
    current_pass_count: usize,
    completed_passes: usize,
}

impl FixtureTrainer {
    pub fn default_deterministic() -> Self {
        Self::new(None, FixtureMode::Conforming)
    }

    pub fn explicit_seed(seed: u64) -> Self {
        Self::new(Some(seed), FixtureMode::Conforming)
    }

    pub fn unstable_cluster_ids() -> Self {
        Self::new(None, FixtureMode::UnstableClusterIds)
    }

    pub fn malformed_input_accepting() -> Self {
        Self::new(None, FixtureMode::MalformedInputAccepting)
    }

    fn new(random_seed: Option<u64>, mode: FixtureMode) -> Self {
        Self {
            config: StreamingClusteringConfig {
                cluster_count: 2,
                dimensions: 2,
                balance_constraints: None,
                random_seed,
            },
            mode,
            state: TrainerState::Idle,
            current_pass_count: 0,
            completed_passes: 0,
        }
    }

    fn invalid_transition(&mut self, operation: &str) -> Result<(), StreamingClusteringError> {
        let state = self.state;
        self.state = TrainerState::Error;
        Err(StreamingClusteringError::InvalidTransition {
            state,
            operation: operation.into(),
        })
    }

    fn cluster_ids_for_pass(&self, pass_index: usize) -> Vec<ClusterId> {
        match (self.mode, pass_index) {
            (FixtureMode::UnstableClusterIds, 1) => vec![1, 0],
            _ => vec![0, 1],
        }
    }
}

impl StreamingClusterTrainer for FixtureTrainer {
    type Classifier = FixtureClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn state(&self) -> TrainerState {
        self.state
    }

    fn ingest_batch(&mut self, embeddings: &[Embedding]) -> Result<(), StreamingClusteringError> {
        validate_config(&self.config)?;
        match self.state {
            TrainerState::Idle | TrainerState::PassComplete => {
                self.state = TrainerState::Ingesting;
            }
            TrainerState::Ingesting => {}
            TrainerState::TrainingComplete | TrainerState::Error => {
                return self.invalid_transition("ingest_batch");
            }
        }

        for embedding in embeddings {
            validate_embedding(embedding, self.config.dimensions)?;
            self.current_pass_count += 1;
        }
        Ok(())
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        if self.state != TrainerState::Ingesting {
            self.invalid_transition("finish_pass")?;
        }
        if self.completed_passes == 0
            && self.current_pass_count < self.config.cluster_count as usize
        {
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::UnsatisfiableConstraint {
                message: format!(
                    "first pass established N = {} which is smaller than K = {}",
                    self.current_pass_count, self.config.cluster_count
                ),
            });
        }

        let pass_index = self.completed_passes;
        let report = PassReport {
            observed_count: self.current_pass_count,
            requested_cluster_count: self.config.cluster_count,
            realized_cluster_count: self.config.cluster_count,
            quality_metric: if pass_index == 0 { 10.0 } else { 5.0 },
            balance_metric: 0.0,
            quality_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            balance_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            cluster_ids: self.cluster_ids_for_pass(pass_index),
        };
        self.completed_passes += 1;
        self.current_pass_count = 0;
        self.state = TrainerState::PassComplete;
        Ok(report)
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        if self.state != TrainerState::PassComplete {
            self.invalid_transition("complete_training")?;
        }
        self.state = TrainerState::TrainingComplete;
        Ok(())
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        if self.state != TrainerState::TrainingComplete {
            let state = self.state;
            return Err(StreamingClusteringError::InvalidTransition {
                state,
                operation: "into_classifier".into(),
            });
        }
        Ok(FixtureClassifier {
            config: self.config.clone(),
            mode: self.mode,
        })
    }
}

pub struct FixtureClassifier {
    config: StreamingClusteringConfig,
    mode: FixtureMode,
}

impl StreamingClusterClassifier for FixtureClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        if matches!(self.mode, FixtureMode::MalformedInputAccepting) {
            let first = embedding.first().copied().unwrap_or(0.0);
            return Ok(if first >= 0.0 { 0 } else { 1 });
        }
        validate_embedding(embedding, self.config.dimensions)?;
        Ok(if embedding[0] >= 0.0 { 0 } else { 1 })
    }
}

pub fn sample_passes() -> Vec<Vec<SampleBatch>> {
    vec![
        vec![vec![vec![1.0, 0.0]], vec![vec![-1.0, 0.0]]],
        vec![vec![vec![0.75, 0.0], vec![-0.75, 0.0]]],
    ]
}

pub fn expected_pass_reports() -> Vec<PassReport> {
    vec![
        PassReport {
            observed_count: 2,
            requested_cluster_count: 2,
            realized_cluster_count: 2,
            quality_metric: 10.0,
            balance_metric: 0.0,
            quality_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            balance_direction: lexongraph_streaming_clustering::MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0, 1],
        },
        PassReport {
            observed_count: 2,
            requested_cluster_count: 2,
            realized_cluster_count: 2,
            quality_metric: 5.0,
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

pub fn underfull_first_pass() -> Vec<SampleBatch> {
    vec![vec![vec![1.0, 0.0]]]
}

pub fn wrong_dimension_embedding() -> Embedding {
    vec![1.0]
}

pub fn nan_embedding() -> Embedding {
    vec![f32::NAN, 0.0]
}

#[cfg(feature = "conformance")]
pub struct FixtureHarness;

#[cfg(feature = "conformance")]
impl lexongraph_streaming_clustering::conformance::StreamingClusteringConformanceHarness
    for FixtureHarness
{
    type Trainer = FixtureTrainer;

    fn conforming_trainer(&self) -> Self::Trainer {
        FixtureTrainer::default_deterministic()
    }

    fn unstable_cluster_ids_trainer(&self) -> Self::Trainer {
        FixtureTrainer::unstable_cluster_ids()
    }

    fn malformed_input_accepting_trainer(&self) -> Self::Trainer {
        FixtureTrainer::malformed_input_accepting()
    }

    fn for_each_sample_pass_event<E, F>(&self, mut on_event: F) -> Result<(), E>
    where
        F: FnMut(
            lexongraph_streaming_clustering::conformance::SamplePassEvent<'_>,
        ) -> Result<(), E>,
    {
        for pass in sample_passes() {
            for batch in pass {
                on_event(
                    lexongraph_streaming_clustering::conformance::SamplePassEvent::Batch(
                        batch.as_slice(),
                    ),
                )?;
            }
            on_event(lexongraph_streaming_clustering::conformance::SamplePassEvent::EndPass)?;
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
        F: FnMut(&[Embedding]) -> Result<(), E>,
    {
        for batch in underfull_first_pass() {
            on_batch(batch.as_slice())?;
        }
        Ok(())
    }

    fn wrong_dimension_embedding(&self) -> Embedding {
        wrong_dimension_embedding()
    }

    fn nan_embedding(&self) -> Embedding {
        nan_embedding()
    }
}
type SampleBatch = Vec<Embedding>;
