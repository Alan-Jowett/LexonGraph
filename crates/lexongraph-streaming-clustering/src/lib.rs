// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Shared LexonGraph streaming multi-pass clustering contract.
//!
//! ```compile_fail
//! #[cfg(feature = "conformance")]
//! compile_error!("the conformance module is intentionally enabled in this doctest configuration");
//!
//! use lexongraph_streaming_clustering::conformance;
//!
//! let _ = std::any::type_name::<conformance::ConformanceError>();
//! ```

use std::fmt;

pub type ClusterId = u32;
pub type Embedding = Vec<f32>;
pub type EmbeddingBatch = Vec<Embedding>;
pub type PassInput = Vec<EmbeddingBatch>;

#[derive(Clone, Debug, PartialEq)]
pub struct StreamingClusteringConfig {
    pub cluster_count: u32,
    pub dimensions: usize,
    pub balance_constraints: Option<BalanceConstraints>,
    pub random_seed: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BalanceConstraints {
    pub min_cluster_occupancy: Option<u32>,
    pub max_cluster_occupancy: Option<u32>,
    pub max_cluster_size_ratio: Option<f64>,
    pub soft_balance_penalty: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricDirection {
    LargerIsBetter,
    SmallerIsBetter,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrainerState {
    Idle,
    Ingesting,
    PassComplete,
    TrainingComplete,
    ClassifierProduced,
    Error,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PassReport {
    pub observed_count: usize,
    pub quality_metric: f64,
    pub balance_metric: f64,
    pub quality_direction: MetricDirection,
    pub balance_direction: MetricDirection,
    pub cluster_ids: Vec<ClusterId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamingClusteringError {
    InvalidConfiguration {
        message: String,
    },
    InvalidTransition {
        state: TrainerState,
        operation: String,
    },
    UnsatisfiableConstraint {
        message: String,
    },
    MalformedInput {
        message: String,
    },
}

impl fmt::Display for StreamingClusteringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration { message } => {
                write!(f, "invalid streaming clustering configuration: {message}")
            }
            Self::InvalidTransition { state, operation } => {
                write!(
                    f,
                    "invalid streaming clustering transition: cannot call {operation} while in {state:?}"
                )
            }
            Self::UnsatisfiableConstraint { message } => {
                write!(
                    f,
                    "unsatisfiable streaming clustering constraint: {message}"
                )
            }
            Self::MalformedInput { message } => {
                write!(f, "malformed streaming clustering input: {message}")
            }
        }
    }
}

impl std::error::Error for StreamingClusteringError {}

pub fn validate_config(config: &StreamingClusteringConfig) -> Result<(), StreamingClusteringError> {
    if config.cluster_count == 0 {
        return Err(StreamingClusteringError::InvalidConfiguration {
            message: "cluster_count must be positive".into(),
        });
    }
    if config.dimensions == 0 {
        return Err(StreamingClusteringError::InvalidConfiguration {
            message: "dimensions must be positive".into(),
        });
    }
    if let Some(constraints) = &config.balance_constraints {
        if let Some(min_cluster_occupancy) = constraints.min_cluster_occupancy
            && min_cluster_occupancy == 0
        {
            return Err(StreamingClusteringError::InvalidConfiguration {
                message: "min_cluster_occupancy must be positive when provided".into(),
            });
        }
        if let Some(max_cluster_occupancy) = constraints.max_cluster_occupancy
            && max_cluster_occupancy == 0
        {
            return Err(StreamingClusteringError::InvalidConfiguration {
                message: "max_cluster_occupancy must be positive when provided".into(),
            });
        }
        if let (Some(min_cluster_occupancy), Some(max_cluster_occupancy)) = (
            constraints.min_cluster_occupancy,
            constraints.max_cluster_occupancy,
        ) && min_cluster_occupancy > max_cluster_occupancy
        {
            return Err(StreamingClusteringError::InvalidConfiguration {
                message: "min_cluster_occupancy cannot exceed max_cluster_occupancy".into(),
            });
        }
        if let Some(max_cluster_size_ratio) = constraints.max_cluster_size_ratio
            && (!max_cluster_size_ratio.is_finite() || max_cluster_size_ratio <= 0.0)
        {
            return Err(StreamingClusteringError::InvalidConfiguration {
                message: "max_cluster_size_ratio must be finite and positive".into(),
            });
        }
        if let Some(soft_balance_penalty) = constraints.soft_balance_penalty
            && (!soft_balance_penalty.is_finite() || soft_balance_penalty < 0.0)
        {
            return Err(StreamingClusteringError::InvalidConfiguration {
                message: "soft_balance_penalty must be finite and non-negative".into(),
            });
        }
    }
    Ok(())
}

pub fn validate_embedding(
    embedding: &[f32],
    dimensions: usize,
) -> Result<(), StreamingClusteringError> {
    if embedding.len() != dimensions {
        return Err(StreamingClusteringError::MalformedInput {
            message: format!(
                "expected embedding dimensionality {dimensions}, got {}",
                embedding.len()
            ),
        });
    }
    if embedding.iter().any(|value| !value.is_finite()) {
        return Err(StreamingClusteringError::MalformedInput {
            message: "embeddings must not contain NaN or infinite values".into(),
        });
    }
    Ok(())
}

pub trait StreamingClusterClassifier {
    fn config(&self) -> &StreamingClusteringConfig;

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError>;

    fn assign_batch(
        &self,
        embeddings: &[Embedding],
    ) -> Result<Vec<ClusterId>, StreamingClusteringError> {
        embeddings
            .iter()
            .map(|embedding| self.assign(embedding.as_slice()))
            .collect()
    }
}

pub trait StreamingClusterTrainer {
    type Classifier: StreamingClusterClassifier;

    fn config(&self) -> &StreamingClusteringConfig;
    fn state(&self) -> TrainerState;

    fn ingest_batch(&mut self, embeddings: &[Embedding]) -> Result<(), StreamingClusteringError>;

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError>;

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError>;

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError>;
}

#[cfg(feature = "conformance")]
mod conformance_support {
    use super::*;

    pub type ConformanceResult = Result<(), ConformanceError>;

    #[derive(Debug)]
    pub enum ConformanceError {
        Implementation(StreamingClusteringError),
        Expectation(String),
    }

    impl fmt::Display for ConformanceError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Implementation(error) => write!(f, "{error}"),
                Self::Expectation(message) => {
                    write!(f, "conformance expectation failed: {message}")
                }
            }
        }
    }

    impl std::error::Error for ConformanceError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Implementation(error) => Some(error),
                Self::Expectation(_) => None,
            }
        }
    }

    impl From<StreamingClusteringError> for ConformanceError {
        fn from(value: StreamingClusteringError) -> Self {
            Self::Implementation(value)
        }
    }

    pub trait StreamingClusteringConformanceHarness {
        type Trainer: StreamingClusterTrainer;

        fn conforming_trainer(&self) -> Self::Trainer;
        fn unstable_cluster_ids_trainer(&self) -> Self::Trainer;
        fn sample_passes(&self) -> Vec<PassInput>;
        fn expected_pass_reports(&self) -> Vec<PassReport>;
        fn expected_assignments(&self) -> Vec<(Embedding, ClusterId)>;
        fn underfull_first_pass(&self) -> PassInput;
        fn wrong_dimension_embedding(&self) -> Embedding;
        fn nan_embedding(&self) -> Embedding;
    }

    pub fn run_streaming_clustering_suite<H>(harness: &H) -> ConformanceResult
    where
        H: StreamingClusteringConformanceHarness,
    {
        let sample_passes = harness.sample_passes();
        let expected_pass_reports = harness.expected_pass_reports();
        let expected_assignments = harness.expected_assignments();

        let conforming_trace = collect_training_trace(
            harness.conforming_trainer(),
            &sample_passes,
            &expected_assignments,
        )?;
        if conforming_trace.pass_reports != expected_pass_reports {
            return Err(ConformanceError::Expectation(format!(
                "expected pass reports {:?}, got {:?}",
                expected_pass_reports, conforming_trace.pass_reports
            )));
        }
        let expected_cluster_ids = expected_assignments
            .iter()
            .map(|(_, cluster_id)| *cluster_id)
            .collect::<Vec<_>>();
        if conforming_trace.assignments != expected_cluster_ids {
            return Err(ConformanceError::Expectation(format!(
                "expected classifier assignments {:?}, got {:?}",
                expected_cluster_ids, conforming_trace.assignments
            )));
        }

        let repeated_trace = collect_training_trace(
            harness.conforming_trainer(),
            &sample_passes,
            &expected_assignments,
        )?;
        if repeated_trace != conforming_trace {
            return Err(ConformanceError::Expectation(
                "expected repeated conforming runs to be deterministic".into(),
            ));
        }

        let unstable_trace = collect_training_trace(
            harness.unstable_cluster_ids_trainer(),
            &sample_passes,
            &expected_assignments,
        );
        match unstable_trace {
            Err(ConformanceError::Expectation(_)) => {}
            Err(error) => return Err(error),
            Ok(trace) => {
                return Err(ConformanceError::Expectation(format!(
                    "expected unstable cluster-ID fixture to fail, got pass reports {:?}",
                    trace.pass_reports
                )));
            }
        }

        let mut invalid_transition_trainer = harness.conforming_trainer();
        match invalid_transition_trainer.finish_pass() {
            Err(StreamingClusteringError::InvalidTransition { .. }) => {}
            Err(error) => return Err(ConformanceError::Implementation(error)),
            Ok(_) => {
                return Err(ConformanceError::Expectation(
                    "expected finish_pass without any ingested batch to fail".into(),
                ));
            }
        }
        if invalid_transition_trainer.state() != TrainerState::Error {
            return Err(ConformanceError::Expectation(
                "expected invalid transition to place the trainer in the Error state".into(),
            ));
        }

        let mut underfull_trainer = harness.conforming_trainer();
        for batch in harness.underfull_first_pass() {
            underfull_trainer.ingest_batch(&batch)?;
        }
        match underfull_trainer.finish_pass() {
            Err(StreamingClusteringError::UnsatisfiableConstraint { .. }) => {}
            Err(error) => return Err(ConformanceError::Implementation(error)),
            Ok(_) => {
                return Err(ConformanceError::Expectation(
                    "expected a first pass with N < K to fail explicitly".into(),
                ));
            }
        }
        if underfull_trainer.state() != TrainerState::Error {
            return Err(ConformanceError::Expectation(
                "expected first-pass unsatisfiable constraints to place the trainer in Error"
                    .into(),
            ));
        }

        let classifier = build_classifier(harness.conforming_trainer(), &sample_passes)?;
        match classifier.assign(harness.wrong_dimension_embedding().as_slice()) {
            Err(StreamingClusteringError::MalformedInput { .. }) => {}
            Err(error) => return Err(ConformanceError::Implementation(error)),
            Ok(cluster_id) => {
                return Err(ConformanceError::Expectation(format!(
                    "expected wrong-dimensionality embedding to fail, got cluster {cluster_id}"
                )));
            }
        }
        match classifier.assign(harness.nan_embedding().as_slice()) {
            Err(StreamingClusteringError::MalformedInput { .. }) => {}
            Err(error) => return Err(ConformanceError::Implementation(error)),
            Ok(cluster_id) => {
                return Err(ConformanceError::Expectation(format!(
                    "expected non-finite embedding to fail, got cluster {cluster_id}"
                )));
            }
        }

        Ok(())
    }

    #[derive(Debug, PartialEq)]
    struct TrainingTrace {
        pass_reports: Vec<PassReport>,
        assignments: Vec<ClusterId>,
    }

    fn collect_training_trace<T>(
        mut trainer: T,
        passes: &[PassInput],
        assignment_inputs: &[(Embedding, ClusterId)],
    ) -> Result<TrainingTrace, ConformanceError>
    where
        T: StreamingClusterTrainer,
    {
        validate_config(trainer.config()).map_err(ConformanceError::Implementation)?;
        if trainer.state() != TrainerState::Idle {
            return Err(ConformanceError::Expectation(format!(
                "expected trainer to start in Idle, got {:?}",
                trainer.state()
            )));
        }

        let mut pass_reports: Vec<PassReport> = Vec::with_capacity(passes.len());
        for pass in passes {
            for batch in pass {
                trainer.ingest_batch(batch)?;
                if trainer.state() != TrainerState::Ingesting {
                    return Err(ConformanceError::Expectation(format!(
                        "expected ingest_batch to leave trainer in Ingesting, got {:?}",
                        trainer.state()
                    )));
                }
            }
            let report = trainer.finish_pass()?;
            if report.cluster_ids.len() != trainer.config().cluster_count as usize {
                return Err(ConformanceError::Expectation(format!(
                    "expected {} cluster ids in pass report, got {}",
                    trainer.config().cluster_count,
                    report.cluster_ids.len()
                )));
            }
            if let Some(previous_report) = pass_reports.last()
                && report.cluster_ids != previous_report.cluster_ids
            {
                return Err(ConformanceError::Expectation(format!(
                    "expected cluster ids {:?} to remain stable across passes, got {:?}",
                    previous_report.cluster_ids, report.cluster_ids
                )));
            }
            if trainer.state() != TrainerState::PassComplete {
                return Err(ConformanceError::Expectation(format!(
                    "expected finish_pass to leave trainer in PassComplete, got {:?}",
                    trainer.state()
                )));
            }
            pass_reports.push(report);
        }

        trainer.complete_training()?;
        if trainer.state() != TrainerState::TrainingComplete {
            return Err(ConformanceError::Expectation(format!(
                "expected complete_training to leave trainer in TrainingComplete, got {:?}",
                trainer.state()
            )));
        }
        let classifier = trainer.into_classifier()?;
        let assignments = assignment_inputs
            .iter()
            .map(|(embedding, _)| classifier.assign(embedding.as_slice()))
            .collect::<Result<Vec<_>, _>>()?;
        for cluster_id in &assignments {
            if *cluster_id >= classifier.config().cluster_count {
                return Err(ConformanceError::Expectation(format!(
                    "expected classifier assignment to be in [0, {}), got {}",
                    classifier.config().cluster_count,
                    cluster_id
                )));
            }
        }

        Ok(TrainingTrace {
            pass_reports,
            assignments,
        })
    }

    fn build_classifier<T>(
        mut trainer: T,
        passes: &[PassInput],
    ) -> Result<T::Classifier, ConformanceError>
    where
        T: StreamingClusterTrainer,
    {
        validate_config(trainer.config()).map_err(ConformanceError::Implementation)?;
        for pass in passes {
            for batch in pass {
                trainer.ingest_batch(batch)?;
            }
            trainer.finish_pass()?;
        }
        trainer.complete_training()?;
        trainer
            .into_classifier()
            .map_err(ConformanceError::Implementation)
    }
}

#[cfg(feature = "conformance")]
pub mod conformance {
    //! Opt-in helper APIs for validating downstream streaming clustering implementations.
    //!
    //! Enable this module from test code with a dev-dependency such as:
    //!
    //! ```toml
    //! [dev-dependencies]
    //! lexongraph-streaming-clustering = { version = "*", features = ["conformance"] }
    //! ```

    pub use super::conformance_support::{
        ConformanceError, ConformanceResult, StreamingClusteringConformanceHarness,
        run_streaming_clustering_suite,
    };
}
