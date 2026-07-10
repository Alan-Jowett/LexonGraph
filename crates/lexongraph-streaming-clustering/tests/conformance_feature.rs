// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![cfg(feature = "conformance")]

mod support;

use std::error::Error;

use lexongraph_streaming_clustering::conformance::{
    SamplePassEvent, StreamingClusteringConformanceHarness, run_streaming_clustering_suite,
};
use lexongraph_streaming_clustering::{
    ClusterId, Embedding, PassReport, StreamingClusterClassifier, StreamingClusterTrainer,
    StreamingClusteringConfig, StreamingClusteringError, TrainerState,
    conformance::ConformanceError,
};
use support::{
    FixtureClassifier, FixtureHarness, FixtureTrainer, expected_assignments, expected_pass_reports,
    nan_embedding, sample_passes, underfull_first_pass, wrong_dimension_embedding,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaultMode {
    None,
    UnexpectedUnstableImplementationError,
    IdleFinishPassWrongError,
    UnderfullFinishPassWrongError,
    WrongDimensionWrongError,
    NanWrongError,
}

struct FaultHarness {
    conforming_unstable: bool,
    conforming_fault: FaultMode,
    unstable_fault: FaultMode,
    malformed_fault: FaultMode,
}

impl FaultHarness {
    fn expectation_failure() -> Self {
        Self {
            conforming_unstable: true,
            conforming_fault: FaultMode::None,
            unstable_fault: FaultMode::None,
            malformed_fault: FaultMode::None,
        }
    }

    fn unexpected_unstable_implementation_error() -> Self {
        Self {
            conforming_unstable: false,
            conforming_fault: FaultMode::None,
            unstable_fault: FaultMode::UnexpectedUnstableImplementationError,
            malformed_fault: FaultMode::None,
        }
    }

    fn idle_finish_pass_wrong_error() -> Self {
        Self {
            conforming_unstable: false,
            conforming_fault: FaultMode::IdleFinishPassWrongError,
            unstable_fault: FaultMode::None,
            malformed_fault: FaultMode::None,
        }
    }

    fn underfull_finish_pass_wrong_error() -> Self {
        Self {
            conforming_unstable: false,
            conforming_fault: FaultMode::UnderfullFinishPassWrongError,
            unstable_fault: FaultMode::None,
            malformed_fault: FaultMode::None,
        }
    }

    fn wrong_dimension_wrong_error() -> Self {
        Self {
            conforming_unstable: false,
            conforming_fault: FaultMode::None,
            unstable_fault: FaultMode::None,
            malformed_fault: FaultMode::WrongDimensionWrongError,
        }
    }

    fn nan_wrong_error() -> Self {
        Self {
            conforming_unstable: false,
            conforming_fault: FaultMode::None,
            unstable_fault: FaultMode::None,
            malformed_fault: FaultMode::NanWrongError,
        }
    }
}

impl StreamingClusteringConformanceHarness for FaultHarness {
    type Trainer = FaultInjectingTrainer;

    fn conforming_trainer(&self) -> Self::Trainer {
        let trainer = if self.conforming_unstable {
            FixtureTrainer::unstable_cluster_ids()
        } else {
            FixtureTrainer::default_deterministic()
        };
        FaultInjectingTrainer::new(trainer, self.conforming_fault)
    }

    fn unstable_cluster_ids_trainer(&self) -> Self::Trainer {
        FaultInjectingTrainer::new(FixtureTrainer::unstable_cluster_ids(), self.unstable_fault)
    }

    fn malformed_input_accepting_trainer(&self) -> Self::Trainer {
        FaultInjectingTrainer::new(
            FixtureTrainer::malformed_input_accepting(),
            self.malformed_fault,
        )
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

struct FaultInjectingTrainer {
    inner: FixtureTrainer,
    fault: FaultMode,
    completed_passes: usize,
    current_pass_count: usize,
}

impl FaultInjectingTrainer {
    fn new(inner: FixtureTrainer, fault: FaultMode) -> Self {
        Self {
            inner,
            fault,
            completed_passes: 0,
            current_pass_count: 0,
        }
    }
}

impl StreamingClusterTrainer for FaultInjectingTrainer {
    type Classifier = FaultInjectingClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        self.inner.config()
    }

    fn state(&self) -> TrainerState {
        self.inner.state()
    }

    fn ingest_batch(&mut self, embeddings: &[Embedding]) -> Result<(), StreamingClusteringError> {
        self.current_pass_count += embeddings.len();
        self.inner.ingest_batch(embeddings)
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        if self.fault == FaultMode::IdleFinishPassWrongError
            && self.inner.state() == TrainerState::Idle
        {
            return Err(StreamingClusteringError::MalformedInput {
                message: "idle finish_pass surfaced implementation error".into(),
            });
        }

        if self.fault == FaultMode::UnderfullFinishPassWrongError
            && self.completed_passes == 0
            && self.current_pass_count < self.inner.config().cluster_count as usize
        {
            return Err(StreamingClusteringError::MalformedInput {
                message: "underfull first pass surfaced implementation error".into(),
            });
        }

        if self.fault == FaultMode::UnexpectedUnstableImplementationError
            && self.completed_passes == 1
        {
            return Err(StreamingClusteringError::MalformedInput {
                message: "unstable fixture surfaced implementation error".into(),
            });
        }

        let report = self.inner.finish_pass()?;
        self.current_pass_count = 0;
        self.completed_passes += 1;
        Ok(report)
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        self.inner.complete_training()
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        let classifier = self.inner.into_classifier()?;
        Ok(FaultInjectingClassifier {
            inner: classifier,
            fault: self.fault,
        })
    }
}

struct FaultInjectingClassifier {
    inner: FixtureClassifier,
    fault: FaultMode,
}

impl StreamingClusterClassifier for FaultInjectingClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        self.inner.config()
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        match self.fault {
            FaultMode::WrongDimensionWrongError if embedding.len() != self.config().dimensions => {
                Err(StreamingClusteringError::InvalidTransition {
                    state: TrainerState::Error,
                    operation: "assign".into(),
                })
            }
            FaultMode::NanWrongError => {
                if embedding.len() != self.config().dimensions {
                    return Err(StreamingClusteringError::MalformedInput {
                        message: format!(
                            "expected embedding dimensionality {}, got {}",
                            self.config().dimensions,
                            embedding.len()
                        ),
                    });
                }
                if embedding.iter().any(|value| !value.is_finite()) {
                    return Err(StreamingClusteringError::UnsatisfiableConstraint {
                        message: "non-finite embedding surfaced implementation error".into(),
                    });
                }
                self.inner.assign(embedding)
            }
            _ => self.inner.assign(embedding),
        }
    }
}

fn assert_runtime_implementation_error(
    result: Result<(), ConformanceError>,
    expected_message: &str,
) {
    let error = result.expect_err("suite should surface an implementation error");
    assert!(matches!(&error, ConformanceError::Implementation(_)));
    assert_eq!(error.to_string(), expected_message);
    assert_eq!(error.source().unwrap().to_string(), expected_message);
}

#[test]
fn downstream_crates_can_run_the_streaming_clustering_suite() {
    run_streaming_clustering_suite(&FixtureHarness).unwrap();
}

#[test]
fn val_stream_trait_017_conformance_error_surface_distinguishes_suite_and_impl_failures() {
    let implementation = ConformanceError::from(StreamingClusteringError::MalformedInput {
        message: "fixture rejected malformed input".into(),
    });
    assert_eq!(
        implementation.to_string(),
        "malformed streaming clustering input: fixture rejected malformed input"
    );
    let implementation_source = implementation.source().unwrap();
    assert_eq!(
        implementation_source.to_string(),
        "malformed streaming clustering input: fixture rejected malformed input"
    );

    let expectation = ConformanceError::Expectation("fixture violated expectation".into());
    assert_eq!(
        expectation.to_string(),
        "conformance expectation failed: fixture violated expectation"
    );
    assert!(expectation.source().is_none());
}

#[test]
fn val_stream_trait_017_runtime_expectation_failures_use_suite_messages_without_sources() {
    let error = run_streaming_clustering_suite(&FaultHarness::expectation_failure()).expect_err(
        "suite should surface an expectation failure when the conforming trainer is unstable",
    );
    assert!(matches!(&error, ConformanceError::Expectation(_)));
    assert_eq!(
        error.to_string(),
        "conformance expectation failed: expected cluster ids [0, 1] to remain stable across passes, got [1, 0]"
    );
    assert!(error.source().is_none());
}

#[test]
fn val_stream_trait_017_runtime_implementation_failures_propagate_from_unstable_fixture() {
    assert_runtime_implementation_error(
        run_streaming_clustering_suite(&FaultHarness::unexpected_unstable_implementation_error()),
        "malformed streaming clustering input: unstable fixture surfaced implementation error",
    );
}

#[test]
fn val_stream_trait_017_runtime_implementation_failures_propagate_from_idle_finish_pass() {
    assert_runtime_implementation_error(
        run_streaming_clustering_suite(&FaultHarness::idle_finish_pass_wrong_error()),
        "malformed streaming clustering input: idle finish_pass surfaced implementation error",
    );
}

#[test]
fn val_stream_trait_017_runtime_implementation_failures_propagate_from_underfull_finish_pass() {
    assert_runtime_implementation_error(
        run_streaming_clustering_suite(&FaultHarness::underfull_finish_pass_wrong_error()),
        "malformed streaming clustering input: underfull first pass surfaced implementation error",
    );
}

#[test]
fn val_stream_trait_017_runtime_implementation_failures_preserve_wrong_dimension_errors() {
    assert_runtime_implementation_error(
        run_streaming_clustering_suite(&FaultHarness::wrong_dimension_wrong_error()),
        "invalid streaming clustering transition: cannot call assign while in Error",
    );
}

#[test]
fn val_stream_trait_017_runtime_implementation_failures_preserve_non_finite_errors() {
    assert_runtime_implementation_error(
        run_streaming_clustering_suite(&FaultHarness::nan_wrong_error()),
        "unsatisfiable streaming clustering constraint: non-finite embedding surfaced implementation error",
    );
}
