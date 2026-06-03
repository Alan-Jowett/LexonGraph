// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![cfg(feature = "conformance")]

use std::cell::Cell;

use lexongraph_block::EmbeddingSpec;
use lexongraph_search::CandidateScorer;
use lexongraph_search::DefaultCandidateScorer;
use lexongraph_search::DefaultEmbeddingCompatibility;
use lexongraph_search::EmbeddingCompatibility;
use lexongraph_search::EncodedTargetEmbedding;
use lexongraph_search::conformance::{
    CandidateScorerConformanceHarness, ConformanceError, EmbeddingCompatibilityConformanceHarness,
    FixtureError, run_candidate_scorer_suite, run_embedding_compatibility_suite,
    run_full_trait_suite,
};

#[derive(Clone)]
struct CompatibilityFixture(CompatibilityMode);

#[derive(Clone)]
enum CompatibilityMode {
    Conforming,
    Flaky,
}

impl EmbeddingCompatibility<()> for CompatibilityFixture {
    type Error = FixtureError;

    fn ensure_compatible(&self, _: &(), embedding_spec: &EmbeddingSpec) -> Result<(), Self::Error> {
        match self.0 {
            CompatibilityMode::Conforming => {
                if embedding_spec.encoding == "i8" {
                    Ok(())
                } else {
                    Err(FixtureError(format!(
                        "expected i8 embedding spec, got {}",
                        embedding_spec.encoding
                    )))
                }
            }
            CompatibilityMode::Flaky => {
                thread_local! {
                    static FLIP: Cell<bool> = const { Cell::new(false) };
                }
                FLIP.with(|flip| {
                    let next = !flip.get();
                    flip.set(next);
                    if next {
                        Ok(())
                    } else {
                        Err(FixtureError("flaky compatibility".into()))
                    }
                })
            }
        }
    }
}

struct CompatibilityHarness;

impl EmbeddingCompatibilityConformanceHarness for CompatibilityHarness {
    type Target = ();
    type Policy = CompatibilityFixture;

    fn target(&self) -> Self::Target {}

    fn compatible_spec(&self) -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: "i8".into(),
        }
    }

    fn incompatible_spec(&self) -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        }
    }

    fn conforming_policy(&self) -> Self::Policy {
        CompatibilityFixture(CompatibilityMode::Conforming)
    }

    fn nondeterministic_policy(&self) -> Self::Policy {
        CompatibilityFixture(CompatibilityMode::Flaky)
    }
}

#[derive(Clone)]
struct ScorerFixture(ScorerMode);

#[derive(Clone)]
enum ScorerMode {
    Conforming,
    Failing,
    Flaky,
}

impl CandidateScorer<()> for ScorerFixture {
    type Error = FixtureError;
    type Score = i32;

    fn score(
        &self,
        _: &(),
        candidate_embedding: &[u8],
        _: &EmbeddingSpec,
    ) -> Result<Self::Score, Self::Error> {
        match self.0 {
            ScorerMode::Conforming => Ok(candidate_embedding[0] as i32),
            ScorerMode::Failing => Err(FixtureError("scorer failure".into())),
            ScorerMode::Flaky => {
                thread_local! {
                    static DELTA: Cell<i32> = const { Cell::new(0) };
                }
                DELTA.with(|delta| {
                    let next = delta.get() ^ 1;
                    delta.set(next);
                    Ok((candidate_embedding[0] as i32) + next)
                })
            }
        }
    }
}

struct ScorerHarness;

impl CandidateScorerConformanceHarness for ScorerHarness {
    type Target = ();
    type Score = i32;
    type Scorer = ScorerFixture;

    fn target(&self) -> Self::Target {}

    fn embedding_spec(&self) -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: "i8".into(),
        }
    }

    fn preferred_candidate_embedding(&self) -> Vec<u8> {
        vec![9, 0]
    }

    fn alternate_candidate_embedding(&self) -> Vec<u8> {
        vec![3, 0]
    }

    fn expected_score(&self) -> Self::Score {
        9
    }

    fn conforming_scorer(&self) -> Self::Scorer {
        ScorerFixture(ScorerMode::Conforming)
    }

    fn failing_scorer(&self) -> Self::Scorer {
        ScorerFixture(ScorerMode::Failing)
    }

    fn nondeterministic_scorer(&self) -> Self::Scorer {
        ScorerFixture(ScorerMode::Flaky)
    }
}

#[derive(Clone)]
enum DefaultCompatibilityFixture {
    Conforming,
    Flaky,
}

impl EmbeddingCompatibility<EncodedTargetEmbedding> for DefaultCompatibilityFixture {
    type Error = FixtureError;

    fn ensure_compatible(
        &self,
        target: &EncodedTargetEmbedding,
        embedding_spec: &EmbeddingSpec,
    ) -> Result<(), Self::Error> {
        match self {
            Self::Conforming => DefaultEmbeddingCompatibility
                .ensure_compatible(target, embedding_spec)
                .map_err(|error| FixtureError(error.to_string())),
            Self::Flaky => {
                thread_local! {
                    static FLIP: Cell<bool> = const { Cell::new(false) };
                }
                FLIP.with(|flip| {
                    let next = !flip.get();
                    flip.set(next);
                    if next {
                        Ok(())
                    } else {
                        Err(FixtureError("flaky compatibility".into()))
                    }
                })
            }
        }
    }
}

struct DefaultCompatibilityHarness;

struct RejectingCompatibilityHarness;

impl EmbeddingCompatibilityConformanceHarness for DefaultCompatibilityHarness {
    type Target = EncodedTargetEmbedding;
    type Policy = DefaultCompatibilityFixture;

    fn target(&self) -> Self::Target {
        EncodedTargetEmbedding::new(f32_embedding([1.0, 0.0]), embedding_spec_f32())
    }

    fn compatible_spec(&self) -> EmbeddingSpec {
        embedding_spec_f32()
    }

    fn incompatible_spec(&self) -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: "f64le".into(),
        }
    }

    fn conforming_policy(&self) -> Self::Policy {
        DefaultCompatibilityFixture::Conforming
    }

    fn nondeterministic_policy(&self) -> Self::Policy {
        DefaultCompatibilityFixture::Flaky
    }
}

impl EmbeddingCompatibilityConformanceHarness for RejectingCompatibilityHarness {
    type Target = ();
    type Policy = CompatibilityFixture;

    fn target(&self) -> Self::Target {}

    fn compatible_spec(&self) -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        }
    }

    fn incompatible_spec(&self) -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: "f64le".into(),
        }
    }

    fn conforming_policy(&self) -> Self::Policy {
        CompatibilityFixture(CompatibilityMode::Conforming)
    }

    fn nondeterministic_policy(&self) -> Self::Policy {
        CompatibilityFixture(CompatibilityMode::Flaky)
    }
}

#[derive(Clone)]
enum DefaultScorerFixture {
    Conforming,
    Failing,
    Flaky,
}

impl CandidateScorer<EncodedTargetEmbedding> for DefaultScorerFixture {
    type Error = FixtureError;
    type Score = <DefaultCandidateScorer as CandidateScorer<EncodedTargetEmbedding>>::Score;

    fn score(
        &self,
        target: &EncodedTargetEmbedding,
        candidate_embedding: &[u8],
        embedding_spec: &EmbeddingSpec,
    ) -> Result<Self::Score, Self::Error> {
        match self {
            Self::Conforming => DefaultCandidateScorer
                .score(target, candidate_embedding, embedding_spec)
                .map_err(|error| FixtureError(error.to_string())),
            Self::Failing => Err(FixtureError("scorer failure".into())),
            Self::Flaky => {
                thread_local! {
                    static DELTA: Cell<bool> = const { Cell::new(false) };
                }
                let base = DefaultCandidateScorer
                    .score(target, candidate_embedding, embedding_spec)
                    .map_err(|error| FixtureError(error.to_string()))?;
                DELTA.with(|delta| {
                    let next = !delta.get();
                    delta.set(next);
                    if next {
                        Ok(base)
                    } else {
                        DefaultCandidateScorer
                            .score(target, &f32_embedding([0.0, 1.0]), &embedding_spec_f32())
                            .map_err(|error| FixtureError(error.to_string()))
                    }
                })
            }
        }
    }
}

struct DefaultScorerHarness;

struct WrongExpectedScoreHarness;

impl CandidateScorerConformanceHarness for DefaultScorerHarness {
    type Target = EncodedTargetEmbedding;
    type Score = <DefaultCandidateScorer as CandidateScorer<EncodedTargetEmbedding>>::Score;
    type Scorer = DefaultScorerFixture;

    fn target(&self) -> Self::Target {
        EncodedTargetEmbedding::new(f32_embedding([1.0, 0.0]), embedding_spec_f32())
    }

    fn embedding_spec(&self) -> EmbeddingSpec {
        embedding_spec_f32()
    }

    fn preferred_candidate_embedding(&self) -> Vec<u8> {
        f32_embedding([1.0, 0.0])
    }

    fn alternate_candidate_embedding(&self) -> Vec<u8> {
        f32_embedding([0.0, 1.0])
    }

    fn expected_score(&self) -> Self::Score {
        DefaultCandidateScorer
            .score(
                &self.target(),
                &self.preferred_candidate_embedding(),
                &self.embedding_spec(),
            )
            .unwrap()
    }

    fn conforming_scorer(&self) -> Self::Scorer {
        DefaultScorerFixture::Conforming
    }

    fn failing_scorer(&self) -> Self::Scorer {
        DefaultScorerFixture::Failing
    }

    fn nondeterministic_scorer(&self) -> Self::Scorer {
        DefaultScorerFixture::Flaky
    }
}

impl CandidateScorerConformanceHarness for WrongExpectedScoreHarness {
    type Target = ();
    type Score = i32;
    type Scorer = ScorerFixture;

    fn target(&self) -> Self::Target {}

    fn embedding_spec(&self) -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: "i8".into(),
        }
    }

    fn preferred_candidate_embedding(&self) -> Vec<u8> {
        vec![9, 0]
    }

    fn alternate_candidate_embedding(&self) -> Vec<u8> {
        vec![3, 0]
    }

    fn expected_score(&self) -> Self::Score {
        42
    }

    fn conforming_scorer(&self) -> Self::Scorer {
        ScorerFixture(ScorerMode::Conforming)
    }

    fn failing_scorer(&self) -> Self::Scorer {
        ScorerFixture(ScorerMode::Failing)
    }

    fn nondeterministic_scorer(&self) -> Self::Scorer {
        ScorerFixture(ScorerMode::Flaky)
    }
}

#[test]
fn downstream_crates_can_run_the_embedding_compatibility_suite() {
    run_embedding_compatibility_suite(&CompatibilityHarness).unwrap();
}

#[test]
fn downstream_crates_can_run_the_candidate_scorer_suite() {
    run_candidate_scorer_suite(&ScorerHarness).unwrap();
}

#[test]
fn downstream_crates_can_run_the_full_trait_suite() {
    run_full_trait_suite(&CompatibilityHarness, &ScorerHarness).unwrap();
}

#[test]
fn crate_defaults_satisfy_the_embedding_compatibility_harness() {
    run_embedding_compatibility_suite(&DefaultCompatibilityHarness).unwrap();
}

#[test]
fn crate_defaults_satisfy_the_candidate_scorer_harness() {
    run_candidate_scorer_suite(&DefaultScorerHarness).unwrap();
}

#[test]
fn embedding_compatibility_suite_reports_helper_expectation_failures() {
    let error = run_embedding_compatibility_suite(&RejectingCompatibilityHarness).unwrap_err();
    assert!(matches!(
        error,
        ConformanceError::Expectation(message)
            if message.contains("expected compatible embedding spec to be accepted")
    ));
}

#[test]
fn candidate_scorer_suite_reports_helper_expectation_failures() {
    let error = run_candidate_scorer_suite(&WrongExpectedScoreHarness).unwrap_err();
    assert!(matches!(
        error,
        ConformanceError::Expectation(message) if message.contains("expected score")
    ));
}

fn embedding_spec_f32() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: "f32le".into(),
    }
}

fn f32_embedding(values: [f32; 2]) -> Vec<u8> {
    values
        .into_iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}
