// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![cfg(feature = "conformance")]

use std::cell::Cell;

use lexongraph_block::EmbeddingSpec;
use lexongraph_search::CandidateScorer;
use lexongraph_search::EmbeddingCompatibility;
use lexongraph_search::conformance::{
    CandidateScorerConformanceHarness, EmbeddingCompatibilityConformanceHarness, FixtureError,
    run_candidate_scorer_suite, run_embedding_compatibility_suite, run_full_trait_suite,
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
