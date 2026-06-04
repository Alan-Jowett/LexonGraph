// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![cfg(feature = "conformance")]

use lexongraph_embeddings_trait::conformance::{
    ConformanceError, EmbeddingProviderConformanceHarness, FixtureError,
    run_embedding_provider_suite,
};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider, EmbeddingSpec};

#[derive(Clone)]
enum ProviderMode {
    Good,
    WrongBytesSameLength,
    WrongOrder,
    Fail,
    WrongLength,
    WrongCount,
}

#[derive(Clone)]
struct ProviderFixture(ProviderMode);

impl EmbeddingProvider for ProviderFixture {
    type Error = FixtureError;

    async fn embed(
        &self,
        input: &EmbeddingInput,
        _: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        match self.0 {
            ProviderMode::Good | ProviderMode::WrongOrder | ProviderMode::WrongCount => {
                Ok(expected_embedding_for_input(input))
            }
            ProviderMode::WrongBytesSameLength => Ok(vec![0x01, 0x02]),
            ProviderMode::Fail => Err(FixtureError("embed failure".into())),
            ProviderMode::WrongLength => Ok(vec![0x01]),
        }
    }

    async fn embed_batch(
        &self,
        inputs: &[EmbeddingInput],
        _: &EmbeddingSpec,
    ) -> Result<Vec<Vec<u8>>, Self::Error> {
        match self.0 {
            ProviderMode::Good => Ok(inputs.iter().map(expected_embedding_for_input).collect()),
            ProviderMode::WrongBytesSameLength => {
                Ok(inputs.iter().map(|_| vec![0x01, 0x02]).collect())
            }
            ProviderMode::WrongOrder => Ok(inputs
                .iter()
                .rev()
                .map(expected_embedding_for_input)
                .collect()),
            ProviderMode::Fail => Err(FixtureError("embed failure".into())),
            ProviderMode::WrongLength => Ok(inputs.iter().map(|_| vec![0x01]).collect()),
            ProviderMode::WrongCount => Ok(inputs
                .first()
                .map(expected_embedding_for_input)
                .into_iter()
                .collect()),
        }
    }
}

struct EmbeddingHarness {
    spec: EmbeddingSpec,
    conforming: ProviderMode,
    failing: ProviderMode,
    invalid_output: ProviderMode,
}

impl EmbeddingHarness {
    fn happy_path() -> Self {
        Self {
            spec: EmbeddingSpec {
                dims: 2,
                encoding: "i8".into(),
            },
            conforming: ProviderMode::Good,
            failing: ProviderMode::Fail,
            invalid_output: ProviderMode::WrongLength,
        }
    }
}

impl EmbeddingProviderConformanceHarness for EmbeddingHarness {
    type Provider = ProviderFixture;

    fn sample_input(&self) -> EmbeddingInput {
        EmbeddingInput {
            media_type: "text/plain".into(),
            body: b"fixture".to_vec(),
        }
    }

    fn compatible_spec(&self) -> EmbeddingSpec {
        self.spec.clone()
    }

    fn sample_inputs(&self) -> Vec<EmbeddingInput> {
        vec![
            EmbeddingInput {
                media_type: "text/plain".into(),
                body: b"fixture".to_vec(),
            },
            EmbeddingInput {
                media_type: "text/plain".into(),
                body: b"batch".to_vec(),
            },
        ]
    }

    fn expected_embedding(&self) -> Vec<u8> {
        vec![b'f', 0x20]
    }

    fn expected_embeddings(&self) -> Vec<Vec<u8>> {
        self.sample_inputs()
            .iter()
            .map(expected_embedding_for_input)
            .collect()
    }

    fn conforming_provider(&self) -> Self::Provider {
        ProviderFixture(self.conforming.clone())
    }

    fn failing_provider(&self) -> Self::Provider {
        ProviderFixture(self.failing.clone())
    }

    fn invalid_output_provider(&self) -> Self::Provider {
        ProviderFixture(self.invalid_output.clone())
    }
}

fn assert_expectation_failure(harness: EmbeddingHarness) {
    match run_embedding_provider_suite(&harness) {
        Err(ConformanceError::Expectation(_)) => {}
        Err(ConformanceError::Provider(message)) => {
            panic!("expected expectation failure, got provider failure: {message}")
        }
        Ok(()) => panic!("expected conformance suite to fail"),
    }
}

#[test]
fn downstream_crates_can_run_the_embedding_provider_suite() {
    run_embedding_provider_suite(&EmbeddingHarness::happy_path()).unwrap();
}

#[test]
fn val_embed_trait_007_rejects_conforming_provider_that_returns_wrong_bytes() {
    assert_expectation_failure(EmbeddingHarness {
        conforming: ProviderMode::WrongBytesSameLength,
        ..EmbeddingHarness::happy_path()
    });
}

#[test]
fn val_embed_trait_008_rejects_failing_provider_that_unexpectedly_succeeds() {
    assert_expectation_failure(EmbeddingHarness {
        failing: ProviderMode::Good,
        ..EmbeddingHarness::happy_path()
    });
}

#[test]
fn val_embed_trait_009_rejects_invalid_output_provider_that_matches_spec() {
    assert_expectation_failure(EmbeddingHarness {
        invalid_output: ProviderMode::Good,
        ..EmbeddingHarness::happy_path()
    });
}

#[test]
fn val_embed_trait_010_rejects_unsupported_embedding_encoding() {
    assert_expectation_failure(EmbeddingHarness {
        spec: EmbeddingSpec {
            dims: 2,
            encoding: "future".into(),
        },
        ..EmbeddingHarness::happy_path()
    });
}

#[test]
fn val_embed_trait_011_provider_failures_surface_through_provider_category() {
    match run_embedding_provider_suite(&EmbeddingHarness {
        conforming: ProviderMode::Fail,
        ..EmbeddingHarness::happy_path()
    }) {
        Err(ConformanceError::Provider(_)) => {}
        Err(ConformanceError::Expectation(message)) => {
            panic!("expected provider failure, got expectation failure: {message}")
        }
        Ok(()) => panic!("expected conformance suite to fail"),
    }
}

#[test]
fn val_embed_trait_012_rejects_conforming_provider_that_returns_wrong_count() {
    assert_expectation_failure(EmbeddingHarness {
        conforming: ProviderMode::WrongCount,
        ..EmbeddingHarness::happy_path()
    });
}

#[test]
fn val_embed_trait_013_rejects_conforming_provider_that_returns_wrong_order() {
    assert_expectation_failure(EmbeddingHarness {
        conforming: ProviderMode::WrongOrder,
        ..EmbeddingHarness::happy_path()
    });
}

fn expected_embedding_for_input(input: &EmbeddingInput) -> Vec<u8> {
    vec![input.body[0], 0x20]
}
