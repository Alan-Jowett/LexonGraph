// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![cfg(feature = "conformance")]

use lexongraph_embeddings_trait::conformance::{
    EmbeddingProviderConformanceHarness, FixtureError, run_embedding_provider_suite,
};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider, EmbeddingSpec};

#[derive(Clone)]
enum ProviderMode {
    Good,
    Fail,
    WrongLength,
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
            ProviderMode::Good => Ok(vec![input.body[0], 0x20]),
            ProviderMode::Fail => Err(FixtureError("embed failure".into())),
            ProviderMode::WrongLength => Ok(vec![0x01]),
        }
    }
}

struct EmbeddingHarness;

impl EmbeddingProviderConformanceHarness for EmbeddingHarness {
    type Provider = ProviderFixture;

    fn sample_input(&self) -> EmbeddingInput {
        EmbeddingInput {
            media_type: "text/plain".into(),
            body: b"fixture".to_vec(),
        }
    }

    fn compatible_spec(&self) -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: "i8".into(),
        }
    }

    fn expected_embedding(&self) -> Vec<u8> {
        vec![b'f', 0x20]
    }

    fn conforming_provider(&self) -> Self::Provider {
        ProviderFixture(ProviderMode::Good)
    }

    fn failing_provider(&self) -> Self::Provider {
        ProviderFixture(ProviderMode::Fail)
    }

    fn invalid_output_provider(&self) -> Self::Provider {
        ProviderFixture(ProviderMode::WrongLength)
    }
}

#[test]
fn downstream_crates_can_run_the_embedding_provider_suite() {
    run_embedding_provider_suite(&EmbeddingHarness).unwrap();
}
