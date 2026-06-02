//! Shared LexonGraph embedding-provider contract.
//!
//! ```compile_fail
//! #[cfg(feature = "conformance")]
//! compile_error!("the conformance module is intentionally enabled in this doctest configuration");
//!
//! use lexongraph_embeddings_trait::conformance;
//!
//! let _ = std::any::type_name::<conformance::ConformanceError>();
//! ```

#[cfg(feature = "conformance")]
use std::fmt;

pub use lexongraph_block::EmbeddingSpec;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingInput {
    pub media_type: String,
    pub body: Vec<u8>,
}

#[allow(async_fn_in_trait)]
pub trait EmbeddingProvider {
    type Error: std::error::Error;

    async fn embed(
        &self,
        input: &EmbeddingInput,
        spec: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error>;
}

#[cfg(feature = "conformance")]
mod conformance_support {
    use super::*;

    pub type ConformanceResult = Result<(), ConformanceError>;

    #[derive(Debug)]
    pub enum ConformanceError {
        Provider(String),
        Expectation(String),
    }

    impl fmt::Display for ConformanceError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Provider(message) => write!(f, "embedding provider failed: {message}"),
                Self::Expectation(message) => {
                    write!(f, "conformance expectation failed: {message}")
                }
            }
        }
    }

    impl std::error::Error for ConformanceError {}

    pub trait EmbeddingProviderConformanceHarness {
        type Provider: EmbeddingProvider;

        fn sample_input(&self) -> EmbeddingInput;
        fn compatible_spec(&self) -> EmbeddingSpec;
        fn expected_embedding(&self) -> Vec<u8>;
        fn conforming_provider(&self) -> Self::Provider;
        fn failing_provider(&self) -> Self::Provider;
        fn invalid_output_provider(&self) -> Self::Provider;
    }

    pub fn run_embedding_provider_suite<H>(harness: &H) -> ConformanceResult
    where
        H: EmbeddingProviderConformanceHarness,
    {
        pollster::block_on(async {
            let input = harness.sample_input();
            let spec = harness.compatible_spec();

            let embedding = harness
                .conforming_provider()
                .embed(&input, &spec)
                .await
                .map_err(|error| ConformanceError::Provider(error.to_string()))?;
            validate_embedding_bytes(&embedding, &spec).map_err(ConformanceError::Expectation)?;
            if embedding != harness.expected_embedding() {
                return Err(ConformanceError::Expectation(format!(
                    "expected embedding {:?}, got {:?}",
                    harness.expected_embedding(),
                    embedding
                )));
            }

            let failure = harness.failing_provider().embed(&input, &spec).await;
            if let Ok(embedding) = failure {
                return Err(ConformanceError::Expectation(format!(
                    "expected provider failure, got successful embedding {embedding:?}"
                )));
            }

            let invalid_output = harness
                .invalid_output_provider()
                .embed(&input, &spec)
                .await
                .map_err(|error| ConformanceError::Provider(error.to_string()))?;
            match validate_embedding_bytes(&invalid_output, &spec) {
                Ok(()) => Err(ConformanceError::Expectation(
                    "expected invalid-output provider to violate embedding_spec length".into(),
                )),
                Err(_) => Ok(()),
            }
        })
    }

    fn validate_embedding_bytes(embedding: &[u8], spec: &EmbeddingSpec) -> Result<(), String> {
        let expected = expected_embedding_len(spec).ok_or_else(|| {
            format!(
                "unsupported embedding encoding {:?} for conformance validation",
                spec.encoding
            )
        })?;
        if embedding.len() != expected {
            return Err(format!(
                "embedding length {} does not match expected length {expected} for {} dims under {}",
                embedding.len(),
                spec.dims,
                spec.encoding
            ));
        }
        Ok(())
    }

    fn expected_embedding_len(spec: &EmbeddingSpec) -> Option<usize> {
        let dims = usize::try_from(spec.dims).ok()?;
        match spec.encoding.as_str() {
            "f32le" => dims.checked_mul(4),
            "f16le" => dims.checked_mul(2),
            "i8" => Some(dims),
            "pq4" => dims.checked_add(1).map(|value| value / 2),
            _ => None,
        }
    }

    #[derive(Clone, Debug)]
    pub struct FixtureError(pub String);

    impl fmt::Display for FixtureError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for FixtureError {}
}

#[cfg(feature = "conformance")]
pub mod conformance {
    //! Opt-in helper APIs for validating downstream embedding-provider implementations.
    //!
    //! Enable this module from test code with a dev-dependency such as:
    //!
    //! ```toml
    //! [dev-dependencies]
    //! lexongraph-embeddings-trait = { version = "*", features = ["conformance"] }
    //! ```

    pub use super::conformance_support::{
        ConformanceError, ConformanceResult, EmbeddingProviderConformanceHarness, FixtureError,
        run_embedding_provider_suite,
    };
}
