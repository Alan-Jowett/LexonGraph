// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider, EmbeddingSpec};

#[derive(Clone, Copy)]
struct AsyncFixtureProvider;

impl EmbeddingProvider for AsyncFixtureProvider {
    type Error = FixtureError;

    async fn embed(
        &self,
        input: &EmbeddingInput,
        spec: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        let first = *input
            .body
            .first()
            .ok_or_else(|| FixtureError("expected non-empty input".into()))?;
        if input.media_type != "text/plain" {
            return Err(FixtureError("unexpected media type".into()));
        }
        if spec.encoding != "i8" || spec.dims != 2 {
            return Err(FixtureError("unexpected embedding spec".into()));
        }
        Ok(vec![first, input.body.len() as u8])
    }
}

#[test]
fn val_embed_trait_002_async_provider_realization_returns_compatible_bytes() {
    let provider = AsyncFixtureProvider;
    let input = EmbeddingInput {
        media_type: "text/plain".into(),
        body: b"fixture".to_vec(),
    };
    let embedding = pollster::block_on(provider.embed(
        &input,
        &EmbeddingSpec {
            dims: 2,
            encoding: "i8".into(),
        },
    ))
    .unwrap();

    assert_eq!(embedding, vec![b'f', 7]);
}

#[test]
fn val_embed_trait_002_default_batch_embedding_fallback_preserves_input_order() {
    let provider = AsyncFixtureProvider;
    let inputs = vec![
        EmbeddingInput {
            media_type: "text/plain".into(),
            body: b"alpha".to_vec(),
        },
        EmbeddingInput {
            media_type: "text/plain".into(),
            body: b"bravo".to_vec(),
        },
    ];
    let embeddings = pollster::block_on(provider.embed_batch(
        &inputs,
        &EmbeddingSpec {
            dims: 2,
            encoding: "i8".into(),
        },
    ))
    .unwrap();

    assert_eq!(embeddings, vec![vec![b'a', 5], vec![b'b', 5]]);
}

#[test]
fn val_embed_trait_014_empty_batch_returns_empty_result() {
    let provider = AsyncFixtureProvider;
    let embeddings = pollster::block_on(provider.embed_batch(
        &[],
        &EmbeddingSpec {
            dims: 2,
            encoding: "i8".into(),
        },
    ))
    .unwrap();

    assert!(embeddings.is_empty());
}

#[derive(Clone, Debug)]
struct FixtureError(String);

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for FixtureError {}
