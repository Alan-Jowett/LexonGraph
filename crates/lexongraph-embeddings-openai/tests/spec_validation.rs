// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use httpmock::prelude::*;
use lexongraph_block::EmbeddingSpec;
use lexongraph_embeddings_openai::{
    AzureOpenAiConfig, OpenAiCompatibleConfig, OpenAiEmbeddingProvider,
    OpenAiEmbeddingProviderError,
};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};

#[tokio::test(flavor = "current_thread")]
async fn val_embed_oai_002_successful_openai_compatible_batch_request_returns_spec_bytes() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/embeddings")
            .header("authorization", "Bearer test-key")
            .body_includes("\"model\":\"test-model\"")
            .body_includes("\"input\":[\"hello world\",\"goodbye world\"]");
        then.status(200)
            .header("content-type", "application/json")
            .body(response_body(&[&[1.0_f32, 2.5_f32], &[3.0_f32, 4.5_f32]]));
    });

    let provider = OpenAiEmbeddingProvider::from_openai_compatible(OpenAiCompatibleConfig {
        api_base: format!("{}/v1", server.base_url()),
        api_key: "test-key".into(),
        model: "test-model".into(),
        org_id: None,
        project_id: None,
    });

    let embeddings = provider
        .embed_batch(
            &[
                text_input("text/plain", "hello world"),
                text_input("text/plain", "goodbye world"),
            ],
            &embedding_spec(2, "f32le"),
        )
        .await
        .unwrap();

    mock.assert();
    assert_eq!(mock.calls(), 1);
    assert_eq!(
        embeddings,
        vec![
            [1.0_f32.to_le_bytes(), 2.5_f32.to_le_bytes()].concat(),
            [3.0_f32.to_le_bytes(), 4.5_f32.to_le_bytes()].concat(),
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_embed_oai_003_azure_configuration_targets_azure_style_endpoint() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/openai/deployments/deploy-1/embeddings")
            .query_param("api-version", "2024-06-01")
            .header("api-key", "azure-key")
            .body_includes("\"model\":\"embedding-deployment\"")
            .body_includes("\"input\":[\"azure text\"]");
        then.status(200)
            .header("content-type", "application/json")
            .body(success_body(&[0.5_f32, -1.5_f32]));
    });

    let provider = OpenAiEmbeddingProvider::from_azure(AzureOpenAiConfig {
        api_base: server.base_url(),
        api_key: "azure-key".into(),
        deployment_id: "deploy-1".into(),
        api_version: "2024-06-01".into(),
        model: "embedding-deployment".into(),
    });

    let embedding = provider
        .embed(
            &text_input("text/plain", "azure text"),
            &embedding_spec(2, "f32le"),
        )
        .await
        .unwrap();

    mock.assert();
    assert_eq!(mock.calls(), 1);
    assert_eq!(
        embedding,
        [0.5_f32.to_le_bytes(), (-1.5_f32).to_le_bytes()].concat()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_embed_oai_003_openai_compatible_request_identity_is_forwarded() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/embeddings")
            .header("authorization", "Bearer test-key")
            .header("openai-organization", "test-org")
            .header("openai-project", "test-project")
            .body_includes("\"model\":\"test-model\"")
            .body_includes("\"input\":[\"hello identity\"]");
        then.status(200)
            .header("content-type", "application/json")
            .body(success_body(&[0.25_f32, 0.75_f32]));
    });

    let provider = OpenAiEmbeddingProvider::from_openai_compatible(OpenAiCompatibleConfig {
        api_base: format!("{}/v1", server.base_url()),
        api_key: "test-key".into(),
        model: "test-model".into(),
        org_id: Some("test-org".into()),
        project_id: Some("test-project".into()),
    });

    let embedding = provider
        .embed(
            &text_input("text/plain", "hello identity"),
            &embedding_spec(2, "f32le"),
        )
        .await
        .unwrap();

    mock.assert();
    assert_eq!(mock.calls(), 1);
    assert_eq!(
        embedding,
        [0.25_f32.to_le_bytes(), 0.75_f32.to_le_bytes()].concat()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_embed_oai_004_non_text_and_invalid_utf8_content_fail_explicitly() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST).path("/v1/embeddings");
        then.status(500);
    });

    let provider = OpenAiEmbeddingProvider::from_openai_compatible(OpenAiCompatibleConfig {
        api_base: format!("{}/v1", server.base_url()),
        api_key: "test-key".into(),
        model: "test-model".into(),
        org_id: None,
        project_id: None,
    });

    let non_text = provider
        .embed(
            &EmbeddingInput {
                media_type: "application/octet-stream".into(),
                body: vec![0xde, 0xad, 0xbe, 0xef],
            },
            &embedding_spec(2, "f32le"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        non_text,
        OpenAiEmbeddingProviderError::UnsupportedContent(_)
    ));

    let invalid_utf8 = provider
        .embed(
            &EmbeddingInput {
                media_type: "text/plain".into(),
                body: vec![0xff, 0xfe],
            },
            &embedding_spec(2, "f32le"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        invalid_utf8,
        OpenAiEmbeddingProviderError::InvalidUtf8(_)
    ));

    assert_eq!(mock.calls(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn val_embed_oai_004_batch_fails_before_request_if_any_input_is_invalid() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST).path("/v1/embeddings");
        then.status(500);
    });

    let provider = OpenAiEmbeddingProvider::from_openai_compatible(OpenAiCompatibleConfig {
        api_base: format!("{}/v1", server.base_url()),
        api_key: "test-key".into(),
        model: "test-model".into(),
        org_id: None,
        project_id: None,
    });

    let invalid_media_type = provider
        .embed_batch(
            &[
                text_input("text/plain", "hello"),
                EmbeddingInput {
                    media_type: "application/octet-stream".into(),
                    body: vec![0xde, 0xad],
                },
            ],
            &embedding_spec(2, "f32le"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        invalid_media_type,
        OpenAiEmbeddingProviderError::UnsupportedContent(_)
    ));

    let invalid_utf8 = provider
        .embed_batch(
            &[
                text_input("text/plain", "hello"),
                EmbeddingInput {
                    media_type: "text/plain".into(),
                    body: vec![0xff, 0xfe],
                },
            ],
            &embedding_spec(2, "f32le"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        invalid_utf8,
        OpenAiEmbeddingProviderError::InvalidUtf8(_)
    ));

    assert_eq!(mock.calls(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn val_embed_oai_005_incompatible_embedding_spec_fails_explicitly() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST).path("/v1/embeddings");
        then.status(200)
            .header("content-type", "application/json")
            .body(success_body(&[1.0_f32]));
    });

    let provider = OpenAiEmbeddingProvider::from_openai_compatible(OpenAiCompatibleConfig {
        api_base: format!("{}/v1", server.base_url()),
        api_key: "test-key".into(),
        model: "test-model".into(),
        org_id: None,
        project_id: None,
    });

    let unsupported_encoding = provider
        .embed(&text_input("text/plain", "hello"), &embedding_spec(2, "i8"))
        .await
        .unwrap_err();
    assert!(matches!(
        unsupported_encoding,
        OpenAiEmbeddingProviderError::UnsupportedEncoding(_)
    ));
    assert_eq!(mock.calls(), 0);

    let mismatched_dims = provider
        .embed(
            &text_input("text/plain", "hello"),
            &embedding_spec(2, "f32le"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        mismatched_dims,
        OpenAiEmbeddingProviderError::DimensionalityMismatch { .. }
    ));
    assert_eq!(mock.calls(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn val_embed_oai_007_batch_response_cardinality_mismatch_fails_explicitly() {
    let empty_server = MockServer::start();
    let empty_mock = empty_server.mock(|when, then| {
        when.method(POST)
            .path("/v1/embeddings")
            .header("authorization", "Bearer test-key")
            .body_includes("\"model\":\"test-model\"")
            .body_includes("\"input\":[\"empty response\",\"second input\"]");
        then.status(200)
            .header("content-type", "application/json")
            .body(success_body(&[1.0_f32, 2.0_f32]));
    });

    let empty_provider = OpenAiEmbeddingProvider::from_openai_compatible(OpenAiCompatibleConfig {
        api_base: format!("{}/v1", empty_server.base_url()),
        api_key: "test-key".into(),
        model: "test-model".into(),
        org_id: None,
        project_id: None,
    });

    let empty_error = empty_provider
        .embed_batch(
            &[
                text_input("text/plain", "empty response"),
                text_input("text/plain", "second input"),
            ],
            &embedding_spec(2, "f32le"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        empty_error,
        OpenAiEmbeddingProviderError::UnexpectedEmbeddingCount(1)
    ));
    empty_mock.assert();
    assert_eq!(empty_mock.calls(), 1);

    let multi_server = MockServer::start();
    let multi_mock = multi_server.mock(|when, then| {
        when.method(POST)
            .path("/v1/embeddings")
            .header("authorization", "Bearer test-key")
            .body_includes("\"model\":\"test-model\"")
            .body_includes("\"input\":[\"multi response\",\"second multi\"]");
        then.status(200)
            .header("content-type", "application/json")
            .body(response_body(&[
                &[1.0_f32, 2.0_f32],
                &[3.0_f32, 4.0_f32],
                &[5.0_f32, 6.0_f32],
            ]));
    });

    let multi_provider = OpenAiEmbeddingProvider::from_openai_compatible(OpenAiCompatibleConfig {
        api_base: format!("{}/v1", multi_server.base_url()),
        api_key: "test-key".into(),
        model: "test-model".into(),
        org_id: None,
        project_id: None,
    });

    let multi_error = multi_provider
        .embed_batch(
            &[
                text_input("text/plain", "multi response"),
                text_input("text/plain", "second multi"),
            ],
            &embedding_spec(2, "f32le"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        multi_error,
        OpenAiEmbeddingProviderError::UnexpectedEmbeddingCount(3)
    ));
    multi_mock.assert();
    assert_eq!(multi_mock.calls(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn val_embed_oai_008_batch_results_preserve_logical_input_order() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/embeddings")
            .header("authorization", "Bearer test-key")
            .body_includes("\"model\":\"test-model\"")
            .body_includes("\"input\":[\"alpha\",\"bravo\"]");
        then.status(200)
            .header("content-type", "application/json")
            .body(indexed_response_body(&[
                (1, &[3.0_f32, 4.0_f32]),
                (0, &[1.0_f32, 2.0_f32]),
            ]));
    });

    let provider = OpenAiEmbeddingProvider::from_openai_compatible(OpenAiCompatibleConfig {
        api_base: format!("{}/v1", server.base_url()),
        api_key: "test-key".into(),
        model: "test-model".into(),
        org_id: None,
        project_id: None,
    });

    let embeddings = provider
        .embed_batch(
            &[
                text_input("text/plain", "alpha"),
                text_input("text/plain", "bravo"),
            ],
            &embedding_spec(2, "f32le"),
        )
        .await
        .unwrap();

    mock.assert();
    assert_eq!(
        embeddings,
        vec![
            [1.0_f32.to_le_bytes(), 2.0_f32.to_le_bytes()].concat(),
            [3.0_f32.to_le_bytes(), 4.0_f32.to_le_bytes()].concat(),
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_embed_oai_009_empty_batch_returns_empty_without_request() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST).path("/v1/embeddings");
        then.status(500);
    });

    let provider = OpenAiEmbeddingProvider::from_openai_compatible(OpenAiCompatibleConfig {
        api_base: format!("{}/v1", server.base_url()),
        api_key: "test-key".into(),
        model: "test-model".into(),
        org_id: None,
        project_id: None,
    });

    let embeddings = provider
        .embed_batch(&[], &embedding_spec(2, "f32le"))
        .await
        .unwrap();

    assert!(embeddings.is_empty());
    assert_eq!(mock.calls(), 0);
}

fn embedding_spec(dims: u64, encoding: &str) -> EmbeddingSpec {
    EmbeddingSpec {
        dims,
        encoding: encoding.into(),
    }
}

fn text_input(media_type: &str, body: &str) -> EmbeddingInput {
    EmbeddingInput {
        media_type: media_type.into(),
        body: body.as_bytes().to_vec(),
    }
}

fn success_body(embedding: &[f32]) -> String {
    response_body(&[embedding])
}

fn response_body(embeddings: &[&[f32]]) -> String {
    let indexed = embeddings
        .iter()
        .enumerate()
        .map(|(index, embedding)| (index as u32, *embedding))
        .collect::<Vec<_>>();
    indexed_response_body(&indexed)
}

fn indexed_response_body(embeddings: &[(u32, &[f32])]) -> String {
    let data = embeddings
        .iter()
        .map(|(index, embedding)| {
            let values = embedding
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(",");
            format!(r#"{{"object":"embedding","index":{index},"embedding":[{values}]}}"#)
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"{{"object":"list","data":[{data}],"model":"test-model","usage":{{"prompt_tokens":1,"total_tokens":1}}}}"#
    )
}
