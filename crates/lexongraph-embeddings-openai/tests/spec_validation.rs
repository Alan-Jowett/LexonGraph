use httpmock::prelude::*;
use lexongraph_block::EmbeddingSpec;
use lexongraph_embeddings_openai::{
    AzureOpenAiConfig, OpenAiCompatibleConfig, OpenAiEmbeddingProvider,
    OpenAiEmbeddingProviderError,
};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};

#[tokio::test(flavor = "current_thread")]
async fn val_embed_oai_002_successful_openai_compatible_request_returns_spec_bytes() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/embeddings")
            .header("authorization", "Bearer test-key")
            .body_includes("\"model\":\"test-model\"")
            .body_includes("\"input\":\"hello world\"");
        then.status(200)
            .header("content-type", "application/json")
            .body(success_body(&[1.0_f32, 2.5_f32]));
    });

    let provider = OpenAiEmbeddingProvider::from_openai_compatible(OpenAiCompatibleConfig {
        api_base: format!("{}/v1", server.base_url()),
        api_key: "test-key".into(),
        model: "test-model".into(),
        org_id: None,
        project_id: None,
    });

    let embedding = provider
        .embed(
            &text_input("text/plain", "hello world"),
            &embedding_spec(2, "f32le"),
        )
        .await
        .unwrap();

    mock.assert();
    assert_eq!(mock.calls(), 1);
    assert_eq!(
        embedding,
        [1.0_f32.to_le_bytes(), 2.5_f32.to_le_bytes()].concat()
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
            .body_includes("\"input\":\"azure text\"");
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
    let values = embedding
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"{{"object":"list","data":[{{"object":"embedding","index":0,"embedding":[{values}]}}],"model":"test-model","usage":{{"prompt_tokens":1,"total_tokens":1}}}}"#
    )
}
