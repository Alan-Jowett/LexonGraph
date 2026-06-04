// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::fmt;
use std::str;
use std::sync::Arc;

use async_openai::{
    Client,
    config::{AzureConfig, Config, OpenAIConfig},
    error::OpenAIError,
    types::embeddings::{
        CreateEmbeddingRequestArgs, Embedding, EmbeddingInput as OpenAiApiEmbeddingInput,
    },
};
use lexongraph_block::EmbeddingSpec;
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenAiCompatibleConfig {
    pub api_base: String,
    pub api_key: String,
    pub model: String,
    pub org_id: Option<String>,
    pub project_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AzureOpenAiConfig {
    pub api_base: String,
    pub api_key: String,
    pub deployment_id: String,
    pub api_version: String,
    pub model: String,
}

#[derive(Clone)]
pub struct OpenAiEmbeddingProvider {
    client: Client<Arc<dyn Config>>,
    model: String,
}

impl OpenAiEmbeddingProvider {
    pub fn from_config<C>(config: C, model: impl Into<String>) -> Self
    where
        C: Config + 'static,
    {
        Self {
            client: Client::with_config(Arc::new(config) as Arc<dyn Config>),
            model: model.into(),
        }
    }

    pub fn from_openai_compatible(config: OpenAiCompatibleConfig) -> Self {
        let mut client_config = OpenAIConfig::new()
            .with_api_base(config.api_base)
            .with_api_key(config.api_key);
        if let Some(org_id) = config.org_id {
            client_config = client_config.with_org_id(org_id);
        }
        if let Some(project_id) = config.project_id {
            client_config = client_config.with_project_id(project_id);
        }
        Self::from_config(client_config, config.model)
    }

    pub fn from_azure(config: AzureOpenAiConfig) -> Self {
        let client_config = AzureConfig::new()
            .with_api_base(config.api_base)
            .with_api_key(config.api_key)
            .with_deployment_id(config.deployment_id)
            .with_api_version(config.api_version);
        Self::from_config(client_config, config.model)
    }
}

#[derive(Debug)]
pub enum OpenAiEmbeddingProviderError {
    UnsupportedContent(String),
    InvalidUtf8(str::Utf8Error),
    UnsupportedEncoding(String),
    InvalidDimensions(u64),
    DimensionalityMismatch { expected: usize, actual: usize },
    UnexpectedEmbeddingCount(usize),
    UnexpectedEmbeddingIndex { index: u32, batch_len: usize },
    DuplicateEmbeddingIndex(u32),
    Request(OpenAIError),
}

impl fmt::Display for OpenAiEmbeddingProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedContent(message) => write!(f, "{message}"),
            Self::InvalidUtf8(error) => {
                write!(f, "resolved text content is not valid UTF-8: {error}")
            }
            Self::UnsupportedEncoding(encoding) => write!(
                f,
                "OpenAI-style embedding provider only supports f32le output in this revision, not {encoding}"
            ),
            Self::InvalidDimensions(dims) => {
                write!(f, "embedding_spec dims {dims} exceed supported size")
            }
            Self::DimensionalityMismatch { expected, actual } => write!(
                f,
                "embedding vector length {actual} does not match embedding_spec dims {expected}"
            ),
            Self::UnexpectedEmbeddingCount(actual) => {
                write!(
                    f,
                    "unexpected embedding count in the provider response: got {actual}"
                )
            }
            Self::UnexpectedEmbeddingIndex { index, batch_len } => write!(
                f,
                "embedding response index {index} is out of range for batch size {batch_len}"
            ),
            Self::DuplicateEmbeddingIndex(index) => {
                write!(f, "embedding response contains duplicate index {index}")
            }
            Self::Request(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for OpenAiEmbeddingProviderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidUtf8(error) => Some(error),
            Self::Request(error) => Some(error),
            Self::UnsupportedContent(_)
            | Self::UnsupportedEncoding(_)
            | Self::InvalidDimensions(_)
            | Self::DimensionalityMismatch { .. }
            | Self::UnexpectedEmbeddingCount(_)
            | Self::UnexpectedEmbeddingIndex { .. }
            | Self::DuplicateEmbeddingIndex(_) => None,
        }
    }
}

impl From<OpenAIError> for OpenAiEmbeddingProviderError {
    fn from(value: OpenAIError) -> Self {
        Self::Request(value)
    }
}

impl EmbeddingProvider for OpenAiEmbeddingProvider {
    type Error = OpenAiEmbeddingProviderError;

    async fn embed(
        &self,
        input: &EmbeddingInput,
        spec: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        let mut embeddings = self.embed_batch(std::slice::from_ref(input), spec).await?;
        match embeddings.pop() {
            Some(embedding) if embeddings.is_empty() => Ok(embedding),
            Some(_) | None => Err(OpenAiEmbeddingProviderError::UnexpectedEmbeddingCount(
                embeddings.len() + 1,
            )),
        }
    }

    async fn embed_batch(
        &self,
        inputs: &[EmbeddingInput],
        spec: &EmbeddingSpec,
    ) -> Result<Vec<Vec<u8>>, Self::Error> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        if spec.encoding != "f32le" {
            return Err(OpenAiEmbeddingProviderError::UnsupportedEncoding(
                spec.encoding.clone(),
            ));
        }

        let expected_dims = usize::try_from(spec.dims)
            .map_err(|_| OpenAiEmbeddingProviderError::InvalidDimensions(spec.dims))?;
        let texts = collect_text_inputs(inputs)?;
        let request = CreateEmbeddingRequestArgs::default()
            .model(&self.model)
            .input(OpenAiApiEmbeddingInput::StringArray(texts))
            .build()?;
        let response = self.client.embeddings().create(request).await?;
        map_response_embeddings(response.data, expected_dims, inputs.len())
    }
}

fn is_textual_media_type(media_type: &str) -> bool {
    let essence = media_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    essence.starts_with("text/")
        || essence == "application/json"
        || essence == "application/xml"
        || essence == "application/javascript"
        || essence == "application/x-www-form-urlencoded"
        || essence.ends_with("+json")
        || essence.ends_with("+xml")
}

fn encode_f32le(embedding: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(embedding));
    for value in embedding {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn collect_text_inputs(
    inputs: &[EmbeddingInput],
) -> Result<Vec<String>, OpenAiEmbeddingProviderError> {
    let mut texts = Vec::with_capacity(inputs.len());
    for input in inputs {
        if !is_textual_media_type(&input.media_type) {
            return Err(OpenAiEmbeddingProviderError::UnsupportedContent(format!(
                "resolved media type {:?} is not supported by the OpenAI-style embedding provider",
                input.media_type
            )));
        }
        texts.push(
            str::from_utf8(&input.body)
                .map_err(OpenAiEmbeddingProviderError::InvalidUtf8)?
                .to_owned(),
        );
    }
    Ok(texts)
}

fn map_response_embeddings(
    response_embeddings: Vec<Embedding>,
    expected_dims: usize,
    batch_len: usize,
) -> Result<Vec<Vec<u8>>, OpenAiEmbeddingProviderError> {
    if response_embeddings.len() != batch_len {
        return Err(OpenAiEmbeddingProviderError::UnexpectedEmbeddingCount(
            response_embeddings.len(),
        ));
    }

    let mut ordered_embeddings = vec![None; batch_len];
    for response_embedding in response_embeddings {
        let index = usize::try_from(response_embedding.index).map_err(|_| {
            OpenAiEmbeddingProviderError::UnexpectedEmbeddingIndex {
                index: response_embedding.index,
                batch_len,
            }
        })?;
        if index >= batch_len {
            return Err(OpenAiEmbeddingProviderError::UnexpectedEmbeddingIndex {
                index: response_embedding.index,
                batch_len,
            });
        }
        if ordered_embeddings[index].is_some() {
            return Err(OpenAiEmbeddingProviderError::DuplicateEmbeddingIndex(
                response_embedding.index,
            ));
        }
        if response_embedding.embedding.len() != expected_dims {
            return Err(OpenAiEmbeddingProviderError::DimensionalityMismatch {
                expected: expected_dims,
                actual: response_embedding.embedding.len(),
            });
        }
        ordered_embeddings[index] = Some(encode_f32le(&response_embedding.embedding));
    }

    ordered_embeddings
        .into_iter()
        .enumerate()
        .map(|(index, embedding)| {
            embedding.ok_or(OpenAiEmbeddingProviderError::UnexpectedEmbeddingIndex {
                index: index as u32,
                batch_len,
            })
        })
        .collect()
}
