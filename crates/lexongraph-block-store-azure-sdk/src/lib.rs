// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Azure Blob Storage `BlockStore` implementation for LexonGraph blocks using the Azure SDK.

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use azure_core::error::ErrorKind;
use azure_core::http::{
    ClientOptions, ExponentialRetryOptions, RequestContent, RetryOptions, StatusCode, Url,
};
use azure_core::time::Duration;
use azure_storage_blob::clients::BlobContainerClientOptions;
use azure_storage_blob::models::{BlockBlobClientUploadOptions, StorageErrorCode};
use azure_storage_blob::{BlobClient, BlobContainerClient};
use futures::TryStreamExt;
use lexongraph_block::BlockHash;
use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};

#[derive(Clone)]
pub struct AzureBlobBlockStore {
    container_client: Arc<BlobContainerClient>,
    container_display: String,
}

impl std::fmt::Debug for AzureBlobBlockStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureBlobBlockStore")
            .field("container", &self.container_display)
            .finish()
    }
}

impl AzureBlobBlockStore {
    pub fn new(container_sas_url: &str) -> Result<Self, BlockStoreError> {
        let (container_url, container_display, _) = normalize_container_url(container_sas_url)?;
        let container_client = BlobContainerClient::new(
            container_url.clone(),
            None,
            Some(Self::blob_container_client_options()),
        )
        .map_err(|error| {
            backend_failure(format!(
                "failed to prepare Azure Blob client for container {}: {}",
                container_display,
                describe_azure_error(&error)
            ))
        })?;
        Ok(Self {
            container_client: Arc::new(container_client),
            container_display,
        })
    }

    fn block_blob_name(block_id: &BlockHash) -> String {
        let hex = block_id.to_string();
        format!("{}/{}/{}.cbor", &hex[..2], &hex[2..4], hex)
    }

    fn blob_container_client_options() -> BlobContainerClientOptions {
        let client_options = ClientOptions {
            retry: RetryOptions::exponential(ExponentialRetryOptions {
                initial_delay: Duration::milliseconds(200),
                max_retries: 5,
                max_total_elapsed: Duration::seconds(5),
                ..Default::default()
            }),
            ..Default::default()
        };
        BlobContainerClientOptions {
            client_options,
            ..Default::default()
        }
    }

    fn blob_client(&self, blob_name: &str) -> BlobClient {
        self.container_client.blob_client(blob_name)
    }
}

#[async_trait]
impl BlockStore for AzureBlobBlockStore {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        let blob_name = Self::block_blob_name(block_id);
        let blob_client = self.blob_client(&blob_name);
        let content = RequestContent::from(block_bytes.to_vec());
        let options = BlockBlobClientUploadOptions::default().if_not_exists();

        blob_client
            .upload(content, Some(options))
            .await
            .map(|_| ())
            .or_else(
                |error| match (error.http_status(), storage_error_code(&error)) {
                    (Some(StatusCode::Conflict | StatusCode::PreconditionFailed), _)
                    | (
                        _,
                        Some(
                            StorageErrorCode::BlobAlreadyExists | StorageErrorCode::ConditionNotMet,
                        ),
                    ) => Ok(()),
                    _ => Err(backend_failure(format!(
                        "failed to publish block {} to blob {} in container {}: {}",
                        block_id,
                        blob_name,
                        self.container_display,
                        describe_azure_error(&error)
                    ))),
                },
            )
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let blob_name = Self::block_blob_name(block_id);
        let blob_client = self.blob_client(&blob_name);

        let response = match blob_client.download(None).await {
            Ok(response) => response,
            Err(error) if storage_error_code(&error) == Some(StorageErrorCode::BlobNotFound) => {
                return Ok(None);
            }
            Err(error) => {
                return Err(backend_failure(format!(
                    "failed to read block {} from blob {} in container {}: {}",
                    block_id,
                    blob_name,
                    self.container_display,
                    describe_azure_error(&error)
                )));
            }
        };
        let body = response.body.collect().await.map_err(|error| {
            backend_failure(format!(
                "failed to read block {} from blob {} in container {}: {}",
                block_id,
                blob_name,
                self.container_display,
                describe_azure_error(&error)
            ))
        })?;
        Ok(Some(body.to_vec()))
    }

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
        let blobs = self.container_client.list_blobs(None).map_err(|error| {
            backend_failure(format!(
                "failed to list Azure container {}: {}",
                self.container_display,
                describe_azure_error(&error)
            ))
        })?;
        let container_display = self.container_display.clone();
        Ok(Box::pin(
            blobs
                .map_err(move |error| {
                    backend_failure(format!(
                        "failed to list Azure container {}: {}",
                        container_display,
                        describe_azure_error(&error)
                    ))
                })
                .and_then(|blob| async move {
                    let Some(name) = blob.name else {
                        return Ok(None);
                    };
                    decode_recognized_block_blob_name(&name).map_err(backend_failure)
                })
                .try_filter_map(|block_id| async move { Ok(block_id) }),
        ))
    }
}

fn normalize_container_url(
    container_sas_url: &str,
) -> Result<(Url, String, String), BlockStoreError> {
    let mut url = Url::parse(container_sas_url).map_err(|error| {
        backend_failure(format!(
            "failed to parse Azure Blob container SAS URL: {error}"
        ))
    })?;
    url.set_fragment(None);
    if url.query().is_none_or(str::is_empty) {
        return Err(backend_failure(
            "Azure Blob container SAS URL must include SAS query parameters".into(),
        ));
    }
    if !has_non_empty_query_param(&url, "sig") {
        return Err(backend_failure(
            "Azure Blob container SAS URL must include a non-empty SAS signature parameter".into(),
        ));
    }

    let path_segments = url
        .path_segments()
        .ok_or_else(|| backend_failure("Azure Blob container SAS URL must be hierarchical".into()))?
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if path_segments.len() != 1 {
        return Err(backend_failure(format!(
            "Azure Blob SAS URL must address a container root, got path {}",
            url.path()
        )));
    }

    let container_path = format!("/{}", path_segments[0]);
    url.set_path(&container_path);
    let container_display = redact_url(&url);
    Ok((url, container_display, container_path))
}

fn redact_url(url: &Url) -> String {
    let mut redacted = url.clone();
    redacted.set_query(None);
    redacted.set_fragment(None);
    redacted.to_string()
}

fn has_non_empty_query_param(url: &Url, name: &str) -> bool {
    url.query_pairs()
        .any(|(candidate, value)| candidate == name && !value.is_empty())
}

fn backend_failure(message: String) -> BlockStoreError {
    BlockStoreError::BackendFailure(message)
}

fn decode_recognized_block_blob_name(value: &str) -> Result<Option<BlockHash>, String> {
    let mut components = value.split('/');
    let Some(first_level) = components.next() else {
        return Ok(None);
    };
    let Some(second_level) = components.next() else {
        return Ok(None);
    };
    let Some(file_name) = components.next() else {
        return Ok(None);
    };
    if components.next().is_some() {
        return Ok(None);
    }

    if !is_lower_hex_prefix(first_level) || !is_lower_hex_prefix(second_level) {
        return Ok(None);
    }

    let Some(hex) = file_name.strip_suffix(".cbor") else {
        return Ok(None);
    };
    let bytes = decode_block_hash_hex(hex).ok_or_else(|| {
        format!("failed to decode an enumerated block ID candidate at blob {value}")
    })?;
    if &hex[..2] != first_level || &hex[2..4] != second_level {
        return Err(format!(
            "failed to decode an enumerated block ID candidate at blob {value}: shard prefix mismatch"
        ));
    }

    Ok(Some(BlockHash::from_bytes(bytes)))
}

fn decode_block_hash_hex(value: &str) -> Option<[u8; BlockHash::LEN]> {
    if value.len() != BlockHash::LEN * 2 {
        return None;
    }

    let mut bytes = [0_u8; BlockHash::LEN];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(chunk[0])?;
        let low = decode_hex_nibble(chunk[1])?;
        bytes[index] = (high << 4) | low;
    }

    Some(bytes)
}

fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn is_lower_hex_prefix(value: &str) -> bool {
    value.len() == 2 && value.bytes().all(|byte| decode_hex_nibble(byte).is_some())
}

fn storage_error_code(error: &azure_core::Error) -> Option<StorageErrorCode> {
    match error.kind() {
        ErrorKind::HttpResponse {
            error_code: Some(error_code),
            ..
        } => StorageErrorCode::from_str(error_code.as_ref()).ok(),
        _ => None,
    }
}

fn describe_azure_error(error: &azure_core::Error) -> String {
    if let Some(status) = error.http_status() {
        let mut description = format_http_status(status);
        if let ErrorKind::HttpResponse {
            error_code: Some(error_code),
            ..
        } = error.kind()
        {
            description.push_str(&format!(" ({error_code})"));
        }
        return description;
    }
    error.to_string()
}

fn format_http_status(status: StatusCode) -> String {
    format!("HTTP {status} {}", status.canonical_reason())
}
