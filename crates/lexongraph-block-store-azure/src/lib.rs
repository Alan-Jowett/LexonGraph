// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Azure Blob Storage `BlockStore` implementation for LexonGraph blocks.

use std::fmt;
use std::time::Duration;

use lexongraph_block::{
    Block, BlockError, BlockHash, ValidatedBlock, deserialize_block, serialize_block,
};
use lexongraph_block_store::{BlockIdIterator, BlockStore, BlockStoreError};
use quick_xml::de::from_str;
use reqwest::StatusCode;
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_TYPE, IF_NONE_MATCH};
use reqwest::{Method, Url};
use serde::Deserialize;

const AZURE_API_VERSION: &str = "2023-11-03";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct AzureBlobBlockStore {
    client: Client,
    container_url: Url,
    container_display: String,
    container_path: String,
}

impl fmt::Debug for AzureBlobBlockStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureBlobBlockStore")
            .field("container", &self.container_display)
            .finish()
    }
}

impl AzureBlobBlockStore {
    pub fn new(container_sas_url: &str) -> Result<Self, BlockStoreError> {
        let (container_url, container_display, container_path) =
            normalize_container_url(container_sas_url)?;
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|error| {
                backend_failure(format!(
                    "failed to prepare Azure Blob client for container {}: {error}",
                    container_display
                ))
            })?;

        Ok(Self {
            client,
            container_url,
            container_display,
            container_path,
        })
    }

    fn block_blob_name(block_id: &BlockHash) -> String {
        let hex = block_id.to_string();
        format!("{}/{}/{}.cbor", &hex[..2], &hex[2..4], hex)
    }

    fn build_blob_url(&self, blob_name: &str) -> Url {
        let mut url = self.container_url.clone();
        url.set_path(&format!("{}/{blob_name}", self.container_path));
        url
    }

    fn fetch_blob_bytes(&self, blob_name: &str) -> Result<Option<Vec<u8>>, String> {
        let response = self
            .request(Method::GET, self.build_blob_url(blob_name))
            .send()
            .map_err(|error| format!("request failed: {error}"))?;
        let header_error_code = azure_error_code_header(&response);

        match response.status() {
            StatusCode::OK => response
                .bytes()
                .map(|bytes| Some(bytes.to_vec()))
                .map_err(|error| format!("response body read failed: {error}")),
            StatusCode::NOT_FOUND => {
                let body = response
                    .bytes()
                    .map_err(|error| format!("response body read failed: {error}"))?;
                let error_code = header_error_code.or_else(|| parse_azure_error_code_body(&body));
                if error_code.as_deref() == Some("BlobNotFound") {
                    Ok(None)
                } else {
                    Err(format!(
                        "backend returned {}{}",
                        format_http_status(StatusCode::NOT_FOUND),
                        format_azure_error_code(error_code.as_deref())
                    ))
                }
            }
            status => {
                let _ = response.bytes();
                Err(format!(
                    "backend returned {}{}",
                    format_http_status(status),
                    format_azure_error_code(header_error_code.as_deref())
                ))
            }
        }
    }

    fn list_blob_page(&self, marker: &str) -> Result<ListPage, BlockStoreError> {
        let mut url = self.container_url.clone();
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("restype", "container");
            query.append_pair("comp", "list");
            if !marker.is_empty() {
                query.append_pair("marker", marker);
            }
        }

        let response = self.request(Method::GET, url).send().map_err(|error| {
            backend_failure(format!(
                "failed to list Azure container {}: {error}",
                self.container_display
            ))
        })?;

        if response.status() != StatusCode::OK {
            return Err(backend_failure(format!(
                "failed to list Azure container {}: {}",
                self.container_display,
                format_http_status(response.status())
            )));
        }

        let body = response.text().map_err(|error| {
            backend_failure(format!(
                "failed to read Azure listing response from {}: {error}",
                self.container_display
            ))
        })?;
        let listing: EnumerationResults = from_str(&body).map_err(|error| {
            backend_failure(format!(
                "failed to decode Azure listing response from {}: {error}",
                self.container_display
            ))
        })?;

        let names = listing
            .blobs
            .map(|blobs| blobs.blobs.into_iter().map(|blob| blob.name).collect())
            .unwrap_or_default();

        Ok(ListPage {
            names,
            next_marker: listing.next_marker.unwrap_or_default(),
        })
    }
    fn request(&self, method: Method, url: Url) -> reqwest::blocking::RequestBuilder {
        self.client
            .request(method, url)
            .header("x-ms-version", AZURE_API_VERSION)
    }
}

impl BlockStore for AzureBlobBlockStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        let serialized = serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
        let block_id = serialized.hash;
        let blob_name = Self::block_blob_name(&block_id);
        let response = self
            .request(Method::PUT, self.build_blob_url(&blob_name))
            .header("x-ms-blob-type", "BlockBlob")
            .header(IF_NONE_MATCH, "*")
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(serialized.bytes)
            .send()
            .map_err(|error| {
                backend_failure(format!(
                    "failed to publish block {} to blob {} in container {}: {error}",
                    block_id, blob_name, self.container_display
                ))
            })?;

        let status = response.status();
        let _ = response.bytes();

        if status.is_success() {
            return Ok(block_id);
        }

        if matches!(
            status,
            StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED
        ) {
            let canonical = serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
            return self.read_existing_or_map_publish_error(
                &blob_name,
                &block_id,
                &canonical.bytes,
                status,
            );
        }

        Err(backend_failure(format!(
            "failed to publish block {} to blob {} in container {}: {}",
            block_id,
            blob_name,
            self.container_display,
            format_http_status(status)
        )))
    }

    fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        let blob_name = Self::block_blob_name(block_id);
        let bytes = self.fetch_blob_bytes(&blob_name).map_err(|error| {
            backend_failure(format!(
                "failed to read block {} from blob {} in container {}: {error}",
                block_id, blob_name, self.container_display
            ))
        })?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };

        deserialize_block(&bytes, block_id)
            .map(Some)
            .map_err(map_get_error)
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        Ok(Box::new(AzureBlockIdIterator::new(self)))
    }
}

impl AzureBlobBlockStore {
    fn read_existing_or_map_publish_error(
        &self,
        blob_name: &str,
        block_id: &BlockHash,
        canonical_bytes: &[u8],
        status: StatusCode,
    ) -> Result<BlockHash, BlockStoreError> {
        match self.fetch_blob_bytes(blob_name) {
            Ok(Some(existing_bytes)) if existing_bytes == canonical_bytes => Ok(*block_id),
            Ok(Some(_)) => Err(backend_failure(format!(
                "integrity conflict at blob {} in container {} for block {} after publish error {}",
                blob_name,
                self.container_display,
                block_id,
                format_http_status(status)
            ))),
            Ok(None) => Err(backend_failure(format!(
                "failed to publish block {} to blob {} in container {}: blob missing after publish error {}",
                block_id,
                blob_name,
                self.container_display,
                format_http_status(status)
            ))),
            Err(error) => Err(backend_failure(format!(
                "failed to inspect blob {} in container {} after publish error {} for block {}: {error}",
                blob_name,
                self.container_display,
                format_http_status(status),
                block_id
            ))),
        }
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
    if url.query().is_none() {
        return Err(backend_failure(
            "Azure Blob container SAS URL must include SAS query parameters".into(),
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

fn backend_failure(message: String) -> BlockStoreError {
    BlockStoreError::BackendFailure(message)
}

fn map_get_error(error: BlockError) -> BlockStoreError {
    match error {
        BlockError::HashMismatch { expected, actual } => {
            BlockStoreError::IntegrityMismatch { expected, actual }
        }
        other => BlockStoreError::MalformedContent(other),
    }
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
    let bytes = decode_block_hash_hex(hex)
        .ok_or_else(|| "failed to decode an enumerated block ID candidate".to_string())?;
    if &hex[..2] != first_level || &hex[2..4] != second_level {
        return Err(
            "failed to decode an enumerated block ID candidate: shard prefix mismatch".into(),
        );
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

fn format_http_status(status: StatusCode) -> String {
    match status.canonical_reason() {
        Some(reason) => format!("HTTP {} {}", status.as_u16(), reason),
        None => format!("HTTP {}", status.as_u16()),
    }
}

fn azure_error_code_header(response: &reqwest::blocking::Response) -> Option<String> {
    response
        .headers()
        .get("x-ms-error-code")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn parse_azure_error_code_body(body: &[u8]) -> Option<String> {
    let body = std::str::from_utf8(body).ok()?;
    let start = body.find("<Code>")?;
    let rest = &body[start + "<Code>".len()..];
    let end = rest.find("</Code>")?;
    Some(rest[..end].to_string())
}

fn format_azure_error_code(error_code: Option<&str>) -> String {
    match error_code {
        Some(error_code) => format!(" (Azure error code {error_code})"),
        None => String::new(),
    }
}

#[derive(Debug, Deserialize)]
struct EnumerationResults {
    #[serde(rename = "Blobs")]
    blobs: Option<BlobList>,
    #[serde(rename = "NextMarker")]
    next_marker: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BlobList {
    #[serde(rename = "Blob", default)]
    blobs: Vec<BlobEntry>,
}

#[derive(Debug, Deserialize)]
struct BlobEntry {
    #[serde(rename = "Name")]
    name: String,
}

struct ListPage {
    names: Vec<String>,
    next_marker: String,
}

struct AzureBlockIdIterator<'a> {
    store: &'a AzureBlobBlockStore,
    pending_names: Vec<String>,
    next_marker: String,
    exhausted: bool,
}

impl<'a> AzureBlockIdIterator<'a> {
    fn new(store: &'a AzureBlobBlockStore) -> Self {
        Self {
            store,
            pending_names: Vec::new(),
            next_marker: String::new(),
            exhausted: false,
        }
    }

    fn load_next_page(&mut self) -> Result<(), BlockStoreError> {
        if self.exhausted {
            return Ok(());
        }

        let page = self.store.list_blob_page(&self.next_marker)?;
        self.next_marker = page.next_marker;
        self.exhausted = self.next_marker.is_empty();
        self.pending_names = page.names;
        self.pending_names.reverse();
        Ok(())
    }
}

impl Iterator for AzureBlockIdIterator<'_> {
    type Item = Result<BlockHash, BlockStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(name) = self.pending_names.pop() {
                match decode_recognized_block_blob_name(&name) {
                    Ok(Some(block_id)) => return Some(Ok(block_id)),
                    Ok(None) => continue,
                    Err(error) => return Some(Err(backend_failure(error))),
                }
            }

            if self.exhausted {
                return None;
            }

            if let Err(error) = self.load_next_page() {
                self.exhausted = true;
                return Some(Err(error));
            }
        }
    }
}
