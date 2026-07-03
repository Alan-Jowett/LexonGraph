// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Azure Blob Storage `BlockStore` implementation for LexonGraph blocks.

use std::env;
use std::error::Error as StdError;
use std::fmt;
use std::sync::OnceLock;
use std::thread::sleep;
use std::time::Duration;

use lexongraph_block::BlockHash;
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
const TRANSPORT_INITIAL_RETRY_DELAY: Duration = Duration::from_millis(250);
const TRANSPORT_MAX_RETRY_DELAY: Duration = Duration::from_secs(4);
const TRANSPORT_MAX_ATTEMPTS: usize = 6;
const DIAGNOSTICS_ENV_VAR: &str = "LEXONGRAPH_AZURE_BLOCK_STORE_DIAGNOSTICS";

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
                    "failed to prepare Azure Blob client for container {}: {}",
                    container_display,
                    redact_reqwest_error(error)
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

    fn fetch_blob_bytes(
        &self,
        operation: &'static str,
        block_id: Option<&BlockHash>,
        blob_name: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        let response = self
            .send_with_transport_retries(operation, block_id, Some(blob_name), || {
                self.request(Method::GET, self.build_blob_url(blob_name))
            })
            .map_err(|error| format!("request failed: {error}"))?;
        let response_meta = response_metadata(&response);

        match response_meta.status {
            StatusCode::OK => response
                .bytes()
                .map(|bytes| Some(bytes.to_vec()))
                .map_err(|error| {
                    let diagnostics = reqwest_error_diagnostics(error);
                    self.log_blob_event(
                        "response_body_read_failed",
                        operation,
                        block_id,
                        Some(blob_name),
                        diagnostics.log_fields_with_response(&response_meta),
                    );
                    format!(
                        "response body read failed: {}",
                        diagnostics.display_with_response(&response_meta)
                    )
                }),
            StatusCode::NOT_FOUND => {
                let body = response.bytes().map_err(|error| {
                    let diagnostics = reqwest_error_diagnostics(error);
                    self.log_blob_event(
                        "response_body_read_failed",
                        operation,
                        block_id,
                        Some(blob_name),
                        diagnostics.log_fields_with_response(&response_meta),
                    );
                    format!(
                        "response body read failed: {}",
                        diagnostics.display_with_response(&response_meta)
                    )
                })?;
                let error_code = response_meta
                    .error_code
                    .clone()
                    .or_else(|| parse_azure_error_code_body(&body));
                if error_code.as_deref() == Some("BlobNotFound") {
                    Ok(None)
                } else {
                    self.log_blob_event(
                        "blob_fetch_unexpected_status",
                        operation,
                        block_id,
                        Some(blob_name),
                        response_meta.log_fields(),
                    );
                    Err(format!(
                        "backend returned {}{}",
                        format_http_status(StatusCode::NOT_FOUND),
                        format_azure_error_code(error_code.as_deref())
                    ))
                }
            }
            _ => {
                let _ = response.bytes();
                self.log_blob_event(
                    "blob_fetch_unexpected_status",
                    operation,
                    block_id,
                    Some(blob_name),
                    response_meta.log_fields(),
                );
                Err(format!(
                    "backend returned {}{}",
                    format_http_status(response_meta.status),
                    format_azure_error_code(response_meta.error_code.as_deref())
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

        let response = self
            .send_with_transport_retries("list", None, None, || {
                self.request(Method::GET, url.clone())
            })
            .map_err(|error| {
                backend_failure(format!(
                    "failed to list Azure container {}: request failed: {}",
                    self.container_display, error
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
                "failed to read Azure listing response from {}: {}",
                self.container_display,
                redact_reqwest_error(error)
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

    fn send_with_transport_retries<F>(
        &self,
        operation: &'static str,
        block_id: Option<&BlockHash>,
        blob_name: Option<&str>,
        mut make_request: F,
    ) -> Result<reqwest::blocking::Response, TransportRetryError>
    where
        F: FnMut() -> reqwest::blocking::RequestBuilder,
    {
        let mut last_error = None;
        let mut attempts_made = 0;

        for attempt in 1..=TRANSPORT_MAX_ATTEMPTS {
            attempts_made = attempt;
            match make_request().send() {
                Ok(response) => {
                    if attempt > 1 {
                        let mut fields = vec![
                            ("attempt", attempt.to_string()),
                            ("max_attempts", TRANSPORT_MAX_ATTEMPTS.to_string()),
                        ];
                        fields.extend(response_metadata(&response).log_fields());
                        self.log_blob_event(
                            "request_succeeded_after_retry",
                            operation,
                            block_id,
                            blob_name,
                            fields,
                        );
                    }
                    return Ok(response);
                }
                Err(error) => {
                    let diagnostics = reqwest_error_diagnostics(error);
                    let retriable = diagnostics.is_retriable();
                    let retry_delay = if retriable && attempt < TRANSPORT_MAX_ATTEMPTS {
                        Some(transport_retry_delay(attempt))
                    } else {
                        None
                    };
                    let mut fields = vec![
                        ("attempt", attempt.to_string()),
                        ("max_attempts", TRANSPORT_MAX_ATTEMPTS.to_string()),
                        ("retriable", retriable.to_string()),
                    ];
                    if let Some(delay) = retry_delay {
                        fields.push(("next_retry_delay_ms", delay.as_millis().to_string()));
                    }
                    fields.extend(diagnostics.log_fields());
                    self.log_blob_event(
                        if retry_delay.is_some() {
                            "request_attempt_failed_will_retry"
                        } else {
                            "request_attempt_failed"
                        },
                        operation,
                        block_id,
                        blob_name,
                        fields,
                    );
                    last_error = Some(diagnostics);
                    if retriable && attempt < TRANSPORT_MAX_ATTEMPTS {
                        sleep(retry_delay.expect("retry delay must exist before retry"));
                        continue;
                    }
                    break;
                }
            }
        }

        let error = TransportRetryError {
            attempts_made,
            last_failure: last_error
                .unwrap_or_else(RequestErrorDiagnostics::unknown_transport_failure),
        };
        let mut fields = vec![
            ("attempts_made", attempts_made.to_string()),
            ("max_attempts", TRANSPORT_MAX_ATTEMPTS.to_string()),
        ];
        fields.extend(error.last_failure.log_fields());
        self.log_blob_event(
            "request_retry_exhausted",
            operation,
            block_id,
            blob_name,
            fields,
        );
        Err(error)
    }
}

impl BlockStore for AzureBlobBlockStore {
    fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        let blob_name = Self::block_blob_name(block_id);
        let blob_url = self.build_blob_url(&blob_name);
        let response = match self.send_with_transport_retries(
            "publish",
            Some(block_id),
            Some(&blob_name),
            || {
                self.request(Method::PUT, blob_url.clone())
                    .header("x-ms-blob-type", "BlockBlob")
                    .header(IF_NONE_MATCH, "*")
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(block_bytes.to_vec())
            },
        ) {
            Ok(response) => response,
            Err(error) => {
                return self.read_existing_or_map_transport_publish_error(
                    &blob_name,
                    block_id,
                    block_bytes,
                    &error,
                );
            }
        };

        let response_meta = response_metadata(&response);
        if let Err(error) = response.bytes() {
            let diagnostics = reqwest_error_diagnostics(error);
            self.log_blob_event(
                "response_body_read_failed",
                "publish",
                Some(block_id),
                Some(&blob_name),
                diagnostics.log_fields_with_response(&response_meta),
            );
        }

        if response_meta.status.is_success() {
            return Ok(());
        }

        if matches!(
            response_meta.status,
            StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED
        ) {
            let mut fields = response_meta.log_fields();
            fields.push(("publish_result", "already_exists".into()));
            self.log_blob_event(
                "publish_received_conflict",
                "publish",
                Some(block_id),
                Some(&blob_name),
                fields,
            );
            return Ok(());
        }

        let publish_probe = self.probe_blob_properties(
            "probe_after_publish_failure",
            Some(block_id),
            &blob_name,
            "publish_failure",
        );
        self.log_blob_event(
            "publish_response_failure",
            "publish",
            Some(block_id),
            Some(&blob_name),
            {
                let mut fields = response_meta.log_fields();
                fields.push(("probe", publish_probe.summary()));
                fields
            },
        );

        Err(backend_failure(format!(
            "failed to publish block {} to blob {} in container {}: {}{}; probe: {}",
            block_id,
            blob_name,
            self.container_display,
            format_http_status(response_meta.status),
            format_azure_error_code(response_meta.error_code.as_deref()),
            publish_probe.summary()
        )))
    }

    fn get_block_bytes(&self, block_id: &BlockHash) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let blob_name = Self::block_blob_name(block_id);
        let bytes = self
            .fetch_blob_bytes("read", Some(block_id), &blob_name)
            .map_err(|error| {
                backend_failure(format!(
                    "failed to read block {} from blob {} in container {}: {error}",
                    block_id, blob_name, self.container_display
                ))
            })?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };

        Ok(Some(bytes))
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        Ok(Box::new(AzureBlockIdIterator::new(self)))
    }
}

impl AzureBlobBlockStore {
    fn read_existing_or_map_transport_publish_error(
        &self,
        blob_name: &str,
        block_id: &BlockHash,
        canonical_bytes: &[u8],
        transport_error: &TransportRetryError,
    ) -> Result<(), BlockStoreError> {
        let publish_probe = self.probe_blob_properties(
            "probe_after_publish_retry_exhausted",
            Some(block_id),
            blob_name,
            "publish_retry_exhausted",
        );
        match self.fetch_blob_bytes(
            "verify_after_publish_retry_exhausted",
            Some(block_id),
            blob_name,
        ) {
            Ok(Some(existing_bytes)) if existing_bytes == canonical_bytes => {
                self.log_blob_event(
                    "publish_verify_succeeded",
                    "verify_after_publish_retry_exhausted",
                    Some(block_id),
                    Some(blob_name),
                    vec![
                        ("transport_error", transport_error.to_string()),
                        ("probe", publish_probe.summary()),
                    ],
                );
                Ok(())
            }
            Ok(Some(_)) => {
                self.log_blob_event(
                    "publish_verify_integrity_conflict",
                    "verify_after_publish_retry_exhausted",
                    Some(block_id),
                    Some(blob_name),
                    vec![
                        ("transport_error", transport_error.to_string()),
                        ("probe", publish_probe.summary()),
                    ],
                );
                Err(backend_failure(format!(
                    "integrity conflict at blob {} in container {} for block {} after publish transport failure ({transport_error}); probe: {}",
                    blob_name,
                    self.container_display,
                    block_id,
                    publish_probe.summary()
                )))
            }
            Ok(None) => {
                self.log_blob_event(
                    "publish_verify_missing",
                    "verify_after_publish_retry_exhausted",
                    Some(block_id),
                    Some(blob_name),
                    vec![
                        ("transport_error", transport_error.to_string()),
                        ("probe", publish_probe.summary()),
                    ],
                );
                Err(backend_failure(format!(
                    "failed to publish block {} to blob {} in container {}: {transport_error}; probe: {}",
                    block_id,
                    blob_name,
                    self.container_display,
                    publish_probe.summary()
                )))
            }
            Err(error) => {
                self.log_blob_event(
                    "publish_verify_failed",
                    "verify_after_publish_retry_exhausted",
                    Some(block_id),
                    Some(blob_name),
                    vec![
                        ("transport_error", transport_error.to_string()),
                        ("probe", publish_probe.summary()),
                        ("verify_error", error.clone()),
                    ],
                );
                Err(backend_failure(format!(
                    "failed to inspect blob {} in container {} after publish transport failure ({transport_error}) for block {}: {error}; probe: {}",
                    blob_name,
                    self.container_display,
                    block_id,
                    publish_probe.summary()
                )))
            }
        }
    }

    fn probe_blob_properties(
        &self,
        operation: &'static str,
        block_id: Option<&BlockHash>,
        blob_name: &str,
        trigger: &'static str,
    ) -> BlobProbeResult {
        let response =
            self.send_with_transport_retries(operation, block_id, Some(blob_name), || {
                self.request(Method::HEAD, self.build_blob_url(blob_name))
            });
        let result = match response {
            Ok(response) => {
                let metadata = response_metadata(&response);
                if let Err(error) = response.bytes() {
                    let diagnostics = reqwest_error_diagnostics(error);
                    self.log_blob_event(
                        "response_body_read_failed",
                        operation,
                        block_id,
                        Some(blob_name),
                        diagnostics.log_fields_with_response(&metadata),
                    );
                }
                let exists = match metadata.status {
                    StatusCode::OK => Some(true),
                    StatusCode::NOT_FOUND => Some(false),
                    _ => None,
                };
                BlobProbeResult::Response { exists, metadata }
            }
            Err(error) => BlobProbeResult::TransportError(error),
        };
        self.log_blob_event(
            "blob_probe_completed",
            operation,
            block_id,
            Some(blob_name),
            result.log_fields(trigger),
        );
        result
    }

    fn log_blob_event(
        &self,
        event: &'static str,
        operation: &'static str,
        block_id: Option<&BlockHash>,
        blob_name: Option<&str>,
        mut fields: Vec<(&'static str, String)>,
    ) {
        if !azure_block_store_diagnostics_enabled() {
            return;
        }
        let mut all_fields = vec![
            ("operation", operation.to_string()),
            ("container", self.container_display.clone()),
        ];
        if let Some(block_id) = block_id {
            all_fields.push(("block_id", block_id.to_string()));
        }
        if let Some(blob_name) = blob_name {
            all_fields.push(("blob_path", blob_name.to_string()));
        }
        all_fields.append(&mut fields);
        emit_diagnostic_log(event, &all_fields);
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

fn backend_failure(message: String) -> BlockStoreError {
    BlockStoreError::BackendFailure(message)
}

fn transport_retry_delay(attempt: usize) -> Duration {
    let exponent = attempt.saturating_sub(1).min(4) as u32;
    let multiplier = 1_u32 << exponent;
    let delay = TRANSPORT_INITIAL_RETRY_DELAY.saturating_mul(multiplier);
    delay.min(TRANSPORT_MAX_RETRY_DELAY)
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

fn has_non_empty_query_param(url: &Url, name: &str) -> bool {
    url.query_pairs()
        .any(|(key, value)| key == name && !value.is_empty())
}

fn redact_reqwest_error(error: reqwest::Error) -> String {
    error.without_url().to_string()
}

#[derive(Clone, Debug)]
struct AzureResponseMetadata {
    status: StatusCode,
    request_id: Option<String>,
    error_code: Option<String>,
    content_length: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
}

impl AzureResponseMetadata {
    fn log_fields(&self) -> Vec<(&'static str, String)> {
        let mut fields = vec![("http_status", format_http_status(self.status))];
        if let Some(request_id) = &self.request_id {
            fields.push(("azure_request_id", request_id.clone()));
        }
        if let Some(error_code) = &self.error_code {
            fields.push(("azure_error_code", error_code.clone()));
        }
        if let Some(content_length) = &self.content_length {
            fields.push(("content_length", content_length.clone()));
        }
        if let Some(etag) = &self.etag {
            fields.push(("etag", etag.clone()));
        }
        if let Some(last_modified) = &self.last_modified {
            fields.push(("last_modified", last_modified.clone()));
        }
        fields
    }

    fn summary(&self) -> String {
        let mut parts = vec![format!("status={}", format_http_status(self.status))];
        if let Some(request_id) = &self.request_id {
            parts.push(format!("request_id={request_id}"));
        }
        if let Some(error_code) = &self.error_code {
            parts.push(format!("error_code={error_code}"));
        }
        if let Some(content_length) = &self.content_length {
            parts.push(format!("content_length={content_length}"));
        }
        if let Some(etag) = &self.etag {
            parts.push(format!("etag={etag}"));
        }
        if let Some(last_modified) = &self.last_modified {
            parts.push(format!("last_modified={last_modified}"));
        }
        parts.join(", ")
    }
}

#[derive(Debug)]
struct RequestErrorDiagnostics {
    summary: String,
    debug: String,
    classifications: Vec<&'static str>,
    chain: Vec<String>,
}

impl RequestErrorDiagnostics {
    fn unknown_transport_failure() -> Self {
        Self {
            summary: "unknown request failure".into(),
            debug: "unknown request failure".into(),
            classifications: vec!["unknown"],
            chain: vec!["unknown".into()],
        }
    }

    fn is_retriable(&self) -> bool {
        self.classifications
            .iter()
            .any(|class| matches!(*class, "timeout" | "connect" | "request"))
    }

    fn class_summary(&self) -> String {
        self.classifications.join("|")
    }

    fn chain_summary(&self) -> String {
        self.chain.join(" <= ")
    }

    fn display(&self) -> String {
        format!("{} [class={}]", self.summary, self.class_summary())
    }

    fn display_with_response(&self, response: &AzureResponseMetadata) -> String {
        format!("{} [response={}]", self.display(), response.summary())
    }

    fn log_fields(&self) -> Vec<(&'static str, String)> {
        vec![
            ("error_class", self.class_summary()),
            ("error_summary", self.summary.clone()),
            ("error_debug", self.debug.clone()),
            ("error_chain", self.chain_summary()),
        ]
    }

    fn log_fields_with_response(
        &self,
        response: &AzureResponseMetadata,
    ) -> Vec<(&'static str, String)> {
        let mut fields = response.log_fields();
        fields.extend(self.log_fields());
        fields
    }
}

#[derive(Debug)]
struct TransportRetryError {
    attempts_made: usize,
    last_failure: RequestErrorDiagnostics,
}

impl fmt::Display for TransportRetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "after {} attempts: {}",
            self.attempts_made,
            self.last_failure.display()
        )
    }
}

#[derive(Debug)]
enum BlobProbeResult {
    Response {
        exists: Option<bool>,
        metadata: AzureResponseMetadata,
    },
    TransportError(TransportRetryError),
}

impl BlobProbeResult {
    fn summary(&self) -> String {
        match self {
            Self::Response { exists, metadata } => match exists {
                Some(true) => format!("exists=true, {}", metadata.summary()),
                Some(false) => format!("exists=false, {}", metadata.summary()),
                None => format!("exists=unknown, {}", metadata.summary()),
            },
            Self::TransportError(error) => format!("probe_transport_error={error}"),
        }
    }

    fn log_fields(&self, trigger: &'static str) -> Vec<(&'static str, String)> {
        let mut fields = vec![("trigger", trigger.to_string())];
        match self {
            Self::Response { exists, metadata } => {
                fields.push((
                    "blob_exists",
                    exists
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unknown".into()),
                ));
                fields.extend(metadata.log_fields());
            }
            Self::TransportError(error) => {
                fields.push(("blob_exists", "unknown".into()));
                fields.push(("probe_error", error.to_string()));
                fields.extend(error.last_failure.log_fields());
            }
        }
        fields
    }
}

fn response_metadata(response: &reqwest::blocking::Response) -> AzureResponseMetadata {
    AzureResponseMetadata {
        status: response.status(),
        request_id: response_header(response, "x-ms-request-id"),
        error_code: azure_error_code_header(response),
        content_length: response_header(response, "content-length"),
        etag: response_header(response, "etag"),
        last_modified: response_header(response, "last-modified"),
    }
}

fn response_header(response: &reqwest::blocking::Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn reqwest_error_diagnostics(error: reqwest::Error) -> RequestErrorDiagnostics {
    let mut classifications = Vec::new();
    if error.is_timeout() {
        classifications.push("timeout");
    }
    if error.is_connect() {
        classifications.push("connect");
    }
    if error.is_request() {
        classifications.push("request");
    }
    if error.is_status() {
        classifications.push("status");
    }
    if error.is_body() {
        classifications.push("body");
    }
    if error.is_decode() {
        classifications.push("decode");
    }
    if classifications.is_empty() {
        classifications.push("other");
    }

    let mut chain = Vec::new();
    let mut source = error.source();
    while let Some(next) = source {
        chain.push(redact_sensitive_text(&next.to_string()));
        source = next.source();
    }

    let redacted = error.without_url();
    chain.insert(0, redacted.to_string());

    RequestErrorDiagnostics {
        summary: redacted.to_string(),
        debug: format!("{redacted:?}"),
        classifications,
        chain,
    }
}

fn emit_diagnostic_log(event: &'static str, fields: &[(&'static str, String)]) {
    if !azure_block_store_diagnostics_enabled() {
        return;
    }
    let mut message = format!("[AZURE-BLOCK-STORE] event={}", quote_log_value(event));
    for (name, value) in fields {
        message.push(' ');
        message.push_str(name);
        message.push('=');
        message.push_str(&quote_log_value(value));
    }
    eprintln!("{message}");
}

fn azure_block_store_diagnostics_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        env::var(DIAGNOSTICS_ENV_VAR)
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

fn quote_log_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ => escaped.push(ch),
        }
    }
    escaped.push('"');
    escaped
}

fn redact_sensitive_text(value: &str) -> String {
    let mut redacted = value.to_string();
    while let Some(sig_index) = redacted.find("sig=") {
        let suffix = &redacted[sig_index..];
        let end = suffix
            .find(['&', ' ', '"', '\'', '\r', '\n', ')', ']', '}'])
            .map(|offset| sig_index + offset)
            .unwrap_or(redacted.len());
        redacted.replace_range(sig_index..end, "sig=<redacted>");
    }
    redacted
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
