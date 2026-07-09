// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Azure Table Storage `BlockStore` implementation for LexonGraph blocks.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::stream;
use lexongraph_block::{BlockError, BlockHash};
use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use url::Url;

const ENTITY_SCHEMA_VERSION: i32 = 3;
const MAX_ROW_PROPERTY_BYTES: usize = 1_048_576;
const MAX_PROPERTY_COUNT: usize = 255;
const FIXED_ROW_PROPERTY_COUNT: usize = 7;
const MAX_CHUNK_PROPERTY_COUNT: usize = MAX_PROPERTY_COUNT - FIXED_ROW_PROPERTY_COUNT;
const MAX_STRING_PROPERTY_CHARS: usize = 32 * 1024;
const RAW_CHUNK_SIZE: usize = 24_576;
const MAX_ROW_COUNT: usize = 8;
const CONTINUATION_ROW_SUFFIX_WIDTH: usize = 4;
const RETRY_INITIAL_DELAY: Duration = Duration::from_millis(250);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(4);
const RETRY_MAX_ATTEMPTS: usize = 6;
const AZURE_TABLE_API_VERSION: &str = "2019-02-02";
const ODATA_NO_METADATA: &str = "application/json;odata=nometadata";
const PREFER_RETURN_NO_CONTENT: &str = "return-no-content";

static ROOT_ROW_CAPACITY_BY_CHUNK_COUNT: OnceLock<Vec<usize>> = OnceLock::new();
static CONTINUATION_ROW_CAPACITY_BY_CHUNK_COUNT: OnceLock<Vec<usize>> = OnceLock::new();

#[derive(Clone)]
pub struct AzureTableBlockStoreV2 {
    backend: Arc<dyn TableBackend>,
    table_display: String,
}

impl fmt::Debug for AzureTableBlockStoreV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureTableBlockStoreV2")
            .field("table", &self.table_display)
            .finish()
    }
}

impl AzureTableBlockStoreV2 {
    pub fn new(table_sas_url: &str) -> Result<Self, BlockStoreError> {
        let endpoint = TableEndpoint::parse(table_sas_url)?;
        let table_display = endpoint.display.clone();
        let backend = ReqwestTableBackend::new(&endpoint)?;
        Ok(Self {
            backend: Arc::new(backend),
            table_display,
        })
    }

    #[cfg(test)]
    fn from_backend_for_tests(
        table_display: impl Into<String>,
        backend: Arc<dyn TableBackend>,
    ) -> Self {
        Self {
            backend,
            table_display: table_display.into(),
        }
    }

    fn partition_key(block_id: &BlockHash) -> String {
        partition_key_for(block_id)
    }

    async fn insert_row_with_retries(
        &self,
        block_id: &BlockHash,
        row: &TableBlockEntity,
    ) -> Result<(), BlockStoreError> {
        let mut attempts = 0usize;
        let mut delay = RETRY_INITIAL_DELAY;
        loop {
            attempts += 1;
            match self.backend.insert_entity_once(row).await {
                Ok(()) | Err(TableBackendAttemptError::AlreadyExists(_)) => return Ok(()),
                Err(TableBackendAttemptError::Transport(_)) if attempts < RETRY_MAX_ATTEMPTS => {
                    sleep(delay).await;
                    delay = next_retry_delay(delay);
                    continue;
                }
                Err(TableBackendAttemptError::Transport(message)) => {
                    return Err(backend_failure(format!(
                        "failed to publish block {} to Azure table {}: retry policy expired after {} attempts: {}",
                        block_id, self.table_display, attempts, message
                    )));
                }
                Err(TableBackendAttemptError::Response(message)) => {
                    return Err(backend_failure(format!(
                        "failed to publish block {} to Azure table {}: {}",
                        block_id, self.table_display, message
                    )));
                }
            }
        }
    }

    async fn get_row_with_retries(
        &self,
        block_id: &BlockHash,
        row_index: usize,
    ) -> Result<Option<TableBlockEntity>, BlockStoreError> {
        let partition_key = Self::partition_key(block_id);
        let row_key = row_key_for(block_id, row_index);
        let mut attempts = 0usize;
        let mut delay = RETRY_INITIAL_DELAY;
        loop {
            attempts += 1;
            match self.backend.get_entity_once(&partition_key, &row_key).await {
                Ok(row) => return Ok(row),
                Err(TableBackendAttemptError::Transport(_)) if attempts < RETRY_MAX_ATTEMPTS => {
                    sleep(delay).await;
                    delay = next_retry_delay(delay);
                    continue;
                }
                Err(TableBackendAttemptError::Transport(message)) => {
                    return Err(backend_failure(format!(
                        "failed to read block {} from Azure table {}: retry policy expired after {} attempts: {}",
                        block_id, self.table_display, attempts, message
                    )));
                }
                Err(TableBackendAttemptError::Response(message)) => {
                    return Err(backend_failure(format!(
                        "failed to read block {} from Azure table {}: {}",
                        block_id, self.table_display, message
                    )));
                }
                Err(TableBackendAttemptError::AlreadyExists(message)) => {
                    return Err(backend_failure(format!(
                        "unexpected already-existing result while reading block {} from Azure table {}: {}",
                        block_id, self.table_display, message
                    )));
                }
            }
        }
    }

    async fn verify_enumerated_block(
        &self,
        metadata: &TableBlockEntityMetadata,
        block_id: &BlockHash,
    ) -> Result<(), BlockStoreError> {
        metadata
            .validate_enumeration_payload()
            .map_err(backend_failure)?;
        let row_count = metadata.root_row_count().map_err(backend_failure)?;
        for row_index in 0..row_count {
            let Some(row) = self.get_row_with_retries(block_id, row_index).await? else {
                return Err(backend_failure(format!(
                    "failed to inspect Azure Table block {} during enumeration: missing row {}",
                    block_id, row_index
                )));
            };
            row.validate_row_identity(block_id, row_index)?;
            let chunk_count = row
                .validate_row_metadata(row_index, row_count, metadata.byte_len()?)
                .map_err(|error| {
                    backend_failure(format!(
                        "failed to inspect Azure Table block {} during enumeration: {}",
                        block_id, error
                    ))
                })?;
            row.validate_chunk_property_presence(chunk_count)
                .map_err(|error| {
                    backend_failure(format!(
                        "failed to inspect Azure Table block {} during enumeration: {}",
                        block_id, error
                    ))
                })?;
        }
        Ok(())
    }

    async fn fetch_page_with_retries(
        &self,
        continuation: Option<QueryContinuation>,
    ) -> Result<EntityPage, BlockStoreError> {
        let context = QueryContext::from_continuation(continuation.clone());
        let mut attempts = 0usize;
        let mut delay = RETRY_INITIAL_DELAY;
        loop {
            attempts += 1;
            match self
                .backend
                .query_entities_page_once(continuation.clone())
                .await
            {
                Ok(page) => return Ok(page),
                Err(TableBackendAttemptError::Transport(_)) if attempts < RETRY_MAX_ATTEMPTS => {
                    sleep(delay).await;
                    delay = next_retry_delay(delay);
                    continue;
                }
                Err(TableBackendAttemptError::Transport(message)) => {
                    return Err(backend_failure(format!(
                        "failed to query Azure table {} {}: retry policy expired after {} attempts: {}",
                        self.table_display,
                        context.description(),
                        attempts,
                        message
                    )));
                }
                Err(TableBackendAttemptError::Response(message)) => {
                    return Err(backend_failure(format!(
                        "failed to query Azure table {} {}: {}",
                        self.table_display,
                        context.description(),
                        message
                    )));
                }
                Err(TableBackendAttemptError::AlreadyExists(message)) => {
                    return Err(backend_failure(format!(
                        "unexpected already-existing result while querying Azure table {} {}: {}",
                        self.table_display,
                        context.description(),
                        message
                    )));
                }
            }
        }
    }
}

#[async_trait]
impl BlockStore for AzureTableBlockStoreV2 {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        let rows = TableBlockEntity::rows_from_block_bytes(block_id, block_bytes)?;
        for row in rows.iter().skip(1) {
            self.insert_row_with_retries(block_id, row).await?;
        }
        if let Some(root_row) = rows.first() {
            self.insert_row_with_retries(block_id, root_row).await?;
        }
        Ok(())
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let Some(root_row) = self.get_row_with_retries(block_id, 0).await? else {
            return Ok(None);
        };
        let root_metadata = root_row.decode_root_metadata(block_id)?;
        let mut rows = Vec::with_capacity(root_metadata.row_count);
        rows.push(root_row);
        for row_index in 1..root_metadata.row_count {
            let Some(row) = self.get_row_with_retries(block_id, row_index).await? else {
                return Err(decode_failure(
                    "Azure Table block row set is missing a required continuation row",
                ));
            };
            rows.push(row);
        }
        TableBlockEntity::decode_block_bytes(block_id, &rows).map(Some)
    }

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
        let store = self.clone();
        let state = IterState {
            store,
            continuation: None,
            pending: VecDeque::new(),
            finished: false,
        };
        Ok(Box::pin(stream::try_unfold(
            state,
            |mut state| async move {
                loop {
                    if let Some(entity) = state.pending.pop_front() {
                        match entity.enumerated_block_id() {
                            Ok(Some(block_id)) => {
                                state
                                    .store
                                    .verify_enumerated_block(&entity, &block_id)
                                    .await?;
                                return Ok(Some((block_id, state)));
                            }
                            Ok(None) => continue,
                            Err(message) => return Err(backend_failure(message)),
                        }
                    }

                    if state.finished {
                        return Ok(None);
                    }

                    let page = state
                        .store
                        .fetch_page_with_retries(state.continuation.clone())
                        .await?;
                    state.continuation = page.continuation;
                    state.finished = state.continuation.is_none();
                    state.pending = page.entities.into();

                    if state.pending.is_empty() && state.finished {
                        return Ok(None);
                    }
                }
            },
        )))
    }
}

#[derive(Clone)]
struct IterState {
    store: AzureTableBlockStoreV2,
    continuation: Option<QueryContinuation>,
    pending: VecDeque<TableBlockEntityMetadata>,
    finished: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct QueryContext(Option<QueryContinuation>);

impl QueryContext {
    fn from_continuation(value: Option<QueryContinuation>) -> Self {
        Self(value)
    }

    fn description(&self) -> String {
        match &self.0 {
            Some(continuation) => match (&continuation.partition_key, &continuation.row_key) {
                (Some(partition_key), Some(row_key)) => {
                    format!(
                        "after continuation PartitionKey={} RowKey={}",
                        partition_key, row_key
                    )
                }
                (Some(partition_key), None) => {
                    format!("after continuation PartitionKey={}", partition_key)
                }
                (None, Some(row_key)) => format!("after continuation RowKey={}", row_key),
                (None, None) => "after continuation".into(),
            },
            None => "from the table root".into(),
        }
    }
}

#[async_trait]
trait TableBackend: Send + Sync {
    async fn insert_entity_once(
        &self,
        entity: &TableBlockEntity,
    ) -> Result<(), TableBackendAttemptError>;

    async fn get_entity_once(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<Option<TableBlockEntity>, TableBackendAttemptError>;

    async fn query_entities_page_once(
        &self,
        continuation: Option<QueryContinuation>,
    ) -> Result<EntityPage, TableBackendAttemptError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TableBackendAttemptError {
    AlreadyExists(String),
    Response(String),
    Transport(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct QueryContinuation {
    partition_key: Option<String>,
    row_key: Option<String>,
}

#[derive(Clone, Debug)]
struct EntityPage {
    entities: Vec<TableBlockEntityMetadata>,
    continuation: Option<QueryContinuation>,
}

#[derive(Clone)]
struct ReqwestTableBackend {
    client: Client,
    endpoint: TableEndpoint,
}

impl ReqwestTableBackend {
    fn new(endpoint: &TableEndpoint) -> Result<Self, BlockStoreError> {
        let client = Client::builder().build().map_err(|error| {
            backend_failure(format!(
                "failed to prepare Azure Table client for {}: {}",
                endpoint.display, error
            ))
        })?;
        Ok(Self {
            client,
            endpoint: endpoint.clone(),
        })
    }

    fn common_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static(ODATA_NO_METADATA));
        headers.insert(
            "x-ms-version",
            HeaderValue::from_static(AZURE_TABLE_API_VERSION),
        );
        let request_date = HeaderValue::from_str(&http_date_now())
            .unwrap_or_else(|_| HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 GMT"));
        headers.insert("x-ms-date", request_date);
        headers
    }

    fn request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        self.client
            .request(method, url)
            .headers(self.common_headers())
    }

    fn classify_send_error(error: reqwest::Error) -> TableBackendAttemptError {
        if error.is_connect() || error.is_timeout() || error.is_request() {
            TableBackendAttemptError::Transport(redact_reqwest_error(error))
        } else {
            TableBackendAttemptError::Response(redact_reqwest_error(error))
        }
    }

    fn block_lookup_filter(partition_key: &str, row_key: &str) -> String {
        format!("PartitionKey eq '{partition_key}' and RowKey eq '{row_key}'")
    }

    fn entity_insert_url(&self) -> Url {
        self.endpoint.table_url.clone()
    }

    fn query_url(&self, continuation: Option<QueryContinuation>, extra: &[(&str, String)]) -> Url {
        let mut url = self.endpoint.table_query_url.clone();
        {
            let mut pairs = url.query_pairs_mut();
            for (name, value) in extra {
                pairs.append_pair(name, value);
            }
            if let Some(continuation) = continuation {
                if let Some(partition_key) = continuation.partition_key {
                    pairs.append_pair("NextPartitionKey", &partition_key);
                }
                if let Some(row_key) = continuation.row_key {
                    pairs.append_pair("NextRowKey", &row_key);
                }
            }
        }
        url
    }

    async fn read_body_text(
        response: reqwest::Response,
    ) -> Result<(StatusCode, HeaderMap, String), TableBackendAttemptError> {
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .text()
            .await
            .map_err(|error| TableBackendAttemptError::Response(redact_reqwest_error(error)))?;
        Ok((status, headers, body))
    }

    fn response_error(status: StatusCode, body: &str) -> TableBackendAttemptError {
        let description = describe_http_response(status, body);
        if status == StatusCode::CONFLICT {
            TableBackendAttemptError::AlreadyExists(description)
        } else {
            TableBackendAttemptError::Response(description)
        }
    }

    fn parse_continuation(headers: &HeaderMap) -> Option<QueryContinuation> {
        let partition_key = headers
            .get("x-ms-continuation-NextPartitionKey")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let row_key = headers
            .get("x-ms-continuation-NextRowKey")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        if partition_key.is_none() && row_key.is_none() {
            None
        } else {
            Some(QueryContinuation {
                partition_key,
                row_key,
            })
        }
    }

    async fn query_page<T: for<'de> Deserialize<'de>>(
        &self,
        continuation: Option<QueryContinuation>,
        extra: &[(&str, String)],
    ) -> Result<QueryResponse<T>, TableBackendAttemptError> {
        let response = self
            .request(Method::GET, self.query_url(continuation, extra))
            .send()
            .await
            .map_err(Self::classify_send_error)?;
        let (status, headers, body) = Self::read_body_text(response).await?;
        if status != StatusCode::OK {
            return Err(Self::response_error(status, &body));
        }
        let envelope: QueryEnvelope<T> = serde_json::from_str(&body).map_err(|error| {
            TableBackendAttemptError::Response(format!(
                "HTTP {}: failed to decode Azure Table query response: {}",
                status.as_u16(),
                error
            ))
        })?;
        Ok(QueryResponse {
            entities: envelope.value,
            continuation: Self::parse_continuation(&headers),
        })
    }

    async fn query_metadata_page(
        &self,
        continuation: Option<QueryContinuation>,
    ) -> Result<QueryResponse<TableBlockEntityMetadata>, TableBackendAttemptError> {
        let extra = [(
            "$select",
            "PartitionKey,RowKey,SchemaVersion,ByteLen,RowCount,RowIndex,ChunkCount".to_string(),
        )];
        self.query_page(continuation, &extra).await
    }

    async fn query_entity_once(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<QueryResponse<TableBlockEntity>, TableBackendAttemptError> {
        let extra = [
            ("$filter", Self::block_lookup_filter(partition_key, row_key)),
            ("$top", "2".to_string()),
        ];
        self.query_page(None, &extra).await
    }
}

#[async_trait]
impl TableBackend for ReqwestTableBackend {
    async fn insert_entity_once(
        &self,
        entity: &TableBlockEntity,
    ) -> Result<(), TableBackendAttemptError> {
        let body = serde_json::to_vec(entity).map_err(|error| {
            TableBackendAttemptError::Response(format!(
                "failed to encode Azure Table entity body: {}",
                error
            ))
        })?;
        let response = self
            .request(Method::POST, self.entity_insert_url())
            .header(CONTENT_TYPE, "application/json")
            .header("Prefer", PREFER_RETURN_NO_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(Self::classify_send_error)?;
        let (status, _headers, body) = Self::read_body_text(response).await?;
        match status {
            StatusCode::NO_CONTENT | StatusCode::CREATED => Ok(()),
            _ => Err(Self::response_error(status, &body)),
        }
    }

    async fn get_entity_once(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<Option<TableBlockEntity>, TableBackendAttemptError> {
        let page = self.query_entity_once(partition_key, row_key).await?;
        match page.entities.len() {
            0 => Ok(None),
            1 => Ok(page.entities.into_iter().next()),
            count => Err(TableBackendAttemptError::Response(format!(
                "lookup for PartitionKey={} RowKey={} returned {} entities",
                partition_key, row_key, count
            ))),
        }
    }

    async fn query_entities_page_once(
        &self,
        continuation: Option<QueryContinuation>,
    ) -> Result<EntityPage, TableBackendAttemptError> {
        let page = self.query_metadata_page(continuation).await?;
        Ok(EntityPage {
            entities: page.entities,
            continuation: page.continuation,
        })
    }
}

#[derive(Debug, Deserialize)]
struct QueryEnvelope<T> {
    value: Vec<T>,
}

#[derive(Debug)]
struct QueryResponse<T> {
    entities: Vec<T>,
    continuation: Option<QueryContinuation>,
}

#[derive(Clone, Debug)]
struct TableEndpoint {
    table_url: Url,
    table_query_url: Url,
    display: String,
}

impl TableEndpoint {
    fn parse(table_sas_url: &str) -> Result<Self, BlockStoreError> {
        let mut url = Url::parse(table_sas_url).map_err(|error| {
            backend_failure(format!("failed to parse Azure Table SAS URL: {error}"))
        })?;
        url.set_fragment(None);
        if url.query().is_none_or(str::is_empty) {
            return Err(backend_failure(
                "Azure Table SAS URL must include SAS query parameters".into(),
            ));
        }
        if !has_non_empty_query_param(&url, "sig") {
            return Err(backend_failure(
                "Azure Table SAS URL must include a non-empty SAS signature parameter".into(),
            ));
        }

        let host = url
            .host_str()
            .ok_or_else(|| backend_failure("Azure Table SAS URL must include a host".into()))?;
        let _account = host
            .split('.')
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                backend_failure("Azure Table SAS URL must include an account host".into())
            })?;

        let path_segments = url
            .path_segments()
            .ok_or_else(|| backend_failure("Azure Table SAS URL must be hierarchical".into()))?
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        if path_segments.len() != 1 {
            return Err(backend_failure(format!(
                "Azure Table SAS URL must address a table root, got path {}",
                url.path()
            )));
        }

        let table_name = path_segments[0].to_string();
        if table_name.contains('(') || table_name.contains(')') {
            return Err(backend_failure(format!(
                "Azure Table SAS URL must address a table root, got path {}",
                url.path()
            )));
        }
        url.set_path(&format!("/{}", table_name));
        let display = redact_url(&url);
        let table_url = url.clone();
        let mut table_query_url = url;
        table_query_url.set_path(&format!("/{}()", table_name));
        Ok(Self {
            table_url,
            table_query_url,
            display,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableBlockEntityMetadata {
    #[serde(rename = "PartitionKey")]
    partition_key: String,
    #[serde(rename = "RowKey")]
    row_key: String,
    #[serde(rename = "SchemaVersion")]
    schema_version: Option<i32>,
    #[serde(rename = "ByteLen")]
    byte_len: Option<i64>,
    #[serde(rename = "RowCount")]
    row_count: Option<i32>,
    #[serde(rename = "RowIndex")]
    row_index: Option<i32>,
    #[serde(rename = "ChunkCount")]
    chunk_count: Option<i32>,
}

impl TableBlockEntityMetadata {
    #[cfg(test)]
    fn from_entity(entity: &TableBlockEntity) -> Self {
        Self {
            partition_key: entity.partition_key.clone(),
            row_key: entity.row_key.clone(),
            schema_version: entity.schema_version,
            byte_len: entity.byte_len,
            row_count: entity.row_count,
            row_index: entity.row_index,
            chunk_count: entity.chunk_count,
        }
    }

    fn enumerated_block_id(&self) -> Result<Option<BlockHash>, String> {
        let Some(block_id) = self.recognized_block_id()? else {
            return Ok(None);
        };
        self.validate_enumeration_payload()?;
        Ok(Some(block_id))
    }

    fn recognized_block_id(&self) -> Result<Option<BlockHash>, String> {
        decode_recognized_block_root_keys(&self.partition_key, &self.row_key)
    }

    fn validate_enumeration_payload(&self) -> Result<(), String> {
        let schema_version = self.schema_version.ok_or_else(|| {
            format!(
                "failed to inspect Azure Table row {} / {}: missing SchemaVersion",
                self.partition_key, self.row_key
            )
        })?;
        if schema_version != ENTITY_SCHEMA_VERSION {
            return Err(format!(
                "failed to inspect Azure Table row {} / {}: unsupported schema version {}",
                self.partition_key, self.row_key, schema_version
            ));
        }
        let expected_len = usize::try_from(self.byte_len.ok_or_else(|| {
            format!(
                "failed to inspect Azure Table row {} / {}: missing ByteLen",
                self.partition_key, self.row_key
            )
        })?)
        .map_err(|_| {
            format!(
                "failed to inspect Azure Table row {} / {}: ByteLen must be non-negative",
                self.partition_key, self.row_key
            )
        })?;
        let row_count = usize::try_from(self.row_count.ok_or_else(|| {
            format!(
                "failed to inspect Azure Table row {} / {}: missing RowCount",
                self.partition_key, self.row_key
            )
        })?)
        .map_err(|_| {
            format!(
                "failed to inspect Azure Table row {} / {}: RowCount must be positive",
                self.partition_key, self.row_key
            )
        })?;
        let row_index = usize::try_from(self.row_index.ok_or_else(|| {
            format!(
                "failed to inspect Azure Table row {} / {}: missing RowIndex",
                self.partition_key, self.row_key
            )
        })?)
        .map_err(|_| {
            format!(
                "failed to inspect Azure Table row {} / {}: RowIndex must be non-negative",
                self.partition_key, self.row_key
            )
        })?;
        let chunk_count = usize::try_from(self.chunk_count.ok_or_else(|| {
            format!(
                "failed to inspect Azure Table row {} / {}: missing ChunkCount",
                self.partition_key, self.row_key
            )
        })?)
        .map_err(|_| {
            format!(
                "failed to inspect Azure Table row {} / {}: ChunkCount must be non-negative",
                self.partition_key, self.row_key
            )
        })?;
        if row_count == 0 {
            return Err(format!(
                "failed to inspect Azure Table row {} / {}: RowCount must be positive",
                self.partition_key, self.row_key
            ));
        }
        if row_count > MAX_ROW_COUNT {
            return Err(format!(
                "failed to inspect Azure Table row {} / {}: RowCount exceeds the supported limit",
                self.partition_key, self.row_key
            ));
        }
        if row_index != 0 {
            return Err(format!(
                "failed to inspect Azure Table row {} / {}: recognized block roots must use RowIndex 0",
                self.partition_key, self.row_key
            ));
        }
        if chunk_count > MAX_CHUNK_PROPERTY_COUNT {
            return Err(format!(
                "failed to inspect Azure Table row {} / {}: ChunkCount exceeds the supported per-row limit",
                self.partition_key, self.row_key
            ));
        }
        if expected_len == 0 && (row_count != 1 || chunk_count != 0) {
            return Err(format!(
                "failed to inspect Azure Table row {} / {}: zero ByteLen requires a single zero-chunk root row",
                self.partition_key, self.row_key
            ));
        }
        if expected_len != 0 && chunk_count == 0 {
            return Err(format!(
                "failed to inspect Azure Table row {} / {}: non-zero ByteLen requires at least one root-row chunk",
                self.partition_key, self.row_key
            ));
        }
        let root_capacity = max_supported_row_payload_bytes(0, chunk_count);
        let continuation_capacity = row_count
            .checked_sub(1)
            .and_then(|count| {
                count.checked_mul(max_supported_row_payload_bytes(1, MAX_CHUNK_PROPERTY_COUNT))
            })
            .ok_or_else(|| {
                format!(
                    "failed to inspect Azure Table row {} / {}: RowCount exceeds the supported layout capacity",
                    self.partition_key, self.row_key
                )
            })?;
        let max_possible_len = root_capacity
            .checked_add(continuation_capacity)
            .ok_or_else(|| {
                format!(
                    "failed to inspect Azure Table row {} / {}: RowCount exceeds the supported layout capacity",
                    self.partition_key, self.row_key
                )
            })?;
        if expected_len > max_possible_len {
            return Err(format!(
                "failed to inspect Azure Table row {} / {}: ByteLen exceeds the capacity implied by RowCount and ChunkCount",
                self.partition_key, self.row_key
            ));
        }
        Ok(())
    }

    fn root_row_count(&self) -> Result<usize, String> {
        usize::try_from(self.row_count.ok_or_else(|| {
            format!(
                "failed to inspect Azure Table row {} / {}: missing RowCount",
                self.partition_key, self.row_key
            )
        })?)
        .map_err(|_| {
            format!(
                "failed to inspect Azure Table row {} / {}: RowCount must be positive",
                self.partition_key, self.row_key
            )
        })
    }

    fn byte_len(&self) -> Result<usize, BlockStoreError> {
        usize::try_from(
            self.byte_len
                .ok_or_else(|| decode_failure("Azure Table block row is missing ByteLen"))?,
        )
        .map_err(|_| decode_failure("Azure Table ByteLen must be non-negative and fit in usize"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableBlockEntity {
    #[serde(rename = "PartitionKey")]
    partition_key: String,
    #[serde(rename = "RowKey")]
    row_key: String,
    #[serde(rename = "SchemaVersion")]
    schema_version: Option<i32>,
    #[serde(rename = "ByteLen")]
    byte_len: Option<i64>,
    #[serde(rename = "RowCount")]
    row_count: Option<i32>,
    #[serde(rename = "RowIndex")]
    row_index: Option<i32>,
    #[serde(rename = "ChunkCount")]
    chunk_count: Option<i32>,
    #[serde(flatten)]
    chunk_properties: BTreeMap<String, String>,
}

impl TableBlockEntity {
    #[cfg(test)]
    fn from_block_bytes(block_id: &BlockHash, block_bytes: &[u8]) -> Result<Self, BlockStoreError> {
        let chunks = block_bytes.chunks(RAW_CHUNK_SIZE).collect::<Vec<_>>();
        Self::from_row_chunks(block_id, block_bytes.len(), 1, 0, &chunks)
    }

    fn rows_from_block_bytes(
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<Vec<Self>, BlockStoreError> {
        let all_chunks = block_bytes.chunks(RAW_CHUNK_SIZE).collect::<Vec<_>>();
        let total_len = block_bytes.len();
        let mut chunk_rows: Vec<Vec<&[u8]>> = Vec::new();
        let mut current_row: Vec<&[u8]> = Vec::new();
        for chunk in all_chunks {
            current_row.push(chunk);
            if Self::from_row_chunks(block_id, total_len, 1, chunk_rows.len(), &current_row).is_ok()
            {
                continue;
            }
            let overflow_chunk = current_row
                .pop()
                .expect("current_row is non-empty after push");
            if current_row.is_empty() {
                return Err(backend_failure(format!(
                    "block {} cannot fit within one Azure Table row because a single chunk exceeds the row limits",
                    block_id
                )));
            }
            chunk_rows.push(current_row);
            current_row = vec![overflow_chunk];
        }
        chunk_rows.push(current_row);
        if chunk_rows.len() > MAX_ROW_COUNT {
            return Err(backend_failure(format!(
                "block {} cannot fit within the supported Azure Table row-set layout because it requires {} rows and the limit is {}",
                block_id,
                chunk_rows.len(),
                MAX_ROW_COUNT
            )));
        }
        let row_count = chunk_rows.len();
        chunk_rows
            .into_iter()
            .enumerate()
            .map(|(row_index, row_chunks)| {
                Self::from_row_chunks(block_id, total_len, row_count, row_index, &row_chunks)
            })
            .collect()
    }

    fn from_row_chunks(
        block_id: &BlockHash,
        total_len: usize,
        row_count: usize,
        row_index: usize,
        row_chunks: &[&[u8]],
    ) -> Result<Self, BlockStoreError> {
        let row_key = row_key_for(block_id, row_index);
        let partition_key = partition_key_for(block_id);
        let mut chunk_properties = BTreeMap::new();
        for (index, chunk) in row_chunks.iter().enumerate() {
            let encoded = BASE64.encode(chunk);
            if encoded.len() > MAX_STRING_PROPERTY_CHARS {
                return Err(backend_failure(format!(
                    "block {} cannot be encoded into one Azure Table row under PartitionKey={}: chunk {} requires {} characters and exceeds the per-property limit of {}",
                    block_id,
                    partition_key,
                    index,
                    encoded.len(),
                    MAX_STRING_PROPERTY_CHARS
                )));
            }
            chunk_properties.insert(chunk_property_name(index), encoded);
        }

        let entity = Self {
            partition_key,
            row_key,
            schema_version: Some(ENTITY_SCHEMA_VERSION),
            byte_len: Some(i64::try_from(total_len).map_err(|_| {
                backend_failure(format!(
                    "block {} length does not fit the Azure Table row metadata representation",
                    block_id
                ))
            })?),
            row_count: Some(i32::try_from(row_count).map_err(|_| {
                backend_failure(format!(
                    "block {} requires too many Azure Table rows",
                    block_id
                ))
            })?),
            row_index: Some(i32::try_from(row_index).map_err(|_| {
                backend_failure(format!(
                    "block {} row index does not fit the Azure Table row metadata representation",
                    block_id
                ))
            })?),
            chunk_count: Some(i32::try_from(chunk_properties.len()).map_err(|_| {
                backend_failure(format!(
                    "block {} requires too many Azure Table chunk properties",
                    block_id
                ))
            })?),
            chunk_properties,
        };

        let property_count = entity.chunk_properties.len() + FIXED_ROW_PROPERTY_COUNT;
        if property_count > MAX_PROPERTY_COUNT {
            return Err(backend_failure(format!(
                "block {} cannot fit within one Azure Table row because it requires {} properties and the limit is {}",
                block_id, property_count, MAX_PROPERTY_COUNT
            )));
        }

        let property_bytes = entity.encoded_property_bytes().map_err(|message| {
            backend_failure(format!(
                "failed to estimate Azure Table row size for block {}: {}",
                block_id, message
            ))
        })?;
        if property_bytes > MAX_ROW_PROPERTY_BYTES {
            return Err(backend_failure(format!(
                "block {} cannot fit within one Azure Table row because the property data requires {} bytes and the limit is {}",
                block_id, property_bytes, MAX_ROW_PROPERTY_BYTES
            )));
        }

        Ok(entity)
    }

    fn decode_root_metadata(
        &self,
        expected_block_id: &BlockHash,
    ) -> Result<RowSetMetadata, BlockStoreError> {
        self.validate_row_identity(expected_block_id, 0)?;
        let total_len = usize::try_from(
            self.byte_len
                .ok_or_else(|| decode_failure("Azure Table block row is missing ByteLen"))?,
        )
        .map_err(|_| decode_failure("Azure Table ByteLen must be non-negative and fit in usize"))?;
        let row_count = usize::try_from(
            self.row_count
                .ok_or_else(|| decode_failure("Azure Table block row is missing RowCount"))?,
        )
        .map_err(|_| decode_failure("Azure Table RowCount must be positive and fit in usize"))?;
        let row_index = usize::try_from(
            self.row_index
                .ok_or_else(|| decode_failure("Azure Table block row is missing RowIndex"))?,
        )
        .map_err(|_| {
            decode_failure("Azure Table RowIndex must be non-negative and fit in usize")
        })?;
        let root_chunk_count = usize::try_from(
            self.chunk_count
                .ok_or_else(|| decode_failure("Azure Table block row is missing ChunkCount"))?,
        )
        .map_err(|_| {
            decode_failure("Azure Table ChunkCount must be non-negative and fit in usize")
        })?;
        let schema_version = self
            .schema_version
            .ok_or_else(|| decode_failure("Azure Table block row is missing SchemaVersion"))?;
        if schema_version != ENTITY_SCHEMA_VERSION {
            return Err(decode_failure(
                "unsupported Azure Table block row schema version",
            ));
        }
        if row_count == 0 || row_count > MAX_ROW_COUNT {
            return Err(decode_failure(
                "Azure Table block row declared an unsupported RowCount",
            ));
        }
        if row_index != 0 {
            return Err(decode_failure(
                "Azure Table block root must declare RowIndex 0",
            ));
        }
        if root_chunk_count > MAX_CHUNK_PROPERTY_COUNT {
            return Err(decode_failure(
                "Azure Table block row declared too many chunks",
            ));
        }
        if total_len == 0 && (row_count != 1 || root_chunk_count != 0) {
            return Err(decode_failure(
                "Azure Table zero-length block must use a single zero-chunk root row",
            ));
        }
        if total_len != 0 && root_chunk_count == 0 {
            return Err(decode_failure(
                "Azure Table non-zero block must include at least one root-row chunk",
            ));
        }
        let root_capacity = max_supported_row_payload_bytes(0, root_chunk_count);
        let continuation_capacity = row_count
            .checked_sub(1)
            .and_then(|count| {
                count.checked_mul(max_supported_row_payload_bytes(1, MAX_CHUNK_PROPERTY_COUNT))
            })
            .ok_or_else(|| {
                decode_failure("Azure Table RowCount exceeds the supported layout capacity")
            })?;
        let max_possible_len = root_capacity
            .checked_add(continuation_capacity)
            .ok_or_else(|| {
                decode_failure("Azure Table RowCount exceeds the supported layout capacity")
            })?;
        if total_len > max_possible_len {
            return Err(decode_failure(
                "Azure Table block row declared a ByteLen larger than RowCount and ChunkCount can contain",
            ));
        }
        Ok(RowSetMetadata {
            total_len,
            row_count,
        })
    }

    fn decode_block_bytes(
        expected_block_id: &BlockHash,
        rows: &[TableBlockEntity],
    ) -> Result<Vec<u8>, BlockStoreError> {
        let Some(root_row) = rows.first() else {
            return Err(decode_failure(
                "Azure Table block row set is missing the root row",
            ));
        };
        let metadata = root_row.decode_root_metadata(expected_block_id)?;
        if rows.len() != metadata.row_count {
            return Err(decode_failure(
                "Azure Table block row set reconstructed an unexpected number of rows",
            ));
        }

        let mut block_bytes = Vec::with_capacity(metadata.total_len);
        for (expected_row_index, row) in rows.iter().enumerate() {
            row.validate_row_identity(expected_block_id, expected_row_index)?;
            let row_chunk_count = row.validate_row_metadata(
                expected_row_index,
                metadata.row_count,
                metadata.total_len,
            )?;
            row.extend_decoded_row_bytes(row_chunk_count, &mut block_bytes)?;
        }

        if block_bytes.len() != metadata.total_len {
            return Err(decode_failure(
                "Azure Table block row set reconstructed an unexpected number of bytes",
            ));
        }

        Ok(block_bytes)
    }

    fn validate_row_identity(
        &self,
        expected_block_id: &BlockHash,
        expected_row_index: usize,
    ) -> Result<(), BlockStoreError> {
        let expected_partition_key = partition_key_for(expected_block_id);
        let expected_row_key = row_key_for(expected_block_id, expected_row_index);
        if self.partition_key != expected_partition_key || self.row_key != expected_row_key {
            return Err(backend_failure(format!(
                "Azure Table lookup for block {} row {} returned unexpected row keys {} / {}",
                expected_block_id, expected_row_index, self.partition_key, self.row_key
            )));
        }
        Ok(())
    }

    fn validate_row_metadata(
        &self,
        expected_row_index: usize,
        expected_row_count: usize,
        expected_total_len: usize,
    ) -> Result<usize, BlockStoreError> {
        let schema_version = self
            .schema_version
            .ok_or_else(|| decode_failure("Azure Table block row is missing SchemaVersion"))?;
        if schema_version != ENTITY_SCHEMA_VERSION {
            return Err(decode_failure(
                "unsupported Azure Table block row schema version",
            ));
        }
        let total_len = usize::try_from(
            self.byte_len
                .ok_or_else(|| decode_failure("Azure Table block row is missing ByteLen"))?,
        )
        .map_err(|_| decode_failure("Azure Table ByteLen must be non-negative and fit in usize"))?;
        if total_len != expected_total_len {
            return Err(decode_failure(
                "Azure Table block rows disagree about ByteLen",
            ));
        }
        let row_count = usize::try_from(
            self.row_count
                .ok_or_else(|| decode_failure("Azure Table block row is missing RowCount"))?,
        )
        .map_err(|_| decode_failure("Azure Table RowCount must be positive and fit in usize"))?;
        if row_count != expected_row_count {
            return Err(decode_failure(
                "Azure Table block rows disagree about RowCount",
            ));
        }
        let row_index = usize::try_from(
            self.row_index
                .ok_or_else(|| decode_failure("Azure Table block row is missing RowIndex"))?,
        )
        .map_err(|_| {
            decode_failure("Azure Table RowIndex must be non-negative and fit in usize")
        })?;
        if row_index != expected_row_index {
            return Err(decode_failure(
                "Azure Table block row declared an unexpected RowIndex",
            ));
        }
        let chunk_count = usize::try_from(
            self.chunk_count
                .ok_or_else(|| decode_failure("Azure Table block row is missing ChunkCount"))?,
        )
        .map_err(|_| {
            decode_failure("Azure Table ChunkCount must be non-negative and fit in usize")
        })?;
        if chunk_count > MAX_CHUNK_PROPERTY_COUNT {
            return Err(decode_failure(
                "Azure Table block row declared too many chunks",
            ));
        }
        if expected_total_len == 0 && chunk_count != 0 {
            return Err(decode_failure(
                "Azure Table zero-length block rows must declare zero chunks",
            ));
        }
        if expected_total_len != 0 && chunk_count == 0 {
            return Err(decode_failure(
                "Azure Table non-zero block rows must declare at least one chunk",
            ));
        }
        Ok(chunk_count)
    }

    fn validate_chunk_property_presence(&self, chunk_count: usize) -> Result<(), BlockStoreError> {
        if self.chunk_properties.len() < chunk_count {
            return Err(decode_failure(
                "Azure Table block row is missing a required chunk property",
            ));
        }
        for index in 0..chunk_count {
            let property_name = chunk_property_name(index);
            if !self.chunk_properties.contains_key(&property_name) {
                return Err(decode_failure(
                    "Azure Table block row is missing a required chunk property",
                ));
            }
        }
        Ok(())
    }

    fn extend_decoded_row_bytes(
        &self,
        chunk_count: usize,
        block_bytes: &mut Vec<u8>,
    ) -> Result<(), BlockStoreError> {
        self.validate_chunk_property_presence(chunk_count)?;
        for index in 0..chunk_count {
            let property_name = chunk_property_name(index);
            let encoded = self.chunk_properties.get(&property_name).ok_or_else(|| {
                decode_failure("Azure Table block row is missing a required chunk property")
            })?;
            let decoded = BASE64
                .decode(encoded)
                .map_err(|_| decode_failure("Azure Table block row chunk is not valid base64"))?;
            block_bytes.extend_from_slice(&decoded);
        }
        Ok(())
    }
    fn encoded_property_bytes(&self) -> Result<usize, &'static str> {
        let mut total = 0usize;
        total = checked_entity_property_bytes(
            total,
            string_property_bytes("PartitionKey", &self.partition_key),
        )?;
        total =
            checked_entity_property_bytes(total, string_property_bytes("RowKey", &self.row_key))?;
        if let Some(schema_version) = self.schema_version {
            total = checked_entity_property_bytes(
                total,
                primitive_property_bytes("SchemaVersion", std::mem::size_of_val(&schema_version)),
            )?;
        }
        if let Some(byte_len) = self.byte_len {
            total = checked_entity_property_bytes(
                total,
                primitive_property_bytes("ByteLen", std::mem::size_of_val(&byte_len)),
            )?;
        }
        if let Some(row_count) = self.row_count {
            total = checked_entity_property_bytes(
                total,
                primitive_property_bytes("RowCount", std::mem::size_of_val(&row_count)),
            )?;
        }
        if let Some(row_index) = self.row_index {
            total = checked_entity_property_bytes(
                total,
                primitive_property_bytes("RowIndex", std::mem::size_of_val(&row_index)),
            )?;
        }
        if let Some(chunk_count) = self.chunk_count {
            total = checked_entity_property_bytes(
                total,
                primitive_property_bytes("ChunkCount", std::mem::size_of_val(&chunk_count)),
            )?;
        }
        for (name, value) in &self.chunk_properties {
            total = checked_entity_property_bytes(total, string_property_bytes(name, value))?;
        }
        Ok(total)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RowSetMetadata {
    total_len: usize,
    row_count: usize,
}

fn partition_key_for(block_id: &BlockHash) -> String {
    let hex = block_id.to_string();
    hex[..4].to_string()
}

fn row_key_for(block_id: &BlockHash, row_index: usize) -> String {
    if row_index == 0 {
        block_id.to_string()
    } else {
        format!(
            "{}-{:0width$}",
            block_id,
            row_index,
            width = CONTINUATION_ROW_SUFFIX_WIDTH
        )
    }
}

fn decode_recognized_block_root_keys(
    partition_key: &str,
    row_key: &str,
) -> Result<Option<BlockHash>, String> {
    if !is_lower_hex(partition_key, 4) {
        return Ok(None);
    }
    let Some(bytes) = decode_block_hash_hex(row_key) else {
        return Ok(None);
    };
    if &row_key[..4] != partition_key {
        return Err(format!(
            "failed to decode an enumerated block ID candidate at entity {partition_key} / {row_key}: shard prefix mismatch"
        ));
    }
    Ok(Some(BlockHash::from_bytes(bytes)))
}

fn chunk_property_name(index: usize) -> String {
    format!("chunk{index}")
}

fn max_supported_row_payload_bytes(row_index: usize, max_chunks: usize) -> usize {
    let cache = if row_index == 0 {
        &ROOT_ROW_CAPACITY_BY_CHUNK_COUNT
    } else {
        &CONTINUATION_ROW_CAPACITY_BY_CHUNK_COUNT
    };
    cache.get_or_init(|| precompute_row_capacity_bytes(row_index))[max_chunks]
}

fn precompute_row_capacity_bytes(row_index: usize) -> Vec<usize> {
    (0..=MAX_CHUNK_PROPERTY_COUNT)
        .map(|max_chunks| simulated_row_capacity_bytes(row_index, max_chunks))
        .collect()
}

fn simulated_row_capacity_bytes(row_index: usize, max_chunks: usize) -> usize {
    let block_id = BlockHash::from_bytes([0_u8; BlockHash::LEN]);
    let full_chunk = vec![0_u8; RAW_CHUNK_SIZE];
    let mut row_chunks: Vec<&[u8]> = Vec::new();
    let mut total_len = 0usize;

    while row_chunks.len() < max_chunks {
        row_chunks.push(full_chunk.as_slice());
        if TableBlockEntity::from_row_chunks(
            &block_id,
            total_len + RAW_CHUNK_SIZE,
            1,
            row_index,
            &row_chunks,
        )
        .is_ok()
        {
            total_len += RAW_CHUNK_SIZE;
            continue;
        }
        row_chunks.pop();
        break;
    }

    if row_chunks.len() == max_chunks {
        return total_len;
    }

    let mut low = 0usize;
    let mut high = RAW_CHUNK_SIZE;
    while low < high {
        let candidate = (low + high).div_ceil(2);
        let partial_chunk = vec![0_u8; candidate];
        let mut candidate_chunks = row_chunks.clone();
        candidate_chunks.push(partial_chunk.as_slice());
        if TableBlockEntity::from_row_chunks(
            &block_id,
            total_len + candidate,
            1,
            row_index,
            &candidate_chunks,
        )
        .is_ok()
        {
            low = candidate;
        } else {
            high = candidate - 1;
        }
    }

    total_len + low
}

fn checked_entity_property_bytes(current: usize, additional: usize) -> Result<usize, &'static str> {
    current
        .checked_add(additional)
        .ok_or("Azure Table entity property size overflowed usize")
}

fn string_property_bytes(name: &str, value: &str) -> usize {
    utf16_bytes(name) + utf16_bytes(value)
}

fn primitive_property_bytes(name: &str, value_bytes: usize) -> usize {
    utf16_bytes(name) + value_bytes
}

fn utf16_bytes(value: &str) -> usize {
    value.encode_utf16().count() * 2
}

fn has_non_empty_query_param(url: &Url, name: &str) -> bool {
    url.query_pairs()
        .any(|(candidate, value)| candidate == name && !value.is_empty())
}

fn redact_url(url: &Url) -> String {
    let mut redacted = url.clone();
    redacted.set_query(None);
    redacted.set_fragment(None);
    redacted.to_string()
}

fn http_date_now() -> String {
    httpdate::fmt_http_date(std::time::SystemTime::now())
}

fn redact_reqwest_error(error: reqwest::Error) -> String {
    error.without_url().to_string()
}

fn describe_http_response(status: StatusCode, body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        format_http_status(status)
    } else {
        format!("{}: {}", format_http_status(status), trimmed)
    }
}

fn decode_block_hash_hex(value: &str) -> Option<[u8; BlockHash::LEN]> {
    if !is_lower_hex(value, BlockHash::LEN * 2) {
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

fn is_lower_hex(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len && value.bytes().all(|byte| decode_hex_nibble(byte).is_some())
}

fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn next_retry_delay(current: Duration) -> Duration {
    std::cmp::min(current.saturating_mul(2), RETRY_MAX_DELAY)
}

fn backend_failure(message: String) -> BlockStoreError {
    BlockStoreError::BackendFailure(message)
}

fn decode_failure(message: &'static str) -> BlockStoreError {
    BlockStoreError::DecodeFailure(BlockError::InvalidEntryShape(message))
}

fn format_http_status(status: StatusCode) -> String {
    format!("HTTP {status}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use lexongraph_block::{
        Block, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_leaf_block, compute_block_hash,
        serialize_block,
    };
    use lexongraph_block_store::BlockStoreExt;
    use lexongraph_block_store::conformance::{
        BlockStoreConformanceHarness, BlockStoreFactory, run_contract_suite, run_full_suite,
    };

    #[derive(Default)]
    struct MockTableBackend {
        state: Mutex<MockState>,
    }

    #[derive(Default)]
    struct MockState {
        entities: BTreeMap<(String, String), TableBlockEntity>,
        deny_insert: bool,
        deny_get: bool,
        deny_query: bool,
        insert_transport_failures: usize,
        get_transport_failures: usize,
        query_transport_failures: usize,
        continuation_transport_failures: HashMap<Option<QueryContinuation>, usize>,
        query_pages: HashMap<Option<QueryContinuation>, EntityPage>,
        query_requests: Vec<Option<QueryContinuation>>,
    }

    impl MockTableBackend {
        fn insert_entity(&self, entity: TableBlockEntity) {
            self.state.lock().unwrap().entities.insert(
                (entity.partition_key.clone(), entity.row_key.clone()),
                entity,
            );
        }

        fn set_insert_transport_failures(&self, count: usize) {
            self.state.lock().unwrap().insert_transport_failures = count;
        }

        fn set_get_transport_failures(&self, count: usize) {
            self.state.lock().unwrap().get_transport_failures = count;
        }

        fn set_query_transport_failures(&self, count: usize) {
            self.state.lock().unwrap().query_transport_failures = count;
        }

        fn set_continuation_transport_failures(
            &self,
            continuation: Option<QueryContinuation>,
            count: usize,
        ) {
            self.state
                .lock()
                .unwrap()
                .continuation_transport_failures
                .insert(continuation, count);
        }

        fn set_query_page(&self, continuation: Option<QueryContinuation>, page: EntityPage) {
            self.state
                .lock()
                .unwrap()
                .query_pages
                .insert(continuation, page);
        }

        fn query_requests(&self) -> Vec<Option<QueryContinuation>> {
            self.state.lock().unwrap().query_requests.clone()
        }
    }

    #[async_trait]
    impl TableBackend for MockTableBackend {
        async fn insert_entity_once(
            &self,
            entity: &TableBlockEntity,
        ) -> Result<(), TableBackendAttemptError> {
            let mut state = self.state.lock().unwrap();
            if state.insert_transport_failures > 0 {
                state.insert_transport_failures -= 1;
                return Err(TableBackendAttemptError::Transport(
                    "mock insert transport failure".into(),
                ));
            }
            if state.deny_insert {
                return Err(TableBackendAttemptError::Response("HTTP 403".into()));
            }
            let key = (entity.partition_key.clone(), entity.row_key.clone());
            if state.entities.contains_key(&key) {
                return Err(TableBackendAttemptError::AlreadyExists("HTTP 409".into()));
            }
            state.entities.insert(key, entity.clone());
            Ok(())
        }

        async fn get_entity_once(
            &self,
            partition_key: &str,
            row_key: &str,
        ) -> Result<Option<TableBlockEntity>, TableBackendAttemptError> {
            let mut state = self.state.lock().unwrap();
            if state.get_transport_failures > 0 {
                state.get_transport_failures -= 1;
                return Err(TableBackendAttemptError::Transport(
                    "mock get transport failure".into(),
                ));
            }
            if state.deny_get {
                return Err(TableBackendAttemptError::Response("HTTP 403".into()));
            }
            Ok(state
                .entities
                .get(&(partition_key.to_string(), row_key.to_string()))
                .cloned())
        }

        async fn query_entities_page_once(
            &self,
            continuation: Option<QueryContinuation>,
        ) -> Result<EntityPage, TableBackendAttemptError> {
            let mut state = self.state.lock().unwrap();
            state.query_requests.push(continuation.clone());
            if let Some(remaining) = state.continuation_transport_failures.get_mut(&continuation)
                && *remaining > 0
            {
                *remaining -= 1;
                return Err(TableBackendAttemptError::Transport(
                    "mock paged query transport failure".into(),
                ));
            }
            if state.query_transport_failures > 0 {
                state.query_transport_failures -= 1;
                return Err(TableBackendAttemptError::Transport(
                    "mock query transport failure".into(),
                ));
            }
            if state.deny_query {
                return Err(TableBackendAttemptError::Response("HTTP 403".into()));
            }
            if let Some(page) = state.query_pages.get(&continuation) {
                return Ok(page.clone());
            }

            let entities = if continuation.is_none() {
                state
                    .entities
                    .values()
                    .map(|entity| TableBlockEntityMetadata {
                        partition_key: entity.partition_key.clone(),
                        row_key: entity.row_key.clone(),
                        schema_version: entity.schema_version,
                        byte_len: entity.byte_len,
                        row_count: entity.row_count,
                        row_index: entity.row_index,
                        chunk_count: entity.chunk_count,
                    })
                    .collect()
            } else {
                Vec::new()
            };
            Ok(EntityPage {
                entities,
                continuation: None,
            })
        }
    }

    fn test_store(backend: Arc<dyn TableBackend>) -> AzureTableBlockStoreV2 {
        AzureTableBlockStoreV2::from_backend_for_tests(
            "https://example.table.core.windows.net/blocks",
            backend,
        )
    }

    fn sample_leaf_block(body_len: usize) -> Block {
        Block::Leaf(
            build_leaf_block(
                VERSION_1,
                EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                vec![LeafEntry {
                    embedding: vec![0xaa, 0xbb],
                    metadata: vec![],
                    content: Content {
                        media_type: "text/plain".into(),
                        body: vec![b'x'; body_len],
                    },
                }],
                None,
            )
            .unwrap(),
        )
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[test]
    fn constructor_accepts_table_root_sas_and_rejects_invalid_paths() {
        AzureTableBlockStoreV2::new("https://acct.table.core.windows.net/blocks?sv=test&sig=fake")
            .unwrap();

        let error =
            AzureTableBlockStoreV2::new("https://acct.table.core.windows.net/?sv=test&sig=fake")
                .unwrap_err();
        assert!(format!("{error}").contains("table root"));

        let error = AzureTableBlockStoreV2::new(
            "https://acct.table.core.windows.net/blocks(PartitionKey='a',RowKey='b')?sv=test&sig=fake",
        )
        .unwrap_err();
        assert!(format!("{error}").contains("table root"));

        let error =
            AzureTableBlockStoreV2::new("https://acct.table.core.windows.net/blocks").unwrap_err();
        assert!(format!("{error}").contains("SAS query parameters"));

        let error =
            AzureTableBlockStoreV2::new("https://acct.table.core.windows.net/blocks?sv=test")
                .unwrap_err();
        assert!(format!("{error}").contains("signature parameter"));
    }

    #[test]
    fn round_trip_missing_and_multi_property_payloads_match_the_contract() {
        let backend = Arc::new(MockTableBackend::default());
        let store = test_store(backend.clone());
        let block = sample_leaf_block(70_000);
        let block_id = block_on(store.put(&block)).unwrap();
        let loaded = block_on(store.get(&block_id)).unwrap().unwrap();
        assert_eq!(loaded.hash, block_id);
        assert_eq!(loaded.block, block);
        assert_eq!(
            block_on(store.get(&BlockHash::from_bytes([0x44; 32]))).unwrap(),
            None
        );

        let orphan_hash = BlockHash::from_bytes([0x45; 32]);
        let single_row_limit = max_supported_row_payload_bytes(0, MAX_CHUNK_PROPERTY_COUNT);
        let orphan_rows = TableBlockEntity::rows_from_block_bytes(
            &orphan_hash,
            &vec![0xcc; single_row_limit + 1],
        )
        .unwrap();
        for row in orphan_rows.into_iter().skip(1) {
            backend.insert_entity(row);
        }
        assert_eq!(block_on(store.get(&orphan_hash)).unwrap(), None);
    }

    #[test]
    fn get_reports_integrity_malformed_and_backend_failures() {
        let backend = Arc::new(MockTableBackend::default());
        let store = test_store(backend.clone());

        let first = serialize_block(&sample_leaf_block(5)).unwrap();
        let second = serialize_block(&sample_leaf_block(6)).unwrap();
        backend
            .insert_entity(TableBlockEntity::from_block_bytes(&second.hash, &first.bytes).unwrap());
        assert_eq!(
            block_on(store.get(&second.hash)).unwrap_err(),
            BlockStoreError::DecodeFailure(BlockError::HashMismatch {
                expected: second.hash,
                actual: first.hash,
            })
        );

        let malformed_hash = compute_block_hash(&[0xff, 0x00, 0x01]);
        let mut malformed_entity =
            TableBlockEntity::from_block_bytes(&malformed_hash, &[0xff, 0x00, 0x01]).unwrap();
        malformed_entity
            .chunk_properties
            .insert("chunk0".into(), "***".into());
        backend.insert_entity(malformed_entity);
        assert!(matches!(
            block_on(store.get(&malformed_hash)).unwrap_err(),
            BlockStoreError::DecodeFailure(BlockError::InvalidEntryShape(_))
        ));

        let schema_hash = BlockHash::from_bytes([0x66; 32]);
        let mut schema_entity =
            TableBlockEntity::from_block_bytes(&schema_hash, b"schema").unwrap();
        schema_entity.schema_version = Some(99);
        backend.insert_entity(schema_entity);
        assert!(matches!(
            block_on(store.get(&schema_hash)).unwrap_err(),
            BlockStoreError::DecodeFailure(BlockError::InvalidEntryShape(_))
        ));

        let missing_hash = BlockHash::from_bytes([0x67; 32]);
        let mut missing_entity =
            TableBlockEntity::from_block_bytes(&missing_hash, b"missing").unwrap();
        missing_entity.byte_len = None;
        backend.insert_entity(missing_entity);
        assert!(matches!(
            block_on(store.get(&missing_hash)).unwrap_err(),
            BlockStoreError::DecodeFailure(BlockError::InvalidEntryShape(_))
        ));

        let missing_row_hash = BlockHash::from_bytes([0x68; 32]);
        let single_row_limit = max_supported_row_payload_bytes(0, MAX_CHUNK_PROPERTY_COUNT);
        let mut rows = TableBlockEntity::rows_from_block_bytes(
            &missing_row_hash,
            &vec![0xaa; single_row_limit + 1],
        )
        .unwrap();
        backend.insert_entity(rows.remove(0));
        assert!(matches!(
            block_on(store.get(&missing_row_hash)).unwrap_err(),
            BlockStoreError::DecodeFailure(BlockError::InvalidEntryShape(_))
        ));

        backend.state.lock().unwrap().deny_get = true;
        let backend_error = block_on(store.get(&first.hash)).unwrap_err();
        assert!(format!("{backend_error}").contains("HTTP 403"));
    }

    #[test]
    fn put_handles_idempotence_permissions_and_oversized_blocks() {
        let backend = Arc::new(MockTableBackend::default());
        let store = test_store(backend.clone());
        let block = sample_leaf_block(10);
        let block_id = block_on(store.put(&block)).unwrap();
        assert_eq!(block_on(store.put(&block)).unwrap(), block_id);

        backend.state.lock().unwrap().deny_insert = true;
        let denied = block_on(store.put(&sample_leaf_block(12))).unwrap_err();
        assert!(format!("{denied}").contains("HTTP 403"));

        let root_row_limit = max_supported_row_payload_bytes(0, MAX_CHUNK_PROPERTY_COUNT);
        let continuation_row_limit = max_supported_row_payload_bytes(1, MAX_CHUNK_PROPERTY_COUNT);
        let max_supported_block_len =
            root_row_limit + ((MAX_ROW_COUNT - 1) * continuation_row_limit);
        let too_large = vec![0_u8; max_supported_block_len + 1];
        let too_large_error =
            block_on(store.put_block_bytes(&BlockHash::from_bytes([0x11; 32]), &too_large))
                .unwrap_err();
        assert!(format!("{too_large_error}").contains("supported Azure Table row-set layout"));
    }

    #[test]
    fn transport_failures_retry_for_put_get_and_paged_enumeration() {
        let backend = Arc::new(MockTableBackend::default());
        backend.set_insert_transport_failures(1);
        let store = test_store(backend.clone());
        let block = sample_leaf_block(8);
        let block_id = block_on(store.put(&block)).unwrap();

        backend.set_get_transport_failures(1);
        let loaded = block_on(store.get(&block_id)).unwrap().unwrap();
        assert_eq!(loaded.hash, block_id);

        let c1 = QueryContinuation {
            partition_key: Some("eeee".into()),
            row_key: Some("eeee0000".into()),
        };
        let page1_row =
            TableBlockEntity::from_block_bytes(&BlockHash::from_bytes([0xaa; 32]), b"one").unwrap();
        let page2_row =
            TableBlockEntity::from_block_bytes(&BlockHash::from_bytes([0xbb; 32]), b"two").unwrap();
        backend.insert_entity(page1_row.clone());
        backend.insert_entity(page2_row.clone());
        let page1_entity = TableBlockEntityMetadata::from_entity(&page1_row);
        let page2_entity = TableBlockEntityMetadata::from_entity(&page2_row);
        backend.set_query_page(
            None,
            EntityPage {
                entities: vec![page1_entity],
                continuation: Some(c1.clone()),
            },
        );
        backend.set_query_page(
            Some(c1.clone()),
            EntityPage {
                entities: vec![page2_entity],
                continuation: None,
            },
        );
        backend.set_continuation_transport_failures(Some(c1.clone()), 1);

        let ids = block_on(store.list_block_ids()).unwrap();
        assert!(ids.contains(&BlockHash::from_bytes([0xaa; 32])));
        assert!(ids.contains(&BlockHash::from_bytes([0xbb; 32])));
        assert_eq!(
            backend.query_requests(),
            vec![None, Some(c1.clone()), Some(c1)]
        );
    }

    #[test]
    fn transport_retry_exhaustion_reports_backend_failures() {
        let backend = Arc::new(MockTableBackend::default());
        backend.set_insert_transport_failures(RETRY_MAX_ATTEMPTS);
        let store = test_store(backend.clone());
        let error = block_on(store.put(&sample_leaf_block(4))).unwrap_err();
        assert!(format!("{error}").contains("retry policy expired"));

        backend.set_get_transport_failures(RETRY_MAX_ATTEMPTS);
        let get_error = block_on(store.get(&BlockHash::from_bytes([0x22; 32]))).unwrap_err();
        assert!(format!("{get_error}").contains("retry policy expired"));

        backend.set_query_transport_failures(RETRY_MAX_ATTEMPTS);
        let list_error = block_on(store.list_block_ids()).unwrap_err();
        assert!(format!("{list_error}").contains("retry policy expired"));
    }

    #[test]
    fn enumeration_filters_unrelated_entities_and_fails_on_malformed_recognized_candidates() {
        let backend = Arc::new(MockTableBackend::default());
        let store = test_store(backend.clone());
        let good_row =
            TableBlockEntity::from_block_bytes(&BlockHash::from_bytes([0x55; 32]), b"good")
                .unwrap();
        backend.insert_entity(good_row.clone());
        let good = TableBlockEntityMetadata::from_entity(&good_row);
        backend.set_query_page(
            None,
            EntityPage {
                entities: vec![
                    TableBlockEntityMetadata {
                        partition_key: "notes".into(),
                        row_key: "freeform".into(),
                        schema_version: None,
                        byte_len: None,
                        row_count: None,
                        row_index: None,
                        chunk_count: None,
                    },
                    TableBlockEntityMetadata {
                        partition_key: "5555".into(),
                        row_key: format!("{}-0001", BlockHash::from_bytes([0x55; 32])),
                        schema_version: Some(ENTITY_SCHEMA_VERSION),
                        byte_len: Some(8),
                        row_count: Some(2),
                        row_index: Some(1),
                        chunk_count: Some(1),
                    },
                    good,
                ],
                continuation: None,
            },
        );
        let ids = block_on(store.list_block_ids()).unwrap();
        assert_eq!(ids, vec![BlockHash::from_bytes([0x55; 32])]);

        let malformed_backend = Arc::new(MockTableBackend::default());
        let malformed_store = test_store(malformed_backend.clone());
        malformed_backend.insert_entity(TableBlockEntity {
            partition_key: "aaaa".into(),
            row_key: format!("bbbb{}", "00".repeat(30)),
            schema_version: Some(ENTITY_SCHEMA_VERSION),
            byte_len: Some(3),
            row_count: Some(1),
            row_index: Some(0),
            chunk_count: Some(1),
            chunk_properties: BTreeMap::from([(String::from("chunk0"), BASE64.encode(b"bad"))]),
        });
        let error = block_on(malformed_store.list_block_ids()).unwrap_err();
        assert!(format!("{error}").contains("shard prefix mismatch"));

        let schema_backend = Arc::new(MockTableBackend::default());
        let schema_store = test_store(schema_backend.clone());
        let mut schema_entity =
            TableBlockEntity::from_block_bytes(&BlockHash::from_bytes([0x88; 32]), b"schema")
                .unwrap();
        schema_entity.schema_version = Some(99);
        schema_backend.insert_entity(schema_entity);
        let schema_error = block_on(schema_store.list_block_ids()).unwrap_err();
        assert!(format!("{schema_error}").contains("unsupported schema version"));

        let missing_backend = Arc::new(MockTableBackend::default());
        let missing_store = test_store(missing_backend.clone());
        missing_backend.set_query_page(
            None,
            EntityPage {
                entities: vec![TableBlockEntityMetadata {
                    partition_key: "abcd".into(),
                    row_key: format!("abcd{}", "00".repeat(30)),
                    schema_version: None,
                    byte_len: Some(3),
                    row_count: Some(1),
                    row_index: Some(0),
                    chunk_count: Some(1),
                }],
                continuation: None,
            },
        );
        let missing_error = block_on(missing_store.list_block_ids()).unwrap_err();
        assert!(format!("{missing_error}").contains("missing SchemaVersion"));

        let zero_len_backend = Arc::new(MockTableBackend::default());
        let zero_len_store = test_store(zero_len_backend.clone());
        zero_len_backend.set_query_page(
            None,
            EntityPage {
                entities: vec![TableBlockEntityMetadata {
                    partition_key: "dcba".into(),
                    row_key: format!("dcba{}", "11".repeat(30)),
                    schema_version: Some(ENTITY_SCHEMA_VERSION),
                    byte_len: Some(0),
                    row_count: Some(1),
                    row_index: Some(0),
                    chunk_count: Some(1),
                }],
                continuation: None,
            },
        );
        let zero_len_error = block_on(zero_len_store.list_block_ids()).unwrap_err();
        assert!(
            format!("{zero_len_error}")
                .contains("zero ByteLen requires a single zero-chunk root row")
        );

        let zero_row_count_backend = Arc::new(MockTableBackend::default());
        let zero_row_count_store = test_store(zero_row_count_backend.clone());
        zero_row_count_backend.set_query_page(
            None,
            EntityPage {
                entities: vec![TableBlockEntityMetadata {
                    partition_key: "cdef".into(),
                    row_key: format!("cdef{}", "22".repeat(30)),
                    schema_version: Some(ENTITY_SCHEMA_VERSION),
                    byte_len: Some(1),
                    row_count: Some(0),
                    row_index: Some(0),
                    chunk_count: Some(1),
                }],
                continuation: None,
            },
        );
        let zero_row_count_error = block_on(zero_row_count_store.list_block_ids()).unwrap_err();
        assert!(format!("{zero_row_count_error}").contains("RowCount must be positive"));

        let missing_row_backend = Arc::new(MockTableBackend::default());
        let missing_row_store = test_store(missing_row_backend.clone());
        let single_row_limit = max_supported_row_payload_bytes(0, MAX_CHUNK_PROPERTY_COUNT);
        let mut multi_row_entities = TableBlockEntity::rows_from_block_bytes(
            &BlockHash::from_bytes([0x91; 32]),
            &vec![0xdd; single_row_limit + 1],
        )
        .unwrap();
        let root_only = multi_row_entities.remove(0);
        missing_row_backend.insert_entity(root_only.clone());
        missing_row_backend.set_query_page(
            None,
            EntityPage {
                entities: vec![TableBlockEntityMetadata::from_entity(&root_only)],
                continuation: None,
            },
        );
        let missing_row_error = block_on(missing_row_store.list_block_ids()).unwrap_err();
        assert!(format!("{missing_row_error}").contains("missing row 1"));
    }

    #[test]
    fn enumeration_permission_denial_is_explicit() {
        let backend = Arc::new(MockTableBackend::default());
        backend.state.lock().unwrap().deny_query = true;
        let store = test_store(backend);
        let error = block_on(store.list_block_ids()).unwrap_err();
        assert!(format!("{error}").contains("HTTP 403"));
    }

    #[derive(Default)]
    struct Harness {
        backends: Mutex<Vec<Arc<MockTableBackend>>>,
    }

    #[derive(Clone)]
    struct HarnessStore {
        inner: AzureTableBlockStoreV2,
        backend: Arc<MockTableBackend>,
    }

    #[async_trait]
    impl BlockStore for HarnessStore {
        async fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            self.inner.put_block_bytes(block_id, block_bytes).await
        }

        async fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            self.inner.get_block_bytes(block_id).await
        }

        fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
            self.inner.iter_block_ids()
        }
    }

    #[async_trait(?Send)]
    impl BlockStoreFactory for Harness {
        type Store = HarnessStore;

        async fn fresh_store(&self) -> Self::Store {
            let backend = Arc::new(MockTableBackend::default());
            self.backends.lock().unwrap().push(backend.clone());
            HarnessStore {
                inner: test_store(backend.clone()),
                backend,
            }
        }
    }

    #[async_trait(?Send)]
    impl BlockStoreConformanceHarness for Harness {
        async fn inject_raw_bytes(
            &self,
            store: &Self::Store,
            block_id: &BlockHash,
            bytes: &[u8],
        ) -> Result<(), String> {
            let rows = TableBlockEntity::rows_from_block_bytes(block_id, bytes)
                .map_err(|error| error.to_string())?;
            for row in rows {
                store.backend.insert_entity(row);
            }
            Ok(())
        }
    }

    #[test]
    fn downstream_crates_can_run_the_contract_suite() {
        block_on(run_contract_suite(&Harness::default())).unwrap();
    }

    #[test]
    fn downstream_crates_can_run_the_full_suite() {
        block_on(run_full_suite(&Harness::default())).unwrap();
    }

    #[test]
    fn constructor_does_not_preflight_table_existence_or_permissions() {
        AzureTableBlockStoreV2::new("https://acct.table.core.windows.net/blocks?sv=test&sig=fake")
            .unwrap();
    }

    #[test]
    fn repository_can_round_trip_raw_bytes_across_multiple_rows() {
        let backend = Arc::new(MockTableBackend::default());
        let store = test_store(backend);
        let block_id = BlockHash::from_bytes([0x77; 32]);
        let single_row_limit = max_supported_row_payload_bytes(0, MAX_CHUNK_PROPERTY_COUNT);
        let bytes = vec![0xab; single_row_limit + 123];
        block_on(store.put_block_bytes(&block_id, &bytes)).unwrap();
        let loaded = block_on(store.get_block_bytes(&block_id)).unwrap().unwrap();
        assert_eq!(loaded, bytes);
    }

    #[test]
    fn entity_size_limit_uses_azure_property_footprint() {
        let block_id = BlockHash::from_bytes([0x78; 32]);
        let bytes = vec![0xab; RAW_CHUNK_SIZE * 16];
        let error = TableBlockEntity::from_block_bytes(&block_id, &bytes).unwrap_err();
        assert!(format!("{error}").contains("property data requires"));
    }

    #[test]
    fn published_rows_use_deterministic_partition_and_row_keys() {
        let backend = Arc::new(MockTableBackend::default());
        let store = test_store(backend.clone());
        let block_id = BlockHash::from_bytes([0x99; 32]);
        let single_row_limit = max_supported_row_payload_bytes(0, MAX_CHUNK_PROPERTY_COUNT);
        let bytes = vec![0xee; single_row_limit + 123];
        block_on(store.put_block_bytes(&block_id, &bytes)).unwrap();

        let row_key = block_id.to_string();
        let state = backend.state.lock().unwrap();
        let root_row = state
            .entities
            .get(&(row_key[..4].to_string(), row_key.clone()))
            .unwrap();
        assert_eq!(root_row.partition_key, row_key[..4].to_string());
        assert_eq!(root_row.row_key, row_key);
        assert!(state.entities.contains_key(&(
            block_id.to_string()[..4].to_string(),
            format!("{block_id}-0001")
        )));
    }

    #[test]
    fn response_interpretation_does_not_require_common_storage_headers() {
        let entity =
            TableBlockEntity::from_block_bytes(&BlockHash::from_bytes([0x42; 32]), b"ok").unwrap();
        let body = serde_json::to_string(&serde_json::json!({
            "value": [entity]
        }))
        .unwrap();
        let envelope: QueryEnvelope<TableBlockEntity> = serde_json::from_str(&body).unwrap();
        assert_eq!(envelope.value.len(), 1);
        assert!(ReqwestTableBackend::parse_continuation(&HeaderMap::new()).is_none());
        assert!(matches!(
            ReqwestTableBackend::response_error(StatusCode::CONFLICT, ""),
            TableBackendAttemptError::AlreadyExists(_)
        ));
        assert_eq!(
            describe_http_response(StatusCode::NO_CONTENT, ""),
            "HTTP 204 No Content"
        );
    }

    #[test]
    fn redact_reqwest_error_removes_sas_urls() {
        let error = block_on(async {
            Client::new()
                .get("http://127.0.0.1:9/blocks?sig=secret")
                .send()
                .await
                .unwrap_err()
        });
        let redacted = redact_reqwest_error(error);
        assert!(!redacted.contains("sig=secret"));
        assert!(!redacted.contains("/blocks?"));
    }

    #[test]
    fn decode_rejects_impossible_lengths_before_allocation() {
        let block_id = BlockHash::from_bytes([0x33; 32]);
        let mut entity = TableBlockEntity::from_block_bytes(&block_id, b"ok").unwrap();
        entity.chunk_count = Some(1);
        entity.byte_len = Some(i64::try_from(RAW_CHUNK_SIZE + 1).unwrap());
        assert!(matches!(
            TableBlockEntity::decode_block_bytes(&block_id, &[entity]).unwrap_err(),
            BlockStoreError::DecodeFailure(BlockError::InvalidEntryShape(_))
        ));

        let mut zero_len_entity = TableBlockEntity::from_block_bytes(&block_id, b"ok").unwrap();
        zero_len_entity.chunk_count = Some(1);
        zero_len_entity.byte_len = Some(0);
        assert!(matches!(
            TableBlockEntity::decode_block_bytes(&block_id, &[zero_len_entity]).unwrap_err(),
            BlockStoreError::DecodeFailure(BlockError::InvalidEntryShape(_))
        ));

        let mut missing_len_entity = TableBlockEntity::from_block_bytes(&block_id, b"ok").unwrap();
        missing_len_entity.byte_len = None;
        assert!(matches!(
            TableBlockEntity::decode_block_bytes(&block_id, &[missing_len_entity]).unwrap_err(),
            BlockStoreError::DecodeFailure(BlockError::InvalidEntryShape(_))
        ));
    }
}
