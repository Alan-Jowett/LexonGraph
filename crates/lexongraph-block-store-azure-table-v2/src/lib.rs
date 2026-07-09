// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Azure Table Storage `BlockStore` implementation for LexonGraph blocks.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::Arc;
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

const ENTITY_SCHEMA_VERSION: i32 = 2;
const MAX_ENTITY_SERIALIZED_BYTES: usize = 1_048_576;
const MAX_PROPERTY_COUNT: usize = 255;
const FIXED_ENTITY_PROPERTY_COUNT: usize = 5;
const MAX_CHUNK_PROPERTY_COUNT: usize = MAX_PROPERTY_COUNT - FIXED_ENTITY_PROPERTY_COUNT;
const MAX_STRING_PROPERTY_CHARS: usize = 32 * 1024;
const RAW_CHUNK_SIZE: usize = 24_576;
const RETRY_INITIAL_DELAY: Duration = Duration::from_millis(250);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(4);
const RETRY_MAX_ATTEMPTS: usize = 6;
const AZURE_TABLE_API_VERSION: &str = "2019-02-02";
const ODATA_NO_METADATA: &str = "application/json;odata=nometadata";
const PREFER_RETURN_NO_CONTENT: &str = "return-no-content";

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
        let hex = block_id.to_string();
        hex[..4].to_string()
    }

    fn row_key(block_id: &BlockHash) -> String {
        block_id.to_string()
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
        let entity = TableBlockEntity::from_block_bytes(block_id, block_bytes)?;
        let mut attempts = 0usize;
        let mut delay = RETRY_INITIAL_DELAY;
        loop {
            attempts += 1;
            match self.backend.insert_entity_once(&entity).await {
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

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let partition_key = Self::partition_key(block_id);
        let row_key = Self::row_key(block_id);
        let mut attempts = 0usize;
        let mut delay = RETRY_INITIAL_DELAY;
        loop {
            attempts += 1;
            match self.backend.get_entity_once(&partition_key, &row_key).await {
                Ok(None) => return Ok(None),
                Ok(Some(entity)) => return entity.decode_block_bytes(block_id).map(Some),
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
                            Ok(Some(block_id)) => return Ok(Some((block_id, state))),
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
            "PartitionKey,RowKey,SchemaVersion,ByteLen,ChunkCount".to_string(),
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
        decode_recognized_block_entity_keys(&self.partition_key, &self.row_key)
    }

    fn validate_enumeration_payload(&self) -> Result<(), String> {
        let schema_version = self.schema_version.ok_or_else(|| {
            format!(
                "failed to inspect Azure Table entity {} / {}: missing SchemaVersion",
                self.partition_key, self.row_key
            )
        })?;
        if schema_version != ENTITY_SCHEMA_VERSION {
            return Err(format!(
                "failed to inspect Azure Table entity {} / {}: unsupported schema version {}",
                self.partition_key, self.row_key, schema_version
            ));
        }
        let expected_len = usize::try_from(self.byte_len.ok_or_else(|| {
            format!(
                "failed to inspect Azure Table entity {} / {}: missing ByteLen",
                self.partition_key, self.row_key
            )
        })?)
        .map_err(|_| {
            format!(
                "failed to inspect Azure Table entity {} / {}: ByteLen must be non-negative",
                self.partition_key, self.row_key
            )
        })?;
        let chunk_count = usize::try_from(self.chunk_count.ok_or_else(|| {
            format!(
                "failed to inspect Azure Table entity {} / {}: missing ChunkCount",
                self.partition_key, self.row_key
            )
        })?)
        .map_err(|_| {
            format!(
                "failed to inspect Azure Table entity {} / {}: ChunkCount must be non-negative",
                self.partition_key, self.row_key
            )
        })?;
        if chunk_count == 0 && expected_len != 0 {
            return Err(format!(
                "failed to inspect Azure Table entity {} / {}: non-zero ByteLen requires at least one chunk",
                self.partition_key, self.row_key
            ));
        }
        if expected_len == 0 && chunk_count != 0 {
            return Err(format!(
                "failed to inspect Azure Table entity {} / {}: zero ByteLen requires zero chunks",
                self.partition_key, self.row_key
            ));
        }
        if chunk_count > MAX_CHUNK_PROPERTY_COUNT {
            return Err(format!(
                "failed to inspect Azure Table entity {} / {}: ChunkCount exceeds the supported per-entity limit",
                self.partition_key, self.row_key
            ));
        }
        let max_possible_len = chunk_count
            .checked_mul(RAW_CHUNK_SIZE)
            .ok_or_else(|| {
                format!(
                    "failed to inspect Azure Table entity {} / {}: ChunkCount exceeds the supported per-entity limit",
                    self.partition_key, self.row_key
                )
            })?;
        if expected_len > max_possible_len {
            return Err(format!(
                "failed to inspect Azure Table entity {} / {}: ByteLen exceeds the capacity implied by ChunkCount",
                self.partition_key, self.row_key
            ));
        }
        Ok(())
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
    #[serde(rename = "ChunkCount")]
    chunk_count: Option<i32>,
    #[serde(flatten)]
    chunk_properties: BTreeMap<String, String>,
}

impl TableBlockEntity {
    fn from_block_bytes(block_id: &BlockHash, block_bytes: &[u8]) -> Result<Self, BlockStoreError> {
        let row_key = block_id.to_string();
        let partition_key = row_key[..4].to_string();
        let mut chunk_properties = BTreeMap::new();
        for (index, chunk) in block_bytes.chunks(RAW_CHUNK_SIZE).enumerate() {
            let encoded = BASE64.encode(chunk);
            if encoded.len() > MAX_STRING_PROPERTY_CHARS {
                return Err(backend_failure(format!(
                    "block {} cannot be encoded into one Azure Table entity in {}: chunk {} requires {} characters and exceeds the per-property limit of {}",
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
            byte_len: Some(i64::try_from(block_bytes.len()).map_err(|_| {
                backend_failure(format!(
                    "block {} length does not fit the Azure Table entity metadata representation",
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

        let property_count = entity.chunk_properties.len() + FIXED_ENTITY_PROPERTY_COUNT;
        if property_count > MAX_PROPERTY_COUNT {
            return Err(backend_failure(format!(
                "block {} cannot fit within one Azure Table entity because it requires {} properties and the limit is {}",
                block_id, property_count, MAX_PROPERTY_COUNT
            )));
        }

        let serialized = serde_json::to_vec(&entity).map_err(|error| {
            backend_failure(format!(
                "failed to encode Azure Table entity for block {}: {}",
                block_id, error
            ))
        })?;
        if serialized.len() > MAX_ENTITY_SERIALIZED_BYTES {
            return Err(backend_failure(format!(
                "block {} cannot fit within one Azure Table entity because the encoded entity requires {} bytes and the limit is {}",
                block_id,
                serialized.len(),
                MAX_ENTITY_SERIALIZED_BYTES
            )));
        }

        Ok(entity)
    }

    fn decode_block_bytes(
        &self,
        expected_block_id: &BlockHash,
    ) -> Result<Vec<u8>, BlockStoreError> {
        match self.recognized_block_id() {
            Ok(Some(block_id)) if &block_id == expected_block_id => {}
            Ok(Some(block_id)) => {
                return Err(backend_failure(format!(
                    "Azure Table entity keys {} / {} resolved to block {} instead of requested {}",
                    self.partition_key, self.row_key, block_id, expected_block_id
                )));
            }
            Ok(None) => {
                return Err(backend_failure(format!(
                    "Azure Table lookup for {} returned unrelated entity keys {} / {}",
                    expected_block_id, self.partition_key, self.row_key
                )));
            }
            Err(_) => return Err(decode_failure("invalid Azure Table entity key mapping")),
        }

        let expected_len = usize::try_from(
            self.byte_len
                .ok_or_else(|| decode_failure("Azure Table block entity is missing ByteLen"))?,
        )
        .map_err(|_| decode_failure("Azure Table ByteLen must be non-negative and fit in usize"))?;
        let chunk_count = usize::try_from(
            self.chunk_count
                .ok_or_else(|| decode_failure("Azure Table block entity is missing ChunkCount"))?,
        )
        .map_err(|_| {
            decode_failure("Azure Table ChunkCount must be non-negative and fit in usize")
        })?;
        let schema_version = self
            .schema_version
            .ok_or_else(|| decode_failure("Azure Table block entity is missing SchemaVersion"))?;
        if schema_version != ENTITY_SCHEMA_VERSION {
            return Err(decode_failure(
                "unsupported Azure Table block entity schema version",
            ));
        }
        if chunk_count == 0 && expected_len != 0 {
            return Err(decode_failure(
                "Azure Table block entity declared non-zero ByteLen with zero chunks",
            ));
        }
        if expected_len == 0 && chunk_count != 0 {
            return Err(decode_failure(
                "Azure Table block entity declared zero ByteLen with non-zero chunks",
            ));
        }
        if chunk_count > MAX_CHUNK_PROPERTY_COUNT {
            return Err(decode_failure(
                "Azure Table block entity declared too many chunks",
            ));
        }
        let max_possible_len = chunk_count
            .checked_mul(RAW_CHUNK_SIZE)
            .ok_or_else(|| decode_failure("Azure Table ChunkCount exceeds the supported limit"))?;
        if expected_len > max_possible_len {
            return Err(decode_failure(
                "Azure Table block entity declared a ByteLen larger than ChunkCount can contain",
            ));
        }
        if self.chunk_properties.len() < chunk_count {
            return Err(decode_failure(
                "Azure Table block entity is missing a required chunk property",
            ));
        }

        let mut block_bytes = Vec::with_capacity(expected_len);
        for index in 0..chunk_count {
            let property_name = chunk_property_name(index);
            let encoded = self.chunk_properties.get(&property_name).ok_or_else(|| {
                decode_failure("Azure Table block entity is missing a required chunk property")
            })?;
            let decoded = BASE64.decode(encoded).map_err(|error| {
                let _ = error;
                decode_failure("Azure Table block entity chunk is not valid base64")
            })?;
            block_bytes.extend_from_slice(&decoded);
        }

        if block_bytes.len() != expected_len {
            return Err(decode_failure(
                "Azure Table block entity reconstructed an unexpected number of bytes",
            ));
        }

        Ok(block_bytes)
    }

    fn recognized_block_id(&self) -> Result<Option<BlockHash>, String> {
        decode_recognized_block_entity_keys(&self.partition_key, &self.row_key)
    }
}

fn decode_recognized_block_entity_keys(
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
        let store = test_store(backend);
        let block = sample_leaf_block(70_000);
        let block_id = block_on(store.put(&block)).unwrap();
        let loaded = block_on(store.get(&block_id)).unwrap().unwrap();
        assert_eq!(loaded.hash, block_id);
        assert_eq!(loaded.block, block);
        assert_eq!(
            block_on(store.get(&BlockHash::from_bytes([0x44; 32]))).unwrap(),
            None
        );
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

        let too_large = vec![0_u8; 900_000];
        let too_large_error =
            block_on(store.put_block_bytes(&BlockHash::from_bytes([0x11; 32]), &too_large))
                .unwrap_err();
        assert!(format!("{too_large_error}").contains("cannot fit within one Azure Table entity"));
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
        let page1_entity = TableBlockEntityMetadata::from_entity(
            &TableBlockEntity::from_block_bytes(&BlockHash::from_bytes([0xaa; 32]), b"one")
                .unwrap(),
        );
        let page2_entity = TableBlockEntityMetadata::from_entity(
            &TableBlockEntity::from_block_bytes(&BlockHash::from_bytes([0xbb; 32]), b"two")
                .unwrap(),
        );
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
        let good = TableBlockEntityMetadata::from_entity(
            &TableBlockEntity::from_block_bytes(&BlockHash::from_bytes([0x55; 32]), b"good")
                .unwrap(),
        );
        backend.set_query_page(
            None,
            EntityPage {
                entities: vec![
                    TableBlockEntityMetadata {
                        partition_key: "notes".into(),
                        row_key: "freeform".into(),
                        schema_version: None,
                        byte_len: None,
                        chunk_count: None,
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
                    chunk_count: Some(1),
                }],
                continuation: None,
            },
        );
        let zero_len_error = block_on(zero_len_store.list_block_ids()).unwrap_err();
        assert!(format!("{zero_len_error}").contains("zero ByteLen requires zero chunks"));
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
            let entity = TableBlockEntity::from_block_bytes(block_id, bytes)
                .map_err(|error| error.to_string())?;
            store.backend.insert_entity(entity);
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
    fn repository_can_round_trip_raw_bytes_above_the_single_property_limit() {
        let backend = Arc::new(MockTableBackend::default());
        let store = test_store(backend);
        let block_id = BlockHash::from_bytes([0x77; 32]);
        let bytes = vec![0xab; RAW_CHUNK_SIZE + 123];
        block_on(store.put_block_bytes(&block_id, &bytes)).unwrap();
        let loaded = block_on(store.get_block_bytes(&block_id)).unwrap().unwrap();
        assert_eq!(loaded, bytes);
    }

    #[test]
    fn published_entities_use_deterministic_partition_and_row_keys() {
        let backend = Arc::new(MockTableBackend::default());
        let store = test_store(backend.clone());
        let block_id = BlockHash::from_bytes([0x99; 32]);
        block_on(store.put_block_bytes(&block_id, b"keys")).unwrap();

        let row_key = block_id.to_string();
        let state = backend.state.lock().unwrap();
        let entity = state
            .entities
            .get(&(row_key[..4].to_string(), row_key.clone()))
            .unwrap();
        assert_eq!(entity.partition_key, row_key[..4].to_string());
        assert_eq!(entity.row_key, row_key);
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
            entity.decode_block_bytes(&block_id).unwrap_err(),
            BlockStoreError::DecodeFailure(BlockError::InvalidEntryShape(_))
        ));

        let mut zero_len_entity = TableBlockEntity::from_block_bytes(&block_id, b"ok").unwrap();
        zero_len_entity.chunk_count = Some(1);
        zero_len_entity.byte_len = Some(0);
        assert!(matches!(
            zero_len_entity.decode_block_bytes(&block_id).unwrap_err(),
            BlockStoreError::DecodeFailure(BlockError::InvalidEntryShape(_))
        ));

        let mut missing_len_entity = TableBlockEntity::from_block_bytes(&block_id, b"ok").unwrap();
        missing_len_entity.byte_len = None;
        assert!(matches!(
            missing_len_entity
                .decode_block_bytes(&block_id)
                .unwrap_err(),
            BlockStoreError::DecodeFailure(BlockError::InvalidEntryShape(_))
        ));
    }
}
