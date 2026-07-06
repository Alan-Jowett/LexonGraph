// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Azure Table Storage `BlockStore` implementation for LexonGraph blocks.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use azure_core::error::ErrorKind;
use azure_core::{Continuable, RetryOptions, StatusCode};
use azure_data_tables::clients::TableServiceClientBuilder;
use azure_data_tables::operations::QueryEntityResponse;
use azure_data_tables::prelude::{Filter, Select, TableClient, Top};
use azure_storage::StorageCredentials;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::{StreamExt, stream};
use lexongraph_block::{BlockError, BlockHash};
use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::sleep;
use url::Url;

const ENTITY_SCHEMA_VERSION: i32 = 1;
const MAX_ENTITY_SERIALIZED_BYTES: usize = 1_048_576;
const MAX_PROPERTY_COUNT: usize = 255;
const MAX_STRING_PROPERTY_CHARS: usize = 65_536;
const RAW_CHUNK_SIZE: usize = 48_000;
const RETRY_INITIAL_DELAY: Duration = Duration::from_millis(250);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(4);
const RETRY_MAX_ATTEMPTS: usize = 6;

#[derive(Clone)]
pub struct AzureTableBlockStore {
    backend: Arc<dyn TableBackend>,
    table_display: String,
}

impl fmt::Debug for AzureTableBlockStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureTableBlockStore")
            .field("table", &self.table_display)
            .finish()
    }
}

impl AzureTableBlockStore {
    pub fn new(table_sas_url: &str) -> Result<Self, BlockStoreError> {
        let endpoint = TableEndpoint::parse(table_sas_url)?;
        let table_display = endpoint.display.clone();
        let backend = AzureDataTablesBackend::new(&endpoint)?;
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
impl BlockStore for AzureTableBlockStore {
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
    store: AzureTableBlockStore,
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
            Some(continuation) => match &continuation.row_key {
                Some(row_key) => format!(
                    "after continuation PartitionKey={} RowKey={}",
                    continuation.partition_key, row_key
                ),
                None => format!(
                    "after continuation PartitionKey={}",
                    continuation.partition_key
                ),
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
    partition_key: String,
    row_key: Option<String>,
}

#[derive(Clone, Debug)]
struct EntityPage {
    entities: Vec<TableBlockEntityMetadata>,
    continuation: Option<QueryContinuation>,
}

#[derive(Clone)]
struct AzureDataTablesBackend {
    table_client: TableClient,
}

impl AzureDataTablesBackend {
    fn new(endpoint: &TableEndpoint) -> Result<Self, BlockStoreError> {
        let credentials = StorageCredentials::sas_token(&endpoint.sas_token).map_err(|error| {
            backend_failure(format!(
                "failed to prepare Azure Table credentials for {}: {}",
                endpoint.display, error
            ))
        })?;
        let table_service = TableServiceClientBuilder::new(endpoint.account.clone(), credentials)
            .retry(RetryOptions::none())
            .build();
        Ok(Self {
            table_client: table_service.table_client(endpoint.table_name.clone()),
        })
    }

    fn describe_error(error: &azure_core::Error) -> String {
        match error.kind() {
            ErrorKind::HttpResponse {
                status, error_code, ..
            } => {
                let mut description = format_http_status(*status);
                if let Some(error_code) = error_code {
                    description.push_str(&format!(" ({error_code})"));
                }
                description
            }
            ErrorKind::Io => error.to_string(),
            _ => error.to_string(),
        }
    }

    fn classify_insert_error(error: azure_core::Error) -> TableBackendAttemptError {
        match error.kind() {
            ErrorKind::HttpResponse {
                status: StatusCode::Conflict,
                ..
            } => TableBackendAttemptError::AlreadyExists(Self::describe_error(&error)),
            ErrorKind::Io => TableBackendAttemptError::Transport(Self::describe_error(&error)),
            _ => TableBackendAttemptError::Response(Self::describe_error(&error)),
        }
    }

    fn classify_query_error(error: azure_core::Error) -> TableBackendAttemptError {
        match error.kind() {
            ErrorKind::Io => TableBackendAttemptError::Transport(Self::describe_error(&error)),
            _ => TableBackendAttemptError::Response(Self::describe_error(&error)),
        }
    }

    fn block_lookup_filter(partition_key: &str, row_key: &str) -> Filter {
        Filter::new(format!(
            "PartitionKey eq '{partition_key}' and RowKey eq '{row_key}'"
        ))
    }

    async fn read_first_page(
        &self,
        continuation: Option<QueryContinuation>,
    ) -> Result<Option<QueryEntityResponse<TableBlockEntityMetadata>>, TableBackendAttemptError>
    {
        let mut query = self.table_client.query();
        query = query.select(Select::new(
            "PartitionKey,RowKey,SchemaVersion,ByteLen,ChunkCount",
        ));
        if let Some(continuation) = continuation {
            query = query.initial_partition_key(continuation.partition_key);
            if let Some(row_key) = continuation.row_key {
                query = query.initial_row_key(row_key);
            }
        }

        let mut pages = query.into_stream::<TableBlockEntityMetadata>();
        let Some(page) = pages.next().await else {
            return Ok(None);
        };
        page.map(Some).map_err(Self::classify_query_error)
    }
}

#[async_trait]
impl TableBackend for AzureDataTablesBackend {
    async fn insert_entity_once(
        &self,
        entity: &TableBlockEntity,
    ) -> Result<(), TableBackendAttemptError> {
        self.table_client
            .insert::<_, Value>(entity)
            .map_err(|error| TableBackendAttemptError::Response(error.to_string()))?
            .await
            .map(|_| ())
            .map_err(Self::classify_insert_error)
    }

    async fn get_entity_once(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<Option<TableBlockEntity>, TableBackendAttemptError> {
        let mut pages = self
            .table_client
            .query()
            .filter(Self::block_lookup_filter(partition_key, row_key))
            .top(Top::new(2))
            .into_stream::<TableBlockEntity>();
        let Some(page) = pages.next().await else {
            return Ok(None);
        };
        let page = page.map_err(Self::classify_query_error)?;
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
        let Some(page) = self.read_first_page(continuation).await? else {
            return Ok(EntityPage {
                entities: Vec::new(),
                continuation: None,
            });
        };
        let next_continuation =
            page.continuation()
                .map(|(partition_key, row_key)| QueryContinuation {
                    partition_key,
                    row_key,
                });
        Ok(EntityPage {
            entities: page.entities,
            continuation: next_continuation,
        })
    }
}

#[derive(Clone, Debug)]
struct TableEndpoint {
    account: String,
    table_name: String,
    sas_token: String,
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
        let account = host
            .split('.')
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                backend_failure("Azure Table SAS URL must include an account host".into())
            })?
            .to_string();

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
        Ok(Self {
            account,
            table_name,
            sas_token: url.query().unwrap_or_default().to_string(),
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
    schema_version: i32,
    #[serde(rename = "ByteLen")]
    byte_len: i64,
    #[serde(rename = "ChunkCount")]
    chunk_count: i32,
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
        if self.schema_version != ENTITY_SCHEMA_VERSION {
            return Err(format!(
                "failed to inspect Azure Table entity {} / {}: unsupported schema version {}",
                self.partition_key, self.row_key, self.schema_version
            ));
        }
        let expected_len = usize::try_from(self.byte_len).map_err(|_| {
            format!(
                "failed to inspect Azure Table entity {} / {}: ByteLen must be non-negative",
                self.partition_key, self.row_key
            )
        })?;
        let chunk_count = usize::try_from(self.chunk_count).map_err(|_| {
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
    schema_version: i32,
    #[serde(rename = "ByteLen")]
    byte_len: i64,
    #[serde(rename = "ChunkCount")]
    chunk_count: i32,
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
            schema_version: ENTITY_SCHEMA_VERSION,
            byte_len: i64::try_from(block_bytes.len()).map_err(|_| {
                backend_failure(format!(
                    "block {} length does not fit the Azure Table entity metadata representation",
                    block_id
                ))
            })?,
            chunk_count: i32::try_from(chunk_properties.len()).map_err(|_| {
                backend_failure(format!(
                    "block {} requires too many Azure Table chunk properties",
                    block_id
                ))
            })?,
            chunk_properties,
        };

        let property_count = entity.chunk_properties.len() + 5;
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

        let expected_len = usize::try_from(self.byte_len).map_err(|_| {
            decode_failure("Azure Table ByteLen must be non-negative and fit in usize")
        })?;
        let chunk_count = usize::try_from(self.chunk_count).map_err(|_| {
            decode_failure("Azure Table ChunkCount must be non-negative and fit in usize")
        })?;
        if self.schema_version != ENTITY_SCHEMA_VERSION {
            return Err(decode_failure(
                "unsupported Azure Table block entity schema version",
            ));
        }
        if chunk_count == 0 && expected_len != 0 {
            return Err(decode_failure(
                "Azure Table block entity declared non-zero ByteLen with zero chunks",
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
    format!("Chunk{index:03}")
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

    fn test_store(backend: Arc<dyn TableBackend>) -> AzureTableBlockStore {
        AzureTableBlockStore::from_backend_for_tests(
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
        AzureTableBlockStore::new("https://acct.table.core.windows.net/blocks?sv=test&sig=fake")
            .unwrap();

        let error =
            AzureTableBlockStore::new("https://acct.table.core.windows.net/?sv=test&sig=fake")
                .unwrap_err();
        assert!(format!("{error}").contains("table root"));

        let error = AzureTableBlockStore::new(
            "https://acct.table.core.windows.net/blocks(PartitionKey='a',RowKey='b')?sv=test&sig=fake",
        )
        .unwrap_err();
        assert!(format!("{error}").contains("table root"));

        let error =
            AzureTableBlockStore::new("https://acct.table.core.windows.net/blocks").unwrap_err();
        assert!(format!("{error}").contains("SAS query parameters"));

        let error = AzureTableBlockStore::new("https://acct.table.core.windows.net/blocks?sv=test")
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
            .insert("Chunk000".into(), "***".into());
        backend.insert_entity(malformed_entity);
        assert!(matches!(
            block_on(store.get(&malformed_hash)).unwrap_err(),
            BlockStoreError::DecodeFailure(BlockError::InvalidEntryShape(_))
        ));

        let schema_hash = BlockHash::from_bytes([0x66; 32]);
        let mut schema_entity =
            TableBlockEntity::from_block_bytes(&schema_hash, b"schema").unwrap();
        schema_entity.schema_version = 99;
        backend.insert_entity(schema_entity);
        assert!(matches!(
            block_on(store.get(&schema_hash)).unwrap_err(),
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
            partition_key: "eeee".into(),
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
        let good = TableBlockEntity::from_block_bytes(&BlockHash::from_bytes([0x55; 32]), b"good")
            .unwrap();
        backend.insert_entity(good);
        backend.insert_entity(TableBlockEntity {
            partition_key: "notes".into(),
            row_key: "freeform".into(),
            schema_version: ENTITY_SCHEMA_VERSION,
            byte_len: 0,
            chunk_count: 0,
            chunk_properties: BTreeMap::new(),
        });
        let ids = block_on(store.list_block_ids()).unwrap();
        assert_eq!(ids, vec![BlockHash::from_bytes([0x55; 32])]);

        let malformed_backend = Arc::new(MockTableBackend::default());
        let malformed_store = test_store(malformed_backend.clone());
        malformed_backend.insert_entity(TableBlockEntity {
            partition_key: "aaaa".into(),
            row_key: format!("bbbb{}", "00".repeat(30)),
            schema_version: ENTITY_SCHEMA_VERSION,
            byte_len: 3,
            chunk_count: 1,
            chunk_properties: BTreeMap::from([(String::from("Chunk000"), BASE64.encode(b"bad"))]),
        });
        let error = block_on(malformed_store.list_block_ids()).unwrap_err();
        assert!(format!("{error}").contains("shard prefix mismatch"));

        let schema_backend = Arc::new(MockTableBackend::default());
        let schema_store = test_store(schema_backend.clone());
        let mut schema_entity =
            TableBlockEntity::from_block_bytes(&BlockHash::from_bytes([0x88; 32]), b"schema")
                .unwrap();
        schema_entity.schema_version = 99;
        schema_backend.insert_entity(schema_entity);
        let schema_error = block_on(schema_store.list_block_ids()).unwrap_err();
        assert!(format!("{schema_error}").contains("unsupported schema version"));
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
        inner: AzureTableBlockStore,
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
        AzureTableBlockStore::new("https://acct.table.core.windows.net/blocks?sv=test&sig=fake")
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
}
