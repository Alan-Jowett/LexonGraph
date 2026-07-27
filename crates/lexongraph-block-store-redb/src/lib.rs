// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Redb-backed durable local `BlockStore` implementation for LexonGraph blocks.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use lexongraph_block::BlockHash;
use lexongraph_block_store::{
    BlockBytesBatchEntry, BlockIdStream, BlockStore, BlockStoreError, BlockStoreTelemetryCallback,
    BlockStoreTelemetryEvent,
};
use redb::{Database, Durability, ReadableTable, RepairSession, TableDefinition};

const BLOCKS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("blocks");
const DATABASE_FILE_NAME: &str = "blocks.redb";

#[derive(Clone)]
pub struct RedbBlockStore {
    store_root: PathBuf,
    state: Arc<SharedState>,
}

struct SharedState {
    database: Database,
    durability_mode: RedbBlockStoreDurabilityMode,
    pending_flush: AtomicBool,
    telemetry_callback: Arc<Mutex<Option<BlockStoreTelemetryCallback>>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RedbBlockStoreDurabilityMode {
    #[default]
    Durable,
    Fast,
}

impl std::fmt::Debug for RedbBlockStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbBlockStore")
            .field("store_root", &self.store_root)
            .field("durability_mode", &self.state.durability_mode)
            .finish()
    }
}

impl RedbBlockStore {
    pub fn new(store_root: impl AsRef<Path>) -> Result<Self, BlockStoreError> {
        Self::new_with_durability_and_telemetry(
            store_root,
            RedbBlockStoreDurabilityMode::Durable,
            None,
        )
    }

    pub fn new_with_durability(
        store_root: impl AsRef<Path>,
        durability_mode: RedbBlockStoreDurabilityMode,
    ) -> Result<Self, BlockStoreError> {
        Self::new_with_durability_and_telemetry(store_root, durability_mode, None)
    }

    pub fn new_with_telemetry(
        store_root: impl AsRef<Path>,
        telemetry_callback: BlockStoreTelemetryCallback,
    ) -> Result<Self, BlockStoreError> {
        Self::new_with_durability_and_telemetry(
            store_root,
            RedbBlockStoreDurabilityMode::Durable,
            Some(telemetry_callback),
        )
    }

    pub fn new_with_durability_and_telemetry(
        store_root: impl AsRef<Path>,
        durability_mode: RedbBlockStoreDurabilityMode,
        telemetry_callback: Option<BlockStoreTelemetryCallback>,
    ) -> Result<Self, BlockStoreError> {
        let requested_root = store_root.as_ref();
        std::fs::create_dir_all(requested_root).map_err(|error| {
            backend_failure(format!(
                "failed to create store root {}: {error}",
                requested_root.display()
            ))
        })?;

        let canonical_root = requested_root.canonicalize().map_err(|error| {
            backend_failure(format!(
                "failed to canonicalize store root {}: {error}",
                requested_root.display()
            ))
        })?;

        let metadata = std::fs::metadata(&canonical_root).map_err(|error| {
            backend_failure(format!(
                "failed to stat store root {}: {error}",
                canonical_root.display()
            ))
        })?;
        if !metadata.is_dir() {
            return Err(backend_failure(format!(
                "store root {} is not a directory",
                canonical_root.display()
            )));
        }

        let database_path = canonical_root.join(DATABASE_FILE_NAME);
        let telemetry_callback = Arc::new(Mutex::new(telemetry_callback));
        let database = open_database(&database_path, telemetry_callback.clone())?;
        initialize_blocks_table(&database, &database_path)?;
        let state = Arc::new(SharedState {
            database,
            durability_mode,
            pending_flush: AtomicBool::new(false),
            telemetry_callback,
        });

        Ok(Self {
            store_root: canonical_root,
            state,
        })
    }

    pub fn compact_now(&mut self) -> Result<(), BlockStoreError> {
        let state = Arc::get_mut(&mut self.state).ok_or_else(|| {
            backend_failure(format!(
                "failed to compact redb database under {} because exclusive store ownership is required",
                self.store_root.display()
            ))
        })?;
        state.flush_pending_writes_before_compaction()?;
        state
            .database
            .compact()
            .map_err(|error| {
                backend_failure(format!(
                    "failed to compact redb database under {}: {error}",
                    self.store_root.display()
                ))
            })
            .map(|_| ())
    }

    #[cfg(feature = "inject")]
    pub fn raw_insert(&self, block_id: BlockHash, bytes: Vec<u8>) -> Result<(), BlockStoreError> {
        self.raw_insert_key_value(block_id.as_bytes().to_vec(), bytes)
    }

    #[cfg(feature = "inject")]
    pub fn raw_insert_key_value(
        &self,
        key: Vec<u8>,
        bytes: Vec<u8>,
    ) -> Result<(), BlockStoreError> {
        let write_txn = self.state.database.begin_write().map_err(|error| {
            backend_failure(format!(
                "failed to start a redb write transaction for test injection: {error}"
            ))
        })?;
        {
            let mut table = write_txn.open_table(BLOCKS_TABLE).map_err(|error| {
                backend_failure(format!(
                    "failed to open the redb block table for test injection: {error}"
                ))
            })?;
            table
                .insert(key.as_slice(), bytes.as_slice())
                .map_err(|error| {
                    backend_failure(format!(
                        "failed to inject raw bytes into the redb block table: {error}"
                    ))
                })?;
        }
        write_txn.commit().map_err(|error| {
            backend_failure(format!(
                "failed to commit a redb write transaction for test injection: {error}"
            ))
        })
    }

    #[cfg(feature = "inject")]
    pub fn pending_fast_mode_flush(&self) -> bool {
        self.state.pending_flush.load(Ordering::Acquire)
    }
}

#[async_trait]
impl BlockStore for RedbBlockStore {
    fn set_telemetry_callback(
        &self,
        telemetry_callback: Option<BlockStoreTelemetryCallback>,
    ) -> Result<(), BlockStoreError> {
        *self.state.telemetry_callback.lock().map_err(|_| {
            backend_failure(format!(
                "failed to update redb telemetry callback for {} because the callback state lock is poisoned",
                self.store_root.display()
            ))
        })? = telemetry_callback;
        Ok(())
    }

    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        let mut write_txn = self.state.database.begin_write().map_err(|error| {
            backend_failure(format!(
                "failed to start a redb write transaction for block {}: {error}",
                block_id
            ))
        })?;
        if self.state.durability_mode == RedbBlockStoreDurabilityMode::Fast {
            write_txn.set_durability(Durability::None);
        }
        let should_commit = {
            let mut table = write_txn.open_table(BLOCKS_TABLE).map_err(|error| {
                backend_failure(format!(
                    "failed to open the redb block table for block {}: {error}",
                    block_id
                ))
            })?;
            enum ExistingEntryState {
                MatchingBytes,
                ConflictingBytes,
                Missing,
            }

            let existing_state = {
                let existing = table.get(&block_id.as_bytes()[..]).map_err(|error| {
                    backend_failure(format!(
                        "failed to inspect persisted redb bytes for block {}: {error}",
                        block_id
                    ))
                })?;
                match existing {
                    Some(existing) if existing.value() == block_bytes => {
                        ExistingEntryState::MatchingBytes
                    }
                    Some(_) => ExistingEntryState::ConflictingBytes,
                    None => ExistingEntryState::Missing,
                }
            };

            match existing_state {
                ExistingEntryState::MatchingBytes => false,
                ExistingEntryState::ConflictingBytes => {
                    return Err(backend_failure(format!(
                        "integrity conflict for block {} in the redb block table",
                        block_id
                    )));
                }
                ExistingEntryState::Missing => {
                    table
                        .insert(&block_id.as_bytes()[..], block_bytes)
                        .map_err(|error| {
                            backend_failure(format!(
                                "failed to persist block {} into the redb block table: {error}",
                                block_id
                            ))
                        })?;
                    true
                }
            }
        };
        if !should_commit {
            return Ok(());
        }
        write_txn.commit().map_err(|error| {
            backend_failure(format!(
                "failed to commit persisted redb bytes for block {}: {error}",
                block_id
            ))
        })?;
        if self.state.durability_mode == RedbBlockStoreDurabilityMode::Fast {
            self.state.pending_flush.store(true, Ordering::Release);
        }
        Ok(())
    }

    async fn put_block_bytes_batch(
        &self,
        entries: &[BlockBytesBatchEntry<'_>],
    ) -> Result<(), BlockStoreError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut write_txn = self.state.database.begin_write().map_err(|error| {
            backend_failure(format!(
                "failed to start a redb write transaction for a block batch: {error}"
            ))
        })?;
        if self.state.durability_mode == RedbBlockStoreDurabilityMode::Fast {
            write_txn.set_durability(Durability::None);
        }
        let should_commit = {
            let mut table = write_txn.open_table(BLOCKS_TABLE).map_err(|error| {
                backend_failure(format!(
                    "failed to open the redb block table for a block batch: {error}"
                ))
            })?;
            let mut inserted_any = false;

            for entry in entries {
                enum ExistingEntryState {
                    MatchingBytes,
                    ConflictingBytes,
                    Missing,
                }

                let existing_state = {
                    let existing = table.get(&entry.block_id.as_bytes()[..]).map_err(|error| {
                        backend_failure(format!(
                            "failed to inspect persisted redb bytes for block {} during batch write: {error}",
                            entry.block_id
                        ))
                    })?;
                    match existing {
                        Some(existing) if existing.value() == entry.block_bytes => {
                            ExistingEntryState::MatchingBytes
                        }
                        Some(_) => ExistingEntryState::ConflictingBytes,
                        None => ExistingEntryState::Missing,
                    }
                };

                match existing_state {
                    ExistingEntryState::MatchingBytes => {}
                    ExistingEntryState::ConflictingBytes => {
                        return Err(backend_failure(format!(
                            "integrity conflict for block {} in the redb block table during batch write",
                            entry.block_id
                        )));
                    }
                    ExistingEntryState::Missing => {
                        table
                            .insert(&entry.block_id.as_bytes()[..], entry.block_bytes)
                            .map_err(|error| {
                                backend_failure(format!(
                                    "failed to persist block {} into the redb block table during batch write: {error}",
                                    entry.block_id
                                ))
                            })?;
                        inserted_any = true;
                    }
                }
            }

            inserted_any
        };
        if !should_commit {
            return Ok(());
        }
        write_txn.commit().map_err(|error| {
            backend_failure(format!(
                "failed to commit persisted redb bytes for a block batch: {error}"
            ))
        })?;
        if self.state.durability_mode == RedbBlockStoreDurabilityMode::Fast {
            self.state.pending_flush.store(true, Ordering::Release);
        }
        Ok(())
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let read_txn = self.state.database.begin_read().map_err(|error| {
            backend_failure(format!(
                "failed to start a redb read transaction for block {}: {error}",
                block_id
            ))
        })?;
        let table = read_txn.open_table(BLOCKS_TABLE).map_err(|error| {
            backend_failure(format!(
                "failed to open the redb block table for block {}: {error}",
                block_id
            ))
        })?;
        table
            .get(&block_id.as_bytes()[..])
            .map_err(|error| {
                backend_failure(format!(
                    "failed to read block {} from the redb block table: {error}",
                    block_id
                ))
            })
            .map(|value| value.map(|value| value.value().to_vec()))
    }

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
        let read_txn = self.state.database.begin_read().map_err(|error| {
            backend_failure(format!(
                "failed to start a redb read transaction for block enumeration: {error}"
            ))
        })?;
        let table = read_txn.open_table(BLOCKS_TABLE).map_err(|error| {
            backend_failure(format!(
                "failed to open the redb block table for block enumeration: {error}"
            ))
        })?;
        let iter = table.iter().map_err(|error| {
            backend_failure(format!(
                "failed to iterate the redb block table for block enumeration: {error}"
            ))
        })?;

        let mut block_ids = Vec::new();
        for entry in iter {
            let (key, _) = entry.map_err(|error| {
                backend_failure(format!(
                    "failed while iterating the redb block table for block enumeration: {error}"
                ))
            })?;
            let key_bytes = key.value();
            let hash_bytes: [u8; BlockHash::LEN] = key_bytes.try_into().map_err(|_| {
                backend_failure(format!(
                    "failed to decode an enumerated redb block key of {} bytes into a block ID",
                    key_bytes.len()
                ))
            })?;
            block_ids.push(BlockHash::from_bytes(hash_bytes));
        }

        Ok(Box::pin(stream::iter(block_ids.into_iter().map(Ok))))
    }
}

impl SharedState {
    fn flush_pending_writes_on_shutdown(&self) -> Result<(), BlockStoreError> {
        self.flush_pending_writes(
            "failed to start a fast-mode graceful-shutdown redb write transaction",
            "failed to flush pending fast-mode redb writes during graceful shutdown",
        )
    }

    fn flush_pending_writes_before_compaction(&self) -> Result<(), BlockStoreError> {
        self.flush_pending_writes(
            "failed to start a pre-compaction redb write transaction",
            "failed to flush pending fast-mode redb writes before compaction",
        )
    }

    fn flush_pending_writes(
        &self,
        begin_context: &str,
        commit_context: &str,
    ) -> Result<(), BlockStoreError> {
        if self.durability_mode != RedbBlockStoreDurabilityMode::Fast
            || !self.pending_flush.load(Ordering::Acquire)
        {
            return Ok(());
        }

        let mut write_txn = self
            .database
            .begin_write()
            .map_err(|error| backend_failure(format!("{begin_context}: {error}")))?;
        write_txn.set_durability(Durability::Immediate);
        write_txn
            .commit()
            .map_err(|error| backend_failure(format!("{commit_context}: {error}")))?;
        self.pending_flush.store(false, Ordering::Release);
        Ok(())
    }
}

impl Drop for SharedState {
    fn drop(&mut self) {
        if let Err(error) = self.flush_pending_writes_on_shutdown() {
            eprintln!("fast-mode graceful shutdown flush failed: {error}");
        }
    }
}

fn initialize_blocks_table(
    database: &Database,
    database_path: &Path,
) -> Result<(), BlockStoreError> {
    let write_txn = database.begin_write().map_err(|error| {
        backend_failure(format!(
            "failed to start a redb initialization transaction for {}: {error}",
            database_path.display()
        ))
    })?;
    {
        write_txn.open_table(BLOCKS_TABLE).map_err(|error| {
            backend_failure(format!(
                "failed to initialize the redb block table in {}: {error}",
                database_path.display()
            ))
        })?;
    }
    write_txn.commit().map_err(|error| {
        backend_failure(format!(
            "failed to commit redb initialization for {}: {error}",
            database_path.display()
        ))
    })
}

fn open_database(
    database_path: &Path,
    telemetry_callback: Arc<Mutex<Option<BlockStoreTelemetryCallback>>>,
) -> Result<Database, BlockStoreError> {
    let mut builder = Database::builder();
    let database_path_for_callback = database_path.to_path_buf();
    builder.set_repair_callback(move |session| {
        emit_repair_telemetry(&telemetry_callback, &database_path_for_callback, session);
    });
    builder.create(database_path).map_err(|error| {
        backend_failure(format!(
            "failed to open redb database {}: {error}",
            database_path.display()
        ))
    })
}

fn emit_repair_telemetry(
    telemetry_callback: &Mutex<Option<BlockStoreTelemetryCallback>>,
    database_path: &Path,
    session: &RepairSession,
) {
    let callback = telemetry_callback
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if let Some(callback) = callback {
        let event = BlockStoreTelemetryEvent::new("repair_status")
            .with_message("redb reported database repair progress")
            .with_attribute("backend", "redb")
            .with_attribute("database_path", database_path.display().to_string())
            .with_attribute("progress", format!("{:.3}", session.progress()));
        if catch_unwind(AssertUnwindSafe(|| callback(event))).is_err() {
            eprintln!(
                "redb repair telemetry callback panicked for {}",
                database_path.display()
            );
        }
    }
}

fn backend_failure(message: String) -> BlockStoreError {
    BlockStoreError::BackendFailure(message)
}
