// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Redb-backed durable local `BlockStore` implementation for LexonGraph blocks.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use lexongraph_block::BlockHash;
use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};
use redb::{Database, ReadableTable, TableDefinition};

const BLOCKS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("blocks");
const DATABASE_FILE_NAME: &str = "blocks.redb";

#[derive(Clone)]
pub struct RedbBlockStore {
    store_root: PathBuf,
    database: Arc<Database>,
}

impl std::fmt::Debug for RedbBlockStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbBlockStore")
            .field("store_root", &self.store_root)
            .finish()
    }
}

impl RedbBlockStore {
    pub fn new(store_root: impl AsRef<Path>) -> Result<Self, BlockStoreError> {
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
        let database = Database::create(&database_path).map_err(|error| {
            backend_failure(format!(
                "failed to open redb database {}: {error}",
                database_path.display()
            ))
        })?;
        initialize_blocks_table(&database, &database_path)?;

        Ok(Self {
            store_root: canonical_root,
            database: Arc::new(database),
        })
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
        let write_txn = self.database.begin_write().map_err(|error| {
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
}

#[async_trait]
impl BlockStore for RedbBlockStore {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        let write_txn = self.database.begin_write().map_err(|error| {
            backend_failure(format!(
                "failed to start a redb write transaction for block {}: {error}",
                block_id
            ))
        })?;
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
        })
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let read_txn = self.database.begin_read().map_err(|error| {
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
        let read_txn = self.database.begin_read().map_err(|error| {
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

fn backend_failure(message: String) -> BlockStoreError {
    BlockStoreError::BackendFailure(message)
}
