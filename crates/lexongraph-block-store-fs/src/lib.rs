//! Local-filesystem `BlockStore` implementation for LexonGraph blocks.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use lexongraph_block::{
    Block, BlockError, BlockHash, ValidatedBlock, deserialize_block, serialize_block,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use tempfile::Builder;

#[derive(Clone, Debug)]
pub struct FilesystemBlockStore {
    store_root: PathBuf,
}

impl FilesystemBlockStore {
    pub fn new(store_root: impl AsRef<Path>) -> Result<Self, BlockStoreError> {
        let requested_root = store_root.as_ref();
        fs::create_dir_all(requested_root).map_err(|error| {
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

        let metadata = fs::metadata(&canonical_root).map_err(|error| {
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

        Ok(Self {
            store_root: canonical_root,
        })
    }

    fn block_path(&self, block_id: &BlockHash) -> PathBuf {
        let hex = block_id.to_string();
        let (first_level, rest) = hex.split_at(2);
        let (second_level, _) = rest.split_at(2);
        self.store_root
            .join(first_level)
            .join(second_level)
            .join(format!("{hex}.cbor"))
    }

    fn read_existing_or_map_publish_error(
        &self,
        published_path: &Path,
        block_id: &BlockHash,
        canonical_bytes: &[u8],
        error: std::io::Error,
    ) -> Result<BlockHash, BlockStoreError> {
        match fs::read(published_path) {
            Ok(existing_bytes) if existing_bytes == canonical_bytes => Ok(*block_id),
            Ok(_) => Err(backend_failure(format!(
                "integrity conflict at {} for block {} after publish error {error}",
                published_path.display(),
                block_id
            ))),
            Err(read_error) if read_error.kind() == std::io::ErrorKind::NotFound => {
                Err(backend_failure(format!(
                    "failed to publish block {} to {}: {error}",
                    block_id,
                    published_path.display()
                )))
            }
            Err(read_error) => Err(backend_failure(format!(
                "failed to inspect published block {} at {} after publish error {error}: {read_error}",
                block_id,
                published_path.display()
            ))),
        }
    }
}

impl BlockStore for FilesystemBlockStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        let serialized = serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
        let published_path = self.block_path(&serialized.hash);
        let parent = published_path
            .parent()
            .expect("published block paths are always rooted below the store root");
        fs::create_dir_all(parent).map_err(|error| {
            backend_failure(format!(
                "failed to create block directory {}: {error}",
                parent.display()
            ))
        })?;

        let mut staged = Builder::new()
            .prefix(".tmp-")
            .suffix(".part")
            .tempfile_in(parent)
            .map_err(|error| {
                backend_failure(format!(
                    "failed to create staging file in {}: {error}",
                    parent.display()
                ))
            })?;
        staged.write_all(&serialized.bytes).map_err(|error| {
            backend_failure(format!(
                "failed to stage block {} in {}: {error}",
                serialized.hash,
                parent.display()
            ))
        })?;
        staged.flush().map_err(|error| {
            backend_failure(format!(
                "failed to flush staged block {} in {}: {error}",
                serialized.hash,
                parent.display()
            ))
        })?;

        match staged.persist_noclobber(&published_path) {
            Ok(_) => Ok(serialized.hash),
            Err(error) => self.read_existing_or_map_publish_error(
                &published_path,
                &serialized.hash,
                &serialized.bytes,
                error.error,
            ),
        }
    }

    fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        let published_path = self.block_path(block_id);
        let bytes = match fs::read(&published_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(backend_failure(format!(
                    "failed to read block {} at {}: {error}",
                    block_id,
                    published_path.display()
                )));
            }
        };

        deserialize_block(&bytes, block_id)
            .map(Some)
            .map_err(map_get_error)
    }
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
