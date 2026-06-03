// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Local-filesystem `BlockStore` implementation for LexonGraph blocks.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
#[cfg(feature = "inject")]
use std::sync::Arc;

use lexongraph_block::{
    Block, BlockError, BlockHash, ValidatedBlock, deserialize_block, serialize_block,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use tempfile::{Builder, NamedTempFile};

#[cfg(feature = "inject")]
pub mod inject {
    use std::io;
    use std::path::{Path, PathBuf};

    pub trait FsOps: Send + Sync {
        fn create_dir_all(&self, path: &Path) -> io::Result<()>;
        fn canonicalize(&self, path: &Path) -> io::Result<PathBuf>;
        fn is_dir(&self, path: &Path) -> io::Result<bool>;
        fn read(&self, path: &Path) -> io::Result<Vec<u8>>;
        fn create_staged_file(&self, dir: &Path) -> io::Result<Box<dyn StagedFile>>;
    }

    pub trait StagedFile: Send {
        fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>;
        fn flush(&mut self) -> io::Result<()>;
        fn persist_noclobber(self: Box<Self>, target: &Path) -> io::Result<()>;
    }
}

#[cfg(feature = "inject")]
struct RealFsOps;

#[cfg(feature = "inject")]
struct RealStagedFile {
    file: NamedTempFile,
}

#[cfg(feature = "inject")]
impl inject::FsOps for RealFsOps {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        path.canonicalize()
    }

    fn is_dir(&self, path: &Path) -> io::Result<bool> {
        fs::metadata(path).map(|metadata| metadata.is_dir())
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        fs::read(path)
    }

    fn create_staged_file(&self, dir: &Path) -> io::Result<Box<dyn inject::StagedFile>> {
        Ok(Box::new(RealStagedFile {
            file: staged_file_in(dir)?,
        }))
    }
}

#[cfg(feature = "inject")]
impl inject::StagedFile for RealStagedFile {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    fn persist_noclobber(self: Box<Self>, target: &Path) -> io::Result<()> {
        self.file
            .persist_noclobber(target)
            .map(|_| ())
            .map_err(|error| error.error)
    }
}

#[derive(Clone)]
pub struct FilesystemBlockStore {
    store_root: PathBuf,
    #[cfg(feature = "inject")]
    ops: Arc<dyn inject::FsOps>,
}

impl std::fmt::Debug for FilesystemBlockStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilesystemBlockStore")
            .field("store_root", &self.store_root)
            .finish()
    }
}

impl FilesystemBlockStore {
    #[cfg(not(feature = "inject"))]
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

    #[cfg(feature = "inject")]
    pub fn new(store_root: impl AsRef<Path>) -> Result<Self, BlockStoreError> {
        Self::new_with_ops(store_root, Arc::new(RealFsOps))
    }

    #[cfg(feature = "inject")]
    pub fn new_with_ops(
        store_root: impl AsRef<Path>,
        ops: Arc<dyn inject::FsOps>,
    ) -> Result<Self, BlockStoreError> {
        let requested_root = store_root.as_ref();
        ops.create_dir_all(requested_root).map_err(|error| {
            backend_failure(format!(
                "failed to create store root {}: {error}",
                requested_root.display()
            ))
        })?;

        let canonical_root = ops.canonicalize(requested_root).map_err(|error| {
            backend_failure(format!(
                "failed to canonicalize store root {}: {error}",
                requested_root.display()
            ))
        })?;

        let is_dir = ops.is_dir(&canonical_root).map_err(|error| {
            backend_failure(format!(
                "failed to stat store root {}: {error}",
                canonical_root.display()
            ))
        })?;
        if !is_dir {
            return Err(backend_failure(format!(
                "store root {} is not a directory",
                canonical_root.display()
            )));
        }

        Ok(Self {
            store_root: canonical_root,
            ops,
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
        match self.read_bytes(published_path) {
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

    #[cfg(feature = "inject")]
    fn read_bytes(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.ops.read(path)
    }

    #[cfg(not(feature = "inject"))]
    fn read_bytes(&self, path: &Path) -> io::Result<Vec<u8>> {
        fs::read(path)
    }

    #[cfg(feature = "inject")]
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.ops.create_dir_all(path)
    }

    #[cfg(not(feature = "inject"))]
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    #[cfg(feature = "inject")]
    fn create_staged_file(&self, dir: &Path) -> io::Result<Box<dyn inject::StagedFile>> {
        self.ops.create_staged_file(dir)
    }

    #[cfg(not(feature = "inject"))]
    fn create_staged_file(&self, dir: &Path) -> io::Result<NamedTempFile> {
        staged_file_in(dir)
    }
}

impl BlockStore for FilesystemBlockStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        let serialized = serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
        let published_path = self.block_path(&serialized.hash);
        let parent = published_path
            .parent()
            .expect("published block paths are always rooted below the store root");
        self.create_dir_all(parent).map_err(|error| {
            backend_failure(format!(
                "failed to create block directory {}: {error}",
                parent.display()
            ))
        })?;

        let mut staged = self.create_staged_file(parent).map_err(|error| {
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

        match persist_staged_file(staged, &published_path) {
            Ok(_) => Ok(serialized.hash),
            Err(error) => self.read_existing_or_map_publish_error(
                &published_path,
                &serialized.hash,
                &serialized.bytes,
                error,
            ),
        }
    }

    fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        let published_path = self.block_path(block_id);
        let bytes = match self.read_bytes(&published_path) {
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

fn staged_file_in(dir: &Path) -> io::Result<NamedTempFile> {
    Builder::new()
        .prefix(".tmp-")
        .suffix(".part")
        .tempfile_in(dir)
}

#[cfg(feature = "inject")]
fn persist_staged_file(
    staged: Box<dyn inject::StagedFile>,
    published_path: &Path,
) -> io::Result<()> {
    staged.persist_noclobber(published_path)
}

#[cfg(not(feature = "inject"))]
fn persist_staged_file(staged: NamedTempFile, published_path: &Path) -> io::Result<()> {
    staged
        .persist_noclobber(published_path)
        .map(|_| ())
        .map_err(|error| error.error)
}

fn map_get_error(error: BlockError) -> BlockStoreError {
    match error {
        BlockError::HashMismatch { expected, actual } => {
            BlockStoreError::IntegrityMismatch { expected, actual }
        }
        other => BlockStoreError::MalformedContent(other),
    }
}
