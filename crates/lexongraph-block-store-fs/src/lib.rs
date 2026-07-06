// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Local-filesystem `BlockStore` implementation for LexonGraph blocks.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
#[cfg(feature = "inject")]
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use lexongraph_block::BlockHash;
use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};
use tempfile::{Builder, NamedTempFile};

#[cfg(feature = "inject")]
pub mod inject {
    use std::io;
    use std::path::{Path, PathBuf};

    pub trait FsOps: Send + Sync {
        fn create_dir_all(&self, path: &Path) -> io::Result<()>;
        fn canonicalize(&self, path: &Path) -> io::Result<PathBuf>;
        fn is_dir(&self, path: &Path) -> io::Result<bool>;
        fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>>;
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
        fs::symlink_metadata(path).map(|metadata| metadata.is_dir())
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        fs::read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        fs::read_dir(path)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect()
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
    pub fn block_path_for_root(store_root: &Path, block_id: &BlockHash) -> PathBuf {
        let hex = block_id.to_string();
        let (first_level, rest) = hex.split_at(2);
        let (second_level, _) = rest.split_at(2);
        store_root
            .join(first_level)
            .join(second_level)
            .join(format!("{hex}.cbor"))
    }

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
        Self::block_path_for_root(&self.store_root, block_id)
    }

    fn read_existing_or_map_publish_error(
        &self,
        published_path: &Path,
        block_id: &BlockHash,
        canonical_bytes: &[u8],
        error: std::io::Error,
    ) -> Result<(), BlockStoreError> {
        match self.read_bytes(published_path) {
            Ok(existing_bytes) if existing_bytes == canonical_bytes => Ok(()),
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

    fn decode_enumerated_block_path(
        &self,
        path: &Path,
    ) -> Result<Option<BlockHash>, BlockStoreError> {
        let relative = path.strip_prefix(&self.store_root).map_err(|error| {
            backend_failure(format!(
                "failed to normalize an enumerated block-store entry relative to the store root: {error}"
            ))
        })?;
        let mut components = relative.components();
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

        let first_level = first_level.as_os_str().to_str().ok_or_else(|| {
            backend_failure("failed to decode an enumerated shard directory name".into())
        })?;
        let second_level = second_level.as_os_str().to_str().ok_or_else(|| {
            backend_failure("failed to decode an enumerated shard directory name".into())
        })?;
        if !is_lower_hex_prefix(first_level) || !is_lower_hex_prefix(second_level) {
            return Ok(None);
        }

        let file_name = file_name.as_os_str().to_str().ok_or_else(|| {
            backend_failure("failed to decode an enumerated block file name".into())
        })?;
        let Some(hex) = file_name.strip_suffix(".cbor") else {
            return Ok(None);
        };
        let bytes = decode_block_hash_hex(hex).ok_or_else(|| {
            backend_failure("failed to decode an enumerated block ID candidate".into())
        })?;
        if &hex[..2] != first_level || &hex[2..4] != second_level {
            return Err(backend_failure(
                "failed to decode an enumerated block ID candidate: shard prefix mismatch".into(),
            ));
        }

        Ok(Some(BlockHash::from_bytes(bytes)))
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
    fn is_dir(&self, path: &Path) -> io::Result<bool> {
        self.ops.is_dir(path)
    }

    #[cfg(not(feature = "inject"))]
    fn is_dir(&self, path: &Path) -> io::Result<bool> {
        fs::symlink_metadata(path).map(|metadata| metadata.is_dir())
    }

    #[cfg(feature = "inject")]
    fn read_dir_paths(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        self.ops.read_dir(path)
    }

    #[cfg(not(feature = "inject"))]
    fn read_dir_paths(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        fs::read_dir(path)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect()
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

#[async_trait]
impl BlockStore for FilesystemBlockStore {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        let published_path = self.block_path(block_id);
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
        staged.write_all(block_bytes).map_err(|error| {
            backend_failure(format!(
                "failed to stage block {} in {}: {error}",
                block_id,
                parent.display()
            ))
        })?;
        staged.flush().map_err(|error| {
            backend_failure(format!(
                "failed to flush staged block {} in {}: {error}",
                block_id,
                parent.display()
            ))
        })?;

        match persist_staged_file(staged, &published_path) {
            Ok(_) => Ok(()),
            Err(error) => self.read_existing_or_map_publish_error(
                &published_path,
                block_id,
                block_bytes,
                error,
            ),
        }
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
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

        Ok(Some(bytes))
    }

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
        Ok(Box::pin(stream::iter(FilesystemBlockIdIterator::new(
            self,
        )?)))
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

struct FilesystemBlockIdIterator<'a> {
    store: &'a FilesystemBlockStore,
    pending: Vec<(PathBuf, usize)>,
}

impl<'a> FilesystemBlockIdIterator<'a> {
    fn new(store: &'a FilesystemBlockStore) -> Result<Self, BlockStoreError> {
        let root_entries = store.read_dir_paths(&store.store_root).map_err(|error| {
            backend_failure(format!("failed to enumerate the block store root: {error}"))
        })?;
        Ok(Self {
            store,
            pending: root_entries
                .into_iter()
                .rev()
                .map(|path| (path, 0))
                .collect(),
        })
    }
}

impl Iterator for FilesystemBlockIdIterator<'_> {
    type Item = Result<BlockHash, BlockStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((path, depth)) = self.pending.pop() {
            let is_dir = match self.store.is_dir(&path) {
                Ok(is_dir) => is_dir,
                Err(error) => {
                    return Some(Err(backend_failure(format!(
                        "failed to stat an enumerated block-store entry: {error}"
                    ))));
                }
            };

            if is_dir {
                if depth >= 2 {
                    continue;
                }
                match self.store.read_dir_paths(&path) {
                    Ok(children) => {
                        for child in children.into_iter().rev() {
                            self.pending.push((child, depth + 1));
                        }
                    }
                    Err(error) => {
                        return Some(Err(backend_failure(format!(
                            "failed to enumerate an internal block-store directory: {error}"
                        ))));
                    }
                }
                continue;
            }

            if depth != 2 {
                continue;
            }

            match self.store.decode_enumerated_block_path(&path) {
                Ok(Some(block_id)) => return Some(Ok(block_id)),
                Ok(None) => continue,
                Err(error) => return Some(Err(error)),
            }
        }

        None
    }
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
