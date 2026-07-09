// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Read-only `BlockStore` implementation over a single zip archive.
//!
//! This crate intentionally does not satisfy the parent trait specification's
//! successful-`put` requirements. It supports `get` and `iter_block_ids` over a
//! caller-supplied archive whose internal entry layout matches the sharded
//! filesystem block-store layout.

use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::stream;
use lexongraph_block::BlockHash;
use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};
use zip::ZipArchive;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZipBlockStoreInitError {
    Open(String),
    Read(String),
}

impl std::fmt::Display for ZipBlockStoreInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(message) | Self::Read(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ZipBlockStoreInitError {}

#[derive(Clone, Debug)]
pub struct ZipBlockStore {
    archive_path: PathBuf,
    archive_offset: u64,
}

impl ZipBlockStore {
    pub fn new(archive_path: impl AsRef<Path>) -> Result<Self, BlockStoreError> {
        Self::new_classified(archive_path).map_err(|error| backend_failure(error.to_string()))
    }

    pub fn new_classified(archive_path: impl AsRef<Path>) -> Result<Self, ZipBlockStoreInitError> {
        let requested_path = archive_path.as_ref();
        let canonical_path = requested_path.canonicalize().map_err(|error| {
            ZipBlockStoreInitError::Open(format!(
                "failed to canonicalize zip archive {}: {error}",
                requested_path.display()
            ))
        })?;
        let metadata = fs::metadata(&canonical_path).map_err(|error| {
            ZipBlockStoreInitError::Open(format!(
                "failed to stat zip archive {}: {error}",
                canonical_path.display()
            ))
        })?;
        if !metadata.is_file() {
            return Err(ZipBlockStoreInitError::Open(format!(
                "zip archive {} is not a file",
                canonical_path.display()
            )));
        }

        let file = open_archive_file(&canonical_path)
            .map_err(|error| ZipBlockStoreInitError::Open(error.to_string()))?;
        let archive = open_archive_from_file(file, &canonical_path)
            .map_err(|error| ZipBlockStoreInitError::Read(error.to_string()))?;
        let archive_offset = archive.offset();

        let mut file = open_archive_file(&canonical_path)
            .map_err(|error| ZipBlockStoreInitError::Open(error.to_string()))?;
        let _ = archive_entry_names(&mut file, &canonical_path, archive_offset)
            .map_err(|error| ZipBlockStoreInitError::Read(error.to_string()))?;

        Ok(Self {
            archive_path: canonical_path,
            archive_offset,
        })
    }

    fn block_entry_name(block_id: &BlockHash) -> String {
        let hex = block_id.to_string();
        format!("{}/{}/{}.cbor", &hex[..2], &hex[2..4], hex)
    }

    fn enumerate_block_ids(&self) -> Result<Vec<BlockHash>, BlockStoreError> {
        let mut file = open_archive_file(&self.archive_path)?;
        let entry_names = archive_entry_names(&mut file, &self.archive_path, self.archive_offset)?;
        let mut seen_names = HashSet::new();
        let mut block_ids = Vec::new();
        for name in entry_names {
            let Some(block_id) = decode_recognized_block_entry_name(&name) else {
                continue;
            };
            if !seen_names.insert(name.clone()) {
                return Err(backend_failure(format!(
                    "duplicate recognized block entry {name} found in {}",
                    self.archive_path.display()
                )));
            }
            block_ids.push(block_id);
        }
        Ok(block_ids)
    }
}

#[async_trait]
impl BlockStore for ZipBlockStore {
    async fn put_block_bytes(
        &self,
        _block_id: &BlockHash,
        _block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        Err(backend_failure(format!(
            "zip archive {} is read-only; put is not supported",
            self.archive_path.display()
        )))
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let target_name = Self::block_entry_name(block_id);
        let mut file = open_archive_file(&self.archive_path)?;
        let match_count = archive_entry_names(&mut file, &self.archive_path, self.archive_offset)?
            .into_iter()
            .filter(|name| name == &target_name)
            .count();
        if match_count > 1 {
            return Err(backend_failure(format!(
                "duplicate recognized block entry {target_name} found in {}",
                self.archive_path.display()
            )));
        }
        if match_count == 0 {
            return Ok(None);
        }

        file.seek(SeekFrom::Start(0)).map_err(|error| {
            backend_failure(format!(
                "failed to rewind zip archive {} before reading {target_name}: {error}",
                self.archive_path.display()
            ))
        })?;
        let mut archive = open_archive_from_file(file, &self.archive_path)?;
        let mut file = archive.by_name(&target_name).map_err(|error| {
            backend_failure(format!(
                "failed to open zip archive entry {target_name} in {}: {error}",
                self.archive_path.display()
            ))
        })?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|error| {
            backend_failure(format!(
                "failed to read zip archive entry {target_name} in {}: {error}",
                self.archive_path.display()
            ))
        })?;

        Ok(Some(bytes))
    }

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
        Ok(Box::pin(stream::iter(
            self.enumerate_block_ids()?.into_iter().map(Ok),
        )))
    }
}

fn backend_failure(message: String) -> BlockStoreError {
    BlockStoreError::BackendFailure(message)
}

const CENTRAL_DIRECTORY_HEADER_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x01, 0x02];
const CENTRAL_DIRECTORY_HEADER_LEN: usize = 46;
const END_OF_CENTRAL_DIRECTORY_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x05, 0x06];
const ZIP64_END_OF_CENTRAL_DIRECTORY_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x06, 0x06];
const ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x06, 0x07];
const ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_LEN: usize = 20;

#[derive(Debug)]
struct CentralDirectory {
    entry_count: usize,
    offset: u64,
    size: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EndOfCentralDirectory {
    entry_count: u16,
    directory_offset: u32,
    directory_size: u32,
    zip64_eocd_offset: Option<u64>,
}

fn open_archive_file(archive_path: &Path) -> Result<File, BlockStoreError> {
    File::open(archive_path).map_err(|error| {
        backend_failure(format!(
            "failed to open zip archive {}: {error}",
            archive_path.display()
        ))
    })
}

fn open_archive_from_file(
    file: File,
    archive_path: &Path,
) -> Result<ZipArchive<File>, BlockStoreError> {
    ZipArchive::new(file).map_err(|error| {
        backend_failure(format!(
            "failed to read zip archive {}: {error}",
            archive_path.display()
        ))
    })
}

fn archive_entry_names(
    file: &mut File,
    archive_path: &Path,
    archive_offset: u64,
) -> Result<Vec<String>, BlockStoreError> {
    let file_len = file
        .metadata()
        .map_err(|error| {
            backend_failure(format!(
                "failed to stat zip archive {} during central-directory scan: {error}",
                archive_path.display()
            ))
        })?
        .len();
    let eocd_tail_len = file_len
        .min((EOCD_LEN + usize::from(u16::MAX) + ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_LEN) as u64)
        as usize;
    file.seek(SeekFrom::End(-(eocd_tail_len as i64)))
        .map_err(|error| {
            backend_failure(format!(
                "failed to seek zip archive {} to its EOCD search window: {error}",
                archive_path.display()
            ))
        })?;
    let mut eocd_tail = vec![0_u8; eocd_tail_len];
    file.read_exact(&mut eocd_tail).map_err(|error| {
        backend_failure(format!(
            "failed to read the EOCD search window from zip archive {}: {error}",
            archive_path.display()
        ))
    })?;

    let eocd = find_end_of_central_directory(&eocd_tail).ok_or_else(|| {
        backend_failure(format!(
            "failed to locate the zip central directory in {}",
            archive_path.display()
        ))
    })?;
    let directory = resolve_central_directory(file, archive_path, file_len, archive_offset, eocd)?;
    let directory_end = directory
        .offset
        .checked_add(directory.size)
        .filter(|end| *end <= file_len)
        .ok_or_else(|| {
            backend_failure(format!(
                "failed to bound the zip central directory in {}",
                archive_path.display()
            ))
        })?;

    file.seek(SeekFrom::Start(directory.offset))
        .map_err(|error| {
            backend_failure(format!(
                "failed to seek zip archive {} to its central directory: {error}",
                archive_path.display()
            ))
        })?;

    let mut names = Vec::new();
    let mut cursor = directory.offset;
    for entry_index in 0..directory.entry_count {
        let header_end = cursor
            .checked_add(CENTRAL_DIRECTORY_HEADER_LEN as u64)
            .filter(|end| *end <= directory_end)
            .ok_or_else(|| {
                backend_failure(format!(
                    "failed to bound central directory entry {entry_index} header in {}",
                    archive_path.display()
                ))
            })?;
        let mut header = [0_u8; CENTRAL_DIRECTORY_HEADER_LEN];
        file.read_exact(&mut header).map_err(|error| {
            backend_failure(format!(
                "failed to read central directory entry {entry_index} header in {}: {error}",
                archive_path.display()
            ))
        })?;
        if !header.starts_with(&CENTRAL_DIRECTORY_HEADER_SIGNATURE) {
            return Err(backend_failure(format!(
                "failed to parse central directory entry {entry_index} in {}",
                archive_path.display()
            )));
        }

        let name_len = read_u16_le(&header, 28).map(usize::from).ok_or_else(|| {
            backend_failure(format!(
                "failed to read central directory entry {entry_index} name length in {}",
                archive_path.display()
            ))
        })?;
        let extra_len = read_u16_le(&header, 30).map(usize::from).ok_or_else(|| {
            backend_failure(format!(
                "failed to read central directory entry {entry_index} extra length in {}",
                archive_path.display()
            ))
        })?;
        let comment_len = read_u16_le(&header, 32).map(usize::from).ok_or_else(|| {
            backend_failure(format!(
                "failed to read central directory entry {entry_index} comment length in {}",
                archive_path.display()
            ))
        })?;
        let variable_len = name_len
            .checked_add(extra_len)
            .and_then(|len| len.checked_add(comment_len))
            .ok_or_else(|| {
                backend_failure(format!(
                    "failed to bound central directory entry {entry_index} variable data in {}",
                    archive_path.display()
                ))
            })?;
        let entry_end = header_end
            .checked_add(variable_len as u64)
            .filter(|end| *end <= directory_end)
            .ok_or_else(|| {
                backend_failure(format!(
                    "failed to bound central directory entry {entry_index} in {}",
                    archive_path.display()
                ))
            })?;

        let mut name_bytes = vec![0_u8; name_len];
        file.read_exact(&mut name_bytes).map_err(|error| {
            backend_failure(format!(
                "failed to read central directory entry {entry_index} name in {}: {error}",
                archive_path.display()
            ))
        })?;
        if let Ok(name) = std::str::from_utf8(&name_bytes) {
            names.push(name.to_string());
        }

        let skip_len = i64::try_from(extra_len + comment_len).map_err(|_| {
            backend_failure(format!(
                "failed to advance past central directory entry {entry_index} in {}",
                archive_path.display()
            ))
        })?;
        file.seek(SeekFrom::Current(skip_len)).map_err(|error| {
            backend_failure(format!(
                "failed to advance past central directory entry {entry_index} in {}: {error}",
                archive_path.display()
            ))
        })?;
        cursor = entry_end;
    }

    Ok(names)
}

fn resolve_central_directory(
    file: &mut File,
    archive_path: &Path,
    file_len: u64,
    archive_offset: u64,
    eocd: EndOfCentralDirectory,
) -> Result<CentralDirectory, BlockStoreError> {
    let classic_directory = classic_central_directory(archive_path, archive_offset, eocd)?;
    let requires_zip64 = eocd.entry_count == u16::MAX
        || eocd.directory_size == u32::MAX
        || eocd.directory_offset == u32::MAX;
    if !requires_zip64 {
        if let Some(zip64_eocd_offset) = eocd.zip64_eocd_offset
            && let Some(directory) = try_read_zip64_central_directory(
                file,
                archive_path,
                file_len,
                archive_offset,
                zip64_eocd_offset,
            )?
        {
            return Ok(directory);
        }
        return Ok(classic_directory);
    }

    let zip64_eocd_offset = eocd.zip64_eocd_offset.ok_or_else(|| {
        backend_failure(format!(
            "failed to locate the zip64 end of central directory in {}",
            archive_path.display()
        ))
    })?;
    try_read_zip64_central_directory(
        file,
        archive_path,
        file_len,
        archive_offset,
        zip64_eocd_offset,
    )
    .and_then(|directory| {
        directory.ok_or_else(|| {
            backend_failure(format!(
                "failed to parse the zip64 end of central directory in {}",
                archive_path.display()
            ))
        })
    })
}

fn classic_central_directory(
    archive_path: &Path,
    archive_offset: u64,
    eocd: EndOfCentralDirectory,
) -> Result<CentralDirectory, BlockStoreError> {
    Ok(CentralDirectory {
        entry_count: usize::from(eocd.entry_count),
        offset: archive_offset
            .checked_add(u64::from(eocd.directory_offset))
            .ok_or_else(|| {
                backend_failure(format!(
                    "failed to bound the zip central directory in {}",
                    archive_path.display()
                ))
            })?,
        size: u64::from(eocd.directory_size),
    })
}

fn try_read_zip64_central_directory(
    file: &mut File,
    archive_path: &Path,
    file_len: u64,
    archive_offset: u64,
    zip64_eocd_offset: u64,
) -> Result<Option<CentralDirectory>, BlockStoreError> {
    let zip64_eocd_offset = archive_offset
        .checked_add(zip64_eocd_offset)
        .ok_or_else(|| {
            backend_failure(format!(
                "failed to bound the zip64 end of central directory in {}",
                archive_path.display()
            ))
        })?;
    read_zip64_central_directory(
        file,
        archive_path,
        file_len,
        archive_offset,
        zip64_eocd_offset,
    )
}

fn read_zip64_central_directory(
    file: &mut File,
    archive_path: &Path,
    file_len: u64,
    archive_offset: u64,
    zip64_eocd_offset: u64,
) -> Result<Option<CentralDirectory>, BlockStoreError> {
    const ZIP64_EOCD_MIN_LEN: usize = 56;

    let Some(zip64_eocd_end) = zip64_eocd_offset
        .checked_add(ZIP64_EOCD_MIN_LEN as u64)
        .filter(|end| *end <= file_len)
    else {
        return Ok(None);
    };

    file.seek(SeekFrom::Start(zip64_eocd_offset))
        .map_err(|error| {
            backend_failure(format!(
                "failed to seek zip archive {} to its zip64 end of central directory: {error}",
                archive_path.display()
            ))
        })?;
    let mut record = [0_u8; ZIP64_EOCD_MIN_LEN];
    file.read_exact(&mut record).map_err(|error| {
        backend_failure(format!(
            "failed to read the zip64 end of central directory from {}: {error}",
            archive_path.display()
        ))
    })?;

    if !record.starts_with(&ZIP64_END_OF_CENTRAL_DIRECTORY_SIGNATURE) {
        return Ok(None);
    }
    let record_size = read_u64_le(&record, 4).ok_or_else(|| {
        backend_failure(format!(
            "failed to read the zip64 end of central directory size in {}",
            archive_path.display()
        ))
    })?;
    if record_size < 44 {
        return Ok(None);
    }
    let Some(total_record_len) = zip64_eocd_offset
        .checked_add(12)
        .and_then(|start| start.checked_add(record_size))
        .filter(|end| *end <= file_len)
    else {
        return Ok(None);
    };
    debug_assert_eq!(
        zip64_eocd_end,
        zip64_eocd_offset + ZIP64_EOCD_MIN_LEN as u64
    );
    if total_record_len < zip64_eocd_end {
        return Ok(None);
    }

    let entry_count = read_u64_le(&record, 32).ok_or_else(|| {
        backend_failure(format!(
            "failed to read the zip64 central directory entry count in {}",
            archive_path.display()
        ))
    })?;
    let size = read_u64_le(&record, 40).ok_or_else(|| {
        backend_failure(format!(
            "failed to read the zip64 central directory size in {}",
            archive_path.display()
        ))
    })?;
    let offset = read_u64_le(&record, 48).ok_or_else(|| {
        backend_failure(format!(
            "failed to read the zip64 central directory offset in {}",
            archive_path.display()
        ))
    })?;

    Ok(Some(CentralDirectory {
        entry_count: usize::try_from(entry_count).map_err(|_| {
            backend_failure(format!(
                "zip archive {} contains too many entries to inspect on this platform",
                archive_path.display()
            ))
        })?,
        offset: archive_offset.checked_add(offset).ok_or_else(|| {
            backend_failure(format!(
                "failed to bound the zip64 central directory in {}",
                archive_path.display()
            ))
        })?,
        size,
    }))
}

fn decode_recognized_block_entry_name(name: &str) -> Option<BlockHash> {
    let mut components = name.split('/');
    let first_level = components.next()?;
    let second_level = components.next()?;
    let file_name = components.next()?;
    if components.next().is_some() {
        return None;
    }
    if !is_lower_hex_prefix(first_level) || !is_lower_hex_prefix(second_level) {
        return None;
    }
    let hex = file_name.strip_suffix(".cbor")?;
    if hex.len() != BlockHash::LEN * 2 || !hex.bytes().all(|byte| decode_hex_nibble(byte).is_some())
    {
        return None;
    }
    if &hex[..2] != first_level || &hex[2..4] != second_level {
        return None;
    }

    let mut bytes = [0_u8; BlockHash::LEN];
    for (index, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(chunk[0])?;
        let low = decode_hex_nibble(chunk[1])?;
        bytes[index] = (high << 4) | low;
    }
    Some(BlockHash::from_bytes(bytes))
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

const EOCD_LEN: usize = 22;

fn find_end_of_central_directory(tail_bytes: &[u8]) -> Option<EndOfCentralDirectory> {
    if tail_bytes.len() < EOCD_LEN {
        return None;
    }

    for eocd_offset in (0..=tail_bytes.len() - EOCD_LEN).rev() {
        if !tail_bytes[eocd_offset..].starts_with(END_OF_CENTRAL_DIRECTORY_SIGNATURE.as_slice()) {
            continue;
        }

        let comment_len = {
            let value = read_u16_le(tail_bytes, eocd_offset + 20)?;
            usize::from(value)
        };
        let Some(record_end) = eocd_offset
            .checked_add(EOCD_LEN)
            .and_then(|offset| offset.checked_add(comment_len))
        else {
            continue;
        };
        if record_end != tail_bytes.len() {
            continue;
        }

        let zip64_eocd_offset = if eocd_offset >= 20
            && tail_bytes[eocd_offset - 20..]
                .starts_with(ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIGNATURE.as_slice())
        {
            read_u64_le(tail_bytes, eocd_offset - 20 + 8)
        } else {
            None
        };

        return Some(EndOfCentralDirectory {
            entry_count: read_u16_le(tail_bytes, eocd_offset + 10)?,
            directory_size: read_u32_le(tail_bytes, eocd_offset + 12)?,
            directory_offset: read_u32_le(tail_bytes, eocd_offset + 16)?,
            zip64_eocd_offset,
        });
    }

    None
}

fn read_u16_le(bytes: &[u8], offset: usize) -> Option<u16> {
    let chunk: [u8; 2] = bytes.get(offset..offset + 2)?.try_into().ok()?;
    Some(u16::from_le_bytes(chunk))
}

fn read_u32_le(bytes: &[u8], offset: usize) -> Option<u32> {
    let chunk: [u8; 4] = bytes.get(offset..offset + 4)?.try_into().ok()?;
    Some(u32::from_le_bytes(chunk))
}

fn read_u64_le(bytes: &[u8], offset: usize) -> Option<u64> {
    let chunk: [u8; 8] = bytes.get(offset..offset + 8)?.try_into().ok()?;
    Some(u64::from_le_bytes(chunk))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use super::{
        END_OF_CENTRAL_DIRECTORY_SIGNATURE, EOCD_LEN, EndOfCentralDirectory,
        ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIGNATURE, find_end_of_central_directory,
        resolve_central_directory,
    };

    #[test]
    fn find_end_of_central_directory_ignores_signature_bytes_inside_the_comment() {
        let comment = b"commentPK\x05\x06tail";
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"prefix");
        bytes.extend_from_slice(&END_OF_CENTRAL_DIRECTORY_SIGNATURE);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u32(&mut bytes, 0);
        push_u32(&mut bytes, 0);
        push_u16(&mut bytes, comment.len() as u16);
        bytes.extend_from_slice(comment);

        let directory = find_end_of_central_directory(&bytes).unwrap();

        assert_eq!(directory.entry_count, 0);
        assert_eq!(directory.directory_offset, 0);
        assert_eq!(directory.directory_size, 0);
        assert_eq!(directory.zip64_eocd_offset, None);
    }

    #[test]
    fn find_end_of_central_directory_preserves_zip64_sentinel_fields() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&END_OF_CENTRAL_DIRECTORY_SIGNATURE);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, u16::MAX);
        push_u16(&mut bytes, u16::MAX);
        push_u32(&mut bytes, u32::MAX);
        push_u32(&mut bytes, u32::MAX);
        push_u16(&mut bytes, 0);

        let directory = find_end_of_central_directory(&bytes).unwrap();

        assert_eq!(directory.entry_count, u16::MAX);
        assert_eq!(directory.directory_size, u32::MAX);
        assert_eq!(directory.directory_offset, u32::MAX);
        assert_eq!(directory.zip64_eocd_offset, None);
    }

    #[test]
    fn find_end_of_central_directory_reads_zip64_locator_records() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIGNATURE);
        push_u32(&mut bytes, 0);
        push_u64(&mut bytes, 0x1122_3344_5566_7788);
        push_u32(&mut bytes, 1);
        bytes.extend_from_slice(&END_OF_CENTRAL_DIRECTORY_SIGNATURE);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 1);
        push_u16(&mut bytes, 1);
        push_u32(&mut bytes, 10);
        push_u32(&mut bytes, 20);
        push_u16(&mut bytes, 0);

        let directory = find_end_of_central_directory(&bytes).unwrap();

        assert_eq!(directory.zip64_eocd_offset, Some(0x1122_3344_5566_7788));
    }

    #[test]
    fn find_end_of_central_directory_uses_the_end_of_the_file_for_comment_bounding() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"prefix");
        bytes.extend_from_slice(&END_OF_CENTRAL_DIRECTORY_SIGNATURE);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u32(&mut bytes, 0);
        push_u32(&mut bytes, 0);
        push_u16(&mut bytes, 0);

        let directory = find_end_of_central_directory(&bytes[6..]).unwrap();

        assert_eq!(directory.entry_count, 0);
        assert_eq!(directory.directory_offset, 0);
        assert_eq!(directory.directory_size, 0);
        assert_eq!(EOCD_LEN, 22);
    }

    #[test]
    fn resolve_central_directory_falls_back_to_classic_when_locator_signature_is_garbage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("classic-with-garbage-locator.zip");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIGNATURE);
        push_u32(&mut bytes, 0);
        push_u64(&mut bytes, 3);
        push_u32(&mut bytes, 1);
        bytes.extend_from_slice(&END_OF_CENTRAL_DIRECTORY_SIGNATURE);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u32(&mut bytes, 0);
        push_u32(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        File::create(&archive_path)
            .unwrap()
            .write_all(&bytes)
            .unwrap();

        let eocd = EndOfCentralDirectory {
            entry_count: 0,
            directory_offset: 0,
            directory_size: 0,
            zip64_eocd_offset: Some(3),
        };
        let mut file = File::open(&archive_path).unwrap();

        let directory =
            resolve_central_directory(&mut file, &archive_path, bytes.len() as u64, 0, eocd)
                .unwrap();

        assert_eq!(directory.entry_count, 0);
        assert_eq!(directory.offset, 0);
        assert_eq!(directory.size, 0);
    }

    fn push_u16(buffer: &mut Vec<u8>, value: u16) {
        buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn push_u32(buffer: &mut Vec<u8>, value: u32) {
        buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn push_u64(buffer: &mut Vec<u8>, value: u64) {
        buffer.extend_from_slice(&value.to_le_bytes());
    }
}
