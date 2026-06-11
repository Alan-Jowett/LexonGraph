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

use lexongraph_block::{Block, BlockError, BlockHash, ValidatedBlock, deserialize_block};
use lexongraph_block_store::{BlockIdIterator, BlockStore, BlockStoreError};
use zip::ZipArchive;

#[derive(Clone, Debug)]
pub struct ZipBlockStore {
    archive_path: PathBuf,
}

impl ZipBlockStore {
    pub fn new(archive_path: impl AsRef<Path>) -> Result<Self, BlockStoreError> {
        let requested_path = archive_path.as_ref();
        let canonical_path = requested_path.canonicalize().map_err(|error| {
            backend_failure(format!(
                "failed to canonicalize zip archive {}: {error}",
                requested_path.display()
            ))
        })?;
        let metadata = fs::metadata(&canonical_path).map_err(|error| {
            backend_failure(format!(
                "failed to stat zip archive {}: {error}",
                canonical_path.display()
            ))
        })?;
        if !metadata.is_file() {
            return Err(backend_failure(format!(
                "zip archive {} is not a file",
                canonical_path.display()
            )));
        }

        let file = open_archive_file(&canonical_path)?;
        let _ = open_archive_from_file(file, &canonical_path)?;

        let mut file = open_archive_file(&canonical_path)?;
        let _ = archive_entry_names(&mut file, &canonical_path)?;

        Ok(Self {
            archive_path: canonical_path,
        })
    }

    fn block_entry_name(block_id: &BlockHash) -> String {
        let hex = block_id.to_string();
        format!("{}/{}/{}.cbor", &hex[..2], &hex[2..4], hex)
    }

    fn enumerate_block_ids(&self) -> Result<Vec<BlockHash>, BlockStoreError> {
        let mut file = open_archive_file(&self.archive_path)?;
        let entry_names = archive_entry_names(&mut file, &self.archive_path)?;
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

impl BlockStore for ZipBlockStore {
    fn put(&self, _block: &Block) -> Result<BlockHash, BlockStoreError> {
        Err(backend_failure(format!(
            "zip archive {} is read-only; put is not supported",
            self.archive_path.display()
        )))
    }

    fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        let target_name = Self::block_entry_name(block_id);
        let mut file = open_archive_file(&self.archive_path)?;
        let match_count = archive_entry_names(&mut file, &self.archive_path)?
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

        deserialize_block(&bytes, block_id)
            .map(Some)
            .map_err(map_get_error)
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        Ok(Box::new(self.enumerate_block_ids()?.into_iter().map(Ok)))
    }
}

fn backend_failure(message: String) -> BlockStoreError {
    BlockStoreError::BackendFailure(message)
}

const CENTRAL_DIRECTORY_HEADER_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x01, 0x02];
const END_OF_CENTRAL_DIRECTORY_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x05, 0x06];
const ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x06, 0x07];

#[derive(Debug)]
struct CentralDirectory {
    entry_count: usize,
    offset: usize,
    size: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CentralDirectoryError {
    Zip64Unsupported,
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
    let eocd_tail_len = file_len.min((EOCD_LEN + usize::from(u16::MAX)) as u64) as usize;
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

    let directory = match find_end_of_central_directory(&eocd_tail) {
        Ok(Some(directory)) => directory,
        Ok(None) => {
            return Err(backend_failure(format!(
                "failed to locate the zip central directory in {}",
                archive_path.display()
            )));
        }
        Err(CentralDirectoryError::Zip64Unsupported) => {
            return Err(backend_failure(format!(
                "zip64 archives are not supported by ZipBlockStore in {}",
                archive_path.display()
            )));
        }
    };
    let directory_end = directory
        .offset
        .checked_add(directory.size)
        .filter(|end| (*end as u64) <= file_len)
        .ok_or_else(|| {
            backend_failure(format!(
                "failed to bound the zip central directory in {}",
                archive_path.display()
            ))
        })?;

    file.seek(SeekFrom::Start(directory.offset as u64))
        .map_err(|error| {
            backend_failure(format!(
                "failed to seek zip archive {} to its central directory: {error}",
                archive_path.display()
            ))
        })?;
    let mut directory_bytes = vec![0_u8; directory_end - directory.offset];
    file.read_exact(&mut directory_bytes).map_err(|error| {
        backend_failure(format!(
            "failed to read the central directory from zip archive {}: {error}",
            archive_path.display()
        ))
    })?;

    let mut names = Vec::with_capacity(directory.entry_count);
    let mut cursor = 0;
    for entry_index in 0..directory.entry_count {
        if directory_bytes.get(cursor..cursor + CENTRAL_DIRECTORY_HEADER_SIGNATURE.len())
            != Some(CENTRAL_DIRECTORY_HEADER_SIGNATURE.as_slice())
        {
            return Err(backend_failure(format!(
                "failed to parse central directory entry {entry_index} in {}",
                archive_path.display()
            )));
        }

        let name_len = read_u16_le(&directory_bytes, cursor + 28)
            .map(usize::from)
            .ok_or_else(|| {
                backend_failure(format!(
                    "failed to read central directory entry {entry_index} name length in {}",
                    archive_path.display()
                ))
            })?;
        let extra_len = read_u16_le(&directory_bytes, cursor + 30)
            .map(usize::from)
            .ok_or_else(|| {
                backend_failure(format!(
                    "failed to read central directory entry {entry_index} extra length in {}",
                    archive_path.display()
                ))
            })?;
        let comment_len = read_u16_le(&directory_bytes, cursor + 32)
            .map(usize::from)
            .ok_or_else(|| {
                backend_failure(format!(
                    "failed to read central directory entry {entry_index} comment length in {}",
                    archive_path.display()
                ))
            })?;
        let name_start = cursor + 46;
        let name_end = name_start
            .checked_add(name_len)
            .filter(|end| *end <= directory_bytes.len())
            .ok_or_else(|| {
                backend_failure(format!(
                    "failed to bound central directory entry {entry_index} name in {}",
                    archive_path.display()
                ))
            })?;
        if let Ok(name) = std::str::from_utf8(&directory_bytes[name_start..name_end]) {
            names.push(name.to_string());
        }

        cursor = name_end
            .checked_add(extra_len)
            .and_then(|next| next.checked_add(comment_len))
            .filter(|next| *next <= directory_bytes.len())
            .ok_or_else(|| {
                backend_failure(format!(
                    "failed to advance past central directory entry {entry_index} in {}",
                    archive_path.display()
                ))
            })?;
    }

    Ok(names)
}

fn map_get_error(error: BlockError) -> BlockStoreError {
    match error {
        BlockError::HashMismatch { expected, actual } => {
            BlockStoreError::IntegrityMismatch { expected, actual }
        }
        other => BlockStoreError::MalformedContent(other),
    }
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

fn find_end_of_central_directory(
    tail_bytes: &[u8],
) -> Result<Option<CentralDirectory>, CentralDirectoryError> {
    if tail_bytes.len() < EOCD_LEN {
        return Ok(None);
    }

    for eocd_offset in (0..=tail_bytes.len() - EOCD_LEN).rev() {
        if !tail_bytes[eocd_offset..].starts_with(END_OF_CENTRAL_DIRECTORY_SIGNATURE.as_slice()) {
            continue;
        }
        if eocd_offset >= 20
            && tail_bytes[eocd_offset - 20..]
                .starts_with(ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIGNATURE.as_slice())
        {
            return Err(CentralDirectoryError::Zip64Unsupported);
        }

        let comment_len = match read_u16_le(tail_bytes, eocd_offset + 20) {
            Some(value) => usize::from(value),
            None => continue,
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

        let entries_on_disk = match read_u16_le(tail_bytes, eocd_offset + 8) {
            Some(value) => value,
            None => continue,
        };
        let entry_count = match read_u16_le(tail_bytes, eocd_offset + 10) {
            Some(value) => value,
            None => continue,
        };
        let directory_size = match read_u32_le(tail_bytes, eocd_offset + 12) {
            Some(value) => value,
            None => continue,
        };
        let directory_offset = match read_u32_le(tail_bytes, eocd_offset + 16) {
            Some(value) => value,
            None => continue,
        };
        if entries_on_disk == u16::MAX
            || entry_count == u16::MAX
            || directory_size == u32::MAX
            || directory_offset == u32::MAX
        {
            return Err(CentralDirectoryError::Zip64Unsupported);
        }

        return Ok(Some(CentralDirectory {
            entry_count: usize::from(entry_count),
            size: directory_size as usize,
            offset: directory_offset as usize,
        }));
    }

    Ok(None)
}

fn read_u16_le(bytes: &[u8], offset: usize) -> Option<u16> {
    let chunk: [u8; 2] = bytes.get(offset..offset + 2)?.try_into().ok()?;
    Some(u16::from_le_bytes(chunk))
}

fn read_u32_le(bytes: &[u8], offset: usize) -> Option<u32> {
    let chunk: [u8; 4] = bytes.get(offset..offset + 4)?.try_into().ok()?;
    Some(u32::from_le_bytes(chunk))
}

#[cfg(test)]
mod tests {
    use super::{
        CentralDirectoryError, END_OF_CENTRAL_DIRECTORY_SIGNATURE, EOCD_LEN,
        ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIGNATURE, find_end_of_central_directory,
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

        let directory = find_end_of_central_directory(&bytes).unwrap().unwrap();

        assert_eq!(directory.entry_count, 0);
        assert_eq!(directory.offset, 0);
        assert_eq!(directory.size, 0);
    }

    #[test]
    fn find_end_of_central_directory_reports_zip64_sentinel_fields() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&END_OF_CENTRAL_DIRECTORY_SIGNATURE);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, u16::MAX);
        push_u16(&mut bytes, u16::MAX);
        push_u32(&mut bytes, u32::MAX);
        push_u32(&mut bytes, u32::MAX);
        push_u16(&mut bytes, 0);

        let error = find_end_of_central_directory(&bytes).unwrap_err();

        assert_eq!(error, CentralDirectoryError::Zip64Unsupported);
    }

    #[test]
    fn find_end_of_central_directory_reports_zip64_locator_records() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIGNATURE);
        bytes.extend_from_slice(&[0_u8; 16]);
        bytes.extend_from_slice(&END_OF_CENTRAL_DIRECTORY_SIGNATURE);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 0);
        push_u16(&mut bytes, 1);
        push_u16(&mut bytes, 1);
        push_u32(&mut bytes, 10);
        push_u32(&mut bytes, 20);
        push_u16(&mut bytes, 0);

        let error = find_end_of_central_directory(&bytes).unwrap_err();

        assert_eq!(error, CentralDirectoryError::Zip64Unsupported);
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

        let directory = find_end_of_central_directory(&bytes[6..]).unwrap().unwrap();

        assert_eq!(directory.entry_count, 0);
        assert_eq!(directory.offset, 0);
        assert_eq!(directory.size, 0);
        assert_eq!(EOCD_LEN, 22);
    }

    fn push_u16(buffer: &mut Vec<u8>, value: u16) {
        buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn push_u32(buffer: &mut Vec<u8>, value: u32) {
        buffer.extend_from_slice(&value.to_le_bytes());
    }
}
