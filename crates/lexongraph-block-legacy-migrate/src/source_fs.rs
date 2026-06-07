// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::fs;
use std::path::{Path, PathBuf};

use lexongraph_block::BlockHash;

use crate::MigrationError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceBlock {
    pub(crate) legacy_hash: BlockHash,
    pub(crate) path: PathBuf,
}

pub(crate) fn collect_source_blocks(root: &Path) -> Result<Vec<SourceBlock>, MigrationError> {
    let root_entries = read_dir_paths(root).map_err(|error| {
        MigrationError::SourceTraversal(format!(
            "failed to enumerate source root {}: {error}",
            root.display()
        ))
    })?;
    let mut pending = root_entries
        .into_iter()
        .rev()
        .map(|path| (path, 0_usize))
        .collect::<Vec<_>>();
    let mut blocks = Vec::new();

    while let Some((path, depth)) = pending.pop() {
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            MigrationError::SourceTraversal(format!(
                "failed to stat enumerated source entry {}: {error}",
                path.display()
            ))
        })?;
        if metadata.is_dir() {
            if depth >= 2 {
                continue;
            }
            let children = read_dir_paths(&path).map_err(|error| {
                MigrationError::SourceTraversal(format!(
                    "failed to enumerate source directory {}: {error}",
                    path.display()
                ))
            })?;
            for child in children.into_iter().rev() {
                pending.push((child, depth + 1));
            }
            continue;
        }

        if depth != 2 {
            continue;
        }

        match decode_source_block_path(root, &path)? {
            Some(legacy_hash) => blocks.push(SourceBlock { legacy_hash, path }),
            None => continue,
        }
    }

    blocks.sort_by(|left, right| {
        left.legacy_hash
            .as_bytes()
            .cmp(right.legacy_hash.as_bytes())
    });
    for pair in blocks.windows(2) {
        if pair[0].legacy_hash == pair[1].legacy_hash {
            return Err(MigrationError::SourceTraversal(format!(
                "multiple source files resolved to legacy block id {}",
                pair[0].legacy_hash
            )));
        }
    }

    Ok(blocks)
}

fn read_dir_paths(path: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut children = fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    children.sort();
    Ok(children)
}

fn decode_source_block_path(root: &Path, path: &Path) -> Result<Option<BlockHash>, MigrationError> {
    let relative = path.strip_prefix(root).map_err(|error| {
        MigrationError::SourceTraversal(format!(
            "failed to normalize source path {} relative to {}: {error}",
            path.display(),
            root.display()
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
        MigrationError::SourceTraversal("failed to decode a source shard directory name".into())
    })?;
    let second_level = second_level.as_os_str().to_str().ok_or_else(|| {
        MigrationError::SourceTraversal("failed to decode a source shard directory name".into())
    })?;
    if !is_lower_hex_prefix(first_level) || !is_lower_hex_prefix(second_level) {
        return Ok(None);
    }

    let file_name = file_name.as_os_str().to_str().ok_or_else(|| {
        MigrationError::SourceTraversal("failed to decode a source block file name".into())
    })?;
    let Some(hex) = file_name.strip_suffix(".cbor") else {
        return Ok(None);
    };
    let bytes = decode_block_hash_hex(hex).ok_or_else(|| {
        MigrationError::SourceTraversal(format!(
            "failed to decode source block id candidate from {}",
            path.display()
        ))
    })?;
    if &hex[..2] != first_level || &hex[2..4] != second_level {
        return Err(MigrationError::SourceTraversal(format!(
            "source block id candidate at {} does not match its shard prefixes",
            path.display()
        )));
    }

    Ok(Some(BlockHash::from_bytes(bytes)))
}

fn is_lower_hex_prefix(value: &str) -> bool {
    value.len() == 2
        && value
            .as_bytes()
            .iter()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
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

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}
