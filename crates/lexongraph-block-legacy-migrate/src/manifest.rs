// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use lexongraph_block::BlockHash;

use crate::MigrationError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManifestRow {
    pub(crate) legacy_hash: BlockHash,
    pub(crate) new_hash: BlockHash,
}

pub(crate) fn write_manifest(path: &Path, rows: &[ManifestRow]) -> Result<(), MigrationError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|error| {
            MigrationError::ManifestWrite(format!(
                "failed to create manifest directory {}: {error}",
                parent.display()
            ))
        })?;
    }

    let mut sorted_rows = rows.to_vec();
    sorted_rows.sort_by(|left, right| {
        left.legacy_hash
            .as_bytes()
            .cmp(right.legacy_hash.as_bytes())
    });
    for pair in sorted_rows.windows(2) {
        if pair[0].legacy_hash == pair[1].legacy_hash {
            return Err(MigrationError::ManifestWrite(format!(
                "duplicate legacy block id {} in manifest rows",
                pair[0].legacy_hash
            )));
        }
    }

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            MigrationError::ManifestWrite(format!(
                "failed to create manifest {}: {error}",
                path.display()
            ))
        })?;
    writeln!(file, "legacy_block_id,new_block_id").map_err(|error| {
        MigrationError::ManifestWrite(format!(
            "failed to write manifest header to {}: {error}",
            path.display()
        ))
    })?;
    for row in sorted_rows {
        writeln!(file, "{},{}", row.legacy_hash, row.new_hash).map_err(|error| {
            MigrationError::ManifestWrite(format!(
                "failed to write manifest row to {}: {error}",
                path.display()
            ))
        })?;
    }
    file.flush().map_err(|error| {
        MigrationError::ManifestWrite(format!(
            "failed to flush manifest {}: {error}",
            path.display()
        ))
    })
}
