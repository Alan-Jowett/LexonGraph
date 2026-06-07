// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
mod legacy_leaf;
mod manifest;
mod source_fs;

use std::fs;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use lexongraph_block::{BlockHash, compute_block_hash};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_block_store_fs::FilesystemBlockStore;
use manifest::ManifestRow;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Migrate legacy LexonGraph kind=\"leaf\" blocks into the current level-based filesystem store"
)]
struct Cli {
    #[command(subcommand)]
    backend: BackendCommand,
}

#[derive(Subcommand, Debug)]
enum BackendCommand {
    /// Migrate a legacy filesystem-backed leaf corpus into a current filesystem-backed store.
    Fs {
        #[arg(long, value_name = "PATH")]
        source_root: PathBuf,
        #[arg(long, value_name = "PATH")]
        destination_root: PathBuf,
        #[arg(long, value_name = "PATH")]
        manifest_path: PathBuf,
    },
}

#[derive(Debug)]
enum MigrationError {
    SourceRoot(String),
    DestinationRoot(String),
    SourceTraversal(String),
    SourceRead {
        path: PathBuf,
        message: String,
    },
    SourceIntegrity {
        path: PathBuf,
        expected: BlockHash,
        actual: BlockHash,
    },
    LegacyDecode(String),
    DestinationWrite(String),
    ManifestWrite(String),
    InPlaceMigration(PathBuf),
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SourceRoot(message) => write!(f, "source root failure: {message}"),
            Self::DestinationRoot(message) => write!(f, "destination root failure: {message}"),
            Self::SourceTraversal(message) => write!(f, "source traversal failure: {message}"),
            Self::SourceRead { path, message } => {
                write!(f, "source read failure at {}: {message}", path.display())
            }
            Self::SourceIntegrity {
                path,
                expected,
                actual,
            } => write!(
                f,
                "source integrity failure at {}: expected legacy block id {expected}, got {actual}",
                path.display()
            ),
            Self::LegacyDecode(message) => write!(f, "legacy decode failure: {message}"),
            Self::DestinationWrite(message) => write!(f, "destination write failure: {message}"),
            Self::ManifestWrite(message) => write!(f, "manifest write failure: {message}"),
            Self::InPlaceMigration(path) => write!(
                f,
                "source and destination roots resolve to the same directory {}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for MigrationError {}

fn main() {
    let cli = Cli::parse();
    match run(cli) {
        Ok(summary) => println!("{summary}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn run(cli: Cli) -> Result<String, MigrationError> {
    match cli.backend {
        BackendCommand::Fs {
            source_root,
            destination_root,
            manifest_path,
        } => run_filesystem_migration(&source_root, &destination_root, &manifest_path),
    }
}

fn run_filesystem_migration(
    source_root: &Path,
    destination_root: &Path,
    manifest_path: &Path,
) -> Result<String, MigrationError> {
    let canonical_source_root = canonicalize_existing_directory(source_root).map_err(|error| {
        MigrationError::SourceRoot(format!(
            "failed to open source root {}: {error}",
            source_root.display()
        ))
    })?;
    let destination_store =
        FilesystemBlockStore::new(destination_root).map_err(map_destination_store_error)?;
    let canonical_destination_root =
        canonicalize_existing_directory(destination_root).map_err(|error| {
            MigrationError::DestinationRoot(format!(
                "failed to open destination root {}: {error}",
                destination_root.display()
            ))
        })?;
    if canonical_source_root == canonical_destination_root {
        return Err(MigrationError::InPlaceMigration(canonical_source_root));
    }

    let source_blocks = source_fs::collect_source_blocks(&canonical_source_root)?;
    let mut manifest_rows = Vec::with_capacity(source_blocks.len());
    for source_block in source_blocks {
        let bytes = fs::read(&source_block.path).map_err(|error| MigrationError::SourceRead {
            path: source_block.path.clone(),
            message: error.to_string(),
        })?;
        let actual_legacy_hash = compute_block_hash(&bytes);
        if actual_legacy_hash != source_block.legacy_hash {
            return Err(MigrationError::SourceIntegrity {
                path: source_block.path,
                expected: source_block.legacy_hash,
                actual: actual_legacy_hash,
            });
        }

        let block = legacy_leaf::decode_legacy_leaf(&bytes)?;
        let new_hash = destination_store
            .put(&block)
            .map_err(map_destination_put_error)?;
        manifest_rows.push(ManifestRow {
            legacy_hash: source_block.legacy_hash,
            new_hash,
        });
    }

    manifest::write_manifest(manifest_path, &manifest_rows)?;
    Ok(format!(
        "migrated {} legacy leaf block(s) from {} to {} and wrote {}",
        manifest_rows.len(),
        canonical_source_root.display(),
        canonical_destination_root.display(),
        manifest_path.display()
    ))
}

fn canonicalize_existing_directory(path: &Path) -> std::io::Result<PathBuf> {
    let canonical = path.canonicalize()?;
    let metadata = fs::metadata(&canonical)?;
    if metadata.is_dir() {
        Ok(canonical)
    } else {
        Err(std::io::Error::other(format!(
            "{} is not a directory",
            canonical.display()
        )))
    }
}

fn map_destination_store_error(error: BlockStoreError) -> MigrationError {
    match error {
        BlockStoreError::BackendFailure(message) => MigrationError::DestinationRoot(message),
        BlockStoreError::MalformedContent(inner) => MigrationError::DestinationRoot(format!(
            "unexpected malformed content while opening destination store: {inner}"
        )),
        BlockStoreError::IntegrityMismatch { expected, actual } => {
            MigrationError::DestinationRoot(format!(
                "unexpected integrity mismatch while opening destination store: expected {expected}, got {actual}"
            ))
        }
        BlockStoreError::ContractViolation(inner) => MigrationError::DestinationRoot(format!(
            "unexpected contract violation while opening destination store: {inner}"
        )),
    }
}

fn map_destination_put_error(error: BlockStoreError) -> MigrationError {
    match error {
        BlockStoreError::BackendFailure(message) => MigrationError::DestinationWrite(message),
        BlockStoreError::MalformedContent(inner) => MigrationError::DestinationWrite(format!(
            "unexpected malformed content while publishing a migrated block: {inner}"
        )),
        BlockStoreError::IntegrityMismatch { expected, actual } => {
            MigrationError::DestinationWrite(format!(
                "unexpected integrity mismatch while publishing a migrated block: expected {expected}, got {actual}"
            ))
        }
        BlockStoreError::ContractViolation(inner) => MigrationError::DestinationWrite(format!(
            "unexpected contract violation while publishing a migrated block: {inner}"
        )),
    }
}
