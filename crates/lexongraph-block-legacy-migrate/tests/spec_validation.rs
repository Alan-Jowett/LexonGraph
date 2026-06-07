// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use ciborium::value::Value as CborValue;
use lexongraph_block::{Block, BlockHash, VERSION_1, compute_block_hash};
use lexongraph_block_store::BlockStore;
use lexongraph_block_store_fs::FilesystemBlockStore;

#[test]
fn val_mig_001_repository_includes_crate_and_verification_artifacts() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_manifest = std::fs::read_to_string(
        manifest_dir
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("Cargo.toml"),
    )
    .unwrap();
    let package_manifest = std::fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();

    assert!(workspace_manifest.contains("\"crates/lexongraph-block-legacy-migrate\""));
    assert!(package_manifest.contains("name = \"lexongraph-block-legacy-migrate\""));
    assert!(
        manifest_dir
            .join("tests")
            .join("spec_validation.rs")
            .is_file()
    );
}

#[test]
fn val_mig_002_cli_help_exposes_filesystem_inputs() {
    let top_level_output = migrate_command().arg("--help").output().unwrap();
    let fs_output = migrate_command().args(["fs", "--help"]).output().unwrap();

    assert!(
        top_level_output.status.success(),
        "{}",
        command_debug(&top_level_output)
    );
    assert!(fs_output.status.success(), "{}", command_debug(&fs_output));

    let top_level_stdout = String::from_utf8(top_level_output.stdout).unwrap();
    let fs_stdout = String::from_utf8(fs_output.stdout).unwrap();
    assert!(top_level_stdout.contains("fs"));
    assert!(fs_stdout.contains("--source-root"));
    assert!(fs_stdout.contains("--destination-root"));
    assert!(fs_stdout.contains("--manifest-path"));
}

#[test]
fn val_mig_003_004_005_006_migrates_legacy_leaf_corpus_and_emits_deterministic_manifest() {
    let source_root = tempfile::tempdir().unwrap();
    let destination_root = tempfile::tempdir().unwrap();
    let manifest_dir = tempfile::tempdir().unwrap();
    let manifest_path = manifest_dir.path().join("migration.csv");

    let first = legacy_leaf_bytes("beta", "leaf-beta");
    let second = legacy_leaf_bytes("alpha", "leaf-alpha");
    let first_hash = write_legacy_block(source_root.path(), &first);
    let second_hash = write_legacy_block(source_root.path(), &second);
    let first_before = std::fs::read(expected_block_path(source_root.path(), &first_hash)).unwrap();
    let second_before =
        std::fs::read(expected_block_path(source_root.path(), &second_hash)).unwrap();

    let output = run_fs_migration(source_root.path(), destination_root.path(), &manifest_path);
    assert!(output.status.success(), "{}", command_debug(&output));

    let manifest = std::fs::read_to_string(&manifest_path).unwrap();
    let rows = manifest.lines().collect::<Vec<_>>();
    assert_eq!(rows[0], "legacy_block_id,new_block_id");

    let expected_legacy_ids = sorted_hexes([first_hash, second_hash]);
    let manifest_legacy_ids = rows[1..]
        .iter()
        .map(|row| row.split(',').next().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(manifest_legacy_ids, expected_legacy_ids);

    let destination_store = FilesystemBlockStore::new(destination_root.path()).unwrap();
    for row in &rows[1..] {
        let mut fields = row.split(',');
        let legacy_hash = parse_block_hash(fields.next().unwrap());
        let new_hash = parse_block_hash(fields.next().unwrap());
        assert_ne!(legacy_hash, new_hash);

        let loaded = destination_store.get(&new_hash).unwrap().unwrap();
        match loaded.block {
            Block::Leaf(block) => assert_eq!(block.level, 0),
            Block::Branch(_) => panic!("expected migrated block to remain a leaf"),
        }
    }

    assert_eq!(
        std::fs::read(expected_block_path(source_root.path(), &first_hash)).unwrap(),
        first_before
    );
    assert_eq!(
        std::fs::read(expected_block_path(source_root.path(), &second_hash)).unwrap(),
        second_before
    );
}

#[test]
fn val_mig_007_unsupported_legacy_non_leaf_fails_explicitly() {
    let source_root = tempfile::tempdir().unwrap();
    let destination_root = tempfile::tempdir().unwrap();
    let manifest_dir = tempfile::tempdir().unwrap();
    let manifest_path = manifest_dir.path().join("migration.csv");

    let branch = legacy_branch_bytes();
    write_legacy_block(source_root.path(), &branch);

    let output = run_fs_migration(source_root.path(), destination_root.path(), &manifest_path);

    assert!(!output.status.success(), "{}", command_debug(&output));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("unsupported legacy block kind"));
}

#[test]
fn val_mig_008_manifest_creation_failures_are_explicit() {
    let source_root = tempfile::tempdir().unwrap();
    let destination_root = tempfile::tempdir().unwrap();
    let manifest_dir = tempfile::tempdir().unwrap();
    let manifest_path = manifest_dir.path().join("migration.csv");

    let leaf = legacy_leaf_bytes("alpha", "leaf-alpha");
    write_legacy_block(source_root.path(), &leaf);
    std::fs::write(&manifest_path, b"already exists").unwrap();

    let output = run_fs_migration(source_root.path(), destination_root.path(), &manifest_path);

    assert!(!output.status.success(), "{}", command_debug(&output));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("manifest write failure"));
}

fn migrate_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_lexongraph-block-legacy-migrate"))
}

fn run_fs_migration(source_root: &Path, destination_root: &Path, manifest_path: &Path) -> Output {
    migrate_command()
        .arg("fs")
        .arg("--source-root")
        .arg(source_root)
        .arg("--destination-root")
        .arg(destination_root)
        .arg("--manifest-path")
        .arg(manifest_path)
        .output()
        .unwrap()
}

fn command_debug(output: &Output) -> String {
    format!(
        "status: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn write_legacy_block(root: &Path, bytes: &[u8]) -> BlockHash {
    let block_id = compute_block_hash(bytes);
    let path = expected_block_path(root, &block_id);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
    block_id
}

fn expected_block_path(root: &Path, block_id: &BlockHash) -> PathBuf {
    let canonical_root = root.canonicalize().unwrap();
    let hex = block_id.to_string();
    canonical_root
        .join(&hex[..2])
        .join(&hex[2..4])
        .join(format!("{hex}.cbor"))
}

fn sorted_hexes(values: [BlockHash; 2]) -> Vec<String> {
    let mut strings = values.map(|value| value.to_string()).to_vec();
    strings.sort();
    strings
}

fn parse_block_hash(input: &str) -> BlockHash {
    assert_eq!(input.len(), BlockHash::LEN * 2);
    let mut bytes = [0_u8; BlockHash::LEN];
    for (index, chunk) in input.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(chunk[0]).unwrap();
        let low = decode_hex_nibble(chunk[1]).unwrap();
        bytes[index] = (high << 4) | low;
    }
    BlockHash::from_bytes(bytes)
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn legacy_leaf_bytes(label: &str, body: &str) -> Vec<u8> {
    encode_cbor(CborValue::Map(vec![
        (int_value(0), int_value(VERSION_1)),
        (CborValue::Integer(1.into()), CborValue::Text("leaf".into())),
        (
            int_value(2),
            CborValue::Map(vec![
                (int_value(0), int_value(4)),
                (
                    CborValue::Integer(1.into()),
                    CborValue::Text("f32le".into()),
                ),
            ]),
        ),
        (
            int_value(3),
            CborValue::Array(vec![CborValue::Map(vec![
                (int_value(0), CborValue::Bytes(vec![0x10, 0x20, 0x30, 0x40])),
                (
                    int_value(1),
                    CborValue::Map(vec![(
                        CborValue::Text("label".into()),
                        CborValue::Text(label.into()),
                    )]),
                ),
                (
                    int_value(2),
                    CborValue::Map(vec![
                        (int_value(0), CborValue::Text("text/plain".into())),
                        (int_value(1), CborValue::Bytes(body.as_bytes().to_vec())),
                    ]),
                ),
            ])]),
        ),
    ]))
}

fn legacy_branch_bytes() -> Vec<u8> {
    encode_cbor(CborValue::Map(vec![
        (int_value(0), int_value(VERSION_1)),
        (
            CborValue::Integer(1.into()),
            CborValue::Text("branch".into()),
        ),
        (
            int_value(2),
            CborValue::Map(vec![
                (int_value(0), int_value(4)),
                (
                    CborValue::Integer(1.into()),
                    CborValue::Text("f32le".into()),
                ),
            ]),
        ),
        (
            int_value(3),
            CborValue::Array(vec![CborValue::Map(vec![
                (int_value(0), CborValue::Bytes(vec![0x10, 0x20, 0x30, 0x40])),
                (int_value(1), CborValue::Bytes([0x77; 32].to_vec())),
            ])]),
        ),
    ]))
}

fn encode_cbor(value: CborValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(&value, &mut bytes).unwrap();
    bytes
}

fn int_value(value: u64) -> CborValue {
    CborValue::Integer(value.into())
}
