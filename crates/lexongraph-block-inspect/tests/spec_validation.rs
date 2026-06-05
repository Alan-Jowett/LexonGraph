// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use ciborium::value::Value as CborValue;
use lexongraph_block::{
    BlockHash, BranchEntry, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_branch_block,
    build_leaf_block, compute_block_hash,
};
use lexongraph_block_store::BlockStore;
use lexongraph_block_store_fs::FilesystemBlockStore;
use serde_json::Value;

#[test]
fn val_inspect_001_and_012_repository_includes_crate_and_verification_artifacts() {
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

    assert!(workspace_manifest.contains("\"crates/lexongraph-block-inspect\""));
    assert!(package_manifest.contains("name = \"lexongraph-block-inspect\""));
    assert!(
        manifest_dir
            .join("tests")
            .join("spec_validation.rs")
            .is_file()
    );
}

#[test]
fn val_inspect_002_cli_help_exposes_backend_selector_and_filesystem_inputs() {
    let top_level_output = inspect_command().arg("--help").output().unwrap();
    let fs_output = inspect_command().args(["fs", "--help"]).output().unwrap();

    assert!(
        top_level_output.status.success(),
        "{}",
        command_debug(&top_level_output)
    );
    assert!(fs_output.status.success(), "{}", command_debug(&fs_output));

    let top_level_stdout = String::from_utf8(top_level_output.stdout).unwrap();
    let fs_stdout = String::from_utf8(fs_output.stdout).unwrap();
    assert!(top_level_stdout.contains("fs"));
    assert!(fs_stdout.contains("--store-root"));
    assert!(fs_stdout.contains("BLOCK_HASH"));
}

#[test]
fn val_inspect_003_and_011_branch_inspection_is_single_block_and_non_recursive() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let child = BlockHash::from_bytes([0x55; 32]);
    let branch = build_branch_block(
        VERSION_1,
        1,
        EmbeddingSpec {
            dims: 4,
            encoding: "f32le".to_owned(),
        },
        vec![BranchEntry {
            embedding: vec![0x10, 0x20, 0x30, 0x40],
            child,
        }],
        None,
    )
    .unwrap();
    let branch_hash = store
        .put(&lexongraph_block::Block::Branch(branch.clone()))
        .unwrap();

    let output = run_fs_inspect(temp_dir.path(), &branch_hash.to_string());
    let json = successful_json(output);

    assert_eq!(json["hash"], branch_hash.to_string());
    assert_eq!(json["level"], 1);
    assert_eq!(json["block"]["entries"][0]["child"], child.to_string());
    assert_eq!(json["block"]["entries"][0]["embedding"]["$type"], "bytes");
}

#[test]
fn val_inspect_004_and_005_leaf_inspection_emits_debug_json_for_opaque_values() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let leaf = sample_leaf_block_with_opaque_values();
    let leaf_hash = store.put(&leaf.clone()).unwrap();

    let output = run_fs_inspect(temp_dir.path(), &leaf_hash.to_string());
    let json = successful_json(output);

    assert_eq!(json["hash"], leaf_hash.to_string());
    assert_eq!(json["level"], 0);
    assert_eq!(json["block"]["entries"][0]["embedding"]["$type"], "bytes");
    assert_eq!(
        json["block"]["entries"][0]["content"]["body"]["$type"],
        "bytes"
    );
    assert_eq!(json["block"]["ext"]["$type"], "map");

    let metadata_entries = json["block"]["entries"][0]["metadata"]["entries"]
        .as_array()
        .unwrap();
    assert!(metadata_entries.iter().any(|entry| {
        entry["key"] == "bytes"
            && entry["value"]["$type"] == "bytes"
            && entry["value"]["hex"] == "aabb"
    }));
    assert!(metadata_entries.iter().any(|entry| {
        entry["key"] == "int"
            && entry["value"]["$type"] == "integer"
            && entry["value"]["value"] == "42"
    }));
    assert!(metadata_entries.iter().any(|entry| {
        entry["key"] == "float"
            && entry["value"]["$type"] == "float"
            && entry["value"]["value"] == "1.5"
    }));
    assert!(metadata_entries.iter().any(|entry| {
        entry["key"] == "tagged"
            && entry["value"]["$type"] == "tag"
            && entry["value"]["tag"] == "24"
            && entry["value"]["value"] == "tagged"
    }));
    assert!(metadata_entries.iter().any(|entry| {
        entry["key"] == "nested"
            && entry["value"]["$type"] == "map"
            && entry["value"]["entries"].is_array()
    }));
}

#[test]
fn val_inspect_015_higher_level_branch_preserves_numeric_level() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let child = BlockHash::from_bytes([0x66; 32]);
    let branch = build_branch_block(
        VERSION_1,
        3,
        EmbeddingSpec {
            dims: 4,
            encoding: "f32le".to_owned(),
        },
        vec![BranchEntry {
            embedding: vec![0x10, 0x20, 0x30, 0x40],
            child,
        }],
        None,
    )
    .unwrap();
    let branch_hash = store.put(&lexongraph_block::Block::Branch(branch)).unwrap();

    let output = run_fs_inspect(temp_dir.path(), &branch_hash.to_string());
    let json = successful_json(output);

    assert_eq!(json["hash"], branch_hash.to_string());
    assert_eq!(json["level"], 3);
}

#[test]
fn val_inspect_006_absent_hash_fails_explicitly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output = run_fs_inspect(
        temp_dir.path(),
        &BlockHash::from_bytes([0x44; 32]).to_string(),
    );

    assert!(!output.status.success(), "{}", command_debug(&output));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("block absence"));
}

#[test]
fn val_inspect_007_malformed_content_fails_explicitly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let malformed_bytes = [0xff, 0xff, 0x00];
    let malformed_hash = compute_block_hash(&malformed_bytes);
    write_raw_block(temp_dir.path(), &malformed_hash, &malformed_bytes);

    let output = run_fs_inspect(temp_dir.path(), &malformed_hash.to_string());

    assert!(!output.status.success(), "{}", command_debug(&output));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("malformed stored content"));
}

#[test]
fn val_inspect_008_integrity_mismatch_fails_explicitly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let first_store = FilesystemBlockStore::new(temp_dir.path()).unwrap();
    let first = sample_simple_leaf_block("first");
    let second = sample_simple_leaf_block("second");
    let first_hash = first_store.put(&first.clone()).unwrap();
    let second_hash = first_store.put(&second.clone()).unwrap();
    let first_path = expected_block_path(temp_dir.path(), &first_hash);
    write_raw_block(
        temp_dir.path(),
        &second_hash,
        &std::fs::read(first_path).unwrap(),
    );

    let output = run_fs_inspect(temp_dir.path(), &second_hash.to_string());

    assert!(!output.status.success(), "{}", command_debug(&output));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("integrity mismatch"));
}

#[test]
fn val_inspect_009_invalid_hash_and_unsupported_backend_fail_explicitly() {
    let temp_dir = tempfile::tempdir().unwrap();

    let invalid_hash_output = run_fs_inspect(temp_dir.path(), "not-a-hash");
    assert!(
        !invalid_hash_output.status.success(),
        "{}",
        command_debug(&invalid_hash_output)
    );
    let invalid_hash_stderr = String::from_utf8(invalid_hash_output.stderr).unwrap();
    assert!(invalid_hash_stderr.contains("invalid block hash"));

    let unsupported_backend_output = inspect_command().arg("bogus").output().unwrap();
    assert!(
        !unsupported_backend_output.status.success(),
        "{}",
        command_debug(&unsupported_backend_output)
    );
    let unsupported_backend_stderr = String::from_utf8(unsupported_backend_output.stderr).unwrap();
    assert!(unsupported_backend_stderr.contains("unrecognized subcommand"));
}

#[test]
fn val_inspect_010_store_construction_failures_are_explicit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let non_directory_root = temp_dir.path().join("not-a-directory");
    std::fs::write(&non_directory_root, b"not a directory").unwrap();
    let output = run_fs_inspect(
        &non_directory_root,
        &BlockHash::from_bytes([0x11; 32]).to_string(),
    );

    assert!(!output.status.success(), "{}", command_debug(&output));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("store construction failure"));
}

#[test]
fn val_inspect_014_backend_retrieval_failures_are_explicit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let unreadable_hash = BlockHash::from_bytes([0x77; 32]);
    let block_path = expected_block_path(temp_dir.path(), &unreadable_hash);
    std::fs::create_dir_all(&block_path).unwrap();

    let output = run_fs_inspect(temp_dir.path(), &unreadable_hash.to_string());

    assert!(!output.status.success(), "{}", command_debug(&output));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("backend retrieval failure"));
}

#[test]
fn val_inspect_013_source_uses_blockstore_boundary() {
    let source = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("main.rs"),
    )
    .unwrap();

    assert!(source.contains("BlockStore"));
    assert!(source.contains("store.get(block_hash)"));
    assert!(source.contains("FilesystemBlockStore::new"));
    assert!(!source.contains(".cbor"));
}

fn sample_simple_leaf_block(body: &str) -> lexongraph_block::Block {
    lexongraph_block::Block::Leaf(
        build_leaf_block(
            VERSION_1,
            EmbeddingSpec {
                dims: 4,
                encoding: "f32le".to_owned(),
            },
            vec![LeafEntry {
                embedding: vec![0x01, 0x02, 0x03, 0x04],
                metadata: vec![(
                    CborValue::Text("body".to_owned()),
                    CborValue::Text(body.to_owned()),
                )],
                content: Content {
                    media_type: "text/plain".to_owned(),
                    body: body.as_bytes().to_vec(),
                },
            }],
            None,
        )
        .unwrap(),
    )
}

fn sample_leaf_block_with_opaque_values() -> lexongraph_block::Block {
    lexongraph_block::Block::Leaf(
        build_leaf_block(
            VERSION_1,
            EmbeddingSpec {
                dims: 4,
                encoding: "f32le".to_owned(),
            },
            vec![LeafEntry {
                embedding: vec![0xde, 0xad, 0xbe, 0xef],
                metadata: vec![
                    (
                        CborValue::Text("bytes".to_owned()),
                        CborValue::Bytes(vec![0xaa, 0xbb]),
                    ),
                    (
                        CborValue::Text("int".to_owned()),
                        CborValue::Integer(42.into()),
                    ),
                    (CborValue::Text("float".to_owned()), CborValue::Float(1.5)),
                    (
                        CborValue::Text("tagged".to_owned()),
                        CborValue::Tag(24, Box::new(CborValue::Text("tagged".to_owned()))),
                    ),
                    (
                        CborValue::Text("nested".to_owned()),
                        CborValue::Map(vec![(
                            CborValue::Integer(7.into()),
                            CborValue::Array(vec![CborValue::Bool(true), CborValue::Null]),
                        )]),
                    ),
                ],
                content: Content {
                    media_type: "application/octet-stream".to_owned(),
                    body: vec![0xca, 0xfe],
                },
            }],
            Some(vec![(
                CborValue::Text("ext-key".to_owned()),
                CborValue::Bytes(vec![0x12, 0x34]),
            )]),
        )
        .unwrap(),
    )
}

fn inspect_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_lexongraph-block-inspect"))
}

fn run_fs_inspect(store_root: &Path, block_hash: &str) -> Output {
    inspect_command()
        .arg("fs")
        .arg("--store-root")
        .arg(store_root)
        .arg(block_hash)
        .output()
        .unwrap()
}

fn successful_json(output: Output) -> Value {
    assert!(output.status.success(), "{}", command_debug(&output));
    serde_json::from_slice(&output.stdout).unwrap()
}

fn command_debug(output: &Output) -> String {
    format!(
        "status: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn expected_block_path(root: &Path, block_id: &BlockHash) -> PathBuf {
    let hex = block_id.to_string();
    root.join(&hex[..2])
        .join(&hex[2..4])
        .join(format!("{hex}.cbor"))
}

fn write_raw_block(root: &Path, block_id: &BlockHash, bytes: &[u8]) {
    let path = expected_block_path(root, block_id);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}
