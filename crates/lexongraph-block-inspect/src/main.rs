// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::path::{Path, PathBuf};

use ciborium::value::Value as CborValue;
use clap::{Parser, Subcommand};
use lexongraph_block::{
    Block, BlockHash, BranchBlock, BranchEntry, Content, EmbeddingSpec, ExtensionMap, LeafBlock,
    LeafEntry, Metadata, ValidatedBlock,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_block_store_fs::FilesystemBlockStore;
use serde_json::{Map, Number, Value};

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Inspect one stored LexonGraph block and emit a JSON debug view"
)]
struct Cli {
    #[command(subcommand)]
    backend: BackendCommand,
}

#[derive(Subcommand, Debug)]
enum BackendCommand {
    /// Inspect a block stored in a filesystem-backed block store.
    Fs {
        #[arg(long, value_name = "PATH")]
        store_root: PathBuf,
        #[arg(value_name = "BLOCK_HASH")]
        block_hash: String,
    },
}

#[derive(Debug)]
enum InspectError {
    InvalidBlockHash(String),
    StoreConstruction(String),
    BlockAbsence(BlockHash),
    BackendRetrieval(String),
    MalformedContent(String),
    IntegrityMismatch {
        expected: BlockHash,
        actual: BlockHash,
    },
    InspectionBoundary(String),
    JsonEncoding(String),
}

impl std::fmt::Display for InspectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBlockHash(message) => write!(f, "invalid block hash: {message}"),
            Self::StoreConstruction(message) => write!(f, "store construction failure: {message}"),
            Self::BlockAbsence(block_hash) => {
                write!(f, "block absence: no block found for {block_hash}")
            }
            Self::BackendRetrieval(message) => write!(f, "backend retrieval failure: {message}"),
            Self::MalformedContent(message) => write!(f, "malformed stored content: {message}"),
            Self::IntegrityMismatch { expected, actual } => {
                write!(f, "integrity mismatch: expected {expected}, got {actual}")
            }
            Self::InspectionBoundary(message) => write!(f, "inspection failure: {message}"),
            Self::JsonEncoding(message) => write!(f, "json encoding failure: {message}"),
        }
    }
}

impl std::error::Error for InspectError {}

fn main() {
    let cli = Cli::parse();
    match run(cli) {
        Ok(json) => println!("{json}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn run(cli: Cli) -> Result<String, InspectError> {
    let document = match cli.backend {
        BackendCommand::Fs {
            store_root,
            block_hash,
        } => {
            let block_hash = parse_block_hash(&block_hash)?;
            let store = open_filesystem_store(&store_root)?;
            inspect_store(&store, &block_hash)?
        }
    };

    serde_json::to_string_pretty(&document)
        .map_err(|error| InspectError::JsonEncoding(error.to_string()))
}

fn parse_block_hash(input: &str) -> Result<BlockHash, InspectError> {
    let expected_length = BlockHash::LEN * 2;
    if input.len() != expected_length {
        return Err(InspectError::InvalidBlockHash(format!(
            "expected {expected_length} hexadecimal characters, got {}",
            input.len()
        )));
    }

    let mut bytes = [0_u8; BlockHash::LEN];
    for (index, chunk) in input.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(chunk[0]).ok_or_else(|| {
            InspectError::InvalidBlockHash(format!(
                "found non-hexadecimal character {:?} at character {}",
                char::from(chunk[0]),
                index * 2
            ))
        })?;
        let low = decode_hex_nibble(chunk[1]).ok_or_else(|| {
            InspectError::InvalidBlockHash(format!(
                "found non-hexadecimal character {:?} at character {}",
                char::from(chunk[1]),
                index * 2 + 1
            ))
        })?;
        bytes[index] = (high << 4) | low;
    }

    Ok(BlockHash::from_bytes(bytes))
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn open_filesystem_store(store_root: &Path) -> Result<FilesystemBlockStore, InspectError> {
    FilesystemBlockStore::new(store_root).map_err(|error| match error {
        BlockStoreError::BackendFailure(message) => InspectError::StoreConstruction(message),
        BlockStoreError::MalformedContent(inner) => {
            InspectError::StoreConstruction(format!("unexpected malformed content: {inner}"))
        }
        BlockStoreError::IntegrityMismatch { expected, actual } => {
            InspectError::StoreConstruction(format!(
                "unexpected integrity mismatch while opening store: expected {expected}, got {actual}"
            ))
        }
        BlockStoreError::ContractViolation(inner) => {
            InspectError::StoreConstruction(format!("unexpected contract violation: {inner}"))
        }
    })
}

fn inspect_store(store: &impl BlockStore, block_hash: &BlockHash) -> Result<Value, InspectError> {
    match store.get(block_hash) {
        Ok(Some(validated_block)) => render_inspection_document(validated_block),
        Ok(None) => Err(InspectError::BlockAbsence(*block_hash)),
        Err(BlockStoreError::BackendFailure(message)) => {
            Err(InspectError::BackendRetrieval(message))
        }
        Err(BlockStoreError::MalformedContent(error)) => {
            Err(InspectError::MalformedContent(error.to_string()))
        }
        Err(BlockStoreError::IntegrityMismatch { expected, actual }) => {
            Err(InspectError::IntegrityMismatch { expected, actual })
        }
        Err(BlockStoreError::ContractViolation(error)) => Err(InspectError::InspectionBoundary(
            format!("unexpected block store contract violation: {error}"),
        )),
    }
}

fn render_inspection_document(validated_block: ValidatedBlock) -> Result<Value, InspectError> {
    let hash = validated_block.hash.to_string();
    let (kind, block) = match validated_block.block {
        Block::Branch(block) => ("branch", render_branch_block(block)?),
        Block::Leaf(block) => ("leaf", render_leaf_block(block)?),
    };

    Ok(object([
        ("hash", Value::String(hash)),
        ("kind", Value::String(kind.to_owned())),
        ("block", block),
    ]))
}

fn render_branch_block(block: BranchBlock) -> Result<Value, InspectError> {
    Ok(object([
        ("version", Value::Number(Number::from(block.version))),
        (
            "embedding_spec",
            render_embedding_spec(block.embedding_spec),
        ),
        (
            "entries",
            Value::Array(
                block
                    .entries
                    .into_iter()
                    .map(render_branch_entry)
                    .collect::<Vec<_>>(),
            ),
        ),
        ("ext", render_optional_map(block.ext.as_ref())?),
    ]))
}

fn render_leaf_block(block: LeafBlock) -> Result<Value, InspectError> {
    let entries = block
        .entries
        .into_iter()
        .map(render_leaf_entry)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(object([
        ("version", Value::Number(Number::from(block.version))),
        (
            "embedding_spec",
            render_embedding_spec(block.embedding_spec),
        ),
        ("entries", Value::Array(entries)),
        ("ext", render_optional_map(block.ext.as_ref())?),
    ]))
}

fn render_embedding_spec(spec: EmbeddingSpec) -> Value {
    object([
        ("dims", Value::Number(Number::from(spec.dims))),
        ("encoding", Value::String(spec.encoding)),
    ])
}

fn render_branch_entry(entry: BranchEntry) -> Value {
    object([
        ("embedding", render_bytes(&entry.embedding)),
        ("child", Value::String(entry.child.to_string())),
    ])
}

fn render_leaf_entry(entry: LeafEntry) -> Result<Value, InspectError> {
    Ok(object([
        ("embedding", render_bytes(&entry.embedding)),
        ("metadata", render_cbor_map(&entry.metadata)?),
        ("content", render_content(entry.content)),
    ]))
}

fn render_content(content: Content) -> Value {
    object([
        ("media_type", Value::String(content.media_type)),
        ("body", render_bytes(&content.body)),
    ])
}

fn render_optional_map(map: Option<&ExtensionMap>) -> Result<Value, InspectError> {
    match map {
        Some(entries) => render_cbor_map(entries),
        None => Ok(Value::Null),
    }
}

fn render_cbor_map(entries: &Metadata) -> Result<Value, InspectError> {
    let rendered_entries = entries
        .iter()
        .map(|(key, value)| {
            Ok(object([
                ("key", render_cbor_value(key)?),
                ("value", render_cbor_value(value)?),
            ]))
        })
        .collect::<Result<Vec<_>, InspectError>>()?;

    Ok(object([
        ("$type", Value::String("map".to_owned())),
        ("entries", Value::Array(rendered_entries)),
    ]))
}

fn render_cbor_value(value: &CborValue) -> Result<Value, InspectError> {
    match value {
        CborValue::Integer(integer) => Ok(object([
            ("$type", Value::String("integer".to_owned())),
            ("value", Value::String(render_cbor_integer(integer)?)),
        ])),
        CborValue::Bytes(bytes) => Ok(render_bytes(bytes)),
        CborValue::Float(float) => Ok(object([
            ("$type", Value::String("float".to_owned())),
            ("value", Value::String(float.to_string())),
        ])),
        CborValue::Text(text) => Ok(Value::String(text.clone())),
        CborValue::Bool(boolean) => Ok(Value::Bool(*boolean)),
        CborValue::Null => Ok(Value::Null),
        CborValue::Tag(tag, nested) => Ok(object([
            ("$type", Value::String("tag".to_owned())),
            ("tag", Value::String(tag.to_string())),
            ("value", render_cbor_value(nested)?),
        ])),
        CborValue::Array(values) => Ok(Value::Array(
            values
                .iter()
                .map(render_cbor_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        CborValue::Map(entries) => Ok(render_cbor_map(entries)?),
        _ => Err(InspectError::JsonEncoding(
            "unsupported CBOR value encountered during debug rendering".to_owned(),
        )),
    }
}

fn render_bytes(bytes: &[u8]) -> Value {
    object([
        ("$type", Value::String("bytes".to_owned())),
        ("hex", Value::String(encode_hex(bytes))),
    ])
}

fn render_cbor_integer(integer: &ciborium::value::Integer) -> Result<String, InspectError> {
    Ok(i128::from(*integer).to_string())
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn object<const N: usize>(fields: [(&str, Value); N]) -> Value {
    let mut object = Map::with_capacity(N);
    for (key, value) in fields {
        object.insert(key.to_owned(), value);
    }
    Value::Object(object)
}
