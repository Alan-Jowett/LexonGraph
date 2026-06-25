// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

use ciborium::value::Value as CborValue;
use clap::{Parser, Subcommand};
use lexongraph_block::{
    Block, BlockHash, BranchBlock, BranchEntry, Content, EmbeddingSpec, ExtensionMap, LeafBlock,
    LeafEntry, Metadata, ValidatedBlock, serialize_block,
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
    /// Traverse a stored block tree and emit structural and size statistics as JSON.
    #[command(name = "fs-tree")]
    FsTree {
        #[arg(long, value_name = "PATH")]
        store_root: PathBuf,
        #[arg(long, default_value_t = 64, value_name = "COUNT")]
        expected_max_children: usize,
        #[arg(value_name = "ROOT_HASH")]
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
        BackendCommand::FsTree {
            store_root,
            expected_max_children,
            block_hash,
        } => {
            let block_hash = parse_block_hash(&block_hash)?;
            let store = open_filesystem_store(&store_root)?;
            analyze_tree(&store, &block_hash, expected_max_children)?
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
    load_validated_block(store, block_hash).and_then(render_inspection_document)
}

fn load_validated_block(
    store: &impl BlockStore,
    block_hash: &BlockHash,
) -> Result<ValidatedBlock, InspectError> {
    match store.get(block_hash) {
        Ok(Some(validated_block)) => Ok(validated_block),
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

fn analyze_tree(
    store: &impl BlockStore,
    root_hash: &BlockHash,
    expected_max_children: usize,
) -> Result<Value, InspectError> {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::from([*root_hash]);
    let mut repeated_reference_count = 0_usize;
    let mut branch_block_count = 0_usize;
    let mut leaf_block_count = 0_usize;
    let mut level_stats = BTreeMap::<u64, LevelStats>::new();
    let mut max_children_in_block = 0_usize;
    let mut max_serialized_bytes = 0_u64;
    let mut max_child_count_block = None::<ChildCountViolation>;
    let mut largest_blocks = Vec::<LargestBlock>::new();
    let mut child_cap_violations = Vec::<ChildCountViolation>::new();
    let mut root_level = None::<u64>;

    while let Some(block_hash) = queue.pop_front() {
        if !visited.insert(block_hash) {
            repeated_reference_count += 1;
            continue;
        }

        let validated = load_validated_block(store, &block_hash)?;
        let serialized = serialize_block(&validated.block)
            .map_err(|error| InspectError::InspectionBoundary(error.to_string()))?;
        let serialized_bytes = u64::try_from(serialized.bytes.len()).map_err(|_| {
            InspectError::InspectionBoundary("serialized block is too large".to_owned())
        })?;

        max_serialized_bytes = max_serialized_bytes.max(serialized_bytes);
        insert_largest_block(
            &mut largest_blocks,
            LargestBlock {
                hash: validated.hash,
                level: block_level(&validated.block),
                serialized_bytes,
                child_count: branch_child_count(&validated.block),
                kind: block_kind(&validated.block),
            },
        );

        match validated.block {
            Block::Branch(branch) => {
                if root_level.is_none() {
                    root_level = Some(branch.level);
                }
                branch_block_count += 1;
                let child_count = branch.entries.len();
                max_children_in_block = max_children_in_block.max(child_count);

                if max_child_count_block
                    .as_ref()
                    .is_none_or(|current| child_count > current.child_count)
                {
                    max_child_count_block = Some(ChildCountViolation {
                        hash: validated.hash,
                        level: branch.level,
                        child_count,
                        serialized_bytes,
                    });
                }

                if child_count > expected_max_children {
                    child_cap_violations.push(ChildCountViolation {
                        hash: validated.hash,
                        level: branch.level,
                        child_count,
                        serialized_bytes,
                    });
                }

                level_stats
                    .entry(branch.level)
                    .or_default()
                    .observe_branch(child_count, serialized_bytes);

                for entry in branch.entries {
                    queue.push_back(entry.child);
                }
            }
            Block::Leaf(leaf) => {
                if root_level.is_none() {
                    root_level = Some(leaf.level);
                }
                leaf_block_count += 1;
                level_stats
                    .entry(leaf.level)
                    .or_default()
                    .observe_leaf(serialized_bytes);
            }
        }
    }

    let unique_block_count = branch_block_count + leaf_block_count;

    Ok(object([
        ("root_hash", Value::String(root_hash.to_string())),
        (
            "root_level",
            Value::Number(Number::from(root_level.unwrap_or_default())),
        ),
        (
            "expected_max_children",
            Value::Number(Number::from(u64::try_from(expected_max_children).map_err(
                |_| InspectError::InspectionBoundary("child cap does not fit in u64".to_owned()),
            )?)),
        ),
        (
            "unique_block_count",
            Value::Number(Number::from(u64::try_from(unique_block_count).map_err(
                |_| InspectError::InspectionBoundary("block count does not fit in u64".to_owned()),
            )?)),
        ),
        (
            "branch_block_count",
            Value::Number(Number::from(u64::try_from(branch_block_count).map_err(
                |_| InspectError::InspectionBoundary("branch count does not fit in u64".to_owned()),
            )?)),
        ),
        (
            "leaf_block_count",
            Value::Number(Number::from(u64::try_from(leaf_block_count).map_err(
                |_| InspectError::InspectionBoundary("leaf count does not fit in u64".to_owned()),
            )?)),
        ),
        (
            "repeated_reference_count",
            Value::Number(Number::from(
                u64::try_from(repeated_reference_count).map_err(|_| {
                    InspectError::InspectionBoundary(
                        "repeated reference count does not fit in u64".to_owned(),
                    )
                })?,
            )),
        ),
        (
            "max_children_in_block",
            Value::Number(Number::from(u64::try_from(max_children_in_block).map_err(
                |_| {
                    InspectError::InspectionBoundary(
                        "max child count does not fit in u64".to_owned(),
                    )
                },
            )?)),
        ),
        (
            "blocks_exceeding_child_cap_count",
            Value::Number(Number::from(
                u64::try_from(child_cap_violations.len()).map_err(|_| {
                    InspectError::InspectionBoundary(
                        "child-cap violation count does not fit in u64".to_owned(),
                    )
                })?,
            )),
        ),
        (
            "largest_serialized_block_bytes",
            Value::Number(Number::from(max_serialized_bytes)),
        ),
        (
            "block_with_max_children",
            render_optional_child_count_violation(max_child_count_block),
        ),
        (
            "levels",
            Value::Array(
                level_stats
                    .into_iter()
                    .map(|(level, stats)| stats.render(level))
                    .collect(),
            ),
        ),
        (
            "blocks_exceeding_child_cap",
            Value::Array(
                child_cap_violations
                    .into_iter()
                    .map(render_child_count_violation)
                    .collect(),
            ),
        ),
        (
            "largest_blocks",
            Value::Array(
                largest_blocks
                    .into_iter()
                    .map(render_largest_block)
                    .collect(),
            ),
        ),
    ]))
}

fn render_inspection_document(validated_block: ValidatedBlock) -> Result<Value, InspectError> {
    let hash = validated_block.hash.to_string();
    let (level, block) = match validated_block.block {
        Block::Branch(block) => (block.level, render_branch_block(block)?),
        Block::Leaf(block) => (block.level, render_leaf_block(block)?),
    };

    Ok(object([
        ("hash", Value::String(hash)),
        ("level", Value::Number(Number::from(level))),
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

#[derive(Default)]
struct LevelStats {
    block_count: u64,
    branch_block_count: u64,
    leaf_block_count: u64,
    total_children: u64,
    min_children: Option<u64>,
    max_children: u64,
    total_serialized_bytes: u64,
    max_serialized_bytes: u64,
}

impl LevelStats {
    fn observe_branch(&mut self, child_count: usize, serialized_bytes: u64) {
        let child_count = u64::try_from(child_count).expect("usize child counts always fit in u64");
        self.block_count += 1;
        self.branch_block_count += 1;
        self.total_children += child_count;
        self.min_children = Some(
            self.min_children
                .map_or(child_count, |current| current.min(child_count)),
        );
        self.max_children = self.max_children.max(child_count);
        self.total_serialized_bytes += serialized_bytes;
        self.max_serialized_bytes = self.max_serialized_bytes.max(serialized_bytes);
    }

    fn observe_leaf(&mut self, serialized_bytes: u64) {
        self.block_count += 1;
        self.leaf_block_count += 1;
        self.total_serialized_bytes += serialized_bytes;
        self.max_serialized_bytes = self.max_serialized_bytes.max(serialized_bytes);
    }

    fn render(self, level: u64) -> Value {
        object([
            ("level", Value::Number(Number::from(level))),
            ("block_count", Value::Number(Number::from(self.block_count))),
            (
                "branch_block_count",
                Value::Number(Number::from(self.branch_block_count)),
            ),
            (
                "leaf_block_count",
                Value::Number(Number::from(self.leaf_block_count)),
            ),
            (
                "total_children",
                Value::Number(Number::from(self.total_children)),
            ),
            (
                "min_children_per_branch",
                self.min_children
                    .map_or(Value::Null, |value| Value::Number(Number::from(value))),
            ),
            (
                "max_children_per_branch",
                Value::Number(Number::from(self.max_children)),
            ),
            (
                "mean_children_per_branch",
                if self.branch_block_count == 0 {
                    Value::Null
                } else {
                    Value::Number(
                        Number::from_f64(
                            self.total_children as f64 / self.branch_block_count as f64,
                        )
                        .expect("finite branch mean"),
                    )
                },
            ),
            (
                "total_serialized_bytes",
                Value::Number(Number::from(self.total_serialized_bytes)),
            ),
            (
                "max_serialized_bytes",
                Value::Number(Number::from(self.max_serialized_bytes)),
            ),
        ])
    }
}

struct ChildCountViolation {
    hash: BlockHash,
    level: u64,
    child_count: usize,
    serialized_bytes: u64,
}

struct LargestBlock {
    hash: BlockHash,
    level: u64,
    serialized_bytes: u64,
    child_count: Option<usize>,
    kind: &'static str,
}

fn block_level(block: &Block) -> u64 {
    match block {
        Block::Branch(branch) => branch.level,
        Block::Leaf(leaf) => leaf.level,
    }
}

fn branch_child_count(block: &Block) -> Option<usize> {
    match block {
        Block::Branch(branch) => Some(branch.entries.len()),
        Block::Leaf(_) => None,
    }
}

fn block_kind(block: &Block) -> &'static str {
    match block {
        Block::Branch(_) => "branch",
        Block::Leaf(_) => "leaf",
    }
}

fn render_optional_child_count_violation(violation: Option<ChildCountViolation>) -> Value {
    violation.map_or(Value::Null, render_child_count_violation)
}

fn render_child_count_violation(violation: ChildCountViolation) -> Value {
    object([
        ("hash", Value::String(violation.hash.to_string())),
        ("level", Value::Number(Number::from(violation.level))),
        (
            "child_count",
            Value::Number(Number::from(
                u64::try_from(violation.child_count).expect("usize child counts always fit in u64"),
            )),
        ),
        (
            "serialized_bytes",
            Value::Number(Number::from(violation.serialized_bytes)),
        ),
    ])
}

fn insert_largest_block(largest_blocks: &mut Vec<LargestBlock>, block: LargestBlock) {
    largest_blocks.push(block);
    largest_blocks.sort_by(|left, right| {
        right
            .serialized_bytes
            .cmp(&left.serialized_bytes)
            .then_with(|| right.level.cmp(&left.level))
            .then_with(|| left.hash.to_string().cmp(&right.hash.to_string()))
    });
    largest_blocks.truncate(16);
}

fn render_largest_block(block: LargestBlock) -> Value {
    object([
        ("hash", Value::String(block.hash.to_string())),
        ("kind", Value::String(block.kind.to_owned())),
        ("level", Value::Number(Number::from(block.level))),
        (
            "serialized_bytes",
            Value::Number(Number::from(block.serialized_bytes)),
        ),
        (
            "child_count",
            block.child_count.map_or(Value::Null, |value| {
                Value::Number(Number::from(
                    u64::try_from(value).expect("usize child counts always fit in u64"),
                ))
            }),
        ),
    ])
}
