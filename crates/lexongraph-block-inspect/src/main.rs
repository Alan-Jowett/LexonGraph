// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

use ciborium::value::Value as CborValue;
use clap::{Parser, Subcommand};
use lexongraph_block::{
    Block, BlockHash, BranchBlock, BranchEntry, Content, DecodedBlock, EmbeddingSpec, ExtensionMap,
    LeafBlock, LeafEntry, Metadata, deserialize_versioned_block, v2,
};
use lexongraph_block_store::{BlockStore, BlockStoreError, BlockStoreExt};
use lexongraph_block_store_fs::FilesystemBlockStore;
use serde_json::{Map, Number, Value};

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Inspect stored LexonGraph blocks or analyze a rooted block tree"
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
        root_hash: String,
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
    match pollster::block_on(run(cli)) {
        Ok(json) => println!("{json}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

async fn run(cli: Cli) -> Result<String, InspectError> {
    let document = match cli.backend {
        BackendCommand::Fs {
            store_root,
            block_hash,
        } => {
            let block_hash = parse_block_hash(&block_hash)?;
            let store = open_filesystem_store(&store_root)?;
            inspect_store(&store, &block_hash).await?
        }
        BackendCommand::FsTree {
            store_root,
            expected_max_children,
            root_hash,
        } => {
            let root_hash = parse_block_hash(&root_hash)?;
            let store = open_filesystem_store(&store_root)?;
            analyze_tree(&store, &root_hash, expected_max_children).await?
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
        BlockStoreError::DecodeFailure(inner) => {
            InspectError::StoreConstruction(format!("unexpected malformed content: {inner}"))
        }
        BlockStoreError::ContractViolation(inner) => {
            InspectError::StoreConstruction(format!("unexpected contract violation: {inner}"))
        }
    })
}

async fn inspect_store(
    store: &impl BlockStore,
    block_hash: &BlockHash,
) -> Result<Value, InspectError> {
    load_decoded_block(store, block_hash)
        .await
        .and_then(render_inspection_document)
}

async fn load_decoded_block(
    store: &impl BlockStore,
    block_hash: &BlockHash,
) -> Result<DecodedBlock, InspectError> {
    match store.get_decoded(block_hash).await {
        Ok(Some(decoded_block)) => Ok(decoded_block),
        Ok(None) => Err(InspectError::BlockAbsence(*block_hash)),
        Err(BlockStoreError::BackendFailure(message)) => {
            Err(InspectError::BackendRetrieval(message))
        }
        Err(BlockStoreError::DecodeFailure(error)) => match error {
            lexongraph_block::BlockError::HashMismatch { expected, actual } => {
                Err(InspectError::IntegrityMismatch { expected, actual })
            }
            other => Err(InspectError::MalformedContent(other.to_string())),
        },
        Err(BlockStoreError::ContractViolation(error)) => Err(InspectError::InspectionBoundary(
            format!("unexpected block store contract violation: {error}"),
        )),
    }
}

async fn load_decoded_block_bytes(
    store: &impl BlockStore,
    block_hash: &BlockHash,
) -> Result<(DecodedBlock, Vec<u8>), InspectError> {
    match store.get_block_bytes(block_hash).await {
        Ok(Some(bytes)) => {
            let decoded = deserialize_versioned_block(&bytes, block_hash)
                .map_err(inspect_error_from_block_error)?;
            Ok((decoded, bytes))
        }
        Ok(None) => Err(InspectError::BlockAbsence(*block_hash)),
        Err(BlockStoreError::BackendFailure(message)) => {
            Err(InspectError::BackendRetrieval(message))
        }
        Err(BlockStoreError::ContractViolation(error)) => Err(InspectError::InspectionBoundary(
            format!("unexpected block store contract violation: {error}"),
        )),
        Err(BlockStoreError::DecodeFailure(error)) => Err(InspectError::InspectionBoundary(
            format!("unexpected block store decode failure while loading raw bytes: {error}"),
        )),
    }
}

async fn analyze_tree(
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
    let mut max_children_in_block = 0_u64;
    let mut max_serialized_bytes = 0_u64;
    let mut max_child_count_block = None::<BranchChildCountSummary>;
    let mut largest_blocks = Vec::<LargestBlock>::new();
    let mut child_cap_violations = Vec::<BranchChildCountSummary>::new();
    let mut root_level = None::<u64>;

    while let Some(block_hash) = queue.pop_front() {
        if !visited.insert(block_hash) {
            repeated_reference_count += 1;
            continue;
        }

        let (decoded, bytes) = load_decoded_block_bytes(store, &block_hash).await?;
        let traversed = classify_traversed_block(decoded, &block_hash)?;
        let serialized_bytes = u64::try_from(bytes.len()).map_err(|_| {
            InspectError::InspectionBoundary("serialized block is too large".to_owned())
        })?;

        max_serialized_bytes = max_serialized_bytes.max(serialized_bytes);
        insert_largest_block(
            &mut largest_blocks,
            LargestBlock {
                hash: block_hash,
                level: traversed.level(),
                serialized_bytes,
                child_count: traversed.child_count()?,
                kind: traversed.kind(),
            },
        );

        match traversed {
            TraversedBlock::Branch(branch) => {
                if root_level.is_none() {
                    root_level = Some(branch.level);
                }
                branch_block_count += 1;
                max_children_in_block = max_children_in_block.max(branch.child_count);

                if max_child_count_block
                    .as_ref()
                    .is_none_or(|current| branch.child_count > current.child_count)
                {
                    max_child_count_block = Some(BranchChildCountSummary {
                        hash: block_hash,
                        level: branch.level,
                        child_count: branch.child_count,
                        serialized_bytes,
                    });
                }

                if usize::try_from(branch.child_count)
                    .ok()
                    .is_some_and(|count| count > expected_max_children)
                {
                    child_cap_violations.push(BranchChildCountSummary {
                        hash: block_hash,
                        level: branch.level,
                        child_count: branch.child_count,
                        serialized_bytes,
                    });
                }

                level_stats
                    .entry(branch.level)
                    .or_default()
                    .observe_branch(branch.child_count, serialized_bytes);

                for child in branch.children {
                    queue.push_back(child);
                }
            }
            TraversedBlock::Leaf(leaf) => {
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
    let root_level = root_level.ok_or_else(|| {
        InspectError::InspectionBoundary("rooted tree analysis produced no root level".to_owned())
    })?;

    Ok(object([
        ("root_hash", Value::String(root_hash.to_string())),
        ("root_level", Value::Number(Number::from(root_level))),
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
            Value::Number(Number::from(max_children_in_block)),
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
            render_optional_child_count_summary(max_child_count_block),
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

fn render_inspection_document(decoded_block: DecodedBlock) -> Result<Value, InspectError> {
    let (hash, level, block) = match decoded_block {
        DecodedBlock::V1(validated_block) => {
            let hash = validated_block.hash.to_string();
            let (level, block) = match validated_block.block {
                Block::Branch(block) => (block.level, render_branch_block(block)?),
                Block::Leaf(block) => (block.level, render_leaf_block(block)?),
            };
            (hash, level, block)
        }
        DecodedBlock::V2(validated_block) => {
            let hash = validated_block.hash.to_string();
            let (level, block) = match v2::into_typed_block(validated_block)
                .map_err(|error| InspectError::InspectionBoundary(error.to_string()))?
            {
                v2::TypedBlock::Branch(block) => (block.level, render_v2_branch_block(block)?),
                v2::TypedBlock::Leaf(block) => (0, render_v2_leaf_block(block)?),
                v2::TypedBlock::Custom(block) => (0, render_v2_custom_block(block)?),
            };
            (hash, level, block)
        }
    };

    Ok(object([
        ("hash", Value::String(hash)),
        ("level", Value::Number(Number::from(level))),
        ("block", block),
    ]))
}

fn inspect_error_from_block_error(error: lexongraph_block::BlockError) -> InspectError {
    match error {
        lexongraph_block::BlockError::HashMismatch { expected, actual } => {
            InspectError::IntegrityMismatch { expected, actual }
        }
        other => InspectError::MalformedContent(other.to_string()),
    }
}

enum TraversedBlock {
    Branch(TraversedBranch),
    Leaf(TraversedLeaf),
}

struct TraversedBranch {
    level: u64,
    child_count: u64,
    children: Vec<BlockHash>,
}

struct TraversedLeaf {
    level: u64,
}

impl TraversedBlock {
    fn child_count(&self) -> Result<Option<u64>, InspectError> {
        match self {
            Self::Branch(branch) => Ok(Some(branch.child_count)),
            Self::Leaf(_) => Ok(None),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Branch(_) => "branch",
            Self::Leaf(_) => "leaf",
        }
    }

    fn level(&self) -> u64 {
        match self {
            Self::Branch(branch) => branch.level,
            Self::Leaf(leaf) => leaf.level,
        }
    }
}

fn classify_traversed_block(
    decoded: DecodedBlock,
    block_hash: &BlockHash,
) -> Result<TraversedBlock, InspectError> {
    match decoded {
        DecodedBlock::V1(validated) => match validated.block {
            lexongraph_block::Block::Branch(branch) => {
                Ok(TraversedBlock::Branch(TraversedBranch {
                    level: branch.level,
                    child_count: u64::try_from(branch.entries.len()).map_err(|_| {
                        InspectError::InspectionBoundary(
                            "branch child count does not fit in u64".to_owned(),
                        )
                    })?,
                    children: branch
                        .entries
                        .into_iter()
                        .map(|entry| entry.child)
                        .collect(),
                }))
            }
            lexongraph_block::Block::Leaf(leaf) => {
                Ok(TraversedBlock::Leaf(TraversedLeaf { level: leaf.level }))
            }
        },
        DecodedBlock::V2(validated) => match v2::into_typed_block(validated)
            .map_err(|error| InspectError::InspectionBoundary(error.to_string()))?
        {
            v2::TypedBlock::Branch(branch) => Ok(TraversedBlock::Branch(TraversedBranch {
                level: branch.level,
                child_count: u64::try_from(branch.entries.len()).map_err(|_| {
                    InspectError::InspectionBoundary(
                        "branch child count does not fit in u64".to_owned(),
                    )
                })?,
                children: branch
                    .entries
                    .into_iter()
                    .map(|entry| entry.child)
                    .collect(),
            })),
            v2::TypedBlock::Leaf(_) => Ok(TraversedBlock::Leaf(TraversedLeaf { level: 0 })),
            v2::TypedBlock::Custom(block) => Err(InspectError::InspectionBoundary(format!(
                "fs-tree supports only traversable reserved branch/leaf blocks; found version-2 custom block type {} at {block_hash}",
                block.type_name
            ))),
        },
    }
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

fn render_v2_branch_block(block: v2::BranchBlock) -> Result<Value, InspectError> {
    Ok(object([
        ("version", Value::Number(Number::from(block.version))),
        ("type", Value::String(block.type_name)),
        ("level", Value::Number(Number::from(block.level))),
        (
            "embedding_spec",
            render_embedding_spec(block.embedding_spec),
        ),
        (
            "entries",
            Value::Array(block.entries.into_iter().map(render_branch_entry).collect()),
        ),
        ("ext", render_optional_map(block.ext.as_ref())?),
    ]))
}

fn render_v2_leaf_block(block: v2::LeafBlock) -> Result<Value, InspectError> {
    Ok(object([
        ("version", Value::Number(Number::from(block.version))),
        ("type", Value::String(block.type_name)),
        ("level", Value::Number(Number::from(0))),
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
                    .map(render_leaf_entry)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        ),
        ("ext", render_optional_map(block.ext.as_ref())?),
    ]))
}

fn render_v2_custom_block(block: v2::CustomBlock) -> Result<Value, InspectError> {
    Ok(object([
        ("version", Value::Number(Number::from(block.version))),
        ("type", Value::String(block.type_name)),
        ("content", render_cbor_value(&block.content)?),
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
    fn observe_branch(&mut self, child_count: u64, serialized_bytes: u64) {
        self.block_count = self.block_count.saturating_add(1);
        self.branch_block_count = self.branch_block_count.saturating_add(1);
        self.total_children = self.total_children.saturating_add(child_count);
        self.min_children = Some(
            self.min_children
                .map_or(child_count, |current| current.min(child_count)),
        );
        self.max_children = self.max_children.max(child_count);
        self.total_serialized_bytes = self.total_serialized_bytes.saturating_add(serialized_bytes);
        self.max_serialized_bytes = self.max_serialized_bytes.max(serialized_bytes);
    }

    fn observe_leaf(&mut self, serialized_bytes: u64) {
        self.block_count = self.block_count.saturating_add(1);
        self.leaf_block_count = self.leaf_block_count.saturating_add(1);
        self.total_serialized_bytes = self.total_serialized_bytes.saturating_add(serialized_bytes);
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
                if self.branch_block_count == 0 {
                    Value::Null
                } else {
                    Value::Number(Number::from(self.max_children))
                },
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

struct BranchChildCountSummary {
    hash: BlockHash,
    level: u64,
    child_count: u64,
    serialized_bytes: u64,
}

struct LargestBlock {
    hash: BlockHash,
    level: u64,
    serialized_bytes: u64,
    child_count: Option<u64>,
    kind: &'static str,
}

fn render_optional_child_count_summary(summary: Option<BranchChildCountSummary>) -> Value {
    summary.map_or(Value::Null, render_child_count_violation)
}

fn render_child_count_violation(violation: BranchChildCountSummary) -> Value {
    object([
        ("hash", Value::String(violation.hash.to_string())),
        ("level", Value::Number(Number::from(violation.level))),
        (
            "child_count",
            Value::Number(Number::from(violation.child_count)),
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
            .then_with(|| left.hash.as_bytes().cmp(right.hash.as_bytes()))
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
            block
                .child_count
                .map_or(Value::Null, |value| Value::Number(Number::from(value))),
        ),
    ])
}
