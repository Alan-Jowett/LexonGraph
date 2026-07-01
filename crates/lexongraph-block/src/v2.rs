// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::cmp::Ordering;

use ciborium::ser::into_writer;
use ciborium::value::Value;

use crate::{
    BlockError, BlockHash, BranchEntry, EmbeddingSpec, ExtensionMap, LeafEntry, SerializedBlock,
    branch_entry_to_value, canonicalize_metadata, canonicalize_value, compare_branch_entries,
    compute_block_hash, decode_single_cbor_value, embedding_spec_to_value, expect_arbitrary_map,
    expect_array, int_value, integer_keyed_map, leaf_entry_to_value, normalize_optional_map,
    parse_branch_ebcp_descriptor, parse_branch_entry, parse_embedding_spec, parse_leaf_entry,
    reject_unknown_keys, required_field, required_text_field, required_u64_field,
    validate_branch_ebcp_payload_lengths, validate_embedding_spec,
};

pub const VERSION_2: u64 = 2;

const TOP_LEVEL_VERSION_KEY: u64 = 0;
const TOP_LEVEL_TYPE_KEY: u64 = 1;
const TOP_LEVEL_CONTENT_KEY: u64 = 2;

const RESERVED_BRANCH_TYPE: &str = "branch";
const RESERVED_LEAF_TYPE: &str = "leaf";

const CONTENT_LEVEL_KEY: u64 = 1;
const CONTENT_EMBEDDING_SPEC_KEY: u64 = 2;
const CONTENT_ENTRIES_KEY: u64 = 3;
const CONTENT_EXT_KEY: u64 = 15;

#[derive(Clone, Debug, PartialEq)]
pub struct Block {
    pub version: u64,
    pub type_name: String,
    pub content: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedBlock {
    pub block: Block,
    pub hash: BlockHash,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BranchBlock {
    pub version: u64,
    pub type_name: String,
    pub level: u64,
    pub embedding_spec: EmbeddingSpec,
    pub entries: Vec<BranchEntry>,
    pub ext: Option<ExtensionMap>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeafBlock {
    pub version: u64,
    pub type_name: String,
    pub embedding_spec: EmbeddingSpec,
    pub entries: Vec<LeafEntry>,
    pub ext: Option<ExtensionMap>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CustomBlock {
    pub version: u64,
    pub type_name: String,
    pub content: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TypedBlock {
    Branch(BranchBlock),
    Leaf(LeafBlock),
    Custom(CustomBlock),
}

pub fn build_branch_block(
    level: u64,
    embedding_spec: EmbeddingSpec,
    entries: Vec<BranchEntry>,
    ext: Option<ExtensionMap>,
) -> Result<Block, BlockError> {
    normalize_block(Block {
        version: VERSION_2,
        type_name: RESERVED_BRANCH_TYPE.to_string(),
        content: Value::Map(branch_content_entries(level, embedding_spec, entries, ext)),
    })
}

pub fn build_leaf_block(
    embedding_spec: EmbeddingSpec,
    entries: Vec<LeafEntry>,
    ext: Option<ExtensionMap>,
) -> Result<Block, BlockError> {
    normalize_block(Block {
        version: VERSION_2,
        type_name: RESERVED_LEAF_TYPE.to_string(),
        content: Value::Map(leaf_content_entries(embedding_spec, entries, ext)),
    })
}

pub fn build_custom_block(
    type_name: impl Into<String>,
    content: Value,
) -> Result<Block, BlockError> {
    let type_name = type_name.into();
    if is_reserved_type_name(&type_name) {
        return Err(BlockError::NonConforming(
            "custom block type must not use reserved type names `branch` or `leaf`",
        ));
    }

    normalize_block(Block {
        version: VERSION_2,
        type_name,
        content,
    })
}

pub fn serialize_block(block: &Block) -> Result<SerializedBlock, BlockError> {
    let normalized = normalize_block(block.clone())?;
    let value = block_to_value(&normalized)?;
    let mut bytes = Vec::new();
    into_writer(&value, &mut bytes)
        .map_err(|error| BlockError::MalformedCbor(error.to_string()))?;
    let hash = compute_block_hash(&bytes);
    Ok(SerializedBlock { bytes, hash })
}

pub fn deserialize_block(
    bytes: &[u8],
    expected_hash: &BlockHash,
) -> Result<ValidatedBlock, BlockError> {
    let actual_hash = compute_block_hash(bytes);
    if &actual_hash != expected_hash {
        return Err(BlockError::HashMismatch {
            expected: *expected_hash,
            actual: actual_hash,
        });
    }

    let value = decode_single_cbor_value(bytes)?;
    deserialize_block_from_value(value, bytes, actual_hash)
}

pub(crate) fn deserialize_block_from_value(
    value: Value,
    bytes: &[u8],
    actual_hash: BlockHash,
) -> Result<ValidatedBlock, BlockError> {
    let block = parse_block(value)?;
    let serialized = serialize_block(&block)?;
    if serialized.bytes != bytes {
        return Err(BlockError::NonConforming(
            "block bytes are not the canonical encoding of the decoded block",
        ));
    }

    Ok(ValidatedBlock {
        block,
        hash: actual_hash,
    })
}

pub fn into_typed_block(validated: ValidatedBlock) -> Result<TypedBlock, BlockError> {
    classify_block(validated.block)
}

fn normalize_block(mut block: Block) -> Result<Block, BlockError> {
    if block.version != VERSION_2 {
        return Err(BlockError::UnsupportedVersion(block.version));
    }
    if block.type_name.is_empty() {
        return Err(BlockError::NonConforming(
            "version-2 block type must be non-empty",
        ));
    }

    match block.type_name.as_str() {
        RESERVED_BRANCH_TYPE => normalize_branch_block(&mut block)?,
        RESERVED_LEAF_TYPE => normalize_leaf_block(&mut block)?,
        _ => {
            block.content = canonicalize_value(block.content)?;
        }
    }

    Ok(block)
}

fn is_reserved_type_name(type_name: &str) -> bool {
    matches!(type_name, RESERVED_BRANCH_TYPE | RESERVED_LEAF_TYPE)
}

fn normalize_branch_block(block: &mut Block) -> Result<(), BlockError> {
    let mut fields = integer_keyed_map(block.content.clone(), "content")?;
    reject_unknown_keys(
        &fields,
        &[
            CONTENT_LEVEL_KEY,
            CONTENT_EMBEDDING_SPEC_KEY,
            CONTENT_ENTRIES_KEY,
            CONTENT_EXT_KEY,
        ],
        "content",
    )?;
    let level = required_u64_field(&mut fields, CONTENT_LEVEL_KEY, "content")?;
    if level == 0 {
        return Err(BlockError::InvalidBlockLevel(level));
    }
    let embedding_spec = parse_embedding_spec(required_field(
        &mut fields,
        CONTENT_EMBEDDING_SPEC_KEY,
        "content",
    )?)?;
    validate_embedding_spec(&embedding_spec)?;
    let entries = expect_array(
        required_field(&mut fields, CONTENT_ENTRIES_KEY, "content")?,
        "branch entries",
    )?
    .into_iter()
    .map(parse_branch_entry)
    .collect::<Result<Vec<_>, _>>()?;
    let ext = fields
        .remove(&CONTENT_EXT_KEY)
        .map(expect_arbitrary_map)
        .transpose()?;
    let ext = normalize_optional_map(ext)?;
    if crate::is_ebcp_encoding(&embedding_spec.encoding) {
        let descriptor = parse_branch_ebcp_descriptor(&embedding_spec, ext.as_ref())?
            .expect("validated EBCP encodings must yield a descriptor");
        validate_branch_ebcp_payload_lengths(&entries, &embedding_spec, &descriptor)?;
    }
    let mut entries = entries;
    entries.sort_by(compare_branch_entries);
    for pair in entries.windows(2) {
        if compare_branch_entries(&pair[0], &pair[1]) == Ordering::Equal {
            return Err(BlockError::NonConforming(
                "duplicate branch entries with the same (embedding, child) pair are forbidden",
            ));
        }
    }

    block.content = Value::Map(branch_content_entries(level, embedding_spec, entries, ext));
    Ok(())
}

fn normalize_leaf_block(block: &mut Block) -> Result<(), BlockError> {
    let mut fields = integer_keyed_map(block.content.clone(), "content")?;
    reject_unknown_keys(
        &fields,
        &[
            CONTENT_EMBEDDING_SPEC_KEY,
            CONTENT_ENTRIES_KEY,
            CONTENT_EXT_KEY,
        ],
        "content",
    )?;
    let embedding_spec = parse_embedding_spec(required_field(
        &mut fields,
        CONTENT_EMBEDDING_SPEC_KEY,
        "content",
    )?)?;
    validate_embedding_spec(&embedding_spec)?;
    if crate::is_ebcp_encoding(&embedding_spec.encoding) {
        return Err(BlockError::NonConforming(
            "leaf blocks must not use EBCP branch encodings",
        ));
    }
    let mut entries = expect_array(
        required_field(&mut fields, CONTENT_ENTRIES_KEY, "content")?,
        "leaf entries",
    )?
    .into_iter()
    .map(parse_leaf_entry)
    .collect::<Result<Vec<_>, _>>()?;
    if entries.len() != 1 {
        return Err(BlockError::NonConforming(
            "leaf blocks must contain exactly one leaf entry",
        ));
    }
    for entry in &mut entries {
        entry.metadata = canonicalize_metadata(entry.metadata.clone())?;
    }
    let ext = fields
        .remove(&CONTENT_EXT_KEY)
        .map(expect_arbitrary_map)
        .transpose()?;
    let ext = normalize_optional_map(ext)?;
    block.content = Value::Map(leaf_content_entries(embedding_spec, entries, ext));
    Ok(())
}

fn block_to_value(block: &Block) -> Result<Value, BlockError> {
    canonicalize_value(Value::Map(vec![
        (int_value(TOP_LEVEL_VERSION_KEY), int_value(block.version)),
        (
            int_value(TOP_LEVEL_TYPE_KEY),
            Value::Text(block.type_name.clone()),
        ),
        (int_value(TOP_LEVEL_CONTENT_KEY), block.content.clone()),
    ]))
}

fn parse_block(value: Value) -> Result<Block, BlockError> {
    let mut fields = integer_keyed_map(value, "block")?;
    let version = required_u64_field(&mut fields, TOP_LEVEL_VERSION_KEY, "block")?;
    if version != VERSION_2 {
        return Err(BlockError::UnsupportedVersion(version));
    }
    reject_unknown_keys(
        &fields,
        &[TOP_LEVEL_TYPE_KEY, TOP_LEVEL_CONTENT_KEY],
        "block",
    )?;
    let type_name = required_text_field(&mut fields, TOP_LEVEL_TYPE_KEY, "block")?;
    let content = required_field(&mut fields, TOP_LEVEL_CONTENT_KEY, "block")?;
    normalize_block(Block {
        version,
        type_name,
        content,
    })
}

fn branch_content_entries(
    level: u64,
    embedding_spec: EmbeddingSpec,
    entries: Vec<BranchEntry>,
    ext: Option<ExtensionMap>,
) -> Vec<(Value, Value)> {
    let mut fields = vec![
        (int_value(CONTENT_LEVEL_KEY), int_value(level)),
        (
            int_value(CONTENT_EMBEDDING_SPEC_KEY),
            embedding_spec_to_value(&embedding_spec),
        ),
        (
            int_value(CONTENT_ENTRIES_KEY),
            Value::Array(entries.iter().map(branch_entry_to_value).collect()),
        ),
    ];
    if let Some(ext) = ext {
        fields.push((int_value(CONTENT_EXT_KEY), Value::Map(ext)));
    }
    fields
}

fn leaf_content_entries(
    embedding_spec: EmbeddingSpec,
    entries: Vec<LeafEntry>,
    ext: Option<ExtensionMap>,
) -> Vec<(Value, Value)> {
    let mut fields = vec![
        (
            int_value(CONTENT_EMBEDDING_SPEC_KEY),
            embedding_spec_to_value(&embedding_spec),
        ),
        (
            int_value(CONTENT_ENTRIES_KEY),
            Value::Array(entries.iter().map(leaf_entry_to_value).collect()),
        ),
    ];
    if let Some(ext) = ext {
        fields.push((int_value(CONTENT_EXT_KEY), Value::Map(ext)));
    }
    fields
}

fn classify_block(block: Block) -> Result<TypedBlock, BlockError> {
    match block.type_name.as_str() {
        RESERVED_BRANCH_TYPE => {
            let mut fields = integer_keyed_map(block.content.clone(), "content")?;
            let level = required_u64_field(&mut fields, CONTENT_LEVEL_KEY, "content")?;
            let embedding_spec = parse_embedding_spec(required_field(
                &mut fields,
                CONTENT_EMBEDDING_SPEC_KEY,
                "content",
            )?)?;
            let entries = expect_array(
                required_field(&mut fields, CONTENT_ENTRIES_KEY, "content")?,
                "branch entries",
            )?
            .into_iter()
            .map(parse_branch_entry)
            .collect::<Result<Vec<_>, _>>()?;
            let ext = fields
                .remove(&CONTENT_EXT_KEY)
                .map(expect_arbitrary_map)
                .transpose()?;
            Ok(TypedBlock::Branch(BranchBlock {
                version: block.version,
                type_name: block.type_name,
                level,
                embedding_spec,
                entries,
                ext,
            }))
        }
        RESERVED_LEAF_TYPE => {
            let mut fields = integer_keyed_map(block.content.clone(), "content")?;
            let embedding_spec = parse_embedding_spec(required_field(
                &mut fields,
                CONTENT_EMBEDDING_SPEC_KEY,
                "content",
            )?)?;
            let entries = expect_array(
                required_field(&mut fields, CONTENT_ENTRIES_KEY, "content")?,
                "leaf entries",
            )?
            .into_iter()
            .map(parse_leaf_entry)
            .collect::<Result<Vec<_>, _>>()?;
            let ext = fields
                .remove(&CONTENT_EXT_KEY)
                .map(expect_arbitrary_map)
                .transpose()?;
            Ok(TypedBlock::Leaf(LeafBlock {
                version: block.version,
                type_name: block.type_name,
                embedding_spec,
                entries,
                ext,
            }))
        }
        _ => Ok(TypedBlock::Custom(CustomBlock {
            version: block.version,
            type_name: block.type_name,
            content: block.content,
        })),
    }
}
