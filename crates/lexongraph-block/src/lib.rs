use std::cmp::Ordering;
use std::fmt;
use std::io::Cursor;

use ciborium::de::from_reader;
use ciborium::ser::into_writer;
use ciborium::value::{Integer, Value};
use sha2::{Digest, Sha256};

pub const VERSION_1: u64 = 1;
const TOP_LEVEL_VERSION_KEY: u64 = 0;
const TOP_LEVEL_KIND_KEY: u64 = 1;
const TOP_LEVEL_EMBEDDING_SPEC_KEY: u64 = 2;
const TOP_LEVEL_ENTRIES_KEY: u64 = 3;
const TOP_LEVEL_EXT_KEY: u64 = 15;
const EMBEDDING_SPEC_DIMS_KEY: u64 = 0;
const EMBEDDING_SPEC_ENCODING_KEY: u64 = 1;
const BRANCH_ENTRY_EMBEDDING_KEY: u64 = 0;
const BRANCH_ENTRY_CHILD_KEY: u64 = 1;
const LEAF_ENTRY_EMBEDDING_KEY: u64 = 0;
const LEAF_ENTRY_METADATA_KEY: u64 = 1;
const LEAF_ENTRY_CONTENT_KEY: u64 = 2;
const CONTENT_MEDIA_TYPE_KEY: u64 = 0;
const CONTENT_BODY_KEY: u64 = 1;
const KIND_BRANCH: &str = "branch";
const KIND_LEAF: &str = "leaf";

pub type Metadata = Vec<(Value, Value)>;
pub type ExtensionMap = Vec<(Value, Value)>;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockHash([u8; 32]);

impl BlockHash {
    pub const LEN: usize = 32;

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl TryFrom<&[u8]> for BlockHash {
    type Error = BlockError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; 32] = value
            .try_into()
            .map_err(|_| BlockError::InvalidEntryShape("block hashes must be 32 bytes"))?;
        Ok(Self(bytes))
    }
}

impl From<[u8; 32]> for BlockHash {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl fmt::Debug for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockHash({self})")
    }
}

impl fmt::Display for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingSpec {
    pub dims: u64,
    pub encoding: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchEntry {
    pub embedding: Vec<u8>,
    pub child: BlockHash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Content {
    pub media_type: String,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeafEntry {
    pub embedding: Vec<u8>,
    pub metadata: Metadata,
    pub content: Content,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BranchBlock {
    pub version: u64,
    pub embedding_spec: EmbeddingSpec,
    pub entries: Vec<BranchEntry>,
    pub ext: Option<ExtensionMap>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeafBlock {
    pub version: u64,
    pub embedding_spec: EmbeddingSpec,
    pub entries: Vec<LeafEntry>,
    pub ext: Option<ExtensionMap>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Block {
    Branch(BranchBlock),
    Leaf(LeafBlock),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SerializedBlock {
    pub bytes: Vec<u8>,
    pub hash: BlockHash,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedBlock {
    pub block: Block,
    pub hash: BlockHash,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BlockMetadata {
    pub version: u64,
    pub embedding_spec: EmbeddingSpec,
    pub ext: Option<ExtensionMap>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TypedEntries {
    Branch(BlockMetadata, Vec<BranchEntry>),
    Leaf(BlockMetadata, Vec<LeafEntry>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockError {
    HashMismatch {
        expected: BlockHash,
        actual: BlockHash,
    },
    MalformedCbor(String),
    UnsupportedVersion(u64),
    InvalidFieldKey {
        context: &'static str,
    },
    MissingField {
        context: &'static str,
        key: u64,
    },
    InvalidBlockKind(String),
    InvalidEntryShape(&'static str),
    NonConforming(&'static str),
    UnsupportedValue(&'static str),
}

impl fmt::Display for BlockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HashMismatch { expected, actual } => {
                write!(f, "block hash mismatch: expected {expected}, got {actual}")
            }
            Self::MalformedCbor(message) => write!(f, "malformed CBOR: {message}"),
            Self::UnsupportedVersion(version) => write!(f, "unsupported block version {version}"),
            Self::InvalidFieldKey { context } => {
                write!(f, "invalid field-key usage while decoding {context}")
            }
            Self::MissingField { context, key } => {
                write!(f, "missing required field {key} in {context}")
            }
            Self::InvalidBlockKind(kind) => write!(f, "invalid block kind {kind:?}"),
            Self::InvalidEntryShape(message) => write!(f, "{message}"),
            Self::NonConforming(message) => write!(f, "{message}"),
            Self::UnsupportedValue(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for BlockError {}

pub fn build_branch_block(
    version: u64,
    embedding_spec: EmbeddingSpec,
    entries: Vec<BranchEntry>,
    ext: Option<ExtensionMap>,
) -> Result<BranchBlock, BlockError> {
    match normalize_block(Block::Branch(BranchBlock {
        version,
        embedding_spec,
        entries,
        ext,
    }))? {
        Block::Branch(block) => Ok(block),
        Block::Leaf(_) => unreachable!("normalizing a branch block must return a branch block"),
    }
}

pub fn build_leaf_block(
    version: u64,
    embedding_spec: EmbeddingSpec,
    entries: Vec<LeafEntry>,
    ext: Option<ExtensionMap>,
) -> Result<LeafBlock, BlockError> {
    match normalize_block(Block::Leaf(LeafBlock {
        version,
        embedding_spec,
        entries,
        ext,
    }))? {
        Block::Leaf(block) => Ok(block),
        Block::Branch(_) => unreachable!("normalizing a leaf block must return a leaf block"),
    }
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

pub fn compute_block_hash(bytes: &[u8]) -> BlockHash {
    let hash = Sha256::digest(bytes);
    let mut bytes_out = [0_u8; 32];
    bytes_out.copy_from_slice(&hash);
    BlockHash(bytes_out)
}

pub fn into_entries(validated: ValidatedBlock) -> TypedEntries {
    match validated.block {
        Block::Branch(block) => TypedEntries::Branch(
            BlockMetadata {
                version: block.version,
                embedding_spec: block.embedding_spec,
                ext: block.ext,
            },
            block.entries,
        ),
        Block::Leaf(block) => TypedEntries::Leaf(
            BlockMetadata {
                version: block.version,
                embedding_spec: block.embedding_spec,
                ext: block.ext,
            },
            block.entries,
        ),
    }
}

fn normalize_block(block: Block) -> Result<Block, BlockError> {
    match block {
        Block::Branch(mut block) => {
            validate_version(block.version)?;
            validate_embedding_spec(&block.embedding_spec)?;
            block.ext = normalize_optional_map(block.ext)?;
            block.entries.sort_by(compare_branch_entries);
            for pair in block.entries.windows(2) {
                if compare_branch_entries(&pair[0], &pair[1]) == Ordering::Equal {
                    return Err(BlockError::NonConforming(
                        "duplicate branch entries with the same (embedding, child) pair are forbidden",
                    ));
                }
            }
            Ok(Block::Branch(block))
        }
        Block::Leaf(mut block) => {
            validate_version(block.version)?;
            validate_embedding_spec(&block.embedding_spec)?;
            block.ext = normalize_optional_map(block.ext)?;
            if block.entries.len() != 1 {
                return Err(BlockError::NonConforming(
                    "leaf blocks must contain exactly one leaf entry",
                ));
            }
            for entry in &mut block.entries {
                entry.metadata = normalize_map(entry.metadata.clone())?;
            }
            Ok(Block::Leaf(block))
        }
    }
}

fn normalize_optional_map(value: Option<ExtensionMap>) -> Result<Option<ExtensionMap>, BlockError> {
    value.map(normalize_map).transpose()
}

fn normalize_map(entries: Vec<(Value, Value)>) -> Result<Vec<(Value, Value)>, BlockError> {
    match canonicalize_value(Value::Map(entries))? {
        Value::Map(entries) => Ok(entries),
        _ => unreachable!("canonicalizing a map must return a map"),
    }
}

fn canonicalize_value(value: Value) -> Result<Value, BlockError> {
    match value {
        Value::Integer(_)
        | Value::Bytes(_)
        | Value::Float(_)
        | Value::Text(_)
        | Value::Bool(_)
        | Value::Null => Ok(value),
        Value::Tag(tag, nested) => Ok(Value::Tag(tag, Box::new(canonicalize_value(*nested)?))),
        Value::Array(values) => Ok(Value::Array(
            values
                .into_iter()
                .map(canonicalize_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Value::Map(entries) => {
            let mut normalized = entries
                .into_iter()
                .map(|(key, value)| Ok((canonicalize_value(key)?, canonicalize_value(value)?)))
                .collect::<Result<Vec<_>, BlockError>>()?;
            normalized
                .sort_by(|(left_key, _), (right_key, _)| canonical_value_cmp(left_key, right_key));

            for pair in normalized.windows(2) {
                if canonical_value_cmp(&pair[0].0, &pair[1].0) == Ordering::Equal {
                    return Err(BlockError::NonConforming(
                        "duplicate map keys are not permitted in canonical blocks",
                    ));
                }
            }

            Ok(Value::Map(normalized))
        }
        _ => Err(BlockError::UnsupportedValue(
            "unsupported CBOR value encountered during canonicalization",
        )),
    }
}

fn canonical_value_cmp(left: &Value, right: &Value) -> Ordering {
    let left_bytes = encoded_value_bytes(left);
    let right_bytes = encoded_value_bytes(right);
    left_bytes
        .len()
        .cmp(&right_bytes.len())
        .then_with(|| left_bytes.cmp(&right_bytes))
}

fn encoded_value_bytes(value: &Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    into_writer(value, &mut bytes).expect("serializing a Value to bytes must succeed");
    bytes
}

fn compare_branch_entries(left: &BranchEntry, right: &BranchEntry) -> Ordering {
    left.embedding
        .cmp(&right.embedding)
        .then_with(|| left.child.as_bytes().cmp(right.child.as_bytes()))
}

fn validate_version(version: u64) -> Result<(), BlockError> {
    if version == VERSION_1 {
        Ok(())
    } else {
        Err(BlockError::UnsupportedVersion(version))
    }
}

fn validate_embedding_spec(spec: &EmbeddingSpec) -> Result<(), BlockError> {
    if matches!(spec.encoding.as_str(), "f32le" | "f16le" | "i8" | "pq4") {
        Ok(())
    } else {
        Err(BlockError::NonConforming(
            "embedding_spec.encoding is not a supported version-1 encoding",
        ))
    }
}

fn block_to_value(block: &Block) -> Result<Value, BlockError> {
    let mut fields = match block {
        Block::Branch(block) => vec![
            (int_value(TOP_LEVEL_VERSION_KEY), int_value(block.version)),
            (
                int_value(TOP_LEVEL_KIND_KEY),
                Value::Text(KIND_BRANCH.to_string()),
            ),
            (
                int_value(TOP_LEVEL_EMBEDDING_SPEC_KEY),
                embedding_spec_to_value(&block.embedding_spec),
            ),
            (
                int_value(TOP_LEVEL_ENTRIES_KEY),
                Value::Array(
                    block
                        .entries
                        .iter()
                        .map(branch_entry_to_value)
                        .collect::<Vec<_>>(),
                ),
            ),
        ],
        Block::Leaf(block) => vec![
            (int_value(TOP_LEVEL_VERSION_KEY), int_value(block.version)),
            (
                int_value(TOP_LEVEL_KIND_KEY),
                Value::Text(KIND_LEAF.to_string()),
            ),
            (
                int_value(TOP_LEVEL_EMBEDDING_SPEC_KEY),
                embedding_spec_to_value(&block.embedding_spec),
            ),
            (
                int_value(TOP_LEVEL_ENTRIES_KEY),
                Value::Array(
                    block
                        .entries
                        .iter()
                        .map(leaf_entry_to_value)
                        .collect::<Vec<_>>(),
                ),
            ),
        ],
    };

    let ext = match block {
        Block::Branch(block) => block.ext.clone(),
        Block::Leaf(block) => block.ext.clone(),
    };

    if let Some(ext) = ext {
        fields.push((int_value(TOP_LEVEL_EXT_KEY), Value::Map(ext)));
    }

    canonicalize_value(Value::Map(fields))
}

fn embedding_spec_to_value(spec: &EmbeddingSpec) -> Value {
    Value::Map(vec![
        (int_value(EMBEDDING_SPEC_DIMS_KEY), int_value(spec.dims)),
        (
            int_value(EMBEDDING_SPEC_ENCODING_KEY),
            Value::Text(spec.encoding.clone()),
        ),
    ])
}

fn branch_entry_to_value(entry: &BranchEntry) -> Value {
    Value::Map(vec![
        (
            int_value(BRANCH_ENTRY_EMBEDDING_KEY),
            Value::Bytes(entry.embedding.clone()),
        ),
        (
            int_value(BRANCH_ENTRY_CHILD_KEY),
            Value::Bytes(entry.child.as_bytes().to_vec()),
        ),
    ])
}

fn leaf_entry_to_value(entry: &LeafEntry) -> Value {
    Value::Map(vec![
        (
            int_value(LEAF_ENTRY_EMBEDDING_KEY),
            Value::Bytes(entry.embedding.clone()),
        ),
        (
            int_value(LEAF_ENTRY_METADATA_KEY),
            Value::Map(entry.metadata.clone()),
        ),
        (
            int_value(LEAF_ENTRY_CONTENT_KEY),
            content_to_value(&entry.content),
        ),
    ])
}

fn content_to_value(content: &Content) -> Value {
    Value::Map(vec![
        (
            int_value(CONTENT_MEDIA_TYPE_KEY),
            Value::Text(content.media_type.clone()),
        ),
        (
            int_value(CONTENT_BODY_KEY),
            Value::Bytes(content.body.clone()),
        ),
    ])
}

fn parse_block(value: Value) -> Result<Block, BlockError> {
    let mut fields = integer_keyed_map(value, "block")?;
    let version = required_u64_field(&mut fields, TOP_LEVEL_VERSION_KEY, "block")?;
    validate_version(version)?;
    reject_unknown_keys(
        &fields,
        &[
            TOP_LEVEL_KIND_KEY,
            TOP_LEVEL_EMBEDDING_SPEC_KEY,
            TOP_LEVEL_ENTRIES_KEY,
            TOP_LEVEL_EXT_KEY,
        ],
        "block",
    )?;
    let kind = required_text_field(&mut fields, TOP_LEVEL_KIND_KEY, "block")?;
    let embedding_spec = parse_embedding_spec(required_field(
        &mut fields,
        TOP_LEVEL_EMBEDDING_SPEC_KEY,
        "block",
    )?)?;
    let ext = fields
        .remove(&TOP_LEVEL_EXT_KEY)
        .map(expect_arbitrary_map)
        .transpose()?;
    let entries = required_field(&mut fields, TOP_LEVEL_ENTRIES_KEY, "block")?;

    match kind.as_str() {
        KIND_BRANCH => parse_branch_block(version, embedding_spec, entries, ext),
        KIND_LEAF => parse_leaf_block(version, embedding_spec, entries, ext),
        _ => Err(BlockError::InvalidBlockKind(kind)),
    }
}

fn parse_branch_block(
    version: u64,
    embedding_spec: EmbeddingSpec,
    entries: Value,
    ext: Option<ExtensionMap>,
) -> Result<Block, BlockError> {
    let entries = expect_array(entries, "branch entries")?
        .into_iter()
        .map(parse_branch_entry)
        .collect::<Result<Vec<_>, _>>()?;
    let block = build_branch_block(version, embedding_spec, entries, ext)?;
    Ok(Block::Branch(block))
}

fn parse_leaf_block(
    version: u64,
    embedding_spec: EmbeddingSpec,
    entries: Value,
    ext: Option<ExtensionMap>,
) -> Result<Block, BlockError> {
    let entries = expect_array(entries, "leaf entries")?
        .into_iter()
        .map(parse_leaf_entry)
        .collect::<Result<Vec<_>, _>>()?;
    let block = build_leaf_block(version, embedding_spec, entries, ext)?;
    Ok(Block::Leaf(block))
}

fn parse_embedding_spec(value: Value) -> Result<EmbeddingSpec, BlockError> {
    let mut fields = integer_keyed_map(value, "embedding_spec")?;
    reject_unknown_keys(
        &fields,
        &[EMBEDDING_SPEC_DIMS_KEY, EMBEDDING_SPEC_ENCODING_KEY],
        "embedding_spec",
    )?;
    Ok(EmbeddingSpec {
        dims: required_u64_field(&mut fields, EMBEDDING_SPEC_DIMS_KEY, "embedding_spec")?,
        encoding: required_text_field(&mut fields, EMBEDDING_SPEC_ENCODING_KEY, "embedding_spec")?,
    })
}

fn parse_branch_entry(value: Value) -> Result<BranchEntry, BlockError> {
    let mut fields = integer_keyed_map(value, "branch entry")?;
    reject_unknown_keys(
        &fields,
        &[BRANCH_ENTRY_EMBEDDING_KEY, BRANCH_ENTRY_CHILD_KEY],
        "branch entry",
    )?;

    Ok(BranchEntry {
        embedding: required_bytes_field(&mut fields, BRANCH_ENTRY_EMBEDDING_KEY, "branch entry")?,
        child: BlockHash::try_from(
            required_bytes_field(&mut fields, BRANCH_ENTRY_CHILD_KEY, "branch entry")?.as_slice(),
        )?,
    })
}

fn parse_leaf_entry(value: Value) -> Result<LeafEntry, BlockError> {
    let mut fields = integer_keyed_map(value, "leaf entry")?;
    reject_unknown_keys(
        &fields,
        &[
            LEAF_ENTRY_EMBEDDING_KEY,
            LEAF_ENTRY_METADATA_KEY,
            LEAF_ENTRY_CONTENT_KEY,
        ],
        "leaf entry",
    )?;

    Ok(LeafEntry {
        embedding: required_bytes_field(&mut fields, LEAF_ENTRY_EMBEDDING_KEY, "leaf entry")?,
        metadata: expect_arbitrary_map(required_field(
            &mut fields,
            LEAF_ENTRY_METADATA_KEY,
            "leaf entry",
        )?)?,
        content: parse_content(required_field(
            &mut fields,
            LEAF_ENTRY_CONTENT_KEY,
            "leaf entry",
        )?)?,
    })
}

fn parse_content(value: Value) -> Result<Content, BlockError> {
    let mut fields = integer_keyed_map(value, "content")?;
    reject_unknown_keys(
        &fields,
        &[CONTENT_MEDIA_TYPE_KEY, CONTENT_BODY_KEY],
        "content",
    )?;
    Ok(Content {
        media_type: required_text_field(&mut fields, CONTENT_MEDIA_TYPE_KEY, "content")?,
        body: required_bytes_field(&mut fields, CONTENT_BODY_KEY, "content")?,
    })
}

fn decode_single_cbor_value(bytes: &[u8]) -> Result<Value, BlockError> {
    let mut cursor = Cursor::new(bytes);
    let value: Value =
        from_reader(&mut cursor).map_err(|error| BlockError::MalformedCbor(error.to_string()))?;
    if cursor.position() != bytes.len() as u64 {
        return Err(BlockError::MalformedCbor(
            "trailing bytes after the top-level CBOR value".to_string(),
        ));
    }
    Ok(value)
}

fn integer_keyed_map(
    value: Value,
    context: &'static str,
) -> Result<std::collections::BTreeMap<u64, Value>, BlockError> {
    let entries = match value {
        Value::Map(entries) => entries,
        _ => return Err(BlockError::InvalidEntryShape("expected a CBOR map")),
    };

    let mut fields = std::collections::BTreeMap::new();
    for (key, value) in entries {
        let key = match key {
            Value::Integer(integer) => {
                u64::try_from(integer).map_err(|_| BlockError::InvalidFieldKey { context })?
            }
            _ => return Err(BlockError::InvalidFieldKey { context }),
        };
        if fields.insert(key, value).is_some() {
            return Err(BlockError::NonConforming("duplicate field key"));
        }
    }
    Ok(fields)
}

fn expect_arbitrary_map(value: Value) -> Result<ExtensionMap, BlockError> {
    match canonicalize_value(value)? {
        Value::Map(entries) => Ok(entries),
        _ => Err(BlockError::InvalidEntryShape("expected a CBOR map")),
    }
}

fn expect_array(value: Value, message: &'static str) -> Result<Vec<Value>, BlockError> {
    match value {
        Value::Array(values) => Ok(values),
        _ => Err(BlockError::InvalidEntryShape(message)),
    }
}

fn required_field(
    fields: &mut std::collections::BTreeMap<u64, Value>,
    key: u64,
    context: &'static str,
) -> Result<Value, BlockError> {
    fields
        .remove(&key)
        .ok_or(BlockError::MissingField { context, key })
}

fn required_u64_field(
    fields: &mut std::collections::BTreeMap<u64, Value>,
    key: u64,
    context: &'static str,
) -> Result<u64, BlockError> {
    match required_field(fields, key, context)? {
        Value::Integer(value) => {
            u64::try_from(value).map_err(|_| BlockError::InvalidEntryShape("expected a uint"))
        }
        _ => Err(BlockError::InvalidEntryShape("expected a uint")),
    }
}

fn required_text_field(
    fields: &mut std::collections::BTreeMap<u64, Value>,
    key: u64,
    context: &'static str,
) -> Result<String, BlockError> {
    match required_field(fields, key, context)? {
        Value::Text(value) => Ok(value),
        _ => Err(BlockError::InvalidEntryShape("expected a text value")),
    }
}

fn required_bytes_field(
    fields: &mut std::collections::BTreeMap<u64, Value>,
    key: u64,
    context: &'static str,
) -> Result<Vec<u8>, BlockError> {
    match required_field(fields, key, context)? {
        Value::Bytes(value) => Ok(value),
        _ => Err(BlockError::InvalidEntryShape("expected a byte string")),
    }
}

fn reject_unknown_keys(
    fields: &std::collections::BTreeMap<u64, Value>,
    allowed: &[u64],
    context: &'static str,
) -> Result<(), BlockError> {
    if fields.keys().all(|key| allowed.contains(key)) {
        Ok(())
    } else {
        Err(BlockError::InvalidFieldKey { context })
    }
}

fn int_value(value: u64) -> Value {
    Value::Integer(Integer::from(value))
}
