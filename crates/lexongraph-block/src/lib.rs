// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::cmp::Ordering;
use std::fmt;
use std::io::Cursor;

use ciborium::de::from_reader;
use ciborium::ser::into_writer;
use ciborium::value::{Integer, Value};
use sha2::{Digest, Sha256};

pub const VERSION_1: u64 = 1;
const TOP_LEVEL_VERSION_KEY: u64 = 0;
const TOP_LEVEL_LEVEL_KEY: u64 = 1;
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

pub type Metadata = Vec<(Value, Value)>;
pub type ExtensionMap = Vec<(Value, Value)>;

const EBCP_EXT_DESCRIPTOR_KEY: u64 = 0;
const EBCP_VERSION_KEY: u64 = 0;
const EBCP_LOGICAL_ENCODING_KEY: u64 = 1;
const EBCP_ORIGINAL_DIMS_KEY: u64 = 2;
const EBCP_BASE_CENTROID_KEY: u64 = 3;
const EBCP_ROTATION_KEY: u64 = 4;
const EBCP_QUANTIZATION_KEY: u64 = 5;
const EBCP_ROTATION_FORMAT_KEY: u64 = 0;
const EBCP_ROTATION_MATRIX_BYTES_KEY: u64 = 1;
const EBCP_QUANTIZATION_MODE_KEY: u64 = 0;
const EBCP_QUANTIZATION_UNIFORM_BIT_WIDTH_KEY: u64 = 1;
const EBCP_QUANTIZATION_BIT_WIDTHS_KEY: u64 = 2;
const EBCP_QUANTIZATION_SCALE_FACTORS_KEY: u64 = 3;
const EBCP_DESCRIPTOR_VERSION_1: u64 = 1;
const EBCP_LOGICAL_ENCODING_F32LE: &str = "f32le";
const EBCP_ROTATION_FORMAT_F32LE_ROW_MAJOR: &str = "f32le-row-major";
const EBCP_QUANTIZATION_MODE_UNIFORM: u64 = 1;
const EBCP_QUANTIZATION_MODE_VARIABLE: u64 = 2;
const EBCP_MAX_SUPPORTED_BIT_WIDTH: u8 = 31;

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

#[derive(Clone, Debug, PartialEq)]
pub struct EbcpRotation {
    pub matrix_format: String,
    pub matrix: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum EbcpQuantization {
    Uniform {
        bit_width: u8,
        scale_factors: Vec<f32>,
    },
    Variable {
        bit_widths: Vec<u8>,
        scale_factors: Vec<f32>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct EbcpDescriptor {
    pub version: u64,
    pub logical_embedding_spec: EmbeddingSpec,
    pub base_centroid: Option<Vec<f32>>,
    pub rotation: Option<EbcpRotation>,
    pub quantization: Option<EbcpQuantization>,
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
    pub level: u64,
    pub embedding_spec: EmbeddingSpec,
    pub entries: Vec<BranchEntry>,
    pub ext: Option<ExtensionMap>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeafBlock {
    pub version: u64,
    pub level: u64,
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
    pub level: u64,
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
    InvalidBlockLevel(u64),
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
            Self::InvalidBlockLevel(level) => write!(f, "invalid block level {level}"),
            Self::InvalidEntryShape(message) => write!(f, "{message}"),
            Self::NonConforming(message) => write!(f, "{message}"),
            Self::UnsupportedValue(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for BlockError {}

pub fn is_ebcp_encoding(encoding: &str) -> bool {
    matches!(
        encoding,
        "pca-rot-f32le"
            | "pca-rot-delta-f32le"
            | "pca-rot-delta-uq"
            | "pca-rot-delta-vbq"
            | "ambient-delta-uq"
    )
}

pub fn parse_branch_ebcp_descriptor(
    embedding_spec: &EmbeddingSpec,
    ext: Option<&ExtensionMap>,
) -> Result<Option<EbcpDescriptor>, BlockError> {
    if !is_ebcp_encoding(&embedding_spec.encoding) {
        return Ok(None);
    }

    let ext = ext.ok_or(BlockError::NonConforming(
        "EBCP branch encodings require ext[0] descriptor metadata",
    ))?;
    let mut ext_fields = integer_keyed_map(Value::Map(ext.to_vec()), "ext")?;
    let descriptor = integer_keyed_map(
        required_field(&mut ext_fields, EBCP_EXT_DESCRIPTOR_KEY, "ext")?,
        "ext[0]",
    )?;
    Ok(Some(parse_ebcp_descriptor_fields(
        embedding_spec,
        descriptor,
    )?))
}

pub fn ebcp_extension_map(descriptor: &EbcpDescriptor) -> ExtensionMap {
    vec![(
        int_value(EBCP_EXT_DESCRIPTOR_KEY),
        Value::Map(ebcp_descriptor_to_entries(descriptor)),
    )]
}

pub fn build_branch_block(
    version: u64,
    level: u64,
    embedding_spec: EmbeddingSpec,
    entries: Vec<BranchEntry>,
    ext: Option<ExtensionMap>,
) -> Result<BranchBlock, BlockError> {
    match normalize_block(Block::Branch(BranchBlock {
        version,
        level,
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
        level: 0,
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
                level: block.level,
                embedding_spec: block.embedding_spec,
                ext: block.ext,
            },
            block.entries,
        ),
        Block::Leaf(block) => TypedEntries::Leaf(
            BlockMetadata {
                version: block.version,
                level: block.level,
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
            if block.level == 0 {
                return Err(BlockError::InvalidBlockLevel(block.level));
            }
            validate_embedding_spec(&block.embedding_spec)?;
            block.ext = normalize_optional_map(block.ext)?;
            if is_ebcp_encoding(&block.embedding_spec.encoding) {
                let descriptor =
                    parse_branch_ebcp_descriptor(&block.embedding_spec, block.ext.as_ref())?
                        .expect("EBCP encodings must return a descriptor after validation");
                validate_branch_ebcp_payload_lengths(
                    &block.entries,
                    &block.embedding_spec,
                    &descriptor,
                )?;
            }
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
            if block.level != 0 {
                return Err(BlockError::InvalidBlockLevel(block.level));
            }
            validate_embedding_spec(&block.embedding_spec)?;
            if is_ebcp_encoding(&block.embedding_spec.encoding) {
                return Err(BlockError::NonConforming(
                    "leaf blocks must not use EBCP branch encodings",
                ));
            }
            block.ext = normalize_optional_map(block.ext)?;
            if block.entries.len() != 1 {
                return Err(BlockError::NonConforming(
                    "leaf blocks must contain exactly one leaf entry",
                ));
            }
            for entry in &mut block.entries {
                entry.metadata = canonicalize_metadata(entry.metadata.clone())?;
            }
            Ok(Block::Leaf(block))
        }
    }
}

fn normalize_optional_map(value: Option<ExtensionMap>) -> Result<Option<ExtensionMap>, BlockError> {
    value.map(normalize_map).transpose()
}

/// Canonicalizes leaf-entry metadata using the same rules enforced during block
/// validation and serialization. Duplicate keys are rejected.
pub fn canonicalize_metadata(metadata: Metadata) -> Result<Metadata, BlockError> {
    normalize_map(metadata)
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
    if matches!(spec.encoding.as_str(), "f32le" | "f16le" | "i8" | "pq4")
        || is_ebcp_encoding(&spec.encoding)
    {
        Ok(())
    } else {
        Err(BlockError::NonConforming(
            "embedding_spec.encoding is not a supported version-1 encoding",
        ))
    }
}

fn validate_branch_ebcp_payload_lengths(
    entries: &[BranchEntry],
    embedding_spec: &EmbeddingSpec,
    descriptor: &EbcpDescriptor,
) -> Result<(), BlockError> {
    let expected_bit_len = ebcp_payload_bit_len(embedding_spec, descriptor)?;
    let expected_len = packed_bit_len_to_byte_len(expected_bit_len)?;
    for entry in entries {
        if entry.embedding.len() != expected_len {
            return Err(BlockError::NonConforming(
                "branch payload bytes are inconsistent with the selected EBCP encoding and metadata",
            ));
        }
        validate_zero_ebcp_padding_bits(entry.embedding.as_slice(), expected_bit_len)?;
    }
    Ok(())
}

fn ebcp_payload_bit_len(
    embedding_spec: &EmbeddingSpec,
    descriptor: &EbcpDescriptor,
) -> Result<usize, BlockError> {
    let dims = usize::try_from(embedding_spec.dims)
        .map_err(|_| BlockError::NonConforming("EBCP dimensionality does not fit in usize"))?;
    match embedding_spec.encoding.as_str() {
        "pca-rot-f32le" | "pca-rot-delta-f32le" => dims
            .checked_mul(std::mem::size_of::<f32>())
            .and_then(|bytes| bytes.checked_mul(8))
            .ok_or(BlockError::NonConforming("EBCP payload length overflowed")),
        "pca-rot-delta-uq" | "ambient-delta-uq" => {
            let EbcpQuantization::Uniform { bit_width, .. } = descriptor
                .quantization
                .as_ref()
                .ok_or(BlockError::NonConforming(
                    "quantized EBCP encodings require quantization metadata",
                ))?
            else {
                return Err(BlockError::NonConforming(
                    "EBCP quantization mode does not match the declared encoding",
                ));
            };
            dims.checked_mul(usize::from(*bit_width))
                .ok_or(BlockError::NonConforming(
                    "EBCP payload bit length overflowed",
                ))
        }
        "pca-rot-delta-vbq" => {
            let EbcpQuantization::Variable { bit_widths, .. } = descriptor
                .quantization
                .as_ref()
                .ok_or(BlockError::NonConforming(
                    "quantized EBCP encodings require quantization metadata",
                ))?
            else {
                return Err(BlockError::NonConforming(
                    "EBCP quantization mode does not match the declared encoding",
                ));
            };
            let total_bits = bit_widths.iter().try_fold(0usize, |sum, width| {
                sum.checked_add(usize::from(*width))
                    .ok_or(BlockError::NonConforming(
                        "EBCP payload bit length overflowed",
                    ))
            })?;
            Ok(total_bits)
        }
        _ => Err(BlockError::NonConforming(
            "EBCP payload validation requires an EBCP embedding encoding",
        )),
    }
}

fn validate_zero_ebcp_padding_bits(
    payload: &[u8],
    payload_bit_len: usize,
) -> Result<(), BlockError> {
    let used_bits_in_last_byte = payload_bit_len % 8;
    if used_bits_in_last_byte == 0 || payload.is_empty() {
        return Ok(());
    }
    let used_mask = ((1u16 << used_bits_in_last_byte) - 1) as u8;
    if payload[payload.len() - 1] & !used_mask != 0 {
        return Err(BlockError::NonConforming(
            "EBCP quantized payload padding bits must be zero",
        ));
    }
    Ok(())
}

fn packed_bit_len_to_byte_len(bit_len: usize) -> Result<usize, BlockError> {
    bit_len
        .checked_add(7)
        .map(|value| value / 8)
        .ok_or(BlockError::NonConforming("EBCP payload length overflowed"))
}

fn parse_ebcp_descriptor_fields(
    embedding_spec: &EmbeddingSpec,
    mut fields: std::collections::BTreeMap<u64, Value>,
) -> Result<EbcpDescriptor, BlockError> {
    let version = required_u64_field(&mut fields, EBCP_VERSION_KEY, "ext[0]")?;
    if version != EBCP_DESCRIPTOR_VERSION_1 {
        return Err(BlockError::NonConforming(
            "EBCP descriptor version is unsupported",
        ));
    }

    let logical_encoding = required_text_field(&mut fields, EBCP_LOGICAL_ENCODING_KEY, "ext[0]")?;
    if logical_encoding != EBCP_LOGICAL_ENCODING_F32LE {
        return Err(BlockError::NonConforming(
            "EBCP logical_encoding must be f32le in this revision",
        ));
    }

    let original_dims = required_u64_field(&mut fields, EBCP_ORIGINAL_DIMS_KEY, "ext[0]")?;
    if original_dims != embedding_spec.dims {
        return Err(BlockError::NonConforming(
            "EBCP original_dims must equal embedding_spec.dims",
        ));
    }
    let dims = usize::try_from(original_dims)
        .map_err(|_| BlockError::NonConforming("EBCP dimensionality does not fit in usize"))?;

    let base_centroid = fields
        .remove(&EBCP_BASE_CENTROID_KEY)
        .map(|value| parse_f32_vector_bytes(value, dims, "ext[0].base_centroid"))
        .transpose()?;
    let rotation = fields
        .remove(&EBCP_ROTATION_KEY)
        .map(|value| parse_ebcp_rotation(value, dims))
        .transpose()?;
    let quantization = fields
        .remove(&EBCP_QUANTIZATION_KEY)
        .map(|value| parse_ebcp_quantization(value, embedding_spec, dims))
        .transpose()?;

    match embedding_spec.encoding.as_str() {
        "pca-rot-f32le" => {
            if rotation.is_none() || base_centroid.is_some() || quantization.is_some() {
                return Err(BlockError::NonConforming(
                    "pca-rot-f32le requires rotation and must not declare base_centroid or quantization metadata",
                ));
            }
        }
        "pca-rot-delta-f32le" => {
            if rotation.is_none() || base_centroid.is_none() || quantization.is_some() {
                return Err(BlockError::NonConforming(
                    "pca-rot-delta-f32le requires rotation, base_centroid, and forbids quantization metadata",
                ));
            }
        }
        "pca-rot-delta-uq" | "pca-rot-delta-vbq"
            if rotation.is_none() || base_centroid.is_none() || quantization.is_none() =>
        {
            return Err(BlockError::NonConforming(
                "rotated quantized EBCP encodings require rotation, base_centroid, and quantization metadata",
            ));
        }
        "ambient-delta-uq" => {
            if rotation.is_some() || base_centroid.is_none() || quantization.is_none() {
                return Err(BlockError::NonConforming(
                    "ambient-delta-uq requires base_centroid and quantization metadata and forbids rotation metadata",
                ));
            }
        }
        _ => {}
    }

    Ok(EbcpDescriptor {
        version,
        logical_embedding_spec: EmbeddingSpec {
            dims: original_dims,
            encoding: logical_encoding,
        },
        base_centroid,
        rotation,
        quantization,
    })
}

fn parse_ebcp_rotation(value: Value, dims: usize) -> Result<EbcpRotation, BlockError> {
    let mut fields = integer_keyed_map(value, "ext[0].rotation")?;
    let matrix_format =
        required_text_field(&mut fields, EBCP_ROTATION_FORMAT_KEY, "ext[0].rotation")?;
    if matrix_format != EBCP_ROTATION_FORMAT_F32LE_ROW_MAJOR {
        return Err(BlockError::NonConforming(
            "EBCP rotation.matrix_format must be f32le-row-major",
        ));
    }
    let expected_len = dims
        .checked_mul(dims)
        .and_then(|count| count.checked_mul(std::mem::size_of::<f32>()))
        .ok_or(BlockError::NonConforming(
            "EBCP rotation matrix length overflowed",
        ))?;
    let matrix = parse_f32_vector_bytes(
        required_field(
            &mut fields,
            EBCP_ROTATION_MATRIX_BYTES_KEY,
            "ext[0].rotation",
        )?,
        expected_len / std::mem::size_of::<f32>(),
        "ext[0].rotation.matrix_bytes",
    )?;
    Ok(EbcpRotation {
        matrix_format,
        matrix,
    })
}

fn parse_ebcp_quantization(
    value: Value,
    embedding_spec: &EmbeddingSpec,
    dims: usize,
) -> Result<EbcpQuantization, BlockError> {
    let mut fields = integer_keyed_map(value, "ext[0].quantization")?;
    let mode = required_u64_field(
        &mut fields,
        EBCP_QUANTIZATION_MODE_KEY,
        "ext[0].quantization",
    )?;
    let scale_factors = parse_f32_vector_bytes(
        required_field(
            &mut fields,
            EBCP_QUANTIZATION_SCALE_FACTORS_KEY,
            "ext[0].quantization",
        )?,
        dims,
        "ext[0].quantization.scale_factors",
    )?;
    if scale_factors
        .iter()
        .any(|scale| !scale.is_finite() || *scale < 0.0)
    {
        return Err(BlockError::NonConforming(
            "EBCP quantization scale factors must be finite and nonnegative",
        ));
    }
    match embedding_spec.encoding.as_str() {
        "pca-rot-delta-uq" | "ambient-delta-uq" => {
            if mode != EBCP_QUANTIZATION_MODE_UNIFORM {
                return Err(BlockError::NonConforming(
                    "uniform quantized EBCP encodings require quantization.mode = 1",
                ));
            }
            let bit_width = required_u64_field(
                &mut fields,
                EBCP_QUANTIZATION_UNIFORM_BIT_WIDTH_KEY,
                "ext[0].quantization",
            )?;
            let bit_width = u8::try_from(bit_width)
                .map_err(|_| BlockError::NonConforming("uniform_bit_width must fit in u8"))?;
            if bit_width == 0 {
                return Err(BlockError::NonConforming(
                    "uniform_bit_width must be at least 1",
                ));
            }
            if bit_width > EBCP_MAX_SUPPORTED_BIT_WIDTH {
                return Err(BlockError::NonConforming(
                    "uniform_bit_width must be at most 31",
                ));
            }
            if fields
                .iter()
                .any(|(key, _)| *key == EBCP_QUANTIZATION_BIT_WIDTHS_KEY)
            {
                return Err(BlockError::NonConforming(
                    "pca-rot-delta-uq must not declare per-dimension bit_widths",
                ));
            }
            Ok(EbcpQuantization::Uniform {
                bit_width,
                scale_factors,
            })
        }
        "pca-rot-delta-vbq" => {
            if mode != EBCP_QUANTIZATION_MODE_VARIABLE {
                return Err(BlockError::NonConforming(
                    "pca-rot-delta-vbq requires quantization.mode = 2",
                ));
            }
            if fields
                .iter()
                .any(|(key, _)| *key == EBCP_QUANTIZATION_UNIFORM_BIT_WIDTH_KEY)
            {
                return Err(BlockError::NonConforming(
                    "pca-rot-delta-vbq must not declare uniform_bit_width",
                ));
            }
            let bit_widths = parse_byte_string(
                required_field(
                    &mut fields,
                    EBCP_QUANTIZATION_BIT_WIDTHS_KEY,
                    "ext[0].quantization",
                )?,
                "ext[0].quantization.bit_widths",
            )?;
            if bit_widths.len() != dims || bit_widths.contains(&0) {
                return Err(BlockError::NonConforming(
                    "EBCP variable bit widths must contain one nonzero byte per dimension",
                ));
            }
            if bit_widths
                .iter()
                .any(|bit_width| *bit_width > EBCP_MAX_SUPPORTED_BIT_WIDTH)
            {
                return Err(BlockError::NonConforming(
                    "EBCP variable bit widths must be at most 31",
                ));
            }
            Ok(EbcpQuantization::Variable {
                bit_widths,
                scale_factors,
            })
        }
        _ => Err(BlockError::NonConforming(
            "EBCP quantization metadata is only valid on quantized encodings",
        )),
    }
}

fn parse_f32_vector_bytes(
    value: Value,
    expected_len: usize,
    context: &'static str,
) -> Result<Vec<f32>, BlockError> {
    let bytes = parse_byte_string(value, context)?;
    let expected_bytes = expected_len
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or(BlockError::NonConforming("EBCP byte length overflowed"))?;
    if bytes.len() != expected_bytes {
        return Err(BlockError::NonConforming(
            "EBCP float payload length does not match the declared dimensionality",
        ));
    }
    bytes
        .chunks_exact(4)
        .map(|chunk| {
            let value = f32::from_le_bytes(chunk.try_into().expect("chunk size is validated"));
            if !value.is_finite() {
                return Err(BlockError::NonConforming(
                    "EBCP float payload must contain only finite f32 values",
                ));
            }
            Ok(value)
        })
        .collect()
}

fn parse_byte_string(value: Value, context: &'static str) -> Result<Vec<u8>, BlockError> {
    match value {
        Value::Bytes(bytes) => Ok(bytes),
        _ => Err(BlockError::InvalidEntryShape({
            let _ = context;
            "expected a byte string value"
        })),
    }
}

fn ebcp_descriptor_to_entries(descriptor: &EbcpDescriptor) -> Vec<(Value, Value)> {
    let mut fields = vec![
        (int_value(EBCP_VERSION_KEY), int_value(descriptor.version)),
        (
            int_value(EBCP_LOGICAL_ENCODING_KEY),
            Value::Text(descriptor.logical_embedding_spec.encoding.clone()),
        ),
        (
            int_value(EBCP_ORIGINAL_DIMS_KEY),
            int_value(descriptor.logical_embedding_spec.dims),
        ),
    ];
    if let Some(rotation) = &descriptor.rotation {
        fields.push((
            int_value(EBCP_ROTATION_KEY),
            Value::Map(ebcp_rotation_to_entries(rotation)),
        ));
    }
    if let Some(base_centroid) = &descriptor.base_centroid {
        fields.push((
            int_value(EBCP_BASE_CENTROID_KEY),
            Value::Bytes(encode_f32_values(base_centroid)),
        ));
    }
    if let Some(quantization) = &descriptor.quantization {
        fields.push((
            int_value(EBCP_QUANTIZATION_KEY),
            Value::Map(ebcp_quantization_to_entries(quantization)),
        ));
    }
    fields
}

fn ebcp_rotation_to_entries(rotation: &EbcpRotation) -> Vec<(Value, Value)> {
    vec![
        (
            int_value(EBCP_ROTATION_FORMAT_KEY),
            Value::Text(rotation.matrix_format.clone()),
        ),
        (
            int_value(EBCP_ROTATION_MATRIX_BYTES_KEY),
            Value::Bytes(encode_f32_values(&rotation.matrix)),
        ),
    ]
}

fn ebcp_quantization_to_entries(quantization: &EbcpQuantization) -> Vec<(Value, Value)> {
    match quantization {
        EbcpQuantization::Uniform {
            bit_width,
            scale_factors,
        } => vec![
            (
                int_value(EBCP_QUANTIZATION_MODE_KEY),
                int_value(EBCP_QUANTIZATION_MODE_UNIFORM),
            ),
            (
                int_value(EBCP_QUANTIZATION_UNIFORM_BIT_WIDTH_KEY),
                int_value(u64::from(*bit_width)),
            ),
            (
                int_value(EBCP_QUANTIZATION_SCALE_FACTORS_KEY),
                Value::Bytes(encode_f32_values(scale_factors)),
            ),
        ],
        EbcpQuantization::Variable {
            bit_widths,
            scale_factors,
        } => vec![
            (
                int_value(EBCP_QUANTIZATION_MODE_KEY),
                int_value(EBCP_QUANTIZATION_MODE_VARIABLE),
            ),
            (
                int_value(EBCP_QUANTIZATION_BIT_WIDTHS_KEY),
                Value::Bytes(bit_widths.clone()),
            ),
            (
                int_value(EBCP_QUANTIZATION_SCALE_FACTORS_KEY),
                Value::Bytes(encode_f32_values(scale_factors)),
            ),
        ],
    }
}

fn encode_f32_values(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn block_to_value(block: &Block) -> Result<Value, BlockError> {
    let mut fields = match block {
        Block::Branch(block) => vec![
            (int_value(TOP_LEVEL_VERSION_KEY), int_value(block.version)),
            (int_value(TOP_LEVEL_LEVEL_KEY), int_value(block.level)),
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
            (int_value(TOP_LEVEL_LEVEL_KEY), int_value(block.level)),
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
            TOP_LEVEL_LEVEL_KEY,
            TOP_LEVEL_EMBEDDING_SPEC_KEY,
            TOP_LEVEL_ENTRIES_KEY,
            TOP_LEVEL_EXT_KEY,
        ],
        "block",
    )?;
    let level = required_u64_field(&mut fields, TOP_LEVEL_LEVEL_KEY, "block")?;
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

    if level == 0 {
        parse_leaf_block(version, level, embedding_spec, entries, ext)
    } else {
        parse_branch_block(version, level, embedding_spec, entries, ext)
    }
}

fn parse_branch_block(
    version: u64,
    level: u64,
    embedding_spec: EmbeddingSpec,
    entries: Value,
    ext: Option<ExtensionMap>,
) -> Result<Block, BlockError> {
    let entries = expect_array(entries, "branch entries")?
        .into_iter()
        .map(parse_branch_entry)
        .collect::<Result<Vec<_>, _>>()?;
    let block = build_branch_block(version, level, embedding_spec, entries, ext)?;
    Ok(Block::Branch(block))
}

fn parse_leaf_block(
    version: u64,
    level: u64,
    embedding_spec: EmbeddingSpec,
    entries: Value,
    ext: Option<ExtensionMap>,
) -> Result<Block, BlockError> {
    let entries = expect_array(entries, "leaf entries")?
        .into_iter()
        .map(parse_leaf_entry)
        .collect::<Result<Vec<_>, _>>()?;
    if level != 0 {
        return Err(BlockError::InvalidBlockLevel(level));
    }
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
