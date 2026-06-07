// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::collections::BTreeMap;
use std::io::Cursor;

use ciborium::de::from_reader;
use ciborium::value::Value;
use lexongraph_block::{
    Block, Content, EmbeddingSpec, ExtensionMap, LeafEntry, VERSION_1, build_leaf_block,
};

use crate::MigrationError;

const TOP_LEVEL_VERSION_KEY: u64 = 0;
const TOP_LEVEL_KIND_KEY: u64 = 1;
const TOP_LEVEL_EMBEDDING_SPEC_KEY: u64 = 2;
const TOP_LEVEL_ENTRIES_KEY: u64 = 3;
const TOP_LEVEL_EXT_KEY: u64 = 15;
const EMBEDDING_SPEC_DIMS_KEY: u64 = 0;
const EMBEDDING_SPEC_ENCODING_KEY: u64 = 1;
const LEAF_ENTRY_EMBEDDING_KEY: u64 = 0;
const LEAF_ENTRY_METADATA_KEY: u64 = 1;
const LEAF_ENTRY_CONTENT_KEY: u64 = 2;
const CONTENT_MEDIA_TYPE_KEY: u64 = 0;
const CONTENT_BODY_KEY: u64 = 1;
const KIND_LEAF: &str = "leaf";

pub(crate) fn decode_legacy_leaf(bytes: &[u8]) -> Result<Block, MigrationError> {
    let value = decode_single_cbor_value(bytes)?;
    let mut fields = integer_keyed_map(value, "legacy block")?;
    let version = required_u64_field(&mut fields, TOP_LEVEL_VERSION_KEY, "legacy block")?;
    if version != VERSION_1 {
        return Err(MigrationError::LegacyDecode(format!(
            "unsupported legacy block version {version}"
        )));
    }
    reject_unknown_keys(
        &fields,
        &[
            TOP_LEVEL_KIND_KEY,
            TOP_LEVEL_EMBEDDING_SPEC_KEY,
            TOP_LEVEL_ENTRIES_KEY,
            TOP_LEVEL_EXT_KEY,
        ],
        "legacy block",
    )?;

    let kind = required_text_field(&mut fields, TOP_LEVEL_KIND_KEY, "legacy block")?;
    if kind != KIND_LEAF {
        return Err(MigrationError::LegacyDecode(format!(
            "unsupported legacy block kind {kind:?}; only {KIND_LEAF:?} blocks can be migrated"
        )));
    }

    let embedding_spec = parse_embedding_spec(required_field(
        &mut fields,
        TOP_LEVEL_EMBEDDING_SPEC_KEY,
        "legacy block",
    )?)?;
    let ext = fields
        .remove(&TOP_LEVEL_EXT_KEY)
        .map(expect_arbitrary_map)
        .transpose()?;
    let entries = expect_array(
        required_field(&mut fields, TOP_LEVEL_ENTRIES_KEY, "legacy block")?,
        "legacy leaf entries",
    )?
    .into_iter()
    .map(parse_leaf_entry)
    .collect::<Result<Vec<_>, _>>()?;

    build_leaf_block(version, embedding_spec, entries, ext)
        .map(Block::Leaf)
        .map_err(|error| {
            MigrationError::LegacyDecode(format!(
                "legacy leaf block does not conform to the translated level-0 model: {error}"
            ))
        })
}

fn decode_single_cbor_value(bytes: &[u8]) -> Result<Value, MigrationError> {
    let mut cursor = Cursor::new(bytes);
    let value: Value = from_reader(&mut cursor).map_err(|error| {
        MigrationError::LegacyDecode(format!("malformed source block CBOR: {error}"))
    })?;
    if cursor.position() != bytes.len() as u64 {
        return Err(MigrationError::LegacyDecode(
            "trailing bytes after the top-level CBOR value".into(),
        ));
    }
    Ok(value)
}

fn integer_keyed_map(
    value: Value,
    context: &'static str,
) -> Result<BTreeMap<u64, Value>, MigrationError> {
    let entries = match value {
        Value::Map(entries) => entries,
        _ => {
            return Err(MigrationError::LegacyDecode(format!(
                "{context} must be a CBOR map"
            )));
        }
    };

    let mut fields = BTreeMap::new();
    for (key, value) in entries {
        let key = match key {
            Value::Integer(integer) => u64::try_from(integer).map_err(|_| {
                MigrationError::LegacyDecode(format!(
                    "{context} must use nonnegative integer field keys"
                ))
            })?,
            _ => {
                return Err(MigrationError::LegacyDecode(format!(
                    "{context} must use integer field keys"
                )));
            }
        };
        if fields.insert(key, value).is_some() {
            return Err(MigrationError::LegacyDecode(format!(
                "{context} contains duplicate field key {key}"
            )));
        }
    }
    Ok(fields)
}

fn reject_unknown_keys(
    fields: &BTreeMap<u64, Value>,
    allowed: &[u64],
    context: &'static str,
) -> Result<(), MigrationError> {
    for key in fields.keys() {
        if !allowed.contains(key) {
            return Err(MigrationError::LegacyDecode(format!(
                "{context} contains unknown field key {key}"
            )));
        }
    }
    Ok(())
}

fn required_field(
    fields: &mut BTreeMap<u64, Value>,
    key: u64,
    context: &'static str,
) -> Result<Value, MigrationError> {
    fields.remove(&key).ok_or_else(|| {
        MigrationError::LegacyDecode(format!("{context} is missing required field key {key}"))
    })
}

fn required_u64_field(
    fields: &mut BTreeMap<u64, Value>,
    key: u64,
    context: &'static str,
) -> Result<u64, MigrationError> {
    match required_field(fields, key, context)? {
        Value::Integer(value) => u64::try_from(value).map_err(|_| {
            MigrationError::LegacyDecode(format!(
                "{context} field key {key} must be an unsigned integer"
            ))
        }),
        _ => Err(MigrationError::LegacyDecode(format!(
            "{context} field key {key} must be an unsigned integer"
        ))),
    }
}

fn required_text_field(
    fields: &mut BTreeMap<u64, Value>,
    key: u64,
    context: &'static str,
) -> Result<String, MigrationError> {
    match required_field(fields, key, context)? {
        Value::Text(value) => Ok(value),
        _ => Err(MigrationError::LegacyDecode(format!(
            "{context} field key {key} must be text"
        ))),
    }
}

fn required_bytes_field(
    fields: &mut BTreeMap<u64, Value>,
    key: u64,
    context: &'static str,
) -> Result<Vec<u8>, MigrationError> {
    match required_field(fields, key, context)? {
        Value::Bytes(value) => Ok(value),
        _ => Err(MigrationError::LegacyDecode(format!(
            "{context} field key {key} must be bytes"
        ))),
    }
}

fn expect_array(value: Value, context: &'static str) -> Result<Vec<Value>, MigrationError> {
    match value {
        Value::Array(values) => Ok(values),
        _ => Err(MigrationError::LegacyDecode(format!(
            "{context} must be a CBOR array"
        ))),
    }
}

fn expect_arbitrary_map(value: Value) -> Result<ExtensionMap, MigrationError> {
    match value {
        Value::Map(entries) => Ok(entries),
        _ => Err(MigrationError::LegacyDecode(
            "legacy ext must be a CBOR map".into(),
        )),
    }
}

fn parse_embedding_spec(value: Value) -> Result<EmbeddingSpec, MigrationError> {
    let mut fields = integer_keyed_map(value, "legacy embedding_spec")?;
    reject_unknown_keys(
        &fields,
        &[EMBEDDING_SPEC_DIMS_KEY, EMBEDDING_SPEC_ENCODING_KEY],
        "legacy embedding_spec",
    )?;
    Ok(EmbeddingSpec {
        dims: required_u64_field(
            &mut fields,
            EMBEDDING_SPEC_DIMS_KEY,
            "legacy embedding_spec",
        )?,
        encoding: required_text_field(
            &mut fields,
            EMBEDDING_SPEC_ENCODING_KEY,
            "legacy embedding_spec",
        )?,
    })
}

fn parse_leaf_entry(value: Value) -> Result<LeafEntry, MigrationError> {
    let mut fields = integer_keyed_map(value, "legacy leaf entry")?;
    reject_unknown_keys(
        &fields,
        &[
            LEAF_ENTRY_EMBEDDING_KEY,
            LEAF_ENTRY_METADATA_KEY,
            LEAF_ENTRY_CONTENT_KEY,
        ],
        "legacy leaf entry",
    )?;
    Ok(LeafEntry {
        embedding: required_bytes_field(
            &mut fields,
            LEAF_ENTRY_EMBEDDING_KEY,
            "legacy leaf entry",
        )?,
        metadata: expect_arbitrary_map(required_field(
            &mut fields,
            LEAF_ENTRY_METADATA_KEY,
            "legacy leaf entry",
        )?)?,
        content: parse_content(required_field(
            &mut fields,
            LEAF_ENTRY_CONTENT_KEY,
            "legacy leaf entry",
        )?)?,
    })
}

fn parse_content(value: Value) -> Result<Content, MigrationError> {
    let mut fields = integer_keyed_map(value, "legacy content")?;
    reject_unknown_keys(
        &fields,
        &[CONTENT_MEDIA_TYPE_KEY, CONTENT_BODY_KEY],
        "legacy content",
    )?;
    Ok(Content {
        media_type: required_text_field(&mut fields, CONTENT_MEDIA_TYPE_KEY, "legacy content")?,
        body: required_bytes_field(&mut fields, CONTENT_BODY_KEY, "legacy content")?,
    })
}
