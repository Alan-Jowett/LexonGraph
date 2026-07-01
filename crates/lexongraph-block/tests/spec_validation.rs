// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use ciborium::ser::into_writer;
use ciborium::value::{Integer, Value};
use lexongraph_block::{
    Block, BlockError, BlockHash, BranchEntry, Content, DecodedBlock, EbcpDescriptor,
    EbcpQuantization, EbcpRotation, EmbeddingSpec, LeafEntry, TypedEntries, VERSION_1,
    VersionedBlock, build_branch_block, build_leaf_block, compute_block_hash, deserialize_block,
    deserialize_versioned_block, ebcp_extension_map, into_entries, parse_branch_ebcp_descriptor,
    reconstruct_logical_branch_embedding_f32, serialize_block, serialize_versioned_block, v2,
};

#[test]
fn val_001_branch_serialization_is_deterministic() {
    let spec = embedding_spec("f16le");
    let entry_a = branch_entry(vec![0x01, 0x02], [0x11; 32]);
    let entry_b = branch_entry(vec![0x02, 0x03], [0x22; 32]);

    let first = Block::Branch(
        build_branch_block(
            VERSION_1,
            1,
            spec.clone(),
            vec![entry_b.clone(), entry_a.clone()],
            None,
        )
        .unwrap(),
    );
    let second = Block::Branch(
        build_branch_block(VERSION_1, 1, spec, vec![entry_a, entry_b], None).unwrap(),
    );

    let first_serialized = serialize_block(&first).unwrap();
    let second_serialized = serialize_block(&second).unwrap();

    assert_eq!(first_serialized.bytes, second_serialized.bytes);
    assert_eq!(first_serialized.hash, second_serialized.hash);
}

#[test]
fn val_002_leaf_serialization_is_deterministic() {
    let first = Block::Leaf(
        build_leaf_block(
            VERSION_1,
            embedding_spec("f32le"),
            vec![leaf_entry(
                vec![0xaa, 0xbb],
                vec![
                    (Value::Text("message_id".into()), Value::Text("<1>".into())),
                    (
                        Value::Text("source".into()),
                        Value::Text("ietf-mail".into()),
                    ),
                ],
            )],
            None,
        )
        .unwrap(),
    );
    let second = Block::Leaf(
        build_leaf_block(
            VERSION_1,
            embedding_spec("f32le"),
            vec![leaf_entry(
                vec![0xaa, 0xbb],
                vec![
                    (
                        Value::Text("source".into()),
                        Value::Text("ietf-mail".into()),
                    ),
                    (Value::Text("message_id".into()), Value::Text("<1>".into())),
                ],
            )],
            None,
        )
        .unwrap(),
    );

    let first_serialized = serialize_block(&first).unwrap();
    let second_serialized = serialize_block(&second).unwrap();

    assert_eq!(first_serialized.bytes, second_serialized.bytes);
    assert_eq!(first_serialized.hash, second_serialized.hash);
}

#[test]
fn val_003_deserialize_with_matching_hash_succeeds() {
    let block = sample_branch_block();
    let serialized = serialize_block(&block).unwrap();

    let validated = deserialize_block(&serialized.bytes, &serialized.hash).unwrap();

    assert_eq!(validated.hash, serialized.hash);
    assert_eq!(validated.block, block);
}

#[test]
fn val_004_hash_mismatch_fails_before_acceptance() {
    let serialized = serialize_block(&sample_branch_block()).unwrap();
    let mut mismatched = serialized.hash.into_bytes();
    mismatched[0] ^= 0xff;
    let expected = BlockHash::from_bytes(mismatched);

    let error = deserialize_block(&serialized.bytes, &expected).unwrap_err();

    assert!(matches!(error, BlockError::HashMismatch { .. }));
}

#[test]
fn val_005_unsorted_branch_entries_are_rejected() {
    let bytes = raw_branch_bytes(vec![
        raw_branch_entry(vec![0x02], [0x22; 32]),
        raw_branch_entry(vec![0x01], [0x11; 32]),
    ]);
    let hash = compute_block_hash(&bytes);

    let error = deserialize_block(&bytes, &hash).unwrap_err();

    assert!(matches!(error, BlockError::NonConforming(_)));
}

#[test]
fn val_006_duplicate_branch_entries_are_rejected() {
    let bytes = raw_branch_bytes(vec![
        raw_branch_entry(vec![0x01], [0x11; 32]),
        raw_branch_entry(vec![0x01], [0x11; 32]),
    ]);
    let hash = compute_block_hash(&bytes);

    let error = deserialize_block(&bytes, &hash).unwrap_err();

    assert!(matches!(error, BlockError::NonConforming(_)));
}

#[test]
fn val_007_leaf_blocks_with_zero_entries_are_rejected() {
    let bytes = raw_leaf_bytes(vec![]);
    let hash = compute_block_hash(&bytes);

    let error = deserialize_block(&bytes, &hash).unwrap_err();

    assert!(matches!(error, BlockError::NonConforming(_)));
}

#[test]
fn val_007_leaf_blocks_with_multiple_entries_are_rejected() {
    let bytes = raw_leaf_bytes(vec![
        raw_leaf_entry(vec![0x01], vec![]),
        raw_leaf_entry(vec![0x02], vec![]),
    ]);
    let hash = compute_block_hash(&bytes);

    let error = deserialize_block(&bytes, &hash).unwrap_err();

    assert!(matches!(error, BlockError::NonConforming(_)));
}

#[test]
fn val_008_missing_required_level_specific_fields_are_rejected() {
    let bytes = encode_value(Value::Map(vec![
        (int_value(0), int_value(VERSION_1)),
        (int_value(1), int_value(1)),
        (
            int_value(2),
            Value::Map(vec![
                (int_value(0), int_value(2)),
                (int_value(1), Value::Text("f16le".into())),
            ]),
        ),
        (
            int_value(3),
            Value::Array(vec![Value::Map(vec![(
                int_value(0),
                Value::Bytes(vec![0x01]),
            )])]),
        ),
    ]));
    let hash = compute_block_hash(&bytes);

    let error = deserialize_block(&bytes, &hash).unwrap_err();

    assert!(matches!(error, BlockError::MissingField { .. }));
}

#[test]
fn val_009_unknown_top_level_fields_outside_ext_are_rejected() {
    let bytes = encode_value(Value::Map(vec![
        (int_value(0), int_value(VERSION_1)),
        (int_value(1), int_value(1)),
        (
            int_value(2),
            Value::Map(vec![
                (int_value(0), int_value(2)),
                (int_value(1), Value::Text("f16le".into())),
            ]),
        ),
        (int_value(3), Value::Array(vec![])),
        (int_value(4), Value::Text("unknown".into())),
    ]));
    let hash = compute_block_hash(&bytes);

    let error = deserialize_block(&bytes, &hash).unwrap_err();

    assert!(matches!(error, BlockError::InvalidFieldKey { .. }));
}

#[test]
fn val_010_unknown_fields_inside_ext_are_accepted() {
    let block = Block::Branch(
        build_branch_block(
            VERSION_1,
            1,
            embedding_spec("f16le"),
            vec![branch_entry(vec![0x01], [0x11; 32])],
            Some(vec![(
                Value::Text("future-field".into()),
                Value::Integer(Integer::from(7_u8)),
            )]),
        )
        .unwrap(),
    );
    let serialized = serialize_block(&block).unwrap();

    let validated = deserialize_block(&serialized.bytes, &serialized.hash).unwrap();

    assert_eq!(validated.block, block);
}

#[test]
fn val_011_textual_field_names_are_rejected() {
    let bytes = encode_value(Value::Map(vec![
        (Value::Text("version".into()), int_value(VERSION_1)),
        (Value::Text("level".into()), int_value(1)),
        (
            Value::Text("embedding_spec".into()),
            Value::Map(vec![
                (Value::Text("dims".into()), int_value(2)),
                (Value::Text("encoding".into()), Value::Text("f16le".into())),
            ]),
        ),
        (Value::Text("entries".into()), Value::Array(vec![])),
    ]));
    let hash = compute_block_hash(&bytes);

    let error = deserialize_block(&bytes, &hash).unwrap_err();

    assert!(matches!(error, BlockError::InvalidFieldKey { .. }));
}

#[test]
fn val_012_round_trip_preserves_block_meaning_and_hash() {
    let block = sample_leaf_block();
    let serialized = serialize_block(&block).unwrap();

    let validated = deserialize_block(&serialized.bytes, &serialized.hash).unwrap();

    assert_eq!(validated.hash, serialized.hash);
    assert_eq!(validated.block, block);
}

#[test]
fn val_013_distinct_embedding_encodings_change_bytes_and_hash() {
    let first = Block::Branch(
        build_branch_block(
            VERSION_1,
            1,
            embedding_spec("f16le"),
            vec![branch_entry(vec![0x01], [0x11; 32])],
            None,
        )
        .unwrap(),
    );
    let second = Block::Branch(
        build_branch_block(
            VERSION_1,
            1,
            embedding_spec("i8"),
            vec![branch_entry(vec![0x01], [0x11; 32])],
            None,
        )
        .unwrap(),
    );

    let first_serialized = serialize_block(&first).unwrap();
    let second_serialized = serialize_block(&second).unwrap();

    assert_ne!(first_serialized.bytes, second_serialized.bytes);
    assert_ne!(first_serialized.hash, second_serialized.hash);
}

#[test]
fn unknown_embedding_encodings_are_rejected() {
    let error = build_branch_block(
        VERSION_1,
        1,
        embedding_spec("unknown"),
        vec![branch_entry(vec![0x01], [0x11; 32])],
        None,
    )
    .unwrap_err();

    assert!(matches!(error, BlockError::NonConforming(_)));
}

#[test]
fn val_014_validated_branch_blocks_decompose_to_typed_entries() {
    let serialized = serialize_block(&sample_branch_block()).unwrap();
    let validated = deserialize_block(&serialized.bytes, &serialized.hash).unwrap();

    match into_entries(validated) {
        TypedEntries::Branch(metadata, entries) => {
            assert_eq!(metadata.version, VERSION_1);
            assert_eq!(metadata.level, 1);
            assert_eq!(metadata.embedding_spec.encoding, "f16le");
            assert_eq!(entries.len(), 2);
        }
        TypedEntries::Leaf(_, _) => panic!("expected a branch block"),
    }
}

#[test]
fn val_015_indexing_consumers_can_construct_protocol_conforming_blocks() {
    let branch = build_branch_block(
        VERSION_1,
        1,
        embedding_spec("f16le"),
        vec![
            branch_entry(vec![0x03], [0x33; 32]),
            branch_entry(vec![0x01], [0x11; 32]),
        ],
        None,
    )
    .unwrap();
    let serialized = serialize_block(&Block::Branch(branch)).unwrap();

    let validated = deserialize_block(&serialized.bytes, &serialized.hash).unwrap();

    assert!(matches!(validated.block, Block::Branch(_)));
}

#[test]
fn val_016_unsupported_versions_and_invalid_version_types_are_rejected() {
    let future_version = encode_value(Value::Map(vec![
        (int_value(0), int_value(2)),
        (int_value(1), int_value(1)),
        (
            int_value(2),
            Value::Map(vec![
                (int_value(0), int_value(2)),
                (int_value(1), Value::Text("f16le".into())),
            ]),
        ),
        (int_value(3), Value::Array(vec![])),
    ]));
    let future_hash = compute_block_hash(&future_version);
    let future_error = deserialize_block(&future_version, &future_hash).unwrap_err();
    assert!(matches!(future_error, BlockError::UnsupportedVersion(2)));

    let future_version_with_new_field = encode_value(Value::Map(vec![
        (int_value(0), int_value(2)),
        (int_value(1), int_value(1)),
        (
            int_value(2),
            Value::Map(vec![
                (int_value(0), int_value(2)),
                (int_value(1), Value::Text("f16le".into())),
            ]),
        ),
        (int_value(3), Value::Array(vec![])),
        (int_value(4), Value::Text("future-required-field".into())),
    ]));
    let future_new_field_hash = compute_block_hash(&future_version_with_new_field);
    let future_new_field_error =
        deserialize_block(&future_version_with_new_field, &future_new_field_hash).unwrap_err();
    assert!(matches!(
        future_new_field_error,
        BlockError::UnsupportedVersion(2)
    ));

    let wrong_typed_version = encode_value(Value::Map(vec![
        (int_value(0), Value::Text("1".into())),
        (int_value(1), int_value(1)),
        (
            int_value(2),
            Value::Map(vec![
                (int_value(0), int_value(2)),
                (int_value(1), Value::Text("f16le".into())),
            ]),
        ),
        (int_value(3), Value::Array(vec![])),
    ]));
    let typed_hash = compute_block_hash(&wrong_typed_version);
    let typed_error = deserialize_block(&wrong_typed_version, &typed_hash).unwrap_err();
    assert!(matches!(typed_error, BlockError::InvalidEntryShape(_)));
}

#[test]
fn val_017_noncanonical_but_logically_valid_blocks_are_rejected() {
    let bytes = encode_value(Value::Map(vec![
        (int_value(1), int_value(1)),
        (int_value(0), int_value(VERSION_1)),
        (
            int_value(2),
            Value::Map(vec![
                (int_value(0), int_value(2)),
                (int_value(1), Value::Text("f16le".into())),
            ]),
        ),
        (
            int_value(3),
            Value::Array(vec![raw_branch_entry(vec![0x01], [0x11; 32])]),
        ),
    ]));
    let hash = compute_block_hash(&bytes);

    let error = deserialize_block(&bytes, &hash).unwrap_err();

    assert!(matches!(error, BlockError::NonConforming(_)));
}

#[test]
fn val_018_higher_level_branch_round_trip_preserves_level() {
    let block = Block::Branch(
        build_branch_block(
            VERSION_1,
            3,
            embedding_spec("f16le"),
            vec![branch_entry(vec![0x01, 0x02], [0x11; 32])],
            None,
        )
        .unwrap(),
    );
    let serialized = serialize_block(&block).unwrap();

    match into_entries(deserialize_block(&serialized.bytes, &serialized.hash).unwrap()) {
        TypedEntries::Branch(metadata, _) => assert_eq!(metadata.level, 3),
        TypedEntries::Leaf(_, _) => panic!("expected a branch block"),
    }
}

#[test]
fn val_019_invalid_level_encodings_and_level_shape_mismatches_are_rejected() {
    let text_level = encode_value(Value::Map(vec![
        (int_value(0), int_value(VERSION_1)),
        (int_value(1), Value::Text("branch".into())),
        (
            int_value(2),
            Value::Map(vec![
                (int_value(0), int_value(2)),
                (int_value(1), Value::Text("f16le".into())),
            ]),
        ),
        (int_value(3), Value::Array(vec![])),
    ]));
    let text_level_hash = compute_block_hash(&text_level);
    let text_level_error = deserialize_block(&text_level, &text_level_hash).unwrap_err();
    assert!(matches!(text_level_error, BlockError::InvalidEntryShape(_)));

    let zero_level_error = build_branch_block(
        VERSION_1,
        0,
        embedding_spec("f16le"),
        vec![branch_entry(vec![0x01], [0x11; 32])],
        None,
    )
    .unwrap_err();
    assert!(matches!(zero_level_error, BlockError::InvalidBlockLevel(0)));

    let invalid_leaf = Block::Leaf(lexongraph_block::LeafBlock {
        version: VERSION_1,
        level: 1,
        embedding_spec: embedding_spec("f16le"),
        entries: vec![leaf_entry(vec![0xaa, 0xbb], vec![])],
        ext: None,
    });
    let invalid_leaf_error = serialize_block(&invalid_leaf).unwrap_err();
    assert!(matches!(
        invalid_leaf_error,
        BlockError::InvalidBlockLevel(1)
    ));
}

#[test]
fn val_021_ebcp_branch_blocks_round_trip_with_metadata_and_payloads() {
    let descriptor = EbcpDescriptor {
        version: 1,
        logical_embedding_spec: EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        },
        base_centroid: None,
        rotation: Some(EbcpRotation {
            matrix_format: "f32le-row-major".into(),
            matrix: vec![1.0, 0.0, 0.0, 1.0],
        }),
        quantization: None,
    };
    let block = Block::Branch(
        build_branch_block(
            VERSION_1,
            1,
            embedding_spec("pca-rot-f32le"),
            vec![
                branch_entry(f32_payload([1.0, 0.0]), [0x11; 32]),
                branch_entry(f32_payload([0.0, 1.0]), [0x22; 32]),
            ],
            Some(ebcp_extension_map(&descriptor)),
        )
        .unwrap(),
    );
    let serialized = serialize_block(&block).unwrap();
    let validated = deserialize_block(&serialized.bytes, &serialized.hash).unwrap();
    match into_entries(validated) {
        TypedEntries::Branch(metadata, entries) => {
            assert_eq!(metadata.embedding_spec.encoding, "pca-rot-f32le");
            let parsed =
                parse_branch_ebcp_descriptor(&metadata.embedding_spec, metadata.ext.as_ref())
                    .unwrap()
                    .unwrap();
            assert_eq!(parsed, descriptor);
            assert_eq!(entries[0].embedding.len(), 8);
            assert_eq!(entries[1].embedding.len(), 8);
        }
        TypedEntries::Leaf(_, _) => panic!("expected a branch block"),
    }
}

#[test]
fn val_022_ebcp_leaf_or_missing_descriptor_blocks_are_rejected() {
    let missing_descriptor_error = build_branch_block(
        VERSION_1,
        1,
        embedding_spec("pca-rot-f32le"),
        vec![branch_entry(f32_payload([1.0, 0.0]), [0x11; 32])],
        None,
    )
    .unwrap_err();
    assert!(matches!(
        missing_descriptor_error,
        BlockError::NonConforming(_)
    ));

    let invalid_leaf_error = build_leaf_block(
        VERSION_1,
        embedding_spec("pca-rot-f32le"),
        vec![leaf_entry(f32_payload([1.0, 0.0]), vec![])],
        None,
    )
    .unwrap_err();
    assert!(matches!(invalid_leaf_error, BlockError::NonConforming(_)));
}

#[test]
fn val_023_ebcp_blocks_reject_inconsistent_metadata_and_payload_lengths() {
    let invalid_descriptor = EbcpDescriptor {
        version: 1,
        logical_embedding_spec: EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        },
        base_centroid: Some(vec![0.0, 0.0]),
        rotation: Some(EbcpRotation {
            matrix_format: "f32le-row-major".into(),
            matrix: vec![1.0, 0.0, 0.0],
        }),
        quantization: None,
    };
    let invalid_rotation_error = build_branch_block(
        VERSION_1,
        1,
        embedding_spec("pca-rot-delta-f32le"),
        vec![branch_entry(f32_payload([1.0, 0.0]), [0x11; 32])],
        Some(ebcp_extension_map(&invalid_descriptor)),
    )
    .unwrap_err();
    assert!(matches!(
        invalid_rotation_error,
        BlockError::NonConforming(_)
    ));

    let valid_descriptor = EbcpDescriptor {
        version: 1,
        logical_embedding_spec: EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        },
        base_centroid: Some(vec![0.0, 0.0]),
        rotation: Some(EbcpRotation {
            matrix_format: "f32le-row-major".into(),
            matrix: vec![1.0, 0.0, 0.0, 1.0],
        }),
        quantization: None,
    };
    let invalid_payload_error = build_branch_block(
        VERSION_1,
        1,
        embedding_spec("pca-rot-delta-f32le"),
        vec![branch_entry(vec![0x00, 0x01], [0x11; 32])],
        Some(ebcp_extension_map(&valid_descriptor)),
    )
    .unwrap_err();
    assert!(matches!(
        invalid_payload_error,
        BlockError::NonConforming(_)
    ));
}

#[test]
fn val_024_ebcp_quantization_rejects_bit_widths_above_31() {
    let uniform_error = build_branch_block(
        VERSION_1,
        1,
        embedding_spec("pca-rot-delta-uq"),
        vec![branch_entry(vec![0; 8], [0x11; 32])],
        Some(ebcp_extension_map(&EbcpDescriptor {
            version: 1,
            logical_embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            base_centroid: Some(vec![0.0, 0.0]),
            rotation: Some(EbcpRotation {
                matrix_format: "f32le-row-major".into(),
                matrix: vec![1.0, 0.0, 0.0, 1.0],
            }),
            quantization: Some(EbcpQuantization::Uniform {
                bit_width: 32,
                scale_factors: vec![1.0, 1.0],
            }),
        })),
    )
    .unwrap_err();
    assert!(matches!(uniform_error, BlockError::NonConforming(_)));

    let variable_error = build_branch_block(
        VERSION_1,
        1,
        embedding_spec("pca-rot-delta-vbq"),
        vec![branch_entry(vec![0; 5], [0x11; 32])],
        Some(ebcp_extension_map(&EbcpDescriptor {
            version: 1,
            logical_embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            base_centroid: Some(vec![0.0, 0.0]),
            rotation: Some(EbcpRotation {
                matrix_format: "f32le-row-major".into(),
                matrix: vec![1.0, 0.0, 0.0, 1.0],
            }),
            quantization: Some(EbcpQuantization::Variable {
                bit_widths: vec![32, 1],
                scale_factors: vec![1.0, 1.0],
            }),
        })),
    )
    .unwrap_err();
    assert!(matches!(variable_error, BlockError::NonConforming(_)));
}

#[test]
fn val_025_ebcp_quantized_payload_padding_bits_must_be_zero() {
    let descriptor = EbcpDescriptor {
        version: 1,
        logical_embedding_spec: EmbeddingSpec {
            dims: 1,
            encoding: "f32le".into(),
        },
        base_centroid: Some(vec![0.0]),
        rotation: None,
        quantization: Some(EbcpQuantization::Uniform {
            bit_width: 1,
            scale_factors: vec![1.0],
        }),
    };
    let error = build_branch_block(
        VERSION_1,
        1,
        EmbeddingSpec {
            dims: 1,
            encoding: "ambient-delta-uq".into(),
        },
        vec![branch_entry(vec![0b1000_0000], [0x11; 32])],
        Some(ebcp_extension_map(&descriptor)),
    )
    .unwrap_err();
    assert!(matches!(error, BlockError::NonConforming(_)));
}

#[test]
fn val_026_ebcp_quantization_rejects_negative_scale_factors() {
    let error = build_branch_block(
        VERSION_1,
        1,
        embedding_spec("pca-rot-delta-uq"),
        vec![branch_entry(vec![0; 1], [0x11; 32])],
        Some(ebcp_extension_map(&EbcpDescriptor {
            version: 1,
            logical_embedding_spec: EmbeddingSpec {
                dims: 1,
                encoding: "f32le".into(),
            },
            base_centroid: Some(vec![0.0]),
            rotation: Some(EbcpRotation {
                matrix_format: "f32le-row-major".into(),
                matrix: vec![1.0],
            }),
            quantization: Some(EbcpQuantization::Uniform {
                bit_width: 1,
                scale_factors: vec![-1.0],
            }),
        })),
    )
    .unwrap_err();
    assert!(matches!(error, BlockError::NonConforming(_)));
}

#[test]
fn val_027_ambient_delta_uq_rejects_rotation_metadata() {
    let error = build_branch_block(
        VERSION_1,
        1,
        EmbeddingSpec {
            dims: 1,
            encoding: "ambient-delta-uq".into(),
        },
        vec![branch_entry(vec![0x80], [0x11; 32])],
        Some(ebcp_extension_map(&EbcpDescriptor {
            version: 1,
            logical_embedding_spec: EmbeddingSpec {
                dims: 1,
                encoding: "f32le".into(),
            },
            base_centroid: Some(vec![0.0]),
            rotation: Some(EbcpRotation {
                matrix_format: "f32le-row-major".into(),
                matrix: vec![1.0],
            }),
            quantization: Some(EbcpQuantization::Uniform {
                bit_width: 8,
                scale_factors: vec![1.0 / 127.0],
            }),
        })),
    )
    .unwrap_err();
    assert!(matches!(error, BlockError::NonConforming(_)));
}

#[test]
fn val_027b_ambient_delta_uq_round_trip_without_rotation_metadata() {
    let descriptor = EbcpDescriptor {
        version: 1,
        logical_embedding_spec: EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        },
        base_centroid: Some(vec![0.5, 0.5]),
        rotation: None,
        quantization: Some(EbcpQuantization::Uniform {
            bit_width: 8,
            scale_factors: vec![1.0 / 127.0, 1.0 / 127.0],
        }),
    };
    let block = Block::Branch(
        build_branch_block(
            VERSION_1,
            1,
            EmbeddingSpec {
                dims: 2,
                encoding: "ambient-delta-uq".into(),
            },
            vec![branch_entry(vec![0xFF, 0x80], [0x11; 32])],
            Some(ebcp_extension_map(&descriptor)),
        )
        .unwrap(),
    );
    let serialized = serialize_block(&block).unwrap();
    let validated = deserialize_block(&serialized.bytes, &serialized.hash).unwrap();
    match into_entries(validated) {
        TypedEntries::Branch(metadata, entries) => {
            assert_eq!(metadata.embedding_spec.encoding, "ambient-delta-uq");
            let parsed =
                parse_branch_ebcp_descriptor(&metadata.embedding_spec, metadata.ext.as_ref())
                    .unwrap()
                    .unwrap();
            assert_eq!(parsed, descriptor);
            assert_eq!(entries[0].embedding, vec![0xFF, 0x80]);
        }
        TypedEntries::Leaf(_, _) => panic!("expected a branch block"),
    }
}

#[test]
fn val_028_branch_embedding_reconstruction_returns_logical_f32_vectors() {
    let f16_values = reconstruct_logical_branch_embedding_f32(
        &[0x00, 0x3C, 0x00, 0xC0],
        &embedding_spec("f16le"),
        None,
    )
    .unwrap();
    assert_eq!(f16_values, vec![1.0, -2.0]);

    let i8_values =
        reconstruct_logical_branch_embedding_f32(&[0x7F, 0x80], &embedding_spec("i8"), None)
            .unwrap();
    assert_eq!(i8_values, vec![127.0, -128.0]);

    let rotated_descriptor = EbcpDescriptor {
        version: 1,
        logical_embedding_spec: EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        },
        base_centroid: Some(vec![0.5, 0.5]),
        rotation: Some(EbcpRotation {
            matrix_format: "f32le-row-major".into(),
            matrix: vec![1.0, 0.0, 0.0, 1.0],
        }),
        quantization: None,
    };
    let rotated_values = reconstruct_logical_branch_embedding_f32(
        &[0x00, 0x00, 0x00, 0x3F, 0x00, 0x00, 0x00, 0xBF],
        &embedding_spec("pca-rot-delta-f32le"),
        Some(&rotated_descriptor),
    )
    .unwrap();
    assert_eq!(rotated_values, vec![1.0, 0.0]);

    let ambient_descriptor = EbcpDescriptor {
        version: 1,
        logical_embedding_spec: EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        },
        base_centroid: Some(vec![0.5, 0.5]),
        rotation: None,
        quantization: Some(EbcpQuantization::Uniform {
            bit_width: 8,
            scale_factors: vec![1.0 / 127.0, 1.0 / 127.0],
        }),
    };
    let ambient_values = reconstruct_logical_branch_embedding_f32(
        &[0xFF, 0x01],
        &embedding_spec("ambient-delta-uq"),
        Some(&ambient_descriptor),
    )
    .unwrap();
    assert_eq!(ambient_values, vec![1.5, -0.5]);
}

#[test]
fn val_029_branch_embedding_reconstruction_fails_explicitly_for_unsupported_or_malformed_inputs() {
    let missing_descriptor = reconstruct_logical_branch_embedding_f32(
        &[0x00, 0x00, 0x80, 0x3F],
        &embedding_spec("pca-rot-f32le"),
        None,
    )
    .unwrap_err();
    assert!(matches!(missing_descriptor, BlockError::NonConforming(_)));

    let bad_f32_payload = reconstruct_logical_branch_embedding_f32(
        &[0x00, 0x00, 0x80],
        &embedding_spec("f32le"),
        None,
    )
    .unwrap_err();
    assert!(matches!(bad_f32_payload, BlockError::InvalidEntryShape(_)));

    let non_finite_f16 =
        reconstruct_logical_branch_embedding_f32(&[0x00, 0x7C], &embedding_spec("f16le"), None)
            .unwrap_err();
    assert!(matches!(non_finite_f16, BlockError::InvalidEntryShape(_)));

    let inconsistent_descriptor = reconstruct_logical_branch_embedding_f32(
        &[0x00, 0x00, 0x80, 0x3F],
        &EmbeddingSpec {
            dims: 1,
            encoding: "pca-rot-f32le".into(),
        },
        Some(&EbcpDescriptor {
            version: 1,
            logical_embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            base_centroid: None,
            rotation: Some(EbcpRotation {
                matrix_format: "f32le-row-major".into(),
                matrix: vec![1.0, 0.0, 0.0, 1.0],
            }),
            quantization: None,
        }),
    )
    .unwrap_err();
    assert!(matches!(
        inconsistent_descriptor,
        BlockError::NonConforming(_)
    ));

    let pq4 = reconstruct_logical_branch_embedding_f32(&[0xAB], &embedding_spec("pq4"), None)
        .unwrap_err();
    assert!(matches!(pq4, BlockError::UnsupportedValue(_)));
}

#[test]
fn val_030_versioned_dispatch_round_trips_reserved_v2_blocks() {
    let leaf = VersionedBlock::V2(
        v2::build_leaf_block(
            EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            vec![leaf_entry(vec![0xaa, 0xbb], vec![])],
            None,
        )
        .unwrap(),
    );
    let serialized_leaf = serialize_versioned_block(&leaf).unwrap();
    match deserialize_versioned_block(&serialized_leaf.bytes, &serialized_leaf.hash).unwrap() {
        DecodedBlock::V2(validated) => match v2::into_typed_block(validated).unwrap() {
            v2::TypedBlock::Leaf(block) => {
                assert_eq!(block.version, v2::VERSION_2);
                assert_eq!(block.type_name, "leaf");
                assert_eq!(block.entries.len(), 1);
            }
            other => panic!("expected v2 leaf, got {other:?}"),
        },
        other => panic!("expected decoded v2 block, got {other:?}"),
    }

    let branch = VersionedBlock::V2(
        v2::build_branch_block(
            3,
            EmbeddingSpec {
                dims: 2,
                encoding: "f16le".into(),
            },
            vec![branch_entry(vec![0x01, 0x02], [0x11; 32])],
            None,
        )
        .unwrap(),
    );
    let serialized_branch = serialize_versioned_block(&branch).unwrap();
    match deserialize_versioned_block(&serialized_branch.bytes, &serialized_branch.hash).unwrap() {
        DecodedBlock::V2(validated) => match v2::into_typed_block(validated).unwrap() {
            v2::TypedBlock::Branch(block) => {
                assert_eq!(block.version, v2::VERSION_2);
                assert_eq!(block.type_name, "branch");
                assert_eq!(block.level, 3);
                assert_eq!(block.entries.len(), 1);
            }
            other => panic!("expected v2 branch, got {other:?}"),
        },
        other => panic!("expected decoded v2 block, got {other:?}"),
    }
}

#[test]
fn val_031_versioned_dispatch_round_trips_v2_custom_blocks() {
    let custom = VersionedBlock::V2(
        v2::build_custom_block(
            "application-metadata",
            Value::Map(vec![
                (int_value(7), Value::Text("payload".into())),
                (
                    Value::Text("nested".into()),
                    Value::Array(vec![Value::Bool(true)]),
                ),
            ]),
        )
        .unwrap(),
    );
    let serialized = serialize_versioned_block(&custom).unwrap();

    match deserialize_versioned_block(&serialized.bytes, &serialized.hash).unwrap() {
        DecodedBlock::V2(validated) => match v2::into_typed_block(validated).unwrap() {
            v2::TypedBlock::Custom(block) => {
                assert_eq!(block.version, v2::VERSION_2);
                assert_eq!(block.type_name, "application-metadata");
                assert!(matches!(block.content, Value::Map(_)));
            }
            other => panic!("expected v2 custom block, got {other:?}"),
        },
        other => panic!("expected decoded v2 block, got {other:?}"),
    }
}

#[test]
fn val_032_versioned_dispatch_never_silently_converts_versions() {
    let v1 = VersionedBlock::V1(sample_leaf_block());
    let serialized_v1 = serialize_versioned_block(&v1).unwrap();
    assert!(matches!(
        deserialize_versioned_block(&serialized_v1.bytes, &serialized_v1.hash).unwrap(),
        DecodedBlock::V1(_)
    ));

    let v2 = VersionedBlock::V2(
        v2::build_leaf_block(
            EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            vec![leaf_entry(vec![0xaa, 0xbb], vec![])],
            None,
        )
        .unwrap(),
    );
    let serialized_v2 = serialize_versioned_block(&v2).unwrap();
    assert!(matches!(
        deserialize_versioned_block(&serialized_v2.bytes, &serialized_v2.hash).unwrap(),
        DecodedBlock::V2(_)
    ));
}

#[test]
fn val_033_reserved_v2_content_reuses_v1_nested_field_keys() {
    let branch = v2::build_branch_block(
        1,
        EmbeddingSpec {
            dims: 2,
            encoding: "f16le".into(),
        },
        vec![branch_entry(vec![0x01, 0x02], [0x11; 32])],
        None,
    )
    .unwrap();
    let branch_serialized = v2::serialize_block(&branch).unwrap();
    let branch_value = decode_value(&branch_serialized.bytes);
    let branch_content = top_level_content_map(branch_value);
    assert_eq!(branch_content[0].0, int_value(1));
    assert_eq!(branch_content[1].0, int_value(2));
    assert_eq!(branch_content[2].0, int_value(3));

    let leaf = v2::build_leaf_block(
        EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        },
        vec![leaf_entry(vec![0xaa, 0xbb], vec![])],
        None,
    )
    .unwrap();
    let leaf_serialized = v2::serialize_block(&leaf).unwrap();
    let leaf_value = decode_value(&leaf_serialized.bytes);
    let leaf_content = top_level_content_map(leaf_value);
    assert_eq!(leaf_content[0].0, int_value(2));
    assert_eq!(leaf_content[1].0, int_value(3));
}

#[test]
fn val_034_build_custom_block_rejects_reserved_type_names() {
    for reserved_type in ["branch", "leaf"] {
        let error = v2::build_custom_block(reserved_type, Value::Map(vec![])).unwrap_err();
        assert_eq!(
            error,
            BlockError::NonConforming(
                "custom block type must not use reserved type names `branch` or `leaf`",
            )
        );
    }
}

fn sample_branch_block() -> Block {
    Block::Branch(
        build_branch_block(
            VERSION_1,
            1,
            embedding_spec("f16le"),
            vec![
                branch_entry(vec![0x02, 0x02], [0x22; 32]),
                branch_entry(vec![0x01, 0x01], [0x11; 32]),
            ],
            None,
        )
        .unwrap(),
    )
}

fn sample_leaf_block() -> Block {
    Block::Leaf(
        build_leaf_block(
            VERSION_1,
            embedding_spec("f32le"),
            vec![leaf_entry(
                vec![0xaa, 0xbb],
                vec![
                    (
                        Value::Text("source".into()),
                        Value::Text("ietf-mail".into()),
                    ),
                    (Value::Text("message_id".into()), Value::Text("<1>".into())),
                ],
            )],
            Some(vec![(Value::Text("preserve".into()), Value::Bool(true))]),
        )
        .unwrap(),
    )
}

fn embedding_spec(encoding: &str) -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: encoding.to_string(),
    }
}

fn branch_entry(embedding: Vec<u8>, child: [u8; 32]) -> BranchEntry {
    BranchEntry {
        embedding,
        child: BlockHash::from_bytes(child),
    }
}

fn f32_payload(values: [f32; 2]) -> Vec<u8> {
    values
        .into_iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

fn leaf_entry(embedding: Vec<u8>, metadata: Vec<(Value, Value)>) -> LeafEntry {
    LeafEntry {
        embedding,
        metadata,
        content: Content {
            media_type: "text/plain".into(),
            body: b"hello".to_vec(),
        },
    }
}

fn raw_branch_bytes(entries: Vec<Value>) -> Vec<u8> {
    encode_value(Value::Map(vec![
        (int_value(0), int_value(VERSION_1)),
        (int_value(1), int_value(1)),
        (
            int_value(2),
            Value::Map(vec![
                (int_value(0), int_value(2)),
                (int_value(1), Value::Text("f16le".into())),
            ]),
        ),
        (int_value(3), Value::Array(entries)),
    ]))
}

fn raw_leaf_bytes(entries: Vec<Value>) -> Vec<u8> {
    encode_value(Value::Map(vec![
        (int_value(0), int_value(VERSION_1)),
        (int_value(1), int_value(0)),
        (
            int_value(2),
            Value::Map(vec![
                (int_value(0), int_value(2)),
                (int_value(1), Value::Text("f16le".into())),
            ]),
        ),
        (int_value(3), Value::Array(entries)),
    ]))
}

fn raw_branch_entry(embedding: Vec<u8>, child: [u8; 32]) -> Value {
    Value::Map(vec![
        (int_value(0), Value::Bytes(embedding)),
        (int_value(1), Value::Bytes(child.to_vec())),
    ])
}

fn raw_leaf_entry(embedding: Vec<u8>, metadata: Vec<(Value, Value)>) -> Value {
    Value::Map(vec![
        (int_value(0), Value::Bytes(embedding)),
        (int_value(1), Value::Map(metadata)),
        (
            int_value(2),
            Value::Map(vec![
                (int_value(0), Value::Text("text/plain".into())),
                (int_value(1), Value::Bytes(b"hello".to_vec())),
            ]),
        ),
    ])
}

fn encode_value(value: Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    into_writer(&value, &mut bytes).unwrap();
    bytes
}

fn decode_value(bytes: &[u8]) -> Value {
    let mut cursor = std::io::Cursor::new(bytes);
    ciborium::de::from_reader(&mut cursor).unwrap()
}

fn top_level_content_map(value: Value) -> Vec<(Value, Value)> {
    let Value::Map(entries) = value else {
        panic!("expected top-level map");
    };
    let (_, content) = entries
        .into_iter()
        .find(|(key, _)| *key == int_value(2))
        .expect("expected content field");
    let Value::Map(content_entries) = content else {
        panic!("expected content map");
    };
    content_entries
}

fn int_value(value: u64) -> Value {
    Value::Integer(Integer::from(value))
}
