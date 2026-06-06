// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use lexongraph_block::{
    Block, BlockError, BlockHash, BranchEntry, Content, EmbeddingSpec, LeafEntry, VERSION_1,
    build_branch_block, build_leaf_block, deserialize_block, serialize_block,
};
use lexongraph_block_store::{BlockIdIterator, BlockStore, BlockStoreError};
use lexongraph_directional_pca::{
    DirectionalPcaEligibility, DirectionalPcaError, DirectionalPcaGroup, DirectionalPcaLayerInput,
    DirectionalPcaLayerOutcome, DirectionalPcaLayerParams, run_directional_pca_layer,
};
use lexongraph_pca::fit;

#[derive(Default)]
struct MemoryBlockStore {
    blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
}

impl MemoryBlockStore {
    fn insert_block(&self, block: &Block) -> BlockHash {
        let serialized = serialize_block(block).unwrap();
        self.blocks
            .borrow_mut()
            .insert(serialized.hash, serialized.bytes.clone());
        serialized.hash
    }
}

impl BlockStore for MemoryBlockStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        Ok(self.insert_block(block))
    }

    fn get(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        let Some(bytes) = self.blocks.borrow().get(block_id).cloned() else {
            return Ok(None);
        };
        deserialize_block(&bytes, block_id)
            .map(Some)
            .map_err(BlockStoreError::MalformedContent)
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        let block_ids = self.blocks.borrow().keys().copied().collect::<Vec<_>>();
        Ok(Box::new(block_ids.into_iter().map(Ok)))
    }
}

struct BackendFailingStore;

impl BlockStore for BackendFailingStore {
    fn put(&self, _: &Block) -> Result<BlockHash, BlockStoreError> {
        Err(BlockStoreError::BackendFailure(
            "backend unavailable".into(),
        ))
    }

    fn get(
        &self,
        _: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        Err(BlockStoreError::BackendFailure(
            "backend unavailable".into(),
        ))
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        Err(BlockStoreError::BackendFailure(
            "backend unavailable".into(),
        ))
    }
}

struct MalformedGetStore;

impl BlockStore for MalformedGetStore {
    fn put(&self, _: &Block) -> Result<BlockHash, BlockStoreError> {
        Err(BlockStoreError::BackendFailure("not used".into()))
    }

    fn get(
        &self,
        _: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        Err(BlockStoreError::MalformedContent(
            BlockError::MalformedCbor("malformed fixture".into()),
        ))
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        Err(BlockStoreError::BackendFailure("not used".into()))
    }
}

#[test]
fn val_dpca_001_public_surface_exposes_the_single_layer_api_boundary() {
    let store = MemoryBlockStore::default();
    let block_id = store.insert_block(&leaf_block_f32([1.0, 0.0]));
    let input = DirectionalPcaLayerInput {
        block_ids: vec![block_id, block_id],
        params: params(1, 1, 1.0, 1.0, 2, 1, 0.0),
    };

    let outcome = run_directional_pca_layer(&input, &store).unwrap();
    assert!(matches!(
        outcome,
        DirectionalPcaLayerOutcome::Partitioned(_) | DirectionalPcaLayerOutcome::Ineligible(_)
    ));
}

#[test]
fn val_dpca_002_repeated_runs_are_deterministic() {
    let store = MemoryBlockStore::default();
    let block_ids = vec![
        store.insert_block(&leaf_block_f32([0.0, 0.0])),
        store.insert_block(&leaf_block_f32([1.0, 0.0])),
        store.insert_block(&leaf_block_f32([10.0, 0.0])),
        store.insert_block(&leaf_block_f32([11.0, 0.0])),
    ];
    let input = DirectionalPcaLayerInput {
        block_ids,
        params: params(1, 2, 1.0, 1.0, 2, 1, 0.0),
    };

    let first = run_directional_pca_layer(&input, &store).unwrap();
    let second = run_directional_pca_layer(&input, &store).unwrap();
    assert_eq!(first, second);
}

#[test]
fn val_dpca_003_input_order_is_semantically_significant() {
    let store = MemoryBlockStore::default();
    let ids = vec![
        store.insert_block(&leaf_block_f32([0.0, 0.0])),
        store.insert_block(&leaf_block_f32([1.0, 0.0])),
        store.insert_block(&leaf_block_f32([10.0, 0.0])),
        store.insert_block(&leaf_block_f32([11.0, 0.0])),
    ];

    let ordered = DirectionalPcaLayerInput {
        block_ids: ids.clone(),
        params: params(1, 2, 1.0, 1.0, 2, 1, 0.0),
    };
    let reordered = DirectionalPcaLayerInput {
        block_ids: vec![ids[1], ids[0], ids[3], ids[2]],
        params: params(1, 2, 1.0, 1.0, 2, 1, 0.0),
    };

    let first = run_directional_pca_layer(&ordered, &store).unwrap();
    let second = run_directional_pca_layer(&reordered, &store).unwrap();
    assert_ne!(first, second);
}

#[test]
fn val_dpca_004_representative_embeddings_follow_branch_centroid_and_leaf_entry_rules() {
    let store = MemoryBlockStore::default();
    let branch_id = store.insert_block(&branch_block_f32(&[[1.0, 0.0], [3.0, 0.0]]));
    let leaf_id = store.insert_block(&leaf_block_f32([0.0, 4.0]));
    let input = DirectionalPcaLayerInput {
        block_ids: vec![branch_id, leaf_id],
        params: params(1, 2, 1.0, 1.0, 2, 1, 0.0),
    };

    let outcome = run_directional_pca_layer(&input, &store).unwrap();
    let DirectionalPcaLayerOutcome::Partitioned(result) = outcome else {
        panic!("expected eligible partition");
    };

    assert_eq!(
        result.groups,
        vec![
            DirectionalPcaGroup {
                centroid: vec![2.0, 0.0],
                member_block_ids: vec![branch_id],
            },
            DirectionalPcaGroup {
                centroid: vec![0.0, 4.0],
                member_block_ids: vec![leaf_id],
            },
        ]
    );
}

#[test]
fn val_dpca_005_representative_embedding_failures_are_explicit() {
    let missing_store = MemoryBlockStore::default();
    let missing = synthetic_hash(0xAA);
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![missing, missing],
                params: params(1, 1, 1.0, 1.0, 2, 1, 0.0),
            },
            &missing_store
        ),
        Err(DirectionalPcaError::MissingBlock { block_id }) if block_id == missing
    ));

    let any_id = synthetic_hash(0x10);
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![any_id, any_id],
                params: params(1, 1, 1.0, 1.0, 2, 1, 0.0),
            },
            &BackendFailingStore
        ),
        Err(DirectionalPcaError::BlockStore(
            BlockStoreError::BackendFailure(_)
        ))
    ));

    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![any_id, any_id],
                params: params(1, 1, 1.0, 1.0, 2, 1, 0.0),
            },
            &MalformedGetStore
        ),
        Err(DirectionalPcaError::BlockStore(
            BlockStoreError::MalformedContent(_)
        ))
    ));

    let store = MemoryBlockStore::default();
    let empty_branch_id = store.insert_block(&empty_branch_block_f32());
    let leaf_id = store.insert_block(&leaf_block_f32([1.0, 0.0]));
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![empty_branch_id, leaf_id],
                params: params(1, 1, 1.0, 1.0, 2, 1, 0.0),
            },
            &store
        ),
        Err(DirectionalPcaError::EmptyBlockEmbeddings { block_id }) if block_id == empty_branch_id
    ));

    let incompatible_store = MemoryBlockStore::default();
    let f32_id = incompatible_store.insert_block(&leaf_block_f32([1.0, 0.0]));
    let i8_id = incompatible_store.insert_block(&leaf_block_i8([1, 2]));
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![f32_id, i8_id],
                params: params(1, 1, 1.0, 1.0, 2, 1, 0.0),
            },
            &incompatible_store
        ),
        Err(DirectionalPcaError::IncompatibleEmbeddingSpec { block_id, .. }) if block_id == i8_id
    ));

    let invalid_length_store = MemoryBlockStore::default();
    let invalid_length_id = invalid_length_store.insert_block(&leaf_block_raw(
        EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        },
        vec![0x00, 0x00, 0x80, 0x3F],
    ));
    let valid_id = invalid_length_store.insert_block(&leaf_block_f32([1.0, 0.0]));
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![invalid_length_id, valid_id],
                params: params(1, 1, 1.0, 1.0, 2, 1, 0.0),
            },
            &invalid_length_store
        ),
        Err(DirectionalPcaError::InvalidEmbeddingLength {
            expected: 8,
            actual: 4,
            ..
        })
    ));

    let unsupported_store = MemoryBlockStore::default();
    let unsupported_id = unsupported_store.insert_block(&leaf_block_raw(
        EmbeddingSpec {
            dims: 2,
            encoding: "pq4".into(),
        },
        vec![0x00],
    ));
    let supported_id = unsupported_store.insert_block(&leaf_block_f32([1.0, 0.0]));
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![unsupported_id, supported_id],
                params: params(1, 1, 1.0, 1.0, 2, 1, 0.0),
            },
            &unsupported_store
        ),
        Err(DirectionalPcaError::UnsupportedEncoding { .. })
    ));

    let overflow_store = MemoryBlockStore::default();
    let overflow_spec = EmbeddingSpec {
        dims: u64::MAX,
        encoding: "f32le".into(),
    };
    let overflow_a =
        overflow_store.insert_block(&leaf_block_raw(overflow_spec.clone(), Vec::new()));
    let overflow_b = overflow_store.insert_block(&leaf_block_raw(overflow_spec, Vec::new()));
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![overflow_a, overflow_b],
                params: params(1, 1, 1.0, 1.0, 2, 1, 0.0),
            },
            &overflow_store
        ),
        Err(DirectionalPcaError::InvalidNumericState(_))
    ));

    let non_finite_store = MemoryBlockStore::default();
    let nan_id = non_finite_store.insert_block(&leaf_block_raw(
        EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        },
        encode_f32_values(&[f32::NAN, 0.0]),
    ));
    let finite_id = non_finite_store.insert_block(&leaf_block_f32([1.0, 0.0]));
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![nan_id, finite_id],
                params: params(1, 1, 1.0, 1.0, 2, 1, 0.0),
            },
            &non_finite_store
        ),
        Err(DirectionalPcaError::NonFiniteValue { block_id, .. }) if block_id == nan_id
    ));
}

#[test]
fn val_dpca_006_invalid_parameter_bounds_fail_explicitly() {
    let store = MemoryBlockStore::default();
    let block_id = store.insert_block(&leaf_block_f32([1.0, 0.0]));

    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![block_id, block_id],
                params: params(0, 1, 1.0, 1.0, 2, 1, 0.0),
            },
            &store
        ),
        Err(DirectionalPcaError::InvalidRetainedDimension { requested: 0, .. })
    ));
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![block_id, block_id],
                params: params(1, 0, 1.0, 1.0, 2, 1, 0.0),
            },
            &store
        ),
        Err(DirectionalPcaError::InvalidAxisResolutionBudget {
            axis_resolution_budget: 0,
            ..
        })
    ));
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![block_id, block_id],
                params: params(1, 1, 1.0, 0.0, 2, 1, 0.0),
            },
            &store
        ),
        Err(DirectionalPcaError::InvalidTemperature { .. })
    ));
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![block_id, block_id],
                params: params(1, 1, 1.0, 1.0, 0, 1, 0.0),
            },
            &store
        ),
        Err(DirectionalPcaError::InvalidMinimumInputCount { .. })
    ));
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![block_id, block_id],
                params: params(1, 1, 1.0, 1.0, 2, 2, 0.0),
            },
            &store
        ),
        Err(DirectionalPcaError::InvalidMinimumEffectiveRank { .. })
    ));
    assert!(matches!(
        run_directional_pca_layer(
            &DirectionalPcaLayerInput {
                block_ids: vec![block_id, block_id],
                params: params(1, 1, 1.0, 1.0, 2, 1, 1.5),
            },
            &store
        ),
        Err(DirectionalPcaError::InvalidMinimumCumulativeVariance { .. })
    ));
}

#[test]
fn val_dpca_007_layer_local_pca_realization_matches_manual_pca_projection() {
    let store = MemoryBlockStore::default();
    let vectors = vec![
        vec![0.0, 0.0],
        vec![1.0, 1.0],
        vec![10.0, 0.0],
        vec![11.0, 1.0],
    ];
    let block_ids = vectors
        .iter()
        .map(|vector| store.insert_block(&leaf_block_f32([vector[0], vector[1]])))
        .collect::<Vec<_>>();
    let params = params(2, 3, 1.0, 1.0, 2, 1, 0.0);

    let actual = run_directional_pca_layer(
        &DirectionalPcaLayerInput {
            block_ids: block_ids.clone(),
            params: params.clone(),
        },
        &store,
    )
    .unwrap();
    let expected = manual_partition(&vectors, &block_ids, &params);

    assert_eq!(actual, expected);
}

#[test]
fn val_dpca_008_directional_scoring_reflects_gamma_weighting() {
    let store = MemoryBlockStore::default();
    let vectors = vec![
        vec![0.0, 5.0],
        vec![0.0, 6.0],
        vec![10.0, 0.0],
        vec![11.0, 0.0],
        vec![12.0, 0.0],
    ];
    let block_ids = vectors
        .iter()
        .map(|vector| store.insert_block(&leaf_block_f32([vector[0], vector[1]])))
        .collect::<Vec<_>>();

    let flat = params(2, 4, 0.0, 1.0, 2, 1, 0.0);
    let weighted = params(2, 4, 1.0, 1.0, 2, 1, 0.0);
    let flat_actual = run_directional_pca_layer(
        &DirectionalPcaLayerInput {
            block_ids: block_ids.clone(),
            params: flat.clone(),
        },
        &store,
    )
    .unwrap();
    let weighted_actual = run_directional_pca_layer(
        &DirectionalPcaLayerInput {
            block_ids: block_ids.clone(),
            params: weighted.clone(),
        },
        &store,
    )
    .unwrap();

    assert_eq!(flat_actual, manual_partition(&vectors, &block_ids, &flat));
    assert_eq!(
        weighted_actual,
        manual_partition(&vectors, &block_ids, &weighted)
    );
    assert_ne!(flat_actual, weighted_actual);
}

#[test]
fn val_dpca_009_axis_allocation_matches_manual_temperature_controlled_budgeting() {
    let store = MemoryBlockStore::default();
    let vectors = vec![
        vec![0.0, 1.0],
        vec![1.0, 2.0],
        vec![2.0, 3.0],
        vec![10.0, 0.0],
        vec![11.0, 0.0],
        vec![12.0, 0.0],
    ];
    let block_ids = vectors
        .iter()
        .map(|vector| store.insert_block(&leaf_block_f32([vector[0], vector[1]])))
        .collect::<Vec<_>>();
    let params = params(2, 5, 1.0, 0.5, 2, 1, 0.0);

    let actual = run_directional_pca_layer(
        &DirectionalPcaLayerInput {
            block_ids: block_ids.clone(),
            params: params.clone(),
        },
        &store,
    )
    .unwrap();

    assert_eq!(actual, manual_partition(&vectors, &block_ids, &params));
}

#[test]
fn val_dpca_010_default_assignment_uses_quantile_bins() {
    let store = MemoryBlockStore::default();
    let vectors = [
        vec![0.0, 0.0],
        vec![1.0, 0.0],
        vec![100.0, 0.0],
        vec![101.0, 0.0],
        vec![102.0, 0.0],
    ];
    let block_ids = vectors
        .iter()
        .map(|vector| store.insert_block(&leaf_block_f32([vector[0], vector[1]])))
        .collect::<Vec<_>>();
    let params = params(1, 3, 1.0, 1.0, 2, 1, 0.0);

    let outcome =
        run_directional_pca_layer(&DirectionalPcaLayerInput { block_ids, params }, &store).unwrap();
    let DirectionalPcaLayerOutcome::Partitioned(result) = outcome else {
        panic!("expected eligible partition");
    };

    let group_sizes = result
        .groups
        .iter()
        .map(|group| group.member_block_ids.len())
        .collect::<Vec<_>>();
    assert_eq!(group_sizes, vec![2, 2, 1]);
}

#[test]
fn val_dpca_011_groups_materialize_only_populated_cells_with_centroids_and_member_ids() {
    let store = MemoryBlockStore::default();
    let left_a = store.insert_block(&leaf_block_f32([0.0, 0.0]));
    let left_b = store.insert_block(&leaf_block_f32([1.0, 0.0]));
    let right_a = store.insert_block(&leaf_block_f32([10.0, 0.0]));
    let right_b = store.insert_block(&leaf_block_f32([11.0, 0.0]));

    let outcome = run_directional_pca_layer(
        &DirectionalPcaLayerInput {
            block_ids: vec![left_a, left_b, right_a, right_b],
            params: params(1, 2, 1.0, 1.0, 2, 1, 0.0),
        },
        &store,
    )
    .unwrap();
    let DirectionalPcaLayerOutcome::Partitioned(result) = outcome else {
        panic!("expected eligible partition");
    };

    assert_eq!(
        result.groups,
        vec![
            DirectionalPcaGroup {
                centroid: vec![0.5, 0.0],
                member_block_ids: vec![left_a, left_b],
            },
            DirectionalPcaGroup {
                centroid: vec![10.5, 0.0],
                member_block_ids: vec![right_a, right_b],
            },
        ]
    );
}

#[test]
fn val_dpca_012_eligibility_outcomes_are_explicit() {
    let store = MemoryBlockStore::default();
    let first = store.insert_block(&leaf_block_f32([1.0, 0.0]));
    let second = store.insert_block(&leaf_block_f32([2.0, 0.0]));

    let insufficient_inputs = run_directional_pca_layer(
        &DirectionalPcaLayerInput {
            block_ids: vec![first, second],
            params: params(1, 1, 1.0, 1.0, 3, 1, 0.0),
        },
        &store,
    )
    .unwrap();
    assert_eq!(
        insufficient_inputs,
        DirectionalPcaLayerOutcome::Ineligible(DirectionalPcaEligibility::InsufficientInputCount {
            actual: 2,
            minimum: 3,
        })
    );

    let identical_store = MemoryBlockStore::default();
    let block_ids = vec![
        identical_store.insert_block(&leaf_block_f32([1.0, 0.0])),
        identical_store.insert_block(&leaf_block_f32([1.0, 0.0])),
        identical_store.insert_block(&leaf_block_f32([1.0, 0.0])),
    ];
    let low_rank = run_directional_pca_layer(
        &DirectionalPcaLayerInput {
            block_ids: block_ids.clone(),
            params: params(1, 1, 1.0, 1.0, 2, 1, 0.1),
        },
        &identical_store,
    )
    .unwrap();
    assert!(matches!(
        low_rank,
        DirectionalPcaLayerOutcome::Ineligible(
            DirectionalPcaEligibility::InsufficientExplainedVariance { .. }
        ) | DirectionalPcaLayerOutcome::Ineligible(
            DirectionalPcaEligibility::InsufficientEffectiveRank { .. }
        )
    ));

    let variance_store = MemoryBlockStore::default();
    let variance_ids = vec![
        variance_store.insert_block(&leaf_block_f32([0.0, 0.0])),
        variance_store.insert_block(&leaf_block_f32([0.0, 10.0])),
        variance_store.insert_block(&leaf_block_f32([10.0, 0.0])),
        variance_store.insert_block(&leaf_block_f32([10.0, 10.0])),
    ];
    let insufficient_variance = run_directional_pca_layer(
        &DirectionalPcaLayerInput {
            block_ids: variance_ids,
            params: params(1, 1, 1.0, 1.0, 2, 1, 0.9),
        },
        &variance_store,
    )
    .unwrap();
    assert!(matches!(
        insufficient_variance,
        DirectionalPcaLayerOutcome::Ineligible(
            DirectionalPcaEligibility::InsufficientExplainedVariance { .. }
        )
    ));
}

#[test]
fn val_dpca_013_repository_includes_verification_artifacts_and_workspace_wiring() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(manifest_dir.join("src/lib.rs").exists());
    assert!(manifest_dir.join("tests/spec_validation.rs").exists());

    let workspace_manifest = manifest_dir.join("..").join("..").join("Cargo.toml");
    let contents = std::fs::read_to_string(workspace_manifest).unwrap();
    assert!(contents.contains("crates/lexongraph-directional-pca"));
}

fn params(
    retained_dimension_count: usize,
    axis_resolution_budget: usize,
    variance_exponent: f32,
    temperature: f32,
    min_input_count: usize,
    min_effective_rank: usize,
    min_cumulative_variance: f32,
) -> DirectionalPcaLayerParams {
    DirectionalPcaLayerParams {
        retained_dimension_count,
        axis_resolution_budget,
        variance_exponent,
        temperature,
        min_input_count,
        min_effective_rank,
        min_cumulative_variance,
    }
}

fn leaf_block_f32(values: [f32; 2]) -> Block {
    leaf_block_raw(
        EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        },
        encode_f32_values(&values),
    )
}

fn leaf_block_i8(values: [i8; 2]) -> Block {
    leaf_block_raw(
        EmbeddingSpec {
            dims: 2,
            encoding: "i8".into(),
        },
        values.iter().map(|value| value.to_le_bytes()[0]).collect(),
    )
}

fn leaf_block_raw(spec: EmbeddingSpec, embedding: Vec<u8>) -> Block {
    Block::Leaf(
        build_leaf_block(
            VERSION_1,
            spec,
            vec![LeafEntry {
                embedding,
                metadata: Vec::new(),
                content: Content {
                    media_type: "application/octet-stream".into(),
                    body: b"fixture".to_vec(),
                },
            }],
            None,
        )
        .unwrap(),
    )
}

fn branch_block_f32(entries: &[[f32; 2]]) -> Block {
    let spec = EmbeddingSpec {
        dims: 2,
        encoding: "f32le".into(),
    };
    Block::Branch(
        build_branch_block(
            VERSION_1,
            1,
            spec,
            entries
                .iter()
                .enumerate()
                .map(|(index, entry)| BranchEntry {
                    embedding: encode_f32_values(entry),
                    child: synthetic_hash(index as u8 + 1),
                })
                .collect(),
            None,
        )
        .unwrap(),
    )
}

fn empty_branch_block_f32() -> Block {
    Block::Branch(
        build_branch_block(
            VERSION_1,
            1,
            EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            Vec::new(),
            None,
        )
        .unwrap(),
    )
}

fn synthetic_hash(seed: u8) -> BlockHash {
    BlockHash::from_bytes([seed; 32])
}

fn encode_f32_values(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn manual_partition(
    vectors: &[Vec<f32>],
    block_ids: &[BlockHash],
    params: &DirectionalPcaLayerParams,
) -> DirectionalPcaLayerOutcome {
    let transform = fit(vectors)
        .unwrap()
        .truncate(params.retained_dimension_count)
        .unwrap();
    let coordinates = vectors
        .iter()
        .map(|vector| transform.apply(vector).unwrap())
        .collect::<Vec<_>>();
    let layer_centroid = centroid(vectors);
    let explained_variance = transform.explained_variance().unwrap();
    let axis_scores = (0..transform.output_dim)
        .map(|column| {
            let alpha = (0..transform.input_dim)
                .map(|row| {
                    f64::from(layer_centroid[row])
                        * f64::from(transform.basis[row + column * transform.input_dim])
                })
                .sum::<f64>();
            let lambda = f64::from(explained_variance[column]);
            let variance_factor = if params.variance_exponent == 0.0 {
                1.0
            } else {
                lambda.powf(f64::from(params.variance_exponent))
            };
            alpha.abs() * variance_factor
        })
        .collect::<Vec<_>>();
    let axis_bins = manual_allocate_axis_bins(
        &axis_scores,
        params.axis_resolution_budget,
        params.temperature,
    );
    let point_bins = manual_quantile_bins(&coordinates, &axis_bins);
    let mut groups = BTreeMap::<Vec<usize>, Vec<usize>>::new();
    for (index, bins) in point_bins.into_iter().enumerate() {
        groups.entry(bins).or_default().push(index);
    }

    DirectionalPcaLayerOutcome::Partitioned(lexongraph_directional_pca::DirectionalPcaLayerResult {
        embedding_spec: EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        },
        groups: groups
            .into_values()
            .map(|point_indexes| DirectionalPcaGroup {
                centroid: centroid(
                    &point_indexes
                        .iter()
                        .map(|index| vectors[*index].clone())
                        .collect::<Vec<_>>(),
                ),
                member_block_ids: point_indexes
                    .iter()
                    .map(|index| block_ids[*index])
                    .collect(),
            })
            .collect(),
    })
}

fn manual_allocate_axis_bins(axis_scores: &[f64], budget: usize, temperature: f32) -> Vec<usize> {
    let damped = axis_scores
        .iter()
        .map(|score| (1.0 + score.max(0.0)).ln())
        .collect::<Vec<_>>();
    let temperature = f64::from(temperature);
    let max_scaled = damped
        .iter()
        .map(|value| value / temperature)
        .fold(f64::NEG_INFINITY, f64::max);
    let exp_values = damped
        .iter()
        .map(|value| ((value / temperature) - max_scaled).exp())
        .collect::<Vec<_>>();
    let sum = exp_values.iter().sum::<f64>();
    let mut bins = vec![1_usize; axis_scores.len()];
    let remaining = budget - axis_scores.len();
    if remaining == 0 {
        return bins;
    }
    let desired = exp_values
        .iter()
        .map(|value| value * remaining as f64 / sum)
        .collect::<Vec<_>>();
    let floors = desired
        .iter()
        .map(|value| value.floor() as usize)
        .collect::<Vec<_>>();
    for (bin, floor) in bins.iter_mut().zip(floors.iter().copied()) {
        *bin += floor;
    }
    let mut leftovers = remaining - floors.iter().sum::<usize>();
    let mut remainders = desired
        .iter()
        .enumerate()
        .map(|(index, value)| (index, value - value.floor()))
        .collect::<Vec<_>>();
    remainders.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    for (index, _) in remainders {
        if leftovers == 0 {
            break;
        }
        bins[index] += 1;
        leftovers -= 1;
    }
    bins
}

fn manual_quantile_bins(coordinates: &[Vec<f32>], axis_bin_counts: &[usize]) -> Vec<Vec<usize>> {
    let point_count = coordinates.len();
    let retained_dims = axis_bin_counts.len();
    let mut point_bins = vec![vec![0_usize; retained_dims]; point_count];
    for (axis, &bin_count) in axis_bin_counts.iter().enumerate() {
        if bin_count == 1 {
            continue;
        }
        let mut order = (0..point_count).collect::<Vec<_>>();
        order.sort_by(|left, right| {
            coordinates[*left][axis]
                .total_cmp(&coordinates[*right][axis])
                .then_with(|| left.cmp(right))
        });
        for (rank, point_index) in order.into_iter().enumerate() {
            point_bins[point_index][axis] = rank * bin_count / point_count;
        }
    }
    point_bins
}

fn centroid(vectors: &[Vec<f32>]) -> Vec<f32> {
    let dims = vectors[0].len();
    let mut sums = vec![0.0_f32; dims];
    for vector in vectors {
        for (index, value) in vector.iter().copied().enumerate() {
            sums[index] += value;
        }
    }
    sums.into_iter()
        .map(|value| value / vectors.len() as f32)
        .collect()
}
