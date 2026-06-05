// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use lexongraph_block::{
    BlockError, BlockHash, BranchBlock, BranchEntry, Content, EmbeddingSpec, SerializedBlock,
    TypedEntries, VERSION_1, build_branch_block, deserialize_block, into_entries, serialize_block,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_indexer::{
    ArithmeticMeanCanonicalEmbeddingPolicy, CanonicalEmbeddingPolicy, ContentResolver,
    DcbcNodePackingPolicy, IndexItem, IndexedChild, Indexer, IndexerError, NodePackingPolicy,
};

use std::cell::RefCell;
use std::collections::HashMap;

#[derive(Default)]
struct MemoryBlockStore {
    blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
}

impl MemoryBlockStore {
    fn len(&self) -> usize {
        self.blocks.borrow().len()
    }
}

impl BlockStore for MemoryBlockStore {
    fn put(&self, block: &lexongraph_block::Block) -> Result<BlockHash, BlockStoreError> {
        let serialized =
            lexongraph_block::serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
        self.blocks
            .borrow_mut()
            .insert(serialized.hash, serialized.bytes);
        Ok(serialized.hash)
    }

    fn get(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        let Some(bytes) = self.blocks.borrow().get(block_id).cloned() else {
            return Ok(None);
        };

        lexongraph_block::deserialize_block(&bytes, block_id)
            .map(Some)
            .map_err(map_get_error)
    }
}

#[derive(Default)]
struct WrongHashBlockStore {
    next: RefCell<u8>,
}

impl BlockStore for WrongHashBlockStore {
    fn put(&self, block: &lexongraph_block::Block) -> Result<BlockHash, BlockStoreError> {
        let _ =
            lexongraph_block::serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
        let mut next = self.next.borrow_mut();
        let hash = synthetic_hash(*next);
        *next = next.wrapping_add(1);
        Ok(hash)
    }

    fn get(
        &self,
        _: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        Ok(None)
    }
}

#[derive(Clone, Copy)]
struct MapResolver;

impl ContentResolver<&'static str> for MapResolver {
    type Error = FixtureError;

    fn resolve(&self, content_ref: &&'static str) -> Result<Content, Self::Error> {
        Ok(Content {
            media_type: "text/plain".into(),
            body: content_ref.as_bytes().to_vec(),
        })
    }
}

#[derive(Clone, Copy)]
struct FailingResolver;

impl ContentResolver<&'static str> for FailingResolver {
    type Error = FixtureError;

    fn resolve(&self, _: &&'static str) -> Result<Content, Self::Error> {
        Err(FixtureError("resolver unavailable".into()))
    }
}

#[derive(Clone, Copy)]
struct UnusableResolver;

impl ContentResolver<&'static str> for UnusableResolver {
    type Error = FixtureError;

    fn resolve(&self, content_ref: &&'static str) -> Result<Content, Self::Error> {
        Ok(Content {
            media_type: String::new(),
            body: content_ref.as_bytes().to_vec(),
        })
    }
}

#[derive(Clone, Copy)]
struct AsciiEmbeddingProvider;

impl EmbeddingProvider for AsciiEmbeddingProvider {
    type Error = FixtureError;

    async fn embed(
        &self,
        input: &EmbeddingInput,
        spec: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        let first = *input
            .body
            .first()
            .ok_or_else(|| FixtureError("expected non-empty content".into()))?;
        let second = input.body.len() as u8;
        let embedding = vec![first, second];
        if spec.encoding == "i8" && spec.dims == 2 {
            Ok(embedding)
        } else {
            Err(FixtureError("unexpected embedding spec".into()))
        }
    }
}

#[derive(Clone, Copy)]
struct FailingEmbeddingProvider;

impl EmbeddingProvider for FailingEmbeddingProvider {
    type Error = FixtureError;

    async fn embed(&self, _: &EmbeddingInput, _: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
        Err(FixtureError("embedding model offline".into()))
    }
}

#[derive(Clone, Copy)]
struct WrongLengthEmbeddingProvider;

impl EmbeddingProvider for WrongLengthEmbeddingProvider {
    type Error = FixtureError;

    async fn embed(&self, _: &EmbeddingInput, _: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![0x01])
    }
}

#[derive(Clone, Copy)]
struct ZeroEmbeddingProvider;

impl EmbeddingProvider for ZeroEmbeddingProvider {
    type Error = FixtureError;

    async fn embed(
        &self,
        _: &EmbeddingInput,
        spec: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        if spec.encoding == "i8" && spec.dims == 2 {
            Ok(vec![0x00, 0x00])
        } else {
            Err(FixtureError("unexpected embedding spec".into()))
        }
    }
}

#[derive(Clone)]
struct BatchOnlyEmbeddingProvider;

impl EmbeddingProvider for BatchOnlyEmbeddingProvider {
    type Error = FixtureError;

    async fn embed(&self, _: &EmbeddingInput, _: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
        Err(FixtureError(
            "single-item embedding path should not be used for batch indexing".into(),
        ))
    }

    async fn embed_batch(
        &self,
        inputs: &[EmbeddingInput],
        spec: &EmbeddingSpec,
    ) -> Result<Vec<Vec<u8>>, Self::Error> {
        if spec.encoding != "i8" || spec.dims != 2 {
            return Err(FixtureError("unexpected embedding spec".into()));
        }

        inputs
            .iter()
            .map(|input| {
                let first = *input
                    .body
                    .first()
                    .ok_or_else(|| FixtureError("expected non-empty content".into()))?;
                Ok(vec![first, input.body.len() as u8])
            })
            .collect()
    }
}

#[derive(Clone, Copy)]
struct FirstChildCanonicalPolicy;

impl CanonicalEmbeddingPolicy for FirstChildCanonicalPolicy {
    type Error = FixtureError;

    fn canonical_embedding(&self, block: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
        Ok(block.entries[0].embedding.clone())
    }
}

#[derive(Clone, Copy)]
struct FailingCanonicalPolicy;

impl CanonicalEmbeddingPolicy for FailingCanonicalPolicy {
    type Error = FixtureError;

    fn canonical_embedding(&self, _: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
        Err(FixtureError("canonical policy failed".into()))
    }
}

#[derive(Clone, Copy)]
struct WrongLengthCanonicalPolicy;

impl CanonicalEmbeddingPolicy for WrongLengthCanonicalPolicy {
    type Error = FixtureError;

    fn canonical_embedding(&self, _: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![0x01])
    }
}

#[derive(Clone, Copy)]
struct PairPackingPolicy;

impl NodePackingPolicy for PairPackingPolicy {
    type Error = FixtureError;

    fn pack(
        &self,
        children: &[IndexedChild],
        _: &EmbeddingSpec,
        _: usize,
    ) -> Result<Vec<Vec<usize>>, Self::Error> {
        if children.len() < 2 {
            return Err(FixtureError("expected at least two children".into()));
        }

        let mut groups: Vec<Vec<usize>> = Vec::new();
        let mut index = 0;
        while index < children.len() {
            let remaining = children.len() - index;
            let width = if remaining == 3 { 3 } else { 2 };
            groups.push((index..index + width).collect());
            index += width;
        }
        Ok(groups)
    }
}

#[derive(Clone, Copy)]
struct SingletonPackingPolicy;

impl NodePackingPolicy for SingletonPackingPolicy {
    type Error = FixtureError;

    fn pack(
        &self,
        children: &[IndexedChild],
        _: &EmbeddingSpec,
        _: usize,
    ) -> Result<Vec<Vec<usize>>, Self::Error> {
        Ok((0..children.len()).map(|index| vec![index]).collect())
    }
}

#[derive(Clone, Copy)]
struct OversizedPackingPolicy;

impl NodePackingPolicy for OversizedPackingPolicy {
    type Error = FixtureError;

    fn pack(
        &self,
        children: &[IndexedChild],
        _: &EmbeddingSpec,
        _: usize,
    ) -> Result<Vec<Vec<usize>>, Self::Error> {
        Ok(vec![(0..children.len()).collect()])
    }
}

#[derive(Clone, Copy)]
struct FailingPackingPolicy;

impl NodePackingPolicy for FailingPackingPolicy {
    type Error = FixtureError;

    fn pack(
        &self,
        _: &[IndexedChild],
        _: &EmbeddingSpec,
        _: usize,
    ) -> Result<Vec<Vec<usize>>, Self::Error> {
        Err(FixtureError("packing policy failed".into()))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_001_empty_input_fails_explicitly() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider);

    let error = indexer
        .index::<&'static str>(&[], embedding_spec(), 256, &store)
        .await
        .unwrap_err();

    assert_eq!(error, IndexerError::EmptyInput);
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_002_and_005_single_item_produces_leaf_root() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider);

    let result = indexer
        .index(&[item("alpha")], embedding_spec(), 256, &store)
        .await
        .unwrap();

    assert_eq!(result.block_ids.len(), 1);
    assert_eq!(store.len(), 1);

    let root = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(root) {
        TypedEntries::Leaf(metadata, entries) => {
            assert_eq!(metadata.level, 0);
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].content.body, b"alpha".to_vec());
        }
        TypedEntries::Branch(_, _) => panic!("expected leaf root"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_leaf_blocks_collapse_to_a_single_root_before_packing() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider);

    let result = indexer
        .index(
            &[item("alpha"), item("alpha")],
            embedding_spec(),
            256,
            &store,
        )
        .await
        .unwrap();

    assert_eq!(result.block_ids.len(), 1);
    let root = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(root) {
        TypedEntries::Leaf(metadata, entries) => {
            assert_eq!(metadata.level, 0);
            assert_eq!(entries[0].content.body, b"alpha".to_vec());
        }
        TypedEntries::Branch(_, _) => {
            panic!("expected duplicate leaf layer to collapse to a leaf root")
        }
    }

    let (staged_root, staged_ids) = staged_index(
        &indexer,
        &[item("alpha"), item("alpha")],
        embedding_spec(),
        256,
    )
    .await
    .unwrap();
    assert_eq!(staged_root, result.root_id);
    assert_eq!(staged_ids, result.block_ids);
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_003_resolution_and_embedding_failures_are_explicit() {
    let store = MemoryBlockStore::default();
    let failing_resolver = Indexer::with_node_packing_policy(
        FailingResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );
    let unusable_resolver = Indexer::with_node_packing_policy(
        UnusableResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );
    let failing_embedder = Indexer::with_node_packing_policy(
        MapResolver,
        FailingEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );
    let wrong_length_embedder = Indexer::with_node_packing_policy(
        MapResolver,
        WrongLengthEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );

    assert!(matches!(
        failing_resolver
            .index(&[item("alpha")], embedding_spec(), 256, &store)
            .await,
        Err(IndexerError::ContentResolution(_))
    ));
    assert!(matches!(
        unusable_resolver
            .index(&[item("alpha")], embedding_spec(), 256, &store)
            .await,
        Err(IndexerError::UnusableContent(_))
    ));
    assert!(matches!(
        failing_embedder
            .index(&[item("alpha")], embedding_spec(), 256, &store)
            .await,
        Err(IndexerError::EmbeddingFailure(_))
    ));
    assert!(matches!(
        wrong_length_embedder
            .index(&[item("alpha")], embedding_spec(), 256, &store)
            .await,
        Err(IndexerError::EmbeddingFailure(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_004_and_011_repeated_runs_with_same_logical_content_are_deterministic() {
    let first_store = MemoryBlockStore::default();
    let second_store = MemoryBlockStore::default();
    let indexer = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider);

    let first = indexer
        .index(
            &[item("same"), item("different-ref")],
            embedding_spec(),
            256,
            &first_store,
        )
        .await
        .unwrap();
    let second = indexer
        .index(
            &[item("same"), item("different-ref")],
            embedding_spec(),
            256,
            &second_store,
        )
        .await
        .unwrap();

    assert_eq!(first.root_id, second.root_id);
    assert_eq!(first.block_ids, second.block_ids);

    let alias_resolver = Indexer::with_defaults(AliasResolver, AsciiEmbeddingProvider);
    let alias_first = alias_resolver
        .index(
            &[item("alias-a"), item("alias-b")],
            embedding_spec(),
            256,
            &MemoryBlockStore::default(),
        )
        .await
        .unwrap();
    let alias_second = alias_resolver
        .index(
            &[item("alias-c"), item("alias-b")],
            embedding_spec(),
            256,
            &MemoryBlockStore::default(),
        )
        .await
        .unwrap();

    assert_eq!(alias_first.root_id, alias_second.root_id);
    assert_eq!(alias_first.block_ids, alias_second.block_ids);

    let staged_first = indexer
        .build_leaf_blocks(
            &[item("alpha"), item("bravo"), item("charlie"), item("delta")],
            embedding_spec(),
        )
        .await
        .unwrap();
    let staged_second = indexer
        .build_leaf_blocks(
            &[item("alpha"), item("bravo"), item("charlie"), item("delta")],
            embedding_spec(),
        )
        .await
        .unwrap();
    assert_eq!(staged_first.block_ids, staged_second.block_ids);

    let staged_parent_first = indexer
        .build_parent_blocks(&staged_first.blocks, embedding_spec(), 256)
        .unwrap();
    let staged_parent_second = indexer
        .build_parent_blocks(&staged_second.blocks, embedding_spec(), 256)
        .unwrap();
    assert_eq!(
        staged_parent_first.block_ids,
        staged_parent_second.block_ids
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_006_multiple_items_build_intermediate_layers_until_one_root_remains() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::with_node_packing_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );

    let result = indexer
        .index(
            &[item("alpha"), item("bravo"), item("charlie"), item("delta")],
            embedding_spec(),
            256,
            &store,
        )
        .await
        .unwrap();

    assert_eq!(store.len(), 7);
    let root = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(root) {
        TypedEntries::Branch(metadata, entries) => {
            assert_eq!(metadata.level, 2);
            assert_eq!(entries.len(), 2);
        }
        TypedEntries::Leaf(_, _) => panic!("expected branch root"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_007_intermediate_nodes_respect_size_limit_or_fail_explicitly() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider);
    let result = indexer
        .index(
            &[item("alpha"), item("bravo"), item("charlie")],
            embedding_spec(),
            256,
            &store,
        )
        .await
        .unwrap();

    for block_id in result.block_ids {
        let validated = store.get(&block_id).unwrap().unwrap();
        if let TypedEntries::Branch(_, entries) = into_entries(validated.clone()) {
            assert!(entries.len() >= 2);
            let serialized = lexongraph_block::serialize_block(&validated.block).unwrap();
            assert!(serialized.bytes.len() <= 256);
        }
    }

    let too_small = Indexer::with_node_packing_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        OversizedPackingPolicy,
    );
    let error = too_small
        .index(
            &[item("alpha"), item("bravo")],
            embedding_spec(),
            24,
            &MemoryBlockStore::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        IndexerError::IntermediateNodeTooLarge { .. }
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_008_child_entries_are_sorted_and_deduplicated_by_child_id() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider);

    let result = indexer
        .index(
            &[item("alpha"), item("alpha"), item("charlie")],
            embedding_spec(),
            256,
            &store,
        )
        .await
        .unwrap();

    let root = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(root) {
        TypedEntries::Branch(_, entries) => {
            for pair in entries.windows(2) {
                assert!(pair[0].embedding <= pair[1].embedding);
                assert_ne!(pair[0].child, pair[1].child);
            }
        }
        TypedEntries::Leaf(_, _) => panic!("expected branch root"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_009_distinct_resolver_types_share_the_same_contract() {
    let store = MemoryBlockStore::default();
    let memory_indexer = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider);
    let alias_indexer = Indexer::with_defaults(AliasResolver, AsciiEmbeddingProvider);

    let memory = memory_indexer
        .index(
            &[item("alpha"), item("bravo")],
            embedding_spec(),
            256,
            &store,
        )
        .await
        .unwrap();
    let alias = alias_indexer
        .index(
            &[item("alias-a"), item("alias-b")],
            embedding_spec(),
            256,
            &MemoryBlockStore::default(),
        )
        .await
        .unwrap();

    assert_eq!(memory.block_ids.len(), alias.block_ids.len());
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_010_different_policy_implementations_share_the_same_api_boundary() {
    let first = Indexer::with_canonical_embedding_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    );
    let second = Indexer::with_node_packing_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        ReverseCanonicalPolicy,
        TriplePackingPolicy,
    );

    assert!(
        first
            .index(
                &[item("alpha"), item("bravo"), item("charlie"), item("delta")],
                embedding_spec(),
                256,
                &MemoryBlockStore::default(),
            )
            .await
            .is_ok()
    );
    assert!(
        second
            .index(
                &[
                    item("alpha"),
                    item("bravo"),
                    item("charlie"),
                    item("delta"),
                    item("echo"),
                    item("foxtrot")
                ],
                embedding_spec(),
                256,
                &MemoryBlockStore::default(),
            )
            .await
            .is_ok()
    );

    let staged_leaves = first
        .build_leaf_blocks(
            &[item("alpha"), item("bravo"), item("charlie"), item("delta")],
            embedding_spec(),
        )
        .await
        .unwrap();
    assert!(
        first
            .build_parent_blocks(&staged_leaves.blocks, embedding_spec(), 256)
            .is_ok()
    );

    let staged_override_leaves = second
        .build_leaf_blocks(
            &[
                item("alpha"),
                item("bravo"),
                item("charlie"),
                item("delta"),
                item("echo"),
                item("foxtrot"),
            ],
            embedding_spec(),
        )
        .await
        .unwrap();
    assert!(
        second
            .build_parent_blocks(&staged_override_leaves.blocks, embedding_spec(), 256)
            .is_ok()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_018_default_constructor_uses_builtin_node_packing() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider);

    let result = indexer
        .index(
            &[
                item("alpha"),
                item("bravo"),
                item("charlie"),
                item("delta"),
                item("echo"),
                item("foxtrot"),
            ],
            embedding_spec(),
            160,
            &store,
        )
        .await
        .unwrap();

    assert!(result.block_ids.len() > 6);
    let root = store.get(&result.root_id).unwrap().unwrap();
    assert!(matches!(into_entries(root), TypedEntries::Branch(_, _)));
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_019_default_and_override_paths_both_work() {
    let items = [
        item("alpha"),
        item("bravo"),
        item("charlie"),
        item("delta"),
        item("echo"),
        item("foxtrot"),
    ];
    let default_result = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider)
        .index(&items, embedding_spec(), 160, &MemoryBlockStore::default())
        .await
        .unwrap();
    let explicit_default_result = Indexer::with_node_packing_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        DcbcNodePackingPolicy,
    )
    .index(&items, embedding_spec(), 160, &MemoryBlockStore::default())
    .await
    .unwrap();
    let canonical_override_result = Indexer::with_canonical_embedding_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    )
    .index(&items, embedding_spec(), 160, &MemoryBlockStore::default())
    .await
    .unwrap();
    let override_result = Indexer::with_node_packing_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    )
    .index(&items, embedding_spec(), 160, &MemoryBlockStore::default())
    .await
    .unwrap();

    assert!(!default_result.block_ids.is_empty());
    assert_eq!(default_result.root_id, explicit_default_result.root_id);
    assert_eq!(default_result.block_ids, explicit_default_result.block_ids);
    assert!(!canonical_override_result.block_ids.is_empty());
    assert!(!override_result.block_ids.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_020_default_dcbc_packing_is_deterministic() {
    let items = [
        item("alpha"),
        item("bravo"),
        item("charlie"),
        item("delta"),
        item("echo"),
        item("foxtrot"),
    ];
    let first = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider)
        .index(&items, embedding_spec(), 160, &MemoryBlockStore::default())
        .await
        .unwrap();
    let second = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider)
        .index(&items, embedding_spec(), 160, &MemoryBlockStore::default())
        .await
        .unwrap();

    assert_eq!(first.root_id, second.root_id);
    assert_eq!(first.block_ids, second.block_ids);
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_021_default_packing_uses_core_size_enforcement_for_tiny_targets() {
    let error = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider)
        .index(
            &[item("alpha"), item("bravo")],
            embedding_spec(),
            24,
            &MemoryBlockStore::default(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        IndexerError::IntermediateNodeTooLarge { .. }
    ));
}

#[test]
fn val_indexer_022_default_policy_uses_shared_dcbc_dependency() {
    assert!(include_str!("../Cargo.toml").contains("lexongraph-dcbc"));

    let groups = DcbcNodePackingPolicy
        .pack(&synthetic_children(), &embedding_spec(), 160)
        .unwrap();

    let mut flattened = groups.into_iter().flatten().collect::<Vec<_>>();
    flattened.sort_unstable();
    assert_eq!(flattened, vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn val_indexer_023_builtin_canonical_policy_computes_i8_arithmetic_mean() {
    let branch = synthetic_branch_block(
        i8_three_dim_embedding_spec(),
        vec![vec![0, 0, 1], vec![1, 0xff, 2]],
    );

    let embedding = ArithmeticMeanCanonicalEmbeddingPolicy
        .canonical_embedding(&branch)
        .unwrap();

    assert_eq!(embedding, vec![1, 0xff, 2]);
}

#[test]
fn val_indexer_023_builtin_canonical_policy_computes_f32le_arithmetic_mean() {
    let branch = synthetic_branch_block(
        f32le_embedding_spec(),
        vec![f32le_embedding([1.0, 0.5]), f32le_embedding([3.0, 1.5])],
    );

    let embedding = ArithmeticMeanCanonicalEmbeddingPolicy
        .canonical_embedding(&branch)
        .unwrap();

    assert_eq!(embedding, f32le_embedding([2.0, 1.0]));
}

#[test]
fn val_indexer_023_builtin_canonical_policy_computes_f16le_arithmetic_mean() {
    let branch = synthetic_branch_block(
        f16le_embedding_spec(),
        vec![f16le_embedding([1.0, 0.5]), f16le_embedding([3.0, 1.5])],
    );

    let embedding = ArithmeticMeanCanonicalEmbeddingPolicy
        .canonical_embedding(&branch)
        .unwrap();

    assert_eq!(embedding, f16le_embedding([2.0, 1.0]));
}

#[test]
fn val_indexer_024_builtin_canonical_policy_fails_explicitly() {
    let empty_error = ArithmeticMeanCanonicalEmbeddingPolicy
        .canonical_embedding(&BranchBlock {
            version: VERSION_1,
            level: 1,
            embedding_spec: embedding_spec(),
            entries: vec![],
            ext: None,
        })
        .unwrap_err();
    assert!(
        empty_error
            .to_string()
            .contains("at least one branch entry")
    );

    let unsupported_error = ArithmeticMeanCanonicalEmbeddingPolicy
        .canonical_embedding(&synthetic_branch_block(
            pq4_embedding_spec(),
            vec![vec![0x12], vec![0x34]],
        ))
        .unwrap_err();
    assert!(unsupported_error.to_string().contains("pq4"));

    let non_finite_error = ArithmeticMeanCanonicalEmbeddingPolicy
        .canonical_embedding(&synthetic_branch_block(
            f32le_embedding_spec(),
            vec![
                f32le_embedding([f32::INFINITY, 1.0]),
                f32le_embedding([2.0, 3.0]),
            ],
        ))
        .unwrap_err();
    assert!(non_finite_error.to_string().contains("non-finite"));
}

#[test]
fn default_policy_rejects_pq4_embeddings() {
    let error = DcbcNodePackingPolicy
        .pack(&pq4_children(), &pq4_embedding_spec(), 96)
        .unwrap_err();

    assert!(error.to_string().contains("pq4"));
}

#[test]
fn default_policy_decodes_f32le_embeddings() {
    let groups = DcbcNodePackingPolicy
        .pack(&f32le_children(), &f32le_embedding_spec(), 160)
        .unwrap();

    let mut flattened = groups.into_iter().flatten().collect::<Vec<_>>();
    flattened.sort_unstable();
    assert_eq!(flattened, vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn default_policy_decodes_f16le_embeddings() {
    let groups = DcbcNodePackingPolicy
        .pack(&f16le_children(), &f16le_embedding_spec(), 160)
        .unwrap();

    let mut flattened = groups.into_iter().flatten().collect::<Vec<_>>();
    flattened.sort_unstable();
    assert_eq!(flattened, vec![0, 1, 2, 3, 4, 5]);
}

#[tokio::test(flavor = "current_thread")]
async fn default_policy_surfaces_zero_norm_embeddings_explicitly() {
    let error = Indexer::with_defaults(MapResolver, ZeroEmbeddingProvider)
        .index(
            &[item("alpha"), item("bravo"), item("charlie"), item("delta")],
            embedding_spec(),
            96,
            &MemoryBlockStore::default(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, IndexerError::NodePackingFailure(_)));
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_012_resolved_content_is_stored_inline() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider);

    let result = indexer
        .index(&[item("alpha")], embedding_spec(), 256, &store)
        .await
        .unwrap();

    let root = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(root) {
        TypedEntries::Leaf(_, entries) => {
            assert_eq!(entries[0].content.media_type, "text/plain");
            assert_eq!(entries[0].content.body, b"alpha".to_vec());
        }
        TypedEntries::Branch(_, _) => panic!("expected leaf root"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_025_incremental_leaf_batch_construction() {
    let indexer = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider);
    let first_batch = indexer
        .build_leaf_blocks(&[item("alpha"), item("bravo")], embedding_spec())
        .await
        .unwrap();
    let second_batch = indexer
        .build_leaf_blocks(&[item("charlie"), item("delta")], embedding_spec())
        .await
        .unwrap();
    let one_shot = indexer
        .build_leaf_blocks(
            &[item("alpha"), item("bravo"), item("charlie"), item("delta")],
            embedding_spec(),
        )
        .await
        .unwrap();

    let mut incremental_ids = first_batch
        .block_ids
        .iter()
        .chain(second_batch.block_ids.iter())
        .copied()
        .collect::<Vec<_>>();
    incremental_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

    assert_eq!(incremental_ids, sorted_hashes(&one_shot.block_ids));
    for block in first_batch.blocks.iter().chain(second_batch.blocks.iter()) {
        match into_entries(deserialize_block(&block.bytes, &block.hash).unwrap()) {
            TypedEntries::Leaf(_, entries) => assert_eq!(entries.len(), 1),
            TypedEntries::Branch(_, _) => {
                panic!("expected staged leaf construction to emit leaves")
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_031_collection_indexing_realizes_batch_embeddings_without_caller_sub_batches()
{
    let store = MemoryBlockStore::default();
    let indexer = Indexer::with_defaults(MapResolver, BatchOnlyEmbeddingProvider);

    let result = indexer
        .index::<&'static str>(
            &[item("alpha"), item("bravo"), item("charlie")],
            embedding_spec(),
            160,
            &store,
        )
        .await
        .unwrap();

    assert!(!result.block_ids.is_empty());
    assert!(store.get(&result.root_id).unwrap().is_some());
}

#[test]
fn val_indexer_026_parent_construction_from_child_blocks() {
    let indexer = Indexer::with_node_packing_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );
    let leaves = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            indexer
                .build_leaf_blocks(
                    &[item("alpha"), item("bravo"), item("charlie")],
                    embedding_spec(),
                )
                .await
                .unwrap()
        });

    let parents = indexer
        .build_parent_blocks(&leaves.blocks, embedding_spec(), 256)
        .unwrap();

    assert_eq!(parents.blocks.len(), 1);
    match into_entries(
        deserialize_block(&parents.blocks[0].bytes, &parents.blocks[0].hash).unwrap(),
    ) {
        TypedEntries::Branch(metadata, entries) => {
            assert_eq!(metadata.level, 1);
            assert_eq!(entries.len(), 3);
            for pair in entries.windows(2) {
                assert!(pair[0].embedding <= pair[1].embedding);
                assert_ne!(pair[0].child, pair[1].child);
            }
        }
        TypedEntries::Leaf(_, _) => panic!("expected parent construction to emit branch blocks"),
    }
    assert!(parents.blocks[0].bytes.len() <= 256);
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_027_resumable_staged_execution() {
    let indexer = Indexer::with_node_packing_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );
    let leaves = indexer
        .build_leaf_blocks(
            &[item("alpha"), item("bravo"), item("charlie"), item("delta")],
            embedding_spec(),
        )
        .await
        .unwrap();

    let reloaded_leaves = round_trip_serialized_blocks(&leaves.blocks);
    let parents = indexer
        .build_parent_blocks(&reloaded_leaves, embedding_spec(), 256)
        .unwrap();
    let reloaded_parents = round_trip_serialized_blocks(&parents.blocks);
    let root = indexer
        .build_parent_blocks(&reloaded_parents, embedding_spec(), 256)
        .unwrap();

    assert_eq!(root.blocks.len(), 1);
    match into_entries(deserialize_block(&root.blocks[0].bytes, &root.blocks[0].hash).unwrap()) {
        TypedEntries::Branch(metadata, _) => assert_eq!(metadata.level, 2),
        TypedEntries::Leaf(_, _) => panic!("expected a branch root"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_028_monolithic_and_staged_paths_are_equivalent() {
    let items = [
        item("alpha"),
        item("bravo"),
        item("charlie"),
        item("delta"),
        item("echo"),
        item("foxtrot"),
    ];
    let indexer = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider);
    let monolithic = indexer
        .index(&items, embedding_spec(), 160, &MemoryBlockStore::default())
        .await
        .unwrap();
    let (staged_root, staged_ids) = staged_index(&indexer, &items, embedding_spec(), 160)
        .await
        .unwrap();

    assert_eq!(staged_root, monolithic.root_id);
    assert_eq!(staged_ids, monolithic.block_ids);
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_029_mixed_leaf_and_branch_parent_inputs() {
    let indexer = Indexer::with_node_packing_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        ReverseCanonicalPolicy,
        PairPackingPolicy,
    );
    let leaves = indexer
        .build_leaf_blocks(
            &[item("alpha"), item("bravo"), item("charlie"), item("delta")],
            embedding_spec(),
        )
        .await
        .unwrap();
    let nested_branch = indexer
        .build_parent_blocks(&leaves.blocks[..2], embedding_spec(), 256)
        .unwrap();

    let mixed_inputs = vec![
        nested_branch.blocks[0].clone(),
        leaves.blocks[2].clone(),
        leaves.blocks[3].clone(),
    ];
    let error = indexer
        .build_parent_blocks(&mixed_inputs, embedding_spec(), 256)
        .unwrap_err();
    assert!(matches!(error, IndexerError::InvalidStagedInput(_)));
    assert!(error.to_string().contains("shared level"));
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_030_staged_inputs_fail_explicitly() {
    let indexer = Indexer::with_defaults(MapResolver, AsciiEmbeddingProvider);

    assert_eq!(
        indexer
            .build_leaf_blocks::<&'static str>(&[], embedding_spec())
            .await
            .unwrap_err(),
        IndexerError::EmptyInput
    );

    let empty_parent_error = indexer
        .build_parent_blocks(&[], embedding_spec(), 256)
        .unwrap_err();
    assert!(matches!(
        empty_parent_error,
        IndexerError::InvalidStagedInput(_)
    ));

    let leaves = indexer
        .build_leaf_blocks(&[item("alpha"), item("bravo")], embedding_spec())
        .await
        .unwrap();
    let invalid_block = SerializedBlock {
        bytes: leaves.blocks[0].bytes.clone(),
        hash: synthetic_hash(99),
    };
    assert!(matches!(
        indexer.build_parent_blocks(&[invalid_block], embedding_spec(), 256),
        Err(IndexerError::InvalidInputBlock(
            BlockError::HashMismatch { .. }
        ))
    ));

    assert!(matches!(
        indexer.build_parent_blocks(&leaves.blocks[..1], i8_three_dim_embedding_spec(), 256),
        Err(IndexerError::InvalidStagedInput(_))
    ));

    let integrity_error = indexer
        .index(
            &[item("alpha"), item("bravo")],
            embedding_spec(),
            256,
            &WrongHashBlockStore::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        integrity_error,
        IndexerError::Storage(BlockStoreError::IntegrityMismatch { .. })
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_canonical_and_packing_failures_are_explicit() {
    let canonical_error = Indexer::with_node_packing_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        FailingCanonicalPolicy,
        PairPackingPolicy,
    )
    .index(
        &[item("alpha"), item("bravo"), item("charlie"), item("delta")],
        embedding_spec(),
        256,
        &MemoryBlockStore::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        canonical_error,
        IndexerError::CanonicalEmbeddingFailure(_)
    ));

    let canonical_length_error = Indexer::with_node_packing_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        WrongLengthCanonicalPolicy,
        PairPackingPolicy,
    )
    .index(
        &[item("alpha"), item("bravo"), item("charlie"), item("delta")],
        embedding_spec(),
        256,
        &MemoryBlockStore::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        canonical_length_error,
        IndexerError::CanonicalEmbeddingFailure(_)
    ));

    let singleton_error = Indexer::with_node_packing_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        SingletonPackingPolicy,
    )
    .index(
        &[item("alpha"), item("bravo"), item("charlie")],
        embedding_spec(),
        256,
        &MemoryBlockStore::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        singleton_error,
        IndexerError::NodePackingFailure(_)
    ));

    let failing_error = Indexer::with_node_packing_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        FailingPackingPolicy,
    )
    .index(
        &[item("alpha"), item("bravo"), item("charlie")],
        embedding_spec(),
        256,
        &MemoryBlockStore::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(failing_error, IndexerError::NodePackingFailure(_)));
}

#[derive(Clone, Copy)]
struct AliasResolver;

impl ContentResolver<&'static str> for AliasResolver {
    type Error = FixtureError;

    fn resolve(&self, content_ref: &&'static str) -> Result<Content, Self::Error> {
        let body = match *content_ref {
            "alias-a" | "alias-c" => "same",
            "alias-b" => "different-ref",
            other => other,
        };
        Ok(Content {
            media_type: "text/plain".into(),
            body: body.as_bytes().to_vec(),
        })
    }
}

#[derive(Clone, Copy)]
struct ReverseCanonicalPolicy;

impl CanonicalEmbeddingPolicy for ReverseCanonicalPolicy {
    type Error = FixtureError;

    fn canonical_embedding(&self, block: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
        Ok(block.entries.last().unwrap().embedding.clone())
    }
}

#[derive(Clone, Copy)]
struct TriplePackingPolicy;

impl NodePackingPolicy for TriplePackingPolicy {
    type Error = FixtureError;

    fn pack(
        &self,
        children: &[IndexedChild],
        _: &EmbeddingSpec,
        _: usize,
    ) -> Result<Vec<Vec<usize>>, Self::Error> {
        let mut groups: Vec<Vec<usize>> = Vec::new();
        let mut index = 0;
        while index < children.len() {
            let remaining = children.len() - index;
            let width = if remaining <= 3 { remaining } else { 3 };
            if width < 2 {
                if let Some(last) = groups.last_mut() {
                    last.push(index);
                    break;
                }
                return Err(FixtureError("expected at least two children".into()));
            }
            groups.push((index..index + width).collect());
            index += width;
        }
        Ok(groups)
    }
}

#[derive(Clone, Debug)]
struct FixtureError(String);

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for FixtureError {}

fn item(content_ref: &'static str) -> IndexItem<&'static str> {
    IndexItem {
        metadata: vec![],
        content_ref,
    }
}

fn embedding_spec() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: "i8".into(),
    }
}

fn pq4_embedding_spec() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: "pq4".into(),
    }
}

fn f32le_embedding_spec() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: "f32le".into(),
    }
}

fn f16le_embedding_spec() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: "f16le".into(),
    }
}

fn i8_three_dim_embedding_spec() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 3,
        encoding: "i8".into(),
    }
}

fn synthetic_children() -> Vec<IndexedChild> {
    vec![
        indexed_child(0, [97, 5]),
        indexed_child(1, [98, 5]),
        indexed_child(2, [99, 7]),
        indexed_child(3, [100, 5]),
        indexed_child(4, [101, 4]),
        indexed_child(5, [102, 7]),
    ]
}

fn pq4_children() -> Vec<IndexedChild> {
    (0..4)
        .map(|index| IndexedChild {
            embedding: vec![0x12],
            child: synthetic_hash(index),
            level: 0,
        })
        .collect()
}

fn f32le_children() -> Vec<IndexedChild> {
    vec![
        float_indexed_child(0, 1.0_f32.to_le_bytes(), 0.5_f32.to_le_bytes()),
        float_indexed_child(1, 1.1_f32.to_le_bytes(), 0.5_f32.to_le_bytes()),
        float_indexed_child(2, 4.0_f32.to_le_bytes(), 0.25_f32.to_le_bytes()),
        float_indexed_child(3, 4.1_f32.to_le_bytes(), 0.25_f32.to_le_bytes()),
        float_indexed_child(4, 8.0_f32.to_le_bytes(), 0.75_f32.to_le_bytes()),
        float_indexed_child(5, 8.1_f32.to_le_bytes(), 0.75_f32.to_le_bytes()),
    ]
}

fn f16le_children() -> Vec<IndexedChild> {
    vec![
        half_indexed_child(0, 1.0, 0.5),
        half_indexed_child(1, 1.1, 0.5),
        half_indexed_child(2, 4.0, 0.25),
        half_indexed_child(3, 4.1, 0.25),
        half_indexed_child(4, 8.0, 0.75),
        half_indexed_child(5, 8.1, 0.75),
    ]
}

fn synthetic_branch_block(spec: EmbeddingSpec, embeddings: Vec<Vec<u8>>) -> BranchBlock {
    build_branch_block(
        VERSION_1,
        1,
        spec,
        embeddings
            .into_iter()
            .enumerate()
            .map(|(index, embedding)| BranchEntry {
                embedding,
                child: synthetic_hash(index as u8),
            })
            .collect(),
        None,
    )
    .unwrap()
}

fn f32le_embedding(values: [f32; 2]) -> Vec<u8> {
    let mut embedding = Vec::with_capacity(8);
    for value in values {
        embedding.extend_from_slice(&value.to_le_bytes());
    }
    embedding
}

fn f16le_embedding(values: [f32; 2]) -> Vec<u8> {
    let mut embedding = Vec::with_capacity(4);
    for value in values {
        embedding.extend_from_slice(&half::f16::from_f32(value).to_le_bytes());
    }
    embedding
}

fn indexed_child(index: u8, embedding: [u8; 2]) -> IndexedChild {
    IndexedChild {
        embedding: embedding.to_vec(),
        child: synthetic_hash(index),
        level: 0,
    }
}

fn float_indexed_child(index: u8, first: [u8; 4], second: [u8; 4]) -> IndexedChild {
    let mut embedding = Vec::with_capacity(8);
    embedding.extend_from_slice(&first);
    embedding.extend_from_slice(&second);
    IndexedChild {
        embedding,
        child: synthetic_hash(index),
        level: 0,
    }
}

fn half_indexed_child(index: u8, first: f32, second: f32) -> IndexedChild {
    let mut embedding = Vec::with_capacity(4);
    embedding.extend_from_slice(&half::f16::from_f32(first).to_le_bytes());
    embedding.extend_from_slice(&half::f16::from_f32(second).to_le_bytes());
    IndexedChild {
        embedding,
        child: synthetic_hash(index),
        level: 0,
    }
}

fn synthetic_hash(index: u8) -> BlockHash {
    let mut bytes = [0_u8; BlockHash::LEN];
    bytes[0] = index;
    BlockHash::from_bytes(bytes)
}

fn map_get_error(error: lexongraph_block::BlockError) -> BlockStoreError {
    match error {
        lexongraph_block::BlockError::HashMismatch { expected, actual } => {
            BlockStoreError::IntegrityMismatch { expected, actual }
        }
        other => BlockStoreError::MalformedContent(other),
    }
}

async fn staged_index<CR, EP, CEP, NPP, R>(
    indexer: &Indexer<CR, EP, CEP, NPP>,
    items: &[IndexItem<R>],
    embedding_spec: EmbeddingSpec,
    block_size_target: usize,
) -> Result<(BlockHash, Vec<BlockHash>), IndexerError>
where
    CR: ContentResolver<R>,
    EP: EmbeddingProvider,
    CEP: CanonicalEmbeddingPolicy,
    NPP: NodePackingPolicy,
{
    let mut layer = indexer
        .build_leaf_blocks(items, embedding_spec.clone())
        .await?;
    let mut block_ids = layer.block_ids.clone();

    while unique_serialized_blocks_by_hash(&layer.blocks).len() > 1 {
        layer = indexer.build_parent_blocks(
            &unique_serialized_blocks_by_hash(&layer.blocks),
            embedding_spec.clone(),
            block_size_target,
        )?;
        block_ids.extend(layer.block_ids.iter().copied());
    }

    block_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    block_ids.dedup_by(|left, right| left.as_bytes() == right.as_bytes());
    let final_blocks = unique_serialized_blocks_by_hash(&layer.blocks);
    Ok((final_blocks[0].hash, block_ids))
}

fn round_trip_serialized_blocks(blocks: &[SerializedBlock]) -> Vec<SerializedBlock> {
    blocks
        .iter()
        .map(|block| {
            let validated = deserialize_block(&block.bytes, &block.hash).unwrap();
            serialize_block(&validated.block).unwrap()
        })
        .collect()
}

fn sorted_hashes(hashes: &[BlockHash]) -> Vec<BlockHash> {
    let mut hashes = hashes.to_vec();
    hashes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    hashes
}

fn unique_serialized_blocks_by_hash(blocks: &[SerializedBlock]) -> Vec<SerializedBlock> {
    let mut blocks = blocks.to_vec();
    blocks.sort_by(|left, right| left.hash.as_bytes().cmp(right.hash.as_bytes()));
    blocks.dedup_by(|left, right| left.hash == right.hash);
    blocks
}
