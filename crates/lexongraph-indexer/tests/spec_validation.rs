// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use lexongraph_block::{
    BlockHash, BranchBlock, Content, EmbeddingSpec, TypedEntries, into_entries,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_indexer::{
    CanonicalEmbeddingPolicy, ContentResolver, DcbcNodePackingPolicy, IndexItem, IndexedChild,
    Indexer, IndexerError, NodePackingPolicy,
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
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    );

    let error = indexer
        .index::<&'static str>(&[], embedding_spec(), 256, &store)
        .await
        .unwrap_err();

    assert_eq!(error, IndexerError::EmptyInput);
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_002_and_005_single_item_produces_leaf_root() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    );

    let result = indexer
        .index(&[item("alpha")], embedding_spec(), 256, &store)
        .await
        .unwrap();

    assert_eq!(result.block_ids.len(), 1);
    assert_eq!(store.len(), 1);

    let root = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(root) {
        TypedEntries::Leaf(_, entries) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].content.body, b"alpha".to_vec());
        }
        TypedEntries::Branch(_, _) => panic!("expected leaf root"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_leaf_blocks_collapse_to_a_single_root_before_packing() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    );

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
        TypedEntries::Leaf(_, entries) => assert_eq!(entries[0].content.body, b"alpha".to_vec()),
        TypedEntries::Branch(_, _) => {
            panic!("expected duplicate leaf layer to collapse to a leaf root")
        }
    }
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
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    );

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

    let alias_resolver = Indexer::new(
        AliasResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    );
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
        TypedEntries::Branch(_, entries) => assert_eq!(entries.len(), 2),
        TypedEntries::Leaf(_, _) => panic!("expected branch root"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_007_intermediate_nodes_respect_size_limit_or_fail_explicitly() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    );
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
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    );

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
    let memory_indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    );
    let alias_indexer = Indexer::new(
        AliasResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    );

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
    let first = Indexer::new(
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
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_018_default_constructor_uses_builtin_node_packing() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    );

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
    let default_result = Indexer::new(
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
    let first = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    )
    .index(&items, embedding_spec(), 160, &MemoryBlockStore::default())
    .await
    .unwrap();
    let second = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    )
    .index(&items, embedding_spec(), 160, &MemoryBlockStore::default())
    .await
    .unwrap();

    assert_eq!(first.root_id, second.root_id);
    assert_eq!(first.block_ids, second.block_ids);
}

#[tokio::test(flavor = "current_thread")]
async fn val_indexer_021_default_packing_uses_core_size_enforcement_for_tiny_targets() {
    let error = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    )
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
    let error = Indexer::new(
        MapResolver,
        ZeroEmbeddingProvider,
        FirstChildCanonicalPolicy,
    )
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
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
    );

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

fn indexed_child(index: u8, embedding: [u8; 2]) -> IndexedChild {
    IndexedChild {
        embedding: embedding.to_vec(),
        child: synthetic_hash(index),
    }
}

fn float_indexed_child(index: u8, first: [u8; 4], second: [u8; 4]) -> IndexedChild {
    let mut embedding = Vec::with_capacity(8);
    embedding.extend_from_slice(&first);
    embedding.extend_from_slice(&second);
    IndexedChild {
        embedding,
        child: synthetic_hash(index),
    }
}

fn half_indexed_child(index: u8, first: f32, second: f32) -> IndexedChild {
    let mut embedding = Vec::with_capacity(4);
    embedding.extend_from_slice(&half::f16::from_f32(first).to_le_bytes());
    embedding.extend_from_slice(&half::f16::from_f32(second).to_le_bytes());
    IndexedChild {
        embedding,
        child: synthetic_hash(index),
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
