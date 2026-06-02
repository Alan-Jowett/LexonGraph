use lexongraph_block::{
    BlockHash, BranchBlock, Content, EmbeddingSpec, TypedEntries, into_entries,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_indexer::{
    CanonicalEmbeddingPolicy, ContentResolver, EmbeddingProvider, IndexItem, IndexedChild, Indexer,
    IndexerError, NodePackingPolicy,
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

    fn embed(&self, content: &Content, spec: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
        let first = *content
            .body
            .first()
            .ok_or_else(|| FixtureError("expected non-empty content".into()))?;
        let second = content.body.len() as u8;
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

    fn embed(&self, _: &Content, _: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
        Err(FixtureError("embedding model offline".into()))
    }
}

#[derive(Clone, Copy)]
struct WrongLengthEmbeddingProvider;

impl EmbeddingProvider for WrongLengthEmbeddingProvider {
    type Error = FixtureError;

    fn embed(&self, _: &Content, _: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![0x01])
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

#[test]
fn val_indexer_001_empty_input_fails_explicitly() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );

    let error = indexer
        .index::<&'static str>(&[], embedding_spec(), 256, &store)
        .unwrap_err();

    assert_eq!(error, IndexerError::EmptyInput);
}

#[test]
fn val_indexer_002_and_005_single_item_produces_leaf_root() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );

    let result = indexer
        .index(&[item("alpha")], embedding_spec(), 256, &store)
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

#[test]
fn duplicate_leaf_blocks_collapse_to_a_single_root_before_packing() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );

    let result = indexer
        .index(
            &[item("alpha"), item("alpha")],
            embedding_spec(),
            256,
            &store,
        )
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

#[test]
fn val_indexer_003_resolution_and_embedding_failures_are_explicit() {
    let store = MemoryBlockStore::default();
    let failing_resolver = Indexer::new(
        FailingResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );
    let unusable_resolver = Indexer::new(
        UnusableResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );
    let failing_embedder = Indexer::new(
        MapResolver,
        FailingEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );
    let wrong_length_embedder = Indexer::new(
        MapResolver,
        WrongLengthEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );

    assert!(matches!(
        failing_resolver.index(&[item("alpha")], embedding_spec(), 256, &store),
        Err(IndexerError::ContentResolution(_))
    ));
    assert!(matches!(
        unusable_resolver.index(&[item("alpha")], embedding_spec(), 256, &store),
        Err(IndexerError::UnusableContent(_))
    ));
    assert!(matches!(
        failing_embedder.index(&[item("alpha")], embedding_spec(), 256, &store),
        Err(IndexerError::EmbeddingFailure(_))
    ));
    assert!(matches!(
        wrong_length_embedder.index(&[item("alpha")], embedding_spec(), 256, &store),
        Err(IndexerError::EmbeddingFailure(_))
    ));
}

#[test]
fn val_indexer_004_and_011_repeated_runs_with_same_logical_content_are_deterministic() {
    let first_store = MemoryBlockStore::default();
    let second_store = MemoryBlockStore::default();
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );

    let first = indexer
        .index(
            &[item("same"), item("different-ref")],
            embedding_spec(),
            256,
            &first_store,
        )
        .unwrap();
    let second = indexer
        .index(
            &[item("same"), item("different-ref")],
            embedding_spec(),
            256,
            &second_store,
        )
        .unwrap();

    assert_eq!(first.root_id, second.root_id);
    assert_eq!(first.block_ids, second.block_ids);

    let alias_resolver = Indexer::new(
        AliasResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );
    let alias_first = alias_resolver
        .index(
            &[item("alias-a"), item("alias-b")],
            embedding_spec(),
            256,
            &MemoryBlockStore::default(),
        )
        .unwrap();
    let alias_second = alias_resolver
        .index(
            &[item("alias-c"), item("alias-b")],
            embedding_spec(),
            256,
            &MemoryBlockStore::default(),
        )
        .unwrap();

    assert_eq!(alias_first.root_id, alias_second.root_id);
    assert_eq!(alias_first.block_ids, alias_second.block_ids);
}

#[test]
fn val_indexer_006_multiple_items_build_intermediate_layers_until_one_root_remains() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::new(
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
        .unwrap();

    assert_eq!(store.len(), 7);
    let root = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(root) {
        TypedEntries::Branch(_, entries) => assert_eq!(entries.len(), 2),
        TypedEntries::Leaf(_, _) => panic!("expected branch root"),
    }
}

#[test]
fn val_indexer_007_intermediate_nodes_respect_size_limit_or_fail_explicitly() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );
    let result = indexer
        .index(
            &[item("alpha"), item("bravo"), item("charlie")],
            embedding_spec(),
            256,
            &store,
        )
        .unwrap();

    for block_id in result.block_ids {
        let validated = store.get(&block_id).unwrap().unwrap();
        if let TypedEntries::Branch(_, entries) = into_entries(validated.clone()) {
            assert!(entries.len() >= 2);
            let serialized = lexongraph_block::serialize_block(&validated.block).unwrap();
            assert!(serialized.bytes.len() <= 256);
        }
    }

    let too_small = Indexer::new(
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
        .unwrap_err();
    assert!(matches!(
        error,
        IndexerError::IntermediateNodeTooLarge { .. }
    ));
}

#[test]
fn val_indexer_008_child_entries_are_sorted_and_deduplicated_by_child_id() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );

    let result = indexer
        .index(
            &[item("alpha"), item("alpha"), item("charlie")],
            embedding_spec(),
            256,
            &store,
        )
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

#[test]
fn val_indexer_009_distinct_resolver_types_share_the_same_contract() {
    let store = MemoryBlockStore::default();
    let memory_indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );
    let alias_indexer = Indexer::new(
        AliasResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );

    let memory = memory_indexer
        .index(
            &[item("alpha"), item("bravo")],
            embedding_spec(),
            256,
            &store,
        )
        .unwrap();
    let alias = alias_indexer
        .index(
            &[item("alias-a"), item("alias-b")],
            embedding_spec(),
            256,
            &MemoryBlockStore::default(),
        )
        .unwrap();

    assert_eq!(memory.block_ids.len(), alias.block_ids.len());
}

#[test]
fn val_indexer_010_different_policy_implementations_share_the_same_api_boundary() {
    let first = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );
    let second = Indexer::new(
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
            .is_ok()
    );
}

#[test]
fn val_indexer_012_resolved_content_is_stored_inline() {
    let store = MemoryBlockStore::default();
    let indexer = Indexer::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairPackingPolicy,
    );

    let result = indexer
        .index(&[item("alpha")], embedding_spec(), 256, &store)
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

#[test]
fn val_indexer_canonical_and_packing_failures_are_explicit() {
    let canonical_error = Indexer::new(
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
    .unwrap_err();
    assert!(matches!(
        canonical_error,
        IndexerError::CanonicalEmbeddingFailure(_)
    ));

    let canonical_length_error = Indexer::new(
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
    .unwrap_err();
    assert!(matches!(
        canonical_length_error,
        IndexerError::CanonicalEmbeddingFailure(_)
    ));

    let singleton_error = Indexer::new(
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
    .unwrap_err();
    assert!(matches!(
        singleton_error,
        IndexerError::NodePackingFailure(_)
    ));

    let failing_error = Indexer::new(
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

fn map_get_error(error: lexongraph_block::BlockError) -> BlockStoreError {
    match error {
        lexongraph_block::BlockError::HashMismatch { expected, actual } => {
            BlockStoreError::IntegrityMismatch { expected, actual }
        }
        other => BlockStoreError::MalformedContent(other),
    }
}
