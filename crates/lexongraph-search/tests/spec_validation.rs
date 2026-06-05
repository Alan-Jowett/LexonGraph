// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;

use lexongraph_block::{
    Block, BlockHash, BranchEntry, Content, EmbeddingSpec, LeafEntry, VERSION_1,
    build_branch_block, build_leaf_block, compute_block_hash,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_search::{
    CandidateScorer, DefaultCandidateScorer, DefaultEmbeddingCompatibility, DefaultPolicyError,
    EmbeddingCompatibility, EncodedTargetEmbedding, SearchError, SearchResult, Searcher,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct FixtureError(String);

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FixtureError {}

#[derive(Default)]
struct MemoryBlockStore {
    blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
    gets: RefCell<HashMap<BlockHash, usize>>,
}

impl MemoryBlockStore {
    fn raw_insert(&self, block_id: BlockHash, bytes: Vec<u8>) {
        self.blocks.borrow_mut().insert(block_id, bytes);
    }

    fn get_count(&self, block_id: &BlockHash) -> usize {
        self.gets.borrow().get(block_id).copied().unwrap_or(0)
    }
}

impl BlockStore for MemoryBlockStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
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
        *self.gets.borrow_mut().entry(*block_id).or_default() += 1;

        let Some(bytes) = self.blocks.borrow().get(block_id).cloned() else {
            return Ok(None);
        };

        lexongraph_block::deserialize_block(&bytes, block_id)
            .map(Some)
            .map_err(map_get_error)
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        let block_ids = self.blocks.borrow().keys().copied().collect::<Vec<_>>();
        Ok(Box::new(block_ids.into_iter().map(Ok)))
    }
}

#[derive(Default)]
struct FailingGetStore {
    inner: MemoryBlockStore,
    fail_on: RefCell<Option<BlockHash>>,
    fail_message: &'static str,
}

impl FailingGetStore {
    fn always_fail(message: &'static str) -> Self {
        Self {
            inner: MemoryBlockStore::default(),
            fail_on: RefCell::new(None),
            fail_message: message,
        }
    }

    fn fail_on(block_id: BlockHash, message: &'static str) -> Self {
        Self {
            inner: MemoryBlockStore::default(),
            fail_on: RefCell::new(Some(block_id)),
            fail_message: message,
        }
    }
}

impl BlockStore for FailingGetStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        self.inner.put(block)
    }

    fn get(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        let configured = *self.fail_on.borrow();
        if configured.is_none() || configured == Some(*block_id) {
            return Err(BlockStoreError::BackendFailure(self.fail_message.into()));
        }

        self.inner.get(block_id)
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        self.inner.iter_block_ids()
    }
}

#[derive(Clone, Copy)]
struct AcceptEncoding(&'static str);

impl EmbeddingCompatibility<()> for AcceptEncoding {
    type Error = FixtureError;

    fn ensure_compatible(&self, _: &(), embedding_spec: &EmbeddingSpec) -> Result<(), Self::Error> {
        if embedding_spec.encoding == self.0 {
            Ok(())
        } else {
            Err(FixtureError(format!(
                "expected encoding {}, got {}",
                self.0, embedding_spec.encoding
            )))
        }
    }
}

#[derive(Clone, Copy)]
struct AcceptAllCompatibility;

impl EmbeddingCompatibility<()> for AcceptAllCompatibility {
    type Error = FixtureError;

    fn ensure_compatible(&self, _: &(), _: &EmbeddingSpec) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct CountingScorer {
    seen: RefCell<Vec<Vec<u8>>>,
}

impl CountingScorer {
    fn new() -> Self {
        Self {
            seen: RefCell::new(Vec::new()),
        }
    }

    fn seen_embeddings(&self) -> Vec<Vec<u8>> {
        self.seen.borrow().clone()
    }
}

impl CandidateScorer<()> for &CountingScorer {
    type Error = FixtureError;
    type Score = i32;

    fn score(
        &self,
        _: &(),
        candidate_embedding: &[u8],
        _: &EmbeddingSpec,
    ) -> Result<Self::Score, Self::Error> {
        self.seen.borrow_mut().push(candidate_embedding.to_vec());
        Ok(candidate_embedding[0] as i32)
    }
}

#[derive(Clone, Copy)]
struct FirstByteScorer;

impl CandidateScorer<()> for FirstByteScorer {
    type Error = FixtureError;
    type Score = i32;

    fn score(
        &self,
        _: &(),
        candidate_embedding: &[u8],
        _: &EmbeddingSpec,
    ) -> Result<Self::Score, Self::Error> {
        Ok(candidate_embedding[0] as i32)
    }
}

#[derive(Clone, Copy)]
struct WeightedFirstByteScorer;

impl CandidateScorer<()> for WeightedFirstByteScorer {
    type Error = FixtureError;
    type Score = i32;

    fn score(
        &self,
        _: &(),
        candidate_embedding: &[u8],
        _: &EmbeddingSpec,
    ) -> Result<Self::Score, Self::Error> {
        Ok((candidate_embedding[0] as i32) * 10)
    }
}

#[derive(Clone, Copy)]
struct FailingScorer;

impl CandidateScorer<()> for FailingScorer {
    type Error = FixtureError;
    type Score = i32;

    fn score(&self, _: &(), _: &[u8], _: &EmbeddingSpec) -> Result<Self::Score, Self::Error> {
        Err(FixtureError("scorer offline".into()))
    }
}

#[test]
fn val_search_001_root_is_loaded_once_and_all_root_entries_are_scored() {
    let store = MemoryBlockStore::default();
    let root = leaf_block(i8_embedding([7, 1]), "root");
    let root_id = store.put(&root).unwrap();
    let scorer = CountingScorer::new();
    let searcher = Searcher::new(AcceptAllCompatibility, &scorer);

    let result = searcher.search(&root_id, &(), 1, 1, &store).unwrap();

    assert_eq!(result.leaves.len(), 1);
    assert_eq!(store.get_count(&root_id), 1);
    assert_eq!(scorer.seen_embeddings(), vec![vec![7, 1]]);
}

#[test]
fn val_search_002_public_api_exposes_protocol_inputs_and_policy_dependencies() {
    fn uses_only_public_contract(
        searcher: &Searcher<AcceptAllCompatibility, FirstByteScorer>,
        root_id: &BlockHash,
        store: &dyn BlockStore,
    ) -> Result<SearchResult, SearchError> {
        searcher.search(root_id, &(), 1, 1, store)
    }

    let store = MemoryBlockStore::default();
    let root_id = store
        .put(&leaf_block(i8_embedding([9, 0]), "public"))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let result = uses_only_public_contract(&searcher, &root_id, &store).unwrap();

    assert_eq!(result.leaves[0].entry.content.body, b"public".to_vec());
}

#[test]
fn val_search_003_repeated_runs_are_deterministic() {
    let store = MemoryBlockStore::default();
    let leaf_a = store
        .put(&leaf_block(i8_embedding([8, 0]), "alpha"))
        .unwrap();
    let leaf_b = store
        .put(&leaf_block(i8_embedding([6, 0]), "bravo"))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([8, 0], leaf_a), branch_entry([6, 0], leaf_b)],
        ))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let first = searcher.search(&root_id, &(), 2, 2, &store).unwrap();
    let second = searcher.search(&root_id, &(), 2, 2, &store).unwrap();

    assert_eq!(first, second);
}

#[test]
fn val_search_004_equal_embedding_branches_to_different_children_remain_distinct() {
    let store = MemoryBlockStore::default();
    let leaf_a = store
        .put(&leaf_block(i8_embedding([5, 0]), "alpha"))
        .unwrap();
    let leaf_b = store
        .put(&leaf_block(i8_embedding([5, 0]), "bravo"))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([5, 0], leaf_a), branch_entry([5, 0], leaf_b)],
        ))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let result = searcher.search(&root_id, &(), 2, 2, &store).unwrap();

    assert_eq!(result.leaves.len(), 2);
    assert_eq!(store.get_count(&leaf_a), 1);
    assert_eq!(store.get_count(&leaf_b), 1);
}

#[test]
fn val_search_005_best_ranked_duplicate_child_occurrence_controls_selection() {
    let store = MemoryBlockStore::default();
    let preferred = store
        .put(&leaf_block(i8_embedding([10, 0]), "preferred"))
        .unwrap();
    let alternate = store
        .put(&leaf_block(i8_embedding([3, 0]), "alternate"))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([1, 0], preferred),
                branch_entry([9, 0], preferred),
                branch_entry([8, 0], alternate),
            ],
        ))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let result = searcher.search(&root_id, &(), 1, 1, &store).unwrap();

    assert_eq!(result.leaves[0].entry.content.body, b"preferred".to_vec());
    assert_eq!(store.get_count(&preferred), 1);
    assert_eq!(store.get_count(&alternate), 0);
}

#[test]
fn val_search_006_equal_leaf_embeddings_in_distinct_blocks_both_survive() {
    let store = MemoryBlockStore::default();
    let leaf_a = store
        .put(&leaf_block(i8_embedding([7, 0]), "alpha"))
        .unwrap();
    let leaf_b = store
        .put(&leaf_block(i8_embedding([7, 0]), "bravo"))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([9, 0], leaf_a), branch_entry([8, 0], leaf_b)],
        ))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let result = searcher.search(&root_id, &(), 2, 2, &store).unwrap();

    assert_eq!(result.leaves.len(), 2);
    assert_ne!(
        result.leaves[0].leaf_block_id,
        result.leaves[1].leaf_block_id
    );
}

#[test]
fn val_search_007_duplicate_branch_targets_expand_once_per_round() {
    let store = MemoryBlockStore::default();
    let duplicated = store.put(&leaf_block(i8_embedding([4, 0]), "dup")).unwrap();
    let distinct = store
        .put(&leaf_block(i8_embedding([3, 0]), "distinct"))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([9, 0], duplicated),
                branch_entry([8, 0], duplicated),
                branch_entry([7, 0], distinct),
            ],
        ))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let result = searcher.search(&root_id, &(), 2, 2, &store).unwrap();

    assert_eq!(result.leaves.len(), 2);
    assert_eq!(store.get_count(&duplicated), 1);
    assert_eq!(store.get_count(&distinct), 1);
}

#[test]
fn val_search_008_incompatible_embedding_specs_fail_explicitly() {
    let store = MemoryBlockStore::default();
    let incompatible_leaf = store
        .put(&leaf_block_with_spec(
            EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            vec![1, 0, 0, 0, 2, 0, 0, 0],
            "incompatible",
        ))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([9, 0], incompatible_leaf),
                branch_entry([8, 0], incompatible_leaf),
            ],
        ))
        .unwrap();
    let searcher = Searcher::new(AcceptEncoding("i8"), FirstByteScorer);

    let error = searcher.search(&root_id, &(), 1, 1, &store).unwrap_err();

    assert!(matches!(error, SearchError::IncompatibleEmbedding { .. }));
}

#[test]
fn val_search_009_missing_root_and_child_blocks_fail_explicitly() {
    let store = MemoryBlockStore::default();
    let root_id = BlockHash::from_bytes([0x55; 32]);
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let missing_root = searcher.search(&root_id, &(), 1, 1, &store).unwrap_err();
    assert!(matches!(missing_root, SearchError::MissingRootBlock { .. }));

    let missing_child = BlockHash::from_bytes([0x77; 32]);
    let branch_root = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([9, 0], missing_child),
                branch_entry([8, 0], missing_child),
            ],
        ))
        .unwrap();

    let error = searcher
        .search(&branch_root, &(), 1, 1, &store)
        .unwrap_err();
    assert!(matches!(error, SearchError::MissingChildBlock { .. }));

    let failing_root_store = FailingGetStore::always_fail("root backend unavailable");
    let error = searcher
        .search(
            &BlockHash::from_bytes([0x11; 32]),
            &(),
            1,
            1,
            &failing_root_store,
        )
        .unwrap_err();
    assert!(matches!(error, SearchError::RootLoad(_)));

    let selected_child = BlockHash::from_bytes([0x99; 32]);
    let failing_child_store = FailingGetStore::fail_on(selected_child, "child backend unavailable");
    let root_id = failing_child_store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([9, 0], selected_child),
                branch_entry([8, 0], selected_child),
            ],
        ))
        .unwrap();
    let error = searcher
        .search(&root_id, &(), 1, 1, &failing_child_store)
        .unwrap_err();
    assert!(matches!(
        error,
        SearchError::ChildLoad { child_id, .. } if child_id == selected_child
    ));
}

#[test]
fn val_search_010_malformed_root_and_child_blocks_fail_explicitly() {
    let store = MemoryBlockStore::default();
    let malformed_bytes = vec![0xff, 0xff, 0x00];
    let malformed_root = compute_block_hash(&malformed_bytes);
    store.raw_insert(malformed_root, malformed_bytes.clone());
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let root_error = searcher
        .search(&malformed_root, &(), 1, 1, &store)
        .unwrap_err();
    assert!(matches!(root_error, SearchError::MalformedBlock { .. }));

    let malformed_child = compute_block_hash(&malformed_bytes);
    let store = MemoryBlockStore::default();
    store.raw_insert(malformed_child, malformed_bytes);
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([9, 0], malformed_child),
                branch_entry([8, 0], malformed_child),
            ],
        ))
        .unwrap();

    let child_error = searcher.search(&root_id, &(), 1, 1, &store).unwrap_err();
    assert!(matches!(child_error, SearchError::MalformedBlock { .. }));
}

#[test]
fn val_search_011_different_deterministic_metrics_share_the_same_api_boundary() {
    let store = MemoryBlockStore::default();
    let leaf_a = store
        .put(&leaf_block(i8_embedding([8, 2]), "alpha"))
        .unwrap();
    let leaf_b = store
        .put(&leaf_block(i8_embedding([6, 5]), "bravo"))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([8, 2], leaf_a), branch_entry([6, 5], leaf_b)],
        ))
        .unwrap();
    let first = Searcher::new(AcceptAllCompatibility, FirstByteScorer);
    let second = Searcher::new(AcceptAllCompatibility, WeightedFirstByteScorer);

    let first_result = first.search(&root_id, &(), 2, 2, &store).unwrap();
    let second_result = second.search(&root_id, &(), 2, 2, &store).unwrap();

    assert_eq!(first_result.leaves.len(), 2);
    assert_eq!(second_result.leaves.len(), 2);
    assert_eq!(
        first_result.leaves[0].leaf_block_id,
        second_result.leaves[0].leaf_block_id
    );
}

#[test]
fn val_search_012_search_stops_when_top_n_are_leaves() {
    let store = MemoryBlockStore::default();
    let top_leaf = store.put(&leaf_block(i8_embedding([9, 0]), "top")).unwrap();
    let deep_leaf = store
        .put(&leaf_block(i8_embedding([1, 0]), "deep"))
        .unwrap();
    let lower_branch = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([1, 0], deep_leaf),
                branch_entry([0, 0], deep_leaf),
            ],
        ))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([9, 0], top_leaf),
                branch_entry([8, 0], lower_branch),
            ],
        ))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let result = searcher.search(&root_id, &(), 1, 1, &store).unwrap();

    assert_eq!(result.leaves[0].entry.content.body, b"top".to_vec());
    assert_eq!(store.get_count(&lower_branch), 0);
}

#[test]
fn val_search_013_search_fails_when_n_reachable_leaves_do_not_exist() {
    let store = MemoryBlockStore::default();
    let only_leaf = store
        .put(&leaf_block(i8_embedding([5, 0]), "only"))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([9, 0], only_leaf),
                branch_entry([8, 0], only_leaf),
            ],
        ))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let error = searcher.search(&root_id, &(), 1, 2, &store).unwrap_err();

    assert_eq!(
        error,
        SearchError::Exhausted {
            requested: 2,
            reachable_leaves: 1,
        }
    );
}

#[test]
fn val_search_019_canonical_tie_breaks_follow_protocol_order() {
    let store = MemoryBlockStore::default();
    let tied_leaf = store
        .put(&leaf_block(i8_embedding([8, 0]), "early-leaf"))
        .unwrap();
    let deferred_leaf = store
        .put(&leaf_block(i8_embedding([1, 0]), "deferred-leaf"))
        .unwrap();
    let deferred_branch = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([1, 0], deferred_leaf),
                branch_entry([0, 0], deferred_leaf),
            ],
        ))
        .unwrap();
    let tied_root = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([9, 0], tied_leaf),
                branch_entry([8, 0], deferred_branch),
            ],
        ))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let result = searcher.search(&tied_root, &(), 1, 1, &store).unwrap();

    assert_eq!(result.leaves[0].entry.content.body, b"early-leaf".to_vec());
    assert_eq!(store.get_count(&deferred_branch), 0);

    let leaf_a = store
        .put(&leaf_block(i8_embedding([7, 0]), "alpha"))
        .unwrap();
    let leaf_b = store
        .put(&leaf_block(i8_embedding([7, 0]), "bravo"))
        .unwrap();
    let leaf_tie_root = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([9, 0], leaf_a), branch_entry([9, 0], leaf_b)],
        ))
        .unwrap();

    let leaf_tie_result = searcher.search(&leaf_tie_root, &(), 2, 2, &store).unwrap();

    assert_eq!(leaf_tie_result.leaves.len(), 2);
    assert!(
        leaf_tie_result.leaves[0].leaf_block_id.as_bytes()
            <= leaf_tie_result.leaves[1].leaf_block_id.as_bytes()
    );

    let branch_first = store
        .put(&leaf_block(i8_embedding([10, 0]), "branch-first"))
        .unwrap();
    let branch_second = store
        .put(&leaf_block(i8_embedding([10, 0]), "branch-second"))
        .unwrap();
    let branch_third = store
        .put(&leaf_block(i8_embedding([10, 0]), "branch-third"))
        .unwrap();
    let branch_tie_root = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([9, 0], branch_first),
                branch_entry([9, 0], branch_second),
                branch_entry([9, 0], branch_third),
            ],
        ))
        .unwrap();

    let branch_tie_result = searcher
        .search(&branch_tie_root, &(), 2, 2, &store)
        .unwrap();

    assert_eq!(branch_tie_result.leaves.len(), 2);
    let mut children = [branch_first, branch_second, branch_third];
    children.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    assert_eq!(store.get_count(&children[0]), 1);
    assert_eq!(store.get_count(&children[1]), 1);
    assert_eq!(store.get_count(&children[2]), 0);

    let low_level_leaf = store
        .put(&leaf_block(i8_embedding([7, 0]), "low-level"))
        .unwrap();
    let deferred_leaf = store
        .put(&leaf_block(i8_embedding([6, 0]), "deferred-level"))
        .unwrap();
    let low_level_branch = store
        .put(&branch_block_at_level(
            1,
            embedding_spec_i8(),
            vec![branch_entry([7, 0], low_level_leaf)],
        ))
        .unwrap();
    let deferred_level_branch = store
        .put(&branch_block_at_level(
            1,
            embedding_spec_i8(),
            vec![branch_entry([6, 0], deferred_leaf)],
        ))
        .unwrap();
    let multi_level_root = store
        .put(&branch_block_at_level(
            2,
            embedding_spec_i8(),
            vec![
                branch_entry([8, 0], low_level_branch),
                branch_entry([7, 0], deferred_level_branch),
            ],
        ))
        .unwrap();

    let multi_level_result = searcher
        .search(&multi_level_root, &(), 1, 1, &store)
        .unwrap();

    assert_eq!(
        multi_level_result.leaves[0].entry.content.body,
        b"low-level".to_vec()
    );
    assert_eq!(store.get_count(&deferred_level_branch), 0);
}

#[test]
fn val_search_020_leaf_candidates_remain_in_frontier_across_rounds() {
    let store = MemoryBlockStore::default();
    let early_leaf = store
        .put(&leaf_block(i8_embedding([7, 0]), "early"))
        .unwrap();
    let late_leaf_high = store
        .put(&leaf_block(i8_embedding([6, 0]), "late-high"))
        .unwrap();
    let late_leaf_low = store
        .put(&leaf_block(i8_embedding([5, 0]), "late-low"))
        .unwrap();
    let nested_branch = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([6, 0], late_leaf_high),
                branch_entry([5, 0], late_leaf_low),
            ],
        ))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([9, 0], early_leaf),
                branch_entry([8, 0], nested_branch),
            ],
        ))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let result = searcher.search(&root_id, &(), 1, 2, &store).unwrap();

    assert_eq!(result.leaves.len(), 2);
    assert_eq!(result.leaves[0].entry.content.body, b"early".to_vec());
    assert_eq!(result.leaves[1].entry.content.body, b"late-high".to_vec());
    assert_eq!(store.get_count(&nested_branch), 1);
    assert_eq!(store.get_count(&late_leaf_high), 1);
    assert_eq!(store.get_count(&late_leaf_low), 0);
}

#[test]
fn val_search_014_public_surface_is_limited_to_runtime_contract() {
    fn uses_only_runtime_contract(
        searcher: &Searcher<AcceptAllCompatibility, FirstByteScorer>,
        root_id: &BlockHash,
        store: &dyn BlockStore,
    ) -> Result<SearchResult, SearchError> {
        searcher.search(root_id, &(), 1, 1, store)
    }

    let store = MemoryBlockStore::default();
    let root_id = store
        .put(&leaf_block(i8_embedding([4, 0]), "surface"))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);
    let result = uses_only_runtime_contract(&searcher, &root_id, &store).unwrap();

    assert_eq!(result.leaves[0].position, 0);

    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = std::fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
    let source = std::fs::read_to_string(manifest_dir.join("src").join("lib.rs")).unwrap();

    assert!(manifest.contains("conformance = []"));
    assert!(source.contains("#[cfg(feature = \"conformance\")]"));
    assert!(!source.contains("lexongraph_block_store::conformance"));
    assert!(!source.contains("BlockStoreConformanceHarness"));
    assert!(!source.contains("run_full_suite"));
}

#[test]
fn val_search_021_zero_parameter_semantics_are_explicit() {
    let store = MemoryBlockStore::default();
    let root_id = store
        .put(&leaf_block(i8_embedding([4, 0]), "leaf"))
        .unwrap();

    let width_error = Searcher::new(AcceptAllCompatibility, FirstByteScorer)
        .search(&root_id, &(), 0, 1, &store)
        .unwrap_err();
    assert_eq!(width_error, SearchError::InvalidTraversalWidth { w: 0 });

    let scoring_error = Searcher::new(AcceptAllCompatibility, FailingScorer)
        .search(&root_id, &(), 1, 1, &store)
        .unwrap_err();
    assert!(matches!(scoring_error, SearchError::ScoringFailure { .. }));

    let store = MemoryBlockStore::default();
    let child = store
        .put(&leaf_block(i8_embedding([3, 0]), "child"))
        .unwrap();
    let zero_root = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([7, 0], child), branch_entry([6, 0], child)],
        ))
        .unwrap();
    let scorer = CountingScorer::new();
    let empty = Searcher::new(AcceptAllCompatibility, &scorer)
        .search(&zero_root, &(), 1, 0, &store)
        .unwrap();
    assert!(empty.leaves.is_empty());
    assert_eq!(store.get_count(&zero_root), 1);
    assert_eq!(store.get_count(&child), 0);
    let mut seen = scorer.seen_embeddings();
    seen.sort();
    assert_eq!(seen, vec![vec![6, 0], vec![7, 0]]);
}

#[test]
fn val_search_022_default_runtime_surface_exposes_default_policy_types() {
    fn uses_default_contract(
        searcher: &Searcher<DefaultEmbeddingCompatibility, DefaultCandidateScorer>,
        root_id: &BlockHash,
        target: &EncodedTargetEmbedding,
        store: &dyn BlockStore,
    ) -> Result<SearchResult, SearchError> {
        searcher.search(root_id, target, 1, 1, store)
    }

    fn uses_custom_contract(
        searcher: &Searcher<AcceptAllCompatibility, FirstByteScorer>,
        root_id: &BlockHash,
        store: &dyn BlockStore,
    ) -> Result<SearchResult, SearchError> {
        searcher.search(root_id, &(), 1, 1, store)
    }

    let store = MemoryBlockStore::default();
    let root_id = store
        .put(&leaf_block_with_spec(
            embedding_spec_f32(),
            f32_embedding([1.0, 0.0]),
            "default",
        ))
        .unwrap();

    let target = EncodedTargetEmbedding::new(f32_embedding([1.0, 0.0]), embedding_spec_f32());
    let default_searcher = Searcher::new(DefaultEmbeddingCompatibility, DefaultCandidateScorer);
    let default_result =
        uses_default_contract(&default_searcher, &root_id, &target, &store).unwrap();
    assert_eq!(
        default_result.leaves[0].entry.content.body,
        b"default".to_vec()
    );

    let custom_root_id = store
        .put(&leaf_block(i8_embedding([9, 0]), "custom"))
        .unwrap();
    let custom_searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);
    let custom_result = uses_custom_contract(&custom_searcher, &custom_root_id, &store).unwrap();
    assert_eq!(
        custom_result.leaves[0].entry.content.body,
        b"custom".to_vec()
    );
}

#[test]
fn val_search_023_default_embedding_compatibility_accepts_matching_specs_only() {
    let target = EncodedTargetEmbedding::new(f32_embedding([1.0, 0.0]), embedding_spec_f32());
    let compatibility = DefaultEmbeddingCompatibility;

    compatibility
        .ensure_compatible(&target, &embedding_spec_f32())
        .unwrap();

    let encoding_error = compatibility
        .ensure_compatible(&target, &embedding_spec_f64())
        .unwrap_err();
    assert!(matches!(
        encoding_error,
        DefaultPolicyError::IncompatibleEmbeddingSpec { .. }
    ));

    let dims_error = compatibility
        .ensure_compatible(
            &target,
            &EmbeddingSpec {
                dims: 3,
                encoding: "f32le".into(),
            },
        )
        .unwrap_err();
    assert!(matches!(
        dims_error,
        DefaultPolicyError::IncompatibleEmbeddingSpec { .. }
    ));
}

#[test]
fn val_search_024_default_candidate_scorer_is_cosine_ranked_and_fails_explicitly() {
    let scorer = DefaultCandidateScorer;
    let target = EncodedTargetEmbedding::new(f32_embedding([1.0, 0.0]), embedding_spec_f32());

    let aligned = scorer
        .score(&target, &f32_embedding([1.0, 0.0]), &embedding_spec_f32())
        .unwrap();
    let repeated = scorer
        .score(&target, &f32_embedding([1.0, 0.0]), &embedding_spec_f32())
        .unwrap();
    let orthogonal = scorer
        .score(&target, &f32_embedding([0.0, 1.0]), &embedding_spec_f32())
        .unwrap();
    let opposite = scorer
        .score(&target, &f32_embedding([-1.0, 0.0]), &embedding_spec_f32())
        .unwrap();
    assert_eq!(aligned, repeated);
    assert!(aligned > orthogonal);
    assert!(orthogonal > opposite);

    let f64_target = EncodedTargetEmbedding::new(f64_embedding([1.0, 0.0]), embedding_spec_f64());
    let f64_score = scorer
        .score(
            &f64_target,
            &f64_embedding([1.0, 0.0]),
            &embedding_spec_f64(),
        )
        .unwrap();
    assert_eq!(
        f64_score,
        scorer
            .score(
                &f64_target,
                &f64_embedding([1.0, 0.0]),
                &embedding_spec_f64()
            )
            .unwrap()
    );

    let non_finite_f64_target =
        EncodedTargetEmbedding::new(f64_embedding([f64::NAN, 0.0]), embedding_spec_f64());
    let non_finite_f64_target_error = scorer
        .score(
            &non_finite_f64_target,
            &f64_embedding([1.0, 0.0]),
            &embedding_spec_f64(),
        )
        .unwrap_err();
    assert!(matches!(
        non_finite_f64_target_error,
        DefaultPolicyError::NonFiniteValue {
            role: "target",
            index: 0
        }
    ));

    let non_finite_f64_candidate_error = scorer
        .score(
            &f64_target,
            &f64_embedding([f64::NEG_INFINITY, 0.0]),
            &embedding_spec_f64(),
        )
        .unwrap_err();
    assert!(matches!(
        non_finite_f64_candidate_error,
        DefaultPolicyError::NonFiniteValue {
            role: "candidate",
            index: 0
        }
    ));

    let overflowing_f64_target =
        EncodedTargetEmbedding::new(f64_embedding([1.0e308, 1.0e308]), embedding_spec_f64());
    let non_finite_score_error = scorer
        .score(
            &overflowing_f64_target,
            &f64_embedding([1.0e308, 1.0e308]),
            &embedding_spec_f64(),
        )
        .unwrap_err();
    assert_eq!(non_finite_score_error, DefaultPolicyError::NonFiniteScore);

    let unsupported_target = EncodedTargetEmbedding::new(i8_embedding([1, 0]), embedding_spec_i8());
    let unsupported_error = scorer
        .score(
            &unsupported_target,
            &i8_embedding([1, 0]),
            &embedding_spec_i8(),
        )
        .unwrap_err();
    assert!(matches!(
        unsupported_error,
        DefaultPolicyError::UnsupportedEncoding { .. }
    ));

    let length_error = scorer
        .score(&target, &[0_u8; 4], &embedding_spec_f32())
        .unwrap_err();
    assert!(matches!(
        length_error,
        DefaultPolicyError::InvalidByteLength {
            role: "candidate",
            ..
        }
    ));

    let zero_target = EncodedTargetEmbedding::new(f32_embedding([0.0, 0.0]), embedding_spec_f32());
    let zero_target_error = scorer
        .score(
            &zero_target,
            &f32_embedding([1.0, 0.0]),
            &embedding_spec_f32(),
        )
        .unwrap_err();
    assert_eq!(
        zero_target_error,
        DefaultPolicyError::ZeroMagnitude { role: "target" }
    );

    let zero_candidate_error = scorer
        .score(&target, &f32_embedding([0.0, 0.0]), &embedding_spec_f32())
        .unwrap_err();
    assert_eq!(
        zero_candidate_error,
        DefaultPolicyError::ZeroMagnitude { role: "candidate" }
    );

    let non_finite_target =
        EncodedTargetEmbedding::new(f32_embedding([f32::NAN, 0.0]), embedding_spec_f32());
    let non_finite_target_error = scorer
        .score(
            &non_finite_target,
            &f32_embedding([1.0, 0.0]),
            &embedding_spec_f32(),
        )
        .unwrap_err();
    assert!(matches!(
        non_finite_target_error,
        DefaultPolicyError::NonFiniteValue {
            role: "target",
            index: 0
        }
    ));

    let non_finite_candidate_error = scorer
        .score(
            &target,
            &f32_embedding([f32::INFINITY, 0.0]),
            &embedding_spec_f32(),
        )
        .unwrap_err();
    assert!(matches!(
        non_finite_candidate_error,
        DefaultPolicyError::NonFiniteValue {
            role: "candidate",
            index: 0
        }
    ));

    let overflow_spec = EmbeddingSpec {
        dims: u64::MAX,
        encoding: "f32le".into(),
    };
    let overflow_target = EncodedTargetEmbedding::new(Vec::new(), overflow_spec.clone());
    let overflow_error = scorer
        .score(&overflow_target, &[], &overflow_spec)
        .unwrap_err();
    assert_eq!(
        overflow_error,
        DefaultPolicyError::DimensionOverflow {
            encoding: "f32le".into(),
            dims: u64::MAX,
        }
    );
}

#[test]
fn val_search_025_scoring_failures_fail_explicitly() {
    let store = MemoryBlockStore::default();
    let leaf_root_id = store
        .put(&leaf_block(i8_embedding([4, 0]), "root"))
        .unwrap();

    let leaf_error = Searcher::new(AcceptAllCompatibility, FailingScorer)
        .search(&leaf_root_id, &(), 1, 1, &store)
        .unwrap_err();
    assert!(matches!(leaf_error, SearchError::ScoringFailure { .. }));

    let branch_child = store
        .put(&leaf_block(i8_embedding([6, 0]), "child"))
        .unwrap();
    let branch_root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([9, 0], branch_child)],
        ))
        .unwrap();

    let branch_error = Searcher::new(AcceptAllCompatibility, FailingScorer)
        .search(&branch_root_id, &(), 1, 1, &store)
        .unwrap_err();
    assert!(matches!(branch_error, SearchError::ScoringFailure { .. }));
}

#[test]
fn val_search_026_expanded_children_are_not_reexpanded_in_later_rounds() {
    let store = MemoryBlockStore::default();
    let shared = store
        .put(&leaf_block(i8_embedding([9, 0]), "shared"))
        .unwrap();
    let fresh = store
        .put(&leaf_block(i8_embedding([8, 0]), "fresh"))
        .unwrap();
    let intermediate = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([9, 0], shared), branch_entry([8, 0], fresh)],
        ))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([10, 0], shared),
                branch_entry([7, 0], intermediate),
            ],
        ))
        .unwrap();

    let result = Searcher::new(AcceptAllCompatibility, FirstByteScorer)
        .search(&root_id, &(), 1, 2, &store)
        .unwrap();

    assert_eq!(result.leaves.len(), 2);
    assert_eq!(result.leaves[0].entry.content.body, b"shared".to_vec());
    assert_eq!(result.leaves[1].entry.content.body, b"fresh".to_vec());
    assert_eq!(store.get_count(&shared), 1);
    assert_eq!(store.get_count(&intermediate), 1);
    assert_eq!(store.get_count(&fresh), 1);
}

#[test]
fn val_search_027_expanded_child_branches_are_removed_from_later_frontiers() {
    let store = MemoryBlockStore::default();
    let duplicated = store
        .put(&leaf_block(i8_embedding([9, 0]), "duplicated"))
        .unwrap();
    let trailing = store
        .put(&leaf_block(i8_embedding([7, 0]), "trailing"))
        .unwrap();
    let intermediate = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([7, 0], trailing)],
        ))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([10, 0], duplicated),
                branch_entry([9, 0], duplicated),
                branch_entry([8, 0], intermediate),
            ],
        ))
        .unwrap();

    let result = Searcher::new(AcceptAllCompatibility, FirstByteScorer)
        .search(&root_id, &(), 1, 2, &store)
        .unwrap();

    assert_eq!(result.leaves.len(), 2);
    assert_eq!(result.leaves[0].entry.content.body, b"duplicated".to_vec());
    assert_eq!(result.leaves[1].entry.content.body, b"trailing".to_vec());
    assert_eq!(store.get_count(&duplicated), 1);
    assert_eq!(store.get_count(&intermediate), 1);
    assert_eq!(store.get_count(&trailing), 1);
}

#[test]
fn val_search_017_workspace_contains_the_search_crate() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let crate_manifest = std::fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
    let workspace_manifest =
        std::fs::read_to_string(manifest_dir.join("..").join("..").join("Cargo.toml")).unwrap();

    assert!(crate_manifest.contains("name = \"lexongraph-search\""));
    assert!(workspace_manifest.contains("crates/lexongraph-search"));
}

#[test]
fn val_search_018_repository_includes_search_verification_artifacts() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));

    assert!(
        manifest_dir
            .join("tests")
            .join("spec_validation.rs")
            .is_file()
    );
    assert!(
        manifest_dir
            .join("tests")
            .join("conformance_feature.rs")
            .is_file()
    );
}

fn map_get_error(error: lexongraph_block::BlockError) -> BlockStoreError {
    match error {
        lexongraph_block::BlockError::HashMismatch { expected, actual } => {
            BlockStoreError::IntegrityMismatch { expected, actual }
        }
        other => BlockStoreError::MalformedContent(other),
    }
}

fn embedding_spec_i8() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: "i8".into(),
    }
}

fn i8_embedding(bytes: [u8; 2]) -> Vec<u8> {
    bytes.to_vec()
}

fn embedding_spec_f32() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: "f32le".into(),
    }
}

fn f32_embedding(values: [f32; 2]) -> Vec<u8> {
    values
        .into_iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

fn embedding_spec_f64() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: "f64le".into(),
    }
}

fn f64_embedding(values: [f64; 2]) -> Vec<u8> {
    values
        .into_iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

fn branch_entry(embedding: [u8; 2], child: BlockHash) -> BranchEntry {
    BranchEntry {
        embedding: embedding.to_vec(),
        child,
    }
}

fn branch_block(spec: EmbeddingSpec, entries: Vec<BranchEntry>) -> Block {
    branch_block_at_level(1, spec, entries)
}

fn branch_block_at_level(level: u64, spec: EmbeddingSpec, entries: Vec<BranchEntry>) -> Block {
    Block::Branch(build_branch_block(VERSION_1, level, spec, entries, None).unwrap())
}

fn leaf_block(embedding: Vec<u8>, body: &str) -> Block {
    leaf_block_with_spec(embedding_spec_i8(), embedding, body)
}

fn leaf_block_with_spec(spec: EmbeddingSpec, embedding: Vec<u8>, body: &str) -> Block {
    Block::Leaf(
        build_leaf_block(
            VERSION_1,
            spec,
            vec![LeafEntry {
                embedding,
                metadata: vec![],
                content: Content {
                    media_type: "text/plain".into(),
                    body: body.as_bytes().to_vec(),
                },
            }],
            None,
        )
        .unwrap(),
    )
}
