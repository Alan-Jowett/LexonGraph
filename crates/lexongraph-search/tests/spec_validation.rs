// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;

use ciborium::ser::into_writer;
use ciborium::value::{Integer, Value};
use lexongraph_block::{
    Block, BlockHash, BranchEntry, Content, EbcpDescriptor, EbcpRotation, EmbeddingSpec, LeafEntry,
    VERSION_1, build_branch_block, build_leaf_block, compute_block_hash, ebcp_extension_map,
    parse_branch_ebcp_descriptor, reconstruct_logical_branch_embedding_f32,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_search::{
    CandidateScorer, DefaultCandidateScorer, DefaultEmbeddingCompatibility, DefaultPolicyError,
    DefaultTopWFrontierSelector, EmbeddingCompatibility, EncodedTargetEmbedding,
    ExpandableFrontierCandidate, FrontierSelector, GeometryAwareFrontierSelector,
    PUBLISHED_PROFILE_V0_1_0, PUBLISHED_PROFILE_V0_2_0, ProfiledSearcher, PublishedProfileVersion,
    SearchError, SearchProfileError, SearchResult, SearchTelemetryObserver, SearchTerminationKind,
    Searcher, published_search_profile,
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
    fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        self.blocks.borrow_mut().insert(*block_id, block_bytes.to_vec());
        Ok(())
    }

    fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        *self.gets.borrow_mut().entry(*block_id).or_default() += 1;
        Ok(self.blocks.borrow().get(block_id).cloned())
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

    fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        self.inner.put_block_bytes(block_id, block_bytes)
    }

    fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let configured = *self.fail_on.borrow();
        if configured.is_none() || configured == Some(*block_id) {
            return Err(BlockStoreError::BackendFailure(self.fail_message.into()));
        }

        self.inner.get_block_bytes(block_id)
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

#[derive(Default)]
struct SummaryRecorder {
    summaries: RefCell<Vec<lexongraph_search::SearchTelemetrySummary>>,
}

impl SummaryRecorder {
    fn summaries(&self) -> Vec<lexongraph_search::SearchTelemetrySummary> {
        self.summaries.borrow().clone()
    }
}

impl SearchTelemetryObserver for SummaryRecorder {
    fn record_summary(&self, summary: &lexongraph_search::SearchTelemetrySummary) {
        self.summaries.borrow_mut().push(summary.clone());
    }
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

#[derive(Clone, Debug)]
struct SelectorFixtureError(String);

impl std::fmt::Display for SelectorFixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SelectorFixtureError {}

#[derive(Clone, Debug)]
struct FixedFrontierSelector {
    selected: Vec<BlockHash>,
}

impl<Score> FrontierSelector<Score> for FixedFrontierSelector {
    type Error = SelectorFixtureError;

    fn select(
        &self,
        _: &[ExpandableFrontierCandidate<'_, Score>],
        _: usize,
    ) -> Result<Vec<BlockHash>, Self::Error> {
        Ok(self.selected.clone())
    }
}

#[derive(Clone, Copy, Debug)]
struct FailingFrontierSelector;

impl<Score> FrontierSelector<Score> for FailingFrontierSelector {
    type Error = SelectorFixtureError;

    fn select(
        &self,
        _: &[ExpandableFrontierCandidate<'_, Score>],
        _: usize,
    ) -> Result<Vec<BlockHash>, Self::Error> {
        Err(SelectorFixtureError("beam planner unavailable".into()))
    }
}

#[derive(Clone, Copy)]
struct FirstF32ComponentScorer;

impl CandidateScorer<()> for FirstF32ComponentScorer {
    type Error = FixtureError;
    type Score = i32;

    fn score(
        &self,
        _: &(),
        candidate_embedding: &[u8],
        _: &EmbeddingSpec,
    ) -> Result<Self::Score, Self::Error> {
        Ok((decode_first_f32(candidate_embedding) * 1000.0).round() as i32)
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
fn val_search_028_telemetry_surface_is_optional_and_trait_based() {
    let store = MemoryBlockStore::default();
    let root_id = store
        .put(&leaf_block(i8_embedding([7, 0]), "telemetry"))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);
    let recorder = SummaryRecorder::default();

    let without_observer = searcher.search(&root_id, &(), 1, 1, &store).unwrap();
    let with_observer = searcher
        .search_with_observer(&root_id, &(), 1, 1, &store, &recorder)
        .unwrap();

    assert_eq!(without_observer, with_observer);
    let summaries = recorder.summaries();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].beam_width, 1);
    assert_eq!(summaries[0].termination, SearchTerminationKind::Success);
}

#[test]
fn val_search_029_telemetry_reporting_is_deterministic() {
    let store = MemoryBlockStore::default();
    let left = store
        .put(&leaf_block(i8_embedding([9, 0]), "left"))
        .unwrap();
    let right = store
        .put(&leaf_block(i8_embedding([8, 0]), "right"))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([9, 0], left), branch_entry([8, 0], right)],
        ))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);

    let (first_result, first_summary) = searcher
        .search_with_telemetry(&root_id, &(), 2, 2, &store)
        .unwrap();
    let (second_result, second_summary) = searcher
        .search_with_telemetry(&root_id, &(), 2, 2, &store)
        .unwrap();

    assert_eq!(first_result, second_result);
    assert_eq!(first_summary, second_summary);
    assert_eq!(first_summary.distinct_blocks_visited, 3);
    assert_eq!(first_summary.max_routing_depth, 1);
    assert_eq!(first_summary.termination, SearchTerminationKind::Success);
}

#[test]
fn val_search_030_telemetry_does_not_change_results_or_failures() {
    let store = MemoryBlockStore::default();
    let child = store
        .put(&leaf_block(i8_embedding([9, 0]), "child"))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([9, 0], child)],
        ))
        .unwrap();
    let searcher = Searcher::new(AcceptAllCompatibility, FirstByteScorer);
    let recorder = SummaryRecorder::default();

    let baseline = searcher.search(&root_id, &(), 1, 1, &store).unwrap();
    let observed = searcher
        .search_with_observer(&root_id, &(), 1, 1, &store, &recorder)
        .unwrap();
    assert_eq!(baseline, observed);

    let failing_searcher = Searcher::new(AcceptAllCompatibility, FailingScorer);
    let plain_error = failing_searcher
        .search(&root_id, &(), 1, 1, &store)
        .unwrap_err();
    let observed_error = failing_searcher
        .search_with_observer(&root_id, &(), 1, 1, &store, &recorder)
        .unwrap_err();
    assert_eq!(plain_error, observed_error);
    assert!(matches!(observed_error, SearchError::ScoringFailure { .. }));

    let summaries = recorder.summaries();
    assert_eq!(summaries.len(), 2);
    assert_eq!(summaries[0].termination, SearchTerminationKind::Success);
    assert_eq!(
        summaries[1].termination,
        SearchTerminationKind::ScoringFailure
    );
}

#[test]
fn regression_exhausted_telemetry_reports_exhausted_termination() {
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
    let recorder = SummaryRecorder::default();

    let telemetry_error = searcher
        .search_with_telemetry(&root_id, &(), 1, 2, &store)
        .unwrap_err();
    let observed_error = searcher
        .search_with_observer(&root_id, &(), 1, 2, &store, &recorder)
        .unwrap_err();

    assert_eq!(telemetry_error, observed_error);
    assert_eq!(recorder.summaries().len(), 1);
    assert_eq!(
        recorder.summaries()[0].termination,
        SearchTerminationKind::Exhausted
    );
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

#[test]
fn val_search_031_published_search_profile_v0_1_0_is_declared_explicitly() {
    let profile = published_search_profile(PUBLISHED_PROFILE_V0_1_0).unwrap();
    assert_eq!(profile.version(), PublishedProfileVersion::new(0, 1, 0));
}

#[test]
fn val_search_032_unknown_published_search_profile_is_rejected() {
    let error = published_search_profile(PublishedProfileVersion::new(9, 9, 9)).unwrap_err();
    assert!(matches!(
        error,
        SearchProfileError::UnsupportedPublishedProfileVersion(version)
            if version == PublishedProfileVersion::new(9, 9, 9)
    ));
}

#[test]
fn val_search_033_profiled_searcher_matches_the_default_policy_bundle() {
    let store = MemoryBlockStore::default();
    let root_id = store
        .put(&leaf_block_with_spec(
            embedding_spec_f32(),
            f32_embedding([1.0, 0.0]),
            "default",
        ))
        .unwrap();

    let target_bytes = f32_embedding([1.0, 0.0]);
    let target_spec = embedding_spec_f32();
    let profiled = ProfiledSearcher::new(PUBLISHED_PROFILE_V0_1_0).unwrap();
    let profiled_result = profiled
        .search(
            &root_id,
            target_bytes.clone(),
            target_spec.clone(),
            1,
            1,
            &store,
        )
        .unwrap();

    let default_target = EncodedTargetEmbedding::new(target_bytes, target_spec);
    let default_searcher = Searcher::new(DefaultEmbeddingCompatibility, DefaultCandidateScorer);
    let default_result = default_searcher
        .search(&root_id, &default_target, 1, 1, &store)
        .unwrap();

    assert_eq!(profiled_result, default_result);
}

#[test]
fn val_search_034_ebcp_rotated_branch_blocks_preserve_uncompressed_results() {
    assert_eq!(
        run_single_result_search(|store| ebcp_fixture(store, "pca-rot-f32le")).unwrap(),
        run_single_result_search(uncompressed_fixture).unwrap()
    );
}

#[test]
fn val_search_035_ebcp_delta_branch_blocks_preserve_uncompressed_results() {
    assert_eq!(
        run_single_result_search(|store| ebcp_fixture(store, "pca-rot-delta-f32le")).unwrap(),
        run_single_result_search(uncompressed_fixture).unwrap()
    );
}

#[test]
fn val_search_036_uniform_quantized_ebcp_branch_blocks_remain_searchable() {
    let result = run_single_result_search(|store| ebcp_fixture(store, "pca-rot-delta-uq")).unwrap();
    assert_eq!(result.leaves[0].entry.content.body, b"left");
}

#[test]
fn val_search_037_variable_quantized_ebcp_branch_blocks_remain_searchable() {
    let result =
        run_single_result_search(|store| ebcp_fixture(store, "pca-rot-delta-vbq")).unwrap();
    assert_eq!(result.leaves[0].entry.content.body, b"left");
}

#[test]
fn val_search_037b_ambient_uniform_quantized_branch_blocks_remain_searchable() {
    let result = run_single_result_search(|store| ebcp_fixture(store, "ambient-delta-uq")).unwrap();
    assert_eq!(result.leaves[0].entry.content.body, b"left");
}

#[test]
fn val_search_038_malformed_ebcp_blocks_fail_through_invalid_block_path() {
    let store = MemoryBlockStore::default();
    let (left_id, right_id) = put_leaf_fixture(&store);
    let malformed_root = malformed_ebcp_root(left_id, right_id);
    let root_hash = compute_block_hash(&malformed_root);
    store.raw_insert(root_hash, malformed_root);

    let target = EncodedTargetEmbedding::new(f32_embedding([1.0, 0.0]), embedding_spec_f32());
    let searcher = Searcher::new(DefaultEmbeddingCompatibility, DefaultCandidateScorer);
    let error = searcher
        .search(&root_hash, &target, 1, 1, &store)
        .unwrap_err();
    assert!(matches!(error, SearchError::MalformedBlock { .. }));
}

#[test]
fn val_search_039_public_block_reconstruction_matches_search_branch_ranking() {
    for encoding in [
        "pca-rot-f32le",
        "pca-rot-delta-f32le",
        "pca-rot-delta-uq",
        "pca-rot-delta-vbq",
        "ambient-delta-uq",
    ] {
        let store = MemoryBlockStore::default();
        let root = ebcp_fixture(&store, encoding);
        let Block::Branch(root_branch) = &root else {
            panic!("expected EBCP fixture root to be a branch block");
        };
        let descriptor =
            parse_branch_ebcp_descriptor(&root_branch.embedding_spec, root_branch.ext.as_ref())
                .unwrap()
                .unwrap();
        let best_child = root_branch
            .entries
            .iter()
            .map(|entry| {
                let reconstructed = reconstruct_logical_branch_embedding_f32(
                    &entry.embedding,
                    &root_branch.embedding_spec,
                    Some(&descriptor),
                )
                .unwrap();
                (entry.child, reconstructed[0])
            })
            .max_by(|left, right| left.1.total_cmp(&right.1))
            .unwrap()
            .0;

        let root_id = store.put(&root).unwrap();
        let searcher = Searcher::new(DefaultEmbeddingCompatibility, DefaultCandidateScorer);
        let target = EncodedTargetEmbedding::new(
            f32_embedding([1.0, 0.0]),
            descriptor.logical_embedding_spec,
        );
        let result = searcher.search(&root_id, &target, 1, 1, &store).unwrap();
        assert_eq!(result.leaves[0].leaf_block_id, best_child, "{encoding}");
        assert_eq!(result.leaves[0].entry.content.body, b"left", "{encoding}");
    }
}

#[test]
fn val_search_040_frontier_selection_is_a_separate_policy_boundary() {
    let store = MemoryBlockStore::default();
    let top_child = store.put(&leaf_block(i8_embedding([9, 0]), "top")).unwrap();
    let alternate_child = store
        .put(&leaf_block(i8_embedding([100, 0]), "alternate"))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![
                branch_entry([9, 0], top_child),
                branch_entry([8, 0], alternate_child),
            ],
        ))
        .unwrap();

    let searcher = Searcher::with_frontier_selector(
        AcceptAllCompatibility,
        FirstByteScorer,
        FixedFrontierSelector {
            selected: vec![alternate_child],
        },
    );
    let result = searcher.search(&root_id, &(), 1, 1, &store).unwrap();

    assert_eq!(result.leaves[0].entry.content.body, b"alternate".to_vec());
}

#[test]
fn val_search_041_default_top_w_frontier_selector_matches_the_legacy_searcher() {
    let store = MemoryBlockStore::default();
    let left = store
        .put(&leaf_block(i8_embedding([9, 0]), "left"))
        .unwrap();
    let right = store
        .put(&leaf_block(i8_embedding([8, 0]), "right"))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([9, 0], left), branch_entry([8, 0], right)],
        ))
        .unwrap();

    let legacy = Searcher::new(AcceptAllCompatibility, FirstByteScorer)
        .search(&root_id, &(), 1, 1, &store)
        .unwrap();
    let explicit = Searcher::with_frontier_selector(
        AcceptAllCompatibility,
        FirstByteScorer,
        DefaultTopWFrontierSelector,
    )
    .search(&root_id, &(), 1, 1, &store)
    .unwrap();

    assert_eq!(legacy, explicit);
}

#[test]
fn val_search_042_geometry_aware_selector_is_deterministic() {
    let store = MemoryBlockStore::default();
    let root_id = fixed_width_recall_fixture(&store);
    let target = EncodedTargetEmbedding::new(f32_embedding([1.0, 0.0]), embedding_spec_f32());
    let searcher = Searcher::with_frontier_selector(
        DefaultEmbeddingCompatibility,
        DefaultCandidateScorer,
        GeometryAwareFrontierSelector,
    );

    let first = searcher.search(&root_id, &target, 2, 1, &store).unwrap();
    let second = searcher.search(&root_id, &target, 2, 1, &store).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.leaves[0].entry.content.body, b"target".to_vec());
}

#[test]
fn val_search_043_frontier_selection_can_use_frontier_geometry_not_just_rank() {
    let fixture_a = geometry_order_fixture([0.99, 0.10], [0.98, 0.09], [0.97, 0.70]);
    let fixture_b = geometry_order_fixture([0.99, 0.10], [0.98, 0.70], [0.97, 0.09]);

    assert_eq!(fixture_a.branch_scores, fixture_b.branch_scores);

    let geometry_result_a = run_geometry_order_fixture(&fixture_a);
    let geometry_result_b = run_geometry_order_fixture(&fixture_b);

    assert_eq!(
        sorted_leaf_bodies(&geometry_result_a),
        vec![b"a".to_vec(), b"c".to_vec()]
    );
    assert_eq!(
        sorted_leaf_bodies(&geometry_result_b),
        vec![b"a".to_vec(), b"b".to_vec()]
    );
}

#[test]
fn val_search_044_geometry_aware_profile_improves_fixed_width_recall_on_the_reference_fixture() {
    let store = MemoryBlockStore::default();
    let root_id = fixed_width_recall_fixture(&store);
    let target_bytes = f32_embedding([1.0, 0.0]);
    let target_spec = embedding_spec_f32();

    let legacy = ProfiledSearcher::new(PUBLISHED_PROFILE_V0_1_0)
        .unwrap()
        .search(
            &root_id,
            target_bytes.clone(),
            target_spec.clone(),
            2,
            1,
            &store,
        )
        .unwrap();
    let geometry = ProfiledSearcher::new(PUBLISHED_PROFILE_V0_2_0)
        .unwrap()
        .search(&root_id, target_bytes, target_spec, 2, 1, &store)
        .unwrap();

    assert_eq!(legacy.leaves[0].entry.content.body, b"decoy-a".to_vec());
    assert_eq!(geometry.leaves[0].entry.content.body, b"target".to_vec());
}

#[test]
fn val_search_045_published_search_profile_v0_2_0_is_declared_explicitly() {
    let profile = published_search_profile(PUBLISHED_PROFILE_V0_2_0).unwrap();
    assert_eq!(profile.version(), PublishedProfileVersion::new(0, 2, 0));
}

#[test]
fn val_search_046_frontier_selector_failures_are_explicit() {
    let store = MemoryBlockStore::default();
    let child = store
        .put(&leaf_block(i8_embedding([9, 0]), "child"))
        .unwrap();
    let root_id = store
        .put(&branch_block(
            embedding_spec_i8(),
            vec![branch_entry([9, 0], child)],
        ))
        .unwrap();
    let searcher = Searcher::with_frontier_selector(
        AcceptAllCompatibility,
        FirstByteScorer,
        FailingFrontierSelector,
    );
    let recorder = SummaryRecorder::default();

    let error = searcher
        .search_with_observer(&root_id, &(), 1, 1, &store, &recorder)
        .unwrap_err();

    assert!(matches!(
        error,
        SearchError::FrontierSelectionFailure { .. }
    ));
    assert_eq!(
        recorder.summaries()[0].termination,
        SearchTerminationKind::FrontierSelectionFailure
    );
}

fn run_single_result_search<F>(build_root: F) -> Result<SearchResult, SearchError>
where
    F: FnOnce(&MemoryBlockStore) -> Block,
{
    let store = MemoryBlockStore::default();
    let root = build_root(&store);
    let root_id = store.put(&root).unwrap();
    let target = EncodedTargetEmbedding::new(f32_embedding([1.0, 0.0]), embedding_spec_f32());
    let searcher = Searcher::new(DefaultEmbeddingCompatibility, DefaultCandidateScorer);
    searcher.search(&root_id, &target, 1, 1, &store)
}

struct GeometryOrderFixture {
    store: MemoryBlockStore,
    root_id: BlockHash,
    branch_scores: [i32; 3],
}

fn fixed_width_recall_fixture(store: &MemoryBlockStore) -> BlockHash {
    let target = store
        .put(&leaf_block_with_spec(
            embedding_spec_f32(),
            f32_embedding([1.0, 0.0]),
            "target",
        ))
        .unwrap();
    let decoy_a = store
        .put(&leaf_block_with_spec(
            embedding_spec_f32(),
            f32_embedding([0.95, 0.31]),
            "decoy-a",
        ))
        .unwrap();
    let decoy_b = store
        .put(&leaf_block_with_spec(
            embedding_spec_f32(),
            f32_embedding([0.94, 0.34]),
            "decoy-b",
        ))
        .unwrap();
    store
        .put(&branch_block_with_ext(
            1,
            embedding_spec_f32(),
            vec![
                BranchEntry {
                    embedding: f32_embedding([0.99, 0.10]),
                    child: decoy_a,
                },
                BranchEntry {
                    embedding: f32_embedding([0.98, 0.11]),
                    child: decoy_b,
                },
                BranchEntry {
                    embedding: f32_embedding([0.80, 0.60]),
                    child: target,
                },
            ],
            None,
        ))
        .unwrap()
}

fn geometry_order_fixture(a: [f32; 2], b: [f32; 2], c: [f32; 2]) -> GeometryOrderFixture {
    let store = MemoryBlockStore::default();
    let a_leaf = store
        .put(&leaf_block_with_spec(
            embedding_spec_f32(),
            f32_embedding([1.0, 0.0]),
            "a",
        ))
        .unwrap();
    let b_leaf = store
        .put(&leaf_block_with_spec(
            embedding_spec_f32(),
            f32_embedding([1.0, 0.0]),
            "b",
        ))
        .unwrap();
    let c_leaf = store
        .put(&leaf_block_with_spec(
            embedding_spec_f32(),
            f32_embedding([1.0, 0.0]),
            "c",
        ))
        .unwrap();
    let root_id = store
        .put(&branch_block_with_ext(
            1,
            embedding_spec_f32(),
            vec![
                BranchEntry {
                    embedding: f32_embedding(a),
                    child: a_leaf,
                },
                BranchEntry {
                    embedding: f32_embedding(b),
                    child: b_leaf,
                },
                BranchEntry {
                    embedding: f32_embedding(c),
                    child: c_leaf,
                },
            ],
            None,
        ))
        .unwrap();

    GeometryOrderFixture {
        store,
        root_id,
        branch_scores: [
            (a[0] * 1000.0).round() as i32,
            (b[0] * 1000.0).round() as i32,
            (c[0] * 1000.0).round() as i32,
        ],
    }
}

fn run_geometry_order_fixture(fixture: &GeometryOrderFixture) -> SearchResult {
    Searcher::with_frontier_selector(
        AcceptAllCompatibility,
        FirstF32ComponentScorer,
        GeometryAwareFrontierSelector,
    )
    .search(&fixture.root_id, &(), 2, 2, &fixture.store)
    .unwrap()
}

fn leaf_bodies(result: &SearchResult) -> Vec<Vec<u8>> {
    result
        .leaves
        .iter()
        .map(|leaf| leaf.entry.content.body.clone())
        .collect()
}

fn sorted_leaf_bodies(result: &SearchResult) -> Vec<Vec<u8>> {
    let mut bodies = leaf_bodies(result);
    bodies.sort();
    bodies
}

fn put_leaf_fixture(store: &MemoryBlockStore) -> (BlockHash, BlockHash) {
    let left_id = store
        .put(&leaf_block_with_spec(
            embedding_spec_f32(),
            f32_embedding([1.0, 0.0]),
            "left",
        ))
        .unwrap();
    let right_id = store
        .put(&leaf_block_with_spec(
            embedding_spec_f32(),
            f32_embedding([0.0, 1.0]),
            "right",
        ))
        .unwrap();
    (left_id, right_id)
}

fn uncompressed_fixture(store: &MemoryBlockStore) -> Block {
    let (left_id, right_id) = put_leaf_fixture(store);
    branch_block_with_ext(
        1,
        embedding_spec_f32(),
        vec![
            BranchEntry {
                embedding: f32_embedding([1.0, 0.0]),
                child: left_id,
            },
            BranchEntry {
                embedding: f32_embedding([0.0, 1.0]),
                child: right_id,
            },
        ],
        None,
    )
}

fn ebcp_fixture(store: &MemoryBlockStore, encoding: &str) -> Block {
    let (left_id, right_id) = put_leaf_fixture(store);
    let rotation = EbcpRotation {
        matrix_format: "f32le-row-major".into(),
        matrix: vec![1.0, 0.0, 0.0, 1.0],
    };
    let descriptor = match encoding {
        "pca-rot-f32le" => EbcpDescriptor {
            version: 1,
            logical_embedding_spec: embedding_spec_f32(),
            base_centroid: None,
            rotation: Some(rotation.clone()),
            quantization: None,
        },
        "pca-rot-delta-f32le" => EbcpDescriptor {
            version: 1,
            logical_embedding_spec: embedding_spec_f32(),
            base_centroid: Some(vec![0.0, 0.0]),
            rotation: Some(rotation.clone()),
            quantization: None,
        },
        "pca-rot-delta-uq" => EbcpDescriptor {
            version: 1,
            logical_embedding_spec: embedding_spec_f32(),
            base_centroid: Some(vec![0.0, 0.0]),
            rotation: Some(rotation.clone()),
            quantization: Some(lexongraph_block::EbcpQuantization::Uniform {
                bit_width: 12,
                scale_factors: vec![1.0 / 2047.0, 1.0 / 2047.0],
            }),
        },
        "pca-rot-delta-vbq" => EbcpDescriptor {
            version: 1,
            logical_embedding_spec: embedding_spec_f32(),
            base_centroid: Some(vec![0.0, 0.0]),
            rotation: Some(rotation),
            quantization: Some(lexongraph_block::EbcpQuantization::Variable {
                bit_widths: vec![12, 12],
                scale_factors: vec![1.0 / 2047.0, 1.0 / 2047.0],
            }),
        },
        "ambient-delta-uq" => EbcpDescriptor {
            version: 1,
            logical_embedding_spec: embedding_spec_f32(),
            base_centroid: Some(vec![0.0, 0.0]),
            rotation: None,
            quantization: Some(lexongraph_block::EbcpQuantization::Uniform {
                bit_width: 12,
                scale_factors: vec![1.0 / 2047.0, 1.0 / 2047.0],
            }),
        },
        other => panic!("unexpected fixture encoding {other}"),
    };

    let (left_embedding, right_embedding) = match encoding {
        "pca-rot-f32le" => (f32_embedding([1.0, 0.0]), f32_embedding([0.0, 1.0])),
        "pca-rot-delta-f32le" => (f32_embedding([1.0, 0.0]), f32_embedding([0.0, 1.0])),
        "pca-rot-delta-uq" | "pca-rot-delta-vbq" | "ambient-delta-uq" => (
            pack_quantized_fixture([1.0, 0.0], [12, 12]),
            pack_quantized_fixture([0.0, 1.0], [12, 12]),
        ),
        _ => unreachable!(),
    };

    branch_block_with_ext(
        1,
        EmbeddingSpec {
            dims: 2,
            encoding: encoding.into(),
        },
        vec![
            BranchEntry {
                embedding: left_embedding,
                child: left_id,
            },
            BranchEntry {
                embedding: right_embedding,
                child: right_id,
            },
        ],
        Some(ebcp_extension_map(&descriptor)),
    )
}

fn branch_block_with_ext(
    level: u64,
    spec: EmbeddingSpec,
    entries: Vec<BranchEntry>,
    ext: Option<Vec<(Value, Value)>>,
) -> Block {
    Block::Branch(build_branch_block(VERSION_1, level, spec, entries, ext).unwrap())
}

fn malformed_ebcp_root(left_id: BlockHash, right_id: BlockHash) -> Vec<u8> {
    encode_value(Value::Map(vec![
        (int_value(0), int_value(VERSION_1)),
        (int_value(1), int_value(1)),
        (
            int_value(2),
            Value::Map(vec![
                (int_value(0), int_value(2)),
                (int_value(1), Value::Text("pca-rot-f32le".into())),
            ]),
        ),
        (
            int_value(3),
            Value::Array(vec![
                raw_branch_entry(f32_embedding([1.0, 0.0]), left_id),
                raw_branch_entry(f32_embedding([0.0, 1.0]), right_id),
            ]),
        ),
    ]))
}

fn pack_quantized_fixture(values: [f32; 2], bit_widths: [u8; 2]) -> Vec<u8> {
    let scales = [1.0 / 2047.0, 1.0 / 2047.0];
    let total_bits = bit_widths
        .iter()
        .map(|width| usize::from(*width))
        .sum::<usize>();
    let mut bytes = vec![0u8; total_bits.div_ceil(8)];
    let mut offset = 0usize;
    for ((value, bit_width), scale) in values.into_iter().zip(bit_widths).zip(scales) {
        let qmax = ((1_i32 << (bit_width - 1)) - 1) as f64;
        let centered = (f64::from(value) / scale)
            .round_ties_even()
            .clamp(-qmax, qmax) as i32;
        let stored = u32::try_from(centered + (1_i32 << (bit_width - 1))).unwrap();
        for bit_index in 0..usize::from(bit_width) {
            let absolute = offset + bit_index;
            let byte_index = absolute / 8;
            let intra = absolute % 8;
            let bit = ((stored >> bit_index) & 1) as u8;
            bytes[byte_index] |= bit << intra;
        }
        offset += usize::from(bit_width);
    }
    bytes
}

fn raw_branch_entry(embedding: Vec<u8>, child: BlockHash) -> Value {
    Value::Map(vec![
        (int_value(0), Value::Bytes(embedding)),
        (int_value(1), Value::Bytes(child.as_bytes().to_vec())),
    ])
}

fn encode_value(value: Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    into_writer(&value, &mut bytes).unwrap();
    bytes
}

fn int_value(value: u64) -> Value {
    Value::Integer(Integer::from(value))
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

fn decode_first_f32(bytes: &[u8]) -> f32 {
    f32::from_le_bytes(bytes[..4].try_into().unwrap())
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
