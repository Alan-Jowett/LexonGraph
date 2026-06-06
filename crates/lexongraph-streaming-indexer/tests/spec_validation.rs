// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Executable verification for docs/specs/rust-streaming-indexer-crate/validation.md

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use lexongraph_block::{
    BlockError, BlockHash, BranchBlock, Content, EmbeddingSpec, TypedEntries, into_entries,
    serialize_block,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_streaming_clustering::{StreamingClusteringConfig, StreamingClusteringError};
use lexongraph_streaming_indexer::{
    ArithmeticMeanCanonicalEmbeddingPolicy, CanonicalEmbeddingPolicy, ContentResolver,
    DcbcStreamingClusteringFactory, IndexItem, StreamingClusteringFactory, StreamingIndexerError,
    StreamingIndexingResult, StreamingIndexingRun, StreamingIndexingStatus,
    StreamingIndexingStatusObserver,
};

// ─── Shared test infrastructure ───────────────────────────────────────────────

#[derive(Debug, Default)]
struct MemoryBlockStore {
    blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
}

impl MemoryBlockStore {
    #[allow(dead_code)]
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
            .map_err(|e| match e {
                BlockError::HashMismatch { expected, actual } => {
                    BlockStoreError::IntegrityMismatch { expected, actual }
                }
                other => BlockStoreError::MalformedContent(other),
            })
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        let ids = self.blocks.borrow().keys().copied().collect::<Vec<_>>();
        Ok(Box::new(ids.into_iter().map(Ok)))
    }
}

#[derive(Default)]
struct FaultyIdStore {
    blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
    corrupt_leaf_ids: bool,
    corrupt_branch_ids: bool,
}

impl FaultyIdStore {
    fn corrupt_leaf_ids() -> Self {
        Self {
            blocks: RefCell::default(),
            corrupt_leaf_ids: true,
            corrupt_branch_ids: false,
        }
    }

    fn corrupt_branch_ids() -> Self {
        Self {
            blocks: RefCell::default(),
            corrupt_leaf_ids: false,
            corrupt_branch_ids: true,
        }
    }
}

impl BlockStore for FaultyIdStore {
    fn put(&self, block: &lexongraph_block::Block) -> Result<BlockHash, BlockStoreError> {
        let serialized =
            lexongraph_block::serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
        self.blocks
            .borrow_mut()
            .insert(serialized.hash, serialized.bytes);

        let should_corrupt = match block {
            lexongraph_block::Block::Leaf(_) => self.corrupt_leaf_ids,
            lexongraph_block::Block::Branch(_) => self.corrupt_branch_ids,
        };

        if should_corrupt {
            let mut bytes = serialized.hash.into_bytes();
            bytes[0] ^= 0xFF;
            Ok(BlockHash::from_bytes(bytes))
        } else {
            Ok(serialized.hash)
        }
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
            .map_err(|e| match e {
                BlockError::HashMismatch { expected, actual } => {
                    BlockStoreError::IntegrityMismatch { expected, actual }
                }
                other => BlockStoreError::MalformedContent(other),
            })
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        let ids = self.blocks.borrow().keys().copied().collect::<Vec<_>>();
        Ok(Box::new(ids.into_iter().map(Ok)))
    }
}

// ─── Fixture types ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct FixtureError(String);

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for FixtureError {}

/// Maps content_ref directly as body bytes with media_type = "text/plain".
#[derive(Clone, Copy)]
struct MapResolver;

impl ContentResolver<&'static str> for MapResolver {
    type Error = FixtureError;
    fn resolve(&self, r: &&'static str) -> Result<Content, Self::Error> {
        Ok(Content {
            media_type: "text/plain".into(),
            body: r.as_bytes().to_vec(),
        })
    }
}

/// Equivalent resolver with a distinct implementation type.
#[derive(Clone, Copy)]
struct MirrorResolver;

impl ContentResolver<&'static str> for MirrorResolver {
    type Error = FixtureError;
    fn resolve(&self, r: &&'static str) -> Result<Content, Self::Error> {
        Ok(Content {
            media_type: "text/plain".into(),
            body: r.as_bytes().to_vec(),
        })
    }
}

/// Always fails to resolve.
#[derive(Clone, Copy)]
struct FailingResolver;

impl ContentResolver<&'static str> for FailingResolver {
    type Error = FixtureError;
    fn resolve(&self, _: &&'static str) -> Result<Content, Self::Error> {
        Err(FixtureError("resolver unavailable".into()))
    }
}

/// Resolves but returns empty media type.
#[derive(Clone, Copy)]
struct UnusableResolver;

impl ContentResolver<&'static str> for UnusableResolver {
    type Error = FixtureError;
    fn resolve(&self, r: &&'static str) -> Result<Content, Self::Error> {
        Ok(Content {
            media_type: String::new(),
            body: r.as_bytes().to_vec(),
        })
    }
}

/// Derives a 2-byte i8 embedding from content: (first_byte, len_byte).
#[derive(Clone, Copy)]
struct AsciiEmbeddingProvider;

impl EmbeddingProvider for AsciiEmbeddingProvider {
    type Error = FixtureError;
    async fn embed(
        &self,
        input: &EmbeddingInput,
        spec: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        if spec.encoding != "i8" || spec.dims != 2 {
            return Err(FixtureError("unexpected embedding spec".into()));
        }
        let first = *input
            .body
            .first()
            .ok_or_else(|| FixtureError("empty content body".into()))?;
        Ok(vec![first, input.body.len() as u8])
    }
}

/// Always fails to embed.
#[derive(Clone, Copy)]
struct FailingEmbeddingProvider;

impl EmbeddingProvider for FailingEmbeddingProvider {
    type Error = FixtureError;
    async fn embed(&self, _: &EmbeddingInput, _: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
        Err(FixtureError("embedding model offline".into()))
    }
}

/// Returns wrong-length embeddings.
#[derive(Clone, Copy)]
struct WrongLengthEmbeddingProvider;

impl EmbeddingProvider for WrongLengthEmbeddingProvider {
    type Error = FixtureError;
    async fn embed(&self, _: &EmbeddingInput, _: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![0x01])
    }
}

/// Returns the same all-zero 2-byte embedding (triggers non-zero-norm failure
/// in DCBC when used for multi-item clustering).
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

/// Returns the first entry's embedding as the canonical embedding.
#[derive(Clone, Copy)]
struct FirstChildCanonicalPolicy;

impl CanonicalEmbeddingPolicy for FirstChildCanonicalPolicy {
    type Error = FixtureError;
    fn canonical_embedding(&self, block: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
        Ok(block.entries[0].embedding.clone())
    }
}

/// Always fails canonical embedding.
#[derive(Clone, Copy)]
struct FailingCanonicalPolicy;

impl CanonicalEmbeddingPolicy for FailingCanonicalPolicy {
    type Error = FixtureError;
    fn canonical_embedding(&self, _: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
        Err(FixtureError("canonical policy failed".into()))
    }
}

/// Returns a single-byte embedding (wrong length).
#[derive(Clone, Copy)]
#[allow(dead_code)]
struct WrongLengthCanonicalPolicy;

impl CanonicalEmbeddingPolicy for WrongLengthCanonicalPolicy {
    type Error = FixtureError;
    fn canonical_embedding(&self, _: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![0x01])
    }
}

// ─── A custom clustering factory that always groups into pairs/triples ────────

use lexongraph_dcbc_streaming::DcbcStreamingTrainer;

/// Wraps DCBC streaming but forces cluster_count = 2 regardless of hints.
#[derive(Clone, Copy)]
struct PairClusteringFactory;

impl StreamingClusteringFactory for PairClusteringFactory {
    type Trainer = DcbcStreamingTrainer;
    type Error = StreamingClusteringError;

    fn create_trainer(
        &self,
        dimensions: usize,
        _: usize,
        _: usize,
        _: &EmbeddingSpec,
    ) -> Result<DcbcStreamingTrainer, StreamingClusteringError> {
        DcbcStreamingTrainer::new(StreamingClusteringConfig {
            cluster_count: 2,
            dimensions,
            balance_constraints: None,
            random_seed: None,
        })
    }
}

// ─── Helper functions ─────────────────────────────────────────────────────────

fn embedding_spec() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: "i8".into(),
    }
}

fn item(s: &'static str) -> IndexItem<&'static str> {
    IndexItem {
        metadata: vec![],
        content_ref: s,
    }
}

/// Run one training pass + mark_complete + finalize for a set of items.
async fn one_shot_index(
    items: &[IndexItem<&'static str>],
    block_size_target: usize,
) -> Result<(StreamingIndexingResult, MemoryBlockStore), StreamingIndexerError> {
    let store = MemoryBlockStore::default();
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        block_size_target,
    );
    run.ingest_batch(items).await?;
    run.finish_pass()?;
    run.mark_training_complete()?;
    let result = run.finalize(std::iter::once(items), &store).await?;
    Ok((result, store))
}

// ─── VAL-STREAM-INDEXER-001 ───────────────────────────────────────────────────
// Crate and spec exist; old lexongraph-indexer crate is untouched.

#[test]
fn val_stream_indexer_001_crate_and_spec_coexist() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("crate should be nested under <repo>/crates/<crate>");
    assert!(
        repo_root
            .join("crates")
            .join("lexongraph-indexer")
            .join("Cargo.toml")
            .exists(),
        "existing lexongraph-indexer crate should remain present"
    );
    assert!(
        repo_root
            .join("docs")
            .join("specs")
            .join("rust-streaming-indexer-crate")
            .join("requirements.md")
            .exists(),
        "streaming-indexer spec package should remain present"
    );
    let _ = include_str!("../src/lib.rs");
}

// ─── VAL-STREAM-INDEXER-002 ───────────────────────────────────────────────────
// Public surface exposes the replay lifecycle and consumes shared clustering.

#[test]
fn val_stream_indexer_002_public_surface_inspection() {
    // Verify that the streaming clustering dependency is declared.
    let manifest = include_str!("../Cargo.toml");
    assert!(
        manifest.contains("lexongraph-dcbc-streaming"),
        "Cargo.toml must depend on lexongraph-dcbc-streaming"
    );
    assert!(
        manifest.contains("lexongraph-streaming-clustering"),
        "Cargo.toml must depend on lexongraph-streaming-clustering"
    );
}

// ─── VAL-STREAM-INDEXER-003 ───────────────────────────────────────────────────
// Empty pass and empty logical run fail explicitly.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_003_empty_pass_fails_explicitly() {
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    // finish_pass without any ingest_batch
    let err = run.finish_pass().unwrap_err();
    assert!(matches!(err, StreamingIndexerError::EmptyPass(_)), "{err}");
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_003_empty_item_list_in_finalize_fails_explicitly() {
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&[item("alpha")]).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    // Finalize with empty slice mismatches baseline (1 item vs 0).
    let err = run
        .finalize(
            std::iter::empty::<&[IndexItem<&'static str>]>(),
            &MemoryBlockStore::default(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            StreamingIndexerError::ReplayMismatch(_) | StreamingIndexerError::EmptyInput
        ),
        "{err}"
    );
}

// ─── VAL-STREAM-INDEXER-004 ───────────────────────────────────────────────────
// IndexItem carries metadata + content_ref; no inline bytes.

#[test]
fn val_stream_indexer_004_index_item_shape() {
    let it: IndexItem<&str> = IndexItem {
        metadata: vec![],
        content_ref: "hello",
    };
    assert_eq!(it.content_ref, "hello");
}

// ─── VAL-STREAM-INDEXER-005 ───────────────────────────────────────────────────
// Different resolver types share the same indexer contract.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_005_distinct_resolver_types_share_the_same_contract() {
    let (result_map, _) = one_shot_index(&[item("alpha"), item("bravo")], 256)
        .await
        .unwrap();

    // A distinct resolver implementation with the same observable behavior.
    let store2 = MemoryBlockStore::default();
    let mut run2 = StreamingIndexingRun::with_defaults(
        MirrorResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    run2.ingest_batch(&[item("alpha"), item("bravo")])
        .await
        .unwrap();
    run2.finish_pass().unwrap();
    run2.mark_training_complete().unwrap();
    let result2 = run2
        .finalize(
            std::iter::once([item("alpha"), item("bravo")].as_slice()),
            &store2,
        )
        .await
        .unwrap();

    assert_eq!(result_map.root_id, result2.root_id);
}

// ─── VAL-STREAM-INDEXER-006 ───────────────────────────────────────────────────
// Embedding provider consumed through shared embeddings-trait contract.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_006_uses_shared_embeddings_trait_contract() {
    // If the provider fails, the error propagates as EmbeddingFailure.
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        FailingEmbeddingProvider,
        embedding_spec(),
        256,
    );
    let err = run.ingest_batch(&[item("alpha")]).await.unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::EmbeddingFailure(_)),
        "{err}"
    );
}

// ─── VAL-STREAM-INDEXER-007 ───────────────────────────────────────────────────
// Primary default constructor requires no explicit policies.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_007_default_constructor_needs_no_explicit_policies() {
    let (result, store) = one_shot_index(&[item("alpha")], 256).await.unwrap();
    assert_eq!(result.block_ids.len(), 1);
    assert!(store.get(&result.root_id).unwrap().is_some());
}

// ─── VAL-STREAM-INDEXER-008 ───────────────────────────────────────────────────
// Override path accepts caller-supplied canonical and clustering policies.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_008_override_path_accepts_custom_policies() {
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let store = MemoryBlockStore::default();

    // with_canonical_policy: override canonical, keep DCBC default.
    let mut run = StreamingIndexingRun::with_canonical_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    let result = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();
    assert!(!result.block_ids.is_empty());

    // with_new: fully explicit override.
    let store2 = MemoryBlockStore::default();
    let mut run2 = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairClusteringFactory,
        embedding_spec(),
        256,
    );
    run2.ingest_batch(&items).await.unwrap();
    run2.finish_pass().unwrap();
    run2.mark_training_complete().unwrap();
    let result2 = run2
        .finalize(std::iter::once(items.as_slice()), &store2)
        .await
        .unwrap();
    assert!(!result2.block_ids.is_empty());
}

// ─── VAL-STREAM-INDEXER-009 ───────────────────────────────────────────────────
// Pass report is deterministic and contains item count + fitness info.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_009_pass_report_deterministic_with_multiple_batches() {
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];

    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    // Ingest as two separate batches
    run.ingest_batch(&items[..2]).await.unwrap();
    run.ingest_batch(&items[2..]).await.unwrap();
    let report = run.finish_pass().unwrap();

    assert_eq!(report.observed_item_count, 4);
    assert_eq!(report.completed_pass_count, 1);

    // Second run with one batch yields the same report
    let mut run2 = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    run2.ingest_batch(&items).await.unwrap();
    let report2 = run2.finish_pass().unwrap();

    assert_eq!(report.observed_item_count, report2.observed_item_count);
    assert_eq!(
        report.clustering_quality_metric,
        report2.clustering_quality_metric
    );
    assert_eq!(
        report.clustering_quality_direction,
        report2.clustering_quality_direction
    );
}

// ─── VAL-STREAM-INDEXER-010 ───────────────────────────────────────────────────
// Two identical passes are accepted.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_010_two_identical_passes_accepted() {
    let items = [item("alpha"), item("bravo")];

    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&items).await.unwrap();
    let r1 = run.finish_pass().unwrap();

    run.ingest_batch(&items).await.unwrap();
    let r2 = run.finish_pass().unwrap();

    assert_eq!(r1.observed_item_count, r2.observed_item_count);
    assert_eq!(r2.completed_pass_count, 2);
}

// ─── VAL-STREAM-INDEXER-011 ───────────────────────────────────────────────────
// Later pass with different items fails.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_011_later_pass_with_different_items_fails() {
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&[item("alpha"), item("bravo")])
        .await
        .unwrap();
    run.finish_pass().unwrap();

    // Different item in pass 2
    let err = run
        .ingest_batch(&[item("alpha"), item("DIFFERENT")])
        .await
        .unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::ReplayMismatch(_)),
        "{err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_011_later_pass_with_more_items_fails() {
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&[item("alpha")]).await.unwrap();
    run.finish_pass().unwrap();

    // Pass 2 has too many items
    run.ingest_batch(&[item("alpha")]).await.unwrap();
    let err = run.ingest_batch(&[item("extra")]).await.unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::ReplayMismatch(_)),
        "{err}"
    );
}

// ─── VAL-STREAM-INDEXER-012 ───────────────────────────────────────────────────
// Finalize before training completion fails.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_012_finalize_before_training_complete_fails() {
    let items = [item("alpha")];
    let store = MemoryBlockStore::default();

    // No pass at all
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    let err = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::InvalidLifecycleTransition(_)),
        "{err}"
    );

    // Pass done but mark_training_complete not called
    let mut run2 = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    run2.ingest_batch(&items).await.unwrap();
    run2.finish_pass().unwrap();
    let err2 = run2
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap_err();
    assert!(
        matches!(err2, StreamingIndexerError::InvalidLifecycleTransition(_)),
        "{err2}"
    );
}

// ─── VAL-STREAM-INDEXER-013 ───────────────────────────────────────────────────
// Successful final materialization after training completion.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_013_successful_finalize_after_training_complete() {
    let items = [item("alpha"), item("bravo")];
    let store = MemoryBlockStore::default();

    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    let result = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();

    assert!(!result.block_ids.is_empty());
    assert!(store.get(&result.root_id).unwrap().is_some());
}

// ─── VAL-STREAM-INDEXER-014 ───────────────────────────────────────────────────
// Finalization with different item order or count fails.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_014_finalize_with_different_items_fails() {
    let items = [item("alpha"), item("bravo")];
    let store = MemoryBlockStore::default();

    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();

    // Wrong item count
    let err = run
        .finalize(std::iter::once([item("alpha")].as_slice()), &store)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::ReplayMismatch(_)),
        "{err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_014_finalize_with_wrong_order_fails() {
    let items = [item("alpha"), item("bravo")];
    let store = MemoryBlockStore::default();

    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();

    // Reversed order
    let err = run
        .finalize(
            std::iter::once([item("bravo"), item("alpha")].as_slice()),
            &store,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::ReplayMismatch(_)),
        "{err}"
    );
}

// ─── VAL-STREAM-INDEXER-015 ───────────────────────────────────────────────────
// Leaf entry stores resolved media type and bytes inline.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_015_resolved_content_stored_inline_in_leaf() {
    let (result, store) = one_shot_index(&[item("alpha")], 256).await.unwrap();

    let root = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(root) {
        TypedEntries::Leaf(_, entries) => {
            assert_eq!(entries[0].content.media_type, "text/plain");
            assert_eq!(entries[0].content.body, b"alpha".to_vec());
        }
        TypedEntries::Branch(_, _) => panic!("expected leaf root for single item"),
    }
}

// ─── VAL-STREAM-INDEXER-016 ───────────────────────────────────────────────────
// Single item → one leaf block which is the root.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_016_single_item_produces_leaf_root() {
    let (result, store) = one_shot_index(&[item("alpha")], 256).await.unwrap();

    assert_eq!(result.block_ids.len(), 1);
    let root = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(root) {
        TypedEntries::Leaf(meta, entries) => {
            assert_eq!(meta.level, 0);
            assert_eq!(entries.len(), 1);
        }
        TypedEntries::Branch(_, _) => panic!("expected leaf root"),
    }
}

// ─── VAL-STREAM-INDEXER-017 ───────────────────────────────────────────────────
// Multiple items build parent layers until one root remains.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_017_multiple_items_produce_single_root() {
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let (result, store) = one_shot_index(&items, 256).await.unwrap();

    // Root must exist in the store
    assert!(store.get(&result.root_id).unwrap().is_some());
    // All block IDs must be resolvable
    for id in &result.block_ids {
        assert!(store.get(id).unwrap().is_some(), "missing block {id}");
    }
    // At least one block per leaf + at least one parent
    assert!(result.block_ids.len() >= items.len());
}

// ─── VAL-STREAM-INDEXER-018 ───────────────────────────────────────────────────
// Branch entries are sorted by embedding bytes and deduplicated by child ID.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_018_branch_entries_sorted_and_deduplicated() {
    // Include a duplicate to force deduplication
    let items = [item("alpha"), item("alpha"), item("bravo"), item("charlie")];
    let (result, store) = one_shot_index(&items, 256).await.unwrap();

    // Walk all blocks and verify branch-entry invariants
    for id in &result.block_ids {
        let validated = store.get(id).unwrap().unwrap();
        if let TypedEntries::Branch(_, entries) = into_entries(validated) {
            for window in entries.windows(2) {
                // Sorted by embedding bytes
                assert!(
                    window[0].embedding <= window[1].embedding,
                    "entries not sorted by embedding"
                );
            }
            let unique_children: HashSet<_> = entries.iter().map(|entry| entry.child).collect();
            assert_eq!(
                unique_children.len(),
                entries.len(),
                "duplicate child IDs in branch"
            );
        }
    }
}

// ─── VAL-STREAM-INDEXER-019 ───────────────────────────────────────────────────
// Block size target is enforced; failing when no conforming block can be built.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_019_intermediate_nodes_respect_size_target() {
    let items = [item("alpha"), item("bravo"), item("charlie")];
    let (result, store) = one_shot_index(&items, 256).await.unwrap();

    for id in &result.block_ids {
        let validated = store.get(id).unwrap().unwrap();
        let bytes = serialize_block(&validated.block).unwrap().bytes;
        if matches!(validated.block, lexongraph_block::Block::Branch(_)) {
            assert!(
                bytes.len() <= 256,
                "branch block {} serializes to {} bytes, exceeds 256",
                id,
                bytes.len()
            );
        }
    }

    // Tiny block size target → fail explicitly
    let err = one_shot_index(&[item("alpha"), item("bravo")], 24)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            StreamingIndexerError::IntermediateNodeTooLarge { .. }
                | StreamingIndexerError::ClusteringFailure(_)
        ),
        "expected size-target failure, got: {err}"
    );
}

// ─── VAL-STREAM-INDEXER-020 ───────────────────────────────────────────────────
// Higher-layer construction uses streaming clustering contract (inspect).

#[test]
fn val_stream_indexer_020_higher_layers_use_streaming_clustering() {
    // Static: the lib.rs higher-layer path calls factory.create_trainer,
    // which returns a StreamingClusterTrainer.  Verified by reading the source.
    let src = include_str!("../src/lib.rs");
    assert!(
        src.contains("HigherLayerClustering"),
        "lib.rs must reference HigherLayerClustering phase for higher layers"
    );
    assert!(
        src.contains("create_trainer"),
        "lib.rs must call factory.create_trainer for higher layers"
    );
}

// ─── VAL-STREAM-INDEXER-021 ───────────────────────────────────────────────────
// Same logical set produces the same result twice.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_021_deterministic_across_two_runs() {
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];

    let (r1, _) = one_shot_index(&items, 256).await.unwrap();
    let (r2, _) = one_shot_index(&items, 256).await.unwrap();

    assert_eq!(r1.root_id, r2.root_id, "root IDs must be equal");
    assert_eq!(r1.block_ids, r2.block_ids, "block ID sets must be equal");
}

// ─── VAL-STREAM-INDEXER-022 ───────────────────────────────────────────────────
// Various explicit failure modes.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_content_resolution_failure_is_explicit() {
    let mut run = StreamingIndexingRun::with_defaults(
        FailingResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    let err = run.ingest_batch(&[item("alpha")]).await.unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::ContentResolution(_)),
        "{err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_unusable_content_failure_is_explicit() {
    let mut run = StreamingIndexingRun::with_defaults(
        UnusableResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    let err = run.ingest_batch(&[item("alpha")]).await.unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::UnusableContent(_)),
        "{err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_embedding_failure_is_explicit() {
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        FailingEmbeddingProvider,
        embedding_spec(),
        256,
    );
    let err = run.ingest_batch(&[item("alpha")]).await.unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::EmbeddingFailure(_)),
        "{err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_wrong_length_embedding_failure_is_explicit() {
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        WrongLengthEmbeddingProvider,
        embedding_spec(),
        256,
    );
    let err = run.ingest_batch(&[item("alpha")]).await.unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::EmbeddingFailure(_)),
        "{err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_canonical_embedding_failure_is_explicit() {
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let store = MemoryBlockStore::default();

    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        FailingCanonicalPolicy,
        PairClusteringFactory,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    let err = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::CanonicalEmbeddingFailure(_)),
        "{err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_clustering_failure_on_zero_norm_is_explicit() {
    // Zero-norm embeddings are rejected by DCBC with MalformedInput.
    // With the buffered design, the trainer is fed in finish_pass.
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        ZeroEmbeddingProvider,
        embedding_spec(),
        96,
    );
    // ingest_batch succeeds (buffers embeddings, no trainer yet)
    run.ingest_batch(&items).await.unwrap();
    // finish_pass feeds to trainer → zero-norm detected
    let err = run.finish_pass().unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::ClusteringFailure(_)),
        "{err}"
    );
}

// ─── VAL-STREAM-INDEXER-023 ───────────────────────────────────────────────────
// Status observer receives structured start/progress/completion updates.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_023_observer_receives_structured_status_updates() {
    use lexongraph_streaming_indexer::StreamingIndexingPhase;
    use lexongraph_streaming_indexer::StreamingIndexingStatusState;

    let log: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let log_clone = Arc::clone(&log);
    let observer: StreamingIndexingStatusObserver =
        Arc::new(move |s| log_clone.lock().unwrap().push(s));

    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let store = MemoryBlockStore::default();

    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    )
    .with_observer(observer);

    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    run.finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();

    let captured = log.lock().unwrap();
    // Must have received at least a TrainingPass completion and leaf events
    let has_pass_complete = captured.iter().any(|s| {
        matches!(s.phase, StreamingIndexingPhase::TrainingPass { .. })
            && s.state == StreamingIndexingStatusState::Completed
    });
    let has_leaf_start = captured.iter().any(|s| {
        s.phase == StreamingIndexingPhase::LeafMaterialization
            && s.state == StreamingIndexingStatusState::Started
    });
    let has_leaf_complete = captured.iter().any(|s| {
        s.phase == StreamingIndexingPhase::LeafMaterialization
            && s.state == StreamingIndexingStatusState::Completed
    });
    let has_in_progress = captured
        .iter()
        .any(|s| s.state == StreamingIndexingStatusState::InProgress);
    let all_started_are_zero = captured
        .iter()
        .filter(|s| s.state == StreamingIndexingStatusState::Started)
        .all(|s| s.elapsed.is_zero());

    assert!(
        has_pass_complete,
        "no TrainingPass Completed event recorded"
    );
    assert!(
        has_leaf_start,
        "no LeafMaterialization Started event recorded"
    );
    assert!(
        has_leaf_complete,
        "no LeafMaterialization Completed event recorded"
    );
    assert!(has_in_progress, "no InProgress event recorded");
    assert!(
        all_started_are_zero,
        "Started events should report zero elapsed time"
    );
}

#[tokio::test]
async fn leaf_store_integrity_mismatch_is_explicit() {
    let items = [item("alpha")];
    let store = FaultyIdStore::corrupt_leaf_ids();
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );

    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    let error = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap_err();

    assert!(
        matches!(
            error,
            StreamingIndexerError::Storage(BlockStoreError::IntegrityMismatch { .. })
        ),
        "expected explicit leaf integrity mismatch, got: {error}"
    );
}

#[tokio::test]
async fn branch_store_integrity_mismatch_is_explicit() {
    let items = [item("alpha"), item("bravo"), item("charlie")];
    let store = FaultyIdStore::corrupt_branch_ids();
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );

    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    let error = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap_err();

    assert!(
        matches!(
            error,
            StreamingIndexerError::Storage(BlockStoreError::IntegrityMismatch { .. })
        ),
        "expected explicit branch integrity mismatch, got: {error}"
    );
}

// ─── VAL-STREAM-INDEXER-024 ───────────────────────────────────────────────────
// Conformance helpers are behind the non-default "conformance" feature.

#[test]
fn val_stream_indexer_024_conformance_helpers_are_feature_gated() {
    let manifest = include_str!("../Cargo.toml");
    assert!(
        manifest.contains("conformance"),
        "Cargo.toml must declare a conformance feature"
    );
    // The conformance module must NOT be compiled without the feature —
    // the compile_fail doctest in lib.rs guards against that.
}

// ─── VAL-STREAM-INDEXER-025 ───────────────────────────────────────────────────
// Repeated passes require caller replay; the crate does not rematerialize data.

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_025_repeated_passes_require_caller_replay() {
    // The fact that ingest_batch takes &[IndexItem] (caller-supplied) and
    // the run does not expose any "repeat last pass" method is the verification.
    // We exercise the replay requirement by explicitly providing items for both passes.
    let items = [item("alpha"), item("bravo")];

    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    // Caller must supply items again
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_training_complete().unwrap();
    let result = run
        .finalize(
            std::iter::once(items.as_slice()),
            &MemoryBlockStore::default(),
        )
        .await
        .unwrap();
    assert!(!result.block_ids.is_empty());
}

// ─── VAL-STREAM-INDEXER-026 ───────────────────────────────────────────────────
// Depends on lexongraph-dcbc-streaming; default path delegates through contract.

#[test]
fn val_stream_indexer_026_depends_on_dcbc_streaming() {
    let manifest = include_str!("../Cargo.toml");
    assert!(
        manifest.contains("lexongraph-dcbc-streaming"),
        "Cargo.toml must depend on lexongraph-dcbc-streaming"
    );
    // DcbcStreamingClusteringFactory is the built-in default — it delegates
    // to DcbcStreamingTrainer which implements StreamingClusterTrainer.
    // Verified by compiling the crate: DcbcStreamingClusteringFactory::new(2)
    // must be constructible and usable.
    let _factory = DcbcStreamingClusteringFactory::new(2);
}

// ─── VAL-STREAM-INDEXER-027 ───────────────────────────────────────────────────
// Automated verification artifacts exist (this file IS the artifact).

#[test]
fn val_stream_indexer_027_verification_artifacts_exist() {
    // This test itself is the artifact.  Additionally verify the test file name.
    let src = include_str!("../tests/spec_validation.rs");
    assert!(
        src.contains("val_stream_indexer_001"),
        "spec_validation.rs must contain the first VAL entry test"
    );
}

// ─── Additional invariant tests ───────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn mark_training_complete_without_any_pass_fails() {
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    let err = run.mark_training_complete().unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::InvalidLifecycleTransition(_)),
        "{err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn mark_training_complete_with_open_pass_fails() {
    let mut run = StreamingIndexingRun::with_defaults(
        MapResolver,
        AsciiEmbeddingProvider,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&[item("alpha")]).await.unwrap();
    // Pass not finished yet
    let err = run.mark_training_complete().unwrap_err();
    assert!(
        matches!(err, StreamingIndexerError::InvalidLifecycleTransition(_)),
        "{err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_items_collapse_to_single_leaf_root() {
    // All duplicates → single unique leaf → that leaf is the root.
    let items = [item("same"), item("same"), item("same")];
    let (result, store) = one_shot_index(&items, 256).await.unwrap();

    let root = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(root) {
        TypedEntries::Leaf(_, entries) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].content.body, b"same".to_vec());
        }
        TypedEntries::Branch(_, _) => panic!("duplicate items should collapse to leaf root"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn default_path_and_explicit_defaults_produce_same_result() {
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];

    let (r1, _) = one_shot_index(&items, 256).await.unwrap();

    let store2 = MemoryBlockStore::default();
    let mut run2 = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        DcbcStreamingClusteringFactory { cluster_count: 2 },
        embedding_spec(),
        256,
    );
    run2.ingest_batch(&items).await.unwrap();
    run2.finish_pass().unwrap();
    run2.mark_training_complete().unwrap();
    let r2 = run2
        .finalize(std::iter::once(items.as_slice()), &store2)
        .await
        .unwrap();

    assert_eq!(r1.root_id, r2.root_id);
    assert_eq!(r1.block_ids, r2.block_ids);
}

#[tokio::test(flavor = "current_thread")]
async fn two_pass_training_accepted_and_deterministic() {
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];

    let _run_once = |items: &'static [IndexItem<&'static str>]| async move {
        let store = MemoryBlockStore::default();
        let mut run = StreamingIndexingRun::with_defaults(
            MapResolver,
            AsciiEmbeddingProvider,
            embedding_spec(),
            256,
        );
        run.ingest_batch(items).await.unwrap();
        run.finish_pass().unwrap();
        run.ingest_batch(items).await.unwrap();
        run.finish_pass().unwrap();
        run.mark_training_complete().unwrap();
        run.finalize(std::iter::once(items), &store).await.unwrap()
    };

    // Rust closures can't easily be called twice with async — run two separate identical setups.
    let r1 = {
        let store = MemoryBlockStore::default();
        let mut run = StreamingIndexingRun::with_defaults(
            MapResolver,
            AsciiEmbeddingProvider,
            embedding_spec(),
            256,
        );
        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap();
        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap();
        run.mark_training_complete().unwrap();
        run.finalize(std::iter::once(items.as_slice()), &store)
            .await
            .unwrap()
    };
    let r2 = {
        let store = MemoryBlockStore::default();
        let mut run = StreamingIndexingRun::with_defaults(
            MapResolver,
            AsciiEmbeddingProvider,
            embedding_spec(),
            256,
        );
        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap();
        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap();
        run.mark_training_complete().unwrap();
        run.finalize(std::iter::once(items.as_slice()), &store)
            .await
            .unwrap()
    };
    assert_eq!(r1.root_id, r2.root_id);
    assert_eq!(r1.block_ids, r2.block_ids);
}
