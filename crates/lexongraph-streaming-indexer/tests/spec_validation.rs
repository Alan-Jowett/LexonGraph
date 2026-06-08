// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Executable verification for docs/specs/rust-streaming-indexer-crate/validation.md

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lexongraph_block::{
    BlockError, BlockHash, BranchBlock, Content, EmbeddingSpec, TypedEntries, into_entries,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_dcbc_streaming::DcbcStreamingTrainer;
use lexongraph_directional_pca::DirectionalPcaParams;
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_streaming_clustering::{
    MetricDirection, StreamingClusteringConfig, StreamingClusteringError,
};
use lexongraph_streaming_indexer::{
    ArithmeticMeanCanonicalEmbeddingPolicy, BuiltInPlanning, BuiltInPlanningPhase,
    CanonicalEmbeddingPolicy, ContentResolver, DcbcBuiltInPlanningSettings,
    DirectionalPcaBuiltInPlanningSettings, FinalizedPartition, FinalizedPartitionHierarchy,
    HierarchicalPlanningPolicy, IndexItem, PlanningPassOutcome, PlanningStage,
    StreamingClusteringFactory, StreamingIndexerError, StreamingIndexingPhase,
    StreamingIndexingRun, StreamingIndexingStatus, StreamingIndexingStatusObserver,
    StreamingIndexingStatusState,
};
use sha2::{Digest, Sha256};

// ─── Shared infrastructure ─────────────────────────────────────────────────────

#[derive(Default)]
struct MemoryBlockStore {
    blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
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
            .map_err(|error| match error {
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

struct FaultyIdStore;

impl BlockStore for FaultyIdStore {
    fn put(&self, block: &lexongraph_block::Block) -> Result<BlockHash, BlockStoreError> {
        let serialized =
            lexongraph_block::serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
        let mut bytes = serialized.hash.into_bytes();
        if matches!(block, lexongraph_block::Block::Branch(_)) {
            bytes[0] ^= 0xFF;
        }
        Ok(BlockHash::from_bytes(bytes))
    }

    fn get(
        &self,
        _: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        Ok(None)
    }

    fn iter_block_ids(
        &self,
    ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
        Ok(Box::new(std::iter::empty()))
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

fn hash_bytes(bytes: &[u8]) -> BlockHash {
    let digest = Sha256::digest(bytes);
    let mut out = [0u8; BlockHash::LEN];
    out.copy_from_slice(&digest);
    BlockHash::from_bytes(out)
}

fn embedding_spec() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: "i8".into(),
    }
}

fn markdown_section<'a>(document: &'a str, heading: &str) -> &'a str {
    let marker = format!("### {heading}");
    let start = document
        .find(&marker)
        .unwrap_or_else(|| panic!("document must contain section heading `{heading}`"));
    let tail = &document[start..];
    let end = tail.find("\n### ").unwrap_or(tail.len());
    &tail[..end]
}

fn item(name: &'static str) -> IndexItem<&'static str> {
    IndexItem {
        metadata: vec![],
        content_ref: name,
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

    fn fingerprint(&self, content_ref: &&'static str) -> Result<BlockHash, Self::Error> {
        Ok(hash_bytes(content_ref.as_bytes()))
    }
}

#[derive(Clone, Copy)]
struct AliasResolver;

impl ContentResolver<&'static str> for AliasResolver {
    type Error = FixtureError;

    fn resolve(&self, content_ref: &&'static str) -> Result<Content, Self::Error> {
        let body = match *content_ref {
            "alpha-alias-1" | "alpha-alias-2" => b"alpha".to_vec(),
            other => other.as_bytes().to_vec(),
        };
        Ok(Content {
            media_type: "text/plain".into(),
            body,
        })
    }

    fn fingerprint(&self, content_ref: &&'static str) -> Result<BlockHash, Self::Error> {
        Ok(hash_bytes(content_ref.as_bytes()))
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

    fn fingerprint(&self, content_ref: &&'static str) -> Result<BlockHash, Self::Error> {
        Ok(hash_bytes(content_ref.as_bytes()))
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

#[derive(Clone, Copy)]
struct FailingEmbeddingProvider;

impl EmbeddingProvider for FailingEmbeddingProvider {
    type Error = FixtureError;

    async fn embed(&self, _: &EmbeddingInput, _: &EmbeddingSpec) -> Result<Vec<u8>, Self::Error> {
        Err(FixtureError("embedding model offline".into()))
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
struct PairClusteringFactory;

impl StreamingClusteringFactory for PairClusteringFactory {
    type Trainer = DcbcStreamingTrainer;
    type Error = StreamingClusteringError;

    fn create_trainer(
        &self,
        dimensions: usize,
        _estimated_child_count: usize,
        _block_size_target: usize,
        _embedding_spec: &EmbeddingSpec,
    ) -> Result<Self::Trainer, Self::Error> {
        DcbcStreamingTrainer::new(StreamingClusteringConfig {
            cluster_count: 2,
            dimensions,
            balance_constraints: None,
            random_seed: None,
        })
    }
}

#[derive(Clone, Default)]
struct InvalidHierarchyPlanningPolicy;

impl HierarchicalPlanningPolicy for InvalidHierarchyPlanningPolicy {
    type Error = FixtureError;

    fn finish_planning_pass(
        &mut self,
        embeddings: &[Vec<f32>],
        _: &EmbeddingSpec,
        _: usize,
        _: usize,
    ) -> Result<PlanningPassOutcome, Self::Error> {
        let max = embeddings.len().saturating_sub(1);
        Ok(PlanningPassOutcome {
            hierarchy: FinalizedPartitionHierarchy {
                root_partition_id: "p0".into(),
                partitions: vec![
                    FinalizedPartition {
                        id: "p0".into(),
                        parent_id: None,
                        child_ids: vec!["p0.0".into(), "p0.1".into()],
                        item_indices: (0..embeddings.len()).collect(),
                        terminal: false,
                        planning_stage: PlanningStage::Custom,
                    },
                    FinalizedPartition {
                        id: "p0.0".into(),
                        parent_id: Some("p0".into()),
                        child_ids: vec![],
                        item_indices: vec![0, 1.min(max)],
                        terminal: true,
                        planning_stage: PlanningStage::Custom,
                    },
                    FinalizedPartition {
                        id: "p0.1".into(),
                        parent_id: Some("p0".into()),
                        child_ids: vec![],
                        item_indices: vec![1.min(max), 2.min(max)],
                        terminal: true,
                        planning_stage: PlanningStage::Custom,
                    },
                ],
            },
            planning_quality_metric: 0.0,
            planning_balance_metric: 0.0,
            planning_quality_direction: MetricDirection::LargerIsBetter,
            planning_balance_direction: MetricDirection::SmallerIsBetter,
            stages_used: [PlanningStage::Custom].into_iter().collect(),
        })
    }
}

#[derive(Clone, Default)]
struct FixedHierarchyPlanningPolicy;

impl HierarchicalPlanningPolicy for FixedHierarchyPlanningPolicy {
    type Error = FixtureError;

    fn finish_planning_pass(
        &mut self,
        embeddings: &[Vec<f32>],
        _: &EmbeddingSpec,
        _: usize,
        _: usize,
    ) -> Result<PlanningPassOutcome, Self::Error> {
        let mid = embeddings.len() / 2;
        Ok(PlanningPassOutcome {
            hierarchy: FinalizedPartitionHierarchy {
                root_partition_id: "p0".into(),
                partitions: vec![
                    FinalizedPartition {
                        id: "p0".into(),
                        parent_id: None,
                        child_ids: vec!["p0.0".into(), "p0.1".into()],
                        item_indices: (0..embeddings.len()).collect(),
                        terminal: false,
                        planning_stage: PlanningStage::Custom,
                    },
                    FinalizedPartition {
                        id: "p0.0".into(),
                        parent_id: Some("p0".into()),
                        child_ids: vec![],
                        item_indices: (0..mid).collect(),
                        terminal: true,
                        planning_stage: PlanningStage::Custom,
                    },
                    FinalizedPartition {
                        id: "p0.1".into(),
                        parent_id: Some("p0".into()),
                        child_ids: vec![],
                        item_indices: (mid..embeddings.len()).collect(),
                        terminal: true,
                        planning_stage: PlanningStage::Custom,
                    },
                ],
            },
            planning_quality_metric: 1.0,
            planning_balance_metric: 0.0,
            planning_quality_direction: MetricDirection::LargerIsBetter,
            planning_balance_direction: MetricDirection::SmallerIsBetter,
            stages_used: [PlanningStage::Custom].into_iter().collect(),
        })
    }
}

#[derive(Clone, Default)]
struct RecoveringHierarchyPlanningPolicy {
    failed_once: bool,
}

impl HierarchicalPlanningPolicy for RecoveringHierarchyPlanningPolicy {
    type Error = FixtureError;

    fn finish_planning_pass(
        &mut self,
        embeddings: &[Vec<f32>],
        _: &EmbeddingSpec,
        _: usize,
        _: usize,
    ) -> Result<PlanningPassOutcome, Self::Error> {
        if !self.failed_once {
            self.failed_once = true;
            let mut invalid = InvalidHierarchyPlanningPolicy;
            return invalid.finish_planning_pass(embeddings, &embedding_spec(), 0, 0);
        }

        let mut fixed = FixedHierarchyPlanningPolicy;
        fixed.finish_planning_pass(embeddings, &embedding_spec(), 0, 0)
    }
}

#[derive(Clone, Default)]
struct ClusteringFailurePlanningPolicy;

impl HierarchicalPlanningPolicy for ClusteringFailurePlanningPolicy {
    type Error = StreamingClusteringError;

    fn declared_stages(&self) -> std::collections::BTreeSet<PlanningStage> {
        [PlanningStage::Custom].into_iter().collect()
    }

    fn finish_planning_pass(
        &mut self,
        _: &[Vec<f32>],
        _: &EmbeddingSpec,
        _: usize,
        _: usize,
    ) -> Result<PlanningPassOutcome, Self::Error> {
        Err(StreamingClusteringError::MalformedInput {
            message: "synthetic clustering failure".into(),
        })
    }
}

#[derive(Clone)]
struct BuiltInAlgorithmCase {
    name: &'static str,
    planning: BuiltInPlanning,
}

fn dcbc_planning() -> BuiltInPlanning {
    BuiltInPlanning::Dcbc(DcbcBuiltInPlanningSettings {
        cluster_count: 2,
        balance_constraints: None,
        random_seed: Some(7),
    })
}

fn directional_pca_planning() -> BuiltInPlanning {
    BuiltInPlanning::DirectionalPca(DirectionalPcaBuiltInPlanningSettings {
        cluster_count: 2,
        random_seed: Some(7),
        params: DirectionalPcaParams {
            retained_dimension_count: 1,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    })
}

fn hybrid_planning() -> BuiltInPlanning {
    BuiltInPlanning::Hybrid(
        lexongraph_streaming_indexer::HybridBuiltInPlanningSettings {
            coarse: BuiltInPlanningPhase::Dcbc(DcbcBuiltInPlanningSettings {
                cluster_count: 2,
                balance_constraints: None,
                random_seed: Some(11),
            }),
            fine: BuiltInPlanningPhase::DirectionalPca(DirectionalPcaBuiltInPlanningSettings {
                cluster_count: 2,
                random_seed: Some(13),
                params: DirectionalPcaParams {
                    retained_dimension_count: 1,
                    variance_exponent: 1.0,
                    temperature: 1.0,
                    min_input_count: 2,
                    min_effective_rank: 1,
                    min_cumulative_variance: 0.0,
                },
            }),
            fine_partition_max_items: 4,
        },
    )
}

fn invalid_hybrid_planning() -> BuiltInPlanning {
    BuiltInPlanning::Hybrid(
        lexongraph_streaming_indexer::HybridBuiltInPlanningSettings {
            coarse: BuiltInPlanningPhase::Dcbc(DcbcBuiltInPlanningSettings {
                cluster_count: 2,
                balance_constraints: None,
                random_seed: None,
            }),
            fine: BuiltInPlanningPhase::DirectionalPca(DirectionalPcaBuiltInPlanningSettings {
                cluster_count: 2,
                random_seed: None,
                params: DirectionalPcaParams {
                    retained_dimension_count: 1,
                    variance_exponent: 1.0,
                    temperature: 1.0,
                    min_input_count: 2,
                    min_effective_rank: 1,
                    min_cumulative_variance: 0.0,
                },
            }),
            fine_partition_max_items: 1,
        },
    )
}

fn built_in_cases() -> [BuiltInAlgorithmCase; 2] {
    [
        BuiltInAlgorithmCase {
            name: "dcbc",
            planning: dcbc_planning(),
        },
        BuiltInAlgorithmCase {
            name: "directional-pca",
            planning: directional_pca_planning(),
        },
    ]
}

fn run_with_builtin(
    planning: BuiltInPlanning,
    block_size_target: usize,
) -> StreamingIndexingRun<
    &'static str,
    MapResolver,
    AsciiEmbeddingProvider,
    ArithmeticMeanCanonicalEmbeddingPolicy,
    lexongraph_streaming_indexer::BuiltInPlanningPolicy,
> {
    StreamingIndexingRun::with_builtin_planning(
        MapResolver,
        AsciiEmbeddingProvider,
        planning,
        embedding_spec(),
        block_size_target,
    )
}

async fn one_shot(
    planning: BuiltInPlanning,
    items: &[IndexItem<&'static str>],
    block_size_target: usize,
) -> Result<
    (
        MemoryBlockStore,
        lexongraph_streaming_indexer::StreamingIndexingResult,
    ),
    StreamingIndexerError,
> {
    let store = MemoryBlockStore::default();
    let mut run = run_with_builtin(planning, block_size_target);
    run.ingest_batch(items).await?;
    run.finish_pass()?;
    run.mark_planning_complete()?;
    let result = run.finalize(std::iter::once(items), &store).await?;
    Ok((store, result))
}

// ─── Validation surface ────────────────────────────────────────────────────────

#[test]
fn val_stream_indexer_001_repository_and_specs_exist() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.parent().and_then(|p| p.parent()).unwrap();
    let requirements_path = repo_root
        .join("docs")
        .join("specs")
        .join("rust-streaming-indexer-crate")
        .join("requirements.md");
    let validation_path = repo_root
        .join("docs")
        .join("specs")
        .join("rust-streaming-indexer-crate")
        .join("validation.md");

    assert!(
        repo_root
            .join("crates")
            .join("lexongraph-streaming-indexer")
            .exists()
    );
    assert!(requirements_path.exists());
    assert!(validation_path.exists());

    let requirements = std::fs::read_to_string(requirements_path).unwrap();
    let validation = std::fs::read_to_string(validation_path).unwrap();
    assert!(
        markdown_section(&requirements, "REQ-STREAM-INDEXER-034")
            .contains("deterministic planning boundary")
    );
    let val_020 = markdown_section(&validation, "VAL-STREAM-INDEXER-020");
    assert!(val_020.contains("finalized partition") && val_020.contains("stored partition"));
}

#[test]
fn val_stream_indexer_002_public_surface_uses_planning_terms() {
    let src = include_str!("../src/lib.rs");
    let manifest = include_str!("../Cargo.toml");
    assert!(src.contains("with_builtin_planning"));
    assert!(src.contains("mark_planning_complete"));
    assert!(src.contains("HierarchyPlanning"));
    assert!(src.contains("BottomUpAssembly"));
    assert!(manifest.contains("lexongraph-dcbc-streaming"));
    assert!(manifest.contains("lexongraph-directional-pca"));
    assert!(manifest.contains("lexongraph-streaming-clustering"));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_003_empty_pass_and_empty_run_fail() {
    for case in built_in_cases() {
        let mut run = run_with_builtin(case.planning.clone(), 256);
        assert!(matches!(
            run.finish_pass().unwrap_err(),
            StreamingIndexerError::EmptyPass(_)
        ));

        run.ingest_batch(&[item("alpha"), item("bravo")])
            .await
            .unwrap();
        run.finish_pass().unwrap();
        run.mark_planning_complete().unwrap();
        assert!(matches!(
            run.finalize(
                std::iter::empty::<&[IndexItem<&'static str>]>(),
                &MemoryBlockStore::default()
            )
            .await
            .unwrap_err(),
            StreamingIndexerError::EmptyInput | StreamingIndexerError::ReplayMismatch(_)
        ));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_004_reference_input_and_inline_leaf_content() {
    let items = [item("alpha")];
    let (store, result) = one_shot(dcbc_planning(), &items, 256).await.unwrap();
    let validated = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(validated) {
        TypedEntries::Leaf(_, entries) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].content.media_type, "text/plain");
            assert_eq!(entries[0].content.body, b"alpha");
        }
        TypedEntries::Branch(_, _) => panic!("single-item result should be a leaf root"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_005_replay_baseline_accepts_identical_passes_and_rejects_drift() {
    let mut run = run_with_builtin(dcbc_planning(), 256);
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let report1 = {
        run.ingest_batch(&items[..2]).await.unwrap();
        run.ingest_batch(&items[2..]).await.unwrap();
        run.finish_pass().unwrap()
    };
    let report2 = {
        run.ingest_batch(&items).await.unwrap();
        run.finish_pass().unwrap()
    };
    assert_eq!(report1.observed_item_count, report2.observed_item_count);
    assert_eq!(report2.completed_pass_count, 2);

    let err = run
        .ingest_batch(&[item("alpha"), item("DIFFERENT")])
        .await
        .unwrap_err();
    assert!(matches!(err, StreamingIndexerError::ReplayMismatch(_)));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_006_content_and_embedding_failures_are_explicit() {
    let mut run = StreamingIndexingRun::with_builtin_planning(
        UnusableResolver,
        AsciiEmbeddingProvider,
        dcbc_planning(),
        embedding_spec(),
        256,
    );
    assert!(matches!(
        run.ingest_batch(&[item("alpha")]).await.unwrap_err(),
        StreamingIndexerError::UnusableContent(_)
    ));

    let mut run = StreamingIndexingRun::with_builtin_planning(
        MapResolver,
        FailingEmbeddingProvider,
        dcbc_planning(),
        embedding_spec(),
        256,
    );
    assert!(matches!(
        run.ingest_batch(&[item("alpha")]).await.unwrap_err(),
        StreamingIndexerError::EmbeddingFailure(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_007_finalize_requires_planning_completion() {
    let items = [item("alpha"), item("bravo")];
    let store = MemoryBlockStore::default();
    let mut run = run_with_builtin(dcbc_planning(), 256);
    assert!(matches!(
        run.finalize(std::iter::once(items.as_slice()), &store)
            .await
            .unwrap_err(),
        StreamingIndexerError::InvalidLifecycleTransition(_)
    ));

    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    assert!(matches!(
        run.finalize(std::iter::once(items.as_slice()), &store)
            .await
            .unwrap_err(),
        StreamingIndexerError::InvalidLifecycleTransition(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_008_built_in_matrix_and_determinism_hold() {
    let items = [
        item("alpha"),
        item("bravo"),
        item("charlie"),
        item("delta"),
        item("echo"),
        item("foxtrot"),
    ];

    for case in built_in_cases() {
        let mut first = run_with_builtin(case.planning.clone(), 256);
        first.ingest_batch(&items).await.unwrap();
        let first_report = first.finish_pass().unwrap();
        let first_hierarchy = first.finalized_partition_hierarchy().unwrap().clone();
        first.mark_planning_complete().unwrap();
        let store1 = MemoryBlockStore::default();
        let first_result = first
            .finalize(std::iter::once(items.as_slice()), &store1)
            .await
            .unwrap();

        let mut second = run_with_builtin(case.planning, 256);
        second.ingest_batch(&items).await.unwrap();
        let second_report = second.finish_pass().unwrap();
        let second_hierarchy = second.finalized_partition_hierarchy().unwrap().clone();
        second.mark_planning_complete().unwrap();
        let store2 = MemoryBlockStore::default();
        let second_result = second
            .finalize(std::iter::once(items.as_slice()), &store2)
            .await
            .unwrap();

        assert_eq!(first_report, second_report, "{}", case.name);
        assert_eq!(first_hierarchy, second_hierarchy, "{}", case.name);
        assert_eq!(first_result.root_id, second_result.root_id, "{}", case.name);
        assert_eq!(
            first_result.block_ids, second_result.block_ids,
            "{}",
            case.name
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_009_custom_override_paths_are_accepted() {
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];

    let store = MemoryBlockStore::default();
    let mut by_factory = StreamingIndexingRun::with_streaming_clustering_factory(
        MapResolver,
        AsciiEmbeddingProvider,
        FirstChildCanonicalPolicy,
        PairClusteringFactory,
        embedding_spec(),
        256,
    );
    by_factory.ingest_batch(&items).await.unwrap();
    by_factory.finish_pass().unwrap();
    by_factory.mark_planning_complete().unwrap();
    let result = by_factory
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();
    assert!(!result.block_ids.is_empty());

    let store = MemoryBlockStore::default();
    let mut by_policy = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        FixedHierarchyPlanningPolicy,
        embedding_spec(),
        256,
    );
    by_policy.ingest_batch(&items).await.unwrap();
    let report = by_policy.finish_pass().unwrap();
    assert_eq!(report.planned_partition_count, 3);
    by_policy.mark_planning_complete().unwrap();
    assert!(
        !by_policy
            .finalize(std::iter::once(items.as_slice()), &store)
            .await
            .unwrap()
            .block_ids
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_010_hierarchy_is_exposed_and_schedule_independent() {
    let items = [
        item("alpha"),
        item("bravo"),
        item("charlie"),
        item("delta"),
        item("echo"),
        item("foxtrot"),
        item("golf"),
        item("hotel"),
    ];
    let mut run = run_with_builtin(dcbc_planning(), 160);
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    let hierarchy = run.finalized_partition_hierarchy().unwrap();
    assert_eq!(hierarchy.root_partition_id, "p0");
    assert!(
        hierarchy
            .partitions
            .iter()
            .all(|partition| partition.id.starts_with("p0"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_011_invalid_hierarchy_fails_explicitly() {
    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        InvalidHierarchyPlanningPolicy,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&[item("alpha"), item("bravo"), item("charlie"), item("delta")])
        .await
        .unwrap();
    assert!(matches!(
        run.finish_pass().unwrap_err(),
        StreamingIndexerError::HierarchyValidation(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_011_hierarchy_failure_preserves_open_pass_for_retry() {
    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        RecoveringHierarchyPlanningPolicy::default(),
        embedding_spec(),
        256,
    );
    run.ingest_batch(&[item("alpha"), item("bravo"), item("charlie"), item("delta")])
        .await
        .unwrap();
    assert!(matches!(
        run.finish_pass().unwrap_err(),
        StreamingIndexerError::HierarchyValidation(_)
    ));
    let retry = run.finish_pass().unwrap();
    assert_eq!(retry.observed_item_count, 4);
    assert_eq!(retry.completed_pass_count, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_012_materializability_bound_is_enforced() {
    let mut run = run_with_builtin(dcbc_planning(), 1);
    run.ingest_batch(&[item("alpha"), item("bravo")])
        .await
        .unwrap();
    assert!(matches!(
        run.finish_pass().unwrap_err(),
        StreamingIndexerError::TerminalPartitionMaterialization(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_invalid_hybrid_configuration_is_explicit() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let mut run = run_with_builtin(invalid_hybrid_planning(), 256).with_observer(observer);
    run.ingest_batch(&[item("alpha"), item("bravo"), item("charlie")])
        .await
        .unwrap();
    assert!(matches!(
        run.finish_pass().unwrap_err(),
        StreamingIndexerError::InvalidHybridPlanningConfiguration(_)
    ));

    let statuses = statuses.lock().unwrap().clone();
    let planning_failures = statuses
        .iter()
        .filter(|status| {
            matches!(status.phase, StreamingIndexingPhase::PlanningPass { .. })
                && status.state == StreamingIndexingStatusState::Failed
        })
        .count();
    assert_eq!(planning_failures, 1);
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::HierarchyPlanning {
                stage: PlanningStage::Coarse
            }
        ) && status.state == StreamingIndexingStatusState::Started
    }));
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::HierarchyPlanning {
                stage: PlanningStage::Coarse
            }
        ) && status.state == StreamingIndexingStatusState::InProgress
    }));
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::HierarchyPlanning {
                stage: PlanningStage::Coarse
            }
        ) && status.state == StreamingIndexingStatusState::Failed
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_013_hybrid_planning_is_explicit_and_deterministic() {
    let items = [
        item("alpha"),
        item("bravo"),
        item("charlie"),
        item("delta"),
        item("echo"),
        item("foxtrot"),
        item("golf"),
        item("hotel"),
    ];
    let mut first = run_with_builtin(hybrid_planning(), 160);
    first.ingest_batch(&items).await.unwrap();
    let report1 = first.finish_pass().unwrap();
    let hierarchy1 = first.finalized_partition_hierarchy().unwrap().clone();
    assert!(
        hierarchy1
            .partitions
            .iter()
            .any(|partition| partition.planning_stage == PlanningStage::Fine)
    );

    let mut second = run_with_builtin(hybrid_planning(), 160);
    second.ingest_batch(&items).await.unwrap();
    let report2 = second.finish_pass().unwrap();
    assert_eq!(report1, report2);
    assert_eq!(
        hierarchy1,
        second.finalized_partition_hierarchy().unwrap().clone()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_014_bottom_up_assembly_returns_complete_block_set() {
    let items = [
        item("alpha"),
        item("bravo"),
        item("charlie"),
        item("delta"),
        item("echo"),
        item("foxtrot"),
    ];
    let (store, result) = one_shot(dcbc_planning(), &items, 160).await.unwrap();
    assert!(store.get(&result.root_id).unwrap().is_some());
    for block_id in &result.block_ids {
        assert!(store.get(block_id).unwrap().is_some());
    }
    assert!(result.block_ids.len() >= 3);
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_015_status_observer_uses_planning_and_bottom_up_phases() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let items = [
        item("alpha"),
        item("bravo"),
        item("charlie"),
        item("delta"),
        item("echo"),
        item("foxtrot"),
    ];
    let store = MemoryBlockStore::default();
    let mut run = run_with_builtin(dcbc_planning(), 160).with_observer(observer);
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_planning_complete().unwrap();
    run.finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();

    let statuses = statuses.lock().unwrap().clone();
    assert!(
        statuses
            .iter()
            .any(|status| matches!(status.phase, StreamingIndexingPhase::PlanningPass { .. }))
    );
    assert!(statuses.iter().any(|status| matches!(
        status.phase,
        StreamingIndexingPhase::FinalMaterializationReplay
    )));
    assert!(statuses.iter().any(|status| matches!(
        status.phase,
        StreamingIndexingPhase::BottomUpAssembly { .. }
    )));
    assert!(statuses.iter().any(|status| {
        matches!(status.phase, StreamingIndexingPhase::PlanningPass { .. })
            && status.state == StreamingIndexingStatusState::Completed
    }));
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::FinalMaterializationReplay
        ) && status.state == StreamingIndexingStatusState::Completed
    }));
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::BottomUpAssembly { .. }
        ) && status.state == StreamingIndexingStatusState::Completed
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_016_terminal_partition_normalization_can_collapse_duplicates() {
    let items = [item("alpha"), item("alpha")];
    let (store, result) = one_shot(dcbc_planning(), &items, 256).await.unwrap();
    assert_eq!(result.block_ids.len(), 1);
    let validated = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(validated) {
        TypedEntries::Leaf(_, entries) => assert_eq!(entries.len(), 1),
        TypedEntries::Branch(_, _) => panic!("duplicate leaves should collapse to a leaf root"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_017_replay_uses_content_reference_identity() {
    let mut run = StreamingIndexingRun::with_builtin_planning(
        AliasResolver,
        AsciiEmbeddingProvider,
        dcbc_planning(),
        embedding_spec(),
        256,
    );
    run.ingest_batch(&[item("alpha-alias-1"), item("bravo")])
        .await
        .unwrap();
    run.finish_pass().unwrap();
    assert!(matches!(
        run.ingest_batch(&[item("alpha-alias-2")])
            .await
            .unwrap_err(),
        StreamingIndexerError::ReplayMismatch(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_018_storage_integrity_is_checked() {
    let mut run = run_with_builtin(dcbc_planning(), 160);
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_planning_complete().unwrap();
    assert!(matches!(
        run.finalize(std::iter::once(items.as_slice()), &FaultyIdStore)
            .await
            .unwrap_err(),
        StreamingIndexerError::Storage(BlockStoreError::IntegrityMismatch { .. })
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_023_failed_bottom_up_assembly_emits_failed_status() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let mut run = run_with_builtin(dcbc_planning(), 160).with_observer(observer);
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_planning_complete().unwrap();
    assert!(matches!(
        run.finalize(std::iter::once(items.as_slice()), &FaultyIdStore)
            .await
            .unwrap_err(),
        StreamingIndexerError::Storage(BlockStoreError::IntegrityMismatch { .. })
    ));

    let statuses = statuses.lock().unwrap().clone();
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::BottomUpAssembly { .. }
        ) && status.state == StreamingIndexingStatusState::Failed
    }));
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::FinalMaterializationReplay
        ) && status.state == StreamingIndexingStatusState::Failed
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_024_invalid_hierarchy_emits_failed_status() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        InvalidHierarchyPlanningPolicy,
        embedding_spec(),
        256,
    )
    .with_observer(observer);
    run.ingest_batch(&[item("alpha"), item("bravo"), item("charlie"), item("delta")])
        .await
        .unwrap();
    assert!(matches!(
        run.finish_pass().unwrap_err(),
        StreamingIndexerError::HierarchyValidation(_)
    ));

    let statuses = statuses.lock().unwrap().clone();
    assert!(statuses.iter().any(|status| {
        matches!(status.phase, StreamingIndexingPhase::PlanningPass { .. })
            && status.state == StreamingIndexingStatusState::Failed
    }));
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::HierarchyPlanning {
                stage: PlanningStage::Custom
            }
        ) && status.state == StreamingIndexingStatusState::Failed
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_025_final_replay_mismatch_emits_failed_status() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let store = MemoryBlockStore::default();
    let mut run = run_with_builtin(dcbc_planning(), 160).with_observer(observer);
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_planning_complete().unwrap();
    assert!(matches!(
        run.finalize(
            std::iter::once(
                [item("delta"), item("charlie"), item("bravo"), item("alpha")].as_slice()
            ),
            &store
        )
        .await
        .unwrap_err(),
        StreamingIndexerError::ReplayMismatch(_)
    ));

    let statuses = statuses.lock().unwrap().clone();
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::FinalMaterializationReplay
        ) && status.state == StreamingIndexingStatusState::Failed
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_026_clustering_failure_is_explicit() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        ClusteringFailurePlanningPolicy,
        embedding_spec(),
        256,
    )
    .with_observer(observer);
    run.ingest_batch(&[item("alpha"), item("bravo"), item("charlie")])
        .await
        .unwrap();
    assert!(matches!(
        run.finish_pass().unwrap_err(),
        StreamingIndexerError::ClusteringFailure(_)
    ));

    let statuses = statuses.lock().unwrap().clone();
    assert!(statuses.iter().any(|status| {
        matches!(status.phase, StreamingIndexingPhase::PlanningPass { .. })
            && status.state == StreamingIndexingStatusState::Failed
    }));
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::HierarchyPlanning {
                stage: PlanningStage::Custom
            }
        ) && status.state == StreamingIndexingStatusState::Failed
    }));
}
