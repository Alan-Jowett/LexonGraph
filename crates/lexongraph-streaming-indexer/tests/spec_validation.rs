// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Executable verification for docs/specs/rust-streaming-indexer-crate/validation.md

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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
    ActivePlanningAlgorithm, AdaptiveDcbcSettings, AdaptiveDirectionalPcaSettings,
    AdaptivePlanningDecisionReason, AdaptivePlanningDirection, AdaptivePlanningSettings,
    DEFAULT_EMBEDDING_COUNT_CUTOFF,
};
use lexongraph_streaming_indexer::{
    ArithmeticMeanCanonicalEmbeddingPolicy, BuiltInPlanning, BuiltInPlanningDirection,
    BuiltInPlanningPhase, CanonicalEmbeddingPolicy, ContentResolver, DcbcBuiltInPlanningSettings,
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

#[derive(Default)]
struct SlowBranchStore {
    blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
}

impl BlockStore for SlowBranchStore {
    fn put(&self, block: &lexongraph_block::Block) -> Result<BlockHash, BlockStoreError> {
        if matches!(block, lexongraph_block::Block::Branch(_)) {
            thread::sleep(Duration::from_millis(250));
        }
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
struct LiveStageObserverPlanningPolicy {
    saw_live_stage_status: Arc<AtomicBool>,
}

impl HierarchicalPlanningPolicy for LiveStageObserverPlanningPolicy {
    type Error = FixtureError;

    fn finish_planning_pass(
        &mut self,
        embeddings: &[Vec<f32>],
        _: &EmbeddingSpec,
        _: usize,
        _: usize,
    ) -> Result<PlanningPassOutcome, Self::Error> {
        if !self.saw_live_stage_status.load(Ordering::SeqCst) {
            return Err(FixtureError(
                "hierarchy stage progress was not emitted before policy execution".into(),
            ));
        }
        let mut fixed = FixedHierarchyPlanningPolicy;
        fixed.finish_planning_pass(embeddings, &embedding_spec(), 0, 0)
    }
}

#[derive(Clone)]
struct BuiltInAlgorithmCase {
    name: &'static str,
    planning: BuiltInPlanning,
}

fn dcbc_planning(direction: BuiltInPlanningDirection) -> BuiltInPlanning {
    BuiltInPlanning::Dcbc(DcbcBuiltInPlanningSettings {
        direction,
        cluster_count: 2,
        balance_constraints: None,
        random_seed: Some(7),
    })
}

fn directional_pca_planning(direction: BuiltInPlanningDirection) -> BuiltInPlanning {
    BuiltInPlanning::DirectionalPca(DirectionalPcaBuiltInPlanningSettings {
        direction,
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

fn hybrid_planning(direction: BuiltInPlanningDirection) -> BuiltInPlanning {
    BuiltInPlanning::Hybrid(
        lexongraph_streaming_indexer::HybridBuiltInPlanningSettings {
            coarse: BuiltInPlanningPhase::Dcbc(DcbcBuiltInPlanningSettings {
                direction,
                cluster_count: 2,
                balance_constraints: None,
                random_seed: Some(11),
            }),
            fine: BuiltInPlanningPhase::DirectionalPca(DirectionalPcaBuiltInPlanningSettings {
                direction,
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

fn adaptive_planning(direction: BuiltInPlanningDirection) -> BuiltInPlanning {
    adaptive_planning_with_cluster_count(direction, 2)
}

fn adaptive_planning_with_cluster_count(
    direction: BuiltInPlanningDirection,
    cluster_count: u32,
) -> BuiltInPlanning {
    let adaptive_direction = match direction {
        BuiltInPlanningDirection::Divisive => AdaptivePlanningDirection::Divisive,
        BuiltInPlanningDirection::Agglomerative => AdaptivePlanningDirection::Agglomerative,
    };
    BuiltInPlanning::Adaptive(AdaptivePlanningSettings {
        direction: adaptive_direction,
        directional_pca: AdaptiveDirectionalPcaSettings {
            cluster_count,
            random_seed: Some(17),
            params: DirectionalPcaParams {
                retained_dimension_count: 1,
                variance_exponent: 1.0,
                temperature: 1.0,
                min_input_count: 2,
                min_effective_rank: 1,
                min_cumulative_variance: 0.0,
            },
        },
        dcbc: AdaptiveDcbcSettings {
            cluster_count: 2,
            balance_constraints: None,
            random_seed: Some(19),
        },
    })
}

fn invalid_adaptive_planning() -> BuiltInPlanning {
    BuiltInPlanning::Adaptive(AdaptivePlanningSettings {
        direction: AdaptivePlanningDirection::Divisive,
        directional_pca: AdaptiveDirectionalPcaSettings {
            cluster_count: 0,
            random_seed: None,
            params: DirectionalPcaParams {
                retained_dimension_count: 1,
                variance_exponent: 1.0,
                temperature: 1.0,
                min_input_count: 2,
                min_effective_rank: 1,
                min_cumulative_variance: 0.0,
            },
        },
        dcbc: AdaptiveDcbcSettings {
            cluster_count: 2,
            balance_constraints: None,
            random_seed: None,
        },
    })
}

fn invalid_hybrid_planning() -> BuiltInPlanning {
    BuiltInPlanning::Hybrid(
        lexongraph_streaming_indexer::HybridBuiltInPlanningSettings {
            coarse: BuiltInPlanningPhase::Dcbc(DcbcBuiltInPlanningSettings {
                direction: BuiltInPlanningDirection::Divisive,
                cluster_count: 2,
                balance_constraints: None,
                random_seed: None,
            }),
            fine: BuiltInPlanningPhase::DirectionalPca(DirectionalPcaBuiltInPlanningSettings {
                direction: BuiltInPlanningDirection::Divisive,
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

fn mixed_direction_hybrid_planning() -> BuiltInPlanning {
    BuiltInPlanning::Hybrid(
        lexongraph_streaming_indexer::HybridBuiltInPlanningSettings {
            coarse: BuiltInPlanningPhase::Dcbc(DcbcBuiltInPlanningSettings {
                direction: BuiltInPlanningDirection::Divisive,
                cluster_count: 2,
                balance_constraints: None,
                random_seed: Some(11),
            }),
            fine: BuiltInPlanningPhase::DirectionalPca(DirectionalPcaBuiltInPlanningSettings {
                direction: BuiltInPlanningDirection::Agglomerative,
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

fn built_in_cases() -> [BuiltInAlgorithmCase; 4] {
    [
        BuiltInAlgorithmCase {
            name: "dcbc-divisive",
            planning: dcbc_planning(BuiltInPlanningDirection::Divisive),
        },
        BuiltInAlgorithmCase {
            name: "dcbc-agglomerative",
            planning: dcbc_planning(BuiltInPlanningDirection::Agglomerative),
        },
        BuiltInAlgorithmCase {
            name: "directional-pca-divisive",
            planning: directional_pca_planning(BuiltInPlanningDirection::Divisive),
        },
        BuiltInAlgorithmCase {
            name: "directional-pca-agglomerative",
            planning: directional_pca_planning(BuiltInPlanningDirection::Agglomerative),
        },
    ]
}

fn switch_trigger_items() -> [IndexItem<&'static str>; 8] {
    [
        item("a"),
        item("aa"),
        item("b"),
        item("bb"),
        item("x"),
        item("xx"),
        item("y"),
        item("yy"),
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

fn unique_adaptive_status_telemetries(
    statuses: &[StreamingIndexingStatus],
) -> Vec<lexongraph_streaming_indexer::AdaptivePlanningStatusTelemetry> {
    let mut by_boundary = BTreeMap::new();
    for status in statuses {
        if matches!(
            status.phase,
            StreamingIndexingPhase::HierarchyPlanning { .. }
        ) && let Some(telemetry) = status.adaptive_planning
        {
            by_boundary
                .entry(telemetry.decision.boundary_position)
                .or_insert(telemetry);
        }
    }
    by_boundary.into_values().collect()
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
    assert!(src.contains("BuiltInPlanningDirection"));
    assert!(src.contains("mark_planning_complete"));
    assert!(src.contains("HierarchyPlanning"));
    assert!(src.contains("BottomUpAssembly"));
    assert!(src.contains("AdaptivePlanningSettings"));
    assert!(src.contains("InvalidAdaptivePlanningConfiguration"));
    assert!(src.contains("BuiltInPlanning::Adaptive"));
    assert!(manifest.contains("lexongraph-adaptive-planning-policy"));
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
    let (store, result) = one_shot(
        dcbc_planning(BuiltInPlanningDirection::Divisive),
        &items,
        256,
    )
    .await
    .unwrap();
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
    let mut run = run_with_builtin(dcbc_planning(BuiltInPlanningDirection::Divisive), 256);
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
        dcbc_planning(BuiltInPlanningDirection::Divisive),
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
        dcbc_planning(BuiltInPlanningDirection::Divisive),
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
    let mut run = run_with_builtin(dcbc_planning(BuiltInPlanningDirection::Divisive), 256);
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
    let mut run = run_with_builtin(dcbc_planning(BuiltInPlanningDirection::Divisive), 160);
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
async fn regression_hierarchy_failure_preserves_open_pass_for_retry() {
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
async fn regression_default_custom_policy_emits_live_stage_progress() {
    let saw_live_stage_status = Arc::new(AtomicBool::new(false));
    let observer_flag = Arc::clone(&saw_live_stage_status);
    let observer: StreamingIndexingStatusObserver = Arc::new(move |status| {
        if matches!(
            status.phase,
            StreamingIndexingPhase::HierarchyPlanning {
                stage: PlanningStage::Custom
            }
        ) && matches!(
            status.state,
            StreamingIndexingStatusState::Started | StreamingIndexingStatusState::InProgress
        ) {
            observer_flag.store(true, Ordering::SeqCst);
        }
    });

    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        LiveStageObserverPlanningPolicy {
            saw_live_stage_status: Arc::clone(&saw_live_stage_status),
        },
        embedding_spec(),
        256,
    )
    .with_observer(observer);
    run.ingest_batch(&[item("alpha"), item("bravo"), item("charlie"), item("delta")])
        .await
        .unwrap();
    run.finish_pass().unwrap();
    assert!(saw_live_stage_status.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_012_materializability_bound_is_enforced() {
    let mut run = run_with_builtin(dcbc_planning(BuiltInPlanningDirection::Divisive), 1);
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
    assert!(!statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::HierarchyPlanning { .. }
        )
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_022_invalid_adaptive_configuration_fails_explicitly() {
    let err = one_shot(
        invalid_adaptive_planning(),
        &[item("alpha"), item("bravo")],
        160,
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(
        err,
        StreamingIndexerError::InvalidAdaptivePlanningConfiguration(_)
    ));
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
    let mut first = run_with_builtin(hybrid_planning(BuiltInPlanningDirection::Divisive), 160);
    first.ingest_batch(&items).await.unwrap();
    let report1 = first.finish_pass().unwrap();
    let hierarchy1 = first.finalized_partition_hierarchy().unwrap().clone();
    assert!(
        hierarchy1
            .partitions
            .iter()
            .any(|partition| partition.planning_stage == PlanningStage::Fine)
    );

    let mut second = run_with_builtin(hybrid_planning(BuiltInPlanningDirection::Divisive), 160);
    second.ingest_batch(&items).await.unwrap();
    let report2 = second.finish_pass().unwrap();
    assert_eq!(report1, report2);
    assert_eq!(
        hierarchy1,
        second.finalized_partition_hierarchy().unwrap().clone()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn regression_hybrid_agglomerative_planning_is_explicit_and_deterministic() {
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
    let mut first = run_with_builtin(
        hybrid_planning(BuiltInPlanningDirection::Agglomerative),
        160,
    );
    first.ingest_batch(&items).await.unwrap();
    let report1 = first.finish_pass().unwrap();
    let hierarchy1 = first.finalized_partition_hierarchy().unwrap().clone();
    assert!(
        hierarchy1
            .partitions
            .iter()
            .any(|partition| partition.planning_stage == PlanningStage::Fine)
    );
    first.mark_planning_complete().unwrap();
    let store = MemoryBlockStore::default();
    let result1 = first
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();
    assert!(store.get(&result1.root_id).unwrap().is_some());

    let mut second = run_with_builtin(
        hybrid_planning(BuiltInPlanningDirection::Agglomerative),
        160,
    );
    second.ingest_batch(&items).await.unwrap();
    let report2 = second.finish_pass().unwrap();
    second.mark_planning_complete().unwrap();
    let store2 = MemoryBlockStore::default();
    let result2 = second
        .finalize(std::iter::once(items.as_slice()), &store2)
        .await
        .unwrap();
    assert_eq!(report1, report2);
    assert_eq!(
        hierarchy1,
        second.finalized_partition_hierarchy().unwrap().clone()
    );
    assert_eq!(result1.root_id, result2.root_id);
    assert_eq!(result1.block_ids, result2.block_ids);
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
    let (store, result) = one_shot(
        dcbc_planning(BuiltInPlanningDirection::Divisive),
        &items,
        160,
    )
    .await
    .unwrap();
    assert!(store.get(&result.root_id).unwrap().is_some());
    for block_id in &result.block_ids {
        assert!(store.get(block_id).unwrap().is_some());
    }
    assert!(result.block_ids.len() >= 3);
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_015_status_observer_uses_planning_and_bottom_up_phases() {
    use lexongraph_streaming_indexer::StreamingIndexingPhase;
    use lexongraph_streaming_indexer::StreamingIndexingStatusState;

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
    let store = SlowBranchStore::default();
    let mut run = run_with_builtin(dcbc_planning(BuiltInPlanningDirection::Divisive), 160)
        .with_observer(observer);
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
    assert!(statuses.iter().any(|status| {
        status.state == StreamingIndexingStatusState::InProgress && status.completed_unit_count > 0
    }));
    assert!(
        statuses
            .iter()
            .filter(|status| status.state == StreamingIndexingStatusState::Started)
            .all(|status| status.elapsed.is_zero() && status.completed_unit_count == 0)
    );
    assert!(statuses.iter().all(|status| {
        match status.phase_total_unit_count {
            Some(total) => {
                total >= status.completed_unit_count
                    && status.remaining_unit_count == Some(total - status.completed_unit_count)
            }
            None => status.remaining_unit_count.is_none(),
        }
    }));
    let monotonic_within_phase = statuses
        .iter()
        .try_fold(
            Vec::<(StreamingIndexingPhase, usize)>::new(),
            |mut seen, status| {
                if let Some((_, previous_completed)) =
                    seen.iter_mut().find(|(phase, _)| *phase == status.phase)
                {
                    if status.state == StreamingIndexingStatusState::Started {
                        *previous_completed = status.completed_unit_count;
                        return Some(seen);
                    }
                    if status.completed_unit_count < *previous_completed {
                        return None;
                    }
                    *previous_completed = status.completed_unit_count;
                    Some(seen)
                } else {
                    seen.push((status.phase.clone(), status.completed_unit_count));
                    Some(seen)
                }
            },
        )
        .is_some();
    assert!(monotonic_within_phase);

    let mut bottom_up_in_progress_counts = HashMap::<usize, usize>::new();
    for status in statuses.iter().filter(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::BottomUpAssembly { .. }
        ) && status.state == StreamingIndexingStatusState::InProgress
    }) {
        let StreamingIndexingPhase::BottomUpAssembly { layer_index } = status.phase else {
            unreachable!("filtered to bottom-up assembly statuses")
        };
        *bottom_up_in_progress_counts.entry(layer_index).or_default() += 1;
    }
    assert!(
        bottom_up_in_progress_counts
            .values()
            .any(|count| *count >= 2)
    );
    for status in statuses.iter().filter(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::BottomUpAssembly { .. }
        ) && status.state == StreamingIndexingStatusState::Completed
    }) {
        assert_eq!(
            status.phase_total_unit_count,
            Some(status.completed_unit_count)
        );
        assert_eq!(status.remaining_unit_count, Some(0));
        assert!(
            status.item_count > status.completed_unit_count,
            "BottomUpAssembly should preserve the legacy item_count input cardinality"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_036_status_progress_counts_have_phase_semantics() {
    use lexongraph_streaming_indexer::StreamingIndexingPhase;
    use lexongraph_streaming_indexer::StreamingIndexingStatusState;

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
    let store = SlowBranchStore::default();
    let mut run = run_with_builtin(dcbc_planning(BuiltInPlanningDirection::Divisive), 160)
        .with_observer(observer);
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_planning_complete().unwrap();
    run.finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();

    let statuses = statuses.lock().unwrap().clone();
    let planning_statuses: Vec<_> = statuses
        .iter()
        .filter(|status| matches!(status.phase, StreamingIndexingPhase::PlanningPass { .. }))
        .collect();
    assert!(!planning_statuses.is_empty());
    assert!(
        planning_statuses
            .iter()
            .all(|status| status.phase_total_unit_count == Some(items.len()))
    );
    assert!(planning_statuses.iter().any(|status| {
        status.state == StreamingIndexingStatusState::InProgress
            && status.completed_unit_count == 0
            && status.remaining_unit_count == Some(items.len())
    }));
    assert!(planning_statuses.iter().any(|status| {
        status.state == StreamingIndexingStatusState::Completed
            && status.completed_unit_count == items.len()
            && status.remaining_unit_count == Some(0)
    }));

    let replay_statuses: Vec<_> = statuses
        .iter()
        .filter(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::FinalMaterializationReplay
            )
        })
        .collect();
    assert!(!replay_statuses.is_empty());
    assert!(
        replay_statuses
            .iter()
            .all(|status| status.phase_total_unit_count == Some(items.len()))
    );
    assert!(replay_statuses.iter().any(|status| {
        status.state == StreamingIndexingStatusState::Completed
            && status.completed_unit_count == items.len()
            && status.remaining_unit_count == Some(0)
    }));

    let hierarchy_statuses: Vec<_> = statuses
        .iter()
        .filter(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::HierarchyPlanning { .. }
            )
        })
        .collect();
    assert!(!hierarchy_statuses.is_empty());
    assert!(
        hierarchy_statuses
            .iter()
            .all(|status| status.phase_total_unit_count.is_none())
    );
    assert!(hierarchy_statuses.iter().any(|status| {
        status.state == StreamingIndexingStatusState::InProgress
            && status.completed_unit_count > 0
            && status.remaining_unit_count.is_none()
    }));

    let bottom_up_layers = statuses
        .iter()
        .filter_map(|status| match status.phase {
            StreamingIndexingPhase::BottomUpAssembly { layer_index } => Some(layer_index),
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert!(!bottom_up_layers.is_empty());
    for layer_index in bottom_up_layers {
        let layer_statuses: Vec<_> = statuses
            .iter()
            .filter(|status| {
                status.phase == StreamingIndexingPhase::BottomUpAssembly { layer_index }
            })
            .collect();
        assert!(layer_statuses.iter().all(|status| {
            status.phase_total_unit_count == layer_statuses[0].phase_total_unit_count
        }));
        assert!(
            layer_statuses
                .iter()
                .all(|status| status.item_count == layer_statuses[0].item_count)
        );
        assert!(layer_statuses.iter().all(|status| {
            layer_statuses[0]
                .phase_total_unit_count
                .is_some_and(|total| status.item_count > total)
        }));
        assert!(layer_statuses.iter().any(|status| {
            status.state == StreamingIndexingStatusState::Completed
                && status.phase_total_unit_count == Some(status.completed_unit_count)
                && status.remaining_unit_count == Some(0)
        }));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_016_terminal_partition_normalization_can_collapse_duplicates() {
    let items = [item("alpha"), item("alpha")];
    let (store, result) = one_shot(
        dcbc_planning(BuiltInPlanningDirection::Divisive),
        &items,
        256,
    )
    .await
    .unwrap();
    assert_eq!(result.block_ids.len(), 1);
    let validated = store.get(&result.root_id).unwrap().unwrap();
    match into_entries(validated) {
        TypedEntries::Leaf(_, entries) => assert_eq!(entries.len(), 1),
        TypedEntries::Branch(_, _) => panic!("duplicate leaves should collapse to a leaf root"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn regression_bottom_up_status_uses_semantic_layer_indexes() {
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
        item("golf"),
        item("hotel"),
    ];
    let store = MemoryBlockStore::default();
    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        FixedHierarchyPlanningPolicy,
        embedding_spec(),
        160,
    )
    .with_observer(observer);
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_planning_complete().unwrap();
    run.finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();

    let statuses = statuses.lock().unwrap().clone();
    let bottom_up_layers = statuses
        .iter()
        .filter_map(|status| match status.phase {
            StreamingIndexingPhase::BottomUpAssembly { layer_index } => Some(layer_index),
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        bottom_up_layers,
        std::collections::BTreeSet::from([0, 1, 2])
    );
    let layer_zero_completions = statuses
        .iter()
        .filter(|status| {
            status.phase == StreamingIndexingPhase::BottomUpAssembly { layer_index: 0 }
                && status.state == StreamingIndexingStatusState::Completed
        })
        .count();
    assert!(
        layer_zero_completions >= 2,
        "sibling terminal assemblies should reuse semantic layer 0 instead of allocating fresh global layer numbers"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_017_replay_uses_content_reference_identity() {
    let mut run = StreamingIndexingRun::with_builtin_planning(
        AliasResolver,
        AsciiEmbeddingProvider,
        dcbc_planning(BuiltInPlanningDirection::Divisive),
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
    let mut run = run_with_builtin(dcbc_planning(BuiltInPlanningDirection::Divisive), 160);
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
async fn val_stream_indexer_023_failed_bottom_up_assembly_does_not_fail_replay_status() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let mut run = run_with_builtin(dcbc_planning(BuiltInPlanningDirection::Divisive), 160)
        .with_observer(observer);
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
        ) && status.state == StreamingIndexingStatusState::Completed
    }));
    assert!(!statuses.iter().any(|status| {
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
    let hierarchy_failed = statuses
        .iter()
        .position(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::HierarchyPlanning {
                    stage: PlanningStage::Custom
                }
            ) && status.state == StreamingIndexingStatusState::Failed
        })
        .expect("hierarchy planning failure status");
    let planning_failed = statuses
        .iter()
        .position(|status| {
            matches!(status.phase, StreamingIndexingPhase::PlanningPass { .. })
                && status.state == StreamingIndexingStatusState::Failed
        })
        .expect("planning pass failure status");
    assert!(hierarchy_failed < planning_failed);
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
async fn val_stream_indexer_027_unused_declared_stages_do_not_emit_status() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let mut run = run_with_builtin(hybrid_planning(BuiltInPlanningDirection::Divisive), 256)
        .with_observer(observer);
    run.ingest_batch(&[item("alpha"), item("bravo")])
        .await
        .unwrap();
    run.finish_pass().unwrap();

    let statuses = statuses.lock().unwrap().clone();
    assert!(!statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::HierarchyPlanning { .. }
        )
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
    let mut run = run_with_builtin(dcbc_planning(BuiltInPlanningDirection::Divisive), 160)
        .with_observer(observer);
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
    let hierarchy_failed = statuses
        .iter()
        .position(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::HierarchyPlanning {
                    stage: PlanningStage::Custom
                }
            ) && status.state == StreamingIndexingStatusState::Failed
        })
        .expect("hierarchy planning failure status");
    let planning_failed = statuses
        .iter()
        .position(|status| {
            matches!(status.phase, StreamingIndexingPhase::PlanningPass { .. })
                && status.state == StreamingIndexingStatusState::Failed
        })
        .expect("planning pass failure status");
    assert!(hierarchy_failed < planning_failed);
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
async fn val_stream_indexer_038_divisive_and_agglomerative_built_in_paths_are_available() {
    let items = [
        item("alpha"),
        item("bravo"),
        item("charlie"),
        item("delta"),
        item("echo"),
        item("foxtrot"),
    ];

    let divisive_store = MemoryBlockStore::default();
    let mut divisive = run_with_builtin(dcbc_planning(BuiltInPlanningDirection::Divisive), 160);
    divisive.ingest_batch(&items).await.unwrap();
    let divisive_report = divisive.finish_pass().unwrap();
    let divisive_hierarchy = divisive.finalized_partition_hierarchy().unwrap().clone();
    divisive.mark_planning_complete().unwrap();
    let divisive_result = divisive
        .finalize(std::iter::once(items.as_slice()), &divisive_store)
        .await
        .unwrap();

    let agglomerative_store = MemoryBlockStore::default();
    let mut agglomerative =
        run_with_builtin(dcbc_planning(BuiltInPlanningDirection::Agglomerative), 160);
    agglomerative.ingest_batch(&items).await.unwrap();
    let agglomerative_report = agglomerative.finish_pass().unwrap();
    let agglomerative_hierarchy = agglomerative
        .finalized_partition_hierarchy()
        .unwrap()
        .clone();
    agglomerative.mark_planning_complete().unwrap();
    let agglomerative_result = agglomerative
        .finalize(std::iter::once(items.as_slice()), &agglomerative_store)
        .await
        .unwrap();

    assert!(divisive_report.planned_partition_count > 0);
    assert!(agglomerative_report.planned_partition_count > 0);
    assert_eq!(divisive_hierarchy.root_partition_id, "p0");
    assert_eq!(agglomerative_hierarchy.root_partition_id, "p0");
    assert!(
        divisive_store
            .get(&divisive_result.root_id)
            .unwrap()
            .is_some()
    );
    assert!(
        agglomerative_store
            .get(&agglomerative_result.root_id)
            .unwrap()
            .is_some()
    );
    assert!(!divisive_result.block_ids.is_empty());
    assert!(!agglomerative_result.block_ids.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_039_mixed_direction_hybrid_is_rejected_explicitly() {
    let mut run = run_with_builtin(mixed_direction_hybrid_planning(), 256);
    run.ingest_batch(&[item("alpha"), item("bravo"), item("charlie"), item("delta")])
        .await
        .unwrap();
    assert!(matches!(
        run.finish_pass().unwrap_err(),
        StreamingIndexerError::InvalidHybridPlanningConfiguration(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_040_adaptive_builtin_constructs_for_both_directions() {
    for planning in [
        adaptive_planning(BuiltInPlanningDirection::Divisive),
        adaptive_planning(BuiltInPlanningDirection::Agglomerative),
    ] {
        let (_, result) = one_shot(planning, &[item("alpha"), item("bravo")], 256)
            .await
            .unwrap();
        assert!(!result.block_ids.is_empty());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_041_adaptive_no_switch_path_remains_pca_compatible() {
    let items = [item("a"), item("m"), item("x"), item("z")];
    let mut run = run_with_builtin(adaptive_planning(BuiltInPlanningDirection::Divisive), 160);
    run.ingest_batch(&items).await.unwrap();
    let report = run.finish_pass().unwrap();
    let decision_records = run.adaptive_decision_records();
    assert!(
        decision_records
            .iter()
            .all(|record| record.active_algorithm == ActivePlanningAlgorithm::DirectionalPca)
    );
    assert_eq!(
        decision_records
            .iter()
            .map(|record| record.boundary_position)
            .collect::<Vec<_>>(),
        (0..decision_records.len()).collect::<Vec<_>>()
    );
    assert_eq!(
        report.adaptive_planning,
        Some(
            lexongraph_streaming_indexer::AdaptivePlanningPassTelemetry {
                pass_number: 1,
                switch_occurred: false,
                latest_decision: lexongraph_streaming_indexer::AdaptivePlanningDecisionTelemetry {
                    boundary_position: decision_records
                        .iter()
                        .rev()
                        .find(|record| record.collapse_diagnostics.is_some())
                        .unwrap_or(decision_records.last().unwrap())
                        .boundary_position,
                    active_algorithm: decision_records
                        .iter()
                        .rev()
                        .find(|record| record.collapse_diagnostics.is_some())
                        .unwrap_or(decision_records.last().unwrap())
                        .active_algorithm,
                    switch_boundary_occurred: decision_records
                        .iter()
                        .rev()
                        .find(|record| record.collapse_diagnostics.is_some())
                        .unwrap_or(decision_records.last().unwrap())
                        .switch_boundary_occurred,
                    embedding_count: decision_records.iter().rev().find_map(|record| {
                        record
                            .collapse_diagnostics
                            .as_ref()
                            .map(|diagnostics| diagnostics.embedding_count)
                    }),
                    embedding_count_cutoff: decision_records
                        .iter()
                        .rev()
                        .find_map(|record| record.embedding_count_cutoff),
                    reason: decision_records
                        .iter()
                        .rev()
                        .find(|record| record.collapse_diagnostics.is_some())
                        .unwrap_or(decision_records.last().unwrap())
                        .reason,
                },
                first_switch_boundary_position: None,
            }
        )
    );
    run.mark_planning_complete().unwrap();
    let store = MemoryBlockStore::default();
    let result = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();
    assert!(!result.block_ids.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_042_adaptive_switch_path_falls_back_to_dcbc() {
    let items = switch_trigger_items();
    let mut run = run_with_builtin(adaptive_planning(BuiltInPlanningDirection::Divisive), 160);
    run.ingest_batch(&items).await.unwrap();
    let report = run.finish_pass().unwrap();
    let decision_records = run.adaptive_decision_records();
    assert_eq!(
        decision_records
            .first()
            .map(|record| record.active_algorithm),
        Some(ActivePlanningAlgorithm::DirectionalPca)
    );
    assert!(
        decision_records
            .iter()
            .any(|record| record.active_algorithm == ActivePlanningAlgorithm::Dcbc)
    );
    assert_eq!(
        decision_records
            .iter()
            .map(|record| record.boundary_position)
            .collect::<Vec<_>>(),
        (0..decision_records.len()).collect::<Vec<_>>()
    );
    assert_eq!(
        report.adaptive_planning.as_ref().map(|telemetry| (
            telemetry.pass_number,
            telemetry.switch_occurred,
            telemetry.latest_decision.active_algorithm,
            telemetry.first_switch_boundary_position,
            telemetry.latest_decision.embedding_count_cutoff,
            telemetry.latest_decision.reason,
        )),
        Some((
            1,
            true,
            decision_records
                .iter()
                .rev()
                .find(|record| record.collapse_diagnostics.is_some())
                .unwrap_or(decision_records.last().unwrap())
                .active_algorithm,
            decision_records
                .iter()
                .find(|record| record.switch_boundary_occurred)
                .map(|record| record.boundary_position),
            decision_records
                .iter()
                .rev()
                .find_map(|record| record.embedding_count_cutoff),
            decision_records
                .iter()
                .rev()
                .find(|record| record.collapse_diagnostics.is_some())
                .unwrap_or(decision_records.last().unwrap())
                .reason,
        ))
    );
    assert!(
        report
            .adaptive_planning
            .as_ref()
            .and_then(|telemetry| telemetry.latest_decision.embedding_count)
            .is_some_and(|embedding_count| embedding_count < DEFAULT_EMBEDDING_COUNT_CUTOFF)
    );
    run.mark_planning_complete().unwrap();
    let store = MemoryBlockStore::default();
    let result = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();
    assert!(!result.block_ids.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_043_adaptive_no_switch_telemetry_surfaces_in_pass_report_and_status() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };
    let items = [item("a"), item("m"), item("x"), item("z")];
    let mut run = run_with_builtin(adaptive_planning(BuiltInPlanningDirection::Divisive), 160)
        .with_observer(observer);
    run.ingest_batch(&items).await.unwrap();
    let report = run.finish_pass().unwrap();

    let status_telemetry = unique_adaptive_status_telemetries(&statuses.lock().unwrap().clone());
    assert!(!status_telemetry.is_empty());
    assert!(
        status_telemetry
            .iter()
            .all(|telemetry| telemetry.pass_number == 1)
    );
    assert!(status_telemetry.iter().all(|telemetry| {
        telemetry.decision.active_algorithm == ActivePlanningAlgorithm::DirectionalPca
            && !telemetry.decision.switch_boundary_occurred
    }));
    assert!(status_telemetry.iter().all(|telemetry| {
        telemetry.decision.embedding_count.is_none()
            && telemetry.decision.embedding_count_cutoff.is_none()
            && telemetry.decision.reason
                == AdaptivePlanningDecisionReason::InitialDirectionalPcaSegment
    }));
    assert_eq!(
        status_telemetry
            .iter()
            .map(|telemetry| telemetry.decision.boundary_position)
            .collect::<Vec<_>>(),
        (0..status_telemetry.len()).collect::<Vec<_>>()
    );

    let pass_telemetry = report.adaptive_planning.unwrap();
    let expected_report_decision = run
        .adaptive_decision_records()
        .iter()
        .rev()
        .find(|record| record.collapse_diagnostics.is_some())
        .unwrap_or(run.adaptive_decision_records().last().unwrap());
    assert_eq!(pass_telemetry.pass_number, 1);
    assert!(!pass_telemetry.switch_occurred);
    assert_eq!(
        pass_telemetry.latest_decision.active_algorithm,
        expected_report_decision.active_algorithm
    );
    assert_eq!(pass_telemetry.first_switch_boundary_position, None);
    assert_eq!(
        pass_telemetry.latest_decision.boundary_position,
        expected_report_decision.boundary_position
    );
    assert_eq!(
        pass_telemetry.latest_decision.embedding_count,
        expected_report_decision
            .collapse_diagnostics
            .as_ref()
            .map(|diagnostics| diagnostics.embedding_count)
    );
    assert_eq!(
        pass_telemetry.latest_decision.embedding_count_cutoff,
        expected_report_decision.embedding_count_cutoff
    );
    assert_eq!(
        pass_telemetry.latest_decision.reason,
        expected_report_decision.reason
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_044_adaptive_switch_telemetry_surfaces_in_pass_report_and_status() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };
    let items = switch_trigger_items();
    let mut run = run_with_builtin(adaptive_planning(BuiltInPlanningDirection::Divisive), 160)
        .with_observer(observer);
    run.ingest_batch(&items).await.unwrap();
    let report = run.finish_pass().unwrap();

    let status_telemetry = unique_adaptive_status_telemetries(&statuses.lock().unwrap().clone());
    assert!(!status_telemetry.is_empty());
    assert!(
        status_telemetry
            .iter()
            .all(|telemetry| telemetry.pass_number == 1)
    );
    let switch_boundary = status_telemetry
        .iter()
        .find(|telemetry| telemetry.decision.switch_boundary_occurred)
        .map(|telemetry| telemetry.decision);
    assert!(switch_boundary.is_some());
    assert_eq!(
        status_telemetry.last().unwrap().decision.active_algorithm,
        ActivePlanningAlgorithm::Dcbc
    );

    let pass_telemetry = report.adaptive_planning.unwrap();
    let expected_report_decision = run
        .adaptive_decision_records()
        .iter()
        .rev()
        .find(|record| record.collapse_diagnostics.is_some())
        .unwrap_or(run.adaptive_decision_records().last().unwrap());
    assert_eq!(pass_telemetry.pass_number, 1);
    assert!(pass_telemetry.switch_occurred);
    assert_eq!(
        pass_telemetry.latest_decision.active_algorithm,
        expected_report_decision.active_algorithm
    );
    assert_eq!(
        pass_telemetry.first_switch_boundary_position,
        switch_boundary.map(|decision| decision.boundary_position)
    );
    let switch_boundary = switch_boundary.unwrap();
    let embedding_count = switch_boundary.embedding_count.unwrap();
    let cutoff = switch_boundary.embedding_count_cutoff.unwrap();
    assert!(embedding_count < cutoff);
    assert_eq!(
        switch_boundary.reason,
        AdaptivePlanningDecisionReason::SwitchedToDcbcBelowEmbeddingCountCutoff
    );
    assert_eq!(
        pass_telemetry.latest_decision.embedding_count,
        expected_report_decision
            .collapse_diagnostics
            .as_ref()
            .map(|diagnostics| diagnostics.embedding_count)
    );
    assert_eq!(
        pass_telemetry.latest_decision.embedding_count_cutoff,
        expected_report_decision.embedding_count_cutoff
    );
    assert_eq!(
        pass_telemetry.latest_decision.reason,
        expected_report_decision.reason
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_045_adaptive_switch_telemetry_is_deterministic_across_runs() {
    let items = switch_trigger_items();

    let first_statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let first_observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&first_statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };
    let mut first = run_with_builtin(adaptive_planning(BuiltInPlanningDirection::Divisive), 160)
        .with_observer(first_observer);
    first.ingest_batch(&items).await.unwrap();
    let first_report = first.finish_pass().unwrap();
    let first_telemetry =
        unique_adaptive_status_telemetries(&first_statuses.lock().unwrap().clone());

    let second_statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> =
        Arc::new(Mutex::new(Vec::new()));
    let second_observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&second_statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };
    let mut second = run_with_builtin(adaptive_planning(BuiltInPlanningDirection::Divisive), 160)
        .with_observer(second_observer);
    second.ingest_batch(&items).await.unwrap();
    let second_report = second.finish_pass().unwrap();
    let second_telemetry =
        unique_adaptive_status_telemetries(&second_statuses.lock().unwrap().clone());

    assert_eq!(first_report, second_report);
    assert_eq!(first_telemetry, second_telemetry);
    assert!(first_telemetry.iter().any(|telemetry| {
        telemetry.decision.embedding_count.is_some()
            && telemetry.decision.embedding_count_cutoff == Some(DEFAULT_EMBEDDING_COUNT_CUTOFF)
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_046_adaptive_switch_boundary_is_deterministic() {
    let items = switch_trigger_items();
    let mut first = run_with_builtin(adaptive_planning(BuiltInPlanningDirection::Divisive), 160);
    first.ingest_batch(&items).await.unwrap();
    let first_report = first.finish_pass().unwrap();
    let first_decisions = first.adaptive_decision_records().to_vec();

    let mut second = run_with_builtin(adaptive_planning(BuiltInPlanningDirection::Divisive), 160);
    second.ingest_batch(&items).await.unwrap();
    let second_report = second.finish_pass().unwrap();
    let second_decisions = second.adaptive_decision_records().to_vec();

    assert_eq!(first_report, second_report);
    assert_eq!(first_decisions, second_decisions);
    assert!(
        first_decisions
            .iter()
            .skip_while(|record| !record.switch_boundary_occurred)
            .all(|record| record.active_algorithm == ActivePlanningAlgorithm::Dcbc)
    );
    assert!(first_decisions.iter().any(|record| {
        record.collapse_diagnostics.is_some() && record.embedding_count_cutoff.is_some()
    }));
}

#[test]
fn regression_adaptive_selector_keeps_one_way_switch_records() {
    let mut selector = lexongraph_adaptive_planning_policy::AdaptivePlanningSelector::new(
        AdaptivePlanningSettings {
            direction: AdaptivePlanningDirection::Divisive,
            directional_pca: AdaptiveDirectionalPcaSettings {
                cluster_count: 2,
                random_seed: Some(17),
                params: DirectionalPcaParams {
                    retained_dimension_count: 1,
                    variance_exponent: 1.0,
                    temperature: 1.0,
                    min_input_count: 2,
                    min_effective_rank: 1,
                    min_cumulative_variance: 0.0,
                },
            },
            dcbc: AdaptiveDcbcSettings {
                cluster_count: 2,
                balance_constraints: None,
                random_seed: Some(19),
            },
        },
    )
    .unwrap();
    let switch_fixture = (0..(DEFAULT_EMBEDDING_COUNT_CUTOFF - 1))
        .map(|index| vec![index as f32, 0.0])
        .collect::<Vec<_>>();
    let stay_fixture = (0..DEFAULT_EMBEDDING_COUNT_CUTOFF)
        .map(|index| vec![index as f32, 1.0])
        .collect::<Vec<_>>();
    assert_eq!(
        selector.select_algorithm(&switch_fixture).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    assert_eq!(
        selector.select_algorithm(&switch_fixture).unwrap(),
        ActivePlanningAlgorithm::Dcbc
    );
    assert_eq!(
        selector.select_algorithm(&stay_fixture).unwrap(),
        ActivePlanningAlgorithm::Dcbc
    );
    assert_eq!(
        selector
            .decision_records()
            .iter()
            .map(|record| record.boundary_position)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        selector
            .decision_records()
            .iter()
            .map(|record| record.embedding_count_cutoff)
            .collect::<Vec<_>>(),
        vec![None, Some(DEFAULT_EMBEDDING_COUNT_CUTOFF), None]
    );
}
