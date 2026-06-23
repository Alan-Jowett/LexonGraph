// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Executable verification for docs/specs/rust-streaming-indexer-crate/validation.md

use std::cell::RefCell;
use std::collections::HashMap;
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
use lexongraph_directional_pca::{
    DirectionalPcaAllocationPolicy, DirectionalPcaBinningPolicy,
    DirectionalPcaClusterCardinalityMode, DirectionalPcaRetainedAxisPolicy,
};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_spherical_kmeans::{SphericalInitializationPolicy, SphericalKmeansParams};
use lexongraph_streaming_clustering::{
    ClusterId, MetricDirection, StreamingClusteringConfig, StreamingClusteringError, TrainerState,
};
use lexongraph_streaming_indexer::{
    ActivePlanningAlgorithm, AdaptiveDcbcSettings, AdaptiveDirectionalPcaSettings,
    AdaptivePlanningDirection, AdaptivePlanningSettings, AdaptiveSwitchCriteria,
    DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
};
use lexongraph_streaming_indexer::{
    ArithmeticMeanCanonicalEmbeddingPolicy, BuiltInPlanning, BuiltInPlanningDirection,
    BuiltInPlanningPhase, CanonicalEmbeddingPolicy, ContentResolver, DcbcBuiltInPlanningSettings,
    DirectionalPcaBuiltInPlanningSettings, ExactCentroidChildSummaryPolicy, FinalizedPartition,
    FinalizedPartitionHierarchy, HierarchicalPlanningPolicy, IndexItem, PUBLISHED_PROFILE_V0_1_0,
    PUBLISHED_PROFILE_V0_2_0, PUBLISHED_PROFILE_V0_3_0, PUBLISHED_PROFILE_V0_3_1,
    PUBLISHED_PROFILE_V0_3_2, PUBLISHED_PROFILE_V0_3_3, PUBLISHED_PROFILE_V0_3_4,
    PUBLISHED_PROFILE_V0_3_5, PUBLISHED_PROFILE_V0_3_6, PUBLISHED_PROFILE_V0_3_7,
    PUBLISHED_PROFILE_V0_3_8, PUBLISHED_PROFILE_V0_3_9, PUBLISHED_PROFILE_V0_3_10,
    PUBLISHED_PROFILE_V0_4_0, PUBLISHED_PROFILE_V0_4_1, PUBLISHED_PROFILE_V0_4_2,
    PUBLISHED_PROFILE_V0_4_3, PUBLISHED_PROFILE_V0_4_4, PUBLISHED_PROFILE_V0_4_5,
    PUBLISHED_PROFILE_V0_4_6, PUBLISHED_PROFILE_V0_4_7, PUBLISHED_PROFILE_V0_4_8,
    PUBLISHED_PROFILE_V0_4_9, PlanningPassOutcome, PlanningStage, PublishedHierarchyMetric,
    PublishedPlanningStrategy, PublishedProfilePlanningPolicy, PublishedProfileVersion,
    SphericalKmeansBuiltInPlanningSettings, StreamingClusteringFactory, StreamingIndexerError,
    StreamingIndexingPhase, StreamingIndexingProgressUnitKind, StreamingIndexingRun,
    StreamingIndexingStatus, StreamingIndexingStatusObserver, StreamingIndexingStatusState,
    published_indexing_profile,
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum RecursivePlannerBehavior {
    Successful,
    FinishPassFailure,
    ShortAssignmentBatch,
    UnderfullSuccess,
}

#[derive(Clone, Copy)]
struct SlowRecursiveClusteringFactory {
    behavior: RecursivePlannerBehavior,
}

impl SlowRecursiveClusteringFactory {
    fn successful() -> Self {
        Self {
            behavior: RecursivePlannerBehavior::Successful,
        }
    }

    fn finish_pass_failure() -> Self {
        Self {
            behavior: RecursivePlannerBehavior::FinishPassFailure,
        }
    }

    fn short_assignment_batch() -> Self {
        Self {
            behavior: RecursivePlannerBehavior::ShortAssignmentBatch,
        }
    }

    fn underfull_success() -> Self {
        Self {
            behavior: RecursivePlannerBehavior::UnderfullSuccess,
        }
    }
}

#[derive(Clone)]
struct SlowRecursiveClassifier {
    config: StreamingClusteringConfig,
    threshold: f32,
    behavior: RecursivePlannerBehavior,
}

impl lexongraph_streaming_clustering::StreamingClusterClassifier for SlowRecursiveClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn realized_cluster_count(&self) -> u32 {
        if self.behavior == RecursivePlannerBehavior::UnderfullSuccess {
            2
        } else {
            self.config.cluster_count
        }
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        if embedding.len() != self.config.dimensions {
            return Err(StreamingClusteringError::MalformedInput {
                message: format!(
                    "expected embedding dimensionality {}, got {}",
                    self.config.dimensions,
                    embedding.len()
                ),
            });
        }
        Ok(u32::from(embedding[0] > self.threshold))
    }

    fn assign_batch(
        &self,
        embeddings: &[Vec<f32>],
    ) -> Result<Vec<ClusterId>, StreamingClusteringError> {
        let mut assignments = embeddings
            .iter()
            .map(|embedding| self.assign(embedding.as_slice()))
            .collect::<Result<Vec<_>, _>>()?;
        if self.behavior == RecursivePlannerBehavior::ShortAssignmentBatch
            && !assignments.is_empty()
        {
            assignments.pop();
        }
        Ok(assignments)
    }
}

struct SlowRecursiveTrainer {
    config: StreamingClusteringConfig,
    state: TrainerState,
    embeddings: Vec<Vec<f32>>,
    threshold: Option<f32>,
    behavior: RecursivePlannerBehavior,
}

impl StreamingClusteringFactory for SlowRecursiveClusteringFactory {
    type Trainer = SlowRecursiveTrainer;
    type Error = StreamingClusteringError;

    fn create_trainer(
        &self,
        dimensions: usize,
        _estimated_child_count: usize,
        _block_size_target: usize,
        _embedding_spec: &EmbeddingSpec,
    ) -> Result<Self::Trainer, Self::Error> {
        Ok(SlowRecursiveTrainer {
            config: StreamingClusteringConfig {
                cluster_count: if self.behavior == RecursivePlannerBehavior::UnderfullSuccess {
                    4
                } else {
                    2
                },
                dimensions,
                balance_constraints: None,
                random_seed: Some(7),
            },
            state: TrainerState::Idle,
            embeddings: Vec::new(),
            threshold: None,
            behavior: self.behavior,
        })
    }
}

impl lexongraph_streaming_clustering::StreamingClusterTrainer for SlowRecursiveTrainer {
    type Classifier = SlowRecursiveClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn state(&self) -> TrainerState {
        self.state
    }

    fn ingest_batch(&mut self, embeddings: &[Vec<f32>]) -> Result<(), StreamingClusteringError> {
        self.embeddings = embeddings.to_vec();
        self.state = TrainerState::Ingesting;
        Ok(())
    }

    fn finish_pass(
        &mut self,
    ) -> Result<lexongraph_streaming_clustering::PassReport, StreamingClusteringError> {
        if self.embeddings.is_empty() {
            return Err(StreamingClusteringError::MalformedInput {
                message: "slow recursive trainer requires embeddings".into(),
            });
        }
        thread::sleep(Duration::from_millis(250));
        if self.behavior == RecursivePlannerBehavior::FinishPassFailure {
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::MalformedInput {
                message: "synthetic recursive planning failure".into(),
            });
        }
        let mut sorted = self
            .embeddings
            .iter()
            .map(|embedding| embedding[0])
            .collect::<Vec<_>>();
        sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
        let threshold = sorted[sorted.len() / 2];
        self.threshold = Some(threshold);
        self.state = TrainerState::PassComplete;
        Ok(lexongraph_streaming_clustering::PassReport {
            observed_count: self.embeddings.len(),
            requested_cluster_count: self.config.cluster_count,
            realized_cluster_count: if self.behavior == RecursivePlannerBehavior::UnderfullSuccess {
                2
            } else {
                self.config.cluster_count
            },
            quality_metric: 1.0,
            balance_metric: 0.0,
            quality_direction: MetricDirection::LargerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0, 1],
        })
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        self.state = TrainerState::TrainingComplete;
        Ok(())
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        let Some(threshold) = self.threshold else {
            return Err(StreamingClusteringError::InvalidTransition {
                state: self.state,
                operation: "into_classifier".into(),
            });
        };
        Ok(SlowRecursiveClassifier {
            config: self.config,
            threshold,
            behavior: self.behavior,
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
            requested_cluster_count: None,
            realized_cluster_count: None,
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
            requested_cluster_count: None,
            realized_cluster_count: None,
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
struct SizeOnlyStatusPlanningPolicy;

impl HierarchicalPlanningPolicy for SizeOnlyStatusPlanningPolicy {
    type Error = FixtureError;

    fn finish_planning_pass(
        &mut self,
        embeddings: &[Vec<f32>],
        _: &EmbeddingSpec,
        _: usize,
        _: usize,
    ) -> Result<PlanningPassOutcome, Self::Error> {
        let mut fixed = FixedHierarchyPlanningPolicy;
        fixed.finish_planning_pass(embeddings, &embedding_spec(), 0, 0)
    }

    fn finish_planning_pass_with_status_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
        mut observe_status: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(lexongraph_streaming_indexer::HierarchyPlanningStatusEvent),
    {
        observe_status(lexongraph_streaming_indexer::HierarchyPlanningStatusEvent {
            stage: PlanningStage::Custom,
            state: StreamingIndexingStatusState::InProgress,
            legacy_item_count: embeddings.len(),
            progress_unit_kind: Some(
                StreamingIndexingProgressUnitKind::PartitionPlanningInvocation,
            ),
            completed_unit_count: Some(0),
            discovered_unit_count: Some(1),
            current_partition_path: None,
            current_partition_size: Some(embeddings.len()),
            current_recursion_depth: None,
            started_subproblem_count: Some(1),
            completed_subproblem_count: Some(0),
            visited_partition_count: Some(1),
            finalized_partition_count: Some(0),
            terminal_partition_count: Some(0),
            completed_planner_invocation_count: Some(0),
            fallback_count: Some(0),
        });
        self.finish_planning_pass(
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
        )
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
struct ExplicitStartedSizeOnlyStatusPlanningPolicy;

impl HierarchicalPlanningPolicy for ExplicitStartedSizeOnlyStatusPlanningPolicy {
    type Error = FixtureError;

    fn finish_planning_pass(
        &mut self,
        embeddings: &[Vec<f32>],
        _: &EmbeddingSpec,
        _: usize,
        _: usize,
    ) -> Result<PlanningPassOutcome, Self::Error> {
        let mut fixed = FixedHierarchyPlanningPolicy;
        fixed.finish_planning_pass(embeddings, &embedding_spec(), 0, 0)
    }

    fn finish_planning_pass_with_status_observer<SO>(
        &mut self,
        embeddings: &[Vec<f32>],
        embedding_spec: &EmbeddingSpec,
        materializability_bound: usize,
        block_size_target: usize,
        mut observe_status: SO,
    ) -> Result<PlanningPassOutcome, Self::Error>
    where
        SO: FnMut(lexongraph_streaming_indexer::HierarchyPlanningStatusEvent),
    {
        observe_status(lexongraph_streaming_indexer::HierarchyPlanningStatusEvent {
            stage: PlanningStage::Custom,
            state: StreamingIndexingStatusState::Started,
            legacy_item_count: embeddings.len(),
            progress_unit_kind: Some(
                StreamingIndexingProgressUnitKind::PartitionPlanningInvocation,
            ),
            completed_unit_count: Some(0),
            discovered_unit_count: Some(1),
            current_partition_path: None,
            current_partition_size: Some(embeddings.len()),
            current_recursion_depth: None,
            started_subproblem_count: Some(1),
            completed_subproblem_count: Some(0),
            visited_partition_count: Some(1),
            finalized_partition_count: Some(0),
            terminal_partition_count: Some(0),
            completed_planner_invocation_count: Some(0),
            fallback_count: Some(0),
        });
        self.finish_planning_pass(
            embeddings,
            embedding_spec,
            materializability_bound,
            block_size_target,
        )
    }
}

#[derive(Clone, Default)]
struct NestedHierarchyPlanningPolicy;

impl HierarchicalPlanningPolicy for NestedHierarchyPlanningPolicy {
    type Error = FixtureError;

    fn finish_planning_pass(
        &mut self,
        embeddings: &[Vec<f32>],
        _: &EmbeddingSpec,
        _: usize,
        _: usize,
    ) -> Result<PlanningPassOutcome, Self::Error> {
        if embeddings.len() != 4 {
            return Err(FixtureError(
                "nested hierarchy fixture requires four embeddings".into(),
            ));
        }
        Ok(PlanningPassOutcome {
            hierarchy: FinalizedPartitionHierarchy {
                root_partition_id: "p0".into(),
                partitions: vec![
                    FinalizedPartition {
                        id: "p0".into(),
                        parent_id: None,
                        child_ids: vec!["p0.0".into(), "p0.1".into()],
                        item_indices: vec![0, 1, 2, 3],
                        terminal: false,
                        planning_stage: PlanningStage::Custom,
                    },
                    FinalizedPartition {
                        id: "p0.0".into(),
                        parent_id: Some("p0".into()),
                        child_ids: vec!["p0.0.0".into(), "p0.0.1".into()],
                        item_indices: vec![0, 1, 2],
                        terminal: false,
                        planning_stage: PlanningStage::Custom,
                    },
                    FinalizedPartition {
                        id: "p0.0.0".into(),
                        parent_id: Some("p0.0".into()),
                        child_ids: vec![],
                        item_indices: vec![0],
                        terminal: true,
                        planning_stage: PlanningStage::Custom,
                    },
                    FinalizedPartition {
                        id: "p0.0.1".into(),
                        parent_id: Some("p0.0".into()),
                        child_ids: vec![],
                        item_indices: vec![1, 2],
                        terminal: true,
                        planning_stage: PlanningStage::Custom,
                    },
                    FinalizedPartition {
                        id: "p0.1".into(),
                        parent_id: Some("p0".into()),
                        child_ids: vec![],
                        item_indices: vec![3],
                        terminal: true,
                        planning_stage: PlanningStage::Custom,
                    },
                ],
            },
            requested_cluster_count: None,
            realized_cluster_count: None,
            planning_quality_metric: 1.0,
            planning_balance_metric: 0.0,
            planning_quality_direction: MetricDirection::LargerIsBetter,
            planning_balance_direction: MetricDirection::SmallerIsBetter,
            stages_used: [PlanningStage::Custom].into_iter().collect(),
        })
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
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
            allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    })
}

fn spherical_kmeans_planning(direction: BuiltInPlanningDirection) -> BuiltInPlanning {
    BuiltInPlanning::SphericalKmeans(SphericalKmeansBuiltInPlanningSettings {
        direction,
        cluster_count: 2,
        random_seed: Some(23),
        params: SphericalKmeansParams {
            initialization_policy: SphericalInitializationPolicy::SeededDeterministicFarthestPoint,
            max_iteration_count: 8,
            convergence_tolerance: 0.0,
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
                    retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
                    allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                    binning_policy: DirectionalPcaBinningPolicy::Quantile,
                    cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
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

fn adaptive_planning(
    direction: BuiltInPlanningDirection,
    mean_cluster_radius_threshold: f32,
) -> BuiltInPlanning {
    let adaptive_direction = match direction {
        BuiltInPlanningDirection::Divisive => AdaptivePlanningDirection::Divisive,
        BuiltInPlanningDirection::Agglomerative => AdaptivePlanningDirection::Agglomerative,
    };
    BuiltInPlanning::Adaptive(AdaptivePlanningSettings {
        direction: adaptive_direction,
        directional_pca: AdaptiveDirectionalPcaSettings {
            cluster_count: 2,
            random_seed: Some(17),
            params: DirectionalPcaParams {
                retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
                allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                binning_policy: DirectionalPcaBinningPolicy::Quantile,
                cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
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
        switch_criteria: AdaptiveSwitchCriteria {
            mean_cluster_radius_threshold,
        },
    })
}

fn invalid_adaptive_planning() -> BuiltInPlanning {
    BuiltInPlanning::Adaptive(AdaptivePlanningSettings {
        direction: AdaptivePlanningDirection::Divisive,
        directional_pca: AdaptiveDirectionalPcaSettings {
            cluster_count: 2,
            random_seed: None,
            params: DirectionalPcaParams {
                retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
                allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                binning_policy: DirectionalPcaBinningPolicy::Quantile,
                cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
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
        switch_criteria: AdaptiveSwitchCriteria {
            mean_cluster_radius_threshold: f32::NAN,
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
                    retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
                    allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                    binning_policy: DirectionalPcaBinningPolicy::Quantile,
                    cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
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
                    retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
                    allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                    binning_policy: DirectionalPcaBinningPolicy::Quantile,
                    cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
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

fn built_in_cases() -> [BuiltInAlgorithmCase; 6] {
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
        BuiltInAlgorithmCase {
            name: "spherical-kmeans-divisive",
            planning: spherical_kmeans_planning(BuiltInPlanningDirection::Divisive),
        },
        BuiltInAlgorithmCase {
            name: "spherical-kmeans-agglomerative",
            planning: spherical_kmeans_planning(BuiltInPlanningDirection::Agglomerative),
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
    assert!(src.contains("BuiltInPlanning::SphericalKmeans"));
    assert!(manifest.contains("lexongraph-adaptive-planning-policy"));
    assert!(manifest.contains("lexongraph-dcbc-streaming"));
    assert!(manifest.contains("lexongraph-directional-pca"));
    assert!(manifest.contains("lexongraph-spherical-kmeans"));
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
async fn val_stream_indexer_045_shared_summary_policy_surface_is_reusable() {
    let items = [item("alpha"), item("bravo"), item("charlie"), item("delta")];
    let store = MemoryBlockStore::default();
    let mut run = StreamingIndexingRun::with_summary_policy(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        dcbc_planning(BuiltInPlanningDirection::Divisive),
        embedding_spec(),
        256,
    );
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_planning_complete().unwrap();
    let result = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();

    assert!(!result.block_ids.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_047_exact_centroid_summary_policy_materializes_deterministically() {
    let items = [item("a"), item("j"), item("p"), item("~")];

    let mut first = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ExactCentroidChildSummaryPolicy,
        NestedHierarchyPlanningPolicy,
        embedding_spec(),
        256,
    );
    first.ingest_batch(&items).await.unwrap();
    first.finish_pass().unwrap();
    first.mark_planning_complete().unwrap();
    let first_store = MemoryBlockStore::default();
    let first_result = first
        .finalize(std::iter::once(items.as_slice()), &first_store)
        .await
        .unwrap();

    let mut second = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ExactCentroidChildSummaryPolicy,
        NestedHierarchyPlanningPolicy,
        embedding_spec(),
        256,
    );
    second.ingest_batch(&items).await.unwrap();
    second.finish_pass().unwrap();
    second.mark_planning_complete().unwrap();
    let second_store = MemoryBlockStore::default();
    let second_result = second
        .finalize(std::iter::once(items.as_slice()), &second_store)
        .await
        .unwrap();

    assert_eq!(first_result, second_result);
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_046_exact_centroid_policy_uses_descendant_counts() {
    let items = [item("a"), item("j"), item("p"), item("~")];
    let store = MemoryBlockStore::default();
    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ExactCentroidChildSummaryPolicy,
        NestedHierarchyPlanningPolicy,
        embedding_spec(),
        256,
    );
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_planning_complete().unwrap();
    let result = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();

    let root = store.get(&result.root_id).unwrap().unwrap();
    let TypedEntries::Branch(_, entries) = into_entries(root) else {
        panic!("nested hierarchy should materialize a branch root");
    };
    let branch_entry = entries
        .iter()
        .find(|entry| {
            matches!(
                into_entries(store.get(&entry.child).unwrap().unwrap()),
                TypedEntries::Branch(_, _)
            )
        })
        .unwrap();
    assert_eq!(
        branch_entry
            .embedding
            .iter()
            .map(|byte| i8::from_le_bytes([*byte]))
            .collect::<Vec<_>>(),
        vec![105, 1]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_048_spherical_kmeans_built_in_path_supports_both_directions() {
    let items = [
        item("alpha"),
        item("bravo"),
        item("charlie"),
        item("delta"),
        item("echo"),
        item("foxtrot"),
    ];

    for planning in [
        spherical_kmeans_planning(BuiltInPlanningDirection::Divisive),
        spherical_kmeans_planning(BuiltInPlanningDirection::Agglomerative),
    ] {
        let (store, result) = one_shot(planning, &items, 256).await.unwrap();
        assert!(!result.block_ids.is_empty());
        assert!(store.get(&result.root_id).unwrap().is_some());
    }
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
async fn regression_legacy_hierarchy_statuses_report_explicit_unit_kind() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        FixedHierarchyPlanningPolicy,
        embedding_spec(),
        256,
    )
    .with_observer(observer);
    run.ingest_batch(&[item("alpha"), item("bravo"), item("charlie"), item("delta")])
        .await
        .unwrap();
    run.finish_pass().unwrap();

    let statuses = statuses.lock().unwrap().clone();
    let legacy_hierarchy = statuses
        .iter()
        .find(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::HierarchyPlanning {
                    stage: PlanningStage::Custom
                }
            ) && matches!(
                status.state,
                StreamingIndexingStatusState::Started | StreamingIndexingStatusState::InProgress
            )
        })
        .expect("legacy hierarchy status");
    assert_eq!(
        legacy_hierarchy.progress_unit_kind,
        Some(StreamingIndexingProgressUnitKind::HierarchyPlanningItem)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn regression_started_status_reports_zero_elapsed_for_size_only_unit_descriptor() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        SizeOnlyStatusPlanningPolicy,
        embedding_spec(),
        256,
    )
    .with_observer(observer);
    run.ingest_batch(&[item("alpha"), item("bravo"), item("charlie"), item("delta")])
        .await
        .unwrap();
    run.finish_pass().unwrap();

    let statuses = statuses.lock().unwrap().clone();
    let started = statuses
        .iter()
        .find(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::HierarchyPlanning {
                    stage: PlanningStage::Custom
                }
            ) && status.state == StreamingIndexingStatusState::Started
                && status.current_partition_path.is_none()
                && status.current_partition_size == Some(4)
        })
        .expect("size-only hierarchy started status");
    assert_eq!(started.current_unit_elapsed, Some(Duration::ZERO));
    assert_eq!(started.last_progress_at, Some(Duration::ZERO));
}

#[tokio::test(flavor = "current_thread")]
async fn regression_explicit_started_status_reports_zero_elapsed() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let mut run = StreamingIndexingRun::new(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        ExplicitStartedSizeOnlyStatusPlanningPolicy,
        embedding_spec(),
        256,
    )
    .with_observer(observer);
    run.ingest_batch(&[item("alpha"), item("bravo"), item("charlie"), item("delta")])
        .await
        .unwrap();
    run.finish_pass().unwrap();

    let statuses = statuses.lock().unwrap().clone();
    let started = statuses
        .iter()
        .find(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::HierarchyPlanning {
                    stage: PlanningStage::Custom
                }
            ) && status.state == StreamingIndexingStatusState::Started
                && status.current_partition_path.is_none()
                && status.current_partition_size == Some(4)
        })
        .expect("explicit size-only hierarchy started status");
    assert_eq!(started.current_unit_elapsed, Some(Duration::ZERO));
    assert_eq!(started.last_progress_at, Some(started.elapsed));
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
            .filter(|status| {
                status.state == StreamingIndexingStatusState::Started
                    && !matches!(
                        status.phase,
                        StreamingIndexingPhase::HierarchyPlanning { .. }
                    )
            })
            .all(|status| status.elapsed.is_zero() && status.completed_unit_count == 0)
    );
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::HierarchyPlanning { .. }
        ) && status.progress_unit_kind
            == Some(StreamingIndexingProgressUnitKind::PartitionPlanningInvocation)
    }));
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
    assert!(hierarchy_statuses.iter().any(|status| {
        status.progress_unit_kind
            == Some(StreamingIndexingProgressUnitKind::PartitionPlanningInvocation)
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
    let hierarchy_failed = statuses
        .iter()
        .find(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::HierarchyPlanning {
                    stage: PlanningStage::Custom
                }
            ) && status.state == StreamingIndexingStatusState::Failed
        })
        .expect("hierarchy planning failure status");
    assert_eq!(
        hierarchy_failed.item_count,
        hierarchy_failed.completed_unit_count
    );
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
        adaptive_planning(
            BuiltInPlanningDirection::Divisive,
            DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
        ),
        adaptive_planning(
            BuiltInPlanningDirection::Agglomerative,
            DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
        ),
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
    let mut run = run_with_builtin(
        adaptive_planning(BuiltInPlanningDirection::Divisive, f32::MAX),
        160,
    );
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    assert!(
        run.adaptive_decision_records()
            .iter()
            .all(|record| record.active_algorithm == ActivePlanningAlgorithm::DirectionalPca)
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
    let mut run = run_with_builtin(
        adaptive_planning(
            BuiltInPlanningDirection::Divisive,
            DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
        ),
        160,
    );
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
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
    run.mark_planning_complete().unwrap();
    let store = MemoryBlockStore::default();
    let result = run
        .finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();
    assert!(!result.block_ids.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_043_adaptive_switch_boundary_is_deterministic() {
    let items = switch_trigger_items();
    let mut first = run_with_builtin(
        adaptive_planning(
            BuiltInPlanningDirection::Divisive,
            DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
        ),
        160,
    );
    first.ingest_batch(&items).await.unwrap();
    let first_report = first.finish_pass().unwrap();
    first.mark_planning_complete().unwrap();
    let first_store = MemoryBlockStore::default();
    let first_result = first
        .finalize(std::iter::once(items.as_slice()), &first_store)
        .await
        .unwrap();
    let first_decisions = first.adaptive_decision_records().to_vec();

    let mut second = run_with_builtin(
        adaptive_planning(
            BuiltInPlanningDirection::Divisive,
            DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
        ),
        160,
    );
    second.ingest_batch(&items).await.unwrap();
    let second_report = second.finish_pass().unwrap();
    second.mark_planning_complete().unwrap();
    let second_store = MemoryBlockStore::default();
    let second_result = second
        .finalize(std::iter::once(items.as_slice()), &second_store)
        .await
        .unwrap();
    let second_decisions = second.adaptive_decision_records().to_vec();
    let first_algorithm_sequence: Vec<_> = first_decisions
        .iter()
        .map(|record| record.active_algorithm)
        .collect();
    let second_algorithm_sequence: Vec<_> = second_decisions
        .iter()
        .map(|record| record.active_algorithm)
        .collect();
    let first_switch_boundaries: Vec<_> = first_decisions
        .iter()
        .enumerate()
        .filter_map(|(index, record)| record.switch_boundary_occurred.then_some(index))
        .collect();
    let second_switch_boundaries: Vec<_> = second_decisions
        .iter()
        .enumerate()
        .filter_map(|(index, record)| record.switch_boundary_occurred.then_some(index))
        .collect();
    let first_reasons: Vec<_> = first_decisions.iter().map(|record| record.reason).collect();
    let second_reasons: Vec<_> = second_decisions
        .iter()
        .map(|record| record.reason)
        .collect();

    assert_eq!(first_report, second_report);
    assert_eq!(first_result, second_result);
    assert_eq!(first_algorithm_sequence, second_algorithm_sequence);
    assert_eq!(first_switch_boundaries, second_switch_boundaries);
    assert_eq!(first_reasons, second_reasons);
}

#[test]
fn val_stream_indexer_044_adaptive_selector_keeps_one_way_switch_records() {
    let mut selector = lexongraph_adaptive_planning_policy::AdaptivePlanningSelector::new(
        AdaptivePlanningSettings {
            direction: AdaptivePlanningDirection::Divisive,
            directional_pca: AdaptiveDirectionalPcaSettings {
                cluster_count: 2,
                random_seed: Some(17),
                params: DirectionalPcaParams {
                    retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
                    allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                    binning_policy: DirectionalPcaBinningPolicy::Quantile,
                    cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
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
            switch_criteria: AdaptiveSwitchCriteria {
                mean_cluster_radius_threshold: DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
            },
        },
    )
    .unwrap();
    let square = vec![
        vec![-1.0, -1.0],
        vec![-1.0, 1.0],
        vec![1.0, -1.0],
        vec![1.0, 1.0],
    ];
    let line = vec![
        vec![-3.0, 0.0],
        vec![-1.0, 0.0],
        vec![1.0, 0.0],
        vec![3.0, 0.0],
    ];
    assert_eq!(
        selector.select_algorithm(square.len(), &square).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    assert_eq!(
        selector.select_algorithm(square.len(), &square).unwrap(),
        ActivePlanningAlgorithm::Dcbc
    );
    assert_eq!(
        selector.select_algorithm(line.len(), &line).unwrap(),
        ActivePlanningAlgorithm::Dcbc
    );
}

#[test]
fn val_stream_indexer_049_published_profile_v0_1_0_is_declared_explicitly() {
    let profile = published_indexing_profile(PUBLISHED_PROFILE_V0_1_0).unwrap();

    assert_eq!(profile.version, PublishedProfileVersion::new(0, 1, 0));
    assert_eq!(profile.planning_algorithm_id, "spherical-kmeans");
    assert_eq!(profile.planning_direction, None);
    assert_eq!(
        profile.packing_strategy_id,
        Some("cluster-order-balanced-range-packer-v1")
    );
    assert_eq!(profile.hierarchy_strategy_id, "greedy-pack");
    assert_eq!(profile.summary_policy_id, "exact-centroid");
    match profile.planning_strategy {
        PublishedPlanningStrategy::SphericalKmeansGreedyPack(settings) => {
            assert_eq!(settings.cluster_count, 157);
            assert_eq!(settings.random_seed, Some(11));
            assert_eq!(
                settings.params.initialization_policy,
                SphericalInitializationPolicy::SeededDeterministicFarthestPoint
            );
            assert_eq!(settings.params.max_iteration_count, 32);
            assert_eq!(settings.params.convergence_tolerance, 1.0e-4);
            assert_eq!(
                settings.hierarchy_metric,
                PublishedHierarchyMetric::Euclidean
            );
        }
        other => panic!("unexpected published planning strategy: {other:?}"),
    }
}

#[test]
fn val_stream_indexer_050_unknown_published_profile_is_rejected() {
    let error = published_indexing_profile(PublishedProfileVersion::new(9, 9, 9)).unwrap_err();
    assert!(matches!(
        error,
        StreamingIndexerError::UnsupportedPublishedProfileVersion(version)
            if version == PublishedProfileVersion::new(9, 9, 9)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_051_published_profile_materializes_deterministically() {
    let items = [item("a"), item("j"), item("p"), item("~")];

    let mut first = StreamingIndexingRun::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_1_0,
        embedding_spec(),
        256,
    )
    .unwrap();
    first.ingest_batch(&items).await.unwrap();
    first.finish_pass().unwrap();
    first.mark_planning_complete().unwrap();
    let first_store = MemoryBlockStore::default();
    let first_result = first
        .finalize(std::iter::once(items.as_slice()), &first_store)
        .await
        .unwrap();

    let mut second = StreamingIndexingRun::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_1_0,
        embedding_spec(),
        256,
    )
    .unwrap();
    second.ingest_batch(&items).await.unwrap();
    second.finish_pass().unwrap();
    second.mark_planning_complete().unwrap();
    let second_store = MemoryBlockStore::default();
    let second_result = second
        .finalize(std::iter::once(items.as_slice()), &second_store)
        .await
        .unwrap();

    assert_eq!(first_result, second_result);
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_052_published_profile_v0_2_0_is_declared_explicitly() {
    let profile = published_indexing_profile(PUBLISHED_PROFILE_V0_2_0).unwrap();

    assert_eq!(profile.version, PublishedProfileVersion::new(0, 2, 0));
    assert_eq!(profile.planning_algorithm_id, "directional-pca");
    assert_eq!(
        profile.planning_direction,
        Some(BuiltInPlanningDirection::Divisive)
    );
    assert_eq!(profile.packing_strategy_id, None);
    assert_eq!(profile.hierarchy_strategy_id, "built-in-divisive");
    assert_eq!(profile.summary_policy_id, "exact-centroid");
    match profile.planning_strategy {
        PublishedPlanningStrategy::DirectionalPcaDivisive(settings) => {
            assert_eq!(settings.cluster_count, 2);
            assert_eq!(settings.random_seed, Some(7));
            assert_eq!(
                settings.params,
                DirectionalPcaParams {
                    retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
                    allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                    binning_policy: DirectionalPcaBinningPolicy::Quantile,
                    cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
                    variance_exponent: 1.0,
                    temperature: 1.0,
                    min_input_count: 2,
                    min_effective_rank: 1,
                    min_cumulative_variance: 0.0,
                }
            );
        }
        other => panic!("unexpected published planning strategy: {other:?}"),
    }

    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };
    let items = [item("a"), item("j"), item("p"), item("~")];
    let store = MemoryBlockStore::default();
    let mut run = StreamingIndexingRun::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_2_0,
        embedding_spec(),
        128,
    )
    .unwrap()
    .with_observer(observer);
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_planning_complete().unwrap();
    run.finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();

    let statuses = statuses.lock().unwrap().clone();
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::BottomUpAssembly { .. }
        )
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_053_published_profile_v0_2_0_materializes_deterministically() {
    let items = [item("a"), item("j"), item("p"), item("~")];

    let mut first = StreamingIndexingRun::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_2_0,
        embedding_spec(),
        128,
    )
    .unwrap();
    first.ingest_batch(&items).await.unwrap();
    first.finish_pass().unwrap();
    first.mark_planning_complete().unwrap();
    let first_store = MemoryBlockStore::default();
    let first_result = first
        .finalize(std::iter::once(items.as_slice()), &first_store)
        .await
        .unwrap();

    let mut second = StreamingIndexingRun::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_2_0,
        embedding_spec(),
        128,
    )
    .unwrap();
    second.ingest_batch(&items).await.unwrap();
    second.finish_pass().unwrap();
    second.mark_planning_complete().unwrap();
    let second_store = MemoryBlockStore::default();
    let second_result = second
        .finalize(std::iter::once(items.as_slice()), &second_store)
        .await
        .unwrap();

    assert_eq!(first_result, second_result);
}

#[test]
fn val_stream_indexer_054_both_published_profiles_remain_resolvable() {
    let v0_1_0 = published_indexing_profile(PUBLISHED_PROFILE_V0_1_0).unwrap();
    let v0_2_0 = published_indexing_profile(PUBLISHED_PROFILE_V0_2_0).unwrap();

    assert_eq!(v0_1_0.version, PublishedProfileVersion::new(0, 1, 0));
    assert_eq!(v0_2_0.version, PublishedProfileVersion::new(0, 2, 0));
    assert_eq!(v0_1_0.planning_algorithm_id, "spherical-kmeans");
    assert_eq!(v0_2_0.planning_algorithm_id, "directional-pca");
    assert_ne!(v0_1_0.hierarchy_strategy_id, v0_2_0.hierarchy_strategy_id);
}

#[test]
fn val_stream_indexer_055_published_profile_v0_2_1_is_not_published() {
    let error = published_indexing_profile(PublishedProfileVersion::new(0, 2, 1)).unwrap_err();
    assert!(matches!(
        error,
        StreamingIndexerError::UnsupportedPublishedProfileVersion(version)
            if version == PublishedProfileVersion::new(0, 2, 1)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_056_published_profile_v0_3_0_is_declared_explicitly() {
    let profile = published_indexing_profile(PUBLISHED_PROFILE_V0_3_0).unwrap();

    assert_eq!(profile.version, PublishedProfileVersion::new(0, 3, 0));
    assert_eq!(profile.planning_algorithm_id, "directional-pca");
    assert_eq!(
        profile.planning_direction,
        Some(BuiltInPlanningDirection::Divisive)
    );
    assert_eq!(profile.packing_strategy_id, None);
    assert_eq!(profile.hierarchy_strategy_id, "built-in-divisive");
    assert_eq!(profile.summary_policy_id, "exact-centroid");
    match profile.planning_strategy {
        PublishedPlanningStrategy::DirectionalPcaDivisive(settings) => {
            assert_eq!(settings.cluster_count, 64);
            assert_eq!(settings.random_seed, Some(7));
            assert_eq!(
                settings.params,
                DirectionalPcaParams {
                    retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                    allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                    binning_policy: DirectionalPcaBinningPolicy::DensityValley,
                    cluster_cardinality_mode:
                        DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
                    variance_exponent: 1.0,
                    temperature: 1.0,
                    min_input_count: 2,
                    min_effective_rank: 1,
                    min_cumulative_variance: 0.0,
                }
            );
        }
        other => panic!("unexpected published planning strategy: {other:?}"),
    }

    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };
    let items = [item("a"), item("j"), item("p"), item("~")];
    let store = MemoryBlockStore::default();
    let mut run = StreamingIndexingRun::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_3_0,
        embedding_spec(),
        128,
    )
    .unwrap()
    .with_observer(observer);
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();
    run.mark_planning_complete().unwrap();
    run.finalize(std::iter::once(items.as_slice()), &store)
        .await
        .unwrap();

    let statuses = statuses.lock().unwrap().clone();
    assert!(statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::BottomUpAssembly { .. }
        )
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_057_published_profile_v0_3_0_materializes_deterministically() {
    let items = [item("a"), item("j"), item("p"), item("~")];

    let mut first = StreamingIndexingRun::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_3_0,
        embedding_spec(),
        128,
    )
    .unwrap();
    first.ingest_batch(&items).await.unwrap();
    first.finish_pass().unwrap();
    first.mark_planning_complete().unwrap();
    let first_store = MemoryBlockStore::default();
    let first_result = first
        .finalize(std::iter::once(items.as_slice()), &first_store)
        .await
        .unwrap();

    let mut second = StreamingIndexingRun::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_3_0,
        embedding_spec(),
        128,
    )
    .unwrap();
    second.ingest_batch(&items).await.unwrap();
    second.finish_pass().unwrap();
    second.mark_planning_complete().unwrap();
    let second_store = MemoryBlockStore::default();
    let second_result = second
        .finalize(std::iter::once(items.as_slice()), &second_store)
        .await
        .unwrap();

    assert_eq!(first_result, second_result);
}

#[test]
fn val_stream_indexer_058_all_published_profiles_remain_resolvable() {
    let v0_1_0 = published_indexing_profile(PUBLISHED_PROFILE_V0_1_0).unwrap();
    let v0_2_0 = published_indexing_profile(PUBLISHED_PROFILE_V0_2_0).unwrap();
    let v0_3_0 = published_indexing_profile(PUBLISHED_PROFILE_V0_3_0).unwrap();

    assert_eq!(v0_1_0.version, PublishedProfileVersion::new(0, 1, 0));
    assert_eq!(v0_2_0.version, PublishedProfileVersion::new(0, 2, 0));
    assert_eq!(v0_3_0.version, PublishedProfileVersion::new(0, 3, 0));
    assert_eq!(v0_1_0.planning_algorithm_id, "spherical-kmeans");
    assert_eq!(v0_2_0.planning_algorithm_id, "directional-pca");
    assert_eq!(v0_3_0.planning_algorithm_id, "directional-pca");
    assert_ne!(v0_1_0.hierarchy_strategy_id, v0_2_0.hierarchy_strategy_id);
    assert_eq!(v0_2_0.hierarchy_strategy_id, v0_3_0.hierarchy_strategy_id);
    match v0_2_0.planning_strategy {
        PublishedPlanningStrategy::DirectionalPcaDivisive(settings) => {
            assert_eq!(settings.cluster_count, 2);
            assert_eq!(
                settings.params.cluster_cardinality_mode,
                DirectionalPcaClusterCardinalityMode::Exact
            );
        }

        other => panic!("unexpected published planning strategy: {other:?}"),
    }
    match v0_3_0.planning_strategy {
        PublishedPlanningStrategy::DirectionalPcaDivisive(settings) => {
            assert_eq!(settings.cluster_count, 64);
            assert_eq!(
                settings.params.retained_axis_policy,
                DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible
            );
            assert_eq!(
                settings.params.allocation_policy,
                DirectionalPcaAllocationPolicy::EigenvalueLogBits
            );
            assert_eq!(
                settings.params.binning_policy,
                DirectionalPcaBinningPolicy::DensityValley
            );
            assert_eq!(
                settings.params.cluster_cardinality_mode,
                DirectionalPcaClusterCardinalityMode::UnderfullSuccess
            );
        }
        other => panic!("unexpected published planning strategy: {other:?}"),
    }
}

fn assert_directional_pca_published_profile(
    version: PublishedProfileVersion,
    expected_cluster_count: u32,
    expected_params: DirectionalPcaParams,
) {
    let profile = published_indexing_profile(version).unwrap();
    assert_eq!(profile.version, version);
    assert_eq!(profile.planning_algorithm_id, "directional-pca");
    assert_eq!(
        profile.planning_direction,
        Some(BuiltInPlanningDirection::Divisive)
    );
    assert_eq!(profile.packing_strategy_id, None);
    assert_eq!(profile.hierarchy_strategy_id, "built-in-divisive");
    assert_eq!(profile.summary_policy_id, "exact-centroid");
    match profile.planning_strategy {
        PublishedPlanningStrategy::DirectionalPcaDivisive(settings) => {
            assert_eq!(settings.cluster_count, expected_cluster_count);
            assert_eq!(settings.random_seed, Some(7));
            assert_eq!(settings.params, expected_params);
        }
        other => panic!("unexpected published planning strategy: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_058b_underfull_success_is_reported_in_indexing_pass_reports() {
    let items = switch_trigger_items();
    let mut run = StreamingIndexingRun::with_streaming_clustering_factory(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        SlowRecursiveClusteringFactory::underfull_success(),
        embedding_spec(),
        128,
    );
    run.ingest_batch(&items).await.unwrap();

    let report = run.finish_pass().unwrap();
    assert_eq!(report.requested_planning_cluster_count, Some(4));
    assert_eq!(report.realized_planning_cluster_count, Some(2));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_059_recursive_planning_emits_live_current_unit_updates() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let items = switch_trigger_items();
    let mut run = StreamingIndexingRun::with_streaming_clustering_factory(
        MapResolver,
        AsciiEmbeddingProvider,
        ArithmeticMeanCanonicalEmbeddingPolicy,
        SlowRecursiveClusteringFactory::successful(),
        embedding_spec(),
        128,
    )
    .with_observer(observer);
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();

    let statuses = statuses.lock().unwrap().clone();
    let planning_pass_completed = statuses
        .iter()
        .position(|status| {
            matches!(status.phase, StreamingIndexingPhase::PlanningPass { .. })
                && status.state == StreamingIndexingStatusState::Completed
        })
        .expect("planning pass completion status");
    let hierarchy_statuses: Vec<_> = statuses[..planning_pass_completed]
        .iter()
        .filter(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::HierarchyPlanning { .. }
            )
        })
        .collect();
    assert!(!hierarchy_statuses.is_empty());
    assert!(hierarchy_statuses.iter().any(|status| {
        status.progress_unit_kind
            == Some(StreamingIndexingProgressUnitKind::PartitionPlanningInvocation)
    }));
    assert!(hierarchy_statuses.iter().any(|status| {
        status.state == StreamingIndexingStatusState::Completed
            && status.item_count == status.completed_unit_count
    }));
    assert!(
        hierarchy_statuses
            .iter()
            .enumerate()
            .any(|(index, status)| {
                status.state == StreamingIndexingStatusState::Started
                    && status.current_partition_path.is_some()
                    && hierarchy_statuses[index + 1..]
                        .iter()
                        .filter(|later| {
                            later.current_partition_path == status.current_partition_path
                                && later.state == StreamingIndexingStatusState::Completed
                        })
                        .any(|completed| {
                            completed.completed_unit_count > status.completed_unit_count
                        })
            })
    );

    let root_in_progress: Vec<_> = hierarchy_statuses
        .iter()
        .filter(|status| {
            status.state == StreamingIndexingStatusState::InProgress
                && status.current_partition_path.as_deref() == Some("p0")
        })
        .collect();
    assert!(
        root_in_progress.len() >= 2,
        "slow recursive trainer should trigger multiple live updates for the root partition"
    );
    assert!(root_in_progress.iter().all(|status| {
        status.current_unit_elapsed.is_some()
            && status.current_partition_size.is_some()
            && status.current_recursion_depth == Some(0)
            && status.last_progress_at.is_some()
    }));
    assert!(
        root_in_progress
            .windows(2)
            .all(|pair| pair[1].current_unit_elapsed.unwrap()
                >= pair[0].current_unit_elapsed.unwrap())
    );
    assert!(root_in_progress.windows(2).any(|pair| {
        pair[0].completed_unit_count == pair[1].completed_unit_count
            && pair[1].last_progress_at.unwrap() > pair[0].last_progress_at.unwrap()
    }));
    assert!(hierarchy_statuses.iter().any(|status| {
        status.state == StreamingIndexingStatusState::InProgress && status.completed_unit_count > 0
    }));
    assert!(hierarchy_statuses.iter().any(|status| {
        status
            .current_partition_path
            .as_deref()
            .is_some_and(|path| path != "p0")
    }));
    assert!(hierarchy_statuses.iter().any(|status| {
        status
            .current_partition_path
            .as_deref()
            .is_some_and(|path| path != "p0")
            && status.current_recursion_depth == Some(1)
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_060_published_directional_pca_reports_structured_recursive_progress() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let items = switch_trigger_items();
    let mut run = StreamingIndexingRun::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_2_0,
        embedding_spec(),
        128,
    )
    .unwrap()
    .with_observer(observer);
    run.ingest_batch(&items).await.unwrap();
    run.finish_pass().unwrap();

    let statuses = statuses.lock().unwrap().clone();
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
    assert!(hierarchy_statuses.iter().any(|status| {
        status.progress_unit_kind
            == Some(StreamingIndexingProgressUnitKind::PartitionPlanningInvocation)
    }));
    assert!(hierarchy_statuses.iter().any(|status| {
        status.current_partition_path.as_deref() == Some("p0")
            && status.current_partition_size.is_some()
            && status.current_recursion_depth == Some(0)
    }));
    assert!(hierarchy_statuses.iter().any(|status| {
        status
            .started_subproblem_count
            .zip(status.discovered_unit_count)
            .is_some_and(|(started, discovered)| started >= discovered)
    }));
    assert!(hierarchy_statuses.iter().any(|status| {
        status
            .current_partition_path
            .as_deref()
            .is_some_and(|path| path != "p0")
            && status.current_recursion_depth == Some(1)
    }));
    assert!(hierarchy_statuses.iter().any(|status| {
        status.discovered_unit_count.is_some()
            && status.completed_planner_invocation_count == Some(status.completed_unit_count)
    }));
    assert!(hierarchy_statuses.iter().any(|status| {
        status
            .completed_subproblem_count
            .zip(status.completed_planner_invocation_count)
            .is_some_and(|(completed_subproblems, completed_invocations)| {
                completed_subproblems >= completed_invocations
            })
    }));
    assert!(
        hierarchy_statuses
            .iter()
            .any(|status| status.last_progress_at.is_some())
    );

    let monotonic_recursive_counters = hierarchy_statuses
        .iter()
        .filter(|status| {
            status.progress_unit_kind
                == Some(StreamingIndexingProgressUnitKind::PartitionPlanningInvocation)
        })
        .try_fold((0, 0, 0, 0), |previous, status| {
            let discovered = status.discovered_unit_count.unwrap_or(previous.0);
            let completed = status.completed_unit_count;
            let visited = status.visited_partition_count.unwrap_or(previous.2);
            let finalized = status.finalized_partition_count.unwrap_or(previous.3);
            if discovered < previous.0
                || completed < previous.1
                || visited < previous.2
                || finalized < previous.3
            {
                return None;
            }
            Some((discovered, completed, visited, finalized))
        })
        .is_some();
    assert!(monotonic_recursive_counters);
}

#[test]
fn val_stream_indexer_061_published_profile_v0_3_1_increases_fanout() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_3_1,
        128,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::DensityValley,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_062_published_profile_v0_3_2_decreases_fanout() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_3_2,
        32,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::DensityValley,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_063_published_profile_v0_3_3_switches_to_quantile() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_3_3,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_064_published_profile_v0_3_4_reverts_to_pc1_only() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_3_4,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
            allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_065_published_profile_v0_3_5_uses_centroid_weighted_density_valley() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_3_5,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
            binning_policy: DirectionalPcaBinningPolicy::DensityValley,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_066_published_profile_v0_3_6_caps_retained_axes_at_two() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_3_6,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(2),
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::DensityValley,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_067_published_profile_v0_3_7_caps_retained_axes_at_three() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_3_7,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(3),
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::DensityValley,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_068_published_profile_v0_3_8_raises_cumulative_variance_floor() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_3_8,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::DensityValley,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.5,
        },
    );
}

#[test]
fn val_stream_indexer_069_published_profile_v0_3_9_raises_effective_rank_floor() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_3_9,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::DensityValley,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 2,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_070_published_profile_v0_3_10_restores_exact_cardinality() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_3_10,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::DensityValley,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_071_experiment_profiles_do_not_mutate_v0_3_0() {
    let baseline = published_indexing_profile(PUBLISHED_PROFILE_V0_3_0).unwrap();
    for version in [
        PUBLISHED_PROFILE_V0_3_1,
        PUBLISHED_PROFILE_V0_3_2,
        PUBLISHED_PROFILE_V0_3_3,
        PUBLISHED_PROFILE_V0_3_4,
        PUBLISHED_PROFILE_V0_3_5,
        PUBLISHED_PROFILE_V0_3_6,
        PUBLISHED_PROFILE_V0_3_7,
        PUBLISHED_PROFILE_V0_3_8,
        PUBLISHED_PROFILE_V0_3_9,
        PUBLISHED_PROFILE_V0_3_10,
    ] {
        published_indexing_profile(version).unwrap();
    }

    assert_eq!(
        published_indexing_profile(PUBLISHED_PROFILE_V0_3_0).unwrap(),
        baseline
    );
}

#[test]
fn val_stream_indexer_072_all_experiment_profiles_resolve_deterministically() {
    for version in [
        PUBLISHED_PROFILE_V0_1_0,
        PUBLISHED_PROFILE_V0_2_0,
        PUBLISHED_PROFILE_V0_3_0,
        PUBLISHED_PROFILE_V0_3_1,
        PUBLISHED_PROFILE_V0_3_2,
        PUBLISHED_PROFILE_V0_3_3,
        PUBLISHED_PROFILE_V0_3_4,
        PUBLISHED_PROFILE_V0_3_5,
        PUBLISHED_PROFILE_V0_3_6,
        PUBLISHED_PROFILE_V0_3_7,
        PUBLISHED_PROFILE_V0_3_8,
        PUBLISHED_PROFILE_V0_3_9,
        PUBLISHED_PROFILE_V0_3_10,
        // 0.4.x ladder determinism is validated separately in VAL-STREAM-INDEXER-084.
    ] {
        assert_eq!(
            published_indexing_profile(version).unwrap(),
            published_indexing_profile(version).unwrap()
        );
    }
}

#[test]
fn val_stream_indexer_073_published_profile_v0_4_0_uses_quantile_as_baseline() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_4_0,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_074_published_profile_v0_4_1_increases_fanout() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_4_1,
        128,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_075_published_profile_v0_4_2_decreases_fanout() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_4_2,
        32,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_076_published_profile_v0_4_3_reverts_to_pc1_only() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_4_3,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
            allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_077_published_profile_v0_4_4_selects_centroid_weighted_allocation() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_4_4,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_078_published_profile_v0_4_5_caps_retained_axes_at_two() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_4_5,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(2),
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_079_published_profile_v0_4_6_caps_retained_axes_at_three() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_4_6,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(3),
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_080_published_profile_v0_4_7_raises_minimum_cumulative_variance() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_4_7,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.5,
        },
    );
}

#[test]
fn val_stream_indexer_081_published_profile_v0_4_8_raises_minimum_effective_rank() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_4_8,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::UnderfullSuccess,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 2,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_082_published_profile_v0_4_9_restores_exact_cardinality() {
    assert_directional_pca_published_profile(
        PUBLISHED_PROFILE_V0_4_9,
        64,
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    );
}

#[test]
fn val_stream_indexer_083_experiment_profiles_do_not_mutate_v0_4_0() {
    let baseline = published_indexing_profile(PUBLISHED_PROFILE_V0_4_0).unwrap();
    for version in [
        PUBLISHED_PROFILE_V0_4_1,
        PUBLISHED_PROFILE_V0_4_2,
        PUBLISHED_PROFILE_V0_4_3,
        PUBLISHED_PROFILE_V0_4_4,
        PUBLISHED_PROFILE_V0_4_5,
        PUBLISHED_PROFILE_V0_4_6,
        PUBLISHED_PROFILE_V0_4_7,
        PUBLISHED_PROFILE_V0_4_8,
        PUBLISHED_PROFILE_V0_4_9,
    ] {
        published_indexing_profile(version).unwrap();
    }

    assert_eq!(
        published_indexing_profile(PUBLISHED_PROFILE_V0_4_0).unwrap(),
        baseline
    );
}

#[test]
fn val_stream_indexer_084_all_profiles_resolve_deterministically_with_parallel_ladders() {
    for version in [
        PUBLISHED_PROFILE_V0_1_0,
        PUBLISHED_PROFILE_V0_2_0,
        PUBLISHED_PROFILE_V0_3_0,
        PUBLISHED_PROFILE_V0_3_1,
        PUBLISHED_PROFILE_V0_3_2,
        PUBLISHED_PROFILE_V0_3_3,
        PUBLISHED_PROFILE_V0_3_4,
        PUBLISHED_PROFILE_V0_3_5,
        PUBLISHED_PROFILE_V0_3_6,
        PUBLISHED_PROFILE_V0_3_7,
        PUBLISHED_PROFILE_V0_3_8,
        PUBLISHED_PROFILE_V0_3_9,
        PUBLISHED_PROFILE_V0_3_10,
        PUBLISHED_PROFILE_V0_4_0,
        PUBLISHED_PROFILE_V0_4_1,
        PUBLISHED_PROFILE_V0_4_2,
        PUBLISHED_PROFILE_V0_4_3,
        PUBLISHED_PROFILE_V0_4_4,
        PUBLISHED_PROFILE_V0_4_5,
        PUBLISHED_PROFILE_V0_4_6,
        PUBLISHED_PROFILE_V0_4_7,
        PUBLISHED_PROFILE_V0_4_8,
        PUBLISHED_PROFILE_V0_4_9,
    ] {
        assert_eq!(
            published_indexing_profile(version).unwrap(),
            published_indexing_profile(version).unwrap()
        );
    }
}

#[test]
fn val_stream_indexer_085_v0_4_profiles_fail_when_materializability_conflicts_with_requested_fanout()
 {
    let result = StreamingIndexingRun::<
        &'static str,
        _,
        _,
        ExactCentroidChildSummaryPolicy,
        PublishedProfilePlanningPolicy,
    >::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_4_1,
        embedding_spec(),
        128,
    );
    let error: StreamingIndexerError = match result {
        Ok(_) => {
            panic!("v0.4.1 should fail when materializability conflicts with requested fanout")
        }
        Err(error) => error,
    };

    let message = error.to_string();
    assert!(
        message.contains("published profile 0.4.1 requires cluster_count 128"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("block-size/materializability bound"),
        "unexpected error: {message}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn val_stream_indexer_086_v0_4_profiles_allow_emergent_underfill_from_too_few_children() {
    let items = (0..220)
        .map(|index| {
            let content_ref = Box::leak(format!("item-{index:03}").into_boxed_str());
            item(content_ref)
        })
        .collect::<Vec<_>>();
    let mut run = StreamingIndexingRun::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_4_1,
        embedding_spec(),
        6000,
    )
    .unwrap();
    run.ingest_batch(items.as_slice()).await.unwrap();
    let report = run.finish_pass().unwrap();
    assert_eq!(report.requested_planning_cluster_count, Some(64));
    assert_eq!(report.realized_planning_cluster_count, Some(64));
}

#[test]
fn val_stream_indexer_087_v0_3_profiles_keep_legacy_materializability_clipping_behavior() {
    let run = StreamingIndexingRun::<
        &'static str,
        _,
        _,
        ExactCentroidChildSummaryPolicy,
        PublishedProfilePlanningPolicy,
    >::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_3_1,
        embedding_spec(),
        128,
    )
    .unwrap();
    drop(run);
}

#[tokio::test(flavor = "current_thread")]
async fn regression_recursive_failed_units_advance_completion_counters() {
    for (factory, expected_error_substring) in [
        (
            SlowRecursiveClusteringFactory::finish_pass_failure(),
            "synthetic recursive planning failure",
        ),
        (
            SlowRecursiveClusteringFactory::short_assignment_batch(),
            "planner returned",
        ),
    ] {
        let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
        let observer: StreamingIndexingStatusObserver = {
            let statuses = Arc::clone(&statuses);
            Arc::new(move |status| statuses.lock().unwrap().push(status))
        };

        let items = switch_trigger_items();
        let mut run = StreamingIndexingRun::with_streaming_clustering_factory(
            MapResolver,
            AsciiEmbeddingProvider,
            ArithmeticMeanCanonicalEmbeddingPolicy,
            factory,
            embedding_spec(),
            128,
        )
        .with_observer(observer);
        let error = async {
            run.ingest_batch(&items).await.unwrap();
            run.finish_pass().unwrap_err()
        }
        .await;
        assert!(
            error.to_string().contains(expected_error_substring),
            "unexpected error: {error}"
        );

        let statuses = statuses.lock().unwrap().clone();
        let failed_unit = statuses
            .iter()
            .find(|status| {
                matches!(
                    status.phase,
                    StreamingIndexingPhase::HierarchyPlanning {
                        stage: PlanningStage::Custom
                    }
                ) && status.state == StreamingIndexingStatusState::Failed
                    && status.progress_unit_kind
                        == Some(StreamingIndexingProgressUnitKind::PartitionPlanningInvocation)
            })
            .expect("recursive failed unit status");
        assert_eq!(failed_unit.current_partition_path.as_deref(), Some("p0"));
        assert_eq!(failed_unit.completed_unit_count, 1);
        assert_eq!(failed_unit.completed_planner_invocation_count, Some(1));
        assert_eq!(failed_unit.discovered_unit_count, Some(1));
        assert_eq!(failed_unit.current_recursion_depth, Some(0));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn regression_published_profile_terminal_short_circuit_does_not_claim_fine_stage() {
    let statuses: Arc<Mutex<Vec<StreamingIndexingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: StreamingIndexingStatusObserver = {
        let statuses = Arc::clone(&statuses);
        Arc::new(move |status| statuses.lock().unwrap().push(status))
    };

    let mut run = StreamingIndexingRun::with_published_profile(
        MapResolver,
        AsciiEmbeddingProvider,
        PUBLISHED_PROFILE_V0_1_0,
        embedding_spec(),
        256,
    )
    .unwrap()
    .with_observer(observer);
    run.ingest_batch(&[item("alpha"), item("bravo")])
        .await
        .unwrap();
    run.finish_pass().unwrap();

    let hierarchy = run.finalized_partition_hierarchy().unwrap();
    assert_eq!(hierarchy.partitions.len(), 1);
    assert_eq!(
        hierarchy.partitions[0].planning_stage,
        PlanningStage::Single
    );

    let statuses = statuses.lock().unwrap().clone();
    assert!(!statuses.iter().any(|status| {
        matches!(
            status.phase,
            StreamingIndexingPhase::HierarchyPlanning { .. }
        )
    }));
}
