// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use futures::StreamExt;
use rayon::prelude::*;
use redb::{Database, ReadableTable, TableDefinition};
use tempfile::TempDir;

use crate::{
    Block, BlockHash, BranchEncodingPolicy, BranchEntry, ChildSummaryInput, ChildSummaryPolicy,
    EmbeddingSpec, ExactCentroidChildSummaryPolicy, HierarchyPlanningDetailFields, IndexedChild,
    LayerBuildStatus, PUBLISHED_PROFILE_V0_7_0, PlanningStage, PublishedBranchEncodingPolicy,
    PublishedDirectionalPcaProfileSettings, PublishedIndexingProfile, PublishedPlanningStrategy,
    PublishedProfileVersion, StreamingClusteringConfig, StreamingIndexerError,
    StreamingIndexingCancellationHandle, StreamingIndexingPhase, StreamingIndexingProgressUnitKind,
    StreamingIndexingResult, StreamingIndexingStatusObserver, StreamingIndexingStatusState,
    VERSION_1, balanced_groups, branch_encoding_policy_for_profile, build_branch_block,
    decode_embedding_as_f32, dedup_sort_ids, effective_directional_pca_cluster_count, emit_status,
    encode_branch_entries, fallback_partition_groups, map_clustering_configuration_error,
    map_clustering_error, materializability_bound, normalize_branch_entries,
    normalize_child_summary_inputs, normalize_current_layer, partition_depth,
    published_indexing_profile, serialize_block, start_status_heartbeat,
    status_with_hierarchy_details, status_with_known_total, validate_embedding_bytes,
    validate_published_profile_configuration, verify_persisted_block_id, with_legacy_item_count,
};
use lexongraph_block::{LeafBlock, ValidatedBlock};
use lexongraph_block_store::BlockStore;
use lexongraph_directional_pca::DirectionalPcaStreamingTrainer;
use lexongraph_streaming_clustering::{
    PassReadiness, StreamingClusterClassifier, StreamingClusterTrainer, StreamingClusteringError,
    TrainerState,
};

const V3_IO_QUEUE_DEPTH: usize = 32;
const V3_BATCH_SIZE: usize = 256;
const V3_PREPARED_BATCH_LOOKAHEAD: usize = 3;
const V3_MAX_REPLAY_PASSES: usize = 4096;
const V3_PARTITION_STORE_FILE_NAME: &str = "partitions.redb";
const V3_PARTITION_COUNTS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("v3_partition_counts");
const V3_BLOCK_HASH_PARTITIONS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("v3_block_hash_partitions");
const V3_INDEXED_CHILD_PARTITIONS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("v3_indexed_child_partitions");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkingItemKind {
    LeafBlockIds,
    IndexedChildren,
}

#[derive(Clone, Debug)]
struct WorkingPartition {
    id: String,
    layer_index: usize,
    item_count: usize,
    kind: WorkingItemKind,
}

#[derive(Clone, Debug)]
struct LoadedLeaf {
    id: BlockHash,
    block: Block,
    embedding: Vec<u8>,
}

#[derive(Clone, Debug)]
struct PreparedLeafAssignmentBatch {
    block_ids: Vec<BlockHash>,
    embeddings: Vec<Vec<f32>>,
}

#[derive(Clone)]
struct PreparedIndexedChildAssignmentBatch {
    children: Vec<IndexedChild>,
    embeddings: Vec<Vec<f32>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum V3Phase {
    Ingesting,
    Finalized,
    Cancelled,
}

pub struct StreamingIndexingRunV3 {
    observer: Option<StreamingIndexingStatusObserver>,
    cancellation: StreamingIndexingCancellationHandle,
    profile: PublishedIndexingProfile,
    branch_encoding_policy: BranchEncodingPolicy,
    embedding_spec: EmbeddingSpec,
    block_size_target: usize,
    temp_root: Option<TempDir>,
    partition_store: V3PartitionStore,
    root_partition_id: String,
    phase: V3Phase,
    ingested_count: usize,
}

impl StreamingIndexingRunV3 {
    pub fn with_published_profile(
        profile_version: PublishedProfileVersion,
        embedding_spec: EmbeddingSpec,
        block_size_target: usize,
        working_root: impl AsRef<Path>,
    ) -> Result<Self, StreamingIndexerError> {
        if profile_version != PUBLISHED_PROFILE_V0_7_0 {
            return Err(StreamingIndexerError::UnsupportedPublishedProfileVersion(
                profile_version,
            ));
        }
        let profile = published_indexing_profile(profile_version)?;
        validate_published_profile_configuration(&profile, &embedding_spec, block_size_target)?;
        let PublishedPlanningStrategy::DirectionalPcaDivisive(_) = &profile.planning_strategy
        else {
            return Err(StreamingIndexerError::ClusteringFailure(
                "streaming v3 currently requires a directional-PCA divisive published profile"
                    .into(),
            ));
        };
        if profile.branch_encoding_policy
            != (PublishedBranchEncodingPolicy::AmbientDeltaUniform {
                root_bits: 12,
                interior_bits: 8,
                lowest_routing_bits: 6,
            })
        {
            return Err(StreamingIndexerError::ClusteringFailure(
                "streaming v3 currently supports only the exact 0.7.0 ambient-delta-uq branch encoding contract".into(),
            ));
        }
        let temp_root = tempfile::Builder::new()
            .prefix("streaming-v3-")
            .tempdir_in(working_root.as_ref())
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not initialize v3 working root {}: {error}",
                    working_root.as_ref().display()
                ))
            })?;
        let partition_store = V3PartitionStore::new(temp_root.path())?;
        Ok(Self {
            observer: None,
            cancellation: StreamingIndexingCancellationHandle::new(),
            branch_encoding_policy: branch_encoding_policy_for_profile(&profile),
            profile,
            embedding_spec,
            block_size_target,
            temp_root: Some(temp_root),
            partition_store,
            root_partition_id: "l0.p0".into(),
            phase: V3Phase::Ingesting,
            ingested_count: 0,
        })
    }

    pub fn with_observer(mut self, observer: StreamingIndexingStatusObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    pub fn with_cancellation_handle(
        mut self,
        cancellation: StreamingIndexingCancellationHandle,
    ) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub async fn ingest_block_id_batch(
        &mut self,
        block_ids: &[BlockHash],
    ) -> Result<(), StreamingIndexerError> {
        if self.phase != V3Phase::Ingesting {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(
                "v3 block-id ingestion requires the ingesting phase".into(),
            ));
        }
        self.check_cancelled_mut("block-id ingestion")?;
        if block_ids.is_empty() {
            return Ok(());
        }
        self.partition_store
            .append_block_hashes(&self.root_partition_id, block_ids)?;
        self.check_cancelled_mut("block-id ingestion")?;
        self.ingested_count += block_ids.len();
        Ok(())
    }

    pub async fn finalize(
        &mut self,
        source_store: &dyn BlockStore,
        output_store: &dyn BlockStore,
    ) -> Result<StreamingIndexingResult, StreamingIndexerError> {
        if self.phase != V3Phase::Ingesting {
            return Err(StreamingIndexerError::InvalidLifecycleTransition(
                "v3 finalize requires the ingesting phase".into(),
            ));
        }
        self.check_cancelled_mut("finalize")?;
        if self.ingested_count == 0 {
            return Err(StreamingIndexerError::EmptyInput);
        }

        let mut persisted_ids = Vec::new();
        let mut layer_index = 0usize;
        let mut current_layer = vec![WorkingPartition {
            id: self.root_partition_id.clone(),
            layer_index,
            item_count: self.ingested_count,
            kind: WorkingItemKind::LeafBlockIds,
        }];

        loop {
            let next_layer_inputs = self
                .process_layer_until_terminal(
                    current_layer,
                    source_store,
                    output_store,
                    &mut persisted_ids,
                )
                .await
                .map_err(|error| self.record_terminal_error(error))?;
            if next_layer_inputs.is_empty() {
                return Err(StreamingIndexerError::EmptyInput);
            }
            if next_layer_inputs.len() == 1 {
                dedup_sort_ids(&mut persisted_ids);
                let root_id = next_layer_inputs[0].child;
                self.phase = V3Phase::Finalized;
                if let Some(temp_root) = self.temp_root.take() {
                    let cleanup_root = temp_root.path().display().to_string();
                    if let Err(error) = temp_root.close() {
                        eprintln!("could not remove v3 working root {}: {error}", cleanup_root);
                    }
                }
                return Ok(StreamingIndexingResult {
                    root_id,
                    block_ids: persisted_ids,
                });
            }

            layer_index += 1;
            let next_root_id = format!("l{layer_index}.p0");
            write_indexed_child_partition(
                &self.partition_store,
                &next_root_id,
                next_layer_inputs.as_slice(),
            )?;
            current_layer = vec![WorkingPartition {
                id: next_root_id,
                layer_index,
                item_count: next_layer_inputs.len(),
                kind: WorkingItemKind::IndexedChildren,
            }];
        }
    }

    async fn process_layer_until_terminal(
        &self,
        mut active: Vec<WorkingPartition>,
        source_store: &dyn BlockStore,
        output_store: &dyn BlockStore,
        persisted_ids: &mut Vec<BlockHash>,
    ) -> Result<Vec<IndexedChild>, StreamingIndexerError> {
        let materializability_bound =
            materializability_bound(&self.embedding_spec, self.block_size_target)
                .map_err(StreamingIndexerError::TerminalPartitionMaterialization)?;
        let mut terminals = Vec::new();
        while !active.is_empty() {
            self.check_cancelled("layer processing")?;
            let mut next = Vec::new();
            for partition in active {
                self.check_cancelled("layer processing")?;
                if partition.item_count <= materializability_bound || partition.item_count <= 1 {
                    terminals.push(
                        self.materialize_terminal_partition(
                            &partition,
                            source_store,
                            output_store,
                            persisted_ids,
                        )
                        .await?,
                    );
                } else {
                    next.extend(
                        self.split_partition(&partition, materializability_bound, source_store)
                            .await?,
                    );
                }
            }
            active = next;
        }
        Ok(normalize_current_layer(terminals))
    }

    async fn split_partition(
        &self,
        partition: &WorkingPartition,
        materializability_bound: usize,
        source_store: &dyn BlockStore,
    ) -> Result<Vec<WorkingPartition>, StreamingIndexerError> {
        let settings = self.profile_settings()?;
        let cluster_count = effective_directional_pca_cluster_count(
            settings.cluster_count,
            partition.item_count,
            materializability_bound,
            settings.params.allocation_policy,
        )
        .map_err(map_clustering_configuration_error)?;
        let mut trainer = DirectionalPcaStreamingTrainer::new(
            StreamingClusteringConfig {
                cluster_count,
                dimensions: self.dimensions()?,
                balance_constraints: None,
                random_seed: settings.random_seed,
            },
            settings.params.clone(),
        )
        .map_err(map_clustering_error)?;

        let planning_phase = StreamingIndexingPhase::HierarchyPlanning {
            stage: PlanningStage::Custom,
        };
        let planning_started = Instant::now();
        emit_status(
            &self.observer,
            status_with_hierarchy_details(
                planning_phase.clone(),
                StreamingIndexingStatusState::Started,
                Some(1),
                0,
                Duration::ZERO,
                None,
                HierarchyPlanningDetailFields {
                    legacy_item_count: Some(partition.item_count),
                    progress_unit_kind: Some(
                        StreamingIndexingProgressUnitKind::PartitionPlanningInvocation,
                    ),
                    discovered_unit_count: Some(1),
                    current_unit_elapsed: Some(Duration::ZERO),
                    current_partition_path: Some(partition.id.clone()),
                    current_partition_size: Some(partition.item_count),
                    current_recursion_depth: Some(partition_depth(&partition.id)),
                    started_subproblem_count: Some(1),
                    completed_subproblem_count: Some(0),
                    visited_partition_count: Some(1),
                    finalized_partition_count: Some(0),
                    terminal_partition_count: Some(0),
                    completed_planner_invocation_count: Some(0),
                    fallback_count: Some(0),
                    last_progress_at: Some(Duration::ZERO),
                },
            ),
        );
        emit_status(
            &self.observer,
            status_with_hierarchy_details(
                planning_phase.clone(),
                StreamingIndexingStatusState::InProgress,
                Some(1),
                0,
                planning_started.elapsed(),
                None,
                HierarchyPlanningDetailFields {
                    legacy_item_count: Some(partition.item_count),
                    progress_unit_kind: Some(
                        StreamingIndexingProgressUnitKind::PartitionPlanningInvocation,
                    ),
                    discovered_unit_count: Some(1),
                    current_unit_elapsed: Some(planning_started.elapsed()),
                    current_partition_path: Some(partition.id.clone()),
                    current_partition_size: Some(partition.item_count),
                    current_recursion_depth: Some(partition_depth(&partition.id)),
                    started_subproblem_count: Some(1),
                    completed_subproblem_count: Some(0),
                    visited_partition_count: Some(1),
                    finalized_partition_count: Some(0),
                    terminal_partition_count: Some(0),
                    completed_planner_invocation_count: Some(0),
                    fallback_count: Some(0),
                    last_progress_at: Some(planning_started.elapsed()),
                },
            ),
        );

        let result = (|| -> Result<(Vec<WorkingPartition>, bool), StreamingIndexerError> {
            let mut replay_passes = 0usize;
            let max_passes = v3_replay_pass_limit(partition.item_count);
            loop {
                self.check_cancelled("partition planning")?;
                replay_passes += 1;
                if replay_passes > max_passes {
                    return Err(StreamingIndexerError::ClusteringFailure(format!(
                        "v3 planner exceeded the maximum replay pass count of {max_passes}"
                    )));
                }
                match partition.kind {
                    WorkingItemKind::LeafBlockIds => {
                        self.ingest_leaf_training_partition_batches(
                            partition,
                            source_store,
                            &mut trainer,
                        )?;
                    }
                    WorkingItemKind::IndexedChildren => {
                        self.ingest_summary_training_partition_batches(partition, &mut trainer)?;
                    }
                }
                self.check_cancelled("partition planning")?;
                let pass_report = trainer.finish_pass().map_err(map_clustering_error)?;
                self.check_cancelled("partition planning")?;
                if pass_report.observed_count != partition.item_count {
                    return Err(StreamingIndexerError::HierarchyValidation(format!(
                        "v3 partition {:?} observed {} items but expected {}",
                        partition.id, pass_report.observed_count, partition.item_count
                    )));
                }
                if pass_report.readiness == PassReadiness::AnalysisOnly {
                    continue;
                }
                match trainer.complete_training() {
                    Ok(()) => break,
                    Err(StreamingClusteringError::InvalidTransition { state, operation })
                        if state == TrainerState::PassComplete
                            && operation == "complete_training" =>
                    {
                        continue;
                    }
                    Err(error) => return Err(map_clustering_error(error)),
                }
            }
            let classifier = trainer.into_classifier().map_err(map_clustering_error)?;

            let child_count =
                usize::try_from(classifier.realized_cluster_count()).map_err(|_| {
                    StreamingIndexerError::HierarchyValidation(
                        "v3 realized cluster count does not fit usize".into(),
                    )
                })?;
            let mut child_ids = (0..child_count)
                .map(|child_index| format!("{}.{}", partition.id, child_index))
                .collect::<Vec<_>>();
            let mut child_item_counts = vec![0usize; child_count];
            if child_count <= 1 {
                child_ids = (0..2)
                    .map(|child_index| format!("{}.{}", partition.id, child_index))
                    .collect();
                child_item_counts = vec![0usize; 2];
            }

            match partition.kind {
                WorkingItemKind::LeafBlockIds => {
                    let mut writers = BlockHashPartitionWriters::create(
                        &self.partition_store,
                        child_ids.as_slice(),
                    )?;
                    self.classify_leaf_partition_batches(
                        partition,
                        source_store,
                        &classifier,
                        &mut writers,
                        child_item_counts.as_mut_slice(),
                    )?;
                    writers.finish()?;
                }
                WorkingItemKind::IndexedChildren => {
                    let mut writers = IndexedChildPartitionWriters::create(
                        &self.partition_store,
                        child_ids.as_slice(),
                    )?;
                    self.classify_summary_partition_batches(
                        partition,
                        &classifier,
                        &mut writers,
                        child_item_counts.as_mut_slice(),
                    )?;
                    writers.finish()?;
                }
            }

            let mut non_empty = child_item_counts
                .iter()
                .enumerate()
                .filter_map(|(index, count)| (*count > 0).then_some(index))
                .collect::<Vec<_>>();
            let used_fallback = non_empty.len() <= 1;
            if used_fallback {
                self.partition_store
                    .clear_partitions(partition.kind, child_ids.as_slice())?;
                let fallback_groups =
                    fallback_partition_groups(partition.item_count, materializability_bound, None)
                        .map_err(|error| {
                            StreamingIndexerError::HierarchyValidation(error.to_string())
                        })?;
                let fallback_assignment =
                    fallback_assignment_map(partition.item_count, fallback_groups.as_slice())?;
                child_ids = (0..fallback_groups.len())
                    .map(|child_index| format!("{}.{}", partition.id, child_index))
                    .collect::<Vec<_>>();
                child_item_counts = fallback_groups.iter().map(Vec::len).collect::<Vec<_>>();
                match partition.kind {
                    WorkingItemKind::LeafBlockIds => {
                        rewrite_block_hash_partition_with_assignments(
                            &self.partition_store,
                            &partition.id,
                            child_ids.as_slice(),
                            fallback_assignment.as_slice(),
                        )?;
                    }
                    WorkingItemKind::IndexedChildren => {
                        rewrite_indexed_child_partition_with_assignments(
                            &self.partition_store,
                            &partition.id,
                            child_ids.as_slice(),
                            fallback_assignment.as_slice(),
                        )?;
                    }
                }
                non_empty = child_item_counts
                    .iter()
                    .enumerate()
                    .filter_map(|(index, count)| (*count > 0).then_some(index))
                    .collect::<Vec<_>>();
            }

            Ok((
                non_empty
                    .into_iter()
                    .map(|index| WorkingPartition {
                        id: child_ids[index].clone(),
                        layer_index: partition.layer_index,
                        item_count: child_item_counts[index],
                        kind: partition.kind,
                    })
                    .collect(),
                used_fallback,
            ))
        })();

        match result {
            Ok((children, used_fallback)) => {
                emit_status(
                    &self.observer,
                    status_with_hierarchy_details(
                        planning_phase,
                        StreamingIndexingStatusState::Completed,
                        Some(1),
                        1,
                        planning_started.elapsed(),
                        None,
                        HierarchyPlanningDetailFields {
                            legacy_item_count: Some(partition.item_count),
                            progress_unit_kind: Some(
                                StreamingIndexingProgressUnitKind::PartitionPlanningInvocation,
                            ),
                            discovered_unit_count: Some(1),
                            current_unit_elapsed: Some(planning_started.elapsed()),
                            current_partition_path: Some(partition.id.clone()),
                            current_partition_size: Some(partition.item_count),
                            current_recursion_depth: Some(partition_depth(&partition.id)),
                            started_subproblem_count: Some(1),
                            completed_subproblem_count: Some(1),
                            visited_partition_count: Some(1),
                            finalized_partition_count: Some(1),
                            terminal_partition_count: Some(0),
                            completed_planner_invocation_count: Some(1),
                            fallback_count: Some(used_fallback as usize),
                            last_progress_at: Some(planning_started.elapsed()),
                        },
                    ),
                );
                Ok(children)
            }
            Err(error) => {
                emit_status(
                    &self.observer,
                    status_with_hierarchy_details(
                        planning_phase,
                        StreamingIndexingStatusState::Failed,
                        Some(1),
                        1,
                        planning_started.elapsed(),
                        Some(error.to_string()),
                        HierarchyPlanningDetailFields {
                            legacy_item_count: Some(partition.item_count),
                            progress_unit_kind: Some(
                                StreamingIndexingProgressUnitKind::PartitionPlanningInvocation,
                            ),
                            discovered_unit_count: Some(1),
                            current_unit_elapsed: Some(planning_started.elapsed()),
                            current_partition_path: Some(partition.id.clone()),
                            current_partition_size: Some(partition.item_count),
                            current_recursion_depth: Some(partition_depth(&partition.id)),
                            started_subproblem_count: Some(1),
                            completed_subproblem_count: Some(1),
                            visited_partition_count: Some(1),
                            finalized_partition_count: Some(0),
                            terminal_partition_count: Some(0),
                            completed_planner_invocation_count: Some(1),
                            fallback_count: Some(0),
                            last_progress_at: Some(planning_started.elapsed()),
                        },
                    ),
                );
                Err(error)
            }
        }
    }

    async fn materialize_terminal_partition(
        &self,
        partition: &WorkingPartition,
        source_store: &dyn BlockStore,
        output_store: &dyn BlockStore,
        persisted_ids: &mut Vec<BlockHash>,
    ) -> Result<IndexedChild, StreamingIndexerError> {
        match partition.kind {
            WorkingItemKind::LeafBlockIds => {
                let block_ids = read_all_block_hashes(&self.partition_store, &partition.id)?;
                let loaded = self
                    .load_leaf_batch(
                        block_ids.as_slice(),
                        StreamingIndexingPhase::V3TerminalMaterializationLoad {
                            layer_index: partition.layer_index,
                        },
                        source_store,
                    )
                    .await?;
                let mut children = Vec::with_capacity(loaded.len());
                for leaf in loaded {
                    self.check_cancelled("terminal materialization load")?;
                    let output_id = output_store
                        .put(&leaf.block)
                        .await
                        .map_err(StreamingIndexerError::Storage)?;
                    verify_persisted_block_id(output_id, leaf.id)?;
                    persisted_ids.push(output_id);
                    children.push(IndexedChild {
                        embedding: leaf.embedding,
                        child: output_id,
                        level: 0,
                        descendant_count: 1,
                    });
                }
                if children.len() == 1 {
                    return Ok(children.remove(0));
                }
                self.assemble_child_set(
                    children,
                    partition.id == format!("l{}.p0", partition.layer_index),
                    output_store,
                    persisted_ids,
                )
                .await
            }
            WorkingItemKind::IndexedChildren => {
                let phase = StreamingIndexingPhase::V3TerminalMaterializationLoad {
                    layer_index: partition.layer_index,
                };
                let mut children =
                    self.run_v3_partition_phase(phase.clone(), partition.item_count, |progress| {
                        let mut reader =
                            IndexedChildPartitionReader::open(&self.partition_store, &partition.id);
                        let mut children = Vec::with_capacity(partition.item_count);
                        while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
                            self.check_cancelled_for_phase(&phase)?;
                            progress.fetch_add(batch.len(), AtomicOrdering::Relaxed);
                            children.extend(batch);
                        }
                        Ok(children)
                    })?;
                if children.len() == 1 {
                    return Ok(children.remove(0));
                }
                self.assemble_child_set(
                    children,
                    partition.id == format!("l{}.p0", partition.layer_index),
                    output_store,
                    persisted_ids,
                )
                .await
            }
        }
    }

    async fn assemble_child_set(
        &self,
        children: Vec<IndexedChild>,
        is_global_root_partition: bool,
        store: &dyn BlockStore,
        persisted_ids: &mut Vec<BlockHash>,
    ) -> Result<IndexedChild, StreamingIndexerError> {
        let materializability_bound =
            materializability_bound(&self.embedding_spec, self.block_size_target)
                .map_err(StreamingIndexerError::TerminalPartitionMaterialization)?;
        let mut current = normalize_current_layer(children);
        if current.is_empty() {
            return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                "child set normalized to zero entries".into(),
            ));
        }
        if current.len() == 1 {
            return Ok(current.remove(0));
        }
        loop {
            self.check_cancelled("bottom-up assembly")?;
            if current.len() == 1 {
                return Ok(current.remove(0));
            }
            let groups = balanced_groups(current.len(), materializability_bound)
                .map_err(StreamingIndexerError::TerminalPartitionMaterialization)?;
            let layer_index =
                usize::try_from(current.iter().map(|child| child.level).max().unwrap_or(0))
                    .map_err(|_| {
                        StreamingIndexerError::TerminalPartitionMaterialization(
                            "semantic bottom-up layer index does not fit usize".into(),
                        )
                    })?;
            let phase = StreamingIndexingPhase::BottomUpAssembly { layer_index };
            let started = Instant::now();
            let legacy_item_count = current.len();
            let phase_total = groups.len();
            let phase_progress = Arc::new(AtomicUsize::new(0));
            emit_status(
                &self.observer,
                with_legacy_item_count(
                    status_with_known_total(
                        phase.clone(),
                        StreamingIndexingStatusState::Started,
                        phase_total,
                        0,
                        Duration::ZERO,
                        None,
                    ),
                    legacy_item_count,
                ),
            );
            emit_status(
                &self.observer,
                with_legacy_item_count(
                    status_with_known_total(
                        phase.clone(),
                        StreamingIndexingStatusState::InProgress,
                        phase_total,
                        0,
                        started.elapsed(),
                        None,
                    ),
                    legacy_item_count,
                ),
            );
            let mut heartbeat = crate::StatusHeartbeatGuard::new(start_status_heartbeat(
                &self.observer,
                phase.clone(),
                Some(phase_total),
                Arc::clone(&phase_progress),
                Some(legacy_item_count),
                started,
            ));
            let next_level = current.iter().map(|child| child.level).max().unwrap_or(0) + 1;
            let next_layer = match self
                .build_branch_layer(
                    current.as_slice(),
                    groups.as_slice(),
                    next_level,
                    LayerBuildStatus {
                        phase: phase.clone(),
                        started,
                        progress: &phase_progress,
                        legacy_item_count,
                        is_global_root_partition,
                    },
                    store,
                    persisted_ids,
                )
                .await
            {
                Ok(next_layer) => next_layer,
                Err(error) => {
                    heartbeat.stop();
                    emit_status(
                        &self.observer,
                        with_legacy_item_count(
                            status_with_known_total(
                                phase,
                                StreamingIndexingStatusState::Failed,
                                phase_total,
                                phase_progress.load(AtomicOrdering::Relaxed),
                                started.elapsed(),
                                Some(error.to_string()),
                            ),
                            legacy_item_count,
                        ),
                    );
                    return Err(error);
                }
            };
            current = normalize_current_layer(next_layer);
            heartbeat.stop();
            emit_status(
                &self.observer,
                with_legacy_item_count(
                    status_with_known_total(
                        phase,
                        StreamingIndexingStatusState::Completed,
                        phase_total,
                        phase_total,
                        started.elapsed(),
                        None,
                    ),
                    legacy_item_count,
                ),
            );
        }
    }

    async fn build_branch_layer(
        &self,
        children: &[IndexedChild],
        groups: &[Vec<usize>],
        parent_level: u64,
        status: LayerBuildStatus<'_>,
        store: &dyn BlockStore,
        persisted_ids: &mut Vec<BlockHash>,
    ) -> Result<Vec<IndexedChild>, StreamingIndexerError> {
        let mut next_layer = Vec::with_capacity(groups.len());
        for group in groups {
            self.check_cancelled("bottom-up assembly")?;
            let raw_entries = group
                .iter()
                .map(|&index| BranchEntry {
                    embedding: children[index].embedding.clone(),
                    child: children[index].child,
                })
                .collect::<Vec<_>>();
            let raw_child_summaries = group
                .iter()
                .map(|&index| ChildSummaryInput {
                    embedding: children[index].embedding.clone(),
                    child: children[index].child,
                    level: children[index].level,
                    descendant_count: children[index].descendant_count,
                })
                .collect::<Vec<_>>();
            let entries = normalize_branch_entries(raw_entries);
            let child_summaries = normalize_child_summary_inputs(raw_child_summaries);
            if entries.len() < 2 || child_summaries.len() < 2 {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    "normalized child-bearing entry set has fewer than two unique children".into(),
                ));
            }
            let encoded_branch = encode_branch_entries(
                self.branch_encoding_policy,
                &self.embedding_spec,
                entries.as_slice(),
                parent_level,
                uses_root_branch_budget(status.is_global_root_partition, groups.len()),
            )?;
            let branch = build_branch_block(
                VERSION_1,
                parent_level,
                encoded_branch.embedding_spec,
                encoded_branch.entries,
                encoded_branch.ext,
            )
            .map_err(StreamingIndexerError::BlockConstruction)?;
            let branch_block = Block::Branch(branch.clone());
            let serialized =
                serialize_block(&branch_block).map_err(StreamingIndexerError::BlockConstruction)?;
            if serialized.bytes.len() > self.block_size_target {
                return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                    format!(
                        "branch block serialized to {} bytes, exceeding block size target {}",
                        serialized.bytes.len(),
                        self.block_size_target
                    ),
                ));
            }
            let block_id = store
                .put(&branch_block)
                .await
                .map_err(StreamingIndexerError::Storage)?;
            verify_persisted_block_id(block_id, serialized.hash)?;
            persisted_ids.push(block_id);
            let canonical = ExactCentroidChildSummaryPolicy
                .summarize_children(&self.embedding_spec, &child_summaries)
                .map_err(|error| {
                    StreamingIndexerError::CanonicalEmbeddingFailure(error.to_string())
                })?;
            validate_embedding_bytes(&canonical, &self.embedding_spec, "canonical")
                .map_err(StreamingIndexerError::CanonicalEmbeddingFailure)?;
            next_layer.push(IndexedChild {
                embedding: canonical,
                child: block_id,
                level: parent_level,
                descendant_count: child_summaries
                    .iter()
                    .map(|child| child.descendant_count)
                    .sum(),
            });
            status.progress.fetch_add(1, AtomicOrdering::Relaxed);
        }
        Ok(next_layer)
    }

    async fn load_leaf_batch(
        &self,
        block_ids: &[BlockHash],
        phase: StreamingIndexingPhase,
        source_store: &dyn BlockStore,
    ) -> Result<Vec<LoadedLeaf>, StreamingIndexerError> {
        let started = Instant::now();
        emit_status(
            &self.observer,
            status_with_known_total(
                phase.clone(),
                StreamingIndexingStatusState::Started,
                block_ids.len(),
                0,
                Duration::ZERO,
                None,
            ),
        );
        let progress = Arc::new(AtomicUsize::new(0));
        emit_status(
            &self.observer,
            status_with_known_total(
                phase.clone(),
                StreamingIndexingStatusState::InProgress,
                block_ids.len(),
                0,
                started.elapsed(),
                None,
            ),
        );
        let mut heartbeat = crate::StatusHeartbeatGuard::new(start_status_heartbeat(
            &self.observer,
            phase.clone(),
            Some(block_ids.len()),
            Arc::clone(&progress),
            Some(block_ids.len()),
            started,
        ));
        let result = async {
            let mut loaded = Vec::with_capacity(block_ids.len());
            for batch in block_ids.chunks(V3_BATCH_SIZE) {
                self.check_cancelled_for_phase(&phase)?;
                let ordered = load_leaf_blocks_raw(batch, source_store).await?;
                let decoded = ordered
                    .into_par_iter()
                    .map(|(block_id, block)| {
                        decode_loaded_leaf(block_id, block, &self.embedding_spec)
                    })
                    .collect::<Vec<_>>();
                for leaf in decoded {
                    loaded.push(leaf?);
                    progress.fetch_add(1, AtomicOrdering::Relaxed);
                }
            }
            Ok::<Vec<LoadedLeaf>, StreamingIndexerError>(loaded)
        }
        .await;
        heartbeat.stop();
        match result {
            Ok(loaded) => {
                emit_status(
                    &self.observer,
                    status_with_known_total(
                        phase,
                        StreamingIndexingStatusState::Completed,
                        block_ids.len(),
                        block_ids.len(),
                        started.elapsed(),
                        None,
                    ),
                );
                Ok(loaded)
            }
            Err(error) => {
                emit_status(
                    &self.observer,
                    status_with_known_total(
                        phase,
                        StreamingIndexingStatusState::Failed,
                        block_ids.len(),
                        progress.load(AtomicOrdering::Relaxed),
                        started.elapsed(),
                        Some(error.to_string()),
                    ),
                );
                Err(error)
            }
        }
    }

    fn ingest_leaf_training_partition_batches(
        &self,
        partition: &WorkingPartition,
        source_store: &dyn BlockStore,
        trainer: &mut DirectionalPcaStreamingTrainer,
    ) -> Result<(), StreamingIndexerError> {
        self.run_v3_partition_phase(
            StreamingIndexingPhase::V3PartitionTrainIngest {
                layer_index: partition.layer_index,
            },
            partition.item_count,
            |progress| {
                let mut reader =
                    BlockHashPartitionReader::open(&self.partition_store, &partition.id);
                let embedding_spec = self.embedding_spec.clone();
                run_prepared_batch_pipeline(
                    V3_PREPARED_BATCH_LOOKAHEAD,
                    move |sender| {
                        let runtime = build_v3_prepare_runtime()?;
                        while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
                            let prepared = runtime.block_on(prepare_leaf_training_batch(
                                batch,
                                source_store,
                                &embedding_spec,
                                None,
                            ))?;
                            if sender.send(Ok(prepared)).is_err() {
                                return Ok(());
                            }
                        }
                        Ok(())
                    },
                    |prepared| {
                        let batch_len = prepared.len();
                        trainer
                            .ingest_batch(prepared.as_slice())
                            .map_err(map_clustering_error)?;
                        progress.fetch_add(batch_len, AtomicOrdering::Relaxed);
                        Ok(())
                    },
                )
            },
        )
    }

    fn ingest_summary_training_partition_batches(
        &self,
        partition: &WorkingPartition,
        trainer: &mut DirectionalPcaStreamingTrainer,
    ) -> Result<(), StreamingIndexerError> {
        self.run_v3_partition_phase(
            StreamingIndexingPhase::V3PartitionTrainIngest {
                layer_index: partition.layer_index,
            },
            partition.item_count,
            |progress| {
                let mut reader =
                    IndexedChildPartitionReader::open(&self.partition_store, &partition.id);
                let embedding_spec = self.embedding_spec.clone();
                run_prepared_batch_pipeline(
                    V3_PREPARED_BATCH_LOOKAHEAD,
                    move |sender| {
                        while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
                            let prepared =
                                prepare_summary_training_batch(batch, &embedding_spec, None)?;
                            if sender.send(Ok(prepared)).is_err() {
                                return Ok(());
                            }
                        }
                        Ok(())
                    },
                    |prepared| {
                        let batch_len = prepared.len();
                        trainer
                            .ingest_batch(prepared.as_slice())
                            .map_err(map_clustering_error)?;
                        progress.fetch_add(batch_len, AtomicOrdering::Relaxed);
                        Ok(())
                    },
                )
            },
        )
    }

    fn classify_leaf_partition_batches(
        &self,
        partition: &WorkingPartition,
        source_store: &dyn BlockStore,
        classifier: &impl StreamingClusterClassifier,
        writers: &mut BlockHashPartitionWriters,
        child_item_counts: &mut [usize],
    ) -> Result<(), StreamingIndexerError> {
        self.run_v3_partition_phase(
            StreamingIndexingPhase::V3PartitionClassify {
                layer_index: partition.layer_index,
            },
            partition.item_count,
            |progress| {
                let mut reader =
                    BlockHashPartitionReader::open(&self.partition_store, &partition.id);
                let embedding_spec = self.embedding_spec.clone();
                run_prepared_batch_pipeline(
                    V3_PREPARED_BATCH_LOOKAHEAD,
                    move |sender| {
                        let runtime = build_v3_prepare_runtime()?;
                        while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
                            let prepared = runtime.block_on(prepare_leaf_assignment_batch(
                                batch,
                                source_store,
                                &embedding_spec,
                                None,
                            ))?;
                            if sender.send(Ok(prepared)).is_err() {
                                return Ok(());
                            }
                        }
                        Ok(())
                    },
                    |prepared| {
                        let assignments = classifier
                            .assign_batch(prepared.embeddings.as_slice())
                            .map_err(map_clustering_error)?;
                        let batch_len = prepared.block_ids.len();
                        let mut grouped = vec![Vec::new(); writers.len()];
                        for (block_id, assignment) in prepared.block_ids.iter().zip(assignments) {
                            let cluster = usize::try_from(assignment).map_err(|_| {
                                StreamingIndexerError::HierarchyValidation(
                                    "v3 cluster id does not fit usize".into(),
                                )
                            })?;
                            let target = validate_v3_cluster_assignment(cluster, writers.len())?;
                            grouped[target].push(*block_id);
                            child_item_counts[target] += 1;
                        }
                        for (target, block_ids) in grouped.iter().enumerate() {
                            writers.write_batch(target, block_ids.as_slice())?;
                        }
                        progress.fetch_add(batch_len, AtomicOrdering::Relaxed);
                        Ok(())
                    },
                )
            },
        )
    }

    fn classify_summary_partition_batches(
        &self,
        partition: &WorkingPartition,
        classifier: &impl StreamingClusterClassifier,
        writers: &mut IndexedChildPartitionWriters,
        child_item_counts: &mut [usize],
    ) -> Result<(), StreamingIndexerError> {
        self.run_v3_partition_phase(
            StreamingIndexingPhase::V3PartitionClassify {
                layer_index: partition.layer_index,
            },
            partition.item_count,
            |progress| {
                let mut reader =
                    IndexedChildPartitionReader::open(&self.partition_store, &partition.id);
                let embedding_spec = self.embedding_spec.clone();
                run_prepared_batch_pipeline(
                    V3_PREPARED_BATCH_LOOKAHEAD,
                    move |sender| {
                        while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
                            let prepared =
                                prepare_summary_assignment_batch(batch, &embedding_spec, None)?;
                            if sender.send(Ok(prepared)).is_err() {
                                return Ok(());
                            }
                        }
                        Ok(())
                    },
                    |prepared| {
                        let assignments = classifier
                            .assign_batch(prepared.embeddings.as_slice())
                            .map_err(map_clustering_error)?;
                        let batch_len = prepared.children.len();
                        let mut grouped = vec![Vec::new(); writers.len()];
                        for (child, assignment) in prepared.children.iter().zip(assignments) {
                            let cluster = usize::try_from(assignment).map_err(|_| {
                                StreamingIndexerError::HierarchyValidation(
                                    "v3 cluster id does not fit usize".into(),
                                )
                            })?;
                            let target = validate_v3_cluster_assignment(cluster, writers.len())?;
                            grouped[target].push(child.clone());
                            child_item_counts[target] += 1;
                        }
                        for (target, children) in grouped.iter().enumerate() {
                            writers.write_batch(target, children.as_slice())?;
                        }
                        progress.fetch_add(batch_len, AtomicOrdering::Relaxed);
                        Ok(())
                    },
                )
            },
        )
    }

    fn run_v3_partition_phase<T>(
        &self,
        phase: StreamingIndexingPhase,
        total_items: usize,
        operation: impl FnOnce(Arc<AtomicUsize>) -> Result<T, StreamingIndexerError>,
    ) -> Result<T, StreamingIndexerError> {
        let started = Instant::now();
        emit_status(
            &self.observer,
            status_with_known_total(
                phase.clone(),
                StreamingIndexingStatusState::Started,
                total_items,
                0,
                Duration::ZERO,
                None,
            ),
        );
        let progress = Arc::new(AtomicUsize::new(0));
        emit_status(
            &self.observer,
            status_with_known_total(
                phase.clone(),
                StreamingIndexingStatusState::InProgress,
                total_items,
                0,
                started.elapsed(),
                None,
            ),
        );
        let mut heartbeat = crate::StatusHeartbeatGuard::new(start_status_heartbeat(
            &self.observer,
            phase.clone(),
            Some(total_items),
            Arc::clone(&progress),
            Some(total_items),
            started,
        ));
        let result = self
            .check_cancelled_for_phase(&phase)
            .and_then(|()| operation(Arc::clone(&progress)));
        heartbeat.stop();
        match result {
            Ok(value) => {
                let completed = progress.load(AtomicOrdering::Relaxed);
                emit_status(
                    &self.observer,
                    status_with_known_total(
                        phase,
                        StreamingIndexingStatusState::Completed,
                        total_items,
                        completed,
                        started.elapsed(),
                        None,
                    ),
                );
                Ok(value)
            }
            Err(error) => {
                emit_status(
                    &self.observer,
                    status_with_known_total(
                        phase,
                        StreamingIndexingStatusState::Failed,
                        total_items,
                        progress.load(AtomicOrdering::Relaxed),
                        started.elapsed(),
                        Some(error.to_string()),
                    ),
                );
                Err(error)
            }
        }
    }

    fn dimensions(&self) -> Result<usize, StreamingIndexerError> {
        usize::try_from(self.embedding_spec.dims).map_err(|_| {
            StreamingIndexerError::ClusteringFailure(format!(
                "embedding dims {} do not fit into usize",
                self.embedding_spec.dims
            ))
        })
    }

    fn profile_settings(
        &self,
    ) -> Result<&PublishedDirectionalPcaProfileSettings, StreamingIndexerError> {
        match &self.profile.planning_strategy {
            PublishedPlanningStrategy::DirectionalPcaDivisive(settings) => Ok(settings),
            _ => Err(StreamingIndexerError::ClusteringFailure(
                "streaming v3 currently requires directional-PCA divisive planning".into(),
            )),
        }
    }
    fn check_cancelled(&self, context: &str) -> Result<(), StreamingIndexerError> {
        if self.cancellation.is_cancelled() {
            return Err(StreamingIndexerError::Cancelled(format!(
                "caller requested cancellation during {context}"
            )));
        }
        Ok(())
    }

    fn check_cancelled_for_phase(
        &self,
        phase: &StreamingIndexingPhase,
    ) -> Result<(), StreamingIndexerError> {
        self.check_cancelled(v3_phase_description(phase))
    }
}

fn decode_loaded_leaf(
    block_id: BlockHash,
    block: ValidatedBlock,
    embedding_spec: &EmbeddingSpec,
) -> Result<LoadedLeaf, StreamingIndexerError> {
    let block = block.block;
    let Block::Leaf(ref leaf) = block else {
        return Err(StreamingIndexerError::HierarchyValidation(format!(
            "v3 input block {} is not a leaf block",
            block_id
        )));
    };
    validate_v3_leaf(block_id, leaf, embedding_spec)?;
    let entry = leaf
        .entries
        .first()
        .expect("validated leaf must contain an entry");
    let embedding = entry.embedding.clone();
    Ok(LoadedLeaf {
        id: block_id,
        block,
        embedding,
    })
}

fn decode_leaf_embedding_f32(
    block_id: BlockHash,
    block: ValidatedBlock,
    embedding_spec: &EmbeddingSpec,
) -> Result<Vec<f32>, StreamingIndexerError> {
    let block = block.block;
    let Block::Leaf(ref leaf) = block else {
        return Err(StreamingIndexerError::HierarchyValidation(format!(
            "v3 input block {} is not a leaf block",
            block_id
        )));
    };
    validate_v3_leaf(block_id, leaf, embedding_spec)?;
    let entry = leaf
        .entries
        .first()
        .expect("validated leaf must contain an entry");
    decode_embedding_as_f32(entry.embedding.as_slice(), embedding_spec)
}

fn v3_replay_pass_limit(item_count: usize) -> usize {
    item_count.saturating_add(4).clamp(1, V3_MAX_REPLAY_PASSES)
}

fn validate_v3_cluster_assignment(
    cluster: usize,
    writer_count: usize,
) -> Result<usize, StreamingIndexerError> {
    if cluster >= writer_count {
        return Err(StreamingIndexerError::HierarchyValidation(format!(
            "v3 cluster id {cluster} exceeds available child partitions {writer_count}"
        )));
    }
    Ok(cluster)
}

fn build_v3_prepare_runtime() -> Result<tokio::runtime::Runtime, StreamingIndexerError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            StreamingIndexerError::ClusteringFailure(format!(
                "could not initialize v3 prepare runtime: {error}"
            ))
        })
}

fn run_prepared_batch_pipeline<Prepared>(
    lookahead: usize,
    produce: impl FnOnce(
        mpsc::SyncSender<Result<Prepared, StreamingIndexerError>>,
    ) -> Result<(), StreamingIndexerError>
    + Send,
    mut consume: impl FnMut(Prepared) -> Result<(), StreamingIndexerError>,
) -> Result<(), StreamingIndexerError>
where
    Prepared: Send,
{
    thread::scope(|scope| {
        let (sender, receiver) = mpsc::sync_channel(lookahead);
        let producer = scope.spawn(move || produce(sender));
        let consumer_result = loop {
            match receiver.recv() {
                Ok(Ok(prepared)) => {
                    if let Err(error) = consume(prepared) {
                        break Err(error);
                    }
                }
                Ok(Err(error)) => break Err(error),
                Err(_) => break Ok(()),
            }
        };
        drop(receiver);
        let producer_result = producer.join().map_err(|panic| {
            StreamingIndexerError::ClusteringFailure(format!(
                "v3 prepared-batch producer thread panicked: {panic:?}"
            ))
        })?;
        consumer_result?;
        producer_result
    })
}

async fn load_leaf_blocks_raw(
    block_ids: &[BlockHash],
    source_store: &dyn BlockStore,
) -> Result<Vec<(BlockHash, ValidatedBlock)>, StreamingIndexerError> {
    let blocks = futures::stream::iter(block_ids.iter().copied())
        .map(|block_id| async move {
            let block = source_store
                .get(&block_id)
                .await
                .map_err(StreamingIndexerError::Storage)?
                .ok_or_else(|| {
                    StreamingIndexerError::Storage(
                        lexongraph_block_store::BlockStoreError::BackendFailure(format!(
                            "v3 input block {} is missing",
                            block_id
                        )),
                    )
                })?;
            Ok::<(BlockHash, ValidatedBlock), StreamingIndexerError>((block_id, block))
        })
        .buffered(V3_IO_QUEUE_DEPTH)
        .collect::<Vec<_>>()
        .await;
    let mut ordered = Vec::with_capacity(blocks.len());
    for result in blocks {
        ordered.push(result?);
    }
    Ok(ordered)
}

async fn load_leaf_batch_raw(
    block_ids: &[BlockHash],
    source_store: &dyn BlockStore,
    embedding_spec: &EmbeddingSpec,
    progress: Option<&Arc<AtomicUsize>>,
) -> Result<Vec<Vec<f32>>, StreamingIndexerError> {
    let ordered = load_leaf_blocks_raw(block_ids, source_store).await?;
    let decoded = ordered
        .into_par_iter()
        .map(|(block_id, block)| decode_leaf_embedding_f32(block_id, block, embedding_spec))
        .collect::<Vec<_>>();
    let mut loaded = Vec::with_capacity(decoded.len());
    for embedding in decoded {
        loaded.push(embedding?);
        if let Some(progress) = progress {
            progress.fetch_add(1, AtomicOrdering::Relaxed);
        }
    }
    Ok(loaded)
}

async fn prepare_leaf_training_batch(
    block_ids: Vec<BlockHash>,
    source_store: &dyn BlockStore,
    embedding_spec: &EmbeddingSpec,
    progress: Option<&Arc<AtomicUsize>>,
) -> Result<Vec<Vec<f32>>, StreamingIndexerError> {
    load_leaf_batch_raw(block_ids.as_slice(), source_store, embedding_spec, progress).await
}

async fn prepare_leaf_assignment_batch(
    block_ids: Vec<BlockHash>,
    source_store: &dyn BlockStore,
    embedding_spec: &EmbeddingSpec,
    progress: Option<&Arc<AtomicUsize>>,
) -> Result<PreparedLeafAssignmentBatch, StreamingIndexerError> {
    let embeddings =
        load_leaf_batch_raw(block_ids.as_slice(), source_store, embedding_spec, progress).await?;
    Ok(PreparedLeafAssignmentBatch {
        block_ids,
        embeddings,
    })
}

fn prepare_summary_training_batch(
    batch: Vec<IndexedChild>,
    embedding_spec: &EmbeddingSpec,
    progress: Option<&Arc<AtomicUsize>>,
) -> Result<Vec<Vec<f32>>, StreamingIndexerError> {
    decode_summary_embeddings(batch.as_slice(), embedding_spec, progress)
}

fn prepare_summary_assignment_batch(
    children: Vec<IndexedChild>,
    embedding_spec: &EmbeddingSpec,
    progress: Option<&Arc<AtomicUsize>>,
) -> Result<PreparedIndexedChildAssignmentBatch, StreamingIndexerError> {
    let embeddings = decode_summary_embeddings(children.as_slice(), embedding_spec, progress)?;
    Ok(PreparedIndexedChildAssignmentBatch {
        children,
        embeddings,
    })
}

fn decode_summary_embeddings(
    children: &[IndexedChild],
    embedding_spec: &EmbeddingSpec,
    progress: Option<&Arc<AtomicUsize>>,
) -> Result<Vec<Vec<f32>>, StreamingIndexerError> {
    let embeddings = children
        .par_iter()
        .map(|child| decode_embedding_as_f32(child.embedding.as_slice(), embedding_spec))
        .collect::<Vec<_>>();
    let mut decoded = Vec::with_capacity(embeddings.len());
    for embedding in embeddings {
        decoded.push(embedding?);
        if let Some(progress) = progress {
            progress.fetch_add(1, AtomicOrdering::Relaxed);
        }
    }
    Ok(decoded)
}

fn validate_v3_leaf(
    block_id: BlockHash,
    leaf: &LeafBlock,
    embedding_spec: &EmbeddingSpec,
) -> Result<(), StreamingIndexerError> {
    if &leaf.embedding_spec != embedding_spec {
        return Err(StreamingIndexerError::HierarchyValidation(format!(
            "v3 input leaf {} uses embedding_spec {:?} but run requires {:?}",
            block_id, leaf.embedding_spec, embedding_spec
        )));
    }
    if leaf.entries.len() != 1 {
        return Err(StreamingIndexerError::HierarchyValidation(format!(
            "v3 input leaf {} has {} entries; exactly one is required",
            block_id,
            leaf.entries.len()
        )));
    }
    validate_embedding_bytes(
        leaf.entries[0].embedding.as_slice(),
        embedding_spec,
        "v3 input leaf",
    )
    .map_err(StreamingIndexerError::HierarchyValidation)?;
    Ok(())
}

fn uses_root_branch_budget(is_global_root_partition: bool, group_count: usize) -> bool {
    is_global_root_partition && group_count > 1
}

struct V3PartitionStore {
    database: Database,
    database_path: PathBuf,
}

impl V3PartitionStore {
    fn new(store_root: &Path) -> Result<Self, StreamingIndexerError> {
        let database_path = store_root.join(V3_PARTITION_STORE_FILE_NAME);
        let database = Database::create(&database_path).map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not initialize v3 partition database {}: {error}",
                database_path.display()
            ))
        })?;
        let write_txn = database.begin_write().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start v3 partition database initialization for {}: {error}",
                database_path.display()
            ))
        })?;
        {
            write_txn
                .open_table(V3_PARTITION_COUNTS_TABLE)
                .map_err(|error| {
                    StreamingIndexerError::LocalSpill(format!(
                        "could not open v3 partition count table in {}: {error}",
                        database_path.display()
                    ))
                })?;
            write_txn
                .open_table(V3_BLOCK_HASH_PARTITIONS_TABLE)
                .map_err(|error| {
                    StreamingIndexerError::LocalSpill(format!(
                        "could not open v3 block-id partition table in {}: {error}",
                        database_path.display()
                    ))
                })?;
            write_txn
                .open_table(V3_INDEXED_CHILD_PARTITIONS_TABLE)
                .map_err(|error| {
                    StreamingIndexerError::LocalSpill(format!(
                        "could not open v3 summary partition table in {}: {error}",
                        database_path.display()
                    ))
                })?;
        }
        write_txn.commit().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not commit v3 partition database initialization for {}: {error}",
                database_path.display()
            ))
        })?;
        Ok(Self {
            database,
            database_path,
        })
    }

    fn append_block_hashes(
        &self,
        partition_id: &str,
        block_ids: &[BlockHash],
    ) -> Result<(), StreamingIndexerError> {
        if block_ids.is_empty() {
            return Ok(());
        }
        let write_txn = self.database.begin_write().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start a v3 block-id partition write in {}: {error}",
                self.database_path.display()
            ))
        })?;
        let start_index = self.partition_count_from_write_txn(
            &write_txn,
            partition_id,
            WorkingItemKind::LeafBlockIds,
        )?;
        {
            let mut table = write_txn
                .open_table(V3_BLOCK_HASH_PARTITIONS_TABLE)
                .map_err(|error| {
                    StreamingIndexerError::LocalSpill(format!(
                        "could not open the v3 block-id partition table in {}: {error}",
                        self.database_path.display()
                    ))
                })?;
            for (offset, block_id) in block_ids.iter().enumerate() {
                let key = partition_entry_key(partition_id, start_index + offset)?;
                table
                    .insert(key.as_slice(), &block_id.as_bytes()[..])
                    .map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not append block-id partition data for {} in {}: {error}",
                            partition_id,
                            self.database_path.display()
                        ))
                    })?;
            }
        }
        self.set_partition_count_in_write_txn(
            &write_txn,
            partition_id,
            WorkingItemKind::LeafBlockIds,
            start_index + block_ids.len(),
        )?;
        write_txn.commit().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not commit v3 block-id partition data for {} in {}: {error}",
                partition_id,
                self.database_path.display()
            ))
        })
    }

    fn append_indexed_children(
        &self,
        partition_id: &str,
        children: &[IndexedChild],
    ) -> Result<(), StreamingIndexerError> {
        if children.is_empty() {
            return Ok(());
        }
        let write_txn = self.database.begin_write().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start a v3 summary partition write in {}: {error}",
                self.database_path.display()
            ))
        })?;
        let start_index = self.partition_count_from_write_txn(
            &write_txn,
            partition_id,
            WorkingItemKind::IndexedChildren,
        )?;
        {
            let mut table = write_txn
                .open_table(V3_INDEXED_CHILD_PARTITIONS_TABLE)
                .map_err(|error| {
                    StreamingIndexerError::LocalSpill(format!(
                        "could not open the v3 summary partition table in {}: {error}",
                        self.database_path.display()
                    ))
                })?;
            for (offset, child) in children.iter().enumerate() {
                let key = partition_entry_key(partition_id, start_index + offset)?;
                let bytes = serialize_spilled_indexed_child_bytes(child)?;
                table
                    .insert(key.as_slice(), bytes.as_slice())
                    .map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not append summary partition data for {} in {}: {error}",
                            partition_id,
                            self.database_path.display()
                        ))
                    })?;
            }
        }
        self.set_partition_count_in_write_txn(
            &write_txn,
            partition_id,
            WorkingItemKind::IndexedChildren,
            start_index + children.len(),
        )?;
        write_txn.commit().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not commit v3 summary partition data for {} in {}: {error}",
                partition_id,
                self.database_path.display()
            ))
        })
    }

    fn clear_partitions(
        &self,
        kind: WorkingItemKind,
        partition_ids: &[String],
    ) -> Result<(), StreamingIndexerError> {
        if partition_ids.is_empty() {
            return Ok(());
        }
        let write_txn = self.database.begin_write().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start a v3 partition cleanup in {}: {error}",
                self.database_path.display()
            ))
        })?;
        for partition_id in partition_ids {
            self.clear_partition_in_write_txn(&write_txn, kind, partition_id)?;
        }
        write_txn.commit().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not commit v3 partition cleanup in {}: {error}",
                self.database_path.display()
            ))
        })
    }

    fn read_block_hash_batch(
        &self,
        partition_id: &str,
        start_index: usize,
        batch_size: usize,
    ) -> Result<Vec<BlockHash>, StreamingIndexerError> {
        let read_txn = self.database.begin_read().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start a v3 block-id partition read in {}: {error}",
                self.database_path.display()
            ))
        })?;
        let table = read_txn
            .open_table(V3_BLOCK_HASH_PARTITIONS_TABLE)
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not open the v3 block-id partition table in {}: {error}",
                    self.database_path.display()
                ))
            })?;
        let start_key = partition_entry_key(partition_id, start_index)?;
        let end_key = partition_entry_key_end(partition_id);
        let mut block_ids = Vec::with_capacity(batch_size);
        let entries = table
            .range(start_key.as_slice()..end_key.as_slice())
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not iterate block-id partition {} in {}: {error}",
                    partition_id,
                    self.database_path.display()
                ))
            })?;
        for entry in entries.take(batch_size) {
            let (_, value) = entry.map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not read block-id partition {} in {}: {error}",
                    partition_id,
                    self.database_path.display()
                ))
            })?;
            let bytes = value.value();
            if bytes.len() != BlockHash::LEN {
                return Err(StreamingIndexerError::LocalSpill(format!(
                    "truncated v3 block-id partition entry for {} in {}",
                    partition_id,
                    self.database_path.display()
                )));
            }
            let mut raw = [0u8; BlockHash::LEN];
            raw.copy_from_slice(bytes);
            block_ids.push(BlockHash::from_bytes(raw));
        }
        Ok(block_ids)
    }

    fn read_indexed_child_batch(
        &self,
        partition_id: &str,
        start_index: usize,
        batch_size: usize,
    ) -> Result<Vec<IndexedChild>, StreamingIndexerError> {
        let read_txn = self.database.begin_read().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start a v3 summary partition read in {}: {error}",
                self.database_path.display()
            ))
        })?;
        let table = read_txn
            .open_table(V3_INDEXED_CHILD_PARTITIONS_TABLE)
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not open the v3 summary partition table in {}: {error}",
                    self.database_path.display()
                ))
            })?;
        let start_key = partition_entry_key(partition_id, start_index)?;
        let end_key = partition_entry_key_end(partition_id);
        let mut children = Vec::with_capacity(batch_size);
        let entries = table
            .range(start_key.as_slice()..end_key.as_slice())
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not iterate summary partition {} in {}: {error}",
                    partition_id,
                    self.database_path.display()
                ))
            })?;
        for entry in entries.take(batch_size) {
            let (_, value) = entry.map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not read summary partition {} in {}: {error}",
                    partition_id,
                    self.database_path.display()
                ))
            })?;
            children.push(deserialize_spilled_indexed_child_bytes(value.value())?);
        }
        Ok(children)
    }

    fn partition_count_from_write_txn(
        &self,
        write_txn: &redb::WriteTransaction,
        partition_id: &str,
        kind: WorkingItemKind,
    ) -> Result<usize, StreamingIndexerError> {
        let counts = write_txn
            .open_table(V3_PARTITION_COUNTS_TABLE)
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not open the v3 partition count table in {}: {error}",
                    self.database_path.display()
                ))
            })?;
        let key = partition_count_key(kind, partition_id);
        let count = counts.get(key.as_slice()).map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not read the v3 partition count for {} in {}: {error}",
                partition_id,
                self.database_path.display()
            ))
        })?;
        match count {
            Some(count) => decode_partition_count(count.value(), partition_id),
            None => Ok(0),
        }
    }

    fn set_partition_count_in_write_txn(
        &self,
        write_txn: &redb::WriteTransaction,
        partition_id: &str,
        kind: WorkingItemKind,
        count: usize,
    ) -> Result<(), StreamingIndexerError> {
        let count = u64::try_from(count).map_err(|_| {
            StreamingIndexerError::LocalSpill(format!(
                "v3 partition count for {} does not fit u64",
                partition_id
            ))
        })?;
        let mut counts = write_txn
            .open_table(V3_PARTITION_COUNTS_TABLE)
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not open the v3 partition count table in {}: {error}",
                    self.database_path.display()
                ))
            })?;
        let key = partition_count_key(kind, partition_id);
        let bytes = count.to_le_bytes();
        counts
            .insert(key.as_slice(), bytes.as_slice())
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not write the v3 partition count for {} in {}: {error}",
                    partition_id,
                    self.database_path.display()
                ))
            })?;
        Ok(())
    }

    fn clear_partition_in_write_txn(
        &self,
        write_txn: &redb::WriteTransaction,
        kind: WorkingItemKind,
        partition_id: &str,
    ) -> Result<(), StreamingIndexerError> {
        let start_key = partition_entry_key(partition_id, 0)?;
        let end_key = partition_entry_key_end(partition_id);
        let keys = match kind {
            WorkingItemKind::LeafBlockIds => {
                let table = write_txn
                    .open_table(V3_BLOCK_HASH_PARTITIONS_TABLE)
                    .map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not open the v3 block-id partition table in {}: {error}",
                            self.database_path.display()
                        ))
                    })?;
                let iter = table
                    .range(start_key.as_slice()..end_key.as_slice())
                    .map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not iterate block-id partition {} in {}: {error}",
                            partition_id,
                            self.database_path.display()
                        ))
                    })?;
                let mut keys = Vec::new();
                for entry in iter {
                    let (key, _) = entry.map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not enumerate block-id partition {} in {}: {error}",
                            partition_id,
                            self.database_path.display()
                        ))
                    })?;
                    keys.push(key.value().to_vec());
                }
                keys
            }
            WorkingItemKind::IndexedChildren => {
                let table = write_txn
                    .open_table(V3_INDEXED_CHILD_PARTITIONS_TABLE)
                    .map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not open the v3 summary partition table in {}: {error}",
                            self.database_path.display()
                        ))
                    })?;
                let iter = table
                    .range(start_key.as_slice()..end_key.as_slice())
                    .map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not iterate summary partition {} in {}: {error}",
                            partition_id,
                            self.database_path.display()
                        ))
                    })?;
                let mut keys = Vec::new();
                for entry in iter {
                    let (key, _) = entry.map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not enumerate summary partition {} in {}: {error}",
                            partition_id,
                            self.database_path.display()
                        ))
                    })?;
                    keys.push(key.value().to_vec());
                }
                keys
            }
        };
        match kind {
            WorkingItemKind::LeafBlockIds => {
                let mut table = write_txn
                    .open_table(V3_BLOCK_HASH_PARTITIONS_TABLE)
                    .map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not reopen the v3 block-id partition table in {}: {error}",
                            self.database_path.display()
                        ))
                    })?;
                for key in keys {
                    table.remove(key.as_slice()).map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not remove block-id partition {} from {}: {error}",
                            partition_id,
                            self.database_path.display()
                        ))
                    })?;
                }
            }
            WorkingItemKind::IndexedChildren => {
                let mut table = write_txn
                    .open_table(V3_INDEXED_CHILD_PARTITIONS_TABLE)
                    .map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not reopen the v3 summary partition table in {}: {error}",
                            self.database_path.display()
                        ))
                    })?;
                for key in keys {
                    table.remove(key.as_slice()).map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not remove summary partition {} from {}: {error}",
                            partition_id,
                            self.database_path.display()
                        ))
                    })?;
                }
            }
        }
        let mut counts = write_txn
            .open_table(V3_PARTITION_COUNTS_TABLE)
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not reopen the v3 partition count table in {}: {error}",
                    self.database_path.display()
                ))
            })?;
        let count_key = partition_count_key(kind, partition_id);
        counts.remove(count_key.as_slice()).map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not clear the v3 partition count for {} in {}: {error}",
                partition_id,
                self.database_path.display()
            ))
        })?;
        Ok(())
    }
}

struct BlockHashPartitionReader<'a> {
    store: &'a V3PartitionStore,
    partition_id: String,
    next_index: usize,
}

impl<'a> BlockHashPartitionReader<'a> {
    fn open(store: &'a V3PartitionStore, partition_id: &str) -> Self {
        Self {
            store,
            partition_id: partition_id.into(),
            next_index: 0,
        }
    }

    fn next_batch(
        &mut self,
        batch_size: usize,
    ) -> Result<Option<Vec<BlockHash>>, StreamingIndexerError> {
        let batch =
            self.store
                .read_block_hash_batch(&self.partition_id, self.next_index, batch_size)?;
        if batch.is_empty() {
            return Ok(None);
        }
        self.next_index += batch.len();
        Ok(Some(batch))
    }
}

struct IndexedChildPartitionReader<'a> {
    store: &'a V3PartitionStore,
    partition_id: String,
    next_index: usize,
}

impl<'a> IndexedChildPartitionReader<'a> {
    fn open(store: &'a V3PartitionStore, partition_id: &str) -> Self {
        Self {
            store,
            partition_id: partition_id.into(),
            next_index: 0,
        }
    }

    fn next_batch(
        &mut self,
        batch_size: usize,
    ) -> Result<Option<Vec<IndexedChild>>, StreamingIndexerError> {
        let batch =
            self.store
                .read_indexed_child_batch(&self.partition_id, self.next_index, batch_size)?;
        if batch.is_empty() {
            return Ok(None);
        }
        self.next_index += batch.len();
        Ok(Some(batch))
    }
}

struct BlockHashPartitionWriters<'a> {
    store: &'a V3PartitionStore,
    partition_ids: Vec<String>,
}

impl<'a> BlockHashPartitionWriters<'a> {
    fn create(
        store: &'a V3PartitionStore,
        partition_ids: &[String],
    ) -> Result<Self, StreamingIndexerError> {
        store.clear_partitions(WorkingItemKind::LeafBlockIds, partition_ids)?;
        Ok(Self {
            store,
            partition_ids: partition_ids.to_vec(),
        })
    }

    fn len(&self) -> usize {
        self.partition_ids.len()
    }

    fn write_batch(
        &mut self,
        index: usize,
        block_ids: &[BlockHash],
    ) -> Result<(), StreamingIndexerError> {
        self.store
            .append_block_hashes(self.partition_ids[index].as_str(), block_ids)
    }

    fn finish(self) -> Result<(), StreamingIndexerError> {
        Ok(())
    }
}

struct IndexedChildPartitionWriters<'a> {
    store: &'a V3PartitionStore,
    partition_ids: Vec<String>,
}

impl<'a> IndexedChildPartitionWriters<'a> {
    fn create(
        store: &'a V3PartitionStore,
        partition_ids: &[String],
    ) -> Result<Self, StreamingIndexerError> {
        store.clear_partitions(WorkingItemKind::IndexedChildren, partition_ids)?;
        Ok(Self {
            store,
            partition_ids: partition_ids.to_vec(),
        })
    }

    fn len(&self) -> usize {
        self.partition_ids.len()
    }

    fn write_batch(
        &mut self,
        index: usize,
        children: &[IndexedChild],
    ) -> Result<(), StreamingIndexerError> {
        self.store
            .append_indexed_children(self.partition_ids[index].as_str(), children)
    }

    fn finish(self) -> Result<(), StreamingIndexerError> {
        Ok(())
    }
}

fn write_indexed_child_partition(
    store: &V3PartitionStore,
    partition_id: &str,
    children: &[IndexedChild],
) -> Result<(), StreamingIndexerError> {
    store.clear_partitions(
        WorkingItemKind::IndexedChildren,
        &[partition_id.to_string()],
    )?;
    store.append_indexed_children(partition_id, children)
}

fn read_all_block_hashes(
    store: &V3PartitionStore,
    partition_id: &str,
) -> Result<Vec<BlockHash>, StreamingIndexerError> {
    let mut reader = BlockHashPartitionReader::open(store, partition_id);
    let mut all = Vec::new();
    while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
        all.extend(batch);
    }
    Ok(all)
}

fn read_all_indexed_children(
    store: &V3PartitionStore,
    partition_id: &str,
    progress: Option<&AtomicUsize>,
) -> Result<Vec<IndexedChild>, StreamingIndexerError> {
    let mut reader = IndexedChildPartitionReader::open(store, partition_id);
    let mut all = Vec::new();
    while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
        if let Some(progress) = progress {
            progress.fetch_add(batch.len(), AtomicOrdering::Relaxed);
        }
        all.extend(batch);
    }
    Ok(all)
}
fn fallback_assignment_map(
    item_count: usize,
    groups: &[Vec<usize>],
) -> Result<Vec<usize>, StreamingIndexerError> {
    let mut assignment = vec![usize::MAX; item_count];
    for (group_index, group) in groups.iter().enumerate() {
        for &item_index in group {
            let slot = assignment.get_mut(item_index).ok_or_else(|| {
                StreamingIndexerError::HierarchyValidation(format!(
                    "fallback split referenced out-of-range item index {item_index} for partition size {item_count}"
                ))
            })?;
            if *slot != usize::MAX {
                return Err(StreamingIndexerError::HierarchyValidation(format!(
                    "fallback split assigned item index {item_index} more than once"
                )));
            }
            *slot = group_index;
        }
    }
    if let Some((item_index, _)) = assignment
        .iter()
        .enumerate()
        .find(|(_, group_index)| **group_index == usize::MAX)
    {
        return Err(StreamingIndexerError::HierarchyValidation(format!(
            "fallback split left item index {item_index} unassigned"
        )));
    }
    Ok(assignment)
}

fn rewrite_block_hash_partition_with_assignments(
    store: &V3PartitionStore,
    source_partition_id: &str,
    destination_partition_ids: &[String],
    assignment: &[usize],
) -> Result<(), StreamingIndexerError> {
    let mut reader = BlockHashPartitionReader::open(store, source_partition_id);
    let mut writers = BlockHashPartitionWriters::create(store, destination_partition_ids)?;
    let mut item_index = 0usize;
    while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
        let mut grouped = vec![Vec::new(); writers.len()];
        for block_id in batch {
            let group_index = *assignment.get(item_index).ok_or_else(|| {
                StreamingIndexerError::HierarchyValidation(format!(
                    "fallback block-id rewrite observed more than {} items",
                    assignment.len()
                ))
            })?;
            grouped[group_index].push(block_id);
            item_index += 1;
        }
        for (group_index, block_ids) in grouped.iter().enumerate() {
            writers.write_batch(group_index, block_ids.as_slice())?;
        }
    }
    if item_index != assignment.len() {
        return Err(StreamingIndexerError::HierarchyValidation(format!(
            "fallback block-id rewrite observed {item_index} items but expected {}",
            assignment.len()
        )));
    }
    writers.finish()
}

fn rewrite_indexed_child_partition_with_assignments(
    store: &V3PartitionStore,
    source_partition_id: &str,
    destination_partition_ids: &[String],
    assignment: &[usize],
) -> Result<(), StreamingIndexerError> {
    let mut reader = IndexedChildPartitionReader::open(store, source_partition_id);
    let mut writers = IndexedChildPartitionWriters::create(store, destination_partition_ids)?;
    let mut item_index = 0usize;
    while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
        let mut grouped = vec![Vec::new(); writers.len()];
        for child in batch {
            let group_index = *assignment.get(item_index).ok_or_else(|| {
                StreamingIndexerError::HierarchyValidation(format!(
                    "fallback summary rewrite observed more than {} items",
                    assignment.len()
                ))
            })?;
            grouped[group_index].push(child);
            item_index += 1;
        }
        for (group_index, children) in grouped.iter().enumerate() {
            writers.write_batch(group_index, children.as_slice())?;
        }
    }
    if item_index != assignment.len() {
        return Err(StreamingIndexerError::HierarchyValidation(format!(
            "fallback summary rewrite observed {item_index} items but expected {}",
            assignment.len()
        )));
    }
    writers.finish()
}

fn partition_count_key(kind: WorkingItemKind, partition_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(partition_id.len() + 1);
    key.push(match kind {
        WorkingItemKind::LeafBlockIds => b'l',
        WorkingItemKind::IndexedChildren => b's',
    });
    key.extend_from_slice(partition_id.as_bytes());
    key
}

fn partition_entry_key(partition_id: &str, index: usize) -> Result<Vec<u8>, StreamingIndexerError> {
    let index = u64::try_from(index).map_err(|_| {
        StreamingIndexerError::LocalSpill(format!(
            "v3 partition index for {} does not fit u64",
            partition_id
        ))
    })?;
    let mut key = Vec::with_capacity(partition_id.len() + 1 + std::mem::size_of::<u64>());
    key.extend_from_slice(partition_id.as_bytes());
    key.push(0);
    key.extend_from_slice(&index.to_be_bytes());
    Ok(key)
}

fn partition_entry_key_end(partition_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(partition_id.len() + 2);
    key.extend_from_slice(partition_id.as_bytes());
    key.push(0xff);
    key
}

fn decode_partition_count(
    bytes: &[u8],
    partition_id: &str,
) -> Result<usize, StreamingIndexerError> {
    if bytes.len() != std::mem::size_of::<u64>() {
        return Err(StreamingIndexerError::LocalSpill(format!(
            "stored v3 partition count for {} is malformed",
            partition_id
        )));
    }
    let mut raw = [0u8; std::mem::size_of::<u64>()];
    raw.copy_from_slice(bytes);
    usize::try_from(u64::from_le_bytes(raw)).map_err(|_| {
        StreamingIndexerError::LocalSpill(format!(
            "stored v3 partition count for {} does not fit usize",
            partition_id
        ))
    })
}

fn serialize_spilled_indexed_child_bytes(
    child: &IndexedChild,
) -> Result<Vec<u8>, StreamingIndexerError> {
    let embedding_len = u32::try_from(child.embedding.len()).map_err(|_| {
        StreamingIndexerError::LocalSpill(
            "embedding length does not fit the v3 partition database format".into(),
        )
    })?;
    let descendant_count = u64::try_from(child.descendant_count).map_err(|_| {
        StreamingIndexerError::LocalSpill(
            "descendant count does not fit the v3 partition database format".into(),
        )
    })?;
    let mut bytes = Vec::with_capacity(4 + child.embedding.len() + BlockHash::LEN + 8 + 8);
    bytes.extend_from_slice(&embedding_len.to_le_bytes());
    bytes.extend_from_slice(child.embedding.as_slice());
    bytes.extend_from_slice(child.child.as_bytes());
    bytes.extend_from_slice(&child.level.to_le_bytes());
    bytes.extend_from_slice(&descendant_count.to_le_bytes());
    Ok(bytes)
}

fn deserialize_spilled_indexed_child_bytes(
    bytes: &[u8],
) -> Result<IndexedChild, StreamingIndexerError> {
    let mut reader = std::io::Cursor::new(bytes);
    let mut embedding_len_bytes = [0u8; 4];
    reader
        .read_exact(&mut embedding_len_bytes)
        .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
    let embedding_len = usize::try_from(u32::from_le_bytes(embedding_len_bytes)).map_err(|_| {
        StreamingIndexerError::LocalSpill(
            "spilled summary embedding length does not fit usize".into(),
        )
    })?;
    let mut embedding = vec![0u8; embedding_len];
    reader
        .read_exact(embedding.as_mut_slice())
        .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
    let mut child_bytes = [0u8; BlockHash::LEN];
    reader
        .read_exact(&mut child_bytes)
        .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
    let mut level_bytes = [0u8; 8];
    reader
        .read_exact(&mut level_bytes)
        .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
    let mut descendant_count_bytes = [0u8; 8];
    reader
        .read_exact(&mut descendant_count_bytes)
        .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
    Ok(IndexedChild {
        embedding,
        child: BlockHash::from_bytes(child_bytes),
        level: u64::from_le_bytes(level_bytes),
        descendant_count: usize::try_from(u64::from_le_bytes(descendant_count_bytes)).map_err(
            |_| {
                StreamingIndexerError::LocalSpill(
                    "spilled summary descendant count does not fit usize".into(),
                )
            },
        )?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::Write;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::stream;
    use lexongraph_block::{
        Block, BranchEntry, Content, LeafEntry, build_branch_block, build_leaf_block,
    };
    use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};

    #[derive(Default)]
    struct MemoryBlockStore {
        blocks: Mutex<HashMap<BlockHash, Vec<u8>>>,
    }

    #[async_trait]
    impl BlockStore for MemoryBlockStore {
        async fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            self.blocks
                .lock()
                .unwrap()
                .insert(*block_id, block_bytes.to_vec());
            Ok(())
        }

        async fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            Ok(self.blocks.lock().unwrap().get(block_id).cloned())
        }

        fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
            let ids = self
                .blocks
                .lock()
                .unwrap()
                .keys()
                .copied()
                .collect::<Vec<_>>();
            Ok(Box::pin(stream::iter(ids.into_iter().map(Ok))))
        }
    }

    fn spec() -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        }
    }

    fn embedding_bytes(values: [f32; 2]) -> Vec<u8> {
        values
            .into_iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }

    async fn store_leaf(store: &MemoryBlockStore, values: [f32; 2], body: &str) -> BlockHash {
        let block = Block::Leaf(
            build_leaf_block(
                VERSION_1,
                spec(),
                vec![LeafEntry {
                    embedding: embedding_bytes(values),
                    metadata: vec![],
                    content: Content {
                        media_type: "text/plain".into(),
                        body: body.as_bytes().to_vec(),
                    },
                }],
                None,
            )
            .unwrap(),
        );
        store.put(&block).await.unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v3_rejects_empty_input() {
        let working_root = tempfile::tempdir().unwrap();
        let mut run = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_7_0,
            spec(),
            4096,
            working_root.path(),
        )
        .unwrap();
        let store = MemoryBlockStore::default();
        let error = run.finalize(&store, &store).await.unwrap_err();
        assert!(matches!(error, StreamingIndexerError::EmptyInput));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v3_rejects_non_leaf_input() {
        let working_root = tempfile::tempdir().unwrap();
        let source = MemoryBlockStore::default();
        let left = store_leaf(&source, [1.0, 0.0], "a").await;
        let right = store_leaf(&source, [0.0, 1.0], "b").await;
        let branch = Block::Branch(
            build_branch_block(
                VERSION_1,
                1,
                spec(),
                vec![
                    BranchEntry {
                        embedding: embedding_bytes([0.0, 1.0]),
                        child: right,
                    },
                    BranchEntry {
                        embedding: embedding_bytes([1.0, 0.0]),
                        child: left,
                    },
                ],
                None,
            )
            .unwrap(),
        );
        let branch_id = source.put(&branch).await.unwrap();
        let output = MemoryBlockStore::default();
        let mut run = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_7_0,
            spec(),
            4096,
            working_root.path(),
        )
        .unwrap();
        run.ingest_block_id_batch(&[branch_id]).await.unwrap();
        let error = run.finalize(&source, &output).await.unwrap_err();
        assert!(matches!(
            error,
            StreamingIndexerError::HierarchyValidation(message)
                if message.contains("not a leaf block")
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v3_is_deterministic_and_cleans_up_successfully() {
        let parent = tempfile::tempdir().unwrap();
        let source = MemoryBlockStore::default();
        let ids = vec![
            store_leaf(&source, [0.0, 0.0], "a").await,
            store_leaf(&source, [0.1, 0.0], "b").await,
            store_leaf(&source, [10.0, 10.0], "c").await,
            store_leaf(&source, [10.1, 10.0], "d").await,
        ];

        let output_a = MemoryBlockStore::default();
        let mut run_a = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_7_0,
            spec(),
            4096,
            parent.path(),
        )
        .unwrap();
        run_a.ingest_block_id_batch(ids.as_slice()).await.unwrap();
        let result_a = run_a.finalize(&source, &output_a).await.unwrap();

        let output_b = MemoryBlockStore::default();
        let mut run_b = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_7_0,
            spec(),
            4096,
            parent.path(),
        )
        .unwrap();
        run_b.ingest_block_id_batch(ids.as_slice()).await.unwrap();
        let result_b = run_b.finalize(&source, &output_b).await.unwrap();

        assert_eq!(result_a.root_id, result_b.root_id);
        assert_eq!(result_a.block_ids, result_b.block_ids);
        assert!(std::fs::read_dir(parent.path()).unwrap().next().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v3_observer_reports_terminal_materialization_phase() {
        let parent = tempfile::tempdir().unwrap();
        let source = MemoryBlockStore::default();
        let output = MemoryBlockStore::default();
        let ids = vec![
            store_leaf(&source, [0.0, 0.0], "a").await,
            store_leaf(&source, [0.1, 0.0], "b").await,
            store_leaf(&source, [10.0, 10.0], "c").await,
        ];
        let phases = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let phases = Arc::clone(&phases);
            Arc::new(move |status: crate::StreamingIndexingStatus| {
                phases.lock().unwrap().push(status.phase);
            }) as StreamingIndexingStatusObserver
        };
        let mut run = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_7_0,
            spec(),
            4096,
            parent.path(),
        )
        .unwrap()
        .with_observer(observer);
        run.ingest_block_id_batch(ids.as_slice()).await.unwrap();
        run.finalize(&source, &output).await.unwrap();
        let phases = phases.lock().unwrap().clone();
        assert!(phases.iter().any(|phase| matches!(
            phase,
            StreamingIndexingPhase::V3TerminalMaterializationLoad { .. }
        )));
        assert!(
            !phases
                .iter()
                .any(|phase| matches!(phase, StreamingIndexingPhase::V3PartitionLoad { .. }))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v3_load_failure_emits_failed_status() {
        let parent = tempfile::tempdir().unwrap();
        let source = MemoryBlockStore::default();
        let output = MemoryBlockStore::default();
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let statuses = Arc::clone(&statuses);
            Arc::new(move |status: crate::StreamingIndexingStatus| {
                statuses.lock().unwrap().push(status);
            }) as StreamingIndexingStatusObserver
        };
        let mut run = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_7_0,
            spec(),
            4096,
            parent.path(),
        )
        .unwrap()
        .with_observer(observer);
        run.ingest_block_id_batch(&[BlockHash::from_bytes([7u8; BlockHash::LEN])])
            .await
            .unwrap();
        let error = run.finalize(&source, &output).await.unwrap_err();
        assert!(matches!(error, StreamingIndexerError::Storage(_)));
        assert!(statuses.lock().unwrap().iter().any(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::V3TerminalMaterializationLoad { .. }
            ) && status.state == StreamingIndexingStatusState::Failed
        }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v3_split_partition_emits_training_and_classification_phases() {
        struct TestClassifier {
            config: StreamingClusteringConfig,
        }

        impl StreamingClusterClassifier for TestClassifier {
            fn config(&self) -> &StreamingClusteringConfig {
                &self.config
            }

            fn assign(&self, embedding: &[f32]) -> Result<u32, StreamingClusteringError> {
                Ok(if embedding[0] < 5.0 { 0 } else { 1 })
            }
        }

        let parent = tempfile::tempdir().unwrap();
        let source = MemoryBlockStore::default();
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let statuses = Arc::clone(&statuses);
            Arc::new(move |status: crate::StreamingIndexingStatus| {
                statuses.lock().unwrap().push(status);
            }) as StreamingIndexingStatusObserver
        };
        let run = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_7_0,
            spec(),
            4096,
            parent.path(),
        )
        .unwrap()
        .with_observer(observer);

        let ids = [
            store_leaf(&source, [0.0, 0.0], "a").await,
            store_leaf(&source, [0.1, 0.0], "b").await,
            store_leaf(&source, [10.0, 10.0], "c").await,
            store_leaf(&source, [10.1, 10.0], "d").await,
        ];
        run.partition_store
            .append_block_hashes(&run.root_partition_id, ids.as_slice())
            .unwrap();

        let partition = WorkingPartition {
            id: "l0.p0".into(),
            layer_index: 0,
            item_count: ids.len(),
            kind: WorkingItemKind::LeafBlockIds,
        };
        let settings = run.profile_settings().unwrap();
        let mut trainer = DirectionalPcaStreamingTrainer::new(
            StreamingClusteringConfig {
                cluster_count: 2,
                dimensions: run.dimensions().unwrap(),
                balance_constraints: None,
                random_seed: settings.random_seed,
            },
            settings.params.clone(),
        )
        .unwrap();
        run.ingest_leaf_training_partition_batches(&partition, &source, &mut trainer)
            .unwrap();
        let classifier = TestClassifier {
            config: StreamingClusteringConfig {
                cluster_count: 2,
                dimensions: run.dimensions().unwrap(),
                balance_constraints: None,
                random_seed: settings.random_seed,
            },
        };
        let child_ids = ["l0.p0.0".to_string(), "l0.p0.1".to_string()];
        let mut writers =
            BlockHashPartitionWriters::create(&run.partition_store, child_ids.as_slice()).unwrap();
        let mut child_item_counts = vec![0usize; child_ids.len()];
        run.classify_leaf_partition_batches(
            &partition,
            &source,
            &classifier,
            &mut writers,
            child_item_counts.as_mut_slice(),
        )
        .unwrap();
        writers.finish().unwrap();
        assert_eq!(child_item_counts.into_iter().sum::<usize>(), ids.len());

        let statuses = statuses.lock().unwrap().clone();
        assert!(statuses.iter().any(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::V3PartitionTrainIngest { layer_index: 0 }
            ) && status.state == StreamingIndexingStatusState::Completed
                && status.progress_unit_kind
                    == Some(StreamingIndexingProgressUnitKind::V3TrainIngestItem)
        }));
        assert!(statuses.iter().any(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::V3PartitionClassify { layer_index: 0 }
            ) && status.state == StreamingIndexingStatusState::Completed
                && status.progress_unit_kind
                    == Some(StreamingIndexingProgressUnitKind::V3ClassifiedItem)
        }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v3_split_partition_emits_failed_planning_status() {
        let parent = tempfile::tempdir().unwrap();
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let statuses = Arc::clone(&statuses);
            Arc::new(move |status: crate::StreamingIndexingStatus| {
                statuses.lock().unwrap().push(status);
            }) as StreamingIndexingStatusObserver
        };
        let run = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_7_0,
            spec(),
            4096,
            parent.path(),
        )
        .unwrap()
        .with_observer(observer);

        let missing_a = BlockHash::from_bytes([11u8; BlockHash::LEN]);
        let missing_b = BlockHash::from_bytes([12u8; BlockHash::LEN]);
        run.partition_store
            .append_block_hashes(&run.root_partition_id, &[missing_a, missing_b])
            .unwrap();

        let partition = WorkingPartition {
            id: "l0.p0".into(),
            layer_index: 0,
            item_count: 2,
            kind: WorkingItemKind::LeafBlockIds,
        };
        let source = MemoryBlockStore::default();
        let error = run
            .split_partition(&partition, 1, &source)
            .await
            .unwrap_err();
        assert!(matches!(error, StreamingIndexerError::Storage(_)));
        assert!(statuses.lock().unwrap().iter().any(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::HierarchyPlanning {
                    stage: PlanningStage::Custom
                }
            ) && status.state == StreamingIndexingStatusState::Failed
        }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v3_finalize_cancellation_returns_explicit_error_and_requires_fresh_run() {
        let parent = tempfile::tempdir().unwrap();
        let source = MemoryBlockStore::default();
        let output = MemoryBlockStore::default();
        let cancellation = StreamingIndexingCancellationHandle::new();
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let cancellation = cancellation.clone();
            let statuses = Arc::clone(&statuses);
            Arc::new(move |status: crate::StreamingIndexingStatus| {
                if matches!(
                    status.phase,
                    StreamingIndexingPhase::V3PartitionTrainIngest { layer_index: 0 }
                ) && status.state == StreamingIndexingStatusState::Started
                {
                    cancellation.cancel();
                }
                statuses.lock().unwrap().push(status);
            }) as StreamingIndexingStatusObserver
        };
        let mut run = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_7_0,
            spec(),
            4096,
            parent.path(),
        )
        .unwrap()
        .with_cancellation_handle(cancellation.clone())
        .with_observer(observer);

        let mut ids = Vec::new();
        for index in 0..(V3_BATCH_SIZE * 8) {
            ids.push(
                store_leaf(
                    &source,
                    [index as f32, ((index * 7) % 97) as f32],
                    &format!("leaf-{index}"),
                )
                .await,
            );
        }
        run.ingest_block_id_batch(ids.as_slice()).await.unwrap();

        let error = run.finalize(&source, &output).await.unwrap_err();
        assert!(matches!(error, StreamingIndexerError::Cancelled(_)));

        let statuses = statuses.lock().unwrap().clone();
        assert!(statuses.iter().any(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::V3PartitionTrainIngest { layer_index: 0 }
            ) && status.state == StreamingIndexingStatusState::Failed
        }));
        assert!(statuses.iter().any(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::HierarchyPlanning {
                    stage: PlanningStage::Custom
                }
            ) && status.state == StreamingIndexingStatusState::Failed
        }));

        let second_finalize = run.finalize(&source, &output).await.unwrap_err();
        assert!(matches!(
            second_finalize,
            StreamingIndexerError::InvalidLifecycleTransition(_)
        ));
        let ingest_after_cancel = run.ingest_block_id_batch(&ids[..1]).await.unwrap_err();
        assert!(matches!(
            ingest_after_cancel,
            StreamingIndexerError::InvalidLifecycleTransition(_)
        ));
    }

    #[test]
    fn v3_block_hash_partition_reader_rejects_truncated_hash() {
        let dir = tempfile::tempdir().unwrap();
        let store = V3PartitionStore::new(dir.path()).unwrap();
        let write_txn = store.database.begin_write().unwrap();
        {
            let mut table = write_txn
                .open_table(V3_BLOCK_HASH_PARTITIONS_TABLE)
                .unwrap();
            let key = partition_entry_key("l0.p0", 0).unwrap();
            table
                .insert(key.as_slice(), &[0u8; BlockHash::LEN / 2][..])
                .unwrap();
        }
        {
            let mut counts = write_txn.open_table(V3_PARTITION_COUNTS_TABLE).unwrap();
            let key = partition_count_key(WorkingItemKind::LeafBlockIds, "l0.p0");
            counts
                .insert(key.as_slice(), 1u64.to_le_bytes().as_slice())
                .unwrap();
        }
        write_txn.commit().unwrap();

        let mut reader = BlockHashPartitionReader::open(&store, "l0.p0");
        let error = reader.next_batch(1).unwrap_err();
        assert!(matches!(
            error,
            StreamingIndexerError::LocalSpill(message)
                if message.contains("truncated v3 block-id partition entry")
        ));
    }

    #[test]
    fn v3_replay_pass_limit_is_capped_for_large_partitions() {
        assert_eq!(v3_replay_pass_limit(0), 4);
        assert_eq!(v3_replay_pass_limit(32), 36);
        assert_eq!(v3_replay_pass_limit(usize::MAX), V3_MAX_REPLAY_PASSES);
    }

    #[test]
    fn v3_cluster_assignment_rejects_out_of_range_clusters() {
        let error = validate_v3_cluster_assignment(3, 3).unwrap_err();
        assert!(matches!(
            error,
            StreamingIndexerError::HierarchyValidation(message)
                if message.contains("exceeds available child partitions")
        ));
    }

    #[test]
    fn v3_prepare_pipeline_caps_future_batch_lead_at_three() {
        fn update_max(target: &AtomicUsize, value: usize) {
            let mut current = target.load(Ordering::SeqCst);
            while value > current {
                match target.compare_exchange(current, value, Ordering::SeqCst, Ordering::SeqCst) {
                    Ok(_) => break,
                    Err(observed) => current = observed,
                }
            }
        }

        let produced = Arc::new(AtomicUsize::new(0));
        let committed = Arc::new(AtomicUsize::new(0));
        let max_lead = Arc::new(AtomicUsize::new(0));
        let expected = Arc::new(AtomicUsize::new(0));

        run_prepared_batch_pipeline(
            V3_PREPARED_BATCH_LOOKAHEAD,
            {
                let produced = Arc::clone(&produced);
                let committed = Arc::clone(&committed);
                let max_lead = Arc::clone(&max_lead);
                move |sender| {
                    for batch in 0..8usize {
                        sender.send(Ok(batch)).unwrap();
                        let produced_after = produced.fetch_add(1, Ordering::SeqCst) + 1;
                        let committed_before = committed.load(Ordering::SeqCst);
                        let lead = produced_after.saturating_sub(committed_before + 1);
                        update_max(&max_lead, lead);
                    }
                    Ok(())
                }
            },
            {
                let committed = Arc::clone(&committed);
                let expected = Arc::clone(&expected);
                move |batch| {
                    assert_eq!(batch, expected.fetch_add(1, Ordering::SeqCst));
                    std::thread::sleep(Duration::from_millis(10));
                    committed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .unwrap();

        assert!(max_lead.load(Ordering::SeqCst) <= V3_PREPARED_BATCH_LOOKAHEAD);
        assert_eq!(max_lead.load(Ordering::SeqCst), V3_PREPARED_BATCH_LOOKAHEAD);
    }
}
