// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use futures::StreamExt;
use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};
use redb::{Database, Durability, ReadOnlyTable, ReadableTable, TableDefinition};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use tempfile::TempDir;

use crate::{
    Block, BlockHash, BranchEncodingPolicy, BranchEntry, ChildSummaryInput, ChildSummaryPolicy,
    EmbeddingSpec, ExactCentroidChildSummaryPolicy, HierarchyPlanningDetailFields, IndexedChild,
    LayerBuildStatus, PUBLISHED_PROFILE_V0_7_0, PUBLISHED_PROFILE_V0_8_0, PlanningStage,
    PublishedBranchEncodingPolicy, PublishedDirectionalPcaProfileSettings,
    PublishedIndexingProfile, PublishedPlanningStrategy, PublishedProfileVersion,
    StreamingClusteringConfig, StreamingIndexerError, StreamingIndexingCancellationHandle,
    StreamingIndexingPhase, StreamingIndexingProgressUnitKind, StreamingIndexingResult,
    StreamingIndexingStatusObserver, StreamingIndexingStatusState, VERSION_1, balanced_groups,
    branch_encoding_policy_for_profile, build_branch_block, decode_embedding_as_f32,
    dedup_sort_ids, effective_directional_pca_cluster_count, emit_status, encode_branch_entries,
    fallback_partition_groups, is_rank_zero_constraint, map_clustering_configuration_error,
    map_clustering_error, materializability_bound, normalize_branch_entries,
    normalize_child_summary_inputs, normalize_current_layer, partition_depth,
    published_indexing_profile, read_spilled_indexed_child, serialize_block,
    start_status_heartbeat, status_with_hierarchy_details, status_with_known_total,
    validate_embedding_bytes, validate_published_profile_configuration, verify_persisted_block_id,
    with_legacy_item_count, write_spilled_indexed_child,
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
const V3_INNER_POOL_THREAD_NAME: &str = "lexongraph-v3-inner";
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
    partition_store: Option<Arc<V3PartitionStore>>,
    inner_pool: Arc<ThreadPool>,
    #[cfg(test)]
    inner_pool_worker_names: Arc<std::sync::Mutex<Vec<String>>>,
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
        if profile_version != PUBLISHED_PROFILE_V0_7_0
            && profile_version != PUBLISHED_PROFILE_V0_8_0
        {
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
                "streaming v3 currently supports only the exact 0.7.0 or 0.8.0 ambient-delta-uq branch encoding contract".into(),
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
        let partition_store = Arc::new(V3PartitionStore::new(temp_root.path())?);
        let inner_pool = build_v3_inner_pool()?;
        Ok(Self {
            observer: None,
            cancellation: StreamingIndexingCancellationHandle::new(),
            branch_encoding_policy: branch_encoding_policy_for_profile(&profile),
            profile,
            embedding_spec,
            block_size_target,
            temp_root: Some(temp_root),
            partition_store: Some(partition_store),
            inner_pool,
            #[cfg(test)]
            inner_pool_worker_names: Arc::new(std::sync::Mutex::new(Vec::new())),
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
        let partition_store = Arc::clone(
            self.partition_store
                .as_ref()
                .expect("v3 partition store should exist until success"),
        );
        let root_partition_id = self.root_partition_id.clone();
        let batch_len = block_ids.len();
        let block_ids = block_ids.to_vec();
        tokio::task::spawn_blocking(move || {
            partition_store.append_block_hashes(&root_partition_id, block_ids.as_slice())
        })
        .await
        .map_err(|error| {
            StreamingIndexerError::ClusteringFailure(format!(
                "v3 block-id ingestion task failed: {error}"
            ))
        })??;
        self.check_cancelled_mut("block-id ingestion")?;
        self.ingested_count += batch_len;
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
                drop(self.partition_store.take());
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
                self.partition_store(),
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
            let refinement_total = active
                .iter()
                .filter(|partition| {
                    partition.item_count > materializability_bound && partition.item_count > 1
                })
                .map(|partition| partition.item_count)
                .sum::<usize>();
            let terminal_total = active
                .iter()
                .filter(|partition| {
                    partition.item_count <= materializability_bound || partition.item_count <= 1
                })
                .map(|partition| partition.item_count)
                .sum::<usize>();
            let layer_started = Instant::now();
            if refinement_total > 0 {
                self.emit_aggregate_v3_phase(
                    StreamingIndexingPhase::V3PartitionTrainIngest {
                        layer_index: active[0].layer_index,
                    },
                    StreamingIndexingStatusState::Started,
                    refinement_total,
                    0,
                    layer_started,
                    None,
                );
                self.emit_aggregate_v3_phase(
                    StreamingIndexingPhase::V3PartitionClassify {
                        layer_index: active[0].layer_index,
                    },
                    StreamingIndexingStatusState::Started,
                    refinement_total,
                    0,
                    layer_started,
                    None,
                );
            }
            if terminal_total > 0 {
                self.emit_aggregate_v3_phase(
                    StreamingIndexingPhase::V3TerminalMaterializationLoad {
                        layer_index: active[0].layer_index,
                    },
                    StreamingIndexingStatusState::Started,
                    terminal_total,
                    0,
                    layer_started,
                    None,
                );
            }
            let terminal_results = futures::stream::iter(
                active
                    .iter()
                    .enumerate()
                    .filter(|(_, partition)| {
                        partition.item_count <= materializability_bound || partition.item_count <= 1
                    })
                    .map(|(index, partition)| async move {
                        (
                            index,
                            self.materialize_terminal_partition(
                                partition,
                                source_store,
                                output_store,
                                false,
                            )
                            .await,
                        )
                    }),
            )
            .buffer_unordered(V3_IO_QUEUE_DEPTH)
            .collect::<Vec<_>>()
            .await;
            let refinement_results = active
                .par_iter()
                .enumerate()
                .filter(|(_, partition)| {
                    partition.item_count > materializability_bound && partition.item_count > 1
                })
                .map(|(index, partition)| {
                    (
                        index,
                        self.split_partition(
                            partition,
                            materializability_bound,
                            source_store,
                            false,
                        )
                        .map(|children| (children, None)),
                    )
                })
                .collect::<Vec<_>>();
            let mut results = (0..active.len()).map(|_| None).collect::<Vec<
                Option<
                    Result<
                        (
                            Vec<WorkingPartition>,
                            Option<(IndexedChild, Vec<BlockHash>)>,
                        ),
                        StreamingIndexerError,
                    >,
                >,
            >>();
            for (index, result) in terminal_results {
                results[index] = Some(result.map(|terminal| (Vec::new(), Some(terminal))));
            }
            for (index, result) in refinement_results {
                results[index] = Some(result);
            }
            let mut next = Vec::new();
            let mut first_error = None;
            for result in results {
                let result = result.expect("every active partition must be processed");
                let (children, terminal) = match result {
                    Ok(result) => result,
                    Err(error) => {
                        first_error = Some(error);
                        break;
                    }
                };
                next.extend(children);
                let Some((child, ids)) = terminal else {
                    continue;
                };
                persisted_ids.extend(ids);
                terminals.push(child);
            }
            if let Some(error) = first_error {
                if matches!(&error, StreamingIndexerError::Cancelled(_)) {
                    let partition = &active[0];
                    emit_status(
                        &self.observer,
                        status_with_hierarchy_details(
                            StreamingIndexingPhase::HierarchyPlanning {
                                stage: PlanningStage::Custom,
                            },
                            StreamingIndexingStatusState::Failed,
                            Some(1),
                            1,
                            layer_started.elapsed(),
                            Some(error.to_string()),
                            HierarchyPlanningDetailFields {
                                legacy_item_count: Some(partition.item_count),
                                progress_unit_kind: Some(
                                    StreamingIndexingProgressUnitKind::PartitionPlanningInvocation,
                                ),
                                discovered_unit_count: Some(1),
                                current_unit_elapsed: Some(layer_started.elapsed()),
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
                                last_progress_at: Some(layer_started.elapsed()),
                            },
                        ),
                    );
                }
                if refinement_total > 0 {
                    self.emit_aggregate_v3_phase(
                        StreamingIndexingPhase::V3PartitionTrainIngest {
                            layer_index: active[0].layer_index,
                        },
                        StreamingIndexingStatusState::Failed,
                        refinement_total,
                        0,
                        layer_started,
                        Some(error.to_string()),
                    );
                    self.emit_aggregate_v3_phase(
                        StreamingIndexingPhase::V3PartitionClassify {
                            layer_index: active[0].layer_index,
                        },
                        StreamingIndexingStatusState::Failed,
                        refinement_total,
                        0,
                        layer_started,
                        Some(error.to_string()),
                    );
                }
                if terminal_total > 0 {
                    self.emit_aggregate_v3_phase(
                        StreamingIndexingPhase::V3TerminalMaterializationLoad {
                            layer_index: active[0].layer_index,
                        },
                        StreamingIndexingStatusState::Failed,
                        terminal_total,
                        0,
                        layer_started,
                        Some(error.to_string()),
                    );
                }
                return Err(error);
            }
            if refinement_total > 0 {
                self.emit_aggregate_v3_phase(
                    StreamingIndexingPhase::V3PartitionTrainIngest {
                        layer_index: active[0].layer_index,
                    },
                    StreamingIndexingStatusState::Completed,
                    refinement_total,
                    refinement_total,
                    layer_started,
                    None,
                );
                self.emit_aggregate_v3_phase(
                    StreamingIndexingPhase::V3PartitionClassify {
                        layer_index: active[0].layer_index,
                    },
                    StreamingIndexingStatusState::Completed,
                    refinement_total,
                    refinement_total,
                    layer_started,
                    None,
                );
            }
            if terminal_total > 0 {
                self.emit_aggregate_v3_phase(
                    StreamingIndexingPhase::V3TerminalMaterializationLoad {
                        layer_index: active[0].layer_index,
                    },
                    StreamingIndexingStatusState::Completed,
                    terminal_total,
                    terminal_total,
                    layer_started,
                    None,
                );
            }
            active = next;
        }
        Ok(normalize_current_layer(terminals))
    }

    fn split_partition(
        &self,
        partition: &WorkingPartition,
        materializability_bound: usize,
        source_store: &dyn BlockStore,
        emit_partition_status: bool,
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
            match run_v3_replay_until_ready(
                &mut trainer,
                &partition.id,
                partition.item_count,
                || self.check_cancelled("partition planning"),
                |trainer| match partition.kind {
                    WorkingItemKind::LeafBlockIds => self.ingest_leaf_training_partition_batches(
                        partition,
                        source_store,
                        trainer,
                        emit_partition_status,
                    ),
                    WorkingItemKind::IndexedChildren => self
                        .ingest_summary_training_partition_batches(
                            partition,
                            trainer,
                            emit_partition_status,
                        ),
                },
            ) {
                Ok(()) => {}
                Err(V3ReplayError::Clustering(error))
                    if self.profile.version == PUBLISHED_PROFILE_V0_8_0
                        && is_rank_zero_constraint(&error) =>
                {
                    let fallback_groups = fallback_partition_groups(
                        partition.item_count,
                        materializability_bound,
                        None,
                    )
                    .map_err(|error| {
                        StreamingIndexerError::HierarchyValidation(error.to_string())
                    })?;
                    let fallback_assignment =
                        fallback_assignment_map(partition.item_count, fallback_groups.as_slice())?;
                    let child_ids = (0..fallback_groups.len())
                        .map(|child_index| format!("{}.{}", partition.id, child_index))
                        .collect::<Vec<_>>();
                    self.partition_store()
                        .clear_partitions(partition.kind, child_ids.as_slice())?;
                    match partition.kind {
                        WorkingItemKind::LeafBlockIds => {
                            rewrite_block_hash_partition_with_assignments(
                                self.partition_store(),
                                &partition.id,
                                child_ids.as_slice(),
                                fallback_assignment.as_slice(),
                            )?;
                        }
                        WorkingItemKind::IndexedChildren => {
                            rewrite_indexed_child_partition_with_assignments(
                                self.partition_store(),
                                &partition.id,
                                child_ids.as_slice(),
                                fallback_assignment.as_slice(),
                            )?;
                        }
                    }
                    return Ok((
                        fallback_groups
                            .into_iter()
                            .enumerate()
                            .map(|(index, group)| WorkingPartition {
                                id: child_ids[index].clone(),
                                layer_index: partition.layer_index,
                                item_count: group.len(),
                                kind: partition.kind,
                            })
                            .collect(),
                        true,
                    ));
                }
                Err(V3ReplayError::Clustering(error)) => return Err(map_clustering_error(error)),
                Err(V3ReplayError::Indexing(error)) => return Err(error),
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
                        self.partition_store(),
                        child_ids.as_slice(),
                    )?;
                    self.classify_leaf_partition_batches(
                        partition,
                        source_store,
                        &classifier,
                        &mut writers,
                        child_item_counts.as_mut_slice(),
                        emit_partition_status,
                    )?;
                    writers.finish()?;
                }
                WorkingItemKind::IndexedChildren => {
                    let mut writers = IndexedChildPartitionWriters::create(
                        self.partition_store(),
                        child_ids.as_slice(),
                    )?;
                    self.classify_summary_partition_batches(
                        partition,
                        &classifier,
                        &mut writers,
                        child_item_counts.as_mut_slice(),
                        emit_partition_status,
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
                self.partition_store()
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
                            self.partition_store(),
                            &partition.id,
                            child_ids.as_slice(),
                            fallback_assignment.as_slice(),
                        )?;
                    }
                    WorkingItemKind::IndexedChildren => {
                        rewrite_indexed_child_partition_with_assignments(
                            self.partition_store(),
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
        emit_partition_status: bool,
    ) -> Result<(IndexedChild, Vec<BlockHash>), StreamingIndexerError> {
        let mut persisted_ids = Vec::new();
        match partition.kind {
            WorkingItemKind::LeafBlockIds => {
                let block_ids = read_all_block_hashes(self.partition_store(), &partition.id)?;
                let loaded = self
                    .load_leaf_batch(
                        block_ids.as_slice(),
                        StreamingIndexingPhase::V3TerminalMaterializationLoad {
                            layer_index: partition.layer_index,
                        },
                        source_store,
                        emit_partition_status,
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
                    return Ok((children.remove(0), persisted_ids));
                }
                let child = self
                    .assemble_child_set(
                        children,
                        partition.id == format!("l{}.p0", partition.layer_index),
                        output_store,
                        &mut persisted_ids,
                    )
                    .await?;
                Ok((child, persisted_ids))
            }
            WorkingItemKind::IndexedChildren => {
                let mut children = self.run_v3_partition_phase(
                    StreamingIndexingPhase::V3TerminalMaterializationLoad {
                        layer_index: partition.layer_index,
                    },
                    partition.item_count,
                    emit_partition_status,
                    |progress| {
                        let phase = StreamingIndexingPhase::V3TerminalMaterializationLoad {
                            layer_index: partition.layer_index,
                        };
                        read_all_indexed_children(
                            self.partition_store(),
                            &partition.id,
                            Some(progress.as_ref()),
                            Some(&self.cancellation),
                            Some(&phase),
                        )
                    },
                )?;
                if children.len() == 1 {
                    return Ok((children.remove(0), persisted_ids));
                }
                let child = self
                    .assemble_child_set(
                        children,
                        partition.id == format!("l{}.p0", partition.layer_index),
                        output_store,
                        &mut persisted_ids,
                    )
                    .await?;
                Ok((child, persisted_ids))
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
            let (next_layer, branch_ids) = match self
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
            persisted_ids.extend(branch_ids);
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
    ) -> Result<(Vec<IndexedChild>, Vec<BlockHash>), StreamingIndexerError> {
        let progress = Arc::clone(status.progress);
        let is_global_root_partition = status.is_global_root_partition;
        let prepared = groups
            .par_iter()
            .enumerate()
            .map(|(index, group)| {
                (|| {
                    self.check_cancelled("bottom-up assembly")?;
                    let raw_entries = group
                        .iter()
                        .map(|&child_index| BranchEntry {
                            embedding: children[child_index].embedding.clone(),
                            child: children[child_index].child,
                        })
                        .collect::<Vec<_>>();
                    let raw_child_summaries = group
                        .iter()
                        .map(|&child_index| ChildSummaryInput {
                            embedding: children[child_index].embedding.clone(),
                            child: children[child_index].child,
                            level: children[child_index].level,
                            descendant_count: children[child_index].descendant_count,
                        })
                        .collect::<Vec<_>>();
                    let entries = normalize_branch_entries(raw_entries);
                    let child_summaries = normalize_child_summary_inputs(raw_child_summaries);
                    if entries.len() < 2 || child_summaries.len() < 2 {
                        return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                            "normalized child-bearing entry set has fewer than two unique children"
                                .into(),
                        ));
                    }
                    let encoded_branch = encode_branch_entries(
                        self.branch_encoding_policy,
                        &self.embedding_spec,
                        entries.as_slice(),
                        parent_level,
                        uses_root_branch_budget(is_global_root_partition, groups.len()),
                    )?;
                    let branch = build_branch_block(
                        VERSION_1,
                        parent_level,
                        encoded_branch.embedding_spec,
                        encoded_branch.entries,
                        encoded_branch.ext,
                    )
                    .map_err(StreamingIndexerError::BlockConstruction)?;
                    let branch_block = Block::Branch(branch);
                    let serialized = serialize_block(&branch_block)
                        .map_err(StreamingIndexerError::BlockConstruction)?;
                    if serialized.bytes.len() > self.block_size_target {
                        return Err(StreamingIndexerError::TerminalPartitionMaterialization(
                            format!(
                                "branch block serialized to {} bytes, exceeding block size target {}",
                                serialized.bytes.len(),
                                self.block_size_target
                            ),
                        ));
                    }
                    let canonical = ExactCentroidChildSummaryPolicy
                        .summarize_children(&self.embedding_spec, &child_summaries)
                        .map_err(|error| {
                            StreamingIndexerError::CanonicalEmbeddingFailure(error.to_string())
                        })?;
                    validate_embedding_bytes(&canonical, &self.embedding_spec, "canonical")
                        .map_err(StreamingIndexerError::CanonicalEmbeddingFailure)?;
                    let descendant_count = child_summaries
                        .iter()
                        .map(|child| child.descendant_count)
                        .sum();
                    Ok((
                        index,
                        branch_block,
                        serialized.hash,
                        canonical,
                        descendant_count,
                    ))
                })()
            })
            .collect::<Vec<_>>();
        let prepared = prepared.into_iter().collect::<Result<Vec<_>, _>>()?;
        let results = futures::stream::iter(prepared)
            .map(
                |(index, branch_block, expected_hash, embedding, descendant_count)| async move {
                    let result: Result<(IndexedChild, BlockHash), StreamingIndexerError> = async {
                        let block_id = store
                            .put(&branch_block)
                            .await
                            .map_err(StreamingIndexerError::Storage)?;
                        verify_persisted_block_id(block_id, expected_hash)?;
                        Ok((
                            IndexedChild {
                                embedding,
                                child: block_id,
                                level: parent_level,
                                descendant_count,
                            },
                            block_id,
                        ))
                    }
                    .await;
                    (index, result)
                },
            )
            .buffer_unordered(V3_IO_QUEUE_DEPTH)
            .collect::<Vec<_>>()
            .await;
        let mut results = results;
        results.sort_by_key(|(index, _)| *index);
        let mut next_layer = Vec::with_capacity(results.len());
        let mut persisted_ids = Vec::with_capacity(results.len());
        for result in results {
            let (_, result) = result;
            let (child, block_id) = result?;
            next_layer.push(child);
            persisted_ids.push(block_id);
            progress.fetch_add(1, AtomicOrdering::Relaxed);
        }
        Ok((next_layer, persisted_ids))
    }

    async fn load_leaf_batch(
        &self,
        block_ids: &[BlockHash],
        phase: StreamingIndexingPhase,
        source_store: &dyn BlockStore,
        emit_partition_status: bool,
    ) -> Result<Vec<LoadedLeaf>, StreamingIndexerError> {
        let started = Instant::now();
        if emit_partition_status {
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
        }
        let progress = Arc::new(AtomicUsize::new(0));
        if emit_partition_status {
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
        }
        let mut heartbeat = emit_partition_status.then(|| {
            crate::StatusHeartbeatGuard::new(start_status_heartbeat(
                &self.observer,
                phase.clone(),
                Some(block_ids.len()),
                Arc::clone(&progress),
                Some(block_ids.len()),
                started,
            ))
        });
        let result = async {
            let mut loaded = Vec::with_capacity(block_ids.len());
            #[cfg(test)]
            let recorded_inner_pool_worker = Arc::new(AtomicBool::new(false));
            for batch in block_ids.chunks(V3_BATCH_SIZE) {
                self.check_cancelled_for_phase(&phase)?;
                let ordered = load_leaf_blocks_raw(batch, source_store).await?;
                #[cfg(test)]
                let recorded_inner_pool_worker = Arc::clone(&recorded_inner_pool_worker);
                let decoded = self.inner_pool.install(|| {
                    ordered
                        .into_par_iter()
                        .map(|(block_id, block)| {
                            #[cfg(test)]
                            if !recorded_inner_pool_worker.swap(true, AtomicOrdering::Relaxed) {
                                self.record_inner_pool_worker();
                            }
                            decode_loaded_leaf(block_id, block, &self.embedding_spec)
                        })
                        .collect::<Vec<_>>()
                });
                for leaf in decoded {
                    loaded.push(leaf?);
                    progress.fetch_add(1, AtomicOrdering::Relaxed);
                }
            }
            Ok::<Vec<LoadedLeaf>, StreamingIndexerError>(loaded)
        }
        .await;
        if let Some(heartbeat) = heartbeat.as_mut() {
            heartbeat.stop();
        }
        match result {
            Ok(loaded) => {
                if emit_partition_status {
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
                }
                Ok(loaded)
            }
            Err(error) => {
                if emit_partition_status {
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
                }
                Err(error)
            }
        }
    }

    fn ingest_leaf_training_partition_batches(
        &self,
        partition: &WorkingPartition,
        source_store: &dyn BlockStore,
        trainer: &mut DirectionalPcaStreamingTrainer,
        emit_partition_status: bool,
    ) -> Result<(), StreamingIndexerError> {
        let phase = StreamingIndexingPhase::V3PartitionTrainIngest {
            layer_index: partition.layer_index,
        };
        self.run_v3_partition_phase(
            phase.clone(),
            partition.item_count,
            emit_partition_status,
            |progress| {
                let mut reader =
                    BlockHashPartitionReader::open(self.partition_store(), &partition.id)?;
                let embedding_spec = self.embedding_spec.clone();
                let inner_pool = Arc::clone(&self.inner_pool);
                let producer_pool = Arc::clone(&inner_pool);
                let produce_phase = phase.clone();
                let consume_phase = phase.clone();
                run_prepared_batch_pipeline(
                    V3_PREPARED_BATCH_LOOKAHEAD,
                    move |sender| {
                        let runtime = build_v3_prepare_runtime()?;
                        while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
                            self.check_cancelled_for_phase(&produce_phase)?;
                            let prepared = runtime.block_on(prepare_leaf_training_batch(
                                batch,
                                source_store,
                                &embedding_spec,
                                producer_pool.as_ref(),
                                None,
                            ))?;
                            if sender.send(Ok(prepared)).is_err() {
                                return Ok(());
                            }
                        }
                        Ok(())
                    },
                    |prepared| {
                        self.check_cancelled_for_phase(&consume_phase)?;
                        let batch_len = prepared.len();
                        inner_pool
                            .install(|| trainer.ingest_batch(prepared.as_slice()))
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
        emit_partition_status: bool,
    ) -> Result<(), StreamingIndexerError> {
        let phase = StreamingIndexingPhase::V3PartitionTrainIngest {
            layer_index: partition.layer_index,
        };
        self.run_v3_partition_phase(
            phase.clone(),
            partition.item_count,
            emit_partition_status,
            |progress| {
                let mut reader =
                    IndexedChildPartitionReader::open(self.partition_store(), &partition.id)?;
                let embedding_spec = self.embedding_spec.clone();
                let inner_pool = Arc::clone(&self.inner_pool);
                let producer_pool = Arc::clone(&inner_pool);
                let produce_phase = phase.clone();
                let consume_phase = phase.clone();
                run_prepared_batch_pipeline(
                    V3_PREPARED_BATCH_LOOKAHEAD,
                    move |sender| {
                        while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
                            self.check_cancelled_for_phase(&produce_phase)?;
                            let prepared = prepare_summary_training_batch(
                                batch,
                                &embedding_spec,
                                producer_pool.as_ref(),
                                None,
                            )?;
                            if sender.send(Ok(prepared)).is_err() {
                                return Ok(());
                            }
                        }
                        Ok(())
                    },
                    |prepared| {
                        self.check_cancelled_for_phase(&consume_phase)?;
                        let batch_len = prepared.len();
                        inner_pool
                            .install(|| trainer.ingest_batch(prepared.as_slice()))
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
        classifier: &(impl StreamingClusterClassifier + Sync),
        writers: &mut BlockHashPartitionWriters,
        child_item_counts: &mut [usize],
        emit_partition_status: bool,
    ) -> Result<(), StreamingIndexerError> {
        let phase = StreamingIndexingPhase::V3PartitionClassify {
            layer_index: partition.layer_index,
        };
        self.run_v3_partition_phase(
            phase.clone(),
            partition.item_count,
            emit_partition_status,
            |progress| {
                let mut reader =
                    BlockHashPartitionReader::open(self.partition_store(), &partition.id)?;
                let embedding_spec = self.embedding_spec.clone();
                let inner_pool = Arc::clone(&self.inner_pool);
                let producer_pool = Arc::clone(&inner_pool);
                let produce_phase = phase.clone();
                let consume_phase = phase.clone();
                run_prepared_batch_pipeline(
                    V3_PREPARED_BATCH_LOOKAHEAD,
                    move |sender| {
                        let runtime = build_v3_prepare_runtime()?;
                        while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
                            self.check_cancelled_for_phase(&produce_phase)?;
                            let prepared = runtime.block_on(prepare_leaf_assignment_batch(
                                batch,
                                source_store,
                                &embedding_spec,
                                producer_pool.as_ref(),
                                None,
                            ))?;
                            if sender.send(Ok(prepared)).is_err() {
                                return Ok(());
                            }
                        }
                        Ok(())
                    },
                    |prepared| {
                        self.check_cancelled_for_phase(&consume_phase)?;
                        let assignments = inner_pool
                            .install(|| classifier.assign_batch(prepared.embeddings.as_slice()))
                            .map_err(map_clustering_error)?;
                        let batch_len = prepared.block_ids.len();
                        let mut grouped =
                            grouped_partition_buffers::<BlockHash>(writers.len(), batch_len);
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
                        writers.write_batch(grouped.as_slice())?;
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
        classifier: &(impl StreamingClusterClassifier + Sync),
        writers: &mut IndexedChildPartitionWriters,
        child_item_counts: &mut [usize],
        emit_partition_status: bool,
    ) -> Result<(), StreamingIndexerError> {
        let phase = StreamingIndexingPhase::V3PartitionClassify {
            layer_index: partition.layer_index,
        };
        self.run_v3_partition_phase(
            phase.clone(),
            partition.item_count,
            emit_partition_status,
            |progress| {
                let mut reader =
                    IndexedChildPartitionReader::open(self.partition_store(), &partition.id)?;
                let embedding_spec = self.embedding_spec.clone();
                let inner_pool = Arc::clone(&self.inner_pool);
                let producer_pool = Arc::clone(&inner_pool);
                let produce_phase = phase.clone();
                let consume_phase = phase.clone();
                run_prepared_batch_pipeline(
                    V3_PREPARED_BATCH_LOOKAHEAD,
                    move |sender| {
                        while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
                            self.check_cancelled_for_phase(&produce_phase)?;
                            let prepared = prepare_summary_assignment_batch(
                                batch,
                                &embedding_spec,
                                producer_pool.as_ref(),
                                None,
                            )?;
                            if sender.send(Ok(prepared)).is_err() {
                                return Ok(());
                            }
                        }
                        Ok(())
                    },
                    |prepared| {
                        self.check_cancelled_for_phase(&consume_phase)?;
                        let PreparedIndexedChildAssignmentBatch {
                            children,
                            embeddings,
                        } = prepared;
                        let assignments = inner_pool
                            .install(|| classifier.assign_batch(embeddings.as_slice()))
                            .map_err(map_clustering_error)?;
                        let batch_len = children.len();
                        let mut grouped =
                            grouped_partition_buffers::<IndexedChild>(writers.len(), batch_len);
                        for (child, assignment) in children.into_iter().zip(assignments) {
                            let cluster = usize::try_from(assignment).map_err(|_| {
                                StreamingIndexerError::HierarchyValidation(
                                    "v3 cluster id does not fit usize".into(),
                                )
                            })?;
                            let target = validate_v3_cluster_assignment(cluster, writers.len())?;
                            grouped[target].push(child);
                            child_item_counts[target] += 1;
                        }
                        writers.write_batch(grouped.as_slice())?;
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
        emit_partition_status: bool,
        operation: impl FnOnce(Arc<AtomicUsize>) -> Result<T, StreamingIndexerError>,
    ) -> Result<T, StreamingIndexerError> {
        let started = Instant::now();
        if emit_partition_status {
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
        }
        let progress = Arc::new(AtomicUsize::new(0));
        if emit_partition_status {
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
        }
        let mut heartbeat = emit_partition_status.then(|| {
            crate::StatusHeartbeatGuard::new(start_status_heartbeat(
                &self.observer,
                phase.clone(),
                Some(total_items),
                Arc::clone(&progress),
                Some(total_items),
                started,
            ))
        });
        let result = self
            .check_cancelled_for_phase(&phase)
            .and_then(|()| operation(Arc::clone(&progress)));
        if let Some(heartbeat) = heartbeat.as_mut() {
            heartbeat.stop();
        }
        match result {
            Ok(value) => {
                let completed = progress.load(AtomicOrdering::Relaxed);
                if emit_partition_status {
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
                }
                Ok(value)
            }
            Err(error) => {
                if emit_partition_status {
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
                }
                Err(error)
            }
        }
    }

    fn emit_aggregate_v3_phase(
        &self,
        phase: StreamingIndexingPhase,
        state: StreamingIndexingStatusState,
        total: usize,
        completed: usize,
        started: Instant,
        error: Option<String>,
    ) {
        emit_status(
            &self.observer,
            status_with_known_total(phase, state, total, completed, started.elapsed(), error),
        );
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

    fn partition_store(&self) -> &V3PartitionStore {
        self.partition_store
            .as_deref()
            .expect("v3 partition store should exist until success")
    }

    fn check_cancelled_mut(&mut self, context: &str) -> Result<(), StreamingIndexerError> {
        if self.cancellation.is_cancelled() {
            self.phase = V3Phase::Cancelled;
            return Err(StreamingIndexerError::Cancelled(format!(
                "caller requested cancellation during {context}"
            )));
        }
        Ok(())
    }

    fn record_terminal_error(&mut self, error: StreamingIndexerError) -> StreamingIndexerError {
        if matches!(error, StreamingIndexerError::Cancelled(_)) {
            self.phase = V3Phase::Cancelled;
        }
        error
    }

    #[cfg(test)]
    fn record_inner_pool_worker(&self) {
        let worker_name = thread::current().name().unwrap_or_default().to_owned();
        self.inner_pool_worker_names
            .lock()
            .unwrap()
            .push(worker_name);
    }
}

fn v3_phase_description(phase: &StreamingIndexingPhase) -> &'static str {
    match phase {
        StreamingIndexingPhase::PlanningPass { .. } => "planning pass",
        StreamingIndexingPhase::HierarchyPlanning { .. } => "partition planning",
        StreamingIndexingPhase::V3PartitionLoad { .. } => "partition load",
        StreamingIndexingPhase::V3PartitionTrainIngest { .. } => "partition trainer ingest",
        StreamingIndexingPhase::V3PartitionClassify { .. } => "partition classification",
        StreamingIndexingPhase::V3TerminalMaterializationLoad { .. } => {
            "terminal materialization load"
        }
        StreamingIndexingPhase::FinalMaterializationReplay => "final materialization replay",
        StreamingIndexingPhase::BottomUpAssembly { .. } => "bottom-up assembly",
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

#[derive(Debug)]
enum V3ReplayError {
    Indexing(StreamingIndexerError),
    Clustering(StreamingClusteringError),
}

fn run_v3_replay_until_ready<T>(
    trainer: &mut T,
    partition_id: &str,
    item_count: usize,
    mut check_cancelled: impl FnMut() -> Result<(), StreamingIndexerError>,
    mut replay_partition: impl FnMut(&mut T) -> Result<(), StreamingIndexerError>,
) -> Result<(), V3ReplayError>
where
    T: StreamingClusterTrainer,
{
    let max_passes = v3_replay_pass_limit(item_count);
    let mut replay_passes = 0usize;
    loop {
        check_cancelled().map_err(V3ReplayError::Indexing)?;
        replay_passes += 1;
        if replay_passes > max_passes {
            return Err(V3ReplayError::Indexing(
                StreamingIndexerError::ClusteringFailure(format!(
                    "v3 partition {partition_id:?} with {item_count} items exceeded the maximum replay pass count of {max_passes}"
                )),
            ));
        }
        replay_partition(trainer).map_err(V3ReplayError::Indexing)?;
        check_cancelled().map_err(V3ReplayError::Indexing)?;
        let pass_report = trainer.finish_pass().map_err(V3ReplayError::Clustering)?;
        if pass_report.observed_count != item_count {
            return Err(V3ReplayError::Indexing(
                StreamingIndexerError::HierarchyValidation(format!(
                    "v3 partition {partition_id:?} observed {} items but expected {item_count}",
                    pass_report.observed_count,
                )),
            ));
        }
        if pass_report.readiness == PassReadiness::AnalysisOnly {
            continue;
        }
        match trainer.complete_training() {
            Ok(()) => return Ok(()),
            Err(StreamingClusteringError::InvalidTransition { state, operation })
                if state == TrainerState::PassComplete && operation == "complete_training" =>
            {
                continue;
            }
            Err(error) => return Err(V3ReplayError::Clustering(error)),
        }
    }
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

fn build_v3_inner_pool() -> Result<Arc<ThreadPool>, StreamingIndexerError> {
    let thread_count = thread::available_parallelism()
        .map_err(|error| {
            StreamingIndexerError::ClusteringFailure(format!(
                "could not determine v3 inner Rayon pool size: {error}"
            ))
        })?
        .get();
    ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .thread_name(|index| format!("{V3_INNER_POOL_THREAD_NAME}-{index}"))
        .build()
        .map(Arc::new)
        .map_err(|error| {
            StreamingIndexerError::ClusteringFailure(format!(
                "could not initialize v3 inner Rayon pool: {error}"
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
    inner_pool: &ThreadPool,
    progress: Option<&Arc<AtomicUsize>>,
) -> Result<Vec<Vec<f32>>, StreamingIndexerError> {
    let ordered = load_leaf_blocks_raw(block_ids, source_store).await?;
    let decoded = inner_pool.install(|| {
        ordered
            .into_par_iter()
            .map(|(block_id, block)| decode_leaf_embedding_f32(block_id, block, embedding_spec))
            .collect::<Vec<_>>()
    });
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
    inner_pool: &ThreadPool,
    progress: Option<&Arc<AtomicUsize>>,
) -> Result<Vec<Vec<f32>>, StreamingIndexerError> {
    load_leaf_batch_raw(
        block_ids.as_slice(),
        source_store,
        embedding_spec,
        inner_pool,
        progress,
    )
    .await
}

async fn prepare_leaf_assignment_batch(
    block_ids: Vec<BlockHash>,
    source_store: &dyn BlockStore,
    embedding_spec: &EmbeddingSpec,
    inner_pool: &ThreadPool,
    progress: Option<&Arc<AtomicUsize>>,
) -> Result<PreparedLeafAssignmentBatch, StreamingIndexerError> {
    let embeddings = load_leaf_batch_raw(
        block_ids.as_slice(),
        source_store,
        embedding_spec,
        inner_pool,
        progress,
    )
    .await?;
    Ok(PreparedLeafAssignmentBatch {
        block_ids,
        embeddings,
    })
}

fn prepare_summary_training_batch(
    batch: Vec<IndexedChild>,
    embedding_spec: &EmbeddingSpec,
    inner_pool: &ThreadPool,
    progress: Option<&Arc<AtomicUsize>>,
) -> Result<Vec<Vec<f32>>, StreamingIndexerError> {
    decode_summary_embeddings(batch.as_slice(), embedding_spec, inner_pool, progress)
}

fn prepare_summary_assignment_batch(
    children: Vec<IndexedChild>,
    embedding_spec: &EmbeddingSpec,
    inner_pool: &ThreadPool,
    progress: Option<&Arc<AtomicUsize>>,
) -> Result<PreparedIndexedChildAssignmentBatch, StreamingIndexerError> {
    let embeddings =
        decode_summary_embeddings(children.as_slice(), embedding_spec, inner_pool, progress)?;
    Ok(PreparedIndexedChildAssignmentBatch {
        children,
        embeddings,
    })
}

fn decode_summary_embeddings(
    children: &[IndexedChild],
    embedding_spec: &EmbeddingSpec,
    inner_pool: &ThreadPool,
    progress: Option<&Arc<AtomicUsize>>,
) -> Result<Vec<Vec<f32>>, StreamingIndexerError> {
    let embeddings = inner_pool.install(|| {
        children
            .par_iter()
            .map(|child| decode_embedding_as_f32(child.embedding.as_slice(), embedding_spec))
            .collect::<Vec<_>>()
    });
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
        let mut write_txn = database.begin_write().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start v3 partition database initialization for {}: {error}",
                database_path.display()
            ))
        })?;
        write_txn.set_durability(Durability::None);
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
        let mut write_txn = self.database.begin_write().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start a v3 block-id partition write in {}: {error}",
                self.database_path.display()
            ))
        })?;
        write_txn.set_durability(Durability::None);
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
            let mut key = partition_entry_key_buffer(partition_id);
            for (offset, block_id) in block_ids.iter().enumerate() {
                let entry_index = checked_partition_entry_index(partition_id, start_index, offset)?;
                set_partition_entry_key_index(&mut key, partition_id, entry_index)?;
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
        let next_count = checked_partition_entry_index(partition_id, start_index, block_ids.len())?;
        self.set_partition_count_in_write_txn(
            &write_txn,
            partition_id,
            WorkingItemKind::LeafBlockIds,
            next_count,
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
        let mut write_txn = self.database.begin_write().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start a v3 summary partition write in {}: {error}",
                self.database_path.display()
            ))
        })?;
        write_txn.set_durability(Durability::None);
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
            let mut key = partition_entry_key_buffer(partition_id);
            for (offset, child) in children.iter().enumerate() {
                let entry_index = checked_partition_entry_index(partition_id, start_index, offset)?;
                set_partition_entry_key_index(&mut key, partition_id, entry_index)?;
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
        let next_count = checked_partition_entry_index(partition_id, start_index, children.len())?;
        self.set_partition_count_in_write_txn(
            &write_txn,
            partition_id,
            WorkingItemKind::IndexedChildren,
            next_count,
        )?;
        write_txn.commit().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not commit v3 summary partition data for {} in {}: {error}",
                partition_id,
                self.database_path.display()
            ))
        })
    }

    fn append_block_hash_groups(
        &self,
        partition_ids: &[String],
        groups: &[Vec<BlockHash>],
    ) -> Result<(), StreamingIndexerError> {
        if partition_ids.len() != groups.len() {
            return Err(StreamingIndexerError::LocalSpill(
                "v3 block-id partition append groups length mismatch".into(),
            ));
        }
        if groups.iter().all(Vec::is_empty) {
            return Ok(());
        }
        let mut write_txn = self.database.begin_write().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start a grouped v3 block-id partition write in {}: {error}",
                self.database_path.display()
            ))
        })?;
        write_txn.set_durability(Durability::None);
        {
            let mut table = write_txn
                .open_table(V3_BLOCK_HASH_PARTITIONS_TABLE)
                .map_err(|error| {
                    StreamingIndexerError::LocalSpill(format!(
                        "could not open the v3 block-id partition table in {}: {error}",
                        self.database_path.display()
                    ))
                })?;
            for (partition_id, block_ids) in partition_ids.iter().zip(groups) {
                if block_ids.is_empty() {
                    continue;
                }
                let start_index = self.partition_count_from_write_txn(
                    &write_txn,
                    partition_id,
                    WorkingItemKind::LeafBlockIds,
                )?;
                let mut key = partition_entry_key_buffer(partition_id);
                for (offset, block_id) in block_ids.iter().enumerate() {
                    let entry_index =
                        checked_partition_entry_index(partition_id, start_index, offset)?;
                    set_partition_entry_key_index(&mut key, partition_id, entry_index)?;
                    table
                        .insert(key.as_slice(), &block_id.as_bytes()[..])
                        .map_err(|error| {
                            StreamingIndexerError::LocalSpill(format!(
                                "could not append grouped block-id partition data for {} in {}: {error}",
                                partition_id,
                                self.database_path.display()
                            ))
                        })?;
                }
                let next_count =
                    checked_partition_entry_index(partition_id, start_index, block_ids.len())?;
                self.set_partition_count_in_write_txn(
                    &write_txn,
                    partition_id,
                    WorkingItemKind::LeafBlockIds,
                    next_count,
                )?;
            }
        }
        write_txn.commit().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not commit grouped v3 block-id partition data in {}: {error}",
                self.database_path.display()
            ))
        })
    }

    fn append_indexed_child_groups(
        &self,
        partition_ids: &[String],
        groups: &[Vec<IndexedChild>],
    ) -> Result<(), StreamingIndexerError> {
        if partition_ids.len() != groups.len() {
            return Err(StreamingIndexerError::LocalSpill(
                "v3 summary partition append groups length mismatch".into(),
            ));
        }
        if groups.iter().all(Vec::is_empty) {
            return Ok(());
        }
        let mut write_txn = self.database.begin_write().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start a grouped v3 summary partition write in {}: {error}",
                self.database_path.display()
            ))
        })?;
        write_txn.set_durability(Durability::None);
        {
            let mut table = write_txn
                .open_table(V3_INDEXED_CHILD_PARTITIONS_TABLE)
                .map_err(|error| {
                    StreamingIndexerError::LocalSpill(format!(
                        "could not open the v3 summary partition table in {}: {error}",
                        self.database_path.display()
                    ))
                })?;
            for (partition_id, children) in partition_ids.iter().zip(groups) {
                if children.is_empty() {
                    continue;
                }
                let start_index = self.partition_count_from_write_txn(
                    &write_txn,
                    partition_id,
                    WorkingItemKind::IndexedChildren,
                )?;
                let mut key = partition_entry_key_buffer(partition_id);
                for (offset, child) in children.iter().enumerate() {
                    let entry_index =
                        checked_partition_entry_index(partition_id, start_index, offset)?;
                    set_partition_entry_key_index(&mut key, partition_id, entry_index)?;
                    let bytes = serialize_spilled_indexed_child_bytes(child)?;
                    table
                        .insert(key.as_slice(), bytes.as_slice())
                        .map_err(|error| {
                            StreamingIndexerError::LocalSpill(format!(
                                "could not append grouped summary partition data for {} in {}: {error}",
                                partition_id,
                                self.database_path.display()
                            ))
                        })?;
                }
                let next_count =
                    checked_partition_entry_index(partition_id, start_index, children.len())?;
                self.set_partition_count_in_write_txn(
                    &write_txn,
                    partition_id,
                    WorkingItemKind::IndexedChildren,
                    next_count,
                )?;
            }
        }
        write_txn.commit().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not commit grouped v3 summary partition data in {}: {error}",
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
        let mut write_txn = self.database.begin_write().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start a v3 partition cleanup in {}: {error}",
                self.database_path.display()
            ))
        })?;
        write_txn.set_durability(Durability::None);
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

    fn partition_count_from_read_txn(
        &self,
        read_txn: &redb::ReadTransaction,
        partition_id: &str,
        kind: WorkingItemKind,
    ) -> Result<usize, StreamingIndexerError> {
        let counts = read_txn
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
        match kind {
            WorkingItemKind::LeafBlockIds => {
                let mut table = write_txn
                    .open_table(V3_BLOCK_HASH_PARTITIONS_TABLE)
                    .map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not open the v3 block-id partition table in {}: {error}",
                            self.database_path.display()
                        ))
                    })?;
                let extracted = table
                    .extract_from_if(start_key.as_slice()..end_key.as_slice(), |_, _| true)
                    .map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not start block-id partition cleanup for {} in {}: {error}",
                            partition_id,
                            self.database_path.display()
                        ))
                    })?;
                for entry in extracted {
                    entry.map_err(|error| {
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
                            "could not open the v3 summary partition table in {}: {error}",
                            self.database_path.display()
                        ))
                    })?;
                let extracted = table
                    .extract_from_if(start_key.as_slice()..end_key.as_slice(), |_, _| true)
                    .map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not start summary partition cleanup for {} in {}: {error}",
                            partition_id,
                            self.database_path.display()
                        ))
                    })?;
                for entry in extracted {
                    entry.map_err(|error| {
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

struct BlockHashPartitionReader {
    database_path: PathBuf,
    table: ReadOnlyTable<&'static [u8], &'static [u8]>,
    partition_id: String,
    count: usize,
    next_index: usize,
}

impl BlockHashPartitionReader {
    fn open(store: &V3PartitionStore, partition_id: &str) -> Result<Self, StreamingIndexerError> {
        let read_txn = store.database.begin_read().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start a v3 block-id partition read in {}: {error}",
                store.database_path.display()
            ))
        })?;
        let table = read_txn
            .open_table(V3_BLOCK_HASH_PARTITIONS_TABLE)
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not open the v3 block-id partition table in {}: {error}",
                    store.database_path.display()
                ))
            })?;
        let count = store.partition_count_from_read_txn(
            &read_txn,
            partition_id,
            WorkingItemKind::LeafBlockIds,
        )?;
        Ok(Self {
            database_path: store.database_path.clone(),
            table,
            partition_id: partition_id.into(),
            count,
            next_index: 0,
        })
    }

    fn next_batch(
        &mut self,
        batch_size: usize,
    ) -> Result<Option<Vec<BlockHash>>, StreamingIndexerError> {
        if self.next_index >= self.count {
            return Ok(None);
        }
        let start_key = partition_entry_key(&self.partition_id, self.next_index)?;
        let end_key = partition_entry_key_end(&self.partition_id);
        let limit = (self.count - self.next_index).min(batch_size);
        let mut batch = Vec::with_capacity(limit);
        let entries = self
            .table
            .range(start_key.as_slice()..end_key.as_slice())
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not iterate block-id partition {} in {}: {error}",
                    self.partition_id,
                    self.database_path.display()
                ))
            })?;
        for entry in entries.take(limit) {
            let (_, value) = entry.map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not read block-id partition {} in {}: {error}",
                    self.partition_id,
                    self.database_path.display()
                ))
            })?;
            let bytes = value.value();
            if bytes.len() != BlockHash::LEN {
                return Err(StreamingIndexerError::LocalSpill(format!(
                    "truncated v3 block-id partition entry for {} in {}",
                    self.partition_id,
                    self.database_path.display()
                )));
            }
            let mut raw = [0u8; BlockHash::LEN];
            raw.copy_from_slice(bytes);
            batch.push(BlockHash::from_bytes(raw));
        }
        self.next_index += batch.len();
        Ok(Some(batch))
    }
}

struct IndexedChildPartitionReader {
    database_path: PathBuf,
    table: ReadOnlyTable<&'static [u8], &'static [u8]>,
    partition_id: String,
    count: usize,
    next_index: usize,
}

impl IndexedChildPartitionReader {
    fn open(store: &V3PartitionStore, partition_id: &str) -> Result<Self, StreamingIndexerError> {
        let read_txn = store.database.begin_read().map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not start a v3 summary partition read in {}: {error}",
                store.database_path.display()
            ))
        })?;
        let table = read_txn
            .open_table(V3_INDEXED_CHILD_PARTITIONS_TABLE)
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not open the v3 summary partition table in {}: {error}",
                    store.database_path.display()
                ))
            })?;
        let count = store.partition_count_from_read_txn(
            &read_txn,
            partition_id,
            WorkingItemKind::IndexedChildren,
        )?;
        Ok(Self {
            database_path: store.database_path.clone(),
            table,
            partition_id: partition_id.into(),
            count,
            next_index: 0,
        })
    }

    fn next_batch(
        &mut self,
        batch_size: usize,
    ) -> Result<Option<Vec<IndexedChild>>, StreamingIndexerError> {
        if self.next_index >= self.count {
            return Ok(None);
        }
        let start_key = partition_entry_key(&self.partition_id, self.next_index)?;
        let end_key = partition_entry_key_end(&self.partition_id);
        let limit = (self.count - self.next_index).min(batch_size);
        let mut batch = Vec::with_capacity(limit);
        let entries = self
            .table
            .range(start_key.as_slice()..end_key.as_slice())
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not iterate summary partition {} in {}: {error}",
                    self.partition_id,
                    self.database_path.display()
                ))
            })?;
        for entry in entries.take(limit) {
            let (_, value) = entry.map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not read summary partition {} in {}: {error}",
                    self.partition_id,
                    self.database_path.display()
                ))
            })?;
            batch.push(deserialize_spilled_indexed_child_bytes(value.value())?);
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

    fn write_batch(&mut self, groups: &[Vec<BlockHash>]) -> Result<(), StreamingIndexerError> {
        self.store
            .append_block_hash_groups(&self.partition_ids, groups)
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

    fn write_batch(&mut self, groups: &[Vec<IndexedChild>]) -> Result<(), StreamingIndexerError> {
        self.store
            .append_indexed_child_groups(&self.partition_ids, groups)
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
    let mut reader = BlockHashPartitionReader::open(store, partition_id)?;
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
    cancellation: Option<&StreamingIndexingCancellationHandle>,
    phase: Option<&StreamingIndexingPhase>,
) -> Result<Vec<IndexedChild>, StreamingIndexerError> {
    let mut reader = IndexedChildPartitionReader::open(store, partition_id)?;
    let mut all = Vec::new();
    while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
        if let (Some(cancellation), Some(phase)) = (cancellation, phase)
            && cancellation.is_cancelled()
        {
            return Err(StreamingIndexerError::Cancelled(format!(
                "caller requested cancellation during {}",
                v3_phase_description(phase)
            )));
        }
        if let Some(progress) = progress {
            progress.fetch_add(batch.len(), AtomicOrdering::Relaxed);
        }
        all.extend(batch);
    }
    Ok(all)
}
fn grouped_partition_buffers<T>(group_count: usize, batch_len: usize) -> Vec<Vec<T>> {
    if group_count == 0 {
        return Vec::new();
    }
    let per_group_capacity = batch_len.div_ceil(group_count);
    std::iter::repeat_with(|| Vec::with_capacity(per_group_capacity))
        .take(group_count)
        .collect()
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
    let mut reader = BlockHashPartitionReader::open(store, source_partition_id)?;
    let mut writers = BlockHashPartitionWriters::create(store, destination_partition_ids)?;
    let mut item_index = 0usize;
    while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
        let mut grouped = grouped_partition_buffers::<BlockHash>(writers.len(), batch.len());
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
        writers.write_batch(grouped.as_slice())?;
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
    let mut reader = IndexedChildPartitionReader::open(store, source_partition_id)?;
    let mut writers = IndexedChildPartitionWriters::create(store, destination_partition_ids)?;
    let mut item_index = 0usize;
    while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
        let mut grouped = grouped_partition_buffers::<IndexedChild>(writers.len(), batch.len());
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
        writers.write_batch(grouped.as_slice())?;
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

fn partition_entry_key_buffer(partition_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(partition_id.len() + 1 + std::mem::size_of::<u64>());
    key.extend_from_slice(partition_id.as_bytes());
    key.push(0);
    key.extend_from_slice(&0u64.to_be_bytes());
    key
}

fn set_partition_entry_key_index(
    key: &mut [u8],
    partition_id: &str,
    index: usize,
) -> Result<(), StreamingIndexerError> {
    let index = u64::try_from(index).map_err(|_| {
        StreamingIndexerError::LocalSpill(format!(
            "v3 partition index for {} does not fit u64",
            partition_id
        ))
    })?;
    let start = partition_id.len() + 1;
    key[start..].copy_from_slice(&index.to_be_bytes());
    Ok(())
}

fn partition_entry_key(partition_id: &str, index: usize) -> Result<Vec<u8>, StreamingIndexerError> {
    let mut key = partition_entry_key_buffer(partition_id);
    set_partition_entry_key_index(&mut key, partition_id, index)?;
    Ok(key)
}

fn partition_entry_key_end(partition_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(partition_id.len() + 2);
    key.extend_from_slice(partition_id.as_bytes());
    key.push(1);
    key
}

fn checked_partition_entry_index(
    partition_id: &str,
    start_index: usize,
    delta: usize,
) -> Result<usize, StreamingIndexerError> {
    start_index.checked_add(delta).ok_or_else(|| {
        StreamingIndexerError::LocalSpill(format!(
            "v3 partition index for {} overflows usize",
            partition_id
        ))
    })
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
    let mut bytes = Vec::with_capacity(4 + child.embedding.len() + BlockHash::LEN + 8 + 8);
    write_spilled_indexed_child(&mut bytes, child)?;
    Ok(bytes)
}

fn deserialize_spilled_indexed_child_bytes(
    bytes: &[u8],
) -> Result<IndexedChild, StreamingIndexerError> {
    let minimum_trailer_len =
        BlockHash::LEN + std::mem::size_of::<u64>() + std::mem::size_of::<u64>();
    if bytes.len() < std::mem::size_of::<u32>() + minimum_trailer_len {
        return Err(StreamingIndexerError::LocalSpill(
            "spilled summary partition entry is malformed".into(),
        ));
    }
    let mut reader = Cursor::new(bytes);
    let mut embedding_len_bytes = [0u8; 4];
    reader
        .read_exact(&mut embedding_len_bytes)
        .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
    let embedding_len = usize::try_from(u32::from_le_bytes(embedding_len_bytes)).map_err(|_| {
        StreamingIndexerError::LocalSpill(
            "spilled summary embedding length does not fit usize".into(),
        )
    })?;
    let required_len = std::mem::size_of::<u32>()
        .checked_add(embedding_len)
        .and_then(|total| total.checked_add(minimum_trailer_len))
        .ok_or_else(|| {
            StreamingIndexerError::LocalSpill(
                "spilled summary embedding length overflows entry size".into(),
            )
        })?;
    if bytes.len() != required_len {
        return Err(StreamingIndexerError::LocalSpill(
            "spilled summary partition entry is malformed".into(),
        ));
    }
    let mut entry = Cursor::new(bytes);
    read_spilled_indexed_child(&mut entry)?.ok_or_else(|| {
        StreamingIndexerError::LocalSpill("spilled summary partition entry is missing".into())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::collections::VecDeque;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::stream;
    use lexongraph_block::{
        Block, BranchEntry, Content, LeafEntry, build_branch_block, build_leaf_block,
    };
    use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};
    use lexongraph_streaming_clustering::{MetricDirection, PassReport};
    use tokio::sync::Barrier;

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

    struct LayerBarrierBlockStore {
        blocks: Mutex<HashMap<BlockHash, Vec<u8>>>,
        first_gets: AtomicUsize,
        active_gets: AtomicUsize,
        max_active_gets: AtomicUsize,
        rendezvous: Barrier,
    }

    impl LayerBarrierBlockStore {
        fn new() -> Self {
            Self {
                blocks: Mutex::new(HashMap::new()),
                first_gets: AtomicUsize::new(0),
                active_gets: AtomicUsize::new(0),
                max_active_gets: AtomicUsize::new(0),
                rendezvous: Barrier::new(2),
            }
        }

        fn record_active_get(&self, active: usize) {
            self.max_active_gets.fetch_max(active, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl BlockStore for LayerBarrierBlockStore {
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
            let active = self.active_gets.fetch_add(1, Ordering::SeqCst) + 1;
            self.record_active_get(active);
            if self.first_gets.fetch_add(1, Ordering::SeqCst) < 2 {
                self.rendezvous.wait().await;
            }
            let block = self.blocks.lock().unwrap().get(block_id).cloned();
            self.active_gets.fetch_sub(1, Ordering::SeqCst);
            Ok(block)
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

    async fn store_leaf(store: &impl BlockStore, values: [f32; 2], body: &str) -> BlockHash {
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

    struct ScriptedClassifier {
        config: StreamingClusteringConfig,
    }

    impl StreamingClusterClassifier for ScriptedClassifier {
        fn config(&self) -> &StreamingClusteringConfig {
            &self.config
        }

        fn assign(&self, _embedding: &[f32]) -> Result<u32, StreamingClusteringError> {
            Ok(0)
        }
    }

    struct ScriptedReplayTrainer {
        config: StreamingClusteringConfig,
        readiness: VecDeque<PassReadiness>,
        state: TrainerState,
        observed_count: usize,
        complete_calls: usize,
    }

    impl ScriptedReplayTrainer {
        fn new(observed_count: usize, readiness: impl IntoIterator<Item = PassReadiness>) -> Self {
            Self {
                config: StreamingClusteringConfig {
                    cluster_count: 2,
                    dimensions: 2,
                    balance_constraints: None,
                    random_seed: Some(7),
                },
                readiness: readiness.into_iter().collect(),
                state: TrainerState::Idle,
                observed_count,
                complete_calls: 0,
            }
        }
    }

    impl StreamingClusterTrainer for ScriptedReplayTrainer {
        type Classifier = ScriptedClassifier;

        fn config(&self) -> &StreamingClusteringConfig {
            &self.config
        }

        fn state(&self) -> TrainerState {
            self.state
        }

        fn ingest_batch(
            &mut self,
            _embeddings: &[Vec<f32>],
        ) -> Result<(), StreamingClusteringError> {
            self.state = TrainerState::Ingesting;
            Ok(())
        }

        fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
            self.state = TrainerState::PassComplete;
            Ok(PassReport {
                observed_count: self.observed_count,
                requested_cluster_count: self.config.cluster_count,
                readiness: self
                    .readiness
                    .pop_front()
                    .expect("script must provide one readiness result per pass"),
                realized_cluster_count: Some(2),
                quality_metric: 0.0,
                balance_metric: 0.0,
                quality_direction: MetricDirection::SmallerIsBetter,
                balance_direction: MetricDirection::SmallerIsBetter,
                cluster_ids: Some(vec![0, 1]),
            })
        }

        fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
            self.complete_calls += 1;
            self.state = TrainerState::TrainingComplete;
            Ok(())
        }

        fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
            Ok(ScriptedClassifier {
                config: self.config,
            })
        }
    }

    #[test]
    fn v3_replay_decision_replays_analysis_only_until_partition_ready() {
        let mut trainer = ScriptedReplayTrainer::new(
            3,
            [PassReadiness::AnalysisOnly, PassReadiness::PartitionReady],
        );
        let mut replay_count = 0;

        run_v3_replay_until_ready(
            &mut trainer,
            "scripted",
            3,
            || Ok(()),
            |trainer| {
                replay_count += 1;
                trainer
                    .ingest_batch(&[vec![0.0, 1.0]])
                    .map_err(map_clustering_error)
            },
        )
        .unwrap();

        assert_eq!(replay_count, 2);
        assert_eq!(trainer.complete_calls, 1);
        assert_eq!(trainer.state(), TrainerState::TrainingComplete);
    }

    #[test]
    fn v3_replay_decision_reports_exhaustion_only_after_the_replay_bound() {
        let item_count = 3;
        let replay_limit = v3_replay_pass_limit(item_count);
        let mut trainer = ScriptedReplayTrainer::new(
            item_count,
            std::iter::repeat_n(PassReadiness::AnalysisOnly, replay_limit),
        );
        let mut replay_count = 0;

        let error = run_v3_replay_until_ready(
            &mut trainer,
            "scripted",
            item_count,
            || Ok(()),
            |trainer| {
                replay_count += 1;
                trainer
                    .ingest_batch(&[vec![0.0, 1.0]])
                    .map_err(map_clustering_error)
            },
        )
        .unwrap_err();

        assert_eq!(replay_count, replay_limit);
        assert!(matches!(
            error,
            V3ReplayError::Indexing(StreamingIndexerError::ClusteringFailure(message))
                if message.contains("maximum replay pass count")
        ));
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
    async fn v3_v0_8_0_uses_rank_zero_fallback() {
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
        let mut ids = Vec::with_capacity(128);
        for index in 0..128 {
            let values = if index % 2 == 0 {
                [0.0, 0.0]
            } else {
                [0.0007, 0.0]
            };
            ids.push(store_leaf(&source, values, &format!("item-{index}")).await);
        }
        let mut run = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_8_0,
            spec(),
            4096,
            parent.path(),
        )
        .unwrap()
        .with_observer(observer);
        run.ingest_block_id_batch(ids.as_slice()).await.unwrap();
        let result = run.finalize(&source, &output).await.unwrap();

        let repeat_output = MemoryBlockStore::default();
        let mut repeat = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_8_0,
            spec(),
            4096,
            parent.path(),
        )
        .unwrap();
        repeat.ingest_block_id_batch(ids.as_slice()).await.unwrap();
        let repeat_result = repeat.finalize(&source, &repeat_output).await.unwrap();
        assert_eq!(result.root_id, repeat_result.root_id);
        assert_eq!(result.block_ids, repeat_result.block_ids);

        let statuses = statuses.lock().unwrap();
        assert!(
            statuses
                .iter()
                .any(|status| status.fallback_count == Some(1))
        );
        assert!(statuses.iter().any(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::V3PartitionTrainIngest { layer_index: 0 }
            ) && status.state == StreamingIndexingStatusState::Completed
                && status.phase_total_unit_count == Some(128)
                && status.completed_unit_count == 128
        }));
        assert!(statuses.iter().any(|status| {
            matches!(
                status.phase,
                StreamingIndexingPhase::V3PartitionClassify { layer_index: 0 }
            ) && status.state == StreamingIndexingStatusState::Completed
                && status.phase_total_unit_count == Some(128)
                && status.completed_unit_count == 128
        }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v3_v0_8_0_preserves_non_rank_zero_failures() {
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
        let mut ids = Vec::with_capacity(128);
        for index in 0..128 {
            let values = if index == 0 {
                [f32::NAN, 0.0]
            } else {
                [index as f32, (index * 3) as f32]
            };
            ids.push(store_leaf(&source, values, &format!("item-{index}")).await);
        }
        let mut run = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_8_0,
            spec(),
            4096,
            parent.path(),
        )
        .unwrap()
        .with_observer(observer);
        run.ingest_block_id_batch(ids.as_slice()).await.unwrap();

        let error = run.finalize(&source, &output).await.unwrap_err();
        assert!(matches!(
            error,
            StreamingIndexerError::ClusteringFailure(message)
                if message.contains("embeddings must not contain NaN or infinite values")
        ));
        assert!(
            !statuses
                .lock()
                .unwrap()
                .iter()
                .any(|status| status.fallback_count == Some(1))
        );
    }

    #[test]
    fn v3_v0_8_0_rejects_impossible_fallback_materialization_bounds() {
        let minimum_two_child_size = crate::serialized_branch_size(&spec(), 2).unwrap();
        let parent = tempfile::tempdir().unwrap();

        let error = match StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_8_0,
            spec(),
            minimum_two_child_size - 1,
            parent.path(),
        ) {
            Ok(_) => panic!("an impossible two-child materialization bound must be rejected"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            StreamingIndexerError::ClusteringFailure(message)
                if message.contains("minimum 2-child branch serializes")
        ));
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
        run.partition_store()
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
        run.ingest_leaf_training_partition_batches(&partition, &source, &mut trainer, true)
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
            BlockHashPartitionWriters::create(run.partition_store(), child_ids.as_slice()).unwrap();
        let mut child_item_counts = vec![0usize; child_ids.len()];
        run.classify_leaf_partition_batches(
            &partition,
            &source,
            &classifier,
            &mut writers,
            child_item_counts.as_mut_slice(),
            true,
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
        run.partition_store()
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
            .split_partition(&partition, 1, &source, true)
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
        let mut write_txn = store.database.begin_write().unwrap();
        write_txn.set_durability(Durability::None);
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

        let mut reader = BlockHashPartitionReader::open(&store, "l0.p0").unwrap();
        let error = reader.next_batch(1).unwrap_err();
        assert!(matches!(
            error,
            StreamingIndexerError::LocalSpill(message)
                if message.contains("truncated v3 block-id partition entry")
        ));
    }

    #[test]
    fn v3_partition_reads_do_not_include_prefix_related_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let store = V3PartitionStore::new(dir.path()).unwrap();
        let parent_block = BlockHash::from_bytes([1u8; BlockHash::LEN]);
        let child_block = BlockHash::from_bytes([2u8; BlockHash::LEN]);

        store.append_block_hashes("l0.p0", &[parent_block]).unwrap();
        store
            .append_block_hashes("l0.p0.0", &[child_block])
            .unwrap();

        let parent = read_all_block_hashes(&store, "l0.p0").unwrap();
        let child = read_all_block_hashes(&store, "l0.p0.0").unwrap();

        assert_eq!(parent, vec![parent_block]);
        assert_eq!(child, vec![child_block]);
    }

    #[test]
    fn v3_clearing_partition_hides_stale_entries_from_later_reads() {
        let dir = tempfile::tempdir().unwrap();
        let store = V3PartitionStore::new(dir.path()).unwrap();
        let old_a = BlockHash::from_bytes([1u8; BlockHash::LEN]);
        let old_b = BlockHash::from_bytes([2u8; BlockHash::LEN]);
        let replacement = BlockHash::from_bytes([3u8; BlockHash::LEN]);

        store.append_block_hashes("l0.p0", &[old_a, old_b]).unwrap();
        store
            .clear_partitions(WorkingItemKind::LeafBlockIds, &["l0.p0".to_string()])
            .unwrap();
        store.append_block_hashes("l0.p0", &[replacement]).unwrap();

        let items = read_all_block_hashes(&store, "l0.p0").unwrap();
        assert_eq!(items, vec![replacement]);
    }

    #[test]
    fn v3_indexed_child_reader_rejects_malformed_entry_length_before_allocating_payload() {
        let bytes = 1_000_000u32.to_le_bytes();
        match deserialize_spilled_indexed_child_bytes(bytes.as_slice()) {
            Err(StreamingIndexerError::LocalSpill(message)) => {
                assert!(message.contains("spilled summary partition entry is malformed"));
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("expected malformed spilled summary partition entry"),
        }
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
    fn v3_layer_processing_accepts_concurrent_refinement_and_reconciles_at_a_barrier() {
        let parent = tempfile::tempdir().unwrap();
        let source = LayerBarrierBlockStore::new();
        let output = MemoryBlockStore::default();
        let runtime = build_v3_prepare_runtime().unwrap();
        let run = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_7_0,
            spec(),
            4096,
            parent.path(),
        )
        .unwrap();

        for partition_index in 0..2 {
            let mut ids = Vec::new();
            for item_index in 0..128 {
                ids.push(runtime.block_on(store_leaf(
                    &source,
                    [
                        (partition_index * 10 + item_index) as f32,
                        (item_index * item_index) as f32,
                    ],
                    &format!("p{partition_index}-{item_index}"),
                )));
            }
            run.partition_store()
                .append_block_hashes(&format!("l0.p{partition_index}"), ids.as_slice())
                .unwrap();
        }
        let active = (0..2)
            .map(|partition_index| WorkingPartition {
                id: format!("l0.p{partition_index}"),
                layer_index: 0,
                item_count: 128,
                kind: WorkingItemKind::LeafBlockIds,
            })
            .collect::<Vec<_>>();
        let outer_pool = ThreadPoolBuilder::new().num_threads(2).build().unwrap();
        let result = outer_pool.install(|| {
            runtime.block_on(run.process_layer_until_terminal(
                active,
                &source,
                &output,
                &mut Vec::new(),
            ))
        });

        let children = result.unwrap();
        assert!(!children.is_empty());
        assert!(source.max_active_gets.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v3_runtime_reuses_its_inner_pool_for_leaf_decode() {
        let parent = tempfile::tempdir().unwrap();
        let source = MemoryBlockStore::default();
        let ids = [
            store_leaf(&source, [0.0, 1.0], "a").await,
            store_leaf(&source, [1.0, 0.0], "b").await,
        ];
        let run = StreamingIndexingRunV3::with_published_profile(
            PUBLISHED_PROFILE_V0_7_0,
            spec(),
            4096,
            parent.path(),
        )
        .unwrap();
        let pool = Arc::clone(&run.inner_pool);

        run.load_leaf_batch(
            &ids,
            StreamingIndexingPhase::V3PartitionLoad { layer_index: 0 },
            &source,
            false,
        )
        .await
        .unwrap();
        run.load_leaf_batch(
            &ids,
            StreamingIndexingPhase::V3PartitionLoad { layer_index: 1 },
            &source,
            false,
        )
        .await
        .unwrap();

        assert!(Arc::ptr_eq(&pool, &run.inner_pool));
        let worker_names = run.inner_pool_worker_names.lock().unwrap();
        assert_eq!(worker_names.len(), 2);
        assert!(
            worker_names
                .iter()
                .all(|name| name.starts_with(V3_INNER_POOL_THREAD_NAME))
        );
    }

    #[test]
    fn v3_runtime_inner_pool_completes_when_outer_workers_wait_on_decode() {
        let parent = tempfile::tempdir().unwrap();
        let source = Arc::new(MemoryBlockStore::default());
        let runtime = build_v3_prepare_runtime().unwrap();
        let ids = vec![
            runtime.block_on(store_leaf(source.as_ref(), [0.0, 1.0], "a")),
            runtime.block_on(store_leaf(source.as_ref(), [1.0, 0.0], "b")),
        ];
        drop(runtime);
        let run = Arc::new(
            StreamingIndexingRunV3::with_published_profile(
                PUBLISHED_PROFILE_V0_7_0,
                spec(),
                4096,
                parent.path(),
            )
            .unwrap(),
        );
        let outer_pool = ThreadPoolBuilder::new().num_threads(2).build().unwrap();
        let (sender, receiver) = mpsc::channel();
        for layer_index in 0..2 {
            let run = Arc::clone(&run);
            let source = Arc::clone(&source);
            let ids = ids.clone();
            let sender = sender.clone();
            outer_pool.spawn(move || {
                let result = thread::spawn(move || {
                    build_v3_prepare_runtime().and_then(|runtime| {
                        runtime
                            .block_on(run.load_leaf_batch(
                                ids.as_slice(),
                                StreamingIndexingPhase::V3PartitionLoad { layer_index },
                                source.as_ref(),
                                false,
                            ))
                            .map(|_| ())
                    })
                })
                .join()
                .map_err(|panic| {
                    StreamingIndexerError::ClusteringFailure(format!(
                        "v3 decode worker panicked: {panic:?}"
                    ))
                })
                .and_then(|result| result);
                sender.send(result).unwrap();
            });
        }
        drop(sender);

        for _ in 0..2 {
            receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("outer workers waiting on v3 decode must not starve the inner pool")
                .unwrap();
        }
        assert!(
            run.inner_pool_worker_names
                .lock()
                .unwrap()
                .iter()
                .all(|name| name.starts_with(V3_INNER_POOL_THREAD_NAME))
        );
    }

    #[test]
    fn v3_inner_pool_uses_dedicated_named_workers() {
        let pool = build_v3_inner_pool().unwrap();
        assert_eq!(
            pool.current_num_threads(),
            thread::available_parallelism().unwrap().get()
        );
        let (sender, receiver) = mpsc::sync_channel(1);
        pool.spawn(move || {
            let worker_name = thread::current()
                .name()
                .expect("inner pool worker should have a name")
                .to_owned();
            sender.send(worker_name).unwrap();
        });
        let worker_name = receiver.recv().unwrap();
        assert!(worker_name.starts_with(V3_INNER_POOL_THREAD_NAME));
    }

    #[test]
    fn v3_inner_pool_runs_nested_parallel_work_on_inner_workers() {
        let pool = build_v3_inner_pool().unwrap();
        let (sender, receiver) = mpsc::sync_channel(1);
        pool.spawn(move || {
            let worker_names = (0..32)
                .into_par_iter()
                .map(|_| thread::current().name().unwrap_or_default().to_owned())
                .collect::<Vec<_>>();
            sender.send(worker_names).unwrap();
        });
        let worker_names = receiver.recv().unwrap();
        assert!(!worker_names.is_empty());
        assert!(
            worker_names
                .iter()
                .all(|name| name.starts_with(V3_INNER_POOL_THREAD_NAME))
        );
    }

    #[test]
    fn v3_outer_workers_can_wait_on_inner_pool_without_starvation() {
        let outer_pool = ThreadPoolBuilder::new()
            .num_threads(2)
            .thread_name(|index| format!("lexongraph-v3-outer-{index}"))
            .build()
            .unwrap();
        let inner_pool = build_v3_inner_pool().unwrap();
        outer_pool.scope(|scope| {
            for _ in 0..2 {
                let inner_pool = Arc::clone(&inner_pool);
                scope.spawn(move |_| {
                    let total = inner_pool.install(|| {
                        (0..128)
                            .into_par_iter()
                            .map(|value| value + 1)
                            .sum::<usize>()
                    });
                    assert_eq!(total, 8256);
                });
            }
        });
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
