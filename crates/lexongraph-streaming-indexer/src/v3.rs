// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use futures::StreamExt;
use rayon::prelude::*;
use tempfile::TempDir;

use crate::{
    Block, BlockHash, BranchEncodingPolicy, BranchEntry, ChildSummaryInput, ChildSummaryPolicy,
    EmbeddingSpec, ExactCentroidChildSummaryPolicy, HierarchyPlanningDetailFields, IndexedChild,
    LayerBuildStatus, PUBLISHED_PROFILE_V0_7_0, PlanningStage, PublishedBranchEncodingPolicy,
    PublishedDirectionalPcaProfileSettings, PublishedIndexingProfile, PublishedPlanningStrategy,
    PublishedProfileVersion, StreamingClusteringConfig, StreamingIndexerError,
    StreamingIndexingPhase, StreamingIndexingProgressUnitKind, StreamingIndexingResult,
    StreamingIndexingStatusObserver, StreamingIndexingStatusState, VERSION_1, balanced_groups,
    branch_encoding_policy_for_profile, build_branch_block, decode_embedding_as_f32,
    dedup_sort_ids, effective_directional_pca_cluster_count, emit_status, encode_branch_entries,
    fallback_partition_groups, map_clustering_configuration_error, map_clustering_error,
    materializability_bound, normalize_branch_entries, normalize_child_summary_inputs,
    normalize_current_layer, partition_depth, published_indexing_profile, serialize_block,
    start_status_heartbeat, status_with_hierarchy_details, status_with_known_total,
    validate_embedding_bytes, validate_published_profile_configuration, verify_persisted_block_id,
    with_legacy_item_count,
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
    path: PathBuf,
}

#[derive(Clone, Debug)]
struct LoadedLeaf {
    id: BlockHash,
    embedding: Vec<u8>,
    embedding_f32: Vec<f32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum V3Phase {
    Ingesting,
    Finalized,
}

pub struct StreamingIndexingRunV3 {
    observer: Option<StreamingIndexingStatusObserver>,
    profile: PublishedIndexingProfile,
    branch_encoding_policy: BranchEncodingPolicy,
    embedding_spec: EmbeddingSpec,
    block_size_target: usize,
    temp_root: Option<TempDir>,
    root_partition_path: PathBuf,
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
        let root_partition_path = temp_root.path().join("layer-0000-root.leafids");
        File::create(&root_partition_path).map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not create v3 root partition file {}: {error}",
                root_partition_path.display()
            ))
        })?;
        Ok(Self {
            observer: None,
            branch_encoding_policy: branch_encoding_policy_for_profile(&profile),
            profile,
            embedding_spec,
            block_size_target,
            temp_root: Some(temp_root),
            root_partition_path,
            phase: V3Phase::Ingesting,
            ingested_count: 0,
        })
    }

    pub fn with_observer(mut self, observer: StreamingIndexingStatusObserver) -> Self {
        self.observer = Some(observer);
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
        if block_ids.is_empty() {
            return Ok(());
        }
        let mut writer = OpenOptions::new()
            .append(true)
            .open(&self.root_partition_path)
            .map(BufWriter::new)
            .map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not open v3 root partition file {} for append: {error}",
                    self.root_partition_path.display()
                ))
            })?;
        for block_id in block_ids {
            writer
                .write_all(block_id.as_bytes())
                .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
        }
        writer
            .flush()
            .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
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
        if self.ingested_count == 0 {
            return Err(StreamingIndexerError::EmptyInput);
        }

        let mut persisted_ids = Vec::new();
        let mut layer_index = 0usize;
        let mut current_layer = vec![WorkingPartition {
            id: format!("l{layer_index}.p0"),
            layer_index,
            item_count: self.ingested_count,
            kind: WorkingItemKind::LeafBlockIds,
            path: self.root_partition_path.clone(),
        }];

        loop {
            let next_layer_inputs = self
                .process_layer_until_terminal(
                    current_layer,
                    source_store,
                    output_store,
                    &mut persisted_ids,
                )
                .await?;
            if next_layer_inputs.is_empty() {
                return Err(StreamingIndexerError::EmptyInput);
            }
            if next_layer_inputs.len() == 1 {
                dedup_sort_ids(&mut persisted_ids);
                let root_id = next_layer_inputs[0].child;
                self.phase = V3Phase::Finalized;
                if let Some(temp_root) = self.temp_root.take() {
                    temp_root.close().map_err(|error| {
                        StreamingIndexerError::LocalSpill(format!(
                            "could not remove v3 working root {}: {error}",
                            self.root_partition_path
                                .parent()
                                .unwrap_or_else(|| Path::new("."))
                                .display()
                        ))
                    })?;
                }
                return Ok(StreamingIndexingResult {
                    root_id,
                    block_ids: persisted_ids,
                });
            }

            layer_index += 1;
            let next_root_id = format!("l{layer_index}.p0");
            let next_root_path = self.partition_file_path(layer_index, &next_root_id, "summary");
            write_indexed_child_partition(&next_root_path, next_layer_inputs.as_slice())?;
            current_layer = vec![WorkingPartition {
                id: next_root_id,
                layer_index,
                item_count: next_layer_inputs.len(),
                kind: WorkingItemKind::IndexedChildren,
                path: next_root_path,
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
            let mut next = Vec::new();
            for partition in active {
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

        let mut replay_passes = 0usize;
        let max_passes = partition.item_count.saturating_add(4).max(1);
        loop {
            replay_passes += 1;
            if replay_passes > max_passes {
                return Err(StreamingIndexerError::ClusteringFailure(format!(
                    "v3 planner exceeded the maximum replay pass count of {max_passes}"
                )));
            }
            match partition.kind {
                WorkingItemKind::LeafBlockIds => {
                    let mut reader = BlockHashPartitionReader::open(&partition.path)?;
                    while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
                        let loaded = self
                            .load_leaf_batch(batch.as_slice(), partition.layer_index, source_store)
                            .await?;
                        let embeddings = loaded
                            .into_par_iter()
                            .map(|leaf| leaf.embedding_f32)
                            .collect::<Vec<_>>();
                        trainer
                            .ingest_batch(embeddings.as_slice())
                            .map_err(map_clustering_error)?;
                    }
                }
                WorkingItemKind::IndexedChildren => {
                    let mut reader = IndexedChildPartitionReader::open(&partition.path)?;
                    while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
                        let embeddings = batch
                            .par_iter()
                            .map(|child| {
                                decode_embedding_as_f32(
                                    child.embedding.as_slice(),
                                    &self.embedding_spec,
                                )
                            })
                            .collect::<Vec<_>>();
                        let mut decoded = Vec::with_capacity(embeddings.len());
                        for embedding in embeddings {
                            decoded.push(embedding?);
                        }
                        trainer
                            .ingest_batch(decoded.as_slice())
                            .map_err(map_clustering_error)?;
                    }
                }
            }
            let pass_report = trainer.finish_pass().map_err(map_clustering_error)?;
            if pass_report.observed_count != partition.item_count {
                return Err(StreamingIndexerError::HierarchyValidation(format!(
                    "v3 partition {:?} observed {} items but expected {}",
                    partition.id, pass_report.observed_count, partition.item_count
                )));
            }
            if pass_report.readiness != PassReadiness::PartitionReady {
                return Err(StreamingIndexerError::ClusteringFailure(
                    "v3 partition planning did not become partition-ready".into(),
                ));
            }
            match trainer.complete_training() {
                Ok(()) => break,
                Err(StreamingClusteringError::InvalidTransition { state, operation })
                    if state == TrainerState::PassComplete && operation == "complete_training" =>
                {
                    continue;
                }
                Err(error) => return Err(map_clustering_error(error)),
            }
        }
        let classifier = trainer.into_classifier().map_err(map_clustering_error)?;

        let child_count = usize::try_from(classifier.realized_cluster_count()).map_err(|_| {
            StreamingIndexerError::HierarchyValidation(
                "v3 realized cluster count does not fit usize".into(),
            )
        })?;
        let mut child_ids = (0..child_count)
            .map(|child_index| format!("{}.{}", partition.id, child_index))
            .collect::<Vec<_>>();
        let mut child_paths = child_ids
            .iter()
            .map(|child_id| {
                self.partition_file_path(
                    partition.layer_index,
                    child_id,
                    match partition.kind {
                        WorkingItemKind::LeafBlockIds => "leafids",
                        WorkingItemKind::IndexedChildren => "summary",
                    },
                )
            })
            .collect::<Vec<_>>();
        let mut child_item_counts = vec![0usize; child_count];
        if child_count <= 1 {
            child_ids = (0..2)
                .map(|child_index| format!("{}.{}", partition.id, child_index))
                .collect();
            child_paths = child_ids
                .iter()
                .map(|child_id| {
                    self.partition_file_path(
                        partition.layer_index,
                        child_id,
                        match partition.kind {
                            WorkingItemKind::LeafBlockIds => "leafids",
                            WorkingItemKind::IndexedChildren => "summary",
                        },
                    )
                })
                .collect();
            child_item_counts = vec![0usize; 2];
        }

        match partition.kind {
            WorkingItemKind::LeafBlockIds => {
                let mut readers = BlockHashPartitionReader::open(&partition.path)?;
                let mut writers = BlockHashPartitionWriters::create(child_paths.as_slice())?;
                while let Some(batch) = readers.next_batch(V3_BATCH_SIZE)? {
                    let loaded = self
                        .load_leaf_batch(batch.as_slice(), partition.layer_index, source_store)
                        .await?;
                    let embeddings = loaded
                        .par_iter()
                        .map(|leaf| leaf.embedding_f32.clone())
                        .collect::<Vec<_>>();
                    let assignments = classifier
                        .assign_batch(embeddings.as_slice())
                        .map_err(map_clustering_error)?;
                    for ((block_id, _loaded), assignment) in
                        batch.iter().zip(loaded).zip(assignments)
                    {
                        let cluster = usize::try_from(assignment).map_err(|_| {
                            StreamingIndexerError::HierarchyValidation(
                                "v3 cluster id does not fit usize".into(),
                            )
                        })?;
                        let target = cluster.min(writers.len().saturating_sub(1));
                        writers.write(target, block_id)?;
                        child_item_counts[target] += 1;
                    }
                }
                writers.finish()?;
            }
            WorkingItemKind::IndexedChildren => {
                let mut reader = IndexedChildPartitionReader::open(&partition.path)?;
                let mut writers = IndexedChildPartitionWriters::create(child_paths.as_slice())?;
                while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
                    let embeddings = batch
                        .par_iter()
                        .map(|child| {
                            decode_embedding_as_f32(
                                child.embedding.as_slice(),
                                &self.embedding_spec,
                            )
                        })
                        .collect::<Vec<_>>();
                    let mut decoded = Vec::with_capacity(embeddings.len());
                    for embedding in embeddings {
                        decoded.push(embedding?);
                    }
                    let assignments = classifier
                        .assign_batch(decoded.as_slice())
                        .map_err(map_clustering_error)?;
                    for (child, assignment) in batch.iter().zip(assignments) {
                        let cluster = usize::try_from(assignment).map_err(|_| {
                            StreamingIndexerError::HierarchyValidation(
                                "v3 cluster id does not fit usize".into(),
                            )
                        })?;
                        let target = cluster.min(writers.len().saturating_sub(1));
                        writers.write(target, child)?;
                        child_item_counts[target] += 1;
                    }
                }
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
            let fallback_groups =
                fallback_partition_groups(partition.item_count, materializability_bound, None)
                    .map_err(|error| {
                        StreamingIndexerError::HierarchyValidation(error.to_string())
                    })?;
            child_ids = (0..fallback_groups.len())
                .map(|child_index| format!("{}.{}", partition.id, child_index))
                .collect::<Vec<_>>();
            child_paths = child_ids
                .iter()
                .map(|child_id| {
                    self.partition_file_path(
                        partition.layer_index,
                        child_id,
                        match partition.kind {
                            WorkingItemKind::LeafBlockIds => "leafids",
                            WorkingItemKind::IndexedChildren => "summary",
                        },
                    )
                })
                .collect::<Vec<_>>();
            child_item_counts = fallback_groups.iter().map(Vec::len).collect::<Vec<_>>();
            match partition.kind {
                WorkingItemKind::LeafBlockIds => {
                    let all_ids = read_all_block_hashes(&partition.path)?;
                    let mut writers = BlockHashPartitionWriters::create(child_paths.as_slice())?;
                    for (group_index, group) in fallback_groups.into_iter().enumerate() {
                        for local_index in group {
                            writers.write(group_index, &all_ids[local_index])?;
                        }
                    }
                    writers.finish()?;
                }
                WorkingItemKind::IndexedChildren => {
                    let all_children = read_all_indexed_children(&partition.path)?;
                    let mut writers = IndexedChildPartitionWriters::create(child_paths.as_slice())?;
                    for (group_index, group) in fallback_groups.into_iter().enumerate() {
                        for local_index in group {
                            writers.write(group_index, &all_children[local_index])?;
                        }
                    }
                    writers.finish()?;
                }
            }
            non_empty = child_item_counts
                .iter()
                .enumerate()
                .filter_map(|(index, count)| (*count > 0).then_some(index))
                .collect::<Vec<_>>();
        }

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

        Ok(non_empty
            .into_iter()
            .map(|index| WorkingPartition {
                id: child_ids[index].clone(),
                layer_index: partition.layer_index,
                item_count: child_item_counts[index],
                kind: partition.kind,
                path: child_paths[index].clone(),
            })
            .collect())
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
                let block_ids = read_all_block_hashes(&partition.path)?;
                let loaded = self
                    .load_leaf_batch(block_ids.as_slice(), partition.layer_index, source_store)
                    .await?;
                let mut children = Vec::with_capacity(loaded.len());
                for leaf in loaded {
                    let validated = source_store
                        .get(&leaf.id)
                        .await
                        .map_err(StreamingIndexerError::Storage)?
                        .ok_or_else(|| {
                            StreamingIndexerError::Storage(
                                lexongraph_block_store::BlockStoreError::BackendFailure(format!(
                                    "leaf block {} disappeared during v3 materialization",
                                    leaf.id
                                )),
                            )
                        })?;
                    let output_id = output_store
                        .put(&validated.block)
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
                let mut children = read_all_indexed_children(&partition.path)?;
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
        emit_status(
            &self.observer,
            with_legacy_item_count(
                status_with_known_total(
                    status.phase,
                    StreamingIndexingStatusState::InProgress,
                    status.progress.load(AtomicOrdering::Relaxed),
                    status.progress.load(AtomicOrdering::Relaxed),
                    status.started.elapsed(),
                    None,
                ),
                status.legacy_item_count,
            ),
        );
        Ok(next_layer)
    }

    async fn load_leaf_batch(
        &self,
        block_ids: &[BlockHash],
        layer_index: usize,
        source_store: &dyn BlockStore,
    ) -> Result<Vec<LoadedLeaf>, StreamingIndexerError> {
        let phase = StreamingIndexingPhase::V3PartitionLoad { layer_index };
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
        let blocks = futures::stream::iter(block_ids.iter().copied())
            .map(|block_id| {
                let progress = Arc::clone(&progress);
                async move {
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
                    progress.fetch_add(1, AtomicOrdering::Relaxed);
                    Ok::<(BlockHash, ValidatedBlock), StreamingIndexerError>((block_id, block))
                }
            })
            .buffered(V3_IO_QUEUE_DEPTH)
            .collect::<Vec<_>>()
            .await;
        let mut ordered = Vec::with_capacity(blocks.len());
        for result in blocks {
            ordered.push(result?);
        }
        let decoded = ordered
            .into_par_iter()
            .map(|(block_id, block)| decode_loaded_leaf(block_id, block, &self.embedding_spec))
            .collect::<Vec<_>>();
        let mut loaded = Vec::with_capacity(decoded.len());
        for leaf in decoded {
            loaded.push(leaf?);
        }
        heartbeat.stop();
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

    fn partition_file_path(&self, layer_index: usize, partition_id: &str, suffix: &str) -> PathBuf {
        let name = partition_id.replace('.', "_");
        self.temp_root
            .as_ref()
            .expect("v3 temp root should exist until success")
            .path()
            .join(format!("layer-{layer_index:04}-{name}.{suffix}"))
    }
}

fn decode_loaded_leaf(
    block_id: BlockHash,
    block: ValidatedBlock,
    embedding_spec: &EmbeddingSpec,
) -> Result<LoadedLeaf, StreamingIndexerError> {
    let Block::Leaf(leaf) = block.block else {
        return Err(StreamingIndexerError::HierarchyValidation(format!(
            "v3 input block {} is not a leaf block",
            block_id
        )));
    };
    validate_v3_leaf(block_id, &leaf, embedding_spec)?;
    let entry = leaf
        .entries
        .first()
        .expect("validated leaf must contain an entry");
    Ok(LoadedLeaf {
        id: block_id,
        embedding: entry.embedding.clone(),
        embedding_f32: decode_embedding_as_f32(entry.embedding.as_slice(), embedding_spec)?,
    })
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

struct BlockHashPartitionReader {
    reader: BufReader<File>,
}

impl BlockHashPartitionReader {
    fn open(path: &Path) -> Result<Self, StreamingIndexerError> {
        let file = File::open(path).map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not open v3 block-id partition {}: {error}",
                path.display()
            ))
        })?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    fn next_batch(
        &mut self,
        batch_size: usize,
    ) -> Result<Option<Vec<BlockHash>>, StreamingIndexerError> {
        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let mut bytes = [0u8; BlockHash::LEN];
            match self.reader.read_exact(&mut bytes) {
                Ok(()) => batch.push(BlockHash::from_bytes(bytes)),
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(StreamingIndexerError::LocalSpill(error.to_string())),
            }
        }
        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}

struct IndexedChildPartitionReader {
    reader: BufReader<File>,
}

impl IndexedChildPartitionReader {
    fn open(path: &Path) -> Result<Self, StreamingIndexerError> {
        let file = File::open(path).map_err(|error| {
            StreamingIndexerError::LocalSpill(format!(
                "could not open v3 summary partition {}: {error}",
                path.display()
            ))
        })?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    fn next_batch(
        &mut self,
        batch_size: usize,
    ) -> Result<Option<Vec<IndexedChild>>, StreamingIndexerError> {
        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let Some(child) = crate::read_spilled_indexed_child(&mut self.reader)? else {
                break;
            };
            batch.push(child);
        }
        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}

struct BlockHashPartitionWriters {
    writers: Vec<BufWriter<File>>,
}

impl BlockHashPartitionWriters {
    fn create(paths: &[PathBuf]) -> Result<Self, StreamingIndexerError> {
        let mut writers = Vec::with_capacity(paths.len());
        for path in paths {
            let writer = File::create(path).map(BufWriter::new).map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not create v3 block-id partition {}: {error}",
                    path.display()
                ))
            })?;
            writers.push(writer);
        }
        Ok(Self { writers })
    }

    fn len(&self) -> usize {
        self.writers.len()
    }

    fn write(&mut self, index: usize, block_id: &BlockHash) -> Result<(), StreamingIndexerError> {
        self.writers[index]
            .write_all(block_id.as_bytes())
            .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))
    }

    fn finish(mut self) -> Result<(), StreamingIndexerError> {
        for writer in &mut self.writers {
            writer
                .flush()
                .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
        }
        Ok(())
    }
}

struct IndexedChildPartitionWriters {
    writers: Vec<BufWriter<File>>,
}

impl IndexedChildPartitionWriters {
    fn create(paths: &[PathBuf]) -> Result<Self, StreamingIndexerError> {
        let mut writers = Vec::with_capacity(paths.len());
        for path in paths {
            let writer = File::create(path).map(BufWriter::new).map_err(|error| {
                StreamingIndexerError::LocalSpill(format!(
                    "could not create v3 summary partition {}: {error}",
                    path.display()
                ))
            })?;
            writers.push(writer);
        }
        Ok(Self { writers })
    }

    fn len(&self) -> usize {
        self.writers.len()
    }

    fn write(&mut self, index: usize, child: &IndexedChild) -> Result<(), StreamingIndexerError> {
        crate::write_spilled_indexed_child(&mut self.writers[index], child)
    }

    fn finish(mut self) -> Result<(), StreamingIndexerError> {
        for writer in &mut self.writers {
            writer
                .flush()
                .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))?;
        }
        Ok(())
    }
}

fn read_all_block_hashes(path: &Path) -> Result<Vec<BlockHash>, StreamingIndexerError> {
    let mut reader = BlockHashPartitionReader::open(path)?;
    let mut all = Vec::new();
    while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
        all.extend(batch);
    }
    Ok(all)
}

fn read_all_indexed_children(path: &Path) -> Result<Vec<IndexedChild>, StreamingIndexerError> {
    let mut reader = IndexedChildPartitionReader::open(path)?;
    let mut all = Vec::new();
    while let Some(batch) = reader.next_batch(V3_BATCH_SIZE)? {
        all.extend(batch);
    }
    Ok(all)
}

fn write_indexed_child_partition(
    path: &Path,
    children: &[IndexedChild],
) -> Result<(), StreamingIndexerError> {
    let file = File::create(path).map_err(|error| {
        StreamingIndexerError::LocalSpill(format!(
            "could not create v3 summary root partition {}: {error}",
            path.display()
        ))
    })?;
    let mut writer = BufWriter::new(file);
    for child in children {
        crate::write_spilled_indexed_child(&mut writer, child)?;
    }
    writer
        .flush()
        .map_err(|error| StreamingIndexerError::LocalSpill(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn v3_observer_reports_partition_load_phase() {
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
        assert!(
            phases
                .lock()
                .unwrap()
                .iter()
                .any(|phase| matches!(phase, StreamingIndexingPhase::V3PartitionLoad { .. }))
        );
    }
}
