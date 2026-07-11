// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Streaming directional-PCA clustering for LexonGraph.

use std::collections::BTreeMap;

use lexongraph_pca::{PcaAccumulator, PcaError, PcaTransform};
use lexongraph_streaming_clustering::{
    ClusterId, Embedding, MetricDirection, PassReadiness, PassReport, StreamingClusterClassifier,
    StreamingClusterTrainer, StreamingClusteringConfig, StreamingClusteringError, TrainerState,
    validate_config, validate_embedding,
};
use sha2::{Digest, Sha256};

pub const DIRECTIONAL_PCA_SOFTWARE_IDENTITY: &str =
    concat!("lexongraph-directional-pca-v", env!("CARGO_PKG_VERSION"));

const DENSITY_VALLEY_HISTOGRAM_BUCKET_CAP: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectionalPcaRetainedAxisPolicy {
    FixedCount(usize),
    AdaptiveAllEligible,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectionalPcaAllocationPolicy {
    CentroidWeightedBins,
    EigenvalueLogBits,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectionalPcaBinningPolicy {
    Quantile,
    DensityValley,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectionalPcaClusterCardinalityMode {
    Exact,
    UnderfullSuccess,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectionalPcaParams {
    pub retained_axis_policy: DirectionalPcaRetainedAxisPolicy,
    pub allocation_policy: DirectionalPcaAllocationPolicy,
    pub binning_policy: DirectionalPcaBinningPolicy,
    pub cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode,
    pub variance_exponent: f32,
    pub temperature: f32,
    pub min_input_count: usize,
    pub min_effective_rank: usize,
    pub min_cumulative_variance: f32,
}

#[derive(Debug)]
pub struct DirectionalPcaStreamingTrainer {
    config: StreamingClusteringConfig,
    params: DirectionalPcaParams,
    state: TrainerState,
    phase: ReplayPhase,
    active_pass: Option<ActivePassState>,
    baseline_fingerprint: Option<PassFingerprint>,
    quality_metric: f64,
    model: Option<DirectionalPcaModel>,
}

#[derive(Clone, Debug)]
pub struct DirectionalPcaStreamingClassifier {
    config: StreamingClusteringConfig,
    centroids: Vec<Embedding>,
}

#[derive(Clone, Debug)]
struct DirectionalPcaModel {
    centroids: Vec<Embedding>,
}

#[derive(Debug)]
enum ReplayPhase {
    AnalyzePca,
    PlanCuts(PartitionAnalysisPlan),
    CountCells(PartitionPlan),
    RealizePartition(ReadyPartitionPlan),
}

#[derive(Clone, Debug)]
struct PartitionAnalysisPlan {
    transform: PcaTransform,
    axis_bin_counts: Vec<usize>,
    binning_policy: DirectionalPcaBinningPolicy,
    total_count: usize,
}

#[derive(Clone, Debug)]
struct PartitionPlan {
    transform: PcaTransform,
    axis_plans: Vec<AxisPlan>,
}

#[derive(Clone, Debug)]
struct ReadyPartitionPlan {
    partition: PartitionPlan,
    cells: Vec<ReadyCellPlan>,
}

#[derive(Debug)]
enum ActivePassState {
    AnalyzePca(ActivePcaPass),
    PlanCuts(ActiveCutPlanningPass),
    CountCells(ActiveCellCountingPass),
    RealizePartition(ActivePartitionRealizationPass),
}

#[derive(Debug)]
struct ActivePcaPass {
    tracker: PassTracker,
    accumulator: PcaAccumulator,
}

#[derive(Debug)]
struct ActiveCutPlanningPass {
    tracker: PassTracker,
    plan: PartitionAnalysisPlan,
    planners: Vec<AxisPlanner>,
}

#[derive(Debug)]
struct ActiveCellCountingPass {
    tracker: PassTracker,
    partition: PartitionPlan,
    cursor_state: AxisCursorState,
    cell_summaries: BTreeMap<Vec<usize>, CellDuplicateSummary>,
}

#[derive(Debug)]
struct ActivePartitionRealizationPass {
    tracker: PassTracker,
    ready: ReadyPartitionPlan,
    cursor_state: AxisCursorState,
    cell_seen_counts: Vec<usize>,
    cell_stats: Vec<CellStats>,
}

#[derive(Clone, Debug)]
enum AxisPlan {
    SingleBin,
    Quantile(QuantileAxisPlan),
    Thresholds(Vec<f32>),
}

#[derive(Clone, Debug)]
struct QuantileAxisPlan {
    groups: Vec<QuantileValueGroup>,
}

#[derive(Clone, Debug)]
struct QuantileValueGroup {
    value: f32,
    quotas: Vec<usize>,
}

#[derive(Debug)]
enum AxisPlanner {
    Quantile(QuantileAxisPlanner),
    DensityMinMax(DensityValleyMinMaxPlanner),
    DensityHistogram(DensityValleyHistogramPlanner),
    Ready(AxisPlan),
}

#[derive(Debug)]
struct QuantileAxisPlanner {
    targets: Vec<usize>,
    next_target_index: usize,
    consumed_count: usize,
    lower_bound: Option<f32>,
    candidate_groups: Vec<ValueCount>,
    groups: Vec<QuantileValueGroup>,
    candidate_capacity: usize,
}

#[derive(Debug)]
struct DensityValleyMinMaxPlanner {
    bin_count: usize,
    minimum: Option<f32>,
    maximum: Option<f32>,
}

#[derive(Debug)]
struct DensityValleyHistogramPlanner {
    minimum: f32,
    maximum: f32,
    counts: Vec<usize>,
    bin_count: usize,
}

#[derive(Clone, Copy, Debug)]
struct ValueCount {
    value: f32,
    count: usize,
}

#[derive(Clone, Debug)]
struct AxisCursorState {
    quantile_positions: Vec<Vec<usize>>,
}

#[derive(Clone, Debug)]
struct CellStats {
    count: usize,
    sums: Vec<f64>,
}

#[derive(Clone, Debug)]
struct ReadyCellPlan {
    key: Vec<usize>,
    count: usize,
    extra_clusters: usize,
    cluster_offset: usize,
}

#[derive(Clone, Debug)]
struct CellDuplicateSummary {
    count: usize,
    coordinate_sums: Vec<f64>,
    coordinate_sum_squares: Vec<f64>,
}

#[derive(Debug)]
struct PassTracker {
    observed_count: usize,
    hasher: Sha256,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PassFingerprint {
    observed_count: usize,
    digest: [u8; 32],
}

impl DirectionalPcaStreamingTrainer {
    pub fn new(
        config: StreamingClusteringConfig,
        params: DirectionalPcaParams,
    ) -> Result<Self, StreamingClusteringError> {
        validate_config(&config)?;
        validate_params(&config, &params)?;
        reject_balance_constraints(&config)?;
        Ok(Self {
            config,
            params,
            state: TrainerState::Idle,
            phase: ReplayPhase::AnalyzePca,
            active_pass: None,
            baseline_fingerprint: None,
            quality_metric: 0.0,
            model: None,
        })
    }

    fn invalid_transition(&mut self, operation: &str) -> StreamingClusteringError {
        let state = self.state;
        self.state = TrainerState::Error;
        StreamingClusteringError::InvalidTransition {
            state,
            operation: operation.into(),
        }
    }

    fn fail(&mut self, error: StreamingClusteringError) -> StreamingClusteringError {
        self.state = TrainerState::Error;
        self.active_pass = None;
        error
    }

    fn ensure_active_pass(&mut self) {
        if self.active_pass.is_some() {
            return;
        }
        self.active_pass = Some(match &self.phase {
            ReplayPhase::AnalyzePca => ActivePassState::AnalyzePca(ActivePcaPass {
                tracker: PassTracker::new(),
                accumulator: PcaAccumulator::new(self.config.dimensions),
            }),
            ReplayPhase::PlanCuts(plan) => ActivePassState::PlanCuts(ActiveCutPlanningPass {
                tracker: PassTracker::new(),
                plan: plan.clone(),
                planners: plan
                    .axis_bin_counts
                    .iter()
                    .map(|&bin_count| match plan.binning_policy {
                        DirectionalPcaBinningPolicy::Quantile => AxisPlanner::new_quantile(
                            bin_count,
                            plan.total_count,
                            self.config.cluster_count as usize,
                        ),
                        DirectionalPcaBinningPolicy::DensityValley => {
                            AxisPlanner::new_density(bin_count)
                        }
                    })
                    .collect(),
            }),
            ReplayPhase::CountCells(partition) => {
                ActivePassState::CountCells(ActiveCellCountingPass {
                    tracker: PassTracker::new(),
                    partition: partition.clone(),
                    cursor_state: AxisCursorState::for_partition(partition),
                    cell_summaries: BTreeMap::new(),
                })
            }
            ReplayPhase::RealizePartition(ready) => {
                ActivePassState::RealizePartition(ActivePartitionRealizationPass {
                    tracker: PassTracker::new(),
                    ready: ready.clone(),
                    cursor_state: AxisCursorState::for_partition(&ready.partition),
                    cell_seen_counts: vec![0; ready.cells.len()],
                    cell_stats: ready
                        .cells
                        .iter()
                        .flat_map(|cell| {
                            std::iter::repeat_with(|| CellStats {
                                count: 0,
                                sums: vec![0.0; self.config.dimensions],
                            })
                            .take(1 + cell.extra_clusters)
                        })
                        .collect(),
                })
            }
        });
    }

    fn finish_pass_impl(&mut self) -> Result<PassReport, StreamingClusteringError> {
        if self.state != TrainerState::Ingesting {
            return Err(self.invalid_transition("finish_pass"));
        }
        let active_pass = self
            .active_pass
            .take()
            .ok_or_else(|| malformed_input("completed pass must contain at least one embedding"))?;

        let (next_phase, report, model) = match active_pass {
            ActivePassState::AnalyzePca(pass) => self.finish_pca_pass(pass)?,
            ActivePassState::PlanCuts(pass) => self.finish_cut_planning_pass(pass)?,
            ActivePassState::CountCells(pass) => self.finish_cell_counting_pass(pass)?,
            ActivePassState::RealizePartition(pass) => {
                self.finish_partition_realization_pass(pass)?
            }
        };

        self.phase = next_phase;
        if let Some(model) = model {
            self.model = Some(model);
        }
        self.state = TrainerState::PassComplete;
        Ok(report)
    }

    fn finish_pca_pass(
        &mut self,
        pass: ActivePcaPass,
    ) -> Result<(ReplayPhase, PassReport, Option<DirectionalPcaModel>), StreamingClusteringError>
    {
        let fingerprint = pass.tracker.finish();
        let observed_count = fingerprint.observed_count;
        if observed_count == 0 {
            return Err(malformed_input(
                "completed pass must contain at least one embedding",
            ));
        }

        let minimum_required = match self.params.cluster_cardinality_mode {
            DirectionalPcaClusterCardinalityMode::Exact => self
                .params
                .min_input_count
                .max(self.config.cluster_count as usize),
            DirectionalPcaClusterCardinalityMode::UnderfullSuccess => self.params.min_input_count,
        };
        if observed_count < minimum_required {
            return Err(unsatisfiable_constraint(format!(
                "first pass established N = {observed_count}, smaller than the required minimum {minimum_required}"
            )));
        }

        self.baseline_fingerprint = Some(fingerprint);
        let transform = pass.accumulator.finalize().map_err(map_pca_error)?;
        let effective_rank = transform.diagnostics().rank_estimate;
        let is_degenerate = transform
            .explained_variance()
            .map(|values| values.iter().all(|value| value.abs() <= f32::EPSILON))
            .unwrap_or(false);
        if !is_degenerate && effective_rank < self.params.min_effective_rank {
            return Err(unsatisfiable_constraint(format!(
                "effective rank {effective_rank} is smaller than the required minimum {}",
                self.params.min_effective_rank
            )));
        }

        let retained_axis_count = resolve_retained_axis_count(
            &transform,
            &self.params,
            self.config.cluster_count as usize,
            effective_rank,
        )?;
        let cumulative_variance = transform
            .cumulative_variance()
            .and_then(|values| values.get(retained_axis_count.saturating_sub(1)).copied())
            .unwrap_or(0.0);
        if !is_degenerate && cumulative_variance < self.params.min_cumulative_variance {
            return Err(unsatisfiable_constraint(format!(
                "cumulative variance {cumulative_variance} is smaller than the required minimum {}",
                self.params.min_cumulative_variance
            )));
        }

        let transform = transform
            .truncate(retained_axis_count)
            .map_err(map_pca_error)?;
        let axis_bin_counts = allocate_axis_bins(
            transform.mean.as_slice(),
            &transform,
            &self.params,
            self.config.cluster_count as usize,
        )?;
        self.quality_metric = quality_metric_from_transform(&transform);
        let report = analysis_only_report(
            observed_count,
            self.config.cluster_count,
            self.quality_metric,
        );
        Ok((
            ReplayPhase::PlanCuts(PartitionAnalysisPlan {
                transform,
                axis_bin_counts,
                binning_policy: self.params.binning_policy,
                total_count: observed_count,
            }),
            report,
            None,
        ))
    }

    fn finish_cut_planning_pass(
        &mut self,
        pass: ActiveCutPlanningPass,
    ) -> Result<(ReplayPhase, PassReport, Option<DirectionalPcaModel>), StreamingClusteringError>
    {
        let fingerprint = pass.tracker.finish();
        self.validate_replayed_pass(&fingerprint)?;

        let advanced_planners = pass
            .planners
            .into_iter()
            .map(AxisPlanner::finish_pass)
            .collect::<Result<Vec<_>, _>>()?;
        let ready = advanced_planners
            .iter()
            .all(|planner| matches!(planner, AxisPlanner::Ready(_)));
        let axis_plans = advanced_planners
            .iter()
            .filter_map(|planner| match planner {
                AxisPlanner::Ready(plan) => Some(plan.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        let report = analysis_only_report(
            fingerprint.observed_count,
            self.config.cluster_count,
            self.quality_metric,
        );
        if ready {
            Ok((
                ReplayPhase::CountCells(PartitionPlan {
                    transform: pass.plan.transform,
                    axis_plans,
                }),
                report,
                None,
            ))
        } else {
            Ok((ReplayPhase::PlanCuts(pass.plan), report, None))
        }
    }

    fn finish_cell_counting_pass(
        &mut self,
        pass: ActiveCellCountingPass,
    ) -> Result<(ReplayPhase, PassReport, Option<DirectionalPcaModel>), StreamingClusteringError>
    {
        let fingerprint = pass.tracker.finish();
        self.validate_replayed_pass(&fingerprint)?;

        let populated_cell_count = pass.cell_summaries.len();
        if populated_cell_count == 0 {
            return Err(unsatisfiable_constraint(
                "directional-PCA partition realized zero populated cells",
            ));
        }
        if populated_cell_count > self.config.cluster_count as usize {
            return Err(unsatisfiable_constraint(format!(
                "directional-PCA partition realized {populated_cell_count} populated cells instead of the required {}",
                self.config.cluster_count
            )));
        }
        let ready = build_ready_partition_plan(
            pass.partition,
            pass.cell_summaries,
            self.config.cluster_count as usize,
            self.params.cluster_cardinality_mode,
        )?;
        let realized_cluster_count = ready
            .cells
            .iter()
            .map(|cell| 1 + cell.extra_clusters)
            .sum::<usize>() as u32;
        let report = PassReport {
            observed_count: fingerprint.observed_count,
            requested_cluster_count: self.config.cluster_count,
            readiness: PassReadiness::PartitionReady,
            realized_cluster_count: Some(realized_cluster_count),
            quality_metric: self.quality_metric,
            balance_metric: 0.0,
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: Some((0..realized_cluster_count).collect()),
        };
        Ok((ReplayPhase::RealizePartition(ready), report, None))
    }

    fn finish_partition_realization_pass(
        &mut self,
        pass: ActivePartitionRealizationPass,
    ) -> Result<(ReplayPhase, PassReport, Option<DirectionalPcaModel>), StreamingClusteringError>
    {
        let fingerprint = pass.tracker.finish();
        self.validate_replayed_pass(&fingerprint)?;

        let centroids = pass
            .cell_stats
            .iter()
            .map(|stats| stats.centroid())
            .collect::<Result<Vec<_>, _>>()?;
        let realized_cluster_count = centroids.len() as u32;
        let report = PassReport {
            observed_count: fingerprint.observed_count,
            requested_cluster_count: self.config.cluster_count,
            readiness: PassReadiness::PartitionReady,
            realized_cluster_count: Some(realized_cluster_count),
            quality_metric: self.quality_metric,
            balance_metric: 0.0,
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: Some((0..realized_cluster_count).collect()),
        };
        Ok((
            ReplayPhase::RealizePartition(pass.ready),
            report,
            Some(DirectionalPcaModel { centroids }),
        ))
    }

    fn validate_replayed_pass(
        &self,
        fingerprint: &PassFingerprint,
    ) -> Result<(), StreamingClusteringError> {
        let baseline = self.baseline_fingerprint.as_ref().ok_or_else(|| {
            unsatisfiable_constraint("missing baseline dataset for later directional-PCA passes")
        })?;
        if baseline != fingerprint {
            return Err(malformed_input(
                "later passes must replay the same logical dataset in the same order",
            ));
        }
        Ok(())
    }
}

impl StreamingClusterTrainer for DirectionalPcaStreamingTrainer {
    type Classifier = DirectionalPcaStreamingClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn state(&self) -> TrainerState {
        self.state
    }

    fn ingest_batch(&mut self, embeddings: &[Embedding]) -> Result<(), StreamingClusteringError> {
        match self.state {
            TrainerState::Idle | TrainerState::PassComplete => {
                self.state = TrainerState::Ingesting;
                self.ensure_active_pass();
            }
            TrainerState::Ingesting => {}
            TrainerState::TrainingComplete | TrainerState::Error => {
                return Err(self.invalid_transition("ingest_batch"));
            }
        }

        let active_pass = self
            .active_pass
            .as_mut()
            .ok_or_else(|| malformed_input("missing active directional-PCA pass state"))?;
        for embedding in embeddings {
            validate_embedding(embedding, self.config.dimensions)?;
            match active_pass {
                ActivePassState::AnalyzePca(pass) => {
                    pass.tracker.update(embedding);
                    pass.accumulator.update(embedding).map_err(map_pca_error)?;
                }
                ActivePassState::PlanCuts(pass) => {
                    pass.tracker.update(embedding);
                    let coordinates = pass
                        .plan
                        .transform
                        .apply(embedding)
                        .map_err(map_pca_error)?;
                    for (planner, value) in pass.planners.iter_mut().zip(coordinates) {
                        planner.observe(value);
                    }
                }
                ActivePassState::CountCells(pass) => {
                    pass.tracker.update(embedding);
                    let coordinates = pass
                        .partition
                        .transform
                        .apply(embedding)
                        .map_err(map_pca_error)?;
                    let key = pass
                        .partition
                        .assign_point_to_cell(coordinates.as_slice(), &mut pass.cursor_state)?;
                    pass.cell_summaries
                        .entry(key)
                        .or_insert_with(|| {
                            CellDuplicateSummary::new(pass.partition.transform.output_dim)
                        })
                        .observe(coordinates.as_slice());
                }
                ActivePassState::RealizePartition(pass) => {
                    pass.tracker.update(embedding);
                    let coordinates = pass
                        .ready
                        .partition
                        .transform
                        .apply(embedding)
                        .map_err(map_pca_error)?;
                    let key = pass
                        .ready
                        .partition
                        .assign_point_to_cell(coordinates.as_slice(), &mut pass.cursor_state)?;
                    let cell_index = pass
                        .ready
                        .cells
                        .binary_search_by(|cell| cell.key.cmp(&key))
                        .map_err(|_| {
                            unsatisfiable_constraint("partition-ready cell plan was not stable")
                        })?;
                    let cell = &pass.ready.cells[cell_index];
                    let seen = pass.cell_seen_counts[cell_index];
                    pass.cell_seen_counts[cell_index] += 1;
                    let base_count = cell.count - cell.extra_clusters;
                    let cluster_index = if seen < base_count {
                        cell.cluster_offset
                    } else {
                        cell.cluster_offset + 1 + (seen - base_count)
                    };
                    pass.cell_stats[cluster_index].observe(embedding);
                }
            }
        }
        Ok(())
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        self.finish_pass_impl().map_err(|error| self.fail(error))
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        if self.state != TrainerState::PassComplete || self.model.is_none() {
            return Err(self.invalid_transition("complete_training"));
        }
        self.state = TrainerState::TrainingComplete;
        Ok(())
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        if self.state != TrainerState::TrainingComplete {
            return Err(StreamingClusteringError::InvalidTransition {
                state: self.state,
                operation: "into_classifier".into(),
            });
        }
        let model = self
            .model
            .ok_or_else(|| StreamingClusteringError::InvalidTransition {
                state: self.state,
                operation: "into_classifier".into(),
            })?;
        Ok(DirectionalPcaStreamingClassifier {
            config: self.config,
            centroids: model.centroids,
        })
    }
}

impl StreamingClusterClassifier for DirectionalPcaStreamingClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn realized_cluster_count(&self) -> u32 {
        self.centroids.len() as u32
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        Ok(self.assigned_distance(embedding)?.0)
    }
}

impl DirectionalPcaStreamingClassifier {
    pub fn assigned_distance(
        &self,
        embedding: &[f32],
    ) -> Result<(ClusterId, f64), StreamingClusteringError> {
        validate_embedding(embedding, self.config.dimensions)?;
        let mut best_cluster = 0usize;
        let mut best_distance = squared_distance(embedding, self.centroids[0].as_slice())?;
        for cluster_index in 1..self.centroids.len() {
            let distance = squared_distance(embedding, self.centroids[cluster_index].as_slice())?;
            if distance < best_distance {
                best_distance = distance;
                best_cluster = cluster_index;
            }
        }
        Ok((best_cluster as ClusterId, best_distance.sqrt()))
    }
}

impl AxisPlanner {
    fn new_quantile(bin_count: usize, total_count: usize, candidate_capacity: usize) -> Self {
        if bin_count <= 1 {
            return Self::Ready(AxisPlan::SingleBin);
        }
        let targets = (1..bin_count)
            .map(|bin| div_ceil(bin * total_count, bin_count))
            .collect();
        Self::Quantile(QuantileAxisPlanner {
            targets,
            next_target_index: 0,
            consumed_count: 0,
            lower_bound: None,
            candidate_groups: Vec::new(),
            groups: Vec::new(),
            candidate_capacity: candidate_capacity.max(1),
        })
    }

    fn new_density(bin_count: usize) -> Self {
        if bin_count <= 1 {
            Self::Ready(AxisPlan::SingleBin)
        } else {
            Self::DensityMinMax(DensityValleyMinMaxPlanner {
                bin_count,
                minimum: None,
                maximum: None,
            })
        }
    }

    fn observe(&mut self, value: f32) {
        match self {
            Self::Ready(_) => {}
            Self::Quantile(planner) => planner.observe(value),
            Self::DensityMinMax(planner) => planner.observe(value),
            Self::DensityHistogram(planner) => planner.observe(value),
        }
    }

    fn finish_pass(self) -> Result<Self, StreamingClusteringError> {
        match self {
            Self::Ready(plan) => Ok(Self::Ready(plan)),
            Self::Quantile(planner) => planner.finish_pass(),
            Self::DensityMinMax(planner) => Ok(planner.finish_pass()),
            Self::DensityHistogram(planner) => planner.finish_pass(),
        }
    }
}

impl From<AxisPlan> for AxisPlanner {
    fn from(value: AxisPlan) -> Self {
        Self::Ready(value)
    }
}

impl From<QuantileAxisPlanner> for AxisPlanner {
    fn from(value: QuantileAxisPlanner) -> Self {
        Self::Quantile(value)
    }
}

impl From<DensityValleyHistogramPlanner> for AxisPlanner {
    fn from(value: DensityValleyHistogramPlanner) -> Self {
        Self::DensityHistogram(value)
    }
}

impl QuantileAxisPlanner {
    fn observe(&mut self, value: f32) {
        if self.next_target_index >= self.targets.len() {
            return;
        }
        if let Some(lower_bound) = self.lower_bound
            && value <= lower_bound
        {
            return;
        }
        if let Some(existing) = self
            .candidate_groups
            .iter_mut()
            .find(|candidate| candidate.value == value)
        {
            existing.count += 1;
            return;
        }

        if self.candidate_groups.len() < self.candidate_capacity {
            self.candidate_groups.push(ValueCount { value, count: 1 });
            self.candidate_groups
                .sort_by(|left, right| left.value.total_cmp(&right.value));
            return;
        }

        if let Some(last) = self.candidate_groups.last()
            && value < last.value
        {
            self.candidate_groups.pop();
            self.candidate_groups.push(ValueCount { value, count: 1 });
            self.candidate_groups
                .sort_by(|left, right| left.value.total_cmp(&right.value));
        }
    }

    fn finish_pass(mut self) -> Result<AxisPlanner, StreamingClusteringError> {
        if self.next_target_index >= self.targets.len() {
            return Ok(AxisPlan::Quantile(QuantileAxisPlan {
                groups: self.groups,
            })
            .into());
        }
        if self.candidate_groups.is_empty() {
            return Err(unsatisfiable_constraint(
                "quantile cut planning could not advance on the replayed pass",
            ));
        }

        for candidate in self.candidate_groups.drain(..) {
            let group_start = self.consumed_count;
            self.consumed_count += candidate.count;
            let mut quotas = Vec::new();
            while self.next_target_index < self.targets.len()
                && self.targets[self.next_target_index] <= self.consumed_count
            {
                quotas.push(self.targets[self.next_target_index] - group_start);
                self.next_target_index += 1;
            }
            if !quotas.is_empty() {
                self.groups.push(QuantileValueGroup {
                    value: candidate.value,
                    quotas,
                });
            }
            self.lower_bound = Some(candidate.value);
            if self.next_target_index >= self.targets.len() {
                return Ok(AxisPlan::Quantile(QuantileAxisPlan {
                    groups: self.groups,
                })
                .into());
            }
        }

        Ok(self.into())
    }
}

impl DensityValleyMinMaxPlanner {
    fn observe(&mut self, value: f32) {
        self.minimum = Some(self.minimum.map_or(value, |current| current.min(value)));
        self.maximum = Some(self.maximum.map_or(value, |current| current.max(value)));
    }

    fn finish_pass(self) -> AxisPlanner {
        AxisPlanner::DensityHistogram(DensityValleyHistogramPlanner {
            minimum: self.minimum.unwrap_or(0.0),
            maximum: self.maximum.unwrap_or(0.0),
            counts: vec![0; self.bin_count.clamp(2, DENSITY_VALLEY_HISTOGRAM_BUCKET_CAP)],
            bin_count: self.bin_count,
        })
    }
}

impl DensityValleyHistogramPlanner {
    fn observe(&mut self, value: f32) {
        let index = histogram_bucket_index(
            f64::from(self.minimum),
            f64::from(self.maximum),
            self.counts.len(),
            f64::from(value),
        );
        self.counts[index] += 1;
    }

    fn finish_pass(self) -> Result<AxisPlanner, StreamingClusteringError> {
        let cuts = select_histogram_valley_cut_values(
            self.minimum,
            self.maximum,
            self.counts.as_slice(),
            self.bin_count,
        )?;
        Ok(AxisPlan::Thresholds(cuts).into())
    }
}

impl AxisCursorState {
    fn for_partition(partition: &PartitionPlan) -> Self {
        Self {
            quantile_positions: partition
                .axis_plans
                .iter()
                .map(|plan| match plan {
                    AxisPlan::Quantile(quantile) => vec![0; quantile.groups.len()],
                    _ => Vec::new(),
                })
                .collect(),
        }
    }
}

impl PartitionPlan {
    fn assign_point_to_cell(
        &self,
        coordinates: &[f32],
        cursor_state: &mut AxisCursorState,
    ) -> Result<Vec<usize>, StreamingClusteringError> {
        self.axis_plans
            .iter()
            .enumerate()
            .map(|(axis, plan)| match plan {
                AxisPlan::SingleBin => Ok(0),
                AxisPlan::Thresholds(cuts) => {
                    Ok(cuts.partition_point(|cut| coordinates[axis] > *cut))
                }
                AxisPlan::Quantile(quantile) => assign_quantile_bin(
                    coordinates[axis],
                    quantile,
                    cursor_state.quantile_positions[axis].as_mut_slice(),
                ),
            })
            .collect()
    }
}

impl CellStats {
    fn observe(&mut self, embedding: &[f32]) {
        self.count += 1;
        for (sum, value) in self.sums.iter_mut().zip(embedding.iter().copied()) {
            *sum += f64::from(value);
        }
    }

    fn centroid(&self) -> Result<Embedding, StreamingClusteringError> {
        if self.count == 0 {
            return Err(unsatisfiable_constraint(
                "partition-ready cell produced an empty centroid",
            ));
        }
        let count = self.count as f64;
        Ok(self.sums.iter().map(|sum| (*sum / count) as f32).collect())
    }
}

impl CellDuplicateSummary {
    fn new(dimensions: usize) -> Self {
        Self {
            count: 0,
            coordinate_sums: vec![0.0; dimensions],
            coordinate_sum_squares: vec![0.0; dimensions],
        }
    }

    fn observe(&mut self, coordinates: &[f32]) {
        self.count += 1;
        for (index, value) in coordinates.iter().copied().enumerate() {
            let value = f64::from(value);
            self.coordinate_sums[index] += value;
            self.coordinate_sum_squares[index] += value * value;
        }
    }

    fn can_refine_duplicates(&self) -> bool {
        if self.count <= 1 {
            return false;
        }
        const EPSILON: f64 = 1e-9;
        self.coordinate_sums
            .iter()
            .zip(self.coordinate_sum_squares.iter())
            .all(|(sum, sum_squares)| {
                let count = self.count as f64;
                let mean = sum / count;
                let variance = (sum_squares / count) - (mean * mean);
                variance.abs() <= EPSILON
            })
    }
}

impl PassTracker {
    fn new() -> Self {
        Self {
            observed_count: 0,
            hasher: Sha256::new(),
        }
    }

    fn update(&mut self, embedding: &[f32]) {
        self.observed_count += 1;
        self.hasher.update((embedding.len() as u64).to_le_bytes());
        for value in embedding {
            self.hasher.update(value.to_bits().to_le_bytes());
        }
    }

    fn finish(self) -> PassFingerprint {
        PassFingerprint {
            observed_count: self.observed_count,
            digest: self.hasher.finalize().into(),
        }
    }
}

fn analysis_only_report(
    observed_count: usize,
    requested_cluster_count: u32,
    quality_metric: f64,
) -> PassReport {
    PassReport {
        observed_count,
        requested_cluster_count,
        readiness: PassReadiness::AnalysisOnly,
        realized_cluster_count: None,
        quality_metric,
        balance_metric: 0.0,
        quality_direction: MetricDirection::SmallerIsBetter,
        balance_direction: MetricDirection::SmallerIsBetter,
        cluster_ids: None,
    }
}

fn build_ready_partition_plan(
    partition: PartitionPlan,
    cell_summaries: BTreeMap<Vec<usize>, CellDuplicateSummary>,
    requested_cluster_count: usize,
    mode: DirectionalPcaClusterCardinalityMode,
) -> Result<ReadyPartitionPlan, StreamingClusteringError> {
    let populated_cell_count = cell_summaries.len();
    let shortfall = requested_cluster_count.saturating_sub(populated_cell_count);
    let mut remaining_shortfall = shortfall;
    let mut cells = Vec::with_capacity(populated_cell_count);
    let mut cluster_offset = 0usize;

    for (key, summary) in cell_summaries {
        let extra_clusters = if remaining_shortfall > 0 && summary.can_refine_duplicates() {
            let extras = remaining_shortfall.min(summary.count.saturating_sub(1));
            remaining_shortfall -= extras;
            extras
        } else {
            0
        };
        cells.push(ReadyCellPlan {
            key,
            count: summary.count,
            extra_clusters,
            cluster_offset,
        });
        cluster_offset += 1 + extra_clusters;
    }

    if mode == DirectionalPcaClusterCardinalityMode::Exact && remaining_shortfall > 0 {
        return Err(unsatisfiable_constraint(format!(
            "directional-PCA partition realized {populated_cell_count} populated cells instead of the required {requested_cluster_count}"
        )));
    }

    Ok(ReadyPartitionPlan { partition, cells })
}

fn validate_params(
    config: &StreamingClusteringConfig,
    params: &DirectionalPcaParams,
) -> Result<(), StreamingClusteringError> {
    match params.retained_axis_policy {
        DirectionalPcaRetainedAxisPolicy::FixedCount(retained_axis_count) => {
            if retained_axis_count == 0 || retained_axis_count > config.dimensions {
                return Err(invalid_configuration(format!(
                    "retained_axis_policy = FixedCount(n) requires n to be in [1, {}], got {}",
                    config.dimensions, retained_axis_count
                )));
            }
            if params.min_effective_rank > retained_axis_count {
                return Err(invalid_configuration(format!(
                    "min_effective_rank must be in [1, FixedCount(n)={}], got {}",
                    retained_axis_count, params.min_effective_rank
                )));
            }
        }
        DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible => {
            if params.min_effective_rank > config.dimensions {
                return Err(invalid_configuration(format!(
                    "min_effective_rank {} cannot exceed adaptive candidate axis count {}",
                    params.min_effective_rank, config.dimensions
                )));
            }
        }
    }
    if params.allocation_policy == DirectionalPcaAllocationPolicy::EigenvalueLogBits
        && !config.cluster_count.is_power_of_two()
    {
        return Err(invalid_configuration(format!(
            "eigenvalue log-bit allocation requires a power-of-two cluster_count, got {}",
            config.cluster_count
        )));
    }
    if !params.variance_exponent.is_finite() || params.variance_exponent < 0.0 {
        return Err(invalid_configuration(format!(
            "variance_exponent must be finite and non-negative, got {}",
            params.variance_exponent
        )));
    }
    if !params.temperature.is_finite() || params.temperature <= 0.0 {
        return Err(invalid_configuration(format!(
            "temperature must be finite and positive, got {}",
            params.temperature
        )));
    }
    if params.min_input_count < 2 {
        return Err(invalid_configuration(format!(
            "min_input_count must be at least 2, got {}",
            params.min_input_count
        )));
    }
    if params.min_effective_rank == 0 {
        return Err(invalid_configuration(format!(
            "min_effective_rank must be at least 1, got {}",
            params.min_effective_rank
        )));
    }
    if !params.min_cumulative_variance.is_finite()
        || !(0.0..=1.0).contains(&params.min_cumulative_variance)
    {
        return Err(invalid_configuration(format!(
            "min_cumulative_variance must be finite and in [0, 1], got {}",
            params.min_cumulative_variance
        )));
    }
    Ok(())
}

fn resolve_retained_axis_count(
    transform: &PcaTransform,
    params: &DirectionalPcaParams,
    cluster_count: usize,
    effective_rank: usize,
) -> Result<usize, StreamingClusteringError> {
    match params.retained_axis_policy {
        DirectionalPcaRetainedAxisPolicy::FixedCount(retained_axis_count) => {
            Ok(retained_axis_count)
        }
        DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible => {
            let retained_axis_count = effective_rank
                .max(params.min_effective_rank)
                .min(transform.output_dim)
                .max(1);
            let retained_axis_count = if params.allocation_policy
                == DirectionalPcaAllocationPolicy::CentroidWeightedBins
            {
                retained_axis_count.min(cluster_count)
            } else {
                retained_axis_count
            };
            if retained_axis_count < params.min_effective_rank {
                return Err(unsatisfiable_constraint(format!(
                    "adaptive retained axis count {retained_axis_count} is smaller than the required minimum {}",
                    params.min_effective_rank
                )));
            }
            Ok(retained_axis_count)
        }
    }
}

fn reject_balance_constraints(
    config: &StreamingClusteringConfig,
) -> Result<(), StreamingClusteringError> {
    if config.balance_constraints.is_some() {
        return Err(invalid_configuration(
            "balance constraints are not supported by the streaming directional-PCA trainer",
        ));
    }
    Ok(())
}

fn map_pca_error(error: PcaError) -> StreamingClusteringError {
    match error {
        PcaError::DimensionMismatch { .. }
        | PcaError::InvalidTruncationDimension { .. }
        | PcaError::ValidationFailure(_)
        | PcaError::QuantizationConfigurationError(_) => {
            invalid_configuration(format!("directional PCA configuration is invalid: {error}"))
        }
        PcaError::NonFiniteInput { .. } => {
            malformed_input(format!("non-finite PCA input: {error}"))
        }
        PcaError::EmptyInput
        | PcaError::InsufficientSamples { .. }
        | PcaError::DegenerateCovariance { .. }
        | PcaError::DecompositionFailure(_)
        | PcaError::InvalidNumericState(_)
        | PcaError::InvalidSerializedFormat(_)
        | PcaError::SchemaVersionMismatch { .. } => {
            unsatisfiable_constraint(format!("directional PCA analysis failed: {error}"))
        }
    }
}

fn allocate_axis_bins(
    centroid: &[f32],
    transform: &PcaTransform,
    params: &DirectionalPcaParams,
    cluster_count: usize,
) -> Result<Vec<usize>, StreamingClusteringError> {
    let axis_scores = compute_axis_scores(centroid, transform, params)?;
    if axis_scores.is_empty() {
        return Err(invalid_configuration(
            "cannot allocate bins with zero retained dimensions",
        ));
    }
    if params.allocation_policy == DirectionalPcaAllocationPolicy::EigenvalueLogBits {
        return allocate_axis_bins_from_eigenvalue_bits(axis_scores.as_slice(), cluster_count);
    }
    if cluster_count < axis_scores.len() {
        return Err(invalid_configuration(format!(
            "cluster_count {cluster_count} must be at least the retained dimension count {}",
            axis_scores.len()
        )));
    }

    let damped = axis_scores
        .iter()
        .map(|score| (1.0 + score.max(0.0)).ln())
        .collect::<Vec<_>>();
    let temperature = f64::from(params.temperature);
    let max_scaled = damped
        .iter()
        .map(|value| value / temperature)
        .fold(f64::NEG_INFINITY, f64::max);
    let exp_values = damped
        .iter()
        .map(|value| ((value / temperature) - max_scaled).exp())
        .collect::<Vec<_>>();
    let exp_sum = exp_values.iter().sum::<f64>();
    if !exp_sum.is_finite() || exp_sum <= 0.0 {
        return Err(unsatisfiable_constraint(
            "axis-allocation normalization failed",
        ));
    }

    let mut counts = vec![1_usize; axis_scores.len()];
    let remaining_budget = cluster_count - axis_scores.len();
    if remaining_budget == 0 {
        return Ok(counts);
    }

    let desired = exp_values
        .iter()
        .map(|value| value * remaining_budget as f64 / exp_sum)
        .collect::<Vec<_>>();
    let base = desired
        .iter()
        .map(|value| value.floor() as usize)
        .collect::<Vec<_>>();
    for (count, addend) in counts.iter_mut().zip(base.iter().copied()) {
        *count += addend;
    }

    let used = base.iter().sum::<usize>();
    let mut leftovers = remaining_budget - used;
    let mut remainders = desired
        .iter()
        .enumerate()
        .map(|(index, value)| (index, value - value.floor()))
        .collect::<Vec<_>>();
    remainders.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    for (index, _) in remainders {
        if leftovers == 0 {
            break;
        }
        counts[index] += 1;
        leftovers -= 1;
    }

    Ok(counts)
}

fn compute_axis_scores(
    centroid: &[f32],
    transform: &PcaTransform,
    params: &DirectionalPcaParams,
) -> Result<Vec<f64>, StreamingClusteringError> {
    let explained_variance = transform
        .explained_variance()
        .ok_or_else(|| unsatisfiable_constraint("missing explained variance in PCA transform"))?;
    if params.allocation_policy == DirectionalPcaAllocationPolicy::EigenvalueLogBits {
        return explained_variance
            .iter()
            .enumerate()
            .map(|(column, variance)| {
                let lambda = f64::from(*variance).max(0.0);
                let score = lambda.powf(f64::from(params.variance_exponent));
                if !score.is_finite() {
                    return Err(unsatisfiable_constraint(format!(
                        "axis score became non-finite for retained dimension {column}"
                    )));
                }
                Ok(score)
            })
            .collect();
    }

    let gamma = f64::from(params.variance_exponent);
    (0..transform.output_dim)
        .map(|column| {
            let alpha = dot_with_basis_column(centroid, transform, column)?;
            let lambda = f64::from(explained_variance[column]).max(0.0);
            let variance_factor = if gamma == 0.0 {
                1.0
            } else {
                lambda.powf(gamma)
            };
            let score = alpha.abs() * variance_factor;
            if !score.is_finite() {
                return Err(unsatisfiable_constraint(format!(
                    "axis score became non-finite for retained dimension {column}"
                )));
            }
            Ok(score)
        })
        .collect()
}

fn dot_with_basis_column(
    vector: &[f32],
    transform: &PcaTransform,
    column: usize,
) -> Result<f64, StreamingClusteringError> {
    let mut dot = 0.0_f64;
    for (row, value) in vector.iter().copied().enumerate() {
        dot += f64::from(value) * f64::from(transform.basis[row + column * transform.input_dim]);
    }
    if !dot.is_finite() {
        return Err(unsatisfiable_constraint(format!(
            "directional coefficient became non-finite for retained dimension {column}"
        )));
    }
    Ok(dot)
}

fn allocate_axis_bins_from_eigenvalue_bits(
    axis_scores: &[f64],
    cluster_count: usize,
) -> Result<Vec<usize>, StreamingClusteringError> {
    if !cluster_count.is_power_of_two() {
        return Err(invalid_configuration(format!(
            "eigenvalue log-bit allocation requires a power-of-two cluster_count, got {cluster_count}"
        )));
    }
    let total_bits = cluster_count.ilog2() as usize;
    if total_bits == 0 {
        return Ok(vec![1; axis_scores.len()]);
    }

    let mut bit_budget = vec![0usize; axis_scores.len()];
    let log_weights = axis_scores
        .iter()
        .map(|score| (1.0 + score.max(0.0)).ln())
        .collect::<Vec<_>>();
    for _ in 0..total_bits {
        let mut best_axis = 0usize;
        let mut best_weight = f64::NEG_INFINITY;
        for (axis, &weight) in log_weights.iter().enumerate() {
            let adjusted_weight = if bit_budget[axis] == 0 {
                weight
            } else {
                weight / (bit_budget[axis] + 1) as f64
            };
            if adjusted_weight > best_weight || (adjusted_weight == best_weight && axis < best_axis)
            {
                best_axis = axis;
                best_weight = adjusted_weight;
            }
        }
        bit_budget[best_axis] += 1;
    }

    bit_budget
        .into_iter()
        .map(|bits| {
            1usize
                .checked_shl(bits as u32)
                .ok_or_else(|| invalid_configuration("allocated bit budget overflowed"))
        })
        .collect()
}

fn assign_quantile_bin(
    value: f32,
    plan: &QuantileAxisPlan,
    positions: &mut [usize],
) -> Result<usize, StreamingClusteringError> {
    let mut bin = 0usize;
    for (group_index, group) in plan.groups.iter().enumerate() {
        if value < group.value {
            return Ok(bin);
        }
        if value > group.value {
            bin += group.quotas.len();
            continue;
        }

        positions[group_index] += 1;
        let seen = positions[group_index];
        for quota in &group.quotas {
            if seen > *quota {
                bin += 1;
            } else {
                break;
            }
        }
        return Ok(bin);
    }
    Ok(bin)
}

fn select_histogram_valley_cut_values(
    minimum: f32,
    maximum: f32,
    counts: &[usize],
    bin_count: usize,
) -> Result<Vec<f32>, StreamingClusteringError> {
    if minimum >= maximum || bin_count <= 1 {
        return Ok(Vec::new());
    }

    let smoothed = smooth_histogram(counts);
    let mut segments = vec![(0usize, counts.len())];
    let mut cuts = Vec::new();
    while cuts.len() < bin_count.saturating_sub(1) {
        let mut best: Option<(usize, usize, f64, f64)> = None;
        for (segment_index, &(start, end)) in segments.iter().enumerate() {
            if end.saturating_sub(start) <= 1 {
                continue;
            }
            if let Some((bucket, density, depth)) = best_bucket_valley(&smoothed, start, end) {
                match best {
                    None => best = Some((segment_index, bucket, density, depth)),
                    Some((_, best_bucket, best_density, best_depth)) => {
                        if depth > best_depth
                            || (depth == best_depth
                                && (density < best_density
                                    || (density == best_density && bucket < best_bucket)))
                        {
                            best = Some((segment_index, bucket, density, depth));
                        }
                    }
                }
            }
        }
        let Some((segment_index, bucket, _, _)) = best else {
            break;
        };
        let (start, end) = segments.remove(segment_index);
        segments.push((start, bucket));
        segments.push((bucket, end));
        segments.sort_unstable();
        cuts.push(bucket_edge_value(minimum, maximum, counts.len(), bucket));
    }
    cuts.sort_by(|left, right| left.total_cmp(right));
    if cuts.is_empty() && bin_count > 1 {
        return Err(unsatisfiable_constraint(
            "density-valley partitioning could not realize any deterministic cuts",
        ));
    }
    Ok(cuts)
}

fn smooth_histogram(counts: &[usize]) -> Vec<f64> {
    let mut smoothed = vec![0.0; counts.len()];
    for index in 0..counts.len() {
        let left = if index > 0 {
            counts[index - 1] as f64
        } else {
            counts[index] as f64
        };
        let center = counts[index] as f64;
        let right = if index + 1 < counts.len() {
            counts[index + 1] as f64
        } else {
            counts[index] as f64
        };
        smoothed[index] = left + (2.0 * center) + right;
    }
    smoothed
}

fn best_bucket_valley(smoothed: &[f64], start: usize, end: usize) -> Option<(usize, f64, f64)> {
    if end.saturating_sub(start) <= 1 {
        return None;
    }
    let mut left_peaks = Vec::with_capacity(end - start);
    let mut running_left = f64::NEG_INFINITY;
    for density in smoothed[start..end].iter().copied() {
        running_left = running_left.max(density);
        left_peaks.push(running_left);
    }
    let mut right_peaks = vec![f64::NEG_INFINITY; end - start];
    let mut running_right = f64::NEG_INFINITY;
    for (offset, density) in smoothed[start..end].iter().copied().enumerate().rev() {
        running_right = running_right.max(density);
        right_peaks[offset] = running_right;
    }

    let mut best = None;
    for bucket in (start + 1)..end {
        let relative = bucket - start;
        let valley_density = 0.5 * (smoothed[bucket - 1] + smoothed[bucket]);
        let valley_depth = left_peaks[relative - 1].min(right_peaks[relative]) - valley_density;
        match best {
            None => best = Some((bucket, valley_density, valley_depth)),
            Some((best_bucket, best_density, best_depth)) => {
                if valley_depth > best_depth
                    || (valley_depth == best_depth
                        && (valley_density < best_density
                            || (valley_density == best_density && bucket < best_bucket)))
                {
                    best = Some((bucket, valley_density, valley_depth));
                }
            }
        }
    }
    best
}

fn bucket_edge_value(minimum: f32, maximum: f32, bucket_count: usize, bucket: usize) -> f32 {
    let fraction = bucket as f64 / bucket_count as f64;
    (f64::from(minimum) + (f64::from(maximum) - f64::from(minimum)) * fraction) as f32
}

fn histogram_bucket_index(minimum: f64, maximum: f64, bucket_count: usize, value: f64) -> usize {
    if bucket_count <= 1 || maximum <= minimum {
        return 0;
    }
    let normalized = ((value - minimum) / (maximum - minimum)).clamp(0.0, 1.0);
    (normalized * (bucket_count - 1) as f64).round() as usize
}

fn quality_metric_from_transform(transform: &PcaTransform) -> f64 {
    1.0 - f64::from(
        transform
            .cumulative_variance()
            .and_then(|values| values.last().copied())
            .unwrap_or(0.0),
    )
}

fn squared_distance(left: &[f32], right: &[f32]) -> Result<f64, StreamingClusteringError> {
    if left.len() != right.len() {
        return Err(malformed_input(format!(
            "distance calculation requires matching dimensions, got {} and {}",
            left.len(),
            right.len()
        )));
    }
    let mut total = 0.0_f64;
    for (index, (l, r)) in left.iter().zip(right.iter()).enumerate() {
        let delta = f64::from(*l) - f64::from(*r);
        total += delta * delta;
        if !total.is_finite() {
            return Err(unsatisfiable_constraint(format!(
                "distance became non-finite at dimension {index}"
            )));
        }
    }
    Ok(total)
}

fn div_ceil(numerator: usize, denominator: usize) -> usize {
    numerator.div_ceil(denominator)
}

fn invalid_configuration(message: impl Into<String>) -> StreamingClusteringError {
    StreamingClusteringError::InvalidConfiguration {
        message: message.into(),
    }
}

fn unsatisfiable_constraint(message: impl Into<String>) -> StreamingClusteringError {
    StreamingClusteringError::UnsatisfiableConstraint {
        message: message.into(),
    }
}

fn malformed_input(message: impl Into<String>) -> StreamingClusteringError {
    StreamingClusteringError::MalformedInput {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> DirectionalPcaParams {
        DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(2),
            allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        }
    }

    #[test]
    fn axis_scoring_uses_direction_and_variance() {
        let embeddings = vec![
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![10.0, 1.0],
            vec![11.0, 1.0],
        ];
        let mut accumulator = PcaAccumulator::new(2);
        for embedding in &embeddings {
            accumulator.update(embedding).unwrap();
        }
        let transform = accumulator.finalize().unwrap().truncate(2).unwrap();
        let scores = compute_axis_scores(transform.mean.as_slice(), &transform, &params()).unwrap();
        assert_eq!(scores.len(), 2);
        assert!(scores[0] > scores[1]);
    }

    #[test]
    fn quantile_assignment_splits_equal_values_by_stable_rank() {
        let plan = QuantileAxisPlan {
            groups: vec![QuantileValueGroup {
                value: 1.0,
                quotas: vec![2, 3],
            }],
        };
        let mut positions = vec![0usize];
        let bins = [1.0, 1.0, 1.0, 1.0]
            .into_iter()
            .map(|value| assign_quantile_bin(value, &plan, positions.as_mut_slice()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(bins, vec![0, 0, 1, 2]);
    }

    #[test]
    fn density_valley_cuts_choose_the_deep_valley() {
        let cuts =
            select_histogram_valley_cut_values(-1.0, 11.0, &[4, 4, 1, 0, 1, 4, 4], 2).unwrap();
        assert_eq!(cuts.len(), 1);
        assert!(cuts[0] > 3.0);
        assert!(cuts[0] < 7.0);
    }
}
