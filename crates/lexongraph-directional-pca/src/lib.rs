// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Streaming directional-PCA clustering for LexonGraph.

use std::cell::RefCell;
use std::collections::BTreeMap;

use lexongraph_pca::{PcaAccumulator, PcaError, PcaTransform};
use lexongraph_streaming_clustering::{
    ClusterId, Embedding, MetricDirection, PassReadiness, PassReport, StreamingClusterClassifier,
    StreamingClusterTrainer, StreamingClusteringConfig, StreamingClusteringError, TrainerState,
    validate_config, validate_embedding,
};
use rayon::prelude::*;
use sha2::{Digest, Sha256};

pub const DIRECTIONAL_PCA_SOFTWARE_IDENTITY: &str =
    concat!("lexongraph-directional-pca-v", env!("CARGO_PKG_VERSION"));

const DENSITY_VALLEY_HISTOGRAM_BUCKET_CAP: usize = 256;
const GK_QUANTILE_RANK_ERROR_DENOMINATOR: usize = 1024;
const MIN_PARALLEL_PROJECTION_BATCH_LEN: usize = 64;

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

pub trait DirectionalPcaOutOfCorePlannerState: Send {
    fn begin_quantile_pass(
        &mut self,
        axis_count: usize,
        expected_value_count: usize,
    ) -> Result<(), String>;

    fn append_quantile_values(&mut self, values: &[f32]) -> Result<(), String>;

    fn finish_quantile_pass(&mut self) -> Result<(), String>;

    fn scan_quantile_axis(
        &self,
        axis_index: usize,
        observe: &mut dyn FnMut(f32) -> Result<(), String>,
    ) -> Result<(), String>;

    fn clear_quantile_pass(&mut self) -> Result<(), String>;
}

pub struct DirectionalPcaStreamingTrainer {
    config: StreamingClusteringConfig,
    params: DirectionalPcaParams,
    state: TrainerState,
    phase: ReplayPhase,
    active_pass: Option<ActivePassState>,
    baseline_fingerprint: Option<PassFingerprint>,
    quality_metric: f64,
    model: Option<DirectionalPcaModel>,
    cached_telemetry: RefCell<Option<DirectionalPcaTrainerTelemetry>>,
    out_of_core_state: Option<Box<dyn DirectionalPcaOutOfCorePlannerState>>,
    projection_execution: ProjectionExecution,
}

impl std::fmt::Debug for DirectionalPcaStreamingTrainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectionalPcaStreamingTrainer")
            .field("config", &self.config)
            .field("params", &self.params)
            .field("state", &self.state)
            .field("phase", &self.phase)
            .field("active_pass", &self.active_pass)
            .field("baseline_fingerprint", &self.baseline_fingerprint)
            .field("quality_metric", &self.quality_metric)
            .field("model", &self.model)
            .field("has_out_of_core_state", &self.out_of_core_state.is_some())
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct DirectionalPcaStreamingClassifier {
    config: StreamingClusteringConfig,
    centroids: Vec<Embedding>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectionalPcaTrainerSubphase {
    AnalyzePca,
    PlanCuts,
    CountCells,
    RealizePartition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectionalPcaTrainerTelemetry {
    pub subphase: DirectionalPcaTrainerSubphase,
    pub observed_count: Option<usize>,
    pub ready_axis_plan_count: Option<usize>,
    pub total_axis_plan_count: Option<usize>,
    pub populated_cell_count: Option<usize>,
    pub realized_cell_count: Option<usize>,
    pub state_fingerprint: [u8; 32],
}

#[derive(Clone, Debug)]
struct DirectionalPcaModel {
    centroids: Vec<Embedding>,
}

#[derive(Debug)]
enum ReplayPhase {
    AnalyzePca,
    PlanCuts(CutPlanningReplayPlan),
    CountCells(PartitionPlan),
    RealizePartition(ReadyPartitionPlan),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProjectionExecution {
    Auto,
    #[cfg(test)]
    Serial,
    #[cfg(test)]
    Parallel,
}

#[derive(Clone, Debug)]
struct PartitionAnalysisPlan {
    transform: PcaTransform,
    axis_bin_counts: Vec<usize>,
    binning_policy: DirectionalPcaBinningPolicy,
    total_count: usize,
}

#[derive(Clone, Debug)]
struct CutPlanningReplayPlan {
    plan: PartitionAnalysisPlan,
    planners: Vec<AxisPlanner>,
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
    Thresholds(Vec<f32>),
}

#[derive(Clone, Debug)]
enum AxisPlanner {
    Quantile(QuantileAxisPlanner),
    DensityMinMax(DensityValleyMinMaxPlanner),
    DensityHistogram(DensityValleyHistogramPlanner),
    Ready(AxisPlan),
}

#[derive(Clone, Debug)]
struct QuantileAxisPlanner {
    targets: Vec<usize>,
    observed_count: usize,
    max_rank_error_denominator: usize,
    summary: Vec<GkSummaryEntry>,
    compress_interval: usize,
}

#[derive(Clone, Debug)]
struct DensityValleyMinMaxPlanner {
    bin_count: usize,
    minimum: Option<f32>,
    maximum: Option<f32>,
}

#[derive(Clone, Debug)]
struct DensityValleyHistogramPlanner {
    minimum: f32,
    maximum: f32,
    counts: Vec<usize>,
    bin_count: usize,
}

#[derive(Clone, Copy, Debug)]
struct GkSummaryEntry {
    value: f32,
    gap: usize,
    delta: usize,
}

#[derive(Clone, Debug, Default)]
struct AxisCursorState;

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

struct DirectionalPcaTelemetryDetail {
    ready_axis_plan_count: Option<usize>,
    total_axis_plan_count: Option<usize>,
    populated_cell_count: Option<usize>,
    realized_cell_count: Option<usize>,
    state_fingerprint: [u8; 32],
}

fn directional_pca_telemetry_detail(
    phase: &ReplayPhase,
    active_pass: Option<&ActivePassState>,
) -> DirectionalPcaTelemetryDetail {
    match active_pass {
        Some(ActivePassState::AnalyzePca(pass)) => DirectionalPcaTelemetryDetail {
            ready_axis_plan_count: None,
            total_axis_plan_count: None,
            populated_cell_count: None,
            realized_cell_count: None,
            state_fingerprint: hash_active_pca_pass(pass),
        },
        Some(ActivePassState::PlanCuts(pass)) => DirectionalPcaTelemetryDetail {
            ready_axis_plan_count: Some(
                pass.planners
                    .iter()
                    .filter(|planner| matches!(planner, AxisPlanner::Ready(_)))
                    .count(),
            ),
            total_axis_plan_count: Some(pass.planners.len()),
            populated_cell_count: None,
            realized_cell_count: None,
            state_fingerprint: hash_active_cut_planning_pass(pass),
        },
        Some(ActivePassState::CountCells(pass)) => DirectionalPcaTelemetryDetail {
            ready_axis_plan_count: Some(pass.partition.axis_plans.len()),
            total_axis_plan_count: Some(pass.partition.axis_plans.len()),
            populated_cell_count: Some(pass.cell_summaries.len()),
            realized_cell_count: None,
            state_fingerprint: hash_active_cell_counting_pass(pass),
        },
        Some(ActivePassState::RealizePartition(pass)) => DirectionalPcaTelemetryDetail {
            ready_axis_plan_count: Some(pass.ready.partition.axis_plans.len()),
            total_axis_plan_count: Some(pass.ready.partition.axis_plans.len()),
            populated_cell_count: Some(pass.ready.cells.len()),
            realized_cell_count: Some(pass.cell_stats.len()),
            state_fingerprint: hash_active_partition_realization_pass(pass),
        },
        None => telemetry_detail_from_phase(phase),
    }
}

fn telemetry_detail_from_phase(phase: &ReplayPhase) -> DirectionalPcaTelemetryDetail {
    match phase {
        ReplayPhase::AnalyzePca => DirectionalPcaTelemetryDetail {
            ready_axis_plan_count: None,
            total_axis_plan_count: None,
            populated_cell_count: None,
            realized_cell_count: None,
            state_fingerprint: hash_replay_phase(phase),
        },
        ReplayPhase::PlanCuts(replay) => DirectionalPcaTelemetryDetail {
            ready_axis_plan_count: Some(
                replay
                    .planners
                    .iter()
                    .filter(|planner| matches!(planner, AxisPlanner::Ready(_)))
                    .count(),
            ),
            total_axis_plan_count: Some(replay.planners.len()),
            populated_cell_count: None,
            realized_cell_count: None,
            state_fingerprint: hash_replay_phase(phase),
        },
        ReplayPhase::CountCells(partition) => DirectionalPcaTelemetryDetail {
            ready_axis_plan_count: Some(partition.axis_plans.len()),
            total_axis_plan_count: Some(partition.axis_plans.len()),
            populated_cell_count: None,
            realized_cell_count: None,
            state_fingerprint: hash_replay_phase(phase),
        },
        ReplayPhase::RealizePartition(ready) => DirectionalPcaTelemetryDetail {
            ready_axis_plan_count: Some(ready.partition.axis_plans.len()),
            total_axis_plan_count: Some(ready.partition.axis_plans.len()),
            populated_cell_count: Some(ready.cells.len()),
            realized_cell_count: None,
            state_fingerprint: hash_replay_phase(phase),
        },
    }
}

fn hash_active_pca_pass(pass: &ActivePcaPass) -> [u8; 32] {
    hash_with(|hasher| {
        hasher.update(b"active-analyze-pca");
        hash_pass_tracker(hasher, &pass.tracker);
    })
}

fn hash_active_cut_planning_pass(pass: &ActiveCutPlanningPass) -> [u8; 32] {
    hash_with(|hasher| {
        hasher.update(b"active-plan-cuts");
        hash_pass_tracker(hasher, &pass.tracker);
        hash_partition_analysis_plan(hasher, &pass.plan);
        hash_axis_planners(hasher, &pass.planners);
    })
}

fn hash_active_cell_counting_pass(pass: &ActiveCellCountingPass) -> [u8; 32] {
    hash_with(|hasher| {
        hasher.update(b"active-count-cells");
        hash_pass_tracker(hasher, &pass.tracker);
        hash_partition_plan(hasher, &pass.partition);
        hash_axis_cursor_state(hasher, &pass.cursor_state);
        hash_cell_duplicate_summaries(hasher, &pass.cell_summaries);
    })
}

fn hash_active_partition_realization_pass(pass: &ActivePartitionRealizationPass) -> [u8; 32] {
    hash_with(|hasher| {
        hasher.update(b"active-realize-partition");
        hash_pass_tracker(hasher, &pass.tracker);
        hash_ready_partition_plan(hasher, &pass.ready);
        hash_axis_cursor_state(hasher, &pass.cursor_state);
        hash_usizes(hasher, &pass.cell_seen_counts);
        hash_cell_stats(hasher, &pass.cell_stats);
    })
}

fn hash_replay_phase(phase: &ReplayPhase) -> [u8; 32] {
    hash_with(|hasher| match phase {
        ReplayPhase::AnalyzePca => {
            hasher.update(b"phase-analyze-pca");
        }
        ReplayPhase::PlanCuts(replay) => {
            hasher.update(b"phase-plan-cuts");
            hash_partition_analysis_plan(hasher, &replay.plan);
            hash_axis_planners(hasher, &replay.planners);
        }
        ReplayPhase::CountCells(partition) => {
            hasher.update(b"phase-count-cells");
            hash_partition_plan(hasher, partition);
        }
        ReplayPhase::RealizePartition(ready) => {
            hasher.update(b"phase-realize-partition");
            hash_ready_partition_plan(hasher, ready);
        }
    })
}

fn hash_partition_analysis_plan(hasher: &mut Sha256, plan: &PartitionAnalysisPlan) {
    hash_pca_transform(hasher, &plan.transform);
    hash_usizes(hasher, &plan.axis_bin_counts);
    hash_usize(hasher, plan.total_count);
    hash_usize(
        hasher,
        match plan.binning_policy {
            DirectionalPcaBinningPolicy::Quantile => 0,
            DirectionalPcaBinningPolicy::DensityValley => 1,
        },
    );
}

fn hash_partition_plan(hasher: &mut Sha256, plan: &PartitionPlan) {
    hash_pca_transform(hasher, &plan.transform);
    hash_usize(hasher, plan.axis_plans.len());
    for axis_plan in &plan.axis_plans {
        hash_axis_plan(hasher, axis_plan);
    }
}

fn hash_ready_partition_plan(hasher: &mut Sha256, ready: &ReadyPartitionPlan) {
    hash_partition_plan(hasher, &ready.partition);
    hash_usize(hasher, ready.cells.len());
    for cell in &ready.cells {
        hash_usizes(hasher, &cell.key);
        hash_usize(hasher, cell.count);
        hash_usize(hasher, cell.extra_clusters);
        hash_usize(hasher, cell.cluster_offset);
    }
}

fn hash_axis_planners(hasher: &mut Sha256, planners: &[AxisPlanner]) {
    hash_usize(hasher, planners.len());
    for planner in planners {
        hash_axis_planner(hasher, planner);
    }
}

fn hash_axis_planner(hasher: &mut Sha256, planner: &AxisPlanner) {
    match planner {
        AxisPlanner::Quantile(planner) => {
            hasher.update(b"planner-quantile");
            hash_usizes(hasher, &planner.targets);
            hash_usize(hasher, planner.observed_count);
            hash_usize(hasher, planner.max_rank_error_denominator);
            hash_usize(hasher, planner.summary.len());
            for entry in &planner.summary {
                hash_f32(hasher, entry.value);
                hash_usize(hasher, entry.gap);
                hash_usize(hasher, entry.delta);
            }
            hash_usize(hasher, planner.compress_interval);
        }
        AxisPlanner::DensityMinMax(planner) => {
            hasher.update(b"planner-density-minmax");
            hash_usize(hasher, planner.bin_count);
            hash_optional_f32(hasher, planner.minimum);
            hash_optional_f32(hasher, planner.maximum);
        }
        AxisPlanner::DensityHistogram(planner) => {
            hasher.update(b"planner-density-histogram");
            hash_f32(hasher, planner.minimum);
            hash_f32(hasher, planner.maximum);
            hash_usize(hasher, planner.bin_count);
            hash_usizes(hasher, &planner.counts);
        }
        AxisPlanner::Ready(plan) => {
            hasher.update(b"planner-ready");
            hash_axis_plan(hasher, plan);
        }
    }
}

fn hash_axis_plan(hasher: &mut Sha256, plan: &AxisPlan) {
    match plan {
        AxisPlan::SingleBin => {
            hasher.update(b"axis-plan-single-bin");
        }
        AxisPlan::Thresholds(thresholds) => {
            hasher.update(b"axis-plan-thresholds");
            hash_usize(hasher, thresholds.len());
            for threshold in thresholds {
                hash_f32(hasher, *threshold);
            }
        }
    }
}

fn hash_axis_cursor_state(hasher: &mut Sha256, cursor_state: &AxisCursorState) {
    let _ = cursor_state;
    hasher.update(b"axis-cursor-state");
}

fn hash_cell_duplicate_summaries(
    hasher: &mut Sha256,
    cell_summaries: &BTreeMap<Vec<usize>, CellDuplicateSummary>,
) {
    hash_usize(hasher, cell_summaries.len());
    for (key, summary) in cell_summaries {
        hash_usizes(hasher, key);
        hash_usize(hasher, summary.count);
        hash_f64s(hasher, &summary.coordinate_sums);
        hash_f64s(hasher, &summary.coordinate_sum_squares);
    }
}

fn hash_cell_stats(hasher: &mut Sha256, stats: &[CellStats]) {
    hash_usize(hasher, stats.len());
    for cell in stats {
        hash_usize(hasher, cell.count);
        hash_f64s(hasher, &cell.sums);
    }
}

fn hash_pca_transform(hasher: &mut Sha256, transform: &PcaTransform) {
    hash_usize(hasher, transform.input_dim);
    hash_usize(hasher, transform.output_dim);
    hash_f32s(hasher, &transform.mean);
    hash_f32s(hasher, &transform.basis);
    match &transform.explained_variance {
        Some(explained_variance) => {
            hasher.update([1]);
            hash_f32s(hasher, explained_variance);
        }
        None => hasher.update([0]),
    }
    hasher.update(transform.schema_version.to_le_bytes());
}

fn hash_pass_tracker(hasher: &mut Sha256, tracker: &PassTracker) {
    hash_usize(hasher, tracker.observed_count);
    hasher.update(tracker.hasher.clone().finalize());
}

fn hash_with(update: impl FnOnce(&mut Sha256)) -> [u8; 32] {
    let mut hasher = Sha256::new();
    update(&mut hasher);
    hasher.finalize().into()
}

fn hash_usize(hasher: &mut Sha256, value: usize) {
    hasher.update((value as u64).to_le_bytes());
}

fn hash_usizes(hasher: &mut Sha256, values: &[usize]) {
    hash_usize(hasher, values.len());
    for value in values {
        hash_usize(hasher, *value);
    }
}

fn hash_f32(hasher: &mut Sha256, value: f32) {
    hasher.update(value.to_bits().to_le_bytes());
}

fn hash_optional_f32(hasher: &mut Sha256, value: Option<f32>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hash_f32(hasher, value);
        }
        None => hasher.update([0]),
    }
}

fn hash_f32s(hasher: &mut Sha256, values: &[f32]) {
    hash_usize(hasher, values.len());
    for value in values {
        hash_f32(hasher, *value);
    }
}

fn hash_f64s(hasher: &mut Sha256, values: &[f64]) {
    hash_usize(hasher, values.len());
    for value in values {
        hasher.update(value.to_bits().to_le_bytes());
    }
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
            cached_telemetry: RefCell::new(None),
            out_of_core_state: None,
            projection_execution: ProjectionExecution::Auto,
        })
    }

    pub fn with_out_of_core_planner_state(
        mut self,
        out_of_core_state: Box<dyn DirectionalPcaOutOfCorePlannerState>,
    ) -> Self {
        self.out_of_core_state = Some(out_of_core_state);
        self
    }

    #[cfg(test)]
    fn with_projection_execution(mut self, projection_execution: ProjectionExecution) -> Self {
        self.projection_execution = projection_execution;
        self
    }

    fn invalidate_cached_telemetry(&self) {
        self.cached_telemetry.replace(None);
    }

    fn invalid_transition(&mut self, operation: &str) -> StreamingClusteringError {
        let state = self.state;
        self.state = TrainerState::Error;
        self.invalidate_cached_telemetry();
        StreamingClusteringError::InvalidTransition {
            state,
            operation: operation.into(),
        }
    }

    fn fail(&mut self, error: StreamingClusteringError) -> StreamingClusteringError {
        self.state = TrainerState::Error;
        self.active_pass = None;
        self.invalidate_cached_telemetry();
        error
    }

    pub fn telemetry(&self) -> DirectionalPcaTrainerTelemetry {
        if let Some(cached) = self.cached_telemetry.borrow().as_ref() {
            return *cached;
        }
        let subphase = match self.phase {
            ReplayPhase::AnalyzePca => DirectionalPcaTrainerSubphase::AnalyzePca,
            ReplayPhase::PlanCuts(_) => DirectionalPcaTrainerSubphase::PlanCuts,
            ReplayPhase::CountCells(_) => DirectionalPcaTrainerSubphase::CountCells,
            ReplayPhase::RealizePartition(_) => DirectionalPcaTrainerSubphase::RealizePartition,
        };
        let observed_count = self
            .active_pass
            .as_ref()
            .map(|active_pass| match active_pass {
                ActivePassState::AnalyzePca(pass) => pass.tracker.observed_count,
                ActivePassState::PlanCuts(pass) => pass.tracker.observed_count,
                ActivePassState::CountCells(pass) => pass.tracker.observed_count,
                ActivePassState::RealizePartition(pass) => pass.tracker.observed_count,
            });
        let detail = directional_pca_telemetry_detail(&self.phase, self.active_pass.as_ref());
        let telemetry = DirectionalPcaTrainerTelemetry {
            subphase,
            observed_count,
            ready_axis_plan_count: detail.ready_axis_plan_count,
            total_axis_plan_count: detail.total_axis_plan_count,
            populated_cell_count: detail.populated_cell_count,
            realized_cell_count: detail.realized_cell_count,
            state_fingerprint: detail.state_fingerprint,
        };
        self.cached_telemetry.replace(Some(telemetry));
        telemetry
    }

    fn ensure_active_pass(&mut self) -> Result<(), StreamingClusteringError> {
        if self.active_pass.is_some() {
            return Ok(());
        }
        let active_pass = match &self.phase {
            ReplayPhase::AnalyzePca => ActivePassState::AnalyzePca(ActivePcaPass {
                tracker: PassTracker::new(),
                accumulator: PcaAccumulator::new(self.config.dimensions),
            }),
            ReplayPhase::PlanCuts(replay) => ActivePassState::PlanCuts(ActiveCutPlanningPass {
                tracker: PassTracker::new(),
                plan: replay.plan.clone(),
                planners: replay.planners.clone(),
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
        };
        self.active_pass = Some(active_pass);
        self.invalidate_cached_telemetry();
        Ok(())
    }

    fn finish_pass_impl(&mut self) -> Result<PassReport, StreamingClusteringError> {
        if self.state != TrainerState::Ingesting {
            return Err(self.invalid_transition("finish_pass"));
        }
        self.invalidate_cached_telemetry();
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
        let plan = PartitionAnalysisPlan {
            transform,
            axis_bin_counts,
            binning_policy: self.params.binning_policy,
            total_count: observed_count,
        };
        let planners = plan
            .axis_bin_counts
            .iter()
            .map(|&bin_count| match plan.binning_policy {
                DirectionalPcaBinningPolicy::Quantile => {
                    AxisPlanner::new_quantile(bin_count, plan.total_count)
                }
                DirectionalPcaBinningPolicy::DensityValley => AxisPlanner::new_density(bin_count),
            })
            .collect();
        Ok((
            ReplayPhase::PlanCuts(CutPlanningReplayPlan { plan, planners }),
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
            Ok((
                ReplayPhase::PlanCuts(CutPlanningReplayPlan {
                    plan: pass.plan,
                    planners: advanced_planners,
                }),
                report,
                None,
            ))
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
        let dimensions = self.config.dimensions;
        let projection_execution = self.projection_execution;
        match self.state {
            TrainerState::Idle | TrainerState::PassComplete => {
                self.ensure_active_pass()?;
                self.state = TrainerState::Ingesting;
            }
            TrainerState::Ingesting => {}
            TrainerState::TrainingComplete | TrainerState::Error => {
                return Err(self.invalid_transition("ingest_batch"));
            }
        }
        self.invalidate_cached_telemetry();
        let active_pass = self
            .active_pass
            .as_mut()
            .ok_or_else(|| malformed_input("missing active directional-PCA pass state"))?;
        match active_pass {
            ActivePassState::AnalyzePca(pass) => {
                validate_and_track_batch(&mut pass.tracker, embeddings, dimensions)?;
                for embedding in embeddings {
                    pass.accumulator.update(embedding).map_err(map_pca_error)?;
                }
            }
            ActivePassState::PlanCuts(pass) => {
                validate_and_track_batch(&mut pass.tracker, embeddings, dimensions)?;
                let coordinates = project_embeddings_in_replay_order(
                    &pass.plan.transform,
                    embeddings,
                    projection_execution,
                )?;
                for axis_coordinates in coordinates {
                    for (planner, value) in pass.planners.iter_mut().zip(axis_coordinates) {
                        planner.observe(value);
                    }
                }
            }
            ActivePassState::CountCells(pass) => {
                validate_and_track_batch(&mut pass.tracker, embeddings, dimensions)?;
                let coordinates = project_embeddings_in_replay_order(
                    &pass.partition.transform,
                    embeddings,
                    projection_execution,
                )?;
                for axis_coordinates in coordinates {
                    let key = pass.partition.assign_point_to_cell(
                        axis_coordinates.as_slice(),
                        &mut pass.cursor_state,
                    )?;
                    pass.cell_summaries
                        .entry(key)
                        .or_insert_with(|| {
                            CellDuplicateSummary::new(pass.partition.transform.output_dim)
                        })
                        .observe(axis_coordinates.as_slice());
                }
            }
            ActivePassState::RealizePartition(pass) => {
                validate_and_track_batch(&mut pass.tracker, embeddings, dimensions)?;
                let coordinates = project_embeddings_in_replay_order(
                    &pass.ready.partition.transform,
                    embeddings,
                    projection_execution,
                )?;
                for (embedding, axis_coordinates) in embeddings.iter().zip(coordinates) {
                    let key = pass.ready.partition.assign_point_to_cell(
                        axis_coordinates.as_slice(),
                        &mut pass.cursor_state,
                    )?;
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
        if self.state != TrainerState::PassComplete {
            return Err(self.invalid_transition("complete_training"));
        }
        if self.model.is_none() {
            return Err(StreamingClusteringError::InvalidTransition {
                state: self.state,
                operation: "complete_training".into(),
            });
        }
        self.state = TrainerState::TrainingComplete;
        self.invalidate_cached_telemetry();
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
    fn new_quantile(bin_count: usize, total_count: usize) -> Self {
        if bin_count <= 1 {
            return Self::Ready(AxisPlan::SingleBin);
        }
        let targets = (1..bin_count)
            .map(|bin| {
                usize::try_from(div_ceil_u128(
                    (bin as u128) * (total_count as u128),
                    bin_count as u128,
                ))
                .expect("quantile target rank should fit usize")
            })
            .collect();
        Self::Quantile(QuantileAxisPlanner {
            targets,
            observed_count: 0,
            max_rank_error_denominator: GK_QUANTILE_RANK_ERROR_DENOMINATOR,
            summary: Vec::new(),
            compress_interval: GK_QUANTILE_RANK_ERROR_DENOMINATOR / 2,
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
        self.observed_count += 1;
        let insert_index = self
            .summary
            .partition_point(|entry| entry.value.total_cmp(&value).is_le());
        let delta = if insert_index == 0 || insert_index == self.summary.len() {
            0
        } else {
            self.compress_threshold().saturating_sub(1)
        };
        self.summary.insert(
            insert_index,
            GkSummaryEntry {
                value,
                gap: 1,
                delta,
            },
        );
        if self.compress_interval > 0 && self.observed_count.is_multiple_of(self.compress_interval)
        {
            self.compress();
        }
    }

    fn finish_pass(mut self) -> Result<AxisPlanner, StreamingClusteringError> {
        if self.summary.is_empty() {
            return Err(unsatisfiable_constraint(
                "quantile cut planning observed no retained-axis coordinates",
            ));
        }
        self.compress();
        let cuts = self
            .targets
            .iter()
            .copied()
            .map(|target_rank| self.approximate_quantile_value(target_rank))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(AxisPlan::Thresholds(cuts).into())
    }

    fn compress_threshold(&self) -> usize {
        self.observed_count
            .saturating_mul(2)
            .saturating_div(self.max_rank_error_denominator.max(1))
            .max(1)
    }

    fn rank_error(&self) -> usize {
        self.observed_count
            .div_ceil(self.max_rank_error_denominator.max(1))
    }

    fn compress(&mut self) {
        if self.summary.len() < 3 {
            return;
        }
        let threshold = self.compress_threshold();
        let mut compacted = Vec::with_capacity(self.summary.len());
        let mut merged_right = *self.summary.last().expect("summary length checked above");
        for index in (1..self.summary.len() - 1).rev() {
            let left = self.summary[index];
            if left.gap + merged_right.gap + merged_right.delta <= threshold {
                merged_right.gap += left.gap;
            } else {
                compacted.push(merged_right);
                merged_right = left;
            }
        }
        compacted.push(merged_right);
        compacted.push(self.summary[0]);
        compacted.reverse();
        self.summary = compacted;
    }

    fn approximate_quantile_value(
        &self,
        target_rank: usize,
    ) -> Result<f32, StreamingClusteringError> {
        let rank_error = self.rank_error();
        let minimum_rank = target_rank.saturating_sub(rank_error).max(1);
        let maximum_rank = target_rank
            .saturating_add(rank_error)
            .min(self.observed_count.max(1));
        let mut running_min_rank = 0usize;
        let mut best_overlap = None;
        let mut best_overlap_distance = usize::MAX;
        let mut fallback_value = None;
        let mut fallback_distance = usize::MAX;
        for entry in &self.summary {
            running_min_rank += entry.gap;
            let running_max_rank = running_min_rank + entry.delta;
            let center_rank = running_min_rank + entry.delta / 2;
            let distance_to_target = center_rank.abs_diff(target_rank);
            if running_min_rank <= maximum_rank
                && running_max_rank >= minimum_rank
                && distance_to_target < best_overlap_distance
            {
                best_overlap_distance = distance_to_target;
                best_overlap = Some(entry.value);
            }
            let distance = if running_max_rank < minimum_rank {
                minimum_rank - running_max_rank
            } else {
                running_min_rank.saturating_sub(maximum_rank)
            };
            if distance < fallback_distance {
                fallback_distance = distance;
                fallback_value = Some(entry.value);
            }
        }
        if let Some(value) = best_overlap {
            return Ok(value);
        }
        fallback_value.ok_or_else(|| {
            unsatisfiable_constraint("quantile cut planning could not derive an approximate cut")
        })
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
        let _ = partition;
        Self
    }
}

impl PartitionPlan {
    fn assign_point_to_cell(
        &self,
        coordinates: &[f32],
        cursor_state: &mut AxisCursorState,
    ) -> Result<Vec<usize>, StreamingClusteringError> {
        let _ = cursor_state;
        self.axis_plans
            .iter()
            .enumerate()
            .map(|(axis, plan)| match plan {
                AxisPlan::SingleBin => Ok(0),
                AxisPlan::Thresholds(cuts) => {
                    Ok(cuts.partition_point(|cut| coordinates[axis] > *cut))
                }
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

fn validate_and_track_batch(
    tracker: &mut PassTracker,
    embeddings: &[Embedding],
    dimensions: usize,
) -> Result<(), StreamingClusteringError> {
    for embedding in embeddings {
        validate_embedding(embedding, dimensions)?;
        tracker.update(embedding);
    }
    Ok(())
}

fn project_embeddings_in_replay_order(
    transform: &PcaTransform,
    embeddings: &[Embedding],
    execution: ProjectionExecution,
) -> Result<Vec<Embedding>, StreamingClusteringError> {
    map_embeddings_in_replay_order(embeddings, execution, |embedding| {
        transform.apply(embedding).map_err(map_pca_error)
    })
}

fn map_embeddings_in_replay_order<T, E, F>(
    embeddings: &[Embedding],
    execution: ProjectionExecution,
    apply: F,
) -> Result<Vec<T>, E>
where
    T: Send,
    E: Send,
    F: Fn(&Embedding) -> Result<T, E> + Send + Sync,
{
    if execution.uses_parallel_projection(embeddings.len()) {
        embeddings.par_iter().map(apply).collect()
    } else {
        embeddings.iter().map(apply).collect()
    }
}

impl ProjectionExecution {
    fn uses_parallel_projection(self, batch_len: usize) -> bool {
        match self {
            Self::Auto => {
                batch_len >= MIN_PARALLEL_PROJECTION_BATCH_LEN && rayon::current_num_threads() > 1
            }
            #[cfg(test)]
            Self::Serial => false,
            #[cfg(test)]
            Self::Parallel => batch_len > 1,
        }
    }
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

fn div_ceil_u128(numerator: u128, denominator: u128) -> u128 {
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
    use std::time::Duration;

    struct FailingOutOfCorePlannerState;

    impl DirectionalPcaOutOfCorePlannerState for FailingOutOfCorePlannerState {
        fn begin_quantile_pass(
            &mut self,
            _axis_count: usize,
            _expected_value_count: usize,
        ) -> Result<(), String> {
            Err("out-of-core quantile spill should not be used by the GK planner".into())
        }

        fn append_quantile_values(&mut self, _values: &[f32]) -> Result<(), String> {
            Err("out-of-core quantile spill should not be used by the GK planner".into())
        }

        fn finish_quantile_pass(&mut self) -> Result<(), String> {
            Err("out-of-core quantile spill should not be used by the GK planner".into())
        }

        fn scan_quantile_axis(
            &self,
            _axis_index: usize,
            _observe: &mut dyn FnMut(f32) -> Result<(), String>,
        ) -> Result<(), String> {
            Err("out-of-core quantile spill should not be used by the GK planner".into())
        }

        fn clear_quantile_pass(&mut self) -> Result<(), String> {
            Err("out-of-core quantile spill should not be used by the GK planner".into())
        }
    }

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

    fn one_dimensional_params() -> DirectionalPcaParams {
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
    fn density_valley_cuts_choose_the_deep_valley() {
        let cuts =
            select_histogram_valley_cut_values(-1.0, 11.0, &[4, 4, 1, 0, 1, 4, 4], 2).unwrap();
        assert_eq!(cuts.len(), 1);
        assert!(cuts[0] > 3.0);
        assert!(cuts[0] < 7.0);
    }

    #[test]
    fn phase_only_realize_partition_telemetry_does_not_claim_realized_cells() {
        let detail =
            telemetry_detail_from_phase(&ReplayPhase::RealizePartition(ReadyPartitionPlan {
                partition: PartitionPlan {
                    transform: PcaTransform {
                        input_dim: 1,
                        output_dim: 1,
                        mean: vec![0.0],
                        basis: vec![1.0],
                        explained_variance: Some(vec![1.0]),
                        schema_version: 1,
                    },
                    axis_plans: vec![AxisPlan::SingleBin],
                },
                cells: vec![ReadyCellPlan {
                    key: vec![0],
                    count: 2,
                    extra_clusters: 0,
                    cluster_offset: 0,
                }],
            }));
        assert_eq!(detail.populated_cell_count, Some(1));
        assert_eq!(detail.realized_cell_count, None);
    }

    #[test]
    fn gk_quantile_planning_advances_without_extra_public_passes() {
        let embeddings = (0..8).map(|value| vec![value as f32]).collect::<Vec<_>>();
        let config = StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 1,
            balance_constraints: None,
            random_seed: None,
        };

        let mut trainer =
            DirectionalPcaStreamingTrainer::new(config, one_dimensional_params()).unwrap();
        trainer.ingest_batch(&embeddings).unwrap();
        trainer.finish_pass().unwrap();
        trainer.ingest_batch(&embeddings).unwrap();
        trainer.finish_pass().unwrap();
        assert_eq!(
            trainer.telemetry().subphase,
            DirectionalPcaTrainerSubphase::CountCells
        );
    }

    #[test]
    fn gk_quantile_planner_derives_deterministic_thresholds() {
        let values = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0];
        let build_cuts = || {
            let mut planner = AxisPlanner::new_quantile(4, values.len());
            for value in values {
                planner.observe(value);
            }
            match planner.finish_pass().unwrap() {
                AxisPlanner::Ready(AxisPlan::Thresholds(cuts)) => cuts,
                other => panic!("expected threshold plan, got {other:?}"),
            }
        };

        let first = build_cuts();
        let second = build_cuts();
        assert_eq!(first, second);
        assert_eq!(first.len(), 3);
        assert!(first.windows(2).all(|pair| pair[0] <= pair[1]));
        let rank_error = values.len().div_ceil(GK_QUANTILE_RANK_ERROR_DENOMINATOR);
        let targets = (1..4)
            .map(|bin| usize::try_from(div_ceil_u128((bin * values.len()) as u128, 4)).unwrap())
            .collect::<Vec<_>>();
        let realized_ranks = first
            .iter()
            .map(|cut| values.partition_point(|value| value.total_cmp(cut).is_le()))
            .collect::<Vec<_>>();
        for (target_rank, realized_rank) in targets.into_iter().zip(realized_ranks) {
            assert!(realized_rank.abs_diff(target_rank) <= rank_error);
        }
    }

    #[test]
    fn gk_quantile_planner_keeps_total_cmp_order_for_signed_zero_values() {
        let mut planner = AxisPlanner::new_quantile(2, 4);
        for value in [0.0f32, -0.0, 0.0, -0.0] {
            planner.observe(value);
        }
        let AxisPlanner::Quantile(planner) = planner else {
            panic!("expected quantile planner during plan-cuts pass");
        };
        assert!(
            planner
                .summary
                .windows(2)
                .all(|pair| pair[0].value.total_cmp(&pair[1].value).is_le())
        );
    }

    #[test]
    fn threshold_assignment_keeps_cut_equal_values_in_the_lower_bin() {
        let partition = PartitionPlan {
            transform: PcaTransform {
                input_dim: 1,
                output_dim: 1,
                mean: vec![0.0],
                basis: vec![1.0],
                explained_variance: Some(vec![1.0]),
                schema_version: 1,
            },
            axis_plans: vec![AxisPlan::Thresholds(vec![1.0, 2.0])],
        };
        let mut cursor_state = AxisCursorState::for_partition(&partition);
        assert_eq!(
            partition
                .assign_point_to_cell(&[1.0], &mut cursor_state)
                .unwrap(),
            vec![0]
        );
        assert_eq!(
            partition
                .assign_point_to_cell(&[2.0], &mut cursor_state)
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            partition
                .assign_point_to_cell(&[2.5], &mut cursor_state)
                .unwrap(),
            vec![2]
        );
    }

    #[test]
    fn gk_quantile_planning_does_not_touch_out_of_core_spill_state() {
        let embeddings = (0..8).map(|value| vec![value as f32]).collect::<Vec<_>>();
        let config = StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 1,
            balance_constraints: None,
            random_seed: None,
        };

        let mut trainer = DirectionalPcaStreamingTrainer::new(config, one_dimensional_params())
            .unwrap()
            .with_out_of_core_planner_state(Box::new(FailingOutOfCorePlannerState));
        trainer.ingest_batch(&embeddings).unwrap();
        trainer.finish_pass().unwrap();
        trainer.ingest_batch(&embeddings).unwrap();
        trainer.finish_pass().unwrap();
        assert_eq!(
            trainer.telemetry().subphase,
            DirectionalPcaTrainerSubphase::CountCells
        );
    }

    fn replay_embeddings() -> Vec<Embedding> {
        (0..8).map(|value| vec![value as f32]).collect()
    }

    fn replay_config() -> StreamingClusteringConfig {
        StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 1,
            balance_constraints: None,
            random_seed: None,
        }
    }

    fn replay_reports_and_classifier(
        projection_execution: ProjectionExecution,
    ) -> (Vec<PassReport>, DirectionalPcaStreamingClassifier) {
        let embeddings = replay_embeddings();
        let mut trainer =
            DirectionalPcaStreamingTrainer::new(replay_config(), one_dimensional_params())
                .unwrap()
                .with_projection_execution(projection_execution);
        let mut reports = Vec::new();
        for _ in 0..4 {
            trainer.ingest_batch(&embeddings).unwrap();
            reports.push(trainer.finish_pass().unwrap());
        }
        trainer.complete_training().unwrap();
        let classifier = trainer.into_classifier().unwrap();
        (reports, classifier)
    }

    #[test]
    fn parallel_projection_preserves_replay_order_under_staggered_completion() {
        let embeddings = (0..8).map(|value| vec![value as f32]).collect::<Vec<_>>();
        let projected = map_embeddings_in_replay_order(
            &embeddings,
            ProjectionExecution::Parallel,
            |embedding| {
                std::thread::sleep(Duration::from_millis(
                    2 * (embeddings.len() - embedding[0] as usize) as u64,
                ));
                Ok::<_, StreamingClusteringError>(embedding[0] as usize)
            },
        )
        .unwrap();
        assert_eq!(projected, (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn parallel_projection_matches_serial_outputs_across_replay_phases() {
        let (serial_reports, serial_classifier) =
            replay_reports_and_classifier(ProjectionExecution::Serial);
        let (parallel_reports, parallel_classifier) =
            replay_reports_and_classifier(ProjectionExecution::Parallel);

        assert_eq!(parallel_reports, serial_reports);
        assert_eq!(parallel_classifier.centroids, serial_classifier.centroids);
    }
}
