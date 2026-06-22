// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Streaming directional-PCA clustering for LexonGraph.

use std::collections::{BTreeMap, BTreeSet};

use lexongraph_pca::{PcaError, PcaTransform, fit};
use lexongraph_streaming_clustering::{
    ClusterId, Embedding, MetricDirection, PassReport, StreamingClusterClassifier,
    StreamingClusterTrainer, StreamingClusteringConfig, StreamingClusteringError, TrainerState,
    validate_config, validate_embedding,
};
use sha2::{Digest, Sha256};

pub const DIRECTIONAL_PCA_SOFTWARE_IDENTITY: &str =
    concat!("lexongraph-directional-pca-v", env!("CARGO_PKG_VERSION"));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectionalPcaRetainedAxisPolicy {
    FixedCount(usize),
    AdaptiveAllEligible,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectionalPcaBinningPolicy {
    Quantile,
    DensityValley,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectionalPcaParams {
    pub retained_axis_policy: DirectionalPcaRetainedAxisPolicy,
    pub binning_policy: DirectionalPcaBinningPolicy,
    pub variance_exponent: f32,
    pub temperature: f32,
    pub min_input_count: usize,
    pub min_effective_rank: usize,
    pub min_cumulative_variance: f32,
}

#[derive(Clone, Debug)]
pub struct DirectionalPcaStreamingTrainer {
    config: StreamingClusteringConfig,
    params: DirectionalPcaParams,
    state: TrainerState,
    current_pass: Vec<Embedding>,
    baseline_fingerprint: Option<PassFingerprint>,
    completed_passes: usize,
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
    quality_metric: f64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PassFingerprint {
    observed_count: usize,
    digest: [u8; 32],
}

#[derive(Clone, Debug)]
struct Cluster {
    centroid: Embedding,
    members: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CoordinateKey(Vec<u32>);

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
            current_pass: Vec::new(),
            baseline_fingerprint: None,
            completed_passes: 0,
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
        self.current_pass.clear();
        error
    }

    fn finish_pass_impl(&mut self) -> Result<PassReport, StreamingClusteringError> {
        if self.state != TrainerState::Ingesting {
            return Err(self.invalid_transition("finish_pass"));
        }
        if self.current_pass.is_empty() {
            return Err(malformed_input(
                "completed pass must contain at least one embedding",
            ));
        }

        let observed_count = self.current_pass.len();
        let current_fingerprint = fingerprint_pass(self.current_pass.as_slice());
        if self.completed_passes == 0 {
            let minimum_required = self
                .params
                .min_input_count
                .max(self.config.cluster_count as usize);
            if observed_count < minimum_required {
                return Err(unsatisfiable_constraint(format!(
                    "first pass established N = {observed_count}, smaller than the required minimum {minimum_required}"
                )));
            }
            self.baseline_fingerprint = Some(current_fingerprint);
        } else {
            let baseline = self.baseline_fingerprint.as_ref().ok_or_else(|| {
                unsatisfiable_constraint(
                    "missing baseline dataset for later directional-PCA passes",
                )
            })?;
            if baseline != &current_fingerprint {
                return Err(malformed_input(
                    "later passes must replay the same logical dataset in the same order",
                ));
            }
        }

        let model = fit_pass_model(&self.current_pass, &self.config, &self.params)?;
        let quality_metric = model.quality_metric;
        self.model = Some(model);
        self.completed_passes += 1;
        self.current_pass.clear();
        self.state = TrainerState::PassComplete;

        Ok(PassReport {
            observed_count,
            quality_metric,
            balance_metric: 0.0,
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: (0..self.config.cluster_count).collect(),
        })
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
            }
            TrainerState::Ingesting => {}
            TrainerState::TrainingComplete | TrainerState::Error => {
                return Err(self.invalid_transition("ingest_batch"));
            }
        }

        for embedding in embeddings {
            validate_embedding(embedding, self.config.dimensions)?;
            self.current_pass.push(embedding.clone());
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

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
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
        Ok(best_cluster as ClusterId)
    }
}

fn fit_pass_model(
    embeddings: &[Embedding],
    config: &StreamingClusteringConfig,
    params: &DirectionalPcaParams,
) -> Result<DirectionalPcaModel, StreamingClusteringError> {
    let allow_duplicate_refinement = embeddings_are_identical(embeddings);
    let transform = fit(embeddings).map_err(map_pca_error)?;
    let effective_rank = transform.diagnostics().rank_estimate;
    if !allow_duplicate_refinement && effective_rank < params.min_effective_rank {
        return Err(unsatisfiable_constraint(format!(
            "effective rank {effective_rank} is smaller than the required minimum {}",
            params.min_effective_rank
        )));
    }

    let retained_axis_count = resolve_retained_axis_count(
        &transform,
        config,
        params,
        effective_rank,
        allow_duplicate_refinement,
    )?;
    let cumulative_variance = transform
        .cumulative_variance()
        .and_then(|values| values.get(retained_axis_count.saturating_sub(1)).copied())
        .unwrap_or(0.0);
    if !allow_duplicate_refinement && cumulative_variance < params.min_cumulative_variance {
        return Err(unsatisfiable_constraint(format!(
            "cumulative variance {cumulative_variance} is smaller than the required minimum {}",
            params.min_cumulative_variance
        )));
    }

    let truncated = transform
        .truncate(retained_axis_count)
        .map_err(map_pca_error)?;
    let coordinates = embeddings
        .iter()
        .map(|embedding| truncated.apply(embedding).map_err(map_pca_error))
        .collect::<Result<Vec<_>, _>>()?;
    let axis_scores = compute_axis_scores(embeddings, &truncated, params)?;
    let axis_bin_counts = allocate_axis_bins(
        axis_scores.as_slice(),
        config.cluster_count as usize,
        params.temperature,
    )?;
    let point_bins = assign_bins(
        coordinates.as_slice(),
        axis_bin_counts.as_slice(),
        params.binning_policy,
    );
    let clusters = materialize_clusters(
        embeddings,
        coordinates.as_slice(),
        point_bins.as_slice(),
        config.cluster_count as usize,
    )?;
    let centroids = clusters
        .iter()
        .map(|cluster| cluster.centroid.clone())
        .collect::<Vec<_>>();
    let quality_metric = compute_quality_metric(embeddings, clusters.as_slice())?;

    Ok(DirectionalPcaModel {
        centroids,
        quality_metric,
    })
}

fn validate_params(
    config: &StreamingClusteringConfig,
    params: &DirectionalPcaParams,
) -> Result<(), StreamingClusteringError> {
    match params.retained_axis_policy {
        DirectionalPcaRetainedAxisPolicy::FixedCount(retained_dimension_count) => {
            if retained_dimension_count == 0 || retained_dimension_count > config.dimensions {
                return Err(invalid_configuration(format!(
                    "retained_dimension_count must be in [1, {}], got {}",
                    config.dimensions, retained_dimension_count
                )));
            }
            if retained_dimension_count > config.cluster_count as usize {
                return Err(invalid_configuration(format!(
                    "retained_dimension_count {} cannot exceed cluster_count {}",
                    retained_dimension_count, config.cluster_count
                )));
            }
            if params.min_effective_rank > retained_dimension_count {
                return Err(invalid_configuration(format!(
                    "min_effective_rank must be in [1, {}], got {}",
                    retained_dimension_count, params.min_effective_rank
                )));
            }
        }
        DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible => {
            let max_eligible_axes = max_exact_k_eligible_axis_count(config.cluster_count as usize)
                .min(config.dimensions);
            if params.min_effective_rank > max_eligible_axes {
                return Err(invalid_configuration(format!(
                    "min_effective_rank {} cannot exceed adaptive eligible axis bound {}",
                    params.min_effective_rank, max_eligible_axes
                )));
            }
        }
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
    config: &StreamingClusteringConfig,
    params: &DirectionalPcaParams,
    effective_rank: usize,
    allow_duplicate_refinement: bool,
) -> Result<usize, StreamingClusteringError> {
    match params.retained_axis_policy {
        DirectionalPcaRetainedAxisPolicy::FixedCount(retained_dimension_count) => {
            Ok(retained_dimension_count)
        }
        DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible => {
            let rank_bound = if allow_duplicate_refinement {
                transform.output_dim
            } else {
                effective_rank
            };
            let retained_axis_count = rank_bound
                .min(transform.output_dim)
                .min(max_exact_k_eligible_axis_count(
                    config.cluster_count as usize,
                ))
                .max(1);
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

fn max_exact_k_eligible_axis_count(cluster_count: usize) -> usize {
    let mut eligible_axis_count = 1usize;
    let mut min_cells = 2usize;
    while min_cells.saturating_mul(2) <= cluster_count {
        eligible_axis_count += 1;
        min_cells *= 2;
    }
    eligible_axis_count
}

fn reject_balance_constraints(
    config: &StreamingClusteringConfig,
) -> Result<(), StreamingClusteringError> {
    if config.balance_constraints.is_some() {
        return Err(invalid_configuration(
            "balance constraints are not supported by the scaled-down streaming directional-PCA trainer",
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
            unsatisfiable_constraint(format!("directional PCA fit failed: {error}"))
        }
    }
}

fn compute_axis_scores(
    embeddings: &[Embedding],
    transform: &PcaTransform,
    params: &DirectionalPcaParams,
) -> Result<Vec<f64>, StreamingClusteringError> {
    let centroid = compute_centroid(embeddings)?;
    let explained_variance = transform
        .explained_variance()
        .ok_or_else(|| unsatisfiable_constraint("missing explained variance in PCA transform"))?;
    let gamma = f64::from(params.variance_exponent);

    (0..transform.output_dim)
        .map(|column| {
            let alpha = dot_with_basis_column(centroid.as_slice(), transform, column)?;
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

fn allocate_axis_bins(
    axis_scores: &[f64],
    cluster_count: usize,
    temperature: f32,
) -> Result<Vec<usize>, StreamingClusteringError> {
    if axis_scores.is_empty() {
        return Err(invalid_configuration(
            "cannot allocate bins with zero retained dimensions",
        ));
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
    let temperature = f64::from(temperature);
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

fn assign_bins(
    coordinates: &[Embedding],
    axis_bin_counts: &[usize],
    binning_policy: DirectionalPcaBinningPolicy,
) -> Vec<Vec<usize>> {
    match binning_policy {
        DirectionalPcaBinningPolicy::Quantile => assign_quantile_bins(coordinates, axis_bin_counts),
        DirectionalPcaBinningPolicy::DensityValley => {
            assign_density_valley_bins(coordinates, axis_bin_counts)
        }
    }
}

fn assign_quantile_bins(coordinates: &[Embedding], axis_bin_counts: &[usize]) -> Vec<Vec<usize>> {
    let point_count = coordinates.len();
    let retained_dims = axis_bin_counts.len();
    let mut point_bins = vec![vec![0_usize; retained_dims]; point_count];

    for (axis, &bin_count) in axis_bin_counts.iter().enumerate() {
        if bin_count == 1 {
            continue;
        }

        let mut order = (0..point_count).collect::<Vec<_>>();
        order.sort_by(|left, right| {
            coordinates[*left][axis]
                .total_cmp(&coordinates[*right][axis])
                .then_with(|| left.cmp(right))
        });

        for (rank, point_index) in order.into_iter().enumerate() {
            point_bins[point_index][axis] = rank * bin_count / point_count;
        }
    }

    point_bins
}

fn assign_density_valley_bins(
    coordinates: &[Embedding],
    axis_bin_counts: &[usize],
) -> Vec<Vec<usize>> {
    let point_count = coordinates.len();
    let retained_dims = axis_bin_counts.len();
    let mut point_bins = vec![vec![0_usize; retained_dims]; point_count];

    for (axis, &bin_count) in axis_bin_counts.iter().enumerate() {
        if bin_count == 1 {
            continue;
        }

        let mut order = (0..point_count).collect::<Vec<_>>();
        order.sort_by(|left, right| {
            coordinates[*left][axis]
                .total_cmp(&coordinates[*right][axis])
                .then_with(|| left.cmp(right))
        });
        let cut_positions =
            select_density_valley_cut_positions(coordinates, order.as_slice(), axis, bin_count);
        let mut next_cut_index = 0usize;
        let mut current_bin = 0usize;
        for (rank, point_index) in order.into_iter().enumerate() {
            while next_cut_index < cut_positions.len() && rank >= cut_positions[next_cut_index] {
                current_bin += 1;
                next_cut_index += 1;
            }
            point_bins[point_index][axis] = current_bin;
        }
    }

    point_bins
}

fn select_density_valley_cut_positions(
    coordinates: &[Embedding],
    order: &[usize],
    axis: usize,
    bin_count: usize,
) -> Vec<usize> {
    let mut candidates = Vec::new();
    for split_after in 0..order.len().saturating_sub(1) {
        let left = coordinates[order[split_after]][axis];
        let right = coordinates[order[split_after + 1]][axis];
        let gap = f64::from(right) - f64::from(left);
        if gap > 0.0 {
            candidates.push((split_after + 1, gap));
        }
    }

    candidates.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });

    let mut cut_positions = candidates
        .into_iter()
        .take(bin_count.saturating_sub(1))
        .map(|(split_position, _)| split_position)
        .collect::<Vec<_>>();
    cut_positions.sort_unstable();
    cut_positions
}

fn materialize_clusters(
    embeddings: &[Embedding],
    coordinates: &[Embedding],
    point_bins: &[Vec<usize>],
    cluster_count: usize,
) -> Result<Vec<Cluster>, StreamingClusteringError> {
    let mut buckets: BTreeMap<Vec<usize>, Vec<usize>> = BTreeMap::new();
    for (point_index, key) in point_bins.iter().cloned().enumerate() {
        buckets.entry(key).or_default().push(point_index);
    }

    let member_groups = if buckets.len() == cluster_count {
        buckets.into_values().collect::<Vec<_>>()
    } else if buckets.len() > cluster_count {
        return Err(unsatisfiable_constraint(format!(
            "directional-PCA partition realized {} populated cells instead of the required {cluster_count}",
            buckets.len()
        )));
    } else {
        refine_duplicate_collapse(coordinates, &buckets, cluster_count).ok_or_else(|| {
            unsatisfiable_constraint(format!(
                "directional-PCA partition realized {} populated cells and duplicate refinement could not realize the required {cluster_count}",
                buckets.len()
            ))
        })?
    };

    member_groups
        .into_iter()
        .map(|members| {
            let centroid = compute_centroid_from_indexes(embeddings, members.as_slice())?;
            Ok(Cluster { centroid, members })
        })
        .collect()
}

fn refine_duplicate_collapse(
    coordinates: &[Embedding],
    buckets: &BTreeMap<Vec<usize>, Vec<usize>>,
    cluster_count: usize,
) -> Option<Vec<Vec<usize>>> {
    let mut remaining_extra_clusters = cluster_count.checked_sub(buckets.len())?;
    let mut planned_splits = Vec::with_capacity(buckets.len());

    for members in buckets.values() {
        let duplicate_groups = duplicate_coordinate_groups(coordinates, members);
        let mut allocated_extras = vec![0_usize; duplicate_groups.len()];
        for (slot, group) in allocated_extras.iter_mut().zip(duplicate_groups.iter()) {
            if remaining_extra_clusters == 0 {
                break;
            }
            let extras_for_group = remaining_extra_clusters.min(group.len().saturating_sub(1));
            *slot = extras_for_group;
            remaining_extra_clusters -= extras_for_group;
        }
        planned_splits.push((members.clone(), duplicate_groups, allocated_extras));
    }

    if remaining_extra_clusters != 0 {
        return None;
    }

    let mut refined = Vec::with_capacity(cluster_count);
    for (members, duplicate_groups, allocated_extras) in planned_splits {
        let mut peeled_members = BTreeSet::new();
        let mut extra_clusters = Vec::new();
        for (group, extras_for_group) in duplicate_groups.iter().zip(allocated_extras) {
            if extras_for_group == 0 {
                continue;
            }
            for &member_index in &group[group.len() - extras_for_group..] {
                peeled_members.insert(member_index);
                extra_clusters.push(vec![member_index]);
            }
        }

        let base_cluster = members
            .into_iter()
            .filter(|member_index| !peeled_members.contains(member_index))
            .collect::<Vec<_>>();
        if base_cluster.is_empty() {
            return None;
        }

        refined.push(base_cluster);
        refined.extend(extra_clusters);
    }

    Some(refined)
}

fn duplicate_coordinate_groups(coordinates: &[Embedding], members: &[usize]) -> Vec<Vec<usize>> {
    let mut grouped = BTreeMap::<CoordinateKey, Vec<usize>>::new();
    for &member_index in members {
        grouped
            .entry(coordinate_key(coordinates[member_index].as_slice()))
            .or_default()
            .push(member_index);
    }

    let mut duplicate_groups = grouped
        .into_values()
        .filter(|group| group.len() > 1)
        .collect::<Vec<_>>();
    duplicate_groups.sort_by_key(|group| group[0]);
    duplicate_groups
}

fn coordinate_key(coordinates: &[f32]) -> CoordinateKey {
    CoordinateKey(coordinates.iter().map(|value| value.to_bits()).collect())
}

fn embeddings_are_identical(embeddings: &[Embedding]) -> bool {
    let Some(first) = embeddings.first() else {
        return false;
    };
    let first_key = coordinate_key(first.as_slice());
    embeddings
        .iter()
        .skip(1)
        .all(|embedding| coordinate_key(embedding.as_slice()) == first_key)
}

fn compute_quality_metric(
    embeddings: &[Embedding],
    clusters: &[Cluster],
) -> Result<f64, StreamingClusteringError> {
    let mut total = 0.0_f64;
    for cluster in clusters {
        for &member_index in &cluster.members {
            total += squared_distance(
                embeddings[member_index].as_slice(),
                cluster.centroid.as_slice(),
            )?;
        }
    }
    if !total.is_finite() {
        return Err(unsatisfiable_constraint("quality metric became non-finite"));
    }
    Ok(total)
}

fn fingerprint_pass(embeddings: &[Embedding]) -> PassFingerprint {
    let mut hasher = Sha256::new();
    for embedding in embeddings {
        hasher.update((embedding.len() as u64).to_le_bytes());
        for value in embedding {
            hasher.update(value.to_bits().to_le_bytes());
        }
    }

    PassFingerprint {
        observed_count: embeddings.len(),
        digest: hasher.finalize().into(),
    }
}

fn compute_centroid(embeddings: &[Embedding]) -> Result<Embedding, StreamingClusteringError> {
    let Some(first) = embeddings.first() else {
        return Err(unsatisfiable_constraint(
            "cannot compute a centroid for zero embeddings",
        ));
    };
    let dims = first.len();
    let mut sums = vec![0.0_f64; dims];
    for embedding in embeddings {
        for (dimension, value) in embedding.iter().copied().enumerate() {
            sums[dimension] += f64::from(value);
            if !sums[dimension].is_finite() {
                return Err(unsatisfiable_constraint(format!(
                    "centroid sum became non-finite at dimension {dimension}"
                )));
            }
        }
    }
    let divisor = embeddings.len() as f64;
    sums.into_iter()
        .enumerate()
        .map(|(dimension, value)| {
            let centroid = (value / divisor) as f32;
            if !centroid.is_finite() {
                return Err(unsatisfiable_constraint(format!(
                    "centroid became non-finite at dimension {dimension}"
                )));
            }
            Ok(centroid)
        })
        .collect()
}

fn compute_centroid_from_indexes(
    embeddings: &[Embedding],
    indexes: &[usize],
) -> Result<Embedding, StreamingClusteringError> {
    let Some(first_index) = indexes.first() else {
        return Err(unsatisfiable_constraint(
            "cannot compute a centroid for zero indexed embeddings",
        ));
    };
    let dims = embeddings[*first_index].len();
    let mut sums = vec![0.0_f64; dims];

    for &index in indexes {
        for (dimension, value) in embeddings[index].iter().copied().enumerate() {
            sums[dimension] += f64::from(value);
            if !sums[dimension].is_finite() {
                return Err(unsatisfiable_constraint(format!(
                    "centroid sum became non-finite at dimension {dimension}"
                )));
            }
        }
    }

    let divisor = indexes.len() as f64;
    sums.into_iter()
        .enumerate()
        .map(|(dimension, value)| {
            let centroid = (value / divisor) as f32;
            if !centroid.is_finite() {
                return Err(unsatisfiable_constraint(format!(
                    "centroid became non-finite at dimension {dimension}"
                )));
            }
            Ok(centroid)
        })
        .collect()
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

    #[test]
    fn axis_scoring_uses_direction_and_variance() {
        let embeddings = vec![
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![10.0, 1.0],
            vec![11.0, 1.0],
        ];
        let transform = fit(&embeddings).unwrap().truncate(2).unwrap();
        let scores = compute_axis_scores(
            &embeddings,
            &transform,
            &DirectionalPcaParams {
                retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(2),
                binning_policy: DirectionalPcaBinningPolicy::Quantile,
                variance_exponent: 1.0,
                temperature: 1.0,
                min_input_count: 2,
                min_effective_rank: 1,
                min_cumulative_variance: 0.0,
            },
        )
        .unwrap();

        assert_eq!(scores.len(), 2);
        assert!(scores[0] > scores[1]);
    }

    #[test]
    fn temperature_controlled_allocation_is_deterministic() {
        let bins = allocate_axis_bins(&[10.0, 1.0], 4, 1.0).unwrap();
        assert_eq!(bins, vec![3, 1]);
    }

    #[test]
    fn quantile_assignment_uses_even_ranks() {
        let coordinates = vec![vec![0.0], vec![0.1], vec![0.2], vec![100.0]];
        let bins = assign_quantile_bins(&coordinates, &[2]);
        assert_eq!(bins, vec![vec![0], vec![0], vec![1], vec![1]]);
    }

    #[test]
    fn density_valley_assignment_splits_on_largest_gap() {
        let coordinates = vec![vec![0.0], vec![0.1], vec![0.2], vec![100.0]];
        let bins = assign_density_valley_bins(&coordinates, &[2]);
        assert_eq!(bins, vec![vec![0], vec![0], vec![0], vec![1]]);
    }

    #[test]
    fn adaptive_retained_axis_policy_caps_by_exact_k_feasibility() {
        assert_eq!(max_exact_k_eligible_axis_count(2), 1);
        assert_eq!(max_exact_k_eligible_axis_count(3), 1);
        assert_eq!(max_exact_k_eligible_axis_count(4), 2);
        assert_eq!(max_exact_k_eligible_axis_count(64), 6);
    }

    #[test]
    fn duplicate_refinement_peels_duplicate_members_deterministically() {
        let coordinates = vec![vec![0.0], vec![0.0], vec![10.0]];
        let buckets = BTreeMap::from([(vec![0], vec![0, 1, 2])]);

        let refined = refine_duplicate_collapse(&coordinates, &buckets, 2).unwrap();

        assert_eq!(refined, vec![vec![0, 2], vec![1]]);
    }

    #[test]
    fn identical_embeddings_bypass_rank_guard_and_realize_exact_k() {
        let config = StreamingClusteringConfig {
            cluster_count: 3,
            dimensions: 2,
            balance_constraints: None,
            random_seed: None,
        };
        let params = DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(2),
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.5,
        };
        let embeddings = vec![
            vec![5.0, 5.0],
            vec![5.0, 5.0],
            vec![5.0, 5.0],
            vec![5.0, 5.0],
        ];

        let model = fit_pass_model(&embeddings, &config, &params).unwrap();

        assert_eq!(model.centroids.len(), 3);
        assert_eq!(model.quality_metric, 0.0);
    }
}
