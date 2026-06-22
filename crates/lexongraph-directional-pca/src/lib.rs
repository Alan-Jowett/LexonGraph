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
pub enum DirectionalPcaAllocationPolicy {
    CentroidWeightedBins,
    EigenvalueLogBits,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectionalPcaBinningPolicy {
    Quantile,
    DensityValley,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectionalPcaParams {
    pub retained_axis_policy: DirectionalPcaRetainedAxisPolicy,
    pub allocation_policy: DirectionalPcaAllocationPolicy,
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

    let candidate_axis_count = resolve_retained_axis_count(
        &transform,
        params,
        effective_rank,
        allow_duplicate_refinement,
    )?;
    let cumulative_variance = transform
        .cumulative_variance()
        .and_then(|values| values.get(candidate_axis_count.saturating_sub(1)).copied())
        .unwrap_or(0.0);
    if !allow_duplicate_refinement && cumulative_variance < params.min_cumulative_variance {
        return Err(unsatisfiable_constraint(format!(
            "cumulative variance {cumulative_variance} is smaller than the required minimum {}",
            params.min_cumulative_variance
        )));
    }

    let truncated = transform
        .truncate(candidate_axis_count)
        .map_err(map_pca_error)?;
    let coordinates = embeddings
        .iter()
        .map(|embedding| truncated.apply(embedding).map_err(map_pca_error))
        .collect::<Result<Vec<_>, _>>()?;
    let axis_bin_counts = allocate_axis_bins(
        embeddings,
        &truncated,
        params,
        config.cluster_count as usize,
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
                    "retained_axis_policy = FixedCount(n) requires n to be in [1, {}], got {}",
                    config.dimensions, retained_dimension_count
                )));
            }
            if retained_dimension_count > config.cluster_count as usize {
                return Err(invalid_configuration(format!(
                    "retained_axis_policy = FixedCount({}) cannot exceed cluster_count {}",
                    retained_dimension_count, config.cluster_count
                )));
            }
            if params.min_effective_rank > retained_dimension_count {
                return Err(invalid_configuration(format!(
                    "min_effective_rank must be in [1, FixedCount(n)={}], got {}",
                    retained_dimension_count, params.min_effective_rank
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
    match (
        params.retained_axis_policy,
        params.allocation_policy,
        params.binning_policy,
    ) {
        (
            DirectionalPcaRetainedAxisPolicy::FixedCount(_),
            DirectionalPcaAllocationPolicy::CentroidWeightedBins,
            DirectionalPcaBinningPolicy::Quantile,
        )
        | (
            DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
            DirectionalPcaAllocationPolicy::EigenvalueLogBits,
            DirectionalPcaBinningPolicy::DensityValley,
        ) => {}
        _ => {
            return Err(invalid_configuration(
                "unsupported directional-PCA policy combination",
            ));
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
                effective_rank.max(params.min_effective_rank)
            };
            let retained_axis_count = rank_bound.min(transform.output_dim).max(1);
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

    let centroid = compute_centroid(embeddings)?;
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
    embeddings: &[Embedding],
    transform: &PcaTransform,
    params: &DirectionalPcaParams,
    cluster_count: usize,
) -> Result<Vec<usize>, StreamingClusteringError> {
    let axis_scores = compute_axis_scores(embeddings, transform, params)?;
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
        let axis_values = order
            .iter()
            .map(|&point_index| coordinates[point_index][axis])
            .collect::<Vec<_>>();
        let cut_positions = select_deepest_valley_cut_positions(axis_values.as_slice(), bin_count);
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

fn select_deepest_valley_cut_positions(axis_values: &[f32], bin_count: usize) -> Vec<usize> {
    if axis_values.len() <= 1 || bin_count <= 1 {
        return Vec::new();
    }
    let mut segments = vec![(0usize, axis_values.len())];
    let mut cut_positions = Vec::with_capacity(bin_count.saturating_sub(1));

    while cut_positions.len() < bin_count.saturating_sub(1) {
        let mut best_split: Option<(usize, usize, usize, f64, f64)> = None;
        for (segment_index, &(start, end)) in segments.iter().enumerate() {
            if end.saturating_sub(start) <= 1 {
                continue;
            }
            if let Some((split_position, valley_density, valley_depth)) =
                best_valley_in_segment(axis_values, start, end)
            {
                match best_split {
                    None => {
                        best_split = Some((
                            segment_index,
                            start,
                            split_position,
                            valley_density,
                            valley_depth,
                        ));
                    }
                    Some((_, _, best_position, best_density, best_depth)) => {
                        if valley_depth > best_depth
                            || (valley_depth == best_depth
                                && (valley_density < best_density
                                    || (valley_density == best_density
                                        && split_position < best_position)))
                        {
                            best_split = Some((
                                segment_index,
                                start,
                                split_position,
                                valley_density,
                                valley_depth,
                            ));
                        }
                    }
                }
            }
        }

        let Some((segment_index, start, split_position, _, _)) = best_split else {
            break;
        };
        let (_, end) = segments.remove(segment_index);
        segments.push((start, split_position));
        segments.push((split_position, end));
        segments.sort_unstable();
        cut_positions.push(split_position);
    }

    cut_positions.sort_unstable();
    cut_positions
}

fn estimate_density_bandwidth(axis_values: &[f32]) -> f64 {
    if axis_values.len() <= 1 {
        return 1.0;
    }
    let min = axis_values[0];
    let max = axis_values[axis_values.len() - 1];
    let spread = (f64::from(max) - f64::from(min)).abs();
    if spread == 0.0 {
        return 1.0;
    }
    (spread / axis_values.len() as f64).max(f64::EPSILON)
}

fn estimate_density(axis_values: &[f32], position: f32, bandwidth: f64) -> f64 {
    const EXP_UNDERFLOW_TO_ZERO_CUTOFF: f64 =
        (f64::MIN_EXP as f64 - f64::MANTISSA_DIGITS as f64) * std::f64::consts::LN_2;
    let variance = bandwidth * bandwidth;
    axis_values
        .iter()
        .map(|&value| {
            let delta = f64::from(value) - f64::from(position);
            let exponent = -0.5 * delta * delta / variance;
            if exponent <= EXP_UNDERFLOW_TO_ZERO_CUTOFF {
                0.0
            } else {
                exponent.exp()
            }
        })
        .sum::<f64>()
}

fn best_valley_in_segment(
    axis_values: &[f32],
    start: usize,
    end: usize,
) -> Option<(usize, f64, f64)> {
    if end.saturating_sub(start) <= 1 {
        return None;
    }
    let segment_values = &axis_values[start..end];
    let bandwidth = estimate_density_bandwidth(segment_values);
    let segment_densities = segment_values
        .iter()
        .map(|&value| estimate_density(segment_values, value, bandwidth))
        .collect::<Vec<_>>();
    let mut best: Option<(usize, f64, f64)> = None;

    for split_after in start..end.saturating_sub(1) {
        let midpoint = 0.5 * (axis_values[split_after] + axis_values[split_after + 1]);
        let valley_density = estimate_density(segment_values, midpoint, bandwidth);
        let left_peak = segment_densities[..=split_after - start]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let right_peak = segment_densities[split_after + 1 - start..]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let valley_depth = left_peak.min(right_peak) - valley_density;
        match best {
            None => best = Some((split_after + 1, valley_density, valley_depth)),
            Some((best_position, best_density, best_depth)) => {
                if valley_depth > best_depth
                    || (valley_depth == best_depth
                        && (valley_density < best_density
                            || (valley_density == best_density && split_after + 1 < best_position)))
                {
                    best = Some((split_after + 1, valley_density, valley_depth));
                }
            }
        }
    }

    best
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
                allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
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
        let embeddings = vec![
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![10.0, 1.0],
            vec![11.0, 1.0],
        ];
        let transform = fit(&embeddings).unwrap().truncate(2).unwrap();
        let bins = allocate_axis_bins(
            &embeddings,
            &transform,
            &DirectionalPcaParams {
                retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(2),
                allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                binning_policy: DirectionalPcaBinningPolicy::Quantile,
                variance_exponent: 1.0,
                temperature: 1.0,
                min_input_count: 2,
                min_effective_rank: 1,
                min_cumulative_variance: 0.0,
            },
            4,
        )
        .unwrap();
        assert_eq!(bins, vec![3, 1]);
    }

    #[test]
    fn quantile_assignment_uses_even_ranks() {
        let coordinates = vec![vec![0.0], vec![0.1], vec![0.2], vec![100.0]];
        let bins = assign_quantile_bins(&coordinates, &[2]);
        assert_eq!(bins, vec![vec![0], vec![0], vec![1], vec![1]]);
    }

    #[test]
    fn density_valley_assignment_splits_at_a_low_density_valley() {
        let coordinates = vec![vec![0.0], vec![0.1], vec![0.2], vec![100.0]];
        let bins = assign_density_valley_bins(&coordinates, &[2]);
        assert_eq!(bins, vec![vec![0], vec![0], vec![0], vec![1]]);
    }

    #[test]
    fn density_valley_assignment_still_realizes_requested_bins_with_duplicate_coordinates() {
        let coordinates = vec![vec![0.0], vec![0.0], vec![0.0], vec![1.0]];
        let bins = assign_density_valley_bins(&coordinates, &[3]);
        let realized_bins = bins
            .into_iter()
            .map(|axis_bins| axis_bins[0])
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(realized_bins, std::collections::BTreeSet::from([0, 1, 2]));
    }

    #[test]
    fn density_estimation_short_circuits_far_tail_underflow() {
        let density = estimate_density(&[0.0], 40.0, 1.0);
        assert_eq!(density, 0.0);
    }

    #[test]
    fn density_valley_assignment_handles_wide_spread_without_nonfinite_work() {
        let coordinates = vec![vec![0.0], vec![0.1], vec![0.2], vec![1.0e20_f32]];
        let bins = assign_density_valley_bins(&coordinates, &[2]);
        assert_eq!(bins, vec![vec![0], vec![0], vec![0], vec![1]]);
    }

    #[test]
    fn eigenvalue_log_bit_budget_allows_zero_bit_axes() {
        let bins = allocate_axis_bins_from_eigenvalue_bits(&[10.0, 1.0, 0.1, 0.01], 64).unwrap();
        assert_eq!(bins.iter().product::<usize>(), 64);
        assert!(bins.contains(&1));
    }

    #[test]
    fn eigenvalue_log_bit_policy_rejects_non_power_of_two_cluster_count_at_construction() {
        let error = DirectionalPcaStreamingTrainer::new(
            StreamingClusteringConfig {
                cluster_count: 3,
                dimensions: 2,
                balance_constraints: None,
                random_seed: None,
            },
            DirectionalPcaParams {
                retained_axis_policy: DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible,
                allocation_policy: DirectionalPcaAllocationPolicy::EigenvalueLogBits,
                binning_policy: DirectionalPcaBinningPolicy::DensityValley,
                variance_exponent: 1.0,
                temperature: 1.0,
                min_input_count: 2,
                min_effective_rank: 1,
                min_cumulative_variance: 0.0,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            StreamingClusteringError::InvalidConfiguration { .. }
        ));
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
            allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
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
