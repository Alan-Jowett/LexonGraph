// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Streaming directional-PCA clustering for LexonGraph.

use std::collections::BTreeMap;

use lexongraph_pca::{PcaError, PcaTransform, fit};
use lexongraph_streaming_clustering::{
    ClusterId, Embedding, MetricDirection, PassReport, StreamingClusterClassifier,
    StreamingClusterTrainer, StreamingClusteringConfig, StreamingClusteringError, TrainerState,
    validate_config, validate_embedding,
};

#[derive(Clone, Debug, PartialEq)]
pub struct DirectionalPcaParams {
    pub retained_dimension_count: usize,
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
    baseline_pass: Option<Vec<Embedding>>,
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

#[derive(Clone, Debug)]
struct Cluster {
    centroid: Embedding,
    members: Vec<usize>,
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
            current_pass: Vec::new(),
            baseline_pass: None,
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
            self.baseline_pass = Some(self.current_pass.clone());
        } else {
            let baseline = self.baseline_pass.as_ref().ok_or_else(|| {
                unsatisfiable_constraint(
                    "missing baseline dataset for later directional-PCA passes",
                )
            })?;
            if baseline != &self.current_pass {
                return Err(malformed_input(
                    "later passes must replay the same logical dataset in the same order",
                ));
            }
        }

        let model = fit_pass_model(&self.current_pass, &self.config, &self.params)?;
        self.model = Some(model.clone());
        self.completed_passes += 1;
        self.current_pass.clear();
        self.state = TrainerState::PassComplete;

        Ok(PassReport {
            observed_count,
            quality_metric: model.quality_metric,
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
    let transform = fit(embeddings).map_err(map_pca_error)?;
    let effective_rank = transform.diagnostics().rank_estimate;
    if effective_rank < params.min_effective_rank {
        return Err(unsatisfiable_constraint(format!(
            "effective rank {effective_rank} is smaller than the required minimum {}",
            params.min_effective_rank
        )));
    }

    let cumulative_variance = transform
        .cumulative_variance()
        .and_then(|values| {
            values
                .get(params.retained_dimension_count.saturating_sub(1))
                .copied()
        })
        .unwrap_or(0.0);
    if cumulative_variance < params.min_cumulative_variance {
        return Err(unsatisfiable_constraint(format!(
            "cumulative variance {cumulative_variance} is smaller than the required minimum {}",
            params.min_cumulative_variance
        )));
    }

    let truncated = transform
        .truncate(params.retained_dimension_count)
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
    let point_bins = assign_quantile_bins(coordinates.as_slice(), axis_bin_counts.as_slice());
    let clusters = materialize_clusters(
        embeddings,
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
    if params.retained_dimension_count == 0 || params.retained_dimension_count > config.dimensions {
        return Err(invalid_configuration(format!(
            "retained_dimension_count must be in [1, {}], got {}",
            config.dimensions, params.retained_dimension_count
        )));
    }
    if params.retained_dimension_count > config.cluster_count as usize {
        return Err(invalid_configuration(format!(
            "retained_dimension_count {} cannot exceed cluster_count {}",
            params.retained_dimension_count, config.cluster_count
        )));
    }
    if !params.variance_exponent.is_finite() || !(0.0..=1.0).contains(&params.variance_exponent) {
        return Err(invalid_configuration(format!(
            "variance_exponent must be finite and in [0, 1], got {}",
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
    if params.min_effective_rank == 0 || params.min_effective_rank > params.retained_dimension_count
    {
        return Err(invalid_configuration(format!(
            "min_effective_rank must be in [1, {}], got {}",
            params.retained_dimension_count, params.min_effective_rank
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

fn materialize_clusters(
    embeddings: &[Embedding],
    point_bins: &[Vec<usize>],
    cluster_count: usize,
) -> Result<Vec<Cluster>, StreamingClusteringError> {
    let mut buckets: BTreeMap<Vec<usize>, Vec<usize>> = BTreeMap::new();
    for (point_index, key) in point_bins.iter().cloned().enumerate() {
        buckets.entry(key).or_default().push(point_index);
    }
    if buckets.len() != cluster_count {
        return Err(unsatisfiable_constraint(format!(
            "directional-PCA partition realized {} populated cells instead of the required {cluster_count}",
            buckets.len()
        )));
    }

    buckets
        .into_values()
        .map(|members| {
            let centroid = compute_centroid_from_indexes(embeddings, members.as_slice())?;
            Ok(Cluster { centroid, members })
        })
        .collect()
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
    let vectors = indexes
        .iter()
        .map(|index| embeddings[*index].clone())
        .collect::<Vec<_>>();
    compute_centroid(vectors.as_slice())
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
                retained_dimension_count: 2,
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
    fn quantile_assignment_is_density_aware() {
        let coordinates = vec![vec![0.0], vec![0.1], vec![0.2], vec![100.0]];
        let bins = assign_quantile_bins(&coordinates, &[2]);
        assert_eq!(bins, vec![vec![0], vec![0], vec![1], vec![1]]);
    }
}
