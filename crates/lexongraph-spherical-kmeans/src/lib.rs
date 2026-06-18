// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Streaming spherical k-means clustering for LexonGraph.

use lexongraph_linear_algebra_acceleration::{
    DenseDistanceMetric, ExecutionBackendRequest, ExecutionBackendResolution,
    dense_distance_matrix, detected_execution_backend_selection, execution_backend_request,
};
use lexongraph_streaming_clustering::{
    ClusterId, Embedding, MetricDirection, PassReport, StreamingClusterClassifier,
    StreamingClusterTrainer, StreamingClusteringConfig, StreamingClusteringError, TrainerState,
    validate_config, validate_embedding,
};

pub const SPHERICAL_KMEANS_SOFTWARE_IDENTITY: &str =
    concat!("lexongraph-spherical-kmeans-v", env!("CARGO_PKG_VERSION"));

const DISTANCE_EPSILON: f32 = 1e-6;
const ACCELERATED_ASSIGNMENT_MIN_OPERATIONS: usize = 1_000_000;
const ACCELERATED_ASSIGNMENT_TARGET_DISTANCES_PER_CHUNK: usize = 1_048_576;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SphericalInitializationPolicy {
    SeededDeterministicFarthestPoint,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SphericalKmeansParams {
    pub initialization_policy: SphericalInitializationPolicy,
    pub max_iteration_count: usize,
    pub convergence_tolerance: f32,
}

#[derive(Clone, Debug)]
pub struct SphericalKmeansStreamingTrainer {
    config: StreamingClusteringConfig,
    params: SphericalKmeansParams,
    state: TrainerState,
    current_pass: Vec<Embedding>,
    baseline_pass: Option<Vec<Embedding>>,
    completed_passes: usize,
    normalized_centroids: Option<Vec<Vec<f32>>>,
}

#[derive(Clone, Debug)]
pub struct SphericalKmeansStreamingClassifier {
    config: StreamingClusteringConfig,
    normalized_centroids: Vec<Vec<f32>>,
}

#[derive(Clone, Debug, PartialEq)]
struct FitResult {
    normalized_centroids: Vec<Vec<f32>>,
    objective_value: f64,
}

impl SphericalKmeansStreamingTrainer {
    pub fn new(
        config: StreamingClusteringConfig,
        params: SphericalKmeansParams,
    ) -> Result<Self, StreamingClusteringError> {
        validate_config(&config)?;
        validate_params(&params)?;
        reject_balance_constraints(&config)?;
        Ok(Self {
            config,
            params,
            state: TrainerState::Idle,
            current_pass: Vec::new(),
            baseline_pass: None,
            completed_passes: 0,
            normalized_centroids: None,
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
        let cluster_count = self.config.cluster_count as usize;
        if observed_count < cluster_count {
            return Err(unsatisfiable_constraint(format!(
                "observed_count {observed_count} is smaller than cluster_count {cluster_count}"
            )));
        }

        if self.completed_passes == 0 {
            self.baseline_pass = Some(self.current_pass.clone());
        } else if self.baseline_pass.as_ref() != Some(&self.current_pass) {
            return Err(malformed_input(
                "later passes must replay the same logical dataset in the same order",
            ));
        }

        let normalized_embeddings =
            normalize_pass(self.current_pass.as_slice(), self.config.dimensions)?;
        let initial_centroids = match self.normalized_centroids.clone() {
            Some(existing) => existing,
            None => initialize_centroids(
                normalized_embeddings.as_slice(),
                cluster_count,
                self.config.random_seed.unwrap_or(0),
                self.params.initialization_policy,
            )?,
        };
        let fit = run_spherical_kmeans(
            normalized_embeddings.as_slice(),
            initial_centroids,
            &self.params,
        )?;

        self.normalized_centroids = Some(fit.normalized_centroids);
        self.completed_passes += 1;
        self.current_pass.clear();
        self.state = TrainerState::PassComplete;

        Ok(PassReport {
            observed_count,
            quality_metric: fit.objective_value,
            balance_metric: 0.0,
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: (0..self.config.cluster_count).collect(),
        })
    }
}

impl StreamingClusterTrainer for SphericalKmeansStreamingTrainer {
    type Classifier = SphericalKmeansStreamingClassifier;

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
            ensure_non_zero_norm(embedding)?;
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
        let normalized_centroids = self.normalized_centroids.ok_or_else(|| {
            StreamingClusteringError::InvalidTransition {
                state: self.state,
                operation: "into_classifier".into(),
            }
        })?;
        Ok(SphericalKmeansStreamingClassifier {
            config: self.config,
            normalized_centroids,
        })
    }
}

impl StreamingClusterClassifier for SphericalKmeansStreamingClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        validate_embedding(embedding, self.config.dimensions)?;
        ensure_non_zero_norm(embedding)?;
        let normalized_embedding = normalize_embedding(embedding)?;
        Ok(best_cluster(
            normalized_embedding.as_slice(),
            self.normalized_centroids.as_slice(),
            None,
        )? as ClusterId)
    }
}

fn validate_params(params: &SphericalKmeansParams) -> Result<(), StreamingClusteringError> {
    if params.max_iteration_count == 0 {
        return Err(StreamingClusteringError::InvalidConfiguration {
            message: "max_iteration_count must be positive".into(),
        });
    }
    if !params.convergence_tolerance.is_finite() || params.convergence_tolerance < 0.0 {
        return Err(StreamingClusteringError::InvalidConfiguration {
            message: "convergence_tolerance must be finite and non-negative".into(),
        });
    }
    Ok(())
}

fn reject_balance_constraints(
    config: &StreamingClusteringConfig,
) -> Result<(), StreamingClusteringError> {
    let Some(constraints) = &config.balance_constraints else {
        return Ok(());
    };
    if constraints.min_cluster_occupancy.is_some()
        || constraints.max_cluster_occupancy.is_some()
        || constraints.max_cluster_size_ratio.is_some()
        || constraints.soft_balance_penalty.is_some()
    {
        return Err(StreamingClusteringError::InvalidConfiguration {
            message: "balance constraints are not supported by the spherical k-means realization"
                .into(),
        });
    }
    Ok(())
}

fn run_spherical_kmeans(
    normalized_embeddings: &[Vec<f32>],
    mut normalized_centroids: Vec<Vec<f32>>,
    params: &SphericalKmeansParams,
) -> Result<FitResult, StreamingClusteringError> {
    let mut previous_assignments: Option<Vec<usize>> = None;
    for _ in 0..params.max_iteration_count {
        let mut assignments = assign_points(
            normalized_embeddings,
            normalized_centroids.as_slice(),
            previous_assignments.as_deref(),
        )?;
        repair_empty_clusters(
            normalized_embeddings,
            normalized_centroids.as_slice(),
            assignments.as_mut_slice(),
        )?;
        let updated = recompute_centroids(
            normalized_embeddings,
            assignments.as_slice(),
            normalized_centroids.len(),
        )?;
        let max_shift =
            maximum_centroid_shift(normalized_centroids.as_slice(), updated.as_slice())?;
        normalized_centroids = updated;
        if max_shift <= params.convergence_tolerance {
            previous_assignments = Some(assignments);
            break;
        }
        previous_assignments = Some(assignments);
    }

    let mut assignments = assign_points(
        normalized_embeddings,
        normalized_centroids.as_slice(),
        previous_assignments.as_deref(),
    )?;
    repair_empty_clusters(
        normalized_embeddings,
        normalized_centroids.as_slice(),
        assignments.as_mut_slice(),
    )?;
    normalized_centroids = recompute_centroids(
        normalized_embeddings,
        assignments.as_slice(),
        normalized_centroids.len(),
    )?;
    let objective_value = average_objective(
        normalized_embeddings,
        assignments.as_slice(),
        normalized_centroids.as_slice(),
    )?;
    Ok(FitResult {
        normalized_centroids,
        objective_value,
    })
}

fn initialize_centroids(
    normalized_embeddings: &[Vec<f32>],
    cluster_count: usize,
    random_seed: u64,
    initialization_policy: SphericalInitializationPolicy,
) -> Result<Vec<Vec<f32>>, StreamingClusteringError> {
    match initialization_policy {
        SphericalInitializationPolicy::SeededDeterministicFarthestPoint => {
            seeded_deterministic_farthest_point(normalized_embeddings, cluster_count, random_seed)
        }
    }
}

fn seeded_deterministic_farthest_point(
    normalized_embeddings: &[Vec<f32>],
    cluster_count: usize,
    random_seed: u64,
) -> Result<Vec<Vec<f32>>, StreamingClusteringError> {
    if cluster_count > normalized_embeddings.len() {
        return Err(unsatisfiable_constraint(format!(
            "cluster_count {cluster_count} exceeds observed_count {}",
            normalized_embeddings.len()
        )));
    }
    let mut selected = Vec::with_capacity(cluster_count);
    let first_index = (0..normalized_embeddings.len())
        .min_by_key(|&index| {
            deterministic_embedding_hash(normalized_embeddings[index].as_slice(), random_seed)
        })
        .ok_or_else(|| {
            unsatisfiable_constraint("spherical k-means requires at least one embedding")
        })?;
    selected.push(first_index);
    while selected.len() < cluster_count {
        let next_index = (0..normalized_embeddings.len())
            .filter(|index| !selected.contains(index))
            .max_by(|left, right| {
                nearest_centroid_distance(
                    normalized_embeddings[*left].as_slice(),
                    normalized_embeddings,
                    selected.as_slice(),
                )
                .partial_cmp(&nearest_centroid_distance(
                    normalized_embeddings[*right].as_slice(),
                    normalized_embeddings,
                    selected.as_slice(),
                ))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right.cmp(left))
            })
            .ok_or_else(|| {
                unsatisfiable_constraint("failed to choose enough deterministic initial centroids")
            })?;
        selected.push(next_index);
    }
    Ok(selected
        .into_iter()
        .map(|index| normalized_embeddings[index].clone())
        .collect())
}

fn nearest_centroid_distance(
    embedding: &[f32],
    normalized_embeddings: &[Vec<f32>],
    selected: &[usize],
) -> f32 {
    selected
        .iter()
        .map(|&index| {
            cosine_distance_normalized(embedding, normalized_embeddings[index].as_slice())
                .unwrap_or(f32::INFINITY)
        })
        .fold(f32::INFINITY, f32::min)
}

fn assign_points(
    normalized_embeddings: &[Vec<f32>],
    normalized_centroids: &[Vec<f32>],
    previous_assignments: Option<&[usize]>,
) -> Result<Vec<usize>, StreamingClusteringError> {
    if should_use_accelerated_assignments(normalized_embeddings, normalized_centroids) {
        return assign_points_accelerated(
            normalized_embeddings,
            normalized_centroids,
            previous_assignments,
        );
    }
    assign_points_cpu(
        normalized_embeddings,
        normalized_centroids,
        previous_assignments,
    )
}

fn assign_points_cpu(
    normalized_embeddings: &[Vec<f32>],
    normalized_centroids: &[Vec<f32>],
    previous_assignments: Option<&[usize]>,
) -> Result<Vec<usize>, StreamingClusteringError> {
    normalized_embeddings
        .iter()
        .enumerate()
        .map(|(point_index, embedding)| {
            best_cluster(
                embedding.as_slice(),
                normalized_centroids,
                previous_assignments.and_then(|values| values.get(point_index).copied()),
            )
        })
        .collect()
}

fn assign_points_accelerated(
    normalized_embeddings: &[Vec<f32>],
    normalized_centroids: &[Vec<f32>],
    previous_assignments: Option<&[usize]>,
) -> Result<Vec<usize>, StreamingClusteringError> {
    let centroid_refs: Vec<&[f32]> = normalized_centroids
        .iter()
        .map(std::vec::Vec::as_slice)
        .collect();
    let rows_per_chunk =
        assignment_chunk_row_count(normalized_embeddings.len(), centroid_refs.len());
    let mut assignments = Vec::with_capacity(normalized_embeddings.len());
    let mut left_refs = Vec::with_capacity(rows_per_chunk);
    for (chunk_index, embedding_chunk) in normalized_embeddings.chunks(rows_per_chunk).enumerate() {
        left_refs.clear();
        left_refs.extend(embedding_chunk.iter().map(std::vec::Vec::as_slice));
        let distances = dense_distance_matrix(
            left_refs.as_slice(),
            centroid_refs.as_slice(),
            DenseDistanceMetric::Cosine,
        )
        .map_err(|error| {
            unsatisfiable_constraint(format!(
                "accelerated spherical k-means assignment failed: {error}"
            ))
        })?;
        for row_index in 0..embedding_chunk.len() {
            let global_row_index = chunk_index * rows_per_chunk + row_index;
            let row_offset = row_index * centroid_refs.len();
            let row = &distances[row_offset..row_offset + centroid_refs.len()];
            assignments.push(best_cluster_from_distances(
                row,
                previous_assignments.and_then(|values| values.get(global_row_index).copied()),
            )?);
        }
    }
    Ok(assignments)
}

fn should_use_accelerated_assignments(
    normalized_embeddings: &[Vec<f32>],
    normalized_centroids: &[Vec<f32>],
) -> bool {
    if normalized_embeddings.is_empty() || normalized_centroids.is_empty() {
        return false;
    }
    match execution_backend_request() {
        ExecutionBackendRequest::Cpu => return false,
        ExecutionBackendRequest::Wgpu => {
            return detected_execution_backend_selection().resolution
                == ExecutionBackendResolution::Wgpu;
        }
        ExecutionBackendRequest::Auto => {}
    }
    let operation_count = normalized_embeddings
        .len()
        .saturating_mul(normalized_centroids.len())
        .saturating_mul(normalized_embeddings[0].len());
    let large_enough = operation_count >= ACCELERATED_ASSIGNMENT_MIN_OPERATIONS;
    if !large_enough {
        return false;
    }
    detected_execution_backend_selection().resolution == ExecutionBackendResolution::Wgpu
}

fn assignment_chunk_row_count(observed_count: usize, cluster_count: usize) -> usize {
    let cluster_count = cluster_count.max(1);
    let rows = ACCELERATED_ASSIGNMENT_TARGET_DISTANCES_PER_CHUNK / cluster_count;
    rows.clamp(1, observed_count.max(1))
}

fn best_cluster(
    normalized_embedding: &[f32],
    normalized_centroids: &[Vec<f32>],
    previous_assignment: Option<usize>,
) -> Result<usize, StreamingClusteringError> {
    let mut best_distance = f32::INFINITY;
    let mut best_cluster_id: Option<usize> = None;
    let mut previous_assignment_is_best = false;
    for (cluster_index, centroid) in normalized_centroids.iter().enumerate() {
        let candidate = cosine_distance_normalized(normalized_embedding, centroid.as_slice())?;
        if candidate + DISTANCE_EPSILON < best_distance {
            best_distance = candidate;
            best_cluster_id = Some(cluster_index);
            previous_assignment_is_best = previous_assignment == Some(cluster_index);
        } else if (candidate - best_distance).abs() <= DISTANCE_EPSILON {
            best_cluster_id =
                Some(best_cluster_id.map_or(cluster_index, |current| current.min(cluster_index)));
            previous_assignment_is_best |= previous_assignment == Some(cluster_index);
        }
    }
    if previous_assignment_is_best && let Some(previous_assignment) = previous_assignment {
        return Ok(previous_assignment);
    }
    best_cluster_id
        .ok_or_else(|| unsatisfiable_constraint("spherical k-means requires at least one centroid"))
}

fn best_cluster_from_distances(
    distances: &[f32],
    previous_assignment: Option<usize>,
) -> Result<usize, StreamingClusteringError> {
    let mut best_distance = f32::INFINITY;
    let mut best_cluster_id: Option<usize> = None;
    let mut previous_assignment_is_best = false;
    for (cluster_index, &candidate) in distances.iter().enumerate() {
        if candidate + DISTANCE_EPSILON < best_distance {
            best_distance = candidate;
            best_cluster_id = Some(cluster_index);
            previous_assignment_is_best = previous_assignment == Some(cluster_index);
        } else if (candidate - best_distance).abs() <= DISTANCE_EPSILON {
            best_cluster_id =
                Some(best_cluster_id.map_or(cluster_index, |current| current.min(cluster_index)));
            previous_assignment_is_best |= previous_assignment == Some(cluster_index);
        }
    }
    if previous_assignment_is_best && let Some(previous_assignment) = previous_assignment {
        return Ok(previous_assignment);
    }
    best_cluster_id
        .ok_or_else(|| unsatisfiable_constraint("spherical k-means requires at least one centroid"))
}

fn recompute_centroids(
    normalized_embeddings: &[Vec<f32>],
    assignments: &[usize],
    cluster_count: usize,
) -> Result<Vec<Vec<f32>>, StreamingClusteringError> {
    let dimensions = normalized_embeddings[0].len();
    let mut sums = vec![vec![0.0f32; dimensions]; cluster_count];
    let mut counts = vec![0usize; cluster_count];
    for (embedding, &cluster_index) in normalized_embeddings.iter().zip(assignments) {
        counts[cluster_index] += 1;
        for (dimension, value) in embedding.iter().enumerate() {
            sums[cluster_index][dimension] += *value;
        }
    }
    sums.into_iter()
        .enumerate()
        .map(|(cluster_index, centroid)| {
            let squared_norm = centroid.iter().map(|value| value * value).sum::<f32>();
            if !squared_norm.is_finite() || squared_norm <= f32::EPSILON {
                return Err(unsatisfiable_constraint(format!(
                    "spherical k-means produced a zero-norm centroid for cluster {cluster_index}"
                )));
            }
            normalize_embedding(centroid.as_slice())
        })
        .collect()
}

fn repair_empty_clusters(
    normalized_embeddings: &[Vec<f32>],
    normalized_centroids: &[Vec<f32>],
    assignments: &mut [usize],
) -> Result<(), StreamingClusteringError> {
    let mut counts = vec![0usize; normalized_centroids.len()];
    for &cluster_index in assignments.iter() {
        counts[cluster_index] += 1;
    }
    while let Some(empty_cluster) = counts.iter().position(|count| *count == 0) {
        let donor_cluster = counts
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 1)
            .max_by(|left, right| left.1.cmp(right.1).then_with(|| right.0.cmp(&left.0)))
            .map(|(cluster_index, _)| cluster_index)
            .ok_or_else(|| {
                unsatisfiable_constraint(
                    "spherical k-means could not repair an empty cluster without violating exact K",
                )
            })?;
        let donor_point = assignments
            .iter()
            .enumerate()
            .filter(|(_, cluster_index)| **cluster_index == donor_cluster)
            .max_by(|left, right| {
                cosine_distance_normalized(
                    normalized_embeddings[left.0].as_slice(),
                    normalized_centroids[donor_cluster].as_slice(),
                )
                .unwrap_or(f32::NEG_INFINITY)
                .partial_cmp(
                    &cosine_distance_normalized(
                        normalized_embeddings[right.0].as_slice(),
                        normalized_centroids[donor_cluster].as_slice(),
                    )
                    .unwrap_or(f32::NEG_INFINITY),
                )
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right.0.cmp(&left.0))
            })
            .map(|(point_index, _)| point_index)
            .ok_or_else(|| {
                unsatisfiable_constraint("spherical k-means could not choose a donor point")
            })?;
        assignments[donor_point] = empty_cluster;
        counts[donor_cluster] -= 1;
        counts[empty_cluster] += 1;
    }
    Ok(())
}

fn average_objective(
    normalized_embeddings: &[Vec<f32>],
    assignments: &[usize],
    normalized_centroids: &[Vec<f32>],
) -> Result<f64, StreamingClusteringError> {
    let mut total = 0.0f64;
    for (embedding, &cluster_index) in normalized_embeddings.iter().zip(assignments) {
        total += f64::from(cosine_distance_normalized(
            embedding.as_slice(),
            normalized_centroids[cluster_index].as_slice(),
        )?);
    }
    Ok(total / normalized_embeddings.len() as f64)
}

fn maximum_centroid_shift(
    previous: &[Vec<f32>],
    updated: &[Vec<f32>],
) -> Result<f32, StreamingClusteringError> {
    previous
        .iter()
        .zip(updated)
        .map(|(left, right)| cosine_distance_normalized(left.as_slice(), right.as_slice()))
        .try_fold(0.0f32, |current_max, candidate| {
            candidate.map(|v| current_max.max(v))
        })
}

fn normalize_pass(
    embeddings: &[Embedding],
    dimensions: usize,
) -> Result<Vec<Vec<f32>>, StreamingClusteringError> {
    embeddings
        .iter()
        .map(|embedding| {
            validate_embedding(embedding, dimensions)?;
            ensure_non_zero_norm(embedding)?;
            normalize_embedding(embedding.as_slice())
        })
        .collect()
}

fn normalize_embedding(embedding: &[f32]) -> Result<Vec<f32>, StreamingClusteringError> {
    let squared_norm = embedding.iter().map(|value| value * value).sum::<f32>();
    if !squared_norm.is_finite() || squared_norm <= 0.0 {
        return Err(malformed_input(
            "embeddings must have a non-zero Euclidean norm",
        ));
    }
    let norm = squared_norm.sqrt();
    Ok(embedding.iter().map(|value| *value / norm).collect())
}

fn ensure_non_zero_norm(embedding: &[f32]) -> Result<(), StreamingClusteringError> {
    let squared_norm = embedding.iter().map(|value| value * value).sum::<f32>();
    if !squared_norm.is_finite() || squared_norm <= 0.0 {
        return Err(malformed_input(
            "embeddings must have a non-zero Euclidean norm",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn cosine_distance(left: &[f32], right: &[f32]) -> Result<f32, StreamingClusteringError> {
    if left.len() != right.len() {
        return Err(malformed_input(
            "cosine distance requires equal embedding dimensionality",
        ));
    }
    let (dot_product, left_norm_sq, right_norm_sq) = left.iter().zip(right).fold(
        (0.0f64, 0.0f64, 0.0f64),
        |(dot, left_sq, right_sq), (l, r)| {
            let left_value = f64::from(*l);
            let right_value = f64::from(*r);
            (
                dot + left_value * right_value,
                left_sq + left_value * left_value,
                right_sq + right_value * right_value,
            )
        },
    );
    if left_norm_sq == 0.0 || right_norm_sq == 0.0 {
        return Err(malformed_input(
            "cosine distance requires non-zero embeddings",
        ));
    }
    let similarity = dot_product / (left_norm_sq.sqrt() * right_norm_sq.sqrt());
    Ok((1.0 - similarity).max(0.0) as f32)
}

fn cosine_distance_normalized(
    left: &[f32],
    right: &[f32],
) -> Result<f32, StreamingClusteringError> {
    if left.len() != right.len() {
        return Err(malformed_input(
            "cosine distance requires equal embedding dimensionality",
        ));
    }
    let similarity = left
        .iter()
        .zip(right)
        .map(|(l, r)| f64::from(*l) * f64::from(*r))
        .sum::<f64>();
    Ok((1.0 - similarity).max(0.0) as f32)
}

fn deterministic_embedding_hash(embedding: &[f32], seed: u64) -> u64 {
    let mut hash = 0x517c_c1b7_2722_0a95u64 ^ seed;
    for value in embedding {
        hash ^= u64::from(value.to_bits());
        hash = hash.rotate_left(17).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    }
    hash
}

fn malformed_input(message: impl Into<String>) -> StreamingClusteringError {
    StreamingClusteringError::MalformedInput {
        message: message.into(),
    }
}

fn unsatisfiable_constraint(message: impl Into<String>) -> StreamingClusteringError {
    StreamingClusteringError::UnsatisfiableConstraint {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use lexongraph_linear_algebra_acceleration::{
        DenseDistanceMetric, ExecutionBackendRequest, ExecutionBackendResolution,
        dense_distance_matrix, detected_execution_backend_selection,
        with_execution_backend_request,
    };

    use super::{
        assign_points_accelerated, assign_points_cpu, best_cluster, best_cluster_from_distances,
    };

    #[test]
    fn best_cluster_from_distances_prefers_previous_assignment_on_tie() {
        let cluster = best_cluster_from_distances(&[0.25, 0.25, 0.5], Some(1)).unwrap();
        assert_eq!(cluster, 1);
    }

    #[test]
    fn best_cluster_from_distances_picks_lowest_cluster_without_previous_tie() {
        let cluster = best_cluster_from_distances(&[0.25, 0.25, 0.5], None).unwrap();
        assert_eq!(cluster, 0);
    }

    #[test]
    fn best_cluster_matches_shared_cosine_metric_for_approximate_unit_vectors() {
        let embedding = vec![0.8, 0.59, 0.1];
        let centroids = vec![
            vec![0.79, 0.6, 0.1],
            vec![0.77, 0.62, 0.1],
            vec![0.1, 0.1, 0.99],
        ];
        let centroid_refs = centroids
            .iter()
            .map(std::vec::Vec::as_slice)
            .collect::<Vec<_>>();
        let distances = with_execution_backend_request(ExecutionBackendRequest::Cpu, || {
            dense_distance_matrix(
                &[embedding.as_slice()],
                centroid_refs.as_slice(),
                DenseDistanceMetric::Cosine,
            )
            .unwrap()
        });

        let direct = best_cluster(&embedding, &centroids, Some(1)).unwrap();
        let shared = best_cluster_from_distances(&distances, Some(1)).unwrap();

        assert_eq!(direct, shared);
    }

    #[test]
    fn accelerated_assignment_matches_cpu_assignment_when_forced_to_cpu_backend() {
        let embeddings = (0..513)
            .map(|index| normalized_test_pattern(index, 8))
            .collect::<Vec<_>>();
        let centroids = (0..2048)
            .map(|index| normalized_test_pattern(index + 10_000, 8))
            .collect::<Vec<_>>();
        let previous_assignments = embeddings
            .iter()
            .enumerate()
            .map(|(index, _)| index % centroids.len())
            .collect::<Vec<_>>();

        let accelerated = with_execution_backend_request(ExecutionBackendRequest::Cpu, || {
            assign_points_accelerated(
                &embeddings,
                &centroids,
                Some(previous_assignments.as_slice()),
            )
            .unwrap()
        });
        let cpu = assign_points_cpu(
            &embeddings,
            &centroids,
            Some(previous_assignments.as_slice()),
        )
        .unwrap();

        assert_eq!(accelerated, cpu);
    }

    #[test]
    fn forced_wgpu_request_bypasses_assignment_size_threshold_when_supported() {
        let selection = with_execution_backend_request(ExecutionBackendRequest::Wgpu, || {
            detected_execution_backend_selection()
        });
        if selection.resolution != ExecutionBackendResolution::Wgpu {
            return;
        }

        let embeddings = vec![normalized_test_pattern(1, 8)];
        let centroids = vec![normalized_test_pattern(2, 8), normalized_test_pattern(3, 8)];

        let should_accelerate =
            with_execution_backend_request(ExecutionBackendRequest::Wgpu, || {
                super::should_use_accelerated_assignments(&embeddings, &centroids)
            });

        assert!(should_accelerate);
    }

    #[test]
    fn accelerated_assignment_matches_cpu_assignment_when_wgpu_is_supported() {
        let selection = with_execution_backend_request(ExecutionBackendRequest::Wgpu, || {
            detected_execution_backend_selection()
        });
        if selection.resolution != ExecutionBackendResolution::Wgpu {
            return;
        }

        let embeddings = (0..257)
            .map(|index| normalized_test_pattern(index, 96))
            .collect::<Vec<_>>();
        let centroids = (0..32)
            .map(|index| normalized_test_pattern(index + 10_000, 96))
            .collect::<Vec<_>>();
        let previous_assignments = embeddings
            .iter()
            .enumerate()
            .map(|(index, _)| index % centroids.len())
            .collect::<Vec<_>>();
        for embedding in &embeddings {
            assert!(
                nearest_assignment_margin(embedding, &centroids) > 1e-5,
                "test fixture should avoid near-ties across CPU/WGPU precision boundaries"
            );
        }

        let accelerated = with_execution_backend_request(ExecutionBackendRequest::Wgpu, || {
            assign_points_accelerated(
                &embeddings,
                &centroids,
                Some(previous_assignments.as_slice()),
            )
            .unwrap()
        });
        let cpu = assign_points_cpu(
            &embeddings,
            &centroids,
            Some(previous_assignments.as_slice()),
        )
        .unwrap();

        assert_eq!(accelerated, cpu);
    }

    fn nearest_assignment_margin(embedding: &[f32], centroids: &[Vec<f32>]) -> f32 {
        let mut best = f32::INFINITY;
        let mut second_best = f32::INFINITY;
        for centroid in centroids {
            let candidate = super::cosine_distance(embedding, centroid.as_slice()).unwrap();
            if candidate < best {
                second_best = best;
                best = candidate;
            } else if candidate < second_best {
                second_best = candidate;
            }
        }
        second_best - best
    }

    fn normalized_test_pattern(seed: usize, dimensions: usize) -> Vec<f32> {
        let mut values = Vec::with_capacity(dimensions);
        for dimension in 0..dimensions {
            let angle = ((seed * 37 + dimension * 17 + 1) % 997) as f32;
            values.push((angle * 0.013).sin() + (angle * 0.007).cos() * 0.5);
        }
        let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
        values.into_iter().map(|value| value / norm).collect()
    }
}
