// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Streaming deterministic capacity-constrained balanced clustering for LexonGraph.

mod solver;

use lexongraph_streaming_clustering::{
    ClusterId, MetricDirection, PassReport, StreamingClusterClassifier, StreamingClusterTrainer,
    StreamingClusteringConfig, StreamingClusteringError, TrainerState, validate_config,
    validate_embedding,
};
use solver::solve_lexicographic_assignment;

pub const EPSILON: f64 = 1e-12;

type DenseVectors = Vec<Vec<f64>>;

#[derive(Clone, Debug)]
pub struct DcbcStreamingTrainer {
    config: StreamingClusteringConfig,
    state: TrainerState,
    current_pass: Vec<Vec<f32>>,
    baseline_pass: Option<Vec<Vec<f32>>>,
    completed_passes: usize,
    occupancy_bounds: Option<OccupancyBounds>,
    raw_centroids: Option<DenseVectors>,
    normalized_centroids: Option<DenseVectors>,
}

#[derive(Clone, Debug)]
pub struct DcbcStreamingClassifier {
    config: StreamingClusteringConfig,
    normalized_centroids: DenseVectors,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OccupancyBounds {
    min: usize,
    max: usize,
}

#[derive(Clone, Debug, PartialEq)]
struct IterationResult {
    assignment: Vec<usize>,
    raw_centroids: DenseVectors,
    normalized_centroids: DenseVectors,
    cluster_sizes: Vec<usize>,
    objective_value: f64,
}

#[derive(Clone, Debug)]
struct PreparedPass {
    raw_points: DenseVectors,
    normalized_points: DenseVectors,
    dimensions: usize,
}

impl DcbcStreamingTrainer {
    pub fn new(config: StreamingClusteringConfig) -> Result<Self, StreamingClusteringError> {
        validate_config(&config)?;
        reject_unsupported_balance_constraints(&config)?;
        Ok(Self {
            config,
            state: TrainerState::Idle,
            current_pass: Vec::new(),
            baseline_pass: None,
            completed_passes: 0,
            occupancy_bounds: None,
            raw_centroids: None,
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

        let prepared = prepare_pass(&self.current_pass, self.config.dimensions)?;
        let cluster_count = self.config.cluster_count as usize;
        let bounds = match self.occupancy_bounds {
            Some(bounds) => bounds,
            None => derive_occupancy_bounds(&self.config, prepared.raw_points.len())?,
        };

        let start_normalized_centroids = if self.completed_passes == 0 {
            self.baseline_pass = Some(self.current_pass.clone());
            self.occupancy_bounds = Some(bounds);
            let (_, normalized_centroids) = initialize_centroids(
                &prepared.raw_points,
                &prepared.normalized_points,
                cluster_count,
            )?;
            normalized_centroids
        } else {
            let baseline_pass = self.baseline_pass.as_ref().ok_or_else(|| {
                constraint_error("missing baseline dataset for later DCBC passes")
            })?;
            validate_dataset_continuity(baseline_pass, &self.current_pass)?;
            self.normalized_centroids
                .clone()
                .ok_or_else(|| constraint_error("missing centroid state for later DCBC passes"))?
        };

        let result = run_iteration(
            &prepared.raw_points,
            &prepared.normalized_points,
            &start_normalized_centroids,
            prepared.dimensions,
            bounds,
            cluster_count,
        )?;

        self.completed_passes += 1;
        self.raw_centroids = Some(result.raw_centroids.clone());
        self.normalized_centroids = Some(result.normalized_centroids.clone());
        self.current_pass.clear();
        self.state = TrainerState::PassComplete;

        Ok(PassReport {
            observed_count: prepared.raw_points.len(),
            quality_metric: result.objective_value,
            balance_metric: balance_metric(self.config.balance_constraints.as_ref()),
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: (0..self.config.cluster_count).collect(),
        })
    }
}

impl StreamingClusterTrainer for DcbcStreamingTrainer {
    type Classifier = DcbcStreamingClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn state(&self) -> TrainerState {
        self.state
    }

    fn ingest_batch(&mut self, embeddings: &[Vec<f32>]) -> Result<(), StreamingClusteringError> {
        match self.state {
            TrainerState::Idle | TrainerState::PassComplete => {
                self.state = TrainerState::Ingesting;
            }
            TrainerState::Ingesting => {}
            TrainerState::TrainingComplete | TrainerState::Error => {
                return Err(self.invalid_transition("ingest_batch"));
            }
        }

        let mut validated_batch = Vec::with_capacity(embeddings.len());
        for embedding in embeddings {
            validate_embedding(embedding, self.config.dimensions)?;
            ensure_non_zero_norm(embedding)?;
            validated_batch.push(embedding.clone());
        }

        self.current_pass.extend(validated_batch);
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

        Ok(DcbcStreamingClassifier {
            config: self.config,
            normalized_centroids,
        })
    }
}

impl StreamingClusterClassifier for DcbcStreamingClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        validate_embedding(embedding, self.config.dimensions)?;
        ensure_non_zero_norm(embedding)?;
        let normalized_embedding = normalize_embedding(embedding)?;

        let mut best_cluster = 0usize;
        let mut best_distance = cosine_distance_from_normalized(
            normalized_embedding.as_slice(),
            self.normalized_centroids[0].as_slice(),
        )?;
        for cluster_index in 1..self.normalized_centroids.len() {
            let candidate = cosine_distance_from_normalized(
                normalized_embedding.as_slice(),
                self.normalized_centroids[cluster_index].as_slice(),
            )?;
            if candidate + EPSILON < best_distance {
                best_distance = candidate;
                best_cluster = cluster_index;
            }
        }

        Ok(best_cluster as ClusterId)
    }
}

fn reject_unsupported_balance_constraints(
    config: &StreamingClusteringConfig,
) -> Result<(), StreamingClusteringError> {
    let Some(constraints) = &config.balance_constraints else {
        return Ok(());
    };

    if constraints.max_cluster_size_ratio.is_some() {
        return Err(StreamingClusteringError::InvalidConfiguration {
            message: "max_cluster_size_ratio is not supported by the streaming DCBC realization"
                .into(),
        });
    }
    if constraints.soft_balance_penalty.is_some() {
        return Err(StreamingClusteringError::InvalidConfiguration {
            message: "soft_balance_penalty is not supported by the streaming DCBC realization"
                .into(),
        });
    }

    Ok(())
}

fn ensure_non_zero_norm(embedding: &[f32]) -> Result<(), StreamingClusteringError> {
    let squared_norm = embedding
        .iter()
        .map(|value| {
            let value = *value as f64;
            value * value
        })
        .sum::<f64>();
    if squared_norm.sqrt() < EPSILON {
        return Err(StreamingClusteringError::MalformedInput {
            message: "embeddings must have non-zero Euclidean norm".into(),
        });
    }
    Ok(())
}

fn normalize_embedding(embedding: &[f32]) -> Result<Vec<f64>, StreamingClusteringError> {
    let raw: Vec<f64> = embedding.iter().map(|value| *value as f64).collect();
    let norm = euclidean_norm(raw.as_slice())?;
    normalize_vector(raw.as_slice(), norm)
}

fn prepare_pass(
    embeddings: &[Vec<f32>],
    dimensions: usize,
) -> Result<PreparedPass, StreamingClusteringError> {
    let mut raw_points = Vec::with_capacity(embeddings.len());
    let mut normalized_points = Vec::with_capacity(embeddings.len());

    for embedding in embeddings {
        validate_embedding(embedding, dimensions)?;
        ensure_non_zero_norm(embedding)?;
        let raw: Vec<f64> = embedding.iter().map(|value| *value as f64).collect();
        let norm = euclidean_norm(raw.as_slice())?;
        let normalized = normalize_vector(raw.as_slice(), norm)?;
        raw_points.push(raw);
        normalized_points.push(normalized);
    }

    Ok(PreparedPass {
        raw_points,
        normalized_points,
        dimensions,
    })
}

fn derive_occupancy_bounds(
    config: &StreamingClusteringConfig,
    observed_count: usize,
) -> Result<OccupancyBounds, StreamingClusteringError> {
    let cluster_count = config.cluster_count as usize;
    if observed_count < cluster_count {
        return Err(StreamingClusteringError::UnsatisfiableConstraint {
            message: format!(
                "first pass established N = {observed_count} which is smaller than K = {}",
                config.cluster_count
            ),
        });
    }

    let min = config
        .balance_constraints
        .as_ref()
        .and_then(|constraints| constraints.min_cluster_occupancy)
        .unwrap_or(1) as usize;
    let max = if let Some(max_cluster_occupancy) = config
        .balance_constraints
        .as_ref()
        .and_then(|constraints| constraints.max_cluster_occupancy)
    {
        max_cluster_occupancy as usize
    } else {
        observed_count
            .checked_sub((cluster_count - 1) * min)
            .ok_or_else(|| constraint_error("observed_count underflow while deriving occupancy"))?
    };

    let minimum_required = cluster_count
        .checked_mul(min)
        .ok_or_else(|| constraint_error("cluster lower-bound multiplication overflowed"))?;
    let maximum_supported = cluster_count
        .checked_mul(max)
        .ok_or_else(|| constraint_error("cluster upper-bound multiplication overflowed"))?;

    if minimum_required > observed_count || observed_count > maximum_supported {
        return Err(StreamingClusteringError::UnsatisfiableConstraint {
            message: format!(
                "capacity constraints are infeasible for N = {observed_count}, K = {}, min = {min}, max = {max}",
                config.cluster_count
            ),
        });
    }

    Ok(OccupancyBounds { min, max })
}

fn validate_dataset_continuity(
    baseline: &[Vec<f32>],
    current_pass: &[Vec<f32>],
) -> Result<(), StreamingClusteringError> {
    if baseline.len() != current_pass.len() {
        return Err(StreamingClusteringError::MalformedInput {
            message: format!(
                "later pass observed_count {} does not match baseline {}",
                current_pass.len(),
                baseline.len()
            ),
        });
    }

    for (point_index, (baseline_embedding, current_embedding)) in
        baseline.iter().zip(current_pass.iter()).enumerate()
    {
        if baseline_embedding != current_embedding {
            return Err(StreamingClusteringError::MalformedInput {
                message: format!(
                    "later pass embedding at index {point_index} does not match the baseline dataset order"
                ),
            });
        }
    }

    Ok(())
}

fn run_iteration(
    raw_points: &[Vec<f64>],
    normalized_points: &[Vec<f64>],
    start_normalized_centroids: &[Vec<f64>],
    dimensions: usize,
    bounds: OccupancyBounds,
    cluster_count: usize,
) -> Result<IterationResult, StreamingClusteringError> {
    let distances = distance_matrix(normalized_points, start_normalized_centroids)?;
    let assignment = solve_lexicographic_assignment(
        distances.as_slice(),
        raw_points.len(),
        cluster_count,
        bounds.min,
        bounds.max,
    )?;
    let memberships = materialize_memberships(assignment.as_slice(), cluster_count, bounds)?;
    let (raw_centroids, normalized_centroids) = recompute_centroids(
        raw_points,
        normalized_points,
        memberships.as_slice(),
        dimensions,
    )?;
    let cluster_sizes = memberships.iter().map(Vec::len).collect::<Vec<_>>();
    let objective_value = compute_objective(
        normalized_points,
        normalized_centroids.as_slice(),
        assignment.as_slice(),
    )?;

    Ok(IterationResult {
        assignment,
        raw_centroids,
        normalized_centroids,
        cluster_sizes,
        objective_value,
    })
}

fn initialize_centroids(
    raw_points: &[Vec<f64>],
    normalized_points: &[Vec<f64>],
    cluster_count: usize,
) -> Result<(DenseVectors, DenseVectors), StreamingClusteringError> {
    let mut raw_centroids = Vec::with_capacity(cluster_count);
    let mut normalized_centroids = Vec::with_capacity(cluster_count);

    raw_centroids.push(raw_points[0].clone());
    normalized_centroids.push(normalized_points[0].clone());

    for _ in 1..cluster_count {
        let distances = distance_matrix(normalized_points, normalized_centroids.as_slice())?;
        let mut best_index = 0usize;
        let mut best_min_distance = f64::NEG_INFINITY;

        for point_index in 0..normalized_points.len() {
            let row_start = point_index * normalized_centroids.len();
            let mut min_distance = distances[row_start];
            for centroid_index in 1..normalized_centroids.len() {
                let candidate = distances[row_start + centroid_index];
                if candidate + EPSILON < min_distance {
                    min_distance = candidate;
                }
            }
            if min_distance > best_min_distance + EPSILON {
                best_min_distance = min_distance;
                best_index = point_index;
            }
        }

        raw_centroids.push(raw_points[best_index].clone());
        normalized_centroids.push(normalized_points[best_index].clone());
    }

    Ok((raw_centroids, normalized_centroids))
}

fn distance_matrix(
    normalized_points: &[Vec<f64>],
    normalized_centroids: &[Vec<f64>],
) -> Result<Vec<f64>, StreamingClusteringError> {
    if normalized_centroids.is_empty() {
        return Err(constraint_error(
            "expected at least one centroid while computing distances",
        ));
    }

    let mut distances = Vec::with_capacity(normalized_points.len() * normalized_centroids.len());
    for point in normalized_points {
        for centroid in normalized_centroids {
            distances.push(cosine_distance_from_normalized(
                point.as_slice(),
                centroid.as_slice(),
            )?);
        }
    }
    Ok(distances)
}

fn materialize_memberships(
    assignment: &[usize],
    cluster_count: usize,
    bounds: OccupancyBounds,
) -> Result<Vec<Vec<usize>>, StreamingClusteringError> {
    let mut memberships = vec![Vec::new(); cluster_count];
    for (point_index, &cluster_index) in assignment.iter().enumerate() {
        let Some(cluster) = memberships.get_mut(cluster_index) else {
            return Err(constraint_error("assignment referenced an unknown cluster"));
        };
        cluster.push(point_index);
    }

    if memberships
        .iter()
        .any(|members| members.len() < bounds.min || members.len() > bounds.max)
    {
        return Err(constraint_error(
            "assignment violates the configured occupancy bounds",
        ));
    }

    Ok(memberships)
}

fn recompute_centroids(
    raw_points: &[Vec<f64>],
    normalized_points: &[Vec<f64>],
    memberships: &[Vec<usize>],
    dimensions: usize,
) -> Result<(DenseVectors, DenseVectors), StreamingClusteringError> {
    let mut raw_centroids = Vec::with_capacity(memberships.len());
    let mut normalized_centroids = Vec::with_capacity(memberships.len());

    for members in memberships {
        if members.is_empty() {
            return Err(constraint_error("cluster memberships must never be empty"));
        }

        let mut centroid = vec![0.0; dimensions];
        for &point_index in members {
            for (slot, value) in centroid.iter_mut().zip(raw_points[point_index].iter()) {
                *slot += *value;
            }
        }

        let divisor = members.len() as f64;
        for value in &mut centroid {
            *value /= divisor;
            if !value.is_finite() {
                return Err(constraint_error(
                    "raw centroid contained a non-finite value after recomputation",
                ));
            }
        }

        let norm = euclidean_norm(centroid.as_slice())?;
        let normalized = if norm < EPSILON {
            normalized_points[members[0]].clone()
        } else {
            normalize_vector(centroid.as_slice(), norm)?
        };

        raw_centroids.push(centroid);
        normalized_centroids.push(normalized);
    }

    Ok((raw_centroids, normalized_centroids))
}

fn compute_objective(
    normalized_points: &[Vec<f64>],
    normalized_centroids: &[Vec<f64>],
    assignment: &[usize],
) -> Result<f64, StreamingClusteringError> {
    let mut total = 0.0;
    for (point_index, &cluster_index) in assignment.iter().enumerate() {
        let centroid = normalized_centroids
            .get(cluster_index)
            .ok_or_else(|| constraint_error("assignment referenced an unknown centroid"))?;
        total += cosine_distance_from_normalized(
            normalized_points[point_index].as_slice(),
            centroid.as_slice(),
        )?;
    }
    Ok(total)
}

fn normalize_vector(vector: &[f64], norm: f64) -> Result<Vec<f64>, StreamingClusteringError> {
    if norm < EPSILON {
        return Err(constraint_error(
            "cannot normalize a vector whose norm is below epsilon",
        ));
    }

    let normalized = vector.iter().map(|value| *value / norm).collect::<Vec<_>>();
    if normalized.iter().any(|value| !value.is_finite()) {
        return Err(constraint_error(
            "normalization produced a non-finite value",
        ));
    }
    Ok(normalized)
}

fn cosine_distance_from_normalized(
    left: &[f64],
    right: &[f64],
) -> Result<f64, StreamingClusteringError> {
    let dot = left
        .iter()
        .zip(right.iter())
        .map(|(lhs, rhs)| lhs * rhs)
        .sum::<f64>();
    let distance = 1.0 - dot;
    if !distance.is_finite() {
        return Err(constraint_error("cosine distance became non-finite"));
    }
    Ok(distance)
}

fn euclidean_norm(vector: &[f64]) -> Result<f64, StreamingClusteringError> {
    let norm = vector.iter().map(|value| value * value).sum::<f64>().sqrt();
    if !norm.is_finite() {
        return Err(constraint_error("Euclidean norm became non-finite"));
    }
    Ok(norm)
}

fn balance_metric(
    _balance_constraints: Option<&lexongraph_streaming_clustering::BalanceConstraints>,
) -> f64 {
    0.0
}

pub(crate) fn costs_equal(left: f64, right: f64) -> bool {
    (left - right).abs() < EPSILON
}

pub(crate) fn assignment_cost(
    distances: &[f64],
    cluster_count: usize,
    assignment: &[usize],
) -> Result<f64, StreamingClusteringError> {
    let mut total = 0.0;
    for (point_index, &cluster_index) in assignment.iter().enumerate() {
        let offset = point_index
            .checked_mul(cluster_count)
            .and_then(|value| value.checked_add(cluster_index))
            .ok_or_else(|| constraint_error("assignment cost indexing overflowed"))?;
        total += *distances
            .get(offset)
            .ok_or_else(|| constraint_error("assignment referenced a missing distance entry"))?;
    }
    Ok(total)
}

pub(crate) fn constraint_error(message: impl Into<String>) -> StreamingClusteringError {
    StreamingClusteringError::UnsatisfiableConstraint {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_completed_pass_uses_protocol_farthest_point_initialization() {
        let prepared = prepare_pass(&[vec![1.0, 0.0], vec![0.0, 1.0], vec![-1.0, 0.0]], 2).unwrap();
        let (raw_centroids, _) =
            initialize_centroids(&prepared.raw_points, &prepared.normalized_points, 3).unwrap();

        assert_eq!(raw_centroids[0], vec![1.0, 0.0]);
        assert_eq!(raw_centroids[1], vec![-1.0, 0.0]);
    }

    #[test]
    fn farthest_point_ties_break_by_smaller_point_index() {
        let prepared = prepare_pass(&[vec![0.0, 1.0], vec![1.0, 0.0], vec![-1.0, 0.0]], 2).unwrap();
        let (raw_centroids, _) =
            initialize_centroids(&prepared.raw_points, &prepared.normalized_points, 3).unwrap();

        assert_eq!(raw_centroids[0], vec![0.0, 1.0]);
        assert_eq!(raw_centroids[1], vec![1.0, 0.0]);
    }

    #[test]
    fn lexicographic_assignment_selection_matches_the_protocol() {
        let prepared = prepare_pass(
            &[
                vec![1.0, 0.0],
                vec![1.0, 0.0],
                vec![1.0, 0.0],
                vec![1.0, 0.0],
            ],
            2,
        )
        .unwrap();
        let (_, start_centroids) =
            initialize_centroids(&prepared.raw_points, &prepared.normalized_points, 2).unwrap();
        let result = run_iteration(
            &prepared.raw_points,
            &prepared.normalized_points,
            &start_centroids,
            prepared.dimensions,
            OccupancyBounds { min: 1, max: 3 },
            2,
        )
        .unwrap();

        assert_eq!(result.assignment, vec![0, 0, 0, 1]);
    }

    #[test]
    fn zero_norm_centroids_use_the_smallest_member_for_distance_computations() {
        let prepared = prepare_pass(&[vec![1.0, 0.0], vec![-1.0, 0.0]], 2).unwrap();
        let (_, start_centroids) =
            initialize_centroids(&prepared.raw_points, &prepared.normalized_points, 1).unwrap();
        let result = run_iteration(
            &prepared.raw_points,
            &prepared.normalized_points,
            &start_centroids,
            prepared.dimensions,
            OccupancyBounds { min: 1, max: 2 },
            1,
        )
        .unwrap();

        assert_eq!(result.raw_centroids[0], vec![0.0, 0.0]);
        assert_eq!(result.objective_value, 2.0);
    }

    #[test]
    fn later_passes_must_match_the_baseline_dataset() {
        let config = StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: None,
            random_seed: None,
        };
        let mut trainer = DcbcStreamingTrainer::new(config).unwrap();
        trainer
            .ingest_batch(&[vec![1.0, 0.0], vec![-1.0, 0.0]])
            .unwrap();
        trainer.finish_pass().unwrap();
        trainer
            .ingest_batch(&[vec![1.0, 0.0], vec![0.0, 1.0]])
            .unwrap();

        assert!(matches!(
            trainer.finish_pass(),
            Err(StreamingClusteringError::MalformedInput { .. })
        ));
        assert_eq!(trainer.state(), TrainerState::Error);
    }

    #[test]
    fn unsupported_balance_constraints_are_rejected() {
        let ratio_config = StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: Some(lexongraph_streaming_clustering::BalanceConstraints {
                min_cluster_occupancy: None,
                max_cluster_occupancy: None,
                max_cluster_size_ratio: Some(1.5),
                soft_balance_penalty: None,
            }),
            random_seed: None,
        };
        assert!(matches!(
            reject_unsupported_balance_constraints(&ratio_config),
            Err(StreamingClusteringError::InvalidConfiguration { .. })
        ));

        let penalty_config = StreamingClusteringConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: Some(lexongraph_streaming_clustering::BalanceConstraints {
                min_cluster_occupancy: None,
                max_cluster_occupancy: None,
                max_cluster_size_ratio: None,
                soft_balance_penalty: Some(0.25),
            }),
            random_seed: None,
        };
        assert!(matches!(
            reject_unsupported_balance_constraints(&penalty_config),
            Err(StreamingClusteringError::InvalidConfiguration { .. })
        ));
    }

    #[test]
    fn occupancy_bounds_are_deterministically_derived_from_balance_constraints() {
        let explicit_bounds_config = StreamingClusteringConfig {
            cluster_count: 3,
            dimensions: 2,
            balance_constraints: Some(lexongraph_streaming_clustering::BalanceConstraints {
                min_cluster_occupancy: Some(2),
                max_cluster_occupancy: Some(4),
                max_cluster_size_ratio: None,
                soft_balance_penalty: None,
            }),
            random_seed: None,
        };
        assert_eq!(
            derive_occupancy_bounds(&explicit_bounds_config, 9).unwrap(),
            OccupancyBounds { min: 2, max: 4 }
        );

        let implicit_max_config = StreamingClusteringConfig {
            cluster_count: 3,
            dimensions: 2,
            balance_constraints: Some(lexongraph_streaming_clustering::BalanceConstraints {
                min_cluster_occupancy: Some(2),
                max_cluster_occupancy: None,
                max_cluster_size_ratio: None,
                soft_balance_penalty: None,
            }),
            random_seed: None,
        };
        assert_eq!(
            derive_occupancy_bounds(&implicit_max_config, 8).unwrap(),
            OccupancyBounds { min: 2, max: 4 }
        );
    }
}
