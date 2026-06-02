// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Deterministic capacity-constrained balanced clustering for LexonGraph.

mod solver;

#[cfg(feature = "tch")]
mod torch_backend;

use std::fmt;

use solver::solve_lexicographic_assignment;

#[cfg(feature = "tch")]
pub use torch_backend::TorchBackend;

pub const EPSILON: f64 = 1e-12;

type DenseVectors = Vec<Vec<f64>>;
type CentroidState = (DenseVectors, DenseVectors);

#[derive(Clone, Debug, PartialEq)]
pub struct DcbcInput {
    pub x: DenseVectors,
    pub cluster_count: usize,
    pub min_cluster_size: usize,
    pub max_cluster_size: usize,
    pub iteration_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DcbcRunResult {
    pub assignment: Vec<usize>,
    pub centroids: DenseVectors,
    pub metadata: DcbcMetadata,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DcbcMetadata {
    pub iteration_count: usize,
    pub cluster_sizes: Vec<usize>,
    pub objective_value: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DcbcError {
    MixedDimensionality {
        expected: usize,
        actual: usize,
        point_index: usize,
    },
    NonFiniteValue {
        point_index: usize,
        dimension_index: usize,
    },
    ZeroNormVector {
        point_index: usize,
    },
    InvalidClusterCount {
        cluster_count: usize,
    },
    InvalidMinimumClusterSize {
        min_cluster_size: usize,
    },
    InvalidClusterBounds {
        min_cluster_size: usize,
        max_cluster_size: usize,
    },
    InvalidIterationCount {
        iteration_count: usize,
    },
    InfeasibleCapacityConstraints {
        point_count: usize,
        cluster_count: usize,
        min_cluster_size: usize,
        max_cluster_size: usize,
    },
    AssignmentInfeasible,
    InvalidNumericState(String),
    BackendFailure(String),
}

impl fmt::Display for DcbcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MixedDimensionality {
                expected,
                actual,
                point_index,
            } => write!(
                f,
                "point {point_index} has dimensionality {actual}, expected {expected}"
            ),
            Self::NonFiniteValue {
                point_index,
                dimension_index,
            } => write!(
                f,
                "point {point_index} contains a non-finite value at dimension {dimension_index}"
            ),
            Self::ZeroNormVector { point_index } => {
                write!(f, "point {point_index} has zero Euclidean norm")
            }
            Self::InvalidClusterCount { cluster_count } => {
                write!(f, "cluster_count must be at least 1, got {cluster_count}")
            }
            Self::InvalidMinimumClusterSize { min_cluster_size } => write!(
                f,
                "min_cluster_size must be at least 1, got {min_cluster_size}"
            ),
            Self::InvalidClusterBounds {
                min_cluster_size,
                max_cluster_size,
            } => write!(
                f,
                "expected min_cluster_size <= max_cluster_size, got {min_cluster_size} > {max_cluster_size}"
            ),
            Self::InvalidIterationCount { iteration_count } => {
                write!(
                    f,
                    "iteration_count must be at least 1, got {iteration_count}"
                )
            }
            Self::InfeasibleCapacityConstraints {
                point_count,
                cluster_count,
                min_cluster_size,
                max_cluster_size,
            } => write!(
                f,
                "capacity constraints are infeasible for {point_count} points with cluster_count={cluster_count}, min_cluster_size={min_cluster_size}, max_cluster_size={max_cluster_size}"
            ),
            Self::AssignmentInfeasible => write!(
                f,
                "the assignment solver could not produce a feasible assignment"
            ),
            Self::InvalidNumericState(message) => write!(f, "invalid numeric state: {message}"),
            Self::BackendFailure(message) => write!(f, "numeric backend failure: {message}"),
        }
    }
}

impl std::error::Error for DcbcError {}

pub trait NumericBackend {
    fn pairwise_cosine_distances(
        &self,
        normalized_points: &[&[f64]],
        normalized_centroids: &[&[f64]],
    ) -> Result<Vec<f64>, DcbcError>;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PureRustBackend;

impl NumericBackend for PureRustBackend {
    fn pairwise_cosine_distances(
        &self,
        normalized_points: &[&[f64]],
        normalized_centroids: &[&[f64]],
    ) -> Result<Vec<f64>, DcbcError> {
        if normalized_centroids.is_empty() {
            return Err(DcbcError::BackendFailure(
                "pure Rust backend requires at least one centroid".into(),
            ));
        }

        let mut distances =
            Vec::with_capacity(normalized_points.len() * normalized_centroids.len());
        for point in normalized_points {
            for centroid in normalized_centroids {
                distances.push(cosine_distance_from_normalized(point, centroid)?);
            }
        }
        Ok(distances)
    }
}

pub fn run_dcbc(input: &DcbcInput) -> Result<DcbcRunResult, DcbcError> {
    run_dcbc_with_backend(input, &PureRustBackend)
}

pub fn run_dcbc_with_backend<B>(input: &DcbcInput, backend: &B) -> Result<DcbcRunResult, DcbcError>
where
    B: NumericBackend,
{
    let prepared = PreparedInput::new(input)?;
    let (mut raw_centroids, mut normalized_centroids) =
        initialize_centroids(input, &prepared.normalized_points, backend)?;
    let mut final_assignment = Vec::new();
    let mut final_cluster_sizes = Vec::new();

    for _ in 0..input.iteration_count {
        let distances =
            backend_distance_matrix(backend, &prepared.normalized_points, &normalized_centroids)?;
        let assignment = solve_lexicographic_assignment(
            &distances,
            prepared.point_count,
            input.cluster_count,
            input.min_cluster_size,
            input.max_cluster_size,
        )?;
        let memberships = materialize_memberships(
            &assignment,
            input.cluster_count,
            input.min_cluster_size,
            input.max_cluster_size,
        )?;
        let (next_raw, next_normalized) = recompute_centroids(
            &input.x,
            &prepared.normalized_points,
            &memberships,
            prepared.dimension,
        )?;

        final_cluster_sizes = memberships.iter().map(Vec::len).collect();
        final_assignment = assignment;
        raw_centroids = next_raw;
        normalized_centroids = next_normalized;
    }

    let objective_value = compute_objective(
        &prepared.normalized_points,
        &normalized_centroids,
        &final_assignment,
    )?;

    Ok(DcbcRunResult {
        assignment: final_assignment,
        centroids: raw_centroids,
        metadata: DcbcMetadata {
            iteration_count: input.iteration_count,
            cluster_sizes: final_cluster_sizes,
            objective_value,
        },
    })
}

#[derive(Clone, Debug)]
struct PreparedInput {
    normalized_points: DenseVectors,
    point_count: usize,
    dimension: usize,
}

impl PreparedInput {
    fn new(input: &DcbcInput) -> Result<Self, DcbcError> {
        validate_scalar_bounds(input)?;

        let point_count = input.x.len();
        let dimension = input.x.first().map_or(0, Vec::len);
        validate_capacity_bounds(input, point_count)?;

        let mut normalized_points = Vec::with_capacity(point_count);
        for (point_index, point) in input.x.iter().enumerate() {
            if point.len() != dimension {
                return Err(DcbcError::MixedDimensionality {
                    expected: dimension,
                    actual: point.len(),
                    point_index,
                });
            }

            let norm = euclidean_norm(point, point_index)?;
            if norm == 0.0 {
                return Err(DcbcError::ZeroNormVector { point_index });
            }

            normalized_points.push(normalize_vector(point, norm)?);
        }

        Ok(Self {
            normalized_points,
            point_count,
            dimension,
        })
    }
}

fn validate_scalar_bounds(input: &DcbcInput) -> Result<(), DcbcError> {
    if input.cluster_count == 0 {
        return Err(DcbcError::InvalidClusterCount {
            cluster_count: input.cluster_count,
        });
    }
    if input.min_cluster_size == 0 {
        return Err(DcbcError::InvalidMinimumClusterSize {
            min_cluster_size: input.min_cluster_size,
        });
    }
    if input.min_cluster_size > input.max_cluster_size {
        return Err(DcbcError::InvalidClusterBounds {
            min_cluster_size: input.min_cluster_size,
            max_cluster_size: input.max_cluster_size,
        });
    }
    if input.iteration_count == 0 {
        return Err(DcbcError::InvalidIterationCount {
            iteration_count: input.iteration_count,
        });
    }
    Ok(())
}

fn validate_capacity_bounds(input: &DcbcInput, point_count: usize) -> Result<(), DcbcError> {
    let minimum_required = input
        .cluster_count
        .checked_mul(input.min_cluster_size)
        .ok_or_else(|| {
            DcbcError::InvalidNumericState("cluster lower-bound multiplication overflowed".into())
        })?;
    let maximum_supported = input
        .cluster_count
        .checked_mul(input.max_cluster_size)
        .ok_or_else(|| {
            DcbcError::InvalidNumericState("cluster upper-bound multiplication overflowed".into())
        })?;

    if minimum_required > point_count || point_count > maximum_supported {
        return Err(DcbcError::InfeasibleCapacityConstraints {
            point_count,
            cluster_count: input.cluster_count,
            min_cluster_size: input.min_cluster_size,
            max_cluster_size: input.max_cluster_size,
        });
    }

    Ok(())
}

fn initialize_centroids<B>(
    input: &DcbcInput,
    normalized_points: &[Vec<f64>],
    backend: &B,
) -> Result<CentroidState, DcbcError>
where
    B: NumericBackend,
{
    let point_refs: Vec<&[f64]> = normalized_points.iter().map(Vec::as_slice).collect();
    let mut raw_centroids = Vec::with_capacity(input.cluster_count);
    let mut normalized_centroids = Vec::with_capacity(input.cluster_count);

    raw_centroids.push(input.x[0].clone());
    normalized_centroids.push(normalized_points[0].clone());

    for _ in 1..input.cluster_count {
        let centroid_refs: Vec<&[f64]> = normalized_centroids.iter().map(Vec::as_slice).collect();
        let distances = backend.pairwise_cosine_distances(&point_refs, &centroid_refs)?;
        let expected_len = normalized_points
            .len()
            .checked_mul(normalized_centroids.len())
            .ok_or_else(|| {
                DcbcError::InvalidNumericState(
                    "initialization distance matrix size overflowed".into(),
                )
            })?;
        if distances.len() != expected_len {
            return Err(DcbcError::BackendFailure(format!(
                "expected {} initialization distances, got {}",
                expected_len,
                distances.len()
            )));
        }

        let mut best_index = 0usize;
        let mut best_min_distance = f64::NEG_INFINITY;
        for point_index in 0..normalized_points.len() {
            let mut min_distance = distances[point_index * normalized_centroids.len()];
            ensure_finite_distance(min_distance)?;
            for centroid_index in 1..normalized_centroids.len() {
                let candidate =
                    distances[point_index * normalized_centroids.len() + centroid_index];
                ensure_finite_distance(candidate)?;
                if candidate + EPSILON < min_distance {
                    min_distance = candidate;
                }
            }

            if min_distance > best_min_distance + EPSILON {
                best_index = point_index;
                best_min_distance = min_distance;
            }
        }

        raw_centroids.push(input.x[best_index].clone());
        normalized_centroids.push(normalized_points[best_index].clone());
    }

    Ok((raw_centroids, normalized_centroids))
}

fn backend_distance_matrix<B>(
    backend: &B,
    normalized_points: &[Vec<f64>],
    normalized_centroids: &[Vec<f64>],
) -> Result<Vec<f64>, DcbcError>
where
    B: NumericBackend,
{
    let point_refs: Vec<&[f64]> = normalized_points.iter().map(Vec::as_slice).collect();
    let centroid_refs: Vec<&[f64]> = normalized_centroids.iter().map(Vec::as_slice).collect();
    let distances = backend.pairwise_cosine_distances(&point_refs, &centroid_refs)?;

    let expected_len = normalized_points
        .len()
        .checked_mul(normalized_centroids.len())
        .ok_or_else(|| DcbcError::InvalidNumericState("distance matrix size overflowed".into()))?;
    if distances.len() != expected_len {
        return Err(DcbcError::BackendFailure(format!(
            "expected {} distances, got {}",
            expected_len,
            distances.len()
        )));
    }
    for value in &distances {
        ensure_finite_distance(*value)?;
    }

    Ok(distances)
}

fn materialize_memberships(
    assignment: &[usize],
    cluster_count: usize,
    min_cluster_size: usize,
    max_cluster_size: usize,
) -> Result<Vec<Vec<usize>>, DcbcError> {
    let mut memberships = vec![Vec::new(); cluster_count];
    for (point_index, &cluster_index) in assignment.iter().enumerate() {
        let Some(cluster) = memberships.get_mut(cluster_index) else {
            return Err(DcbcError::AssignmentInfeasible);
        };
        cluster.push(point_index);
    }

    if memberships
        .iter()
        .any(|members| members.len() < min_cluster_size || members.len() > max_cluster_size)
    {
        return Err(DcbcError::AssignmentInfeasible);
    }

    Ok(memberships)
}

fn recompute_centroids(
    points: &[Vec<f64>],
    normalized_points: &[Vec<f64>],
    memberships: &[Vec<usize>],
    dimension: usize,
) -> Result<CentroidState, DcbcError> {
    let mut raw_centroids = Vec::with_capacity(memberships.len());
    let mut normalized_centroids = Vec::with_capacity(memberships.len());

    for members in memberships {
        if members.is_empty() {
            return Err(DcbcError::AssignmentInfeasible);
        }

        let mut centroid = vec![0.0; dimension];
        for &point_index in members {
            for (slot, value) in centroid.iter_mut().zip(&points[point_index]) {
                *slot += value;
            }
        }

        let divisor = members.len() as f64;
        for value in &mut centroid {
            *value /= divisor;
            if !value.is_finite() {
                return Err(DcbcError::InvalidNumericState(
                    "raw centroid contained a non-finite value".into(),
                ));
            }
        }

        let norm = euclidean_norm_for_centroid(&centroid)?;
        let normalized = if norm < EPSILON {
            normalized_points[members[0]].clone()
        } else {
            normalize_vector(&centroid, norm)?
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
) -> Result<f64, DcbcError> {
    let mut objective = 0.0;
    for (point_index, &cluster_index) in assignment.iter().enumerate() {
        let Some(normalized_centroid) = normalized_centroids.get(cluster_index) else {
            return Err(DcbcError::AssignmentInfeasible);
        };
        objective +=
            cosine_distance_from_normalized(&normalized_points[point_index], normalized_centroid)?;
    }
    if !objective.is_finite() {
        return Err(DcbcError::InvalidNumericState(
            "objective value was non-finite".into(),
        ));
    }
    Ok(objective)
}

pub(crate) fn assignment_cost(
    distances: &[f64],
    cluster_count: usize,
    assignment: &[usize],
) -> Result<f64, DcbcError> {
    let mut total = 0.0;
    for (point_index, &cluster_index) in assignment.iter().enumerate() {
        if cluster_index >= cluster_count {
            return Err(DcbcError::AssignmentInfeasible);
        }
        let cost = distances[point_index * cluster_count + cluster_index];
        ensure_finite_distance(cost)?;
        total += cost;
    }
    if !total.is_finite() {
        return Err(DcbcError::InvalidNumericState(
            "assignment cost was non-finite".into(),
        ));
    }
    Ok(total)
}

pub(crate) fn costs_equal(left: f64, right: f64) -> bool {
    (left - right).abs() < EPSILON
}

fn euclidean_norm(vector: &[f64], point_index: usize) -> Result<f64, DcbcError> {
    let mut sum = 0.0;
    for (dimension_index, value) in vector.iter().enumerate() {
        if !value.is_finite() {
            return Err(DcbcError::NonFiniteValue {
                point_index,
                dimension_index,
            });
        }
        sum += value * value;
    }
    if !sum.is_finite() {
        return Err(DcbcError::InvalidNumericState(
            "input vector norm overflowed".into(),
        ));
    }
    Ok(sum.sqrt())
}

fn euclidean_norm_for_centroid(vector: &[f64]) -> Result<f64, DcbcError> {
    let mut sum = 0.0;
    for value in vector {
        sum += value * value;
    }
    if !sum.is_finite() {
        return Err(DcbcError::InvalidNumericState(
            "centroid norm overflowed".into(),
        ));
    }
    Ok(sum.sqrt())
}

fn normalize_vector(vector: &[f64], norm: f64) -> Result<Vec<f64>, DcbcError> {
    let mut normalized = Vec::with_capacity(vector.len());
    for value in vector {
        let normalized_value = value / norm;
        if !normalized_value.is_finite() {
            return Err(DcbcError::InvalidNumericState(
                "normalization produced a non-finite value".into(),
            ));
        }
        normalized.push(normalized_value);
    }
    Ok(normalized)
}

pub(crate) fn cosine_distance_from_normalized(
    normalized_point: &[f64],
    normalized_centroid: &[f64],
) -> Result<f64, DcbcError> {
    if normalized_point.len() != normalized_centroid.len() {
        return Err(DcbcError::BackendFailure(format!(
            "cosine distance requires matching dimensions, got {} and {}",
            normalized_point.len(),
            normalized_centroid.len()
        )));
    }
    let mut dot = 0.0;
    for (point_value, centroid_value) in normalized_point.iter().zip(normalized_centroid) {
        dot += point_value * centroid_value;
    }
    let distance = 1.0 - dot;
    ensure_finite_distance(distance)?;
    Ok(distance)
}

fn ensure_finite_distance(distance: f64) -> Result<(), DcbcError> {
    if distance.is_finite() {
        Ok(())
    } else {
        Err(DcbcError::InvalidNumericState(
            "distance computation produced a non-finite value".into(),
        ))
    }
}
