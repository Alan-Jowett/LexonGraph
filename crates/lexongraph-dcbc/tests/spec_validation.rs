use std::path::Path;

use lexongraph_dcbc::{
    DcbcError, DcbcInput, DcbcRunResult, EPSILON, PureRustBackend, run_dcbc, run_dcbc_with_backend,
};

#[test]
fn val_dcbc_001_infeasible_capacity_bounds_fail_explicitly() {
    let too_few_points = fixture(vec![vec![1.0], vec![2.0]], 3, 1, 1, 1);
    assert!(matches!(
        run_dcbc(&too_few_points),
        Err(DcbcError::InfeasibleCapacityConstraints { .. })
    ));

    let too_many_points = fixture(vec![vec![1.0], vec![2.0], vec![3.0]], 1, 1, 2, 1);
    assert!(matches!(
        run_dcbc(&too_many_points),
        Err(DcbcError::InfeasibleCapacityConstraints { .. })
    ));
}

#[test]
fn val_dcbc_002_invalid_scalar_bounds_fail_explicitly() {
    assert!(matches!(
        run_dcbc(&fixture(vec![vec![1.0]], 0, 1, 1, 1)),
        Err(DcbcError::InvalidClusterCount { .. })
    ));
    assert!(matches!(
        run_dcbc(&fixture(vec![vec![1.0]], 1, 0, 1, 1)),
        Err(DcbcError::InvalidMinimumClusterSize { .. })
    ));
    assert!(matches!(
        run_dcbc(&fixture(vec![vec![1.0]], 1, 2, 1, 1)),
        Err(DcbcError::InvalidClusterBounds { .. })
    ));
    assert!(matches!(
        run_dcbc(&fixture(vec![vec![1.0]], 1, 1, 1, 0)),
        Err(DcbcError::InvalidIterationCount { .. })
    ));
}

#[test]
fn val_dcbc_003_mixed_dimensional_vectors_fail_explicitly() {
    let input = fixture(vec![vec![1.0], vec![1.0, 2.0]], 1, 1, 2, 1);
    assert!(matches!(
        run_dcbc(&input),
        Err(DcbcError::MixedDimensionality {
            expected: 1,
            actual: 2,
            point_index: 1,
        })
    ));
}

#[test]
fn val_dcbc_004_non_finite_values_fail_explicitly() {
    let input = fixture(vec![vec![1.0], vec![f64::NAN]], 1, 1, 2, 1);
    assert!(matches!(
        run_dcbc(&input),
        Err(DcbcError::NonFiniteValue {
            point_index: 1,
            dimension_index: 0,
        })
    ));
}

#[test]
fn val_dcbc_005_zero_norm_vectors_fail_explicitly() {
    let input = fixture(vec![vec![0.0]], 1, 1, 1, 1);
    assert!(matches!(
        run_dcbc(&input),
        Err(DcbcError::ZeroNormVector { point_index: 0 })
    ));
}

#[test]
fn val_dcbc_006_repeated_runs_are_deterministic() {
    let input = fixture(
        vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![-1.0, 0.0]],
        2,
        1,
        2,
        2,
    );

    let first = run_dcbc(&input).unwrap();
    let second = run_dcbc(&input).unwrap();
    assert_eq!(first, second);
}

#[test]
fn val_dcbc_007_input_order_is_semantically_significant() {
    let ordered = fixture(
        vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![-1.0, 0.0]],
        3,
        1,
        1,
        1,
    );
    let reordered = fixture(
        vec![vec![0.0, 1.0], vec![1.0, 0.0], vec![-1.0, 0.0]],
        3,
        1,
        1,
        1,
    );

    let first = run_dcbc(&ordered).unwrap();
    let second = run_dcbc(&reordered).unwrap();
    assert_ne!(first.centroids, second.centroids);
}

#[test]
fn val_dcbc_008_initialization_uses_unique_farthest_candidates() {
    let input = fixture(
        vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![-1.0, 0.0]],
        3,
        1,
        1,
        1,
    );

    let result = run_dcbc(&input).unwrap();
    assert_eq!(result.centroids[0], vec![1.0, 0.0]);
    assert_eq!(result.centroids[1], vec![-1.0, 0.0]);
}

#[test]
fn val_dcbc_009_initialization_breaks_farthest_ties_by_smaller_point_index() {
    let input = fixture(
        vec![vec![0.0, 1.0], vec![1.0, 0.0], vec![-1.0, 0.0]],
        3,
        1,
        1,
        1,
    );

    let result = run_dcbc(&input).unwrap();
    assert_eq!(result.centroids[0], vec![0.0, 1.0]);
    assert_eq!(result.centroids[1], vec![1.0, 0.0]);
}

#[test]
fn val_dcbc_010_assignments_cover_each_point_once_and_respect_capacities() {
    let input = fixture(
        vec![
            vec![1.0, 0.0],
            vec![0.9, 0.1],
            vec![-1.0, 0.0],
            vec![-0.9, 0.1],
        ],
        2,
        2,
        2,
        1,
    );

    let result = run_dcbc(&input).unwrap();
    assert_eq!(result.assignment.len(), input.x.len());
    assert_eq!(result.metadata.cluster_sizes, vec![2, 2]);
}

#[test]
fn constrained_unique_optimum_fixture_returns_the_global_minimum_assignment() {
    let input = fixture(
        vec![
            vec![1.0, 2.0, -1.0],
            vec![1.0, 2.0, 2.0],
            vec![2.0, 2.0, 1.0],
            vec![-1.0, -1.0, -2.0],
        ],
        2,
        2,
        2,
        1,
    );

    let result = run_dcbc(&input).unwrap();
    assert_eq!(result.assignment, vec![1, 0, 0, 1]);
}

#[test]
fn val_dcbc_011_multiple_optima_choose_the_lexicographically_minimal_assignment() {
    let input = fixture(
        vec![
            vec![1.0, 0.0],
            vec![1.0, 0.0],
            vec![1.0, 0.0],
            vec![1.0, 0.0],
        ],
        2,
        1,
        3,
        1,
    );

    let result = run_dcbc(&input).unwrap();
    assert_eq!(result.assignment, vec![0, 0, 0, 1]);
}

#[test]
fn val_dcbc_012_centroid_summation_uses_ascending_point_index() {
    let input = fixture(vec![vec![1e16], vec![-1e16], vec![1.0]], 1, 1, 3, 1);

    let result = run_dcbc(&input).unwrap();
    assert_eq!(result.centroids[0][0], 1.0 / 3.0);
}

#[test]
fn val_dcbc_013_zero_norm_centroids_use_smallest_member_for_normalized_distance() {
    let input = fixture(vec![vec![1.0, 0.0], vec![-1.0, 0.0]], 1, 1, 2, 1);

    let result = run_dcbc(&input).unwrap();
    assert_eq!(result.centroids[0], vec![0.0, 0.0]);
    assert_eq!(result.metadata.objective_value, 2.0);
}

#[test]
fn val_dcbc_014_exactly_t_iterations_execute_without_early_stopping() {
    let input = fixture(vec![vec![1.0], vec![2.0], vec![3.0]], 1, 1, 3, 3);

    let result = run_dcbc(&input).unwrap();
    assert_eq!(result.metadata.iteration_count, 3);
}

#[test]
fn val_dcbc_015_metadata_matches_the_final_clustering_state() {
    let input = fixture(
        vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![-1.0, 0.0]],
        2,
        1,
        2,
        2,
    );

    let result = run_dcbc(&input).unwrap();
    assert_eq!(result.metadata.iteration_count, 2);
    assert_eq!(
        result.metadata.cluster_sizes.iter().sum::<usize>(),
        input.x.len()
    );
    assert!(result.metadata.objective_value.is_finite());
}

#[test]
fn val_dcbc_016_objective_matches_a_protocol_recomputation() {
    let input = fixture(
        vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![-1.0, 0.0]],
        2,
        1,
        2,
        2,
    );

    let result = run_dcbc(&input).unwrap();
    let recomputed = recompute_objective(&input, &result);
    assert!((result.metadata.objective_value - recomputed).abs() < EPSILON);
}

#[test]
fn val_dcbc_017_public_surface_supports_default_and_explicit_backends() {
    let input = fixture(vec![vec![1.0], vec![2.0], vec![3.0]], 1, 1, 3, 1);

    let default_result = run_dcbc(&input).unwrap();
    let explicit_result = run_dcbc_with_backend(&input, &PureRustBackend).unwrap();
    assert_eq!(default_result, explicit_result);
}

#[test]
fn val_dcbc_018_repository_includes_verification_artifacts() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(manifest_dir.join("src/lib.rs").exists());
    assert!(manifest_dir.join("tests/spec_validation.rs").exists());
}

fn fixture(
    x: Vec<Vec<f64>>,
    cluster_count: usize,
    min_cluster_size: usize,
    max_cluster_size: usize,
    iteration_count: usize,
) -> DcbcInput {
    DcbcInput {
        x,
        cluster_count,
        min_cluster_size,
        max_cluster_size,
        iteration_count,
    }
}

fn recompute_objective(input: &DcbcInput, result: &DcbcRunResult) -> f64 {
    let normalized_points: Vec<Vec<f64>> = input.x.iter().map(|point| normalize(point)).collect();
    let memberships = memberships(result.assignment.as_slice(), result.centroids.len());
    let normalized_centroids: Vec<Vec<f64>> = result
        .centroids
        .iter()
        .enumerate()
        .map(|(cluster_index, centroid)| {
            let norm = norm(centroid);
            if norm < EPSILON {
                normalized_points[memberships[cluster_index][0]].clone()
            } else {
                centroid.iter().map(|value| value / norm).collect()
            }
        })
        .collect();

    result
        .assignment
        .iter()
        .enumerate()
        .map(|(point_index, &cluster_index)| {
            1.0 - dot(
                &normalized_points[point_index],
                &normalized_centroids[cluster_index],
            )
        })
        .sum()
}

fn memberships(assignment: &[usize], cluster_count: usize) -> Vec<Vec<usize>> {
    let mut memberships = vec![Vec::new(); cluster_count];
    for (point_index, &cluster_index) in assignment.iter().enumerate() {
        memberships[cluster_index].push(point_index);
    }
    memberships
}

fn normalize(vector: &[f64]) -> Vec<f64> {
    let vector_norm = norm(vector);
    vector.iter().map(|value| value / vector_norm).collect()
}

fn norm(vector: &[f64]) -> f64 {
    vector.iter().map(|value| value * value).sum::<f64>().sqrt()
}

fn dot(left: &[f64], right: &[f64]) -> f64 {
    left.iter().zip(right).map(|(lhs, rhs)| lhs * rhs).sum()
}
