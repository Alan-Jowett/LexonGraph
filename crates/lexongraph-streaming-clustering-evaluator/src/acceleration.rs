// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

pub use lexongraph_linear_algebra_acceleration::{
    ExecutionBackendRequest, ExecutionBackendResolution, ExecutionBackendSelection,
    execution_backend_request, with_execution_backend_request,
};

pub(crate) use lexongraph_linear_algebra_acceleration::{
    DenseDistanceMetric, backend_resolution_label, dense_distance_matrix,
    detected_execution_backend_selection,
};

#[cfg(test)]
pub(crate) fn fixture_cpu_execution_backend_selection() -> ExecutionBackendSelection {
    ExecutionBackendSelection {
        request: ExecutionBackendRequest::Cpu,
        resolution: ExecutionBackendResolution::Cpu,
        detail: "fixture execution pinned to the cpu backend".into(),
    }
}

#[cfg(test)]
pub(crate) fn with_execution_backend_request_for_test<T>(
    request: ExecutionBackendRequest,
    run: impl FnOnce() -> T,
) -> T {
    with_execution_backend_request(request, run)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_distance_matrix_cpu_matches_expected_euclidean_values() {
        let distances =
            with_execution_backend_request_for_test(ExecutionBackendRequest::Cpu, || {
                dense_distance_matrix(
                    &[&[0.0, 0.0], &[3.0, 4.0]],
                    &[&[0.0, 0.0], &[6.0, 8.0]],
                    DenseDistanceMetric::Euclidean,
                )
                .unwrap()
            });
        assert_eq!(distances, vec![0.0, 10.0, 5.0, 5.0]);
    }

    #[cfg(feature = "wgpu-accel")]
    #[test]
    fn dense_distance_matrix_wgpu_matches_cpu_when_supported() {
        let selection =
            with_execution_backend_request_for_test(ExecutionBackendRequest::Wgpu, || {
                detected_execution_backend_selection()
            });
        if selection.resolution != ExecutionBackendResolution::Wgpu {
            return;
        }

        let expected =
            with_execution_backend_request_for_test(ExecutionBackendRequest::Cpu, || {
                dense_distance_matrix(
                    &[&[1.0, 0.0], &[0.0, 1.0], &[1.0, 1.0]],
                    &[&[1.0, 0.0], &[0.0, 1.0]],
                    DenseDistanceMetric::Cosine,
                )
                .unwrap()
            });
        let actual = with_execution_backend_request_for_test(ExecutionBackendRequest::Wgpu, || {
            dense_distance_matrix(
                &[&[1.0, 0.0], &[0.0, 1.0], &[1.0, 1.0]],
                &[&[1.0, 0.0], &[0.0, 1.0]],
                DenseDistanceMetric::Cosine,
            )
            .unwrap()
        });

        for (left, right) in expected.iter().zip(actual.iter()) {
            assert!((left - right).abs() < 1e-4);
        }
    }
}
