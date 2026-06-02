// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

#[cfg(feature = "tch")]
use lexongraph_dcbc::{DcbcInput, TorchBackend, run_dcbc, run_dcbc_with_backend};

#[cfg(feature = "tch")]
#[test]
fn torch_backend_matches_the_default_runner_on_a_simple_fixture() {
    let input = DcbcInput {
        x: vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![-1.0, 0.0]],
        cluster_count: 2,
        min_cluster_size: 1,
        max_cluster_size: 2,
        iteration_count: 2,
    };

    let torch_result = run_dcbc_with_backend(&input, &TorchBackend::default()).unwrap();
    let default_result = run_dcbc(&input).unwrap();
    assert_eq!(torch_result.assignment, default_result.assignment);
    assert_eq!(
        torch_result.metadata.cluster_sizes,
        default_result.metadata.cluster_sizes
    );
    assert!(
        (torch_result.metadata.objective_value - default_result.metadata.objective_value).abs()
            < 1e-12
    );
}
