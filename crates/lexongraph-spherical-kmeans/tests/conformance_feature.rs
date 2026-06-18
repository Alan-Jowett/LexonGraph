// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

mod support;

use lexongraph_streaming_clustering::conformance::run_streaming_clustering_suite;
use support::Harness;

#[test]
fn streaming_spherical_kmeans_passes_the_shared_conformance_suite() {
    run_streaming_clustering_suite(&Harness).unwrap();
}
