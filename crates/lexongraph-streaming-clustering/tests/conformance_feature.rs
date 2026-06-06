// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![cfg(feature = "conformance")]

mod support;

use std::error::Error;

use lexongraph_streaming_clustering::conformance::run_streaming_clustering_suite;
use lexongraph_streaming_clustering::{StreamingClusteringError, conformance::ConformanceError};
use support::FixtureHarness;

#[test]
fn downstream_crates_can_run_the_streaming_clustering_suite() {
    run_streaming_clustering_suite(&FixtureHarness).unwrap();
}

#[test]
fn val_stream_trait_017_conformance_error_surface_distinguishes_suite_and_impl_failures() {
    let implementation = ConformanceError::from(StreamingClusteringError::MalformedInput {
        message: "fixture rejected malformed input".into(),
    });
    assert_eq!(
        implementation.to_string(),
        "malformed streaming clustering input: fixture rejected malformed input"
    );
    let implementation_source = implementation.source().unwrap();
    assert_eq!(
        implementation_source.to_string(),
        "malformed streaming clustering input: fixture rejected malformed input"
    );

    let expectation = ConformanceError::Expectation("fixture violated expectation".into());
    assert_eq!(
        expectation.to_string(),
        "conformance expectation failed: fixture violated expectation"
    );
    assert!(expectation.source().is_none());
}
