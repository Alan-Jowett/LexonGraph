// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![cfg(feature = "conformance")]

mod support;

use lexongraph_streaming_clustering::conformance::run_streaming_clustering_suite;
use support::FixtureHarness;

#[test]
fn downstream_crates_can_run_the_streaming_clustering_suite() {
    run_streaming_clustering_suite(&FixtureHarness).unwrap();
}
