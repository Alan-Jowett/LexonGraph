// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![cfg(feature = "inject")]

use lexongraph_block_store::conformance::{run_contract_suite, run_full_suite};

mod support;

use support::MemoryHarness;

#[test]
fn downstream_crates_can_run_the_contract_suite() {
    run_contract_suite(&MemoryHarness).unwrap();
}

#[test]
fn downstream_crates_can_run_the_full_suite() {
    run_full_suite(&MemoryHarness).unwrap();
}
