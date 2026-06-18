<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->

# Rust Linear Algebra Acceleration Crate Design

## Status

Draft design specification for a Rust crate that provides a shared CPU/WGPU
acceleration boundary for LexonGraph kernels.

## Design Goals

The crate design is intended to be:

- reusable across multiple repository crates
- explicit about backend selection and fallback
- careful not to smuggle algorithm policy into the acceleration layer
- capable of bounded-memory tiled execution for large dense workloads
- safe to adopt incrementally by consumers that still require CPU fallback

## Crate Boundary

The crate owns:

- shared backend request and resolution types
- shared capability probing and fallback behavior
- reusable dense or embedding-oriented kernel surfaces
- reusable chunked or tiled execution helpers

The crate does not own:

- spherical-kmeans semantics
- evaluator benchmark policy
- candidate-ranking logic
- algorithm-specific convergence, tie-breaking, or repair policy

## Design Entries

### DSG-ACCEL-001 `Shared subordinate acceleration boundary`

The crate defines one repository-owned acceleration boundary that can be reused
by current and future algorithms or evaluators. Consuming specifications remain
authoritative for the semantics of any higher-level operation.

### DSG-ACCEL-002 `Explicit backend selection surface`

The crate exposes explicit backend request and resolution types that support at
least automatic selection, forced CPU execution, and forced WGPU execution.

### DSG-ACCEL-003 `Capability-gated fallback`

Every WGPU-backed kernel remains subordinate to an explicit capability result.
Unsupported, declined, or probe-failed WGPU execution falls back or reports
failure explicitly rather than silently claiming acceleration.

### DSG-ACCEL-004 `Policy-free kernel surfaces`

The crate exposes reusable dense or embedding-oriented kernel operations without
embedding evaluator-owned or algorithm-owned policy in the kernel boundary.

### DSG-ACCEL-005 `Chunked large-workload execution`

For large dense workloads, the crate supports tiled or chunked execution so a
consumer can process the logical workload without requiring whole-matrix device
allocation.

### DSG-ACCEL-006 `Backend-parity contract`

The crate preserves the documented observable numerical contract of the exposed
kernels across CPU and WGPU execution, admitting only explicitly documented
floating-point tolerance.

### DSG-ACCEL-007 `Consumer-visible backend attribution`

The crate returns enough backend-resolution metadata for consuming crates to
record whether work executed on CPU, WGPU, or CPU fallback after an unavailable
or failed probe.

### DSG-ACCEL-008 `Incremental adoption`

The crate is designed for partial adoption. A consumer may accelerate only the
hot kernels that produce a measured end-to-end win while continuing to execute
other steps on CPU.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-ACCEL-001 | REQ-ACCEL-001, REQ-ACCEL-002 |
| DSG-ACCEL-002 | REQ-ACCEL-003, REQ-ACCEL-005 |
| DSG-ACCEL-003 | REQ-ACCEL-004, REQ-ACCEL-005 |
| DSG-ACCEL-004 | REQ-ACCEL-006 |
| DSG-ACCEL-005 | REQ-ACCEL-007 |
| DSG-ACCEL-006 | REQ-ACCEL-008, REQ-ACCEL-009 |
| DSG-ACCEL-007 | REQ-ACCEL-005, REQ-ACCEL-009 |
| DSG-ACCEL-008 | REQ-ACCEL-004, REQ-ACCEL-010 |
