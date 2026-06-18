<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->

# Rust Linear Algebra Acceleration Crate Requirements

## Status

Draft specification for a Rust crate that provides a shared CPU/WGPU
acceleration boundary for LexonGraph algorithms and evaluators.

## Scope

This document specifies the crate-level requirements for a Rust crate that:

- provides a repository-owned shared acceleration boundary rather than
  algorithm-specific ad hoc GPU code
- exposes optional CPU and WGPU execution for dense linear-algebra and
  embedding-oriented kernels used by current or future repository crates
- preserves correct CPU fallback and auditable backend attribution
- supports chunked or tiled execution for workloads that should not require
  whole-matrix materialization in device memory

This document does not define the full behavior of any consuming algorithm,
candidate benchmark policy, or evaluator-owned ranking rule.

## Terminology

In this spec package, `acceleration backend` means the concrete execution mode
selected for one kernel invocation or higher-level operation, such as CPU or
WGPU.

`Chunked execution` means an execution strategy that partitions a larger logical
matrix or reduction workload into bounded tiles or slices so the observable
operation need not allocate the whole logical result in device memory at once.

`Capability result` means the explicit observed status of backend selection,
such as supported, declined, unsupported, or probe-failed.

## Requirements

### REQ-ACCEL-001

The repository shall define a dedicated Rust crate at
`crates/lexongraph-linear-algebra-acceleration`.

### REQ-ACCEL-002

The crate shall remain subordinate to consuming repository specifications such
as:

- `docs/specs/rust-spherical-kmeans-crate/`
- `docs/specs/rust-streaming-clustering-evaluator-crate/`

Those consuming specifications remain authoritative for algorithm or evaluator
semantics, while this specification is authoritative only for the shared
acceleration boundary it defines.

### REQ-ACCEL-003

The crate shall expose a shared execution-backend surface with at least:

- automatic selection
- explicit CPU selection
- explicit WGPU selection
- persistent process-wide set/get/reset control for the selected request until
  changed

### REQ-ACCEL-004

The crate shall preserve a correct CPU implementation for every accelerated
kernel it exposes. Unsupported, declined, or probe-failed WGPU execution shall
not silently claim acceleration.

### REQ-ACCEL-005

The crate shall expose explicit backend-resolution or capability-reporting
metadata sufficient for consuming artifacts to distinguish at minimum:

- CPU execution
- WGPU execution
- WGPU available but declined
- WGPU unsupported fallback
- WGPU probe failure fallback

### REQ-ACCEL-006

The crate shall support dense linear-algebra or embedding-oriented kernels that
can be reused by multiple consumers without embedding evaluator-owned or
algorithm-owned policy in the acceleration boundary.

### REQ-ACCEL-007

For kernels whose full logical workload may exceed practical device-memory or
transfer limits, the crate shall support chunked or tiled execution rather than
requiring whole-matrix materialization.

### REQ-ACCEL-008

Backend selection and chunking strategy shall not change the documented
observable semantics of a consuming algorithm beyond explicitly documented
floating-point tolerance.

### REQ-ACCEL-009

The crate shall support executable verification of cross-backend numerical
parity and backend-attribution behavior.

### REQ-ACCEL-010

The crate shall not, by itself, claim that GPU offload is worthwhile. Any claim
of net acceleration remains the responsibility of the consuming specification
and its benchmark validation surface.

### REQ-ACCEL-011

The crate shall allow callers to persistently pin the process-wide backend
request to CPU, WGPU, or automatic selection until changed, while preserving
compatible scoped-override behavior for temporary call-site control.
