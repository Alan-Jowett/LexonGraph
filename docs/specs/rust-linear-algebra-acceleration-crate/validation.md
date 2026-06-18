<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->

# Rust Linear Algebra Acceleration Crate Validation

## Status

Draft validation specification for a Rust crate that provides a shared CPU/WGPU
acceleration boundary for LexonGraph kernels.

## Validation Scope

These validation entries define the conformance surface for the shared
acceleration boundary. They cover artifact presence, backend selection,
fallback, numerical parity, and chunked execution behavior.

## Validation Entries

### VAL-ACCEL-001

Inspect the repository artifacts for the crate.

**Pass condition:** the repository includes a crate at
`crates/lexongraph-linear-algebra-acceleration` and this spec package.

**Traces to:** REQ-ACCEL-001

### VAL-ACCEL-002

Inspect the crate's public surface and specification references.

**Pass condition:** the crate remains subordinate to consuming repository
specifications, exposes a shared acceleration boundary, and does not claim
algorithm semantics of its own.

**Traces to:** REQ-ACCEL-002, REQ-ACCEL-006

### VAL-ACCEL-003

Exercise backend request parsing and backend resolution on supported and
unsupported configurations.

**Pass condition:** the crate reports explicit CPU, WGPU, declined, unsupported,
or probe-failed outcomes rather than silently claiming acceleration.

**Traces to:** REQ-ACCEL-003, REQ-ACCEL-004, REQ-ACCEL-005

### VAL-ACCEL-004

Run at least one shared dense or embedding-oriented kernel on CPU and on WGPU
when supported.

**Pass condition:** the observable numerical outputs match within the documented
floating-point tolerance across backends.

**Traces to:** REQ-ACCEL-006, REQ-ACCEL-008, REQ-ACCEL-009

### VAL-ACCEL-005

Exercise one workload whose logical dense shape is large enough that a
whole-matrix device allocation would be an unreasonable requirement.

**Pass condition:** the crate exposes chunked or tiled execution that preserves
the documented result semantics without requiring whole logical output
materialization in device memory.

**Traces to:** REQ-ACCEL-007, REQ-ACCEL-008

### VAL-ACCEL-006

Inspect one consuming artifact surface that records acceleration provenance.

**Pass condition:** the consuming surface can record which backend actually ran
and whether fallback occurred.

**Traces to:** REQ-ACCEL-005, REQ-ACCEL-009

### VAL-ACCEL-007

Inspect the shared acceleration spec against one consuming algorithm
specification that claims a net speedup.

**Pass condition:** the shared acceleration crate does not itself claim that
WGPU is worthwhile; net-benefit proof remains owned by the consuming
specification and benchmark surface.

**Traces to:** REQ-ACCEL-010

### VAL-ACCEL-008

Exercise persistent process-wide backend pinning together with the scoped
override helper.

**Pass condition:** callers can set, read, and reset the persistent request, and
any scoped override restores the previous persistent request even on unwind.

**Traces to:** REQ-ACCEL-003, REQ-ACCEL-005, REQ-ACCEL-011
