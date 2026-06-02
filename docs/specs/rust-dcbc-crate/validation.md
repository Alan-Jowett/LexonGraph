# Rust DCBC Crate Validation

## Status

Draft validation specification for a Rust crate that implements the
deterministic capacity-constrained balanced clustering protocol.

## Validation Scope

These validation entries define the expected conformance surface for a crate
that implements the requirements and design in this spec package.

Protocol-level DCBC semantics remain normatively defined by
`docs/protocol/dcbc.md`.

## Validation Entries

### VAL-DCBC-001

Invoke the crate with infeasible capacity bounds where `k * L > n` or
`n > k * N`.

**Pass condition:** fails explicitly.

**Traces to:** REQ-DCBC-005

### VAL-DCBC-002

Invoke the crate with invalid scalar bounds such as `k < 1`, `L < 1`,
`L > N`, or `T < 1`.

**Pass condition:** fails explicitly.

**Traces to:** REQ-DCBC-005

### VAL-DCBC-003

Supply mixed-dimensional vectors.

**Pass condition:** fails explicitly.

**Traces to:** REQ-DCBC-005

### VAL-DCBC-004

Supply input vectors containing `NaN` or infinite values.

**Pass condition:** fails explicitly.

**Traces to:** REQ-DCBC-005

### VAL-DCBC-005

Supply a zero-norm input vector.

**Pass condition:** fails explicitly.

**Traces to:** REQ-DCBC-005

### VAL-DCBC-006

Run the crate twice with identical ordered inputs and parameters.

**Pass condition:** both runs produce identical outputs or the same explicit
failure.

**Traces to:** REQ-DCBC-014

### VAL-DCBC-007

Run the crate on the same logical vector set in different input orders.

**Pass condition:** the crate preserves input-order semantics and does not
claim permutation equivalence.

**Traces to:** REQ-DCBC-004

### VAL-DCBC-008

Inspect initialization on a fixture with unique farthest candidates.

**Pass condition:** the first centroid is `X[0]`, and later centroids follow
the protocol's farthest-point selection rule.

**Traces to:** REQ-DCBC-007

### VAL-DCBC-009

Inspect initialization on a fixture with tied farthest candidates.

**Pass condition:** the smaller point index wins the tie.

**Traces to:** REQ-DCBC-007

### VAL-DCBC-010

Run assignment on a feasible fixture.

**Pass condition:** each point is assigned exactly once and every cluster size
remains within `[L, N]`.

**Traces to:** REQ-DCBC-009, REQ-DCBC-010

### VAL-DCBC-011

Use a fixture with multiple optimal assignments.

**Pass condition:** the returned assignment vector is the lexicographically
minimal optimum.

**Traces to:** REQ-DCBC-009

### VAL-DCBC-012

Use a numerically sensitive centroid fixture.

**Pass condition:** ordered summation follows ascending point index and remains
deterministic.

**Traces to:** REQ-DCBC-006, REQ-DCBC-011

### VAL-DCBC-013

Use a cluster whose raw centroid norm falls below `epsilon`.

**Pass condition:** the normalized distance centroid is derived from the
smallest-index member of the same materialized membership set, and the raw
stored centroid remains unchanged.

**Traces to:** REQ-DCBC-011

### VAL-DCBC-014

Run with `T > 1` on a fixture that would otherwise converge early.

**Pass condition:** exactly `T` iterations execute and no early stopping
occurs.

**Traces to:** REQ-DCBC-008

### VAL-DCBC-015

Inspect final metadata.

**Pass condition:** `iteration_count`, `cluster_sizes`, and `objective_value`
are present and consistent with the final clustering state.

**Traces to:** REQ-DCBC-012

### VAL-DCBC-016

Recompute the objective from the final output using protocol semantics.

**Pass condition:** the reported `objective_value` matches the recomputed
final-state value in full double precision.

**Traces to:** REQ-DCBC-013

### VAL-DCBC-017

Inspect the crate's public surface.

**Pass condition:** the crate exposes the Rust clustering API boundary and
explicit failure surface without redefining protocol semantics or requiring a
specific numeric backend.

**Traces to:** REQ-DCBC-001, REQ-DCBC-002

### VAL-DCBC-018

Inspect repository verification artifacts for the crate.

**Pass condition:** executable automated tests realize the validation surface
defined in this specification package.

**Traces to:** REQ-DCBC-015
