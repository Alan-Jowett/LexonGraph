# Rust DCBC Crate Requirements

## Status

Draft specification for a Rust crate that implements the deterministic
capacity-constrained balanced clustering protocol.

## Scope

This document specifies the crate-level requirements for a Rust crate that
implements `docs/protocol/dcbc.md`.

This document does not redefine protocol math, deterministic tie-breaking,
output semantics, or failure conditions. Those remain normative in
`docs/protocol/dcbc.md`. This document defines what the crate must do in order
to conform to that protocol.

## Terminology

In this spec package, `DCBC run` means one complete invocation of the Rust API
boundary that performs validation, initialization, exactly `T` assignment and
centroid-update iterations, and final output materialization.

`Assignment vector`, `centroid array`, `cluster membership set`, and
`objective value` refer to the protocol-defined values in `docs/protocol/dcbc.md`.

## Requirements

### REQ-DCBC-001

The crate shall define the Rust API boundary for a deterministic
capacity-constrained balanced clustering component that implements
`docs/protocol/dcbc.md`.

### REQ-DCBC-002

The crate shall remain subordinate to `docs/protocol/dcbc.md` for input
validity, distance semantics, determinism rules, initialization, assignment
behavior, centroid updates, output semantics, invariants, and failure
conditions.

### REQ-DCBC-003

The public clustering operation shall require:

- an ordered input vector collection `X`
- a cluster count `k`
- a minimum cluster size `L`
- a maximum cluster size `N`
- an iteration count `T`

### REQ-DCBC-004

The crate shall preserve protocol-significant input order and shall not treat
permutation of `X` as semantically equivalent input.

### REQ-DCBC-005

The crate shall fail explicitly when inputs violate the protocol's validity
rules, including:

- mixed vector dimensionality
- non-finite numeric values
- zero-norm input vectors
- invalid integer bounds
- infeasible capacity constraints

### REQ-DCBC-006

The crate shall realize result-affecting numerical semantics with IEEE 754
double precision and deterministic operation ordering, including the
protocol-defined `epsilon = 1e-12` comparison behavior.

### REQ-DCBC-007

The crate shall implement deterministic centroid initialization exactly as
defined by `docs/protocol/dcbc.md`.

### REQ-DCBC-008

The crate shall perform exactly `T` iterations, each consisting of an
assignment phase followed by a centroid update phase, with no early stopping
or convergence-based termination.

### REQ-DCBC-009

The assignment phase shall realize the protocol's capacity-constrained
minimum-cost assignment semantics, including deterministic selection of the
lexicographically minimal assignment vector when multiple optimal solutions
exist.

### REQ-DCBC-010

After each assignment phase, the crate shall materialize cluster memberships
and validate that each point is assigned exactly once and each cluster size
remains within `[L, N]`.

### REQ-DCBC-011

The centroid update phase shall compute raw centroids using ascending
point-index summation order and shall apply the protocol's zero-norm centroid
fallback rule while preserving the raw stored centroid.

### REQ-DCBC-012

The crate shall produce:

- an assignment vector `A`
- a centroid array `C`
- metadata containing `iteration_count`, `cluster_sizes`, and `objective_value`

### REQ-DCBC-013

The reported objective value shall be computed after the final iteration using
the protocol's cosine-distance semantics and full double precision without
protocol-level rounding.

### REQ-DCBC-014

Given identical ordered inputs and parameters, the crate shall return
identical outputs or the same explicit failure.

### REQ-DCBC-015

The repository shall include a Rust crate and automated verification artifacts
that realize this specification package.

## Out of Scope

This crate does not define or own:

- host-application storage formats or transport APIs
- approximate clustering methods
- stochastic initialization
- relaxed capacity enforcement
- convergence-based early stopping
- any required numeric backend, tensor library, or accelerator runtime
- protocol evolution beyond implementing the current DCBC protocol revision

## Relationship to the Protocol

This document is subordinate to `docs/protocol/dcbc.md`.

If this document appears to conflict with the protocol document, the protocol
document is authoritative.
