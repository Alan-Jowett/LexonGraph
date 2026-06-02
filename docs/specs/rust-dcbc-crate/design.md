# Rust DCBC Crate Design

## Status

Draft design specification for a Rust crate that implements the deterministic
capacity-constrained balanced clustering protocol.

## Design Goals

The crate design is intended to be:

- protocol-conforming
- deterministic at the crate boundary
- explicit about result-affecting numeric ordering
- strict about capacity constraints and failure modes
- minimal at the public API boundary
- independent of any required tensor or accelerator backend

## Crate Boundary

The crate owns:

- typed clustering input and output types
- protocol-conforming input validation
- deterministic centroid initialization
- capacity-constrained assignment orchestration
- centroid update and objective computation
- DCBC-oriented error taxonomy

The crate does not own:

- host storage or transport concerns
- downstream indexing or search semantics
- approximate or stochastic clustering behavior
- any required numeric backend, tensor library, or accelerator runtime

## Protocol Conformance Boundary

All normative protocol behavior is defined by `docs/protocol/dcbc.md`.

This crate implements those rules; it does not redefine them.

## Core Types

### DSG-DCBC-001 `Protocol dependency boundary`

The crate depends on `docs/protocol/dcbc.md` for authoritative input validity,
distance semantics, deterministic tie-breaking, iteration behavior, output
shape, and failure conditions.

### DSG-DCBC-002 `DcbcInput`

A typed input structure containing:

- ordered vectors `X`
- cluster count `k`
- minimum cluster size `L`
- maximum cluster size `N`
- iteration count `T`

### DSG-DCBC-003 `DcbcRunResult`

A successful result containing:

- assignment vector `A`
- raw centroid array `C`
- metadata for the completed run

### DSG-DCBC-004 `DcbcMetadata`

A typed metadata structure containing:

- `iteration_count`
- `cluster_sizes`
- `objective_value`

### DSG-DCBC-005 `DcbcError`

An explicit error taxonomy covering at least:

- mixed dimensionality
- non-finite numeric values
- zero-norm vectors
- invalid integer bounds
- infeasible capacity constraints
- assignment infeasibility
- invalid numeric state required for protocol output

## API Surface

### DSG-DCBC-006 `run_dcbc(input) -> Result<DcbcRunResult, DcbcError>`

The public API exposes one deterministic clustering operation that accepts a
`DcbcInput` and returns either a typed protocol-conforming result or an
explicit failure.

## Execution Flow

### DSG-DCBC-007 `Validation and preprocessing`

Before initialization, the crate validates all protocol preconditions,
including dimensional consistency, numeric finiteness, non-zero input norms,
integer bounds, and feasible capacity constraints.

### DSG-DCBC-008 `Numeric semantics`

All result-affecting numeric values use `f64`. Ordered comparisons and
result-affecting reductions follow the protocol's deterministic semantics,
including `epsilon = 1e-12`.

### DSG-DCBC-009 `Initialization realization`

Initialization is realized exactly as follows:

1. set the first centroid to `X[0]`
2. for each later centroid, evaluate each candidate point by its minimum
   distance to already selected centroids
3. choose the candidate maximizing that minimum distance
4. break ties by smaller point index

### DSG-DCBC-010 `Assignment engine`

The assignment phase realizes the protocol's logical minimum-cost-flow
semantics over source, point, cluster, and sink nodes with:

- unit source-to-point capacity
- unit point-to-cluster capacity
- cluster lower bound `L`
- cluster upper bound `N`
- point-to-cluster cost equal to protocol cosine distance

Point-to-cluster edges are generated in ascending point index, then ascending
cluster index.

### DSG-DCBC-011 `Deterministic optimum selection`

If multiple optimal assignments exist, the assignment realization preserves the
protocol-required lexicographically minimal assignment vector in ascending
point-index order.

### DSG-DCBC-012 `Cluster materialization`

After assignment, the crate materializes `S[j] = { i | A[i] = j }` and
validates:

- each point is assigned exactly once
- each cluster size satisfies `L <= |S[j]| <= N`

### DSG-DCBC-013 `Centroid update`

For each cluster, the crate computes the raw centroid by averaging member
vectors with summation in ascending point-index order.

### DSG-DCBC-014 `Zero-norm centroid handling`

If a raw centroid norm is smaller than `epsilon`, the normalized centroid used
for distance computations is derived from the smallest-index member of the same
materialized membership set, while the raw stored centroid remains unchanged.

### DSG-DCBC-015 `Iteration controller`

The crate executes exactly `T` iterations, each consisting of assignment
followed by centroid update. It does not terminate early based on convergence
or objective stability.

### DSG-DCBC-016 `Objective computation`

After the final iteration, the crate computes the reported objective value from
the final assignment and the normalized centroids required by the protocol.

### DSG-DCBC-017 `Determinism boundary`

Physical parallelism is conforming only if it preserves the same externally
visible result as the protocol's single-threaded logical model.

### DSG-DCBC-018 `Implementation realization`

This specification package shall be realized as a concrete Rust crate in the
repository, and that implementation shall expose the public API and behavioral
surface defined by this document.

### DSG-DCBC-019 `Verification realization`

The repository shall include automated tests that realize the validation
entries in `docs/specs/rust-dcbc-crate/validation.md`, with each validation
entry mapped to one or more executable tests.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-DCBC-001 | REQ-DCBC-001, REQ-DCBC-002 |
| DSG-DCBC-002..005 | REQ-DCBC-001, REQ-DCBC-003, REQ-DCBC-005, REQ-DCBC-012 |
| DSG-DCBC-006 | REQ-DCBC-001, REQ-DCBC-003, REQ-DCBC-012 |
| DSG-DCBC-007 | REQ-DCBC-005 |
| DSG-DCBC-008 | REQ-DCBC-006 |
| DSG-DCBC-009 | REQ-DCBC-007 |
| DSG-DCBC-010..012 | REQ-DCBC-009, REQ-DCBC-010 |
| DSG-DCBC-013..014 | REQ-DCBC-011 |
| DSG-DCBC-015 | REQ-DCBC-008 |
| DSG-DCBC-016 | REQ-DCBC-012, REQ-DCBC-013 |
| DSG-DCBC-017 | REQ-DCBC-006, REQ-DCBC-014 |
| DSG-DCBC-018..019 | REQ-DCBC-015 |
