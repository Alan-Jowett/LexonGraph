<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Clustering Trait Crate Design

## Status

Draft design specification for a Rust crate that defines the shared LexonGraph
streaming multi-pass clustering contract.

## Design Goals

The crate design is intended to be:

- reusable across future clustering implementations
- explicit about lifecycle and error semantics
- deterministic at the observable API boundary
- minimal on the default production-facing surface
- algorithm-neutral

## Crate Boundary

The crate owns:

- shared trainer and classifier traits
- shared configuration, metric, and lifecycle types
- shared malformed-input and configuration validation helpers
- conformance helpers for downstream implementations

The crate does not own:

- a concrete clustering algorithm
- centroid update rules or optimization heuristics
- block transport, indexing orchestration, or search traversal
- standardized classifier serialization bytes

## Design Entries

### DSG-STREAM-TRAIT-001 `Shared crate boundary`

The crate boundary owns shared trainer/classifier traits, shared configuration
and metric types, lifecycle/state-machine types, and conformance helpers. It
does not own a concrete clustering algorithm.

### DSG-STREAM-TRAIT-002 `Trainer lifecycle surface`

The trainer trait exposes incremental ingestion over caller-supplied batches,
an explicit `finish_pass` transition, caller-directed completion of training,
and finalization into a classifier only after that completion step.

### DSG-STREAM-TRAIT-003 `Classifier surface`

The classifier trait exposes deterministic hard assignment for valid embeddings
and rejects malformed embeddings through the shared error surface.

The classifier also exposes its realized cluster count so callers can interpret
assignment IDs within `[0, R)`.

### DSG-STREAM-TRAIT-004 `Shared configuration types`

The crate defines shared configuration types that include `K`, input
dimensionality, optional balance-constraint configuration, and an optional
deterministic seed without fixing the downstream optimization method.

### DSG-STREAM-TRAIT-005 `Pass reporting`

The crate defines shared pass-report types carrying requested cluster count,
`quality_metric`, `balance_metric`, explicit metric-direction metadata, and a
small readiness enum equivalent to `AnalysisOnly | PartitionReady`.

For `AnalysisOnly` passes, `realized_cluster_count` and `cluster_ids` are
absent. For `PartitionReady` passes, both are present and become part of the
observable contract.

### DSG-STREAM-TRAIT-006 `Shared error categories`

The crate defines a shared error enum with category-level variants for invalid
configuration, invalid transition, unsatisfiable constraint, and malformed
input. Exact diagnostic wording is non-normative.

### DSG-STREAM-TRAIT-007 `Observable state machine`

The crate defines an explicit lifecycle model equivalent to
`Idle -> Ingesting -> PassComplete -> Ingesting/TrainingComplete`, followed by
consuming `into_classifier()` from `TrainingComplete`, with terminal failure on
illegal transitions.

### DSG-STREAM-TRAIT-008 `Cluster ID continuity`

Cluster identity continuity is a contract-level observable once a pass report is
partition-ready. Implementations may choose any internal matching strategy, but
the externally visible cluster IDs in partition-ready pass reports and in the
final classifier must remain stable across later partition-ready passes.

### DSG-STREAM-TRAIT-009 `Dataset-size-independent surface`

The public API, including feature-gated conformance-helper surfaces, avoids any
surface requiring dataset replay buffers, full assignment materialization, or
whole-dataset ownership inside shared contract types.

The shared contract also forbids implementation-owned indexing state or
scratch/storage whose required footprint scales with total dataset size `N`.
Allowed growth is limited to requested cluster count, embedding dimensionality,
caller-provided balance configuration, and documented bounded batch size.

### DSG-STREAM-TRAIT-010 `Feature-gated conformance helpers`

The crate exposes conformance helpers behind a non-default `conformance`
feature so downstream implementations can verify lifecycle, metric, rejection,
and classifier semantics from tests.

### DSG-STREAM-TRAIT-011 `Harness shape`

The conformance-helper surface provides reusable harness contracts for:

- a conforming trainer fixture
- a fixture that changes observable cluster IDs across passes
- caller-supplied streamed pass events and expected pass reports without
  requiring whole-run dataset materialization
- caller-supplied sample embeddings and expected assignments without requiring
  full-dataset assignment retention
- malformed-input fixtures for classifier rejection checks

### DSG-STREAM-TRAIT-012 `Deterministic seed policy`

Deterministic default behavior is modeled as either explicit use of a supplied
seed or a fixed implementation-defined deterministic default seed path.
Implicit nondeterministic seeding is disallowed at the contract boundary.

### DSG-STREAM-TRAIT-013 `Serialization boundary`

Deterministic classifier serialization is not standardized in this revision. If
an implementation exposes serialization in its own tests, determinism may be
validated there, but not as a cross-implementation contract requirement.

### DSG-STREAM-TRAIT-014 `Base configuration validity`

The shared validation helper rejects invalid base configuration before any
training activity begins. In this revision, zero cluster count and zero
embedding dimensionality are classified as `InvalidConfiguration`.

### DSG-STREAM-TRAIT-015 `Balance-constraint validity`

When balance constraints are supplied, the shared validation helper rejects
zero occupancies, contradictory occupancy bounds, non-finite or non-positive
maximum cluster-size ratios, and non-finite or negative soft-balance penalties
as `InvalidConfiguration`.

### DSG-STREAM-TRAIT-016 `Conformance error surface`

The feature-gated conformance module exposes a public
`ConformanceError::{Implementation, Expectation}` enum. `Display` forwards
implementation failures to the shared streaming-clustering error text,
expectation failures use suite-authored diagnostic strings, and `source()`
returns the underlying implementation error only for `Implementation`.

### DSG-STREAM-TRAIT-017 `Malformed-input-accepting fixture`

The conformance harness contract includes a dedicated trainer fixture whose
resulting classifier intentionally accepts wrong-dimensional and non-finite
embeddings as valid assignments. The suite executes this fixture so downstream
tests can prove the helper rejects malformed-input-accepting implementations.

### DSG-STREAM-TRAIT-018 `Backend-transparent shared contract`

The shared trait crate remains backend-transparent. Concrete implementations may
 internally choose CPU or optional accelerator-backed execution, but backend
 selection does not widen the shared API, does not introduce
 accelerator-shaped trait methods, and does not weaken the contract's
 observable lifecycle, assignment, or error semantics.

### DSG-STREAM-TRAIT-019 `Bounded indexing working set`

Conformant implementations bound implementation-owned indexing memory and
scratch/storage independently of total dataset size `N`.

The shared contract therefore excludes dataset-shaped replay buffers,
assignment logs, spill files, or similar indexing surfaces at the public
boundary, while still permitting implementation-owned state that scales with
requested cluster count, embedding dimensionality, caller-provided balance
configuration, and caller-selected batch size.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-STREAM-TRAIT-001 | REQ-STREAM-TRAIT-001, REQ-STREAM-TRAIT-002, REQ-STREAM-TRAIT-017 |
| DSG-STREAM-TRAIT-002 | REQ-STREAM-TRAIT-003, REQ-STREAM-TRAIT-005, REQ-STREAM-TRAIT-009, REQ-STREAM-TRAIT-012 |
| DSG-STREAM-TRAIT-003 | REQ-STREAM-TRAIT-007, REQ-STREAM-TRAIT-009, REQ-STREAM-TRAIT-010 |
| DSG-STREAM-TRAIT-004 | REQ-STREAM-TRAIT-003, REQ-STREAM-TRAIT-004, REQ-STREAM-TRAIT-013, REQ-STREAM-TRAIT-017 |
| DSG-STREAM-TRAIT-005 | REQ-STREAM-TRAIT-006 |
| DSG-STREAM-TRAIT-006 | REQ-STREAM-TRAIT-010 |
| DSG-STREAM-TRAIT-007 | REQ-STREAM-TRAIT-005, REQ-STREAM-TRAIT-012 |
| DSG-STREAM-TRAIT-008 | REQ-STREAM-TRAIT-008 |
| DSG-STREAM-TRAIT-009 | REQ-STREAM-TRAIT-011 |
| DSG-STREAM-TRAIT-010..011 | REQ-STREAM-TRAIT-014, REQ-STREAM-TRAIT-015, REQ-STREAM-TRAIT-016 |
| DSG-STREAM-TRAIT-012 | REQ-STREAM-TRAIT-013 |
| DSG-STREAM-TRAIT-013 | REQ-STREAM-TRAIT-018 |
| DSG-STREAM-TRAIT-014 | REQ-STREAM-TRAIT-010, REQ-STREAM-TRAIT-019 |
| DSG-STREAM-TRAIT-015 | REQ-STREAM-TRAIT-004, REQ-STREAM-TRAIT-010, REQ-STREAM-TRAIT-020 |
| DSG-STREAM-TRAIT-016 | REQ-STREAM-TRAIT-021 |
| DSG-STREAM-TRAIT-017 | REQ-STREAM-TRAIT-014, REQ-STREAM-TRAIT-022 |
| DSG-STREAM-TRAIT-018 | REQ-STREAM-TRAIT-023 |
| DSG-STREAM-TRAIT-019 | REQ-STREAM-TRAIT-024 |
