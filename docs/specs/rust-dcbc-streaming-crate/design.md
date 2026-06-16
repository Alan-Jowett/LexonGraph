<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming DCBC Crate Design

## Status

Draft design specification for a Rust crate that realizes deterministic DCBC
through the shared LexonGraph streaming clustering contract.

## Design Goals

The crate design is intended to be:

- protocol-conforming
- contract-conforming
- deterministic at the observable boundary
- explicit about pass lifecycle and failure behavior
- exact in its realization of DCBC mechanics
- independent of dataset-size-coupled public API shapes

## Crate Boundary

The crate owns:

- a concrete streaming DCBC trainer implementation
- a concrete streaming DCBC classifier implementation
- mapping from shared balance constraints to DCBC occupancy bounds
- pass-scoped realization of DCBC initialization, assignment, and centroid
  update mechanics
- internal state and storage required to preserve exact protocol behavior across
  caller-driven passes

The crate does not own:

- the authoritative DCBC protocol definition
- the shared streaming trait definitions
- the existing batch DCBC crate's API boundary
- a repository-wide canonical classifier serialization format

## Design Entries

### DSG-DCBC-STREAM-001 `Composite normative boundary`

The crate depends on `docs/protocol/dcbc.md` for DCBC mechanics and on
`docs/specs/rust-streaming-clustering-crate` for the shared trainer/classifier
contract. The crate does not redefine either source.

### DSG-DCBC-STREAM-002 `Concrete trainer/classifier realization`

The crate exposes one trainer type implementing `StreamingClusterTrainer` and
one classifier type implementing `StreamingClusterClassifier`.

### DSG-DCBC-STREAM-003 `Shared configuration mapping`

The trainer is constructed from `StreamingClusteringConfig`, validating:

- `K`
- dimensions
- supported balance-constraint combinations
- deterministic seed behavior

The crate derives protocol occupancy bounds from the shared balance-constraint
surface without extending the public shared contract.

Occupancy-based constraints that map directly to the protocol's lower and upper
cluster-size bounds are accepted; unsupported non-occupancy balance controls are
rejected through the invalid-configuration category.

### DSG-DCBC-STREAM-004 `Observable lifecycle`

The trainer follows the shared lifecycle:

`Idle -> Ingesting -> PassComplete -> Ingesting/TrainingComplete -> Classifier`

Illegal transitions enter the shared terminal error state via the invalid
transition category.

That includes at least rejecting `complete_training()` before `PassComplete`.
Because `into_classifier()` consumes the trainer, it must still reject calls
before `TrainingComplete` deterministically through the shared invalid-transition
surface.

### DSG-DCBC-STREAM-005 `Pass ingestion boundary`

`ingest_batch()` validates embedding shape and finiteness through the shared
malformed-input surface and appends embeddings to the current pass dataset order
without exposing any dataset-sized public API artifact.

`ingest_batch()` also rejects zero-norm embeddings so that pass-local material
always remains valid for the protocol's normalization and distance semantics.

During an in-progress pass, the trainer maintains only pass-local working state
needed to complete the next `finish_pass()`, including:

- the ordered embeddings for the current pass in memory or in an equivalent
  spill-backed representation
- the current pass observed count
- enough deterministic comparison state to validate later passes against the
  run's established dataset baseline
- scratch space needed to materialize assignments, memberships, and centroid
  updates for that pass

### DSG-DCBC-STREAM-006 `First-pass feasibility gate`

The crate defers dataset-size-dependent checks until the first completed pass.
At `finish_pass()` for pass 0, the trainer computes `Observed N` and rejects
configurations that cannot satisfy:

- `Observed N >= K`
- any deterministically derived DCBC occupancy bounds

This gate covers both trivial underfull first passes and occupancy-derived
infeasibility where `Observed N >= K` but the derived DCBC bounds still cannot
be satisfied.

### DSG-DCBC-STREAM-007 `Cross-pass dataset continuity`

The first completed pass establishes the logical dataset for the run. Each later
pass is validated against that baseline for:

- identical observed count
- identical ordered embedding content

Deviation fails explicitly before claiming protocol-conformant continuation of
the run.

State preserved between completed passes is limited to the state required to
continue the DCBC run deterministically, including:

- validated shared configuration and deterministic seed path
- trainer lifecycle state and completed-pass count
- the baseline dataset continuity record established by the first completed pass
- the latest raw centroids and normalized centroids
- the stable externally visible cluster-ID mapping
- any spill-backed or temporary storage handles needed, if the implementation
  chooses to use them, to preserve exact mechanics without requiring
  dataset-sized public API state

### DSG-DCBC-STREAM-008 `Iteration-to-pass mapping`

Each successful `finish_pass()` realizes exactly one DCBC iteration over the
embeddings observed in that pass:

1. choose the normalized centroids for that iteration
2. solve the constrained assignment problem over the pass dataset order
3. materialize memberships
4. recompute centroids in ascending point-index order
5. compute pass metrics and expose the pass report

The caller-visible streaming passes are the protocol-visible repeated DCBC
passes for the run. No hidden extra iteration occurs before or after the
caller-visible pass completion, and no separate public iteration-count parameter
duplicates that control surface.

### DSG-DCBC-STREAM-009 `Initialization realization`

For the first completed pass, centroid initialization uses the protocol-defined
deterministic farthest-point procedure rooted at the first embedding in pass
dataset order.

### DSG-DCBC-STREAM-010 `Assignment realization`

Assignment uses the protocol's constrained minimum-cost semantics with
deterministic generation order over point-cluster edges in ascending point index
then ascending cluster index, preserving lexicographically minimal optimal
assignment selection.

### DSG-DCBC-STREAM-011 `Centroid update realization`

Centroid updates compute raw centroids using ascending point-index summation
order and use the protocol-defined smallest-index-member fallback when a raw
centroid norm falls below `epsilon`, while preserving the raw stored centroid.

### DSG-DCBC-STREAM-012 `Stable cluster identity`

Externally visible cluster IDs remain stable across completed passes. If the
implementation needs to reorder internal cluster state between passes, it applies
deterministic matching with deterministic tie-breaking before exposing pass
reports or classifier assignments.

### DSG-DCBC-STREAM-013 `Pass-report metrics`

Each completed pass yields a `PassReport` whose:

- `observed_count` equals the number of embeddings ingested in that pass
- `quality_metric` is deterministic and comparable across passes within one run
- `balance_metric` is deterministic and comparable across passes within one run
- metric directions remain fixed for the full run
- `cluster_ids` match the stable externally visible cluster identifiers

### DSG-DCBC-STREAM-014 `Classifier realization`

After `complete_training()`, `into_classifier()` consumes the trainer and yields
a classifier that uses the final stable centroids and cluster-ID mapping to
assign valid embeddings deterministically into `[0, K)`.

The classifier reuses the shared malformed-input surface, including rejection of
zero-norm embeddings before assignment.

### DSG-DCBC-STREAM-015 `Dataset-size-independent public surface`

The public API does not require callers to provide or retain the entire dataset
at once. Any storage needed to preserve exact DCBC mechanics across passes is
kept behind the crate boundary. That may be entirely in-memory for bounded
workloads or may use implementation-internal spill or temporary externalization
if required.

### DSG-DCBC-STREAM-016 `Error mapping`

The crate maps failures into the shared error categories:

- invalid configuration
- invalid transition
- unsatisfiable constraint
- malformed input

Protocol-specific diagnostics may be retained internally or in messages, but the
observable category surface stays aligned with the shared contract.

### DSG-DCBC-STREAM-017 `Serialization boundary`

If classifier serialization is exposed by the crate, it is deterministic for
identical final state but remains implementation-defined and non-canonical
across implementations.

### DSG-DCBC-STREAM-018 `Verification realization`

The repository includes automated tests that exercise both:

- DCBC-specific mechanics at the new crate's conformant boundary
- the shared streaming clustering conformance helpers

### DSG-DCBC-STREAM-019 `Optional WGPU dense-kernel backend`

The DCBC crate may layer an optional WGPU backend beneath its existing trainer
implementation for dense internal kernels such as distance-matrix
materialization and assignment-support computation. Backend selection is
internal, capability-gated, and must preserve the CPU-defined observable pass
results, stable cluster IDs, and classifier semantics.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-DCBC-STREAM-001 | REQ-DCBC-STREAM-002 |
| DSG-DCBC-STREAM-002 | REQ-DCBC-STREAM-001, REQ-DCBC-STREAM-003 |
| DSG-DCBC-STREAM-003 | REQ-DCBC-STREAM-004, REQ-DCBC-STREAM-009 |
| DSG-DCBC-STREAM-004..005 | REQ-DCBC-STREAM-003, REQ-DCBC-STREAM-015 |
| DSG-DCBC-STREAM-006 | REQ-DCBC-STREAM-008, REQ-DCBC-STREAM-015 |
| DSG-DCBC-STREAM-007 | REQ-DCBC-STREAM-007 |
| DSG-DCBC-STREAM-008..011 | REQ-DCBC-STREAM-005, REQ-DCBC-STREAM-006, REQ-DCBC-STREAM-010 |
| DSG-DCBC-STREAM-012 | REQ-DCBC-STREAM-011 |
| DSG-DCBC-STREAM-013 | REQ-DCBC-STREAM-012 |
| DSG-DCBC-STREAM-014 | REQ-DCBC-STREAM-013 |
| DSG-DCBC-STREAM-015 | REQ-DCBC-STREAM-014 |
| DSG-DCBC-STREAM-016 | REQ-DCBC-STREAM-015 |
| DSG-DCBC-STREAM-017 | REQ-DCBC-STREAM-016 |
| DSG-DCBC-STREAM-018 | REQ-DCBC-STREAM-017 |
| DSG-DCBC-STREAM-019 | REQ-DCBC-STREAM-018 |
