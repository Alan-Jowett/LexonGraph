<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust PCA Chunking Crate Design

## Status

Draft design specification for a Rust crate that realizes streaming PCA
projection + deterministic sort + exact chunking through the shared LexonGraph
streaming clustering contract.

## Design Goals

The crate design is intended to be:

- a simple reusable realization of research-plan candidate family **B**
- conformant to the shared streaming clustering contract
- deterministic at the observable API boundary
- explicit about exact chunk-formation behavior
- true streaming with respect to full logical dataset size
- minimal in its public surface

## Crate Boundary

The crate owns:

- a concrete streaming trainer implementation
- a concrete streaming classifier implementation
- replay-based PCA accumulation and deterministic boundary discovery
- deterministic exact chunk formation and chunk-boundary classification
- only bounded implementation-owned state needed for same-dataset multi-pass
  refinement

The crate does not own:

- the shared streaming trait definitions
- PCA eigendecomposition internals beyond invoking the PCA crate
- block-store integration or hierarchy construction
- alternate clustering algorithms

## Design Entries

### DSG-PCA-CHUNK-001 `Composite normative boundary`

The crate depends on `docs/research/clustering_plan.md` candidate family **B**
for its motivating algorithm family, on
`docs/specs/rust-streaming-clustering-crate/` for the shared trainer/classifier
contract, and on `docs/specs/rust-pca-crate/` for PCA behavior.

The crate does not redefine those sources.

### DSG-PCA-CHUNK-002 `Concrete trainer/classifier realization`

The crate exposes one trainer type implementing `StreamingClusterTrainer` and
one classifier type implementing `StreamingClusterClassifier`.

### DSG-PCA-CHUNK-003 `True streaming pass realization`

`ingest_batch()` validates embeddings through the shared malformed-input
surface.

During the PCA-analysis phase, the implementation streams embeddings into a
mergeable PCA accumulator and a pass fingerprint without retaining the full
logical dataset.

During boundary-discovery and ready replay phases, the implementation streams
the replayed pass while retaining only bounded state for the currently best
candidate classifier-visible sort key group and the already discovered boundary
keys.

### DSG-PCA-CHUNK-004 `Cross-pass continuity`

The first completed pass establishes the logical dataset for one training run.
Each later pass is validated against that baseline for identical observed count
and identical ordered embedding content before the trainer claims conformant
refinement of the same run.

### DSG-PCA-CHUNK-005 `Streaming PCA realization`

The first completed pass finalizes PCA through the repository PCA crate by
using the streaming accumulator path, truncates to the retained dimensionality
configured for the crate, and derives per-dimension projection weights from the
explained variance raised to the configured variance exponent.

No design path may require a full-pass `fit(...)` call over a retained logical
dataset.

### DSG-PCA-CHUNK-006 `Deterministic classifier-visible ordering`

For any embedding, the crate derives one scalar projection key by taking the
weighted sum of retained PCA coordinates, where each coordinate is weighted by
the corresponding explained variance raised to the configured variance
exponent.

The classifier-visible total order is determined by:

1. scalar projection key
2. retained PCA coordinates under lexicographic comparison
3. the original embedding values under lexicographic comparison

If exact chunking would require splitting members whose classifier-visible sort
keys remain fully identical, the crate fails explicitly instead of learning a
boundary the classifier cannot replay.

### DSG-PCA-CHUNK-007 `Replay-based exact contiguous chunk formation`

After the first PCA-analysis pass, the trainer computes the target cumulative
ranks for exact contiguous chunk boundaries.

Each later caller-visible replay pass discovers at most one next
classifier-visible boundary key above the prior lower bound. If that key group's
occupancy exactly reaches the required cumulative rank, the boundary is fixed.
If the key group would overshoot the required cumulative rank, the trainer fails
explicitly because exact classifier-replayable chunking is impossible for that
dataset.

This design intentionally trades additional caller-visible passes for bounded
implementation-owned state.

### DSG-PCA-CHUNK-008 `Remainder allocation and stable cluster identity`

When `N % K != 0`, the earlier chunks in sorted order receive the remainder so
that chunk sizes differ by at most one while still covering all members exactly
once.

Externally visible cluster IDs correspond to deterministic chunk position in the
sorted order, beginning at the first chunk and increasing monotonically through
the last chunk.

### DSG-PCA-CHUNK-009 `Classifier realization`

After `complete_training()`, `into_classifier()` consumes the trainer and yields
a classifier that reuses:

- the learned retained PCA transform
- the configured projection-key weighting rule
- the discovered classifier-visible chunk upper bounds

The classifier maps valid embeddings into `[0, K)` by computing the same
classifier-visible sort key and applying the learned chunk upper bounds.
Boundary ties are resolved toward the earliest matching chunk.

### DSG-PCA-CHUNK-010 `Pass reports`

Each completed pass yields a deterministic `PassReport` whose:

- `observed_count` equals the number of ingested embeddings
- `quality_metric` is deterministic and comparable across repeated runs for the
  same candidate
- `balance_metric` is zero when no explicit balance constraints are configured
- metric directions remain fixed for the run
- `readiness` reports whether the pass is still analysis-only or is now
  partition-ready

Analysis-only passes omit partition outputs. Partition-ready passes enumerate
the stable chunk IDs and realized cluster count.

### DSG-PCA-CHUNK-011 `Unsupported balance constraints`

The crate does not implement a separate balance policy beyond deterministic
exact chunk formation. If shared balance constraints are supplied, trainer
construction fails through the shared invalid-configuration category.

### DSG-PCA-CHUNK-012 `Error mapping`

The observable boundary maps failures into the shared error categories:

- invalid configuration
- invalid transition
- unsatisfiable constraint
- malformed input

PCA-specific diagnostics may appear in messages, but the public category
surface remains aligned with the shared streaming contract.

### DSG-PCA-CHUNK-013 `Verification realization`

The repository includes automated tests that exercise both the crate's
algorithm-specific observable behavior and the shared streaming clustering
conformance helpers.

### DSG-PCA-CHUNK-014 `Bounded implementation-owned state`

The implementation shall not retain, materialize, or spill the full logical
dataset, projected pass coordinates, or full sort tables.

Implementation-owned state may grow with the currently ingested batch, the
current classifier-visible sort-key group, or the small set of discovered
boundary keys, but not with the full logical dataset size.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-PCA-CHUNK-001 | REQ-PCA-CHUNK-001, REQ-PCA-CHUNK-002 |
| DSG-PCA-CHUNK-002 | REQ-PCA-CHUNK-003 |
| DSG-PCA-CHUNK-003 | REQ-PCA-CHUNK-010, REQ-PCA-CHUNK-017, REQ-PCA-CHUNK-018 |
| DSG-PCA-CHUNK-004 | REQ-PCA-CHUNK-010 |
| DSG-PCA-CHUNK-005 | REQ-PCA-CHUNK-004, REQ-PCA-CHUNK-005 |
| DSG-PCA-CHUNK-006 | REQ-PCA-CHUNK-008, REQ-PCA-CHUNK-009 |
| DSG-PCA-CHUNK-007 | REQ-PCA-CHUNK-005, REQ-PCA-CHUNK-006, REQ-PCA-CHUNK-007 |
| DSG-PCA-CHUNK-008 | REQ-PCA-CHUNK-007, REQ-PCA-CHUNK-013 |
| DSG-PCA-CHUNK-009 | REQ-PCA-CHUNK-012, REQ-PCA-CHUNK-013 |
| DSG-PCA-CHUNK-010 | REQ-PCA-CHUNK-011 |
| DSG-PCA-CHUNK-011 | REQ-PCA-CHUNK-015 |
| DSG-PCA-CHUNK-012 | REQ-PCA-CHUNK-014 |
| DSG-PCA-CHUNK-013 | REQ-PCA-CHUNK-016 |
| DSG-PCA-CHUNK-014 | REQ-PCA-CHUNK-017, REQ-PCA-CHUNK-018 |
