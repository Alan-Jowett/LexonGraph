<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust PCA Chunking Crate Design

## Status

Draft design specification for a Rust crate that realizes PCA projection +
deterministic sort + exact chunking through the shared LexonGraph streaming
clustering contract.

## Design Goals

The crate design is intended to be:

- a simple reusable realization of research-plan candidate family **B**
- conformant to the shared streaming clustering contract
- deterministic at the observable API boundary
- explicit about exact chunk-formation behavior
- minimal in its public surface

## Crate Boundary

The crate owns:

- a concrete streaming trainer implementation
- a concrete streaming classifier implementation
- projection-key derivation and deterministic sort behavior
- deterministic exact chunk formation and chunk-boundary classification
- the minimal retained pass-scoped state needed for same-dataset multi-pass
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

### DSG-PCA-CHUNK-003 `Pass-scoped realization`

`ingest_batch()` validates embeddings through the shared malformed-input surface
and appends them to the current pass dataset order.

The implementation may retain one completed pass internally when required to
fit PCA, derive deterministic ordering, and produce the chunk-boundary model.

### DSG-PCA-CHUNK-004 `Cross-pass continuity`

The first completed pass establishes the logical dataset for one training run.
Each later pass is validated against that baseline for identical observed count
and identical ordered embedding content before the trainer claims conformant
refinement of the same run.

### DSG-PCA-CHUNK-005 `Projection-key realization`

Each successful completed pass fits PCA through the repository PCA crate,
truncates to the retained dimensionality configured for the crate, and projects
each embedding into retained PCA coordinates.

The crate then derives one scalar projection key per embedding by taking the
weighted sum of retained coordinates, where each coordinate is weighted by the
corresponding explained variance raised to the configured variance exponent.

### DSG-PCA-CHUNK-006 `Deterministic total ordering`

The pass realization deterministically sorts embeddings by:

1. scalar projection key
2. retained PCA coordinates under lexicographic comparison
3. the original embedding values under lexicographic comparison
4. original pass dataset order as the final tie-break

This ordering remains valid even when many embeddings share equal projection
keys because the pass-order tie-break defines a total order.

The classifier boundary model is defined only over the reproducible portion of
that ordering. If exact chunking would require splitting members whose
classifier sort keys remain fully identical after removing pass-order position,
the crate fails explicitly instead of learning a boundary the classifier cannot
replay.

### DSG-PCA-CHUNK-007 `Exact contiguous chunk formation`

After sorting, the crate partitions the ordered pass into exactly `K`
contiguous chunks.

If `N % K == 0`, all chunks have identical occupancy. Otherwise, the crate
assigns the remainder deterministically to the earliest chunks in sorted order
so chunk sizes differ by at most one while still covering all members exactly
once.

### DSG-PCA-CHUNK-008 `Stable cluster identity`

Externally visible cluster IDs correspond to deterministic chunk position in the
sorted order, beginning at the first chunk and increasing monotonically through
the last chunk.

Repeated identical passes therefore preserve stable cluster IDs without needing
an independent cross-pass matching algorithm.

### DSG-PCA-CHUNK-009 `Classifier realization`

After `complete_training()`, `into_classifier()` consumes the trainer and yields
a classifier that reuses:

- the learned retained PCA transform
- the configured projection-key weighting rule
- the learned scalar chunk boundaries

The classifier maps valid embeddings into `[0, K)` by computing the same scalar
projection key plus the same classifier-visible lexicographic tie-break fields
and applying the learned chunk upper bounds. Boundary ties are resolved toward
the earliest matching chunk.

### DSG-PCA-CHUNK-010 `Pass reports`

Each completed pass yields a deterministic `PassReport` whose:

- `observed_count` equals the number of ingested embeddings
- `quality_metric` is deterministic and comparable across repeated runs for the
  same candidate
- `balance_metric` is zero when no explicit balance constraints are configured
- metric directions remain fixed for the run
- `cluster_ids` enumerate the stable chunk IDs

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

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-PCA-CHUNK-001 | REQ-PCA-CHUNK-001, REQ-PCA-CHUNK-002 |
| DSG-PCA-CHUNK-002 | REQ-PCA-CHUNK-003 |
| DSG-PCA-CHUNK-003 | REQ-PCA-CHUNK-010, REQ-PCA-CHUNK-014 |
| DSG-PCA-CHUNK-004 | REQ-PCA-CHUNK-010 |
| DSG-PCA-CHUNK-005 | REQ-PCA-CHUNK-004, REQ-PCA-CHUNK-005 |
| DSG-PCA-CHUNK-006 | REQ-PCA-CHUNK-008, REQ-PCA-CHUNK-009 |
| DSG-PCA-CHUNK-007 | REQ-PCA-CHUNK-006, REQ-PCA-CHUNK-007 |
| DSG-PCA-CHUNK-008 | REQ-PCA-CHUNK-013 |
| DSG-PCA-CHUNK-009 | REQ-PCA-CHUNK-012, REQ-PCA-CHUNK-013 |
| DSG-PCA-CHUNK-010 | REQ-PCA-CHUNK-011 |
| DSG-PCA-CHUNK-011 | REQ-PCA-CHUNK-015 |
| DSG-PCA-CHUNK-012 | REQ-PCA-CHUNK-014 |
| DSG-PCA-CHUNK-013 | REQ-PCA-CHUNK-016 |
