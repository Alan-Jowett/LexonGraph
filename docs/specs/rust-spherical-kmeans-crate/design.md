<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->

# Rust Spherical K-Means Crate Design

## Status

Draft design specification for a Rust crate that realizes vanilla spherical
k-means through the shared LexonGraph streaming clustering contract.

## Design Goals

The crate design is intended to be:

- deliberately boring and interpretable as a control candidate
- conformant to the shared streaming clustering contract
- deterministic at the observable API boundary
- explicit about normalized-space semantics

## Crate Boundary

The crate owns:

- a concrete streaming spherical-k-means trainer implementation
- a concrete streaming spherical-k-means classifier implementation
- deterministic normalized-space assignment and centroid-update behavior
- the minimal retained pass-scoped state needed for same-dataset multi-pass
  refinement

The crate does not own:

- the shared streaming trait definitions
- recursive hierarchy construction
- block-store integration
- evaluator-owned benchmark policy

## Design Entries

### DSG-SPHKM-001 `Composite normative boundary`

The crate depends on `docs/research/clustering_plan.md` for the motivating
control-candidate role and on `docs/specs/rust-streaming-clustering-crate/` for
the shared trainer/classifier contract.

### DSG-SPHKM-002 `Concrete trainer/classifier realization`

The crate exposes one trainer type implementing `StreamingClusterTrainer` and
one classifier type implementing `StreamingClusterClassifier`.

### DSG-SPHKM-003 `Normalized-space realization`

`ingest_batch()` validates embeddings through the shared malformed-input surface
and appends them to the current pass dataset order. `finish_pass()` normalizes
the completed-pass embeddings into unit-norm space before fitting spherical
k-means.

Zero-norm embeddings are rejected explicitly rather than normalized silently.

### DSG-SPHKM-004 `Deterministic initialization and refinement`

Each successful completed pass:

1. validates exact-`K` feasibility prerequisites
2. normalizes the completed-pass embeddings
3. chooses exactly `K` initial centroids through one documented deterministic
   initialization rule
4. alternates normalized-space assignment and centroid recomputation for a
   deterministic bounded number of Lloyd steps or until the documented
   convergence condition is met
5. fails explicitly if the documented process cannot realize exactly `K`
   non-empty clusters without changing the algorithmic semantics

The crate does not perform hidden caller-invisible passes.

### DSG-SPHKM-005 `Cross-pass continuity`

The first completed pass establishes the logical dataset for one training run.
Each later pass is validated against that baseline for identical observed count
and identical ordered embedding content before the trainer claims conformant
refinement of the same run.

### DSG-SPHKM-006 `Stable cluster identity`

Externally visible cluster IDs remain stable across repeated identical passes.
If repeated deterministic fits would otherwise permute internal centroid order,
the crate applies deterministic matching and tie-breaking before exposing pass
reports or classifier assignments.

### DSG-SPHKM-007 `Classifier realization`

After `complete_training()`, `into_classifier()` consumes the trainer and yields
a classifier that normalizes each valid query embedding and assigns it to the
best learned centroid under the documented spherical similarity ordering.

### DSG-SPHKM-008 `Unsupported balance constraints`

The crate does not implement a separate balance policy beyond exact `K` cluster
realization. If shared balance constraints are supplied, trainer construction
fails through the shared invalid-configuration category.

### DSG-SPHKM-009 `Error mapping`

The observable boundary maps failures into the shared error categories:

- invalid configuration
- invalid transition
- unsatisfiable constraint
- malformed input

### DSG-SPHKM-010 `Verification realization`

The repository includes automated tests that exercise both the crate's
algorithm-specific observable behavior and the shared streaming clustering
conformance helpers.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-SPHKM-001 | REQ-SPHKM-001, REQ-SPHKM-002 |
| DSG-SPHKM-002 | REQ-SPHKM-003 |
| DSG-SPHKM-003 | REQ-SPHKM-006, REQ-SPHKM-007 |
| DSG-SPHKM-004 | REQ-SPHKM-004, REQ-SPHKM-005, REQ-SPHKM-013 |
| DSG-SPHKM-005 | REQ-SPHKM-008 |
| DSG-SPHKM-006 | REQ-SPHKM-009, REQ-SPHKM-012 |
| DSG-SPHKM-007 | REQ-SPHKM-011, REQ-SPHKM-012 |
| DSG-SPHKM-008 | REQ-SPHKM-010 |
| DSG-SPHKM-009 | REQ-SPHKM-013 |
| DSG-SPHKM-010 | REQ-SPHKM-014 |
