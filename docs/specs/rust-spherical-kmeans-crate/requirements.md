<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->

# Rust Spherical K-Means Crate Requirements

## Status

Draft specification for a Rust crate that realizes vanilla spherical k-means
for LexonGraph through the shared streaming clustering contract.

## Scope

This document specifies the crate-level requirements for a Rust crate that:

- realizes a boring spherical-k-means control candidate for
  `docs/research/clustering_plan.md`
- conforms to the shared streaming trainer/classifier contract defined by
  `docs/specs/rust-streaming-clustering-crate/`
- remains deterministic enough for repeated comparative evaluator runs

This document does not define recursive tree construction, hierarchy routing,
block-store integration, or evaluator-owned ranking logic.

## Terminology

In this spec package, `spherical parameters` means the algorithm-local controls
for deterministic initialization, iteration budget, and any explicit convergence
or stabilization thresholds retained by this crate boundary.

`Normalized embedding space` means the unit-norm vector space obtained by
explicit L2 normalization of valid input embeddings before spherical-k-means
assignment and centroid updates.

## Requirements

### REQ-SPHKM-001

The repository shall define a dedicated Rust crate at
`crates/lexongraph-spherical-kmeans`.

### REQ-SPHKM-002

The crate shall remain subordinate to:

- `docs/research/clustering_plan.md` for the motivating control-candidate role
- `docs/specs/rust-streaming-clustering-crate/` for the shared streaming
  trainer/classifier contract
- `docs/specs/rust-linear-algebra-acceleration-crate/` for any shared optional
  acceleration boundary reused by this crate

If those sources appear to conflict, the shared streaming clustering
specification is authoritative for the trainer/classifier contract, this
specification package is authoritative for the concrete crate boundary it
defines, and the research plan is authoritative for the candidate's role as a
boring baseline.

### REQ-SPHKM-003

The crate shall expose a trainer implementation conforming to
`StreamingClusterTrainer` and a classifier implementation conforming to
`StreamingClusterClassifier`.

### REQ-SPHKM-004

Trainer construction shall accept the shared `StreamingClusteringConfig` plus
typed spherical-k-means parameters controlling at minimum:

- deterministic centroid-initialization policy
- maximum Lloyd-iteration count or equivalent refinement budget
- explicit convergence or stabilization thresholds retained by the crate

### REQ-SPHKM-005

For each completed pass, the crate shall realize vanilla spherical k-means by:

1. validating and L2-normalizing the completed-pass embeddings
2. constructing exactly `K` initial centroids through one documented
   deterministic initialization rule
3. iterating deterministic assignment and centroid-update steps in normalized
   embedding space
4. producing exactly `K` stable, non-empty clusters or failing explicitly if the
   documented algorithm cannot realize that outcome

### REQ-SPHKM-006

The classifier assignment and training-time assignment semantics shall both be
defined in normalized embedding space using the crate's documented spherical
similarity or equivalent angular-distance ordering.

### REQ-SPHKM-007

The crate shall validate malformed streamed input explicitly, including at
minimum:

- wrong embedding dimensionality
- non-finite embedding values
- zero-norm embeddings
- empty completed passes

### REQ-SPHKM-008

The first completed pass of one training run shall establish the logical
dataset. Each later completed pass shall represent the same logical dataset in
the same pass dataset order or fail explicitly.

### REQ-SPHKM-009

Each completed pass shall return a deterministic `PassReport` containing:

- `observed_count`
- `quality_metric`
- `balance_metric`
- quality and balance metric directions
- stable cluster identifiers

The balance metric shall be zero when no explicit balance constraints are
configured.

### REQ-SPHKM-010

This revision shall not define or claim a spherical-k-means-specific balance
policy beyond exact `K` cluster realization.

If caller-provided shared balance constraints are present, the trainer shall
reject them explicitly through the shared invalid-configuration category.

### REQ-SPHKM-011

After caller-directed training completion, the crate shall produce a
deterministic classifier that:

- reuses the learned normalized centroids
- normalizes each valid query embedding before assignment
- assigns each valid embedding to exactly one cluster ID in `[0, K)`
- rejects malformed embeddings through the shared malformed-input error category
- does not require replay of the original training dataset after classifier
  production

### REQ-SPHKM-012

The observable contract shall preserve stable cluster identifiers across
repeated identical passes and in the final classifier surface.

### REQ-SPHKM-013

Invalid configuration, invalid state transitions, unsatisfiable exact-`K`
clustering, and malformed input shall be surfaced through the shared streaming
error categories.

### REQ-SPHKM-014

The repository shall include executable verification artifacts covering both:

- this crate's observable spherical-k-means behavior
- this crate's conformance to the shared streaming clustering contract,
  including the opt-in conformance-helper surface

### REQ-SPHKM-015

This revision shall add optional backend-selectable acceleration through the
shared repository-owned linear-algebra acceleration boundary while preserving a
correct CPU realization.

### REQ-SPHKM-016

This revision shall not treat GPU offload as sufficient by itself. Any WGPU path
claimed by this crate shall be justified by a statistically repeatable wall-
clock win over the CPU path on both:

- a targeted spherical-kmeans microbenchmark
- the canonical realistic section-4 qualification benchmark on this machine

For each proof surface above, the benchmark rule is:

- run 5 identical executions on CPU
- run 5 identical executions on WGPU
- compare median wall-clock time per backend
- accept acceleration only if the WGPU median is strictly lower than the CPU
  median

### REQ-SPHKM-017

The accelerated realization may target only the computational hot path or hot
paths whose offload yields the measured win. It need not offload every step of
the algorithm.

### REQ-SPHKM-018

If the accelerated realization performs dense point-to-centroid or equivalent
large linear-algebra work, it shall support chunked or tiled execution through
the shared acceleration boundary rather than requiring whole logical matrix
materialization in device memory.

### REQ-SPHKM-019

CPU and WGPU executions of the same conformant workload shall preserve the same
observable spherical-kmeans semantics, stable cluster IDs, and classifier
behavior, allowing only explicitly documented floating-point tolerance.

### REQ-SPHKM-020

Artifacts used to prove accelerated conformance shall record which backend
executed and whether fallback occurred, so net-speedup claims are auditable.
