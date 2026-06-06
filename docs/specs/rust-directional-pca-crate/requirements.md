<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Directional PCA Crate Requirements

## Status

Draft specification for a Rust crate that realizes streaming directional-PCA
clustering for LexonGraph through the shared streaming clustering contract.

## Scope

This document specifies the crate-level requirements for a Rust crate that:

- preserves the directional-PCA partitioning algorithm described in
  `docs/arch/Directional PCA tree.md`
- conforms to the shared trainer/classifier contract defined by
  `crates/lexongraph-streaming-clustering`
- removes the obsolete block-store-backed single-layer boundary in favor of a
  native embedding-streaming surface

This document does not define recursive tree construction, block loading,
representative-embedding derivation from stored blocks, centroid block
materialization, or alternate clustering algorithms.

## Terminology

In this spec package, `streaming directional-PCA trainer` means a concrete
implementation of `StreamingClusterTrainer` whose completed passes realize the
directional-PCA partitioning mechanics owned by this crate.

`Pass dataset order` means the ordered embedding sequence observed across all
batches ingested before one `finish_pass()` call.

`Directional parameters` means the algorithm-specific controls for retained PCA
dimensionality, variance exponent `gamma`, temperature `tau`, and any explicit
stability thresholds retained by this crate boundary.

`Exact-K realizability` means that one completed pass can be partitioned into
exactly `K` stable, non-empty clusters under the crate's documented
directional-PCA mechanics, where `K` is the shared `cluster_count` from the
streaming contract.

## Requirements

### REQ-DPCA-STREAM-001

The repository shall define a dedicated Rust crate for streaming directional
PCA at `crates/lexongraph-directional-pca`.

### REQ-DPCA-STREAM-002

The crate shall remain subordinate to:

- `docs/arch/Directional PCA tree.md` for the algorithm's directional-PCA
  intent, allocation rationale, and stated stabilizers
- `docs/specs/rust-streaming-clustering-crate` for the shared streaming
  trainer/classifier contract
- `docs/specs/rust-pca-crate` for PCA behavior consumed by this crate

If those sources appear to conflict, the streaming trait specification is
authoritative for the shared contract surface, this specification package is
authoritative for the crate boundary and parameter-domain rules it defines, and
the architecture note is authoritative for the directional-PCA algorithmic
intent preserved here.

### REQ-DPCA-STREAM-003

The crate shall expose a trainer implementation conforming to
`StreamingClusterTrainer` and a classifier implementation conforming to
`StreamingClusterClassifier`.

### REQ-DPCA-STREAM-004

The public crate boundary shall be native to streamed embeddings.

This revision shall not retain the obsolete public block-ID plus `BlockStore`
execution boundary, representative-embedding derivation from loaded blocks, or
block-store-specific result and error ownership.

### REQ-DPCA-STREAM-005

Trainer construction shall accept the shared `StreamingClusteringConfig` plus
typed directional parameters.

The shared `cluster_count` is a hard requirement for the observable clustering
surface, and the directional parameters shall include at minimum:

- retained PCA dimension count or equivalent truncation control
- variance exponent `gamma`
- temperature `tau`
- explicit stability or eligibility thresholds retained by the scaled-down
  streaming crate boundary

For this crate boundary, `gamma` shall be finite and non-negative. The
architecture note's discussion of example or heuristic `gamma` ranges is
non-normative for this crate's accepted configuration domain.

### REQ-DPCA-STREAM-006

This scaled-down revision shall not define a directional-PCA-specific balance
policy beyond exact-K cluster realization.

If caller-provided shared balance constraints are present, the trainer shall
reject them explicitly through the shared invalid-configuration category rather
than silently ignore them or claim unsupported balancing behavior.

### REQ-DPCA-STREAM-007

The trainer shall validate malformed streamed input explicitly, including at
minimum:

- wrong embedding dimensionality
- non-finite embedding values
- empty completed passes

This revision does not require the crate to accept zero-norm embeddings unless
the finalized design or downstream cosine-aware consumers explicitly demand that
constraint.

### REQ-DPCA-STREAM-008

The crate shall preserve protocol-significant pass dataset order and shall not
treat permutation of the completed-pass embedding sequence as semantically
equivalent input.

### REQ-DPCA-STREAM-009

The trainer shall support caller-driven multi-pass refinement over the same
logical dataset through repeated `ingest_batch()` / `finish_pass()` cycles.

The crate shall not hide additional caller-invisible passes or replace the
shared pass lifecycle with an independent iteration API.

### REQ-DPCA-STREAM-010

After the first completed pass establishes the logical dataset for one training
run, each later completed pass shall represent the same logical dataset in the
same pass dataset order.

If a later pass differs in observed count or ordered embedding content from the
first completed pass, the trainer shall fail explicitly rather than claim
conformant refinement of the same run.

### REQ-DPCA-STREAM-011

For each completed pass, the crate shall realize directional-PCA partitioning
by using the repository PCA crate rather than redefining PCA decomposition
behavior independently.

### REQ-DPCA-STREAM-012

For each completed pass, the crate shall compute per-axis allocation scores
using both:

- centroid-direction coefficients
- explained-variance information

The conformant score shall be equivalent in effect to
`|alpha_i| * lambda_i^gamma`, where `gamma` is an explicit typed parameter.

### REQ-DPCA-STREAM-013

For each completed pass, the crate shall convert the per-axis scores into
per-axis resolution using a temperature-controlled allocation rule over the
shared hard cluster target `K`, with deterministic rounding and correction
behavior.

### REQ-DPCA-STREAM-014

The conformant default binning policy shall be quantile binning over the
retained PCA coordinates.

### REQ-DPCA-STREAM-015

The crate shall fail explicitly when one completed pass cannot realize exact-K
partitioning under the documented directional-PCA mechanics, including at
minimum:

- first-pass `Observed N < K`
- invalid or infeasible directional parameters
- inability of the realized directional-PCA partition to produce exactly `K`
  stable, non-empty clusters without changing the documented algorithmic
  semantics

The crate shall not silently adapt the partitioning behavior merely to force an
exact-K outcome.

### REQ-DPCA-STREAM-016

Each completed pass shall return a deterministic `PassReport` containing:

- `observed_count`
- `quality_metric`
- `balance_metric`
- quality and balance metric directions
- stable cluster identifiers

The balance metric shall be zero when no explicit balance constraints are
configured.

### REQ-DPCA-STREAM-017

The observable contract shall preserve stable cluster identifiers across
completed passes and in the final classifier surface.

### REQ-DPCA-STREAM-018

After caller-directed training completion, the crate shall produce a
deterministic classifier that:

- assigns each valid embedding to exactly one cluster ID in `[0, K)`
- rejects malformed embeddings through the shared malformed-input error category
- does not require the original dataset after classifier production

### REQ-DPCA-STREAM-019

Invalid configuration, invalid state transitions, unsatisfiable exact-K
constraints, and malformed input shall be surfaced through the shared streaming
error categories with deterministic terminal-error behavior for illegal
lifecycle transitions.

### REQ-DPCA-STREAM-020

The public API surface shall remain trimmed to the minimal behavior needed to
realize the native streaming directional-PCA contract.

Helpers, types, and tests that only support the retired block-store boundary
shall not remain as dead compatibility ballast.

### REQ-DPCA-STREAM-021

The repository shall include executable verification artifacts covering both:

- this crate's realization of the directional-PCA mechanics preserved by this
  specification package
- this crate's conformance to the shared streaming clustering contract,
  including the opt-in conformance-helper surface

## Out of Scope

This crate does not define or own:

- recursive tree construction across multiple directional-PCA layers
- block loading or block validation
- representative-embedding derivation from branch or leaf blocks
- centroid block persistence or block-store integration
- adaptive compressed-size estimation
- alternate clustering algorithms
- undocumented compatibility wrappers for the retired block-store API

## Relationship to Other Specifications

This document bridges the directional-PCA architecture note and the shared
streaming clustering trait package for one concrete crate boundary.

It intentionally narrows the public surface from the previous block-store-backed
single-layer crate contract to the scaled-down native streaming boundary needed
by the current repository direction.
