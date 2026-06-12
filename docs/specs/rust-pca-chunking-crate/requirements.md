<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust PCA Chunking Crate Requirements

## Status

Draft specification for a Rust crate that realizes PCA projection +
deterministic sort + exact chunking for LexonGraph through the shared streaming
clustering contract.

## Scope

This document specifies the crate-level requirements for a Rust crate that:

- realizes candidate family **B** from `docs/research/clustering_plan.md`
- conforms to the shared streaming trainer/classifier contract defined by
  `docs/specs/rust-streaming-clustering-crate/`
- reuses the repository PCA crate rather than introducing an independent PCA
  implementation

This document does not define recursive tree construction, block-store
integration, hierarchy routing, or alternate clustering algorithms.

## Terminology

In this spec package, `projection key` means the deterministic scalar ordering
key derived from one completed pass's retained PCA coordinates and documented
weighting rule.

`Exact chunking` means partitioning one deterministically sorted pass order into
exactly `K` contiguous non-empty chunks, where `K` is the shared
`cluster_count`.

`Chunk boundary model` means the retained projection transform plus the
deterministic decision rule used by the final classifier to map valid
embeddings into one of the trained chunk IDs.

## Requirements

### REQ-PCA-CHUNK-001

The repository shall define a dedicated Rust crate at
`crates/lexongraph-pca-chunking`.

### REQ-PCA-CHUNK-002

The crate shall remain subordinate to:

- `docs/research/clustering_plan.md` candidate family **B**
- `docs/specs/rust-streaming-clustering-crate/` for the shared streaming
  trainer/classifier contract
- `docs/specs/rust-pca-crate/` for PCA behavior consumed by this crate

If those sources appear to conflict, the shared streaming clustering
specification is authoritative for the trainer/classifier contract, this
specification package is authoritative for the concrete crate boundary it
defines, and the research plan is authoritative for the motivating candidate
family.

### REQ-PCA-CHUNK-003

The crate shall expose a trainer implementation conforming to
`StreamingClusterTrainer` and a classifier implementation conforming to
`StreamingClusterClassifier`.

### REQ-PCA-CHUNK-004

Trainer construction shall accept the shared `StreamingClusteringConfig` plus
typed algorithm parameters controlling the retained PCA dimensionality and the
documented projection-key weighting behavior.

### REQ-PCA-CHUNK-005

For each completed pass, the crate shall realize the candidate's core algorithm
by:

1. fitting PCA through the repository PCA crate
2. deriving the documented retained projection
3. computing one deterministic scalar projection key per embedding from that
   retained projection
4. deterministically sorting embeddings by projection key
5. partitioning the sorted order into exactly `K` contiguous non-empty chunks

### REQ-PCA-CHUNK-006

When `N` is divisible by `K`, the realized exact chunking shall assign exactly
`N / K` members to every final chunk.

### REQ-PCA-CHUNK-007

When `N` is not divisible by `K`, the crate shall apply one documented
deterministic remainder-allocation rule while still producing exactly `K`
non-empty chunks.

### REQ-PCA-CHUNK-008

The projection-key sort order shall be deterministic.

Projected-value ties shall be resolved through a documented total-order rule
whose final tie-break preserves pass dataset order.

### REQ-PCA-CHUNK-009

Low-rank, duplicate-heavy, or tied-projection completed passes shall not by
themselves force failure if deterministic exact `K` chunking remains realizable
under the documented total ordering.

### REQ-PCA-CHUNK-010

The first completed pass of one training run shall establish the logical
dataset. Each later completed pass shall represent the same logical dataset in
the same pass dataset order or fail explicitly.

### REQ-PCA-CHUNK-011

Each completed pass shall return a deterministic `PassReport` containing:

- `observed_count`
- `quality_metric`
- `balance_metric`
- quality and balance metric directions
- stable cluster identifiers

The balance metric shall be zero when no explicit balance constraints are
configured.

### REQ-PCA-CHUNK-012

After caller-directed training completion, the crate shall produce a
deterministic classifier that:

- reuses the learned projection transform plus the documented chunk-boundary
  model
- assigns each valid embedding to exactly one cluster ID in `[0, K)`
- rejects malformed embeddings through the shared malformed-input error category
- does not require the original training dataset after classifier production

### REQ-PCA-CHUNK-013

The observable contract shall preserve stable cluster identifiers across
repeated identical passes and in the final classifier surface.

### REQ-PCA-CHUNK-014

Invalid configuration, invalid state transitions, unsatisfiable chunking
constraints, and malformed input shall be surfaced through the shared streaming
error categories.

### REQ-PCA-CHUNK-015

This revision shall not define or claim a PCA-chunking-specific balance policy
beyond deterministic exact chunk formation.

If caller-provided shared balance constraints are present, the trainer shall
reject them explicitly through the shared invalid-configuration category.

### REQ-PCA-CHUNK-016

The repository shall include executable verification artifacts covering both:

- this crate's realization of PCA projection + deterministic sort + exact
  chunking
- this crate's conformance to the shared streaming clustering contract,
  including the opt-in conformance-helper surface

## Out of Scope

This crate does not define or own:

- recursive tree construction
- block loading or block validation
- representative-embedding derivation from stored blocks
- hierarchy routing or search behavior
- alternate candidate families from the research plan

## Relationship to Other Specifications

This document defines one concrete reusable candidate crate layered under the
shared streaming clustering contract and alongside other concrete
algorithm-specific crates.
