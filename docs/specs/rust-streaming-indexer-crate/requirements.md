<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Indexer Crate Requirements

## Status

Draft specification for a Rust crate that implements the LexonGraph indexing
protocol through a caller-visible streaming replay boundary.

## Scope

This document specifies the crate-level requirements for a new Rust crate that:

- implements `docs/protocol/indexing.md`
- preserves the protocol-visible indexing outputs and invariants defined by
  `docs/protocol/indexing.md` and `docs/protocol/blocks.md`
- exposes a caller-visible replay-based streaming API for large datasets
- uses the shared streaming clustering contract for its clustering boundary

This document is layered on top of:

- `docs/protocol/indexing.md`
- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`
- `docs/specs/rust-embeddings-trait/`
- `docs/specs/rust-streaming-clustering-crate/`
- `docs/specs/rust-dcbc-streaming-crate/` for one built-in clustering
  realization
- `docs/specs/rust-directional-pca-crate/` for one built-in clustering
  realization

This document defines the streaming indexer line directly against the protocol
documents and owned subordinate specifications listed above. Legacy
batch-oriented indexer artifacts are outside this specification package's
normative boundary.

## Terminology

In this spec package, `planning pass` (also referred to as a `streaming
indexing pass`) means one caller-driven replay of the logical item set
consisting of one or more streamed batches followed by a pass-completion
operation.

`Final materialization replay` means one additional caller-driven replay of the
same logical item set after planning completion, used to construct the finished
persisted block tree without requiring the crate to retain the full dataset as a
public-API obligation.

`Item replay order` means the ordered sequence of indexing items observed across
all batches in one completed streaming indexing pass.

`Partition hierarchy` means the deterministic coarse-to-fine planning tree over
the replayed logical item set that is finalized before bottom-up block
assembly.

`Terminal partition` means one partition in that hierarchy chosen as a direct
input to bottom-up parent construction over materialized leaves.

## Requirements

### REQ-STREAM-INDEXER-001

The repository shall define a dedicated Rust crate at
`crates/lexongraph-streaming-indexer` that owns the caller-visible streaming
indexing API boundary.

### REQ-STREAM-INDEXER-002

The new crate shall remain subordinate to:

- `docs/protocol/indexing.md` for indexing invariants
- `docs/protocol/blocks.md` for block semantics and canonicalization
- the block crate, block-storage trait crate, and embeddings-trait crate for
  their owned dependency concerns
- `docs/specs/rust-streaming-clustering-crate/` for the shared streaming
  clustering contract
- `docs/specs/rust-dcbc-streaming-crate/` and
  `docs/specs/rust-directional-pca-crate/` for the built-in clustering
  realizations owned outside this crate

### REQ-STREAM-INDEXER-003

This specification package shall define the streaming indexer line directly
against `docs/protocol/indexing.md`, `docs/protocol/blocks.md`, and its owned
subordinate specifications without making any retired legacy batch-oriented
indexing crate or specification package part of the streaming crate's
normative conformance boundary.

### REQ-STREAM-INDEXER-004

The crate shall define a caller-visible streaming indexing API whose lifecycle
includes:

- starting a streaming indexing run for one indexing context
- ingesting one or more batches of indexing items for the current planning pass
- completing the current planning pass and obtaining a deterministic pass report
- caller-directed planning continuation or completion
- final materialization through a final materialization replay that assembles the
  finished block tree bottom-up from the finalized partition hierarchy

### REQ-STREAM-INDEXER-005

The caller-visible streaming indexing API shall accept a non-empty ordered
stream of indexing items partitioned into caller-chosen batches for each pass.

### REQ-STREAM-INDEXER-006

Each indexing item shall carry application metadata plus a content reference at
the public API boundary.

### REQ-STREAM-INDEXER-007

This revision shall use a reference-based input model and shall not require raw
content bytes or inline content bodies to be passed directly in the input item
stream.

### REQ-STREAM-INDEXER-008

The crate shall require a pluggable content-resolution trait that accepts an
item's content reference and returns the concrete content used for indexing.

### REQ-STREAM-INDEXER-009

The crate shall depend on the shared embeddings-trait crate for the
embedding-provider contract used by streaming indexing.

### REQ-STREAM-INDEXER-010

The crate shall keep protocol-required orchestration separate from
implementation-defined policy concerns through trait-based extension points.

At minimum, the crate shall expose or depend on policy boundaries for:

- content resolution
- embedding generation through the shared embeddings-trait contract
- canonical-embedding selection for child-bearing blocks
- hierarchical planning over replayed original-item embeddings, with any
  clustering subproblems flowing through the shared streaming clustering
  contract
- terminal-partition normalization or termination policy used by bottom-up
  assembly

### REQ-STREAM-INDEXER-011

The crate shall provide built-in planning realizations for hierarchical planning
that depend on both `lexongraph-dcbc-streaming` and
`lexongraph-directional-pca` rather than reimplementing either clustering
algorithm locally.

### REQ-STREAM-INDEXER-012

The crate shall provide an explicit API path that accepts a caller-supplied
hierarchical planning realization, strategy, or factory so downstream users can
replace the built-in planning behavior.

### REQ-STREAM-INDEXER-013

The crate shall provide a built-in default `CanonicalEmbeddingPolicy`
implementation whose canonical embedding for a produced child-bearing block is
the component-wise arithmetic mean of the embeddings stored in that block's
finalized entries.

### REQ-STREAM-INDEXER-014

The crate shall not assign an implicit built-in default planning algorithm.

The caller-facing built-in planning path for the streaming indexing runtime
API shall require the caller to select one supported built-in planning
realization explicitly.

### REQ-STREAM-INDEXER-015

The crate shall continue to provide explicit API paths that accept
caller-supplied canonical-embedding, hierarchical planning, and streaming
clustering policy implementations.

### REQ-STREAM-INDEXER-016

The first completed streaming indexing pass shall establish the logical item set
and item replay order for the run.

Each later completed pass and the final materialization replay shall represent
the same logical item set in the same item replay order or fail explicitly.

### REQ-STREAM-INDEXER-017

The public contract shall remain dataset-size independent by requiring caller
replay for repeated passes and final materialization rather than requiring the
crate's default API surface to retain or rematerialize the full logical dataset
on the caller's behalf.

### REQ-STREAM-INDEXER-018

The core streaming indexer shall own the protocol-required orchestration, leaf
construction, normalization, block construction, higher-layer construction, and
block persistence flow.

Implementation-defined policies may propose clustering behavior, but the core
indexer shall remain authoritative for protocol conformance checks.

### REQ-STREAM-INDEXER-019

For the caller-visible replay passes over original indexing items, the crate
shall use the shared streaming clustering contract as the clustering boundary
for deriving a deterministic hierarchical partition plan over original-item
embeddings rather than directly deriving the first parent-producing layer from
leaf-level embeddings.

### REQ-STREAM-INDEXER-020

After planning completion and during the final materialization replay, the crate
shall construct the finished block tree by materializing leaves and assembling
parent layers bottom-up from the finalized partition hierarchy.

Any clustering used while deriving or refining that hierarchy shall continue to
flow through the shared streaming clustering contract rather than an older
batch-only clustering boundary.

### REQ-STREAM-INDEXER-021

Each completed streaming indexing pass shall return a deterministic structured
pass report that includes:

- the observed item count for that pass
- deterministic planning progress or quality information for the caller-visible
  replayed hierarchy-building work, derived from the shared streaming
  clustering surface wherever clustering participates in that work
- enough structured state for the caller to decide whether to continue or stop

### REQ-STREAM-INDEXER-022

The crate shall provide an optional caller-supplied status observer contract for
streaming planning progress, final materialization progress, and bottom-up
assembly progress.

Status updates shall be emitted as structured data suitable for arbitrary
caller-owned handling and shall not require any particular sink.

### REQ-STREAM-INDEXER-023

When planning, final materialization, or higher-layer assembly work remains
active long enough to be non-trivial, the crate shall emit periodic in-progress
status updates rather than only terminal state.

### REQ-STREAM-INDEXER-024

The crate shall surface explicit failure when:

- the input pass is empty
- the overall logical item set is empty
- content resolution fails, is inaccessible, or returns content unusable for
  indexing
- embedding generation fails
- the caller omits a required built-in planning algorithm selection or required
  planning settings
- a later replay differs from the established logical item set or replay order
- the finalized partition hierarchy is invalid, overlapping, non-covering, or
  otherwise inconsistent with the replayed logical item set
- hybrid planning configuration is invalid
- a terminal partition cannot be normalized or assembled into
  protocol-conforming parent blocks
- clustering, canonical-embedding selection, block construction, or storage
  fails
- final materialization is requested before planning completion

### REQ-STREAM-INDEXER-025

In this revision, successful content resolution shall supply the media type and
bytes stored inline in the produced leaf entry's `content` payload.

### REQ-STREAM-INDEXER-026

Given the same logical item set, metadata, content references resolving to the
same logical content, `embedding_spec`, block size target, deterministic
dependency behavior, pass boundaries, and replay order, the crate shall produce
the same pass reports, the same finalized partition hierarchy, the same final
root block ID, and the same persisted block set.

### REQ-STREAM-INDEXER-027

The final materialization replay and the higher-layer construction flow shall
preserve the indexing protocol's required externally visible invariants,
including:

- exactly one leaf block per content item
- exactly one leaf entry in each produced leaf block
- normalized child-bearing entries sorted by raw embedding bytes with required
  deterministic tie-breaks
- child-bearing entry deduplication by child block ID
- intermediate node blocks that remain at or below the configured size target
- intermediate node blocks that contain at least two child entries
- exactly one final root block

### REQ-STREAM-INDEXER-028

The crate shall produce a successful final result containing a root block ID and
the complete persisted block set required to materialize that root.

### REQ-STREAM-INDEXER-029

The crate shall provide reusable conformance-test helpers for the
implementation-defined policy traits it defines, and those helpers shall be
exposed only through an opt-in, non-default, test-oriented surface.

### REQ-STREAM-INDEXER-030

The repository shall include automated verification artifacts that realize the
validation surface defined in
`docs/specs/rust-streaming-indexer-crate/validation.md`.

### REQ-STREAM-INDEXER-031

The crate shall provide a caller-visible built-in planning-selection surface
that requires callers to choose either the built-in streaming directional-PCA
realization or the built-in streaming DCBC realization without implementing a
custom planning factory.

### REQ-STREAM-INDEXER-032

When a built-in planning realization is selected through the indexer API, the
crate shall require the caller to provide that algorithm's settings rather than
supplying implicit built-in planning settings.

### REQ-STREAM-INDEXER-033

The repository verification artifacts for algorithm-agnostic built-in-path
behavior over fixtures compatible with both built-in planning realizations'
caller-supplied settings shall realize the corresponding validation coverage as
a matrix over both built-in planning realizations.

Algorithm-specific behavior may be validated through separate targeted cases
rather than forced into that symmetric matrix.

### REQ-STREAM-INDEXER-034

The crate shall define an explicit deterministic planning boundary over replayed
original-item embeddings that is distinct from final block materialization.

### REQ-STREAM-INDEXER-035

The crate shall define a deterministic mapping from the finalized partition
hierarchy to a bottom-up block tree, including deterministic normalization or
explicit failure for singleton, undersized, or oversized terminal partitions.

### REQ-STREAM-INDEXER-036

The crate shall support hybrid coarse/fine algorithm selection and require
explicit caller-visible configuration for the phase boundary and
algorithm-specific settings.

### REQ-STREAM-INDEXER-037

Independent subpartitions may be processed concurrently only if partition
identity, pass reports, root block ID, and persisted block set remain
deterministic and schedule-independent.

### REQ-STREAM-INDEXER-038

Terminal planning units shall be reconciled against a deterministic
materializability bound derived from the block size target and
`embedding_spec` before or during final assembly, or fail explicitly before
claiming a conformant result.

## Out of Scope

This crate does not define or own:

- block wire encoding or block validity rules
- block-ID derivation rules
- storage backend implementations
- the shared embedding-provider trait contract
- the shared streaming clustering trait definitions
- legacy batch-oriented implementation lines or their repository lifecycle
- any concrete clustering algorithm beyond the built-in directional-PCA and
  DCBC planning options exposed by this crate

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/indexing.md` and
`docs/protocol/blocks.md`.

This document is also subordinate to the block crate, block-storage trait,
embeddings-trait, streaming clustering, streaming DCBC, and directional-PCA
specification packages for their owned concerns.
