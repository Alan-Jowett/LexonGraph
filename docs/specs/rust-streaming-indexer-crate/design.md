<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Indexer Crate Design

## Status

Draft design specification for a Rust crate that implements the LexonGraph
indexing protocol through a caller-visible streaming replay boundary.

## Design Goals

The crate design is intended to be:

- protocol-conforming
- deterministic at the observable boundary
- explicit about replay lifecycle and failure behavior
- dataset-size independent at the caller-facing API boundary
- reusable across content sources and embedding providers
- compatible with the shared streaming clustering contract

## Crate Boundary

The crate owns:

- streaming indexing-oriented public types
- caller-visible replay lifecycle orchestration
- leaf construction and final block materialization orchestration
- normalization required by the indexing protocol
- indexer-owned error taxonomy and status reporting
- conformance helpers for indexer-owned policy traits

The crate does not own:

- block wire encoding or block-ID derivation
- block validation rules beyond invoking the block crate
- storage backend implementations
- the shared embedding-provider trait contract
- the shared streaming clustering trait definitions
- legacy batch-oriented implementation lines or their repository lifecycle

## Design Entries

### DSG-STREAM-INDEXER-001 `Composite normative boundary`

The crate depends on the indexing and block protocol documents plus the block
crate, block-storage trait crate, embeddings-trait crate, streaming clustering
trait crate, streaming DCBC crate specification package, and directional-PCA
crate specification package for their owned concerns. The crate does not
redefine those sources.

### DSG-STREAM-INDEXER-002 `Direct protocol-anchored line`

The streaming crate is specified directly against the indexing and block
protocols plus its owned subordinate specifications. Retired legacy
batch-oriented indexing crate and specification artifacts are outside this
package's conformance boundary and are not required to remain present for this
package to apply.

### DSG-STREAM-INDEXER-003 `Public replay lifecycle`

The crate exposes a public orchestration type or equivalent API for one
streaming indexing run with the following observable lifecycle:

1. start a run for one indexing context
2. ingest one or more batches of indexing items for the current pass
3. complete the pass and obtain an `IndexingPassReport`
4. either begin the next pass or mark training complete
5. consume one final materialization replay to produce the finished index result

### DSG-STREAM-INDEXER-004 `IndexItem`

A public input type representing one application-supplied indexing unit. Each
value contains:

- application metadata
- a content reference

The public input boundary remains reference-based rather than carrying inline
raw content bytes.

### DSG-STREAM-INDEXER-005 `Dependency and policy seams`

The crate consumes:

- a content resolver through an indexer-owned trait
- an embedding provider through the shared embeddings-trait contract
- a canonical-embedding policy through an indexer-owned trait
- a streaming clustering realization or factory whose trainer/classifier surface
  is defined by the shared streaming clustering contract

### DSG-STREAM-INDEXER-006 `Built-in clustering selection`

The crate exposes:

- a built-in arithmetic-mean canonical-embedding policy
- built-in streaming clustering realizations backed by
  `lexongraph-directional-pca` and `lexongraph-dcbc-streaming`
- a caller-visible built-in clustering-selection surface that requires explicit
  selection of one realization without requiring a caller-implemented factory
- caller-supplied algorithm settings for the selected built-in clustering
  realization
- no implicit built-in default clustering algorithm or implicit built-in
  clustering settings

The crate also exposes override paths for caller-supplied canonical-embedding
and clustering policies.

### DSG-STREAM-INDEXER-007 `Replay baseline establishment`

The first successful completed pass establishes the run's logical item set and
item replay order. The run stores only the deterministic baseline information
needed to verify later replay equivalence rather than a public obligation to
retain the full dataset for callers.

### DSG-STREAM-INDEXER-008 `Replay continuity enforcement`

Each later completed pass and the final materialization replay are validated
against the first-pass baseline for:

- identical observed item count
- identical ordered metadata and content-reference sequence
- identical resolved-content and embedding outcomes wherever those values are
  part of the deterministic indexing context

Deviation fails explicitly before claiming conformant continuation.

### DSG-STREAM-INDEXER-009 `Dataset-size-independent public surface`

The public API requires the caller to replay the logical item set for repeated
passes and final materialization rather than requiring the crate to keep the
entire corpus resident or rematerializable through hidden caller-facing state.

Implementation-internal transient storage is permitted, but it is not part of
the caller contract.

### DSG-STREAM-INDEXER-010 `Caller-visible pass realization`

Each completed pass over original indexing items performs, in deterministic item
replay order:

1. content resolution for each item
2. ordered embedding generation for each resolved item
3. deterministic derivation of leaf-level embeddings and any pass-local leaf
   artifacts
4. streaming ingestion of those leaf-level embeddings into the selected shared
   streaming clustering realization for the first parent-producing layer
5. pass completion on that clustering realization
6. construction of the public `IndexingPassReport`

The completed pass does not yet claim a finished persisted block tree.

### DSG-STREAM-INDEXER-011 `Pass report surface`

`IndexingPassReport` carries at least:

- the observed item count for the completed pass
- deterministic clustering fitness information for the caller-visible replayed
  layer, derived from the shared streaming clustering pass-report surface
- structured state sufficient for caller stop/continue decisions

The report remains deterministic for a fixed indexing context and replay order.

### DSG-STREAM-INDEXER-012 `Training completion gate`

The run exposes a caller-directed transition that marks training complete only
after at least one successful completed pass. Final materialization before that
transition fails explicitly.

### DSG-STREAM-INDEXER-013 `Final materialization replay`

After training completion, the run consumes one final materialization replay of
the same logical item set in the same replay order.

During that replay, the crate:

1. resolves content and generates embeddings again in deterministic order
2. constructs exactly one leaf block per item
3. persists the produced leaf blocks
4. applies the finalized first-layer clustering state to materialize the first
   parent-producing layer deterministically

### DSG-STREAM-INDEXER-014 `Higher-layer realization`

Once the first parent-producing layer has been materialized, the crate may build
higher parent layers through internal replay of already materialized child
blocks rather than requiring further caller replay of the original items.

Any clustering used for those higher layers still flows through the shared
streaming clustering contract by constructing deterministic internal replays
over the already materialized child layer.

### DSG-STREAM-INDEXER-015 `Core conformance ownership`

The core streaming indexer remains responsible for:

- one-leaf-per-item construction
- parent-entry normalization
- child-bearing entry deduplication
- size-target enforcement
- minimum-child-count enforcement
- final root determination
- block persistence and result packaging

Policy seams may propose clustering or canonical embeddings, but they do not
own those protocol checks.

### DSG-STREAM-INDEXER-016 `Arithmetic-mean canonical policy`

The built-in default canonical-embedding policy computes the component-wise
arithmetic mean of the embeddings stored in a child-bearing block's finalized
entries, using deterministic numeric rules that preserve the indexing and block
protocols' externally visible invariants for supported encodings.

### DSG-STREAM-INDEXER-017 `Status observer model`

The crate exposes an optional caller-supplied status observer receiving
structured progress updates for:

- caller-visible replay-pass progress
- first-layer clustering start, in-progress, completion, and failure
- final materialization progress
- higher-layer construction progress

The observer surface is sink-agnostic and does not require console output,
tracing integration, or repository-specific telemetry.

### DSG-STREAM-INDEXER-018 `Explicit error taxonomy`

The crate defines an explicit error surface covering at least:

- empty input or empty pass
- replay mismatch
- invalid metadata
- content-resolution failure
- unusable resolved content
- embedding failure
- clustering failure
- canonical-embedding failure
- block-construction failure
- storage failure
- invalid lifecycle transition

### DSG-STREAM-INDEXER-019 `Determinism boundary`

If the content resolver, embedding provider, canonical-embedding policy, block
store interactions affecting visibility, and streaming clustering realization
are deterministic within one indexing context, then identical item replays,
pass boundaries, and final materialization replay produce the same pass reports,
root block ID, and persisted block set.

### DSG-STREAM-INDEXER-020 `Result packaging`

A successful final result contains the root block ID and the complete persisted
block set, or the complete produced block IDs sufficient to identify that set,
for the protocol-conforming finished tree.

### DSG-STREAM-INDEXER-021 `Feature-gated conformance surface`

The crate exposes reusable conformance-test helpers for its indexer-owned policy
traits behind a non-default Cargo feature intended for downstream tests only.

### DSG-STREAM-INDEXER-022 `Built-in algorithm verification matrix`

Algorithm-agnostic behavior exercised through the built-in clustering-selection
surface over fixtures compatible with both algorithms' caller-supplied settings
is intended to hold regardless of whether the caller selects the built-in
directional-PCA realization or the built-in DCBC realization.

Repository verification artifacts therefore realize that algorithm-agnostic
built-in-path behavior through a two-algorithm matrix, while algorithm-specific
behavior remains covered by separate targeted cases.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-STREAM-INDEXER-001 | REQ-STREAM-INDEXER-002 |
| DSG-STREAM-INDEXER-002 | REQ-STREAM-INDEXER-003 |
| DSG-STREAM-INDEXER-003..004 | REQ-STREAM-INDEXER-001, REQ-STREAM-INDEXER-004, REQ-STREAM-INDEXER-005, REQ-STREAM-INDEXER-006, REQ-STREAM-INDEXER-007 |
| DSG-STREAM-INDEXER-005 | REQ-STREAM-INDEXER-008, REQ-STREAM-INDEXER-009, REQ-STREAM-INDEXER-010, REQ-STREAM-INDEXER-012, REQ-STREAM-INDEXER-015 |
| DSG-STREAM-INDEXER-006 | REQ-STREAM-INDEXER-011, REQ-STREAM-INDEXER-013, REQ-STREAM-INDEXER-014, REQ-STREAM-INDEXER-015, REQ-STREAM-INDEXER-031, REQ-STREAM-INDEXER-032 |
| DSG-STREAM-INDEXER-007..009 | REQ-STREAM-INDEXER-016, REQ-STREAM-INDEXER-017 |
| DSG-STREAM-INDEXER-010..012 | REQ-STREAM-INDEXER-004, REQ-STREAM-INDEXER-018, REQ-STREAM-INDEXER-019, REQ-STREAM-INDEXER-021, REQ-STREAM-INDEXER-024 |
| DSG-STREAM-INDEXER-013..015 | REQ-STREAM-INDEXER-018, REQ-STREAM-INDEXER-020, REQ-STREAM-INDEXER-024, REQ-STREAM-INDEXER-025, REQ-STREAM-INDEXER-027, REQ-STREAM-INDEXER-028 |
| DSG-STREAM-INDEXER-016 | REQ-STREAM-INDEXER-013 |
| DSG-STREAM-INDEXER-017 | REQ-STREAM-INDEXER-022, REQ-STREAM-INDEXER-023 |
| DSG-STREAM-INDEXER-018 | REQ-STREAM-INDEXER-024 |
| DSG-STREAM-INDEXER-019 | REQ-STREAM-INDEXER-026 |
| DSG-STREAM-INDEXER-020 | REQ-STREAM-INDEXER-028 |
| DSG-STREAM-INDEXER-021 | REQ-STREAM-INDEXER-029, REQ-STREAM-INDEXER-030 |
| DSG-STREAM-INDEXER-022 | REQ-STREAM-INDEXER-033 |
