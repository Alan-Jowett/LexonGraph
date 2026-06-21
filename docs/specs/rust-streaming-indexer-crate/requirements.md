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
- `docs/specs/rust-adaptive-planning-policy-crate/` for one built-in adaptive
  aggregate planning realization

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
all batches in one completed planning pass.

`Partition hierarchy` means the deterministic coarse-to-fine planning tree over
the replayed logical item set that is finalized before bottom-up block
assembly.

`Terminal partition` means one partition in that hierarchy chosen as a direct
input to bottom-up parent construction over materialized leaves.

`Built-in hierarchy construction direction` means the caller-selected policy for
how the built-in planning path derives the finalized partition hierarchy. In
this revision the supported direction values are `Divisive` (top-down) and
`Agglomerative` (bottom-up).

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
- `docs/specs/rust-adaptive-planning-policy-crate/` for the adaptive aggregate
  built-in planning realization owned outside this crate

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
- hierarchical planning over replayed original-item embeddings or lower-layer
  planning units, with any clustering subproblems flowing through the shared
  streaming clustering contract
- built-in hierarchy construction direction selection governing whether the
  built-in planning path derives the finalized partition hierarchy divisively or
  agglomeratively
- terminal-partition normalization or termination policy used by bottom-up
  assembly

### REQ-STREAM-INDEXER-011

The crate shall provide built-in planning realizations for hierarchical planning
that depend on `lexongraph-dcbc-streaming`,
`lexongraph-directional-pca`, and `lexongraph-spherical-kmeans`,
whether consumed directly or through a dedicated adaptive aggregate
planning-policy crate where applicable, rather than reimplementing those
clustering algorithms locally.

Across those built-in realizations, the caller-visible built-in planning path
shall support at least one conforming `Divisive` option and at least one
conforming `Agglomerative` option.

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

The crate shall not assign an implicit built-in default planning algorithm or
built-in hierarchy construction direction.

The caller-facing built-in planning path for the streaming indexing runtime
API shall require the caller to select one supported built-in planning
realization-and-direction combination explicitly.

This requirement applies to the crate's low-level explicit built-in planning
surface. A separate higher-level convenience surface may resolve an explicitly
selected published profile version into repository-owned defaults.

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
for deriving or refining a deterministic finalized partition hierarchy.

When the built-in planning path is selected:

- `Divisive` mode shall derive or refine that hierarchy by partitioning replayed
  original-item embeddings from coarser planning units to finer ones
- `Agglomerative` mode shall derive or refine that hierarchy by grouping
  lower-layer planning units bottom-up

Both modes shall normalize into the same deterministic finalized partition
hierarchy abstraction before final materialization.

### REQ-STREAM-INDEXER-020

After planning completion and during the final materialization replay, the crate
shall construct the finished block tree by materializing leaves and assembling
parent layers bottom-up from the finalized partition hierarchy.

Any clustering used while deriving or refining that hierarchy in either
`Divisive` or `Agglomerative` mode shall continue to flow through the shared
streaming clustering contract rather than an older batch-only clustering
boundary.

### REQ-STREAM-INDEXER-021

Each completed streaming indexing pass shall return a deterministic structured
pass report that includes:

- the observed item count for that pass
- deterministic planning progress or quality information for the caller-visible
  hierarchy-building work for the selected planning direction, derived from the
  shared streaming clustering surface wherever clustering participates in that
  work
- enough structured state for the caller to decide whether to continue or stop

### REQ-STREAM-INDEXER-022

The crate shall provide an optional caller-supplied status observer contract for
streaming planning progress, final materialization progress, and bottom-up
assembly progress.

Status updates shall be emitted as structured data suitable for arbitrary
caller-owned handling and shall not require any particular sink.

For each reported phase, the observer contract shall expose:

- the total planned work units for that phase when knowable
- the completed work units observed so far for that phase
- the remaining work units for that phase when derivable from total and
  completed counts
- explicit phase-local semantics for those work units

### REQ-STREAM-INDEXER-023

When planning, final materialization, or higher-layer assembly work remains
active long enough to be non-trivial, the crate shall emit periodic in-progress
status updates rather than only terminal state.

Those periodic updates shall report the latest available phase progress counts
so a caller can distinguish forward progress within the current phase from mere
elapsed time.

### REQ-STREAM-INDEXER-024

The crate shall surface explicit failure when:

- the input pass is empty
- the overall logical item set is empty
- content resolution fails, is inaccessible, or returns content unusable for
  indexing
- embedding generation fails
- the caller omits a required built-in planning algorithm selection, built-in
  hierarchy construction direction selection, or required planning settings
- a later replay differs from the established logical item set or replay order
- the finalized partition hierarchy is invalid, overlapping, non-covering, or
  otherwise inconsistent with the replayed logical item set
- hybrid planning configuration is invalid
- adaptive planning configuration is invalid
- a selected built-in planning realization does not support the requested
  hierarchy construction direction
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
that requires callers to choose a supported built-in planning
realization-and-direction combination backed by either the built-in streaming
directional-PCA realization, the built-in streaming spherical-k-means
realization, the built-in streaming DCBC realization, or the built-in adaptive
aggregate realization without implementing a custom planning factory.

### REQ-STREAM-INDEXER-032

When a built-in planning realization is selected through the indexer API, the
crate shall require the caller to provide that algorithm's settings rather than
supplying implicit built-in planning settings.

The crate shall validate that the supplied settings are compatible with the
requested built-in hierarchy construction direction.

### REQ-STREAM-INDEXER-033

The repository verification artifacts for algorithm-agnostic built-in-path
behavior over fixtures compatible with supported built-in planning
realization-and-direction combinations' caller-supplied settings shall realize
the corresponding validation coverage as a matrix over those supported
combinations.

Algorithm-specific behavior, including adaptive switch-trigger coverage, may be
validated through separate targeted cases rather than forced into that
symmetric matrix.

### REQ-STREAM-INDEXER-034

The crate shall define an explicit deterministic planning boundary over replayed
original-item embeddings or lower-layer planning units that is distinct from
final block materialization.

This boundary shall remain expressed as a finalized partition hierarchy
regardless of whether the built-in planning path derives that hierarchy
divisively, agglomeratively, or through an adaptive aggregate realization that
preserves the selected built-in direction while switching internal algorithms.

### REQ-STREAM-INDEXER-035

The crate shall define a deterministic mapping from the finalized partition
hierarchy to a bottom-up block tree, including deterministic normalization or
explicit failure for singleton, undersized, or oversized terminal partitions.

Built-in `Divisive` and `Agglomerative` planning modes shall both normalize into
that finalized partition hierarchy before this mapping is applied.

Any adaptive built-in planning realization shall also normalize its pre-switch
directional-PCA output and post-switch DCBC output into that same finalized
partition hierarchy before final assembly.

### REQ-STREAM-INDEXER-036

The crate shall support hybrid coarse/fine algorithm selection and require
explicit caller-visible configuration for the phase boundary, any phase-local
hierarchy construction direction policy, and algorithm-specific settings.

This caller-configured hybrid coarse/fine capability is distinct from any
adaptive built-in realization whose PCA-to-DCBC switch decisions are made
internally from deterministic diagnostics rather than from a caller-selected
coarse/fine phase boundary.

### REQ-STREAM-INDEXER-041

The built-in planning path shall expose an explicit deterministic
hierarchy-construction-direction policy whose supported values in this revision
are `Divisive` and `Agglomerative`.

### REQ-STREAM-INDEXER-042

If a caller omits the required built-in hierarchy construction direction or
selects a realization/settings combination that does not support the requested
direction, the crate shall fail explicitly rather than silently substituting a
different direction.

### REQ-STREAM-INDEXER-043

This revision shall retain a conforming built-in `Divisive` planning path after
adding built-in `Agglomerative` support.

### REQ-STREAM-INDEXER-044

The built-in planning path shall support an adaptive aggregate realization,
backed by the dedicated adaptive planning-policy crate, that begins planning
with directional PCA and may switch internally to streaming DCBC without
introducing a caller-interactive per-layer planning protocol.

### REQ-STREAM-INDEXER-045

The adaptive aggregate built-in realization shall support both `Divisive` and
`Agglomerative` hierarchy-construction directions.

Across any internal algorithm switch, the selected built-in direction shall
remain unchanged.

### REQ-STREAM-INDEXER-046

The adaptive aggregate built-in realization shall derive its PCA-to-DCBC switch
decisions from explicit deterministic diagnostics and configured thresholds.

Given the same logical item set, replay order, planning settings, and
deterministic dependency behavior, the same switch boundary shall be selected.

### REQ-STREAM-INDEXER-047

Within one adaptive planning flow, once the built-in realization switches from
directional PCA to DCBC, it shall not switch back to directional PCA later in
that same flow.

### REQ-STREAM-INDEXER-048

The indexer shall expose a reusable parent-summary policy surface that can
derive one branch-entry embedding from carried-forward child summaries during
final materialization without requiring a caller to reimplement the rest of the
indexer lifecycle.

That surface shall make available, for each child summary input, both the
normalized child embedding and the descendant-count weight represented by that
child.

### REQ-STREAM-INDEXER-049

The crate shall provide a conforming built-in exact-centroid child-summary
policy that computes each parent branch-entry embedding as the descendant-count-
weighted centroid of its carried-forward child summaries.

Given the same finalized hierarchy and child summaries, that exact-centroid
policy shall produce the same parent summary embeddings deterministically.

### REQ-STREAM-INDEXER-050

Existing canonical-embedding policies remain supported after the child-summary
policy surface is added.

The crate may adapt a canonical-embedding policy into the new child-summary
surface when the caller does not need descendant-count-aware summary semantics.

### REQ-STREAM-INDEXER-037

Independent subpartitions may be processed concurrently only if partition
identity, pass reports, root block ID, and persisted block set remain
deterministic and schedule-independent.

### REQ-STREAM-INDEXER-038

Terminal planning units shall be reconciled against a deterministic
materializability bound derived from the block size target and
`embedding_spec` before or during final assembly, or fail explicitly before
claiming a conformant result.

### REQ-STREAM-INDEXER-039

For each caller-visible status phase, the crate shall define explicit semantics
for the progress-count fields exposed to the status observer, including:

- the work unit represented by the phase counts
- whether the reported total is the planned work for that phase
- what event advances the completed count for that phase
- how the remaining count is derived when the total is known
- whether any quantity may be unavailable because it is not yet knowable

These semantics shall be phase-specific for at least:

- `PlanningPass`
- `HierarchyPlanning`
- `FinalMaterializationReplay`
- `BottomUpAssembly`

If a quantity is unavailable for a phase at a given moment, the observer
contract shall represent that explicitly rather than overloading another count
with ambiguous meaning.

### REQ-STREAM-INDEXER-040

For `BottomUpAssembly { layer_index }` status phases, `layer_index` shall name
the semantic bottom-up layer being materialized, measured from leaf level 0 in
the block protocol, rather than the temporal count of recursive or sequential
assembly operations.

Independent subtree or sibling assemblies that materialize the same semantic
parent layer shall therefore report the same `layer_index`.

### REQ-STREAM-INDEXER-051

The crate shall expose a higher-level convenience indexing surface whose shape
remains stable across published profile revisions.

That surface shall accept an explicit published semantic-version profile
selector rather than requiring callers to choose planning, packing, and summary
knobs individually.

### REQ-STREAM-INDEXER-052

The convenience indexing surface shall fail explicitly for unknown or
unsupported published profile versions.

It shall not silently substitute the latest, nearest, or repository-current
profile.

### REQ-STREAM-INDEXER-053

A published indexing profile version shall map to one deterministic bundle of
crate-owned indexing behavior for its lifetime.

Repeated use of the same published profile version under the same logical item
set, replay order, and deterministic dependency behavior shall preserve the
same effective planning, packing, and summary behavior.

### REQ-STREAM-INDEXER-054

Later patch versions in the same published pre-1.0 profile line may refine
constants or thresholds while preserving the same algorithm family choices as
the earlier patch versions in that line.

### REQ-STREAM-INDEXER-055

Later minor versions in the published pre-1.0 profile line may adopt different
algorithm families when the repository publishes a new recommended profile.

### REQ-STREAM-INDEXER-056

The repository shall publish indexing profile `0.1.0`.

For the crate-owned runtime knobs currently owned by this crate, that published
profile shall resolve to:

- leaf formation via `spherical-kmeans`
- packing via `cluster-order-balanced-range-packer-v1`
- hierarchy construction via `greedy-pack` using Euclidean centroid distance
- child summary via `exact-centroid`

The published `0.1.0` profile shall also pin the crate-owned spherical-k-means
configuration values needed by that bundle, including its initialization policy,
iteration limit, convergence tolerance, requested cluster count, and random
seed.

In this revision, those pinned spherical-k-means values for published profile
`0.1.0` are:

- initialization policy = `SeededDeterministicFarthestPoint`
- max iterations = `32`
- convergence tolerance = `1e-4`
- requested cluster count = `157`
- random seed = `11`

### REQ-STREAM-INDEXER-057

The existing low-level explicit indexing surface shall remain available for
callers that want direct control over planning realization, direction, settings,
or summary policy instead of selecting a published profile.

### REQ-STREAM-INDEXER-058

The repository shall publish indexing profile `0.2.0` alongside `0.1.0`.

Both published profile versions shall remain explicitly resolvable through the
stable convenience profile selector in this revision.

### REQ-STREAM-INDEXER-059

For the crate-owned runtime knobs currently owned by this crate, published
indexing profile `0.2.0` shall resolve to a deterministic directional-PCA
bundle that:

- uses the built-in directional-PCA planning realization
- selects `Divisive` hierarchy construction
- preserves the existing finalized partition hierarchy abstraction
- preserves the existing bottom-up final block materialization flow
- preserves the `exact-centroid` child-summary policy

This published profile introduces an alternate repository-recommended planning
algorithm family without mutating the behavior of published profile `0.1.0`.

### REQ-STREAM-INDEXER-060

Published indexing profile `0.2.0` shall pin the crate-owned
directional-PCA settings used by that bundle.

In this revision, those pinned directional-PCA values for published profile
`0.2.0` are:

- requested cluster count = `2`
- random seed = `7`
- retained dimension count = `1`
- variance exponent = `1.0`
- temperature = `1.0`
- minimum input count = `2`
- minimum effective rank = `1`
- minimum cumulative variance = `0.0`

## Out of Scope

This crate does not define or own:

- block wire encoding or block validity rules
- block-ID derivation rules
- storage backend implementations
- the shared embedding-provider trait contract
- the shared streaming clustering trait definitions
- legacy batch-oriented implementation lines or their repository lifecycle
- any concrete clustering algorithm beyond the built-in directional-PCA,
  spherical-k-means, and DCBC planning options exposed by this crate or the
  adaptive aggregate option that composes directional PCA with DCBC

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/indexing.md` and
`docs/protocol/blocks.md`.

This document is also subordinate to the block crate, block-storage trait,
embeddings-trait, streaming clustering, streaming DCBC, directional-PCA,
spherical-k-means, and adaptive planning-policy specification packages for
their owned concerns.
