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
public-API or implementation-owned obligation.

`Item replay order` means the ordered sequence of indexing items observed across
all batches in one completed planning pass.

`Partition hierarchy` means the deterministic coarse-to-fine planning tree over
the replayed logical item set that is finalized before bottom-up block
assembly.

`Replay-driven resident-memory-bounded` means the public API advances through
caller-visible replay while implementation-owned resident memory does not scale
with the size of the full logical dataset. Planner-managed out-of-core state
may scale with the dataset when the public contract explicitly requires it.

`v1 compatibility surface` means the existing caller-facing API retained for
incremental migration and backward compatibility during the bring-up of a new
streaming surface.

`v2 streaming surface` means the new authoritative caller-facing API in this
crate that owns replay-driven, resident-memory-bounded conformance for the
memory-reduced indexing path.

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

### REQ-STREAM-INDEXER-003A

The crate may expose retained versioned indexing surfaces side-by-side within
`crates/lexongraph-streaming-indexer`.

When multiple surfaces are present:

- the v1 surface may remain available for migration compatibility
- the v2 surface is the authoritative conformance target for the replay-driven,
  resident-memory-bounded indexing path it defines
- a v3 surface may coexist additively for partition-working-store-based
  execution
- validation and documentation shall distinguish those surfaces explicitly
  rather than silently treating one surface's behavior as another's

### REQ-STREAM-INDEXER-004

The crate shall define a caller-visible streaming indexing API whose lifecycle
includes:

- starting a streaming indexing run for one indexing context
- ingesting one or more batches of indexing items for the current planning pass
- completing the current planning pass and obtaining a deterministic pass report
- caller-directed planning continuation or completion
- final materialization through a final replay of the logical item set in
  established replay order, where the crate may classify items into
  implementation-owned temporary per-terminal-partition spill files before
  assembling the finished block tree bottom-up from the finalized partition
  hierarchy

### REQ-STREAM-INDEXER-004A

The v2 streaming surface shall require the caller to provide a writable
directory root for planner-managed out-of-core state.

That planner-state root is part of the v2 run-construction contract. The
implementation manages concrete file names, mmap layout, and lifecycle beneath
that root rather than requiring the caller to provide individual state-file
paths.

When the selected directional-PCA quantile realization uses deterministic
Greenwald-Khanna summaries, that path shall not require per-axis quantile spill
capture or replay beneath the planner-state root.

### REQ-STREAM-INDEXER-004B

The constrained v3 surface shall require a caller-supplied writable temporary
working root for implementation-owned partition artifacts.

That working root is separate from the production block store used to resolve
input leaf blocks and persist the final result.

### REQ-STREAM-INDEXER-004C

The first v3 slice shall be single-process only and shall not provide crash
recovery or resume semantics.

An interrupted v3 run may require restart from the beginning.

### REQ-STREAM-INDEXER-004D

On successful completion, the constrained v3 surface shall clean up its
temporary working subtree.

Those intermediate artifacts are implementation-owned and are not part of the
durable production result contract.

### REQ-STREAM-INDEXER-005

The caller-visible streaming indexing API shall accept a non-empty ordered
stream of indexing items partitioned into caller-chosen batches for each pass.

### REQ-STREAM-INDEXER-005A

The constrained v3 surface shall accept a non-empty ordered stream of existing
production leaf block IDs as its public input boundary.

That v3 boundary shall not require the caller to resend content references,
raw content bytes, or inline leaf payloads for those inputs.

### REQ-STREAM-INDEXER-005B

After a v3 partition has become terminal for its current layer, later
refinement rounds for sibling or unrelated partitions shall not require
rereading that terminal partition's full membership.

The implementation may reread terminal partition contents only when needed for
deterministic next-layer assembly.

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
- v3 partition-working-store orchestration over ordered leaf-block or
  lower-layer child membership without widening the production block-store
  contract with mutable scheduler semantics
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

The retained low-level streaming surface shall continue to provide explicit API
paths that accept caller-supplied canonical-embedding, hierarchical planning,
and streaming clustering policy implementations.

The published-profile-only v2 streaming surface introduced in this revision is
not yet required to expose corresponding override entry points. Until those v2
override paths exist, attempts to request override behavior through v2 shall
fail explicitly rather than silently delegating to the retained v1
compatibility surface.

Those override seams shall remain replay-driven and resident-memory-bounded.
This revision shall not treat full-dataset embedding slices, full partition-
membership tables, or equivalent dataset-sized override inputs or outputs as
conformant public API shapes.

The same restriction applies to implementation-owned planning boundaries used to
realize those seams: a conformant path shall not require a full-pass decoded
embedding table, full-pass assignment vector, or equivalent replay-sized
materialization to cross from replay ingestion into planning.

### REQ-STREAM-INDEXER-016

The first completed streaming indexing pass shall establish the logical item set
and item replay order for the run.

Each later completed pass and the final materialization replay shall represent
that same logical item set in the same item replay order or fail explicitly.

If the crate needs to revisit prior data, that revisit shall occur only through
caller-visible replay or caller-visible staged progression rather than hidden
implementation-owned retention or spill of the full logical dataset.

### REQ-STREAM-INDEXER-016A

For v3, partition identity, child ordinal assignment, ordered membership, and
parent-assembly ordering shall be derived deterministically from the indexing
context and partition state rather than from temp-path naming, completion
timing, or execution schedule.

### REQ-STREAM-INDEXER-017

The v2 streaming surface shall remain dataset-size independent by requiring
caller replay for repeated passes and final materialization rather than
requiring the crate's v2 default API surface to retain or rematerialize the
full logical dataset on the caller's behalf.

A replay-shaped public API is not sufficient by itself: conformant public input,
output, and extension-point shapes shall also avoid constructs whose memory
footprint scales `O(full logical dataset size)`.

This restriction does not forbid the v2 implementation from retaining
dataset-sized planner-managed out-of-core state beneath the caller-provided
planner-state root, provided the caller-visible API still uses replay rather
than full-dataset payloads as the progression contract.

### REQ-STREAM-INDEXER-017A

The constrained v3 surface shall keep hot memory bounded to the currently
active partition work set plus bounded pipeline buffers, rather than retaining
planner-hot state for all discovered partitions simultaneously.

Transient pipeline buffers may support overlapped storage and CPU work, but
they shall not grow with the total discovered partition count or become a
hidden full-run resident materialization.

Within one active v3 partition, prepared-but-not-yet-committed future batches
may lead the oldest uncommitted processing batch by at most three batches.

### REQ-STREAM-INDEXER-018

The core streaming indexer shall own the protocol-required orchestration, leaf
construction, normalization, block construction, higher-layer construction, and
block persistence flow.

Implementation-defined policies may propose clustering behavior, but the core
indexer shall remain authoritative for protocol conformance checks.

### REQ-STREAM-INDEXER-019

For the caller-visible replay passes over original indexing items on the v2
streaming surface, the crate shall use the shared streaming clustering contract
as the clustering boundary for deriving or refining a deterministic finalized
partition hierarchy.

When the built-in planning path is selected:

- `Divisive` mode shall derive or refine that hierarchy by partitioning replayed
  original-item embeddings from coarser planning units to finer ones
- `Agglomerative` mode shall derive or refine that hierarchy by grouping
  lower-layer planning units bottom-up

Both modes shall normalize into the same deterministic finalized partition
hierarchy abstraction before final materialization.

Any planning orchestration around the shared streaming clustering contract shall
remain replay-driven at the caller-visible boundary and shall not depend on
retained full-dataset in-memory embedding tables, replay-verification tables,
or partition-membership tables.

Retained implementation-owned planning metadata for the v2 surface shall also
avoid unnecessary per-partition heap overhead when the partition set is
repository-generated and deterministically ordered, including avoidable
string-keyed lookup tables or duplicated string ancestry state where
equivalent compact internal identifiers and contiguous storage suffice.

In particular, a conformant planning path shall not depend on carrying one
implementation-owned decoded embedding table for the entire planning pass in
resident memory from `ingest_batch` through `finish_pass`, even when that table
is later consumed only inside built-in or override planning logic.

### REQ-STREAM-INDEXER-020

After planning completion and during the final materialization replay, the crate
shall construct the finished block tree by materializing leaves and assembling
parent layers bottom-up from the finalized partition hierarchy.

Any clustering used while deriving or refining that hierarchy in either
`Divisive` or `Agglomerative` mode shall continue to flow through the shared
streaming clustering contract rather than an older batch-only clustering
boundary.

The final materialization replay may use implementation-owned temporary local
append-only spill scoped to terminal partitions, provided that:

- the spill is populated only after planning completion
- the spill is written in replay order during the final materialization replay
- the spill is read back by the crate to materialize terminal partitions and
  then assemble parents bottom-up
- the crate remains authoritative for leaf construction, block persistence,
  parent assembly, final result packaging, and spill cleanup

### REQ-STREAM-INDEXER-020A

The constrained v3 surface shall persist only the final result blocks required
for the completed index into the production output store.

Intermediate partition manifests, membership artifacts, split staging, and
similar v3 working artifacts remain outside the production-store contract.

### REQ-STREAM-INDEXER-020B

For v3, partition terminality and parent-assembly correctness shall remain
derived from the deterministic materializability bound implied by
`embedding_spec` and `block_size_target`.

Implementation-owned constants such as `64` may be used for batching,
parallelism, or memory heuristics, but shall not redefine the correctness
contract for whether a partition is terminal.

### REQ-STREAM-INDEXER-021

Each completed streaming indexing pass shall return a deterministic structured
pass report that includes:

- the observed item count for that pass
- the requested and realized planning cluster counts when clustering
  participates in the reported planning work
- deterministic planning progress or quality information for the caller-visible
  hierarchy-building work for the selected planning direction, derived from the
  shared streaming clustering surface wherever clustering participates in that
  work
- enough structured state for the caller to decide whether to continue or stop

If exact planning cannot yet expose a final partition-ready hierarchy under the
bounded-state rules of this revision, the report shall expose deterministic
readiness or progress semantics rather than claiming final partition readiness
early.

When the reported planning work consumes the shared streaming clustering
contract, a successful completed clustering pass that remains
`PassReadiness::AnalysisOnly` is a conformant unresolved outcome rather than a
failure by itself.

The consuming surface may continue deterministic replay or planning from that
state and shall not convert it into a terminal error unless a separate bounded
termination rule or other explicit failure condition is reached.

### REQ-STREAM-INDEXER-021A

The v2 streaming surface shall be a replay-driven, resident-memory-bounded
realization.

It may retain planning-time implementation-owned state whose on-disk footprint
scales with the full logical dataset, but that state shall live beneath the
caller-provided planner-state root rather than as replay-sized resident-memory
materialization.

Planner-owned resident pages for that state shall remain bounded independently
of the full logical dataset size rather than growing with the mapped file size.

The retained in-memory hierarchy state that remains necessary for v2 planning
may scale with discovered partition count, but shall use a compact
representation that does not make externally formatted partition identifiers or
string-keyed indexing structures the primary retained identity on the hot
in-memory path.
### REQ-STREAM-INDEXER-021B

The v2 streaming surface shall not expose public planning, finalization, or
hierarchy surfaces whose required inputs or returned state scale with the full
logical dataset, including full embedding slices, full partition-membership
vectors, or equivalent dataset-sized API constructs.

### REQ-STREAM-INDEXER-021C

Transient working memory may scale with the currently ingested batch or the
currently processed work unit, provided that such growth does not scale with the
full logical dataset size.

Such transient working memory shall not span the full planning pass merely to
bridge replay ingestion and later planner execution.

Planner-managed mmap-backed or equivalent out-of-core state may bridge replay
ingestion and later planner execution, provided that the resident-memory working
set remains bounded and the caller-visible replay lifecycle is unchanged.

That bound shall be enforced by active residency management for inactive mapped
regions, through a cross-platform abstraction that is realizable on each
supported OS target rather than relying solely on ambient kernel eviction
heuristics.

### REQ-STREAM-INDEXER-021D

A caller-visible v2 replay lifecycle backed by hidden implementation-owned
full-dataset planning-time buffering or retained state that substitutes for
caller-visible replay is non-conformant.

This includes hidden buffering or spill of decoded embeddings, planner-ready
assignment state, or equivalent full-pass planning materializations retained so
that later planning work can proceed without additional caller-visible replay.

Implementation-owned temporary local spill used only after planning completion
to stage terminal-partition materialization inputs is conformant in this
revision. During planning, planner-managed mmap-backed or equivalent out-of-core
state is conformant only when it is part of the documented v2 contract,
subordinate to caller-visible replay, and not a hidden replacement for it.

### REQ-STREAM-INDEXER-021E

The v2 conformant planning boundary shall realize hierarchy derivation through
caller-visible replay stages together with bounded in-memory summaries,
bounded per-subproblem working sets, or planner-managed out-of-core state under
the caller-provided planner-state root.

If an exact planning realization cannot expose a final partition-ready
hierarchy under those constraints, it shall surface deterministic readiness or
progress state rather than silently retaining a full-pass decoded embedding
table, full-pass assignment vector, or equivalent implementation-owned replay
materialization.

### REQ-STREAM-INDEXER-021G

If the v2 planning path uses planner-managed out-of-core state beneath the
caller-provided planner-state root, that state shall be:

- deterministic for identical replay input and configuration
- rooted beneath the caller-provided directory
- writable and reopenable across repeated passes of the same run
- actively residency-managed so planner-owned resident pages stay within a
  documented bound independent of total mapped file size, through a
  cross-platform residency-management abstraction
- behavior-preserving with respect to replay validation, pass-report semantics,
  and finalized partition-hierarchy semantics
- subordinate to the caller-visible replay lifecycle rather than a hidden
  substitute for it

### REQ-STREAM-INDEXER-021H

Construction of a v2 run shall fail explicitly when the caller omits the
planner-state root or when the supplied root is unusable for the planner's
required out-of-core state model.
### REQ-STREAM-INDEXER-021F

The crate shall support incremental migration from the v1 compatibility surface
to the v2 streaming surface.

During that migration:

- any planning mode, direction, profile, or override path not yet implemented
  on v2 shall fail explicitly when requested on v2
- the crate shall not silently fall back from v2 to v1 buffering behavior
- conformance claims for replay-driven, resident-memory-bounded behavior apply
  only to the v2 surface and the explicitly validated v2 feature subset

### REQ-STREAM-INDEXER-022

The crate shall provide an optional caller-supplied status observer contract for
streaming planning progress, final materialization progress, and bottom-up
assembly progress.

Status updates shall be emitted as structured data suitable for arbitrary
caller-owned handling and shall not require any particular sink.

The same optional observer contract shall be available on the caller-visible
v2 / published-profile `0.7.0` execution surface and on the constrained v3
surface. Those surfaces may reuse the existing structured status payload with
additive optional detail rather than requiring separate log-only telemetry
channels.

For each reported phase, the observer contract shall expose:

- the total planned work units for that phase when knowable
- the completed work units observed so far for that phase
- the remaining work units for that phase when derivable from total and
  completed counts
- explicit phase-local semantics for those work units

For recursive or otherwise dynamically discovered hierarchy-planning work, the
observer contract shall additionally expose structured phase-local progress
detail sufficient for a downstream caller to render meaningful progress
summaries and suspected-stall diagnostics without inferring state from logs or
host-resource sampling.

That detail shall include, when relevant and knowable without materially
per-item instrumentation:

- the kind of work unit represented by the phase-local progress counts
- the current planning unit's deterministic partition identity or path when
  available
- the current planning unit's logical input size
- the current planning unit's recursion depth when recursive planning is active
- the elapsed time spent in the current planning unit
- the number of planning units discovered so far when the eventual total is not
  yet known
- aggregate counters such as visited partitions, finalized partitions, terminal
  partitions produced, completed planner invocations, and deterministic
  fallback or regrouping events
- for v3, enough structured phase or stage detail to distinguish partition
  trainer ingest, partition classification into child files, partition
  planning, terminal materialization load, next-layer assembly, and final
  persistence when those activities are separately observable

### REQ-STREAM-INDEXER-023

When planning, final materialization, or higher-layer assembly work remains
active long enough to be non-trivial, the crate shall emit periodic in-progress
status updates rather than only terminal state.

Those periodic updates shall report the latest available phase progress counts
so a caller can distinguish forward progress within the current phase from mere
elapsed time.

For long-running recursive planning, those periodic updates shall also report
the latest available current-unit detail and discovered-unit counters so a
caller can distinguish active work within one expensive planning unit from a
run whose observable state is not advancing at all.

For v2 planning, those periodic updates shall cover both pass-wide progress and
planning-unit progress when both are available from retained state, so a
downstream caller can distinguish replay advancement from pending-partition
trainer work.

For v2 planning, the observer-visible telemetry across repeated periodic
updates and completed pass boundaries shall expose enough deterministic visible
state for a caller to distinguish advancing replay, advancing planner state,
unchanged planner state, and change that appears only when one pass is compared
to the previous completed pass.

For the constrained v3 surface, long-running partition-trainer ingest,
partition-classification, terminal-materialization load, partition-planning,
and assembly work shall likewise emit periodic in-progress updates carrying
the latest observable progress counts rather than only elapsed time or
terminal completion.

The constrained v3 runtime shall emit the more specific phase identities when
those activities are separately observable.

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

For the constrained v3 surface:

- non-trivial independent CPU-bound work shall default to rayon-backed
  parallel execution when ordering is not needed to preserve determinism
- production-store and temp-working-store disk I/O shall use a bounded
  cross-platform concurrency realization that keeps multiple storage loads
  effectively in flight when work exists, including when an underlying backend
  realizes reads through blocking filesystem calls
- an async-looking orchestration layer by itself is not sufficient for
  conformance when it funnels blocking filesystem reads through an effectively
  single-threaded producer that leaves ready CPU work or additional storage
  capacity materially idle
- the implementation shall keep both pending storage work and ready CPU work
  in flight when work exists, without changing deterministic externally visible
  results
- within one active partition, batch preparation may overlap with processing of
  earlier batches, but the prepare-ahead window shall be bounded to at most
  three future batches
- within one active partition, trainer-visible state updates, order-sensitive
  floating-point reductions, and deterministic child-partition emission shall
  commit in deterministic batch order even when later batches have already been
  prepared

### REQ-STREAM-INDEXER-037A

The constrained v3 surface shall provide at least one portable load-path
realization that preserves the existing public `BlockStore` contract while
maintaining effective concurrency for bounded numbers of independent block-load
operations.

That realization may use dedicated blocking I/O workers, runtime-managed
blocking workers, or an equivalent bounded internal mechanism, but it shall not
require callers or downstream block-store implementations to adopt a new
platform-specific API solely to obtain conforming load concurrency.

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
- how a recursive phase reports discovered work before an eventual total is
  fully known
- what structured fields identify the current planning unit and its elapsed time
  when counts alone are insufficient to disambiguate active progress from a
  suspected stall
- for v2 planning, what fields summarize pending partitions, per-partition
  replay advancement, coarse trainer subphase, and any explicit
  suspected-stall indicator
- for v2 planning, what fields summarize completed-pass boundary deltas or
  fingerprints so a caller can compare pass `N` with earlier completed passes
  without inferring semantics from logs
- for v3, what phases or stage detail distinguish partition-trainer ingest,
  partition classification into child files, terminal materialization load,
  partition planning, next-layer assembly, and final persistence
- for v3, what counts and elapsed-time fields let a downstream caller derive an
  honest throughput or progress-rate estimate for training-ingest,
  classification, materialization-load, or assembly work without fabricating a
  completion percentage when totals are not yet knowable
- for v3, how prepared-but-not-yet-committed batches are distinguished from
  batches whose processing effects have already been committed
- for v3, when storage preparation is separately observable, what additive
  detail distinguishes in-flight or prepared load backlog from committed
  processing progress

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

The repository shall publish indexing profile `0.2.0` and indexing profile
`0.3.0` alongside `0.1.0`.

All published profile versions shall remain explicitly resolvable through the
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

### REQ-STREAM-INDEXER-061

For the crate-owned runtime knobs currently owned by this crate, published
indexing profile `0.3.0` shall resolve to a deterministic directional-PCA
bundle that:

- uses the built-in directional-PCA planning realization
- selects `Divisive` hierarchy construction
- preserves the existing finalized partition hierarchy abstraction
- preserves the existing bottom-up final block materialization flow
- preserves the `exact-centroid` child-summary policy

This published profile introduces a new versioned directional-PCA contract
without mutating the behavior of published profiles `0.1.0` or `0.2.0`.

### REQ-STREAM-INDEXER-062

Published indexing profile `0.3.0` shall pin the crate-owned
directional-PCA settings used by that bundle.

In this revision, those pinned directional-PCA values for published profile
`0.3.0` are:

- requested cluster count = `64`
- random seed = `7`
- retained-axis policy = `AdaptiveAllEligible`
- allocation policy = `EigenvalueLogBits`
- binning policy = `DensityValley`
- cluster-cardinality mode = `UnderfullSuccess`
- variance exponent = `1.0`
- temperature = `1.0`
- minimum input count = `2`
- minimum effective rank = `1`
- minimum cumulative variance = `0.0`

### REQ-STREAM-INDEXER-063

Published indexing profile `0.3.0` shall select the adaptive retained-axis,
eigenvalue-log-bit allocation, density-valley, and underfull-success
directional-PCA policies without mutating the lower-level explicit
directional-PCA default path or the published behavior of `0.2.0`.

### REQ-STREAM-INDEXER-064

For recursive or divisive hierarchy-planning flows, the crate shall emit
structured planning-unit lifecycle updates at meaningful unit boundaries before
the enclosing planning pass completes.

In this revision, a planning unit means one deterministic
partition-planning invocation together with any explicit fallback or regrouping
work required to produce that partition's child split or terminal decision.

`completed_unit_count` for that planning-unit family advances when one such
unit completes successfully or fails explicitly, and the reported current-unit
descriptor changes when work moves to a different partition.

The crate may rate-limit repeated in-progress updates for the same unit, but it
shall not regress to per-pass heartbeats that leave long-running recursive
planning indistinguishable from a stuck computation.

For the v2 / published-profile `0.7.0` divisive path, the crate shall emit
`PlanningPass` and `HierarchyPlanning` updates together when that combination is
needed to represent both observed pass advancement and pending
partition-planning activity without collapsing one into the other.

When a v2 planning run remains unresolved across multiple completed passes, the
observer-visible status and pass-boundary telemetry together shall preserve
enough deterministic partition-local state to attribute repeated
non-advancement to the same unresolved partition or planner subphase whenever
that attribution is knowable from retained state.

### REQ-STREAM-INDEXER-065

The repository shall publish an experimental `0.3.x` directional-PCA profile
ladder alongside `0.3.0`.

Each experimental profile shall remain explicitly resolvable through the stable
published-profile selector and shall not mutate the declared behavior of
`0.1.0`, `0.2.0`, or `0.3.0`.

### REQ-STREAM-INDEXER-066

The experimental `0.3.x` ladder shall be curated for attribution rather than as
an exhaustive combinatorial sweep.

Each experimental profile shall differ from `0.3.0` in one named primary
directional-PCA variable unless a secondary mechanical adjustment is required to
realize that selected policy combination.

### REQ-STREAM-INDEXER-067

Published indexing profile `0.3.0` shall remain the baseline directional-PCA
contract for the experimental `0.3.x` ladder.

### REQ-STREAM-INDEXER-068

Each experimental `0.3.x` published profile shall carry a documented evaluation
hypothesis and remain compatible with the existing quality-report workflow so
the profiles can be run sequentially and compared.

### REQ-STREAM-INDEXER-069

Published indexing profile `0.3.1` shall pin the `0.3.0` directional-PCA bundle
except that it increases requested cluster count to `128`.

### REQ-STREAM-INDEXER-070

Published indexing profile `0.3.2` shall pin the `0.3.0` directional-PCA bundle
except that it decreases requested cluster count to `32`.

### REQ-STREAM-INDEXER-071

Published indexing profile `0.3.3` shall pin the `0.3.0` directional-PCA bundle
except that it selects quantile binning in place of density-valley binning.

### REQ-STREAM-INDEXER-072

Published indexing profile `0.3.4` shall pin the `0.3.0` directional-PCA bundle
except that it reverts to fixed PC1-only splitting through:

- retained-axis policy = `FixedCount(1)`
- allocation policy = `CentroidWeightedBins`
- binning policy = `Quantile`

### REQ-STREAM-INDEXER-073

Published indexing profile `0.3.5` shall pin the `0.3.0` directional-PCA bundle
except that it selects centroid-weighted allocation in place of eigenvalue
log-bit allocation.

### REQ-STREAM-INDEXER-074

Published indexing profile `0.3.6` shall pin the `0.3.0` directional-PCA bundle
except that it caps retained-axis selection at `FixedCount(2)`.

### REQ-STREAM-INDEXER-075

Published indexing profile `0.3.7` shall pin the `0.3.0` directional-PCA bundle
except that it caps retained-axis selection at `FixedCount(3)`.

### REQ-STREAM-INDEXER-076

Published indexing profile `0.3.8` shall pin the `0.3.0` directional-PCA bundle
except that it raises minimum cumulative variance to `0.5`.

### REQ-STREAM-INDEXER-077

Published indexing profile `0.3.9` shall pin the `0.3.0` directional-PCA bundle
except that it raises minimum effective rank to `2`.

### REQ-STREAM-INDEXER-078

Published indexing profile `0.3.10` shall pin the `0.3.0` directional-PCA
bundle except that it restores exact cardinality mode in place of
underfull-success mode.

### REQ-STREAM-INDEXER-079

The repository shall publish a parallel experimental `0.4.x`
directional-PCA profile ladder alongside the existing `0.3.x` ladder.

Each `0.4.x` profile shall remain explicitly resolvable through the stable
published-profile selector, shall remain compatible with the existing
quality-report workflow, and shall not mutate the declared behavior of any
existing `0.1.x`, `0.2.x`, or `0.3.x` profile.

### REQ-STREAM-INDEXER-080

Published indexing profile `0.4.0` shall define the baseline directional-PCA
contract for the experimental `0.4.x` ladder and shall equal the `0.3.3`
directional-PCA bundle:

- requested cluster count = `64`
- retained-axis policy = `AdaptiveAllEligible`
- allocation policy = `EigenvalueLogBits`
- binning policy = `Quantile`
- cluster cardinality mode = `UnderfullSuccess`
- `min_effective_rank = 1`
- `min_cumulative_variance = 0.0`

### REQ-STREAM-INDEXER-081

Published indexing profile `0.4.1` shall pin the `0.4.0` directional-PCA
bundle except that it increases requested cluster count to `128`.

### REQ-STREAM-INDEXER-082

Published indexing profile `0.4.2` shall pin the `0.4.0` directional-PCA
bundle except that it decreases requested cluster count to `32`.

### REQ-STREAM-INDEXER-083

Published indexing profile `0.4.3` shall pin the `0.4.0` directional-PCA
bundle except that it reverts to fixed PC1-only splitting through:

- retained-axis policy = `FixedCount(1)`
- allocation policy = `CentroidWeightedBins`
- binning policy = `Quantile`

### REQ-STREAM-INDEXER-084

Published indexing profile `0.4.4` shall pin the `0.4.0` directional-PCA
bundle except that it selects centroid-weighted allocation in place of
eigenvalue log-bit allocation.

### REQ-STREAM-INDEXER-085

Published indexing profile `0.4.5` shall pin the `0.4.0` directional-PCA
bundle except that it caps retained-axis selection at `FixedCount(2)`.

### REQ-STREAM-INDEXER-086

Published indexing profile `0.4.6` shall pin the `0.4.0` directional-PCA
bundle except that it caps retained-axis selection at `FixedCount(3)`.

### REQ-STREAM-INDEXER-087

Published indexing profile `0.4.7` shall pin the `0.4.0` directional-PCA
bundle except that it raises minimum cumulative variance to `0.5`.

### REQ-STREAM-INDEXER-088

Published indexing profile `0.4.8` shall pin the `0.4.0` directional-PCA
bundle except that it raises minimum effective rank to `2`.

### REQ-STREAM-INDEXER-089

Published indexing profile `0.4.9` shall pin the `0.4.0` directional-PCA
bundle except that it restores exact cardinality mode in place of
underfull-success mode.

### REQ-STREAM-INDEXER-090

Published directional-PCA profiles in the `0.4.x` ladder shall fail explicitly
when their requested cluster count conflicts with configured non-data
constraints required by the same profile contract, including the effective
branch materializability bound implied by the selected embedding spec and block
size target.

### REQ-STREAM-INDEXER-091

When `REQ-STREAM-INDEXER-090` is violated, the published-profile execution path
shall return an explicit error that reports the requested fanout and the
conflicting configured limit rather than silently clipping the request.

### REQ-STREAM-INDEXER-092

Published directional-PCA profiles in the `0.4.x` ladder may still realize
fewer clusters when a specific partition has too few represented children to
support the requested fanout, because that reduction is emergent runtime
behavior rather than a conflict between configured requirements.

### REQ-STREAM-INDEXER-093

Published profiles `0.1.x`, `0.2.x`, and `0.3.x` shall remain behaviorally
unchanged and shall not inherit the `0.4.x` fail-fast rule for configured
fanout conflicts.

### REQ-STREAM-INDEXER-094

The repository shall publish a parallel experimental `0.5.x` compression ladder
alongside the existing published indexing profiles.

Each `0.5.x` profile shall remain explicitly resolvable through the stable
published-profile selector, shall remain compatible with the quality-report
workflow, and shall not mutate the declared behavior of existing `0.1.x`,
`0.2.x`, `0.3.x`, or `0.4.x` profiles.

### REQ-STREAM-INDEXER-095

Published indexing profile `0.5.0` shall define the baseline contract for the
experimental `0.5.x` ladder and shall preserve the same tree-construction
settings, emitted block topology, and ordinary uncompressed branch-entry
representation as published indexing profile `0.4.0`.

### REQ-STREAM-INDEXER-096

Published indexing profile `0.5.1` shall preserve the `0.5.0` topology and
logical branch centroids while authoring non-leaf branch-entry embeddings with
the EBCP encoding `pca-rot-f32le`.

### REQ-STREAM-INDEXER-097

Published indexing profile `0.5.2` shall preserve the `0.5.0` topology and
logical branch centroids while authoring non-leaf branch-entry embeddings with
the EBCP encoding `pca-rot-delta-f32le`.

### REQ-STREAM-INDEXER-098

Published indexing profile `0.5.3` shall preserve the `0.5.0` topology while
authoring non-leaf branch-entry embeddings with the EBCP encoding
`pca-rot-delta-uq`.

For the `0.5.3` ladder rung, the uniform per-dimension quantization budget
shall be:

- `12` bits on the root non-leaf level
- `8` bits on interior non-leaf levels above the lowest routing layer
- `6` bits on the lowest routing non-leaf level whose children are leaf blocks

### REQ-STREAM-INDEXER-099

Published indexing profile `0.5.4` shall preserve the `0.5.0` topology while
authoring non-leaf branch-entry embeddings with the EBCP encoding
`pca-rot-delta-vbq`.

For the `0.5.4` ladder rung, each non-leaf block shall use the same total
per-level bit budget that `0.5.3` would have used at that level and
dimensionality, redistributed across dimensions according to variance.

### REQ-STREAM-INDEXER-100

Published indexing profile `0.5.5` shall preserve the `0.5.0` topology while
authoring non-leaf branch-entry embeddings with the EBCP encoding
`ambient-delta-uq`.

For the `0.5.5` ladder rung, the uniform per-dimension quantization budget
shall be:

- `12` bits on the root non-leaf level
- `8` bits on interior non-leaf levels above the lowest routing layer
- `6` bits on the lowest routing non-leaf level whose children are leaf blocks

### REQ-STREAM-INDEXER-101

The `0.5.x` ladder applies only to stored non-leaf branch-entry embedding
representations after tree construction.

It shall not:

- alter leaf-block payload encodings
- change the pre-compression partition hierarchy relative to `0.5.0`
- require out-of-band search-side state to interpret authored blocks

### REQ-STREAM-INDEXER-102

When a `0.5.x` profile emits an EBCP-encoded non-leaf block, the emitted block
shall conform to both `docs/protocol/blocks.md` and `docs/protocol/ebcp.md`,
including the required `ext` metadata for the selected EBCP encoding.

### REQ-STREAM-INDEXER-103

The repository shall publish a parallel experimental `0.6.x` compression ladder
alongside the existing published indexing profiles.

Each `0.6.x` profile shall remain explicitly resolvable through the stable
published-profile selector, shall remain compatible with the quality-report
workflow, and shall not mutate the declared behavior of existing `0.1.x`,
`0.2.x`, `0.3.x`, `0.4.x`, or `0.5.x` profiles.

### REQ-STREAM-INDEXER-104

Published indexing profile `0.6.0` shall define the baseline contract for the
experimental `0.6.x` ladder.

It shall preserve the same directional-PCA planning parameters and ordinary
uncompressed non-leaf branch-entry representation as published indexing profile
`0.5.0`, except that the configured `cluster_count` becomes an opt-in hard
maximum child count for every emitted non-leaf block.

### REQ-STREAM-INDEXER-105

Published indexing profile `0.6.1` shall preserve the `0.6.0` fanout-capped
topology and logical branch centroids while authoring non-leaf branch-entry
embeddings with the EBCP encoding `pca-rot-f32le`.

### REQ-STREAM-INDEXER-106

Published indexing profile `0.6.2` shall preserve the `0.6.0` fanout-capped
topology and logical branch centroids while authoring non-leaf branch-entry
embeddings with the EBCP encoding `pca-rot-delta-f32le`.

### REQ-STREAM-INDEXER-107

Published indexing profile `0.6.3` shall preserve the `0.6.0` fanout-capped
topology while authoring non-leaf branch-entry embeddings with the EBCP
encoding `pca-rot-delta-uq`.

For the `0.6.3` ladder rung, the uniform per-dimension quantization budget
shall be:

- `12` bits on the root non-leaf level
- `8` bits on interior non-leaf levels above the lowest routing layer
- `6` bits on the lowest routing non-leaf level whose children are leaf blocks

### REQ-STREAM-INDEXER-108

Published indexing profile `0.6.4` shall preserve the `0.6.0` fanout-capped
topology while authoring non-leaf branch-entry embeddings with the EBCP
encoding `pca-rot-delta-vbq`.

For the `0.6.4` ladder rung, each non-leaf block shall use the same total
per-level bit budget that `0.6.3` would have used at that level and
dimensionality, redistributed across dimensions according to variance.

### REQ-STREAM-INDEXER-109

Published indexing profile `0.6.5` shall preserve the `0.6.0` fanout-capped
topology while authoring non-leaf branch-entry embeddings with the EBCP
encoding `ambient-delta-uq`.

For the `0.6.5` ladder rung, the uniform per-dimension quantization budget
shall be:

- `12` bits on the root non-leaf level
- `8` bits on interior non-leaf levels above the lowest routing layer
- `6` bits on the lowest routing non-leaf level whose children are leaf blocks

### REQ-STREAM-INDEXER-110

The `0.6.x` ladder shall apply the published directional-PCA `cluster_count` as
an opt-in hard maximum child count for every non-leaf block by continuing to
subdivide partitions until they satisfy both that cap and the configured branch
materializability limit.

This rule applies only to the selected `0.6.x` profile and shall not
retroactively change the emitted topology of any earlier published ladder.

### REQ-STREAM-INDEXER-111

The `0.6.x` ladder applies only to selected opt-in profiles and to stored
non-leaf branch-entry representations plus the associated fanout-capped
partition hierarchy.

It shall not:

- alter leaf-block payload encodings
- require out-of-band search-side state to interpret authored blocks
- mutate the declared mapping of any `0.1.x` through `0.5.x` published profile

### REQ-STREAM-INDEXER-112

When a `0.6.x` profile emits an EBCP-encoded non-leaf block, the emitted block
shall conform to both `docs/protocol/blocks.md` and `docs/protocol/ebcp.md`,
including the required `ext` metadata for the selected EBCP encoding.

### REQ-STREAM-INDEXER-113

The repository shall publish indexing profile `0.7.0` alongside the existing
published indexing profiles.

Published profile `0.7.0` shall remain explicitly resolvable through the stable
published-profile selector and shall not mutate the declared mapping of any
`0.1.x` through `0.6.x` published profile.

### REQ-STREAM-INDEXER-114

Published indexing profile `0.7.0` shall define the baseline contract for the
`0.7.x` line.

It shall preserve the `0.6.5` fanout-capped topology contract, the same
directional-PCA planning parameters, the EBCP encoding `ambient-delta-uq`, the
uniform non-leaf quantization widths `12`, `8`, and `6` bits on the root,
interior, and lowest routing non-leaf levels respectively, and the absence of
rotation metadata.

### REQ-STREAM-INDEXER-115

When published profile `0.7.0` emits an EBCP-encoded non-leaf block, the
emitted block shall conform to both `docs/protocol/blocks.md` and
`docs/protocol/ebcp.md`, including the required `ext` metadata for the selected
EBCP encoding.

Repeated resolution of published profile `0.7.0` shall remain deterministic and
independently addressable alongside the `0.6.x` ladder.

### REQ-STREAM-INDEXER-116

The crate shall provide a caller-visible API path that accepts a caller-owned
indexing configuration derived from a resolved published profile.

That path shall allow downstream callers to execute local experiments without
manually re-specifying the originating profile's planning, packing, summary, or
branch-encoding behavior.

### REQ-STREAM-INDEXER-117

For published profiles whose planning strategy includes `cluster_count`, the
caller-visible derived-profile path shall allow the caller to override
`cluster_count` while preserving the originating profile's remaining published
semantics unless the caller changes them explicitly.

This includes preserving the originating profile's branch-encoding policy,
finalized-partition abstraction, and materialization behavior.

### REQ-STREAM-INDEXER-118

Caller-local overrides derived from a resolved published profile shall not
mutate the declared mapping of the originating published profile version and
shall not silently redefine any published contract.

They shall remain caller-owned experimental configurations rather than new
implicit published profiles.

### REQ-STREAM-INDEXER-119

When the crate executes a caller-derived configuration based on a resolved
published profile, it shall enforce the same compatibility and materializability
constraints that would apply to the originating published semantics, except
where the caller's explicit override changes the checked value itself.

### REQ-STREAM-INDEXER-120

The v2 streaming implementation may represent planning partitions internally
using opaque repository-owned identifiers and compact storage layouts that are
independent of caller-visible partition-label formatting.

If the crate surfaces partition identities through topology, status, or
diagnostics, those surfaced identities shall be derived deterministically from
the internal hierarchy state at the external boundary rather than serving as
the primary retained in-memory key.

Internal identifier compaction shall not change replay validation semantics,
parent-child topology semantics, pass-report semantics, or final materialization
behavior for successful runs.

When surfaced through telemetry, per-partition replay counters, routing
bucket-fill counters, trainer subphase summaries, and suspected-stall
diagnostics shall be derived from the compact retained state at the reporting
boundary and shall not require string-keyed primary retained structures.

If the crate surfaces completed-pass convergence summaries, blocker summaries,
or pass-to-pass fingerprints, those summaries shall likewise be derived from
compact retained state or deterministic summaries thereof rather than from
string-keyed hot-path state or host-resource sampling.

### REQ-STREAM-INDEXER-121

The v2 / published-profile `0.7.0` execution surface shall expose structured
intra-pass telemetry sufficient for a downstream caller to determine whether a
planning pass is still ingesting or replaying items, which pending partitions
still require planning work, and which coarse directional-PCA trainer subphase
each active pending partition is currently executing.

When relevant and knowable without material extra per-item instrumentation, that
telemetry shall expose:

- the current pass number
- the current pass observed-item count
- the current pass total and remaining counts when the baseline item count is
  already established
- the current pending-partition count
- each pending partition's deterministic external identity
- each pending partition's expected logical item count
- each pending partition's observed replay progress or child-routing bucket-fill
  progress when available
- each pending partition's coarse directional-PCA trainer subphase
- any additional deterministic pending-partition state summary that is needed to
  distinguish changed planner state from unchanged planner state while that
  partition remains unresolved, when knowable without material extra per-item
  instrumentation

### REQ-STREAM-INDEXER-122

The v2 intra-pass telemetry shall additionally surface explicit
suspected-stall indicators derived from observer-visible retained state rather
than from host-resource sampling.

Those indicators may summarize unchanged pass-observed counts, unchanged
pending-partition replay progress, unchanged child-routing bucket-fill state, or
an unchanged trainer subphase across reported intervals, but shall not claim a
fabricated percentage-to-convergence unsupported by the retained planning state.

When the strongest reason a v2 planning run remains unresolved is knowable from
observer-visible retained state at a completed-pass boundary, the crate shall
surface that blocker evidence explicitly and identify the responsible
unresolved partition or planner subphase deterministically rather than only
reporting a generic stuck symptom.

### REQ-STREAM-INDEXER-123

Each completed v2 / published-profile `0.7.0` planning pass shall expose a
deterministic pass-boundary convergence summary sufficient for a downstream
caller to determine whether unresolved planning work is shrinking, changing
shape, repeating a prior completed-pass state, or remaining effectively
unchanged.

That summary shall be based on observer-visible retained state and may use
deterministic fingerprints or structured deltas, but shall not fabricate a
percentage-to-convergence unsupported by the retained planning state.

### REQ-STREAM-INDEXER-124

When a completed v2 planning pass cannot yet complete planning, the crate shall
expose a deterministic blocker summary naming the unresolved partitions that
still prevent completion and the strongest observer-visible blocker evidence for
each such partition when that evidence is knowable from retained state.

If the blocker is not knowable from the retained state, the summary shall
represent that uncertainty explicitly rather than guessing.

### REQ-STREAM-INDEXER-125

Each completed v2 planning pass shall expose deterministic pass-to-pass delta
evidence sufficient for a downstream caller to answer "what changed from pass
`N-1` to pass `N`?" for the unresolved planning state.

That delta evidence shall include deterministic summaries, fingerprints, or
explicit field deltas covering at least pending partitions, terminal or routed
partitions when present, and any topology or planner-visible unresolved state
whose change or non-change is required to distinguish advancement, stalling, or
cycling across passes.

### REQ-STREAM-INDEXER-126

When the v2 / published-profile `0.7.0` execution surface has initialized its
planner-state scratch root and later returns an error from `ingest_batch`,
`finish_pass`, `mark_planning_complete`, or `finalize`, it shall retain that
run-scoped scratch subtree on disk instead of deleting it during teardown.

This retained subtree shall preserve any planner-state artifacts already emitted
for that failed run and shall additionally retain failure-scoped deterministic
debugging artifacts sufficient to explain planner-state-dependent v2 failures
without requiring access to transient in-memory state after the process
unwinds.

On successful completion, the implementation may continue cleaning up the same
run-scoped planner-state scratch subtree according to its existing temporary
resource lifecycle.

### REQ-STREAM-INDEXER-127

When a v2 planning failure is caused by replay/classifier child-support
validation, the retained failure artifacts shall record deterministic
per-failing-partition replay assignment evidence sufficient to identify:

- the failing partition identity
- the expected child count used by the validation
- the observed per-child replay counts
- which child buckets were empty
- the total observed replay count used by the check

### REQ-STREAM-INDEXER-128

When a retained v2 failure depends on trainer-produced routing or planning
state, the retained planner-state store shall preserve reconstructable
deterministic evidence for the routing or planning state referenced by that
failure.

This evidence may be emitted as a structured failure artifact and shall be
sufficient to relate the retained replay assignment evidence to the
classifier/plan that produced it.

### REQ-STREAM-INDEXER-129

Retained v2 failure artifacts shall be structured and reconstructable rather
than fingerprint-only.

They shall remain bounded to run-scoped and failure-scoped evidence and shall
not require unbounded retention of per-item hot-path state or full-dataset
dumps in order to explain the failure.

### REQ-STREAM-INDEXER-130

When a v2 directional-PCA partition reaches exact cardinality only because
duplicate refinement added children beyond the populated-cell geometry, the v2
finalization surface shall preserve that exact child cardinality through
replay-faithful routing semantics instead of relying on centroid-only replay
assignment.

The crate shall not finalize such a partition as a plain classifier-backed
hierarchy node if deterministic replay through that plain classifier would leave
one or more declared children empty.

### REQ-STREAM-INDEXER-131

When the constrained v3 surface completes a partition-local clustering replay
pass successfully, observes exactly the partition's expected item count, and
receives `PassReadiness::AnalysisOnly`, it shall treat that result as valid
unresolved planning progress rather than as an immediate clustering failure.

The v3 partition-planning loop shall deterministically replay that same
partition again through the existing trainer state machine until the pass
becomes `PartitionReady` and training completion succeeds, or until the
existing bounded replay-pass limit is exceeded.

If the bounded replay-pass limit is exceeded before training completion
succeeds, the v3 surface shall fail explicitly. It shall not fail solely
because the first successful pass remained `AnalysisOnly` or because an earlier
successful pass reached `PartitionReady` before the trainer could complete.
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

This document is subordinate to `docs/protocol/indexing.md`,
`docs/protocol/blocks.md`, and `docs/protocol/ebcp.md`.

This document is also subordinate to the block crate, block-storage trait,
embeddings-trait, streaming clustering, streaming DCBC, directional-PCA,
spherical-k-means, and adaptive planning-policy specification packages for
their owned concerns.
