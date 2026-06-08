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
2. ingest one or more batches of indexing items for the current planning pass
3. complete the pass and obtain an `IndexingPassReport`
4. either begin the next pass or mark planning complete
5. consume one final materialization replay to produce the finished index result
   by assembling parent layers bottom-up from the finalized partition hierarchy

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
- a hierarchical planning strategy whose clustering subproblems flow through
  streaming clustering realizations or factories whose trainer/classifier
  surface is defined by the shared streaming clustering contract
- a terminal-partition normalization or termination policy, or equivalent
  planning rule, used by bottom-up assembly

### DSG-STREAM-INDEXER-006 `Built-in planning selection`

The crate exposes:

- a built-in arithmetic-mean canonical-embedding policy
- built-in hierarchical planning choices backed by
  `lexongraph-directional-pca` and `lexongraph-dcbc-streaming`
- a caller-visible built-in planning-selection surface that requires explicit
  selection of one realization or hybrid coarse/fine combination without
  requiring a caller-implemented factory
- caller-supplied algorithm settings supported by the selected built-in
  planning realization or each selected hybrid phase
- no implicit built-in default planning algorithm or implicit built-in planning
  settings

The crate also exposes override paths for caller-supplied canonical-embedding
and planning policies.

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
3. deterministic derivation of planning-time item embeddings and any pass-local
   planning artifacts
4. deterministic refinement or validation of the hierarchical partition plan
   over original-item embeddings, using selected shared streaming clustering
   realizations wherever clustering is required
5. pass completion on each clustering realization used in that planning work
6. construction of the public `IndexingPassReport`

The completed pass does not yet claim a finished persisted block tree or a
materialized parent layer.

### DSG-STREAM-INDEXER-011 `Pass report surface`

`IndexingPassReport` carries at least:

- the observed item count for the completed pass
- deterministic planning progress or quality information for the caller-visible
  replayed hierarchy-building work, derived from the shared streaming
  clustering pass-report surface wherever clustering participates in that work
- structured state sufficient for caller stop/continue decisions

The report remains deterministic for a fixed indexing context and replay order.

### DSG-STREAM-INDEXER-012 `Planning completion gate`

The run exposes a caller-directed transition that marks planning complete only
after at least one successful completed pass and after the retained partition
hierarchy covers the established logical item set. Final materialization before
that transition fails explicitly.

### DSG-STREAM-INDEXER-013 `Final materialization replay`

After planning completion, the run consumes one final materialization replay of
the same logical item set in the same replay order.

During that replay, the crate:

1. resolves content and generates embeddings again in deterministic order
2. constructs exactly one leaf block per item
3. persists the produced leaf blocks
4. binds each produced leaf deterministically to one terminal partition in the
   finalized partition hierarchy
5. materializes parent layers bottom-up from the terminal partitions until
   exactly one root remains

### DSG-STREAM-INDEXER-014 `Higher-layer realization`

Once leaves have been materialized and bound to terminal partitions, the crate
builds higher parent layers by following ancestor relations in the finalized
partition hierarchy rather than by further caller replay of the original items.

Any clustering used to derive or refine that hierarchy before planning
completion still flows through the shared streaming clustering contract.

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

- caller-visible replay-pass planning progress
- hierarchy-planning start, in-progress, completion, and failure for coarse and
  fine partition work
- final materialization progress
- bottom-up assembly progress

Each status update includes:

- phase identity
- lifecycle state
- elapsed time
- `phase_total_unit_count: Option<usize>`
- `completed_unit_count: usize`
- `remaining_unit_count: Option<usize>`

For `InProgress` updates, the observer receives the latest measured completion
state for the phase rather than a heartbeat carrying only a fixed total. If a
quantity is not knowable for a phase at a given moment, the observer represents
that explicitly as unavailable rather than by reusing another field with
ambiguous meaning.

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
- hierarchy-validation failure
- invalid hybrid-planning configuration
- canonical-embedding failure
- block-construction failure
- terminal-partition materialization failure
- storage failure
- invalid lifecycle transition

### DSG-STREAM-INDEXER-019 `Determinism boundary`

If the content resolver, embedding provider, canonical-embedding policy,
planning strategy, block-store interactions affecting visibility, and any
streaming clustering realizations used by that planning strategy are
deterministic within one indexing context, then identical item replays, pass
boundaries, and final materialization replay produce the same pass reports, the
same finalized partition hierarchy, the same root block ID, and the same
persisted block set regardless of concurrent execution schedule.

### DSG-STREAM-INDEXER-020 `Result packaging`

A successful final result contains the root block ID and the complete persisted
block set, or the complete produced block IDs sufficient to identify that set,
for the protocol-conforming finished tree.

### DSG-STREAM-INDEXER-021 `Feature-gated conformance surface`

The crate exposes reusable conformance-test helpers for its indexer-owned policy
traits behind a non-default Cargo feature intended for downstream tests only.

### DSG-STREAM-INDEXER-022 `Built-in algorithm verification matrix`

Algorithm-agnostic behavior exercised through the built-in planning-selection
surface over fixtures compatible with both algorithms' caller-supplied settings
is intended to hold regardless of whether the caller selects the built-in
directional-PCA realization or the built-in DCBC realization.

Repository verification artifacts therefore realize that algorithm-agnostic
built-in-path behavior through a two-algorithm matrix, while algorithm-specific
behavior remains covered by separate targeted cases.

### DSG-STREAM-INDEXER-023 `Planning boundary`

The run maintains a deterministic partition hierarchy over the established
logical item set. Planning operates over replayed original-item embeddings and
stores only the hierarchy state and replay-baseline information needed for
later replay validation and bottom-up assembly.

### DSG-STREAM-INDEXER-024 `Partition identity and ancestry`

Every partition in the finalized hierarchy has a deterministic identity derived
from stable ancestry plus deterministic local child ordering so independent
subpartition processing does not change observable IDs or parent-child
relations.

### DSG-STREAM-INDEXER-025 `Terminal partition normalization`

Terminal partitions are not required to correspond one-to-one with final branch
blocks until normalized against entry deduplication, minimum-child-count, and
size-target constraints. The crate applies deterministic normalization rules or
fails explicitly when no conforming normalization exists.

### DSG-STREAM-INDEXER-026 `Materializability bound`

The crate derives a deterministic materializability bound from
the block size target and `embedding_spec` and uses that bound to determine
when a partition may remain terminal, must be refined further, or must be fused
or rejected before materialization.

### DSG-STREAM-INDEXER-027 `Hybrid planning selection`

The built-in planning path supports hybrid coarse/fine behavior through
caller-visible configuration that selects the coarse-phase algorithm, the
fine-phase algorithm, the phase boundary, and the settings for each phase
explicitly.

### DSG-STREAM-INDEXER-028 `Concurrent subpartition execution`

Independent subpartitions may be planned or assembled concurrently, but the
crate normalizes work ordering and output ordering so observable results remain
schedule-independent.

### DSG-STREAM-INDEXER-029 `Phase-specific status progress semantics`

The observer contract defines phase-native work-unit semantics as follows:

- `TrainingPass { pass_number }`: units are logical input items in the current
  replayed training pass; the total is the established pass item count; the
  completed count advances as pass items are handed to the streaming clustering
  trainer.
- `LeafMaterialization`: units are replayed logical items materialized into
  leaf blocks; the total is the baseline logical item count; the completed
  count advances as replay-verified items are persisted as leaf blocks.
- `FirstLayerClustering`: units are unique leaf children assigned into
  first-layer parent groups; the total is the unique leaf-child count entering
  first-layer clustering; the completed count advances as child assignments are
  produced.
- `HigherLayerClustering { layer_index }`: units are current-layer child
  entries assigned into parent groups for that higher layer; the total is the
  child-entry count entering that layer's clustering step; the completed count
  advances as assignments are produced.
- `LayerMaterialization { layer_index }`: units are planned parent groups for
  that layer's block-construction step; the total is the number of groups
  scheduled for materialization in that layer; the completed count advances as
  branch blocks for those groups are materialized.

When a total is known, the remaining count is derived as `total - completed`.
Within one execution of a phase, the completed count is monotonic
non-decreasing and shall not exceed the phase total.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-STREAM-INDEXER-001 | REQ-STREAM-INDEXER-002 |
| DSG-STREAM-INDEXER-002 | REQ-STREAM-INDEXER-003 |
| DSG-STREAM-INDEXER-003..004 | REQ-STREAM-INDEXER-001, REQ-STREAM-INDEXER-004, REQ-STREAM-INDEXER-005, REQ-STREAM-INDEXER-006, REQ-STREAM-INDEXER-007 |
| DSG-STREAM-INDEXER-005 | REQ-STREAM-INDEXER-008, REQ-STREAM-INDEXER-009, REQ-STREAM-INDEXER-010, REQ-STREAM-INDEXER-012, REQ-STREAM-INDEXER-015, REQ-STREAM-INDEXER-034 |
| DSG-STREAM-INDEXER-006 | REQ-STREAM-INDEXER-011, REQ-STREAM-INDEXER-013, REQ-STREAM-INDEXER-014, REQ-STREAM-INDEXER-015, REQ-STREAM-INDEXER-031, REQ-STREAM-INDEXER-032, REQ-STREAM-INDEXER-036 |
| DSG-STREAM-INDEXER-007..009 | REQ-STREAM-INDEXER-016, REQ-STREAM-INDEXER-017 |
| DSG-STREAM-INDEXER-010..012 | REQ-STREAM-INDEXER-004, REQ-STREAM-INDEXER-018, REQ-STREAM-INDEXER-019, REQ-STREAM-INDEXER-021, REQ-STREAM-INDEXER-024, REQ-STREAM-INDEXER-034 |
| DSG-STREAM-INDEXER-013..015 | REQ-STREAM-INDEXER-018, REQ-STREAM-INDEXER-020, REQ-STREAM-INDEXER-024, REQ-STREAM-INDEXER-025, REQ-STREAM-INDEXER-027, REQ-STREAM-INDEXER-028, REQ-STREAM-INDEXER-035, REQ-STREAM-INDEXER-038 |
| DSG-STREAM-INDEXER-016 | REQ-STREAM-INDEXER-013 |
| DSG-STREAM-INDEXER-017 | REQ-STREAM-INDEXER-022, REQ-STREAM-INDEXER-023 |
| DSG-STREAM-INDEXER-018 | REQ-STREAM-INDEXER-024 |
| DSG-STREAM-INDEXER-019 | REQ-STREAM-INDEXER-026, REQ-STREAM-INDEXER-037 |
| DSG-STREAM-INDEXER-020 | REQ-STREAM-INDEXER-028 |
| DSG-STREAM-INDEXER-021 | REQ-STREAM-INDEXER-029, REQ-STREAM-INDEXER-030 |
| DSG-STREAM-INDEXER-022 | REQ-STREAM-INDEXER-033 |
| DSG-STREAM-INDEXER-023 | REQ-STREAM-INDEXER-034 |
| DSG-STREAM-INDEXER-024 | REQ-STREAM-INDEXER-035, REQ-STREAM-INDEXER-037 |
| DSG-STREAM-INDEXER-025..026 | REQ-STREAM-INDEXER-035, REQ-STREAM-INDEXER-038 |
| DSG-STREAM-INDEXER-027 | REQ-STREAM-INDEXER-036 |
| DSG-STREAM-INDEXER-028 | REQ-STREAM-INDEXER-037 |
| DSG-STREAM-INDEXER-029 | REQ-STREAM-INDEXER-022, REQ-STREAM-INDEXER-023, REQ-STREAM-INDEXER-039 |
