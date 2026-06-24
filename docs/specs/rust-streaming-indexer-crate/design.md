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
trait crate, streaming DCBC crate specification package, directional-PCA crate
specification package, spherical-k-means crate specification package, and
adaptive planning-policy crate specification package for their owned concerns.
The crate does not redefine those sources.

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
- a built-in hierarchy-construction-direction policy that selects whether the
  built-in planning path derives the finalized partition hierarchy divisively or
  agglomeratively
- a terminal-partition normalization or termination policy, or equivalent
  planning rule, used by bottom-up assembly

### DSG-STREAM-INDEXER-006 `Built-in planning selection`

The crate exposes:

- a built-in arithmetic-mean canonical-embedding policy
- built-in hierarchical planning choices backed by
  `lexongraph-directional-pca`, `lexongraph-spherical-kmeans`,
  `lexongraph-dcbc-streaming`, and the adaptive planning-policy crate that
  composes directional PCA with DCBC for one built-in aggregate option
- a caller-visible built-in planning-selection surface that requires explicit
  selection of one supported realization-and-direction combination or hybrid
  coarse/fine combination without requiring a caller-implemented factory
- caller-supplied algorithm settings supported by the selected built-in
  planning realization or each selected hybrid phase, validated against the
  selected direction
- no implicit built-in default planning algorithm, no implicit built-in default
  direction, and no implicit built-in planning settings

The built-in surface collectively exposes at least one supported `Divisive`
option and at least one supported `Agglomerative` option. Not every built-in
realization is required to support every direction in this revision.

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
   using the selected planning direction, where built-in `Divisive` mode
   partitions replayed original-item embeddings from coarser units to finer ones
   and built-in `Agglomerative` mode groups lower-layer planning units bottom-up
   into the same finalized partition-hierarchy abstraction, using selected
   shared streaming clustering realizations wherever clustering is required; an
   adaptive built-in realization may begin with directional-PCA-backed planning
   work and then switch one-way to DCBC-backed planning work while preserving
   that selected direction
5. pass completion on each clustering realization used in that planning work
6. construction of the public `IndexingPassReport`

The completed pass does not yet claim a finished persisted block tree or a
materialized parent layer.

### DSG-STREAM-INDEXER-011 `Pass report surface`

`IndexingPassReport` carries at least:

- the observed item count for the completed pass
- the requested and realized planning cluster counts when the reported planning
  work flowed through the shared clustering surface
- deterministic planning progress or quality information for the caller-visible
  hierarchy-building work for the selected planning direction, derived from the
  shared streaming clustering pass-report surface wherever clustering
  participates in that work
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

The final materialization flow does not depend on whether the retained
partition hierarchy was derived through built-in `Divisive` or
`Agglomerative` planning.

### DSG-STREAM-INDEXER-014 `Higher-layer realization`

Once leaves have been materialized and bound to terminal partitions, the crate
builds higher parent layers by following ancestor relations in the finalized
partition hierarchy rather than by further caller replay of the original items.

Any clustering used to derive or refine that hierarchy before planning
completion in either built-in direction still flows through the shared
streaming clustering contract.

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

For hierarchy-planning updates that cover recursive or dynamically discovered
work, `StreamingIndexingStatus` also carries additive structured detail fields
for:

- `progress_unit_kind`
- `discovered_unit_count`
- `current_unit_elapsed`
- `current_partition_path`
- `current_partition_size`
- `current_recursion_depth`
- `visited_partition_count`
- `finalized_partition_count`
- `terminal_partition_count`
- `completed_planner_invocation_count`
- `fallback_count`

Each detail field is optional and is represented as unavailable when it is not
relevant or not yet knowable for the current phase. Existing consumers that
only inspect phase, lifecycle state, and coarse progress counts therefore
remain valid.

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
- invalid adaptive-planning configuration
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
surface over fixtures compatible with supported built-in
realization-and-direction combinations' caller-supplied settings is intended to
hold regardless of which supported combination the caller selects.

Repository verification artifacts therefore realize that algorithm-agnostic
built-in-path behavior through a matrix over supported built-in
realization-and-direction combinations, while algorithm-specific or
direction-specific behavior remains covered by separate targeted cases.

### DSG-STREAM-INDEXER-023 `Planning boundary`

The run maintains a deterministic partition hierarchy over the established
logical item set. Planning operates over replayed original-item embeddings or
lower-layer planning units, depending on the selected built-in direction, and
stores only the hierarchy state and replay-baseline information needed for
later replay validation and bottom-up assembly.

Both built-in directions normalize into the same finalized partition-hierarchy
representation before final materialization.

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
fine-phase algorithm, the phase boundary, any phase-local built-in direction
policy, and the settings for each phase explicitly.

This caller-configured hybrid surface remains distinct from adaptive built-in
switching, whose phase transition is owned internally by the selected adaptive
realization rather than by a caller-specified coarse/fine boundary.

### DSG-STREAM-INDEXER-028 `Concurrent subpartition execution`

Independent subpartitions may be planned or assembled concurrently, but the
crate normalizes work ordering and output ordering so observable results remain
schedule-independent.

### DSG-STREAM-INDEXER-029 `Phase-specific status progress semantics`

The observer contract defines phase-native work-unit semantics as follows:

- `PlanningPass { pass_number }`: units are logical input items in the current
  replayed planning pass; the total is the established pass item count; the
  completed count is an envelope count that remains zero for non-terminal
  planning-pass status updates and advances to the pass total only when the
  planning pass completes. Measurable intra-pass work is surfaced through the
  stage-specific `HierarchyPlanning { stage }` updates rather than by
  aggregating overlapping recursive planning units into one misleading pass-wide
  partial count.
- `HierarchyPlanning { stage }`: units are declared by `progress_unit_kind`.
  Legacy hierarchy stage observers that do not provide recursive partition
  detail report `HierarchyPlanningItem` units. Recursive or divisive planning
  in this revision reports one partition-planning invocation per unit, the
  completed count advances when one such invocation finishes, and
  `current_partition_path`, `current_partition_size`,
  `current_recursion_depth`, and `current_unit_elapsed` identify the active
  planning unit. The eventual total may be unavailable because recursive work
  is discovered incrementally; when so, `discovered_unit_count` and the
  aggregate partition counters provide the best current boundary of known work
  without guessing.
- `FinalMaterializationReplay`: units are replayed logical items materialized
  into leaf blocks; the total is the baseline logical item count; the completed
  count advances as replay-verified items are persisted as leaf blocks.
- `BottomUpAssembly { layer_index }`: units are planned parent groups for that
  bottom-up layer; the total is the number of groups scheduled for
  materialization in that layer; the completed count advances as branch blocks
  for those groups are materialized.

When a total is known, the remaining count is derived as `total - completed`.
Within one execution of a phase, the completed count is monotonic
non-decreasing and shall not exceed the phase total.

### DSG-STREAM-INDEXER-030 `Semantic bottom-up layer identity`

For `BottomUpAssembly { layer_index }`, `layer_index` identifies the semantic
parent layer currently being materialized rather than a globally incrementing
assembly step counter.

The reported value is derived from the child block level being merged in that
phase, so:

- materializing parents directly over leaves reports `layer_index = 0`
- materializing parents over level-1 branch children reports `layer_index = 1`
- higher layers continue analogously

Recursive, repeated, or concurrent subtree assembly may therefore reuse the
same `layer_index` multiple times when those operations build the same semantic
layer.

### DSG-STREAM-INDEXER-031 `Explicit built-in direction policy`

The built-in planning surface exposes an explicit deterministic direction policy
whose supported values in this revision are `Divisive` and `Agglomerative`.

### DSG-STREAM-INDEXER-032 `No silent direction substitution`

If a caller omits the required built-in direction or selects a
realization/settings combination that does not support the requested direction,
the crate fails explicitly rather than silently substituting a different
direction.

### DSG-STREAM-INDEXER-033 `Divisive continuity`

Adding built-in `Agglomerative` support does not retire the existing conforming
built-in `Divisive` path.

### DSG-STREAM-INDEXER-034 `Adaptive built-in realization`

The built-in planning surface includes one adaptive aggregate realization backed
by the dedicated adaptive planning-policy crate.

That realization composes the existing directional-PCA and streaming DCBC
implementations behind the existing built-in planning-selection surface rather
than introducing a caller-interactive per-layer planning protocol or a new
shared clustering contract.

### DSG-STREAM-INDEXER-035 `Deterministic adaptive switch boundary`

Within one planning flow, the adaptive realization starts with directional PCA
and evaluates deterministic diagnostics plus configured thresholds to determine
whether PCA remains eligible for the current planning work.

Given the same replayed inputs, settings, and deterministic dependency
behavior, the realization chooses the same PCA-to-DCBC switch boundary.

### DSG-STREAM-INDEXER-036 `Adaptive direction continuity and one-way switch`

The adaptive realization supports both built-in directions:

- in `Divisive` mode, the internal algorithm switch changes the clustering
  realization used for top-down partition refinement without changing the
  top-down direction policy
- in `Agglomerative` mode, the internal algorithm switch changes the clustering
  realization used for bottom-up grouping without changing the bottom-up
  direction policy

Once the realization switches from directional PCA to DCBC within one planning
flow, it does not switch back to directional PCA later in that same flow.

### DSG-STREAM-INDEXER-037 `Adaptive normalization compatibility`

The adaptive realization normalizes both its pre-switch directional-PCA output
and its post-switch DCBC output into the same finalized partition-hierarchy
abstraction used by the rest of the indexer design, so final materialization
and bottom-up assembly remain unchanged.

### DSG-STREAM-INDEXER-038 `Child-summary policy seam`

Final materialization accepts a reusable child-summary policy that consumes the
carried-forward finalized hierarchy plus one normalized summary input per child.

Each child-summary input carries the child embedding together with the
descendant-count weight represented by that child so descendant-aware parent
summary policies can be implemented without widening the rest of the replay or
planning lifecycle.

### DSG-STREAM-INDEXER-039 `Canonical-policy compatibility adapter`

The existing canonical-embedding policy surface remains valid through a blanket
adapter into the child-summary seam.

That adapter preserves the previous arithmetic-mean-like behavior for policies
that only need branch-level child embeddings and do not consume descendant
weights explicitly.

### DSG-STREAM-INDEXER-040 `Built-in exact-centroid summary policy`

The crate exposes a built-in exact-centroid child-summary policy that computes a
parent embedding as the descendant-count-weighted mean of the normalized child
summary embeddings presented at assembly time.

### DSG-STREAM-INDEXER-041 `Published profile identifier`

The crate defines a public published-profile identifier value carrying a
semantic version tuple.

The convenience indexing surface accepts that identifier directly rather than
using an ambient "current best" default.

### DSG-STREAM-INDEXER-042 `Convenience indexing façade`

The crate exposes a stable-shape convenience constructor or equivalent façade
that:

- accepts a published profile version
- resolves it deterministically to repository-owned indexing behavior
- constructs the runtime without requiring callers to wire the low-level policy
  seams manually

### DSG-STREAM-INDEXER-043 `Published profile catalog`

Published profile resolution is deterministic and exact-match only.

Unknown or unsupported versions fail explicitly rather than aliasing to another
published profile.

### DSG-STREAM-INDEXER-044 `Indexing profile 0.1.0 mapping`

Published indexing profile `0.1.0` resolves, for the crate-owned runtime knobs
in this revision, to:

- spherical-k-means leaf formation
- cluster-order balanced-range packing
- greedy-pack hierarchy construction using Euclidean centroid distance
- exact-centroid child summaries

The same mapping also pins the published spherical-k-means settings for that
profile version:

- initialization policy = `SeededDeterministicFarthestPoint`
- max iterations = `32`
- convergence tolerance = `1e-4`
- requested cluster count = `157`
- random seed = `11`

### DSG-STREAM-INDEXER-045 `Published profile evolution contract`

The repository treats published profile versions as durable behavioral
contracts.

Within the pre-1.0 line, later patch versions preserve algorithm-family
selection while later minor versions may publish a different recommended
algorithm family bundle.

### DSG-STREAM-INDEXER-046 `Convenience profile escape hatch`

The convenience indexing façade delegates into the existing runtime
orchestration surface without removing the lower-level explicit constructors and
policy seams.

### DSG-STREAM-INDEXER-047 `Indexing profile 0.2.0 catalog entry`

Published indexing profile `0.2.0` is added alongside `0.1.0` rather than
replacing it.

The published-profile resolver therefore remains an explicit exact-match
catalog, with both versions preserved as independently addressable behavioral
contracts.

### DSG-STREAM-INDEXER-048 `Indexing profile 0.2.0 mapping`

Published indexing profile `0.2.0` resolves, for the crate-owned runtime knobs
in this revision, to:

- the built-in directional-PCA planning realization
- `Divisive` hierarchy construction
- exact-centroid child summaries
- the existing finalized partition hierarchy boundary followed by the existing
  bottom-up final block materialization flow

The convenience surface may realize that bundle by delegating into the same
runtime machinery used by the explicit built-in planning path for
directional-PCA with `Divisive` direction, rather than by inventing a separate
profile-specific assembly pipeline.

### DSG-STREAM-INDEXER-049 `Published profile 0.2.0 pinned settings`

Published indexing profile `0.2.0` pins the following directional-PCA settings
for its lifetime in this revision:

- requested cluster count = `2`
- random seed = `7`
- retained dimension count = `1`
- variance exponent = `1.0`
- temperature = `1.0`
- minimum input count = `2`
- minimum effective rank = `1`
- minimum cumulative variance = `0.0`

### DSG-STREAM-INDEXER-050 `Indexing profile 0.3.0 catalog entry`

Published indexing profile `0.3.0` is added alongside `0.1.0` and `0.2.0`
rather than replacing either earlier profile.

The published-profile resolver therefore remains an explicit exact-match
catalog, with all three versions preserved as independently addressable
behavioral contracts.

### DSG-STREAM-INDEXER-051 `Indexing profile 0.3.0 mapping`

Published indexing profile `0.3.0` resolves, for the crate-owned runtime knobs
in this revision, to:

- the built-in directional-PCA planning realization
- `Divisive` hierarchy construction
- exact-centroid child summaries
- the existing finalized partition hierarchy boundary followed by the existing
  bottom-up final block materialization flow
- explicit adaptive retained-axis participation
- explicit density-valley binning

The convenience surface may realize that bundle by delegating into the same
runtime machinery used by the explicit built-in planning path for
directional-PCA with `Divisive` direction, rather than by inventing a separate
profile-specific assembly pipeline.

### DSG-STREAM-INDEXER-052 `Published profile 0.3.0 pinned settings`

Published indexing profile `0.3.0` pins the following directional-PCA settings
for its lifetime in this revision:

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

### DSG-STREAM-INDEXER-053 `Published profile 0.3.0 policy isolation`

Published indexing profile `0.3.0` realizes the new adaptive retained-axis,
eigenvalue-log-bit allocation, density-valley, and underfull-success
cardinality policies only through the explicit published-profile mapping.

The lower-level explicit directional-PCA default path and the previously
published `0.2.0` contract therefore remain unchanged.

### DSG-STREAM-INDEXER-054 `Recursive planning progress telemetry`

For recursive or divisive hierarchy planning, the crate emits observer updates
at four lifecycle boundaries for each planning unit: `Started` when entering a
partition-planning invocation, `InProgress` periodically while that invocation
remains active, `Completed` when the invocation produces a child split or
terminal decision, and `Failed` if it aborts explicitly.

`current_partition_path` is derived from the deterministic hierarchy path being
explored (`p0`, `p0.1`, ...) or an equivalent stable pre-finalization path that
maps one-to-one onto the eventual partition ancestry.

`discovered_unit_count`, `visited_partition_count`, `finalized_partition_count`,
`terminal_partition_count`, `completed_planner_invocation_count`, and
`fallback_count` are monotonic non-decreasing within one planning pass whenever
they are exposed.

If repeated `InProgress` updates retain the same `current_partition_path`,
`current_recursion_depth`, and `completed_unit_count` while
`current_unit_elapsed` continues to grow, downstream callers may treat that as
a suspected stall or hot planning unit without interpreting free-form log text.

### DSG-STREAM-INDEXER-055 `Experimental 0.3.x profile ladder catalog`

Published indexing profiles `0.3.1` through `0.3.10` are added alongside
`0.3.0` rather than replacing it.

The published-profile resolver therefore remains an explicit exact-match
catalog whose experimental entries are independently addressable behavioral
contracts.

### DSG-STREAM-INDEXER-056 `One-change experiment attribution`

Each experimental `0.3.x` profile is specified relative to the `0.3.0`
directional-PCA baseline with one named primary changed variable wherever
feasible.

If a selected experiment requires a secondary mechanical compatibility rule,
that rule is part of the declared profile mapping rather than an implicit
runtime adjustment.

### DSG-STREAM-INDEXER-057 `Indexing profile 0.3.1 mapping`

Published indexing profile `0.3.1` resolves to the `0.3.0` directional-PCA
bundle with requested cluster count increased to `128`.

### DSG-STREAM-INDEXER-058 `Indexing profile 0.3.2 mapping`

Published indexing profile `0.3.2` resolves to the `0.3.0` directional-PCA
bundle with requested cluster count decreased to `32`.

### DSG-STREAM-INDEXER-059 `Indexing profile 0.3.3 mapping`

Published indexing profile `0.3.3` resolves to the `0.3.0` directional-PCA
bundle with quantile binning selected in place of density-valley binning.

### DSG-STREAM-INDEXER-060 `Indexing profile 0.3.4 mapping`

Published indexing profile `0.3.4` resolves to the `0.3.0` directional-PCA
bundle with fixed PC1-only retained-axis selection plus the centroid-weighted
and quantile policies required by that single-axis path.

### DSG-STREAM-INDEXER-061 `Indexing profile 0.3.5 mapping`

Published indexing profile `0.3.5` resolves to the `0.3.0` directional-PCA
bundle with centroid-weighted allocation selected in place of eigenvalue
log-bit allocation.

### DSG-STREAM-INDEXER-062 `Indexing profile 0.3.6 mapping`

Published indexing profile `0.3.6` resolves to the `0.3.0` directional-PCA
bundle with retained-axis selection capped at `FixedCount(2)`.

### DSG-STREAM-INDEXER-063 `Indexing profile 0.3.7 mapping`

Published indexing profile `0.3.7` resolves to the `0.3.0` directional-PCA
bundle with retained-axis selection capped at `FixedCount(3)`.

### DSG-STREAM-INDEXER-064 `Indexing profile 0.3.8 mapping`

Published indexing profile `0.3.8` resolves to the `0.3.0` directional-PCA
bundle with `min_cumulative_variance = 0.5`.

### DSG-STREAM-INDEXER-065 `Indexing profile 0.3.9 mapping`

Published indexing profile `0.3.9` resolves to the `0.3.0` directional-PCA
bundle with `min_effective_rank = 2`.

### DSG-STREAM-INDEXER-066 `Indexing profile 0.3.10 mapping`

Published indexing profile `0.3.10` resolves to the `0.3.0` directional-PCA
bundle with exact cardinality mode restored in place of underfull-success mode.

### DSG-STREAM-INDEXER-067 `Experiment ladder evaluation contract`

Each experimental `0.3.x` published profile is documented as a sequentially
evaluable comparison point against `0.3.0`, not as an alias for "current best"
behavior.

### DSG-STREAM-INDEXER-068 `Baseline preservation`

Resolving any experimental `0.3.x` profile does not mutate the declared mapping
of `0.3.0`.

### DSG-STREAM-INDEXER-069 `Experimental 0.4.x profile ladder catalog`

Published indexing profiles `0.4.0` through `0.4.9` are added alongside the
existing `0.3.x` ladder rather than replacing it.

The published-profile resolver therefore remains an explicit exact-match
catalog whose experimental ladder entries are independently addressable
behavioral contracts.

### DSG-STREAM-INDEXER-070 `Quantile-baseline experiment attribution`

Each experimental `0.4.x` profile is specified relative to the `0.4.0`
directional-PCA baseline with one named primary changed variable wherever
feasible.

If a selected experiment requires a secondary mechanical compatibility rule,
that rule is part of the declared profile mapping rather than an implicit
runtime adjustment.

### DSG-STREAM-INDEXER-071 `Indexing profile 0.4.0 mapping`

Published indexing profile `0.4.0` resolves to the `0.3.3` directional-PCA
bundle, making quantile binning the baseline for the `0.4.x` ladder.

### DSG-STREAM-INDEXER-072 `Indexing profile 0.4.1 mapping`

Published indexing profile `0.4.1` resolves to the `0.4.0` directional-PCA
bundle with requested cluster count increased to `128`.

### DSG-STREAM-INDEXER-073 `Indexing profile 0.4.2 mapping`

Published indexing profile `0.4.2` resolves to the `0.4.0` directional-PCA
bundle with requested cluster count decreased to `32`.

### DSG-STREAM-INDEXER-074 `Indexing profile 0.4.3 mapping`

Published indexing profile `0.4.3` resolves to the `0.4.0` directional-PCA
bundle with fixed PC1-only retained-axis selection plus the centroid-weighted
and quantile policies required by that single-axis path.

### DSG-STREAM-INDEXER-075 `Indexing profile 0.4.4 mapping`

Published indexing profile `0.4.4` resolves to the `0.4.0` directional-PCA
bundle with centroid-weighted allocation selected in place of eigenvalue
log-bit allocation while preserving quantile binning.

### DSG-STREAM-INDEXER-076 `Indexing profile 0.4.5 mapping`

Published indexing profile `0.4.5` resolves to the `0.4.0` directional-PCA
bundle with retained-axis selection capped at `FixedCount(2)` while preserving
quantile binning.

### DSG-STREAM-INDEXER-077 `Indexing profile 0.4.6 mapping`

Published indexing profile `0.4.6` resolves to the `0.4.0` directional-PCA
bundle with retained-axis selection capped at `FixedCount(3)` while preserving
quantile binning.

### DSG-STREAM-INDEXER-078 `Indexing profile 0.4.7 mapping`

Published indexing profile `0.4.7` resolves to the `0.4.0` directional-PCA
bundle with `min_cumulative_variance = 0.5` while preserving quantile binning.

### DSG-STREAM-INDEXER-079 `Indexing profile 0.4.8 mapping`

Published indexing profile `0.4.8` resolves to the `0.4.0` directional-PCA
bundle with `min_effective_rank = 2` while preserving quantile binning.

### DSG-STREAM-INDEXER-080 `Indexing profile 0.4.9 mapping`

Published indexing profile `0.4.9` resolves to the `0.4.0` directional-PCA
bundle with exact cardinality mode restored in place of underfull-success mode
while preserving quantile binning.

### DSG-STREAM-INDEXER-081 `Quantile-baseline ladder evaluation contract`

Each experimental `0.4.x` published profile is documented as a sequentially
evaluable comparison point against `0.4.0`, not as an alias for "current best"
behavior.

### DSG-STREAM-INDEXER-082 `Quantile baseline preservation`

Resolving any experimental `0.4.x` profile does not mutate the declared mapping
of `0.4.0`.

### DSG-STREAM-INDEXER-083 `v0.4 published-profile preflight contract`

The published-profile construction path performs a preflight compatibility
check for directional-PCA profiles in the `0.4.x` ladder before constructing
the indexing run.

That preflight compares the profile's requested cluster count against
configured non-data limits derived from the selected embedding spec and block
size target, including the branch materializability bound.

### DSG-STREAM-INDEXER-084 `Configured conflict versus emergent underfill`

The `0.4.x` preflight rejection applies only to configured non-data conflicts.
Runtime reductions caused by a partition exposing too few represented children
remain allowed and are not treated as configuration errors.

### DSG-STREAM-INDEXER-085 `Fail-fast scope isolation`

The configured-conflict preflight rule applies only to published directional-PCA
profiles in the `0.4.x` ladder. Earlier published profiles and non-published
planning paths retain their existing clipping or underfill behavior.

### DSG-STREAM-INDEXER-086 `Conflict diagnostic surface`

When the preflight rejects a `0.4.x` profile, the surfaced error reports the
published profile version, the requested fanout, and the conflicting
configured limit so callers can distinguish profile-contract incompatibility
from emergent runtime underfill.

### DSG-STREAM-INDEXER-087 `Experimental 0.5.x profile ladder catalog`

Published indexing profiles `0.5.0` through `0.5.4` are added alongside the
existing published ladders rather than replacing them.

The published-profile resolver therefore remains an explicit exact-match catalog
whose compression-ladder entries are independently addressable behavioral
contracts.

### DSG-STREAM-INDEXER-088 `Compression-ladder authoring boundary`

The `0.5.x` ladder keeps the `0.5.0` tree-construction path fixed and applies
its changes only at the non-leaf branch-embedding authoring boundary after the
logical hierarchy and child centroids are determined.

Leaf-block payloads and the partition topology are therefore outside the
compression-ladder mutation surface.

### DSG-STREAM-INDEXER-089 `Indexing profile 0.5.0 mapping`

Published indexing profile `0.5.0` resolves to the same tree-construction
settings, emitted topology contract, and ordinary uncompressed branch-entry
representation as `0.4.0`.

### DSG-STREAM-INDEXER-090 `Indexing profile 0.5.1 mapping`

Published indexing profile `0.5.1` resolves to the `0.5.0` contract, except
that authored non-leaf branch-entry embeddings use the EBCP encoding
`pca-rot-f32le` together with the required EBCP metadata.

### DSG-STREAM-INDEXER-091 `Indexing profile 0.5.2 mapping`

Published indexing profile `0.5.2` resolves to the `0.5.0` contract, except
that authored non-leaf branch-entry embeddings use the EBCP encoding
`pca-rot-delta-f32le` together with the required EBCP metadata.

### DSG-STREAM-INDEXER-092 `Indexing profile 0.5.3 mapping`

Published indexing profile `0.5.3` resolves to the `0.5.0` contract, except
that authored non-leaf branch-entry embeddings use the EBCP encoding
`pca-rot-delta-uq`.

The encoder assigns uniform per-dimension quantization widths of `12`, `8`, and
`6` bits on the root, interior, and lowest routing non-leaf levels
respectively.

### DSG-STREAM-INDEXER-093 `Indexing profile 0.5.4 mapping`

Published indexing profile `0.5.4` resolves to the `0.5.0` contract, except
that authored non-leaf branch-entry embeddings use the EBCP encoding
`pca-rot-delta-vbq`.

For each non-leaf block, the encoder preserves the total bit budget that
`0.5.3` would have used at the same level and dimensionality while
redistributing those bits across dimensions according to variance.

### DSG-STREAM-INDEXER-094 `EBCP authoring contract`

When the selected published profile emits EBCP-encoded branch blocks, the
indexer writes blocks that remain valid under `docs/protocol/blocks.md` and
carry all protocol-required EBCP metadata in `ext` so a search reader can
interpret the branch embeddings without out-of-band state.

### DSG-STREAM-INDEXER-095 `Compression-ladder evaluation contract`

Each experimental `0.5.x` published profile is documented as a sequentially
evaluable comparison point against `0.5.0`, not as an alias for "current best"
behavior.

Resolving any experimental `0.5.x` profile does not mutate the declared mapping
of `0.5.0`.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-STREAM-INDEXER-001 | REQ-STREAM-INDEXER-002 |
| DSG-STREAM-INDEXER-002 | REQ-STREAM-INDEXER-003 |
| DSG-STREAM-INDEXER-003..004 | REQ-STREAM-INDEXER-001, REQ-STREAM-INDEXER-004, REQ-STREAM-INDEXER-005, REQ-STREAM-INDEXER-006, REQ-STREAM-INDEXER-007 |
| DSG-STREAM-INDEXER-005 | REQ-STREAM-INDEXER-008, REQ-STREAM-INDEXER-009, REQ-STREAM-INDEXER-010, REQ-STREAM-INDEXER-012, REQ-STREAM-INDEXER-015, REQ-STREAM-INDEXER-034, REQ-STREAM-INDEXER-041 |
| DSG-STREAM-INDEXER-006 | REQ-STREAM-INDEXER-011, REQ-STREAM-INDEXER-013, REQ-STREAM-INDEXER-014, REQ-STREAM-INDEXER-015, REQ-STREAM-INDEXER-031, REQ-STREAM-INDEXER-032, REQ-STREAM-INDEXER-036, REQ-STREAM-INDEXER-041, REQ-STREAM-INDEXER-042, REQ-STREAM-INDEXER-043, REQ-STREAM-INDEXER-044 |
| DSG-STREAM-INDEXER-007..009 | REQ-STREAM-INDEXER-016, REQ-STREAM-INDEXER-017 |
| DSG-STREAM-INDEXER-010..012 | REQ-STREAM-INDEXER-004, REQ-STREAM-INDEXER-018, REQ-STREAM-INDEXER-019, REQ-STREAM-INDEXER-021, REQ-STREAM-INDEXER-024, REQ-STREAM-INDEXER-034, REQ-STREAM-INDEXER-044, REQ-STREAM-INDEXER-045, REQ-STREAM-INDEXER-046, REQ-STREAM-INDEXER-047 |
| DSG-STREAM-INDEXER-013..015 | REQ-STREAM-INDEXER-018, REQ-STREAM-INDEXER-020, REQ-STREAM-INDEXER-024, REQ-STREAM-INDEXER-025, REQ-STREAM-INDEXER-027, REQ-STREAM-INDEXER-028, REQ-STREAM-INDEXER-035, REQ-STREAM-INDEXER-038 |
| DSG-STREAM-INDEXER-016 | REQ-STREAM-INDEXER-013 |
| DSG-STREAM-INDEXER-017 | REQ-STREAM-INDEXER-022, REQ-STREAM-INDEXER-023, REQ-STREAM-INDEXER-064 |
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
| DSG-STREAM-INDEXER-029 | REQ-STREAM-INDEXER-022, REQ-STREAM-INDEXER-023, REQ-STREAM-INDEXER-039, REQ-STREAM-INDEXER-064 |
| DSG-STREAM-INDEXER-030 | REQ-STREAM-INDEXER-040 |
| DSG-STREAM-INDEXER-031 | REQ-STREAM-INDEXER-041 |
| DSG-STREAM-INDEXER-032 | REQ-STREAM-INDEXER-024, REQ-STREAM-INDEXER-042 |
| DSG-STREAM-INDEXER-033 | REQ-STREAM-INDEXER-043 |
| DSG-STREAM-INDEXER-034 | REQ-STREAM-INDEXER-011, REQ-STREAM-INDEXER-031, REQ-STREAM-INDEXER-044 |
| DSG-STREAM-INDEXER-035 | REQ-STREAM-INDEXER-044, REQ-STREAM-INDEXER-046 |
| DSG-STREAM-INDEXER-036 | REQ-STREAM-INDEXER-045, REQ-STREAM-INDEXER-047 |
| DSG-STREAM-INDEXER-037 | REQ-STREAM-INDEXER-019, REQ-STREAM-INDEXER-035, REQ-STREAM-INDEXER-045 |
| DSG-STREAM-INDEXER-038 | REQ-STREAM-INDEXER-048 |
| DSG-STREAM-INDEXER-039 | REQ-STREAM-INDEXER-050 |
| DSG-STREAM-INDEXER-040 | REQ-STREAM-INDEXER-049 |
| DSG-STREAM-INDEXER-041..046 | REQ-STREAM-INDEXER-051, REQ-STREAM-INDEXER-052, REQ-STREAM-INDEXER-053, REQ-STREAM-INDEXER-054, REQ-STREAM-INDEXER-055, REQ-STREAM-INDEXER-056, REQ-STREAM-INDEXER-057 |
| DSG-STREAM-INDEXER-047 | REQ-STREAM-INDEXER-051, REQ-STREAM-INDEXER-053, REQ-STREAM-INDEXER-058 |
| DSG-STREAM-INDEXER-048 | REQ-STREAM-INDEXER-035, REQ-STREAM-INDEXER-055, REQ-STREAM-INDEXER-057, REQ-STREAM-INDEXER-059 |
| DSG-STREAM-INDEXER-049 | REQ-STREAM-INDEXER-053, REQ-STREAM-INDEXER-060 |
| DSG-STREAM-INDEXER-050 | REQ-STREAM-INDEXER-058, REQ-STREAM-INDEXER-061 |
| DSG-STREAM-INDEXER-051 | REQ-STREAM-INDEXER-061, REQ-STREAM-INDEXER-063 |
| DSG-STREAM-INDEXER-052 | REQ-STREAM-INDEXER-062 |
| DSG-STREAM-INDEXER-053 | REQ-STREAM-INDEXER-063 |
| DSG-STREAM-INDEXER-055 | REQ-STREAM-INDEXER-065 |
| DSG-STREAM-INDEXER-056 | REQ-STREAM-INDEXER-066, REQ-STREAM-INDEXER-067 |
| DSG-STREAM-INDEXER-057 | REQ-STREAM-INDEXER-069 |
| DSG-STREAM-INDEXER-058 | REQ-STREAM-INDEXER-070 |
| DSG-STREAM-INDEXER-059 | REQ-STREAM-INDEXER-071 |
| DSG-STREAM-INDEXER-060 | REQ-STREAM-INDEXER-072 |
| DSG-STREAM-INDEXER-061 | REQ-STREAM-INDEXER-073 |
| DSG-STREAM-INDEXER-062 | REQ-STREAM-INDEXER-074 |
| DSG-STREAM-INDEXER-063 | REQ-STREAM-INDEXER-075 |
| DSG-STREAM-INDEXER-064 | REQ-STREAM-INDEXER-076 |
| DSG-STREAM-INDEXER-065 | REQ-STREAM-INDEXER-077 |
| DSG-STREAM-INDEXER-066 | REQ-STREAM-INDEXER-078 |
| DSG-STREAM-INDEXER-067 | REQ-STREAM-INDEXER-068 |
| DSG-STREAM-INDEXER-068 | REQ-STREAM-INDEXER-067 |
| DSG-STREAM-INDEXER-069 | REQ-STREAM-INDEXER-079 |
| DSG-STREAM-INDEXER-070 | REQ-STREAM-INDEXER-079, REQ-STREAM-INDEXER-080 |
| DSG-STREAM-INDEXER-071 | REQ-STREAM-INDEXER-080 |
| DSG-STREAM-INDEXER-072 | REQ-STREAM-INDEXER-081 |
| DSG-STREAM-INDEXER-073 | REQ-STREAM-INDEXER-082 |
| DSG-STREAM-INDEXER-074 | REQ-STREAM-INDEXER-083 |
| DSG-STREAM-INDEXER-075 | REQ-STREAM-INDEXER-084 |
| DSG-STREAM-INDEXER-076 | REQ-STREAM-INDEXER-085 |
| DSG-STREAM-INDEXER-077 | REQ-STREAM-INDEXER-086 |
| DSG-STREAM-INDEXER-078 | REQ-STREAM-INDEXER-087 |
| DSG-STREAM-INDEXER-079 | REQ-STREAM-INDEXER-088 |
| DSG-STREAM-INDEXER-080 | REQ-STREAM-INDEXER-089 |
| DSG-STREAM-INDEXER-081 | REQ-STREAM-INDEXER-079 |
| DSG-STREAM-INDEXER-082 | REQ-STREAM-INDEXER-080 |
| DSG-STREAM-INDEXER-083 | REQ-STREAM-INDEXER-090 |
| DSG-STREAM-INDEXER-084 | REQ-STREAM-INDEXER-092 |
| DSG-STREAM-INDEXER-085 | REQ-STREAM-INDEXER-093 |
| DSG-STREAM-INDEXER-086 | REQ-STREAM-INDEXER-091 |
| DSG-STREAM-INDEXER-087 | REQ-STREAM-INDEXER-094 |
| DSG-STREAM-INDEXER-088 | REQ-STREAM-INDEXER-095, REQ-STREAM-INDEXER-100 |
| DSG-STREAM-INDEXER-089 | REQ-STREAM-INDEXER-095 |
| DSG-STREAM-INDEXER-090 | REQ-STREAM-INDEXER-096, REQ-STREAM-INDEXER-101 |
| DSG-STREAM-INDEXER-091 | REQ-STREAM-INDEXER-097, REQ-STREAM-INDEXER-101 |
| DSG-STREAM-INDEXER-092 | REQ-STREAM-INDEXER-098, REQ-STREAM-INDEXER-101 |
| DSG-STREAM-INDEXER-093 | REQ-STREAM-INDEXER-099, REQ-STREAM-INDEXER-101 |
| DSG-STREAM-INDEXER-094 | REQ-STREAM-INDEXER-100, REQ-STREAM-INDEXER-101 |
| DSG-STREAM-INDEXER-095 | REQ-STREAM-INDEXER-094, REQ-STREAM-INDEXER-095 |
| DSG-STREAM-INDEXER-054 | REQ-STREAM-INDEXER-022, REQ-STREAM-INDEXER-023, REQ-STREAM-INDEXER-039, REQ-STREAM-INDEXER-064 |
