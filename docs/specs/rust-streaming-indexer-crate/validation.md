<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Indexer Crate Validation

## Status

Draft validation specification for a Rust crate that implements the LexonGraph
indexing protocol through a caller-visible streaming replay boundary.

## Validation Scope

These validation entries define the expected conformance surface for the new
streaming indexer crate. They cover both:

- the caller-visible replay lifecycle
- realization of protocol-conforming final block outputs

## Validation Entries

### VAL-STREAM-INDEXER-001

Inspect the repository artifacts for the new crate.

**Pass condition:** the repository includes a crate at
`crates/lexongraph-streaming-indexer` and this specification package, and the
streaming line's applicability does not depend on retired legacy
batch-oriented indexing artifacts remaining present.

**Traces to:** REQ-STREAM-INDEXER-001, REQ-STREAM-INDEXER-003

### VAL-STREAM-INDEXER-002

Inspect the new crate's public surface and specification references.

**Pass condition:** the crate exposes a caller-visible streaming replay
lifecycle, remains subordinate to the indexing and block protocols, and
consumes the shared streaming clustering contract, references the built-in DCBC,
directional-PCA, and spherical-k-means specification packages for their owned
clustering algorithms, and does not define a new clustering contract locally or
depend on a retired legacy batch-oriented indexing crate/specification package
as part of its normative boundary.

**Traces to:** REQ-STREAM-INDEXER-002, REQ-STREAM-INDEXER-004,
REQ-STREAM-INDEXER-010

### VAL-STREAM-INDEXER-003

Invoke the streaming indexing API with zero items for a pass or with no items
for the logical run.

**Pass condition:** the crate fails explicitly and does not report a successful
pass or final indexing result.

**Traces to:** REQ-STREAM-INDEXER-005, REQ-STREAM-INDEXER-024

### VAL-STREAM-INDEXER-004

Inspect one streamed indexing item at the public boundary.

**Pass condition:** the item carries application metadata plus a content
reference, and the API does not require inline raw content bytes.

**Traces to:** REQ-STREAM-INDEXER-006, REQ-STREAM-INDEXER-007

### VAL-STREAM-INDEXER-005

Use distinct content resolver implementations for different reference classes.

**Pass condition:** the same streaming indexing contract remains applicable
without backend-specific API changes in the indexer crate.

**Traces to:** REQ-STREAM-INDEXER-008, REQ-STREAM-INDEXER-010

### VAL-STREAM-INDEXER-006

Use an embedding-provider implementation satisfying the shared
embeddings-trait contract.

**Pass condition:** the streaming indexer consumes the provider through that
shared contract without redefining embedding-provider behavior locally.

**Traces to:** REQ-STREAM-INDEXER-009, REQ-STREAM-INDEXER-010

### VAL-STREAM-INDEXER-007

Construct the streaming indexer through its built-in planning-selection path,
selecting directional PCA, selecting a supported built-in hierarchy
construction direction, and supplying caller-provided directional-PCA settings
supported by that realization-and-direction combination.

**Pass condition:** the runtime can be created without a caller-implemented
factory, uses the built-in arithmetic-mean canonical policy unless another
canonical policy is explicitly supplied, requires explicit selection of a
supported built-in realization-and-direction combination, and consumes the
caller-provided directional-PCA settings supported by that combination.

**Traces to:** REQ-STREAM-INDEXER-011, REQ-STREAM-INDEXER-013,
REQ-STREAM-INDEXER-014, REQ-STREAM-INDEXER-031, REQ-STREAM-INDEXER-032,
REQ-STREAM-INDEXER-041

### VAL-STREAM-INDEXER-008

Construct another streaming indexer through an explicit override path using
caller-supplied canonical-embedding, hierarchical-planning, or clustering
implementations.

**Pass condition:** the crate accepts those replacements without changing the
rest of the streaming runtime contract.

**Traces to:** REQ-STREAM-INDEXER-012, REQ-STREAM-INDEXER-015

### VAL-STREAM-INDEXER-009

Complete one successful planning pass with multiple caller-chosen batches.

**Pass condition:** the pass report is deterministic and includes the observed
item count plus deterministic planning progress or quality information for the
caller-visible hierarchy-building work of the selected planning direction.

**Traces to:** REQ-STREAM-INDEXER-004, REQ-STREAM-INDEXER-021

### VAL-STREAM-INDEXER-010

Run two completed passes whose replayed item sequence is identical.

**Pass condition:** the second pass is accepted and yields a deterministic pass
report under the same indexing context.

**Traces to:** REQ-STREAM-INDEXER-016, REQ-STREAM-INDEXER-026

### VAL-STREAM-INDEXER-011

Run a later pass whose observed item count, item order, metadata, or content
reference sequence differs from the first completed pass.

**Pass condition:** the crate fails explicitly before claiming conformant
continuation of the run.

**Traces to:** REQ-STREAM-INDEXER-016, REQ-STREAM-INDEXER-024

### VAL-STREAM-INDEXER-012

Attempt final materialization before planning completion.

**Pass condition:** the crate fails explicitly rather than producing a finished
index result.

**Traces to:** REQ-STREAM-INDEXER-004, REQ-STREAM-INDEXER-024

### VAL-STREAM-INDEXER-013

After planning completion, supply a final materialization replay identical to
the established logical item set and replay order.

**Pass condition:** final materialization succeeds without requiring the crate's
public API to have retained the full logical dataset between passes, while
using the finalized partition hierarchy to drive bottom-up assembly regardless
of whether that hierarchy was derived divisively or agglomeratively.

**Traces to:** REQ-STREAM-INDEXER-004, REQ-STREAM-INDEXER-016,
REQ-STREAM-INDEXER-017, REQ-STREAM-INDEXER-028, REQ-STREAM-INDEXER-035

### VAL-STREAM-INDEXER-014

After planning completion, supply a final materialization replay whose item
count or replay order differs from the established baseline.

**Pass condition:** final materialization fails explicitly.

**Traces to:** REQ-STREAM-INDEXER-016, REQ-STREAM-INDEXER-024

### VAL-STREAM-INDEXER-015

Resolve one item successfully during final materialization.

**Pass condition:** the produced leaf entry stores the resolved media type and
resolved bytes inline in the leaf `content` payload.

**Traces to:** REQ-STREAM-INDEXER-025

### VAL-STREAM-INDEXER-016

Materialize a final result for exactly one logical item.

**Pass condition:** the crate constructs exactly one leaf block containing
exactly one leaf entry, persists it, and returns that leaf block as the root.

**Traces to:** REQ-STREAM-INDEXER-027, REQ-STREAM-INDEXER-028

### VAL-STREAM-INDEXER-017

Materialize a final result for multiple items that require one or more
intermediate layers.

**Pass condition:** the crate produces exactly one leaf block per item, builds
protocol-conforming parent layers bottom-up from the finalized partition
hierarchy until exactly one root block remains, and returns the root block ID
plus the complete persisted block set.

**Traces to:** REQ-STREAM-INDEXER-018, REQ-STREAM-INDEXER-027,
REQ-STREAM-INDEXER-028, REQ-STREAM-INDEXER-035

### VAL-STREAM-INDEXER-018

Construct candidate child-entry sets that include out-of-order embeddings or
duplicate child block IDs before final block construction.

**Pass condition:** finalized child-bearing block entries are sorted by raw
embedding bytes with required deterministic tie-breaks and deduplicated by
child block ID before block construction.

**Traces to:** REQ-STREAM-INDEXER-018, REQ-STREAM-INDEXER-027

### VAL-STREAM-INDEXER-019

Use a block size target that constrains parent construction.

**Pass condition:** each intermediate node block remains at or below the target
size and contains at least two child entries, or the crate fails explicitly if
no conforming parent block can be constructed.

**Traces to:** REQ-STREAM-INDEXER-018, REQ-STREAM-INDEXER-024,
REQ-STREAM-INDEXER-027

### VAL-STREAM-INDEXER-020

Inspect the implementation of final assembly from the finalized partition
hierarchy.

**Pass condition:** parent-layer construction is driven by the stored partition
hierarchy, and any clustering used while deriving or refining that hierarchy
for either built-in direction still flows through the shared streaming
clustering contract rather than an older batch-only clustering boundary.

**Traces to:** REQ-STREAM-INDEXER-020, REQ-STREAM-INDEXER-035

### VAL-STREAM-INDEXER-021

Run the same logical item set, indexing context, pass boundaries, and final
materialization replay twice with deterministic dependency behavior.

**Pass condition:** both runs produce the same pass reports, the same finalized
partition hierarchy, the same root block ID, and the same persisted block set.

**Traces to:** REQ-STREAM-INDEXER-026, REQ-STREAM-INDEXER-034,
REQ-STREAM-INDEXER-037

### VAL-STREAM-INDEXER-022

Invoke the streaming indexing API with content-resolution failure, unusable
resolved content, embedding failure, clustering failure, invalid hierarchy,
invalid hybrid-planning configuration, invalid adaptive-planning
configuration, canonical-embedding failure,
block-construction failure, terminal-partition materialization failure, and
storage failure fixtures.

**Pass condition:** each failure is explicit and does not masquerade as success
or partial success.

**Traces to:** REQ-STREAM-INDEXER-024

### VAL-STREAM-INDEXER-023

Attach a caller-owned in-memory status observer and run a fixture whose
planning or bottom-up assembly work remains active long enough to be
non-trivial.

**Pass condition:** the observer receives structured start, in-progress, and
completion or failure updates without requiring stdout, tracing integration, or
repository-specific telemetry, and for each observed phase:

- `completed_unit_count` is present and monotonic non-decreasing within that
  phase execution
- `phase_total_unit_count`, when present, never falls below
  `completed_unit_count`
- `remaining_unit_count`, when present, equals
  `phase_total_unit_count - completed_unit_count`
- in-progress updates reflect advancing completion state when the underlying
  work advances measurably, rather than only elapsed time

**Traces to:** REQ-STREAM-INDEXER-022, REQ-STREAM-INDEXER-023,
REQ-STREAM-INDEXER-039

### VAL-STREAM-INDEXER-024

Inspect the crate feature surface and downstream test usage.

**Pass condition:** reusable conformance-test helpers for indexer-owned policy
traits exist only behind a non-default, test-oriented feature.

**Traces to:** REQ-STREAM-INDEXER-029

### VAL-STREAM-INDEXER-025

Inspect the caller-visible API surface for dataset-size coupling.

**Pass condition:** repeated planning passes and final materialization require
caller replay of the logical item set rather than a default public API
obligation for the crate to retain or rematerialize the entire dataset on the
caller's behalf, even if internal partition-plan state is retained.

**Traces to:** REQ-STREAM-INDEXER-017

### VAL-STREAM-INDEXER-026

Inspect the new crate's dependency manifest and built-in planning realizations.

**Pass condition:** the crate depends on `lexongraph-dcbc-streaming` and
`lexongraph-directional-pca`, and each supported built-in
realization-and-direction combination delegates through the shared streaming
clustering contract rather than reimplementing either algorithm locally.

**Traces to:** REQ-STREAM-INDEXER-011, REQ-STREAM-INDEXER-019

### VAL-STREAM-INDEXER-027

Inspect the repository verification artifacts for the new crate.

**Pass condition:** executable automated tests exist that realize this
validation surface.

**Traces to:** REQ-STREAM-INDEXER-030

### VAL-STREAM-INDEXER-028

Construct the streaming indexer through its built-in planning-selection
surface, selecting supported built-in realization-and-direction combinations.

**Pass condition:** callers can choose supported built-in planning combinations
through the indexer API without implementing a custom planning factory, each
selection requires caller-supplied settings for the chosen algorithm and
direction, attempts to omit the required algorithm choice, required direction,
or required settings fail explicitly, and the rest of the streaming runtime
contract remains unchanged. Supported selections include the adaptive aggregate
built-in realization when its required settings are supplied.

**Traces to:** REQ-STREAM-INDEXER-011, REQ-STREAM-INDEXER-014,
REQ-STREAM-INDEXER-024, REQ-STREAM-INDEXER-031, REQ-STREAM-INDEXER-032,
REQ-STREAM-INDEXER-041, REQ-STREAM-INDEXER-042, REQ-STREAM-INDEXER-044

### VAL-STREAM-INDEXER-029

Inspect the repository verification artifacts for algorithm-agnostic built-in
planning behavior.

**Pass condition:** algorithm-agnostic built-in-path planning and assembly
cases whose fixtures are compatible with supported built-in
realization-and-direction combinations' caller-supplied settings are realized
as a matrix over those supported combinations rather than favoring one built-in
algorithm or direction, while unsupported or algorithm-specific behavior
remains covered by separate targeted tests.

**Traces to:** REQ-STREAM-INDEXER-030, REQ-STREAM-INDEXER-033,
REQ-STREAM-INDEXER-041

### VAL-STREAM-INDEXER-030

Run identical planning passes twice over the same logical item set and compare
the resulting partition hierarchies.

**Pass condition:** partition identities, ancestry, and terminal memberships are
deterministic across both runs.

**Traces to:** REQ-STREAM-INDEXER-034, REQ-STREAM-INDEXER-037

### VAL-STREAM-INDEXER-031

Construct a finalized partition hierarchy containing overlapping, non-covering,
or ancestry-inconsistent partitions.

**Pass condition:** the crate fails explicitly before claiming conformant final
assembly.

**Traces to:** REQ-STREAM-INDEXER-024, REQ-STREAM-INDEXER-035

### VAL-STREAM-INDEXER-032

Drive planning or assembly to terminal partitions near the
materializability bound imposed by the block size target and `embedding_spec`.

**Pass condition:** terminal partitions are refined, normalized, or rejected
deterministically according to the materializability rules and block
constraints.

**Traces to:** REQ-STREAM-INDEXER-035, REQ-STREAM-INDEXER-038

### VAL-STREAM-INDEXER-033

Construct the built-in hierarchical planning path using one algorithm for the
coarse phase and another for the fine phase.

**Pass condition:** the coarse/fine phase boundary and the settings for each
algorithm are explicit, any phase-local direction policy is explicit, and the
resulting planning behavior is deterministic. This validation remains about the
caller-configured hybrid coarse/fine surface rather than adaptive switching.

**Traces to:** REQ-STREAM-INDEXER-036, REQ-STREAM-INDEXER-041

### VAL-STREAM-INDEXER-034

Execute the same independent subpartitions under different concurrent
scheduling orders.

**Pass condition:** both executions produce identical partition hierarchies,
pass reports, root block IDs, and persisted block sets.

**Traces to:** REQ-STREAM-INDEXER-037

### VAL-STREAM-INDEXER-035

Construct terminal partitions that collapse to singleton or undersized child
sets after child-ID deduplication.

**Pass condition:** the crate performs deterministic normalization or fails
explicitly before reporting a successful final result.

**Traces to:** REQ-STREAM-INDEXER-035, REQ-STREAM-INDEXER-038

### VAL-STREAM-INDEXER-036

Run a deterministic fixture that exercises:

- at least one non-trivial planning pass
- at least one hierarchy-planning stage
- final materialization replay
- at least one bottom-up assembly layer

Capture the observer stream and inspect the per-phase progress payloads.

**Pass condition:** for each exercised phase, the recorded
`phase_total_unit_count`, `completed_unit_count`, and `remaining_unit_count`
match the phase-specific semantics defined in the design, and a downstream can
derive materially useful progress such as "processed X / Y, Z remaining"
without inferring semantics from elapsed time alone. For recursive
hierarchy-planning work, the captured structured payload also exposes the
declared planning-unit kind plus any current-unit descriptor and
discovered-unit fields required by the design, or explicitly reports those
fields as unavailable when they are not yet knowable.

**Traces to:** REQ-STREAM-INDEXER-022, REQ-STREAM-INDEXER-023,
REQ-STREAM-INDEXER-039, REQ-STREAM-INDEXER-064

### VAL-STREAM-INDEXER-037

Run a deterministic fixture whose finalized hierarchy causes multiple bottom-up
assemblies at the same semantic depth plus at least one higher-layer merge.

Capture the observer stream and inspect the reported
`BottomUpAssembly { layer_index }` phases.

**Pass condition:** the recorded `layer_index` values identify the semantic
parent layer being materialized rather than the temporal count of recursive
assembly operations. Distinct subtree or sibling assemblies that build the same
semantic layer reuse the same `layer_index`, and the observed layer indexes are
bounded by the assembled tree depth implied by the hierarchy and block levels.

**Traces to:** REQ-STREAM-INDEXER-039, REQ-STREAM-INDEXER-040

### VAL-STREAM-INDEXER-038

Construct two deterministic built-in planning runs over compatible fixtures:
one using a supported `Divisive` combination and one using a supported
`Agglomerative` combination.

**Pass condition:** both runs derive a deterministic finalized partition
hierarchy that can drive the same final-materialization contract, and the
addition of the `Agglomerative` option does not retire the conforming
`Divisive` path.

**Traces to:** REQ-STREAM-INDEXER-019, REQ-STREAM-INDEXER-020,
REQ-STREAM-INDEXER-041, REQ-STREAM-INDEXER-043

### VAL-STREAM-INDEXER-039

Attempt to invoke the built-in planning surface without specifying a direction,
or by selecting a realization/settings combination that does not support the
requested direction.

**Pass condition:** the crate fails explicitly and does not silently substitute
another planning direction.

**Traces to:** REQ-STREAM-INDEXER-024, REQ-STREAM-INDEXER-031,
REQ-STREAM-INDEXER-032, REQ-STREAM-INDEXER-042

### VAL-STREAM-INDEXER-040

Construct the built-in planning path using the adaptive aggregate realization
with explicit adaptive settings, once in a supported `Divisive` configuration
and once in a supported `Agglomerative` configuration.

**Pass condition:** both constructions succeed without a caller-implemented
planning factory, both require explicit adaptive settings, and neither silently
substitutes another built-in direction or non-adaptive realization.

**Traces to:** REQ-STREAM-INDEXER-014, REQ-STREAM-INDEXER-031,
REQ-STREAM-INDEXER-032, REQ-STREAM-INDEXER-041, REQ-STREAM-INDEXER-042,
REQ-STREAM-INDEXER-044, REQ-STREAM-INDEXER-045

### VAL-STREAM-INDEXER-041

Run a deterministic adaptive-planning fixture whose configured switch criteria
are never met.

**Pass condition:** the adaptive realization remains on its directional-PCA path
throughout the exercised planning flow, does not spuriously switch to DCBC, and
still produces a deterministic finalized partition hierarchy compatible with the
existing final-materialization contract.

**Traces to:** REQ-STREAM-INDEXER-034, REQ-STREAM-INDEXER-035,
REQ-STREAM-INDEXER-044, REQ-STREAM-INDEXER-046

### VAL-STREAM-INDEXER-042

Run a deterministic adaptive-planning fixture whose configured switch criteria
are met during planning.

**Pass condition:** the adaptive realization begins with directional PCA,
switches deterministically to DCBC at a reproducible boundary, preserves the
selected built-in direction across that switch, and continues through the same
finalized partition-hierarchy abstraction without caller-interactive algorithm
selection.

**Traces to:** REQ-STREAM-INDEXER-034, REQ-STREAM-INDEXER-035,
REQ-STREAM-INDEXER-044, REQ-STREAM-INDEXER-045, REQ-STREAM-INDEXER-046

### VAL-STREAM-INDEXER-043

Repeat the same adaptive switch-triggering fixture twice with identical logical
input, replay order, settings, and deterministic dependency behavior.

**Pass condition:** both runs choose the same PCA-to-DCBC switch boundary and,
after switching, do not revert from DCBC back to directional PCA later in the
same planning flow.

**Traces to:** REQ-STREAM-INDEXER-026, REQ-STREAM-INDEXER-046,
REQ-STREAM-INDEXER-047

### VAL-STREAM-INDEXER-044

Inspect the repository verification artifacts for the built-in planning matrix
and adaptive targeted cases.

**Pass condition:** algorithm-agnostic fixtures continue to cover supported
built-in realization-and-direction combinations as a matrix where compatible,
while the adaptive no-switch and switch-trigger behaviors are covered by
separate targeted cases rather than being omitted.

**Traces to:** REQ-STREAM-INDEXER-030, REQ-STREAM-INDEXER-033,
REQ-STREAM-INDEXER-044, REQ-STREAM-INDEXER-046

### VAL-STREAM-INDEXER-045

Materialize one carried-forward finalized hierarchy through the child-summary
policy surface and through an adapted canonical-embedding policy.

**Pass condition:** both paths remain valid public materialization routes, and
the canonical-policy path does not require callers to adopt descendant-aware
policy logic when they do not need it.

**Traces to:** REQ-STREAM-INDEXER-048, REQ-STREAM-INDEXER-050

### VAL-STREAM-INDEXER-046

Materialize a hierarchy whose children represent different descendant counts
through the built-in exact-centroid child-summary policy.

**Pass condition:** each parent summary equals the descendant-count-weighted
centroid of the carried-forward child summaries rather than an unweighted mean.

**Traces to:** REQ-STREAM-INDEXER-048, REQ-STREAM-INDEXER-049

### VAL-STREAM-INDEXER-047

Repeat exact-centroid materialization on the same finalized hierarchy and child
summary inputs.

**Pass condition:** the built-in exact-centroid summary policy produces the same
parent summary embeddings deterministically across repeated runs.

**Traces to:** REQ-STREAM-INDEXER-049

### VAL-STREAM-INDEXER-048

Construct the built-in planning path using the spherical-k-means realization,
once in a supported `Divisive` configuration and once in a supported
`Agglomerative` configuration.

**Pass condition:** both constructions succeed without a caller-implemented
planning factory, both require explicit spherical-k-means settings, and both
produce finalized hierarchies that can drive the existing final-materialization
contract.

**Traces to:** REQ-STREAM-INDEXER-011, REQ-STREAM-INDEXER-031,
REQ-STREAM-INDEXER-032, REQ-STREAM-INDEXER-041

### VAL-STREAM-INDEXER-049

Resolve published indexing profile `0.1.0` through the crate's convenience
surface.

**Pass condition:** the crate exposes a published profile version selector, the
selected `0.1.0` profile resolves successfully, and its declared crate-owned
runtime knobs match the published spherical-k-means, balanced-range packing,
greedy-pack hierarchy, and exact-centroid summary bundle, including the pinned
Euclidean hierarchy metric plus the published spherical-k-means initialization
policy, iteration limit, convergence tolerance, requested cluster count, and
random seed values.

**Traces to:** REQ-STREAM-INDEXER-051, REQ-STREAM-INDEXER-056

### VAL-STREAM-INDEXER-050

Attempt to resolve an unknown published indexing profile version.

**Pass condition:** the crate fails explicitly and does not silently substitute
another published profile.

**Traces to:** REQ-STREAM-INDEXER-052

### VAL-STREAM-INDEXER-051

Run the same deterministic indexing fixture twice through published indexing
profile `0.1.0`.

**Pass condition:** both runs realize the same effective crate-owned planning,
packing, and summary behavior and produce the same deterministic final result.

**Traces to:** REQ-STREAM-INDEXER-053, REQ-STREAM-INDEXER-056

### VAL-STREAM-INDEXER-052

Resolve published indexing profile `0.2.0` through the crate's convenience
surface.

**Pass condition:** the crate exposes a published profile version selector, the
selected `0.2.0` profile resolves successfully, and its declared crate-owned
runtime knobs match the published divisive directional-PCA bundle, including
its preserved exact-centroid summary policy, preserved bottom-up final
materialization behavior, requested cluster count, random seed, retained
dimension count, variance exponent, temperature, minimum input count, minimum
effective rank, and minimum cumulative variance.

**Traces to:** REQ-STREAM-INDEXER-051, REQ-STREAM-INDEXER-058, REQ-STREAM-INDEXER-059, REQ-STREAM-INDEXER-060

### VAL-STREAM-INDEXER-053

Run the same deterministic indexing fixture twice through published indexing
profile `0.2.0`.

**Pass condition:** both runs realize the same effective crate-owned planning
and summary behavior and produce the same deterministic final result without
substituting `0.1.0` behavior.

**Traces to:** REQ-STREAM-INDEXER-053, REQ-STREAM-INDEXER-058, REQ-STREAM-INDEXER-059, REQ-STREAM-INDEXER-060

### VAL-STREAM-INDEXER-054

Resolve both published indexing profile `0.1.0` and published indexing profile
`0.2.0` through the convenience surface in the same revision.

**Pass condition:** both profile versions remain explicitly resolvable, and
selecting `0.2.0` does not mutate the published behavior declared for `0.1.0`.

**Traces to:** REQ-STREAM-INDEXER-056, REQ-STREAM-INDEXER-058

### VAL-STREAM-INDEXER-055

Resolve published indexing profile `0.2.1` through the crate's convenience
surface.

**Pass condition:** the crate rejects `0.2.1` as an unsupported published
profile version.

**Traces to:** REQ-STREAM-INDEXER-058

### VAL-STREAM-INDEXER-056

Resolve published indexing profile `0.3.0` through the crate's convenience
surface.

**Pass condition:** the crate exposes a published profile version selector, the
selected `0.3.0` profile resolves successfully, and its declared crate-owned
runtime knobs match the published divisive directional-PCA bundle, including
its preserved exact-centroid summary policy, preserved bottom-up final
materialization behavior, requested cluster count, random seed, retained-axis
policy, allocation policy, binning policy, cluster-cardinality mode, variance
exponent, temperature, minimum input count, minimum effective rank, and minimum
cumulative variance.

**Traces to:** REQ-STREAM-INDEXER-051, REQ-STREAM-INDEXER-058, REQ-STREAM-INDEXER-061, REQ-STREAM-INDEXER-062, REQ-STREAM-INDEXER-063

### VAL-STREAM-INDEXER-057

Run the same deterministic indexing fixture twice through published indexing
profile `0.3.0`.

**Pass condition:** both runs realize the same effective crate-owned planning
and summary behavior and produce the same deterministic final result without
substituting `0.2.0` behavior.

**Traces to:** REQ-STREAM-INDEXER-053, REQ-STREAM-INDEXER-058, REQ-STREAM-INDEXER-061, REQ-STREAM-INDEXER-062, REQ-STREAM-INDEXER-063

### VAL-STREAM-INDEXER-058

Resolve published indexing profiles `0.1.0`, `0.2.0`, and `0.3.0` through the
convenience surface in the same revision.

**Pass condition:** all published profile versions remain explicitly
resolvable, selecting `0.3.0` does not mutate the published behavior declared
for `0.1.0` or `0.2.0`, and published profile `0.2.0` retains its declared
requested cluster count of `2`, retained dimension count of `1`, and exact-`K`
cardinality mode.

**Traces to:** REQ-STREAM-INDEXER-056, REQ-STREAM-INDEXER-058, REQ-STREAM-INDEXER-060, REQ-STREAM-INDEXER-061, REQ-STREAM-INDEXER-062, REQ-STREAM-INDEXER-063

### VAL-STREAM-INDEXER-059

Run a deterministic recursive or divisive planning fixture that forces at least
one hierarchy-planning unit to remain active long enough for multiple
`InProgress` observer updates to be emitted before the enclosing planning pass
completes.

Capture the observer stream for the relevant `HierarchyPlanning { stage }`
phase.

**Pass condition:** before planning-pass completion, the observer stream shows
all of the following:

- a declared planning-unit kind for the recursive phase
- repeated `InProgress` updates for the same current planning unit with
  monotonically increasing `current_unit_elapsed`
- at least one later update whose `current_partition_path` or equivalent unit
  descriptor changes because work moved to a different partition, or whose
  `completed_unit_count` advances because one planning unit completed
- no requirement for downstream callers to infer that state transition from
  free-form log text

**Traces to:** REQ-STREAM-INDEXER-022, REQ-STREAM-INDEXER-023, REQ-STREAM-INDEXER-064

### VAL-STREAM-INDEXER-060

Run a deterministic recursive or divisive planning fixture that discovers
multiple subpartitions and completes multiple planning units.

Capture the `HierarchyPlanning { stage }` observer updates and inspect the
recursive planning detail fields.

**Pass condition:** the observer stream exposes, or explicitly marks as
unavailable, the recursive planning fields required by the design for current
unit identity and discovered work. When those fields are available,
`discovered_unit_count`, `completed_unit_count`, and the aggregate partition or
planner counters are monotonic non-decreasing, and a downstream caller can
distinguish "still working the same partition" from "advanced to another
partition" without guessing from elapsed time alone.

**Traces to:** REQ-STREAM-INDEXER-022, REQ-STREAM-INDEXER-039, REQ-STREAM-INDEXER-064

### VAL-STREAM-INDEXER-061

Resolve published indexing profile `0.3.1` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.3.0` directional-PCA bundle except for requested cluster count `128`.

**Traces to:** REQ-STREAM-INDEXER-065, REQ-STREAM-INDEXER-069

### VAL-STREAM-INDEXER-062

Resolve published indexing profile `0.3.2` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.3.0` directional-PCA bundle except for requested cluster count `32`.

**Traces to:** REQ-STREAM-INDEXER-065, REQ-STREAM-INDEXER-070

### VAL-STREAM-INDEXER-063

Resolve published indexing profile `0.3.3` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.3.0` directional-PCA bundle except that it selects quantile binning.

**Traces to:** REQ-STREAM-INDEXER-065, REQ-STREAM-INDEXER-071

### VAL-STREAM-INDEXER-064

Resolve published indexing profile `0.3.4` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.3.0` directional-PCA bundle except that it selects fixed PC1-only splitting.

**Traces to:** REQ-STREAM-INDEXER-065, REQ-STREAM-INDEXER-072

### VAL-STREAM-INDEXER-065

Resolve published indexing profile `0.3.5` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.3.0` directional-PCA bundle except that it selects centroid-weighted
allocation.

**Traces to:** REQ-STREAM-INDEXER-065, REQ-STREAM-INDEXER-073

### VAL-STREAM-INDEXER-066

Resolve published indexing profile `0.3.6` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.3.0` directional-PCA bundle except that it caps retained axes at `2`.

**Traces to:** REQ-STREAM-INDEXER-065, REQ-STREAM-INDEXER-074

### VAL-STREAM-INDEXER-067

Resolve published indexing profile `0.3.7` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.3.0` directional-PCA bundle except that it caps retained axes at `3`.

**Traces to:** REQ-STREAM-INDEXER-065, REQ-STREAM-INDEXER-075

### VAL-STREAM-INDEXER-068

Resolve published indexing profile `0.3.8` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.3.0` directional-PCA bundle except that it raises minimum cumulative
variance to `0.5`.

**Traces to:** REQ-STREAM-INDEXER-065, REQ-STREAM-INDEXER-076

### VAL-STREAM-INDEXER-069

Resolve published indexing profile `0.3.9` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.3.0` directional-PCA bundle except that it raises minimum effective rank to
`2`.

**Traces to:** REQ-STREAM-INDEXER-065, REQ-STREAM-INDEXER-077

### VAL-STREAM-INDEXER-070

Resolve published indexing profile `0.3.10` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.3.0` directional-PCA bundle except that it restores exact cardinality mode.

**Traces to:** REQ-STREAM-INDEXER-065, REQ-STREAM-INDEXER-078

### VAL-STREAM-INDEXER-071

Resolve `0.3.0` and the full experimental `0.3.x` ladder through the
convenience surface in the same revision.

**Pass condition:** the experimental profiles remain explicitly resolvable and
selecting them does not mutate the declared mapping of `0.3.0`.

**Traces to:** REQ-STREAM-INDEXER-065, REQ-STREAM-INDEXER-067

### VAL-STREAM-INDEXER-072

Resolve the full published profile set, including the experimental `0.3.x`
ladder, more than once under identical conditions.

**Pass condition:** profile resolution is deterministic for each version and the
experiment ladder remains compatible with sequential comparative execution.

**Traces to:** REQ-STREAM-INDEXER-053, REQ-STREAM-INDEXER-068

### VAL-STREAM-INDEXER-073

Resolve published indexing profile `0.4.0` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and matches the
quantile-baseline directional-PCA bundle previously published as `0.3.3`.

**Traces to:** REQ-STREAM-INDEXER-079, REQ-STREAM-INDEXER-080

### VAL-STREAM-INDEXER-074

Resolve published indexing profile `0.4.1` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.4.0` directional-PCA bundle except for requested cluster count `128`.

**Traces to:** REQ-STREAM-INDEXER-080, REQ-STREAM-INDEXER-081

### VAL-STREAM-INDEXER-075

Resolve published indexing profile `0.4.2` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.4.0` directional-PCA bundle except for requested cluster count `32`.

**Traces to:** REQ-STREAM-INDEXER-080, REQ-STREAM-INDEXER-082

### VAL-STREAM-INDEXER-076

Resolve published indexing profile `0.4.3` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.4.0` directional-PCA bundle except that it selects fixed PC1-only splitting.

**Traces to:** REQ-STREAM-INDEXER-080, REQ-STREAM-INDEXER-083

### VAL-STREAM-INDEXER-077

Resolve published indexing profile `0.4.4` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.4.0` directional-PCA bundle except that it selects centroid-weighted
allocation while preserving quantile binning.

**Traces to:** REQ-STREAM-INDEXER-080, REQ-STREAM-INDEXER-084

### VAL-STREAM-INDEXER-078

Resolve published indexing profile `0.4.5` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.4.0` directional-PCA bundle except that it caps retained axes at `2` while
preserving quantile binning.

**Traces to:** REQ-STREAM-INDEXER-080, REQ-STREAM-INDEXER-085

### VAL-STREAM-INDEXER-079

Resolve published indexing profile `0.4.6` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.4.0` directional-PCA bundle except that it caps retained axes at `3` while
preserving quantile binning.

**Traces to:** REQ-STREAM-INDEXER-080, REQ-STREAM-INDEXER-086

### VAL-STREAM-INDEXER-080

Resolve published indexing profile `0.4.7` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.4.0` directional-PCA bundle except that it raises minimum cumulative
variance to `0.5` while preserving quantile binning.

**Traces to:** REQ-STREAM-INDEXER-080, REQ-STREAM-INDEXER-087

### VAL-STREAM-INDEXER-081

Resolve published indexing profile `0.4.8` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.4.0` directional-PCA bundle except that it raises minimum effective rank to
`2` while preserving quantile binning.

**Traces to:** REQ-STREAM-INDEXER-080, REQ-STREAM-INDEXER-088

### VAL-STREAM-INDEXER-082

Resolve published indexing profile `0.4.9` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
`0.4.0` directional-PCA bundle except that it restores exact cardinality mode
while preserving quantile binning.

**Traces to:** REQ-STREAM-INDEXER-080, REQ-STREAM-INDEXER-089

### VAL-STREAM-INDEXER-083

Resolve `0.4.0` and the full experimental `0.4.x` ladder through the
convenience surface in the same revision.

**Pass condition:** the experimental profiles remain explicitly resolvable and
selecting them does not mutate the declared mapping of `0.4.0`.

**Traces to:** REQ-STREAM-INDEXER-079, REQ-STREAM-INDEXER-080

### VAL-STREAM-INDEXER-084

Resolve the full published profile set, including both the `0.3.x` and `0.4.x`
ladders, more than once under identical conditions.

**Pass condition:** profile resolution is deterministic for each version and
the parallel experiment ladders remain compatible with sequential comparative
execution.

**Traces to:** REQ-STREAM-INDEXER-079, REQ-STREAM-INDEXER-080

### VAL-STREAM-INDEXER-085

Resolve published indexing profile `0.4.1` through the convenience surface
under a block-size target whose branch materializability bound is smaller than
the profile's requested fanout.

**Pass condition:** published-profile construction fails explicitly and the
error reports a configured fanout conflict rather than silently clipping the
request.

**Traces to:** REQ-STREAM-INDEXER-090, REQ-STREAM-INDEXER-091

### VAL-STREAM-INDEXER-086

Run published indexing profile `0.4.1` under a configuration whose requested
fanout is compatible with the configured materializability bound, but on data
that does not provide enough represented children to realize a larger
partition-local fanout.

**Pass condition:** execution remains allowed and any reduction caused by
insufficient represented children is treated as emergent runtime behavior, not
as a configuration conflict.

**Traces to:** REQ-STREAM-INDEXER-092

### VAL-STREAM-INDEXER-087

Resolve published indexing profile `0.3.1` under the same conflicting
configured materializability conditions used for `VAL-STREAM-INDEXER-085`.

**Pass condition:** the legacy profile retains its existing behavior and does
not inherit the `0.4.x` fail-fast configured-conflict rule.

**Traces to:** REQ-STREAM-INDEXER-093

### VAL-STREAM-INDEXER-088

Resolve published indexing profile `0.5.0` through the convenience surface.

**Pass condition:** the selected profile resolves successfully and preserves the
same tree-construction settings, emitted block-topology contract, and ordinary
uncompressed branch-entry representation as `0.4.0`.

**Traces to:** REQ-STREAM-INDEXER-094, REQ-STREAM-INDEXER-095

### VAL-STREAM-INDEXER-089

Resolve published indexing profile `0.5.1` through the convenience surface.

**Pass condition:** the selected profile resolves successfully, preserves the
`0.5.0` topology contract, and changes only the authored non-leaf branch-entry
representation to EBCP `pca-rot-f32le`.

**Traces to:** REQ-STREAM-INDEXER-096, REQ-STREAM-INDEXER-101,
REQ-STREAM-INDEXER-102

### VAL-STREAM-INDEXER-090

Resolve published indexing profile `0.5.2` through the convenience surface.

**Pass condition:** the selected profile resolves successfully, preserves the
`0.5.0` topology contract, and changes only the authored non-leaf branch-entry
representation to EBCP `pca-rot-delta-f32le`.

**Traces to:** REQ-STREAM-INDEXER-097, REQ-STREAM-INDEXER-101,
REQ-STREAM-INDEXER-102

### VAL-STREAM-INDEXER-091

Resolve published indexing profile `0.5.3` through the convenience surface.

**Pass condition:** the selected profile resolves successfully, preserves the
`0.5.0` topology contract, uses EBCP `pca-rot-delta-uq`, and assigns uniform
per-dimension bit widths of `12`, `8`, and `6` on the root, interior, and
lowest routing non-leaf levels respectively.

**Traces to:** REQ-STREAM-INDEXER-098, REQ-STREAM-INDEXER-101,
REQ-STREAM-INDEXER-102

### VAL-STREAM-INDEXER-092

Resolve published indexing profile `0.5.4` through the convenience surface.

**Pass condition:** the selected profile resolves successfully, preserves the
`0.5.0` topology contract, uses EBCP `pca-rot-delta-vbq`, and preserves the
same total per-level bit budget that `0.5.3` would have used at the same level
and dimensionality while redistributing those bits by variance.

**Traces to:** REQ-STREAM-INDEXER-099, REQ-STREAM-INDEXER-101,
REQ-STREAM-INDEXER-102

### VAL-STREAM-INDEXER-092b

Resolve published indexing profile `0.5.5` through the convenience surface.

**Pass condition:** the selected profile resolves successfully, preserves the
`0.5.0` topology contract, uses EBCP `ambient-delta-uq`, assigns uniform
per-dimension bit widths of `12`, `8`, and `6` on the root, interior, and
lowest routing non-leaf levels respectively, and emits no rotation metadata.

**Traces to:** REQ-STREAM-INDEXER-100, REQ-STREAM-INDEXER-101,
REQ-STREAM-INDEXER-102

### VAL-STREAM-INDEXER-093

Resolve the full published profile set, including the `0.4.x` and `0.5.x`
ladders, more than once under identical conditions.

**Pass condition:** profile resolution remains deterministic for each version,
the `0.5.x` ladder remains explicitly addressable alongside earlier ladders,
and selecting any `0.5.x` profile does not mutate the declared mapping of
`0.5.0` or any earlier published profile.

**Traces to:** REQ-STREAM-INDEXER-094, REQ-STREAM-INDEXER-095

### VAL-STREAM-INDEXER-094

Resolve published indexing profile `0.6.0` through the convenience surface.

**Pass condition:** the selected profile resolves successfully, preserves the
same directional-PCA planning parameters and ordinary uncompressed non-leaf
branch-entry representation as `0.5.0`, and advertises the new opt-in
fanout-capped topology contract.

**Traces to:** REQ-STREAM-INDEXER-103, REQ-STREAM-INDEXER-104

### VAL-STREAM-INDEXER-095

Run published indexing profiles `0.5.0` and `0.6.0` under the same large
block-size target on a dataset whose first `64`-way split leaves child
partitions larger than `64` items.

**Pass condition:** `0.5.0` preserves its earlier uncapped fanout behavior,
while `0.6.0` recursively subdivides until every emitted non-leaf block has at
most `cluster_count` children.

**Traces to:** REQ-STREAM-INDEXER-104, REQ-STREAM-INDEXER-110,
REQ-STREAM-INDEXER-111

### VAL-STREAM-INDEXER-096

Resolve published indexing profile `0.6.1` through the convenience surface.

**Pass condition:** the selected profile resolves successfully, preserves the
`0.6.0` fanout-capped topology contract, and changes only the authored non-leaf
branch-entry representation to EBCP `pca-rot-f32le`.

**Traces to:** REQ-STREAM-INDEXER-105, REQ-STREAM-INDEXER-111,
REQ-STREAM-INDEXER-112

### VAL-STREAM-INDEXER-097

Resolve published indexing profile `0.6.2` through the convenience surface.

**Pass condition:** the selected profile resolves successfully, preserves the
`0.6.0` fanout-capped topology contract, and changes only the authored non-leaf
branch-entry representation to EBCP `pca-rot-delta-f32le`.

**Traces to:** REQ-STREAM-INDEXER-106, REQ-STREAM-INDEXER-111,
REQ-STREAM-INDEXER-112

### VAL-STREAM-INDEXER-098

Resolve published indexing profile `0.6.3` through the convenience surface.

**Pass condition:** the selected profile resolves successfully, preserves the
`0.6.0` fanout-capped topology contract, uses EBCP `pca-rot-delta-uq`, and
assigns uniform per-dimension bit widths of `12`, `8`, and `6` on the root,
interior, and lowest routing non-leaf levels respectively.

**Traces to:** REQ-STREAM-INDEXER-107, REQ-STREAM-INDEXER-111,
REQ-STREAM-INDEXER-112

### VAL-STREAM-INDEXER-099

Resolve published indexing profile `0.6.4` through the convenience surface.

**Pass condition:** the selected profile resolves successfully, preserves the
`0.6.0` fanout-capped topology contract, uses EBCP `pca-rot-delta-vbq`, and
preserves the same total per-level bit budget that `0.6.3` would have used at
the same level and dimensionality while redistributing those bits by variance.

**Traces to:** REQ-STREAM-INDEXER-108, REQ-STREAM-INDEXER-111,
REQ-STREAM-INDEXER-112

### VAL-STREAM-INDEXER-099b

Resolve published indexing profile `0.6.5` through the convenience surface.

**Pass condition:** the selected profile resolves successfully, preserves the
`0.6.0` fanout-capped topology contract, uses EBCP `ambient-delta-uq`, assigns
uniform per-dimension bit widths of `12`, `8`, and `6` on the root, interior,
and lowest routing non-leaf levels respectively, and emits no rotation
metadata.

**Traces to:** REQ-STREAM-INDEXER-109, REQ-STREAM-INDEXER-111,
REQ-STREAM-INDEXER-112

### VAL-STREAM-INDEXER-100

Resolve the full published profile set, including the `0.4.x`, `0.5.x`, and
`0.6.x` ladders, more than once under identical conditions.

**Pass condition:** profile resolution remains deterministic for each version,
the `0.6.x` ladder remains explicitly addressable alongside earlier ladders,
and selecting any `0.6.x` profile does not mutate the declared mapping of
`0.5.0` or any earlier published profile.

**Traces to:** REQ-STREAM-INDEXER-103, REQ-STREAM-INDEXER-111

### VAL-STREAM-INDEXER-101

Resolve published indexing profiles `0.6.5` and `0.7.0` through the convenience
surface.

**Pass condition:** the selected `0.7.0` profile resolves successfully and
exposes the same directional-PCA planning parameters, fanout-capped topology
contract, and ambient `12/8/6` non-leaf branch encoding contract as `0.6.5`.

**Traces to:** REQ-STREAM-INDEXER-114

### VAL-STREAM-INDEXER-102

Materialize a representative root under published indexing profile `0.7.0`.

**Pass condition:** the authored branch block uses EBCP `ambient-delta-uq`,
emits no rotation metadata, and uses the expected root-level uniform
quantization width.

**Traces to:** REQ-STREAM-INDEXER-114, REQ-STREAM-INDEXER-115

### VAL-STREAM-INDEXER-103

Resolve the full published profile set, including `0.7.0`, more than once under
identical conditions.

**Pass condition:** `0.7.0` resolution remains deterministic, the profile
remains explicitly addressable alongside the `0.6.x` ladder, and selecting
`0.7.0` does not mutate the declared mapping of `0.6.5` or any earlier
published profile.

**Traces to:** REQ-STREAM-INDEXER-113, REQ-STREAM-INDEXER-115

### VAL-STREAM-INDEXER-104

Resolve a published indexing profile, derive a caller-owned variant from it,
and execute that derived variant through the caller-visible surface.

**Pass condition:** the derived variant is accepted without requiring the caller
to restate the originating profile's branch-encoding, summary, or
materialization policies manually.

**Traces to:** REQ-STREAM-INDEXER-116

### VAL-STREAM-INDEXER-105

Start from published indexing profile `0.7.0`, override only `cluster_count`,
and materialize a representative fixture.

**Pass condition:** the run preserves `0.7.0`'s non-overridden semantics,
including `ambient-delta-uq` branch encoding and the existing fanout-capped
topology/materialization behavior, while reflecting the caller-supplied
effective planning cluster count.

**Traces to:** REQ-STREAM-INDEXER-117

### VAL-STREAM-INDEXER-106

Resolve a published indexing profile before and after executing one or more
caller-derived variants from it.

**Pass condition:** the original published profile still resolves
deterministically to the same declared mapping, and the derived variants are
not treated as new implicit published versions.

**Traces to:** REQ-STREAM-INDEXER-118

### VAL-STREAM-INDEXER-107

Execute a caller-derived variant whose overridden `cluster_count` conflicts with
the applicable materializability bound or profile compatibility rules.

**Pass condition:** the derived-profile path fails explicitly under the same
class of compatibility and materializability checks as the version-selected
published-profile path, rather than bypassing validation.

**Traces to:** REQ-STREAM-INDEXER-119
