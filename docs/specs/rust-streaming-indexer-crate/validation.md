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
consumes the shared streaming clustering contract, references the built-in DCBC
and directional-PCA specification packages for their owned clustering
algorithms, and does not define a new clustering contract locally or depend on
a retired legacy batch-oriented indexing crate/specification package as part of
its normative boundary.

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
selecting directional PCA and supplying caller-provided directional-PCA
settings supported by that realization.

**Pass condition:** the runtime can be created without a caller-implemented
factory, uses the built-in arithmetic-mean canonical policy unless another
canonical policy is explicitly supplied, requires explicit built-in algorithm
selection, and consumes the caller-provided directional-PCA settings supported
by that realization.

**Traces to:** REQ-STREAM-INDEXER-011, REQ-STREAM-INDEXER-013,
REQ-STREAM-INDEXER-014, REQ-STREAM-INDEXER-031, REQ-STREAM-INDEXER-032

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
caller-visible replayed hierarchy-building work.

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
using the finalized partition hierarchy to drive bottom-up assembly.

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
still flows through the shared streaming clustering contract rather than an
older batch-only clustering boundary.

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
invalid hybrid-planning configuration, canonical-embedding failure,
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
`lexongraph-directional-pca`, and each built-in planning path delegates through
the shared streaming clustering contract rather than reimplementing either
algorithm locally.

**Traces to:** REQ-STREAM-INDEXER-011, REQ-STREAM-INDEXER-019

### VAL-STREAM-INDEXER-027

Inspect the repository verification artifacts for the new crate.

**Pass condition:** executable automated tests exist that realize this
validation surface.

**Traces to:** REQ-STREAM-INDEXER-030

### VAL-STREAM-INDEXER-028

Construct the streaming indexer through its built-in planning-selection
surface, selecting directional PCA in one case and DCBC in another.

**Pass condition:** callers can choose either built-in planning algorithm
through the indexer API without implementing a custom planning factory, each
selection requires caller-supplied settings for the chosen algorithm, attempts
to omit the required algorithm choice or required settings fail explicitly, and
the rest of the streaming runtime contract remains unchanged.

**Traces to:** REQ-STREAM-INDEXER-011, REQ-STREAM-INDEXER-014,
REQ-STREAM-INDEXER-024, REQ-STREAM-INDEXER-031, REQ-STREAM-INDEXER-032

### VAL-STREAM-INDEXER-029

Inspect the repository verification artifacts for algorithm-agnostic built-in
planning behavior.

**Pass condition:** algorithm-agnostic built-in-path planning and assembly
cases whose fixtures are compatible with both built-in algorithms'
caller-supplied settings
are realized as a matrix over both built-in planning realizations rather than
favoring one built-in algorithm, while unsupported or algorithm-specific
behavior remains covered by separate targeted tests.

**Traces to:** REQ-STREAM-INDEXER-030, REQ-STREAM-INDEXER-033

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
algorithm are explicit, and the resulting planning behavior is deterministic.

**Traces to:** REQ-STREAM-INDEXER-036

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

- at least one non-trivial training pass
- leaf materialization
- first-layer clustering
- at least one higher-layer clustering step
- at least one layer materialization step

Capture the observer stream and inspect the per-phase progress payloads.

**Pass condition:** for each exercised phase, the recorded
`phase_total_unit_count`, `completed_unit_count`, and `remaining_unit_count`
match the phase-specific semantics defined in the design, and a downstream can
derive materially useful progress such as "processed X / Y, Z remaining"
without inferring semantics from elapsed time alone.

**Traces to:** REQ-STREAM-INDEXER-039
