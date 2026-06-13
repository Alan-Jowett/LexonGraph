<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Clustering Evaluator Crate Validation

## Status

Draft validation specification for a Rust crate that evaluates candidate
streaming clustering implementations for LexonGraph at the leaf-partition
boundary.

## Validation Scope

These validation entries define the conformance surface for the new streaming
clustering evaluator crate. They cover benchmark-profile definition, candidate
execution, leaf membership scoring, comparative reporting, determinism checking,
explicit deferred research-goal handling, and corpus sourcing through both
inline fixtures and block-store-backed references.

## Validation Entries

### VAL-STREAM-EVAL-001

Inspect the repository artifacts for the new crate.

**Pass condition:** the repository includes a crate at
`crates/lexongraph-streaming-clustering-evaluator` and this spec package.

**Traces to:** REQ-STREAM-EVAL-001

### VAL-STREAM-EVAL-002

Inspect the crate's public surface and specification references.

**Pass condition:** the crate remains subordinate to the clustering research
documents and the shared streaming clustering contract while defining an
evaluator-owned benchmark boundary rather than a broader candidate API, and the
crate's leaf-stage scope limits only what it may claim directly rather than
redefining the end-state requirements from `docs/research/clustering.md`.

**Traces to:** REQ-STREAM-EVAL-002, REQ-STREAM-EVAL-004

### VAL-STREAM-EVAL-003

Run the evaluator with a benchmark profile and at least two registered
candidates.

**Pass condition:** one executable campaign evaluates the candidates through a
shared leaf-stage benchmark profile and emits comparative outputs without
requiring an algorithm-specific candidate API. At least one such campaign uses
one repository-owned reusable non-fixture candidate alongside another
registered candidate.

**Traces to:** REQ-STREAM-EVAL-003, REQ-STREAM-EVAL-005, REQ-STREAM-EVAL-007, REQ-STREAM-EVAL-038

### VAL-STREAM-EVAL-004

Inspect the candidate registration surface.

**Pass condition:** each candidate enters through an adapter or factory that
constructs a trainer conforming to the shared streaming clustering contract.

**Traces to:** REQ-STREAM-EVAL-004

### VAL-STREAM-EVAL-005

Inspect one benchmark profile definition.

**Pass condition:** the profile fixes corpus identities, streaming pass inputs,
classifier-side probe workloads, the declared source mode for each workload, the
leaf model, metric declarations, gate declarations, comparative ranking
weights, deferred research-goal records, any declared later-phase workload or
artifact identities that section-4 must carry forward, and reproducibility
metadata for one campaign.

**Traces to:** REQ-STREAM-EVAL-006

### VAL-STREAM-EVAL-006

Run the same candidate twice under the same benchmark profile.

**Pass condition:** the evaluator compares pass-report sequences and
classifier-side probe outputs across repeated executions and reports whether the
observable results are deterministic.

**Traces to:** REQ-STREAM-EVAL-008, REQ-STREAM-EVAL-011

### VAL-STREAM-EVAL-007

Run a conforming deterministic candidate under a benchmark profile whose
reproducibility metadata is fixed.

**Pass condition:** the resulting provenance manifest deterministically records
benchmark profile identity, corpus identities, candidate identity, clustering
configuration, seed policy, software identity, floating-point profile metadata,
and hardware-profile metadata, plus any declared source-reference identities
needed to reproduce block-store-backed corpus inputs.

**Traces to:** REQ-STREAM-EVAL-008

### VAL-STREAM-EVAL-008

Execute a candidate through at least one multi-batch pass, final classifier
production, full-corpus assignment replay, and one classifier probe workload.

**Pass condition:** the evaluator drives the candidate through trainer
construction, pass ingestion, `finish_pass()`, training completion, classifier
production, evaluator-owned leaf membership materialization, and
classifier-side probing according to the benchmark profile for both inline
fixture workloads and block-store-backed referenced workloads, including
zip-archive-backed sources resolved through the repository overlay and zip
block-store implementations.

**Traces to:** REQ-STREAM-EVAL-009, REQ-STREAM-EVAL-010, REQ-STREAM-EVAL-022, REQ-STREAM-EVAL-024

### VAL-STREAM-EVAL-009

Run the evaluator on a benchmark profile with declared strict alignment and leaf
size `L`.

**Pass condition:** the evaluator verifies exact final cluster occupancy,
complete coverage, one-cluster-per-entity assignment, and absence of empty
declared clusters from the leaf membership artifact.

**Traces to:** REQ-STREAM-EVAL-017

### VAL-STREAM-EVAL-010

Run the evaluator on a benchmark profile with deterministic synthetic padding.

**Pass condition:** the evaluator distinguishes real from synthetic entities in
the leaf membership artifact, enforces exact final occupancy against the padded
evaluated set, and excludes synthetic entities from externally reported
locality and compression metrics, while also reporting whether synthetic
padding is concentrated into the minimum possible number of final clusters
permitted by the deterministic procedure. Synthetic padding identities are
stably tagged, do not collide with real-entity identities in the evaluated
corpus, and are not misreported as real benchmark members in externally visible
evaluator entity listings.

**Traces to:** REQ-STREAM-EVAL-017, REQ-STREAM-EVAL-018

### VAL-STREAM-EVAL-011

Run the evaluator on a benchmark profile with exact nearest-neighbor ground
truth over real entities.

**Pass condition:** the evaluator computes same-leaf neighborhood coherence from
the leaf membership artifact without claiming same-or-sibling locality proof.

**Traces to:** REQ-STREAM-EVAL-019

### VAL-STREAM-EVAL-012

Run the evaluator on a benchmark profile with a declared local compression
method and global baseline.

**Pass condition:** the evaluator computes local per-cluster compression quality
versus the declared global real-dataset baseline from the leaf membership
artifact, reports the declared local-versus-global delta semantics, and emits
the per-cluster or per-bucket distribution needed to interpret that aggregate
comparison. The declared global baseline excludes synthetic padding entities.

**Traces to:** REQ-STREAM-EVAL-020

### VAL-STREAM-EVAL-013

Inspect one completed campaign report.

**Pass condition:** the result model distinguishes shared-contract prerequisite
checks, must-pass gates, and comparative metrics, and does not rank a gate
failing candidate as a surviving success.

**Traces to:** REQ-STREAM-EVAL-012

### VAL-STREAM-EVAL-014

Inspect the metric and gate declarations in a benchmark profile and the
corresponding campaign report.

**Pass condition:** each declared metric, gate, or deferred research-goal
record traces to one or more research goals and is tagged as direct, proxy, or
deferred. When a deferred record corresponds to a frozen section-1 benchmark
item, the report also identifies the later proof surface expected to discharge
it.

**Traces to:** REQ-STREAM-EVAL-013

### VAL-STREAM-EVAL-015

Inspect the emitted campaign artifacts for one benchmark execution.

**Pass condition:** the evaluator emits a machine-readable run report per
candidate, a machine-readable comparative campaign report, and a human-readable
scorecard summarizing pass/fail status, metric values, and survivor ranking.

**Traces to:** REQ-STREAM-EVAL-014

### VAL-STREAM-EVAL-016

Run the evaluator with an invalid benchmark profile and with a candidate that
returns a shared-contract failure.

**Pass condition:** failures are surfaced deterministically with a stable error
code and deterministic human-readable message and distinguish invalid evaluator
configuration, invalid or unresolved corpus-source references,
block-store-backed corpus-loading failures, zip-archive open or read failures,
and candidate-reported shared-contract failure. A failed section-4
candidate/configuration execution does not expose success-shaped completed
artifacts beyond the point of failure.

**Traces to:** REQ-STREAM-EVAL-015, REQ-STREAM-EVAL-022

### VAL-STREAM-EVAL-017

Run the evaluator with a candidate that fails one declared benchmark gate and
with a benchmark profile that includes at least one deferred research goal.

**Pass condition:** the resulting report distinguishes evaluator-owned gate
failure from incomplete or unsupported measurement due to a deferred research
requirement.

**Traces to:** REQ-STREAM-EVAL-015, REQ-STREAM-EVAL-021

### VAL-STREAM-EVAL-018

Inspect the benchmark profile and scorecard against the clustering research
documents.

**Pass condition:** research goals requiring full hierarchy, sibling structure,
routing, or durable index artifacts are recorded as deferred rather than
claimed as fully proven by the streaming clustering evaluator alone, the future
end-to-end evaluator is called out as a separate later line, and the deferred
status is presented as staged evidence toward `docs/research/clustering.md`
rather than as a narrowing of that parent requirement set.

**Traces to:** REQ-STREAM-EVAL-013, REQ-STREAM-EVAL-021

### VAL-STREAM-EVAL-019

Inspect repository verification artifacts for the new crate.

**Pass condition:** executable tests exist for benchmark-profile validation,
candidate execution, inline-fixture and block-store-backed corpus-source
handling, leaf membership materialization, occupancy/locality/compression
scoring, repeated-run determinism checks, comparative scorecard generation,
failure classification, deferred-goal reporting, and archive-backed overlay
resolution.

**Traces to:** REQ-STREAM-EVAL-016, REQ-STREAM-EVAL-022, REQ-STREAM-EVAL-023, REQ-STREAM-EVAL-024

### VAL-STREAM-EVAL-020

Run one campaign whose training passes, evaluation replay entities, and
classifier-side probes all resolve through block-store-backed corpus
references.

**Pass condition:** the evaluator consumes all three workload families through
the same declared scalable corpus-source model without requiring a monolithic
profile-embedded JSON corpus, including when those references are
zip-archive-backed and resolved through a temporary-filesystem-over-zip
overlay.

**Traces to:** REQ-STREAM-EVAL-024, REQ-STREAM-EVAL-025, REQ-STREAM-EVAL-026

### VAL-STREAM-EVAL-021

Run the evaluator with one small inline benchmark fixture and one functionally
equivalent block-store-backed benchmark fixture.

**Pass condition:** both source modes remain supported, and the resulting leaf
membership semantics and report semantics are equivalent apart from provenance
details specific to the referenced external corpus source, whether the
block-store-backed fixture is filesystem-root-backed or zip-archive-backed.

**Traces to:** REQ-STREAM-EVAL-014, REQ-STREAM-EVAL-022, REQ-STREAM-EVAL-023

### VAL-STREAM-EVAL-022

Inspect one benchmark profile using archive-backed scalable corpora.

**Pass condition:** the profile can declare a zip archive path plus root block
ID for a training pass, probe workload, or evaluation corpus without requiring
the user to declare a writable overlay directory.

**Traces to:** REQ-STREAM-EVAL-027, REQ-STREAM-EVAL-029

### VAL-STREAM-EVAL-023

Run one campaign whose training passes, evaluation replay entities, and probe
workloads are supplied through zip-archive-backed corpus references.

**Pass condition:** the evaluator resolves each referenced archive through a
higher-priority temporary writable filesystem layer over a lower-priority
immutable zip layer and successfully consumes the resulting overlay-backed
block-store view.

**Traces to:** REQ-STREAM-EVAL-028, REQ-STREAM-EVAL-029

### VAL-STREAM-EVAL-024

Exercise the reusable filesystem-over-zip overlay helper, if present.

**Pass condition:** new block creation through the helper lands in the mutable
filesystem layer without mutating the underlying zip archive and without
widening the parent `BlockStore` API.

**Traces to:** REQ-STREAM-EVAL-030

### VAL-STREAM-EVAL-025

Inspect the repository benchmark-suite artifacts and documentation for section-4
leaf-stage screening.

**Pass condition:** the repository defines a reproducible benchmark-suite
workflow that can materialize the benchmark profiles and supporting assets used
to compare candidate leaf-partition strategies, declares the metric contract and
top-10 fixed neighborhood size used for exact-neighbor ground truth in the
checked-in screening panel, rejects malformed suite-level configuration such as
empty `suite_id`, zero-valued positive-count controls, and empty profile sets,
and does not claim hierarchy-stage coverage.

**Traces to:** REQ-STREAM-EVAL-031, REQ-STREAM-EVAL-047

### VAL-STREAM-EVAL-026

Inspect the benchmark-suite corpus panel.

**Pass condition:** the checked-in suite defines stable benchmark identities for
the required corpus families relevant to leaf-stage screening, including a
real-world harvested corpus, a well-clustered synthetic corpus, a weak-cluster
or uniform corpus, an anisotropic or manifold corpus, a near-duplicate-heavy
corpus, and any deterministic scale-tier variants used for repeated
comparisons.

**Traces to:** REQ-STREAM-EVAL-032

### VAL-STREAM-EVAL-027

Inspect one corpus used for locality scoring together with its supporting
ground-truth artifact.

**Pass condition:** the exact-neighbor ground truth is deterministically tied to
the corpus identity, scale-tier identity, and metric contract, uses top-10
neighbors for the checked-in screening panel, excludes synthetic padding
entities from externally reported locality scoring, and rejects too-small
corpora plus cosine-metric zero-norm embeddings deterministically.

**Traces to:** REQ-STREAM-EVAL-033, REQ-STREAM-EVAL-050

### VAL-STREAM-EVAL-028

Exercise the real-world corpus harvesting workflow against repository-approved
block-store-backed source data.

**Pass condition:** the harvesting workflow deterministically extracts
embeddings and entity identities from the declared source, emits any declared
scale-tier subsets reproducibly, preserves the source identity needed to
reconstruct the harvested benchmark asset, rejects malformed metadata,
inadmissible embeddings, and underfilled retained-real-entity sets
deterministically, and the repository includes at least one checked-in
harvested benchmark asset produced by that workflow.

**Traces to:** REQ-STREAM-EVAL-034, REQ-STREAM-EVAL-049

### VAL-STREAM-EVAL-029

Inspect one large repository-managed benchmark corpus asset and execute one
campaign against it through the archive-backed corpus-source path.

**Pass condition:** the checked-in harvested corpus is stored in the git tree as
a `.zip` asset, the campaign consumes it directly from the archive-backed
declaration, and no manual pre-decompression step is required.

**Traces to:** REQ-STREAM-EVAL-035

### VAL-STREAM-EVAL-030

Run the section-4 leaf-stage screening workflow against at least one strict
alignment corpus, one deterministic-padding corpus, and one harvested real-world
corpus with at least two candidate strategies.

**Pass condition:** the resulting outputs include fixed-capacity invariant
outcomes, repeated-run determinism, same-leaf locality against top-10
exact-neighbor ground truth, local-versus-global compression gain, and the
distinct outcomes for strict alignment versus deterministic padding where both
are applicable, including deterministic rejection of non-divisible strict
alignment and impossible or degenerate deterministic-padding inputs, together
with the declared normalized build-cost comparison used to judge the
alignment-policy tradeoff.

**Traces to:** REQ-STREAM-EVAL-031, REQ-STREAM-EVAL-036, REQ-STREAM-EVAL-048

### VAL-STREAM-EVAL-031

Inspect one completed section-4 screening report spanning more than one corpus
scale tier.

**Pass condition:** the report includes the evaluated entity count or equivalent
corpus-size measure, the scale-tier identity, build time per vector or the
suite's declared equivalent normalized build-cost measure, and peak build
memory across more than one checked-in scale tier. If the experiment track
declares a later-phase loaded-index memory obligation that section-4 does not
directly measure, the report preserves that obligation as deferred rather than
omitting it.

**Traces to:** REQ-STREAM-EVAL-037

### VAL-STREAM-EVAL-032

Run the checked-in section-4 suite with each repository-owned registered
section-4 candidate.

**Pass condition:** each checked-in section-4 family candidate enters through
the shared streaming trainer/classifier contract and emits ordinary section-4
evaluator artifacts under a stable candidate identity.

**Traces to:** REQ-STREAM-EVAL-038, REQ-STREAM-EVAL-042

### VAL-STREAM-EVAL-033

Compare one evaluator-local fixture candidate and each repository-owned
registered candidate within the same campaign.

**Pass condition:** all compared candidates use the same evaluator-owned registration
and report model, and the repository-owned concrete candidate does not require
evaluator-private algorithm hooks.

**Traces to:** REQ-STREAM-EVAL-004, REQ-STREAM-EVAL-038, REQ-STREAM-EVAL-039, REQ-STREAM-EVAL-040

### VAL-STREAM-EVAL-034

Inspect the registered-candidate discovery surface and its CLI exposure.

**Pass condition:** the evaluator's ordinary candidate-listing surface includes
the full checked-in section-4 family candidate set, including
`pca-sort-exact-chunking`, `recursive-balanced-kmeans`,
`space-filling-curve-exact-chunking`, `graph-neighborhood-balance`,
`hybrid-coarse-rebalance`, `random-shuffle-exact-chunking`,
`directional-pca`, and `dcbc-streaming`, so each can be selected through the
same discovery path used for fixture candidates.

**Traces to:** REQ-STREAM-EVAL-042

### VAL-STREAM-EVAL-035

Run profiles that trigger known candidate-specific shared-contract limits for
the registered repository-owned candidates.

**Pass condition:** the evaluator reports explicit ordinary candidate outcomes,
rather than silently filtering candidates out, for at least directional-PCA
rejection of shared balance constraints and DCBC rejection of zero-norm
embeddings plus unsupported shared balance settings.

**Traces to:** REQ-STREAM-EVAL-041

### VAL-STREAM-EVAL-036

Inspect one checked-in section-4 experiment track contract together with one
derived benchmark profile.

**Pass condition:** the suite freezes the metric family, any transformed-metric
policy, the primary `leaf_size` and any declared sensitivity sizes, the
dimensionality contract, the alignment-policy family, the quantization or
compression baseline policy over real entities only, any declared search-target
threshold and beam-width policy carried forward to later routing phases, the
declared floating-point profile, the declared candidate-threading model, the
declared reduction-order strategy, and the declared hardware profile; the
artifacts also label which of those frozen items are measured directly during
section-4 versus deferred.

**Traces to:** REQ-STREAM-EVAL-006, REQ-STREAM-EVAL-031, REQ-STREAM-EVAL-043

### VAL-STREAM-EVAL-037

Inspect one section-4 campaign report and its deferred-goal records.

**Pass condition:** any frozen section-1 obligation that is not directly proven
at the leaf-stage boundary is preserved as an explicit deferred record rather
than omitted, including where applicable same-or-sibling locality targets,
routing targets or routing assumptions, beam-width policies or related routing-
study assumptions, bounded fanout or depth constraints, parent-summary
obligations, parent-summary metric or dispersion-contract obligations,
refinement-contract obligations, serialization or persisted-artifact
obligations, multi-thread reproducibility obligations beyond the direct
section-4 observable boundary, and any declared held-out query-set or later-
phase routing-workload identities.

**Traces to:** REQ-STREAM-EVAL-013, REQ-STREAM-EVAL-021, REQ-STREAM-EVAL-044

### VAL-STREAM-EVAL-038

Inspect the checked-in corpus panel and one locality-scored tiered corpus
family.

**Pass condition:** the suite defines stable small, medium, and large
scale-tier identities or deterministic nearest-practical equivalents, declares
the tier-growth rule for repeated comparison, ties each tier to its corpus and
any exact-neighbor ground-truth asset, and includes at least one checked-in
harvested real-world corpus family within that same tiered comparison surface.
For any corpus family intended to carry forward into later routing studies, the
same contract also declares held-out query-set identities, including at least
one such identity for a checked-in harvested real-world corpus family in the
first complete checked-in section-4 panel, and that preserved identity points
at a checked-in materialized asset.

**Traces to:** REQ-STREAM-EVAL-032, REQ-STREAM-EVAL-033, REQ-STREAM-EVAL-034, REQ-STREAM-EVAL-045

### VAL-STREAM-EVAL-039

Run a section-4 campaign in which one candidate fails a hard invariant gate for
one configuration.

**Pass condition:** the evaluator stops further comparative metric evaluation
for that candidate/configuration pair, emits deterministic failure-classified
artifacts without presenting a success-shaped completed result for the rejected
configuration, includes artifact-hygiene evidence that later comparative
metrics and success-shaped completion artifacts were not exposed after the
failing gate, and the comparative outputs still identify which candidates, if
any, survive to carry forward into later hierarchy-stage comparison.

**Traces to:** REQ-STREAM-EVAL-015, REQ-STREAM-EVAL-036, REQ-STREAM-EVAL-046

### VAL-STREAM-EVAL-040

Inspect one checked-in section-4 experiment track contract together with one
campaign report produced from that track.

**Pass condition:** the experiment track declares the exact build, locality,
compression, and deferred-routing metric roles, any transformed-metric
ordering-preservation obligation, the metric-contract consistency checks and
reported audit results, the compatible dispersion functional for any deferred
summary or refinement obligation, the threading model and deterministic
reduction-order strategy, and whether 1-thread versus N-thread bitwise
observable identity is measured directly or deferred.

**Traces to:** REQ-STREAM-EVAL-051

### VAL-STREAM-EVAL-041

Inspect one frozen section-4 contract that carries later-phase obligations and
the corresponding deferred-ledger records.

**Pass condition:** when the experiment track declares held-out query-set,
later routing-workload, hierarchy, summary, or persistence artifact identities,
the suite preserves those identities together with the later evaluation line
expected to consume each one even if section-4 execution does not directly use
them. For the first complete checked-in section-4 panel, that preserved set
includes at least one held-out query-set identity for a harvested real-world
corpus family expected to feed later routing phases, and the identity retains a
checked-in asset path.

**Traces to:** REQ-STREAM-EVAL-052

### VAL-STREAM-EVAL-042

Inspect one completed section-4 campaign with more than one surviving
candidate/configuration pair.

**Pass condition:** the workflow's carry-forward decision rule rejects hard-gate
failures first, ranks surviving candidates using same-leaf locality evidence,
declared compression benefit, and normalized build-cost evidence, and applies a
deterministic tie-break when survivors remain otherwise indistinguishable on
the declared comparison surface.

**Traces to:** REQ-STREAM-EVAL-053

### VAL-STREAM-EVAL-043

Inspect the checked-in canonical section-4 suite run artifacts.

**Pass condition:** the repository includes a machine-readable suite report, a
human-readable suite scorecard, and a human-readable survivor-decision summary
for the checked-in canonical section-4 run, and those artifacts remain
consistent with the current checked-in suite manifest and candidate identities.

**Traces to:** REQ-STREAM-EVAL-014, REQ-STREAM-EVAL-046, REQ-STREAM-EVAL-053
