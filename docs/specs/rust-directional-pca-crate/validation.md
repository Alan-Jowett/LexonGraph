<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Directional PCA Crate Validation

## Status

Draft validation specification for a Rust crate that realizes streaming
directional-PCA clustering through the shared LexonGraph streaming clustering
contract.

## Validation Scope

These validation entries define the conformance surface for the streaming
directional-PCA crate. They cover both:

- realization of the directional-PCA mechanics preserved from
  `docs/arch/Directional PCA tree.md`
- conformance to the shared streaming trainer/classifier contract

## Validation Entries

### VAL-DPCA-STREAM-001

Inspect the repository artifacts for the crate.

**Pass condition:** the repository includes a crate at
`crates/lexongraph-directional-pca` and this spec package.

**Traces to:** REQ-DPCA-STREAM-001

### VAL-DPCA-STREAM-002

Inspect the crate's public surface and specification references.

**Pass condition:** the crate exposes concrete implementations of
`StreamingClusterTrainer` and `StreamingClusterClassifier`, remains subordinate
to the directional-PCA architecture note, the shared streaming clustering
contract, and the PCA crate specification, and does not expose the retired
public block-store boundary.

**Traces to:** REQ-DPCA-STREAM-002, REQ-DPCA-STREAM-003, REQ-DPCA-STREAM-004

### VAL-DPCA-STREAM-003

Construct a trainer with valid shared configuration and valid directional
parameters.

**Pass condition:** construction succeeds deterministically and preserves hard
`K`, dimensionality, deterministic seed behavior, and the supplied directional
parameters.

**Traces to:** REQ-DPCA-STREAM-005

### VAL-DPCA-STREAM-004

Construct a trainer with caller-provided shared balance constraints.

**Pass condition:** construction fails explicitly through the shared
invalid-configuration category rather than silently ignoring unsupported balance
configuration.

**Traces to:** REQ-DPCA-STREAM-006, REQ-DPCA-STREAM-019

### VAL-DPCA-STREAM-005

Exercise one pass with multiple batches whose concatenated order is known.

**Pass condition:** `finish_pass()` realizes exactly one caller-visible
directional-PCA pass over the concatenated pass dataset order and does not
perform hidden extra passes or require implementation-owned full-dataset
resident-memory retention to do so. Planner-managed out-of-core state for the
active replay phase or current planning subproblem is permitted when it does
not change the observable pass boundary. The pass may report `AnalysisOnly`
status when exact partitioning requires later replay passes. Internal parallel
projection is conformant only when it does not add hidden passes or alter the
caller-visible replay-order semantics of the pass.

**Traces to:** REQ-DPCA-STREAM-008, REQ-DPCA-STREAM-009

### VAL-DPCA-STREAM-006

Complete a second pass whose observed count or ordered embedding content differs
from the first completed pass.

**Pass condition:** continuation fails explicitly before claiming conformant
refinement of the same training run, and the continuity check does not depend
on crate-owned full-pass replay buffers.

**Traces to:** REQ-DPCA-STREAM-010

### VAL-DPCA-STREAM-007

Exercise malformed streamed input, including wrong dimensionality, non-finite
values, and an empty completed pass.

**Pass condition:** each case fails explicitly through the shared
malformed-input surface.

**Traces to:** REQ-DPCA-STREAM-007, REQ-DPCA-STREAM-019

### VAL-DPCA-STREAM-008

Inspect the execution path over a representative conformant fixture.

**Pass condition:** the directional-PCA pass is realized by the repository PCA
crate surface through streaming or mergeable sufficient-statistics behavior
rather than an undocumented independent PCA implementation or a full-pass
`fit(...)` convenience path. When a replay-driven phase uses an already-fixed
transform, any internal parallel projection still flows through the documented
PCA crate projection surface.

**Traces to:** REQ-DPCA-STREAM-011

### VAL-DPCA-STREAM-008A

Run the same replay-driven fixture through serial and parallel projection modes
for `PlanCuts`, `CountCells`, and `RealizePartition`.

**Pass condition:** pass reports, derived partition structure, and final
classifier-visible realization are identical between serial and parallel
execution of the same ordered replay input.

**Traces to:** REQ-DPCA-STREAM-011A, REQ-DPCA-STREAM-011C

### VAL-DPCA-STREAM-008B

Exercise a replay-driven batch where worker completion is deliberately staggered
away from caller replay order.

**Pass condition:** planner observations, cell-summary accumulation, and
partition-realization outputs still reflect canonical replay order rather than
worker completion order.

**Traces to:** REQ-DPCA-STREAM-010, REQ-DPCA-STREAM-011B

### VAL-DPCA-STREAM-009

Use a legacy-path fixture with known retained PCA coordinates, centroid
direction, and explained variance.

**Pass condition:** the realized per-axis scores reflect both directional
coefficients and explained variance according to the configured `gamma`.

**Traces to:** REQ-DPCA-STREAM-012

### VAL-DPCA-STREAM-010

Exercise both supported allocation modes.

**Pass condition:** the centroid-weighted path follows the documented
temperature-controlled allocation rule, and the eigenvalue-log-bit path
allocates eigenvalue-driven split bits while permitting at least one weak
eligible axis to receive zero bits.

**Traces to:** REQ-DPCA-STREAM-012, REQ-DPCA-STREAM-013

### VAL-DPCA-STREAM-011

Use fixtures whose retained PCA coordinates are unevenly distributed.

**Pass condition:** when quantile binning is selected, the crate uses quantile
binning through the documented deterministic realization rather than equal-
width binning or an undocumented heuristic, and when density-valley binning is
selected, the crate chooses cuts through deepest density valleys rather than by
a largest-gap proxy.

**Traces to:** REQ-DPCA-STREAM-012, REQ-DPCA-STREAM-014

### VAL-DPCA-STREAM-011A

Run the same quantile-planning fixture twice with identical ordered input.

**Pass condition:** the Greenwald-Khanna quantile realization yields identical
cut values, deterministic tie behavior at selected cuts, and identical
partition-ready output.

**Traces to:** REQ-DPCA-STREAM-014, REQ-DPCA-STREAM-014A

### VAL-DPCA-STREAM-011B

Exercise a fixture with uneven retained-axis coordinates under the documented
approximation contract for this revision.

**Pass condition:** the realized cuts satisfy the documented deterministic rank
or boundary error contract rather than exact-quantile equality.

**Traces to:** REQ-DPCA-STREAM-014B

### VAL-DPCA-STREAM-012

Exercise three exact-K boundary fixtures:

- first-pass `Observed N < K`
- infeasible directional parameters
- a realized directional-PCA partition that cannot produce exactly `K` stable,
  non-empty clusters without changing the documented semantics

**Pass condition:** in the default exact-`K` mode, each case fails explicitly
rather than silently forcing an exact-K outcome.

**Traces to:** REQ-DPCA-STREAM-015, REQ-DPCA-STREAM-019

### VAL-DPCA-STREAM-013

Inspect pass reports across at least two passes.

**Pass condition:** each report exposes deterministic `observed_count`,
`requested_cluster_count`, `quality_metric`, `balance_metric`, fixed metric
directions, and readiness status. `AnalysisOnly` reports may omit
`realized_cluster_count` and stable cluster IDs; `PartitionReady` reports
include them. When no explicit balance constraints are configured,
`balance_metric` is zero.

**Traces to:** REQ-DPCA-STREAM-016, REQ-DPCA-STREAM-017

### VAL-DPCA-STREAM-014

Exercise multiple completed passes on a fixture whose internal group ordering
would otherwise change.

**Pass condition:** partition-ready pass reports and classifier assignments
preserve stable externally visible cluster IDs across partition-ready passes.

**Traces to:** REQ-DPCA-STREAM-017

### VAL-DPCA-STREAM-015

Complete training and exercise classifier assignment on valid and malformed
embeddings.

**Pass condition:** the classifier deterministically maps each valid embedding
to exactly one cluster ID in `[0, R)`, where `R` is the realized cluster count,
rejects malformed embeddings through the shared malformed-input category, and
does not require replay of the original training dataset.

**Traces to:** REQ-DPCA-STREAM-018, REQ-DPCA-STREAM-019

### VAL-DPCA-STREAM-016

Exercise invalid configuration and illegal lifecycle transitions.

**Pass condition:** failures are surfaced deterministically through the shared
streaming error categories, and illegal lifecycle transitions place the trainer
in terminal error state where required by the shared contract.

**Traces to:** REQ-DPCA-STREAM-019

### VAL-DPCA-STREAM-017

Inspect the crate's public surface and executable verification artifacts after
the streaming rework.

**Pass condition:** public helpers, types, and tests that existed only to
support the retired public block-store boundary have been removed, and the
retained artifacts are limited to the scaled-down native streaming crate
boundary.

**Traces to:** REQ-DPCA-STREAM-020

### VAL-DPCA-STREAM-018

Run the shared streaming clustering conformance helpers against the crate.

**Pass condition:** the crate passes the shared lifecycle, metric,
malformed-input, determinism, and cluster-ID continuity checks.

**Traces to:** REQ-DPCA-STREAM-021

### VAL-DPCA-STREAM-019

Run directional-PCA-focused executable tests for the crate's observable
boundary.

**Pass condition:** executable tests exist for pass ordering, cross-pass
continuity, PCA reuse, scoring, allocation, quantile binning, exact-K failure,
stable cluster IDs, classifier assignment, and dead-code cleanup of the retired
block-store boundary. Quantile-binning coverage includes deterministic
Greenwald-Khanna cut derivation and repeated-run reproducibility.

**Traces to:** REQ-DPCA-STREAM-021

### VAL-DPCA-STREAM-020

Use a fixture where all embeddings in the completed pass are identical and
`Observed N >= K`.

**Pass condition:** the crate does not fail exact-K solely because geometric
partitioning collapsed the pass; instead it deterministically realizes exactly
`K` non-empty clusters through duplicate refinement.

**Traces to:** REQ-DPCA-STREAM-015, REQ-DPCA-STREAM-022, REQ-DPCA-STREAM-023

### VAL-DPCA-STREAM-021

Use a fixture where one populated cell contains duplicate members and the pass
under-realizes `K` only because of that collapsed duplicate cell.

**Pass condition:** the crate refines only the collapsed duplicate members,
preserves the rest of the primary partition, and realizes exactly `K`
non-empty clusters.

**Traces to:** REQ-DPCA-STREAM-023, REQ-DPCA-STREAM-024

### VAL-DPCA-STREAM-022

Repeat duplicate-collapse fixtures across at least two passes with identical
ordered input.

**Pass condition:** partition-ready pass reports and classifier assignments
preserve stable cluster IDs and deterministic assignments across
partition-ready passes that exercise duplicate refinement.

**Traces to:** REQ-DPCA-STREAM-016, REQ-DPCA-STREAM-017, REQ-DPCA-STREAM-018

### VAL-DPCA-STREAM-023

Use a fixture where exact-K remains infeasible for a reason other than
duplicate-collapse.

**Pass condition:** the crate still fails explicitly and does not invoke the
duplicate-refinement fallback.

**Traces to:** REQ-DPCA-STREAM-024

### VAL-DPCA-STREAM-024

Exercise the crate with the explicit adaptive retained-axis policy selected and
again with the default fixed retained-dimension policy.

**Pass condition:** the adaptive policy deterministically retains all eligible
axes admitted by the realized PCA output and effective-rank guard, while the
default path remains fixed retained-dimension truncation.

**Traces to:** REQ-DPCA-STREAM-025, REQ-DPCA-STREAM-027

### VAL-DPCA-STREAM-025

Use a fixture whose retained PCA coordinates contain a clear deep valley.

**Pass condition:** when density-valley binning is selected, the crate chooses
deterministic valley cut points rather than quantile cuts.

**Traces to:** REQ-DPCA-STREAM-026

### VAL-DPCA-STREAM-026

Inspect the explicit default path after the opt-in policy additions.

**Pass condition:** absent explicit selection of the new policies, the crate
still uses fixed retained-dimension truncation and quantile binning.

**Traces to:** REQ-DPCA-STREAM-014, REQ-DPCA-STREAM-027

### VAL-DPCA-STREAM-027

Construct trainers using mixed retained-axis, allocation, and binning policy
combinations that are relied on by published experiment profiles.

**Pass condition:** the crate accepts those compatible mixed-policy
configurations rather than rejecting them as unsupported policy bundles.

**Traces to:** REQ-DPCA-STREAM-028

### VAL-DPCA-STREAM-028

Use a fixture with adaptive retained-axis selection, eigenvalue log-bit
allocation, and quantile binning.

**Pass condition:** construction succeeds and the realized partition follows the
quantile-binning semantics rather than silently switching to density-valley
cuts.

**Traces to:** REQ-DPCA-STREAM-013, REQ-DPCA-STREAM-014, REQ-DPCA-STREAM-028

### VAL-DPCA-STREAM-029

Use a fixture with adaptive retained-axis selection, centroid-weighted
allocation, and density-valley binning.

**Pass condition:** construction succeeds, the crate preserves
centroid-weighted allocation semantics, and any required retained-axis cap is
applied deterministically rather than by rejecting the mixed policy outright.

**Traces to:** REQ-DPCA-STREAM-013, REQ-DPCA-STREAM-014, REQ-DPCA-STREAM-028

### VAL-DPCA-STREAM-030

Use a fixture with fixed retained-axis selection, eigenvalue log-bit
allocation, and density-valley binning.

**Pass condition:** construction succeeds when the selected `K` satisfies the
eigenvalue log-bit invariants, and the crate preserves the selected fixed-axis
boundary rather than silently switching to the adaptive retained-axis policy.

**Traces to:** REQ-DPCA-STREAM-013, REQ-DPCA-STREAM-014, REQ-DPCA-STREAM-028

### VAL-DPCA-STREAM-031

Inspect or execute a conformant implementation while exercising passes whose
full logical dataset is larger than one chunk.

**Pass condition:** implementation-owned resident memory remains bounded by
current chunk size, PCA/statistical summaries, fixed configuration terms, and
any planner-managed out-of-core state for the active replay phase or current
planning subproblem rather than by full completed-pass dataset size `N`.
For mmap-backed realizations, inactive mapped regions are actively managed
through a cross-platform abstraction whose backend is valid on the exercised
target so resident pages stay within the intended bound.

**Traces to:** REQ-DPCA-STREAM-029

### VAL-DPCA-STREAM-032

Assess an implementation whose public API is batch-streaming shaped but whose
normal execution retains or spills the full pass.

**Pass condition:** the implementation is classified as non-conformant under
this revision.

**Traces to:** REQ-DPCA-STREAM-030
