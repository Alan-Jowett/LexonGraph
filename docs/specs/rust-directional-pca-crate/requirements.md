<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Directional PCA Crate Requirements

## Status

Draft specification for a Rust crate that realizes streaming directional-PCA
clustering for LexonGraph through the shared streaming clustering contract.

## Scope

This document specifies the crate-level requirements for a Rust crate that:

- preserves the directional-PCA partitioning algorithm described in
  `docs/arch/Directional PCA tree.md`
- conforms to the shared trainer/classifier contract defined by
  `crates/lexongraph-streaming-clustering`
- removes the obsolete block-store-backed single-layer boundary in favor of a
  native embedding-streaming surface

This document does not define recursive tree construction, block loading,
representative-embedding derivation from stored blocks, centroid block
materialization, or alternate clustering algorithms.

## Terminology

In this spec package, `streaming directional-PCA trainer` means a concrete
implementation of `StreamingClusterTrainer` whose completed passes realize the
directional-PCA partitioning mechanics owned by this crate.

`Pass dataset order` means the ordered embedding sequence observed across all
batches ingested before one `finish_pass()` call.

`Directional parameters` means the algorithm-specific controls for retained PCA
dimensionality, variance exponent `gamma`, temperature `tau`, and any explicit
stability thresholds retained by this crate boundary.

`Exact-K realizability` means that one completed pass can be partitioned into
exactly `K` stable, non-empty clusters under the crate's documented
directional-PCA mechanics, where `K` is the shared `cluster_count` from the
streaming contract.

## Requirements

### REQ-DPCA-STREAM-001

The repository shall define a dedicated Rust crate for streaming directional
PCA at `crates/lexongraph-directional-pca`.

### REQ-DPCA-STREAM-002

The crate shall remain subordinate to:

- `docs/arch/Directional PCA tree.md` for the algorithm's directional-PCA
  intent, allocation rationale, and stated stabilizers
- `docs/specs/rust-streaming-clustering-crate` for the shared streaming
  trainer/classifier contract
- `docs/specs/rust-pca-crate` for PCA behavior consumed by this crate

If those sources appear to conflict, the streaming trait specification is
authoritative for the shared contract surface, this specification package is
authoritative for the crate boundary and parameter-domain rules it defines, and
the architecture note is authoritative for the directional-PCA algorithmic
intent preserved here.

### REQ-DPCA-STREAM-003

The crate shall expose a trainer implementation conforming to
`StreamingClusterTrainer` and a classifier implementation conforming to
`StreamingClusterClassifier`.

### REQ-DPCA-STREAM-004

The public crate boundary shall be native to streamed embeddings.

This revision shall not retain the obsolete public block-ID plus `BlockStore`
execution boundary, representative-embedding derivation from loaded blocks, or
block-store-specific result and error ownership.

The public crate boundary shall also not imply or require
implementation-owned resident-memory retention or replay buffering of the
completed dataset as part of conformant execution.

Implementation-owned working state may scale with the currently processed
batch/chunk size and fixed configuration terms. When directional-PCA is used by
the v2 streaming indexer planning path, planner-managed out-of-core state may
also scale with the completed-pass dataset size so long as the caller-visible
replay lifecycle remains unchanged.

### REQ-DPCA-STREAM-005

Trainer construction shall accept the shared `StreamingClusteringConfig` plus
typed directional parameters.

The shared `cluster_count` is a hard requirement for the observable clustering
surface, and the directional parameters shall include at minimum:

- retained PCA dimension count or equivalent truncation control
- cluster-cardinality mode for exact-`K` versus underfull-success behavior
- variance exponent `gamma`
- temperature `tau`
- explicit stability or eligibility thresholds retained by the scaled-down
  streaming crate boundary

For this crate boundary, `gamma` shall be finite and non-negative. The
architecture note's discussion of example or heuristic `gamma` ranges is
non-normative for this crate's accepted configuration domain.

### REQ-DPCA-STREAM-006

This scaled-down revision shall not define a directional-PCA-specific balance
policy beyond exact-K cluster realization.

If caller-provided shared balance constraints are present, the trainer shall
reject them explicitly through the shared invalid-configuration category rather
than silently ignore them or claim unsupported balancing behavior.

### REQ-DPCA-STREAM-007

The trainer shall validate malformed streamed input explicitly, including at
minimum:

- wrong embedding dimensionality
- non-finite embedding values
- empty completed passes

This revision does not require the crate to accept zero-norm embeddings unless
the finalized design or downstream cosine-aware consumers explicitly demand that
constraint.

### REQ-DPCA-STREAM-008

The crate shall preserve protocol-significant pass dataset order and shall not
treat permutation of the completed-pass embedding sequence as semantically
equivalent input.

### REQ-DPCA-STREAM-009

The trainer shall support caller-driven multi-pass refinement over the same
logical dataset through repeated `ingest_batch()` / `finish_pass()` cycles.

The crate shall not hide additional caller-invisible passes or replace the
shared pass lifecycle with an independent iteration API.

When a later caller-visible pass must revisit the logical dataset, the replay
shall come from the caller re-streaming that dataset rather than from
implementation-owned full-pass retention or pass snapshots.

Per-batch transient working state remains conformant. Planner-managed
out-of-core state may persist across caller-visible passes when the surrounding
v2 streaming surface explicitly provides that contract.

When deterministic approximate quantile binning is selected, the crate shall
complete that planning step from bounded streaming summaries gathered during the
caller-visible pass rather than hidden retained-axis rescans or sort phases.

### REQ-DPCA-STREAM-010

After the first completed pass establishes the logical dataset for one training
run, each later completed pass shall represent the same logical dataset in the
same pass dataset order.

If a later pass differs in observed count or ordered embedding content from the
first completed pass, the trainer shall fail explicitly rather than claim
conformant refinement of the same run.

### REQ-DPCA-STREAM-011

For each completed pass, the crate shall realize directional-PCA partitioning
by using the repository PCA crate rather than redefining PCA decomposition
behavior independently.

Conformant execution shall use the PCA crate through streaming or mergeable
sufficient-statistics behavior and shall not rely on a full-pass convenience
fitting path over a materialized embedding collection.

Transient implementation-owned working memory proportional to the current chunk
is conformant; resident-memory materialization of the full completed pass is
not. Planner-managed out-of-core state is conformant when it preserves the
shared caller-visible replay contract.

Deterministic approximate quantile planning shall prefer bounded streaming
summaries over spill-and-replay retained-axis ordering for this revision.

### REQ-DPCA-STREAM-012

The crate shall expose explicit directional-PCA policy selection rather than
silently mutating one scoring path.

At minimum, the public typed directional parameters shall name:

- retained-axis policy
- allocation policy
- binning policy
- cluster-cardinality mode

For the centroid-weighted allocation policy, the crate shall compute per-axis
allocation scores using both centroid-direction coefficients and
explained-variance information.

The conformant centroid-weighted score shall be equivalent in effect to
`|alpha_i| * lambda_i^gamma`, where `gamma` is an explicit typed parameter.

### REQ-DPCA-STREAM-013

For the centroid-weighted allocation policy, the crate shall convert the
per-axis scores into per-axis resolution using a temperature-controlled
allocation rule over the shared hard cluster target `K`, with deterministic
rounding and correction behavior.

For the eigenvalue log-bit allocation policy, the crate shall:

- allocate split budget from eigenvalue-only log-weight semantics rather than
  centroid-direction coefficients
- permit weak axes to receive zero split bits
- deterministically realize per-axis bin counts from that sparse bit budget

### REQ-DPCA-STREAM-014

The conformant default binning policy shall remain quantile binning over the
retained PCA coordinates.

When quantile binning is selected, the crate may realize those cuts through a
deterministic approximate quantile algorithm rather than exact retained-axis
ordering, provided the same replay order, algorithm version, and input dataset
produce the same cuts, partition assignments, and downstream partition result.

When density-valley binning is selected, the crate shall instead place cuts by
selecting deepest density valleys along each participating retained PCA axis
rather than by quantiles or by a largest-gap proxy.

### REQ-DPCA-STREAM-014A

The deterministic approximate quantile realization for this revision shall use
Greenwald-Khanna summaries rather than randomized compaction or unspecified
merge order.

### REQ-DPCA-STREAM-014B

The crate shall document approximate quantile semantics in terms of a
deterministic rank or boundary error contract rather than exact quantile
equality, while preserving deterministic tie handling at selected cut values.

### REQ-DPCA-STREAM-015

By default, the crate shall fail explicitly when the completed-pass sequence
required by the documented directional-PCA mechanics cannot realize exact-K
partitioning, including at minimum:

- first-pass `Observed N < K`
- invalid or infeasible directional parameters
- inability of the realized directional-PCA partition to produce exactly `K`
  stable, non-empty clusters without changing the documented algorithmic
  semantics

The crate shall not silently adapt the partitioning behavior merely to force an
exact-K outcome.

The only conformant exception is duplicate-collapse recovery: if the realized
partition-ready directional-PCA partition under-realizes `K` solely because duplicate or
otherwise indistinguishable members collapse into too few populated cells, the
crate shall apply the documented deterministic duplicate-refinement rule rather
than fail.

When an explicit underfull-success cardinality mode is selected, the crate may
instead succeed with the best deterministic realized count `R` such that
`1 <= R <= K` after applying the same primary partitioning and
duplicate-refinement mechanics.

### REQ-DPCA-STREAM-016

Each completed pass shall return a deterministic `PassReport` containing:

- `observed_count`
- `requested_cluster_count`
- `quality_metric`
- `balance_metric`
- quality and balance metric directions
- explicit readiness status

For `AnalysisOnly` passes, `realized_cluster_count` and stable cluster
identifiers may be absent.

For `PartitionReady` passes, `realized_cluster_count` and stable cluster
identifiers shall be present.

The balance metric shall be zero when no explicit balance constraints are
configured.

This determinism requirement includes completed passes that exercised the
duplicate-refinement fallback.

### REQ-DPCA-STREAM-017

The observable contract shall preserve stable cluster identifiers across
partition-ready completed passes and in the final classifier surface.

### REQ-DPCA-STREAM-018

After caller-directed training completion, the crate shall produce a
deterministic classifier that:

- assigns each valid embedding to exactly one cluster ID in `[0, R)`, where
  `R` is the realized cluster count from the final pass and `1 <= R <= K`
- rejects malformed embeddings through the shared malformed-input error category
- does not require the original dataset after classifier production

If training completed through duplicate refinement, classifier assignment shall
remain deterministic for the resulting stable cluster IDs.

### REQ-DPCA-STREAM-019

Invalid configuration, invalid state transitions, unsatisfiable exact-K
constraints, and malformed input shall be surfaced through the shared streaming
error categories with deterministic terminal-error behavior for illegal
lifecycle transitions.

### REQ-DPCA-STREAM-020

The public API surface shall remain trimmed to the minimal behavior needed to
realize the native streaming directional-PCA contract.

Helpers, types, and tests that only support the retired block-store boundary
shall not remain as dead compatibility ballast.

### REQ-DPCA-STREAM-022

The crate shall detect when populated-cell shortfall arises from
duplicate-collapse, including the all-identical-embedding case and later stages
where retained PCA coordinates remain indistinguishable for the collapsed
members.

### REQ-DPCA-STREAM-023

When duplicate-collapse detection triggers and first-pass `Observed N >= K`, the
crate shall refine only the collapsed duplicate members with a deterministic
non-geometric tie-break that is stable for the same pass dataset order, thereby
realizing exactly `K` stable non-empty clusters without randomness.

### REQ-DPCA-STREAM-024

The duplicate-refinement fallback shall not be used for:

- invalid or infeasible configuration
- first-pass `Observed N < K`
- malformed input
- exact-K failures not attributable to duplicate-collapse

Those cases shall continue to fail explicitly through the existing shared error
categories.

### REQ-DPCA-STREAM-025

The crate shall support an explicit opt-in adaptive retained-axis policy as an
equivalent truncation control.

When that policy is selected, the crate shall retain all eligible PCA axes
deterministically rather than requiring a fixed retained-dimension count.
Eligibility shall remain bounded by the realized PCA output and the
effective-rank guard preserved by this crate boundary.

### REQ-DPCA-STREAM-026

The crate shall support an explicit opt-in density-valley binning policy for
retained PCA coordinates.

If per-axis resolution assigns `b_i` bins to axis `i`, the density-valley
policy shall select `b_i - 1` deterministic valley cut points on that axis to
form `b_i` high-density intervals rather than using quantile cuts.

### REQ-DPCA-STREAM-027

The adaptive retained-axis policy and density-valley binning policy shall be
opt-in only.

Absent explicit selection of those policies, the default directional-PCA path
shall continue to use fixed retained-dimension truncation and quantile binning.

### REQ-DPCA-STREAM-028

The crate shall support the compatible mixed-policy combinations needed by the
published directional-PCA experiment ladder rather than restricting conformance
to only two hard-coded policy bundles.

Each selected policy shall retain its documented semantics when combined with a
different retained-axis, allocation, or binning policy, subject to the explicit
invariants of that selected policy such as power-of-two `K` for eigenvalue
log-bit allocation.

### REQ-DPCA-STREAM-029

A conformant implementation shall realize indexing and training with
implementation-owned memory and scratch/storage bounded independently of the
full completed-pass dataset size `N`.

Allowed implementation-owned growth may depend on:

- the currently processed batch/chunk size
- embedding dimensionality
- requested cluster count and retained-axis configuration
- other fixed documented configuration parameters

The crate shall not require retaining the completed dataset, retained-coordinate
tables for all members, replay logs, or other resident in-memory state whose
footprint scales with the full dataset size `N`.

Planner-managed out-of-core state for the active replay phase or current
planning subproblem is conformant when it reduces peak resident memory without
replacing caller-visible replay across passes.

When that state is mmap-backed, conformant execution actively bounds resident
pages for inactive regions through a cross-platform abstraction that maps to
valid target-native primitives rather than relying solely on passive kernel
eviction.

### REQ-DPCA-STREAM-030

When evaluating a concrete implementation against this revision, any design
that requires implementation-owned storage scaling with the full completed-pass
dataset size `N` is non-conformant even if the public API is batch-streaming
shaped.

Transient storage scaling with the currently processed chunk, or documented
planner-managed out-of-core state subordinate to caller-visible replay, is
conformant.

### REQ-DPCA-STREAM-021

The repository shall include executable verification artifacts covering both:

- this crate's realization of the directional-PCA mechanics preserved by this
  specification package
- this crate's conformance to the shared streaming clustering contract,
  including the opt-in conformance-helper surface

## Out of Scope

This crate does not define or own:

- recursive tree construction across multiple directional-PCA layers
- block loading or block validation
- representative-embedding derivation from branch or leaf blocks
- centroid block persistence or block-store integration
- adaptive compressed-size estimation
- alternate clustering algorithms
- undocumented compatibility wrappers for the retired block-store API

## Relationship to Other Specifications

This document bridges the directional-PCA architecture note and the shared
streaming clustering trait package for one concrete crate boundary.

It intentionally narrows the public surface from the previous block-store-backed
single-layer crate contract to the scaled-down native streaming boundary needed
by the current repository direction.
