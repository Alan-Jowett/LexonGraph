<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Clustering Evaluator Crate Requirements

## Status

Draft specification for a Rust crate that evaluates candidate streaming
clustering implementations for LexonGraph at the leaf-partition boundary.

## Scope

This document specifies the crate-level requirements for a new Rust crate that:

- provides a reusable executable benchmark harness for comparing candidate
  streaming clustering implementations as leaf-partition realizations
- reuses the shared trainer/classifier boundary defined by
  `docs/specs/rust-streaming-clustering-crate/`
- aligns scalable corpus consumption with the repository's existing block-store
  abstraction rather than requiring monolithic profile-embedded JSON datasets
- translates applicable intent from `docs/research/clustering.md` and
  `docs/research/clustering_plan.md` into an evaluator-owned benchmark contract

This document defines the evaluator boundary, benchmark contract, campaign
execution model, leaf-membership scoring surface, scorecard outputs, and
failure taxonomy. It does not require a concrete clustering algorithm and does
not redefine the shared streaming clustering contract.

## Terminology

In this spec package, `candidate` means one clustering implementation entered
into evaluation through the shared streaming clustering trainer/classifier
boundary.

`Benchmark profile` means the evaluator-owned description of the fixed corpora,
pass plan, probe workloads, leaf-model declarations, metric declarations,
gates, and ranking weights used for one comparative evaluation campaign.

`Corpus source` means the benchmark-declared mechanism by which the evaluator
obtains the embeddings and related entity identities for a workload. In this
revision the supported source families are inline fixture data,
filesystem-root-backed block-store corpus references, and zip-archive-backed
block-store corpus references resolved through an overlay of repository
block-store implementations.

`Evaluation campaign` means one execution of the evaluator over one benchmark
profile and one or more named candidates.

`Leaf size L` means the benchmark-declared target occupancy for each final
cluster when the evaluator treats the candidate's final clusters as would-be
LexonGraph leaves for that experiment.

`Alignment policy` means the benchmark-declared rule for handling datasets whose
real-item count is not divisible by `L`. In this revision the supported policy
families are strict alignment and deterministic synthetic padding.

`Leaf membership artifact` means the evaluator-owned materialization of final
entity-to-cluster assignments produced by replaying the benchmark corpus through
the candidate's finished classifier.

`Direct metric` means an evaluator result whose measured behavior is directly
observable from the shared streaming clustering boundary and the benchmark
fixtures.

`Proxy metric` means an evaluator result intended to approximate a research goal
whose full end-to-end LexonGraph property is not directly observable from the
shared streaming clustering boundary alone.

`Deferred research requirement` means one requirement from
`docs/research/clustering.md` that this evaluator revision records as
out-of-scope because proving it requires artifacts outside the shared streaming
clustering boundary.

## Requirements

### REQ-STREAM-EVAL-001

The repository shall define a dedicated Rust crate at
`crates/lexongraph-streaming-clustering-evaluator` that owns the reusable
streaming clustering leaf-partition evaluation boundary for LexonGraph.

### REQ-STREAM-EVAL-002

The new crate shall remain subordinate to:

- `docs/research/clustering.md` for the research goals motivating evaluation
- `docs/research/clustering_plan.md` for the intended comparative benchmark
  workflow
- `docs/specs/rust-streaming-clustering-crate/` for the shared candidate
  trainer/classifier contract
- `docs/specs/rust-block-storage-trait/` for the backend-neutral storage
  contract used by scalable corpus references

If those sources appear to conflict, the narrower evaluator scope remains
authoritative for this crate's boundary: the research documents are
authoritative for evaluation intent, and the shared streaming clustering
specification is authoritative for the candidate integration surface. The
block-storage trait specification is authoritative for the scalable external
corpus-loading contract.

### REQ-STREAM-EVAL-003

The crate shall provide a reusable executable benchmark harness, with a
supporting reusable library surface, for running comparative evaluations of one
or more candidate streaming clustering implementations as leaf-partition
realizations.

### REQ-STREAM-EVAL-004

Candidates shall plug into the evaluator through the existing shared streaming
clustering trainer/classifier contract. This revision shall not require a
broader candidate API than the boundary defined by
`docs/specs/rust-streaming-clustering-crate/`.

### REQ-STREAM-EVAL-005

The evaluator shall remain algorithm-neutral and shall not require any specific
clustering method, centroid model, update rule, distance function, or balance
policy realization beyond what is observable through the shared candidate
boundary and benchmark fixtures.

### REQ-STREAM-EVAL-006

The evaluator shall define a benchmark profile that fixes, for one evaluation
campaign:

- the corpus panel or equivalent named input datasets, including the declared
  corpus-source mode for each dataset or workload
- the candidate training pass plan and any held-out probe workloads, including
  whether each workload is supplied inline or through a block-store-backed
  corpus reference and, for archive-backed references, the declared zip archive
  path
- the leaf model, including target leaf size `L`, the relationship between
  observed item count `N`, target cluster count `K`, and expected occupancy, and
  the alignment policy
- metric declarations and whether each metric is direct or proxy
- must-pass gates and any comparative ranking weights
- deferred research-goal records for requirements that cannot be proven at this
  boundary
- deterministic execution-profile metadata needed to interpret reproducibility

### REQ-STREAM-EVAL-007

Within one evaluation campaign, all compared candidates shall be executed
against the same benchmark profile rather than candidate-specific benchmark
contracts.

### REQ-STREAM-EVAL-008

Each evaluation campaign shall emit a deterministic provenance manifest that
records at least:

- benchmark profile identity
- corpus identities
- source-reference identities needed to reproduce any block-store-backed corpus
  inputs used by the campaign
- candidate identity
- shared clustering configuration used for the candidate
- deterministic seed policy
- executable or crate version identity
- declared floating-point execution profile metadata
- declared hardware-profile metadata

### REQ-STREAM-EVAL-009

For each candidate run, the evaluator shall exercise the candidate through the
shared lifecycle by:

- constructing or obtaining a trainer through the shared candidate boundary
- executing the caller-driven streaming passes declared by the benchmark profile
  from the workload's declared corpus source, including archive-backed sources
  resolved through an overlay of repository block-store implementations
- obtaining pass reports from completed passes
- marking training complete when the benchmark profile requires it
- producing a classifier and running the declared classifier-side probe
  workloads from the workload's declared corpus source

### REQ-STREAM-EVAL-010

For each candidate run, after producing the final classifier, the evaluator
shall replay the benchmark corpus through that classifier and materialize an
evaluator-owned leaf membership artifact that assigns every evaluated entity to
exactly one final cluster, whether the evaluated corpus is declared inline in
the benchmark profile or supplied through a block-store-backed corpus
reference.

The leaf membership artifact shall be sufficient to drive evaluator-owned leaf
occupancy, coverage, locality, and compression checks without requiring the
candidate to expose a broader API than the shared streaming clustering
boundary.

### REQ-STREAM-EVAL-011

The evaluator shall support repeated identical executions of the same candidate
under the same benchmark profile and shall compare observable results for
determinism, including pass reports and classifier-side assignments over the
declared probe workloads.

### REQ-STREAM-EVAL-012

The evaluator shall distinguish, in its observable result model:

- shared-contract prerequisite checks
- benchmark must-pass gates
- comparative score metrics used only among candidates that pass the required
  gates

The evaluator shall not rank a candidate as a successful survivor if it fails a
must-pass gate.

### REQ-STREAM-EVAL-013

Each evaluator-owned metric, gate, or deferred research-goal record shall trace
to one or more motivating research goals from `docs/research/clustering.md` or
`docs/research/clustering_plan.md` and shall declare whether that research-goal
coverage is:

- direct
- proxy
- deferred because the research goal cannot be proven at this boundary

### REQ-STREAM-EVAL-014

The evaluator shall emit:

- a machine-readable per-candidate run report
- a machine-readable comparative campaign report
- a human-readable scorecard summarizing pass/fail status, metric values, and
  comparative ranking for surviving candidates

These outputs remain evaluator-owned and source-neutral: changing a workload
from inline fixture data to a block-store-backed corpus reference shall change
input acquisition and provenance detail, not the semantic meaning of the
reported evaluator results.

### REQ-STREAM-EVAL-015

The evaluator shall surface deterministic structured failures that distinguish
at least:

- invalid evaluator or benchmark-profile configuration
- invalid or unresolved corpus-source references
- explicit block-store or corpus-content loading failure encountered while
  consuming referenced corpora
- explicit zip-archive open or read failure encountered while consuming
  archive-backed referenced corpora
- explicit temporary writable-layer creation failure encountered while resolving
  archive-backed referenced corpora
- candidate-reported shared-contract failure
- evaluator-owned gate failure
- incomplete or unsupported measurement caused by a deferred research
  requirement

### REQ-STREAM-EVAL-016

The repository shall include executable verification artifacts that realize the
validation plan for the streaming clustering evaluator crate, including both
inline-fixture and block-store-backed corpus-source modes where this revision
defines both.

### REQ-STREAM-EVAL-017

The evaluator shall directly verify leaf-stage fixed-capacity invariants against
the leaf membership artifact according to the benchmark profile's leaf model.

At minimum, this includes:

- exact final cluster occupancy when the benchmark profile declares strict
  alignment or synthetic padding sufficient to realize exact occupancy
- complete coverage of all evaluated entities
- exactly one final cluster assignment per evaluated entity
- no empty clusters among the declared `K` final clusters

### REQ-STREAM-EVAL-018

When the benchmark profile uses deterministic synthetic padding, the evaluator
shall distinguish real entities from synthetic padding entities in the leaf
membership artifact and shall exclude synthetic padding from externally reported
locality and compression metrics.

### REQ-STREAM-EVAL-019

The evaluator shall directly compute leaf-stage locality metrics from the leaf
membership artifact and benchmark ground truth.

In this revision, the required direct locality metric is same-leaf neighborhood
coherence over real entities. Same-or-sibling locality remains outside this
crate's direct proof boundary unless a future revision introduces explicit
sibling structure at this evaluator boundary.

### REQ-STREAM-EVAL-020

The evaluator shall directly compute leaf-stage compression-friendliness metrics
from the leaf membership artifact by comparing evaluator-declared local
per-cluster compression quality against a declared global baseline over the real
benchmark dataset.

### REQ-STREAM-EVAL-021

This revision shall not claim to prove full end-to-end LexonGraph hierarchy
conformance for properties that require artifacts outside the shared streaming
clustering boundary, including leaf packing, internal-node summaries, bounded
fanout and depth, search routing over a persisted hierarchy, artifact
serialization, and durable index build semantics.

This revision also shall not define the future end-to-end evaluator layered on
`docs/specs/rust-streaming-indexer-crate/` and
`docs/specs/rust-search-crate/`; that line remains future work.

### REQ-STREAM-EVAL-022

The evaluator shall support benchmark corpora supplied by block-store-backed
corpus references in addition to inline benchmark-profile fixture data,
including both filesystem-root-backed and zip-archive-backed references.

### REQ-STREAM-EVAL-023

This revision shall preserve inline benchmark-profile corpus data for small
fixtures and focused tests; scalable corpus support shall extend rather than
replace that fixture path.

### REQ-STREAM-EVAL-024

The evaluator shall use one unified corpus-source model across:

- candidate training-pass inputs
- final evaluation replay entities
- classifier-side probe workloads

The evaluator shall not require a separate large-corpus transport contract for
those workload families.

### REQ-STREAM-EVAL-025

The evaluator's scalable corpus path shall align with the repository's existing
block-store-based abstraction rather than defining a separate evaluator-specific
large-corpus persistence contract in benchmark-profile JSON. This path may
compose existing repository block-store implementations, including a mutable
filesystem layer over an immutable zip layer, without widening the parent
`BlockStore` trait.

### REQ-STREAM-EVAL-026

This revision shall not require benchmark profiles for large corpora to embed
every training-pass entry, evaluation entity, and probe embedding directly in a
single JSON document.

### REQ-STREAM-EVAL-027

The benchmark-profile JSON surface shall support an evaluator-owned
zip-archive-backed corpus-source declaration that carries:

- a source identity
- a zip archive path
- a root block ID

This declaration shall be valid anywhere the unified corpus-source model accepts
block-store-backed corpus references.

### REQ-STREAM-EVAL-028

When resolving a zip-archive-backed corpus source, the evaluator shall construct
an overlay block store consisting of:

- a higher-priority mutable filesystem block-store layer
- a lower-priority immutable zip block-store layer bound to the declared
  archive

The evaluator shall use that overlay-backed view uniformly for training-pass
inputs, evaluation replay entities, and classifier-side probe workloads.

### REQ-STREAM-EVAL-029

For zip-archive-backed corpus sources, the benchmark profile shall not require
the user to provide a writable overlay directory.

The evaluator shall create and manage the temporary writable filesystem layer
needed by the overlay for the duration of source resolution and any block
creation performed through that overlay-backed view.

### REQ-STREAM-EVAL-030

If doing so reduces implementation duplication without widening the parent
`BlockStore` API, the repository may expose a reusable helper or constructor for
the mutable-filesystem-over-immutable-zip overlay used by the evaluator.

### REQ-STREAM-EVAL-031

The repository shall define a reproducible section-4 benchmark-suite workflow
for the streaming clustering evaluator that can materialize the benchmark
profiles and supporting assets needed to compare candidate leaf-partition
strategies under the shared evaluator boundary.

The suite shall also declare the metric contract and fixed neighborhood size
used for exact-neighbor ground-truth generation and same-leaf locality scoring.

For the repository-owned checked-in section-4 screening panel in this revision,
that fixed neighborhood size shall be top-10.

This suite remains leaf-stage only: it prepares and executes comparative
leaf-formation campaigns and does not claim to validate hierarchy construction,
parent summaries, persisted routing, or full end-to-end index conformance.

### REQ-STREAM-EVAL-032

The section-4 benchmark suite shall realize a repository-owned corpus panel
whose benchmark identities trace to the research-plan corpus families relevant
to leaf-stage comparison:

- a real-world sample corpus harvested from repository-approved source data
- a well-clustered synthetic corpus
- a weak-cluster or uniform synthetic corpus
- an anisotropic or manifold synthetic corpus
- a near-duplicate-heavy corpus
- deterministic size-scaled subsets where scalability assessment is required

For the first complete checked-in section-4 screening panel in this revision,
the repository shall include at least one checked-in profile for each listed
family plus stable scale-tier identities for the profiles used in repeated
comparison.

For each family used by the suite, the repository shall declare the corpus
identity, construction or harvesting policy, and any scale-tier identities used
for repeated comparisons.

### REQ-STREAM-EVAL-033

For benchmark corpora used in section-4 locality evaluation, the repository
shall define deterministic exact-neighbor ground-truth assets computed from the
benchmark entities under the benchmark-declared metric contract.

For the checked-in section-4 screening panel in this revision, these
ground-truth assets shall be top-10 neighborhoods.

These ground-truth assets shall:

- identify the corpus identity and scale tier to which they apply
- exclude synthetic padding entities from externally reported locality scoring
- remain reproducible from the benchmark corpus contents and the declared metric
  configuration

### REQ-STREAM-EVAL-034

The section-4 benchmark suite shall support deterministic harvesting of a
real-world benchmark corpus from repository-approved block-store-backed source
data, including deterministic extraction of embeddings, entity identities, and
scale-tier subsets used for comparative leaf-stage campaigns.

This revision shall include at least one checked-in harvested benchmark corpus
asset derived through that workflow.

The harvesting policy shall define:

- the source identity and root block ID or equivalent source locator
- the entity-identity extraction contract
- the embedding extraction and admissibility contract
- the deterministic subset-selection rule used for any size-scaled variants

### REQ-STREAM-EVAL-035

Large benchmark corpora managed by the repository for section-4 execution shall
be stored in the git tree as `.zip` assets and consumed directly through the
evaluator's zip-archive-backed corpus-source path without requiring a
pre-decompression step.

The checked-in harvested benchmark corpus asset required by this revision shall
also follow this zip-native storage and direct-consumption rule.

Any writable layer needed to satisfy block-store semantics remains
evaluator-managed temporary state rather than a user-prepared extracted corpus
tree.

### REQ-STREAM-EVAL-036

The section-4 benchmark suite shall define a leaf-stage screening workflow that
executes each compared candidate against the same corpus-panel profiles and
reports, at minimum:

- exact leaf-size compliance and related leaf-stage invariant gates
- repeated-run observable determinism
- same-leaf neighborhood coherence over exact-neighbor ground truth
- local-versus-global compression gain
- strict-alignment and deterministic-synthetic-padding outcomes where both are
  applicable to the evaluated corpus family

For the checked-in section-4 screening panel in this revision, the workflow
shall execute across the expanded synthetic-plus-harvested profile set and
shall use top-10 exact-neighbor ground truth for locality-scored profiles.

The resulting comparative outputs shall be sufficient to down-select candidate
leaf strategies for later hierarchy-stage work without claiming hierarchy-stage
proof.

### REQ-STREAM-EVAL-037

For section-4 benchmark executions, the evaluator shall report leaf-stage
build-cost measurements sufficient to compare candidate strategies across corpus
scale tiers.

At minimum, this revision shall support deterministic reporting of:

- corpus size or evaluated entity count
- scale-tier identity
- build time per vector or an equivalent normalized leaf-stage build-cost
  measure declared by the benchmark suite

These reports shall remain consistent across the scale-tier set used by the
checked-in section-4 screening panel in this revision.

## Out of Scope

This crate does not define or own:

- a concrete clustering algorithm
- changes to the shared streaming clustering contract
- the full LexonGraph indexing or search runtime
- a canonical report schema shared with unrelated repository crates
- proof of end-to-end hierarchical index conformance beyond the shared
  streaming clustering boundary
- the future end-to-end evaluator over the indexer and search specifications
- a new production storage backend or a widening of the parent `BlockStore`
  trait

## Relationship to Other Specifications

This document creates a leaf-stage evaluator line layered on top of the shared
streaming clustering trait boundary and motivated by the clustering research
documents.

This document also layers scalable corpus consumption on top of the existing
block-storage trait specification rather than redefining a separate evaluator
storage contract.

If future repository specifications define an end-to-end index evaluation line
on top of `docs/specs/rust-streaming-indexer-crate/` and
`docs/specs/rust-search-crate/`, that future narrower package may own the
requirements currently recorded here as deferred research requirements.
