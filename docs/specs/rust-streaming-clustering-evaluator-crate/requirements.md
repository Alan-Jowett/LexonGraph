<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Clustering Evaluator Crate Requirements

## Status

Draft specification for a Rust crate that evaluates candidate streaming
clustering implementations for LexonGraph across the staged leaf-partition and
hierarchy-construction boundaries.

## Scope

This document specifies the crate-level requirements for a new Rust crate that:

- provides a reusable executable benchmark harness for comparing candidate
  streaming clustering implementations as leaf-partition realizations and for
  comparing hierarchy-construction strategies over the surviving leaf-stage
  outputs
- reuses the shared trainer/classifier boundary defined by
  `docs/specs/rust-streaming-clustering-crate/`
- aligns scalable corpus consumption with the repository's existing block-store
  abstraction rather than requiring monolithic profile-embedded JSON datasets
- translates applicable intent from `docs/research/clustering.md` and
  `docs/research/clustering_plan.md` into evaluator-owned benchmark contracts
  for the section-4 leaf-stage screening slice and the section-5 hierarchy-
  construction comparison slice without redefining the remaining end-state
  requirements owned by `docs/research/clustering.md`

This document defines the evaluator boundary, benchmark contracts, campaign
execution model, leaf-membership scoring surface, hierarchy-construction
scoring surface, scorecard outputs, and failure taxonomy. It does not require
a concrete clustering algorithm and does not redefine the shared streaming
clustering contract.

## Terminology

In this spec package, `candidate` means one clustering implementation entered
into evaluation through the shared streaming clustering trainer/classifier
boundary.

`Benchmark profile` means the evaluator-owned description of the fixed corpora,
pass plan, probe workloads, leaf-model declarations, any hierarchy-model
declarations, metric declarations, gates, and ranking weights used for one
comparative evaluation campaign.

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

`Hierarchy strategy` means an evaluator-owned strategy for aggregating the
surviving leaf-stage outputs into a bounded tree for section-5 comparison.

`Hierarchy-stage pair` means one surviving section-4 leaf strategy combined with
one registered hierarchy strategy under a shared section-5 benchmark contract.

`Direct metric` means an evaluator result whose measured behavior is directly
observable from the shared streaming clustering boundary and the benchmark
fixtures.

`Proxy metric` means an evaluator result intended to approximate a research goal
whose full end-to-end LexonGraph property is not directly observable from the
shared streaming clustering boundary alone.

`Deferred research requirement` means one requirement from
`docs/research/clustering.md` that this evaluator revision records as
out-of-scope because proving it requires artifacts outside the staged
evaluation boundaries owned by this crate.

## Requirements

### REQ-STREAM-EVAL-001

The repository shall define a dedicated Rust crate at
`crates/lexongraph-streaming-clustering-evaluator` that owns the reusable
streaming clustering staged leaf-partition and hierarchy-construction
evaluation boundary for LexonGraph.

### REQ-STREAM-EVAL-002

The new crate shall remain subordinate to:

- `docs/research/clustering.md` for the research goals motivating evaluation
- `docs/research/clustering_plan.md` for the intended comparative benchmark
  workflow
- `docs/specs/rust-streaming-clustering-crate/` for the shared candidate
  trainer/classifier contract
- `docs/specs/rust-block-storage-trait/` for the backend-neutral storage
  contract used by scalable corpus references

If those sources appear to conflict, `docs/research/clustering.md` remains
authoritative for the end-state black-box requirements, and
`docs/research/clustering_plan.md` remains authoritative for the staged
benchmark workflow that serves those requirements. The narrower evaluator scope
only limits what this crate may directly prove or claim at the section-4 leaf-
stage and section-5 hierarchy-construction boundaries; it does not relax,
replace, or reinterpret the parent research requirements. The shared streaming
clustering specification remains authoritative for the candidate integration
surface, and the block-storage trait specification remains authoritative for the
scalable external corpus-loading contract.

### REQ-STREAM-EVAL-003

The crate shall provide a reusable executable benchmark harness, with a
supporting reusable library surface, for running comparative evaluations of one
or more candidate streaming clustering implementations as leaf-partition
realizations and for running comparative evaluations of hierarchy-construction
strategies over the surviving section-4 leaf-stage outputs.

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
- for hierarchy-stage campaigns, the hierarchy model, including the declared
  `f_min` and `f_max`, depth-bound semantics, the compatible dispersion
  functional for refinement checks, and the declared penultimate-layer
  `epsilon` exception policy
- metric declarations and whether each metric is direct or proxy
- must-pass gates and any comparative ranking weights
- deferred research-goal records for requirements that cannot be proven at this
  boundary
- the benchmark-declared metric contract and any transformed-metric policy used
  by build, locality scoring, compression scoring, and any later carried-forward
  routing obligations
- any later-phase workload or artifact identities that section-4 must carry
  forward without directly executing, including held-out query-set identities
  when the experiment track declares them
- deterministic execution-profile metadata needed to interpret reproducibility

For section-4 screening profiles, this fixed campaign contract shall remain
consistent with the repository-owned benchmark-suite contract for the selected
experiment track rather than allowing per-candidate reinterpretation of metric
family, dimensionality contract, quantization baseline policy, alignment-policy
family, or declared execution environment.

For section-5 hierarchy-stage campaigns, the fixed campaign contract shall
remain consistent across all compared hierarchy-stage pairs derived from the
same surviving section-4 leaf-stage outputs rather than allowing pair-specific
reinterpretation of fanout bounds, depth-bound semantics, refinement semantics,
or exception policy.

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

When a metric, gate, or deferred record corresponds to a frozen section-1
benchmark-contract item, the evaluator shall also identify the later proof
surface expected to discharge any deferred obligation.

When a frozen benchmark-contract item is not directly measured during the
current evaluation stage, the evaluator shall still carry it forward as an
explicit deferred proof obligation rather than dropping it from the campaign
contract.

### REQ-STREAM-EVAL-014

The evaluator shall emit:

- a machine-readable per-candidate run report
- a machine-readable comparative campaign report
- a human-readable scorecard summarizing pass/fail status, metric values, and
  comparative ranking for surviving candidates
- for checked-in section-4 suite execution, a human-readable survivor-decision
  artifact summarizing which candidates carry forward and why

For section-5 hierarchy-stage execution, these outputs shall additionally
identify the originating section-4 survivor set and the compared hierarchy-stage
pairs and shall emit a human-readable carry-forward artifact summarizing which
leaf-strategy × hierarchy-strategy pairs remain eligible for the later
parent-summary and routing phases.

These outputs remain evaluator-owned and source-neutral: changing a workload
from inline fixture data to a block-store-backed corpus reference shall change
input acquisition and provenance detail, not the semantic meaning of the
reported evaluator results.

### REQ-STREAM-EVAL-015

The evaluator shall surface deterministic structured failures with a stable
error code and deterministic human-readable message and shall distinguish at
least:

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

For failures that terminate a section-4 candidate/configuration execution, the
evaluator shall also preserve artifact-hygiene evidence showing that no
success-shaped completed artifact set was exposed for the failed execution.

### REQ-STREAM-EVAL-016

The repository shall include executable verification artifacts that realize the
validation plan for the streaming clustering evaluator crate, including both
inline-fixture and block-store-backed corpus-source modes where this revision
defines both and including any section-5 hierarchy-stage verification surfaces
introduced by this revision.

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

The evaluator shall also verify and report whether synthetic padding entities
are concentrated into the minimum possible number of final clusters permitted
by the deterministic procedure. At this boundary, that concentration result is
part of the leaf-stage fixed-capacity evidence surface rather than a separate
hierarchy-stage claim.

The evaluator shall also require synthetic padding entities to carry stable
synthetic identity tagging that does not collide with real-entity identities in
the same evaluated corpus and shall ensure that externally reported evaluator
entity listings do not misclassify synthetic padding as real benchmark members.

### REQ-STREAM-EVAL-019

The evaluator shall directly compute leaf-stage locality metrics from the leaf
membership artifact and benchmark ground truth.

In this revision, the required direct locality metric is same-leaf neighborhood
coherence over real entities. Same-or-sibling locality remains outside this
crate's direct proof boundary unless a future revision introduces explicit
sibling structure at this evaluator boundary. The same-leaf metric therefore
acts as a staged proxy subordinate to, rather than a redefinition of, the
full same-or-sibling locality objective from `docs/research/clustering.md`.

### REQ-STREAM-EVAL-020

The evaluator shall directly compute leaf-stage compression-friendliness metrics
from the leaf membership artifact by comparing evaluator-declared local
per-cluster compression quality against a declared global baseline over the real
benchmark dataset.

At minimum, the declared comparison shall define:

- the reconstruction-error or equivalent compression-quality functional used for
  both the local and global measurements
- the global baseline over the unpadded real benchmark dataset only, excluding
  synthetic padding entities
- the reported local-versus-global delta semantics
- the per-cluster or per-bucket distribution reported alongside the aggregate
  comparison

### REQ-STREAM-EVAL-021

This revision shall not claim to prove full end-to-end LexonGraph hierarchy
conformance for properties that still require artifacts outside the staged
section-4 and section-5 evaluator boundaries, including parent-summary accuracy
or stability, search routing over a persisted hierarchy, artifact
serialization, and durable index build semantics.

This revision also shall not define the future end-to-end evaluator layered on
`docs/specs/rust-streaming-indexer-crate/` and
`docs/specs/rust-search-crate/`; that line remains future work. These
deferments constrain only what this evaluator revision may claim as direct
evidence. They do not narrow the parent end-state obligations defined by
`docs/research/clustering.md` and staged by `docs/research/clustering_plan.md`.

For section-4 screening in this revision, the evaluator shall preserve at least
the following as explicit later-phase proof obligations whenever the benchmark
contract freezes them for the experiment track:

- the declared end-state locality target, including same-or-sibling semantics
- the declared routing target, including any threshold values, and the routing-
  procedure assumptions needed to interpret that target
- any declared beam-width policy that later routing phases must evaluate
- parent-summary accuracy and stability obligations
- serialization round-trip and persisted-artifact durability obligations
- any declared multi-thread reproducibility obligation that exceeds the direct
  section-4 observable boundary

For section-5 hierarchy-stage execution in this revision, the evaluator shall
preserve as later-phase proof obligations any declared parent-summary accuracy
or stability target, routing target, routing-procedure assumption, beam-width
policy, serialization or persistence obligation, or multi-thread
reproducibility obligation that the hierarchy-stage campaign does not directly
prove.

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
Instead, it provides the repository-owned leaf-stage evidence slice that feeds
the later hierarchy, summary, routing, and persistence phases in
`docs/research/clustering_plan.md`.

The suite shall freeze, per experiment track, the benchmark-contract items that
section-4 candidate comparisons are not allowed to reinterpret, including:

- the declared metric family and any transformed-metric policy
- the primary `leaf_size` and any declared sensitivity sizes
- the dimensionality contract used by the selected corpus panel
- the alignment-policy family
- the quantization or compression baseline policy over real entities
- any declared search-target threshold and beam-width policy that must carry
  forward to later routing phases
- the declared floating-point profile
- the declared candidate-threading model
- the declared reduction-order strategy for any deterministic parallel or
  aggregate computation permitted by the track
- the declared hardware profile
- any declared wall-clock execution budget or timeout-disqualification policy

When an experiment track is intended to qualify realistic corpus behavior rather
than only smoke or regression behavior, the frozen contract shall also identify
that track as a realistic qualification track and shall freeze a primary
`leaf_size` in the `64..128` regime for that track.

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

For the checked-in panel in this revision, the suite shall define stable
small, medium, and large scale-tier identities or deterministic nearest-
practical equivalents for each corpus family that participates in repeated
size-tier comparison.

The first complete checked-in section-4 panel in this revision shall include
repeated-comparison small, medium, and large tiers for the harvested,
well-clustered, weak-cluster or uniform, anisotropic or manifold, and
near-duplicate-heavy families, plus at least one explicit deterministic-padding
profile used to compare strict-alignment versus padding behavior.

The canonical repository-managed realistic qualification panel shall include at
least one harvested real-world corpus family whose checked-in qualification tier
or deterministic nearest-practical equivalent:

- contains tens of thousands of real entities
- uses a dimensionality within the `384..4096` range
- is not evenly divisible by the track's primary `leaf_size`

Smaller fixture or smoke-oriented corpora may remain in the repository for
regression ergonomics, but they shall not be the only checked-in basis for
claiming realistic-corpus qualification.

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
proof. The workflow therefore serves the broader end-state requirements from
`docs/research/clustering.md` by staged screening rather than by redefining
those requirements at the evaluator boundary.

Where both strict-alignment and deterministic-padding configurations are
applicable to the same corpus family or comparison study, the workflow shall
report their distinct invariant and metric outcomes together with the declared
normalized build-cost comparison used to judge the alignment-policy tradeoff.

For realistic qualification tracks, the workflow shall also apply the declared
bounded-time contract and record deterministic timeout-disqualification outcomes
for candidates that fail to complete within the frozen execution budget.

### REQ-STREAM-EVAL-037

For section-4 benchmark executions, the evaluator shall report leaf-stage
build-cost measurements sufficient to compare candidate strategies across corpus
scale tiers.

At minimum, this revision shall support deterministic reporting of:

- corpus size or evaluated entity count
- scale-tier identity
- build time per vector or an equivalent normalized leaf-stage build-cost
  measure declared by the benchmark suite
- peak build memory during section-4 execution
- wall-clock elapsed time relative to the declared execution budget, including
  whether the candidate completed, timed out, or was disqualified on bounded-
  time grounds

When the frozen experiment-track contract carries a later-phase loaded-index
memory obligation but section-4 does not materialize a persisted loadable
artifact, the evaluator shall record that memory obligation as deferred rather
than omitting it.

These reports shall remain consistent across the scale-tier set used by the
checked-in section-4 screening panel in this revision.

### REQ-STREAM-EVAL-038

The checked-in section-4 screening workflow shall support at least one
repository-owned reusable concrete candidate implementation in addition to
evaluator-local fixture candidates.

This revision's checked-in repository-owned candidate set shall include:

- the PCA projection + deterministic sort + exact chunking implementation
  provided by `crates/lexongraph-pca-chunking`
- the directional PCA clustering implementation provided by
  `crates/lexongraph-directional-pca`
- the streaming DCBC clustering implementation provided by
  `crates/lexongraph-dcbc-streaming`

The checked-in runnable section-4 candidate set for repeated comparison shall
also include concrete representatives for the remaining research-plan families:

- recursive balanced partitioning with exact-size enforcement
- space-filling-curve ordering plus exact chunking
- graph-neighborhood partitioning with exact-size balancing
- hybrid coarse partitioning with exact-size local rebalance
- random shuffle plus exact chunking as a null baseline

### REQ-STREAM-EVAL-039

For the repository-owned `lexongraph-directional-pca` candidate, the evaluator
shall own deterministic default `DirectionalPcaParams` sufficient to register
and execute that candidate through the shared streaming
trainer/classifier contract without introducing an evaluator-private candidate
API.

### REQ-STREAM-EVAL-040

The evaluator shall register `lexongraph-dcbc-streaming` through the same
shared candidate adapter surface and report model used for evaluator-local
fixtures and other repository-owned candidates.

### REQ-STREAM-EVAL-041

When a registered repository-owned candidate cannot execute under a benchmark
profile because of candidate-specific limits that remain within the shared
streaming clustering contract, the evaluator shall surface that outcome as an
ordinary candidate result rather than silently filtering the candidate out of
the run.

At minimum, this revision shall preserve explicit candidate outcomes for:

- `lexongraph-directional-pca` rejection of shared balance constraints
- `lexongraph-dcbc-streaming` rejection of zero-norm embeddings
- `lexongraph-dcbc-streaming` rejection of unsupported shared balance settings
  such as `max_cluster_size_ratio` and `soft_balance_penalty`

### REQ-STREAM-EVAL-042

The evaluator's candidate-discovery surface and checked-in validation artifacts
shall include all registered section-4 candidates used by the checked-in
screening workflow so they can be listed, selected, and exercised through
ordinary campaign and section-4 suite execution paths.

### REQ-STREAM-EVAL-047

The section-4 validation surface shall exercise deterministic rejection of
malformed suite-level configuration that remains unrepresented in the checked-in
regression set.

At minimum, this revision shall cover rejection of:

- empty `suite_id`
- zero `leaf_size`
- zero `dimensions`
- zero `batch_size`
- zero `neighbor_count`
- an empty declared profile set

### REQ-STREAM-EVAL-048

The section-4 validation surface shall exercise deterministic rejection of
invalid alignment-policy inputs before candidate comparison proceeds.

At minimum, this revision shall cover:

- strict-alignment corpora whose real-entity count is not divisible by
  `leaf_size`
- deterministic-padding corpora with no real entities
- deterministic-padding corpora whose real-entity count is already divisible by
  `leaf_size`

### REQ-STREAM-EVAL-049

The section-4 validation surface shall exercise deterministic rejection of
malformed harvested-corpus inputs.

At minimum, this revision shall cover:

- missing or non-text entity-identity metadata
- non-boolean `synthetic` metadata
- harvested embeddings that fail the declared admissibility contract
- harvested sources with too few remaining real entities after synthetic
  filtering

### REQ-STREAM-EVAL-050

The section-4 validation surface shall exercise deterministic rejection of
invalid exact-neighbor ground-truth inputs that prevent valid benchmark-owned
ground-truth generation.

At minimum, this revision shall cover:

- corpora with too few real entities for the declared `neighbor_count`
- cosine-metric inputs containing zero-norm embeddings

### REQ-STREAM-EVAL-043

The repository-owned section-4 benchmark suite shall define a frozen benchmark
contract for each experiment track that keeps candidate comparisons comparable
across repeated runs and later phases.

At minimum, that frozen contract shall declare:

- the metric family and any transformed-metric policy
- the exact role of the declared metric family in build-time comparison,
  compression scoring, deferred summary obligations, and any later routing
  obligations
- the primary `leaf_size` and any declared sensitivity sizes
- the dimensionality contract, including deterministic out-of-range rejection
  semantics where applicable
- the alignment-policy family
- the quantization or compression baseline policy over real entities only,
  excluding synthetic padding
- the declared floating-point profile
- the declared candidate-threading model for the track
- the declared reduction-order strategy for any deterministic aggregate
  computation permitted by the track
- the declared hardware profile

The suite shall also label which frozen items are measured directly during
section-4 screening versus carried forward as deferred obligations.

For realistic qualification tracks, the frozen contract shall also distinguish
the realistic qualification surface from smoke or regression-only tracks and
shall record the realistic dimensionality band and non-aligned corpus
expectation used to interpret qualification claims.

### REQ-STREAM-EVAL-044

When a frozen benchmark-contract item from
`docs/research/clustering_plan.md` section 1 cannot be directly proven by the
leaf-stage evaluator boundary, the suite and emitted campaign artifacts shall
preserve it as an explicit later-phase proof obligation.

At minimum, this revision shall preserve explicit deferred records for any
frozen:

- same-or-sibling locality target not directly proven by same-leaf scoring
- routing target or routing-procedure assumption
- beam-width policy or related routing-study assumption
- bounded fanout or depth constraint
- parent-summary accuracy or stability obligation
- parent-summary metric or dispersion-contract obligation needed to interpret a
  later summary-quality proof
- refinement-contract obligations such as `beta = Disp(C) / Disp(P)` semantics
  and any declared penultimate-layer exception
- serialization round-trip or persisted-artifact durability obligation
- multi-thread reproducibility obligation beyond the direct section-4
  observable boundary
- held-out query-set or equivalent later-phase routing-workload identity needed
  to discharge a deferred routing obligation

Each deferred record shall identify the frozen target or constraint, why
section-4 does not directly prove it, and the later evaluation surface expected
to discharge it.

### REQ-STREAM-EVAL-045

For corpus families used in repeated section-4 comparison, the benchmark suite
shall define a deterministic scale-tier contract that keeps corpus-size studies
comparable across candidates and reruns.

At minimum, this revision shall support:

- stable small, medium, and large tier identities, or deterministic nearest-
  practical equivalents when exact target sizes are infeasible
- a declared tier-growth rule that makes the large tier materially larger than
  the small tier for the same family
- deterministic ties among each tier identity, its corpus asset, and any
  exact-neighbor ground-truth asset used for locality scoring
- participation of at least one checked-in harvested real-world corpus family
  in the same tiered comparison surface used by the synthetic families

For any corpus family intended to carry forward into later routing phases, this
scale-tier contract shall declare held-out query-set identities for those later
studies even though section-4 execution in this revision shall not depend on
consuming those later-phase query assets.

For the canonical realistic qualification panel, this scale-tier contract shall
include at least one repository-managed harvested real-world tier in the tens-
of-thousands regime whose real-entity count is not an exact multiple of the
primary `leaf_size`.

The first complete checked-in section-4 panel in this revision shall include at
least one such held-out query-set identity for a checked-in harvested
real-world corpus family carried forward beyond leaf-stage screening, together
with a checked-in materialized asset path for that held-out identity.

### REQ-STREAM-EVAL-046

The section-4 evaluator and benchmark suite shall enforce hard-gate termination
and artifact hygiene for each candidate/configuration execution.

At minimum:

- if a candidate fails a hard invariant gate for a configuration, the evaluator
  shall stop further comparative metric evaluation for that candidate under that
  configuration
- the emitted artifact set for that failed execution shall preserve the
  deterministic failure classification, including the failure's stable error
  code and message, without presenting a success-shaped completed result for the
  rejected configuration
- the emitted artifact set for that failed execution shall include
  artifact-hygiene evidence showing that later comparative metrics and
  success-shaped completion artifacts were not exposed for the rejected
  configuration
- section-4 comparative outputs shall identify the surviving candidates, if
  any, that remain eligible to carry forward into later hierarchy-stage
  comparison
- the checked-in section-4 suite artifact set shall include a machine-readable
  suite report, a human-readable suite scorecard, and a human-readable
  survivor-decision summary for the canonical checked-in run

This requirement constrains candidate-comparison semantics; it does not require
section-4 to perform later hierarchy-stage execution itself.

### REQ-STREAM-EVAL-051

In addition to naming the frozen section-1 benchmark-contract items, each
section-4 experiment track shall define the metric and execution-semantics
contract needed to interpret comparable results.

At minimum, this revision shall declare:

- the exact build, locality, compression, and deferred-routing metric roles
- any transformed-metric policy together with the ordering-preservation
  obligation needed for later routing interpretation
- the metric-contract consistency checks and reported audit results needed to
  show that build, compression, deferred summary obligations, and any carried-
  forward routing obligations interpret the declared metric consistently
- the compatible dispersion functional used by any deferred summary or
  refinement obligation
- the declared threading model together with the deterministic reduction-order
  strategy for the track
- whether 1-thread versus N-thread bitwise observable identity is measured
  directly by section-4 or preserved as a deferred obligation

### REQ-STREAM-EVAL-052

When section-4 freezes obligations that must be discharged by later hierarchy,
summary, routing, persistence, or service-level evaluation lines, the benchmark
suite shall preserve the artifact and workload identities needed to continue
that proof chain.

At minimum, this revision shall support preserving:

- held-out query-set or later routing-workload identities
- later-phase summary, persistence, or hierarchy artifact identities when the
  experiment track declares them
- the later evaluation line expected to consume each preserved identity

Section-4 execution in this revision may leave those identities unused, but it
shall not omit them from the frozen contract when the experiment track declares
them. The first complete checked-in section-4 panel shall include at least one
preserved held-out query-set identity for a harvested real-world corpus family
that later routing phases are expected to consume, and that identity shall
retain a checked-in materialized asset path.

### REQ-STREAM-EVAL-053

The section-4 workflow shall define a deterministic carry-forward rule for
choosing which candidates survive leaf-stage screening for later hierarchy-stage
comparison.

At minimum, the rule shall:

- reject any candidate/configuration that fails a hard invariant gate
- rank surviving candidates using same-leaf locality evidence, declared local
  compression benefit, and normalized leaf-stage build-cost evidence
- prefer the highest-quality surviving candidates without allowing build-cost
  comparisons to rescue a hard-gate failure
- define deterministic tie-breaking behavior when surviving candidates remain
  otherwise indistinguishable on the declared comparison surface

The checked-in section-4 workflow in this revision shall publish the resulting
carry-forward decision as a checked-in survivor-decision artifact produced from
the canonical section-4 suite run.

### REQ-STREAM-EVAL-054

The evaluator shall define a hierarchy-strategy registration surface for
section-5 comparison that accepts the surviving section-4 leaf-stage outputs as
its inputs and does not require a broader candidate API than the shared
streaming clustering contract used to produce those leaf-stage outputs.

The registered section-5 hierarchy strategies shall execute under the shared
section-5 metric-semantics contract rather than hard-wiring Euclidean-only
grouping behavior. When a hierarchy strategy uses nearest-centroid packing,
ordering, or any other metric-sensitive grouping decision, the evaluator shall
apply the grouping functional declared by the section-5 hierarchy-stage
benchmark contract.

At minimum, the registered section-5 hierarchy strategies shall support the
research-plan comparison families of:

- bottom-up agglomeration with bounded fanout
- recursive top-down partitioning over leaf summaries
- greedy pack-by-centroid nearest grouping
- a hybrid strategy that combines top-down coarse partitioning with lower-level
  bottom-up grouping

### REQ-STREAM-EVAL-055

The evaluator shall define a shared section-5 hierarchy-stage benchmark
contract for each compared leaf-strategy survivor set.

At minimum, that contract shall declare:

- the originating section-4 survivor identities and the leaf-stage profile or
  suite artifacts from which they were derived
- the fixed `f_min` and `f_max` bounds used for hierarchy construction
- the depth-bound semantics and theoretical-bound formula used for comparison
- the grouping-distance or equivalent ordering functional used for section-5
  hierarchy construction decisions
- the compatible dispersion functional used to interpret refinement checks
- the declared `beta` refinement threshold
- the declared penultimate-layer `epsilon` exception and its admissibility
  conditions
- the hierarchy-stage build-throughput and memory-reporting semantics

The contract shall also declare the deterministic compatibility rule that ties
the grouping functional and refinement-dispersion functional back to the
benchmark-declared metric semantics. If the declared combination is unsupported
or internally inconsistent, the evaluator shall reject the contract
deterministically before pair execution begins.

The same hierarchy-stage contract shall also declare the bounded-time execution
budget shared by all compared pairs together with the timeout-disqualification
semantics used when a pair exceeds that budget.

### REQ-STREAM-EVAL-056

For each surviving section-4 leaf strategy × registered hierarchy strategy
pair, the evaluator shall execute a section-5 hierarchy-stage comparison that
builds a full tree under the shared hierarchy-stage benchmark contract.

At minimum, the direct section-5 measurements shall report:

- fanout compliance against the declared `f_min` and `f_max`
- absence of single-child internal nodes
- depth relative to the declared theoretical bound
- per-edge refinement coefficients `beta = Disp(C) / Disp(P)` computed with the
  declared compatible dispersion functional
- any use of the declared penultimate-layer `epsilon` exception
- the effective grouping functional used by any metric-sensitive hierarchy
  strategy decisions
- the effective refinement-dispersion functional used by `beta` and
  `epsilon`-gated checks
- the reported metric-semantics consistency result for the compared pair
- hierarchy-stage build throughput and peak build memory

### REQ-STREAM-EVAL-057

The section-5 hierarchy-stage workflow shall enforce deterministic hard-gate
rejection for leaf-strategy × hierarchy-strategy pairs that violate the shared
hierarchy-stage benchmark contract.

At minimum, this revision shall reject pairs that:

- violate the declared fanout bounds
- emit one or more single-child internal nodes
- exceed the declared depth bound
- violate the declared `beta` refinement threshold outside the admitted
  `epsilon` exception scope
- apply the `epsilon` exception outside its declared penultimate-layer
  admissibility conditions
- require a grouping functional or refinement-dispersion functional that the
  evaluator does not support for the declared section-5 metric semantics
- declare a grouping-functional and refinement-dispersion-functional
  combination that fails the contract's deterministic compatibility rule

### REQ-STREAM-EVAL-058

Section-5 hierarchy-stage reports and scorecards shall preserve explicit
cross-stage traceability to the section-4 survivor set from which each compared
pair was derived.

At minimum, the hierarchy-stage artifact set shall:

- identify the originating section-4 profile, suite, or survivor-decision
  artifact for each compared pair
- preserve the provenance needed to reconstruct the leaf-stage inputs consumed
  by hierarchy construction
- identify the effective grouping functional and refinement-dispersion
  functional used for the pair together with the metric-semantics consistency
  result
- publish the resulting carry-forward decision as a deterministic hierarchy-
  stage pair summary for later parent-summary and routing phases

### REQ-STREAM-EVAL-059

Even after section-5 hierarchy-stage execution is added, this revision shall
continue to treat the following as deferred unless a later specification expands
the evaluator boundary again:

- parent-summary accuracy and stability comparison from section 6 of
  `docs/research/clustering_plan.md`
- routing recall, latency, and beam-width benchmarking from section 7
- robustness, serialization, and persisted-artifact validation from section 8

Section-5 direct hierarchy measurements shall therefore be reported as staged
evidence toward the parent research goals rather than as proof of those later
phases.

### REQ-STREAM-EVAL-060

The repository shall define a canonical realistic-corpus qualification surface
for the streaming clustering evaluator that is distinct from fixture-only or
smoke-only validation surfaces.

At minimum, this canonical qualification surface shall be repository-managed and
reproducible from checked-in assets or checked-in generation specifications and
shall include at least one harvested real-world corpus family whose
qualification tier:

- contains tens of thousands of real entities
- uses a uniform dimensionality in the `384..4096` range
- is not an exact multiple of the primary `leaf_size`
- participates in the same section-4 and later section-5 qualification workflow
  used for candidate down-selection

### REQ-STREAM-EVAL-061

The evaluator shall treat bounded-time completion as a qualification constraint
for both section-4 and section-5 realistic-corpus runs.

At minimum:

- each realistic qualification track shall declare a deterministic wall-clock
  execution budget or equivalent timeout contract for section-4 candidate runs
  and for section-5 hierarchy-stage pairs
- a candidate or pair that exceeds the declared budget shall be reported as a
  deterministic timeout-disqualification outcome rather than as a survivor
- timeout-disqualification shall preserve deterministic artifact hygiene and
  provenance in the same report model used for other gate or contract failures

### REQ-STREAM-EVAL-062

The evaluator may provide an optional WGPU-backed acceleration path for
evaluator-owned dense kernels, including:

- exact-neighbor ground-truth generation
- section-4 evaluator replay or dense distance work
- section-5 dense distance and dispersion work

The CPU path remains required.

### REQ-STREAM-EVAL-063

The first accelerated revision shall target a repository-declared qualification
hardware profile that includes Windows on AMD Radeon 780M, while preserving
explicit CPU fallback on unsupported hosts.

### REQ-STREAM-EVAL-064

When the accelerated path is used, the evaluator shall record the selected
execution backend and capability result in provenance or reporting sufficient to
distinguish:

- CPU execution
- WGPU-accelerated execution
- capability probe succeeded but the backend was declined for the run
- capability probe failed or the host was unsupported and CPU fallback was used

### REQ-STREAM-EVAL-065

The first accelerated revision may scope GPU support to evaluator-owned dense
kernels and shared DCBC dense kernels only. It shall not require
directional-PCA or PCA eigendecomposition acceleration in order to claim
conformance for this revision.

### REQ-STREAM-EVAL-066

For the realistic qualification surface, at least one accelerated validation
path shall demonstrate that the WGPU-backed execution preserves campaign verdict
semantics relative to CPU execution while materially reducing runtime on the
declared qualification hardware profile.

## Out of Scope

This crate does not define or own:

- a concrete clustering algorithm
- changes to the shared streaming clustering contract
- the full LexonGraph indexing or search runtime
- a canonical report schema shared with unrelated repository crates
- proof of end-to-end hierarchical index conformance beyond the staged
  evaluator boundaries defined by this crate
- the future end-to-end evaluator over the indexer and search specifications
- a new production storage backend or a widening of the parent `BlockStore`
  trait

## Relationship to Other Specifications

This document creates a staged evaluator line layered on top of the shared
streaming clustering trait boundary and motivated by the clustering research
documents.

This document also layers scalable corpus consumption on top of the existing
block-storage trait specification rather than redefining a separate evaluator
storage contract.

If future repository specifications define an end-to-end index evaluation line
on top of `docs/specs/rust-streaming-indexer-crate/` and
`docs/specs/rust-search-crate/`, that future narrower package may own the
requirements currently recorded here as deferred research requirements.
