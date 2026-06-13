<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Clustering Evaluator Crate Design

## Status

Draft design specification for a Rust crate that evaluates candidate streaming
clustering implementations for LexonGraph across the staged leaf-partition and
hierarchy-construction boundaries.

## Design Goals

The crate design is intended to be:

- reusable across candidate algorithm crates
- explicit about what is measured directly versus approximately
- deterministic at the observable benchmark boundary
- comparative without redefining the shared candidate contract
- able to score leaf occupancy, locality, and compression directly from final
  assignments
- able to compare bounded hierarchy-construction strategies over surviving
  leaf-stage outputs
- able to consume large benchmark corpora through the existing block-store
  abstraction without embedding every workload entry in one profile document
- honest about research goals that remain deferred at this boundary

## Crate Boundary

The crate owns:

- evaluator-owned benchmark profile types
- evaluator-owned candidate registration and campaign orchestration types
- evaluator-owned leaf membership materialization and scoring types
- evaluator-owned hierarchy-construction materialization and scoring types
- evaluator-owned provenance, result, and scorecard types
- evaluator-owned gate and ranking logic
- executable and reusable-library entry points for benchmark campaigns

The crate does not own:

- the shared streaming clustering trainer/classifier definitions
- a concrete clustering implementation
- the full LexonGraph index build, routing, or storage lifecycle
- a claim that proxy metrics fully prove end-to-end hierarchy behavior
- the future end-to-end evaluator layered on the indexer and search
  specifications

## Design Entries

### DSG-STREAM-EVAL-001 `Composite normative boundary`

The crate depends on `docs/research/clustering.md` for the end-state black-box
requirements, on `docs/research/clustering_plan.md` for the staged benchmark
workflow, and on `docs/specs/rust-streaming-clustering-crate/` for the
candidate trainer/classifier boundary. For scalable corpus inputs, it also
depends on `docs/specs/rust-block-storage-trait/` for the backend-neutral
storage contract. The crate does not redefine those sources; it only defines
the evaluator-owned section-4 leaf-stage and section-5 hierarchy-stage evidence
slices that are subordinate to them.

### DSG-STREAM-EVAL-002 `Evaluator-owned boundary`

The crate owns benchmark profile, campaign orchestration, provenance, result,
scorecard, leaf-membership scoring, and hierarchy-construction scoring types.
It does not own a broader candidate algorithm API than the shared streaming
clustering contract.

### DSG-STREAM-EVAL-003 `Executable plus reusable library surface`

The primary deliverable is an executable benchmark harness. The executable is a
thin front end over a reusable library surface so repository code and tests can
construct benchmark profiles, register candidates, execute campaigns, and
consume reports without shelling out.

### DSG-STREAM-EVAL-004 `Candidate adapter shape`

Each candidate is represented through an evaluator-owned adapter or factory that
constructs a `StreamingClusterTrainer` conforming to the shared contract and
provides the candidate identity metadata needed for campaign reports. The
adapter does not expose algorithm-specific evaluation hooks outside that shared
boundary.

Repository-owned reusable concrete candidates and evaluator-local fixtures use
the same adapter shape.

### DSG-STREAM-EVAL-005 `Benchmark profile shape`

The benchmark profile fixes all candidate-comparable inputs for one campaign,
including:

- corpus panel identities or equivalent dataset handles, plus the declared
  source mode for each workload
- the streaming pass plan used for candidate training, whether supplied inline
  or through referenced corpus material
- classifier-side probe workloads such as held-out embeddings or other
  benchmark-owned probes, whether supplied inline or through referenced corpus
  material
- the leaf model, including target leaf size `L`, the relationship among `N`,
  `K`, and expected occupancy, and the alignment policy
- for hierarchy-stage campaigns, the hierarchy model, including fanout bounds,
  depth-bound semantics, refinement semantics, and the penultimate-layer
  `epsilon` exception policy
- metric declarations, gate declarations, and comparative ranking weights
- explicit deferred research-goal records for goals that cannot be proven at
  this boundary
- any later-phase workload or artifact identities that section-4 must carry
  forward without directly executing, including held-out query-set identities
  when the experiment track declares them
- declared reproducibility metadata such as floating-point and hardware profile
  descriptors

For scalable corpora, the source declaration may identify either a
filesystem-root-backed block store or a zip-archive-backed block store plus the
root block ID to traverse.

For section-4 screening, the profile remains subordinate to a suite-owned
experiment-track contract that freezes the metric family, dimensionality
contract, alignment-policy family, quantization baseline policy, and declared
execution environment so candidates cannot silently shift the comparison basis.

For section-5 hierarchy-stage comparison, the profile also records the
originating section-4 survivor identities and freezes the shared hierarchy
contract so compared leaf-strategy × hierarchy-strategy pairs cannot silently
shift fanout bounds, refinement semantics, or exception semantics.

### DSG-STREAM-EVAL-006 `Shared-profile campaign execution`

One evaluation campaign binds one benchmark profile to one or more registered
candidates. The runner fans the shared profile out across candidates rather than
allowing candidate-specific benchmark contracts that would break comparability.

This same shared-profile rule applies to corpus sourcing: compared candidates
observe the same declared inline fixtures or referenced block-store-backed
datasets rather than candidate-specific corpus materialization paths.

### DSG-STREAM-EVAL-007 `Provenance manifest`

Before reporting comparative results, the runner materializes a deterministic
provenance manifest containing the benchmark profile identity, corpus identities,
source-reference identities for referenced corpora, candidate identity, shared
clustering configuration, deterministic seed policy, software version identity,
declared floating-point execution-profile metadata, and declared
hardware-profile metadata.

### DSG-STREAM-EVAL-008 `Candidate execution flow`

For each candidate run, the runner:

1. constructs a trainer through the candidate adapter
2. resolves the benchmark profile's declared training-pass workload source and
   replays those inputs through `ingest_batch()`
3. completes each pass with `finish_pass()` and records the resulting pass
   reports
4. transitions through `complete_training()` when required by the profile
5. produces a classifier through `into_classifier()`
6. resolves the benchmark profile's declared evaluation corpus source and
   replays the benchmark corpus through the classifier to materialize the
   evaluator-owned leaf membership artifact
7. resolves and executes the classifier-side probe workloads and records the
   observed outputs

Inline fixtures and block-store-backed corpus references therefore share one
execution shape after workload resolution rather than splitting the evaluator
into unrelated small-corpus and large-corpus code paths.

### DSG-STREAM-EVAL-008A `Unified corpus-source model`

The evaluator defines one corpus-source abstraction that can back:

- training-pass inputs
- evaluation replay entities
- classifier-side probe workloads

The abstraction supports:

- inline fixture payloads embedded in the benchmark profile
- block-store-backed references that identify externally stored corpus material,
  including filesystem-root-backed and zip-archive-backed declarations

This preserves small-fixture ergonomics while allowing large corpora to remain
outside the profile document.

### DSG-STREAM-EVAL-008B `Block-store-backed corpus references`

For scalable workloads, the benchmark profile carries evaluator-owned corpus
references whose external loading semantics align with the existing
block-storage trait boundary.

This design does not widen `BlockStore` with evaluator-specific query
operations, whole-corpus JSON payloads, or candidate-owned shortcuts. Any
concrete store construction inputs remain outside the parent trait boundary.

The scalable path may resolve a corpus reference through composition of existing
repository block-store implementations when needed to preserve the evaluator's
source-neutral execution flow.

### DSG-STREAM-EVAL-008C `Archive-backed corpus reference`

The evaluator's corpus-source model includes an archive-backed reference form
that carries:

- source identity
- zip archive path
- root block ID

This declaration is valid anywhere the unified corpus-source model accepts a
block-store-backed source.

### DSG-STREAM-EVAL-008D `Temporary filesystem-over-zip overlay resolution`

When the evaluator resolves an archive-backed corpus reference, it:

1. opens the declared archive through the zip block-store implementation
2. creates a temporary writable filesystem block-store layer
3. composes the writable filesystem layer above the immutable zip layer through
   the overlay block-store implementation
4. traverses the declared root block ID through the resulting overlay-backed
   block-store view

The writable filesystem layer is evaluator-managed lifecycle state and is not a
required user-supplied benchmark-profile input.

### DSG-STREAM-EVAL-008E `Reusable overlay helper`

If the evaluator would otherwise duplicate overlay-construction logic, it may
expose a small reusable helper or constructor for the temporary
filesystem-over-zip composition used by archive-backed corpus sources.

This helper remains subordinate to the existing overlay and zip block-store
specifications and does not widen `BlockStore`.

### DSG-STREAM-EVAL-009 `Observable determinism checks`

The runner can re-execute the same candidate under the same benchmark profile
and compare the observable boundary:

- pass report sequences
- classifier-side assignments or other classifier probe results
- deterministic provenance fields expected to remain identical

Determinism evaluation is scoped to this observable boundary rather than to
unobservable internal state.

### DSG-STREAM-EVAL-010 `Leaf membership artifact`

The evaluator materializes a leaf membership artifact by assigning every
evaluated entity to exactly one final cluster through the candidate's finished
classifier. This artifact is evaluator-owned derived state rather than a
candidate-owned public API surface.

The artifact is the common basis for:

- leaf occupancy and coverage checks
- same-leaf locality scoring
- local-versus-global compression scoring
- padding-aware metric exclusion when synthetic padding is enabled

The artifact's semantics are independent of whether evaluation entities were
declared inline or loaded through block-store-backed corpus references.

### DSG-STREAM-EVAL-011 `Leaf-stage invariant scoring`

The evaluator applies the benchmark profile's leaf model to the leaf membership
artifact to verify exact occupancy, full coverage, one-cluster-per-entity
assignment, and absence of empty declared clusters.

If the benchmark profile uses strict alignment, occupancy checks apply directly
to real entities. If the profile uses deterministic synthetic padding, the
evaluator adds or consumes the declared synthetic entities before scoring and
still requires exact final occupancy against the combined evaluated entity set.
The same padding-aware scoring step also reports whether synthetic padding
concentrates into the minimum possible number of final clusters permitted by
the deterministic procedure. Synthetic padding identities remain stably tagged,
collision-free with real-entity identities in the evaluated corpus, and
externally distinguishable from real benchmark members.

### DSG-STREAM-EVAL-012 `Leaf-stage locality scoring`

The evaluator computes same-leaf neighborhood coherence from benchmark ground
truth and the leaf membership artifact over real entities only.

This design intentionally does not synthesize sibling structure from unrelated
clusters, so same-or-sibling locality remains deferred to a future evaluator
that owns explicit hierarchy structure.

### DSG-STREAM-EVAL-013 `Leaf-stage compression scoring`

The evaluator computes local compression-friendliness by applying the
benchmark-declared compression or quantization method per final cluster and
comparing the resulting real-entity reconstruction quality against the declared
global baseline over the real benchmark dataset.

The same scoring surface records the declared local-versus-global delta
semantics and the per-cluster or per-bucket distribution needed to explain
whether compression benefit is broad or concentrated.

### DSG-STREAM-EVAL-014 `Result taxonomy`

The evaluator result model separates:

- shared-contract prerequisite checks needed before comparative interpretation
- must-pass gates that decide campaign survival
- comparative metrics used to rank only surviving candidates

Each metric, gate, or deferred research-goal record carries traceability to its
motivating research goal and is tagged as direct, proxy, or deferred. When a
deferred record corresponds to a frozen section-1 benchmark-contract item, the
same result model also names the later proof surface expected to discharge it.

### DSG-STREAM-EVAL-015 `Deferred research requirement handling`

When a research goal from `docs/research/clustering.md` cannot be proven through
the shared streaming clustering boundary and benchmark fixtures alone, the
benchmark profile and result schema record that goal as deferred rather than
misreporting a proxy as full proof. Deferred status therefore limits the
crate's claims, not the parent end-state requirement.

The same deferred-record mechanism also carries frozen section-1 benchmark-
contract items that remain outside the direct section-4 proof surface, such as
same-or-sibling locality targets, routing targets, bounded hierarchy-shape
constraints, parent-summary obligations, persistence obligations, and
multi-thread reproducibility obligations beyond the direct observable boundary.

### DSG-STREAM-EVAL-016 `Output artifacts`

The evaluator emits:

- a machine-readable run report per candidate
- a machine-readable comparative campaign report spanning all candidates
- a human-readable scorecard that summarizes gates, direct metrics, proxy
  metrics, deferred goals, and survivor ranking

Output artifacts remain source-neutral. When block-store-backed corpus
references are used, provenance expands to identify those sources, but run
reports and comparative scoring semantics do not change.

### DSG-STREAM-EVAL-017 `Failure taxonomy`

Evaluator failures are reported through an evaluator-owned structured taxonomy
that distinguishes invalid evaluator configuration, candidate-reported
shared-contract failure, evaluator-owned gate failure, invalid corpus-source
references, corpus-loading failures encountered through block-store-backed
inputs, zip-archive open/read failures, temporary writable-layer creation
failures, and incomplete or unsupported measurement due to a deferred research
requirement.

Each structured failure also carries a stable error code, deterministic
human-readable message, and enough artifact-hygiene state to prove that a
failed section-4 execution did not expose success-shaped completion artifacts or
later comparative metrics beyond the gate that failed.

### DSG-STREAM-EVAL-018 `Explicit non-goal boundary`

This revision does not model or claim proof of full end-to-end LexonGraph
hierarchy properties that still require artifacts beyond the staged leaf and
hierarchy-construction boundaries, such as parent-summary accuracy or
stability, persisted-hierarchy routing, or durable storage semantics.

The future end-to-end evaluator on top of the streaming indexer and search
specifications is called out as a separate later line rather than collapsed into
this crate. This non-goal boundary prevents false proof claims while remaining
subordinate to the broader staged plan and end-state contract.

### DSG-STREAM-EVAL-019 `Verification artifacts`

The repository includes executable verification artifacts covering benchmark
profile validation, candidate execution, observable determinism checking,
inline-fixture and block-store-backed corpus-source handling, leaf membership
materialization, occupancy/locality/compression scoring, hierarchy-stage
construction scoring, comparative scorecard generation, failure classification,
and deferred-goal reporting for the evaluator crate.

### DSG-STREAM-EVAL-020 `Section-4 benchmark suite layer`

Above individual benchmark profiles, the repository defines a reproducible
section-4 benchmark suite that materializes the profiles, corpus assets, and
supporting metadata needed for repeated leaf-stage candidate screening.

The suite also declares the metric contract and fixed neighborhood size used by
its deterministic exact-neighbor ground-truth assets and same-leaf locality
reports.

For the repository-owned checked-in section-4 screening panel in this revision,
that fixed neighborhood size is top-10.

This suite remains subordinate to the evaluator's section-4 leaf-stage
boundary: it orchestrates comparative leaf-partition studies and feeds the
crate's later hierarchy-stage workflow without replacing it. It is a
repository-owned screening layer rather than the full staged evaluator by
itself.

The suite layer also owns deterministic invalid-configuration rejection for the
malformed suite-level controls that would otherwise make asset generation
ambiguous, including empty suite identity, empty profile sets, and zero-valued
positive-count controls such as `leaf_size`, `dimensions`, `batch_size`, and
`neighbor_count`.

The same suite layer owns the frozen benchmark-contract declaration for each
experiment track, including the primary `leaf_size`, any declared sensitivity
sizes, metric family, transformed-metric policy if any, dimensionality
contract, alignment-policy family, quantization baseline policy over real
entities only, any declared search-target threshold and beam-width policy that
must carry forward to later routing phases, declared floating-point profile,
declared candidate-threading model, and declared hardware profile.

### DSG-STREAM-EVAL-021 `Repository-owned corpus panel`

The section-4 benchmark suite defines a repository-owned corpus panel whose
benchmark identities cover the corpus families needed for leaf-stage comparison:

- a real-world harvested corpus
- well-clustered synthetic data
- weak-cluster or uniform synthetic data
- anisotropic or manifold synthetic data
- near-duplicate-heavy data
- deterministic size-scaled variants where scalability assessment is required

The first complete checked-in panel in this revision includes at least one
checked-in profile for each required family plus stable scale-tier identities
for the profiles used in repeated comparison.

Each panel member carries a stable corpus identity plus a deterministic
construction or harvesting policy so that repeated candidate comparisons remain
reproducible.

For corpus families that participate in repeated scale comparison, the panel
also carries stable small, medium, and large tier identities or deterministic
nearest-practical equivalents together with the rule that relates those tiers.

In this revision, the checked-in panel realizes three repeated-comparison tiers
for the harvested, well-clustered, weak-cluster or uniform, anisotropic or
manifold, and near-duplicate-heavy families, and keeps one separate
deterministic-padding near-duplicate profile for alignment-policy comparison.

### DSG-STREAM-EVAL-022 `Deterministic ground-truth assets`

For any corpus used in same-leaf locality evaluation, the benchmark suite
materializes deterministic exact-neighbor ground-truth assets derived from the
benchmark entities under the suite's declared metric contract.

For the checked-in section-4 screening panel in this revision, those assets are
top-10 neighborhoods.

These assets are benchmark-owned supporting artifacts rather than
candidate-owned outputs. They remain leaf-stage artifacts and exclude synthetic
padding entities from externally reported locality scoring.

This ground-truth layer also owns deterministic rejection of invalid exact-
neighbor preconditions, including corpora too small for the declared
`neighbor_count` and cosine-metric inputs containing zero-norm embeddings.

### DSG-STREAM-EVAL-023 `Deterministic real-world corpus harvesting`

The suite derives a real-world benchmark corpus from repository-approved
block-store-backed source data through a deterministic harvesting policy that
defines:

- the source identity and root block to traverse
- the entity-identity extraction contract
- the embedding admissibility contract
- the deterministic subset-selection rule for any scale tiers

This revision checks in at least one harvested benchmark corpus asset produced
through that deterministic workflow so the repository-owned screening panel is
not synthetic-only.

This design keeps large benchmark corpora reproducible even when they originate
from external block-store material rather than hand-authored fixture JSON.

The harvesting workflow also classifies deterministic invalid inputs at the
evaluator boundary, including malformed entity-identity metadata, malformed
`synthetic` metadata, embeddings that fail the declared admissibility contract,
and harvested sources that do not retain enough real entities after filtering.

### DSG-STREAM-EVAL-024 `Zip-native large benchmark assets`

Large repository-managed benchmark corpora are stored as `.zip` assets in the
git tree and consumed directly through the evaluator's existing
zip-archive-backed corpus-source path.

The checked-in harvested benchmark corpus asset required by this revision uses
that same zip-native direct-consumption path.

The user workflow therefore does not require a manual pre-decompression step.
Any writable layer needed for block-store semantics remains evaluator-managed
temporary state provided by the existing filesystem-over-zip overlay design.

### DSG-STREAM-EVAL-025 `Section-4 screening workflow`

The benchmark suite can execute a section-4 screening workflow that runs the
same compared candidates against the same corpus-panel profiles and records, at
minimum:

- exact-occupancy and related fixed-capacity invariant outcomes
- repeated-run observable determinism
- same-leaf locality against exact-neighbor ground truth
- local-versus-global compression gain
- strict-alignment versus deterministic-padding outcomes where both are
  relevant, including the declared normalized build-cost comparison used to
  judge the alignment-policy tradeoff

For the checked-in section-4 screening panel in this revision, the workflow
executes across the expanded synthetic-plus-harvested profile set and uses
top-10 exact-neighbor ground truth for locality-scored profiles.

The outputs are intended to support down-selection of candidate leaf strategies
for later hierarchy-stage work without claiming hierarchy-stage proof.

Before candidate execution, the workflow validates alignment-policy
preconditions and surfaces deterministic invalid-configuration failures for
strict-alignment corpora that are not divisible by `leaf_size` and for
deterministic-padding corpora that are empty or already divisible by
`leaf_size`.

If a candidate fails a hard invariant gate for a configuration, the workflow
stops further comparative metric evaluation for that candidate/configuration
pair and preserves only the failure-classified artifact surface needed for
deterministic reporting and survivor selection.

### DSG-STREAM-EVAL-026 `Leaf-stage build-cost reporting`

For section-4 benchmark executions, the evaluator reports a small leaf-stage
resource surface sufficient for comparing candidate strategies across corpus
scale tiers.

This revision keeps that resource surface narrow: deterministic reporting of
evaluated entity count, scale-tier identity, build time per vector or an
equivalent benchmark-declared normalized build-cost measure, and peak build
memory is sufficient across the checked-in section-4 panel's scale tiers.

When the frozen experiment-track contract carries a later-phase loaded-index
memory obligation but section-4 does not materialize a persisted loadable
artifact, the workflow preserves that obligation as deferred rather than
omitting it. This does not widen the crate into a full query-runtime or
end-to-end service-level evaluator.

### DSG-STREAM-EVAL-027 `Repository-owned concrete section-4 candidates`

The checked-in section-4 workflow includes a repository-owned set of
non-fixture candidate implementations that are reusable outside the evaluator
and are entered solely through the shared streaming trainer/classifier
contract.

In this revision, the runnable section-4 comparison set includes:

- reusable repository crates `crates/lexongraph-pca-chunking`,
  `crates/lexongraph-directional-pca`, and `crates/lexongraph-dcbc-streaming`
- evaluator-owned deterministic representatives for recursive balanced
  partitioning, space-filling-curve chunking, graph-neighborhood balancing,
  hybrid coarse rebalance, and random-shuffle exact chunking

### DSG-STREAM-EVAL-028 `Stable candidate identity in reports`

Section-4 reports and scorecards surface stable candidate identities for the
full checked-in runnable section-4 set in the same result model used for
fixture candidates.

### DSG-STREAM-EVAL-029 `Evaluator-owned registration defaults`

For repository-owned reusable candidates that require algorithm-local parameter
objects in addition to the shared clustering configuration, the evaluator owns
deterministic registration defaults inside its ordinary registered-candidate
surface rather than widening the shared candidate contract.

In this revision, the evaluator provides deterministic default
`DirectionalPcaParams` for `lexongraph-directional-pca`, while
`lexongraph-dcbc-streaming` is registered directly from the shared clustering
configuration.

### DSG-STREAM-EVAL-030 `Explicit incompatibility outcomes`

Candidate-specific incompatibilities that are still expressed through the
shared contract are surfaced as ordinary candidate outcomes in the same report
model used for other candidate failures.

In this revision, that includes at least:

- `lexongraph-directional-pca` rejection of shared balance constraints
- `lexongraph-dcbc-streaming` rejection of zero-norm embeddings
- `lexongraph-dcbc-streaming` rejection of unsupported shared balance settings

The evaluator does not pre-filter such candidates out of the checked-in
registration surface; it lists them, allows selection, and records explicit
outcomes when a profile triggers one of those limits.

### DSG-STREAM-EVAL-031 `Frozen section-1 benchmark contract`

The section-4 suite defines one explicit frozen benchmark contract per
experiment track so repeated leaf-stage comparisons do not silently move the
goalposts.

That contract records:

- the metric family and any transformed-metric policy
- the exact role of the metric family in build-time comparison, compression
  scoring, deferred summary obligations, and any later routing obligations
- the primary `leaf_size` and any declared sensitivity sizes
- the dimensionality contract, including deterministic out-of-range rejection
  semantics where applicable
- the alignment-policy family
- the quantization or compression baseline policy over real entities only
- any declared search-target threshold and beam-width policy carried forward to
  later routing phases
- the declared floating-point profile
- the declared candidate-threading model
- the declared reduction-order strategy for any deterministic aggregate
  computation permitted by the track
- the declared hardware profile

Each item is also labeled as either directly measured during section-4
screening or deferred to a later proof surface.

### DSG-STREAM-EVAL-032 `Deferred obligation ledger`

The suite and campaign report model include an explicit deferred-obligation
ledger for frozen section-1 contract items that remain outside the leaf-stage
direct-evidence boundary.

In this revision, the ledger can record at least:

- same-or-sibling locality targets carried forward from same-leaf proxy scoring
- routing targets or routing-procedure assumptions
- beam-width policies or related routing-study assumptions
- bounded fanout and depth constraints
- parent-summary accuracy or stability obligations
- parent-summary metric or dispersion-contract obligations
- refinement-contract obligations such as `beta = Disp(C) / Disp(P)` semantics
  and any declared penultimate-layer exception
- serialization round-trip and persisted-artifact durability obligations
- multi-thread reproducibility obligations beyond the direct section-4
  observable boundary
- held-out query-set or equivalent later-phase routing-workload identities

Each ledger entry names the deferred target, why section-4 cannot discharge it,
and which later evaluation line is expected to prove it.

### DSG-STREAM-EVAL-033 `Deterministic scale-tier contract`

For corpus families used in repeated section-4 size-tier comparison, the suite
defines a deterministic scale-tier contract that binds:

- a stable small, medium, and large identity or deterministic nearest-practical
  equivalents
- the tier-growth rule for the family
- the corpus asset and exact-neighbor ground-truth asset associated with each
  tier identity

The same contract allows a checked-in harvested real-world corpus family to
participate in the same tiered comparison surface used by synthetic families.

Later routing phases may reuse these tier identities. For any corpus family
intended to carry forward into those routing phases, the scale-tier contract
declares held-out query-set identities even though section-4 execution does not
depend on consuming those later-phase query assets.

The first complete checked-in section-4 panel in this revision includes at
least one such held-out query-set identity for a harvested real-world corpus
family. When the experiment track declares later-phase identities, the suite
preserves them in the frozen contract and deferred ledger rather than dropping
them from the section-4 artifact model. Held-out query-set identities in this
revision also carry checked-in materialized asset paths so later routing work
can reuse the frozen corpus/tier identity without regenerating unnamed inputs.

### DSG-STREAM-EVAL-034 `Hard-gate termination and artifact hygiene`

The section-4 workflow treats hard invariant gates as short-circuit conditions
for a candidate/configuration execution.

When a candidate fails such a gate, the workflow:

1. stops further comparative metric evaluation for that candidate under that
   configuration
2. emits deterministic failure-classified artifacts without presenting a
   success-shaped completed result for the rejected configuration
3. records artifact-hygiene evidence showing that later comparative metrics and
   success-shaped completion artifacts were not exposed after the failing gate
4. preserves enough campaign state to identify which candidates, if any, remain
   survivors eligible for later hierarchy-stage comparison

For the canonical checked-in section-4 run, the workflow writes a machine-
readable suite report plus human-readable scorecard and survivor-decision
artifacts beside the per-profile outputs.

### DSG-STREAM-EVAL-035 `Metric and execution semantics contract`

In addition to naming frozen benchmark items, each section-4 experiment track
defines the semantic rules needed to interpret candidate-comparable metrics and
execution behavior.

This contract names:

- the exact build, locality, compression, and deferred-routing metric roles
- any ordering-preservation obligation attached to a transformed metric policy
- the metric-contract consistency checks and reported audit results that show
  build-time comparison, compression scoring, deferred summary obligations, and
  any carried-forward routing obligations use the declared metric coherently
- the compatible dispersion functional used by deferred summary or refinement
  obligations
- the threading model and deterministic reduction-order strategy used by the
  track
- whether 1-thread versus N-thread bitwise observable identity is measured
  directly in section-4 or carried as a deferred obligation

### DSG-STREAM-EVAL-036 `Later-phase obligation identities`

When section-4 freezes obligations that a later evaluator line must discharge,
the suite preserves the artifact and workload identities needed to continue the
proof chain.

This includes, when declared by the experiment track:

- held-out query-set or later routing-workload identities
- later-phase hierarchy, summary, persistence, or service-level artifact
  identities
- the later evaluation line expected to consume each preserved identity

For the first complete checked-in section-4 panel in this revision, the
preserved identity set includes at least one held-out query-set identity for a
harvested real-world corpus family that later routing phases are expected to
consume.

### DSG-STREAM-EVAL-037 `Deterministic survivor-selection rule`

The section-4 workflow applies a deterministic carry-forward rule after hard
gates and direct measurements have been recorded.

That rule:

1. rejects any candidate/configuration that fails a hard invariant gate
2. ranks surviving candidates using same-leaf locality evidence, declared local
   compression benefit, and normalized leaf-stage build-cost evidence
3. prevents build-cost comparisons from rescuing a hard-gate failure
4. defines deterministic tie-breaking when surviving candidates remain otherwise
   indistinguishable on the declared comparison surface

The checked-in workflow materializes that decision as a stable human-readable
survivor summary listing carried-forward candidates, average ranking evidence,
and rejected or non-carried-forward candidates.

### DSG-STREAM-EVAL-038 `Hierarchy-strategy registration surface`

Section-5 hierarchy construction uses an evaluator-owned registration surface
that combines:

- the surviving section-4 leaf-stage outputs
- a named hierarchy strategy
- the shared hierarchy-stage benchmark contract

The hierarchy-strategy registration surface remains evaluator-owned and does not
widen the shared streaming clustering trainer/classifier boundary that produced
the leaf-stage outputs.

When a strategy uses metric-sensitive grouping behavior such as nearest-centroid
packing or metric-dependent ordering, the strategy executes through evaluator-
owned metric-semantics helpers selected from the shared section-5 contract
rather than hard-wiring Euclidean-only grouping behavior.

### DSG-STREAM-EVAL-039 `Shared hierarchy-stage contract`

The evaluator defines a shared section-5 hierarchy-stage contract per compared
leaf-stage survivor set.

That contract records:

- the originating section-4 survivor identities and artifact references
- the fixed `f_min` and `f_max` bounds
- the declared depth-bound semantics and theoretical-bound interpretation
- the grouping-distance or equivalent ordering functional used by metric-
  sensitive hierarchy-construction decisions
- the compatible dispersion functional used for refinement checks
- the declared `beta` threshold
- the declared penultimate-layer `epsilon` exception and its admissibility
  conditions
- the hierarchy-stage build-throughput and memory-reporting semantics

The same contract also carries a deterministic compatibility rule that states
which grouping functionals and refinement-dispersion functionals are valid for
the declared metric semantics. Unsupported or inconsistent combinations are
rejected before pair execution begins.

### DSG-STREAM-EVAL-040 `Hierarchy-stage pair execution`

For each surviving leaf strategy × hierarchy strategy pair, the evaluator
materializes a full tree and computes the direct section-5 evidence surface.

That evidence surface includes:

- fanout compliance
- detection of single-child internal nodes
- depth relative to the declared bound
- per-edge refinement coefficients computed with the declared compatible
  dispersion functional
- recorded uses of the declared `epsilon` exception
- the effective grouping functional used by metric-sensitive strategy decisions
- the effective refinement-dispersion functional used by `beta` and
  `epsilon`-gated checks
- the pair's metric-semantics consistency result
- hierarchy-stage build-throughput and peak-memory reporting

### DSG-STREAM-EVAL-041 `Hierarchy-stage hard gates`

The hierarchy-stage workflow applies deterministic hard-gate rejection before
later pair ranking or carry-forward decisions.

Pairs are rejected when they violate declared fanout bounds, emit single-child
internal nodes, exceed the declared depth bound, violate the declared
refinement contract outside the admitted `epsilon` scope, or apply the
`epsilon` exception outside its declared penultimate-layer admissibility
conditions.

The same hard-gate path rejects pairs whose declared grouping functional or
refinement-dispersion functional is unsupported for the selected metric
semantics or whose declared combination fails the contract's deterministic
compatibility rule.

### DSG-STREAM-EVAL-042 `Cross-stage provenance and carry-forward`

Hierarchy-stage reports retain explicit traceability to the originating
section-4 survivor set. The artifact model therefore carries enough provenance
to reconstruct which leaf-stage profile, suite, or survivor decision produced
the leaf-stage inputs consumed by each compared hierarchy-stage pair.

The same artifact model emits a deterministic carry-forward summary identifying
which leaf-strategy × hierarchy-strategy pairs remain eligible for the later
parent-summary and routing phases.

The same artifact model also records the effective grouping functional,
effective refinement-dispersion functional, and metric-semantics consistency
result used by each compared pair so non-Euclidean section-5 execution is
auditable rather than implicit.

### DSG-STREAM-EVAL-043 `Remaining deferred hierarchy obligations`

Even after section-5 hierarchy construction is added, the crate still records
parent-summary accuracy or stability, routing recall or latency, beam-width
outcomes, serialization identity, persistence durability, and broader
service-level behavior as deferred unless a later specification expands the
boundary again.

Section-5 direct hierarchy measurements therefore remain staged evidence toward
the parent research goals rather than proof of the later plan sections.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-STREAM-EVAL-001 | REQ-STREAM-EVAL-002 |
| DSG-STREAM-EVAL-002 | REQ-STREAM-EVAL-001, REQ-STREAM-EVAL-004, REQ-STREAM-EVAL-005 |
| DSG-STREAM-EVAL-003 | REQ-STREAM-EVAL-003 |
| DSG-STREAM-EVAL-004 | REQ-STREAM-EVAL-004, REQ-STREAM-EVAL-005 |
| DSG-STREAM-EVAL-005 | REQ-STREAM-EVAL-006, REQ-STREAM-EVAL-023, REQ-STREAM-EVAL-026, REQ-STREAM-EVAL-027 |
| DSG-STREAM-EVAL-006 | REQ-STREAM-EVAL-007 |
| DSG-STREAM-EVAL-007 | REQ-STREAM-EVAL-008 |
| DSG-STREAM-EVAL-008 | REQ-STREAM-EVAL-009, REQ-STREAM-EVAL-024, REQ-STREAM-EVAL-028 |
| DSG-STREAM-EVAL-008A | REQ-STREAM-EVAL-022, REQ-STREAM-EVAL-023, REQ-STREAM-EVAL-024, REQ-STREAM-EVAL-026, REQ-STREAM-EVAL-027 |
| DSG-STREAM-EVAL-008B | REQ-STREAM-EVAL-002, REQ-STREAM-EVAL-022, REQ-STREAM-EVAL-025, REQ-STREAM-EVAL-028 |
| DSG-STREAM-EVAL-008C | REQ-STREAM-EVAL-027 |
| DSG-STREAM-EVAL-008D | REQ-STREAM-EVAL-028, REQ-STREAM-EVAL-029 |
| DSG-STREAM-EVAL-008E | REQ-STREAM-EVAL-030 |
| DSG-STREAM-EVAL-009 | REQ-STREAM-EVAL-011 |
| DSG-STREAM-EVAL-010 | REQ-STREAM-EVAL-010, REQ-STREAM-EVAL-022 |
| DSG-STREAM-EVAL-011 | REQ-STREAM-EVAL-017, REQ-STREAM-EVAL-018 |
| DSG-STREAM-EVAL-012 | REQ-STREAM-EVAL-019 |
| DSG-STREAM-EVAL-013 | REQ-STREAM-EVAL-020 |
| DSG-STREAM-EVAL-014 | REQ-STREAM-EVAL-012, REQ-STREAM-EVAL-013 |
| DSG-STREAM-EVAL-015 | REQ-STREAM-EVAL-013, REQ-STREAM-EVAL-021 |
| DSG-STREAM-EVAL-016 | REQ-STREAM-EVAL-014 |
| DSG-STREAM-EVAL-017 | REQ-STREAM-EVAL-015, REQ-STREAM-EVAL-022, REQ-STREAM-EVAL-028, REQ-STREAM-EVAL-029 |
| DSG-STREAM-EVAL-018 | REQ-STREAM-EVAL-021, REQ-STREAM-EVAL-025 |
| DSG-STREAM-EVAL-019 | REQ-STREAM-EVAL-016 |
| DSG-STREAM-EVAL-020 | REQ-STREAM-EVAL-031, REQ-STREAM-EVAL-036, REQ-STREAM-EVAL-047 |
| DSG-STREAM-EVAL-021 | REQ-STREAM-EVAL-032 |
| DSG-STREAM-EVAL-022 | REQ-STREAM-EVAL-033, REQ-STREAM-EVAL-050 |
| DSG-STREAM-EVAL-023 | REQ-STREAM-EVAL-032, REQ-STREAM-EVAL-034, REQ-STREAM-EVAL-049 |
| DSG-STREAM-EVAL-024 | REQ-STREAM-EVAL-035 |
| DSG-STREAM-EVAL-025 | REQ-STREAM-EVAL-031, REQ-STREAM-EVAL-036, REQ-STREAM-EVAL-048 |
| DSG-STREAM-EVAL-026 | REQ-STREAM-EVAL-037 |
| DSG-STREAM-EVAL-027 | REQ-STREAM-EVAL-038, REQ-STREAM-EVAL-042 |
| DSG-STREAM-EVAL-028 | REQ-STREAM-EVAL-008, REQ-STREAM-EVAL-038, REQ-STREAM-EVAL-042 |
| DSG-STREAM-EVAL-029 | REQ-STREAM-EVAL-004, REQ-STREAM-EVAL-039, REQ-STREAM-EVAL-040 |
| DSG-STREAM-EVAL-030 | REQ-STREAM-EVAL-041, REQ-STREAM-EVAL-042 |
| DSG-STREAM-EVAL-031 | REQ-STREAM-EVAL-006, REQ-STREAM-EVAL-031, REQ-STREAM-EVAL-043 |
| DSG-STREAM-EVAL-032 | REQ-STREAM-EVAL-013, REQ-STREAM-EVAL-021, REQ-STREAM-EVAL-044 |
| DSG-STREAM-EVAL-033 | REQ-STREAM-EVAL-032, REQ-STREAM-EVAL-033, REQ-STREAM-EVAL-034, REQ-STREAM-EVAL-045 |
| DSG-STREAM-EVAL-034 | REQ-STREAM-EVAL-015, REQ-STREAM-EVAL-036, REQ-STREAM-EVAL-046 |
| DSG-STREAM-EVAL-035 | REQ-STREAM-EVAL-006, REQ-STREAM-EVAL-043, REQ-STREAM-EVAL-051 |
| DSG-STREAM-EVAL-036 | REQ-STREAM-EVAL-013, REQ-STREAM-EVAL-044, REQ-STREAM-EVAL-045, REQ-STREAM-EVAL-052 |
| DSG-STREAM-EVAL-037 | REQ-STREAM-EVAL-036, REQ-STREAM-EVAL-046, REQ-STREAM-EVAL-053 |
| DSG-STREAM-EVAL-038 | REQ-STREAM-EVAL-054 |
| DSG-STREAM-EVAL-039 | REQ-STREAM-EVAL-006, REQ-STREAM-EVAL-055 |
| DSG-STREAM-EVAL-040 | REQ-STREAM-EVAL-003, REQ-STREAM-EVAL-056 |
| DSG-STREAM-EVAL-041 | REQ-STREAM-EVAL-015, REQ-STREAM-EVAL-057 |
| DSG-STREAM-EVAL-042 | REQ-STREAM-EVAL-014, REQ-STREAM-EVAL-058 |
| DSG-STREAM-EVAL-043 | REQ-STREAM-EVAL-021, REQ-STREAM-EVAL-059 |
