<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Directional PCA Crate Design

## Status

Draft design specification for a Rust crate that realizes streaming
directional-PCA clustering through the shared LexonGraph streaming clustering
contract.

## Design Goals

The crate design is intended to be:

- faithful to the directional-PCA algorithm described in
  `docs/arch/Directional PCA tree.md`
- conformant to the shared streaming clustering contract
- deterministic at the observable boundary
- explicit about exact-K failure behavior
- minimal after removal of the retired block-store surface

## Crate Boundary

The crate owns:

- a concrete streaming directional-PCA trainer implementation
- a concrete streaming directional-PCA classifier implementation
- mapping from shared streaming configuration to the crate's directional
  parameters
- pass-scoped directional-PCA fitting, partitioning, and stable cluster-ID
  realization
- the minimal retained state needed for same-dataset multi-pass refinement

The crate does not own:

- block loading or representative-embedding derivation from stored blocks
- recursive tree construction
- centroid block persistence
- the shared streaming trait definitions
- PCA eigendecomposition internals beyond invoking the PCA crate

## Design Entries

### DSG-DPCA-STREAM-001 `Composite normative boundary`

The crate depends on `docs/arch/Directional PCA tree.md` for the algorithm's
directional-PCA mechanics and stabilizer rationale, on
`docs/specs/rust-streaming-clustering-crate` for the shared trainer/classifier
contract, and on `docs/specs/rust-pca-crate` for PCA behavior.

The crate does not redefine those sources.

### DSG-DPCA-STREAM-002 `Concrete trainer/classifier realization`

The crate exposes one trainer type implementing `StreamingClusterTrainer` and
one classifier type implementing `StreamingClusterClassifier`.

### DSG-DPCA-STREAM-003 `Minimal public surface`

The public crate boundary is native to streamed embeddings and excludes the
retired block-store-backed single-layer API.

The observable type surface is limited to the configuration, trainer,
classifier, and diagnostics needed for the streaming contract plus
directional-PCA-specific configuration.

### DSG-DPCA-STREAM-004 `Shared configuration plus directional parameters`

Trainer construction is driven by:

- `StreamingClusteringConfig` for hard `K`, dimensionality, optional balance
  constraints, and deterministic seed behavior
- typed directional parameters for retained dimensions, cluster-cardinality
  mode, `gamma`, `tau`, and any retained eligibility or stability thresholds

The shared `cluster_count` is the hard observable cluster target for each
completed pass and for the final classifier.

Shared balance constraints are not realized as a directional-PCA balancing
policy in this scaled-down revision; if they are supplied, construction fails
through the shared invalid-configuration category.

### DSG-DPCA-STREAM-005 `Observable lifecycle`

The trainer follows the shared lifecycle:

`Idle -> Ingesting -> PassComplete -> Ingesting/TrainingComplete -> Classifier`

Illegal transitions are rejected deterministically through the shared invalid
transition category and enter the terminal error state required by the shared
contract.

### DSG-DPCA-STREAM-006 `Pass ingestion boundary`

`ingest_batch()` validates streamed embeddings through the shared malformed-input
surface and appends them to the current pass dataset order.

The crate does not materialize block IDs, loaded blocks, or representative
embedding records at this boundary.

Implementation-owned transient state at this boundary may scale with the
currently processed batch/chunk size and fixed configuration terms, but not
with the full completed-pass dataset size.

### DSG-DPCA-STREAM-007 `First-pass baseline and cross-pass continuity`

The first completed pass establishes the logical dataset for one training run.
Each later pass is validated against that baseline for:

- identical observed count
- identical ordered embedding content

Deviation fails explicitly before the trainer claims conformant refinement of
the same run.

The continuity mechanism must be realizable from streaming-compatible summaries
or fingerprints rather than crate-owned full-pass replay buffers.

### DSG-DPCA-STREAM-008 `Pass realization`

Each successful `finish_pass()` realizes one caller-visible directional-PCA pass
over the embeddings observed in that pass:

1. validate exact-K feasibility prerequisites
2. fit layer-local PCA through the repository PCA crate
3. select one explicit policy combination
4. realize either the legacy centroid-weighted/quantile path or the redesigned
   adaptive eigenvalue-bit/density-valley path
5. materialize populated cells from the resulting PCA-partition grid
6. if the populated cells already realize exactly `K` stable, non-empty
   clusters, expose them directly
7. otherwise, if the shortfall is attributable to duplicate-collapse, refine the
   collapsed duplicate members deterministically
8. otherwise either fail explicitly or, when underfull-success mode is selected,
   expose the best deterministic underfull realization
9. compute pass metrics and expose the pass report

The crate does not perform hidden extra passes.

Conformant execution uses streaming or mergeable PCA accumulation and other
streaming-compatible summaries over caller-replayed passes. A path that
requires materializing the full pass in implementation-owned memory or spill is
non-conformant.

When exact partitioning semantics require additional caller-visible replay
passes, earlier passes may legitimately report `AnalysisOnly` status before a
later `PartitionReady` pass exposes stable cluster IDs and realized cluster
count.

### DSG-DPCA-STREAM-009 `Legacy directional scoring`

For the legacy explicit/default path, the crate computes:

- the pass centroid in embedding space
- directional coefficients by projecting that centroid onto retained PCA axes
- per-axis scores by combining directional magnitude with explained variance
  using the configured `gamma`

The conformant legacy scoring effect is equivalent to
`|alpha_i| * lambda_i^gamma`.

### DSG-DPCA-STREAM-010 `Policy-specific resolution allocation`

For the centroid-weighted allocation policy, the crate log-damps the per-axis
scores, applies temperature-controlled normalization, converts the result into
per-axis resolution counts relative to the hard cluster target `K`, and applies
deterministic correction so the documented allocation semantics are satisfied.

For the eigenvalue log-bit allocation policy, the crate derives a total
split-bit budget from `K`, scores retained axes from eigenvalue-only log
weights, permits many axes to keep zero bits, and deterministically converts
the resulting sparse bit budget into per-axis bin counts.

### DSG-DPCA-STREAM-011 `Policy-specific coordinate partitioning`

When quantile binning is selected, the crate partitions each retained PCA
coordinate with quantile binning and assigns each embedding to one grid cell
determined by its retained-coordinate bin tuple.

When density-valley binning is selected, the crate instead estimates
one-dimensional density along each participating retained PCA axis and places
cuts at the deepest available valleys within the recursively selected segments.

The partitioning realization must remain compatible with caller-visible replay
passes and bounded implementation-owned working state. Exact quantile or
density-valley semantics therefore may require additional caller-visible replay
passes, but shall not require crate-owned retained-coordinate tables for the
full dataset.

### DSG-DPCA-STREAM-012 `Exact-K boundary`

The crate's observable contract requires exact `K` stable, non-empty clusters.

If the realized directional-PCA partition yields fewer populated cells than `K`,
the crate first checks whether duplicate-collapse recovery is applicable.

If recovery is not applicable, if recovery still cannot realize exact `K`, if
the partition yields more than `K` populated cells without a documented
deterministic collapse rule, or if exact-K otherwise cannot be satisfied without
changing the documented semantics, the trainer either fails explicitly through
the shared unsatisfiable-constraint or invalid-configuration surface as
appropriate, or in underfull-success mode exposes the best deterministic
realized count below `K`.

### DSG-DPCA-STREAM-013 `Stable cluster identity`

Externally visible cluster IDs remain stable across partition-ready completed
passes.

If repeated directional-PCA fits would otherwise permute internal group order,
the crate applies deterministic matching and tie-breaking before exposing pass
reports or classifier assignments.

### DSG-DPCA-STREAM-014 `Pass reports`

Each completed pass yields a `PassReport` whose:

- `observed_count` equals the number of embeddings ingested in that pass
- `requested_cluster_count` equals the configured target for that pass
- `quality_metric` is deterministic and comparable across passes within one run
- `balance_metric` is deterministic and comparable across passes within one run
- metric directions remain fixed for the full run
- readiness status indicates whether the pass is `AnalysisOnly` or
  `PartitionReady`

For `PartitionReady` passes:

- `realized_cluster_count` equals the number of stable clusters actually exposed
- `cluster_ids` match the stable externally visible cluster identifiers

When no explicit balance constraints are configured, `balance_metric` is zero.

### DSG-DPCA-STREAM-015 `Classifier realization`

After `complete_training()`, `into_classifier()` consumes the trainer and yields
a classifier that uses the final stable directional-PCA partition state to
assign valid embeddings deterministically into `[0, R)`, where `R` is the
realized cluster count from the final pass.

The classifier reuses the shared malformed-input surface.

If multiple refined clusters remain geometrically indistinguishable to the
classifier surface, assignment breaks ties deterministically by the stable
externally visible cluster-ID order.

### DSG-DPCA-STREAM-016 `Error mapping`

The observable boundary maps failures into the shared error categories:

- invalid configuration
- invalid transition
- unsatisfiable constraint
- malformed input

Directional-PCA-specific diagnostics may still appear in messages or internal
helpers, but the public category surface remains aligned with the shared
contract.

### DSG-DPCA-STREAM-017 `Verification realization`

The repository includes automated tests that exercise both:

- directional-PCA-specific mechanics at the crate's conformant boundary
- the shared streaming clustering conformance helpers

### DSG-DPCA-STREAM-018 `Dead-code removal`

The concrete crate realization removes public types, helpers, and verification
artifacts whose only purpose was supporting the retired block-store-backed API.

The retained implementation is intentionally the minimal code needed for the
scaled-down native streaming directional-PCA boundary.

### DSG-DPCA-STREAM-019 `Duplicate-collapse detection`

After populated cells are materialized from quantile-bin tuples and before exact
`K` failure is emitted, the crate checks whether the shortfall is attributable
to duplicate-collapse.

The conformant duplicate-collapse detection identifies populated cells that can
only grow additional clusters by subdividing members that remain
indistinguishable in retained PCA coordinates.

### DSG-DPCA-STREAM-020 `Stable duplicate refinement`

When duplicate-collapse detection succeeds and first-pass `Observed N >= K`, the
crate preserves the primary PCA-plus-quantile partition and deterministically
refines only the collapsed duplicate members.

The refinement tie-break is non-geometric and stable for the same pass dataset
order.

### DSG-DPCA-STREAM-021 `Narrow fallback scope`

Duplicate refinement is a narrow post-partition repair step. It does not replace
PCA fitting, directional scoring, temperature-controlled allocation, quantile
binning, malformed-input validation, or ordinary exact-K failure behavior.

### DSG-DPCA-STREAM-022 `Refined identity continuity`

Stable externally visible cluster IDs, pass reports, and classifier assignments
are derived from the final refined partition state. Replaying the same ordered
dataset across passes therefore reproduces the same observable cluster-ID
surface.

### DSG-DPCA-STREAM-023 `Adaptive retained-axis selection`

The crate may realize retained-coordinate truncation through an explicit
adaptive retained-axis policy instead of a fixed retained-dimension count.

When selected, the policy deterministically retains all eligible PCA axes, where
eligibility is bounded by realized PCA output rank and the configured
effective-rank guard, rather than by a caller-provided fixed count.

### DSG-DPCA-STREAM-024 `Density-valley binning`

The crate may realize retained-coordinate partitioning through an explicit
density-valley binning policy instead of quantile binning.

For each retained axis with assigned resolution `b_i`, the policy sorts the
retained coordinates, scores candidate cuts by valley depth, chooses the
strongest `b_i - 1` deterministic valleys with stable tie-breaking, and assigns
embeddings by the resulting interval boundaries.

### DSG-DPCA-STREAM-025 `Policy isolation`

Adaptive retained-axis selection and density-valley binning are opt-in policy
choices.

The conformant default path remains fixed retained-dimension truncation plus
quantile binning unless the caller explicitly selects the alternate policies.

### DSG-DPCA-STREAM-026 `Mixed-policy compatibility`

The crate accepts compatible retained-axis, allocation, and binning policy
combinations independently rather than limiting conformance to two bundled
presets.

When a selected allocation policy imposes a realizability bound on the number of
participating axes, the crate deterministically enforces that bound without
changing the semantics of the selected binning or cardinality policies.

### DSG-DPCA-STREAM-027 `Bounded implementation-owned working set`

The concrete crate bounds implementation-owned memory and scratch/storage
independently of the full completed-pass dataset size.

Allowed growth is limited to the currently processed chunk, PCA sufficient
statistics, streaming-compatible continuity summaries, and other fixed
configuration-bounded state.

### DSG-DPCA-STREAM-028 `Assessment rule for streaming-shaped APIs`

Conformance is determined by the actual execution model, not by a streaming-
shaped public API alone.

An implementation that exposes `ingest_batch()` but still requires full-pass
materialization or dataset spill is non-conformant under this revision.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-DPCA-STREAM-001 | REQ-DPCA-STREAM-002 |
| DSG-DPCA-STREAM-002 | REQ-DPCA-STREAM-001, REQ-DPCA-STREAM-003 |
| DSG-DPCA-STREAM-003 | REQ-DPCA-STREAM-004, REQ-DPCA-STREAM-020 |
| DSG-DPCA-STREAM-004 | REQ-DPCA-STREAM-005, REQ-DPCA-STREAM-006 |
| DSG-DPCA-STREAM-005..007 | REQ-DPCA-STREAM-007, REQ-DPCA-STREAM-008, REQ-DPCA-STREAM-009, REQ-DPCA-STREAM-010, REQ-DPCA-STREAM-019 |
| DSG-DPCA-STREAM-008 | REQ-DPCA-STREAM-009, REQ-DPCA-STREAM-011, REQ-DPCA-STREAM-015 |
| DSG-DPCA-STREAM-009 | REQ-DPCA-STREAM-012 |
| DSG-DPCA-STREAM-010 | REQ-DPCA-STREAM-013 |
| DSG-DPCA-STREAM-011 | REQ-DPCA-STREAM-014 |
| DSG-DPCA-STREAM-012 | REQ-DPCA-STREAM-015 |
| DSG-DPCA-STREAM-013 | REQ-DPCA-STREAM-016, REQ-DPCA-STREAM-017 |
| DSG-DPCA-STREAM-014 | REQ-DPCA-STREAM-016 |
| DSG-DPCA-STREAM-015 | REQ-DPCA-STREAM-018 |
| DSG-DPCA-STREAM-016 | REQ-DPCA-STREAM-019 |
| DSG-DPCA-STREAM-017 | REQ-DPCA-STREAM-021 |
| DSG-DPCA-STREAM-018 | REQ-DPCA-STREAM-020 |
| DSG-DPCA-STREAM-019 | REQ-DPCA-STREAM-022 |
| DSG-DPCA-STREAM-020 | REQ-DPCA-STREAM-015, REQ-DPCA-STREAM-023 |
| DSG-DPCA-STREAM-021 | REQ-DPCA-STREAM-015, REQ-DPCA-STREAM-024 |
| DSG-DPCA-STREAM-022 | REQ-DPCA-STREAM-016, REQ-DPCA-STREAM-017, REQ-DPCA-STREAM-018 |
| DSG-DPCA-STREAM-023 | REQ-DPCA-STREAM-025 |
| DSG-DPCA-STREAM-024 | REQ-DPCA-STREAM-026 |
| DSG-DPCA-STREAM-025 | REQ-DPCA-STREAM-014, REQ-DPCA-STREAM-027 |
| DSG-DPCA-STREAM-026 | REQ-DPCA-STREAM-013, REQ-DPCA-STREAM-028 |
| DSG-DPCA-STREAM-027 | REQ-DPCA-STREAM-029 |
| DSG-DPCA-STREAM-028 | REQ-DPCA-STREAM-030 |
