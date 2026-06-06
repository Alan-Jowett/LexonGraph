<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Directional PCA Crate Design

## Status

Draft design specification for a Rust crate that implements single-layer
directional-PCA partitioning for LexonGraph.

## Design Goals

The crate design is intended to be:

- deterministic at the crate boundary
- explicit about block-to-vector derivation
- narrowly scoped to one partitioning layer
- aligned with the existing PCA and block-storage crates
- easy to migrate to a future shared clustering trait

## Crate Boundary

The crate owns:

- the typed directional-PCA layer input, output, and error surfaces
- representative-embedding derivation from loaded blocks
- layer eligibility evaluation
- directional axis scoring, axis-resolution allocation, and bucket assignment
- directional-PCA-oriented diagnostics and error taxonomy

The crate does not own:

- recursive orchestration across multiple layers
- centroid block persistence
- shared multi-algorithm trait ownership
- block canonicalization or storage backend implementation details
- PCA eigendecomposition internals beyond invoking the PCA crate

## Core Types

### DSG-DPCA-001 `Crate boundary`

The repository contains a dedicated `lexongraph-directional-pca` crate whose
public surface implements this specification package.

### DSG-DPCA-002 `DirectionalPcaLayerInput`

A typed input structure containing:

- ordered input block IDs
- typed layer parameters

The storage dependency is supplied separately as a `BlockStore` implementer at
the execution boundary rather than embedded inside the input value.

### DSG-DPCA-003 `DirectionalPcaLayerParams`

A typed parameter structure containing at minimum:

- retained PCA dimension count or equivalent truncation control
- axis-resolution budget
- variance exponent `gamma`
- temperature `tau`
- minimum input count
- minimum effective rank
- minimum explained-variance support

### DSG-DPCA-004 `DirectionalPcaLayerOutcome`

The public execution boundary returns one of:

- a successful partitioning result
- an explicit eligibility outcome
- an explicit failure

Eligibility is not represented as successful partitioning with synthetic
singleton groups.

### DSG-DPCA-005 `DirectionalPcaLayerResult`

A successful result contains:

- the representative embedding specification shared across the loaded inputs
- a collection of partition groups

### DSG-DPCA-006 `DirectionalPcaGroup`

Each group contains:

- a numeric centroid vector
- ordered member block IDs

Group member order preserves the input order induced by the requested block-ID
sequence.

### DSG-DPCA-007 `DirectionalPcaEligibility`

An explicit eligibility structure classifies at least:

- insufficient input count
- insufficient explained variance
- insufficient effective rank

### DSG-DPCA-008 `DirectionalPcaError`

An explicit error taxonomy covers at minimum:

- block-store failures
- missing blocks
- malformed or invalid loaded blocks
- empty block embedding sets
- incompatible embedding specifications
- unsupported embedding encodings
- invalid or non-finite decoded embedding values
- invalid parameter bounds
- PCA failures surfaced from the PCA crate
- invalid numeric state during scoring, allocation, or centroid materialization

## API Surface

### DSG-DPCA-009
`run_directional_pca_layer(input, store) -> Result<DirectionalPcaLayerOutcome, DirectionalPcaError>`

The public API exposes one deterministic single-layer partitioning operation
that accepts typed input plus a `BlockStore` implementation and returns either a
typed outcome or an explicit failure.

## Execution Flow

### DSG-DPCA-010 `Block loading and representative embedding derivation`

For each requested block ID, the crate:

1. loads the block through the storage trait
2. fails explicitly on absence or storage failure
3. inspects the typed block entries
4. decodes all entry embeddings according to the shared embedding specification
5. computes the arithmetic centroid of those entry embeddings as the block's
   representative embedding

For branch blocks, the centroid is taken over the branch-entry embeddings.

For leaf blocks under the current block model, the representative embedding is
the single leaf-entry embedding.

### DSG-DPCA-011 `Embedding compatibility boundary`

All requested inputs must agree on one embedding specification before PCA
begins.

The crate does not silently coerce between encodings or dimensionalities.

### DSG-DPCA-012 `Parameter validation`

Before PCA execution, the crate validates parameter bounds, including retained
dimension controls, positive axis budget, valid temperature, minimum input count
of at least two samples, and non-negative eligibility thresholds.

### DSG-DPCA-013 `Eligibility evaluation`

After representative embeddings are materialized and before bucket assignment,
the crate evaluates whether the layer is eligible to partition under the typed
minimum-count and numerical-stability thresholds.

If the layer is ineligible, the operation returns an explicit eligibility
outcome instead of fabricating a partition.

### DSG-DPCA-014 `PCA realization`

When eligible, the crate fits a layer-local PCA transform by invoking the
repository PCA crate on the ordered representative embeddings and truncates or
retains coordinates according to the typed parameters.

### DSG-DPCA-015 `Directional scoring`

The crate computes:

- the layer centroid in the original embedding space
- directional coefficients by projecting that centroid onto retained PCA axes
- per-axis scores by combining directional magnitude with explained variance
  using the configured `gamma`

### DSG-DPCA-016 `Axis allocation`

The crate log-damps the per-axis scores, applies temperature-controlled
normalization, converts the result into per-axis resolution counts under the
configured axis-resolution budget, and applies deterministic correction so the
final allocation satisfies the documented budget semantics.

### DSG-DPCA-017 `Quantile binning`

The conformant bucket-assignment path partitions each retained PCA coordinate
axis with quantile binning and assigns each representative embedding to one grid
cell determined by its retained-coordinate bin tuple.

### DSG-DPCA-018 `Group materialization`

For each populated grid cell, the crate materializes one output group whose
member IDs are the assigned block IDs in preserved input order and whose centroid
vector is the arithmetic centroid of the member representative embeddings in the
original embedding space.

Empty cells are not materialized as groups.

### DSG-DPCA-019 `Determinism boundary`

The conformant execution boundary is the public crate behavior claimed by this
specification package.

Conformance requires that repeated executions over the same ordered inputs,
loaded block contents, and parameters produce the same groups, the same
eligibility outcome, or the same explicit failure.

### DSG-DPCA-020 `Repository realization`

This specification package shall be realized as a concrete Rust crate in the
repository, and that implementation shall expose the public API and behavioral
surface defined by this document.

### DSG-DPCA-021 `Verification realization`

The repository shall include automated tests that realize the validation entries
in `docs/specs/rust-directional-pca-crate/validation.md`, with each validation
entry mapped to one or more executable tests.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-DPCA-001 | REQ-DPCA-001, REQ-DPCA-014, REQ-DPCA-015 |
| DSG-DPCA-002..004 | REQ-DPCA-001, REQ-DPCA-002, REQ-DPCA-011, REQ-DPCA-012 |
| DSG-DPCA-005..008 | REQ-DPCA-011, REQ-DPCA-012, REQ-DPCA-014 |
| DSG-DPCA-009 | REQ-DPCA-001, REQ-DPCA-002, REQ-DPCA-011, REQ-DPCA-012 |
| DSG-DPCA-010..011 | REQ-DPCA-004, REQ-DPCA-005 |
| DSG-DPCA-012 | REQ-DPCA-006 |
| DSG-DPCA-013 | REQ-DPCA-012 |
| DSG-DPCA-014 | REQ-DPCA-007 |
| DSG-DPCA-015 | REQ-DPCA-008 |
| DSG-DPCA-016 | REQ-DPCA-009 |
| DSG-DPCA-017 | REQ-DPCA-010 |
| DSG-DPCA-018 | REQ-DPCA-003, REQ-DPCA-011 |
| DSG-DPCA-019 | REQ-DPCA-003, REQ-DPCA-013 |
| DSG-DPCA-020..021 | REQ-DPCA-014, REQ-DPCA-015 |
