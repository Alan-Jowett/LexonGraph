<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Directional PCA Crate Requirements

## Status

Draft specification for a Rust crate that implements single-layer
directional-PCA partitioning for LexonGraph.

## Scope

This document specifies the crate-level requirements for a Rust crate that
realizes one directional-PCA partitioning layer derived from
`docs/arch/Directional PCA tree.md`.

This document does not define recursive tree construction, centroid block
materialization, shared cross-algorithm clustering traits, or transport and
storage APIs beyond consuming the block-storage trait.

## Terminology

In this spec package, `representative embedding` means the single numeric
vector derived for an input block before PCA and partitioning.

`Eligibility outcome` means an explicit, non-success partitioning outcome that
reports why a layer should not be partitioned under the configured layer
stability or minimum-size rules.

`Directional PCA layer run` means one complete invocation of the public crate
boundary that loads the ordered input block IDs, derives representative
embeddings, evaluates layer eligibility, performs one conformant directional-PCA
partitioning pass when eligible, and materializes the result or explicit
outcome.

## Requirements

### REQ-DPCA-001

The crate shall define the Rust API boundary for an algorithm-specific,
single-layer directional-PCA partitioning component for LexonGraph.

### REQ-DPCA-002

The public partitioning operation shall require:

- an ordered collection of input block IDs
- a `BlockStore` implementation used to load those blocks
- typed layer parameters controlling retained PCA dimensionality, axis
  resolution budget, variance blending, temperature, and eligibility
  thresholds

### REQ-DPCA-003

The crate shall preserve protocol-significant input order and shall not treat
permutation of the input block-ID collection as semantically equivalent input.

### REQ-DPCA-004

For each successfully loaded input block, the crate shall derive one
representative embedding as the arithmetic centroid of the embeddings stored in
that block.

For branch blocks, the contributing embeddings are the branch-entry embeddings.

For leaf blocks under the current block model, the contributing embedding is the
single leaf-entry embedding.

### REQ-DPCA-005

The crate shall fail explicitly when it cannot derive a valid representative
embedding for every requested input block, including at minimum:

- missing blocks
- block-store retrieval failures
- malformed or invalid loaded blocks
- empty embedding sets within an input block
- incompatible embedding specifications across inputs
- unsupported, dimensionally inconsistent, or non-finite embedding values

### REQ-DPCA-006

The crate shall fail explicitly on invalid parameter bounds, including at
minimum:

- invalid retained-dimension controls
- non-positive axis-resolution budgets
- non-positive temperature values
- invalid eligibility thresholds

### REQ-DPCA-007

The crate shall realize layer-local PCA by using the repository PCA crate
rather than redefining PCA decomposition behavior independently.

### REQ-DPCA-008

The crate shall compute per-axis allocation scores using both:

- centroid-direction coefficients
- explained-variance information

The conformant score shall be equivalent in effect to
`|alpha_i| * lambda_i^gamma`, where `gamma` is an explicit typed parameter.

### REQ-DPCA-009

The crate shall convert the per-axis scores into per-axis resolution using a
temperature-controlled allocation rule over an explicit axis-resolution budget,
with deterministic rounding and correction behavior.

### REQ-DPCA-010

The conformant default binning policy shall be quantile binning over the
retained PCA coordinates.

### REQ-DPCA-011

When the layer run is eligible and succeeds, the crate shall return a
collection of groups, where each group contains:

- a numeric centroid vector
- the ordered member block IDs assigned to that group

### REQ-DPCA-012

The crate shall expose an explicit eligibility outcome when the requested layer
should not be partitioned under configured minima or stability rules,
including at minimum:

- insufficient input count
- insufficient explained-variance support
- insufficient effective rank

### REQ-DPCA-013

Given identical ordered input block IDs, identical loaded block content, and
identical parameters, the conformant execution boundary shall return identical
groups, the same explicit eligibility outcome, or the same explicit failure.

### REQ-DPCA-014

The repository shall include a Rust crate and automated verification artifacts
that realize this specification package.

### REQ-DPCA-015

The repository shall wire the directional-PCA crate into the Cargo workspace.

## Out of Scope

This crate does not define or own:

- recursive tree construction over repeated layer runs
- materialization or storage of centroid blocks
- the shared clustering trait planned for future migration with DCBC
- alternate partitioning algorithms
- approximate search traversal or ranking
- block canonicalization, block validation, or block-ID derivation rules

## Relationship to Other Specifications

This document is derived from `docs/arch/Directional PCA tree.md` for the
single-layer crate boundary.

This document is subordinate to:

- `docs/specs/rust-pca-crate/` for PCA-crate behavior it consumes
- `docs/specs/rust-block-storage-trait/` for block-store contract behavior it
  consumes
- `docs/specs/rust-block-crate/` for typed block structure and block validity

If this document appears to conflict with those authorities for their owned
concerns, those authorities are normative.
