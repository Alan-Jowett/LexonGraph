<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Directional PCA Crate Validation

## Status

Draft validation specification for a Rust crate that implements single-layer
directional-PCA partitioning for LexonGraph.

## Validation Scope

These validation entries define the expected conformance surface for the
directional-PCA crate, including block-to-vector derivation, eligibility
classification, and successful partition materialization.

## Validation Entries

### VAL-DPCA-001

Inspect the crate's public surface.

**Pass condition:** the crate exposes one deterministic single-layer
directional-PCA API boundary over ordered block IDs, a block-store dependency,
typed parameters, typed outcomes, and explicit failures.

**Traces to:** REQ-DPCA-001, REQ-DPCA-002

### VAL-DPCA-002

Run the crate twice with the same ordered input block IDs, the same loaded
block contents, and the same parameters.

**Pass condition:** both runs produce identical groups, the same explicit
eligibility outcome, or the same explicit failure.

**Traces to:** REQ-DPCA-003, REQ-DPCA-013

### VAL-DPCA-003

Run the crate on the same logical input block set in different block-ID orders.

**Pass condition:** the crate preserves input-order semantics and does not
claim permutation equivalence.

**Traces to:** REQ-DPCA-003

### VAL-DPCA-004

Load one branch block with known branch-entry embeddings and one conformant leaf
block with a known leaf-entry embedding.

**Pass condition:** the crate derives each representative embedding as the
arithmetic centroid of that block's stored entry embeddings.

**Traces to:** REQ-DPCA-004

### VAL-DPCA-005

Cause representative-embedding derivation to encounter each of the following:

- a missing block ID
- a block-store failure
- a malformed or invalid loaded block
- an empty block embedding set
- incompatible embedding specifications
- unsupported, dimensionally inconsistent, or non-finite embeddings

**Pass condition:** each case fails explicitly.

**Traces to:** REQ-DPCA-005

### VAL-DPCA-006

Invoke the crate with invalid retained-dimension controls, non-positive
axis-resolution budget, non-positive temperature, and invalid eligibility
thresholds.

**Pass condition:** each invalid parameter case fails explicitly.

**Traces to:** REQ-DPCA-006

### VAL-DPCA-007

Inspect the execution path over a representative eligible fixture.

**Pass condition:** the layer-local PCA is realized by the repository PCA crate
surface rather than an undocumented independent PCA implementation.

**Traces to:** REQ-DPCA-007

### VAL-DPCA-008

Use a fixture with known retained PCA coordinates, centroid direction, and
explained variance.

**Pass condition:** the realized per-axis scores reflect both directional
coefficients and explained variance according to the configured `gamma`.

**Traces to:** REQ-DPCA-008

### VAL-DPCA-009

Use a fixture whose damped axis scores produce non-trivial allocation under a
configured axis-resolution budget.

**Pass condition:** the per-axis resolution counts follow the documented
temperature-controlled allocation rule and deterministic correction behavior.

**Traces to:** REQ-DPCA-009

### VAL-DPCA-010

Use a fixture whose retained PCA coordinates are unevenly distributed.

**Pass condition:** the conformant default assignment path uses quantile binning
rather than equal-width binning.

**Traces to:** REQ-DPCA-010

### VAL-DPCA-011

Run an eligible fixture that yields multiple populated grid cells.

**Pass condition:** the result contains one group per populated cell, each group
includes a numeric centroid vector and the ordered member block IDs assigned to
that cell, and no empty cells are materialized.

**Traces to:** REQ-DPCA-011

### VAL-DPCA-012

Run fixtures that violate the configured minimum input count, explained-variance
support threshold, and effective-rank threshold.

**Pass condition:** each case returns an explicit eligibility outcome instead of
a successful partition or a silent no-op.

**Traces to:** REQ-DPCA-012

### VAL-DPCA-013

Inspect the repository workspace and verification artifacts for the crate.

**Pass condition:** the repository contains the directional-PCA crate, the spec
package, executable tests realizing this validation surface, and Cargo workspace
wiring for the crate.

**Traces to:** REQ-DPCA-014, REQ-DPCA-015
