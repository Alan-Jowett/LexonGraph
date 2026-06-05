<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust PCA Crate Validation

## Status

Draft validation specification for a Rust crate that implements deterministic,
streaming-first PCA transforms for LexonGraph.

## Validation Scope

These validation entries define the expected conformance surface for the PCA
crate, its deterministic artifact encodings, and its affine transform helpers.

## Validation Entries

### VAL-PCA-001

Run streaming accumulation twice on the same ordered vectors, and also build
two partial accumulators and merge them with the same merge tree.

**Pass condition:** repeated executions produce the same sufficient statistics
and the same finalized transform bytes.

**Traces to:** REQ-PCA-003, REQ-PCA-004, REQ-PCA-005, REQ-PCA-008, REQ-PCA-009,
REQ-PCA-024

### VAL-PCA-002

Invoke accumulator update and merge, transform application, reconstruction, and
truncation with mismatched dimensionality.

**Pass condition:** each mismatch fails explicitly with a typed dimension
failure.

**Traces to:** REQ-PCA-006, REQ-PCA-032

### VAL-PCA-003

Finalize one empty accumulator and one single-sample accumulator.

**Pass condition:** both fail explicitly instead of producing an invalid PCA
transform.

**Traces to:** REQ-PCA-007, REQ-PCA-032

### VAL-PCA-004

Fit the same data twice.

**Pass condition:** the resulting serialized PCA transform bytes are identical.

**Traces to:** REQ-PCA-008, REQ-PCA-024

### VAL-PCA-005

Merge fixed partial accumulators built over disjoint partitions of the same
data.

**Pass condition:** the merged covariance and finalized transform agree with the
single-pass result within the documented floating-point tolerance, and repeated
runs with the same merge tree yield identical serialized transform bytes.

**Traces to:** REQ-PCA-008, REQ-PCA-009, REQ-PCA-024

### VAL-PCA-006

Apply and then reconstruct a full-rank transform over representative inputs.

**Pass condition:** reconstruction matches the original inputs within the
documented tolerance.

**Traces to:** REQ-PCA-010, REQ-PCA-011, REQ-PCA-012

### VAL-PCA-007

Truncate a full-rank transform.

**Pass condition:** the truncated transform preserves the same mean, the first
`k` basis vectors, and the first `k` explained-variance entries.

**Traces to:** REQ-PCA-013, REQ-PCA-015

### VAL-PCA-008

Finalize PCA on fixtures with tied or repeated eigenvalues.

**Pass condition:** eigenvalue ordering and the canonical sign rule are
deterministic and stable.

**Traces to:** REQ-PCA-017, REQ-PCA-018

### VAL-PCA-009

Finalize PCA on a rank-deficient fixture.

**Pass condition:** the crate handles the degeneracy explicitly and does not
emit undocumented non-finite or structurally invalid transforms.

**Traces to:** REQ-PCA-019, REQ-PCA-029, REQ-PCA-032

### VAL-PCA-010

Compose two PCA transforms whose mean offsets are non-zero.

**Pass condition:** the composed affine transform matches `b(a(v))` and is not
computed as raw basis multiplication alone.

**Traces to:** REQ-PCA-020, REQ-PCA-021

### VAL-PCA-011

Construct two full-rank PCA transforms over the same input dimensionality and
compute an exact delta or rebase.

**Pass condition:** the delta matches `to(from^{-1}(x))` for representative
inputs.

**Traces to:** REQ-PCA-022, REQ-PCA-023

### VAL-PCA-012

Construct a truncated source transform and compute a reconstructing delta or
rebase.

**Pass condition:** the resulting delta is marked reconstructing and its result
matches the explicit reconstruct-then-apply path within the documented
tolerance.

**Traces to:** REQ-PCA-022, REQ-PCA-023

### VAL-PCA-013

Serialize and deserialize both PCA and affine transforms.

**Pass condition:** roundtrip preserves the transform exactly.

**Traces to:** REQ-PCA-024

### VAL-PCA-014

Quantize and dequantize representative transforms, including half-integer
rounding fixtures and clipping fixtures.

**Pass condition:** quantization is deterministic, uses the documented rounding
and clipping rules, excludes `-128` for 8-bit symmetric quantization, and
dequantization produces a valid transform.

**Traces to:** REQ-PCA-025

### VAL-PCA-015

Validate transforms with non-finite values, shape mismatches, invalid variance
metadata, and non-orthonormal basis vectors.

**Pass condition:** validation fails explicitly for each invalid case.

**Traces to:** REQ-PCA-029, REQ-PCA-032

### VAL-PCA-016

Inspect diagnostics on valid transforms.

**Pass condition:** diagnostics expose consistent dimensions, variance
derivations, orthonormality error, rank estimate, and finite-value flags.

**Traces to:** REQ-PCA-030

### VAL-PCA-017

Inspect the repository workspace and PCA crate verification artifacts.

**Pass condition:** the repository contains the PCA crate, the spec package, and
executable automated tests realizing this validation surface.

**Traces to:** REQ-PCA-026, REQ-PCA-027, REQ-PCA-028

