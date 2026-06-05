<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust PCA Crate Design

## Status

Draft design specification for a Rust crate that implements deterministic,
streaming-first PCA transforms for LexonGraph.

## Design Goals

The crate design is intended to be:

- deterministic at the crate boundary
- explicit about affine semantics
- streaming-first
- audit-friendly
- numerically explicit
- suitable for stable artifact encoding

## Crate Boundary

The crate owns:

- streaming accumulation of PCA sufficient statistics
- deterministic PCA finalization
- immutable PCA and affine transform surfaces
- deterministic serialization and quantization for transform artifacts
- validation, diagnostics, and PCA-oriented error taxonomy

The crate does not own:

- ANN search
- protocol-level block formats
- randomized or approximate PCA
- GPU backends

## Core Types

### DSG-PCA-001 `Crate boundary`

The repository contains a dedicated `lexongraph-pca` crate whose public surface
implements this specification package and does not claim ownership of unrelated
numeric or indexing concerns.

### DSG-PCA-002 `PcaAccumulator`

`PcaAccumulator` stores:

- `input_dim`
- `sample_count`
- `mean: Vec<f64>`
- `scatter: Vec<f64>`

The `scatter` matrix is stored densely in deterministic column-major order.

### DSG-PCA-003 `Streaming update and merge`

`update` uses a deterministic Welford-style sequential update in `f64`.

`merge` uses the Chan parallel-merge formula in `f64`, including the required
between-group rank-1 correction:

`S = S_a + S_b + (delta * delta^T) * (n_a * n_b / (n_a + n_b))`

where `delta = mean_b - mean_a`.

### DSG-PCA-004 `Determinism boundary`

The first conformant execution boundary uses strict deterministic mode.

Bitwise-identical results are claimed only when the input vectors, update
order, partitioning, merge tree, crate version, dependency version, target
architecture assumptions, and serialization version are fixed.

### DSG-PCA-005 `Finalization algorithm`

`finalize` requires at least two samples, derives the sample covariance matrix
from the stored scatter matrix, and performs symmetric eigendecomposition using
`nalgebra::linalg::SymmetricEigen` in `f64`.

### DSG-PCA-006 `Eigenpair ordering`

Eigenpairs are sorted by descending eigenvalue.

If two eigenvalues compare equal under the implementation's direct `f64`
ordering, the lower original eigenvector index in the decomposition output wins
the tie.

### DSG-PCA-007 `Canonical sign rule`

Each retained basis vector is canonicalized by locating the element with the
largest absolute magnitude, breaking ties by lower element index, and forcing
that element to be non-negative.

### DSG-PCA-008 `PcaTransform`

`PcaTransform` stores:

- `input_dim`
- `output_dim`
- `mean: Vec<f32>`
- `basis: Vec<f32>` as a column-major `input_dim x output_dim` matrix
- `explained_variance: Option<Vec<f32>>`
- `schema_version`

### DSG-PCA-009 `PCA transform operations`

`PcaTransform` exposes:

- `apply`
- `apply_batch`
- `apply_in_place`
- `reconstruct`
- `truncate`
- `explained_variance`
- `cumulative_variance`
- `validate`
- `diagnostics`
- `serialize`
- `deserialize`
- `quantize`

All forward and inverse operations use the affine PCA model consistently.

### DSG-PCA-010 `AffineTransform`

The crate exposes a separate `AffineTransform` for affine results that are not
guaranteed to remain in PCA form.

`AffineTransform` stores:

- `input_dim`
- `output_dim`
- `matrix: Vec<f32>` as a column-major `output_dim x input_dim` matrix
- `bias: Vec<f32>`
- `schema_version`

### DSG-PCA-011 `Composition and rebase`

`compose(a, b)` returns an `AffineTransform` equivalent to `b(a(v))`.

`delta_exact(from, to)` is allowed only when `from` is full-rank.

`delta_reconstructing(from, to)` and reconstructing rebase operations pass
through `from.reconstruct(...)` semantics and surface the reconstructing mode
explicitly through a `DeltaTransform` type.

### DSG-PCA-012 `Serialization`

`PcaTransform` and `AffineTransform` use deterministic binary encodings with:

- fixed magic bytes
- fixed version fields
- little-endian integers
- little-endian IEEE-754 `f32`
- fixed field ordering
- explicit optional-field flags

### DSG-PCA-013 `Quantization`

Quantization is symmetric and deterministic.

The conformant quantized surface supports:

- signed 8-bit values in `[-127, 127]`
- signed 16-bit values in `[-32767, 32767]`
- per-column scaling for basis columns
- ties-to-even rounding
- deterministic clipping

Mean and explained-variance vectors are quantized with their own explicit
scales.

### DSG-PCA-014 `Validation`

`validate` checks:

- vector and matrix dimensional consistency
- finite values
- basis orthonormality under documented tolerances
- explained-variance shape, monotonicity, and non-negativity
- schema compatibility for serialized forms

### DSG-PCA-015 `Diagnostics`

`diagnostics()` returns at least:

- input and output dimensionality
- explained and cumulative variance
- orthonormality error
- condition estimate
- truncation flag
- rank estimate
- finite-value flags

### DSG-PCA-016 `Convenience fitting`

`fit` and `fit_truncated` are convenience layers over `PcaAccumulator` and do
not bypass validation, determinism, or explicit failure behavior.

### DSG-PCA-017 `Error taxonomy`

`PcaError` is a typed error taxonomy covering accumulator failures, numeric
failures, serialization failures, validation failures, and quantization
configuration failures.

### DSG-PCA-018 `Repository realization`

The repository contains:

- a `lexongraph-pca` crate in the workspace
- executable tests mapping the validation entries in this package

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-PCA-001 | REQ-PCA-001, REQ-PCA-002, REQ-PCA-026, REQ-PCA-028 |
| DSG-PCA-002..004 | REQ-PCA-003..REQ-PCA-009 |
| DSG-PCA-005..007 | REQ-PCA-016..REQ-PCA-019 |
| DSG-PCA-008..009 | REQ-PCA-010..REQ-PCA-015, REQ-PCA-029, REQ-PCA-030, REQ-PCA-031 |
| DSG-PCA-010..011 | REQ-PCA-020..REQ-PCA-023 |
| DSG-PCA-012..013 | REQ-PCA-024, REQ-PCA-025 |
| DSG-PCA-014..017 | REQ-PCA-029..REQ-PCA-032 |
| DSG-PCA-018 | REQ-PCA-026, REQ-PCA-027, REQ-PCA-028 |

