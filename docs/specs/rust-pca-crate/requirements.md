<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust PCA Crate Requirements

## Status

Draft specification for a Rust crate that implements deterministic,
streaming-first PCA transforms for LexonGraph.

## Scope

This document specifies the crate-level requirements for a Rust crate that
implements deterministic PCA accumulation, affine PCA transforms, affine
composition between PCA coordinate systems, deterministic serialization, and
deterministic quantization suitable for audit-friendly transform artifacts.

This document does not define approximate nearest-neighbor search, randomized
PCA, GPU kernels, arbitrary preprocessing pipelines, or non-PCA manifold
learning methods.

## Terminology

In this spec package, `strict deterministic mode` means bitwise reproducibility
is guaranteed only when the input vectors, update order, partitioning, and
merge tree are fixed.

`Reconstructing rebase` means a delta or rebase operation that passes through a
truncated PCA transform's reconstruction map rather than a true inverse.

## Requirements

### REQ-PCA-001

The crate shall define the Rust API boundary for a deterministic, streaming,
auditable PCA transform engine for LexonGraph.

### REQ-PCA-002

The crate shall remain subordinate to this specification package for
crate-level PCA behavior, determinism semantics, artifact encoding, validation,
and failure rules.

### REQ-PCA-003

The crate shall support streaming PCA accumulation without retaining all input
vectors in memory.

### REQ-PCA-004

The crate shall expose an accumulator surface equivalent to:

- `new(dim: usize) -> Self`
- `update(&mut self, v: &[f32]) -> Result<(), PcaError>`
- `merge(&mut self, other: &Self) -> Result<(), PcaError>`
- `finalize(&self) -> Result<PcaTransform, PcaError>`

### REQ-PCA-005

The accumulator shall maintain sufficient statistics to derive at least:

- sample count
- mean vector
- covariance or scatter information required for PCA finalization

### REQ-PCA-006

The crate shall fail explicitly on dimension mismatch across accumulator
updates, accumulator merges, transform application, reconstruction, and
truncation.

### REQ-PCA-007

The crate shall fail explicitly on empty input and on insufficient samples for
covariance-based PCA finalization.

### REQ-PCA-008

The crate shall publish a determinism contract.

At minimum, the first conformant execution boundary shall use strict
deterministic mode and shall make explicit that reproducibility depends on the
same input vectors, update order, partitioning, merge tree, decomposition
algorithm, eigenpair ordering rules, canonical sign rule, and serialization
version.

### REQ-PCA-009

The crate shall define the accumulator's internal numeric model and precision,
including how partial accumulators are merged.

### REQ-PCA-010

The crate shall expose an immutable `PcaTransform` that models PCA as the
affine transform `U^T (v - m)` and stores:

- `input_dim`
- `output_dim`
- mean vector
- basis matrix
- optional explained variance
- schema or format version metadata

### REQ-PCA-011

The crate shall expose a forward transform operation that computes `U^T (v - m)`
for one input vector and for batches of input vectors.

### REQ-PCA-012

The crate shall expose a reconstruction operation that computes `Uy + m`.

For truncated transforms, reconstruction semantics shall be explicit and may be
lossy.

### REQ-PCA-013

The crate shall expose transform truncation.

Truncation shall preserve the same input mean, the leading basis vectors, and
the leading explained variance entries when available.

### REQ-PCA-014

Batch application and in-place convenience operations shall preserve the same
behavioral semantics as repeated single-vector application, apart from the
documented floating-point tolerance of the implementation.

### REQ-PCA-015

The crate shall expose explained variance metadata and cumulative variance
derivations when variance metadata is present.

### REQ-PCA-016

PCA finalization shall use an explicitly defined decomposition algorithm over
the derived covariance or scatter structure.

### REQ-PCA-017

Eigenvalues or singular values shall be ordered deterministically in descending
order.

If equal values require tie-breaking, the tie-break rule shall be explicit.

### REQ-PCA-018

The crate shall apply a canonical sign rule to each basis vector so sign
ambiguity is resolved deterministically.

### REQ-PCA-019

The crate shall define explicit handling for rank-deficient or numerically
degenerate covariance structures and shall not silently emit undocumented
numeric artifacts.

### REQ-PCA-020

The crate shall support affine composition of PCA-derived transforms, including
correct propagation of mean-offset terms.

### REQ-PCA-021

If exact closure under `PcaTransform` is not guaranteed for composition or
delta operations, the crate shall expose a separate affine transform surface
rather than misrepresenting the result as a PCA transform.

### REQ-PCA-022

The crate shall support delta and rebase operations between PCA spaces.

### REQ-PCA-023

The crate shall distinguish exact rebase semantics from reconstructing rebase
semantics when the source transform is truncated.

### REQ-PCA-024

The crate shall serialize and deserialize PCA transforms deterministically using
a versioned, endian-stable, hash-friendly binary encoding with explicit field
ordering and explicit handling of optional fields.

### REQ-PCA-025

The crate shall support deterministic quantization and dequantization of PCA
transforms with explicit bit width, scaling, rounding, and clipping semantics.

### REQ-PCA-026

The repository shall include a Rust crate that realizes the requirements and
design in this specification package.

### REQ-PCA-027

The repository shall include automated verification artifacts that realize the
validation surface defined in `docs/specs/rust-pca-crate/validation.md`.

### REQ-PCA-028

The PCA crate shall be wired into the repository Cargo workspace.

### REQ-PCA-029

The crate shall expose validation of structural and numeric consistency,
including dimension checks, finite-value checks, orthonormality checks, and
explained-variance consistency checks under documented tolerances.

### REQ-PCA-030

The crate shall expose diagnostics suitable for audit logs, testing, and
numerical inspection.

### REQ-PCA-031

The crate may expose convenience fitting and batch-application APIs, but those
APIs shall preserve the deterministic and error semantics of the core model.

### REQ-PCA-032

The crate shall expose an explicit typed error model covering, at minimum:

- dimension mismatch
- empty input
- insufficient samples
- invalid truncation dimension
- non-finite input
- singular or degenerate covariance
- decomposition failure
- invalid serialized format
- schema version mismatch
- validation failure
- quantization configuration failure

## Out of Scope

This crate does not define or own:

- randomized PCA
- approximate nearest-neighbor indexing
- GPU kernels
- arbitrary preprocessing pipelines beyond PCA centering logic
- t-SNE, UMAP, or non-PCA embeddings

