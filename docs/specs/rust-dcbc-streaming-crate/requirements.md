<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming DCBC Crate Requirements

## Status

Draft specification for a Rust crate that realizes the deterministic
capacity-constrained balanced clustering protocol through the shared
LexonGraph streaming multi-pass clustering contract.

## Scope

This document specifies the crate-level requirements for a new Rust crate that:

- implements `docs/protocol/dcbc.md`
- conforms to the shared contract defined by
  `crates/lexongraph-streaming-clustering`
- preserves the behavioral mechanics of deterministic DCBC while exposing a
  streaming, multi-pass trainer/classifier boundary

This document does not redefine protocol math, deterministic tie-breaking,
output semantics, or failure conditions from `docs/protocol/dcbc.md`, and it
does not modify the shared trait contract defined by
`docs/specs/rust-streaming-clustering-crate`.

## Terminology

In this spec package, `streaming DCBC trainer` means a concrete implementation
of `StreamingClusterTrainer` whose completed passes collectively realize the
deterministic DCBC mechanics defined in `docs/protocol/dcbc.md`.

`Pass dataset order` means the ordered sequence of embeddings observed by the
trainer across all batches ingested before one `finish_pass()` call.

`Observed N` means the total number of embeddings ingested in a completed pass.

`Protocol pass` means one complete DCBC traversal of the logical dataset. In
this spec package, a protocol pass is exposed directly as one caller-visible
streaming pass completed by `finish_pass()`.

## Requirements

### REQ-DCBC-STREAM-001

The repository shall define a dedicated Rust crate for streaming DCBC at
`crates/lexongraph-dcbc-streaming`.

### REQ-DCBC-STREAM-002

The new crate shall remain subordinate to:

- `docs/protocol/dcbc.md` for DCBC protocol semantics
- `docs/specs/rust-streaming-clustering-crate` for the shared streaming
  trainer/classifier contract

If those sources appear to conflict, the narrower scope remains authoritative:
the protocol is authoritative for DCBC mechanics, and the streaming trait spec
is authoritative for the shared contract surface.

### REQ-DCBC-STREAM-003

The crate shall expose a trainer implementation conforming to
`StreamingClusterTrainer` and a classifier implementation conforming to
`StreamingClusterClassifier`.

### REQ-DCBC-STREAM-004

The trainer configuration shall accept:

- the hard required cluster count `K`
- embedding dimensionality
- optional caller-provided balance constraints from the shared trait surface
- an optional deterministic seed

### REQ-DCBC-STREAM-005

The trainer shall expose the protocol's repeated full-dataset DCBC passes
directly through the shared streaming interface.

One completed streaming pass shall correspond to exactly one protocol pass and
one DCBC assignment/update iteration over the pass dataset order observed before
`finish_pass()`.

The crate shall not hide those protocol passes behind a separate public
iteration-count parameter or perform hidden extra iterations beyond the number
of completed caller-driven passes.

### REQ-DCBC-STREAM-006

The crate shall preserve protocol-significant order within each pass. The pass
dataset order is semantically significant and shall not be treated as
permutation-equivalent input.

### REQ-DCBC-STREAM-007

After the first completed pass establishes the logical dataset for the run,
each subsequent completed pass shall represent the same logical dataset in the
same pass dataset order.

If a later pass differs in observed count or ordered embedding content from the
first completed pass, the trainer shall fail explicitly because exact DCBC
multi-iteration mechanics can no longer be preserved.

### REQ-DCBC-STREAM-008

The trainer shall defer dataset-size-dependent feasibility checks until the
first completed pass establishes `Observed N`.

If the first completed pass proves that `Observed N < K`, the trainer shall
fail explicitly through the shared unsatisfiable-constraint error category.

### REQ-DCBC-STREAM-009

The crate shall define a deterministic mapping from shared balance constraints
to the DCBC occupancy bounds required by `docs/protocol/dcbc.md`.

If explicit occupancy bounds are absent, the mapping shall still preserve the
shared contract's requirement to produce exactly `K` non-empty clusters once
`Observed N >= K`.

### REQ-DCBC-STREAM-010

For each completed pass, the crate shall realize the DCBC mechanics required by
`docs/protocol/dcbc.md`, including:

- deterministic centroid initialization
- cosine-distance assignment semantics
- deterministic comparison and tie-breaking behavior
- lexicographically minimal optimal assignment selection when multiple optima
  exist
- deterministic centroid recomputation
- zero-norm centroid fallback behavior

### REQ-DCBC-STREAM-011

The observable contract shall preserve stable cluster identifiers across
completed passes and in the final classifier surface.

### REQ-DCBC-STREAM-012

Each completed pass shall return a deterministic pass report containing:

- `observed_count`
- `quality_metric`
- `balance_metric`
- quality and balance metric directions
- stable cluster identifiers

The balance metric shall be zero when no explicit balance constraints are
configured.

### REQ-DCBC-STREAM-013

After caller-directed training completion, the crate shall produce a
deterministic classifier that:

- assigns each valid embedding to exactly one cluster ID in `[0, K)`
- rejects malformed embeddings through the shared malformed-input error category
- does not require the original dataset after classifier production

### REQ-DCBC-STREAM-014

The crate's public API surface shall remain independent of dataset size.

The implementation may use internal spill or externalized temporary storage to
preserve exact protocol mechanics without requiring RAM usage that scales with
the total dataset size, but this revision does not require spill as long as the
same conformant behavior is preserved.

### REQ-DCBC-STREAM-015

Invalid configuration, invalid state transitions, unsatisfiable constraints,
and malformed input shall be surfaced through the shared streaming error
categories with deterministic terminal-error behavior for illegal lifecycle
transitions.

### REQ-DCBC-STREAM-016

If the crate exposes classifier serialization, that serialization shall be
deterministic for identical trained state, but this revision shall not claim a
repository-wide canonical cross-implementation byte encoding.

### REQ-DCBC-STREAM-017

The repository shall include executable verification artifacts covering both:

- this crate's realization of the DCBC protocol mechanics at its conformant
  boundary
- this crate's conformance to the shared streaming clustering contract,
  including the opt-in conformance-helper surface

## Out of Scope

This crate does not define or own:

- changes to `docs/protocol/dcbc.md`
- changes to the existing `lexongraph-dcbc` crate
- changes to the shared streaming clustering trait crate
- approximate clustering methods
- stochastic initialization without deterministic seeding
- hidden early stopping or convergence-based termination
- a repository-wide standard classifier byte encoding

## Relationship to Other Specifications

This document composes two existing normative sources:

- `docs/protocol/dcbc.md`
- `docs/specs/rust-streaming-clustering-crate`

It defines the requirements for a new crate that bridges them without modifying
either existing specification package.
