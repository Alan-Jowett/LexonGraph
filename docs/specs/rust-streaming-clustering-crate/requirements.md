<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Clustering Trait Crate Requirements

## Status

Draft specification for a Rust trait crate that defines the shared LexonGraph
streaming multi-pass clustering contract.

## Scope

This document specifies the crate-level requirements for a Rust crate that
realizes the contract boundary derived from
`docs/arch/streaming-clustering.md`.

This document defines the shared trainer/classifier boundary, lifecycle,
configuration, metrics, and conformance-helper surface. It does not require a
concrete clustering algorithm.

## Terminology

In this spec package, `pass` means one full caller-driven traversal of the
dataset consisting of one or more streamed batches followed by
`finish_pass()`.

`Stable cluster identifier` means the externally visible cluster ID observed in
pass reports and in the final classifier surface.

## Requirements

### REQ-STREAM-TRAIT-001

The repository shall define a dedicated Rust crate at
`crates/lexongraph-streaming-clustering` that owns the shared streaming
multi-pass clustering contract for LexonGraph.

### REQ-STREAM-TRAIT-002

The crate shall define trainer-side and classifier-side contract boundaries and
shall not require a concrete clustering algorithm.

### REQ-STREAM-TRAIT-003

The trainer contract shall accept a requested cluster count `K`.

By default, the shared contract remains exact-`K`: the trainer shall surface
explicit failure once the first completed pass establishes `N < K`.

Concrete implementations may additionally expose an implementation-owned opt-in
underfull-success mode that permits successful completion with a realized count
`R` such that `1 <= R <= K` when exact-`K` realization is infeasible under the
implementation's documented mechanics.

### REQ-STREAM-TRAIT-004

The trainer contract shall accept optional caller-provided balance constraints
without mandating a single balancing policy.

### REQ-STREAM-TRAIT-005

The contract shall model pass boundaries explicitly and shall leave pass count
and stop/continue decisions to the caller.

### REQ-STREAM-TRAIT-006

The contract shall expose deterministic per-pass fitness reporting with:

- requested cluster count
- realized cluster count
- separate `quality_metric` and `balance_metric` values
- direction-of-improvement metadata

### REQ-STREAM-TRAIT-007

The classifier contract shall deterministically map each valid embedding to
exactly one cluster ID in `[0, R)`, where `R` is the classifier's realized
cluster count and `1 <= R <= K`.

For exact-`K` runs, `R = K`.

### REQ-STREAM-TRAIT-008

The observable contract shall preserve stable cluster identifiers across passes
and in the final classifier.

### REQ-STREAM-TRAIT-009

The trainer contract shall support producing a classifier from final training
state without requiring the original dataset thereafter.

### REQ-STREAM-TRAIT-010

The crate shall define deterministic explicit error categories covering invalid
configuration, invalid state transition, unsatisfiable constraint, and
malformed input.

When a conformant implementation has entered an explicit underfull-success mode,
failure is still required for malformed input, invalid configuration, invalid
state transitions, and any case where zero clusters would be realized.

### REQ-STREAM-TRAIT-011

The public contract, including feature-gated conformance-helper surfaces, shall
remain dataset-size independent during indexing by avoiding API shapes or
observable shared-contract obligations whose implementation-owned memory or
scratch/storage footprint scales with total dataset size `N`.

Caller-owned per-batch inputs may scale with caller-selected batch size.
Shared-contract state may scale with requested cluster count, embedding
dimensionality, caller-provided balance configuration, and documented bounded
batch size, but not with `N`.

### REQ-STREAM-TRAIT-012

The crate shall define the trainer lifecycle so illegal state transitions are
rejected deterministically.

### REQ-STREAM-TRAIT-013

The contract shall support deterministic seeded behavior and deterministic
default behavior when no seed is supplied.

### REQ-STREAM-TRAIT-014

The crate shall provide reusable conformance-test helpers for downstream
implementations of the shared streaming clustering contract.

### REQ-STREAM-TRAIT-015

The conformance helpers shall be exposed only through an opt-in, non-default,
test-oriented surface.

### REQ-STREAM-TRAIT-016

The repository shall include executable verification artifacts that realize the
validation plan for the streaming clustering contract crate and its
conformance helpers.

### REQ-STREAM-TRAIT-017

This revision shall not require a specific centroid model, update rule,
distance function, optimization heuristic, or balance-policy realization.

### REQ-STREAM-TRAIT-018

This revision shall not require a crate-owned canonical byte encoding for
classifier serialization. Deterministic classification behavior is required,
while serialization remains implementation-defined unless a future revision
standardizes it.

### REQ-STREAM-TRAIT-019

The configuration surface shall reject invalid base configuration values
through the shared `InvalidConfiguration` error category, including
zero cluster count and zero embedding dimensionality.

### REQ-STREAM-TRAIT-020

When caller-provided balance constraints are present, the configuration surface
shall reject invalid balance-constraint values through the shared
`InvalidConfiguration` error category. In this revision:

- minimum occupancy must be positive when provided
- maximum occupancy must be positive when provided
- minimum occupancy must not exceed maximum occupancy when both are provided
- maximum cluster size ratio must be finite and positive when provided
- soft balance penalty must be finite and non-negative when provided

### REQ-STREAM-TRAIT-021

The feature-gated conformance-helper surface shall expose a public
`ConformanceError` type that distinguishes implementation-reported shared
contract errors from suite expectation failures. Only implementation-reported
failures shall preserve an underlying source error.

### REQ-STREAM-TRAIT-022

The conformance harness contract shall provide a trainer fixture whose
resulting classifier accepts wrong-dimensional or non-finite embeddings as
valid so the suite can verify malformed-input rejection behavior.

### REQ-STREAM-TRAIT-023

Concrete implementations may realize internal compute through optional
heterogeneous-acceleration backends, provided that:

- the shared public trainer/classifier contract remains unchanged
- unsupported hosts or unavailable accelerator paths fall back explicitly rather
  than silently changing observable semantics
- the observable lifecycle, assignment semantics, error categories, and
  deterministic guarantees required by this specification remain authoritative
  at the shared boundary

### REQ-STREAM-TRAIT-024

A conformant implementation shall perform indexing and training with
implementation-owned memory and scratch/storage bounded independently of total
dataset size `N`.

It shall not require dataset replay buffers, per-item assignment logs, spill
files, or equivalent temporary or persistent indexing state whose footprint
scales `O(N)` with the dataset.

Bounded implementation-owned state may scale with requested cluster count,
embedding dimensionality, caller-provided balance configuration, and
caller-selected batch size, but not with `N`.

## Out of Scope

This crate does not define or own:

- a concrete streaming clustering algorithm
- centroid update rules or distance metrics
- recursive tree construction
- block encoding or block storage contracts
- a standardized cross-implementation classifier byte format

## Relationship to Other Specifications

This document is derived from `docs/arch/streaming-clustering.md` for the
shared trait boundary.

If this document appears to conflict with that architecture note about
algorithm-specific details, the narrower trait-crate scope in this
specification package is authoritative for the crate boundary it defines.
