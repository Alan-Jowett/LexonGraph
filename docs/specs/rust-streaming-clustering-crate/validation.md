<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Clustering Trait Crate Validation

## Status

Draft validation specification for a Rust crate that defines the shared
LexonGraph streaming multi-pass clustering contract.

## Validation Scope

These validation entries define the expected conformance surface for the shared
streaming clustering trait crate.

## Validation Entries

### VAL-STREAM-TRAIT-001

Inspect the crate public surface.

**Pass condition:** the default public API exposes trainer/classifier contract
types, shared configuration/metric/error types, and no concrete clustering
implementation.

**Traces to:** REQ-STREAM-TRAIT-001, REQ-STREAM-TRAIT-002, REQ-STREAM-TRAIT-017

### VAL-STREAM-TRAIT-002

Exercise a conforming fixture that ingests batches across multiple passes and
reports pass completion.

**Pass condition:** the caller controls `finish_pass` and training
stop/continue decisions.

**Traces to:** REQ-STREAM-TRAIT-005, REQ-STREAM-TRAIT-012

### VAL-STREAM-TRAIT-003

Exercise a fixture where first-pass completion establishes `N < K`.

**Pass condition:** the contract surfaces explicit failure rather than silently
reducing `K` or producing empty clusters.

**Traces to:** REQ-STREAM-TRAIT-003, REQ-STREAM-TRAIT-010

### VAL-STREAM-TRAIT-004

Inspect or execute pass reporting.

**Pass condition:** each pass report exposes deterministic `quality_metric`,
`balance_metric`, and direction-of-improvement metadata.

**Traces to:** REQ-STREAM-TRAIT-006

### VAL-STREAM-TRAIT-005

Exercise classifier production after training completion.

**Pass condition:** the classifier deterministically assigns each valid
embedding to exactly one cluster ID in `[0, K)`.

**Traces to:** REQ-STREAM-TRAIT-007, REQ-STREAM-TRAIT-009

### VAL-STREAM-TRAIT-006

Exercise malformed embeddings and illegal lifecycle transitions.

**Pass condition:** failures are explicit and route through the shared
category-level error surface.

**Traces to:** REQ-STREAM-TRAIT-010, REQ-STREAM-TRAIT-012

### VAL-STREAM-TRAIT-007

Inspect the public API for dataset-size coupling.

**Pass condition:** the contract does not require full-dataset materialization
or full assignment retention as part of normal trait use by callers or as an
observable trait obligation. Concrete implementation-internal pass buffering is
not by itself a contract violation.

**Traces to:** REQ-STREAM-TRAIT-011

### VAL-STREAM-TRAIT-008

Exercise seeded and default deterministic fixtures twice with identical inputs
and pass boundaries.

**Pass condition:** observable metrics and assignments are identical across
runs.

**Traces to:** REQ-STREAM-TRAIT-013

### VAL-STREAM-TRAIT-009

Inspect the crate feature surface.

**Pass condition:** conformance helpers are exposed only through a non-default
`conformance` feature.

**Traces to:** REQ-STREAM-TRAIT-014, REQ-STREAM-TRAIT-015

### VAL-STREAM-TRAIT-010

Inspect repository verification artifacts.

**Pass condition:** executable tests exist for the validation surface and
conformance-helper rejection behavior.

**Traces to:** REQ-STREAM-TRAIT-016

### VAL-STREAM-TRAIT-011

Run the conformance helpers against a fixture that changes externally visible
cluster IDs across passes without preserving continuity.

**Pass condition:** the suite rejects the fixture as an expectation failure.

**Traces to:** REQ-STREAM-TRAIT-008, REQ-STREAM-TRAIT-014

### VAL-STREAM-TRAIT-012

Run the conformance helpers against the dedicated malformed-input-accepting
fixture whose classifier accepts wrong dimensionality and `NaN` input as valid.

**Pass condition:** the suite rejects the fixture.

**Traces to:** REQ-STREAM-TRAIT-007, REQ-STREAM-TRAIT-010, REQ-STREAM-TRAIT-014, REQ-STREAM-TRAIT-022

### VAL-STREAM-TRAIT-013

Inspect the default public surface.

**Pass condition:** the shared contract remains algorithm-neutral and does not
require a specific clustering method.

**Traces to:** REQ-STREAM-TRAIT-017

### VAL-STREAM-TRAIT-014

Inspect the classifier contract shape.

**Pass condition:** this revision does not claim a standardized
cross-implementation byte encoding for classifiers.

**Traces to:** REQ-STREAM-TRAIT-018

### VAL-STREAM-TRAIT-015

Exercise invalid base configuration inputs.

**Pass condition:** zero cluster count and zero embedding dimensionality are
rejected explicitly through the shared `InvalidConfiguration` error category.

**Traces to:** REQ-STREAM-TRAIT-010, REQ-STREAM-TRAIT-019

### VAL-STREAM-TRAIT-016

Exercise invalid caller-provided balance constraints.

**Pass condition:** zero occupancies, contradictory occupancy bounds,
non-finite or non-positive maximum cluster-size ratios, and non-finite or
negative soft-balance penalties are rejected explicitly through the shared
`InvalidConfiguration` error category.

**Traces to:** REQ-STREAM-TRAIT-004, REQ-STREAM-TRAIT-010, REQ-STREAM-TRAIT-020

### VAL-STREAM-TRAIT-017

Exercise the public conformance-helper error surface through
`run_streaming_clustering_suite()`.

**Pass condition:** tests drive the suite through:

- at least one implementation-reported shared contract failure returned from
  `run_streaming_clustering_suite()`
- at least one suite-authored expectation failure returned from
  `run_streaming_clustering_suite()`

For the implementation-reported failure, the returned `ConformanceError` shall
preserve the underlying source error and shared display text. For the
expectation failure, the returned `ConformanceError` shall use a suite-authored
message and expose no source error.

**Traces to:** REQ-STREAM-TRAIT-021

### VAL-STREAM-TRAIT-018

Inspect one accelerator-capable concrete implementation and its use of the
shared contract.

**Pass condition:** optional accelerator use does not widen the shared
trainer/classifier API, and unsupported-host behavior remains an
implementation-internal fallback rather than a contract change.

**Traces to:** REQ-STREAM-TRAIT-023
