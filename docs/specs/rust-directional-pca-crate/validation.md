<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Directional PCA Crate Validation

## Status

Draft validation specification for a Rust crate that realizes streaming
directional-PCA clustering through the shared LexonGraph streaming clustering
contract.

## Validation Scope

These validation entries define the conformance surface for the streaming
directional-PCA crate. They cover both:

- realization of the directional-PCA mechanics preserved from
  `docs/arch/Directional PCA tree.md`
- conformance to the shared streaming trainer/classifier contract

## Validation Entries

### VAL-DPCA-STREAM-001

Inspect the repository artifacts for the crate.

**Pass condition:** the repository includes a crate at
`crates/lexongraph-directional-pca` and this spec package.

**Traces to:** REQ-DPCA-STREAM-001

### VAL-DPCA-STREAM-002

Inspect the crate's public surface and specification references.

**Pass condition:** the crate exposes concrete implementations of
`StreamingClusterTrainer` and `StreamingClusterClassifier`, remains subordinate
to the directional-PCA architecture note, the shared streaming clustering
contract, and the PCA crate specification, and does not expose the retired
public block-store boundary.

**Traces to:** REQ-DPCA-STREAM-002, REQ-DPCA-STREAM-003, REQ-DPCA-STREAM-004

### VAL-DPCA-STREAM-003

Construct a trainer with valid shared configuration and valid directional
parameters.

**Pass condition:** construction succeeds deterministically and preserves hard
`K`, dimensionality, deterministic seed behavior, and the supplied directional
parameters.

**Traces to:** REQ-DPCA-STREAM-005

### VAL-DPCA-STREAM-004

Construct a trainer with caller-provided shared balance constraints.

**Pass condition:** construction fails explicitly through the shared
invalid-configuration category rather than silently ignoring unsupported balance
configuration.

**Traces to:** REQ-DPCA-STREAM-006, REQ-DPCA-STREAM-019

### VAL-DPCA-STREAM-005

Exercise one pass with multiple batches whose concatenated order is known.

**Pass condition:** `finish_pass()` realizes exactly one caller-visible
directional-PCA pass over the concatenated pass dataset order and does not
perform hidden extra passes.

**Traces to:** REQ-DPCA-STREAM-008, REQ-DPCA-STREAM-009

### VAL-DPCA-STREAM-006

Complete a second pass whose observed count or ordered embedding content differs
from the first completed pass.

**Pass condition:** continuation fails explicitly before claiming conformant
refinement of the same training run.

**Traces to:** REQ-DPCA-STREAM-010

### VAL-DPCA-STREAM-007

Exercise malformed streamed input, including wrong dimensionality, non-finite
values, and an empty completed pass.

**Pass condition:** each case fails explicitly through the shared
malformed-input surface.

**Traces to:** REQ-DPCA-STREAM-007, REQ-DPCA-STREAM-019

### VAL-DPCA-STREAM-008

Inspect the execution path over a representative conformant fixture.

**Pass condition:** the directional-PCA pass is realized by the repository PCA
crate surface rather than an undocumented independent PCA implementation.

**Traces to:** REQ-DPCA-STREAM-011

### VAL-DPCA-STREAM-009

Use a fixture with known retained PCA coordinates, centroid direction, and
explained variance.

**Pass condition:** the realized per-axis scores reflect both directional
coefficients and explained variance according to the configured `gamma`.

**Traces to:** REQ-DPCA-STREAM-012

### VAL-DPCA-STREAM-010

Use a fixture whose damped axis scores produce non-trivial allocation relative
to a hard cluster target `K`.

**Pass condition:** the per-axis resolution counts follow the documented
temperature-controlled allocation rule and deterministic correction behavior.

**Traces to:** REQ-DPCA-STREAM-013

### VAL-DPCA-STREAM-011

Use a fixture whose retained PCA coordinates are unevenly distributed.

**Pass condition:** the conformant default assignment path uses quantile binning
rather than equal-width binning.

**Traces to:** REQ-DPCA-STREAM-014

### VAL-DPCA-STREAM-012

Exercise three exact-K boundary fixtures:

- first-pass `Observed N < K`
- infeasible directional parameters
- a realized directional-PCA partition that cannot produce exactly `K` stable,
  non-empty clusters without changing the documented semantics

**Pass condition:** each case fails explicitly rather than silently forcing an
exact-K outcome.

**Traces to:** REQ-DPCA-STREAM-015, REQ-DPCA-STREAM-019

### VAL-DPCA-STREAM-013

Inspect pass reports across at least two passes.

**Pass condition:** each report exposes deterministic `observed_count`,
`quality_metric`, `balance_metric`, fixed metric directions, and stable cluster
IDs. When no explicit balance constraints are configured, `balance_metric` is
zero.

**Traces to:** REQ-DPCA-STREAM-016, REQ-DPCA-STREAM-017

### VAL-DPCA-STREAM-014

Exercise multiple completed passes on a fixture whose internal group ordering
would otherwise change.

**Pass condition:** pass reports and classifier assignments preserve stable
externally visible cluster IDs across passes.

**Traces to:** REQ-DPCA-STREAM-017

### VAL-DPCA-STREAM-015

Complete training and exercise classifier assignment on valid and malformed
embeddings.

**Pass condition:** the classifier deterministically maps each valid embedding
to exactly one cluster ID in `[0, K)`, rejects malformed embeddings through the
shared malformed-input category, and does not require replay of the original
training dataset.

**Traces to:** REQ-DPCA-STREAM-018, REQ-DPCA-STREAM-019

### VAL-DPCA-STREAM-016

Exercise invalid configuration and illegal lifecycle transitions.

**Pass condition:** failures are surfaced deterministically through the shared
streaming error categories, and illegal lifecycle transitions place the trainer
in terminal error state where required by the shared contract.

**Traces to:** REQ-DPCA-STREAM-019

### VAL-DPCA-STREAM-017

Inspect the crate's public surface and executable verification artifacts after
the streaming rework.

**Pass condition:** public helpers, types, and tests that existed only to
support the retired public block-store boundary have been removed, and the
retained artifacts are limited to the scaled-down native streaming crate
boundary.

**Traces to:** REQ-DPCA-STREAM-020

### VAL-DPCA-STREAM-018

Run the shared streaming clustering conformance helpers against the crate.

**Pass condition:** the crate passes the shared lifecycle, metric,
malformed-input, determinism, and cluster-ID continuity checks.

**Traces to:** REQ-DPCA-STREAM-021

### VAL-DPCA-STREAM-019

Run directional-PCA-focused executable tests for the crate's observable
boundary.

**Pass condition:** executable tests exist for pass ordering, cross-pass
continuity, PCA reuse, scoring, allocation, quantile binning, exact-K failure,
stable cluster IDs, classifier assignment, and dead-code cleanup of the retired
block-store boundary.

**Traces to:** REQ-DPCA-STREAM-021

### VAL-DPCA-STREAM-020

Use a fixture where all embeddings in the completed pass are identical and
`Observed N >= K`.

**Pass condition:** the crate does not fail exact-K solely because geometric
partitioning collapsed the pass; instead it deterministically realizes exactly
`K` non-empty clusters through duplicate refinement.

**Traces to:** REQ-DPCA-STREAM-015, REQ-DPCA-STREAM-022, REQ-DPCA-STREAM-023

### VAL-DPCA-STREAM-021

Use a fixture where one populated cell contains duplicate members and the pass
under-realizes `K` only because of that collapsed duplicate cell.

**Pass condition:** the crate refines only the collapsed duplicate members,
preserves the rest of the primary partition, and realizes exactly `K`
non-empty clusters.

**Traces to:** REQ-DPCA-STREAM-023, REQ-DPCA-STREAM-024

### VAL-DPCA-STREAM-022

Repeat duplicate-collapse fixtures across at least two passes with identical
ordered input.

**Pass condition:** pass reports and classifier assignments preserve stable
cluster IDs and deterministic assignments across passes that exercise duplicate
refinement.

**Traces to:** REQ-DPCA-STREAM-016, REQ-DPCA-STREAM-017, REQ-DPCA-STREAM-018

### VAL-DPCA-STREAM-023

Use a fixture where exact-K remains infeasible for a reason other than
duplicate-collapse.

**Pass condition:** the crate still fails explicitly and does not invoke the
duplicate-refinement fallback.

**Traces to:** REQ-DPCA-STREAM-024
