<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming DCBC Crate Validation

## Status

Draft validation specification for a Rust crate that realizes deterministic
DCBC through the shared LexonGraph streaming clustering contract.

## Validation Scope

These validation entries define the conformance surface for the streaming DCBC
crate. They cover both:

- realization of DCBC protocol mechanics at the crate's observable boundary
- conformance to the shared streaming trainer/classifier contract

## Validation Entries

### VAL-DCBC-STREAM-001

Inspect the repository artifacts for the crate.

**Pass condition:** the repository includes a crate at
`crates/lexongraph-dcbc-streaming` and this spec package.

**Traces to:** REQ-DCBC-STREAM-001

### VAL-DCBC-STREAM-002

Inspect the crate's public surface and its specification references.

**Pass condition:** the crate exposes concrete implementations of
`StreamingClusterTrainer` and `StreamingClusterClassifier` while remaining
subordinate to `docs/protocol/dcbc.md` and the shared streaming clustering
contract.

**Traces to:** REQ-DCBC-STREAM-002, REQ-DCBC-STREAM-003

### VAL-DCBC-STREAM-003

Construct a trainer with valid shared configuration and supported
balance-constraint inputs.

**Pass condition:** construction succeeds deterministically and the resulting
configuration preserves `K`, dimensions, optional constraints, and deterministic
seed behavior.

**Traces to:** REQ-DCBC-STREAM-004, REQ-DCBC-STREAM-009

### VAL-DCBC-STREAM-004

Exercise repeated completed passes with a fixture whose concatenated order is
known.

**Pass condition:** the observable sequence of pass reports preserves the
documented caller-visible mapping between replay/progress stages and completed
DCBC protocol passes. The implementation does not claim partition-ready output
before enough caller-visible passes have occurred.

**Traces to:** REQ-DCBC-STREAM-005, REQ-DCBC-STREAM-010, REQ-DCBC-STREAM-012

### VAL-DCBC-STREAM-005

Complete a second pass whose observed count or ordered embedding content differs
from the first completed pass.

**Pass condition:** continuation fails explicitly before claiming
protocol-conformant multi-pass execution.

**Traces to:** REQ-DCBC-STREAM-007

### VAL-DCBC-STREAM-006

Complete a first pass whose observed count is smaller than `K`.

**Pass condition:** `finish_pass()` fails explicitly through the shared
unsatisfiable-constraint error category.

**Traces to:** REQ-DCBC-STREAM-008, REQ-DCBC-STREAM-015

### VAL-DCBC-STREAM-007

Run a fixture whose shared balance constraints map deterministically to DCBC
occupancy bounds.

**Pass condition:** the derived occupancy bounds are deterministic and the
resulting cluster sizes satisfy those bounds.

**Traces to:** REQ-DCBC-STREAM-009

### VAL-DCBC-STREAM-008

Inspect the first completed protocol pass on a fixture with unique farthest
candidates.

**Pass condition:** initialization chooses the first embedding in pass dataset
order as the first centroid and later centroids follow the protocol's
deterministic farthest-point rule.

**Traces to:** REQ-DCBC-STREAM-010

### VAL-DCBC-STREAM-009

Run assignment on a fixture with multiple optimal solutions.

**Pass condition:** the realized protocol pass preserves the protocol-required
lexicographically minimal optimal assignment.

**Traces to:** REQ-DCBC-STREAM-010

### VAL-DCBC-STREAM-010

Use a numerically sensitive centroid-update fixture and a zero-norm centroid
fixture.

**Pass condition:** centroid recomputation follows ascending point-index
summation order and applies the protocol-defined smallest-index-member fallback
for normalized distance computations while preserving the raw stored centroid.

**Traces to:** REQ-DCBC-STREAM-010

### VAL-DCBC-STREAM-011

Exercise multiple completed partition-ready passes on a fixture whose internal
cluster ordering would otherwise change.

**Pass condition:** pass reports and classifier assignments preserve stable
externally visible cluster IDs across passes.

**Traces to:** REQ-DCBC-STREAM-011

### VAL-DCBC-STREAM-012

Inspect pass reports across at least two passes.

**Pass condition:** each report exposes deterministic `observed_count`,
`quality_metric`, `balance_metric`, fixed metric directions, and readiness
semantics consistent with analysis-only versus partition-ready states;
`balance_metric` is zero when no explicit balance constraints are configured.

**Traces to:** REQ-DCBC-STREAM-012

### VAL-DCBC-STREAM-013

Complete training and exercise classifier assignment on valid and malformed
embeddings.

**Pass condition:** the classifier deterministically maps each valid embedding
to exactly one cluster ID in `[0, K)`, rejects malformed embeddings through the
shared malformed-input category, and does not require replay of the original
training dataset.

**Traces to:** REQ-DCBC-STREAM-013, REQ-DCBC-STREAM-015

### VAL-DCBC-STREAM-014

Inspect the crate's implementation path and exercise a fixture larger than any
single transient batch-sized working set.

**Pass condition:** no conformant path retains, materializes, or spills
implementation-owned full-dataset embeddings, normalized-point tables, distance
matrices, assignment vectors, memberships, or equivalent replayable
full-dataset state.

**Traces to:** REQ-DCBC-STREAM-014, REQ-DCBC-STREAM-019, REQ-DCBC-STREAM-020

### VAL-DCBC-STREAM-015

Exercise invalid configuration, illegal lifecycle transitions, unsatisfiable
constraints, and malformed input.

**Pass condition:** failures are surfaced deterministically through the shared
streaming error categories, and illegal lifecycle transitions place the trainer
in terminal error state.

**Traces to:** REQ-DCBC-STREAM-015

### VAL-DCBC-STREAM-016

If the crate exposes classifier serialization, serialize the same trained state
twice.

**Pass condition:** repeated serialization yields identical bytes while making
no claim that the encoding is canonical across implementations.

**Traces to:** REQ-DCBC-STREAM-016

### VAL-DCBC-STREAM-017

Run the shared streaming clustering conformance helpers against the crate.

**Pass condition:** the crate passes the shared lifecycle, metric,
malformed-input, determinism, and partition-ready cluster-ID continuity checks.

**Traces to:** REQ-DCBC-STREAM-017

### VAL-DCBC-STREAM-018

Run DCBC-focused executable tests for the crate's observable boundary.

**Pass condition:** executable tests exist for protocol-significant ordering,
initialization, assignment determinism, centroid-update semantics, first-pass
feasibility rejection, replay/progress staging if applicable, and pass-to-pass
protocol realization.

**Traces to:** REQ-DCBC-STREAM-017

### VAL-DCBC-STREAM-019

Construct a trainer with supported occupancy-based balance constraints.

**Pass condition:** trainer construction succeeds deterministically and preserves
the supplied occupancy-based balance configuration.

**Traces to:** REQ-DCBC-STREAM-004, REQ-DCBC-STREAM-009

### VAL-DCBC-STREAM-020

Complete a second pass whose observed count differs from the first completed
pass while preserving the shared prefix ordering.

**Pass condition:** continuation fails explicitly before claiming
protocol-conformant multi-pass execution.

**Traces to:** REQ-DCBC-STREAM-007

### VAL-DCBC-STREAM-021

Complete a first pass where `Observed N >= K` but the deterministically derived
occupancy bounds are still infeasible.

**Pass condition:** `finish_pass()` fails explicitly through the shared
unsatisfiable-constraint error category.

**Traces to:** REQ-DCBC-STREAM-008, REQ-DCBC-STREAM-009, REQ-DCBC-STREAM-015

### VAL-DCBC-STREAM-022

Exercise zero-norm embeddings at trainer-ingestion time and at classifier
assignment time.

**Pass condition:** both surfaces reject the zero-norm embedding through the
shared malformed-input category.

**Traces to:** REQ-DCBC-STREAM-013, REQ-DCBC-STREAM-015

### VAL-DCBC-STREAM-023

Exercise `complete_training()` before `PassComplete` and `into_classifier()`
before `TrainingComplete`.

**Pass condition:** both calls fail explicitly through the shared
invalid-transition category, and `complete_training()` places the trainer in the
terminal error state.

**Traces to:** REQ-DCBC-STREAM-015

### VAL-DCBC-STREAM-024

Run one deterministic DCBC fixture through CPU execution and, on a supported
host, through the WGPU-backed dense-kernel path.

**Pass condition:** pass reports, stable cluster IDs, classifier assignments,
and shared error-class outcomes remain observably equivalent; unsupported hosts
report explicit CPU fallback.

**Traces to:** REQ-DCBC-STREAM-018
