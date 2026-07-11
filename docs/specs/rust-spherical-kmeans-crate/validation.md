<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->

# Rust Spherical K-Means Crate Validation

## Status

Draft validation specification for a Rust crate that realizes vanilla spherical
k-means through the shared LexonGraph streaming clustering contract.

## Validation Scope

These validation entries define the conformance surface for the spherical
k-means crate. They cover both the crate's observable clustering mechanics and
its conformance to the shared streaming trainer/classifier contract.

## Validation Entries

### VAL-SPHKM-001

Inspect the repository artifacts for the crate.

**Pass condition:** the repository includes a crate at
`crates/lexongraph-spherical-kmeans` and this spec package.

**Traces to:** REQ-SPHKM-001

### VAL-SPHKM-002

Inspect the crate's public surface and specification references.

**Pass condition:** the crate exposes concrete shared-contract
implementations, remains subordinate to the research plan and the shared
streaming contract, and does not widen into an unrelated candidate API.

**Traces to:** REQ-SPHKM-002, REQ-SPHKM-003

### VAL-SPHKM-003

Construct a trainer with valid shared configuration and valid spherical-k-means
parameters.

**Pass condition:** construction succeeds deterministically and preserves hard
`K`, dimensionality, and the supplied algorithm parameters.

**Traces to:** REQ-SPHKM-004

### VAL-SPHKM-004

Exercise one completed pass with multiple input batches whose concatenated order
is known.

**Pass condition:** `finish_pass()` realizes exactly one caller-visible
spherical-k-means pass over the concatenated pass dataset order.

**Traces to:** REQ-SPHKM-005, REQ-SPHKM-008

### VAL-SPHKM-005

Exercise malformed streamed input, including wrong dimensionality, non-finite
values, zero-norm embeddings, and an empty completed pass.

**Pass condition:** each case fails explicitly through the shared
malformed-input surface.

**Traces to:** REQ-SPHKM-007, REQ-SPHKM-013

### VAL-SPHKM-006

Complete a later pass whose observed count or ordered embedding content differs
from the first completed pass.

**Pass condition:** continuation fails explicitly before claiming conformant
refinement of the same logical dataset.

**Traces to:** REQ-SPHKM-008, REQ-SPHKM-013

### VAL-SPHKM-007

Inspect the execution path over a representative conformant fixture.

**Pass condition:** the pass realization normalizes embeddings, applies the
documented deterministic initialization rule, and performs deterministic
assignment and centroid-update steps in normalized embedding space.

**Traces to:** REQ-SPHKM-005, REQ-SPHKM-006

### VAL-SPHKM-008

Exercise a conformant fixture across repeated identical runs.

**Pass condition:** pass reports expose deterministic `observed_count`,
explicit partition-ready readiness, populated `realized_cluster_count`,
populated stable `cluster_ids`, deterministic `quality_metric`,
`balance_metric`, and fixed metric directions. When no explicit balance
constraints are configured, `balance_metric` is zero.

**Traces to:** REQ-SPHKM-009, REQ-SPHKM-012

### VAL-SPHKM-009

Complete training and exercise classifier assignment on valid and malformed
embeddings.

**Pass condition:** the classifier normalizes each valid query embedding,
assigns it deterministically to exactly one cluster ID in `[0, K)`, rejects
malformed embeddings through the shared malformed-input category, and does not
require replay of the original training dataset.

**Traces to:** REQ-SPHKM-006, REQ-SPHKM-011, REQ-SPHKM-013

### VAL-SPHKM-010

Exercise invalid configuration, unsupported balance constraints, and illegal
lifecycle transitions.

**Pass condition:** failures are surfaced deterministically through the shared
streaming error categories.

**Traces to:** REQ-SPHKM-010, REQ-SPHKM-013

### VAL-SPHKM-011

Run the shared streaming clustering conformance helpers against the crate.

**Pass condition:** the crate passes the shared lifecycle, malformed-input,
determinism, cluster-ID continuity, readiness-aware report-shape, and
underfull-first-pass checks using the current streamed-event and callback-style
helper surface rather than removed whole-run helper APIs.

**Traces to:** REQ-SPHKM-014

### VAL-SPHKM-012

Inspect the crate's optional acceleration surface and backend-selection path.

**Pass condition:** the crate exposes optional CPU/WGPU execution only through
the shared acceleration boundary, preserves a correct CPU path, and records
explicit fallback when acceleration is unavailable or declined.

**Traces to:** REQ-SPHKM-015, REQ-SPHKM-020

### VAL-SPHKM-013

Run one conformant spherical-kmeans workload on CPU and on the accelerated path
when supported.

**Pass condition:** the observable cluster IDs, classifier behavior, and pass-
level semantics remain equivalent within the documented floating-point
tolerance.

**Traces to:** REQ-SPHKM-019

### VAL-SPHKM-014

Run one accelerated workload large enough to require bounded-memory dense
point-to-centroid work.

**Pass condition:** the crate exercises chunked or tiled accelerated execution
through the shared acceleration boundary rather than requiring whole logical
matrix materialization in device memory.

**Traces to:** REQ-SPHKM-018

### VAL-SPHKM-015

Run the targeted spherical-kmeans microbenchmark on CPU and on the accelerated
path under repeated identical conditions.

**Pass condition:** the accelerated path demonstrates a statistically repeatable
wall-clock win over CPU, where the proof rule is 5 identical CPU runs and 5
identical WGPU runs with WGPU's median wall-clock time strictly lower than the
CPU median.

**Traces to:** REQ-SPHKM-016

### VAL-SPHKM-016

Run the canonical realistic section-4 qualification workflow with spherical
k-means on CPU and on the accelerated path under repeated identical conditions.

**Pass condition:** the accelerated path demonstrates a statistically repeatable
wall-clock win over CPU on the realistic qualification surface rather than only
on a microbenchmark, using the same 5-run-per-backend median rule.

**Traces to:** REQ-SPHKM-016, REQ-SPHKM-017

### VAL-SPHKM-017

Inspect the artifacts emitted for the accelerated proof runs.

**Pass condition:** the artifacts record the actual backend resolution and
whether fallback occurred, so the reported speedup claims are auditable.

**Traces to:** REQ-SPHKM-020

### VAL-SPHKM-018

Run one medium-size conformant workload twice with CPU execution pinned.

**Pass condition:** repeated runs produce the same pass report and the same
batch classifier assignments, demonstrating that assignment parallelism
preserves the current deterministic CPU observable outputs.

**Traces to:** REQ-SPHKM-009, REQ-SPHKM-011, REQ-SPHKM-021

### VAL-SPHKM-019

Exercise a tie-heavy CPU assignment fixture with explicit previous assignments.

**Pass condition:** the CPU parallel assignment path preserves the
`previous_assignment` tie preference and lowest-cluster-id fallback exactly.

**Traces to:** REQ-SPHKM-021

### VAL-SPHKM-020

Exercise classifier batch assignment over a representative conformant fixture.

**Pass condition:** batch assignment yields the same cluster IDs as repeated
elementwise `assign()` calls.

**Traces to:** REQ-SPHKM-011, REQ-SPHKM-021

### VAL-SPHKM-021

Exercise one workload after callers persistently pin CPU execution through the
shared acceleration boundary.

**Pass condition:** the crate uses the CPU path, preserves the documented
observable semantics, and exposes auditable backend attribution for the pinned
execution mode.

**Traces to:** REQ-SPHKM-015, REQ-SPHKM-020
