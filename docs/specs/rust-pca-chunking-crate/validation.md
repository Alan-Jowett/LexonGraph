<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust PCA Chunking Crate Validation

## Status

Draft validation specification for a Rust crate that realizes PCA projection +
deterministic sort + exact chunking through the shared LexonGraph streaming
clustering contract.

## Validation Scope

These validation entries define the conformance surface for the PCA chunking
crate. They cover both the crate's observable chunking mechanics and its
conformance to the shared streaming trainer/classifier contract.

## Validation Entries

### VAL-PCA-CHUNK-001

Inspect the repository artifacts for the crate.

**Pass condition:** the repository includes a crate at
`crates/lexongraph-pca-chunking` and this spec package.

**Traces to:** REQ-PCA-CHUNK-001

### VAL-PCA-CHUNK-002

Inspect the crate's public surface and specification references.

**Pass condition:** the crate exposes concrete shared-contract
implementations, remains subordinate to the research plan, the shared streaming
contract, and the PCA crate specification, and does not widen into an unrelated
candidate API.

**Traces to:** REQ-PCA-CHUNK-002, REQ-PCA-CHUNK-003

### VAL-PCA-CHUNK-003

Construct a trainer with valid shared configuration and valid PCA chunking
parameters.

**Pass condition:** construction succeeds deterministically and preserves hard
`K`, dimensionality, and the supplied algorithm parameters.

**Traces to:** REQ-PCA-CHUNK-004

### VAL-PCA-CHUNK-004

Exercise one completed pass with multiple input batches whose concatenated order
is known.

**Pass condition:** `finish_pass()` realizes exactly one caller-visible pass
over the concatenated pass dataset order.

**Traces to:** REQ-PCA-CHUNK-005, REQ-PCA-CHUNK-010

### VAL-PCA-CHUNK-005

Complete a later pass whose observed count or ordered embedding content differs
from the first completed pass.

**Pass condition:** continuation fails explicitly before claiming conformant
refinement of the same logical dataset.

**Traces to:** REQ-PCA-CHUNK-010, REQ-PCA-CHUNK-014

### VAL-PCA-CHUNK-006

Inspect the execution path over a representative conformant fixture.

**Pass condition:** the pass realization uses the repository PCA crate and then
applies deterministic scalar projection, stable sort, and contiguous exact
chunking.

**Traces to:** REQ-PCA-CHUNK-005

### VAL-PCA-CHUNK-007

Use a fixture where `N % K == 0`.

**Pass condition:** all final chunks have exact equal occupancy.

**Traces to:** REQ-PCA-CHUNK-006

### VAL-PCA-CHUNK-008

Use a fixture where `N % K != 0`.

**Pass condition:** chunk sizes follow the documented deterministic
remainder-allocation rule while still yielding exactly `K` non-empty chunks.

**Traces to:** REQ-PCA-CHUNK-007

### VAL-PCA-CHUNK-009

Use duplicate-heavy or tied-projection fixtures across repeated runs.

**Pass condition:** projection-key ties are resolved deterministically and
repeated identical executions remain observable-identical.

**Traces to:** REQ-PCA-CHUNK-008, REQ-PCA-CHUNK-009, REQ-PCA-CHUNK-013

### VAL-PCA-CHUNK-009A

Use a fixture where exact chunking would split fully identical classifier sort
keys across a chunk boundary.

**Pass condition:** the crate fails explicitly rather than claiming a classifier
boundary model that cannot replay the trained membership.

**Traces to:** REQ-PCA-CHUNK-008, REQ-PCA-CHUNK-014

### VAL-PCA-CHUNK-010

Inspect pass reports across repeated identical runs.

**Pass condition:** reports expose deterministic `observed_count`,
`quality_metric`, `balance_metric`, fixed metric directions, and stable cluster
IDs. When no explicit balance constraints are configured, `balance_metric` is
zero.

**Traces to:** REQ-PCA-CHUNK-011, REQ-PCA-CHUNK-013

### VAL-PCA-CHUNK-011

Complete training and exercise classifier assignment on valid and malformed
embeddings.

**Pass condition:** the classifier deterministically maps each valid embedding
to exactly one cluster ID in `[0, K)`, reuses the learned chunk-boundary model,
rejects malformed embeddings through the shared malformed-input category, and
does not require replay of the original training dataset.

**Traces to:** REQ-PCA-CHUNK-012, REQ-PCA-CHUNK-014

### VAL-PCA-CHUNK-012

Exercise invalid configuration, unsupported balance constraints, and illegal
lifecycle transitions.

**Pass condition:** failures are surfaced deterministically through the shared
streaming error categories.

**Traces to:** REQ-PCA-CHUNK-014, REQ-PCA-CHUNK-015

### VAL-PCA-CHUNK-013

Run the shared streaming clustering conformance helpers against the crate.

**Pass condition:** the crate passes the shared lifecycle, malformed-input,
determinism, and cluster-ID continuity checks.

**Traces to:** REQ-PCA-CHUNK-016
