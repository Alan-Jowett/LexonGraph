<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Clustering Evaluator Crate Validation

## Status

Draft validation specification for a Rust crate that evaluates candidate
streaming clustering implementations for LexonGraph at the leaf-partition
boundary.

## Validation Scope

These validation entries define the conformance surface for the new streaming
clustering evaluator crate. They cover benchmark-profile definition, candidate
execution, leaf membership scoring, comparative reporting, determinism checking,
explicit deferred research-goal handling, and corpus sourcing through both
inline fixtures and block-store-backed references.

## Validation Entries

### VAL-STREAM-EVAL-001

Inspect the repository artifacts for the new crate.

**Pass condition:** the repository includes a crate at
`crates/lexongraph-streaming-clustering-evaluator` and this spec package.

**Traces to:** REQ-STREAM-EVAL-001

### VAL-STREAM-EVAL-002

Inspect the crate's public surface and specification references.

**Pass condition:** the crate remains subordinate to the clustering research
documents and the shared streaming clustering contract while defining an
evaluator-owned benchmark boundary rather than a broader candidate API.

**Traces to:** REQ-STREAM-EVAL-002, REQ-STREAM-EVAL-004

### VAL-STREAM-EVAL-003

Run the evaluator with a benchmark profile and at least two registered
candidates.

**Pass condition:** one executable campaign evaluates the candidates through a
shared leaf-stage benchmark profile and emits comparative outputs without
requiring an algorithm-specific candidate API.

**Traces to:** REQ-STREAM-EVAL-003, REQ-STREAM-EVAL-005, REQ-STREAM-EVAL-007

### VAL-STREAM-EVAL-004

Inspect the candidate registration surface.

**Pass condition:** each candidate enters through an adapter or factory that
constructs a trainer conforming to the shared streaming clustering contract.

**Traces to:** REQ-STREAM-EVAL-004

### VAL-STREAM-EVAL-005

Inspect one benchmark profile definition.

**Pass condition:** the profile fixes corpus identities, streaming pass inputs,
classifier-side probe workloads, the declared source mode for each workload, the
leaf model, metric declarations, gate declarations, comparative ranking
weights, deferred research-goal records, and reproducibility metadata for one
campaign.

**Traces to:** REQ-STREAM-EVAL-006

### VAL-STREAM-EVAL-006

Run the same candidate twice under the same benchmark profile.

**Pass condition:** the evaluator compares pass-report sequences and
classifier-side probe outputs across repeated executions and reports whether the
observable results are deterministic.

**Traces to:** REQ-STREAM-EVAL-008, REQ-STREAM-EVAL-011

### VAL-STREAM-EVAL-007

Run a conforming deterministic candidate under a benchmark profile whose
reproducibility metadata is fixed.

**Pass condition:** the resulting provenance manifest deterministically records
benchmark profile identity, corpus identities, candidate identity, clustering
configuration, seed policy, software identity, floating-point profile metadata,
and hardware-profile metadata, plus any declared source-reference identities
needed to reproduce block-store-backed corpus inputs.

**Traces to:** REQ-STREAM-EVAL-008

### VAL-STREAM-EVAL-008

Execute a candidate through at least one multi-batch pass, final classifier
production, full-corpus assignment replay, and one classifier probe workload.

**Pass condition:** the evaluator drives the candidate through trainer
construction, pass ingestion, `finish_pass()`, training completion, classifier
production, evaluator-owned leaf membership materialization, and
classifier-side probing according to the benchmark profile for both inline
fixture workloads and block-store-backed referenced workloads, including
zip-archive-backed sources resolved through the repository overlay and zip
block-store implementations.

**Traces to:** REQ-STREAM-EVAL-009, REQ-STREAM-EVAL-010, REQ-STREAM-EVAL-022, REQ-STREAM-EVAL-024

### VAL-STREAM-EVAL-009

Run the evaluator on a benchmark profile with declared strict alignment and leaf
size `L`.

**Pass condition:** the evaluator verifies exact final cluster occupancy,
complete coverage, one-cluster-per-entity assignment, and absence of empty
declared clusters from the leaf membership artifact.

**Traces to:** REQ-STREAM-EVAL-017

### VAL-STREAM-EVAL-010

Run the evaluator on a benchmark profile with deterministic synthetic padding.

**Pass condition:** the evaluator distinguishes real from synthetic entities in
the leaf membership artifact, enforces exact final occupancy against the padded
evaluated set, and excludes synthetic entities from externally reported
locality and compression metrics.

**Traces to:** REQ-STREAM-EVAL-017, REQ-STREAM-EVAL-018

### VAL-STREAM-EVAL-011

Run the evaluator on a benchmark profile with exact nearest-neighbor ground
truth over real entities.

**Pass condition:** the evaluator computes same-leaf neighborhood coherence from
the leaf membership artifact without claiming same-or-sibling locality proof.

**Traces to:** REQ-STREAM-EVAL-019

### VAL-STREAM-EVAL-012

Run the evaluator on a benchmark profile with a declared local compression
method and global baseline.

**Pass condition:** the evaluator computes local per-cluster compression quality
versus the declared global real-dataset baseline from the leaf membership
artifact.

**Traces to:** REQ-STREAM-EVAL-020

### VAL-STREAM-EVAL-013

Inspect one completed campaign report.

**Pass condition:** the result model distinguishes shared-contract prerequisite
checks, must-pass gates, and comparative metrics, and does not rank a gate
failing candidate as a surviving success.

**Traces to:** REQ-STREAM-EVAL-012

### VAL-STREAM-EVAL-014

Inspect the metric and gate declarations in a benchmark profile and the
corresponding campaign report.

**Pass condition:** each declared metric, gate, or deferred research-goal
record traces to one or more research goals and is tagged as direct, proxy, or
deferred.

**Traces to:** REQ-STREAM-EVAL-013

### VAL-STREAM-EVAL-015

Inspect the emitted campaign artifacts for one benchmark execution.

**Pass condition:** the evaluator emits a machine-readable run report per
candidate, a machine-readable comparative campaign report, and a human-readable
scorecard summarizing pass/fail status, metric values, and survivor ranking.

**Traces to:** REQ-STREAM-EVAL-014

### VAL-STREAM-EVAL-016

Run the evaluator with an invalid benchmark profile and with a candidate that
returns a shared-contract failure.

**Pass condition:** failures are surfaced deterministically and distinguish
invalid evaluator configuration, invalid or unresolved corpus-source
references, block-store-backed corpus-loading failures, zip-archive open or
read failures, and candidate-reported shared-contract failure.

**Traces to:** REQ-STREAM-EVAL-015, REQ-STREAM-EVAL-022

### VAL-STREAM-EVAL-017

Run the evaluator with a candidate that fails one declared benchmark gate and
with a benchmark profile that includes at least one deferred research goal.

**Pass condition:** the resulting report distinguishes evaluator-owned gate
failure from incomplete or unsupported measurement due to a deferred research
requirement.

**Traces to:** REQ-STREAM-EVAL-015, REQ-STREAM-EVAL-021

### VAL-STREAM-EVAL-018

Inspect the benchmark profile and scorecard against the clustering research
documents.

**Pass condition:** research goals requiring full hierarchy, sibling structure,
routing, or durable index artifacts are recorded as deferred rather than
claimed as fully proven by the streaming clustering evaluator alone, and the
future end-to-end evaluator is called out as a separate later line.

**Traces to:** REQ-STREAM-EVAL-013, REQ-STREAM-EVAL-021

### VAL-STREAM-EVAL-019

Inspect repository verification artifacts for the new crate.

**Pass condition:** executable tests exist for benchmark-profile validation,
candidate execution, inline-fixture and block-store-backed corpus-source
handling, leaf membership materialization, occupancy/locality/compression
scoring, repeated-run determinism checks, comparative scorecard generation,
failure classification, deferred-goal reporting, and archive-backed overlay
resolution.

**Traces to:** REQ-STREAM-EVAL-016, REQ-STREAM-EVAL-022, REQ-STREAM-EVAL-023, REQ-STREAM-EVAL-024

### VAL-STREAM-EVAL-020

Run one campaign whose training passes, evaluation replay entities, and
classifier-side probes all resolve through block-store-backed corpus
references.

**Pass condition:** the evaluator consumes all three workload families through
the same declared scalable corpus-source model without requiring a monolithic
profile-embedded JSON corpus, including when those references are
zip-archive-backed and resolved through a temporary-filesystem-over-zip
overlay.

**Traces to:** REQ-STREAM-EVAL-024, REQ-STREAM-EVAL-025, REQ-STREAM-EVAL-026

### VAL-STREAM-EVAL-021

Run the evaluator with one small inline benchmark fixture and one functionally
equivalent block-store-backed benchmark fixture.

**Pass condition:** both source modes remain supported, and the resulting leaf
membership semantics and report semantics are equivalent apart from provenance
details specific to the referenced external corpus source, whether the
block-store-backed fixture is filesystem-root-backed or zip-archive-backed.

**Traces to:** REQ-STREAM-EVAL-014, REQ-STREAM-EVAL-022, REQ-STREAM-EVAL-023

### VAL-STREAM-EVAL-022

Inspect one benchmark profile using archive-backed scalable corpora.

**Pass condition:** the profile can declare a zip archive path plus root block
ID for a training pass, probe workload, or evaluation corpus without requiring
the user to declare a writable overlay directory.

**Traces to:** REQ-STREAM-EVAL-027, REQ-STREAM-EVAL-029

### VAL-STREAM-EVAL-023

Run one campaign whose training passes, evaluation replay entities, and probe
workloads are supplied through zip-archive-backed corpus references.

**Pass condition:** the evaluator resolves each referenced archive through a
higher-priority temporary writable filesystem layer over a lower-priority
immutable zip layer and successfully consumes the resulting overlay-backed
block-store view.

**Traces to:** REQ-STREAM-EVAL-028, REQ-STREAM-EVAL-029

### VAL-STREAM-EVAL-024

Exercise the reusable filesystem-over-zip overlay helper, if present.

**Pass condition:** new block creation through the helper lands in the mutable
filesystem layer without mutating the underlying zip archive and without
widening the parent `BlockStore` API.

**Traces to:** REQ-STREAM-EVAL-030
