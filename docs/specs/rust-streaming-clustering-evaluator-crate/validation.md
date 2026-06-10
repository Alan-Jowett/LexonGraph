<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Clustering Evaluator Crate Validation

## Status

Draft validation specification for a Rust crate that evaluates candidate
streaming clustering implementations for LexonGraph.

## Validation Scope

These validation entries define the conformance surface for the new streaming
clustering evaluator crate. They cover benchmark-profile definition, candidate
execution, comparative reporting, determinism checking, and explicit deferred
research-goal handling.

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
shared benchmark profile and emits comparative outputs without requiring an
algorithm-specific candidate API.

**Traces to:** REQ-STREAM-EVAL-003, REQ-STREAM-EVAL-005, REQ-STREAM-EVAL-007

### VAL-STREAM-EVAL-004

Inspect the candidate registration surface.

**Pass condition:** each candidate enters through an adapter or factory that
constructs a trainer conforming to the shared streaming clustering contract.

**Traces to:** REQ-STREAM-EVAL-004

### VAL-STREAM-EVAL-005

Inspect one benchmark profile definition.

**Pass condition:** the profile fixes corpus identities, streaming pass inputs,
classifier-side probe workloads, metric declarations, gate declarations,
comparative ranking weights, deferred research-goal records, and
reproducibility metadata for one campaign.

**Traces to:** REQ-STREAM-EVAL-006

### VAL-STREAM-EVAL-006

Run the same candidate twice under the same benchmark profile.

**Pass condition:** the evaluator compares pass-report sequences and
classifier-side probe outputs across repeated executions and reports whether the
observable results are deterministic.

**Traces to:** REQ-STREAM-EVAL-008, REQ-STREAM-EVAL-010

### VAL-STREAM-EVAL-007

Run a conforming deterministic candidate under a benchmark profile whose
reproducibility metadata is fixed.

**Pass condition:** the resulting provenance manifest deterministically records
benchmark profile identity, corpus identities, candidate identity, clustering
configuration, seed policy, software identity, floating-point profile metadata,
and hardware-profile metadata.

**Traces to:** REQ-STREAM-EVAL-008

### VAL-STREAM-EVAL-008

Execute a candidate through at least one multi-batch pass and one classifier
probe workload.

**Pass condition:** the evaluator drives the candidate through trainer
construction, pass ingestion, `finish_pass()`, training completion, classifier
production, and classifier-side probing according to the benchmark profile.

**Traces to:** REQ-STREAM-EVAL-009

### VAL-STREAM-EVAL-009

Inspect one completed campaign report.

**Pass condition:** the result model distinguishes shared-contract prerequisite
checks, must-pass gates, and comparative metrics, and does not rank a gate
failing candidate as a surviving success.

**Traces to:** REQ-STREAM-EVAL-011

### VAL-STREAM-EVAL-010

Inspect the metric and gate declarations in a benchmark profile and the
corresponding campaign report.

**Pass condition:** each declared metric, gate, or deferred research-goal
record traces to one or more research goals and is tagged as direct, proxy, or
deferred.

**Traces to:** REQ-STREAM-EVAL-012

### VAL-STREAM-EVAL-011

Inspect the emitted campaign artifacts for one benchmark execution.

**Pass condition:** the evaluator emits a machine-readable run report per
candidate, a machine-readable comparative campaign report, and a human-readable
scorecard summarizing pass/fail status, metric values, and survivor ranking.

**Traces to:** REQ-STREAM-EVAL-013

### VAL-STREAM-EVAL-012

Run the evaluator with an invalid benchmark profile and with a candidate that
returns a shared-contract failure.

**Pass condition:** failures are surfaced deterministically and distinguish
invalid evaluator configuration from candidate-reported shared-contract failure.

**Traces to:** REQ-STREAM-EVAL-014

### VAL-STREAM-EVAL-013

Run the evaluator with a candidate that fails one declared benchmark gate and
with a benchmark profile that includes at least one deferred research goal.

**Pass condition:** the resulting report distinguishes evaluator-owned gate
failure from incomplete or unsupported measurement due to a deferred research
requirement.

**Traces to:** REQ-STREAM-EVAL-014, REQ-STREAM-EVAL-016

### VAL-STREAM-EVAL-014

Inspect the benchmark profile and scorecard against the clustering research
documents.

**Pass condition:** research goals requiring full hierarchy, routing, or durable
index artifacts are recorded as deferred rather than claimed as fully proven by
the streaming clustering evaluator alone.

**Traces to:** REQ-STREAM-EVAL-012, REQ-STREAM-EVAL-016

### VAL-STREAM-EVAL-015

Inspect repository verification artifacts for the new crate.

**Pass condition:** executable tests exist for benchmark-profile validation,
candidate execution, repeated-run determinism checks, comparative scorecard
generation, failure classification, and deferred-goal reporting.

**Traces to:** REQ-STREAM-EVAL-015
