<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Clustering Evaluator Crate Requirements

## Status

Draft specification for a Rust crate that evaluates candidate streaming
clustering implementations for LexonGraph.

## Scope

This document specifies the crate-level requirements for a new Rust crate that:

- provides a reusable executable benchmark harness for comparing candidate
  streaming clustering implementations
- reuses the shared trainer/classifier boundary defined by
  `docs/specs/rust-streaming-clustering-crate/`
- translates applicable intent from `docs/research/clustering.md` and
  `docs/research/clustering_plan.md` into an evaluator-owned benchmark contract

This document defines the evaluator boundary, benchmark contract, campaign
execution model, scorecard outputs, and failure taxonomy. It does not require a
concrete clustering algorithm and does not redefine the shared streaming
clustering contract.

## Terminology

In this spec package, `candidate` means one clustering implementation entered
into evaluation through the shared streaming clustering trainer/classifier
boundary.

`Benchmark profile` means the evaluator-owned description of the fixed corpora,
pass plan, probe workloads, metric declarations, gates, and ranking weights
used for one comparative evaluation campaign.

`Evaluation campaign` means one execution of the evaluator over one benchmark
profile and one or more named candidates.

`Direct metric` means an evaluator result whose measured behavior is directly
observable from the shared streaming clustering boundary and the benchmark
fixtures.

`Proxy metric` means an evaluator result intended to approximate a research goal
whose full end-to-end LexonGraph property is not directly observable from the
shared streaming clustering boundary alone.

`Deferred research requirement` means one requirement from
`docs/research/clustering.md` that this evaluator revision records as
out-of-scope because proving it requires artifacts outside the shared streaming
clustering boundary.

## Requirements

### REQ-STREAM-EVAL-001

The repository shall define a dedicated Rust crate at
`crates/lexongraph-streaming-clustering-evaluator` that owns the reusable
streaming clustering evaluation boundary for LexonGraph.

### REQ-STREAM-EVAL-002

The new crate shall remain subordinate to:

- `docs/research/clustering.md` for the research goals motivating evaluation
- `docs/research/clustering_plan.md` for the intended comparative benchmark
  workflow
- `docs/specs/rust-streaming-clustering-crate/` for the shared candidate
  trainer/classifier contract

If those sources appear to conflict, the narrower evaluator scope remains
authoritative for this crate's boundary: the research documents are
authoritative for evaluation intent, and the shared streaming clustering
specification is authoritative for the candidate integration surface.

### REQ-STREAM-EVAL-003

The crate shall provide a reusable executable benchmark harness, with a
supporting reusable library surface, for running comparative evaluations of one
or more candidate streaming clustering implementations.

### REQ-STREAM-EVAL-004

Candidates shall plug into the evaluator through the existing shared streaming
clustering trainer/classifier contract. This revision shall not require a
broader candidate API than the boundary defined by
`docs/specs/rust-streaming-clustering-crate/`.

### REQ-STREAM-EVAL-005

The evaluator shall remain algorithm-neutral and shall not require any specific
clustering method, centroid model, update rule, distance function, or balance
policy realization beyond what is observable through the shared candidate
boundary and benchmark fixtures.

### REQ-STREAM-EVAL-006

The evaluator shall define a benchmark profile that fixes, for one evaluation
campaign:

- the corpus panel or equivalent named input datasets
- the candidate training pass plan and any held-out probe workloads
- metric declarations and whether each metric is direct or proxy
- must-pass gates and any comparative ranking weights
- deferred research-goal records for requirements that cannot be proven at this
  boundary
- deterministic execution-profile metadata needed to interpret reproducibility

### REQ-STREAM-EVAL-007

Within one evaluation campaign, all compared candidates shall be executed
against the same benchmark profile rather than candidate-specific benchmark
contracts.

### REQ-STREAM-EVAL-008

Each evaluation campaign shall emit a deterministic provenance manifest that
records at least:

- benchmark profile identity
- corpus identities
- candidate identity
- shared clustering configuration used for the candidate
- deterministic seed policy
- executable or crate version identity
- declared floating-point execution profile metadata
- declared hardware-profile metadata

### REQ-STREAM-EVAL-009

For each candidate run, the evaluator shall exercise the candidate through the
shared lifecycle by:

- constructing or obtaining a trainer through the shared candidate boundary
- executing the caller-driven streaming passes declared by the benchmark profile
- obtaining pass reports from completed passes
- marking training complete when the benchmark profile requires it
- producing a classifier and running the declared classifier-side probe
  workloads

### REQ-STREAM-EVAL-010

The evaluator shall support repeated identical executions of the same candidate
under the same benchmark profile and shall compare observable results for
determinism, including pass reports and classifier-side assignments over the
declared probe workloads.

### REQ-STREAM-EVAL-011

The evaluator shall distinguish, in its observable result model:

- shared-contract prerequisite checks
- benchmark must-pass gates
- comparative score metrics used only among candidates that pass the required
  gates

The evaluator shall not rank a candidate as a successful survivor if it fails a
must-pass gate.

### REQ-STREAM-EVAL-012

Each evaluator-owned metric, gate, or deferred research-goal record shall trace
to one or more motivating research goals from `docs/research/clustering.md` or
`docs/research/clustering_plan.md` and shall declare whether that research-goal
coverage is:

- direct
- proxy
- deferred because the research goal cannot be proven at this boundary

### REQ-STREAM-EVAL-013

The evaluator shall emit:

- a machine-readable per-candidate run report
- a machine-readable comparative campaign report
- a human-readable scorecard summarizing pass/fail status, metric values, and
  comparative ranking for surviving candidates

### REQ-STREAM-EVAL-014

The evaluator shall surface deterministic structured failures that distinguish
at least:

- invalid evaluator or benchmark-profile configuration
- candidate-reported shared-contract failure
- evaluator-owned gate failure
- incomplete or unsupported measurement caused by a deferred research
  requirement

### REQ-STREAM-EVAL-015

The repository shall include executable verification artifacts that realize the
validation plan for the streaming clustering evaluator crate.

### REQ-STREAM-EVAL-016

This revision shall not claim to prove full end-to-end LexonGraph hierarchy
conformance for properties that require artifacts outside the shared streaming
clustering boundary, including leaf packing, internal-node summaries, bounded
fanout and depth, search routing over a persisted hierarchy, artifact
serialization, and durable index build semantics.

## Out of Scope

This crate does not define or own:

- a concrete clustering algorithm
- changes to the shared streaming clustering contract
- the full LexonGraph indexing or search runtime
- a canonical report schema shared with unrelated repository crates
- proof of end-to-end hierarchical index conformance beyond the shared
  streaming clustering boundary

## Relationship to Other Specifications

This document creates a new evaluator line layered on top of the shared
streaming clustering trait boundary and motivated by the clustering research
documents.

If future repository specifications define an end-to-end index evaluation line,
that future narrower package may own the requirements currently recorded here as
deferred research requirements.
