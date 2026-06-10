<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Streaming Clustering Evaluator Crate Design

## Status

Draft design specification for a Rust crate that evaluates candidate streaming
clustering implementations for LexonGraph.

## Design Goals

The crate design is intended to be:

- reusable across candidate algorithm crates
- explicit about what is measured directly versus approximately
- deterministic at the observable benchmark boundary
- comparative without redefining the shared candidate contract
- honest about research goals that remain deferred at this boundary

## Crate Boundary

The crate owns:

- evaluator-owned benchmark profile types
- evaluator-owned candidate registration and campaign orchestration types
- evaluator-owned provenance, result, and scorecard types
- evaluator-owned gate and ranking logic
- executable and reusable-library entry points for benchmark campaigns

The crate does not own:

- the shared streaming clustering trainer/classifier definitions
- a concrete clustering implementation
- the full LexonGraph index build, routing, or storage lifecycle
- a claim that proxy metrics fully prove end-to-end hierarchy behavior

## Design Entries

### DSG-STREAM-EVAL-001 `Composite normative boundary`

The crate depends on the two clustering research documents for evaluation intent
and on `docs/specs/rust-streaming-clustering-crate/` for the candidate
trainer/classifier boundary. The crate does not redefine those sources.

### DSG-STREAM-EVAL-002 `Evaluator-owned boundary`

The crate owns benchmark profile, campaign orchestration, provenance, result,
and scorecard types. It does not own a broader candidate algorithm API than the
shared streaming clustering contract.

### DSG-STREAM-EVAL-003 `Executable plus reusable library surface`

The primary deliverable is an executable benchmark harness. The executable is a
thin front end over a reusable library surface so repository code and tests can
construct benchmark profiles, register candidates, execute campaigns, and
consume reports without shelling out.

### DSG-STREAM-EVAL-004 `Candidate adapter shape`

Each candidate is represented through an evaluator-owned adapter or factory that
constructs a `StreamingClusterTrainer` conforming to the shared contract and
provides the candidate identity metadata needed for campaign reports. The
adapter does not expose algorithm-specific evaluation hooks outside that shared
boundary.

### DSG-STREAM-EVAL-005 `Benchmark profile shape`

The benchmark profile fixes all candidate-comparable inputs for one campaign,
including:

- corpus panel identities or equivalent dataset handles
- the streaming pass plan used for candidate training
- classifier-side probe workloads such as held-out embeddings or other
  benchmark-owned probes
- metric declarations, gate declarations, and comparative ranking weights
- explicit deferred research-goal records for goals that cannot be proven at
  this boundary
- declared reproducibility metadata such as floating-point and hardware profile
  descriptors

### DSG-STREAM-EVAL-006 `Shared-profile campaign execution`

One evaluation campaign binds one benchmark profile to one or more registered
candidates. The runner fans the shared profile out across candidates rather than
allowing candidate-specific benchmark contracts that would break comparability.

### DSG-STREAM-EVAL-007 `Provenance manifest`

Before reporting comparative results, the runner materializes a deterministic
provenance manifest containing the benchmark profile identity, corpus identities,
candidate identity, shared clustering configuration, deterministic seed policy,
software version identity, and declared execution-profile metadata.

### DSG-STREAM-EVAL-008 `Candidate execution flow`

For each candidate run, the runner:

1. constructs a trainer through the candidate adapter
2. replays the benchmark profile's declared pass inputs through
   `ingest_batch()`
3. completes each pass with `finish_pass()` and records the resulting pass
   reports
4. transitions through `complete_training()` when required by the profile
5. produces a classifier through `into_classifier()`
6. executes the classifier-side probe workloads and records the observed outputs

### DSG-STREAM-EVAL-009 `Observable determinism checks`

The runner can re-execute the same candidate under the same benchmark profile
and compare the observable boundary:

- pass report sequences
- classifier-side assignments or other classifier probe results
- deterministic provenance fields expected to remain identical

Determinism evaluation is scoped to this observable boundary rather than to
unobservable internal state.

### DSG-STREAM-EVAL-010 `Result taxonomy`

The evaluator result model separates:

- shared-contract prerequisite checks needed before comparative interpretation
- must-pass gates that decide campaign survival
- comparative metrics used to rank only surviving candidates

Each metric, gate, or deferred research-goal record carries traceability to its
motivating research goal and is tagged as direct, proxy, or deferred.

### DSG-STREAM-EVAL-011 `Deferred research requirement handling`

When a research goal from `docs/research/clustering.md` cannot be proven through
the shared streaming clustering boundary and benchmark fixtures alone, the
benchmark profile and result schema record that goal as deferred rather than
misreporting a proxy as full proof.

### DSG-STREAM-EVAL-012 `Output artifacts`

The evaluator emits:

- a machine-readable run report per candidate
- a machine-readable comparative campaign report spanning all candidates
- a human-readable scorecard that summarizes gates, direct metrics, proxy
  metrics, deferred goals, and survivor ranking

### DSG-STREAM-EVAL-013 `Failure taxonomy`

Evaluator failures are reported through an evaluator-owned structured taxonomy
that distinguishes invalid evaluator configuration, candidate-reported
shared-contract failure, evaluator-owned gate failure, and incomplete or
unsupported measurement due to a deferred research requirement.

### DSG-STREAM-EVAL-014 `Explicit non-goal boundary`

This revision does not model or claim proof of full end-to-end LexonGraph
hierarchy properties requiring artifacts beyond the shared streaming clustering
boundary, such as leaf-packing invariants, internal-node summaries, bounded tree
shape, persisted-hierarchy routing, or durable storage semantics.

### DSG-STREAM-EVAL-015 `Verification artifacts`

The repository includes executable verification artifacts covering benchmark
profile validation, candidate execution, observable determinism checking,
comparative scorecard generation, failure classification, and deferred-goal
reporting for the evaluator crate.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-STREAM-EVAL-001 | REQ-STREAM-EVAL-002 |
| DSG-STREAM-EVAL-002 | REQ-STREAM-EVAL-001, REQ-STREAM-EVAL-004, REQ-STREAM-EVAL-005 |
| DSG-STREAM-EVAL-003 | REQ-STREAM-EVAL-003 |
| DSG-STREAM-EVAL-004 | REQ-STREAM-EVAL-004, REQ-STREAM-EVAL-005 |
| DSG-STREAM-EVAL-005 | REQ-STREAM-EVAL-006 |
| DSG-STREAM-EVAL-006 | REQ-STREAM-EVAL-007 |
| DSG-STREAM-EVAL-007 | REQ-STREAM-EVAL-008 |
| DSG-STREAM-EVAL-008 | REQ-STREAM-EVAL-009 |
| DSG-STREAM-EVAL-009 | REQ-STREAM-EVAL-010 |
| DSG-STREAM-EVAL-010 | REQ-STREAM-EVAL-011, REQ-STREAM-EVAL-012 |
| DSG-STREAM-EVAL-011 | REQ-STREAM-EVAL-012, REQ-STREAM-EVAL-016 |
| DSG-STREAM-EVAL-012 | REQ-STREAM-EVAL-013 |
| DSG-STREAM-EVAL-013 | REQ-STREAM-EVAL-014 |
| DSG-STREAM-EVAL-014 | REQ-STREAM-EVAL-016 |
| DSG-STREAM-EVAL-015 | REQ-STREAM-EVAL-015 |
