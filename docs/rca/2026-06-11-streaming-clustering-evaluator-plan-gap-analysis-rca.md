<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# RCA: Streaming Clustering Evaluator Spec Package vs Clustering Plan

## Status

Gap analysis comparing `docs/research/clustering_plan.md` to
`docs/specs/rust-streaming-clustering-evaluator-crate/`.

## Summary

The evaluator spec package is a **deliberately narrowed translation** of the
clustering plan rather than a full transcription of it.

It translates the plan's **leaf-stage benchmark harness** ideas into normative
requirements, design entries, and validation artifacts:

- shared benchmark profiles
- candidate-neutral harness execution
- leaf-membership materialization
- leaf-stage invariants
- same-leaf locality scoring
- local compression scoring
- deterministic provenance and structured failures
- scalable corpus sourcing through block stores and zip-backed overlays

The largest gaps are not accidental omissions inside the current evaluator
boundary. They are mostly the result of an explicit scope cut:

1. the research plan is for **full hierarchical design selection**
2. the evaluator spec package is for **leaf-partition evaluation only**

That scope cut is made explicit in:

- `docs/specs/rust-streaming-clustering-evaluator-crate/requirements.md`
  (`REQ-STREAM-EVAL-021`, out-of-scope hierarchy, routing, serialization, and
  durable build semantics)
- `docs/specs/rust-streaming-clustering-evaluator-crate/design.md`
  (`DSG-STREAM-EVAL-018`, explicit non-goal boundary)
- `docs/specs/rust-streaming-clustering-evaluator-crate/validation.md`
  (`VAL-STREAM-EVAL-018`, deferred hierarchy and routing goals remain explicit)

## Scope Summary

| Surface | Artifact |
|---|---|
| Research workflow compared | `docs/research/clustering_plan.md` |
| Governing spec package compared | `docs/specs/rust-streaming-clustering-evaluator-crate/requirements.md`, `design.md`, `validation.md` |
| Implementation context consulted | `crates/lexongraph-streaming-clustering-evaluator/src/lib.rs`, `src/main.rs`, `tests/spec_validation.rs` |
| Coverage artifact consulted for implementation-validation context | `lcov.info` generated via `cargo llvm-cov --workspace --all-features --locked --lcov --output-path lcov.info` |

## Translation Status Legend

| Status | Meaning |
|---|---|
| Adopted | The plan section is materially represented in the evaluator spec package |
| Narrowed | The section is represented, but only for the evaluator's leaf-stage boundary |
| Deferred | The spec package explicitly records the section as outside this revision |
| Omitted | The section is not translated into the evaluator spec package and is not explicitly owned there |

## Section-by-Section Mapping

| Plan section | Status | Spec-package translation | Main gap |
|---|---|---|---|
| `## 1. Freeze the Evaluation Contract First` | Narrowed | `REQ-STREAM-EVAL-006`, `REQ-STREAM-EVAL-008`, `REQ-STREAM-EVAL-017` to `REQ-STREAM-EVAL-020`; `DSG-STREAM-EVAL-005`, `DSG-STREAM-EVAL-007`, `DSG-STREAM-EVAL-011` to `DSG-STREAM-EVAL-013`; `VAL-STREAM-EVAL-005`, `VAL-STREAM-EVAL-007`, `VAL-STREAM-EVAL-009` to `VAL-STREAM-EVAL-012` | Leaf size, dimensions, alignment policy, compression baseline, FP profile, and hardware profile are translated; metric contract, beam width, fanout, search target, dimensionality range, dispersion functional, and threading contract are not |
| `## 2. Build a Corpus Panel` | Narrowed | `REQ-STREAM-EVAL-006`, `REQ-STREAM-EVAL-022` to `REQ-STREAM-EVAL-029`; `DSG-STREAM-EVAL-005`, `DSG-STREAM-EVAL-008A` to `DSG-STREAM-EVAL-008E`; `VAL-STREAM-EVAL-020` to `VAL-STREAM-EVAL-024` | The spec supports corpus identities and scalable corpus sources, but it does not require a representative multi-family corpus panel, small/medium/large scaling tiers, or a mandatory held-out exact-NN query suite |
| `## 3. Implement a Common Evaluation Harness` | Narrowed | `REQ-STREAM-EVAL-003`, `REQ-STREAM-EVAL-007` to `REQ-STREAM-EVAL-020`, `REQ-STREAM-EVAL-024` to `REQ-STREAM-EVAL-030`; `DSG-STREAM-EVAL-003` to `DSG-STREAM-EVAL-017`; `VAL-STREAM-EVAL-003` to `VAL-STREAM-EVAL-017`, `VAL-STREAM-EVAL-020` to `VAL-STREAM-EVAL-024` | The spec defines a common harness, provenance, reports, failures, and leaf-stage gates, but not a full tree artifact, serialization round-trip identity, artifact-hygiene verification, or resource metrics |
| `## 4. Compare Candidate Leaf-Formation Strategies First` | Narrowed | `REQ-STREAM-EVAL-004`, `REQ-STREAM-EVAL-005`, `REQ-STREAM-EVAL-017` to `REQ-STREAM-EVAL-020`; `DSG-STREAM-EVAL-004`, `DSG-STREAM-EVAL-010` to `DSG-STREAM-EVAL-015`; `VAL-STREAM-EVAL-009` to `VAL-STREAM-EVAL-018` | The spec supports leaf-stage comparison, padding handling, and gate-based elimination, but does not encode the plan's candidate-family matrix (A-F), build-time-per-vector metric, or explicit top-2/top-3 down-selection workflow |
| `## 5. Evaluate Hierarchy Construction Separately from Leaf Formation` | Deferred | `REQ-STREAM-EVAL-021`; `DSG-STREAM-EVAL-018`; `VAL-STREAM-EVAL-018` | Bounded fanout, depth, and refinement checks are explicitly outside this evaluator revision |
| `## 6. Compare Parent Summary Schemes` | Deferred | `REQ-STREAM-EVAL-021`; `DSG-STREAM-EVAL-018`; `VAL-STREAM-EVAL-018` | Parent summaries, relative summary error, perturbation sensitivity, and summary storage cost are explicitly outside this evaluator revision |
| `## 7. Run Search and Routing Benchmarks` | Deferred | `REQ-STREAM-EVAL-019`, `REQ-STREAM-EVAL-021`; `DSG-STREAM-EVAL-012`, `DSG-STREAM-EVAL-018`; `VAL-STREAM-EVAL-011`, `VAL-STREAM-EVAL-018` | The evaluator keeps only same-leaf locality; routing recall, beam-width studies, latency, QPS, and node-visit metrics are deferred |
| `## 8. Run Robustness and Invariant Tests` | Narrowed | `REQ-STREAM-EVAL-011`, `REQ-STREAM-EVAL-015`, `REQ-STREAM-EVAL-017`, `REQ-STREAM-EVAL-018`; `DSG-STREAM-EVAL-009`, `DSG-STREAM-EVAL-011`, `DSG-STREAM-EVAL-017`; `VAL-STREAM-EVAL-006`, `VAL-STREAM-EVAL-016`, `VAL-STREAM-EVAL-017` | Repeated-run determinism and structured failures are translated, but 1-thread vs N-thread identity, scheduler-jitter checks, serialization reload identity, and durable-success/artifact-cleanup requirements are not |
| `## 9. Use a Stage-Gated Scorecard` | Narrowed | `REQ-STREAM-EVAL-012` to `REQ-STREAM-EVAL-014`; `DSG-STREAM-EVAL-014` to `DSG-STREAM-EVAL-016`; `VAL-STREAM-EVAL-013` to `VAL-STREAM-EVAL-015` | The spec has must-pass gates, ranking among survivors, and a scorecard, but not the plan's four-stage decision flow or recall/latency/build-cost/memory weighting model |
| `## 10. Practical Execution Order` | Omitted | None as normative evaluator requirements | The spec package does not encode the plan's setup/screening/optimization/scale-validation project sequence |
| `## 11. Expected Outputs` | Narrowed | `REQ-STREAM-EVAL-008`, `REQ-STREAM-EVAL-014`, `REQ-STREAM-EVAL-015`; `DSG-STREAM-EVAL-007`, `DSG-STREAM-EVAL-016`, `DSG-STREAM-EVAL-017`; `VAL-STREAM-EVAL-007`, `VAL-STREAM-EVAL-015`, `VAL-STREAM-EVAL-016` | Provenance, run reports, campaign reports, and scorecards are covered; final recommendation outputs and explicit cross-design gap-analysis outputs are not |
| `## 12. Likely Best First Bets` | Omitted | None by design | The section is exploratory portfolio guidance, not evaluator-contract behavior |
| `## 13. Key Risk to Watch Early` | Omitted | None directly; only partial indirect coverage through leaf invariants and same-leaf locality | The spec package does not record this as an explicit risk statement or decision heuristic |

## Gap Inventory

### G-001 `Evaluation contract under-translates several section-1 controls`

**Type:** Narrowed translation  
**Priority:** Medium  

The research plan's section 1 defines a richer frozen contract than the spec
package currently models. The evaluator spec captures:

- leaf size
- dimensionality
- alignment policy
- quantization baseline label
- floating-point profile
- hardware profile

But it does **not** model:

- declared metric family for build/summary/routing/evaluation
- beam width
- search target
- fanout bounds
- supported dimensionality range (`d_min`, `d_max`)
- dispersion functional
- threading model and 1-thread vs N-thread identity contract

**Evidence**

- Plan section 1 requires those controls:
  `docs/research/clustering_plan.md:32-54`
- The evaluator benchmark profile is narrower:
  `REQ-STREAM-EVAL-006` in
  `docs/specs/rust-streaming-clustering-evaluator-crate/requirements.md`
- The design mirrors that narrower profile:
  `DSG-STREAM-EVAL-005` in
  `docs/specs/rust-streaming-clustering-evaluator-crate/design.md`

**Impact**

The evaluator spec can freeze a **leaf-stage campaign contract**, but not the
full experiment contract needed to compare end-to-end hierarchical candidates
under the research plan.

**Recommended framing**

Decide whether these controls belong in:

1. a future end-to-end evaluator spec package, or
2. an expanded benchmark-contract layer above the current leaf-stage evaluator

### G-002 `Corpus-panel methodology is only partially translated`

**Type:** Narrowed translation  
**Priority:** Medium  

The research plan requires a representative corpus panel with multiple corpus
families, size tiers, and a held-out query set with exact ground truth. The
evaluator spec package instead requires only that one benchmark profile define
its corpus identities, workload sources, and ground-truth material relevant to
the leaf-stage metrics.

**Evidence**

- Corpus-panel workflow:
  `docs/research/clustering_plan.md:58-83`
- Evaluator profile and corpus-source requirements:
  `REQ-STREAM-EVAL-006`, `REQ-STREAM-EVAL-022` to `REQ-STREAM-EVAL-029`
- Unified source model design:
  `DSG-STREAM-EVAL-005`, `DSG-STREAM-EVAL-008A` to `DSG-STREAM-EVAL-008E`

**Impact**

The spec package supports scalable corpus ingestion, but it does not by itself
force the broad benchmark panel needed for design-selection confidence.

**Recommended framing**

If the repository wants the evaluator spec to own comparative experiment
discipline, add a benchmark-suite or corpus-panel concept. Otherwise, make
explicit that corpus-panel composition remains research-playbook policy rather
than evaluator conformance.

### G-003 `The common harness is translated only at the leaf boundary`

**Type:** Intentional scope cut  
**Priority:** High clarity, low defect risk  

The research plan's section 3 expects one harness that emits full tree
artifacts, evaluates serialization round trips, verifies artifact hygiene, and
collects search, summary, refinement, and resource metrics. The evaluator spec
package deliberately limits the harness to leaf-stage behavior observable from
the shared trainer/classifier boundary.

**Evidence**

- Full harness expectations:
  `docs/research/clustering_plan.md:87-119`
- Leaf-stage evaluator boundary:
  `REQ-STREAM-EVAL-003` to `REQ-STREAM-EVAL-021`
- Explicit non-goal boundary:
  `DSG-STREAM-EVAL-018`
- Deferred-goal validation:
  `VAL-STREAM-EVAL-018`

**Impact**

This is the main reason the plan and the spec package differ. Without reading
the out-of-scope language, a reader could mistake the evaluator spec for a full
hierarchy-selection harness when it is actually a leaf-stage precursor.

**Recommended framing**

Keep the current scope cut, but make the translation boundary more prominent
whenever the evaluator package is referenced from the research plan.

### G-004 `Hierarchy, summary, and routing sections are intentionally deferred`

**Type:** Explicit deferral  
**Priority:** Informational  

Plan sections 5, 6, and 7 describe hierarchy construction, parent summary
schemes, and routing/search benchmarks. The evaluator spec package does not
attempt to own those sections in this revision.

**Evidence**

- Hierarchy plan:
  `docs/research/clustering_plan.md:173-200`
- Summary plan:
  `docs/research/clustering_plan.md:203-229`
- Routing plan:
  `docs/research/clustering_plan.md:232-254`
- Explicit deferral:
  `REQ-STREAM-EVAL-021`, `DSG-STREAM-EVAL-018`, `VAL-STREAM-EVAL-018`

**Impact**

These are not accidental gaps. They are the clearest statement that the current
spec package is a **leaf-stage evaluator line**, not a full index-evaluation
line.

**Recommended framing**

No correction is needed inside this package unless the repository decides to
expand this spec beyond the leaf boundary.

### G-005 `Robustness and scorecard methodology are only partly normalized`

**Type:** Mixed: some narrowing, some omission  
**Priority:** Medium  

The research plan expects:

- thread-count determinism studies
- CPU-jitter robustness checks
- serialization reload identity
- artifact cleanup and durable-success verification
- stage-gated ranking across invariants, geometry, routing, and cost

The evaluator spec package instead normalizes only:

- repeated observable determinism
- leaf-stage gates
- structured failures
- rank-only-among-survivors scorecard behavior

**Evidence**

- Robustness plan:
  `docs/research/clustering_plan.md:257-272`
- Scorecard plan:
  `docs/research/clustering_plan.md:276-300`
- Evaluator result taxonomy and scorecard:
  `REQ-STREAM-EVAL-012` to `REQ-STREAM-EVAL-015`
- Determinism and failure design:
  `DSG-STREAM-EVAL-009`, `DSG-STREAM-EVAL-014` to `DSG-STREAM-EVAL-017`

**Impact**

The evaluator scorecard is appropriate for leaf-stage comparison, but it is not
yet the stage-gated design-selection scorecard envisioned by the research plan.

**Recommended framing**

Treat the current scorecard as **Gate 1 / leaf-quality preselection**, not as
the final multi-stage winner-selection mechanism from the full plan.

### G-006 `Execution-order and final decision memo outputs are not spec-owned`

**Type:** Omitted by design  
**Priority:** Low  

The plan's project-management sections are not translated into evaluator
requirements:

- practical execution order (`## 10`)
- final recommendation memo outputs (`## 11`)
- suggested starting candidates (`## 12`)
- explicit early-risk framing (`## 13`)

**Evidence**

- Plan sections:
  `docs/research/clustering_plan.md:303-389`
- No corresponding REQ/DSG/VAL entries in
  `docs/specs/rust-streaming-clustering-evaluator-crate/`

**Impact**

The spec package defines evaluator conformance, not the full research program
or final architecture-decision workflow.

**Recommended framing**

Keep these sections in research or program-management documents unless the
repository wants a higher-level experiment-orchestration specification.

## Dominant Gap Pattern

**Intentional narrowing from full hierarchy-selection plan to leaf-stage
evaluator contract.**

Most of the apparent gaps are not contradictions. They are translation
decisions:

- the plan is an end-to-end comparative research workflow
- the spec package is a normative leaf-boundary evaluator package

The smaller set of genuine under-translations are the places where the plan's
benchmark-contract discipline is only partly carried over even for leaf-stage
comparisons, especially around:

- metric-contract declaration
- dimensionality-range declaration
- corpus-panel methodology
- explicit positioning of the current scorecard as only a first-stage filter

## Recommended Next Actions

1. Decide whether the missing section-1 controls belong in this package or in a
   future end-to-end evaluator package.
2. Decide whether corpus-panel composition should become normative evaluator
   behavior or remain research-playbook guidance.
3. Add one short bridge note from the research plan to the evaluator spec
   package clarifying that the current package realizes only the leaf-stage
   screening subset of the plan.
4. If the repository expects this evaluator to remain the long-term benchmark
   front end, consider a follow-on spec package for hierarchy, summary,
   routing, and durable-artifact evaluation rather than stretching this package
   past its current boundary.

## Scope Limitation

This document compares the **research plan** to the **evaluator spec package**.
It is not itself a proof that the implementation fully satisfies the spec
package, and it does not clear covered implementation paths for compliance.
