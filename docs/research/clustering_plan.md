<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->

# Experimental Plan for Selecting a Hierarchical Clustering / Index Design

## Purpose

Identify the simplest design that can satisfy the black-box requirements in `clustering.md`, especially:

- exact leaf size `L`
- deterministic build behavior
- bounded fanout/depth
- strong greedy-routing recall
- locality preservation
- compression-friendly leaves
- stable parent summaries
- acceptable build/query cost

This plan is designed to be executable on a representative sample corpus before committing to a production architecture.

## Decision Outcome

At the end of this plan, we should be able to answer:

1. Which leaf-construction strategy best satisfies exact-size leaves without destroying locality?
2. Which hierarchy-construction strategy gives the best recall/latency tradeoff under greedy routing?
3. Which parent-summary method is accurate and stable enough for routing?
4. Whether one design can satisfy the hard invariants and quality thresholds together, or whether the requirements need revision.

---

## 1. Freeze the Evaluation Contract First

Before comparing approaches, lock these items so results are comparable:

| Item | What to freeze |
| --- | --- |
| Metric | Cosine or Euclidean, chosen once per experiment track |
| Leaf size | One primary value, e.g. `L=64`, plus at most two sensitivity values such as `32` and `128` |
| Fanout bounds | Example: `f_min=4`, `f_max=16` |
| Beam widths | `1` by default; optionally `2`, `4`, `8` only as fallback studies |
| Search target | `TNN Recall@10 >= 90%` under the declared routing procedure |
| Locality target | `>=80%` of true top-10 neighbors in same or sibling leaves |
| Dimensionality | Fixed per corpus |
| Supported dimensionality contract | Declare `d_min`, `d_max`, and deterministic out-of-range rejection behavior |
| Metric contract | Declare the exact build, summary, routing, and evaluation metric; if transformed, prove ordering preservation and routing validity |
| Dispersion functional | For Euclidean use variance; for non-Euclidean metrics declare the compatible dispersion functional before evaluation |
| Quantization benchmark | Fix one standard scheme, e.g. 8-bit scalar quantization; define the global baseline over the unindexed real dataset only, excluding synthetic padding |
| Floating-point profile | Fixed compiler flags, deterministic reduction order, fixed thread count policy |
| Threading contract | Declare candidate threading model and reduction-order strategy; require 1-thread vs N-thread bitwise identity |
| Hardware profile | Single named machine for all benchmark runs |
| Dataset alignment policy | Strict alignment or deterministic synthetic padding |

**Deliverable:** a one-page benchmark contract that all later experiments must use.

---

## 2. Build a Corpus Panel

Do not evaluate on a single corpus. Use a small panel that exposes different failure modes.

### Required corpus slices

| Slice | Why it matters |
| --- | --- |
| Real sample corpus | Measures actual usefulness |
| Well-clustered synthetic corpus | Reveals best-case structure quality |
| Weak-cluster / uniform corpus | Reveals brittle routing assumptions |
| Anisotropic / manifold corpus | Tests locality preservation under non-spherical structure |
| Near-duplicate-heavy corpus | Tests determinism, tie-breaking, and bucket quality |
| Size-scaled subsets | Needed for scaling and depth/fanout validation |

### Minimum sizes

For each corpus family, prepare at least:

- small: enough for rapid iteration
- medium: realistic development benchmark
- large: at least 4x small, to satisfy the scaling requirement

If possible, use three sizes such as `50k`, `200k`, and `800k` vectors, or the closest practical equivalents from the sample corpus.

**Deliverable:** immutable train/build corpora plus a separate query set and exact-nearest-neighbor ground truth.

---

## 3. Implement a Common Evaluation Harness

Every candidate design must plug into the same harness and emit the same artifacts.

### Required harness outputs

1. Build manifest: corpus ID, config, seed, binary version, FP profile, hardware profile.
2. Tree artifact: leaf assignments, internal structure, summaries, metadata.
3. Metrics report: all hard invariants and all quality metrics.
4. Failure report: deterministic error code and message when a build is invalid.

### Hard-invariant gates that must be implemented in the harness

1. Exact leaf-size check and complete-coverage check.
2. Bitwise-deterministic rebuild check, including 1-thread vs N-thread comparison when multithreading is supported.
3. Serialization round-trip gate: persisted artifact must reload into a bitwise-identical queryable structure.
4. Structured-failure gate: deterministic error code, no exposed partial artifact, and explicit artifact-hygiene verification on injected failures.

### Metrics to compute for every run

| Category | Metrics |
| --- | --- |
| Hard invariants | exact leaf sizes, no duplicates, no dropped vectors, depth bound, fanout bound, determinism, serialization round-trip identity |
| Search | TNN recall@10, routing path length, beam width, p95 latency, QPS |
| Locality | percent of true top-10 neighbors in same leaf or sibling leaves |
| Compression | local-vs-global reconstruction error delta under the fixed quantization scheme, where the global baseline is computed over the unindexed real dataset excluding synthetic padding, plus per-bucket distribution |
| Summary quality | exact-vs-approx parent summary relative `L2` error using `||S_approx - S_exact||_2 / max(||S_exact||_2, delta)`, perturbation sensitivity, logged `delta` where `delta <= 10^-6 * mean(||S_exact||_2)` |
| Refinement | per-edge `beta = Disp(C) / Disp(P)`, fraction of edges with `beta <= 0.85`, and explicit tracking of the penultimate-layer `epsilon` exception where `Disp(P) <= 0.01 * Disp(Root)` and all children are leaves |
| Padding | padding count, unique-tag validation, concentration into the minimum possible number of leaves, exclusion from recall/locality/compression metrics |
| Metric contract | declared metric, any transformed metric, ordering-preservation audit result, routing/build/summary/evaluation consistency result |
| Resources | build throughput, peak build memory, loaded-index memory |

**Gate:** if a candidate fails any hard invariant, stop evaluating it further for that configuration.

---

## 4. Compare Candidate Leaf-Formation Strategies First

The leaf layer is the most constrained part of the problem because exact leaf size interacts directly with locality and compression.

### Candidate families to test

| ID | Strategy | Why test it |
| --- | --- | --- |
| A | Recursive balanced k-means / constrained k-means | Strong locality potential, but may be expensive |
| B | PCA projection + deterministic sort + exact chunking | Very simple, highly deterministic, likely fast |
| C | Space-filling curve ordering + exact chunking | Cheap, deterministic baseline |
| D | Graph-based neighborhood partitioning with exact-size balancing | May preserve local topology better |
| E | Hybrid: coarse partitioning then exact-size local rebalance | Likely practical compromise |
| F | Random shuffle + exact chunking | Null baseline for measuring algorithmic value |

### Leaf-stage experiments

For each corpus slice and each candidate:

1. Build only the leaf partition.
2. Enforce exact leaf size using the same alignment policy.
3. Measure:
   - exact-size compliance
   - determinism across repeated runs
   - top-10 neighborhood coherence
   - local compression gain vs global quantization
   - build time per vector
4. Reject any candidate that cannot reliably produce exact-size leaves deterministically.

### Required padding sub-experiment

For each leaf strategy, run an explicit `N mod L != 0` experiment under deterministic synthetic padding:

1. Verify padding entities are uniquely tagged and never duplicate real vectors.
2. Verify padding is concentrated into the minimum possible number of leaves allowed by the deterministic procedure.
3. Verify padding never appears in externally visible search results.
4. Verify padding is excluded from recall, locality, and compression metrics.
5. Compare strict-alignment rejection vs deterministic-padding behavior and cost.

### Decision rule

Carry forward only the top 2-3 leaf strategies that:

- never violate the hard invariants
- come closest to the `>=80%` locality target
- show meaningful local compression benefit
- do not have obviously unacceptable build cost

---

## 5. Evaluate Hierarchy Construction Separately from Leaf Formation

Once the best leaf strategies are identified, compare different ways to aggregate leaves into a bounded tree.

### Candidate hierarchy strategies

| ID | Strategy | Focus |
| --- | --- | --- |
| H1 | Bottom-up agglomeration with bounded fanout | Best geometric grouping, possibly expensive |
| H2 | Recursive top-down partitioning over leaf summaries | Likely scalable |
| H3 | Greedy pack-by-centroid nearest grouping | Simple, deterministic baseline |
| H4 | Hybrid: top-down until coarse scale, bottom-up at lower levels | Balances quality and cost |

### Experiments

For each surviving leaf strategy x hierarchy strategy pair:

1. Build full trees under fixed `f_min` and `f_max`.
2. Measure:
   - fanout compliance
   - absence of single-child internal nodes
   - depth vs theoretical bound
   - per-edge refinement coefficient `beta`
   - use of the penultimate-layer `epsilon` exception
   - declared dispersion functional used for `beta` when the metric is non-Euclidean
   - build throughput and memory
3. Reject pairs that routinely violate the depth bound, the `beta <= 0.85` refinement rule, or the narrow `epsilon`-gated exception.

---

## 6. Compare Parent Summary Schemes

Routing quality will depend heavily on the summary stored at each internal node.

### Summary candidates

| ID | Summary |
| --- | --- |
| S1 | Exact centroid from descendant leaves |
| S2 | Composed centroid from child summaries |
| S3 | Centroid + radius / variance scalar |
| S4 | Low-rank summary such as centroid + first principal direction |

### Experiments

For each viable leaf+higher-tree pair:

1. Compute exact parent summaries as the reference.
2. Compute approximate/composed variants.
3. Measure:
   - relative `L2` error vs exact summary using the declared `delta` floor
   - routing recall impact
   - sensitivity to small perturbations in child summaries
   - storage cost per node
4. Reject any summary scheme that exceeds the `1%` relative-error bound where that bound is intended to apply.
5. If the metric is non-Euclidean, require the candidate to declare and justify the compatible dispersion functional used for refinement checks.

---

## 7. Run Search and Routing Benchmarks

This is the main decision phase.

### Benchmark procedure

For each surviving full design:

1. Use the held-out query set.
2. Compute exact top-10 neighbors as ground truth.
3. Run greedy routing with beam width `1`.
4. If recall is below target, rerun with fixed fallback beam widths.
5. Measure:
   - recall@10
   - p50/p95 latency
   - QPS
   - average routing depth
   - nodes visited per query

### Elimination rule

Reject any design that cannot reach `TNN Recall@10 >= 90%` with a beam width small enough to preserve acceptable latency on the reference hardware.

---

## 8. Run Robustness and Invariant Tests

Any candidate that looks strong on recall must still survive invariant stress tests.

### Required stress tests

1. Repeat identical builds multiple times and bit-compare outputs.
2. Compare 1-thread vs N-thread outputs bit-for-bit when multithreading is supported.
3. Run builds under CPU contention / scheduling jitter and verify the artifact remains bit-identical.
4. Log and audit each candidate's threading model and FP reduction-order policy.
5. Test aligned and misaligned dataset sizes.
6. Inject near-duplicate and tie-heavy data.
7. Serialize and reload the index, then require bitwise-identical structure and search behavior.
8. Force build failures and verify deterministic error codes, artifact cleanup, and durable-success behavior.

**Pass condition:** no hard invariant regressions under any supported operating condition.

---

## 9. Use a Stage-Gated Scorecard

Do not optimize everything at once. Use this decision flow:

1. **Gate 1: Hard invariants**  
   Eliminate anything non-deterministic, structurally invalid, unable to enforce exact leaf sizes, or unable to pass serialization round-trip identity.
2. **Gate 2: Geometry quality**  
   Prefer designs with strongest locality and compression behavior.
3. **Gate 3: Routing quality**  
   Prefer designs that hit `TNN Recall@10 >= 90%` with beam width `1` or the smallest fallback beam.
4. **Gate 4: Cost**  
   Among remaining candidates, choose the one with the best build/query efficiency and memory behavior.

### Recommended ranking weights for survivors

| Dimension | Weight |
| --- | --- |
| Hard invariant compliance | must-pass |
| Recall / latency outcome | 35% |
| Locality preservation | 20% |
| Compression friendliness | 15% |
| Build cost | 15% |
| Query memory / storage cost | 10% |
| Implementation complexity / maintainability | 5% |

---

## 10. Practical Execution Order

### Phase 0: Setup

- freeze benchmark contract
- prepare corpus panel
- prepare exact-NN ground truth
- build common harness
- define the failure taxonomy and structured error schema
- implement artifact-hygiene and durable-success checks in the harness
- implement the metric-consistency audit for build, summary, routing, and evaluation

### Phase 1: Leaf-only screening

- run strategies A-E on all small corpora
- include Strategy F as the null baseline
- run the explicit misaligned-size padding sub-experiment
- down-select to top 2-3

### Phase 2: Full-tree screening

- combine surviving leaf strategies with H1-H4
- test on small and medium corpora
- enforce the `beta <= 0.85` refinement rule as a first-class gate
- remove structurally weak designs

### Phase 3: Summary and routing optimization

- compare S1-S4 on viable full designs
- audit exact metric consistency and any transformed-metric ordering guarantees
- run recall/latency benchmarks

### Phase 4: Scale validation

- rerun finalists on the three dataset sizes
- test dimensionality boundaries at `d_min` and `d_max`
- verify deterministic rejection outside the supported dimensionality range
- rerun 1-thread vs N-thread determinism checks on finalists
- verify scaling, memory, and service levels

### Phase 5: Final selection

- choose the lowest-complexity design that passes all hard gates and best meets the quality targets

---

## 11. Expected Outputs

At the end of the experiment, produce:

1. A candidate comparison table with one row per design.
2. A failure log showing why rejected designs failed.
3. A final recommendation naming:
   - chosen leaf strategy
   - chosen hierarchy strategy
   - chosen summary method
   - required beam width
   - expected memory/performance envelope
4. A gap analysis listing any requirements that no tested design could satisfy simultaneously.

---

## 12. Likely Best First Bets

If we want a focused starting point instead of exploring the full matrix immediately, begin with these four:

1. **Balanced k-means leaves + top-down leaf-summary hierarchy + centroid summaries**
2. **Projection-sort chunked leaves + greedy centroid grouping hierarchy + centroid summaries**
3. **Hybrid coarse partition + exact-size local rebalance leaves + top-down hierarchy + centroid+radius summaries**
4. **Random-shuffle exact chunking baseline**

These four should quickly show whether the requirements are achievable with:

- a geometry-first design
- a systems-simple deterministic design
- a compromise design

## 13. Key Risk to Watch Early

The hardest combination in the requirements is likely:

- **exact leaf size**
- **high locality**
- **high greedy-routing recall at beam width 1**
- **strict determinism**

The plan should therefore prioritize experiments that expose this tradeoff as early as possible, especially at the leaf-formation stage.
