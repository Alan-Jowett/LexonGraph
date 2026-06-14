<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->

# Black‑Box Requirements Specification for Hierarchical Vector Indexing

This document defines the externally observable properties and behavioral contracts that any valid system solution must satisfy. It completely decouples **what** the system must guarantee from **how** the internal algorithms achieve it.

The requirements are divided into two distinct tiers, followed by a supplemental operational contract section:

1. **Hard Invariants:** Binary, non-negotiable structural and behavioral constraints. Failure to meet any of these constitutes an invalid system.
2. **Quality Metrics & Objectives:** Measurable performance and geometric goals evaluated against thresholds on a defined benchmark dataset.
3. **Operational Contracts:** Resource, metric, dimensionality, and failure-handling requirements that must be fixed and verifiable before evaluation.

---

## Part I: Hard Invariants

These properties are absolute. Any implementation must satisfy them 100% of the time, across all supported datasets and declared operating conditions, or the build must fail.

### 1. Deterministic Behavior

#### Requirement

Given the exact same input dataset, configuration parameters, executable build, floating-point execution profile, and supported hardware class, the system must always produce bitwise-identical output hierarchies, bucket assignments, and search routing behavior.

#### Acceptance Criteria

- **Zero Randomness:** Algorithms must use fixed seeds or completely deterministic variants.
- **Platform-Fixed Determinism:** Bitwise identity is required only within the same executable build, floating-point execution profile, and supported hardware class.
- **Canonical Floating-Point Profile:** The implementation must define and enforce a canonical floating-point execution profile, including rounding mode, contraction behavior for fused multiply-add, and deterministic reduction ordering. Mixed execution modes that can change results across runs are prohibited.
- **Concurrency Isolation:** Output cannot vary based on thread scheduling, race conditions, async execution order, or non-deterministic work stealing.
- **Pure Function:** The hierarchy build process is a pure function: $f(\text{Dataset}, \text{Config}, \text{Binary}, \text{FPProfile}, \text{HardwareClass}) \to \text{Hierarchy}$.

### 2. Fixed Leaf Capacity and Dataset Alignment

#### Requirement

Every leaf bucket must contain exactly $L$ vectors (where $L$ is the configured leaf size, e.g., $L = 64$).

#### Dataset Constraints & Handling

To guarantee this structural invariant on arbitrary dataset sizes ($N$), the system must enforce one of the following configuration-driven protocols:

- **Strict Alignment:** The input dataset size $N$ must be an integer multiple of $L$ ($N \equiv 0 \pmod L$). If it is not, the build must fail immediately with an alignment error.
- **Deterministic Synthetic Padding:** If $N \pmod L \neq 0$, the system must add a pre-defined, deterministic set of synthetic padding entities prior to building the tree to bring the total count to the next multiple of $L$. Synthetic padding entities must be uniquely tagged, must not duplicate any real input vector, and must be excluded from externally visible search results and benchmark metrics.

#### Invariant Triggers

- No underfilled, overfilled, or variable-sized leaf buckets are permitted.
- No dynamic "spillover" or overflow buckets are permitted.
- Replicating real dataset vectors to satisfy padding is prohibited.
- Synthetic padding entities must be concentrated into the minimum possible number of leaves permitted by the deterministic build procedure and must not be scattered across otherwise fully real leaves unless the deterministic partitioning algorithm assigns them to different subtrees prior to leaf formation.

### 3. Bounded Hierarchical Structure (Fanout & Depth)

#### Requirement

The system must produce a multi-level hierarchy where the branching factor (fanout) $f$ and the total tree depth $D$ are strictly bounded.

#### Acceptance Criteria

- **Configured Fanout Range:** Every internal node must have a number of children $f$ such that $f_{\min} \le f \le f_{\max}$ (where $1 < f_{\min} \le f_{\max}$).
- **Root Edge Case:** If the total number of leaves is less than $f_{\min}$, the root node may have fanout equal to the total number of leaves, provided that number is at least $2$.
- **Degenerate Chain Prohibition:** Single-child internal nodes ($f = 1$) are strictly prohibited.
- **Strict Depth Bound:** The total tree depth $D$ (where the root is at depth $0$ and leaves are at depth $D$) must not exceed the depth implied by a balanced tree using the minimum allowed fanout, where $N_{\text{indexed}}$ is the total number of indexed entities (real vectors plus any synthetic padding):

$$
D \le \left\lceil \log_{f_{\min}} \left(\frac{N_{\text{indexed}}}{L}\right) \right\rceil
$$

- **Maximum-Only Bound:** This is a maximum depth constraint only. Trees that are shallower than this bound are permitted.

### 4. Complete Coverage

#### Requirement

The system must guarantee a strict bijective mapping between real input vector entities and the leaf layer. If deterministic synthetic padding is enabled, each synthetic padding entity must also map to exactly one leaf bucket while remaining externally identifiable as synthetic.

#### Acceptance Criteria

Let $R$ be the set of real input vectors, let $P_{\text{syn}}$ be the optional set of synthetic padding entities, and let $B_i$ denote the contents of leaf bucket $i$.

$$
\bigcup_{i=1}^{M} B_i = R \cup P_{\text{syn}} \quad \text{and} \quad B_i \cap B_j = \emptyset \quad \forall i \neq j
$$

*(Where $B_i$ represents a leaf bucket, and $M$ is the total number of leaves).*

- Every real input vector must appear in **exactly one** leaf bucket. There must be no dropped real vectors and no duplicate real vectors.
- Every synthetic padding entity, if present, must appear in **exactly one** leaf bucket.
- Synthetic padding entities must never be returned as search results and must be excluded from recall, locality, and compression benchmark calculations.

### 5. Immutable Structure and Lifecycle

#### Requirement

The produced hierarchy is a write-once, read-many static artifact.

#### Acceptance Criteria

- Once the build phase terminates, the tree structure, bucket assignments, and parent node summaries are entirely frozen.
- The structure must be fully serializable to a binary stream. When reloaded into memory, it must be instantly operational and reproducible bit-for-bit within the same executable build, floating-point execution profile, and supported hardware class, requiring zero dynamic recomputation or structural adjustment.

---

## Part II: Quality Metrics & Objectives

These requirements recognize the natural trade-offs between strict structural constraints (like exact leaf sizes) and geometric reality. They are evaluated against a standardized benchmark dataset utilizing a fixed specified distance metric $M$ (e.g., Euclidean or Cosine) and a declared reference hardware profile where applicable.

### 6. Search Quality Guarantee

#### Requirement

The structural layout of the hierarchy must natively support highly accurate routing.

#### Acceptance Criteria

- **Routing Procedure Definition:** The standard greedy routing search procedure must use beam width $= 1$ unless a different value is explicitly declared as part of the benchmark contract.
- **Recall Benchmark:** When evaluated using the system’s standard greedy routing search procedure, the hierarchy must achieve:

$$
\text{True Nearest Neighbor (TNN) Recall} \ge 90\% \text{ at } k = 10
$$

- **Evaluation Protocol:** This metric must hold true when averaged over a benchmark of $Q$ distinct out-of-sample query vectors using metric $M$ on a pre-declared benchmark dataset class or named benchmark corpus compatible with the declared routing procedure.
- **Fallback Routing Option:** If the implementation cannot satisfy the recall threshold with beam width $= 1$ on the declared benchmark, it may declare a larger fixed beam width, but the declared latency and throughput contracts in Requirement #11 must then be measured using that same beam width.

### 7. Locality Preservation

#### Requirement

The hierarchy must approximate the local manifold topology of the input space by ensuring close neighbors stay in close structural proximity.

#### Acceptance Criteria

- Let $V$ be an arbitrary real vector in the dataset, and let $N_k(V)$ be its true top-$k$ nearest real neighbors in the entire dataset according to metric $M$.
- **Neighborhood Coherence:** When evaluated at a small neighborhood scale ($k = 10$), at least $80\%$ of the vectors in $N_k(V)$ must reside either within the same leaf bucket as $V$, or within immediate sibling leaf buckets sharing a direct parent node.
- **Padding-Neutral Evaluation:** Locality must be evaluated over post-build physical leaves after excluding all synthetic padding entities from both the candidate neighbor sets and the occupancy counts used for the metric. Synthetic padding entities must not count toward neighborhood membership or denominator calculations.

### 8. Compression‑Friendly Leaf Buckets

#### Requirement

Leaf buckets must group vectors tightly enough to ensure high data coherence, making them highly receptive to local compression schemes (e.g., Product Quantization, Delta Encoding, or PCA).

#### Acceptance Criteria

- Instead of volatile global variance comparisons, the system must achieve a target local quantization or compression efficiency.
- **Target Metric:** When a standard quantization scheme (e.g., 8-bit scalar quantization) is applied locally to each leaf bucket individually, the average reconstruction error across all buckets must be at least $20\%$ lower than if the same quantization scheme were applied globally across the unindexed real dataset.

### 9. Mathematically Grounded Parent Summaries

#### Requirement

Each internal node must store a fixed-size mathematical summary representing its underlying children that supports routing without introducing global system instability.

#### Acceptance Criteria

- **Build-Time Computation Scope:** The summary of an internal node $I$ may be computed either from its direct children’s stored summaries or by a one-time bottom-up pass over descendant leaf data during the build phase. After the build phase terminates, query-time routing may consult only the stored summaries.
- **Approximation Bound:** If parent summaries are composed from child summaries rather than recomputed from descendant leaf data, the resulting parent summary must deviate by no more than $1\%$ relative $L_2$ error from the exact leaf-derived parent summary, where relative $L_2$ error is defined as $\frac{\lVert S_{\text{approx}} - S_{\text{exact}} \rVert_2}{\max(\lVert S_{\text{exact}} \rVert_2, \delta)}$ for a fixed declared numerical floor $\delta > 0$, and $\delta$ must be no greater than $10^{-6}$ times the mean $L_2$ norm of all exact leaf-derived summaries in the built hierarchy.
- **Operational Stability:** The summary function must be operationally stable. Bounded perturbations or numerical noise introduced to child summaries must result in bounded, predictable changes to the parent summary, preventing chaotic routing failures from minor data changes.

### 10. Quantifiable Monotonic Refinement

#### Requirement

As a path is traversed down the hierarchy from the root to a leaf node, the data space represented by sub-trees must shrink at a minimum quantifiable rate.

#### Acceptance Criteria

- Let $P$ be a parent node and $C$ be any direct child of $P$.
- Let $\text{Var}(N)$ denote the total variance of all real vectors contained within the sub-tree rooted at node $N$.
- Rather than allowing trivial decreases, the hierarchy must satisfy a strict refinement decay rate controlled by a coefficient $\beta$:

$$
\text{Var}(C) \le \beta \cdot \text{Var}(P) \quad \text{where } \beta \le 0.85
$$

- **Exception Handling:** The refinement constraint may flatten to $\beta = 1.0$ only at the penultimate internal layer, only when all children of $P$ are leaves, and only if $\text{Var}(P) \le \epsilon$ where $\epsilon = 0.01 \cdot \text{Var}(\text{Root})$.
- **Metric Scope:** Requirement #10 applies only when the declared metric is Euclidean distance or another declared metric with an explicitly defined compatible variance functional. If a non-Euclidean metric is used, the implementation must declare the exact metric-compatible dispersion functional substituted for $\text{Var}$ and use it consistently throughout this requirement.

---

## Part III: Operational Contracts

These requirements define the resource, dimensionality, metric, and failure-handling conditions under which the preceding invariants and quality objectives are evaluated.

### 11. Build and Query Performance Service Levels

#### Requirement

The system must declare fixed build and query performance thresholds for the benchmark and must satisfy them on a named reference hardware profile.

#### Acceptance Criteria

- **Reference Hardware Profile:** The benchmark must name a fixed reference hardware profile $H_{\text{ref}}$ before evaluation.
- **Build Throughput Contract:** The implementation must declare a minimum build throughput $R_{\text{build,min}}$ in vectors per second on $H_{\text{ref}}$ and meet or exceed it.
- **Query Latency Contract:** The implementation must declare a maximum p95 query latency $T_{\text{query,p95,max}}$ for search using the declared beam width at $k = 10$ on $H_{\text{ref}}$ and must not exceed it.
- **Query Throughput Contract:** The implementation must declare a minimum sustained query throughput $QPS_{\min}$ on $H_{\text{ref}}$ and meet or exceed it.
- **Pre-Declaration Rule:** $R_{\text{build,min}}$, $T_{\text{query,p95,max}}$, and $QPS_{\min}$ must be fixed before evaluation and cannot be changed after benchmark results are observed.
- **Scaling Verification:** The declared performance contracts must be verified on at least three benchmark dataset sizes spanning a factor-of-4 range in $N$, all within the declared supported dimensionality range. In addition, the implementation must declare asymptotic build complexity and per-query routing complexity as functions of $N$ and $d$.
- **Realistic Qualification Corpus:** Any benchmark suite used to qualify the system for realistic archival embeddings must include at least one corpus family whose real-vector count is in the tens-of-thousands, whose dimensionality lies within the realistic embedding range, and whose real-vector count is not an exact multiple of the configured leaf size $L$ so that alignment-policy behavior is exercised under realistic conditions rather than toy multiples.
- **Bounded-Time Qualification Rule:** The benchmark contract must declare a maximum wall-clock build time or equivalent timeout budget for each evaluated dataset size on $H_{\text{ref}}$. An implementation that does not complete within the declared budget is disqualified for that configuration and must be reported as a deterministic timeout outcome rather than being silently omitted.

### 12. Memory Budget

#### Requirement

The system must operate within a declared and enforceable memory budget during both build and query execution.

#### Acceptance Criteria

- **Measurement Definition:** Peak resident memory must be measured as process working set on the reference operating system used for the benchmark, or another explicitly named OS-specific resident-memory metric fixed before evaluation.
- **Build Memory Contract:** Peak resident memory during build must not exceed a pre-declared limit $B_{\text{build,max}}$.
- **Query Memory Contract:** Peak resident memory of the fully loaded read-only index during query execution must not exceed a pre-declared limit $B_{\text{query,max}}$.
- **Pre-Declaration Rule:** $B_{\text{build,max}}$ and $B_{\text{query,max}}$ must be fixed before evaluation and cannot be changed after benchmark results are observed.

### 13. Supported Dimensionality Range

#### Requirement

The system must declare the dimensionality range for which all guarantees in this specification are valid.

#### Acceptance Criteria

- **Declared Range:** The implementation must declare fixed supported bounds $d_{\min}$ and $d_{\max}$ such that all indexed vectors satisfy $d_{\min} \le d \le d_{\max}$.
- **Uniform Dimensionality:** All vectors within a single dataset must share the same dimensionality $d$.
- **Within-Range Guarantee:** All requirements in this specification must hold for any dataset whose vectors lie within the declared dimensionality range.
- **Out-of-Range Rejection:** Datasets outside the declared range must be rejected deterministically before the build begins.
- **Realistic Qualification Coverage:** Any benchmark suite used to claim realistic-corpus support must include at least one qualification corpus whose dimensionality lies within the realistic embedding band of $384 \le d \le 4096$ and must not rely exclusively on toy low-dimensional corpora.

### 14. Distance Metric Contract

#### Requirement

The system must define the exact distance or similarity contract under which the hierarchy is built, summarized, routed, and evaluated.

#### Acceptance Criteria

- **Metric Consistency:** The same declared metric $M$ must be used consistently for build-time partitioning, parent summary construction, routing, and benchmark evaluation unless the implementation explicitly declares and justifies a different but equivalent transformed metric.
- **Equivalence Definition:** An equivalent transformed metric must preserve the total ordering of all pairwise distances relevant to routing and evaluation; that is, for any vectors $x$, $y$, and $z$, if $d_M(x,y) < d_M(x,z)$ then the transformed metric must preserve that ordering.
- **Routing Validity Under Transformation:** Preserving pairwise ordering is not by itself sufficient to establish routing equivalence. Any implementation using a transformed metric must still satisfy Requirements #6 through #10 under that transformed routing procedure on the declared benchmark.
- **Assumption Disclosure:** Any mathematical assumptions required by the routing or summary scheme (e.g., symmetry, triangle inequality, or compatibility with averaging-based summaries) must be declared explicitly.
- **Invalid Metric Rejection:** A metric that violates the declared assumptions is invalid for that routing or summary scheme unless the implementation provides an alternative scheme and separately satisfies Requirements #6 through #10 under that metric.

### 15. Failure Atomicity and Error Reporting

#### Requirement

The build process must fail atomically and report errors deterministically.

#### Acceptance Criteria

- **Atomic Failure:** If the build fails at any point, no partially constructed hierarchy may be exposed as a valid, queryable artifact.
- **Deterministic Error Contract:** Every failure mode must emit a deterministic structured error containing a machine-readable code and a human-readable message identifying the violated precondition, invariant, or operational contract.
- **Artifact Hygiene:** Temporary files or partially written output produced during a failed build must either be removed or marked unambiguously unusable so that they cannot be mistaken for a valid index.
- **Durable Success Criterion:** A build may be marked successful only after all persisted index data and required metadata have been flushed durably according to the storage contract of the target platform, so that a crash cannot leave a torn artifact incorrectly marked as valid.

---

## Requirements Traceability Matrix (Summary)

| # | Requirement Name | Classification | Primary Metric / Verification Method |
| --- | --- | --- | --- |
| **1** | Deterministic Behavior | **Hard Invariant** | Bitwise comparison of repeated identical builds within the same binary, FP profile, and hardware class. |
| **2** | Fixed Leaf Capacity | **Hard Invariant** | Cardinality check on all leaves $\equiv L$; strict alignment or tagged concentrated synthetic padding. |
| **3** | Bounded Structure | **Hard Invariant** | Tree traversal; verifying fanout bounds, root exception, and maximum depth formula. |
| **4** | Complete Coverage | **Hard Invariant** | Set operations: Union of leaves $\equiv R \cup P_{\text{syn}}$; intersections $\equiv \emptyset$; padding excluded from results. |
| **5** | Immutable Structure | **Hard Invariant** | Read-only memory enforcement; serialization round-trip verification. |
| **6** | Search Quality | Quality Objective | $\ge 90\%$ TNN@10 using declared greedy routing procedure on the named benchmark dataset class, with fixed beam width. |
| **7** | Locality Preservation | Quality Objective | $\ge 80\%$ of top-10 real neighbors found within same or sibling physical leaves after excluding synthetic padding from the metric. |
| **8** | Compression-Friendly | Quality Objective | $\ge 20\%$ local quantization error reduction vs. global baseline. |
| **9** | Stable Parent Summaries | Quality Objective | Exact or bounded-error parent summary composition using constrained relative $L_2$ error with perturbation stability testing. |
| **10** | Monotonic Refinement | Quality Objective | Metric-compatible dispersion reduction step-down check ($\le 0.85$) with $\epsilon$-gated exception at the penultimate layer only. |
| **11** | Performance Service Levels | Operational Contract | Verification of declared build throughput, p95 latency at the declared beam width, sustained throughput, and scaling across multiple dataset sizes on $H_{\text{ref}}$. |
| **12** | Memory Budget | Operational Contract | OS-defined peak resident memory measurement against declared build and query limits. |
| **13** | Supported Dimensionality | Operational Contract | Range validation for uniform-dimensional datasets with $d_{\min} \le d \le d_{\max}$; deterministic rejection outside range. |
| **14** | Distance Metric Contract | Operational Contract | Verification that declared metric assumptions match routing and summary behavior and that any transformed metric preserves pairwise ordering while independently meeting routing-quality requirements. |
| **15** | Failure Atomicity | Operational Contract | Fault-injection and partial-failure tests confirming deterministic errors, durable success marking, and no usable partial index. |
