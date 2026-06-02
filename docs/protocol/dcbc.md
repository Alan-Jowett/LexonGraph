# Deterministic Capacity-Constrained Balanced Clustering (DCBC) Protocol

## Status

This document is the canonical protocol for deterministic
capacity-constrained balanced clustering (DCBC).

It defines the required inputs, outputs, invariants, and execution rules for a
conforming DCBC run.

## Goals

The DCBC protocol is designed to be:

- deterministic at the protocol boundary
- explicit about capacity-constrained clustering behavior
- explicit about tie-breaking and numerical ordering
- stable across repeated runs on identical inputs
- permissive about physical execution strategy when the logical result is
  preserved

## Scope

This protocol defines the required externally visible behavior of a DCBC run.

It specifies:

- input validity conditions
- deterministic centroid initialization
- capacity-constrained assignment behavior
- centroid update rules
- output semantics
- failure conditions

This revision defines a single-threaded logical model. An implementation may
execute work physically in parallel only if it preserves the same externally
visible result required by this protocol.

This protocol does not define a host application's storage format, API shape,
or downstream use of the produced clustering result.

## Inputs

A conforming DCBC invocation requires:

- an ordered array `X` of input vectors
- a cluster count `k`
- a minimum cluster size `L`
- a maximum cluster size `N`
- an iteration count `T`

### Input Vectors

`X` is an ordered array of vectors. Its length is `n`.

Input order is normative for this protocol. Several deterministic choices,
including initialization and tie-breaking, depend on ascending input index.

All input vectors must:

- have the same dimensionality
- contain finite numeric values
- have non-zero norm under the protocol's Euclidean norm

Inputs containing mixed dimensionality, `NaN`, infinite values, or zero-norm
vectors are invalid and must fail explicitly.

### Clustering Parameters

`k`, `L`, `N`, and `T` are integer inputs.

This revision requires `T >= 1`. A conforming invocation must also satisfy:

- `k >= 1`
- `L >= 1`
- `L <= N`
- `k * L <= n`
- `n <= k * N`

If those conditions do not hold, the invocation is invalid and must fail
explicitly.

## Distance and Objective Semantics

This revision uses cosine distance with normalized centroids for assignment and
objective evaluation.

For this protocol:

- `dot(x, y)` is the standard Euclidean dot product over the shared input
  dimensionality
- `||v||` is the Euclidean norm `sqrt(sum_m v_m^2)`
- `normalize(v)` is `v / ||v||`
- `cosine_distance(x, y)` is `1 - dot(x, y) / (||x|| * ||y||)`

The objective is:

`J = sum_i cosine_distance(X[i], normalized(C[A[i]]))`

where:

- `A[i]` is the assigned cluster index for point `i`
- `C[j]` is the raw stored centroid for cluster `j`
- `normalized(C[j])` means the normalized centroid representation used for
  distance computations for cluster `j`, derived from the raw centroid `C[j]`
  and, when `||C[j]|| < epsilon`, from the corresponding materialized
  membership set `S[j]` according to the zero-norm handling rules in this
  protocol

## Required Invariants

The following invariants are normative for every materialized assignment state,
including the final output:

1. The assignment vector `A` has length `n`.
2. Each point is assigned to exactly one cluster.
3. Each cluster size satisfies `L <= |S[j]| <= N`.
4. The clustering result is deterministic for identical inputs.
5. Numerical reductions that affect the result use deterministic ordering.

These invariants constrain the externally visible result of the protocol. They
do not require a particular internal implementation technique beyond preserving
the required behavior.

## Determinism Requirements

### Numerical Semantics

A conforming implementation must:

- use IEEE 754 double precision
- use deterministic operation ordering
- avoid nondeterministic reductions in any result-affecting computation
- perform ordered summation in strictly increasing point-index order where this
  protocol requires summation

This revision uses:

- `epsilon = 1e-12`

### Comparison Rules

Distance comparisons must treat values as equal when:

`abs(d1 - d2) < epsilon`

### Tie-Breaking Rules

This protocol uses context-specific deterministic tie-breaking rules:

1. When choosing among candidate clusters for a fixed point, prefer the smaller
   cluster index.
2. When choosing among candidate points for a fixed cluster or initialization
   step, prefer the smaller point index.
3. When a procedure depends on a canonical ordering over point-cluster pairs,
   use ascending point index and then ascending cluster index.

## Initialization Procedure

A conforming DCBC run performs the following initialization:

1. Set `C[0] = X[0]`.
2. For each `j` from `1` through `k - 1`, compute for every candidate point
   `X[i]` the minimum distance from `X[i]` to the already selected centroids
   `C[0..j-1]`.
3. Select the point maximizing that minimum distance.
4. Break ties by smaller point index.
5. Set `C[j]` to the selected point.

No alternative initialization strategy is conforming in this revision.

## Iterative Optimization Procedure

A conforming DCBC run performs exactly `T` iterations. Each iteration consists
of an assignment phase followed by a centroid update phase.

There is no early stopping and no convergence-based termination in this
revision.

### Assignment Phase

The assignment phase must solve the constrained assignment problem:

- each point is assigned exactly once
- each cluster size remains within `[L, N]`
- total assignment cost is minimized under the protocol's distance semantics

This revision requires assignment to be realized as minimum-cost flow with lower
and upper bounds over the logical graph:

- source -> points
- points -> clusters
- clusters -> sink

Normative graph semantics:

- source-to-point capacity is `1`
- point-to-cluster capacity is `1`
- point-to-cluster edge cost is the cosine distance from `X[i]` to the
  normalized form of centroid `C[j]`
- cluster-to-sink lower bound is `L`
- cluster-to-sink upper bound is `N`

### Deterministic Assignment Selection

If multiple optimal assignments exist, the implementation must choose the
lexicographically minimal assignment vector when comparing the tuple
`(A[0], A[1], ..., A[n-1])` in ascending point-index order.

To preserve that result, point-to-cluster edges must be generated in this
canonical order:

1. ascending point index
2. ascending cluster index

Any queue traversal, augmenting-path selection, or equivalent solver behavior
that can affect the chosen optimum must also be deterministic.

### Cluster Materialization

After assignment, the implementation must materialize cluster memberships so
that:

- `S[j] = { i | A[i] = j }`

The implementation must then validate that:

- each point is assigned exactly once
- every cluster size satisfies `L <= |S[j]| <= N`

## Centroid Update Procedure

For each cluster `j`, compute the raw centroid:

`C[j] = (1 / |S[j]|) * sum_{i in S[j]} X[i]`

The summation order must be ascending point index.

### Zero-Norm Centroid Handling

If the norm of the raw centroid for cluster `j` is smaller than `epsilon`, the
normalized centroid used for distance computations must be derived from the
smallest-index point in the same materialized membership set `S[j]` that was
used to compute that raw centroid.

Normatively:

1. Select `i*`, the smallest point index in `S[j]`.
2. Use `normalize(X[i*])` as the normalized centroid for distance computations.
3. Preserve the raw stored centroid `C[j]` unchanged.

If the raw centroid norm is at least `epsilon`, the normalized centroid used for
distance computations is `normalize(C[j])`.

## Required Outputs

A conforming DCBC run produces:

- an assignment vector `A` of length `n`
- a centroid array `C` of length `k`
- metadata containing:
  - `iteration_count = T`
  - `cluster_sizes`
  - `objective_value = J`

### Output Semantics

The reported objective value must:

- use the protocol's distance semantics
- be computed after the final iteration
- use full double precision without protocol-level rounding

The protocol guarantees reproducibility for identical ordered inputs:

`run(X, k, L, N, T) == run(X, k, L, N, T)`

This revision does not guarantee invariance under reordering of the input array.

## Failure Conditions

A conforming implementation must fail explicitly if:

- the capacity constraints are infeasible
- `k < 1`
- `L < 1`
- `L > N`
- `T < 1`
- any input value is `NaN` or infinite
- any input vector has zero norm
- the solver cannot produce a feasible assignment
- any invalid numeric state required for the protocol result is encountered

An implementation must not silently continue after such failures.

## Explicit Non-Goals

This protocol intentionally does not define:

- approximate assignment methods
- stochastic initialization
- relaxed capacity enforcement
- convergence-based early stopping
- host-application storage or transport formats

Implementations using those behaviors must not claim conformance with this
revision of the DCBC protocol.

## Validation Checklist

The following validation cases define the minimum conformance surface for this
revision:

1. Invalid capacity bounds fail explicitly when `k * L > n` or `n > k * N`.
2. Inputs with `k < 1` fail explicitly.
3. Inputs with `L < 1` fail explicitly.
4. Inputs with `L > N` fail explicitly.
5. Inputs with `T < 1` fail explicitly.
6. Inputs containing mixed vector dimensionality fail explicitly.
7. Inputs containing `NaN` or infinite values fail explicitly.
8. Inputs containing zero-norm vectors fail explicitly.
9. Cosine distance, normalization, dot products, and norms follow the protocol's
   Euclidean definitions.
10. Initialization always chooses `X[0]` as the first centroid.
11. Later initialization choices follow farthest-point selection with smaller
   point index as the tie-break.
12. Assignment produces exactly one cluster assignment per point.
13. Every cluster size after assignment satisfies `L <= |S[j]| <= N`.
14. Assignment selection is deterministic when multiple optimal solutions exist.
15. Assignment edge generation follows ascending point index, then ascending
    cluster index.
16. Point-to-cluster edges have unit capacity.
17. Centroid summation uses ascending point-index order.
18. Zero-norm raw centroids use the smallest-index cluster member from the same
    materialized membership set used to compute the raw centroid for normalized
    distance computations while preserving the raw stored centroid.
19. The protocol runs exactly `T` iterations with no early stopping.
20. The reported objective value is computed after the final iteration under the
    protocol's distance semantics.
21. Repeated runs on identical ordered inputs produce identical outputs.
