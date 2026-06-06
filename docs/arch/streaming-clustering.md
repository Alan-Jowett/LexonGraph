<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# **Streaming Clustering Requirements Specification (Updated Revision)**

## 1. Purpose

Define a standard interface for streaming, multi‑pass clustering over datasets larger than RAM. The caller provides:

- The desired number of clusters K  
- Optional balance constraints  
- A convergence policy based on fitness metrics  

The algorithm performs multiple passes over the dataset, maintains internal state, and produces a classifier mapping embeddings → cluster IDs in \0, K).

This specification defines deterministic behavior, memory bounds, balance enforcement, and non‑conformance conditions for large‑scale semantic systems such as **[LexonGraph**.

---

## 2. Inputs and Configuration

### 2.1 Required Inputs

- **Number of clusters (K)**  
  - Must be a positive integer  
  - Must be treated as a hard requirement  
  - The algorithm must be capable of producing exactly K non‑empty clusters **once N ≥ K has been established**  
  - If K > N after the first pass (when N becomes known), the configuration is invalid and must be rejected  
  - The algorithm must not internally modify K or derive additional clusters  

### 2.2 Optional Inputs

- **Balance constraints**, such as:  
  - Maximum cluster size ratio  
  - Minimum cluster occupancy  
  - Hard capacity constraints  
  - Soft balance penalties  

The requirements do not mandate a specific balance policy, but the algorithm must enforce the caller‑provided constraints.

### 2.3 Dataset Size Definition

> **Dataset size N is defined as the total number of embeddings ingested across all batches in a pass. N becomes known only after `finish_pass()` of the first pass.**

Balance constraints depending on N become enforceable only after N is known.

### 2.4 Definition of a Cluster

> **A cluster is a partition element in a hard assignment of the dataset. Each embedding belongs to exactly one cluster.**

No probabilistic or overlapping assignments are permitted.

### 2.5 Definition of a Pass

> **A pass is defined as a complete traversal of the dataset, consisting of all batches streamed by the caller for that pass, followed by a call to `finish_pass()`.**

This ensures the streaming model is well‑defined.

---

## 3. Behavioral Guarantees

### 3.1 Cluster Count Guarantee

A conformant implementation must:

- Produce exactly K clusters once N ≥ K  
- Ensure no cluster is empty  
- Singleton clusters are permitted unless explicitly prohibited by caller‑provided balance constraints  
- Maintain stable cluster identifiers across passes  

Cluster identity stability is defined operationally:

> **If an implementation internally reorders clusters between passes, it must apply a deterministic matching procedure (e.g., centroid matching or assignment matching) with deterministic tie‑breaking rules to preserve cluster ID continuity.**

This requirement is fully deterministic and testable.

### 3.2 Balance Guarantee

If the caller provides balance constraints, the implementation must:

- Enforce them during training  
- Reject constraints proven unsatisfiable from known information  
- Validate cardinality‑based constraints after the first pass  
- Validate distribution‑based constraints during optimization  
- Report balance metrics reflecting violations  

If the caller does not provide explicit balance constraints:

- No balance guarantees are imposed beyond the cluster count guarantee  
- The algorithm may produce unbalanced clusters if that is the natural structure of the data  

### 3.3 Degenerate Data Behavior

If all embeddings are identical or otherwise indistinguishable:

> **The implementation must still produce K non‑empty clusters unless prohibited by balance constraints. The resulting partition need not be semantically meaningful.**

This explicitly adopts Option A: partitioning is required even for degenerate datasets.

---

## 4. Streaming Execution Model

### 4.1 Multi‑Pass Structure

The algorithm operates in passes:

1. Caller streams all data batches for the pass  
2. Algorithm updates internal state incrementally  
3. Caller signals end of pass  
4. Algorithm computes fitness metrics  
5. Caller decides whether to continue or stop  

The algorithm does not decide how many passes are required.

### 4.2 Memory Bound

> **Memory consumption must remain independent of dataset size N.**

This replaces the earlier O(KD + C) requirement with a simpler, more general constraint.

Implementations must not buffer the dataset or any unbounded fraction of it.

---

## 5. Fitness Metric Requirements

### 5.1 Purpose

Fitness metrics allow the caller to detect:

- Convergence  
- Instability  
- Degenerate cluster collapse  
- Balance violations  
- Centroid drift  
- Assignment instability  

### 5.2 Requirements

The fitness reporting interface must provide two metrics:

- **quality_metric** — numeric scalar, deterministic, comparable across passes  
- **balance_metric** — numeric scalar, deterministic, comparable across passes  

The balance metric must always be present; if no balance constraints are configured, it must be zero.

### 5.3 Metric Directionality

Each metric must define its direction of improvement:

- Either **larger is better**  
- Or **smaller is better**  

This direction must be documented and consistent across passes.

### 5.4 Comparison Scope

> **Metrics must be comparable across passes within a single training run. Cross‑run comparability is not required.**

### 5.5 Acceptable Quality Metrics

Examples include:

- SSE  
- Maximum centroid shift  
- Assignment stability score  
- Any deterministic scalar  

### 5.6 Acceptable Balance Metrics

- Degree of imbalance  
- Penalty for violating caller‑provided constraints  
- Zero if no constraints are configured  

---

## 6. Classifier Requirements

After the caller decides training is complete, the algorithm must produce a classifier that:

- Maps embeddings → cluster IDs in \0, K)  
- Is deterministic  
- Accepts arbitrary batch sizes  
- Does not require the original dataset  
- Reflects the final clustering state  
- Preserves cluster ID continuity  
- Is serializable into a deterministic byte representation  
- Rejects malformed embeddings (wrong dimensionality, NaNs)  
- Deterministically assigns any valid embedding to exactly one cluster  

---

## 7. Behavioral Invariants

### 7.1 Ingestion Invariants

- Batches may be of any size  
- Memory usage must remain independent of N  
- `finish_pass()` must be called exactly once per pass  
- No ingestion after `finish_pass()` until next pass begins  
- Input order is part of the input; implementations are not required to be order‑invariant  

### 7.2 Completion Invariants

- `into_classifier()` may only be called after caller decides training is complete  
- After producing the classifier, the clusterer cannot accept further passes  

### 7.3 Determinism

Given identical:

- Input batches  
- Input ordering  
- Pass boundaries  
- Configuration  
- Random seeds (if applicable)  

The algorithm must produce identical:

- Quality metrics  
- Balance metrics  
- Final classifier  
- Cluster assignments  

### 7.4 Randomness Policy

- Implementations may use randomness  
- They must expose a deterministic seed parameter  
- If no seed is provided, the implementation must behave deterministically  

---

## 8. State Machine Definition

A conformant implementation must follow this state machine:

```
Idle
  → Ingesting
Ingesting
  → PassComplete
PassComplete
  → Ingesting        (start next pass)
  → TrainingComplete (caller stops training)
TrainingComplete
  → ClassifierProduced
Any State
  → Error            (on invalid transition or unsatisfiable configuration)
```

The **Error** state is terminal.  
Implementations must provide deterministic error reporting semantics:

> **Errors must be reported via a deterministic, implementation‑defined error type that identifies the cause (invalid configuration, invalid transition, unsatisfiable constraint, or malformed input).**

Illegal transitions (e.g., ingesting after classifier production) must enter the Error state.

---

## 9. Non‑Conformance Examples

The following implementations do not satisfy the requirements:

- Producing K clusters but leaving K–1 empty  
- Producing K clusters but violating caller‑provided balance constraints  
- Using memory that scales with dataset size N  
- Producing non‑deterministic classifiers without a seed  
- Failing to maintain cluster ID continuity across passes  
- Failing to reject configurations where K > N after the first pass  

These are explicitly disallowed.

---

## 10. Summary

This revised specification ensures:

- K is a hard requirement once N ≥ K  
- Configurations where K > N are rejected  
- Balance is enforceable only when explicitly configured  
- Degenerate clusterings are handled consistently  
- Fitness metrics are separated into quality and balance components  
- Metric directionality is defined  
- Metric comparability is scoped to a single run  
- The caller controls pass count  
- The classifier is deterministic, serializable, and usable downstream  
- Memory usage is independent of dataset size N  
- Dataset size is well‑defined  
- Cluster identities remain stable via deterministic matching with deterministic tie‑breaking  
- Constraint failures are detected at deterministic times  
- The state machine supports multiple passes and includes an Error state  
- Error reporting semantics are explicit and deterministic  
- The partition need not be semantically meaningful for identical data  

