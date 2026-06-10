<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Adaptive Planning Policy Crate Design

## Status

Draft design specification for a Rust crate that composes streaming directional
PCA and streaming DCBC into one deterministic adaptive built-in planning
realization for the LexonGraph streaming indexer.

## Design Goals

The crate design is intended to be:

- deterministic at the observable boundary
- minimal in how much new algorithm surface it introduces
- explicit about switch criteria and direction continuity
- compatible with the indexer's existing finalized partition hierarchy
- respectful of existing crate ownership boundaries

## Crate Boundary

The crate owns:

- adaptive planning-policy configuration
- deterministic switch-decision logic
- structured adaptive diagnostics and switch records
- composition of the existing directional-PCA and DCBC realizations for indexer
  built-in planning

The crate does not own:

- the shared streaming clustering trait definitions
- the owned mechanics of the directional-PCA crate
- the owned mechanics of the DCBC crate
- final block materialization or block-tree construction

## Design Entries

### DSG-ADAPTIVE-POLICY-001 `Composite normative boundary`

The crate depends on the streaming indexer specification for finalized
partition-hierarchy semantics and on the directional-PCA and streaming DCBC
specification packages for their owned algorithm realizations.

The crate does not redefine those sources or the shared streaming clustering
contract.

### DSG-ADAPTIVE-POLICY-002 `Adaptive built-in realization boundary`

The crate exposes one aggregate planning realization or factory intended for the
streaming indexer's built-in planning-selection surface.

The caller chooses the adaptive realization up front; the crate then owns the
algorithm-switch logic internally rather than requiring a caller-driven
per-layer conversation.

### DSG-ADAPTIVE-POLICY-003 `Explicit adaptive configuration`

The adaptive configuration contains:

- one selected built-in direction
- one directional-PCA configuration block
- one DCBC configuration block
- one tunable `pc1_explained_variance_ratio_threshold`
- one tunable `dcbc_max_embedding_count`

### DSG-ADAPTIVE-POLICY-004 `Directional-PCA initial mode`

Each adaptive planning flow starts in directional-PCA mode.

The adaptive realization does not begin in DCBC mode and does not require the
caller to decide the first algorithm dynamically after planning has started.

### DSG-ADAPTIVE-POLICY-005 `Deterministic collapse diagnostics`

At deterministic planning boundaries, the adaptive realization derives
structured collapse diagnostics from the represented planning inputs or outputs
available at that boundary.

For divisive adaptive planning, those diagnostics include the explained
variance ratio of the first principal component and the represented embedding
count observed for the current collection.

For agglomerative adaptive planning, the existing count-based diagnostic inputs
remain unchanged in this experiment.

### DSG-ADAPTIVE-POLICY-006 `Deterministic switch execution`

For divisive adaptive planning, the adaptive realization selects DCBC for a
collection when:

1. the collection's first-principal-component explained variance ratio is less
   than `pc1_explained_variance_ratio_threshold`, and
2. the collection's represented embedding count is less than
   `dcbc_max_embedding_count`

If the explained variance ratio is greater than or equal to the threshold, the
divisive realization selects directional PCA for that collection. If the
explained variance ratio is below threshold but the embedding count is at or
above `dcbc_max_embedding_count`, the divisive realization also selects
directional PCA for that collection.

Agglomerative adaptive planning retains the existing one-way count-based
switching behavior in this experiment.

### DSG-ADAPTIVE-POLICY-007 `Direction continuity`

The selected built-in direction remains stable across the full adaptive
planning flow:

- in `Divisive` mode, each evaluated collection independently selects the
  clustering realization used for top-down refinement without changing the
  top-down direction
- in `Agglomerative` mode, the switch changes the clustering realization used
  for bottom-up grouping without changing the bottom-up direction

### DSG-ADAPTIVE-POLICY-008 `One-way switch rule`

Divisive adaptive planning does not impose one-way ownership across a full
planning flow: each collection is evaluated independently and may choose a
different realization from adjacent collections.

Agglomerative adaptive planning continues to mark the flow as DCBC-owned after
its first switch and therefore skips any attempt to switch back to directional
PCA later in that same flow.

### DSG-ADAPTIVE-POLICY-009 `Structured diagnostics and switch records`

For each evaluated boundary, the crate retains a structured record identifying:

- the active algorithm realization
- the deterministic zero-based adaptive boundary position for that evaluated
  planning boundary
- for divisive adaptive planning, the deterministic inputs to the collection
  decision, including `pc1_explained_variance_ratio`,
  `pc1_explained_variance_ratio_threshold`, `embedding_count`, and
  `dcbc_max_embedding_count` when diagnostics exist
- the explicit decision reason for that boundary or collection
- whether the switch criteria were satisfied
- for agglomerative adaptive planning, whether the first switch boundary
  occurred at that boundary

If surfaced publicly, these diagnostics remain deterministic and suitable for
validation without requiring parsing of free-form messages or inference from
record ordering alone. Boundaries that have not yet computed diagnostics expose
those numeric values as explicitly unavailable rather than synthesized.

### DSG-ADAPTIVE-POLICY-010 `Hierarchy normalization compatibility`

Regardless of whether a given planning segment is realized by directional PCA
or DCBC, the adaptive crate normalizes the resulting planning output into the
same finalized partition-hierarchy abstraction expected by the streaming
indexer.

The adaptive crate therefore does not require a different final materialization
contract downstream.

### DSG-ADAPTIVE-POLICY-011 `Explicit failure behavior`

Invalid configuration, contradictory switch rules, unsupported direction
combinations, or inability to make a deterministic switch decision fail
explicitly.

The crate does not silently reinterpret thresholds, choose a different
direction, or fall back to caller interaction.

### DSG-ADAPTIVE-POLICY-012 `Verification realization`

Repository verification artifacts cover construction, no-switch behavior,
switch-trigger behavior, deterministic switch-boundary reproduction, both
direction modes, and hierarchy-compatibility behavior for the adaptive crate.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-ADAPTIVE-POLICY-001 | REQ-ADAPTIVE-POLICY-002 |
| DSG-ADAPTIVE-POLICY-002 | REQ-ADAPTIVE-POLICY-001, REQ-ADAPTIVE-POLICY-003 |
| DSG-ADAPTIVE-POLICY-003 | REQ-ADAPTIVE-POLICY-004 |
| DSG-ADAPTIVE-POLICY-004 | REQ-ADAPTIVE-POLICY-005 |
| DSG-ADAPTIVE-POLICY-005..006 | REQ-ADAPTIVE-POLICY-007, REQ-ADAPTIVE-POLICY-008, REQ-ADAPTIVE-POLICY-010 |
| DSG-ADAPTIVE-POLICY-007 | REQ-ADAPTIVE-POLICY-006 |
| DSG-ADAPTIVE-POLICY-008 | REQ-ADAPTIVE-POLICY-009 |
| DSG-ADAPTIVE-POLICY-009 | REQ-ADAPTIVE-POLICY-012 |
| DSG-ADAPTIVE-POLICY-010 | REQ-ADAPTIVE-POLICY-011 |
| DSG-ADAPTIVE-POLICY-011 | REQ-ADAPTIVE-POLICY-013 |
| DSG-ADAPTIVE-POLICY-012 | REQ-ADAPTIVE-POLICY-014 |
