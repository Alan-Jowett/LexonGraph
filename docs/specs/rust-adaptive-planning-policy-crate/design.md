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
- one deterministic switch-policy configuration block

The switch-policy block contains an explicit mean-cluster-radius threshold used
to decide when directional PCA has become too diffuse for the current planning
boundary.

### DSG-ADAPTIVE-POLICY-004 `Directional-PCA initial mode`

Each adaptive planning flow starts in directional-PCA mode.

The adaptive realization does not begin in DCBC mode and does not require the
caller to decide the first algorithm dynamically after planning has started.

### DSG-ADAPTIVE-POLICY-005 `Deterministic collapse diagnostics`

At deterministic planning boundaries, the adaptive realization derives
structured collapse diagnostics from the represented planning inputs or outputs
available at that boundary.

Those diagnostics include the mean cluster radius produced by the current
directional-PCA realization, computed as the arithmetic mean of per-cluster
mean distances from represented items to their realized cluster centroids.

The adaptive realization compares that mean cluster radius with the configured
switch threshold to determine whether directional PCA remains eligible.

### DSG-ADAPTIVE-POLICY-006 `Deterministic switch execution`

When the measured mean cluster radius is greater than the configured switch
threshold, the adaptive realization switches from directional PCA to DCBC at
that boundary.

When the measured mean cluster radius is less than or equal to the configured
switch threshold, the realization remains on the directional-PCA path for that
boundary.

### DSG-ADAPTIVE-POLICY-007 `Direction continuity`

The selected built-in direction remains stable across the full adaptive
planning flow:

- in `Divisive` mode, the switch changes the clustering realization used for
  top-down refinement without changing the top-down direction
- in `Agglomerative` mode, the switch changes the clustering realization used
  for bottom-up grouping without changing the bottom-up direction

### DSG-ADAPTIVE-POLICY-008 `One-way switch rule`

After the first switch from directional PCA to DCBC, the adaptive realization
marks the flow as DCBC-owned for the remainder of that flow.

Later planning boundaries in the same flow therefore skip any attempt to switch
back to directional PCA.

### DSG-ADAPTIVE-POLICY-009 `Structured diagnostics and switch records`

For each evaluated boundary, the crate retains a structured record identifying:

- the active algorithm realization
- the deterministic zero-based adaptive boundary position for that evaluated
  planning boundary
- the deterministic inputs to the switch decision, including the measured mean
  cluster radius
- whether the switch criteria were satisfied
- whether the switch boundary occurred at that boundary

If surfaced publicly, these diagnostics remain deterministic and suitable for
validation without requiring parsing of free-form messages or inference from
record ordering alone.

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
