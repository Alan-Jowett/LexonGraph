<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Adaptive Planning Policy Crate Requirements

## Status

Draft specification for a Rust crate that composes streaming directional PCA and
streaming DCBC behind one deterministic adaptive built-in planning realization
for the LexonGraph streaming indexer.

## Scope

This document specifies the crate-level requirements for a Rust crate that:

- defines an adaptive aggregate planning-policy crate at
  `crates/lexongraph-adaptive-planning-policy`
- composes `docs/specs/rust-directional-pca-crate/` and
  `docs/specs/rust-dcbc-streaming-crate/`
- is consumed by `docs/specs/rust-streaming-indexer-crate/` as one built-in
  planning realization
- starts planning with directional PCA and switches deterministically to DCBC
  when configured PCA-collapse criteria are met

This document does not redefine the shared streaming clustering contract, block
materialization semantics, or the owned algorithmic boundaries of the
directional-PCA and DCBC crates.

## Terminology

In this spec package, `adaptive planning flow` means one internally coordinated
planning execution over a logical planning problem in which the aggregate
realization may use directional PCA first and later switch to DCBC.

`Switch boundary` means the first deterministic planning boundary after which
the adaptive realization no longer uses directional PCA for the remainder of the
same adaptive planning flow.

`Adaptive boundary position` means the deterministic zero-based position of an
evaluated adaptive planning boundary within one adaptive planning flow.

`Collapse diagnostics` means the explicit deterministic measurements and
threshold comparisons used to decide whether directional PCA remains eligible
for the current planning work.

`Mean cluster radius` means the arithmetic mean of the realized per-cluster
mean distances from represented items to their deterministic cluster centroids
at one adaptive decision boundary.

## Requirements

### REQ-ADAPTIVE-POLICY-001

The repository shall define a dedicated Rust crate for adaptive aggregate
planning policy at `crates/lexongraph-adaptive-planning-policy`.

### REQ-ADAPTIVE-POLICY-002

The crate shall remain subordinate to:

- `docs/specs/rust-streaming-indexer-crate/` for indexer-owned planning and
  finalized hierarchy semantics
- `docs/specs/rust-directional-pca-crate/` for the owned streaming
  directional-PCA realization
- `docs/specs/rust-dcbc-streaming-crate/` for the owned streaming DCBC
  realization

The crate shall not redefine the shared streaming clustering contract or the
algorithm-specific mechanics already owned by those subordinate crates.

### REQ-ADAPTIVE-POLICY-003

The crate shall expose a deterministic aggregate planning realization or factory
consumable by the streaming indexer's built-in planning path.

This realization shall remain internal to built-in planning selection rather
than introducing a caller-visible interactive "choose the next algorithm after
each layer" lifecycle.

### REQ-ADAPTIVE-POLICY-004

The adaptive policy configuration shall accept, at minimum:

- a built-in hierarchy-construction direction
- directional-PCA settings
- DCBC settings
- an explicit deterministic mean-cluster-radius switch threshold

### REQ-ADAPTIVE-POLICY-005

For each adaptive planning flow, the realization shall begin with the streaming
directional-PCA path before any switch to DCBC is considered.

### REQ-ADAPTIVE-POLICY-006

The adaptive realization shall support both `Divisive` and `Agglomerative`
indexer planning directions.

The selected direction governs the planning flow before and after any internal
algorithm switch.

### REQ-ADAPTIVE-POLICY-007

The adaptive realization shall evaluate explicit deterministic collapse
diagnostics at deterministic planning boundaries.

Those diagnostics shall be derived from the represented planning inputs or
planning outputs available at the adaptive boundary and shall be sufficient to
decide whether directional PCA remains eligible without relying on randomness or
free-form human intervention.

The recorded diagnostics shall include the mean cluster radius measured for the
current directional-PCA realization at that boundary.

If a boundary reports a measured mean cluster radius of `0.0`, the
implementation shall preserve enough faithful diagnostic state to determine
whether that value arose from legitimately zero-radius realized clusters or
from a defect in the diagnostic path.

### REQ-ADAPTIVE-POLICY-008

When the measured mean cluster radius exceeds the configured switch threshold,
the adaptive realization shall switch deterministically from directional PCA to
DCBC.

### REQ-ADAPTIVE-POLICY-009

Within one adaptive planning flow, once the realization switches from
directional PCA to DCBC, it shall not switch back to directional PCA later in
that same flow.

### REQ-ADAPTIVE-POLICY-010

If the measured mean cluster radius does not exceed the configured switch
threshold, the adaptive realization shall remain on the directional-PCA path
and shall not switch merely because DCBC would also be a valid planning
realization.

### REQ-ADAPTIVE-POLICY-011

The adaptive realization shall preserve compatibility with the indexer's
existing finalized partition hierarchy abstraction.

Both its pre-switch directional-PCA output and post-switch DCBC output shall be
normalized into that same hierarchy abstraction before final materialization.

### REQ-ADAPTIVE-POLICY-012

The crate shall define structured diagnostics and switch-decision records
sufficient to explain and validate:

- why directional PCA remained eligible or became ineligible
- where the switch boundary occurred
- the caller-usable adaptive boundary position associated with each evaluated
  planning boundary
- which algorithm realization was active for a given planning segment
- the measured mean cluster radius and the configured mean cluster radius
  threshold for each evaluated boundary whose diagnostics were computed, plus
  explicit unavailability when those diagnostics do not yet exist

If a measured mean cluster radius of `0.0` is surfaced, those diagnostics and
switch-decision records shall remain sufficient to audit that reported value
without requiring inference from lossy formatting or omitted diagnostic state.

If any of those diagnostics are surfaced beyond internal crate state, they shall
remain deterministic for identical inputs and configuration.

### REQ-ADAPTIVE-POLICY-013

Invalid adaptive configuration, invalid mean-cluster-radius thresholds, failure
to compute deterministic mean-cluster-radius diagnostics, and unsupported
direction or realization combinations shall fail explicitly rather than silently
substituting a different algorithm, threshold interpretation, or direction.

### REQ-ADAPTIVE-POLICY-014

The repository shall include automated verification artifacts covering:

- construction of the adaptive realization through the intended built-in path
- deterministic no-switch directional-PCA behavior
- deterministic PCA-to-DCBC switch behavior
- deterministic switch-boundary reproduction
- deterministic below-threshold and above-threshold mean-cluster-radius
  behavior using a current threshold assumption of `0.25`
- support for both `Divisive` and `Agglomerative` direction modes
- compatibility with the existing finalized partition hierarchy abstraction

## Out of Scope

This crate does not define or own:

- a new shared streaming clustering contract
- caller-interactive per-layer algorithm selection
- nondeterministic or heuristic-only switching without explicit deterministic
  criteria
- final block materialization semantics
- reimplementation of the owned directional-PCA or DCBC algorithm mechanics

## Relationship to Other Specifications

This document is subordinate to the streaming indexer, streaming directional
PCA, and streaming DCBC specification packages for their owned concerns.
