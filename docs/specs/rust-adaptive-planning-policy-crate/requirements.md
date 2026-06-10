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
parameter comparisons used to decide whether directional PCA remains eligible
for the current planning work.

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
- a tunable `pc1_explained_variance_ratio_threshold` for divisive adaptive
  collection selection
- a tunable `dcbc_max_embedding_count` upper bound that gates divisive DCBC use

### REQ-ADAPTIVE-POLICY-005

For each adaptive divisive planning flow, the realization shall begin with the
streaming directional-PCA path before any per-collection decision is evaluated.

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

For divisive adaptive planning, the recorded diagnostics shall include:

- the explained variance ratio of the first principal component for the current
  collection of embeddings
- the represented embedding count observed for that collection

### REQ-ADAPTIVE-POLICY-008

For divisive adaptive planning, when the explained variance ratio of the first
principal component for an evaluated collection is less than the configured
`pc1_explained_variance_ratio_threshold` and the collection's represented
embedding count is less than the configured `dcbc_max_embedding_count`, the
adaptive realization shall select DCBC for that collection.

For agglomerative adaptive planning, the existing count-based PCA-to-DCBC
switch behavior remains unchanged in this experiment.

### REQ-ADAPTIVE-POLICY-009

For divisive adaptive planning, each evaluated collection shall choose its
realization independently; a later collection may return to directional PCA or
choose DCBC regardless of earlier collections in the same pass or layer.

For agglomerative adaptive planning, once the realization switches from
directional PCA to DCBC, it shall not switch back to directional PCA later in
that same flow.

### REQ-ADAPTIVE-POLICY-010

For divisive adaptive planning, the adaptive realization shall select
directional PCA for an evaluated collection when either:

- the measured first-principal-component explained variance ratio is greater
  than or equal to the configured `pc1_explained_variance_ratio_threshold`, or
- the measured explained variance ratio is below that threshold but the
  collection's represented embedding count is greater than or equal to the
  configured `dcbc_max_embedding_count`

For agglomerative adaptive planning, if the represented embedding count at an
evaluated boundary is greater than or equal to the unchanged count cutoff, the
adaptive realization shall remain on the directional-PCA path and shall not
switch merely because DCBC would also be a valid planning realization.

### REQ-ADAPTIVE-POLICY-011

The adaptive realization shall preserve compatibility with the indexer's
existing finalized partition hierarchy abstraction.

Both its pre-switch directional-PCA output and post-switch DCBC output shall be
normalized into that same hierarchy abstraction before final materialization.

### REQ-ADAPTIVE-POLICY-012

The crate shall define structured diagnostics and switch-decision records
sufficient to explain and validate:

- why directional PCA remained eligible or became ineligible
- for agglomerative adaptive planning, where the first switch boundary occurred
- the caller-usable adaptive boundary position associated with each evaluated
  planning boundary
- which algorithm realization was active for a given planning segment
- for divisive adaptive planning, the measured
  `pc1_explained_variance_ratio`, configured
  `pc1_explained_variance_ratio_threshold`, measured `embedding_count`, and
  configured `dcbc_max_embedding_count` for each evaluated collection whose
  diagnostics were computed, plus explicit unavailability when those diagnostics
  do not yet exist
- an explicit structured reason identifying whether a divisive collection chose
  directional PCA because PC1 stayed at or above the threshold, chose DCBC
  because PC1 was below the threshold and the embedding count was below the
  configured upper bound, or stayed on PCA because DCBC was disallowed by the
  embedding-count upper bound

If any of those diagnostics are surfaced beyond internal crate state, they shall
remain deterministic for identical inputs and configuration.

### REQ-ADAPTIVE-POLICY-013

Invalid adaptive configuration, failure to compute deterministic PC1 or
embedding-count diagnostics, and unsupported direction or realization
combinations shall fail explicitly rather than silently substituting a
different algorithm, parameter interpretation, or direction.

### REQ-ADAPTIVE-POLICY-014

The repository shall include automated verification artifacts covering:

- construction of the adaptive realization through the intended built-in path
- deterministic divisive directional-PCA selection when PC1 is at or above the
  configured threshold
- deterministic divisive DCBC selection when PC1 is below threshold and the
  embedding count is below the configured upper bound
- deterministic divisive directional-PCA retention when PC1 is below threshold
  but the embedding count is at or above the configured upper bound
- deterministic agglomerative switch-boundary reproduction under the unchanged
  agglomerative rule
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
