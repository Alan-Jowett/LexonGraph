<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Adaptive Planning Policy Crate Validation

## Status

Draft validation specification for a Rust crate that composes streaming
directional PCA and streaming DCBC into one deterministic adaptive built-in
planning realization for the LexonGraph streaming indexer.

## Validation Scope

These validation entries define the conformance surface for the adaptive
planning-policy crate. They cover both:

- deterministic adaptive switching behavior
- compatibility with the indexer's existing finalized partition hierarchy

## Validation Entries

### VAL-ADAPTIVE-POLICY-001

Inspect the repository artifacts for the new crate.

**Pass condition:** the repository includes a crate at
`crates/lexongraph-adaptive-planning-policy` and this spec package.

**Traces to:** REQ-ADAPTIVE-POLICY-001

### VAL-ADAPTIVE-POLICY-002

Inspect the crate's public surface and its specification references.

**Pass condition:** the crate exposes an aggregate planning realization or
factory for the streaming indexer's built-in planning path, remains subordinate
to the streaming indexer, directional-PCA, and streaming DCBC specification
packages, and does not redefine the shared streaming clustering contract.

**Traces to:** REQ-ADAPTIVE-POLICY-002, REQ-ADAPTIVE-POLICY-003

### VAL-ADAPTIVE-POLICY-003

Construct the adaptive realization with valid explicit settings.

**Pass condition:** construction succeeds only when direction, directional-PCA
settings, DCBC settings, `pc1_explained_variance_ratio_threshold`, and
`dcbc_max_embedding_count` are all provided in supported combinations.

**Traces to:** REQ-ADAPTIVE-POLICY-004, REQ-ADAPTIVE-POLICY-013

### VAL-ADAPTIVE-POLICY-004

Start an adaptive planning flow on a conformant fixture.

**Pass condition:** the first realized planning segment uses directional PCA
rather than DCBC.

**Traces to:** REQ-ADAPTIVE-POLICY-005

### VAL-ADAPTIVE-POLICY-005

Construct two adaptive runs over fixtures compatible with both supported
directions: one `Divisive` and one `Agglomerative`.

**Pass condition:** both runs preserve their selected direction across the full
adaptive flow, regardless of whether divisive collections choose different
realizations or agglomerative planning switches.

**Traces to:** REQ-ADAPTIVE-POLICY-006

### VAL-ADAPTIVE-POLICY-006

Run a deterministic fixture and inspect the adaptive diagnostics at each
decision boundary.

**Pass condition:** the recorded diagnostics are structured, deterministic, and
sufficient to decide whether directional PCA remained eligible at that
boundary, including for divisive planning the measured
`pc1_explained_variance_ratio`, configured
`pc1_explained_variance_ratio_threshold`, measured `embedding_count`,
configured `dcbc_max_embedding_count`, an explicit structured decision reason,
a caller-usable adaptive boundary position, and explicit unavailability
semantics where diagnostics do not yet exist.

**Traces to:** REQ-ADAPTIVE-POLICY-007, REQ-ADAPTIVE-POLICY-012

### VAL-ADAPTIVE-POLICY-007

Run a deterministic divisive fixture whose evaluated collections produce first
principal component explained-variance ratios greater than or equal to the
configured threshold.

**Pass condition:** the adaptive realization selects directional PCA for each
evaluated collection and does not route those collections through DCBC.

**Traces to:** REQ-ADAPTIVE-POLICY-008, REQ-ADAPTIVE-POLICY-010

### VAL-ADAPTIVE-POLICY-008

Run a deterministic divisive fixture whose evaluated collection produces a
first principal component explained-variance ratio below the configured
threshold while also keeping the embedding count below the configured DCBC
upper bound.

**Pass condition:** the adaptive realization selects DCBC for that collection
deterministically.

**Traces to:** REQ-ADAPTIVE-POLICY-008

### VAL-ADAPTIVE-POLICY-009

Continue a divisive adaptive flow across multiple collections whose PCA ratios
and embedding counts exercise different selection outcomes.

**Pass condition:** later collections can independently choose directional PCA
or DCBC according to the configured divisive decision rule, while agglomerative
flows retain their existing one-way DCBC ownership.

**Traces to:** REQ-ADAPTIVE-POLICY-009

### VAL-ADAPTIVE-POLICY-010

Complete both a no-switch adaptive flow and a switch-triggering adaptive flow
through the indexer's hierarchy-normalization boundary.

**Pass condition:** both flows normalize into the indexer's existing finalized
partition hierarchy abstraction without requiring a different final
materialization contract.

**Traces to:** REQ-ADAPTIVE-POLICY-011

### VAL-ADAPTIVE-POLICY-011

Exercise invalid adaptive configuration and an unsupported direction or
realization combination.

**Pass condition:** each case fails explicitly rather than silently
substituting another algorithm, parameter interpretation, or direction.

**Traces to:** REQ-ADAPTIVE-POLICY-013

### VAL-ADAPTIVE-POLICY-012

Inspect the repository verification artifacts for the adaptive crate and repeat
the same switch-triggering fixture twice.

**Pass condition:** automated coverage exists for construction, no-switch
behavior, switch-trigger behavior, both directions, and hierarchy
compatibility, including coverage for divisive PC1-at-or-above-threshold
behavior, divisive below-threshold-and-below-upper-bound DCBC behavior,
divisive below-threshold-but-too-large PCA retention, and both repeated runs
surface the same adaptive boundary position, measured PC1 ratio, configured
threshold, measured embedding count, configured upper bound, and availability
semantics for the exercised decision.

**Traces to:** REQ-ADAPTIVE-POLICY-014
