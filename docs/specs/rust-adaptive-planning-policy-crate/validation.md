<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Adaptive Planning Policy Crate Validation

## Status

Draft validation specification for a Rust crate that defines deterministic
adaptive planning-policy settings, switch selection, and diagnostics for the
LexonGraph streaming indexer's adaptive built-in planning path.

## Validation Scope

These validation entries define the conformance surface for the adaptive
planning-policy crate. They cover both:

- deterministic adaptive switching behavior
- compatibility of selector outputs and diagnostics with the indexer's existing
  finalized partition hierarchy boundary

## Validation Entries

### VAL-ADAPTIVE-POLICY-001

Inspect the repository artifacts for the new crate.

**Pass condition:** the repository includes a crate at
`crates/lexongraph-adaptive-planning-policy` and this spec package.

**Traces to:** REQ-ADAPTIVE-POLICY-001

### VAL-ADAPTIVE-POLICY-002

Inspect the crate's public surface and its specification references.

**Pass condition:** the crate exposes an adaptive planning-policy settings and
selector surface for the streaming indexer's built-in planning path, remains
subordinate to the streaming indexer, directional-PCA, and streaming DCBC
specification packages, and does not redefine the shared streaming clustering
contract.

**Traces to:** REQ-ADAPTIVE-POLICY-002, REQ-ADAPTIVE-POLICY-003

### VAL-ADAPTIVE-POLICY-003

Construct the adaptive selector surface with valid explicit settings.

**Pass condition:** construction succeeds only when direction, directional-PCA
settings, DCBC settings, and a finite non-negative mean-cluster-radius switch
threshold are all provided in supported combinations.

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
adaptive flow, regardless of whether a switch occurs.

**Traces to:** REQ-ADAPTIVE-POLICY-006

### VAL-ADAPTIVE-POLICY-006

Run a deterministic fixture and inspect the adaptive diagnostics at each
decision boundary.

**Pass condition:** the recorded diagnostics are structured, deterministic, and
sufficient to decide whether directional PCA remained eligible at that
boundary, including the measured mean cluster radius.

**Traces to:** REQ-ADAPTIVE-POLICY-007, REQ-ADAPTIVE-POLICY-012

### VAL-ADAPTIVE-POLICY-007

Run a deterministic fixture whose measured mean cluster radius stays at or
below the configured threshold.

**Pass condition:** the adaptive realization does not switch to DCBC and
remains on the directional-PCA path throughout the exercised flow.

**Traces to:** REQ-ADAPTIVE-POLICY-008, REQ-ADAPTIVE-POLICY-010

### VAL-ADAPTIVE-POLICY-008

Run a deterministic fixture whose measured mean cluster radius exceeds the
configured threshold.

**Pass condition:** the adaptive realization switches from directional PCA to
DCBC at a deterministic boundary.

**Traces to:** REQ-ADAPTIVE-POLICY-008

### VAL-ADAPTIVE-POLICY-009

Continue the same switch-triggering flow after the first adaptive switch.

**Pass condition:** later planning segments in that same flow remain DCBC-owned
and do not switch back to directional PCA.

**Traces to:** REQ-ADAPTIVE-POLICY-009

### VAL-ADAPTIVE-POLICY-010

Complete both a no-switch adaptive flow and a switch-triggering adaptive flow
through the indexer's hierarchy-normalization boundary.

**Pass condition:** both flows produce selector outputs and diagnostics that
remain compatible with the indexer's existing finalized partition hierarchy
abstraction without requiring a different final materialization contract.

**Traces to:** REQ-ADAPTIVE-POLICY-011

### VAL-ADAPTIVE-POLICY-011

Exercise invalid adaptive configuration, invalid mean-cluster-radius threshold
configuration, and an unsupported direction or realization combination.

**Pass condition:** each case fails explicitly rather than silently
substituting another algorithm, threshold interpretation, or direction.

**Traces to:** REQ-ADAPTIVE-POLICY-013

### VAL-ADAPTIVE-POLICY-012

Inspect the repository verification artifacts for the adaptive crate and repeat
the same switch-triggering fixture twice.

**Pass condition:** automated coverage exists for selector construction,
no-switch behavior, switch-trigger behavior, both directions, and deterministic
structured diagnostics compatible with the indexer-owned hierarchy boundary,
including coverage for a current threshold assumption of `0.25`, and both
repeated runs select the same switch boundary.

**Traces to:** REQ-ADAPTIVE-POLICY-014
