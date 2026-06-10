<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Overlay Block Store Validation

## Status

Draft validation specification for a Rust crate that implements the LexonGraph
block-storage contract as an overlay of multiple stores.

## Validation Entries

### VAL-OVERLAY-001

Construct an overlay with fewer than two layers.

**Pass condition:** construction fails explicitly.

**Traces to:** REQ-OVERLAY-STORE-001

### VAL-OVERLAY-002

Place a readable block in a higher-priority layer and another readable block in
a lower-priority layer for the same request path.

**Pass condition:** `get` returns the higher-priority success and does not
consult lower layers after that success.

**Traces to:** REQ-OVERLAY-STORE-002, REQ-OVERLAY-STORE-003

### VAL-OVERLAY-003

Make a higher-priority layer return `Ok(None)` and a lower-priority layer
return `Ok(Some(_))`.

**Pass condition:** `get` falls through and returns the lower-layer block.

**Traces to:** REQ-OVERLAY-STORE-003, REQ-OVERLAY-STORE-004

### VAL-OVERLAY-004

Make one or more higher-priority layers return explicit `BlockStoreError`, with
a lower-priority layer returning `Ok(Some(_))`.

**Pass condition:** `get` succeeds from the lower layer.

**Traces to:** REQ-OVERLAY-STORE-004

### VAL-OVERLAY-005

Make all layers fail `get`, mixing absence and explicit errors.

**Pass condition:** the result is `Ok(None)` only when all layers report
absence; otherwise the overlay returns the last explicit error.

**Traces to:** REQ-OVERLAY-STORE-004

### VAL-OVERLAY-006

Make a higher-priority layer reject `put` and a lower-priority layer accept it.

**Pass condition:** `put` returns success from the first accepting layer and
does not continue below that layer.

**Traces to:** REQ-OVERLAY-STORE-005, REQ-OVERLAY-STORE-006

### VAL-OVERLAY-007

Make all layers fail `put`.

**Pass condition:** the overlay returns the last explicit error.

**Traces to:** REQ-OVERLAY-STORE-006

### VAL-OVERLAY-008

Run the parent conformance suite against the overlay implementation.

**Pass condition:** the overlay still satisfies the inherited `BlockStore`
contract.

**Traces to:** REQ-OVERLAY-STORE-011

### VAL-OVERLAY-009

Enumerate block IDs where some IDs exist in multiple layers and others exist in
only one layer.

**Pass condition:** enumeration yields a streaming, de-duplicated union in
priority order.

**Traces to:** REQ-OVERLAY-STORE-007, REQ-OVERLAY-STORE-008

### VAL-OVERLAY-010

Cause enumeration to encounter an explicit layer failure.

**Pass condition:** the failure is surfaced explicitly and is not silently
skipped.

**Traces to:** REQ-OVERLAY-STORE-011

### VAL-OVERLAY-011

Use layers where some opt into notifications and some do not.

**Pass condition:** operations complete normally and only opted-in layers are
notified.

**Traces to:** REQ-OVERLAY-STORE-009

### VAL-OVERLAY-012

Complete a `get` via a lower-priority layer while higher-priority notification
participants sit above it.

**Pass condition:** notifications run from low priority to high priority and
expose the final `get` result.

**Traces to:** REQ-OVERLAY-STORE-010

### VAL-OVERLAY-013

Complete a `put` via a lower-priority layer after higher-priority failures.

**Pass condition:** notifications run from low priority to high priority and
expose the final `put` result.

**Traces to:** REQ-OVERLAY-STORE-010

### VAL-OVERLAY-014

Inspect the public API surface.

**Pass condition:** notification support is additive and optional, and the
parent `BlockStore` trait remains unchanged.

**Traces to:** REQ-OVERLAY-STORE-009, REQ-OVERLAY-STORE-011
