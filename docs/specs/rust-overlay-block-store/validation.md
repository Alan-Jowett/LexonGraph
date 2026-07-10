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

Use an overlay with writable, cache, and read-only layers.

**Pass condition:** `put` attempts every writable layer in order and skips
cache and read-only layers.

**Traces to:** REQ-OVERLAY-STORE-005, REQ-OVERLAY-STORE-006

### VAL-OVERLAY-007

Make one writable layer fail while other writable layers still exist later in
priority order.

**Pass condition:** the overlay still attempts the later writable layers and
returns an explicit error rather than reporting success.

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

Construct an overlay with no layer that accepts direct writes.

**Pass condition:** `put` fails explicitly.

**Traces to:** REQ-OVERLAY-STORE-009

### VAL-OVERLAY-012

Complete a `get` via a lower-priority layer while a higher-priority cache layer
sits above it.

**Pass condition:** the retrieved block is written back into the higher cache
layer without widening the parent `BlockStore` trait.

**Traces to:** REQ-OVERLAY-STORE-010

### VAL-OVERLAY-013

Cause cache write-back after a successful lower-layer `get` to fail.

**Pass condition:** `get` still succeeds with the lower-layer block and the
cache write-back failure remains non-fatal.

**Traces to:** REQ-OVERLAY-STORE-010

### VAL-OVERLAY-014

Inspect the public API surface.

**Pass condition:** role-based layering is additive, and the parent
`BlockStore` trait remains unchanged.

**Traces to:** REQ-OVERLAY-STORE-009, REQ-OVERLAY-STORE-011

### VAL-OVERLAY-015

Make a writable layer report success with a block ID that does not match the
canonical content-addressed ID for the input block.

**Pass condition:** `put` fails explicitly rather than reporting success with an
incorrect block ID.

**Traces to:** REQ-OVERLAY-STORE-005, REQ-OVERLAY-STORE-006

### VAL-OVERLAY-016

Type-check the overlay public composition surface in a compile-time assertion
context.

**Pass condition:** `OverlayBlockStore` and the overlay-owned public layer
abstractions used for composition satisfy `Send + Sync`.

**Traces to:** REQ-OVERLAY-STORE-012

### VAL-OVERLAY-017

Construct one overlay from the repository's `MemoryBlockStore`,
`FilesystemBlockStore`, and `AzureBlobBlockStore` using only overlay-owned role
adapters and construction entry points.

**Pass condition:** the heterogeneous composition succeeds without downstream
code reimplementing overlay read ordering, write ordering, or enumeration
de-duplication behavior.

**Traces to:** REQ-OVERLAY-STORE-001, REQ-OVERLAY-STORE-009, REQ-OVERLAY-STORE-013

### VAL-OVERLAY-018

Complete a `get` from a lower-priority layer while a higher-priority bounded
cache layer sits above it, and make the resolved block too large to fit within
that cache layer's configured payload-byte budget.

**Pass condition:** the lower-layer `get` still succeeds, the oversized block is
not admitted into the bounded cache layer, and the cache-admission failure
remains non-fatal to the overall `get`.

**Traces to:** REQ-OVERLAY-STORE-014
