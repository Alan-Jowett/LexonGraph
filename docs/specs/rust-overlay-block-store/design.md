<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Overlay Block Store Design

## Status

Draft design specification for a Rust crate that implements the LexonGraph
block-storage contract as an overlay of multiple stores.

## Design Goals

The overlay design is intended to be:

- subordinate to the parent block-storage trait
- explicit about priority and fallthrough semantics
- reusable with heterogeneous store implementations
- reusable from concurrent callers through a thread-safe composition surface
- strict about inherited integrity and failure rules
- minimal at the public boundary

## Crate Boundary

The crate owns:

- overlay-specific construction and layer-adapter types
- ordered dispatch for `get`, `put`, and `iter_block_ids`
- overlay-owned layer roles and cache refill behavior

The crate does not own:

- block canonicalization or validation
- production backend implementations
- changes to the parent `BlockStore` trait
- durability policy beyond attempting the configured writable layers
- store-specific notification hooks or callbacks

## Core Types

### DSG-OVERLAY-001 `OverlayBlockStore`

The crate defines `OverlayBlockStore` as a `BlockStore` implementation that
owns an ordered stack of two or more overlay layers.

### DSG-OVERLAY-002 `Priority ordering`

The dispatch order is descending priority: layer index 0 is the highest
priority and the final layer is the lowest priority.

### DSG-OVERLAY-003 `Layer adapters and roles`

The crate defines an overlay-specific layer trait plus reusable adapter types
that let the caller classify each participating `BlockStore` as:

- a writable layer that accepts direct overlay `put`
- a cache layer that is skipped for direct overlay `put` but may receive
  overlay-managed write-back after a lower-layer `get` hit
- a read-only layer that participates in reads and enumeration only

Custom layer types may implement the overlay-specific layer trait directly.

The public layer-composition surface is overlay-owned so downstream crates can
reuse the overlay crate's dispatch logic instead of recreating it.

### DSG-OVERLAY-004 `Construction guard`

Overlay construction fails explicitly when the caller supplies fewer than two
layers.

## Behavioral Rules

### DSG-OVERLAY-005 `Read dispatch`

`get(block_id)` walks layers from highest priority to lowest priority.

- `Ok(Some(validated_block))` terminates successfully
- `Ok(None)` falls through
- `Err(error)` is recorded and falls through

If no layer succeeds, the overlay returns `Ok(None)` when all layers were
absent; otherwise it returns the last recorded error.

### DSG-OVERLAY-006 `Write dispatch`

`put(block)` walks layers from highest priority to lowest priority, attempting
only layers classified as writable.

- cache layers are skipped
- read-only layers are skipped
- each writable layer is attempted in order
- any writable layer that reports a non-canonical block ID is treated as an
  explicit write failure
- success is reported only if every writable layer succeeds with the
  content-addressed block ID for the stored block

If no writable layer exists, the overlay fails explicitly.

If one or more writable layers fail, the overlay returns an explicit error
after attempting all writable layers.

### DSG-OVERLAY-007 `Enumeration dispatch`

`iter_block_ids()` traverses layer enumeration surfaces in descending priority
order and yields a de-duplicated union of block IDs.

Duplicate IDs already yielded from a higher-priority layer are suppressed using
an internal seen-set.

### DSG-OVERLAY-008 `Enumeration failure handling`

Enumeration startup and mid-stream failures remain explicit.

The overlay does not silently skip a layer whose enumeration cannot be started
or continued, because the parent trait requires enumeration failures to be
surfaced rather than omitted.

### DSG-OVERLAY-009 `Read-through cache refill`

When `get` succeeds from a lower-priority layer, the overlay may write the
resolved block back into any higher-priority layers classified as caches.

### DSG-OVERLAY-010 `Non-fatal cache refill`

Cache-refill attempts are best-effort in this revision.

Failure to refill one or more higher-priority cache layers does not alter the
successful `get` result returned to the caller.

### DSG-OVERLAY-011 `Inherited integrity boundary`

The overlay reuses the parent trait's `ValidatedBlock`, `BlockHash`, and
`BlockStoreError` contract.

It does not weaken the parent trait's integrity validation, explicit-failure
semantics, or backend-neutral API boundary.

### DSG-OVERLAY-012 `Thread-safe composition boundary`

`OverlayBlockStore` and the overlay-owned public layer abstractions used for
composition are `Send + Sync`.

This lets concurrent callers share one overlay instance without widening the
parent `BlockStore` trait.

### DSG-OVERLAY-013 `Heterogeneous backend composition`

The generic role adapters and overlay construction path accept existing
repository `BlockStore` implementations, including `MemoryBlockStore`,
`FilesystemBlockStore`, and `AzureBlobBlockStore`, without requiring downstream
crates to duplicate overlay dispatch behavior.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-OVERLAY-001..004 | REQ-OVERLAY-STORE-001, REQ-OVERLAY-STORE-002, REQ-OVERLAY-STORE-009 |
| DSG-OVERLAY-005 | REQ-OVERLAY-STORE-003, REQ-OVERLAY-STORE-004, REQ-OVERLAY-STORE-011 |
| DSG-OVERLAY-006 | REQ-OVERLAY-STORE-005, REQ-OVERLAY-STORE-006, REQ-OVERLAY-STORE-011 |
| DSG-OVERLAY-007..008 | REQ-OVERLAY-STORE-007, REQ-OVERLAY-STORE-008, REQ-OVERLAY-STORE-011 |
| DSG-OVERLAY-009..010 | REQ-OVERLAY-STORE-010 |
| DSG-OVERLAY-011 | REQ-OVERLAY-STORE-011 |
| DSG-OVERLAY-012 | REQ-OVERLAY-STORE-012 |
| DSG-OVERLAY-013 | REQ-OVERLAY-STORE-001, REQ-OVERLAY-STORE-013 |
