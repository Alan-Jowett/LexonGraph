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
- strict about inherited integrity and failure rules
- minimal at the public boundary

## Crate Boundary

The crate owns:

- overlay-specific construction and layer-adapter types
- ordered dispatch for `get`, `put`, and `iter_block_ids`
- optional outcome-notification hooks for `get` and `put`

The crate does not own:

- block canonicalization or validation
- production backend implementations
- changes to the parent `BlockStore` trait
- write replication to all layers
- notification hooks for enumeration in this revision

## Core Types

### DSG-OVERLAY-001 `OverlayBlockStore`

The crate defines `OverlayBlockStore` as a `BlockStore` implementation that
owns an ordered stack of two or more overlay layers.

### DSG-OVERLAY-002 `Priority ordering`

The dispatch order is descending priority: layer index 0 is the highest
priority and the final layer is the lowest priority.

### DSG-OVERLAY-003 `Layer adapters`

The crate defines an overlay-specific layer trait plus reusable adapter types
for:

- passive participation by plain `BlockStore` implementations
- participation with an optional outcome observer

Custom layer types may implement the overlay-specific layer trait directly.

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

`put(block)` walks layers from highest priority to lowest priority.

- the first `Ok(block_id)` terminates successfully
- `Err(error)` is recorded and falls through

If no layer succeeds, the overlay returns the last recorded error.

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

### DSG-OVERLAY-009 `Notification trait`

The crate defines an optional trait, separate from `BlockStore`, that a layer
may expose to receive the completed outcome of `get` and `put`.

The notification surface carries the final operation result observed by the
overlay, including successful block hits, misses, stored block IDs, and
explicit failures.

### DSG-OVERLAY-010 `Notification order`

After `get` or `put` completes, the overlay notifies participating layers from
lowest priority to highest priority.

Notification is observational in this revision: it must not alter the already
determined result returned to the caller.

### DSG-OVERLAY-011 `Inherited integrity boundary`

The overlay reuses the parent trait's `ValidatedBlock`, `BlockHash`, and
`BlockStoreError` contract.

It does not weaken the parent trait's integrity validation, explicit-failure
semantics, or backend-neutral API boundary.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-OVERLAY-001..004 | REQ-OVERLAY-STORE-001, REQ-OVERLAY-STORE-002 |
| DSG-OVERLAY-005 | REQ-OVERLAY-STORE-003, REQ-OVERLAY-STORE-004, REQ-OVERLAY-STORE-011 |
| DSG-OVERLAY-006 | REQ-OVERLAY-STORE-005, REQ-OVERLAY-STORE-006, REQ-OVERLAY-STORE-011 |
| DSG-OVERLAY-007..008 | REQ-OVERLAY-STORE-007, REQ-OVERLAY-STORE-008, REQ-OVERLAY-STORE-011 |
| DSG-OVERLAY-009..010 | REQ-OVERLAY-STORE-009, REQ-OVERLAY-STORE-010 |
| DSG-OVERLAY-011 | REQ-OVERLAY-STORE-011 |
