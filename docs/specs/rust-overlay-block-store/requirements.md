<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Overlay Block Store Requirements

## Status

Draft specification for a Rust crate that implements the LexonGraph
block-storage contract as an ordered overlay of multiple block stores.

## Scope

This document specifies implementation-specific requirements for a Rust crate
that realizes `docs/specs/rust-block-storage-trait/` by composing two or more
underlying `BlockStore` implementations into an overlay.

This document is layered on top of:

- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`

This document does not redefine the parent trait contract. It adds only the
overlay-specific requirements needed to realize layered block-store behavior in
this repository.

## Terminology

In this spec package, `layer` means one underlying implementation of
`BlockStore` that participates in the overlay.

`Higher-priority layer` means a layer consulted earlier for reads, writes, and
enumeration. `Lower-priority layer` means a layer consulted later.

## Requirements

### REQ-OVERLAY-STORE-001

The repository shall include a Rust crate, separate from
`crates/lexongraph-block-store`, that implements the parent `BlockStore`
contract by composing two or more ordered layers.

### REQ-OVERLAY-STORE-002

The overlay crate shall define a deterministic priority order from highest
layer to lowest layer while preserving the backend-neutral public API inherited
from the parent trait.

### REQ-OVERLAY-STORE-003

`get` shall attempt layers in descending priority order and shall stop at the
first layer that returns `Ok(Some(validated_block))`.

### REQ-OVERLAY-STORE-004

During `get`, the overlay shall continue to lower-priority layers when a
higher-priority layer returns `Ok(None)` or an explicit `BlockStoreError`.

If no layer returns `Ok(Some(_))`, the overlay shall return `Ok(None)` when all
layers report absence; otherwise it shall return the last explicit error
encountered.

### REQ-OVERLAY-STORE-005

`put` shall attempt layers in descending priority order and shall stop at the
first layer that returns success.

### REQ-OVERLAY-STORE-006

During `put`, the overlay shall continue to lower-priority layers when a
higher-priority layer returns an explicit `BlockStoreError`.

If no layer accepts the write, the overlay shall return the last explicit error
encountered.

### REQ-OVERLAY-STORE-007

`iter_block_ids` shall expose a streaming, de-duplicated union of block IDs
across all layers without requiring callers to know which layer supplied an ID.

### REQ-OVERLAY-STORE-008

When the same block ID is present in multiple layers, enumeration shall yield
that block ID once and shall preserve overlay-visible higher-priority
precedence rather than per-layer duplication.

### REQ-OVERLAY-STORE-009

The overlay crate shall define an optional overlay-specific notification trait,
separate from the parent `BlockStore` trait, that a layer may opt into to
observe completed `get` and `put` outcomes.

Layers that do not opt into this trait shall remain valid overlay
participants.

### REQ-OVERLAY-STORE-010

After a `get` or `put` completes, the overlay shall notify participating layers
in ascending order from lowest layer to highest layer using the completed
operation result.

### REQ-OVERLAY-STORE-011

The overlay crate shall preserve the parent trait's integrity, explicit-failure,
streaming-enumeration, and backend-neutrality rules.

## Out of Scope

This crate does not define or own:

- changes to the parent `BlockStore` trait
- production backend implementations
- write-through replication to all layers
- cache eviction policy or persistence policy
- notification behavior for identifier enumeration in this revision

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
