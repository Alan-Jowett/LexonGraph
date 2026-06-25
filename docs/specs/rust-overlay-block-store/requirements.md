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

The overlay crate shall own the layer-composition logic so downstream crates do
not need to reimplement overlay dispatch semantics.

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

`put` shall attempt each direct-write-capable layer in descending priority
order.

Cache layers shall not participate in direct writes.

Direct-write-capable layers that succeed shall return the content-addressed
block ID for the block being stored.

If a direct-write-capable layer reports success with a block ID different from
the canonical content-addressed ID for the input block, the overlay shall fail
explicitly rather than reporting success.

### REQ-OVERLAY-STORE-006

During `put`, the overlay shall skip layers designated as cache-only or
read-only.

The overlay shall report success only when every direct-write-capable layer
succeeds.

If any direct-write-capable layer fails, the overlay shall return an explicit
error.

If no layer accepts direct writes, the overlay shall fail explicitly.

### REQ-OVERLAY-STORE-007

`iter_block_ids` shall expose a streaming, de-duplicated union of block IDs
across all layers without requiring callers to know which layer supplied an ID.

### REQ-OVERLAY-STORE-008

When the same block ID is present in multiple layers, enumeration shall yield
that block ID once and shall preserve overlay-visible higher-priority
precedence rather than per-layer duplication.

### REQ-OVERLAY-STORE-009

The overlay crate shall let callers classify each layer as cache, writable, or
read-only without widening the parent `BlockStore` trait.

### REQ-OVERLAY-STORE-010

After a `get` completes from a lower-priority layer, the overlay may write the
retrieved block back into higher-priority cache layers.

Write-back failure to one or more cache layers shall be non-fatal and shall not
change the `get` result returned to the caller.

### REQ-OVERLAY-STORE-011

The overlay crate shall preserve the parent trait's integrity, explicit failure,
streaming enumeration, and backend neutrality rules.

### REQ-OVERLAY-STORE-012

`OverlayBlockStore` and the overlay-owned public layer abstractions used to
compose layers shall be `Send + Sync`.

### REQ-OVERLAY-STORE-013

The overlay crate shall provide a reusable, generic composition surface that can
combine the repository's memory, filesystem, and Azure/blob `BlockStore`
implementations into one overlay `BlockStore`.

Downstream crates shall not need to duplicate overlay read ordering, write
ordering, or de-duplication logic to use that heterogeneous composition.

## Out of Scope

This crate does not define or own:

- changes to the parent `BlockStore` trait
- production backend implementations
- write-through replication to all layers
- cache eviction policy or persistence policy
- store-specific notification hooks or callbacks

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
