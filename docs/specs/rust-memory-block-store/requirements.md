<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Memory Block Store Requirements

## Status

Draft specification for a Rust crate that implements the LexonGraph
block-storage contract using volatile in-memory residency.

## Scope

This document specifies implementation-specific requirements for a Rust crate
that realizes `docs/specs/rust-block-storage-trait/` as a bounded in-memory
backend.

This document is layered on top of:

- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`
- `docs/specs/rust-overlay-block-store/`

This document does not redefine the parent `BlockStore` contract. It adds only
memory-backend-specific requirements needed to realize a volatile cache-oriented
store in this repository.

## Terminology

In this spec package, `resident entry` means one block currently retained in the
store's in-memory state.

`Resident capacity` means the configured maximum number of resident block
entries that may be retained simultaneously.

## Requirements

### REQ-MEM-STORE-001

The repository shall include a Rust crate, separate from
`crates/lexongraph-block-store`, that implements the parent `BlockStore`
contract using volatile in-memory residency.

### REQ-MEM-STORE-002

The memory block-store crate shall remain subordinate to
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`,
`docs/specs/rust-block-storage-trait/`, and
`docs/specs/rust-overlay-block-store/` for block identity, validation, the
backend-neutral `BlockStore` contract, and overlay notification semantics.

### REQ-MEM-STORE-003

Construction shall accept an explicit resident capacity outside the `BlockStore`
trait boundary.

Construction shall fail explicitly when the requested resident capacity is zero.

### REQ-MEM-STORE-004

The memory block-store crate shall retain stored block content in process memory
only and shall provide no durability across process termination.

### REQ-MEM-STORE-005

`put` shall derive canonical bytes and the block ID through the block crate,
store one resident entry for that block ID, and return the block ID.

### REQ-MEM-STORE-006

`get` shall return `Ok(None)` when a requested block ID is not currently
resident.

When a requested block ID is resident, `get` shall validate the resident bytes
against the requested block ID before reporting success.

### REQ-MEM-STORE-007

`iter_block_ids` shall enumerate the set of block IDs currently resident in
memory without exposing backend details beyond the parent trait surface.

### REQ-MEM-STORE-008

Successful direct `get` and `put` operations on resident entries shall refresh
their least-recently-used recency.

### REQ-MEM-STORE-009

When inserting or promoting a block would exceed configured resident capacity,
the implementation shall evict the least-recently-used resident block before
reporting success.

### REQ-MEM-STORE-010

The memory block-store crate may implement the overlay crate's optional
notification trait so it can populate or refresh cache residency after a
completed overlay `get` returns `Ok(Some(validated_block))`.

### REQ-MEM-STORE-011

Notification-driven cache population in this revision shall occur only for
successful completed `get` outcomes and shall not occur for `get` miss, `get`
error, or any `put` outcome.

### REQ-MEM-STORE-012

The repository shall include automated verification artifacts that realize the
validation surface defined in `docs/specs/rust-memory-block-store/`, including
reuse of the parent trait crate's conformance helpers where applicable.

## Out of Scope

This crate does not define or own:

- durable write-through or write-back propagation to lower layers
- negative caching
- byte-budgeted eviction
- cross-process or shared-memory cache coherence
- background refresh or prefetch behavior
- changes to the parent `BlockStore` trait
- changes to overlay `put` dispatch semantics

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/`,
`docs/specs/rust-block-storage-trait/`, and
`docs/specs/rust-overlay-block-store/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
