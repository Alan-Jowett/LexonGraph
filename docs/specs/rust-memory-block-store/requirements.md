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

This document does not redefine the parent `BlockStore` contract. It adds only
memory-backend-specific requirements needed to realize a volatile bounded store
in this repository.

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
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`, and
`docs/specs/rust-block-storage-trait/` for block identity, validation, and the
backend-neutral `BlockStore` contract.

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

The memory block-store crate shall remain usable as either a writable layer or
cache layer when composed by the overlay crate, without requiring any
store-specific interface beyond the parent `BlockStore` contract.

### REQ-MEM-STORE-011

Overlay-managed cache refill, direct-write routing, and write-back policy shall
remain outside this crate.

This crate shall not expose notification or callback surfaces beyond the parent
`BlockStore` contract.

### REQ-MEM-STORE-012

The repository shall include automated verification artifacts that realize the
validation surface defined in `docs/specs/rust-memory-block-store/`, including
reuse of the parent trait crate's conformance helpers where applicable.

### REQ-MEM-STORE-013

The memory block-store crate shall offer an opt-in cache-mode construction path
outside the `BlockStore` trait boundary that configures a payload-byte budget in
MB, where `1 MB = 1,048,576 bytes`.

Construction shall fail explicitly when the requested cache-mode MB budget is
zero or cannot be converted into the corresponding byte budget.

### REQ-MEM-STORE-014

The cache-mode memory block-store shall account only canonical block payload
bytes retained in resident entries against the configured byte budget.

The byte budget shall not include allocator overhead, hash-map bookkeeping, or
other process-local metadata.

### REQ-MEM-STORE-015

When a direct cache-mode `put` would exceed the configured byte budget, the
implementation shall evict least-recently-used resident entries until the new
 block fits and success can be reported.

### REQ-MEM-STORE-016

If one block's canonical payload bytes exceed the entire configured cache-mode
byte budget, the implementation shall reject that direct cache write
explicitly, shall not cache the block, and shall leave the existing resident set
unchanged.

### REQ-MEM-STORE-017

The cache-mode construction path shall remain usable as an overlay cache layer
through the ordinary `BlockStore` contract only.

This crate shall not require overlay-specific notification, delete, or eviction
callbacks beyond ordinary `put`, `get`, and `iter_block_ids`.

### REQ-MEM-STORE-018

The memory block-store crate shall expose a public capacity-introspection surface
that distinguishes count-bounded and byte-bounded construction modes.

That surface shall report the configured resident-block count for ordinary
construction and the configured resident-byte budget for cache-mode
construction without requiring callers to infer mode from sentinel values.

## Out of Scope

This crate does not define or own:

- durable write-through or write-back propagation to lower layers
- negative caching
- byte-budgeted eviction outside the opt-in cache-mode construction path
- cross-process or shared-memory cache coherence
- background refresh or prefetch behavior
- changes to the parent `BlockStore` trait
- overlay-managed cache refill or direct-write policy

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/`,
`docs/specs/rust-block-storage-trait/` specification package for its owned
concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
