<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Memory Block Store Validation

## Status

Draft validation specification for a Rust crate that implements the LexonGraph
block-storage contract using volatile in-memory residency.

## Validation Scope

These validation entries define the expected verification surface for the
memory-backed implementation in addition to the parent protocol, block, and
block-store trait requirements it depends on.

## Validation Entries

### VAL-MEM-STORE-001

Construct the memory-backed store with a positive resident capacity.

**Pass condition:** construction succeeds.

**Traces to:** REQ-MEM-STORE-001, REQ-MEM-STORE-003

### VAL-MEM-STORE-002

Construct the memory-backed store with zero resident capacity.

**Pass condition:** construction fails explicitly.

**Traces to:** REQ-MEM-STORE-003

### VAL-MEM-STORE-003

Store a valid block through `put`, then retrieve it through `get`.

**Pass condition:** round-trip succeeds with the same block ID.

**Traces to:** REQ-MEM-STORE-005, REQ-MEM-STORE-006

### VAL-MEM-STORE-004

Store the same logical block multiple times.

**Pass condition:** each `put` returns the same block ID and the store retains
one resident entry for that block ID.

**Traces to:** REQ-MEM-STORE-005, REQ-MEM-STORE-008

### VAL-MEM-STORE-005

Request a non-resident block ID.

**Pass condition:** `get` returns `Ok(None)`.

**Traces to:** REQ-MEM-STORE-006

### VAL-MEM-STORE-006

Store multiple valid blocks, then enumerate resident block IDs.

**Pass condition:** enumeration yields the currently resident IDs only.

**Traces to:** REQ-MEM-STORE-007

### VAL-MEM-STORE-007

Fill the store to capacity, refresh one resident block with a successful direct
`get`, then insert a new block.

**Pass condition:** the least-recently-used non-refreshed block is evicted.

**Traces to:** REQ-MEM-STORE-008, REQ-MEM-STORE-009

### VAL-MEM-STORE-008

Fill the store to capacity, then insert a new block without refreshing older
entries.

**Pass condition:** the least-recently-used block is evicted before success is
reported.

**Traces to:** REQ-MEM-STORE-009

### VAL-MEM-STORE-009

Inspect the store boundary when composing it with higher-level overlay policy.

**Pass condition:** the memory store remains usable through the ordinary
`BlockStore` contract without requiring overlay-specific notification or
callback surfaces.

**Traces to:** REQ-MEM-STORE-010, REQ-MEM-STORE-011

### VAL-MEM-STORE-010

Run the parent block-store conformance suite against the memory-backed
implementation.

**Pass condition:** the backend satisfies the shared `put`/`get`/enumeration
contract.

**Traces to:** REQ-MEM-STORE-012

### VAL-MEM-STORE-011

Inspect the implementation's public and behavioral boundary.

**Pass condition:** the crate exposes a standalone volatile bounded backend,
remains subordinate to `docs/protocol/blocks.md`,
`docs/specs/rust-block-crate/`, and `docs/specs/rust-block-storage-trait/` for
their owned concerns, and does not claim overlay-managed cache refill policy as
part of its own API boundary. Its public capacity-introspection surface also
distinguishes count-bounded and byte-bounded construction modes without
requiring callers to infer mode from sentinel values.

**Traces to:** REQ-MEM-STORE-002, REQ-MEM-STORE-004, REQ-MEM-STORE-010,
REQ-MEM-STORE-011, REQ-MEM-STORE-012, REQ-MEM-STORE-018

### VAL-MEM-STORE-012

Construct the opt-in cache-mode memory store with a positive MB budget and with
zero MB.

**Pass condition:** positive MB construction succeeds, zero MB construction
fails explicitly, and cache-mode behavior uses `1 MB = 1,048,576 bytes` for the
configured payload-byte budget.

**Traces to:** REQ-MEM-STORE-013

### VAL-MEM-STORE-013

Fill the cache-mode memory store close to its byte budget, refresh one resident
entry with a successful `get`, then insert another block whose payload bytes
require eviction.

**Pass condition:** the least-recently-used non-refreshed resident entry is
evicted according to payload-byte pressure rather than resident-entry count.

**Traces to:** REQ-MEM-STORE-014, REQ-MEM-STORE-015

### VAL-MEM-STORE-014

Attempt to store one block whose canonical payload bytes exceed the total
cache-mode byte budget.

**Pass condition:** the direct cache write fails explicitly, the block is not
cached, and the existing resident set remains unchanged.

**Traces to:** REQ-MEM-STORE-016

### VAL-MEM-STORE-015

Compose the cache-mode memory store as an overlay cache layer and inspect its
boundary.

**Pass condition:** byte-budgeted eviction and oversize rejection are realized
through ordinary `put`, `get`, and `iter_block_ids` only, without any
overlay-specific callback or delete surface.

**Traces to:** REQ-MEM-STORE-017
