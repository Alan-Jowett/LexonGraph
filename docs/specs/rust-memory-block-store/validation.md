<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Memory Block Store Validation

## Status

Draft validation specification for a Rust crate that implements the LexonGraph
block-storage contract using volatile in-memory residency.

## Validation Scope

These validation entries define the expected verification surface for the
memory-backed implementation in addition to the parent protocol, block, and
block-store trait requirements it depends on, including the overlay
notification semantics layered in from `docs/specs/rust-overlay-block-store/`.

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

Deliver a completed overlay `get` hit notification to an empty store.

**Pass condition:** the returned block becomes resident and is subsequently
retrievable by direct `get`.

**Traces to:** REQ-MEM-STORE-010, REQ-MEM-STORE-011

### VAL-MEM-STORE-010

Deliver a completed overlay `get` hit notification for an already resident
block, then force capacity pressure.

**Pass condition:** the notified block's recency is refreshed and a different
least-recently-used block is evicted.

**Traces to:** REQ-MEM-STORE-008, REQ-MEM-STORE-009, REQ-MEM-STORE-010,
REQ-MEM-STORE-011

### VAL-MEM-STORE-011

Deliver completed overlay `get` miss and `get` error notifications.

**Pass condition:** neither notification creates resident state.

**Traces to:** REQ-MEM-STORE-011

### VAL-MEM-STORE-012

Deliver completed overlay `put` success and `put` error notifications.

**Pass condition:** neither notification creates or refreshes resident state.

**Traces to:** REQ-MEM-STORE-011

### VAL-MEM-STORE-013

Run the parent block-store conformance suite against the memory-backed
implementation.

**Pass condition:** the backend satisfies the shared `put`/`get`/enumeration
contract.

**Traces to:** REQ-MEM-STORE-012

### VAL-MEM-STORE-014

Inspect the implementation's public and behavioral boundary.

**Pass condition:** the crate exposes a standalone volatile backend with
optional overlay read-population support, remains subordinate to
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`,
`docs/specs/rust-block-storage-trait/`, and
`docs/specs/rust-overlay-block-store/` for their owned concerns, and does not
claim durable lower-layer write propagation.

**Traces to:** REQ-MEM-STORE-002, REQ-MEM-STORE-004, REQ-MEM-STORE-010,
REQ-MEM-STORE-011, REQ-MEM-STORE-012
