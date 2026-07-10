<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Memory Block Store Design

## Status

Draft design specification for a Rust crate that implements the LexonGraph
block-storage contract using volatile in-memory residency.

## Design Goals

The crate design is intended to be:

- subordinate to the backend-neutral `BlockStore` boundary
- explicit about its volatile durability boundary
- bounded by caller-supplied resident capacity
- strict about inherited integrity and failure rules
- reusable as a standalone backend or as an overlay-managed layer

## Crate Boundary

The crate owns:

- memory-specific realization of `put`, `get`, and identifier enumeration
- resident-capacity construction behavior
- in-memory least-recently-used tracking and eviction
- no overlay-specific callback or notification surface

The crate does not own:

- block canonicalization or block-ID derivation
- block validation rules beyond invoking the block crate
- any durability guarantee beyond the current process memory image
- changes to overlay `put` dispatch semantics or lower-layer write guarantees

## External Dependencies

### DSG-MEM-STORE-001 `Parent dependencies`

The memory block-store crate depends on:

- the block crate for canonical serialization, block-ID derivation, and
  validated decoding
- the block-storage trait crate for the `BlockStore` trait and shared error
  taxonomy

The memory block-store crate does not redefine those behaviors.

## Core Types

### DSG-MEM-STORE-002 `MemoryBlockStore`

The crate defines `MemoryBlockStore` as a concrete `BlockStore`
implementation configured with a maximum resident block count.

Cloned `MemoryBlockStore` values share one underlying resident state so the
same logical cache instance can participate as both a store and an overlay
notification observer.

### DSG-MEM-STORE-003 `Resident state`

Resident state is keyed by `BlockHash` and stores:

- canonical block bytes for each resident block
- recency metadata sufficient to identify the least-recently-used resident
  entry

### DSG-MEM-STORE-004 `Construction boundary`

The crate exposes a constructor that accepts `max_resident_blocks`.

Construction fails explicitly when `max_resident_blocks` is zero.

The crate also exposes an opt-in cache-mode constructor that accepts an MB
budget and converts it to a payload-byte budget using `1 MB = 1,048,576 bytes`.

Cache-mode construction fails explicitly when the requested MB budget is zero or
cannot be converted to the corresponding byte budget.

## Runtime Behavior

### DSG-MEM-STORE-005 `put`

`put`:

1. canonicalizes the supplied block through the block crate
2. inserts or replaces the resident bytes at the derived block ID
3. refreshes that entry's recency
4. evicts one least-recently-used resident entry if the insert would otherwise
   exceed configured capacity
5. returns the derived block ID

Repeated `put` of the same logical block refreshes recency and preserves one
resident entry for that block ID.

### DSG-MEM-STORE-006 `get`

`get`:

1. returns `Ok(None)` when the requested block ID is not resident
2. clones resident bytes for the requested block ID when present
3. validates those bytes against the requested block ID through the block crate
4. returns `Ok(Some(validated_block))` only for resident, valid, matching
   content
5. refreshes recency only after successful validation

Malformed resident bytes map to malformed-content failures, and resident bytes
whose verified identity differs from the requested block ID map to
integrity-mismatch failures.

### DSG-MEM-STORE-007 `iter_block_ids`

`iter_block_ids` snapshots the currently resident block-ID set and streams those
identifiers without promising ordering.

### DSG-MEM-STORE-008 `LRU eviction`

Capacity is measured in resident block count only.

When a new resident insertion would exceed configured capacity, the
least-recently-used resident entry is evicted before success is reported.

Recency is refreshed by successful direct `get`, successful direct `put`, and
successful overlay-notified `get` hit promotion.

In cache mode, capacity is instead measured in canonical payload bytes retained
for resident entries.

When a direct cache-mode `put` would exceed the configured byte budget, the
implementation repeatedly evicts the least-recently-used resident entry until
the incoming block fits or the resident set is exhausted.

### DSG-MEM-STORE-009 `Oversize direct-write rejection`

If one block's canonical payload bytes exceed the total configured cache-mode
byte budget, the direct cache write fails explicitly without mutating the
resident set.

### DSG-MEM-STORE-010 `Overlay notification integration`

Overlay compositions interact with this crate through ordinary `put`, `get`,
and `iter_block_ids` only.

If an overlay chooses to refill a higher-level cache after a lower-layer read
hit, it does so by calling the ordinary `put` contract owned by this crate.

### DSG-MEM-STORE-011 `Durability boundary`

`MemoryBlockStore` remains a standalone volatile backend.

When used in an overlay composition, direct `put` success into this store does
not imply that any lower durable layer has accepted the write. Durable write
guarantees remain owned by overlay composition choices outside this crate.

## Verification Strategy

### DSG-MEM-STORE-012 `Conformance and cache verification`

The crate reuses the parent block-store conformance helpers to verify the shared
`put`, `get`, and identifier-enumeration contract.

The crate adds backend-specific tests for:

- zero-capacity constructor failure
- resident enumeration
- least-recently-used eviction under direct access
- cache-mode byte-budget eviction under direct access
- cache-mode rejection of direct writes larger than the total byte budget
- absence of any overlay-specific callback surface on the store itself

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-MEM-STORE-001 | REQ-MEM-STORE-001, REQ-MEM-STORE-002 |
| DSG-MEM-STORE-002..004 | REQ-MEM-STORE-001, REQ-MEM-STORE-003, REQ-MEM-STORE-004, REQ-MEM-STORE-013 |
| DSG-MEM-STORE-005 | REQ-MEM-STORE-005, REQ-MEM-STORE-008 |
| DSG-MEM-STORE-006 | REQ-MEM-STORE-006, REQ-MEM-STORE-008 |
| DSG-MEM-STORE-007 | REQ-MEM-STORE-007 |
| DSG-MEM-STORE-008 | REQ-MEM-STORE-008, REQ-MEM-STORE-009, REQ-MEM-STORE-014, REQ-MEM-STORE-015 |
| DSG-MEM-STORE-009 | REQ-MEM-STORE-016 |
| DSG-MEM-STORE-010 | REQ-MEM-STORE-010, REQ-MEM-STORE-011, REQ-MEM-STORE-017 |
| DSG-MEM-STORE-011 | REQ-MEM-STORE-004, REQ-MEM-STORE-010 |
| DSG-MEM-STORE-012 | REQ-MEM-STORE-012 |
