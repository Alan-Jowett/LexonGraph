<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Redb Block Store Validation

## Status

Draft validation specification for a Rust crate that implements the LexonGraph
block-storage contract using Redb-backed durable local storage.

## Validation Scope

These validation entries define the expected verification surface for the
Redb-backed implementation in addition to the parent protocol, block, and
block-store trait requirements it depends on.

## Validation Entries

### VAL-REDB-STORE-001

Construct the Redb-backed store from a caller-supplied store-root directory that
does not yet exist.

**Pass condition:** construction succeeds, initializes the backend-owned Redb
database state beneath that root, and does not require the caller to know
backend-private database details.

**Traces to:** REQ-REDB-STORE-001, REQ-REDB-STORE-003, REQ-REDB-STORE-004

### VAL-REDB-STORE-002

Attempt to construct the Redb-backed store from a non-directory root and from a
store root whose backend-private database path cannot be opened as a Redb
database file.

**Pass condition:** construction fails explicitly as a backend failure.

**Traces to:** REQ-REDB-STORE-003, REQ-REDB-STORE-010

### VAL-REDB-STORE-003

Store a valid block through `put`, then retrieve it through `get`.

**Pass condition:** round-trip succeeds with the same block ID and validated
block content.

**Traces to:** REQ-REDB-STORE-005, REQ-REDB-STORE-006

### VAL-REDB-STORE-004

Request a block ID that is not present in the Redb-backed store.

**Pass condition:** `get` returns `Ok(None)`.

**Traces to:** REQ-REDB-STORE-006

### VAL-REDB-STORE-005

Persist malformed or protocol-invalid bytes under an otherwise valid block ID.

**Pass condition:** `get` fails explicitly with a malformed-content error.

**Traces to:** REQ-REDB-STORE-006, REQ-REDB-STORE-007

### VAL-REDB-STORE-006

Persist valid block bytes under the block ID of a different block.

**Pass condition:** `get` fails explicitly with an integrity-mismatch error.

**Traces to:** REQ-REDB-STORE-006, REQ-REDB-STORE-007

### VAL-REDB-STORE-007

Persist conflicting bytes for a block ID, then attempt `put` for the canonical
block bytes of that same block ID.

**Pass condition:** `put` fails explicitly as a backend failure describing
integrity conflict and does not overwrite the conflicting bytes.

**Traces to:** REQ-REDB-STORE-008

### VAL-REDB-STORE-008

Store a valid block, drop the first store instance, then reopen a new store
instance on the same store root and retrieve the block.

**Pass condition:** the committed block remains observable after reopening.

**Traces to:** REQ-REDB-STORE-005

### VAL-REDB-STORE-009

Persist multiple valid blocks, then enumerate block IDs.

**Pass condition:** enumeration yields the persisted block IDs only.

**Traces to:** REQ-REDB-STORE-009

### VAL-REDB-STORE-010

Persist a key in the backend-private table whose bytes cannot decode into one
block ID, then enumerate block IDs.

**Pass condition:** enumeration fails explicitly as a backend failure rather
than silently skipping the malformed persisted state.

**Traces to:** REQ-REDB-STORE-009, REQ-REDB-STORE-010

### VAL-REDB-STORE-011

Run the parent block-store conformance suite against the Redb-backed
implementation.

**Pass condition:** the backend satisfies the shared `put`/`get`/enumeration
contract.

**Traces to:** REQ-REDB-STORE-011

### VAL-REDB-STORE-012

Inspect the Redb-backed implementation's public and verification surface.

**Pass condition:** the repository includes automated verification artifacts for
the approved backend behavior, remains subordinate to `docs/protocol/blocks.md`,
`docs/specs/rust-block-crate/`, and `docs/specs/rust-block-storage-trait/` for
their owned concerns, and exposes the backend to callers through store
construction plus the ordinary `BlockStore` contract rather than Redb-native
runtime surfaces.

**Traces to:** REQ-REDB-STORE-002, REQ-REDB-STORE-004, REQ-REDB-STORE-011
