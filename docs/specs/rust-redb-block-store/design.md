<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Redb Block Store Design

## Status

Draft design specification for a Rust crate that implements the LexonGraph
block-storage contract using Redb-backed durable local storage.

## Design Goals

The crate design is intended to be:

- subordinate to the backend-neutral `BlockStore` boundary
- explicit about Redb-backed durable local persistence
- narrow at the public API boundary
- strict about inherited integrity and failure rules
- suitable as a standalone repository-owned backend

## Crate Boundary

The crate owns:

- Redb-specific realization of `put`, `get`, and identifier enumeration
- store-root construction behavior
- backend-private Redb database initialization under the supplied root
- Redb-specific error mapping

The crate does not own:

- block canonicalization or block-ID derivation
- block validation rules beyond invoking the block crate
- any consumer-facing API wider than the parent `BlockStore` contract plus
  store construction
- evaluator, CLI, or benchmark-profile integration in this revision
- cache-mode or bounded-eviction behavior in this revision

## External Dependencies

### DSG-REDB-STORE-001 `Parent dependencies`

The Redb block-store crate depends on:

- the block crate for canonical serialization, block-ID derivation, and
  validated decoding
- the block-storage trait crate for the `BlockStore` trait and shared error
  taxonomy
- the `redb` crate for durable local key/value persistence

The Redb block-store crate does not redefine those behaviors.

## Core Types

### DSG-REDB-STORE-002 `RedbBlockStore`

The crate defines `RedbBlockStore` as a concrete `BlockStore`
implementation that owns:

- a canonicalized store-root directory path
- one initialized Redb database handle bound to backend-private state below
  that root

Cloned `RedbBlockStore` values share the same underlying Redb database handle.

### DSG-REDB-STORE-003 `Construction boundary`

The crate exposes a constructor that accepts a store-root directory path.

Construction:

1. creates the requested directory when needed
2. canonicalizes the resolved directory path
3. verifies that the resolved path is a directory
4. initializes or opens one backend-private Redb database file below that root
5. ensures the block table exists before returning an initialized store

Failures to create the root, canonicalize it, stat it, confirm that it is a
directory, open the database, or initialize the block table map to explicit
backend failures.

### DSG-REDB-STORE-004 `Backend-private storage model`

This revision uses one backend-private Redb database file below the store root.

Within that database, one backend-private table maps:

- key: raw 32-byte block ID
- value: canonical block bytes

The database file name, table name, key representation, and any Redb page-level
layout remain implementation details and do not cross the parent trait
boundary.

## Runtime Behavior

### DSG-REDB-STORE-005 `put`

`put`:

1. canonicalizes the input block through the block crate
2. derives the block ID from the canonical bytes
3. opens a Redb write transaction
4. inspects the existing value, if any, for that block ID
5. returns success without mutation when the existing value already matches the
   canonical bytes
6. fails explicitly as a backend failure describing integrity conflict when the
   existing value differs
7. otherwise inserts the canonical bytes under the block ID
8. commits the transaction before reporting success

Successful committed writes remain observable through later store instances
opened on the same store root.

### DSG-REDB-STORE-006 `get`

`get`:

1. opens a Redb read transaction
2. looks up the requested block ID in the backend-private table
3. returns `Ok(None)` when no value is present
4. clones the stored bytes when a value is present
5. delegates decode and block-ID verification to the parent helper path layered
   above `get_block_bytes`

Malformed bytes and block-ID mismatch remain inherited decode failures through
the parent trait helper layer.

### DSG-REDB-STORE-007 `iter_block_ids`

`iter_block_ids` opens a Redb read transaction, snapshots the current set of
persisted keys by iterating the backend-private block table, decodes each
32-byte key into a `BlockHash`, and streams only those block IDs to callers.

If iteration encounters a Redb failure or a persisted key whose bytes cannot be
decoded as one block ID, enumeration fails explicitly as a backend failure.

### DSG-REDB-STORE-008 `Public boundary and error mapping`

The public API is limited to store construction plus the parent `BlockStore`
trait implementation.

The crate does not expose Redb database handles, table definitions, transaction
objects, file paths below the store root, page metadata, or Redb-native query
surfaces to callers.

Database-open, transaction, read, write, commit, and iteration failures map to
explicit backend failures through the parent error taxonomy.

## Verification Strategy

### DSG-REDB-STORE-009 `Conformance and backend-specific verification`

The crate reuses the parent block-store conformance helpers to verify the shared
`put`, `get`, and identifier-enumeration contract.

The crate adds backend-specific tests for:

- constructor success on a caller-supplied store root
- explicit constructor failure for invalid or unusable roots
- successful durable round-trip through `put` and `get`
- explicit absence for missing block IDs
- explicit malformed-content and integrity-mismatch failures via injected raw
  bytes
- explicit integrity-conflict failure for conflicting existing bytes
- visibility of committed writes after reopening the same store root
- enumeration of persisted block IDs only
- explicit failure for malformed persisted block-ID keys
- backend-neutral public API boundary

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-REDB-STORE-001 | REQ-REDB-STORE-001, REQ-REDB-STORE-002 |
| DSG-REDB-STORE-002..004 | REQ-REDB-STORE-001, REQ-REDB-STORE-003, REQ-REDB-STORE-004 |
| DSG-REDB-STORE-005 | REQ-REDB-STORE-005, REQ-REDB-STORE-008 |
| DSG-REDB-STORE-006 | REQ-REDB-STORE-006, REQ-REDB-STORE-007 |
| DSG-REDB-STORE-007 | REQ-REDB-STORE-009, REQ-REDB-STORE-010 |
| DSG-REDB-STORE-008 | REQ-REDB-STORE-002, REQ-REDB-STORE-004, REQ-REDB-STORE-010 |
| DSG-REDB-STORE-009 | REQ-REDB-STORE-011 |
