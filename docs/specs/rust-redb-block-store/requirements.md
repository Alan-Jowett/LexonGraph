<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Redb Block Store Requirements

## Status

Draft specification for a Rust crate that implements the LexonGraph
block-storage contract using Redb-backed durable local storage.

## Scope

This document specifies implementation-specific requirements for a Rust crate
that realizes `docs/specs/rust-block-storage-trait/` over a local Redb
database rooted at a caller-supplied store directory.

This document is layered on top of:

- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`

This document does not redefine the parent `BlockStore` contract. It adds only
Redb-backend-specific requirements needed to realize a durable local backend in
this repository.

## Terminology

In this spec package, `store root` means the filesystem directory supplied to
the implementation as the root under which the backend owns its Redb database
state.

`Committed block entry` means one Redb key/value entry whose key is the block ID
and whose value is the canonical block bytes retained for that block.

## Requirements

### REQ-REDB-STORE-001

The repository shall include a Rust crate, separate from
`crates/lexongraph-block-store`, that implements the parent `BlockStore`
contract using Redb-backed durable local storage.

### REQ-REDB-STORE-002

The Redb block-store crate shall remain subordinate to
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`, and
`docs/specs/rust-block-storage-trait/` for block identity, validation, and the
backend-neutral `BlockStore` contract.

### REQ-REDB-STORE-003

Construction shall accept a caller-supplied store-root directory outside the
`BlockStore` trait boundary.

Construction shall either return an initialized store rooted at a canonical
directory path or fail explicitly as a backend failure when the requested root
cannot be created, canonicalized, stat'ed, resolved to a non-directory, or
used to initialize or open the backend-owned Redb database state.

### REQ-REDB-STORE-004

The Redb-backed implementation shall retain its database state beneath the
configured store root without requiring callers to know the backend-owned
database file path, table names, key encoding, page layout, or other Redb
details.

### REQ-REDB-STORE-005

`put` shall derive the canonical block bytes and block ID through the block
crate and persist those bytes keyed by block ID in Redb-backed durable local
storage.

Successful committed writes shall remain observable through later store
instances opened on the same store root.

### REQ-REDB-STORE-006

`get` shall return `Ok(None)` when a requested block ID is absent.

When bytes are present for the requested block ID, `get` shall validate the
retrieved bytes against the requested block ID before reporting success.

### REQ-REDB-STORE-007

If retrieved bytes are malformed, protocol-invalid, or verify to a block ID
different from the requested block ID, the Redb-backed implementation shall
fail explicitly and shall not treat those conditions as success or absence.

### REQ-REDB-STORE-008

Repeated `put` of the same logical block shall remain idempotent.

If `put` encounters already-persisted bytes at the target block ID that differ
from the canonical bytes being stored, it shall fail explicitly as a backend
failure describing corruption or integrity conflict and shall not silently
overwrite those bytes.

### REQ-REDB-STORE-009

The Redb-backed implementation shall implement the parent trait's streaming
block-ID enumeration over persisted block entries.

Enumeration shall expose only block identifiers and shall not expose Redb
tables, key encodings, pages, or other backend-private details.

### REQ-REDB-STORE-010

The Redb-backed implementation shall surface explicit backend failures for
database open, transaction, read, write, and iteration errors and shall not
silently skip unreadable or undecodable persisted state as though the
operation succeeded.

### REQ-REDB-STORE-011

The repository shall include automated verification artifacts that realize the
validation surface defined in `docs/specs/rust-redb-block-store/`, including
reuse of the parent trait crate's conformance helpers where applicable.

## Out of Scope

This crate does not define or own:

- block canonicalization, block validity, or block-ID derivation rules
- consumer-facing query, delete, compaction, or maintenance APIs beyond the
  parent trait
- cache-mode byte-budget semantics in this revision
- consumer-facing integration with evaluator, CLI, or benchmark-profile store
  selection in this revision
- changes to the parent `BlockStore` trait

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
