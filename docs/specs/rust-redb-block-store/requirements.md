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
this repository, including narrowly scoped concrete-type administrative
surfaces that remain outside the shared trait boundary.

## Terminology

In this spec package, `store root` means the filesystem directory supplied to
the implementation as the root under which the backend owns its Redb database
state.

`Committed block entry` means one Redb key/value entry whose key is the block ID
and whose value is the canonical block bytes retained for that block.

`Durability mode` means the constructor-selected persistence policy that controls
when Redb-backed writes must be flushed to stable storage.

`Fast mode` means the opt-in durability mode in which ordinary `put` operations
may complete without forcing the usual per-write flush, with pending writes
instead flushed during graceful shutdown when the final store handle drops.

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

When this crate exposes Redb-specific administrative functionality, it shall do
so without widening, replacing, or redefining the shared `BlockStore`
contract.

### REQ-REDB-STORE-003

Construction shall accept a caller-supplied store-root directory, a
caller-selectable access mode, and a caller-selectable durability mode outside
the `BlockStore` trait boundary.

The crate may also expose backend-specific administrative methods on the
concrete `RedbBlockStore` type outside the `BlockStore` trait boundary.

Construction shall either return an initialized store rooted at a canonical
directory path or fail explicitly as a backend failure when the requested root
cannot be created, canonicalized, stat'ed, resolved to a non-directory, or
used to initialize or open the backend-owned Redb database state.

When the caller does not explicitly select writable fast mode, construction
shall retain the crate's default writable durable behavior.

### REQ-REDB-STORE-004

The Redb-backed implementation shall retain its database state beneath the
configured store root without requiring callers to know the backend-owned
database file path, table names, key encoding, page layout, or other Redb
details.

### REQ-REDB-STORE-005

`put` shall derive the canonical block bytes and block ID through the block
crate and persist those bytes keyed by block ID in Redb-backed local storage.

In the default durability mode, successful committed writes shall remain
durably observable through later store instances opened on the same store root
without depending on any deferred graceful-shutdown flush beyond the successful
`put` itself.

In fast mode, successful `put` operations shall preserve the same block-store
correctness and integrity behavior as the default mode, but later store
instances opened on the same store root are required to observe those writes
only after the fast-mode graceful-shutdown flush has completed.

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

Read-only open requests that cannot be satisfied without mutating persisted
state shall also fail explicitly.

Attempted mutating operations through a read-only store handle shall fail
explicitly.

### REQ-REDB-STORE-011

The repository shall include automated verification artifacts that realize the
validation surface defined in `docs/specs/rust-redb-block-store/`, including
reuse of the parent trait crate's conformance helpers where applicable.

The verification surface shall cover both default durable mode and fast mode,
including mode selection, ordinary-write behavior, graceful-shutdown flush
behavior, and the crash-durability boundary for fast mode.

The verification surface shall also cover read-only open behavior, read-only
read and enumeration behavior, explicit refusal of mutating operations through
read-only handles, failure behavior for recovery-required databases, and the
absence of dirty-header side effects from read-only open.

### REQ-REDB-STORE-012

When constructed in fast mode, ordinary successful `put` operations shall not
force the default per-write flush path before returning success.

### REQ-REDB-STORE-013

When constructed in fast mode, any pending successfully written block entries
shall be flushed during graceful shutdown when the final `RedbBlockStore`
handle for that store instance is dropped.

### REQ-REDB-STORE-014

Fast mode shall weaken durability only until the graceful-shutdown flush
required by `REQ-REDB-STORE-013`.

Fast mode shall not guarantee that writes survive abnormal termination, process
crash, abort, or power loss before that graceful-shutdown flush occurs.

### REQ-REDB-STORE-015

The Redb block-store crate shall expose a caller-invocable `compact_now`
operation on the concrete `RedbBlockStore` type for requesting immediate Redb
file compaction.

This operation shall remain Redb-specific and shall not be surfaced through
`BlockStore` or any repository-wide backend-neutral storage trait in this
revision.

### REQ-REDB-STORE-016

The `compact_now` operation shall require exclusive ownership of the targeted
store instance.

If the store has shared or cloned ownership, or otherwise cannot satisfy the
exclusive-ownership precondition required by the Redb-backed implementation, the
operation shall fail explicitly as a backend failure and shall not silently
return success or no-op.

### REQ-REDB-STORE-017

When `compact_now` is invoked under valid preconditions, the implementation
shall preserve the inherited block-store correctness and integrity contract
while compacting backend-private Redb storage.

The operation shall surface compaction and pre-compaction Redb failures
explicitly through the existing error taxonomy and shall not expose Redb
database handles, table definitions, or other backend-private storage details
to callers.

### REQ-REDB-STORE-018

The Redb-backed implementation shall opt in to the shared batch-write
capability for caller-supplied block identifiers and canonical block bytes.

### REQ-REDB-STORE-019

The Redb-backed batch-write operation shall commit atomically.

If any requested batch entry encounters a backend failure, a conflicting
already-persisted value, or another explicit inherited block-store failure,
none of the entries in that batch operation shall become committed by that
operation.

### REQ-REDB-STORE-020

Within a successful Redb-backed batch-write operation, entries already present
with identical bytes shall remain idempotent, and entries absent at batch start
shall become persisted under their supplied block identifiers.

The implementation shall not silently overwrite divergent already-persisted
bytes.

### REQ-REDB-STORE-021

Successful Redb-backed batch writes shall inherit the existing durability-mode
rules.

In the default durability mode, successful committed batches shall remain
durably observable through later store instances opened on the same store root
without depending on a deferred graceful-shutdown flush beyond the successful
batch operation itself.

In fast mode, successful batch writes shall preserve the same correctness and
integrity behavior as the default mode, but later store instances opened on the
same store root are required to observe those writes only after the fast-mode
graceful-shutdown flush has completed.

### REQ-REDB-STORE-022

When the shared block-store telemetry callback capability is configured, the
Redb-backed implementation shall translate Redb repair and status updates into
the shared generic telemetry event representation.

### REQ-REDB-STORE-023

Redb-backed telemetry emission shall remain observational only.

In this revision, telemetry observers shall not gain authority to steer, abort,
or otherwise alter Redb repair behavior through the shared callback surface.

### REQ-REDB-STORE-024

Redb-specific telemetry payloads shall be expressible through the shared event
name, optional message, and structured name/value attributes without exposing
raw Redb repair-session types in the stable caller contract.

### REQ-REDB-STORE-025

The Redb block-store crate shall expose an explicit read-only open mode on the
concrete `RedbBlockStore` type outside the shared `BlockStore` trait boundary.

### REQ-REDB-STORE-026

When opened in read-only mode, the Redb-backed implementation shall permit
ordinary read and enumeration behavior without mutating the database state
beneath the configured store root.

Read-only open shall not mark the on-disk Redb database dirty, publish a new
repair obligation, or otherwise require a later writable open merely because a
client opened the store read-only and then terminated unexpectedly.

Read-only open shall use Redb's native read-only database mechanism rather than
loading the complete database file into a backend-owned heap buffer.

### REQ-REDB-STORE-027

If the Redb database is already marked recovery-required at read-only open
time, the implementation shall either open successfully without performing
repair when the backend supports that behavior safely, or fail explicitly
without mutating the persisted database state.

This revision shall not silently fall back from a requested read-only open into
writable repair behavior.

The native read-only open shall preserve Redb's file-locking semantics. On
platforms where Redb supports file locking, a read-only handle shall fail
explicitly when a writable database handle is already open for the same file.

### REQ-REDB-STORE-028

When a store is opened in read-only mode, mutating operations including `put`,
raw-byte batch persistence, and the Redb-specific `compact_now` maintenance
operation shall fail explicitly.

These operations shall not silently succeed, no-op, partially commit, or
upgrade the store to writable behavior.

### REQ-REDB-STORE-029

The Redb-backed implementation shall represent read-only and read-write
database handles without exposing Redb-native handles through the public
`RedbBlockStore` API. Read operations shall work through the shared readable
database behavior, while write and maintenance operations shall remain
restricted to read-write handles.

### REQ-REDB-STORE-030

Read-only construction and ordinary read operations shall not require a memory
allocation proportional to the complete persisted database file size. Memory
usage shall follow Redb's native file-backed page and cache behavior.

## Out of Scope

This crate does not define or own:

- block canonicalization, block validity, or block-ID derivation rules
- backend-neutral query, delete, compaction, or maintenance APIs beyond the
  parent trait
- Redb-specific maintenance surfaces other than the concrete-type
  `compact_now` operation defined by this revision
- raw Redb repair-session control surfaces at the shared telemetry boundary
- cache-mode byte-budget semantics in this revision
- consumer-facing integration with evaluator, CLI, or benchmark-profile store
  selection in this revision
- repository-wide requirement that every `BlockStore` backend implement the
  optional shared batch-write capability
- repository-wide backend-neutral read-only open semantics in this revision

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
