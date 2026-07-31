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
- access-mode selection and lifecycle behavior at construction time
- durability-mode selection and lifecycle behavior at construction time
- backend-private Redb database initialization under the supplied root
- Redb-specific error mapping

The crate does not own:

- block canonicalization or block-ID derivation
- block validation rules beyond invoking the block crate
- any consumer-facing API wider than the parent `BlockStore` contract plus
  store construction and explicitly approved concrete-type maintenance methods
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
- one configured durability mode
- one initialized Redb database handle bound to backend-private state below
  that root

Cloned `RedbBlockStore` values share the same underlying Redb database handle.
When fast mode is selected, cloned values also share responsibility for one
flush-on-final-drop lifecycle, so dropping a non-final clone does not satisfy
the graceful-shutdown flush requirement by itself.

### DSG-REDB-STORE-003 `Construction boundary`

The crate exposes a constructor that accepts a store-root directory path plus a
durability-mode selection.

Construction:

1. creates the requested directory when needed
2. canonicalizes the resolved directory path
3. verifies that the resolved path is a directory
4. resolves the requested durability mode, defaulting to the current durable
   behavior when fast mode is not selected
5. initializes or opens one backend-private Redb database file below that root
6. ensures the block table exists before returning an initialized store

Failures to create the root, canonicalize it, stat it, confirm that it is a
directory, open the database, or initialize the block table map to explicit
backend failures.

### DSG-REDB-STORE-003C `Access-mode construction boundary`

The crate also resolves one caller-selected access mode at construction time.

The default access mode remains writable.

The read-only access mode remains a concrete `RedbBlockStore` concern and does
not widen the shared `BlockStore` trait with repository-wide read-only-open
semantics in this revision.

### DSG-REDB-STORE-003A `Concrete maintenance boundary`

The crate may expose Redb-specific administrative methods on `RedbBlockStore`
when those methods are intentionally outside the shared `BlockStore` trait.

Such methods remain concrete-type-only surfaces: they do not appear on
`BlockStore`, do not define repository-wide backend-neutral maintenance
semantics, and do not expose Redb internals beyond the approved operation's
result.

### DSG-REDB-STORE-003B `Telemetry attachment boundary`

The Redb crate may accept or attach the shared optional telemetry callback
through crate-owned configuration and runtime registration surfaces.

This attachment path does not require callers to construct or retain a raw
`redb::Builder`, `Database`, or `RepairSession` value.

### DSG-REDB-STORE-004 `Backend-private storage model`

This revision uses one backend-private Redb database file below the store root.

Within that database, one backend-private table maps:

- key: raw 32-byte block ID
- value: canonical block bytes

The database file name, table name, key representation, and any Redb page-level
layout remain implementation details and do not cross the parent trait
boundary.

### DSG-REDB-STORE-004A `Read-only snapshot opening strategy`

In this revision, read-only open is realized without granting the live
`RedbBlockStore` handle authority to mutate the on-disk database under the
configured store root.

When the on-disk database is in a clean state acceptable for read-only open,
the implementation may load a backend-private read-only snapshot of the Redb
database bytes into a private backend owned by the concrete store instance and
then open Redb against that private snapshot.

This preserves ordinary Redb-backed read semantics for `get` and identifier
enumeration while ensuring that read-only open does not dirty the caller's
persisted database file.

If the on-disk state would require repair before the backend can proceed, the
implementation fails explicitly rather than mutating the persisted database
state.

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
9. in default durable mode, completes the inherited durable write path before
   reporting success
10. in fast mode, records that a later graceful-shutdown flush is required and
    returns success without forcing the default per-write flush path

Successful writes in both modes remain immediately observable through the live
store handle and its clones.

Writes performed in default mode remain durably observable through later store
instances opened on the same store root without depending on any deferred
shutdown flush beyond the successful `put`.

Writes performed in fast mode become durably observable through later store
instances after the shared graceful-shutdown flush completes.

### DSG-REDB-STORE-005A `put_block_bytes_batch`

`put_block_bytes_batch` realizes the shared raw-byte batch-write capability for
Redb-backed storage.

The operation:

1. accepts a caller-supplied batch of block identifiers and canonical block
   bytes
2. opens one Redb write transaction for the full batch
3. applies the selected durability mode to that transaction in the same way as
   single-entry writes
4. inspects each requested block ID against the currently persisted value in the
   backend-private table
5. treats identical already-persisted bytes as idempotent acceptance for that
   entry
6. stages inserts for entries that are absent
7. fails explicitly if any entry encounters conflicting already-persisted bytes
   or another Redb/write-path failure
8. commits once, after all entries have been accepted for the batch
9. in fast mode, records one pending graceful-shutdown flush obligation after a
   successful commit

Because all staged mutations live inside one Redb write transaction until the
single commit point, a failed batch operation does not publish a partial prefix
of the batch to persisted store state.

### DSG-REDB-STORE-006 `Fast-mode graceful shutdown`

When fast mode is selected, the shared store state tracks whether successful
writes are pending a flush.

Dropping a non-final `RedbBlockStore` clone does not trigger shutdown flush
behavior.

Dropping the final handle triggers the required graceful-shutdown flush of
pending Redb state before the shared resources are released.

The design treats abnormal termination, crash, abort, or power loss before the
final-handle drop as outside the fast-mode durability guarantee.

### DSG-REDB-STORE-006A `compact_now`

`RedbBlockStore` exposes a Redb-specific `compact_now(&mut self)` maintenance
operation.

`compact_now`:

1. requires mutable access to the concrete store handle
2. requires exclusive ownership of the shared store state
3. fails explicitly as a backend failure when another clone or shared owner
   still exists
4. brings pending Redb state to a compaction-safe point before requesting
   compaction
5. invokes Redb compaction against the backend-private database
6. returns an explicit success result to the caller without exposing Redb
   database handles or transaction objects
7. preserves block readability, identity, and persisted-store reopen behavior
   across successful compaction

The public `compact_now` result communicates success or failure only and does
not expose Redb-native compaction-status detail as part of this crate's stable
caller contract.

When fast mode previously left pending successfully written entries awaiting the
graceful-shutdown flush, a successful `compact_now` satisfies the persistence
work needed for compaction so later store instances observe those writes after
the compaction operation completes.

When the store was opened in read-only mode, `compact_now` fails explicitly
before attempting any backend mutation.

### DSG-REDB-STORE-006B `Repair/status translation`

When Redb invokes its repair callback or status update path, the backend maps
that update into the shared telemetry event shape.

The emitted event may include repair progress and related diagnostic attributes,
but the mapping keeps Redb-native repair-session control private to the backend.

### DSG-REDB-STORE-006C `Read-only runtime behavior`

When opened in read-only mode:

1. `get` remains available
2. `iter_block_ids` remains available
3. telemetry callback attachment remains observational only
4. `put` fails explicitly before starting caller-visible persistence work
5. raw-byte batch persistence fails explicitly before starting caller-visible
   persistence work
6. `compact_now` fails explicitly before starting caller-visible backend
   maintenance work

The read-only runtime surface does not silently upgrade itself to writable
behavior.

### DSG-REDB-STORE-006D `Read-only recovery gate`

Before opening a store in read-only mode, the implementation inspects the
backend-owned Redb file for a recovery-required state.

If recovery is already required and the implementation cannot safely provide a
non-repairing read-only open for that state, construction fails explicitly.

This avoids silently invoking Redb repair on behalf of a caller that requested
observational-only access.

### DSG-REDB-STORE-007 `get`

`get`:

1. opens a Redb read transaction
2. looks up the requested block ID in the backend-private table
3. returns `Ok(None)` when no value is present
4. clones the stored bytes when a value is present
5. delegates decode and block-ID verification to the parent helper path layered
   above `get_block_bytes`

Malformed bytes and block-ID mismatch remain inherited decode failures through
the parent trait helper layer.

### DSG-REDB-STORE-008 `iter_block_ids`

`iter_block_ids` opens a Redb read transaction, snapshots the current set of
persisted keys by iterating the backend-private block table, decodes each
32-byte key into a `BlockHash`, and streams only those block IDs to callers.

If iteration encounters a Redb failure or a persisted key whose bytes cannot be
decoded as one block ID, enumeration fails explicitly as a backend failure.

### DSG-REDB-STORE-009 `Public boundary and error mapping`

The public API is limited to store construction, the parent `BlockStore` trait
implementation, and the approved Redb-specific `compact_now` method on
`RedbBlockStore`.

The crate does not expose Redb database handles, table definitions, transaction
objects, file paths below the store root, page metadata, or Redb-native query
surfaces to callers.

Database-open, transaction, read, write, commit, iteration, and compaction
failures map to explicit backend failures through the parent error taxonomy.

If the fast-mode graceful-shutdown flush fails while the final handle is
dropping, the implementation emits an explicit shutdown-visible error and does
not claim that the fast-mode durability guarantee was satisfied.

Read-only open failures caused by a missing database file, a recovery-required
database, or the inability to realize non-mutating read-only access also map to
explicit backend failures through the same parent error taxonomy.

### DSG-REDB-STORE-009A `Telemetry privacy boundary`

The public telemetry contract exposes shared events only.

It does not expose raw Redb database handles, builder objects, file-format
internals, or repair-session control surfaces to callers.

## Verification Strategy

### DSG-REDB-STORE-010 `Conformance and backend-specific verification`

The crate reuses the parent block-store conformance helpers to verify the shared
`put`, `get`, and identifier-enumeration contract.

The crate adds backend-specific tests for:

- constructor success on a caller-supplied store root
- explicit constructor failure for invalid or unusable roots
- successful durable round-trip through `put` and `get`
- default-mode durability remaining unchanged when fast mode is not selected
- fast-mode construction and ordinary-write behavior
- fast-mode flush on final-handle drop
- fast-mode crash-durability boundary as an explicitly documented non-guarantee
- public exposure of the Redb-specific `compact_now` method without widening
  `BlockStore`
- repair/status telemetry emitted through the shared generic callback surface
- unchanged behavior when the shared telemetry callback is absent
- observational-only telemetry semantics with no caller repair control
- successful read-only open against a clean database
- ordinary `get` and identifier enumeration through a read-only handle
- explicit failure for `put`, batch persistence, and `compact_now` through a
  read-only handle
- explicit failure for read-only open when the persisted database already
  requires recovery in a way this revision cannot satisfy safely
- absence of on-disk dirty-header side effects from read-only open
- successful compaction preserving block visibility and reopen behavior
- explicit compaction failure when exclusive ownership is unavailable
- explicit absence for missing block IDs
- explicit malformed-content and integrity-mismatch failures via injected raw
  bytes
- explicit integrity-conflict failure for conflicting existing bytes
- successful atomic batch persistence through the shared batch-write method
- explicit failure for unsupported batch behavior remaining outside this backend
  only through the parent trait's optional-capability contract
- explicit atomic rollback when one batch entry conflicts with divergent
  persisted bytes
- durable-mode reopen visibility after successful batch commit
- fast-mode batch visibility in-process and durability after the required flush
- visibility of committed writes after reopening the same store root
- enumeration of persisted block IDs only
- explicit failure for malformed persisted block-ID keys
- backend-neutral public API boundary

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-REDB-STORE-001 | REQ-REDB-STORE-001, REQ-REDB-STORE-002 |
| DSG-REDB-STORE-002..004 | REQ-REDB-STORE-001, REQ-REDB-STORE-003, REQ-REDB-STORE-004 |
| DSG-REDB-STORE-003A..003C | REQ-REDB-STORE-002, REQ-REDB-STORE-003, REQ-REDB-STORE-015, REQ-REDB-STORE-022, REQ-REDB-STORE-025 |
| DSG-REDB-STORE-004A | REQ-REDB-STORE-025, REQ-REDB-STORE-026, REQ-REDB-STORE-027 |
| DSG-REDB-STORE-005 | REQ-REDB-STORE-005, REQ-REDB-STORE-008, REQ-REDB-STORE-012 |
| DSG-REDB-STORE-005A | REQ-REDB-STORE-018, REQ-REDB-STORE-019, REQ-REDB-STORE-020, REQ-REDB-STORE-021 |
| DSG-REDB-STORE-006..006D | REQ-REDB-STORE-013, REQ-REDB-STORE-014, REQ-REDB-STORE-022, REQ-REDB-STORE-023, REQ-REDB-STORE-024, REQ-REDB-STORE-026, REQ-REDB-STORE-027, REQ-REDB-STORE-028 |
| DSG-REDB-STORE-006A | REQ-REDB-STORE-005, REQ-REDB-STORE-013, REQ-REDB-STORE-015, REQ-REDB-STORE-016, REQ-REDB-STORE-017, REQ-REDB-STORE-028 |
| DSG-REDB-STORE-007 | REQ-REDB-STORE-006, REQ-REDB-STORE-007 |
| DSG-REDB-STORE-008 | REQ-REDB-STORE-009, REQ-REDB-STORE-010, REQ-REDB-STORE-026 |
| DSG-REDB-STORE-009..009A | REQ-REDB-STORE-002, REQ-REDB-STORE-004, REQ-REDB-STORE-010, REQ-REDB-STORE-015, REQ-REDB-STORE-017, REQ-REDB-STORE-018, REQ-REDB-STORE-023, REQ-REDB-STORE-024, REQ-REDB-STORE-027, REQ-REDB-STORE-028 |
| DSG-REDB-STORE-010 | REQ-REDB-STORE-011, REQ-REDB-STORE-015, REQ-REDB-STORE-016, REQ-REDB-STORE-017, REQ-REDB-STORE-018, REQ-REDB-STORE-019, REQ-REDB-STORE-020, REQ-REDB-STORE-021, REQ-REDB-STORE-022, REQ-REDB-STORE-023, REQ-REDB-STORE-024, REQ-REDB-STORE-025, REQ-REDB-STORE-026, REQ-REDB-STORE-027, REQ-REDB-STORE-028 |
