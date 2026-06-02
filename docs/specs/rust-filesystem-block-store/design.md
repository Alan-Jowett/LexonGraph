<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Filesystem Block Store Design

## Status

Draft design specification for a Rust crate that implements the LexonGraph
block-storage contract on a local filesystem.

## Design Goals

The crate design is intended to be:

- subordinate to the backend-neutral `BlockStore` contract
- deterministic in its mapping from block ID to on-disk location
- atomic at the reader visibility boundary
- strict about integrity conflicts
- narrow at the public API boundary

## Crate Boundary

The crate owns:

- filesystem-specific realization of `BlockStore`
- store-root configuration and path derivation
- atomic publication mechanics for block files
- filesystem-specific error mapping

The crate does not own:

- block canonicalization or block-ID derivation
- block validation rules beyond invoking the block crate
- any consumer-facing API wider than the parent `BlockStore` contract plus
  store construction
- enumeration, deletion, or compaction behavior

## External Dependencies

### DSG-FS-STORE-001 `Parent crate dependencies`

The filesystem block-store crate depends on:

- the block crate for canonical serialization, block-ID derivation, and
  validated decoding
- the block-storage trait crate for the `BlockStore` trait and shared error
  taxonomy

The filesystem crate does not redefine those behaviors.

## Core Types

### DSG-FS-STORE-002 `FilesystemBlockStore`

A concrete store type owns a canonicalized store-root path and implements the
parent `BlockStore` trait for that root.

### DSG-FS-STORE-003 `Construction boundary`

The crate exposes a constructor that accepts a filesystem path for the store
root and returns an initialized store instance or an explicit error.

Initialization may create required directories below the root, but those
implementation details remain outside the parent trait boundary.

## On-Disk Mapping

### DSG-FS-STORE-004 `Deterministic block path`

Each block ID maps to exactly one deterministic published file path below the
store root.

This revision uses a sharded layout derived from the hexadecimal block ID:

- a first-level directory named by the first two hex characters
- a second-level directory named by the next two hex characters
- a file named by the full lowercase hexadecimal block ID plus `.cbor`

Example:

`<store-root>/ab/cd/abcdef...0123.cbor`

The path separators shown in this example are illustrative; the implementation
uses platform-native separators through Rust `Path` joins.

This layout is an implementation detail for this crate and is not promoted into
the backend-neutral parent contract.

### DSG-FS-STORE-005 `Publish staging`

`put` writes candidate bytes to a temporary file created under the same target
directory as the published block file.

Publishing completes by atomically moving or renaming that temporary file into
the deterministic published file path.

Placing the temporary file in the same target directory preserves the atomic
replacement semantics available from the local filesystem.

## Runtime Behavior

### DSG-FS-STORE-006 `put`

`put`:

1. canonicalizes the input block through the block crate
2. derives the published block path from the returned block ID
3. ensures the containing directories exist
4. writes the canonical bytes to a temporary sibling file
5. attempts atomic publication into the published file path
6. if publication detects an already-published file, reads the existing bytes
   and:
   - treats byte-identical content as idempotent success
   - treats differing bytes as an explicit integrity-conflict failure

### DSG-FS-STORE-007 `get`

`get`:

1. derives the published block path from the requested block ID
2. returns `Ok(None)` when the file is absent
3. reads the stored bytes when the file is present
4. validates those bytes against the requested block ID through the block crate
5. returns `Ok(Some(validated_block))` only for present, valid, matching content

Malformed bytes, unreadable files, or block-ID mismatch remain explicit
failures and are not downgraded to absence.

### DSG-FS-STORE-008 `Concurrent publication`

Two or more store instances targeting the same store root may attempt to
publish the same block concurrently.

The design relies on deterministic target paths plus atomic publication so that
readers observe either absence or one fully published block file, never a
partially written file.

If concurrent publishers race on the same block ID, the losing publisher
re-checks the published file:

- if the published bytes equal the canonical bytes, `put` reports success
- if the published bytes differ, `put` reports an integrity-conflict failure

### DSG-FS-STORE-009 `Durability scope`

This revision guarantees atomic visibility of published block files within the
store root but does not guarantee crash persistence through explicit syncing of
file contents or directory metadata.

### DSG-FS-STORE-010 `Error mapping`

Filesystem access failures are surfaced as explicit backend failures through the
parent error taxonomy.

Existing-file byte conflicts are surfaced as explicit backend failures that
describe corruption or integrity conflict at the published block path.

Block decoding failures and block-ID mismatches continue to map to the parent
crate's malformed-content and integrity-mismatch errors.

## Verification Strategy

### DSG-FS-STORE-011 `Conformance reuse`

The crate reuses the parent block-store conformance helpers to verify the
backend-neutral `put` and `get` contract.

### DSG-FS-STORE-012 `Filesystem-specific verification`

The crate adds backend-specific tests for:

- deterministic path mapping under the store root
- atomic publish behavior via staged sibling files
- explicit failure on conflicting pre-existing bytes
- successful convergence for concurrent publication of the same block

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-FS-STORE-001 | REQ-FS-STORE-001, REQ-FS-STORE-002 |
| DSG-FS-STORE-002..003 | REQ-FS-STORE-002, REQ-FS-STORE-003 |
| DSG-FS-STORE-004 | REQ-FS-STORE-003, REQ-FS-STORE-004 |
| DSG-FS-STORE-005..006 | REQ-FS-STORE-004, REQ-FS-STORE-005, REQ-FS-STORE-007 |
| DSG-FS-STORE-007 | REQ-FS-STORE-006 |
| DSG-FS-STORE-008 | REQ-FS-STORE-005, REQ-FS-STORE-007, REQ-FS-STORE-008 |
| DSG-FS-STORE-009 | REQ-FS-STORE-005 |
| DSG-FS-STORE-010 | REQ-FS-STORE-001, REQ-FS-STORE-006, REQ-FS-STORE-007 |
| DSG-FS-STORE-011..012 | REQ-FS-STORE-009 |

