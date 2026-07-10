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
- filesystem traversal needed to enumerate published block IDs
- filesystem-specific error mapping

The crate does not own:

- block canonicalization or block-ID derivation
- block validation rules beyond invoking the block crate
- any consumer-facing API wider than the parent `BlockStore` contract plus
  store construction
- consumer-facing deletion or compaction behavior

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

Successful construction stores a canonicalized directory path as the root.
Failures to create the root, canonicalize it, stat it, or confirm that the
resolved path is a directory map to explicit backend failures.

The crate also exposes an opt-in cache-mode constructor that accepts an MB
budget and converts it to a payload-byte budget using `1 MB = 1,048,576 bytes`.

Cache-mode construction fails explicitly when the requested MB budget is zero or
cannot be converted to the corresponding byte budget.

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
6. fails explicitly as a backend failure without publishing the target block
   file if directory creation, staging-file creation, staged write, or staged
   flush fails
7. if atomic publication fails, re-reads the published path and:
   - treats byte-identical content as idempotent success
   - treats differing bytes as an explicit backend failure describing integrity
     conflict
   - treats a still-missing target as an explicit backend failure
   - treats post-failure inspection errors as explicit backend failures

### DSG-FS-STORE-007 `get`

`get`:

1. derives the published block path from the requested block ID
2. returns `Ok(None)` when the file is absent
3. reads the stored bytes when the file is present
4. validates those bytes against the requested block ID through the block crate
5. returns `Ok(Some(validated_block))` only for present, valid, matching
   content

Malformed bytes map to malformed-content failures, block-ID mismatch maps to an
integrity-mismatch failure, and unreadable files map to backend failures. None
of those states are downgraded to absence.

### DSG-FS-STORE-008 `iter_block_ids`

`iter_block_ids` walks the configured store root and streams block identifiers
for published block files that match the deterministic on-disk block layout.

The enumeration realization:

1. traverses the store root without exposing traversal paths at the trait
   boundary
2. recognizes published block files by the deterministic sharded layout and file
   naming convention owned by this crate
3. decodes each recognized published file path back into its block ID
4. yields only decoded block IDs to callers
5. ignores directories and transient staging artifacts that are not published
   block files

Traversal or path-decoding failures map to explicit backend failures through the
parent error taxonomy.

### DSG-FS-STORE-009 `Concurrent publication`

Two or more store instances targeting the same store root may attempt to
publish the same block concurrently.

The design relies on deterministic target paths plus atomic publication so that
readers observe either absence or one fully published block file, never a
partially written file.

If concurrent publishers race on the same block ID, the losing publisher
re-checks the published file:

- if the published bytes equal the canonical bytes, `put` reports success
- if the published bytes differ, `put` reports an explicit backend failure
  describing integrity conflict

### DSG-FS-STORE-010 `Durability scope`

This revision guarantees atomic visibility of published block files within the
store root but does not guarantee crash persistence through explicit syncing of
file contents or directory metadata.

### DSG-FS-STORE-011 `Error mapping`

Filesystem access failures are surfaced as explicit backend failures through the
parent error taxonomy.

Existing-file byte conflicts are surfaced as explicit backend failures that
describe corruption or integrity conflict at the published block path.

Block decoding failures and block-ID mismatches continue to map to the parent
crate's malformed-content and integrity-mismatch errors.

This mapping applies both to constructor-time filesystem failures and to
runtime negative paths during `put` and `get`, including publication-failure
recovery.

Enumeration-time directory traversal failures, metadata failures, or published
path-decoding failures also map to explicit backend failures through the parent
error taxonomy.

### DSG-FS-STORE-012 `Cache-mode accounting and construction`

The opt-in cache-mode constructor scans existing published block files rooted
under the configured store root, computes their payload-byte usage from the
published file lengths, and derives their initial recency order from filesystem
last-modified times with a deterministic path-based tie-breaker.

If the discovered published payload set already exceeds the configured byte
budget, the constructor evicts least-recently-used published cache files until
the retained payload set fits the budget.

### DSG-FS-STORE-013 `Cache-mode direct-write admission`

Before a direct cache-mode `put` publishes a new block file, the implementation
plans any required evictions against the current payload-byte budget.

If one block's payload bytes exceed the entire byte budget, the direct cache
write fails explicitly without publishing the new block file.

Otherwise, the implementation evicts the least-recently-used cached published
files before attempting publication of the new block.

If publication subsequently fails, previously evicted cached files may already
be gone.

### DSG-FS-STORE-014 `Cache-mode recency refresh`

Successful direct cache-mode `put` and `get` operations refresh the affected
block's recency within the process-local cache accounting state.

### DSG-FS-STORE-015 `Cache-mode metadata boundary`

Cache-mode byte accounting and recency tracking remain implementation-private.

The crate does not widen the parent `BlockStore` trait and does not surface
implementation-private cache bookkeeping artifacts through `iter_block_ids`.

## Verification Strategy

### DSG-FS-STORE-016 `Conformance reuse`

The crate reuses the parent block-store conformance helpers to verify the
backend-neutral `put`, `get`, and block-ID enumeration contract.

### DSG-FS-STORE-017 `Filesystem-specific verification`

The crate adds backend-specific tests for:

- constructor success and explicit constructor failure cases
- deterministic path mapping under the store root
- explicit backend failure for unreadable published files during `get`
- atomic publish behavior via staged sibling files
- explicit backend failure for pre-publication staging failures
- publication-failure recovery to matching bytes as idempotent success
- explicit failure for publication-failure recovery to differing bytes
- explicit backend failure when publication fails and the target remains missing
- explicit backend failure when publication fails and the target cannot be
  re-inspected
- explicit failure on conflicting pre-existing bytes
- successful convergence for concurrent publication of the same block
- cache-mode constructor eviction of over-budget existing cache roots
- cache-mode byte-budget eviction under direct access
- cache-mode rejection of direct writes larger than the total byte budget
- enumeration of published block IDs rooted under the configured store
- exclusion of staging files and other non-published artifacts from enumeration
- explicit failure for directory traversal or path-decoding errors during
  enumeration

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-FS-STORE-001 | REQ-FS-STORE-001, REQ-FS-STORE-002 |
| DSG-FS-STORE-002..003 | REQ-FS-STORE-002, REQ-FS-STORE-003, REQ-FS-STORE-017 |
| DSG-FS-STORE-004 | REQ-FS-STORE-003, REQ-FS-STORE-004 |
| DSG-FS-STORE-005..006 | REQ-FS-STORE-004, REQ-FS-STORE-005, REQ-FS-STORE-007, REQ-FS-STORE-010, REQ-FS-STORE-011 |
| DSG-FS-STORE-007 | REQ-FS-STORE-006 |
| DSG-FS-STORE-008 | REQ-FS-STORE-013, REQ-FS-STORE-014, REQ-FS-STORE-015, REQ-FS-STORE-016, REQ-FS-STORE-022 |
| DSG-FS-STORE-009 | REQ-FS-STORE-005, REQ-FS-STORE-007, REQ-FS-STORE-008 |
| DSG-FS-STORE-010 | REQ-FS-STORE-005 |
| DSG-FS-STORE-011 | REQ-FS-STORE-001, REQ-FS-STORE-003, REQ-FS-STORE-006, REQ-FS-STORE-007, REQ-FS-STORE-010, REQ-FS-STORE-011, REQ-FS-STORE-016 |
| DSG-FS-STORE-012 | REQ-FS-STORE-017, REQ-FS-STORE-018, REQ-FS-STORE-019, REQ-FS-STORE-020 |
| DSG-FS-STORE-013..015 | REQ-FS-STORE-018, REQ-FS-STORE-019, REQ-FS-STORE-021, REQ-FS-STORE-022 |
| DSG-FS-STORE-016..017 | REQ-FS-STORE-009, REQ-FS-STORE-012, REQ-FS-STORE-013, REQ-FS-STORE-015, REQ-FS-STORE-016 |
