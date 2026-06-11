<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Zip Block Store Design

## Status

Draft design specification for a Rust crate that exposes read-only LexonGraph
block storage over a single zip archive.

## Design Goals

The crate design is intended to be:

- subordinate to the backend-neutral `BlockStore` boundary
- read-only and explicit about that constraint
- deterministic in archive-entry recognition
- strict about integrity and duplicate-path ambiguity
- narrow at the public API boundary

## Crate Boundary

The crate owns:

- zip-specific realization of `get` and identifier enumeration
- constructor behavior for one archive path
- recognized archive-entry decoding
- explicit read-only failure for `put`
- zip-specific error mapping

The crate does not own:

- block canonicalization or block-ID derivation
- block validation rules beyond invoking the block crate
- any consumer-facing API wider than the parent `BlockStore` contract plus
  store construction
- archive mutation or publication behavior

## External Dependencies

### DSG-ZIP-STORE-001 `Parent dependencies and declared exception`

The zip block-store crate depends on:

- the block crate for typed block values, block IDs, and verified decoding
- the block-storage trait crate for the `BlockStore` trait and shared error
  taxonomy
- a zip-reader dependency for archive access

The crate explicitly documents that it implements the `BlockStore` API surface
for read operations while intentionally not realizing parent successful-`put`
requirements.

## Core Types

### DSG-ZIP-STORE-002 `ZipBlockStore`

A concrete store type owns a canonicalized filesystem path to one zip archive.

The constructor validates that the path resolves to a file and that the file can
be opened and parsed as a zip archive before returning an initialized store.

This revision accepts both classic zip and zip64 archives when the selected
zip-reader dependency can open and enumerate them successfully. `ZipBlockStore`
maps dependency-reported archive parsing or access failures to explicit backend
failures, including any residual dependency-specific zip64 limitations.

## Archive Recognition Model

### DSG-ZIP-STORE-003 `Recognized block entry layout`

Recognized block entries use the deterministic path:

`<hh>/<hh>/<full-lowercase-block-id>.cbor`

where the first two path components match the first two bytes of the block ID in
lowercase hexadecimal.

Archive entries that do not match that layout are unrelated content and are not
considered stored blocks.

### DSG-ZIP-STORE-004 `Archive inspection model`

Each `get` or enumeration operation opens the archive through the selected
zip-reader dependency and inspects the archive metadata needed for recognized
entry discovery.

This revision may inspect central-directory metadata directly, in addition to
using the zip-reader dependency for archive acceptance and entry reads, so that
duplicate recognized block-entry paths remain observable across both classic zip
and zip64 archives.

This revision does not require the store to cache a persistent in-memory index
across calls.

Duplicate recognized block-entry paths are treated as explicit backend failures
because the archive no longer maps one logical block location to one stored
representation.

## Runtime Behavior

### DSG-ZIP-STORE-005 `get`

`get`:

1. derives the deterministic recognized entry path from the requested block ID
2. opens the archive
3. finds the unique recognized entry at that path, if any
4. returns `Ok(None)` when no recognized entry exists
5. reads the entry bytes when exactly one recognized entry exists
6. validates those bytes against the requested block ID through the block crate

Malformed entry bytes map to malformed-content failures, block-ID mismatch maps
to integrity-mismatch failure, and archive/entry access issues map to backend
failure.

### DSG-ZIP-STORE-006 `iter_block_ids`

`iter_block_ids` opens the archive, scans entries, recognizes only unique block
entries matching the deterministic layout, and yields only their block IDs.

The enumeration realization:

1. exposes block identifiers only
2. ignores unrelated archive entries
3. fails explicitly if archive inspection cannot complete
4. fails explicitly if duplicate recognized block-entry paths are present

### DSG-ZIP-STORE-007 `put`

`put` performs no archive mutation and always returns an explicit backend
failure that describes read-only storage.

No append, rewrite, or temporary-archive publication semantics are part of this
revision.

### DSG-ZIP-STORE-008 `Public boundary`

The public API is limited to store construction plus the parent `BlockStore`
trait implementation.

Archive-entry paths, central-directory offsets, compression methods, and other
zip-specific details do not cross the trait boundary.

## Verification Strategy

### DSG-ZIP-STORE-009 `Read-only verification`

The crate verifies:

- constructor success and explicit constructor failures
- successful `get` from a recognized block entry
- explicit absence for missing recognized entries
- explicit malformed-content and integrity-mismatch failures
- explicit duplicate recognized-entry failure
- enumeration of recognized block IDs only
- ignoring unrelated archive entries
- explicit read-only `put` failure without archive mutation
- repository presence of zip-backend verification artifacts

The crate does not reuse or claim the parent conformance harness as proof of
full trait conformance because the approved design intentionally leaves `put`
non-conformant with the parent successful-write requirements.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-ZIP-STORE-001 | REQ-ZIP-STORE-001, REQ-ZIP-STORE-008 |
| DSG-ZIP-STORE-002 | REQ-ZIP-STORE-002, REQ-ZIP-STORE-003 |
| DSG-ZIP-STORE-003 | REQ-ZIP-STORE-004 |
| DSG-ZIP-STORE-004 | REQ-ZIP-STORE-005, REQ-ZIP-STORE-006 |
| DSG-ZIP-STORE-005 | REQ-ZIP-STORE-005 |
| DSG-ZIP-STORE-006 | REQ-ZIP-STORE-004, REQ-ZIP-STORE-006 |
| DSG-ZIP-STORE-007 | REQ-ZIP-STORE-007 |
| DSG-ZIP-STORE-008 | REQ-ZIP-STORE-001, REQ-ZIP-STORE-008 |
| DSG-ZIP-STORE-009 | REQ-ZIP-STORE-009 |
