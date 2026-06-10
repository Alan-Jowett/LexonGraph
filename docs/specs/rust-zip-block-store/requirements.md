<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Zip Block Store Requirements

## Status

Draft specification for a Rust crate that exposes read-only LexonGraph block
storage over a single zip archive.

## Scope

This document specifies implementation-specific requirements for a Rust crate
that realizes the `BlockStore` API surface over one caller-supplied zip file.

This document is layered on top of:

- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`

This document adds zip-specific behavior needed for this repository. It does
not redefine block identity, canonical encoding, or block validation.

## Terminology

In this spec package, `archive path` means the filesystem path to the single zip
file used as the storage source.

`Recognized block entry` means an archive entry whose internal path matches the
deterministic sharded block-file layout:

`<hh>/<hh>/<full-lowercase-block-id>.cbor`

where the first two directory levels are the first two bytes of the block ID in
lowercase hexadecimal.

## Requirements

### REQ-ZIP-STORE-001

The zip block-store crate shall remain subordinate to
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`, and
`docs/specs/rust-block-storage-trait/` for block identity, typed values, and
read-path integrity rules.

This backend intentionally does not conform to parent requirements that require
successful immutable persistence through `put`.

### REQ-ZIP-STORE-002

The repository shall include a Rust crate, separate from
`crates/lexongraph-block-store`, that implements the `BlockStore` trait surface
over a caller-supplied single zip archive.

### REQ-ZIP-STORE-003

Construction shall accept the archive path outside the `BlockStore` trait
boundary.

Construction shall either return an initialized store bound to a canonical
archive file path or fail explicitly as a backend failure when the requested
path cannot be canonicalized, stat'ed, resolved to a non-file, opened, or read
as a zip archive.

### REQ-ZIP-STORE-004

The zip block-store crate shall recognize stored blocks only from archive
entries whose internal paths match the deterministic sharded layout:

- first-level directory: first two lowercase hexadecimal characters of the
  block ID
- second-level directory: next two lowercase hexadecimal characters of the
  block ID
- file name: full lowercase hexadecimal block ID plus `.cbor`

Archive entries outside that layout are unrelated content and shall not
participate in `get` or enumeration.

### REQ-ZIP-STORE-005

`get` shall map a requested block ID to its deterministic recognized block-entry
path inside the archive.

`get` shall return:

- `Ok(None)` when the recognized block entry is absent
- `Ok(Some(validated_block))` only when the recognized entry bytes decode and
  verify for the requested block ID
- explicit malformed-content failure for malformed or protocol-invalid entry
  bytes
- explicit integrity-mismatch failure for valid block bytes whose verified
  identity differs from the requested block ID
- explicit backend failure for archive-access, entry-read, or duplicate-entry
  conditions

### REQ-ZIP-STORE-006

The zip block-store crate shall implement the parent trait's streaming
block-ID enumeration over recognized block entries in the archive.

Enumeration shall:

- yield block identifiers only
- ignore unrelated archive entries
- fail explicitly as a backend failure when archive inspection cannot complete
- fail explicitly as a backend failure when duplicate recognized block-entry
  paths are present

### REQ-ZIP-STORE-007

`put` shall always fail explicitly as a backend failure describing read-only
storage and shall not modify the archive.

### REQ-ZIP-STORE-008

The crate and its specification package shall explicitly document that this
backend implements the `BlockStore` API surface for read operations but is
intentionally non-conformant with parent successful-`put` requirements,
including `REQ-BLOCK-STORE-003` and `REQ-BLOCK-STORE-006`.

### REQ-ZIP-STORE-009

The repository shall include automated verification artifacts that realize this
zip-backend validation surface, including constructor behavior, retrieval,
enumeration, ignored unrelated entries, duplicate recognized-entry failure, and
explicit read-only `put` failure.

## Out of Scope

This crate does not define or own:

- block canonicalization, block validity, or block-ID derivation rules
- zip archive mutation, append, rewrite, deletion, or compaction behavior
- any public compatibility promise for unrelated archive entries
- parent trait conformance claims for successful `put`
- non-zip storage backends

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
