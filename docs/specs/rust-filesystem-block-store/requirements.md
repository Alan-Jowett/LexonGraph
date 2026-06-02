<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Filesystem Block Store Requirements

## Status

Draft specification for a Rust crate that implements the LexonGraph
block-storage contract on a local filesystem.

## Scope

This document specifies implementation-specific requirements for a Rust crate
that realizes `docs/specs/rust-block-storage-trait/` on a local filesystem.

This document is layered on top of:

- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`

This document does not repeat or redefine the parent trait contract. It adds
only filesystem-specific requirements needed to realize that contract in this
repository.

## Terminology

In this spec package, `store root` means the filesystem directory supplied to
the implementation as the root under which block content is persisted.

`Published block file` means the file at the implementation-defined location
under the store root that represents one stored block ID and is eligible to be
observed by readers.

## Requirements

### REQ-FS-STORE-001

The filesystem block-store crate shall remain subordinate to
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`, and
`docs/specs/rust-block-storage-trait/` for block identity, validation, and the
backend-neutral `BlockStore` contract.

### REQ-FS-STORE-002

The repository shall include a Rust crate, separate from
`crates/lexongraph-block-store`, that implements `BlockStore` using a
caller-supplied local filesystem store root.

### REQ-FS-STORE-003

The filesystem block-store crate shall configure persistence from a store-root
path supplied outside the `BlockStore` trait boundary.

The consumer-facing runtime contract shall not require callers to know the
implementation's internal path layout below that root.

### REQ-FS-STORE-004

`put` shall derive the canonical block bytes and block ID through the block
crate and map that block ID to exactly one deterministic on-disk location under
the configured store root.

### REQ-FS-STORE-005

The filesystem-backed implementation shall publish stored block content
atomically within the store root so that readers observe only fully published
block files.

This revision does not require explicit `fsync`-style crash-durability
guarantees for file contents or directory metadata.

### REQ-FS-STORE-006

`get` shall return `Ok(None)` when the mapped block file is absent.

When a mapped block file is present, `get` shall validate the retrieved bytes
against the requested block ID before reporting success.

### REQ-FS-STORE-007

If `put` encounters pre-existing bytes at the target block location that do not
match the canonical bytes for the block being stored, it shall fail explicitly
as a corruption or integrity-conflict condition and shall not overwrite those
bytes.

### REQ-FS-STORE-008

Concurrent publishers of the same logical block to the same store root may
race, but the implementation shall converge on one valid published block file
for that block ID without exposing partial content to readers.

### REQ-FS-STORE-009

The repository shall include automated verification artifacts that realize the
validation surface defined in `docs/specs/rust-filesystem-block-store/`,
including reuse of the parent trait crate's conformance helpers where
applicable.

## Out of Scope

This crate does not define or own:

- block canonicalization, block validity, or block-ID derivation rules
- backend enumeration, listing, deletion, or query APIs
- any public compatibility promise for the internal path layout below the store
  root
- explicit `fsync`-based crash-durability guarantees
- non-filesystem storage backends

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.

