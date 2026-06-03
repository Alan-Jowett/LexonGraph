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

Construction shall either return an initialized store rooted at a canonical
directory path or fail explicitly as a backend failure when the requested root
cannot be created, canonicalized, stat'ed, or resolved to a non-directory.

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

`get` shall be total over the mapped file state:

- present readable valid content for the requested block ID shall return
  `Ok(Some(validated_block))`
- present readable malformed or protocol-invalid content shall fail explicitly
  as malformed content
- present readable content whose verified identity differs from the requested
  block ID shall fail explicitly as an integrity-mismatch condition
- present unreadable content shall fail explicitly as a backend failure

### REQ-FS-STORE-007

If `put` encounters published bytes at the target block location that do not
match the canonical bytes for the block being stored, whether before or after a
publication race is re-inspected, it shall fail explicitly as a backend failure
describing corruption or integrity conflict and shall not overwrite those
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

### REQ-FS-STORE-010

`put` shall fail explicitly as a backend failure without publishing a target
block file when parent-directory creation, staging-file creation, staged write,
or staged flush fails before atomic publication completes.

### REQ-FS-STORE-011

If atomic publication of staged canonical bytes fails, the implementation shall
inspect the deterministic target path and:

- return success when the published bytes already match the canonical bytes for
  the block
- fail explicitly as a backend failure describing integrity conflict, without
  overwrite, when the published bytes differ
- fail explicitly as a backend failure when no published file is present after
  the failed publication attempt
- fail explicitly as a backend failure when post-failure inspection cannot
  complete

### REQ-FS-STORE-012

The repository shall include automated verification artifacts that exercise the
filesystem-specific negative-path semantics for:

- constructor root failures
- present-but-unreadable files during `get`
- pre-publication staging failures during `put`
- publication failure followed by matching published bytes
- publication failure followed by differing published bytes
- publication failure followed by a missing target
- publication failure followed by an unreadable or otherwise uninspectable
  target

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
