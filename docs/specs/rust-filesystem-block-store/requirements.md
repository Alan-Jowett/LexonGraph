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

### REQ-FS-STORE-013

The filesystem block-store crate shall implement the parent trait's streaming
block-ID enumeration for published block files rooted under the configured store
root.

### REQ-FS-STORE-014

Filesystem enumeration shall expose only block identifiers at the parent trait
boundary and shall not expose the implementation's internal path layout below
the store root.

### REQ-FS-STORE-015

Filesystem enumeration shall report only published block files that conform to
the implementation's deterministic block-file layout.

It shall not report staging files, temporary files, directories, or other
non-published filesystem artifacts as stored block IDs.

### REQ-FS-STORE-016

Filesystem enumeration shall surface explicit backend failure when traversal of
the store root or decoding of a published block-file location into a block ID
cannot be completed.

### REQ-FS-STORE-017

The filesystem block-store crate shall offer an opt-in cache-mode construction
path outside the `BlockStore` trait boundary that configures a payload-byte
budget in MB, where `1 MB = 1,048,576 bytes`.

Construction shall fail explicitly when the requested cache-mode MB budget is
zero or cannot be converted into the corresponding byte budget.

### REQ-FS-STORE-018

The cache-mode filesystem block-store shall account only canonical block payload
bytes stored in published block files against the configured byte budget.

The byte budget shall not include filesystem allocation slack, directory
entries, staging files, or implementation-private bookkeeping.

### REQ-FS-STORE-019

When a direct cache-mode `put` would exceed the configured byte budget, the
implementation shall evict least-recently-used cached published blocks until
the new block fits and success can be reported.

If construction discovers an existing cache root whose published block payloads
already exceed the configured byte budget, it shall evict least-recently-used
existing cached published blocks during construction until the cache root fits
the budget.

### REQ-FS-STORE-020

For existing cached published blocks discovered during cache-mode construction,
initial recency ordering shall derive from filesystem last-modified time, with a
deterministic tie-breaker.

### REQ-FS-STORE-021

If one block's canonical payload bytes exceed the entire configured cache-mode
byte budget, the implementation shall reject that direct cache write
explicitly, shall not publish the block file, and shall leave the remaining
cache contents unchanged.

### REQ-FS-STORE-022

Cache-mode eviction and recency accounting may remain implementation-private,
but shall not widen the parent `BlockStore` contract or cause internal
bookkeeping artifacts to appear through the parent trait's enumeration surface.

## Out of Scope

This crate does not define or own:

- block canonicalization, block validity, or block-ID derivation rules
- backend deletion or query APIs beyond the parent trait's block-ID enumeration
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
