<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Block Inspect CLI Requirements

## Status

Draft specification for a Rust workspace binary crate that inspects one
LexonGraph block through a configured block store and renders a JSON debug view.

## Scope

This document specifies the requirements for a Rust CLI crate named
`lexongraph-block-inspect`.

This document is layered on top of:

- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`
- `docs/specs/rust-filesystem-block-store/`

This document does not redefine block encoding, block identifiers, block-store
contract semantics, or filesystem store layout. Those concerns remain owned by
the protocol document and the subordinate specification packages.

## Terminology

`Backend selector` means the CLI input that chooses which `BlockStore`
implementation the tool constructs for the inspection request.

`Debug JSON encoding` means the documented JSON representation this crate uses
for block fields that are not natively representable in ordinary JSON, such as
byte strings and arbitrary CBOR-backed metadata or extension values.

## Requirements

### REQ-INSPECT-001

The repository shall include a Rust workspace binary crate named
`lexongraph-block-inspect` that provides a debugging CLI for inspecting one
stored LexonGraph block by block hash.

### REQ-INSPECT-002

The crate shall remain subordinate to `docs/protocol/blocks.md`,
`docs/specs/rust-block-crate/`, and `docs/specs/rust-block-storage-trait/` for
block identity, typed decoding, validation, and retrieval semantics.

### REQ-INSPECT-003

The CLI shall accept:

- a backend selector
- backend-specific construction parameters supplied outside the `BlockStore`
  trait boundary
- a target block hash

### REQ-INSPECT-004

This revision shall support inspection through the filesystem-backed
`BlockStore` implementation using a caller-supplied store-root path.

### REQ-INSPECT-005

The CLI shall load the requested block through a `BlockStore` implementation and
shall not bypass the storage contract by reading backend internals directly for
inspection.

### REQ-INSPECT-006

The CLI shall obtain the inspected typed block through the block crate's
verified decode path as realized by the `BlockStore` retrieval contract and
shall not treat the block as successfully inspected unless the requested block
hash has been verified.

### REQ-INSPECT-007

On successful inspection, the CLI shall emit JSON containing at least:

- the verified block hash
- the decoded block kind
- the decoded block content

### REQ-INSPECT-008

The CLI shall use a documented debug JSON encoding for block fields that are not
natively representable in ordinary JSON, including byte strings and arbitrary
CBOR-backed metadata or extension values.

### REQ-INSPECT-009

The CLI shall surface explicit failure for:

- invalid CLI input
- unsupported backend selector
- store construction failure
- block absence
- backend retrieval failure
- malformed stored content
- integrity mismatch

The CLI shall not present any of those conditions as successful inspection
output.

### REQ-INSPECT-010

The CLI surface shall be extensible to additional backend kinds in future
revisions without requiring changes to the existing caller contract for the
filesystem backend.

### REQ-INSPECT-011

This revision shall remain read-only and single-block in scope.

This crate shall not add recursive traversal, search behavior, block mutation,
backend enumeration, listing, or deletion behavior.

### REQ-INSPECT-012

The repository shall include automated verification artifacts that realize the
validation surface defined in `docs/specs/rust-block-inspect-cli/validation.md`.

## Out of Scope

This crate does not define or own:

- recursive tree walking
- search traversal or ranking
- block mutation or repair
- block-store trait changes
- backend enumeration or listing
- a protocol-stable interchange format distinct from the documented debug JSON
  encoding

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/`,
`docs/specs/rust-block-storage-trait/`, and
`docs/specs/rust-filesystem-block-store/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
