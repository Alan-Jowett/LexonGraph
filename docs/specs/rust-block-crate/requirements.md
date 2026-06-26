<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Block Crate Requirements

## Status

Draft specification for the first-pass shared Rust crate that implements the
LexonGraph block protocol for both indexing and search components.

## Scope

This document specifies the crate-level requirements for a Rust crate that
implements `docs/protocol/blocks.md` and `docs/protocol/blocks-v2.md`.

This document does not define canonicalization or wire-format rules. Those
remain normative in the block protocol documents. This document defines what
the crate must do in order to conform to those protocols.

## Terminology

In this spec package, `block identifier`, `block ID`, and `block hash` refer to
the same protocol-defined value: `sha256(canonical_cbor_bytes(block))`.

## Requirements

### REQ-BLOCK-CRATE-001

The crate shall define the canonical in-memory model and protocol boundary for
LexonGraph blocks, consumable by both indexing and search components.

### REQ-BLOCK-CRATE-002

The crate shall support constructing a valid level-0 leaf block or a valid
level-`k > 0` child-bearing block from an input collection of entries plus
required block metadata.

### REQ-BLOCK-CRATE-003

The crate shall support decomposing a validated block into block metadata,
including decoded `level`, and a typed entry collection.

### REQ-BLOCK-CRATE-004

The crate shall implement canonical CBOR serialization, deserialization,
validation, and block-ID derivation in conformance with the canonicalization
and validity rules defined by `docs/protocol/blocks.md`.

### REQ-BLOCK-CRATE-005

The crate shall preserve all normative invariants in
`docs/protocol/blocks.md` and reject malformed or non-conforming version-1
blocks.

### REQ-BLOCK-CRATE-006

The crate shall remain protocol-focused and shall not own indexing heuristics,
search traversal logic, storage backends, or similarity functions.

### REQ-BLOCK-CRATE-007

The crate shall expose child-bearing (`level > 0`) and leaf (`level = 0`)
transformations as distinct typed operations.

### REQ-BLOCK-CRATE-008

The crate shall preserve version-1 extension semantics and versioned wire-key
interpretation.

### REQ-BLOCK-CRATE-009

Given logically identical inputs, the crate shall produce identical canonical
bytes and identical block identifiers across callers.

### REQ-BLOCK-CRATE-010

This pass requires a Rust implementation of this specification package,
including automated verification artifacts that realize the validation surface
defined in `docs/specs/rust-block-crate/validation.md`.

### REQ-BLOCK-CRATE-011

The crate shall accept serialized block bytes together with an expected block
hash and shall validate that hash before treating the bytes as a valid block
instance.

### REQ-BLOCK-CRATE-012

The crate shall serialize a typed block into canonical CBOR bytes and return
both the bytes and the derived block hash.

### REQ-BLOCK-CRATE-013

The crate shall fail explicitly on hash mismatch and shall not deserialize
mismatched content as valid.

### REQ-BLOCK-CRATE-014

The crate shall preserve and expose arbitrary nonnegative block levels in
version-1 serialization, deserialization, and typed validated output.

### REQ-BLOCK-CRATE-015

The crate shall implement the EBCP branch-encoding validity rules defined by
`docs/protocol/ebcp.md` together with the enclosing block rules from
`docs/protocol/blocks.md`.

### REQ-BLOCK-CRATE-016

The crate shall accept EBCP encodings only on non-leaf blocks and shall reject
leaf blocks that declare any EBCP encoding.

### REQ-BLOCK-CRATE-017

For an EBCP-encoded non-leaf block, the crate shall validate the presence and
shape of the required `ext` metadata and shall reject payload bytes whose length
is inconsistent with the declared EBCP encoding and metadata.

### REQ-BLOCK-CRATE-018

The crate shall preserve and expose EBCP branch-encoding metadata and raw branch
payload bytes to downstream indexing and search consumers without embedding
indexing or search policy decisions into the block crate itself.

The crate shall also expose a canonical public helper that reconstructs the
logical branch embedding values for each supported stored branch-embedding
encoding without requiring downstream consumers to duplicate encoding-specific
decode logic.

### REQ-BLOCK-CRATE-019

For a supported stored branch embedding, the crate shall provide a stable public
reconstruction surface that accepts the stored `EmbeddingSpec`, the stored
payload bytes, and any required parsed EBCP descriptor metadata and returns the
logical ambient-space `f32` vector used for comparison.

That surface shall fail explicitly when the stored encoding is unsupported for
logical `f32` reconstruction or when the stored payload bytes or EBCP metadata
are malformed or inconsistent.

### REQ-BLOCK-CRATE-020

The crate shall expose a version-aware encode/decode surface that can round-trip
both version-1 blocks and version-2 blocks without silently upgrading one
version into the other.

### REQ-BLOCK-CRATE-021

Version 2 shall use a unified top-level `version + type + content` envelope,
with reserved protocol-defined `branch` and `leaf` types, exact top-level field
keys `0`, `1`, and `2`, and support for application-defined non-empty UTF-8
custom type strings.

### REQ-BLOCK-CRATE-022

For a version-2 custom type, the crate shall validate only that `content` is
canonical CBOR and shall not impose additional shared-schema semantics beyond
the reserved protocol-defined types.

### REQ-BLOCK-CRATE-023

The crate shall keep version-1 support available without mutating the
version-1 protocol authority in `docs/protocol/blocks.md`.

Version-aware decode shall determine the block version from the decoded
top-level envelope and shall not silently convert one version into the other.

### REQ-BLOCK-CRATE-024

The repository shall introduce a separate version-2 protocol authority in
`docs/protocol/blocks-v2.md`, and the crate's version-2 implementation shall be
subordinate to that document.

## Out of Scope

This crate does not define or own:

- indexing tree construction strategy
- canonical embedding selection strategy
- search traversal and ranking behavior
- storage transport or block retrieval
- protocol evolution beyond implementing the current versioned block protocol

## Relationship to the Protocol

This document is subordinate to `docs/protocol/blocks.md` and
`docs/protocol/blocks-v2.md` together with `docs/protocol/ebcp.md`.

If this document appears to conflict with the protocol document, the protocol
document is authoritative for wire format, canonicalization, and validity
rules.
