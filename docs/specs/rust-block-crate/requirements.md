# Rust Block Crate Requirements

## Status

Draft specification for the first-pass shared Rust crate that implements the
LexonGraph block protocol for both indexing and search components.

## Scope

This document specifies the crate-level requirements for a Rust crate that
implements `docs/protocol/blocks.md`.

This document does not define canonicalization or wire-format rules. Those
remain normative in `docs/protocol/blocks.md`. This document defines what the
crate must do in order to conform to that protocol.

## Requirements

### REQ-BLOCK-CRATE-001

The crate shall define the canonical in-memory model and protocol boundary for
LexonGraph blocks, consumable by both indexing and search components.

### REQ-BLOCK-CRATE-002

The crate shall support constructing a valid branch block or leaf block from an
input collection of entries plus required block metadata.

### REQ-BLOCK-CRATE-003

The crate shall support decomposing a validated block into block metadata and a
typed entry collection.

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

The crate shall expose branch-block and leaf-block transformations as distinct
typed operations.

### REQ-BLOCK-CRATE-008

The crate shall preserve version-1 extension semantics and versioned wire-key
interpretation.

### REQ-BLOCK-CRATE-009

Given logically identical inputs, the crate shall produce identical canonical
bytes and identical block IDs across callers.

### REQ-BLOCK-CRATE-010

This pass defines specification artifacts only. No Rust implementation is
required by this document.

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

## Out of Scope

This crate does not define or own:

- indexing tree construction strategy
- canonical embedding selection strategy
- search traversal and ranking behavior
- storage transport or block retrieval
- protocol evolution beyond implementing the current versioned block protocol

## Relationship to the Protocol

This document is subordinate to `docs/protocol/blocks.md`.

If this document appears to conflict with the protocol document, the protocol
document is authoritative for wire format, canonicalization, and validity
rules.
