# Rust Block Storage Trait Requirements

## Status

Draft specification for a Rust trait crate that defines the storage-layer
contract for LexonGraph blocks.

## Scope

This document specifies the crate-level requirements for a Rust crate that
defines the contract between LexonGraph consumers and block-storage
implementations.

This crate is layered on top of `docs/protocol/blocks.md` and the
`docs/specs/rust-block-crate/` specification package.

This document does not define block canonicalization, block validation, or
block-ID derivation rules. Those remain owned by the block protocol and block
crate.

## Terminology

In this spec package, `block identifier`, `block ID`, and `block hash` refer to
the same protocol-defined value: `sha256(canonical_cbor_bytes(block))`.

`Block storage backend` means any concrete persistence layer capable of storing
and retrieving blocks by block ID, including filesystem, sqlite, Azure Blob,
S3, or similar systems.

## Requirements

### REQ-BLOCK-STORE-001

The crate shall define the backend-agnostic contract between LexonGraph
consumers and block-storage backends.

### REQ-BLOCK-STORE-002

The crate shall use typed block-crate values and protocol-defined block
identifiers at its public API boundary.

### REQ-BLOCK-STORE-003

The crate shall expose a `put` operation that stores a block immutably and
returns its block ID.

### REQ-BLOCK-STORE-004

The crate shall expose a `get` operation that retrieves a block by block ID and
returns explicit absence when that block is not present.

### REQ-BLOCK-STORE-005

The crate shall only treat `get` as successful when the retrieved content
conforms to the requested block ID under the protocol's content-addressed
rules.

### REQ-BLOCK-STORE-006

The crate shall make `put` idempotent for logically identical blocks.

### REQ-BLOCK-STORE-007

The crate shall preserve a backend-neutral contract that does not expose
filesystem paths, SQL schemas, blob prefixes, bucket layout, or similar
backend-specific addressing details to consumers.

### REQ-BLOCK-STORE-008

The crate shall surface explicit failures for backend access, malformed stored
content, and block-identity mismatch, and shall not silently treat those
conditions as successful retrieval.

### REQ-BLOCK-STORE-009

The crate shall not include production storage backend implementations.

### REQ-BLOCK-STORE-010

The crate may include an internal-only memory-backed implementation solely to
support tests for the crate's contract.

### REQ-BLOCK-STORE-011

The crate shall not own search traversal behavior, indexing strategy, block
canonicalization policy, wire-format policy, or protocol evolution rules.

## Out of Scope

This crate does not define or own:

- block canonicalization or wire encoding
- block-ID derivation rules
- indexing tree construction strategy
- search traversal or ranking behavior
- backend enumeration, listing, deletion, or query APIs
- production filesystem, sqlite, Azure Blob, S3, or other concrete backend
  implementations

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md` for block identity,
wire format, canonicalization, and validity rules.

This document is also subordinate to the `docs/specs/rust-block-crate/`
specification package for typed block modeling and block verification behavior.

If this document appears to conflict with either of those authorities, they are
authoritative for their respective concerns.
