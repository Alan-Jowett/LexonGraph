<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Block Crate Design

## Status

Draft design specification for the first-pass shared Rust crate that implements
the LexonGraph block protocol.

## Design Goals

The crate design is intended to be:

- shared between indexing and search
- protocol-focused
- deterministic
- explicit about hash verification
- strict about protocol conformance

## Crate Boundary

The crate owns:

- typed block-domain modeling
- protocol-conforming block construction
- protocol-conforming validation
- canonical CBOR encoding and decoding
- block-hash computation and verification

The crate does not own:

- indexing heuristics
- canonical embedding algorithms
- search traversal or ranking
- storage or block transport

## Protocol Conformance Boundary

Canonicalization, field-key semantics, and validity rules are defined
normatively by `docs/protocol/blocks.md`.

This crate implements those rules; it does not redefine them.

## Core Types

### DSG-001 `Block`

A sum type over `BranchBlock` and `LeafBlock`.

### DSG-002 `BranchBlock` and `LeafBlock`

Distinct typed block structs with shared block metadata and level-governed entry
collections. `LeafBlock` is fixed at `level = 0`; `BranchBlock` carries an
arbitrary `level > 0`.

### DSG-003 `EmbeddingSpec`

A typed representation of the block-scoped embedding specification containing
`dims` and `encoding`.

### DSG-004 `BranchEntry` and `LeafEntry`

Typed entry structs matching the exact version-1 protocol shapes.

### DSG-005 `Content`

A typed representation of the leaf content payload containing `media_type` and
`body`.

### DSG-006 `BlockHash`

A strongly typed 32-byte SHA-256 block identifier.

### DSG-007 `SerializedBlock`

A serialization result containing canonical bytes and the derived `BlockHash`.

### DSG-008 `ValidatedBlock`

A successful decode result containing a typed block and the verified hash used
to accept it.

### DSG-009 `BlockError`

An explicit error taxonomy covering at least:

- hash mismatch
- malformed CBOR
- unsupported version
- invalid field-key usage
- invalid block level or entry shape
- non-conforming block structure under the protocol rules

## API Surface

### DSG-010 `build_branch_block(...) -> Result<BranchBlock, BlockError>`

Constructs a child-bearing block from required metadata, explicit `level > 0`,
and a typed branch-entry collection, then validates protocol conformance.

### DSG-011 `build_leaf_block(...) -> Result<LeafBlock, BlockError>`

Constructs a leaf block at `level = 0` from required metadata and a typed
leaf-entry payload, then validates protocol conformance.

### DSG-012 `serialize_block(&Block) -> Result<SerializedBlock, BlockError>`

Serializes a typed block into canonical CBOR and returns both bytes and the
derived hash.

### DSG-013 `deserialize_block(bytes, expected_hash) -> Result<ValidatedBlock, BlockError>`

Computes the hash of the supplied bytes, compares it to `expected_hash`, and
only then accepts and decodes the block as valid.

### DSG-014 `compute_block_hash(bytes) -> BlockHash`

Exposes block-hash computation for callers that need to compare or precompute
hashes independently of decode.

### DSG-015 `into_entries(ValidatedBlock) -> TypedEntries`

Decomposes a validated block into block metadata, including decoded `level`, and
a typed entry collection without embedding search- or indexing-specific
behavior.

### DSG-016 `Implementation realization`

This specification package shall be realized as a concrete Rust crate in the
repository, and the implementation shall expose the typed API surface defined by
`DSG-001` through `DSG-015`.

### DSG-017 `Verification realization`

The repository shall include automated tests that realize the validation
entries in `docs/specs/rust-block-crate/validation.md`, with each validation
entry mapped to one or more executable tests.

### DSG-018 `EBCP-aware typed block model`

The typed block model continues to treat `EmbeddingSpec` as the protocol-owned
declaration of the stored branch-entry representation, including the EBCP
branch-only encodings defined by `docs/protocol/ebcp.md`.

The block crate preserves that declaration exactly and makes the enclosing
block's `ext` metadata available to downstream consumers as typed or otherwise
structured protocol data without deciding how indexing or search should use it.

### DSG-019 `EBCP validity gate`

During branch-block construction and hash-verified deserialization, the crate
enforces the EBCP-specific structural rules owned by `docs/protocol/ebcp.md`.

That includes:

- rejecting EBCP encodings on leaf blocks
- requiring the EBCP descriptor for EBCP branch blocks
- validating EBCP dimensionality and metadata shape
- validating that branch-entry payload lengths match the selected EBCP encoding
  and descriptor metadata

### DSG-020 `Policy-neutral consumer surface`

When a validated block uses an EBCP encoding, the block crate exposes the
decoded protocol metadata and raw branch payload bytes in a consumer-neutral
form. Search or indexing layers may then reconstruct or compare embeddings
according to their own subordinate specifications without re-parsing canonical
CBOR or bypassing block-level validation.

## Decode and Verification Flow

The deserialize path is:

1. accept `bytes` and `expected_hash`
2. compute `sha256(bytes)`
3. fail explicitly on mismatch
4. decode CBOR only for content accepted by hash
5. interpret integer wire keys according to the versioned protocol registry
6. validate protocol conformance
7. re-encode the decoded block to canonical CBOR and require byte-for-byte
   equality with the supplied `bytes`
8. reject the input if the supplied bytes are not the canonical encoding of the
   decoded block
9. return the typed block with its verified hash

This makes hash verification part of the crate trust boundary rather than an
optional caller-side convention, while still requiring the accepted bytes to be
the canonical encoding mandated by `docs/protocol/blocks.md`.

## Consumer Usage Model

### Indexing

Indexing uses:

- block construction APIs
- serialization APIs
- returned hashes for parent-link construction

### Search

Search uses:

- hash-verified deserialization
- typed block decomposition

Both consumers use the same typed model and protocol-conformance logic.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-001..009 | REQ-BLOCK-CRATE-001, 003, 004, 005, 007, 008, 011, 012, 013, 014 |
| DSG-010..011 | REQ-BLOCK-CRATE-002, 005, 007 |
| DSG-012 | REQ-BLOCK-CRATE-004, 009, 012 |
| DSG-013 | REQ-BLOCK-CRATE-003, 011, 013 |
| DSG-014 | REQ-BLOCK-CRATE-004, 011, 012 |
| DSG-015 | REQ-BLOCK-CRATE-003, 006 |
| DSG-016..017 | REQ-BLOCK-CRATE-010 |
| DSG-018 | REQ-BLOCK-CRATE-015, REQ-BLOCK-CRATE-018 |
| DSG-019 | REQ-BLOCK-CRATE-015, REQ-BLOCK-CRATE-016, REQ-BLOCK-CRATE-017 |
| DSG-020 | REQ-BLOCK-CRATE-006, REQ-BLOCK-CRATE-018 |
