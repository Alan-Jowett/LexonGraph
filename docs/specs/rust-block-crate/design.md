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

Distinct typed block structs with shared block metadata and kind-specific entry
collections.

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
- invalid block kind or entry shape
- non-conforming block structure under the protocol rules

## API Surface

### DSG-010 `build_branch_block(...) -> Result<BranchBlock, BlockError>`

Constructs a branch block from required metadata and a typed branch-entry
collection, then validates protocol conformance.

### DSG-011 `build_leaf_block(...) -> Result<LeafBlock, BlockError>`

Constructs a leaf block from required metadata and a typed leaf-entry payload,
then validates protocol conformance.

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

Decomposes a validated block into block metadata and a typed entry collection
without embedding search- or indexing-specific behavior.

## Decode and Verification Flow

The deserialize path is:

1. accept `bytes` and `expected_hash`
2. compute `sha256(bytes)`
3. fail explicitly on mismatch
4. decode CBOR only for content accepted by hash
5. interpret integer wire keys according to the versioned protocol registry
6. validate protocol conformance
7. return the typed block with its verified hash

This makes hash verification part of the crate trust boundary rather than an
optional caller-side convention.

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
| DSG-001..009 | REQ-BLOCK-CRATE-001, 003, 004, 005, 007, 011, 012, 013 |
| DSG-010..011 | REQ-BLOCK-CRATE-002, 005, 007 |
| DSG-012 | REQ-BLOCK-CRATE-004, 009, 012 |
| DSG-013 | REQ-BLOCK-CRATE-003, 011, 013 |
| DSG-014 | REQ-BLOCK-CRATE-004, 011, 012 |
| DSG-015 | REQ-BLOCK-CRATE-003, 006 |
