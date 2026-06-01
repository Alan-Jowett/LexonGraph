# Rust Block Storage Trait Design

## Status

Draft design specification for a Rust trait crate that defines the LexonGraph
block-storage contract.

## Design Goals

The crate design is intended to be:

- backend-agnostic
- content-address aware
- strict about integrity
- minimal at the public boundary
- reusable by both indexing and search
- free of production backend implementations

## Crate Boundary

The crate owns:

- the public storage trait contract
- storage-oriented error taxonomy
- contract-level integrity rules for `put` and `get`
- internal-only test support for exercising the contract

The crate does not own:

- block canonicalization or wire encoding
- block validation rules beyond invoking the block crate's protocol checks
- indexing heuristics
- search traversal
- production backend adapters

## External Dependencies

### DSG-STORE-001 `Block crate dependency`

The storage trait crate depends on the block crate for:

- typed block values
- typed block identifiers
- canonical serialization and block-ID derivation during `put`
- verified decoding and protocol conformance during `get`

The storage trait crate does not redefine those behaviors.

## Core Types

### DSG-STORE-002 `BlockStore`

A trait representing the backend-agnostic contract for storing and retrieving
LexonGraph blocks by block ID.

### DSG-STORE-003 `BlockStoreError`

An explicit error taxonomy covering at least:

- backend access failure
- malformed stored content
- block-ID integrity mismatch
- other explicit contract violations that are not equivalent to absence

Explicit block absence is represented separately from error.

## API Surface

### DSG-STORE-004 `put(block) -> Result<BlockHash, BlockStoreError>`

`put` accepts a typed block value, derives its canonical identity through the
block crate, stores the block under that content-derived identifier, and returns
the block ID.

### DSG-STORE-005 `get(block_id) -> Result<Option<ValidatedBlock>, BlockStoreError>`

`get` accepts a block ID, retrieves the stored representation associated with
that ID, verifies that the retrieved content is valid for the requested block
ID through the block crate, and returns either:

- `Ok(Some(validated_block))` for a present and valid block
- `Ok(None)` when the block is absent
- `Err(...)` for explicit backend or integrity failure

## Behavioral Rules

### DSG-STORE-006 Immutability and idempotence

Because LexonGraph blocks are immutable and content-addressed, repeated `put`
operations for logically identical blocks return the same block ID and must not
create divergent observable content under that identifier.

### DSG-STORE-007 Integrity boundary

The storage trait's trust boundary is at `get`: a caller receives success only
after the retrieved content has been verified against the requested block ID by
the block crate.

Corruption, malformed bytes, or a mismatched block ID are explicit failures and
must not be downgraded to absence.

### DSG-STORE-008 Backend neutrality

The public contract is limited to `put` and `get` over typed block values and
block IDs.

The trait does not require or expose:

- file naming conventions
- directory layout
- SQL queries or schema knowledge
- blob-container structure
- bucket/key composition rules
- backend-native filtering or enumeration capabilities

This preserves portability across filesystem, sqlite, Azure Blob, S3, and
similar backends.

### DSG-STORE-009 Implementation restriction

No production backend implementation is part of this crate.

Concrete adapters for filesystem, sqlite, Azure Blob, S3, or other backends are
expected to live in separate crates that implement this trait.

### DSG-STORE-010 Internal test support

The crate may include an internal-only memory-backed implementation for use in
contract tests.

That implementation exists solely to validate the trait semantics and is not a
supported production backend surface.

## Consumer Usage Model

### Indexing

Indexing uses:

- `put` to persist typed blocks
- returned block IDs to construct parent references

### Search

Search uses:

- `get` to resolve root and child block IDs into validated typed blocks

Both consumers use the same backend-neutral contract.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-STORE-001 | REQ-BLOCK-STORE-002, REQ-BLOCK-STORE-005, REQ-BLOCK-STORE-011 |
| DSG-STORE-002..003 | REQ-BLOCK-STORE-001, REQ-BLOCK-STORE-007, REQ-BLOCK-STORE-008 |
| DSG-STORE-004 | REQ-BLOCK-STORE-002, REQ-BLOCK-STORE-003, REQ-BLOCK-STORE-006 |
| DSG-STORE-005 | REQ-BLOCK-STORE-002, REQ-BLOCK-STORE-004, REQ-BLOCK-STORE-005, REQ-BLOCK-STORE-008 |
| DSG-STORE-006 | REQ-BLOCK-STORE-003, REQ-BLOCK-STORE-006 |
| DSG-STORE-007 | REQ-BLOCK-STORE-005, REQ-BLOCK-STORE-008 |
| DSG-STORE-008 | REQ-BLOCK-STORE-001, REQ-BLOCK-STORE-007 |
| DSG-STORE-009 | REQ-BLOCK-STORE-009 |
| DSG-STORE-010 | REQ-BLOCK-STORE-010 |
