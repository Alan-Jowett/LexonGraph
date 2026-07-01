<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
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
- contract-level integrity rules for `put`, `get`, and identifier enumeration
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

### DSG-STORE-006
`iter_block_ids() -> Result<Box<dyn Iterator<Item = Result<BlockHash, BlockStoreError>> + '_>, BlockStoreError>`

`iter_block_ids` begins backend-neutral streaming enumeration of stored block
identifiers and returns an iterator-like surface whose items are either:

- `Ok(block_id)` for one observed stored block identifier
- `Err(...)` for an explicit backend failure encountered during enumeration

Starting enumeration may itself fail explicitly before any item is produced.

The yielded values are identifiers only; callers that need to distinguish leaf
blocks, branch blocks, roots, or other structural roles do so by combining the
enumerated IDs with existing `get` calls and caller-owned analysis.

## Behavioral Rules

### DSG-STORE-007 Immutability and idempotence

Because LexonGraph blocks are immutable and content-addressed, repeated `put`
operations for logically identical blocks return the same block ID and must not
create divergent observable content under that identifier.

### DSG-STORE-008 Integrity boundary

The storage trait's trust boundary is at `get`: a caller receives success only
after the retrieved content has been verified against the requested block ID by
the block crate.

Corruption, malformed bytes, or a mismatched block ID are explicit failures and
must not be downgraded to absence.

### DSG-STORE-009 Backend neutrality

The public contract is limited to `put`, `get`, and identifier enumeration over
typed block values and block IDs.

The trait does not require or expose:

- file naming conventions
- directory layout
- SQL queries or schema knowledge
- blob-container structure
- bucket/key composition rules
- backend-native filtering capabilities

This preserves portability across filesystem, sqlite, Azure Blob, S3, and
similar backends.

### DSG-STORE-010 `Enumeration semantics`

Enumeration is a whole-store capability over stored block identifiers rather
than a traversal API over an indexing root or reachable subtree.

This revision intentionally leaves the following unspecified:

- enumeration order
- snapshot consistency across concurrent writes
- whether newly published blocks may appear after enumeration begins
- any root, level, cluster, leaf, or branch classification

Those concerns remain owned by higher-level callers or future specifications.

### DSG-STORE-011 Implementation restriction

No production backend implementation is part of this crate.

Concrete adapters for filesystem, sqlite, Azure Blob, S3, or other backends are
expected to live in separate crates that implement this trait.

### DSG-STORE-012 Internal test support

The crate may include an internal-only memory-backed implementation for use in
contract tests.

That implementation exists solely to validate the trait semantics and is not a
supported production backend surface.

### DSG-STORE-013 `Feature-gated conformance module`

The crate exposes a public conformance-test helper surface behind a non-default
Cargo feature intended for downstream tests only.

That feature is not part of the default crate surface and does not change the
runtime `BlockStore` contract used by production consumers.

### DSG-STORE-014 `Harness shape`

The conformance-test helper surface provides reusable checks for the validation
entries in this spec package.

To cover integrity-mismatch and malformed-content scenarios without adding
backend-specific methods to `BlockStore`, the helper surface may define a
test-only harness contract that supplies:

- a way to create a fresh store instance for an individual conformance case
- a way to seed raw stored bytes under a chosen block ID for corruption tests

The helper surface may also provide convenience runners that bundle multiple
validation cases into a single invocation when the caller supplies the required
test hooks.

To cover enumeration without adding backend-specific methods to `BlockStore`,
the helper surface may define shared checks that consume the production
enumeration method and compare the observed ID set against a caller-managed
expected set.

### DSG-STORE-015 `Raw-byte production boundary`

The production `BlockStore` trait stores and retrieves canonical block bytes
under caller-supplied block identifiers.

The trait does not require concrete backends to parse block protocol versions or
reserved/custom block types.

### DSG-STORE-016 `Shared typed helper layer`

The trait crate may expose shared helper methods layered on top of the raw-byte
boundary for common version-1 and version-aware decode/encode flows.

Those helpers centralize codec dispatch so concrete backends remain
protocol-agnostic while higher-level callers retain a convenient typed surface.

When helpers decode stored bytes, they determine the block version from the
top-level canonical CBOR envelope and then apply the corresponding versioned
block protocol rules from the block crate.

## Consumer Usage Model

### Indexing

Indexing uses:

- `put` to persist typed blocks
- returned block IDs to construct parent references

### Search

Search uses:

- `get` to resolve root and child block IDs into validated typed blocks

### Analysis and maintenance

Analysis-oriented callers use:

- `iter_block_ids` to discover stored block IDs across the backend
- `get` to classify or analyze those IDs as leaf blocks, branch blocks, roots,
  levels, or clusters according to caller-owned logic

Both consumers use the same backend-neutral contract.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-STORE-001 | REQ-BLOCK-STORE-002, REQ-BLOCK-STORE-005, REQ-BLOCK-STORE-011 |
| DSG-STORE-002..003 | REQ-BLOCK-STORE-001, REQ-BLOCK-STORE-007, REQ-BLOCK-STORE-008 |
| DSG-STORE-004 | REQ-BLOCK-STORE-002, REQ-BLOCK-STORE-003, REQ-BLOCK-STORE-006 |
| DSG-STORE-005 | REQ-BLOCK-STORE-002, REQ-BLOCK-STORE-004, REQ-BLOCK-STORE-005, REQ-BLOCK-STORE-008 |
| DSG-STORE-006 | REQ-BLOCK-STORE-001, REQ-BLOCK-STORE-014, REQ-BLOCK-STORE-015, REQ-BLOCK-STORE-017 |
| DSG-STORE-007 | REQ-BLOCK-STORE-003, REQ-BLOCK-STORE-006 |
| DSG-STORE-008 | REQ-BLOCK-STORE-005, REQ-BLOCK-STORE-008 |
| DSG-STORE-009 | REQ-BLOCK-STORE-001, REQ-BLOCK-STORE-007, REQ-BLOCK-STORE-016 |
| DSG-STORE-010 | REQ-BLOCK-STORE-015, REQ-BLOCK-STORE-017, REQ-BLOCK-STORE-018 |
| DSG-STORE-011 | REQ-BLOCK-STORE-009 |
| DSG-STORE-012 | REQ-BLOCK-STORE-010 |
| DSG-STORE-013 | REQ-BLOCK-STORE-013 |
| DSG-STORE-014 | REQ-BLOCK-STORE-012, REQ-BLOCK-STORE-013, REQ-BLOCK-STORE-014, REQ-BLOCK-STORE-017 |
