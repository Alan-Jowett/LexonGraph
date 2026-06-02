# Rust Indexer Crate Requirements

## Status

Draft specification for a Rust crate that implements the LexonGraph indexing
protocol for constructing immutable block sets from application-supplied items.

## Scope

This document specifies the crate-level requirements for a Rust crate that
implements `docs/protocol/indexing.md`.

This document is layered on top of:

- `docs/protocol/indexing.md`
- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`

This document does not redefine block encoding, block identifiers, or
storage-backend semantics. Those concerns remain owned by the block protocol,
block crate, and block-storage trait crate.

## Terminology

In this spec package, `content reference` means an application-defined value
carried by an indexing item and used to resolve the concrete content that will
be indexed.

`Indexing context` means the logical environment in which an indexing run is
performed, including the item set, metadata, content-resolution behavior,
embedding behavior, `embedding_spec`, and block size target.

## Requirements

### REQ-INDEXER-001

The crate shall define the Rust API boundary for a LexonGraph indexer that
implements `docs/protocol/indexing.md`.

### REQ-INDEXER-002

The crate shall remain subordinate to `docs/protocol/indexing.md` for
index-construction invariants and to `docs/protocol/blocks.md` for block
semantics, canonicalization, and validity rules.

### REQ-INDEXER-003

The crate shall depend on the block crate for typed block construction,
canonical serialization, validation, and block-ID derivation.

### REQ-INDEXER-004

The crate shall depend on the block-storage trait crate for immutable
persistence of produced blocks and block retrieval required by indexing.

### REQ-INDEXER-005

The crate shall not redefine block wire encoding, block-ID derivation,
block-storage backend semantics, or search traversal behavior.

### REQ-INDEXER-006

The crate shall accept a non-empty collection of indexing items.

### REQ-INDEXER-007

Each indexing item shall carry application metadata plus a content reference at
the public API boundary.

### REQ-INDEXER-008

This revision shall use a reference-based input model and shall not require raw
content bytes or inline content bodies to be passed directly in the input
collection.

### REQ-INDEXER-009

The crate shall require a pluggable content-resolution trait that accepts an
item's content reference and returns the concrete content used for indexing.

### REQ-INDEXER-010

The crate shall surface explicit failure when the input item set is empty or
when content resolution fails, is inaccessible, or returns content unusable for
indexing.

### REQ-INDEXER-011

The crate shall keep protocol-required orchestration separate from
implementation-defined policy concerns through trait-based extension points.

### REQ-INDEXER-012

At minimum, the crate shall expose trait-governed policy boundaries for content
resolution, embedding generation, canonical-embedding selection, and
intermediate-node grouping or packing behavior.

### REQ-INDEXER-013

The core indexer shall own the protocol-required orchestration, layering,
normalization, block construction, and block persistence flow.

### REQ-INDEXER-014

Given the same logical item set, metadata, content references resolving to the
same logical content, `embedding_spec`, block size target, and deterministic
trait implementations within the same indexing context, the crate shall produce
the same root block ID and the same persisted block set.

### REQ-INDEXER-015

The repository shall include automated verification artifacts that realize the
validation surface defined in `docs/specs/rust-indexer-crate/validation.md`.

### REQ-INDEXER-016

In this revision, successful content resolution shall supply the media type and
bytes stored inline in the produced leaf entry's `content` payload.

### REQ-INDEXER-017

The crate shall provide reusable conformance-test harnesses for the
implementation-defined policy traits it defines for:

- content resolution
- embedding generation
- canonical-embedding selection
- node packing

### REQ-INDEXER-018

The reusable conformance-test harnesses shall be exposed through an opt-in,
non-default, test-oriented surface so downstream implementers can use them in
tests without broadening the crate's default production-facing API.

### REQ-INDEXER-019

The crate shall not redefine or duplicate reusable conformance-test contracts
for dependency surfaces already owned by subordinate specifications, including
the block crate and block-storage trait crate.

## Out of Scope

This crate does not define or own:

- block wire encoding or block validity rules
- block-ID derivation rules
- storage backend implementations
- search traversal or ranking behavior
- any single required embedding model or embedding runtime
- any single required canonical-embedding algorithm
- any single required grouping, clustering, routing, or packing strategy
- reusable conformance contracts already owned by the block crate or
  block-storage trait crate

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/indexing.md` and
`docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
