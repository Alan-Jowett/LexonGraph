<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Indexer Crate Design

## Status

Draft design specification for a Rust crate that implements the LexonGraph
indexing protocol.

## Design Goals

The crate design is intended to be:

- protocol-conforming
- deterministic at the crate boundary
- explicit about policy seams
- reusable across storage backends
- strict about normalization invariants
- minimal at the public API boundary

## Crate Boundary

The crate owns:

- indexing-oriented public types
- indexing orchestration
- normalization required by the indexing protocol
- block construction and persistence flow
- indexer-oriented error taxonomy

The crate does not own:

- block wire encoding or block-ID derivation
- block validation rules beyond invoking the block crate
- storage backend implementations
- search traversal or ranking
- any required embedding, clustering, grouping, or routing algorithm

## External Dependencies

### DSG-INDEXER-001 `Protocol dependency boundary`

The indexer crate depends on the protocol documents for normative indexing and
block invariants. It implements those constraints and does not redefine them.

### DSG-INDEXER-002 `Crate dependencies`

The indexer crate depends on:

- the block crate for typed block values, canonical serialization, block-ID
  derivation, and protocol-conforming block validation
- the block-storage trait crate for backend-agnostic block persistence and
  retrieval

## Core Types

### DSG-INDEXER-003 `IndexItem`

A public input type representing one application-supplied indexing unit. Each
value contains:

- application metadata
- a content reference

This revision does not require inline content at the input boundary.

### DSG-INDEXER-004 `IndexingResult`

A successful indexing result containing at least:

- the root block ID
- the produced block set, or the produced block IDs sufficient to identify that
  set

### DSG-INDEXER-005 `IndexerError`

An explicit error taxonomy covering at least:

- empty input
- content-resolution failure
- unusable resolved content
- embedding-generation failure
- canonical-embedding selection failure
- node-packing policy failure
- block-construction failure
- storage failure

## Policy Traits

### DSG-INDEXER-006 `ContentResolver`

A trait that accepts a content reference from an `IndexItem` and returns the
concrete content to be indexed for that item.

Resolution is the extension point that allows content to originate from memory,
filesystem, Azure Blob, S3, or similar systems without exposing those backend
details in the indexer API.

In this revision, the resolved content includes the media type and bytes that
will be stored inline in the produced leaf entry's `content` payload.

### DSG-INDEXER-007 `EmbeddingProvider`

A trait that accepts resolved content plus the indexing context needed for
embedding generation and returns an embedding compatible with the supplied
`embedding_spec`.

The indexer crate requires this trait boundary but does not define the model,
feature extraction pipeline, or runtime.

### DSG-INDEXER-008 `CanonicalEmbeddingPolicy`

A trait that derives the canonical embedding for a produced block when that
embedding is needed as the parent-entry embedding for a child block.

The indexer crate requires the result to be deterministic, comparable within
the indexing context, and stable across rebuilds of the same logical content.

### DSG-INDEXER-009 `NodePackingPolicy`

A trait that determines how leaf blocks or lower-layer node blocks are grouped
into candidate intermediate blocks.

This trait owns implementation-defined grouping and packing choices, but the
core indexer remains responsible for enforcing protocol invariants such as
minimum child count, maximum serialized size, ordering, and deduplication.

## API Surface

### DSG-INDEXER-010 `Indexer`

A public orchestration type or trait exposing an indexing operation that
accepts:

- a non-empty collection of `IndexItem` values
- an `EmbeddingSpec`
- a block size target
- a block store implementation
- implementations of the required policy traits

and returns `Result<IndexingResult, IndexerError>`.

## Orchestration Flow

### DSG-INDEXER-011 `Core indexing pipeline`

The fixed orchestration flow is:

1. reject empty input explicitly
2. for each indexing item, resolve its content reference
3. generate one item-level embedding compatible with `embedding_spec`
4. construct exactly one leaf block containing exactly one leaf entry derived
   from that item and storing the resolved content inline
5. persist each produced leaf block through the block store
6. if one leaf block exists, return that leaf block as the root
7. otherwise, repeatedly invoke the node-packing policy to obtain candidate
   child groups for the current layer
8. derive each child-bearing block's canonical embedding through the
   canonical-embedding policy
9. normalize candidate node entries by sorting by raw embedding bytes and
   deduplicating by child block ID
10. construct and persist intermediate node blocks under the protocol-defined
    size limit
11. repeat until exactly one root block remains

The core indexer owns this flow even when implementation-defined policy traits
participate in individual steps.

### DSG-INDEXER-012 `Determinism boundary`

Conformance requires deterministic behavior from the resolver and policy traits
within a given indexing context.

If those trait implementations are deterministic and the logical inputs are the
same, the indexer produces the same root block ID and the same persisted block
set.

### DSG-INDEXER-013 `Implementation realization`

This specification package shall be realized as a concrete Rust crate in the
repository, and the implementation shall expose the public API boundary,
orchestration behavior, and policy traits defined by this document.

### DSG-INDEXER-014 `Verification realization`

The repository shall include automated tests that realize the validation
entries in `docs/specs/rust-indexer-crate/validation.md`, with each validation
entry mapped to one or more executable tests.

### DSG-INDEXER-015 `Feature-gated conformance module`

The crate exposes a public conformance-test helper surface behind a non-default
Cargo feature intended for downstream tests only.

That feature is not part of the default runtime API and does not change the
production-facing indexing contract.

### DSG-INDEXER-016 `Harness shape`

The conformance-test helper surface provides reusable checks for the
`ContentResolver`, `EmbeddingProvider`, `CanonicalEmbeddingPolicy`, and
`NodePackingPolicy` trait contracts.

To verify those trait contracts without requiring production implementations in
the crate, the helper surface may define test-only harness contracts that
supply deterministic fixtures, trait implementations under test, and any
policy-specific assertions needed for the validation cases.

The helper surface does not redefine conformance for the block crate or the
block-storage trait crate, which continue to own their respective reusable
conformance contracts.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-INDEXER-001 | REQ-INDEXER-001, REQ-INDEXER-002, REQ-INDEXER-005 |
| DSG-INDEXER-002 | REQ-INDEXER-003, REQ-INDEXER-004, REQ-INDEXER-005 |
| DSG-INDEXER-003..005 | REQ-INDEXER-001, REQ-INDEXER-006, REQ-INDEXER-007, REQ-INDEXER-008, REQ-INDEXER-010 |
| DSG-INDEXER-006 | REQ-INDEXER-007, REQ-INDEXER-008, REQ-INDEXER-009, REQ-INDEXER-010, REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-016 |
| DSG-INDEXER-007 | REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-014 |
| DSG-INDEXER-008 | REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-014 |
| DSG-INDEXER-009 | REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-013 |
| DSG-INDEXER-010 | REQ-INDEXER-001, REQ-INDEXER-004, REQ-INDEXER-006, REQ-INDEXER-010, REQ-INDEXER-011, REQ-INDEXER-013 |
| DSG-INDEXER-011 | REQ-INDEXER-001, REQ-INDEXER-006, REQ-INDEXER-009, REQ-INDEXER-010, REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-013, REQ-INDEXER-016 |
| DSG-INDEXER-012 | REQ-INDEXER-014 |
| DSG-INDEXER-013 | REQ-INDEXER-001 |
| DSG-INDEXER-014 | REQ-INDEXER-015 |
| DSG-INDEXER-015..016 | REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-017, REQ-INDEXER-018, REQ-INDEXER-019 |

