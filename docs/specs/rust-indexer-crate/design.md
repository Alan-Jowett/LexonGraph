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
- compatible with asynchronously realized embedding providers
- strict about normalization invariants
- minimal at the public API boundary

## Crate Boundary

The crate owns:

- indexing-oriented public types
- indexing orchestration
- normalization required by the indexing protocol
- block construction and persistence flow
- indexer-oriented error taxonomy
- conformance helpers for indexer-owned policy traits

The crate does not own:

- block wire encoding or block-ID derivation
- block validation rules beyond invoking the block crate
- storage backend implementations
- search traversal or ranking
- the shared embedding-provider trait contract
- provider-specific embedding implementations
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
- the DCBC crate for the built-in default node-packing clustering engine
- the embeddings-trait crate for the shared embedding-provider contract used by
  indexing

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

## Policy Traits and Dependency Boundaries

### DSG-INDEXER-006 `ContentResolver`

A trait that accepts a content reference from an `IndexItem` and returns the
concrete content to be indexed for that item.

Resolution is the extension point that allows content to originate from memory,
filesystem, Azure Blob, S3, or similar systems without exposing those backend
details in the indexer API.

In this revision, the resolved content includes the media type and bytes that
will be stored inline in the produced leaf entry's `content` payload.

### DSG-INDEXER-007 `EmbeddingProvider dependency`

The indexer crate consumes an embedding-provider implementation that satisfies
the shared contract defined by the embeddings-trait crate.

The indexer crate does not define the embedding-provider trait itself and does
not own provider-specific realization details such as OpenAI-compatible request
construction.

### DSG-INDEXER-008 `CanonicalEmbeddingPolicy`

A trait that derives the canonical embedding for a produced block when that
embedding is needed as the parent-entry embedding for a child block.

The indexer crate requires the result to be deterministic, comparable within
the indexing context, and stable across rebuilds of the same logical content.

The crate also provides a built-in default realization of this trait. That
default operates on the branch block's finalized stored entries rather than on
pre-normalization candidate inputs.

### DSG-INDEXER-009 `NodePackingPolicy`

A trait that determines how leaf blocks or lower-layer node blocks are grouped
into candidate intermediate blocks.

This trait owns implementation-defined grouping and packing choices, but the
core indexer remains responsible for enforcing protocol invariants such as
minimum child count, maximum serialized size, ordering, and deduplication.

The crate also provides a built-in default implementation of this trait backed
by the shared DCBC crate, while continuing to accept downstream
implementations.

## API Surface

### DSG-INDEXER-010 `Indexer`

A public orchestration type or trait exposing an indexing operation that
accepts:

- a non-empty collection of `IndexItem` values
- an `EmbeddingSpec`
- a block size target
- a block store implementation
- implementations of the required policy traits
- an embedding-provider implementation satisfying the shared
  embeddings-trait contract

and returns an awaitable result equivalent to
`Result<IndexingResult, IndexerError>`.

The public API includes both:

- a primary default-instantiation path that supplies the built-in
  arithmetic-mean canonical-embedding policy and the built-in DCBC-backed
  node-packing policy automatically
- a constructor path that accepts a caller-supplied `CanonicalEmbeddingPolicy`
  while continuing to use the built-in DCBC-backed node-packing policy
- an explicit override path that accepts caller-supplied
  `CanonicalEmbeddingPolicy` and `NodePackingPolicy`

## Orchestration Flow

### DSG-INDEXER-011 `Core indexing pipeline`

The fixed orchestration flow is:

1. reject empty input explicitly
2. for each indexing item, resolve its content reference
3. await one item-level embedding compatible with `embedding_spec` from the
   supplied embedding provider
4. construct exactly one leaf block containing exactly one leaf entry derived
   from that item and storing the resolved content inline
5. persist each produced leaf block through the block store
6. if one leaf block exists, return that leaf block as the root
7. otherwise, repeatedly invoke the selected node-packing policy, whether the
   built-in default or a caller-supplied override, to obtain candidate child
   groups for the current layer
8. normalize candidate node entries by sorting by raw embedding bytes and
   deduplicating by child block ID
9. construct each intermediate branch block from those finalized entries and
   validate it against the block protocol
10. derive each child-bearing block's canonical embedding through the
   canonical-embedding policy applied to that finalized branch block
11. construct and persist intermediate node blocks under the protocol-defined
   size limit
12. repeat until exactly one root block remains

The core indexer owns this flow even when implementation-defined policy traits
participate in individual steps.

### DSG-INDEXER-012 `Determinism boundary`

Conformance requires deterministic behavior from the resolver, embedding
provider, and policy traits within a given indexing context.

If those trait implementations are deterministic and the logical inputs are the
same, the indexer produces the same root block ID and the same persisted block
set.

For remotely backed embedding providers, the relevant indexing context includes
provider configuration that can affect the embedding output, but the ownership
of that configuration contract remains with the embeddings-trait crate and any
provider-specific crate layered above it.

### DSG-INDEXER-013 `Implementation realization`

This specification package shall be realized as a concrete Rust crate in the
repository, and the implementation shall expose the public API boundary,
orchestration behavior, and indexer-owned policy traits defined by this
document.

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
`ContentResolver`, `CanonicalEmbeddingPolicy`, and `NodePackingPolicy` trait
contracts.

To verify those trait contracts without requiring production implementations in
the crate, the helper surface may define test-only harness contracts that
supply deterministic fixtures, trait implementations under test, and any
policy-specific assertions needed for the validation cases.

The helper surface does not redefine conformance for the block crate,
block-storage trait crate, or embeddings-trait crate, which continue to own
their respective reusable conformance contracts.

### DSG-INDEXER-017 `Embedding-provider non-ownership`

The indexer crate accepts embedding providers through the shared
embeddings-trait contract but does not include provider-specific implementations
or provider-specific conformance helpers in its default or opt-in API surface.

### DSG-INDEXER-018 `Default DCBC-backed node-packing realization`

The crate exposes a built-in default `NodePackingPolicy` implementation that
uses the shared DCBC crate to cluster current-layer child embeddings into
candidate groups.

That realization derives the DCBC input vectors and scalar parameters
deterministically from the ordered child layer and block size target, invokes
the DCBC crate through its public API, and maps the DCBC assignment result back
into child-index groups without changing the core indexer's ownership of
protocol normalization or block-validity checks.

### DSG-INDEXER-019 `Primary default constructor path`

The crate exposes a primary constructor or equivalent default-instantiation API
for `Indexer` that requires the resolver and embedding provider, and internally
instantiates both the built-in arithmetic-mean canonical-embedding policy and
the built-in DCBC-backed node-packing policy.

This path is the default production-facing way to construct the indexer when no
custom canonical-embedding or node-packing behavior is required.

### DSG-INDEXER-020 `Custom canonical-policy constructor path`

The crate also exposes a constructor or equivalent API that accepts a
caller-supplied `CanonicalEmbeddingPolicy` while continuing to instantiate the
built-in DCBC-backed node-packing policy.

Using that path preserves the canonical-embedding policy seam for downstream
consumers that need a non-default routing representative without also requiring
custom node-packing behavior.

### DSG-INDEXER-021 `Explicit full-policy override path`

The crate also exposes an explicit constructor or equivalent API that accepts
caller-supplied `CanonicalEmbeddingPolicy` and `NodePackingPolicy`
implementations.

Using that override path preserves the existing policy seams for downstream
consumers that need non-default canonical embedding and grouping or packing
behavior.

### DSG-INDEXER-022 `Built-in arithmetic-mean canonical policy`

The crate exposes a built-in default `CanonicalEmbeddingPolicy`
implementation that computes a branch block's canonical embedding as the
component-wise arithmetic mean of the embeddings stored in that block's
finalized entries.

The branch block must already reflect protocol-required entry normalization, so
the default canonical embedding is deterministic over persisted block content.

### DSG-INDEXER-023 `Deterministic numeric encoding for built-in canonical means`

The built-in arithmetic-mean canonical policy:

1. decodes stored entry embeddings according to the branch block
   `embedding_spec`
2. computes each component mean in `f64` using deterministic finalized entry
   order
3. re-encodes supported outputs as follows:
   - `f32le`: direct `f64` to IEEE-754 binary32 cast, written little-endian
   - `f16le`: direct `f64` to IEEE-754 binary16 conversion, written
     little-endian
   - `i8`: round to nearest integer with midpoint ties away from zero, then
     encode as signed 8-bit
4. fails explicitly for empty branch-entry sets, unsupported encodings,
   non-finite decoded or reduced values, and means that overflow the target
   encoding

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-INDEXER-001 | REQ-INDEXER-001, REQ-INDEXER-002, REQ-INDEXER-005 |
| DSG-INDEXER-002 | REQ-INDEXER-003, REQ-INDEXER-004, REQ-INDEXER-005, REQ-INDEXER-020, REQ-INDEXER-022 |
| DSG-INDEXER-003..005 | REQ-INDEXER-001, REQ-INDEXER-006, REQ-INDEXER-007, REQ-INDEXER-008, REQ-INDEXER-010 |
| DSG-INDEXER-006 | REQ-INDEXER-007, REQ-INDEXER-008, REQ-INDEXER-009, REQ-INDEXER-010, REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-016 |
| DSG-INDEXER-007 | REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-020, REQ-INDEXER-021 |
| DSG-INDEXER-008 | REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-014, REQ-INDEXER-028 |
| DSG-INDEXER-009 | REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-013, REQ-INDEXER-023, REQ-INDEXER-026, REQ-INDEXER-027 |
| DSG-INDEXER-010 | REQ-INDEXER-001, REQ-INDEXER-004, REQ-INDEXER-006, REQ-INDEXER-010, REQ-INDEXER-011, REQ-INDEXER-013, REQ-INDEXER-020, REQ-INDEXER-024, REQ-INDEXER-025, REQ-INDEXER-028 |
| DSG-INDEXER-011 | REQ-INDEXER-001, REQ-INDEXER-006, REQ-INDEXER-009, REQ-INDEXER-010, REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-013, REQ-INDEXER-016, REQ-INDEXER-020, REQ-INDEXER-026, REQ-INDEXER-028 |
| DSG-INDEXER-012 | REQ-INDEXER-014 |
| DSG-INDEXER-013 | REQ-INDEXER-001 |
| DSG-INDEXER-014 | REQ-INDEXER-015 |
| DSG-INDEXER-015..016 | REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-017, REQ-INDEXER-018, REQ-INDEXER-019 |
| DSG-INDEXER-017 | REQ-INDEXER-019, REQ-INDEXER-021 |
| DSG-INDEXER-018 | REQ-INDEXER-022, REQ-INDEXER-023, REQ-INDEXER-026, REQ-INDEXER-027 |
| DSG-INDEXER-019 | REQ-INDEXER-024 |
| DSG-INDEXER-020 | REQ-INDEXER-024, REQ-INDEXER-025 |
| DSG-INDEXER-021 | REQ-INDEXER-025 |
| DSG-INDEXER-022 | REQ-INDEXER-013, REQ-INDEXER-024, REQ-INDEXER-028 |
| DSG-INDEXER-023 | REQ-INDEXER-028, REQ-INDEXER-029 |
