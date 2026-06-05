<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
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
- `docs/specs/rust-dcbc-crate/`
- `docs/specs/rust-embeddings-trait/`

This document does not redefine block encoding, block identifiers,
storage-backend semantics, or the shared embedding-provider contract. Those
concerns remain owned by the block protocol, block crate, block-storage trait
crate, shared embeddings-trait crate, and the DCBC crate specification for the
default node-packing clustering behavior layered into this crate.

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
implements `docs/protocol/indexing.md`, including both:

- a monolithic indexing API
- staged block-construction APIs for incremental leaf construction and
  parent-layer construction

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

The crate shall also surface explicit failure for invalid staged invocations,
including empty staged batches, invalid child-block inputs, incompatible staged
input combinations, and staged construction attempts that cannot yield
protocol-conforming parent blocks.

### REQ-INDEXER-011

The crate shall keep protocol-required orchestration separate from
implementation-defined policy concerns through trait-based extension points.

Those extension points shall remain authoritative across the monolithic and
staged APIs, and the staged APIs shall not introduce hidden persistence or
hidden cross-call orchestration state.

### REQ-INDEXER-012

At minimum, the crate shall expose or depend on trait-governed policy
boundaries for content resolution, embedding generation, canonical-embedding
selection, and intermediate-node grouping or packing behavior.

The embedding-generation boundary shall be consumed from the shared
embeddings-trait crate rather than defined by the indexer crate itself,
including the shared trait crate's ordered batch-embedding semantics.

The node-packing boundary shall remain overridable by downstream consumers even
when the crate provides a built-in default implementation.

The staged APIs shall not require caller-supplied intermediate descriptors
beyond the constructed blocks themselves.

### REQ-INDEXER-013

The core indexer shall own the protocol-required orchestration, layering,
normalization, block construction, and block persistence flow, including the
default-construction path that wires in the crate's built-in
canonical-embedding and node-packing implementations.

The core indexer shall also own caller-visible status emission points for
long-running parent-layer construction work, including clustering performed
through the selected node-packing policy.

The monolithic API shall continue to realize that full flow, while the staged
APIs shall expose semantically equivalent decomposed portions of the same
protocol-conforming construction behavior.

When multiple indexing items are provided, the core indexer may realize
embedding generation through internal batching while preserving one leaf block
per input item and the same externally observable semantics.

### REQ-INDEXER-014

Given the same logical item set, metadata, content references resolving to the
same logical content, `embedding_spec`, block size target, and deterministic
trait implementations within the same indexing context, the crate shall produce
the same root block ID and the same persisted block set.

When embedding generation is delegated to a provider supplied through the shared
embeddings-trait crate, determinism is defined over the provider behavior and
configuration that affect the produced embedding output.

That determinism boundary also includes any indexer or provider batching
behavior that affects the produced embedding output.

This determinism requirement also applies to staged invocations: repeating the
same staged call with the same logical inputs shall produce the same block
content and block identifiers without relying on cross-call duplicate tracking.

That determinism guarantee shall hold even when the implementation uses
internal parallelism; scheduling differences shall not change grouping results,
block content, explicit failure behavior, the root block ID, or the complete
persisted block set.

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
- canonical-embedding selection
- node packing

### REQ-INDEXER-018

The reusable conformance-test harnesses shall be exposed through an opt-in,
non-default, test-oriented surface so downstream implementers can use them in
tests without broadening the crate's default production-facing API.

### REQ-INDEXER-019

The crate shall not redefine or duplicate reusable conformance-test contracts
for dependency surfaces already owned by subordinate specifications, including
the block crate, block-storage trait crate, and embeddings-trait crate.

### REQ-INDEXER-020

The crate shall depend on the shared embeddings-trait crate for the
embedding-provider contract used by indexing.

### REQ-INDEXER-021

The crate shall not bundle provider-specific embedding implementations or
embedding-provider conformance helpers that are owned by the shared
embeddings-trait crate or provider-specific crates layered on top of it.

### REQ-INDEXER-022

The crate shall depend on the shared `lexongraph-dcbc` crate for its built-in
default `NodePackingPolicy` realization rather than reimplementing DCBC
clustering semantics locally inside the indexer crate.

### REQ-INDEXER-023

The crate shall provide a built-in default `NodePackingPolicy` implementation
that uses `lexongraph-dcbc` to derive deterministic candidate child groups for
intermediate-node construction from current-layer child embeddings.

The built-in default implementation may realize behavior-preserving internal
parallelism while deriving those candidate groups.

### REQ-INDEXER-024

The crate shall provide a primary default-instantiation path for the indexer
runtime API that uses the built-in arithmetic-mean `CanonicalEmbeddingPolicy`
and the built-in DCBC-backed `NodePackingPolicy` without requiring callers to
pass either policy explicitly.

### REQ-INDEXER-025

The crate shall continue to provide an explicit API path that accepts a
caller-supplied `NodePackingPolicy`, allowing downstream users to replace the
built-in default node-packing behavior.

### REQ-INDEXER-026

The built-in DCBC-backed node-packing implementation shall remain subordinate
to the core indexer's protocol-conformance checks. It may propose candidate
groupings, but the core indexer shall continue to own enforcement of minimum
child count, maximum serialized size, ordering, deduplication, and explicit
failure semantics for invalid candidates.

### REQ-INDEXER-027

Given the same ordered current-layer children, embedding bytes, block size
target, and deterministic DCBC dependency behavior, the built-in default
node-packing implementation shall produce the same candidate grouping result or
the same explicit failure.

This guarantee explicitly includes executions that use behavior-preserving
internal parallelism.

### REQ-INDEXER-037

The crate shall provide an optional caller-supplied status observer contract
for indexing progress.

The observer contract shall be reusable across the monolithic indexing API and
the staged parent-construction API.

### REQ-INDEXER-038

When parent-layer construction performs clustering work whose runtime is
non-trivial, the crate shall emit periodic in-progress status updates while
that clustering work remains active rather than only reporting terminal state.

### REQ-INDEXER-039

Status updates shall be emitted as structured data suitable for arbitrary
caller-owned handling.

The crate shall not require any particular sink such as stdout, a tracing
framework, Azure storage, or another repository-specific telemetry backend.

### REQ-INDEXER-040

Internal parallelism for clustering-related work shall be limited to
decompositions that preserve protocol conformance, normalization invariants,
explicit failure semantics, and reproducibility requirements.

### REQ-INDEXER-028

The crate shall provide a built-in default `CanonicalEmbeddingPolicy`
implementation whose canonical embedding for a produced child-bearing block is
the component-wise arithmetic mean of the embeddings stored in that block's
finalized entries.

### REQ-INDEXER-029

The built-in arithmetic-mean canonical-embedding implementation shall decode
stored entry embeddings according to the block `embedding_spec`, compute the
mean in deterministic entry order using `f64`, re-encode the result for
supported arithmetic encodings (`i8`, `f16le`, `f32le`), and fail explicitly
for empty entry sets, unsupported encodings, non-finite values, or results that
cannot be represented under the block `embedding_spec`.

### REQ-INDEXER-030

The crate shall provide a staged API that accepts a non-empty batch of indexing
items and incrementally constructs the corresponding leaf blocks from source
material, including content resolution and embedding generation, without
persisting those blocks as part of the staged API contract.

The staged API does not require caller-managed embedding sub-batches in this
revision.

### REQ-INDEXER-031

Given the same logical item set and indexing context, partitioning items across
multiple staged leaf-construction calls shall produce the same leaf-block set
that the monolithic indexing flow would have produced for those items.

### REQ-INDEXER-032

The crate shall provide a staged API that accepts a collection of already
constructed child blocks and produces the next parent layer without requiring
store lookups or caller-supplied intermediate descriptors.

The parent-construction API shall derive the required child-link and embedding
inputs from the supplied blocks themselves.

### REQ-INDEXER-033

The staged parent-construction API shall accept any protocol-valid current-layer
child set whose inputs all share one decoded child level and are otherwise
compatible within one indexing context. The constructed parent level shall be
that shared child level plus one.

### REQ-INDEXER-034

The staged APIs shall be resumable through explicit artifact passing alone: a
caller may persist or reload produced blocks outside the crate and later resume
construction by supplying those blocks to the next staged API without relying on
hidden in-memory state from earlier calls.

### REQ-INDEXER-035

Repeated application of the staged APIs over the same logical indexing context
shall be observationally equivalent to the monolithic API, producing the same
protocol-conforming block contents, root block ID, and complete block set.

### REQ-INDEXER-036

The consumer-facing indexing API surface shall remain collection-based in this
revision: callers pass collections of items to index, while any embedding batch
size or chunking decisions remain internal to the indexer and embedding
provider.

## Out of Scope

This crate does not define or own:

- block wire encoding or block validity rules
- block-ID derivation rules
- storage backend implementations
- search traversal or ranking behavior
- the shared embedding-provider trait contract
- provider-specific embedding implementations such as OpenAI-compatible clients
- any single required embedding model, endpoint, deployment configuration, or
  runtime for all consumers
- any single required canonical-embedding algorithm
- any single required grouping, clustering, routing, or packing strategy
- reusable conformance contracts already owned by the block crate,
  block-storage trait crate, or embeddings-trait crate

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/indexing.md` and
`docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/`,
`docs/specs/rust-block-storage-trait/`, `docs/specs/rust-dcbc-crate/`, and
`docs/specs/rust-embeddings-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
