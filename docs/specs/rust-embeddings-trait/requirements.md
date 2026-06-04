<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Embeddings Trait Requirements

## Status

Draft specification for a Rust crate that defines the shared LexonGraph
embedding-provider contract.

## Scope

This document specifies the crate-level requirements for a Rust crate that owns
the shared embedding-provider trait boundary used by LexonGraph consumers such
as indexers and search-oriented clients.

This document is layered on top of:

- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`

This document does not redefine block encoding, block identifiers, or any
provider-specific embedding API. Those concerns remain owned by the block
protocol, block crate, and provider-specific crates layered above this package.

## Terminology

In this spec package, `embedding input` means the caller-supplied media type and
bytes that an embedding provider consumes to produce an embedding compatible
with an `EmbeddingSpec`.

## Requirements

### REQ-EMBED-TRAIT-001

The repository shall define a dedicated Rust crate at
`crates/lexongraph-embeddings-trait` that owns the shared embedding-provider
API boundary for LexonGraph consumers.

### REQ-EMBED-TRAIT-002

The crate shall depend on the block crate for the `EmbeddingSpec` contract used
to describe required embedding dimensionality and encoding.

### REQ-EMBED-TRAIT-003

The crate shall define a reusable public input type representing embedding
input without depending on `lexongraph-indexer` or `lexongraph-search`.

### REQ-EMBED-TRAIT-004

The crate shall define an asynchronous embedding-provider trait that accepts an
embedding input plus an `EmbeddingSpec` and returns embedding bytes compatible
with that specification or explicit failure.

The shared contract shall also expose an additive ordered batch-embedding
operation over multiple embedding inputs plus one `EmbeddingSpec`.

### REQ-EMBED-TRAIT-005

The crate shall permit provider implementations to fail explicitly for
unsupported input, provider-side failures, or output incompatible with the
requested `EmbeddingSpec`.

For batch embedding, the crate shall also permit explicit failure when the
provider cannot return exactly one compatible embedding per input in input
order.

### REQ-EMBED-TRAIT-006

The crate shall not require any specific embedding model, endpoint, deployment,
or runtime.

### REQ-EMBED-TRAIT-007

The crate shall provide reusable conformance-test harnesses for the shared
embedding-provider trait it defines, including specified acceptance and
rejection semantics for downstream fixtures.

### REQ-EMBED-TRAIT-008

The reusable conformance-test harnesses shall be exposed through an opt-in,
non-default, test-oriented surface so downstream implementers can use them in
tests without broadening the crate's default production-facing API.

### REQ-EMBED-TRAIT-009

The repository shall include automated verification artifacts that realize the
validation surface defined in
`docs/specs/rust-embeddings-trait/validation.md`, including nominal and helper
rejection behavior.

### REQ-EMBED-TRAIT-010

The indexer crate and provider-specific embedding crates shall depend on this
shared trait crate rather than defining independent embedding-provider
contracts.

### REQ-EMBED-TRAIT-011

The conformance harness shall require the conforming provider fixture to return
embedding bytes that are both compatible with the requested `EmbeddingSpec` and
exactly equal to the harness-provided expected bytes.

For batch conformance, that requirement applies to every returned embedding in
the harness-provided ordered result set.

### REQ-EMBED-TRAIT-012

The conformance helper shall reject a downstream harness when:

- the supposed failing provider fixture succeeds
- the supposed invalid-output provider fixture returns embedding bytes that
  satisfy the requested `EmbeddingSpec`
- the supposed conforming provider fixture returns the wrong number of
  embeddings
- the supposed conforming provider fixture returns embeddings in the wrong
  order

### REQ-EMBED-TRAIT-013

The conformance helper shall reject `EmbeddingSpec.encoding` values that it
does not support for conformance validation, including future protocol
encodings that may be defined by `docs/protocol/blocks.md` but are not yet
understood by the helper implementation.

### REQ-EMBED-TRAIT-014

The public conformance-helper contract shall define `ConformanceError`
categories and category-level rejection behavior for provider failures and
expectation failures, while leaving exact display wording as non-normative
diagnostic detail.

### REQ-EMBED-TRAIT-015

The shared batch-embedding contract shall preserve one-to-one cardinality
between the ordered input collection and the returned ordered embedding
collection.

### REQ-EMBED-TRAIT-016

The shared trait surface shall preserve a single-input compatibility path so
existing callers and implementers are not forced onto a batch-first API shape
in this revision.

## Out of Scope

This crate does not define or own:

- block wire encoding or block validity rules
- block-ID derivation rules
- indexing orchestration
- search traversal behavior
- any provider-specific configuration surface
- any single required embedding model, endpoint, deployment, or runtime

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/`
specification package for block-owned concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
