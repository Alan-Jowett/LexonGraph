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

### REQ-EMBED-TRAIT-005

The crate shall permit provider implementations to fail explicitly for
unsupported input, provider-side failures, or output incompatible with the
requested `EmbeddingSpec`.

### REQ-EMBED-TRAIT-006

The crate shall not require any specific embedding model, endpoint, deployment,
or runtime.

### REQ-EMBED-TRAIT-007

The crate shall provide reusable conformance-test harnesses for the shared
embedding-provider trait it defines.

### REQ-EMBED-TRAIT-008

The reusable conformance-test harnesses shall be exposed through an opt-in,
non-default, test-oriented surface so downstream implementers can use them in
tests without broadening the crate's default production-facing API.

### REQ-EMBED-TRAIT-009

The repository shall include automated verification artifacts that realize the
validation surface defined in
`docs/specs/rust-embeddings-trait/validation.md`.

### REQ-EMBED-TRAIT-010

The indexer crate and provider-specific embedding crates shall depend on this
shared trait crate rather than defining independent embedding-provider
contracts.

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

