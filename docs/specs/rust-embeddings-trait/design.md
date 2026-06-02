# Rust Embeddings Trait Design

## Status

Draft design specification for a Rust crate that defines the shared LexonGraph
embedding-provider contract.

## Design Goals

The crate design is intended to be:

- reusable across indexing and search-oriented consumers
- explicit about the embedding-provider boundary
- independent of any single provider runtime
- compatible with asynchronous provider realization
- minimal at the production-facing API boundary

## Crate Boundary

The crate owns:

- the shared embedding input type
- the shared embedding-provider trait contract
- conformance helpers for that contract

The crate does not own:

- indexing or search orchestration
- provider-specific request construction
- provider-specific configuration types
- block encoding or block-ID semantics

## External Dependencies

### DSG-EMBED-TRAIT-001 `Block dependency boundary`

The embeddings-trait crate depends on the block crate for the `EmbeddingSpec`
contract and does not redefine dimensionality or encoding semantics already
owned there.

## Core Types

### DSG-EMBED-TRAIT-002 `EmbeddingInput`

A public input type representing one embedding request unit. It contains at
least:

- a media type
- raw bytes

The type is generic enough to be produced by indexing, search-query, or other
LexonGraph-adjacent callers without introducing a dependency on those crates.

## Trait Surface

### DSG-EMBED-TRAIT-003 `EmbeddingProvider`

A trait that accepts an `EmbeddingInput` plus an `EmbeddingSpec` and
asynchronously returns embedding bytes compatible with that specification.

The trait owns the shared contract shape only. It does not define any required
model, endpoint, or runtime.

### DSG-EMBED-TRAIT-004 `Failure boundary`

The shared contract requires explicit failure propagation when:

- the provider does not support the supplied input
- provider execution fails
- the provider cannot satisfy the requested `EmbeddingSpec`

The trait may express those failures through an implementation-defined error
type, but it shall not require silent fallback or undocumented coercion.

## Verification Surface

### DSG-EMBED-TRAIT-005 `Feature-gated conformance module`

The crate exposes a public conformance-test helper surface behind a non-default
Cargo feature intended for downstream tests only.

That feature is not part of the default runtime API and does not change the
production-facing contract.

### DSG-EMBED-TRAIT-006 `Harness shape`

The conformance-test helper surface provides reusable checks for the shared
`EmbeddingProvider` trait contract.

The helper surface may define test-only harness contracts that supply:

- a conforming provider fixture
- a provider fixture that fails explicitly
- a provider fixture that returns output incompatible with the requested
  `EmbeddingSpec`

### DSG-EMBED-TRAIT-007 `Implementation realization`

This specification package shall be realized as a concrete Rust crate in the
repository, and downstream crates such as the indexer crate and provider
implementations shall consume this shared contract rather than defining
independent embedding-provider traits.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-EMBED-TRAIT-001 | REQ-EMBED-TRAIT-002 |
| DSG-EMBED-TRAIT-002 | REQ-EMBED-TRAIT-001, REQ-EMBED-TRAIT-003, REQ-EMBED-TRAIT-010 |
| DSG-EMBED-TRAIT-003 | REQ-EMBED-TRAIT-001, REQ-EMBED-TRAIT-004, REQ-EMBED-TRAIT-006, REQ-EMBED-TRAIT-010 |
| DSG-EMBED-TRAIT-004 | REQ-EMBED-TRAIT-005 |
| DSG-EMBED-TRAIT-005..006 | REQ-EMBED-TRAIT-007, REQ-EMBED-TRAIT-008 |
| DSG-EMBED-TRAIT-007 | REQ-EMBED-TRAIT-001, REQ-EMBED-TRAIT-009, REQ-EMBED-TRAIT-010 |
