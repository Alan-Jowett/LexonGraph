# Rust OpenAI Embeddings Crate Design

## Status

Draft design specification for a Rust crate that implements the shared
LexonGraph embedding-provider contract against OpenAI-compatible embeddings
endpoints.

## Design Goals

The crate design is intended to be:

- reusable across indexing and search-oriented callers
- explicit about provider-specific configuration
- isolated from the indexer crate and search crate
- strict about unsupported input and unsupported output encodings
- minimal at the public API boundary

## Crate Boundary

The crate owns:

- a concrete embedding-provider implementation built on `async-openai`
- provider-specific configuration types for OpenAI-compatible and Azure-style
  endpoints
- request construction and response translation for that provider

The crate does not own:

- the shared embedding-provider trait contract
- indexing orchestration
- search traversal behavior

## External Dependencies

### DSG-EMBED-OAI-001 `Dependency boundary`

The crate depends on:

- the embeddings-trait crate for the shared embedding-provider contract and
  embedding input type
- the block crate for `EmbeddingSpec`
- `async-openai` for OpenAI-compatible request execution

The crate does not redefine those dependency-owned contracts.

## Public Types

### DSG-EMBED-OAI-002 `Provider configuration`

The crate defines provider-specific configuration types that cover at least:

- an OpenAI-compatible base URL plus model or request identity
- Azure OpenAI API base, deployment identifier, API version, and request
  identity

These configuration types remain provider-specific and do not broaden the
shared trait crate or indexer crate API boundaries.

### DSG-EMBED-OAI-003 `OpenAI embedding provider`

The crate defines a concrete embedding-provider implementation that realizes the
shared embeddings-trait contract by issuing OpenAI-compatible embedding
requests through `async-openai`.

## Input Mapping

### DSG-EMBED-OAI-004 `Text-only input policy`

In this revision, the provider accepts embedding input only when:

- the media type denotes text content or a compatible textual subtype
- the input bytes decode successfully as UTF-8

If either condition is not met, the provider fails explicitly before issuing a
remote embedding request.

## Request and Response Behavior

### DSG-EMBED-OAI-005 `Single-input request path`

The provider issues one-input embedding requests for the LexonGraph request path
in this revision.

### DSG-EMBED-OAI-006 `Response translation`

The provider receives one embedding vector from the OpenAI-compatible endpoint
and translates that vector into the byte representation required by the
supplied `EmbeddingSpec`.

In this revision, the provider may support only the subset of encodings that
can be specified and implemented without undocumented or lossy translation. Any
unsupported encoding shall fail explicitly.

If the endpoint response dimensionality does not match the requested
`EmbeddingSpec`, the provider fails explicitly rather than returning mismatched
bytes.

### DSG-EMBED-OAI-007 `Independent consumption`

The provider crate is consumed by depending on the crate itself rather than by
enabling an opt-in feature on the shared trait crate or indexer crate.

## Verification Surface

### DSG-EMBED-OAI-008 `Implementation realization`

This specification package shall be realized as a concrete Rust crate in the
repository, and the repository shall include automated tests that exercise
provider configuration, request execution, explicit failure behavior, and
response translation.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-EMBED-OAI-001 | REQ-EMBED-OAI-001, REQ-EMBED-OAI-002 |
| DSG-EMBED-OAI-002 | REQ-EMBED-OAI-003 |
| DSG-EMBED-OAI-003 | REQ-EMBED-OAI-001, REQ-EMBED-OAI-002 |
| DSG-EMBED-OAI-004 | REQ-EMBED-OAI-004 |
| DSG-EMBED-OAI-005 | REQ-EMBED-OAI-005 |
| DSG-EMBED-OAI-006 | REQ-EMBED-OAI-006 |
| DSG-EMBED-OAI-007 | REQ-EMBED-OAI-007 |
| DSG-EMBED-OAI-008 | REQ-EMBED-OAI-008 |
