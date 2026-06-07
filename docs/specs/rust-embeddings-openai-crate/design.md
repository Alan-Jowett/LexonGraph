<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
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

- an OpenAI-compatible API base, API key, model, and optional request-identity
  fields `org_id` and `project_id`
- Azure OpenAI API base, API key, deployment identifier, API version, and
  model

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

For batch embedding, if any input violates that policy, the provider fails
explicitly for the logical batch rather than partially succeeding.

## Request and Response Behavior

### DSG-EMBED-OAI-005 `Batch-capable request path`

The provider issues embedding requests that may contain multiple ordered textual
inputs for the LexonGraph request path in this revision.

The provider may choose its own internal request grouping or chunking strategy,
but that choice is not exposed as part of the public API.

When the caller supplies an empty logical batch, the provider returns an empty
ordered embedding batch and does not issue a remote embeddings request.

### DSG-EMBED-OAI-006 `Response translation`

For each provider-issued request in this revision, the provider requires the
OpenAI-compatible endpoint response to contain exactly one embedding vector per
supplied request input. If the endpoint returns too few or too many embeddings,
the provider fails explicitly before translating bytes.

When the endpoint returns the expected count, the provider maps the response
back into caller input order and translates each embedding vector into the byte
representation required by the supplied `EmbeddingSpec`.

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
provider configuration including optional OpenAI-compatible request identity,
request execution, explicit failure behavior including response-cardinality
rejection, order preservation, and response translation.

### DSG-EMBED-OAI-009 `Logical batch preservation`

If the provider internally splits one caller batch across multiple endpoint
requests, it still returns one logical ordered embedding batch to the caller.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-EMBED-OAI-001 | REQ-EMBED-OAI-001, REQ-EMBED-OAI-002 |
| DSG-EMBED-OAI-002 | REQ-EMBED-OAI-003 |
| DSG-EMBED-OAI-003 | REQ-EMBED-OAI-001, REQ-EMBED-OAI-002 |
| DSG-EMBED-OAI-004 | REQ-EMBED-OAI-004 |
| DSG-EMBED-OAI-005 | REQ-EMBED-OAI-005 |
| DSG-EMBED-OAI-006 | REQ-EMBED-OAI-005, REQ-EMBED-OAI-006, REQ-EMBED-OAI-009, REQ-EMBED-OAI-010 |
| DSG-EMBED-OAI-007 | REQ-EMBED-OAI-007 |
| DSG-EMBED-OAI-008 | REQ-EMBED-OAI-008 |
| DSG-EMBED-OAI-009 | REQ-EMBED-OAI-005, REQ-EMBED-OAI-010 |
