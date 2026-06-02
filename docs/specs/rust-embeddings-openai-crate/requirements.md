<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust OpenAI Embeddings Crate Requirements

## Status

Draft specification for a Rust crate that implements the shared LexonGraph
embedding-provider contract against OpenAI-compatible embeddings endpoints.

## Scope

This document specifies the crate-level requirements for a Rust crate at
`crates/lexongraph-embeddings-openai`.

This document is layered on top of:

- `docs/specs/rust-embeddings-trait/`
- `docs/specs/rust-block-crate/`

This document does not redefine the shared embedding-provider trait contract or
block-owned embedding semantics. Those concerns remain owned by the
embeddings-trait crate and block crate.

## Requirements

### REQ-EMBED-OAI-001

The repository shall define a dedicated Rust crate at
`crates/lexongraph-embeddings-openai` that implements the shared
embedding-provider trait using `async-openai`.

### REQ-EMBED-OAI-002

The crate shall depend on the embeddings-trait crate for the shared
embedding-provider contract it realizes.

### REQ-EMBED-OAI-003

The crate shall be configurable for standard OpenAI-style embeddings endpoints,
including:

- OpenAI-compatible base URLs
- Azure OpenAI deployments
- local services that expose a compatible embeddings API

### REQ-EMBED-OAI-004

In this revision, the crate shall accept embedding input only when the input
represents UTF-8 textual content and shall fail explicitly for non-text or
non-UTF-8 payloads.

### REQ-EMBED-OAI-005

In this revision, the crate shall use one embedding input per request path used
by LexonGraph consumers. General multi-item batching is out of scope.

### REQ-EMBED-OAI-006

The crate shall convert returned embeddings into bytes compatible with the
requested `EmbeddingSpec` and shall fail explicitly when the endpoint response
cannot satisfy the requested encoding or dimensionality.

### REQ-EMBED-OAI-007

The crate shall remain independently consumable so consumers that choose a
different embedding backend are not forced to depend on OpenAI-specific code.

### REQ-EMBED-OAI-008

The repository shall include automated verification artifacts covering
successful request execution, OpenAI-compatible request construction,
Azure-style configuration, explicit failure behavior, and `EmbeddingSpec`
compatibility handling.

## Out of Scope

This crate does not define or own:

- the shared embedding-provider trait contract
- indexing orchestration
- search traversal behavior
- any single required OpenAI-compatible deployment for all consumers
- general multi-item embedding batching in this revision

## Relationship to Other Specifications

This document is subordinate to the `docs/specs/rust-embeddings-trait/` and
`docs/specs/rust-block-crate/` specification packages for their respective
concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.

