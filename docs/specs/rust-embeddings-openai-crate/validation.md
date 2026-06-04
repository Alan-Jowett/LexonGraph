<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust OpenAI Embeddings Crate Validation

## Status

Draft validation specification for a Rust crate that implements the shared
LexonGraph embedding-provider contract against OpenAI-compatible embeddings
endpoints.

## Validation Scope

These validation entries define the expected conformance surface for the
OpenAI-compatible embedding-provider crate.

## Validation Entries

### VAL-EMBED-OAI-001

Use the crate from a downstream consumer that depends only on the
embeddings-trait crate and this provider crate.

**Pass condition:** the downstream consumer can instantiate the provider
without depending on the indexer crate or search crate.

**Traces to:** REQ-EMBED-OAI-001, REQ-EMBED-OAI-002, REQ-EMBED-OAI-007

### VAL-EMBED-OAI-002

Use the provider with a controlled OpenAI-compatible endpoint fixture that
returns a successful embedding response for multiple UTF-8 text inputs.

**Pass condition:** the provider issues multi-input request semantics, receives
one embedding vector per supplied input, and returns ordered bytes compatible
with the requested `EmbeddingSpec`.

**Traces to:** REQ-EMBED-OAI-005, REQ-EMBED-OAI-006, REQ-EMBED-OAI-008

### VAL-EMBED-OAI-003

Configure the provider for distinct endpoint styles, including an
OpenAI-compatible base URL with optional `org_id` and `project_id`, and an
Azure OpenAI deployment.

**Pass condition:** the provider constructs provider-specific requests using the
supplied configuration without changing the shared embedding-provider contract,
including forwarding the optional OpenAI-compatible request-identity fields
when present.

**Traces to:** REQ-EMBED-OAI-003, REQ-EMBED-OAI-008

### VAL-EMBED-OAI-004

Provide embedding input whose media type is non-text or whose bytes are not
valid UTF-8, including in a logical batch containing otherwise valid textual
inputs.

**Pass condition:** the provider fails explicitly before reporting success and
before silently coercing the input or partially submitting a batch request.

**Traces to:** REQ-EMBED-OAI-004

### VAL-EMBED-OAI-005

Provide an `EmbeddingSpec` whose encoding or dimensionality cannot be satisfied
by the provider's translated OpenAI-compatible response.

**Pass condition:** the provider fails explicitly rather than returning bytes
with a mismatched length or undocumented encoding.

**Traces to:** REQ-EMBED-OAI-006

### VAL-EMBED-OAI-006

Inspect the repository verification artifacts for the OpenAI-compatible
embeddings crate.

**Pass condition:** the repository includes executable automated tests that
realize the validation surface in this specification package.

**Traces to:** REQ-EMBED-OAI-008

### VAL-EMBED-OAI-007

Use the provider with a controlled OpenAI-compatible endpoint fixture that
returns too few or too many embeddings for a batch request.

**Pass condition:** the provider fails explicitly rather than returning bytes,
silently selecting one embedding, or masking the response-cardinality
mismatch.

**Traces to:** REQ-EMBED-OAI-005, REQ-EMBED-OAI-008, REQ-EMBED-OAI-009

### VAL-EMBED-OAI-008

Use the provider with a controlled OpenAI-compatible endpoint fixture that
returns embedding objects out of order relative to the supplied input batch but
with correct response indices.

**Pass condition:** the provider preserves the caller's logical input order in
the returned translated embeddings.

**Traces to:** REQ-EMBED-OAI-010
