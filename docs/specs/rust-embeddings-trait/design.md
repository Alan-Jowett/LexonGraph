<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
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

The shared trait surface also provides an additive ordered batch-embedding
operation over multiple `EmbeddingInput` values plus one `EmbeddingSpec`.

The trait owns the shared contract shape only. It does not define any required
model, endpoint, or runtime.

### DSG-EMBED-TRAIT-004 `Failure boundary`

The shared contract requires explicit failure propagation when:

- the provider does not support the supplied input
- provider execution fails
- the provider cannot satisfy the requested `EmbeddingSpec`
- the provider cannot return exactly one embedding per supplied batch input
- the provider cannot preserve input-to-output ordering for batch results

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

- a sample input
- a sample input batch
- a compatible embedding specification
- an exact expected embedding byte vector for the conforming fixture
- an exact ordered embedding byte-vector set for the conforming batch fixture
- a conforming provider fixture
- a provider fixture that fails explicitly
- a provider fixture that returns output incompatible with the requested
  `EmbeddingSpec`

The helper surface shall treat those fixture roles as contract-relevant, not
merely advisory, when evaluating downstream harnesses.

### DSG-EMBED-TRAIT-007 `Conforming fixture exactness`

The reusable conformance suite shall validate the conforming provider fixture in
two stages:

1. confirm the returned bytes are compatible with the requested
   `EmbeddingSpec`
2. confirm the returned bytes exactly equal the harness-provided
   `expected_embedding`

Length compatibility alone is insufficient for the conforming fixture path.

For batch conformance, both stages apply to every embedding in the returned
ordered result set.

### DSG-EMBED-TRAIT-008 `Misconfigured fixture rejection`

The reusable conformance suite shall reject a downstream harness with an
expectation-category conformance failure when:

- the supposed failing provider fixture succeeds
- the supposed invalid-output provider fixture returns bytes that satisfy the
  requested `EmbeddingSpec`
- the supposed conforming provider fixture returns bytes that are compatible
  with the requested `EmbeddingSpec` but do not equal `expected_embedding`
- the supposed conforming provider fixture returns the wrong number of
  embeddings
- the supposed conforming provider fixture returns embeddings in the wrong
  order

### DSG-EMBED-TRAIT-009 `Encoding validation boundary`

The conformance suite shall validate embedding-byte compatibility only for the
set of `EmbeddingSpec.encoding` values that the helper explicitly understands.

For known supported encodings, the helper shall derive the required byte length
from `EmbeddingSpec.dims` and reject mismatched lengths.

For unsupported or future encodings, the helper shall fail closed with an
expectation-category conformance failure rather than inferring acceptance from
length or silently bypassing validation.

### DSG-EMBED-TRAIT-010 `Public conformance error surface`

The public conformance surface shall expose distinct error categories for:

- provider-execution failure
- conformance-expectation failure

Those categories and their routing behavior are contractual. The exact display
strings used for diagnostics are not contractual.

### DSG-EMBED-TRAIT-011 `Implementation realization`

This specification package shall be realized as a concrete Rust crate in the
repository, and downstream crates such as the indexer crate and provider
implementations shall consume this shared contract rather than defining
independent embedding-provider traits.

### DSG-EMBED-TRAIT-012 `Ordered batch semantics`

The ordered batch-embedding operation is contract-relevant behavior at the
shared trait boundary.

Provider-specific crates may choose their own batching or chunking strategy, but
the observable result seen by callers shall remain one compatible embedding per
input in input order.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-EMBED-TRAIT-001 | REQ-EMBED-TRAIT-002 |
| DSG-EMBED-TRAIT-002 | REQ-EMBED-TRAIT-001, REQ-EMBED-TRAIT-003, REQ-EMBED-TRAIT-010 |
| DSG-EMBED-TRAIT-003 | REQ-EMBED-TRAIT-001, REQ-EMBED-TRAIT-004, REQ-EMBED-TRAIT-006, REQ-EMBED-TRAIT-010 |
| DSG-EMBED-TRAIT-004 | REQ-EMBED-TRAIT-005 |
| DSG-EMBED-TRAIT-005..006 | REQ-EMBED-TRAIT-007, REQ-EMBED-TRAIT-008 |
| DSG-EMBED-TRAIT-007 | REQ-EMBED-TRAIT-011 |
| DSG-EMBED-TRAIT-008 | REQ-EMBED-TRAIT-007, REQ-EMBED-TRAIT-012 |
| DSG-EMBED-TRAIT-009 | REQ-EMBED-TRAIT-013 |
| DSG-EMBED-TRAIT-010 | REQ-EMBED-TRAIT-014 |
| DSG-EMBED-TRAIT-011 | REQ-EMBED-TRAIT-001, REQ-EMBED-TRAIT-009, REQ-EMBED-TRAIT-010 |
| DSG-EMBED-TRAIT-012 | REQ-EMBED-TRAIT-004, REQ-EMBED-TRAIT-005, REQ-EMBED-TRAIT-015, REQ-EMBED-TRAIT-016 |
