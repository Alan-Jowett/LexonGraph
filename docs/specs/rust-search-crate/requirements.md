<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Search Crate Requirements

## Status

Draft specification for a Rust crate that implements the LexonGraph search
protocol.

## Scope

This document specifies the crate-level requirements for a Rust crate that
implements `docs/protocol/search.md`.

This document is layered on top of:

- `docs/protocol/search.md`
- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`

This document does not redefine block encoding, block identifiers, storage
backend semantics, or protocol search invariants. Those concerns remain owned
by the protocol documents, the block crate, and the block-storage trait crate.

## Terminology

In this spec package, `root block ID` means the protocol-defined content
identifier of the block where search starts.

`Compatibility context` means the logical environment in which a target
embedding is compared against candidate embeddings, including the target
embedding representation, the visited block's `embedding_spec`, and the
comparison and ranking traits supplied for the invocation.

In this spec package, lowercase `w` and `n` name the Rust API parameters that
correspond to protocol-level `W` and `N` in `docs/protocol/search.md`.

## Requirements

### REQ-SEARCH-001

The crate shall define the Rust API boundary for a LexonGraph search component
that implements `docs/protocol/search.md`.

### REQ-SEARCH-002

The crate shall remain subordinate to `docs/protocol/search.md` for traversal,
ranking, deduplication, termination, and failure semantics, and subordinate to
`docs/protocol/blocks.md` for block identity and validity semantics.

### REQ-SEARCH-003

The crate shall depend on the block crate for typed block decoding, validated
block decomposition, and protocol-conforming block interpretation.

### REQ-SEARCH-004

The crate shall depend on the block-storage trait crate for loading the root
block and selected child blocks by block ID.

### REQ-SEARCH-005

The public search operation shall require:

- a root block ID
- a target embedding
- a traversal width `w`
- a final result count `n`

The public API boundary shall also require access to:

- a block store capable of loading the root and selected child blocks
- implementations of the required policy traits

Those dependencies may be supplied either as direct operation inputs or through
construction or configuration of the searcher instance that serves the
operation.

### REQ-SEARCH-006

The crate shall surface explicit failure when the root block cannot be loaded,
when a selected child block cannot be loaded, when a visited block is malformed,
when a visited block is incompatible with the target embedding, or when the
search cannot produce `n` reachable leaf candidates.

### REQ-SEARCH-007

The crate shall keep protocol-required search orchestration separate from
implementation-defined policy concerns through trait-based extension points.

The crate may also provide public default implementations of those policy
traits, but those defaults shall remain optional and shall not prevent callers
from supplying their own implementations.

### REQ-SEARCH-008

At minimum, the crate shall expose trait-governed policy boundaries for:

- embedding compatibility checks between the target embedding and a visited
  block's `embedding_spec`
- candidate scoring inputs derived from the target embedding and candidate
  embedding

The crate shall also expose public default implementations for those policy
boundaries.

### REQ-SEARCH-009

The core search engine shall own the protocol-required orchestration, candidate
accumulation, deterministic ordering, branch-child deduplication, width-limited
expansion, visited-child tracking, and termination decisions.

### REQ-SEARCH-010

The crate shall preserve the protocol's candidate-identity rules so that equal
embeddings do not collapse distinct branch or leaf candidates before the
protocol-defined deduplication points.

### REQ-SEARCH-011

Given the same root block ID, target embedding, `n`, `w`, stored block set, and
deterministic trait implementations within the same compatibility context, the
crate shall return the same ordered leaf results or the same explicit failure.

### REQ-SEARCH-012

The crate shall not require any specific embedding model, similarity metric, or
ranking heuristic beyond the deterministic ordering and tie-break behavior
mandated by `docs/protocol/search.md`.

If the crate provides a default similarity metric, that metric is an optional
crate convenience rather than a mandatory protocol requirement.

### REQ-SEARCH-013

The repository shall include a Rust crate that realizes the requirements and
design in this specification package.

### REQ-SEARCH-014

The repository shall include automated verification artifacts that realize the
validation surface defined in `docs/specs/rust-search-crate/validation.md`.

### REQ-SEARCH-015

The crate shall provide reusable conformance-test harnesses for the
implementation-defined search-owned policy traits it defines, including at
minimum:

- embedding compatibility checks
- candidate scoring

If implementation requires additional search-owned policy traits, the crate may
also provide reusable conformance-test harnesses for those traits.

### REQ-SEARCH-016

The reusable conformance-test harnesses shall be exposed through an opt-in,
non-default, test-oriented surface so downstream implementers can use them in
tests without broadening the crate's default production-facing API.

### REQ-SEARCH-017

The crate shall not redefine or duplicate reusable conformance-test contracts
for dependency surfaces already owned by subordinate specifications, including
the block crate and block-storage trait crate.

### REQ-SEARCH-018

The crate shall treat `w = 0` as invalid input and fail explicitly.

The crate may accept `n = 0`; in that case it shall still load the root block
and score the root candidate set under the normal compatibility and scoring
rules, then return success with an empty ordered result without child
expansion.

### REQ-SEARCH-019

The crate shall expose a public default `EmbeddingCompatibility`
implementation for crate-owned encoded target embeddings.

That default compatibility policy shall accept a visited block when the target
embedding and the block's `embedding_spec` have the same encoding and
dimensionality, and shall reject the block explicitly otherwise.

### REQ-SEARCH-020

The crate shall expose a public default `CandidateScorer` implementation for
the same crate-owned encoded target-embedding representation used by the
default compatibility policy.

That default scorer shall compute a deterministic cosine similarity, or an
equivalent standard cosine-based comparison, over compatible target and
candidate embeddings.

### REQ-SEARCH-021

The crate shall define a public encoded target-embedding representation for the
crate-provided default policies. That representation shall preserve the target
embedding bytes together with the embedding specification needed to validate and
score candidate embeddings.

The crate-provided default policies shall fail explicitly when given unsupported
encodings or target or candidate byte sequences whose lengths are inconsistent
with the applicable embedding specification.

## Out of Scope

This crate does not define or own:

- block wire encoding or block validity rules
- block-ID derivation rules
- storage backend implementations
- indexing tree construction strategy
- any single required embedding model or embedding runtime
- any single required similarity metric
- any policy that overrides the protocol-defined ordering and tie-break rules
- reusable conformance contracts already owned by the block crate or
  block-storage trait crate

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/search.md` and
`docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
