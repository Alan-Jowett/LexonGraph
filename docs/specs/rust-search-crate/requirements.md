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
when a visited block is incompatible with the target embedding, when candidate
scoring fails for candidates loaded from a visited block regardless of candidate
level, or when the search cannot produce `n` reachable leaf candidates.

### REQ-SEARCH-007

The crate shall keep protocol-required search orchestration separate from
implementation-defined policy concerns through trait-based extension points.

The crate shall also provide public default implementations of those policy
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
embeddings do not collapse distinct child-bearing or leaf candidates before the
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

At minimum, those reusable harnesses shall verify repeated-input stability for
contract-satisfying implementations, explicit rejection or failure for
contract-violating fixtures, and detection of nondeterministic
implementations.

Candidate-scoring harnesses shall also verify that a preferred candidate can
outrank a lower-ranked alternate candidate within the same compatible scoring
context.

The repository verification artifacts shall also include representative direct
checks of helper-owned expectation failures so the reusable harness surface is
validated on both success and failure paths.

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
embedding and the block's logical comparison representation have the same
encoding and dimensionality, and shall reject the block explicitly otherwise.

For ordinary non-EBCP blocks, the logical comparison representation is the
block's declared `embedding_spec`.

For EBCP-encoded non-leaf blocks, the logical comparison representation is the
ambient-space encoding declared by `docs/protocol/ebcp.md`.

### REQ-SEARCH-020

The crate shall expose a public default `CandidateScorer` implementation for
the same crate-owned encoded target-embedding representation used by the
default compatibility policy.

That default scorer shall compute a deterministic cosine similarity, or an
equivalent standard cosine-based comparison, over compatible target and
candidate embeddings.

For supported EBCP branch encodings, the default scorer shall reconstruct or
otherwise compare the logical ambient-space branch embeddings defined by
`docs/protocol/ebcp.md` rather than scoring the compressed payload bytes
directly.

### REQ-SEARCH-021

The crate shall define a public encoded target-embedding representation for the
crate-provided default policies. That representation shall preserve the target
embedding bytes together with the embedding specification needed to validate and
score candidate embeddings.

The crate-provided default policies shall fail explicitly when given unsupported
encodings or target or candidate byte sequences whose lengths are inconsistent
with the applicable embedding specification, when target or candidate
embeddings have zero magnitude, when encoded floating-point values are
non-finite, or when an embedding specification's dimensionality is too large to
validate safely.

The crate-provided default scorer shall also fail explicitly when cosine
computation over otherwise-supported inputs yields a non-finite result.

For the crate's supported default-scorer floating-point encodings, those
guardrails shall apply consistently across each supported decoding path.

### REQ-SEARCH-022

Within one search invocation, once a child block ID has been selected for
expansion and loaded, later references to that same child block ID shall not
cause that child to be selected for expansion again.

### REQ-SEARCH-023

After a round expands one or more child block IDs, the crate shall remove from
the frontier all expandable candidates that target child block IDs already expanded
in the invocation before the next ranking round.

### REQ-SEARCH-024

The crate shall expose an optional per-invocation telemetry surface for search.

That surface shall allow callers to observe, without changing search ranking or
failure semantics:

- the beam width used for the invocation
- the count of distinct blocks visited during the invocation
- the maximum routing depth reached during the invocation
- the terminal outcome classification for the invocation

### REQ-SEARCH-027

The crate shall expose a higher-level convenience search surface whose shape
remains stable across published profile revisions.

That surface shall accept an explicit published semantic-version profile
selector rather than requiring callers to wire the crate-owned default search
policies manually.

### REQ-SEARCH-028

The convenience search surface shall fail explicitly for unknown or unsupported
published profile versions.

It shall not silently substitute the latest, nearest, or repository-current
profile.

### REQ-SEARCH-029

A published search profile version shall map to one deterministic bundle of
crate-owned search defaults for its lifetime.

### REQ-SEARCH-030

The repository shall publish search profile `0.1.0`.

For the crate-owned runtime knobs in this revision, that published profile
shall resolve to the crate-owned encoded target representation together with the
crate-provided default embedding-compatibility and candidate-scoring policies.

### REQ-SEARCH-031

Even on the convenience search surface, callers shall continue to supply `w`
and `n` explicitly.

### REQ-SEARCH-032

The existing low-level explicit search surface shall remain available for
callers that want direct policy substitution instead of selecting a published
profile.

### REQ-SEARCH-033

The search crate shall accept non-leaf blocks whose `embedding_spec.encoding`
uses one of the EBCP branch encodings defined by `docs/protocol/ebcp.md`,
provided the enclosing block is otherwise valid under `docs/protocol/blocks.md`.

### REQ-SEARCH-034

When search visits an EBCP-encoded non-leaf block, it shall interpret each
branch-entry embedding according to `docs/protocol/ebcp.md` so that ranking and
expansion decisions are based on the logical child-centroid embeddings defined
by that protocol rather than on the raw stored payload bytes.

### REQ-SEARCH-035

The search crate shall remain subordinate to `docs/protocol/search.md` for
traversal, ranking, deduplication, and termination behavior when visiting
EBCP-encoded blocks.

EBCP support shall extend the candidate-interpretation boundary only; it shall
not alter the search protocol itself.

### REQ-SEARCH-036

If two indexes differ only in that one stores non-leaf branch embeddings using
`pca-rot-f32le` or `pca-rot-delta-f32le` while preserving the same logical
branch centroids and tree topology, the search crate shall return the same
ordered leaf results or the same explicit failure for identical search inputs.

### REQ-SEARCH-037

When an index uses the lossy EBCP encodings `pca-rot-delta-uq`,
`pca-rot-delta-vbq`, or `ambient-delta-uq`, any observable recall difference
relative to the same
topology under uncompressed branch embeddings shall arise only from the encoded
branch-vector approximation rather than from a change in search API shape,
traversal rules, or termination rules.

### REQ-SEARCH-038

For supported EBCP branch encodings, the search crate shall realize its
candidate interpretation through the canonical logical-branch reconstruction
semantics exposed by `lexongraph-block` rather than maintaining an independent
private reconstruction path.

This preserves one protocol-owned interpretation of stored branch embeddings
across search and downstream diagnostics consumers.

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

This document is subordinate to `docs/protocol/search.md`,
`docs/protocol/blocks.md`, and `docs/protocol/ebcp.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
