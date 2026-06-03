<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Search Crate Design

## Status

Draft design specification for a Rust crate that implements the LexonGraph
search protocol.

In this spec package, lowercase `w` and `n` name the Rust API parameters that
correspond to protocol-level `W` and `N` in `docs/protocol/search.md`.

## Design Goals

The crate design is intended to be:

- protocol-conforming
- deterministic at the crate boundary
- explicit about policy seams
- reusable across storage backends
- strict about failure propagation
- minimal at the public API boundary

## Crate Boundary

The crate owns:

- search-oriented public types
- search orchestration
- protocol-required candidate accounting and deduplication
- search-oriented error taxonomy

The crate does not own:

- block wire encoding or block-ID derivation
- block validation rules beyond invoking the block crate
- storage backend implementations
- indexing strategy
- any required embedding model or similarity metric

## External Dependencies

### DSG-SEARCH-001 `Protocol dependency boundary`

The search crate depends on the protocol documents for normative search and
block invariants. It implements those constraints and does not redefine them.

### DSG-SEARCH-002 `Crate dependencies`

The search crate depends on:

- the block crate for hash-verified block decoding, typed block values, and
  typed entry decomposition
- the block-storage trait crate for backend-agnostic block retrieval by block ID

## Core Types

### DSG-SEARCH-003 `SearchResult`

A successful search result containing the ordered leaf candidates returned by
the search invocation.

Each leaf candidate contains at least:

- the containing leaf block ID
- the matched leaf entry
- the candidate's final ordering position

### DSG-SEARCH-004 `SearchError`

An explicit error taxonomy covering at least:

- root-block retrieval failure
- child-block retrieval failure
- malformed or non-conforming block content
- embedding compatibility failure
- scoring-policy failure
- search exhaustion before `n` reachable leaves

### DSG-SEARCH-005 `SearchCandidate`

An internal typed representation of one ranked candidate in the current search
frontier.

This representation preserves:

- candidate kind, distinguishing branch from leaf entries
- candidate identity, using child block ID for branches and containing block ID
  for leaves
- the original entry payload needed for later return or expansion
- the ranking inputs produced for the current target embedding

## Policy Traits

### DSG-SEARCH-006 `EmbeddingCompatibility`

A trait that accepts the target embedding and a visited block's
`embedding_spec`, and determines whether that block can participate in the
current search invocation.

This trait defines compatibility policy, but the search crate owns when that
compatibility check is required and how failure is surfaced.

The crate provides public default implementations of this trait, but callers
remain free to supply their own implementations.

### DSG-SEARCH-007 `CandidateScorer`

A trait that accepts the target embedding, one candidate embedding, and the
current compatibility context, and returns the ranking input used as the
protocol's primary ordering key.

The trait may define how similarity or distance is computed, but it does not
replace the protocol-defined tie-break rules.

The crate provides public default implementations of this trait, but callers
remain free to supply their own implementations.

### DSG-SEARCH-017 `EncodedTargetEmbedding`

The crate defines a public target-embedding representation used by the
crate-provided default policy implementations.

That representation carries:

- the raw encoded target-embedding bytes
- the target embedding's `EmbeddingSpec`

This type does not become a mandatory search input for custom policies; it is a
crate-owned convenience for the default policy surface only.

### DSG-SEARCH-018 `DefaultEmbeddingCompatibility`

The crate exposes a public default `EmbeddingCompatibility` implementation for
`EncodedTargetEmbedding`.

That implementation accepts a visited block when:

- the target and visited `embedding_spec.encoding` values are equal
- the target and visited `embedding_spec.dims` values are equal

and rejects the block explicitly otherwise.

### DSG-SEARCH-019 `DefaultCandidateScorer`

The crate exposes a public default `CandidateScorer` implementation for
`EncodedTargetEmbedding`.

That implementation:

- validates that the candidate embedding bytes are well-formed for the visited
  block's `EmbeddingSpec`
- validates that the target bytes are well-formed for the target
  `EmbeddingSpec`
- requires the target and visited embedding specifications to be compatible
  under the default compatibility policy
- computes a deterministic cosine similarity, or an equivalent standard
  cosine-based comparison, over the decoded target and candidate vectors
- returns a score representation with total ordering suitable for the crate's
  deterministic ranking boundary

Unsupported encodings and inconsistent byte lengths are surfaced as explicit
scoring or compatibility failures rather than being silently normalized.

## API Surface

### DSG-SEARCH-008 `Searcher`

A public orchestration type or trait exposing a search operation that accepts:

- a root block ID
- a target embedding
- a traversal width `w`
- a final result count `n`
- a block store implementation
- implementations of the required policy traits

and returns `Result<SearchResult, SearchError>`.

The crate's runtime API also exposes the public default policy types and the
crate-owned encoded target-embedding representation needed to use them, without
removing the existing ability to pass caller-defined policy implementations.

## Orchestration Flow

### DSG-SEARCH-009 `Core search pipeline`

The fixed orchestration flow is:

1. load the root block through the block store
2. verify and decode each loaded block through the block crate
3. reject blocks incompatible with the target embedding according to the
   compatibility trait
4. load the block's entries into the current candidate set
5. score each candidate embedding against the target embedding through the
   scoring trait
6. rank the full candidate set using descending primary score plus the
   protocol-defined deterministic tie-break order
7. terminate successfully when the top `n` ranked candidates are all leaves
8. select ranked branch candidates whose child block IDs have not already been
   expanded in the invocation
9. de-duplicate those branch candidates by child block ID, keeping the
   highest-ranked occurrence as the effective rank for that child
10. select the top `w` unique child block IDs from that de-duplicated branch set
11. load the selected child blocks and mark their block IDs as expanded
12. remove from the current candidate set the branch candidates whose child
    blocks were expanded
13. retain all remaining candidates and add the entries from the newly loaded
    child blocks to form the next candidate set
14. fail explicitly if no expandable branch candidates remain before successful
    termination

The core search engine owns this flow even when policy traits participate in
individual steps.

### DSG-SEARCH-010 `Deterministic ordering boundary`

Conformance requires deterministic behavior from the compatibility and scoring
traits within a given compatibility context.

If those trait implementations are deterministic and the logical inputs are the
same, the search crate produces the same ordered leaf results or the same
explicit failure.

### DSG-SEARCH-011 `Candidate identity preservation`

Candidate ranking and accumulation preserve the protocol distinction between:

- equal embeddings pointing to different child blocks
- different embeddings pointing to the same child block before branch-child
  deduplication
- equal embeddings occurring in different leaf blocks

The crate does not collapse those candidates before the protocol-defined
deduplication points.

### DSG-SEARCH-012 `Repository realization`

The repository shall contain a Rust Cargo package for the search crate within
the workspace, and that package shall realize the public search contract and
search-owned conformance-helper surface defined by this specification package.

### DSG-SEARCH-013 `Verification realization`

The repository shall include automated tests that realize the validation
entries in `docs/specs/rust-search-crate/validation.md`, with each validation
entry mapped to one or more executable tests.

### DSG-SEARCH-014 `Feature-gated conformance module`

The crate exposes a public conformance-test helper surface behind a non-default
Cargo feature intended for downstream tests only.

That feature is not part of the default runtime API and does not change the
production-facing search contract.

### DSG-SEARCH-015 `Harness shape`

The conformance-test helper surface provides reusable checks for the
search-owned policy traits defined by this document, including at minimum
`EmbeddingCompatibility` and `CandidateScorer`.

If implementation introduces additional search-owned policy traits, the helper
surface may also provide reusable checks for those traits.

To verify those trait contracts without requiring production implementations in
the crate, the helper surface may define test-only harness contracts that
supply deterministic fixtures, trait implementations under test, and any
policy-specific assertions needed for the validation cases.

The helper surface does not redefine conformance for the block crate or the
block-storage trait crate, which continue to own their respective reusable
conformance contracts.

### DSG-SEARCH-016 `Zero-value parameter semantics`

The search API rejects `w = 0` with an explicit error before entering the
expansion loop.

The search API may accept `n = 0`. In that case, the engine still performs the
root-block load, compatibility checks, root candidate loading, and root
candidate scoring before terminating successfully with an empty
`SearchResult`.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-SEARCH-001 | REQ-SEARCH-001, REQ-SEARCH-002, REQ-SEARCH-012 |
| DSG-SEARCH-002 | REQ-SEARCH-003, REQ-SEARCH-004, REQ-SEARCH-012 |
| DSG-SEARCH-003..005 | REQ-SEARCH-001, REQ-SEARCH-006, REQ-SEARCH-009, REQ-SEARCH-010 |
| DSG-SEARCH-006 | REQ-SEARCH-006, REQ-SEARCH-007, REQ-SEARCH-008, REQ-SEARCH-011 |
| DSG-SEARCH-007 | REQ-SEARCH-007, REQ-SEARCH-008, REQ-SEARCH-011, REQ-SEARCH-012 |
| DSG-SEARCH-008 | REQ-SEARCH-001, REQ-SEARCH-004, REQ-SEARCH-005, REQ-SEARCH-007, REQ-SEARCH-009, REQ-SEARCH-019, REQ-SEARCH-020, REQ-SEARCH-021 |
| DSG-SEARCH-009 | REQ-SEARCH-002, REQ-SEARCH-006, REQ-SEARCH-007, REQ-SEARCH-009, REQ-SEARCH-010, REQ-SEARCH-012 |
| DSG-SEARCH-010 | REQ-SEARCH-011 |
| DSG-SEARCH-011 | REQ-SEARCH-002, REQ-SEARCH-010 |
| DSG-SEARCH-012 | REQ-SEARCH-013 |
| DSG-SEARCH-013 | REQ-SEARCH-014 |
| DSG-SEARCH-014 | REQ-SEARCH-015, REQ-SEARCH-016 |
| DSG-SEARCH-015 | REQ-SEARCH-015, REQ-SEARCH-016, REQ-SEARCH-017 |
| DSG-SEARCH-016 | REQ-SEARCH-005, REQ-SEARCH-006, REQ-SEARCH-018 |
| DSG-SEARCH-017 | REQ-SEARCH-020, REQ-SEARCH-021 |
| DSG-SEARCH-018 | REQ-SEARCH-007, REQ-SEARCH-008, REQ-SEARCH-019, REQ-SEARCH-021 |
| DSG-SEARCH-019 | REQ-SEARCH-007, REQ-SEARCH-008, REQ-SEARCH-012, REQ-SEARCH-020, REQ-SEARCH-021 |
