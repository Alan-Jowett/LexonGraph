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

### DSG-SEARCH-007 `CandidateScorer`

A trait that accepts the target embedding, one candidate embedding, and the
current compatibility context, and returns the ranking input used as the
protocol's primary ordering key.

The trait may define how similarity or distance is computed, but it does not
replace the protocol-defined tie-break rules.

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

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-SEARCH-001 | REQ-SEARCH-001, REQ-SEARCH-002, REQ-SEARCH-012 |
| DSG-SEARCH-002 | REQ-SEARCH-003, REQ-SEARCH-004, REQ-SEARCH-012 |
| DSG-SEARCH-003..005 | REQ-SEARCH-001, REQ-SEARCH-006, REQ-SEARCH-009, REQ-SEARCH-010 |
| DSG-SEARCH-006 | REQ-SEARCH-006, REQ-SEARCH-007, REQ-SEARCH-008, REQ-SEARCH-011 |
| DSG-SEARCH-007 | REQ-SEARCH-007, REQ-SEARCH-008, REQ-SEARCH-011, REQ-SEARCH-012 |
| DSG-SEARCH-008 | REQ-SEARCH-001, REQ-SEARCH-004, REQ-SEARCH-005, REQ-SEARCH-007, REQ-SEARCH-009 |
| DSG-SEARCH-009 | REQ-SEARCH-002, REQ-SEARCH-006, REQ-SEARCH-007, REQ-SEARCH-009, REQ-SEARCH-010, REQ-SEARCH-012 |
| DSG-SEARCH-010 | REQ-SEARCH-011 |
| DSG-SEARCH-011 | REQ-SEARCH-002, REQ-SEARCH-010 |
