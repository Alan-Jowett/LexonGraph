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

### REQ-SEARCH-006

The crate shall surface explicit failure when the root block cannot be loaded,
when a selected child block cannot be loaded, when a visited block is malformed,
when a visited block is incompatible with the target embedding, or when the
search cannot produce `n` reachable leaf candidates.

### REQ-SEARCH-007

The crate shall keep protocol-required search orchestration separate from
implementation-defined policy concerns through trait-based extension points.

### REQ-SEARCH-008

At minimum, the crate shall expose trait-governed policy boundaries for:

- embedding compatibility checks between the target embedding and a visited
  block's `embedding_spec`
- candidate scoring inputs derived from the target embedding and candidate
  embedding

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

## Out of Scope

This crate does not define or own:

- block wire encoding or block validity rules
- block-ID derivation rules
- storage backend implementations
- indexing tree construction strategy
- any single required embedding model or embedding runtime
- any single required similarity metric
- any policy that overrides the protocol-defined ordering and tie-break rules

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/search.md` and
`docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
