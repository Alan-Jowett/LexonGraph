# Rust Search Crate Validation

## Status

Draft validation specification for a Rust crate that implements the LexonGraph
search protocol.

In this spec package, lowercase `w` and `n` name the Rust API parameters that
correspond to protocol-level `W` and `N` in `docs/protocol/search.md`.

## Validation Scope

These validation entries define the expected conformance surface for a crate
that implements the requirements and design in this spec package.

Protocol-level search invariants referenced here remain normatively defined by
`docs/protocol/search.md`. Block-validity and block-identity expectations remain
normatively defined by `docs/protocol/blocks.md` and the
`docs/specs/rust-block-crate/` specification package.

## Validation Entries

### VAL-SEARCH-001

Invoke search with one valid root block ID, one target embedding, one `w`, and
one `n`.

**Pass condition:** the search crate loads exactly one root block before any
child expansion and scores all entries in that root block.

**Traces to:** REQ-SEARCH-004, REQ-SEARCH-005, REQ-SEARCH-009

### VAL-SEARCH-002

Run search twice with the same root block ID, target embedding, `n`, `w`,
stored block set, and deterministic trait implementations.

**Pass condition:** both runs return the same ordered leaf results or the same
explicit failure.

**Traces to:** REQ-SEARCH-011

### VAL-SEARCH-003

Provide equal-embedding branch entries that point to different child block IDs.

**Pass condition:** those entries remain distinct branch candidates before
branch-child deduplication and only compete through the protocol-defined
ordering rules.

**Traces to:** REQ-SEARCH-009, REQ-SEARCH-010

### VAL-SEARCH-004

Provide differently embedded branch entries that point to the same child block
ID.

**Pass condition:** those entries are scored and ranked independently before
branch-child deduplication, and the highest-ranked occurrence determines whether
that child can be selected for expansion.

**Traces to:** REQ-SEARCH-009, REQ-SEARCH-010

### VAL-SEARCH-005

Provide equal embeddings in leaf entries that reside in different leaf blocks.

**Pass condition:** those entries remain distinct leaf candidates and may both
appear in the final ordered results.

**Traces to:** REQ-SEARCH-009, REQ-SEARCH-010

### VAL-SEARCH-006

Construct a search round where multiple ranked branch candidates refer to the
same child block ID and at least one such child falls within the effective top
`w` selection after deduplication.

**Pass condition:** that child block is expanded at most once in the round, and
selection is computed over unique child block IDs rather than raw branch-entry
occurrences.

**Traces to:** REQ-SEARCH-009, REQ-SEARCH-010

### VAL-SEARCH-007

Configure a compatibility trait that rejects a visited block's
`embedding_spec` for the target embedding.

**Pass condition:** search fails explicitly instead of silently skipping the
incompatible block.

**Traces to:** REQ-SEARCH-006, REQ-SEARCH-007, REQ-SEARCH-008

### VAL-SEARCH-008

Cause the block store to report absence or explicit failure for the root block
ID or for a selected child block ID.

**Pass condition:** search fails explicitly and does not report partial success
as though the missing block did not exist.

**Traces to:** REQ-SEARCH-004, REQ-SEARCH-006

### VAL-SEARCH-009

Cause the block crate to reject loaded bytes as malformed, non-conforming, or
invalid for the requested block ID.

**Pass condition:** search fails explicitly and does not continue traversal with
that block.

**Traces to:** REQ-SEARCH-003, REQ-SEARCH-006

### VAL-SEARCH-010

Supply scoring-trait implementations that use different similarity metrics but
remain deterministic within the same compatibility context.

**Pass condition:** the same consumer-facing search contract remains applicable
without changing the search crate's public API or the protocol-defined
tie-break behavior.

**Traces to:** REQ-SEARCH-007, REQ-SEARCH-008, REQ-SEARCH-012

### VAL-SEARCH-011

Run search over a graph where the top `n` ranked candidates are all leaves
before all reachable branch children have been expanded.

**Pass condition:** search terminates successfully at that point and returns the
top `n` leaves without expanding lower-ranked remaining branches.

**Traces to:** REQ-SEARCH-002, REQ-SEARCH-009

### VAL-SEARCH-012

Run search over a graph that cannot produce `n` reachable leaf candidates after
all expandable branch candidates have been exhausted.

**Pass condition:** search fails explicitly rather than returning fewer than `n`
results as success.

**Traces to:** REQ-SEARCH-006, REQ-SEARCH-009
