# LexonGraph Search Protocol

## Status

This document is the canonical client-side search protocol for LexonGraph.

It defines how a client traverses immutable LexonGraph blocks, starting from a
root block and a target embedding, to produce the top `N` leaf nodes.

This document is layered on top of the block protocol defined in
`docs/protocol/blocks.md`.

## Goals

The search protocol is designed to be:

- deterministic
- compatible with immutable content-addressed blocks
- tolerant of repeated embeddings
- explicit about ranking and expansion semantics
- independent of any specific similarity metric

## Inputs

A search invocation requires:

- a root block reference
- a target embedding
- a traversal width `W`
- a final result count `N`
- a client-supplied similarity function

`W` and `N` are independent:

- `W` is the number of unique child blocks selected for expansion in each round
- `N` is the number of final leaf results returned

`W` must be at least 1. An invocation with `W = 0` is invalid and must fail
explicitly.

`N` may be 0. An invocation with `N = 0` still loads the root block and scores
its current candidate set, then terminates successfully with an empty result
before any child expansion.

## Embedding Requirements

The search protocol imposes the following requirements on the embedding domain
and similarity function used by the client:

1. Embeddings must be comparable to the target embedding under the supplied
   similarity function.
2. Embeddings must be stable: repeated evaluation of the same candidate against
   the same target under the same compatibility context must yield the same
   ranking inputs.
3. Candidate ranking must be total, either directly from the similarity domain
   or by applying the protocol tie-break rules defined below.

This protocol does not define a mandatory similarity metric. It defines only how
the resulting scores are used for ranking, deduplication, and traversal.

## Candidate Semantics

Search candidates are not identified by embedding value alone.

- A branch candidate is distinguished by the referenced `child` block ID.
- A leaf candidate is distinguished by its containing block ID.

Equal embeddings do not imply equal candidates.

The following are all legal and must be treated as distinct until the protocol
explicitly says otherwise:

- equal embeddings that point to different child blocks
- equal embeddings that correspond to different leaf blocks
- different embeddings that point to the same child block

Block identity is the block hash defined by the block protocol, not the
embedding value.

## Ranking and Tie-Breaking

Candidates are ranked primarily by descending similarity to the target
embedding.

If candidates are tied on similarity, the client must apply a deterministic
tie-break so that ranking is total.

A conforming ranking order is:

1. descending similarity
2. candidate kind, with leaf candidates ordered before branch candidates
3. candidate identity:
   - branch candidates by `child` block ID in ascending bytewise order
   - leaf candidates by containing block ID in ascending bytewise order

This ordering is canonical for this revision.

## Search Procedure

The client performs the following steps:

1. Load the root block.
2. Load the block's entries into the current candidate set.
3. Score each candidate embedding against the target embedding.
4. Rank the full candidate set using the deterministic ordering rules above.
5. If the top `N` ranked candidates are all leaf candidates, return those `N`
   leaf candidates and stop.
6. Select the ranked branch candidates from the current set whose target `child`
   block IDs have not already been expanded in this search.
7. De-duplicate those branch candidates by target `child` block ID, keeping the
   highest-ranked occurrence of each child block as that block's effective rank.
8. Select the top `W` unique child blocks from that de-duplicated branch set.
9. Load the selected child blocks and mark their block IDs as expanded.
10. Remove from the current candidate set the branch candidates whose target
    child blocks were expanded in step 9.
11. Retain all remaining candidates and add the entries from the newly loaded
    child blocks to form the next candidate set.
12. Go to step 3.

If step 6 yields no expandable branch candidates and step 5 did not terminate
the search, the client must fail because the search cannot produce `N`
reachable leaf candidates.

`W` is applied after branch-candidate deduplication. If one occurrence of a
child block ranks within raw top `W` branch candidates and another occurrence of
the same child block ranks outside it, that child block is still treated as
within `W` because selection uses its best-ranked occurrence.

## Duplicate Embeddings and Repeated Targets

Repeated embeddings are legal.

If more than `W` branch candidates with identical embedding values survive to
the expansion cutoff, the client includes only the top `W` unique child blocks
according to the full deterministic ranking order after child-block
deduplication.

Repeated references to the same child block are legal. They do not cause that
child block to be expanded more than once in the same round.

Repeated identical-embedding overflow may indicate poor separation in the index.
Implementations may log this as an operational signal that the corpus may need
re-indexing.

## Compatibility and Failure Conditions

The client must fail explicitly if:

- the root block cannot be loaded
- a selected child block cannot be loaded
- a visited block is malformed
- a visited block's `embedding_spec` is incompatible with the target embedding
- the search cannot produce `N` reachable leaf candidates

Clients must not silently skip such failures and continue as though the missing
or incompatible block did not exist.

## Relationship to the Block Protocol

This document defines traversal and ranking behavior only.

It does not change:

- block wire encoding
- block identifiers
- Merkle linkage semantics
- the version 1 field-key registry

Those are defined by `docs/protocol/blocks.md`.

## Validation Checklist

The following validation cases define the minimum conformance surface for this
revision:

1. A search begins by loading exactly one root block and scoring all of its
   entries.
2. `W` and `N` are independently configurable.
3. Candidate ranking is deterministic and total for identical inputs.
4. Equal embeddings that point to different child blocks remain distinct branch
   candidates before child-block deduplication.
5. Different embeddings that point to the same child block are ranked
   independently before child-block deduplication.
6. Child-block deduplication keeps the highest-ranked occurrence of each target
   block and expands that block at most once per round.
7. Top `W` expansion targets are computed over unique child block IDs, not raw
   branch entries.
8. Leaf candidates remain eligible across rounds until termination.
9. Search terminates when the top `N` ranked candidates are all leaves.
10. Incompatible `embedding_spec`, missing blocks, and malformed blocks produce
    explicit failure.
11. Equal embeddings in different leaf blocks remain distinct leaf candidates.
