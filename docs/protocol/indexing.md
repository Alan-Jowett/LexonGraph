# LexonGraph Indexing Protocol

## Status

This document is the canonical core indexing protocol for LexonGraph.

It defines the required inputs, outputs, and invariants for constructing
immutable LexonGraph blocks from a set of source items.

This document is layered on top of the block protocol defined in
`docs/protocol/blocks.md`.

## Goals

The indexing protocol is designed to be:

- deterministic at the protocol boundary
- compatible with immutable content-addressed blocks
- explicit about required normalization invariants
- stable across rebuilds given the same indexing context
- permissive about internal indexing strategy

## Scope

This protocol defines only the contract for what a conforming indexing run must
accept and produce.

It does not define a mandatory indexing algorithm.

## Inputs

A conforming indexing invocation requires:

- a set of items
- an embedding function
- a block size target
- an `embedding_spec`

### Items

An item is an application-supplied indexing unit from which leaf-block content
is derived.

This protocol does not define the item schema.

### Embedding Function

The embedding function maps indexed items, or implementation-defined derived
structures, into embeddings compatible with the supplied `embedding_spec`.

This protocol requires only that the indexing process use an embedding function.
It does not define the model, feature extraction process, or runtime used to
realize that function.

### Block Size Target

The block size target is an indexing input that influences block construction.

It is a target used by the indexing implementation when building blocks. It is
not, by itself, a protocol-level guarantee that every produced block satisfies a
universal serialized-size limit.

### EmbeddingSpec

`embedding_spec` defines the embedding representation used by the produced
blocks.

Compatibility and wire-level meaning of `embedding_spec` are defined by
`docs/protocol/blocks.md`.

## Required Invariants

The following invariants are normative:

1. Produced blocks are immutable.
2. Produced blocks are content-addressed.
3. Produced blocks form a tree through child-block references.
4. Every produced block has exactly one canonical embedding.
5. A canonical embedding is deterministic.
6. A canonical embedding is comparable within the indexing context.
7. A canonical embedding is stable across rebuilds of the same logical content
   under the same indexing context.
8. Block entries are sorted by raw embedding bytes.
9. Child-bearing block entries are deduplicated by child block ID.

These invariants constrain the externally visible result of indexing. They do
not prescribe the internal construction strategy that an implementation uses to
arrive at that result.

## Indexing Result Semantics

At the indexing layer, LexonGraph distinguishes two structural roles:

- a **leaf block**, which terminates traversal with indexed payloads
- a **node block**, which collects references to leaf blocks or other node
  blocks

Under the block protocol, node blocks are realized as branch blocks, and leaf
blocks are realized as leaf blocks.

The root output of indexing may be either:

- a leaf block, when the indexed set fits directly into one leaf block, or
- a node block, when the indexed set spans multiple blocks

When node blocks are present, they reference leaf blocks, other node blocks, or
both, subject to the block protocol.

## Canonical Embedding Requirements

Each produced block has one canonical embedding used as that block's embedding
representative at the indexing layer.

This protocol requires that canonical embeddings be:

- deterministic
- comparable
- stable across rebuilds

Stability is evaluated relative to the same logical content and the same
indexing context, including compatible item inputs, embedding behavior, and
`embedding_spec`.

This protocol does not require a single universal comparison metric across all
LexonGraph deployments. It requires only that canonical embeddings admit a
deterministic comparison relation within the indexing context in which they are
produced and consumed.

## Entry Normalization

Before a produced block is finalized:

- its entries must be sorted by raw embedding bytes in ascending bytewise order
- if multiple entries reference the same child block ID, they must be
  deduplicated so that the finalized block contains at most one entry for that
  child block ID

This protocol does not define which duplicate survives deduplication when an
implementation's internal construction process yields multiple candidate entries
for the same child block ID. It defines only the required normalized result.

## Required Outputs

A conforming indexing run produces:

- a root block ID
- a set of blocks that conform to the Block Protocol

The root block ID identifies the root of the constructed tree.

The produced block set is the complete block set required to materialize that
root under the indexing result.

## Explicit Non-Goals

This protocol intentionally does not define:

- how items are grouped into blocks
- how canonical embeddings are computed
- how clusters are formed
- how routing vectors are chosen

Implementations may choose different strategies for those concerns while still
conforming to this protocol, provided the required inputs, outputs, and
invariants are satisfied.

This revision also does not define shared-subtree DAG reuse. That remains a
block-protocol evolution concern rather than a requirement of the core indexing
protocol.

## Relationship to Other Protocols

This document defines index-construction constraints only.

It does not change:

- block wire encoding
- block identifiers
- block field registries
- block validity rules
- search traversal behavior

Those are defined by `docs/protocol/blocks.md` and `docs/protocol/search.md`.

## Validation Checklist

The following validation cases define the minimum conformance surface for this
revision:

1. A conforming indexing invocation accepts a set of items, an embedding
   function, a block size target, and an `embedding_spec`.
2. A conforming indexing run produces a root block ID and a set of blocks that
   conform to the Block Protocol.
3. Produced blocks are immutable and content-addressed.
4. Produced blocks form a tree through child references.
5. Each produced block has exactly one canonical embedding.
6. Canonical embeddings are deterministic, comparable, and stable across
   rebuilds of the same logical content under the same indexing context.
7. Produced block entries are sorted by raw embedding bytes.
8. Produced child-bearing block entries are deduplicated by child block ID.
9. The indexing root may be either a leaf block or a node block, depending on
   the indexed set.
10. Node blocks contain references to leaf blocks, other node blocks, or both.
11. Conformance does not depend on any specific item-grouping strategy.
12. Conformance does not depend on any specific canonical-embedding algorithm,
    clustering method, or routing-vector selection method.
