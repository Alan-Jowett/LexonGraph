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
- explicit about the required construction steps
- explicit about required normalization invariants
- stable across rebuilds given the same indexing context
- permissive about internal packing and clustering strategy

## Scope

This protocol defines the required high-level procedure, inputs, outputs, and
invariants for a conforming indexing run.

It does not define the internal heuristics used to pack, cluster, split, or
re-organize intermediate nodes while satisfying those requirements.

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

The block size target is an indexing input that defines the maximum permissible
size for produced intermediate node blocks.

This is a hard conformance limit for intermediate node blocks. A conforming
indexing run must not produce an intermediate node block whose serialized form
exceeds that input limit.

This limit does not apply to leaf blocks in this revision. Leaf blocks are
created directly from individual content items and are not split by this
protocol.

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
8. Every content item produces exactly one leaf block containing exactly one
   leaf entry.
9. Intermediate node blocks do not exceed the input block size limit.
10. Block entries are sorted by raw embedding bytes, with any additional
    deterministic tie-breaks required by the Block Protocol.
11. Child-bearing block entries are deduplicated by child block ID.

These invariants constrain the externally visible result of indexing. They do
not prescribe the internal construction strategy that an implementation uses to
arrive at that result.

## Indexing Procedure

A conforming indexing run performs the following steps:

1. For each content item, generate an embedding compatible with the supplied
   `embedding_spec`.
2. For each content item, create exactly one leaf block containing exactly one
   leaf entry derived from that item.
3. If exactly one leaf block exists, that leaf block is the root and indexing
   stops.
4. Otherwise, create intermediate node blocks whose entries reference leaf
   blocks or lower-layer node blocks.
5. Construct each intermediate node block so that its serialized form remains at
   or below the input block size limit.
6. If more than one node block exists at the current highest layer, create a new
   higher layer of node blocks referencing the next layer down.
7. Repeat step 6 until exactly one node exists.
8. The final remaining node is the root block.

This protocol defines the required layering procedure and its externally visible
constraints. It does not define the internal strategy used to decide which child
blocks are grouped together in a particular intermediate node block.

## Indexing Result Semantics

At the indexing layer, LexonGraph distinguishes two structural roles:

- a **leaf block**, which terminates traversal with indexed payloads for one
  content item
- a **node block**, which collects references to leaf blocks or other node
  blocks

Under the block protocol, node blocks are realized as branch blocks, and leaf
blocks are realized as leaf blocks.

The root output of indexing may be either:

- a leaf block, when the indexed set fits directly into one leaf block, or
- a node block, when the indexed set spans multiple blocks

When node blocks are present, they reference leaf blocks, other node blocks, or
both, subject to the block protocol.

Each leaf block contains exactly one leaf entry derived from exactly one content
item.

Each node-block entry references one child block and uses that child block's
canonical embedding as the entry embedding.

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

- its entries must be sorted by raw embedding bytes in ascending bytewise order,
  with any additional deterministic tie-breaks required by the Block Protocol
- if multiple entries reference the same child block ID, they must be
  deduplicated so that the finalized block contains at most one entry for that
  child block ID

For leaf blocks, this revision's one-entry-per-leaf rule makes entry ordering
trivial.

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

- how leaf blocks or lower-layer node blocks are grouped into intermediate node
  blocks
- how canonical embeddings are computed
- how clusters are formed
- how routing vectors are chosen
- how an implementation packs, splits, or re-organizes intermediate node blocks
  in order to remain under the intermediate-node size limit

This protocol also does not require leaf content to be stored inline. For large
content items, an implementation may store a reference to external content in
the leaf payload instead of the content itself, but this revision does not
require that strategy.

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
7. Produced block entries are sorted by raw embedding bytes, with any additional
   deterministic tie-breaks required by the Block Protocol.
8. Produced child-bearing block entries are deduplicated by child block ID.
9. The indexing root may be either a leaf block or a node block, depending on
   the indexed set.
10. Node blocks contain references to leaf blocks, other node blocks, or both.
11. Each content item produces exactly one leaf block containing exactly one
    leaf entry.
12. If only one leaf block exists, that leaf block becomes the root.
13. Intermediate node blocks do not exceed the input block size limit.
14. If more than one node exists at a layer, indexing creates a higher layer and
    repeats until exactly one root node remains.
15. Conformance does not depend on any specific intermediate-node grouping,
    packing, or re-organization strategy.
16. Conformance does not depend on any specific canonical-embedding algorithm,
    clustering method, or routing-vector selection method.
