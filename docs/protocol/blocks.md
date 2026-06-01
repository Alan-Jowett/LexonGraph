# LexonGraph Block Protocol

## Status

This document is the canonical protocol and layout specification for LexonGraph
blocks.

## Goals

LexonGraph blocks are designed to be:

- immutable
- content-addressed
- Merkle-linked
- evolvable over time
- compact and deterministic on the wire

This revision defines a block as a logical mapping from an embedding to either:

1. another block, referenced by content hash, or
2. a leaf payload containing embedding, metadata, and content

The format is optimized for density, but not at the cost of future evolution.
Version 1 therefore keeps a canonical CBOR map-based layout and concentrates
size reductions on shared structure and redundant wrappers rather than on
position-only tuple encoding.

## Invariants

The following invariants are normative:

1. A block is immutable once serialized and addressed.
2. A block identifier is derived from the canonical serialized bytes of the
   entire block.
3. Logically identical blocks serialize to identical bytes.
4. A block entry target is exactly one of a child block reference or a leaf.
5. Parent blocks commit to child block identifiers, forming a Merkle tree.
6. Protocol evolution must not invalidate existing block identifiers.
7. Common entry-wide embedding properties are encoded once per block when
   possible.

## Wire Encoding

Blocks are encoded as canonical CBOR.

Canonical CBOR is required so that logically identical blocks produce identical
bytes and therefore identical block identifiers.

This document uses human-readable logical field names in prose. The canonical
wire encoding uses compact integer field keys assigned by this specification.

Unless a future revision says otherwise:

- protocol-defined map keys use compact integers on wire
- byte-oriented payloads use CBOR byte strings
- unknown extension fields are allowed only inside `ext`
- canonical map ordering is required

### Logical Names vs Wire Keys

Logical names such as `version`, `kind`, `embedding_spec`, `entries`,
`embedding`, `child`, `metadata`, and `content` are documentation labels only.

Canonical on-wire CBOR uses integer field keys. A decoder must interpret those
keys according to the versioned field registry below, not by textual names.

### Version 1 Field-Key Registry

Top-level block map:

- `0` -> `version`
- `1` -> `kind`
- `2` -> `embedding_spec`
- `3` -> `entries`
- `15` -> `ext`

`embedding_spec` map:

- `0` -> `dims`
- `1` -> `encoding`

Branch entry map:

- `0` -> `embedding`
- `1` -> `child`

Leaf entry map:

- `0` -> `embedding`
- `1` -> `metadata`
- `2` -> `content`

`content` map:

- `0` -> `media_type`
- `1` -> `body`

## Block Identifier

The block identifier is:

`sha256(canonical_cbor_bytes(block))`

Child references use the same identifier format. Any change to a block's
content yields a new block identifier.

## Data Model

### EmbeddingSpec

An `embedding_spec` is shared by every entry in a block:

```text
EmbeddingSpec {
  dims: uint,
  encoding: text
}
```

Required fields:

- `dims`: embedding dimensionality
- `encoding`: numeric or compressed representation

Known `encoding` values in this revision:

- `f32le`
- `f16le`
- `i8`
- `pq4`

Future revisions may define additional encodings.

### BranchBlock

A branch block has the following logical shape:

```text
BranchBlock {
  version: uint,
  kind: "branch",
  embedding_spec: EmbeddingSpec,
  entries: [BranchEntry],
  ext?: map
}
```

Field requirements:

- `version` is required and identifies the protocol version
- `kind` is required and is `"branch"` for branch blocks
- `embedding_spec` is required and applies to every entry in the block
- `entries` is required and contains child references keyed by embedding bytes
- `ext` is optional and reserved for forward-compatible extensions

Normatively, a branch block defines the mapping:

`embedding_bytes -> child_block_id`

where:

- `embedding_bytes` are interpreted under the block's `embedding_spec`
- `child_block_id` is the raw SHA-256 identifier of the referenced child block

### BranchEntry

```text
BranchEntry {
  embedding: bytes,
  child: bytes
}
```

- `embedding` is the raw embedding bytes interpreted under the enclosing
  block's `embedding_spec`
- `child` is the referenced child block ID as raw SHA-256 bytes

### LeafBlock

A leaf block has the following logical shape:

```text
LeafBlock {
  version: uint,
  kind: "leaf",
  embedding_spec: EmbeddingSpec,
  entries: [LeafEntry],
  ext?: map
}
```

### LeafEntry

```text
LeafEntry {
  embedding: bytes,
  metadata: map,
  content: Content
}
```

- `embedding` is the raw embedding bytes interpreted under the enclosing
  block's `embedding_spec`
- `metadata` is application-defined descriptive data for the leaf payload
- `content` is the indexed payload

### Content

```text
Content {
  media_type: text,
  body: bytes
}
```

No unknown top-level fields are valid outside `ext` in version 1.

## Canonical Entry Ordering

Although a block is a logical map, entries are encoded as an array of compact
maps rather than as a literal CBOR map keyed by embedding bytes.

This keeps deterministic hashing straightforward while preserving room for
future additive fields on blocks and entries.

Entries are sorted deterministically under the enclosing block's
`embedding_spec`.

For branch blocks, entries are ordered by the tuple:

`(embedding_bytes, child_block_id)`

For leaf blocks, entries are ordered by their canonical entry encoding. This
preserves deterministic ordering even when multiple leaf entries carry identical
embedding bytes.

The following are invalid:

- duplicate branch entries with the same `(embedding, child)` pair
- unsorted entries
- branch entries missing `child`
- leaf entries missing `metadata` or `content`

## Merkle Tree Semantics

At the protocol layer, branch and leaf blocks form a Merkle tree:

- branch entries point to child blocks by `child`
- leaf entries terminate traversal with inline payloads
- changing a child block changes its `block_id`
- updating a parent to reference that new child produces a new parent
  `block_id`

This document does not define shared-subtree reuse. Future revisions may define
that separately if LexonGraph chooses to admit DAG-style storage reuse.

## Evolution Rules

Version 1 evolution rules:

1. `version` is mandatory.
2. Readers must reject unknown required fields outside `ext`.
3. Readers may ignore unknown fields inside `ext`.
4. New required top-level fields require a new `version`.
5. New optional capabilities should prefer additive fields or `ext`.
6. New block kinds require a new specification revision.
7. Assigned integer field keys may not be reused for different meanings within
   the same version.

These rules preserve deterministic hashing while allowing controlled protocol
growth.

## Canonical Example

The following illustrates the logical structure, not literal encoded bytes:

```text
{
  version: 1,
  kind: "branch",
  embedding_spec: { dims: 1536, encoding: "f16le" },
  entries: [
    {
      embedding: <embedding-a>,
      child: <child-a>
    },
    {
      embedding: <embedding-b>,
      child: <child-b>
    }
  ]
}
```

The corresponding canonical on-wire shape uses the field-key registry:

```text
{
  0: 1,
  1: "branch",
  2: { 0: 1536, 1: "f16le" },
  3: [
    { 0: <embedding-a>, 1: <child-a> },
    { 0: <embedding-b>, 1: <child-b> }
  ]
}
```

```text
{
  version: 1,
  kind: "leaf",
  embedding_spec: { dims: 1536, encoding: "f16le" },
  entries: [
    {
      embedding: <bytes>,
      metadata: { source: "ietf-mail", message_id: "<...>" },
      content: { media_type: "text/plain", body: <bytes> }
    }
  ]
}
```

```text
{
  0: 1,
  1: "leaf",
  2: { 0: 1536, 1: "f16le" },
  3: [
    {
      0: <bytes>,
      1: { source: "ietf-mail", message_id: "<...>" },
      2: { 0: "text/plain", 1: <bytes> }
    }
  ]
}
```

## Validation Checklist

The following validation cases define the minimum conformance surface for this
revision:

1. Logically identical blocks serialize to identical canonical bytes.
2. A block identifier equals `sha256(canonical_bytes)`.
3. Branch blocks contain only `{ embedding, child }` entries.
4. Leaf blocks contain only `{ embedding, metadata, content }` entries.
5. Unknown extension fields under `ext` do not invalidate parsing for known
   versions.
6. Redundant duplicate branch entries or unsorted entries are rejected within a
   block.
7. Updating a descendant changes the identifiers of all rewritten ancestors.
8. Distinct embedding encodings remain distinguishable in canonical bytes.
9. Canonical on-wire encoding uses the versioned integer field-key registry.
10. Reusing an assigned field key for a different meaning within the same
    version is invalid.

## Relationship to Higher-Level Indexing

This document specifies the block protocol only.

Higher-level indexing concepts such as centroids, routing summaries, rebuild
manifests, transport strategy, and the client-side search procedure may be
layered on top of this protocol, but they are not required fields of the
version 1 block format.

The canonical search procedure is defined in `docs/protocol/search.md`.

The compactness strategy for version 1 is:

1. canonical CBOR maps for future flexibility
2. compact integer map keys on wire to avoid repeated text-key overhead
3. block-scoped `embedding_spec` to avoid repeated per-entry descriptors
4. specialized branch and leaf block kinds to avoid mixed per-entry unions
5. raw bytes for embeddings and child block IDs
6. minimal nested wrappers in hot-path structures
