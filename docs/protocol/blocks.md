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

## Invariants

The following invariants are normative:

1. A block is immutable once serialized and addressed.
2. A block identifier is derived from the canonical serialized bytes of the
   entire block.
3. Logically identical blocks serialize to identical bytes.
4. A block entry target is exactly one of a child block reference or a leaf.
5. Parent blocks commit to child block identifiers, forming a Merkle tree.
6. Protocol evolution must not invalidate existing block identifiers.

## Wire Encoding

Blocks are encoded as canonical CBOR.

Canonical CBOR is required so that logically identical blocks produce identical
bytes and therefore identical block identifiers.

Unless a future revision says otherwise:

- map keys use text strings
- byte-oriented payloads use CBOR byte strings
- unknown extension fields are allowed only inside `ext`

## Block Identifier

The block identifier is:

`sha256(canonical_cbor_bytes(block))`

Child references use the same identifier format. Any change to a block's
content yields a new block identifier.

## Data Model

### Block

A block has the following logical shape:

```text
Block {
  version: uint,
  kind: "map",
  entries: [Entry],
  ext?: map
}
```

Field requirements:

- `version` is required and identifies the protocol version
- `kind` is required and is `"map"` for this revision
- `entries` is required and contains the block's embedding-keyed mappings
- `ext` is optional and reserved for forward-compatible extensions

No unknown top-level fields are valid outside `ext` in version 1.

### Entry

An entry has the following logical shape:

```text
Entry {
  embedding: Embedding,
  target: ChildRef | Leaf
}
```

Each entry associates exactly one embedding with exactly one target.

### Embedding

An embedding is encoded as typed bytes:

```text
Embedding {
  dims: uint,
  encoding: text,
  data: bytes
}
```

Required fields:

- `dims`: embedding dimensionality
- `encoding`: numeric or compressed representation
- `data`: raw bytes for that representation

Known `encoding` values in this revision:

- `f32le`
- `f16le`
- `i8`
- `pq4`

Future revisions may define additional encodings.

### ChildRef

```text
ChildRef {
  block_id: bytes
}
```

`block_id` is the SHA-256 digest of the canonical CBOR bytes of the referenced
child block.

### Leaf

```text
Leaf {
  embedding: Embedding,
  metadata: map,
  content: Content
}
```

Leaf payloads carry the actual indexed material.

### Content

```text
Content {
  media_type: text,
  body: bytes
}
```

`metadata` is application-defined descriptive data for the leaf payload.
`content` is the leaf body plus its media type.

## Canonical Entry Ordering

Although a block is a logical map, it is encoded as a sorted array of entries.

This avoids relying on literal embedding values as CBOR map keys and keeps
deterministic hashing straightforward.

Entries are sorted by the canonical byte encoding of their `embedding` value.

The following are invalid:

- duplicate embeddings in the same block
- unsorted entries
- entries with both child and leaf targets
- entries with neither child nor leaf targets

## Merkle Tree Semantics

At the protocol layer, blocks form a Merkle tree:

- branch entries point to child blocks by `block_id`
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
4. New required fields require a new `version`.
5. New optional capabilities should prefer `ext` or a new versioned block kind.

These rules preserve deterministic hashing while allowing controlled protocol
growth.

## Canonical Example

The following illustrates the logical structure, not literal encoded bytes:

```text
{
  version: 1,
  kind: "map",
  entries: [
    {
      embedding: { dims: 1536, encoding: "f16le", data: <bytes> },
      target: {
        child_ref: { block_id: <32-byte sha256> }
      }
    },
    {
      embedding: { dims: 1536, encoding: "f16le", data: <bytes> },
      target: {
        leaf: {
          embedding: { dims: 1536, encoding: "f16le", data: <bytes> },
          metadata: { source: "ietf-mail", message_id: "<...>" },
          content: { media_type: "text/plain", body: <bytes> }
        }
      }
    }
  ]
}
```

## Validation Checklist

The following validation cases define the minimum conformance surface for this
revision:

1. Logically identical blocks serialize to identical canonical bytes.
2. A block identifier equals `sha256(canonical_bytes)`.
3. Each entry target is exactly one of `child_ref` or `leaf`.
4. Leaf nodes contain embedding, metadata, and content.
5. Unknown extension fields under `ext` do not invalidate parsing for known
   versions.
6. Duplicate or unsorted embeddings are rejected.
7. Updating a descendant changes the identifiers of all rewritten ancestors.
8. Distinct embedding encodings remain distinguishable in canonical bytes.

## Relationship to Higher-Level Indexing

This document specifies the block protocol only.

Higher-level indexing concepts such as centroids, routing summaries, rebuild
manifests, and transport strategy may be layered on top of this protocol, but
they are not required fields of the version 1 block format.
