<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# LexonGraph Block Protocol Version 2

## Status

Draft protocol for a version-2 block envelope that coexists with the frozen
version-1 protocol in `docs/protocol/blocks.md`.

## Scope

This document defines the top-level version-2 block envelope and the reserved
protocol-defined `branch` and `leaf` block types.

Unlike version 1, version 2 does not infer the block shape from the top-level
`level` field. Version 2 uses an explicit `type` discriminator.

## Top-Level Shape

Every version-2 block has the logical shape:

```text
BlockV2 {
  version: 2,
  type: text,
  content: cbor-value
}
```

The canonical on-wire representation uses versioned integer field keys:

- `0` -> `version`
- `1` -> `type`
- `2` -> `content`

Unknown top-level fields are invalid in this published version.

## Type Governance

The following `type` values are reserved by the shared protocol:

- `branch`
- `leaf`

All other non-empty type strings are application-defined custom block types.

The shared protocol validates only the canonical CBOR form of custom-block
`content`. Any richer meaning of a custom type is subordinate to higher-layer
specifications for that type.

## Reserved `branch` Type

The reserved `branch` type uses the logical content shape:

```text
BranchContentV2 {
  level: uint (> 0),
  embedding_spec: EmbeddingSpec,
  entries: [BranchEntry],
  ext?: map
}
```

The `EmbeddingSpec`, `BranchEntry`, canonical ordering, duplicate rejection, and
EBCP rules are the same as in version 1, but they are nested under `content`
instead of living at the top level.

## Reserved `leaf` Type

The reserved `leaf` type uses the logical content shape:

```text
LeafContentV2 {
  embedding_spec: EmbeddingSpec,
  entries: [LeafEntry],
  ext?: map
}
```

Version 2 leaf content keeps the version-1 rule that `entries` contains exactly
one `LeafEntry`.

## Custom Types

For any non-reserved `type` string:

- `content` may be any canonical CBOR value
- the shared protocol does not interpret or validate its inner schema
- higher layers may use custom content to carry application metadata

Custom blocks are first-class content-addressed blocks. They are storable,
retrievable, and enumerable through the block-store abstraction.

## Reference Semantics

The shared protocol reserves graph traversal semantics for the reserved
`branch` and `leaf` types only.

Custom block content may contain application-defined references to branch or
leaf block hashes, but the shared protocol does not interpret those references
and reserved branch content does not point to custom blocks in this revision.

## Identity and Canonicalization

As in version 1, a version-2 block identifier is:

`sha256(canonical_cbor_bytes(block))`

Canonical CBOR map-key ordering and duplicate-key rejection apply at the whole
block level and recursively within `content`.

## Validation Checklist

1. A version-2 block must use the `version + type + content` envelope.
2. Reserved `branch` content must satisfy the branch invariants inherited from
   version 1.
3. Reserved `leaf` content must satisfy the leaf invariants inherited from
   version 1.
4. A custom type must use a non-empty non-reserved `type` string.
5. Custom content must be canonical CBOR but is otherwise opaque to the shared
   protocol layer.
