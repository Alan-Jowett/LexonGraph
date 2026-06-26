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

Version 2 separates structural metadata (`version`, `type`) from block content
so the shared protocol can preserve the existing branch/leaf semantics while
also admitting first-class custom block types for higher-layer metadata and
future evolution.

## Top-Level Shape

Every version-2 block has the logical shape:

```text
BlockV2 {
  version: 2,
  type: utf8-string,
  content: cbor-value
}
```

The canonical on-wire representation uses versioned integer field keys:

- `0` -> `version`
- `1` -> `type`
- `2` -> `content`

The top-level envelope is valid only when:

- `version` is the unsigned integer value `2`
- `type` is a non-empty UTF-8 text string
- the top-level map contains exactly keys `0`, `1`, and `2`, in canonical order
- no other top-level keys are present

If `content` is a map, canonical CBOR ordering and duplicate-key rejection
apply recursively within that map and any nested maps it contains.

## Type Governance

The following `type` values are reserved by the shared protocol:

- `branch`
- `leaf`

All other non-empty UTF-8 text strings are application-defined custom block
types.

The shared protocol does not constrain the internal structure of a custom type
string beyond requiring it to be non-empty UTF-8 text, and it does not assign
hierarchical meaning to any separators or substrings within that value.

The reserved type strings `branch` and `leaf` are valid only for the
corresponding reserved content shapes in this document and must not be reused
for application-defined custom content.

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

`entries` is a CBOR array. The optional `ext` field, when present, is a
canonical CBOR map with the same forward-compatible and EBCP-governed semantics
as version 1.

In particular, version 2 does not redefine the key space inside reserved-type
`ext`; any protocol-defined `ext` keys continue to come from the inherited
version-1 and EBCP authorities.

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

Leaf content does not contain a `level` field. `entries` is a CBOR array. The
optional `ext` field, when present, is a canonical CBOR map with the same
forward-compatible semantics as version 1.

## Custom Types

For any non-reserved `type` string:

- `content` may be any canonical CBOR value
- the shared protocol does not interpret or validate its inner schema
- higher layers may use custom content to carry application metadata

Custom blocks are first-class content-addressed blocks. They are storable,
retrievable, and enumerable through the block-store abstraction.

Version-2 custom blocks do not embed version-1 or version-2 blocks as nested
protocol values under shared-protocol semantics. Cross-block relationships are
expressed only through block hashes carried inside application-defined content.

## Reference Semantics

The shared protocol reserves graph traversal semantics for the reserved
`branch` and `leaf` types only.

Custom block content may contain application-defined references to branch or
leaf block hashes, but the shared protocol does not interpret those references
and reserved branch content does not point to custom blocks in this revision.

Traversal semantics for version-2 reserved `branch` and `leaf` blocks are
otherwise identical to the version-1 branch/leaf traversal semantics.

## Coexistence with Version 1

Version 1 in `docs/protocol/blocks.md` remains frozen and valid. Version 2 is a
separate protocol authority with its own top-level envelope.

A block-store or other block consumer may contain both version-1 and version-2
blocks simultaneously. The block version is determined by decoding the
top-level canonical CBOR map and inspecting the `version` field.

Version-aware decoders may interpret either version, but they must not silently
upgrade, downgrade, or rewrite one version into the other.

Future protocol versions may define additional reserved types or different
top-level envelope rules, but version 2 does not permit unknown top-level
fields.

## Identity and Canonicalization

As in version 1, a version-2 block identifier is:

`sha256(canonical_cbor_bytes(block))`

Canonical CBOR map-key ordering and duplicate-key rejection apply at the whole
block level and recursively within `content`.

## Validation Checklist

1. A version-2 block must use the `version + type + content` envelope.
2. A version-2 block must contain exactly top-level keys `0`, `1`, and `2`,
   with `version = 2` and no unknown top-level fields.
3. Reserved `branch` content must satisfy the branch invariants inherited from
   version 1.
4. Reserved `leaf` content must satisfy the leaf invariants inherited from
   version 1.
5. A custom type must use a non-empty non-reserved UTF-8 `type` string.
6. Custom content must be canonical CBOR but is otherwise opaque to the shared
   protocol layer.
7. Cross-version coexistence must not rely on silent version conversion or
   nested reuse of one block version as the other's top-level block value.
