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

Canonical CBOR in this document has the same meaning as in
`docs/protocol/blocks.md`, consistent with the canonical encoding defined by
RFC 8949 Section 4.2.

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
- the top-level CBOR map contains exactly three entries with integer keys `0`,
  `1`, and `2`, in canonical order
- no other top-level keys are present

The entire block, including `content`, must be encoded as canonical CBOR.

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

Within reserved `branch` content, the inherited version-1 field-key registry is
preserved inside the nested `content` map: `level`, `embedding_spec`,
`entries`, and `ext` continue to use the same integer wire keys they use in the
version-1 top-level block map, and nested `EmbeddingSpec` and `BranchEntry`
maps likewise keep their version-1 integer wire keys.

`entries` is a CBOR array of `BranchEntry` items. The optional `ext` field,
when present, is a canonical CBOR map with the same forward-compatible and
EBCP-governed semantics as version 1.

A block with `type = "branch"` is invalid unless `content` conforms exactly to
the reserved `BranchContentV2` schema and its inherited version-1 and EBCP
invariants.

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

Within reserved `leaf` content, the inherited version-1 field-key registry is
preserved inside the nested `content` map: `embedding_spec`, `entries`, and
`ext` continue to use the same integer wire keys they use in the version-1
top-level leaf block map, and nested `EmbeddingSpec`, `LeafEntry`, and
`content` maps likewise keep their version-1 integer wire keys.

Leaf content does not contain a `level` field. `entries` is a CBOR array of
`LeafEntry` items. The optional `ext` field, when present, is a canonical CBOR
map with the same forward-compatible semantics as version 1.

A block with `type = "leaf"` is invalid unless `content` conforms exactly to
the reserved `LeafContentV2` schema and its inherited version-1 invariants.

## Custom Types

For any non-reserved `type` string:

- `content` may be any canonical CBOR value
- the shared protocol does not interpret or validate its inner schema
- higher layers may use custom content to carry application metadata

Custom blocks participate in hashing, storage, retrieval, and enumeration
exactly like reserved blocks.

The shared protocol assigns block semantics only to the outermost decoded block
envelope. Any value nested within `content`, even if it is byte-for-byte a
valid version-1 or version-2 block encoding, has no shared-protocol block
semantics unless interpreted by a higher-layer specification.

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
