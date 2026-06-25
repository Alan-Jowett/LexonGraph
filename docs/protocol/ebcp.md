<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# LexonGraph Embedding Block Compression Protocol (EBCP)

## Status

This document is the canonical wire-format specification for compressed
branch-entry embeddings carried inside LexonGraph non-leaf blocks.

## Scope

EBCP defines:

1. the branch-only `EmbeddingSpec.encoding` values used by compressed
   non-leaf blocks
2. the required `ext` metadata layout for those encodings
3. the byte-level payload layout of branch-entry `embedding` values stored under
   those encodings
4. the reconstruction semantics that search and other readers use to interpret
   those payloads

EBCP does **not** define a standalone block container. The enclosing block
layout, canonical CBOR encoding, block validity rules, and block identifiers
remain defined by `docs/protocol/blocks.md`.

## Relationship to Other Protocols

- `docs/protocol/blocks.md` remains authoritative for block structure, field
  keys, canonicalization, and hash identity.
- `docs/protocol/indexing.md` owns indexing inputs and outputs, not compressed
  branch payload layout.
- `docs/protocol/search.md` owns traversal, ranking, and termination semantics,
  not compressed branch payload layout.

This document defines only how a non-leaf block's branch-entry `embedding`
bytes and related `ext` metadata are interpreted when one of the EBCP
encodings is selected.

## Branch-Only Scope

EBCP applies only to `NonLeafBlock` branch-entry embeddings.

It is invalid in this revision for a `LeafBlock` to use an EBCP encoding.
Leaf-entry payloads continue to use the ordinary embedding encodings governed by
`docs/protocol/blocks.md`.

## Encoding Registry

The following `EmbeddingSpec.encoding` values are defined by this revision:

| Encoding value | Meaning | Loss model |
| --- | --- | --- |
| `pca-rot-f32le` | Orthogonally rotated full-precision branch embedding | lossless with respect to the stored `f32` rotated vector |
| `pca-rot-delta-f32le` | Rotated `f32` delta from the enclosing block's base centroid | lossless with respect to the stored `f32` rotated delta |
| `pca-rot-delta-uq` | Rotated delta encoded with one uniform per-level bit width across dimensions | lossy through quantization |
| `pca-rot-delta-vbq` | Rotated delta encoded with per-dimension variable bit widths | lossy through quantization |
| `ambient-delta-uq` | Ambient-space delta from the enclosing block's base centroid encoded with one uniform per-level bit width across dimensions | lossy through quantization |

In this document, the logical child-centroid embedding reconstructed for search
or inspection is denoted `x`.

## Common EBCP `ext` Metadata

When a non-leaf block uses any EBCP encoding, its top-level `ext` map shall
contain one `ebcp` descriptor map under extension key `0`.

The `ebcp` descriptor uses the following integer wire keys:

- `0` -> `version`
- `1` -> `logical_encoding`
- `2` -> `original_dims`
- `3` -> `base_centroid`
- `4` -> `rotation`
- `5` -> `quantization`

### `version`

Unsigned integer. This revision defines `version = 1`.

### `logical_encoding`

Text naming the logical ambient-space encoding reconstructed by EBCP. In this
revision, it shall be `f32le`.

### `original_dims`

Unsigned integer giving the dimensionality of the reconstructed ambient-space
embedding. It shall equal the enclosing block's `embedding_spec.dims`.

### `base_centroid`

Byte string containing `original_dims` little-endian `f32` values in ambient
space.

It is:

- absent for `pca-rot-f32le`
- required for `pca-rot-delta-f32le`
- required for `pca-rot-delta-uq`
- required for `pca-rot-delta-vbq`
- required for `ambient-delta-uq`

### `rotation`

Map describing the orthogonal ambient-space rotation used by the block:

- `0` -> `matrix_format`
- `1` -> `matrix_bytes`

`matrix_format` shall be the text value `f32le-row-major`.

`matrix_bytes` shall contain an `original_dims x original_dims` row-major
little-endian `f32` matrix `R`.

`R` maps ambient-space embeddings into the rotated space stored by the branch
payloads. Because this revision uses orthogonal rotations, a conforming reader
reconstructs ambient-space values using `R^-1`, which is equivalently `R^T`.

`rotation` is required for the `pca-rot-*` encodings in this revision and shall
be absent for `ambient-delta-uq`.

### `quantization`

Map present only for quantized encodings:

- `0` -> `mode`
- `1` -> `uniform_bit_width`
- `2` -> `bit_widths`
- `3` -> `scale_factors`

`mode` shall be:

- `1` for `pca-rot-delta-uq`
- `1` for `ambient-delta-uq`
- `2` for `pca-rot-delta-vbq`

`uniform_bit_width` is:

- required when `mode = 1`
- absent when `mode = 2`

`bit_widths` is:

- absent when `mode = 1`
- required when `mode = 2`

When present, `bit_widths` is a byte string containing one unsigned byte per
dimension in rotated order.

`scale_factors` is required for both quantized modes and is a byte string
containing `original_dims` little-endian `f32` values. Each scale factor
converts one signed quantized code into one rotated-space delta component.

## Branch-Entry `embedding` Byte Layout

The enclosing non-leaf block's `entries` array still uses ordinary branch-entry
maps from `docs/protocol/blocks.md`. Only the interpretation of the `embedding`
byte string changes.

### `pca-rot-f32le`

The `embedding` byte string contains `original_dims` little-endian `f32`
components representing one rotated ambient-space vector `y`.

Reconstruction:

`x = R^-1 y`

### `pca-rot-delta-f32le`

The `embedding` byte string contains `original_dims` little-endian `f32`
components representing one rotated delta vector `d`.

Reconstruction:

`x = base_centroid + R^-1 d`

### `pca-rot-delta-uq`

The `embedding` byte string contains one signed integer code per dimension,
packed in dimension order using the block's declared `uniform_bit_width`.

Packing rules:

1. codes are packed least-significant-bit first inside the byte stream
2. no padding bits appear between dimensions
3. trailing high bits in the final byte, if any, shall be zero

To decode one dimension `i`:

1. read one unsigned code word `q_i`
2. convert to a centered signed integer `s_i = q_i - 2^(b-1)`, where `b` is the
   declared uniform bit width
3. compute rotated-space delta component `d_i = s_i * scale_factors[i]`

After decoding all dimensions:

`x = base_centroid + R^-1 d`

### `pca-rot-delta-vbq`

The `embedding` byte string contains one signed integer code per dimension,
packed in dimension order using the per-dimension bit widths from
`bit_widths`.

Packing rules are the same as `pca-rot-delta-uq`, except each dimension uses
its own declared width `b_i`.

To decode one dimension `i`:

1. read one unsigned code word `q_i` using width `b_i`
2. convert to a centered signed integer `s_i = q_i - 2^(b_i-1)`
3. compute rotated-space delta component `d_i = s_i * scale_factors[i]`

After decoding all dimensions:

`x = base_centroid + R^-1 d`

### `ambient-delta-uq`

The `embedding` byte string contains one signed integer code per dimension,
packed in ambient dimension order using the block's declared
`uniform_bit_width`.

Packing rules:

1. codes are packed least-significant-bit first inside the byte stream
2. no padding bits appear between dimensions
3. trailing high bits in the final byte, if any, shall be zero

To decode one dimension `i`:

1. read one unsigned code word `q_i`
2. convert to a centered signed integer `s_i = q_i - 2^(b-1)`, where `b` is the
   declared uniform bit width
3. compute ambient-space delta component `d_i = s_i * scale_factors[i]`

After decoding all dimensions:

`x = base_centroid + d`

## Level-Budget Contract for the `0.5.x` Ladder

This protocol document defines encodings generically. The published `0.5.x`
indexing ladder uses them with the following declared budgets:

- root non-leaf blocks: 12 bits per dimension for the uniform rung
- interior non-leaf blocks above the lowest routing layer: 8 bits per dimension
  for the uniform rung
- lowest routing non-leaf blocks whose children are leaf blocks: 6 bits per
  dimension for the uniform rung

For the variable-bit-rate rung, the sum of per-dimension bit widths for a block
shall equal the total bit budget that the uniform rung would have used at the
same level and dimensionality.

## Validity Rules

The following are invalid in this revision:

- an EBCP encoding on a leaf block
- an EBCP branch block without `ext[0]`
- an EBCP descriptor whose `version` is unsupported
- `original_dims` that disagrees with the enclosing `EmbeddingSpec.dims`
- a missing `rotation` descriptor on a `pca-rot-*` encoding
- a present `rotation` descriptor on `ambient-delta-uq`
- a missing `base_centroid` for any delta encoding
- a present `quantization` descriptor on `pca-rot-f32le` or
  `pca-rot-delta-f32le`
- a missing `quantization` descriptor on `pca-rot-delta-uq` or
  `pca-rot-delta-vbq` or `ambient-delta-uq`
- quantization metadata whose dimensions, scales, or bit widths do not match the
  enclosing block dimensionality
- branch payload bytes whose length is inconsistent with the selected EBCP
  encoding and descriptor metadata

## Reader Contract

A conforming reader shall be able to recover the logical ambient-space branch
embedding `x` for each entry, or an equivalent comparison result against a
target embedding, using only:

- the enclosing block's canonical bytes
- the enclosing block's `embedding_spec`
- the enclosing block's EBCP `ext` metadata
- the branch entry's `embedding` bytes

No out-of-band transform catalog, training artifact, or repository-local side
channel is required for reconstruction.
