# LexonGraph Embedding Block Compression Protocol (EBCP)

---

## 1. Scope

### 1.1 Standardized by EBCP

EBCP defines:

1. Binary block container format
2. Transform metadata representation
3. Embedding reconstruction semantics
4. Block layout and parsing rules

EBCP is a **container + semantic reconstruction specification**, not a single codec.

### 1.2 Out of Scope

- Specific compression algorithms
- Clustering
- Retrieval
- Transport

---

## 2. Observable Property

EBCP imposes **no fixed embedding count per block**.

---

## 3. Reconstruction Semantics (Normative)

For each embedding j:

x̂_j = G⁻¹(c + U z_j + r_j)

Decoder MUST apply this reconstruction once z_j and r_j are decoded.

---

## 4. Encoding Layer Model

EBCP separates:

Semantic layer:
- centroid (c)
- basis (U)
- coefficients (z)
- residuals (r)

Encoding layer:
- codec (per stream)
- numeric format

> EBCP defines semantics, while coefficient encoding is codec-dependent.

---

## 5. Lossless vs Approximate

### 5.1 Lossless Mode

Lossless mode guarantees:

After decoding, the following MUST be bitwise identical to encoder-produced values:

- centroid
- basis
- coefficients
- residuals (if present)

No guarantee is made about reconstructing original input vectors.

### 5.2 Approximate Mode

Quantization is permitted.

If specified:

RMSE ≤ ε

---

## 6. Endianness

All multibyte integers SHALL use little-endian encoding.

---

## 7. Alignment

Streams MAY begin at any byte offset unless a future version specifies alignment constraints.

---

## 8. Header

struct BlockHeader {
    magic: u32
    version_major: u8
    version_minor: u8
    flags: u16

    block_size: u32
    embedding_count: u32

    dimension: u32
    k: u16
    mode: u8
    reserved: u8

    stream_count: u16
}

`block_id` is not an encoded header field. In content-addressed systems, it is
derived from the canonical serialized block bytes rather than stored inside the
block.

---

## 9. Stream Directory

struct StreamEntry {
    stream_id: u16
    offset: u32
    length: u32
    codec: u16
    numeric_format: u16
}

---

## 10. Codec Registry (Initial)

0 = raw
1 = bitpack
2 = ANS
3 = Huffman

---

## 11. Numeric Formats

Each stream MUST declare numeric_format.

Implementations MUST support at least:

- float32
- int8

---

## 12. Global Transform Registry

struct GlobalTransform {
    transform_id: u128
    version: u32
    input_dim: u32
    output_dim: u32

    encoding_format: u16
    checksum: u256
}

Checksum MUST be computed over canonical serialization:

- row-major
- fixed precision
- little-endian

### 12.1 Transform Modes

Two modes are allowed:

Referenced Transform:
- transform_id only

Embedded Transform:
- transform bytes included in block

---

## 13. Basis

Basis vectors MUST be explicitly stored.

Encoder MUST normalize vectors.

Decoder MAY validate but MUST NOT reject on floating-point variance.

---

## 14. Deterministic Encoding Profile

If enabled:

- centroid: float64 accumulation

- basis ordering:
  descending variance

- tie-breaking:
  lexicographic comparison of normalized vectors

- sign normalization:
  first nonzero element ≥ 0

- zero vector handling:
  unchanged

Termination:

Δgain = D(k) − D(k+1)
Δcost = T(k+1) − T(k)

Stop when:

N·Δgain ≤ Δcost

---

## 15. Block Invariants

- EncodedSize ≤ B
- embedding_count ≥ 1
- k ≤ dimension
- stream_count matches directory
- stream_id values unique within block
- stream bounds valid

---

## 16. Versioning

- Major mismatch: MUST reject
- Minor mismatch: MAY accept

---

## 17. Informative: Retrieval Metrics

Useful metrics include:

- Recall@K
- cosine distortion
- nearest-neighbor preservation

---

## 18. Informative: Query Strategies

Possible approaches:

- decode + search
- coefficient-space search
- centroid + bound pruning

---

## 19. Informative: Compressibility

Block capacity varies based on compressibility.

Relationship to intrinsic dimensionality is outside scope.

---

## 20. Compliance Levels

Level 0: parse + reconstruct
Level 1: validate invariants
Level 2: deterministic encoding
Level 3: error-bounded approximate mode
