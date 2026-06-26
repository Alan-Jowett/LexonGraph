<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Block Crate Validation

## Status

Draft validation specification for the first-pass shared Rust crate that
implements the LexonGraph block protocol.

## Validation Scope

These validation entries define the expected conformance surface for a crate
that implements the requirements and design in this spec package.

Protocol-validity expectations referenced here are defined normatively by
`docs/protocol/blocks.md`.

## Validation Entries

### VAL-001

Serialize logically identical branch blocks from differently ordered input
collections.

**Pass condition:** canonical bytes and returned hashes are identical.

**Traces to:** REQ-BLOCK-CRATE-004, REQ-BLOCK-CRATE-009, REQ-BLOCK-CRATE-012

### VAL-002

Serialize logically identical leaf blocks.

**Pass condition:** canonical bytes and returned hashes are identical.

**Traces to:** REQ-BLOCK-CRATE-004, REQ-BLOCK-CRATE-009, REQ-BLOCK-CRATE-012

### VAL-003

Deserialize block bytes with a matching expected hash.

**Pass condition:** decode succeeds and returns a typed validated block plus the
verified hash.

**Traces to:** REQ-BLOCK-CRATE-003, REQ-BLOCK-CRATE-011

### VAL-004

Deserialize block bytes with a non-matching expected hash.

**Pass condition:** fails explicitly with a hash mismatch before the block is
accepted as valid.

**Traces to:** REQ-BLOCK-CRATE-011, REQ-BLOCK-CRATE-013

### VAL-005

Attempt to accept a branch block whose entries violate canonical ordering as
defined by `docs/protocol/blocks.md`.

**Pass condition:** rejected as non-conforming.

**Traces to:** REQ-BLOCK-CRATE-004, REQ-BLOCK-CRATE-005

### VAL-006

Attempt to accept a branch block with duplicate entries having the same
`(embedding_bytes, child_block_id)` pair, which is forbidden by
`docs/protocol/blocks.md`.

**Pass condition:** rejected as non-conforming.

**Traces to:** REQ-BLOCK-CRATE-005

### VAL-007

Attempt to accept a leaf block with zero or multiple entries contrary to
`docs/protocol/blocks.md`.

**Pass condition:** rejected as non-conforming.

**Traces to:** REQ-BLOCK-CRATE-005, REQ-BLOCK-CRATE-007

### VAL-008

Attempt to accept an entry whose required level-governed fields are missing.

**Pass condition:** rejected as non-conforming.

**Traces to:** REQ-BLOCK-CRATE-005, REQ-BLOCK-CRATE-007

### VAL-009

Attempt to accept an unknown top-level field outside `ext` for version 1.

**Pass condition:** rejected according to the version-1 block protocol rules.

**Traces to:** REQ-BLOCK-CRATE-005, REQ-BLOCK-CRATE-008

### VAL-010

Attempt to accept unknown fields inside `ext`.

**Pass condition:** accepted without changing the interpretation of required
protocol fields.

**Traces to:** REQ-BLOCK-CRATE-008

### VAL-011

Attempt to accept a wire encoding that uses textual field names where the
protocol requires versioned integer field keys.

**Pass condition:** rejected as non-conforming.

**Traces to:** REQ-BLOCK-CRATE-004, REQ-BLOCK-CRATE-005, REQ-BLOCK-CRATE-008

### VAL-012

Round-trip a block through serialize then deserialize using the returned hash.

**Pass condition:** succeeds and preserves block meaning and hash identity.

**Traces to:** REQ-BLOCK-CRATE-003, REQ-BLOCK-CRATE-011, REQ-BLOCK-CRATE-012

### VAL-013

Serialize otherwise similar blocks with distinct `embedding_spec.encoding`
values.

**Pass condition:** canonical bytes and returned hashes remain distinguishable.

**Traces to:** REQ-BLOCK-CRATE-005, REQ-BLOCK-CRATE-009

### VAL-014

Use the crate from a search consumer to decompose a validated branch block.

**Pass condition:** returns typed metadata and branch entries without search
logic embedded in the crate.

**Traces to:** REQ-BLOCK-CRATE-003, REQ-BLOCK-CRATE-006

### VAL-015

Use the crate from an indexing consumer to construct a block from an entry
collection.

**Pass condition:** returns a protocol-conforming block without indexing
strategy logic embedded in the crate.

**Traces to:** REQ-BLOCK-CRATE-002, REQ-BLOCK-CRATE-006

### VAL-016

Attempt to accept an unsupported future version or an invalid interpretation of
the versioned field-key registry.

**Pass condition:** rejected explicitly.

**Traces to:** REQ-BLOCK-CRATE-005, REQ-BLOCK-CRATE-008

### VAL-017

Attempt to accept a non-canonical CBOR encoding that decodes to an otherwise
valid logical block.

**Pass condition:** rejected explicitly rather than accepted under a hash
computed over non-canonical bytes.

**Traces to:** REQ-BLOCK-CRATE-004, REQ-BLOCK-CRATE-005

### VAL-018

Realize the block-crate specification as a compiled Rust crate in the
repository and execute its automated verification suite.

**Pass condition:** the repository contains a Rust crate implementing the
specified public API surface, and the automated tests cover the behavioral
validation entries `VAL-001` through `VAL-017`.

**Traces to:** REQ-BLOCK-CRATE-001, REQ-BLOCK-CRATE-010

### VAL-019

Round-trip a child-bearing block whose decoded `level` is greater than one.

**Pass condition:** decode succeeds and preserves the higher numeric `level`.

**Traces to:** REQ-BLOCK-CRATE-003, REQ-BLOCK-CRATE-014

### VAL-020

Attempt to accept a block whose top-level `level` field uses the legacy textual
`kind` encoding or otherwise fails the unsigned-integer requirement.

**Pass condition:** rejected explicitly.

**Traces to:** REQ-BLOCK-CRATE-004, REQ-BLOCK-CRATE-005, REQ-BLOCK-CRATE-008,
REQ-BLOCK-CRATE-014

### VAL-021

Round-trip a non-leaf block that uses one of the EBCP branch encodings together
with the required EBCP `ext` metadata.

**Pass condition:** serialization and hash-verified deserialization preserve the
declared EBCP encoding, the EBCP metadata, and the raw branch payload bytes
without embedding search or indexing policy logic in the block crate.

**Traces to:** REQ-BLOCK-CRATE-015, REQ-BLOCK-CRATE-018

### VAL-022

Attempt to accept a leaf block that declares an EBCP encoding, or a non-leaf
block that declares an EBCP encoding but omits the required EBCP descriptor.

**Pass condition:** both are rejected explicitly as non-conforming.

**Traces to:** REQ-BLOCK-CRATE-015, REQ-BLOCK-CRATE-016

### VAL-023

Attempt to accept an EBCP-encoded non-leaf block whose descriptor dimensionality,
quantization metadata, or branch payload length is inconsistent with the
enclosing block's declared dimensionality or encoding.

**Pass condition:** rejected explicitly as non-conforming.

**Traces to:** REQ-BLOCK-CRATE-015, REQ-BLOCK-CRATE-017

### VAL-028

Reconstruct logical branch embedding vectors through the crate's public
reconstruction helper for supported stored branch encodings, including ordinary
branch encodings and EBCP branch encodings.

**Pass condition:** the helper returns the protocol-defined logical ambient-space
`f32` vector for each supported fixture without re-parsing block bytes outside
the block crate.

**Traces to:** REQ-BLOCK-CRATE-018, REQ-BLOCK-CRATE-019

### VAL-029

Attempt public logical-branch reconstruction with an unsupported stored branch
encoding, a malformed payload length, or missing required EBCP metadata.

**Pass condition:** the helper fails explicitly rather than silently returning a
plausible vector.

**Traces to:** REQ-BLOCK-CRATE-019

### VAL-030

Serialize and hash-verify deserialize a version-2 reserved `leaf` or `branch`
block through the crate's version-aware dispatch surface.

**Pass condition:** the decoded block preserves version `2`, the reserved type,
and the canonical nested content structure.

**Traces to:** REQ-BLOCK-CRATE-020, REQ-BLOCK-CRATE-021

### VAL-031

Serialize and hash-verify deserialize a version-2 custom block with
application-defined `type` and canonical CBOR content.

**Pass condition:** the crate preserves the custom `type` string and canonical
content value without imposing reserved-type interpretation on that content.

**Traces to:** REQ-BLOCK-CRATE-020, REQ-BLOCK-CRATE-021, REQ-BLOCK-CRATE-022

### VAL-032

Attempt to decode a version-2 block through the version-aware dispatch surface
and then re-emit it as version 1 without explicit caller selection.

**Pass condition:** no silent version conversion occurs; versioned encode/decode
behavior stays explicit.

**Traces to:** REQ-BLOCK-CRATE-020, REQ-BLOCK-CRATE-023, REQ-BLOCK-CRATE-024
