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

Attempt to accept a branch block with duplicate entry identity forbidden by
`docs/protocol/blocks.md`.

**Pass condition:** rejected as non-conforming.

**Traces to:** REQ-BLOCK-CRATE-005

### VAL-007

Attempt to accept a leaf block with zero or multiple entries contrary to
`docs/protocol/blocks.md`.

**Pass condition:** rejected as non-conforming.

**Traces to:** REQ-BLOCK-CRATE-005, REQ-BLOCK-CRATE-007

### VAL-008

Attempt to accept an entry whose required kind-specific fields are missing.

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
