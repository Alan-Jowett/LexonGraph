# Rust Indexer Crate Validation

## Status

Draft validation specification for a Rust crate that implements the LexonGraph
indexing protocol.

## Validation Scope

These validation entries define the expected conformance surface for a crate
that implements the requirements and design in this spec package.

Protocol-level indexing invariants referenced here remain normatively defined by
`docs/protocol/indexing.md`. Block validity, canonical serialization, and
block-ID expectations remain normatively defined by `docs/protocol/blocks.md`
and the `docs/specs/rust-block-crate/` specification package.

## Validation Entries

### VAL-INDEXER-001

Invoke the indexer with zero items.

**Pass condition:** indexing fails explicitly and does not produce a root block
ID or block set.

**Traces to:** REQ-INDEXER-006, REQ-INDEXER-010

### VAL-INDEXER-002

Provide one indexing item whose content reference resolves successfully to
content usable for indexing.

**Pass condition:** the indexer constructs exactly one leaf block, persists it,
and returns that leaf block as the root.

**Traces to:** REQ-INDEXER-006, REQ-INDEXER-007, REQ-INDEXER-008,
REQ-INDEXER-009, REQ-INDEXER-013

### VAL-INDEXER-003

Provide an indexing item whose content reference cannot be resolved or resolves
to content unusable for indexing.

**Pass condition:** indexing fails explicitly rather than reporting success or
partial success.

**Traces to:** REQ-INDEXER-009, REQ-INDEXER-010

### VAL-INDEXER-004

Run indexing twice with the same logical item set, metadata, content
references resolving to the same logical content, `embedding_spec`, block size
target, and deterministic trait implementations.

**Pass condition:** both runs produce the same root block ID and the same
persisted block set.

**Traces to:** REQ-INDEXER-014

### VAL-INDEXER-005

Index exactly one item.

**Pass condition:** exactly one leaf block is produced, that leaf contains
exactly one leaf entry derived from the item, and that leaf block is the root.

**Traces to:** REQ-INDEXER-006, REQ-INDEXER-013, REQ-INDEXER-014

### VAL-INDEXER-006

Index multiple items that require one or more intermediate layers.

**Pass condition:** the indexer produces exactly one leaf block per item and
repeats node construction until exactly one root block remains.

**Traces to:** REQ-INDEXER-006, REQ-INDEXER-013, REQ-INDEXER-014

### VAL-INDEXER-007

Index enough items to require intermediate node blocks under a configured block
size target.

**Pass condition:** each intermediate node block remains at or below the input
block size limit and contains at least two child entries.

**Traces to:** REQ-INDEXER-013, REQ-INDEXER-014

### VAL-INDEXER-008

Construct a candidate child-entry set that includes entries out of sort order or
multiple entries referencing the same child block ID.

**Pass condition:** the finalized child-bearing block entries are sorted by raw
embedding bytes and deduplicated by child block ID before block construction.

**Traces to:** REQ-INDEXER-013, REQ-INDEXER-014

### VAL-INDEXER-009

Use distinct resolver implementations for different reference classes such as
memory-backed references, filesystem paths, Azure Blob identifiers, or S3
object keys.

**Pass condition:** the same consumer-facing indexing contract remains
applicable without requiring backend-specific API changes in the indexer crate.

**Traces to:** REQ-INDEXER-009, REQ-INDEXER-011, REQ-INDEXER-012

### VAL-INDEXER-010

Use different embedding-generation or node-packing policy implementations that
all satisfy the crate's trait contracts.

**Pass condition:** the indexer remains conforming without changing its public
API boundary.

**Traces to:** REQ-INDEXER-011, REQ-INDEXER-012

### VAL-INDEXER-011

Provide distinct content references that resolve to the same logical content in
the same indexing context.

**Pass condition:** if metadata, `embedding_spec`, block size target, and
deterministic policy behavior are otherwise the same, the root block ID and
persisted block set remain the same.

**Traces to:** REQ-INDEXER-009, REQ-INDEXER-014

### VAL-INDEXER-012

Index one item whose content reference resolves successfully to a media type and
content bytes.

**Pass condition:** the produced leaf entry stores that resolved media type and
those resolved bytes inline in the leaf `content` payload.

**Traces to:** REQ-INDEXER-009, REQ-INDEXER-013, REQ-INDEXER-016

### VAL-INDEXER-013

Inspect the crate's public surface.

**Pass condition:** the crate's default public surface exposes the runtime
indexing contract and related public types only, keeps implementer-facing
conformance helpers behind an opt-in non-default test-oriented surface, and
does not redefine block or block-store conformance surfaces.

**Traces to:** REQ-INDEXER-017, REQ-INDEXER-018, REQ-INDEXER-019

### VAL-INDEXER-014

Use the crate's opt-in conformance-test helper surface from a downstream crate
that implements one or more of the indexer policy traits.

**Pass condition:** the downstream crate can depend on the helper surface in
tests and run the shared conformance checks without changing the default
production-facing API of the indexer crate.

**Traces to:** REQ-INDEXER-017, REQ-INDEXER-018

### VAL-INDEXER-015

Run the shared conformance harnesses against deterministic implementations of
`ContentResolver`, `EmbeddingProvider`, `CanonicalEmbeddingPolicy`, and
`NodePackingPolicy`, including fixtures that intentionally violate each trait's
contract.

**Pass condition:** the shared helpers accept contract-satisfying
implementations, reject contract-violating implementations at the appropriate
trait boundary, and rely on the existing block and block-store conformance
surfaces rather than redefining them.

**Traces to:** REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-017,
REQ-INDEXER-018, REQ-INDEXER-019

### VAL-INDEXER-016

Inspect the repository verification artifacts for the indexer crate.

**Pass condition:** the repository includes executable automated tests that
realize the validation surface in this specification package, including runtime
indexing behavior and the opt-in trait-conformance helper surface.

**Traces to:** REQ-INDEXER-015
