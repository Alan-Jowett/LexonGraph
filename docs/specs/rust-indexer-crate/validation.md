<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
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

The shared embedding-provider trait contract and provider-specific embedding
implementations are validated by their own specification packages. This package
validates only how the indexer consumes that dependency surface.

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

Repeating the same staged leaf-construction or parent-construction call with the
same logical inputs produces the same constructed block bytes and block IDs.

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

The same logical job can also be realized by repeated staged leaf and
parent-construction calls with the same final root block ID and block set.

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

Use different embedding-provider implementations satisfying the shared
embeddings-trait contract, together with different node-packing policy
implementations satisfying the indexer-owned trait contracts.

**Pass condition:** the indexer remains conforming without changing its public
API boundary across the monolithic API and the staged parent-construction API.

**Traces to:** REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-020

### VAL-INDEXER-011

Provide distinct content references that resolve to the same logical content in
the same indexing context.

**Pass condition:** if metadata, `embedding_spec`, block size target, and
deterministic dependency behavior are otherwise the same, the root block ID and
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
does not expose provider-specific embedding implementations or redefine block,
block-store, or embeddings-trait conformance surfaces.

**Traces to:** REQ-INDEXER-017, REQ-INDEXER-018, REQ-INDEXER-019,
REQ-INDEXER-021

### VAL-INDEXER-014

Use the crate's opt-in conformance-test helper surface from a downstream crate
that implements one or more of the indexer-owned policy traits.

**Pass condition:** the downstream crate can depend on the helper surface in
tests and run the shared conformance checks for `ContentResolver`,
`CanonicalEmbeddingPolicy`, and `NodePackingPolicy` without changing the
default production-facing API of the indexer crate.

**Traces to:** REQ-INDEXER-017, REQ-INDEXER-018

### VAL-INDEXER-015

Run the shared conformance harnesses against deterministic implementations of
`ContentResolver`, `CanonicalEmbeddingPolicy`, and `NodePackingPolicy`,
including fixtures that intentionally violate each trait's contract.

**Pass condition:** the shared helpers accept contract-satisfying
implementations, reject contract-violating implementations at the appropriate
trait boundary, and rely on the existing block, block-store, and
embeddings-trait conformance surfaces rather than redefining them.

**Traces to:** REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-017,
REQ-INDEXER-018, REQ-INDEXER-019

### VAL-INDEXER-016

Inspect the repository verification artifacts for the indexer crate.

**Pass condition:** the repository includes executable automated tests that
realize the validation surface in this specification package, including runtime
indexing behavior and the opt-in trait-conformance helper surface.

**Traces to:** REQ-INDEXER-015

### VAL-INDEXER-017

Invoke the indexer with an embedding-provider implementation supplied through
the shared embeddings-trait contract that performs asynchronous work before
returning one or more valid ordered embeddings.

**Pass condition:** the indexing operation awaits the provider successfully and
produces the same protocol-conforming result shape as with an in-memory
deterministic fixture.

**Traces to:** REQ-INDEXER-012, REQ-INDEXER-020

### VAL-INDEXER-018

Construct the indexer through its primary default-instantiation path and index
multiple items that require one or more intermediate layers.

**Pass condition:** the indexing operation succeeds without the caller
supplying either a `CanonicalEmbeddingPolicy` or a `NodePackingPolicy`, and the
resulting blocks conform to the same runtime invariants as the explicit-policy
path.

**Traces to:** REQ-INDEXER-023, REQ-INDEXER-024, REQ-INDEXER-028

### VAL-INDEXER-019

Construct one indexer through the primary default-instantiation path and a
second indexer through a custom canonical-policy or full explicit-policy
override path, then index the same logical item set with both.

**Pass condition:** the default path uses the built-in arithmetic-mean
canonical policy and built-in DCBC-backed node-packing policy without explicit
policy injection, while the override path accepts caller-supplied policy
implementations and remains conforming without changing the rest of the runtime
contract.

**Traces to:** REQ-INDEXER-024, REQ-INDEXER-025, REQ-INDEXER-028

### VAL-INDEXER-020

Run indexing twice through the primary default-instantiation path with the same
logical item set, `embedding_spec`, block size target, and deterministic
dependency behavior.

**Pass condition:** the built-in DCBC-backed node-packing behavior yields the
same root block ID and persisted block set across both runs.

**Traces to:** REQ-INDEXER-027

### VAL-INDEXER-021

Use the built-in default node-packing behavior with a block size target that is
too small for the candidate intermediate-node grouping it proposes.

**Pass condition:** indexing fails explicitly through the core indexer's
protocol-enforcement path rather than emitting an oversized or otherwise
non-conforming intermediate node.

**Traces to:** REQ-INDEXER-022, REQ-INDEXER-026

### VAL-INDEXER-022

Inspect the indexer crate's dependency manifest and the implementation of its
built-in default node-packing realization.

**Pass condition:** the `lexongraph-indexer` crate depends on the shared
`lexongraph-dcbc` crate, and the built-in default `NodePackingPolicy`
realization delegates DCBC clustering behavior through that dependency rather
than reimplementing DCBC semantics locally.

**Traces to:** REQ-INDEXER-022

### VAL-INDEXER-023

Construct branch blocks with known finalized entry embeddings under supported
encodings (`i8`, `f16le`, and `f32le`) and invoke the built-in arithmetic-mean
canonical policy directly.

**Pass condition:** the built-in policy returns the expected component-wise
arithmetic mean encoded according to the block `embedding_spec`, including
midpoint ties away from zero for `i8`.

**Traces to:** REQ-INDEXER-028, REQ-INDEXER-029

### VAL-INDEXER-024

Invoke the built-in arithmetic-mean canonical policy on branch blocks whose
stored entry embeddings use an unsupported encoding or produce a non-finite
arithmetic mean.

**Pass condition:** canonical-embedding derivation fails explicitly at the
canonical-policy boundary; the indexer does not silently substitute a different
vector or continue as though canonical embedding succeeded.

**Traces to:** REQ-INDEXER-029

### VAL-INDEXER-025

Invoke the staged leaf-construction API on multiple item batches representing
one logical indexing job.

**Pass condition:** each item yields exactly one constructed leaf block, and
partitioning across batches does not alter the leaf-block set produced for the
same logical items.

**Traces to:** REQ-INDEXER-030, REQ-INDEXER-031

### VAL-INDEXER-026

Supply a valid collection of child blocks to the staged parent-construction API.

**Pass condition:** the API constructs protocol-conforming parent blocks whose
entries reference those children, remain normalized, and satisfy the size and
minimum-child-count invariants.

**Traces to:** REQ-INDEXER-032, REQ-INDEXER-033

### VAL-INDEXER-027

Construct leaf blocks in one staged call, persist or reload those blocks outside
the crate, and invoke a later staged parent-construction call on the reloaded
artifacts.

**Pass condition:** later stages succeed without any hidden in-memory state from
the earlier call.

**Traces to:** REQ-INDEXER-034

### VAL-INDEXER-028

Run one logical indexing job through the monolithic `index(...)` API and the
same job through staged leaf construction followed by repeated staged
parent-layer construction.

**Pass condition:** both paths produce the same root block ID and complete block
set.

**Traces to:** REQ-INDEXER-014, REQ-INDEXER-035

### VAL-INDEXER-029

Invoke the staged parent-construction API with a valid mixed current layer that
contains both leaf and branch blocks.

**Pass condition:** parent construction succeeds and produces conforming parent
blocks whose child-entry embeddings are derived according to the configured
canonical-embedding policy for the supplied branch children.

**Traces to:** REQ-INDEXER-033

### VAL-INDEXER-030

Invoke the staged APIs with empty batches, invalid child blocks, incompatible
inputs, or inputs that cannot produce a conforming parent layer.

**Pass condition:** failures are explicit; the crate does not silently skip,
partially succeed, or synthesize hidden recovery behavior.

**Traces to:** REQ-INDEXER-010, REQ-INDEXER-030, REQ-INDEXER-032, REQ-INDEXER-034

### VAL-INDEXER-031

Invoke the collection-based indexing API with multiple items and an
embedding-provider implementation that only realizes the shared ordered batch
embedding path rather than a single-item embedding path.

**Pass condition:** collection indexing succeeds without caller-managed
sub-batches, proving the indexer can realize multi-item embedding through the
shared batch contract while preserving the existing collection-based API shape.

**Traces to:** REQ-INDEXER-012, REQ-INDEXER-030, REQ-INDEXER-036

### VAL-INDEXER-032

Invoke monolithic indexing with a caller-supplied status observer on an item
set that requires one or more parent layers.

**Pass condition:** the observer receives structured status updates covering
clustering start and clustering completion without requiring any
repository-specific logging sink.

**Traces to:** REQ-INDEXER-037, REQ-INDEXER-039

### VAL-INDEXER-033

Use a fixture whose node-packing work remains active long enough to appear
non-trivial.

**Pass condition:** at least one in-progress clustering status update is emitted
before clustering completion, demonstrating that the crate does not report only
terminal state for long-running clustering work.

**Traces to:** REQ-INDEXER-038

### VAL-INDEXER-034

Run the same logical indexing job twice on a fixture that exercises the
internally parallel clustering path.

**Pass condition:** both runs produce the same root block ID and complete block
set, and failures remain explicit if the fixture is made non-conforming.

**Traces to:** REQ-INDEXER-014, REQ-INDEXER-027, REQ-INDEXER-040

### VAL-INDEXER-035

Connect the status observer to a caller-owned in-memory collection or similar
sink.

**Pass condition:** status delivery works without the crate owning console
output, tracing integration, or external telemetry storage.

**Traces to:** REQ-INDEXER-039

### VAL-INDEXER-036

Invoke the staged parent-construction API with a caller-supplied observer on a
valid child layer.

**Pass condition:** the staged parent-construction path emits the same status
model for clustering-related work as the monolithic indexing path.

**Traces to:** REQ-INDEXER-013, REQ-INDEXER-037, REQ-INDEXER-038
