<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Indexer Crate Design

## Status

Draft design specification for a Rust crate that implements the LexonGraph
indexing protocol.

## Design Goals

The crate design is intended to be:

- protocol-conforming
- deterministic at the crate boundary
- explicit about policy seams
- reusable across storage backends
- compatible with asynchronously realized embedding providers
- strict about normalization invariants
- minimal at the public API boundary

## Crate Boundary

The crate owns:

- indexing-oriented public types
- indexing orchestration
- normalization required by the indexing protocol
- block construction and persistence flow
- indexer-oriented error taxonomy
- conformance helpers for indexer-owned policy traits

The crate does not own:

- block wire encoding or block-ID derivation
- block validation rules beyond invoking the block crate
- storage backend implementations
- search traversal or ranking
- the shared embedding-provider trait contract
- provider-specific embedding implementations
- any required embedding, clustering, grouping, or routing algorithm

## External Dependencies

### DSG-INDEXER-001 `Protocol dependency boundary`

The indexer crate depends on the protocol documents for normative indexing and
block invariants. It implements those constraints and does not redefine them.

### DSG-INDEXER-002 `Crate dependencies`

The indexer crate depends on:

- the block crate for typed block values, canonical serialization, block-ID
  derivation, and protocol-conforming block validation
- the block-storage trait crate for backend-agnostic block persistence and
  retrieval
- the DCBC crate for the built-in default node-packing clustering engine
- the embeddings-trait crate for the shared embedding-provider contract used by
  indexing

## Core Types

### DSG-INDEXER-003 `IndexItem`

A public input type representing one application-supplied indexing unit. Each
value contains:

- application metadata
- a content reference

This revision does not require inline content at the input boundary.

### DSG-INDEXER-004 `IndexingResult`

A successful indexing result containing at least:

- the root block ID
- the produced block set, or the produced block IDs sufficient to identify that
  set

Staged APIs return constructed block artifacts grouped by stage output rather
than persistence side effects.

### DSG-INDEXER-005 `IndexerError`

An explicit error taxonomy covering at least:

- empty input
- content-resolution failure
- unusable resolved content
- embedding-generation failure
- canonical-embedding selection failure
- node-packing policy failure
- block-construction failure
- storage failure

## Policy Traits and Dependency Boundaries

### DSG-INDEXER-006 `ContentResolver`

A trait that accepts a content reference from an `IndexItem` and returns the
concrete content to be indexed for that item.

Resolution is the extension point that allows content to originate from memory,
filesystem, Azure Blob, S3, or similar systems without exposing those backend
details in the indexer API.

In this revision, the resolved content includes the media type and bytes that
will be stored inline in the produced leaf entry's `content` payload.

### DSG-INDEXER-007 `EmbeddingProvider dependency`

The indexer crate consumes an embedding-provider implementation that satisfies
the shared contract defined by the embeddings-trait crate.

The indexer crate does not define the embedding-provider trait itself and does
not own provider-specific realization details such as OpenAI-compatible request
construction.

When handling multiple items, the indexer may consume the shared ordered batch
embedding operation provided by that trait boundary.

### DSG-INDEXER-008 `CanonicalEmbeddingPolicy`

A trait that derives the canonical embedding for a produced block when that
embedding is needed as the parent-entry embedding for a child block.

The indexer crate requires the result to be deterministic, comparable within
the indexing context, and stable across rebuilds of the same logical content.

The crate also provides a built-in default realization of this trait. That
default operates on the branch block's finalized stored entries rather than on
pre-normalization candidate inputs.

### DSG-INDEXER-009 `NodePackingPolicy`

A trait that determines how leaf blocks or lower-layer node blocks are grouped
into candidate intermediate blocks.

This trait owns implementation-defined grouping and packing choices, but the
core indexer remains responsible for enforcing protocol invariants such as
minimum child count, maximum serialized size, ordering, and deduplication.

The crate also provides a built-in default implementation of this trait backed
by the shared DCBC crate, while continuing to accept downstream
implementations.

## API Surface

### DSG-INDEXER-010 `Indexer`

A public orchestration type or trait exposing an indexing operation that
accepts:

- a non-empty collection of `IndexItem` values
- an `EmbeddingSpec`
- a block size target
- a block store implementation
- implementations of the required policy traits
- an embedding-provider implementation satisfying the shared
  embeddings-trait contract

and returns an awaitable result equivalent to
`Result<IndexingResult, IndexerError>`.

The public API includes both:

- a primary default-instantiation path that supplies the built-in
  arithmetic-mean canonical-embedding policy and the built-in DCBC-backed
  node-packing policy automatically
- a constructor path that accepts a caller-supplied `CanonicalEmbeddingPolicy`
  while continuing to use the built-in DCBC-backed node-packing policy
- an explicit override path that accepts caller-supplied
  `CanonicalEmbeddingPolicy` and `NodePackingPolicy`

The public API also includes staged operations for:

- incremental leaf-block construction from `IndexItem` batches
- parent-layer construction from already-constructed child blocks

The monolithic indexing API and the staged parent-construction API also expose
observer-capable variants that accept an optional caller-supplied status
observer for parent-layer progress reporting.

Because staged parent construction is resumable through explicit artifact
passing rather than hidden orchestration state, the observer-capable staged API
also accepts a caller-supplied parent-layer ordinal for status reporting.

## Orchestration Flow

### DSG-INDEXER-011 `Core indexing pipeline`

The fixed orchestration flow is:

1. reject empty input explicitly
2. for each indexing item, resolve its content reference
3. submit the resolved inputs through the supplied embedding provider and obtain
   one ordered item-level embedding compatible with `embedding_spec` per input
4. construct exactly one leaf block containing exactly one leaf entry derived
   from that item and storing the resolved content inline
5. persist each produced leaf block through the block store
6. if one leaf block exists, return that leaf block as the root
7. otherwise, repeatedly invoke the selected node-packing policy, whether the
   built-in default or a caller-supplied override, to obtain candidate child
   groups for the current layer
   - emit clustering-start status for the current parent layer before invoking
    node packing
   - emit periodic in-progress status while clustering remains active
   - emit clustering-complete or clustering-failed status when node packing
    finishes
8. normalize candidate node entries by sorting by raw embedding bytes and
   deduplicating by child block ID
9. construct each intermediate branch block from those finalized entries and
   validate it against the block protocol
10. derive each child-bearing block's canonical embedding through the
   canonical-embedding policy applied to that finalized branch block
11. construct and persist intermediate node blocks under the protocol-defined
   size limit
   - emit parent-layer completion status after branch blocks are materialized
12. repeat until exactly one root block remains

The core indexer owns this flow even when implementation-defined policy traits
participate in individual steps.

The staged APIs decompose this flow into:

1. a leaf-construction stage that resolves content references, generates
   embeddings, and constructs one leaf block per input item
2. a parent-construction stage that accepts a current-layer child set, derives
   child-link inputs from the supplied blocks, normalizes candidate entries, and
   constructs the next branch layer
3. a monolithic composition path that repeatedly applies those stages until one
   root block remains

### DSG-INDEXER-012 `Determinism boundary`

Conformance requires deterministic behavior from the resolver, embedding
provider, and policy traits within a given indexing context.

If those trait implementations are deterministic and the logical inputs are the
same, the indexer produces the same root block ID and the same persisted block
set.

This determinism boundary also applies to each staged operation: the same staged
inputs under the same deterministic indexing context produce the same block
bytes and block identifiers.

For remotely backed embedding providers, the relevant indexing context includes
provider configuration that can affect the embedding output, but the ownership
of that configuration contract remains with the embeddings-trait crate and any
provider-specific crate layered above it.

That boundary includes internal batching behavior when it can affect embedding
output.

### DSG-INDEXER-013 `Implementation realization`

This specification package shall be realized as a concrete Rust crate in the
repository, and the implementation shall expose the public API boundary,
orchestration behavior, and indexer-owned policy traits defined by this
document.

### DSG-INDEXER-014 `Verification realization`

The repository shall include automated tests that realize the validation
entries in `docs/specs/rust-indexer-crate/validation.md`, with each validation
entry mapped to one or more executable tests.

### DSG-INDEXER-015 `Feature-gated conformance module`

The crate exposes a public conformance-test helper surface behind a non-default
Cargo feature intended for downstream tests only.

That feature is not part of the default runtime API and does not change the
production-facing indexing contract.

### DSG-INDEXER-016 `Harness shape`

The conformance-test helper surface provides reusable checks for the
`ContentResolver`, `CanonicalEmbeddingPolicy`, and `NodePackingPolicy` trait
contracts.

To verify those trait contracts without requiring production implementations in
the crate, the helper surface may define test-only harness contracts that
supply deterministic fixtures, trait implementations under test, and any
policy-specific assertions needed for the validation cases.

The helper surface does not redefine conformance for the block crate,
block-storage trait crate, or embeddings-trait crate, which continue to own
their respective reusable conformance contracts.

### DSG-INDEXER-017 `Embedding-provider non-ownership`

The indexer crate accepts embedding providers through the shared
embeddings-trait contract but does not include provider-specific implementations
or provider-specific conformance helpers in its default or opt-in API surface.

### DSG-INDEXER-018 `Default DCBC-backed node-packing realization`

The crate exposes a built-in default `NodePackingPolicy` implementation that
uses the shared DCBC crate to cluster current-layer child embeddings into
candidate groups.

That realization derives the DCBC input vectors and scalar parameters
deterministically from the ordered child layer and block size target, invokes
the DCBC crate through its public API, and maps the DCBC assignment result back
into child-index groups without changing the core indexer's ownership of
protocol normalization or block-validity checks.

The realization may use internal parallel execution only for clustering-related
substeps whose decomposition preserves the same externally visible candidate
groups, tie-breaking behavior, and explicit failure outcomes as the
single-threaded logical model.

### DSG-INDEXER-019 `Primary default constructor path`

The crate exposes a primary constructor or equivalent default-instantiation API
for `Indexer` that requires the resolver and embedding provider, and internally
instantiates both the built-in arithmetic-mean canonical-embedding policy and
the built-in DCBC-backed node-packing policy.

This path is the default production-facing way to construct the indexer when no
custom canonical-embedding or node-packing behavior is required.

### DSG-INDEXER-020 `Custom canonical-policy constructor path`

The crate also exposes a constructor or equivalent API that accepts a
caller-supplied `CanonicalEmbeddingPolicy` while continuing to instantiate the
built-in DCBC-backed node-packing policy.

Using that path preserves the canonical-embedding policy seam for downstream
consumers that need a non-default routing representative without also requiring
custom node-packing behavior.

### DSG-INDEXER-021 `Explicit full-policy override path`

The crate also exposes an explicit constructor or equivalent API that accepts
caller-supplied `CanonicalEmbeddingPolicy` and `NodePackingPolicy`
implementations.

Using that override path preserves the existing policy seams for downstream
consumers that need non-default canonical embedding and grouping or packing
behavior.

### DSG-INDEXER-022 `Built-in arithmetic-mean canonical policy`

The crate exposes a built-in default `CanonicalEmbeddingPolicy`
implementation that computes a branch block's canonical embedding as the
component-wise arithmetic mean of the embeddings stored in that block's
finalized entries.

The branch block must already reflect protocol-required entry normalization, so
the default canonical embedding is deterministic over persisted block content.

### DSG-INDEXER-023 `Deterministic numeric encoding for built-in canonical means`

The built-in arithmetic-mean canonical policy:

1. decodes stored entry embeddings according to the branch block
   `embedding_spec`
2. computes each component mean in `f64` using deterministic finalized entry
   order
3. re-encodes supported outputs as follows:
   - `f32le`: direct `f64` to IEEE-754 binary32 cast, written little-endian
   - `f16le`: direct `f64` to IEEE-754 binary16 conversion, written
     little-endian
   - `i8`: round to nearest integer with midpoint ties away from zero, then
     encode as signed 8-bit
4. fails explicitly for empty branch-entry sets, unsupported encodings,
   non-finite decoded or reduced values, and means that overflow the target
   encoding

### DSG-INDEXER-024 `Staged leaf construction API`

The crate exposes a public staged API that accepts:

- a non-empty batch of `IndexItem` values
- an `EmbeddingSpec`
- the configured content resolver and embedding-provider dependencies

and returns the constructed leaf blocks for that batch without persisting them
through the block-store contract as part of the staged operation itself.

The staged leaf-construction API remains collection-based and does not expose
provider-specific or caller-managed batching controls.

### DSG-INDEXER-025 `Staged parent construction API`

The crate exposes a public staged API that accepts:

- a non-empty collection of already-constructed child blocks representing one
  current layer
- an `EmbeddingSpec`
- a block size target
- the configured canonical-embedding and node-packing policies

and returns the constructed next-layer branch blocks.

### DSG-INDEXER-026 `Block-derived parent inputs`

The staged parent-construction API derives each child entry from the supplied
blocks themselves:

- child block ID from the canonical serialized block bytes
- child structural validity from block-protocol validation of those bytes
- child embedding from the block content, using the leaf entry embedding for
  leaf blocks and the configured canonical-embedding policy for branch blocks

The staged API does not require caller-supplied intermediate descriptors beyond
the child blocks.

### DSG-INDEXER-027 `Resumable artifact-driven composition`

The staged APIs are resumable through explicit artifact passing: callers may
persist or reload constructed blocks outside the crate and later supply those
blocks back into later stages.

The crate maintains no hidden cross-call orchestration state.

### DSG-INDEXER-029 `Collection-based batch indexing surface`

Multi-item collection indexing is the consumer-facing batch indexing surface in
this revision.

The indexer may partition work internally when invoking the shared embedding
provider, but callers continue to provide collections of `IndexItem` values
through the existing collection-shaped APIs.

### DSG-INDEXER-030 `Indexing status model`

The crate exposes a structured status payload for caller-visible indexing
progress during parent-layer construction.

That payload includes at minimum:

- a phase kind covering clustering and parent-layer materialization
- a state describing whether the phase has started, is in progress, completed,
  or failed
- the current parent-layer ordinal
- current-layer work context such as child count and block size target
- elapsed time suitable for caller reporting
- output counts or error text when applicable to terminal events

### DSG-INDEXER-031 `Observer contract`

The status observer contract is caller-owned and infallible at the indexer
boundary.

The crate invokes that observer with structured status values and does not
prescribe any specific side-effect sink such as console logging, tracing, or
external telemetry storage.

### DSG-INDEXER-032 `Periodic heartbeat semantics`

For clustering work that remains active longer than one heartbeat interval, the
indexer emits repeated in-progress status updates before terminal status.

If clustering finishes before the first heartbeat would naturally occur, the
indexer may emit only the start and terminal statuses for that phase.

### DSG-INDEXER-033 `Behavior-preserving parallelism boundary`

Internal parallelism is limited to clustering-related pure computation and
independent per-group realization steps whose decomposition cannot change:

- candidate partition membership
- branch-entry normalization inputs or ordering
- canonical-embedding derivation inputs
- explicit error outcomes
- final block IDs or the complete block set

### DSG-INDEXER-028 `Mixed child-set admissibility`

The staged parent-construction API accepts any protocol-valid current-layer
child set, including mixes of leaf and branch blocks, provided all inputs are
compatible within one indexing context, including a compatible `embedding_spec`
and deterministic policy behavior.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-INDEXER-001 | REQ-INDEXER-001, REQ-INDEXER-002, REQ-INDEXER-005 |
| DSG-INDEXER-002 | REQ-INDEXER-003, REQ-INDEXER-004, REQ-INDEXER-005, REQ-INDEXER-020, REQ-INDEXER-022 |
| DSG-INDEXER-003..005 | REQ-INDEXER-001, REQ-INDEXER-006, REQ-INDEXER-007, REQ-INDEXER-008, REQ-INDEXER-010 |
| DSG-INDEXER-006 | REQ-INDEXER-007, REQ-INDEXER-008, REQ-INDEXER-009, REQ-INDEXER-010, REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-016 |
| DSG-INDEXER-007 | REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-020, REQ-INDEXER-021 |
| DSG-INDEXER-008 | REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-014, REQ-INDEXER-028 |
| DSG-INDEXER-009 | REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-013, REQ-INDEXER-023, REQ-INDEXER-026, REQ-INDEXER-027 |
| DSG-INDEXER-010 | REQ-INDEXER-001, REQ-INDEXER-004, REQ-INDEXER-006, REQ-INDEXER-010, REQ-INDEXER-011, REQ-INDEXER-013, REQ-INDEXER-020, REQ-INDEXER-024, REQ-INDEXER-025, REQ-INDEXER-028 |
| DSG-INDEXER-011 | REQ-INDEXER-001, REQ-INDEXER-006, REQ-INDEXER-009, REQ-INDEXER-010, REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-013, REQ-INDEXER-016, REQ-INDEXER-020, REQ-INDEXER-026, REQ-INDEXER-028 |
| DSG-INDEXER-012 | REQ-INDEXER-014 |
| DSG-INDEXER-013 | REQ-INDEXER-001 |
| DSG-INDEXER-014 | REQ-INDEXER-015 |
| DSG-INDEXER-015..016 | REQ-INDEXER-011, REQ-INDEXER-012, REQ-INDEXER-017, REQ-INDEXER-018, REQ-INDEXER-019 |
| DSG-INDEXER-017 | REQ-INDEXER-019, REQ-INDEXER-021 |
| DSG-INDEXER-018 | REQ-INDEXER-022, REQ-INDEXER-023, REQ-INDEXER-026, REQ-INDEXER-027 |
| DSG-INDEXER-019 | REQ-INDEXER-024 |
| DSG-INDEXER-020 | REQ-INDEXER-024, REQ-INDEXER-025 |
| DSG-INDEXER-021 | REQ-INDEXER-025 |
| DSG-INDEXER-022 | REQ-INDEXER-013, REQ-INDEXER-024, REQ-INDEXER-028 |
| DSG-INDEXER-023 | REQ-INDEXER-028, REQ-INDEXER-029 |
| DSG-INDEXER-024 | REQ-INDEXER-030, REQ-INDEXER-031 |
| DSG-INDEXER-025 | REQ-INDEXER-032, REQ-INDEXER-033, REQ-INDEXER-035 |
| DSG-INDEXER-026 | REQ-INDEXER-032, REQ-INDEXER-034 |
| DSG-INDEXER-027 | REQ-INDEXER-011, REQ-INDEXER-034 |
| DSG-INDEXER-028 | REQ-INDEXER-033 |
| DSG-INDEXER-029 | REQ-INDEXER-013, REQ-INDEXER-030, REQ-INDEXER-035, REQ-INDEXER-036 |
| DSG-INDEXER-030 | REQ-INDEXER-037, REQ-INDEXER-039 |
| DSG-INDEXER-031 | REQ-INDEXER-037, REQ-INDEXER-039 |
| DSG-INDEXER-032 | REQ-INDEXER-038 |
| DSG-INDEXER-033 | REQ-INDEXER-014, REQ-INDEXER-023, REQ-INDEXER-027, REQ-INDEXER-040 |
