<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Azure Table Block Store v2 Design

## Status

Draft design specification for `lexongraph-block-store-azure-table-v2`.

## Design Goals

The crate design is intended to be:

- subordinate to the backend-neutral `BlockStore` contract
- deterministic in its mapping from block ID to Azure Table row keys
- compatible with real Azure Table responses for single-block reads and writes
- explicit about backend failures and deterministic multi-row oversized-block
  support
- narrow at the public API boundary
- separate from, not a mutation of, the existing Azure Table predecessor crate

## Crate Boundary

### DSG-AZURE-TABLE-STORE-V2-001 `Dependencies and boundary`

The Azure Table block-store v2 crate depends on:

- `lexongraph-block` for canonical serialization, block-ID derivation, and
  validated decoding
- `lexongraph-block-store` for the `BlockStore` trait and shared error taxonomy
- a SAS-authenticated Azure Table client surface capable of create-without-
  overwrite entity insert, deterministic entity lookup, and table query
  operations against a real Azure Table backend

The crate is a new implementation line and does not modify the existing
`lexongraph-block-store-azure-table` crate.

The crate does not redefine block validation, parent trait semantics, deletion,
or compaction behavior.

### DSG-AZURE-TABLE-STORE-V2-002 `Store type`

`AzureTableBlockStoreV2` owns:

- a normalized table SAS `Url`
- a redacted table display string for diagnostics
- internal Azure client state dispatched through a replaceable interface for
  authenticated entity operations against the configured table

### DSG-AZURE-TABLE-STORE-V2-003 `Construction`

Construction:

1. parses the supplied table SAS URL
2. strips fragments
3. verifies that the URL addresses a table root rather than an account root or
   specific entity
4. verifies that the query includes a non-empty SAS signature parameter
5. prepares the Azure client state needed for later table operations

Construction does not create the table and does not perform an eager existence
or permission probe.

## Row-Set Mapping

### DSG-AZURE-TABLE-STORE-V2-004 `Deterministic row keys`

Each block ID maps to exactly one deterministic root-row key:

- `PartitionKey`: first four lowercase hexadecimal characters of the block ID
- `RowKey`: full lowercase hexadecimal block ID

If a block requires more than one row, each continuation row uses:

- the same `PartitionKey`
- a `RowKey` equal to the full lowercase hexadecimal block ID plus a
  deterministic zero-padded row ordinal suffix

This keyed layout remains an implementation detail of the Azure backend and is
not promoted into the parent trait boundary.

### DSG-AZURE-TABLE-STORE-V2-005 `v2 chunked row-set format`

Each logical block is stored in one Azure Table row set whose payload
properties encode the canonical block bytes plus the metadata needed to
reconstruct those bytes deterministically.

The v2 chunked row-set format stores:

1. deterministic root-row metadata sufficient to validate schema version, total
   byte length, and total row count
2. deterministic per-row metadata sufficient to validate row ordinal and the
   number of `chunkN` properties stored in that row
3. deterministic payload properties named `chunk0`, `chunk1`, `chunk2`, ...
   within each physical row
4. row ordering defined by ascending row ordinal
5. chunk ordering within a row defined by ascending numeric suffix

Chunk sizing is derived from the real Azure Table property-value limits that
govern accepted writes for this representation, not merely from local encoding
assumptions.

The stored representation must be sufficient to:

1. reconstruct the exact canonical bytes for `get`
2. reject malformed or incomplete row-set payloads explicitly
3. determine before publication whether the logical block fits within the
   supported row-set layout for this revision

This revision's row-set layout is not bounded by a fixed artificial maximum
number of Azure Table rows per logical block. Instead, support is bounded only
by the real per-row Azure Table limits and the row-set metadata representation
chosen by this crate revision.

## Runtime Behavior

### DSG-AZURE-TABLE-STORE-V2-006 `Real-Azure response-compatible client behavior`

The production client boundary for single-block publish and single-block read
interprets Azure outcomes from the HTTP status line, response payload, and only
the response metadata required to distinguish:

1. successful publication
2. already-existing row
3. successful row read
4. absent row
5. explicit backend failure

The v2 design does not rely on a response-conversion path that rejects
otherwise usable real Azure Table responses solely because optional or
non-decisive common storage headers are absent.

### DSG-AZURE-TABLE-STORE-V2-007 `put`

`put`:

1. canonicalizes the input block through the block crate
2. derives the deterministic root-row key and any required continuation-row keys
   from the returned block ID
3. encodes the canonical bytes into the v2 chunked row-set format
4. rejects the write explicitly before publication if the encoded row set would
   exceed Azure Table limits for this revision
5. publishes continuation rows first and the root row last using create-
   without-overwrite semantics, using one continuation-row transaction when all
   continuation rows fit within a single Azure Table transaction and otherwise
   issuing the required continuation-row inserts concurrently
6. returns the block ID on successful publication
7. treats already-existing root-row outcomes as successful convergence
8. maps other denied or failed publish outcomes to explicit backend failures

The root row acts as the publication commit point for a recognized block. A
transport or backend failure may leave orphan continuation rows behind, but
those rows are not treated as a published block until the root row exists.

This revision does not fall back to blob storage or any other backend.

If the Azure client reports a transport failure while issuing a deterministic
row insert or a continuation-row transaction request, before any backend
response has been received, the implementation retries that same deterministic
request with a bounded retry policy.

When multiple required continuation-row inserts are in flight concurrently,
this retry behavior applies independently to each row-addressed insert request.

If a later retry reaches a backend response, `put` resumes the normal success,
already-existing, and explicit-failure handling for that response.

If the bounded retry budget is exhausted with transport failure on every insert
attempt, `put` reports an explicit backend failure and does not claim success.

### DSG-AZURE-TABLE-STORE-V2-008 `get`

`get`:

1. derives the deterministic root-row key from the requested block ID
2. attempts to retrieve that root row directly through an entity-addressed
   lookup rather than a filtered table query
3. returns `Ok(None)` when the root row is absent
4. uses the root-row metadata to derive the deterministic continuation-row keys
   required for the logical block
5. issues the required continuation-row direct reads concurrently once those
   deterministic row keys are known
6. reconstructs canonical bytes from the v2 chunked row-set format when the
   root row is present
7. validates the reconstructed bytes through the block crate before returning
   success

Malformed row-set payloads and malformed reconstructed bytes map to
malformed-content failures, block-ID mismatches map to integrity-mismatch
failures, and inaccessible reads map to backend failures.

If the Azure client reports a transport failure while issuing an entity-
addressed read request, before any backend response has been received, the
implementation retries that same deterministic direct-read request with a
bounded retry policy rather than switching to a filtered query path for the
same known row address.

When multiple required continuation-row reads are in flight concurrently, this
retry behavior applies independently to each direct row-addressed request.

If a later retry reaches a backend response, `get` resumes the normal absence,
success, decode-failure, and explicit-failure handling for that response.

If the bounded retry budget is exhausted with transport failure on every
read attempt, `get` reports an explicit backend failure and does not claim
success or absence.

### DSG-AZURE-TABLE-STORE-V2-009 `iter_block_ids`

`iter_block_ids` queries the configured table and streams block identifiers for
recognized block roots that match the deterministic root-row key layout.

The enumeration realization:

1. issues table queries without exposing Azure query details at the trait
   boundary
2. recognizes block roots by the deterministic `PartitionKey`/`RowKey` layout
3. ignores continuation rows and unrelated entities
4. validates the minimal root-row v2 metadata returned by the query response
   and needed to confirm that the recognized stored state is enumerable
5. decodes each recognized root key back into its block ID
6. yields only decoded block IDs to callers

The normal enumeration path does not fetch continuation rows or otherwise
re-read a recognized block row set solely to verify completeness before
yielding its block ID.

Malformed recognized candidates, including shard-prefix mismatches or malformed
required root metadata, are surfaced as explicit backend failures.

If the Azure client reports a transport failure while issuing the table query,
before any backend response has been received, the implementation retries that
same query with a bounded retry policy.

If a later retry reaches a backend response, enumeration resumes the normal
query, filtering, decoding, and explicit-failure handling for that response.

If a transport failure occurs after a paginated enumeration has already yielded
one or more pages, the retry reissues only the failed page request using the
current continuation state rather than restarting enumeration from the
beginning.

If the bounded retry budget is exhausted with transport failure on every query
attempt, enumeration reports an explicit backend failure and does not claim
that querying completed successfully.

### DSG-AZURE-TABLE-STORE-V2-010 `Concurrency`

Concurrent publishers rely on deterministic row keys plus insert-without-
overwrite publication so that multiple writers of the same block converge on one
published row set.

### DSG-AZURE-TABLE-STORE-V2-011 `Error mapping`

Azure client construction, authorization, serialization-limit rejection, and
query or transport failures map to explicit backend failures through the parent
error taxonomy.

Row-set payload decoding failures and reconstructed block-ID mismatches continue
to map to the parent crate's malformed-content and integrity-mismatch errors.

## Verification Strategy

### DSG-AZURE-TABLE-STORE-V2-012 `Mock-backed verification`

The crate reuses the parent block-store conformance helpers and adds Azure
Table-focused tests for:

- constructor acceptance and rejection cases
- constructor non-creation and no-preflight behavior
- deterministic root-row and continuation-row key mapping
- round-trip `put`/`get`
- round-trip `put`/`get` for a block that requires multiple `chunkN`
  properties within one row
- round-trip `put`/`get` for a block that requires multiple rows
- round-trip `put`/`get` for a block that requires more rows than the earlier
  fixed per-block cap and still fits the actual representational limits
- explicit oversized-block rejection before publication when the block exceeds
  the supported row-set layout because of real Azure or metadata-encoding
  limits rather than an artificial fixed row cap
- conflict-success handling for already-existing root rows
- `get` integrity-mismatch failure when stored row-set bytes decode to a
  different block ID
- `get` malformed-content failure when stored row-set payload metadata,
  continuation rows, `chunkN` properties, or reconstructed bytes are malformed
- explicit backend failure for `put` when SAS permissions deny the required row
  insert
- explicit backend failure for `get` when the root row or a required
  continuation row is inaccessible or the backend denies the read
- explicit backend failure for enumeration when SAS permissions deny table query
- transient publish, read, and query retries
- multi-row `put` verification that continuation rows use one transaction when
  they fit within one Azure Table transaction and otherwise issue concurrent
  inserts before root-row publication
- point-read verification that `get` uses direct entity-addressed lookups for
  known root-row and continuation-row addresses rather than filtered table
  queries
- multi-row `get` verification that required continuation rows are issued as
  concurrent direct reads once root metadata makes their addresses known
- enumeration verification that recognized root rows are yielded using query
  metadata alone without per-block continuation-row reads in the normal path
- response-parsing compatibility cases where otherwise valid Azure outcomes omit
  non-decisive common storage headers
- enumeration filtering and malformed-candidate failures

### DSG-AZURE-TABLE-STORE-V2-013 `Live Azure verification`

The crate provides an ignored live Azure integration test that consumes a real
table SAS URL from the environment and proves constructor success, publication,
retrieval, absence handling, enumeration, multi-chunk single-row publication,
multi-row publication, and idempotent re-publication against a real provisioned
table.

The live test assumes the table already exists through the repository's IaC
flow; the crate itself does not create that table.

### DSG-AZURE-TABLE-STORE-V2-014 `Injectable verification boundary`

Azure Table publish, read, and query operations are dispatched through a
replaceable internal client interface so mock-backed test doubles can:

- observe constructor behavior without changing the public API
- simulate publish, read, and query authorization outcomes
- distinguish a continuation-row transaction from individual row inserts closely
  enough to verify the transaction-versus-concurrent publication split
- distinguish direct entity-addressed reads from table queries so tests can
  verify the point-read access path
- observe multi-row `get` request issuance closely enough to verify that
  required continuation-row reads can be dispatched concurrently
- observe enumeration-time query requests and confirm the normal enumeration
  path does not perform per-block point reads for recognized roots
- inject malformed or integrity-mismatched recognized block row sets in the v2
  chunked row-set format
- simulate transient transport failures, including paginated query retries
- simulate otherwise valid Azure outcomes whose responses omit non-decisive
  common storage headers

The replaceable client boundary remains an internal or test-only design detail
and is not exposed through the production `BlockStore` trait boundary.

## Traceability

| Design ID | Satisfies |
| --- | --- |
| DSG-AZURE-TABLE-STORE-V2-001 | REQ-AZURE-TABLE-STORE-V2-001, REQ-AZURE-TABLE-STORE-V2-002 |
| DSG-AZURE-TABLE-STORE-V2-002 | REQ-AZURE-TABLE-STORE-V2-002, REQ-AZURE-TABLE-STORE-V2-003, REQ-AZURE-TABLE-STORE-V2-022 |
| DSG-AZURE-TABLE-STORE-V2-003 | REQ-AZURE-TABLE-STORE-V2-003 |
| DSG-AZURE-TABLE-STORE-V2-004 | REQ-AZURE-TABLE-STORE-V2-004, REQ-AZURE-TABLE-STORE-V2-005 |
| DSG-AZURE-TABLE-STORE-V2-005 | REQ-AZURE-TABLE-STORE-V2-006, REQ-AZURE-TABLE-STORE-V2-014, REQ-AZURE-TABLE-STORE-V2-015, REQ-AZURE-TABLE-STORE-V2-016 |
| DSG-AZURE-TABLE-STORE-V2-006 | REQ-AZURE-TABLE-STORE-V2-007, REQ-AZURE-TABLE-STORE-V2-021 |
| DSG-AZURE-TABLE-STORE-V2-007 | REQ-AZURE-TABLE-STORE-V2-004, REQ-AZURE-TABLE-STORE-V2-007, REQ-AZURE-TABLE-STORE-V2-008, REQ-AZURE-TABLE-STORE-V2-009, REQ-AZURE-TABLE-STORE-V2-016, REQ-AZURE-TABLE-STORE-V2-019, REQ-AZURE-TABLE-STORE-V2-021 |
| DSG-AZURE-TABLE-STORE-V2-008 | REQ-AZURE-TABLE-STORE-V2-006, REQ-AZURE-TABLE-STORE-V2-008, REQ-AZURE-TABLE-STORE-V2-020, REQ-AZURE-TABLE-STORE-V2-021 |
| DSG-AZURE-TABLE-STORE-V2-009 | REQ-AZURE-TABLE-STORE-V2-008, REQ-AZURE-TABLE-STORE-V2-010, REQ-AZURE-TABLE-STORE-V2-011, REQ-AZURE-TABLE-STORE-V2-012, REQ-AZURE-TABLE-STORE-V2-013, REQ-AZURE-TABLE-STORE-V2-020 |
| DSG-AZURE-TABLE-STORE-V2-010 | REQ-AZURE-TABLE-STORE-V2-009 |
| DSG-AZURE-TABLE-STORE-V2-011 | REQ-AZURE-TABLE-STORE-V2-006, REQ-AZURE-TABLE-STORE-V2-008 |
| DSG-AZURE-TABLE-STORE-V2-012 | REQ-AZURE-TABLE-STORE-V2-022 |
| DSG-AZURE-TABLE-STORE-V2-013 | REQ-AZURE-TABLE-STORE-V2-017, REQ-AZURE-TABLE-STORE-V2-018 |
| DSG-AZURE-TABLE-STORE-V2-014 | REQ-AZURE-TABLE-STORE-V2-022 |
