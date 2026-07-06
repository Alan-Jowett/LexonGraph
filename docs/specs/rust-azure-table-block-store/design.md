<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Azure Table Block Store Design

## Status

Draft design specification for `lexongraph-block-store-azure-table`.

## Design Goals

The crate design is intended to be:

- subordinate to the backend-neutral `BlockStore` contract
- deterministic in its mapping from block ID to Azure Table entity keys
- explicit about backend failures and oversized-block rejection
- narrow at the public API boundary
- parallel to, not a replacement for, the existing Azure Blob backends

## Crate Boundary

### DSG-AZURE-TABLE-STORE-001 `Dependencies and boundary`

The Azure Table block-store crate depends on:

- `lexongraph-block` for canonical serialization, block-ID derivation, and
  validated decoding
- `lexongraph-block-store` for the `BlockStore` trait and shared error taxonomy
- an Azure Table client surface capable of SAS-authenticated entity insert,
  lookup, and query operations

This revision keeps the Azure Table production client on the pre-v1
`azure_data_tables`/`azure_storage` stack because the workspace's current v1
Azure SDK usage covers Blob Storage rather than SAS-authenticated Table entity
operations. The older Table client is intentionally isolated to this crate, and
the dependency configuration uses rustls-backed HTTP support to avoid adding a
native OpenSSL requirement.

The crate does not redefine block validation, parent trait semantics, deletion,
or compaction behavior.

### DSG-AZURE-TABLE-STORE-002 `Store type`

`AzureTableBlockStore` owns:

- a normalized table SAS `Url`
- a redacted table display string for diagnostics
- internal Azure client state dispatched through a replaceable interface for
  authenticated entity operations against the configured table

### DSG-AZURE-TABLE-STORE-003 `Construction`

Construction:

1. parses the supplied table SAS URL
2. strips fragments
3. verifies that the URL addresses a table root rather than an account root or
   specific entity
4. verifies that the query includes a non-empty SAS signature parameter
5. prepares the Azure client state needed for later table operations

Construction does not create the table and does not perform an eager existence
or permission probe.

## Entity Mapping

### DSG-AZURE-TABLE-STORE-004 `Deterministic entity keys`

Each block ID maps to exactly one deterministic entity key:

- `PartitionKey`: first four lowercase hexadecimal characters of the block ID
- `RowKey`: full lowercase hexadecimal block ID

This keyed layout remains an implementation detail of the Azure backend and is
not promoted into the parent trait boundary.

### DSG-AZURE-TABLE-STORE-005 `Single-entity payload encoding`

Each logical block is stored in one Azure Table entity whose payload properties
encode the canonical block bytes plus any metadata needed to reconstruct those
bytes deterministically.

Because Azure Table Storage limits a single string property to 64 KiB while also
limiting the serialized entity to 1 MiB, the design shall split canonical bytes
across multiple deterministic base64 string payload properties within the same
entity when one property would be insufficient but the full payload still fits
within the entity-level limit.

The property naming scheme, chunk ordering, and supporting metadata remain crate
implementation details, but the stored representation must be sufficient to:

1. reconstruct the exact canonical bytes for `get`
2. reject malformed or incomplete entity payloads explicitly
3. determine before publication whether the logical block fits within one entity

## Runtime Behavior

### DSG-AZURE-TABLE-STORE-006 `put`

`put`:

1. canonicalizes the input block through the block crate
2. derives the deterministic entity key from the returned block ID
3. encodes the canonical bytes into the single-entity payload representation
4. rejects the write explicitly before publication if the encoded entity would
   exceed Azure Table limits for this revision
5. attempts to insert the entity using create-without-overwrite semantics
6. returns the block ID on successful publication
7. treats already-existing-entity outcomes as successful convergence
8. maps other denied or failed publish outcomes to explicit backend failures

This revision does not fragment one logical block across multiple entities and
does not fall back to blob storage or any other backend.

If the Azure client reports a transport failure while issuing the insert
request, before any backend response has been received, the implementation
retries that same deterministic insert request with a bounded retry policy.

If a later retry reaches a backend response, `put` resumes the normal success,
already-existing, and explicit-failure handling for that response.

If the bounded retry budget is exhausted with transport failure on every insert
attempt, `put` reports an explicit backend failure and does not claim success.

### DSG-AZURE-TABLE-STORE-007 `get`

`get`:

1. derives the deterministic entity key from the requested block ID
2. attempts to retrieve that entity directly
3. returns `Ok(None)` when the entity is absent
4. reconstructs canonical bytes from the stored payload properties when the
   entity is present
5. validates the reconstructed bytes through the block crate before returning
   success

Malformed entity payloads and malformed reconstructed bytes map to
malformed-content failures, block-ID mismatches map to integrity-mismatch
failures, and inaccessible reads map to backend failures.

If the Azure client reports a transport failure while issuing the entity-read
request, before any backend response has been received, the implementation
retries that same deterministic read request with a bounded retry policy.

If a later retry reaches a backend response, `get` resumes the normal absence,
success, decode-failure, and explicit-failure handling for that response.

If the bounded retry budget is exhausted with transport failure on every
read attempt, `get` reports an explicit backend failure and does not claim
success or absence.

### DSG-AZURE-TABLE-STORE-008 `iter_block_ids`

`iter_block_ids` queries the configured table and streams block identifiers for
recognized block entities that match the deterministic entity-key layout.

The enumeration realization:

1. issues table queries without exposing Azure query details at the trait
   boundary
2. recognizes block entities by the deterministic `PartitionKey`/`RowKey`
   layout
3. decodes each recognized entity key back into its block ID
4. yields only decoded block IDs to callers
5. ignores unrelated entities that do not conform to the recognized key layout

Malformed recognized candidates, including shard-prefix mismatches or malformed
required payload metadata encountered during enumeration, are surfaced as
explicit backend failures.

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

### DSG-AZURE-TABLE-STORE-009 `Concurrency`

Concurrent publishers rely on deterministic entity keys plus insert-without-
overwrite publication so that multiple writers of the same block converge on one
published entity.

### DSG-AZURE-TABLE-STORE-010 `Error mapping`

Azure client construction, authorization, serialization-limit rejection, and
query or transport failures map to explicit backend failures through the parent
error taxonomy.

Entity-payload decoding failures and reconstructed block-ID mismatches continue
to map to the parent crate's malformed-content and integrity-mismatch errors.

## Verification Strategy

### DSG-AZURE-TABLE-STORE-011 `Mock-backed verification`

The crate reuses the parent block-store conformance helpers and adds Azure
Table-focused tests for:

- constructor acceptance and rejection cases
- constructor non-creation and no-preflight behavior
- deterministic entity-key mapping
- round-trip `put`/`get`
- round-trip `put`/`get` for a block that requires multi-property payload
  encoding within one entity
- explicit oversized-block rejection before publication
- conflict-success handling for already-existing entities
- `get` integrity-mismatch failure when stored entity bytes decode to a
  different block ID
- `get` malformed-content failure when stored entity payload metadata, chunk
  properties, or reconstructed bytes are malformed
- explicit backend failure for `put` when SAS permissions deny the table entity
  insert
- explicit backend failure for `get` when the entity is inaccessible or the
  backend denies the read
- explicit backend failure for enumeration when SAS permissions deny table query
- transient publish, read, and query retries
- enumeration filtering and malformed-candidate failures

### DSG-AZURE-TABLE-STORE-012 `Live Azure verification`

The crate provides an ignored live Azure integration test that consumes a real
table SAS URL from the environment and proves constructor success, publication,
retrieval, absence handling, and enumeration against a real provisioned table.

The live test assumes the table already exists through the repository's IaC
flow; the crate itself does not create that table.

### DSG-AZURE-TABLE-STORE-013 `Injectable verification boundary`

Azure Table publish, read, and query operations are dispatched through a
replaceable internal client interface so mock-backed test doubles can:

- observe constructor behavior without changing the public API
- simulate publish, read, and query authorization outcomes
- inject malformed or integrity-mismatched recognized block entities
- simulate transient transport failures, including paginated query retries

The replaceable client boundary remains an internal or test-only design detail
and is not exposed through the production `BlockStore` trait boundary.

## Traceability

| Design ID | Satisfies |
| --- | --- |
| DSG-AZURE-TABLE-STORE-001 | REQ-AZURE-TABLE-STORE-001 |
| DSG-AZURE-TABLE-STORE-002 | REQ-AZURE-TABLE-STORE-002, REQ-AZURE-TABLE-STORE-003, REQ-AZURE-TABLE-STORE-020 |
| DSG-AZURE-TABLE-STORE-003 | REQ-AZURE-TABLE-STORE-003 |
| DSG-AZURE-TABLE-STORE-004 | REQ-AZURE-TABLE-STORE-004, REQ-AZURE-TABLE-STORE-005 |
| DSG-AZURE-TABLE-STORE-005 | REQ-AZURE-TABLE-STORE-014, REQ-AZURE-TABLE-STORE-015 |
| DSG-AZURE-TABLE-STORE-006 | REQ-AZURE-TABLE-STORE-004, REQ-AZURE-TABLE-STORE-007, REQ-AZURE-TABLE-STORE-008, REQ-AZURE-TABLE-STORE-015, REQ-AZURE-TABLE-STORE-018 |
| DSG-AZURE-TABLE-STORE-007 | REQ-AZURE-TABLE-STORE-006, REQ-AZURE-TABLE-STORE-008, REQ-AZURE-TABLE-STORE-019 |
| DSG-AZURE-TABLE-STORE-008 | REQ-AZURE-TABLE-STORE-008, REQ-AZURE-TABLE-STORE-010, REQ-AZURE-TABLE-STORE-011, REQ-AZURE-TABLE-STORE-012, REQ-AZURE-TABLE-STORE-013, REQ-AZURE-TABLE-STORE-019 |
| DSG-AZURE-TABLE-STORE-009 | REQ-AZURE-TABLE-STORE-009 |
| DSG-AZURE-TABLE-STORE-010 | REQ-AZURE-TABLE-STORE-006, REQ-AZURE-TABLE-STORE-008 |
| DSG-AZURE-TABLE-STORE-011 | REQ-AZURE-TABLE-STORE-020 |
| DSG-AZURE-TABLE-STORE-012 | REQ-AZURE-TABLE-STORE-016, REQ-AZURE-TABLE-STORE-017 |
| DSG-AZURE-TABLE-STORE-013 | REQ-AZURE-TABLE-STORE-020 |
