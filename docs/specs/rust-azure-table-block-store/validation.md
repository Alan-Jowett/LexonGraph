<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Azure Table Block Store Validation

## Status

Draft validation specification for `lexongraph-block-store-azure-table`.

## Validation Scope

These validation entries define the expected conformance surface for the Azure
Table backend in addition to the parent block-store trait validation surface.

Block validity, canonical serialization, block-ID derivation, and the
backend-neutral `BlockStore` contract remain normatively defined by
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`, and
`docs/specs/rust-block-storage-trait/`.

## Validation Entries

### VAL-AZURE-TABLE-STORE-001

Construct the Azure Table-backed store with a caller-supplied table SAS URL.

**Pass condition:** construction succeeds for a valid table-root SAS URL, and
the consumer is not required to know any implementation-specific entity-key
layout.

**Traces to:** REQ-AZURE-TABLE-STORE-003

### VAL-AZURE-TABLE-STORE-002

Attempt to construct the Azure Table-backed store with malformed URLs, account-
root URLs, entity-scoped URLs, or URLs without a non-empty SAS signature
parameter.

**Pass condition:** construction fails explicitly as a backend failure and does
not return an initialized store.

**Traces to:** REQ-AZURE-TABLE-STORE-003

### VAL-AZURE-TABLE-STORE-003

Store a valid typed block through `put`, then retrieve it through `get` using
the returned block ID.

**Pass condition:** `get` succeeds and returns a validated typed block with the
same logical meaning and block ID.

**Traces to:** REQ-AZURE-TABLE-STORE-004, REQ-AZURE-TABLE-STORE-006

### VAL-AZURE-TABLE-STORE-004

Store the same logical block multiple times through `put`, including a case in
which another publisher has already created the same deterministic entity.

**Pass condition:** each successful call returns the same block ID and the store
does not present divergent content under that identifier.

**Traces to:** REQ-AZURE-TABLE-STORE-007, REQ-AZURE-TABLE-STORE-009

### VAL-AZURE-TABLE-STORE-005

Request a block ID whose mapped entity is not present in the table.

**Pass condition:** `get` returns `Ok(None)`.

**Traces to:** REQ-AZURE-TABLE-STORE-006

### VAL-AZURE-TABLE-STORE-006

Populate the mapped entity for a requested block ID with bytes whose verified
identity differs from that block ID.

**Pass condition:** `get` fails explicitly with an integrity-mismatch error.

**Traces to:** REQ-AZURE-TABLE-STORE-006

### VAL-AZURE-TABLE-STORE-007

Populate the mapped entity for a requested block ID with malformed payload
metadata, malformed chunk properties, or protocol-invalid reconstructed bytes.

**Pass condition:** `get` fails explicitly with a malformed-content error and
does not report absence.

**Traces to:** REQ-AZURE-TABLE-STORE-006

### VAL-AZURE-TABLE-STORE-008

Attempt `put` through a store constructed from a SAS URL that lacks the
permissions required to publish the deterministic entity.

**Pass condition:** `put` fails explicitly as a backend failure, and
construction itself was not required to reject the SAS URL beforehand.

**Traces to:** REQ-AZURE-TABLE-STORE-003, REQ-AZURE-TABLE-STORE-008

### VAL-AZURE-TABLE-STORE-009

Construct the Azure Table-backed store against a mock or probe surface that
would observe any eager table-creation or table-existence check during
construction.

**Pass condition:** construction succeeds without issuing table-creation or
table-existence probes, leaving table provisioning to IaC and runtime failures
to the operations that require backend access.

**Traces to:** REQ-AZURE-TABLE-STORE-003

### VAL-AZURE-TABLE-STORE-010

Attempt `put` for canonical block bytes whose encoded payload would exceed the
Azure Table limits for one entity in this revision.

**Pass condition:** `put` fails explicitly before publication, does not create
partial state, and does not silently fragment the block across multiple
entities or another backend.

**Traces to:** REQ-AZURE-TABLE-STORE-014, REQ-AZURE-TABLE-STORE-015

### VAL-AZURE-TABLE-STORE-011

Store and retrieve a valid typed block whose canonical bytes exceed one Azure
Table binary property's limit but still fit within one Azure Table entity in
this revision.

**Pass condition:** `put` succeeds, `get` succeeds, and the retrieved block
matches the original logical block and block ID, proving the multi-property
single-entity payload path works for supported larger blocks.

**Traces to:** REQ-AZURE-TABLE-STORE-006, REQ-AZURE-TABLE-STORE-015

### VAL-AZURE-TABLE-STORE-012

Run the parent block-store conformance suite against the Azure Table-backed
implementation.

**Pass condition:** the backend satisfies the shared `put`/`get`/enumeration`
contract without backend-specific changes to the parent trait API.

**Traces to:** REQ-AZURE-TABLE-STORE-001

### VAL-AZURE-TABLE-STORE-013

Publish multiple valid blocks in one table, then consume the parent trait's
enumeration surface through the Azure Table-backed implementation.

**Pass condition:** enumeration yields the published block IDs without exposing
table URLs, partition keys, or row keys at the trait boundary.

**Traces to:** REQ-AZURE-TABLE-STORE-010, REQ-AZURE-TABLE-STORE-011

### VAL-AZURE-TABLE-STORE-014

Populate the table with recognized block entities and unrelated entities, then
enumerate block IDs.

**Pass condition:** enumeration reports only recognized block IDs and does not
report unrelated entities or other table artifacts.

**Traces to:** REQ-AZURE-TABLE-STORE-012

### VAL-AZURE-TABLE-STORE-015

Cause entity listing, payload inspection, or decoding of a malformed recognized
block-entity candidate such as a shard-prefix-mismatched key pair to fail
during enumeration.

**Pass condition:** enumeration fails explicitly as a backend failure rather
than silently omitting the affected stored state.

**Traces to:** REQ-AZURE-TABLE-STORE-013

### VAL-AZURE-TABLE-STORE-016

Store a valid typed block through `put`, then inspect the Azure Table entity
keys used for the published payload.

**Pass condition:** the implementation derives the deterministic
`PartitionKey`/`RowKey` pair from the returned block ID and stores the
canonical bytes within that one entity.

**Traces to:** REQ-AZURE-TABLE-STORE-004, REQ-AZURE-TABLE-STORE-005,
REQ-AZURE-TABLE-STORE-014

### VAL-AZURE-TABLE-STORE-017

Populate the mapped entity for a requested block ID with valid content, then
make the entity unreadable or otherwise inaccessible before calling `get`.

**Pass condition:** `get` fails explicitly as a backend failure and does not
report absence.

**Traces to:** REQ-AZURE-TABLE-STORE-006, REQ-AZURE-TABLE-STORE-008

### VAL-AZURE-TABLE-STORE-018

Attempt identifier enumeration through a store constructed from a SAS URL that
lacks the permissions required to query or list the deterministic entities.

**Pass condition:** identifier enumeration fails explicitly as a backend failure,
and construction itself was not required to reject the SAS URL beforehand.

**Traces to:** REQ-AZURE-TABLE-STORE-003, REQ-AZURE-TABLE-STORE-008

### VAL-AZURE-TABLE-STORE-019

Cause a transient transport failure during `put` before Azure returns any
backend response, including both a case that later succeeds on retry and a case
that continues failing until the bounded retry policy is exhausted.

**Pass condition:** `put` retries the deterministic entity insert after the
transient transport failure, succeeds when a later retry reaches a successful
backend response, and otherwise fails explicitly as a backend failure.

**Traces to:** REQ-AZURE-TABLE-STORE-018

### VAL-AZURE-TABLE-STORE-020

Cause a transient transport failure during `get` or table query before Azure
returns any backend response, including both a case that later succeeds on
retry and a case that continues failing until the bounded retry policy is
exhausted.

**Pass condition:** `get` and identifier enumeration retry the deterministic
entity read or table query after the transient transport failure, succeed when a
later retry reaches a successful backend response, retries a mid-stream paged
query without restarting from the beginning, and otherwise fails explicitly as
backend failures without claiming success or absence.

**Traces to:** REQ-AZURE-TABLE-STORE-019

### VAL-AZURE-TABLE-STORE-021

Inspect the crate's integration-test surface for live Azure verification.

**Pass condition:** a dedicated live-test mode exists, it requires explicit
selection rather than running as part of the default workspace test path, and
it documents or enforces the live configuration needed to supply a real table
SAS URL.

**Traces to:** REQ-AZURE-TABLE-STORE-016

### VAL-AZURE-TABLE-STORE-022

Run the dedicated live Azure verification mode against a fresh real Azure Table
Storage table using a valid table SAS URL.

**Pass condition:** the live verification succeeds after proving constructor
success, `put` publication, `get` round-trip retrieval, `get` absence handling
for an unmapped block ID, and block-ID enumeration for blocks published by the
test.

**Traces to:** REQ-AZURE-TABLE-STORE-017

### VAL-AZURE-TABLE-STORE-023

Inspect the repository for a crate named `lexongraph-block-store-azure-table`
that is separate from the existing Azure blob-backed crates and exposes a
`BlockStore` implementation for the Azure Table backend.

**Pass condition:** the repository contains the dedicated crate with that name,
its workspace wiring keeps it distinct from `lexongraph-block-store-azure` and
`lexongraph-block-store-azure-sdk`, and the crate is the implementation home for
the Azure Table backend.

**Traces to:** REQ-AZURE-TABLE-STORE-002

### VAL-AZURE-TABLE-STORE-024

Inspect the crate's test-only verification surface for mock-backed Azure Table
simulation.

**Pass condition:** the repository provides a mock-backed surface that can
observe constructor behavior, simulate publish/read/query outcomes, and inject
malformed or integrity-mismatched recognized block entities without broadening
the production `BlockStore` API.

**Traces to:** REQ-AZURE-TABLE-STORE-020
