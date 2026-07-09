<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Azure Table Block Store v2 Validation

## Status

Draft validation specification for `lexongraph-block-store-azure-table-v2`.

## Validation Scope

These validation entries define the expected conformance surface for the Azure
Table v2 backend in addition to the parent block-store trait validation
surface.

Block validity, canonical serialization, block-ID derivation, and the
backend-neutral `BlockStore` contract remain normatively defined by
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`, and
`docs/specs/rust-block-storage-trait/`.

## Validation Entries

### VAL-AZURE-TABLE-STORE-V2-001

Construct the Azure Table-backed v2 store with a caller-supplied table SAS URL.

**Pass condition:** construction succeeds for a valid table-root SAS URL, and
the consumer is not required to know any implementation-specific entity-key
layout.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-003

### VAL-AZURE-TABLE-STORE-V2-002

Attempt to construct the Azure Table-backed v2 store with malformed URLs,
account-root URLs, entity-scoped URLs, or URLs without a non-empty SAS
signature parameter.

**Pass condition:** construction fails explicitly as a backend failure and does
not return an initialized store.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-003

### VAL-AZURE-TABLE-STORE-V2-003

Store a valid typed block through `put`, then retrieve it through `get` using
the returned block ID.

**Pass condition:** `get` succeeds and returns a validated typed block with the
same logical meaning and block ID.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-004, REQ-AZURE-TABLE-STORE-V2-006

### VAL-AZURE-TABLE-STORE-V2-004

Store the same logical block multiple times through `put`, including a case in
which another publisher has already created the same deterministic entity.

**Pass condition:** each successful call returns the same block ID and the store
does not present divergent content under that identifier.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-007, REQ-AZURE-TABLE-STORE-V2-009

### VAL-AZURE-TABLE-STORE-V2-005

Request a block ID whose mapped entity is not present in the table.

**Pass condition:** `get` returns `Ok(None)`.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-006

### VAL-AZURE-TABLE-STORE-V2-006

Populate the mapped entity for a requested block ID with bytes whose verified
identity differs from that block ID.

**Pass condition:** `get` fails explicitly with an integrity-mismatch error.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-006

### VAL-AZURE-TABLE-STORE-V2-007

Populate the mapped entity for a requested block ID with malformed v2 chunked
payload metadata, missing `chunkN` properties, malformed `chunkN` values, or
protocol-invalid reconstructed bytes.

**Pass condition:** `get` fails explicitly with a malformed-content error and
does not report absence.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-006

### VAL-AZURE-TABLE-STORE-V2-008

Attempt `put` through a v2 store constructed from a SAS URL that lacks the
permissions required to publish the deterministic entity.

**Pass condition:** `put` fails explicitly as a backend failure, and
construction itself was not required to reject the SAS URL beforehand.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-003, REQ-AZURE-TABLE-STORE-V2-008

### VAL-AZURE-TABLE-STORE-V2-009

Construct the Azure Table-backed v2 store against a mock or probe surface that
would observe any eager table-creation or table-existence check during
construction.

**Pass condition:** construction succeeds without issuing table-creation or
table-existence probes, leaving table provisioning to IaC and runtime failures
to the operations that require backend access.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-003

### VAL-AZURE-TABLE-STORE-V2-010

Attempt `put` for canonical block bytes whose encoded payload would exceed the
Azure Table limits for one entity in this revision.

**Pass condition:** `put` fails explicitly before publication, does not create
partial state, and does not silently fragment the block across multiple
entities or another backend.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-014, REQ-AZURE-TABLE-STORE-V2-016

### VAL-AZURE-TABLE-STORE-V2-011

Store and retrieve a valid typed block whose canonical bytes require more than
one deterministic `chunkN` property in the v2 chunked entity format but still
fit within one Azure Table entity in this revision.

**Pass condition:** `put` succeeds, `get` succeeds, and the retrieved block
matches the original logical block and block ID, proving the multi-chunk
single-entity payload path works for supported larger blocks.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-006, REQ-AZURE-TABLE-STORE-V2-015,
REQ-AZURE-TABLE-STORE-V2-016

### VAL-AZURE-TABLE-STORE-V2-012

Run the parent block-store conformance suite against the Azure Table-backed v2
implementation.

**Pass condition:** the backend satisfies the shared `put`/`get`/`enumeration`
contract without backend-specific changes to the parent trait API.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-001

### VAL-AZURE-TABLE-STORE-V2-013

Publish multiple valid blocks in one table, then consume the parent trait's
enumeration surface through the Azure Table-backed v2 implementation.

**Pass condition:** enumeration yields the published block IDs without exposing
table URLs, partition keys, or row keys at the trait boundary.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-010, REQ-AZURE-TABLE-STORE-V2-011

### VAL-AZURE-TABLE-STORE-V2-014

Populate the table with recognized block entities and unrelated entities, then
enumerate block IDs.

**Pass condition:** enumeration reports only recognized block IDs and does not
report unrelated entities or other table artifacts.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-012

### VAL-AZURE-TABLE-STORE-V2-015

Cause entity listing, payload inspection, or decoding of a malformed recognized
block-entity candidate such as a shard-prefix-mismatched key pair or malformed
required v2 metadata to fail during enumeration.

**Pass condition:** enumeration fails explicitly as a backend failure rather
than silently omitting the affected stored state.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-013

### VAL-AZURE-TABLE-STORE-V2-016

Store a valid typed block through `put`, then inspect the Azure Table entity
keys and payload-property names used for the published state.

**Pass condition:** the implementation derives the deterministic
`PartitionKey`/`RowKey` pair from the returned block ID and stores the
canonical bytes in one entity using deterministic `chunkN` payload properties.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-004, REQ-AZURE-TABLE-STORE-V2-005,
REQ-AZURE-TABLE-STORE-V2-014, REQ-AZURE-TABLE-STORE-V2-015

### VAL-AZURE-TABLE-STORE-V2-017

Populate the mapped entity for a requested block ID with valid content, then
make the entity unreadable or otherwise inaccessible before calling `get`.

**Pass condition:** `get` fails explicitly as a backend failure and does not
report absence.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-006, REQ-AZURE-TABLE-STORE-V2-008

### VAL-AZURE-TABLE-STORE-V2-018

Attempt identifier enumeration through a v2 store constructed from a SAS URL
that lacks the permissions required to query or list the deterministic
entities.

**Pass condition:** identifier enumeration fails explicitly as a backend
failure, and construction itself was not required to reject the SAS URL
beforehand.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-003, REQ-AZURE-TABLE-STORE-V2-008

### VAL-AZURE-TABLE-STORE-V2-019

Cause a transient transport failure during `put` before Azure returns any
backend response, including both a case that later succeeds on retry and a case
that continues failing until the bounded retry policy is exhausted.

**Pass condition:** `put` retries the deterministic entity insert after the
transient transport failure, succeeds when a later retry reaches a successful
backend response, and otherwise fails explicitly as a backend failure.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-019

### VAL-AZURE-TABLE-STORE-V2-020

Cause a transient transport failure during `get` or table query before Azure
returns any backend response, including both a case that later succeeds on
retry and a case that continues failing until the bounded retry policy is
exhausted.

**Pass condition:** `get` and identifier enumeration retry the deterministic
entity read or table query after the transient transport failure, succeed when a
later retry reaches a successful backend response, retries a mid-stream paged
query without restarting from the beginning, and otherwise fails explicitly as
backend failures without claiming success or absence.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-020

### VAL-AZURE-TABLE-STORE-V2-021

Exercise the v2 single-block publish and single-block read paths against a
mock, probe, or live response surface that returns otherwise valid Azure Table
outcomes without non-decisive common storage headers such as `server`.

**Pass condition:** the operation outcome is interpreted correctly from the
available response status and payload information rather than failing solely due
to the absent non-decisive headers.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-021

### VAL-AZURE-TABLE-STORE-V2-022

Inspect the repository for a crate named `lexongraph-block-store-azure-table-v2`
that is separate from the existing Azure Table predecessor crate and the Azure
blob-backed crates and exposes a `BlockStore` implementation for the Azure
Table v2 backend.

**Pass condition:** the repository contains the dedicated successor crate with
that name, its workspace wiring keeps it distinct from
`lexongraph-block-store-azure-table`, `lexongraph-block-store-azure`, and
`lexongraph-block-store-azure-sdk`, and the successor crate is the
implementation home for the Azure Table v2 backend.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-002

### VAL-AZURE-TABLE-STORE-V2-023

Inspect the crate's integration-test surface for live Azure verification.

**Pass condition:** a dedicated live-test mode exists, it requires explicit
selection rather than running as part of the default workspace test path, and
it documents or enforces the live configuration needed to supply a real table
SAS URL.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-017

### VAL-AZURE-TABLE-STORE-V2-024

Run the dedicated live Azure verification mode against a fresh real Azure Table
Storage table using a valid table SAS URL, including a block large enough to
require multiple `chunkN` properties and a repeated publish of an already
present block.

**Pass condition:** the live verification succeeds after proving constructor
success, `put` publication, `get` round-trip retrieval, `get` absence handling,
block-ID enumeration, multi-chunk payload storage, and idempotent re-publish
success against the real backend.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-018

### VAL-AZURE-TABLE-STORE-V2-025

Inspect the repository's test-only verification surface for mock-backed Azure
Table v2 simulation.

**Pass condition:** the repository provides a mock-backed surface that can
observe constructor behavior, simulate publish/read/query outcomes, inject
malformed or integrity-mismatched recognized block entities in the v2 chunked
entity format, and simulate responses that omit non-decisive common storage
headers without broadening the production `BlockStore` API.

**Traces to:** REQ-AZURE-TABLE-STORE-V2-022
