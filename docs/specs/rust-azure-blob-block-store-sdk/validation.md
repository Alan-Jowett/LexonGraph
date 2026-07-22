<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Azure SDK Blob Block Store Validation

## Status

Draft validation specification for `lexongraph-block-store-azure-sdk`.

## Validation Scope

These validation entries define the expected conformance surface for the
SDK-backed Azure Blob backend in addition to the parent block-store trait
validation surface.

## Validation Entries

### VAL-AZURE-SDK-STORE-001

Inspect the SDK-backed crate boundary and construct the store with a valid
container-root SAS URL.

**Pass condition:** the crate uses the official Azure Rust SDK client surface
for container-root construction and blob operations, construction succeeds, and
callers do not need to know the deterministic blob-name layout.

**Traces to:** REQ-AZURE-SDK-STORE-002, REQ-AZURE-SDK-STORE-003,
REQ-AZURE-SDK-STORE-004

### VAL-AZURE-SDK-STORE-002

Attempt to construct the store with malformed URLs, blob-scoped URLs, or URLs
without a non-empty SAS signature parameter.

**Pass condition:** construction fails explicitly as a backend failure.

**Traces to:** REQ-AZURE-SDK-STORE-004

### VAL-AZURE-SDK-STORE-003

Store a valid typed block through `put`, retrieve it through `get`, and request
an unmapped block ID.

**Pass condition:** round-trip retrieval succeeds for the stored block and
`get` returns `Ok(None)` for the unmapped block ID.

**Traces to:** REQ-AZURE-SDK-STORE-005, REQ-AZURE-SDK-STORE-006

### VAL-AZURE-SDK-STORE-004

Store the same logical block multiple times, including cases where the
deterministic blob already exists and where multiple writers race on the same
block.

**Pass condition:** successful calls return the same block ID, `409` and `412`
conflicts are accepted as success, and concurrent writers converge on one valid
published blob.

**Traces to:** REQ-AZURE-SDK-STORE-007, REQ-AZURE-SDK-STORE-009

### VAL-AZURE-SDK-STORE-005

Populate the mapped blob for a requested block ID with malformed bytes,
integrity-mismatched bytes, and inaccessible bytes.

**Pass condition:** `get` fails explicitly with malformed-content,
integrity-mismatch, or backend-failure outcomes as appropriate.

**Traces to:** REQ-AZURE-SDK-STORE-006, REQ-AZURE-SDK-STORE-008

### VAL-AZURE-SDK-STORE-006

Cause transient publish transport failures before Azure returns a successful
backend response, including a case that later succeeds on retry and a case that
continues until retry exhaustion.

**Pass condition:** `put` retries through the SDK retry policy, succeeds when a
later retry reaches a backend response, and otherwise fails explicitly as a
backend failure after retry exhaustion.

**Traces to:** REQ-AZURE-SDK-STORE-007, REQ-AZURE-SDK-STORE-012

### VAL-AZURE-SDK-STORE-007

Cause transient read and list transport failures before Azure returns a
successful backend response, including both eventual-success and retry-exhausted
cases.

**Pass condition:** `get` and enumeration retry through the SDK retry policy,
succeed when a later retry reaches a backend response, and otherwise fail
explicitly as backend failures after retry exhaustion.

**Traces to:** REQ-AZURE-SDK-STORE-006, REQ-AZURE-SDK-STORE-010,
REQ-AZURE-SDK-STORE-012

### VAL-AZURE-SDK-STORE-008

Populate the container with recognized block blobs, unrelated blobs, and
malformed recognized block-blob candidates.

**Pass condition:** enumeration yields only recognized block IDs, ignores
unrelated content, and fails explicitly on malformed recognized candidates.

**Traces to:** REQ-AZURE-SDK-STORE-010, REQ-AZURE-SDK-STORE-011

### VAL-AZURE-SDK-STORE-009

Run the parent block-store conformance suite against the SDK-backed
implementation.

**Pass condition:** the backend satisfies the shared `BlockStore` contract
without changing the parent trait API.

**Traces to:** REQ-AZURE-SDK-STORE-001, REQ-AZURE-SDK-STORE-002

### VAL-AZURE-SDK-STORE-010

Inspect the crate's live integration-test surface.

**Pass condition:** a dedicated ignored live test exists, requires an explicit
container SAS URL configuration, and remains outside the default workspace test
path.

**Traces to:** REQ-AZURE-SDK-STORE-013

### VAL-AZURE-SDK-STORE-011

Run the ignored live Azure test against a fresh real Azure container provisioned
for CI.

**Pass condition:** the live test proves constructor success, `put`
publication, `get` round-trip retrieval, `get` absence handling, and
enumeration against the real Azure backend, and the workflow cleans up the
temporary Azure resources afterward.

**Traces to:** REQ-AZURE-SDK-STORE-013, REQ-AZURE-SDK-STORE-014
