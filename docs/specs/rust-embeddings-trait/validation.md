# Rust Embeddings Trait Validation

## Status

Draft validation specification for a Rust crate that defines the shared
LexonGraph embedding-provider contract.

## Validation Scope

These validation entries define the expected conformance surface for the shared
embedding-provider trait crate.

## Validation Entries

### VAL-EMBED-TRAIT-001

Inspect the crate's public surface.

**Pass condition:** the default public surface exposes the shared embedding
input type and embedding-provider trait contract without depending on the
indexer or search crates, and does not require any provider-specific model,
endpoint, deployment, or runtime contract.

**Traces to:** REQ-EMBED-TRAIT-001, REQ-EMBED-TRAIT-003, REQ-EMBED-TRAIT-006,
REQ-EMBED-TRAIT-010

### VAL-EMBED-TRAIT-002

Implement the shared embedding-provider trait with a fixture that performs
asynchronous work before returning valid embedding bytes.

**Pass condition:** the shared contract supports asynchronous provider
realization and returns bytes compatible with the requested `EmbeddingSpec`.

**Traces to:** REQ-EMBED-TRAIT-002, REQ-EMBED-TRAIT-004

### VAL-EMBED-TRAIT-003

Run the shared conformance harnesses against:

- a contract-satisfying provider fixture
- a provider fixture that fails explicitly
- a provider fixture that returns bytes incompatible with the requested
  `EmbeddingSpec`

**Pass condition:** the shared helpers accept the contract-satisfying fixture
and reject the invalid fixtures at the embedding-provider boundary.

**Traces to:** REQ-EMBED-TRAIT-004, REQ-EMBED-TRAIT-005, REQ-EMBED-TRAIT-007

### VAL-EMBED-TRAIT-004

Inspect the crate feature surface.

**Pass condition:** the conformance helpers are exposed only through an opt-in
non-default test-oriented surface and do not broaden the default
production-facing API.

**Traces to:** REQ-EMBED-TRAIT-007, REQ-EMBED-TRAIT-008

### VAL-EMBED-TRAIT-005

Inspect the repository verification artifacts for the embeddings-trait crate.

**Pass condition:** the repository includes executable automated tests that
realize the validation surface in this specification package.

**Traces to:** REQ-EMBED-TRAIT-009

### VAL-EMBED-TRAIT-006

Inspect repository embedding-related crates that consume the shared trait
surface.

**Pass condition:** embedding-consuming crates such as the indexer crate and
provider-specific embedding crates depend on the shared embeddings-trait crate
and do not define independent embedding-provider contracts.

**Traces to:** REQ-EMBED-TRAIT-010
