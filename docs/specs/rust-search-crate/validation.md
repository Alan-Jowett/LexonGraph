<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Search Crate Validation

## Status

Draft validation specification for a Rust crate that implements the LexonGraph
search protocol.

In this spec package, lowercase `w` and `n` name the Rust API parameters that
correspond to protocol-level `W` and `N` in `docs/protocol/search.md`.

## Validation Scope

These validation entries define the expected conformance surface for a crate
that implements the requirements and design in this spec package.

Protocol-level search invariants referenced here remain normatively defined by
`docs/protocol/search.md`. Block-validity and block-identity expectations remain
normatively defined by `docs/protocol/blocks.md` and the
`docs/specs/rust-block-crate/` specification package.

## Validation Entries

### VAL-SEARCH-001

Invoke search with one valid root block ID, one target embedding, one `w`, and
one `n`.

**Pass condition:** the search crate loads exactly one root block before any
child expansion and scores all entries in that root block.

**Traces to:** REQ-SEARCH-004, REQ-SEARCH-005, REQ-SEARCH-009

### VAL-SEARCH-002

Inspect the public search API boundary.

**Pass condition:** the API requires `root block ID`, `target embedding`, `w`,
and `n`, and it also requires access to a block store plus the required policy
trait implementations, whether those dependencies are supplied per invocation or
bound through searcher construction or configuration.

**Traces to:** REQ-SEARCH-001, REQ-SEARCH-004, REQ-SEARCH-005, REQ-SEARCH-007,
REQ-SEARCH-008

### VAL-SEARCH-003

Run search twice with the same root block ID, target embedding, `n`, `w`,
stored block set, and deterministic trait implementations.

**Pass condition:** both runs return the same ordered leaf results or the same
explicit failure.

**Traces to:** REQ-SEARCH-011

### VAL-SEARCH-004

Provide equal-embedding branch entries that point to different child block IDs.

**Pass condition:** those entries remain distinct branch candidates before
branch-child deduplication and only compete through the protocol-defined
ordering rules.

**Traces to:** REQ-SEARCH-009, REQ-SEARCH-010

### VAL-SEARCH-005

Provide differently embedded branch entries that point to the same child block
ID.

**Pass condition:** those entries are scored and ranked independently before
branch-child deduplication, and the highest-ranked occurrence determines whether
that child can be selected for expansion.

**Traces to:** REQ-SEARCH-009, REQ-SEARCH-010

### VAL-SEARCH-006

Provide equal embeddings in leaf entries that reside in different leaf blocks.

**Pass condition:** those entries remain distinct leaf candidates and may both
appear in the final ordered results.

**Traces to:** REQ-SEARCH-009, REQ-SEARCH-010

### VAL-SEARCH-007

Construct a search round where multiple ranked branch candidates refer to the
same child block ID and at least one such child falls within the effective top
`w` selection after deduplication.

**Pass condition:** that child block is expanded at most once in the round, and
selection is computed over unique child block IDs rather than raw branch-entry
occurrences.

**Traces to:** REQ-SEARCH-009, REQ-SEARCH-010

### VAL-SEARCH-008

Configure a compatibility trait that rejects a visited block's
`embedding_spec` for the target embedding.

**Pass condition:** search fails explicitly instead of silently skipping the
incompatible block.

**Traces to:** REQ-SEARCH-006, REQ-SEARCH-007, REQ-SEARCH-008

### VAL-SEARCH-009

Cause the block store to report absence or explicit failure for the root block
ID or for a selected child block ID.

**Pass condition:** search fails explicitly and does not report partial success
as though the missing block did not exist.

**Traces to:** REQ-SEARCH-004, REQ-SEARCH-006

### VAL-SEARCH-010

Cause the block crate to reject loaded bytes as malformed, non-conforming, or
invalid for the requested block ID.

**Pass condition:** search fails explicitly and does not continue traversal with
that block.

**Traces to:** REQ-SEARCH-003, REQ-SEARCH-006

### VAL-SEARCH-011

Supply scoring-trait implementations that use different similarity metrics but
remain deterministic within the same compatibility context.

**Pass condition:** the same consumer-facing search contract remains applicable
without changing the search crate's public API or the protocol-defined
tie-break behavior.

**Traces to:** REQ-SEARCH-007, REQ-SEARCH-008, REQ-SEARCH-012

### VAL-SEARCH-012

Run search over a graph where the top `n` ranked candidates are all leaves
before all reachable branch children have been expanded.

**Pass condition:** search terminates successfully at that point and returns the
top `n` leaves without expanding lower-ranked remaining branches.

**Traces to:** REQ-SEARCH-002, REQ-SEARCH-009

### VAL-SEARCH-013

Run search over a graph that cannot produce `n` reachable leaf candidates after
all expandable branch candidates have been exhausted.

**Pass condition:** search fails explicitly rather than returning fewer than `n`
results as success.

**Traces to:** REQ-SEARCH-006, REQ-SEARCH-009

### VAL-SEARCH-014

Inspect the crate's public surface.

**Pass condition:** the crate's default public surface exposes the runtime
search contract and related public types only, keeps implementer-facing
conformance helpers behind an opt-in non-default test-oriented surface, and
does not redefine block or block-store conformance surfaces.

**Traces to:** REQ-SEARCH-015, REQ-SEARCH-016, REQ-SEARCH-017

### VAL-SEARCH-015

Use the crate's opt-in conformance-test helper surface from a downstream crate
that implements one or more search-owned policy traits.

**Pass condition:** the downstream crate can depend on the helper surface in
tests and run the shared conformance checks without changing the default
production-facing API of the search crate.

**Traces to:** REQ-SEARCH-015, REQ-SEARCH-016

### VAL-SEARCH-016

Run the shared conformance harnesses against deterministic implementations of
the search-owned policy traits, including fixtures that intentionally violate
each trait's contract.

**Pass condition:** the shared helpers accept contract-satisfying
implementations, reject contract-violating implementations at the appropriate
trait boundary, detect nondeterministic implementations, verify repeated-input
stability for conforming implementations, verify preferred candidates outrank
lower-ranked alternate candidates for candidate scorers, expose
representative helper-owned expectation failures, and rely on the existing
block and block-store conformance surfaces rather than redefining them.

**Traces to:** REQ-SEARCH-007, REQ-SEARCH-008, REQ-SEARCH-015,
REQ-SEARCH-016, REQ-SEARCH-017

### VAL-SEARCH-017

Inspect the repository's Rust workspace and package artifacts for the search
crate.

**Pass condition:** the repository contains a Rust crate for the search
contract, and that crate is wired into the workspace as the implementation
artifact for this specification package.

**Traces to:** REQ-SEARCH-013

### VAL-SEARCH-018

Inspect the repository verification artifacts for the search crate.

**Pass condition:** the repository includes executable automated tests that
realize the validation surface in this specification package, including runtime
search behavior and the opt-in trait-conformance helper surface, including
representative direct checks of helper-owned expectation failures.

**Traces to:** REQ-SEARCH-014

### VAL-SEARCH-019

Construct ranked ties between:

- candidates from different numeric levels with equal scores
- expandable candidates with equal scores and different child block IDs
- leaf candidates with equal scores and different containing block IDs

**Pass condition:** search uses the canonical protocol ordering for those ties:
lower level before higher level, then ascending block ID within the candidate
shape at that level.

**Traces to:** REQ-SEARCH-002, REQ-SEARCH-009, REQ-SEARCH-010, REQ-SEARCH-011

### VAL-SEARCH-020

Run search over a multi-round graph where one or more leaf candidates are found
before lower-ranked branches are exhausted, but termination has not yet been
reached.

**Pass condition:** the previously found leaf candidates remain in the frontier
across later rounds and continue to compete in the final ranking until
termination.

**Traces to:** REQ-SEARCH-002, REQ-SEARCH-009, REQ-SEARCH-010, REQ-SEARCH-011

### VAL-SEARCH-021

Invoke search once with `w = 0`, and once with `n = 0`.

**Pass condition:** `w = 0` fails explicitly. `n = 0` succeeds with an empty
ordered result after the root block has still been loaded and its entries
scored, and without any child expansion.

**Traces to:** REQ-SEARCH-005, REQ-SEARCH-006, REQ-SEARCH-018

### VAL-SEARCH-022

Inspect the crate's default runtime API surface.

**Pass condition:** the crate exposes a public encoded target-embedding
representation plus public default implementations of `EmbeddingCompatibility`
and `CandidateScorer`, while still allowing callers to supply their own policy
implementations through the existing trait-based search API.

**Traces to:** REQ-SEARCH-007, REQ-SEARCH-008, REQ-SEARCH-019, REQ-SEARCH-020,
REQ-SEARCH-021

### VAL-SEARCH-023

Run the crate-provided default compatibility policy against visited blocks whose
logical comparison representations alternately match and differ from the target
embedding's specification, including EBCP-encoded non-leaf blocks whose
ambient-space logical encoding either matches or differs from the target.

**Pass condition:** matching logical encoding and dimensionality are accepted;
mismatched logical encoding or dimensionality is rejected explicitly.

**Traces to:** REQ-SEARCH-006, REQ-SEARCH-019, REQ-SEARCH-021

### VAL-SEARCH-024

Run the crate-provided default scorer with compatible embeddings, then with one
or more unsupported encodings, target or candidate byte sequences whose lengths
are inconsistent with the applicable embedding specification, zero-magnitude
embeddings, non-finite encoded floating-point values, inputs whose cosine
computation yields a non-finite result, or embedding specifications whose
dimensionality is too large to validate safely.

**Pass condition:** compatible inputs produce a deterministic cosine-based score
with a total ordering compatible with search ranking, and unsupported encodings
or inconsistent byte lengths fail explicitly rather than producing arbitrary
scores. Zero-magnitude embeddings, non-finite encoded values, and dimension
overflow also fail explicitly across the supported `f32le` and `f64le`
decoding paths. If cosine computation over otherwise-supported inputs becomes
non-finite, the scorer fails explicitly instead of returning a rankable score.

**Traces to:** REQ-SEARCH-012, REQ-SEARCH-020, REQ-SEARCH-021

### VAL-SEARCH-025

Invoke search with a scorer that rejects one or more candidates in a visited
leaf block and in a visited branch block.

**Pass condition:** search fails explicitly with a scoring failure for candidates
from both terminal and expandable levels rather than silently skipping those candidates or reporting
partial success.

**Traces to:** REQ-SEARCH-006

### VAL-SEARCH-026

Run search over a multi-round graph where a later-expanded block contains a
branch candidate that points to a child block that was already expanded in an
earlier round.

**Pass condition:** the already-expanded child block is not selected for
expansion again within the same invocation, even if its later branch candidate
outranks other available branches in that round.

**Traces to:** REQ-SEARCH-009, REQ-SEARCH-022

### VAL-SEARCH-027

Run search over a graph where one round expands a child block that is
represented by multiple branch candidates in the current frontier, then
continues into a later round.

**Pass condition:** all expandable candidates targeting any child block already
expanded in the invocation are removed before the next ranking round so stale
branch entries do not affect termination or later expansion choices.

**Traces to:** REQ-SEARCH-009, REQ-SEARCH-023

### VAL-SEARCH-028

Run search through the optional telemetry-returning surface on a deterministic
multi-round fixture.

**Pass condition:** the crate reports the invocation beam width, distinct blocks
visited, maximum routing depth, and successful termination classification in a
deterministic telemetry summary without changing the ordered search result.

**Traces to:** REQ-SEARCH-024

### VAL-SEARCH-029

Run the same deterministic search fixture through both the result-only surface
and the optional telemetry surface.

**Pass condition:** both invocations return the same ordered search result and
observable failure behavior; telemetry only adds the declared summary surface.

**Traces to:** REQ-SEARCH-024

### VAL-SEARCH-030

Run search through the observer-based telemetry surface on a fixture that fails
explicitly after the invocation starts.

**Pass condition:** the observer receives the same terminal outcome
classification and routing summary the returned telemetry surface would report,
including explicit failure termination rather than silent omission.

**Traces to:** REQ-SEARCH-024

### VAL-SEARCH-031

Resolve published search profile `0.1.0` through the crate's convenience
surface.

**Pass condition:** the crate exposes a published profile version selector, the
selected `0.1.0` profile resolves successfully, and the convenience surface
binds the crate-owned encoded target representation plus the default
compatibility and scoring policies.

**Traces to:** REQ-SEARCH-027, REQ-SEARCH-030

### VAL-SEARCH-032

Attempt to resolve an unknown published search profile version.

**Pass condition:** the crate fails explicitly and does not silently substitute
another published profile.

**Traces to:** REQ-SEARCH-028

### VAL-SEARCH-033

Run the same deterministic search fixture once through the low-level default
policy bundle and once through published search profile `0.1.0`.

**Pass condition:** both paths produce the same deterministic search behavior,
and the convenience path still requires explicit `w` and `n`.

**Traces to:** REQ-SEARCH-029, REQ-SEARCH-030, REQ-SEARCH-031, REQ-SEARCH-032

### VAL-SEARCH-034

Run search over a deterministic fixture whose visited non-leaf blocks use the
`pca-rot-f32le` EBCP encoding.

**Pass condition:** search succeeds through the existing runtime surface,
interprets the EBCP branch payloads according to `docs/protocol/ebcp.md`, and
preserves the same ordered leaf result as the logically equivalent
uncompressed-branch fixture.

**Traces to:** REQ-SEARCH-033, REQ-SEARCH-034, REQ-SEARCH-035, REQ-SEARCH-036

### VAL-SEARCH-035

Run the same deterministic fixture with EBCP branch blocks using
`pca-rot-delta-f32le`.

**Pass condition:** search succeeds and preserves the same ordered leaf result
as the logically equivalent uncompressed-branch fixture.

**Traces to:** REQ-SEARCH-033, REQ-SEARCH-034, REQ-SEARCH-035, REQ-SEARCH-036

### VAL-SEARCH-036

Run search over a deterministic fixture whose visited non-leaf blocks use
`pca-rot-delta-uq`.

**Pass condition:** search succeeds through the existing API shape, continues to
apply the protocol-defined traversal and termination rules, and any observed
difference from the logically equivalent uncompressed-branch fixture is
attributable only to the lossy branch-vector approximation.

**Traces to:** REQ-SEARCH-033, REQ-SEARCH-034, REQ-SEARCH-035, REQ-SEARCH-037

### VAL-SEARCH-037

Run search over a deterministic fixture whose visited non-leaf blocks use
`pca-rot-delta-vbq`.

**Pass condition:** search succeeds through the existing API shape, continues to
apply the protocol-defined traversal and termination rules, and any observed
difference from the logically equivalent uncompressed-branch fixture is
attributable only to the lossy branch-vector approximation.

**Traces to:** REQ-SEARCH-033, REQ-SEARCH-034, REQ-SEARCH-035, REQ-SEARCH-037

### VAL-SEARCH-037b

Run search over a deterministic fixture whose visited non-leaf blocks use
`ambient-delta-uq`.

**Pass condition:** search succeeds through the existing API shape, continues to
apply the protocol-defined traversal and termination rules, and any observed
difference from the logically equivalent uncompressed-branch fixture is
attributable only to the lossy branch-vector approximation.

**Traces to:** REQ-SEARCH-033, REQ-SEARCH-034, REQ-SEARCH-035, REQ-SEARCH-037

### VAL-SEARCH-038

Attempt search over a fixture containing a block that violates the EBCP branch
encoding contract, such as an EBCP encoding on a leaf block or missing EBCP
metadata on a non-leaf block.

**Pass condition:** search fails explicitly through the existing invalid-block
path rather than silently treating the malformed payload as an ordinary
uncompressed branch embedding.

**Traces to:** REQ-SEARCH-006, REQ-SEARCH-033, REQ-SEARCH-035

### VAL-SEARCH-039

For supported EBCP branch encodings, compare the public logical-branch
reconstruction results from `lexongraph-block` against the branch ordering
observed through search on the same deterministic fixtures.

**Pass condition:** the logical vectors reconstructed through the block crate's
public surface induce the same winning branch choice that search uses through
its existing runtime surface.

**Traces to:** REQ-SEARCH-034, REQ-SEARCH-038
