// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Protocol-conforming LexonGraph search orchestration.
//!
//! ```
//! use lexongraph_block::BlockHash;
//! use lexongraph_block_store::BlockStore;
//! use lexongraph_search::{
//!     CandidateScorer, EmbeddingCompatibility, SearchError, SearchResult, Searcher,
//! };
//!
//! async fn search_one<Target, EC, CS>(
//!     searcher: &Searcher<EC, CS>,
//!     root_id: &BlockHash,
//!     target: &Target,
//!     store: &dyn BlockStore,
//! ) -> Result<SearchResult, SearchError>
//! where
//!     EC: EmbeddingCompatibility<Target>,
//!     CS: CandidateScorer<Target>,
//! {
//!     searcher.search(root_id, target, 1, 1, store).await
//! }
//! ```
//!
//! ```compile_fail
//! #[cfg(feature = "conformance")]
//! compile_error!("the conformance module is intentionally enabled in this doctest configuration");
//!
//! use lexongraph_search::conformance;
//!
//! let _ = std::any::type_name::<conformance::ConformanceError>();
//! ```

use std::cmp::Ordering;
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;

use futures::future;
pub use lexongraph_block::{BlockHash, EmbeddingSpec, LeafEntry};

use lexongraph_block::{
    TypedEntries, ValidatedBlock, into_entries, parse_branch_ebcp_descriptor,
    reconstruct_logical_branch_embedding_f32,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};

const MAX_CONCURRENT_CHILD_LOADS: usize = 32;

pub trait EmbeddingCompatibility<Target> {
    type Error: std::error::Error;

    fn ensure_compatible(
        &self,
        target: &Target,
        embedding_spec: &EmbeddingSpec,
    ) -> Result<(), Self::Error>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedTargetEmbedding {
    pub bytes: Vec<u8>,
    pub embedding_spec: EmbeddingSpec,
}

impl EncodedTargetEmbedding {
    pub fn new(bytes: Vec<u8>, embedding_spec: EmbeddingSpec) -> Self {
        Self {
            bytes,
            embedding_spec,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DefaultEmbeddingCompatibility;

pub trait CandidateScorer<Target> {
    type Error: std::error::Error;
    type Score: Ord;

    /// Higher scores rank ahead of lower scores.
    fn score(
        &self,
        target: &Target,
        candidate_embedding: &[u8],
        embedding_spec: &EmbeddingSpec,
    ) -> Result<Self::Score, Self::Error>;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DefaultCandidateScorer;

#[derive(Clone, Copy, Debug)]
pub struct ExpandableFrontierCandidate<'a, Score> {
    pub child: BlockHash,
    pub depth: usize,
    pub level: u64,
    pub score: &'a Score,
    pub embedding: &'a [u8],
    pub embedding_spec: &'a EmbeddingSpec,
}

pub trait FrontierSelector<Score> {
    type Error: std::error::Error;

    fn select(
        &self,
        frontier: &[ExpandableFrontierCandidate<'_, Score>],
        w: usize,
    ) -> Result<Vec<BlockHash>, Self::Error>;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DefaultTopWFrontierSelector;

impl<Score> FrontierSelector<Score> for DefaultTopWFrontierSelector {
    type Error = Infallible;

    fn select(
        &self,
        frontier: &[ExpandableFrontierCandidate<'_, Score>],
        w: usize,
    ) -> Result<Vec<BlockHash>, Self::Error> {
        Ok(frontier
            .iter()
            .take(w)
            .map(|candidate| candidate.child)
            .collect())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GeometryAwareFrontierSelector;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GeometryAwareFrontierSelectionError {
    UnsupportedEncoding {
        encoding: String,
    },
    InvalidByteLength {
        child: BlockHash,
        encoding: String,
        dims: u64,
        expected: usize,
        actual: usize,
    },
    DimensionOverflow {
        child: BlockHash,
        encoding: String,
        dims: u64,
    },
    ZeroMagnitude {
        child: BlockHash,
    },
    NonFiniteValue {
        child: BlockHash,
        index: usize,
    },
}

impl fmt::Display for GeometryAwareFrontierSelectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedEncoding { encoding } => {
                write!(
                    f,
                    "geometry-aware frontier selection does not support encoding {encoding}"
                )
            }
            Self::InvalidByteLength {
                child,
                encoding,
                dims,
                expected,
                actual,
            } => write!(
                f,
                "child block {child} embedding length {actual} does not match encoding {encoding} with {dims} dims (expected {expected} bytes)"
            ),
            Self::DimensionOverflow {
                child,
                encoding,
                dims,
            } => write!(
                f,
                "child block {child} embedding spec with encoding {encoding} and {dims} dims is too large to validate"
            ),
            Self::ZeroMagnitude { child } => {
                write!(
                    f,
                    "child block {child} embedding must not have zero magnitude"
                )
            }
            Self::NonFiniteValue { child, index } => write!(
                f,
                "child block {child} embedding contains a non-finite value at index {index}"
            ),
        }
    }
}

impl std::error::Error for GeometryAwareFrontierSelectionError {}

impl<Score> FrontierSelector<Score> for GeometryAwareFrontierSelector {
    type Error = GeometryAwareFrontierSelectionError;

    fn select(
        &self,
        frontier: &[ExpandableFrontierCandidate<'_, Score>],
        w: usize,
    ) -> Result<Vec<BlockHash>, Self::Error> {
        if frontier.is_empty() || w == 0 {
            return Ok(Vec::new());
        }

        let window_len = frontier.len().min(w.saturating_mul(2).max(w));
        let window = &frontier[..window_len];
        let vectors = window
            .iter()
            .map(|candidate| decode_geometry_vector(candidate))
            .collect::<Result<Vec<_>, _>>()?;
        let mut selected = vec![0usize];
        let mut remaining = (1..window.len()).collect::<Vec<_>>();

        while selected.len() < window.len().min(w) && !remaining.is_empty() {
            let mut best_index = remaining[0];
            let mut best_distance = min_cosine_distance(&vectors[best_index], &selected, &vectors);

            for &candidate_index in &remaining[1..] {
                let distance = min_cosine_distance(&vectors[candidate_index], &selected, &vectors);
                if distance.total_cmp(&best_distance).is_gt() {
                    best_distance = distance;
                    best_index = candidate_index;
                }
            }

            selected.push(best_index);
            remaining.retain(|index| *index != best_index);
        }

        Ok(selected
            .into_iter()
            .map(|index| window[index].child)
            .collect())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishedDefaultFrontierSelector {
    TopW,
    GeometryAware,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublishedProfileVersion {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

impl PublishedProfileVersion {
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

impl fmt::Display for PublishedProfileVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

pub const PUBLISHED_PROFILE_V0_1_0: PublishedProfileVersion = PublishedProfileVersion::new(0, 1, 0);
pub const PUBLISHED_PROFILE_V0_2_0: PublishedProfileVersion = PublishedProfileVersion::new(0, 2, 0);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishedSearchProfile {
    version: PublishedProfileVersion,
}

impl PublishedSearchProfile {
    pub fn version(&self) -> PublishedProfileVersion {
        self.version
    }

    pub fn encode_target(
        &self,
        bytes: Vec<u8>,
        embedding_spec: EmbeddingSpec,
    ) -> EncodedTargetEmbedding {
        EncodedTargetEmbedding::new(bytes, embedding_spec)
    }

    pub fn searcher(&self) -> Searcher<DefaultEmbeddingCompatibility, DefaultCandidateScorer> {
        Searcher::with_frontier_selector(
            DefaultEmbeddingCompatibility,
            DefaultCandidateScorer,
            match self.version {
                PUBLISHED_PROFILE_V0_1_0 => PublishedDefaultFrontierSelector::TopW,
                PUBLISHED_PROFILE_V0_2_0 => PublishedDefaultFrontierSelector::GeometryAware,
                _ => unreachable!("published profiles are validated before construction"),
            },
        )
    }
}

pub fn published_search_profile(
    version: PublishedProfileVersion,
) -> Result<PublishedSearchProfile, SearchProfileError> {
    match version {
        PUBLISHED_PROFILE_V0_1_0 | PUBLISHED_PROFILE_V0_2_0 => {
            Ok(PublishedSearchProfile { version })
        }
        _ => Err(SearchProfileError::UnsupportedPublishedProfileVersion(
            version,
        )),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchProfileError {
    UnsupportedPublishedProfileVersion(PublishedProfileVersion),
}

impl fmt::Display for SearchProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPublishedProfileVersion(version) => {
                write!(f, "unsupported published search profile version {version}")
            }
        }
    }
}

impl std::error::Error for SearchProfileError {}

#[derive(Clone, Debug)]
pub struct ProfiledSearcher {
    profile: PublishedSearchProfile,
    inner: Searcher<DefaultEmbeddingCompatibility, DefaultCandidateScorer>,
}

impl ProfiledSearcher {
    pub fn new(profile_version: PublishedProfileVersion) -> Result<Self, SearchProfileError> {
        let profile = published_search_profile(profile_version)?;
        let inner = profile.searcher();
        Ok(Self { profile, inner })
    }

    pub fn profile(&self) -> PublishedSearchProfile {
        self.profile
    }

    pub async fn search(
        &self,
        root_id: &BlockHash,
        target_bytes: Vec<u8>,
        embedding_spec: EmbeddingSpec,
        w: usize,
        n: usize,
        store: &dyn BlockStore,
    ) -> Result<SearchResult, SearchError> {
        let target = self.profile.encode_target(target_bytes, embedding_spec);
        self.inner.search(root_id, &target, w, n, store).await
    }

    pub async fn search_with_telemetry(
        &self,
        root_id: &BlockHash,
        target_bytes: Vec<u8>,
        embedding_spec: EmbeddingSpec,
        w: usize,
        n: usize,
        store: &dyn BlockStore,
    ) -> Result<(SearchResult, SearchTelemetrySummary), SearchError> {
        let target = self.profile.encode_target(target_bytes, embedding_spec);
        self.inner
            .search_with_telemetry(root_id, &target, w, n, store)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search_with_observer<TO>(
        &self,
        root_id: &BlockHash,
        target_bytes: Vec<u8>,
        embedding_spec: EmbeddingSpec,
        w: usize,
        n: usize,
        store: &dyn BlockStore,
        observer: &TO,
    ) -> Result<SearchResult, SearchError>
    where
        TO: SearchTelemetryObserver,
    {
        let target = self.profile.encode_target(target_bytes, embedding_spec);
        self.inner
            .search_with_observer(root_id, &target, w, n, store, observer)
            .await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CosineScore(u64);

impl CosineScore {
    fn from_f64(value: f64) -> Result<Self, DefaultPolicyError> {
        if !value.is_finite() {
            return Err(DefaultPolicyError::NonFiniteScore);
        }
        Ok(Self(total_order_key_f64(value)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DefaultPolicyError {
    IncompatibleEmbeddingSpec {
        target: EmbeddingSpec,
        candidate: EmbeddingSpec,
    },
    UnsupportedEncoding {
        encoding: String,
    },
    InvalidByteLength {
        role: &'static str,
        encoding: String,
        dims: u64,
        expected: usize,
        actual: usize,
    },
    DimensionOverflow {
        encoding: String,
        dims: u64,
    },
    ZeroMagnitude {
        role: &'static str,
    },
    NonFiniteValue {
        role: &'static str,
        index: usize,
    },
    NonFiniteScore,
}

impl fmt::Display for DefaultPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompatibleEmbeddingSpec { target, candidate } => write!(
                f,
                "target embedding spec ({}, {} dims) does not match candidate embedding spec ({}, {} dims)",
                target.encoding, target.dims, candidate.encoding, candidate.dims
            ),
            Self::UnsupportedEncoding { encoding } => {
                write!(f, "unsupported embedding encoding {encoding}")
            }
            Self::InvalidByteLength {
                role,
                encoding,
                dims,
                expected,
                actual,
            } => write!(
                f,
                "{role} embedding length {actual} does not match encoding {encoding} with {dims} dims (expected {expected} bytes)"
            ),
            Self::DimensionOverflow { encoding, dims } => write!(
                f,
                "embedding spec with encoding {encoding} and {dims} dims is too large to validate"
            ),
            Self::ZeroMagnitude { role } => {
                write!(f, "{role} embedding must not have zero magnitude")
            }
            Self::NonFiniteValue { role, index } => {
                write!(
                    f,
                    "{role} embedding contains a non-finite value at index {index}"
                )
            }
            Self::NonFiniteScore => write!(f, "cosine similarity produced a non-finite score"),
        }
    }
}

impl std::error::Error for DefaultPolicyError {}

impl EmbeddingCompatibility<EncodedTargetEmbedding> for DefaultEmbeddingCompatibility {
    type Error = DefaultPolicyError;

    fn ensure_compatible(
        &self,
        target: &EncodedTargetEmbedding,
        embedding_spec: &EmbeddingSpec,
    ) -> Result<(), Self::Error> {
        ensure_matching_specs(&target.embedding_spec, embedding_spec)
    }
}

impl CandidateScorer<EncodedTargetEmbedding> for DefaultCandidateScorer {
    type Error = DefaultPolicyError;
    type Score = CosineScore;

    fn score(
        &self,
        target: &EncodedTargetEmbedding,
        candidate_embedding: &[u8],
        embedding_spec: &EmbeddingSpec,
    ) -> Result<Self::Score, Self::Error> {
        ensure_matching_specs(&target.embedding_spec, embedding_spec)?;
        cosine_similarity_bytes(&target.bytes, candidate_embedding, embedding_spec)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeafSearchResult {
    pub leaf_block_id: BlockHash,
    pub entry: LeafEntry,
    /// Zero-based rank in the returned result set.
    pub position: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchResult {
    pub leaves: Vec<LeafSearchResult>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchTerminationKind {
    Success,
    Exhausted,
    InvalidTraversalWidth,
    MissingRootBlock,
    RootLoadFailure,
    MissingChildBlock,
    ChildLoadFailure,
    MalformedBlock,
    IncompatibleEmbedding,
    ScoringFailure,
    FrontierSelectionFailure,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchTelemetrySummary {
    pub beam_width: usize,
    pub distinct_blocks_visited: usize,
    pub max_routing_depth: usize,
    pub termination: SearchTerminationKind,
}

pub trait SearchTelemetryObserver {
    fn record_summary(&self, summary: &SearchTelemetrySummary);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchError {
    InvalidTraversalWidth {
        w: usize,
    },
    MissingRootBlock {
        root_id: BlockHash,
    },
    RootLoad(BlockStoreError),
    MissingChildBlock {
        child_id: BlockHash,
    },
    ChildLoad {
        child_id: BlockHash,
        error: BlockStoreError,
    },
    MalformedBlock {
        block_id: BlockHash,
        error: BlockStoreError,
    },
    IncompatibleEmbedding {
        block_id: BlockHash,
        message: String,
    },
    ScoringFailure {
        block_id: BlockHash,
        message: String,
    },
    FrontierSelectionFailure {
        message: String,
    },
    Exhausted {
        requested: usize,
        reachable_leaves: usize,
    },
}

impl fmt::Display for SearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTraversalWidth { w } => {
                write!(f, "search traversal width must be at least 1, got {w}")
            }
            Self::MissingRootBlock { root_id } => {
                write!(f, "root block {root_id} was not present in the block store")
            }
            Self::RootLoad(error) => write!(f, "failed to load root block: {error}"),
            Self::MissingChildBlock { child_id } => {
                write!(
                    f,
                    "selected child block {child_id} was not present in the block store"
                )
            }
            Self::ChildLoad { child_id, error } => {
                write!(f, "failed to load child block {child_id}: {error}")
            }
            Self::MalformedBlock { block_id, error } => {
                write!(
                    f,
                    "block {block_id} was malformed or non-conforming: {error}"
                )
            }
            Self::IncompatibleEmbedding { block_id, message } => {
                write!(
                    f,
                    "block {block_id} is incompatible with the target embedding: {message}"
                )
            }
            Self::ScoringFailure { block_id, message } => {
                write!(
                    f,
                    "failed to score candidates from block {block_id}: {message}"
                )
            }
            Self::FrontierSelectionFailure { message } => {
                write!(f, "failed to select frontier expansion targets: {message}")
            }
            Self::Exhausted {
                requested,
                reachable_leaves,
            } => write!(
                f,
                "search exhausted after finding {reachable_leaves} reachable leaves, fewer than requested {requested}"
            ),
        }
    }
}

impl std::error::Error for SearchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RootLoad(error)
            | Self::ChildLoad { error, .. }
            | Self::MalformedBlock { error, .. } => Some(error),
            Self::InvalidTraversalWidth { .. }
            | Self::MissingRootBlock { .. }
            | Self::MissingChildBlock { .. }
            | Self::IncompatibleEmbedding { .. }
            | Self::ScoringFailure { .. }
            | Self::FrontierSelectionFailure { .. }
            | Self::Exhausted { .. } => None,
        }
    }
}

impl<Score> FrontierSelector<Score> for PublishedDefaultFrontierSelector {
    type Error = GeometryAwareFrontierSelectionError;

    fn select(
        &self,
        frontier: &[ExpandableFrontierCandidate<'_, Score>],
        w: usize,
    ) -> Result<Vec<BlockHash>, Self::Error> {
        match self {
            Self::TopW => Ok(frontier
                .iter()
                .take(w)
                .map(|candidate| candidate.child)
                .collect()),
            Self::GeometryAware => GeometryAwareFrontierSelector.select(frontier, w),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Searcher<EC, CS, FS = PublishedDefaultFrontierSelector> {
    compatibility: EC,
    scorer: CS,
    frontier_selector: FS,
}

impl<EC, CS> Searcher<EC, CS> {
    pub fn new(compatibility: EC, scorer: CS) -> Self {
        Self::with_frontier_selector(
            compatibility,
            scorer,
            PublishedDefaultFrontierSelector::TopW,
        )
    }
}

impl<EC, CS, FS> Searcher<EC, CS, FS> {
    pub fn with_frontier_selector(compatibility: EC, scorer: CS, frontier_selector: FS) -> Self {
        Self {
            compatibility,
            scorer,
            frontier_selector,
        }
    }
}

impl<EC, CS, FS> Searcher<EC, CS, FS> {
    pub async fn search<Target>(
        &self,
        root_id: &BlockHash,
        target: &Target,
        w: usize,
        n: usize,
        store: &dyn BlockStore,
    ) -> Result<SearchResult, SearchError>
    where
        EC: EmbeddingCompatibility<Target>,
        CS: CandidateScorer<Target>,
        FS: FrontierSelector<CS::Score>,
    {
        self.search_internal(root_id, target, w, n, store, TelemetryMode::Disabled)
            .await
            .map(|(result, _)| result)
    }

    pub async fn search_with_telemetry<Target>(
        &self,
        root_id: &BlockHash,
        target: &Target,
        w: usize,
        n: usize,
        store: &dyn BlockStore,
    ) -> Result<(SearchResult, SearchTelemetrySummary), SearchError>
    where
        EC: EmbeddingCompatibility<Target>,
        CS: CandidateScorer<Target>,
        FS: FrontierSelector<CS::Score>,
    {
        self.search_internal(root_id, target, w, n, store, TelemetryMode::Enabled(None))
            .await
    }

    pub async fn search_with_observer<Target, TO>(
        &self,
        root_id: &BlockHash,
        target: &Target,
        w: usize,
        n: usize,
        store: &dyn BlockStore,
        observer: &TO,
    ) -> Result<SearchResult, SearchError>
    where
        EC: EmbeddingCompatibility<Target>,
        CS: CandidateScorer<Target>,
        FS: FrontierSelector<CS::Score>,
        TO: SearchTelemetryObserver,
    {
        self.search_internal(
            root_id,
            target,
            w,
            n,
            store,
            TelemetryMode::Enabled(Some(observer)),
        )
        .await
        .map(|(result, _)| result)
    }

    async fn search_internal<Target>(
        &self,
        root_id: &BlockHash,
        target: &Target,
        w: usize,
        n: usize,
        store: &dyn BlockStore,
        telemetry_mode: TelemetryMode<'_>,
    ) -> Result<(SearchResult, SearchTelemetrySummary), SearchError>
    where
        EC: EmbeddingCompatibility<Target>,
        CS: CandidateScorer<Target>,
        FS: FrontierSelector<CS::Score>,
    {
        let mut telemetry = SearchTelemetryCollector::new(w, telemetry_mode.enabled());

        if w == 0 {
            let error = SearchError::InvalidTraversalWidth { w };
            telemetry.finish_with_error(&error);
            emit_search_telemetry(telemetry_mode.observer(), telemetry.summary());
            return Err(error);
        }

        let mut frontier = match self
            .load_block_candidates(root_id, target, store, true, 0, &mut telemetry)
            .await
        {
            Ok(frontier) => frontier,
            Err(error) => {
                telemetry.finish_with_error(&error);
                emit_search_telemetry(telemetry_mode.observer(), telemetry.summary());
                return Err(error);
            }
        };
        let mut expanded_children = HashSet::new();

        loop {
            frontier.retain(|candidate| {
                !matches!(
                    candidate,
                    SearchCandidate::Branch { child, .. } if expanded_children.contains(child)
                )
            });
            frontier.sort_by(compare_candidates::<CS::Score>);

            if frontier.len() >= n && frontier.iter().take(n).all(SearchCandidate::is_terminal) {
                let leaves = frontier
                    .into_iter()
                    .take(n)
                    .enumerate()
                    .map(|(position, candidate)| match candidate {
                        SearchCandidate::Leaf {
                            block_id, entry, ..
                        } => LeafSearchResult {
                            leaf_block_id: block_id,
                            entry,
                            position,
                        },
                        SearchCandidate::Branch { .. } => {
                            unreachable!("termination requires the top n candidates to be leaves")
                        }
                    })
                    .collect();
                telemetry.finish_success();
                emit_search_telemetry(telemetry_mode.observer(), telemetry.summary());
                return Ok((SearchResult { leaves }, telemetry.into_summary()));
            }

            let expandable_frontier =
                deduplicated_expandable_frontier(&frontier, &expanded_children);
            let current_round = match self
                .frontier_selector
                .select(&expandable_frontier, w)
                .map_err(|error| SearchError::FrontierSelectionFailure {
                    message: error.to_string(),
                })
                .and_then(|selected| validate_selected_children(&selected, &expandable_frontier, w))
            {
                Ok(selected) => selected,
                Err(error) => {
                    telemetry.finish_with_error(&error);
                    emit_search_telemetry(telemetry_mode.observer(), telemetry.summary());
                    return Err(error);
                }
            };
            if current_round.is_empty() {
                let error = SearchError::Exhausted {
                    requested: n,
                    reachable_leaves: frontier
                        .iter()
                        .filter(|candidate| candidate.is_terminal())
                        .count(),
                };
                telemetry.finish_with_error(&error);
                emit_search_telemetry(telemetry_mode.observer(), telemetry.summary());
                return Err(error);
            }
            let current_round_set: HashSet<_> = current_round.iter().copied().collect();
            let mut next_candidates = Vec::new();
            let current_round_depths = current_round
                .iter()
                .map(|child_id| {
                    let child_depth = frontier
                        .iter()
                        .find_map(|candidate| match candidate {
                            SearchCandidate::Branch { child, depth, .. } if child == child_id => {
                                Some(*depth)
                            }
                            _ => None,
                        })
                        .unwrap_or(1);
                    (*child_id, child_depth)
                })
                .collect::<Vec<_>>();
            let child_load_limit = w.clamp(1, MAX_CONCURRENT_CHILD_LOADS);
            for current_round_chunk in current_round_depths.chunks(child_load_limit) {
                let loaded_children = future::join_all(current_round_chunk.iter().map(
                    |(child_id, child_depth)| async move {
                        (
                            *child_id,
                            *child_depth,
                            Self::load_validated_block(store, child_id, false).await,
                        )
                    },
                ))
                .await;
                for (child_id, child_depth, validated) in loaded_children {
                    let validated = match validated {
                        Ok(validated) => validated,
                        Err(error) => {
                            telemetry.finish_with_error(&error);
                            emit_search_telemetry(telemetry_mode.observer(), telemetry.summary());
                            return Err(error);
                        }
                    };
                    match self.build_block_candidates(
                        &child_id,
                        target,
                        validated,
                        child_depth,
                        &mut telemetry,
                    ) {
                        Ok(candidates) => next_candidates.extend(candidates),
                        Err(error) => {
                            telemetry.finish_with_error(&error);
                            emit_search_telemetry(telemetry_mode.observer(), telemetry.summary());
                            return Err(error);
                        }
                    }
                    expanded_children.insert(child_id);
                }
            }

            frontier.retain(|candidate| {
                !matches!(
                    candidate,
                    SearchCandidate::Branch { child, .. } if current_round_set.contains(child)
                )
            });
            frontier.extend(next_candidates);
        }
    }

    async fn load_block_candidates<Target>(
        &self,
        block_id: &BlockHash,
        target: &Target,
        store: &dyn BlockStore,
        is_root: bool,
        depth: usize,
        telemetry: &mut SearchTelemetryCollector,
    ) -> Result<Vec<SearchCandidate<CS::Score>>, SearchError>
    where
        EC: EmbeddingCompatibility<Target>,
        CS: CandidateScorer<Target>,
        FS: FrontierSelector<CS::Score>,
    {
        let validated = Self::load_validated_block(store, block_id, is_root).await?;
        self.build_block_candidates(block_id, target, validated, depth, telemetry)
    }

    async fn load_validated_block(
        store: &dyn BlockStore,
        block_id: &BlockHash,
        is_root: bool,
    ) -> Result<ValidatedBlock, SearchError> {
        match store.get(block_id).await {
            Ok(Some(validated)) => Ok(validated),
            Ok(None) if is_root => Err(SearchError::MissingRootBlock { root_id: *block_id }),
            Ok(None) => Err(SearchError::MissingChildBlock {
                child_id: *block_id,
            }),
            Err(error) => Err(classify_store_error(*block_id, is_root, error)),
        }
    }

    fn build_block_candidates<Target>(
        &self,
        block_id: &BlockHash,
        target: &Target,
        validated: ValidatedBlock,
        depth: usize,
        telemetry: &mut SearchTelemetryCollector,
    ) -> Result<Vec<SearchCandidate<CS::Score>>, SearchError>
    where
        EC: EmbeddingCompatibility<Target>,
        CS: CandidateScorer<Target>,
        FS: FrontierSelector<CS::Score>,
    {
        let entries = into_entries(validated);
        let (metadata, entries) = match entries {
            TypedEntries::Branch(metadata, entries) => (metadata, LoadedEntries::Branch(entries)),
            TypedEntries::Leaf(metadata, entries) => (metadata, LoadedEntries::Leaf(entries)),
        };
        telemetry.record_visited_block(*block_id, depth);

        let branch_ebcp = match &entries {
            LoadedEntries::Branch(_) => {
                parse_branch_ebcp_descriptor(&metadata.embedding_spec, metadata.ext.as_ref())
                    .map_err(|error| SearchError::ScoringFailure {
                        block_id: *block_id,
                        message: error.to_string(),
                    })?
            }
            LoadedEntries::Leaf(_) => None,
        };
        let comparison_spec = branch_ebcp
            .as_ref()
            .map(|descriptor| &descriptor.logical_embedding_spec)
            .unwrap_or(&metadata.embedding_spec);

        self.compatibility
            .ensure_compatible(target, comparison_spec)
            .map_err(|error| SearchError::IncompatibleEmbedding {
                block_id: *block_id,
                message: error.to_string(),
            })?;

        match entries {
            LoadedEntries::Branch(entries) => {
                let geometry_spec = Arc::new(comparison_spec.clone());
                entries
                    .into_iter()
                    .map(|entry| match branch_ebcp.as_ref() {
                        Some(descriptor) => {
                            let reconstructed = reconstruct_logical_branch_embedding_f32(
                                &entry.embedding,
                                &metadata.embedding_spec,
                                Some(descriptor),
                            )
                            .map_err(|error| {
                                SearchError::ScoringFailure {
                                    block_id: *block_id,
                                    message: error.to_string(),
                                }
                            })?;
                            let embedding = reconstructed
                                .iter()
                                .flat_map(|value| value.to_le_bytes())
                                .collect::<Vec<_>>();
                            self.scorer
                                .score(target, &embedding, comparison_spec)
                                .map(|score| SearchCandidate::Branch {
                                    child: entry.child,
                                    depth: depth + 1,
                                    level: metadata.level,
                                    embedding,
                                    embedding_spec: Arc::clone(&geometry_spec),
                                    score,
                                })
                                .map_err(|error| SearchError::ScoringFailure {
                                    block_id: *block_id,
                                    message: error.to_string(),
                                })
                        }
                        None => self
                            .scorer
                            .score(target, &entry.embedding, comparison_spec)
                            .map(|score| SearchCandidate::Branch {
                                child: entry.child,
                                depth: depth + 1,
                                level: metadata.level,
                                embedding: entry.embedding,
                                embedding_spec: Arc::clone(&geometry_spec),
                                score,
                            })
                            .map_err(|error| SearchError::ScoringFailure {
                                block_id: *block_id,
                                message: error.to_string(),
                            }),
                    })
                    .collect()
            }
            LoadedEntries::Leaf(entries) => entries
                .into_iter()
                .map(|entry| {
                    self.scorer
                        .score(target, &entry.embedding, &metadata.embedding_spec)
                        .map(|score| SearchCandidate::Leaf {
                            block_id: *block_id,
                            level: metadata.level,
                            entry,
                            score,
                        })
                        .map_err(|error| SearchError::ScoringFailure {
                            block_id: *block_id,
                            message: error.to_string(),
                        })
                })
                .collect(),
        }
    }
}

enum LoadedEntries {
    Branch(Vec<lexongraph_block::BranchEntry>),
    Leaf(Vec<LeafEntry>),
}

#[derive(Clone, Copy)]
enum TelemetryMode<'a> {
    Disabled,
    Enabled(Option<&'a dyn SearchTelemetryObserver>),
}

impl<'a> TelemetryMode<'a> {
    fn enabled(self) -> bool {
        matches!(self, Self::Enabled(_))
    }

    fn observer(self) -> Option<&'a dyn SearchTelemetryObserver> {
        match self {
            Self::Disabled => None,
            Self::Enabled(observer) => observer,
        }
    }
}

enum SearchCandidate<Score> {
    Branch {
        child: BlockHash,
        depth: usize,
        level: u64,
        embedding: Vec<u8>,
        embedding_spec: Arc<EmbeddingSpec>,
        score: Score,
    },
    Leaf {
        block_id: BlockHash,
        level: u64,
        entry: LeafEntry,
        score: Score,
    },
}

impl<Score> SearchCandidate<Score> {
    fn is_terminal(&self) -> bool {
        candidate_level(self) == 0
    }
}

fn classify_store_error(block_id: BlockHash, is_root: bool, error: BlockStoreError) -> SearchError {
    match error {
        BlockStoreError::BackendFailure(_) if is_root => SearchError::RootLoad(error),
        BlockStoreError::BackendFailure(_) => SearchError::ChildLoad {
            child_id: block_id,
            error,
        },
        other => SearchError::MalformedBlock {
            block_id,
            error: other,
        },
    }
}

fn compare_candidates<Score: Ord>(
    left: &SearchCandidate<Score>,
    right: &SearchCandidate<Score>,
) -> Ordering {
    candidate_score(right)
        .cmp(candidate_score(left))
        .then_with(|| candidate_level(left).cmp(&candidate_level(right)))
        .then_with(|| candidate_identity(left).cmp(candidate_identity(right)))
}

fn candidate_score<Score>(candidate: &SearchCandidate<Score>) -> &Score {
    match candidate {
        SearchCandidate::Branch { score, .. } | SearchCandidate::Leaf { score, .. } => score,
    }
}

fn candidate_level<Score>(candidate: &SearchCandidate<Score>) -> u64 {
    match candidate {
        SearchCandidate::Branch { level, .. } | SearchCandidate::Leaf { level, .. } => *level,
    }
}

fn candidate_identity(candidate: &SearchCandidate<impl Ord>) -> &[u8; 32] {
    match candidate {
        SearchCandidate::Branch { child, .. } => child.as_bytes(),
        SearchCandidate::Leaf { block_id, .. } => block_id.as_bytes(),
    }
}

struct SearchTelemetryCollector {
    visited_blocks: Option<HashSet<BlockHash>>,
    enabled: bool,
    summary: SearchTelemetrySummary,
}

impl SearchTelemetryCollector {
    fn new(beam_width: usize, enabled: bool) -> Self {
        Self {
            visited_blocks: enabled.then(HashSet::new),
            enabled,
            summary: SearchTelemetrySummary {
                beam_width,
                distinct_blocks_visited: 0,
                max_routing_depth: 0,
                termination: SearchTerminationKind::Success,
            },
        }
    }

    fn record_visited_block(&mut self, block_id: BlockHash, depth: usize) {
        if let Some(visited_blocks) = &mut self.visited_blocks {
            visited_blocks.insert(block_id);
            self.summary.distinct_blocks_visited = visited_blocks.len();
            self.summary.max_routing_depth = self.summary.max_routing_depth.max(depth);
        }
    }

    fn finish_success(&mut self) {
        if self.enabled {
            self.summary.termination = SearchTerminationKind::Success;
        }
    }

    fn finish_with_error(&mut self, error: &SearchError) {
        if self.enabled {
            self.summary.termination = match error {
                SearchError::InvalidTraversalWidth { .. } => {
                    SearchTerminationKind::InvalidTraversalWidth
                }
                SearchError::MissingRootBlock { .. } => SearchTerminationKind::MissingRootBlock,
                SearchError::RootLoad(_) => SearchTerminationKind::RootLoadFailure,
                SearchError::MissingChildBlock { .. } => SearchTerminationKind::MissingChildBlock,
                SearchError::ChildLoad { .. } => SearchTerminationKind::ChildLoadFailure,
                SearchError::MalformedBlock { .. } => SearchTerminationKind::MalformedBlock,
                SearchError::IncompatibleEmbedding { .. } => {
                    SearchTerminationKind::IncompatibleEmbedding
                }
                SearchError::ScoringFailure { .. } => SearchTerminationKind::ScoringFailure,
                SearchError::FrontierSelectionFailure { .. } => {
                    SearchTerminationKind::FrontierSelectionFailure
                }
                SearchError::Exhausted { .. } => SearchTerminationKind::Exhausted,
            };
        }
    }

    fn summary(&self) -> Option<&SearchTelemetrySummary> {
        self.enabled.then_some(&self.summary)
    }

    fn into_summary(self) -> SearchTelemetrySummary {
        self.summary
    }
}

fn emit_search_telemetry(
    observer: Option<&dyn SearchTelemetryObserver>,
    summary: Option<&SearchTelemetrySummary>,
) {
    if let (Some(observer), Some(summary)) = (observer, summary) {
        observer.record_summary(summary);
    }
}

fn deduplicated_expandable_frontier<'a, Score: Ord>(
    frontier: &'a [SearchCandidate<Score>],
    expanded_children: &HashSet<BlockHash>,
) -> Vec<ExpandableFrontierCandidate<'a, Score>> {
    let mut seen_children = HashSet::new();
    let mut expandable = Vec::new();

    for candidate in frontier {
        let SearchCandidate::Branch {
            child,
            depth,
            level,
            embedding,
            embedding_spec,
            score,
        } = candidate
        else {
            continue;
        };

        if expanded_children.contains(child) || !seen_children.insert(*child) {
            continue;
        }

        expandable.push(ExpandableFrontierCandidate {
            child: *child,
            depth: *depth,
            level: *level,
            score,
            embedding,
            embedding_spec: embedding_spec.as_ref(),
        });
    }

    expandable
}

fn validate_selected_children<Score>(
    selected: &[BlockHash],
    frontier: &[ExpandableFrontierCandidate<'_, Score>],
    w: usize,
) -> Result<Vec<BlockHash>, SearchError> {
    let expected_len = frontier.len().min(w);
    if selected.len() != expected_len {
        return Err(SearchError::FrontierSelectionFailure {
            message: format!(
                "frontier selector returned {} child blocks, expected {} for frontier size {} and width {}",
                selected.len(),
                expected_len,
                frontier.len(),
                w
            ),
        });
    }

    let allowed = frontier
        .iter()
        .map(|candidate| candidate.child)
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    for child in selected {
        if !allowed.contains(child) {
            return Err(SearchError::FrontierSelectionFailure {
                message: format!("frontier selector returned unknown child block {child}"),
            });
        }
        if !seen.insert(*child) {
            return Err(SearchError::FrontierSelectionFailure {
                message: format!("frontier selector returned duplicate child block {child}"),
            });
        }
    }

    Ok(selected.to_vec())
}

fn ensure_matching_specs(
    target: &EmbeddingSpec,
    candidate: &EmbeddingSpec,
) -> Result<(), DefaultPolicyError> {
    if target.encoding == candidate.encoding && target.dims == candidate.dims {
        Ok(())
    } else {
        Err(DefaultPolicyError::IncompatibleEmbeddingSpec {
            target: target.clone(),
            candidate: candidate.clone(),
        })
    }
}

fn validate_embedding_bytes(
    bytes: &[u8],
    spec: &EmbeddingSpec,
    role: &'static str,
) -> Result<usize, DefaultPolicyError> {
    let width = element_width(spec)?;
    let expected = expected_byte_len(spec, width)?;
    if bytes.len() != expected {
        return Err(DefaultPolicyError::InvalidByteLength {
            role,
            encoding: spec.encoding.clone(),
            dims: spec.dims,
            expected,
            actual: bytes.len(),
        });
    }
    Ok(width)
}

fn decode_geometry_vector<Score>(
    candidate: &ExpandableFrontierCandidate<'_, Score>,
) -> Result<Vec<f64>, GeometryAwareFrontierSelectionError> {
    let width = match candidate.embedding_spec.encoding.as_str() {
        "f32le" => std::mem::size_of::<f32>(),
        "f64le" => std::mem::size_of::<f64>(),
        _ => {
            return Err(GeometryAwareFrontierSelectionError::UnsupportedEncoding {
                encoding: candidate.embedding_spec.encoding.clone(),
            });
        }
    };
    let expected = candidate
        .embedding_spec
        .dims
        .checked_mul(width as u64)
        .ok_or_else(|| GeometryAwareFrontierSelectionError::DimensionOverflow {
            child: candidate.child,
            encoding: candidate.embedding_spec.encoding.clone(),
            dims: candidate.embedding_spec.dims,
        })?;
    let expected = usize::try_from(expected).map_err(|_| {
        GeometryAwareFrontierSelectionError::DimensionOverflow {
            child: candidate.child,
            encoding: candidate.embedding_spec.encoding.clone(),
            dims: candidate.embedding_spec.dims,
        }
    })?;
    if candidate.embedding.len() != expected {
        return Err(GeometryAwareFrontierSelectionError::InvalidByteLength {
            child: candidate.child,
            encoding: candidate.embedding_spec.encoding.clone(),
            dims: candidate.embedding_spec.dims,
            expected,
            actual: candidate.embedding.len(),
        });
    }

    let mut vector = Vec::with_capacity(candidate.embedding_spec.dims as usize);
    let mut norm_sq = 0.0f64;
    match candidate.embedding_spec.encoding.as_str() {
        "f32le" => {
            for (index, chunk) in candidate.embedding.chunks_exact(width).enumerate() {
                let value = f32::from_le_bytes(chunk.try_into().expect("chunk size is validated"));
                if !value.is_finite() {
                    return Err(GeometryAwareFrontierSelectionError::NonFiniteValue {
                        child: candidate.child,
                        index,
                    });
                }
                let value = value as f64;
                norm_sq += value * value;
                vector.push(value);
            }
        }
        "f64le" => {
            for (index, chunk) in candidate.embedding.chunks_exact(width).enumerate() {
                let value = f64::from_le_bytes(chunk.try_into().expect("chunk size is validated"));
                if !value.is_finite() {
                    return Err(GeometryAwareFrontierSelectionError::NonFiniteValue {
                        child: candidate.child,
                        index,
                    });
                }
                norm_sq += value * value;
                vector.push(value);
            }
        }
        _ => unreachable!("unsupported encodings return early"),
    }

    if norm_sq == 0.0 {
        return Err(GeometryAwareFrontierSelectionError::ZeroMagnitude {
            child: candidate.child,
        });
    }

    let norm = norm_sq.sqrt();
    for value in &mut vector {
        *value /= norm;
    }
    Ok(vector)
}

fn min_cosine_distance(candidate: &[f64], selected: &[usize], vectors: &[Vec<f64>]) -> f64 {
    selected
        .iter()
        .map(|index| 1.0 - dot(candidate, &vectors[*index]))
        .fold(f64::INFINITY, f64::min)
}

fn dot(left: &[f64], right: &[f64]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(left, right)| left * right)
        .sum()
}

fn cosine_similarity_bytes(
    target: &[u8],
    candidate: &[u8],
    spec: &EmbeddingSpec,
) -> Result<CosineScore, DefaultPolicyError> {
    let width = validate_embedding_bytes(target, spec, "target")?;
    validate_embedding_bytes(candidate, spec, "candidate")?;

    match spec.encoding.as_str() {
        "f32le" => {
            let mut dot = 0.0f64;
            let mut target_norm_sq = 0.0f64;
            let mut candidate_norm_sq = 0.0f64;

            for (index, (target_chunk, candidate_chunk)) in target
                .chunks_exact(width)
                .zip(candidate.chunks_exact(width))
                .enumerate()
            {
                let target_value =
                    f32::from_le_bytes(target_chunk.try_into().expect("chunk size is validated"));
                if !target_value.is_finite() {
                    return Err(DefaultPolicyError::NonFiniteValue {
                        role: "target",
                        index,
                    });
                }

                let candidate_value = f32::from_le_bytes(
                    candidate_chunk.try_into().expect("chunk size is validated"),
                );
                if !candidate_value.is_finite() {
                    return Err(DefaultPolicyError::NonFiniteValue {
                        role: "candidate",
                        index,
                    });
                }

                let target_value = target_value as f64;
                let candidate_value = candidate_value as f64;
                dot += target_value * candidate_value;
                target_norm_sq += target_value * target_value;
                candidate_norm_sq += candidate_value * candidate_value;
            }

            cosine_similarity_from_parts(dot, target_norm_sq, candidate_norm_sq)
        }
        "f64le" => {
            let mut dot = 0.0f64;
            let mut target_norm_sq = 0.0f64;
            let mut candidate_norm_sq = 0.0f64;

            for (index, (target_chunk, candidate_chunk)) in target
                .chunks_exact(width)
                .zip(candidate.chunks_exact(width))
                .enumerate()
            {
                let target_value =
                    f64::from_le_bytes(target_chunk.try_into().expect("chunk size is validated"));
                if !target_value.is_finite() {
                    return Err(DefaultPolicyError::NonFiniteValue {
                        role: "target",
                        index,
                    });
                }

                let candidate_value = f64::from_le_bytes(
                    candidate_chunk.try_into().expect("chunk size is validated"),
                );
                if !candidate_value.is_finite() {
                    return Err(DefaultPolicyError::NonFiniteValue {
                        role: "candidate",
                        index,
                    });
                }

                dot += target_value * candidate_value;
                target_norm_sq += target_value * target_value;
                candidate_norm_sq += candidate_value * candidate_value;
            }

            cosine_similarity_from_parts(dot, target_norm_sq, candidate_norm_sq)
        }
        _ => Err(DefaultPolicyError::UnsupportedEncoding {
            encoding: spec.encoding.clone(),
        }),
    }
}

fn cosine_similarity_from_parts(
    dot: f64,
    target_norm_sq: f64,
    candidate_norm_sq: f64,
) -> Result<CosineScore, DefaultPolicyError> {
    if target_norm_sq == 0.0 {
        return Err(DefaultPolicyError::ZeroMagnitude { role: "target" });
    }
    if candidate_norm_sq == 0.0 {
        return Err(DefaultPolicyError::ZeroMagnitude { role: "candidate" });
    }

    CosineScore::from_f64(dot / (target_norm_sq.sqrt() * candidate_norm_sq.sqrt()))
}

fn element_width(spec: &EmbeddingSpec) -> Result<usize, DefaultPolicyError> {
    match spec.encoding.as_str() {
        "f32le" => Ok(std::mem::size_of::<f32>()),
        "f64le" => Ok(std::mem::size_of::<f64>()),
        _ => Err(DefaultPolicyError::UnsupportedEncoding {
            encoding: spec.encoding.clone(),
        }),
    }
}

fn expected_byte_len(spec: &EmbeddingSpec, width: usize) -> Result<usize, DefaultPolicyError> {
    let expected = spec.dims.checked_mul(width as u64).ok_or_else(|| {
        DefaultPolicyError::DimensionOverflow {
            encoding: spec.encoding.clone(),
            dims: spec.dims,
        }
    })?;
    usize::try_from(expected).map_err(|_| DefaultPolicyError::DimensionOverflow {
        encoding: spec.encoding.clone(),
        dims: spec.dims,
    })
}

fn total_order_key_f64(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits >> 63 == 0 {
        bits ^ (1_u64 << 63)
    } else {
        !bits
    }
}

#[cfg(any(test, feature = "conformance"))]
#[allow(dead_code)]
mod conformance_support {
    use std::fmt;

    use super::*;

    pub type ConformanceResult = Result<(), ConformanceError>;

    #[derive(Debug)]
    pub enum ConformanceError {
        Expectation(String),
    }

    impl fmt::Display for ConformanceError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Expectation(message) => {
                    write!(f, "conformance expectation failed: {message}")
                }
            }
        }
    }

    impl std::error::Error for ConformanceError {}

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct FixtureError(pub String);

    impl fmt::Display for FixtureError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl std::error::Error for FixtureError {}

    pub trait EmbeddingCompatibilityConformanceHarness {
        type Target;
        type Policy: EmbeddingCompatibility<Self::Target>;

        fn target(&self) -> Self::Target;
        fn compatible_spec(&self) -> EmbeddingSpec;
        fn incompatible_spec(&self) -> EmbeddingSpec;
        fn conforming_policy(&self) -> Self::Policy;
        fn nondeterministic_policy(&self) -> Self::Policy;
    }

    pub trait CandidateScorerConformanceHarness {
        type Target;
        type Score: Ord + Eq + fmt::Debug;
        type Scorer: CandidateScorer<Self::Target, Score = Self::Score>;

        fn target(&self) -> Self::Target;
        fn embedding_spec(&self) -> EmbeddingSpec;
        fn preferred_candidate_embedding(&self) -> Vec<u8>;
        fn alternate_candidate_embedding(&self) -> Vec<u8>;
        fn expected_score(&self) -> Self::Score;
        fn conforming_scorer(&self) -> Self::Scorer;
        fn failing_scorer(&self) -> Self::Scorer;
        fn nondeterministic_scorer(&self) -> Self::Scorer;
    }

    pub fn run_embedding_compatibility_suite<H>(harness: &H) -> ConformanceResult
    where
        H: EmbeddingCompatibilityConformanceHarness,
    {
        let target = harness.target();
        let compatible_spec = harness.compatible_spec();
        let policy = harness.conforming_policy();
        policy
            .ensure_compatible(&target, &compatible_spec)
            .map_err(|error| {
                ConformanceError::Expectation(format!(
                    "expected compatible embedding spec to be accepted, got {error}"
                ))
            })?;
        policy
            .ensure_compatible(&target, &compatible_spec)
            .map_err(|error| {
                ConformanceError::Expectation(format!(
                    "expected repeated compatible embedding-spec check to remain accepted, got {error}"
                ))
            })?;

        let incompatible_spec = harness.incompatible_spec();
        if policy
            .ensure_compatible(&target, &incompatible_spec)
            .is_ok()
        {
            return Err(ConformanceError::Expectation(
                "expected incompatible embedding spec to be rejected".into(),
            ));
        }
        if policy
            .ensure_compatible(&target, &incompatible_spec)
            .is_ok()
        {
            return Err(ConformanceError::Expectation(
                "expected repeated incompatible embedding-spec check to remain rejected".into(),
            ));
        }

        let flaky = harness.nondeterministic_policy();
        let first = flaky.ensure_compatible(&target, &compatible_spec).is_ok();
        let second = flaky.ensure_compatible(&target, &compatible_spec).is_ok();
        if first == second {
            return Err(ConformanceError::Expectation(
                "expected nondeterministic embedding-compatibility fixture to change outcome on repeated inputs".into(),
            ));
        }

        Ok(())
    }

    pub fn run_candidate_scorer_suite<H>(harness: &H) -> ConformanceResult
    where
        H: CandidateScorerConformanceHarness,
    {
        let target = harness.target();
        let embedding_spec = harness.embedding_spec();
        let preferred = harness.preferred_candidate_embedding();
        let alternate = harness.alternate_candidate_embedding();

        let scorer = harness.conforming_scorer();
        let preferred_score =
            scorer
                .score(&target, &preferred, &embedding_spec)
                .map_err(|error| {
                    ConformanceError::Expectation(format!(
                        "expected conforming scorer to produce a score, got {error}"
                    ))
                })?;
        let repeated_preferred_score =
            scorer
                .score(&target, &preferred, &embedding_spec)
                .map_err(|error| {
                    ConformanceError::Expectation(format!(
                        "expected repeated preferred-candidate scoring call to succeed, got {error}"
                    ))
                })?;
        if preferred_score != harness.expected_score() {
            return Err(ConformanceError::Expectation(format!(
                "expected score {:?}, got {:?}",
                harness.expected_score(),
                preferred_score
            )));
        }
        if preferred_score != repeated_preferred_score {
            return Err(ConformanceError::Expectation(format!(
                "expected repeated preferred-candidate score {:?}, got {:?}",
                preferred_score, repeated_preferred_score
            )));
        }

        let alternate_score =
            scorer
                .score(&target, &alternate, &embedding_spec)
                .map_err(|error| {
                    ConformanceError::Expectation(format!(
                        "expected alternate candidate to score successfully, got {error}"
                    ))
                })?;
        if preferred_score <= alternate_score {
            return Err(ConformanceError::Expectation(
                "expected preferred candidate to outrank the alternate candidate".into(),
            ));
        }

        if harness
            .failing_scorer()
            .score(&target, &preferred, &embedding_spec)
            .is_ok()
        {
            return Err(ConformanceError::Expectation(
                "expected failing scorer fixture to return an error".into(),
            ));
        }

        let flaky = harness.nondeterministic_scorer();
        let first = flaky
            .score(&target, &preferred, &embedding_spec)
            .map_err(|error| {
                ConformanceError::Expectation(format!(
                    "expected first nondeterministic scoring call to succeed, got {error}"
                ))
            })?;
        let second = flaky
            .score(&target, &preferred, &embedding_spec)
            .map_err(|error| {
                ConformanceError::Expectation(format!(
                    "expected second nondeterministic scoring call to succeed, got {error}"
                ))
            })?;
        if first == second {
            return Err(ConformanceError::Expectation(
                "expected nondeterministic candidate-scorer fixture to change score on repeated inputs".into(),
            ));
        }

        Ok(())
    }

    pub fn run_full_trait_suite<EC, CS>(
        compatibility_harness: &EC,
        scorer_harness: &CS,
    ) -> ConformanceResult
    where
        EC: EmbeddingCompatibilityConformanceHarness,
        CS: CandidateScorerConformanceHarness<Target = EC::Target>,
    {
        run_embedding_compatibility_suite(compatibility_harness)?;
        run_candidate_scorer_suite(scorer_harness)
    }
}

#[cfg(feature = "conformance")]
pub mod conformance {
    //! Opt-in helper APIs for validating downstream search-owned policy traits.
    //!
    //! Enable this module from test code with a dev-dependency such as:
    //!
    //! ```toml
    //! [dev-dependencies]
    //! lexongraph-search = { version = "*", features = ["conformance"] }
    //! ```

    pub use super::conformance_support::{
        CandidateScorerConformanceHarness, ConformanceError, ConformanceResult,
        EmbeddingCompatibilityConformanceHarness, FixtureError, run_candidate_scorer_suite,
        run_embedding_compatibility_suite, run_full_trait_suite,
    };
}
