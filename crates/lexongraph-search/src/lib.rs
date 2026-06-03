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
//! fn search_one<Target, EC, CS>(
//!     searcher: &Searcher<EC, CS>,
//!     root_id: &BlockHash,
//!     target: &Target,
//!     store: &dyn BlockStore,
//! ) -> Result<SearchResult, SearchError>
//! where
//!     EC: EmbeddingCompatibility<Target>,
//!     CS: CandidateScorer<Target>,
//! {
//!     searcher.search(root_id, target, 1, 1, store)
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
use std::fmt;

pub use lexongraph_block::{BlockHash, EmbeddingSpec, LeafEntry};

use lexongraph_block::{TypedEntries, into_entries};
use lexongraph_block_store::{BlockStore, BlockStoreError};

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
            | Self::Exhausted { .. } => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Searcher<EC, CS> {
    compatibility: EC,
    scorer: CS,
}

impl<EC, CS> Searcher<EC, CS> {
    pub fn new(compatibility: EC, scorer: CS) -> Self {
        Self {
            compatibility,
            scorer,
        }
    }
}

impl<EC, CS> Searcher<EC, CS> {
    pub fn search<Target>(
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
    {
        if w == 0 {
            return Err(SearchError::InvalidTraversalWidth { w });
        }

        let mut frontier = self.load_block_candidates(root_id, target, store, true)?;
        let mut expanded_children = HashSet::new();

        loop {
            frontier.retain(|candidate| {
                !matches!(
                    candidate,
                    SearchCandidate::Branch { child, .. } if expanded_children.contains(child)
                )
            });
            frontier.sort_by(compare_candidates::<CS::Score>);

            if frontier.len() >= n && frontier.iter().take(n).all(SearchCandidate::is_leaf) {
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
                return Ok(SearchResult { leaves });
            }

            let current_round = select_children_to_expand(&frontier, &expanded_children, w);
            if current_round.is_empty() {
                return Err(SearchError::Exhausted {
                    requested: n,
                    reachable_leaves: frontier
                        .iter()
                        .filter(|candidate| candidate.is_leaf())
                        .count(),
                });
            }

            let current_round_set: HashSet<_> = current_round.iter().copied().collect();
            let mut next_candidates = Vec::new();
            for child_id in &current_round {
                next_candidates.extend(self.load_block_candidates(child_id, target, store, false)?);
                expanded_children.insert(*child_id);
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

    fn load_block_candidates<Target>(
        &self,
        block_id: &BlockHash,
        target: &Target,
        store: &dyn BlockStore,
        is_root: bool,
    ) -> Result<Vec<SearchCandidate<CS::Score>>, SearchError>
    where
        EC: EmbeddingCompatibility<Target>,
        CS: CandidateScorer<Target>,
    {
        let validated = match store.get(block_id) {
            Ok(Some(validated)) => validated,
            Ok(None) if is_root => {
                return Err(SearchError::MissingRootBlock { root_id: *block_id });
            }
            Ok(None) => {
                return Err(SearchError::MissingChildBlock {
                    child_id: *block_id,
                });
            }
            Err(error) => return Err(classify_store_error(*block_id, is_root, error)),
        };

        let entries = into_entries(validated);
        let (metadata, entries) = match entries {
            TypedEntries::Branch(metadata, entries) => (metadata, LoadedEntries::Branch(entries)),
            TypedEntries::Leaf(metadata, entries) => (metadata, LoadedEntries::Leaf(entries)),
        };

        self.compatibility
            .ensure_compatible(target, &metadata.embedding_spec)
            .map_err(|error| SearchError::IncompatibleEmbedding {
                block_id: *block_id,
                message: error.to_string(),
            })?;

        match entries {
            LoadedEntries::Branch(entries) => entries
                .into_iter()
                .map(|entry| {
                    self.scorer
                        .score(target, &entry.embedding, &metadata.embedding_spec)
                        .map(|score| SearchCandidate::Branch {
                            child: entry.child,
                            score,
                        })
                        .map_err(|error| SearchError::ScoringFailure {
                            block_id: *block_id,
                            message: error.to_string(),
                        })
                })
                .collect(),
            LoadedEntries::Leaf(entries) => entries
                .into_iter()
                .map(|entry| {
                    self.scorer
                        .score(target, &entry.embedding, &metadata.embedding_spec)
                        .map(|score| SearchCandidate::Leaf {
                            block_id: *block_id,
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

enum SearchCandidate<Score> {
    Branch {
        child: BlockHash,
        score: Score,
    },
    Leaf {
        block_id: BlockHash,
        entry: LeafEntry,
        score: Score,
    },
}

impl<Score> SearchCandidate<Score> {
    fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf { .. })
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
        .then_with(|| match (left, right) {
            (SearchCandidate::Leaf { .. }, SearchCandidate::Branch { .. }) => Ordering::Less,
            (SearchCandidate::Branch { .. }, SearchCandidate::Leaf { .. }) => Ordering::Greater,
            _ => Ordering::Equal,
        })
        .then_with(|| candidate_identity(left).cmp(candidate_identity(right)))
}

fn candidate_score<Score>(candidate: &SearchCandidate<Score>) -> &Score {
    match candidate {
        SearchCandidate::Branch { score, .. } | SearchCandidate::Leaf { score, .. } => score,
    }
}

fn candidate_identity(candidate: &SearchCandidate<impl Ord>) -> &[u8; 32] {
    match candidate {
        SearchCandidate::Branch { child, .. } => child.as_bytes(),
        SearchCandidate::Leaf { block_id, .. } => block_id.as_bytes(),
    }
}

fn select_children_to_expand<Score: Ord>(
    frontier: &[SearchCandidate<Score>],
    expanded_children: &HashSet<BlockHash>,
    w: usize,
) -> Vec<BlockHash> {
    let mut selected = Vec::new();
    let mut seen_children = HashSet::new();

    for candidate in frontier {
        let SearchCandidate::Branch { child, .. } = candidate else {
            continue;
        };

        if expanded_children.contains(child) || !seen_children.insert(*child) {
            continue;
        }

        selected.push(*child);
        if selected.len() == w {
            break;
        }
    }

    selected
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
