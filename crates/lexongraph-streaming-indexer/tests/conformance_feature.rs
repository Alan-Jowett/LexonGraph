// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![cfg(feature = "conformance")]

use lexongraph_block::{BranchBlock, Content};
use lexongraph_dcbc_streaming::DcbcStreamingTrainer;
use lexongraph_streaming_clustering::{StreamingClusteringConfig, StreamingClusteringError};
use lexongraph_streaming_indexer::conformance::{
    CanonicalEmbeddingPolicyConformanceHarness, ContentResolverConformanceHarness, FixtureError,
    StreamingClusteringFactoryConformanceHarness, run_content_resolver_suite, run_full_trait_suite,
};
use lexongraph_streaming_indexer::{
    CanonicalEmbeddingPolicy, ContentResolver, EmbeddingSpec, IndexItem, StreamingClusteringFactory,
};
use sha2::{Digest, Sha256};

// ─── ContentResolver fixture ─────────────────────────────────────────────────

#[derive(Clone)]
enum ResolverMode {
    Good,
    Fail,
    Unusable,
}

#[derive(Clone)]
struct ResolverFixture(ResolverMode);

impl ContentResolver<&'static str> for ResolverFixture {
    type Error = FixtureError;

    fn resolve(&self, content_ref: &&'static str) -> Result<Content, Self::Error> {
        match self.0 {
            ResolverMode::Good => Ok(Content {
                media_type: "text/plain".into(),
                body: content_ref.as_bytes().to_vec(),
            }),
            ResolverMode::Fail => Err(FixtureError("resolver failure".into())),
            ResolverMode::Unusable => Ok(Content {
                media_type: String::new(),
                body: content_ref.as_bytes().to_vec(),
            }),
        }
    }

    fn fingerprint(
        &self,
        content_ref: &&'static str,
    ) -> Result<lexongraph_streaming_indexer::BlockHash, Self::Error> {
        Ok(lexongraph_streaming_indexer::BlockHash::from_bytes(
            Sha256::digest(content_ref.as_bytes()).into(),
        ))
    }
}

struct ContentHarness;

impl ContentResolverConformanceHarness for ContentHarness {
    type Ref = &'static str;
    type Resolver = ResolverFixture;

    fn sample_item(&self) -> IndexItem<Self::Ref> {
        IndexItem {
            metadata: vec![],
            content_ref: "fixture",
        }
    }

    fn expected_content(&self) -> Content {
        Content {
            media_type: "text/plain".into(),
            body: b"fixture".to_vec(),
        }
    }

    fn conforming_resolver(&self) -> Self::Resolver {
        ResolverFixture(ResolverMode::Good)
    }

    fn failing_resolver(&self) -> Self::Resolver {
        ResolverFixture(ResolverMode::Fail)
    }

    fn unusable_resolver(&self) -> Self::Resolver {
        ResolverFixture(ResolverMode::Unusable)
    }
}

// ─── CanonicalEmbeddingPolicy fixture ────────────────────────────────────────

#[derive(Clone)]
enum CanonicalMode {
    Good,
    Fail,
    WrongLength,
}

#[derive(Clone)]
struct CanonicalFixture(CanonicalMode);

impl CanonicalEmbeddingPolicy for CanonicalFixture {
    type Error = FixtureError;

    fn canonical_embedding(&self, block: &BranchBlock) -> Result<Vec<u8>, Self::Error> {
        match self.0 {
            CanonicalMode::Good => Ok(block.entries[0].embedding.clone()),
            CanonicalMode::Fail => Err(FixtureError("canonical failure".into())),
            CanonicalMode::WrongLength => Ok(vec![0x01]),
        }
    }
}

struct CanonicalHarness;

impl CanonicalEmbeddingPolicyConformanceHarness for CanonicalHarness {
    type Policy = CanonicalFixture;

    fn conforming_policy(&self) -> Self::Policy {
        CanonicalFixture(CanonicalMode::Good)
    }

    fn failing_policy(&self) -> Self::Policy {
        CanonicalFixture(CanonicalMode::Fail)
    }

    fn invalid_length_policy(&self) -> Self::Policy {
        CanonicalFixture(CanonicalMode::WrongLength)
    }
}

// ─── StreamingClusteringFactory fixture ──────────────────────────────────────

struct FactoryHarness;

struct ConformingFactory;

impl StreamingClusteringFactory for ConformingFactory {
    type Trainer = DcbcStreamingTrainer;
    type Error = StreamingClusteringError;

    fn create_trainer(
        &self,
        dimensions: usize,
        _estimated_child_count: usize,
        _block_size_target: usize,
        _embedding_spec: &EmbeddingSpec,
    ) -> Result<DcbcStreamingTrainer, StreamingClusteringError> {
        DcbcStreamingTrainer::new(StreamingClusteringConfig {
            cluster_count: 2,
            dimensions,
            balance_constraints: None,
            random_seed: None,
        })
    }
}

impl StreamingClusteringFactoryConformanceHarness for FactoryHarness {
    type Factory = ConformingFactory;

    fn conforming_factory(&self) -> Self::Factory {
        ConformingFactory
    }
}

// ─── Suite invocations ────────────────────────────────────────────────────────

#[test]
fn downstream_crates_can_run_the_content_resolver_suite() {
    run_content_resolver_suite(&ContentHarness).unwrap();
}

#[test]
fn downstream_crates_can_run_the_full_trait_suite() {
    run_full_trait_suite(&ContentHarness, &CanonicalHarness, &FactoryHarness).unwrap();
}
