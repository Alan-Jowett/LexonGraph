#![cfg(feature = "conformance")]

use lexongraph_block::{BranchBlock, Content};
use lexongraph_indexer::conformance::{
    CanonicalEmbeddingPolicyConformanceHarness, ContentResolverConformanceHarness,
    EmbeddingProviderConformanceHarness, FixtureError, NodePackingPolicyConformanceHarness,
    run_content_resolver_suite, run_embedding_provider_suite, run_full_trait_suite,
};
use lexongraph_indexer::{
    CanonicalEmbeddingPolicy, ContentResolver, EmbeddingProvider, IndexItem, IndexedChild,
    NodePackingPolicy,
};

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

#[derive(Clone)]
enum ProviderMode {
    Good,
    Fail,
    WrongLength,
}

#[derive(Clone)]
struct ProviderFixture(ProviderMode);

impl EmbeddingProvider for ProviderFixture {
    type Error = FixtureError;

    fn embed(
        &self,
        content: &Content,
        _: &lexongraph_block::EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        match self.0 {
            ProviderMode::Good => Ok(vec![content.body[0], 0x20]),
            ProviderMode::Fail => Err(FixtureError("embed failure".into())),
            ProviderMode::WrongLength => Ok(vec![0x01]),
        }
    }
}

struct EmbeddingHarness;

impl EmbeddingProviderConformanceHarness for EmbeddingHarness {
    type Provider = ProviderFixture;

    fn expected_embedding(&self) -> Vec<u8> {
        vec![b'f', 0x20]
    }

    fn conforming_provider(&self) -> Self::Provider {
        ProviderFixture(ProviderMode::Good)
    }

    fn failing_provider(&self) -> Self::Provider {
        ProviderFixture(ProviderMode::Fail)
    }

    fn invalid_length_provider(&self) -> Self::Provider {
        ProviderFixture(ProviderMode::WrongLength)
    }
}

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

#[derive(Clone)]
enum PackingMode {
    Good,
    Fail,
    Singleton,
    OutOfBounds,
    Missing,
}

#[derive(Clone)]
struct PackingFixture(PackingMode);

impl NodePackingPolicy for PackingFixture {
    type Error = FixtureError;

    fn pack(
        &self,
        children: &[IndexedChild],
        _: &lexongraph_block::EmbeddingSpec,
        _: usize,
    ) -> Result<Vec<Vec<usize>>, Self::Error> {
        match self.0 {
            PackingMode::Good => {
                let mut groups = Vec::new();
                let mut index = 0;
                while index < children.len() {
                    let remaining = children.len() - index;
                    let width = if remaining == 3 { 3 } else { 2 };
                    groups.push((index..index + width).collect());
                    index += width;
                }
                Ok(groups)
            }
            PackingMode::Fail => Err(FixtureError("packing failure".into())),
            PackingMode::Singleton => Ok((0..children.len()).map(|index| vec![index]).collect()),
            PackingMode::OutOfBounds => Ok(vec![vec![0, 1], vec![2, children.len()]]),
            PackingMode::Missing => Ok(vec![vec![0, 1], vec![2]]),
        }
    }
}

struct PackingHarness;

impl NodePackingPolicyConformanceHarness for PackingHarness {
    type Policy = PackingFixture;

    fn conforming_policy(&self) -> Self::Policy {
        PackingFixture(PackingMode::Good)
    }

    fn failing_policy(&self) -> Self::Policy {
        PackingFixture(PackingMode::Fail)
    }

    fn singleton_group_policy(&self) -> Self::Policy {
        PackingFixture(PackingMode::Singleton)
    }

    fn out_of_bounds_policy(&self) -> Self::Policy {
        PackingFixture(PackingMode::OutOfBounds)
    }

    fn missing_child_policy(&self) -> Self::Policy {
        PackingFixture(PackingMode::Missing)
    }
}

#[test]
fn downstream_crates_can_run_the_content_resolver_suite() {
    run_content_resolver_suite(&ContentHarness).unwrap();
}

#[test]
fn downstream_crates_can_run_the_embedding_provider_suite() {
    run_embedding_provider_suite(&EmbeddingHarness).unwrap();
}

#[test]
fn downstream_crates_can_run_the_full_trait_suite() {
    run_full_trait_suite(
        &ContentHarness,
        &EmbeddingHarness,
        &CanonicalHarness,
        &PackingHarness,
    )
    .unwrap();
}
