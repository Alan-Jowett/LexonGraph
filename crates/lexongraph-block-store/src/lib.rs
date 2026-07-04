// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Backend-agnostic storage contract for LexonGraph block bytes.
//!
//! ```
//! use lexongraph_block::{Block, BlockHash, ValidatedBlock};
//! use lexongraph_block_store::{BlockStore, BlockStoreError};
//!
//! async fn exercise_contract(
//!     store: &dyn BlockStore,
//!     block: &Block,
//! ) -> Result<Option<ValidatedBlock>, BlockStoreError> {
//!     let block_id = store.put(block).await?;
//!     store.get(&block_id).await
//! }
//! ```
//!
//! ```compile_fail
//! use lexongraph_block_store::MemoryBlockStore;
//!
//! let _ = MemoryBlockStore::default();
//! ```

use std::fmt;

use async_trait::async_trait;
use futures::{TryStreamExt, stream::Stream};
use lexongraph_block::{
    Block, BlockError, BlockHash, DecodedBlock, ValidatedBlock, VersionedBlock, deserialize_block,
    deserialize_versioned_block, serialize_block, serialize_versioned_block,
};

pub type BlockIdStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<BlockHash, BlockStoreError>> + Send + 'a>>;

#[async_trait]
pub trait BlockStore: Sync {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError>;

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError>;

    async fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        let serialized = serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
        self.put_block_bytes(&serialized.hash, &serialized.bytes)
            .await?;
        Ok(serialized.hash)
    }

    async fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        let Some(bytes) = self.get_block_bytes(block_id).await? else {
            return Ok(None);
        };
        deserialize_block(&bytes, block_id)
            .map(Some)
            .map_err(BlockStoreError::DecodeFailure)
    }

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError>;
}

#[async_trait]
pub trait BlockStoreExt: BlockStore {
    async fn put_versioned(&self, block: &VersionedBlock) -> Result<BlockHash, BlockStoreError> {
        let serialized =
            serialize_versioned_block(block).map_err(BlockStoreError::ContractViolation)?;
        self.put_block_bytes(&serialized.hash, &serialized.bytes)
            .await?;
        Ok(serialized.hash)
    }

    async fn get_decoded(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<DecodedBlock>, BlockStoreError> {
        let Some(bytes) = self.get_block_bytes(block_id).await? else {
            return Ok(None);
        };

        deserialize_versioned_block(&bytes, block_id)
            .map(Some)
            .map_err(BlockStoreError::DecodeFailure)
    }

    async fn list_block_ids(&self) -> Result<Vec<BlockHash>, BlockStoreError> {
        self.iter_block_ids()?.try_collect().await
    }
}

impl<T: BlockStore + ?Sized> BlockStoreExt for T {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockStoreError {
    BackendFailure(String),
    DecodeFailure(BlockError),
    ContractViolation(BlockError),
}

impl fmt::Display for BlockStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BackendFailure(message) => write!(f, "block store backend failure: {message}"),
            Self::DecodeFailure(error) => {
                write!(
                    f,
                    "stored block bytes failed shared decode/validation: {error}"
                )
            }
            Self::ContractViolation(error) => {
                write!(f, "block store contract violation: {error}")
            }
        }
    }
}

impl std::error::Error for BlockStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DecodeFailure(error) | Self::ContractViolation(error) => Some(error),
            Self::BackendFailure(_) => None,
        }
    }
}

#[cfg(any(test, feature = "conformance"))]
mod conformance_support {
    use std::collections::HashSet;
    use std::fmt;

    use async_trait::async_trait;
    use futures::TryStreamExt;
    use lexongraph_block::{
        Block, BlockHash, BranchEntry, Content, EmbeddingSpec, LeafEntry, VERSION_1,
        build_branch_block, build_leaf_block, serialize_block,
    };

    use super::{BlockStore, BlockStoreError};

    #[async_trait(?Send)]
    pub trait BlockStoreFactory {
        type Store: BlockStore;

        async fn fresh_store(&self) -> Self::Store;
    }

    #[allow(dead_code)]
    #[async_trait(?Send)]
    pub trait BlockStoreConformanceHarness: BlockStoreFactory {
        async fn inject_raw_bytes(
            &self,
            _store: &Self::Store,
            _block_id: &BlockHash,
            _bytes: &[u8],
        ) -> Result<(), String> {
            Ok(())
        }
    }

    pub type ConformanceResult = Result<(), ConformanceError>;

    #[derive(Debug)]
    pub enum ConformanceError {
        Store(BlockStoreError),
        Expectation(String),
    }

    impl fmt::Display for ConformanceError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Store(error) => write!(f, "{error}"),
                Self::Expectation(message) => {
                    write!(f, "conformance expectation failed: {message}")
                }
            }
        }
    }

    impl std::error::Error for ConformanceError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Store(error) => Some(error),
                Self::Expectation(_) => None,
            }
        }
    }

    impl From<BlockStoreError> for ConformanceError {
        fn from(value: BlockStoreError) -> Self {
            Self::Store(value)
        }
    }

    pub async fn run_contract_suite<F>(factory: &F) -> ConformanceResult
    where
        F: BlockStoreFactory,
    {
        run_round_trip_case(&factory.fresh_store().await).await?;
        run_idempotence_case(&factory.fresh_store().await).await?;
        run_missing_block_case(&factory.fresh_store().await).await?;
        run_enumeration_case(&factory.fresh_store().await).await?;
        Ok(())
    }

    pub async fn run_full_suite<F>(factory: &F) -> ConformanceResult
    where
        F: BlockStoreFactory,
    {
        run_contract_suite(factory).await
    }

    pub async fn run_round_trip_case(store: &impl BlockStore) -> ConformanceResult {
        let block = sample_leaf_block("hello");
        let serialized = serialize_block(&block).expect("sample block must serialize");
        store
            .put_block_bytes(&serialized.hash, &serialized.bytes)
            .await?;
        let loaded = store
            .get_block_bytes(&serialized.hash)
            .await?
            .ok_or_else(|| {
                ConformanceError::Expectation("expected stored block bytes to be present".into())
            })?;
        if loaded != serialized.bytes {
            return Err(ConformanceError::Expectation(
                "expected round-tripped bytes to remain unchanged".into(),
            ));
        }
        Ok(())
    }

    pub async fn run_idempotence_case(store: &impl BlockStore) -> ConformanceResult {
        let first = serialize_block(&sample_branch_block([0x11; 32], [0x22; 32], false))
            .expect("sample block must serialize");
        let second = serialize_block(&sample_branch_block([0x11; 32], [0x22; 32], true))
            .expect("sample block must serialize");

        store.put_block_bytes(&first.hash, &first.bytes).await?;
        store.put_block_bytes(&second.hash, &second.bytes).await?;

        if first.hash != second.hash {
            return Err(ConformanceError::Expectation(
                "expected logically identical blocks to share a hash".into(),
            ));
        }

        let loaded = store.get_block_bytes(&first.hash).await?.ok_or_else(|| {
            ConformanceError::Expectation("expected idempotently stored bytes to be present".into())
        })?;
        if loaded != first.bytes {
            return Err(ConformanceError::Expectation(
                "expected identical bytes after repeated puts".into(),
            ));
        }

        let enumerated = collect_block_ids(store.iter_block_ids()?).await?;
        let expected = HashSet::from([first.hash]);
        if enumerated != expected {
            return Err(ConformanceError::Expectation(format!(
                "expected enumeration after idempotent puts to yield {:?}, got {:?}",
                expected, enumerated
            )));
        }

        Ok(())
    }

    pub async fn run_missing_block_case(store: &impl BlockStore) -> ConformanceResult {
        let missing = BlockHash::from_bytes([0x55; 32]);
        let loaded = store.get_block_bytes(&missing).await?;
        if loaded.is_some() {
            return Err(ConformanceError::Expectation(format!(
                "expected missing block {missing} to return Ok(None)"
            )));
        }
        Ok(())
    }

    pub async fn run_enumeration_case(store: &impl BlockStore) -> ConformanceResult {
        let first =
            serialize_block(&sample_leaf_block("first")).expect("sample block must serialize");
        let second =
            serialize_block(&sample_leaf_block("second")).expect("sample block must serialize");
        let branch = serialize_block(&sample_branch_block([0x11; 32], [0x22; 32], false))
            .expect("sample block must serialize");

        for serialized in [&first, &second, &branch] {
            store
                .put_block_bytes(&serialized.hash, &serialized.bytes)
                .await?;
        }

        let expected = HashSet::from([first.hash, second.hash, branch.hash]);
        let enumerated = collect_block_ids(store.iter_block_ids()?).await?;

        if enumerated != expected {
            return Err(ConformanceError::Expectation(format!(
                "expected enumeration to yield {:?}, got {:?}",
                expected, enumerated
            )));
        }

        Ok(())
    }

    async fn collect_block_ids(
        iter: super::BlockIdStream<'_>,
    ) -> Result<HashSet<BlockHash>, ConformanceError> {
        iter.try_collect::<HashSet<_>>()
            .await
            .map_err(ConformanceError::from)
    }

    pub(super) fn sample_branch_block(
        first_child: [u8; 32],
        second_child: [u8; 32],
        reversed: bool,
    ) -> Block {
        let mut entries = vec![
            branch_entry(vec![0x01], first_child),
            branch_entry(vec![0x02], second_child),
        ];
        if reversed {
            entries.reverse();
        }

        Block::Branch(
            build_branch_block(VERSION_1, 1, embedding_spec("f16le"), entries, None).unwrap(),
        )
    }

    pub(super) fn sample_leaf_block(body: &str) -> Block {
        Block::Leaf(
            build_leaf_block(
                VERSION_1,
                embedding_spec("f32le"),
                vec![leaf_entry(vec![0xaa, 0xbb], body)],
                None,
            )
            .unwrap(),
        )
    }

    #[cfg(test)]
    pub(super) async fn persist_leaf_blocks_for_indexing(
        store: &dyn BlockStore,
        blocks: &[Block],
    ) -> Result<Vec<BlockHash>, BlockStoreError> {
        let mut persisted = Vec::with_capacity(blocks.len());
        for block in blocks {
            persisted.push(store.put(block).await?);
        }
        Ok(persisted)
    }

    #[cfg(test)]
    pub(super) async fn resolve_blocks_for_search(
        store: &dyn BlockStore,
        block_ids: &[BlockHash],
    ) -> Result<Vec<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        let mut resolved = Vec::with_capacity(block_ids.len());
        for block_id in block_ids {
            let bytes = store.get_block_bytes(block_id).await?.ok_or_else(|| {
                BlockStoreError::BackendFailure(format!("missing block {block_id}"))
            })?;
            resolved.push(
                lexongraph_block::deserialize_block(&bytes, block_id)
                    .map_err(BlockStoreError::DecodeFailure)?,
            );
        }
        Ok(resolved)
    }

    pub(super) fn embedding_spec(encoding: &str) -> EmbeddingSpec {
        EmbeddingSpec {
            dims: 2,
            encoding: encoding.to_string(),
        }
    }

    pub(super) fn branch_entry(embedding: Vec<u8>, child: [u8; 32]) -> BranchEntry {
        BranchEntry {
            embedding,
            child: BlockHash::from_bytes(child),
        }
    }

    pub(super) fn leaf_entry(embedding: Vec<u8>, body: &str) -> LeafEntry {
        LeafEntry {
            embedding,
            metadata: vec![],
            content: Content {
                media_type: "text/plain".into(),
                body: body.as_bytes().to_vec(),
            },
        }
    }
}

#[cfg(feature = "conformance")]
pub mod conformance {
    //! Opt-in helper APIs for validating downstream `BlockStore` implementations.
    //!
    //! Enable this module from test code with a dev-dependency such as:
    //!
    //! ```toml
    //! [dev-dependencies]
    //! lexongraph-block-store = { version = "*", features = ["conformance"] }
    //! ```

    pub use super::conformance_support::{
        BlockStoreConformanceHarness, BlockStoreFactory, ConformanceError, ConformanceResult,
        run_contract_suite, run_enumeration_case, run_full_suite, run_idempotence_case,
        run_missing_block_case, run_round_trip_case,
    };
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::{StreamExt, TryStreamExt, executor::block_on, stream};
    use lexongraph_block::{
        Block, BlockHash, VERSION_1, build_branch_block, deserialize_block, serialize_block,
    };

    use super::conformance_support::{
        BlockStoreFactory, branch_entry, embedding_spec, persist_leaf_blocks_for_indexing,
        resolve_blocks_for_search, run_enumeration_case, run_full_suite, run_idempotence_case,
        run_missing_block_case, run_round_trip_case, sample_branch_block, sample_leaf_block,
    };
    use super::{BlockStore, BlockStoreError};

    #[derive(Default)]
    struct MemoryBlockStore {
        blocks: Mutex<HashMap<BlockHash, Vec<u8>>>,
    }

    impl MemoryBlockStore {
        fn len(&self) -> usize {
            self.blocks.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl BlockStore for MemoryBlockStore {
        async fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            self.blocks
                .lock()
                .unwrap()
                .insert(*block_id, block_bytes.to_vec());
            Ok(())
        }

        async fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            Ok(self.blocks.lock().unwrap().get(block_id).cloned())
        }

        fn iter_block_ids(&self) -> Result<super::BlockIdStream<'_>, BlockStoreError> {
            let block_ids = self
                .blocks
                .lock()
                .unwrap()
                .keys()
                .copied()
                .collect::<Vec<_>>();
            Ok(Box::pin(stream::iter(block_ids.into_iter().map(Ok))))
        }
    }

    struct MemoryHarness;

    #[async_trait(?Send)]
    impl BlockStoreFactory for MemoryHarness {
        type Store = MemoryBlockStore;

        async fn fresh_store(&self) -> Self::Store {
            MemoryBlockStore::default()
        }
    }

    #[derive(Default)]
    struct HexKeyMemoryBlockStore {
        blocks: Mutex<HashMap<String, Vec<u8>>>,
    }

    #[async_trait]
    impl BlockStore for HexKeyMemoryBlockStore {
        async fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            self.blocks
                .lock()
                .unwrap()
                .insert(block_id.to_string(), block_bytes.to_vec());
            Ok(())
        }

        async fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            Ok(self
                .blocks
                .lock()
                .unwrap()
                .get(&block_id.to_string())
                .cloned())
        }

        fn iter_block_ids(&self) -> Result<super::BlockIdStream<'_>, BlockStoreError> {
            let block_ids = self
                .blocks
                .lock()
                .unwrap()
                .keys()
                .map(|block_id| {
                    let bytes = decode_block_hash_hex(block_id).ok_or_else(|| {
                        BlockStoreError::BackendFailure(format!(
                            "invalid hex block ID in memory store: {block_id}"
                        ))
                    })?;
                    Ok(BlockHash::from_bytes(bytes))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Box::pin(stream::iter(block_ids.into_iter().map(Ok))))
        }
    }

    #[derive(Default)]
    struct MidstreamFailingEnumerationStore {
        inner: MemoryBlockStore,
    }

    #[async_trait]
    impl BlockStore for MidstreamFailingEnumerationStore {
        async fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            self.inner.put_block_bytes(block_id, block_bytes).await
        }

        async fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            self.inner.get_block_bytes(block_id).await
        }

        fn iter_block_ids(&self) -> Result<super::BlockIdStream<'_>, BlockStoreError> {
            let block_ids = self
                .inner
                .blocks
                .lock()
                .unwrap()
                .keys()
                .copied()
                .collect::<Vec<_>>();
            let failure = BlockStoreError::BackendFailure("enumeration interrupted".into());
            Ok(Box::pin(stream::iter(
                block_ids
                    .into_iter()
                    .map(Ok)
                    .chain(std::iter::once(Err(failure))),
            )))
        }
    }

    #[test]
    fn val_store_001_put_then_get_round_trips_a_valid_block() {
        let store = MemoryBlockStore::default();

        block_on(run_round_trip_case(&store)).unwrap();
    }

    #[test]
    fn val_store_002_put_is_idempotent_for_logically_identical_blocks() {
        let store = MemoryBlockStore::default();

        block_on(run_idempotence_case(&store)).unwrap();
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn val_store_003_missing_blocks_return_ok_none() {
        let store = MemoryBlockStore::default();

        block_on(run_missing_block_case(&store)).unwrap();
    }

    #[test]
    fn val_store_006_indexing_consumers_can_persist_blocks_without_backend_specifics() {
        let store = MemoryBlockStore::default();
        let child_ids = block_on(persist_leaf_blocks_for_indexing(
            &store,
            &[sample_leaf_block("left"), sample_leaf_block("right")],
        ))
        .unwrap();

        let branch = build_branch_block(
            VERSION_1,
            1,
            embedding_spec("f16le"),
            vec![
                branch_entry(vec![0x02], child_ids[1].into_bytes()),
                branch_entry(vec![0x01], child_ids[0].into_bytes()),
            ],
            None,
        )
        .unwrap();
        let branch_id = block_on(store.put(&Block::Branch(branch))).unwrap();

        assert!(block_on(store.get(&branch_id)).unwrap().is_some());
    }

    #[test]
    fn val_store_007_search_consumers_can_resolve_root_and_child_blocks() {
        let store = MemoryBlockStore::default();
        let first_leaf = sample_leaf_block("left");
        let second_leaf = sample_leaf_block("right");
        let first_id = block_on(store.put(&first_leaf)).unwrap();
        let second_id = block_on(store.put(&second_leaf)).unwrap();

        let root = Block::Branch(
            build_branch_block(
                VERSION_1,
                1,
                embedding_spec("f16le"),
                vec![
                    branch_entry(vec![0x02], second_id.into_bytes()),
                    branch_entry(vec![0x01], first_id.into_bytes()),
                ],
                None,
            )
            .unwrap(),
        );
        let root_id = block_on(store.put(&root)).unwrap();

        let resolved = block_on(resolve_blocks_for_search(
            &store,
            &[root_id, first_id, second_id],
        ))
        .unwrap();

        assert_eq!(resolved.len(), 3);
        assert_eq!(resolved[0].hash, root_id);
        assert_eq!(resolved[1].hash, first_id);
        assert_eq!(resolved[2].hash, second_id);
    }

    #[test]
    fn val_store_008_same_contract_applies_to_multiple_backend_shapes() {
        let hash_store = MemoryBlockStore::default();
        let hex_store = HexKeyMemoryBlockStore::default();
        let block = sample_leaf_block("shared contract");

        let hash_id = block_on(store_and_reload(&hash_store, &block)).unwrap();
        let hex_id = block_on(store_and_reload(&hex_store, &block)).unwrap();

        assert_eq!(hash_id, hex_id);
    }

    #[test]
    fn val_store_009_internal_memory_store_supports_contract_tests() {
        block_on(run_full_suite(&MemoryHarness)).unwrap();
    }

    #[test]
    fn val_store_010_public_surface_is_limited_to_the_contract() {
        async fn uses_only_public_contract(
            store: &dyn BlockStore,
            block: &Block,
            block_id: &BlockHash,
        ) -> Result<(), BlockStoreError> {
            let serialized = serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
            store
                .put_block_bytes(&serialized.hash, &serialized.bytes)
                .await?;
            let _ = store.get_block_bytes(block_id).await?;
            let _ = store.iter_block_ids()?.try_collect::<Vec<_>>().await?;
            Ok(())
        }

        let store = MemoryBlockStore::default();
        let block = sample_leaf_block("public");
        let block_id = block_on(store.put(&block)).unwrap();

        block_on(uses_only_public_contract(&store, &block, &block_id)).unwrap();
    }

    #[test]
    fn val_store_013_enumeration_streams_the_stored_block_ids() {
        let store = MemoryBlockStore::default();

        block_on(run_enumeration_case(&store)).unwrap();
    }

    #[test]
    fn val_store_014_callers_can_classify_enumerated_ids_via_get() {
        let store = MemoryBlockStore::default();
        let leaf_id = block_on(store.put(&sample_leaf_block("leaf"))).unwrap();
        let branch = match sample_branch_block([0x22; 32], leaf_id.into_bytes(), false) {
            Block::Branch(branch) => branch,
            Block::Leaf(_) => unreachable!("sample_branch_block must return a branch block"),
        };
        let branch_id = block_on(store.put(&Block::Branch(branch))).unwrap();

        let enumerated = block_on(collect_block_ids(store.iter_block_ids().unwrap())).unwrap();

        assert_eq!(enumerated, HashSet::from([leaf_id, branch_id]));

        let mut leaf_count = 0;
        let mut branch_count = 0;
        for block_id in enumerated {
            let bytes = block_on(store.get_block_bytes(&block_id)).unwrap().unwrap();
            match deserialize_block(&bytes, &block_id).unwrap().block {
                Block::Leaf(_) => leaf_count += 1,
                Block::Branch(_) => branch_count += 1,
            }
        }

        assert_eq!((leaf_count, branch_count), (1, 1));
    }

    #[test]
    fn val_store_015_same_enumeration_contract_applies_to_multiple_backend_shapes() {
        let hash_store = MemoryBlockStore::default();
        let hex_store = HexKeyMemoryBlockStore::default();
        let leaf = sample_leaf_block("shared contract");
        let branch = sample_branch_block([0x33; 32], [0x44; 32], false);

        let expected = HashSet::from([
            block_on(hash_store.put(&leaf)).unwrap(),
            block_on(hash_store.put(&branch)).unwrap(),
        ]);
        block_on(hex_store.put(&leaf)).unwrap();
        block_on(hex_store.put(&branch)).unwrap();

        assert_eq!(
            block_on(collect_block_ids(hash_store.iter_block_ids().unwrap())).unwrap(),
            expected
        );
        assert_eq!(
            block_on(collect_block_ids(hex_store.iter_block_ids().unwrap())).unwrap(),
            expected
        );
    }

    #[test]
    fn val_store_016_enumeration_failures_are_explicit() {
        let store = MidstreamFailingEnumerationStore::default();
        let block_id = block_on(store.put(&sample_leaf_block("midstream"))).unwrap();
        let mut iter = store.iter_block_ids().unwrap();

        assert_eq!(block_on(iter.next()).unwrap().unwrap(), block_id);
        assert_eq!(
            block_on(iter.next()).unwrap().unwrap_err(),
            BlockStoreError::BackendFailure("enumeration interrupted".into())
        );
    }

    #[test]
    fn explicit_contract_violations_are_not_reported_as_backend_failures() {
        let store = MemoryBlockStore::default();
        let invalid = Block::Leaf(lexongraph_block::LeafBlock {
            version: VERSION_1,
            level: 0,
            embedding_spec: embedding_spec("f32le"),
            entries: vec![],
            ext: None,
        });

        let error = block_on(store.put(&invalid)).unwrap_err();

        assert!(matches!(error, BlockStoreError::ContractViolation(_)));
    }

    async fn store_and_reload(
        store: &dyn BlockStore,
        block: &Block,
    ) -> Result<BlockHash, BlockStoreError> {
        let block_id = store.put(block).await?;
        let loaded = store
            .get(&block_id)
            .await?
            .expect("stored block should be present");
        assert_eq!(loaded.block, *block);
        Ok(block_id)
    }

    async fn collect_block_ids(
        iter: super::BlockIdStream<'_>,
    ) -> Result<HashSet<BlockHash>, BlockStoreError> {
        iter.try_collect::<HashSet<_>>().await
    }

    fn decode_block_hash_hex(value: &str) -> Option<[u8; 32]> {
        if value.len() != BlockHash::LEN * 2 {
            return None;
        }

        let mut bytes = [0_u8; BlockHash::LEN];
        for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = decode_hex_nibble(chunk[0])?;
            let low = decode_hex_nibble(chunk[1])?;
            bytes[index] = (high << 4) | low;
        }

        Some(bytes)
    }

    fn decode_hex_nibble(value: u8) -> Option<u8> {
        match value {
            b'0'..=b'9' => Some(value - b'0'),
            b'a'..=b'f' => Some(value - b'a' + 10),
            _ => None,
        }
    }
}
