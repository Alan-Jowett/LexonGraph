// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Backend-agnostic storage contract for LexonGraph block bytes.
//!
//! ```
//! use lexongraph_block::{Block, BlockHash, ValidatedBlock};
//! use lexongraph_block_store::{BlockStore, BlockStoreError, BlockStoreExt};
//!
//! fn exercise_contract(
//!     store: &dyn BlockStore,
//!     block: &Block,
//! ) -> Result<Option<ValidatedBlock>, BlockStoreError> {
//!     let block_id = store.put(block)?;
//!     store.get(&block_id)
//! }
//! ```
//!
//! ```compile_fail
//! use lexongraph_block_store::MemoryBlockStore;
//!
//! let _ = MemoryBlockStore::default();
//! ```

use std::fmt;

use lexongraph_block::{
    Block, BlockError, BlockHash, DecodedBlock, ValidatedBlock, VersionedBlock, deserialize_block,
    deserialize_versioned_block, serialize_block, serialize_versioned_block,
};

pub trait BlockStore {
    fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError>;

    fn get_block_bytes(&self, block_id: &BlockHash) -> Result<Option<Vec<u8>>, BlockStoreError>;

    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        let serialized = serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
        self.put_block_bytes(&serialized.hash, &serialized.bytes)?;
        Ok(serialized.hash)
    }

    fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        let Some(bytes) = self.get_block_bytes(block_id)? else {
            return Ok(None);
        };
        deserialize_block(&bytes, block_id)
            .map(Some)
            .map_err(BlockStoreError::DecodeFailure)
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError>;
}

pub trait BlockStoreExt: BlockStore {
    fn put_versioned(&self, block: &VersionedBlock) -> Result<BlockHash, BlockStoreError> {
        let serialized =
            serialize_versioned_block(block).map_err(BlockStoreError::ContractViolation)?;
        self.put_block_bytes(&serialized.hash, &serialized.bytes)?;
        Ok(serialized.hash)
    }

    fn get_decoded(&self, block_id: &BlockHash) -> Result<Option<DecodedBlock>, BlockStoreError> {
        let Some(bytes) = self.get_block_bytes(block_id)? else {
            return Ok(None);
        };

        deserialize_versioned_block(&bytes, block_id)
            .map(Some)
            .map_err(BlockStoreError::DecodeFailure)
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

pub type BlockIdIterator<'a> = Box<dyn Iterator<Item = Result<BlockHash, BlockStoreError>> + 'a>;

#[cfg(any(test, feature = "conformance"))]
mod conformance_support {
    use std::collections::HashSet;
    use std::fmt;

    use lexongraph_block::{
        Block, BlockHash, BranchEntry, Content, EmbeddingSpec, LeafEntry, VERSION_1,
        build_branch_block, build_leaf_block, serialize_block,
    };

    use super::{BlockStore, BlockStoreError};

    pub trait BlockStoreFactory {
        type Store: BlockStore;

        fn fresh_store(&self) -> Self::Store;
    }

    #[allow(dead_code)]
    pub trait BlockStoreConformanceHarness: BlockStoreFactory {
        fn inject_raw_bytes(
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

    pub fn run_contract_suite<F>(factory: &F) -> ConformanceResult
    where
        F: BlockStoreFactory,
    {
        run_round_trip_case(&factory.fresh_store())?;
        run_idempotence_case(&factory.fresh_store())?;
        run_missing_block_case(&factory.fresh_store())?;
        run_enumeration_case(&factory.fresh_store())?;
        Ok(())
    }

    pub fn run_full_suite<F>(factory: &F) -> ConformanceResult
    where
        F: BlockStoreFactory,
    {
        run_contract_suite(factory)
    }

    pub fn run_round_trip_case(store: &impl BlockStore) -> ConformanceResult {
        let block = sample_leaf_block("hello");
        let serialized = serialize_block(&block).expect("sample block must serialize");
        store.put_block_bytes(&serialized.hash, &serialized.bytes)?;
        let loaded = store.get_block_bytes(&serialized.hash)?.ok_or_else(|| {
            ConformanceError::Expectation("expected stored block bytes to be present".into())
        })?;
        if loaded != serialized.bytes {
            return Err(ConformanceError::Expectation(
                "expected round-tripped bytes to remain unchanged".into(),
            ));
        }
        Ok(())
    }

    pub fn run_idempotence_case(store: &impl BlockStore) -> ConformanceResult {
        let first = serialize_block(&sample_branch_block([0x11; 32], [0x22; 32], false))
            .expect("sample block must serialize");
        let second = serialize_block(&sample_branch_block([0x11; 32], [0x22; 32], true))
            .expect("sample block must serialize");

        store.put_block_bytes(&first.hash, &first.bytes)?;
        store.put_block_bytes(&second.hash, &second.bytes)?;

        if first.hash != second.hash {
            return Err(ConformanceError::Expectation(
                "expected logically identical blocks to share a hash".into(),
            ));
        }

        let loaded = store.get_block_bytes(&first.hash)?.ok_or_else(|| {
            ConformanceError::Expectation("expected idempotently stored bytes to be present".into())
        })?;
        if loaded != first.bytes {
            return Err(ConformanceError::Expectation(
                "expected identical bytes after repeated puts".into(),
            ));
        }

        let enumerated = collect_block_ids(store.iter_block_ids()?)?;
        let expected = HashSet::from([first.hash]);
        if enumerated != expected {
            return Err(ConformanceError::Expectation(format!(
                "expected enumeration after idempotent puts to yield {:?}, got {:?}",
                expected, enumerated
            )));
        }

        Ok(())
    }

    pub fn run_missing_block_case(store: &impl BlockStore) -> ConformanceResult {
        let missing = BlockHash::from_bytes([0x55; 32]);
        let loaded = store.get_block_bytes(&missing)?;
        if loaded.is_some() {
            return Err(ConformanceError::Expectation(format!(
                "expected missing block {missing} to return Ok(None)"
            )));
        }
        Ok(())
    }

    pub fn run_enumeration_case(store: &impl BlockStore) -> ConformanceResult {
        let first =
            serialize_block(&sample_leaf_block("first")).expect("sample block must serialize");
        let second =
            serialize_block(&sample_leaf_block("second")).expect("sample block must serialize");
        let branch = serialize_block(&sample_branch_block([0x11; 32], [0x22; 32], false))
            .expect("sample block must serialize");

        for serialized in [&first, &second, &branch] {
            store.put_block_bytes(&serialized.hash, &serialized.bytes)?;
        }

        let expected = HashSet::from([first.hash, second.hash, branch.hash]);
        let enumerated = collect_block_ids(store.iter_block_ids()?)?;

        if enumerated != expected {
            return Err(ConformanceError::Expectation(format!(
                "expected enumeration to yield {:?}, got {:?}",
                expected, enumerated
            )));
        }

        Ok(())
    }

    fn collect_block_ids(
        iter: super::BlockIdIterator<'_>,
    ) -> Result<HashSet<BlockHash>, ConformanceError> {
        iter.collect::<Result<HashSet<_>, _>>()
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
    pub(super) fn persist_leaf_blocks_for_indexing(
        store: &dyn BlockStore,
        blocks: &[Block],
    ) -> Result<Vec<BlockHash>, BlockStoreError> {
        blocks.iter().map(|block| store.put(block)).collect()
    }

    #[cfg(test)]
    pub(super) fn resolve_blocks_for_search(
        store: &dyn BlockStore,
        block_ids: &[BlockHash],
    ) -> Result<Vec<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        block_ids
            .iter()
            .map(|block_id| {
                let bytes = store.get_block_bytes(block_id)?.ok_or_else(|| {
                    BlockStoreError::BackendFailure(format!("missing block {block_id}"))
                })?;
                lexongraph_block::deserialize_block(&bytes, block_id)
                    .map_err(BlockStoreError::DecodeFailure)
            })
            .collect()
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
    use std::cell::RefCell;
    use std::collections::{HashMap, HashSet};

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
        blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
    }

    impl MemoryBlockStore {
        fn len(&self) -> usize {
            self.blocks.borrow().len()
        }
    }

    impl BlockStore for MemoryBlockStore {
        fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            self.blocks
                .borrow_mut()
                .insert(*block_id, block_bytes.to_vec());
            Ok(())
        }

        fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            Ok(self.blocks.borrow().get(block_id).cloned())
        }

        fn iter_block_ids(&self) -> Result<super::BlockIdIterator<'_>, BlockStoreError> {
            let block_ids = self.blocks.borrow().keys().copied().collect::<Vec<_>>();
            Ok(Box::new(block_ids.into_iter().map(Ok)))
        }
    }

    struct MemoryHarness;

    impl BlockStoreFactory for MemoryHarness {
        type Store = MemoryBlockStore;

        fn fresh_store(&self) -> Self::Store {
            MemoryBlockStore::default()
        }
    }

    #[derive(Default)]
    struct HexKeyMemoryBlockStore {
        blocks: RefCell<HashMap<String, Vec<u8>>>,
    }

    impl BlockStore for HexKeyMemoryBlockStore {
        fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            self.blocks
                .borrow_mut()
                .insert(block_id.to_string(), block_bytes.to_vec());
            Ok(())
        }

        fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            Ok(self.blocks.borrow().get(&block_id.to_string()).cloned())
        }

        fn iter_block_ids(&self) -> Result<super::BlockIdIterator<'_>, BlockStoreError> {
            let block_ids = self
                .blocks
                .borrow()
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
            Ok(Box::new(block_ids.into_iter().map(Ok)))
        }
    }

    #[derive(Default)]
    struct MidstreamFailingEnumerationStore {
        inner: MemoryBlockStore,
    }

    impl BlockStore for MidstreamFailingEnumerationStore {
        fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            self.inner.put_block_bytes(block_id, block_bytes)
        }

        fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            self.inner.get_block_bytes(block_id)
        }

        fn iter_block_ids(&self) -> Result<super::BlockIdIterator<'_>, BlockStoreError> {
            let block_ids = self
                .inner
                .blocks
                .borrow()
                .keys()
                .copied()
                .collect::<Vec<_>>();
            let failure = BlockStoreError::BackendFailure("enumeration interrupted".into());
            Ok(Box::new(
                block_ids
                    .into_iter()
                    .map(Ok)
                    .chain(std::iter::once(Err(failure))),
            ))
        }
    }

    #[test]
    fn val_store_001_put_then_get_round_trips_a_valid_block() {
        let store = MemoryBlockStore::default();

        run_round_trip_case(&store).unwrap();
    }

    #[test]
    fn val_store_002_put_is_idempotent_for_logically_identical_blocks() {
        let store = MemoryBlockStore::default();

        run_idempotence_case(&store).unwrap();
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn val_store_003_missing_blocks_return_ok_none() {
        let store = MemoryBlockStore::default();

        run_missing_block_case(&store).unwrap();
    }

    #[test]
    fn val_store_006_indexing_consumers_can_persist_blocks_without_backend_specifics() {
        let store = MemoryBlockStore::default();
        let child_ids = persist_leaf_blocks_for_indexing(
            &store,
            &[sample_leaf_block("left"), sample_leaf_block("right")],
        )
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
        let branch_id = store.put(&Block::Branch(branch)).unwrap();

        assert!(store.get(&branch_id).unwrap().is_some());
    }

    #[test]
    fn val_store_007_search_consumers_can_resolve_root_and_child_blocks() {
        let store = MemoryBlockStore::default();
        let first_leaf = sample_leaf_block("left");
        let second_leaf = sample_leaf_block("right");
        let first_id = store.put(&first_leaf).unwrap();
        let second_id = store.put(&second_leaf).unwrap();

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
        let root_id = store.put(&root).unwrap();

        let resolved = resolve_blocks_for_search(&store, &[root_id, first_id, second_id]).unwrap();

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

        let hash_id = store_and_reload(&hash_store, &block).unwrap();
        let hex_id = store_and_reload(&hex_store, &block).unwrap();

        assert_eq!(hash_id, hex_id);
    }

    #[test]
    fn val_store_009_internal_memory_store_supports_contract_tests() {
        run_full_suite(&MemoryHarness).unwrap();
    }

    #[test]
    fn val_store_010_public_surface_is_limited_to_the_contract() {
        fn uses_only_public_contract(
            store: &dyn BlockStore,
            block: &Block,
            block_id: &BlockHash,
        ) -> Result<(), BlockStoreError> {
            let serialized = serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
            store.put_block_bytes(&serialized.hash, &serialized.bytes)?;
            let _ = store.get_block_bytes(block_id)?;
            let _ = store.iter_block_ids()?.collect::<Result<Vec<_>, _>>()?;
            Ok(())
        }

        let store = MemoryBlockStore::default();
        let block = sample_leaf_block("public");
        let block_id = store.put(&block).unwrap();

        uses_only_public_contract(&store, &block, &block_id).unwrap();
    }

    #[test]
    fn val_store_013_enumeration_streams_the_stored_block_ids() {
        let store = MemoryBlockStore::default();

        run_enumeration_case(&store).unwrap();
    }

    #[test]
    fn val_store_014_callers_can_classify_enumerated_ids_via_get() {
        let store = MemoryBlockStore::default();
        let leaf_id = store.put(&sample_leaf_block("leaf")).unwrap();
        let branch = match sample_branch_block([0x22; 32], leaf_id.into_bytes(), false) {
            Block::Branch(branch) => branch,
            Block::Leaf(_) => unreachable!("sample_branch_block must return a branch block"),
        };
        let branch_id = store.put(&Block::Branch(branch)).unwrap();

        let enumerated = collect_block_ids(store.iter_block_ids().unwrap()).unwrap();

        assert_eq!(enumerated, HashSet::from([leaf_id, branch_id]));

        let mut leaf_count = 0;
        let mut branch_count = 0;
        for block_id in enumerated {
            let bytes = store.get_block_bytes(&block_id).unwrap().unwrap();
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
            hash_store.put(&leaf).unwrap(),
            hash_store.put(&branch).unwrap(),
        ]);
        hex_store.put(&leaf).unwrap();
        hex_store.put(&branch).unwrap();

        assert_eq!(
            collect_block_ids(hash_store.iter_block_ids().unwrap()).unwrap(),
            expected
        );
        assert_eq!(
            collect_block_ids(hex_store.iter_block_ids().unwrap()).unwrap(),
            expected
        );
    }

    #[test]
    fn val_store_016_enumeration_failures_are_explicit() {
        let store = MidstreamFailingEnumerationStore::default();
        let block_id = store.put(&sample_leaf_block("midstream")).unwrap();
        let mut iter = store.iter_block_ids().unwrap();

        assert_eq!(iter.next().unwrap().unwrap(), block_id);
        assert_eq!(
            iter.next().unwrap().unwrap_err(),
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

        let error = store.put(&invalid).unwrap_err();

        assert!(matches!(error, BlockStoreError::ContractViolation(_)));
    }

    fn store_and_reload(
        store: &dyn BlockStore,
        block: &Block,
    ) -> Result<BlockHash, BlockStoreError> {
        let block_id = store.put(block)?;
        let loaded = store
            .get(&block_id)?
            .expect("stored block should be present");
        assert_eq!(loaded.block, *block);
        Ok(block_id)
    }

    fn collect_block_ids(
        iter: super::BlockIdIterator<'_>,
    ) -> Result<HashSet<BlockHash>, BlockStoreError> {
        iter.collect::<Result<HashSet<_>, _>>()
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
