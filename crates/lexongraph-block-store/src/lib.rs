//! Backend-agnostic storage contract for LexonGraph blocks.
//!
//! ```
//! use lexongraph_block::{Block, BlockHash, ValidatedBlock};
//! use lexongraph_block_store::{BlockStore, BlockStoreError};
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

use lexongraph_block::{Block, BlockError, BlockHash, ValidatedBlock};

pub trait BlockStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError>;

    fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockStoreError {
    BackendFailure(String),
    MalformedContent(BlockError),
    IntegrityMismatch {
        expected: BlockHash,
        actual: BlockHash,
    },
    ContractViolation(BlockError),
}

impl fmt::Display for BlockStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BackendFailure(message) => write!(f, "block store backend failure: {message}"),
            Self::MalformedContent(error) => write!(f, "malformed stored block content: {error}"),
            Self::IntegrityMismatch { expected, actual } => {
                write!(
                    f,
                    "stored block identity mismatch: expected {expected}, got {actual}"
                )
            }
            Self::ContractViolation(error) => write!(f, "block store contract violation: {error}"),
        }
    }
}

impl std::error::Error for BlockStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MalformedContent(error) | Self::ContractViolation(error) => Some(error),
            Self::BackendFailure(_) | Self::IntegrityMismatch { .. } => None,
        }
    }
}

#[cfg(any(test, feature = "conformance"))]
mod conformance_support {
    use std::fmt;

    use lexongraph_block::{
        Block, BlockHash, BranchEntry, Content, EmbeddingSpec, LeafEntry, VERSION_1,
        ValidatedBlock, build_branch_block, build_leaf_block, compute_block_hash,
    };

    use super::{BlockStore, BlockStoreError};

    pub trait BlockStoreFactory {
        type Store: BlockStore;

        fn fresh_store(&self) -> Self::Store;
    }

    pub trait BlockStoreConformanceHarness: BlockStoreFactory {
        fn inject_raw_bytes(
            &self,
            store: &Self::Store,
            block_id: &BlockHash,
            bytes: &[u8],
        ) -> Result<(), String>;
    }

    pub type ConformanceResult = Result<(), ConformanceError>;

    #[derive(Debug)]
    pub enum ConformanceError {
        Store(BlockStoreError),
        Injection(String),
        Expectation(String),
    }

    impl fmt::Display for ConformanceError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Store(error) => write!(f, "{error}"),
                Self::Injection(message) => write!(f, "conformance injection failed: {message}"),
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
                Self::Injection(_) | Self::Expectation(_) => None,
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
        Ok(())
    }

    pub fn run_integrity_suite<H>(harness: &H) -> ConformanceResult
    where
        H: BlockStoreConformanceHarness,
    {
        run_integrity_mismatch_case(harness)?;
        run_malformed_content_case(harness)?;
        Ok(())
    }

    pub fn run_full_suite<H>(harness: &H) -> ConformanceResult
    where
        H: BlockStoreConformanceHarness,
    {
        run_contract_suite(harness)?;
        run_integrity_suite(harness)
    }

    pub fn run_round_trip_case(store: &impl BlockStore) -> ConformanceResult {
        let block = sample_leaf_block("hello");
        let block_id = store.put(&block)?;
        let loaded = store.get(&block_id)?.ok_or_else(|| {
            ConformanceError::Expectation("expected stored block to be present".into())
        })?;

        expect_loaded_block(&loaded, &block, &block_id)
    }

    pub fn run_idempotence_case(store: &impl BlockStore) -> ConformanceResult {
        let first = sample_branch_block([0x11; 32], [0x22; 32], false);
        let second = sample_branch_block([0x11; 32], [0x22; 32], true);

        let first_id = store.put(&first)?;
        let second_id = store.put(&second)?;
        if first_id != second_id {
            return Err(ConformanceError::Expectation(format!(
                "expected logically identical blocks to share a hash, got {first_id} and {second_id}"
            )));
        }

        let loaded = store.get(&first_id)?.ok_or_else(|| {
            ConformanceError::Expectation(
                "expected idempotently stored block to remain present".into(),
            )
        })?;

        expect_loaded_block(&loaded, &first, &first_id)
    }

    pub fn run_missing_block_case(store: &impl BlockStore) -> ConformanceResult {
        let missing = BlockHash::from_bytes([0x55; 32]);
        let loaded = store.get(&missing)?;
        if loaded.is_some() {
            return Err(ConformanceError::Expectation(format!(
                "expected missing block {missing} to return Ok(None)"
            )));
        }
        Ok(())
    }

    pub fn run_integrity_mismatch_case<H>(harness: &H) -> ConformanceResult
    where
        H: BlockStoreConformanceHarness,
    {
        let store = harness.fresh_store();
        let first = lexongraph_block::serialize_block(&sample_leaf_block("first"))
            .map_err(BlockStoreError::ContractViolation)?;
        let second = lexongraph_block::serialize_block(&sample_leaf_block("second"))
            .map_err(BlockStoreError::ContractViolation)?;
        harness
            .inject_raw_bytes(&store, &second.hash, &first.bytes)
            .map_err(ConformanceError::Injection)?;

        match store.get(&second.hash) {
            Err(BlockStoreError::IntegrityMismatch { expected, actual })
                if expected == second.hash && actual == first.hash =>
            {
                Ok(())
            }
            Err(other) => Err(ConformanceError::Expectation(format!(
                "expected IntegrityMismatch({}, {}), got {other}",
                second.hash, first.hash
            ))),
            Ok(result) => Err(ConformanceError::Expectation(format!(
                "expected integrity mismatch error, got {result:?}"
            ))),
        }
    }

    pub fn run_malformed_content_case<H>(harness: &H) -> ConformanceResult
    where
        H: BlockStoreConformanceHarness,
    {
        let store = harness.fresh_store();
        let malformed_bytes = [0xff, 0xff, 0x00];
        let block_id = compute_block_hash(&malformed_bytes);
        harness
            .inject_raw_bytes(&store, &block_id, &malformed_bytes)
            .map_err(ConformanceError::Injection)?;

        match store.get(&block_id) {
            Err(BlockStoreError::MalformedContent(_)) => Ok(()),
            Err(other) => Err(ConformanceError::Expectation(format!(
                "expected MalformedContent for {block_id}, got {other}"
            ))),
            Ok(result) => Err(ConformanceError::Expectation(format!(
                "expected malformed-content error, got {result:?}"
            ))),
        }
    }

    fn expect_loaded_block(
        loaded: &ValidatedBlock,
        expected_block: &Block,
        expected_hash: &BlockHash,
    ) -> ConformanceResult {
        if loaded.hash != *expected_hash {
            return Err(ConformanceError::Expectation(format!(
                "expected loaded hash {expected_hash}, got {}",
                loaded.hash
            )));
        }
        if loaded.block != *expected_block {
            return Err(ConformanceError::Expectation(
                "expected loaded block to preserve logical meaning".into(),
            ));
        }
        Ok(())
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
            build_branch_block(VERSION_1, embedding_spec("f16le"), entries, None).unwrap(),
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
    ) -> Result<Vec<ValidatedBlock>, BlockStoreError> {
        block_ids
            .iter()
            .map(|block_id| {
                store.get(block_id)?.ok_or_else(|| {
                    BlockStoreError::BackendFailure(format!("missing block {block_id}"))
                })
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
        run_contract_suite, run_full_suite, run_idempotence_case, run_integrity_mismatch_case,
        run_integrity_suite, run_malformed_content_case, run_missing_block_case,
        run_round_trip_case,
    };
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;

    use lexongraph_block::{Block, BlockHash, VERSION_1, build_branch_block};

    use super::conformance_support::{
        BlockStoreConformanceHarness, BlockStoreFactory, branch_entry, embedding_spec,
        persist_leaf_blocks_for_indexing, resolve_blocks_for_search, run_full_suite,
        run_idempotence_case, run_integrity_mismatch_case, run_malformed_content_case,
        run_missing_block_case, run_round_trip_case, sample_leaf_block,
    };
    use super::{BlockStore, BlockStoreError};

    #[derive(Default)]
    struct MemoryBlockStore {
        blocks: RefCell<HashMap<BlockHash, Vec<u8>>>,
    }

    impl MemoryBlockStore {
        fn raw_insert(&self, hash: BlockHash, bytes: Vec<u8>) {
            self.blocks.borrow_mut().insert(hash, bytes);
        }

        fn len(&self) -> usize {
            self.blocks.borrow().len()
        }
    }

    impl BlockStore for MemoryBlockStore {
        fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
            let serialized = lexongraph_block::serialize_block(block)
                .map_err(BlockStoreError::ContractViolation)?;
            self.blocks
                .borrow_mut()
                .insert(serialized.hash, serialized.bytes);
            Ok(serialized.hash)
        }

        fn get(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
            let Some(bytes) = self.blocks.borrow().get(block_id).cloned() else {
                return Ok(None);
            };

            lexongraph_block::deserialize_block(&bytes, block_id)
                .map(Some)
                .map_err(map_get_error)
        }
    }

    struct MemoryHarness;

    impl BlockStoreFactory for MemoryHarness {
        type Store = MemoryBlockStore;

        fn fresh_store(&self) -> Self::Store {
            MemoryBlockStore::default()
        }
    }

    impl BlockStoreConformanceHarness for MemoryHarness {
        fn inject_raw_bytes(
            &self,
            store: &Self::Store,
            block_id: &BlockHash,
            bytes: &[u8],
        ) -> Result<(), String> {
            store.raw_insert(*block_id, bytes.to_vec());
            Ok(())
        }
    }

    #[derive(Default)]
    struct HexKeyMemoryBlockStore {
        blocks: RefCell<HashMap<String, Vec<u8>>>,
    }

    impl BlockStore for HexKeyMemoryBlockStore {
        fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
            let serialized = lexongraph_block::serialize_block(block)
                .map_err(BlockStoreError::ContractViolation)?;
            self.blocks
                .borrow_mut()
                .insert(serialized.hash.to_string(), serialized.bytes);
            Ok(serialized.hash)
        }

        fn get(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
            let Some(bytes) = self.blocks.borrow().get(&block_id.to_string()).cloned() else {
                return Ok(None);
            };

            lexongraph_block::deserialize_block(&bytes, block_id)
                .map(Some)
                .map_err(map_get_error)
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
    fn val_store_004_hash_mismatch_is_reported_as_an_integrity_error() {
        run_integrity_mismatch_case(&MemoryHarness).unwrap();
    }

    #[test]
    fn val_store_005_malformed_content_is_reported_explicitly() {
        run_malformed_content_case(&MemoryHarness).unwrap();
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
            let _ = store.put(block)?;
            let _ = store.get(block_id)?;
            Ok(())
        }

        let store = MemoryBlockStore::default();
        let block = sample_leaf_block("public");
        let block_id = store.put(&block).unwrap();

        uses_only_public_contract(&store, &block, &block_id).unwrap();
    }

    #[test]
    fn explicit_contract_violations_are_not_reported_as_backend_failures() {
        let store = MemoryBlockStore::default();
        let invalid = Block::Leaf(lexongraph_block::LeafBlock {
            version: VERSION_1,
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

    fn map_get_error(error: lexongraph_block::BlockError) -> BlockStoreError {
        match error {
            lexongraph_block::BlockError::HashMismatch { expected, actual } => {
                BlockStoreError::IntegrityMismatch { expected, actual }
            }
            other => BlockStoreError::MalformedContent(other),
        }
    }
}
