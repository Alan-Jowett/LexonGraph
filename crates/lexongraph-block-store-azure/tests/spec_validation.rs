// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
mod support;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use lexongraph_block::{
    BlockError, BlockHash, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_leaf_block,
    compute_block_hash, serialize_block,
};
use lexongraph_block_store::conformance::run_full_suite;
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_block_store_azure::AzureBlobBlockStore;
use reqwest::Url;

use support::{MockAzureServer, collect_block_ids};

#[test]
fn val_azure_store_001_002_014_constructor_and_publish_path_are_deterministic() {
    let server = MockAzureServer::start();
    let store = server.store();
    let block = sample_leaf_block("path");
    let serialized = serialize_block(&block).unwrap();

    let block_id = store.put(&block).unwrap();
    let expected_blob_name = server.blob_name(&block_id);
    let requests = server.recorded_requests();

    assert_eq!(block_id, serialized.hash);
    assert_eq!(
        server.blob_bytes(&expected_blob_name).unwrap(),
        serialized.bytes
    );
    assert!(
        requests
            .iter()
            .any(|request| request.method == "PUT" && request.target.contains(&expected_blob_name))
    );

    let mut blob_scoped = Url::parse(&server.sas_url()).unwrap();
    blob_scoped.set_path("/container/blob.cbor");
    let error = AzureBlobBlockStore::new(blob_scoped.as_str()).unwrap_err();
    expect_backend_failure_contains(error, "container root");

    let mut empty_query = Url::parse(&server.sas_url()).unwrap();
    empty_query.set_query(Some(""));
    let error = AzureBlobBlockStore::new(empty_query.as_str()).unwrap_err();
    expect_backend_failure_contains(error, "must include SAS query parameters");

    let mut non_sas_query = Url::parse(&server.sas_url()).unwrap();
    non_sas_query.set_query(Some("foo=bar"));
    let error = AzureBlobBlockStore::new(non_sas_query.as_str()).unwrap_err();
    expect_backend_failure_contains(error, "must include a non-empty SAS signature parameter");
}

#[test]
fn val_azure_store_003_005_round_trip_and_missing_blocks_match_the_contract() {
    let server = MockAzureServer::start();
    let store = server.store();
    let block = sample_leaf_block("round-trip");

    let block_id = store.put(&block).unwrap();
    let loaded = store.get(&block_id).unwrap().unwrap();

    assert_eq!(loaded.hash, block_id);
    assert_eq!(loaded.block, block);
    assert_eq!(store.get(&BlockHash::from_bytes([0x44; 32])).unwrap(), None);
}

#[test]
fn val_azure_store_006_007_015_get_reports_integrity_malformed_and_backend_failures() {
    let server = MockAzureServer::start();
    let store = server.store();

    let first = serialize_block(&sample_leaf_block("first")).unwrap();
    let second = serialize_block(&sample_leaf_block("second")).unwrap();
    server.insert_blob(server.blob_name(&second.hash), first.bytes.clone());

    assert_eq!(
        store.get(&second.hash).unwrap_err(),
        BlockStoreError::IntegrityMismatch {
            expected: second.hash,
            actual: first.hash,
        }
    );

    let malformed_bytes = [0xff, 0xff, 0x00];
    let malformed_hash = compute_block_hash(&malformed_bytes);
    let malformed_blob = server.blob_name(&malformed_hash);
    server.insert_blob(&malformed_blob, malformed_bytes.to_vec());

    assert!(matches!(
        store.get(&malformed_hash).unwrap_err(),
        BlockStoreError::MalformedContent(BlockError::MalformedCbor(_))
    ));

    let unreadable = serialize_block(&sample_leaf_block("forbidden")).unwrap();
    let unreadable_blob = server.blob_name(&unreadable.hash);
    server.insert_blob(&unreadable_blob, unreadable.bytes);
    server.set_blob_status(&unreadable_blob, 403);
    expect_backend_failure_contains(store.get(&unreadable.hash).unwrap_err(), "HTTP 403");
}

#[test]
fn val_azure_store_004_008_009_put_handles_idempotence_permissions_and_conflicts() {
    let server = MockAzureServer::start();
    let store = server.store();
    let block = sample_leaf_block("shared");
    let serialized = serialize_block(&block).unwrap();
    let blob_name = server.blob_name(&serialized.hash);

    assert_eq!(store.put(&block).unwrap(), serialized.hash);
    assert_eq!(store.put(&block).unwrap(), serialized.hash);

    let permission_server = MockAzureServer::start();
    permission_server.set_deny_put(true);
    expect_backend_failure_contains(
        permission_server.store().put(&block).unwrap_err(),
        "HTTP 403",
    );

    let conflict_server = MockAzureServer::start();
    conflict_server.insert_blob(blob_name.clone(), b"not canonical bytes".to_vec());
    expect_backend_failure_contains(
        conflict_server.store().put(&block).unwrap_err(),
        "integrity conflict",
    );
    assert_eq!(
        conflict_server.blob_bytes(&blob_name).unwrap(),
        b"not canonical bytes".to_vec()
    );
}

#[test]
fn val_azure_store_004_concurrent_publishers_converge_on_one_valid_blob() {
    let server = MockAzureServer::start();
    let store = server.store();
    let block = Arc::new(sample_leaf_block("shared"));
    let expected_hash = serialize_block(block.as_ref()).unwrap().hash;
    let mut threads = Vec::new();

    for _ in 0..6 {
        let store = store.clone();
        let block = Arc::clone(&block);
        threads.push(thread::spawn(move || store.put(block.as_ref())));
    }

    for result in threads {
        assert_eq!(result.join().unwrap().unwrap(), expected_hash);
    }

    let loaded = server.store().get(&expected_hash).unwrap().unwrap();
    assert_eq!(loaded.hash, expected_hash);
    assert_eq!(loaded.block, *block);
}

#[test]
fn val_azure_store_010_parent_conformance_requirements_are_realized_by_tests() {
    #[derive(Default)]
    struct Harness {
        servers: Mutex<Vec<MockAzureServer>>,
    }

    #[derive(Clone)]
    struct HarnessStore {
        inner: AzureBlobBlockStore,
        server: MockAzureServer,
    }

    impl BlockStore for HarnessStore {
        fn put(&self, block: &lexongraph_block::Block) -> Result<BlockHash, BlockStoreError> {
            self.inner.put(block)
        }

        fn get(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
            self.inner.get(block_id)
        }

        fn iter_block_ids(
            &self,
        ) -> Result<lexongraph_block_store::BlockIdIterator<'_>, BlockStoreError> {
            self.inner.iter_block_ids()
        }
    }

    impl lexongraph_block_store::conformance::BlockStoreFactory for Harness {
        type Store = HarnessStore;

        fn fresh_store(&self) -> Self::Store {
            let server = MockAzureServer::start();
            let store = HarnessStore {
                inner: server.store(),
                server: server.clone(),
            };
            self.servers.lock().unwrap().push(server);
            store
        }
    }

    impl lexongraph_block_store::conformance::BlockStoreConformanceHarness for Harness {
        fn inject_raw_bytes(
            &self,
            store: &Self::Store,
            block_id: &BlockHash,
            bytes: &[u8],
        ) -> Result<(), String> {
            store
                .server
                .insert_blob(store.server.blob_name(block_id), bytes.to_vec());
            Ok(())
        }
    }

    run_full_suite(&Harness::default()).unwrap();
}

#[test]
fn val_azure_store_011_012_enumeration_yields_only_recognized_block_ids() {
    let server = MockAzureServer::start();
    let store = server.store();
    let first = sample_leaf_block("first");
    let second = sample_leaf_block("second");

    let expected = HashSet::from([store.put(&first).unwrap(), store.put(&second).unwrap()]);
    server.add_extra_list_name("notes/readme.txt");
    server.add_extra_list_name("aa/bb/temporary.part");

    let enumerated = collect_block_ids(store.iter_block_ids().unwrap()).unwrap();

    assert_eq!(enumerated, expected);
}

#[test]
fn val_azure_store_013_enumeration_surfaces_listing_and_decoding_failures() {
    let list_error_server = MockAzureServer::start();
    list_error_server.set_list_error(500);
    match list_error_server.store().iter_block_ids() {
        Err(error) => expect_backend_failure_contains(error, "HTTP 500"),
        Ok(iter) => {
            let error = iter.collect::<Result<Vec<_>, _>>().unwrap_err();
            expect_backend_failure_contains(error, "HTTP 500");
        }
    }

    let decode_error_server = MockAzureServer::start();
    decode_error_server.add_extra_list_name("aa/bb/not-a-block-id.cbor");
    match decode_error_server.store().iter_block_ids() {
        Err(error) => expect_backend_failure_contains(
            error,
            "failed to decode an enumerated block ID candidate at blob aa/bb/not-a-block-id.cbor",
        ),
        Ok(iter) => {
            let error = iter.collect::<Result<Vec<_>, _>>().unwrap_err();
            expect_backend_failure_contains(
                error,
                "failed to decode an enumerated block ID candidate at blob aa/bb/not-a-block-id.cbor",
            );
        }
    }
}

#[test]
fn malformed_listing_xml_is_an_explicit_backend_failure() {
    let server = MockAzureServer::start();
    server.set_malformed_listing(true);
    match server.store().iter_block_ids() {
        Err(error) => {
            expect_backend_failure_contains(error, "failed to decode Azure listing response")
        }
        Ok(iter) => {
            let error = iter.collect::<Result<Vec<_>, _>>().unwrap_err();
            expect_backend_failure_contains(error, "failed to decode Azure listing response");
        }
    }
}

fn sample_leaf_block(body: &str) -> lexongraph_block::Block {
    lexongraph_block::Block::Leaf(
        build_leaf_block(
            VERSION_1,
            EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            vec![LeafEntry {
                embedding: vec![0xaa, 0xbb],
                metadata: vec![],
                content: Content {
                    media_type: "text/plain".into(),
                    body: body.as_bytes().to_vec(),
                },
            }],
            None,
        )
        .unwrap(),
    )
}

fn expect_backend_failure_contains(error: BlockStoreError, expected: &str) {
    match error {
        BlockStoreError::BackendFailure(message) => {
            assert!(
                message.contains(expected),
                "expected backend failure containing {expected:?}, got {message:?}"
            );
        }
        other => panic!("expected backend failure containing {expected:?}, got {other:?}"),
    }
}
