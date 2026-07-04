// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
mod support;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use azure_core::http::Url;
use lexongraph_block::{
    BlockError, BlockHash, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_leaf_block,
    compute_block_hash, serialize_block,
};
use lexongraph_block_store::conformance::run_full_suite;
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_block_store_azure_sdk::AzureBlobBlockStore;

use support::{MockAzureServer, collect_block_ids};

#[test]
fn val_azure_sdk_store_001_002_constructor_and_publish_path_are_deterministic() {
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
fn val_azure_sdk_store_003_round_trip_and_missing_blocks_match_the_contract() {
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
fn val_azure_sdk_store_005_007_get_reports_integrity_malformed_transient_and_backend_failures() {
    let server = MockAzureServer::start();
    let store = server.store();

    let first = serialize_block(&sample_leaf_block("first")).unwrap();
    let second = serialize_block(&sample_leaf_block("second")).unwrap();
    server.insert_blob(server.blob_name(&second.hash), first.bytes.clone());

    assert_eq!(
        store.get(&second.hash).unwrap_err(),
        BlockStoreError::DecodeFailure(BlockError::HashMismatch {
            expected: second.hash,
            actual: first.hash,
        })
    );

    let malformed_bytes = [0xff, 0xff, 0x00];
    let malformed_hash = compute_block_hash(&malformed_bytes);
    let malformed_blob = server.blob_name(&malformed_hash);
    server.insert_blob(&malformed_blob, malformed_bytes.to_vec());

    assert!(matches!(
        store.get(&malformed_hash).unwrap_err(),
        BlockStoreError::DecodeFailure(BlockError::MalformedCbor(_))
    ));

    let unreadable = serialize_block(&sample_leaf_block("forbidden")).unwrap();
    let unreadable_blob = server.blob_name(&unreadable.hash);
    server.insert_blob(&unreadable_blob, unreadable.bytes);
    server.set_blob_status(&unreadable_blob, 403);
    expect_backend_failure_contains(store.get(&unreadable.hash).unwrap_err(), "HTTP 403");

    let retry_server = MockAzureServer::start();
    let retry_store = retry_server.store();
    let retried = serialize_block(&sample_leaf_block("retry-get")).unwrap();
    let retried_blob = retry_server.blob_name(&retried.hash);
    retry_server.insert_blob(&retried_blob, retried.bytes.clone());
    retry_server.set_disconnect_get_attempts(1);
    let loaded = retry_store.get(&retried.hash).unwrap().unwrap();
    assert_eq!(loaded.hash, retried.hash);
    let retry_requests = retry_server.recorded_requests();
    assert_eq!(
        retry_requests
            .iter()
            .filter(|request| request.method == "GET" && request.target.contains(&retried_blob))
            .count(),
        2
    );

    let disappeared_server = MockAzureServer::start();
    let disappeared_store = disappeared_server.store();
    let disappeared = serialize_block(&sample_leaf_block("disappeared-after-exists")).unwrap();
    let disappeared_blob = disappeared_server.blob_name(&disappeared.hash);
    disappeared_server.insert_blob(&disappeared_blob, disappeared.bytes);
    disappeared_server.set_get_status(&disappeared_blob, 404);
    assert_eq!(disappeared_store.get(&disappeared.hash).unwrap(), None);

    let exhausted_retry_server = MockAzureServer::start();
    let exhausted_retry_store = exhausted_retry_server.store();
    let exhausted = serialize_block(&sample_leaf_block("retry-exhausted-get")).unwrap();
    exhausted_retry_server.insert_blob(
        exhausted_retry_server.blob_name(&exhausted.hash),
        exhausted.bytes,
    );
    exhausted_retry_server.set_disconnect_get_attempts(10);
    expect_backend_failure_contains(
        exhausted_retry_store.get(&exhausted.hash).unwrap_err(),
        "retry policy expired",
    );
}

#[test]
fn val_azure_sdk_store_004_006_put_handles_idempotence_transient_transport_failures_permissions_and_conflicts()
 {
    let server = MockAzureServer::start();
    let store = server.store();
    let block = sample_leaf_block("shared");
    let serialized = serialize_block(&block).unwrap();

    assert_eq!(store.put(&block).unwrap(), serialized.hash);
    assert_eq!(store.put(&block).unwrap(), serialized.hash);

    let flaky_server = MockAzureServer::start();
    let flaky_blob_name = flaky_server.blob_name(&serialized.hash);
    flaky_server.set_disconnect_put_attempts(1);
    assert_eq!(flaky_server.store().put(&block).unwrap(), serialized.hash);
    assert_eq!(
        flaky_server.blob_bytes(&flaky_blob_name).unwrap(),
        serialized.bytes
    );
    let flaky_requests = flaky_server.recorded_requests();
    assert_eq!(
        flaky_requests
            .iter()
            .filter(|request| {
                request.method == "PUT" && request.target.contains(&flaky_blob_name)
            })
            .count(),
        2
    );
    assert!(
        !flaky_requests
            .iter()
            .any(|request| request.method == "GET" && request.target.contains(&flaky_blob_name))
    );
    assert!(
        !flaky_requests
            .iter()
            .any(|request| request.method == "HEAD" && request.target.contains(&flaky_blob_name))
    );

    let unknown_outcome_server = MockAzureServer::start();
    let unknown_outcome_blob_name = unknown_outcome_server.blob_name(&serialized.hash);
    unknown_outcome_server.set_disconnect_put_attempts(10);
    expect_backend_failure_contains(
        unknown_outcome_server.store().put(&block).unwrap_err(),
        "retry policy expired",
    );
    assert_eq!(
        unknown_outcome_server
            .blob_bytes(&unknown_outcome_blob_name)
            .unwrap(),
        serialized.bytes
    );
    let unknown_outcome_requests = unknown_outcome_server.recorded_requests();
    assert!(
        unknown_outcome_requests
            .iter()
            .filter(|request| {
                request.method == "PUT" && request.target.contains(&unknown_outcome_blob_name)
            })
            .count()
            >= 2
    );
    assert!(
        !unknown_outcome_requests
            .iter()
            .any(|request| request.method == "GET"
                && request.target.contains(&unknown_outcome_blob_name))
    );
    assert!(
        !unknown_outcome_requests
            .iter()
            .any(|request| request.method == "HEAD"
                && request.target.contains(&unknown_outcome_blob_name))
    );

    let exhausted_retry_server = MockAzureServer::start();
    let exhausted_retry_blob_name = exhausted_retry_server.blob_name(&serialized.hash);
    exhausted_retry_server.set_drop_put_attempts(10);
    expect_backend_failure_contains(
        exhausted_retry_server.store().put(&block).unwrap_err(),
        "retry policy expired",
    );
    let exhausted_retry_requests = exhausted_retry_server.recorded_requests();
    assert!(
        !exhausted_retry_requests
            .iter()
            .any(|request| request.method == "HEAD"
                && request.target.contains(&exhausted_retry_blob_name))
    );
    assert_eq!(
        exhausted_retry_server.blob_bytes(&exhausted_retry_blob_name),
        None
    );

    let permission_server = MockAzureServer::start();
    permission_server.set_deny_put(true);
    expect_backend_failure_contains(
        permission_server.store().put(&block).unwrap_err(),
        "HTTP 403",
    );

    let conflict_server = MockAzureServer::start();
    let conflict_blob_name = conflict_server.blob_name(&serialized.hash);
    conflict_server.insert_blob(conflict_blob_name.clone(), b"not canonical bytes".to_vec());
    assert_eq!(
        conflict_server.store().put(&block).unwrap(),
        serialized.hash
    );
    let conflict_requests = conflict_server.recorded_requests();
    assert!(
        !conflict_requests
            .iter()
            .any(|request| request.method == "HEAD" && request.target.contains(&conflict_blob_name))
    );
    assert!(
        !conflict_requests
            .iter()
            .any(|request| request.method == "GET" && request.target.contains(&conflict_blob_name))
    );
    assert_eq!(
        conflict_server.blob_bytes(&conflict_blob_name).unwrap(),
        b"not canonical bytes".to_vec()
    );

    let conflict_409_server = MockAzureServer::start();
    let conflict_409_blob_name = conflict_409_server.blob_name(&serialized.hash);
    conflict_409_server.insert_blob(conflict_409_blob_name.clone(), b"other bytes".to_vec());
    conflict_409_server.set_put_conflict_status(409);
    assert_eq!(
        conflict_409_server.store().put(&block).unwrap(),
        serialized.hash
    );
    let conflict_409_requests = conflict_409_server.recorded_requests();
    assert!(!conflict_409_requests.iter().any(
        |request| request.method == "HEAD" && request.target.contains(&conflict_409_blob_name)
    ));
    assert!(
        !conflict_409_requests
            .iter()
            .any(|request| request.method == "GET"
                && request.target.contains(&conflict_409_blob_name))
    );
    assert_eq!(
        conflict_409_server
            .blob_bytes(&conflict_409_blob_name)
            .unwrap(),
        b"other bytes".to_vec()
    );
}

#[test]
fn val_azure_sdk_store_004_concurrent_publishers_converge_on_one_valid_blob() {
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
fn val_azure_sdk_store_009_parent_conformance_requirements_are_realized_by_tests() {
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
fn val_azure_sdk_store_008_enumeration_yields_only_recognized_block_ids() {
    let server = MockAzureServer::start();
    let store = server.store();
    let first = sample_leaf_block("first");
    let second = sample_leaf_block("second");

    let expected = HashSet::from([store.put(&first).unwrap(), store.put(&second).unwrap()]);
    server.add_extra_list_name("notes/readme.txt");
    server.add_extra_list_name("aa/bb/temporary.part");
    server.add_extra_list_name("notes/a&b<c>.txt");

    let enumerated = collect_block_ids(store.iter_block_ids().unwrap()).unwrap();

    assert_eq!(enumerated, expected);
}

#[test]
fn val_azure_sdk_store_007_008_enumeration_surfaces_listing_transient_and_decoding_failures() {
    let retry_server = MockAzureServer::start();
    let retry_store = retry_server.store();
    let first = sample_leaf_block("first");
    let second = sample_leaf_block("second");
    let expected = HashSet::from([
        retry_store.put(&first).unwrap(),
        retry_store.put(&second).unwrap(),
    ]);
    retry_server.set_disconnect_list_attempts(1);
    let enumerated = collect_block_ids(retry_store.iter_block_ids().unwrap()).unwrap();
    assert_eq!(enumerated, expected);
    let retry_requests = retry_server.recorded_requests();
    assert_eq!(
        retry_requests
            .iter()
            .filter(|request| request.method == "GET" && request.target.contains("comp=list"))
            .count(),
        2
    );

    let exhausted_retry_server = MockAzureServer::start();
    exhausted_retry_server.set_disconnect_list_attempts(10);
    match exhausted_retry_server.store().iter_block_ids() {
        Err(error) => expect_backend_failure_contains(error, "retry policy expired"),
        Ok(iter) => {
            let error = iter.collect::<Result<Vec<_>, _>>().unwrap_err();
            expect_backend_failure_contains(error, "retry policy expired");
        }
    }

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

    let shard_mismatch_server = MockAzureServer::start();
    shard_mismatch_server.add_extra_list_name(
        "aa/bb/cc00000000000000000000000000000000000000000000000000000000000000.cbor",
    );
    match shard_mismatch_server.store().iter_block_ids() {
        Err(error) => expect_backend_failure_contains(
            error,
            "failed to decode an enumerated block ID candidate at blob aa/bb/cc00000000000000000000000000000000000000000000000000000000000000.cbor: shard prefix mismatch",
        ),
        Ok(iter) => {
            let error = iter.collect::<Result<Vec<_>, _>>().unwrap_err();
            expect_backend_failure_contains(
                error,
                "failed to decode an enumerated block ID candidate at blob aa/bb/cc00000000000000000000000000000000000000000000000000000000000000.cbor: shard prefix mismatch",
            );
        }
    }
}

#[test]
fn malformed_listing_xml_is_an_explicit_backend_failure() {
    let server = MockAzureServer::start();
    server.set_malformed_listing(true);
    match server.store().iter_block_ids() {
        Err(error) => expect_backend_failure_contains(error, "failed to list Azure container"),
        Ok(iter) => {
            let error = iter.collect::<Result<Vec<_>, _>>().unwrap_err();
            expect_backend_failure_contains(error, "failed to list Azure container");
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
