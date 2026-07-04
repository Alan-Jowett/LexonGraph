// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::collections::HashSet;
use std::env;

use futures::TryStreamExt;
use lexongraph_block::{BlockHash, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_leaf_block};
use lexongraph_block_store::BlockStore;
use lexongraph_block_store_azure::AzureBlobBlockStore;

const TEST_CONTAINER_SAS_URL_ENV: &str = "LEXONGRAPH_AZURE_TEST_CONTAINER_SAS_URL";

#[test]
#[ignore = "requires explicit live Azure CI selection"]
fn live_azure_round_trip_missing_and_enumeration_match_the_contract() {
    let container_sas_url = env::var(TEST_CONTAINER_SAS_URL_ENV).unwrap_or_else(|_| {
        panic!(
            "set {TEST_CONTAINER_SAS_URL_ENV} to a container SAS URL before selecting this live Azure test"
        )
    });
    let store = AzureBlobBlockStore::new(&container_sas_url).unwrap();

    let first = sample_leaf_block("live-first");
    let second = sample_leaf_block("live-second");
    let first_id = pollster::block_on(store.put(&first)).unwrap();
    let second_id = pollster::block_on(store.put(&second)).unwrap();
    let expected = HashSet::from([first_id, second_id]);

    let loaded = pollster::block_on(store.get(&first_id)).unwrap().unwrap();
    assert_eq!(loaded.hash, first_id);
    assert_eq!(loaded.block, first);

    assert_eq!(
        pollster::block_on(store.get(&BlockHash::from_bytes([0x44; 32]))).unwrap(),
        None
    );

    let enumerated =
        pollster::block_on(store.iter_block_ids().unwrap().try_collect::<HashSet<_>>()).unwrap();
    assert!(expected.is_subset(&enumerated));
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
