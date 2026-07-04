// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::future::Future;

use ciborium::value::Value;
use lexongraph_block::{DecodedBlock, VersionedBlock, v2};
use lexongraph_block_store::BlockStoreExt;
use lexongraph_block_store_memory::MemoryBlockStore;

trait BlockingResultFutureExt<T, E>: Future<Output = Result<T, E>> + Sized {
    fn unwrap(self) -> T
    where
        E: std::fmt::Debug,
    {
        pollster::block_on(self).unwrap()
    }
}

impl<F, T, E> BlockingResultFutureExt<T, E> for F where F: Future<Output = Result<T, E>> {}

#[test]
fn versioned_custom_blocks_round_trip_through_the_store() {
    let store = MemoryBlockStore::new(8).unwrap();
    let block = v2::build_custom_block(
        "example.metadata",
        Value::Map(vec![
            (Value::Text("owner".into()), Value::Text("search".into())),
            (
                Value::Text("refs".into()),
                Value::Array(vec![Value::Bytes([0x11; 32].to_vec())]),
            ),
        ]),
    )
    .unwrap();

    let block_id = store
        .put_versioned(&VersionedBlock::V2(block.clone()))
        .unwrap();
    let decoded = store.get_decoded(&block_id).unwrap().unwrap();

    match decoded {
        DecodedBlock::V2(validated) => assert_eq!(validated.block, block),
        other => panic!("expected a version-2 decoded block, got {other:?}"),
    }
}
