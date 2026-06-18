// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

mod support;

use lexongraph_linear_algebra_acceleration::{
    ExecutionBackendRequest, ExecutionBackendResolution, detected_execution_backend_selection,
    with_execution_backend_request,
};
use lexongraph_spherical_kmeans::SphericalKmeansStreamingTrainer;
use lexongraph_streaming_clustering::{StreamingClusterClassifier, StreamingClusterTrainer};
use support::{config, params};

#[test]
fn accelerated_and_cpu_training_preserve_assignments_when_wgpu_is_supported() {
    let cpu = with_execution_backend_request(ExecutionBackendRequest::Cpu, train_classifier);
    let selection = with_execution_backend_request(ExecutionBackendRequest::Wgpu, || {
        detected_execution_backend_selection()
    });
    if selection.resolution != ExecutionBackendResolution::Wgpu {
        return;
    }
    let wgpu = with_execution_backend_request(ExecutionBackendRequest::Wgpu, train_classifier);

    for query in validation_queries() {
        assert_eq!(
            cpu.assign(query.as_slice()).unwrap(),
            wgpu.assign(query.as_slice()).unwrap()
        );
    }
}

fn train_classifier() -> <SphericalKmeansStreamingTrainer as StreamingClusterTrainer>::Classifier {
    let mut trainer = SphericalKmeansStreamingTrainer::new(large_config(), params()).unwrap();
    for batch in large_pass().chunks(256) {
        trainer.ingest_batch(batch).unwrap();
    }
    trainer.finish_pass().unwrap();
    trainer.complete_training().unwrap();
    trainer.into_classifier().unwrap()
}

fn large_config() -> lexongraph_streaming_clustering::StreamingClusteringConfig {
    lexongraph_streaming_clustering::StreamingClusteringConfig {
        cluster_count: 16,
        dimensions: 96,
        balance_constraints: None,
        random_seed: config().random_seed,
    }
}

fn validation_queries() -> Vec<Vec<f32>> {
    vec![
        normalized_pattern(5, 96),
        normalized_pattern(23, 96),
        normalized_pattern(71, 96),
        normalized_pattern(127, 96),
    ]
}

fn large_pass() -> Vec<Vec<f32>> {
    (0..2048)
        .map(|index| normalized_pattern(index, 96))
        .collect()
}

fn normalized_pattern(seed: usize, dimensions: usize) -> Vec<f32> {
    let mut values = Vec::with_capacity(dimensions);
    for dimension in 0..dimensions {
        let angle = ((seed * 37 + dimension * 17 + 1) % 997) as f32;
        values.push((angle * 0.013).sin() + (angle * 0.007).cos() * 0.5);
    }
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    values.into_iter().map(|value| value / norm).collect()
}
