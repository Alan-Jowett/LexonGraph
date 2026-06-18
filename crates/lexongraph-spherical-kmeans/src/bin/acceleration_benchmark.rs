// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::time::Instant;

use lexongraph_linear_algebra_acceleration::{
    ExecutionBackendRequest, backend_resolution_label, detected_execution_backend_selection,
    with_execution_backend_request,
};
use lexongraph_spherical_kmeans::{
    SphericalInitializationPolicy, SphericalKmeansParams, SphericalKmeansStreamingTrainer,
};
use lexongraph_streaming_clustering::{
    StreamingClusterTrainer, StreamingClusteringConfig, StreamingClusteringError,
};

fn main() -> Result<(), String> {
    let observed_count = parse_arg("--observed-count").unwrap_or(8192);
    let dimensions = parse_arg("--dimensions").unwrap_or(384);
    let cluster_count = parse_arg("--cluster-count").unwrap_or(256);
    let repeats = parse_arg("--repeats").unwrap_or(5);

    let dataset = build_dataset(observed_count, dimensions);
    let config = StreamingClusteringConfig {
        cluster_count: u32::try_from(cluster_count)
            .map_err(|_| "cluster-count overflowed u32".to_string())?,
        dimensions,
        balance_constraints: None,
        random_seed: Some(7),
    };
    let params = SphericalKmeansParams {
        initialization_policy: SphericalInitializationPolicy::SeededDeterministicFarthestPoint,
        max_iteration_count: 8,
        convergence_tolerance: 0.0,
    };

    let cpu_runs = benchmark_backend(
        ExecutionBackendRequest::Cpu,
        repeats,
        dataset.as_slice(),
        &config,
        &params,
    )?;
    let wgpu_runs = benchmark_backend(
        ExecutionBackendRequest::Wgpu,
        repeats,
        dataset.as_slice(),
        &config,
        &params,
    )?;

    println!(
        "cpu backend={} median_ms={:.3} runs_ms={:?}",
        cpu_runs.backend_label, cpu_runs.median_ms, cpu_runs.run_millis
    );
    println!(
        "wgpu backend={} median_ms={:.3} runs_ms={:?}",
        wgpu_runs.backend_label, wgpu_runs.median_ms, wgpu_runs.run_millis
    );
    println!(
        "wgpu_faster_than_cpu={}",
        (wgpu_runs.median_ms < cpu_runs.median_ms)
    );
    Ok(())
}

struct BenchmarkRuns {
    backend_label: String,
    run_millis: Vec<f64>,
    median_ms: f64,
}

fn benchmark_backend(
    request: ExecutionBackendRequest,
    repeats: usize,
    dataset: &[Vec<f32>],
    config: &StreamingClusteringConfig,
    params: &SphericalKmeansParams,
) -> Result<BenchmarkRuns, String> {
    with_execution_backend_request(request, || {
        let selection = detected_execution_backend_selection();
        let backend_label = backend_resolution_label(&selection).to_string();
        let mut run_millis = Vec::with_capacity(repeats);
        for _ in 0..repeats {
            let started = Instant::now();
            run_training(dataset, config, params).map_err(|error| error.to_string())?;
            run_millis.push(started.elapsed().as_secs_f64() * 1000.0);
        }
        let median_ms = median(run_millis.as_slice());
        Ok(BenchmarkRuns {
            backend_label,
            run_millis,
            median_ms,
        })
    })
}

fn run_training(
    dataset: &[Vec<f32>],
    config: &StreamingClusteringConfig,
    params: &SphericalKmeansParams,
) -> Result<(), StreamingClusteringError> {
    let mut trainer = SphericalKmeansStreamingTrainer::new(config.clone(), params.clone())?;
    for batch in dataset.chunks(512) {
        trainer.ingest_batch(batch)?;
    }
    trainer.finish_pass()?;
    Ok(())
}

fn build_dataset(observed_count: usize, dimensions: usize) -> Vec<Vec<f32>> {
    (0..observed_count)
        .map(|index| normalized_pattern(index, dimensions))
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

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    sorted[sorted.len() / 2]
}

fn parse_arg(flag: &str) -> Option<usize> {
    let mut args = std::env::args().skip(1);
    while let Some(current) = args.next() {
        if current == flag {
            return args.next().and_then(|value| value.parse::<usize>().ok());
        }
    }
    None
}
