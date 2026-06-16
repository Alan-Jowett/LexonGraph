// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::sync::{OnceLock, RwLock};

use serde::{Deserialize, Serialize};

const QUALIFICATION_HARDWARE_PROFILE: &str = "Windows + AMD Radeon 780M";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ExecutionBackendRequest {
    #[default]
    Auto,
    Cpu,
    Wgpu,
}

impl ExecutionBackendRequest {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Cpu => "cpu",
            Self::Wgpu => "wgpu",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "cpu" => Some(Self::Cpu),
            "wgpu" => Some(Self::Wgpu),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionBackendResolution {
    Cpu,
    Wgpu,
    WgpuAvailableButDeclined,
    WgpuUnsupportedFallback,
    WgpuProbeFailed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBackendSelection {
    pub request: ExecutionBackendRequest,
    pub resolution: ExecutionBackendResolution,
    pub detail: String,
}

impl Default for ExecutionBackendSelection {
    fn default() -> Self {
        resolve_execution_backend_selection(ExecutionBackendRequest::Auto)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DenseDistanceMetric {
    Euclidean,
    Cosine,
}

pub fn execution_backend_request() -> ExecutionBackendRequest {
    *backend_request_lock()
        .read()
        .expect("execution backend request lock poisoned")
}

pub fn set_execution_backend_request(request: ExecutionBackendRequest) {
    *backend_request_lock()
        .write()
        .expect("execution backend request lock poisoned") = request;
}

pub(crate) fn detected_execution_backend_selection() -> ExecutionBackendSelection {
    resolve_execution_backend_selection(execution_backend_request())
}

pub(crate) fn backend_resolution_label(selection: &ExecutionBackendSelection) -> &'static str {
    match selection.resolution {
        ExecutionBackendResolution::Cpu => "cpu",
        ExecutionBackendResolution::Wgpu => "wgpu",
        ExecutionBackendResolution::WgpuAvailableButDeclined => "wgpu-declined",
        ExecutionBackendResolution::WgpuUnsupportedFallback => "wgpu-unsupported-fallback",
        ExecutionBackendResolution::WgpuProbeFailed => "wgpu-probe-failed",
    }
}

pub(crate) fn dense_distance_matrix(
    left: &[&[f32]],
    right: &[&[f32]],
    metric: DenseDistanceMetric,
) -> Result<Vec<f32>, String> {
    validate_dense_inputs(left, right, metric)?;
    match detected_execution_backend_selection().resolution {
        ExecutionBackendResolution::Wgpu => {
            #[cfg(feature = "wgpu-accel")]
            {
                let context = wgpu_context().map_err(|error| error.to_string())?;
                return context.compute_distance_matrix(left, right, metric);
            }
            #[cfg(not(feature = "wgpu-accel"))]
            {
                unreachable!("wgpu resolution is impossible without the wgpu-accel feature");
            }
        }
        ExecutionBackendResolution::Cpu
        | ExecutionBackendResolution::WgpuAvailableButDeclined
        | ExecutionBackendResolution::WgpuUnsupportedFallback
        | ExecutionBackendResolution::WgpuProbeFailed => {}
    }
    cpu_dense_distance_matrix(left, right, metric)
}

#[cfg(test)]
pub(crate) fn fixture_cpu_execution_backend_selection() -> ExecutionBackendSelection {
    ExecutionBackendSelection {
        request: ExecutionBackendRequest::Cpu,
        resolution: ExecutionBackendResolution::Cpu,
        detail: "fixture execution pinned to the cpu backend".into(),
    }
}

#[cfg(test)]
pub(crate) fn with_execution_backend_request_for_test<T>(
    request: ExecutionBackendRequest,
    run: impl FnOnce() -> T,
) -> T {
    let previous = execution_backend_request();
    set_execution_backend_request(request);
    let result = run();
    set_execution_backend_request(previous);
    result
}

fn backend_request_lock() -> &'static RwLock<ExecutionBackendRequest> {
    static REQUEST: OnceLock<RwLock<ExecutionBackendRequest>> = OnceLock::new();
    REQUEST.get_or_init(|| RwLock::new(ExecutionBackendRequest::Auto))
}

fn resolve_execution_backend_selection(
    request: ExecutionBackendRequest,
) -> ExecutionBackendSelection {
    #[cfg(feature = "wgpu-accel")]
    {
        match wgpu_probe() {
            WgpuProbe::Supported(info) => match request {
                ExecutionBackendRequest::Auto | ExecutionBackendRequest::Wgpu => {
                    ExecutionBackendSelection {
                        request,
                        resolution: ExecutionBackendResolution::Wgpu,
                        detail: format!(
                            "using wgpu backend on {} (target profile match: {})",
                            info.summary,
                            if info.matches_declared_target {
                                "yes"
                            } else {
                                "no"
                            }
                        ),
                    }
                }
                ExecutionBackendRequest::Cpu => ExecutionBackendSelection {
                    request,
                    resolution: ExecutionBackendResolution::WgpuAvailableButDeclined,
                    detail: format!(
                        "wgpu capability probe succeeded on {}; execution was pinned to cpu",
                        info.summary
                    ),
                },
            },
            WgpuProbe::Unsupported(message) => ExecutionBackendSelection {
                request,
                resolution: ExecutionBackendResolution::WgpuUnsupportedFallback,
                detail: format!("{message}; using cpu backend"),
            },
            WgpuProbe::ProbeFailed(message) => ExecutionBackendSelection {
                request,
                resolution: ExecutionBackendResolution::WgpuProbeFailed,
                detail: format!("{message}; using cpu backend"),
            },
        }
    }
    #[cfg(not(feature = "wgpu-accel"))]
    {
        ExecutionBackendSelection {
            request,
            resolution: ExecutionBackendResolution::WgpuUnsupportedFallback,
            detail: "binary was built without the wgpu-accel feature; using cpu backend".into(),
        }
    }
}

fn validate_dense_inputs(
    left: &[&[f32]],
    right: &[&[f32]],
    metric: DenseDistanceMetric,
) -> Result<(), String> {
    if left.is_empty() {
        return Err("dense distance matrix requires at least one left-hand vector".into());
    }
    if right.is_empty() {
        return Err("dense distance matrix requires at least one right-hand vector".into());
    }
    let dimensions = left[0].len();
    if dimensions == 0 {
        return Err("dense distance matrix requires non-empty vectors".into());
    }
    if left.iter().any(|vector| vector.len() != dimensions)
        || right.iter().any(|vector| vector.len() != dimensions)
    {
        return Err("dense distance matrix requires matching vector dimensions".into());
    }
    if matches!(metric, DenseDistanceMetric::Cosine)
        && left
            .iter()
            .chain(right.iter())
            .any(|vector| l2_norm_sq(vector) == 0.0)
    {
        return Err("cosine distance requires non-zero embeddings".into());
    }
    Ok(())
}

fn cpu_dense_distance_matrix(
    left: &[&[f32]],
    right: &[&[f32]],
    metric: DenseDistanceMetric,
) -> Result<Vec<f32>, String> {
    let mut distances = Vec::with_capacity(left.len() * right.len());
    for source in left {
        for target in right {
            let value = match metric {
                DenseDistanceMetric::Euclidean => euclidean_distance(source, target),
                DenseDistanceMetric::Cosine => cosine_distance(source, target)?,
            };
            distances.push(value as f32);
        }
    }
    Ok(distances)
}

fn l2_norm_sq(vector: &[f32]) -> f64 {
    vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum()
}

fn euclidean_distance(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right.iter())
        .map(|(lhs, rhs)| {
            let delta = f64::from(*lhs) - f64::from(*rhs);
            delta * delta
        })
        .sum::<f64>()
        .sqrt()
}

fn cosine_distance(left: &[f32], right: &[f32]) -> Result<f64, String> {
    let left_norm = l2_norm_sq(left).sqrt();
    let right_norm = l2_norm_sq(right).sqrt();
    if left_norm == 0.0 || right_norm == 0.0 {
        return Err("cosine distance requires non-zero embeddings".into());
    }
    let cosine_similarity = left
        .iter()
        .zip(right.iter())
        .map(|(lhs, rhs)| f64::from(*lhs) * f64::from(*rhs))
        .sum::<f64>()
        / (left_norm * right_norm);
    Ok((1.0 - cosine_similarity).max(0.0))
}

#[cfg(feature = "wgpu-accel")]
#[derive(Clone, Debug)]
struct WgpuAdapterInfo {
    summary: String,
    matches_declared_target: bool,
}

#[cfg(feature = "wgpu-accel")]
enum WgpuProbe {
    Supported(WgpuAdapterInfo),
    Unsupported(String),
    ProbeFailed(String),
}

#[cfg(feature = "wgpu-accel")]
fn wgpu_probe() -> &'static WgpuProbe {
    static PROBE: OnceLock<WgpuProbe> = OnceLock::new();
    PROBE.get_or_init(|| match create_wgpu_context() {
        Ok(context) => WgpuProbe::Supported(context.adapter_info.clone()),
        Err(error) => match error {
            WgpuContextError::Unsupported(message) => WgpuProbe::Unsupported(message),
            WgpuContextError::ProbeFailed(message) => WgpuProbe::ProbeFailed(message),
        },
    })
}

#[cfg(feature = "wgpu-accel")]
fn wgpu_context() -> Result<&'static WgpuContext, WgpuContextError> {
    static CONTEXT: OnceLock<Result<WgpuContext, WgpuContextError>> = OnceLock::new();
    CONTEXT
        .get_or_init(create_wgpu_context)
        .as_ref()
        .map_err(Clone::clone)
}

#[cfg(feature = "wgpu-accel")]
#[derive(Clone, Debug)]
enum WgpuContextError {
    Unsupported(String),
    ProbeFailed(String),
}

#[cfg(feature = "wgpu-accel")]
impl std::fmt::Display for WgpuContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(message) | Self::ProbeFailed(message) => f.write_str(message),
        }
    }
}

#[cfg(feature = "wgpu-accel")]
struct WgpuContext {
    _instance: wgpu::Instance,
    device: wgpu::Device,
    queue: wgpu::Queue,
    bind_group_layout: wgpu::BindGroupLayout,
    pipeline: wgpu::ComputePipeline,
    adapter_info: WgpuAdapterInfo,
}

#[cfg(feature = "wgpu-accel")]
impl WgpuContext {
    fn compute_distance_matrix(
        &self,
        left: &[&[f32]],
        right: &[&[f32]],
        metric: DenseDistanceMetric,
    ) -> Result<Vec<f32>, String> {
        use bytemuck::{Pod, Zeroable, cast_slice};
        use wgpu::util::DeviceExt;

        #[repr(C)]
        #[derive(Clone, Copy, Pod, Zeroable)]
        struct Params {
            left_count: u32,
            right_count: u32,
            dimensions: u32,
            metric_kind: u32,
        }

        let left_values = flatten_embeddings(left);
        let right_values = flatten_embeddings(right);
        let params = Params {
            left_count: u32::try_from(left.len())
                .map_err(|_| "left-hand matrix row count overflowed u32".to_string())?,
            right_count: u32::try_from(right.len())
                .map_err(|_| "right-hand matrix row count overflowed u32".to_string())?,
            dimensions: u32::try_from(left[0].len())
                .map_err(|_| "dense-kernel dimensionality overflowed u32".to_string())?,
            metric_kind: match metric {
                DenseDistanceMetric::Euclidean => 0,
                DenseDistanceMetric::Cosine => 1,
            },
        };
        let output_len = left
            .len()
            .checked_mul(right.len())
            .ok_or_else(|| "dense-kernel output size overflowed usize".to_string())?;
        let output_size = u64::try_from(
            output_len
                .checked_mul(std::mem::size_of::<f32>())
                .ok_or_else(|| "dense-kernel output byte size overflowed usize".to_string())?,
        )
        .map_err(|_| "dense-kernel output byte size overflowed u64".to_string())?;

        let left_buffer = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("evaluator-left-vectors"),
                contents: cast_slice(left_values.as_slice()),
                usage: wgpu::BufferUsages::STORAGE,
            });
        let right_buffer = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("evaluator-right-vectors"),
                contents: cast_slice(right_values.as_slice()),
                usage: wgpu::BufferUsages::STORAGE,
            });
        let output_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("evaluator-distance-output"),
            size: output_size,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });
        let readback_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("evaluator-distance-readback"),
            size: output_size,
            usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
            mapped_at_creation: false,
        });
        let params_buffer = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("evaluator-distance-params"),
                contents: cast_slice(&[params]),
                usage: wgpu::BufferUsages::UNIFORM,
            });
        let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("evaluator-distance-bind-group"),
            layout: &self.bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: left_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: right_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: output_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: params_buffer.as_entire_binding(),
                },
            ],
        });

        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("evaluator-distance-encoder"),
            });
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("evaluator-distance-pass"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&self.pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups(
                params.right_count.div_ceil(8),
                params.left_count.div_ceil(8),
                1,
            );
        }
        encoder.copy_buffer_to_buffer(&output_buffer, 0, &readback_buffer, 0, output_size);
        self.queue.submit([encoder.finish()]);

        let slice = readback_buffer.slice(..);
        let (sender, receiver) = std::sync::mpsc::channel();
        slice.map_async(wgpu::MapMode::Read, move |result| {
            let _ = sender.send(result.map_err(|error| format!("{error:?}")));
        });
        let _ = self.device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
        let map_result = receiver
            .recv()
            .map_err(|error| format!("failed to receive wgpu readback status: {error}"))?;
        map_result?;
        let mapped = slice.get_mapped_range();
        let values = bytemuck::cast_slice(&mapped).to_vec();
        drop(mapped);
        readback_buffer.unmap();
        Ok(values)
    }
}

#[cfg(feature = "wgpu-accel")]
fn create_wgpu_context() -> Result<WgpuContext, WgpuContextError> {
    use pollster::block_on;

    let instance = wgpu::Instance::default();
    let adapter = block_on(instance.request_adapter(&wgpu::RequestAdapterOptions::default()))
        .map_err(|error| {
            WgpuContextError::Unsupported(format!(
                "no supported wgpu adapter was available for the evaluator: {error:?}"
            ))
        })?;
    let adapter_info = adapter.get_info();
    let summary = format!(
        "{} via {:?} (vendor={:#06x}, device={:#06x}, target={})",
        adapter_info.name,
        adapter_info.backend,
        adapter_info.vendor,
        adapter_info.device,
        QUALIFICATION_HARDWARE_PROFILE
    );
    let matches_declared_target = cfg!(target_os = "windows")
        && adapter_info
            .name
            .to_ascii_lowercase()
            .contains("radeon 780m");
    let (device, queue) = block_on(adapter.request_device(&wgpu::DeviceDescriptor {
        label: Some("lexongraph-evaluator-wgpu-device"),
        required_features: wgpu::Features::empty(),
        required_limits: wgpu::Limits::downlevel_defaults(),
        memory_hints: wgpu::MemoryHints::Performance,
        trace: wgpu::Trace::Off,
        experimental_features: Default::default(),
    }))
    .map_err(|error| {
        WgpuContextError::ProbeFailed(format!(
            "wgpu adapter probe succeeded for {summary}, but device creation failed: {error}"
        ))
    })?;

    let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("evaluator-dense-distance-shader"),
        source: wgpu::ShaderSource::Wgsl(
            r#"
struct Params {
    left_count: u32,
    right_count: u32,
    dimensions: u32,
    metric_kind: u32,
}

@group(0) @binding(0) var<storage, read> left_values: array<f32>;
@group(0) @binding(1) var<storage, read> right_values: array<f32>;
@group(0) @binding(2) var<storage, read_write> output_values: array<f32>;
@group(0) @binding(3) var<uniform> params: Params;

@compute @workgroup_size(8, 8, 1)
fn main(@builtin(global_invocation_id) global_id: vec3<u32>) {
    let column = global_id.x;
    let row = global_id.y;
    if (row >= params.left_count || column >= params.right_count) {
        return;
    }

    let left_base = row * params.dimensions;
    let right_base = column * params.dimensions;
    let output_index = row * params.right_count + column;

    if (params.metric_kind == 0u) {
        var sum_sq: f32 = 0.0;
        for (var dim: u32 = 0u; dim < params.dimensions; dim = dim + 1u) {
            let delta = left_values[left_base + dim] - right_values[right_base + dim];
            sum_sq = sum_sq + delta * delta;
        }
        output_values[output_index] = sqrt(sum_sq);
        return;
    }

    var dot: f32 = 0.0;
    var left_norm_sq: f32 = 0.0;
    var right_norm_sq: f32 = 0.0;
    for (var dim: u32 = 0u; dim < params.dimensions; dim = dim + 1u) {
        let lhs = left_values[left_base + dim];
        let rhs = right_values[right_base + dim];
        dot = dot + lhs * rhs;
        left_norm_sq = left_norm_sq + lhs * lhs;
        right_norm_sq = right_norm_sq + rhs * rhs;
    }
    if (left_norm_sq == 0.0 || right_norm_sq == 0.0) {
        output_values[output_index] = 0.0;
        return;
    }
    output_values[output_index] = max(1.0 - dot / sqrt(left_norm_sq * right_norm_sq), 0.0);
}
"#
            .into(),
        ),
    });
    let bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
        label: Some("evaluator-dense-distance-bind-group-layout"),
        entries: &[
            wgpu::BindGroupLayoutEntry {
                binding: 0,
                visibility: wgpu::ShaderStages::COMPUTE,
                ty: wgpu::BindingType::Buffer {
                    ty: wgpu::BufferBindingType::Storage { read_only: true },
                    has_dynamic_offset: false,
                    min_binding_size: None,
                },
                count: None,
            },
            wgpu::BindGroupLayoutEntry {
                binding: 1,
                visibility: wgpu::ShaderStages::COMPUTE,
                ty: wgpu::BindingType::Buffer {
                    ty: wgpu::BufferBindingType::Storage { read_only: true },
                    has_dynamic_offset: false,
                    min_binding_size: None,
                },
                count: None,
            },
            wgpu::BindGroupLayoutEntry {
                binding: 2,
                visibility: wgpu::ShaderStages::COMPUTE,
                ty: wgpu::BindingType::Buffer {
                    ty: wgpu::BufferBindingType::Storage { read_only: false },
                    has_dynamic_offset: false,
                    min_binding_size: None,
                },
                count: None,
            },
            wgpu::BindGroupLayoutEntry {
                binding: 3,
                visibility: wgpu::ShaderStages::COMPUTE,
                ty: wgpu::BindingType::Buffer {
                    ty: wgpu::BufferBindingType::Uniform,
                    has_dynamic_offset: false,
                    min_binding_size: None,
                },
                count: None,
            },
        ],
    });
    let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
        label: Some("evaluator-dense-distance-pipeline-layout"),
        bind_group_layouts: &[Some(&bind_group_layout)],
        immediate_size: 0,
    });
    let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: Some("evaluator-dense-distance-pipeline"),
        layout: Some(&pipeline_layout),
        module: &shader,
        entry_point: Some("main"),
        cache: None,
        compilation_options: Default::default(),
    });

    Ok(WgpuContext {
        _instance: instance,
        device,
        queue,
        bind_group_layout,
        pipeline,
        adapter_info: WgpuAdapterInfo {
            summary,
            matches_declared_target,
        },
    })
}

#[cfg(feature = "wgpu-accel")]
fn flatten_embeddings(vectors: &[&[f32]]) -> Vec<f32> {
    let total_len = vectors.iter().map(|vector| vector.len()).sum();
    let mut flattened = Vec::with_capacity(total_len);
    for vector in vectors {
        flattened.extend_from_slice(vector);
    }
    flattened
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_parser_accepts_known_values() {
        assert_eq!(
            ExecutionBackendRequest::parse("auto"),
            Some(ExecutionBackendRequest::Auto)
        );
        assert_eq!(
            ExecutionBackendRequest::parse("CPU"),
            Some(ExecutionBackendRequest::Cpu)
        );
        assert_eq!(
            ExecutionBackendRequest::parse("wgpu"),
            Some(ExecutionBackendRequest::Wgpu)
        );
        assert_eq!(ExecutionBackendRequest::parse("bogus"), None);
    }

    #[test]
    fn dense_distance_matrix_cpu_matches_expected_euclidean_values() {
        let distances =
            with_execution_backend_request_for_test(ExecutionBackendRequest::Cpu, || {
                dense_distance_matrix(
                    &[&[0.0, 0.0], &[3.0, 4.0]],
                    &[&[0.0, 0.0], &[6.0, 8.0]],
                    DenseDistanceMetric::Euclidean,
                )
                .unwrap()
            });
        assert_eq!(distances, vec![0.0, 10.0, 5.0, 5.0]);
    }

    #[cfg(feature = "wgpu-accel")]
    #[test]
    fn dense_distance_matrix_wgpu_matches_cpu_when_supported() {
        let selection =
            with_execution_backend_request_for_test(ExecutionBackendRequest::Wgpu, || {
                detected_execution_backend_selection()
            });
        if selection.resolution != ExecutionBackendResolution::Wgpu {
            return;
        }

        let expected =
            with_execution_backend_request_for_test(ExecutionBackendRequest::Cpu, || {
                dense_distance_matrix(
                    &[&[1.0, 0.0], &[0.0, 1.0], &[1.0, 1.0]],
                    &[&[1.0, 0.0], &[0.0, 1.0]],
                    DenseDistanceMetric::Cosine,
                )
                .unwrap()
            });
        let actual = with_execution_backend_request_for_test(ExecutionBackendRequest::Wgpu, || {
            dense_distance_matrix(
                &[&[1.0, 0.0], &[0.0, 1.0], &[1.0, 1.0]],
                &[&[1.0, 0.0], &[0.0, 1.0]],
                DenseDistanceMetric::Cosine,
            )
            .unwrap()
        });

        for (left, right) in expected.iter().zip(actual.iter()) {
            assert!((left - right).abs() < 1e-4);
        }
    }
}
