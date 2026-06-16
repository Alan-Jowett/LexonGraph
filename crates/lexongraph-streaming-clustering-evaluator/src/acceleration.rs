// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionBackendRequest {
    Auto,
    Cpu,
    Wgpu,
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
        unsupported_fallback_selection()
    }
}

pub(crate) fn detected_execution_backend_selection() -> &'static ExecutionBackendSelection {
    static DETECTED: OnceLock<ExecutionBackendSelection> = OnceLock::new();
    DETECTED.get_or_init(detect_execution_backend_selection)
}

#[cfg(test)]
pub(crate) fn fixture_cpu_execution_backend_selection() -> ExecutionBackendSelection {
    ExecutionBackendSelection {
        request: ExecutionBackendRequest::Cpu,
        resolution: ExecutionBackendResolution::Cpu,
        detail: "fixture execution pinned to the cpu backend".into(),
    }
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

fn detect_execution_backend_selection() -> ExecutionBackendSelection {
    if cfg!(feature = "wgpu-accel") {
        ExecutionBackendSelection {
            request: ExecutionBackendRequest::Auto,
            resolution: ExecutionBackendResolution::WgpuAvailableButDeclined,
            detail: "binary was built with wgpu-accel scaffolding, but accelerated evaluator kernels are not enabled for this run; using cpu backend".into(),
        }
    } else {
        unsupported_fallback_selection()
    }
}

fn unsupported_fallback_selection() -> ExecutionBackendSelection {
    ExecutionBackendSelection {
        request: ExecutionBackendRequest::Auto,
        resolution: ExecutionBackendResolution::WgpuUnsupportedFallback,
        detail: "binary was built without the wgpu-accel feature; using cpu backend".into(),
    }
}
