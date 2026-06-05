// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Deterministic, streaming-first PCA transforms for LexonGraph.

use std::cmp::Ordering;
use std::fmt;

use nalgebra::{DMatrix, linalg::SymmetricEigen};

const PCA_MAGIC: &[u8; 4] = b"LPCA";
const AFFINE_MAGIC: &[u8; 4] = b"LAFF";
const SERIALIZATION_VERSION: u32 = 1;
pub const CURRENT_SCHEMA_VERSION: u32 = 1;
const SMALL_NEGATIVE_EIGENVALUE_TOLERANCE: f64 = 1e-10;

#[derive(Clone, Debug, PartialEq)]
pub enum PcaError {
    DimensionMismatch {
        context: &'static str,
        expected: usize,
        actual: usize,
    },
    EmptyInput,
    InsufficientSamples {
        sample_count: u64,
    },
    NonFiniteInput {
        context: &'static str,
        index: usize,
    },
    InvalidTruncationDimension {
        requested: usize,
        available: usize,
    },
    DegenerateCovariance {
        index: usize,
        eigenvalue: f64,
    },
    DecompositionFailure(String),
    InvalidNumericState(String),
    InvalidSerializedFormat(String),
    SchemaVersionMismatch {
        expected: u32,
        actual: u32,
    },
    ValidationFailure(String),
    QuantizationConfigurationError(String),
}

impl fmt::Display for PcaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DimensionMismatch {
                context,
                expected,
                actual,
            } => write!(
                f,
                "{context} dimension mismatch: expected {expected}, got {actual}"
            ),
            Self::EmptyInput => write!(f, "PCA fitting requires at least one input vector"),
            Self::InsufficientSamples { sample_count } => write!(
                f,
                "PCA finalization requires at least two samples, got {sample_count}"
            ),
            Self::NonFiniteInput { context, index } => {
                write!(f, "{context} contains a non-finite value at index {index}")
            }
            Self::InvalidTruncationDimension {
                requested,
                available,
            } => write!(
                f,
                "invalid truncation dimension {requested}; available output dimension is {available}"
            ),
            Self::DegenerateCovariance { index, eigenvalue } => write!(
                f,
                "covariance eigenvalue at index {index} is negative beyond tolerance: {eigenvalue}"
            ),
            Self::DecompositionFailure(message) => {
                write!(f, "PCA decomposition failed: {message}")
            }
            Self::InvalidNumericState(message) => write!(f, "invalid numeric state: {message}"),
            Self::InvalidSerializedFormat(message) => {
                write!(f, "invalid serialized PCA artifact: {message}")
            }
            Self::SchemaVersionMismatch { expected, actual } => write!(
                f,
                "schema version mismatch: expected {expected}, got {actual}"
            ),
            Self::ValidationFailure(message) => write!(f, "validation failed: {message}"),
            Self::QuantizationConfigurationError(message) => {
                write!(f, "invalid quantization configuration: {message}")
            }
        }
    }
}

impl std::error::Error for PcaError {}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ValidationTolerances {
    pub orthonormality: f64,
    pub variance: f32,
}

impl Default for ValidationTolerances {
    fn default() -> Self {
        Self {
            orthonormality: 1e-4,
            variance: 1e-6,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuantizationBits {
    I8,
    I16,
}

impl QuantizationBits {
    fn max_magnitude(self) -> i32 {
        match self {
            Self::I8 => 127,
            Self::I16 => 32_767,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuantizationConfig {
    pub bits: QuantizationBits,
}

#[derive(Clone, Debug, PartialEq)]
pub enum QuantizedValues {
    I8(Vec<i8>),
    I16(Vec<i16>),
}

impl QuantizedValues {
    fn len(&self) -> usize {
        match self {
            Self::I8(values) => values.len(),
            Self::I16(values) => values.len(),
        }
    }

    fn dequantize(&self, scale: f32) -> Vec<f32> {
        match self {
            Self::I8(values) => values
                .iter()
                .map(|value| f32::from(*value) * scale)
                .collect(),
            Self::I16(values) => values
                .iter()
                .map(|value| f32::from(*value) * scale)
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct QuantizedVector {
    pub scale: f32,
    pub values: QuantizedValues,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QuantizedPcaTransform {
    pub input_dim: usize,
    pub output_dim: usize,
    pub schema_version: u32,
    pub mean: QuantizedVector,
    pub basis_columns: Vec<QuantizedVector>,
    pub explained_variance: Option<QuantizedVector>,
}

impl QuantizedPcaTransform {
    pub fn dequantize(&self) -> Result<PcaTransform, PcaError> {
        dequantize(self)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PcaAccumulator {
    input_dim: usize,
    sample_count: u64,
    mean: Vec<f64>,
    scatter: Vec<f64>,
}

impl PcaAccumulator {
    pub fn new(dim: usize) -> Self {
        Self {
            input_dim: dim,
            sample_count: 0,
            mean: vec![0.0; dim],
            scatter: vec![0.0; dim * dim],
        }
    }

    pub fn input_dim(&self) -> usize {
        self.input_dim
    }

    pub fn sample_count(&self) -> u64 {
        self.sample_count
    }

    pub fn mean_f64(&self) -> &[f64] {
        &self.mean
    }

    pub fn scatter_f64(&self) -> &[f64] {
        &self.scatter
    }

    pub fn update(&mut self, vector: &[f32]) -> Result<(), PcaError> {
        ensure_dim("accumulator update", self.input_dim, vector.len())?;

        let values = vector
            .iter()
            .enumerate()
            .map(|(index, value)| {
                if !value.is_finite() {
                    return Err(PcaError::NonFiniteInput {
                        context: "accumulator update",
                        index,
                    });
                }
                Ok(f64::from(*value))
            })
            .collect::<Result<Vec<_>, _>>()?;

        if self.sample_count == 0 {
            self.mean = values;
            self.sample_count = 1;
            return Ok(());
        }

        let next_count = self
            .sample_count
            .checked_add(1)
            .ok_or_else(|| PcaError::InvalidNumericState("sample count overflow".into()))?;
        let next_count_f64 = next_count as f64;
        let mut delta = vec![0.0; self.input_dim];
        let mut delta2 = vec![0.0; self.input_dim];

        for (index, value) in values.iter().copied().enumerate() {
            delta[index] = value - self.mean[index];
            self.mean[index] += delta[index] / next_count_f64;
            delta2[index] = value - self.mean[index];
        }

        for (column, delta2_value) in delta2.iter().copied().enumerate() {
            for (row, delta_value) in delta.iter().copied().enumerate() {
                self.scatter[matrix_index(self.input_dim, row, column)] +=
                    delta_value * delta2_value;
            }
        }

        self.sample_count = next_count;
        Ok(())
    }

    pub fn merge(&mut self, other: &Self) -> Result<(), PcaError> {
        ensure_dim("accumulator merge", self.input_dim, other.input_dim)?;

        if other.sample_count == 0 {
            return Ok(());
        }

        if self.sample_count == 0 {
            *self = other.clone();
            return Ok(());
        }

        let total_count = self
            .sample_count
            .checked_add(other.sample_count)
            .ok_or_else(|| PcaError::InvalidNumericState("sample count overflow".into()))?;
        let self_count_f64 = self.sample_count as f64;
        let other_count_f64 = other.sample_count as f64;
        let total_count_f64 = total_count as f64;

        let delta = other
            .mean
            .iter()
            .zip(&self.mean)
            .map(|(other_mean, self_mean)| other_mean - self_mean)
            .collect::<Vec<_>>();

        let correction_scale = (self_count_f64 * other_count_f64) / total_count_f64;

        for (index, self_mean) in self.mean.iter_mut().enumerate() {
            *self_mean += delta[index] * (other_count_f64 / total_count_f64);
        }

        for (index, scatter_entry) in self.scatter.iter_mut().enumerate() {
            *scatter_entry += other.scatter[index];
        }

        for (column, delta_column) in delta.iter().copied().enumerate() {
            for (row, delta_row) in delta.iter().copied().enumerate() {
                self.scatter[matrix_index(self.input_dim, row, column)] +=
                    delta_row * delta_column * correction_scale;
            }
        }

        self.sample_count = total_count;
        Ok(())
    }

    pub fn covariance_f64(&self) -> Result<Vec<f64>, PcaError> {
        if self.sample_count == 0 {
            return Err(PcaError::EmptyInput);
        }
        if self.sample_count < 2 {
            return Err(PcaError::InsufficientSamples {
                sample_count: self.sample_count,
            });
        }

        let denominator = (self.sample_count - 1) as f64;
        Ok(self
            .scatter
            .iter()
            .map(|value| value / denominator)
            .collect())
    }

    pub fn finalize(&self) -> Result<PcaTransform, PcaError> {
        if self.input_dim == 0 {
            return Err(PcaError::ValidationFailure(
                "input_dim must be greater than zero".into(),
            ));
        }

        let covariance = self.covariance_f64()?;
        let covariance_matrix =
            DMatrix::from_column_slice(self.input_dim, self.input_dim, &covariance);
        let decomposition = SymmetricEigen::new(covariance_matrix);

        let mut eigenpairs = Vec::with_capacity(self.input_dim);
        for column in 0..self.input_dim {
            let eigenvalue = decomposition.eigenvalues[column];
            if !eigenvalue.is_finite() {
                return Err(PcaError::DecompositionFailure(
                    "eigendecomposition produced a non-finite eigenvalue".into(),
                ));
            }

            let adjusted_eigenvalue = if eigenvalue < 0.0 {
                if eigenvalue.abs() <= SMALL_NEGATIVE_EIGENVALUE_TOLERANCE {
                    0.0
                } else {
                    return Err(PcaError::DegenerateCovariance {
                        index: column,
                        eigenvalue,
                    });
                }
            } else {
                eigenvalue
            };

            let mut basis_column = decomposition
                .eigenvectors
                .column(column)
                .iter()
                .copied()
                .collect::<Vec<_>>();
            if let Some((index, _)) = basis_column
                .iter()
                .enumerate()
                .find(|(_, value)| !value.is_finite())
            {
                return Err(PcaError::DecompositionFailure(format!(
                    "eigendecomposition produced a non-finite basis value at index {index}"
                )));
            }
            canonicalize_sign(&mut basis_column);
            eigenpairs.push((column, adjusted_eigenvalue, basis_column));
        }

        eigenpairs.sort_by(|left, right| match right.1.total_cmp(&left.1) {
            Ordering::Equal => left.0.cmp(&right.0),
            ordering => ordering,
        });

        let basis = eigenpairs
            .iter()
            .flat_map(|(_, _, column)| column.iter().map(|value| *value as f32))
            .collect::<Vec<_>>();
        let explained_variance = Some(
            eigenpairs
                .iter()
                .map(|(_, eigenvalue, _)| *eigenvalue as f32)
                .collect::<Vec<_>>(),
        );
        let transform = PcaTransform {
            input_dim: self.input_dim,
            output_dim: self.input_dim,
            mean: self.mean.iter().map(|value| *value as f32).collect(),
            basis,
            explained_variance,
            schema_version: CURRENT_SCHEMA_VERSION,
        };
        transform.validate()?;
        Ok(transform)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PcaTransform {
    pub input_dim: usize,
    pub output_dim: usize,
    pub mean: Vec<f32>,
    pub basis: Vec<f32>,
    pub explained_variance: Option<Vec<f32>>,
    pub schema_version: u32,
}

impl PcaTransform {
    pub fn apply(&self, vector: &[f32]) -> Result<Vec<f32>, PcaError> {
        ensure_dim("PCA apply", self.input_dim, vector.len())?;
        self.ensure_finite_input("PCA apply", vector)?;
        self.ensure_runtime_shape("PCA apply")?;

        let mut output = vec![0.0; self.output_dim];
        for (column, output_value) in output.iter_mut().enumerate() {
            let mut acc = 0.0f32;
            for (row, value) in vector.iter().copied().enumerate() {
                acc += self.basis[matrix_index(self.input_dim, row, column)]
                    * (value - self.mean[row]);
            }
            *output_value = acc;
        }
        Ok(output)
    }

    pub fn apply_batch(&self, input: &[&[f32]]) -> Result<Vec<Vec<f32>>, PcaError> {
        input.iter().map(|vector| self.apply(vector)).collect()
    }

    pub fn reconstruct(&self, coordinates: &[f32]) -> Result<Vec<f32>, PcaError> {
        ensure_dim("PCA reconstruct", self.output_dim, coordinates.len())?;
        self.ensure_finite_input("PCA reconstruct", coordinates)?;
        self.ensure_runtime_shape("PCA reconstruct")?;

        let mut output = self.mean.clone();
        for (column, coordinate) in coordinates.iter().copied().enumerate() {
            for (row, output_value) in output.iter_mut().enumerate() {
                *output_value += self.basis[matrix_index(self.input_dim, row, column)] * coordinate;
            }
        }
        Ok(output)
    }

    pub fn truncate(&self, k: usize) -> Result<PcaTransform, PcaError> {
        if k == 0 || k > self.output_dim {
            return Err(PcaError::InvalidTruncationDimension {
                requested: k,
                available: self.output_dim,
            });
        }

        let mut truncated = self.clone();
        truncated.output_dim = k;
        truncated.basis.truncate(self.input_dim * k);
        if let Some(explained_variance) = &self.explained_variance {
            truncated.explained_variance = Some(explained_variance[..k].to_vec());
        }
        truncated.validate()?;
        Ok(truncated)
    }

    pub fn explained_variance(&self) -> Option<&[f32]> {
        self.explained_variance.as_deref()
    }

    pub fn cumulative_variance(&self) -> Option<Vec<f32>> {
        let explained_variance = self.explained_variance.as_ref()?;
        let total = explained_variance.iter().sum::<f32>();
        if total <= 0.0 {
            return Some(vec![0.0; explained_variance.len()]);
        }

        let mut running = 0.0;
        Some(
            explained_variance
                .iter()
                .map(|value| {
                    running += *value;
                    running / total
                })
                .collect(),
        )
    }

    pub fn validate(&self) -> Result<(), PcaError> {
        self.validate_with_tolerances(&ValidationTolerances::default())
    }

    pub fn validate_with_tolerances(
        &self,
        tolerances: &ValidationTolerances,
    ) -> Result<(), PcaError> {
        if self.input_dim == 0 {
            return Err(PcaError::ValidationFailure(
                "input_dim must be greater than zero".into(),
            ));
        }
        if self.output_dim == 0 {
            return Err(PcaError::ValidationFailure(
                "output_dim must be greater than zero".into(),
            ));
        }
        if self.output_dim > self.input_dim {
            return Err(PcaError::ValidationFailure(format!(
                "output_dim {} exceeds input_dim {}",
                self.output_dim, self.input_dim
            )));
        }
        ensure_dim("PCA mean", self.input_dim, self.mean.len())?;
        ensure_dim(
            "PCA basis",
            self.input_dim * self.output_dim,
            self.basis.len(),
        )?;
        ensure_finite_slice("PCA mean", &self.mean)?;
        ensure_finite_slice("PCA basis", &self.basis)?;

        if let Some(explained_variance) = &self.explained_variance {
            ensure_dim(
                "PCA explained_variance",
                self.output_dim,
                explained_variance.len(),
            )?;
            ensure_finite_slice("PCA explained_variance", explained_variance)?;
            for (index, value) in explained_variance.iter().copied().enumerate() {
                if value < -tolerances.variance {
                    return Err(PcaError::ValidationFailure(format!(
                        "explained variance at index {index} is negative: {value}"
                    )));
                }
                if index > 0 && value > explained_variance[index - 1] + tolerances.variance {
                    return Err(PcaError::ValidationFailure(format!(
                        "explained variance is not monotone nonincreasing at index {index}"
                    )));
                }
            }
        }

        let orthonormality_error =
            orthonormality_error(self.input_dim, self.output_dim, &self.basis);
        if orthonormality_error > tolerances.orthonormality {
            return Err(PcaError::ValidationFailure(format!(
                "basis orthonormality error {orthonormality_error} exceeds tolerance {}",
                tolerances.orthonormality
            )));
        }

        Ok(())
    }

    pub fn diagnostics(&self) -> PcaDiagnostics {
        let contains_nan = self
            .mean
            .iter()
            .chain(self.basis.iter())
            .chain(self.explained_variance.iter().flatten())
            .any(|value| value.is_nan());
        let contains_inf = self
            .mean
            .iter()
            .chain(self.basis.iter())
            .chain(self.explained_variance.iter().flatten())
            .any(|value| value.is_infinite());
        let orthonormality_error =
            orthonormality_error(self.input_dim, self.output_dim, &self.basis);
        let rank_estimate = self
            .explained_variance
            .as_ref()
            .map(|values| values.iter().filter(|value| **value > 1e-6).count())
            .unwrap_or(self.output_dim);
        let condition_number = self.explained_variance.as_ref().and_then(|values| {
            let positive = values
                .iter()
                .copied()
                .filter(|value| *value > 1e-6)
                .collect::<Vec<_>>();
            if positive.is_empty() {
                None
            } else {
                let max = positive.iter().copied().reduce(f32::max).map(f64::from)?;
                let min = positive.iter().copied().reduce(f32::min).map(f64::from)?;
                Some(max / min)
            }
        });

        PcaDiagnostics {
            input_dim: self.input_dim,
            output_dim: self.output_dim,
            explained_variance: self.explained_variance.clone(),
            cumulative_variance: self.cumulative_variance(),
            orthonormality_error,
            condition_number,
            is_truncated: self.output_dim < self.input_dim,
            rank_estimate,
            contains_nan,
            contains_inf,
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>, PcaError> {
        self.validate()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(PCA_MAGIC);
        write_u32(&mut bytes, SERIALIZATION_VERSION);
        write_u32(&mut bytes, self.schema_version);
        write_u64(&mut bytes, self.input_dim as u64);
        write_u64(&mut bytes, self.output_dim as u64);
        bytes.push(u8::from(self.explained_variance.is_some()));
        write_f32_slice(&mut bytes, &self.mean);
        write_f32_slice(&mut bytes, &self.basis);
        if let Some(explained_variance) = &self.explained_variance {
            write_f32_slice(&mut bytes, explained_variance);
        }
        Ok(bytes)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, PcaError> {
        let mut offset = 0;
        expect_magic(bytes, &mut offset, PCA_MAGIC)?;
        let serialization_version = read_u32(bytes, &mut offset)?;
        if serialization_version != SERIALIZATION_VERSION {
            return Err(PcaError::SchemaVersionMismatch {
                expected: SERIALIZATION_VERSION,
                actual: serialization_version,
            });
        }
        let schema_version = read_u32(bytes, &mut offset)?;
        if schema_version != CURRENT_SCHEMA_VERSION {
            return Err(PcaError::SchemaVersionMismatch {
                expected: CURRENT_SCHEMA_VERSION,
                actual: schema_version,
            });
        }
        let input_dim = read_usize(bytes, &mut offset, "input_dim")?;
        let output_dim = read_usize(bytes, &mut offset, "output_dim")?;
        let has_variance = read_u8(bytes, &mut offset)? != 0;
        let mean = read_f32_vec(bytes, &mut offset, input_dim)?;
        let basis = read_f32_vec(
            bytes,
            &mut offset,
            input_dim
                .checked_mul(output_dim)
                .ok_or_else(|| PcaError::InvalidSerializedFormat("basis length overflow".into()))?,
        )?;
        let explained_variance = if has_variance {
            Some(read_f32_vec(bytes, &mut offset, output_dim)?)
        } else {
            None
        };
        if offset != bytes.len() {
            return Err(PcaError::InvalidSerializedFormat(
                "unexpected trailing bytes".into(),
            ));
        }

        let transform = Self {
            input_dim,
            output_dim,
            mean,
            basis,
            explained_variance,
            schema_version,
        };
        transform.validate()?;
        Ok(transform)
    }

    pub fn quantize(&self, config: QuantizationConfig) -> Result<QuantizedPcaTransform, PcaError> {
        self.validate()?;
        let mean = quantize_slice(&self.mean, config)?;
        let basis_columns = (0..self.output_dim)
            .map(|column| {
                let start = column * self.input_dim;
                let end = start + self.input_dim;
                quantize_slice(&self.basis[start..end], config)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let explained_variance = self
            .explained_variance
            .as_ref()
            .map(|values| quantize_slice(values, config))
            .transpose()?;

        Ok(QuantizedPcaTransform {
            input_dim: self.input_dim,
            output_dim: self.output_dim,
            schema_version: self.schema_version,
            mean,
            basis_columns,
            explained_variance,
        })
    }

    pub fn to_affine(&self) -> AffineTransform {
        let mut matrix = vec![0.0; self.output_dim * self.input_dim];
        let mut bias = vec![0.0; self.output_dim];

        for input_column in 0..self.input_dim {
            for output_row in 0..self.output_dim {
                let value = self.basis[matrix_index(self.input_dim, input_column, output_row)];
                matrix[matrix_index(self.output_dim, output_row, input_column)] = value;
                bias[output_row] -= value * self.mean[input_column];
            }
        }

        AffineTransform {
            input_dim: self.input_dim,
            output_dim: self.output_dim,
            matrix,
            bias,
            schema_version: self.schema_version,
        }
    }

    fn inverse_affine(&self) -> AffineTransform {
        let mut matrix = vec![0.0; self.input_dim * self.output_dim];
        for column in 0..self.output_dim {
            for row in 0..self.input_dim {
                matrix[matrix_index(self.input_dim, row, column)] =
                    self.basis[matrix_index(self.input_dim, row, column)];
            }
        }
        AffineTransform {
            input_dim: self.output_dim,
            output_dim: self.input_dim,
            matrix,
            bias: self.mean.clone(),
            schema_version: self.schema_version,
        }
    }

    fn ensure_finite_input(&self, context: &'static str, values: &[f32]) -> Result<(), PcaError> {
        ensure_finite_slice(context, values)
    }

    fn ensure_runtime_shape(&self, context: &'static str) -> Result<(), PcaError> {
        ensure_dim(context, self.input_dim, self.mean.len())?;
        ensure_dim(context, self.input_dim * self.output_dim, self.basis.len())?;
        ensure_finite_slice(context, &self.mean)?;
        ensure_finite_slice(context, &self.basis)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PcaDiagnostics {
    pub input_dim: usize,
    pub output_dim: usize,
    pub explained_variance: Option<Vec<f32>>,
    pub cumulative_variance: Option<Vec<f32>>,
    pub orthonormality_error: f64,
    pub condition_number: Option<f64>,
    pub is_truncated: bool,
    pub rank_estimate: usize,
    pub contains_nan: bool,
    pub contains_inf: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AffineTransform {
    pub input_dim: usize,
    pub output_dim: usize,
    pub matrix: Vec<f32>,
    pub bias: Vec<f32>,
    pub schema_version: u32,
}

impl AffineTransform {
    pub fn apply(&self, vector: &[f32]) -> Result<Vec<f32>, PcaError> {
        ensure_dim("affine apply", self.input_dim, vector.len())?;
        ensure_finite_slice("affine apply", vector)?;
        self.validate_operable("affine apply")?;

        let mut output = self.bias.clone();
        for (column, value) in vector.iter().copied().enumerate() {
            for (row, output_value) in output.iter_mut().enumerate() {
                *output_value += self.matrix[matrix_index(self.output_dim, row, column)] * value;
            }
        }
        Ok(output)
    }

    pub fn compose(first: &AffineTransform, second: &AffineTransform) -> Result<Self, PcaError> {
        first.validate_operable("affine composition")?;
        second.validate_operable("affine composition")?;
        ensure_dim("affine composition", first.output_dim, second.input_dim)?;

        let mut matrix = vec![0.0; first.input_dim * second.output_dim];
        for input_column in 0..first.input_dim {
            for output_row in 0..second.output_dim {
                let mut value = 0.0;
                for shared in 0..first.output_dim {
                    value += second.matrix[matrix_index(second.output_dim, output_row, shared)]
                        * first.matrix[matrix_index(first.output_dim, shared, input_column)];
                }
                matrix[matrix_index(second.output_dim, output_row, input_column)] = value;
            }
        }

        let mut bias = second.bias.clone();
        for shared in 0..first.output_dim {
            let first_bias = first.bias[shared];
            for (row, bias_value) in bias.iter_mut().enumerate() {
                *bias_value +=
                    second.matrix[matrix_index(second.output_dim, row, shared)] * first_bias;
            }
        }

        Ok(Self {
            input_dim: first.input_dim,
            output_dim: second.output_dim,
            matrix,
            bias,
            schema_version: second.schema_version,
        })
    }

    pub fn serialize(&self) -> Result<Vec<u8>, PcaError> {
        self.validate_operable("affine serialize")?;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(AFFINE_MAGIC);
        write_u32(&mut bytes, SERIALIZATION_VERSION);
        write_u32(&mut bytes, self.schema_version);
        write_u64(&mut bytes, self.input_dim as u64);
        write_u64(&mut bytes, self.output_dim as u64);
        write_f32_slice(&mut bytes, &self.matrix);
        write_f32_slice(&mut bytes, &self.bias);
        Ok(bytes)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, PcaError> {
        let mut offset = 0;
        expect_magic(bytes, &mut offset, AFFINE_MAGIC)?;
        let serialization_version = read_u32(bytes, &mut offset)?;
        if serialization_version != SERIALIZATION_VERSION {
            return Err(PcaError::SchemaVersionMismatch {
                expected: SERIALIZATION_VERSION,
                actual: serialization_version,
            });
        }
        let schema_version = read_u32(bytes, &mut offset)?;
        if schema_version != CURRENT_SCHEMA_VERSION {
            return Err(PcaError::SchemaVersionMismatch {
                expected: CURRENT_SCHEMA_VERSION,
                actual: schema_version,
            });
        }
        let input_dim = read_usize(bytes, &mut offset, "input_dim")?;
        let output_dim = read_usize(bytes, &mut offset, "output_dim")?;
        let matrix = read_f32_vec(
            bytes,
            &mut offset,
            input_dim.checked_mul(output_dim).ok_or_else(|| {
                PcaError::InvalidSerializedFormat("matrix length overflow".into())
            })?,
        )?;
        let bias = read_f32_vec(bytes, &mut offset, output_dim)?;
        if offset != bytes.len() {
            return Err(PcaError::InvalidSerializedFormat(
                "unexpected trailing bytes".into(),
            ));
        }
        let transform = Self {
            input_dim,
            output_dim,
            matrix,
            bias,
            schema_version,
        };
        transform.validate_operable("affine deserialize")?;
        Ok(transform)
    }

    fn validate_operable(&self, context: &'static str) -> Result<(), PcaError> {
        ensure_dim(context, self.output_dim, self.bias.len())?;
        ensure_dim(context, self.input_dim * self.output_dim, self.matrix.len())?;
        ensure_finite_slice(context, &self.matrix)?;
        ensure_finite_slice(context, &self.bias)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeltaMode {
    Exact,
    Reconstructing,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeltaTransform {
    pub affine: AffineTransform,
    pub mode: DeltaMode,
}

impl DeltaTransform {
    pub fn apply(&self, vector: &[f32]) -> Result<Vec<f32>, PcaError> {
        self.affine.apply(vector)
    }

    pub fn is_exact(&self) -> bool {
        self.mode == DeltaMode::Exact
    }
}

pub fn fit(vectors: &[Vec<f32>]) -> Result<PcaTransform, PcaError> {
    if vectors.is_empty() {
        return Err(PcaError::EmptyInput);
    }

    let mut accumulator = PcaAccumulator::new(vectors[0].len());
    for vector in vectors {
        accumulator.update(vector)?;
    }
    accumulator.finalize()
}

pub fn fit_truncated(vectors: &[Vec<f32>], k: usize) -> Result<PcaTransform, PcaError> {
    fit(vectors)?.truncate(k)
}

pub fn apply_in_place(batch: &mut [Vec<f32>], transform: &PcaTransform) -> Result<(), PcaError> {
    for vector in batch.iter_mut() {
        *vector = transform.apply(vector)?;
    }
    Ok(())
}

pub fn compose(a: &PcaTransform, b: &PcaTransform) -> Result<AffineTransform, PcaError> {
    AffineTransform::compose(&a.to_affine(), &b.to_affine())
}

pub fn delta_exact(from: &PcaTransform, to: &PcaTransform) -> Result<DeltaTransform, PcaError> {
    if from.output_dim != from.input_dim {
        return Err(PcaError::ValidationFailure(
            "exact delta requires a full-rank source transform".into(),
        ));
    }
    let affine = delta_reconstructing(from, to)?.affine;
    Ok(DeltaTransform {
        affine,
        mode: DeltaMode::Exact,
    })
}

pub fn delta_reconstructing(
    from: &PcaTransform,
    to: &PcaTransform,
) -> Result<DeltaTransform, PcaError> {
    ensure_dim("delta transform", from.input_dim, to.input_dim)?;
    let inverse = from.inverse_affine();
    let target = to.to_affine();
    let affine = AffineTransform::compose(&inverse, &target)?;
    Ok(DeltaTransform {
        affine,
        mode: if from.output_dim == from.input_dim {
            DeltaMode::Exact
        } else {
            DeltaMode::Reconstructing
        },
    })
}

pub fn rebase_exact(
    vector: &[f32],
    from: &PcaTransform,
    to: &PcaTransform,
) -> Result<Vec<f32>, PcaError> {
    delta_exact(from, to)?.apply(vector)
}

pub fn rebase_reconstructing(
    vector: &[f32],
    from: &PcaTransform,
    to: &PcaTransform,
) -> Result<Vec<f32>, PcaError> {
    delta_reconstructing(from, to)?.apply(vector)
}

pub fn apply_delta_chain(chain: &[DeltaTransform], vector: &[f32]) -> Result<Vec<f32>, PcaError> {
    let mut current = vector.to_vec();
    for transform in chain {
        current = transform.apply(&current)?;
    }
    Ok(current)
}

pub fn dequantize(quantized: &QuantizedPcaTransform) -> Result<PcaTransform, PcaError> {
    if quantized.schema_version != CURRENT_SCHEMA_VERSION {
        return Err(PcaError::SchemaVersionMismatch {
            expected: CURRENT_SCHEMA_VERSION,
            actual: quantized.schema_version,
        });
    }
    if quantized.basis_columns.len() != quantized.output_dim {
        return Err(PcaError::ValidationFailure(format!(
            "expected {} quantized basis columns, got {}",
            quantized.output_dim,
            quantized.basis_columns.len()
        )));
    }

    let mean = dequantize_vector("quantized mean", &quantized.mean, quantized.input_dim)?;
    let mut basis = Vec::with_capacity(quantized.input_dim * quantized.output_dim);
    for column in &quantized.basis_columns {
        let dequantized = dequantize_vector("quantized basis column", column, quantized.input_dim)?;
        basis.extend(dequantized);
    }
    let explained_variance = quantized
        .explained_variance
        .as_ref()
        .map(|vector| {
            dequantize_vector("quantized explained variance", vector, quantized.output_dim)
        })
        .transpose()?;

    let transform = PcaTransform {
        input_dim: quantized.input_dim,
        output_dim: quantized.output_dim,
        mean,
        basis,
        explained_variance,
        schema_version: quantized.schema_version,
    };
    transform.validate()?;
    Ok(transform)
}

fn quantize_slice(values: &[f32], config: QuantizationConfig) -> Result<QuantizedVector, PcaError> {
    ensure_finite_slice("quantization input", values)?;
    if values.is_empty() {
        return Err(PcaError::QuantizationConfigurationError(
            "quantization input must not be empty".into(),
        ));
    }

    let max_magnitude = values
        .iter()
        .copied()
        .map(f32::abs)
        .reduce(f32::max)
        .unwrap_or(0.0);
    let qmax = config.bits.max_magnitude();
    let scale = if max_magnitude == 0.0 {
        1.0
    } else {
        max_magnitude / qmax as f32
    };

    let values = match config.bits {
        QuantizationBits::I8 => QuantizedValues::I8(
            values
                .iter()
                .copied()
                .map(|value| quantize_scalar_i8(value, scale))
                .collect(),
        ),
        QuantizationBits::I16 => QuantizedValues::I16(
            values
                .iter()
                .copied()
                .map(|value| quantize_scalar_i16(value, scale))
                .collect(),
        ),
    };

    Ok(QuantizedVector { scale, values })
}

fn quantize_scalar_i8(value: f32, scale: f32) -> i8 {
    let normalized = if scale == 0.0 {
        0.0
    } else {
        f64::from(value / scale)
    };
    let rounded = normalized.round_ties_even();
    let clipped = rounded.clamp(-127.0, 127.0);
    clipped as i8
}

fn quantize_scalar_i16(value: f32, scale: f32) -> i16 {
    let normalized = if scale == 0.0 {
        0.0
    } else {
        f64::from(value / scale)
    };
    let rounded = normalized.round_ties_even();
    let clipped = rounded.clamp(-32_767.0, 32_767.0);
    clipped as i16
}

fn dequantize_vector(
    context: &'static str,
    vector: &QuantizedVector,
    expected_len: usize,
) -> Result<Vec<f32>, PcaError> {
    if !vector.scale.is_finite() || vector.scale <= 0.0 {
        return Err(PcaError::ValidationFailure(format!(
            "{context} scale must be finite and positive"
        )));
    }
    if vector.values.len() != expected_len {
        return Err(PcaError::ValidationFailure(format!(
            "{context} length mismatch: expected {expected_len}, got {}",
            vector.values.len()
        )));
    }
    Ok(vector.values.dequantize(vector.scale))
}

fn canonicalize_sign(values: &mut [f64]) {
    let pivot = values
        .iter()
        .enumerate()
        .max_by(|(left_index, left_value), (right_index, right_value)| {
            let magnitude_order = left_value.abs().total_cmp(&right_value.abs());
            if magnitude_order == Ordering::Equal {
                right_index.cmp(left_index)
            } else {
                magnitude_order
            }
        })
        .map(|(index, _)| index)
        .unwrap_or(0);
    if values[pivot].is_sign_negative() {
        for value in values.iter_mut() {
            *value = -*value;
        }
    }
}

fn ensure_dim(context: &'static str, expected: usize, actual: usize) -> Result<(), PcaError> {
    if expected != actual {
        return Err(PcaError::DimensionMismatch {
            context,
            expected,
            actual,
        });
    }
    Ok(())
}

fn ensure_finite_slice(context: &'static str, values: &[f32]) -> Result<(), PcaError> {
    for (index, value) in values.iter().enumerate() {
        if !value.is_finite() {
            return Err(PcaError::NonFiniteInput { context, index });
        }
    }
    Ok(())
}

fn orthonormality_error(input_dim: usize, output_dim: usize, basis: &[f32]) -> f64 {
    let mut sum = 0.0;
    for left in 0..output_dim {
        for right in 0..output_dim {
            let mut dot = 0.0f64;
            for row in 0..input_dim {
                dot += f64::from(basis[matrix_index(input_dim, row, left)])
                    * f64::from(basis[matrix_index(input_dim, row, right)]);
            }
            let expected = if left == right { 1.0 } else { 0.0 };
            let delta = dot - expected;
            sum += delta * delta;
        }
    }
    sum.sqrt()
}

fn matrix_index(row_count: usize, row: usize, column: usize) -> usize {
    row + column * row_count
}

fn write_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn write_f32_slice(bytes: &mut Vec<u8>, values: &[f32]) {
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
}

fn expect_magic(bytes: &[u8], offset: &mut usize, expected: &[u8; 4]) -> Result<(), PcaError> {
    let actual = bytes
        .get(*offset..(*offset + expected.len()))
        .ok_or_else(|| PcaError::InvalidSerializedFormat("missing magic bytes".into()))?;
    if actual != expected {
        return Err(PcaError::InvalidSerializedFormat(
            "unexpected magic bytes".into(),
        ));
    }
    *offset += expected.len();
    Ok(())
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> Result<u8, PcaError> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| PcaError::InvalidSerializedFormat("unexpected end of input".into()))?;
    *offset += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, PcaError> {
    let slice = bytes
        .get(*offset..(*offset + 4))
        .ok_or_else(|| PcaError::InvalidSerializedFormat("unexpected end of input".into()))?;
    *offset += 4;
    let mut array = [0u8; 4];
    array.copy_from_slice(slice);
    Ok(u32::from_le_bytes(array))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, PcaError> {
    let slice = bytes
        .get(*offset..(*offset + 8))
        .ok_or_else(|| PcaError::InvalidSerializedFormat("unexpected end of input".into()))?;
    *offset += 8;
    let mut array = [0u8; 8];
    array.copy_from_slice(slice);
    Ok(u64::from_le_bytes(array))
}

fn read_usize(bytes: &[u8], offset: &mut usize, field: &'static str) -> Result<usize, PcaError> {
    usize::try_from(read_u64(bytes, offset)?)
        .map_err(|_| PcaError::InvalidSerializedFormat(format!("{field} does not fit in usize")))
}

fn read_f32_vec(bytes: &[u8], offset: &mut usize, count: usize) -> Result<Vec<f32>, PcaError> {
    let byte_count = count
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| PcaError::InvalidSerializedFormat("f32 byte count overflow".into()))?;
    let slice = bytes
        .get(*offset..(*offset + byte_count))
        .ok_or_else(|| PcaError::InvalidSerializedFormat("unexpected end of input".into()))?;
    *offset += byte_count;

    let mut values = Vec::with_capacity(count);
    for chunk in slice.chunks_exact(4) {
        let mut array = [0u8; 4];
        array.copy_from_slice(chunk);
        values.push(f32::from_le_bytes(array));
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::{QuantizationBits, QuantizationConfig, quantize_scalar_i8};

    #[test]
    fn ties_to_even_quantization_matches_expected_boundaries() {
        let scale = 1.0;
        assert_eq!(quantize_scalar_i8(2.5, scale), 2);
        assert_eq!(quantize_scalar_i8(3.5, scale), 4);
        assert_eq!(quantize_scalar_i8(-2.5, scale), -2);
        assert_eq!(quantize_scalar_i8(-3.5, scale), -4);

        let config = QuantizationConfig {
            bits: QuantizationBits::I8,
        };
        let quantized = super::quantize_slice(&[-500.0, 500.0], config).unwrap();
        if let super::QuantizedValues::I8(values) = quantized.values {
            assert_eq!(values, vec![-127, 127]);
        } else {
            panic!("expected i8 quantization");
        }
    }
}
