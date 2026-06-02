use tch::{Device, Kind, Tensor};

use crate::{DcbcError, NumericBackend};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TorchBackend {
    device: Device,
}

impl TorchBackend {
    pub fn new(device: Device) -> Self {
        Self { device }
    }

    pub fn device(&self) -> Device {
        self.device
    }
}

impl Default for TorchBackend {
    fn default() -> Self {
        Self {
            device: Device::Cpu,
        }
    }
}

impl NumericBackend for TorchBackend {
    fn pairwise_cosine_distances(
        &self,
        normalized_points: &[&[f64]],
        normalized_centroids: &[&[f64]],
    ) -> Result<Vec<f64>, DcbcError> {
        let point_count = normalized_points.len();
        let centroid_count = normalized_centroids.len();
        if point_count == 0 || centroid_count == 0 {
            return Err(DcbcError::BackendFailure(
                "torch backend requires at least one point and one centroid".into(),
            ));
        }

        let dimension = normalized_points[0].len();
        let mut flat_points = Vec::with_capacity(point_count * dimension);
        for point in normalized_points {
            if point.len() != dimension {
                return Err(DcbcError::BackendFailure(
                    "torch backend received mixed point dimensions".into(),
                ));
            }
            flat_points.extend_from_slice(point);
        }

        let mut flat_centroids = Vec::with_capacity(centroid_count * dimension);
        for centroid in normalized_centroids {
            if centroid.len() != dimension {
                return Err(DcbcError::BackendFailure(
                    "torch backend received mixed centroid dimensions".into(),
                ));
            }
            flat_centroids.extend_from_slice(centroid);
        }

        let points = Tensor::f_from_slice(&flat_points)
            .map_err(|error| DcbcError::BackendFailure(error.to_string()))?
            .view([point_count as i64, dimension as i64])
            .to_device(self.device)
            .to_kind(Kind::Double);
        let centroids = Tensor::f_from_slice(&flat_centroids)
            .map_err(|error| DcbcError::BackendFailure(error.to_string()))?
            .view([centroid_count as i64, dimension as i64])
            .to_device(self.device)
            .to_kind(Kind::Double);
        let distances = (Tensor::ones(
            [point_count as i64, centroid_count as i64],
            (Kind::Double, self.device),
        ) - points.matmul(&centroids.transpose(0, 1)))
        .to_device(Device::Cpu)
        .to_kind(Kind::Double);

        let numel = distances.numel();
        let mut values = vec![0.0; numel];
        distances
            .f_copy_data(&mut values, numel)
            .map_err(|error| DcbcError::BackendFailure(error.to_string()))?;
        Ok(values)
    }
}
