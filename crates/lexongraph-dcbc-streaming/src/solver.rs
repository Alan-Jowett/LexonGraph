// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use crate::{assignment_cost, constraint_error, costs_equal};
use lexongraph_streaming_clustering::StreamingClusteringError;

// For normalized vectors, cosine distance stays in [0, 2]. Using a penalty
// strictly above that range keeps every mandatory cluster slot cheaper than
// replacing it with an optional slot in the min-cost-flow formulation.
const OPTIONAL_CAPACITY_PENALTY: f64 = 8.0;

pub(crate) fn solve_lexicographic_assignment(
    distances: &[f64],
    point_count: usize,
    cluster_count: usize,
    min_cluster_size: usize,
    max_cluster_size: usize,
    mut observe_progress: impl FnMut(usize, usize),
) -> Result<Vec<usize>, StreamingClusteringError> {
    let total_progress_units = point_count.saturating_add(1);
    let mut fixed = vec![None; point_count];
    let optimal = solve_subproblem(
        distances,
        point_count,
        cluster_count,
        min_cluster_size,
        max_cluster_size,
        &fixed,
    )?
    .ok_or_else(|| {
        constraint_error("the assignment solver could not produce a feasible assignment")
    })?;
    let optimal_cost = optimal.true_cost;
    observe_progress(1, total_progress_units);

    for point_index in 0..point_count {
        let mut chosen = None;
        for cluster_index in 0..cluster_count {
            fixed[point_index] = Some(cluster_index);
            let Some(candidate) = solve_subproblem(
                distances,
                point_count,
                cluster_count,
                min_cluster_size,
                max_cluster_size,
                &fixed,
            )?
            else {
                fixed[point_index] = None;
                continue;
            };

            if costs_equal(candidate.true_cost, optimal_cost) {
                chosen = Some(cluster_index);
                break;
            }

            fixed[point_index] = None;
        }

        let Some(cluster_index) = chosen else {
            return Err(constraint_error(
                "the assignment solver could not preserve the lexicographically minimal optimum",
            ));
        };
        fixed[point_index] = Some(cluster_index);
        observe_progress(point_index.saturating_add(2), total_progress_units);
    }

    fixed
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| constraint_error("the assignment solver failed to finalize all assignments"))
}

#[derive(Clone, Debug)]
struct SubproblemSolution {
    true_cost: f64,
}

fn solve_subproblem(
    distances: &[f64],
    point_count: usize,
    cluster_count: usize,
    min_cluster_size: usize,
    max_cluster_size: usize,
    fixed: &[Option<usize>],
) -> Result<Option<SubproblemSolution>, StreamingClusteringError> {
    let mut committed = vec![0usize; cluster_count];
    let mut remaining_points = Vec::new();

    for (point_index, fixed_cluster) in fixed.iter().enumerate() {
        match fixed_cluster {
            Some(cluster_index) if *cluster_index < cluster_count => {
                committed[*cluster_index] += 1;
                if committed[*cluster_index] > max_cluster_size {
                    return Ok(None);
                }
            }
            Some(_) => return Ok(None),
            None => remaining_points.push(point_index),
        }
    }

    let mut remaining_lower = vec![0usize; cluster_count];
    let mut remaining_upper = vec![0usize; cluster_count];
    let mut required_assignments = 0usize;
    let mut available_assignments = 0usize;
    for cluster_index in 0..cluster_count {
        let lower = min_cluster_size.saturating_sub(committed[cluster_index]);
        let upper = max_cluster_size.saturating_sub(committed[cluster_index]);
        if lower > upper {
            return Ok(None);
        }
        remaining_lower[cluster_index] = lower;
        remaining_upper[cluster_index] = upper;
        required_assignments += lower;
        available_assignments += upper;
    }

    if required_assignments > remaining_points.len()
        || remaining_points.len() > available_assignments
    {
        return Ok(None);
    }

    let source = 0usize;
    let point_base = 1usize;
    let cluster_base = point_base + remaining_points.len();
    let sink = cluster_base + cluster_count;
    let mut flow = MinCostFlow::new(sink + 1);

    for point_offset in 0..remaining_points.len() {
        let point_node = point_base + point_offset;
        flow.add_edge(source, point_node, 1, 0.0);
        for cluster_index in 0..cluster_count {
            let cost = distances[remaining_points[point_offset] * cluster_count + cluster_index];
            flow.add_edge(point_node, cluster_base + cluster_index, 1, cost);
        }
    }

    for cluster_index in 0..cluster_count {
        let cluster_node = cluster_base + cluster_index;
        if remaining_lower[cluster_index] > 0 {
            flow.add_edge(cluster_node, sink, remaining_lower[cluster_index], 0.0);
        }
        let optional_capacity = remaining_upper[cluster_index] - remaining_lower[cluster_index];
        if optional_capacity > 0 {
            flow.add_edge(
                cluster_node,
                sink,
                optional_capacity,
                OPTIONAL_CAPACITY_PENALTY,
            );
        }
    }

    let sent = flow.send_flow(source, sink, remaining_points.len())?;
    if sent != remaining_points.len() {
        return Ok(None);
    }

    let mut assignment = vec![usize::MAX; point_count];
    for (point_index, cluster_index) in fixed.iter().enumerate() {
        if let Some(cluster_index) = cluster_index {
            assignment[point_index] = *cluster_index;
        }
    }

    for (point_offset, &point_index) in remaining_points.iter().enumerate() {
        let point_node = point_base + point_offset;
        let mut assigned_cluster = None;
        for edge in &flow.graph[point_node] {
            if edge.to < cluster_base || edge.to >= sink {
                continue;
            }
            if flow.graph[edge.to][edge.rev].capacity == 1 {
                assigned_cluster = Some(edge.to - cluster_base);
                break;
            }
        }
        let Some(cluster_index) = assigned_cluster else {
            return Ok(None);
        };
        assignment[point_index] = cluster_index;
    }

    if assignment.contains(&usize::MAX) {
        return Ok(None);
    }

    let true_cost = assignment_cost(distances, cluster_count, assignment.as_slice())?;
    Ok(Some(SubproblemSolution { true_cost }))
}

#[derive(Clone, Debug)]
struct Edge {
    to: usize,
    rev: usize,
    capacity: usize,
    cost: f64,
}

#[derive(Clone, Debug)]
struct MinCostFlow {
    graph: Vec<Vec<Edge>>,
}

impl MinCostFlow {
    fn new(node_count: usize) -> Self {
        Self {
            graph: vec![Vec::new(); node_count],
        }
    }

    fn add_edge(&mut self, from: usize, to: usize, capacity: usize, cost: f64) {
        let forward_rev = self.graph[to].len();
        let backward_rev = self.graph[from].len();
        self.graph[from].push(Edge {
            to,
            rev: forward_rev,
            capacity,
            cost,
        });
        self.graph[to].push(Edge {
            to: from,
            rev: backward_rev,
            capacity: 0,
            cost: -cost,
        });
    }

    fn send_flow(
        &mut self,
        source: usize,
        sink: usize,
        target_flow: usize,
    ) -> Result<usize, StreamingClusteringError> {
        let node_count = self.graph.len();
        let mut sent = 0usize;

        while sent < target_flow {
            let mut dist = vec![f64::INFINITY; node_count];
            let mut prev_node = vec![usize::MAX; node_count];
            let mut prev_edge = vec![usize::MAX; node_count];
            dist[source] = 0.0;

            for _ in 0..node_count.saturating_sub(1) {
                let mut updated = false;
                for node in 0..node_count {
                    if !dist[node].is_finite() {
                        continue;
                    }
                    for edge_index in 0..self.graph[node].len() {
                        let edge = &self.graph[node][edge_index];
                        if edge.capacity == 0 || edge.to == source {
                            continue;
                        }
                        let candidate = dist[node] + edge.cost;
                        if candidate + crate::EPSILON < dist[edge.to] {
                            dist[edge.to] = candidate;
                            prev_node[edge.to] = node;
                            prev_edge[edge.to] = edge_index;
                            updated = true;
                        }
                    }
                }

                if !updated {
                    break;
                }
            }

            if prev_node[sink] == usize::MAX {
                break;
            }

            for node in 0..node_count {
                if !dist[node].is_finite() {
                    continue;
                }
                for edge in &self.graph[node] {
                    if edge.capacity == 0 {
                        continue;
                    }
                    if dist[node] + edge.cost + crate::EPSILON < dist[edge.to] {
                        return Err(constraint_error(
                            "negative-cost residual cycle detected in assignment solver",
                        ));
                    }
                }
            }

            let mut augment = target_flow - sent;
            let mut node = sink;
            let mut steps = 0usize;
            while node != source {
                steps += 1;
                if steps > node_count {
                    return Err(constraint_error(
                        "assignment solver predecessor chain became cyclic",
                    ));
                }
                let parent = prev_node[node];
                let edge_index = prev_edge[node];
                augment = augment.min(self.graph[parent][edge_index].capacity);
                node = parent;
            }

            let mut node = sink;
            let mut steps = 0usize;
            while node != source {
                steps += 1;
                if steps > node_count {
                    return Err(constraint_error(
                        "assignment solver predecessor chain became cyclic",
                    ));
                }
                let parent = prev_node[node];
                let edge_index = prev_edge[node];
                let reverse_index = self.graph[parent][edge_index].rev;
                self.graph[parent][edge_index].capacity -= augment;
                self.graph[node][reverse_index].capacity += augment;
                node = parent;
            }

            sent += augment;
        }

        Ok(sent)
    }
}
