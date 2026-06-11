// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use ciborium::value::Value as CborValue;
use lexongraph_block::{
    Block, BranchEntry, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_branch_block,
    build_leaf_block,
};
use lexongraph_block_store::BlockStore;
use lexongraph_block_store_fs::FilesystemBlockStore;
use lexongraph_streaming_clustering::validate_embedding;
use serde::{Deserialize, Serialize};
use tempfile::tempdir_in;
use zip::CompressionMethod as ZipCompressionMethod;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

use crate::{
    AlignmentPolicy, BenchmarkProfile, BlockStoreCorpusReference, BlockStoreEvaluationCorpus,
    BlockStoreReferenceStore, CandidateRunStatus, CompressionBenchmark, DeferredResearchGoal,
    EmbeddingWorkloadSource, EvaluationEntity, EvaluationEntitySource, EvaluatorError,
    GateDeclaration, GateKind, GroundTruthNeighborhood, MetricDeclaration, MetricKind,
    ProbeWorkload, RegisteredCandidate, ReproducibilityMetadata, ResearchCoverage,
    SharedBalanceConstraints, SharedCandidateConfig, TrainingPassSource,
    built_in_fixture_candidate, decode_embedding_to_f32, emit_campaign_artifacts,
    load_leaf_records, metadata_value, run_evaluation_campaign, write_campaign_artifacts,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section4MetricContract {
    Cosine,
    Euclidean,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Section4CorpusFamily {
    RealWorldHarvested,
    WellClusteredSynthetic,
    WeakClusterUniform,
    AnisotropicManifold,
    NearDuplicateHeavy,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source_kind", rename_all = "kebab-case")]
pub enum Section4ProfileSourceSpec {
    Synthetic {
        family: Section4CorpusFamily,
        real_entity_count: usize,
        alignment_policy: AlignmentPolicy,
    },
    Harvested {
        family: Section4CorpusFamily,
        source: BlockStoreCorpusReference,
        entity_id_metadata_key: String,
        real_entity_count: usize,
        alignment_policy: AlignmentPolicy,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section4ProfileSpec {
    pub profile_id: String,
    pub corpus_id: String,
    pub scale_tier_id: String,
    pub source: Section4ProfileSourceSpec,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section4SuiteSpec {
    pub suite_id: String,
    pub leaf_size: usize,
    pub dimensions: usize,
    pub batch_size: usize,
    pub metric_contract: Section4MetricContract,
    pub neighbor_count: usize,
    pub balance_constraints: Option<SharedBalanceConstraints>,
    pub random_seed: Option<u64>,
    pub compression_benchmark: CompressionBenchmark,
    pub reproducibility: ReproducibilityMetadata,
    pub profiles: Vec<Section4ProfileSpec>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section4GeneratedProfile {
    pub profile_id: String,
    pub corpus_id: String,
    pub family: Section4CorpusFamily,
    pub scale_tier_id: String,
    pub alignment_policy: AlignmentPolicy,
    pub metric_contract: Section4MetricContract,
    pub neighbor_count: usize,
    pub real_entity_count: usize,
    pub evaluated_entity_count: usize,
    pub cluster_count: u32,
    #[serde(
        serialize_with = "crate::serialize_portable_pathbuf",
        deserialize_with = "crate::deserialize_cross_platform_pathbuf"
    )]
    pub profile_path: PathBuf,
    #[serde(
        serialize_with = "crate::serialize_portable_pathbuf",
        deserialize_with = "crate::deserialize_cross_platform_pathbuf"
    )]
    pub corpus_archive_path: PathBuf,
    pub root_block_id: String,
    pub harvested_source_id: Option<String>,
    pub harvested_source_root_block_id: Option<String>,
    pub harvested_entity_id_metadata_key: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section4SuiteManifest {
    pub suite_id: String,
    pub generated_profiles: Vec<Section4GeneratedProfile>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section4SuiteRunCandidateReport {
    pub candidate_id: String,
    pub run_status: CandidateRunStatus,
    pub survived_required_gates: bool,
    pub ranking_score: Option<f64>,
    pub build_elapsed_nanos: u128,
    pub build_time_per_vector_nanos: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section4SuiteRunProfileReport {
    pub profile_id: String,
    pub corpus_id: String,
    pub family: Section4CorpusFamily,
    pub scale_tier_id: String,
    pub real_entity_count: usize,
    pub evaluated_entity_count: usize,
    pub candidate_reports: Vec<Section4SuiteRunCandidateReport>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Section4SuiteRunReport {
    pub suite_id: String,
    pub profile_reports: Vec<Section4SuiteRunProfileReport>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Section4SuiteRunArtifacts {
    pub suite_report_path: PathBuf,
    pub scorecard_path: PathBuf,
    pub profile_output_dirs: Vec<PathBuf>,
}

pub fn generate_section4_suite_assets(
    spec: &Section4SuiteSpec,
    output_dir: &Path,
) -> Result<Section4SuiteManifest, EvaluatorError> {
    validate_suite_spec(spec)?;
    std::fs::create_dir_all(output_dir).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to create suite output directory {}: {error}",
            output_dir.display()
        ))
    })?;
    let corpora_dir = output_dir.join("corpora");
    let profiles_dir = output_dir.join("profiles");
    std::fs::create_dir_all(&corpora_dir).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to create corpora directory {}: {error}",
            corpora_dir.display()
        ))
    })?;
    std::fs::create_dir_all(&profiles_dir).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to create profiles directory {}: {error}",
            profiles_dir.display()
        ))
    })?;

    let mut generated_profiles = Vec::with_capacity(spec.profiles.len());
    for profile_spec in &spec.profiles {
        let family = profile_spec.source.family().clone();
        let alignment_policy = profile_spec.source.alignment_policy().clone();
        let harvested_source_metadata = match &profile_spec.source {
            Section4ProfileSourceSpec::Synthetic { .. } => (None, None, None),
            Section4ProfileSourceSpec::Harvested {
                source,
                entity_id_metadata_key,
                ..
            } => (
                Some(source.source_id.clone()),
                Some(source.root_block_id.clone()),
                Some(entity_id_metadata_key.clone()),
            ),
        };
        let real_entities = materialize_real_entities(profile_spec, spec)?;
        let evaluation_entities =
            apply_alignment_policy(&real_entities, spec.leaf_size, &alignment_policy)?;
        let cluster_count =
            u32::try_from(evaluation_entities.len() / spec.leaf_size).map_err(|_| {
                EvaluatorError::InvalidConfiguration(format!(
                    "profile {} cluster_count overflowed u32",
                    profile_spec.profile_id
                ))
            })?;
        let training_source_id = format!("{}-training", profile_spec.profile_id);
        let probe_source_id = format!("{}-probe", profile_spec.profile_id);
        let evaluation_source_id = format!("{}-evaluation", profile_spec.profile_id);
        let corpus_archive_path = corpora_dir.join(format!("{}.zip", profile_spec.profile_id));
        let corpus_reference = materialize_corpus_archive(
            &corpus_archive_path,
            &evaluation_entities,
            &evaluation_source_id,
        )?;
        let training_reference =
            clone_reference_with_source_id(&corpus_reference, &training_source_id);
        let probe_reference = clone_reference_with_source_id(&corpus_reference, &probe_source_id);

        let locality_ground_truth = compute_ground_truth(
            &real_entities,
            spec.metric_contract.clone(),
            spec.neighbor_count,
        )?;
        let profile = BenchmarkProfile {
            profile_id: profile_spec.profile_id.clone(),
            corpus_ids: vec![profile_spec.corpus_id.clone()],
            shared_candidate_config: SharedCandidateConfig {
                cluster_count,
                dimensions: spec.dimensions,
                balance_constraints: spec.balance_constraints.clone(),
                random_seed: spec.random_seed,
            },
            training_passes: vec![TrainingPassSource::BlockStore {
                corpus: training_reference,
                batch_size: spec.batch_size,
            }],
            probe_workloads: vec![ProbeWorkload {
                workload_id: format!("{}-probes", profile_spec.profile_id),
                source: EmbeddingWorkloadSource::BlockStore {
                    corpus: probe_reference,
                },
            }],
            evaluation_entities: EvaluationEntitySource::BlockStore {
                corpora: vec![BlockStoreEvaluationCorpus {
                    corpus_id: profile_spec.corpus_id.clone(),
                    corpus: corpus_reference.clone(),
                    entity_id_metadata_key: "entity_id".into(),
                    synthetic_metadata_key: Some("synthetic".into()),
                }],
            },
            leaf_model: crate::LeafModel {
                leaf_size: spec.leaf_size,
                declared_final_cluster_count: cluster_count,
                alignment_policy: alignment_policy.clone(),
            },
            locality_ground_truth,
            compression_benchmark: spec.compression_benchmark.clone(),
            metric_declarations: section4_metric_declarations(),
            gate_declarations: section4_gate_declarations(),
            deferred_research_goals: default_deferred_research_goals(),
            reproducibility: spec.reproducibility.clone(),
        };
        let profile_path = profiles_dir.join(format!("{}.json", profile_spec.profile_id));
        std::fs::write(
            &profile_path,
            serde_json::to_string_pretty(&profile)
                .map_err(|error| EvaluatorError::Json(error.to_string()))?,
        )
        .map_err(|error| {
            EvaluatorError::Io(format!(
                "failed to write suite profile {}: {error}",
                profile_path.display()
            ))
        })?;

        generated_profiles.push(Section4GeneratedProfile {
            profile_id: profile_spec.profile_id.clone(),
            corpus_id: profile_spec.corpus_id.clone(),
            family,
            scale_tier_id: profile_spec.scale_tier_id.clone(),
            alignment_policy,
            metric_contract: spec.metric_contract.clone(),
            neighbor_count: spec.neighbor_count,
            real_entity_count: real_entities.len(),
            evaluated_entity_count: evaluation_entities.len(),
            cluster_count,
            profile_path,
            corpus_archive_path,
            root_block_id: corpus_reference.root_block_id,
            harvested_source_id: harvested_source_metadata.0,
            harvested_source_root_block_id: harvested_source_metadata.1,
            harvested_entity_id_metadata_key: harvested_source_metadata.2,
        });
    }

    let manifest = Section4SuiteManifest {
        suite_id: spec.suite_id.clone(),
        generated_profiles,
    };
    let manifest_path = output_dir.join("section4-suite-manifest.json");
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest)
            .map_err(|error| EvaluatorError::Json(error.to_string()))?,
    )
    .map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to write suite manifest {}: {error}",
            manifest_path.display()
        ))
    })?;
    Ok(manifest)
}

pub fn run_section4_suite(
    manifest: &Section4SuiteManifest,
    candidates: &[RegisteredCandidate],
    output_dir: &Path,
) -> Result<Section4SuiteRunReport, EvaluatorError> {
    if candidates.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "at least one candidate must be registered".into(),
        ));
    }
    std::fs::create_dir_all(output_dir).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to create suite report directory {}: {error}",
            output_dir.display()
        ))
    })?;

    let mut profile_reports = Vec::with_capacity(manifest.generated_profiles.len());
    for generated in &manifest.generated_profiles {
        let profile_contents =
            std::fs::read_to_string(&generated.profile_path).map_err(|error| {
                EvaluatorError::Io(format!(
                    "failed to read generated profile {}: {error}",
                    generated.profile_path.display()
                ))
            })?;
        let profile: BenchmarkProfile = serde_json::from_str(&profile_contents)
            .map_err(|error| EvaluatorError::Json(error.to_string()))?;

        let mut timings = HashMap::new();
        for candidate in candidates {
            let started = Instant::now();
            let single_report = run_evaluation_campaign(&profile, std::slice::from_ref(candidate))?;
            let elapsed = started.elapsed().as_nanos();
            timings.insert(
                candidate.identity.candidate_id.clone(),
                (
                    elapsed,
                    single_report.run_reports[0].run_status.clone(),
                    single_report.run_reports[0].survived_required_gates,
                    single_report.run_reports[0].ranking_score,
                ),
            );
        }

        let comparative_report = run_evaluation_campaign(&profile, candidates)?;
        let comparative_artifacts = emit_campaign_artifacts(&comparative_report)?;
        let profile_output_dir = output_dir.join(&generated.profile_id);
        write_campaign_artifacts(&profile_output_dir, &comparative_artifacts)?;

        let mut candidate_reports = Vec::with_capacity(comparative_report.run_reports.len());
        for run_report in &comparative_report.run_reports {
            let (elapsed, status, survived, ranking_score) = timings
                .get(&run_report.candidate_identity.candidate_id)
                .cloned()
                .ok_or_else(|| {
                    EvaluatorError::InvalidConfiguration(format!(
                        "missing timing entry for candidate {}",
                        run_report.candidate_identity.candidate_id
                    ))
                })?;
            candidate_reports.push(Section4SuiteRunCandidateReport {
                candidate_id: run_report.candidate_identity.candidate_id.clone(),
                run_status: status,
                survived_required_gates: survived,
                ranking_score,
                build_elapsed_nanos: elapsed,
                build_time_per_vector_nanos: elapsed as f64
                    / generated.evaluated_entity_count as f64,
            });
        }

        profile_reports.push(Section4SuiteRunProfileReport {
            profile_id: generated.profile_id.clone(),
            corpus_id: generated.corpus_id.clone(),
            family: generated.family.clone(),
            scale_tier_id: generated.scale_tier_id.clone(),
            real_entity_count: generated.real_entity_count,
            evaluated_entity_count: generated.evaluated_entity_count,
            candidate_reports,
        });
    }

    Ok(Section4SuiteRunReport {
        suite_id: manifest.suite_id.clone(),
        profile_reports,
    })
}

pub fn render_section4_suite_scorecard(report: &Section4SuiteRunReport) -> String {
    let mut lines = vec![format!("Section-4 suite scorecard for {}", report.suite_id)];
    for profile in &report.profile_reports {
        lines.push(format!(
            "- {} [{} / {} / real={} / evaluated={}]",
            profile.profile_id,
            family_label(&profile.family),
            profile.scale_tier_id,
            profile.real_entity_count,
            profile.evaluated_entity_count
        ));
        for candidate in &profile.candidate_reports {
            lines.push(format!(
                "  candidate {}: {:?}, survived={}, build_time_per_vector_nanos={:.3}",
                candidate.candidate_id,
                candidate.run_status,
                candidate.survived_required_gates,
                candidate.build_time_per_vector_nanos
            ));
        }
    }
    lines.join("\n")
}

pub fn write_section4_suite_artifacts(
    report: &Section4SuiteRunReport,
    output_dir: &Path,
) -> Result<Section4SuiteRunArtifacts, EvaluatorError> {
    std::fs::create_dir_all(output_dir).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to create suite artifact directory {}: {error}",
            output_dir.display()
        ))
    })?;
    let suite_report_path = output_dir.join("section4-suite-report.json");
    std::fs::write(
        &suite_report_path,
        serde_json::to_string_pretty(report)
            .map_err(|error| EvaluatorError::Json(error.to_string()))?,
    )
    .map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to write suite report {}: {error}",
            suite_report_path.display()
        ))
    })?;
    let scorecard_path = output_dir.join("section4-suite-scorecard.txt");
    std::fs::write(&scorecard_path, render_section4_suite_scorecard(report)).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to write suite scorecard {}: {error}",
            scorecard_path.display()
        ))
    })?;

    let profile_output_dirs = report
        .profile_reports
        .iter()
        .map(|profile| output_dir.join(&profile.profile_id))
        .collect();
    Ok(Section4SuiteRunArtifacts {
        suite_report_path,
        scorecard_path,
        profile_output_dirs,
    })
}

fn validate_suite_spec(spec: &Section4SuiteSpec) -> Result<(), EvaluatorError> {
    if spec.suite_id.trim().is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "suite_id must not be empty".into(),
        ));
    }
    if spec.leaf_size == 0 {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-4 suite leaf_size must be positive".into(),
        ));
    }
    if spec.dimensions == 0 {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-4 suite dimensions must be positive".into(),
        ));
    }
    if spec.batch_size == 0 {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-4 suite batch_size must be positive".into(),
        ));
    }
    if spec.neighbor_count == 0 {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-4 suite neighbor_count must be positive".into(),
        ));
    }
    if spec.profiles.is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-4 suite must declare at least one profile".into(),
        ));
    }
    for profile in &spec.profiles {
        validate_profile_id(&profile.profile_id)?;
        let real_entity_count = match &profile.source {
            Section4ProfileSourceSpec::Synthetic {
                real_entity_count, ..
            }
            | Section4ProfileSourceSpec::Harvested {
                real_entity_count, ..
            } => *real_entity_count,
        };
        if real_entity_count <= spec.neighbor_count {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "profile {} must declare more than {} real entities to compute exact-neighbor ground truth",
                profile.profile_id, spec.neighbor_count
            )));
        }
    }
    Ok(())
}

fn validate_profile_id(profile_id: &str) -> Result<(), EvaluatorError> {
    if profile_id.trim().is_empty() {
        return Err(EvaluatorError::InvalidConfiguration(
            "section-4 suite profile_id must not be empty".into(),
        ));
    }
    if profile_id.contains('/')
        || profile_id.contains('\\')
        || profile_id == "."
        || profile_id == ".."
    {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "section-4 suite profile_id {:?} must not contain path separators or dot segments",
            profile_id
        )));
    }
    Ok(())
}

fn materialize_real_entities(
    profile_spec: &Section4ProfileSpec,
    suite_spec: &Section4SuiteSpec,
) -> Result<Vec<EvaluationEntity>, EvaluatorError> {
    match &profile_spec.source {
        Section4ProfileSourceSpec::Synthetic {
            family,
            real_entity_count,
            ..
        } => generate_synthetic_entities(
            family.clone(),
            profile_spec.corpus_id.as_str(),
            *real_entity_count,
            suite_spec.dimensions,
        ),
        Section4ProfileSourceSpec::Harvested {
            source,
            entity_id_metadata_key,
            real_entity_count,
            ..
        } => harvest_real_entities(
            source,
            entity_id_metadata_key,
            profile_spec.corpus_id.as_str(),
            *real_entity_count,
            suite_spec.dimensions,
        ),
    }
}

fn generate_synthetic_entities(
    family: Section4CorpusFamily,
    corpus_id: &str,
    count: usize,
    dimensions: usize,
) -> Result<Vec<EvaluationEntity>, EvaluatorError> {
    if count < 2 {
        return Err(EvaluatorError::InvalidConfiguration(
            "synthetic section-4 corpora require at least two real entities".into(),
        ));
    }
    let mut entities = Vec::with_capacity(count);
    for index in 0..count {
        let embedding = match family {
            Section4CorpusFamily::RealWorldHarvested => {
                return Err(EvaluatorError::InvalidConfiguration(
                    "real-world harvested family cannot be generated synthetically".into(),
                ));
            }
            Section4CorpusFamily::WellClusteredSynthetic => {
                let cluster = index % 4;
                let mut values = vec![0.0; dimensions];
                for (dim, value) in values.iter_mut().enumerate() {
                    *value = cluster as f32 * 25.0 + dim as f32 * 0.05 + (index / 4) as f32 * 0.01;
                }
                values
            }
            Section4CorpusFamily::WeakClusterUniform => (0..dimensions)
                .map(|dim| ((index * (dim + 3) + dim * 17) % 97) as f32 / 97.0)
                .collect(),
            Section4CorpusFamily::AnisotropicManifold => (0..dimensions)
                .map(|dim| {
                    let t = index as f32 / count as f32;
                    if dim == 0 {
                        t * 12.0
                    } else if dim == 1 {
                        (t * std::f32::consts::TAU).sin() * 3.0
                    } else {
                        t * (dim as f32 + 1.0) * 0.2
                    }
                })
                .collect(),
            Section4CorpusFamily::NearDuplicateHeavy => {
                let base = index % 5;
                (0..dimensions)
                    .map(|dim| base as f32 * 4.0 + dim as f32 * 0.25 + (index / 5) as f32 * 0.0005)
                    .collect()
            }
        };
        entities.push(EvaluationEntity {
            entity_id: format!("{corpus_id}-real-{index:06}"),
            corpus_id: corpus_id.into(),
            embedding,
            synthetic: false,
        });
    }
    Ok(entities)
}

fn harvest_real_entities(
    source: &BlockStoreCorpusReference,
    entity_id_metadata_key: &str,
    corpus_id: &str,
    count: usize,
    dimensions: usize,
) -> Result<Vec<EvaluationEntity>, EvaluatorError> {
    let records = load_leaf_records(source)
        .map_err(|error| EvaluatorError::InvalidConfiguration(format!("{error:?}")))?;
    let mut entities = records
        .into_iter()
        .map(|record| {
            let entity_id = match metadata_value(&record.entry.metadata, entity_id_metadata_key) {
                Some(CborValue::Text(text)) => Ok(text.clone()),
                Some(_) => Err(EvaluatorError::InvalidConfiguration(format!(
                    "metadata key {:?} in harvested source {} block {} must be text",
                    entity_id_metadata_key, source.source_id, record.block_id
                ))),
                None => Err(EvaluatorError::InvalidConfiguration(format!(
                    "metadata key {:?} was missing in harvested source {} block {}",
                    entity_id_metadata_key, source.source_id, record.block_id
                ))),
            }?;
            let synthetic = match metadata_value(&record.entry.metadata, "synthetic") {
                Some(CborValue::Bool(value)) => *value,
                Some(_) => {
                    return Err(EvaluatorError::InvalidConfiguration(format!(
                        "metadata key \"synthetic\" in harvested source {} block {} must be boolean",
                        source.source_id, record.block_id
                    )))
                }
                None => false,
            };
            let embedding = decode_embedding_to_f32(
                &record.entry.embedding,
                &record.embedding_spec,
                &format!("harvested source {} block {}", source.source_id, record.block_id),
            )
            .map_err(EvaluatorError::InvalidConfiguration)?;
            validate_embedding(&embedding, dimensions).map_err(|error| {
                EvaluatorError::InvalidConfiguration(format!(
                    "harvested source {} block {} failed embedding validation: {error}",
                    source.source_id, record.block_id
                ))
            })?;
            Ok(EvaluationEntity {
                entity_id,
                corpus_id: corpus_id.into(),
                embedding,
                synthetic,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    entities.retain(|entity| !entity.synthetic);
    entities.sort_by(|left, right| left.entity_id.cmp(&right.entity_id));
    if entities.len() < count {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "harvested source {} contains only {} real entities, requested {}",
            source.source_id,
            entities.len(),
            count
        )));
    }
    entities.truncate(count);
    Ok(entities)
}

fn apply_alignment_policy(
    real_entities: &[EvaluationEntity],
    leaf_size: usize,
    alignment_policy: &AlignmentPolicy,
) -> Result<Vec<EvaluationEntity>, EvaluatorError> {
    match alignment_policy {
        AlignmentPolicy::StrictAlignment => {
            if !real_entities.len().is_multiple_of(leaf_size) {
                return Err(EvaluatorError::InvalidConfiguration(format!(
                    "strict-alignment section-4 corpus has {} real entities, which is not divisible by leaf_size {}",
                    real_entities.len(),
                    leaf_size
                )));
            }
            Ok(real_entities.to_vec())
        }
        AlignmentPolicy::DeterministicSyntheticPadding => {
            if real_entities.is_empty() {
                return Err(EvaluatorError::InvalidConfiguration(
                    "deterministic-padding section-4 corpus must include at least one real entity"
                        .into(),
                ));
            }
            if real_entities.len().is_multiple_of(leaf_size) {
                return Err(EvaluatorError::InvalidConfiguration(format!(
                    "deterministic-padding section-4 corpus has {} real entities, which is already divisible by leaf_size {}",
                    real_entities.len(),
                    leaf_size
                )));
            }
            let target_total = real_entities.len().div_ceil(leaf_size) * leaf_size;
            let mut entities = real_entities.to_vec();
            let padding_needed = target_total - entities.len();
            let max_abs = real_entities
                .iter()
                .flat_map(|entity| entity.embedding.iter().copied())
                .fold(1.0f32, |current, value| current.max(value.abs()));
            for padding_index in 0..padding_needed {
                let mut embedding = Vec::with_capacity(real_entities[0].embedding.len());
                for dim in 0..real_entities[0].embedding.len() {
                    embedding.push(max_abs + 10.0 + padding_index as f32 + dim as f32 * 0.125);
                }
                entities.push(EvaluationEntity {
                    entity_id: format!(
                        "{}-synthetic-{padding_index:06}",
                        real_entities[0].corpus_id
                    ),
                    corpus_id: real_entities[0].corpus_id.clone(),
                    embedding,
                    synthetic: true,
                });
            }
            Ok(entities)
        }
    }
}

fn compute_ground_truth(
    real_entities: &[EvaluationEntity],
    metric_contract: Section4MetricContract,
    neighbor_count: usize,
) -> Result<Vec<GroundTruthNeighborhood>, EvaluatorError> {
    if real_entities.len() <= neighbor_count {
        return Err(EvaluatorError::InvalidConfiguration(format!(
            "section-4 ground truth requires more than {} real entities, found {}",
            neighbor_count,
            real_entities.len()
        )));
    }
    let mut ground_truth = Vec::with_capacity(real_entities.len());
    for (entity_index, entity) in real_entities.iter().enumerate() {
        let mut distances = Vec::with_capacity(real_entities.len() - 1);
        for (neighbor_index, neighbor) in real_entities.iter().enumerate() {
            if entity_index == neighbor_index {
                continue;
            }
            let distance = match metric_contract {
                Section4MetricContract::Cosine => {
                    cosine_distance(&entity.embedding, &neighbor.embedding)?
                }
                Section4MetricContract::Euclidean => {
                    euclidean_distance(&entity.embedding, &neighbor.embedding)?
                }
            };
            distances.push((distance, neighbor.entity_id.clone()));
        }
        distances.sort_by(|left, right| {
            left.0
                .partial_cmp(&right.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.1.cmp(&right.1))
        });
        ground_truth.push(GroundTruthNeighborhood {
            entity_id: entity.entity_id.clone(),
            neighbor_ids: distances
                .into_iter()
                .take(neighbor_count)
                .map(|(_, neighbor_id)| neighbor_id)
                .collect(),
        });
    }
    Ok(ground_truth)
}

fn cosine_distance(left: &[f32], right: &[f32]) -> Result<f64, EvaluatorError> {
    let mut dot = 0.0f64;
    let mut left_norm = 0.0f64;
    let mut right_norm = 0.0f64;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        dot += *left_value as f64 * *right_value as f64;
        left_norm += (*left_value as f64).powi(2);
        right_norm += (*right_value as f64).powi(2);
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return Err(EvaluatorError::InvalidConfiguration(
            "cosine ground-truth generation does not support zero-norm embeddings".into(),
        ));
    }
    Ok(1.0 - dot / (left_norm.sqrt() * right_norm.sqrt()))
}

fn euclidean_distance(left: &[f32], right: &[f32]) -> Result<f64, EvaluatorError> {
    if left.len() != right.len() {
        return Err(EvaluatorError::InvalidConfiguration(
            "euclidean ground-truth generation requires equal dimensions".into(),
        ));
    }
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left_value, right_value)| {
            let delta = *left_value as f64 - *right_value as f64;
            delta * delta
        })
        .sum())
}

fn materialize_corpus_archive(
    archive_path: &Path,
    entities: &[EvaluationEntity],
    source_id: &str,
) -> Result<BlockStoreCorpusReference, EvaluatorError> {
    let archive_parent = archive_path.parent().ok_or_else(|| {
        EvaluatorError::InvalidConfiguration(format!(
            "archive path {} has no parent directory",
            archive_path.display()
        ))
    })?;
    let temp_dir = tempdir_in(archive_parent).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to create temporary corpus directory under {}: {error}",
            archive_parent.display()
        ))
    })?;
    let store_root = temp_dir.path().join("store");
    std::fs::create_dir_all(&store_root).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to create temporary corpus store root {}: {error}",
            store_root.display()
        ))
    })?;
    let store = FilesystemBlockStore::new(&store_root)
        .map_err(|error| EvaluatorError::Io(format!("failed to open filesystem store: {error}")))?;
    let spec = EmbeddingSpec {
        dims: u64::try_from(entities[0].embedding.len()).map_err(|_| {
            EvaluatorError::InvalidConfiguration("entity dimensions overflowed u64".into())
        })?,
        encoding: "f32le".into(),
    };
    let mut leaves = Vec::with_capacity(entities.len());
    for entity in entities {
        let leaf = build_leaf_block(
            VERSION_1,
            spec.clone(),
            vec![LeafEntry {
                embedding: encode_embedding(&entity.embedding),
                metadata: vec![
                    (
                        CborValue::Text("entity_id".into()),
                        CborValue::Text(entity.entity_id.clone()),
                    ),
                    (
                        CborValue::Text("synthetic".into()),
                        CborValue::Bool(entity.synthetic),
                    ),
                ],
                content: Content {
                    media_type: "application/octet-stream".into(),
                    body: Vec::new(),
                },
            }],
            None,
        )
        .map_err(|error| EvaluatorError::InvalidConfiguration(error.to_string()))?;
        let block_id = store
            .put(&Block::Leaf(leaf))
            .map_err(|error| EvaluatorError::Io(format!("failed to store leaf block: {error}")))?;
        leaves.push((block_id, encode_embedding(&entity.embedding)));
    }

    let root_block_id = if leaves.len() == 1 {
        leaves[0].0
    } else {
        let root = build_branch_block(
            VERSION_1,
            1,
            spec,
            leaves
                .iter()
                .map(|(block_id, embedding)| BranchEntry {
                    embedding: embedding.clone(),
                    child: *block_id,
                })
                .collect(),
            None,
        )
        .map_err(|error| EvaluatorError::InvalidConfiguration(error.to_string()))?;
        store
            .put(&Block::Branch(root))
            .map_err(|error| EvaluatorError::Io(format!("failed to store branch block: {error}")))?
    };

    write_zip_archive_from_directory(&store_root, archive_path)?;
    Ok(BlockStoreCorpusReference {
        source_id: source_id.into(),
        root_block_id: root_block_id.to_string(),
        store: BlockStoreReferenceStore::ZipArchive {
            archive_path: archive_path.to_path_buf(),
        },
    })
}

fn encode_embedding(values: &[f32]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

fn write_zip_archive_from_directory(
    store_root: &Path,
    archive_path: &Path,
) -> Result<(), EvaluatorError> {
    let file = File::create(archive_path).map_err(|error| {
        EvaluatorError::Io(format!(
            "failed to create archive {}: {error}",
            archive_path.display()
        ))
    })?;
    let mut zip = ZipWriter::new(file);
    write_directory_to_zip(store_root, store_root, &mut zip)?;
    zip.finish()
        .map_err(|error| EvaluatorError::Io(format!("failed to finalize archive: {error}")))?;
    Ok(())
}

fn write_directory_to_zip(
    root: &Path,
    directory: &Path,
    zip: &mut ZipWriter<File>,
) -> Result<(), EvaluatorError> {
    let options = SimpleFileOptions::default().compression_method(ZipCompressionMethod::Stored);
    let mut entries = std::fs::read_dir(directory)
        .map_err(|error| {
            EvaluatorError::Io(format!(
                "failed to read directory {}: {error}",
                directory.display()
            ))
        })?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            EvaluatorError::Io(format!(
                "failed to enumerate directory {}: {error}",
                directory.display()
            ))
        })?;
    entries.sort();

    for path in entries {
        if path.is_dir() {
            write_directory_to_zip(root, &path, zip)?;
            continue;
        }

        let relative = path
            .strip_prefix(root)
            .map_err(|error| EvaluatorError::Io(format!("failed to strip prefix: {error}")))?
            .to_string_lossy()
            .replace('\\', "/");
        zip.start_file(relative, options)
            .map_err(|error| EvaluatorError::Io(format!("failed to start zip entry: {error}")))?;
        let mut file = File::open(&path).map_err(|error| {
            EvaluatorError::Io(format!("failed to open file {}: {error}", path.display()))
        })?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|error| {
            EvaluatorError::Io(format!("failed to read file {}: {error}", path.display()))
        })?;
        zip.write_all(&bytes)
            .map_err(|error| EvaluatorError::Io(format!("failed to write zip entry: {error}")))?;
    }
    Ok(())
}

fn clone_reference_with_source_id(
    reference: &BlockStoreCorpusReference,
    source_id: &str,
) -> BlockStoreCorpusReference {
    BlockStoreCorpusReference {
        source_id: source_id.into(),
        root_block_id: reference.root_block_id.clone(),
        store: reference.store.clone(),
    }
}

fn section4_metric_declarations() -> Vec<MetricDeclaration> {
    vec![
        MetricDeclaration {
            metric_id: "same-leaf-neighborhood-coherence".into(),
            label: "Same-leaf neighborhood coherence".into(),
            kind: MetricKind::SameLeafNeighborhoodCoherence,
            coverage: ResearchCoverage::Direct,
            research_goal_ids: vec!["RG-LOCALITY".into()],
            ranking_weight: 1.0,
        },
        MetricDeclaration {
            metric_id: "local-compression-gain".into(),
            label: "Local compression gain".into(),
            kind: MetricKind::LocalCompressionGain,
            coverage: ResearchCoverage::Direct,
            research_goal_ids: vec!["RG-COMPRESSION".into()],
            ranking_weight: 0.5,
        },
    ]
}

fn section4_gate_declarations() -> Vec<GateDeclaration> {
    vec![
        GateDeclaration {
            gate_id: "exact-leaf-occupancy".into(),
            label: "Exact leaf occupancy".into(),
            kind: GateKind::ExactLeafOccupancy,
            coverage: ResearchCoverage::Direct,
            research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
        },
        GateDeclaration {
            gate_id: "complete-coverage".into(),
            label: "Complete coverage".into(),
            kind: GateKind::CompleteCoverage,
            coverage: ResearchCoverage::Direct,
            research_goal_ids: vec!["RG-COVERAGE".into()],
        },
        GateDeclaration {
            gate_id: "one-cluster-per-entity".into(),
            label: "One cluster per entity".into(),
            kind: GateKind::OneClusterPerEntity,
            coverage: ResearchCoverage::Direct,
            research_goal_ids: vec!["RG-COVERAGE".into()],
        },
        GateDeclaration {
            gate_id: "no-empty-declared-clusters".into(),
            label: "No empty declared clusters".into(),
            kind: GateKind::NoEmptyDeclaredClusters,
            coverage: ResearchCoverage::Direct,
            research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
        },
        GateDeclaration {
            gate_id: "deterministic-observable-results".into(),
            label: "Deterministic observable results".into(),
            kind: GateKind::DeterministicObservableResults,
            coverage: ResearchCoverage::Direct,
            research_goal_ids: vec!["RG-DETERMINISM".into()],
        },
    ]
}

fn default_deferred_research_goals() -> Vec<DeferredResearchGoal> {
    vec![DeferredResearchGoal {
        deferred_id: "deferred-hierarchy-routing".into(),
        label: "Hierarchy routing proof".into(),
        reason: "full hierarchy, sibling structure, and persisted search routing remain outside the leaf-stage evaluator boundary".into(),
        research_goal_ids: vec!["RG-HIERARCHY".into(), "RG-ROUTING".into()],
        coverage: ResearchCoverage::Deferred,
    }]
}

fn family_label(family: &Section4CorpusFamily) -> &'static str {
    match family {
        Section4CorpusFamily::RealWorldHarvested => "real-world-harvested",
        Section4CorpusFamily::WellClusteredSynthetic => "well-clustered-synthetic",
        Section4CorpusFamily::WeakClusterUniform => "weak-cluster-uniform",
        Section4CorpusFamily::AnisotropicManifold => "anisotropic-manifold",
        Section4CorpusFamily::NearDuplicateHeavy => "near-duplicate-heavy",
    }
}

impl Section4ProfileSourceSpec {
    fn family(&self) -> &Section4CorpusFamily {
        match self {
            Self::Synthetic { family, .. } | Self::Harvested { family, .. } => family,
        }
    }

    fn alignment_policy(&self) -> &AlignmentPolicy {
        match self {
            Self::Synthetic {
                alignment_policy, ..
            }
            | Self::Harvested {
                alignment_policy, ..
            } => alignment_policy,
        }
    }
}

pub fn resolve_registered_candidates(
    candidate_names: &[String],
) -> Result<Vec<RegisteredCandidate>, EvaluatorError> {
    let mut registered = Vec::with_capacity(candidate_names.len());
    for candidate_name in candidate_names {
        let Some(candidate) = built_in_fixture_candidate(candidate_name) else {
            return Err(EvaluatorError::InvalidConfiguration(format!(
                "unknown registered candidate {candidate_name}; available fixture candidates: {}",
                crate::built_in_fixture_candidate_names().join(", ")
            )));
        };
        registered.push(candidate);
    }
    Ok(registered)
}
