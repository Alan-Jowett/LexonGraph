// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use lexongraph_streaming_clustering_evaluator::{
    BenchmarkProfile, EvaluatorError, ExecutionBackendRequest, Section4SuiteManifest,
    Section4SuiteSpec, Section5HierarchyContract, emit_campaign_artifacts,
    emit_section5_campaign_artifacts, generate_section4_suite_assets,
    materialize_section4_archive_from_json, registered_candidate_names,
    registered_hierarchy_strategy_names, resolve_profile_block_store_paths,
    resolve_registered_candidates, resolve_registered_hierarchy_strategies,
    resolve_section4_suite_manifest_paths, resolve_section4_suite_spec_paths,
    run_evaluation_campaign, run_section4_suite, run_section5_campaign,
    set_execution_backend_request, write_campaign_artifacts, write_section4_suite_artifacts,
    write_section5_campaign_artifacts,
};

#[derive(Parser, Debug)]
#[command(version, about = "Run the LexonGraph streaming clustering evaluator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliExecutionBackend {
    Auto,
    Cpu,
    Wgpu,
}

impl CliExecutionBackend {
    fn into_request(self) -> ExecutionBackendRequest {
        match self {
            Self::Auto => ExecutionBackendRequest::Auto,
            Self::Cpu => ExecutionBackendRequest::Cpu,
            Self::Wgpu => ExecutionBackendRequest::Wgpu,
        }
    }
}

#[derive(Subcommand, Debug)]
enum Command {
    /// List the registered candidates that this executable can run.
    #[command(alias = "list-fixture-candidates")]
    ListCandidates,
    /// List the registered hierarchy strategies that section-5 execution can run.
    ListHierarchyStrategies,
    /// Run one benchmark profile against one or more registered candidates.
    Run {
        #[arg(long, value_name = "PATH")]
        profile: PathBuf,
        #[arg(long = "candidate", value_name = "NAME", required = true)]
        candidates: Vec<String>,
        #[arg(long, value_name = "PATH")]
        output_dir: PathBuf,
        #[arg(long, value_enum, default_value = "auto")]
        execution_backend: CliExecutionBackend,
    },
    /// Generate section-4 benchmark assets and profile JSON files.
    GenerateSection4Assets {
        #[arg(long, value_name = "PATH")]
        suite: PathBuf,
        #[arg(long, value_name = "PATH")]
        output_dir: PathBuf,
        #[arg(long, value_enum, default_value = "auto")]
        execution_backend: CliExecutionBackend,
    },
    /// Materialize a block-store zip archive from a section-4 JSON entity list.
    MaterializeSection4Archive {
        #[arg(long, value_name = "PATH")]
        input: PathBuf,
        #[arg(long, value_name = "PATH")]
        output: PathBuf,
        #[arg(long, value_name = "TEXT")]
        source_id: Option<String>,
        #[arg(long, value_name = "TEXT")]
        corpus_id: Option<String>,
    },
    /// Run a generated section-4 benchmark suite against one or more candidates.
    RunSection4Suite {
        #[arg(long, value_name = "PATH")]
        manifest: PathBuf,
        #[arg(long = "candidate", value_name = "NAME", required = true)]
        candidates: Vec<String>,
        #[arg(long, value_name = "PATH")]
        output_dir: PathBuf,
        #[arg(long, value_enum, default_value = "auto")]
        execution_backend: CliExecutionBackend,
    },
    /// Run section-5 hierarchy construction over the survivors from a leaf-stage profile.
    RunSection5 {
        #[arg(long, value_name = "PATH")]
        profile: PathBuf,
        #[arg(long = "candidate", value_name = "NAME", required = true)]
        candidates: Vec<String>,
        #[arg(long, value_name = "PATH")]
        contract: PathBuf,
        #[arg(long = "hierarchy-strategy", value_name = "NAME", required = true)]
        hierarchy_strategies: Vec<String>,
        #[arg(long, value_name = "PATH")]
        output_dir: PathBuf,
        #[arg(long, value_enum, default_value = "auto")]
        execution_backend: CliExecutionBackend,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), EvaluatorError> {
    match Cli::parse().command {
        Command::ListCandidates => {
            for candidate in registered_candidate_names() {
                println!("{candidate}");
            }
            Ok(())
        }
        Command::ListHierarchyStrategies => {
            for strategy in registered_hierarchy_strategy_names() {
                println!("{strategy}");
            }
            Ok(())
        }
        Command::Run {
            profile,
            candidates,
            output_dir,
            execution_backend,
        } => {
            set_execution_backend_request(execution_backend.into_request());
            let profile_path = profile;
            let profile = std::fs::read_to_string(&profile_path).map_err(|error| {
                EvaluatorError::Io(format!(
                    "failed to read benchmark profile {}: {error}",
                    profile_path.display()
                ))
            })?;
            let profile: BenchmarkProfile = serde_json::from_str(&profile).map_err(|error| {
                EvaluatorError::Json(format!(
                    "failed to parse benchmark profile {}: {error}",
                    profile_path.display()
                ))
            })?;
            let mut profile = profile;
            if let Some(profile_dir) = profile_path.parent() {
                resolve_profile_block_store_paths(&mut profile, profile_dir);
            }

            let registered_candidates = resolve_registered_candidates(&candidates)?;
            let report = run_evaluation_campaign(&profile, &registered_candidates)?;
            let artifacts = emit_campaign_artifacts(&report)?;
            let paths = write_campaign_artifacts(&output_dir, &artifacts)?;
            for path in paths {
                println!("{}", path.display());
            }
            Ok(())
        }
        Command::GenerateSection4Assets {
            suite,
            output_dir,
            execution_backend,
        } => {
            set_execution_backend_request(execution_backend.into_request());
            let suite_path = suite;
            let suite = std::fs::read_to_string(&suite_path).map_err(|error| {
                EvaluatorError::Io(format!(
                    "failed to read section-4 suite spec {}: {error}",
                    suite_path.display()
                ))
            })?;
            let suite: Section4SuiteSpec = serde_json::from_str(&suite).map_err(|error| {
                EvaluatorError::Json(format!(
                    "failed to parse section-4 suite spec {}: {error}",
                    suite_path.display()
                ))
            })?;
            let mut suite = suite;
            if let Some(suite_dir) = suite_path.parent() {
                resolve_section4_suite_spec_paths(&mut suite, suite_dir);
            }
            let manifest = generate_section4_suite_assets(&suite, &output_dir)?;
            let manifest_path = output_dir.join("section4-suite-manifest.json");
            println!("{}", manifest_path.display());
            for generated in manifest.generated_profiles {
                println!("{}", generated.profile_path.display());
                println!("{}", generated.corpus_archive_path.display());
            }
            Ok(())
        }
        Command::MaterializeSection4Archive {
            input,
            output,
            source_id,
            corpus_id,
        } => {
            let reference = materialize_section4_archive_from_json(
                &input,
                &output,
                source_id.as_deref(),
                corpus_id.as_deref(),
            )?;
            println!("{}", output.display());
            println!("{}", reference.root_block_id);
            Ok(())
        }
        Command::RunSection4Suite {
            manifest,
            candidates,
            output_dir,
            execution_backend,
        } => {
            set_execution_backend_request(execution_backend.into_request());
            let manifest_path = manifest;
            let manifest = std::fs::read_to_string(&manifest_path).map_err(|error| {
                EvaluatorError::Io(format!(
                    "failed to read section-4 suite manifest {}: {error}",
                    manifest_path.display()
                ))
            })?;
            let manifest: Section4SuiteManifest =
                serde_json::from_str(&manifest).map_err(|error| {
                    EvaluatorError::Json(format!(
                        "failed to parse section-4 suite manifest {}: {error}",
                        manifest_path.display()
                    ))
                })?;
            let mut manifest = manifest;
            if let Some(manifest_dir) = manifest_path.parent() {
                resolve_section4_suite_manifest_paths(&mut manifest, manifest_dir);
            }
            let candidates = resolve_registered_candidates(&candidates)?;
            let report = run_section4_suite(&manifest, &candidates, &output_dir)?;
            let artifacts = write_section4_suite_artifacts(&report, &output_dir)?;
            println!("{}", artifacts.suite_report_path.display());
            println!("{}", artifacts.scorecard_path.display());
            println!("{}", artifacts.survivor_decision_path.display());
            for path in artifacts.profile_output_dirs {
                println!("{}", path.display());
            }
            Ok(())
        }
        Command::RunSection5 {
            profile,
            candidates,
            contract,
            hierarchy_strategies,
            output_dir,
            execution_backend,
        } => {
            set_execution_backend_request(execution_backend.into_request());
            let profile_path = profile;
            let profile = std::fs::read_to_string(&profile_path).map_err(|error| {
                EvaluatorError::Io(format!(
                    "failed to read benchmark profile {}: {error}",
                    profile_path.display()
                ))
            })?;
            let profile: BenchmarkProfile = serde_json::from_str(&profile).map_err(|error| {
                EvaluatorError::Json(format!(
                    "failed to parse benchmark profile {}: {error}",
                    profile_path.display()
                ))
            })?;
            let mut profile = profile;
            if let Some(profile_dir) = profile_path.parent() {
                resolve_profile_block_store_paths(&mut profile, profile_dir);
            }

            let contract_path = contract;
            let contract = std::fs::read_to_string(&contract_path).map_err(|error| {
                EvaluatorError::Io(format!(
                    "failed to read section-5 hierarchy contract {}: {error}",
                    contract_path.display()
                ))
            })?;
            let contract: Section5HierarchyContract =
                serde_json::from_str(&contract).map_err(|error| {
                    EvaluatorError::Json(format!(
                        "failed to parse section-5 hierarchy contract {}: {error}",
                        contract_path.display()
                    ))
                })?;

            let registered_candidates = resolve_registered_candidates(&candidates)?;
            let registered_strategies =
                resolve_registered_hierarchy_strategies(&hierarchy_strategies)?;
            let report = run_section5_campaign(
                &profile,
                &registered_candidates,
                &contract,
                &registered_strategies,
            )?;
            let artifacts = emit_section5_campaign_artifacts(&report)?;
            let paths = write_section5_campaign_artifacts(&output_dir, &artifacts)?;
            for path in paths {
                println!("{}", path.display());
            }
            Ok(())
        }
    }
}
