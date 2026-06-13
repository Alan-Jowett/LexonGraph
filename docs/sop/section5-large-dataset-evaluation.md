# SOP: Running section-5 evaluation on the checked-in large harvested dataset

This SOP describes how to run the streaming clustering evaluator's section-5 hierarchy-stage comparison on the checked-in large harvested profile, inspect the outputs, and repeat the run consistently.

## Purpose

Use this procedure to:

- rerun the current section-5 benchmark manually
- compare section-5 results across dataset sizes
- let other contributors reproduce the same hierarchy-stage experiment

## Preconditions

- Work from the repository root: `C:\dev\LexonGraph`
- Use the existing evaluator crate
- Do not harvest a new corpus for this run; the repository already includes the required large harvested profile and corpus archive

## Checked-in assets

### Section-4 survivor set

Current carried-forward leaf-stage candidates are listed in:

`C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\corpus-panel-suite\reports\section4-survivor-decision.txt`

At the time of writing, those candidates are:

- `recursive-balanced-kmeans`
- `hybrid-coarse-rebalance`
- `graph-neighborhood-balance`

### Large harvested section-4 profile

Use this checked-in large profile:

`C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\corpus-panel-suite\profiles\real-world-harvested-strict-large.json`

Its backing corpus archive is:

`C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\corpus-panel-suite\corpora\real-world-harvested-strict-large.zip`

The repository also includes a medium comparison profile:

`C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\corpus-panel-suite\profiles\real-world-harvested-strict-medium.json`

### Huge harvested section-4 profile for high-fanout experiments

For 64-128 fanout experiments, use the checked-in huge harvested suite:

- suite spec: `C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\huge-harvest-suite\section4-suite-spec.json`
- suite manifest: `C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\huge-harvest-suite\section4-suite-manifest.json`
- suite source archive: `C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\huge-harvest-suite\sources\real-world-harvested-huge-source.zip`
- generated profile: `C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\huge-harvest-suite\profiles\real-world-harvested-strict-huge.json`
- generated corpus archive: `C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\huge-harvest-suite\corpora\real-world-harvested-strict-huge.zip`

This checked-in huge profile was derived from `C:\data2\block-store.zip` using:

- harvested source root block: `ee22a9daf7644cc894e5e3a6e1eaa28ba26d615937720ff75b3c41855d17fcc8`
- harvested slice root block: `725df06751cab5a599d23393d919cc3302f6dda8aa452884b04f80202d3253b3`
- materialized generated profile root block: `a810900c463806b785d17c3b00287a06cb18968906067cc0e7810177c5dd2514`
- dimensionality: `384`
- harvested identity key: `chunk_locator`
- real/evaluated entity count: `186`
- declared cluster count at `leaf_size = 2`: `93`

Because the huge profile has 93 leaf clusters, it is suitable for section-5 hierarchy contracts in the 64-128 fanout regime.

## Step 1: List available hierarchy strategies

Run:

```powershell
cargo run -p lexongraph-streaming-clustering-evaluator -- list-hierarchy-strategies
```

Use the printed strategy names exactly as emitted by the CLI.

## Step 2: Create a section-5 contract JSON

The repository does not currently include a checked-in section-5 contract JSON for CLI use, so create one locally.

Example:

```powershell
$contractPath = 'C:\temp\section5-contract-euclidean.json'
@'
{
  "contract_id": "section5-large-euclidean",
  "fanout_min": 2,
  "fanout_max": 4,
  "depth_bound_policy": "CeilLogByMinFanout",
  "metric_semantics_profile": "euclidean",
  "grouping_functional": "euclidean-centroid-distance",
  "dispersion_functional": "mean-squared-radius",
  "metric_compatibility_rule": "closed-profile-v1",
  "beta_threshold": 1.25,
  "epsilon_policy": {
    "parent_to_root_dispersion_ratio_max": 0.01
  },
  "section4_source_label": "real-world-harvested-strict-large",
  "later_evaluation_line": "future parent-summary and routing evaluator"
}
'@ | Set-Content -Path $contractPath
```

For real corpus-panel runs, use a bounded fanout range rather than an exact binary
fanout. With 24 leaf clusters, an exact `[2,2]` layerwise grouping contract can
strand a 3-node layer and cause deterministic hierarchy-build failure before
refinement is evaluated.

## Step 3: Run section 5 on the large harvested profile

```powershell
$profile = 'C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\corpus-panel-suite\profiles\real-world-harvested-strict-large.json'
$contractPath = 'C:\temp\section5-contract-euclidean.json'
$outDir = 'C:\temp\section5-large-run'

cargo run -p lexongraph-streaming-clustering-evaluator -- run-section5 `
  --profile $profile `
  --candidate recursive-balanced-kmeans `
  --candidate hybrid-coarse-rebalance `
  --candidate graph-neighborhood-balance `
  --contract $contractPath `
  --hierarchy-strategy bottom-up-agglomeration `
  --hierarchy-strategy greedy-pack `
  --hierarchy-strategy recursive-top-down `
  --hierarchy-strategy hybrid-top-down-bottom-up `
  --output-dir $outDir
```

## Step 4: Review the outputs

The section-5 CLI writes:

- `section5-scorecard.txt`
- `section5-carry-forward-summary.txt`
- `section5-campaign-report.json`
- one `*-pair-report.json` file per leaf-candidate x hierarchy-strategy pair

Open these first:

- `C:\temp\section5-large-run\section5-scorecard.txt`
- `C:\temp\section5-large-run\section5-carry-forward-summary.txt`

Use the pair reports when you need per-edge and per-gate detail.

## Step 5: Interpret the results

For each pair, check:

- `run_status`
- `survived_required_gates`
- `maximum_observed_beta`
- `epsilon_exception_use_count`
- `gate_results`

Focus on whether failures come from:

- fanout bounds
- single-child internal nodes
- depth bound
- metric-semantics compatibility
- refinement beta threshold
- epsilon-exception scope

## Step 6: Compare medium vs large

To compare size effects without harvesting a new corpus, rerun the same procedure with:

`C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\corpus-panel-suite\profiles\real-world-harvested-strict-medium.json`

Keep the same candidate set, hierarchy strategies, and contract so the only intended variable is dataset size.

## Step 7: Run the huge high-fanout profile

First run section 4 on the huge profile to identify survivors:

```powershell
$manifest = 'C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\huge-harvest-suite\section4-suite-manifest.json'
$outDir = 'C:\temp\section4-huge-run'

cargo run -p lexongraph-streaming-clustering-evaluator -- run-section4-suite `
  --manifest $manifest `
  --candidate recursive-balanced-kmeans `
  --candidate hybrid-coarse-rebalance `
  --candidate graph-neighborhood-balance `
  --candidate pca-sort-exact-chunking `
  --candidate space-filling-curve-exact-chunking `
  --candidate random-shuffle-exact-chunking `
  --output-dir $outDir
```

Then create a high-fanout section-5 contract, for example:

```powershell
$contractPath = 'C:\temp\section5-contract-high-fanout.json'
@'
{
  "contract_id": "section5-huge-high-fanout",
  "fanout_min": 64,
  "fanout_max": 128,
  "depth_bound_policy": "CeilLogByMinFanout",
  "metric_semantics_profile": "euclidean",
  "grouping_functional": "euclidean-centroid-distance",
  "dispersion_functional": "mean-squared-radius",
  "metric_compatibility_rule": "closed-profile-v1",
  "beta_threshold": 1.25,
  "epsilon_policy": {
    "parent_to_root_dispersion_ratio_max": 0.01
  },
  "section4_source_label": "real-world-harvested-strict-huge",
  "later_evaluation_line": "future parent-summary and routing evaluator"
}
'@ | Set-Content -Path $contractPath
```

Finally run section 5 against the huge profile and the section-4 survivors:

```powershell
$profile = 'C:\dev\LexonGraph\crates\lexongraph-streaming-clustering-evaluator\section4\huge-harvest-suite\profiles\real-world-harvested-strict-huge.json'
$outDir = 'C:\temp\section5-huge-run'

cargo run -p lexongraph-streaming-clustering-evaluator -- run-section5 `
  --profile $profile `
  --candidate <survivor-1> `
  --candidate <survivor-2> `
  --candidate <survivor-3> `
  --contract $contractPath `
  --hierarchy-strategy bottom-up-agglomeration `
  --hierarchy-strategy greedy-pack `
  --hierarchy-strategy recursive-top-down `
  --hierarchy-strategy hybrid-top-down-bottom-up `
  --output-dir $outDir
```

## When to use `C:\data2\block-store.zip`

Do not use `C:\data2\block-store.zip` for the first larger-dataset pass.

The repository now includes one checked-in huge harvested suite derived from `C:\data2\block-store.zip`.

Consider harvesting from the external archive again only if:

- the checked-in huge profile is still too small for the question being asked
- you want a different harvested corpus than the repository-docs-derived suite asset
- you need a different deterministic slice or a larger scale tier beyond the checked-in huge profile

## Expected current state

At the time this SOP was written:

- section 5 is implemented in the evaluator
- the checked-in harvested small run had all pairs fail
- those failures were driven by refinement `beta` threshold violations rather than tree-shape or metric-semantics failures

This SOP is intended to make the next larger-dataset comparison repeatable.
