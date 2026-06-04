<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Repository Dependabot Validation

## Status

Draft validation specification for repository-managed Dependabot version-update
automation.

## Validation Scope

These validation entries define the expected verification surface for the
repository Dependabot configuration.

## Validation Entries

### VAL-DEPS-001

Inspect the repository root and `.github` directory.

**Pass condition:** `.github/dependabot.yml` exists as a tracked file.

**Traces to:** REQ-DEPS-001

### VAL-DEPS-002

Inspect `.github/dependabot.yml` for Cargo configuration.

**Pass condition:** the file declares a `cargo` update entry rooted at `/`.

**Traces to:** REQ-DEPS-002, REQ-DEPS-005

### VAL-DEPS-003

Inspect `.github/dependabot.yml` for GitHub Actions configuration.

**Pass condition:** the file declares a `github-actions` update entry rooted at
`/`.

**Traces to:** REQ-DEPS-003, REQ-DEPS-005

### VAL-DEPS-004

Inspect the schedule configuration for each declared ecosystem.

**Pass condition:** each configured ecosystem uses a weekly schedule.

**Traces to:** REQ-DEPS-004

### VAL-DEPS-005

Compare the configuration against the repository contents.

**Pass condition:** the file declares Cargo and GitHub Actions only, with no
extra ecosystems.

**Traces to:** REQ-DEPS-005, REQ-DEPS-006

### VAL-DEPS-006

Inspect `.github/dependabot.yml` header lines.

**Pass condition:** the file contains the required SPDX YAML header.

**Traces to:** REQ-DEPS-001
