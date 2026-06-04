<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Repository Dependabot Design

## Status

Draft design specification for repository-managed Dependabot version-update
automation.

## Design Goals

The Dependabot configuration is intended to be:

- minimal
- deterministic
- aligned with the current repository structure
- easy to review
- explicit about non-goals

## Workflow Boundary

The repository Dependabot automation owns:

- version-update proposal configuration
- supported ecosystem declarations
- update schedule configuration
- repository-managed configuration file placement

The repository Dependabot automation does not own:

- CI quality-gate execution
- release automation
- package publishing
- automerge behavior
- reviewer, assignee, or label routing

## Configuration Shape

### DSG-DEPS-001 `Configuration file`

The repository defines Dependabot configuration at `.github/dependabot.yml`.

### DSG-DEPS-002 `Configuration version`

The configuration uses Dependabot `version: 2`.

### DSG-DEPS-003 `Cargo ecosystem entry`

The configuration defines a `cargo` update entry rooted at `/` so Dependabot
can evaluate the repository workspace manifest at the repository root.

### DSG-DEPS-004 `GitHub Actions ecosystem entry`

The configuration defines a `github-actions` update entry rooted at `/`, which
is the repository-root directory value used for GitHub Actions workflow update
discovery.

### DSG-DEPS-005 `Schedule`

Each configured ecosystem uses a weekly schedule.

### DSG-DEPS-006 `Repository alignment`

The configuration is limited to dependency ecosystems currently present in the
repository: Cargo and GitHub Actions.

### DSG-DEPS-007 `Non-goals`

The configuration does not define automerge behavior, reviewer routing,
assignee routing, label routing, or ecosystems absent from the repository.

### DSG-DEPS-008 `Governed file compliance`

The tracked YAML configuration file uses the repository's required SPDX YAML
header form.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-DEPS-001, DSG-DEPS-002 | REQ-DEPS-001 |
| DSG-DEPS-003 | REQ-DEPS-002, REQ-DEPS-005 |
| DSG-DEPS-004 | REQ-DEPS-003, REQ-DEPS-005 |
| DSG-DEPS-005 | REQ-DEPS-004 |
| DSG-DEPS-006 | REQ-DEPS-005 |
| DSG-DEPS-007 | REQ-DEPS-006 |
| DSG-DEPS-008 | REQ-DEPS-001 |
