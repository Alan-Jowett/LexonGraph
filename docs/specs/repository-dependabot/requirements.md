<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Repository Dependabot Requirements

## Status

Draft specification for repository-managed Dependabot version-update
automation.

## Scope

This document specifies repository-level Dependabot version-update behavior for
the current repository.

This document defines dependency update proposal automation only. It does not
define CI quality gates, release automation, publishing automation, or merge
policy.

## Requirements

### REQ-DEPS-001

The repository shall define a versioned Dependabot configuration file at
`.github/dependabot.yml`.

### REQ-DEPS-002

The repository shall configure Dependabot to propose version updates for Cargo
dependencies used by the Rust workspace.

### REQ-DEPS-003

The repository shall configure Dependabot to propose version updates for
GitHub Actions dependencies used by the repository automation surface.

### REQ-DEPS-004

The Dependabot configuration shall check for updates on a weekly schedule.

### REQ-DEPS-005

The Dependabot configuration shall align with the current repository structure
and declare only dependency ecosystems that are present in the repository.

### REQ-DEPS-006

This pass shall not introduce release automation, publishing automation,
automerge behavior, or reviewer, assignee, or label routing policy.

## Out of Scope

This change does not define or own:

- CI quality-gate behavior
- release automation
- package publishing
- automerge behavior
- reviewer, assignee, or label routing
- dependency ecosystems not currently present in the repository
