# Implementation Plan Template

**Branch**: `{branch_name}` | **Date**: `{date}` | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/{feature_name}/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

`{implementation_summary}`

## Technical Context

**Language/Version**: `{language_version}`

**Primary Dependencies**: `{dependencies}`

**Storage**: `{storage_description}`

**Testing**: `{testing_framework}`

**Target Platform**: `{platform_description}`

**Project Type**: `{project_type}`

**Performance Goals**: `{performance_goals}`

**Constraints**:
- `{constraint_1}`
- `{constraint_2}`
- `{constraint_3}`

**Scale/Scope**: `{scope_description}`

## Enforcement Kernel Specification

### CI Execution DAG

**Stage Order** (strict linear sequence with artifact passing):

```
Stage 1: {stage_1_name}
  ↓ Passes
  artifact: {stage_1_artifact}

Stage 2: {stage_2_name}
  ↓ Passes, consumes {stage_1_artifact}
  artifact: {stage_2_artifact}

...
```

**Failure Propagation Rules**:
- Hard-stop: Any stage failure immediately aborts all subsequent stages
- Deterministic exit codes: 0=success, 1=failure, 2=skipped
- No partial success allowed
- Each stage logs detailed failure reason to CI output

**Artifact Passing Model**:
- Each stage produces strict JSON artifact on success
- Next stage consumes previous artifact as input
- No side effects between stages
- All artifacts stored in `.specify/ci-artifacts/` for audit trail

### Enforcement Validator Specifications

#### 1. {validator_1_name}

**Input**: `{validator_1_input}`

**Algorithm**:
- {algorithm_description}

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "{summary}"
}
```

**Failure Signature**: Exit code 1 with failure details

---

#### 2. {validator_2_name}

**Input**: `{validator_2_input}`

**Algorithm**:
- {algorithm_description}

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "{summary}"
}
```

**Failure Signature**: Exit code 1 with failure details

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Gate 1: {gate_1_name} ({status})

**Constitution Requirement**: `{requirement}`

**Compliance Status**: ✅ PASS / ✗ FAIL

**Justification**: `{justification}`

**Verification**: `{verification_steps}`

---

## Project Structure

### Documentation (this feature)

```text
specs/{feature_name}/
├── spec.md              # Feature specification
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output
```

### Source Code (repository root)

```text
{structure_description}
```

**Structure Decision**: `{decision_description}`

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

No constitution violations in this feature.

### Enforcement Kernel Complexity

The enforcement kernel introduces complexity to ensure constitutional compliance:

| Complexity Component | Why Needed | Simpler Alternative Rejected Because |
|---------------------|------------|-------------------------------------|
| {component_1} | {reason_1} | {alternative_1_rejected} |
| {component_2} | {reason_2} | {alternative_2_rejected} |
