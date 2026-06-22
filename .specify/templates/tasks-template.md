# Tasks Template

**Input**: Design documents from `/specs/{feature_name}/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: `{test_requirements}`

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths in descriptions

## Path Conventions

- `{path_convention_description}`

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: `{purpose_description}`

- [ ] T001 `{task_description}`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: `{purpose_description}`

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T009 `{task_description}`

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - {story_title}

**Goal**: `{story_goal}`

**Independent Test**: `{how_to_test}`

### Implementation for User Story 1

- [ ] T025 [P] [US1] `{task_description}`

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - {story_title}

**Goal**: `{story_goal}`

**Independent Test**: `{how_to_test}`

### Implementation for User Story 2

- [ ] T062 [P] [US2] `{task_description}`

**Checkpoint**: At this point, User Story 2 should be fully functional and testable independently

---

## Phase 5: User Story 3 - {story_title}

**Goal**: `{story_goal}`

**Independent Test**: `{how_to_test}`

### Implementation for User Story 3

- [ ] T074 [P] [US3] `{task_description}`

**Checkpoint**: At this point, User Story 3 should be fully functional and testable independently

---

## Phase 6: User Story 4 - {story_title}

**Goal**: `{story_goal}`

**Independent Test**: `{how_to_test}`

### Implementation for User Story 4

- [ ] T086 [P] [US4] `{task_description}`

**Checkpoint**: At this point, User Story 4 should be fully functional and testable independently

---

## Phase 7: User Story 5 - {story_title}

**Goal**: `{story_goal}`

**Independent Test**: `{how_to_test}`

### Implementation for User Story 5

- [ ] T100 [P] [US5] `{task_description}`

**Checkpoint**: At this point, User Story 5 should be fully functional and testable independently

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T109 [P] `{task_description}`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-7)**: All depend on Foundational phase completion
- **Polish (Phase 8)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (Priority)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (Priority)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 3 (Priority)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 4 (Priority)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 5 (Priority)**: Can start after Foundational (Phase 2) - No dependencies on other stories

**All user stories are independently testable and can be implemented in parallel after Foundational phase completes.**

### Within Each User Story

- Parallelizable tasks (marked [P]) can be executed simultaneously
- Non-parallelizable tasks depend on previous tasks within the story
- Stories are fully independent of each other

### Parallel Opportunities

- All Setup tasks (Phase 1) marked [P] can run in parallel
- All Foundational tasks (Phase 2) marked [P] can run in parallel (within Phase 2)
- Within User Story 1: All [P] tasks can run in parallel
- Within User Story 2: All 9 stage tasks can run in parallel
- Within User Story 3: All migration creation tasks can run in parallel
- Within User Story 4: All service skeleton tasks can run in parallel
- Within User Story 5: All documentation creation tasks can run in parallel
- Once Foundational phase completes, ALL 5 user stories can start in parallel (if team capacity allows)

## Sprint 0 Task Summary

**Total Tasks**: {total_tasks}

**Task Count per User Story**:
- User Story 1: {us1_tasks} tasks
- User Story 2: {us2_tasks} tasks
- User Story 3: {us3_tasks} tasks
- User Story 4: {us4_tasks} tasks
- User Story 5: {us5_tasks} tasks

**Parallel Opportunities**:
- Phase 1: {phase1_parallel} parallelizable tasks
- Phase 2: {phase2_parallel} parallelizable tasks
- Phase 3: {phase3_parallel} parallelizable tasks
- Phase 4: {phase4_parallel} parallelizable tasks
- Phase 5: {phase5_parallel} parallelizable tasks
- Phase 6: {phase6_parallel} parallelizable tasks
- Phase 7: {phase7_parallel} parallelizable tasks
- **Total Parallelizable Tasks**: {total_parallel} out of {total_tasks} ({parallel_percent}%)

**Independent Test Criteria**:
- US1: Verify directory structure exists and matches spec
- US2: Run `make ci` and verify all 9 stages pass
- US3: Connect to databases and verify tables exist
- US4: Start services and verify health endpoints respond
- US5: Verify all SpecKit markers are present
