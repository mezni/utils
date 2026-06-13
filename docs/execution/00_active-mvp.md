# Execution System Index

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🎯 PURPOSE

**Live MVP control + LLM task orchestration**

This folder acts as:

1. **Memory for LLM execution**
   - Prevents hallucinated features
   - Enforces scope boundaries
   - Tracks execution history

2. **Real-time project tracker**
   - Replaces Jira/Trello
   - Shows what's being worked on
   - Tracks completion status

3. **MVP validator**
   - Defines DONE state objectively
   - Validates MVP completion
   - Prevents premature completion

---

## 🧠 CORE PURPOSE OF /execution

### 1. Memory for LLM Execution

**What it does:**
- Stores LLM execution sessions
- Tracks what was done before
- Prevents repeated mistakes
- Creates learning loop

**Why it matters:**
- Prevents hallucinated features
- Ensures consistent execution
- Improves predictability
- Reduces waste

---

### 2. Real-time Project Tracker

**What it does:**
- Shows current sprint status
- Lists active tasks
- Tracks blockers
- Records completed work

**Why it matters:**
- Single source of truth
- Real-time visibility
- No documentation lag
- Aligns team on progress

---

### 3. MVP Validator

**What it does:**
- Defines validation gates
- Checks completion criteria
- Prevents premature completion
- Ensures quality standards

**Why it matters:**
- Objective DONE criteria
- Quality assurance
- Prevents scope creep
- Ensures MVP readiness

---

## 📁 FILE STRUCTURE

### 00_active-mvp.md
**Purpose:** Defines what is currently being built

**Content:**
- Active MVP identification
- Current scope
- Forbidden features
- Timeline

---

### 01_sprint-backlog.md
**Purpose:** List of tasks for current MVP only

**Format:**
- [ ] Task description
- Tag: component/service
- Est: hours

**Rule:**
- No task outside active MVP is allowed here

---

### 02_in-progress.md
**Purpose:** Tracks what is currently being worked on

**Format:**
- Task description
- Current status
- Progress %
- Owner

**Rule:**
- Only 3–5 tasks max at a time

---

### 03_blocked.md
**Purpose:** Tracks blockers preventing progress

**Format:**
- Blocker description
- Severity: HIGH/MED/LOW
- Root cause
- Dependency

**Rule:**
- Each blocker MUST include reason + dependency

---

### 04_done-log.md
**Purpose:** History of completed work (MVP traceability)

**Format:**
- Task description
- Date completed
- Component/service
- Notes

**Rule:**
- Never delete entries (append-only log)

---

### 05_release-notes.md
**Purpose:** MVP-level release tracking

**Format:**
- Version
- Date
- Changes
- Breaking changes

---

### 06_llm-execution-runs.md
**Purpose:** Tracks every OpenCode / LLM execution session

**Format:**
- Run # (incremental)
- Scope
- Result: completed / partial / failed
- Issues found
- Why this matters

---

### 07_scope-guard.md
**Purpose:** Prevents MVP scope drift

**Content:**
- ACTIVE MVP definition
- ALLOWED features
- FORBIDDEN features

**Rule:**
- If it's not here, OpenCode must not build it

---

### 08_mvp-checkpoints.md
**Purpose:** Defines validation gates per MVP phase

**Format:**
- [ ] Checklist item
- Status: complete / pending
- Date verified

---

## 🚫 NON-NEGOTIABLE RULES

1. **No task outside active MVP**
   - Every task must belong to current MVP
   - No future MVP tasks allowed
   - No maintenance tasks without MVP context

2. **No hidden features in backlog**
   - All planned features documented
   - No ad-hoc tasks
   - No undocumented work

3. **No skipping checkpoints**
   - Every MVP must pass all checkpoints
   - No partial completion allowed
   - Quality gates required

4. **No silent scope expansion**
   - Any feature outside scope must be rejected
   - Any deviation must be documented
   - No exceptions without approval

5. **No undocumented work**
   - Every task must be tracked
   - Every completion must be logged
   - No work happens without documentation

---

## 🧠 CORE PRINCIPLE

**Execution is not development tracking. It is MVP constraint enforcement.**

---

## 🔄 EXECUTION FLOW

### Standard Workflow

1. **Define Active MVP**
   - Identify current MVP
   - Document scope
   - Define forbidden features

2. **Create Sprint Backlog**
   - Break MVP into tasks
   - Estimate effort
   - Assign ownership

3. **Track Execution**
   - Move tasks to in-progress
   - Update progress
   - Document blockers

4. **Complete Tasks**
   - Validate against checkpoints
   - Log in done-log
   - Update scope-guard

5. **Complete MVP**
   - Pass all checkpoints
   - Verify against definition of done
   - Release with documentation

---

## 🎯 CURRENT STATUS

### Active MVP: MVP-1 (Discovery Core)

**Status:** IN PROGRESS

**Scope:**
- Map view
- Station markers
- Nearby search
- Station details

**Forbidden:**
- auth system
- admin dashboard
- partner flows
- analytics dashboards

---

## 📊 EXECUTION METRICS

### Sprint Progress

- **Total Tasks:** 0
- **Completed:** 0
- **In Progress:** 0
- **Blocked:** 0
- **Progress:** 0%

### Quality Metrics

- **Checkpoints Passed:** 0/6
- **Blockers Resolved:** 0
- **LLM Runs Successful:** 0

---

*This execution system ensures MVP completion, prevents scope drift, and provides LLM memory for consistent behavior.*