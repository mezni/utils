# Graph Report - .  (2026-06-09)

## Corpus Check
- Corpus is ~43,086 words - fits in a single context window. You may not need a graph.

## Summary
- 248 nodes · 292 edges · 33 communities (20 shown, 13 thin omitted)
- Extraction: 92% EXTRACTED · 8% INFERRED · 0% AMBIGUOUS · INFERRED: 23 edges (avg confidence: 0.87)
- Token cost: 8,500 input · 3,200 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Search Engine Core|Search Engine Core]]
- [[_COMMUNITY_Speckit Command Suite|Speckit Command Suite]]
- [[_COMMUNITY_Shell Common Utilities|Shell Common Utilities]]
- [[_COMMUNITY_Design System Formatting|Design System Formatting]]
- [[_COMMUNITY_Design System Generator|Design System Generator]]
- [[_COMMUNITY_Agent Context & Git Config|Agent Context & Git Config]]
- [[_COMMUNITY_Specify Scripts & Templates|Specify Scripts & Templates]]
- [[_COMMUNITY_OpenCode Command Definitions|OpenCode Command Definitions]]
- [[_COMMUNITY_Feature Branch Shell Script|Feature Branch Shell Script]]
- [[_COMMUNITY_SDD Workflow & Templates|SDD Workflow & Templates]]
- [[_COMMUNITY_Integration Configuration|Integration Configuration]]
- [[_COMMUNITY_Workflow Registry|Workflow Registry]]
- [[_COMMUNITY_Feature Branch Script (dup)|Feature Branch Script (dup)]]
- [[_COMMUNITY_Feature Branch PowerShell|Feature Branch PowerShell]]
- [[_COMMUNITY_Init Options Config|Init Options Config]]
- [[_COMMUNITY_Git Shell Common|Git Shell Common]]
- [[_COMMUNITY_Git PowerShell Common|Git PowerShell Common]]
- [[_COMMUNITY_Auto Commit Shell|Auto Commit Shell]]
- [[_COMMUNITY_Initialize Repo Shell|Initialize Repo Shell]]
- [[_COMMUNITY_OpenCode Plugin Config|OpenCode Plugin Config]]
- [[_COMMUNITY_OpenCode Plugin Dependencies|OpenCode Plugin Dependencies]]
- [[_COMMUNITY_Search Output Formatting|Search Output Formatting]]
- [[_COMMUNITY_Design System Pattern|Design System Pattern]]
- [[_COMMUNITY_Check Prerequisites|Check Prerequisites]]
- [[_COMMUNITY_Setup Plan|Setup Plan]]
- [[_COMMUNITY_Setup Tasks|Setup Tasks]]
- [[_COMMUNITY_Agent Context Update Shell|Agent Context Update Shell]]
- [[_COMMUNITY_Git Init Command|Git Init Command]]
- [[_COMMUNITY_Git Validate Command|Git Validate Command]]

## God Nodes (most connected - your core abstractions)
1. `Extension Hook System` - 12 edges
2. `DesignSystemGenerator` - 11 edges
3. `files` - 11 edges
4. `files` - 10 edges
5. `Speckit Constitution Command` - 10 edges
6. `Spec Kit Workflow Pipeline` - 9 edges
7. `_search_csv()` - 8 edges
8. `Speckit Plan Command` - 8 edges
9. `Speckit Checklist Command` - 8 edges
10. `Git Extension Definition` - 8 edges

## Surprising Connections (you probably didn't know these)
- `Speckit Constitution Command` --conceptually_related_to--> `Constitution Framework`  [INFERRED]
  .opencode/commands/speckit.constitution.md → .specify/templates/constitution-template.md
- `Speckit Constitution Command` --references--> `Constitution Template`  [EXTRACTED]
  .opencode/commands/speckit.constitution.md → .specify/templates/constitution-template.md
- `Speckit Analyze Command` --references--> `Constitution Framework`  [EXTRACTED]
  .opencode/commands/speckit.analyze.md → .specify/templates/constitution-template.md
- `Speckit Checklist Command` --references--> `Checklist Template`  [EXTRACTED]
  .opencode/commands/speckit.checklist.md → .specify/templates/checklist-template.md
- `_generate_intelligent_overrides()` --calls--> `search()`  [INFERRED]
  .opencode/skills/ui-ux-pro-max/scripts/design_system.py → .opencode/skills/ui-ux-pro-max/scripts/core.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Core Spec Kit Development Pipeline** — commands_speckit_constitution, commands_speckit_specify, commands_speckit_clarify, commands_speckit_plan, commands_speckit_tasks, commands_speckit_analyze, commands_speckit_checklist, commands_speckit_implement [EXTRACTED 1.00]
- **Git Extension Command Suite** — commands_speckit_git_commit, commands_speckit_git_feature, commands_speckit_git_initialize, commands_speckit_git_remote, commands_speckit_git_validate, extension_hook_system [EXTRACTED 1.00]
- **Spec Kit Template Artifacts** — templates_checklist_template, templates_constitution_template, commands_speckit_checklist, commands_speckit_constitution, checklist_as_unit_tests, constitution_framework [INFERRED 0.95]
- **Specify Template System** — templates_plan_template_document, templates_spec_template_document, templates_tasks_template_document [EXTRACTED 1.00]
- **Speckit Implementation Pipeline** — speckit_workflow_sddpipeline, templates_spec_template_document, templates_plan_template_document, templates_tasks_template_document [INFERRED 0.85]
- **Extension Registry System** — specify_extensions_globalconfig, git_extension_definition, agent_context_extension_definition [EXTRACTED 1.00]

## Communities (33 total, 13 thin omitted)

### Community 0 - "Search Engine Core"
Cohesion: 0.15
Nodes (15): BM25, detect_domain(), _load_csv(), Lowercase, split, remove punctuation, filter short words, Build BM25 index from documents, Score all documents against query, Load CSV and return list of dicts, Core search function using BM25 (+7 more)

### Community 1 - "Speckit Command Suite"
Cohesion: 0.26
Nodes (19): Checklist as Unit Tests for Requirements, Speckit Agent Context Update Command, Speckit Analyze Command, Speckit Checklist Command, Speckit Clarify Command, Speckit Constitution Command, Speckit Git Commit Command, Speckit Git Feature Command (+11 more)

### Community 2 - "Shell Common Utilities"
Cohesion: 0.12
Nodes (4): get_current_branch(), get_feature_paths(), has_git(), common.sh script

### Community 3 - "Design System Formatting"
Cohesion: 0.17
Nodes (16): _detect_page_type(), format_ascii_box(), format_markdown(), format_master_md(), format_page_override_md(), generate_design_system(), _generate_intelligent_overrides(), persist_design_system() (+8 more)

### Community 4 - "Design System Generator"
Cohesion: 0.16
Nodes (9): DesignSystemGenerator, Select best matching result based on priority keywords., Extract results list from search result dict., Generate complete design system recommendation., Generates design system recommendations from aggregated searches., Load reasoning rules from CSV., Execute searches across multiple domains., Find matching reasoning rule for a category. (+1 more)

### Community 5 - "Agent Context & Git Config"
Cohesion: 0.18
Nodes (15): Agent Context Extension Config, Agent Context Extension Definition, speckit.agent-context.update Command, Agent Context Extension README, Coding Agent Context File, Agent Context Update Command Definition, speckit.git.commit Command, Git Extension Config Template (+7 more)

### Community 6 - "Specify Scripts & Templates"
Cohesion: 0.13
Nodes (14): files, .specify/scripts/bash/check-prerequisites.sh, .specify/scripts/bash/common.sh, .specify/scripts/bash/create-new-feature.sh, .specify/scripts/bash/setup-plan.sh, .specify/scripts/bash/setup-tasks.sh, .specify/templates/checklist-template.md, .specify/templates/constitution-template.md (+6 more)

### Community 7 - "OpenCode Command Definitions"
Cohesion: 0.14
Nodes (13): files, .opencode/commands/speckit.analyze.md, .opencode/commands/speckit.checklist.md, .opencode/commands/speckit.clarify.md, .opencode/commands/speckit.constitution.md, .opencode/commands/speckit.implement.md, .opencode/commands/speckit.plan.md, .opencode/commands/speckit.specify.md (+5 more)

### Community 8 - "Feature Branch Shell Script"
Cohesion: 0.20
Nodes (3): create-new-feature.sh script, _extract_highest_number(), get_highest_from_branches()

### Community 9 - "SDD Workflow & Templates"
Cohesion: 0.25
Nodes (11): Git Branching Workflow Extension README, Graceful Degradation Pattern, Constitution Template, Full SDD Cycle Workflow, SDD Cycle Pipeline (Specify→Plan→Tasks→Implement), Constitution Check Gate, Implementation Plan Template, Feature Specification Template (+3 more)

### Community 10 - "Integration Configuration"
Cohesion: 0.20
Nodes (9): default_integration, installed_integrations, integration, integration_settings, opencode, integration_state_schema, invoke_separator, script (+1 more)

### Community 11 - "Workflow Registry"
Cohesion: 0.20
Nodes (9): schema_version, description, installed_at, name, source, updated_at, version, workflows (+1 more)

### Community 12 - "Feature Branch Script (dup)"
Cohesion: 0.25
Nodes (3): create-new-feature.sh script, _extract_highest_number(), get_highest_from_branches()

### Community 13 - "Feature Branch PowerShell"
Cohesion: 0.39
Nodes (7): ConvertTo-CleanBranchName(), Get-BranchName(), Get-HighestNumberFromBranches(), Get-HighestNumberFromNames(), Get-HighestNumberFromRemoteRefs(), Get-HighestNumberFromSpecs(), Get-NextBranchNumber()

### Community 14 - "Init Options Config"
Cohesion: 0.29
Nodes (6): ai, branch_numbering, here, integration, script, speckit_version

### Community 23 - "Design System Pattern"
Cohesion: 1.00
Nodes (3): Design System Generator, UI/UX Pro Max Skill, Master-Override Hierarchical Pattern

## Knowledge Gaps
- **71 isolated node(s):** `$schema`, `plugin`, `@opencode-ai/plugin`, `update-agent-context.sh script`, `auto-commit.sh script` (+66 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **13 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `search()` connect `Search Engine Core` to `Design System Formatting`, `Design System Generator`?**
  _High betweenness centrality (0.023) - this node is a cross-community bridge._
- **Why does `DesignSystemGenerator` connect `Design System Generator` to `Design System Formatting`?**
  _High betweenness centrality (0.013) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `Extension Hook System` (e.g. with `Speckit Git Commit Command` and `Speckit Git Feature Command`) actually correct?**
  _`Extension Hook System` has 2 INFERRED edges - model-reasoned connections that need verification._
- **Are the 2 inferred relationships involving `Speckit Constitution Command` (e.g. with `Constitution Framework` and `Spec Kit Workflow Pipeline`) actually correct?**
  _`Speckit Constitution Command` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `$schema`, `plugin`, `@opencode-ai/plugin` to the rest of the system?**
  _98 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Search Engine Core` be split into smaller, more focused modules?**
  _Cohesion score 0.14736842105263157 - nodes in this community are weakly interconnected._
- **Should `Shell Common Utilities` be split into smaller, more focused modules?**
  _Cohesion score 0.12418300653594772 - nodes in this community are weakly interconnected._