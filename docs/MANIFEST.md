# Documentation Manifest

Complete inventory of all BorneMap documentation files.

## File Listing

### Core Documents (3 files)
| File | Lines | Purpose |
|------|-------|---------|
| constitution.md | 1,500+ | Source of truth: principles, rules, architecture |
| implementation-plan.md | 1,000+ | MVP roadmap with sprint breakdowns |
| out-of-scope-registry.md | 100 | Permanently deferred features |

### Navigation (2 files)
| File | Lines | Purpose |
|------|-------|---------|
| README.md | 150 | Documentation index and how-to guide |
| SPRINT-QUICK-REF.md | 200 | Quick lookup for sprint info |

### Architecture Decisions (5 files)
| File | Lines | Purpose |
|------|-------|---------|
| adr/ADR-001-postgresql-single-database.md | 50 | PostgreSQL decision |
| adr/ADR-016-python-fastapi-mvp1.md | 50 | Python FastAPI for MVP-1 |
| adr/ADR-011-react-vite-web.md | 50 | React + Vite decision |
| adr/ADR-012-react-native-expo-mobile.md | 80 | React Native + Expo decision |
| adr/ADR-014-leaflet-openstreetmap.md | 50 | Leaflet + OSM decision |

### Project Management (4 files)
| File | Lines | Purpose |
|------|-------|---------|
| project/backlog.md | 150 | Feature backlog MVP-1 to MVP-6 |
| project/decisions.md | 200 | Small decisions for MVP-1 |
| project/bugs.md | 100 | Bug tracker template |
| project/phases/mvp-01-status.md | 250 | MVP-1 phase status and sprints |

### Technical Specifications (2 files)
| File | Lines | Purpose |
|------|-------|---------|
| api/bornemap-service.md | 600 | 16 endpoints with full documentation |
| schema/inventory-schema.md | 300 | Database schema and migrations |

---

## Total Statistics

- **Total files**: 16 markdown files
- **Total lines**: ~7,500 lines of documentation
- **Total size**: 152 KB
- **Format**: Markdown (GitHub-flavored)

## File Organization

```
docs/
├── README.md                    ← START HERE
├── MANIFEST.md                  ← This file
├── SPRINT-QUICK-REF.md         ← Sprint lookup
├── constitution.md             ← Source of truth
├── implementation-plan.md       ← Roadmap
├── out-of-scope-registry.md     ← Deferred features
├── adr/
│   ├── ADR-001-postgresql-single-database.md
│   ├── ADR-011-react-vite-web.md
│   ├── ADR-012-react-native-expo-mobile.md
│   ├── ADR-014-leaflet-openstreetmap.md
│   └── ADR-016-python-fastapi-mvp1.md
├── api/
│   └── bornemap-service.md
├── schema/
│   └── inventory-schema.md
└── project/
    ├── backlog.md
    ├── decisions.md
    ├── bugs.md
    └── phases/
        └── mvp-01-status.md
```

## Reading Order

**For New Team Members**:
1. docs/README.md (5 min)
2. AGENTS.md (5 min)
3. docs/constitution.md sections 1-2 (10 min)
4. docs/SPRINT-QUICK-REF.md (5 min)

**For Developers Starting Sprint 1.1**:
1. docs/api/bornemap-service.md (15 min)
2. docs/schema/inventory-schema.md (10 min)
3. docs/project/decisions.md (5 min)
4. docs/project/phases/mvp-01-status.md (5 min)

**For Architecture Decisions**:
1. docs/constitution.md section 14 (2 min) - ADR index
2. Relevant ADR file (5 min each)

**For Bug Reporting**:
1. docs/project/bugs.md (2 min) - Classification system
2. docs/project/decisions.md (2 min) - For decision-related bugs

## Update Procedures

**During Development**:
- Update `docs/project/phases/mvp-01-status.md` weekly
- Record bugs in `docs/project/bugs.md` as they arise
- Record decisions in `docs/project/decisions.md` before code

**At Sprint Close**:
- Archive sprint in mvp-01-status.md
- Update `docs/project/backlog.md` if scope changes
- Create new ADR if major decision was made

**At MVP Close**:
- Finalize mvp-01-status.md with all done criteria checked
- Update constitution.md if principles changed
- Create new implementation plan for next MVP

## Document Properties

**Never Edit** (once accepted):
- constitution.md (full document)
- Accepted ADRs
- Recorded decisions in decisions.md

**Updated Regularly**:
- docs/project/phases/mvp-01-status.md (weekly during sprints)
- docs/project/bugs.md (as bugs reported)
- docs/api/bornemap-service.md (as endpoints completed)
- docs/schema/inventory-schema.md (as schema evolves)

**Supersede, Don't Edit**:
- For constitution changes: write rationale, don't edit existing sections
- For ADR changes: create new ADR referencing old one
- For decision changes: create new decision with reason

## Version Control

All documentation is committed to git. No secrets in docs. If sensitive info needed, reference environment variable or secret management system.

- Commits should reference which document changed
- Diffs should be readable (markdown format helps)
- Branch and PR reviews should check documentation accuracy

---

**Generated**: MVP-1 Sprint 1.1
**Last Updated**: Project initialization
