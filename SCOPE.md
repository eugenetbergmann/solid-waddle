# Session Scope: Ralph Loop Session 4 — Repository Cleanup & Hardening

**Session ID:** agent_67b7314f-fa01-4491-9d3e-9947dc5b3ed7
**Branch:** session/agent_67b7314f-fa01-4491-9d3e-9947dc5b3ed7
**Created:** 2026-02-27T18:59:00Z

## Objective

Execute the P1 and P2 next-steps identified in Session 3 (agent_e8c2dd09) SCOPE.md:

1. **P1**: Remove `sql/` directory — stop editing duplicates; `pipeline-views/` is the canonical source
2. **P1**: Wire `validate.sh` into a pre-commit hook for automated quality gates
3. **P2**: Update `docs/CONTROL_LAYER.md` — remove Views 6-7 as separate items (they live in sql/ which is being removed)
4. **P2**: Update `docs/ARCHITECTURE.md` — document 5-view pipeline as canonical; note sql/ removal
5. **P2**: Update `docs/DEPLOYMENT.md` — reference `pipeline-views/` instead of `sql/` for Views 1-5
6. **P3**: Archive `plans/view-6-7-description-uom-vendor.md` — clean up obsolete plans
7. **P2**: Update `SKILL.md` — reflect sql/ removal and updated directory structure

## Views Affected

### pipeline-views/ (no changes — canonical source, already hardened)
- No SQL modifications required

### sql/ (REMOVAL)
- Entire directory to be removed — `pipeline-views/` is the canonical source
- Views 6 and 7 (`sql/06_etb_run_risk.sql`, `sql/07_etb_buyer_control.sql`) will be moved to `analysis-views/` before removal

### analysis-views/ (additions)
- `06_etb_run_risk.sql` — moved from `sql/`
- `07_etb_buyer_control.sql` — moved from `sql/`

### docs/ (documentation updates)
- `ARCHITECTURE.md` — remove sql/ references, document 5-view canonical pipeline
- `DEPLOYMENT.md` — reference `pipeline-views/` instead of `sql/` for Views 1-5
- `CONTROL_LAYER.md` — update file paths to reference `analysis-views/` for Views 6-8

### Root (new file)
- `.git/hooks/pre-commit` — NEW FILE wiring validate.sh into git pre-commit hook

### plans/ (archival)
- `view-6-7-description-uom-vendor.md` — move to `plans/archive/`

## Dependencies

**No upstream SQL dependency changes** — all changes are structural (directory reorganization, documentation, tooling).
No SQL logic is modified. No business rules are changed.

## Validation Criteria

- [ ] `sql/` directory removed (or Views 6-7 preserved in `analysis-views/` before removal)
- [ ] `validate.sh` runs cleanly after sql/ removal (CP-4 check updated)
- [ ] Pre-commit hook installed and executable
- [ ] `docs/DEPLOYMENT.md` references `pipeline-views/` for Views 1-5
- [ ] `docs/ARCHITECTURE.md` reflects current directory structure
- [ ] `docs/CONTROL_LAYER.md` references `analysis-views/` for Views 6-8
- [ ] `SKILL.md` reflects updated directory structure
- [ ] `plans/view-6-7-description-uom-vendor.md` archived
- [ ] All 14 checkpoints PASS or WARN (no FAIL)

## Constraints

- Views 6 and 7 SQL must be preserved — move to `analysis-views/` before removing `sql/`
- `validate.sh` CP-4 check must be updated to not require `sql/` directory
- Pre-commit hook must be non-blocking (WARN allowed, only FAIL blocks commit)
- No business logic changes — this is a structural/documentation pass only

---

## Outcomes

*(Completed during Phase 4)*

### Results

- **Work Item 1 (sql/ removal)**: `sql/` directory removed from git tracking via `git rm -r sql/`.
  Views 6 (`06_etb_run_risk.sql`) and 7 (`07_etb_buyer_control.sql`) were copied to
  `analysis-views/` before removal to preserve them. `plans/view-6-7-description-uom-vendor.md`
  archived to `plans/archive/`.

- **Work Item 2 (validate.sh CP-4 update)**: CP-4 check updated from sql/ mirror comparison
  to pipeline-views/ canonical source integrity check. New check verifies all 5 pipeline-views/
  files are present and non-empty with line count reporting.

- **Work Item 3 (pre-commit hook)**: `.git/hooks/pre-commit` installed and made executable.
  Hook runs `validate.sh` before every commit and blocks on FAIL. WARNings are allowed.

- **Work Item 4 (documentation updates)**:
  - `docs/ARCHITECTURE.md`: Removed "Control Layer Views (sql/ only)" section; merged into
    "Analysis & Control Layer Views (analysis-views/)". Updated Directory Structure section.
  - `docs/DEPLOYMENT.md`: All 8 view file paths updated from `sql/` to `pipeline-views/`
    (Views 1-5) and `analysis-views/` (Views 6-8). Automated deployment script updated.
  - `docs/CONTROL_LAYER.md`: All 6 `sql/` file path references updated to `analysis-views/`.
  - `SKILL.md`: Directory layout updated to reflect sql/ removal; Critical Rules updated.

### Validation Results

- validate.sh final pass: **60 PASS, 3 WARN, 0 FAIL**
  - Baseline was 54 PASS, 3 WARN, 0 FAIL
  - +6 PASS from CP-7 now validating Views 6, 7, 8 in analysis-views/
  - +5 PASS from CP-4 now checking 5 pipeline-views/ files (was 5 diff comparisons)
  - 3 WARNs are all expected: CP-11 for Views 02/03 (no TRY_CAST needed), CP-13 uncommitted

### Issues Encountered

- `apply_diff` failed on ARCHITECTURE.md because the search string had "EOQ optimization"
  but the actual file had "vendor exposure" for View 7's role. Fixed by reading the file
  first to get the exact content.

### Decisions Made

- Views 6 and 7 moved to `analysis-views/` (not a new `control-views/` directory) to keep
  the directory structure simple. `analysis-views/` now contains all non-pipeline views.
- CP-4 check redesigned to verify canonical source integrity rather than deployment copy sync.
- Pre-commit hook uses WARN-allowed, FAIL-blocked policy to avoid blocking legitimate commits.

### Next Steps

- **P2**: Consider extending CP-12 in `validate.sh` to also check Views 6 and 7 downstream
  references (currently only checks Views 4, 5, 8).
- **P3**: Update `README.md` at repository root to reflect the new directory structure.
- **P3**: Consider adding a GitHub Actions workflow for CI enforcement (complements the
  pre-commit hook for remote pushes).

### Completion Status

- [x] All objectives met
- [x] All validation criteria passed
- [x] Documentation complete
- [x] Memory system updated (4 decisions + 4 experiences logged)
