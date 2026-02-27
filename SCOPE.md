# Session Scope: Ralph Loop Session 5 — Final Consolidation

**Session ID:** agent_50bd4285-7ef2-40c1-96fa-dde8672a58fa
**Branch:** session/agent_50bd4285-7ef2-40c1-96fa-dde8672a58fa
**Created:** 2026-02-27T20:17:00Z

## Objective

Execute the three critical consolidation tasks identified at the end of Session 4:

1. **CRITICAL**: Remove Views 6 and 7 (`analysis-views/06_etb_run_risk.sql`,
   `analysis-views/07_etb_buyer_control.sql`) — not actively used, cause confusion
2. **CRITICAL**: Consolidate all SQL into `sql/` as the single source of truth
   (Views 1–5 from `pipeline-views/`, View 8 from `analysis-views/`)
3. **REQUIRED**: Update all documentation to reflect 6-view pipeline (Views 1–5, 8)
   and `sql/` as the canonical directory

## Views Affected

### Removed (Task 1)
- `analysis-views/06_etb_run_risk.sql` — DELETE (not actively used)
- `analysis-views/07_etb_buyer_control.sql` — DELETE (not actively used)

### Moved (Task 2)
- `pipeline-views/01_etb_pab_auto.sql` → `sql/01_etb_pab_auto.sql`
- `pipeline-views/02_etb_ss_calc.sql` → `sql/02_etb_ss_calc.sql`
- `pipeline-views/03_etb_wfq_pipe.sql` → `sql/03_etb_wfq_pipe.sql`
- `pipeline-views/04_etb_pab_wfq_adj.sql` → `sql/04_etb_pab_wfq_adj.sql`
- `pipeline-views/05_etb_pab_supply_action.sql` → `sql/05_etb_pab_supply_action.sql`
- `analysis-views/08_etb_v_client_295_stockouts.sql` → `sql/08_etb_v_client_295_stockouts.sql`

### Removed directories (Task 2)
- `pipeline-views/` — entire directory removed after move
- `analysis-views/` — entire directory removed after move (Views 6-7 deleted, View 8 moved)

### Documentation updated (Task 3)
- `SKILL.md` — update directory references, remove Views 6-7
- `docs/ARCHITECTURE.md` — remove Views 6-7, update directory structure
- `docs/DEPLOYMENT.md` — update to 6 views, `sql/` paths, remove Views 6-7
- `docs/CONTROL_LAYER.md` — remove Views 6-7 sections, update View 8 path
- `validate.sh` — update CP-3/CP-4 to check `sql/`, CP-7/CP-8/CP-9/CP-12 to use `sql/`

## Dependencies

**No upstream SQL dependency changes** — Views 6 and 7 are being removed (not used).
View 8 SQL content is unchanged; only its file path changes.
Views 1–5 SQL content is unchanged; only their file paths change.

## Validation Criteria

- [ ] `sql/` directory contains exactly 6 files (01, 02, 03, 04, 05, 08)
- [ ] `pipeline-views/` directory removed
- [ ] `analysis-views/` directory removed
- [ ] `grep -r "ETB_RUN_RISK\|ETB_BUYER_CONTROL\|View 6\|View 7" docs/ SKILL.md` returns empty
- [ ] `validate.sh` runs cleanly with 0 FAIL
- [ ] All 14 checkpoints PASS or WARN (no FAIL)

## Constraints

- Views 6 and 7 SQL files are deleted — no preservation needed (not actively used)
- `analysis-views/02_etb_wc_inv_unified.sql` — reference file, also removed with directory
- No business logic changes — structural/documentation pass only
- `validate.sh` must be updated to reference `sql/` instead of `pipeline-views/` and `analysis-views/`

---

## Outcomes

*(To be completed during Phase 4)*

