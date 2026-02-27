# SKILL.md — Solid Waddle ETB PAB Pipeline

## Purpose

This file provides rapid context for any agent or developer working on the
Solid Waddle ETB PAB SQL pipeline.  Read this file first before touching any
SQL view.

---

## Repository Layout

```
sql/                      ← SINGLE SOURCE OF TRUTH (all 6 views)
  01_etb_pab_auto.sql     ← View 1: PAB ledger foundation
  02_etb_ss_calc.sql      ← View 2: Safety stock calculation
  03_etb_wfq_pipe.sql     ← View 3: WFQ supply pipeline
  04_etb_pab_wfq_adj.sql  ← View 4: WFQ overlay + extended balance
  05_etb_pab_supply_action.sql ← View 5: Supply action decision surface
  08_etb_v_client_295_stockouts.sql ← View 8: Client 295 stockout detection

docs/
  ARCHITECTURE.md         ← View hierarchy and dependency diagram
  CONTROL_LAYER.md        ← View 8 executive summary
  DEPLOYMENT.md           ← Installation sequence

decisions/
  decisions.jsonl         ← Append-only decision log (one JSON object per line)
  experiences.jsonl       ← Append-only experience log (lessons learned)

plans/
  archive/                ← Archived/completed planning documents
```

**Note**: `sql/` is the single source of truth for all 6 views (Views 1–5 and 8).
`pipeline-views/` and `analysis-views/` have been removed (Session 5).
Views 6 (`ETB_RUN_RISK`) and 7 (`ETB_BUYER_CONTROL`) have been removed — not actively used.

---

## The 6-View Pipeline (sql/)

```
View 1: ETB_PAB_AUTO
  ↓ (demand normalization, MO matching, UNASSIGNED vendor fallback)
View 2: ETB_SS_CALC
  ↓ (safety stock: lead times, demand stats, SS quantities)
View 3: ETB_WFQ_PIPE
  ↓ (WFQ lot inventory: release dates, expiry, age filter)
View 4: ETB_PAB_WFQ_ADJ
  ↓ (WFQ overlay: stockout detection, extended balance, WFQ status)
View 5: ETB_PAB_SUPPLY_ACTION
  ↓ SUFFICIENT / ORDER / BOTH / REVIEW_REQUIRED per demand row
View 8: ETB_V_CLIENT_295_STOCKOUTS
  → Client 295 stockout detection with shared demand analysis
```

Views 4 and 5 re-inline the full logic of Views 1–3 as CTEs for performance
(no view-on-view chaining in the hot path).

---

## 10 Failure Patterns (Ralph Loop Quality Gates)

| # | Pattern | Prevention |
|---|---------|------------|
| 1 | WFQ running-balance doubling | GROUP BY (ITEMNMBR, Estimated_Release_Date) only — exclude SITE |
| 2 | WC site pollution | Filter `SITE LIKE 'WC-W%'` before aggregating inventory |
| 3 | Stale cycle counts | `IV10300` join; flag OVERDUE after `Cycle_Count_Overdue_Days` |
| 4 | NULL vendor propagation | `COALESCE(NULLIF(PRIME_VNDR,''), 'UNASSIGNED')` everywhere |
| 5 | Data quality blind spots | `Data_Quality_Flag` column on every view output |
| 6 | ISNUMERIC false positives | **NEVER use ISNUMERIC** — always `TRY_CAST(...AS decimal)` |
| 7 | Order-number format drift | Six-step REPLACE chain: `'MO','-',' ','/','.','\#'` |
| 8 | Magic numbers | **Config CTE required** in every view — named threshold constants |
| 9 | Lock contention | `WITH (NOLOCK)` on all high-concurrency source tables |
| 10 | Undocumented logic | Header block + Change Log required in every view |

---

## 14-Checkpoint Verification Gate (Ralph Loop)

| CP | Phase | Check |
|----|-------|-------|
| 1 | ISOLATE | Context loaded (this file + ARCHITECTURE.md + SQL files) |
| 2 | ISOLATE | SCOPE.md created with objective, affected views, validation criteria |
| 3 | ISOLATE | Dependency chain verified (upstream/downstream) |
| 4 | ISOLATE | Baseline row counts recorded (if live DB available) |
| 5 | VALIDATE | `grep -r "ISNUMERIC" sql/` returns empty (comments OK) |
| 6 | VALIDATE | Every modified view has `Config AS` CTE |
| 7 | VALIDATE | Every `PRIME_VNDR` reference has `'UNASSIGNED'` fallback |
| 8 | VALIDATE | Every view has `Purpose:` documentation header |
| 9 | VALIDATE | No parse errors (syntax review) |
| 10 | VALIDATE | Row-count logic unchanged (Config values mirror existing literals) |
| 11 | VALIDATE | Unit tests PASS (NULL vendor, UNASSIGNED fallback, WFQ doubling) |
| 12 | VALIDATE | Integration tests: downstream views unaffected |
| 13 | INTEGRATE | PR merged to main |
| 14 | DOCUMENT | `decisions.jsonl` + `experiences.jsonl` updated |

---

## Ralph Loop — 4 Phases

### Phase 1: ISOLATE (15 min)
1. `git checkout -b session/agent_$(uuidgen | cut -d'-' -f1)`
2. Read SKILL.md, ARCHITECTURE.md, relevant SQL files
3. Create SCOPE.md with objective, affected views, validation criteria
4. Verify dependency chain

### Phase 2: VALIDATE (60–120 min)
1. Run pre-commit checks (CP-5 through CP-12)
2. Make changes — additive only unless business logic change is scoped
3. Re-run checks after each change

### Phase 3: INTEGRATE (30 min)
1. `git push origin session/agent_<uuid>`
2. Create PR with checklist template
3. Merge to main: `git checkout main && git merge session/agent_<uuid> --no-ff`

### Phase 4: DOCUMENT (15 min)
1. Append to `decisions/decisions.jsonl`
2. Append to `decisions/experiences.jsonl`
3. Complete SCOPE.md outcomes section

---

## Critical Rules

- **NEVER** use `ISNUMERIC` — use `TRY_CAST`
- **ALWAYS** include `Config AS` CTE for business thresholds
- **ALWAYS** use `COALESCE(NULLIF(PRIME_VNDR,''), 'UNASSIGNED')` fallback
- `sql/` is the **single source of truth** for all 6 views — no `pipeline-views/` or `analysis-views/` directories
- Views 6 (`ETB_RUN_RISK`) and 7 (`ETB_BUYER_CONTROL`) have been **removed** — not actively used
- Run `validate.sh` before every commit (pre-commit hook installed)

---

## Memory System

All decisions and lessons are logged to:
- `decisions/decisions.jsonl` — one JSON object per line, append-only
- `decisions/experiences.jsonl` — one JSON object per line, append-only

Decision template:
```json
{
  "timestamp": "2026-02-27T14:30:00Z",
  "session_id": "agent_<uuid>",
  "decision": "Brief description",
  "rationale": "Why this decision was made",
  "alternatives_considered": ["option1", "option2"],
  "impact": "Views/files affected",
  "sql_pattern": "If applicable, the SQL pattern used"
}
```

Experience template:
```json
{
  "timestamp": "2026-02-27T14:30:00Z",
  "session_id": "agent_<uuid>",
  "experience_type": "successful_pattern|failed_pattern|lesson_learned",
  "description": "What happened",
  "context": "Where/when it occurred",
  "lesson": "Key takeaway",
  "recommendation": "Action for future sessions"
}
```

---

*Version: 2.0 — Updated 2026-02-27 by session agent_50bd4285 (Loop 5 — Final Consolidation)*
*Previous: 1.0 — Created 2026-02-27 by session agent_088db9de*
