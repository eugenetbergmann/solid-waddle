# ETB PAB Supply Chain Architecture

## View Hierarchy & Dependencies

```
                    ┌─────────────────────┐
                    │   ETB_PAB_AUTO      │
                    │  (View 1 — Foundation)│
                    └──────────┬──────────┘
                               │
                    ┌──────────┴──────────┐
                    │   ETB_SS_CALC       │
                    │  (View 2 — Safety   │
                    │   Stock Reference)  │
                    └──────────┬──────────┘
                               │
                    ┌──────────┴──────────┐
                    │   ETB_WFQ_PIPE      │
                    │  (View 3 — WFQ      │
                    │   Supply Pipeline)  │
                    └──────────┬──────────┘
                               │
                    ┌──────────┴──────────┐
                    │  ETB_PAB_WFQ_ADJ    │
                    │  (View 4 — WFQ      │
                    │   Overlay + Extended│
                    │   Balance)          │
                    └──────────┬──────────┘
                               │
                     ┌──────────┴──────────┐
                     │ ETB_PAB_SUPPLY_ACTION│
                     │  (View 5 — Decision │
                     │   Surface)          │
                     └─────────┬───────────┘
                               │
              ┌────────────────┼────────────────────┐
              ▼                ▼                     ▼
 ┌─────────────────┐  ┌───────────────────┐  ┌─────────────────────────┐
 │ ETB_V_CLIENT_295│  │  ETB_STOCKOUTS    │  │ ETB_PROGRAM_WEIGHTS     │
 │  _STOCKOUTS     │  │  (View 10 —       │  │ ETB_WEIGHTED_DEMAND     │
 │  (View 8 —      │  │   180d Stockout   │  │ ETB_WEIGHTED_SUMMARY    │
 │   Client 295)   │  │   Aggregation)    │  │ (View 11 — Weighted     │
 └─────────────────┘  └───────────────────┘  │  Universe)              │
                                              └─────────────────────────┘
```

**Note**: Views 4 and 5 re-inline the full logic of Views 1–3 as CTEs for
performance (no view-on-view chaining in the hot path).  View 8 consumes
`dbo.ETB_PAB_SUPPLY_ACTION` (View 5) and `dbo.ETB_SS_CALC` (View 2).
Views 10 and 11 consume `dbo.ETB_PAB_SUPPLY_ACTION` (View 5) only.

Views 6 (`ETB_RUN_RISK`) and 7 (`ETB_BUYER_CONTROL`) have been **removed** —
they were not actively used and have been superseded by View 8 for control
layer functionality.

---

## Object Catalog

### Pipeline Views (sql/)

| # | Object | Role | File | Dependencies | Status |
|---|--------|------|------|--------------|--------|
| 1 | ETB_PAB_AUTO | PAB ledger foundation: demand normalization, MO matching, UNASSIGNED vendor fallback | `01_etb_pab_auto.sql` | `ETB_PAB_MO`, `ETB_ActiveDemand_Union_FG_MO`, `Prosenthal_Vendor_Items`, `PK010033`, `WO010032`, `IV00101` | Production |
| 2 | ETB_SS_CALC | Safety stock calculation: lead times, demand statistics, SS quantities | `02_etb_ss_calc.sql` | `ETB_SS`, `ReceivingsLineItems`, `POP30330`, `PHR_MO_CostCalc1` | Production |
| 3 | ETB_WFQ_PIPE | WFQ pipeline source: lot-level quarantine inventory with release estimates | `03_etb_wfq_pipe.sql` | `IV00300`, `IV00101` | Production |
| 4 | ETB_PAB_WFQ_ADJ | WFQ overlay: stockout detection, extended balance, WFQ status classification | `04_etb_pab_wfq_adj.sql` | Views 1–3 (re-inlined) + `Prosenthal_INV_BIN_QTY_wQTYTYPE`, `IV10300` | Production |
| 5 | ETB_PAB_SUPPLY_ACTION | Final decision surface: deficit analysis, PO timing, supply action recommendations | `05_etb_supply_action.sql` | Views 1–4 (re-inlined) | Production |
| 8 | ETB_V_CLIENT_295_STOCKOUTS | Client 295 stockout detection: item/run-level risk with shared demand analysis | `08_etb_v_client_295_stockouts.sql` | View 5 + View 2 + View 4 | Production |
| 9 | ETB_RALPH_LOOP_37D | 37-day horizon universal item-level view: stockout flag, program flags, ETB supply coverage | `09_etb_ralph_loop_37d.sql` | View 5 + `CustomerMap` | Production |
| 10 | ETB_STOCKOUTS | 180-day forward stockout aggregation: program flags, max deficit, action counts | `10_etb_stockouts.sql` | View 5 | Production |
| 11 | ETB_PROGRAM_WEIGHTS / ETB_WEIGHTED_DEMAND / ETB_WEIGHTED_SUMMARY | Weighted Universe: credibility weights table, RAW vs WEIGHTED demand deltas and summary | `11_weighted_universe.sql` | View 5 | Production |

---

## Data Flow

1. **ETB_PAB_AUTO** (View 1): Ingests raw demand and MO data, applies inventory suppression rules, attaches vendor and item master data
2. **ETB_SS_CALC** (View 2): Calculates safety stock quantities using historical weekly demand variability and series-specific lead times
3. **ETB_WFQ_PIPE** (View 3): Provides WFQ supply pipeline data — lot-level quarantine inventory with estimated release dates
4. **ETB_PAB_WFQ_ADJ** (View 4): Extends ledger with WFQ supply coverage, calculates extended balances, classifies WFQ dependency status
5. **ETB_PAB_SUPPLY_ACTION** (View 5): Evaluates supply adequacy, generates action recommendations (SUFFICIENT/ORDER/BOTH/REVIEW_REQUIRED)
6. **ETB_V_CLIENT_295_STOCKOUTS** (View 8): Scopes stockout detection to Client 295 with market-wide demand context

---

## Decision Logic (ETB_PAB_SUPPLY_ACTION — View 5)

| Rule | Condition | Recommendation |
|------|-----------|----------------|
| 1 | `Ledger_Extended_Balance >= Net_Demand` | SUFFICIENT |
| 2 | `Deficit_Qty > 0` AND `POs_On_Order_Qty = 0` | ORDER |
| 3 | `Deficit_Qty > 0` AND POs cover deficit but late | ORDER |
| 4 | `Deficit_Qty > 0` AND POs cover deficit and on time | SUFFICIENT |
| 5 | `Deficit_Qty > 0` AND POs partially cover deficit | BOTH |
| Default | Edge case requiring review | REVIEW_REQUIRED |

---

## WFQ Status Classification (ETB_PAB_WFQ_ADJ — View 4)

| Status | Meaning |
|--------|---------|
| `LEDGER_ONLY` | No WFQ supply available for this demand row |
| `WFQ_RESCUED` | WFQ pushed balance from negative to positive |
| `WFQ_ENHANCED` | WFQ improved an already-positive balance |
| `WFQ_INSUFFICIENT` | WFQ supply available but balance still negative |
| `NON_DEMAND_LEDGER_ROW` | BegBal or non-actionable row |
| `BEGINNING BALANCE` | BegBal seed row |

---

## Config CTE Pattern (All Views)

Every view in this pipeline uses a `Config AS` CTE as the first CTE to
centralise all business threshold constants.  This eliminates magic numbers
and provides a single authoritative source for each threshold.

```sql
WITH Config AS (
    SELECT
        7   AS Stale_Suppression_Days,    -- Days past due for stale suppression
        7   AS Fence_Suppression_Days,    -- Near-term fence window (days forward)
        45  AS WC_Inventory_Age_Days,     -- Max age of active WC bin inventory
        90  AS Cycle_Count_Overdue_Days,  -- Days before cycle count is OVERDUE
        7   AS Early_Issue_Flag_Days      -- Early issue detection threshold
),
-- ... remaining CTEs
```

---

## Directory Structure

```
sql/           ← SINGLE SOURCE OF TRUTH for all 9 views/objects (Views 1–5, 8–11)
docs/          ← Architecture documentation (this file)
decisions/     ← Memory system (decisions.jsonl, experiences.jsonl)
plans/archive/ ← Archived planning documents
SKILL.md       ← Quick-start context for agents and developers
validate.sh    ← Pre-commit validation script (14 Ralph Loop checkpoints)
```

**Rule**: `sql/` is the single source of truth for all views.
`pipeline-views/` and `analysis-views/` have been removed (Session 5 — 2026-02-27).
Views 6 (`ETB_RUN_RISK`) and 7 (`ETB_BUYER_CONTROL`) have been removed — not actively used.
