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
                    └──────────┬──────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  ETB_RUN_RISK    │  │ ETB_BUYER_CONTROL│  │ ETB_V_CLIENT_295 │
│  (View 6 —       │  │  (View 7 — Buyer │  │  _STOCKOUTS      │
│   Executive Risk │  │   PO Consolidation│  │  (View 8 —       │
│   Dashboard)     │  │   Engine)        │  │   Client 295     │
└──────────────────┘  └──────────────────┘  │   Stockout       │
                                             │   Detection)     │
                                             └──────────────────┘
```

**Note**: Views 4 and 5 re-inline the full logic of Views 1–3 as CTEs for
performance (no view-on-view chaining in the hot path).  Views 6, 7, and 8
consume `dbo.ETB_PAB_SUPPLY_ACTION` (View 5) and `dbo.ETB_SS_CALC` (View 2).

---

## Object Catalog

### Core Pipeline Views (pipeline-views/)

| # | Object | Role | Dependencies | Status |
|---|--------|------|--------------|--------|
| 1 | ETB_PAB_AUTO | PAB ledger foundation: demand normalization, MO matching, UNASSIGNED vendor fallback | `ETB_PAB_MO`, `ETB_ActiveDemand_Union_FG_MO`, `Prosenthal_Vendor_Items`, `PK010033`, `WO010032`, `IV00101` | Production |
| 2 | ETB_SS_CALC | Safety stock calculation: lead times, demand statistics, SS quantities | `ETB_SS`, `ReceivingsLineItems`, `POP30330`, `PHR_MO_CostCalc1` | Production |
| 3 | ETB_WFQ_PIPE | WFQ pipeline source: lot-level quarantine inventory with release estimates | `IV00300`, `IV00101` | Production |
| 4 | ETB_PAB_WFQ_ADJ | WFQ overlay: stockout detection, extended balance, WFQ status classification | Views 1–3 (re-inlined) + `Prosenthal_INV_BIN_QTY_wQTYTYPE`, `IV10300` | Production |
| 5 | ETB_PAB_SUPPLY_ACTION | Final decision surface: deficit analysis, PO timing, supply action recommendations | Views 1–4 (re-inlined) | Production |

### Control Layer Views (sql/ only)

| # | Object | Role | Dependencies | Status |
|---|--------|------|--------------|--------|
| 6 | ETB_RUN_RISK | Executive risk dashboard: stockout timing, client exposure, schedule threats | View 5 + View 2 | Production |
| 7 | ETB_BUYER_CONTROL | Buyer action queue: PO consolidation, urgency classification, vendor exposure | View 5 + View 2 | Production |
| 8 | ETB_V_CLIENT_295_STOCKOUTS | Client 295 stockout detection: item/run-level risk with shared demand analysis | View 5 + View 2 + View 4 | Production |

### Analysis Views (analysis-views/)

| # | Object | Role | Notes |
|---|--------|------|-------|
| — | ETB_WC_INV_UNIFIED | WC inventory integration with running balance adjustments | Reference only — logic re-inlined in Views 4 & 5 |
| 8 | ETB_V_CLIENT_295_STOCKOUTS | Client 295 stockout detection | Canonical copy in `analysis-views/`; deployment copy in `sql/` |

---

## Data Flow

1. **ETB_PAB_AUTO** (View 1): Ingests raw demand and MO data, applies inventory suppression rules, attaches vendor and item master data
2. **ETB_SS_CALC** (View 2): Calculates safety stock quantities using historical weekly demand variability and series-specific lead times
3. **ETB_WFQ_PIPE** (View 3): Provides WFQ supply pipeline data — lot-level quarantine inventory with estimated release dates
4. **ETB_PAB_WFQ_ADJ** (View 4): Extends ledger with WFQ supply coverage, calculates extended balances, classifies WFQ dependency status
5. **ETB_PAB_SUPPLY_ACTION** (View 5): Evaluates supply adequacy, generates action recommendations (SUFFICIENT/ORDER/BOTH/REVIEW_REQUIRED)
6. **ETB_RUN_RISK** (View 6): Compresses demand rows into risk signals per item/vendor — answers WHERE/WHEN/WHO/HOW
7. **ETB_BUYER_CONTROL** (View 7): Groups deficit demand into PO recommendations with urgency classification and EOQ optimization
8. **ETB_V_CLIENT_295_STOCKOUTS** (View 8): Scopes stockout detection to Client 295 with market-wide demand context

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
pipeline-views/    ← CANONICAL SOURCE for Views 1–5
analysis-views/    ← Analysis views (WC Unified, Client 295)
sql/               ← DEPLOYMENT COPIES (mirrors pipeline-views/ + analysis-views/)
docs/              ← Architecture documentation (this file)
decisions/         ← Memory system (decisions.jsonl, experiences.jsonl)
SKILL.md           ← Quick-start context for agents and developers
```

**Rule**: `sql/01–05` must always be identical to `pipeline-views/01–05`.
`sql/02_etb_wc_inv_unified.sql` must always be identical to
`analysis-views/02_etb_wc_inv_unified.sql`.
