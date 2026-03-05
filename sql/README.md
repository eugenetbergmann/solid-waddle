# sql/ — ETB PAB Pipeline SQL Views

This directory is the **single source of truth** for all deployable SQL view definitions.
Execute in the numbered order to satisfy dependencies.

---

## View Index

### 01 — ETB_PAB_AUTO (`01_etb_pab_auto.sql`)
Foundation layer. Demand normalization, MO matching, UNASSIGNED vendor fallback.
Produces the base PAB (Projected Available Balance) rows consumed by all downstream views.

### 02 — ETB_SS_CALC (`02_etb_ss_calc.sql`)
Safety stock reference. Computes lead times, demand statistics, and safety stock quantities
per item. Referenced by Views 5 and 8.

### 03 — ETB_WFQ_PIPE (`03_etb_wfq_pipe.sql`)
WFQ supply pipeline. Lot-level quarantine inventory with estimated release dates.
Consumed by Views 4 and 5.

### 04 — ETB_PAB_WFQ_ADJ (`04_etb_pab_wfq_adj.sql`)
WFQ overlay. Stockout detection, extended balance calculation, WFQ dependency status
per demand row. Basis for supply-action recommendations in View 5.

**Session 5 fix (2026-02-27):** Unicode smart quotes purged; CycleCount CTE stubbed
(`WHERE 1=0`) due to absence of `dbo.IV10300` in target environment; `CREATE OR ALTER VIEW`
header removed for manual SSMS deployment pattern. Cycle count output columns
(`Last_Cycle_Count_Date`, `Days_Since_Last_Cycle_Count`, `Cycle_Count_Status`) preserved
in schema as NULL / NEVER_COUNTED for downstream compatibility.

### 05 — ETB_PAB_SUPPLY_ACTION (`05_etb_supply_action.sql`)
Decision surface. Produces `Supply_Action_Recommendation` (SUFFICIENT / ORDER / BOTH /
REVIEW_REQUIRED) and `Additional_Order_Qty` per demand row. Primary output for buyers.
*(File renamed from `05_etb_pab_supply_action.sql` — March 2026)*

### 10 — ETB_STOCKOUTS (`10_etb_stockouts.sql`)
180-day forward stockout aggregation view (`dbo.ETB_STOCKOUTS`). Filters to
`Data_Quality_Flag = 'CLEAN'` demand rows within the next 180 days, aggregates to
item level. Outputs program flags (291/295/298/301/303), `Max_Deficit_180D`,
`Total_PO_Qty_On_Order`, `WFQ_Rescue_Count`, `COUNT_ORDER`, `COUNT_BOTH`,
`URGENT_COUNT` (ORDER/BOTH within 10 days). HAVING clause retains only items
with `MIN(Adjusted_Running_Balance) < 0`. Replaces legacy Views 08 and 09.

### 11 — Weighted Universe (`11_weighted_universe.sql`)
Credibility weights and delta calculations. Creates two tables
(`ETB_PROGRAM_WEIGHTS`, `ETB_PROGRAM_WEIGHTS_AUDIT`) and three views
(`ETB_CURRENT_PROGRAM_WEIGHTS`, `ETB_WEIGHTED_DEMAND`, `ETB_WEIGHTED_SUMMARY`).
Weights are per Program_ID (construct number), range 0.00–1.00, with governance
audit trail. Defaults to weight 1.00 (no reduction) for unweighted programs.
Governance precondition: weight changes require PO approval.

---

## Deployment Notes

- **Deployment pattern:** Manual SSMS execution (paste-and-run). Files 01–07 are plain
  `WITH ... SELECT` statements. File 11 (`11_weighted_universe.sql`) uses
  `CREATE TABLE` and `CREATE OR ALTER VIEW` — execute in a single SSMS pass.
- **Encoding:** All files must be plain ASCII / UTF-8 without BOM. Never author SQL
  through rich-text editors (Word, web forms) — they corrupt apostrophes to smart quotes
  (U+2018/U+2019) which SQL Server cannot parse.
- **Environment constraint:** `dbo.IV10300` does not exist in the target database.
  Do not reference it in any view.
- **View 5 rename:** File is `05_etb_supply_action.sql` — the database object name
  `dbo.ETB_PAB_SUPPLY_ACTION` is unchanged. All downstream views reference the object
  name, not the file name.
- **Legacy views removed:** Views 08 (`etb_v_client_295_stockouts`) and 09 (`etb_ralph_loop_37d`) 
  superseded by `10_etb_stockouts.sql` (March 2026).

See [`docs/DEPLOYMENT.md`](../docs/DEPLOYMENT.md) for full deployment sequence and validation queries.
