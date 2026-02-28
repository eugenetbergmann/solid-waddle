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

### 05 — ETB_PAB_SUPPLY_ACTION (`05_etb_pab_supply_action.sql`)
Decision surface. Produces `Supply_Action_Recommendation` (SUFFICIENT / ORDER / BOTH /
REVIEW_REQUIRED) and `Additional_Order_Qty` per demand row. Primary output for buyers.

### 08 — ETB_V_CLIENT_295_STOCKOUTS (`08_etb_v_client_295_stockouts.sql`)
Client 295 stockout monitor. Item/run-level risk with shared demand analysis across
all customers. References Views 4 and 5.

### 09 — ETB_RALPH_LOOP_37D (`09_etb_ralph_loop_37d.sql`)
Ralph Loop 37-Day Horizon Universal View. Single Excel-ready flat table (one row per
item) covering all demand within the next 37 calendar days. Integrates item-level
stockout flag + first deficit date, client-level stockout flag, program presence flags
(Is291 / Is295 / Is298), and ETB supply coverage columns (ETB_PO_Status,
ETB_WFQ_Status, ETB_Deficit_37D). Ordered by stockout severity for direct manager
export. References View 5 (ETB_PAB_SUPPLY_ACTION) and dbo.CustomerMap.

---

## Deployment Notes

- **Deployment pattern:** Manual SSMS execution (paste-and-run). Do **not** add
  `CREATE OR ALTER VIEW` headers — files are plain `WITH ... SELECT` for direct execution.
- **Encoding:** All files must be plain ASCII / UTF-8 without BOM. Never author SQL
  through rich-text editors (Word, web forms) — they corrupt apostrophes to smart quotes
  (U+2018/U+2019) which SQL Server cannot parse.
- **Environment constraint:** `dbo.IV10300` does not exist in the target database.
  Do not reference it in any view.

See [`docs/DEPLOYMENT.md`](../docs/DEPLOYMENT.md) for full deployment sequence and validation queries.
