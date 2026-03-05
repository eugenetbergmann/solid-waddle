# ETB PAB Supply Chain Deployment

## Deployment Sequence

Execute views in the following order to maintain dependency integrity.
Views 4 and 5 re-inline Views 1–3 as CTEs (no view-on-view chaining in the hot path),
but the underlying database views must still exist before downstream views can reference them.

| Step | Object | File | Notes |
|------|--------|------|-------|
| 1 | ETB_PAB_AUTO | `sql/01_etb_pab_auto.sql` | Foundation layer — demand normalization, MO matching, UNASSIGNED vendor fallback |
| 2 | ETB_SS_CALC | `sql/02_etb_ss_calc.sql` | Safety stock reference — lead times, demand statistics, SS quantities |
| 3 | ETB_WFQ_PIPE | `sql/03_etb_wfq_pipe.sql` | WFQ supply pipeline — lot-level quarantine inventory with release estimates |
| 4 | ETB_PAB_WFQ_ADJ | `sql/04_etb_pab_wfq_adj.sql` | WFQ overlay — stockout detection, extended balance, WFQ status. Session 5: Smart quotes fixed, IV10300 stub (no cycle count data), CREATE VIEW removed. |
| 5 | ETB_PAB_SUPPLY_ACTION | `sql/05_etb_supply_action.sql` | Decision surface — SUFFICIENT/ORDER/BOTH/REVIEW_REQUIRED per demand row |
| 6 | ETB_STOCKOUTS | `sql/10_etb_stockouts.sql` | 180-day forward stockout aggregation — program flags, max deficit, action counts |
| 7 | ETB_PROGRAM_WEIGHTS + ETB_WEIGHTED_DEMAND + ETB_WEIGHTED_SUMMARY | `sql/11_weighted_universe.sql` | Weighted Universe — credibility weights table/views, RAW vs WEIGHTED demand deltas |

**Dependency note**: `ETB_STOCKOUTS` (step 6) and the Weighted Universe objects (step 7) both consume
`dbo.ETB_PAB_SUPPLY_ACTION` (View 5). Always deploy in the order above.

**Step 7 note**: `sql/11_weighted_universe.sql` creates a TABLE (`ETB_PROGRAM_WEIGHTS`),
an audit TABLE (`ETB_PROGRAM_WEIGHTS_AUDIT`), and three views. Deploy via SSMS in
a single execution pass. Weights are editable in `dbo.ETB_PROGRAM_WEIGHTS`.

**Removed views**: Views 6 (`ETB_RUN_RISK`), 7 (`ETB_BUYER_CONTROL`), 08 (`ETB_V_CLIENT_295_STOCKOUTS`), 
and 09 (`ETB_RALPH_LOOP_37D`) have been removed from the pipeline — superseded by `ETB_STOCKOUTS` (file 10).

---

## Prerequisites

- SQL Server 2016+ (for `TRY_CAST`, `STRING_AGG` requires 2017+)
- Appropriate database permissions (`CREATE VIEW`, `SELECT` on dependent objects)
- All upstream source tables already present in the target database:
  - `dbo.ETB_PAB_MO`, `dbo.ETB_ActiveDemand_Union_FG_MO`
  - `dbo.Prosenthal_Vendor_Items`, `dbo.PK010033`, `dbo.WO010032`, `dbo.IV00101`
  - `dbo.Prosenthal_INV_BIN_QTY_wQTYTYPE`
  - `dbo.ReceivingsLineItems`, `dbo.POP30330`, `dbo.PHR_MO_CostCalc1`, `dbo.ETB_SS`
  - `dbo.IV00300`
  - **Note:** `dbo.IV10300` (cycle count table) is **not required** — View 4 CycleCount CTE is stubbed with `WHERE 1=0`. Cycle count columns output as NULL/NEVER_COUNTED.

---

## Deployment Steps

### Manual Deployment via SSMS

1. Open SSMS and connect to the target database
2. Execute each script in the order listed in the table above (Steps 1–6)
3. Verify each view after deployment (see Validation Queries below)

### Automated Deployment (SQLCMD)

```bash
# Deploy all views in dependency order
for f in sql/01_etb_pab_auto.sql \
          sql/02_etb_ss_calc.sql \
          sql/03_etb_wfq_pipe.sql \
          sql/04_etb_pab_wfq_adj.sql \
          sql/05_etb_supply_action.sql \
          sql/10_etb_stockouts.sql \
          sql/11_weighted_universe.sql; do
    echo "Deploying $f..."
    sqlcmd -S <server> -d <database> -i "$f"
done
```

---

## Validation Queries

### View 1 — ETB_PAB_AUTO

```sql
SELECT * FROM sys.views WHERE name = 'ETB_PAB_AUTO';
SELECT TOP 5 ITEMNMBR, PRIME_VNDR, Data_Quality_Flag FROM dbo.ETB_PAB_AUTO;
```

### View 2 — ETB_SS_CALC

```sql
SELECT * FROM sys.views WHERE name = 'ETB_SS_CALC';
SELECT TOP 5 ITEMNMBR, PRIME_VNDR, LeadDays, CalculatedSS_MfgUOM FROM dbo.ETB_SS_CALC;
```

### View 3 — ETB_WFQ_PIPE

```sql
SELECT * FROM sys.views WHERE name = 'ETB_WFQ_PIPE';
SELECT TOP 5 Item_Number, SITE, QTY_ON_HAND, Estimated_Release_Date FROM dbo.ETB_WFQ_PIPE;
```

### View 4 — ETB_PAB_WFQ_ADJ

```sql
SELECT * FROM sys.views WHERE name = 'ETB_PAB_WFQ_ADJ';
SELECT TOP 5 ITEMNMBR, PRIME_VNDR, Ledger_Extended_Balance, WFQ_Extended_Status
FROM dbo.ETB_PAB_WFQ_ADJ;
```

### View 5 — ETB_PAB_SUPPLY_ACTION

```sql
SELECT * FROM sys.views WHERE name = 'ETB_PAB_SUPPLY_ACTION';
SELECT TOP 10
    ITEMNMBR,
    Supply_Action_Recommendation,
    Additional_Order_Qty,
    Data_Quality_Flag
FROM dbo.ETB_PAB_SUPPLY_ACTION;
```

### View 10 — ETB_STOCKOUTS

```sql
SELECT * FROM sys.views WHERE name = 'ETB_STOCKOUTS';
SELECT TOP 10
    Item_Number, Description, First_Deficit_Date,
    Min_Projected_Stockout, Max_Deficit_180D,
    COUNT_ORDER, COUNT_BOTH, URGENT_COUNT
FROM dbo.ETB_STOCKOUTS
ORDER BY URGENT_COUNT DESC, Min_Projected_Stockout ASC;
```

### View 11 — Weighted Universe

```sql
SELECT * FROM sys.tables WHERE name = 'ETB_PROGRAM_WEIGHTS';
SELECT * FROM sys.views WHERE name IN ('ETB_CURRENT_PROGRAM_WEIGHTS', 'ETB_WEIGHTED_DEMAND', 'ETB_WEIGHTED_SUMMARY');
SELECT TOP 5 * FROM dbo.ETB_CURRENT_PROGRAM_WEIGHTS;
SELECT TOP 5 ITEMNMBR, RAW_Net_Demand, Weighted_Net_Demand, Program_Weight FROM dbo.ETB_WEIGHTED_DEMAND;
```

---

## Validation Checklist

- [ ] All views/tables created successfully (`sys.views` / `sys.tables` check)
- [ ] No errors during execution
- [ ] Views return expected columns
- [ ] `Supply_Action_Recommendation` populated for all rows in View 5
- [ ] No NULL values in `PRIME_VNDR` (should be `'UNASSIGNED'` as fallback)
- [ ] `ETB_STOCKOUTS` returns rows with `HAVING MIN < 0` filter active
- [ ] `ETB_PROGRAM_WEIGHTS` table created; populate weights before querying `ETB_WEIGHTED_DEMAND`

---

## Rollback Procedure

```sql
-- Drop in reverse dependency order
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_WEIGHTED_SUMMARY')     DROP VIEW dbo.ETB_WEIGHTED_SUMMARY;
GO
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_WEIGHTED_DEMAND')      DROP VIEW dbo.ETB_WEIGHTED_DEMAND;
GO
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_CURRENT_PROGRAM_WEIGHTS') DROP VIEW dbo.ETB_CURRENT_PROGRAM_WEIGHTS;
GO
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'ETB_PROGRAM_WEIGHTS_AUDIT')  DROP TABLE dbo.ETB_PROGRAM_WEIGHTS_AUDIT;
GO
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'ETB_PROGRAM_WEIGHTS')        DROP TABLE dbo.ETB_PROGRAM_WEIGHTS;
GO
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_STOCKOUTS')            DROP VIEW dbo.ETB_STOCKOUTS;
GO
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_PAB_SUPPLY_ACTION')
    DROP VIEW dbo.ETB_PAB_SUPPLY_ACTION;
GO
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_PAB_WFQ_ADJ')
    DROP VIEW dbo.ETB_PAB_WFQ_ADJ;
GO
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_WFQ_PIPE')
    DROP VIEW dbo.ETB_WFQ_PIPE;
GO
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_SS_CALC')
    DROP VIEW dbo.ETB_SS_CALC;
GO
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_PAB_AUTO')
    DROP VIEW dbo.ETB_PAB_AUTO;
GO
```

---

## Key Output Columns by View

### View 5 — ETB_PAB_SUPPLY_ACTION (Primary Decision Surface)

| Column | Description |
|--------|-------------|
| `Supply_Action_Recommendation` | `SUFFICIENT`, `ORDER`, `BOTH`, `REVIEW_REQUIRED` |
| `Additional_Order_Qty` | Quantity to order after accounting for existing POs |
| `Deficit_Qty` | Shortfall between demand and extended balance |
| `PO_On_Time` | 1 if PO arrives before demand due date |
| `Is_Past_Due_In_Backlog` | 1 if demand due date has passed |
| `Data_Quality_Flag` | `CLEAN`, `MISSING_VENDOR`, `MISSING_COST`, `MISSING_BOTH`, `NON_WC_SITE` |

### View 10 — ETB_STOCKOUTS (180-Day Aggregation)

| Column | Description |
|--------|-------------|
| `Item_Number` | Item identifier |
| `First_Deficit_Date` | Earliest demand due date with deficit |
| `Min_Projected_Stockout` | Worst projected balance (MIN) |
| `Max_Deficit_180D` | Largest single demand deficit |
| `Program_291_Flag` ... `Program_303_Flag` | 1 if item has demand from that construct |
| `COUNT_ORDER` | Count of ORDER supply action rows |
| `COUNT_BOTH` | Count of BOTH supply action rows |
| `URGENT_COUNT` | ORDER/BOTH rows due within 10 days |
| `WFQ_Rescue_Count` | Count of WFQ_RESCUED demand rows |
