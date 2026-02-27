# ETB PAB Supply Chain Deployment

## Deployment Sequence

Execute views in the following order to maintain dependency integrity.
Views 4 and 5 re-inline Views 1–3 as CTEs (no view-on-view chaining in the hot path),
but the underlying database views must still exist before Views 6, 7, and 8 can reference them.

| Step | Object | File | Notes |
|------|--------|------|-------|
| 1 | ETB_PAB_AUTO | `sql/01_etb_pab_auto.sql` | Foundation layer — demand normalization, MO matching, UNASSIGNED vendor fallback |
| 2 | ETB_SS_CALC | `sql/02_etb_ss_calc.sql` | Safety stock reference — lead times, demand statistics, SS quantities |
| 3 | ETB_WFQ_PIPE | `sql/03_etb_wfq_pipe.sql` | WFQ supply pipeline — lot-level quarantine inventory with release estimates |
| 4 | ETB_PAB_WFQ_ADJ | `sql/04_etb_pab_wfq_adj.sql` | WFQ overlay — stockout detection, extended balance, WFQ status |
| 5 | ETB_PAB_SUPPLY_ACTION | `sql/05_etb_pab_supply_action.sql` | Decision surface — SUFFICIENT/ORDER/BOTH/REVIEW_REQUIRED per demand row |
| 6 | ETB_RUN_RISK | `sql/06_etb_run_risk.sql` | Executive risk dashboard — stockout timing, client exposure, schedule threats |
| 7 | ETB_BUYER_CONTROL | `sql/07_etb_buyer_control.sql` | Buyer action queue — PO consolidation, urgency classification, EOQ optimization |
| 8 | ETB_V_CLIENT_295_STOCKOUTS | `sql/08_etb_v_client_295_stockouts.sql` | Client 295 stockout detection — item/run-level risk with shared demand analysis |

**Dependency note**: Views 6, 7, and 8 all consume `dbo.ETB_PAB_SUPPLY_ACTION` (View 5)
and `dbo.ETB_SS_CALC` (View 2).  View 8 also consumes `dbo.ETB_PAB_WFQ_ADJ` (View 4).
Always deploy in the order above.

---

## Prerequisites

- SQL Server 2016+ (for `TRY_CAST`, `STRING_AGG` requires 2017+)
- Appropriate database permissions (`CREATE VIEW`, `SELECT` on dependent objects)
- All upstream source tables already present in the target database:
  - `dbo.ETB_PAB_MO`, `dbo.ETB_ActiveDemand_Union_FG_MO`
  - `dbo.Prosenthal_Vendor_Items`, `dbo.PK010033`, `dbo.WO010032`, `dbo.IV00101`
  - `dbo.Prosenthal_INV_BIN_QTY_wQTYTYPE`, `dbo.IV10300`
  - `dbo.ReceivingsLineItems`, `dbo.POP30330`, `dbo.PHR_MO_CostCalc1`, `dbo.ETB_SS`
  - `dbo.IV00300`

---

## Deployment Steps

### Manual Deployment via SSMS

1. Open SSMS and connect to the target database
2. Execute each script in the order listed in the table above (Steps 1–8)
3. Verify each view after deployment (see Validation Queries below)

### Automated Deployment (SQLCMD)

```bash
# Deploy all 8 views in dependency order
for f in sql/01_etb_pab_auto.sql \
          sql/02_etb_ss_calc.sql \
          sql/03_etb_wfq_pipe.sql \
          sql/04_etb_pab_wfq_adj.sql \
          sql/05_etb_pab_supply_action.sql \
          sql/06_etb_run_risk.sql \
          sql/07_etb_buyer_control.sql \
          sql/08_etb_v_client_295_stockouts.sql; do
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

### View 6 — ETB_RUN_RISK

```sql
SELECT * FROM sys.views WHERE name = 'ETB_RUN_RISK';
SELECT TOP 5
    ITEMNMBR, PRIME_VNDR, First_Stockout_Date,
    Days_To_Stockout, Total_Deficit_Qty, Schedule_Threat
FROM dbo.ETB_RUN_RISK;
```

### View 7 — ETB_BUYER_CONTROL

```sql
SELECT * FROM sys.views WHERE name = 'ETB_BUYER_CONTROL';
SELECT TOP 5
    ITEMNMBR, PRIME_VNDR, Recommended_Order_Qty,
    Urgency_Classification, EOQ_Recommended_Qty
FROM dbo.ETB_BUYER_CONTROL;
```

### View 8 — ETB_V_CLIENT_295_STOCKOUTS

```sql
SELECT * FROM sys.views WHERE name = 'ETB_V_CLIENT_295_STOCKOUTS';
SELECT TOP 5
    ITEMNMBR, Run_Bucket, Is_Stockout, Stockout_Qty,
    Client295_Demand, Aggregate_Demand_All_Customers, Primary_Vendor
FROM dbo.ETB_V_CLIENT_295_STOCKOUTS;
```

---

## Validation Checklist

- [ ] All 8 views created successfully (`sys.views` check)
- [ ] No errors during execution
- [ ] Views return expected columns
- [ ] `Supply_Action_Recommendation` populated for all rows in View 5
- [ ] No NULL values in `PRIME_VNDR` (should be `'UNASSIGNED'` as fallback)
- [ ] `Is_Stockout` column populated in View 8

---

## Rollback Procedure

```sql
-- Drop in reverse dependency order
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_V_CLIENT_295_STOCKOUTS')
    DROP VIEW dbo.ETB_V_CLIENT_295_STOCKOUTS;
GO
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_BUYER_CONTROL')
    DROP VIEW dbo.ETB_BUYER_CONTROL;
GO
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_RUN_RISK')
    DROP VIEW dbo.ETB_RUN_RISK;
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

### View 6 — ETB_RUN_RISK (Executive Dashboard)

| Column | Description |
|--------|-------------|
| `First_Stockout_Date` | Earliest projected stockout date for this item/vendor |
| `Days_To_Stockout` | Calendar days until first stockout |
| `Total_Deficit_Qty` | Total units short across all demand rows |
| `Schedule_Threat` | 1 if stockout occurs before PO lead time |
| `Threatened_Clients` | Comma-separated list of impacted customers |

### View 7 — ETB_BUYER_CONTROL (Buyer Action Queue)

| Column | Description |
|--------|-------------|
| `Recommended_Order_Qty` | Suggested PO quantity |
| `Urgency_Classification` | `URGENT`, `WARNING`, `MONITOR` |
| `EOQ_Recommended_Qty` | Economic order quantity recommendation |
| `Holding_Cost_Annual` | Annual carrying cost estimate |

### View 8 — ETB_V_CLIENT_295_STOCKOUTS (Client 295 Monitor)

| Column | Description |
|--------|-------------|
| `Is_Stockout` | `YES` / `NO` — stockout flag for this item/run |
| `Stockout_Qty` | Absolute depth of worst deficit |
| `Client295_Demand` | Client 295's demand for this item/run |
| `Aggregate_Demand_All_Customers` | Total market demand for this item/run |
| `Shared_Demand_Ratio` | Client 295's share of total demand |
| `Affected_Customers` | Comma-separated list of all customers sharing this item/run |
