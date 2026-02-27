# Control Layer Views — Executive Summary

## Purpose

Production-ready SQL Server view that transforms operational supply chain
data into actionable client-specific intelligence.  Consumes
`dbo.ETB_PAB_SUPPLY_ACTION` (View 5), `dbo.ETB_SS_CALC` (View 2), and
`dbo.ETB_PAB_WFQ_ADJ` (View 4).

**Note**: Views 6 (`ETB_RUN_RISK`) and 7 (`ETB_BUYER_CONTROL`) have been
**removed** from the pipeline — they were not actively used and have been
superseded by View 8 for control layer functionality.

---

## View 8: [`dbo.ETB_V_CLIENT_295_STOCKOUTS`](../sql/08_etb_v_client_295_stockouts.sql)

**Client 295 stockout detection with shared demand analysis**

### What It Does
Provides clear, actionable stockout signals scoped to Construct 295 while
capturing market-wide demand context.  One binary stockout signal per
item/run — `Is_Stockout = YES/NO` — with full visibility into how much of
the total demand Client 295 represents (`Shared_Demand_Ratio`).

### Key Metrics
- **Is_Stockout**: YES if `MIN(Adjusted_Running_Balance) < 0` within the run
- **Stockout_Qty**: Absolute depth of the worst deficit
- **Shared_Demand_Ratio**: Client 295's demand / all customers' demand for this item/run
- **Affected_Customers**: Comma-separated list of ALL customers sharing this item/run
- **WFQ_Dependency_Flag**: 1 if any row in this run has WFQ rescue/enhancement status

### Business Questions Answered
1. **Is Client 295 at risk?** (Is_Stockout)
2. **How severe is the shortfall?** (Stockout_Qty)
3. **Is this a shared risk?** (Shared_Demand_Ratio, Affected_Customers)
4. **When does the stockout occur?** (First_Stockout_Date)

### Usage
```sql
-- All confirmed stockouts for Client 295
SELECT *
FROM dbo.ETB_V_CLIENT_295_STOCKOUTS
WHERE Is_Stockout = 'YES'
ORDER BY Stockout_Qty DESC, Run_Bucket;

-- Items where Client 295 is the sole customer at risk
SELECT *
FROM dbo.ETB_V_CLIENT_295_STOCKOUTS
WHERE Is_Stockout = 'YES'
  AND Shared_Demand_Flag = 'NO';

-- Shared risk items (multiple customers affected)
SELECT *
FROM dbo.ETB_V_CLIENT_295_STOCKOUTS
WHERE Is_Stockout = 'YES'
  AND Customer_Count > 1
ORDER BY Customer_Count DESC;
```

---

## Architecture Principles

### What This View DOES
✅ Aggregates operational data into decision signals  
✅ Applies deterministic business rules  
✅ Surfaces risk and urgency categorically  
✅ Enables Excel export with zero transformation  
✅ Scopes stockout detection to Client 295 with market-wide context  

### What This View DOES NOT Do
❌ Forecast future demand  
❌ Modify upstream logic  
❌ Create new tables or staging structures  
❌ Use statistical modeling  

---

## Data Flow

```
dbo.ETB_PAB_SUPPLY_ACTION (View 5 — operational ledger)
         +
dbo.ETB_SS_CALC (View 2 — safety stock reference)
         +
dbo.ETB_PAB_WFQ_ADJ (View 4 — WFQ vendor fallback)
         ↓
    ┌────────────────────────────────────────┐
    │  CONTROL LAYER                         │
    ├────────────────────────────────────────┤
    │ ETB_V_CLIENT_295_STOCKOUTS (View 8)    │ → Client 295 stockout monitor
    └────────────────────────────────────────┘
```

---

## Deployment

### Prerequisites
- [`dbo.ETB_PAB_SUPPLY_ACTION`](../sql/05_etb_pab_supply_action.sql) must exist (View 5)
- `dbo.ETB_SS_CALC` must exist (View 2)
- `dbo.ETB_PAB_WFQ_ADJ` must exist (View 4) — required by View 8 vendor fallback

### Installation
Execute:
1. [`sql/08_etb_v_client_295_stockouts.sql`](../sql/08_etb_v_client_295_stockouts.sql)

### Validation
```sql
-- Verify view creation
SELECT name, type_desc 
FROM sys.views 
WHERE name = 'ETB_V_CLIENT_295_STOCKOUTS';

-- Test row count
SELECT 'ETB_V_CLIENT_295_STOCKOUTS' AS ViewName, COUNT(*) AS RowCount
FROM dbo.ETB_V_CLIENT_295_STOCKOUTS;

-- Verify no NULL vendors
SELECT 'ETB_V_CLIENT_295_STOCKOUTS' AS View_Name, COUNT(*) AS Null_Vendors
FROM dbo.ETB_V_CLIENT_295_STOCKOUTS WHERE Primary_Vendor IS NULL;
```

---

## Edge Case Handling

| Scenario | Handling |
|----------|----------|
| Demand_Due_Date is NULL | Excluded via WHERE clause |
| Deficit_Qty ≤ 0 | Excluded (overages not stockout signals) |
| No matching safety stock record | LEFT JOIN with `ISNULL(LeadDays, Default_Lead_Days)` from Config |
| WFQ_Extended_Status is NULL | Treated as 0 (no WFQ dependency) |
| Construct is NULL | Excluded from Client_Exposure_Count |
| First_Stockout_Date is NULL | Days_To_Stockout = NULL, Schedule_Threat = 0 |
| PRIME_VNDR NULL in supply action | Four-tier fallback: SS_CALC → SUPPLY_ACTION → WFQ_ADJ → UNASSIGNED |
| Client 295 is sole customer | Customer_Count = 1, Shared_Demand_Flag = 'NO', Shared_Demand_Ratio = 1.0 |
| Aggregate_Demand_All_Customers = 0 | Shared_Demand_Ratio = NULL (divide-by-zero guard) |

---

## Performance Characteristics

- **Target execution time**: < 2 seconds on 500K+ row tables
- **Indexing recommendations**:
  - `ETB_PAB_SUPPLY_ACTION`: `(ITEMNMBR, PRIME_VNDR, Demand_Due_Date) INCLUDE (Deficit_Qty, Suppression_Status, WFQ_Extended_Status, ItemDescription, UOM)`
  - `ETB_SS_CALC`: `(ITEMNMBR, PRIME_VNDR) INCLUDE (LeadDays, CalculatedSS_PurchasingUOM)`

---

## Success Criteria

The implementation is correct when:

✅ Client 295 stockout signals are clean (no WC noise, no phantom demand)  
✅ `Is_Stockout = YES/NO` is populated for every item/run  
✅ `Shared_Demand_Ratio` correctly reflects Client 295's share of total demand  
✅ View exports cleanly to Excel with zero transformation  
✅ No additional post-processing required  

---

## Operational Clarity

This view answers the fundamental question:

**IS CLIENT 295 AT RISK?** → [`ETB_V_CLIENT_295_STOCKOUTS`](../sql/08_etb_v_client_295_stockouts.sql)

No algebra. No theory. No debate.

**Operational clarity beats elegance every time.**
