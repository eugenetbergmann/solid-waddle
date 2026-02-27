# Control Layer Views — Executive Summary

## Purpose

Three production-ready SQL Server views that transform operational supply chain
data into actionable executive and buyer intelligence.  All three consume
`dbo.ETB_PAB_SUPPLY_ACTION` (View 5) and `dbo.ETB_SS_CALC` (View 2).

---

## View 6: [`dbo.ETB_RUN_RISK`](../analysis-views/06_etb_run_risk.sql)

**Risk aggregation engine for planner + executive visibility**

### What It Does
Compresses thousands of demand rows into a single risk signal per item/vendor combination.

### Key Metrics
- **First_Stockout_Date**: Earliest date when inventory will run out
- **Days_To_Stockout**: Time remaining before stockout
- **Client_Exposure_Count**: Number of distinct customers impacted
- **Total_Deficit_Qty**: Total units short across all demand
- **WFQ_Dependency_Flag**: Binary indicator if item relies on quarantine inventory
- **Schedule_Threat**: Binary flag (1 = stockout occurs before PO can arrive)

### Business Questions Answered
1. **WHERE will we fail?** (Item + Vendor)
2. **WHEN will we fail?** (First_Stockout_Date)
3. **WHO is impacted?** (Client_Exposure_Count)
4. **HOW bad is it?** (Total_Deficit_Qty)
5. **Can we recover?** (Schedule_Threat)

### Usage
```sql
-- Critical items requiring immediate action
SELECT * 
FROM dbo.ETB_RUN_RISK
WHERE Schedule_Threat = 1
ORDER BY Days_To_Stockout ASC;

-- Items dependent on WFQ rescue
SELECT * 
FROM dbo.ETB_RUN_RISK
WHERE WFQ_Dependency_Flag = 1;

-- High-impact stockouts (multiple clients)
SELECT * 
FROM dbo.ETB_RUN_RISK
WHERE Client_Exposure_Count >= 5
ORDER BY Total_Deficit_Qty DESC;
```

---

## View 7: [`dbo.ETB_BUYER_CONTROL`](../analysis-views/07_etb_buyer_control.sql)

**PO consolidation and buyer action engine**

### What It Does
Groups demand into lead-time-aligned buckets and recommends consolidated PO quantities.

### Key Metrics
- **Earliest_Demand_Date**: Drop-dead date for PO placement
- **Recommended_PO_Qty**: Deficit + Safety Stock (mathematically defensible order quantity)
- **Urgency**: Categorical action signal (PLACE_NOW / PLAN / MONITOR)
- **Demand_Lines_In_Bucket**: Number of demand rows consolidated
- **Vendor_Total_Exposure**: Total deficit across all items for this vendor
- **Recommended_PO_Qty_Optimized**: EOQ-based recommendation with deficit floor

### Business Questions Answered
1. **WHAT do I order?** (Recommended_PO_Qty / Recommended_PO_Qty_Optimized)
2. **WHEN do I order?** (Urgency)
3. **HOW many POs can I consolidate?** (Demand_Lines_In_Bucket)
4. **Which vendors are at risk?** (Vendor_Total_Exposure)

### Vendor Fallback Hierarchy
Four-tier: `ETB_SS_CALC` → `ETB_PAB_SUPPLY_ACTION` → `ETB_PAB_WFQ_ADJ` → `UNASSIGNED`

### Usage
```sql
-- Immediate PO actions required today
SELECT * 
FROM dbo.ETB_BUYER_CONTROL
WHERE Urgency = 'PLACE_NOW'
ORDER BY Earliest_Demand_Date ASC;

-- High-consolidation opportunities (reduce PO count)
SELECT * 
FROM dbo.ETB_BUYER_CONTROL
WHERE Demand_Lines_In_Bucket >= 10
ORDER BY Recommended_PO_Qty DESC;

-- Vendor risk exposure summary
SELECT 
    PRIME_VNDR,
    MAX(Vendor_Total_Exposure) AS Total_Exposure,
    COUNT(*) AS Item_Count
FROM dbo.ETB_BUYER_CONTROL
GROUP BY PRIME_VNDR
ORDER BY Total_Exposure DESC;
```

---

## View 8: [`dbo.ETB_V_CLIENT_295_STOCKOUTS`](../analysis-views/08_etb_v_client_295_stockouts.sql)

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

### What These Views DO
✅ Aggregate operational data into decision signals  
✅ Apply deterministic business rules  
✅ Surface risk and urgency categorically  
✅ Enable Excel export with zero transformation  
✅ Reduce buyer workload through smart consolidation  

### What These Views DO NOT Do
❌ Forecast future demand  
❌ Calculate EOQ or service levels (View 7 provides EOQ as a recommendation aid only)  
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
dbo.ETB_PAB_WFQ_ADJ (View 4 — WFQ vendor fallback for View 8)
         ↓
    ┌────────────────────────────────────────┐
    │  CONTROL LAYER                         │
    ├────────────────────────────────────────┤
    │ ETB_RUN_RISK (View 6)                  │ → Executive risk dashboard
    │ ETB_BUYER_CONTROL (View 7)             │ → Buyer action queue
    │ ETB_V_CLIENT_295_STOCKOUTS (View 8)    │ → Client 295 stockout monitor
    └────────────────────────────────────────┘
```

---

## Deployment

### Prerequisites
- [`dbo.ETB_PAB_SUPPLY_ACTION`](../pipeline-views/05_etb_pab_supply_action.sql) must exist (View 5)
- `dbo.ETB_SS_CALC` must exist (View 2)
- `dbo.ETB_PAB_WFQ_ADJ` must exist (View 4) — required by View 8 vendor fallback

### Installation
Execute in sequence:
1. [`analysis-views/06_etb_run_risk.sql`](../analysis-views/06_etb_run_risk.sql)
2. [`analysis-views/07_etb_buyer_control.sql`](../analysis-views/07_etb_buyer_control.sql)
3. [`analysis-views/08_etb_v_client_295_stockouts.sql`](../analysis-views/08_etb_v_client_295_stockouts.sql)

### Validation
```sql
-- Verify view creation
SELECT name, type_desc 
FROM sys.views 
WHERE name IN ('ETB_RUN_RISK', 'ETB_BUYER_CONTROL', 'ETB_V_CLIENT_295_STOCKOUTS');

-- Test row counts
SELECT 'ETB_RUN_RISK' AS ViewName, COUNT(*) AS RowCount FROM dbo.ETB_RUN_RISK
UNION ALL
SELECT 'ETB_BUYER_CONTROL', COUNT(*) FROM dbo.ETB_BUYER_CONTROL
UNION ALL
SELECT 'ETB_V_CLIENT_295_STOCKOUTS', COUNT(*) FROM dbo.ETB_V_CLIENT_295_STOCKOUTS;

-- Verify no NULL vendors
SELECT 'ETB_RUN_RISK' AS View_Name, COUNT(*) AS Null_Vendors
FROM dbo.ETB_RUN_RISK WHERE PRIME_VNDR IS NULL
UNION ALL
SELECT 'ETB_BUYER_CONTROL', COUNT(*)
FROM dbo.ETB_BUYER_CONTROL WHERE PRIME_VNDR IS NULL
UNION ALL
SELECT 'ETB_V_CLIENT_295_STOCKOUTS', COUNT(*)
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

✅ Planners instantly see which items will stockout and when  
✅ Leadership can quantify: "15 clients impacted, 3,200 units short, WFQ covers 2 items only"  
✅ Buyers see exactly what to order (Recommended_PO_Qty) and when (Urgency)  
✅ PO count naturally decreases due to smart bucketization  
✅ Vendor risk surfaces early — no surprises in receiving  
✅ Client 295 stockout signals are clean (no WC noise, no phantom demand)  
✅ All three views export cleanly to Excel with zero transformation  
✅ No additional post-processing required  

---

## Operational Clarity

These views answer three fundamental questions:

1. **WHERE will we fail?** → [`ETB_RUN_RISK`](../analysis-views/06_etb_run_risk.sql)
2. **WHAT do we order?** → [`ETB_BUYER_CONTROL`](../analysis-views/07_etb_buyer_control.sql)
3. **IS CLIENT 295 AT RISK?** → [`ETB_V_CLIENT_295_STOCKOUTS`](../analysis-views/08_etb_v_client_295_stockouts.sql)

No algebra. No theory. No debate.

**Operational clarity beats elegance every time.**
