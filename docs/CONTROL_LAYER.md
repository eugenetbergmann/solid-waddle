# Control Layer Views — Executive Summary

## Purpose

Two production-ready SQL Server views that transform operational supply chain data into actionable executive and buyer intelligence.

---

## View 1: [`dbo.ETB_RUN_RISK`](../sql/06_etb_run_risk.sql)

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

## View 2: [`dbo.ETB_BUYER_CONTROL`](../sql/07_etb_buyer_control.sql)

**PO consolidation and buyer action engine**

### What It Does
Groups demand into lead-time-aligned buckets and recommends consolidated PO quantities.

### Key Metrics
- **Bucket_Date**: Lead-time-aligned demand window start date
- **Recommended_PO_Qty**: Deficit + Safety Stock (mathematically defensible order quantity)
- **Earliest_Demand_Date**: Drop-dead date for PO placement
- **Urgency**: Categorical action signal (PLACE_NOW / PLAN / MONITOR)
- **Demand_Lines_In_Bucket**: Number of demand rows consolidated
- **Vendor_Total_Exposure**: Total deficit across all items for this vendor

### Business Questions Answered
1. **WHAT do I order?** (Recommended_PO_Qty)
2. **WHEN do I order?** (Urgency)
3. **HOW many POs can I consolidate?** (Demand_Lines_In_Bucket)
4. **Which vendors are at risk?** (Vendor_Total_Exposure)

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

## Architecture Principles

### What These Views DO
✅ Aggregate operational data into decision signals  
✅ Apply deterministic business rules  
✅ Surface risk and urgency categorically  
✅ Enable Excel export with zero transformation  
✅ Reduce buyer workload through smart consolidation  

### What These Views DO NOT Do
❌ Forecast future demand  
❌ Calculate EOQ or service levels  
❌ Modify upstream logic  
❌ Create new tables or staging structures  
❌ Use statistical modeling  

---

## Data Flow

```
dbo.ETB_PAB_SUPPLY_ACTION (operational ledger)
         +
dbo.ETB_SS_CALC (safety stock reference)
         ↓
    ┌────────────────────┐
    │  CONTROL LAYER     │
    ├────────────────────┤
    │ ETB_RUN_RISK       │ → Executive risk dashboard
    │ ETB_BUYER_CONTROL  │ → Buyer action queue
    └────────────────────┘
```

---

## Deployment

### Prerequisites
- [`dbo.ETB_PAB_SUPPLY_ACTION`](../sql/05_etb_pab_supply_action.sql) must exist
- `dbo.ETB_SS_CALC` must exist

### Installation
Execute in sequence:
1. [`sql/06_etb_run_risk.sql`](../sql/06_etb_run_risk.sql)
2. [`sql/07_etb_buyer_control.sql`](../sql/07_etb_buyer_control.sql)

Or use combined deployment script:
```sql
-- Execute all at once
:r sql/08_control_layer_deployment.sql
```

### Validation
```sql
-- Verify view creation
SELECT name, type_desc 
FROM sys.views 
WHERE name IN ('ETB_RUN_RISK', 'ETB_BUYER_CONTROL');

-- Test row counts
SELECT 'ETB_RUN_RISK' AS ViewName, COUNT(*) AS RowCount FROM dbo.ETB_RUN_RISK
UNION ALL
SELECT 'ETB_BUYER_CONTROL', COUNT(*) FROM dbo.ETB_BUYER_CONTROL;
```

---

## Edge Case Handling

| Scenario | Handling |
|----------|----------|
| Demand_Due_Date is NULL | Excluded via WHERE clause |
| Deficit_Qty ≤ 0 | Excluded (overages not stockout signals) |
| No matching safety stock record | LEFT JOIN with ISNULL(LeadDays, 30) fallback |
| WFQ_Extended_Status is NULL | Treated as 0 (no WFQ dependency) |
| Construct is NULL | Excluded from Client_Exposure_Count |
| First_Stockout_Date is NULL | Days_To_Stockout = NULL, Schedule_Threat = 0 |

---

## Performance Characteristics

- **Target execution time**: < 2 seconds on 500K+ row tables
- **Indexing recommendations**:
  - `ETB_PAB_SUPPLY_ACTION`: (ITEMNMBR, PRIME_VNDR, Demand_Due_Date) INCLUDE (Deficit_Qty, Suppression_Status)
  - `ETB_SS_CALC`: (ITEMNMBR, PRIME_VNDR) INCLUDE (LeadDays, CalculatedSS_PurchasingUOM)

---

## Success Criteria

The implementation is correct when:

✅ Planners instantly see which items will stockout and when  
✅ Leadership can quantify: "15 clients impacted, 3,200 units short, WFQ covers 2 items only"  
✅ Buyers see exactly what to order (Recommended_PO_Qty) and when (Urgency)  
✅ PO count naturally decreases due to smart bucketization  
✅ Vendor risk surfaces early—no surprises in receiving  
✅ Both views export cleanly to Excel with zero transformation  
✅ No additional post-processing required  

---

## Operational Clarity

These views answer two fundamental questions:

1. **WHERE will we fail?** → [`ETB_RUN_RISK`](../sql/06_etb_run_risk.sql)
2. **WHAT do we order?** → [`ETB_BUYER_CONTROL`](../sql/07_etb_buyer_control.sql)

No algebra. No theory. No debate.

**Operational clarity beats elegance every time.**
