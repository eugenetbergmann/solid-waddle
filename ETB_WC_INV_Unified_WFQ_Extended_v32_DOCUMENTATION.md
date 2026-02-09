# ETB_WC_INV_Unified_WFQ_Extended_v32 - Technical Documentation

## Overview

**View Name:** `[dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]`  
**Version:** 3.2  
**Engine:** Sovereign Schedule Deduction Engine - WFQ Pipeline Extension  
**Environment:** Microsoft Dynamics GP SQL Server  

## Purpose

This orchestration-grade view extends the immutable PAB (Projected Available Balance) ledger by intelligently applying WFQ (Work-in-Process/Quarantine) pipeline supply **only after stockout detection**. It projects available balance forward per demand row and applies WFQ coverage based on projected release dates.

## Core Architecture

### Design Principles

1. **Immutability**: Base ledger ([`ETB_WC_INV_Unified`](ETB_WC_INV_Unified)) remains unchanged
2. **Additive Coverage**: WFQ supply is overlaid, not substituted
3. **Stockout-Triggered**: WFQ only applies when `Running_Balance <= 0`
4. **Time-Aware**: WFQ supply must have `Estimated_Release_Date <= DUEDATE`
5. **Site-Aware**: WFQ supply matched to demand site (`WCID_From_MO`)

### Data Flow

```
ETB_WC_INV_Unified (Base Ledger)
    ↓
Demand Sequencing (by ITEMNMBR, DUEDATE)
    ↓
Stockout Detection (Running_Balance <= 0)
    ↓
WFQ Supply Aggregation (ETB_WFQ_PIPE)
    ↓
WFQ Allocation (post-stockout, time-constrained)
    ↓
Extended Balance Calculation
    ↓
Final Projection (Base + WFQ columns)
```

## Data Sources

### Primary Source: ETB_WC_INV_Unified
- **Type**: Demand ledger with PAB calculations
- **Key Fields**:
  - `ITEMNMBR`: Item identifier
  - `DUEDATE`: Demand due date
  - `Running_Balance`: Projected available balance
  - `Net_Demand`: Active demand quantity (> 0)
  - `WCID_From_MO`: Work center/site identifier
  - `BEG_BAL`: Beginning balance

### Secondary Source: ETB_WFQ_PIPE
- **Type**: WFQ pipeline supply projection
- **Key Fields**:
  - `Item_Number`: Item identifier
  - `SITE`: Location code (WF-Q, UNDERINV)
  - `QTY_ON_HAND`: Available WFQ quantity
  - `Estimated_Release_Date`: Projected release date
  - `View_Level`: Must be 'ITEM_LEVEL'

## CTE Architecture

### 1. Demand_Ledger
**Purpose**: Load base immutable ledger with active demand  
**Filter**: `Net_Demand > 0`  
**Output**: All 31 base columns from [`ETB_WC_INV_Unified`](ETB_WC_INV_Unified)

### 2. Demand_Seq
**Purpose**: Sequence demand rows per item by due date  
**Logic**: `ROW_NUMBER() OVER (PARTITION BY ITEMNMBR ORDER BY DUEDATE, ORDERNUMBER)`  
**Output**: Base columns + `Demand_Seq`

### 3. Stockout_Detection
**Purpose**: Identify first stockout point per item  
**Logic**: 
```sql
MIN(CASE WHEN TRY_CAST(Running_Balance AS decimal(18,6)) <= 0 
     THEN Demand_Seq END) OVER (PARTITION BY ITEMNMBR)
```
**Output**: Base columns + `Demand_Seq` + `Stockout_Seq`

### 4. WFQ_Supply
**Purpose**: Aggregate WFQ pipeline supply  
**Filters**:
- `View_Level = 'ITEM_LEVEL'`
- `QTY_ON_HAND > 0`

**Grouping**: `ITEMNMBR`, `SITE`, `Estimated_Release_Date`  
**Output**: `ITEMNMBR`, `SITE`, `Estimated_Release_Date`, `WFQ_Qty`

### 5. WFQ_Allocated
**Purpose**: Allocate WFQ supply to post-stockout demand  
**Allocation Rules**:
1. `Demand_Seq >= Stockout_Seq` (post-stockout only)
2. `Estimated_Release_Date <= DUEDATE` (available in time)
3. `WCID_From_MO = SITE` (site match, or null/empty)

**Output**: `ITEMNMBR`, `Demand_Seq`, `Ledger_Base_Balance`, `Ledger_WFQ_Influx`

### 6. Extended_Ledger
**Purpose**: Merge base ledger with WFQ allocation  
**Calculations**:
- `Ledger_Extended_Balance = Ledger_Base_Balance + Ledger_WFQ_Influx`
- `WFQ_Extended_Status` (see status matrix below)

## Output Schema

### Base Columns (31 fields - unchanged order)
All columns from [`ETB_WC_INV_Unified`](ETB_WC_INV_Unified) are preserved:

| Column | Type | Description |
|--------|------|-------------|
| `ITEMNMBR` | varchar | Item number |
| `ItemDescription` | varchar | Item description |
| `UOM` | varchar | Unit of measure |
| `ORDERNUMBER` | varchar | Manufacturing order number |
| `Construct` | varchar | Customer/construct |
| `DUEDATE` | date | Demand due date |
| `Expiry Dates` | varchar | Expiry date string |
| `Date + Expiry` | varchar | Combined date/expiry |
| `BEG_BAL` | varchar | Beginning balance |
| `Deductions` | varchar | Original deductions |
| `Expiry` | varchar | Original expiry |
| `PO's` | varchar | Purchase orders |
| `Running_Balance` | varchar | PAB running balance |
| `MRP_IssueDate` | varchar | MRP issue date |
| `WCID_From_MO` | varchar | Work center ID |
| `Issued` | decimal | Quantity issued |
| `Original_Required` | decimal | Original required quantity |
| `Net_Demand` | decimal | Net demand after inventory |
| `Inventory_Qty_Available` | decimal | Available inventory |
| `Suppression_Status` | varchar | Suppression rule status |
| `VendorItem` | varchar | Vendor item code |
| `PRIME_VNDR` | varchar | Primary vendor |
| `PURCHASING_LT` | int | Purchasing lead time |
| `PLANNING_LT` | int | Planning lead time |
| `ORDER_POINT_QTY` | decimal | Order point quantity |
| `SAFETY_STOCK` | decimal | Safety stock level |
| `FG` | varchar | Finished good code |
| `FG Desc` | varchar | Finished good description |
| `STSDESCR` | varchar | Status description |
| `MRPTYPE` | varchar | MRP type |
| `Unified_Value` | varchar | Unified tracking value |

### WFQ Extension Columns (3 new fields)

| Column | Type | Description | Calculation |
|--------|------|-------------|-------------|
| `Ledger_WFQ_Influx` | decimal(18,6) | WFQ supply allocated to this demand row | Sum of WFQ_Qty where conditions met |
| `Ledger_Extended_Balance` | decimal(18,6) | Extended balance including WFQ | `Running_Balance + Ledger_WFQ_Influx` |
| `WFQ_Extended_Status` | varchar | Coverage status indicator | See status matrix below |

## WFQ_Extended_Status Matrix

| Status | Condition | Meaning |
|--------|-----------|---------|
| `PAB_SUFFICIENT` | No stockout detected | Base PAB covers all demand |
| `PRE_STOCKOUT` | Before stockout sequence | Demand covered by base PAB |
| `WFQ_COVERED` | Post-stockout + WFQ > 0 | Demand covered by WFQ supply |
| `WFQ_INSUFFICIENT` | Post-stockout + WFQ = 0 | Stockout not resolved by WFQ |
| `UNKNOWN` | Edge case | Unexpected state |

## Usage Examples

### Example 1: Identify WFQ-Covered Demand
```sql
SELECT 
    ITEMNMBR,
    ORDERNUMBER,
    DUEDATE,
    Running_Balance,
    Ledger_WFQ_Influx,
    Ledger_Extended_Balance,
    WFQ_Extended_Status
FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]
WHERE WFQ_Extended_Status = 'WFQ_COVERED'
ORDER BY ITEMNMBR, DUEDATE;
```

### Example 2: Stockout Analysis
```sql
SELECT 
    ITEMNMBR,
    COUNT(*) AS Total_Demand_Rows,
    SUM(CASE WHEN WFQ_Extended_Status = 'WFQ_INSUFFICIENT' THEN 1 ELSE 0 END) AS Unresolved_Stockouts,
    SUM(CASE WHEN WFQ_Extended_Status = 'WFQ_COVERED' THEN 1 ELSE 0 END) AS WFQ_Resolved,
    SUM(Ledger_WFQ_Influx) AS Total_WFQ_Applied
FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]
GROUP BY ITEMNMBR
HAVING SUM(CASE WHEN WFQ_Extended_Status = 'WFQ_INSUFFICIENT' THEN 1 ELSE 0 END) > 0
ORDER BY Unresolved_Stockouts DESC;
```

### Example 3: Extended Balance Projection
```sql
SELECT 
    ITEMNMBR,
    DUEDATE,
    CAST(Running_Balance AS decimal(18,6)) AS Base_Balance,
    Ledger_WFQ_Influx,
    Ledger_Extended_Balance,
    CASE 
        WHEN Ledger_Extended_Balance > 0 THEN 'POSITIVE'
        WHEN Ledger_Extended_Balance = 0 THEN 'ZERO'
        ELSE 'NEGATIVE'
    END AS Balance_Status
FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]
WHERE ITEMNMBR = '10.12345'  -- Replace with actual item
ORDER BY DUEDATE;
```

## Performance Considerations

### Indexing Recommendations

**ETB_WC_INV_Unified:**
```sql
CREATE NONCLUSTERED INDEX IX_ETB_WC_INV_Unified_Item_Date 
ON dbo.ETB_WC_INV_Unified (ITEMNMBR, DUEDATE) 
INCLUDE (Net_Demand, Running_Balance, WCID_From_MO);
```

**ETB_WFQ_PIPE:**
```sql
CREATE NONCLUSTERED INDEX IX_ETB_WFQ_PIPE_Item_Release 
ON dbo.ETB_WFQ_PIPE (Item_Number, Estimated_Release_Date) 
INCLUDE (SITE, QTY_ON_HAND, View_Level);
```

### Query Optimization
- View uses window functions efficiently (single partition pass)
- LEFT JOINs preserve all demand rows
- Aggregations are pre-filtered (View_Level, QTY_ON_HAND > 0)
- TRY_CAST prevents conversion errors

## Business Rules

### WFQ Allocation Logic
1. **Stockout Detection**: Balance must reach <= 0 before WFQ applies
2. **Time Constraint**: WFQ release date must be <= demand due date
3. **Site Matching**: WFQ site should match demand work center (or be null/empty)
4. **Cumulative Application**: All eligible WFQ supply is summed per demand row

### Non-Destructive Overlay
- Base [`Running_Balance`](ETB_WC_INV_Unified:253) is never modified
- WFQ is additive via `Ledger_WFQ_Influx`
- Original 31 columns maintain exact order and values

## Maintenance & Monitoring

### Health Check Query
```sql
SELECT 
    COUNT(*) AS Total_Rows,
    COUNT(DISTINCT ITEMNMBR) AS Unique_Items,
    SUM(CASE WHEN Ledger_WFQ_Influx > 0 THEN 1 ELSE 0 END) AS Rows_With_WFQ,
    SUM(Ledger_WFQ_Influx) AS Total_WFQ_Applied,
    AVG(CAST(Ledger_Extended_Balance AS decimal(18,6))) AS Avg_Extended_Balance
FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32];
```

### Status Distribution
```sql
SELECT 
    WFQ_Extended_Status,
    COUNT(*) AS Row_Count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS decimal(5,2)) AS Percentage
FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]
GROUP BY WFQ_Extended_Status
ORDER BY Row_Count DESC;
```

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 3.2 | 2026-02-09 | Initial WFQ extension implementation |

## Dependencies

- **Base View**: [`dbo.ETB_WC_INV_Unified`](ETB_WC_INV_Unified)
- **Supply View**: [`dbo.ETB_WFQ_PIPE`](ETB_WFQ_PIPE)
- **SQL Server Version**: 2016+ (for TRY_CAST)

## Contact & Support

For questions or issues with this view, contact the SQL Orchestration team or reference the Sovereign Schedule Deduction Engine documentation.

---

**Last Updated**: 2026-02-09  
**Document Version**: 1.0
