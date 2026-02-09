# ETB_WC_INV_Unified_WFQ_Extended_v32

## Sovereign Schedule Deduction Engine v3.2 - WFQ Pipeline Extension

### 📋 Overview

This repository contains the **orchestration-grade SQL view** that extends the Sovereign Schedule Deduction Engine by integrating WFQ (Work-in-Process/Quarantine) pipeline supply into the Projected Available Balance (PAB) ledger. The view intelligently applies WFQ coverage **only after stockout detection** and when supply release dates align with demand due dates.

**Environment:** Microsoft Dynamics GP SQL Server  
**Version:** 3.2  
**Date:** 2026-02-09  

---

## 🎯 Key Features

### ✅ Non-Destructive Extension
- Base ledger ([`ETB_WC_INV_Unified`](ETB_WC_INV_Unified)) remains **fully immutable**
- All 31 original columns preserved in exact order
- WFQ coverage is **additive**, not substitutive

### ✅ Intelligent Stockout Detection
- Monitors `Running_Balance` per item across demand sequence
- Identifies first stockout point (`Running_Balance <= 0`)
- Applies WFQ supply **only post-stockout**

### ✅ Time-Aware Supply Allocation
- WFQ supply matched by `Estimated_Release_Date <= DUEDATE`
- Site-aware matching (`WCID_From_MO = SITE`)
- Prevents premature or late supply application

### ✅ Comprehensive Status Tracking
- **PAB_SUFFICIENT**: Base balance covers all demand
- **PRE_STOCKOUT**: Demand row before stockout point
- **WFQ_COVERED**: Stockout resolved by WFQ supply
- **WFQ_INSUFFICIENT**: Stockout persists despite WFQ

---

## 📁 Repository Contents

| File | Description |
|------|-------------|
| [`ETB_WC_INV_Unified_WFQ_Extended_v32.sql`](ETB_WC_INV_Unified_WFQ_Extended_v32.sql) | **Main view definition** - Production-ready SQL view |
| [`ETB_WC_INV_Unified_WFQ_Extended_v32_DOCUMENTATION.md`](ETB_WC_INV_Unified_WFQ_Extended_v32_DOCUMENTATION.md) | **Technical documentation** - Architecture, schema, usage examples |
| [`ETB_WC_INV_Unified_WFQ_Extended_v32_DEPLOYMENT.sql`](ETB_WC_INV_Unified_WFQ_Extended_v32_DEPLOYMENT.sql) | **Deployment script** - Automated deployment with validation |
| [`ETB_WC_INV_Unified_WFQ_Extended_v32_VALIDATION.sql`](ETB_WC_INV_Unified_WFQ_Extended_v32_VALIDATION.sql) | **Validation suite** - 10 comprehensive test queries |
| [`ETB_WC_INV_Unified`](ETB_WC_INV_Unified) | **Base ledger view** - Immutable demand ledger source |
| [`ETB_WFQ_PIPE`](ETB_WFQ_PIPE) | **WFQ supply view** - Pipeline supply source |

---

## 🚀 Quick Start

### Prerequisites
- Microsoft SQL Server 2016+ (requires `TRY_CAST`)
- Existing views: `dbo.ETB_WC_INV_Unified`, `dbo.ETB_WFQ_PIPE`
- Appropriate database permissions

### Deployment Steps

1. **Review Dependencies**
   ```sql
   -- Verify base views exist
   SELECT name FROM sys.views 
   WHERE name IN ('ETB_WC_INV_Unified', 'ETB_WFQ_PIPE');
   ```

2. **Execute Deployment Script**
   ```sql
   -- Update database name in script
   USE [YOUR_DATABASE_NAME];
   GO
   
   -- Run deployment
   :r ETB_WC_INV_Unified_WFQ_Extended_v32_DEPLOYMENT.sql
   ```

3. **Run Validation Suite**
   ```sql
   -- Execute all validation tests
   :r ETB_WC_INV_Unified_WFQ_Extended_v32_VALIDATION.sql
   ```

4. **Verify Results**
   ```sql
   -- Quick health check
   SELECT 
       COUNT(*) AS Total_Rows,
       SUM(CASE WHEN Ledger_WFQ_Influx > 0 THEN 1 ELSE 0 END) AS WFQ_Applied_Rows,
       COUNT(DISTINCT ITEMNMBR) AS Unique_Items
   FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32];
   ```

---

## 📊 Schema Overview

### Input Sources

#### ETB_WC_INV_Unified (Base Ledger)
- **Purpose**: Demand ledger with PAB calculations
- **Key Fields**: `ITEMNMBR`, `DUEDATE`, `Running_Balance`, `Net_Demand`, `WCID_From_MO`
- **Filter**: `Net_Demand > 0` (active demand only)

#### ETB_WFQ_PIPE (WFQ Supply)
- **Purpose**: WFQ pipeline supply projection
- **Key Fields**: `Item_Number`, `SITE`, `QTY_ON_HAND`, `Estimated_Release_Date`
- **Filter**: `View_Level = 'ITEM_LEVEL'` AND `QTY_ON_HAND > 0`

### Output Schema

**Base Columns (31)**: All columns from [`ETB_WC_INV_Unified`](ETB_WC_INV_Unified) unchanged

**WFQ Extension Columns (3)**:
- `Ledger_WFQ_Influx` (decimal): WFQ supply allocated to this demand row
- `Ledger_Extended_Balance` (decimal): `Running_Balance + Ledger_WFQ_Influx`
- `WFQ_Extended_Status` (varchar): Coverage status indicator

---

## 💡 Usage Examples

### Example 1: Identify WFQ-Covered Demand
```sql
SELECT 
    ITEMNMBR,
    ORDERNUMBER,
    DUEDATE,
    CAST(Running_Balance AS decimal(18,6)) AS Base_Balance,
    Ledger_WFQ_Influx,
    Ledger_Extended_Balance,
    WFQ_Extended_Status
FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]
WHERE WFQ_Extended_Status = 'WFQ_COVERED'
ORDER BY ITEMNMBR, DUEDATE;
```

### Example 2: Stockout Analysis by Item
```sql
SELECT 
    ITEMNMBR,
    ItemDescription,
    COUNT(*) AS Total_Demand_Rows,
    SUM(CASE WHEN WFQ_Extended_Status = 'WFQ_INSUFFICIENT' THEN 1 ELSE 0 END) AS Unresolved_Stockouts,
    SUM(CASE WHEN WFQ_Extended_Status = 'WFQ_COVERED' THEN 1 ELSE 0 END) AS WFQ_Resolved,
    SUM(Ledger_WFQ_Influx) AS Total_WFQ_Applied,
    MIN(Ledger_Extended_Balance) AS Min_Extended_Balance
FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]
GROUP BY ITEMNMBR, ItemDescription
HAVING SUM(CASE WHEN WFQ_Extended_Status = 'WFQ_INSUFFICIENT' THEN 1 ELSE 0 END) > 0
ORDER BY Unresolved_Stockouts DESC;
```

### Example 3: Extended Balance Projection
```sql
SELECT 
    ITEMNMBR,
    ORDERNUMBER,
    DUEDATE,
    CAST(Running_Balance AS decimal(18,6)) AS Base_Balance,
    Ledger_WFQ_Influx,
    Ledger_Extended_Balance,
    CASE 
        WHEN Ledger_Extended_Balance > 0 THEN 'POSITIVE'
        WHEN Ledger_Extended_Balance = 0 THEN 'ZERO'
        ELSE 'NEGATIVE'
    END AS Balance_Status,
    WFQ_Extended_Status
FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]
WHERE ITEMNMBR = '10.12345'  -- Replace with actual item
ORDER BY DUEDATE;
```

### Example 4: WFQ Coverage Summary
```sql
SELECT 
    WFQ_Extended_Status,
    COUNT(*) AS Row_Count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS decimal(5,2)) AS Percentage,
    COUNT(DISTINCT ITEMNMBR) AS Unique_Items,
    SUM(Ledger_WFQ_Influx) AS Total_WFQ_Applied
FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]
GROUP BY WFQ_Extended_Status
ORDER BY Row_Count DESC;
```

---

## 🏗️ Architecture

### CTE Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Demand_Ledger                                            │
│    Load base ledger (Net_Demand > 0)                        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Demand_Seq                                               │
│    Sequence demand by ITEMNMBR, DUEDATE                     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Stockout_Detection                                       │
│    Identify first Running_Balance <= 0 per item             │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. WFQ_Supply                                               │
│    Aggregate WFQ pipeline (ITEM_LEVEL, QTY > 0)            │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. WFQ_Allocated                                            │
│    Allocate WFQ post-stockout, time-constrained             │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. Extended_Ledger                                          │
│    Merge base + WFQ, calculate extended balance & status    │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ Final SELECT                                                │
│    31 base columns + 3 WFQ extension columns                │
└─────────────────────────────────────────────────────────────┘
```

### Allocation Rules

WFQ supply is allocated to a demand row **only when ALL conditions are met**:

1. ✅ `Demand_Seq >= Stockout_Seq` (post-stockout)
2. ✅ `Estimated_Release_Date <= DUEDATE` (available in time)
3. ✅ `WCID_From_MO = SITE` (site match, or null/empty)

---

## 🔍 Validation & Testing

The validation suite includes **10 comprehensive tests**:

1. **Basic Row Count & Column Validation** - Verify immutability
2. **WFQ Extension Column Validation** - Check new columns
3. **WFQ Status Distribution** - Analyze status breakdown
4. **Stockout Detection Validation** - Verify stockout logic
5. **Time-Based WFQ Allocation Validation** - Confirm time constraints
6. **Base Ledger Immutability Check** - Ensure no modifications
7. **Sample Item Deep Dive** - Detailed item analysis
8. **WFQ Supply Source Validation** - Compare available vs. applied
9. **Performance Metrics** - Execution time and IO stats
10. **Edge Case Validation** - NULL checks, negative values, invalid statuses

**Run all tests:**
```sql
:r ETB_WC_INV_Unified_WFQ_Extended_v32_VALIDATION.sql
```

---

## ⚡ Performance Considerations

### Recommended Indexes

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

### Optimization Notes
- Window functions use single partition pass
- LEFT JOINs preserve all demand rows
- Pre-filtered aggregations (View_Level, QTY_ON_HAND > 0)
- TRY_CAST prevents conversion errors

---

## 📈 Monitoring & Health Checks

### Daily Health Check
```sql
SELECT 
    CAST(GETDATE() AS date) AS Check_Date,
    COUNT(*) AS Total_Rows,
    COUNT(DISTINCT ITEMNMBR) AS Unique_Items,
    SUM(CASE WHEN Ledger_WFQ_Influx > 0 THEN 1 ELSE 0 END) AS WFQ_Applied_Rows,
    SUM(Ledger_WFQ_Influx) AS Total_WFQ_Applied,
    AVG(Ledger_Extended_Balance) AS Avg_Extended_Balance
FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32];
```

### Status Distribution Monitor
```sql
SELECT 
    WFQ_Extended_Status,
    COUNT(*) AS Row_Count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS decimal(5,2)) AS Percentage
FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]
GROUP BY WFQ_Extended_Status
ORDER BY Row_Count DESC;
```

---

## 🛠️ Troubleshooting

### Issue: No WFQ Applied
**Symptoms**: `Ledger_WFQ_Influx = 0` for all rows

**Checks**:
1. Verify WFQ supply exists:
   ```sql
   SELECT COUNT(*) FROM dbo.ETB_WFQ_PIPE 
   WHERE View_Level = 'ITEM_LEVEL' AND QTY_ON_HAND > 0;
   ```
2. Check for stockouts:
   ```sql
   SELECT COUNT(*) FROM dbo.ETB_WC_INV_Unified 
   WHERE CAST(Running_Balance AS decimal(18,6)) <= 0;
   ```
3. Verify time alignment:
   ```sql
   SELECT * FROM dbo.ETB_WFQ_PIPE 
   WHERE Estimated_Release_Date > GETDATE() + 365;
   ```

### Issue: Unexpected Status Values
**Symptoms**: `WFQ_Extended_Status = 'UNKNOWN'`

**Resolution**: Check for NULL values in key fields:
```sql
SELECT * FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]
WHERE WFQ_Extended_Status = 'UNKNOWN';
```

### Issue: Performance Degradation
**Symptoms**: Slow query execution

**Resolution**:
1. Verify indexes exist (see Performance Considerations)
2. Update statistics:
   ```sql
   UPDATE STATISTICS dbo.ETB_WC_INV_Unified;
   UPDATE STATISTICS dbo.ETB_WFQ_PIPE;
   ```
3. Review execution plan for table scans

---

## 📝 Change Log

| Version | Date | Changes |
|---------|------|---------|
| 3.2 | 2026-02-09 | Initial WFQ extension implementation |

---

## 🤝 Support & Contribution

### Documentation
- **Technical Docs**: [`ETB_WC_INV_Unified_WFQ_Extended_v32_DOCUMENTATION.md`](ETB_WC_INV_Unified_WFQ_Extended_v32_DOCUMENTATION.md)
- **Deployment Guide**: [`ETB_WC_INV_Unified_WFQ_Extended_v32_DEPLOYMENT.sql`](ETB_WC_INV_Unified_WFQ_Extended_v32_DEPLOYMENT.sql)
- **Validation Suite**: [`ETB_WC_INV_Unified_WFQ_Extended_v32_VALIDATION.sql`](ETB_WC_INV_Unified_WFQ_Extended_v32_VALIDATION.sql)

### Contact
For questions or issues, contact the SQL Orchestration team or reference the Sovereign Schedule Deduction Engine documentation.

---

## 📜 License & Compliance

This view is designed for Microsoft Dynamics GP environments and follows enterprise SQL Server best practices. Ensure compliance with your organization's database governance policies before deployment.

---

**Last Updated**: 2026-02-09  
**Version**: 3.2  
**Status**: Production Ready ✅
