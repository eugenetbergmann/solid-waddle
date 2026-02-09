-- ============================================================================
-- VALIDATION & TESTING QUERIES
-- View: ETB_WC_INV_Unified_WFQ_Extended_v32
-- ============================================================================
-- Purpose: Comprehensive validation and testing suite for WFQ extension
-- Version: 3.2
-- Date: 2026-02-09
-- ============================================================================

USE [YOUR_DATABASE_NAME];  -- Replace with actual database name
GO

PRINT '============================================================================';
PRINT 'VALIDATION SUITE: ETB_WC_INV_Unified_WFQ_Extended_v32';
PRINT '============================================================================';
PRINT '';

-- ============================================================================
-- TEST 1: Basic Row Count & Column Validation
-- ============================================================================

PRINT '----------------------------------------------------------------------------';
PRINT 'TEST 1: Basic Row Count & Column Validation';
PRINT '----------------------------------------------------------------------------';
PRINT '';

SELECT 
    'Base View' AS Source,
    COUNT(*) AS Row_Count,
    COUNT(DISTINCT ITEMNMBR) AS Unique_Items
FROM dbo.ETB_WC_INV_Unified
WHERE Net_Demand > 0

UNION ALL

SELECT 
    'Extended View' AS Source,
    COUNT(*) AS Row_Count,
    COUNT(DISTINCT ITEMNMBR) AS Unique_Items
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32;

PRINT '';
PRINT 'Expected: Row counts should match (immutable base ledger)';
PRINT '';

-- ============================================================================
-- TEST 2: WFQ Extension Column Validation
-- ============================================================================

PRINT '----------------------------------------------------------------------------';
PRINT 'TEST 2: WFQ Extension Column Validation';
PRINT '----------------------------------------------------------------------------';
PRINT '';

SELECT 
    'Ledger_WFQ_Influx' AS Column_Name,
    COUNT(*) AS Total_Rows,
    SUM(CASE WHEN Ledger_WFQ_Influx IS NULL THEN 1 ELSE 0 END) AS Null_Count,
    SUM(CASE WHEN Ledger_WFQ_Influx > 0 THEN 1 ELSE 0 END) AS Positive_Count,
    SUM(Ledger_WFQ_Influx) AS Total_WFQ_Applied,
    MIN(Ledger_WFQ_Influx) AS Min_Value,
    MAX(Ledger_WFQ_Influx) AS Max_Value,
    AVG(Ledger_WFQ_Influx) AS Avg_Value
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32

UNION ALL

SELECT 
    'Ledger_Extended_Balance' AS Column_Name,
    COUNT(*) AS Total_Rows,
    SUM(CASE WHEN Ledger_Extended_Balance IS NULL THEN 1 ELSE 0 END) AS Null_Count,
    SUM(CASE WHEN Ledger_Extended_Balance > 0 THEN 1 ELSE 0 END) AS Positive_Count,
    NULL AS Total_WFQ_Applied,
    MIN(Ledger_Extended_Balance) AS Min_Value,
    MAX(Ledger_Extended_Balance) AS Max_Value,
    AVG(Ledger_Extended_Balance) AS Avg_Value
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32;

PRINT '';
PRINT 'Expected: No NULL values, positive counts indicate WFQ application';
PRINT '';

-- ============================================================================
-- TEST 3: WFQ Status Distribution
-- ============================================================================

PRINT '----------------------------------------------------------------------------';
PRINT 'TEST 3: WFQ Status Distribution';
PRINT '----------------------------------------------------------------------------';
PRINT '';

SELECT 
    WFQ_Extended_Status,
    COUNT(*) AS Row_Count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS decimal(5,2)) AS Percentage,
    COUNT(DISTINCT ITEMNMBR) AS Unique_Items,
    SUM(Ledger_WFQ_Influx) AS Total_WFQ_Applied
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32
GROUP BY WFQ_Extended_Status
ORDER BY Row_Count DESC;

PRINT '';
PRINT 'Expected: Valid status values (PAB_SUFFICIENT, PRE_STOCKOUT, WFQ_COVERED, WFQ_INSUFFICIENT)';
PRINT '';

-- ============================================================================
-- TEST 4: Stockout Detection Validation
-- ============================================================================

PRINT '----------------------------------------------------------------------------';
PRINT 'TEST 4: Stockout Detection Validation';
PRINT '----------------------------------------------------------------------------';
PRINT '';

-- Items with stockouts and WFQ coverage
SELECT TOP 10
    ITEMNMBR,
    COUNT(*) AS Total_Demand_Rows,
    SUM(CASE WHEN WFQ_Extended_Status = 'PRE_STOCKOUT' THEN 1 ELSE 0 END) AS Pre_Stockout_Rows,
    SUM(CASE WHEN WFQ_Extended_Status = 'WFQ_COVERED' THEN 1 ELSE 0 END) AS WFQ_Covered_Rows,
    SUM(CASE WHEN WFQ_Extended_Status = 'WFQ_INSUFFICIENT' THEN 1 ELSE 0 END) AS WFQ_Insufficient_Rows,
    SUM(Ledger_WFQ_Influx) AS Total_WFQ_Applied,
    MIN(CAST(Running_Balance AS decimal(18,6))) AS Min_Base_Balance,
    MIN(Ledger_Extended_Balance) AS Min_Extended_Balance
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32
GROUP BY ITEMNMBR
HAVING SUM(CASE WHEN WFQ_Extended_Status IN ('WFQ_COVERED', 'WFQ_INSUFFICIENT') THEN 1 ELSE 0 END) > 0
ORDER BY Total_WFQ_Applied DESC;

PRINT '';
PRINT 'Expected: Items with stockouts show WFQ application';
PRINT '';

-- ============================================================================
-- TEST 5: Time-Based WFQ Allocation Validation
-- ============================================================================

PRINT '----------------------------------------------------------------------------';
PRINT 'TEST 5: Time-Based WFQ Allocation Validation';
PRINT '----------------------------------------------------------------------------';
PRINT '';

-- Verify WFQ is only applied when release date <= due date
WITH WFQ_Detail AS (
    SELECT 
        v.ITEMNMBR,
        v.DUEDATE,
        v.Ledger_WFQ_Influx,
        w.Estimated_Release_Date,
        w.WFQ_Qty,
        CASE 
            WHEN w.Estimated_Release_Date <= v.DUEDATE THEN 'VALID'
            WHEN w.Estimated_Release_Date > v.DUEDATE THEN 'INVALID'
            ELSE 'NO_WFQ'
        END AS Time_Validation
    FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32 v
    LEFT JOIN (
        SELECT 
            TRIM(Item_Number) AS ITEMNMBR,
            Estimated_Release_Date,
            SUM(QTY_ON_HAND) AS WFQ_Qty
        FROM dbo.ETB_WFQ_PIPE
        WHERE View_Level = 'ITEM_LEVEL' AND QTY_ON_HAND > 0
        GROUP BY TRIM(Item_Number), Estimated_Release_Date
    ) w ON v.ITEMNMBR = w.ITEMNMBR
    WHERE v.Ledger_WFQ_Influx > 0
)
SELECT 
    Time_Validation,
    COUNT(*) AS Row_Count,
    SUM(Ledger_WFQ_Influx) AS Total_WFQ
FROM WFQ_Detail
GROUP BY Time_Validation;

PRINT '';
PRINT 'Expected: All WFQ allocations should be VALID (release date <= due date)';
PRINT '';

-- ============================================================================
-- TEST 6: Base Ledger Immutability Check
-- ============================================================================

PRINT '----------------------------------------------------------------------------';
PRINT 'TEST 6: Base Ledger Immutability Check';
PRINT '----------------------------------------------------------------------------';
PRINT '';

-- Verify base columns are unchanged
SELECT 
    'Immutability Check' AS Test,
    COUNT(*) AS Total_Rows,
    SUM(CASE 
        WHEN b.ITEMNMBR = e.ITEMNMBR 
         AND b.ORDERNUMBER = e.ORDERNUMBER
         AND b.DUEDATE = e.DUEDATE
         AND b.Running_Balance = e.Running_Balance
         AND b.Net_Demand = e.Net_Demand
        THEN 1 ELSE 0 
    END) AS Matching_Rows,
    SUM(CASE 
        WHEN b.ITEMNMBR <> e.ITEMNMBR 
          OR b.ORDERNUMBER <> e.ORDERNUMBER
          OR b.DUEDATE <> e.DUEDATE
          OR b.Running_Balance <> e.Running_Balance
          OR b.Net_Demand <> e.Net_Demand
        THEN 1 ELSE 0 
    END) AS Mismatched_Rows
FROM dbo.ETB_WC_INV_Unified b
INNER JOIN dbo.ETB_WC_INV_Unified_WFQ_Extended_v32 e
    ON b.ITEMNMBR = e.ITEMNMBR 
   AND b.ORDERNUMBER = e.ORDERNUMBER
   AND b.DUEDATE = e.DUEDATE
WHERE b.Net_Demand > 0;

PRINT '';
PRINT 'Expected: Mismatched_Rows = 0 (base ledger unchanged)';
PRINT '';

-- ============================================================================
-- TEST 7: Sample Item Deep Dive
-- ============================================================================

PRINT '----------------------------------------------------------------------------';
PRINT 'TEST 7: Sample Item Deep Dive';
PRINT '----------------------------------------------------------------------------';
PRINT '';

-- Select a sample item with WFQ coverage for detailed analysis
DECLARE @SampleItem VARCHAR(50);

SELECT TOP 1 @SampleItem = ITEMNMBR
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32
WHERE Ledger_WFQ_Influx > 0
ORDER BY Ledger_WFQ_Influx DESC;

IF @SampleItem IS NOT NULL
BEGIN
    PRINT 'Sample Item: ' + @SampleItem;
    PRINT '';
    
    SELECT 
        ITEMNMBR,
        ORDERNUMBER,
        DUEDATE,
        CAST(Running_Balance AS decimal(18,6)) AS Base_Balance,
        Net_Demand,
        Ledger_WFQ_Influx,
        Ledger_Extended_Balance,
        WFQ_Extended_Status,
        CASE 
            WHEN Ledger_Extended_Balance > 0 THEN 'POSITIVE'
            WHEN Ledger_Extended_Balance = 0 THEN 'ZERO'
            ELSE 'NEGATIVE'
        END AS Balance_Status
    FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32
    WHERE ITEMNMBR = @SampleItem
    ORDER BY DUEDATE;
    
    PRINT '';
    PRINT 'Expected: Sequential balance projection with WFQ applied post-stockout';
END
ELSE
BEGIN
    PRINT 'No items with WFQ coverage found';
END

PRINT '';

-- ============================================================================
-- TEST 8: WFQ Supply Source Validation
-- ============================================================================

PRINT '----------------------------------------------------------------------------';
PRINT 'TEST 8: WFQ Supply Source Validation';
PRINT '----------------------------------------------------------------------------';
PRINT '';

-- Compare WFQ supply available vs. applied
SELECT 
    'WFQ Supply Available' AS Metric,
    COUNT(DISTINCT TRIM(Item_Number)) AS Unique_Items,
    SUM(QTY_ON_HAND) AS Total_Qty
FROM dbo.ETB_WFQ_PIPE
WHERE View_Level = 'ITEM_LEVEL' AND QTY_ON_HAND > 0

UNION ALL

SELECT 
    'WFQ Supply Applied' AS Metric,
    COUNT(DISTINCT ITEMNMBR) AS Unique_Items,
    SUM(Ledger_WFQ_Influx) AS Total_Qty
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32
WHERE Ledger_WFQ_Influx > 0;

PRINT '';
PRINT 'Expected: Applied <= Available (not all WFQ may be needed)';
PRINT '';

-- ============================================================================
-- TEST 9: Performance Metrics
-- ============================================================================

PRINT '----------------------------------------------------------------------------';
PRINT 'TEST 9: Performance Metrics';
PRINT '----------------------------------------------------------------------------';
PRINT '';

SET STATISTICS TIME ON;
SET STATISTICS IO ON;

SELECT COUNT(*) AS Row_Count
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32;

SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;

PRINT '';
PRINT 'Review execution time and IO statistics above';
PRINT '';

-- ============================================================================
-- TEST 10: Edge Case Validation
-- ============================================================================

PRINT '----------------------------------------------------------------------------';
PRINT 'TEST 10: Edge Case Validation';
PRINT '----------------------------------------------------------------------------';
PRINT '';

-- Check for unexpected NULL values
SELECT 
    'NULL Check' AS Test,
    SUM(CASE WHEN ITEMNMBR IS NULL THEN 1 ELSE 0 END) AS Null_ITEMNMBR,
    SUM(CASE WHEN DUEDATE IS NULL THEN 1 ELSE 0 END) AS Null_DUEDATE,
    SUM(CASE WHEN Ledger_WFQ_Influx IS NULL THEN 1 ELSE 0 END) AS Null_WFQ_Influx,
    SUM(CASE WHEN Ledger_Extended_Balance IS NULL THEN 1 ELSE 0 END) AS Null_Extended_Balance,
    SUM(CASE WHEN WFQ_Extended_Status IS NULL THEN 1 ELSE 0 END) AS Null_Status
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32;

PRINT '';

-- Check for negative WFQ influx (should not occur)
SELECT 
    'Negative WFQ Check' AS Test,
    COUNT(*) AS Rows_With_Negative_WFQ
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32
WHERE Ledger_WFQ_Influx < 0;

PRINT '';

-- Check for invalid status values
SELECT 
    'Invalid Status Check' AS Test,
    COUNT(*) AS Rows_With_Invalid_Status
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32
WHERE WFQ_Extended_Status NOT IN ('PAB_SUFFICIENT', 'PRE_STOCKOUT', 'WFQ_COVERED', 'WFQ_INSUFFICIENT', 'UNKNOWN');

PRINT '';
PRINT 'Expected: All counts = 0 (no edge case violations)';
PRINT '';

-- ============================================================================
-- VALIDATION SUMMARY
-- ============================================================================

PRINT '============================================================================';
PRINT 'VALIDATION SUMMARY';
PRINT '============================================================================';
PRINT '';

SELECT 
    'Total Rows' AS Metric,
    COUNT(*) AS Value
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32

UNION ALL

SELECT 
    'Unique Items' AS Metric,
    COUNT(DISTINCT ITEMNMBR) AS Value
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32

UNION ALL

SELECT 
    'Rows with WFQ Applied' AS Metric,
    COUNT(*) AS Value
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32
WHERE Ledger_WFQ_Influx > 0

UNION ALL

SELECT 
    'Total WFQ Quantity Applied' AS Metric,
    CAST(SUM(Ledger_WFQ_Influx) AS INT) AS Value
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32

UNION ALL

SELECT 
    'Items with Stockouts' AS Metric,
    COUNT(DISTINCT ITEMNMBR) AS Value
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32
WHERE WFQ_Extended_Status IN ('WFQ_COVERED', 'WFQ_INSUFFICIENT')

UNION ALL

SELECT 
    'Items Fully Covered by WFQ' AS Metric,
    COUNT(DISTINCT ITEMNMBR) AS Value
FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32
WHERE WFQ_Extended_Status = 'WFQ_COVERED'
  AND ITEMNMBR NOT IN (
      SELECT ITEMNMBR 
      FROM dbo.ETB_WC_INV_Unified_WFQ_Extended_v32 
      WHERE WFQ_Extended_Status = 'WFQ_INSUFFICIENT'
  );

PRINT '';
PRINT '============================================================================';
PRINT 'VALIDATION COMPLETE';
PRINT '============================================================================';
PRINT '';
PRINT 'Review all test results above for any anomalies or unexpected values.';
PRINT 'All tests should pass with expected results as noted in each section.';
PRINT '';

GO
