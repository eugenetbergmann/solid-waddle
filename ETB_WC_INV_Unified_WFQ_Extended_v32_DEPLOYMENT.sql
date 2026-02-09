-- ============================================================================
-- DEPLOYMENT SCRIPT: ETB_WC_INV_Unified_WFQ_Extended_v32
-- ============================================================================
-- Sovereign Schedule Deduction Engine v3.2 - WFQ Pipeline Extension
-- Deployment Date: 2026-02-09
-- Target Environment: Microsoft Dynamics GP SQL Server
-- ============================================================================

USE [YOUR_DATABASE_NAME];  -- Replace with actual database name
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- ============================================================================
-- STEP 1: Pre-Deployment Validation
-- ============================================================================

PRINT '============================================================================';
PRINT 'PRE-DEPLOYMENT VALIDATION';
PRINT '============================================================================';
PRINT '';

-- Check if base views exist
PRINT 'Checking base view dependencies...';

IF NOT EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_WC_INV_Unified')
BEGIN
    RAISERROR('ERROR: Base view [dbo].[ETB_WC_INV_Unified] does not exist. Deployment aborted.', 16, 1);
    RETURN;
END
ELSE
    PRINT '  ✓ ETB_WC_INV_Unified exists';

IF NOT EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_WFQ_PIPE')
BEGIN
    RAISERROR('ERROR: Supply view [dbo].[ETB_WFQ_PIPE] does not exist. Deployment aborted.', 16, 1);
    RETURN;
END
ELSE
    PRINT '  ✓ ETB_WFQ_PIPE exists';

PRINT '';
PRINT 'All dependencies validated successfully.';
PRINT '';

-- ============================================================================
-- STEP 2: Backup Existing View (if exists)
-- ============================================================================

PRINT '============================================================================';
PRINT 'BACKUP EXISTING VIEW';
PRINT '============================================================================';
PRINT '';

IF EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_WC_INV_Unified_WFQ_Extended_v32')
BEGIN
    DECLARE @BackupName NVARCHAR(255);
    DECLARE @BackupSQL NVARCHAR(MAX);
    
    SET @BackupName = 'ETB_WC_INV_Unified_WFQ_Extended_v32_BACKUP_' + 
                      CONVERT(VARCHAR(8), GETDATE(), 112) + '_' + 
                      REPLACE(CONVERT(VARCHAR(8), GETDATE(), 108), ':', '');
    
    PRINT 'Existing view found. Creating backup: ' + @BackupName;
    
    -- Get existing view definition
    SELECT @BackupSQL = OBJECT_DEFINITION(OBJECT_ID('dbo.ETB_WC_INV_Unified_WFQ_Extended_v32'));
    
    -- Create backup view
    SET @BackupSQL = REPLACE(@BackupSQL, 
                             'CREATE VIEW [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]',
                             'CREATE VIEW [dbo].[' + @BackupName + ']');
    
    EXEC sp_executesql @BackupSQL;
    
    PRINT '  ✓ Backup created: ' + @BackupName;
    
    -- Drop existing view
    DROP VIEW [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32];
    PRINT '  ✓ Existing view dropped';
END
ELSE
BEGIN
    PRINT 'No existing view found. Proceeding with fresh deployment.';
END

PRINT '';

-- ============================================================================
-- STEP 3: Create View
-- ============================================================================

PRINT '============================================================================';
PRINT 'CREATING VIEW: ETB_WC_INV_Unified_WFQ_Extended_v32';
PRINT '============================================================================';
PRINT '';

GO

CREATE VIEW [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]
AS

WITH Demand_Ledger AS (
    -- Base immutable ledger from ETB_WC_INV_Unified
    -- All upstream columns preserved in original order
    SELECT 
        ITEMNMBR,
        ItemDescription,
        UOM,
        ORDERNUMBER,
        Construct,
        DUEDATE,
        [Expiry Dates],
        [Date + Expiry],
        BEG_BAL,
        Deductions,
        Expiry,
        [PO's],
        Running_Balance,
        MRP_IssueDate,
        WCID_From_MO,
        Issued,
        Original_Required,
        Net_Demand,
        Inventory_Qty_Available,
        Suppression_Status,
        VendorItem,
        PRIME_VNDR,
        PURCHASING_LT,
        PLANNING_LT,
        ORDER_POINT_QTY,
        SAFETY_STOCK,
        FG,
        [FG Desc],
        STSDESCR,
        MRPTYPE,
        Unified_Value
    FROM dbo.ETB_WC_INV_Unified
    WHERE Net_Demand > 0  -- Only process active demand rows
),

Demand_Seq AS (
    -- Sequence demand rows per item by due date for stockout detection
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ITEMNMBR 
            ORDER BY DUEDATE, ORDERNUMBER
        ) AS Demand_Seq
    FROM Demand_Ledger
),

Stockout_Detection AS (
    -- Identify the first demand sequence where balance goes negative
    -- This is the trigger point for WFQ supply application
    SELECT 
        *,
        MIN(
            CASE 
                WHEN TRY_CAST(Running_Balance AS decimal(18,6)) <= 0 
                THEN Demand_Seq 
                ELSE NULL 
            END
        ) OVER (PARTITION BY ITEMNMBR) AS Stockout_Seq
    FROM Demand_Seq
),

WFQ_Supply AS (
    -- Aggregate WFQ pipeline supply by item, site, and release date
    -- Only include valid supply (QTY_ON_HAND > 0, ITEM_LEVEL view)
    SELECT 
        TRIM(Item_Number) AS ITEMNMBR,
        TRIM(SITE) AS SITE,
        Estimated_Release_Date,
        SUM(QTY_ON_HAND) AS WFQ_Qty
    FROM dbo.ETB_WFQ_PIPE
    WHERE View_Level = 'ITEM_LEVEL' 
      AND QTY_ON_HAND > 0
    GROUP BY 
        TRIM(Item_Number), 
        TRIM(SITE), 
        Estimated_Release_Date
),

WFQ_Allocated AS (
    -- Allocate WFQ supply to demand rows after stockout
    -- Supply is only applied when:
    --   1. Demand sequence >= Stockout sequence (post-stockout)
    --   2. WFQ release date <= Demand due date (available in time)
    SELECT 
        d.ITEMNMBR,
        d.Demand_Seq,
        d.DUEDATE,
        d.WCID_From_MO,
        TRY_CAST(d.Running_Balance AS decimal(18,6)) AS Ledger_Base_Balance,
        ISNULL(
            SUM(
                CASE 
                    WHEN d.Demand_Seq >= d.Stockout_Seq 
                         AND w.Estimated_Release_Date <= d.DUEDATE
                         AND (d.WCID_From_MO = w.SITE OR d.WCID_From_MO IS NULL OR d.WCID_From_MO = '')
                    THEN w.WFQ_Qty
                    ELSE 0
                END
            ), 0
        ) AS Ledger_WFQ_Influx
    FROM Stockout_Detection d
    LEFT JOIN WFQ_Supply w
        ON d.ITEMNMBR = w.ITEMNMBR
    GROUP BY 
        d.ITEMNMBR, 
        d.Demand_Seq, 
        d.DUEDATE,
        d.WCID_From_MO,
        d.Running_Balance,
        d.Stockout_Seq
),

Extended_Ledger AS (
    -- Merge base ledger with WFQ allocation
    -- Calculate extended balance and status
    SELECT 
        d.*,
        ISNULL(w.Ledger_WFQ_Influx, 0) AS Ledger_WFQ_Influx,
        ISNULL(w.Ledger_Base_Balance, 0) + ISNULL(w.Ledger_WFQ_Influx, 0) AS Ledger_Extended_Balance,
        CASE 
            WHEN d.Stockout_Seq IS NULL THEN 'PAB_SUFFICIENT'
            WHEN d.Demand_Seq < d.Stockout_Seq THEN 'PRE_STOCKOUT'
            WHEN d.Demand_Seq >= d.Stockout_Seq AND ISNULL(w.Ledger_WFQ_Influx, 0) > 0 THEN 'WFQ_COVERED'
            WHEN d.Demand_Seq >= d.Stockout_Seq AND ISNULL(w.Ledger_WFQ_Influx, 0) = 0 THEN 'WFQ_INSUFFICIENT'
            ELSE 'UNKNOWN'
        END AS WFQ_Extended_Status
    FROM Stockout_Detection d
    LEFT JOIN WFQ_Allocated w
        ON d.ITEMNMBR = w.ITEMNMBR
        AND d.Demand_Seq = w.Demand_Seq
)

-- Final projection: All base columns + WFQ extension columns
SELECT 
    ITEMNMBR,
    ItemDescription,
    UOM,
    ORDERNUMBER,
    Construct,
    DUEDATE,
    [Expiry Dates],
    [Date + Expiry],
    BEG_BAL,
    Deductions,
    Expiry,
    [PO's],
    Running_Balance,
    MRP_IssueDate,
    WCID_From_MO,
    Issued,
    Original_Required,
    Net_Demand,
    Inventory_Qty_Available,
    Suppression_Status,
    VendorItem,
    PRIME_VNDR,
    PURCHASING_LT,
    PLANNING_LT,
    ORDER_POINT_QTY,
    SAFETY_STOCK,
    FG,
    [FG Desc],
    STSDESCR,
    MRPTYPE,
    Unified_Value,
    -- ========================================
    -- WFQ EXTENSION COLUMNS (v3.2)
    -- ========================================
    Ledger_WFQ_Influx,
    Ledger_Extended_Balance,
    WFQ_Extended_Status
FROM Extended_Ledger;

GO

PRINT '  ✓ View created successfully';
PRINT '';

-- ============================================================================
-- STEP 4: Post-Deployment Validation
-- ============================================================================

PRINT '============================================================================';
PRINT 'POST-DEPLOYMENT VALIDATION';
PRINT '============================================================================';
PRINT '';

-- Verify view exists
IF EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_WC_INV_Unified_WFQ_Extended_v32')
BEGIN
    PRINT '  ✓ View exists in database';
    
    -- Test query execution
    BEGIN TRY
        DECLARE @RowCount INT;
        SELECT @RowCount = COUNT(*) FROM [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32];
        PRINT '  ✓ View is queryable';
        PRINT '  ✓ Total rows: ' + CAST(@RowCount AS VARCHAR(20));
        
        -- Validate new columns exist
        IF EXISTS (
            SELECT 1 
            FROM sys.columns 
            WHERE object_id = OBJECT_ID('dbo.ETB_WC_INV_Unified_WFQ_Extended_v32') 
              AND name IN ('Ledger_WFQ_Influx', 'Ledger_Extended_Balance', 'WFQ_Extended_Status')
        )
        BEGIN
            PRINT '  ✓ WFQ extension columns present';
        END
        ELSE
        BEGIN
            RAISERROR('ERROR: WFQ extension columns not found', 16, 1);
        END
        
    END TRY
    BEGIN CATCH
        PRINT '  ✗ ERROR: View query failed';
        PRINT '    Error Message: ' + ERROR_MESSAGE();
        RAISERROR('Post-deployment validation failed', 16, 1);
    END CATCH
END
ELSE
BEGIN
    RAISERROR('ERROR: View was not created successfully', 16, 1);
END

PRINT '';

-- ============================================================================
-- STEP 5: Grant Permissions (Optional - Adjust as needed)
-- ============================================================================

PRINT '============================================================================';
PRINT 'GRANT PERMISSIONS';
PRINT '============================================================================';
PRINT '';

-- Example: Grant SELECT to specific roles/users
-- Uncomment and modify as needed for your environment

-- GRANT SELECT ON [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32] TO [YourRole];
-- PRINT '  ✓ SELECT permission granted to [YourRole]';

PRINT 'Permissions step skipped (configure manually if needed)';
PRINT '';

-- ============================================================================
-- DEPLOYMENT COMPLETE
-- ============================================================================

PRINT '============================================================================';
PRINT 'DEPLOYMENT COMPLETE';
PRINT '============================================================================';
PRINT '';
PRINT 'View: [dbo].[ETB_WC_INV_Unified_WFQ_Extended_v32]';
PRINT 'Status: Successfully deployed';
PRINT 'Version: 3.2';
PRINT 'Timestamp: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '';
PRINT 'Next Steps:';
PRINT '  1. Review validation results above';
PRINT '  2. Run test queries (see ETB_WC_INV_Unified_WFQ_Extended_v32_VALIDATION.sql)';
PRINT '  3. Configure permissions if needed';
PRINT '  4. Update dependent reports/processes';
PRINT '';
PRINT '============================================================================';

GO
