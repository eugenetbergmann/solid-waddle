-- ============================================================================
-- VIEW: ETB_WC_INV_Unified_WFQ_Extended_v32
-- ============================================================================
-- Sovereign Schedule Deduction Engine v3.2 - WFQ Pipeline Extension
--
-- PURPOSE:
--   Extends the immutable PAB ledger (ETB_WC_INV_Unified) by projecting 
--   AVAILABLE BALANCE forward per demand row. When balance stocks out 
--   (Running_Balance <= 0), applies WFQ pipeline supply based on projected 
--   release dates (Estimated_Release_Date <= DUEDATE).
--
-- ARCHITECTURE:
--   - Base ledger: ETB_WC_INV_Unified (fully immutable)
--   - Stockout detection: Running_Balance <= 0 per ITEMNMBR
--   - WFQ overlay: Additive, non-destructive coverage
--   - New columns: Ledger_WFQ_Influx, Ledger_Extended_Balance, WFQ_Extended_Status
--
-- DATA SOURCES:
--   - Demand + Ledger: dbo.ETB_WC_INV_Unified
--   - WFQ Supply: dbo.ETB_WFQ_PIPE (View_Level='ITEM_LEVEL', QTY_ON_HAND > 0)
--
-- AUTHOR: SQL Orchestration Engine
-- DATE: 2026-02-09
-- VERSION: 3.2
-- ============================================================================

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
