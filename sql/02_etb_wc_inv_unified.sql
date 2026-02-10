-- ============================================================================
-- VIEW: ETB_WC_INV_Unified
-- ============================================================================
-- Role: Inventory netting + demand adjustment
-- Dependencies: ETB_PAB_AUTO, Prosenthal_INV_BIN_QTY_wQTYTYPE
-- Status: PRODUCTION CODE - Deploy this view
-- ============================================================================
-- Selects from View 1, joins to warehouse inventory (WC-W%, <=45 days),
-- calculates Net_Demand, applies suppression rules.
-- EMBEDDED BegBal: Re-parse with ISNUMERIC logic (defense in depth)
-- ============================================================================

CREATE VIEW [dbo].[ETB_WC_INV_Unified]
AS

WITH RawData AS (
    -- Select from View 1 with embedded BegBal re-validation
    SELECT
        ITEMNMBR,
        ItemDescription,
        UOM,
        ORDERNUMBER,
        Construct,
        DUEDATE,
        [Expiry Dates],
        [Date + Expiry],
        -- EMBEDDED BegBal: Re-parse with ISNUMERIC logic (defense in depth)
        CAST(
            CASE 
                WHEN ISNUMERIC(LTRIM(RTRIM(BEG_BAL))) = 1 
                    THEN CAST(LTRIM(RTRIM(BEG_BAL)) AS decimal(18, 6))
                ELSE 0 
            END AS VARCHAR(50)
        ) AS BEG_BAL,
        Deductions,
        Expiry,
        [PO's],
        Running_Balance,
        MRP_IssueDate,
        WCID_From_MO,
        Issued,
        Original_Required,
        Has_Issued,
        IssueDate_Mismatch,
        Early_Issue_Flag,
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
    FROM dbo.ETB_PAB_AUTO
),

InventoryJoin AS (
    -- Join to warehouse inventory (SITE LIKE 'WC-W%', days <= 45)
    SELECT
        r.*,
        inv.QTYTYPE,
        inv.BIN,
        inv.QTY AS Inventory_Qty_Available,
        inv.SITE AS Inventory_Site,
        inv.LOCNCODE AS Inventory_Location,
        -- Calculate days from today to due date
        DATEDIFF(DAY, CAST(GETDATE() AS DATE), r.DUEDATE) AS Days_To_Due
    FROM RawData r
    LEFT JOIN dbo.Prosenthal_INV_BIN_QTY_wQTYTYPE inv
        ON r.ITEMNMBR = inv.ITEMNMBR
        AND inv.SITE LIKE 'WC-W%'
        AND DATEDIFF(DAY, CAST(GETDATE() AS DATE), r.DUEDATE) <= 45
        AND inv.QTY > 0
),

DemandCalculation AS (
    -- Calculate Net_Demand and suppression flags
    SELECT
        *,
        -- Original Required quantity
        Original_Required,
        -- Net_Demand: Demand - Inv_Qty if (Inv > 0 AND Inv < Demand) else Demand
        CASE
            WHEN Inventory_Qty_Available IS NULL THEN Original_Required
            WHEN Inventory_Qty_Available <= 0 THEN Original_Required
            WHEN Inventory_Qty_Available >= Original_Required THEN 0
            ELSE Original_Required - Inventory_Qty_Available
        END AS Net_Demand,
        -- Suppression Rule 1: Stale & Unissued (DUEDATE <= TODAY-7 AND Issued = 0)
        CASE
            WHEN DUEDATE <= DATEADD(DAY, -7, CAST(GETDATE() AS DATE))
                 AND (Issued = 0 OR Issued IS NULL)
            THEN 1
            ELSE 0
        END AS Suppress_Stale,
        -- Suppression Rule 2: Full Coverage in Fence (DUEDATE <= TODAY+7 AND Inv_Qty >= Demand)
        CASE
            WHEN DUEDATE <= DATEADD(DAY, 7, CAST(GETDATE() AS DATE))
                 AND Inventory_Qty_Available >= Original_Required
                 AND Inventory_Qty_Available > 0
            THEN 1
            ELSE 0
        END AS Suppress_Fence
    FROM InventoryJoin
),

SuppressionStatus AS (
    -- Determine final suppression status
    SELECT
        *,
        CASE
            WHEN Suppress_Stale = 1 THEN 'SUPPRESSED: Stale & Unissued'
            WHEN Suppress_Fence = 1 THEN 'SUPPRESSED: Full Coverage in Fence'
            WHEN Net_Demand < Original_Required AND Net_Demand > 0 THEN 'PARTIAL: Partial Coverage'
            WHEN Net_Demand = 0 THEN 'COVERED: Full Inventory Coverage'
            ELSE 'ACTIVE: No Suppression'
        END AS Suppression_Status
    FROM DemandCalculation
)

-- Final output: Exclude suppressed rows
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
FROM SuppressionStatus
WHERE Suppress_Stale = 0
  AND Suppress_Fence = 0;

GO
