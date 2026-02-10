-- ============================================================================
-- VIEW: ETB_PAB_AUTO
-- ============================================================================
-- Role: Foundation demand + inventory suppression
-- Dependencies: ETB_PAB_MO, ETB_ActiveDemand_Union_FG_MO, Prosenthal_Vendor_Items,
--               PK010033, WO010032
-- Status: PRODUCTION CODE - Deploy this view
-- ============================================================================
-- Normalizes order/item numbers, joins MO to ActiveDemand to VendorItems,
-- ranks and deduplicates, matches to ledger for issue tracking.
-- EMBEDDED BegBal: ISNUMERIC validation with CAST to decimal(18,6), default 0
-- ============================================================================

CREATE VIEW [dbo].[ETB_PAB_AUTO]
AS

WITH CleanData AS (
    -- Normalize order and item numbers
    SELECT
        LTRIM(RTRIM(mo.ITEMNMBR)) AS ITEMNMBR,
        LTRIM(RTRIM(mo.ITEMDESC)) AS ItemDescription,
        LTRIM(RTRIM(mo.UOFM)) AS UOM,
        LTRIM(RTRIM(mo.ORDERNUMBER)) AS ORDERNUMBER,
        LTRIM(RTRIM(mo.Construct)) AS Construct,
        mo.DUEDATE,
        mo.[Expiry Dates],
        mo.[Date + Expiry],
        mo.BEG_BAL,
        mo.Deductions,
        mo.Expiry,
        mo.[PO's],
        mo.Running_Balance,
        mo.MRP_IssueDate,
        mo.WCID_From_MO,
        mo.Issued,
        mo.Remaining AS Original_Required,
        mo.Has_Issued,
        mo.IssueDate_Mismatch,
        mo.Early_Issue_Flag,
        -- Clean order and item for joining
        REPLACE(LTRIM(RTRIM(mo.ORDERNUMBER)), '-', '') AS CleanOrder,
        REPLACE(LTRIM(RTRIM(mo.ITEMNMBR)), '-', '') AS CleanItem
    FROM dbo.ETB_PAB_MO mo
),

ActiveDemandJoin AS (
    -- Join to ActiveDemand for FG and demand context
    SELECT
        cd.*,
        ad.FG,
        ad.[FG Desc],
        ad.STSDESCR,
        ad.MRPTYPE
    FROM CleanData cd
    LEFT JOIN dbo.ETB_ActiveDemand_Union_FG_MO ad
        ON cd.CleanOrder = REPLACE(LTRIM(RTRIM(ad.ORDERNUMBER)), '-', '')
        AND cd.CleanItem = REPLACE(LTRIM(RTRIM(ad.ITEMNMBR)), '-', '')
),

VendorItemJoin AS (
    -- Join to Vendor Items for supply chain attributes
    SELECT
        adj.*,
        vi.VendorItem,
        vi.PRIME_VNDR,
        vi.PURCHASING_LT,
        vi.PLANNING_LT,
        vi.ORDER_POINT_QTY,
        vi.SAFETY_STOCK
    FROM ActiveDemandJoin adj
    LEFT JOIN dbo.Prosenthal_Vendor_Items vi
        ON adj.ITEMNMBR = vi.ITEMNMBR
),

RankedData AS (
    -- Rank and deduplicate by ORDERNUMBER, FG, ITEMNMBR
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ORDERNUMBER, FG, ITEMNMBR
            ORDER BY DUEDATE ASC, MRP_IssueDate ASC
        ) AS rn
    FROM VendorItemJoin
),

LedgerMatch AS (
    -- Match to ledger (PK010033) for issue tracking
    SELECT
        r.*,
        pk.IssueDate AS Ledger_IssueDate,
        pk.QtyIssued AS Ledger_QtyIssued
    FROM RankedData r
    LEFT JOIN dbo.PK010033 pk
        ON r.CleanOrder = REPLACE(LTRIM(RTRIM(pk.ORDERNUMBER)), '-', '')
        AND r.CleanItem = REPLACE(LTRIM(RTRIM(pk.ITEMNMBR)), '-', '')
    WHERE r.rn = 1
),

FinalOutput AS (
    -- Final preparation with embedded BegBal validation
    SELECT
        ITEMNMBR,
        ItemDescription,
        UOM,
        ORDERNUMBER,
        Construct,
        DUEDATE,
        [Expiry Dates],
        [Date + Expiry],
        -- EMBEDDED BegBal: ISNUMERIC validation, cast to decimal(18,6), default 0
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
        -- Unified_Value: ITEMNMBR + Date+Expiry + (Required - Issued)
        CONCAT(
            ITEMNMBR,
            '|',
            [Date + Expiry],
            '|',
            CAST((Original_Required - ISNULL(Issued, 0)) AS VARCHAR(50))
        ) AS Unified_Value
    FROM LedgerMatch
)

SELECT *
FROM FinalOutput;

GO
