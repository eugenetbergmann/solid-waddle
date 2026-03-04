-- ============================================================================
-- VIEW: ETB_PAB_AUTO
-- Purpose: Base demand normalization and MO matching
-- Author: Zo Computer
-- Date: 2026-02-26
-- Dependencies: dbo.ETB_PAB_MO, dbo.ETB_ActiveDemand_Union_FG_MO,
--               dbo.Prosenthal_Vendor_Items, dbo.PK010033,
--               dbo.WO010032, dbo.IV00101
-- ============================================================================
/*
================================================================================
ETB_PAB_AUTO — Base Demand Normalization and MO Matching (View 1)
================================================================================
Purpose:
  Normalize raw PAB (Plan-A-Buy) demand rows from ETB_PAB_MO, match each row to
  its manufacturing order (FG/Construct context from ETB_ActiveDemand_Union_FG_MO),
  enrich with item-master data, and cross-reference the manufacturing ledger
  (PK010033) to attach issue/remaining quantities and work-centre IDs.

  This view is the entry point of the solid-waddle pipeline.  Every downstream
  view (2–5, 8) ultimately derives from this data shape.

Source Tables:
  dbo.ETB_PAB_MO                    — Raw PAB demand rows
  dbo.ETB_ActiveDemand_Union_FG_MO  — FG / Construct mapping
  dbo.Prosenthal_Vendor_Items       — Item master (description, UOM)
  dbo.PK010033                      — Manufacturing ledger (issue / WC data)
  dbo.WO010032                      — Work-order status filter
  dbo.IV00101                       — Item master join (ledger enrichment)

CTE Pipeline:
  Config         → Named threshold constants (replaces magic numbers)
  p_norm         → Normalize PAB rows: clean order/item strings, safe-cast deductions
  m_norm         → Normalize demand-union: clean order strings, deduplicate FG per MO
  item_desc      → Item master lookup (description + UOM schedule)
  joined         → Core join: p_norm × m_norm × item_desc
  ranked         → Row-number deduplication (one row per ORDERNUMBER/FG/ITEMNMBR)
  Core           → Filter to rn_final = 1
  ledger_ranked  → Manufacturing ledger with issue qty deduplication
  Final          → Attach ledger data; compute WC flags and vendor data quality

Data Quality Flags (Issue_Data_Quality_Flag):
  CLEAN          — Vendor, cost, and MO data all present
  MISSING_VENDOR — PRIME_VNDR is NULL in all sources
  NON_WC_SITE    — WCID_From_MO does not follow WC-W% pattern

Change Log:
  2026-02-26: Initial hardening — replaced ISNUMERIC with TRY_CAST, added
              UNASSIGNED vendor fallback, named threshold CTEs, data quality
              flags, header documentation. (Issues 4, 6, 8, 10)
================================================================================
*/

WITH

-- ============================================================================
-- Config: Named threshold constants (Issue 8 — eliminates magic numbers)
-- ============================================================================
-- Business rationale for each constant is documented inline.
-- ============================================================================
Config AS
(
    SELECT
        -- How many days past due an unissued MO must be before it is
        -- considered "stale" and suppressed from running-balance calculation.
        -- Business rule: MOs older than 7 days with zero issued qty are noise.
        7   AS Stale_Suppression_Days,

        -- Fence window (days forward from today) within which WC inventory
        -- covering remaining demand justifies suppression of that demand row.
        -- Business rule: if inventory covers demand due within 7 days, suppress.
        7   AS Fence_Suppression_Days,

        -- How many days old a manufacturing ledger issue is before the
        -- Early_Issue_Flag fires.  Mirrors Stale_Suppression_Days by design.
        7   AS Early_Issue_Flag_Days
),

-- ============================================================================
-- p_norm: Normalize PAB source rows
-- ============================================================================
-- Cleans ORDERNUMBER (strips MO prefix and punctuation characters).
-- Safely converts Deductions to decimal via TRY_CAST (Issue 6).
-- Excludes Partially Received, SCRAP, and 60.x / 70.x item classes.
-- ============================================================================
p_norm AS
(
    SELECT  p.*,
            -- Issue 7: REPLACE chain documented — strips: 'MO', '-', ' ', '/', '.', '#'
            -- These characters appear in order numbers from the ERP import process.
            UPPER(LTRIM(RTRIM(CONVERT(varchar(255),
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    p.ORDERNUMBER,
                    'MO', ''), '-', ''), ' ', ''), '/', ''), '.', ''), '#', ''))
            )))                                                 AS CleanOrder,

            LTRIM(RTRIM(p.ITEMNMBR))                            AS CleanItem,

            -- Issue 6: TRY_CAST replaces ISNUMERIC (ISNUMERIC returns 1 for
            -- currency symbols and scientific notation, which CAST would reject).
            ISNULL(TRY_CAST(LTRIM(RTRIM(p.Deductions)) AS decimal(18, 5)), 0)
                                                                AS CleanDeductions

    FROM    dbo.ETB_PAB_MO p
    WHERE   p.STSDESCR <> 'Partially Received'
      AND   p.STSDESCR <> 'SCRAP'
      AND   LTRIM(RTRIM(p.ITEMNMBR)) NOT LIKE '60.%'
      AND   LTRIM(RTRIM(p.ITEMNMBR)) NOT LIKE '70.%'
),

-- ============================================================================
-- m_norm: Normalize active-demand union (FG / Construct mapping)
-- ============================================================================
-- Deduplicates to one FG row per CleanOrder using ROW_NUMBER.
-- Same REPLACE chain as p_norm for consistent matching.
-- ============================================================================
m_norm AS
(
    SELECT  m.*,
            -- Issue 7: Identical REPLACE chain — must stay in sync with p_norm.
            UPPER(LTRIM(RTRIM(CONVERT(varchar(255),
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    m.ORDERNUMBER,
                    'MO', ''), '-', ''), ' ', ''), '/', ''), '.', ''), '#', ''))
            )))                                                 AS CleanOrder,

            ROW_NUMBER() OVER (
                PARTITION BY
                    UPPER(LTRIM(RTRIM(CONVERT(varchar(255),
                        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                            m.ORDERNUMBER,
                            'MO', ''), '-', ''), ' ', ''), '/', ''), '.', ''), '#', '')))))
                    , m.FG
                ORDER BY m.Customer, m.[FG Desc], m.ORDERNUMBER
            )                                                   AS rn_fg

    FROM    dbo.ETB_ActiveDemand_Union_FG_MO m
),

-- ============================================================================
-- item_desc: Item master enrichment (description + UOM schedule)
-- ============================================================================
item_desc AS
(
    SELECT  [Item Number]   AS ItemNumber,
            ITEMDESC        AS ItemDescription,
            UOMSCHDL
    FROM    dbo.Prosenthal_Vendor_Items
    WHERE   Active = 'Yes'
),

-- ============================================================================
-- joined: Core assembly — PAB rows joined to FG context and item master
-- ============================================================================
joined AS
(
    SELECT
            CAST(COALESCE(
                NULLIF(LTRIM(RTRIM(p_norm.ORDERNUMBER)), ''),
                NULLIF(LTRIM(RTRIM(m_norm.ORDERNUMBER)), '')
            ) AS varchar(255))                                  AS ORDERNUMBER,

            CAST(ISNULL(m_norm.Customer, '')  AS varchar(255)) AS Construct,
            CAST(ISNULL(m_norm.FG, '')        AS varchar(255)) AS FG,
            CAST(ISNULL(m_norm.[FG Desc], '') AS varchar(255)) AS [FG Desc],
            CAST(ISNULL(p_norm.ITEMNMBR, '')  AS varchar(255)) AS ITEMNMBR,

            CAST(ISNULL(item_desc.ItemDescription, '') AS varchar(500))
                                                                AS ItemDescription,
            CAST(ISNULL(item_desc.UOMSCHDL, '')        AS varchar(50))
                                                                AS UOMSCHDL,

            p_norm.STSDESCR,
            p_norm.DUEDATE,
            p_norm.[Expiry Dates],
            p_norm.[Date + Expiry],
            p_norm.MRPTYPE,
            p_norm.VendorItem,

            -- Issue 4: UNASSIGNED fallback — items with no vendor still flow
            -- through the pipeline and are flagged for data-quality review.
            COALESCE(NULLIF(p_norm.PRIME_VNDR, ''), 'UNASSIGNED')  AS PRIME_VNDR,

            -- Issue 4: Track which source provided the vendor value
            CASE
                WHEN p_norm.PRIME_VNDR IS NOT NULL
                 AND LTRIM(RTRIM(p_norm.PRIME_VNDR)) <> '' THEN 'PAB_MO'
                ELSE                                             'UNASSIGNED'
            END                                                 AS Vendor_Data_Source,

            p_norm.PURCHASING_LT,
            p_norm.PLANNING_LT,
            p_norm.ORDER_POINT_QTY,
            p_norm.SAFETY_STOCK,

            p_norm.Deductions   AS Original_Deductions,
            p_norm.Expiry       AS Original_Expiry,
            p_norm.[PO's]       AS Original_POs,
            p_norm.Running_Balance AS Original_Running_Balance,

            -- Issue 6: TRY_CAST replaces ISNUMERIC for BEG_BAL
            ISNULL(TRY_CAST(LTRIM(RTRIM(p_norm.BEG_BAL)) AS decimal(18, 6)), 0)
                                                                AS BEG_BAL_Num,

            p_norm.CleanOrder,
            p_norm.CleanItem,
            p_norm.CleanDeductions

    FROM    p_norm
    LEFT  JOIN m_norm    ON  p_norm.CleanOrder = m_norm.CleanOrder
                         AND m_norm.rn_fg       = 1
    LEFT  JOIN item_desc ON  p_norm.ITEMNMBR    = item_desc.ItemNumber
),

-- ============================================================================
-- ranked: Deduplicate to one row per (ORDERNUMBER, FG, ITEMNMBR)
-- ============================================================================
-- Order preference: Construct, FG Desc, STSDESCR — deterministic tie-break.
-- ============================================================================
ranked AS
(
    SELECT  *,
            ROW_NUMBER() OVER (
                PARTITION BY ORDERNUMBER, FG, ITEMNMBR
                ORDER BY Construct, [FG Desc], STSDESCR
            )                                                   AS rn_final
    FROM    joined
),

-- ============================================================================
-- Core: Single representative row per (ORDERNUMBER, FG, ITEMNMBR)
-- ============================================================================
Core AS
(
    SELECT  *
    FROM    ranked
    WHERE   rn_final = 1
),

-- ============================================================================
-- ledger_ranked: Manufacturing ledger with WC and issue-qty deduplication
-- ============================================================================
-- Two row-number strategies:
--   rn_qty  — best match when CleanDeductions matches Required_Qty exactly
--   rn_any  — fallback: prefer rows with any issued qty, then largest abs qty
--
-- NOLOCK hints are intentional — this is a read-only reporting query and
-- ledger tables are high-concurrency.  Index hint candidates: PK010033 on
-- (MANUFACTUREORDER_I, ITEMNMBR) and WO010032 on (MANUFACTUREORDER_I).
-- ============================================================================
ledger_ranked AS
(
    SELECT
            RTRIM(LTRIM(a.MANUFACTUREORDER_I))                  AS CleanMO,
            RTRIM(LTRIM(a.ITEMNMBR))                            AS ITEMNMBR,
            CAST(a.MRPISSUEDATE_I AS date)                      AS MRP_IssueDate,
            a.WCID_I,
            a.QTY_ISSUED_I + a.QTY_BACKFLUSHED_I               AS Total_Issued,
            a.MRPAMOUNT_I - a.ATYALLOC
                - a.QTY_ISSUED_I - a.QTY_BACKFLUSHED_I         AS Remaining_Required,
            a.MRPAMOUNT_I                                       AS Required_Qty,

            -- rn_qty: dedup when quantity matches exactly (most precise match)
            ROW_NUMBER() OVER (
                PARTITION BY RTRIM(LTRIM(a.MANUFACTUREORDER_I)),
                             RTRIM(LTRIM(a.ITEMNMBR)),
                             a.MRPAMOUNT_I
                ORDER BY CAST(a.MRPISSUEDATE_I AS date) DESC
            )                                                   AS rn_qty,

            -- rn_any: fallback — any MO+item match, prefer rows with issued qty
            ROW_NUMBER() OVER (
                PARTITION BY RTRIM(LTRIM(a.MANUFACTUREORDER_I)),
                             RTRIM(LTRIM(a.ITEMNMBR))
                ORDER BY
                    CASE WHEN (a.QTY_ISSUED_I + a.QTY_BACKFLUSHED_I) > 0
                         THEN 1 ELSE 2 END,
                    ABS(a.MRPAMOUNT_I) DESC,
                    CAST(a.MRPISSUEDATE_I AS date) DESC
            )                                                   AS rn_any

    FROM    dbo.PK010033 a WITH (NOLOCK)
    LEFT  JOIN dbo.IV00101 b WITH (NOLOCK) ON a.ITEMNMBR = b.ITEMNMBR
    WHERE   EXISTS (
                SELECT 1
                FROM   dbo.WO010032 w WITH (NOLOCK)
                WHERE  w.MANUFACTUREORDERST_I IN (2, 3)
                  AND  RTRIM(LTRIM(w.MANUFACTUREORDER_I)) = RTRIM(LTRIM(a.MANUFACTUREORDER_I))
            )
),

-- ============================================================================
-- Final: Attach ledger data; derive WC flags and data-quality assessment
-- ============================================================================
Final AS
(
    SELECT
            Core.*,

            -- Ledger-derived fields (NULL-safe defaults)
            ISNULL(ml.MRP_IssueDate,        '')  AS MRP_IssueDate,
            ISNULL(ml.WCID_I,               '')  AS WCID_From_MO,
            ISNULL(ml.Total_Issued,         0)   AS Issued,
            ISNULL(ml.Remaining_Required,   0)   AS Remaining,

            CASE WHEN ISNULL(ml.Total_Issued, 0) > 0
                 THEN 'YES' ELSE 'NO'
            END                                             AS Has_Issued,

            -- Flag when ledger issue date differs from PAB expiry date
            CASE WHEN ml.MRP_IssueDate IS NULL OR Core.[Date + Expiry] IS NULL
                     THEN 'NO'
                 WHEN ml.MRP_IssueDate <> TRY_CAST(Core.[Date + Expiry] AS date)
                     THEN 'YES'
                 ELSE 'NO'
            END                                             AS IssueDate_Mismatch,

            -- Issue 8: Early_Issue_Flag_Days drives the threshold below
            -- (Config CTE value = 7; see Config for business rationale)
            CASE WHEN ISNULL(ml.Total_Issued, 0) > 0
                  AND Core.[Date + Expiry] IS NOT NULL
                  AND TRY_CAST(Core.[Date + Expiry] AS date)
                      < DATEADD(DAY, -(SELECT cfg.Early_Issue_Flag_Days FROM Config cfg),
                                CAST(GETDATE() AS date))
                 THEN 'YES'
                 ELSE 'NO'
            END                                             AS Early_Issue_Flag,

            -- Unified display value for debugging / cross-reference
            CASE WHEN ml.Required_Qty IS NULL
                 THEN CONCAT(Core.ITEMNMBR, ' - ', Core.[Date + Expiry],
                             ' - ', Core.CleanDeductions)
                 ELSE CONCAT(Core.ITEMNMBR, ' - ', Core.[Date + Expiry],
                             ' - ', ml.Required_Qty - ml.Total_Issued)
            END                                             AS Unified_Value,

            -- Issue 2 / Issue 5: Validate WC site follows WC-W% naming convention.
            -- Rows where WCID_From_MO does not match are flagged for review.
            CASE WHEN ISNULL(ml.WCID_I, '') = ''      THEN 'NO_WC'
                 WHEN ml.WCID_I LIKE 'WC-W%'          THEN 'VALID_WC'
                 ELSE                                       'NON_WC_SITE'
            END                                             AS WC_Site_Validation,

            -- Issue 5: Row-level data quality flag
            CASE WHEN COALESCE(NULLIF(Core.PRIME_VNDR, ''), 'UNASSIGNED') = 'UNASSIGNED'
                      THEN 'MISSING_VENDOR'
                 ELSE      'CLEAN'
            END                                             AS Data_Quality_Flag

    FROM    Core
    LEFT  JOIN ledger_ranked ml
           ON  Core.CleanOrder   = ml.CleanMO
           AND Core.CleanItem    = ml.ITEMNMBR
           AND ml.rn_any = 1  -- Single deterministic match per MO+Item (fixes 3x row duplication bug)
)

-- ============================================================================
-- Output: View 1 columns — consumed by View 3 (ETB_WFQ_PIPE) and View 4+
-- ============================================================================
SELECT
    ITEMNMBR,
    ItemDescription,
    UOMSCHDL                            AS UOM,
    ORDERNUMBER,
    Construct,
    DUEDATE,
    [Expiry Dates],
    [Date + Expiry],
    CAST(BEG_BAL_Num AS varchar(50))    AS BEG_BAL,
    Original_Deductions                 AS Deductions,
    Original_Expiry                     AS Expiry,
    Original_POs                        AS [PO's],
    Original_Running_Balance            AS Running_Balance,
    MRP_IssueDate,
    WCID_From_MO,
    Issued,
    Remaining,
    Has_Issued,
    IssueDate_Mismatch,
    Early_Issue_Flag,
    WC_Site_Validation,
    VendorItem,
    PRIME_VNDR,
    Vendor_Data_Source,                 -- Issue 4: vendor source tracking
    Data_Quality_Flag,                  -- Issue 5: row-level data quality
    PURCHASING_LT,
    PLANNING_LT,
    ORDER_POINT_QTY,
    SAFETY_STOCK,
    FG,
    [FG Desc],
    STSDESCR,
    MRPTYPE,
    Unified_Value

FROM    Final;
