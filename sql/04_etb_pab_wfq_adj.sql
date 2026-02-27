-- ============================================================================
-- VIEW: ETB_PAB_WFQ_ADJ
-- Purpose: WFQ-adjusted balances with extended demand logic
-- Author: Zo Computer
-- Date: 2026-02-26
-- Dependencies: dbo.ETB_PAB_MO, dbo.ETB_ActiveDemand_Union_FG_MO,
--               dbo.Prosenthal_Vendor_Items, dbo.PK010033, dbo.WO010032,
--               dbo.IV00101, dbo.Prosenthal_INV_BIN_QTY_wQTYTYPE,
--               dbo.ETB_WFQ_PIPE (View 3)
-- ============================================================================
/*
================================================================================
ETB_PAB_WFQ_ADJ — WFQ-Adjusted Balances with Extended Demand Logic (View 4)
================================================================================
Purpose:
  Extends View 2 (ETB_WC_INV_UNIFIED / analysis-views) by integrating WFQ
  supply from View 3 (ETB_WFQ_PIPE) to compute an extended balance that reflects
  upcoming WFQ arrivals.  Identifies stockout positions, the first stockout
  sequence, and classifies each demand row's WFQ dependency status.

  This view is the basis for supply-action recommendations in View 5.

Source / Dependency:
  dbo.ETB_WFQ_PIPE     — WFQ lot supply (View 3) — already a database view
  + All upstream sources from View 2 (re-materialized here for performance)

CTE Pipeline:
  Config              → Named threshold constants (Issue 8)
  p_norm … Adjusted   → Full View 2 logic re-inline (WC inv + running balance)
  ETB_WC_INV          → Reshaped Adjusted CTE output
  Demand_Ledger       → Full ledger including BegBal rows
  DemandRowsOnly      → Filter to actionable demand rows only
  Demand_Seq          → Row number per item ordered by DUEDATE (for WFQ join)
  Stockout_Detection  → MIN(Demand_Seq) where balance first goes negative
  WFQ_Supply          → Aggregated WFQ qty per (ITEMNMBR, Estimated_Release_Date)
                        (Issue 1: GROUP BY ITEMNMBR, Estimated_Release_Date only —
                         SITE is intentionally excluded to consolidate supply)
  WFQ_Allocated       → Per demand-row WFQ influx calculation
                        (Issue 1: joined on ITEMNMBR + Demand_Seq — prevents
                         row multiplication from multiple WFQ lots matching the
                         same demand row)
  Extended_Demand     → Demand rows with Ledger_Extended_Balance
  Final_Ledger        → All ledger rows with WFQ status attached

Issue 1 Fix — Running Balance Doubling:
  Root cause: WFQ_Supply aggregated including SITE caused multiple rows per
  item when the same item existed in both WF-Q and UNDERINV.

  Fix applied:
    1. WFQ_Supply aggregates by (ITEMNMBR, Estimated_Release_Date) only.
    2. WFQ_Allocated joins on (ITEMNMBR) only, filtering by release date in CASE.
    3. GROUP BY in WFQ_Allocated is (ITEMNMBR, Demand_Seq, DUEDATE,
       Adjusted_Running_Balance, Stockout_Seq) — one row per demand row.
    4. Extended_Demand joins on (ITEMNMBR, Demand_Seq) — 1:1 join guarantee.

WFQ_Extended_Status Values:
  LEDGER_ONLY      — No WFQ supply available for this demand row
  WFQ_RESCUED      — WFQ pushed balance from negative to positive
  WFQ_ENHANCED     — WFQ improved an already-positive balance
  WFQ_INSUFFICIENT — WFQ supply available but balance still negative
  NON_DEMAND_LEDGER_ROW — BegBal or non-actionable row
  BEGINNING BALANCE — BegBal seed row

Change Log:
  2026-02-26: Full hardening — Issue 1 (WFQ doubling fix), Issues 2–10
              (TRY_CAST, UNASSIGNED vendor, cycle counts, Config thresholds,
              WC site validation, data quality flags, NOLOCK consistency,
              documentation).
================================================================================
*/

WITH

-- ============================================================================
-- Config: Named threshold constants (Issue 8)
-- ============================================================================
Config AS
(
    SELECT
        7   AS Stale_Suppression_Days,    -- Days past due for stale suppression
        7   AS Fence_Suppression_Days,    -- Near-term fence window (days forward)
        45  AS WC_Inventory_Age_Days,     -- Max age of active WC bin inventory
        90  AS Cycle_Count_Overdue_Days,  -- Days before cycle count is OVERDUE
        7   AS Early_Issue_Flag_Days      -- Early issue detection threshold
),

-- ============================================================================
-- p_norm: Normalize PAB source rows (Issue 6: TRY_CAST; Issue 7: documented)
-- ============================================================================
p_norm AS
(
    SELECT  p.*,
            -- Issue 7: REPLACE chain strips 'MO', '-', ' ', '/', '.', '#'
            UPPER(LTRIM(RTRIM(CONVERT(varchar(255),
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    p.ORDERNUMBER,
                    'MO', ''), '-', ''), ' ', ''), '/', ''), '.', ''), '#', ''))
            )))                                                 AS CleanOrder,

            LTRIM(RTRIM(p.ITEMNMBR))                            AS CleanItem,

            -- Issue 6: TRY_CAST replaces ISNUMERIC
            ISNULL(TRY_CAST(LTRIM(RTRIM(p.Deductions)) AS decimal(18, 5)), 0)
                                                                AS CleanDeductions

    FROM    dbo.ETB_PAB_MO p
    WHERE   p.STSDESCR <> 'Partially Received'
      AND   p.STSDESCR <> 'SCRAP'
      AND   LTRIM(RTRIM(p.ITEMNMBR)) NOT LIKE '60.%'
      AND   LTRIM(RTRIM(p.ITEMNMBR)) NOT LIKE '70.%'
),

-- ============================================================================
-- m_norm: Normalize demand-union with FG deduplication
-- ============================================================================
m_norm AS
(
    SELECT  m.*,
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
                            'MO', ''), '-', ''), ' ', ''), '/', ''), '.', ''), '#', ')))))
                    , m.FG
                ORDER BY m.Customer, m.[FG Desc], m.ORDERNUMBER
            )                                                   AS rn_fg

    FROM    dbo.ETB_ActiveDemand_Union_FG_MO m
),

-- ============================================================================
-- item_desc: Item master lookup
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
-- joined: Core assembly with UNASSIGNED vendor fallback (Issue 4)
-- ============================================================================
joined AS
(
    SELECT
            CAST(COALESCE(
                NULLIF(LTRIM(RTRIM(p_norm.ORDERNUMBER)), ''),
                NULLIF(LTRIM(RTRIM(m_norm.ORDERNUMBER)), '')
            ) AS varchar(255))                                  AS ORDERNUMBER,

            CAST(ISNULL(m_norm.Customer,  '') AS varchar(255)) AS Construct,
            CAST(ISNULL(m_norm.FG,        '') AS varchar(255)) AS FG,
            CAST(ISNULL(m_norm.[FG Desc], '') AS varchar(255)) AS [FG Desc],
            CAST(ISNULL(p_norm.ITEMNMBR,  '') AS varchar(255)) AS ITEMNMBR,

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

            -- Issue 4: UNASSIGNED fallback + source tracking
            COALESCE(NULLIF(p_norm.PRIME_VNDR, ''), 'UNASSIGNED')  AS PRIME_VNDR,
            CASE
                WHEN p_norm.PRIME_VNDR IS NOT NULL
                 AND LTRIM(RTRIM(p_norm.PRIME_VNDR)) <> '' THEN 'PAB_MO'
                ELSE                                             'UNASSIGNED'
            END                                                 AS Vendor_Data_Source,

            p_norm.PURCHASING_LT,
            p_norm.PLANNING_LT,
            p_norm.ORDER_POINT_QTY,
            p_norm.SAFETY_STOCK,

            p_norm.Deductions       AS Original_Deductions,
            p_norm.Expiry           AS Original_Expiry,
            p_norm.[PO's]           AS Original_POs,
            p_norm.Running_Balance  AS Original_Running_Balance,

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
-- ranked / Core: Deduplicate to one row per (ORDERNUMBER, FG, ITEMNMBR)
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
Core AS
(
    SELECT  * FROM ranked WHERE rn_final = 1
),

-- ============================================================================
-- ledger_ranked: Manufacturing ledger deduplication (Issue 9: NOLOCK)
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

            ROW_NUMBER() OVER (
                PARTITION BY RTRIM(LTRIM(a.MANUFACTUREORDER_I)),
                             RTRIM(LTRIM(a.ITEMNMBR)),
                             a.MRPAMOUNT_I
                ORDER BY CAST(a.MRPISSUEDATE_I AS date) DESC
            )                                                   AS rn_qty,

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
-- PAB_Final: Attach ledger data (Issue 2: WC site validation)
-- ============================================================================
PAB_Final AS
(
    SELECT
            Core.*,
            ISNULL(ml.MRP_IssueDate,       '')  AS MRP_IssueDate,
            ISNULL(ml.WCID_I,              '')  AS WCID_From_MO,
            ISNULL(ml.Total_Issued,        0)   AS Issued,
            ISNULL(ml.Remaining_Required,  0)   AS Remaining,

            CASE WHEN ISNULL(ml.Total_Issued, 0) > 0
                 THEN 'YES' ELSE 'NO'
            END                                             AS Has_Issued,

            CASE WHEN ml.MRP_IssueDate IS NULL OR Core.[Date + Expiry] IS NULL
                     THEN 'NO'
                 WHEN ml.MRP_IssueDate <> TRY_CAST(Core.[Date + Expiry] AS date)
                     THEN 'YES'
                 ELSE 'NO'
            END                                             AS IssueDate_Mismatch,

            CASE WHEN ISNULL(ml.Total_Issued, 0) > 0
                  AND Core.[Date + Expiry] IS NOT NULL
                  AND TRY_CAST(Core.[Date + Expiry] AS date)
                      < DATEADD(DAY, -(SELECT cfg.Early_Issue_Flag_Days FROM Config cfg),
                                CAST(GETDATE() AS date))
                 THEN 'YES' ELSE 'NO'
            END                                             AS Early_Issue_Flag,

            CASE WHEN ml.Required_Qty IS NULL
                 THEN CONCAT(Core.ITEMNMBR, ' - ', Core.[Date + Expiry],
                             ' - ', Core.CleanDeductions)
                 ELSE CONCAT(Core.ITEMNMBR, ' - ', Core.[Date + Expiry],
                             ' - ', ml.Required_Qty - ml.Total_Issued)
            END                                             AS Unified_Value,

            -- Issue 2: WC site pattern validation
            CASE WHEN ISNULL(ml.WCID_I, '') = ''   THEN 'NO_WC'
                 WHEN ml.WCID_I LIKE 'WC-W%'       THEN 'VALID_WC'
                 ELSE                                    'NON_WC_SITE'
            END                                             AS WC_Site_Validation

    FROM    Core
    LEFT  JOIN ledger_ranked ml
           ON  Core.CleanOrder   = ml.CleanMO
           AND Core.CleanItem    = ml.ITEMNMBR
           AND (  (Core.CleanDeductions = ml.Required_Qty AND ml.rn_qty = 1)
               OR  ml.rn_any = 1)
),

-- ============================================================================
-- Base: Column reshape
-- ============================================================================
Base AS
(
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
            WC_Site_Validation,
            Issued,
            Remaining,
            Has_Issued,
            IssueDate_Mismatch,
            Early_Issue_Flag,
            VendorItem,
            PRIME_VNDR,
            Vendor_Data_Source,
            PURCHASING_LT,
            PLANNING_LT,
            ORDER_POINT_QTY,
            SAFETY_STOCK,
            FG,
            [FG Desc],
            STSDESCR,
            MRPTYPE,
            Unified_Value
    FROM    PAB_Final
),

-- ============================================================================
-- InventoryAgg: WC bin inventory (Issue 2: WC-W% filter; Issue 8: age from Config)
-- ============================================================================
InventoryAgg AS
(
    SELECT
            Item_Number,
            SITE,
            SUM(QTY_Available)                                  AS Total_QTY_Available
    FROM    dbo.Prosenthal_INV_BIN_QTY_wQTYTYPE
    WHERE   SITE LIKE 'WC-W%'
      AND   DATEDIFF(DAY, DATERECD, GETDATE())
              <= (SELECT cfg.WC_Inventory_Age_Days FROM Config cfg)
    GROUP BY Item_Number, SITE
),

-- ============================================================================
-- CycleCount: Last cycle count per item (Issue 3)
-- NOTE: dbo.IV10300 does not exist in this environment (no cycle count data).
--       Stub returns zero rows with typed NULLs to preserve output schema.
--       Downstream columns (Last_Cycle_Count_Date, Days_Since_Last_Cycle_Count,
--       Cycle_Count_Status) will output as NULL / NEVER_COUNTED.
-- ============================================================================
CycleCount AS
(
    SELECT
            CAST(NULL AS varchar(31))                           AS ITEMNMBR,
            CAST(NULL AS date)                                  AS Last_Cycle_Count_Date
    WHERE   1 = 0
),

-- ============================================================================
-- WithInventory: Attach WC inventory and cycle count
-- ============================================================================
WithInventory AS
(
    SELECT
            b.*,
            ISNULL(inv.Total_QTY_Available, 0)                  AS Inventory_Qty_Available,
            cc.Last_Cycle_Count_Date,
            CASE WHEN cc.Last_Cycle_Count_Date IS NULL THEN NULL
                 ELSE DATEDIFF(DAY, cc.Last_Cycle_Count_Date, CAST(GETDATE() AS date))
            END                                                 AS Days_Since_Last_Cycle_Count,
            CASE WHEN cc.Last_Cycle_Count_Date IS NULL
                     THEN 'NEVER_COUNTED'
                 WHEN DATEDIFF(DAY, cc.Last_Cycle_Count_Date, CAST(GETDATE() AS date))
                      > (SELECT cfg.Cycle_Count_Overdue_Days FROM Config cfg)
                     THEN 'OVERDUE'
                 ELSE 'CURRENT'
            END                                                 AS Cycle_Count_Status
    FROM    Base b
    LEFT  JOIN InventoryAgg inv
           ON  b.ITEMNMBR     = inv.Item_Number
           AND b.WCID_From_MO = inv.SITE
    LEFT  JOIN CycleCount cc  ON  b.ITEMNMBR = cc.ITEMNMBR
),

-- ============================================================================
-- Flags: Suppress_Stale, Suppress_Fence (Issues 2, 8)
-- ============================================================================
Flags AS
(
    SELECT
            wi.*,
            CASE WHEN Inventory_Qty_Available > 0
                  AND Inventory_Qty_Available < Remaining
                 THEN Remaining - Inventory_Qty_Available
                 ELSE Remaining
            END                                                 AS Net_Demand,

            CASE WHEN LTRIM(RTRIM(ORDERNUMBER)) = 'Beg Bal'
                 THEN 1 ELSE 0
            END                                                 AS Is_BegBal_Row,

            CASE WHEN LTRIM(RTRIM(ORDERNUMBER)) = 'Beg Bal' THEN 0
                 WHEN DUEDATE <= DATEADD(DAY,
                          -(SELECT cfg.Stale_Suppression_Days FROM Config cfg),
                          CAST(GETDATE() AS date))
                  AND ISNULL(Issued, 0) = 0
                     THEN 1
                 ELSE 0
            END                                                 AS Suppress_Stale,

            CASE WHEN LTRIM(RTRIM(ORDERNUMBER)) = 'Beg Bal' THEN 0
                 WHEN Remaining > 0
                  AND DUEDATE <= DATEADD(DAY,
                           (SELECT cfg.Fence_Suppression_Days FROM Config cfg),
                           CAST(GETDATE() AS date))
                  AND Inventory_Qty_Available >= Remaining
                     THEN 1
                 ELSE 0
            END                                                 AS Suppress_Fence
    FROM    WithInventory wi
),

-- ============================================================================
-- Unified: Suppression status and audit trail (Issue 2)
-- ============================================================================
Unified AS
(
    SELECT
            f.*,
            CASE WHEN Is_BegBal_Row = 1     THEN 0
                 WHEN Suppress_Stale = 1    THEN 1
                 WHEN Suppress_Fence = 1    THEN 1
                 ELSE 0
            END                                                 AS Is_Suppressed,

            CASE WHEN Is_BegBal_Row = 1
                     THEN 'BEGINNING BALANCE'
                 WHEN Suppress_Stale = 1
                     THEN 'SUPPRESSED: Stale & Unissued'
                 WHEN Suppress_Fence = 1
                     THEN 'SUPPRESSED: Full Coverage in Fence'
                 WHEN Inventory_Qty_Available > 0
                  AND Inventory_Qty_Available < Remaining
                     THEN 'PARTIAL: Demand Netted'
                 ELSE     'VALID DEMAND'
            END                                                 AS Suppression_Status,

            CASE WHEN Is_BegBal_Row = 1                THEN NULL
                 WHEN (Suppress_Stale = 1 OR Suppress_Fence = 1) THEN 0
                 ELSE Remaining
            END                                                 AS Remaining_After_Suppression
    FROM    Flags f
),

-- ============================================================================
-- Ordered: Sequence number for running-balance window
-- ============================================================================
Ordered AS
(
    SELECT
            u.*,
            ROW_NUMBER() OVER (
                PARTITION BY u.ITEMNMBR
                ORDER BY
                    CASE WHEN u.Is_BegBal_Row = 1 THEN 0 ELSE 1 END,
                    COALESCE(u.DUEDATE,
                             TRY_CAST(u.[Date + Expiry] AS date),
                             TRY_CAST(u.MRP_IssueDate   AS date),
                             CAST('9999-12-31' AS date)),
                    u.ORDERNUMBER,
                    COALESCE(u.Unified_Value, ''),
                    COALESCE(u.STSDESCR, '')
            )                                                   AS Seq
    FROM    Unified u
),

-- ============================================================================
-- Deltas / Adjusted: Running balance recalculation with suppressed rows zeroed
-- ============================================================================
Deltas AS
(
    SELECT
            o.*,
            TRY_CAST(o.Running_Balance AS decimal(18, 6))       AS RB_Num,
            TRY_CAST(o.Running_Balance AS decimal(18, 6))
                - LAG(TRY_CAST(o.Running_Balance AS decimal(18, 6)))
                  OVER (PARTITION BY o.ITEMNMBR ORDER BY o.Seq) AS RB_Delta
    FROM    Ordered o
),
Adjusted AS
(
    SELECT
            d.*,
            FIRST_VALUE(d.RB_Num) OVER (
                PARTITION BY d.ITEMNMBR ORDER BY d.Seq
            )                                                   AS RB_Anchor,

            CASE WHEN d.Seq = 1           THEN CAST(0 AS decimal(18, 6))
                 WHEN d.Is_Suppressed = 1  THEN CAST(0 AS decimal(18, 6))
                 ELSE ISNULL(d.RB_Delta, 0)
            END                                                 AS RB_Delta_Adjusted
    FROM    Deltas d
),

-- ============================================================================
-- ETB_WC_INV: Reshaped View 2 output (Adjusted_Running_Balance computed here)
-- ============================================================================
ETB_WC_INV AS
(
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
            CAST(RB_Anchor + SUM(RB_Delta_Adjusted) OVER (
                PARTITION BY ITEMNMBR ORDER BY Seq ROWS UNBOUNDED PRECEDING
            ) AS decimal(18, 6))                                AS Adjusted_Running_Balance,
            MRP_IssueDate,
            WCID_From_MO,
            WC_Site_Validation,
            Issued,
            Remaining,
            Net_Demand,
            Inventory_Qty_Available,
            Is_Suppressed,
            Suppression_Status,
            Remaining_After_Suppression,
            Has_Issued,
            IssueDate_Mismatch,
            Early_Issue_Flag,
            Last_Cycle_Count_Date,
            Days_Since_Last_Cycle_Count,
            Cycle_Count_Status,
            VendorItem,
            PRIME_VNDR,
            Vendor_Data_Source,
            PURCHASING_LT,
            PLANNING_LT,
            ORDER_POINT_QTY,
            SAFETY_STOCK,
            FG,
            [FG Desc],
            STSDESCR,
            MRPTYPE,
            Unified_Value
    FROM    Adjusted
),

-- ============================================================================
-- Demand_Ledger: Full ledger including BegBal and supply rows
-- ============================================================================
Demand_Ledger AS
(
    SELECT
            ITEMNMBR, ItemDescription, UOM, ORDERNUMBER, Construct,
            DUEDATE, [Expiry Dates], [Date + Expiry], BEG_BAL,
            Deductions, Expiry, [PO's], Running_Balance, Adjusted_Running_Balance,
            MRP_IssueDate, WCID_From_MO, WC_Site_Validation,
            Issued, Remaining AS Original_Required, Net_Demand,
            Inventory_Qty_Available, Suppression_Status, Is_Suppressed,
            Has_Issued, IssueDate_Mismatch, Early_Issue_Flag,
            Last_Cycle_Count_Date, Days_Since_Last_Cycle_Count, Cycle_Count_Status,
            VendorItem, PRIME_VNDR, Vendor_Data_Source,
            PURCHASING_LT, PLANNING_LT, ORDER_POINT_QTY, SAFETY_STOCK,
            FG, [FG Desc], STSDESCR, MRPTYPE, Unified_Value
    FROM    ETB_WC_INV
),

-- ============================================================================
-- DemandRowsOnly: Filter to rows with actionable demand
-- ============================================================================
-- Excludes BegBal rows, rows with no due date, and rows with zero required qty.
-- ============================================================================
DemandRowsOnly AS
(
    SELECT  *
    FROM    Demand_Ledger
    WHERE   LTRIM(RTRIM(ORDERNUMBER)) <> 'Beg Bal'
      AND   DUEDATE IS NOT NULL
      AND   ISNULL(Original_Required, 0) <> 0
),

-- ============================================================================
-- Demand_Seq: Assign sequential demand position per item (for WFQ matching)
-- ============================================================================
Demand_Seq AS
(
    SELECT
            d.*,
            ROW_NUMBER() OVER (
                PARTITION BY d.ITEMNMBR
                ORDER BY d.DUEDATE, d.ORDERNUMBER, COALESCE(d.Unified_Value, '')
            )                                                   AS Demand_Seq
    FROM    DemandRowsOnly d
),

-- ============================================================================
-- Stockout_Detection: First demand sequence where balance goes negative
-- ============================================================================
Stockout_Detection AS
(
    SELECT
            ds.*,
            MIN(CASE WHEN ds.Adjusted_Running_Balance <= 0
                     THEN ds.Demand_Seq END)
                OVER (PARTITION BY ds.ITEMNMBR)                 AS Stockout_Seq
    FROM    Demand_Seq ds
),

-- ============================================================================
-- WFQ_Supply: Aggregate WFQ lot supply per item per estimated release date
-- ============================================================================
-- Issue 1 FIX: GROUP BY uses (ITEMNMBR, Estimated_Release_Date) only.
-- SITE excluded to consolidate supply across WF-Q and UNDERINV.
-- ============================================================================
WFQ_Supply AS
(
    SELECT
            ITEM_Number                                         AS ITEMNMBR,
            Estimated_Release_Date,
            SUM(QTY_ON_HAND)                                    AS WFQ_Qty
    FROM    dbo.ETB_WFQ_PIPE
    WHERE   View_Level  = 'ITEM_LEVEL'
      AND   QTY_ON_HAND > 0
    GROUP BY
            ITEM_Number,
            Estimated_Release_Date
),

-- ============================================================================
-- WFQ_Allocated: Per-demand-row WFQ influx calculation (Issue 1 FIX: 1:1 guarantee)
-- ============================================================================
WFQ_Allocated AS
(
    SELECT
            d.ITEMNMBR,
            d.Demand_Seq,
            d.DUEDATE,
            d.Adjusted_Running_Balance                          AS Ledger_Base_Balance,
            d.Stockout_Seq,

            -- Conditional SUM: only WFQ lots released on or before this demand date
            SUM(
                CASE
                    WHEN d.Stockout_Seq IS NOT NULL
                     AND d.Demand_Seq  >= d.Stockout_Seq
                     AND w.Estimated_Release_Date <= d.DUEDATE
                    THEN w.WFQ_Qty
                    ELSE 0
                END
            )                                                   AS Ledger_WFQ_Influx

    FROM    Stockout_Detection d
    LEFT  JOIN WFQ_Supply w ON d.ITEMNMBR = w.ITEMNMBR
    GROUP BY
            d.ITEMNMBR,
            d.Demand_Seq,
            d.DUEDATE,
            d.Adjusted_Running_Balance,
            d.Stockout_Seq
),

-- ============================================================================
-- Extended_Demand: Demand rows enriched with WFQ influx and extended balance
-- ============================================================================
Extended_Demand AS
(
    SELECT
            d.*,
            ISNULL(w.Ledger_WFQ_Influx, 0)                     AS Ledger_WFQ_Influx,

            -- Extended balance = ledger balance + WFQ supply arriving in time
            d.Adjusted_Running_Balance + ISNULL(w.Ledger_WFQ_Influx, 0)
                                                                AS Ledger_Extended_Balance,

            CASE
                WHEN ISNULL(w.Ledger_WFQ_Influx, 0) <= 0
                     THEN 'LEDGER_ONLY'
                WHEN d.Adjusted_Running_Balance <= 0
                 AND (d.Adjusted_Running_Balance + ISNULL(w.Ledger_WFQ_Influx, 0)) > 0
                     THEN 'WFQ_RESCUED'
                WHEN (d.Adjusted_Running_Balance + ISNULL(w.Ledger_WFQ_Influx, 0)) > 0
                     THEN 'WFQ_ENHANCED'
                ELSE     'WFQ_INSUFFICIENT'
            END                                                 AS WFQ_Extended_Status

    FROM    Stockout_Detection d
    LEFT  JOIN WFQ_Allocated w
           ON  d.ITEMNMBR  = w.ITEMNMBR
           AND d.Demand_Seq = w.Demand_Seq
),

-- ============================================================================
-- Final_Ledger: Merge Extended_Demand back onto full Demand_Ledger
-- ============================================================================
Final_Ledger AS
(
    SELECT
            l.*,
            ISNULL(ed.Ledger_WFQ_Influx, 0)                    AS Ledger_WFQ_Influx,

            CASE WHEN ed.ITEMNMBR IS NULL
                 THEN NULL
                 ELSE ed.Ledger_Extended_Balance
            END                                                 AS Ledger_Extended_Balance,

            CASE WHEN LTRIM(RTRIM(l.ORDERNUMBER)) = 'Beg Bal'
                     THEN 'BEGINNING BALANCE'
                 WHEN ed.ITEMNMBR IS NULL
                     THEN 'NON_DEMAND_LEDGER_ROW'
                 ELSE ed.WFQ_Extended_Status
            END                                                 AS WFQ_Extended_Status

    FROM    Demand_Ledger l
    LEFT  JOIN Extended_Demand ed
           ON  l.ITEMNMBR    = ed.ITEMNMBR
           AND l.DUEDATE     = ed.DUEDATE
           AND l.ORDERNUMBER = ed.ORDERNUMBER
           AND COALESCE(l.Unified_Value, '') = COALESCE(ed.Unified_Value, '')
)

-- ============================================================================
-- Output: View 4 columns — consumed by View 5 (ETB_PAB_SUPPLY_ACTION)
-- ============================================================================
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
    Adjusted_Running_Balance,
    MRP_IssueDate,
    WCID_From_MO,
    WC_Site_Validation,
    Issued,
    Original_Required,
    Net_Demand,
    Inventory_Qty_Available,
    Suppression_Status,
    Last_Cycle_Count_Date,          -- Issue 3
    Days_Since_Last_Cycle_Count,    -- Issue 3
    Cycle_Count_Status,             -- Issue 3
    VendorItem,
    PRIME_VNDR,
    Vendor_Data_Source,             -- Issue 4
    PURCHASING_LT,
    PLANNING_LT,
    ORDER_POINT_QTY,
    SAFETY_STOCK,
    FG,
    [FG Desc],
    STSDESCR,
    MRPTYPE,
    Unified_Value,
    Ledger_WFQ_Influx,
    Ledger_Extended_Balance,
    WFQ_Extended_Status

FROM    Final_Ledger;
