-- ============================================================================
-- VIEW: ETB_PAB_SUPPLY_ACTION
-- Purpose: Supply action recommendations and PO timing analysis
-- Author: Zo Computer
-- Date: 2026-02-26
-- Dependencies: dbo.ETB_PAB_MO, dbo.ETB_ActiveDemand_Union_FG_MO,
--               dbo.Prosenthal_Vendor_Items, dbo.PK010033, dbo.WO010032,
--               dbo.IV00101, dbo.Prosenthal_INV_BIN_QTY_wQTYTYPE,
--               dbo.IV10300, dbo.ETB_WFQ_PIPE (View 3)
-- ============================================================================
/*
================================================================================
ETB_PAB_SUPPLY_ACTION — Supply Action Recommendations and PO Timing (View 5)
================================================================================
Purpose:
  Builds on View 4 (ETB_PAB_WFQ_ADJ) to compute supply action recommendations
  for each demand row.  Determines whether existing POs on order are sufficient,
  on time, or whether additional procurement is required.  Provides urgency
  classification for buyer workflow prioritization.

  This is the primary buyer-facing decision surface for the pipeline.
  View 8 (ETB_V_CLIENT_295_STOCKOUTS) consumes this view for Client 295 analysis.

Source / Dependency:
  This view re-materializes all of Views 1–4 inline (same CTE chain).
  dbo.ETB_WFQ_PIPE is consumed for WFQ turnaround analysis.

CTE Pipeline:
  Config                   → Named threshold constants (Issue 8)
  p_norm … Final_Ledger    → Full pipeline through View 4 logic
  WFQ_Extended             → Reshaped Final_Ledger output
  Balance_Analysis         → Compute Deficit_Qty, POs_On_Order_Qty, Demand_Due_Date
  WFQ_Turnaround_Analysis  → PO turnaround days per item from WFQ pipe
  PO_Timing_Analysis       → Attach turnaround data; derive PO_On_Time flag
  Supply_Action_Decision   → Final recommendation: SUFFICIENT/ORDER/BOTH/REVIEW_REQUIRED

Supply_Action_Recommendation Values:
  SUFFICIENT       — Extended balance covers demand (WFQ + existing POs)
  ORDER            — Deficit exists and either no POs or existing POs are late
  BOTH             — Partial PO coverage; both existing POs and new order needed
  REVIEW_REQUIRED  — Edge case not covered by explicit rules

Data Quality Flags (Data_Quality_Flag):
  CLEAN                   — Vendor and cost data present
  MISSING_VENDOR          — PRIME_VNDR resolved to UNASSIGNED
  MISSING_COST            — POs_On_Order_Qty could not be parsed
  MISSING_BOTH            — Vendor and cost data missing
  NON_WC_SITE             — WCID_From_MO doesn't follow WC-W% pattern

Change Log:
  2026-02-26: Full hardening — Config thresholds (added Urgent_PO_Days,
              Warning_PO_Days, WFQ_Priority_Window_Days), TRY_CAST, UNASSIGNED
              vendor, WC site validation, data quality flags, cycle counts,
              NOLOCK consistency, documentation. (Issues 2–10)
================================================================================
*/

WITH

-- ============================================================================
-- Config: Named threshold constants (Issue 8)
-- ============================================================================
Config AS
(
    SELECT
        7   AS Stale_Suppression_Days,         -- Stale suppression: days past due
        7   AS Fence_Suppression_Days,          -- Fence suppression: forward window
        45  AS WC_Inventory_Age_Days,           -- WC bin inventory age cutoff
        90  AS Cycle_Count_Overdue_Days,        -- Cycle count overdue threshold
        7   AS Early_Issue_Flag_Days,           -- Early issue detection threshold
        5   AS Urgent_PO_Days,                  -- Days to stockout for urgent status
        10  AS Warning_PO_Days,                 -- Days to stockout for warning status
        3   AS WFQ_Priority_Window_Days         -- Days for WFQ priority allocation
),

-- ============================================================================
-- p_norm: Normalize PAB rows (Issue 6: TRY_CAST; Issue 7: REPLACE chain docs)
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
-- m_norm: Normalize demand-union
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
                            'MO', ''), '-', ''), ' ', ''), '/', ''), '.', ''), '#', '')))))
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
-- ranked / Core: Deduplicate
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
-- ledger_ranked: Manufacturing ledger (Issue 9: NOLOCK)
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

            CASE WHEN ISNULL(ml.WCID_I, '') = ''   THEN 'NO_WC'
                 WHEN ml.WCID_I LIKE 'WC-W%'       THEN 'VALID_WC'
                 ELSE                                    'NON_WC_SITE'
            END                                             AS WC_Site_Validation

    FROM    Core
    LEFT  JOIN ledger_ranked ml
           ON  Core.CleanOrder   = ml.CleanMO
           AND Core.CleanItem    = ml.ITEMNMBR
           AND ml.rn_any = 1  -- Single deterministic match per MO+Item (fixes 3x row duplication bug)
),

-- ============================================================================
-- Base: Column reshape
-- ============================================================================
Base AS
(
    SELECT
            ITEMNMBR, ItemDescription,
            UOMSCHDL AS UOM,
            ORDERNUMBER, Construct, DUEDATE, [Expiry Dates], [Date + Expiry],
            CAST(BEG_BAL_Num AS varchar(50)) AS BEG_BAL,
            Original_Deductions AS Deductions, Original_Expiry AS Expiry,
            Original_POs AS [PO's], Original_Running_Balance AS Running_Balance,
            MRP_IssueDate, WCID_From_MO, WC_Site_Validation,
            Issued, Remaining, Has_Issued, IssueDate_Mismatch, Early_Issue_Flag,
            VendorItem, PRIME_VNDR, Vendor_Data_Source,
            PURCHASING_LT, PLANNING_LT, ORDER_POINT_QTY, SAFETY_STOCK,
            FG, [FG Desc], STSDESCR, MRPTYPE, Unified_Value
    FROM    PAB_Final
),

-- ============================================================================
-- InventoryAgg: WC bin inventory (Issue 2: WC-W% enforced; Issue 8: Config age)
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
-- ============================================================================
CycleCount AS
(
    SELECT
            RTRIM(LTRIM(cc.ITEMNMBR))   AS ITEMNMBR,
            MAX(CAST(cc.CYCLDATE AS date)) AS Last_Cycle_Count_Date
    FROM    dbo.IV10300 cc WITH (NOLOCK)
    GROUP BY RTRIM(LTRIM(cc.ITEMNMBR))
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
-- Unified: Suppression and audit trail
-- ============================================================================
Unified AS
(
    SELECT
            f.*,
            CASE WHEN Is_BegBal_Row = 1  THEN 0
                 WHEN Suppress_Stale = 1 THEN 1
                 WHEN Suppress_Fence = 1 THEN 1
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
            END                                                 AS Demand_Status,

            CASE WHEN Is_BegBal_Row = 1                THEN NULL
                 WHEN (Suppress_Stale = 1 OR Suppress_Fence = 1) THEN 0
                 ELSE Remaining
            END                                                 AS Remaining_After_Suppression
    FROM    Flags f
),

-- ============================================================================
-- Ordered, Deltas, Adjusted: Running balance recalculation
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
-- ETB_WC_INV: Reshaped Adjusted with computed Adjusted_Running_Balance
-- ============================================================================
ETB_WC_INV AS
(
    SELECT
            ITEMNMBR, ItemDescription, UOM, ORDERNUMBER, Construct,
            DUEDATE, [Expiry Dates], [Date + Expiry], BEG_BAL,
            Deductions, Expiry, [PO's], Running_Balance,
            CAST(RB_Anchor + SUM(RB_Delta_Adjusted) OVER (
                PARTITION BY ITEMNMBR ORDER BY Seq ROWS UNBOUNDED PRECEDING
            ) AS decimal(18, 6))                                AS Adjusted_Running_Balance,
            MRP_IssueDate, WCID_From_MO, WC_Site_Validation,
            Issued, Remaining, Net_Demand, Inventory_Qty_Available,
            Is_Suppressed,
            Demand_Status                                       AS Suppression_Status,
            Remaining_After_Suppression, Has_Issued, IssueDate_Mismatch, Early_Issue_Flag,
            Last_Cycle_Count_Date, Days_Since_Last_Cycle_Count, Cycle_Count_Status,
            VendorItem, PRIME_VNDR, Vendor_Data_Source,
            PURCHASING_LT, PLANNING_LT, ORDER_POINT_QTY, SAFETY_STOCK,
            FG, [FG Desc], STSDESCR, MRPTYPE, Unified_Value
    FROM    Adjusted
),

-- ============================================================================
-- Demand_Ledger / DemandRowsOnly / Demand_Seq / Stockout_Detection
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
DemandRowsOnly AS
(
    SELECT  *
    FROM    Demand_Ledger
    WHERE   LTRIM(RTRIM(ORDERNUMBER)) <> 'Beg Bal'
      AND   DUEDATE IS NOT NULL
      AND   ISNULL(Original_Required, 0) <> 0
),
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
-- WFQ_Supply: Aggregate WFQ by (ITEMNMBR, Estimated_Release_Date)
-- (Issue 1 fix: SITE excluded — see View 4 documentation)
-- ============================================================================
WFQ_Supply AS
(
    SELECT
            ITEM_Number                 AS ITEMNMBR,
            Estimated_Release_Date,
            SUM(QTY_ON_HAND)            AS WFQ_Qty
    FROM    dbo.ETB_WFQ_PIPE
    WHERE   View_Level  = 'ITEM_LEVEL'
      AND   QTY_ON_HAND > 0
    GROUP BY ITEM_Number, Estimated_Release_Date
),

-- ============================================================================
-- WFQ_Allocated: Per-demand-row WFQ influx (Issue 1 fix: 1:1 join guarantee)
-- ============================================================================
WFQ_Allocated AS
(
    SELECT
            d.ITEMNMBR,
            d.Demand_Seq,
            d.DUEDATE,
            d.Adjusted_Running_Balance  AS Ledger_Base_Balance,
            d.Stockout_Seq,
            SUM(
                CASE
                    WHEN d.Stockout_Seq IS NOT NULL
                     AND d.Demand_Seq  >= d.Stockout_Seq
                     AND w.Estimated_Release_Date <= d.DUEDATE
                    THEN w.WFQ_Qty
                    ELSE 0
                END
            )                           AS Ledger_WFQ_Influx
    FROM    Stockout_Detection d
    LEFT  JOIN WFQ_Supply w ON d.ITEMNMBR = w.ITEMNMBR
    GROUP BY
            d.ITEMNMBR, d.Demand_Seq, d.DUEDATE,
            d.Adjusted_Running_Balance,  d.Stockout_Seq
),

-- ============================================================================
-- Extended_Demand / Final_Ledger: WFQ status attachment
-- ============================================================================
Extended_Demand AS
(
    SELECT
            d.*,
            ISNULL(w.Ledger_WFQ_Influx, 0)                     AS Ledger_WFQ_Influx,
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
           ON  d.ITEMNMBR   = w.ITEMNMBR
           AND d.Demand_Seq = w.Demand_Seq
),
Final_Ledger AS
(
    SELECT
            l.*,
            ISNULL(ed.Ledger_WFQ_Influx, 0)                    AS Ledger_WFQ_Influx,
            CASE WHEN ed.ITEMNMBR IS NULL THEN NULL
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
),

-- ============================================================================
-- WFQ_Extended: Reshaped Final_Ledger for supply analysis
-- ============================================================================
WFQ_Extended AS
(
    SELECT
            ITEMNMBR, ItemDescription, UOM, ORDERNUMBER, Construct,
            DUEDATE, [Expiry Dates], [Date + Expiry], BEG_BAL,
            Deductions, Expiry, [PO's], Running_Balance, Adjusted_Running_Balance,
            MRP_IssueDate, WCID_From_MO, WC_Site_Validation,
            Issued, Original_Required, Net_Demand, Inventory_Qty_Available,
            Suppression_Status,
            Last_Cycle_Count_Date, Days_Since_Last_Cycle_Count, Cycle_Count_Status,
            VendorItem, PRIME_VNDR, Vendor_Data_Source,
            PURCHASING_LT, PLANNING_LT, ORDER_POINT_QTY, SAFETY_STOCK,
            FG, [FG Desc], STSDESCR, MRPTYPE, Unified_Value,
            Ledger_WFQ_Influx, Ledger_Extended_Balance, WFQ_Extended_Status
    FROM    Final_Ledger
),

-- ============================================================================
-- Balance_Analysis: Compute Deficit_Qty, safe-parse POs_On_Order_Qty
-- ============================================================================
-- Issue 5: POs_Can_Parse flag tracks rows where [PO's] could not be converted.
-- Issue 6: TRY_CAST replaces ISNUMERIC for [PO's] parsing.
-- ============================================================================
Balance_Analysis AS
(
    SELECT
            w.*,

            -- Deficit = how much demand the current extended balance cannot cover
            CASE WHEN ISNULL(w.Ledger_Extended_Balance, 0) >= ISNULL(w.Net_Demand, 0)
                 THEN 0
                 ELSE ISNULL(w.Net_Demand, 0) - ISNULL(w.Ledger_Extended_Balance, 0)
            END                                                 AS Deficit_Qty,

            -- Issue 6: TRY_CAST for [PO's]; NULL when unparseable
            ISNULL(
                CASE WHEN NULLIF(LTRIM(RTRIM(w.[PO's])), '') IS NULL THEN 0
                     ELSE TRY_CAST(LTRIM(RTRIM(w.[PO's])) AS decimal(18, 6))
                END,
                0
            )                                                   AS POs_On_Order_Qty,

            -- Issue 5: track whether [PO's] value was parseable
            CASE WHEN NULLIF(LTRIM(RTRIM(w.[PO's])), '') IS NULL THEN 'NULL'
                 WHEN TRY_CAST(LTRIM(RTRIM(w.[PO's])) AS decimal(18, 6)) IS NOT NULL
                      THEN 'PARSED'
                 ELSE 'UNPARSEABLE'
            END                                                 AS POs_Parse_Status,

            -- Effective due date: prefer DUEDATE, fall back to [Date + Expiry]
            COALESCE(w.DUEDATE, TRY_CAST(w.[Date + Expiry] AS date))
                                                                AS Demand_Due_Date
    FROM    WFQ_Extended w
),

-- ============================================================================
-- WFQ_Turnaround_Analysis: PO turnaround days per item from WFQ pipe
-- ============================================================================
WFQ_Turnaround_Analysis AS
(
    SELECT
            ITEM_Number                 AS ITEMNMBR,
            Estimated_Release_Date,
            CASE WHEN Estimated_Release_Date IS NOT NULL
                  AND Estimated_Release_Date >= CAST(GETDATE() AS date)
                 THEN DATEDIFF(DAY, CAST(GETDATE() AS date), Estimated_Release_Date)
                 ELSE 0
            END                         AS PO_Turn_Around_Days,
            Estimated_Release_Date      AS PO_Actual_Arrival_Date
    FROM    dbo.ETB_WFQ_PIPE
    WHERE   View_Level = 'ITEM_LEVEL'
),

-- ============================================================================
-- PO_Timing_Analysis: Attach WFQ turnaround; derive PO_On_Time and backlog flag
-- ============================================================================
PO_Timing_Analysis AS
(
    SELECT
            b.*,
            t.PO_Turn_Around_Days,
            t.PO_Actual_Arrival_Date,
            t.Estimated_Release_Date        AS PO_Release_Date,

            -- PO is on time if it arrives on or before demand due date
            CASE WHEN t.PO_Actual_Arrival_Date IS NOT NULL
                  AND b.Demand_Due_Date     IS NOT NULL
                  AND t.PO_Actual_Arrival_Date <= b.Demand_Due_Date
                 THEN 1 ELSE 0
            END                             AS PO_On_Time,

            -- Past-due demand rows that are still in backlog
            CASE WHEN b.Demand_Due_Date IS NOT NULL
                  AND b.Demand_Due_Date < CAST(GETDATE() AS date)
                 THEN 1 ELSE 0
            END                             AS Is_Past_Due_In_Backlog
    FROM    Balance_Analysis b
    LEFT  JOIN WFQ_Turnaround_Analysis t ON b.ITEMNMBR = t.ITEMNMBR
),

-- ============================================================================
-- Supply_Action_Decision: Final recommendation logic
-- ============================================================================
-- Decision matrix (evaluated in order):
--   SUFFICIENT   — Extended balance >= net demand (no gap)
--   ORDER        — Deficit > 0, no POs in system
--   ORDER        — Deficit > 0, POs exist but will not arrive on time
--   SUFFICIENT   — Deficit > 0, POs exist, quantity sufficient, and on time
--   BOTH         — Deficit > 0, POs cover partially (POs < Deficit)
--   REVIEW_REQUIRED — All other conditions
-- ============================================================================
Supply_Action_Decision AS
(
    SELECT
            p.*,
            CASE
                WHEN ISNULL(p.Ledger_Extended_Balance, 0) >= ISNULL(p.Net_Demand, 0)
                     THEN 'SUFFICIENT'
                WHEN p.Deficit_Qty > 0 AND p.POs_On_Order_Qty = 0
                     THEN 'ORDER'
                WHEN p.Deficit_Qty > 0 AND p.POs_On_Order_Qty >= p.Deficit_Qty
                  AND p.PO_On_Time = 0
                     THEN 'ORDER'
                WHEN p.Deficit_Qty > 0 AND p.POs_On_Order_Qty >= p.Deficit_Qty
                  AND p.PO_On_Time = 1
                     THEN 'SUFFICIENT'
                WHEN p.Deficit_Qty > 0 AND p.POs_On_Order_Qty > 0
                  AND p.POs_On_Order_Qty < p.Deficit_Qty
                     THEN 'BOTH'
                ELSE     'REVIEW_REQUIRED'
            END                                                 AS Supply_Action_Recommendation,

            -- Shortfall quantity even after accounting for existing POs
            CASE WHEN p.Deficit_Qty > 0 AND p.POs_On_Order_Qty < p.Deficit_Qty
                 THEN p.Deficit_Qty - p.POs_On_Order_Qty
                 ELSE 0
            END                                                 AS Additional_Order_Qty,

            -- Issue 5: Data quality flag incorporating vendor, cost, and WC validation
            CASE
                WHEN p.PRIME_VNDR = 'UNASSIGNED' AND p.POs_Parse_Status = 'UNPARSEABLE'
                     THEN 'MISSING_BOTH'
                WHEN p.PRIME_VNDR = 'UNASSIGNED'
                     THEN 'MISSING_VENDOR'
                WHEN p.POs_Parse_Status = 'UNPARSEABLE'
                     THEN 'MISSING_COST'
                WHEN p.WC_Site_Validation = 'NON_WC_SITE'
                     THEN 'NON_WC_SITE'
                ELSE     'CLEAN'
            END                                                 AS Data_Quality_Flag
    FROM    PO_Timing_Analysis p
)

-- ============================================================================
-- Output: View 5 columns — consumed by View 8 (ETB_V_CLIENT_295_STOCKOUTS)
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
    WC_Site_Validation,             -- Issue 2
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
    Data_Quality_Flag,              -- Issue 5
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
    WFQ_Extended_Status,
    Deficit_Qty,
    POs_On_Order_Qty,
    POs_Parse_Status,               -- Issue 5
    Demand_Due_Date,
    PO_Release_Date,
    PO_Turn_Around_Days,
    PO_Actual_Arrival_Date,
    PO_On_Time,
    Is_Past_Due_In_Backlog,
    Supply_Action_Recommendation,
    Additional_Order_Qty

FROM    Supply_Action_Decision;
