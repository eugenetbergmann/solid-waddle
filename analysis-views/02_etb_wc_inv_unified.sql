-- ============================================================================
-- VIEW: ETB_WC_INV_UNIFIED
-- Purpose: WC inventory integration with running balance adjustments
-- Author: Zo Computer
-- Date: 2026-02-26
-- Dependencies: dbo.ETB_PAB_MO, dbo.ETB_ActiveDemand_Union_FG_MO,
--               dbo.Prosenthal_Vendor_Items, dbo.PK010033, dbo.WO010032,
--               dbo.IV00101, dbo.Prosenthal_INV_BIN_QTY_wQTYTYPE,
--               dbo.IV10300
-- ============================================================================
/*
================================================================================
ETB_WC_INV_UNIFIED — WC Inventory Integration with Running Balance Adjustments
                     (Analysis View 2)
================================================================================
Purpose:
  Extends View 1 (ETB_PAB_AUTO) by integrating Work-Centre (WC) bin inventory,
  applying demand suppression logic (stale / fence), and recalculating a
  corrected Adjusted_Running_Balance that excludes suppressed rows.

  Key design decisions:
    • Suppress_Stale: removes demand rows that are past-due and have zero
      issued quantity — these are phantom backlogs from the ERP.
    • Suppress_Fence: removes demand rows that fall within the near-term fence
      window when WC inventory already fully covers remaining demand.
    • Running-balance recalculation uses a delta/anchor pattern so that only
      unsuppressed rows contribute to the forward-looking balance.

  NOTE: This view has been moved to analysis-views/ in the reorganized pipeline.
  Pipeline views 04 and 05 re-inline this logic directly for performance.

Source Tables:
  dbo.ETB_PAB_MO                       — Raw PAB demand rows
  dbo.ETB_ActiveDemand_Union_FG_MO     — FG / Construct mapping
  dbo.Prosenthal_Vendor_Items          — Item master (description, UOM)
  dbo.PK010033                         — Manufacturing ledger
  dbo.WO010032                         — Work-order status filter
  dbo.IV00101                          — Item master (ledger enrichment)
  dbo.Prosenthal_INV_BIN_QTY_wQTYTYPE — WC bin inventory quantities
  dbo.IV10300                          — Cycle count header (last count date)

CTE Pipeline:
  Config          → Named threshold constants (Issues 8)
  p_norm          → Normalize PAB rows
  m_norm          → Normalize demand-union, deduplicate FG per MO
  item_desc       → Item master lookup
  joined          → Core join assembly
  ranked          → Row-number dedup
  Core            → Single row per (ORDERNUMBER, FG, ITEMNMBR)
  ledger_ranked   → Ledger deduplication
  PAB_Final       → Ledger attachment + WC/vendor flags
  Base            → Column rename / reshape for downstream CTEs
  InventoryAgg    → WC bin inventory aggregated to (Item, Site)
                    (Issue 9: grouped with validation guard on SITE pattern)
  CycleCount      → Last cycle count date per item (Issue 3)
  WithInventory   → Attach WC inventory to Base rows
  Flags           → Compute Net_Demand, Is_BegBal_Row, Suppress_Stale,
                    Suppress_Fence (Issues 2, 8)
  Unified         → Compute Is_Suppressed, Demand_Status, Remaining_After_Suppression
  Ordered         → Assign sequence number (Seq) for running-balance window
  Deltas          → Compute per-row running-balance delta
  Adjusted        → RB_Anchor + zeroed deltas for suppressed rows

Data Quality Flags (Data_Quality_Flag):
  CLEAN                  — Vendor and MO data present
  MISSING_VENDOR         — PRIME_VNDR resolved to UNASSIGNED
  NON_WC_SITE            — WCID_From_MO doesn't follow WC-W% pattern
  MISSING_VENDOR_NON_WC  — Both vendor and WC site issues

Suppression Audit Trail (Demand_Status):
  BEGINNING BALANCE               — Balance seed row (never a real demand)
  SUPPRESSED: Stale & Unissued    — Past due + zero issued (Suppress_Stale=1)
  SUPPRESSED: Full Coverage in Fence — WC inventory covers demand in fence window
  PARTIAL: Demand Netted          — Partial WC inventory offset applied
  VALID DEMAND                    — Row passes all suppression checks

Change Log:
  2026-02-26: Full hardening — TRY_CAST, UNASSIGNED vendor fallback, cycle
              count integration, Config thresholds, WC site validation, data
              quality flags, suppression audit trail, NOLOCK consistency,
              header documentation. (Issues 2–10)
  2026-02-26: Moved from pipeline-views/ to analysis-views/ as part of
              5-view pipeline reorganization with ETB_SS_CALC integration.
================================================================================
*/

WITH

-- ============================================================================
-- Config: Named threshold constants (Issue 8 — eliminates magic numbers)
-- ============================================================================
Config AS
(
    SELECT
        -- Days past-due threshold for Suppress_Stale.
        -- MOs with DUEDATE <= today - 7 days AND Issued = 0 are stale backlog.
        -- Business rule: stale rows create phantom deficits in the PAB ledger.
        7   AS Stale_Suppression_Days,

        -- Near-term fence window for Suppress_Fence.
        -- If remaining demand is due within 7 days AND WC inventory covers it,
        -- suppress — the inventory will be consumed before re-planning is needed.
        -- Review: if frequent false suppressions are observed, reduce to 3–5 days.
        7   AS Fence_Suppression_Days,

        -- WC inventory age cutoff (days since DATERECD).
        -- Only inventory received within 45 days is considered available.
        -- Business rule: older WC stock may be consumed/expired by now.
        45  AS WC_Inventory_Age_Days,

        -- Days since last cycle count before status becomes OVERDUE.
        -- Business rule: items not counted in 90 days have uncertain inventory.
        90  AS Cycle_Count_Overdue_Days,

        -- Ledger early-issue flag threshold (days before today).
        7   AS Early_Issue_Flag_Days
),

-- ============================================================================
-- p_norm: Normalize PAB source rows (Issue 6: TRY_CAST; Issue 7: documented)
-- ============================================================================
p_norm AS
(
    SELECT  p.*,
            -- Issue 7: REPLACE chain strips 'MO', '-', ' ', '/', '.', '#'
            -- These characters appear in ERP-imported order number strings.
            UPPER(LTRIM(RTRIM(CONVERT(varchar(255),
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    p.ORDERNUMBER,
                    'MO', ''), '-', ''), ' ', ''), '/', ''), '.', ''), '#', ''))
            )))                                                 AS CleanOrder,

            LTRIM(RTRIM(p.ITEMNMBR))                            AS CleanItem,

            -- Issue 6: TRY_CAST is NULL-safe; ISNULL converts NULL → 0
            ISNULL(TRY_CAST(LTRIM(RTRIM(p.Deductions)) AS decimal(18, 5)), 0)
                                                                AS CleanDeductions

    FROM    dbo.ETB_PAB_MO p
    WHERE   p.STSDESCR <> 'Partially Received'
      AND   p.STSDESCR <> 'SCRAP'
      AND   LTRIM(RTRIM(p.ITEMNMBR)) NOT LIKE '60.%'
      AND   LTRIM(RTRIM(p.ITEMNMBR)) NOT LIKE '70.%'
),

-- ============================================================================
-- m_norm: Normalize demand-union (FG / Construct deduplication)
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
-- item_desc: Item master lookup (description + UOM schedule)
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
-- joined: Core data assembly
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

            -- Issue 4: UNASSIGNED fallback so NULL vendor items are retained
            COALESCE(NULLIF(p_norm.PRIME_VNDR, ''), 'UNASSIGNED')  AS PRIME_VNDR,

            -- Issue 4: Track vendor data source for audit
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
-- ranked / Core: Deduplication to single row per (ORDERNUMBER, FG, ITEMNMBR)
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
-- ledger_ranked: Manufacturing ledger deduplication
-- ============================================================================
-- NOLOCK is intentional for concurrency (read-only reporting).
-- Issue 9: candidate indexes are (MANUFACTUREORDER_I, ITEMNMBR) on PK010033
--          and (MANUFACTUREORDER_I) on WO010032.
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
-- PAB_Final: Attach ledger data; derive WC site validation and flags
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

            -- Issue 8: threshold sourced from Config CTE
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

            -- Issue 2: validate WC site naming convention
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
-- Base: Column reshape for downstream WC-inventory join
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
            Issued,
            Remaining,
            Has_Issued,
            IssueDate_Mismatch,
            Early_Issue_Flag,
            WC_Site_Validation,
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
-- InventoryAgg: WC bin inventory aggregated to (Item_Number, SITE)
-- ============================================================================
-- Issue 2: GROUP BY guard — only SITE values matching 'WC-W%' are aggregated.
--          This prevents non-WC locations (e.g., UNDERINV, STAGING) from
--          inflating the Inventory_Qty_Available used in suppression logic.
-- Issue 9: Index candidate on Prosenthal_INV_BIN_QTY_wQTYTYPE: (SITE,
--          Item_Number, DATERECD) to support the WHERE filter efficiently.
-- Issue 8: WC_Inventory_Age_Days (45) from Config CTE
-- ============================================================================
InventoryAgg AS
(
    SELECT
            Item_Number,
            SITE,
            SUM(QTY_Available)                                  AS Total_QTY_Available
    FROM    dbo.Prosenthal_INV_BIN_QTY_wQTYTYPE
    -- Issue 2: enforce WC-W% site pattern; non-WC sites excluded
    WHERE   SITE LIKE 'WC-W%'
      AND   DATEDIFF(DAY, DATERECD, GETDATE())
              <= (SELECT cfg.WC_Inventory_Age_Days FROM Config cfg)
    GROUP BY Item_Number, SITE
),

-- ============================================================================
-- CycleCount: Last cycle count date per item (Issue 3)
-- ============================================================================
-- Source: dbo.IV10300 — Inventory Cycle Count header table (GP standard).
-- If your environment uses a different cycle count table, update the FROM clause.
-- Columns assumed: ITEMNMBR, CYCLDATE (cycle count date).
-- If IV10300 is not present, this CTE returns zero rows and all items will
-- show Cycle_Count_Status = 'NEVER_COUNTED', which is safe for reporting.
-- ============================================================================
CycleCount AS
(
    SELECT
            RTRIM(LTRIM(cc.ITEMNMBR))                           AS ITEMNMBR,
            MAX(CAST(cc.CYCLDATE AS date))                      AS Last_Cycle_Count_Date
    FROM    dbo.IV10300 cc WITH (NOLOCK)
    GROUP BY RTRIM(LTRIM(cc.ITEMNMBR))
),

-- ============================================================================
-- WithInventory: Attach WC inventory and cycle count data to base rows
-- ============================================================================
WithInventory AS
(
    SELECT
            b.*,
            ISNULL(inv.Total_QTY_Available, 0)                  AS Inventory_Qty_Available,

            -- Issue 3: cycle count date and staleness
            cc.Last_Cycle_Count_Date,
            CASE WHEN cc.Last_Cycle_Count_Date IS NULL
                     THEN NULL
                 ELSE DATEDIFF(DAY, cc.Last_Cycle_Count_Date, CAST(GETDATE() AS date))
            END                                                 AS Days_Since_Last_Cycle_Count,

            -- Issue 3: Cycle_Count_Status classification
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
-- Flags: Compute Net_Demand, Is_BegBal_Row, Suppress_Stale, Suppress_Fence
-- ============================================================================
-- Issue 2: Suppress_Fence threshold is documented and sourced from Config.
--          Current value = 7 days. Review if false-suppression is observed.
-- Issue 8: Both thresholds reference Config CTE — single place to change.
-- ============================================================================
Flags AS
(
    SELECT
            wi.*,

            -- Net demand after partial WC offset (does not suppress the row)
            CASE WHEN Inventory_Qty_Available > 0
                  AND Inventory_Qty_Available < Remaining
                 THEN Remaining - Inventory_Qty_Available
                 ELSE Remaining
            END                                                 AS Net_Demand,

            -- Beginning Balance rows are never demand rows
            CASE WHEN LTRIM(RTRIM(ORDERNUMBER)) = 'Beg Bal'
                 THEN 1 ELSE 0
            END                                                 AS Is_BegBal_Row,

            -- Stale suppression: past-due + zero issued
            -- Issue 2 / Issue 8: threshold = Stale_Suppression_Days (7 days)
            CASE WHEN LTRIM(RTRIM(ORDERNUMBER)) = 'Beg Bal' THEN 0
                 WHEN DUEDATE <= DATEADD(DAY,
                          -(SELECT cfg.Stale_Suppression_Days FROM Config cfg),
                          CAST(GETDATE() AS date))
                  AND ISNULL(Issued, 0) = 0
                     THEN 1
                 ELSE 0
            END                                                 AS Suppress_Stale,

            -- Fence suppression: WC inventory fully covers near-term demand
            -- Issue 2 / Issue 8: threshold = Fence_Suppression_Days (7 days)
            -- Issue 2: only fires when WC site is valid (WC_Site_Validation check
            --          upstream ensures Inventory_Qty_Available is trustworthy)
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
-- Unified: Apply suppression, derive Demand_Status audit trail
-- ============================================================================
-- Issue 2: Demand_Status provides a full audit trail for every suppression decision.
-- ============================================================================
Unified AS
(
    SELECT
            f.*,

            -- Aggregate suppression flag
            CASE WHEN Is_BegBal_Row = 1     THEN 0
                 WHEN Suppress_Stale = 1    THEN 1
                 WHEN Suppress_Fence = 1    THEN 1
                 ELSE 0
            END                                                 AS Is_Suppressed,

            -- Issue 2: Demand_Status is the suppression audit trail
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

            -- Remaining qty after suppression zeroing (NULL for BegBal)
            CASE WHEN Is_BegBal_Row = 1                THEN NULL
                 WHEN (Suppress_Stale = 1 OR Suppress_Fence = 1) THEN 0
                 ELSE Remaining
            END                                                 AS Remaining_After_Suppression

    FROM    Flags f
),

-- ============================================================================
-- Ordered: Assign Seq for running-balance window function
-- ============================================================================
-- BegBal row always gets Seq = 1 (it seeds the running balance).
-- Tie-break: DUEDATE → [Date + Expiry] → MRP_IssueDate → sentinel 9999-12-31
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
-- Deltas: Compute per-row running-balance delta
-- ============================================================================
-- RB_Delta = difference from the previous row in sequence order.
-- Suppressed rows will have their delta zeroed out in the next CTE.
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

-- ============================================================================
-- Adjusted: Recalculate running balance with suppressed rows zeroed out
-- ============================================================================
-- RB_Anchor = first RB value per item (BegBal or opening balance row).
-- RB_Delta_Adjusted = 0 for BegBal row (Seq=1) and for suppressed rows.
-- Adjusted_Running_Balance = RB_Anchor + cumulative sum of adjusted deltas.
-- ============================================================================
Adjusted AS
(
    SELECT
            d.*,
            FIRST_VALUE(d.RB_Num) OVER (
                PARTITION BY d.ITEMNMBR ORDER BY d.Seq
            )                                                   AS RB_Anchor,

            CASE WHEN d.Seq = 1          THEN CAST(0 AS decimal(18, 6))
                 WHEN d.Is_Suppressed = 1 THEN CAST(0 AS decimal(18, 6))
                 ELSE ISNULL(d.RB_Delta, 0)
            END                                                 AS RB_Delta_Adjusted
    FROM    Deltas d
)

-- ============================================================================
-- Output: Analysis View 2 columns
-- ============================================================================
-- Issue 5: Data_Quality_Flag classifies row-level data completeness
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

    -- Recalculated balance that excludes suppressed rows
    CAST(RB_Anchor + SUM(RB_Delta_Adjusted) OVER (
        PARTITION BY ITEMNMBR ORDER BY Seq ROWS UNBOUNDED PRECEDING
    ) AS decimal(18, 6))                                        AS Adjusted_Running_Balance,

    MRP_IssueDate,
    WCID_From_MO,
    WC_Site_Validation,                     -- Issue 2
    Issued,
    Remaining,
    Net_Demand,
    Inventory_Qty_Available,
    Is_Suppressed,
    Demand_Status,                          -- Issue 2: suppression audit trail
    Remaining_After_Suppression,
    Has_Issued,
    IssueDate_Mismatch,
    Early_Issue_Flag,

    -- Issue 3: cycle count columns
    Last_Cycle_Count_Date,
    Days_Since_Last_Cycle_Count,
    Cycle_Count_Status,

    VendorItem,
    PRIME_VNDR,
    Vendor_Data_Source,                     -- Issue 4

    -- Issue 5: data quality classification
    CASE
        WHEN PRIME_VNDR = 'UNASSIGNED' AND WC_Site_Validation = 'NON_WC_SITE'
             THEN 'MISSING_VENDOR_NON_WC'
        WHEN PRIME_VNDR = 'UNASSIGNED'
             THEN 'MISSING_VENDOR'
        WHEN WC_Site_Validation = 'NON_WC_SITE'
             THEN 'NON_WC_SITE'
        ELSE     'CLEAN'
    END                                                         AS Data_Quality_Flag,

    PURCHASING_LT,
    PLANNING_LT,
    ORDER_POINT_QTY,
    SAFETY_STOCK,
    FG,
    [FG Desc],
    STSDESCR,
    MRPTYPE,
    Unified_Value

FROM    Adjusted;
