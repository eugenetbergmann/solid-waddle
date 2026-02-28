-- ============================================================================
-- VIEW: ETB_RALPH_LOOP_37D
-- Purpose: 37-Day Horizon Universal View — item-level stockout, client-level
--          stockout, program flags (291/295/298), and ETB supply coverage
--          columns in a single Excel-ready flat table.
-- Author: Zo Computer
-- Date: 2026-02-28
-- Dependencies: dbo.ETB_PAB_SUPPLY_ACTION (View 5)
--               dbo.CustomerMap
-- ============================================================================
/*
================================================================================
ETB_RALPH_LOOP_37D — Ralph Loop 37-Day Horizon Universal View (View 9)
================================================================================
Purpose:
  Produces a single, flat, Excel-ready table covering every item that has
  demand within the next 37 calendar days.  Integrates:

    1. Item-level stockout flag + first deficit date
    2. Client-level stockout flag (any customer sharing the item)
    3. Program presence flags — Is291, Is295, Is298
    4. ETB supply coverage — worst PO coverage status, WFQ status, max deficit

  One row per item.  All columns are plain scalars — no heatmaps, no nested
  XML, no JSON.  Paste the result set directly into Excel or export as CSV.

Horizon:
  @HorizonStart = CAST(GETDATE() AS DATE)          -- today
  @HorizonEnd   = DATEADD(DAY, 37, @HorizonStart)  -- today + 37 days

  All demand rows whose Demand_Due_Date falls within [HorizonStart, HorizonEnd]
  are included.  Rows outside the window are excluded.

Source / Dependency:
  dbo.ETB_PAB_SUPPLY_ACTION  — View 5 (authoritative demand + supply surface).
    Columns consumed:
      ITEMNMBR, ItemDescription, Inventory_Qty_Available,
      Adjusted_Running_Balance, Deficit_Qty, Demand_Due_Date,
      Construct, PO_Coverage_Status (derived below), WFQ_Extended_Status
  dbo.CustomerMap            — maps ITEMNMBR → CustomerID for client stockout.

CTE Pipeline:
  Config            → Horizon dates + named constants
  ItemBase          → Non-suppressed demand rows inside horizon from View 5
  ItemStockout      → Binary stockout flag + first deficit date per item
  ClientStockout    → Binary stockout flag per (CustomerID, ITEMNMBR)
  ProgramFlags      → Is291 / Is295 / Is298 presence flags per item
  ETB_Status        → Worst PO rank + worst WFQ rank + max deficit per item
  ETB_Status_Mapped → Numeric ranks decoded back to string labels
  ItemDistinct      → One representative row per item (for OnHand / MinBal)
  Final             → Left-join assembly → ORDER BY urgency

PO_Coverage_Status derivation (inline, from View 5 columns):
  The upstream ETB_PAB_SUPPLY_ACTION view does not emit a single
  PO_Coverage_Status column.  This view derives it from the combination of
  Deficit_Qty and POs_On_Order_Qty that View 5 already exposes:

    NO_PO_ON_ORDER    — Deficit_Qty > 0 AND POs_On_Order_Qty = 0
    PO_PARTIAL_COVER  — Deficit_Qty > 0 AND 0 < POs_On_Order_Qty < Deficit_Qty
    PO_COVERS_DEFICIT — Deficit_Qty > 0 AND POs_On_Order_Qty >= Deficit_Qty
    NO_DEFICIT        — Deficit_Qty = 0 (supply sufficient without PO action)

Output Columns (one row per ITEMNMBR):
  ITEMNMBR              — Item number
  ItemDescription       — Item description
  OnHand                — WC bin inventory available (from View 5)
  MinBal                — Minimum Adjusted_Running_Balance inside horizon
  FirstDeficit          — Earliest date balance goes negative (NULL if none)
  ItemStockout          — 1 if any row inside horizon has negative balance
  ClientStockout        — 1 if any customer mapped to this item has a deficit
  Is291                 — 1 if item appears in Construct 291 demand
  Is295                 — 1 if item appears in Construct 295 demand
  Is298                 — 1 if item appears in Construct 298 demand
  ETB_PO_Status         — Worst PO coverage status across horizon rows
  ETB_WFQ_Status        — Worst WFQ extended status across horizon rows
  ETB_Deficit_37D       — Maximum Deficit_Qty across horizon rows

Ordering:
  ItemStockout DESC, ETB_Deficit_37D DESC, ITEMNMBR ASC
  (Stockouts first, then by severity, then alphabetical.)

Change Log:
  2026-02-28: Initial creation — Ralph Loop 37-Day Horizon Universal View.
              Integrates all program flags, item/client stockout, ETB supply
              coverage into single Excel-ready flat table.
================================================================================
*/

WITH

-- ============================================================================
-- Config: Horizon dates and named constants
-- ============================================================================
-- All horizon arithmetic is centralised here.  Change the 37 in one place.
-- ============================================================================
Config AS (
    SELECT
        -- Horizon window
        CAST(GETDATE() AS date)                         AS HorizonStart,
        DATEADD(DAY, 37, CAST(GETDATE() AS date))       AS HorizonEnd,

        -- PO coverage rank sentinels (higher = worse)
        3   AS PO_Rank_No_PO,
        2   AS PO_Rank_Partial,
        1   AS PO_Rank_Covers,
        0   AS PO_Rank_No_Deficit,

        -- WFQ rank sentinels (higher = worse)
        3   AS WFQ_Rank_Rescued,
        2   AS WFQ_Rank_Unknown,
        1   AS WFQ_Rank_Ledger_Only,
        0   AS WFQ_Rank_Default,

        -- Pattern A suppression labels (must match View 5 output exactly)
        'BEGINNING BALANCE'                             AS Status_Beg_Bal,
        'SUPPRESSED: Stale & Unissued'                  AS Status_Stale,
        'SUPPRESSED: Full Coverage in Fence'            AS Status_Fence
),

-- ============================================================================
-- ItemBase: Non-suppressed demand rows inside the 37-day horizon
-- ============================================================================
-- Pulls from dbo.ETB_PAB_SUPPLY_ACTION (View 5).
-- Excludes Pattern A suppression noise (same logic as View 8).
-- Requires a valid Demand_Due_Date within the horizon window.
-- ============================================================================
ItemBase AS (
    SELECT
        sa.ITEMNMBR,
        sa.ItemDescription,
        sa.Inventory_Qty_Available,
        sa.Adjusted_Running_Balance,
        sa.Deficit_Qty,
        sa.Demand_Due_Date,
        sa.Construct,
        sa.WFQ_Extended_Status,
        sa.POs_On_Order_Qty,

        -- Derive PO_Coverage_Status from Deficit_Qty + POs_On_Order_Qty
        CASE
            WHEN ISNULL(sa.Deficit_Qty, 0) = 0
                 THEN 'NO_DEFICIT'
            WHEN ISNULL(sa.POs_On_Order_Qty, 0) = 0
                 THEN 'NO_PO_ON_ORDER'
            WHEN sa.POs_On_Order_Qty < sa.Deficit_Qty
                 THEN 'PO_PARTIAL_COVER'
            ELSE     'PO_COVERS_DEFICIT'
        END                                             AS PO_Coverage_Status

    FROM dbo.ETB_PAB_SUPPLY_ACTION sa
    CROSS JOIN Config cfg

    WHERE
        -- Horizon filter
        sa.Demand_Due_Date BETWEEN cfg.HorizonStart AND cfg.HorizonEnd

        -- Exclude Pattern A suppression noise
        AND sa.Suppression_Status NOT IN (
                cfg.Status_Beg_Bal,
                cfg.Status_Stale,
                cfg.Status_Fence
            )

        -- Require a valid due date (belt-and-suspenders — already in WHERE above)
        AND sa.Demand_Due_Date IS NOT NULL

        -- Genuine positive demand only
        AND ISNULL(sa.Net_Demand, 0) > 0
),

-- ============================================================================
-- ItemStockout: Binary stockout flag + first deficit date per item
-- ============================================================================
-- ItemStockout = 1 if ANY demand row inside the horizon has a negative
-- Adjusted_Running_Balance.  FirstDeficit = earliest such date.
-- ============================================================================
ItemStockout AS (
    SELECT
        ITEMNMBR,
        MAX(CASE WHEN Adjusted_Running_Balance < 0 THEN 1 ELSE 0 END)
                                                        AS ItemStockout,
        MIN(CASE WHEN Adjusted_Running_Balance < 0
                 THEN Demand_Due_Date
                 ELSE NULL
            END)                                        AS FirstDeficit
    FROM ItemBase
    GROUP BY ITEMNMBR
),

-- ============================================================================
-- ClientStockout: Binary stockout flag per (CustomerID, ITEMNMBR)
-- ============================================================================
-- Joins ItemBase to dbo.CustomerMap to resolve CustomerID.
-- ClientStockout = 1 if any demand row for that customer/item pair is negative.
-- The final SELECT aggregates this to item level (MAX across all customers).
-- ============================================================================
ClientStockout AS (
    SELECT
        c.CustomerID,
        ib.ITEMNMBR,
        MAX(CASE WHEN ib.Adjusted_Running_Balance < 0 THEN 1 ELSE 0 END)
                                                        AS ClientStockout
    FROM ItemBase ib
    INNER JOIN dbo.CustomerMap c
           ON  c.ItemNMBR = ib.ITEMNMBR
    GROUP BY c.CustomerID, ib.ITEMNMBR
),

-- ============================================================================
-- ProgramFlags: Is291 / Is295 / Is298 presence flags per item
-- ============================================================================
-- Binary: 1 if the item appears in at least one demand row for that Construct
-- inside the horizon, 0 otherwise.
-- ============================================================================
ProgramFlags AS (
    SELECT
        ITEMNMBR,
        MAX(CASE WHEN TRY_CAST(Construct AS int) = 291 THEN 1 ELSE 0 END)
                                                        AS Is291,
        MAX(CASE WHEN TRY_CAST(Construct AS int) = 295 THEN 1 ELSE 0 END)
                                                        AS Is295,
        MAX(CASE WHEN TRY_CAST(Construct AS int) = 298 THEN 1 ELSE 0 END)
                                                        AS Is298
    FROM ItemBase
    GROUP BY ITEMNMBR
),

-- ============================================================================
-- ETB_Status: Worst PO rank + worst WFQ rank + max deficit per item
-- ============================================================================
-- Uses numeric rank sentinels from Config so the CASE logic is self-documenting
-- and the mapping back to strings in ETB_Status_Mapped is unambiguous.
-- ============================================================================
ETB_Status AS (
    SELECT
        ib.ITEMNMBR,

        -- Worst PO coverage status (highest rank = most severe)
        MAX(
            CASE ib.PO_Coverage_Status
                WHEN 'NO_PO_ON_ORDER'    THEN cfg.PO_Rank_No_PO
                WHEN 'PO_PARTIAL_COVER'  THEN cfg.PO_Rank_Partial
                WHEN 'PO_COVERS_DEFICIT' THEN cfg.PO_Rank_Covers
                ELSE                          cfg.PO_Rank_No_Deficit
            END
        )                                               AS PO_Rank,

        -- Worst WFQ extended status (highest rank = most severe)
        MAX(
            CASE ib.WFQ_Extended_Status
                WHEN 'WFQ_RESCUED'   THEN cfg.WFQ_Rank_Rescued
                WHEN 'UNKNOWN'       THEN cfg.WFQ_Rank_Unknown
                WHEN 'LEDGER_ONLY'   THEN cfg.WFQ_Rank_Ledger_Only
                ELSE                      cfg.WFQ_Rank_Default
            END
        )                                               AS WFQ_Rank,

        -- Maximum deficit depth across all horizon rows for this item
        MAX(ISNULL(ib.Deficit_Qty, 0))                  AS MaxDeficit

    FROM ItemBase ib
    CROSS JOIN Config cfg
    GROUP BY ib.ITEMNMBR
),

-- ============================================================================
-- ETB_Status_Mapped: Decode numeric ranks back to string labels
-- ============================================================================
ETB_Status_Mapped AS (
    SELECT
        ITEMNMBR,

        CASE PO_Rank
            WHEN 3 THEN 'NO_PO_ON_ORDER'
            WHEN 2 THEN 'PO_PARTIAL_COVER'
            WHEN 1 THEN 'PO_COVERS_DEFICIT'
            ELSE        'NO_DEFICIT'
        END                                             AS ETB_PO_Status,

        CASE WFQ_Rank
            WHEN 3 THEN 'WFQ_RESCUED'
            WHEN 2 THEN 'UNKNOWN'
            WHEN 1 THEN 'LEDGER_ONLY'
            ELSE        'UNKNOWN'
        END                                             AS ETB_WFQ_Status,

        MaxDeficit                                      AS ETB_Deficit_37D

    FROM ETB_Status
),

-- ============================================================================
-- ItemDistinct: One representative row per item for OnHand and MinBal
-- ============================================================================
-- Inventory_Qty_Available is item-level (same value on every row for an item).
-- MinBal = minimum Adjusted_Running_Balance across all horizon rows.
-- ============================================================================
ItemDistinct AS (
    SELECT
        ITEMNMBR,
        MAX(ItemDescription)                            AS ItemDescription,
        MAX(Inventory_Qty_Available)                    AS OnHand,
        MIN(Adjusted_Running_Balance)                   AS MinBal
    FROM ItemBase
    GROUP BY ITEMNMBR
)

-- ============================================================================
-- Final SELECT — one row per item, all columns, Excel-ready
-- ============================================================================
SELECT
    -- Item identification
    id.ITEMNMBR,
    id.ItemDescription,

    -- Supply position
    id.OnHand,
    id.MinBal,

    -- Stockout signals
    ist.FirstDeficit,
    ISNULL(ist.ItemStockout, 0)                         AS ItemStockout,
    ISNULL(MAX(cs.ClientStockout), 0)                   AS ClientStockout,

    -- Program presence flags
    ISNULL(pf.Is291, 0)                                 AS Is291,
    ISNULL(pf.Is295, 0)                                 AS Is295,
    ISNULL(pf.Is298, 0)                                 AS Is298,

    -- ETB supply coverage
    ISNULL(es.ETB_PO_Status,  'UNKNOWN')                AS ETB_PO_Status,
    ISNULL(es.ETB_WFQ_Status, 'UNKNOWN')                AS ETB_WFQ_Status,
    ISNULL(es.ETB_Deficit_37D, 0)                       AS ETB_Deficit_37D,

    -- Metadata
    CAST(GETDATE() AS date)                             AS Analysis_Date,
    'RALPH_LOOP_37D'                                    AS Report_Type

FROM         ItemDistinct id

LEFT JOIN    ItemStockout ist
             ON  ist.ITEMNMBR = id.ITEMNMBR

LEFT JOIN    ClientStockout cs
             ON  cs.ITEMNMBR  = id.ITEMNMBR

LEFT JOIN    ProgramFlags pf
             ON  pf.ITEMNMBR  = id.ITEMNMBR

LEFT JOIN    ETB_Status_Mapped es
             ON  es.ITEMNMBR  = id.ITEMNMBR

GROUP BY
    id.ITEMNMBR,
    id.ItemDescription,
    id.OnHand,
    id.MinBal,
    ist.FirstDeficit,
    ist.ItemStockout,
    pf.Is291,
    pf.Is295,
    pf.Is298,
    es.ETB_PO_Status,
    es.ETB_WFQ_Status,
    es.ETB_Deficit_37D

ORDER BY
    -- Confirmed stockouts first
    ISNULL(ist.ItemStockout, 0) DESC,

    -- Most severe deficit within stockouts
    ISNULL(es.ETB_Deficit_37D, 0) DESC,

    -- Alphabetical item within same severity tier
    id.ITEMNMBR ASC;
