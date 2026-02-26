/*
================================================================================
ETB_V_CLIENT_295_STOCKOUTS — Client 295 Stockout Detection View (View 8)
================================================================================
Purpose:
  Provide clear, actionable stockout signals for Construct 295 (Client 295)
  while filtering out WC inventory noise and other non-critical demand patterns.
  Aggregates demand across ALL customers sharing the same item/run, not just
  Client 295, to give a true market-wide picture of supply risk.

Design Principles:
  1. ONE clear stockout signal per item/run (YES/NO boolean via Is_Stockout)
  2. Aggregate demand = sum of ALL customer demand for that item/run
  3. Client demand = Client 295's demand only (individual client context)
  4. Shared_Demand_Ratio = Client295_Demand / Aggregate_Demand_All_Customers
  5. Flat, readable 4-step pipeline (no deeply nested CTEs)

Pattern A Suppression (enforced via Suppression_Status filter):
  - Excludes BEGINNING BALANCE rows (balance seed, not real demand)
  - Excludes SUPPRESSED: Stale & Unissued (stale MOs, zero issued)
  - Excludes SUPPRESSED: Full Coverage in Fence (WC inventory covers demand)
  - Excludes rows with NULL Demand_Due_Date (no actionable date)
  - Requires positive Net_Demand (genuine demand signal)

Pipeline:
  Step 1 — Client295_Demand     : Non-suppressed Client 295 demand rows
  Step 2 — AllCustomers_Demand  : All customers sharing same item/run as Step 1
           DistinctCustomers     : One row per customer per item/run (for STRING_AGG)
           DemandTotals          : Aggregate qty + first stockout date per item/run
           AggDemand_Summary     : Join totals + customer list
  Step 3 — Stockout_Detection   : Min balance per item/run → Is_Stockout flag
  Step 4 — Final_Output         : Combine stockout risk + vendor info + agg demand

Vendor Fallback Hierarchy (Issue 4 — UNASSIGNED added):
  Priority 1: dbo.ETB_SS_CALC   (safety stock calculation vendor — most reliable)
  Priority 2: dbo.ETB_PAB_SUPPLY_ACTION (supply action vendor)
  Priority 3: dbo.ETB_PAB_WFQ_ADJ      (WFQ adjustment vendor)
  Fallback:   'UNASSIGNED'              (all three sources NULL)

Source:
  dbo.ETB_PAB_SUPPLY_ACTION   — View 5 (authoritative demand + supply surface)
  dbo.ETB_PAB_WFQ_ADJ         — View 4 (vendor fallback — Priority 3)
  dbo.ETB_SS_CALC              — Safety stock calc (vendor fallback — Priority 1)

Note: ETB_RUN_RISK (View 6) and ETB_BUYER_CONTROL (View 7) have been REMOVED
from the pipeline per user direction.  All references to those views have been
removed from this file.

Change Log:
  2026-02-26: Initial creation for Client 295 stockout monitoring.
  2026-02-26: Hardening — removed references to Views 6/7 (ETB_RUN_RISK,
              ETB_BUYER_CONTROL); added UNASSIGNED vendor fallback (Issue 4);
              added Data_Quality_Flag (Issue 5); updated header documentation
              to reflect 6-view pipeline (Issues 10, removed-views cleanup).
================================================================================
*/

WITH

-- ============================================================================
-- STEP 1: Base Demand Data for Client 295
-- ============================================================================
-- Pull all non-suppressed, positive-demand rows for Construct 295.
-- Run_Bucket uses ISO year-week (e.g. "2026-W09") for grouping.
-- Pattern A suppression is enforced via Suppression_Status exclusions.
-- ============================================================================
Client295_Demand AS
(
    SELECT
        -- Item identification
        ITEMNMBR,
        ItemDescription,
        UOM,

        -- Vendor information (Issue 4: PRIME_VNDR may now be 'UNASSIGNED')
        PRIME_VNDR,
        VendorItem                                              AS Vendor_Item_Number,

        -- Data quality from View 5 (Issue 5)
        Data_Quality_Flag,

        -- Order / construct context
        Construct,
        ORDERNUMBER                                             AS MO_Number,

        -- Dates and balance
        Demand_Due_Date                                         AS DUEDATE,
        Net_Demand                                              AS QTY,
        Adjusted_Running_Balance,
        Deficit_Qty,
        Suppression_Status,
        WFQ_Extended_Status,

        -- Run bucketing: ISO year-week (e.g. "2026-W09")
        DATEPART(YEAR,     Demand_Due_Date)                     AS Demand_Year,
        DATEPART(ISO_WEEK, Demand_Due_Date)                     AS Demand_Week,
        CONCAT(
            DATEPART(YEAR, Demand_Due_Date),
            '-W',
            RIGHT('0' + CAST(DATEPART(ISO_WEEK, Demand_Due_Date) AS varchar(2)), 2)
        )                                                       AS Run_Bucket

    FROM    dbo.ETB_PAB_SUPPLY_ACTION

    WHERE
        -- Client 295 only
        Construct = '295'

        -- Exclude Pattern A noise (suppression audit trail from View 2/5)
        AND Suppression_Status NOT IN (
                'BEGINNING BALANCE',
                'SUPPRESSED: Stale & Unissued',
                'SUPPRESSED: Full Coverage in Fence'
            )

        -- Require a valid due date
        AND Demand_Due_Date IS NOT NULL

        -- Genuine positive demand only
        AND Net_Demand > 0
),

-- ============================================================================
-- STEP 2a: All Customers' Demand for the Same Item/Run Combinations
-- ============================================================================
-- Pulls ALL customers' demand for every item/week that appears in Step 1.
-- We do NOT filter to Construct = '295' here — full market picture.
-- The WHERE EXISTS subquery limits scope to item/runs relevant to Client 295.
-- ============================================================================
AllCustomers_Demand AS
(
    SELECT
        sa.ITEMNMBR,

        -- Run bucketing — must exactly match Step 1 bucketing logic
        DATEPART(YEAR,     sa.Demand_Due_Date)                  AS Demand_Year,
        DATEPART(ISO_WEEK, sa.Demand_Due_Date)                  AS Demand_Week,
        CONCAT(
            DATEPART(YEAR, sa.Demand_Due_Date),
            '-W',
            RIGHT('0' + CAST(DATEPART(ISO_WEEK, sa.Demand_Due_Date) AS varchar(2)), 2)
        )                                                       AS Run_Bucket,

        sa.Construct,
        sa.Net_Demand                                           AS QTY,
        sa.Adjusted_Running_Balance,
        sa.Demand_Due_Date

    FROM    dbo.ETB_PAB_SUPPLY_ACTION sa

    -- Scope: only item/run combinations relevant to Client 295
    WHERE   EXISTS (
                SELECT 1
                FROM   Client295_Demand c
                WHERE  c.ITEMNMBR    = sa.ITEMNMBR
                  AND  c.Demand_Year = DATEPART(YEAR,     sa.Demand_Due_Date)
                  AND  c.Demand_Week = DATEPART(ISO_WEEK, sa.Demand_Due_Date)
            )

            -- Apply the same Pattern A suppression to all customers
            AND sa.Suppression_Status NOT IN (
                    'BEGINNING BALANCE',
                    'SUPPRESSED: Stale & Unissued',
                    'SUPPRESSED: Full Coverage in Fence'
                )

            AND sa.Demand_Due_Date IS NOT NULL

            AND sa.Net_Demand > 0
),

-- ============================================================================
-- STEP 2b: Distinct customer list per item/run (pre-dedup for STRING_AGG)
-- ============================================================================
-- STRING_AGG(DISTINCT ...) requires SQL Server 2022+.
-- This pattern (dedup first, then STRING_AGG over already-unique rows) works
-- on SQL Server 2016+ and avoids the compatibility issue.
-- ============================================================================
DistinctCustomers AS
(
    SELECT DISTINCT
        ITEMNMBR,
        Demand_Year,
        Demand_Week,
        Run_Bucket,
        Construct
    FROM    AllCustomers_Demand
),

-- ============================================================================
-- STEP 2c: Demand totals per item/run (all rows — quantities and balance)
-- ============================================================================
DemandTotals AS
(
    SELECT
        ITEMNMBR,
        Demand_Year,
        Demand_Week,
        Run_Bucket,
        SUM(QTY)                                                AS Aggregate_Demand_All_Customers,
        MIN(CASE WHEN Adjusted_Running_Balance < 0
                 THEN Demand_Due_Date END)                      AS First_Stockout_Date_All,
        MIN(Adjusted_Running_Balance)                           AS Min_Balance_All_Customers
    FROM    AllCustomers_Demand
    GROUP BY
        ITEMNMBR,
        Demand_Year,
        Demand_Week,
        Run_Bucket
),

-- ============================================================================
-- STEP 2d: Aggregate summary per item/run
-- ============================================================================
-- Joins DemandTotals (qty/balance) with DistinctCustomers (one row per customer)
-- so STRING_AGG operates over pre-deduplicated rows.
-- ============================================================================
AggDemand_Summary AS
(
    SELECT
        dt.ITEMNMBR,
        dt.Demand_Year,
        dt.Demand_Week,
        dt.Run_Bucket,

        -- Total market demand for this item/run
        dt.Aggregate_Demand_All_Customers,

        -- Count and list of distinct customers sharing this item/run
        COUNT(dc.Construct)                                     AS Customer_Count,

        -- Comma-separated customer list (safe: DistinctCustomers is pre-deduped)
        STRING_AGG(dc.Construct, ', ')
            WITHIN GROUP (ORDER BY dc.Construct)                AS Affected_Customers,

        -- All-customer stockout metrics
        dt.First_Stockout_Date_All,
        dt.Min_Balance_All_Customers

    FROM    DemandTotals dt
    INNER JOIN DistinctCustomers dc
               ON  dc.ITEMNMBR    = dt.ITEMNMBR
               AND dc.Demand_Year = dt.Demand_Year
               AND dc.Demand_Week = dt.Demand_Week

    GROUP BY
        dt.ITEMNMBR,
        dt.Demand_Year,
        dt.Demand_Week,
        dt.Run_Bucket,
        dt.Aggregate_Demand_All_Customers,
        dt.First_Stockout_Date_All,
        dt.Min_Balance_All_Customers
),

-- ============================================================================
-- STEP 3: Stockout Detection per Item/Run
-- ============================================================================
-- Stockout is determined by whether the minimum Adjusted_Running_Balance
-- (driven by ALL customers' demand against shared supply) goes negative.
-- Uses Client 295's own demand rows since they share the same running balance.
-- ============================================================================
Stockout_Detection AS
(
    SELECT
        c.ITEMNMBR,
        c.Run_Bucket,
        c.Demand_Year,
        c.Demand_Week,

        -- First date within this run where any demand row goes negative
        MIN(CASE WHEN c.Adjusted_Running_Balance < 0
                 THEN c.DUEDATE END)                            AS First_Stockout_Date,

        -- Balance range within this run
        MAX(c.Adjusted_Running_Balance)                         AS Max_Balance_In_Run,
        MIN(c.Adjusted_Running_Balance)                         AS Min_Balance_In_Run,

        -- Stockout flag: YES if balance ever goes negative within this run
        CASE WHEN MIN(c.Adjusted_Running_Balance) < 0
             THEN 'YES'
             ELSE 'NO'
        END                                                     AS Is_Stockout,

        -- Stockout quantity: absolute depth of the worst deficit
        CASE WHEN MIN(c.Adjusted_Running_Balance) < 0
             THEN ABS(MIN(c.Adjusted_Running_Balance))
             ELSE 0
        END                                                     AS Stockout_Qty,

        -- WFQ dependency: was WFQ supply active for any row in this run?
        MAX(CASE WHEN c.WFQ_Extended_Status IN ('WFQ_RESCUED', 'WFQ_ENHANCED', 'WFQ_INSUFFICIENT')
                 THEN 1 ELSE 0 END)                             AS WFQ_Dependency_In_Run

    FROM    Client295_Demand c
    GROUP BY
        c.ITEMNMBR,
        c.Run_Bucket,
        c.Demand_Year,
        c.Demand_Week
),

-- ============================================================================
-- STEP 4: Final Assembly — Vendor Info + Full Aggregate Demand Context
-- ============================================================================
-- Joins Stockout_Detection (item/run risk) to Client295_Demand for client
-- quantities, then LEFT JOIN AggDemand_Summary for market-wide demand.
--
-- Vendor fallback hierarchy (Issue 4):
--   Priority 1: dbo.ETB_SS_CALC    (safety stock calc — most stable vendor ref)
--   Priority 2: Client295_Demand   (PRIME_VNDR from View 5 / ETB_PAB_SUPPLY_ACTION)
--   Priority 3: dbo.ETB_PAB_WFQ_ADJ (WFQ adjustment vendor)
--   Fallback:   'UNASSIGNED'        (Issue 4 — ensures no NULL vendor in output)
--
-- Note: ETB_RUN_RISK and ETB_BUYER_CONTROL (Views 6 and 7) are REMOVED from
-- the pipeline. The fallback chain has been updated to exclude those views.
-- ============================================================================
Final_Output AS
(
    SELECT
        -- ----------------------------------------------------------------
        -- Item Identification
        -- ----------------------------------------------------------------
        sd.ITEMNMBR,
        MAX(c.ItemDescription)                                  AS Item_Description,
        MAX(c.UOM)                                              AS UOM,

        -- ----------------------------------------------------------------
        -- Run Identification
        -- ----------------------------------------------------------------
        sd.Run_Bucket,
        sd.Demand_Year,
        sd.Demand_Week,
        sd.First_Stockout_Date,

        -- ----------------------------------------------------------------
        -- Demand Quantities
        -- ----------------------------------------------------------------
        -- Client 295's demand for this item/run
        SUM(c.QTY)                                              AS Client295_Demand,

        -- Total market demand across ALL customers for this item/run
        MAX(agg.Aggregate_Demand_All_Customers)                 AS Aggregate_Demand_All_Customers,

        -- Ratio: share of total demand belonging to Client 295
        -- NULL-safe: returns NULL when aggregate is 0 (avoids divide-by-zero)
        CAST(
            CASE
                WHEN MAX(agg.Aggregate_Demand_All_Customers) > 0
                THEN SUM(c.QTY)
                     / CAST(MAX(agg.Aggregate_Demand_All_Customers) AS decimal(18, 6))
                ELSE NULL
            END
        AS decimal(10, 6))                                      AS Shared_Demand_Ratio,

        -- ----------------------------------------------------------------
        -- Stockout Metrics
        -- ----------------------------------------------------------------
        sd.Is_Stockout,
        sd.Stockout_Qty,
        sd.Max_Balance_In_Run,
        sd.Min_Balance_In_Run,

        -- ----------------------------------------------------------------
        -- Customer Impact (ALL customers, not just 295)
        -- ----------------------------------------------------------------
        MAX(agg.Customer_Count)                                 AS Customer_Count,
        MAX(agg.Affected_Customers)                             AS Affected_Customers,

        -- Shared demand flag: YES if more than one customer shares this item/run
        CASE WHEN MAX(agg.Customer_Count) > 1 THEN 'YES' ELSE 'NO'
        END                                                     AS Shared_Demand_Flag,

        -- ----------------------------------------------------------------
        -- Vendor Information (Issue 4: UNASSIGNED fallback added)
        -- Views 6 and 7 are REMOVED — fallback chain now ends at ETB_PAB_WFQ_ADJ
        -- ----------------------------------------------------------------
        COALESCE(
            MAX(ss.PRIME_VNDR),     -- Priority 1: Safety stock calc vendor
            MAX(c.PRIME_VNDR),      -- Priority 2: Supply action vendor (may be UNASSIGNED)
            MAX(wfq.PRIME_VNDR),    -- Priority 3: WFQ adjustment vendor
            'UNASSIGNED'            -- Issue 4: explicit fallback (no more NULLs)
        )                                                       AS Primary_Vendor,

        MAX(c.Vendor_Item_Number)                               AS Vendor_Item_Number,

        -- ----------------------------------------------------------------
        -- WFQ Context
        -- ----------------------------------------------------------------
        MAX(sd.WFQ_Dependency_In_Run)                           AS WFQ_Dependency_Flag,

        -- ----------------------------------------------------------------
        -- Data Quality (Issue 5)
        -- ----------------------------------------------------------------
        -- Worst data quality flag across all Client 295 rows for this item/run
        MAX(c.Data_Quality_Flag)                                AS Data_Quality_Flag,

        -- ----------------------------------------------------------------
        -- Metadata
        -- ----------------------------------------------------------------
        CAST(GETDATE() AS date)                                 AS Analysis_Date,
        'CLIENT_295_STOCKOUT_MONITOR'                           AS Report_Type

    FROM         Stockout_Detection sd

    -- Join back to Client 295 rows for demand quantities and vendor info
    INNER JOIN   Client295_Demand c
                 ON  c.ITEMNMBR   = sd.ITEMNMBR
                 AND c.Run_Bucket = sd.Run_Bucket

    -- LEFT JOIN aggregate summary (item/run may have only Client 295 demand)
    LEFT  JOIN   AggDemand_Summary agg
                 ON  agg.ITEMNMBR   = sd.ITEMNMBR
                 AND agg.Run_Bucket = sd.Run_Bucket

    -- LEFT JOIN safety stock for vendor fallback (Priority 1)
    LEFT  JOIN   dbo.ETB_SS_CALC ss
                 ON  ss.ITEMNMBR   = sd.ITEMNMBR

    -- LEFT JOIN WFQ adj for vendor fallback (Priority 3)
    -- Note: ETB_RUN_RISK (View 6) and ETB_BUYER_CONTROL (View 7) are REMOVED.
    -- This is now the final fallback before 'UNASSIGNED'.
    LEFT  JOIN   dbo.ETB_PAB_WFQ_ADJ wfq
                 ON  wfq.ITEMNMBR  = sd.ITEMNMBR

    GROUP BY
        sd.ITEMNMBR,
        sd.Run_Bucket,
        sd.Demand_Year,
        sd.Demand_Week,
        sd.First_Stockout_Date,
        sd.Is_Stockout,
        sd.Stockout_Qty,
        sd.Max_Balance_In_Run,
        sd.Min_Balance_In_Run,
        sd.WFQ_Dependency_In_Run
)

-- ============================================================================
-- Final SELECT — Buyer-facing output, ordered by urgency
-- ============================================================================
SELECT
    -- Item Identification
    ITEMNMBR,
    Item_Description,
    UOM,

    -- Run Identification
    Run_Bucket,
    Demand_Year,
    Demand_Week,
    First_Stockout_Date,

    -- Demand Quantities
    Client295_Demand,
    Aggregate_Demand_All_Customers,
    Shared_Demand_Ratio,

    -- Stockout Metrics
    Is_Stockout,
    Stockout_Qty,
    Max_Balance_In_Run,
    Min_Balance_In_Run,

    -- Customer Impact
    Customer_Count,
    Affected_Customers,
    Shared_Demand_Flag,

    -- Vendor Information (Issue 4: will never be NULL — UNASSIGNED fallback)
    Primary_Vendor,
    Vendor_Item_Number,

    -- Data Quality (Issue 5)
    Data_Quality_Flag,

    -- Metadata
    Analysis_Date,
    Report_Type

FROM    Final_Output

ORDER BY
    -- Confirmed stockouts first
    CASE WHEN Is_Stockout = 'YES' THEN 0 ELSE 1 END,

    -- Most severe stockout (largest shortage) within confirmed stockouts
    Stockout_Qty DESC,

    -- Then by earliest run bucket (soonest risk first)
    Run_Bucket,

    -- Alphabetical item within same run bucket
    ITEMNMBR;
