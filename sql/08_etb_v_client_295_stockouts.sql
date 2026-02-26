/*
================================================================================
ETB_V_CLIENT_295_STOCKOUTS — Client 295 Stockout Detection View
================================================================================
Purpose: Provide clear, actionable stockout signals for Construct 295 (Client 295)
         while filtering out WC inventory noise and other non-critical demand
         patterns. Aggregates demand across ALL customers sharing the same
         item/run, not just Client 295.

Design Principles:
  1. ONE clear stockout signal per item/run (YES/NO boolean)
  2. Aggregate demand = sum of ALL customer demand for that item/run
  3. Customer demand = Client 295's demand only (individual client context)
  4. Shared_Demand_Ratio = Client295_Demand / Aggregate_Demand_All_Customers
  5. No nested CTEs — flat, readable 4-step pipeline

Pattern A Suppression (applied via Suppression_Status):
  - Excludes BEGINNING BALANCE rows (balance seed, not real demand)
  - Excludes SUPPRESSED: Stale & Unissued (stale MOs with zero issued)
  - Excludes SUPPRESSED: Full Coverage in Fence (WC inventory covers demand)
  - Excludes rows with NULL Demand_Due_Date (no actionable date)
  - Requires positive Net_Demand (genuine demand signal)

Source: dbo.ETB_PAB_SUPPLY_ACTION (View 5 — the authoritative decision surface)

Dependencies:
  - dbo.ETB_PAB_SUPPLY_ACTION   (View 5 — demand ledger + stockout signals)
  - dbo.ETB_PAB_WFQ_ADJ         (for vendor fallback)
  - dbo.ETB_SS_CALC              (for safety stock / lead time data)
  - dbo.Prosenthal_Vendor_Items  (for item descriptions — already in View 5)

Change Log:
  - 2026-02-26: Initial creation for Client 295 stockout monitoring
================================================================================
*/

WITH

-- ============================================================================
-- STEP 1: Base Demand Data for Client 295
-- ============================================================================
-- Pull all non-suppressed, positive-demand rows for Construct 295.
-- Calculates the week bucket (Run_Bucket) for run-level grouping.
-- Pattern A suppression is enforced via Suppression_Status exclusions.
-- ============================================================================
Client295_Demand AS
(
    SELECT
        -- Item identification
        ITEMNMBR,
        ItemDescription,
        UOM,

        -- Vendor information
        PRIME_VNDR,
        VendorItem                                          AS Vendor_Item_Number,

        -- Order / construct context
        Construct,
        ORDERNUMBER                                         AS MO_Number,

        -- Dates and balance
        Demand_Due_Date                                     AS DUEDATE,
        Net_Demand                                          AS QTY,
        Adjusted_Running_Balance,
        Deficit_Qty,
        Suppression_Status,
        WFQ_Extended_Status,

        -- Run bucketing: ISO year-week (e.g. "2026-W09")
        DATEPART(YEAR,  Demand_Due_Date)                    AS Demand_Year,
        DATEPART(ISO_WEEK, Demand_Due_Date)                 AS Demand_Week,
        CONCAT(
            DATEPART(YEAR, Demand_Due_Date),
            '-W',
            RIGHT('0' + CAST(DATEPART(ISO_WEEK, Demand_Due_Date) AS varchar(2)), 2)
        )                                                   AS Run_Bucket

    FROM    dbo.ETB_PAB_SUPPLY_ACTION

    WHERE
        -- Client 295 only
        Construct = '295'

        -- Exclude Pattern A noise
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
-- STEP 2: Aggregate Demand Across ALL Customers per Item/Run (CRITICAL)
-- ============================================================================
-- Pull ALL customers' demand for every item/week combination that appears in
-- Client295_Demand. This gives us the true market-wide demand picture.
-- We do NOT filter to Construct = '295' here.
-- ============================================================================
AllCustomers_Demand AS
(
    SELECT
        sa.ITEMNMBR,

        -- Run bucketing — must match Step 1 bucketing logic exactly
        DATEPART(YEAR, sa.Demand_Due_Date)                  AS Demand_Year,
        DATEPART(ISO_WEEK, sa.Demand_Due_Date)              AS Demand_Week,
        CONCAT(
            DATEPART(YEAR, sa.Demand_Due_Date),
            '-W',
            RIGHT('0' + CAST(DATEPART(ISO_WEEK, sa.Demand_Due_Date) AS varchar(2)), 2)
        )                                                   AS Run_Bucket,

        sa.Construct,
        sa.Net_Demand                                       AS QTY,
        sa.Adjusted_Running_Balance,
        sa.Demand_Due_Date

    FROM    dbo.ETB_PAB_SUPPLY_ACTION sa

    -- Only include item/run combinations that are relevant to Client 295
    -- (prevents aggregating unrelated items that 295 has no stake in)
    WHERE   EXISTS (
                SELECT 1
                FROM   Client295_Demand c
                WHERE  c.ITEMNMBR    = sa.ITEMNMBR
                  AND  c.Demand_Year = DATEPART(YEAR, sa.Demand_Due_Date)
                  AND  c.Demand_Week = DATEPART(ISO_WEEK, sa.Demand_Due_Date)
            )

            -- Same Pattern A suppression applied to all customers
            AND sa.Suppression_Status NOT IN (
                    'BEGINNING BALANCE',
                    'SUPPRESSED: Stale & Unissued',
                    'SUPPRESSED: Full Coverage in Fence'
                )

            AND sa.Demand_Due_Date IS NOT NULL

            AND sa.Net_Demand > 0
),

-- ============================================================================
-- Distinct customer list per item/run
-- Pre-deduplication step required because STRING_AGG(DISTINCT ...) is only
-- supported in SQL Server 2022+. This pattern (dedup first, then STRING_AGG
-- over already-unique rows) works on SQL Server 2016+ and mirrors the
-- STRING_AGG usage in ETB_RUN_RISK (which also operates on pre-distinct rows
-- via DISTINCT inside threatened_clients_detail).
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
-- Demand totals per item/run (quantities and balance, all rows)
-- ============================================================================
DemandTotals AS
(
    SELECT
        ITEMNMBR,
        Demand_Year,
        Demand_Week,
        Run_Bucket,
        SUM(QTY)                                            AS Aggregate_Demand_All_Customers,
        MIN(CASE WHEN Adjusted_Running_Balance < 0
                 THEN Demand_Due_Date END)                  AS First_Stockout_Date_All,
        MIN(Adjusted_Running_Balance)                       AS Min_Balance_All_Customers
    FROM    AllCustomers_Demand
    GROUP BY
        ITEMNMBR,
        Demand_Year,
        Demand_Week,
        Run_Bucket
),

-- ============================================================================
-- Aggregate summary per item/run across ALL customers
-- Combines demand totals (from DemandTotals) with customer list (from
-- DistinctCustomers) so STRING_AGG runs over exactly one row per customer.
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

        -- Count of distinct customers sharing this item/run
        COUNT(dc.Construct)                                 AS Customer_Count,

        -- Comma-separated list of ALL affected customers
        -- (safe: DistinctCustomers already has one row per customer per run)
        STRING_AGG(dc.Construct, ', ')
            WITHIN GROUP (ORDER BY dc.Construct)            AS Affected_Customers,

        -- Earliest stockout date and worst balance (all-customer view)
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
-- STEP 3: Stockout Detection per Item/Run (Using AGGREGATE Demand)
-- ============================================================================
-- For each item/run, determine whether a stockout occurs by examining whether
-- the minimum Adjusted_Running_Balance (which is driven by ALL customers'
-- demand against shared supply) ever goes negative.
-- ============================================================================
Stockout_Detection AS
(
    SELECT
        c.ITEMNMBR,
        c.Run_Bucket,
        c.Demand_Year,
        c.Demand_Week,

        -- First date any demand row goes negative within this run
        MIN(CASE WHEN c.Adjusted_Running_Balance < 0
                 THEN c.DUEDATE END)                        AS First_Stockout_Date,

        -- Balance range within this run
        MAX(c.Adjusted_Running_Balance)                     AS Max_Balance_In_Run,
        MIN(c.Adjusted_Running_Balance)                     AS Min_Balance_In_Run,

        -- Stockout flag: YES if the running balance ever goes negative
        CASE WHEN MIN(c.Adjusted_Running_Balance) < 0
             THEN 'YES'
             ELSE 'NO'
        END                                                 AS Is_Stockout,

        -- Stockout quantity: absolute value of the deepest negative balance
        CASE WHEN MIN(c.Adjusted_Running_Balance) < 0
             THEN ABS(MIN(c.Adjusted_Running_Balance))
             ELSE 0
        END                                                 AS Stockout_Qty,

        -- WFQ dependency: did WFQ supply affect any row in this run?
        MAX(CASE WHEN c.WFQ_Extended_Status IN ('WFQ_RESCUED', 'WFQ_ENHANCED', 'WFQ_INSUFFICIENT')
                 THEN 1 ELSE 0 END)                         AS WFQ_Dependency_In_Run

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
-- Join Stockout_Detection (item/run-level risk) to Client295_Demand for
-- client-specific quantities, then LEFT JOIN AggDemand_Summary for the
-- full market-wide picture. Vendor fallback mirrors the pattern used in
-- ETB_RUN_RISK and ETB_BUYER_CONTROL.
-- ============================================================================
Final_Output AS
(
    SELECT
        -- ----------------------------------------------------------------
        -- Item Identification
        -- ----------------------------------------------------------------
        sd.ITEMNMBR,
        MAX(c.ItemDescription)                              AS Item_Description,
        MAX(c.UOM)                                          AS UOM,

        -- ----------------------------------------------------------------
        -- Run Identification
        -- ----------------------------------------------------------------
        sd.Run_Bucket,
        sd.Demand_Year,
        sd.Demand_Week,
        sd.First_Stockout_Date,

        -- ----------------------------------------------------------------
        -- Demand Quantities (ENHANCED)
        -- ----------------------------------------------------------------

        -- This client's demand for this item/run
        SUM(c.QTY)                                         AS Client295_Demand,

        -- Total market demand across ALL customers for this item/run
        MAX(agg.Aggregate_Demand_All_Customers)            AS Aggregate_Demand_All_Customers,

        -- Ratio: how much of total demand does Client 295 represent?
        -- NULL-safe: returns NULL when aggregate is 0 (avoids divide-by-zero)
        CAST(
            CASE
                WHEN MAX(agg.Aggregate_Demand_All_Customers) > 0
                THEN SUM(c.QTY)
                     / CAST(MAX(agg.Aggregate_Demand_All_Customers) AS decimal(18, 6))
                ELSE NULL
            END
        AS decimal(10, 6))                                 AS Shared_Demand_Ratio,

        -- ----------------------------------------------------------------
        -- Stockout Metrics
        -- ----------------------------------------------------------------
        sd.Is_Stockout,
        sd.Stockout_Qty,
        sd.Max_Balance_In_Run,
        sd.Min_Balance_In_Run,

        -- ----------------------------------------------------------------
        -- Customer Impact (ENHANCED — ALL customers, not just 295)
        -- ----------------------------------------------------------------
        MAX(agg.Customer_Count)                            AS Customer_Count,
        MAX(agg.Affected_Customers)                        AS Affected_Customers,

        -- Shared demand flag: YES if more than one customer shares this item/run
        CASE
            WHEN MAX(agg.Customer_Count) > 1 THEN 'YES'
            ELSE 'NO'
        END                                                AS Shared_Demand_Flag,

        -- ----------------------------------------------------------------
        -- Vendor Information
        -- Fallback hierarchy mirrors ETB_RUN_RISK: SS_CALC → SUPPLY_ACTION
        -- ----------------------------------------------------------------
        COALESCE(
            MAX(ss.PRIME_VNDR),      -- Priority 1: Safety stock calc vendor
            MAX(c.PRIME_VNDR),       -- Priority 2: Supply action vendor
            MAX(wfq.PRIME_VNDR)      -- Priority 3: WFQ adjustment vendor
        )                                                  AS Primary_Vendor,

        MAX(c.Vendor_Item_Number)                          AS Vendor_Item_Number,

        -- ----------------------------------------------------------------
        -- WFQ Context
        -- ----------------------------------------------------------------
        MAX(sd.WFQ_Dependency_In_Run)                      AS WFQ_Dependency_Flag,

        -- ----------------------------------------------------------------
        -- Metadata
        -- ----------------------------------------------------------------
        CAST(GETDATE() AS date)                            AS Analysis_Date,
        'CLIENT_295_STOCKOUT_MONITOR'                      AS Report_Type

    FROM         Stockout_Detection sd

    -- Join back to Client 295 rows for demand quantities and vendor info
    INNER JOIN   Client295_Demand c
                 ON  c.ITEMNMBR    = sd.ITEMNMBR
                 AND c.Run_Bucket  = sd.Run_Bucket

    -- LEFT JOIN aggregate summary (item/run may have only Client 295 demand)
    LEFT  JOIN   AggDemand_Summary agg
                 ON  agg.ITEMNMBR    = sd.ITEMNMBR
                 AND agg.Run_Bucket  = sd.Run_Bucket

    -- LEFT JOIN safety stock for vendor fallback (Priority 1)
    LEFT  JOIN   dbo.ETB_SS_CALC ss
                 ON  ss.ITEMNMBR    = sd.ITEMNMBR

    -- LEFT JOIN WFQ adj for vendor fallback (Priority 3)
    LEFT  JOIN   dbo.ETB_PAB_WFQ_ADJ wfq
                 ON  wfq.ITEMNMBR   = sd.ITEMNMBR

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

    -- Demand Quantities (ENHANCED)
    Client295_Demand,
    Aggregate_Demand_All_Customers,
    Shared_Demand_Ratio,

    -- Stockout Metrics
    Is_Stockout,
    Stockout_Qty,
    Max_Balance_In_Run,
    Min_Balance_In_Run,

    -- Customer Impact (ENHANCED)
    Customer_Count,
    Affected_Customers,
    Shared_Demand_Flag,

    -- Vendor Information
    Primary_Vendor,
    Vendor_Item_Number,

    -- Metadata
    Analysis_Date,
    Report_Type

FROM    Final_Output

ORDER BY
    -- Confirmed stockouts first
    CASE WHEN Is_Stockout = 'YES' THEN 0 ELSE 1 END,

    -- Most severe stockout (largest shortage) first within confirmed stockouts
    Stockout_Qty DESC,

    -- Then by earliest run bucket (soonest risk first)
    Run_Bucket,

    -- Alphabetical item within same run
    ITEMNMBR;
