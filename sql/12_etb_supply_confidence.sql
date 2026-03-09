-- ============================================================
-- ETB_SUPPLY_CONFIDENCE — Deployable View Script
-- Version:  1.0
-- Date:     March 2026
-- Author:   Zo Computer + Taylor
-- Purpose:  Shared demand detection, multi-construct overlap analysis,
--           and contention risk scoring for supply planning.
-- Target:   SQL Server 2017+ (STRING_AGG required)
--           For SQL Server ≤2016 replace STRING_AGG blocks with
--           FOR XML PATH fallback per design memo §13
-- Runs:     Idempotent — safe to re-run; prereq tables guarded by
--           IF NOT EXISTS; views use CREATE OR ALTER
-- Order:    1) ETB_SUPPLY_CONFIDENCE  2) ETB_SUPPLY_CONFIDENCE_DETAIL
--           3) Validation tests
-- Note:     No tables are created. All objects are views.
-- Governance: ETB_SUPPLY_CONFIDENCE_DESIGN_MEMO_v1.6.md
-- ============================================================





-- ============================================================
-- SECTION 2 — ETB_SUPPLY_CONFIDENCE  (grain: ITEMNMBR)
-- ============================================================

CREATE OR ALTER VIEW dbo.ETB_SUPPLY_CONFIDENCE AS

WITH

-- ── 1. Config ─────────────────────────────────────────────
-- All thresholds live here. No magic numbers anywhere else.
Config AS (
    SELECT
        CAST(14     AS int)   AS Overlap_Window_Days,
        CAST(25.714 AS float) AS Forecast_Horizon_Weeks,  -- 180.0 / 7.0
        CAST(1.2    AS float) AS Thin_Coverage_Threshold,
        CAST(14     AS int)   AS Critical_Days_Threshold,
        CAST(30     AS int)   AS High_Days_Threshold,
        CAST(30     AS int)   AS Default_Lead_Time_Days,
        CAST(9999.0 AS float) AS Past_Due_Urgency_Sentinel,
        CAST(10     AS int)   AS Urgent_Action_Days,
        CAST(0.10   AS float) AS Trend_Deteriorating_Threshold,
        CAST(-0.10  AS float) AS Trend_Improving_Threshold
),

-- ── 2. ConstructConfig ────────────────────────────────────
-- Confidence weights are governance assumptions, not data.
-- Review triggers: 5th run, commercial validation, cancellation, new construct.
ConstructConfig AS (
    SELECT Construct_ID, Confidence_Weight,
           CAST(1.0 + (1.0 - Confidence_Weight) AS float) AS Demand_Multiplier
    FROM (VALUES
        (291, CAST(0.75 AS float)),  -- commercial, established
        (295, CAST(0.75 AS float)),  -- commercial, established
        (298, CAST(0.30 AS float)),  -- early clinical, low confidence
        (301, CAST(0.50 AS float)),  -- development
        (303, CAST(0.60 AS float))   -- development, slightly more mature than 301
    ) AS t(Construct_ID, Confidence_Weight)
),

-- ── 3. SharedBase ─────────────────────────────────────────
-- Active planning rows. BEGINNING BALANCE excluded.
-- Suppressed rows INCLUDED and flagged — do not filter them out.
SharedBase AS (
    SELECT
        sa.ITEMNMBR,
        sa.ORDERNUMBER,           -- already cleaned upstream; do NOT re-apply REPLACE chain
        sa.Construct,
        sa.Demand_Due_Date,
        sa.Net_Demand,
        sa.Ledger_Extended_Balance,   -- item-level; identical on every row for an ITEMNMBR
        sa.WFQ_Extended_Status,       -- pass through; do not recompute
        sa.Suppression_Status,
        sa.PURCHASING_LT,

        sa.Data_Quality_Flag,
        sa.ItemDescription,
        sa.Supply_Action_Recommendation,
        COALESCE(sa.PURCHASING_LT, cfg.Default_Lead_Time_Days) AS Effective_LT
        -- Default_Lead_Time_Days = 30; exceeds most actual LTs; conservative direction
    FROM dbo.ETB_SUPPLY_ACTION sa WITH (NOLOCK)
    CROSS JOIN Config cfg
    WHERE sa.Suppression_Status <> 'BEGINNING BALANCE'
      AND sa.Demand_Due_Date    IS NOT NULL
      AND sa.ITEMNMBR NOT LIKE '60.%'
      AND sa.ITEMNMBR NOT LIKE '70.%'
),

-- ── 4. SharedDetection ────────────────────────────────────
-- Item-level counts, date range, balance, and metadata.
SharedDetection AS (
    SELECT
        sb.ITEMNMBR,
        COUNT(DISTINCT sb.Construct)   AS Construct_Count,
        COUNT(DISTINCT sb.ORDERNUMBER) AS MO_Count,
        MIN(sb.Demand_Due_Date)        AS First_Demand_Date,
        MAX(sb.Demand_Due_Date)        AS Last_Demand_Date,
        DATEDIFF(day,
            MIN(sb.Demand_Due_Date),
            MAX(sb.Demand_Due_Date))   AS Demand_Window_Days,
        MAX(sb.Ledger_Extended_Balance) AS Ledger_Extended_Balance,
        -- item-level; identical on every row; MAX is safe
        MAX(sb.Effective_LT)           AS Effective_LT,
        MAX(sb.ItemDescription)        AS ItemDescription
    FROM SharedBase sb
    GROUP BY sb.ITEMNMBR
),

-- ── 5. OverlapCheck ───────────────────────────────────────
-- Items where ≥2 distinct constructs have demand dates
-- within Overlap_Window_Days of each other.
OverlapCheck AS (
    SELECT DISTINCT a.ITEMNMBR
    FROM SharedBase a
    JOIN SharedBase b
      ON  b.ITEMNMBR    = a.ITEMNMBR
      AND b.Construct   <> a.Construct
      AND b.ORDERNUMBER <> a.ORDERNUMBER
      AND ABS(DATEDIFF(day, a.Demand_Due_Date, b.Demand_Due_Date))
              <= (SELECT Overlap_Window_Days FROM Config)
),

-- ── 6. QualifiedItems ─────────────────────────────────────
-- Assigns Shared_Demand_Type. Rows where type is NULL
-- do not qualify and are excluded from both output views.
QualifiedItems AS (
    SELECT
        sd.ITEMNMBR,
        sd.Construct_Count,
        sd.MO_Count,
        sd.First_Demand_Date,
        sd.Last_Demand_Date,
        sd.Demand_Window_Days,
        sd.Ledger_Extended_Balance,
        sd.Effective_LT,
        sd.ItemDescription,

        CAST(sd.MO_Count AS float)
            / NULLIF(CAST(sd.Demand_Window_Days AS float), 0)       AS Demand_Density,
        -- MOs per day in the demand window; NULL when window = 0 (same-day MOs)
        CAST(sd.Demand_Window_Days AS float)
            / NULLIF(CAST(sd.Effective_LT AS float), 0)             AS Lead_Time_Coverage_Ratio,
        -- demand window as multiple of procurement cycle; < 1.0 = collision within one LT
        CASE
            WHEN sd.Construct_Count > 1
                 AND oc.ITEMNMBR IS NOT NULL            THEN 'MULTI_CONSTRUCT_OVERLAP'
            -- cross-construct + ≥2 constructs have demand within Overlap_Window_Days
            WHEN sd.Construct_Count > 1
                 AND sd.MO_Count > sd.Construct_Count   THEN 'MULTI_CONSTRUCT_MULTI_MO'
            -- cross-construct + more MOs than constructs
            WHEN sd.Construct_Count > 1                 THEN 'MULTI_CONSTRUCT'
            -- cross-construct, no additional modifier
            WHEN sd.Construct_Count = 1
                 AND CAST(sd.Demand_Window_Days AS float)
                     / NULLIF(CAST(sd.Effective_LT AS float), 0) < 1.0
                                                         THEN 'SINGLE_CONSTRUCT_COLLISION'
            -- single construct; multiple MOs within one procurement cycle
            ELSE NULL  -- does not qualify; filtered below
        END                                             AS Shared_Demand_Type
    FROM SharedDetection sd
    LEFT JOIN OverlapCheck oc ON oc.ITEMNMBR = sd.ITEMNMBR
        -- absence = no cross-construct date overlap for this item
    WHERE
        -- pre-filter: exclude items that will have NULL Shared_Demand_Type
        -- avoids a downstream wrapper CTE for readability
        (sd.Construct_Count > 1)
        OR (sd.Construct_Count = 1
            AND CAST(sd.Demand_Window_Days AS float)
                / NULLIF(CAST(sd.Effective_LT AS float), 0) < 1.0)
),

-- ── 7. ConstructDemand ────────────────────────────────────
-- Per-ITEMNMBR × Construct aggregations with multipliers.
ConstructDemand AS (
    SELECT
        sb.ITEMNMBR,
        sb.Construct,
        SUM(sb.Net_Demand)  AS Demand_Total,
        SUM(CASE
                WHEN sb.Demand_Due_Date >= CAST(GETDATE() AS date)
                 AND sb.Demand_Due_Date <= DATEADD(day, qi.Effective_LT,
                                                   CAST(GETDATE() AS date))
                THEN sb.Net_Demand ELSE 0.0
            END)            AS Demand_InLT,
        -- demand due within the purchasing lead time window from today
        cc.Confidence_Weight,
        COALESCE(cc.Demand_Multiplier, 1.0)                AS Demand_Multiplier,
        -- COALESCE: construct not in ConstructConfig → no hedge (data quality condition)
        SUM(sb.Net_Demand) * COALESCE(cc.Demand_Multiplier, 1.0) AS Hedged_Demand
    FROM SharedBase sb
    JOIN QualifiedItems qi
      ON  qi.ITEMNMBR = sb.ITEMNMBR
      -- restricts to items that qualified; excludes single-construct non-collisions
    LEFT JOIN ConstructConfig cc
      ON  cc.Construct_ID = sb.Construct
      -- LEFT JOIN: unknown constructs receive Demand_Multiplier = 1.0 via COALESCE above
    GROUP BY sb.ITEMNMBR, sb.Construct,
             cc.Confidence_Weight, cc.Demand_Multiplier
),

-- ── 8. ItemPivot ─────────────────────────────────────────
-- Pivots ConstructDemand to one row per ITEMNMBR.
-- All five construct columns appear for every item;
-- COALESCE to 0.0 so buyers never see NULL where zero is correct.
ItemPivot AS (
    SELECT
        cd.ITEMNMBR,

        -- ── Nominal demand per construct ──────────────────
        COALESCE(SUM(CASE WHEN cd.Construct = 291 THEN cd.Demand_Total END), 0.0) AS Demand_291_Total,
        COALESCE(SUM(CASE WHEN cd.Construct = 295 THEN cd.Demand_Total END), 0.0) AS Demand_295_Total,
        COALESCE(SUM(CASE WHEN cd.Construct = 298 THEN cd.Demand_Total END), 0.0) AS Demand_298_Total,
        COALESCE(SUM(CASE WHEN cd.Construct = 301 THEN cd.Demand_Total END), 0.0) AS Demand_301_Total,
        COALESCE(SUM(CASE WHEN cd.Construct = 303 THEN cd.Demand_Total END), 0.0) AS Demand_303_Total,

        -- ── Demand within lead time per construct ─────────
        COALESCE(SUM(CASE WHEN cd.Construct = 291 THEN cd.Demand_InLT END), 0.0) AS Demand_291_InLT,
        COALESCE(SUM(CASE WHEN cd.Construct = 295 THEN cd.Demand_InLT END), 0.0) AS Demand_295_InLT,
        COALESCE(SUM(CASE WHEN cd.Construct = 298 THEN cd.Demand_InLT END), 0.0) AS Demand_298_InLT,
        COALESCE(SUM(CASE WHEN cd.Construct = 301 THEN cd.Demand_InLT END), 0.0) AS Demand_301_InLT,
        COALESCE(SUM(CASE WHEN cd.Construct = 303 THEN cd.Demand_InLT END), 0.0) AS Demand_303_InLT,

        -- ── Hedged (multiplier-inflated) demand per construct ─
        COALESCE(SUM(CASE WHEN cd.Construct = 291 THEN cd.Hedged_Demand END), 0.0) AS Hedged_Demand_291,
        COALESCE(SUM(CASE WHEN cd.Construct = 295 THEN cd.Hedged_Demand END), 0.0) AS Hedged_Demand_295,
        COALESCE(SUM(CASE WHEN cd.Construct = 298 THEN cd.Hedged_Demand END), 0.0) AS Hedged_Demand_298,
        COALESCE(SUM(CASE WHEN cd.Construct = 301 THEN cd.Hedged_Demand END), 0.0) AS Hedged_Demand_301,
        COALESCE(SUM(CASE WHEN cd.Construct = 303 THEN cd.Hedged_Demand END), 0.0) AS Hedged_Demand_303,

        -- ── Item-level totals (sum of five construct columns) ─
        COALESCE(SUM(CASE WHEN cd.Construct = 291 THEN cd.Demand_Total END), 0.0)
      + COALESCE(SUM(CASE WHEN cd.Construct = 295 THEN cd.Demand_Total END), 0.0)
      + COALESCE(SUM(CASE WHEN cd.Construct = 298 THEN cd.Demand_Total END), 0.0)
      + COALESCE(SUM(CASE WHEN cd.Construct = 301 THEN cd.Demand_Total END), 0.0)
      + COALESCE(SUM(CASE WHEN cd.Construct = 303 THEN cd.Demand_Total END), 0.0) AS Total_Nominal_Demand,

        COALESCE(SUM(CASE WHEN cd.Construct = 291 THEN cd.Hedged_Demand END), 0.0)
      + COALESCE(SUM(CASE WHEN cd.Construct = 295 THEN cd.Hedged_Demand END), 0.0)
      + COALESCE(SUM(CASE WHEN cd.Construct = 298 THEN cd.Hedged_Demand END), 0.0)
      + COALESCE(SUM(CASE WHEN cd.Construct = 301 THEN cd.Hedged_Demand END), 0.0)
      + COALESCE(SUM(CASE WHEN cd.Construct = 303 THEN cd.Hedged_Demand END), 0.0) AS Total_Hedged_Demand

    FROM ConstructDemand cd
    GROUP BY cd.ITEMNMBR
),

-- ── 9. MO_Slack ──────────────────────────────────────────
-- Per-row slack; ROW_NUMBER = 1 = most-at-risk MO for this item.
-- ROW_NUMBER (not RANK) guarantees exactly one Slack_Rank = 1 per ITEMNMBR.
-- Tiebreaker: earliest due date first, then ORDERNUMBER for determinism.
-- RANK() would assign Slack_Rank = 1 to multiple rows when slack is tied,
-- causing fan-out in AtRiskMO and duplicate PKs in the final SELECT.
MO_Slack AS (
    SELECT
        sb.ITEMNMBR,
        sb.ORDERNUMBER,
        sb.Construct,
        sb.Demand_Due_Date,
        sb.Net_Demand,
        sb.Ledger_Extended_Balance - sb.Net_Demand AS MO_Slack,
        -- negative = this MO cannot be covered by current balance alone
        ROW_NUMBER() OVER (
            PARTITION BY sb.ITEMNMBR
            ORDER BY (sb.Ledger_Extended_Balance - sb.Net_Demand) ASC,
                     sb.Demand_Due_Date ASC,
                     sb.ORDERNUMBER     ASC
        ) AS Slack_Rank
        -- Slack_Rank = 1 is the MO most likely to cause a fulfillment failure
    FROM SharedBase sb
    JOIN QualifiedItems qi ON qi.ITEMNMBR = sb.ITEMNMBR
),

-- ── 10. AtRiskMO ─────────────────────────────────────────
-- One row per item: the single most-at-risk MO.
AtRiskMO AS (
    SELECT ITEMNMBR, ORDERNUMBER, Demand_Due_Date, MO_Slack
    FROM MO_Slack
    WHERE Slack_Rank = 1
),

-- ── 11. WFQ_Summary ──────────────────────────────────────
-- Action and quality flags per item.
WFQ_Summary AS (
    SELECT
        sb.ITEMNMBR,
        SUM(CASE WHEN sb.Supply_Action_Recommendation = 'WFQ_RESCUE' THEN 1 ELSE 0 END) AS WFQ_Rescue_Count,
        SUM(CASE WHEN sb.Supply_Action_Recommendation = 'ORDER'       THEN 1 ELSE 0 END) AS Count_Order,
        SUM(CASE WHEN sb.Supply_Action_Recommendation = 'BOTH'        THEN 1 ELSE 0 END) AS Count_Both,
        SUM(CASE WHEN DATEDIFF(day, CAST(GETDATE() AS date), sb.Demand_Due_Date)
                          <= (SELECT Urgent_Action_Days FROM Config)
                 THEN 1 ELSE 0 END)                                                       AS Urgent_Count,
        MAX(CASE WHEN sb.Suppression_Status LIKE 'SUPPRESSED%' THEN 'Y' ELSE 'N' END)    AS Has_Suppressed_Row,
        MIN(CASE WHEN sb.Data_Quality_Flag  <> 'CLEAN'         THEN 'N' ELSE 'Y' END)    AS All_Rows_Clean
    FROM SharedBase sb
    JOIN QualifiedItems qi ON qi.ITEMNMBR = sb.ITEMNMBR
    GROUP BY sb.ITEMNMBR
),

-- ── 12. ConstructList ────────────────────────────────────
-- Pipe-delimited display label; unsortable / unfilterable.
-- For analysis use ETB_SUPPLY_CONFIDENCE_DETAIL.
ConstructList AS (
    SELECT ITEMNMBR,
           STRING_AGG(CAST(Construct AS varchar(20)), ' | ')
               WITHIN GROUP (ORDER BY Construct) AS Construct_List
    FROM (SELECT DISTINCT ITEMNMBR, Construct FROM SharedBase) t
    GROUP BY ITEMNMBR
),

-- ── 13. MO_List ──────────────────────────────────────────
MO_List AS (
    SELECT ITEMNMBR,
           STRING_AGG(ORDERNUMBER, ' | ')
               WITHIN GROUP (ORDER BY ORDERNUMBER) AS MO_List
    FROM (SELECT DISTINCT ITEMNMBR, ORDERNUMBER FROM SharedBase) t
    GROUP BY ITEMNMBR
)


-- ── Final SELECT ─────────────────────────────────────────
SELECT

    -- ── Identity ─────────────────────────────────────────
    qi.ITEMNMBR                                              AS ITEMNMBR,
    qi.ItemDescription,
    qi.Shared_Demand_Type,
    qi.Construct_Count,
    qi.MO_Count,
    CASE WHEN qi.Construct_Count > 1 THEN 'Y' ELSE 'N' END  AS Is_Multi_Construct,
    qi.First_Demand_Date,
    qi.Last_Demand_Date,
    qi.Demand_Window_Days,
    qi.Demand_Density,
    qi.Effective_LT                                          AS Lead_Time_Days,
    qi.Lead_Time_Coverage_Ratio,



    -- ── Construct presence flags ──────────────────────────
    CASE WHEN ip.Demand_291_Total > 0 THEN 'Y' ELSE 'N' END  AS Has_Construct_291,
    CASE WHEN ip.Demand_295_Total > 0 THEN 'Y' ELSE 'N' END  AS Has_Construct_295,
    CASE WHEN ip.Demand_298_Total > 0 THEN 'Y' ELSE 'N' END  AS Has_Construct_298,
    CASE WHEN ip.Demand_301_Total > 0 THEN 'Y' ELSE 'N' END  AS Has_Construct_301,
    CASE WHEN ip.Demand_303_Total > 0 THEN 'Y' ELSE 'N' END  AS Has_Construct_303,

    -- ── Display labels (pipe-delimited; use DETAIL view for analysis) ──
    cl.Construct_List,
    ml.MO_List                                               AS ORDERNUMBER_List,

    -- ── Nominal demand ────────────────────────────────────
    ip.Total_Nominal_Demand,
    ip.Demand_291_Total,
    ip.Demand_295_Total,
    ip.Demand_298_Total,
    ip.Demand_301_Total,
    ip.Demand_303_Total,

    -- ── Demand share per construct ────────────────────────
    ip.Demand_291_Total / NULLIF(ip.Total_Nominal_Demand, 0) AS Demand_291_Share,
    ip.Demand_295_Total / NULLIF(ip.Total_Nominal_Demand, 0) AS Demand_295_Share,
    ip.Demand_298_Total / NULLIF(ip.Total_Nominal_Demand, 0) AS Demand_298_Share,
    ip.Demand_301_Total / NULLIF(ip.Total_Nominal_Demand, 0) AS Demand_301_Share,
    ip.Demand_303_Total / NULLIF(ip.Total_Nominal_Demand, 0) AS Demand_303_Share,

    -- ── Dominant construct (highest nominal share) ────────
    (SELECT TOP 1 c
     FROM (VALUES (291, ip.Demand_291_Total),
                  (295, ip.Demand_295_Total),
                  (298, ip.Demand_298_Total),
                  (301, ip.Demand_301_Total),
                  (303, ip.Demand_303_Total)) t(c, v)
     ORDER BY v DESC)                                        AS Dominant_Construct,

    (SELECT MAX(v)
     FROM (VALUES (ip.Demand_291_Total),
                  (ip.Demand_295_Total),
                  (ip.Demand_298_Total),
                  (ip.Demand_301_Total),
                  (ip.Demand_303_Total)) t(v))
        / NULLIF(ip.Total_Nominal_Demand, 0)                 AS Dominant_Construct_Share,

    -- ── Hedged (multiplier-inflated) demand ───────────────
    ip.Total_Hedged_Demand,
    ip.Hedged_Demand_291,
    ip.Hedged_Demand_295,
    ip.Hedged_Demand_298,
    ip.Hedged_Demand_301,
    ip.Hedged_Demand_303,
    ip.Total_Hedged_Demand - ip.Total_Nominal_Demand         AS Hedge_Volume,
    -- additional demand added by confidence weighting; always >= 0

    -- ── Demand within lead time per construct ─────────────
    ip.Demand_291_InLT,
    ip.Demand_295_InLT,
    ip.Demand_298_InLT,
    ip.Demand_301_InLT,
    ip.Demand_303_InLT,

    -- ── Weekly mean (180-day horizon / 7) ─────────────────
    ip.Demand_291_Total / cfg.Forecast_Horizon_Weeks         AS Weekly_Mean_291,
    ip.Demand_295_Total / cfg.Forecast_Horizon_Weeks         AS Weekly_Mean_295,
    ip.Demand_298_Total / cfg.Forecast_Horizon_Weeks         AS Weekly_Mean_298,
    ip.Demand_301_Total / cfg.Forecast_Horizon_Weeks         AS Weekly_Mean_301,
    ip.Demand_303_Total / cfg.Forecast_Horizon_Weeks         AS Weekly_Mean_303,

    -- ── InLT vs weekly mean (positive = front-loaded spike) ──
    ip.Demand_291_InLT - (ip.Demand_291_Total / cfg.Forecast_Horizon_Weeks) AS InLT_vs_Mean_291,
    ip.Demand_295_InLT - (ip.Demand_295_Total / cfg.Forecast_Horizon_Weeks) AS InLT_vs_Mean_295,
    ip.Demand_298_InLT - (ip.Demand_298_Total / cfg.Forecast_Horizon_Weeks) AS InLT_vs_Mean_298,
    ip.Demand_301_InLT - (ip.Demand_301_Total / cfg.Forecast_Horizon_Weeks) AS InLT_vs_Mean_301,
    ip.Demand_303_InLT - (ip.Demand_303_Total / cfg.Forecast_Horizon_Weeks) AS InLT_vs_Mean_303,

    -- ── Balance and coverage ─────────────────────────────
    qi.Ledger_Extended_Balance,

    qi.Ledger_Extended_Balance
        / NULLIF(ip.Total_Nominal_Demand, 0)                 AS Nominal_Coverage_Ratio,

    qi.Ledger_Extended_Balance
        / NULLIF(ip.Total_Hedged_Demand, 0)                  AS Hedged_Coverage_Ratio,

    CASE
        WHEN ip.Total_Nominal_Demand = 0 OR ip.Total_Hedged_Demand = 0 THEN NULL
        ELSE (qi.Ledger_Extended_Balance / NULLIF(ip.Total_Nominal_Demand, 0))
           - (qi.Ledger_Extended_Balance / NULLIF(ip.Total_Hedged_Demand, 0))
    END                                                      AS Coverage_Confidence_Gap,
    -- magnitude by which nominal ratio overstates hedged confidence; always >= 0

    ip.Total_Nominal_Demand - qi.Ledger_Extended_Balance     AS Aggregate_Deficit_Nominal,
    -- NOT clamped to zero; negative = surplus; meaningful
    ip.Total_Hedged_Demand  - qi.Ledger_Extended_Balance     AS Aggregate_Deficit_Hedged,
    -- NOT clamped to zero; negative = surplus; meaningful

    ip.Total_Hedged_Demand
        / NULLIF(qi.Ledger_Extended_Balance, 0)              AS Demand_Pressure_Ratio,
    -- hedged demand relative to balance; > 1.0 = demand exceeds supply on hedged basis

    -- ── Supply coverage classification ───────────────────
    CASE
        WHEN (ip.Total_Nominal_Demand - qi.Ledger_Extended_Balance) <= 0
             AND (ws.WFQ_Rescue_Count = 0 OR ws.WFQ_Rescue_Count IS NULL) THEN 'INVENTORY'
        -- on-hand alone covers demand; no WFQ rescue MOs involved
        WHEN (ip.Total_Nominal_Demand - qi.Ledger_Extended_Balance) <= 0
             AND ws.WFQ_Rescue_Count > 0                                  THEN 'WFQ'
        -- coverage requires WFQ lots pending QC release
        WHEN (ip.Total_Nominal_Demand - qi.Ledger_Extended_Balance) > 0
             AND (ip.Total_Hedged_Demand - qi.Ledger_Extended_Balance) <= 0 THEN 'MIXED'
        -- nominal deficit but hedged position covered; mixed supply types
        WHEN (ip.Total_Nominal_Demand - qi.Ledger_Extended_Balance) > 0  THEN 'UNCOVERED'
        -- demand exceeds balance with no coverage path
        ELSE                                                                   'MIXED'
        -- NOTE: PO tier not implemented; open PO qty not yet in ETB_SUPPLY_ACTION
    END                                                      AS Supply_Coverage_Type,

    -- ── Contention risk band ──────────────────────────────
    CASE
        WHEN (ip.Total_Hedged_Demand - qi.Ledger_Extended_Balance) > 0
             AND DATEDIFF(day, CAST(GETDATE() AS date),
                          ISNULL(arm.Demand_Due_Date, qi.First_Demand_Date))
                     <= cfg.Critical_Days_Threshold                        THEN 'CRITICAL'
        -- hedged deficit + ≤14 days: standard PO cannot respond
        WHEN (ip.Total_Hedged_Demand - qi.Ledger_Extended_Balance) > 0
             AND DATEDIFF(day, CAST(GETDATE() AS date),
                          ISNULL(arm.Demand_Due_Date, qi.First_Demand_Date))
                     <= cfg.High_Days_Threshold                            THEN 'HIGH'
        -- hedged deficit + ≤30 days: within one procurement cycle
        WHEN (ip.Total_Hedged_Demand - qi.Ledger_Extended_Balance) <= 0
             AND (qi.Ledger_Extended_Balance
                  / NULLIF(ip.Total_Hedged_Demand, 0)) < cfg.Thin_Coverage_Threshold
                                                                           THEN 'MONITOR'
        -- no hedged deficit but buffer thin; one demand revision from deficit
        ELSE                                                                    'WATCH'
        -- qualifying item with comfortable hedged coverage
    END                                                      AS Contention_Risk_Band,

    -- ── Procurement action summary ────────────────────────
    CASE
        WHEN ws.WFQ_Rescue_Count > 0 AND ws.Count_Order > 0 THEN 'BOTH'
        WHEN ws.WFQ_Rescue_Count > 0                         THEN 'WFQ_RESCUE'
        WHEN ws.Count_Order > 0 OR ws.Count_Both > 0         THEN 'ORDER'
        ELSE                                                      'SUFFICIENT'
    END                                                      AS Supply_Action_Summary,

    COALESCE(ws.WFQ_Rescue_Count, 0)                         AS WFQ_Rescue_Count,
    COALESCE(ws.Count_Order,      0)                         AS Count_Order,
    COALESCE(ws.Count_Both,       0)                         AS Count_Both,
    COALESCE(ws.Urgent_Count,     0)                         AS Urgent_Count,

    -- ── Most-at-risk MO ───────────────────────────────────
    arm.MO_Slack                                             AS Tightest_MO_Slack,
    -- negative = this MO is underwater against current balance alone
    arm.ORDERNUMBER                                          AS First_At_Risk_MO,
    arm.Demand_Due_Date                                      AS First_At_Risk_Date,

    DATEDIFF(day, CAST(GETDATE() AS date),
             ISNULL(arm.Demand_Due_Date, qi.First_Demand_Date)) AS Days_To_First_Risk,
    -- negative = past due; raw value exposed; not clamped

    -- ── Urgency score ─────────────────────────────────────
    CASE
        WHEN DATEDIFF(day, CAST(GETDATE() AS date),
                      ISNULL(arm.Demand_Due_Date, qi.First_Demand_Date)) <= 0
             AND (ip.Total_Hedged_Demand - qi.Ledger_Extended_Balance) > 0
                                                             THEN cfg.Past_Due_Urgency_Sentinel
        -- past-due with hedged deficit = maximum urgency (9999.0)
        WHEN (ip.Total_Hedged_Demand - qi.Ledger_Extended_Balance) <= 0
                                                             THEN CAST(0.0 AS float)
        -- no hedged deficit = zero urgency regardless of days remaining
        ELSE CAST(ip.Total_Hedged_Demand - qi.Ledger_Extended_Balance AS float)
             / NULLIF(CAST(
                   DATEDIFF(day, CAST(GETDATE() AS date),
                            ISNULL(arm.Demand_Due_Date, qi.First_Demand_Date))
               AS float), 0)
        -- deficit per day remaining; higher = more urgent
    END                                                      AS Urgency_Score,

    -- ── Data quality flags ────────────────────────────────
    ws.Has_Suppressed_Row,
    ws.All_Rows_Clean,



    -- ── Metadata ─────────────────────────────────────────
    CAST(GETDATE() AS date)                                  AS Analysis_Date,
    'RALPH_LOOP_SUPPLY_CONFIDENCE'                           AS Report_Type

FROM QualifiedItems qi

JOIN ItemPivot ip
  ON  ip.ITEMNMBR = qi.ITEMNMBR
  -- ItemPivot: one row per qualifying item; all pivot columns

JOIN WFQ_Summary ws
  ON  ws.ITEMNMBR = qi.ITEMNMBR
  -- WFQ_Summary: one row per qualifying item; action and quality flags

LEFT JOIN AtRiskMO arm
  ON  arm.ITEMNMBR = qi.ITEMNMBR
  -- ROW_NUMBER in MO_Slack guarantees exactly one row per ITEMNMBR in AtRiskMO;
  -- LEFT JOIN because an item with no demand rows would have no AtRiskMO row

LEFT JOIN ConstructList cl
  ON  cl.ITEMNMBR = qi.ITEMNMBR
  -- pipe-delimited display label; absence acceptable

LEFT JOIN MO_List ml
  ON  ml.ITEMNMBR = qi.ITEMNMBR
  -- pipe-delimited display label; absence acceptable



LEFT JOIN dbo.ETB_SDA_SNAPSHOT snap
  ON  snap.ITEMNMBR     = qi.ITEMNMBR
  AND snap.Snapshot_Date = CAST(DATEADD(day, -1, GETDATE()) AS date)
  -- prior day; NULLs are correct when table is empty (Phase 2)

CROSS JOIN Config cfg
  -- single-row CTE; CROSS JOIN exposes threshold columns for inline expressions
;

GO


-- ============================================================
-- SECTION 3 — ETB_SUPPLY_CONFIDENCE_DETAIL
--             (grain: ITEMNMBR × ORDERNUMBER × Construct)
-- ============================================================
-- The summary view is the index. This view is the book.
-- Construct_List and ORDERNUMBER_List in the summary are
-- display labels only. All sort/filter/rank operations live here.
-- ============================================================

CREATE OR ALTER VIEW dbo.ETB_SUPPLY_CONFIDENCE_DETAIL AS

WITH

Config AS (
    SELECT
        CAST(30     AS int)   AS Default_Lead_Time_Days,
        CAST(14     AS int)   AS Overlap_Window_Days
),

ConstructConfig AS (
    SELECT Construct_ID, Confidence_Weight,
           CAST(1.0 + (1.0 - Confidence_Weight) AS float) AS Demand_Multiplier
    FROM (VALUES
        (291, CAST(0.75 AS float)),
        (295, CAST(0.75 AS float)),
        (298, CAST(0.30 AS float)),
        (301, CAST(0.50 AS float)),
        (303, CAST(0.60 AS float))
    ) AS t(Construct_ID, Confidence_Weight)
),

SharedBase AS (
    SELECT
        sa.ITEMNMBR, sa.ORDERNUMBER, sa.Construct,
        sa.Demand_Due_Date, sa.Net_Demand, sa.Ledger_Extended_Balance,
        sa.WFQ_Extended_Status, sa.Suppression_Status,
        sa.PURCHASING_LT, sa.Data_Quality_Flag,
        sa.ItemDescription, sa.Supply_Action_Recommendation,
        COALESCE(sa.PURCHASING_LT, cfg.Default_Lead_Time_Days) AS Effective_LT
    FROM dbo.ETB_SUPPLY_ACTION sa WITH (NOLOCK)
    CROSS JOIN Config cfg
    WHERE sa.Suppression_Status <> 'BEGINNING BALANCE'
      AND sa.Demand_Due_Date    IS NOT NULL
      AND sa.ITEMNMBR NOT LIKE '60.%'
      AND sa.ITEMNMBR NOT LIKE '70.%'
),

SharedDetection AS (
    SELECT
        sb.ITEMNMBR,
        COUNT(DISTINCT sb.Construct)   AS Construct_Count,
        COUNT(DISTINCT sb.ORDERNUMBER) AS MO_Count,
        DATEDIFF(day,
            MIN(sb.Demand_Due_Date),
            MAX(sb.Demand_Due_Date))   AS Demand_Window_Days,
        -- must match summary view SharedDetection; SINGLE_CONSTRUCT_COLLISION
        -- qualification uses Demand_Window_Days / Effective_LT, not MO_Count
        MAX(sb.Effective_LT)           AS Effective_LT
    FROM SharedBase sb
    GROUP BY sb.ITEMNMBR
),

OverlapCheck AS (
    SELECT DISTINCT a.ITEMNMBR
    FROM SharedBase a
    JOIN SharedBase b
      ON  b.ITEMNMBR    = a.ITEMNMBR
      AND b.Construct   <> a.Construct
      AND b.ORDERNUMBER <> a.ORDERNUMBER
      AND ABS(DATEDIFF(day, a.Demand_Due_Date, b.Demand_Due_Date))
              <= (SELECT Overlap_Window_Days FROM Config)
),

QualifiedItems AS (
    SELECT
        sd.ITEMNMBR,
        sd.Construct_Count,
        sd.MO_Count,
        sd.Effective_LT,
        CASE
            WHEN sd.Construct_Count > 1
                 AND oc.ITEMNMBR IS NOT NULL            THEN 'MULTI_CONSTRUCT_OVERLAP'
            WHEN sd.Construct_Count > 1
                 AND sd.MO_Count > sd.Construct_Count   THEN 'MULTI_CONSTRUCT_MULTI_MO'
            WHEN sd.Construct_Count > 1                 THEN 'MULTI_CONSTRUCT'
            WHEN sd.Construct_Count = 1
                 AND CAST(sd.Demand_Window_Days AS float)
                     / NULLIF(CAST(sd.Effective_LT AS float), 0) < 1.0
                                                         THEN 'SINGLE_CONSTRUCT_COLLISION'
            -- condition mirrors summary view exactly: Demand_Window_Days / Effective_LT
            -- NOT MO_Count / Effective_LT — both views must qualify identical item sets
            ELSE NULL
        END AS Shared_Demand_Type
    FROM SharedDetection sd
    LEFT JOIN OverlapCheck oc ON oc.ITEMNMBR = sd.ITEMNMBR
    WHERE (sd.Construct_Count > 1)
       OR (sd.Construct_Count = 1
           AND CAST(sd.Demand_Window_Days AS float)
               / NULLIF(CAST(sd.Effective_LT AS float), 0) < 1.0)
)


SELECT

    -- ── Grain keys ────────────────────────────────────────
    sb.ITEMNMBR,
    sb.ORDERNUMBER,
    sb.Construct,

    -- ── Item and MO fields ────────────────────────────────
    sb.ItemDescription,
    sb.Demand_Due_Date,
    sb.Net_Demand,

    -- ── Demand share within item ──────────────────────────
    sb.Net_Demand
        / NULLIF(SUM(sb.Net_Demand) OVER (PARTITION BY sb.ITEMNMBR), 0) AS Program_Demand_Share,
    -- this MO's share of total nominal item demand

    -- ── Confidence weighting ─────────────────────────────
    cc.Confidence_Weight,
    -- NULL if construct not in ConstructConfig; data quality condition
    COALESCE(cc.Demand_Multiplier, 1.0)                      AS Demand_Multiplier,
    sb.Net_Demand * COALESCE(cc.Demand_Multiplier, 1.0)      AS Hedged_Demand,

    -- ── Urgency fields ────────────────────────────────────
    DATEDIFF(day, CAST(GETDATE() AS date), sb.Demand_Due_Date) AS Days_Until_This_Need,
    RANK() OVER (
        PARTITION BY sb.ITEMNMBR
        ORDER BY sb.Demand_Due_Date ASC
    )                                                        AS MO_Urgency_Rank,
    -- 1 = most urgent (earliest due date)

    -- ── MO slack ─────────────────────────────────────────
    sb.Ledger_Extended_Balance - sb.Net_Demand               AS MO_Slack,
    -- negative = this MO cannot be covered by current balance alone

    -- ── Pass-through status fields ────────────────────────
    sb.Suppression_Status,
    sb.WFQ_Extended_Status,
    sb.Supply_Action_Recommendation,
    sb.Data_Quality_Flag,

    -- ── Carried from summary view ─────────────────────────
    qi.Shared_Demand_Type,
    sc.Contention_Risk_Band,
    sc.Demand_Pressure_Ratio

FROM SharedBase sb

JOIN QualifiedItems qi
  ON  qi.ITEMNMBR = sb.ITEMNMBR
  -- restricts detail rows to qualifying items only

LEFT JOIN ConstructConfig cc
  ON  cc.Construct_ID = sb.Construct
  -- LEFT JOIN: unknown constructs get NULL Confidence_Weight; Demand_Multiplier coalesced

JOIN dbo.ETB_SUPPLY_CONFIDENCE sc
  ON  sc.ITEMNMBR = sb.ITEMNMBR
  -- many-to-one join back to summary; carries band, pressure ratio, sourcing flag
  -- no fan-out risk: summary grain is ITEMNMBR; detail grain is ITEMNMBR × ORDERNUMBER × Construct
;

GO


-- ============================================================
-- SECTION 4 — VALIDATION TESTS
-- Run after both views are deployed.
-- Uncomment each block and execute individually.
-- ============================================================

/*

-- Test 1: Surplus contested items — invisible to ETB_STOCKOUTS
-- Expected: count > 0  (confirms view adds value beyond stockout detection)
SELECT COUNT(*) AS Shared_Not_In_Stockout
FROM dbo.ETB_SUPPLY_CONFIDENCE sc
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.ETB_STOCKOUTS so
    WHERE so.Item_Number = sc.ITEMNMBR   -- verify column name in ETB_STOCKOUTS
);

-- Test 2: Coverage distribution by risk band
-- Expected: CRITICAL + HIGH avg < 1.0; MONITOR avg 1.0–1.2; WATCH avg >= 1.2
SELECT Contention_Risk_Band,
       COUNT(*)                    AS Item_Count,
       AVG(Hedged_Coverage_Ratio)  AS Avg_Hedged_Coverage
FROM dbo.ETB_SUPPLY_CONFIDENCE
GROUP BY Contention_Risk_Band
ORDER BY CASE Contention_Risk_Band
             WHEN 'CRITICAL' THEN 1 WHEN 'HIGH'  THEN 2
             WHEN 'MONITOR'  THEN 3 WHEN 'WATCH' THEN 4
         END;

-- Test 3: Hidden risk — aggregate fine but one MO is underwater
-- Expected: any rows here are invisible to ETB_STOCKOUTS (that is the point)
SELECT ITEMNMBR, Hedged_Coverage_Ratio, Tightest_MO_Slack,
       First_At_Risk_MO, Contention_Risk_Band, ORDERNUMBER_List
FROM dbo.ETB_SUPPLY_CONFIDENCE
WHERE Aggregate_Deficit_Nominal <= 0
  AND Tightest_MO_Slack < 0;

-- Test 4: No duplicate primary keys
-- Expected: zero rows
SELECT ITEMNMBR, COUNT(*) AS n
FROM dbo.ETB_SUPPLY_CONFIDENCE
GROUP BY ITEMNMBR
HAVING COUNT(*) > 1;

-- Test 5: NULL audit on non-nullable output columns
-- Expected: all zeros
SELECT
    SUM(CASE WHEN Shared_Demand_Type    IS NULL THEN 1 ELSE 0 END) AS Null_Type,
    SUM(CASE WHEN Contention_Risk_Band  IS NULL THEN 1 ELSE 0 END) AS Null_Band,

    SUM(CASE WHEN Supply_Action_Summary IS NULL THEN 1 ELSE 0 END) AS Null_Action,
    SUM(CASE WHEN Urgency_Score         IS NULL THEN 1 ELSE 0 END) AS Null_Urgency,
    SUM(CASE WHEN Report_Type           IS NULL THEN 1 ELSE 0 END) AS Null_ReportType
FROM dbo.ETB_SUPPLY_CONFIDENCE;

-- Test 6: No phantom items (rows in view not grounded in source)
-- Expected: zero rows
SELECT COUNT(*) AS Phantoms
FROM dbo.ETB_SUPPLY_CONFIDENCE sc
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.ETB_SUPPLY_ACTION sa WITH (NOLOCK)
    WHERE sa.ITEMNMBR = sc.ITEMNMBR
);

*/
