-- ============================================================================
-- VIEW: ETB_SS_CALC
-- Purpose: Safety stock calculations based on demand history and lead times
-- Author: Zo Computer
-- Date: 2026-02-26
-- Dependencies: dbo.ReceivingsLineItems, dbo.POP30330, dbo.PHR_MO_CostCalc1,
--               dbo.ETB_SS
-- ============================================================================
/*
================================================================================
ETB_SS_CALC — Safety Stock Calculation (Reference View)
================================================================================
Purpose:
  Calculates safety stock quantities and values per item using historical weekly
  demand variability and series-specific lead times.  Provides SS quantities in
  both manufacturing UOM and purchasing UOM for downstream views (6, 7, 8).

Source Tables:
  dbo.ReceivingsLineItems   — Receiving history for average cost derivation
  dbo.POP30330              — PO unit costs (cost join for receivings)
  dbo.PHR_MO_CostCalc1      — MO consumption history for demand statistics
  dbo.ETB_SS                — Safety stock master (item/vendor reference)

Named Thresholds (Config CTE — Issue 8):
  Lead_Days_Series_30   = 100  days (series 30 items)
  Lead_Days_Series_10   =  60  days (series 10 items)
  Lead_Days_Default     =  45  days (all other series)
  SS_Value_Ceiling      = 20000 (exclude outlier SS values above this)
  Demand_Lookback_Years =   1  years back from today for demand history

Change Log:
  2026-02-26: Initial hardening — UNASSIGNED vendor fallback (Issue 4),
              dynamic demand lookback year (replaces hardcoded 2024/2025).
  2026-02-27: Added Config CTE for named thresholds (Issue 8 — eliminates
              magic numbers 100, 60, 45, 20000 scattered through SSCalculation).
================================================================================
*/

-- ============================================================================
-- Config: Named threshold constants (Issue 8 — eliminates magic numbers)
-- ============================================================================
WITH Config AS (
    SELECT
        -- Lead time by item series (sourced from vendor SLA agreements)
        100 AS Lead_Days_Series_30,     -- Series 30: long-lead API ingredients
         60 AS Lead_Days_Series_10,     -- Series 10: intermediate lead items
         45 AS Lead_Days_Default,       -- All other series: standard lead time

        -- Safety stock value ceiling — items with SSValue above this are
        -- outliers (data anomalies or intentional exclusions from SS planning)
        20000 AS SS_Value_Ceiling,

        -- Demand history lookback: YEAR(GETDATE()) - N gives a rolling window
        -- Value 1 = last 2 calendar years (current year + previous year)
        1 AS Demand_Lookback_Years
),

-- ============================================================================
-- CTE 1: TempAvgCost
-- Purpose: Calculate average cost from receivings data
-- ============================================================================
TempAvgCost AS (
    SELECT 
        r.[Item Number],
        r.[Item Description],
        r.[U Of M],
        SUM(r.[QTY Shipped]) AS TotalQtyShipped,
        SUM(p.UNITCOST * r.[QTY Shipped]) AS TotalCost,
        CASE 
            WHEN SUM(r.[QTY Shipped]) > 0 
            THEN SUM(p.UNITCOST * r.[QTY Shipped]) / SUM(r.[QTY Shipped]) 
            ELSE 0 
        END AS AverageCost
    FROM dbo.ReceivingsLineItems AS r
    LEFT OUTER JOIN dbo.POP30330 AS p 
        ON r.[Receipt Line Number] = p.RCPTLNNM 
        AND r.[Item Number] = p.ITEMNMBR 
        AND r.[POP Receipt Number] = p.POPRCTNM
    WHERE r.[Created Date] > CONVERT(DATETIME, '2020-01-01 00:00:00', 102)
        AND r.[Item Tracking Option] <> 'None'
        AND p.SERLTNUM IS NOT NULL
        AND NOT (r.[Item Number] LIKE '20.%')
    GROUP BY r.[Item Number], r.[Item Description], r.[U Of M]
),

-- ============================================================================
-- CTE 2: DemandHistory
-- Purpose: Aggregate weekly demand from manufacturing history
-- NOTE: Uses Config.Demand_Lookback_Years (rolling window, not hardcoded year)
-- ============================================================================
DemandHistory AS (
    SELECT 
        Component,
        DATEPART(WEEK, IssueDate) AS WeekNum,
        YEAR(IssueDate) AS YearNum,
        SUM(IssueQty) AS WeeklyDemand,
        UoM
    FROM dbo.PHR_MO_CostCalc1
    -- Issue 8: Demand_Lookback_Years from Config (replaces hardcoded year 2024)
    WHERE YEAR(IssueDate) >= YEAR(GETDATE()) - (SELECT Demand_Lookback_Years FROM Config)
    GROUP BY Component, DATEPART(WEEK, IssueDate), YEAR(IssueDate), UoM
),

-- ============================================================================
-- CTE 3: DemandStats
-- Purpose: Calculate demand statistics for safety stock
-- ============================================================================
DemandStats AS (
    SELECT 
        Component,
        AVG(WeeklyDemand) AS AvgWeeklyDemand,
        STDEV(WeeklyDemand) AS StdDevWeekly,
        MAX(WeeklyDemand) AS MaxWeeklyDemand,
        SUM(WeeklyDemand) AS TotalDemand,
        UoM
    FROM DemandHistory
    GROUP BY Component, UoM
),

-- ============================================================================
-- CTE 4: SSCalculation
-- Purpose: Calculate safety stock values with lead time considerations
-- ============================================================================
SSCalculation AS (
    SELECT 
        ss.ITEMNMBR,
        ss.VendorItem,
        COALESCE(NULLIF(ss.PRIME_VNDR, ''), 'UNASSIGNED') AS PRIME_VNDR,  -- Issue 4: NULL vendor handling

        -- Issue 8: Lead time sourced from Config CTE (replaces scattered magic numbers)
        CASE 
            WHEN ss.ITEMNMBR LIKE '30.%' THEN (SELECT Lead_Days_Series_30 FROM Config)
            WHEN ss.ITEMNMBR LIKE '10.%' THEN (SELECT Lead_Days_Series_10 FROM Config)
            ELSE                               (SELECT Lead_Days_Default   FROM Config)
        END AS LeadDays,

        ac.TotalQtyShipped,
        ac.[U Of M] AS PurchasingUOM,
        ac.TotalCost,
        ac.AverageCost,
        ds.AvgWeeklyDemand,
        ds.StdDevWeekly,
        ds.MaxWeeklyDemand,
        ds.UoM AS MfgUOM,

        -- Safety stock in manufacturing UOM
        -- Formula: 2 × (MaxWeekly - AvgWeekly) × (LeadDays / 7)
        ROUND((2.0 * (ds.MaxWeeklyDemand - ds.AvgWeeklyDemand)) 
              * (CAST(
                    CASE WHEN ss.ITEMNMBR LIKE '30.%' THEN (SELECT Lead_Days_Series_30 FROM Config)
                         WHEN ss.ITEMNMBR LIKE '10.%' THEN (SELECT Lead_Days_Series_10 FROM Config)
                         ELSE                              (SELECT Lead_Days_Default   FROM Config)
                    END
                AS FLOAT) / 7.0), 0) AS CalculatedSS_MfgUOM,

        -- Safety stock value = CalculatedSS_MfgUOM × AverageCost
        ROUND((2.0 * (ds.MaxWeeklyDemand - ds.AvgWeeklyDemand)) 
              * (CAST(
                    CASE WHEN ss.ITEMNMBR LIKE '30.%' THEN (SELECT Lead_Days_Series_30 FROM Config)
                         WHEN ss.ITEMNMBR LIKE '10.%' THEN (SELECT Lead_Days_Series_10 FROM Config)
                         ELSE                              (SELECT Lead_Days_Default   FROM Config)
                    END
                AS FLOAT) / 7.0) * ac.AverageCost, 2) AS SSValue,

        -- UOM conversion factor: manufacturing qty per purchasing unit
        ROUND(CAST(ds.TotalDemand AS FLOAT) / NULLIF(ac.TotalQtyShipped, 0), 2) AS PURUOM,

        -- Safety stock in purchasing UOM = CEILING(CalculatedSS_MfgUOM / PURUOM)
        CEILING(CAST(ROUND((2.0 * (ds.MaxWeeklyDemand - ds.AvgWeeklyDemand)) 
              * (CAST(
                    CASE WHEN ss.ITEMNMBR LIKE '30.%' THEN (SELECT Lead_Days_Series_30 FROM Config)
                         WHEN ss.ITEMNMBR LIKE '10.%' THEN (SELECT Lead_Days_Series_10 FROM Config)
                         ELSE                              (SELECT Lead_Days_Default   FROM Config)
                    END
                AS FLOAT) / 7.0), 0) AS FLOAT)
              / NULLIF(ROUND(CAST(ds.TotalDemand AS FLOAT) / NULLIF(ac.TotalQtyShipped, 0), 2), 0)) AS CalculatedSS_PurchasingUOM

    FROM dbo.ETB_SS AS ss
    INNER JOIN TempAvgCost AS ac ON ss.ITEMNMBR = ac.[Item Number]
    LEFT OUTER JOIN DemandStats AS ds ON ss.ITEMNMBR = ds.Component
    WHERE ss.VendorItem IS NOT NULL 
        AND ss.VendorItem <> '' 
        AND ss.INCLUDE_MRP = 'YES' 
        AND ss.VendorItem NOT LIKE '%**%'
)

-- ============================================================================
-- Final SELECT
-- ============================================================================
SELECT 
    ITEMNMBR, 
    VendorItem, 
    PRIME_VNDR, 
    LeadDays, 
    TotalQtyShipped, 
    PurchasingUOM, 
    TotalCost, 
    AverageCost, 
    AvgWeeklyDemand, 
    StdDevWeekly, 
    MaxWeeklyDemand, 
    MfgUOM, 
    CalculatedSS_MfgUOM, 
    SSValue, 
    PURUOM, 
    CalculatedSS_PurchasingUOM
FROM SSCalculation
-- Issue 8: SS_Value_Ceiling from Config (replaces hardcoded 20000)
WHERE SSValue <= (SELECT SS_Value_Ceiling FROM Config)
ORDER BY ITEMNMBR;
