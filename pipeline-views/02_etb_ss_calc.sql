-- ============================================================================
-- VIEW: ETB_SS_CALC
-- Purpose: Safety stock calculations based on demand history and lead times
-- Author: Zo Computer
-- Date: 2026-02-26
-- Dependencies: dbo.ReceivingsLineItems, dbo.POP30330, dbo.PHR_MO_CostCalc1,
--               dbo.ETB_SS
-- ============================================================================

-- ============================================================================
-- CTE 1: TempAvgCost
-- Purpose: Calculate average cost from receivings data
-- ============================================================================
WITH TempAvgCost AS (
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
-- NOTE: Uses dynamic years (last 2 years) instead of hardcoded values
-- ============================================================================
DemandHistory AS (
    SELECT 
        Component,
        DATEPART(WEEK, IssueDate) AS WeekNum,
        YEAR(IssueDate) AS YearNum,
        SUM(IssueQty) AS WeeklyDemand,
        UoM
    FROM dbo.PHR_MO_CostCalc1
    WHERE YEAR(IssueDate) >= YEAR(GETDATE()) - 1  -- Dynamic: last 2 years
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
        COALESCE(NULLIF(ss.PRIME_VNDR, ''), 'UNASSIGNED') AS PRIME_VNDR,  -- NULL vendor handling
        -- Lead time based on item series
        CASE 
            WHEN ss.ITEMNMBR LIKE '30.%' THEN 100
            WHEN ss.ITEMNMBR LIKE '10.%' THEN 60
            ELSE 45
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
        ROUND((2.0 * (ds.MaxWeeklyDemand - ds.AvgWeeklyDemand)) 
              * ((CASE WHEN ss.ITEMNMBR LIKE '30.%' THEN 100 WHEN ss.ITEMNMBR LIKE '10.%' THEN 60 ELSE 45 END) / 7.0), 0) AS CalculatedSS_MfgUOM,
        -- Safety stock value
        ROUND((2.0 * (ds.MaxWeeklyDemand - ds.AvgWeeklyDemand)) 
              * ((CASE WHEN ss.ITEMNMBR LIKE '30.%' THEN 100 WHEN ss.ITEMNMBR LIKE '10.%' THEN 60 ELSE 45 END) / 7.0) * ac.AverageCost, 2) AS SSValue,
        -- UOM conversion factor
        ROUND(CAST(ds.TotalDemand AS FLOAT) / NULLIF(ac.TotalQtyShipped, 0), 2) AS PURUOM,
        -- Safety stock in purchasing UOM
        CEILING(CAST(ROUND((2.0 * (ds.MaxWeeklyDemand - ds.AvgWeeklyDemand)) 
              * ((CASE WHEN ss.ITEMNMBR LIKE '30.%' THEN 100 WHEN ss.ITEMNMBR LIKE '10.%' THEN 60 ELSE 45 END) / 7.0), 0) AS FLOAT) 
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
WHERE SSValue <= 20000
ORDER BY ITEMNMBR;
