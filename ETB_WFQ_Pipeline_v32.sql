CREATE VIEW [dbo].[ETB_WFQ_Pipeline_v32] AS
/*
================================================================================
CDMO Sovereign Schedule Deduction Engine v3.2 - Tier 2 WFQ Pipeline Health
================================================================================
Purpose: Standalone view for WFQ inventory pipeline health and forecast analysis.
         Processes ONLY WFQ data from dbo.ETBNO_WFR - no demand/MO/order logic.

Tier 2 Specifications:
- Lot-level accuracy with item-level aggregation for forecast output
- Series-based SOP targets (21 days for '10' series, 14 days for others)
- Velocity Index = Lot_Age_Days / SOP_Target_Days
- Risk Zones: GREEN (<SOP), YELLOW (SOP to 21), ORANGE (21 to 31), RED (>=31)
- Pressure Score: 0-100 scale based on age progression through zones
- Valid_Qty_For_Supply: Only VALID lots with >90 days shelf life remaining
- Pipeline Health: HEALTHY (<SOP), CONCERN (SOP to 21), CRITICAL (>=21)

Output: 
- ITEM_LEVEL rows (primary output for Tier 3 joins on Item_Number)
- LOT_LEVEL detail rows (for KPIs and debugging)

Author: CDMO Development Team
Version: 3.2
================================================================================
*/

WITH WFQ_Lots AS (
    -- Base extraction: Filter WFQ lots with positive quantity and valid expiration
    SELECT 
        Item_Number,
        Description,
        SITE,
        Bin,
        [Date in Bin] AS DATERECD,
        LOT_Number,
        EXPNDATE,
        UoM,
        [QTY_ON HAND] AS Qty_Available,
        
        -- Series extraction for SOP target determination
        LEFT(Item_Number, 2) AS Series,
        
        -- SOP Target Days: 21 for Series '10' (controlled substances), 14 for others
        CASE 
            WHEN LEFT(Item_Number, 2) = '10' THEN 21 
            ELSE 14 
        END AS SOP_Target_Days,
        
        -- Lot Age: Days since material was received/placed in WFQ bin
        DATEDIFF(DAY, [Date in Bin], GETDATE()) AS Lot_Age_Days,
        
        -- Estimated Release Date: DATERECD + SOP_Target_Days per v3.2 spec
        DATEADD(DAY, 
            CASE WHEN LEFT(Item_Number, 2) = '10' THEN 21 ELSE 14 END, 
            [Date in Bin]
        ) AS Estimated_Release_Date,
        
        -- Valid Expiration: >90 days shelf life remaining from today
        CASE 
            WHEN EXPNDATE > DATEADD(DAY, 90, GETDATE()) THEN 1 
            ELSE 0 
        END AS Valid_Expiration
    FROM dbo.ETBNO_WFR
    WHERE SITE = 'WFQ'  -- Filter to WFQ inventory only
      AND [QTY_ON HAND] > 0  -- Exclude zero-quantity lots
      AND EXPNDATE IS NOT NULL  -- Ensure expiration date exists for validity check
),

Lot_Metrics AS (
    -- Calculate per-lot metrics: Velocity Index, Risk Zone, Pressure Score, Penalty Flag
    SELECT 
        *,
        
        -- Velocity Index: Age as percentage of SOP target (float for precise trending)
        CAST(Lot_Age_Days AS FLOAT) / 
            CASE WHEN SOP_Target_Days > 0 THEN SOP_Target_Days ELSE 1 END 
            AS Velocity_Index,
        
        -- Risk Zone: Categorize lot age relative to SOP targets and escalation thresholds
        CASE 
            -- GREEN: Within SOP target (on track for release)
            WHEN Lot_Age_Days < SOP_Target_Days THEN 'GREEN'
            
            -- YELLOW: SOP to 21 days (monitoring zone)
            WHEN Lot_Age_Days >= SOP_Target_Days AND Lot_Age_Days < 21 THEN 'YELLOW'
            
            -- ORANGE: 21 to 31 days (escalation risk)
            WHEN Lot_Age_Days >= 21 AND Lot_Age_Days < 31 THEN 'ORANGE'
            
            -- RED: 31+ days (critical/suppression zone)
            WHEN Lot_Age_Days >= 31 THEN 'RED'
            
            ELSE 'UNKNOWN'
        END AS Risk_Zone,
        
        -- Pressure Score: 0-100 scale measuring pipeline stress
        -- GREEN: 0 (no pressure)
        -- YELLOW: 0-50 (ramping pressure as approaches 21 days)
        -- ORANGE: 50-100 (escalating pressure to critical threshold)
        -- RED: 100 (critical/suppressed)
        CASE 
            WHEN Lot_Age_Days < SOP_Target_Days THEN 0
            
            WHEN Lot_Age_Days >= SOP_Target_Days AND Lot_Age_Days < 21 THEN 
                ((Lot_Age_Days - SOP_Target_Days) / 
                    CASE WHEN (21.0 - SOP_Target_Days) > 0 THEN (21.0 - SOP_Target_Days) ELSE 1 END
                ) * 50
            
            WHEN Lot_Age_Days >= 21 AND Lot_Age_Days < 31 THEN 
                50 + (((Lot_Age_Days - 21) / 
                    CASE WHEN (31.0 - 21.0) > 0 THEN (31.0 - 21.0) ELSE 1 END
                ) * 50)
            
            WHEN Lot_Age_Days >= 31 THEN 100
            
            ELSE 0
        END AS Pressure_Score,
        
        -- Penalty Flag: Mark lots for nullification/suppression
        -- Lots >= 31 days are flagged as NULL_SUPPLY (excluded from valid supply calculation)
        CASE 
            WHEN Lot_Age_Days >= 31 THEN 'NULL_SUPPLY' 
            ELSE 'VALID' 
        END AS Penalty_Flag,
        
        -- Days To/From Estimated Release: Positive = future, Negative = past due
        DATEDIFF(DAY, GETDATE(), Estimated_Release_Date) AS Days_To_Estimated_Release,
        
        -- Valid Quantity For Supply: Only VALID lots with acceptable shelf life
        -- Excludes NULL_SUPPLY lots AND lots with <=90 days remaining shelf life
        CASE 
            WHEN Penalty_Flag = 'VALID' 
             AND Valid_Expiration = 1 
            THEN Qty_Available 
            ELSE 0 
        END AS Valid_Qty_For_Supply
    FROM WFQ_Lots
),

Item_Aggregated AS (
    -- Aggregate lot-level metrics to item-level for forecast output
    SELECT 
        Item_Number,
        Description,
        UoM,
        
        -- Total quantities in WFQ pipeline
        SUM(Qty_Available) AS Total_Qty_In_Testing,
        SUM(CASE WHEN Penalty_Flag = 'VALID' THEN Qty_Available ELSE 0 END) AS Valid_Qty_In_Testing,
        SUM(Valid_Qty_For_Supply) AS Valid_Qty_For_Supply,
        
        -- Release date range for valid lots (for planning horizon)
        MIN(CASE 
            WHEN Penalty_Flag = 'VALID' AND Valid_Expiration = 1 
            THEN Estimated_Release_Date 
            ELSE NULL 
        END) AS Earliest_Release_Date,
        
        MAX(CASE 
            WHEN Penalty_Flag = 'VALID' AND Valid_Expiration = 1 
            THEN Estimated_Release_Date 
            ELSE NULL 
        END) AS Latest_Release_Date,
        
        -- Week bucket calculations with proper year-boundary handling
        -- Uses ISO week logic: DATEPART(WEEK, date) combined with DATEPART(YEAR, date)
        -- to correctly handle weeks that cross year boundaries
        
        -- Release This Week: Current ISO week of current year
        SUM(CASE 
            WHEN Penalty_Flag = 'VALID' AND Valid_Expiration = 1 
             AND DATEPART(ISO_WEEK, Estimated_Release_Date) = DATEPART(ISO_WEEK, GETDATE())
             AND DATEPART(YEAR, Estimated_Release_Date) = DATEPART(YEAR, GETDATE())
            THEN Qty_Available 
            ELSE 0 
        END) AS Release_This_Week,
        
        -- Release Next Week: Next ISO week (handles year boundary via DATEPART(YEAR))
        SUM(CASE 
            WHEN Penalty_Flag = 'VALID' AND Valid_Expiration = 1 
             AND DATEPART(ISO_WEEK, Estimated_Release_Date) = 
                 CASE 
                     -- Handle year boundary: Week 52/53 to Week 1 transition
                     WHEN DATEPART(ISO_WEEK, GETDATE()) >= 52 
                     THEN 1 
                     ELSE DATEPART(ISO_WEEK, GETDATE()) + 1 
                 END
             AND (
                 -- Normal case: same year
                 (DATEPART(ISO_WEEK, GETDATE()) < 52 AND DATEPART(YEAR, Estimated_Release_Date) = DATEPART(YEAR, GETDATE()))
                 OR
                 -- Year boundary case: Week 52/53 from prev year maps to Week 1 of current year
                 (DATEPART(ISO_WEEK, GETDATE()) >= 52 AND DATEPART(ISO_WEEK, Estimated_Release_Date) = 1)
             )
            THEN Qty_Available 
            ELSE 0 
        END) AS Release_Next_Week,
        
        -- Release Week 3: Two weeks ahead
        SUM(CASE 
            WHEN Penalty_Flag = 'VALID' AND Valid_Expiration = 1 
             AND DATEPART(ISO_WEEK, Estimated_Release_Date) = 
                 CASE 
                     WHEN DATEPART(ISO_WEEK, GETDATE()) >= 51 THEN 1 
                     ELSE DATEPART(ISO_WEEK, GETDATE()) + 2 
                 END
             AND (
                 (DATEPART(ISO_WEEK, GETDATE()) < 51 AND DATEPART(YEAR, Estimated_Release_Date) = DATEPART(YEAR, GETDATE()))
                 OR
                 (DATEPART(ISO_WEEK, GETDATE()) >= 51 AND DATEPART(ISO_WEEK, Estimated_Release_Date) <= 2)
             )
            THEN Qty_Available 
            ELSE 0 
        END) AS Release_Week_3,
        
        -- Release Week 4 Plus: 3+ weeks ahead (cumulative)
        SUM(CASE 
            WHEN Penalty_Flag = 'VALID' AND Valid_Expiration = 1 
             AND (
                 -- Case 1: More than 3 weeks ahead within current year
                 (DATEPART(ISO_WEEK, GETDATE()) <= 50 
                  AND DATEPART(ISO_WEEK, Estimated_Release_Date) >= DATEPART(ISO_WEEK, GETDATE()) + 3
                  AND DATEPART(YEAR, Estimated_Release_Date) = DATEPART(YEAR, GETDATE()))
                 OR
                 -- Case 2: Future year releases (year boundary scenario)
                 (DATEPART(ISO_WEEK, GETDATE()) >= 51 
                  AND (
                      (DATEPART(ISO_WEEK, Estimated_Release_Date) >= 3 AND DATEPART(YEAR, Estimated_Release_Date) = DATEPART(YEAR, GETDATE()))
                      OR
                      (DATEPART(ISO_WEEK, Estimated_Release_Date) >= 1 AND DATEPART(YEAR, Estimated_Release_Date) > DATEPART(YEAR, GETDATE()))
                  ))
             )
            THEN Qty_Available 
            ELSE 0 
        END) AS Release_Week_4_Plus,
        
        -- Gap Detection: Flag if any week 1-4 has zero valid release despite overall supply
        -- "YES" if weeks 1-4 have gaps but total Valid_Qty_For_Supply > 0
        CASE 
            WHEN SUM(CASE 
                WHEN Penalty_Flag = 'VALID' AND Valid_Expiration = 1 
                 AND (
                     (DATEPART(ISO_WEEK, Estimated_Release_Date) BETWEEN 
                      DATEPART(ISO_WEEK, GETDATE()) AND DATEPART(ISO_WEEK, GETDATE()) + 3
                      AND DATEPART(YEAR, Estimated_Release_Date) = DATEPART(YEAR, GETDATE()))
                     OR
                     (DATEPART(ISO_WEEK, GETDATE()) >= 51 
                      AND DATEPART(ISO_WEEK, Estimated_Release_Date) BETWEEN 1 AND 3)
                 )
                THEN Qty_Available 
                ELSE 0 
            END) = 0
             AND SUM(CASE WHEN Penalty_Flag = 'VALID' AND Valid_Expiration = 1 THEN Qty_Available ELSE 0 END) > 0
            THEN 'YES' 
            ELSE 'NO' 
        END AS Release_Gap_In_Next_4_Weeks,
        
        -- Pipeline health metrics (averaged across valid lots only)
        -- WFQ Index Average Age: Mean age of VALID lots
        AVG(CASE 
            WHEN Penalty_Flag = 'VALID' 
            THEN CAST(Lot_Age_Days AS FLOAT) 
            ELSE NULL 
        END) AS WFQ_Index_Avg_Age,
        
        -- Average Velocity Index across valid lots
        AVG(CASE 
            WHEN Penalty_Flag = 'VALID' 
            THEN CAST(Velocity_Index AS FLOAT) 
            ELSE NULL 
        END) AS Avg_Velocity_Index,
        
        -- Average Pressure Score across valid lots
        AVG(CASE 
            WHEN Penalty_Flag = 'VALID' 
            THEN CAST(Pressure_Score AS FLOAT) 
            ELSE NULL 
        END) AS Avg_Pressure_Score,
        
        -- Quantity distribution by risk zone
        SUM(CASE WHEN Risk_Zone = 'GREEN' THEN Qty_Available ELSE 0 END) AS Qty_Green,
        SUM(CASE WHEN Risk_Zone = 'YELLOW' THEN Qty_Available ELSE 0 END) AS Qty_Yellow,
        SUM(CASE WHEN Risk_Zone = 'ORANGE' THEN Qty_Available ELSE 0 END) AS Qty_Orange,
        SUM(CASE WHEN Risk_Zone = 'RED' THEN Qty_Available ELSE 0 END) AS Qty_Red,
        SUM(CASE WHEN Penalty_Flag = 'NULL_SUPPLY' THEN Qty_Available ELSE 0 END) AS Qty_Nulled
    FROM Lot_Metrics
    GROUP BY Item_Number, Description, UoM
)

-- Final output: ITEM_LEVEL rows (primary for Tier 3 join) + optional LOT_LEVEL for debugging
SELECT 
    'ITEM_LEVEL' AS View_Level,
    Item_Number,
    Description,
    UoM,
    Total_Qty_In_Testing,
    Valid_Qty_In_Testing,
    Valid_Qty_For_Supply,
    Earliest_Release_Date,
    Latest_Release_Date,
    Release_This_Week,
    Release_Next_Week,
    Release_Week_3,
    Release_Week_4_Plus,
    Release_Gap_In_Next_4_Weeks,
    WFQ_Index_Avg_Age,
    Avg_Velocity_Index,
    Avg_Pressure_Score,
    Qty_Green,
    Qty_Yellow,
    Qty_Orange,
    Qty_Red,
    Qty_Nulled,
    
    -- Pipeline Health Status per v3.2 spec
    -- HEALTHY: Average age < SOP target for the series
    -- CONCERN: Average age >= SOP target but < 21 days
    -- CRITICAL: Average age >= 21 days
    CASE 
        WHEN LEFT(Item_Number, 2) = '10' THEN
            -- Series '10' has 21-day SOP target
            CASE 
                WHEN WFQ_Index_Avg_Age < 21 THEN 'HEALTHY'
                WHEN WFQ_Index_Avg_Age >= 21 AND WFQ_Index_Avg_Age < 31 THEN 'CONCERN'
                WHEN WFQ_Index_Avg_Age >= 31 THEN 'CRITICAL'
                ELSE 'UNKNOWN'
            END
        ELSE
            -- All other series have 14-day SOP target
            CASE 
                WHEN WFQ_Index_Avg_Age < 14 THEN 'HEALTHY'
                WHEN WFQ_Index_Avg_Age >= 14 AND WFQ_Index_Avg_Age < 31 THEN 'CONCERN'
                WHEN WFQ_Index_Avg_Age >= 31 THEN 'CRITICAL'
                ELSE 'UNKNOWN'
            END
    END AS Pipeline_Health_Status
FROM Item_Aggregated

UNION ALL

-- Optional: LOT_LEVEL detail rows for debugging, KPIs, and granular analysis
SELECT 
    'LOT_LEVEL' AS View_Level,
    Item_Number,
    NULL AS Description,  -- Description not needed at lot level
    NULL AS UoM,          -- UoM not needed at lot level
    Qty_Available AS Total_Qty_In_Testing,
    CASE WHEN Penalty_Flag = 'VALID' THEN Qty_Available ELSE 0 END AS Valid_Qty_In_Testing,
    Valid_Qty_For_Supply,
    Estimated_Release_Date AS Earliest_Release_Date,  -- Same value for lot-level
    Estimated_Release_Date AS Latest_Release_Date,
    NULL AS Release_This_Week,       -- Not applicable at lot level
    NULL AS Release_Next_Week,
    NULL AS Release_Week_3,
    NULL AS Release_Week_4_Plus,
    NULL AS Release_Gap_In_Next_4_Weeks,
    Lot_Age_Days AS WFQ_Index_Avg_Age,
    Velocity_Index AS Avg_Velocity_Index,
    Pressure_Score AS Avg_Pressure_Score,
    CASE WHEN Risk_Zone = 'GREEN' THEN Qty_Available ELSE 0 END AS Qty_Green,
    CASE WHEN Risk_Zone = 'YELLOW' THEN Qty_Available ELSE 0 END AS Qty_Yellow,
    CASE WHEN Risk_Zone = 'ORANGE' THEN Qty_Available ELSE 0 END AS Qty_Orange,
    CASE WHEN Risk_Zone = 'RED' THEN Qty_Available ELSE 0 END AS Qty_Red,
    CASE WHEN Penalty_Flag = 'NULL_SUPPLY' THEN Qty_Available ELSE 0 END AS Qty_Nulled,
    -- Combined status for lot-level view
    CASE 
        WHEN Penalty_Flag = 'NULL_SUPPLY' THEN 'RED (NULL_SUPPLY)'
        ELSE Risk_Zone + ' (' + Penalty_Flag + ')'
    END AS Pipeline_Health_Status
FROM Lot_Metrics;

-- No ORDER BY in view definition - ORDER should be applied at query time if needed
GO