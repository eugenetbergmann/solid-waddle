/*
================================================================================
ETB_BUYER_CONTROL - Enhanced Buyer Control View
================================================================================
Purpose: Provide buyers with prioritized purchase recommendations including
         vendor fallback logic, holding cost calculations, and EOQ optimization.

Key Enhancements:
1. Vendor Fallback Hierarchy: SS_CALC -> PAB_SUPPLY_ACTION -> PAB_WFQ_ADJ -> FALLBACK
2. Holding Cost Calculations: Annual carrying cost based on inventory value
3. EOQ-based Recommendations: Economic order quantity for items without SS data
4. Data Quality Flags: Track completeness of source data

Change Log:
- 2026-02-13: Added vendor fallback logic, holding costs, EOQ calculations
================================================================================
*/

WITH filter_supply AS
(
    /*
    Step 1: Filter supply action to only deficit items with actual demand dates.
    Excludes BEGINNING BALANCE entries which are planning artifacts.
    */
    SELECT        ITEMNMBR, 
                  PRIME_VNDR, 
                  ItemDescription, 
                  UOM, 
                  Demand_Due_Date, 
                  Deficit_Qty,
                  Construct
    FROM            dbo.ETB_PAB_SUPPLY_ACTION
    WHERE        Suppression_Status <> 'BEGINNING BALANCE' 
                  AND Deficit_Qty > 0 
                  AND Demand_Due_Date IS NOT NULL
),

/*
================================================================================
VENDOR FALLBACK HIERARCHY
================================================================================
Priority 1: ETB_SS_CALC - Validated items with historical demand patterns
Priority 2: ETB_PAB_SUPPLY_ACTION - Items with active supply recommendations
Priority 3: ETB_PAB_WFQ_ADJ - Items with work-for-queue allocations
Priority 4: FALLBACK - 'UNASSIGNED' placeholder for unmatched items
================================================================================
*/
vendor_fallback AS
(
    SELECT        fs.ITEMNMBR,
                  -- Vendor hierarchy: SS_CALC -> PAB_SUPPLY_ACTION -> PAB_WFQ_ADJ -> FALLBACK
                  COALESCE(
                      ss.PRIME_VNDR,           -- Priority 1: Safety stock calc vendor
                      fs.PRIME_VNDR,           -- Priority 2: Supply action vendor
                      wfq.PRIME_VNDR,          -- Priority 3: WFQ adjustment vendor
                      'UNASSIGNED'             -- Priority 4: Fallback placeholder
                  ) AS PRIME_VNDR,
                  
                  -- Track the source of vendor data for audit trail
                  CASE 
                      WHEN ss.PRIME_VNDR IS NOT NULL THEN 'ETB_SS_CALC'
                      WHEN fs.PRIME_VNDR IS NOT NULL THEN 'ETB_PAB_SUPPLY_ACTION'
                      WHEN wfq.PRIME_VNDR IS NOT NULL THEN 'ETB_PAB_WFQ_ADJ'
                      ELSE 'FALLBACK'
                  END AS Vendor_Data_Source,
                  
                  MAX(fs.ItemDescription) AS ItemDescription,
                  MAX(fs.UOM) AS UOM,
                  MIN(fs.Demand_Due_Date) AS First_Stockout_Date,
                  SUM(fs.Deficit_Qty) AS Total_Deficit_Qty,
                  COUNT(DISTINCT fs.Demand_Due_Date) AS Demand_Lines_In_Bucket,
                  
                  -- Lead days: Use SS_CALC if available, otherwise default to 30 days
                  COALESCE(ss.LeadDays, 30) AS LeadDays,
                  
                  -- Safety stock: Use SS_CALC if available, otherwise 0
                  COALESCE(ss.CalculatedSS_PurchasingUOM, 0) AS CalculatedSS_PurchasingUOM,
                  
                  -- Track SS data source for transparency
                  CASE 
                      WHEN ss.ITEMNMBR IS NOT NULL THEN 'CALCULATED'
                      ELSE 'DEFAULT_30_DAYS'
                  END AS SS_Data_Source,
                  
                  -- Flag completeness of SS data
                  CASE 
                      WHEN ss.ITEMNMBR IS NOT NULL AND ss.CalculatedSS_PurchasingUOM IS NOT NULL THEN 'YES'
                      WHEN ss.ITEMNMBR IS NOT NULL THEN 'PARTIAL'
                      ELSE 'NO'
                  END AS Has_Complete_SS_Data,
                  
                  -- Calculate annual demand for EOQ (approximate from deficit patterns)
                  -- Using deficit as proxy for demand when historical data unavailable
                  SUM(fs.Deficit_Qty) * 12 AS Annual_Demand_Estimate
                  
    FROM            filter_supply fs
    LEFT JOIN       dbo.ETB_SS_CALC ss 
                    ON fs.ITEMNMBR = ss.ITEMNMBR 
                    AND fs.PRIME_VNDR = ss.PRIME_VNDR
    LEFT JOIN       dbo.ETB_PAB_WFQ_ADJ wfq
                    ON fs.ITEMNMBR = wfq.ITEMNMBR
                    AND fs.PRIME_VNDR = wfq.PRIME_VNDR
    GROUP BY        fs.ITEMNMBR,
                    COALESCE(ss.PRIME_VNDR, fs.PRIME_VNDR, wfq.PRIME_VNDR, 'UNASSIGNED'),
                    ss.PRIME_VNDR,
                    fs.PRIME_VNDR,
                    wfq.PRIME_VNDR,
                    ss.ITEMNMBR,
                    ss.LeadDays,
                    ss.CalculatedSS_PurchasingUOM
),

/*
================================================================================
COST LOOKUP
================================================================================
Sources unit costs from multiple tables:
Priority 1: ETB_SS_CALC.AverageCost - Primary cost source
Priority 2: POP30330 - Purchase order history cost table
Priority 3: NULL - Flagged for manual review
================================================================================
*/
cost_lookup AS
(
    SELECT        vf.*,
                  -- Unit cost hierarchy: SS_CALC -> POP30330 -> NULL
                  COALESCE(
                      ss.AverageCost,
                      pop.UnitCost
                  ) AS Unit_Cost,
                  
                  -- Track cost data source
                  CASE 
                      WHEN ss.AverageCost IS NOT NULL THEN 'ETB_SS_CALC'
                      WHEN pop.UnitCost IS NOT NULL THEN 'POP30330'
                      ELSE 'MISSING'
                  END AS Cost_Data_Source,
                  
                  -- SS value for holding cost percentage calculation
                  ss.SSValue,
                  ss.CalculatedSS_MfgUOM
                  
    FROM            vendor_fallback vf
    LEFT JOIN       dbo.ETB_SS_CALC ss
                    ON vf.ITEMNMBR = ss.ITEMNMBR
                    AND vf.PRIME_VNDR = ss.PRIME_VNDR
    LEFT JOIN       (
                        -- Get most recent unit cost from PO history
                        SELECT      ITEMNMBR,
                                    PRIME_VNDR,
                                    AVG(UnitCost) AS UnitCost
                        FROM        dbo.POP30330
                        WHERE       UnitCost IS NOT NULL 
                                    AND UnitCost > 0
                        GROUP BY    ITEMNMBR, PRIME_VNDR
                    ) pop
                    ON vf.ITEMNMBR = pop.ITEMNMBR
                    AND vf.PRIME_VNDR = pop.PRIME_VNDR
),

/*
================================================================================
HOLDING COST CALCULATIONS
================================================================================
Holding_Cost_Pct: Annual carrying cost as percentage of inventory value
  - If SS_CALC data available: Derive from SSValue / (SS_Qty * Unit_Cost)
  - If not available: Use 25% industry standard

Holding_Cost_Annual: Cost to hold average inventory for one year
  Formula: (Recommended_PO_Qty / 2) * Unit_Cost * Holding_Cost_Pct
  (Dividing by 2 assumes average inventory is half of order quantity)

Order_Cost_Estimate: Fixed cost per purchase order (default $50)

Total_Carrying_Cost: Combined holding + ordering costs
================================================================================
*/
holding_cost_calc AS
(
    SELECT        cl.*,
                  -- Holding cost percentage: derived or default 25%
                  CASE 
                      WHEN cl.SSValue IS NOT NULL 
                           AND cl.CalculatedSS_MfgUOM IS NOT NULL 
                           AND cl.CalculatedSS_MfgUOM > 0
                           AND cl.Unit_Cost IS NOT NULL
                           AND cl.Unit_Cost > 0
                      THEN 
                          -- Derive from: SSValue / (SS_Qty * Unit_Cost)
                          -- This gives us the implied carrying cost percentage
                          CASE 
                              WHEN (cl.CalculatedSS_MfgUOM * cl.Unit_Cost) > 0
                              THEN cl.SSValue / (cl.CalculatedSS_MfgUOM * cl.Unit_Cost)
                              ELSE 0.25
                          END
                      ELSE 0.25  -- Industry standard 25% annual carrying cost
                  END AS Holding_Cost_Pct,
                  
                  -- Fixed order cost per PO (can be parameterized later)
                  50.00 AS Order_Cost_Estimate
                  
    FROM            cost_lookup cl
),

/*
================================================================================
EOQ AND FINAL RECOMMENDATIONS
================================================================================
Recommended_PO_Qty (Original): Simple deficit + safety stock sum

Recommended_PO_Qty_Optimized: 
  - If complete SS data: Use original calculation (deficit + SS)
  - If partial/no SS data: Use EOQ formula with deficit as minimum
  
EOQ Formula: SQRT((2 * Annual_Demand * Order_Cost) / Holding_Cost_Pct)

Data_Quality_Flag: Indicates completeness of supporting data
================================================================================
*/
final_output AS
(
    SELECT        hcc.PRIME_VNDR,
                  hcc.ITEMNMBR,
                  hcc.ItemDescription,
                  hcc.UOM,
                  hcc.First_Stockout_Date AS Earliest_Demand_Date,
                  
                  -- Original recommended quantity (preserved for audit trail)
                  hcc.Total_Deficit_Qty + hcc.CalculatedSS_PurchasingUOM AS Recommended_PO_Qty,
                  
                  hcc.Demand_Lines_In_Bucket,
                  
                  -- Vendor exposure rollup
                  SUM(hcc.Total_Deficit_Qty) OVER (PARTITION BY hcc.PRIME_VNDR) AS Vendor_Total_Exposure,
                  
                  hcc.LeadDays,
                  hcc.CalculatedSS_PurchasingUOM,
                  
                  -- Urgency classification based on lead time vs stockout date
                  CASE 
                      WHEN hcc.First_Stockout_Date <= DATEADD(DAY, hcc.LeadDays, CAST(GETDATE() AS DATE)) 
                      THEN 'PLACE_NOW'
                      WHEN hcc.First_Stockout_Date <= DATEADD(DAY, hcc.LeadDays * 2, CAST(GETDATE() AS DATE)) 
                      THEN 'PLAN'
                      ELSE 'MONITOR'
                  END AS Urgency,
                  
                  -- ========================================
                  -- NEW HOLDING COST COLUMNS
                  -- ========================================
                  
                  -- Holding cost percentage (annual carrying cost %)
                  CAST(hcc.Holding_Cost_Pct AS DECIMAL(10, 4)) AS Holding_Cost_Pct,
                  
                  -- Unit cost from available sources
                  CAST(hcc.Unit_Cost AS DECIMAL(18, 6)) AS Unit_Cost,
                  
                  -- Annual holding cost for recommended quantity
                  -- Formula: (Avg Inventory) * Unit Cost * Holding Cost %
                  -- Avg Inventory = Recommended_Qty / 2
                  CAST(
                      CASE 
                          WHEN hcc.Unit_Cost IS NOT NULL
                          THEN ((hcc.Total_Deficit_Qty + hcc.CalculatedSS_PurchasingUOM) / 2.0) 
                               * hcc.Unit_Cost 
                               * hcc.Holding_Cost_Pct
                          ELSE NULL
                      END AS DECIMAL(18, 2)
                  ) AS Holding_Cost_Annual,
                  
                  -- Fixed order cost estimate
                  CAST(hcc.Order_Cost_Estimate AS DECIMAL(18, 2)) AS Order_Cost_Estimate,
                  
                  -- Total carrying cost (holding + ordering)
                  CAST(
                      CASE 
                          WHEN hcc.Unit_Cost IS NOT NULL
                          THEN (((hcc.Total_Deficit_Qty + hcc.CalculatedSS_PurchasingUOM) / 2.0) 
                               * hcc.Unit_Cost 
                               * hcc.Holding_Cost_Pct)
                               + hcc.Order_Cost_Estimate
                          ELSE hcc.Order_Cost_Estimate
                      END AS DECIMAL(18, 2)
                  ) AS Total_Carrying_Cost,
                  
                  -- ========================================
                  -- OPTIMIZED PO QUANTITY (EOQ-based)
                  -- ========================================
                  
                  -- EOQ-based recommendation with deficit as floor
                  CAST(
                      CASE 
                          -- If we have complete SS data, use traditional calculation
                          WHEN hcc.Has_Complete_SS_Data = 'YES'
                          THEN hcc.Total_Deficit_Qty + hcc.CalculatedSS_PurchasingUOM
                          
                          -- If partial or no SS data, use EOQ formula with safeguards
                          ELSE
                              CASE
                                  -- Calculate EOQ if we have the necessary components
                                  WHEN hcc.Annual_Demand_Estimate > 0 
                                       AND hcc.Holding_Cost_Pct > 0 
                                       AND hcc.Unit_Cost > 0
                                  THEN
                                      -- EOQ = SQRT((2 * Annual_Demand * Order_Cost) / (Unit_Cost * Holding_Cost_Pct))
                                      -- Note: We use Unit_Cost * Holding_Cost_Pct as the holding cost per unit per year
                                      GREATEST(
                                          hcc.Total_Deficit_Qty,  -- Never order less than deficit
                                          SQRT(
                                              (2.0 * hcc.Annual_Demand_Estimate * hcc.Order_Cost_Estimate)
                                              / NULLIF(hcc.Unit_Cost * hcc.Holding_Cost_Pct, 0)
                                          )
                                      )
                                  
                                  -- Fallback: 1.5x deficit if EOQ can't be calculated
                                  ELSE hcc.Total_Deficit_Qty * 1.5
                              END
                      END AS DECIMAL(18, 2)
                  ) AS Recommended_PO_Qty_Optimized,
                  
                  -- ========================================
                  -- DATA SOURCE TRACKING
                  -- ========================================
                  
                  hcc.Vendor_Data_Source,
                  hcc.SS_Data_Source,
                  hcc.Has_Complete_SS_Data,
                  
                  -- Data quality flag for buyer awareness
                  CASE 
                      WHEN hcc.Vendor_Data_Source = 'FALLBACK' 
                           AND (hcc.Unit_Cost IS NULL OR hcc.Cost_Data_Source = 'MISSING')
                      THEN 'MISSING_BOTH'
                      WHEN hcc.Vendor_Data_Source = 'FALLBACK'
                      THEN 'MISSING_VENDOR'
                      WHEN hcc.Unit_Cost IS NULL OR hcc.Cost_Data_Source = 'MISSING'
                      THEN 'MISSING_COST'
                      ELSE 'CLEAN'
                  END AS Data_Quality_Flag,
                  
                  -- Cost data source for transparency
                  hcc.Cost_Data_Source
                  
    FROM            holding_cost_calc hcc
)

SELECT        PRIME_VNDR,
              ITEMNMBR,
              ItemDescription,
              UOM,
              Earliest_Demand_Date,
              Recommended_PO_Qty,
              Demand_Lines_In_Bucket,
              Vendor_Total_Exposure,
              LeadDays,
              CalculatedSS_PurchasingUOM,
              Urgency,
              Holding_Cost_Pct,
              Unit_Cost,
              Holding_Cost_Annual,
              Order_Cost_Estimate,
              Total_Carrying_Cost,
              Recommended_PO_Qty_Optimized,
              Vendor_Data_Source,
              SS_Data_Source,
              Has_Complete_SS_Data,
              Data_Quality_Flag,
              Cost_Data_Source
FROM            final_output
ORDER BY        Urgency,           -- Most urgent first
              Vendor_Total_Exposure DESC,  -- Highest exposure within urgency
              Earliest_Demand_Date;        -- Earliest stockout date
