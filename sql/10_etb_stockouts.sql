-- ============================================================================
-- VIEW: ETB_STOCKOUTS
-- Purpose: 180-day forward stockout summary per item
--          Aggregates active, clean-data demand rows from ETB_PAB_SUPPLY_ACTION
--          Includes program flags (291/295/298/301/303), max deficit, PO totals,
--          WFQ rescues, and supply action counts (ORDER/BOTH/urgent)
-- Report type: 'RALPH_LOOP_180D_STOCKOUTS'
-- Integrated: March 2026 — KILO loop (replaces prior client-specific stockout views)
-- Consumes: dbo.ETB_PAB_SUPPLY_ACTION (View 5 — fixed upstream)
-- ============================================================================

CREATE VIEW dbo.ETB_STOCKOUTS
AS
WITH Windowed AS (
    SELECT 
        s.ITEMNMBR,
        s.ItemDescription,
        s.UOM,
        s.Construct,
        s.Demand_Due_Date,
        s.Inventory_Qty_Available,
        s.Adjusted_Running_Balance,
        s.Deficit_Qty,
        s.POs_On_Order_Qty,
        s.WFQ_Extended_Status,
        s.Suppression_Status,
        s.Net_Demand,
        s.Data_Quality_Flag,
        s.Supply_Action_Recommendation
    FROM dbo.ETB_PAB_SUPPLY_ACTION s WITH (NOLOCK)
    WHERE s.Demand_Due_Date >= CAST(GETDATE() AS date)
      AND s.Demand_Due_Date < DATEADD(DAY, 181, CAST(GETDATE() AS date))
      AND ISNULL(s.Net_Demand, 0) > 0
      AND s.Suppression_Status NOT IN (
          'BEGINNING BALANCE',
          'SUPPRESSED: Stale & Unissued',
          'SUPPRESSED: Full Coverage in Fence'
      )
      AND s.Data_Quality_Flag = 'CLEAN'
)
SELECT 
    w.ITEMNMBR                          AS Item_Number,
    MAX(w.ItemDescription)              AS Description,
    MAX(w.UOM)                          AS Unit_Of_Measure,
    MAX(w.Inventory_Qty_Available)      AS On_Hand_Qty,
    MIN(w.Adjusted_Running_Balance)     AS Min_Projected_Stockout,
    MIN(w.Demand_Due_Date)              AS First_Deficit_Date,
    1                                   AS Item_Stockout_Flag,
    MAX(CASE WHEN TRY_CAST(w.Construct AS int) = 291 THEN 1 ELSE 0 END) AS Program_291_Flag,
    MAX(CASE WHEN TRY_CAST(w.Construct AS int) = 295 THEN 1 ELSE 0 END) AS Program_295_Flag,
    MAX(CASE WHEN TRY_CAST(w.Construct AS int) = 298 THEN 1 ELSE 0 END) AS Program_298_Flag,
    MAX(CASE WHEN TRY_CAST(w.Construct AS int) = 301 THEN 1 ELSE 0 END) AS Program_301_Flag,
    MAX(CASE WHEN TRY_CAST(w.Construct AS int) = 303 THEN 1 ELSE 0 END) AS Program_303_Flag,
    MAX(ISNULL(w.Deficit_Qty, 0))       AS Max_Deficit_180D,
    SUM(ISNULL(w.POs_On_Order_Qty, 0))  AS Total_PO_Qty_On_Order,
    SUM(CASE WHEN w.WFQ_Extended_Status = 'WFQ_RESCUED' THEN 1 ELSE 0 END) AS WFQ_Rescue_Count,
    SUM(CASE WHEN w.Supply_Action_Recommendation = 'ORDER' THEN 1 ELSE 0 END) AS COUNT_ORDER,
    SUM(CASE WHEN w.Supply_Action_Recommendation = 'BOTH'  THEN 1 ELSE 0 END) AS COUNT_BOTH,
    SUM(CASE WHEN w.Supply_Action_Recommendation IN ('ORDER', 'BOTH')
             AND w.Demand_Due_Date <= DATEADD(DAY, 10, CAST(GETDATE() AS date))
             THEN 1 ELSE 0 END) AS URGENT_COUNT,
    CAST(GETDATE() AS date)             AS Analysis_Date,
    'RALPH_LOOP_180D_STOCKOUTS'         AS Report_Type
FROM Windowed w
GROUP BY w.ITEMNMBR
HAVING MIN(w.Adjusted_Running_Balance) < 0;
