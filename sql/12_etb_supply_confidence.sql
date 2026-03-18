SELECT s.ITEMNMBR, s.ItemDescription, s.UOM, s.Construct, s.Demand_Due_Date, s.Inventory_Qty_Available, s.Adjusted_Running_Balance, s.Deficit_Qty, s.POs_On_Order_Qty, s.WFQ_Extended_Status, s.Suppression_Status, s.Net_Demand, s.Data_Quality_Flag, 
             s.Supply_Action_Recommendation, /* Added 2026-03-16: vendor and planning fields for order session */ s.PRIME_VNDR, s.PURCHASING_LT, s.ORDER_POINT_QTY
FROM   dbo.ETB_SUPPLY_ACTION s WITH (NOLOCK)
WHERE s.Data_Quality_Flag = 'CLEAN' AND (/* Demand rows */ (ISNULL(s.Net_Demand, 0) > 0 AND s.Suppression_Status NOT IN ('BEGINNING BALANCE', 'SUPPRESSED: Stale & Unissued', 'SUPPRESSED: Full Coverage in Fence')) OR
             /* PO/supply rows */ s.POs_On_Order_Qty > 0)), BegBal AS
    (SELECT ITEMNMBR, BEG_BAL AS Beginning_Balance
    FROM    dbo.ETB_SUPPLY_ACTION WITH (NOLOCK)
    WHERE Suppression_Status = 'BEGINNING BALANCE' AND Data_Quality_Flag = 'CLEAN')
    SELECT w.ITEMNMBR AS Item_Number, MAX(w.ItemDescription) AS Description, MAX(w.UOM) AS Unit_Of_Measure, MAX(b.Beginning_Balance) AS Beginning_Balance, MIN(w.Adjusted_Running_Balance) AS Min_Projected_Balance, MIN(w.Demand_Due_Date) AS First_Deficit_Date, 
                1 AS Item_Stockout_Flag, MAX(CASE WHEN TRY_CAST(w.Construct AS int) = 291 THEN 1 ELSE 0 END) AS [291], MAX(CASE WHEN TRY_CAST(w.Construct AS int) = 295 THEN 1 ELSE 0 END) AS [295], MAX(CASE WHEN TRY_CAST(w.Construct AS int) = 298 THEN 1 ELSE 0 END) 
                AS [298], MAX(CASE WHEN TRY_CAST(w.Construct AS int) = 301 THEN 1 ELSE 0 END) AS [301], MAX(CASE WHEN TRY_CAST(w.Construct AS int) = 303 THEN 1 ELSE 0 END) AS [303], MAX(ISNULL(w.Deficit_Qty, 0)) AS Max_Deficit_Unconstrained, SUM(ISNULL(w.POs_On_Order_Qty, 
                0)) AS Total_PO_Qty_On_Order, SUM(CASE WHEN w.WFQ_Extended_Status = 'WFQ_RESCUED' THEN 1 ELSE 0 END) AS WFQ_Rescue_Count, SUM(CASE WHEN w.Supply_Action_Recommendation = 'ORDER' THEN 1 ELSE 0 END) AS Count_Order, 
                SUM(CASE WHEN w.Supply_Action_Recommendation = 'BOTH' THEN 1 ELSE 0 END) AS Count_Both, SUM(CASE WHEN w.Supply_Action_Recommendation IN ('ORDER', 'BOTH') AND w.Demand_Due_Date <= DATEADD(DAY, 10, CAST(GETDATE() AS date)) THEN 1 ELSE 0 END) 
                AS Urgent_Count, /* Added 2026-03-16: dominant vendor per item — MAX over ORDER/BOTH rows, excludes UNASSIGNED */ MAX(CASE WHEN w.Supply_Action_Recommendation IN ('ORDER', 'BOTH') AND ISNULL(w.PRIME_VNDR, '') NOT IN ('', 'UNASSIGNED') 
                THEN w.PRIME_VNDR ELSE NULL END) AS Vendor, /* Added 2026-03-16: lead time and MOQ for order session */ MAX(CASE WHEN w.Supply_Action_Recommendation IN ('ORDER', 'BOTH') THEN w.PURCHASING_LT ELSE NULL END) AS Purchasing_LT, 
                MAX(CASE WHEN w.Supply_Action_Recommendation IN ('ORDER', 'BOTH') THEN w.ORDER_POINT_QTY ELSE NULL END) AS MOQ, CAST(GETDATE() AS date) AS Analysis_Date
   FROM    Windowed w LEFT JOIN
                BegBal b ON b.ITEMNMBR = w.ITEMNMBR
   GROUP BY w.ITEMNMBR
   HAVING MIN(w.Adjusted_Running_Balance) < 0;
