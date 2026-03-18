SELECT ITEMNMBR, MAX(ItemDescription) AS ItemDescription, MAX(UOM) AS UOM, MAX(PRIME_VNDR) AS Vendor, MAX(VendorItem) AS VendorItem, MAX(PURCHASING_LT) AS PURCHASING_LT, MIN(Demand_Due_Date) AS First_Deficit_Date, 
             SUM(CASE WHEN Deficit_Qty > 0 THEN Deficit_Qty ELSE 0 END) AS Total_Deficit_Unconstrained, SUM(ISNULL(POs_On_Order_Qty, 0)) AS Total_PO_Qty_On_Order, MAX(CASE WHEN WFQ_Extended_Status = 'RESCUE' THEN 1 ELSE 0 END) AS WFQ_Rescue_Flag, 
             SUM(CASE WHEN Construct = 'ORDER' THEN 1 ELSE 0 END) AS Count_Order, SUM(CASE WHEN Construct = 'BOTH' THEN 1 ELSE 0 END) AS Count_Both, SUM(CASE WHEN Is_Past_Due_In_Backlog = 1 THEN 1 ELSE 0 END) AS Urgent_Count, SUM(ISNULL(Additional_Order_Qty, 0)) 
             AS Suggested_Order_Qty, MAX(CAST(GETDATE() AS DATE)) AS Analysis_Date, /* CUSTOMER STOCKOUT FLAGS */ MAX(CASE WHEN FG = '291' AND Deficit_Qty > 0 THEN 1 ELSE 0 END) AS [291], MAX(CASE WHEN FG = '295' AND Deficit_Qty > 0 THEN 1 ELSE 0 END) AS [295], 
             MAX(CASE WHEN FG = '298' AND Deficit_Qty > 0 THEN 1 ELSE 0 END) AS [298], MAX(CASE WHEN FG = '301' AND Deficit_Qty > 0 THEN 1 ELSE 0 END) AS [301], MAX(CASE WHEN FG = '303' AND Deficit_Qty > 0 THEN 1 ELSE 0 END) AS [303]
FROM   dbo.ETB_SUPPLY_ACTION
/* change if your base table/view name differs*/ GROUP BY ITEMNMBR)
    SELECT ITEMNMBR, ItemDescription, UOM, Vendor, VendorItem, PURCHASING_LT, TRY_CAST(PURCHASING_LT AS INT) AS Purchasing_LT_Int, First_Deficit_Date, Total_Deficit_Unconstrained, Total_PO_Qty_On_Order, WFQ_Rescue_Flag, Count_Order, Count_Both, Urgent_Count, 
                Suggested_Order_Qty, Analysis_Date, [291], [295], [298], [301], [303], /* Latest Safe Order Date */ DATEADD(DAY, - TRY_CAST(PURCHASING_LT AS INT), First_Deficit_Date) AS Latest_Order_Date, /* Priority */ CASE WHEN DATEADD(DAY, - TRY_CAST(PURCHASING_LT AS INT), 
                First_Deficit_Date) < CAST(GETDATE() AS DATE) THEN 'LATE' WHEN First_Deficit_Date <= DATEADD(DAY, 5, CAST(GETDATE() AS DATE)) THEN 'URGENT' ELSE 'NORMAL' END AS Priority, /* Customer Exposure */ CONCAT_WS(',', CASE WHEN [291] = 1 THEN '291' END, 
                CASE WHEN [295] = 1 THEN '295' END, CASE WHEN [298] = 1 THEN '298' END, CASE WHEN [301] = 1 THEN '301' END, CASE WHEN [303] = 1 THEN '303' END) AS Customer_Risk, /* Vendor Execution Order */ ROW_NUMBER() OVER (PARTITION BY Vendor
   ORDER BY CASE WHEN DATEADD(DAY, - TRY_CAST(PURCHASING_LT AS INT), First_Deficit_Date) < CAST(GETDATE() AS DATE) THEN 1 WHEN First_Deficit_Date <= DATEADD(DAY, 5, CAST(GETDATE() AS DATE)) THEN 2 ELSE 3 END, First_Deficit_Date, Suggested_Order_Qty DESC) 
AS Vendor_Execution_Order
FROM   Item_Aggregation;
