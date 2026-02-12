SELECT        ITEMNMBR, PRIME_VNDR, Demand_Due_Date, Deficit_Qty
FROM            dbo.ETB_PAB_SUPPLY_ACTION
WHERE        Suppression_Status <> 'BEGINNING BALANCE' AND Deficit_Qty > 0 AND Demand_Due_Date IS NOT NULL), risk_data AS
    (SELECT        fs.ITEMNMBR, fs.PRIME_VNDR, MIN(fs.Demand_Due_Date) AS First_Stockout_Date, SUM(fs.Deficit_Qty) AS Total_Deficit_Qty, COUNT(DISTINCT fs.Demand_Due_Date) AS Demand_Lines_In_Bucket, ss.LeadDays, 
                                ss.CalculatedSS_PurchasingUOM
      FROM            filter_supply fs LEFT JOIN
                                dbo.ETB_SS_CALC ss ON fs.ITEMNMBR = ss.ITEMNMBR AND fs.PRIME_VNDR = ss.PRIME_VNDR
      GROUP BY fs.ITEMNMBR, fs.PRIME_VNDR, ss.LeadDays, ss.CalculatedSS_PurchasingUOM)
    SELECT        PRIME_VNDR, ITEMNMBR, First_Stockout_Date AS Earliest_Demand_Date, Total_Deficit_Qty + ISNULL(CalculatedSS_PurchasingUOM, 0) AS Recommended_PO_Qty, Demand_Lines_In_Bucket, SUM(Total_Deficit_Qty) 
                              OVER (PARTITION BY PRIME_VNDR) AS Vendor_Total_Exposure, ISNULL(LeadDays, 30) AS LeadDays, CalculatedSS_PurchasingUOM, CASE WHEN First_Stockout_Date <= DATEADD(DAY, ISNULL(LeadDays, 30), 
                              CAST(GETDATE() AS DATE)) THEN 'PLACE_NOW' WHEN First_Stockout_Date <= DATEADD(DAY, ISNULL(LeadDays, 30) * 2, CAST(GETDATE() AS DATE)) THEN 'PLAN' ELSE 'MONITOR' END AS Urgency
     FROM            risk_data;
