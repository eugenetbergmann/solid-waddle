WITH threatened_clients_detail AS
(
    SELECT DISTINCT ITEMNMBR, PRIME_VNDR, ItemDescription, UOM, Construct
    FROM            dbo.ETB_PAB_SUPPLY_ACTION
    WHERE        Suppression_Status <> 'BEGINNING BALANCE' 
                  AND Deficit_Qty > 0 
                  AND Demand_Due_Date IS NOT NULL 
                  AND Construct IS NOT NULL
),
client_summary AS
(
    SELECT        ITEMNMBR, PRIME_VNDR, 
                  MAX(ItemDescription) AS ItemDescription,
                  MAX(UOM) AS UOM,
                  STRING_AGG(Construct, ', ') WITHIN GROUP (ORDER BY Construct) AS Threatened_Clients, 
                  COUNT(*) AS Client_Exposure_Count
    FROM            threatened_clients_detail
    GROUP BY ITEMNMBR, PRIME_VNDR
),
deficit_rows AS
(
    SELECT        p.ITEMNMBR, p.PRIME_VNDR, p.ItemDescription, p.UOM, p.Demand_Due_Date, p.Deficit_Qty, p.WFQ_Extended_Status, 
                  cs.Threatened_Clients, cs.Client_Exposure_Count, 
                  MIN(p.Demand_Due_Date) OVER (PARTITION BY p.ITEMNMBR, p.PRIME_VNDR) AS First_Stockout_Date, 
                  SUM(CASE WHEN p.Deficit_Qty > 0 THEN p.Deficit_Qty ELSE 0 END) OVER (PARTITION BY p.ITEMNMBR, p.PRIME_VNDR) AS Total_Deficit_Qty, 
                  MAX(CASE WHEN p.WFQ_Extended_Status IN ('WFQ_RESCUED', 'WFQ_ENHANCED') THEN 1 ELSE 0 END) OVER (PARTITION BY p.ITEMNMBR, p.PRIME_VNDR) AS WFQ_Dependency_Flag
    FROM            dbo.ETB_PAB_SUPPLY_ACTION p 
    LEFT JOIN client_summary cs ON p.ITEMNMBR = cs.ITEMNMBR AND p.PRIME_VNDR = cs.PRIME_VNDR
    WHERE        p.Suppression_Status <> 'BEGINNING BALANCE' 
                  AND p.Deficit_Qty > 0 
                  AND p.Demand_Due_Date IS NOT NULL
),
with_threat AS
(
    SELECT        d.ITEMNMBR, 
                  COALESCE(d.PRIME_VNDR, ss.PRIME_VNDR) AS PRIME_VNDR, 
                  d.ItemDescription, d.UOM,
                  d.Threatened_Clients, d.Client_Exposure_Count, d.First_Stockout_Date, d.Total_Deficit_Qty, d.WFQ_Dependency_Flag, 
                  ISNULL(ss.LeadDays, 30) AS LeadDays, 
                  DATEDIFF(DAY, CAST(GETDATE() AS DATE), d.First_Stockout_Date) AS Days_To_Stockout, 
                  CASE WHEN d.First_Stockout_Date IS NOT NULL AND d.First_Stockout_Date <= DATEADD(DAY, ISNULL(ss.LeadDays, 30), CAST(GETDATE() AS DATE)) THEN 1 ELSE 0 END AS Schedule_Threat
    FROM            deficit_rows d 
    LEFT JOIN dbo.ETB_SS_CALC ss ON d.ITEMNMBR = ss.ITEMNMBR AND d.PRIME_VNDR = ss.PRIME_VNDR
)
SELECT DISTINCT ITEMNMBR, PRIME_VNDR, ItemDescription, UOM, Threatened_Clients, Client_Exposure_Count, First_Stockout_Date, Days_To_Stockout, Total_Deficit_Qty, WFQ_Dependency_Flag, Schedule_Threat, LeadDays
FROM            with_threat;
