-- =====================================================
-- VIEW 2: ETB_BUYER_CONTROL
-- PO consolidation and buyer action engine
-- =====================================================

CREATE VIEW dbo.ETB_BUYER_CONTROL
AS
WITH demand_base AS (
    SELECT 
        p.PRIME_VNDR,
        p.ITEMNMBR,
        p.Demand_Due_Date,
        p.Deficit_Qty,
        ISNULL(ss.LeadDays, 30) AS LeadDays,
        ISNULL(ss.CalculatedSS_PurchasingUOM, 0) AS CalculatedSS_PurchasingUOM,
        DATEADD(
            DAY,
            -(DATEDIFF(DAY, 0, p.Demand_Due_Date) % ISNULL(ss.LeadDays, 30)),
            p.Demand_Due_Date
        ) AS Bucket_Date
    FROM dbo.ETB_PAB_SUPPLY_ACTION p
    INNER JOIN dbo.ETB_SS_CALC ss 
        ON p.ITEMNMBR = ss.ITEMNMBR 
       AND p.PRIME_VNDR = ss.PRIME_VNDR
    WHERE p.Suppression_Status <> 'BEGINNING BALANCE'
      AND p.Deficit_Qty > 0
      AND p.Demand_Due_Date IS NOT NULL
),
aggregated AS (
    SELECT 
        PRIME_VNDR,
        ITEMNMBR,
        Bucket_Date,
        LeadDays,
        CalculatedSS_PurchasingUOM,
        MIN(Demand_Due_Date) AS Earliest_Demand_Date,
        COUNT(*) AS Demand_Lines_In_Bucket,
        SUM(CASE WHEN Deficit_Qty > 0 THEN Deficit_Qty ELSE 0 END) 
            + MAX(CalculatedSS_PurchasingUOM)
            AS Recommended_PO_Qty,
        SUM(CASE WHEN Deficit_Qty > 0 THEN Deficit_Qty ELSE 0 END)
            OVER (PARTITION BY PRIME_VNDR)
            AS Vendor_Total_Exposure,
        ROW_NUMBER() OVER (
            PARTITION BY PRIME_VNDR, ITEMNMBR, Bucket_Date 
            ORDER BY MIN(Demand_Due_Date) ASC
        ) AS Consolidation_Order
    FROM demand_base
    GROUP BY 
        PRIME_VNDR,
        ITEMNMBR,
        Bucket_Date,
        LeadDays,
        CalculatedSS_PurchasingUOM
)
SELECT 
    PRIME_VNDR,
    ITEMNMBR,
    Bucket_Date,
    Recommended_PO_Qty,
    Earliest_Demand_Date,
    CASE 
        WHEN Earliest_Demand_Date <= DATEADD(DAY, LeadDays, CAST(GETDATE() AS DATE))
            THEN 'PLACE_NOW'
        WHEN Earliest_Demand_Date <= DATEADD(DAY, LeadDays * 2, CAST(GETDATE() AS DATE))
            THEN 'PLAN'
        ELSE 'MONITOR'
    END AS Urgency,
    Demand_Lines_In_Bucket,
    Vendor_Total_Exposure,
    LeadDays,
    CalculatedSS_PurchasingUOM
FROM aggregated
WHERE Consolidation_Order = 1;

GO
