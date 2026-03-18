SELECT 7 AS Stale_Suppression_Days, /* Business rule: MOs older than 7 days with zero issued qty are noise. */ 7 AS Fence_Suppression_Days, 
             /* Business rule: if inventory covers demand due within 7 days, suppress. */ 7 AS Early_Issue_Flag_Days/* Early_Issue_Flag fires. Mirrors Stale_Suppression_Days by design. */ ), 
/* ============================================================================ */ p_norm AS
    (SELECT p.*, /* These characters appear in order numbers from the ERP import process. */ UPPER(LTRIM(RTRIM(CONVERT(varchar(255), REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(p.ORDERNUMBER, 'MO', ''), '-', ''), ' ', ''), '/', ''), '.', ''), '#', ''))))) AS CleanOrder, 
                 LTRIM(RTRIM(p.ITEMNMBR)) AS CleanItem, /* currency symbols and scientific notation, which CAST would reject */ ISNULL(TRY_CAST(LTRIM(RTRIM(p.Deductions)) AS decimal(18, 5)), 0) AS CleanDeductions
    FROM    dbo.ETB_PAB_MO p
    WHERE p.STSDESCR <> 'Partially Received' AND p.STSDESCR <> 'SCRAP' AND LTRIM(RTRIM(p.ITEMNMBR)) NOT LIKE '60.%' AND LTRIM(RTRIM(p.ITEMNMBR)) NOT LIKE '70.%'), 
/* ============================================================================ */ m_norm AS
    (SELECT m.*, /* Issue 7: Identical REPLACE chain — must stay in sync with p_norm. */ UPPER(LTRIM(RTRIM(CONVERT(varchar(255), REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(m.ORDERNUMBER, 'MO', ''), '-', ''), ' ', ''), '/', ''), '.', ''), '#', ''))))) AS CleanOrder, ROW_NUMBER() 
                 OVER (PARTITION BY UPPER(LTRIM(RTRIM(CONVERT(varchar(255), REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(m.ORDERNUMBER, 'MO', ''), '-', ''), ' ', ''), '/', ''), '.', ''), '#', ''))))), m.FG
    ORDER BY m.Customer, m.[FG Desc], m.ORDERNUMBER) AS rn_fg
FROM   dbo.ETB_ActiveDemand_Union_FG_MO m), /* ============================================================================ */ item_desc AS
    (SELECT [Item Number] AS ItemNumber, ITEMDESC AS ItemDescription, UOMSCHDL
    FROM    dbo.Prosenthal_Vendor_Items
    WHERE Active = 'Yes'), /* ============================================================================ */ joined AS
    (SELECT CAST(COALESCE (NULLIF (LTRIM(RTRIM(p_norm.ORDERNUMBER)), ''), NULLIF (LTRIM(RTRIM(m_norm.ORDERNUMBER)), '')) AS varchar(255)) AS ORDERNUMBER, CAST(ISNULL(m_norm.Customer, '') AS varchar(255)) AS Construct, CAST(ISNULL(m_norm.FG, '') AS varchar(255)) 
                 AS FG, CAST(ISNULL(m_norm.[FG Desc], '') AS varchar(255)) AS [FG Desc], CAST(ISNULL(p_norm.ITEMNMBR, '') AS varchar(255)) AS ITEMNMBR, CAST(ISNULL(item_desc.ItemDescription, '') AS varchar(500)) AS ItemDescription, CAST(ISNULL(item_desc.UOMSCHDL, '') 
                 AS varchar(50)) AS UOMSCHDL, p_norm.STSDESCR, p_norm.DUEDATE, p_norm.[Expiry Dates], p_norm.[Date + Expiry], p_norm.MRPTYPE, p_norm.VendorItem, /* through the pipeline and are flagged for data-quality review. */ COALESCE (NULLIF (p_norm.PRIME_VNDR, ''), 
                 'UNASSIGNED') AS PRIME_VNDR, /* Issue 4: Track which source provided the vendor value */ CASE WHEN p_norm.PRIME_VNDR IS NOT NULL AND LTRIM(RTRIM(p_norm.PRIME_VNDR)) <> '' THEN 'PAB_MO' ELSE 'UNASSIGNED' END AS Vendor_Data_Source, 
                 p_norm.PURCHASING_LT, p_norm.PLANNING_LT, p_norm.ORDER_POINT_QTY, p_norm.SAFETY_STOCK, p_norm.Deductions AS Original_Deductions, p_norm.Expiry AS Original_Expiry, p_norm.[PO's] AS Original_POs, p_norm.Running_Balance AS Original_Running_Balance, 
                 /* Issue 6: TRY_CAST replaces ISNUMERIC for BEG_BAL */ ISNULL(TRY_CAST(LTRIM(RTRIM(p_norm.BEG_BAL)) AS decimal(18, 6)), 0) AS BEG_BAL_Num, p_norm.CleanOrder, p_norm.CleanItem, p_norm.CleanDeductions
    FROM    p_norm LEFT JOIN
                 m_norm ON p_norm.CleanOrder = m_norm.CleanOrder AND m_norm.rn_fg = 1 LEFT JOIN
                 item_desc ON p_norm.ITEMNMBR = item_desc.ItemNumber), /* ============================================================================ */ ranked AS
    (SELECT *, ROW_NUMBER() OVER (PARTITION BY ORDERNUMBER, FG, ITEMNMBR, DUEDATE
    /* FIX: added DUEDATE to preserve multi-date demand rows */ ORDER BY Construct, [FG Desc], STSDESCR) AS rn_final
FROM   joined), /* ============================================================================ */ Core AS
    (SELECT *
    FROM    ranked
    WHERE rn_final = 1), /* ============================================================================ */ ledger_ranked AS
    (SELECT RTRIM(LTRIM(a.MANUFACTUREORDER_I)) AS CleanMO, RTRIM(LTRIM(a.ITEMNMBR)) AS ITEMNMBR, CAST(a.MRPISSUEDATE_I AS date) AS MRP_IssueDate, a.WCID_I, a.QTY_ISSUED_I + a.QTY_BACKFLUSHED_I AS Total_Issued, 
                 a.MRPAMOUNT_I - a.ATYALLOC - a.QTY_ISSUED_I - a.QTY_BACKFLUSHED_I AS Remaining_Required, a.MRPAMOUNT_I AS Required_Qty, /* rn_qty: dedup when quantity matches exactly (most precise match) */ ROW_NUMBER() OVER (PARTITION BY 
                 RTRIM(LTRIM(a.MANUFACTUREORDER_I)), RTRIM(LTRIM(a.ITEMNMBR)), a.MRPAMOUNT_I
    ORDER BY CAST(a.MRPISSUEDATE_I AS date) DESC) AS rn_qty, /* rn_any: fallback — any MO+item match, prefer rows with issued qty */ ROW_NUMBER() OVER (PARTITION BY RTRIM(LTRIM(a.MANUFACTUREORDER_I)), RTRIM(LTRIM(a.ITEMNMBR))
ORDER BY CASE WHEN (a.QTY_ISSUED_I + a.QTY_BACKFLUSHED_I) > 0 THEN 1 ELSE 2 END, ABS(a.MRPAMOUNT_I) DESC, CAST(a.MRPISSUEDATE_I AS date) DESC) AS rn_any
FROM   dbo.PK010033 a WITH (NOLOCK) LEFT JOIN
             dbo.IV00101 b WITH (NOLOCK) ON a.ITEMNMBR = b.ITEMNMBR
WHERE EXISTS
                 (SELECT 1
                 FROM    dbo.WO010032 w WITH (NOLOCK)
                 WHERE w.MANUFACTUREORDERST_I IN (2, 3) AND RTRIM(LTRIM(w.MANUFACTUREORDER_I)) = RTRIM(LTRIM(a.MANUFACTUREORDER_I)))), /* ============================================================================ */ Final AS
    (SELECT Core.*, /* Ledger-derived fields (NULL-safe defaults) */ ISNULL(ml.MRP_IssueDate, '') AS MRP_IssueDate, ISNULL(ml.WCID_I, '') AS WCID_From_MO, ISNULL(ml.Total_Issued, 0) AS Issued, ISNULL(ml.Remaining_Required, 0) AS Remaining, CASE WHEN ISNULL(ml.Total_Issued, 0) 
                 > 0 THEN 'YES' ELSE 'NO' END AS Has_Issued, /* Flag when ledger issue date differs from PAB expiry date */ CASE WHEN ml.MRP_IssueDate IS NULL OR
                 Core.[Date + Expiry] IS NULL THEN 'NO' WHEN ml.MRP_IssueDate <> TRY_CAST(Core.[Date + Expiry] AS date) THEN 'YES' ELSE 'NO' END AS IssueDate_Mismatch, /* Config CTE value = 7; see Config for business rationale */ CASE WHEN ISNULL(ml.Total_Issued, 0) > 0 AND 
                 Core.[Date + Expiry] IS NOT NULL AND TRY_CAST(Core.[Date + Expiry] AS date) < DATEADD(DAY, -
                     (SELECT cfg.Early_Issue_Flag_Days
                     FROM    Config cfg), CAST(GETDATE() AS date)) THEN 'YES' ELSE 'NO' END AS Early_Issue_Flag, /* Unified display value for debugging / cross-reference */ CASE WHEN ml.Required_Qty IS NULL THEN CONCAT(Core.ITEMNMBR, ' - ', Core.[Date + Expiry], ' - ', 
                 Core.CleanDeductions) ELSE CONCAT(Core.ITEMNMBR, ' - ', Core.[Date + Expiry], ' - ', ml.Required_Qty - ml.Total_Issued) END AS Unified_Value, /* Rows where WCID_From_MO does not match are flagged for review. */ CASE WHEN ISNULL(ml.WCID_I, '') 
                 = '' THEN 'NO_WC' WHEN ml.WCID_I LIKE 'WC-W%' THEN 'VALID_WC' ELSE 'NON_WC_SITE' END AS WC_Site_Validation, /* Issue 5: Row-level data quality flag */ CASE WHEN COALESCE (NULLIF (Core.PRIME_VNDR, ''), 'UNASSIGNED') 
                 = 'UNASSIGNED' THEN 'MISSING_VENDOR' ELSE 'CLEAN' END AS Data_Quality_Flag
    FROM    Core LEFT JOIN
                 ledger_ranked ml ON Core.CleanOrder = ml.CleanMO AND Core.CleanItem = ml.ITEMNMBR AND ml.rn_any = 1/* Single deterministic match per MO+Item (fixes 3x row duplication bug) */ )
    /* ============================================================================ */ SELECT ITEMNMBR, ItemDescription, UOMSCHDL AS UOM, ORDERNUMBER, Construct, DUEDATE, [Expiry Dates], [Date + Expiry], CAST(BEG_BAL_Num AS varchar(50)) AS BEG_BAL, 
                Original_Deductions AS Deductions, Original_Expiry AS Expiry, Original_POs AS [PO's], Original_Running_Balance AS Running_Balance, MRP_IssueDate, WCID_From_MO, Issued, Remaining, Has_Issued, IssueDate_Mismatch, Early_Issue_Flag, WC_Site_Validation, VendorItem, 
                PRIME_VNDR, Vendor_Data_Source, /* Issue 4: vendor source tracking */ Data_Quality_Flag, /* Issue 5: row-level data quality */ PURCHASING_LT, PLANNING_LT, ORDER_POINT_QTY, SAFETY_STOCK, FG, [FG Desc], STSDESCR, MRPTYPE, Unified_Value
   FROM    Final;
