SELECT        ITEMNMBR, ItemDescription, UOM, ORDERNUMBER, Construct, DUEDATE, [Expiry Dates], [Date + Expiry], BEG_BAL, Deductions, Expiry, [PO's], Running_Balance, MRP_IssueDate, WCID_From_MO, Issued, Remaining, 
                         Has_Issued, IssueDate_Mismatch, Early_Issue_Flag, VendorItem, PRIME_VNDR, PURCHASING_LT, PLANNING_LT, ORDER_POINT_QTY, SAFETY_STOCK, FG, [FG Desc], STSDESCR, MRPTYPE, Unified_Value
FROM            dbo.ETB_PAB_AUTO), InventoryAgg AS
    (SELECT        Item_Number, SITE, SUM(QTY_Available) AS Total_QTY_Available
      FROM            dbo.Prosenthal_INV_BIN_QTY_wQTYTYPE
      WHERE        SITE LIKE 'WC-W%' AND DATEDIFF(DAY, DATERECD, GETDATE()) <= 45
      GROUP BY Item_Number, SITE), WithInventory AS
    (SELECT        b.*, ISNULL(inv.Total_QTY_Available, 0) AS Inventory_Qty_Available
      FROM            Base b LEFT JOIN
                                InventoryAgg inv ON b.ITEMNMBR = inv.Item_Number AND b.WCID_From_MO = inv.SITE), Flags AS
    (SELECT        wi.*, /* reporting net demand (does not change RB) */ CASE WHEN Inventory_Qty_Available > 0 AND Inventory_Qty_Available < Remaining THEN Remaining - Inventory_Qty_Available ELSE Remaining END AS Net_Demand, 
                                /* BEG_BAL row id per your data */ CASE WHEN LTRIM(RTRIM(ORDERNUMBER)) = 'Beg Bal' THEN 1 ELSE 0 END AS Is_BegBal_Row, 
                                /* suppression rules apply to demand rows only */ CASE WHEN LTRIM(RTRIM(ORDERNUMBER)) = 'Beg Bal' THEN 0 WHEN DUEDATE <= DATEADD(DAY, - 7, CAST(GETDATE() AS date)) AND ISNULL(Issued, 0) 
                                = 0 THEN 1 ELSE 0 END AS Suppress_Stale, CASE WHEN LTRIM(RTRIM(ORDERNUMBER)) = 'Beg Bal' THEN 0 WHEN Remaining > 0 AND DUEDATE <= DATEADD(DAY, 7, CAST(GETDATE() AS date)) AND 
                                Inventory_Qty_Available >= Remaining THEN 1 ELSE 0 END AS Suppress_Fence
      FROM            WithInventory wi), Unified AS
    (SELECT        f.*, CASE WHEN Is_BegBal_Row = 1 THEN 0 WHEN Suppress_Stale = 1 THEN 1 WHEN Suppress_Fence = 1 THEN 1 ELSE 0 END AS Is_Suppressed, 
                                CASE WHEN Is_BegBal_Row = 1 THEN 'BEGINNING BALANCE' WHEN Suppress_Stale = 1 THEN 'SUPPRESSED: Stale & Unissued' WHEN Suppress_Fence = 1 THEN 'SUPPRESSED: Full Coverage in Fence' WHEN Inventory_Qty_Available
                                 > 0 AND Inventory_Qty_Available < Remaining THEN 'PARTIAL: Demand Netted' ELSE 'VALID DEMAND' END AS Demand_Status, CASE WHEN Is_BegBal_Row = 1 THEN NULL WHEN (Suppress_Stale = 1 OR
                                Suppress_Fence = 1) THEN 0 ELSE Remaining END AS Remaining_After_Suppression
      FROM            Flags f), Ordered AS
    (SELECT        u.*, ROW_NUMBER() OVER (PARTITION BY u.ITEMNMBR
      ORDER BY CASE WHEN u.Is_BegBal_Row = 1 THEN 0 ELSE 1 END, COALESCE (u.DUEDATE, TRY_CAST(u.[Date + Expiry] AS date), TRY_CAST(u.MRP_IssueDate AS date), CAST('9999-12-31' AS date)), u.ORDERNUMBER, 
                                COALESCE (u.Unified_Value, ''), COALESCE (u.STSDESCR, '')) AS Seq
FROM            Unified u), Deltas AS
    (SELECT        o.*, TRY_CAST(o.Running_Balance AS decimal(18, 6)) AS RB_Num, TRY_CAST(o.Running_Balance AS decimal(18, 6)) - LAG(TRY_CAST(o.Running_Balance AS decimal(18, 6))) OVER (PARTITION BY o.ITEMNMBR
      ORDER BY o.Seq) AS RB_Delta
FROM            Ordered o), Adjusted AS
    (SELECT        d .*, FIRST_VALUE(d .RB_Num) OVER (PARTITION BY d .ITEMNMBR
      ORDER BY d .Seq) AS RB_Anchor, CASE WHEN d .Seq = 1 THEN CAST(0 AS decimal(18, 6)) WHEN d .Is_Suppressed = 1 THEN CAST(0 AS decimal(18, 6)) ELSE ISNULL(d .RB_Delta, 0) END AS RB_Delta_Adjusted
FROM            Deltas d)
    SELECT        ITEMNMBR, ItemDescription, UOM, ORDERNUMBER, Construct, DUEDATE, [Expiry Dates], [Date + Expiry], BEG_BAL, Deductions, Expiry, [PO's], Running_Balance, CAST(RB_Anchor + SUM(RB_Delta_Adjusted) 
                              OVER (PARTITION BY ITEMNMBR
     ORDER BY Seq ROWS UNBOUNDED PRECEDING) AS decimal(18, 6)) AS Adjusted_Running_Balance, MRP_IssueDate, WCID_From_MO, Issued, Remaining, Net_Demand, Inventory_Qty_Available, Is_Suppressed, Demand_Status, 
Remaining_After_Suppression, Has_Issued, IssueDate_Mismatch, Early_Issue_Flag, VendorItem, PRIME_VNDR, PURCHASING_LT, PLANNING_LT, ORDER_POINT_QTY, SAFETY_STOCK, FG, [FG Desc], STSDESCR, MRPTYPE, 
Unified_Value
FROM            Adjusted;
