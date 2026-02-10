/* Carry forward EVERYTHING exactly as ETB_WC_INV_Unified already computed it
       (which itself carries the PAB_AUTO BEG BAL + deduction/PO/expiry impacts into Running_Balance),
       plus the suppression fields + adjusted RB. */ SELECT
                          ITEMNMBR, ItemDescription, UOM, ORDERNUMBER, Construct, DUEDATE, [Expiry Dates], [Date + Expiry], BEG_BAL, Deductions, Expiry, [PO's], Running_Balance, Adjusted_Running_Balance, MRP_IssueDate, 
                         WCID_From_MO, Issued, Remaining AS Original_Required, Net_Demand, Inventory_Qty_Available, Demand_Status AS Suppression_Status, Is_Suppressed, Has_Issued, IssueDate_Mismatch, Early_Issue_Flag, VendorItem, 
                         PRIME_VNDR, PURCHASING_LT, PLANNING_LT, ORDER_POINT_QTY, SAFETY_STOCK, FG, [FG Desc], STSDESCR, MRPTYPE, Unified_Value
FROM            dbo.ETB_WC_INV_Unified), /* Keep BEG BAL + ALL non-demand ledger rows intact.
   Only build a demand sequence for rows that actually consume demand (so stockout detection isn't distorted). */ DemandRowsOnly AS
    (SELECT        *
      FROM            Demand_Ledger
      WHERE        LTRIM(RTRIM(ORDERNUMBER)) <> 'Beg Bal' AND DUEDATE IS NOT NULL /* demand-like rows: keep these broad unless you have a clean MRPTYPE/STSDESCR filter */ AND ISNULL(Original_Required, 0) <> 0), 
Demand_Seq AS
    (SELECT        d .*, ROW_NUMBER() OVER (PARTITION BY d .ITEMNMBR
      ORDER BY d .DUEDATE, d .ORDERNUMBER, COALESCE (d .Unified_Value, '')) AS Demand_Seq
FROM            DemandRowsOnly d), Stockout_Detection AS
    (/* Threshold uses the adjusted ledger so suppression is respected */ SELECT ds.*, MIN(CASE WHEN ds.Adjusted_Running_Balance <= 0 THEN ds.Demand_Seq END) OVER (PARTITION BY ds.ITEMNMBR) AS Stockout_Seq
      FROM            Demand_Seq ds), WFQ_Supply AS
    (/* WFQ supply sources */ SELECT ITEM_Number AS ITEMNMBR, SITE, SUM(QTY_ON_HAND) AS WFQ_Qty, Estimated_Release_Date
      FROM            dbo.ETB_WFQ_PIPE
      WHERE        View_Level = 'ITEM_LEVEL' AND QTY_ON_HAND > 0
      GROUP BY ITEM_Number, SITE, Estimated_Release_Date), WFQ_Allocated AS
    (/* Allocate all WFQ qty that releases by each demand due date, only after stockout begins */ SELECT d .ITEMNMBR, d .Demand_Seq, d .DUEDATE, d .Adjusted_Running_Balance AS Ledger_Base_Balance, d .Stockout_Seq, 
                                SUM(CASE WHEN d .Stockout_Seq IS NOT NULL AND d .Demand_Seq >= d .Stockout_Seq AND w.Estimated_Release_Date <= d .DUEDATE THEN w.WFQ_Qty ELSE 0 END) AS Ledger_WFQ_Influx
      FROM            Stockout_Detection d LEFT JOIN
                                WFQ_Supply w ON d .ITEMNMBR = w.ITEMNMBR
      GROUP BY d .ITEMNMBR, d .Demand_Seq, d .DUEDATE, d .Adjusted_Running_Balance, d .Stockout_Seq), Extended_Demand AS
    (/* Add WFQ overlay columns onto demand rows */ SELECT d .*, ISNULL(w.Ledger_WFQ_Influx, 0) AS Ledger_WFQ_Influx, d .Adjusted_Running_Balance + ISNULL(w.Ledger_WFQ_Influx, 0) AS Ledger_Extended_Balance, 
                                CASE WHEN ISNULL(w.Ledger_WFQ_Influx, 0) <= 0 THEN 'LEDGER_ONLY' WHEN d .Adjusted_Running_Balance <= 0 AND (d .Adjusted_Running_Balance + ISNULL(w.Ledger_WFQ_Influx, 0)) 
                                > 0 THEN 'WFQ_RESCUED' WHEN (d .Adjusted_Running_Balance + ISNULL(w.Ledger_WFQ_Influx, 0)) > 0 THEN 'WFQ_ENHANCED' ELSE 'WFQ_INSUFFICIENT' END AS WFQ_Extended_Status
      FROM            Stockout_Detection d LEFT JOIN
                                WFQ_Allocated w ON d .ITEMNMBR = w.ITEMNMBR AND d .Demand_Seq = w.Demand_Seq), 
/* Re-attach the WFQ overlay onto the FULL ledger so BEG_BAL + PO + expiry + all PAB deduction logic carries forward unchanged */ Final_Ledger AS
    (SELECT        l.*, /* overlay columns populated only for sequenced demand rows; else 0/NULL */ ISNULL(ed.Ledger_WFQ_Influx, 0) AS Ledger_WFQ_Influx, CASE WHEN ed.ITEMNMBR IS NULL THEN NULL 
                                ELSE ed.Ledger_Extended_Balance END AS Ledger_Extended_Balance, CASE WHEN LTRIM(RTRIM(l.ORDERNUMBER)) = 'Beg Bal' THEN 'BEGINNING BALANCE' WHEN ed.ITEMNMBR IS NULL 
                                THEN 'NON_DEMAND_LEDGER_ROW' ELSE ed.WFQ_Extended_Status END AS WFQ_Extended_Status
      FROM            Demand_Ledger l LEFT JOIN
                                Extended_Demand ed ON l.ITEMNMBR = ed.ITEMNMBR AND l.DUEDATE = ed.DUEDATE AND l.ORDERNUMBER = ed.ORDERNUMBER AND COALESCE (l.Unified_Value, '') = COALESCE (ed.Unified_Value, ''))
    SELECT        ITEMNMBR, ItemDescription, UOM, ORDERNUMBER, Construct, DUEDATE, [Expiry Dates], [Date + Expiry], BEG_BAL, Deductions, Expiry, [PO's], Running_Balance, Adjusted_Running_Balance, MRP_IssueDate, 
                              WCID_From_MO, Issued, Original_Required, Net_Demand, Inventory_Qty_Available, Suppression_Status, VendorItem, PRIME_VNDR, PURCHASING_LT, PLANNING_LT, ORDER_POINT_QTY, SAFETY_STOCK, FG, [FG Desc], 
                              STSDESCR, MRPTYPE, Unified_Value, /* WFQ overlay columns */ Ledger_WFQ_Influx, Ledger_Extended_Balance, WFQ_Extended_Status
     FROM            Final_Ledger;
