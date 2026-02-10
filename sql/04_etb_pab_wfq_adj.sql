-- ============================================================================
-- VIEW: ETB_PAB_WFQ_ADJ
-- ============================================================================
-- Role: WFQ pipeline overlay + extended balance
-- Dependencies: ETB_WC_INV_Unified, ETB_WFQ_PIPE
-- Status: PRODUCTION CODE - Deploy this view
-- ============================================================================
-- Calculates WFQ supply aggregation, stockout detection, and extended ledger
-- with WFQ influx. EMBEDDED BegBal: Triple-validated ISNUMERIC logic
-- ============================================================================

CREATE VIEW [dbo].[ETB_PAB_WFQ_ADJ]
AS

WITH Demand_Ledger AS (
    -- Select from View 2 with embedded BegBal validation
    SELECT
        ITEMNMBR,
        ItemDescription,
        UOM,
        ORDERNUMBER,
        Construct,
        DUEDATE,
        [Expiry Dates],
        [Date + Expiry],
        -- EMBEDDED BegBal: Triple-validated ISNUMERIC logic
        CAST(
            CASE 
                WHEN ISNUMERIC(LTRIM(RTRIM(BEG_BAL))) = 1 
                    THEN CAST(LTRIM(RTRIM(BEG_BAL)) AS decimal(18, 6))
                ELSE 0 
            END AS VARCHAR(50)
        ) AS BEG_BAL,
        Deductions,
        Expiry,
        [PO's],
        Running_Balance,
        MRP_IssueDate,
        WCID_From_MO,
        Issued,
        Original_Required,
        Net_Demand,
        Inventory_Qty_Available,
        Suppression_Status,
        VendorItem,
        PRIME_VNDR,
        PURCHASING_LT,
        PLANNING_LT,
        ORDER_POINT_QTY,
        SAFETY_STOCK,
        FG,
        [FG Desc],
        STSDESCR,
        MRPTYPE,
        Unified_Value
    FROM dbo.ETB_WC_INV_Unified
),

Demand_Seq AS (
    -- Assign sequence numbers per item by due date
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ITEMNMBR
            ORDER BY DUEDATE ASC, ORDERNUMBER ASC
        ) AS Demand_Seq
    FROM Demand_Ledger
),

Stockout_Detection AS (
    -- Find the first demand sequence where running balance goes <= 0
    SELECT
        ITEMNMBR,
        MIN(Demand_Seq) AS Stockout_Seq
    FROM Demand_Seq
    WHERE TRY_CAST(Running_Balance AS decimal(18, 6)) <= 0
       OR Running_Balance IS NULL
    GROUP BY ITEMNMBR
),

WFQ_Supply AS (
    -- Aggregate WFQ supply from ETB_WFQ_PIPE
    SELECT
        ITEM_Number AS ITEMNMBR,
        Estimated_Release_Date,
        Expected_Delivery_Date,
        SUM(QTY_ON_HAND) AS WFQ_Qty_Available
    FROM dbo.ETB_WFQ_PIPE
    WHERE View_Level = 'ITEM_LEVEL'
      AND QTY_ON_HAND > 0
    GROUP BY ITEM_Number, Estimated_Release_Date, Expected_Delivery_Date
),

WFQ_Allocated AS (
    -- Allocate WFQ supply to demand rows
    -- WFQ supply counts if Expected_Delivery_Date <= Demand_DUEDATE
    SELECT
        ds.*,
        sd.Stockout_Seq,
        ISNULL(
            (SELECT SUM(w.WFQ_Qty_Available)
             FROM WFQ_Supply w
             WHERE w.ITEMNMBR = ds.ITEMNMBR
               AND w.Expected_Delivery_Date <= ds.DUEDATE
               AND ds.Demand_Seq >= ISNULL(sd.Stockout_Seq, ds.Demand_Seq)
            ), 0
        ) AS WFQ_Influx
    FROM Demand_Seq ds
    LEFT JOIN Stockout_Detection sd
        ON ds.ITEMNMBR = sd.ITEMNMBR
),

Extended_Ledger AS (
    -- Calculate extended balance and status
    SELECT
        *,
        -- Ledger_Extended_Balance = Running_Balance + WFQ_Influx
        TRY_CAST(Running_Balance AS decimal(18, 6)) + WFQ_Influx AS Ledger_Extended_Balance,
        -- WFQ_Extended_Status classification
        CASE
            WHEN WFQ_Influx <= 0 THEN 'LEDGER_ONLY'
            WHEN (TRY_CAST(Running_Balance AS decimal(18, 6)) + WFQ_Influx) > 0
                 AND (TRY_CAST(Running_Balance AS decimal(18, 6)) <= 0)
            THEN 'WFQ_RESCUED'
            WHEN (TRY_CAST(Running_Balance AS decimal(18, 6)) + WFQ_Influx) > 0
            THEN 'WFQ_ENHANCED'
            ELSE 'WFQ_INSUFFICIENT'
        END AS WFQ_Extended_Status
    FROM WFQ_Allocated
)

-- Final output
SELECT
    ITEMNMBR,
    ItemDescription,
    UOM,
    ORDERNUMBER,
    Construct,
    DUEDATE,
    [Expiry Dates],
    [Date + Expiry],
    BEG_BAL,
    Deductions,
    Expiry,
    [PO's],
    Running_Balance,
    MRP_IssueDate,
    WCID_From_MO,
    Issued,
    Original_Required,
    Net_Demand,
    Inventory_Qty_Available,
    Suppression_Status,
    VendorItem,
    PRIME_VNDR,
    PURCHASING_LT,
    PLANNING_LT,
    ORDER_POINT_QTY,
    SAFETY_STOCK,
    FG,
    [FG Desc],
    STSDESCR,
    MRPTYPE,
    Unified_Value,
    -- WFQ extended columns
    WFQ_Influx AS Ledger_WFQ_Influx,
    Ledger_Extended_Balance,
    WFQ_Extended_Status
FROM Extended_Ledger;

GO
