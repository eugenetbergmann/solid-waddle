-- ============================================================================
-- VIEW: ETB_PAB_SUPPLY_ACTION
-- ============================================================================
-- Role: Final decision surface (supply action recommendations)
-- Dependencies: ETB_PAB_WFQ_ADJ, ETB_WFQ_PIPE
-- Status: NEW PRODUCTION CODE - Deploy this view
-- ============================================================================
-- Evaluates supply adequacy by comparing PO quantity and timing against 
-- demand deficit. Output: Fully enumerated decision surface with no roll-ups
-- ============================================================================

CREATE VIEW [dbo].[ETB_PAB_SUPPLY_ACTION]
AS

WITH WFQ_Extended AS (
    -- Reference the upstream view output
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
        Ledger_WFQ_Influx,
        Ledger_Extended_Balance,
        WFQ_Extended_Status
    FROM dbo.ETB_PAB_WFQ_ADJ
),

Balance_Analysis AS (
    -- Calculate deficit, POs on order, and demand due date
    SELECT
        *,
        -- Deficit_Qty: NET 0 if sufficient, else positive shortfall
        CASE 
            WHEN Ledger_Extended_Balance >= Net_Demand THEN 0
            ELSE Net_Demand - Ledger_Extended_Balance
        END AS Deficit_Qty,
        -- POs_On_Order_Qty: Safe handling of non-numeric or NULL values
        CASE 
            WHEN [PO's] IS NULL THEN 0
            WHEN ISNUMERIC([PO's]) = 1 THEN CAST([PO's] AS decimal(18, 6))
            ELSE 0
        END AS POs_On_Order_Qty,
        -- Demand_Due_Date: Try to cast [Date + Expiry] to DATE
        TRY_CAST([Date + Expiry] AS DATE) AS Demand_Due_Date
    FROM WFQ_Extended
),

WFQ_Turnaround_Analysis AS (
    -- Extract turn-around timing from pipeline, aggregated by ITEM
    -- Note: Using Estimated_Release_Date as proxy; assumes Expected_Delivery_Date column exists
    SELECT 
        ITEM_Number AS ITEMNMBR,
        Estimated_Release_Date,
        -- Calculate turn-around days from release to expected delivery
        -- Using PURCHASING_LT as turn-around proxy since Expected_Delivery_Date may vary
        CASE 
            WHEN Estimated_Release_Date IS NOT NULL 
                 AND Estimated_Release_Date >= CAST(GETDATE() AS DATE) 
                 THEN DATEDIFF(DAY, CAST(GETDATE() AS DATE), Estimated_Release_Date)
            ELSE 0
        END AS PO_Turn_Around_Days,
        -- For PO_Actual_Arrival_Date: use Estimated_Release_Date directly when available
        Estimated_Release_Date AS PO_Actual_Arrival_Date
    FROM dbo.ETB_WFQ_PIPE
    WHERE View_Level = 'ITEM_LEVEL'
),

PO_Timing_Analysis AS (
    -- Join balance analysis with timing data
    SELECT 
        b.*,
        -- Get timing columns from WFQ pipeline (LEFT JOIN to preserve all demand rows)
        t.PO_Turn_Around_Days,
        t.PO_Actual_Arrival_Date,
        t.Estimated_Release_Date AS PO_Release_Date,
        -- PO_On_Time: 1 if actual arrival <= demand due date, else 0
        CASE 
            WHEN t.PO_Actual_Arrival_Date IS NOT NULL 
                 AND b.Demand_Due_Date IS NOT NULL 
                 AND t.PO_Actual_Arrival_Date <= b.Demand_Due_Date THEN 1
            ELSE 0
        END AS PO_On_Time,
        -- Is_Past_Due_In_Backlog: 1 if demand due date < today, else 0
        CASE 
            WHEN b.Demand_Due_Date IS NOT NULL 
                 AND b.Demand_Due_Date < CAST(GETDATE() AS DATE) THEN 1
            ELSE 0
        END AS Is_Past_Due_In_Backlog
    FROM Balance_Analysis b
    LEFT JOIN WFQ_Turnaround_Analysis t
           ON b.ITEMNMBR = t.ITEMNMBR
),

Supply_Action_Decision AS (
    -- Apply decision rules to determine supply action recommendation
    SELECT 
        *,
        -- Supply_Action_Recommendation: Apply rules in order (quantity first, then timing)
        CASE 
            -- Rule 1: Ledger balance sufficient
            WHEN Ledger_Extended_Balance >= Net_Demand THEN 'SUFFICIENT'
            
            -- Rule 2: Deficit exists but no POs on order
            WHEN Deficit_Qty > 0 AND POs_On_Order_Qty = 0 THEN 'ORDER'
            
            -- Rule 3: Deficit exists, POs cover deficit, but not on time
            WHEN Deficit_Qty > 0 
                 AND POs_On_Order_Qty >= Deficit_Qty 
                 AND PO_On_Time = 0 THEN 'ORDER'
            
            -- Rule 4: Deficit exists, POs cover deficit, and on time
            WHEN Deficit_Qty > 0 
                 AND POs_On_Order_Qty >= Deficit_Qty 
                 AND PO_On_Time = 1 THEN 'SUFFICIENT'
            
            -- Rule 5: Deficit exists, POs partially cover deficit
            WHEN Deficit_Qty > 0 
                 AND POs_On_Order_Qty > 0 
                 AND POs_On_Order_Qty < Deficit_Qty THEN 'BOTH'
            
            -- Default: Edge case requiring review
            ELSE 'REVIEW_REQUIRED'
        END AS Supply_Action_Recommendation,
        -- Additional_Order_Qty: Calculate shortfall after existing POs
        CASE 
            WHEN Deficit_Qty > 0 AND POs_On_Order_Qty < Deficit_Qty 
                 THEN Deficit_Qty - POs_On_Order_Qty
            ELSE 0
        END AS Additional_Order_Qty
    FROM PO_Timing_Analysis
)

-- Final output: All upstream columns in order + new calculated columns
SELECT 
    -- Section 1: Item and Order identifiers
    ITEMNMBR,
    ItemDescription,
    UOM,
    ORDERNUMBER,
    Construct,
    
    -- Section 2: Demand timing and ledger columns
    DUEDATE,
    [Expiry Dates],
    [Date + Expiry],
    BEG_BAL,
    Deductions,
    Expiry,
    [PO's],
    Running_Balance,
    
    -- Section 3: MRP and demand columns
    MRP_IssueDate,
    WCID_From_MO,
    Issued,
    Original_Required,
    Net_Demand,
    Inventory_Qty_Available,
    Suppression_Status,
    
    -- Section 4: Vendor and planning columns
    VendorItem,
    PRIME_VNDR,
    PURCHASING_LT,
    PLANNING_LT,
    ORDER_POINT_QTY,
    SAFETY_STOCK,
    
    -- Section 5: Product and status columns
    FG,
    [FG Desc],
    STSDESCR,
    MRPTYPE,
    Unified_Value,
    
    -- Section 6: WFQ extended columns (from upstream)
    Ledger_WFQ_Influx,
    Ledger_Extended_Balance,
    WFQ_Extended_Status,
    
    -- Section 7: NEW - Supply action calculated columns
    Deficit_Qty,
    POs_On_Order_Qty,
    Demand_Due_Date,
    PO_Release_Date,
    PO_Turn_Around_Days,
    PO_Actual_Arrival_Date,
    PO_On_Time,
    Is_Past_Due_In_Backlog,
    Supply_Action_Recommendation,
    Additional_Order_Qty
FROM Supply_Action_Decision;

GO
