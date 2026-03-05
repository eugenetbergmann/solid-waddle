-- ============================================================================
-- WEIGHTED UNIVERSE — Credibility Weights and Delta Calculations
-- Purpose: Define credibility weights per program/construct for RAW vs WEIGHTED deltas
-- Implemented: March 2026 — KILO loop
-- ============================================================================

CREATE TABLE dbo.ETB_PROGRAM_WEIGHTS
(
    Program_ID              VARCHAR(50)     NOT NULL,
    Program_Name            VARCHAR(255)    NULL,
    Weight                  DECIMAL(3,2)    NOT NULL
        CONSTRAINT CHK_ETB_PROGRAM_WEIGHTS_Weight_Range 
        CHECK (Weight >= 0.00 AND Weight <= 1.00),
    Weight_Basis            VARCHAR(50)     NOT NULL,
    Weight_Reason           VARCHAR(500)    NULL,
    Effective_Date          DATE            NOT NULL DEFAULT (GETDATE()),
    Expiry_Date             DATE            NULL,
    Set_By                  VARCHAR(100)    NOT NULL,
    Last_Updated            DATETIME        NOT NULL DEFAULT (GETDATE()),
    CONSTRAINT PK_ETB_PROGRAM_WEIGHTS PRIMARY KEY (Program_ID, Effective_Date)
);

CREATE INDEX IX_ETB_PROGRAM_WEIGHTS_Current 
ON dbo.ETB_PROGRAM_WEIGHTS(Program_ID) 
WHERE Expiry_Date IS NULL;

CREATE TABLE dbo.ETB_PROGRAM_WEIGHTS_AUDIT
(
    Audit_ID                INT IDENTITY(1,1) PRIMARY KEY,
    Program_ID              VARCHAR(50)     NOT NULL,
    Old_Weight              DECIMAL(3,2)    NULL,
    New_Weight              DECIMAL(3,2)    NOT NULL,
    Old_Basis               VARCHAR(50)     NULL,
    New_Basis               VARCHAR(50)     NOT NULL,
    Change_Reason           VARCHAR(500)    NOT NULL,
    Changed_By              VARCHAR(100)    NOT NULL,
    Changed_At              DATETIME        NOT NULL DEFAULT (GETDATE())
);

SELECT 
    pw.Program_ID,
    pw.Program_Name,
    pw.Weight,
    pw.Weight_Basis,
    pw.Weight_Reason,
    pw.Effective_Date,
    pw.Set_By,
    pw.Last_Updated
FROM dbo.ETB_PROGRAM_WEIGHTS pw
WHERE pw.Expiry_Date IS NULL OR pw.Expiry_Date >= CAST(GETDATE() AS DATE);

WITH Demand_Base AS (
    SELECT 
        s.ITEMNMBR,
        s.ItemDescription,
        s.UOM,
        s.Construct,
        s.Demand_Due_Date,
        s.Net_Demand,
        s.Adjusted_Running_Balance,
        s.Deficit_Qty,
        s.Supply_Action_Recommendation,
        s.Data_Quality_Flag,
        s.Suppression_Status,
        s.WFQ_Extended_Status
    FROM dbo.ETB_PAB_SUPPLY_ACTION s WITH (NOLOCK)
    WHERE s.Demand_Due_Date >= CAST(GETDATE() AS DATE)
      AND ISNULL(s.Net_Demand, 0) > 0
      AND s.Suppression_Status NOT IN (
          'BEGINNING BALANCE',
          'SUPPRESSED: Stale & Unissued',
          'SUPPRESSED: Full Coverage in Fence'
      )
),
Weighted_Calc AS (
    SELECT 
        d.*,
        ISNULL(cpw.Weight, 1.00) AS Program_Weight,
        cpw.Weight_Basis,
        cpw.Weight_Reason,
        d.Net_Demand * ISNULL(cpw.Weight, 1.00) AS Weighted_Net_Demand,
        d.Deficit_Qty * ISNULL(cpw.Weight, 1.00) AS Weighted_Deficit_Qty
    FROM Demand_Base d
    LEFT JOIN dbo.ETB_CURRENT_PROGRAM_WEIGHTS cpw 
        ON TRY_CAST(d.Construct AS VARCHAR(50)) = cpw.Program_ID
)
SELECT 
    ITEMNMBR,
    ItemDescription,
    UOM,
    Construct,
    Demand_Due_Date,
    Net_Demand                          AS RAW_Net_Demand,
    Deficit_Qty                         AS RAW_Deficit_Qty,
    Weighted_Net_Demand,
    Weighted_Deficit_Qty,
    Net_Demand - Weighted_Net_Demand    AS Delta_RAW_Weighted_Net,
    Deficit_Qty - Weighted_Deficit_Qty  AS Delta_RAW_Weighted_Deficit,
    CASE WHEN Net_Demand > 0 
         THEN (Net_Demand - Weighted_Net_Demand) / Net_Demand 
         ELSE 0 
    END                                 AS Weight_Reduction_Pct,
    Program_Weight,
    Weight_Basis,
    Weight_Reason,
    Adjusted_Running_Balance,
    Supply_Action_Recommendation,
    Data_Quality_Flag,
    Suppression_Status,
    WFQ_Extended_Status,
    'RAW_VS_WEIGHTED'                   AS Analysis_Type,
    CAST(GETDATE() AS date)             AS Analysis_Date
FROM Weighted_Calc;

SELECT 
    ITEMNMBR,
    ItemDescription,
    UOM,
    MIN(Demand_Due_Date)                AS First_Demand_Date,
    MAX(Demand_Due_Date)                AS Last_Demand_Date,
    SUM(RAW_Net_Demand)                 AS Total_RAW_Net_Demand,
    SUM(RAW_Deficit_Qty)                AS Total_RAW_Deficit_Qty,
    SUM(Weighted_Net_Demand)            AS Total_Weighted_Net_Demand,
    SUM(Weighted_Deficit_Qty)           AS Total_Weighted_Deficit_Qty,
    SUM(Delta_RAW_Weighted_Net)         AS Total_Delta_Net,
    SUM(Delta_RAW_Weighted_Deficit)     AS Total_Delta_Deficit,
    AVG(Program_Weight)                 AS Avg_Program_Weight,
    MIN(Program_Weight)                 AS Min_Program_Weight,
    MAX(Program_Weight)                 AS Max_Program_Weight,
    COUNT(DISTINCT Construct)           AS Program_Count,
    SUM(CASE WHEN Supply_Action_Recommendation = 'ORDER' THEN 1 ELSE 0 END) AS Count_ORDER,
    SUM(CASE WHEN Supply_Action_Recommendation = 'BOTH' THEN 1 ELSE 0 END) AS Count_BOTH,
    SUM(CASE WHEN Supply_Action_Recommendation = 'ORDER' THEN Program_Weight ELSE 0 END) AS Weighted_COUNT_ORDER,
    SUM(CASE WHEN Supply_Action_Recommendation = 'BOTH' THEN Program_Weight ELSE 0 END) AS Weighted_COUNT_BOTH,
    'WEIGHTED_AGGREGATE'                AS Summary_Type,
    CAST(GETDATE() AS date)             AS Analysis_Date
FROM dbo.ETB_WEIGHTED_DEMAND
GROUP BY ITEMNMBR, ItemDescription, UOM;
