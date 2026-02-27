-- ============================================================================
-- VIEW: ETB_RUN_RISK
-- Purpose: Executive risk aggregation — stockout timing, client exposure, schedule threats
-- Author: Zo Computer
-- Date: 2026-02-26
-- Dependencies: dbo.ETB_PAB_SUPPLY_ACTION (View 5), dbo.ETB_SS_CALC
-- ============================================================================
/*
================================================================================
ETB_RUN_RISK — Executive Risk Dashboard (View 6)
================================================================================
Purpose:
  Compresses thousands of demand rows into a single risk signal per item/vendor
  combination.  Identifies threatened clients, calculates stockout timing, and
  flags schedule threats where stockout occurs before a PO could arrive based on
  vendor lead time.

  This is the primary executive-visibility surface — answers WHERE/WHEN/WHO/HOW.

Source / Dependency:
  dbo.ETB_PAB_SUPPLY_ACTION — View 5 (demand surface with deficit and suppression)
  dbo.ETB_SS_CALC           — Safety stock reference (lead days, vendor fallback)

CTE Pipeline:
  Config                  → Named threshold constants (Issue 8)
  threatened_clients_detail → Distinct item/vendor/client combinations with deficit
  client_summary          → Rollup: threatened client list and count per item/vendor
  deficit_rows            → Windowed aggregation: first stockout date, total deficit
  with_threat             → Join ETB_SS_CALC for lead days; compute Schedule_Threat

Key Outputs:
  ITEMNMBR, PRIME_VNDR, ItemDescription, UOM
  Threatened_Clients      — Comma-separated list of impacted customers
  Client_Exposure_Count   — Number of distinct impacted customers
  First_Stockout_Date     — Earliest projected stockout date
  Days_To_Stockout        — Calendar days until stockout
  Total_Deficit_Qty       — Total units short across all demand
  WFQ_Dependency_Flag     — 1 if item relies on quarantine inventory
  Schedule_Threat         — 1 if stockout occurs before PO lead time
  LeadDays                — Vendor lead time (default from Config)

Change Log:
  2026-02-13: Initial production deployment.
  2026-02-26: Added ItemDescription and UOM columns; COALESCE vendor from
              ETB_SS_CALC fallback.
  2026-02-27: Added documentation header (Issue 7), Config CTE for named
              thresholds (Issue 8), UNASSIGNED fallback guard on PRIME_VNDR
              COALESCE (Issue 4).
================================================================================
*/

WITH Config AS (
    SELECT
        -- Default vendor lead time when ETB_SS_CALC has no matching record
        -- Business rule: 30 days is the standard fallback lead time
        30 AS Default_Lead_Days,

        -- Schedule threat threshold multiplier (1x lead days)
        -- A stockout is a "schedule threat" if it occurs within 1x lead time
        1 AS Schedule_Threat_Multiplier
),

threatened_clients_detail AS
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
                  -- Issue 4: COALESCE ensures vendor is never NULL; UNASSIGNED is the final fallback
                  COALESCE(d.PRIME_VNDR, ss.PRIME_VNDR, 'UNASSIGNED') AS PRIME_VNDR, 
                  d.ItemDescription, d.UOM,
                  d.Threatened_Clients, d.Client_Exposure_Count, d.First_Stockout_Date, d.Total_Deficit_Qty, d.WFQ_Dependency_Flag, 
                  -- Issue 8: Default_Lead_Days from Config (replaces hardcoded 30)
                  ISNULL(ss.LeadDays, (SELECT Default_Lead_Days FROM Config)) AS LeadDays, 
                  DATEDIFF(DAY, CAST(GETDATE() AS DATE), d.First_Stockout_Date) AS Days_To_Stockout, 
                  -- Issue 8: Default_Lead_Days from Config for schedule threat window
                  CASE WHEN d.First_Stockout_Date IS NOT NULL 
                            AND d.First_Stockout_Date <= DATEADD(DAY, 
                                    ISNULL(ss.LeadDays, (SELECT Default_Lead_Days FROM Config)), 
                                    CAST(GETDATE() AS DATE)) 
                       THEN 1 ELSE 0 END AS Schedule_Threat
    FROM            deficit_rows d 
    LEFT JOIN dbo.ETB_SS_CALC ss ON d.ITEMNMBR = ss.ITEMNMBR AND d.PRIME_VNDR = ss.PRIME_VNDR
)
SELECT DISTINCT ITEMNMBR, PRIME_VNDR, ItemDescription, UOM, Threatened_Clients, Client_Exposure_Count, First_Stockout_Date, Days_To_Stockout, Total_Deficit_Qty, WFQ_Dependency_Flag, Schedule_Threat, LeadDays
FROM            with_threat;
