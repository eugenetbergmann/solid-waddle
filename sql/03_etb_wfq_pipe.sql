/*
================================================================================
ETB_WFQ_PIPE — WFQ (Work-For-Queue) Supply Pipeline (View 3)
================================================================================
Purpose:
  Provides a current snapshot of WFQ/UNDERINV lot inventory that is eligible
  for release into the production pipeline.  Each row represents one lot at the
  ITEM_LEVEL with its estimated release date (calculated from receipt date + a
  series-specific SOP target window).

  This view is consumed by View 4 (ETB_PAB_WFQ_ADJ) to offset demand deficits
  with upcoming WFQ supply.

Source Tables:
  dbo.IV00300  — Inventory lot detail (qty received, qty sold, dates)
  dbo.IV00101  — Item master (UOM schedule)

Business Logic:
  • Locations: WF-Q (Work-For-Queue staging) and UNDERINV (under-inventory hold)
  • Age filter: lots received within 65 days are considered active supply
  • Series 10 items have a 21-day SOP window; all others use 14 days
  • Estimated_Release_Date = DATERECD + SOP_Target_Days
  • Valid_Expiration: 1 if lot expiry > 90 days from today (usable stock)
  • Lots with net quantity = 0 (QTYRECVD - QTYSOLD = 0) are excluded

Named Thresholds (Issue 8):
  SOP_Target_Days_Series10  = 21  days (series '10' items)
  SOP_Target_Days_Default   = 14  days (all other series)
  Lot_Age_Cutoff_Days       = 65  days (max age of active WFQ lot)
  Expiry_Valid_Window_Days  = 90  days (min days to expiry for Valid_Expiration=1)

Change Log:
  2026-02-26: Added header documentation, named thresholds (Issue 8), NOLOCK
              hint for consistency (Issue 9), series-based SOP logic documented
              (Issue 10).
  2026-02-27: Added Config CTE so all thresholds are sourced from named constants
              rather than inline magic numbers (Issue 8 complete for this file).
================================================================================
*/

WITH Config AS (
    SELECT
        21 AS SOP_Target_Days_Series_10,    -- Target days for series 10 items (API/actives)
        14 AS SOP_Target_Days_Other,         -- Target days for all other series
        65 AS Lot_Age_Limit_Days,            -- Max lot age (days) to be considered active supply
        90 AS Expiration_Buffer_Days         -- Min days to expiry for Valid_Expiration = 1
)

SELECT
    -- View-level identifier — downstream CTEs filter on this value
    'ITEM_LEVEL'                                                AS View_Level,

    TRIM(dbo.IV00300.ITEMNMBR)                                  AS Item_Number,
    TRIM(dbo.IV00300.LOCNCODE)                                  AS SITE,
    TRIM(dbo.IV00300.LOTNUMBR)                                  AS LOT_Number,
    dbo.IV00300.EXPNDATE,
    TRIM(dbo.IV00101.UOMSCHDL)                                  AS UOM,

    -- Net on-hand quantity for this lot
    SUM(dbo.IV00300.QTYRECVD - dbo.IV00300.QTYSOLD)             AS QTY_ON_HAND,

    -- Series code (first 2 characters of item number)
    LEFT(TRIM(dbo.IV00300.ITEMNMBR), 2)                         AS Series,

    -- Issue 8: SOP target window sourced from Config CTE.
    -- Business rule: series 10 items (API/active ingredients) require a longer
    -- in-process testing window before they can be released to production.
    CASE WHEN LEFT(TRIM(dbo.IV00300.ITEMNMBR), 2) = '10'
         THEN (SELECT SOP_Target_Days_Series_10 FROM Config)
         ELSE (SELECT SOP_Target_Days_Other     FROM Config)
    END                                                         AS SOP_Target_Days,

    -- Age of this lot in days (from receipt to today)
    DATEDIFF(DAY, dbo.IV00300.DATERECD, GETDATE())              AS Lot_Age_Days,

    -- Estimated release date = receipt date + series-specific SOP window
    DATEADD(DAY,
        CASE WHEN LEFT(TRIM(dbo.IV00300.ITEMNMBR), 2) = '10'
             THEN (SELECT SOP_Target_Days_Series_10 FROM Config)
             ELSE (SELECT SOP_Target_Days_Other     FROM Config)
        END,
        dbo.IV00300.DATERECD
    )                                                           AS Estimated_Release_Date,

    -- 1 = lot expiry is far enough out to be usable supply
    -- Issue 8: Expiration_Buffer_Days from Config CTE
    CASE WHEN dbo.IV00300.EXPNDATE > DATEADD(DAY,
                                         (SELECT Expiration_Buffer_Days FROM Config),
                                         GETDATE())
         THEN 1
         ELSE 0
    END                                                         AS Valid_Expiration

FROM    dbo.IV00300
-- Issue 9: NOLOCK for consistency with other views in this pipeline
LEFT OUTER JOIN dbo.IV00101 WITH (NOLOCK) ON dbo.IV00300.ITEMNMBR = dbo.IV00101.ITEMNMBR
CROSS JOIN Config

WHERE
    -- Exclude lots that have been fully consumed
    (dbo.IV00300.QTYRECVD - dbo.IV00300.QTYSOLD <> 0)

    -- WFQ-eligible locations only
    AND TRIM(dbo.IV00300.LOCNCODE) IN ('WF-Q', 'UNDERINV')

    -- Issue 8: Lot_Age_Limit_Days from Config; lots older than this are no longer
    -- considered active supply (likely already consumed or condemned)
    AND DATEDIFF(DAY, dbo.IV00300.DATERECD, GETDATE()) <= (SELECT Lot_Age_Limit_Days FROM Config)

GROUP BY
    TRIM(dbo.IV00300.ITEMNMBR),
    TRIM(dbo.IV00300.LOCNCODE),
    TRIM(dbo.IV00300.LOTNUMBR),
    dbo.IV00300.EXPNDATE,
    TRIM(dbo.IV00101.UOMSCHDL),
    dbo.IV00300.DATERECD;
