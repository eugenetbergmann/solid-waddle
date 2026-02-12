# ETB PAB Supply Chain — SQL View Pipeline

A complete, production-ready SQL Server view pipeline that transforms raw manufacturing order and inventory data into actionable supply chain decisions. The system progresses from a foundational PAB (Projected Available Balance) ledger through suppression, WFQ overlay, and supply action logic, culminating in executive risk dashboards and buyer action queues.

---

## Repository Structure

```
sql/
  01_etb_pab_auto.sql              # View 1 — PAB ledger foundation
  02_etb_wc_inv_unified.sql        # View 2 — WC suppression + Adjusted_Running_Balance
  03_etb_wfq_pipe.sql              # View 3 — WFQ pipeline source data
  04_etb_pab_wfq_adj.sql           # View 4 — WFQ overlay + extended balance
  05_etb_pab_supply_action.sql     # View 5 — Supply action decision surface
  06_etb_run_risk.sql              # View 6 — Executive risk aggregation
  07_etb_buyer_control.sql         # View 7 — Buyer PO consolidation engine
  ETB_SS_CALC                      # Reference — Safety stock calculation query

docs/
  ARCHITECTURE.md                  # View hierarchy & dependency diagram
  CONTROL_LAYER.md                 # Views 6 & 7 executive summary
  DEPLOYMENT.md                    # Installation instructions

plans/
  view-6-7-description-uom-vendor.md  # Implementation plan for recent changes
```

---

## Data Flow

```
                    ┌─────────────────────────────┐
                    │  Raw Source Tables           │
                    │  ETB_PAB_MO                  │
                    │  ETB_ActiveDemand_Union_FG_MO│
                    │  Prosenthal_Vendor_Items      │
                    │  PK010033, WO010032, IV00101  │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │  View 1: ETB_PAB_AUTO        │
                    │  PAB Ledger Foundation        │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │  View 2: ETB_WC_INV_Unified  │
                    │  Suppression + Adjusted RB    │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                              ▼
     ┌──────────────────────┐       ┌──────────────────────┐
     │  View 3: ETB_WFQ_PIPE│       │  View 4: ETB_PAB_    │
     │  WFQ Pipeline Source  │       │  WFQ_ADJ             │
     └──────────────────────┘       │  WFQ Overlay          │
                                    └──────────┬───────────┘
                                               │
                                               ▼
                                    ┌──────────────────────┐
                                    │  View 5: ETB_PAB_    │
                                    │  SUPPLY_ACTION        │
                                    │  Decision Surface     │
                                    └──────────┬───────────┘
                                               │
                              ┌────────────────┴────────────────┐
                              ▼                                 ▼
                   ┌──────────────────────┐          ┌──────────────────────┐
                   │  View 6: ETB_RUN_RISK│          │  View 7: ETB_BUYER_  │
                   │  Risk Aggregation     │          │  CONTROL             │
                   │  + ETB_SS_CALC        │          │  PO Consolidation    │
                   └──────────────────────┘          │  + ETB_SS_CALC        │
                                                     └──────────────────────┘
```

---

## Object Catalog

| # | Object | SQL File | Role | Dependencies | Status |
|---|--------|----------|------|--------------|--------|
| 1 | `ETB_PAB_AUTO` | `01_etb_pab_auto.sql` | PAB ledger: Beg Bal seed + demand/expiry/PO impacts → `Running_Balance` | `ETB_PAB_MO`, `ETB_ActiveDemand_Union_FG_MO`, `Prosenthal_Vendor_Items`, `PK010033`, `WO010032`, `IV00101` | Reference |
| 2 | `ETB_WC_INV_Unified` | `02_etb_wc_inv_unified.sql` | Pattern A suppression + inventory netting → `Adjusted_Running_Balance` | View 1 + `Prosenthal_INV_BIN_QTY_wQTYTYPE` | Reference |
| 3 | `ETB_WFQ_PIPE` | `03_etb_wfq_pipe.sql` | WFQ pipeline source: lot-level quarantine inventory with release estimates | `IV00300`, `IV00101` | Reference |
| 4 | `ETB_PAB_WFQ_ADJ` | `04_etb_pab_wfq_adj.sql` | WFQ overlay during stockouts → `Ledger_Extended_Balance`, `WFQ_Extended_Status` | View 2 + View 3 | Reference |
| 5 | `ETB_PAB_SUPPLY_ACTION` | `05_etb_pab_supply_action.sql` | Final decision surface: deficit analysis, PO timing, supply action recommendations | View 4 + View 3 | **Production** |
| 6 | `ETB_RUN_RISK` | `06_etb_run_risk.sql` | Executive risk dashboard: stockout timing, client exposure, schedule threats | View 5 + `ETB_SS_CALC` | **Production** |
| 7 | `ETB_BUYER_CONTROL` | `07_etb_buyer_control.sql` | Buyer action queue: PO consolidation, urgency classification, vendor exposure | View 5 + `ETB_SS_CALC` | **Production** |
| — | `ETB_SS_CALC` | `ETB_SS_CALC` | Safety stock calculation: lead times, demand statistics, SS quantities | `ETB_SS`, `ReceivingsLineItems`, `POP30330`, `PHR_MO_CostCalc1` | Reference |

---

## View Details

### View 1 — `ETB_PAB_AUTO` (PAB Ledger Foundation)

Builds the authoritative item-level demand ledger from manufacturing orders. Joins MO data with active demand and vendor item descriptions, deduplicates via ranked windows, and enriches with MRP issue tracking from the GP picklist table (`PK010033`).

**Key outputs**: `ITEMNMBR`, `ItemDescription`, `UOM`, `Running_Balance`, `BEG_BAL`, `Deductions`, `Expiry`, `PO's`, `PRIME_VNDR`, `Unified_Value`

**Invariant**: This is the single source of truth for baseline ledger math. The "Beg Bal" row seeds all downstream balance calculations.

---

### View 2 — `ETB_WC_INV_Unified` (WC Suppression + Adjusted Balance)

Applies Pattern A suppression to prevent double-counted demand without deleting ledger rows. Nets WC warehouse inventory against demand within a 45-day receipt window and 7-day fence.

**Suppression rules**:
- **Stale & Unissued**: Due > 7 days ago with zero issued quantity
- **Full Coverage in Fence**: Due within 7 days and WC inventory covers full requirement
- **Partial Netting**: WC inventory partially covers demand → `Net_Demand` reduced

**Key outputs**: `Adjusted_Running_Balance` (suppression-aware), `Is_Suppressed`, `Demand_Status`, `Remaining_After_Suppression`

**Critical rule**: The "Beg Bal" row is never suppressed or removed.

---

### View 3 — `ETB_WFQ_PIPE` (WFQ Pipeline Source)

Queries lot-level quarantine inventory from `WF-Q` and `UNDERINV` sites. Calculates lot age, estimated release dates (21 days for series 10, 14 days otherwise), and expiration validity.

**Key outputs**: `Item_Number`, `SITE`, `QTY_ON_HAND`, `Estimated_Release_Date`, `Lot_Age_Days`, `Valid_Expiration`

**Filter**: Only lots received within 65 days with non-zero on-hand quantity.

---

### View 4 — `ETB_PAB_WFQ_ADJ` (WFQ Overlay + Extended Ledger)

Detects stockout points in the suppression-aware ledger and overlays WFQ supply to determine if quarantine inventory can rescue or enhance projected balances.

**Stockout detection**: First demand row where `Adjusted_Running_Balance <= 0`

**WFQ allocation**: Cumulative WFQ quantity where `Estimated_Release_Date <= DUEDATE`, applied only at/after stockout.

**Key outputs**: `Ledger_WFQ_Influx`, `Ledger_Extended_Balance`, `WFQ_Extended_Status` (LEDGER_ONLY / WFQ_RESCUED / WFQ_ENHANCED / WFQ_INSUFFICIENT)

---

### View 5 — `ETB_PAB_SUPPLY_ACTION` (Decision Surface)

The final operational ledger combining all upstream logic. Calculates deficits, parses PO quantities, evaluates PO timing against WFQ release dates, and produces supply action recommendations.

**Decision logic**:

| Rule | Condition | Recommendation |
|------|-----------|----------------|
| 1 | `Ledger_Extended_Balance >= Net_Demand` | SUFFICIENT |
| 2 | `Deficit_Qty > 0` AND `POs_On_Order_Qty = 0` | ORDER |
| 3 | `Deficit_Qty > 0` AND POs cover deficit but late | ORDER |
| 4 | `Deficit_Qty > 0` AND POs cover deficit and on time | SUFFICIENT |
| 5 | `Deficit_Qty > 0` AND POs partially cover deficit | BOTH |
| Default | Edge case | REVIEW_REQUIRED |

**Key outputs**: `Supply_Action_Recommendation`, `Additional_Order_Qty`, `Deficit_Qty`, `PO_On_Time`, `Is_Past_Due_In_Backlog`, `Demand_Due_Date`

---

### View 6 — `ETB_RUN_RISK` (Executive Risk Dashboard)

Compresses thousands of demand rows into a single risk signal per item/vendor combination. Identifies threatened clients, calculates stockout timing, and flags schedule threats where stockout occurs before a PO could arrive based on lead time.

**Vendor resolution**: `COALESCE(ETB_PAB_SUPPLY_ACTION.PRIME_VNDR, ETB_SS_CALC.PRIME_VNDR)` ensures vendor is always populated when either source has a value.

**Key outputs**:

| Column | Description |
|--------|-------------|
| `ITEMNMBR` | Item number |
| `PRIME_VNDR` | Resolved vendor (COALESCE from supply action + safety stock) |
| `ItemDescription` | Item description from vendor master |
| `UOM` | Unit of measure |
| `Threatened_Clients` | Comma-separated list of impacted customers |
| `Client_Exposure_Count` | Number of distinct customers impacted |
| `First_Stockout_Date` | Earliest projected stockout date |
| `Days_To_Stockout` | Calendar days until stockout |
| `Total_Deficit_Qty` | Total units short across all demand |
| `WFQ_Dependency_Flag` | 1 if item relies on quarantine inventory |
| `Schedule_Threat` | 1 if stockout occurs before PO lead time |
| `LeadDays` | Vendor lead time (default 30) |

**Business questions answered**: Where will we fail? When? Who is impacted? How bad? Can we recover?

---

### View 7 — `ETB_BUYER_CONTROL` (Buyer PO Consolidation Engine)

Groups deficit demand by item/vendor and recommends consolidated PO quantities that include safety stock. Classifies urgency based on lead-time windows and calculates total vendor exposure.

**Vendor resolution**: Same `COALESCE` strategy as View 6.

**Key outputs**:

| Column | Description |
|--------|-------------|
| `PRIME_VNDR` | Resolved vendor |
| `ITEMNMBR` | Item number |
| `ItemDescription` | Item description from vendor master |
| `UOM` | Unit of measure |
| `Earliest_Demand_Date` | Drop-dead date for PO placement |
| `Recommended_PO_Qty` | Deficit + Safety Stock (order quantity) |
| `Demand_Lines_In_Bucket` | Number of demand rows consolidated |
| `Vendor_Total_Exposure` | Total deficit across all items for this vendor |
| `LeadDays` | Vendor lead time (default 30) |
| `CalculatedSS_PurchasingUOM` | Safety stock in purchasing UOM |
| `Urgency` | PLACE_NOW / PLAN / MONITOR |

**Urgency classification**:
- **PLACE_NOW**: Stockout within 1x lead time
- **PLAN**: Stockout within 2x lead time
- **MONITOR**: Stockout beyond 2x lead time

---

### `ETB_SS_CALC` (Safety Stock Calculation)

Calculates safety stock using a demand variability model based on 2024–2025 weekly consumption from `PHR_MO_CostCalc1`. Derives average cost from receiving history, computes demand statistics, and applies a `2 × (MaxWeekly - AvgWeekly) × (LeadDays / 7)` formula.

**Lead time rules**: 100 days for series 30, 60 days for series 10, 45 days default.

**Key outputs**: `ITEMNMBR`, `PRIME_VNDR`, `LeadDays`, `CalculatedSS_PurchasingUOM`, `CalculatedSS_MfgUOM`, `PurchasingUOM`, `MfgUOM`, `AverageCost`, `SSValue`

**Filter**: Only items with `SSValue <= 20000` and valid vendor items (`INCLUDE_MRP = 'YES'`).

---

## Source Tables Referenced

| Table | Used By | Purpose |
|-------|---------|---------|
| `dbo.ETB_PAB_MO` | Views 1, 2, 4, 5 | Manufacturing order demand data |
| `dbo.ETB_ActiveDemand_Union_FG_MO` | Views 1, 2, 4, 5 | Active demand with FG/customer mapping |
| `dbo.Prosenthal_Vendor_Items` | Views 1, 2, 4, 5 | Item descriptions, UOM, vendor info |
| `dbo.PK010033` | Views 1, 2, 4, 5 | GP picklist (MRP issue tracking) |
| `dbo.WO010032` | Views 1, 2, 4, 5 | Work order status filter |
| `dbo.IV00101` | Views 1, 2, 3, 4, 5 | Item master |
| `dbo.IV00300` | View 3 | Lot-level inventory (quarantine) |
| `dbo.Prosenthal_INV_BIN_QTY_wQTYTYPE` | Views 2, 4, 5 | WC warehouse bin inventory |
| `dbo.ETB_WFQ_PIPE` | Views 4, 5 | WFQ pipeline (quarantine supply) |
| `dbo.ETB_SS` | ETB_SS_CALC | Safety stock master |
| `dbo.ReceivingsLineItems` | ETB_SS_CALC | Receiving history for avg cost |
| `dbo.POP30330` | ETB_SS_CALC | PO unit costs |
| `dbo.PHR_MO_CostCalc1` | ETB_SS_CALC | MO consumption history for demand stats |

---

## Deployment

### Prerequisites

- SQL Server 2016+ (requires `TRY_CAST`, `STRING_AGG`, `TRIM`)
- All source tables listed above must exist
- Appropriate database permissions (`CREATE VIEW`, `SELECT` on dependent objects)

### Installation Sequence

Execute in order — each view depends on its predecessors:

| Step | File | Object Created |
|------|------|----------------|
| 1 | `sql/01_etb_pab_auto.sql` | `dbo.ETB_PAB_AUTO` |
| 2 | `sql/02_etb_wc_inv_unified.sql` | `dbo.ETB_WC_INV_Unified` |
| 3 | `sql/03_etb_wfq_pipe.sql` | `dbo.ETB_WFQ_PIPE` |
| 4 | `sql/04_etb_pab_wfq_adj.sql` | `dbo.ETB_PAB_WFQ_ADJ` |
| 5 | `sql/05_etb_pab_supply_action.sql` | `dbo.ETB_PAB_SUPPLY_ACTION` |
| 6 | `sql/ETB_SS_CALC` | `dbo.ETB_SS_CALC` (if not already deployed) |
| 7 | `sql/06_etb_run_risk.sql` | `dbo.ETB_RUN_RISK` |
| 8 | `sql/07_etb_buyer_control.sql` | `dbo.ETB_BUYER_CONTROL` |

> **Note**: Views 1–4 are reference only (already deployed in SSMS). View 5 and the control layer views (6, 7) are production code.

### Validation

```sql
-- Verify all views exist
SELECT name, type_desc
FROM sys.views
WHERE name IN (
    'ETB_PAB_AUTO', 'ETB_WC_INV_Unified', 'ETB_WFQ_PIPE',
    'ETB_PAB_WFQ_ADJ', 'ETB_PAB_SUPPLY_ACTION',
    'ETB_RUN_RISK', 'ETB_BUYER_CONTROL'
);

-- Row count check
SELECT 'ETB_PAB_SUPPLY_ACTION' AS View_Name, COUNT(*) AS Rows FROM dbo.ETB_PAB_SUPPLY_ACTION
UNION ALL SELECT 'ETB_RUN_RISK', COUNT(*) FROM dbo.ETB_RUN_RISK
UNION ALL SELECT 'ETB_BUYER_CONTROL', COUNT(*) FROM dbo.ETB_BUYER_CONTROL;

-- Verify no NULL vendors in control layer
SELECT 'ETB_RUN_RISK' AS View_Name, COUNT(*) AS Null_Vendors
FROM dbo.ETB_RUN_RISK WHERE PRIME_VNDR IS NULL
UNION ALL
SELECT 'ETB_BUYER_CONTROL', COUNT(*)
FROM dbo.ETB_BUYER_CONTROL WHERE PRIME_VNDR IS NULL;
```

### Rollback

```sql
-- Drop in reverse order
IF EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_BUYER_CONTROL') DROP VIEW dbo.ETB_BUYER_CONTROL;
IF EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_RUN_RISK') DROP VIEW dbo.ETB_RUN_RISK;
IF EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_PAB_SUPPLY_ACTION') DROP VIEW dbo.ETB_PAB_SUPPLY_ACTION;
```

---

## Performance Recommendations

| View | Recommended Index |
|------|-------------------|
| `ETB_PAB_SUPPLY_ACTION` | `(ITEMNMBR, PRIME_VNDR, Demand_Due_Date) INCLUDE (Deficit_Qty, Suppression_Status, WFQ_Extended_Status, ItemDescription, UOM)` |
| `ETB_SS_CALC` | `(ITEMNMBR, PRIME_VNDR) INCLUDE (LeadDays, CalculatedSS_PurchasingUOM)` |

---

## Edge Case Handling

| Scenario | Handling |
|----------|----------|
| `Demand_Due_Date` is NULL | Excluded via WHERE clause in Views 6, 7 |
| `Deficit_Qty <= 0` | Excluded (overages are not stockout signals) |
| No matching safety stock record | LEFT JOIN with `ISNULL(LeadDays, 30)` fallback |
| `PRIME_VNDR` NULL in supply action | `COALESCE` with `ETB_SS_CALC.PRIME_VNDR` in Views 6, 7 |
| `WFQ_Extended_Status` is NULL | Treated as 0 for `WFQ_Dependency_Flag` |
| `Construct` is NULL | Excluded from `Client_Exposure_Count` |
| `First_Stockout_Date` is NULL | `Days_To_Stockout` = NULL, `Schedule_Threat` = 0 |
| "Beg Bal" row | Never suppressed; anchors all balance calculations |

---

## Key Design Decisions

1. **Pattern A Suppression**: Rows are flagged, not deleted. Both `Running_Balance` (raw) and `Adjusted_Running_Balance` (suppression-aware) are preserved for audit.

2. **Cumulative WFQ Overlay**: WFQ supply is summed cumulatively by due date. A consume-once waterfall model is not currently implemented.

3. **Vendor COALESCE Strategy**: Views 6 and 7 resolve `PRIME_VNDR` via `COALESCE(supply_action.PRIME_VNDR, ss_calc.PRIME_VNDR)` to ensure vendor is always populated when either source has a value.

4. **MAX Aggregation for Description/UOM**: `ItemDescription` and `UOM` use `MAX()` in aggregation CTEs rather than being added to `GROUP BY`, preserving rollup cardinality.

5. **No Table Modifications**: The entire pipeline operates through views only. No staging tables, temp tables, or data modifications.

---

## Documentation

- **[Architecture](docs/ARCHITECTURE.md)** — View hierarchy diagram and dependency map
- **[Control Layer](docs/CONTROL_LAYER.md)** — Detailed documentation for Views 6 and 7
- **[Deployment](docs/DEPLOYMENT.md)** — Step-by-step installation instructions
