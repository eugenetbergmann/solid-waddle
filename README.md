# ETB PAB Supply Chain — SQL View Pipeline

A complete, production-ready SQL Server view pipeline that transforms raw manufacturing order and inventory data into actionable supply chain decisions. The system progresses from a foundational PAB (Projected Available Balance) ledger through suppression, WFQ overlay, and supply action logic, culminating in client-specific stockout detection.

---

## Repository Structure

```
sql/                                     ← SINGLE SOURCE OF TRUTH (all 6 views)
  01_etb_pab_auto.sql                    # View 1 — PAB ledger foundation
  02_etb_ss_calc.sql                     # View 2 — Safety stock calculation
  03_etb_wfq_pipe.sql                    # View 3 — WFQ pipeline source data
  04_etb_pab_wfq_adj.sql                 # View 4 — WFQ overlay + extended balance
  05_etb_pab_supply_action.sql           # View 5 — Supply action decision surface
  08_etb_v_client_295_stockouts.sql      # View 8 — Client 295 stockout detection

docs/
  ARCHITECTURE.md                        # View hierarchy & dependency diagram
  CONTROL_LAYER.md                       # View 8 executive summary
  DEPLOYMENT.md                          # Installation instructions

decisions/
  decisions.jsonl                        # Append-only decision log
  experiences.jsonl                      # Append-only experience log

plans/archive/                           # Archived planning documents
SKILL.md                                 # Quick-start guide for agents/developers
validate.sh                              # Pre-commit validation (14 checkpoints)
```

**Note**: Views 6 (`ETB_RUN_RISK`) and 7 (`ETB_BUYER_CONTROL`) have been **removed** —
they were not actively used. `pipeline-views/` and `analysis-views/` directories have
been consolidated into `sql/` (Session 5 — 2026-02-27).

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
                    │  View 2: ETB_SS_CALC         │
                    │  Safety Stock Calculation     │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │  View 3: ETB_WFQ_PIPE        │
                    │  WFQ Pipeline Source          │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │  View 4: ETB_PAB_WFQ_ADJ     │
                    │  WFQ Overlay + Extended Bal   │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │  View 5: ETB_PAB_SUPPLY_     │
                    │  ACTION — Decision Surface    │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │  View 8: ETB_V_CLIENT_295_   │
                    │  STOCKOUTS                    │
                    │  Client 295 Stockout Monitor  │
                    └─────────────────────────────┘
```

---

## Object Catalog

| # | Object | SQL File | Role | Dependencies | Status |
|---|--------|----------|------|--------------|--------|
| 1 | `ETB_PAB_AUTO` | `sql/01_etb_pab_auto.sql` | PAB ledger: demand normalization, MO matching, UNASSIGNED vendor fallback | `ETB_PAB_MO`, `ETB_ActiveDemand_Union_FG_MO`, `Prosenthal_Vendor_Items`, `PK010033`, `WO010032`, `IV00101` | Production |
| 2 | `ETB_SS_CALC` | `sql/02_etb_ss_calc.sql` | Safety stock: lead times, demand statistics, SS quantities | `ETB_SS`, `ReceivingsLineItems`, `POP30330`, `PHR_MO_CostCalc1` | Production |
| 3 | `ETB_WFQ_PIPE` | `sql/03_etb_wfq_pipe.sql` | WFQ pipeline source: lot-level quarantine inventory with release estimates | `IV00300`, `IV00101` | Production |
| 4 | `ETB_PAB_WFQ_ADJ` | `sql/04_etb_pab_wfq_adj.sql` | WFQ overlay: stockout detection, extended balance, WFQ status classification | Views 1–3 (re-inlined) + `Prosenthal_INV_BIN_QTY_wQTYTYPE`, `IV10300` | Production |
| 5 | `ETB_PAB_SUPPLY_ACTION` | `sql/05_etb_pab_supply_action.sql` | Final decision surface: deficit analysis, PO timing, supply action recommendations | Views 1–4 (re-inlined) | Production |
| 8 | `ETB_V_CLIENT_295_STOCKOUTS` | `sql/08_etb_v_client_295_stockouts.sql` | Client 295 stockout detection: item/run-level risk with shared demand analysis | View 5 + View 2 + View 4 | Production |

---

## View Details

### View 1 — `ETB_PAB_AUTO` (PAB Ledger Foundation)

Builds the authoritative item-level demand ledger from manufacturing orders. Joins MO data with active demand and vendor item descriptions, deduplicates via ranked windows, and enriches with MRP issue tracking from the GP picklist table (`PK010033`).

**Key outputs**: `ITEMNMBR`, `ItemDescription`, `UOM`, `Running_Balance`, `BEG_BAL`, `Deductions`, `Expiry`, `PO's`, `PRIME_VNDR`, `Unified_Value`

**Invariant**: This is the single source of truth for baseline ledger math. The "Beg Bal" row seeds all downstream balance calculations.

---

### View 2 — `ETB_SS_CALC` (Safety Stock Calculation)

Calculates safety stock using a demand variability model based on 2024–2025 weekly consumption from `PHR_MO_CostCalc1`. Derives average cost from receiving history, computes demand statistics, and applies a `2 × (MaxWeekly - AvgWeekly) × (LeadDays / 7)` formula.

**Lead time rules**: 100 days for series 30, 60 days for series 10, 45 days default.

**Key outputs**: `ITEMNMBR`, `PRIME_VNDR`, `LeadDays`, `CalculatedSS_PurchasingUOM`, `CalculatedSS_MfgUOM`, `PurchasingUOM`, `MfgUOM`, `AverageCost`, `SSValue`

**Filter**: Only items with `SSValue <= 20000` and valid vendor items (`INCLUDE_MRP = 'YES'`).

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

### View 8 — `ETB_V_CLIENT_295_STOCKOUTS` (Client 295 Stockout Detection)

Provides clear, actionable stockout signals scoped to Construct 295 while capturing market-wide demand context. The key design goal is one binary stockout signal per item/run — `Is_Stockout = YES/NO` — with full visibility into how much of the total demand Client 295 represents (`Shared_Demand_Ratio`).

**4-step pipeline**:

1. **`Client295_Demand`** — Non-suppressed positive-demand rows for `Construct = '295'`, bucketed by ISO year-week into `Run_Bucket`.
2. **`AllCustomers_Demand` + `AggDemand_Summary`** — All customers' non-suppressed demand for the same item/run combinations. Aggregates `Aggregate_Demand_All_Customers`, `Customer_Count`, and `Affected_Customers` (STRING_AGG comma list).
3. **`Stockout_Detection`** — Groups Client 295 rows by item/run and determines `Is_Stockout` (`YES` if `MIN(Adjusted_Running_Balance) < 0`), `Stockout_Qty` (ABS of min balance), and balance range.
4. **`Final_Output`** — Joins steps 1–3, calculates `Shared_Demand_Ratio`, applies vendor fallback (`ETB_SS_CALC` → `ETB_PAB_SUPPLY_ACTION` → `ETB_PAB_WFQ_ADJ`), and emits the buyer-facing result set.

**Key outputs**:

| Column | Description |
|--------|-------------|
| `ITEMNMBR` | Item number |
| `Item_Description` | Item description from vendor master |
| `UOM` | Unit of measure |
| `Run_Bucket` | ISO year-week run identifier (e.g. `2026-W09`) |
| `Is_Stockout` | `YES` if `MIN(Adjusted_Running_Balance) < 0`, else `NO` |
| `Stockout_Qty` | `ABS(MIN(Adjusted_Running_Balance))` when stocked out, else 0 |
| `Client295_Demand` | Sum of Client 295's `Net_Demand` for this item/run |
| `Aggregate_Demand_All_Customers` | Sum of ALL customers' `Net_Demand` for this item/run |
| `Shared_Demand_Ratio` | `Client295_Demand / Aggregate_Demand_All_Customers` |
| `Customer_Count` | Distinct customers sharing this item/run |
| `Affected_Customers` | Comma-separated list of ALL customers sharing this item/run |
| `Primary_Vendor` | Vendor via COALESCE: `ETB_SS_CALC` → `ETB_PAB_SUPPLY_ACTION` → `ETB_PAB_WFQ_ADJ` |
| `WFQ_Dependency_Flag` | 1 if any row in this run has WFQ rescue/enhancement status |

---

## Source Tables Referenced

| Table | Used By | Purpose |
|-------|---------|---------|
| `dbo.ETB_PAB_MO` | Views 1, 4, 5 | Manufacturing order demand data |
| `dbo.ETB_ActiveDemand_Union_FG_MO` | Views 1, 4, 5 | Active demand with FG/customer mapping |
| `dbo.Prosenthal_Vendor_Items` | Views 1, 4, 5 | Item descriptions, UOM, vendor info |
| `dbo.PK010033` | Views 1, 4, 5 | GP picklist (MRP issue tracking) |
| `dbo.WO010032` | Views 1, 4, 5 | Work order status filter |
| `dbo.IV00101` | Views 1, 3, 4, 5 | Item master |
| `dbo.IV00300` | View 3 | Lot-level inventory (quarantine) |
| `dbo.Prosenthal_INV_BIN_QTY_wQTYTYPE` | Views 4, 5 | WC warehouse bin inventory |
| `dbo.ETB_WFQ_PIPE` | Views 4, 5 | WFQ pipeline (quarantine supply) |
| `dbo.ETB_SS` | View 2 | Safety stock master |
| `dbo.ReceivingsLineItems` | View 2 | Receiving history for avg cost |
| `dbo.POP30330` | View 2 | PO unit costs |
| `dbo.PHR_MO_CostCalc1` | View 2 | MO consumption history for demand stats |

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
| 2 | `sql/02_etb_ss_calc.sql` | `dbo.ETB_SS_CALC` |
| 3 | `sql/03_etb_wfq_pipe.sql` | `dbo.ETB_WFQ_PIPE` |
| 4 | `sql/04_etb_pab_wfq_adj.sql` | `dbo.ETB_PAB_WFQ_ADJ` |
| 5 | `sql/05_etb_pab_supply_action.sql` | `dbo.ETB_PAB_SUPPLY_ACTION` |
| 6 | `sql/08_etb_v_client_295_stockouts.sql` | `dbo.ETB_V_CLIENT_295_STOCKOUTS` |

### Validation

```sql
-- Verify all views exist
SELECT name, type_desc
FROM sys.views
WHERE name IN (
    'ETB_PAB_AUTO', 'ETB_SS_CALC', 'ETB_WFQ_PIPE',
    'ETB_PAB_WFQ_ADJ', 'ETB_PAB_SUPPLY_ACTION',
    'ETB_V_CLIENT_295_STOCKOUTS'
);

-- Row count check
SELECT 'ETB_PAB_SUPPLY_ACTION' AS View_Name, COUNT(*) AS Rows FROM dbo.ETB_PAB_SUPPLY_ACTION
UNION ALL SELECT 'ETB_V_CLIENT_295_STOCKOUTS', COUNT(*) FROM dbo.ETB_V_CLIENT_295_STOCKOUTS;

-- Verify no NULL vendors
SELECT 'ETB_V_CLIENT_295_STOCKOUTS' AS View_Name, COUNT(*) AS Null_Vendors
FROM dbo.ETB_V_CLIENT_295_STOCKOUTS WHERE Primary_Vendor IS NULL;
```

### Rollback

```sql
-- Drop in reverse order
IF EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_V_CLIENT_295_STOCKOUTS') DROP VIEW dbo.ETB_V_CLIENT_295_STOCKOUTS;
IF EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_PAB_SUPPLY_ACTION') DROP VIEW dbo.ETB_PAB_SUPPLY_ACTION;
IF EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_PAB_WFQ_ADJ') DROP VIEW dbo.ETB_PAB_WFQ_ADJ;
IF EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_WFQ_PIPE') DROP VIEW dbo.ETB_WFQ_PIPE;
IF EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_SS_CALC') DROP VIEW dbo.ETB_SS_CALC;
IF EXISTS (SELECT 1 FROM sys.views WHERE name = 'ETB_PAB_AUTO') DROP VIEW dbo.ETB_PAB_AUTO;
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
| `Demand_Due_Date` is NULL | Excluded via WHERE clause |
| `Deficit_Qty <= 0` | Excluded (overages are not stockout signals) |
| No matching safety stock record | LEFT JOIN with `ISNULL(LeadDays, 30)` fallback |
| `PRIME_VNDR` NULL in supply action | Four-tier fallback: SS_CALC → SUPPLY_ACTION → WFQ_ADJ → UNASSIGNED |
| `WFQ_Extended_Status` is NULL | Treated as 0 for `WFQ_Dependency_Flag` |
| `Construct` is NULL | Excluded from `Client_Exposure_Count` |
| `First_Stockout_Date` is NULL | `Days_To_Stockout` = NULL, `Schedule_Threat` = 0 |
| "Beg Bal" row | Never suppressed; anchors all balance calculations |
| Client 295 is sole customer for item/run | `Customer_Count = 1`, `Shared_Demand_Flag = 'NO'`, `Shared_Demand_Ratio = 1.0` |
| `Aggregate_Demand_All_Customers = 0` | `Shared_Demand_Ratio` = NULL (divide-by-zero guard) |

---

## Key Design Decisions

1. **Pattern A Suppression**: Rows are flagged, not deleted. Both `Running_Balance` (raw) and `Adjusted_Running_Balance` (suppression-aware) are preserved for audit.

2. **Cumulative WFQ Overlay**: WFQ supply is summed cumulatively by due date. A consume-once waterfall model is not currently implemented.

3. **Vendor Fallback Hierarchy**: View 8 uses a three-tier vendor resolution strategy: `ETB_SS_CALC` → `ETB_PAB_SUPPLY_ACTION` → `ETB_PAB_WFQ_ADJ`, ensuring complete vendor coverage.

4. **MAX Aggregation for Description/UOM**: `ItemDescription` and `UOM` use `MAX()` in aggregation CTEs rather than being added to `GROUP BY`, preserving rollup cardinality.

5. **No Table Modifications**: The entire pipeline operates through views only. No staging tables, temp tables, or data modifications.

---

## Documentation

- **[Architecture](docs/ARCHITECTURE.md)** — View hierarchy diagram and dependency map
- **[Control Layer](docs/CONTROL_LAYER.md)** — Detailed documentation for View 8
- **[Deployment](docs/DEPLOYMENT.md)** — Step-by-step installation instructions
- **[SKILL.md](SKILL.md)** — Quick-start guide for agents and developers
