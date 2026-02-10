# PAB Supply Chain Decision Views Architecture

## System Overview

The PAB (Planned Availability Balance) system is a five-layer SQL view architecture that transforms raw manufacturing order (MO) demand and inventory data into actionable supply chain decisions. Each layer applies specific business logic—demand normalization, inventory suppression, WFQ (Work-in-Flight Queue) pipeline integration, and finally supply action recommendations—without aggregation or roll-ups. All rows remain visible with complete context, enabling operational teams to see the full decision surface and trace decisions back to source data.

The system embeds consistent numeric validation (BegBal pattern) across all views to ensure data integrity. It processes MO demand against warehouse inventory, applies suppression rules for stale or fully-covered demands, overlays WFQ pipeline supply, and generates five distinct supply action recommendations (SUFFICIENT, ORDER, BOTH, REVIEW_REQUIRED, or timing-based variants).

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                      SOURCE TABLES                              │
├─────────────────────────────────────────────────────────────────┤
│ ETB_PAB_MO                                                      │
│ ETB_ActiveDemand_Union_FG_MO                                    │
│ Prosenthal_Vendor_Items                                         │
│ PK010033 (Ledger)                                               │
│ Prosenthal_INV_BIN_QTY_wQTYTYPE (Warehouse Inventory)          │
│ ETB_WFQ_PIPE (WFQ Pipeline - Table)                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ VIEW 1: ETB_PAB_AUTO                                            │
│ Foundation: MO demand + inventory suppression                   │
│ • Normalizes order/item numbers                                 │
│ • Joins MO → ActiveDemand → VendorItems → Ledger               │
│ • Deduplicates by ORDERNUMBER, FG, ITEMNMBR                    │
│ • Embeds BegBal validation (ISNUMERIC)                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ VIEW 2: ETB_WC_INV_Unified                                      │
│ Inventory netting + demand adjustment                           │
│ • Joins View 1 → Warehouse Inventory (WC-W%, ≤45 days)         │
│ • Calculates Net_Demand (Demand - Inventory)                   │
│ • Applies suppression rules (Stale, Fence, Partial)            │
│ • Filters out suppressed rows                                   │
│ • Re-validates BegBal (defense-in-depth)                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ VIEW 3: ETB_WFQ_PIPE (TABLE - Reference Only)                  │
│ WFQ pipeline source data                                        │
│ • Columns: ITEM_Number, Estimated_Release_Date,               │
│   Expected_Delivery_Date, QTY_ON_HAND, View_Level              │
│ • Not a view; existing table in SSMS                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                ┌─────────────┴─────────────┐
                │                           │
                ▼                           ▼
┌──────────────────────────────┐  ┌──────────────────────────────┐
│ VIEW 4: ETB_PAB_WFQ_ADJ      │  │ (WFQ_PIPE feeds both)        │
│ WFQ overlay + extended       │  │                              │
│ balance calculation          │  │                              │
│ • Joins View 2 → WFQ_PIPE    │  │                              │
│ • Detects stockout sequence  │  │                              │
│ • Allocates WFQ supply       │  │                              │
│ • Calculates extended balance│  │                              │
│ • Classifies WFQ status      │  │                              │
│ • Triple-validates BegBal    │  │                              │
└──────────────────────────────┘  │                              │
                │                 │                              │
                └─────────────┬───┘                              │
                              │                                  │
                              ▼                                  │
┌─────────────────────────────────────────────────────────────────┐
│ VIEW 5: ETB_PAB_SUPPLY_ACTION                                   │
│ Final decision surface: supply action recommendations           │
│ • Joins View 4 → WFQ_PIPE (timing analysis)                    │
│ • Calculates deficit, PO timing, backlog status                │
│ • Applies 5 supply action rules                                │
│ • Quad-validates BegBal (final layer)                          │
│ • Output: Full enumerated decision surface                     │
└─────────────────────────────────────────────────────────────────┘
```

## View-by-View Documentation

### View 1: ETB_PAB_AUTO

**Purpose:** Foundation layer that normalizes MO demand data, joins to active demand and vendor attributes, deduplicates by order/item/FG, and matches to ledger for issue tracking.

**Dependencies:**
- `ETB_PAB_MO` (source MO demand)
- `ETB_ActiveDemand_Union_FG_MO` (demand context, FG, status)
- `Prosenthal_Vendor_Items` (vendor, lead times, safety stock)
- `PK010033` (ledger issue tracking)

**CTEs:**

- **CleanData:** Trims and normalizes ITEMNMBR, ORDERNUMBER, and other string fields. Creates CleanOrder and CleanItem (hyphens removed) for joining. Preserves all MO columns including BEG_BAL, Deductions, Running_Balance, Issued, and timing flags.

- **ActiveDemandJoin:** LEFT JOIN to ETB_ActiveDemand_Union_FG_MO on cleaned order/item numbers. Adds FG, FG Desc, STSDESCR, MRPTYPE for demand classification.

- **VendorItemJoin:** LEFT JOIN to Prosenthal_Vendor_Items on ITEMNMBR. Adds VendorItem, PRIME_VNDR, PURCHASING_LT, PLANNING_LT, ORDER_POINT_QTY, SAFETY_STOCK.

- **RankedData:** ROW_NUMBER() PARTITION BY ORDERNUMBER, FG, ITEMNMBR ORDER BY DUEDATE, MRP_IssueDate. Deduplicates to first (earliest due date) occurrence.

- **LedgerMatch:** LEFT JOIN to PK010033 on cleaned order/item. Adds Ledger_IssueDate, Ledger_QtyIssued. Filters to rn=1 (deduplicated rows only).

- **FinalOutput:** Embeds BegBal validation: ISNUMERIC(LTRIM(RTRIM(BEG_BAL))) = 1 → CAST to decimal(18,6), else 0. Casts result back to VARCHAR(50). Creates Unified_Value concatenation: ITEMNMBR|Date+Expiry|(Original_Required - Issued).

**Transformation Logic:**

1. Normalize all string fields (LTRIM, RTRIM) to remove leading/trailing spaces.
2. Create clean versions of order/item numbers (remove hyphens) for reliable joining across systems.
3. LEFT JOIN to ActiveDemand to enrich with FG and demand status; unmatched rows retain NULL.
4. LEFT JOIN to VendorItems to add supply chain attributes; unmatched rows retain NULL.
5. Deduplicate by order/item/FG, keeping earliest due date.
6. Match to ledger to track actual issue quantities and dates.
7. Validate BegBal numerically; default to 0 if non-numeric or NULL.

**Output Columns:**

| Column | Data Type | Purpose |
|--------|-----------|---------|
| ITEMNMBR | VARCHAR | Item number (normalized) |
| ItemDescription | VARCHAR | Item description |
| UOM | VARCHAR | Unit of measure |
| ORDERNUMBER | VARCHAR | Manufacturing order number |
| Construct | VARCHAR | Order construct/type |
| DUEDATE | DATE | Demand due date |
| Expiry Dates | VARCHAR | Expiry date range |
| Date + Expiry | VARCHAR | Combined date + expiry |
| BEG_BAL | VARCHAR(50) | Beginning balance (validated numeric) |
| Deductions | NUMERIC | Deductions from balance |
| Expiry | NUMERIC | Expiry quantity |
| PO's | VARCHAR | POs on order (quantity) |
| Running_Balance | VARCHAR | Current running balance |
| MRP_IssueDate | DATE | MRP issue date |
| WCID_From_MO | VARCHAR | Work center ID |
| Issued | NUMERIC | Quantity issued |
| Original_Required | NUMERIC | Original required quantity |
| VendorItem | VARCHAR | Vendor item code |
| PRIME_VNDR | VARCHAR | Primary vendor |
| PURCHASING_LT | NUMERIC | Purchasing lead time (days) |
| PLANNING_LT | NUMERIC | Planning lead time (days) |
| ORDER_POINT_QTY | NUMERIC | Reorder point quantity |
| SAFETY_STOCK | NUMERIC | Safety stock quantity |
| FG | VARCHAR | Finished good code |
| FG Desc | VARCHAR | Finished good description |
| STSDESCR | VARCHAR | Status description |
| MRPTYPE | VARCHAR | MRP type |
| Unified_Value | VARCHAR | Concatenated key: ITEMNMBR\|Date+Expiry\|(Req-Issued) |

**Validation Logic:**

BegBal pattern: `ISNUMERIC(LTRIM(RTRIM(BEG_BAL))) = 1 → CAST(... AS decimal(18,6)) ELSE 0`. Embedded in FinalOutput CTE. Ensures all downstream views receive numeric BegBal; non-numeric or NULL values default to 0, preventing calculation errors.

---

### View 2: ETB_WC_INV_Unified

**Purpose:** Inventory netting layer that joins View 1 to warehouse inventory, calculates net demand after inventory offset, applies three suppression rules, and filters out suppressed rows.

**Dependencies:**
- `ETB_PAB_AUTO` (View 1)
- `Prosenthal_INV_BIN_QTY_wQTYTYPE` (warehouse inventory by bin/location)

**CTEs:**

- **RawData:** Selects all columns from View 1. Re-validates BegBal with ISNUMERIC logic (defense-in-depth).

- **InventoryJoin:** LEFT JOIN to Prosenthal_INV_BIN_QTY_wQTYTYPE on ITEMNMBR. Filters to SITE LIKE 'WC-W%' (warehouse locations) and DATEDIFF(DAY, TODAY, DUEDATE) ≤ 45 (within 45-day window). Adds QTYTYPE, BIN, Inventory_Qty_Available (QTY), Inventory_Site, Inventory_Location, Days_To_Due.

- **DemandCalculation:** Calculates Net_Demand and suppression flags:
  - **Net_Demand:** If Inventory_Qty_Available IS NULL or ≤ 0 → Original_Required. If Inventory_Qty_Available ≥ Original_Required → 0. Else → Original_Required - Inventory_Qty_Available.
  - **Suppress_Stale:** 1 if DUEDATE ≤ TODAY-7 AND (Issued = 0 OR NULL). Identifies old, unissued demands.
  - **Suppress_Fence:** 1 if DUEDATE ≤ TODAY+7 AND Inventory_Qty_Available ≥ Original_Required AND Inventory_Qty_Available > 0. Identifies near-term demands fully covered by inventory.

- **SuppressionStatus:** Assigns Suppression_Status label:
  - 'SUPPRESSED: Stale & Unissued' if Suppress_Stale = 1
  - 'SUPPRESSED: Full Coverage in Fence' if Suppress_Fence = 1
  - 'PARTIAL: Partial Coverage' if Net_Demand < Original_Required AND Net_Demand > 0
  - 'COVERED: Full Inventory Coverage' if Net_Demand = 0
  - 'ACTIVE: No Suppression' else

**Transformation Logic:**

1. Re-validate BegBal from View 1 (defense-in-depth).
2. LEFT JOIN to warehouse inventory filtered to WC-W% sites within 45-day window.
3. Calculate Net_Demand: demand minus available inventory, with edge cases for NULL, zero, or excess inventory.
4. Apply Suppress_Stale rule: demands older than 7 days with no issues are stale and suppressed.
5. Apply Suppress_Fence rule: demands within 7 days with full inventory coverage are suppressed (no action needed).
6. Classify suppression status for visibility.
7. Filter final output to exclude rows where Suppress_Stale = 1 OR Suppress_Fence = 1.

**Output Columns:**

| Column | Data Type | Purpose |
|--------|-----------|---------|
| ITEMNMBR | VARCHAR | Item number |
| ItemDescription | VARCHAR | Item description |
| UOM | VARCHAR | Unit of measure |
| ORDERNUMBER | VARCHAR | Manufacturing order number |
| Construct | VARCHAR | Order construct |
| DUEDATE | DATE | Demand due date |
| Expiry Dates | VARCHAR | Expiry date range |
| Date + Expiry | VARCHAR | Combined date + expiry |
| BEG_BAL | VARCHAR(50) | Beginning balance (re-validated) |
| Deductions | NUMERIC | Deductions |
| Expiry | NUMERIC | Expiry quantity |
| PO's | VARCHAR | POs on order |
| Running_Balance | VARCHAR | Running balance |
| MRP_IssueDate | DATE | MRP issue date |
| WCID_From_MO | VARCHAR | Work center ID |
| Issued | NUMERIC | Quantity issued |
| Original_Required | NUMERIC | Original required quantity |
| Net_Demand | NUMERIC | Demand after inventory offset |
| Inventory_Qty_Available | NUMERIC | Warehouse inventory available |
| Suppression_Status | VARCHAR | Suppression classification |
| VendorItem | VARCHAR | Vendor item code |
| PRIME_VNDR | VARCHAR | Primary vendor |
| PURCHASING_LT | NUMERIC | Purchasing lead time |
| PLANNING_LT | NUMERIC | Planning lead time |
| ORDER_POINT_QTY | NUMERIC | Reorder point |
| SAFETY_STOCK | NUMERIC | Safety stock |
| FG | VARCHAR | Finished good |
| FG Desc | VARCHAR | Finished good description |
| STSDESCR | VARCHAR | Status description |
| MRPTYPE | VARCHAR | MRP type |
| Unified_Value | VARCHAR | Concatenated key |

**Validation Logic:**

BegBal re-parsed with ISNUMERIC in RawData CTE. Ensures View 2 output is clean numeric; any non-numeric BegBal from View 1 is corrected to 0. This defense-in-depth approach prevents downstream calculation errors.

---

### View 4: ETB_PAB_WFQ_ADJ

**Purpose:** WFQ overlay layer that integrates WFQ pipeline supply, detects stockout sequences, allocates WFQ supply to demand rows, and calculates extended balance (ledger + WFQ).

**Dependencies:**
- `ETB_WC_INV_Unified` (View 2)
- `ETB_WFQ_PIPE` (WFQ pipeline table)

**CTEs:**

- **Demand_Ledger:** Selects all columns from View 2. Triple-validates BegBal with ISNUMERIC logic.

- **Demand_Seq:** ROW_NUMBER() PARTITION BY ITEMNMBR ORDER BY DUEDATE, ORDERNUMBER. Assigns sequence number to each demand row per item, ordered by due date.

- **Stockout_Detection:** Identifies the first demand sequence where Running_Balance ≤ 0 or NULL. Groups by ITEMNMBR, returns MIN(Demand_Seq) as Stockout_Seq. If no stockout, Stockout_Seq is NULL.

- **WFQ_Supply:** Aggregates ETB_WFQ_PIPE filtered to View_Level = 'ITEM_LEVEL' and QTY_ON_HAND > 0. Groups by ITEM_Number, Estimated_Release_Date, Expected_Delivery_Date. Sums QTY_ON_HAND as WFQ_Qty_Available.

- **WFQ_Allocated:** LEFT JOIN Demand_Seq to Stockout_Detection on ITEMNMBR. For each demand row, calculates WFQ_Influx: SUM of WFQ_Qty_Available where Expected_Delivery_Date ≤ Demand_DUEDATE AND Demand_Seq ≥ Stockout_Seq (or Demand_Seq if no stockout). Defaults to 0 if no matching WFQ supply.

- **Extended_Ledger:** Calculates Ledger_Extended_Balance = TRY_CAST(Running_Balance AS decimal(18,6)) + WFQ_Influx. Classifies WFQ_Extended_Status:
  - 'LEDGER_ONLY' if WFQ_Influx ≤ 0
  - 'WFQ_RESCUED' if Extended_Balance > 0 AND Running_Balance ≤ 0 (WFQ saves a stockout)
  - 'WFQ_ENHANCED' if Extended_Balance > 0 (WFQ improves balance)
  - 'WFQ_INSUFFICIENT' else (WFQ present but insufficient)

**Transformation Logic:**

1. Triple-validate BegBal from View 2.
2. Assign demand sequence numbers per item by due date.
3. Detect stockout point: first demand where ledger balance goes negative.
4. Aggregate WFQ supply from pipeline, filtered to item-level and positive quantities.
5. Allocate WFQ supply to each demand row: include WFQ if expected delivery ≤ demand due date AND demand sequence ≥ stockout sequence (or all demands if no stockout).
6. Calculate extended balance: ledger balance + WFQ influx.
7. Classify WFQ status: LEDGER_ONLY (no WFQ), WFQ_RESCUED (saves stockout), WFQ_ENHANCED (improves balance), WFQ_INSUFFICIENT (WFQ present but insufficient).

**Output Columns:**

| Column | Data Type | Purpose |
|--------|-----------|---------|
| ITEMNMBR | VARCHAR | Item number |
| ItemDescription | VARCHAR | Item description |
| UOM | VARCHAR | Unit of measure |
| ORDERNUMBER | VARCHAR | Manufacturing order number |
| Construct | VARCHAR | Order construct |
| DUEDATE | DATE | Demand due date |
| Expiry Dates | VARCHAR | Expiry date range |
| Date + Expiry | VARCHAR | Combined date + expiry |
| BEG_BAL | VARCHAR(50) | Beginning balance (triple-validated) |
| Deductions | NUMERIC | Deductions |
| Expiry | NUMERIC | Expiry quantity |
| PO's | VARCHAR | POs on order |
| Running_Balance | VARCHAR | Running balance (from ledger) |
| MRP_IssueDate | DATE | MRP issue date |
| WCID_From_MO | VARCHAR | Work center ID |
| Issued | NUMERIC | Quantity issued |
| Original_Required | NUMERIC | Original required quantity |
| Net_Demand | NUMERIC | Net demand (after inventory) |
| Inventory_Qty_Available | NUMERIC | Warehouse inventory |
| Suppression_Status | VARCHAR | Suppression classification |
| VendorItem | VARCHAR | Vendor item code |
| PRIME_VNDR | VARCHAR | Primary vendor |
| PURCHASING_LT | NUMERIC | Purchasing lead time |
| PLANNING_LT | NUMERIC | Planning lead time |
| ORDER_POINT_QTY | NUMERIC | Reorder point |
| SAFETY_STOCK | NUMERIC | Safety stock |
| FG | VARCHAR | Finished good |
| FG Desc | VARCHAR | Finished good description |
| STSDESCR | VARCHAR | Status description |
| MRPTYPE | VARCHAR | MRP type |
| Unified_Value | VARCHAR | Concatenated key |
| Ledger_WFQ_Influx | NUMERIC | WFQ supply allocated to this demand |
| Ledger_Extended_Balance | NUMERIC | Running_Balance + WFQ_Influx |
| WFQ_Extended_Status | VARCHAR | WFQ classification (LEDGER_ONLY, WFQ_RESCUED, WFQ_ENHANCED, WFQ_INSUFFICIENT) |

**Validation Logic:**

BegBal triple-validated in Demand_Ledger CTE. Ensures View 4 receives clean numeric BegBal; any non-numeric values from View 2 are corrected to 0. This layered validation prevents propagation of data quality issues.

---

### View 5: ETB_PAB_SUPPLY_ACTION

**Purpose:** Final decision surface that evaluates supply adequacy by comparing PO quantity and timing against demand deficit, outputting five supply action recommendations with full context.

**Dependencies:**
- `ETB_PAB_WFQ_ADJ` (View 4)
- `ETB_WFQ_PIPE` (WFQ pipeline for timing analysis)

**CTEs:**

- **WFQ_Extended:** Selects all columns from View 4. Quad-validates BegBal with ISNUMERIC logic (final layer).

- **Balance_Analysis:** Calculates:
  - **Deficit_Qty:** 0 if Ledger_Extended_Balance ≥ Net_Demand, else Net_Demand - Ledger_Extended_Balance.
  - **POs_On_Order_Qty:** ISNUMERIC([PO's]) = 1 → CAST to decimal(18,6), else 0. Safe handling of non-numeric or NULL.
  - **Demand_Due_Date:** TRY_CAST([Date + Expiry] AS DATE).

- **WFQ_Turnaround_Analysis:** Extracts timing from ETB_WFQ_PIPE filtered to View_Level = 'ITEM_LEVEL' and QTY_ON_HAND > 0. Calculates Turn_Around_Days = DATEDIFF(DAY, Estimated_Release_Date, Expected_Delivery_Date). Sets WFQ_Actual_Arrival_Date = Expected_Delivery_Date.

- **PO_Timing_Analysis:** LEFT JOIN Balance_Analysis to WFQ_Turnaround_Analysis on ITEMNMBR. Adds:
  - **PO_Release_Date:** Estimated_Release_Date from WFQ pipeline.
  - **PO_Turn_Around_Days:** Turn-around days from WFQ pipeline.
  - **PO_Actual_Arrival_Date:** Expected_Delivery_Date from WFQ pipeline.
  - **PO_On_Time:** 1 if WFQ_Actual_Arrival_Date ≤ Demand_Due_Date, else 0.
  - **Is_Past_Due_In_Backlog:** 1 if Demand_Due_Date < TODAY, else 0.

- **Supply_Action_Decision:** Applies five supply action rules in order:
  1. **SUFFICIENT:** Ledger_Extended_Balance ≥ Net_Demand (no action needed).
  2. **ORDER:** Deficit_Qty > 0 AND POs_On_Order_Qty = 0 (no POs; must order).
  3. **ORDER:** Deficit_Qty > 0 AND POs_On_Order_Qty ≥ Deficit_Qty AND PO_On_Time = 0 (POs cover deficit but late; must expedite or reorder).
  4. **SUFFICIENT:** Deficit_Qty > 0 AND POs_On_Order_Qty ≥ Deficit_Qty AND PO_On_Time = 1 (POs cover deficit and on time; sufficient).
  5. **BOTH:** Deficit_Qty > 0 AND POs_On_Order_Qty > 0 AND POs_On_Order_Qty < Deficit_Qty (POs partially cover; order additional).
  6. **REVIEW_REQUIRED:** Default edge case.

  Calculates **Additional_Order_Qty:** If Deficit_Qty > 0 AND POs_On_Order_Qty < Deficit_Qty → Deficit_Qty - POs_On_Order_Qty, else 0.

**Transformation Logic:**

1. Quad-validate BegBal from View 4 (final layer).
2. Calculate deficit: demand minus extended balance (ledger + WFQ).
3. Parse POs_On_Order_Qty safely; default to 0 if non-numeric.
4. Extract WFQ timing: release date, turn-around days, expected arrival date.
5. Determine PO on-time status: actual arrival ≤ demand due date.
6. Identify past-due demands: due date < today.
7. Apply five supply action rules in sequence (quantity first, then timing).
8. Calculate additional order quantity needed.

**Output Columns:**

| Column | Data Type | Purpose |
|--------|-----------|---------|
| ITEMNMBR | VARCHAR | Item number |
| ItemDescription | VARCHAR | Item description |
| UOM | VARCHAR | Unit of measure |
| ORDERNUMBER | VARCHAR | Manufacturing order number |
| Construct | VARCHAR | Order construct |
| DUEDATE | DATE | Demand due date |
| Expiry Dates | VARCHAR | Expiry date range |
| Date + Expiry | VARCHAR | Combined date + expiry |
| BEG_BAL | VARCHAR(50) | Beginning balance (quad-validated) |
| Deductions | NUMERIC | Deductions |
| Expiry | NUMERIC | Expiry quantity |
| PO's | VARCHAR | POs on order (raw) |
| Running_Balance | VARCHAR | Running balance (ledger) |
| MRP_IssueDate | DATE | MRP issue date |
| WCID_From_MO | VARCHAR | Work center ID |
| Issued | NUMERIC | Quantity issued |
| Original_Required | NUMERIC | Original required quantity |
| Net_Demand | NUMERIC | Net demand (after inventory) |
| Inventory_Qty_Available | NUMERIC | Warehouse inventory |
| Suppression_Status | VARCHAR | Suppression classification |
| VendorItem | VARCHAR | Vendor item code |
| PRIME_VNDR | VARCHAR | Primary vendor |
| PURCHASING_LT | NUMERIC | Purchasing lead time |
| PLANNING_LT | NUMERIC | Planning lead time |
| ORDER_POINT_QTY | NUMERIC | Reorder point |
| SAFETY_STOCK | NUMERIC | Safety stock |
| FG | VARCHAR | Finished good |
| FG Desc | VARCHAR | Finished good description |
| STSDESCR | VARCHAR | Status description |
| MRPTYPE | VARCHAR | MRP type |
| Unified_Value | VARCHAR | Concatenated key |
| Ledger_WFQ_Influx | NUMERIC | WFQ supply allocated |
| Ledger_Extended_Balance | NUMERIC | Ledger + WFQ balance |
| WFQ_Extended_Status | VARCHAR | WFQ classification |
| Deficit_Qty | NUMERIC | Shortfall quantity (0 if sufficient) |
| POs_On_Order_Qty | NUMERIC | PO quantity on order (parsed) |
| Demand_Due_Date | DATE | Demand due date (parsed) |
| PO_Release_Date | DATE | WFQ release date |
| PO_Turn_Around_Days | INT | Days from release to delivery |
| PO_Actual_Arrival_Date | DATE | Expected delivery date |
| PO_On_Time | INT | 1 if arrival ≤ due date, else 0 |
| Is_Past_Due_In_Backlog | INT | 1 if due date < today, else 0 |
| Supply_Action_Recommendation | VARCHAR | Decision: SUFFICIENT, ORDER, BOTH, REVIEW_REQUIRED |
| Additional_Order_Qty | NUMERIC | Shortfall after existing POs |

**Validation Logic:**

BegBal quad-validated in WFQ_Extended CTE (final layer). Ensures View 5 receives clean numeric BegBal; any non-numeric values are corrected to 0. This final validation guarantees data integrity at the decision surface.

---

## BegBal Validation Pattern

**Pattern Description:**

```sql
CAST(
    CASE 
        WHEN ISNUMERIC(LTRIM(RTRIM(BEG_BAL))) = 1 
            THEN CAST(LTRIM(RTRIM(BEG_BAL)) AS decimal(18, 6))
        ELSE 0 
    END AS VARCHAR(50)
) AS BEG_BAL
```

**Logic:**

1. LTRIM(RTRIM(BEG_BAL)): Remove leading/trailing whitespace.
2. ISNUMERIC(...) = 1: Test if the trimmed value is numeric.
3. If numeric: CAST to decimal(18,6) for precision arithmetic.
4. If non-numeric or NULL: Default to 0.
5. CAST result back to VARCHAR(50) for storage/display consistency.

**Why Embedded in Every View:**

Defense-in-depth. BegBal originates from ETB_PAB_MO and may contain non-numeric values, NULLs, or whitespace. Each view re-validates to ensure:
- View 1 (ETB_PAB_AUTO): Initial validation at foundation.
- View 2 (ETB_WC_INV_Unified): Re-validation after inventory join.
- View 4 (ETB_PAB_WFQ_ADJ): Triple-validation before WFQ calculations.
- View 5 (ETB_PAB_SUPPLY_ACTION): Quad-validation at decision surface.

This layered approach prevents data quality issues from propagating downstream and ensures all calculations use clean numeric values.

**Views Applying BegBal Validation:**

- ETB_PAB_AUTO (View 1): FinalOutput CTE
- ETB_WC_INV_Unified (View 2): RawData CTE
- ETB_PAB_WFQ_ADJ (View 4): Demand_Ledger CTE
- ETB_PAB_SUPPLY_ACTION (View 5): WFQ_Extended CTE

---

## Data Flow & Transformations

### Input Layer

**Source Tables:**

- **ETB_PAB_MO:** Raw manufacturing order demand with item numbers, order numbers, due dates, beginning balance, deductions, running balance, issue tracking.
- **ETB_ActiveDemand_Union_FG_MO:** Demand context including finished good (FG) codes, descriptions, status, MRP type.
- **Prosenthal_Vendor_Items:** Supply chain attributes: vendor, lead times (purchasing, planning), reorder points, safety stock.
- **PK010033:** Ledger table with issue dates and quantities issued.
- **Prosenthal_INV_BIN_QTY_wQTYTYPE:** Warehouse inventory by bin/location, site, quantity type.
- **ETB_WFQ_PIPE:** WFQ pipeline with item numbers, estimated release dates, expected delivery dates, quantities on hand, view level.

### Suppression Layer (View 2)

**Rules Applied:**

1. **Stale & Unissued:** DUEDATE ≤ TODAY-7 AND Issued = 0 or NULL → Suppress. Rationale: Old demands with no issue activity are stale; no action needed.
2. **Full Coverage in Fence:** DUEDATE ≤ TODAY+7 AND Inventory_Qty_Available ≥ Original_Required → Suppress. Rationale: Near-term demands fully covered by warehouse inventory; no supply action needed.
3. **Partial Coverage:** Net_Demand < Original_Required AND Net_Demand > 0 → Flag as PARTIAL. Rationale: Inventory partially covers demand; net demand is reduced but not eliminated.

**Output:** Filtered to exclude Stale and Fence suppressions. Partial coverage rows remain active with reduced Net_Demand.

### WFQ Application Layer (View 4)

**Stockout Detection:**

- Sequence demands per item by due date.
- Identify first demand where Running_Balance ≤ 0 or NULL.
- Mark as Stockout_Seq; if no stockout, Stockout_Seq = NULL.

**WFQ Allocation:**

- Aggregate WFQ supply from pipeline (item-level, positive quantities).
- For each demand row, allocate WFQ supply if Expected_Delivery_Date ≤ Demand_DUEDATE AND Demand_Seq ≥ Stockout_Seq.
- If no stockout, allocate WFQ to all demands (Demand_Seq ≥ Demand_Seq is always true).
- Sum allocated WFQ as WFQ_Influx.

**Extended Balance Calculation:**

- Ledger_Extended_Balance = Running_Balance + WFQ_Influx.
- Classify WFQ_Extended_Status:
  - LEDGER_ONLY: No WFQ contribution.
  - WFQ_RESCUED: WFQ saves a stockout (Running_Balance ≤ 0, Extended_Balance > 0).
  - WFQ_ENHANCED: WFQ improves balance (Extended_Balance > 0).
  - WFQ_INSUFFICIENT: WFQ present but insufficient (Extended_Balance ≤ 0).

### Decision Layer (View 5)

**Supply Action Rules:**

1. **SUFFICIENT:** Ledger_Extended_Balance ≥ Net_Demand. No action; demand is covered.
2. **ORDER:** Deficit_Qty > 0 AND POs_On_Order_Qty = 0. No POs on order; must place order.
3. **ORDER:** Deficit_Qty > 0 AND POs_On_Order_Qty ≥ Deficit_Qty AND PO_On_Time = 0. POs cover deficit but arrive late; must expedite or reorder.
4. **SUFFICIENT:** Deficit_Qty > 0 AND POs_On_Order_Qty ≥ Deficit_Qty AND PO_On_Time = 1. POs cover deficit and arrive on time; sufficient.
5. **BOTH:** Deficit_Qty > 0 AND POs_On_Order_Qty > 0 AND POs_On_Order_Qty < Deficit_Qty. POs partially cover; order additional quantity.
6. **REVIEW_REQUIRED:** Default edge case (e.g., missing data, unexpected condition).

**Additional Calculations:**

- **Deficit_Qty:** Shortfall after extended balance. 0 if sufficient, else positive shortfall.
- **Additional_Order_Qty:** Quantity to order beyond existing POs. Deficit_Qty - POs_On_Order_Qty if Deficit_Qty > 0 AND POs < Deficit_Qty, else 0.
- **PO_On_Time:** 1 if WFQ expected delivery ≤ demand due date, else 0.
- **Is_Past_Due_In_Backlog:** 1 if demand due date < today, else 0.

---

## Key Decision Rules (Complete Reference)

### Suppression Rules (View 2)

| Rule | Condition | Action | Rationale |
|------|-----------|--------|-----------|
| Stale & Unissued | DUEDATE ≤ TODAY-7 AND Issued = 0 or NULL | Suppress (exclude from View 2) | Old demands with no issue activity are stale; no supply action needed |
| Full Coverage in Fence | DUEDATE ≤ TODAY+7 AND Inventory_Qty_Available ≥ Original_Required | Suppress (exclude from View 2) | Near-term demands fully covered by warehouse inventory; no supply action needed |
| Partial Coverage | Net_Demand < Original_Required AND Net_Demand > 0 | Flag as PARTIAL; retain in View 2 | Inventory partially covers demand; net demand is reduced but not eliminated |

### WFQ Status Classification (View 4)

| Status | Condition | Meaning |
|--------|-----------|---------|
| LEDGER_ONLY | WFQ_Influx ≤ 0 | No WFQ supply allocated; rely on ledger balance only |
| WFQ_RESCUED | Running_Balance ≤ 0 AND (Running_Balance + WFQ_Influx) > 0 | WFQ supply saves a stockout; without WFQ, demand would be unmet |
| WFQ_ENHANCED | (Running_Balance + WFQ_Influx) > 0 | WFQ supply improves balance; demand is met with better margin |
| WFQ_INSUFFICIENT | (Running_Balance + WFQ_Influx) ≤ 0 | WFQ supply present but insufficient; demand remains unmet |

### Supply Action Rules (View 5)

| Rule | Condition | Recommendation | Additional_Order_Qty |
|------|-----------|-----------------|----------------------|
| 1 | Ledger_Extended_Balance ≥ Net_Demand | SUFFICIENT | 0 |
| 2 | Deficit_Qty > 0 AND POs_On_Order_Qty = 0 | ORDER | Deficit_Qty |
| 3 | Deficit_Qty > 0 AND POs_On_Order_Qty ≥ Deficit_Qty AND PO_On_Time = 0 | ORDER | 0 (expedite existing POs) |
| 4 | Deficit_Qty > 0 AND POs_On_Order_Qty ≥ Deficit_Qty AND PO_On_Time = 1 | SUFFICIENT | 0 |
| 5 | Deficit_Qty > 0 AND POs_On_Order_Qty > 0 AND POs_On_Order_Qty < Deficit_Qty | BOTH | Deficit_Qty - POs_On_Order_Qty |
| Default | Any other condition | REVIEW_REQUIRED | NULL |

---

## Column Mapping (Source → Final)

**Trace of key columns through all 5 views:**

| Column | View 1 | View 2 | View 4 | View 5 | Notes |
|--------|--------|--------|--------|--------|-------|
| ITEMNMBR | ✓ (normalized) | ✓ | ✓ | ✓ | Normalized in View 1; consistent through all views |
| ORDERNUMBER | ✓ (normalized) | ✓ | ✓ | ✓ | Normalized in View 1; used for deduplication |
| DUEDATE | ✓ | ✓ | ✓ | ✓ | Demand due date; used for sequencing and timing |
| BEG_BAL | ✓ (validated) | ✓ (re-validated) | ✓ (triple-validated) | ✓ (quad-validated) | Validated at each layer; defense-in-depth |
| Running_Balance | ✓ | ✓ | ✓ | ✓ | Ledger balance; used for stockout detection |
| Original_Required | ✓ | ✓ | ✓ | ✓ | Original demand quantity; baseline for calculations |
| Issued | ✓ | ✓ | ✓ | ✓ | Quantity issued; used for stale detection |
| Inventory_Qty_Available | — | ✓ (added) | ✓ | ✓ | Added in View 2; used for Net_Demand calculation |
| Net_Demand | — | ✓ (added) | ✓ | ✓ | Added in View 2; demand after inventory offset |
| Suppression_Status | — | ✓ (added) | ✓ | ✓ | Added in View 2; classification of suppression rules |
| Ledger_WFQ_Influx | — | — | ✓ (added) | ✓ | Added in View 4; WFQ supply allocated to demand |
| Ledger_Extended_Balance | — | — | ✓ (added) | ✓ | Added in View 4; Running_Balance + WFQ_Influx |
| WFQ_Extended_Status | — | — | ✓ (added) | ✓ | Added in View 4; WFQ classification |
| Deficit_Qty | — | — | — | ✓ (added) | Added in View 5; shortfall quantity |
| POs_On_Order_Qty | — | — | — | ✓ (added) | Added in View 5; parsed PO quantity |
| Demand_Due_Date | — | — | — | ✓ (added) | Added in View 5; parsed due date |
| PO_Release_Date | — | — | — | ✓ (added) | Added in View 5; from WFQ pipeline |
| PO_Turn_Around_Days | — | — | — | ✓ (added) | Added in View 5; calculated from WFQ pipeline |
| PO_Actual_Arrival_Date | — | — | — | ✓ (added) | Added in View 5; from WFQ pipeline |
| PO_On_Time | — | — | — | ✓ (added) | Added in View 5; 1 if arrival ≤ due date |
| Is_Past_Due_In_Backlog | — | — | — | ✓ (added) | Added in View 5; 1 if due date < today |
| Supply_Action_Recommendation | — | — | — | ✓ (added) | Added in View 5; final decision (SUFFICIENT, ORDER, BOTH, REVIEW_REQUIRED) |
| Additional_Order_Qty | — | — | — | ✓ (added) | Added in View 5; quantity to order beyond existing POs |

---

## Query Examples

### Example 1: Find All Items Requiring Orders

```sql
SELECT 
    ITEMNMBR,
    ItemDescription,
    ORDERNUMBER,
    DUEDATE,
    Net_Demand,
    Deficit_Qty,
    POs_On_Order_Qty,
    Additional_Order_Qty,
    Supply_Action_Recommendation
FROM dbo.ETB_PAB_SUPPLY_ACTION
WHERE Supply_Action_Recommendation = 'ORDER'
ORDER BY DUEDATE ASC, ITEMNMBR ASC;
```

**Purpose:** Identify all items with supply action = ORDER. These items have a deficit and either no POs on order or POs that arrive late. Operational teams use this to prioritize new purchase orders.

### Example 2: Identify Items in Receiving Backlog

```sql
SELECT 
    ITEMNMBR,
    ItemDescription,
    ORDERNUMBER,
    DUEDATE,
    Is_Past_Due_In_Backlog,
    Deficit_Qty,
    PO_Actual_Arrival_Date,
    Supply_Action_Recommendation
FROM dbo.ETB_PAB_SUPPLY_ACTION
WHERE Is_Past_Due_In_Backlog = 1
  AND Deficit_Qty > 0
ORDER BY DUEDATE ASC;
```

**Purpose:** Find items with past-due demands and active deficits. These are backlog items requiring immediate attention. Receiving teams use this to expedite inbound shipments.

### Example 3: Find WFQ Rescues

```sql
SELECT 
    ITEMNMBR,
    ItemDescription,
    ORDERNUMBER,
    DUEDATE,
    Running_Balance,
    Ledger_WFQ_Influx,
    Ledger_Extended_Balance,
    WFQ_Extended_Status,
    Supply_Action_Recommendation
FROM dbo.ETB_PAB_SUPPLY_ACTION
WHERE WFQ_Extended_Status = 'WFQ_RESCUED'
ORDER BY ITEMNMBR ASC, DUEDATE ASC;
```

**Purpose:** Identify items where WFQ supply saves a stockout (Running_Balance ≤ 0 but Extended_Balance > 0). These are critical dependencies on WFQ pipeline; if WFQ fails, demand becomes unmet. Planning teams use this to monitor WFQ reliability.

### Example 4: Show Items with Timing Conflicts

```sql
SELECT 
    ITEMNMBR,
    ItemDescription,
    ORDERNUMBER,
    DUEDATE,
    PO_Actual_Arrival_Date,
    PO_On_Time,
    Deficit_Qty,
    Supply_Action_Recommendation
FROM dbo.ETB_PAB_SUPPLY_ACTION
WHERE PO_On_Time = 0
  AND Deficit_Qty > 0
ORDER BY DUEDATE ASC;
```

**Purpose:** Find items where POs exist but arrive after demand due date. These are timing conflicts requiring expediting or alternative sourcing. Supply chain teams use this to identify expedite opportunities.

### Example 5: View Full Decision Surface for Single Item

```sql
SELECT 
    ITEMNMBR,
    ItemDescription,
    ORDERNUMBER,
    DUEDATE,
    Original_Required,
    Net_Demand,
    Inventory_Qty_Available,
    Running_Balance,
    Ledger_WFQ_Influx,
    Ledger_Extended_Balance,
    Deficit_Qty,
    POs_On_Order_Qty,
    Additional_Order_Qty,
    PO_On_Time,
    Is_Past_Due_In_Backlog,
    Supply_Action_Recommendation,
    Suppression_Status,
    WFQ_Extended_Status
FROM dbo.ETB_PAB_SUPPLY_ACTION
WHERE ITEMNMBR = 'ABC123'
ORDER BY DUEDATE ASC;
```

**Purpose:** Drill into a single item to see complete decision context: demand, inventory, ledger, WFQ, deficit, POs, timing, and final recommendation. Useful for root-cause analysis and decision validation.

---

## Deployment & Testing

### View Creation Order

Deploy views in this sequence to satisfy dependencies:

1. **ETB_PAB_AUTO** (View 1): Foundation. Depends only on source tables.
2. **ETB_WC_INV_Unified** (View 2): Depends on View 1.
3. **ETB_PAB_WFQ_ADJ** (View 4): Depends on View 2 and ETB_WFQ_PIPE table.
4. **ETB_PAB_SUPPLY_ACTION** (View 5): Depends on View 4 and ETB_WFQ_PIPE table.

Note: ETB_WFQ_PIPE (View 3) is a table, not a view. It must exist before deploying Views 4 and 5.

### Quick Validation Queries

**Check BegBal Validation:**

```sql
SELECT 
    COUNT(*) AS Total_Rows,
    COUNT(CASE WHEN BEG_BAL = '0' THEN 1 END) AS Defaulted_To_Zero,
    COUNT(CASE WHEN BEG_BAL <> '0' THEN 1 END) AS Non_Zero_Values
FROM dbo.ETB_PAB_AUTO;
```

**Expected:** All rows have BEG_BAL as VARCHAR(50); defaulted values are '0'; non-zero values are numeric strings.

**Count Rows by View:**

```sql
SELECT 
    'View 1: ETB_PAB_AUTO' AS View_Name,
    COUNT(*) AS Row_Count
FROM dbo.ETB_PAB_AUTO
UNION ALL
SELECT 
    'View 2: ETB_WC_INV_Unified',
    COUNT(*)
FROM dbo.ETB_WC_INV_Unified
UNION ALL
SELECT 
    'View 4: ETB_PAB_WFQ_ADJ',
    COUNT(*)
FROM dbo.ETB_PAB_WFQ_ADJ
UNION ALL
SELECT 
    'View 5: ETB_PAB_SUPPLY_ACTION',
    COUNT(*)
FROM dbo.ETB_PAB_SUPPLY_ACTION;
```

**Expected:** Row counts decrease from View 1 → View 2 (suppression filters), then remain stable or increase slightly in Views 4 and 5 (WFQ allocation and decision calculations).

**Verify Decision Categories:**

```sql
SELECT 
    Supply_Action_Recommendation,
    COUNT(*) AS Count
FROM dbo.ETB_PAB_SUPPLY_ACTION
GROUP BY Supply_Action_Recommendation
ORDER BY Count DESC;
```

**Expected:** Rows distributed across SUFFICIENT, ORDER, BOTH, REVIEW_REQUIRED. SUFFICIENT should be largest (no action needed); ORDER and BOTH should be significant (action required).

**Check WFQ Status Distribution:**

```sql
SELECT 
    WFQ_Extended_Status,
    COUNT(*) AS Count
FROM dbo.ETB_PAB_SUPPLY_ACTION
GROUP BY WFQ_Extended_Status
ORDER BY Count DESC;
```

**Expected:** LEDGER_ONLY should be largest (no WFQ); WFQ_RESCUED and WFQ_ENHANCED should be present (WFQ supply allocated); WFQ_INSUFFICIENT should be small (WFQ present but insufficient).

---

## Scaffolding for Next Stage

### Identified Decision Points

1. **Suppression Override:** View 2 suppresses stale and fence-covered demands. Next stage could allow operational override (e.g., "force review of stale demand if customer escalates").

2. **WFQ Reliability Scoring:** View 4 allocates WFQ supply but doesn't assess reliability. Next stage could score WFQ supply by historical on-time delivery, supplier risk, or lead time variance.

3. **Expedite vs. Reorder:** View 5 recommends ORDER for late POs but doesn't distinguish between expediting existing POs vs. placing new orders. Next stage could add expedite cost/lead time analysis.

4. **Multi-Item Constraints:** Views 1-5 treat each item independently. Next stage could add constraints (e.g., "cannot order more than X units per supplier per week" or "must order in multiples of Y").

5. **Demand Prioritization:** View 5 treats all demands equally. Next stage could prioritize by customer tier, margin, or strategic importance.

### Data Enrichment Opportunities

1. **Supplier Performance Data:** Join to supplier on-time delivery, quality, and lead time variance. Use to score WFQ reliability and adjust safety stock.

2. **Customer Tier & Margin:** Join to customer master. Use to prioritize demands and set service level targets.

3. **Inventory Aging:** Join to inventory transaction history. Identify slow-moving stock and adjust reorder points.

4. **Demand Forecast Accuracy:** Join to forecast vs. actual history. Adjust safety stock and planning lead times based on forecast error.

5. **Capacity Constraints:** Join to production capacity by work center. Identify bottlenecks and adjust MRP issue dates.

### Recommended Next-Stage Inputs

1. **Expedite Recommendation View:** Consume View 5. For each ORDER recommendation with PO_On_Time = 0, calculate expedite cost vs. stockout cost. Recommend expedite if cost-effective.

2. **Supplier Allocation View:** Consume View 5. Group by PRIME_VNDR and sum Additional_Order_Qty. Allocate orders to suppliers based on capacity, lead time, and cost.

3. **Safety Stock Adjustment View:** Consume View 5. For each item, calculate stockout frequency and lead time variance. Recommend safety stock adjustment.

4. **Demand Prioritization View:** Consume View 5. Rank demands by customer tier, margin, and strategic importance. Recommend fulfillment sequence if capacity is constrained.

5. **WFQ Reliability Scorecard:** Consume View 4 and ETB_WFQ_PIPE. For each supplier/item, calculate on-time delivery rate, lead time variance, and quantity variance. Flag unreliable suppliers.

### Extension Patterns

1. **Non-Destructive Enrichment:** Add new columns to View 5 output without modifying existing logic. Example: Add `Expedite_Cost`, `Expedite_Lead_Days`, `Expedite_Recommendation` as new CTEs in a wrapper view.

2. **Parallel Decision Branches:** Create separate views for different decision contexts (e.g., "ETB_PAB_EXPEDITE_ANALYSIS" for expedite decisions, "ETB_PAB_SUPPLIER_ALLOCATION" for supplier allocation). Each consumes View 5 independently.

3. **Aggregation Views:** Create roll-up views that consume View 5 and aggregate by supplier, customer, work center, or time period. Example: "ETB_PAB_SUPPLIER_SUMMARY" groups by PRIME_VNDR and sums Additional_Order_Qty.

4. **Scenario Analysis:** Create parameterized views that consume View 5 and apply "what-if" scenarios (e.g., "what if safety stock increases by 10%?" or "what if lead time increases by 5 days?"). Use table-valued parameters or temporary tables for scenario inputs.

5. **Audit Trail:** Create a logging view that captures View 5 output at regular intervals (daily, weekly). Use for trend analysis, decision audit, and performance measurement.

---

