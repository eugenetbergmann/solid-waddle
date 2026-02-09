# ETB PAB Supply Chain Architecture

## View Hierarchy & Dependencies

```
                    ┌─────────────────────┐
                    │   ETB_PAB_AUTO      │
                    │  (Foundation Layer) │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ ETB_WC_INV_Unified │
                    │  (Inventory Netting)│
                    └──────────┬──────────┘
                               │
                ┌──────────────┴──────────────┐
                ▼                             ▼
    ┌─────────────────────┐       ┌─────────────────────┐
    │   ETB_WFQ_PIPE      │       │  ETB_PAB_WFQ_ADJ    │
    │  (Source Table)     │       │  (WFQ Overlay)      │
    └─────────────────────┘       └──────────┬──────────┘
                                              │
                                              ▼
                                    ┌─────────────────────┐
                                    │ ETB_PAB_SUPPLY_ACTION
                                    │  (Decision Surface) │
                                    └─────────────────────┘
```

## Object Catalog

### Views (4)

| # | Object | Role | Dependencies | Status |
|---|--------|------|--------------|--------|
| 1 | ETB_PAB_AUTO | Foundation demand + inventory suppression | Raw tables (ETB_PAB_MO, ETB_ActiveDemand_Union_FG_MO, etc.) | Reference only |
| 2 | ETB_WC_INV_Unified | Inventory netting + demand adjustment | ETB_PAB_AUTO | Reference only |
| 3 | ETB_PAB_WFQ_ADJ | WFQ pipeline overlay + extended balance | ETB_WC_INV_Unified, ETB_WFQ_PIPE | Reference only |
| 4 | ETB_PAB_SUPPLY_ACTION | Final decision surface (supply action recommendations) | ETB_PAB_WFQ_ADJ, ETB_WFQ_PIPE | **NEW** |

### Tables (1)

| # | Object | Role | Columns | Status |
|---|--------|------|---------|--------|
| 1 | ETB_WFQ_PIPE | WFQ pipeline source data | ITEM_Number, Estimated_Release_Date, Expected_Delivery_Date, QTY_ON_HAND, View_Level | Reference only |

## Data Flow

1. **ETB_PAB_AUTO**: Ingests raw demand and MO data, applies inventory suppression rules
2. **ETB_WC_INV_Unified**: Performs inventory netting against demand, calculates running balances
3. **ETB_WFQ_PIPE**: Provides WFQ supply pipeline data (timing and quantities)
4. **ETB_PAB_WFQ_ADJ**: Extends ledger with WFQ supply coverage, calculates extended balances
5. **ETB_PAB_SUPPLY_ACTION**: Evaluates supply adequacy, generates action recommendations

## Decision Logic (ETB_PAB_SUPPLY_ACTION)

| Rule | Condition | Recommendation |
|------|-----------|----------------|
| 1 | Ledger_Extended_Balance >= Net_Demand | SUFFICIENT |
| 2 | Deficit_Qty > 0 AND POs_On_Order_Qty = 0 | ORDER |
| 3 | Deficit_Qty > 0 AND POs_On_Order_Qty >= Deficit_Qty AND PO_On_Time = 0 | ORDER |
| 4 | Deficit_Qty > 0 AND POs_On_Order_Qty >= Deficit_Qty AND PO_On_Time = 1 | SUFFICIENT |
| 5 | Deficit_Qty > 0 AND 0 < POs_On_Order_Qty < Deficit_Qty | BOTH |
| Default | Edge case requiring review | REVIEW_REQUIRED |
