# ETB PAB Supply Chain

Production-ready SQL objects for the ETB PAB (Product Availability Balance) supply chain analysis system.

## Overview

This repository contains the 5 core PAB supply chain objects that form a complete, functional decision surface for supply action recommendations.

## Repository Structure

```
/sql/
  01_etb_pab_auto.sql              # Foundation demand + inventory suppression
  02_etb_wc_inv_unified.sql        # Inventory netting + demand adjustment
  03_etb_wfq_pipe.sql              # WFQ pipeline source data (reference)
  04_etb_pab_wfq_adj.sql           # WFQ pipeline overlay + extended balance
  05_etb_pab_supply_action.sql     # Final decision surface (NEW PRODUCTION CODE)

/docs/
  ARCHITECTURE.md                  # View hierarchy & dependencies
  DEPLOYMENT.md                    # Installation instructions
```

## Object Catalog

| # | Object | Role | Status |
|---|--------|------|--------|
| 1 | ETB_PAB_AUTO | Foundation demand + inventory suppression | Reference only |
| 2 | ETB_WC_INV_Unified | Inventory netting + demand adjustment | Reference only |
| 3 | ETB_WFQ_PIPE | WFQ pipeline source data | Reference only |
| 4 | ETB_PAB_WFQ_ADJ | WFQ pipeline overlay + extended balance | Reference only |
| 5 | **ETB_PAB_SUPPLY_ACTION** | Final decision surface (supply action recommendations) | **NEW** |

## Key Points

- Views 1-4 are reference only (already deployed in SSMS)
- Only `ETB_PAB_SUPPLY_ACTION` is new production code
- All 5 objects form a complete, functional chain
- No table modifications required

## Quick Start

1. Review [ARCHITECTURE.md](docs/ARCHITECTURE.md) for dependency overview
2. Follow deployment steps in [DEPLOYMENT.md](docs/DEPLOYMENT.md)
3. Deploy `sql/05_etb_pab_supply_action.sql` via SSMS

## Decision Recommendations

The final view (`ETB_PAB_SUPPLY_ACTION`) outputs one of:

- **SUFFICIENT**: Ledger balance covers demand
- **ORDER**: New orders required
- **BOTH**: Partial coverage - additional orders needed
- **REVIEW_REQUIRED**: Edge cases requiring manual review
