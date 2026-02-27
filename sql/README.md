# sql/ — Deployment Copies

## Purpose

This directory contains the SQL scripts used for deploying the ETB PAB Supply Chain
views to the production SQL Server database.

---

## Directory Relationship

```
pipeline-views/    ← CANONICAL SOURCE for Views 1–5
analysis-views/    ← CANONICAL SOURCE for View 8 (Client 295 stockouts)
sql/               ← DEPLOYMENT COPIES (this directory)
```

**Rule**: `sql/01–05` must always be **identical** to `pipeline-views/01–05`.
`sql/08_etb_v_client_295_stockouts.sql` must always be identical to
`analysis-views/08_etb_v_client_295_stockouts.sql`.

**Never edit `sql/01–05` or `sql/08` directly.**
Always edit the canonical source in `pipeline-views/` or `analysis-views/`,
then sync to `sql/` using the procedure below.

---

## File Inventory

| File | View | Canonical Source | Notes |
|------|------|-----------------|-------|
| `01_etb_pab_auto.sql` | ETB_PAB_AUTO (View 1) | `pipeline-views/01_etb_pab_auto.sql` | Mirror — do not edit here |
| `02_etb_ss_calc.sql` | ETB_SS_CALC (View 2) | `pipeline-views/02_etb_ss_calc.sql` | Mirror — do not edit here |
| `02_etb_wc_inv_unified.sql` | ETB_WC_INV_UNIFIED | `analysis-views/02_etb_wc_inv_unified.sql` | Mirror — do not edit here |
| `03_etb_wfq_pipe.sql` | ETB_WFQ_PIPE (View 3) | `pipeline-views/03_etb_wfq_pipe.sql` | Mirror — do not edit here |
| `04_etb_pab_wfq_adj.sql` | ETB_PAB_WFQ_ADJ (View 4) | `pipeline-views/04_etb_pab_wfq_adj.sql` | Mirror — do not edit here |
| `05_etb_pab_supply_action.sql` | ETB_PAB_SUPPLY_ACTION (View 5) | `pipeline-views/05_etb_pab_supply_action.sql` | Mirror — do not edit here |
| `06_etb_run_risk.sql` | ETB_RUN_RISK (View 6) | **This directory** | Control-layer view — no pipeline-views/ counterpart |
| `07_etb_buyer_control.sql` | ETB_BUYER_CONTROL (View 7) | **This directory** | Control-layer view — no pipeline-views/ counterpart |
| `08_etb_v_client_295_stockouts.sql` | ETB_V_CLIENT_295_STOCKOUTS (View 8) | `analysis-views/08_etb_v_client_295_stockouts.sql` | Mirror — do not edit here |
| `ETB_SS_CALC` | (reference query) | — | Legacy reference file — not a deployment script |

---

## Sync Procedure

After modifying any file in `pipeline-views/` or `analysis-views/`, run:

```bash
# Sync Views 1–5 from pipeline-views/
cp pipeline-views/01_etb_pab_auto.sql          sql/01_etb_pab_auto.sql
cp pipeline-views/02_etb_ss_calc.sql           sql/02_etb_ss_calc.sql
cp pipeline-views/03_etb_wfq_pipe.sql          sql/03_etb_wfq_pipe.sql
cp pipeline-views/04_etb_pab_wfq_adj.sql       sql/04_etb_pab_wfq_adj.sql
cp pipeline-views/05_etb_pab_supply_action.sql sql/05_etb_pab_supply_action.sql

# Sync View 8 from analysis-views/
cp analysis-views/08_etb_v_client_295_stockouts.sql sql/08_etb_v_client_295_stockouts.sql

# Sync WC Unified from analysis-views/
cp analysis-views/02_etb_wc_inv_unified.sql sql/02_etb_wc_inv_unified.sql
```

Then verify with:

```bash
diff pipeline-views/01_etb_pab_auto.sql          sql/01_etb_pab_auto.sql
diff pipeline-views/02_etb_ss_calc.sql           sql/02_etb_ss_calc.sql
diff pipeline-views/03_etb_wfq_pipe.sql          sql/03_etb_wfq_pipe.sql
diff pipeline-views/04_etb_pab_wfq_adj.sql       sql/04_etb_pab_wfq_adj.sql
diff pipeline-views/05_etb_pab_supply_action.sql sql/05_etb_pab_supply_action.sql
diff analysis-views/08_etb_v_client_295_stockouts.sql sql/08_etb_v_client_295_stockouts.sql
```

Or use the repository's `validate.sh` script (CP-4 check):

```bash
./validate.sh
```

---

## Deployment Order

Views must be deployed in dependency order. See [`docs/DEPLOYMENT.md`](../docs/DEPLOYMENT.md)
for the full deployment sequence, validation queries, and rollback procedure.

Quick reference:

```
1 → ETB_PAB_AUTO
2 → ETB_SS_CALC
3 → ETB_WFQ_PIPE
4 → ETB_PAB_WFQ_ADJ
5 → ETB_PAB_SUPPLY_ACTION
6 → ETB_RUN_RISK
7 → ETB_BUYER_CONTROL
8 → ETB_V_CLIENT_295_STOCKOUTS
```

---

## Control-Layer Views (Views 6, 7, 8)

Views 6, 7, and 8 are **control-layer views** — they consume View 5
(`dbo.ETB_PAB_SUPPLY_ACTION`) and View 2 (`dbo.ETB_SS_CALC`) as their primary
data sources.  They have no counterpart in `pipeline-views/` because they are
not part of the core 5-view pipeline.

- **View 6 (ETB_RUN_RISK)**: Executive risk dashboard — stockout timing, client exposure
- **View 7 (ETB_BUYER_CONTROL)**: Buyer action queue — PO consolidation, EOQ optimization
- **View 8 (ETB_V_CLIENT_295_STOCKOUTS)**: Client 295 stockout detection

To modify Views 6 or 7, edit `sql/06_etb_run_risk.sql` or `sql/07_etb_buyer_control.sql`
directly (they have no canonical source elsewhere).

To modify View 8, edit `analysis-views/08_etb_v_client_295_stockouts.sql` (canonical source),
then sync to `sql/08_etb_v_client_295_stockouts.sql`.
