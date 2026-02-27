# sql/ — ETB PAB Pipeline SQL Views

This directory is the **single source of truth** for all deployed SQL views.
All files are plain `WITH/SELECT` statements for manual execution in SSMS.
Do **not** add `CREATE OR ALTER VIEW` headers — deployment pattern is manual SSMS execution.

## View Index

| File | View Name | Description |
|------|-----------|-------------|
| `01_etb_pab_auto.sql` | ETB_PAB_AUTO | Foundation layer — demand normalization, MO matching, UNASSIGNED vendor fallback |
| `02_etb_ss_calc.sql` | ETB_SS_CALC | Safety stock reference — lead times, demand statistics, SS quantities |
| `03_etb_wfq_pipe.sql` | ETB_WFQ_PIPE | WFQ supply pipeline — lot-level quarantine inventory with release estimates |
| `04_etb_pab_wfq_adj.sql` | ETB_PAB_WFQ_ADJ | WFQ overlay — stockout detection, extended balance, WFQ status |
| `05_etb_pab_supply_action.sql` | ETB_PAB_SUPPLY_ACTION | Decision surface — SUFFICIENT/ORDER/BOTH/REVIEW_REQUIRED per demand row |
| `08_etb_v_client_295_stockouts.sql` | ETB_V_CLIENT_295_STOCKOUTS | Client 295 stockout detection — item/run-level risk with shared demand analysis |

## View Notes

### 04_etb_pab_wfq_adj.sql (ETB_PAB_WFQ_ADJ — View 4)

Session 5 fix: Unicode smart quotes purged; CycleCount CTE stubbed (`WHERE 1=0`)
due to absence of `dbo.IV10300` in target environment; `CREATE OR ALTER VIEW`
header removed for manual SSMS deployment pattern.

- **Smart quotes:** All U+2018/U+2019/U+201C/U+201D replaced with ASCII equivalents.
  SQL Server parser rejects Unicode typographic quotes. Never author SQL through
  rich-text systems (Word, web forms) that auto-convert apostrophes.
- **CycleCount CTE:** Stubbed with `WHERE 1 = 0` returning typed NULLs.
  `dbo.IV10300` does not exist in the target environment. Output columns
  `Last_Cycle_Count_Date`, `Days_Since_Last_Cycle_Count`, `Cycle_Count_Status`
  are preserved in the SELECT list for schema compatibility with View 5,
  and will output as `NULL` / `NEVER_COUNTED`.
- **No CREATE VIEW header:** Deployment is manual SSMS execution. The file opens
  and executes cleanly in SSMS as a plain `WITH/SELECT` statement.

## Deployment Order

Execute in this order to maintain dependency integrity:

```
01 → 02 → 03 → 04 → 05 → 08
```

See `docs/DEPLOYMENT.md` for full deployment instructions and validation queries.

## Environment Constraints

- `dbo.IV10300` is **not present** in the target database — do not reference in any view
- Deployment is manual SSMS execution — do not include `CREATE OR ALTER VIEW` in SQL files
- All SQL must be authored/stored in plain-text editors only (VS Code, SSMS)
  — rich-text systems corrupt apostrophes to Unicode smart quotes
