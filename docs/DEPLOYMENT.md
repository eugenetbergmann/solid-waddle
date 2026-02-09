# ETB PAB Supply Chain Deployment

## Deployment Sequence

Execute views in the following order to maintain dependency integrity:

| Step | Object | File | Notes |
|------|--------|------|-------|
| 1 | ETB_PAB_AUTO | 01_etb_pab_auto.sql | Foundation layer - already exists in SSMS |
| 2 | ETB_WC_INV_Unified | 02_etb_wc_inv_unified.sql | Depends on ETB_PAB_AUTO - already exists in SSMS |
| 3 | ETB_WFQ_PIPE | 03_etb_wfq_pipe.sql | Source table - already exists in SSMS |
| 4 | ETB_PAB_WFQ_ADJ | 04_etb_pab_wfq_adj.sql | Depends on ETB_WC_INV_Unified - already exists in SSMS |
| 5 | ETB_PAB_SUPPLY_ACTION | 05_etb_pab_supply_action.sql | **NEW** - Deploy after upstream views |

## Prerequisites

- SQL Server 2016+ (for TRY_CAST function)
- Appropriate database permissions (CREATE VIEW, SELECT on dependent objects)
- Upstream views and tables already deployed in SSMS

## Deployment Steps

### Manual Deployment via SSMS

1. Open SSMS and connect to the target database
2. Open `sql/05_etb_pab_supply_action.sql`
3. Execute the script to create the view
4. Verify deployment with:

```sql
-- Check view exists
SELECT * FROM sys.views WHERE name = 'ETB_PAB_SUPPLY_ACTION';

-- Quick validation query
SELECT TOP 10 
    ITEMNMBR,
    Supply_Action_Recommendation,
    Additional_Order_Qty
FROM dbo.ETB_PAB_SUPPLY_ACTION;
```

### Automated Deployment (SQLCMD)

```bash
sqlcmd -S <server> -d <database> -i sql/05_etb_pab_supply_action.sql
```

## Validation Checklist

- [ ] View created successfully
- [ ] No errors in execution
- [ ] Returns expected columns
- [ ] Supply_Action_Recommendation populated for all rows
- [ ] No NULL values in critical calculation columns

## Rollback Procedure

```sql
IF EXISTS (SELECT * FROM sys.views WHERE name = 'ETB_PAB_SUPPLY_ACTION')
    DROP VIEW dbo.ETB_PAB_SUPPLY_ACTION;
GO
```

## Key Output Columns

| Column | Description |
|--------|-------------|
| Supply_Action_Recommendation | SUFFICIENT, ORDER, BOTH, REVIEW_REQUIRED |
| Additional_Order_Qty | Quantity to order after accounting for existing POs |
| Deficit_Qty | Shortfall between demand and extended balance |
| PO_On_Time | 1 if PO arrives before demand due date |
| Is_Past_Due_In_Backlog | 1 if demand due date has passed |
