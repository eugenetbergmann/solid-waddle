Overview
This repository contains the 5 core PAB supply chain objects that form a complete, functional decision surface for supply action recommendations:

PAB ledger foundation (seeded by “Beg Bal” beginning inventory snapshot)
WC suppression + adjusted balance (prevents phantom/double-counted demand while preserving audit trail)
WFQ pipeline source (supply visibility)
WFQ overlay (extends the ledger during stockouts)
Supply action decision surface (SUFFICIENT / ORDER / BOTH / REVIEW_REQUIRED)
Repository Structure
text

Copy
/sql/
  01_etb_pab_auto.sql              # PAB ledger foundation (Beg Bal + demand/expiry/PO impacts)
  02_etb_wc_inv_unified.sql        # WC suppression (Pattern A) + Adjusted_Running_Balance
  03_etb_wfq_pipe.sql              # WFQ pipeline source data (reference)
  04_etb_pab_wfq_adj.sql           # WFQ overlay during stockouts + extended balance
  05_etb_pab_supply_action.sql     # Final decision surface (supply action recommendations)

/docs/
  ARCHITECTURE.md                  # View hierarchy & dependencies
  DEPLOYMENT.md                    # Installation instructions
Object Catalog
Table


#	Object	Role	Status
1	ETB_PAB_AUTO	PAB ledger: Beg Bal seed + deductions/expiry + PO increases reflected in Running_Balance	Reference only
2	ETB_WC_INV_Unified	Pattern A suppression + netting + Adjusted_Running_Balance (suppression-aware ledger)	Reference only
3	ETB_WFQ_PIPE	WFQ pipeline source data used for overlay and timing	Reference only
4	ETB_PAB_WFQ_ADJ	WFQ overlay applied after stockout threshold + extended ledger balance	Reference only
5	ETB_PAB_SUPPLY_ACTION	Final decision surface: deficit, PO coverage/timing proxy, and recommended action	New production code
Key Points
Views 1–4 are reference only (already deployed in SSMS).
Only ETB_PAB_SUPPLY_ACTION is new production code (per the current plan).
All 5 objects form a complete functional chain.
No table modifications required.
The Beg Bal row is foundational:
ORDERNUMBER = 'Beg Bal' in ETB_PAB_AUTO
It seeds the ledger so demand/expiry deductions and PO supply can drive Running_Balance.
Architecture (What each layer adds)
1) ETB_PAB_AUTO — PAB Ledger Foundation
Purpose: Create the authoritative item-level ledger used by downstream logic.

Core outputs:

Running_Balance (authoritative PAB ledger)
BEG_BAL (beginning inventory snapshot; exposed as text)
Demand enrichment (e.g., MRP_IssueDate, WCID_From_MO, Issued, Remaining)
Identifiers like Unified_Value to help track/relate rows
Invariant:

ETB_PAB_AUTO is the single source of truth for the baseline ledger math.
2) ETB_WC_INV_Unified — WC Suppression + Adjusted Balance (Pattern A)
Purpose: Prevent double-counted/phantom demand due to WC matching logic without deleting ledger rows.

Pattern A behavior (required):

Do not remove rows from the ledger.
Add:
Is_Suppressed (0/1)
Demand_Status (e.g., VALID DEMAND / SUPPRESSED…)
Adjusted_Running_Balance (suppression-aware ledger)
Why two balances exist:

Running_Balance: exact value from ETB_PAB_AUTO (full ledger)
Adjusted_Running_Balance: recomputed version where suppressed rows contribute zero delta
How Adjusted_Running_Balance is computed (conceptually):

Convert Running_Balance to numeric (TRY_CAST)
Derive row delta:
Δ
𝑖
=
𝑅
𝐵
𝑖
−
𝑅
𝐵
𝑖
−
1
Δ 
i
​
 =RB 
i
​
 −RB 
i−1
​
 
If row is suppressed: set 
Δ
𝑖
=
0
Δ 
i
​
 =0
Re-sum deltas from the Beg Bal anchor to produce Adjusted_Running_Balance
Critical rule:

The “Beg Bal” row must never be suppressed or removed.
3) ETB_WFQ_PIPE — WFQ Pipeline Source (Reference)
Purpose: Provide WFQ supply signal (e.g., QTY_ON_HAND, Estimated_Release_Date) at ITEM_LEVEL.

Used downstream to determine whether WFQ supply can rescue or enhance the projected ledger at/after stockout.

4) ETB_PAB_WFQ_ADJ — WFQ Overlay + Extended Ledger
Purpose: Extend the suppression-aware ledger during stockouts using WFQ supply availability.

Stockout detection:

Stockout threshold is detected using Adjusted_Running_Balance (not the raw RB):
First sequenced demand row where Adjusted_Running_Balance <= 0
WFQ overlay rules (current model):

WFQ supply is counted when Estimated_Release_Date <= DUEDATE
WFQ is applied only at/after the stockout point
Output columns include:
Ledger_WFQ_Influx
Ledger_Extended_Balance = Adjusted_Running_Balance + Ledger_WFQ_Influx
WFQ_Extended_Status:
LEDGER_ONLY
WFQ_RESCUED
WFQ_ENHANCED
WFQ_INSUFFICIENT
Important note on allocation model:

Current overlay is cumulative by due date (i.e., sums WFQ qty released up to each due date).
If a consume-once allocation is needed, that requires a different model (waterfall / bucket depletion).
5) ETB_PAB_SUPPLY_ACTION — Final Decision Surface (New Production Code)
Purpose: Produce supply action recommendations by combining:

Extended ledger status,
Net demand and deficits,
PO quantity visibility ([PO's] parsed to numeric),
PO timing proxy derived from WFQ pipeline release dates.
New computed outputs:

Deficit_Qty
POs_On_Order_Qty (safe numeric parse of [PO's])
Demand_Due_Date
PO_On_Time, Is_Past_Due_In_Backlog
Supply_Action_Recommendation
Additional_Order_Qty
