#!/bin/bash
# =============================================================================
# validate.sh — Solid Waddle ETB PAB Pipeline Pre-Commit Validation
# =============================================================================
# Implements the 14-checkpoint Ralph Loop verification gate.
# Run before every commit that touches pipeline-views/ or analysis-views/.
#
# Usage:
#   chmod +x validate.sh
#   ./validate.sh
#
# Exit codes:
#   0 — All checks passed (safe to commit)
#   1 — One or more checks failed (do NOT commit)
#
# Note on CP-5 (ISNUMERIC ban):
#   Uses grep -r 'ISNUMERIC(' (with parenthesis) to match only function calls.
#   Plain 'ISNUMERIC' without parenthesis produces false positives from changelog
#   lines like "-- replaced ISNUMERIC with TRY_CAST".
# =============================================================================

set -euo pipefail

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

pass() { echo "  PASS: $1"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { echo "  FAIL: $1"; FAIL_COUNT=$((FAIL_COUNT + 1)); }
warn() { echo "  WARN: $1"; WARN_COUNT=$((WARN_COUNT + 1)); }

echo ""
echo "============================================================"
echo " Solid Waddle ETB PAB Pipeline — Pre-Commit Validation"
echo " $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "============================================================"

# =============================================================================
# PHASE 1: ISOLATE CHECKS (CP-1 through CP-4)
# =============================================================================
echo ""
echo "--- Phase 1: ISOLATE ---"

# CP-1: Context files exist
echo ""
echo "[CP-1] Context files present"
for f in SKILL.md docs/ARCHITECTURE.md docs/CONTROL_LAYER.md docs/DEPLOYMENT.md; do
    if [ -f "$f" ]; then
        pass "$f exists"
    else
        fail "$f MISSING"
    fi
done

# CP-2: SCOPE.md exists
echo ""
echo "[CP-2] SCOPE.md present"
if [ -f "SCOPE.md" ]; then
    pass "SCOPE.md exists"
else
    warn "SCOPE.md not found — create before committing"
fi

# CP-3: Dependency chain — all 5 pipeline views present
echo ""
echo "[CP-3] Pipeline view files present"
for f in pipeline-views/01_etb_pab_auto.sql \
          pipeline-views/02_etb_ss_calc.sql \
          pipeline-views/03_etb_wfq_pipe.sql \
          pipeline-views/04_etb_pab_wfq_adj.sql \
          pipeline-views/05_etb_pab_supply_action.sql; do
    if [ -f "$f" ]; then
        pass "$f"
    else
        fail "$f MISSING"
    fi
done

# CP-4: sql/ mirrors pipeline-views/ (01-05 must be identical)
echo ""
echo "[CP-4] sql/ mirrors pipeline-views/ (01-05 identical)"
for n in 01 02 03 04 05; do
    pv_file=$(ls pipeline-views/${n}_*.sql 2>/dev/null | head -1)
    sql_file=$(ls sql/${n}_*.sql 2>/dev/null | grep -v wc_inv | head -1)
    if [ -z "$pv_file" ]; then
        fail "pipeline-views/${n}_*.sql not found"
    elif [ -z "$sql_file" ]; then
        fail "sql/${n}_*.sql not found (excluding wc_inv)"
    elif diff -q "$pv_file" "$sql_file" > /dev/null 2>&1; then
        pass "$(basename $pv_file) == $(basename $sql_file)"
    else
        fail "$(basename $pv_file) DIFFERS from $(basename $sql_file) — run: cp $pv_file $sql_file"
    fi
done

# =============================================================================
# PHASE 2: VALIDATE CHECKS (CP-5 through CP-12)
# =============================================================================
echo ""
echo "--- Phase 2: VALIDATE ---"

# CP-5: ISNUMERIC ban — function calls only (parenthesis-anchored)
echo ""
echo "[CP-5] ISNUMERIC ban (function calls only)"
ISNUMERIC_HITS=$(grep -rn "ISNUMERIC(" pipeline-views/ analysis-views/ 2>/dev/null || true)
if [ -z "$ISNUMERIC_HITS" ]; then
    pass "No ISNUMERIC( function calls found"
else
    fail "ISNUMERIC( found — replace with TRY_CAST:"
    echo "$ISNUMERIC_HITS" | while read line; do echo "    $line"; done
fi

# CP-6: Config CTE required in every pipeline view
echo ""
echo "[CP-6] Config CTE present in all pipeline views"
for f in pipeline-views/*.sql; do
    if grep -q "Config AS" "$f"; then
        pass "$(basename $f)"
    else
        fail "$(basename $f) — missing Config AS CTE"
    fi
done

# CP-7: UNASSIGNED fallback for every PRIME_VNDR reference
echo ""
echo "[CP-7] UNASSIGNED fallback present where PRIME_VNDR is referenced"
for f in pipeline-views/*.sql analysis-views/*.sql; do
    if grep -q "PRIME_VNDR" "$f"; then
        if grep -q "'UNASSIGNED'" "$f"; then
            pass "$(basename $f)"
        else
            fail "$(basename $f) — references PRIME_VNDR but has no 'UNASSIGNED' fallback"
        fi
    fi
done

# CP-8: Documentation header (Purpose:) in every view
echo ""
echo "[CP-8] Documentation header (Purpose:) in all views"
for f in pipeline-views/*.sql analysis-views/*.sql; do
    if grep -q "Purpose:" "$f"; then
        pass "$(basename $f)"
    else
        fail "$(basename $f) — missing Purpose: documentation header"
    fi
done

# CP-9: Syntax check — no obvious parse errors (check for unmatched WITH/SELECT)
echo ""
echo "[CP-9] Basic syntax check (WITH...SELECT structure)"
for f in pipeline-views/*.sql analysis-views/*.sql; do
    if grep -q "WITH" "$f" && grep -q "SELECT" "$f"; then
        pass "$(basename $f) — WITH...SELECT keywords present"
    else
        warn "$(basename $f) — could not verify WITH...SELECT structure"
    fi
done

# CP-10: Row-count logic — Config values should mirror existing literals
# (Static check: verify Config CTE has numeric values, not empty)
echo ""
echo "[CP-10] Config CTE contains numeric threshold values"
for f in pipeline-views/*.sql; do
    # Use extended regex (-E) to match patterns like "7   AS Stale_Suppression_Days"
    # or "100 AS Lead_Days_Series_30" anywhere in the file
    if grep -qE "^\s+[0-9]+(\.[0-9]+)?\s+AS\s+[A-Za-z_]+" "$f"; then
        pass "$(basename $f) — Config CTE has numeric constants"
    else
        warn "$(basename $f) — Config CTE may be empty or non-numeric"
    fi
done

# CP-11: Unit test patterns — check for TRY_CAST usage (replaces ISNUMERIC)
echo ""
echo "[CP-11] TRY_CAST usage (ISNUMERIC replacement pattern)"
for f in pipeline-views/*.sql; do
    if grep -q "TRY_CAST" "$f"; then
        pass "$(basename $f) — uses TRY_CAST"
    else
        warn "$(basename $f) — no TRY_CAST found (may be OK if no numeric parsing needed)"
    fi
done

# CP-12: Integration check — downstream views reference correct upstream objects
echo ""
echo "[CP-12] Downstream view references"
# View 4 should reference ETB_WFQ_PIPE
if grep -q "ETB_WFQ_PIPE" pipeline-views/04_etb_pab_wfq_adj.sql; then
    pass "View 4 references ETB_WFQ_PIPE"
else
    fail "View 4 does NOT reference ETB_WFQ_PIPE"
fi
# View 5 should reference ETB_WFQ_PIPE
if grep -q "ETB_WFQ_PIPE" pipeline-views/05_etb_pab_supply_action.sql; then
    pass "View 5 references ETB_WFQ_PIPE"
else
    fail "View 5 does NOT reference ETB_WFQ_PIPE"
fi
# View 8 should reference ETB_PAB_SUPPLY_ACTION
if grep -q "ETB_PAB_SUPPLY_ACTION" analysis-views/08_etb_v_client_295_stockouts.sql; then
    pass "View 8 references ETB_PAB_SUPPLY_ACTION"
else
    fail "View 8 does NOT reference ETB_PAB_SUPPLY_ACTION"
fi

# =============================================================================
# PHASE 3: INTEGRATE CHECKS (CP-13)
# =============================================================================
echo ""
echo "--- Phase 3: INTEGRATE ---"

# CP-13: Git status — check for uncommitted changes
echo ""
echo "[CP-13] Git status"
if git diff --quiet && git diff --cached --quiet; then
    pass "Working tree clean — no uncommitted changes"
else
    CHANGED=$(git diff --name-only; git diff --cached --name-only)
    warn "Uncommitted changes present:"
    echo "$CHANGED" | while read f; do echo "    $f"; done
fi

# =============================================================================
# PHASE 4: DOCUMENT CHECKS (CP-14)
# =============================================================================
echo ""
echo "--- Phase 4: DOCUMENT ---"

# CP-14: Memory system files exist and are non-empty
echo ""
echo "[CP-14] Memory system files present and non-empty"
for f in decisions/decisions.jsonl decisions/experiences.jsonl; do
    if [ -f "$f" ] && [ -s "$f" ]; then
        LINE_COUNT=$(wc -l < "$f")
        pass "$f ($LINE_COUNT entries)"
    elif [ -f "$f" ]; then
        warn "$f exists but is empty"
    else
        fail "$f MISSING"
    fi
done

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo "============================================================"
echo " Validation Summary"
echo "============================================================"
echo "  PASS:  $PASS_COUNT"
echo "  WARN:  $WARN_COUNT"
echo "  FAIL:  $FAIL_COUNT"
echo ""

if [ "$FAIL_COUNT" -gt 0 ]; then
    echo "  STATUS: FAIL — $FAIL_COUNT check(s) failed. Do NOT commit."
    echo ""
    exit 1
elif [ "$WARN_COUNT" -gt 0 ]; then
    echo "  STATUS: PASS WITH WARNINGS — $WARN_COUNT warning(s). Review before committing."
    echo ""
    exit 0
else
    echo "  STATUS: PASS — All checks passed. Safe to commit."
    echo ""
    exit 0
fi
