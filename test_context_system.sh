#!/bin/bash
# Comprehensive Context System Test Suite
# Tests all aspects of stateful analyst framework

set -e

API_URL="http://localhost:8000/api/v1/query"
SESSION_BASE="context-test-$(date +%s)"

echo "=================================================="
echo "COMPREHENSIVE CONTEXT SYSTEM TEST SUITE"
echo "=================================================="
echo ""

# Helper function to test a query
test_query() {
    local test_num=$1
    local question=$2
    local session=$3
    local expected_rows=$4
    local check_type=$5
    
    echo "---"
    echo "Test $test_num: $question"
    
    response=$(curl -s -X POST "$API_URL" \
        -H "Content-Type: application/json" \
        -d "{\"question\": \"$question\", \"session_id\": \"$session\"}")
    
    rows=$(echo "$response" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('row_count', 0))" 2>/dev/null || echo "ERROR")
    sql=$(echo "$response" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('sql', '')[:150])" 2>/dev/null || echo "")
    
    echo "→ Rows returned: $rows"
    
    if [ "$check_type" = "WITH" ]; then
        has_with=$(echo "$sql" | grep -i "WITH" || echo "")
        if [ -n "$has_with" ]; then
            echo "→ ✅ Contains WITH clause (context preserved)"
        else
            echo "→ ❌ Missing WITH clause (context NOT preserved)"
        fi
    fi
    
    if [ "$expected_rows" != "ANY" ] && [ "$rows" = "$expected_rows" ]; then
        echo "→ ✅ PASS"
    elif [ "$expected_rows" = "ANY" ]; then
        echo "→ ✅ PASS (any result acceptable)"
    else
        echo "→ ❌ FAIL (expected $expected_rows rows)"
    fi
    
    sleep 1
}

# ===================================
# TEST SUITE 1: Basic Continuation
# ===================================
echo "TEST SUITE 1: Basic Continuation"
echo "=================================="
SESSION1="${SESSION_BASE}-1"

test_query "1.1" "Show top 3 branches by collections" "$SESSION1" "3" ""
test_query "1.2" "What about their repayments?" "$SESSION1" "3" "WITH"
test_query "1.3" "And their outstanding balances?" "$SESSION1" "3" "WITH"

echo ""

# ===================================
# TEST SUITE 2: Refinement Commands
# ===================================
echo "TEST SUITE 2: Refinement Commands"
echo "=================================="
SESSION2="${SESSION_BASE}-2"

test_query "2.1" "Show top 5 branches by collections" "$SESSION2" "5" ""
test_query "2.2" "Make it 10" "$SESSION2" "10" ""
test_query "2.3" "Sort by name" "$SESSION2" "10" ""
test_query "2.4" "Make it 3" "$SESSION2" "3" ""

echo ""

# ===================================
# TEST SUITE 3: Pronoun Resolution
# ===================================
echo "TEST SUITE 3: Pronoun Resolution"
echo "================================="
SESSION3="${SESSION_BASE}-3"

test_query "3.1" "Show top 5 field officers by collections last 6 months" "$SESSION3" "5" ""
test_query "3.2" "What about their loan counts?" "$SESSION3" "5" "WITH"
test_query "3.3" "Show them sorted by loan count" "$SESSION3" "5" "WITH"

echo ""

# ===================================
# TEST SUITE 4: Unrelated Questions
# ===================================
echo "TEST SUITE 4: Unrelated Questions"
echo "=================================="
SESSION4="${SESSION_BASE}-4"

test_query "4.1" "Show top 3 branches by collections" "$SESSION4" "3" ""
test_query "4.2" "Show all borrowers" "$SESSION4" "ANY" ""
echo "→ Note: Should treat 4.2 as NEW query (different subject)"

echo ""

# ===================================
# TEST SUITE 5: Multi-Turn Scenario
# ===================================
echo "TEST SUITE 5: Multi-Turn Complex Scenario"
echo "=========================================="
SESSION5="${SESSION_BASE}-5"

test_query "5.1" "Show top 2 branches by collections" "$SESSION5" "2" ""
test_query "5.2" "Make it 4" "$SESSION5" "4" ""
test_query "5.3" "What about their repayments?" "$SESSION5" "4" "WITH"
test_query "5.4" "Sort by repayments desc" "$SESSION5" "4" ""
test_query "5.5" "Make it 2" "$SESSION5" "2" ""

echo ""

# ===================================
# TEST SUITE 6: Limit Preservation
# ===================================
echo "TEST SUITE 6: Limit Preservation Across Metrics"
echo "================================================"
SESSION6="${SESSION_BASE}-6"

test_query "6.1" "Show top 7 branches by collections" "$SESSION6" "7" ""
test_query "6.2" "What about their total repayments?" "$SESSION6" "7" "WITH"
test_query "6.3" "What about outstanding balances?" "$SESSION6" "7" ""

echo ""

# ===================================
# TEST SUITE 7: Time Window Handling
# ===================================
echo "TEST SUITE 7: Time Window Handling"
echo "==================================="
SESSION7="${SESSION_BASE}-7"

test_query "7.1" "Show branches by collections last 6 months" "$SESSION7" "ANY" ""
test_query "7.2" "What about repayments?" "$SESSION7" "ANY" ""
echo "→ Note: Should maintain 'last 6 months' time window"

echo ""

# ===================================
# TEST SUITE 8: Selective Context Use
# ===================================
echo "TEST SUITE 8: Selective Context (Only Recent Turns)"
echo "===================================================="
SESSION8="${SESSION_BASE}-8"

test_query "8.1" "Show branches" "$SESSION8" "ANY" ""
test_query "8.2" "Show officers" "$SESSION8" "ANY" ""
test_query "8.3" "Show borrowers" "$SESSION8" "ANY" ""
test_query "8.4" "Show top 5 branches by collections" "$SESSION8" "5" ""
test_query "8.5" "What about their repayments?" "$SESSION8" "5" "WITH"
echo "→ Note: Turn 8.5 should only use 8.4, not 8.1-8.3"

echo ""
echo "=================================================="
echo "TEST SUITE COMPLETE"
echo "=================================================="
echo ""
echo "Review results above to verify:"
echo "1. ✅ Basic continuation works (Suite 1)"
echo "2. ✅ Refinements preserve context (Suite 2)"
echo "3. ✅ Pronouns resolve correctly (Suite 3)"
echo "4. ✅ Unrelated questions start fresh (Suite 4)"
echo "5. ✅ Multi-turn scenarios work (Suite 5)"
echo "6. ✅ Limits preserved across metrics (Suite 6)"
echo "7. ✅ Time windows maintained (Suite 7)"
echo "8. ✅ Selective context use (Suite 8)"
