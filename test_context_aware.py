#!/usr/bin/env python3
"""
Comprehensive Test Suite for Context-Aware SQL Generation
Tests 10 scenarios with continuation, refinement, and pronoun resolution
"""
import requests
import json
from typing import Dict, List
from datetime import datetime

API_URL = "http://localhost:8000/api/v1/query"

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

def query(question: str, session_id: str) -> Dict:
    """Execute a query and return the response."""
    response = requests.post(API_URL, json={
        "question": question,
        "session_id": session_id
    })
    return response.json()

def check_sql_contains(sql: str, *patterns: str) -> bool:
    """Check if SQL contains all patterns (case-insensitive)."""
    sql_lower = sql.lower() if sql else ""
    return all(pattern.lower() in sql_lower for pattern in patterns)

def print_result(test_name: str, question: str, sql: str, expected: str, passed: bool, notes: str = ""):
    """Print formatted test result."""
    status = f"{Colors.GREEN}✓ PASS{Colors.RESET}" if passed else f"{Colors.RED}✗ FAIL{Colors.RESET}"
    print(f"\n{status} {test_name}")
    print(f"  Q: {Colors.BLUE}{question}{Colors.RESET}")
    print(f"  Expected: {expected}")
    if sql:
        print(f"  Got SQL: {sql[:150]}..." if len(sql) > 150 else f"  Got SQL: {sql}")
    else:
        print(f"  {Colors.RED}No SQL generated!{Colors.RESET}")
    if notes:
        print(f"  {Colors.YELLOW}Notes: {notes}{Colors.RESET}")

def test_set_1():
    """Test Set 1: LIMIT refinement"""
    print(f"\n{Colors.YELLOW}{'='*80}{Colors.RESET}")
    print(f"{Colors.YELLOW}Test Set 1: LIMIT Refinement{Colors.RESET}")
    print(f"{Colors.YELLOW}{'='*80}{Colors.RESET}")
    
    session = f"test1-{datetime.now().timestamp()}"
    
    # Q1
    r1 = query("Show top 2 branches by total lending", session)
    sql1 = r1.get('sql', '')
    passed1 = check_sql_contains(sql1, "LIMIT 2", "branches", "SUM")
    print_result("Q1", "Show top 2 branches by total lending", sql1, 
                 "LIMIT 2, aggregation on branches", passed1)
    
    # Q2
    r2 = query("make it 5", session)
    sql2 = r2.get('sql', '')
    passed2 = check_sql_contains(sql2, "LIMIT 5", "branches", "SUM") and "LIMIT 2" not in sql2
    print_result("Q2", "make it 5", sql2, 
                 "Same structure, LIMIT changed to 5", passed2,
                 "Should preserve aggregation, only change LIMIT")
    
    # Q3
    r3 = query("make it 10", session)
    sql3 = r3.get('sql', '')
    passed3 = check_sql_contains(sql3, "LIMIT 10", "branches", "SUM") and "LIMIT 5" not in sql3
    print_result("Q3", "make it 10", sql3,
                 "Same structure, LIMIT changed to 10", passed3)
    
    return all([passed1, passed2, passed3])

def test_set_2():
    """Test Set 2: ORDER refinement"""
    print(f"\n{Colors.YELLOW}{'='*80}{Colors.RESET}")
    print(f"{Colors.YELLOW}Test Set 2: ORDER Refinement{Colors.RESET}")
    print(f"{Colors.YELLOW}{'='*80}{Colors.RESET}")
    
    session = f"test2-{datetime.now().timestamp()}"
    
    # Q1
    r1 = query("Show top 5 branches by total lending", session)
    sql1 = r1.get('sql', '')
    passed1 = check_sql_contains(sql1, "LIMIT 5", "ORDER BY", "DESC")
    print_result("Q1", "Show top 5 branches by total lending", sql1,
                 "LIMIT 5, ORDER BY DESC", passed1)
    
    # Q2
    r2 = query("sort by branch_name asc", session)
    sql2 = r2.get('sql', '')
    passed2 = check_sql_contains(sql2, "branch_name", "ASC", "LIMIT 5")
    print_result("Q2", "sort by branch_name asc", sql2,
                 "ORDER BY branch_name ASC, keep LIMIT 5", passed2)
    
    # Q3
    r3 = query("sort by total_lending desc", session)
    sql3 = r3.get('sql', '')
    passed3 = check_sql_contains(sql3, "total_lending", "DESC", "LIMIT 5")
    print_result("Q3", "sort by total_lending desc", sql3,
                 "ORDER BY total_lending DESC, keep LIMIT 5", passed3)
    
    return all([passed1, passed2, passed3])

def test_set_3():
    """Test Set 3: Time window refinement"""
    print(f"\n{Colors.YELLOW}{'='*80}{Colors.RESET}")
    print(f"{Colors.YELLOW}Test Set 3: Time Window Refinement{Colors.RESET}")
    print(f"{Colors.YELLOW}{'='*80}{Colors.RESET}")
    
    session = f"test3-{datetime.now().timestamp()}"
    
    # Q1
    r1 = query("Show top 5 branches by total lending in June 2025", session)
    sql1 = r1.get('sql', '')
    passed1 = check_sql_contains(sql1, "2025-06", "branches", "LIMIT 5")
    print_result("Q1", "Show top 5 branches by total lending in June 2025", sql1,
                 "Filter for June 2025", passed1)
    
    # Q2
    r2 = query("what about May 2025?", session)
    sql2 = r2.get('sql', '')
    passed2 = check_sql_contains(sql2, "2025-05", "branches", "LIMIT 5")
    print_result("Q2", "what about May 2025?", sql2,
                 "Same query, filter changed to May 2025", passed2,
                 "Should preserve structure, only change month")
    
    # Q3
    r3 = query("what about January 2025?", session)
    sql3 = r3.get('sql', '')
    passed3 = check_sql_contains(sql3, "2025-01", "branches", "LIMIT 5")
    print_result("Q3", "what about January 2025?", sql3,
                 "Same query, filter changed to January 2025", passed3)
    
    return all([passed1, passed2, passed3])

def test_set_4():
    """Test Set 4: Switch metric, keep subject"""
    print(f"\n{Colors.YELLOW}{'='*80}{Colors.RESET}")
    print(f"{Colors.YELLOW}Test Set 4: Metric Change (Keep Subject){Colors.RESET}")
    print(f"{Colors.YELLOW}{'='*80}{Colors.RESET}")
    
    session = f"test4-{datetime.now().timestamp()}"
    
    # Q1
    r1 = query("Show top 5 branches by total lending", session)
    sql1 = r1.get('sql', '')
    passed1 = check_sql_contains(sql1, "branches", "SUM", "LIMIT 5")
    print_result("Q1", "Show top 5 branches by total lending", sql1,
                 "Branch aggregation, LIMIT 5", passed1)
    
    # Q2
    r2 = query("now by outstanding balance", session)
    sql2 = r2.get('sql', '')
    passed2 = check_sql_contains(sql2, "outstanding_balance", "branches", "LIMIT 5")
    print_result("Q2", "now by outstanding balance", sql2,
                 "Keep branches, change to outstanding_balance", passed2,
                 "Should change SUM(principal) to SUM(outstanding_balance)")
    
    # Q3
    r3 = query("make it 3", session)
    sql3 = r3.get('sql', '')
    passed3 = check_sql_contains(sql3, "LIMIT 3", "outstanding_balance", "branches")
    print_result("Q3", "make it 3", sql3,
                 "Keep metric, change LIMIT to 3", passed3)
    
    return all([passed1, passed2, passed3])

def test_set_5():
    """Test Set 5: Pronoun resolution"""
    print(f"\n{Colors.YELLOW}{'='*80}{Colors.RESET}")
    print(f"{Colors.YELLOW}Test Set 5: Pronoun Resolution ('those branches'){Colors.RESET}")
    print(f"{Colors.YELLOW}{'='*80}{Colors.RESET}")
    
    session = f"test5-{datetime.now().timestamp()}"
    
    # Q1
    r1 = query("Show top 3 branches by total lending", session)
    sql1 = r1.get('sql', '')
    passed1 = check_sql_contains(sql1, "LIMIT 3", "branches")
    print_result("Q1", "Show top 3 branches by total lending", sql1,
                 "Top 3 branches", passed1)
    
    # Q2
    r2 = query("for those branches, show total collections amount_collected", session)
    sql2 = r2.get('sql', '')
    # Should use CTE or subquery to preserve exact branch subset
    passed2 = check_sql_contains(sql2, "collections", "amount_collected") and \
              (check_sql_contains(sql2, "WITH") or check_sql_contains(sql2, "IN (SELECT"))
    print_result("Q2", "for those branches, show total collections amount_collected", sql2,
                 "Use CTE/subquery to preserve top 3 branches, sum collections", passed2,
                 "CRITICAL: Must preserve exact branch IDs from Q1")
    
    # Q3
    r3 = query("make it 5", session)
    sql3 = r3.get('sql', '')
    passed3 = "LIMIT 5" in sql3.lower() if sql3 else False
    print_result("Q3", "make it 5", sql3,
                 "Change LIMIT to 5", passed3)
    
    return all([passed1, passed2, passed3])

def test_set_6():
    """Test Set 6: Drill down"""
    print(f"\n{Colors.YELLOW}{'='*80}{Colors.RESET}")
    print(f"{Colors.YELLOW}Test Set 6: Drill Down ('show details'){Colors.RESET}")
    print(f"{Colors.YELLOW}{'='*80}{Colors.RESET}")
    
    session = f"test6-{datetime.now().timestamp()}"
    
    # Q1
    r1 = query("Show top 2 borrowers by total loan principal", session)
    sql1 = r1.get('sql', '')
    passed1 = check_sql_contains(sql1, "borrowers", "principal", "LIMIT 2")
    print_result("Q1", "Show top 2 borrowers by total loan principal", sql1,
                 "Top 2 borrowers by principal", passed1)
    
    # Q2
    r2 = query("show details", session)
    sql2 = r2.get('sql', '')
    passed2 = check_sql_contains(sql2, "borrower") and bool(sql2)
    print_result("Q2", "show details", sql2,
                 "Show borrower details (more columns)", passed2,
                 "Should expand columns, keep borrower scope")
    
    # Q3
    r3 = query("only include active loans", session)
    sql3 = r3.get('sql', '')
    passed3 = check_sql_contains(sql3, "status", "active") if sql3 else False
    print_result("Q3", "only include active loans", sql3,
                 "Add status filter to active", passed3)
    
    return all([passed1, passed2, passed3])

def test_set_7():
    """Test Set 7: 'that top one' reference"""
    print(f"\n{Colors.YELLOW}{'='*80}{Colors.RESET}")
    print(f"{Colors.YELLOW}Test Set 7: Top Entity Reference{Colors.RESET}")
    print(f"{Colors.YELLOW}{'='*80}{Colors.RESET}")
    
    session = f"test7-{datetime.now().timestamp()}"
    
    # Q1
    r1 = query("Show top 5 branches by total lending", session)
    sql1 = r1.get('sql', '')
    passed1 = check_sql_contains(sql1, "LIMIT 5", "branches")
    print_result("Q1", "Show top 5 branches by total lending", sql1,
                 "Top 5 branches", passed1)
    
    # Q2
    r2 = query("in that top one, show all loans", session)
    sql2 = r2.get('sql', '')
    passed2 = check_sql_contains(sql2, "loans") and bool(sql2)
    print_result("Q2", "in that top one, show all loans", sql2,
                 "Filter loans to #1 branch only", passed2,
                 "Should use CTE or subquery for top 1 branch")
    
    # Q3
    r3 = query("make it 20", session)
    sql3 = r3.get('sql', '')
    passed3 = "LIMIT 20" in sql3.lower() if sql3 else False
    print_result("Q3", "make it 20", sql3,
                 "Change LIMIT to 20", passed3)
    
    return all([passed1, passed2, passed3])

def test_set_8():
    """Test Set 8: Cross-table continuation"""
    print(f"\n{Colors.YELLOW}{'='*80}{Colors.RESET}")
    print(f"{Colors.YELLOW}Test Set 8: Cross-Table Continuation{Colors.RESET}")
    print(f"{Colors.YELLOW}{'='*80}{Colors.RESET}")
    
    session = f"test8-{datetime.now().timestamp()}"
    
    # Q1
    r1 = query("Show top 1 branch by total collections amount_collected", session)
    sql1 = r1.get('sql', '')
    passed1 = check_sql_contains(sql1, "branch", "collections", "LIMIT 1")
    print_result("Q1", "Show top 1 branch by total collections", sql1,
                 "Top 1 branch by collections", passed1)
    
    # Q2
    r2 = query("in that branch, show top 5 field officers by amount_collected", session)
    sql2 = r2.get('sql', '')
    passed2 = check_sql_contains(sql2, "field_officers", "collections") and bool(sql2)
    print_result("Q2", "in that branch, show top 5 field officers by amount_collected", sql2,
                 "Officers in that branch, LIMIT 5", passed2,
                 "Should preserve branch scope")
    
    # Q3
    r3 = query("what about last 30 days?", session)
    sql3 = r3.get('sql', '')
    passed3 = bool(sql3)  # Time filter added
    print_result("Q3", "what about last 30 days?", sql3,
                 "Add time filter for 30 days", passed3)
    
    return all([passed1, passed2, passed3])

def test_set_9():
    """Test Set 9: Borrower scope + repayments"""
    print(f"\n{Colors.YELLOW}{'='*80}{Colors.RESET}")
    print(f"{Colors.YELLOW}Test Set 9: Borrower Scope Continuation{Colors.RESET}")
    print(f"{Colors.YELLOW}{'='*80}{Colors.RESET}")
    
    session = f"test9-{datetime.now().timestamp()}"
    
    # Q1
    r1 = query("Show top 3 borrowers by total repayments amount", session)
    sql1 = r1.get('sql', '')
    passed1 = check_sql_contains(sql1, "borrowers", "repayments", "LIMIT 3")
    print_result("Q1", "Show top 3 borrowers by total repayments amount", sql1,
                 "Top 3 borrowers by repayments", passed1)
    
    # Q2
    r2 = query("for those borrowers, show their loans with outstanding_balance", session)
    sql2 = r2.get('sql', '')
    passed2 = check_sql_contains(sql2, "loans", "outstanding_balance", "borrower") and bool(sql2)
    print_result("Q2", "for those borrowers, show their loans with outstanding_balance", sql2,
                 "Loans for those 3 borrowers", passed2,
                 "Should preserve borrower subset with CTE/subquery")
    
    # Q3
    r3 = query("sort by outstanding_balance desc", session)
    sql3 = r3.get('sql', '')
    passed3 = check_sql_contains(sql3, "ORDER BY", "outstanding_balance", "DESC") if sql3 else False
    print_result("Q3", "sort by outstanding_balance desc", sql3,
                 "Add ORDER BY outstanding_balance DESC", passed3)
    
    return all([passed1, passed2, passed3])

def test_set_10():
    """Test Set 10: Explicit unrelated reset"""
    print(f"\n{Colors.YELLOW}{'='*80}{Colors.RESET}")
    print(f"{Colors.YELLOW}Test Set 10: Unrelated Query (Context Reset){Colors.RESET}")
    print(f"{Colors.YELLOW}{'='*80}{Colors.RESET}")
    
    session = f"test10-{datetime.now().timestamp()}"
    
    # Q1
    r1 = query("Show top 5 branches by total lending", session)
    sql1 = r1.get('sql', '')
    passed1 = check_sql_contains(sql1, "branches", "lending", "LIMIT 5")
    print_result("Q1", "Show top 5 branches by total lending", sql1,
                 "Top 5 branches", passed1)
    
    # Q2 - Completely unrelated
    r2 = query("List all borrowers", session)
    sql2 = r2.get('sql', '')
    # Should NOT filter by branches from Q1
    passed2 = check_sql_contains(sql2, "borrowers") and ("branch" not in sql2.lower())
    print_result("Q2", "List all borrowers", sql2,
                 "All borrowers (ignore branch context)", passed2,
                 "CRITICAL: Should NOT use context from Q1")
    
    return all([passed1, passed2])

def main():
    """Run all test sets."""
    print(f"\n{Colors.BLUE}{'='*80}{Colors.RESET}")
    print(f"{Colors.BLUE}Context-Aware SQL Generation - Comprehensive Test Suite{Colors.RESET}")
    print(f"{Colors.BLUE}{'='*80}{Colors.RESET}\n")
    
    results = {
        "Test Set 1 (LIMIT refinement)": test_set_1(),
        "Test Set 2 (ORDER refinement)": test_set_2(),
        "Test Set 3 (Time window)": test_set_3(),
        "Test Set 4 (Metric change)": test_set_4(),
        "Test Set 5 (Pronoun resolution)": test_set_5(),
        "Test Set 6 (Drill down)": test_set_6(),
        "Test Set 7 (Top entity reference)": test_set_7(),
        "Test Set 8 (Cross-table)": test_set_8(),
        "Test Set 9 (Borrower scope)": test_set_9(),
        "Test Set 10 (Unrelated reset)": test_set_10(),
    }
    
    # Summary
    print(f"\n{Colors.BLUE}{'='*80}{Colors.RESET}")
    print(f"{Colors.BLUE}TEST SUMMARY{Colors.RESET}")
    print(f"{Colors.BLUE}{'='*80}{Colors.RESET}\n")
    
    passed_count = sum(1 for passed in results.values() if passed)
    total_count = len(results)
    
    for test_name, passed in results.items():
        status = f"{Colors.GREEN}✓ PASS{Colors.RESET}" if passed else f"{Colors.RED}✗ FAIL{Colors.RESET}"
        print(f"{status} {test_name}")
    
    print(f"\n{Colors.BLUE}Overall: {passed_count}/{total_count} test sets passed{Colors.RESET}")
    
    if passed_count == total_count:
        print(f"{Colors.GREEN}\n🎉 ALL TESTS PASSED! Context-aware SQL generation is working perfectly.{Colors.RESET}\n")
    else:
        print(f"{Colors.YELLOW}\n⚠️  Some tests failed. Review the output above for details.{Colors.RESET}\n")
    
    return passed_count == total_count

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
