#!/usr/bin/env python3
"""
Query the database to get expected results for test chains
"""
import asyncio
import asyncpg
from datetime import datetime, timedelta
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.config import settings

async def query_db():
    """Query database for expected results"""
    
    # Connect to database
    conn = await asyncpg.connect(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        database=settings.db_name
    )
    
    print("=" * 80)
    print("DATABASE CONTENTS - EXPECTED RESULTS FOR TEST CHAINS")
    print("=" * 80)
    
    try:
        # Check what tables exist
        print("\n### TABLES IN CORE SCHEMA ###")
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'core'
            ORDER BY table_name
        """)
        for t in tables:
            print(f"  - {t['table_name']}")
        
        # Count records in each table
        print("\n### RECORD COUNTS ###")
        for t in tables:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM core.{t['table_name']}")
            print(f"  {t['table_name']}: {count} records")
        
        # CHAIN 1: Loans with borrower and branch names
        print("\n" + "=" * 80)
        print("CHAIN 1: LIMIT + filter + sort (latest loans)")
        print("=" * 80)
        
        # Q1: "Show latest 5 loans with borrower name and branch name"
        print("\nQ1: Latest 5 loans with borrower name and branch name")
        loans = await conn.fetch("""
            SELECT 
                l.id,
                l.loan_number,
                b.full_name as borrower_name,
                br.branch_name,
                l.principal_amount,
                l.outstanding_balance,
                l.status,
                l.disbursement_date
            FROM core.loans l
            JOIN core.borrowers b ON l.borrower_id = b.id
            JOIN core.branches br ON l.branch_id = br.id
            ORDER BY l.disbursement_date DESC
            LIMIT 5
        """)
        for loan in loans:
            print(f"  {loan['loan_number']} | {loan['borrower_name']} | {loan['branch_name']} | {loan['status']} | {loan['disbursement_date']}")
        
        # Q2: Make it 15
        print(f"\nQ2: Make it 15 (should return {min(15, len(await conn.fetch('SELECT * FROM core.loans')))} loans)")
        
        # Q3: Only active loans
        active_count = await conn.fetchval("SELECT COUNT(*) FROM core.loans WHERE status = 'ACTIVE'")
        print(f"Q3: Only active loans (should return {min(15, active_count)} active loans)")
        
        # Q4: Sort by highest outstanding balance
        print("\nQ4: Sort by highest outstanding balance (top 15 active loans by balance)")
        top_balance = await conn.fetch("""
            SELECT loan_number, borrower_id, outstanding_balance, status
            FROM core.loans
            WHERE status = 'ACTIVE'
            ORDER BY outstanding_balance DESC
            LIMIT 15
        """)
        for i, loan in enumerate(top_balance[:5], 1):
            print(f"  {i}. {loan['loan_number']} - Balance: {loan['outstanding_balance']}")
        
        # CHAIN 2: Pronouns ("those / them / their")
        print("\n" + "=" * 80)
        print("CHAIN 2: Pronouns (those borrowers)")
        print("=" * 80)
        
        # Q1: List 5 borrowers who have loans
        print("\nQ1: List 5 borrowers who have loans")
        borrowers_with_loans = await conn.fetch("""
            SELECT DISTINCT b.id, b.full_name, b.borrower_code
            FROM core.borrowers b
            JOIN core.loans l ON b.id = l.borrower_id
            ORDER BY b.id
            LIMIT 5
        """)
        for br in borrowers_with_loans:
            print(f"  {br['borrower_code']} | {br['full_name']}")
        
        # Q2: For those borrowers, show their loan numbers and outstanding balances
        print("\nQ2: For THOSE borrowers, show their loans")
        borrower_ids = [b['id'] for b in borrowers_with_loans]
        their_loans = await conn.fetch(f"""
            SELECT l.loan_number, l.outstanding_balance, b.full_name
            FROM core.loans l
            JOIN core.borrowers b ON l.borrower_id = b.id
            WHERE b.id = ANY($1::bigint[])
        """, borrower_ids)
        for loan in their_loans[:10]:
            print(f"  {loan['loan_number']} | {loan['full_name']} | Balance: {loan['outstanding_balance']}")
        
        # Q3: Which branches are those loans from?
        print("\nQ3: Which branches are THOSE loans from?")
        branches_for_those_loans = await conn.fetch(f"""
            SELECT DISTINCT br.branch_name, br.branch_code
            FROM core.branches br
            JOIN core.loans l ON br.id = l.branch_id
            WHERE l.borrower_id = ANY($1::bigint[])
        """, borrower_ids)
        for branch in branches_for_those_loans:
            print(f"  {branch['branch_code']} | {branch['branch_name']}")
        
        # CHAIN 3: Keep grouping dimension (branch stays branch)
        print("\n" + "=" * 80)
        print("CHAIN 3: Keep grouping dimension (collections by branch)")
        print("=" * 80)
        
        # Q1: Show total collections by branch for last month
        one_month_ago = datetime.now() - timedelta(days=30)
        print(f"\nQ1: Total collections by branch (last 30 days from {one_month_ago.date()})")
        collections_by_branch = await conn.fetch("""
            SELECT 
                br.branch_name,
                br.branch_code,
                SUM(c.amount_collected) as total_collected,
                COUNT(c.id) as num_collections
            FROM core.collections c
            JOIN core.loans l ON c.loan_id = l.id
            JOIN core.branches br ON l.branch_id = br.id
            WHERE c.collection_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY br.id, br.branch_name, br.branch_code
            ORDER BY total_collected DESC
        """)
        for branch in collections_by_branch:
            print(f"  {branch['branch_code']} | {branch['branch_name']} | ${branch['total_collected']} | {branch['num_collections']} collections")
        
        # Q3: Show only top 3 branches
        print(f"\nQ3: Top 3 branches (same data, LIMIT 3)")
        for i, branch in enumerate(collections_by_branch[:3], 1):
            print(f"  {i}. {branch['branch_name']} | ${branch['total_collected']} | {branch['num_collections']} collections")
        
        # CHAIN 4: Metric swap (collections → outstanding balance)
        print("\n" + "=" * 80)
        print("CHAIN 4: Metric swap (keep branch grouping)")
        print("=" * 80)
        
        # Q1: Show total collections by branch
        print("\nQ1: Total collections by branch (all time)")
        all_collections = await conn.fetch("""
            SELECT 
                br.branch_name,
                SUM(c.amount_collected) as total_collected
            FROM core.collections c
            JOIN core.loans l ON c.loan_id = l.id
            JOIN core.branches br ON l.branch_id = br.id
            GROUP BY br.id, br.branch_name
            ORDER BY total_collected DESC
        """)
        for branch in all_collections:
            print(f"  {branch['branch_name']}: ${branch['total_collected']}")
        
        # Q2: Now show total outstanding balance by branch
        print("\nQ2: Switch to total outstanding balance by branch")
        balance_by_branch = await conn.fetch("""
            SELECT 
                br.branch_name,
                SUM(l.outstanding_balance) as total_balance
            FROM core.loans l
            JOIN core.branches br ON l.branch_id = br.id
            GROUP BY br.id, br.branch_name
            ORDER BY total_balance DESC
        """)
        for branch in balance_by_branch:
            print(f"  {branch['branch_name']}: ${branch['total_balance']}")
        
        # Q3: Only include active loans
        print("\nQ3: Only active loans")
        active_balance = await conn.fetch("""
            SELECT 
                br.branch_name,
                SUM(l.outstanding_balance) as total_balance
            FROM core.loans l
            JOIN core.branches br ON l.branch_id = br.id
            WHERE l.status = 'ACTIVE'
            GROUP BY br.id, br.branch_name
            ORDER BY total_balance DESC
        """)
        for branch in active_balance:
            print(f"  {branch['branch_name']}: ${branch['total_balance']}")
        
        # CHAIN 5: Time window changes
        print("\n" + "=" * 80)
        print("CHAIN 5: Time window follow-up")
        print("=" * 80)
        
        print("\nNote: Need to check if repayments table exists")
        has_repayments = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'core' AND table_name = 'repayments'
            )
        """)
        
        if has_repayments:
            print("Q1: Total repayments by branch (last month)")
            # Query would go here
        else:
            print("⚠️  No 'repayments' table - using collections instead")
            print("\nQ1: Collections by branch (last 30 days)")
            last_month_collections = await conn.fetch("""
                SELECT 
                    br.branch_name,
                    SUM(c.amount_collected) as total
                FROM core.collections c
                JOIN core.loans l ON c.loan_id = l.id
                JOIN core.branches br ON l.branch_id = br.id
                WHERE c.collection_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY br.id, br.branch_name
            """)
            for b in last_month_collections:
                print(f"  {b['branch_name']}: ${b['total']}")
        
        # CHAIN 6: Collections + field officers
        print("\n" + "=" * 80)
        print("CHAIN 6: Collections + field officers continuity")
        print("=" * 80)
        
        # Q1: Latest 10 collections with field officer and branch
        print("\nQ1: Latest 10 collections with field officer name and branch name")
        collections = await conn.fetch("""
            SELECT 
                c.id,
                c.amount_collected,
                c.collection_date,
                fo.full_name as officer_name,
                br.branch_name
            FROM core.collections c
            JOIN core.field_officers fo ON c.field_officer_id = fo.id
            JOIN core.loans l ON c.loan_id = l.id
            JOIN core.branches br ON l.branch_id = br.id
            ORDER BY c.collection_date DESC, c.id DESC
            LIMIT 10
        """)
        for col in collections:
            print(f"  {col['collection_date']} | ${col['amount_collected']} | {col['officer_name']} | {col['branch_name']}")
        
        # Q2: Only for Branch 001
        print("\nQ2: Only for branch with code 'BR001' or similar")
        first_branch = await conn.fetchrow("SELECT * FROM core.branches ORDER BY id LIMIT 1")
        if first_branch:
            print(f"  First branch: {first_branch['branch_code']} - {first_branch['branch_name']}")
            branch_collections = await conn.fetchval("""
                SELECT COUNT(*) FROM core.collections c
                JOIN core.loans l ON c.loan_id = l.id
                WHERE l.branch_id = $1
            """, first_branch['id'])
            print(f"  Collections count: {branch_collections}")
        
        # CHAIN 7: Loan documents
        print("\n" + "=" * 80)
        print("CHAIN 7: Loan documents context")
        print("=" * 80)
        
        # Q1: Show 10 loans that have documents
        print("\nQ1: 10 loans that have documents with loan number")
        loans_with_docs = await conn.fetch("""
            SELECT DISTINCT l.id, l.loan_number, COUNT(ld.id) as doc_count
            FROM core.loans l
            JOIN core.loan_documents ld ON l.id = ld.loan_id
            GROUP BY l.id, l.loan_number
            ORDER BY l.id
            LIMIT 10
        """)
        for loan in loans_with_docs:
            print(f"  {loan['loan_number']} ({loan['doc_count']} documents)")
        
        # Q2: For those loans, show document type and verification status
        print("\nQ2: For THOSE loans, show document details")
        if loans_with_docs:
            loan_ids = [l['id'] for l in loans_with_docs]
            docs = await conn.fetch(f"""
                SELECT l.loan_number, ld.document_type, ld.verification_status
                FROM core.loan_documents ld
                JOIN core.loans l ON ld.loan_id = l.id
                WHERE ld.loan_id = ANY($1::bigint[])
                ORDER BY l.loan_number, ld.document_type
            """, loan_ids)
            for doc in docs[:15]:
                print(f"  {doc['loan_number']} | {doc['document_type']} | Status: {doc['verification_status']}")
            
            # Q3: Only not VERIFIED
            print("\nQ3: Only documents NOT VERIFIED")
            unverified = await conn.fetchval(f"""
                SELECT COUNT(*) FROM core.loan_documents
                WHERE loan_id = ANY($1::bigint[]) 
                AND verification_status != 'VERIFIED'
            """, loan_ids)
            print(f"  Expected count: {unverified} unverified documents")
        
        # CHAIN 8: Context reset
        print("\n" + "=" * 80)
        print("CHAIN 8: Context reset test")
        print("=" * 80)
        
        print("\nQ1: Total collections by branch")
        print("  (Already shown above)")
        
        print("\nQ2: Most recently created borrowers (should NOT filter by branch)")
        recent_borrowers = await conn.fetch("""
            SELECT borrower_code, full_name, created_at
            FROM core.borrowers
            ORDER BY created_at DESC
            LIMIT 10
        """)
        for br in recent_borrowers:
            print(f"  {br['borrower_code']} | {br['full_name']} | {br['created_at']}")
        
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        total_loans = await conn.fetchval("SELECT COUNT(*) FROM core.loans")
        total_borrowers = await conn.fetchval("SELECT COUNT(*) FROM core.borrowers")
        total_branches = await conn.fetchval("SELECT COUNT(*) FROM core.branches")
        total_collections = await conn.fetchval("SELECT COUNT(*) FROM core.collections")
        print(f"""
        Total Loans: {total_loans}
        Total Borrowers: {total_borrowers}
        Total Branches: {total_branches}
        Total Collections: {total_collections}
        """)
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(query_db())
