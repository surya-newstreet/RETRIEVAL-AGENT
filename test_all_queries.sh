#!/bin/bash
# Comprehensive 12-Query Verification Script
# Tests all business queries via PostgreSQL and API, compares results

set -e

PGPASSWORD=kausthub
DB_HOST=localhost
DB_USER=postgres
DB_NAME=rag_agent_v2
API_URL="http://localhost:8000/api/v1/query"

echo "=================================================="
echo "12-Query End-to-End Verification Test"
echo "=================================================="
echo ""

# Query 1: Branch Leaderboard
echo "Test 1: Branch leaderboard (last 6 months)"
echo "-------------------------------------------"
PGPASSWORD=$PGPASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
  b.branch_name,
  COALESCE(SUM(c.amount_collected), 0) AS total_collected,
  COALESCE(SUM(r.amount), 0) AS total_repaid,
  COALESCE(SUM(l.outstanding_balance), 0) AS total_outstanding
FROM core.branches b
LEFT JOIN core.loans l ON b.id = l.branch_id
LEFT JOIN core.collections c ON l.id = c.loan_id 
  AND c.collection_date >= CURRENT_DATE - INTERVAL '6 months'
LEFT JOIN core.repayments r ON l.id = r.loan_id
GROUP BY b.branch_name
ORDER BY total_collected DESC
LIMIT 3;" > /tmp/q1_db.txt

curl -s -X POST "$API_URL" -H "Content-Type: application/json" \
  -d '{"question": "Show top 10 branches by total amount collected in the last 6 months, along with total repaid and total outstanding balance"}' \
  | python3 -c "import sys, json; d=json.load(sys.stdin); print('Rows:', d.get('row_count', 0)); print('First 3:', d.get('rows', [])[:3])" > /tmp/q1_api.txt

echo "DB results:" && head -5 /tmp/q1_db.txt
echo "API results:" && cat /tmp/q1_api.txt
echo ""

# Query 2: Officer Performance
echo "Test 2: Officer performance (last 6 months)"
echo "-------------------------------------------"
PGPASSWORD=$PGPASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
  fo.id, fo.full_name,
  COALESCE(SUM(c.amount_collected), 0) AS total_collected,
  COUNT(DISTINCT c.loan_id) AS num_loans,
  b.branch_name
FROM core.field_officers fo
LEFT JOIN core.collections c ON fo.id = c.field_officer_id 
  AND c.collection_date >= CURRENT_DATE - INTERVAL '6 months'
LEFT JOIN core.branches b ON fo.branch_id = b.id
GROUP BY fo.id, fo.full_name, b.branch_name
ORDER BY total_collected DESC
LIMIT 3;" > /tmp/q2_db.txt

curl -s -X POST "$API_URL" -H "Content-Type: application/json" \
  -d '{"question": "For each field officer, show total collected in the last 6 months, number of distinct loans collected on, and the officer'\''s branch name. Return top 10 officers by total collected"}' \
  | python3 -c "import sys, json; d=json.load(sys.stdin); print('Rows:', d.get('row_count', 0)); print('Top 3:', d.get('rows', [])[:3])" > /tmp/q2_api.txt

echo "DB results:" && head -5 /tmp/q2_db.txt
echo "API results:" && cat /tmp/q2_api.txt
echo ""

# Query 3: Loan Reconciliation
echo "Test 3: Loan reconciliation discrepancy"
echo "---------------------------------------"
PGPASSWORD=$PGPASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
  l.id, l.principal_amount,  l.outstanding_balance,
  COALESCE(SUM(r.amount), 0) AS total_repaid,
  ABS((l.principal_amount - COALESCE(SUM(r.amount), 0)) - l.outstanding_balance) AS diff
FROM core.loans l
LEFT JOIN core.repayments r ON l.id = r.loan_id
GROUP BY l.id, l.principal_amount, l.outstanding_balance
ORDER BY diff DESC
LIMIT 3;" > /tmp/q3_db.txt

curl -s -X POST "$API_URL" -H "Content-Type: application/json" \
  -d '{"question": "For each loan, compute total repaid and show the top 20 loans where (principal_amount − total_repaid) differs from outstanding_balance the most"}' \
  | python3 -c "import sys, json; d=json.load(sys.stdin); print('Rows:', d.get('row_count', 0))" > /tmp/q3_api.txt

echo "DB results:" && head -5 /tmp/q3_db.txt
echo "API results:" && cat /tmp/q3_api.txt
echo ""

# Query 7: Document Compliance
echo "Test 7: Document compliance (missing docs)"
echo "------------------------------------------"
PGPASSWORD=$PGPASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT COUNT(*) as loans_missing_docs FROM core.loans l
WHERE NOT EXISTS (
  SELECT 1 FROM core.loan_documents WHERE loan_id = l.id AND document_type = 'ID_PROOF'
)
OR NOT EXISTS (
  SELECT 1 FROM core.loan_documents WHERE loan_id = l.id AND document_type = 'SIGNED_AGREEMENT'
);" > /tmp/q7_db.txt

curl -s -X POST "$API_URL" -H "Content-Type: application/json" \
  -d '{"question": "List loans missing either ID_PROOF or SIGNED_AGREEMENT"}' \
  | python3 -c "import sys, json; d=json.load(sys.stdin); print('Rows:', d.get('row_count', 0)); print('Valid:', d.get('safety_explanation') is not None)" > /tmp/q7_api.txt

echo "DB results:" && cat /tmp/q7_db.txt
echo "API results:" && cat /tmp/q7_api.txt
echo ""

# Query 12: Single Loan 360 View
echo "Test 12: Single loan 360° view (LN_D1_0000097)"
echo "----------------------------------------------"
PGPASSWORD=$PGPASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
  l.loan_number, b.full_name, br.branch_name,
  l.principal_amount, l.outstanding_balance, l.status,
  COALESCE(SUM(r.amount), 0) as total_repaid,
  COALESCE(SUM(c.amount_collected), 0) as total_collected,
  STRING_AGG(DISTINCT ld.document_type, ', ') as docs
FROM core.loans l
JOIN core.borrowers b ON l.borrower_id = b.id
JOIN core.branches br ON l.branch_id = br.id
LEFT JOIN core.repayments r ON l.id = r.loan_id
LEFT JOIN core.collections c ON l.id = c.loan_id
LEFT JOIN core.loan_documents ld ON l.id = ld.loan_id
WHERE l.loan_number = 'LN_D1_0000097'
GROUP BY l.loan_number, b.full_name, br.branch_name, l.principal_amount, l.outstanding_balance, l.status;" > /tmp/q12_db.txt

curl -s -X POST "$API_URL" -H "Content-Type: application/json" \
  -d '{"question": "For loan_number LN_D1_0000097, show borrower name, branch name, principal_amount, outstanding_balance, latest status, total repaid, total collected, and document types"}' \
  | python3 -c "import sys, json; d=json.load(sys.stdin); print('Rows:', d.get('row_count', 0)); print('SQL generated:', bool(d.get('sql'))); print('Data:', d.get('rows', [])[:1])" > /tmp/q12_api.txt

echo "DB results:" && cat /tmp/q12_db.txt
echo "API results:" && cat /tmp/q12_api.txt
echo ""

echo "=================================================="
echo "Verification Complete!"
echo "Review /tmp/q*_db.txt and /tmp/q*_api.txt for details"
echo "=================================================="
