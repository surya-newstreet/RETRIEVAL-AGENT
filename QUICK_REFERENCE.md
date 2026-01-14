# Quick Reference Guide - NL to SQL System

## 🎯 Project at a Glance

**Type**: Production-grade Natural Language to SQL converter  
**Stack**: Python 3.11, FastAPI, PostgreSQL, Groq LLM, Streamlit  
**Purpose**: Convert plain English questions into safe, validated SQL queries  

---

## 🏃 Quick Start

### 1. Setup Environment
```bash
# Create .env file with configuration
cp .env.example .env  # Configure database and LLM keys

# Install dependencies
pip install -r requirements.txt

# Initialize database
psql -h localhost -U postgres -d rag_agent_v2 -f scripts/init_db.sql
```

### 2. Run Locally
```bash
# Terminal 1: Start API
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000

# Terminal 2: Start UI
streamlit run ui/app.py

# Terminal 3: Optional - run scheduler separately
python scheduler/kb_refresh.py
```

### 3. Using Docker
```bash
docker-compose up --build
# API: http://localhost:8000
# UI: http://localhost:8501
# Docs: http://localhost:8000/docs
```

---

## 📊 Data Flow Map

```
INPUT: "How many users signed up last month?"
  ↓
[Context Resolver] - Check if referential question
  ↓
[LLM SQL Generator] - Generate SQL + check for clarification
  ↓
[SQL Validator] - 12-stage validation pipeline
  ↓
[Safe Executor] - Read-only execution with timeouts
  ↓
[Result Formatter] - Format with metadata
  ↓
OUTPUT: SQL + rows + confidence + provenance
```

---

## 🔑 Key Components Quick Reference

| Component | File | Purpose | Key Method |
|-----------|------|---------|------------|
| Context | `core/context_resolver.py` | Track conversation state | `get_context()` |
| LLM | `llm/groq_client.py` | Generate SQL from NL | `generate_structured_completion()` |
| Schema | `core/schema_introspector.py` | Extract metadata | `build_kb_schema()` |
| Joins | `core/join_graph_builder.py` | Validate FK paths | `validate_join_path()` |
| Validation | `core/sql_validator.py` | Multi-stage validation | `validate_sql()` |
| Executor | `core/safe_executor.py` | Execute safely | `execute_query()` |
| KB Compiler | `core/rules_compiler.py` | Merge artifacts | `compile_rules()` |
| Scheduler | `scheduler/kb_refresh.py` | Auto-refresh KB | `refresh_knowledge_base()` |
| DB Pool | `db/connection.py` | Manage connections | `acquire_query_connection()` |
| Logging | `observability/logger.py` | Structured logs | `get_logger()` |
| Metrics | `observability/metrics.py` | Collect metrics | `record_query()` |

---

## 🛣️ API Endpoints Summary

```
POST /api/v1/query
  Input:  { question: str, session_id?: str }
  Output: { sql: str, rows: [...], needs_clarification?: bool, ... }

POST /api/v1/clarify
  Input:  { original_question, clarification_answer, partial_intent, session_id }
  Output: { sql: str, rows: [...], ... }

GET /api/v1/health
  Output: { status, timestamp, db_metadata_pool, db_query_pool, kb_status }

GET /api/v1/kb-status
  Output: { last_refresh, next_refresh, status, table_count, error }

GET /api/v1/metrics
  Output: { queries, clarifications, validation, execution, kb, llm }
```

---

## 🔐 Validation Stages (12-Point Checklist)

1. ✅ **Parse**: SQL to AST
2. ✅ **Single Statement**: Only one statement allowed
3. ✅ **SELECT Only**: No DML/DDL/DCL
4. ✅ **Blocked Keywords**: No INSERT, DROP, ALTER, etc.
5. ✅ **Table Existence**: All tables must exist
6. ✅ **Column Existence**: Columns should exist (best effort)
7. ✅ **Schema Qualification**: Prefer `schema.table` format
8. ✅ **Blocked Functions**: No pg_sleep, dblink, file I/O
9. ✅ **Join Types**: No CROSS joins
10. ✅ **Join Paths**: Only FK-based joins allowed
11. ✅ **Join Depth**: Max 4 (hard cap 6)
12. ✅ **LIMIT Enforcement**: Auto-inject if missing

---

## 📁 Knowledge Base File Structure

```
kb/
├── kb_schema.json        ← Auto-generated from DB metadata
│   └── { tables: {...}, schema_name: "core", ... }
│
├── kb_semantic.json      ← Human-enriched, auto-appended
│   └── { tables: [...], version: "..." }
│
└── compiled_rules.json   ← Runtime source of truth
    └── { tables: {...}, join_paths: {...}, policies: {...} }
```

---

## 🔄 KB Refresh Lifecycle

```
STARTUP:
  SchemaIntrospector → kb_schema.json
  Merge Semantic → kb_semantic.json
  Compile Rules → compiled_rules.json

HOURLY (Configurable):
  [Repeat startup sequence]
  Validate artifacts
  Atomic file swap
  [On failure: Keep last known good]

STATUS:
  GET /api/v1/kb-status → { status, table_count, version, last_refresh }
```

---

## 🧪 Common Development Tasks

### Add a Blocked Function
**File**: `validation/blocked_patterns.py`
```python
BLOCKED_FUNCTIONS = [
    ...existing...,
    "your_new_blocked_function",
]
```

### Modify Query Policies
**File**: `core/config.py`
```python
class Settings(BaseSettings):
    max_limit: int = 5000        # Increase from 2000
    max_join_depth: int = 5      # Increase from 4
```

### Add Referential Pattern
**File**: `core/context_resolver.py`
```python
referential_patterns = [
    ...existing...,
    r'\byour_new_pattern\b',
]
```

### Check System Metrics
```bash
curl http://localhost:8000/api/v1/metrics | jq '.'
```

### View Logs
```bash
# Structured JSON logs to stdout
python -m uvicorn api.main:app --log-level info
```

---

## 📈 Performance Tuning

| Setting | Purpose | Default | Range |
|---------|---------|---------|-------|
| DEFAULT_LIMIT | Default LIMIT clause | 200 | 1-10000 |
| MAX_LIMIT | Hard max LIMIT | 2000 | - |
| STATEMENT_TIMEOUT_SECONDS | Query timeout | 30 | 5-300 |
| MAX_JOIN_DEPTH | Recommended join depth | 4 | 1-6 |
| HARD_CAP_JOIN_DEPTH | Absolute max | 6 | - |
| KB_REFRESH_INTERVAL_HOURS | Auto-refresh frequency | 1 | 1-24 |
| Metadata Pool Size | Min-max connections | 2-5 | - |
| Query Pool Size | Min-max connections | 5-20 | - |

---

## 🐛 Debugging Tips

### 1. **SQL Generation Issues**
```python
# Check prompt construction
# File: core/llm_sql_generator.py
# Method: _build_sql_prompt()

# Enable debug logging
LOG_LEVEL=DEBUG python -m uvicorn api.main:app
```

### 2. **Validation Failures**
```bash
# See validation pipeline details
# File: core/sql_validator.py
# Look for validation_failure logs

curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"question": "your test question"}'
```

### 3. **KB Refresh Issues**
```bash
# Check temp files (created during refresh)
ls -la kb/kb_*_temp.json

# View KB status
curl http://localhost:8000/api/v1/kb-status | jq '.'
```

### 4. **Connection Pool Issues**
```bash
# Check pool status via health endpoint
curl http://localhost:8000/api/v1/health | jq '.db_metadata_pool'
curl http://localhost:8000/api/v1/health | jq '.db_query_pool'
```

---

## 🔍 Correlation ID Tracing

Every request gets a unique correlation ID for end-to-end tracing:

```bash
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"question": "How many orders?"}' \
  | jq '.correlation_id'

# Use correlation_id in logs: grep "correlation_id"
```

---

## 📚 Key Files to Understand

### Priority 1 (Critical)
1. `api/routes.py` - Query execution flow
2. `core/sql_validator.py` - Validation pipeline
3. `core/safe_executor.py` - Safe execution
4. `scheduler/kb_refresh.py` - KB management

### Priority 2 (Important)
5. `core/llm_sql_generator.py` - SQL generation
6. `core/context_resolver.py` - Conversation context
7. `db/connection.py` - Database pools
8. `core/join_graph_builder.py` - Join validation

### Priority 3 (Reference)
9. `observability/logger.py` - Logging
10. `observability/metrics.py` - Metrics
11. `llm/groq_client.py` - LLM integration
12. `validation/ast_parser.py` - SQL parsing

---

## 🚨 Common Issues & Solutions

### Issue: "Knowledge base not initialized"
**Cause**: KB refresh failed on startup
**Solution**:
```bash
# Check if kb_schema.json exists and is valid
ls -la kb/
cat kb/kb_schema.json | jq '.' | head -20

# Manually refresh KB
curl -X POST http://localhost:8000/api/v1/kb-refresh
```

### Issue: "Table does not exist"
**Cause**: Schema not qualified or table missing
**Solution**:
```bash
# Verify table exists in database
psql -h localhost -U postgres -d rag_agent_v2 \
  -c "SELECT * FROM information_schema.tables WHERE table_schema='core';"

# Check compiled rules
cat kb/compiled_rules.json | jq '.tables | keys'
```

### Issue: "No FK path found"
**Cause**: Tables not connected via foreign keys
**Solution**:
```bash
# Add foreign key relationship or update KB
# Check join graph in compiled_rules.json
cat kb/compiled_rules.json | jq '.join_graph'
```

### Issue: "Query timeout"
**Cause**: Query running too long
**Solution**:
```bash
# Increase timeout (temporary)
STATEMENT_TIMEOUT_SECONDS=60 python -m uvicorn api.main:app

# Or suggest adding WHERE clause to user
```

---

## 📞 Endpoints for Monitoring

```bash
# System Health
curl http://localhost:8000/api/v1/health

# KB Status
curl http://localhost:8000/api/v1/kb-status

# System Metrics
curl http://localhost:8000/api/v1/metrics

# API Documentation
open http://localhost:8000/docs
```

---

## 🔧 Configuration Checklist

Before deployment, verify:
- [ ] `.env` file created with all required variables
- [ ] PostgreSQL running and accessible
- [ ] Groq API key configured
- [ ] Database schema created (`core` schema)
- [ ] Initial tables exist
- [ ] `kb/` directory writable
- [ ] Docker containers (if using) have correct env vars
- [ ] SSL/TLS certificates configured (for production)
- [ ] Logging level set appropriately
- [ ] Connection pool sizes tuned for workload

---

## 📝 Log Format

All logs are JSON-structured:
```json
{
  "event": "query_executed",
  "timestamp": "2024-01-05T10:30:45.123456Z",
  "correlation_id": "abc123",
  "session_id": "sess456",
  "question": "How many users?",
  "sql": "SELECT COUNT(*) FROM core.users",
  "execution_time_ms": 45.23,
  "row_count": 1,
  "success": true
}
```

---

## 🎓 Learning Path

1. **Understand Architecture**: Read `PROJECT_STRUCTURE.md`
2. **Trace Request**: Follow `api/routes.py` → `core/` → `db/`
3. **Study Validation**: Review `core/sql_validator.py` (12 stages)
4. **Learn Context**: Explore `core/context_resolver.py`
5. **Check Execution**: Review `core/safe_executor.py`
6. **Examine KB**: Look at `scheduler/kb_refresh.py`
7. **Run Examples**: Test with `/api/v1/query` endpoint

---

## 💡 Pro Tips

1. **Always use session_id** for conversation continuity
2. **Check `/api/v1/kb-status`** before investigating SQL errors
3. **Use correlation_id** in logs for debugging
4. **Monitor execution times** via `/api/v1/metrics`
5. **Keep KB refresh logs** for troubleshooting
6. **Test with simple queries first** (e.g., "count users")
7. **Increase STATEMENT_TIMEOUT_SECONDS** if working with large datasets
8. **Use schema.table format** in questions (e.g., "core.users")

---

## 📞 System Dependencies Minimal Check

```bash
# Python 3.11+
python --version

# PostgreSQL running
psql -V
psql -h localhost -U postgres -c "SELECT 1"

# Port availability
netstat -tuln | grep -E ":(5432|8000|8501)"
```

---

**Last Updated**: January 5, 2025  
**Version**: 1.0 Production Ready  
**Maintainers**: Project Team
