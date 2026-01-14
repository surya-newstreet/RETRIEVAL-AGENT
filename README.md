# Production-Grade Natural Language to SQL System

A comprehensive, schema-grounded system that converts natural language questions into safe, validated SQL queries for PostgreSQL databases.

## Overview

This system is designed with **production-grade** requirements:
- ✅ **Schema Grounding**: Automatic metadata extraction, no hallucinated tables/columns
- ✅ **Defense-in-Depth Security**: AST-based validation + read-only execution
- ✅ **Auto-Adaptive**: KB refreshes hourly + on startup, handles schema changes
- ✅ **Conversation Context**: Last 3-5 turns with referential question detection
- ✅ **Clarification Loop**: Single-question clarification for incomplete intents
- ✅ **Observability**: Structured logging, metrics, health endpoints, correlation IDs

## Architecture

### System Components

```
┌─────────────┐
│   User UI   │ (Streamlit)
└──────┬──────┘
       │
┌──────▼──────────────────────────────────────────┐
│              FastAPI Application                │
├─────────────────────────────────────────────────┤
│  Context     │  LLM SQL      │   SQL           │
│  Resolver    │  Generator    │   Validator     │
│              │               │   (AST-based)   │
├──────────────┴───────────────┴─────────────────┤
│         Safe Executor (Read-only + Timeout)     │
└──────────────┬──────────────────────────────────┘
               │
    ┌──────────┴──────────┐
    │                     │
┌───▼────────┐ ┌─────────▼─────────┐
│ PostgreSQL │ │   Knowledge Base  │
│  (core.*)  │ │   (Auto-refresh)  │
└────────────┘ └───────────────────┘
```

### Knowledge Base Artifacts

1. **`kb_schema.json`** (AUTO-GENERATED)
   - Tables, columns, types, PKs, FKs, indexes
   - Extracted from `information_schema` + `pg_catalog`

2. **`kb_semantic.json`** (SEMI-AUTOMATIC)
   - Table purposes, aliases, PII columns
   - Default filters, recommended metrics/dimensions
   - Auto-appends new tables with safe defaults

3. **`compiled_rules.json`** (RUNTIME)
   - Merged schema + semantic
   - Join graph with shortest paths
   - Query policies (limits, timeouts, blocked patterns)

### KB Refresh Lifecycle

- **On Startup**: Full KB generation (blocking)
- **Hourly Scheduler**: Background refresh
- **Atomic Swap**: Temp files → validate → swap
- **Fallback**: Keeps "last known good" on failure

## Quick Start

### 1. Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Docker & Docker Compose (optional)

### 2. Environment Configuration

The `.env` file is already configured. Ensure your PostgreSQL database is running:

```bash
# Database should already be running at:
# Host: localhost
# Port: 5432
# Database: rag_agent_v2
# User: postgres
# Password: kausthub
```

### 3. Local Development Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Initialize database with sample schema
psql -h localhost -U postgres -d rag_agent_v2 -f scripts/init_db.sql

# Start API
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000

# In another terminal, start UI
streamlit run ui/app.py
```

### 4. Access the System

- **Streamlit UI**: http://localhost:8501
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/api/v1/health

## Usage Examples

### Example Questions

```
# Simple queries
"How many users do we have?"
"Show me all products"

# Joins
"List orders with customer names"
"Show products ordered by john_doe"

# Aggregations
"Total revenue by product category"
"Average order value per user"

# Time-based
"Orders placed in the last 30 days"
"New users this month"

# Clarification triggers
"Show sales"  → System asks: "Which time period?"
```

### API Usage

```python
import requests

# Query endpoint
response = requests.post(
    "http://localhost:8000/api/v1/query",
    json={"question": "How many users are active?"}
)

result = response.json()

if result.get('needs_clarification'):
    # Handle clarification
    clarify_response = requests.post(
        "http://localhost:8000/api/v1/clarify",
        json={
            "original_question": result['original_question'],
            "clarification_answer": "last month",
            "partial_intent": result['partial_intent'],
            "session_id": result['session_id']
        }
    )
else:
    # Process results
    print(result['sql'])
    print(result['rows'])
```

## Security Features

### Validation (Pre-Execution)

- ✅ AST parsing with `sqlglot`
- ✅ SELECT-only enforcement
- ✅ Single statement only
- ✅ Table/column existence checks
- ✅ Schema qualification verification
- ✅ Blocked functions: `pg_sleep`, `dblink`, file I/O, etc.
- ✅ Blocked keywords: INSERT, UPDATE, DELETE, DDL, DCL
- ✅ Join path validation (must use valid FKs)
- ✅ Join depth policies (max 4, hard cap 6)
- ✅ LIMIT enforcement (auto-inject default 200, max 2000)
- ✅ WHERE required for deep joins (5-6 tables)

### Execution (Defense-in-Depth)

> **Why Both?**  
> *Validation is logic; execution is power.* Defense-in-depth required because validators can have bugs, parser mismatches, or miss expensive-but-valid queries.

- ✅ Read-only transaction mode: `BEGIN TRANSACTION READ ONLY`
- ✅ Statement timeout (30s default, kills long queries)
- ✅ Connection pool limits
- ✅ Row limit already enforced by validator
- ✅ Sanitized error messages (no internal leakage)

## System Policies

### Query Policies (Default)

```json
{
  "default_limit": 200,
  "max_limit": 2000,
  "max_join_depth": 4,
  "hard_cap_join_depth": 6,
  "statement_timeout_seconds": 30,
  "require_where_for_deep_joins": true,
  "deep_join_threshold": 5
}
```

### KB Refresh Policy

- **Interval**: Every 1 hour
- **On Failure**: Keep last known good KB
- **Startup**: Blocking initialization (fails if no KB and refresh fails)

## Project Structure

```
FINAL PROJECT/
├── api/                    # FastAPI application
│   ├── main.py            # App entry + lifespan
│   ├── routes.py          # Endpoints
│   └── models.py          # Pydantic models
├── core/                   # Business logic
│   ├── config.py          # Settings
│   ├── schema_introspector.py
│   ├── join_graph_builder.py
│   ├── semantic_store.py
│   ├── rules_compiler.py
│   ├── context_resolver.py
│   ├── llm_sql_generator.py
│   ├── sql_validator.py
│   ├── safe_executor.py
│   └── result_formatter.py
├── db/                     # Database layer
│   └── connection.py      # Pool management
├── llm/                    # LLM providers
│   ├── base.py
│   └── groq_client.py
├── validation/             # SQL validation
│   ├── ast_parser.py
│   ├── blocked_patterns.py
│   └── join_validator.py
├── scheduler/              # Background tasks
│   └── kb_refresh.py
├── observability/          # Logging & metrics
│   ├── logger.py
│   └── metrics.py
├── ui/                     # Streamlit frontend
│   └── app.py
├── kb/                     # Knowledge base artifacts
│   ├── kb_schema.json     # (generated)
│   ├── kb_semantic.json   # (semi-automatic)
│   └── compiled_rules.json # (generated)
├── scripts/                # Utilities
│   └── init_db.sql
├── tests/                  # Test suite
├── requirements.txt
├── .env
└── README.md
```

## Observability

### Structured Logging

All logs output as JSON with correlation IDs:

```json
{
  "event": "query_executed",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
  "question": "How many users?",
  "sql": "SELECT COUNT(*) FROM core.users LIMIT 200",
  "execution_time_ms": 15.3,
  "row_count": 1,
  "timestamp": "2026-01-04T22:45:00.123Z"
}
```

### Metrics Endpoint

`GET /api/v1/metrics`

```json
{
  "queries": {
    "total": 150,
    "successful": 142,
    "failed": 8,
    "success_rate": 0.947
  },
  "clarifications": {
    "total": 23,
    "rate": 0.153
  },
  "execution": {
    "avg_time_ms": 87.5,
    "max_time_ms": 2342.1
  },
  "kb": {
    "last_refresh": "2026-01-04T21:00:00Z",
    "version": "2026-01-04T21:00:00.456Z",
    "refresh_count": 24
  }
}
```

### Health Endpoint

`GET /api/v1/health`

Returns:
- DB pool status (metadata + query)
- KB refresh status
- Overall system health

## Failure Scenarios & Mitigations

| Scenario | Mitigation |
|----------|-----------|
| Schema changes mid-refresh | Temp files + atomic swap, validate before swap |
| Refresh fails (permissions) | Keep last known good, alert, health shows staleness |
| LLM timeout/provider down | Configurable timeout, user-friendly error, logging |
| Validator parsing fails | Catch exceptions, return error with SQL shown |
| Valid but expensive query | `statement_timeout` kills it, LIMIT cap (2000) |
| Missing time window | Clarification: "Which time period?" |
| Join explosion | LIMIT + timeout enforcement |
| DB pool exhaustion | Pool limits, monitor metrics |
| UI clarification state desync | Session state in browser, no permanent loss |

## Extending the System

### Add New LLM Provider

1. Create `llm/new_provider.py`:
```python
from llm.base import BaseLLMProvider

class NewProvider(BaseLLMProvider):
    async def generate_completion(self, prompt, **kwargs):
        # Implementation
        pass
```

2. Update configuration to select provider

### Enrich Semantic KB

Edit `kb/kb_semantic.json`:

```json
{
  "tables": [
    {
      "table_name":  "users",
      "purpose": "Customer accounts and authentication",
      "aliases": ["users", "customers", "accounts"],
      "pii_columns": ["email", "phone"],
      "recommended_metrics": ["count", "active_count"]
    }
  ]
}
```

### Add Custom Validation Rules

Edit `validation/blocked_patterns.py` or extend `core/sql_validator.py`

## Testing

```bash
# Run unit tests
pytest tests/unit/ -v

# Run integration tests
pytest tests/integration/ -v

# Run with coverage
pytest --cov=core --cov=validation tests/
```

## Troubleshooting

### KB Not Initializing

```bash
# Check database connectivity
psql -h localhost -U postgres -d rag_agent_v2 -c "SELECT 1"

# Check API logs (the terminal where uvicorn is running)
# Or check log files if you're logging to file

# Manual KB refresh (if needed)
# You can restart the API server to trigger KB refresh
```

### LLM Errors

- Check `GROQ_API_KEY` in `.env`
- Verify API quota/limits
- Check model name matches Groq offerings

### Query Validation Failures

- Check logs for correlation ID
- Review validation errors in UI
- Simplify question or add more specificity

## Production Deployment

### Recommendations

1. **Database Roles**: Create separate roles
   ```sql
   CREATE ROLE rag_metadata_role WITH LOGIN PASSWORD 'xxx';
   GRANT SELECT ON information_schema.* TO rag_metadata_role;
   
   CREATE ROLE rag_query_role WITH LOGIN PASSWORD 'yyy';
   GRANT SELECT ON core.* TO rag_query_role;
   ```

2. **Resource Limits**:
   - Set appropriate connection pool sizes
   - Configure `statement_timeout` per workload
   - Monitor query execution time metrics

3. **Monitoring**:
   - Ingest JSON logs into ELK/Splunk
   - Set alerts on KB refresh failures
   - Monitor success rate and avg execution time

4. **Security**:
   - Use secrets manager for API keys
   - Enable TLS for PostgreSQL connections
   - Restrict CORS origins in production
   - Rate limit API endpoints

## License

This is a production-grade system built for internal analytics use.

## Support

For issues or questions:
1. Check logs with correlation ID
2. Review validation errors
3. Check health endpoint status
4. Review KB refresh status

---

**Built with**: FastAPI, Streamlit, PostgreSQL, Groq LLM, sqlglot, NetworkX, asyncpg
# RETRIEVAL-AGENT
