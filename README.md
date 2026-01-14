# Natural Language to SQL System

> **Production-Grade NL to SQL conversion system with schema grounding, defense-in-depth security, and auto-adaptive knowledge base management**

[![FastAPI](https://img.shields.io/badge/FastAPI-0.109.0-009688.svg?style=flat&logo=FastAPI)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791.svg?style=flat&logo=postgresql)](https://www.postgresql.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30.0-FF4B4B.svg?style=flat&logo=streamlit)](https://streamlit.io)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB.svg?style=flat&logo=python)](https://www.python.org)
[![LLM](https://img.shields.io/badge/LLM-Groq-000000.svg?style=flat)](https://groq.com)

## 🎯 Overview

A comprehensive, production-ready system that safely converts natural language questions into validated SQL queries for PostgreSQL databases. Built with enterprise-grade security, observability, and reliability.

**Transform this:**
```
"How many users signed up last month?"
```

**Into this:**
```sql
SELECT COUNT(*) 
FROM core.users 
WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
  AND created_at < DATE_TRUNC('month', CURRENT_DATE)
LIMIT 200
```

## ✨ Key Features

### 🛡️ Production-Grade Security
- ✅ **Zero Hallucinations** - Schema grounding prevents AI from inventing tables/columns
- ✅ **12-Stage Validation** - AST-based SQL parsing with comprehensive safety checks
- ✅ **Defense-in-Depth** - Read-only execution + statement timeouts + connection isolation
- ✅ **Blocked Patterns** - Prevents DDL/DML/DCL operations and dangerous functions

### 🧠 Intelligent SQL Generation
- ✅ **Conversation Context** - Remembers last 5 turns, understands referential questions
- ✅ **Clarification Loop** - Asks users for clarification on ambiguous queries
- ✅ **Schema-Grounded Prompts** - LLM receives actual database schema to prevent errors
- ✅ **Confidence Scoring** - Every SQL query includes confidence and provenance

### 🔄 Auto-Adaptive Knowledge Base
- ✅ **Hourly Refresh** - Automatically adapts to schema changes
- ✅ **Atomic Swaps** - Safe KB updates with rollback on failure
- ✅ **Semantic Enrichment** - Human-curated metadata for better SQL generation
- ✅ **Join Graph** - Pre-computed FK relationships for validated joins

### 📊 Enterprise Observability
- ✅ **Structured Logging** - JSON logs with correlation IDs for tracing
- ✅ **Performance Metrics** - Query times, success rates, clarification rates
- ✅ **Health Endpoints** - Real-time system status monitoring
- ✅ **Audit Trail** - Complete record of all queries and validations

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    User Interface                       │
│                   (Streamlit UI)                        │
└────────────────────┬────────────────────────────────────┘
                     │ HTTP POST
┌────────────────────▼────────────────────────────────────┐
│               FastAPI Application                       │
│  ┌────────────────────────────────────────────────┐    │
│  │ 1. Context Resolver                            │    │
│  │    └─ Detect referential questions             │    │
│  ├────────────────────────────────────────────────┤    │
│  │ 2. LLM SQL Generator (Groq)                    │    │
│  │    ├─ Schema-grounded prompting                │    │
│  │    └─ Detect incomplete intents                │    │
│  ├────────────────────────────────────────────────┤    │
│  │ 3. SQL Validator (12 stages)                   │    │
│  │    ├─ AST parsing (sqlglot)                    │    │
│  │    ├─ Table/column existence                   │    │
│  │    ├─ Blocked patterns check                   │    │
│  │    ├─ Join path validation                     │    │
│  │    └─ LIMIT enforcement                        │    │
│  ├────────────────────────────────────────────────┤    │
│  │ 4. Safe Executor                               │    │
│  │    ├─ Read-only transaction                    │    │
│  │    └─ Statement timeout (30s)                  │    │
│  ├────────────────────────────────────────────────┤    │
│  │ 5. Observability Layer                         │    │
│  │    ├─ Structured logging                       │    │
│  │    └─ Metrics collection                       │    │
│  └────────────────────────────────────────────────┘    │
└────────────────────┬────────────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
┌───────▼───┐ ┌──────▼────┐ ┌────▼──────┐
│PostgreSQL │ │ Knowledge │ │ Scheduler │
│ Database  │ │   Base    │ │ (hourly)  │
└───────────┘ └───────────┘ └───────────┘
```

### Knowledge Base Artifacts

| File | Generated | Purpose |
|------|-----------|---------|
| `kb_schema.json` | Auto | Tables, columns, types, PKs, FKs, indexes from `information_schema` |
| `kb_semantic.json` | Semi-Auto | Business metadata, aliases, PII columns, recommended metrics |
| `compiled_rules.json` | Runtime | Merged KB + join graph + query policies |

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Groq API key ([Get one free](https://console.groq.com))

### 1. Environment Setup

```bash
# Clone the repository
git clone https://github.com/surya-newstreet/RETRIEVAL-AGENT.git
cd RETRIEVAL-AGENT

# Create .env file
cat > .env << EOF
# Database
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=rag_agent_v2

# LLM
GROQ_API_KEY=your_groq_api_key
GROQ_MODEL=meta-llama/llama-3-70b-8192

# Query Policies
DEFAULT_LIMIT=200
MAX_LIMIT=2000
STATEMENT_TIMEOUT_SECONDS=30

# KB Refresh
KB_REFRESH_INTERVAL_HOURS=1
EOF

# Install dependencies
pip install -r requirements.txt
```

### 2. Initialize Database

```bash
# Create database
createdb rag_agent_v2

# Initialize sample schema
psql -d rag_agent_v2 -f scripts/init_db.sql
```

### 3. Start the System

```bash
# Terminal 1: Start API server
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000

# Terminal 2: Start UI (in another terminal)
streamlit run ui/app.py
```

### 4. Access the System

- **🎨 Streamlit UI**: http://localhost:8501
- **📚 API Docs**: http://localhost:8000/docs
- **❤️ Health Check**: http://localhost:8000/api/v1/health

## 💡 Usage Examples

### Natural Language Queries

```python
# Simple queries
"How many users do we have?"
"Show me all products"

# Queries with joins
"List orders with customer names"
"Show products ordered by john_doe"

# Aggregations
"Total revenue by product category"
"Average order value per user"

# Time-based queries
"Orders placed in the last 30 days"
"New users this month"

# Queries that trigger clarification
"Show sales" → System asks: "Which time period?"
"Top customers" → System asks: "By what metric?"
```

### API Usage

```python
import requests

# Execute query
response = requests.post(
    "http://localhost:8000/api/v1/query",
    json={
        "question": "How many active users are there?",
        "session_id": "session_123"  # Optional for context
    }
)

result = response.json()

# Handle clarification if needed
if result.get('needs_clarification'):
    print(f"Clarification needed: {result['clarification_question']}")
    
    # Provide clarification
    clarify_response = requests.post(
        "http://localhost:8000/api/v1/clarify",
        json={
            "original_question": result['original_question'],
            "clarification_answer": "last 30 days",
            "partial_intent": result['partial_intent'],
            "session_id": result['session_id']
        }
    )
    result = clarify_response.json()

# Use results
print(f"SQL: {result['sql']}")
print(f"Rows: {result['row_count']}")
print(f"Execution time: {result['execution_time_ms']}ms")
print(f"Results: {result['rows']}")
```

## 🔐 Security Features

### 12-Stage Validation Pipeline

| Stage | Check | Prevents |
|-------|-------|----------|
| 1 | AST Parsing | Syntax errors, malformed SQL |
| 2 | Single Statement | SQL injection via multiple statements |
| 3 | SELECT-only | Data modification (INSERT/UPDATE/DELETE) |
| 4 | Blocked Keywords | DDL (DROP/CREATE), DCL (GRANT/REVOKE) |
| 5 | Table Existence | Querying non-existent tables |
| 6 | Column Existence | Referencing non-existent columns |
| 7 | Schema Qualification | Accessing wrong schemas |
| 8 | Blocked Functions | `pg_sleep`, `dblink`, file I/O functions |
| 9 | Blocked Join Types | CROSS joins (cartesian products) |
| 10 | Join Path Validation | Non-FK joins, join explosions |
| 11 | Join Depth Limits | Overly complex joins (max 4, cap 6) |
| 12 | LIMIT Enforcement | Unbounded result sets |

### Execution Safety (Defense-in-Depth)

> **Why both validation AND execution controls?**  
> Validators can have bugs, miss edge cases, or allow expensive-but-valid queries. Multiple safety layers ensure comprehensive protection.

- ✅ `BEGIN TRANSACTION READ ONLY` - Database-level write prevention
- ✅ Statement timeout (30s default) - Kills runaway queries
- ✅ Connection pool isolation - Separate metadata and query pools
- ✅ Sanitized errors - No internal details leaked to users
- ✅ Row limits enforced - Auto-inject LIMIT if missing

### Blocked Patterns

**Functions:**
```
pg_sleep*, pg_read_*, pg_ls_dir, dblink*, lo_*, 
pg_terminate_backend, pg_cancel_backend
```

**Keywords:**
```
INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE,
GRANT, REVOKE, BEGIN, COMMIT, ROLLBACK
```

## 📂 Project Structure

```
.
├── api/                      # FastAPI application (454 lines)
│   ├── main.py              # App entry, lifespan management
│   ├── models.py            # Pydantic request/response models
│   └── routes.py            # API endpoints
│
├── core/                     # Business logic (1,894 lines)
│   ├── config.py            # Configuration management
│   ├── context_resolver.py  # Conversation context (5-turn window)
│   ├── llm_sql_generator.py # NL → SQL with Groq LLM
│   ├── sql_validator.py     # 12-stage validation pipeline
│   ├── safe_executor.py     # Read-only execution
│   ├── schema_introspector.py # DB metadata extraction
│   ├── join_graph_builder.py  # FK relationship graph
│   ├── rules_compiler.py    # KB compilation
│   ├── semantic_store.py    # Semantic metadata
│   └── result_formatter.py  # Result formatting
│
├── validation/               # SQL validation (516 lines)
│   ├── ast_parser.py        # SQL AST parsing (sqlglot)
│   ├── blocked_patterns.py  # Security rules
│   └── join_validator.py    # Join path validation
│
├── db/                       # Database layer (128 lines)
│   └── connection.py        # Connection pool management
│
├── llm/                      # LLM integration (150 lines)
│   ├── base.py              # Abstract LLM interface
│   └── groq_client.py       # Groq API client
│
├── observability/            # Monitoring (268 lines)
│   ├── logger.py            # Structured logging
│   └── metrics.py           # Metrics collection
│
├── scheduler/                # Background tasks (235 lines)
│   └── kb_refresh.py        # Hourly KB refresh
│
├── ui/                       # Streamlit UI (287 lines)
│   └── app.py               # Web interface
│
├── kb/                       # Knowledge base (auto-generated)
│   ├── kb_schema.json       # Database schema
│   ├── kb_semantic.json     # Semantic metadata
│   └── compiled_rules.json  # Runtime rules
│
├── tests/                    # Test suite
│   ├── unit/                # Unit tests
│   └── integration/         # Integration tests
│
├── scripts/                  # Database scripts
│   └── init_db.sql          # Sample schema
│
├── requirements.txt          # Python dependencies
├── .env                      # Configuration (gitignored)
├── docker-compose.yml        # Container orchestration
├── Dockerfile.api            # API container
└── Dockerfile.ui             # UI container
```

**Total:** ~3,600 lines of Python code across 25 modules

## ⚙️ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | localhost | PostgreSQL host |
| `DB_PORT` | 5432 | PostgreSQL port |
| `DB_USER` | postgres | Database user |
| `DB_PASSWORD` | - | Database password (required) |
| `DB_NAME` | rag_agent_v2 | Database name |
| `GROQ_API_KEY` | - | Groq API key (required) |
| `GROQ_MODEL` | meta-llama/llama-3-70b-8192 | LLM model |
| `DEFAULT_LIMIT` | 200 | Default row limit |
| `MAX_LIMIT` | 2000 | Maximum row limit |
| `MAX_JOIN_DEPTH` | 4 | Recommended join depth |
| `HARD_CAP_JOIN_DEPTH` | 6 | Maximum join depth |
| `STATEMENT_TIMEOUT_SECONDS` | 30 | Query timeout |
| `KB_REFRESH_INTERVAL_HOURS` | 1 | KB refresh frequency |

### Query Policies

```python
{
  "default_limit": 200,           # Auto-injected if no LIMIT
  "max_limit": 2000,              # Hard cap
  "max_join_depth": 4,            # Recommended max
  "hard_cap_join_depth": 6,       # Absolute max
  "statement_timeout_seconds": 30, # Query timeout
  "require_where_for_deep_joins": true,  # 5+ table joins
  "deep_join_threshold": 5        # When WHERE is required
}
```

## 📊 API Endpoints

### Query Execution

```http
POST /api/v1/query
Content-Type: application/json

{
  "question": "How many users signed up last month?",
  "session_id": "optional-session-id"
}
```

**Response (Success):**
```json
{
  "sql": "SELECT COUNT(*) FROM core.users WHERE ...",
  "rows": [{"count": 42}],
  "row_count": 1,
  "execution_time_ms": 15.3,
  "confidence": 0.95,
  "tables_used": ["core.users"],
  "correlation_id": "uuid-here",
  "session_id": "session-id"
}
```

**Response (Clarification):**
```json
{
  "needs_clarification": true,
  "clarification_question": "Which time period would you like to analyze?",
  "original_question": "Show sales",
  "partial_intent": {"domain": "sales"},
  "session_id": "session-id"
}
```

### Other Endpoints

- `POST /api/v1/clarify` - Handle clarification responses
- `GET /api/v1/health` - System health check
- `GET /api/v1/kb-status` - Knowledge base status
- `GET /api/v1/metrics` - Performance metrics

## 📈 Observability

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
  "timestamp": "2026-01-14T07:35:00.123Z"
}
```

### Metrics Endpoint

```bash
curl http://localhost:8000/api/v1/metrics | jq
```

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
    "last_refresh": "2026-01-14T06:00:00Z",
    "version": "2026-01-14T06:00:00.456Z",
    "refresh_count": 24
  }
}
```

## 🐛 Troubleshooting

### KB Not Initializing

```bash
# Check database connectivity
psql -h localhost -U postgres -d rag_agent_v2 -c "SELECT 1"

# Check API logs for errors
# Look for "kb_refresh_failed" events

# Manual verification
python -c "from scheduler.kb_refresh import kb_scheduler; import asyncio; asyncio.run(kb_scheduler.startup_kb_init())"
```

### LLM Errors

- Verify `GROQ_API_KEY` in `.env`
- Check API quota at https://console.groq.com
- Ensure model name is correct: `meta-llama/llama-3-70b-8192`
- Check logs for LLM timeout errors (default 10s)

### Query Validation Failures

1. Check logs for correlation ID
2. Review validation error message in UI/API response
3. Simplify question or add more specificity
4. Check if question references non-existent tables

### Connection Pool Issues

```bash
# Check pool health
curl http://localhost:8000/api/v1/health | jq '.db_pools'

# Should show:
# {
#   "metadata_pool": {"status": "healthy", "active": 1, "idle": 4},
#   "query_pool": {"status": "healthy", "active": 2, "idle": 18}
# }
```

## 🐳 Docker Deployment

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f api

# Stop all services
docker-compose down

# Rebuild after code changes
docker-compose up -d --build
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest --cov=core --cov=validation tests/

# Run specific test suite
pytest tests/integration/ -v
```

## 🔧 Extending the System

### Add New LLM Provider

```python
# llm/anthropic_client.py
from llm.base import BaseLLMProvider

class AnthropicProvider(BaseLLMProvider):
    async def generate_completion(self, prompt: str, **kwargs):
        # Implementation
        pass
```

### Customize Semantic KB

Edit `kb/kb_semantic.json`:

```json
{
  "tables": [
    {
      "table_name": "users",
      "purpose": "Customer accounts and authentication",
      "aliases": ["users", "customers", "accounts"],
      "pii_columns": ["email", "phone"],
      "recommended_metrics": ["count", "active_count"],
      "recommended_dimensions": ["created_at", "status"]
    }
  ]
}
```

### Add Custom Validation Rules

Extend `validation/blocked_patterns.py`:

```python
BLOCKED_FUNCTIONS = [
    # Existing functions...
    "your_custom_function",
]
```

## 📚 Additional Documentation

- **[00_START_HERE.md](./00_START_HERE.md)** - Comprehensive project overview
- **[PROJECT_STRUCTURE.md](./PROJECT_STRUCTURE.md)** - Detailed architecture documentation
- **[FILE_INVENTORY.md](./FILE_INVENTORY.md)** - Complete file listing
- **[QUICK_REFERENCE.md](./QUICK_REFERENCE.md)** - Developer quick reference

## 🤝 Production Deployment

### Pre-Production Checklist

- [ ] Configure strong database credentials
- [ ] Set up SSL/TLS for PostgreSQL connections
- [ ] Enable CORS restrictions for production origins
- [ ] Configure rate limiting on API endpoints
- [ ] Set up log aggregation (ELK, Splunk, etc.)
- [ ] Configure monitoring and alerts
- [ ] Test with production-like workload
- [ ] Set up database backups
- [ ] Review and tune query policies
- [ ] Set up secrets management for API keys

### Recommended Resource Limits

- **API Server**: 2-4 CPU cores, 4-8GB RAM
- **PostgreSQL**: Dedicated instance, 4+ CPU cores, 8GB+ RAM
- **Connection Pools**: 
  - Metadata: 2-5 connections
  - Query: 5-20 connections (scale with load)

## 📄 License

This is a production-grade system built for internal analytics use.

## 🙋 Support

For issues or questions:
1. Check logs with correlation ID
2. Review validation errors in API response
3. Check health endpoint: `GET /api/v1/health`
4. Review metrics: `GET /api/v1/metrics`
5. Consult documentation files

---

**Built with**: FastAPI · StreamlIt · PostgreSQL · Groq LLM · sqlglot · NetworkX · asyncpg

**Repository**: [github.com/surya-newstreet/RETRIEVAL-AGENT](https://github.com/surya-newstreet/RETRIEVAL-AGENT)
