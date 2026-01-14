# Production-Grade Natural Language to SQL System - Project Structure

## 📋 Executive Summary

A **production-grade Natural Language to SQL system** that converts plain English questions into safe, validated SQL queries for PostgreSQL databases. The system implements defense-in-depth security, auto-adaptive knowledge base management, conversation context tracking, and comprehensive observability.

**Key Characteristics:**
- ✅ Schema grounding with automatic metadata extraction (zero hallucinations)
- ✅ Defense-in-depth security (AST-based validation + read-only execution + timeouts)
- ✅ Auto-adaptive KB with hourly refresh and schema change handling
- ✅ Conversation context with referential question detection
- ✅ Clarification loop for incomplete intents
- ✅ Structured logging, metrics, health endpoints, correlation IDs

---

## 📂 Project Folder Structure

```
/home/agirekula/Videos/FINAL PROJECT/
├── api/                          # FastAPI application layer
├── core/                         # Core business logic
├── db/                           # Database connectivity
├── kb/                           # Knowledge base artifacts (auto-generated)
├── llm/                          # LLM provider integrations
├── observability/                # Logging and metrics
├── scheduler/                    # KB refresh scheduler
├── tests/                        # Test suite
├── ui/                           # Streamlit UI
├── validation/                   # SQL validation rules
├── scripts/                      # Database scripts
├── docker-compose.yml            # Multi-container orchestration
├── Dockerfile.api                # API container
├── Dockerfile.ui                 # UI container
├── requirements.txt              # Python dependencies
└── README.md                     # Project documentation
```

---

## 🏗️ System Architecture

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

---

## 📁 Detailed File Documentation

### 1. **API Layer** (`api/`)

#### `api/main.py` (59 lines)
- **Purpose**: FastAPI application entry point
- **Key Components**:
  - Lifespan manager for startup/shutdown hooks
  - CORS middleware configuration
  - Route registration
  - Root endpoint
- **Lifecycle Events**:
  - Startup: Initializes DB pools, KB, and scheduler
  - Shutdown: Gracefully closes pools and stops scheduler
- **Endpoints**: Registers routes from `api.routes`

#### `api/models.py` (76 lines)
- **Purpose**: Pydantic data models for request/response validation
- **Key Models**:
  - `QueryRequest`: NL question + session ID
  - `ClarificationResponse`: Response to clarification questions
  - `QueryResponse`: Complete response with SQL, rows, metadata
  - `HealthStatus`: System health check response
  - `KBStatus`: Knowledge base refresh status
  - `MetricsResponse`: System performance metrics
- **Features**: Type validation, optional fields, defaults

#### `api/routes.py` (319 lines)
- **Purpose**: API endpoints for query execution and system monitoring
- **Key Endpoints**:
  - `POST /api/v1/query` - Execute NL query (returns results or clarification)
  - `POST /api/v1/clarify` - Handle clarification responses
  - `GET /api/v1/health` - System health check
  - `GET /api/v1/kb-status` - Knowledge base status
  - `GET /api/v1/metrics` - System metrics
- **Query Flow**:
  1. Load compiled rules
  2. Get conversation context
  3. Generate SQL (detect if clarification needed)
  4. Validate SQL
  5. Execute safely
  6. Format results
  7. Log and record metrics
- **Error Handling**: HTTPException with proper status codes

---

### 2. **Core Business Logic** (`core/`)

#### `core/config.py` (41 lines)
- **Purpose**: Configuration management using Pydantic Settings
- **Settings Categories**:
  - **Database**: Host, port, user, password, database name
  - **LLM**: Groq API key, model, temperature, timeouts
  - **Query Policies**: Default/max limits, join depths, timeouts
  - **KB Refresh**: Interval hours, schema name
  - **API**: Host, port, CORS origins
  - **Observability**: Log level, metrics enablement
- **Source**: Loaded from `.env` file

#### `core/context_resolver.py` (137 lines)
- **Purpose**: Manages conversation context with sliding window
- **Key Classes**:
  - `Turn`: Single conversation turn (question, SQL, intent)
  - `Context`: Conversation context with last turns and summary
  - `ContextResolver`: Session manager
- **Features**:
  - Maintains last 5 turns per session (sliding window)
  - Detects referential questions (pronouns, "same but...", "what about")
  - Builds rolling summaries of recent context
  - Handles session cleanup
- **Referential Patterns Detected**:
  - "same but...", "what about...", "also", "too"
  - Time references: "last month", "this quarter"
  - Grouping references: "split by", "group by"

#### `core/schema_introspector.py` (335 lines)
- **Purpose**: Extracts PostgreSQL metadata from `information_schema` and `pg_catalog`
- **Key Classes**:
  - `ColumnMetadata`: Column name, type, nullability, precision, scale
  - `ForeignKeyMetadata`: FK relationships with schema qualification
  - `IndexMetadata`: Index information
  - `TableMetadata`: Complete table metadata
  - `SchemaIntrospector`: Async metadata extraction
- **Extraction Methods**:
  - `extract_tables()`: All tables in schema
  - `extract_columns()`: Column metadata per table
  - `extract_primary_keys()`: PK columns
  - `extract_foreign_keys()`: FK relationships
  - `extract_indexes()`: Index definitions
  - `extract_check_constraints()`: Check constraints
  - `build_kb_schema()`: Complete schema KB artifact
- **Output**: `kb_schema.json` with full database structure

#### `core/join_graph_builder.py` (250 lines)
- **Purpose**: Constructs FK relationship graph for join validation
- **Key Classes**:
  - `JoinPath`: Represents path between two tables
  - `JoinGraphBuilder`: FK graph builder using NetworkX
- **Key Methods**:
  - `build_fk_graph()`: Creates directed graph from FKs (bidirectional)
  - `compute_join_paths()`: Uses shortest path algorithm (max depth 4)
  - `validate_join_path()`: Validates if tables can be joined
  - `get_join_depth()`: Calculates join complexity
  - `get_join_sql_hint()`: Generates JOIN ON clause suggestions
- **Graph Features**:
  - Nodes: Schema-qualified table names
  - Edges: FK relationships with metadata
  - Algorithms: NetworkX shortest path

#### `core/llm_sql_generator.py` (316 lines)
- **Purpose**: Generates SQL from NL using Groq LLM with schema grounding
- **Key Classes**:
  - `ClarificationRequest`: Indicates clarification needed
  - `SQLGenerationResult`: Generated SQL with confidence + tables
  - `LLMSQLGenerator`: Main generator class
- **Key Methods**:
  - `generate_sql()`: Main entry point for SQL generation
  - `_detect_incomplete_intent()`: Detects if question needs clarification
  - `_build_sql_prompt()`: Constructs schema-grounded prompt
- **Features**:
  - Clarification detection for incomplete intents
  - Schema-grounded prompting (prevents hallucinations)
  - Context incorporation for referential questions
  - Structured JSON output parsing
  - Confidence scores for generated SQL
- **Clarification Triggers**:
  - Missing target table/domain
  - Unclear metric (which field to aggregate)
  - Ambiguous time window
  - Missing grouping dimension

#### `core/sql_validator.py` (229 lines)
- **Purpose**: Multi-stage SQL validation pipeline (defense-in-depth)
- **Key Class**:
  - `ValidationResult`: Validation outcome with errors/warnings
  - `SQLValidator`: Orchestrates all validation rules
- **Validation Pipeline** (12 stages):
  1. Parse to AST
  2. Single statement check
  3. SELECT-only enforcement
  4. Blocked keywords check
  5. Table existence verification
  6. Column existence check (best effort)
  7. Schema qualification check
  8. Blocked functions check
  9. Blocked join types check
  10. Join path validation
  11. Join depth policy enforcement
  12. LIMIT injection and WHERE clause requirement for deep joins
- **Safety Features**:
  - Prevents DDL/DML/DCL operations
  - Blocks dangerous functions (pg_sleep, dblink, file I/O)
  - Enforces schema qualification
  - Validates FK-based joins only
  - Limits join depth (soft: 4, hard: 6)

#### `core/safe_executor.py` (88 lines)
- **Purpose**: Executes validated SQL with defense-in-depth
- **Key Class**:
  - `ExecutionResult`: Row data, count, timing
  - `SafeExecutor`: Query executor
- **Safety Mechanisms**:
  - Read-only transaction mode (`BEGIN TRANSACTION READ ONLY`)
  - Statement-level timeouts (configurable, default 30s)
  - Connection-level timeouts
  - Row limits enforced by validator
  - Error sanitization (no internal details leaked)
- **Return Data**: Rows as list of dicts, execution time in ms
- **Justification**: Defense-in-depth because validators can have bugs or edge cases

#### `core/result_formatter.py` (54 lines)
- **Purpose**: Formats query results with metadata
- **Key Classes**:
  - `FormattedResult`: Complete result structure
  - `ResultFormatter`: Formatter class
- **Output Fields**:
  - SQL, rows, row count, execution time
  - Warnings, safety explanation, confidence
  - Provenance (tables used, KB version)
  - Correlation ID

#### `core/rules_compiler.py` (253 lines)
- **Purpose**: Compiles schema + semantic KB into runtime rules
- **Key Class**:
  - `RulesCompiler`: Compilation orchestrator
- **Key Methods**:
  - `load_kb_schema()`: Loads schema KB
  - `compile_rules()`: Merges schema + semantic + join graph
  - `save_compiled_rules()`: Persists compiled rules
  - `validate_compiled_rules()`: Validates structure
  - `atomic_swap()`: Safe file swap with fallback
- **Output**: `compiled_rules.json` with:
  - Version timestamp
  - Merged table metadata
  - Join graph and paths
  - Query policies (limits, timeouts, blocked patterns)
- **Blocked Functions/Keywords**: Security and safety list

#### `core/semantic_store.py` (191 lines)
- **Purpose**: Manages semantic KB with auto-append for new tables
- **Key Classes**:
  - `SemanticStore`: Semantic KB manager
- **Semantic Metadata Per Table**:
  - Purpose, aliases
  - PII columns, default filters
  - Recommended dimensions and metrics
  - Join policies (max depth, blocked paths)
  - Business rules
- **Features**:
  - Auto-appends new tables with defaults
  - Preserves human enrichments during merge
  - Uses inflection for alias generation
  - Fallback-safe loading

---

### 3. **Database Layer** (`db/`)

#### `db/connection.py` (128 lines)
- **Purpose**: Database connection pool management with two-role strategy
- **Key Class**:
  - `DatabaseManager`: Pool manager
- **Connection Pools**:
  - **Metadata Pool**: 2-5 connections for KB generation
    - Access to `information_schema` and `pg_catalog`
  - **Query Pool**: 5-20 connections for user query execution
    - Read-only SELECT access to `core.*` schema
    - Statement timeout enforcement
- **Key Methods**:
  - `init_metadata_pool()`: Initialize metadata extraction pool
  - `init_query_pool()`: Initialize query execution pool
  - `acquire_metadata_connection()`: Context manager for metadata queries
  - `acquire_query_connection()`: Context manager with timeout enforcement
  - `health_check()`: Check both pools' health
- **Features**:
  - Async context managers for safe connection handling
  - Session-level timeouts
  - Idle transaction timeouts
  - Application name tracking

---

### 4. **Knowledge Base** (`kb/`)

#### `kb/kb_schema.json` (AUTO-GENERATED)
- **Purpose**: Schema artifact generated from database metadata
- **Contents**:
  - All tables in schema
  - Columns with types, nullability, precision
  - Primary keys, foreign keys
  - Indexes, check constraints
- **Generated By**: `SchemaIntrospector` on startup and hourly refresh

#### `kb/kb_semantic.json` (SEMI-AUTOMATIC)
- **Purpose**: Human-enriched semantic metadata
- **Per Table**:
  - Purpose/description
  - Aliases and business names
  - PII columns
  - Default filters
  - Recommended metrics/dimensions
  - Join policies
  - Business rules
- **Management**: Auto-appended with safe defaults, human updates preserved

#### `kb/compiled_rules.json` (RUNTIME)
- **Purpose**: Merged schema + semantic, runtime source of truth
- **Contents**:
  - Version (timestamp)
  - Merged table metadata
  - Join graph with edges
  - Pre-computed join paths
  - Query policies
  - Blocked functions/keywords

---

### 5. **LLM Integration** (`llm/`)

#### `llm/base.py` (31 lines)
- **Purpose**: Abstract base class for LLM providers
- **Key Methods**:
  - `generate_completion()`: Text generation
  - `generate_structured_completion()`: JSON output

#### `llm/groq_client.py` (119 lines)
- **Purpose**: Groq LLM provider implementation
- **Key Class**:
  - `GroqProvider`: Groq API client
- **Features**:
  - Async completion generation
  - Structured JSON output with parsing
  - Handles markdown code blocks in responses
  - Cleans newlines in JSON string values
  - Metrics recording for LLM calls
  - Timeout enforcement (configurable, default 10s)
- **Model**: `meta-llama/llama-3-70b-8192` (configurable)

---

### 6. **Validation** (`validation/`)

#### `validation/ast_parser.py` (231 lines)
- **Purpose**: SQL AST parsing and analysis using sqlglot
- **Key Class**:
  - `SQLParser`: Parser and analyzer
- **Key Methods**:
  - `parse()`: Parse SQL to AST
  - `is_single_statement()`: Verify single statement
  - `is_select_only()`: Verify SELECT-only (no DML/DDL)
  - `extract_tables()`: Get table names
  - `extract_columns()`: Get columns by table
  - `extract_functions()`: Get function names
  - `extract_joins()`: Get join information
  - `get_join_depth()`: Calculate join depth
  - `has_where()`: Check for WHERE clause
  - `get_limit_clause()`: Extract LIMIT
- **Dialect**: PostgreSQL

#### `validation/blocked_patterns.py` (143 lines)
- **Purpose**: Blocked SQL functions and patterns for security
- **Blocked Functions** (safety/security):
  - Sleep functions: `pg_sleep*`
  - File I/O: `pg_read_*`, `pg_ls_dir`
  - External connections: `dblink*`
  - Large objects: `lo_*`
  - Administrative: `pg_terminate_backend`, `pg_cancel_backend`
- **Blocked Keywords** (DDL/DML/DCL):
  - Data: INSERT, UPDATE, DELETE, TRUNCATE
  - Schema: DROP, CREATE, ALTER, RENAME
  - Permissions: GRANT, REVOKE
  - Transactions: BEGIN, COMMIT, ROLLBACK
  - System: VACUUM, ANALYZE, CLUSTER
- **Blocked Join Types**: CROSS (cartesian product risk)

#### `validation/join_validator.py` (142 lines)
- **Purpose**: Join path validation against FK graph
- **Key Class**:
  - `JoinValidator`: Join validation orchestrator
- **Key Methods**:
  - `validate_join_path()`: Validate consecutive table joins
  - `check_join_depth()`: Enforce depth policies
  - `check_table_specific_policies()`: Check semantic policies
- **Policies**:
  - Max join depth: 4 (recommended)
  - Hard cap: 6
  - Deep joins (5+) require WHERE clause
  - Supports table-specific overrides

---

### 7. **Observability** (`observability/`)

#### `observability/logger.py` (126 lines)
- **Purpose**: Structured logging using structlog
- **Key Features**:
  - JSON output format
  - Correlation IDs for tracing
  - Contextual logging
  - Time stamping (ISO format)
  - Structured fields
- **Helper Functions**:
  - `get_logger()`: Get logger instance
  - `log_query_execution()`: Log query with metrics
  - `log_validation_failure()`: Log validation errors
  - `log_kb_refresh()`: Log KB refresh
  - `log_clarification_request()`: Log clarification questions

#### `observability/metrics.py` (142 lines)
- **Purpose**: In-memory metrics collection
- **Key Class**:
  - `MetricsCollector`: Collects and aggregates metrics
- **Metrics Tracked**:
  - Query execution: total, success, failure, times
  - Clarification: request count
  - Validation: failure count and reasons
  - KB refresh: count, failures, last version
  - LLM: request count, failures, total time
- **Methods**:
  - `record_query()`: Record query execution
  - `record_clarification()`: Record clarification
  - `record_validation_failure()`: Record validation error
  - `record_kb_refresh()`: Record KB refresh
  - `record_llm_request()`: Record LLM API call
  - `get_success_rate()`: Calculate success rate
  - `get_average_execution_time_ms()`: Average query time

---

### 8. **Scheduling** (`scheduler/`)

#### `scheduler/kb_refresh.py` (235 lines)
- **Purpose**: Knowledge base refresh scheduler
- **Key Class**:
  - `KBRefreshScheduler`: Refresh orchestrator
- **Key Methods**:
  - `startup_kb_init()`: Blocking KB initialization on startup
  - `refresh_knowledge_base()`: Main refresh pipeline
  - `start_scheduler()`: Start hourly scheduler
  - `stop_scheduler()`: Stop scheduler
  - `get_status()`: Get refresh status
- **Refresh Pipeline**:
  1. Extract schema metadata
  2. Merge semantic KB (auto-append new tables)
  3. Compile rules with join graph
  4. Validate all artifacts
  5. Atomic file swap
- **Safety Features**:
  - Uses temp files during refresh
  - Validates before swapping
  - Keeps "last known good" on failure
  - Scheduled refresh: hourly (configurable)
  - Status tracking: success/failed/in-progress
- **Output Files**:
  - `kb_schema.json`
  - `kb_semantic.json`
  - `compiled_rules.json`

---

### 9. **UI Layer** (`ui/`)

#### `ui/app.py` (287 lines)
- **Purpose**: Streamlit web interface
- **Key Features**:
  - Query input interface
  - System status sidebar (health, KB, metrics)
  - Clarification handling
  - Query history display
  - Results visualization
- **Components**:
  - Query input with placeholder
  - Execute button with loading state
  - SQL display with code highlighting
  - Results table with pagination
  - Query timing and row count display
- **Session Management**:
  - Maintains `session_id` for conversation context
  - Tracks clarification state
  - Keeps query history (last 5 queries)
- **API Integration**:
  - `/api/v1/query` - Execute queries
  - `/api/v1/clarify` - Handle clarifications
  - `/api/v1/health` - System status
  - `/api/v1/kb-status` - KB status
  - `/api/v1/metrics` - System metrics

---

### 10. **Deployment & Configuration**

#### `docker-compose.yml`
- **Services**:
  - `postgres`: PostgreSQL 15 database
  - `api`: FastAPI application
  - `ui`: Streamlit UI
- **Networking**: Services communicate by name
- **Volumes**: `postgres_data` for persistence, `kb/` shared
- **Health Checks**: All services monitored
- **Env File**: `.env` for configuration

#### `Dockerfile.api`
- Base: `python:3.11-slim`
- Installs: System dependencies (gcc)
- Copies: Application code and requirements
- Ports: 8000
- Command: `uvicorn api.main:app`

#### `Dockerfile.ui`
- Base: `python:3.11-slim`
- Installs: streamlit, pandas, requests
- Ports: 8501
- Command: `streamlit run ui/app.py`

#### `requirements.txt`
**Core Framework**: fastapi, uvicorn, pydantic
**Database**: asyncpg, psycopg2-binary
**SQL**: sqlglot
**Graphs**: networkx, inflection
**LLM**: groq, openai
**Async**: apscheduler, aiofiles
**UI**: streamlit, pandas
**Logging**: structlog, python-json-logger
**Testing**: pytest, pytest-asyncio, httpx
**Dev**: black, ruff, mypy

#### `scripts/init_db.sql`
- Creates `core` schema
- Sample tables: users, products, orders, order_items
- Demonstrates: PK, FK, indexes
- Auto-initialized on container startup

---

## 🔄 Request Processing Flow

### 1. **Query Execution Pipeline**
```
User Question
    ↓
Load Compiled Rules (KB)
    ↓
Get Conversation Context (if referential)
    ↓
Detect Intent Completeness → [Clarification if incomplete]
    ↓
Build Schema-Grounded Prompt
    ↓
Generate SQL (Groq LLM)
    ↓
Validate SQL (12-stage pipeline)
    ↓
Execute Safely (Read-only, timeouts)
    ↓
Format Results (with metadata)
    ↓
Record Metrics & Logs
    ↓
Return to User
```

### 2. **Clarification Loop**
```
Incomplete Intent Detected
    ↓
Send Clarification Question to User
    ↓
User Provides Clarification Answer
    ↓
Merge Clarification with Original Question
    ↓
Generate SQL (with clarification context)
    ↓
Validate & Execute
    ↓
Return Results
```

### 3. **Knowledge Base Refresh**
```
Startup or Hourly Trigger
    ↓
Extract Schema from DB
    ↓
Merge Semantic KB (auto-append)
    ↓
Compile Rules (schema + semantic + joins)
    ↓
Validate All Artifacts
    ↓
Atomic Swap (temp → live)
    ↓
Update Status & Metrics
    ↓
[On Failure: Keep Last Known Good]
```

---

## 🔐 Security Architecture

### Defense-in-Depth Strategy

**Layer 1: Validation**
- AST-based SQL parsing
- Syntax verification
- Table/column existence checks

**Layer 2: Policies**
- Blocked functions list
- Blocked keywords (DDL/DML/DCL)
- Schema qualification requirement
- Join depth limits
- FK path requirement

**Layer 3: Execution**
- Read-only transaction mode
- Statement-level timeouts
- Connection timeouts
- Row limits

**Layer 4: Error Handling**
- Sanitized error messages
- No internal details leaked
- User-friendly explanations

### Blocked Operations
- **DDL**: DROP, CREATE, ALTER (schema changes)
- **DML**: INSERT, UPDATE, DELETE (data modification)
- **DCL**: GRANT, REVOKE (permission changes)
- **Functions**: pg_sleep, dblink, file I/O, system operations
- **Joins**: CROSS (cartesian products)

---

## 📊 Observability & Monitoring

### Metrics Collected
- Query counts (total, success, failure)
- Execution times (min, max, avg)
- Clarification request rate
- Validation failure reasons
- KB refresh status and versions
- LLM request latency
- Success rates and error rates

### Health Checks
- Database pool status (metadata & query)
- Connection counts (active & idle)
- KB readiness status
- System timestamp

### Logging
- Structured JSON format
- Correlation IDs for tracing
- Contextual fields
- Severity levels (info, warning, error)

---

## 🚀 Key Features & Patterns

### 1. **Schema Grounding**
- No hallucinated tables/columns
- FK relationships only
- Schema-qualified names
- Automatic metadata extraction

### 2. **Conversation Context**
- Last 5 turns maintained
- Referential question detection
- Rolling context summary
- Session-based management

### 3. **Clarification System**
- Intent completeness detection
- Single-question clarification
- Partial intent tracking
- Merge with clarification answer

### 4. **Async Architecture**
- FastAPI with async/await
- Async database pools
- Async file I/O
- Async LLM calls

### 5. **Atomic KB Refresh**
- Temp file generation
- Pre-swap validation
- Atomic file swap
- "Last known good" fallback

### 6. **Two-Role Database Strategy**
- Metadata pool for KB extraction
- Query pool for user queries
- Separate connection limits
- Distinct timeout policies

---

## 📝 Configuration Reference

### Environment Variables (from `.env`)
```
# Database
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=kausthub
DB_NAME=rag_agent_v2

# LLM
GROQ_API_KEY=your_groq_key
GROQ_MODEL=meta-llama/llama-3-70b-8192

# Query Policies
DEFAULT_LIMIT=200
MAX_LIMIT=2000
MAX_JOIN_DEPTH=4
HARD_CAP_JOIN_DEPTH=6
STATEMENT_TIMEOUT_SECONDS=30

# KB Refresh
KB_REFRESH_INTERVAL_HOURS=1

# API
API_HOST=0.0.0.0
API_PORT=8000

# Observability
LOG_LEVEL=INFO
```

---

## 🧪 Testing Structure

```
tests/
├── fixtures/          # Test data and factories
├── integration/       # End-to-end tests
└── unit/              # Component tests
```

---

## 🔗 Key Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| fastapi | 0.109.0 | Web framework |
| asyncpg | 0.29.0 | Async PostgreSQL |
| sqlglot | 20.11.0 | SQL parsing/AST |
| networkx | 3.2.1 | Graph algorithms |
| groq | 0.4.1 | LLM provider |
| streamlit | 1.30.0 | UI framework |
| structlog | 24.1.0 | Structured logging |
| apscheduler | 3.10.4 | Task scheduling |

---

## 📌 Design Decisions & Rationale

### 1. **Why Defense-in-Depth?**
Validators can have bugs. Even if SQL passes validation, multiple runtime safeguards prevent resource exhaustion and ensure safety.

### 2. **Why Two Database Pools?**
- Metadata pool: Read `information_schema` with relaxed timeouts
- Query pool: Execute user queries with strict timeouts and limits

### 3. **Why Async Architecture?**
Better resource utilization under concurrent load. Connection pooling is more efficient with async/await.

### 4. **Why Atomic File Swaps?**
Ensures KB consistency. If refresh fails, system continues with last known good KB instead of corrupted state.

### 5. **Why Clarification Loop?**
Incomplete questions cause invalid SQL. Clarification is better than hallucinations.

### 6. **Why Conversation Context?**
Users ask follow-up questions ("same but for last month"). Context helps understand referential queries.

---

## 🎯 Summary

This is a **production-ready NL-to-SQL system** designed with:
- ✅ **Zero Hallucinations**: Schema grounding, FK-based joins only
- ✅ **Maximum Safety**: Defense-in-depth validation + read-only execution
- ✅ **High Reliability**: Atomic KB refresh with fallback
- ✅ **Rich Context**: Conversation management with referential detection
- ✅ **Full Observability**: Structured logging, metrics, health checks
- ✅ **Scalability**: Async architecture, connection pooling, resource limits

The system is ready for deployment in production environments with appropriate configuration and monitoring.
