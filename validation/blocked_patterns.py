"""
Blocked patterns and functions for SQL validation.
These patterns are blocked for security and safety.
"""

import re  # NEW: for robust keyword scanning after sanitization

# Blocked SQL functions (security risks or expensive operations)
BLOCKED_FUNCTIONS = [
    # Sleep and delay functions
    "pg_sleep",
    "pg_sleep_for",
    "pg_sleep_until",

    # File system access
    "pg_read_file",
    "pg_read_binary_file",
    "pg_ls_dir",

    # External connections
    "dblink",
    "dblink_exec",
    "dblink_connect",
    "dblink_open",

    # Large object functions
    "lo_import",
    "lo_export",
    "lo_create",
    "lo_unlink",

    # Administrative functions
    "pg_terminate_backend",
    "pg_cancel_backend",
    "pg_reload_conf",

    # System catalog modification
    "pg_advisory_lock",
    "pg_try_advisory_lock",

    # Potentially expensive (optional - uncomment if needed)
    # "generate_series",  # Can create large datasets
]

# Blocked SQL keywords and patterns (DDL, DML, DCL)
BLOCKED_KEYWORDS = [
    # Data modification
    "INSERT",
    "UPDATE",
    "DELETE",
    "TRUNCATE",

    # Schema modification
    "DROP",
    "CREATE",
    "ALTER",
    "RENAME",

    # Permissions
    "GRANT",
    "REVOKE",

    # Transaction control (can interfere with connection pool)
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",

    # System operations
    "VACUUM",
    "ANALYZE",
    "CLUSTER",
    "REINDEX",

    # Procedural
    "DO",
    "CALL",

    # Utility commands
    "COPY",
    "LISTEN",
    "NOTIFY",
    "UNLISTEN",

    # Explain can be expensive with ANALYZE
    # "EXPLAIN",  # Optional
]

# Blocked join types
BLOCKED_JOIN_TYPES = [
    "CROSS",  # Can create cartesian products
]


def check_blocked_functions(functions: set) -> list:
    """
    Check if any blocked functions are used.
    Returns list of blocked functions found.
    """
    blocked_found = []
    functions_lower = {f.lower() for f in functions}

    for blocked in BLOCKED_FUNCTIONS:
        if blocked.lower() in functions_lower:
            blocked_found.append(blocked)

    return blocked_found


def _strip_sql_literals_and_comments(sql: str) -> str:
    """
    Remove string literals and comments so keyword scanning:
    - doesn't false-positive on SELECT 'DELETE'
    - is harder to bypass via comments
    """
    if not sql:
        return ""

    # Remove /* ... */ comments
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)

    # Remove -- ... end-of-line comments
    sql = re.sub(r"--[^\n]*", " ", sql)

    # Remove single-quoted strings (handles escaped '' inside strings)
    sql = re.sub(r"'(?:''|[^'])*'", " ", sql)

    # Remove double-quoted identifiers (Postgres allows these)
    sql = re.sub(r'"(?:[^"]|"")*"', " ", sql)

    # Normalize whitespace
    sql = re.sub(r"\s+", " ", sql).strip()

    return sql


def check_blocked_keywords(sql: str) -> list:
    """
    Check if any blocked keywords appear in SQL.

    IMPORTANT FIX:
    - Strip literals/comments first to avoid false positives and bypasses.
    - Use word-boundary regex so it matches across whitespace reliably.
    Returns list of blocked keywords found.
    """
    blocked_found = []
    cleaned = _strip_sql_literals_and_comments(sql)
    if not cleaned:
        return blocked_found

    for keyword in BLOCKED_KEYWORDS:
        # Word boundary match, case-insensitive
        if re.search(rf"\b{re.escape(keyword)}\b", cleaned, flags=re.IGNORECASE):
            blocked_found.append(keyword)

    return blocked_found


def check_blocked_join_types(joins: list) -> list:
    """
    Check if any blocked join types are used.
    Returns list of blocked join types found.
    """
    blocked_found = []

    for join in joins:
        join_type = join.get("type", "")
        join_type = str(join_type).upper().strip()

        # IMPORTANT FIX: be tolerant if the extractor returns "CROSS JOIN"
        if any(join_type.startswith(b) for b in BLOCKED_JOIN_TYPES):
            # Store the canonical blocked type (e.g., "CROSS")
            blocked_found.append("CROSS")

    return blocked_found


def check_non_select_statement(sql: str) -> bool:
    """
    Check if SQL contains non-SELECT statements (DML/DDL/DCL).

    Returns:
        True if non-SELECT found (should be blocked)
        False if SELECT only (safe)
    """
    import sqlglot

    try:
        # Parse SQL
        parsed = sqlglot.parse_one(sql, dialect='postgres')

        # Check statement type
        statement_type = type(parsed).__name__

        # Allowed: Select, With (CTE with SELECT)
        allowed_types = ['Select', 'With', 'Union', 'Intersect', 'Except']
        # NEW: allow Subquery wrappers if they show up as root
        allowed_types.append('Subquery')

        if statement_type not in allowed_types:
            return True  # Block it

        # Additional check: if it's a WITH (CTE), ensure final query is SELECT
        if statement_type == 'With':
            # Get the main query from CTE
            if hasattr(parsed, 'this') and parsed.this:
                main_query_type = type(parsed.this).__name__
                if main_query_type not in ['Select', 'Union', 'Intersect', 'Except']:
                    return True

        return False  # Safe

    except Exception as e:
        # If parsing fails, fall back to keyword check
        sql_upper = sql.upper().strip()

        # Check if SQL starts with non-SELECT keywords
        non_select_starters = [
            'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE',
            'DROP', 'CREATE', 'ALTER', 'GRANT', 'REVOKE',
            'BEGIN', 'COMMIT', 'ROLLBACK', 'VACUUM', 'ANALYZE',
            'COPY', 'DO', 'CALL'
        ]

        for keyword in non_select_starters:
            if sql_upper.startswith(keyword):
                return True

        return False
