"""
Enhanced prompt builder for SQL generation with schema-agnostic features.

This module provides helper functions to build an enhanced SQL generation prompt
with FK edges, enum values, date columns, natural keys, and comprehensive rules.
"""

from typing import Dict, List
import json


# -------------------------------------------------------------------
# FK EDGES
# -------------------------------------------------------------------

def build_fk_edges_text(fk_edges: List[Dict]) -> str:
    """Build formatted FK edges text for prompt."""
    if not fk_edges:
        return "No FK relationships defined."

    lines = []

    for edge in fk_edges[:30]:  # Limit to first 30
        from_table = edge.get("from_table")
        from_col = edge.get("from_column")
        to_table = edge.get("to_table")
        to_col = edge.get("to_column")

        # Skip malformed edges safely
        if not all([from_table, from_col, to_table, to_col]):
            continue

        lines.append(f"{from_table}.{from_col} = {to_table}.{to_col}")

    return "\n".join(lines) if lines else "No FK relationships defined."


# -------------------------------------------------------------------
# ENUM / CHECK CONSTRAINT COLUMNS
# -------------------------------------------------------------------

def build_enum_columns_text(tables: Dict) -> str:
    """Build formatted enum and check-constrained columns text."""
    lines = []

    for table_name, table_meta in (tables or {}).items():
        columns = table_meta.get("columns", []) or []

        for col in columns:
            col_name = col.get("column_name")
            if not col_name:
                continue

            enum_values = col.get("enum_values")
            if enum_values:
                lines.append(
                    f"{table_name}.{col_name}: {', '.join(map(str, enum_values))}"
                )

            check_values = col.get("check_constraint_values")
            if check_values:
                lines.append(
                    f"{table_name}.{col_name}: {', '.join(map(str, check_values))}"
                )

    return "\n".join(lines) if lines else "No enum/constrained columns."


# -------------------------------------------------------------------
# DATE COLUMNS
# -------------------------------------------------------------------

def build_date_columns_text(tables: Dict) -> str:
    """Build formatted date columns text."""
    lines = []

    for table_name, table_meta in (tables or {}).items():
        date_cols = table_meta.get("date_columns", []) or []
        if date_cols:
            lines.append(f"{table_name}: {', '.join(map(str, date_cols))}")

    return "\n".join(lines) if lines else "No date columns identified."


# -------------------------------------------------------------------
# NATURAL KEYS
# -------------------------------------------------------------------

def build_natural_key_text(tables: Dict) -> str:
    """Build formatted natural key candidate columns text."""
    lines = []

    for table_name, table_meta in (tables or {}).items():
        nk_cols = table_meta.get("natural_key_candidates", []) or []
        if nk_cols:
            lines.append(f"{table_name}: {', '.join(map(str, nk_cols))}")

    return "\n".join(lines) if lines else "No natural key candidates."


# -------------------------------------------------------------------
# MAIN PROMPT BUILDER
# -------------------------------------------------------------------

def build_enhanced_sql_prompt(
    question: str,
    schema_name: str,
    tables: Dict,
    fk_edges: List[Dict],
    query_policies: Dict,
    context_text: str = "",
    clarification_text: str = "",
) -> str:
    """
    Build enhanced SQL generation prompt with all schema-agnostic features.
    """

    default_limit = (query_policies or {}).get("default_limit", 20)
    max_limit = (query_policies or {}).get("max_limit", 100)

    # -------------------------
    # SCHEMA SECTION
    # -------------------------

    schema_lines = []

    for table_name, table_meta in (tables or {}).items():
        columns = (table_meta.get("columns", []) or [])[:15]
        pks = table_meta.get("primary_keys", []) or []
        fks = (table_meta.get("foreign_keys", []) or [])[:5]

        schema_lines.append(f"\nTable: {table_name}")

        col_parts = []
        for col in columns:
            col_name = col.get("column_name")
            data_type = col.get("data_type")
            if col_name and data_type:
                col_parts.append(f"{col_name}:{data_type}")

        schema_lines.append(
            "Columns: " + ", ".join(col_parts)
            if col_parts
            else "Columns: (unavailable)"
        )

        if pks:
            schema_lines.append(f"PK: {', '.join(map(str, pks))}")

        for fk in fks:
            fk_col = fk.get("column_name")
            ref_table_name = fk.get("referenced_table_name")
            ref_col = fk.get("referenced_column_name")

            if not all([fk_col, ref_table_name, ref_col]):
                continue

            ref_schema = fk.get("referenced_schema", schema_name)
            schema_lines.append(
                f"FK: {fk_col} -> {ref_schema}.{ref_table_name}.{ref_col}"
            )

    schema_text = "\n".join(schema_lines)

    # -------------------------
    # METADATA SECTIONS
    # -------------------------

    fk_text = build_fk_edges_text(fk_edges or [])
    enum_text = build_enum_columns_text(tables or {})
    date_text = build_date_columns_text(tables or {})
    nk_text = build_natural_key_text(tables or {})

    # -------------------------
    # FINAL PROMPT
    # -------------------------

    prompt = f"""
You are a PostgreSQL SQL generator. Convert natural language to safe, read-only SQL.

{context_text}

## SCHEMA
{schema_text}

## FK RELATIONSHIPS (CRITICAL - JOINS MUST USE ONLY THESE)
{fk_text}

## ENUM COLUMNS (use exact values)
{enum_text}

## DATE COLUMNS (for time filtering)
{date_text}

## NATURAL KEYS (for filtering)
{nk_text}

---

## GENERATION RULES

1. **Schema**: Always use `{schema_name}.table_name` (even in subqueries)
2. **READ-ONLY**: Only SELECT queries. No INSERT/UPDATE/DELETE/DDL
3. **JOINS**: MUST use FK relationships above. Never join on name-matching columns
4. **TEXT**: ENUM columns use exact values. Other text use `lower(col) = lower('val')`
5. **TIME**: If time mentioned, MUST include WHERE on date column
6. **AGGREGATION**: For "latest N then sum", use subquery with ORDER + LIMIT
7. **LIMIT**: Always include (default {default_limit}, max {max_limit})

## CRITICAL SQL PATTERNS

### Time Filtering with LEFT JOIN
❌ WRONG:
LEFT JOIN collections c ON l.id = c.loan_id
WHERE c.collection_date >= CURRENT_DATE - INTERVAL '6 months'

✅ CORRECT:
LEFT JOIN collections c
  ON l.id = c.loan_id
 AND c.collection_date >= CURRENT_DATE - INTERVAL '6 months'

### LEFT JOIN Aggregation
Always use:
COALESCE(SUM(c.amount), 0)

### Latest-N then Aggregate
SELECT SUM(amount)
FROM (
  SELECT amount
  FROM {schema_name}.table
  ORDER BY date DESC
  LIMIT 20
) t

### Multiple Result Sets
Use UNION / UNION ALL only.

### Schema Qualification
Every table MUST be prefixed with `{schema_name}.`

{clarification_text}

QUESTION:
"{question}"

Respond ONLY with JSON:
{{
  "sql": "SELECT ...",
  "confidence": 0.0,
  "tables_used": ["{schema_name}.table"],
  "intent_summary": {{
    "subject": "",
    "metric": "",
    "time_window": null,
    "grouping": [],
    "ordering": null,
    "limit": null,
    "tables": []
  }}
}}

SQL:
"""

    return prompt
