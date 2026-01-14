"""
SQL regeneration loop with explicit fix instructions.

When validation detects specific issues (missing time filter, wrong join),
regenerate SQL with targeted fix instructions while preserving context.
"""
from typing import Optional
from llm.groq_client import llm_provider
from core.context_resolver import ResolvedContext
from observability.logger import get_logger

logger = get_logger(__name__)


class SQLRegenerator:
    """Regenerate SQL with explicit fix instructions while preserving context."""
    
    MAX_RETRIES = 1  # Only retry once to avoid loops
    
    async def regenerate_with_fix(
        self,
        original_sql: str,
        fix_instruction: str,
        kb_context: dict,
        question: str,
        correlation_id: str,
        resolved_context: Optional[ResolvedContext] = None
    ) -> Optional[str]:
        """
        Regenerate SQL with a specific fix instruction while preserving context.
        
        Args:
            original_sql: SQL that failed validation
            fix_instruction: Specific instruction for what to fix
            kb_context: Schema context for regeneration
            question: Original user question
            correlation_id: For tracing
            resolved_context: Context to preserve during regeneration
        
        Returns:
            Regenerated SQL or None if regeneration fails
        """
        logger.info(
            "regenerating_sql",
            correlation_id=correlation_id,
            fix_instruction=fix_instruction
        )
        
        schema_name = kb_context.get('schema_name', 'core')
        tables = kb_context.get('tables', {})
        
        # Build concise schema description
        schema_lines = []
        for table_name, table_meta in tables.items():
            columns = table_meta['columns'][:10]  # Limit to first 10
            pks = table_meta['primary_keys']
            fks = table_meta['foreign_keys'][:3]  # Limit to first 3
            
            schema_lines.append(f"\nTable: {table_name}")
            schema_lines.append(f"Columns: {', '.join([c['column_name'] for c in columns])}")
            if pks:
                schema_lines.append(f"PK: {', '.join(pks)}")
            if fks:
                for fk in fks:
                    schema_lines.append(
                        f"FK: {fk['column_name']} -> {fk['referenced_schema']}.{fk['referenced_table_name']}.{fk['referenced_column_name']}"
                    )
        
        schema_text = '\n'.join(schema_lines)
        
        # Build context preservation section
        context_text = ""
        if resolved_context and resolved_context.is_related:
            dims = resolved_context.preserved_dimensions
            context_text = "\n\nCRITICAL - PRESERVE THESE DIMENSIONS:\n"
            if dims.limit:
                context_text += f"- Keep LIMIT {dims.limit}\n"
            if dims.ordering:
                context_text += f"- Keep ORDER BY {dims.ordering['column']} {dims.ordering.get('direction', 'ASC')}\n"
            if dims.grouping:
                context_text += f"- Keep GROUP BY {', '.join(dims.grouping)}\n"
            if dims.time_window:
                context_text += f"- Keep time window: {dims.time_window}\n"
        
        # Build regeneration prompt
        prompt = f"""You are a SQL expert. The following SQL query needs to be fixed.

ORIGINAL QUESTION: "{question}"

CURRENT SQL (has issues):
{original_sql}

FIX REQUIRED:
{fix_instruction}{context_text}

SCHEMA:
{schema_text}

RULES:
1. Use schema-qualified table names: {schema_name}.table_name
2. Generate ONLY SELECT queries
3. All joins must use valid FK relationships from the schema
4. Always include a LIMIT clause
5. Apply the fix instruction above
6. CRITICAL: Preserve all dimensions listed above unless they conflict with the fix

Generate the corrected SQL and respond with JSON:
{{
  "sql": "SELECT ...",
  "confidence": 0.0-1.0,
  "fix_applied": "brief description of what was fixed"
}}

SQL:"""
        
        try:
            response = await llm_provider.generate_structured_completion(
                prompt=prompt,
                temperature=0.0
            )
            
            fixed_sql = response.get('sql', '').strip()
            fix_applied = response.get('fix_applied', 'unknown')
            
            logger.info(
                "sql_regenerated",
                correlation_id=correlation_id,
                fix_applied=fix_applied,
                sql_length=len(fixed_sql)
            )
            
            return fixed_sql
        
        except Exception as e:
            logger.error(
                "sql_regeneration_failed",
                correlation_id=correlation_id,
                error=str(e)
            )
            return None


# Global instance
sql_regenerator = SQLRegenerator()
