"""
SQL validator - orchestrates all validation rules.
Defense-in-depth: validates before execution for safety and correctness.
"""
from dataclasses import dataclass
from typing import List
from validation.ast_parser import sql_parser
from validation.blocked_patterns import (
    check_blocked_functions,
    check_blocked_keywords,
    check_blocked_join_types,
    check_non_select_statement  # NEW
)
from validation.join_validator import JoinValidator
from observability.logger import get_logger, log_validation_failure
from observability.metrics import metrics

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Result of SQL validation."""
    is_valid: bool
    sql: str  # Possibly modified (LIMIT injected)
    errors: List[str]
    warnings: List[str]
    safety_explanation: str


class SQLValidator:
    """Main SQL validator coordinating all validation rules."""
    
    def __init__(self, compiled_rules: dict):
        self.compiled_rules = compiled_rules
        self.query_policies = compiled_rules.get('query_policies', {})
        self.join_validator = JoinValidator(compiled_rules)
    
    async def validate_sql(self, sql: str, correlation_id: str) -> ValidationResult:
        """
        Complete SQL validation pipeline.
        
        Validation pipeline:
        1. Parse to AST
        2.Single statement check
        3. SELECT-only check
        4. Blocked keywords check
        5. Table existence check
        6. Column existence check (best effort)
        7. Schema qualification check
        8. Blocked functions check
        9. Join path validation
        10. Join depth policy
        11. LIMIT enforcement
        12. WHERE clause for deep joins
        
        Returns ValidationResult
        """
        # DEBUG: Log input types
        logger.info("validate_sql_input", sql_type=str(type(sql)), sql_preview=str(sql)[:100])

        errors = []
        warnings = []

        # ✅ FIX 1: Hard guard against None / non-string input to avoid parser crashes
        if not isinstance(sql, str) or not sql.strip():
            errors.append("Empty or invalid SQL generated.")
            metrics.record_validation_failure("empty_sql")
            log_validation_failure(logger, correlation_id, str(sql), errors)
            return ValidationResult(
                is_valid=False,
                sql=str(sql) if sql is not None else "",
                errors=errors,
                warnings=warnings,
                safety_explanation=""
            )
        
        # Step 1: Parse SQL
        ast = sql_parser.parse(sql)
        if ast is None:
            errors.append("SQL parsing failed. Check syntax.")
            metrics.record_validation_failure("parse_error")
            log_validation_failure(logger, correlation_id, sql, errors)
            return ValidationResult(
                is_valid=False,
                sql=sql,
                errors=errors,
                warnings=warnings,
                safety_explanation=""
            )
        
        # Step 2: Single statement check
        if not sql_parser.is_single_statement(sql):
            errors.append("Only single SQL statements are allowed. No multi-statement or stacked queries.")
            metrics.record_validation_failure("multi_statement")
        
        # Step 3: SELECT-only check (enhanced)
        # First check using AST
        if not sql_parser.is_select_only(ast):
            errors.append("Only SELECT queries are allowed. No INSERT/UPDATE/DELETE/DDL operations.")
            metrics.record_validation_failure("not_select")
        
        # Additional check using sqlglot for robust detection
        if check_non_select_statement(sql):
            errors.append("Non-SELECT statement detected. Only SELECT queries are allowed.")
            metrics.record_validation_failure("not_select_enhanced")
        
        # Step 4: Blocked keywords check
        blocked_keywords = check_blocked_keywords(sql)
        if blocked_keywords:
            errors.append(f"Blocked keywords found: {', '.join(blocked_keywords)}")
            metrics.record_validation_failure("blocked_keywords")
        
        # Step 5: Extract tables and functions
        tables = sql_parser.extract_tables(ast)
        functions = sql_parser.extract_functions(ast)
        joins = sql_parser.extract_joins(ast)
        
        # C1) Extract CTE names for validation filtering
        cte_names = sql_parser.extract_cte_names(ast)
        # ✅ FIX 2: Normalize CTE names to lowercase for consistent comparison
        cte_names = set(n.lower() for n in cte_names)
        logger.debug("cte_extraction", cte_names=list(cte_names), count=len(cte_names))
        
        # Step 6: Table existence check
        # ✅ FIX 3: More robust schema_name selection (policy -> compiled_rules -> default)
        allowed_schemas = self.query_policies.get('allowed_schemas') or []
        schema_name = allowed_schemas[0] if len(allowed_schemas) > 0 else (self.compiled_rules.get('schema_name') or 'core')

        available_tables = self.compiled_rules.get('tables', {})
        
        for table in tables:
            # C2) Skip CTE names from table existence check
            table_base = table.split('.')[-1]
            if table_base.lower() in cte_names:
                logger.debug("skipping_cte_table_existence", table=table)
                continue

            # ✅ FIX A: Explicitly block non-allowed schemas when user provides qualification
            if '.' in table:
                table_schema, table_name_only = table.split('.', 1)
                if table_schema != schema_name:
                    errors.append(
                        f"Schema '{table_schema}' is not allowed. Use only schema '{schema_name}'."
                    )
                    metrics.record_validation_failure("schema_not_allowed")
                    continue
                schema_qualified = table
            else:
                schema_qualified = f"{schema_name}.{table}"
            
            # Check if this qualified name exists in available_tables
            if schema_qualified not in available_tables:
                errors.append(f"Table '{schema_qualified}' does not exist in schema '{schema_name}'")
                metrics.record_validation_failure("table_not_found")
        
        # Step 7: Schema qualification check
        if self.query_policies.get('require_schema_qualification', True):
            for table in tables:
                # C3) Don't warn for CTE names
                table_base = table.split('.')[-1]
                if table_base.lower() in cte_names:
                    continue
                
                if '.' not in table:
                    warnings.append(
                        f"Table '{table}' should be schema-qualified as '{schema_name}.{table}'"
                    )
                    # Auto-fix: this could be done by AST transformation
        
        # Step 8: Blocked functions check
        blocked_funcs = check_blocked_functions(functions)
        if blocked_funcs:
            errors.append(f"Blocked functions found: {', '.join(blocked_funcs)}")
            metrics.record_validation_failure("blocked_functions")
        
        # Step 9: Blocked join types check
        blocked_joins = check_blocked_join_types(joins)
        if blocked_joins:
            errors.append(f"Blocked join types found: {', '.join(blocked_joins)}. CROSS JOIN is not allowed.")
            metrics.record_validation_failure("blocked_join_type")
        
        # Step 9: Join path validation
        # C4) Filter to physical tables only
        physical_tables = [t for t in tables if t.split('.')[-1].lower() not in cte_names]
        logger.debug(
            "physical_tables_for_join_validation", 
            physical=physical_tables, 
            ctes_excluded=len(tables) - len(physical_tables)
        )
        
        if len(physical_tables) > 1:
            is_valid_path, path_errors = self.join_validator.validate_join_path(
                list(physical_tables), 
                schema_name=schema_name
            )
            if not is_valid_path:
                errors.extend(path_errors)
                metrics.record_validation_failure("invalid_join_path")
        
        # Step 9b: JOIN ON clause validation (NEW)
        # ✅ FIX B: Do NOT skip validation just because a CTE exists.
        # JoinValidator already skips FK validation only for JOINs involving CTE sides.
        if len(joins) > 0:
            fk_edges = self.compiled_rules.get('fk_edges', [])
            is_valid_join_on, join_on_errors = self.join_validator.validate_join_on_clauses(sql, fk_edges)
            if not is_valid_join_on:
                errors.extend(join_on_errors)
                metrics.record_validation_failure("invalid_join_on")
        
        # Step 10: Join depth policy
        # C5) Use physical-only join depth
        join_depth = sql_parser.get_join_depth(ast, exclude_ctes=True)
        has_where = sql_parser.has_where(ast)
        
        if join_depth > 0:
            depth_result = self.join_validator.check_join_depth(join_depth, has_where)
            errors.extend(depth_result['errors'])
            warnings.extend(depth_result['warnings'])
            
            if not depth_result['is_valid']:
                metrics.record_validation_failure("join_depth_violation")
        
        # Step 12: LIMIT enforcement
        default_limit = self.query_policies.get('default_limit', 200)
        max_limit = self.query_policies.get('max_limit', 2000)
        
        # Initialize sql from the original input
        sql = sql  # Keep the original SQL by default
        
        has_limit_original = sql_parser.has_limit(ast)
        limit_changed = False

        if not has_limit_original:
            # Auto-inject default LIMIT
            sql = sql_parser.inject_limit(ast, default_limit)
            warnings.append(f"No LIMIT specified. Auto-injected LIMIT {default_limit}")
            limit_changed = True
        else:
            # Check if LIMIT exceeds max
            limit_value = sql_parser.get_limit_value(ast)
            if limit_value and limit_value > max_limit:
                # Cap the LIMIT
                sql = sql_parser.inject_limit(ast, max_limit)
                warnings.append(f"LIMIT {limit_value} exceeds maximum {max_limit}. Capped to {max_limit}")
                limit_changed = True
            # else: LIMIT is fine, keep original sql
        
        # ✅ FIX 5: safety explanation should reflect FINAL SQL, not only original AST
        has_limit_final = True if (has_limit_original or limit_changed) else False
        
        # Build safety explanation
        safety_explanation = self._build_safety_explanation(
            len(tables),
            join_depth,
            has_where,
            has_limit_final
        )
        
        # Final validation result
        is_valid = len(errors) == 0
        
        if not is_valid:
            log_validation_failure(logger, correlation_id, sql, errors)
        
        return ValidationResult(
            is_valid=is_valid,
            sql=sql,
            errors=errors,
            warnings=warnings,
            safety_explanation=safety_explanation if is_valid else ""
        )
    
    def _build_safety_explanation(
        self,
        table_count: int,
        join_depth: int,
        has_where: bool,
        has_limit: bool
    ) -> str:
        """Build human-readable explanation of why this query is safe."""
        explanations = []
        
        explanations.append("✓ Query validated as SELECT-only (no data modification)")
        explanations.append("✓ All tables exist in allowed schema")
        explanations.append("✓ No blocked functions or keywords detected")
        
        if table_count > 1:
            explanations.append(f"✓ Join path validated against FK graph (depth: {join_depth})")
        
        if has_where:
            explanations.append("✓ WHERE clause present for result scoping")
        
        if has_limit:
            explanations.append("✓ LIMIT enforced to prevent excessive results")
        
        explanations.append("✓ Will execute with read-only role and statement timeout")
        
        return "\n".join(explanations)
