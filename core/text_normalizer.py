"""
Generic text comparison helper (enum-aware, case-insensitive).

Provides SQL snippet generation for text comparisons based on column type:
- ENUM types: exact value matching
- CHECK constraints: validate against allowed values
- Free text: case-insensitive comparison
"""
from typing import Optional, List, Dict
from observability.logger import get_logger

logger = get_logger(__name__)


class TextNormalizer:
    """Generate SQL comparison snippets based on column metadata."""
    
    def __init__(self, compiled_rules: dict):
        self.compiled_rules = compiled_rules
        self.tables = compiled_rules.get('tables', {})
    
    def get_comparison_sql(
        self,
        table_name: str,
        column_name: str,
        value: str,
        comparison_op: str = '='
    ) -> str:
        """
        Generate SQL comparison snippet based on column type.
        
        Args:
            table_name: Schema-qualified table name
            column_name: Column to compare
            value: Value to compare against
            comparison_op: Operator (=, <>, LIKE, etc.)
        
        Returns:
            SQL snippet like "lower(col) = lower('value')" or "col = 'exact_value'"
        """
        # Get column metadata
        column_meta = self._get_column_metadata(table_name, column_name)
        
        if not column_meta:
            # Column not found, default to case-insensitive
            logger.warning(
                "column_not_found_defaulting_to_case_insensitive",
                table=table_name,
                column=column_name
            )
            return self._case_insensitive_comparison(column_name, value, comparison_op)
        
        # Check if ENUM type
        if column_meta.get('enum_values'):
            return self._enum_comparison(column_name, value, column_meta['enum_values'], comparison_op)
        
        # Check if CHECK constraint with known values
        if column_meta.get('check_constraint_values'):
            return self._constraint_comparison(
                column_name, value, column_meta['check_constraint_values'], comparison_op
            )
        
        # Default: case-insensitive for text types
        data_type = column_meta.get('data_type', '').lower()
        if  'char' in data_type or 'text' in data_type:
            return self._case_insensitive_comparison(column_name, value, comparison_op)
        
        # For non-text types (numeric, date, etc.), use direct comparison
        return f"{column_name} {comparison_op} '{value}'"
    
    def get_column_comparison_sql(
        self,
        table_name: str,
        column1_name: str,
        column2_name: str,
        comparison_op: str = '<>'
    ) -> str:
        """
        Generate SQL for comparing two text columns (e.g., status mismatches).
        
        Returns:
            SQL snippet like "lower(col1) <> lower(col2)"
        """
        col1_meta = self._get_column_metadata(table_name, column1_name)
        col2_meta = self._get_column_metadata(table_name, column2_name)
        
        # If either is text type, use case-insensitive
        col1_is_text = self._is_text_type(col1_meta.get('data_type', '') if col1_meta else '')
        col2_is_text = self._is_text_type(col2_meta.get('data_type', '') if col2_meta else '')
        
        if col1_is_text or col2_is_text:
            return f"lower({column1_name}) {comparison_op} lower({column2_name})"
        
        # Both non-text, direct comparison
        return f"{column1_name} {comparison_op} {column2_name}"
    
    def validate_enum_value(
        self,
        table_name: str,
        column_name: str,
        value: str
    ) -> bool:
        """
        Check if value is valid for an enum column.
        
        Returns:
            True if valid or not an enum column
        """
        column_meta = self._get_column_metadata(table_name, column_name)
        
        if not column_meta or not column_meta.get('enum_values'):
            return True  # Not an enum, any value is fine
        
        enum_values = column_meta['enum_values']
        return value in enum_values
    
    def get_allowed_values(
        self,
        table_name: str,
        column_name: str
    ) -> Optional[List[str]]:
        """
        Get list of allowed values for a column (if enum or constrained).
        
        Returns:
            List of allowed values or None if unconstrained
        """
        column_meta = self._get_column_metadata(table_name, column_name)
        
        if not column_meta:
            return None
        
        if column_meta.get('enum_values'):
            return column_meta['enum_values']
        
        if column_meta.get('check_constraint_values'):
            return column_meta['check_constraint_values']
        
        return None
    
    def _get_column_metadata(self, table_name: str, column_name: str) -> Optional[Dict]:
        """Get column metadata from compiled rules."""
        table_meta = self.tables.get(table_name)
        
        if not table_meta:
            return None
        
        columns = table_meta.get('columns', [])
        for col in columns:
            if col['column_name'] == column_name:
                return col
        
        return None
    
    def _is_text_type(self, data_type: str) -> bool:
        """Check if data type is text-based."""
        data_type_lower = data_type.lower()
        return any(t in data_type_lower for t in ['char', 'text', 'varchar'])
    
    def _enum_comparison(
        self,
        column_name: str,
        value: str,
        enum_values: List[str],
        comparison_op: str
    ) -> str:
        """Generate comparison for ENUM column."""
        # Find exact match (case-insensitive search)
        value_lower = value.lower()
        matched_value = None
        
        for enum_val in enum_values:
            if enum_val.lower() == value_lower:
                matched_value = enum_val
                break
        
        if matched_value:
            # Use exact enum value
            logger.info(
                "enum_value_matched",
                column=column_name,
                input_value=value,
                exact_value=matched_value
            )
            return f"{column_name} {comparison_op} '{matched_value}'"
        else:
            # No match, log warning and use provided value (will likely fail)
            logger.warning(
                "enum_value_not_found",
                column=column_name,
                value=value,
                allowed_values=enum_values
            )
            return f"{column_name} {comparison_op} '{value}'"
    
    def _constraint_comparison(
        self,
        column_name: str,
        value: str,
        constraint_values: List[str],
        comparison_op: str
    ) -> str:
        """Generate comparison for CHECK-constrained column."""
        # Similar to enum
        value_lower = value.lower()
        matched_value = None
        
        for const_val in constraint_values:
            if const_val.lower() == value_lower:
                matched_value = const_val
                break
        
        if matched_value:
            return f"{column_name} {comparison_op} '{matched_value}'"
        else:
            logger.warning(
                "constraint_value_not_found",
                column=column_name,
                value=value,
                allowed_values=constraint_values
            )
            return f"{column_name} {comparison_op} '{value}'"
    
    def _case_insensitive_comparison(
        self,
        column_name: str,
        value: str,
        comparison_op: str
    ) -> str:
        """Generate case-insensitive comparison for free text."""
        if comparison_op == '=' or comparison_op == '<>':
            return f"lower({column_name}) {comparison_op} lower('{value}')"
        elif comparison_op.upper() == 'LIKE':
            return f"lower({column_name}) LIKE lower('{value}')"
        else:
            # For other operators, use as-is
            return f"{column_name} {comparison_op} '{value}'"
