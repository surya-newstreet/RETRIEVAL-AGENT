"""
SQL AST parser using sqlglot.
Provides SQL parsing, analysis, and transformation capabilities.
"""
import re
import sqlglot
from sqlglot import exp
from typing import List, Dict, Optional, Set, Tuple
from observability.logger import get_logger

logger = get_logger(__name__)


class SQLParser:
    """SQL AST parser and analyzer using sqlglot."""
    
    def __init__(self, dialect: str = "postgres"):
        self.dialect = dialect
    
    def parse(self, sql: str) -> Optional[exp.Expression]:
        """
        Parse SQL string to AST.
        Returns None if parsing fails.
        """
        try:
            ast = sqlglot.parse_one(sql, dialect=self.dialect)
            return ast
        except Exception as e:
            logger.warning("sql_parse_failed", sql=sql, error=str(e))
            return None
    
    def is_single_statement(self, sql: str) -> bool:
        """Check if SQL contains exactly one statement."""
        try:
            statements = sqlglot.parse(sql, dialect=self.dialect)
            return len(statements) == 1
        except:
            return False
    
    def is_select_only(self, ast: exp.Expression) -> bool:
        """Check if AST is a SELECT statement (no DML/DDL)."""
        if ast is None:
            return False
        
        # A1) Tree-wide forbidden node scan (defensive)
        # SECURITY FIX #1: Expanded to include Command, Set, Use, Kill
        forbidden_class_names = [
            'Insert', 'Update', 'Delete', 'Merge',
            'Create', 'Drop', 'Alter', 'Truncate', 'Rename',
            'Grant', 'Revoke',
            'Command', 'Set', 'Use', 'Kill'
        ]
        
        forbidden_types = []
        for class_name in forbidden_class_names:
            if hasattr(exp, class_name):
                forbidden_types.append(getattr(exp, class_name))
        
        # Scan entire tree for forbidden nodes
        for forbidden_type in forbidden_types:
            if ast.find(forbidden_type) is not None:
                return False
        
        # A2) Query structure acceptance (not just root type)
        allowed_root_types = [exp.Select, exp.Union, exp.Intersect, exp.Except]
        
        # Add optional types if they exist in this sqlglot version
        if hasattr(exp, 'Subquery'):
            allowed_root_types.append(exp.Subquery)
        if hasattr(exp, 'With'):
            allowed_root_types.append(exp.With)
        
        # ROBUSTNESS FIX #1: Use isinstance() instead of type() for subclass safety
        if isinstance(ast, tuple(allowed_root_types)):
            # Special handling for WITH: verify final query is SELECT-like
            if hasattr(exp, 'With') and isinstance(ast, exp.With):
                final_query = ast.this if hasattr(ast, 'this') else ast.args.get('this')
                if final_query:
                    # Use isinstance here too
                    if isinstance(final_query, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
                        return True
                    return False
            return True
        
        return False
    
    def extract_tables(self, ast: exp.Expression) -> List[str]:
        """
        Extract all table names from the query.
        Returns fully qualified names if present, otherwise table name only.
        """
        if ast is None:
            return []
        
        tables = []
        for table in ast.find_all(exp.Table):
            if table.db:
                # Schema qualified: schema.table
                tables.append(f"{table.db}.{table.name}")
            else:
                # Table name only
                tables.append(table.name)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_tables = []
        for table in tables:
            if table not in seen:
                seen.add(table)
                unique_tables.append(table)
        
        return unique_tables
    
    def extract_columns(self, ast: exp.Expression) -> Dict[str, List[str]]:
        """
        Extract columns used in SELECT, WHERE, GROUP BY, ORDER BY.
        Returns dict: {table_name: [columns]}
        """
        if ast is None:
            return {}
        
        columns_by_table = {}
        
        # Extract all column references
        for column in ast.find_all(exp.Column):
            table_name = column.table or "unknown"
            column_name = column.name
            
            if table_name not in columns_by_table:
                columns_by_table[table_name] = []
            
            if column_name not in columns_by_table[table_name]:
                columns_by_table[table_name].append(column_name)
        
        return columns_by_table
    
    def extract_functions(self, ast: exp.Expression) -> Set[str]:
        """Extract all function names used in the query."""
        if ast is None:
            return set()
        
        functions = set()
        for func in ast.find_all(exp.Func):
            functions.add(func.sql_name().lower())
        
        return functions
    
    def extract_joins(self, ast: exp.Expression) -> List[Dict]:
        """Extract JOIN information from query."""
        if ast is None:
            return []
        
        joins = []
        for join in ast.find_all(exp.Join):
            # ✅ IMPORTANT FIX: join.kind captures CROSS/NATURAL in sqlglot.
            # join.side captures LEFT/RIGHT/FULL. We prefer kind, then side, then INNER.
            join_kind = None
            try:
                join_kind = join.kind  # often "CROSS", "NATURAL", "INNER", etc. (depending on sqlglot version)
            except Exception:
                join_kind = None

            join_side = None
            try:
                join_side = join.side  # often "LEFT", "RIGHT", etc.
            except Exception:
                join_side = None

            join_type = join_kind or join_side or "INNER"

            # Normalize to uppercase string for validators
            if join_type is None:
                join_type = "INNER"
            join_type = str(join_type).upper()

            join_info = {
                "type": join_type,
                "table": None,
                "on_condition": None
            }
            
            # Extract joined table name
            if join.this:
                join_info["table"] = str(join.this)
            
            # Extract ON condition with type safety
            # D) CRITICAL: Never access join.on directly, use args.get()
            on_node = join.args.get('on', None)
            if on_node:
                # Check if it's an Expression with .sql() method
                if hasattr(on_node, 'sql'):
                    sql_method = getattr(on_node, 'sql', None)
                    if callable(sql_method):
                        try:
                            join_info["on_condition"] = sql_method(dialect=self.dialect)
                        except Exception:
                            # Fallback to string representation
                            join_info["on_condition"] = str(on_node)
                    else:
                        join_info["on_condition"] = str(on_node)
                else:
                    # Not an Expression, just stringify it
                    join_info["on_condition"] = str(on_node)
            
            joins.append(join_info)
        
        return joins
    
    def extract_cte_names(self, ast: exp.Expression) -> Set[str]:
        """
        Extract CTE names from WITH clauses.
        Returns lowercase set of CTE identifiers.
        """
        if ast is None:
            return set()
        
        cte_names = set()
        
        try:
            # STRATEGY 1: Find exp.CTE nodes
            if hasattr(exp, 'CTE'):
                cte_nodes = list(ast.find_all(exp.CTE))
                for cte in cte_nodes:
                    alias = None
                    
                    # Try alias_or_name (preferred)
                    if hasattr(cte, 'alias_or_name'):
                        try:
                            alias = cte.alias_or_name
                        except:
                            pass
                    
                    # Fallback to alias
                    if alias is None and hasattr(cte, 'alias'):
                        alias = cte.alias
                    
                    # Fallback to args
                    if alias is None:
                        alias = cte.args.get('alias')
                    
                    # Extract string from alias
                    if alias:
                        if isinstance(alias, str):
                            cte_names.add(alias.lower())
                        elif hasattr(alias, 'name'):
                            cte_names.add(str(alias.name).lower())
                        elif hasattr(alias, 'this'):
                            cte_names.add(str(alias.this).lower())
                        else:
                            cte_names.add(str(alias).lower())
            
            # STRATEGY 2: Find exp.With nodes and extract CTEs
            if hasattr(exp, 'With'):
                with_nodes = list(ast.find_all(exp.With))
                for with_node in with_nodes:
                    # Try to get CTE expressions
                    ctes = None
                    if hasattr(with_node, 'expressions'):
                        ctes = with_node.expressions
                    elif 'expressions' in with_node.args:
                        ctes = with_node.args.get('expressions')
                    
                    if ctes:
                        for cte_expr in ctes:
                            # Apply same alias extraction as STRATEGY 1
                            alias = None
                            if hasattr(cte_expr, 'alias_or_name'):
                                try:
                                    alias = cte_expr.alias_or_name
                                except:
                                    pass
                            if alias is None and hasattr(cte_expr, 'alias'):
                                alias = cte_expr.alias
                            if alias is None:
                                alias = cte_expr.args.get('alias') if hasattr(cte_expr, 'args') else None
                            
                            if alias:
                                if isinstance(alias, str):
                                    cte_names.add(alias.lower())
                                elif hasattr(alias, 'name'):
                                    cte_names.add(str(alias.name).lower())
                                else:
                                    cte_names.add(str(alias).lower())
        
        except Exception as e:
            logger.warning("cte_extraction_failed", error=str(e))
            return set()
        
        return cte_names

    
    def has_limit(self, ast: exp.Expression) -> bool:
        """Check if query has a LIMIT clause."""
        if ast is None:
            return False
        return ast.find(exp.Limit) is not None
    
    def get_limit_value(self, ast: exp.Expression) -> Optional[int]:
        """Get the LIMIT value if present."""
        if ast is None:
            return None
        
        limit_node = ast.find(exp.Limit)
        if limit_node and limit_node.expression:
            try:
                return int(limit_node.expression.this)
            except:
                return None
        return None
    
    def inject_limit(self, sql_or_ast, limit: int):
        """
        Inject or replace LIMIT clause in the query.
        Can accept either SQL string or AST, returns SQL string.
        """
        # Convert AST to SQL if needed
        if isinstance(sql_or_ast, exp.Expression):
            sql = sql_or_ast.sql(dialect=self.dialect)
        else:
            sql = sql_or_ast
        
        if not sql:
            return ""
        
        # Check if LIMIT already exists
        if re.search(r'\bLIMIT\s+\d+', sql, flags=re.IGNORECASE):
            # Replace existing LIMIT
            sql = re.sub(r'\bLIMIT\s+\d+', f'LIMIT {limit}', sql, flags=re.IGNORECASE)
        else:
            # Add LIMIT at the end
            sql = sql.rstrip(';').strip() + f' LIMIT {limit}'
        
        return sql
    
    def has_where(self, ast: exp.Expression) -> bool:
        """Check if query has a WHERE clause."""
        if ast is None:
            return False
        return ast.find(exp.Where) is not None
    
    def sql_from_ast(self, ast: exp.Expression) -> str:
        """Convert AST back to SQL string."""
        if ast is None:
            return ""
        return ast.sql(dialect=self.dialect)
    
    def format_sql(self, sql: str, pretty: bool = True) -> str:
        """Format SQL for readability."""
        try:
            ast = self.parse(sql)
            if ast:
                return ast.sql(dialect=self.dialect, pretty=pretty)
            return sql
        except:
            return sql
    
    def count_joins(self, ast: exp.Expression) -> int:
        """Count the number of joins in the query."""
        if ast is None:
            return 0
        return len(list(ast.find_all(exp.Join)))
    
    def get_join_depth(self, ast: exp.Expression, exclude_ctes: bool = False) -> int:
        """
        Calculate join depth as number of unique tables - 1.
        Join depth = number of joins required.
        
        Args:
            ast: SQL AST to analyze
            exclude_ctes: If True, exclude CTE names from table count
        """
        if ast is None:
            return 0
        
        tables = self.extract_tables(ast)
        
        # C5) Exclude CTEs if requested
        if exclude_ctes:
            cte_names = self.extract_cte_names(ast)
            tables = [t for t in tables if t.split('.')[-1].lower() not in cte_names]
        
        # Deduplicate table names (case-insensitive)
        unique_tables = set(t.lower() for t in tables)
        
        # Join depth = number of unique tables - 1
        return max(0, len(unique_tables) - 1)



# Global parser instance
sql_parser = SQLParser()
