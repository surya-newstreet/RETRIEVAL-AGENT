"""
Join validation logic.
Validates join paths against FK graph and enforces join depth policies.
"""
from typing import List, Dict, Tuple, Optional
from core.join_graph_builder import JoinGraphBuilder
from observability.logger import get_logger

logger = get_logger(__name__)


class JoinValidator:
    """Validates joins against FK graph and depth policies."""

    def __init__(self, compiled_rules: dict):
        self.compiled_rules = compiled_rules
        self.join_paths = compiled_rules.get('join_paths', {})
        self.query_policies = compiled_rules.get('query_policies', {})

    def _default_schema(self) -> str:
        # NEW: derive default schema consistently
        allowed = self.query_policies.get('allowed_schemas', None)
        if isinstance(allowed, list) and allowed:
            return str(allowed[0])
        return "core"

    def _qualify_table(self, table: str, schema_name: Optional[str] = None) -> str:
        # NEW: normalize table names so join_paths matching is stable
        if not table:
            return table
        if "." in table:
            return table
        schema = schema_name or self._default_schema()
        return f"{schema}.{table}"

    def _alias_str(self, table_node) -> str:
        # NEW: robust alias extraction to always return a string
        # Prefer sqlglot's alias_or_name (usually returns a string)
        alias_or_name = getattr(table_node, "alias_or_name", None)
        try:
            if alias_or_name:
                return str(alias_or_name)
        except Exception:
            pass

        alias = getattr(table_node, "alias", None)
        if alias is None:
            return str(getattr(table_node, "name", "") or "")

        # sqlglot alias can be an expression (TableAlias)
        if isinstance(alias, str):
            return alias
        if hasattr(alias, "name") and alias.name:
            return str(alias.name)
        if hasattr(alias, "this") and alias.this:
            return str(alias.this)

        return str(getattr(table_node, "name", "") or "")

    def _iter_join_path_pairs(self, schema_name: Optional[str] = None):
        """
        Iterate over join_paths keys, yielding (from_table, to_table) pairs.

        Supports:
        - Tuple keys: (from_table, to_table)
        - String keys: "from_table->to_table"
        - Mixed keys (handles per-key)

        Yields:
            Tuple[str, str]: (from_table, to_table) pairs (schema-qualified)
        """
        if not self.join_paths:
            return

        schema = schema_name or self._default_schema()

        for key in self.join_paths.keys():
            # Tuple key
            if isinstance(key, tuple) and len(key) == 2:
                yield self._qualify_table(key[0], schema), self._qualify_table(key[1], schema)
                continue

            # String key "A->B"
            if isinstance(key, str) and "->" in key:
                left, right = key.split("->", 1)
                left = left.strip()
                right = right.strip()
                if left and right:
                    yield self._qualify_table(left, schema), self._qualify_table(right, schema)

    def validate_join_path(self, tables: List[str], schema_name: str = "core") -> Tuple[bool, List[str]]:
        """
        Validate if a series of tables can be joined via FK relationships.
        """
        if len(tables) <= 1:
            return True, []

        # Normalize all table names to schema-qualified format
        qualified_tables = []
        for table in tables:
            qualified_tables.append(self._qualify_table(table, schema_name))

        # If no join_paths dictionary, log warning and allow (cannot validate)
        if not self.join_paths:
            logger.warning("join_path_validation_skipped", reason="no_join_paths_available")
            return True, []

        # DEBUG: Inspect join_paths structure
        sample_keys = list(self.join_paths.keys())[:5]
        logger.debug(
            "join_paths_inspect",
            type=str(type(self.join_paths)),
            key_count=len(self.join_paths),
            sample_keys=sample_keys,
            sample_key_types=[type(k).__name__ for k in sample_keys]
        )

        # Build set of available tables from join_paths keys (normalized)
        available_tables = set()
        for from_t, to_t in self._iter_join_path_pairs(schema_name):
            available_tables.add(from_t)
            available_tables.add(to_t)

        # Check all tables exist in FK graph
        missing = []
        for table in qualified_tables:
            if table not in available_tables:
                missing.append(table)

        if missing:
            errors = [f"Table '{t}' not found in FK graph" for t in missing]
            return False, errors

        # Graph connectivity check: all tables must be reachable from anchor
        anchor = qualified_tables[0]
        reachable = {anchor}
        queue = [anchor]

        # NEW: pre-build adjacency for performance + correctness
        adjacency = {}
        for from_t, to_t in self._iter_join_path_pairs(schema_name):
            adjacency.setdefault(from_t, set()).add(to_t)
            adjacency.setdefault(to_t, set()).add(from_t)

        while queue:
            current = queue.pop(0)
            for nxt in adjacency.get(current, set()):
                if nxt not in reachable:
                    reachable.add(nxt)
                    queue.append(nxt)

        unreachable = set(qualified_tables) - reachable
        if unreachable:
            errors = [
                f"Tables {list(unreachable)} cannot be joined to {anchor} via FK relationships. "
                f"No FK path exists between these tables."
            ]
            return False, errors

        return True, []

    def check_join_depth(self, join_depth: int, has_where: bool) -> Dict[str, any]:
        """
        Check join depth against policies.
        """
        errors = []
        warnings = []

        max_depth = self.query_policies.get('max_join_depth', 4)
        hard_cap = self.query_policies.get('hard_cap_join_depth', 6)
        deep_threshold = self.query_policies.get('deep_join_threshold', 5)
        require_where_for_deep = self.query_policies.get('require_where_for_deep_joins', True)

        if join_depth > hard_cap:
            errors.append(
                f"Join depth {join_depth} exceeds hard cap of {hard_cap}. "
                "This query is too complex."
            )
            return {"is_valid": False, "errors": errors, "warnings": warnings}

        if join_depth > max_depth:
            warnings.append(
                f"Join depth {join_depth} exceeds recommended maximum of {max_depth}. "
                "Query may be slow."
            )

        if join_depth >= deep_threshold and require_where_for_deep and not has_where:
            errors.append(
                f"Join depth {join_depth} requires WHERE clause for scoping. "
                "Add a filter to limit the result set."
            )
            return {"is_valid": False, "errors": errors, "warnings": warnings}

        is_valid = len(errors) == 0
        return {"is_valid": is_valid, "errors": errors, "warnings": warnings}

    def validate_join_on_clauses(self, sql: str, fk_edges: list) -> Tuple[bool, List[str]]:
        """
        Validate JOIN ON clauses against FK edges.
        """
        import sqlglot
        from sqlglot import exp

        errors = []

        try:
            parsed = sqlglot.parse_one(sql, dialect='postgres')

            # E1) Extract CTE names
            cte_names = set()
            try:
                if hasattr(exp, 'CTE'):
                    for cte in parsed.find_all(exp.CTE):
                        alias = getattr(cte, 'alias_or_name', None) or getattr(cte, 'alias', None) or cte.args.get('alias')
                        if alias:
                            if isinstance(alias, str):
                                cte_names.add(alias.lower())
                            elif hasattr(alias, 'name'):
                                cte_names.add(str(alias.name).lower())
                            else:
                                cte_names.add(str(alias).lower())
            except Exception as e:
                logger.warning("cte_extraction_failed_in_join_validator", error=str(e))

            logger.info("join_on_validation_ctes", cte_names=list(cte_names))

            joins = list(parsed.find_all(exp.Join))
            if not joins:
                return True, []

            alias_map = {}
            default_schema = self._default_schema()

            # FROM tables
            for from_clause in parsed.find_all(exp.From):
                for table_node in from_clause.find_all(exp.Table):
                    table_name = table_node.name
                    alias = self._alias_str(table_node)

                    if table_name and table_name.lower() in cte_names:
                        full_name = table_name
                    else:
                        if table_node.db:
                            full_name = f"{table_node.db}.{table_name}"
                        else:
                            full_name = f"{default_schema}.{table_name}"

                    alias_map[str(alias)] = full_name

            # JOIN tables
            for join in joins:
                for table_node in join.find_all(exp.Table):
                    table_name = table_node.name
                    alias = self._alias_str(table_node)

                    if table_name and table_name.lower() in cte_names:
                        full_name = table_name
                    else:
                        if table_node.db:
                            full_name = f"{table_node.db}.{table_name}"
                        else:
                            full_name = f"{default_schema}.{table_name}"

                    alias_map[str(alias)] = full_name

            logger.info("alias_map_built", alias_map=alias_map)

            fk_lookup = set()
            for edge in fk_edges:
                fk_lookup.add((edge['from_table'], edge['from_column'], edge['to_table'], edge['to_column']))
                fk_lookup.add((edge['to_table'], edge['to_column'], edge['from_table'], edge['from_column']))

            for join_node in joins:
                on_condition = join_node.args.get('on')

                if not on_condition:
                    errors.append("JOIN found without ON clause. All joins must use explicit FK relationships.")
                    continue

                # EQ case
                if isinstance(on_condition, exp.EQ):
                    left_table, left_col = self._extract_table_column(on_condition.left)
                    right_table, right_col = self._extract_table_column(on_condition.right)

                    # NEW: If join columns aren’t qualified, treat as invalid (for FK enforcement)
                    if not all([left_table, left_col, right_table, right_col]):
                        errors.append(f"Unparseable JOIN ON condition: {str(on_condition)}. Use explicit table_alias.column = table_alias.column.")
                        continue

                    left_resolved = alias_map.get(left_table, left_table)
                    right_resolved = alias_map.get(right_table, right_table)

                    left_base = left_resolved.split('.')[-1].lower()
                    right_base = right_resolved.split('.')[-1].lower()
                    if left_base in cte_names or right_base in cte_names:
                        continue

                    join_tuple_1 = (left_resolved, left_col, right_resolved, right_col)
                    join_tuple_2 = (right_resolved, right_col, left_resolved, left_col)

                    if join_tuple_1 not in fk_lookup and join_tuple_2 not in fk_lookup:
                        errors.append(
                            f"JOIN between '{left_resolved}' and '{right_resolved}' on "
                            f"({left_col}, {right_col}) does not follow an FK relationship. "
                            f"Only FK-based joins are allowed."
                        )

                # AND case
                elif isinstance(on_condition, exp.And):
                    eq_predicates = list(on_condition.find_all(exp.EQ))
                    found_fk_match = False
                    any_cte_involved = False
                    parsed_any = False  # NEW

                    for eq_pred in eq_predicates:
                        lt, lc = self._extract_table_column(eq_pred.left)
                        rt, rc = self._extract_table_column(eq_pred.right)

                        if not all([lt, lc, rt, rc]):
                            continue

                        parsed_any = True
                        left_resolved = alias_map.get(lt, lt)
                        right_resolved = alias_map.get(rt, rt)

                        left_base = left_resolved.split('.')[-1].lower()
                        right_base = right_resolved.split('.')[-1].lower()
                        if left_base in cte_names or right_base in cte_names:
                            any_cte_involved = True
                            continue

                        jt1 = (left_resolved, lc, right_resolved, rc)
                        jt2 = (right_resolved, rc, left_resolved, lc)
                        if jt1 in fk_lookup or jt2 in fk_lookup:
                            found_fk_match = True
                            break

                    # NEW: if we couldn't parse any EQ predicates and no CTE involved, fail
                    if not parsed_any and not any_cte_involved:
                        errors.append(
                            f"Unparseable complex JOIN ON condition: {str(on_condition)}. "
                            f"Only FK-based joins are allowed."
                        )
                    elif not found_fk_match and not any_cte_involved:
                        errors.append(
                            "Complex JOIN condition with AND found, but no FK relationship matches any predicate. "
                            "Only FK-based joins are allowed."
                        )

                else:
                    # NEW: If it's neither EQ nor AND, enforce strictness unless CTE involved
                    errors.append(
                        f"Unsupported JOIN ON condition format: {str(on_condition)}. "
                        f"Use explicit FK join predicates (a.col = b.col)."
                    )

        except Exception as e:
            logger.error("join_on_validation_failed", error=str(e))
            return True, []

        return (len(errors) == 0), errors

    def _extract_table_column(self, node) -> Tuple[Optional[str], Optional[str]]:
        """Extract table.column from a sqlglot node."""
        from sqlglot import exp

        if isinstance(node, exp.Column):
            table = node.table if hasattr(node, 'table') and node.table else None
            column = node.name if hasattr(node, 'name') else str(node)
            return table, column

        if isinstance(node, exp.Dot):
            if hasattr(node, 'this') and hasattr(node, 'expression'):
                table = str(node.this)
                column = str(node.expression)
                return table, column

        return None, None

    def check_table_specific_policies(self, table_name: str) -> Dict[str, any]:
        """
        Check table-specific join policies from semantic KB.
        """
        if '.' in table_name:
            schema_part, table_part = table_name.rsplit('.', 1)
        else:
            schema_part = None
            table_part = table_name

        tables = self.compiled_rules.get('tables', {})

        table_meta = tables.get(table_name)
        if not table_meta:
            table_meta = tables.get(table_part)
        if not table_meta and schema_part:
            table_meta = tables.get(f"{schema_part}.{table_part}")
        if not table_meta:
            table_meta = tables.get(f"core.{table_part}")

        if not table_meta:
            table_meta = {}

        semantic = table_meta.get('semantic', {})
        join_policies = semantic.get('join_policies', {})

        return {
            "blocked_paths": join_policies.get('blocked_paths', []),
            "max_depth_override": join_policies.get('max_depth')
        }
