"""
Rules compiler.
Merges kb_schema.json + kb_semantic.json → compiled_rules.json
This is the runtime source of truth for SQL generation and validation.
"""
import json
import aiofiles
from pathlib import Path
from datetime import datetime
from typing import Dict
from core.schema_introspector import SchemaIntrospector
from core.join_graph_builder import JoinGraphBuilder
from core.semantic_store import semantic_store
from observability.logger import get_logger
from core.config import settings

logger = get_logger(__name__)

KB_DIR = Path(__file__).parent.parent / "kb"
KB_SCHEMA_PATH = KB_DIR / "kb_schema.json"
COMPILED_RULES_PATH = KB_DIR / "compiled_rules.json"


# Blocked SQL functions and patterns for safety
BLOCKED_FUNCTIONS = [
    "pg_sleep",
    "pg_read_file",
    "pg_ls_dir",
    "pg_read_binary_file",
    "dblink",
    "dblink_exec",
    "lo_import",
    "lo_export",
    "copy",
    # Optionally block EXPLAIN ANALYZE (can be expensive)
    # "explain",
]

BLOCKED_PATTERNS = [
    "DROP",
    "TRUNCATE",
    "ALTER",
    "CREATE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "GRANT",
    "REVOKE",
    "VACUUM",
    "ANALYZE",  # Can be blocked if wanting to prevent schema analysis
]


class RulesCompiler:
    """Compiles schema and semantic KB into runtime rules."""

    def __init__(self):
        KB_DIR.mkdir(parents=True, exist_ok=True)

    async def load_kb_schema(self) -> dict:
        """Load kb_schema.json."""
        # Check temp file first (during refresh)
        temp_path = KB_DIR / "kb_schema_temp.json"
        if temp_path.exists():
            async with aiofiles.open(temp_path, 'r') as f:
                content = await f.read()
                return json.loads(content)

        if not KB_SCHEMA_PATH.exists():
            logger.error("kb_schema_not_found", path=str(KB_SCHEMA_PATH))
            return None

        async with aiofiles.open(KB_SCHEMA_PATH, 'r') as f:
            content = await f.read()
            return json.loads(content)

    async def compile_rules(self) -> dict:
        """
        Compile complete rules from schema + semantic + join graph.

        Returns compiled_rules.json structure:
        - version (timestamp)
        - schema_name
        - tables (merged schema + semantic)
        - join_graph
        - join_paths
        - fk_edges (NEW: for JOIN ON validation)
        - query_policies
        """
        logger.info("rules_compilation_started")

        # Load schema KB
        kb_schema = await self.load_kb_schema()
        if not kb_schema:
            raise ValueError("kb_schema.json not found or invalid")

        # Load semantic KB
        semantic_kb = await semantic_store.load()

        # Build join graph
        join_builder = JoinGraphBuilder(kb_schema)
        join_data = join_builder.to_dict()

        # Extract FK edges for JOIN ON validation
        fk_edges = join_builder.get_fk_edges()
        fk_edges_dict = [
            {
                "from_table": edge.from_table,
                "from_column": edge.from_column,
                "to_table": edge.to_table,
                "to_column": edge.to_column,
                "constraint_name": edge.constraint_name
            }
            for edge in fk_edges
        ]

        # Merge tables with semantic metadata
        merged_tables = {}
        for schema_qualified_name, table_meta in kb_schema['tables'].items():
            # Extract unqualified table name for semantic lookup
            table_name = table_meta.get('table', schema_qualified_name.split('.')[-1])
            semantic_meta = semantic_store.get_table_semantic(table_name)

            # Keep schema-qualified key and include all new fields
            merged_tables[schema_qualified_name] = {
                "schema": table_meta.get('schema', kb_schema['schema_name']),
                "table": table_name,
                "schema_qualified_name": schema_qualified_name,
                "columns": table_meta['columns'],
                "primary_keys": table_meta['primary_keys'],
                "foreign_keys": table_meta['foreign_keys'],
                "indexes": table_meta['indexes'],
                "check_constraints": table_meta.get('check_constraints', []),
                "domain": table_meta.get('domain', 'general'),
                "date_columns": table_meta.get('date_columns', []),  # NEW
                "status_columns": table_meta.get('status_columns', []),  # NEW
                "natural_key_candidates": table_meta.get('natural_key_candidates', []),  # NEW
                "semantic": semantic_meta
            }

        # Build query policies
        query_policies = {
            "default_limit": settings.default_limit,
            "max_limit": settings.max_limit,
            "max_join_depth": settings.max_join_depth,
            "hard_cap_join_depth": settings.hard_cap_join_depth,
            "require_where_for_deep_joins": True,
            "deep_join_threshold": 5,  # Depth >= 5 requires WHERE
            "blocked_functions": BLOCKED_FUNCTIONS,
            "blocked_patterns": BLOCKED_PATTERNS,
            "require_schema_qualification": True,
            "allowed_schemas": [kb_schema['schema_name']],
            "statement_timeout_seconds": settings.statement_timeout_seconds,
        }

        # Build compiled rules
        version = datetime.utcnow().isoformat()
        compiled_rules = {
            "version": version,
            "schema_name": kb_schema['schema_name'],
            "tables": merged_tables,
            "join_graph": join_data['graph'],
            "join_paths": join_data['join_paths'],
            "fk_edges": fk_edges_dict,  # NEW: FK edges for validation
            "query_policies": query_policies,
            "compiled_at": version
        }

        logger.info(
            "rules_compilation_completed",
            version=version,
            table_count=len(merged_tables),
            fk_edge_count=len(fk_edges_dict)
        )

        return compiled_rules

    async def validate_compiled_rules(self, rules: dict) -> bool:
        """Validate compiled rules structure."""
        required_keys = [
            'version',
            'schema_name',
            'tables',
            'join_graph',
            'join_paths',
            'fk_edges',        # REQUIRED (used by JOIN ON validation)
            'query_policies'
        ]

        for key in required_keys:
            if key not in rules:
                logger.error("compiled_rules_missing_key", key=key)
                return False

        if not rules['tables']:
            logger.error("compiled_rules_no_tables")
            return False

        # Keep this permissive: fk_edges can be empty for schemas with no FKs,
        # but key must exist and be a list.
        fk_edges = rules.get('fk_edges')
        if fk_edges is None or not isinstance(fk_edges, list):
            logger.error("compiled_rules_invalid_fk_edges", fk_edges_type=str(type(fk_edges)))
            return False

        logger.info("compiled_rules_validated", table_count=len(rules['tables']))
        return True

    async def save_compiled_rules(self, rules: dict, temp: bool = False) -> None:
        """
        Save compiled rules to file.

        Args:
            rules: Compiled rules data
            temp: If True, saves to temp file for atomic swap
        """
        file_path = KB_DIR / "compiled_rules_temp.json" if temp else COMPILED_RULES_PATH

        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(rules, indent=2))

        logger.info(
            "compiled_rules_saved",
            path=str(file_path),
            version=rules['version'],
            is_temp=temp
        )

    async def load_compiled_rules(self) -> dict:
        """Load current compiled rules."""
        if not COMPILED_RULES_PATH.exists():
            logger.warning("compiled_rules_not_found")
            return None

        async with aiofiles.open(COMPILED_RULES_PATH, 'r') as f:
            content = await f.read()
            rules = json.loads(content)

        logger.info(
            "compiled_rules_loaded",
            version=rules.get('version'),
            table_count=len(rules.get('tables', {}))
        )
        return rules

    async def atomic_swap(self) -> None:
        """
        Atomically swap temp files to production.
        Renames temp files to production files in a single operation.
        """
        temp_files = {
            "schema": KB_DIR / "kb_schema_temp.json",
            "semantic": KB_DIR / "kb_semantic_temp.json",
            "rules": KB_DIR / "compiled_rules_temp.json"
        }

        prod_files = {
            "schema": KB_SCHEMA_PATH,
            "semantic": KB_DIR / "kb_semantic.json",
            "rules": COMPILED_RULES_PATH
        }

        # Verify all temp files exist
        for name, path in temp_files.items():
            if not path.exists():
                raise FileNotFoundError(f"Temp file missing: {path}")

        # Atomic rename (on same filesystem this is atomic)
        for name in temp_files.keys():
            temp_files[name].replace(prod_files[name])

        logger.info(
            "atomic_swap_completed",
            files=list(prod_files.keys())
        )


# Global rules compiler instance
rules_compiler = RulesCompiler()
