"""
Schema introspection module.
Extracts database metadata from information_schema and pg_catalog.
Produces kb_schema.json with tables, columns, PKs, FKs, and indexes.
"""
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
from db.connection import db_manager
from observability.logger import get_logger
from core.config import settings

logger = get_logger(__name__)


@dataclass
class ColumnMetadata:
    """Metadata for a single column."""
    column_name: str
    data_type: str
    is_nullable: bool
    column_default: Optional[str] = None
    character_maximum_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None  # FIX #3: Added numeric_scale
    ordinal_position: int = 0


@dataclass
class ForeignKeyMetadata:
    """Metadata for a foreign key relationship."""
    constraint_name: str
    table_name: str
    column_name: str
    referenced_schema: str  # FIX #2: Added referenced schema
    referenced_table_name: str
    referenced_column_name: str


@dataclass
class IndexMetadata:
    """Metadata for an index."""
    index_name: str
    table_name: str
    columns: List[str]
    is_unique: bool
    is_primary: bool


@dataclass
class TableMetadata:
    """Complete metadata for a table."""
    table_name: str
    columns: List[ColumnMetadata]
    primary_keys: List[str]
    foreign_keys: List[ForeignKeyMetadata]
    indexes: List[IndexMetadata]


class SchemaIntrospector:
    """Extracts metadata from PostgreSQL database."""
    
    def __init__(self, schema_name: str = None):
        self.schema_name = schema_name or settings.schema_name
    
    async def extract_tables(self) -> List[str]:
        """Extract all table names from the schema."""
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        
        async with db_manager.acquire_metadata_connection() as conn:
            rows = await conn.fetch(query, self.schema_name)
            tables = [row['table_name'] for row in rows]
            
        logger.info(
            "tables_extracted",
            schema=self.schema_name,
            table_count=len(tables)
        )
        return tables
    
    async def extract_columns(self, table_name: str) -> List[ColumnMetadata]:
        """Extract column metadata for a specific table."""
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = $2
            ORDER BY ordinal_position
        """
        
        async with db_manager.acquire_metadata_connection() as conn:
            rows = await conn.fetch(query, self.schema_name, table_name)
            
        columns = [
            ColumnMetadata(
                column_name=row['column_name'],
                data_type=row['data_type'],
                is_nullable=row['is_nullable'] == 'YES',
                column_default=row['column_default'],
                character_maximum_length=row['character_maximum_length'],
                numeric_precision=row['numeric_precision'],
                numeric_scale=row['numeric_scale'],  # FIX #3: Include scale
                ordinal_position=row['ordinal_position']
            )
            for row in rows
        ]
        
        return columns
    
    async def extract_primary_keys(self, table_name: str) -> List[str]:
        """Extract primary key columns for a table."""
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
              AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = $1
              AND tc.table_name = $2
            ORDER BY kcu.ordinal_position
        """
        
        async with db_manager.acquire_metadata_connection() as conn:
            rows = await conn.fetch(query, self.schema_name, table_name)
            
        return [row['column_name'] for row in rows]
    
    async def extract_foreign_keys(self, table_name: str = None) -> List[ForeignKeyMetadata]:
        """
        Extract foreign key relationships.
        If table_name is None, extracts all FKs in the schema.
        """
        query = """
            SELECT 
                tc.constraint_name,
                tc.table_name,
                kcu.column_name,
                ccu.table_schema AS referenced_schema,
                ccu.table_name AS referenced_table_name,
                ccu.column_name AS referenced_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON tc.constraint_name = ccu.constraint_name
              AND tc.table_schema = ccu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = $1
        """
        
        params = [self.schema_name]
        if table_name:
            query += " AND tc.table_name = $2"
            params.append(table_name)
        
        query += " ORDER BY tc.table_name, kcu.ordinal_position"
        
        async with db_manager.acquire_metadata_connection() as conn:
            rows = await conn.fetch(query, *params)
        
        fks = [
            ForeignKeyMetadata(
                constraint_name=row['constraint_name'],
                table_name=row['table_name'],
                column_name=row['column_name'],
                referenced_schema=row['referenced_schema'],  # FIX #2: Include schema
                referenced_table_name=row['referenced_table_name'],
                referenced_column_name=row['referenced_column_name']
            )
            for row in rows
        ]
        
        logger.info(
            "foreign_keys_extracted",
            schema=self.schema_name,
            table=table_name or "all",
            fk_count=len(fks)
        )
        return fks
    
    async def extract_indexes(self, table_name: str) -> List[IndexMetadata]:
        """Extract index metadata for a table using pg_catalog."""
        query = """
            SELECT
                i.relname AS index_name,
                t.relname AS table_name,
                array_agg(a.attname ORDER BY a.attnum) AS columns,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE n.nspname = $1
              AND t.relname = $2
            GROUP BY i.relname, t.relname, ix.indisunique, ix.indisprimary
            ORDER BY i.relname
        """
        
        try:
            async with db_manager.acquire_metadata_connection() as conn:
                rows = await conn.fetch(query, self.schema_name, table_name)
            
            indexes = [
                IndexMetadata(
                    index_name=row['index_name'],
                    table_name=row['table_name'],
                    columns=list(row['columns']),
                    is_unique=row['is_unique'],
                    is_primary=row['is_primary']
                )
                for row in rows
            ]
            return indexes
        except Exception as e:
            # If pg_catalog is not accessible, return empty list
            logger.warning(
                "index_extraction_failed",
                table=table_name,
                error=str(e)
            )
            return []
    
    async def extract_enum_types(self) -> Dict[str, List[str]]:
        """Extract all ENUM types and their allowed values from pg_enum."""
        query = """
            SELECT 
                t.typname AS enum_type,
                array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname = $1
            GROUP BY t.typname
        """
        
        try:
            async with db_manager.acquire_metadata_connection() as conn:
                rows = await conn.fetch(query, self.schema_name)
            
            enum_types = {
                row['enum_type']: list(row['enum_values'])
                for row in rows
            }
            
            logger.info(
                "enum_types_extracted",
                schema=self.schema_name,
                enum_count=len(enum_types)
            )
            return enum_types
        
        except Exception as e:
            logger.warning("enum_extraction_failed", error=str(e))
            return {}
    
    async def extract_check_constraints(self, table_name: str) -> List[Dict]:
        """Extract CHECK constraints for a table."""
        query = """
            SELECT 
                cc.constraint_name,
                cc.check_clause
            FROM information_schema.check_constraints cc
            JOIN information_schema.table_constraints tc
              ON cc.constraint_name = tc.constraint_name
              AND cc.constraint_schema = tc.table_schema
            WHERE tc.table_schema = $1
              AND tc.table_name = $2
        """
        
        try:
            async with db_manager.acquire_metadata_connection() as conn:
                rows = await conn.fetch(query, self.schema_name, table_name)
            return [
                {
                    "constraint_name": row['constraint_name'],
                    "check_clause": row['check_clause']
                }
                for row in rows
            ]
        except Exception as e:
            logger.warning("check_constraints_extraction_failed", table=table_name, error=str(e))
            return []
    
    def _parse_check_constraint_values(self, check_clause: str) -> Optional[List[str]]:
        """
        Parse CHECK constraint clause to extract allowed values.
        
        Example: "((status)::text = ANY (ARRAY['active'::text, 'inactive'::text]))"
        Returns: ['active', 'inactive']
        """
        import re
        
        # Pattern for ARRAY['val1', 'val2', ...]
        array_pattern = r"ARRAY\[([^\]]+)\]"
        match = re.search(array_pattern, check_clause, re.IGNORECASE)
        
        if match:
            values_str = match.group(1)
            # Extract quoted strings
            value_pattern = r"'([^']+)'"
            values = re.findall(value_pattern, values_str)
            return values if values else None
        
        # Pattern for (col = 'val1' OR col = 'val2')
        or_pattern = r"=\s*'([^']+)'"
        matches = re.findall(or_pattern, check_clause)
        if matches:
            return matches
        
        return None
    
    def identify_date_columns(self, columns: List[Dict]) -> List[str]:
        """
        Identify date/timestamp columns from column list.
        
        Returns:
            List of column names that are date/time types
        """
        date_types = ['date', 'timestamp', 'timestamptz', 'timestamp with time zone', 'timestamp without time zone']
        
        date_cols = [
            col['column_name']
            for col in columns
            if any(dt in col['data_type'].lower() for dt in date_types)
        ]
        
        return date_cols
    
    def identify_status_columns(self, columns: List[Dict]) -> List[str]:
        """
        Identify likely status/state columns (heuristic: name + type).
        
        Returns:
            List of column names that are likely status columns
        """
        status_keywords = ['status', 'state', 'type', 'stage', 'phase']
        text_types = ['character varying', 'varchar', 'text', 'char', 'USER-DEFINED']  # USER-DEFINED = enum
        
        status_cols = [
            col['column_name']
            for col in columns
            if any(keyword in col['column_name'].lower() for keyword in status_keywords)
            and any(ttype in col['data_type'] for ttype in text_types)
        ]
        
        return status_cols
    
    def identify_natural_key_candidates(self, columns: List[Dict]) -> List[str]:
        """
        Identify likely natural key columns (heuristic: name patterns, NOT FKs).
        
        Returns:
            List of column names that could be natural keys
        """
        natural_key_keywords = ['number', 'code', 'name', 'email', 'username']
        
        # Exclude columns ending with _id (likely FKs)
        candidates = [
            col['column_name']
            for col in columns
            if any(keyword in col['column_name'].lower() for keyword in natural_key_keywords)
            and not col['column_name'].lower().endswith('_id')
        ]
        
        return candidates
    
    async def build_kb_schema(self) -> dict:
        """
        Build complete kb_schema.json structure.
        This is the auto-generated metadata artifact.
        """
        logger.info("kb_schema_build_started", schema=self.schema_name)
        
        tables = await self.extract_tables()
        all_fks = await self.extract_foreign_keys()
        enum_types = await self.extract_enum_types()  # NEW: Extract enum types
        
        # Build table metadata with schema qualification
        tables_metadata = {}
        for table_name in tables:
            columns = await self.extract_columns(table_name)
            pks = await self.extract_primary_keys(table_name)
            indexes = await self.extract_indexes(table_name)
            check_constraints = await self.extract_check_constraints(table_name)
            
            # Filter FKs for this table
            table_fks = [fk for fk in all_fks if fk.table_name == table_name]
            
            # Convert columns to dict and enrich with enum/constraint info
            columns_dict = []
            for col in columns:
                col_dict = asdict(col)
                
                # Add enum values if column is USER-DEFINED type
                if col.data_type == 'USER-DEFINED':
                    # Get UDT name from pg_catalog
                    col_dict['enum_values'] = await self._get_enum_values_for_column(
                        table_name, col.column_name
                    )
                else:
                    col_dict['enum_values'] = None
                
                # Parse CHECK constraint values if any
                col_dict['check_constraint_values'] = None
                for constraint in check_constraints:
                    if col.column_name in constraint['check_clause']:
                        parsed_values = self._parse_check_constraint_values(
                            constraint['check_clause']
                        )
                        if parsed_values:
                            col_dict['check_constraint_values'] = parsed_values
                            break
                
                columns_dict.append(col_dict)
            
            # Identify special column types
            date_columns = self.identify_date_columns(columns_dict)
            status_columns = self.identify_status_columns(columns_dict)
            natural_key_candidates = self.identify_natural_key_candidates(columns_dict)
            
            # Use schema-qualified key
            schema_qualified_name = f"{self.schema_name}.{table_name}"
            
            tables_metadata[schema_qualified_name] = {
                "schema": self.schema_name,
                "table": table_name,
                "columns": columns_dict,
                "primary_keys": pks,
                "foreign_keys": [asdict(fk) for fk in table_fks],
                "indexes": [asdict(idx) for idx in indexes],
                "check_constraints": check_constraints,
                "domain": self._infer_domain(table_name),
                "date_columns": date_columns,  # NEW
                "status_columns": status_columns,  # NEW
                "natural_key_candidates": natural_key_candidates  # NEW
            }
        
        kb_schema = {
            "schema_name": self.schema_name,
            "tables": tables_metadata,
            "generated_at": None,  # Will be set during save
        }
        
        logger.info(
            "kb_schema_build_completed",
            schema=self.schema_name,
            table_count=len(tables)
        )
        
        return kb_schema
    
    async def _get_enum_values_for_column(self, table_name: str, column_name: str) -> Optional[List[str]]:
        """
        Get enum values for a USER-DEFINED column by querying pg_type and pg_enum.
        """
        query = """
            SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_namespace n ON t.typnamespace = n.oid
            JOIN information_schema.columns c ON c.udt_name = t.typname
            WHERE c.table_schema = $1
              AND c.table_name = $2
              AND c.column_name = $3
              AND n.nspname = $1
        """
        
        try:
            async with db_manager.acquire_metadata_connection() as conn:
                row = await conn.fetchrow(query, self.schema_name, table_name, column_name)
                if row and row['enum_values']:
                    return list(row['enum_values'])
        except Exception as e:
            logger.debug("enum_values_fetch_failed", table=table_name, column=column_name, error=str(e))
        
        return None
    
    def _infer_domain(self, table_name: str) -> str:
        """Infer domain/business area from table name."""
        # Simple heuristic-based domain classification
        microfinance_keywords = ['borrower', 'loan', 'repayment', 'collection', 'field_officer', 'branch']
        ecommerce_keywords = ['user', 'order', 'product', 'cart', 'payment', 'shipping']
        audit_keywords = ['history', 'audit', 'log', 'event']
        
        table_lower = table_name.lower()
        
        for keyword in microfinance_keywords:
            if keyword in table_lower:
                return "microfinance"
        
        for keyword in ecommerce_keywords:
            if keyword in table_lower:
                return "ecommerce"
        
        for keyword in audit_keywords:
            if keyword in table_lower:
                return "audit"
        
        return "general"  # Default domain
