"""
Semantic knowledge base store.
Manages kb_semantic.json with auto-append for new tables and preservation of enrichments.
"""
import json
import aiofiles
from typing import Dict, List
from pathlib import Path
import inflection
from observability.logger import get_logger
from core.config import settings

logger = get_logger(__name__)

KB_DIR = Path(__file__).parent.parent / "kb"
SEMANTIC_KB_PATH = KB_DIR / "kb_semantic.json"


def create_default_semantic_entry(table_name: str) -> dict:
    """
    Create default semantic entry for a new table.
    Uses safe defaults that require human enrichment.
    """
    # Generate aliases using inflection
    aliases = [
        table_name,
        inflection.pluralize(table_name),
        inflection.singularize(table_name),
        table_name.replace('_', ' '),
    ]
    
    # Remove duplicates while preserving order
    aliases = list(dict.fromkeys(aliases))
    
    return {
        "table_name": table_name,
        "purpose": "unknown, needs enrichment",
        "aliases": aliases,
        "pii_columns": [],
        "default_filters": [],
        "recommended_dimensions": [],
        "recommended_metrics": [],
        "join_policies": {
            "max_depth": settings.max_join_depth,
            "blocked_paths": []
        },
        "business_rules": []
    }


class SemanticStore:
    """Manages semantic knowledge base with merge and auto-append logic."""
    
    def __init__(self):
        self.semantic_kb: Dict[str, dict] = {}
        KB_DIR.mkdir(parents=True, exist_ok=True)
    
    async def load(self) -> dict:
        """Load existing kb_semantic.json if it exists."""
        if not SEMANTIC_KB_PATH.exists():
            logger.info("semantic_kb_not_found", path=str(SEMANTIC_KB_PATH))
            return {}
        
        try:
            async with aiofiles.open(SEMANTIC_KB_PATH, 'r') as f:
                content = await f.read()
                data = json.loads(content)
                
            # Convert list to dict keyed by table_name for easier lookup
            if isinstance(data, list):
                self.semantic_kb = {item['table_name']: item for item in data}
            elif isinstance(data, dict) and 'tables' in data:
                self.semantic_kb = {item['table_name']: item for item in data['tables']}
            else:
                self.semantic_kb = data
            
            logger.info(
                "semantic_kb_loaded",
                table_count=len(self.semantic_kb)
            )
            return self.semantic_kb
        except Exception as e:
            logger.error("semantic_kb_load_failed", error=str(e))
            return {}
    
    async def merge_with_schema(self, kb_schema: dict) -> dict:
        """
        Merge semantic KB with schema KB.
        
        Logic:
        1. Load existing semantic KB
        2. For each table in schema:
           - If exists in semantic: preserve all human enrichments
           - If new: auto-append with default values
        3. Optionally preserve semantic entries for tables not in schema
           (for future migration or manual additions)
        """
        existing_semantic = await self.load()
        merged_semantic = {}
        
        new_tables = []
        preserved_tables = []
        
        # Process all tables from schema (now schema-qualified)
        for schema_qualified_name, table_meta in kb_schema['tables'].items():
            # Extract unqualified table name
            # schema_qualified_name is like "core.borrowers"
            table_name = table_meta.get('table', schema_qualified_name.split('.')[-1])
            
            if table_name in existing_semantic:
                # Preserve existing enrichment
                merged_semantic[table_name] = existing_semantic[table_name]
                preserved_tables.append(table_name)
            else:
                # Auto-append new table with defaults
                merged_semantic[table_name] = create_default_semantic_entry(table_name)
                new_tables.append(table_name)
        
        logger.info(
            "semantic_kb_merged",
            total_tables=len(merged_semantic),
            new_tables=len(new_tables),
            preserved_tables=len(preserved_tables),
            new_table_names=new_tables
        )
        
        self.semantic_kb = merged_semantic
        return merged_semantic
    
    async def save(self, semantic_kb: dict = None, temp: bool = False) -> None:
        """
        Save semantic KB to file.
        
        Args:
            semantic_kb: Data to save (uses self.semantic_kb if None)
            temp: If True, saves to temp file for atomic swap
        """
        if semantic_kb is None:
            semantic_kb = self.semantic_kb
        
        # Convert to list format for cleaner JSON
        tables_list = list(semantic_kb.values())
        
        output_data = {
            "tables": tables_list,
            "metadata": {
                "table_count": len(tables_list)
            }
        }
        
        file_path = KB_DIR / "kb_semantic_temp.json" if temp else SEMANTIC_KB_PATH
        
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(output_data, indent=2))
        
        logger.info(
            "semantic_kb_saved",
            path=str(file_path),
            table_count=len(tables_list),
            is_temp=temp
        )
    
    def get_table_semantic(self, table_name: str) -> dict:
        """Get semantic metadata for a specific table."""
        return self.semantic_kb.get(table_name, create_default_semantic_entry(table_name))
    
    def get_all_aliases(self) -> Dict[str, str]:
        """
        Get mapping of all aliases to canonical table names.
        Used for NL question parsing.
        """
        alias_map = {}
        for table_name, semantic in self.semantic_kb.items():
            for alias in semantic.get('aliases', []):
                alias_map[alias.lower()] = table_name
        return alias_map
    
    def get_pii_columns(self, table_name: str) -> List[str]:
        """Get PII columns for a table (informational only)."""
        semantic = self.get_table_semantic(table_name)
        return semantic.get('pii_columns', [])
    
    def get_default_filters(self, table_name: str) -> List[dict]:
        """Get default filters for a table."""
        semantic = self.get_table_semantic(table_name)
        return semantic.get('default_filters', [])


# Global semantic store instance
semantic_store = SemanticStore()
