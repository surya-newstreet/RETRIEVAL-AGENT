"""
Metadata cache for expensive runtime queries.

Caches results of MAX(date_col), table statistics, etc. with TTL expiry.
Invalidates on KB refresh.
"""
from typing import Optional
from datetime import datetime, date, timedelta
from db.connection import db_manager
from observability.logger import get_logger

logger = get_logger(__name__)


class MetadataCache:
    """In-memory TTL cache for database metadata queries."""
    
    def __init__(self, ttl_minutes: int = 15):
        self.ttl_minutes = ttl_minutes
        self._max_date_cache = {}  # (table, column) -> date
        self._row_estimate_cache = {}  # table -> int
        self._cache_timestamps = {}  # key -> datetime
    
    async def get_max_date(self, table: str, date_column: str) -> Optional[date]:
        """
        Get MAX(date_column) from table with caching.
        
        Args:
            table: Schema-qualified table name
            date_column: Date column name
        
        Returns:
            Maximum date or None if no data
        """
        cache_key = f"max_date:{table}:{date_column}"
        
        # Check cache
        cached_value = self._get_cached(cache_key)
        if cached_value is not None:
            return cached_value
        
        # Query database
        try:
            query = f"SELECT MAX({date_column}) AS max_date FROM {table}"
            async with db_manager.acquire_metadata_connection() as conn:
                row = await conn.fetchrow(query)
                max_date = row['max_date'] if row and row['max_date'] else None
            
            # Cache result
            self._set_cached(cache_key, max_date)
            
            logger.info(
                "max_date_queried",
                table=table,
                column=date_column,
                max_date=str(max_date)
            )
            return max_date
        
        except Exception as e:
            logger.error(
                "max_date_query_failed",
                table=table,
                column=date_column,
                error=str(e)
            )
            return None
    
    async def get_table_row_estimate(self, table: str) -> int:
        """
        Get estimated row count from pg_stat_user_tables.
        
        Args:
            table: Schema-qualified table name (schema.table)
        
        Returns:
            Estimated row count or 0 if unknown
        """
        cache_key = f"row_estimate:{table}"
        
        # Check cache
        cached_value = self._get_cached(cache_key)
        if cached_value is not None:
            return cached_value
        
        # Parse schema and table
        if '.' in table:
            schema_name, table_name = table.split('.', 1)
        else:
            schema_name = 'public'
            table_name = table
        
        # Query pg_stat
        try:
            query = """
                SELECT n_live_tup
                FROM pg_stat_user_tables
                WHERE schemaname = $1 AND relname = $2
            """
            async with db_manager.acquire_metadata_connection() as conn:
                row = await conn.fetchrow(query, schema_name, table_name)
                row_count = int(row['n_live_tup']) if row and row['n_live_tup'] else 0
            
            # Cache result
            self._set_cached(cache_key, row_count)
            
            logger.debug(
                "row_estimate_queried",
                table=table,
                row_count=row_count
            )
            return row_count
        
        except Exception as e:
            logger.error(
                "row_estimate_query_failed",
                table=table,
                error=str(e)
            )
            return 0
    
    def invalidate(self, table: Optional[str] = None):
        """
        Invalidate cache entries.
        
        Args:
            table: If provided, invalidate only entries for this table.
                   If None, invalidate all.
        """
        if table:
            # Invalidate all keys containing this table
            keys_to_remove = [
                k for k in self._cache_timestamps.keys()
                if table in k
            ]
            for key in keys_to_remove:
                self._cache_timestamps.pop(key, None)
                # Remove from all caches
                if key.startswith('max_date:'):
                    self._max_date_cache.pop(key, None)
                elif key.startswith('row_estimate:'):
                    self._row_estimate_cache.pop(key, None)
            
            logger.info("cache_invalidated_for_table", table=table, keys_removed=len(keys_to_remove))
        else:
            # Clear all
            self._max_date_cache.clear()
            self._row_estimate_cache.clear()
            self._cache_timestamps.clear()
            logger.info("cache_fully_invalidated")
    
    def _get_cached(self, key: str):
        """Get cached value if not expired."""
        if key not in self._cache_timestamps:
            return None
        
        # Check expiry
        cached_time = self._cache_timestamps[key]
        now = datetime.now()
        if now - cached_time > timedelta(minutes=self.ttl_minutes):
            # Expired
            self._cache_timestamps.pop(key)
            return None
        
        # Return cached value
        if key.startswith('max_date:'):
            return self._max_date_cache.get(key)
        elif key.startswith('row_estimate:'):
            return self._row_estimate_cache.get(key)
        
        return None
    
    def _set_cached(self, key: str, value):
        """Set cached value with timestamp."""
        self._cache_timestamps[key] = datetime.now()
        
        if key.startswith('max_date:'):
            self._max_date_cache[key] = value
        elif key.startswith('row_estimate:'):
            self._row_estimate_cache[key] = value


# Global instance
metadata_cache = MetadataCache()
