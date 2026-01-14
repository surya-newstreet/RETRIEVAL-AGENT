"""
Database connection pool management with two-role strategy.

Roles:
- Metadata Role: Read-only access to information_schema and pg_catalog
- Query Role: Read-only SELECT access to core.* schema with strict timeouts
"""
import asyncpg
from typing import Optional
from contextlib import asynccontextmanager
from core.config import settings
from observability.logger import get_logger

logger = get_logger(__name__)


class DatabaseManager:
    """Manages database connection pools for metadata and query operations."""
    
    def __init__(self):
        self.metadata_pool: Optional[asyncpg.Pool] = None
        self.query_pool: Optional[asyncpg.Pool] = None

    # --- Small compatibility helpers (avoids crashing across asyncpg versions) ---
    def _pool_size(self, pool: Optional[asyncpg.Pool]) -> int:
        if not pool:
            return 0
        if hasattr(pool, "get_size"):
            return pool.get_size()
        # Fallback: best-effort
        if hasattr(pool, "get_max_size"):
            return pool.get_max_size()
        return 0

    def _pool_free(self, pool: Optional[asyncpg.Pool]) -> int:
        if not pool:
            return 0
        # asyncpg uses get_free_size() (some folks mistakenly use get_idle_size())
        if hasattr(pool, "get_free_size"):
            return pool.get_free_size()
        if hasattr(pool, "get_idle_size"):
            return pool.get_idle_size()
        return 0
    
    async def init_metadata_pool(self):
        """
        Initialize connection pool for metadata extraction.
        This pool is used for KB generation from information_schema/pg_catalog.
        """
        try:
            # Backward-compatible "two-role" creds (falls back to db_user/db_password if not present)
            meta_user = getattr(settings, "db_metadata_user", settings.db_user)
            meta_password = getattr(settings, "db_metadata_password", settings.db_password)

            self.metadata_pool = await asyncpg.create_pool(
                host=settings.db_host,
                port=settings.db_port,
                user=meta_user,
                password=meta_password,
                database=settings.db_name,
                min_size=2,
                max_size=5,
                command_timeout=60,
                server_settings={
                    "application_name": "nl_to_sql_metadata",
                },
            )
            logger.info("metadata_pool_initialized", pool_size=self._pool_size(self.metadata_pool))
        except Exception as e:
            logger.error("metadata_pool_init_failed", error=str(e))
            raise
    
    async def init_query_pool(self):
        """
        Initialize connection pool for user query execution.
        Enforces read-only mode and strict timeouts.
        """
        try:
            # Backward-compatible "two-role" creds (falls back to db_user/db_password if not present)
            query_user = getattr(settings, "db_query_user", settings.db_user)
            query_password = getattr(settings, "db_query_password", settings.db_password)

            self.query_pool = await asyncpg.create_pool(
                host=settings.db_host,
                port=settings.db_port,
                user=query_user,
                password=query_password,
                database=settings.db_name,
                min_size=5,
                max_size=20,
                command_timeout=settings.statement_timeout_seconds,
                server_settings={
                    "application_name": "nl_to_sql_query",
                },
            )
            logger.info("query_pool_initialized", pool_size=self._pool_size(self.query_pool))
        except Exception as e:
            logger.error("query_pool_init_failed", error=str(e))
            raise
    
    @asynccontextmanager
    async def acquire_metadata_connection(self):
        """Acquire a connection from the metadata pool."""
        if not self.metadata_pool:
            raise RuntimeError("Metadata pool not initialized. Call init_metadata_pool() first.")
        async with self.metadata_pool.acquire() as conn:
            yield conn
    
    @asynccontextmanager
    async def acquire_query_connection(self):
        """
        Acquire a connection from the query pool with read-only enforcement.
        Sets session-level timeouts and read-only mode.
        """
        if not self.query_pool:
            raise RuntimeError("Query pool not initialized. Call init_query_pool() first.")
        async with self.query_pool.acquire() as conn:
            # Defense-in-depth: session defaults
            # (SafeExecutor still uses BEGIN TRANSACTION READ ONLY and SET LOCAL statement_timeout too.)
            await conn.execute("SET default_transaction_read_only = on")
            await conn.execute(f"SET statement_timeout = '{settings.statement_timeout_seconds}s'")
            await conn.execute("SET idle_in_transaction_session_timeout = '60s'")
            yield conn
    
    async def close_pools(self):
        """Close all connection pools gracefully."""
        if self.metadata_pool:
            await self.metadata_pool.close()
            logger.info("metadata_pool_closed")
            self.metadata_pool = None
        
        if self.query_pool:
            await self.query_pool.close()
            logger.info("query_pool_closed")
            self.query_pool = None
    
    async def health_check(self) -> dict:
        """
        Perform health check on both pools.
        Returns status and connection counts.
        """
        metadata_status = "unknown"
        query_status = "unknown"
        
        try:
            if self.metadata_pool:
                async with self.metadata_pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                metadata_status = "healthy"
            else:
                metadata_status = "not_initialized"
        except Exception as e:
            logger.error("metadata_pool_health_check_failed", error=str(e))
            metadata_status = "unhealthy"
        
        try:
            if self.query_pool:
                async with self.query_pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                query_status = "healthy"
            else:
                query_status = "not_initialized"
        except Exception as e:
            logger.error("query_pool_health_check_failed", error=str(e))
            query_status = "unhealthy"
        
        return {
            "metadata_pool": {
                "status": metadata_status,
                "size": self._pool_size(self.metadata_pool),
                "free": self._pool_free(self.metadata_pool),
            },
            "query_pool": {
                "status": query_status,
                "size": self._pool_size(self.query_pool),
                "free": self._pool_free(self.query_pool),
            }
        }


# Global database manager instance
db_manager = DatabaseManager()
