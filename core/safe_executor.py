"""
Safe SQL executor with defense-in-depth.
Enforces read-only execution with timeouts and row limits.
"""
from dataclasses import dataclass
from typing import List, Dict, Optional
import time
from db.connection import db_manager
from observability.logger import get_logger
from core.config import settings

logger = get_logger(__name__)


@dataclass
class ExecutionResult:
    """Result of SQL execution."""
    rows: List[Dict]
    row_count: int
    execution_time_ms: float
    correlation_id: str


class SafeExecutor:
    """Executes validated SQL with defense-in-depth security."""
    
    async def execute_query(
        self,
        sql: str,
        correlation_id: str,
        timeout_seconds: Optional[int] = None
    ) -> ExecutionResult:
        """
        Execute validated SQL query with safety enforcement.
        
        Defense-in-depth justification:
        Validation is logic; execution is power. Defense-in-depth required because:
        - Validators can have bugs or edge cases
        - Parser mismatches between sqlglot and PostgreSQL
        - Expensive-but-valid queries can still exhaust resources
        - Unknown PostgreSQL extensions or functions might bypass validation
        
        Therefore, execution MUST enforce:
        - Read-only transaction mode
        - Statement timeout (kills long-running queries)
        - Connection-level timeout
        - Row limit already enforced by validator
        
        Args:
            sql: Validated SQL query
            correlation_id: For tracing
            timeout_seconds: Override default timeout
        
        Returns:
            ExecutionResult with rows and metadata
        """
        timeout = timeout_seconds or settings.statement_timeout_seconds
        timeout_ms = int(timeout * 1000)
        start_time = time.time()
        
        try:
            async with db_manager.acquire_query_connection() as conn:
                # Defense-in-depth: Set read-only transaction
                await conn.execute("BEGIN TRANSACTION READ ONLY")
                
                try:
                    # Defense-in-depth: enforce statement timeout at session/transaction scope
                    # SET LOCAL applies only for the current transaction.
                    # Option A: set as integer milliseconds
                    await conn.execute(f"SET LOCAL statement_timeout = '{timeout_ms}ms'")

                    
                    # Execute query
                    rows = await conn.fetch(sql)
                    
                    # Commit read-only transaction
                    await conn.execute("COMMIT")
                    
                    # Convert to list of dicts
                    result_rows = [dict(row) for row in rows]
                    execution_time_ms = (time.time() - start_time) * 1000
                    
                    logger.info(
                        "query_executed",
                        correlation_id=correlation_id,
                        row_count=len(result_rows),
                        execution_time_ms=round(execution_time_ms, 2)
                    )
                    
                    return ExecutionResult(
                        rows=result_rows,
                        row_count=len(result_rows),
                        execution_time_ms=execution_time_ms,
                        correlation_id=correlation_id
                    )
                    
                except Exception:
                    # Rollback on error
                    await conn.execute("ROLLBACK")
                    raise
                    
        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            
            logger.error(
                "query_execution_failed",
                correlation_id=correlation_id,
                error=str(e),
                sql=sql[:200],  # Log first 200 chars
                execution_time_ms=round(execution_time_ms, 2)
            )
            
            # Sanitize error message (don't expose internal details)
            error_msg = self._sanitize_error(str(e))
            raise RuntimeError(f"Query execution failed: {error_msg}")
    
    def _sanitize_error(self, error: str) -> str:
        """
        Sanitize error messages to avoid leaking internal details.
        Returns user-friendly error message.
        """
        error_lower = error.lower()
        
        if "timeout" in error_lower or "statement timeout" in error_lower:
            return "Query execution time limit exceeded. Try adding more filters to reduce result size."
        
        if "connection" in error_lower:
            return "Database connection error. Please try again."
        
        if "syntax" in error_lower:
            return "SQL syntax error. Please rephrase your question."
        
        # Generic error for anything else
        return "An error occurred while executing the query. Please try rephrasing your question."


# Global safe executor instance
safe_executor = SafeExecutor()
