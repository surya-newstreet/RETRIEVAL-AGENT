"""
Structured logging configuration using structlog.
Provides correlation IDs, contextual logging, and JSON output.
"""
import logging
import sys
import structlog
from typing import Any
from core.config import settings


def configure_logging():
    """Configure structured logging for the application."""
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.log_level.upper()),
    )
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.log_level.upper())
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a configured logger instance."""
    return structlog.get_logger(name)


def log_query_execution(
    logger: structlog.BoundLogger,
    correlation_id: str,
    question: str,
    sql: str,
    execution_time_ms: float,
    row_count: int,
    success: bool = True,
    **extra: Any
):
    """Log query execution with structured fields."""
    logger.info(
        "query_executed",
        correlation_id=correlation_id,
        question=question,
        sql=sql,
        execution_time_ms=execution_time_ms,
        row_count=row_count,
        success=success,
        **extra
    )


def log_validation_failure(
    logger: structlog.BoundLogger,
    correlation_id: str,
    sql: str,
    errors: list[str],
    **extra: Any
):
    """Log SQL validation failure."""
    logger.warning(
        "validation_failed",
        correlation_id=correlation_id,
        sql=sql,
        errors=errors,
        **extra
    )


def log_kb_refresh(
    logger: structlog.BoundLogger,
    success: bool,
    duration_seconds: float,
    table_count: int = 0,
    error: str = None,
    **extra: Any
):
    """Log knowledge base refresh operation."""
    logger.info(
        "kb_refresh",
        success=success,
        duration_seconds=duration_seconds,
        table_count=table_count,
        error=error,
        **extra
    )


def log_clarification_request(
    logger: structlog.BoundLogger,
    correlation_id: str,
    original_question: str,
    clarification_question: str,
    partial_intent: dict,
    **extra: Any
):
    """Log clarification request."""
    logger.info(
        "clarification_requested",
        correlation_id=correlation_id,
        original_question=original_question,
        clarification_question=clarification_question,
        partial_intent=partial_intent,
        **extra
    )


# Configure logging on module import
configure_logging()
