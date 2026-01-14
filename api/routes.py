"""
API routes for NL to SQL system.
"""
import uuid
from datetime import datetime
from fastapi import APIRouter, HTTPException
from api.models import (
    QueryRequest,
    ClarificationResponse,
    QueryResponse,
    HealthStatus,
    KBStatus,
    MetricsResponse
)
from core.context_resolver import context_resolver
from core.llm_sql_generator import llm_sql_generator
from core.sql_validator import SQLValidator
from core.safe_executor import safe_executor
from core.result_formatter import result_formatter
from core.rules_compiler import rules_compiler
from scheduler.kb_refresh import kb_scheduler
from db.connection import db_manager
from observability.logger import get_logger, log_query_execution
from observability.metrics import metrics

logger = get_logger(__name__)
router = APIRouter()


@router.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest) -> QueryResponse:
    """
    Execute a natural language query.
    Returns either results or a clarification request.
    """
    correlation_id = str(uuid.uuid4())
    session_id = request.session_id or str(uuid.uuid4())

    # DEBUG LOGGING (Step 1: API logging)
    logger.info(
        "query_received",
        correlation_id=correlation_id,
        session_id=session_id,
        question=request.question
    )

    # Additional debug logging for context debugging
    # IMPORTANT FIX: include resolver instance id so you can detect silent reloads in logs.
    logger.info(
        "query_debug_info",
        correlation_id=correlation_id,
        session_id=session_id,
        question_length=len(request.question),
        session_exists=(session_id in context_resolver.sessions),
        turns_count=len(context_resolver.sessions.get(session_id, [])),
        resolver_instance_id=id(context_resolver)
    )

    try:
        # Load compiled rules
        compiled_rules = await rules_compiler.load_compiled_rules()
        if not compiled_rules:
            raise HTTPException(
                status_code=503,
                detail="Knowledge base not initialized. Please wait for KB refresh."
            )

        # Resolve conversation context (code-based decision logic)
        resolved_context = context_resolver.resolve_context(session_id, request.question)

        # Generate SQL with structured context
        sql_result = await llm_sql_generator.generate_sql(
            question=request.question,
            compiled_rules=compiled_rules,
            correlation_id=correlation_id,
            resolved_context=resolved_context
        )

        # Check for read-only refusal (NEW)
        if sql_result.clarification and sql_result.clarification.partial_intent.get('refusal') == 'read_only_system':
            logger.warning(
                "read_only_refusal",
                correlation_id=correlation_id,
                question=request.question
            )
            return QueryResponse(
                needs_clarification=False,
                refusal_message="This system is read-only. DELETE/UPDATE/INSERT/DDL operations are not allowed.",
                correlation_id=correlation_id,
                session_id=session_id
            )

        # Check if clarification is needed
        if sql_result.clarification and sql_result.clarification.needs_clarification:
            metrics.record_clarification()

            return QueryResponse(
                needs_clarification=True,
                clarification_question=sql_result.clarification.clarification_question,
                partial_intent=sql_result.clarification.partial_intent,
                correlation_id=correlation_id,
                session_id=session_id
            )

        # IMPORTANT FIX: defensive guard (prevents validator crash if LLM returns empty sql)
        if not sql_result.sql or not str(sql_result.sql).strip():
            metrics.record_query(success=False)
            return QueryResponse(
                sql=sql_result.sql,
                warnings=["Empty SQL generated"],
                correlation_id=correlation_id,
                session_id=session_id,
                rows=[],
                row_count=0,
                execution_time_ms=0.0,
                safety_explanation="SQL generation returned empty SQL. Please rephrase your question."
            )

        # Validate SQL
        validator = SQLValidator(compiled_rules)
        validation_result = await validator.validate_sql(sql_result.sql, correlation_id)

        if not validation_result.is_valid:
            metrics.record_query(success=False)
            return QueryResponse(
                sql=sql_result.sql,
                warnings=validation_result.warnings,
                correlation_id=correlation_id,
                session_id=session_id,
                rows=[],
                row_count=0,
                execution_time_ms=0.0,
                safety_explanation=f"Validation failed: {'; '.join(validation_result.errors)}"
            )

        # Execute query
        execution_result = await safe_executor.execute_query(
            sql=validation_result.sql,
            correlation_id=correlation_id
        )

        # Format result
        provenance = {
            "tables_used": sql_result.tables_used,
            "kb_version": compiled_rules.get('version'),
            "correlation_id": correlation_id
        }

        formatted = result_formatter.format_result(
            execution_result=execution_result,
            validation_result=validation_result,
            sql=validation_result.sql,
            confidence=sql_result.confidence,
            provenance=provenance
        )

        # Add to context
        context_resolver.add_turn(
            session_id=session_id,
            question=request.question,
            sql=formatted.sql,
            intent_summary=sql_result.intent_summary
        )

        # DEBUG: Log stored turns count (Step 3)
        stored_turns_count = len(context_resolver.sessions.get(session_id, []))
        logger.info(
            "context_stored",
            correlation_id=correlation_id,
            session_id=session_id,
            stored_turns_count=stored_turns_count,
            latest_question=request.question[:50],
            resolver_instance_id=id(context_resolver)
        )

        # Log and record metrics
        log_query_execution(
            logger,
            correlation_id,
            request.question,
            formatted.sql,
            formatted.execution_time_ms,
            formatted.row_count,
            success=True
        )
        metrics.record_query(success=True, execution_time_ms=formatted.execution_time_ms)

        return QueryResponse(
            sql=formatted.sql,
            rows=formatted.rows,
            row_count=formatted.row_count,
            execution_time_ms=formatted.execution_time_ms,
            warnings=formatted.warnings,
            safety_explanation=formatted.safety_explanation,
            confidence=formatted.confidence,
            provenance=formatted.provenance,
            correlation_id=correlation_id,
            session_id=session_id
        )

    except Exception as e:
        logger.error("query_failed", correlation_id=correlation_id, error=str(e))
        metrics.record_query(success=False)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clarify", response_model=QueryResponse)
async def clarify(request: ClarificationResponse) -> QueryResponse:
    """
    Handle clarification response and generate SQL.
    """
    correlation_id = str(uuid.uuid4())

    logger.info(
        "clarification_received",
        correlation_id=correlation_id,
        session_id=request.session_id,
        original_question=request.original_question,
        clarification_answer=request.clarification_answer
    )

    try:
        # Load compiled rules
        compiled_rules = await rules_compiler.load_compiled_rules()
        if not compiled_rules:
            raise HTTPException(status_code=503, detail="Knowledge base not initialized")

        # Resolve context (code-based decision logic)
        resolved_context = context_resolver.resolve_context(request.session_id, request.original_question)

        # Generate SQL with clarification and structured context
        sql_result = await llm_sql_generator.generate_sql(
            question=request.original_question,
            compiled_rules=compiled_rules,
            correlation_id=correlation_id,
            resolved_context=resolved_context,
            clarification_answer=request.clarification_answer,
            partial_intent=request.partial_intent
        )

        # DEBUG: Log what we got from SQL generator
        logger.info(
            "clarification_sql_generated_debug",
            correlation_id=correlation_id,
            sql_type=str(type(sql_result.sql)),
            sql_value=str(sql_result.sql)[:200] if sql_result.sql else "None",
            has_sql=sql_result.sql is not None
        )

        # IMPORTANT FIX: defensive guard (prevents validator crash if LLM returns empty sql)
        if not sql_result.sql or not str(sql_result.sql).strip():
            metrics.record_query(success=False)
            raise HTTPException(
                status_code=400,
                detail="SQL generation returned empty SQL during clarification. Please answer with a table/entity and optional filters."
            )

        # Validate and execute (same as /query)
        validator = SQLValidator(compiled_rules)

        # DEBUG: Log before validation
        logger.info("about_to_validate", sql_type=str(type(sql_result.sql)))

        validation_result = await validator.validate_sql(sql_result.sql, correlation_id)

        if not validation_result.is_valid:
            metrics.record_query(success=False)
            raise HTTPException(
                status_code=400,
                detail=f"Validation failed: {'; '.join(validation_result.errors)}"
            )

        execution_result = await safe_executor.execute_query(
            sql=validation_result.sql,
            correlation_id=correlation_id
        )

        provenance = {
            "tables_used": sql_result.tables_used,
            "kb_version": compiled_rules.get('version'),
            "correlation_id": correlation_id
        }

        formatted = result_formatter.format_result(
            execution_result=execution_result,
            validation_result=validation_result,
            sql=validation_result.sql,
            confidence=sql_result.confidence,
            provenance=provenance
        )

        # Add to context - CRITICAL: Store clarified version, not original vague question
        # This ensures follow-ups use the clarified anchor, not the vague one
        clarified_question = f"{request.original_question} [clarified: {request.clarification_answer}]"

        context_resolver.add_turn(
            session_id=request.session_id,
            question=clarified_question,
            sql=formatted.sql,
            intent_summary=sql_result.intent_summary
        )

        # IMPORTANT FIX: add context log here too (parity with /query)
        stored_turns_count = len(context_resolver.sessions.get(request.session_id, []))
        logger.info(
            "context_stored",
            correlation_id=correlation_id,
            session_id=request.session_id,
            stored_turns_count=stored_turns_count,
            latest_question=clarified_question[:50],
            resolver_instance_id=id(context_resolver)
        )

        log_query_execution(
            logger,
            correlation_id,
            request.original_question,
            formatted.sql,
            formatted.execution_time_ms,
            formatted.row_count,
            success=True
        )
        metrics.record_query(success=True, execution_time_ms=formatted.execution_time_ms)

        return QueryResponse(
            sql=formatted.sql,
            rows=formatted.rows,
            row_count=formatted.row_count,
            execution_time_ms=formatted.execution_time_ms,
            warnings=formatted.warnings,
            safety_explanation=formatted.safety_explanation,
            confidence=formatted.confidence,
            provenance=formatted.provenance,
            correlation_id=correlation_id,
            session_id=request.session_id
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("clarification_failed", correlation_id=correlation_id, error=str(e))
        metrics.record_query(success=False)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", response_model=HealthStatus)
async def health() -> HealthStatus:
    """Health check endpoint."""
    db_health = await db_manager.health_check()
    kb_status = kb_scheduler.get_status()

    overall_status = "healthy"
    if db_health['metadata_pool']['status'] != 'healthy' or \
       db_health['query_pool']['status'] != 'healthy':
        overall_status = "degraded"

    if kb_status['status'] == 'failed':
        overall_status = "degraded"

    return HealthStatus(
        status=overall_status,
        timestamp=datetime.now(),
        db_metadata_pool=db_health['metadata_pool'],
        db_query_pool=db_health['query_pool'],
        kb_status=kb_status
    )


@router.get("/kb-status", response_model=KBStatus)
async def kb_status() -> KBStatus:
    """Get knowledge base refresh status."""
    status = kb_scheduler.get_status()

    # Load compiled rules to get table count
    compiled_rules = await rules_compiler.load_compiled_rules()
    table_count = len(compiled_rules.get('tables', {})) if compiled_rules else 0

    return KBStatus(
        last_refresh=datetime.fromisoformat(status['last_refresh']) if status['last_refresh'] else None,
        next_refresh=datetime.fromisoformat(status['next_refresh']) if status['next_refresh'] else None,
        status=status['status'],
        version=status['version'],
        table_count=table_count,
        error=status['error'],
        is_refreshing=status['is_refreshing']
    )


@router.get("/metrics", response_model=MetricsResponse)
async def get_metrics() -> MetricsResponse:
    """Get system metrics."""
    return MetricsResponse(**metrics.to_dict())
