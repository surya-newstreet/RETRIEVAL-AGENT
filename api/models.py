"""
Pydantic models for API requests and responses.
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime


class QueryRequest(BaseModel):
    """Request to execute a natural language query."""
    question: str = Field(..., description="Natural language question")
    session_id: Optional[str] = Field(None, description="Session ID for context")


class ClarificationResponse(BaseModel):
    """Response to a clarification question."""
    original_question: str = Field(..., description="Original NL question")
    clarification_answer: str = Field(..., description="User's answer to clarification")
    partial_intent: Dict = Field(..., description="Partial intent from clarification request")
    session_id: str = Field(..., description="Session ID")


class QueryResponse(BaseModel):
    """Response for a query (either results or clarification)."""
    # Clarification fields
    needs_clarification: bool = False
    clarification_question: Optional[str] = None
    partial_intent: Optional[Dict] = None

    # Result fields
    sql: Optional[str] = None
    rows: Optional[List[Dict]] = None
    row_count: Optional[int] = None
    execution_time_ms: Optional[float] = None
    warnings: List[str] = Field(default_factory=list)  # FIX: avoid mutable default list
    safety_explanation: Optional[str] = None
    confidence: Optional[float] = None
    provenance: Optional[Dict] = None
    refusal_message: Optional[str] = None  # NEW: For blocked operations

    # Metadata
    correlation_id: str
    session_id: Optional[str] = None


class HealthStatus(BaseModel):
    """Health status of the system."""
    status: str
    timestamp: datetime
    db_metadata_pool: Dict
    db_query_pool: Dict
    kb_status: Dict


class KBStatus(BaseModel):
    """Knowledge base status."""
    last_refresh: Optional[datetime] = None
    next_refresh: Optional[datetime] = None
    status: str
    version: Optional[str] = None
    table_count: int = 0
    error: Optional[str] = None
    is_refreshing: bool = False


class MetricsResponse(BaseModel):
    """System metrics."""
    queries: Dict
    clarifications: Dict
    validation: Dict
    execution: Dict
    kb: Dict
    llm: Dict
