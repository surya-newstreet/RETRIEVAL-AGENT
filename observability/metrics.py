"""
Metrics collection for monitoring system performance.
Tracks query counts, execution times, validation failures, etc.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List
from collections import defaultdict
import asyncio


@dataclass
class MetricsCollector:
    """In-memory metrics collector for system monitoring."""
    
    # Query metrics
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    
    # Clarification metrics
    clarification_requests: int = 0
    
    # Validation metrics
    validation_failures: int = 0
    validation_failure_reasons: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    # Execution metrics
    total_execution_time_ms: float = 0.0
    execution_time_samples: List[float] = field(default_factory=list)
    max_execution_time_ms: float = 0.0
    
    # KB metrics
    kb_refresh_count: int = 0
    kb_refresh_failures: int = 0
    last_kb_refresh: datetime = None
    kb_version: str = None
    
    # LLM metrics
    llm_requests: int = 0
    llm_failures: int = 0
    total_llm_time_ms: float = 0.0
    
    # RAG metrics
    rag_requests: int = 0
    rag_failures: int = 0
    total_rag_time_ms: float = 0.0
    
    def record_query(self, success: bool, execution_time_ms: float = None):
        """Record a query execution."""
        self.total_queries += 1
        if success:
            self.successful_queries += 1
            if execution_time_ms:
                self.total_execution_time_ms += execution_time_ms
                self.execution_time_samples.append(execution_time_ms)
                self.max_execution_time_ms = max(self.max_execution_time_ms, execution_time_ms)
                
                # Keep only last 1000 samples
                if len(self.execution_time_samples) > 1000:
                    self.execution_time_samples = self.execution_time_samples[-1000:]
        else:
            self.failed_queries += 1
    
    def record_clarification(self):
        """Record a clarification request."""
        self.clarification_requests += 1
    
    def record_validation_failure(self, reason: str):
        """Record a validation failure."""
        self.validation_failures += 1
        self.validation_failure_reasons[reason] += 1
    
    def record_kb_refresh(self, success: bool, version: str = None):
        """Record a KB refresh operation."""
        self.kb_refresh_count += 1
        if success:
            self.last_kb_refresh = datetime.now()
            if version:
                self.kb_version = version
        else:
            self.kb_refresh_failures += 1
    
    def record_llm_request(self, success: bool, duration_ms: float):
        """Record an LLM API request."""
        self.llm_requests += 1
        self.total_llm_time_ms += duration_ms
        if not success:
            self.llm_failures += 1
    
    def record_rag_request(self, success: bool, duration_ms: float):
        """Record a RAG retrieval request."""
        self.rag_requests += 1
        self.total_rag_time_ms += duration_ms
        if not success:
            self.rag_failures += 1
    
    def get_average_execution_time_ms(self) -> float:
        """Calculate average query execution time."""
        if not self.execution_time_samples:
            return 0.0
        return sum(self.execution_time_samples) / len(self.execution_time_samples)
    
    def get_success_rate(self) -> float:
        """Calculate query success rate."""
        if self.total_queries == 0:
            return 0.0
        return self.successful_queries / self.total_queries
    
    def get_clarification_rate(self) -> float:
        """Calculate clarification request rate."""
        if self.total_queries == 0:
            return 0.0
        return self.clarification_requests / self.total_queries
    
    def to_dict(self) -> dict:
        """Convert metrics to dictionary for API response."""
        return {
            "queries": {
                "total": self.total_queries,
                "successful": self.successful_queries,
                "failed": self.failed_queries,
                "success_rate": self.get_success_rate(),
            },
            "clarifications": {
                "total": self.clarification_requests,
                "rate": self.get_clarification_rate(),
            },
            "validation": {
                "failures": self.validation_failures,
                "failure_reasons": dict(self.validation_failure_reasons),
            },
            "execution": {
                "avg_time_ms": self.get_average_execution_time_ms(),
                "max_time_ms": self.max_execution_time_ms,
                "total_time_ms": self.total_execution_time_ms,
            },
            "kb": {
                "refresh_count": self.kb_refresh_count,
                "refresh_failures": self.kb_refresh_failures,
                "last_refresh": self.last_kb_refresh.isoformat() if self.last_kb_refresh else None,
                "version": self.kb_version,
            },
            "llm": {
                "requests": self.llm_requests,
                "failures": self.llm_failures,
                "avg_time_ms": self.total_llm_time_ms / self.llm_requests if self.llm_requests > 0 else 0.0,
            },
            "rag": {
                "requests": self.rag_requests,
                "failures": self.rag_failures,
                "avg_time_ms": self.total_rag_time_ms / self.rag_requests if self.rag_requests > 0 else 0.0,
            }
        }


# Global metrics instance
metrics = MetricsCollector()
