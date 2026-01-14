"""
Result formatter.
Formats exec results with warnings, confidence, and provenance.
"""
from dataclasses import dataclass, asdict
from typing import List, Dict
from core.safe_executor import ExecutionResult
from core.sql_validator import ValidationResult


@dataclass
class FormattedResult:
    """Complete formatted result for API response."""
    sql: str
    rows: List[Dict]
    row_count: int
    execution_time_ms: float
    warnings: List[str]
    safety_explanation: str
    confidence: float
    provenance: Dict  # {tables_used, join_depth, kb_version}
    correlation_id: str


class ResultFormatter:
    """Formats query results with metadata."""
    
    def format_result(
        self,
        execution_result: ExecutionResult,
        validation_result: ValidationResult,
        sql: str,
        confidence: float,
        provenance: Dict
    ) -> FormattedResult:
        """
        Format execution result with validation metadata.
        
        Args:
            execution_result: Result from safe_executor
            validation_result: Result from sql_validator
            sql: Final SQL that was executed
            confidence: LLM confidence score
            provenance: Metadata about query origin
        
        Returns:
            FormattedResult
        """
        return FormattedResult(
            sql=sql,
            rows=execution_result.rows,
            row_count=execution_result.row_count,
            execution_time_ms=execution_result.execution_time_ms,
            warnings=validation_result.warnings,
            safety_explanation=validation_result.safety_explanation,
            confidence=confidence,
            provenance=provenance,
            correlation_id=execution_result.correlation_id
        )
    
    def to_dict(self, result: FormattedResult) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(result)


# Global result formatter instance
result_formatter = ResultFormatter()
