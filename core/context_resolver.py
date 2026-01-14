"""
Context resolver for conversation continuity.
Moves decision logic from LLM to code with structured context resolution.
"""
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from enum import Enum
import re
from observability.logger import get_logger

logger = get_logger(__name__)


class ContinuationType(Enum):
    """Type of continuation for the current query."""
    NEW = "new"              # Independent new query
    REFINE = "refine"        # Refine previous query (sort, limit, filter)
    DRILLDOWN = "drilldown"  # Drill down into previous result (pronouns)


@dataclass
class Turn:
    """Single conversation turn."""
    question: str
    sql: Optional[str]
    intent_summary: Dict  # {subject, metric, time_window, grouping, ordering, limit, tables}


@dataclass
class PreservedDimensions:
    """Dimensions preserved from previous query for continuation."""
    subject: Optional[str] = None           # Primary entity (loans, branches, borrowers)
    metric: Optional[str] = None            # Main measure (sum(amount), count(*))
    time_window: Optional[str] = None       # Date range/period
    grouping: Optional[List[str]] = None    # GROUP BY dimensions
    ordering: Optional[Dict] = None         # {column: str, direction: 'ASC'/'DESC'}
    limit: Optional[int] = None             # LIMIT value
    result_scope: Optional[List] = None     # IDs from previous result for pronouns
    tables: Optional[List[str]] = None      # Tables used


@dataclass
class ResolvedContext:
    """Structured context resolution result."""
    is_related: bool
    continuation_type: ContinuationType
    anchor_turn: Optional[Turn] = None               # Most recent relevant turn
    preserved_dimensions: PreservedDimensions = field(default_factory=PreservedDimensions)
    current_question: str = ""
    refinement_instruction: Optional[str] = None     # Specific refinement detected


class ContextResolver:
    """Manages conversation context with code-based decision logic."""

    def __init__(self, max_turns: int = 5):
        self.max_turns = max_turns
        self.sessions: Dict[str, deque] = {}  # session_id -> deque of turns

    def _normalize_question(self, q: str) -> str:
        """
        Normalize question text for pattern matching.
        - Strips smart quotes and extra punctuation that can break regex matches
        - Collapses whitespace
        """
        if not q:
            return ""
        q2 = q.strip()

        # Replace smart quotes with normal quotes then strip outer quotes
        q2 = q2.replace("“", '"').replace("”", '"').replace("’", "'").replace("‘", "'")
        q2 = q2.strip().strip('"').strip("'")

        # Collapse whitespace and remove trailing punctuation noise
        q2 = re.sub(r"\s+", " ", q2)
        q2 = q2.strip().rstrip(".").rstrip("?").rstrip("!")
        return q2

    def add_turn(
        self,
        session_id: str,
        question: str,
        sql: str,
        intent_summary: Dict
    ) -> None:
        """Add a new turn to the conversation history."""
        if session_id not in self.sessions:
            self.sessions[session_id] = deque(maxlen=self.max_turns)

        turn = Turn(
            question=question,
            sql=sql,
            intent_summary=intent_summary
        )

        self.sessions[session_id].append(turn)

        logger.info(
            "turn_added",
            session_id=session_id,
            question=question[:50],
            total_turns=len(self.sessions[session_id])
        )

    def resolve_context(self, session_id: str, current_question: str) -> ResolvedContext:
        """
        Resolve context for the current question using code-based decision logic.

        Returns structured ResolvedContext with continuation type and preserved dimensions.
        """
        current_question_norm = self._normalize_question(current_question)

        logger.info(
            "resolve_context_start",
            session_id=session_id,
            question=current_question_norm,
            session_exists=session_id in self.sessions,
            turn_count=len(self.sessions.get(session_id, [])),
            resolver_instance_id=id(self),
            all_session_ids=list(self.sessions.keys())[:10]
        )

        # No session = NEW query
        if session_id not in self.sessions or not self.sessions[session_id]:
            logger.info(
                "resolve_context_result",
                session_id=session_id,
                continuation_type="NEW",
                reason="no_session_or_empty"
            )
            return ResolvedContext(
                is_related=False,
                continuation_type=ContinuationType.NEW,
                current_question=current_question_norm
            )

        turns = list(self.sessions[session_id])

        # Find most recent turn with SQL (anchor turn)
        anchor_turn = None
        for turn in reversed(turns):
            if turn.sql:
                anchor_turn = turn
                break

        logger.info(
            "anchor_turn_check",
            session_id=session_id,
            anchor_found=anchor_turn is not None,
            anchor_question=anchor_turn.question if anchor_turn else None,
            total_turns=len(turns)
        )

        # No valid anchor = NEW query
        if not anchor_turn:
            logger.info(
                "resolve_context_result",
                session_id=session_id,
                continuation_type="NEW",
                reason="no_anchor_turn_with_sql"
            )
            return ResolvedContext(
                is_related=False,
                continuation_type=ContinuationType.NEW,
                current_question=current_question_norm
            )

        # Detect refinement patterns first (highest priority)
        refinement = self._detect_refinement(current_question_norm)
        if refinement:
            logger.info(
                "resolve_context_result",
                session_id=session_id,
                continuation_type="REFINE",
                refinement_instruction=refinement,
                anchor_question=anchor_turn.question
            )
            return ResolvedContext(
                is_related=True,
                continuation_type=ContinuationType.REFINE,
                anchor_turn=anchor_turn,
                preserved_dimensions=self._extract_dimensions(anchor_turn),
                current_question=current_question_norm,
                refinement_instruction=refinement
            )

        # Detect drilldown (pronouns)
        if self._is_drilldown(current_question_norm):
            logger.info(
                "resolve_context_result",
                session_id=session_id,
                continuation_type="DRILLDOWN",
                reason="pronoun_detected"
            )
            return ResolvedContext(
                is_related=True,
                continuation_type=ContinuationType.DRILLDOWN,
                anchor_turn=anchor_turn,
                preserved_dimensions=self._extract_dimensions(anchor_turn),
                current_question=current_question_norm
            )

        # Check for general referential patterns
        if self._is_referential(current_question_norm):
            logger.info(
                "resolve_context_result",
                session_id=session_id,
                continuation_type="REFINE",
                reason="referential_keyword_detected"
            )
            return ResolvedContext(
                is_related=True,
                continuation_type=ContinuationType.REFINE,
                anchor_turn=anchor_turn,
                preserved_dimensions=self._extract_dimensions(anchor_turn),
                current_question=current_question_norm
            )

        # F3) Short follow-up rule: requires BOTH word count AND refinement pattern
        # NOTE: refinement is already handled above with an early return.
        # Keeping this block for future short-followup-specific rules without changing behavior.
        word_count = len(current_question_norm.split())
        _ = word_count  # keep variable to avoid "unused" in strict linters

        # Default: NEW query
        logger.info(
            "resolve_context_result",
            session_id=session_id,
            continuation_type="NEW",
            reason="no_pattern_matched"
        )
        return ResolvedContext(
            is_related=False,
            continuation_type=ContinuationType.NEW,
            current_question=current_question_norm
        )

    def _detect_refinement(self, question: str) -> Optional[str]:
        """
        Detect specific refinement patterns and return instruction.

        Patterns:
        - "make it N" / "increase to N" / "decrease to N" -> LIMIT change
        - "sort by X" / "order by X" -> ORDER BY change
        - "only X" / "exclude X" / "include X" -> Filter change
        """
        question_lower = (question or "").lower().strip()

        # LIMIT changes (MORE CONSERVATIVE)
        # IMPORTANT: Avoid classifying "show me 10 borrowers" as a refinement.
        # Only treat "show me 10" / "give me 10" as refinement when the message is ONLY about the number.
        limit_patterns = [
            # True refinement phrases
            (r'^(make it|increase to|decrease to|change to|set to|limit to)\s+(\d+)\s*$', 'limit_change'),

            # Just a number (common follow-up)
            (r'^(\d+)\s*$', 'limit_change'),

            # "top 10" as a follow-up (not "top 10 borrowers")
            (r'^top\s+(\d+)\s*$', 'limit_change'),

            # explicit limit follow-up
            (r'^limit\s+(\d+)\s*$', 'limit_change'),

            # "show 10" / "show me 10" / "give me 10" ONLY if it's the whole message
            (r'^(show|show me|give me)\s+(\d+)\s*(rows|results)?\s*$', 'limit_change'),
        ]

        for pattern, instruction in limit_patterns:
            if re.search(pattern, question_lower):
                return instruction

        # Metric change patterns (IMPORTANT FIX)
        # Examples:
        # - "now by outstanding balance"
        # - "by outstanding balance"
        # - "now by total collections"
        metric_patterns = [
            (r'\b(now|instead)\s+by\s+(outstanding|outstanding balance|principal|collections|repayments|loan count|number of loans)\b', 'metric_change'),
            (r'\bby\s+(outstanding|outstanding balance|principal|collections|repayments|loan count|number of loans)\b', 'metric_change'),
        ]
        for pattern, instruction in metric_patterns:
            if re.search(pattern, question_lower):
                return instruction

        # ORDER BY changes (ENHANCED - Step 5)
        order_patterns = [
            (r'\b(sort|order)\s+by\b', 'order_change'),
            (r'\b(highest|lowest|most|least)\b', 'order_change'),
            (r'\b(asc|desc|ascending|descending)\b', 'order_change'),  # NEW
        ]

        for pattern, instruction in order_patterns:
            if re.search(pattern, question_lower):
                return instruction

        # Filter changes
        filter_patterns = [
            (r'\b(only|just|exclude|include|without|with)\s+\w+', 'filter_change'),
        ]

        for pattern, instruction in filter_patterns:
            if re.search(pattern, question_lower):
                return instruction

        # F2) Time-window refinement patterns
        time_patterns = [
            (r'\b(last|past|previous)\s+\d+\s+(day|week|month|quarter|year)s?\b', 'time_window_change'),
            (r'\b(last|past|previous)\s+(day|week|month|quarter|year)\b', 'time_window_change'),  # IMPORTANT FIX: "last month"
            (r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\b', 'time_window_change'),
            (r'\bin\s+(january|february|march|april|may|june|july|august|september|october|november|december)(\s+\d{4})?\b', 'time_window_change'),
            (r'\bin\s+\d{4}\b', 'time_window_change'),
            (r'\bin\s+q[1-4](\s+\d{4})?\b', 'time_window_change'),
            (r'\b(this|current)\s+(day|week|month|quarter|year)\b', 'time_window_change'),
            (r'\b(today|yesterday)\b', 'time_window_change'),
        ]

        for pattern, instruction in time_patterns:
            if re.search(pattern, question_lower):
                return instruction

        return None

    def _is_drilldown(self, question: str) -> bool:
        """
        Detect if question is drilling down into previous results using pronouns.
        """
        question_lower = (question or "").lower()

        drilldown_patterns = [
            # F1) Strong pronouns (keep conservative, but include "their" which is needed)
            r'\b(they|them|those|these|their)\b',
            # Optional: explicit previous-reference phrases
            r'\b(from|in)\s+(the\s+)?(above|previous|prior)\s+(results?|data|rows?|query)\b',
        ]

        for pattern in drilldown_patterns:
            if re.search(pattern, question_lower):
                return True

        return False

    def _is_referential(self, question: str) -> bool:
        """
        Detect general referential patterns.
        """
        question_lower = (question or "").lower()

        referential_patterns = [
            r'\bsame\b',
            r'\bwhat about\b',
            r'\balso\b',
            r'\btoo\b',
            r'\bsimilar\b',
            r'\bsplit by\b',
            r'\bgroup by\b',
            r'\bbreak down\b',
            r'\bshow details\b',
        ]

        for pattern in referential_patterns:
            if re.search(pattern, question_lower):
                return True

        return False

    def _extract_dimensions(self, turn: Turn) -> PreservedDimensions:
        """
        Extract preserved dimensions from a turn's intent summary.
        """
        intent = turn.intent_summary

        return PreservedDimensions(
            subject=intent.get('subject'),
            metric=intent.get('metric'),
            time_window=intent.get('time_window'),
            grouping=intent.get('grouping'),
            ordering=intent.get('ordering'),
            limit=intent.get('limit'),
            result_scope=intent.get('result_scope'),
            tables=intent.get('tables', [])
        )

    def clear_session(self, session_id: str):
        """Clear context for a session."""
        if session_id in self.sessions:
            del self.sessions[session_id]


# Global context resolver instance
context_resolver = ContextResolver()
