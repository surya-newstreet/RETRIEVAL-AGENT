"""
LLM SQL Generator.
Converts natural language questions to SQL using schema-grounded prompting.
Uses code-based context resolution instead of LLM decisions.
"""
from dataclasses import dataclass
from typing import Optional, Dict, List
import json
import re  # For deterministic refinement pattern matching
from llm.groq_client import llm_provider
from core.context_resolver import ResolvedContext, ContinuationType
from core.retrieval.kb_retriever import retrieve_kb_context
from core.prompt_builder import build_enhanced_sql_prompt
from observability.logger import get_logger, log_clarification_request
from observability.metrics import metrics
import time

logger = get_logger(__name__)


@dataclass
class ClarificationRequest:
    """Clarification needed for incomplete intent."""

    needs_clarification: bool
    clarification_question: str
    original_question: str
    partial_intent: Dict


@dataclass
class SQLGenerationResult:
    """Result of SQL generation."""

    sql: Optional[str]
    confidence: float
    tables_used: List[str]
    intent_summary: Dict
    clarification: Optional[ClarificationRequest] = None


class LLMSQLGenerator:
    """Generates SQL from natural language using LLM with schema grounding."""

    # ========================================================================
    # DETERMINISTIC REFINEMENT HELPERS (Step 7)
    # ========================================================================

    def _parse_limit_value(self, question: str) -> Optional[int]:
        """Extract limit number from questions like 'make it 5', 'show 10', 'top 3'."""
        question_lower = question.lower().strip()

        patterns = [
            r"\b(make it|increase to|decrease to|change to|set to|limit to|show me|give me)\s+(\d+)\b",
            r"^(\d+)$",  # Just a number
            r"\btop\s+(\d+)\b",
            r"\blimit\s+(\d+)\b",
            r"\bshow\s+(\d+)\b",
        ]

        for pattern in patterns:
            match = re.search(pattern, question_lower)
            if match:
                # Find the number in the match groups
                for group in match.groups():
                    if group and group.isdigit():
                        return int(group)

        return None

    def _rewrite_limit(self, sql: str, new_limit: int) -> str:
        """Rewrite SQL to change LIMIT value."""
        if not sql:
            return sql

        # Pattern to match LIMIT clause
        limit_pattern = r"\bLIMIT\s+\d+\b"

        if re.search(limit_pattern, sql, re.IGNORECASE):
            # Replace existing LIMIT
            new_sql = re.sub(
                limit_pattern, f"LIMIT {new_limit}", sql, flags=re.IGNORECASE
            )
        else:
            # Add LIMIT if not present
            # Find the end of the query (before semicolon if present)
            sql_stripped = sql.rstrip(";").rstrip()
            new_sql = f"{sql_stripped}\nLIMIT {new_limit}"

        return new_sql

    def _parse_order_clause(self, question: str) -> Optional[Dict[str, str]]:
        """Extract ORDER BY info from questions like 'sort by X desc'."""
        question_lower = question.lower().strip()

        # Match: "sort by column_name asc/desc"
        pattern = (
            r"\b(?:sort|order)\s+by\s+([\w_]+)(?:\s+(asc|desc|ascending|descending))?\b"
        )
        match = re.search(pattern, question_lower)

        if match:
            column = match.group(1)
            direction = match.group(2)
            if direction:
                direction = "ASC" if direction.startswith("asc") else "DESC"
            else:
                direction = "DESC"  # Default to DESC if not specified

            return {"column": column, "direction": direction}

        return None

    def _rewrite_order(self, sql: str, order_info: Dict[str, str]) -> str:
        """Rewrite SQL to change ORDER BY clause."""
        if not sql or not order_info:
            return sql

        column = order_info["column"]
        direction = order_info["direction"]
        new_order_clause = f"ORDER BY {column} {direction}"

        # Pattern to match existing ORDER BY
        order_pattern = r"\bORDER\s+BY\s+[\w_.]+\s+(?:ASC|DESC)\b"

        if re.search(order_pattern, sql, re.IGNORECASE):
            # Replace existing ORDER BY
            new_sql = re.sub(order_pattern, new_order_clause, sql, flags=re.IGNORECASE)
        else:
            # Add ORDER BY before LIMIT if present, otherwise at end
            limit_match = re.search(r"(\bLIMIT\s+\d+)", sql, re.IGNORECASE)
            if limit_match:
                # Insert ORDER BY before LIMIT
                new_sql = sql.replace(
                    limit_match.group(1), f"{new_order_clause}\n{limit_match.group(1)}"
                )
            else:
                # Add at end
                sql_stripped = sql.rstrip(";").rstrip()
                new_sql = f"{sql_stripped}\n{new_order_clause}"

        return new_sql

    # ========================================================================
    # MAIN SQL GENERATION
    # ========================================================================

    async def generate_sql(
        self,
        question: str,
        compiled_rules: dict,
        correlation_id: str,
        resolved_context: Optional[ResolvedContext] = None,
        clarification_answer: Optional[str] = None,
        partial_intent: Optional[Dict] = None,
    ) -> SQLGenerationResult:
        """
        Generate SQL from natural language question with code-based context resolution.

        Args:
            question: User's NL question
            compiled_rules: Runtime KB
            correlation_id: For tracing
            resolved_context: Structured context from context_resolver
            clarification_answer: User's answer to clarification
            partial_intent: Partial intent from previous clarification

        Returns:
            SQLGenerationResult with SQL or clarification request
        """
        # Step 0: Check for modification keywords (read-only enforcement)
        # NOTE (IMPORTANT FIX): Do NOT treat natural-language "change" / "modify" as DB writes.
        # Only block actual SQL write/DDL intent.
        modification_keywords = [
            "delete",
            "remove",
            "drop",
            "update",
            "insert",
            "add row",
            "create table",
            "alter",
            "truncate",
            "grant",
            "revoke",
        ]
        question_lower = question.lower()

        if any(
            re.search(rf"\b{re.escape(keyword)}\b", question_lower)
            for keyword in modification_keywords
        ):
            logger.warning(
                "modification_request_blocked",
                correlation_id=correlation_id,
                question=question,
            )

            return SQLGenerationResult(
                sql=None,
                confidence=0.0,
                tables_used=[],
                intent_summary={},
                clarification=ClarificationRequest(
                    needs_clarification=False,
                    clarification_question="",
                    original_question=question,
                    partial_intent={"refusal": "read_only_system"},
                ),
            )

        # Step 0.5: DETERMINISTIC REFINEMENT (Step 7 - CRITICAL FIX)
        # For simple refinements, don't call LLM - just rewrite SQL directly
        # IMPORTANT FIX: Gate deterministic rewrites by refinement_instruction to prevent accidental rewrites.
        if resolved_context and resolved_context.is_related:
            prev_sql = (
                resolved_context.anchor_turn.sql
                if resolved_context.anchor_turn
                else None
            )
            refinement_type = (
                resolved_context.refinement_instruction
            )  # e.g. "limit_change", "order_change", "filter_change", etc.

            if prev_sql:
                # Try LIMIT change only when resolver indicates limit_change
                if refinement_type == "limit_change":
                    new_limit = self._parse_limit_value(question)
                    if new_limit:
                        logger.info(
                            "deterministic_limit_refinement",
                            correlation_id=correlation_id,
                            prev_sql_length=len(prev_sql),
                            new_limit=new_limit,
                        )

                        new_sql = self._rewrite_limit(prev_sql, new_limit)

                        # Reuse previous intent but update limit
                        intent_summary = (
                            dict(resolved_context.anchor_turn.intent_summary)
                            if resolved_context.anchor_turn.intent_summary
                            else {}
                        )
                        intent_summary["limit"] = new_limit

                        return SQLGenerationResult(
                            sql=new_sql,
                            confidence=0.99,  # High confidence for deterministic rewrite
                            tables_used=resolved_context.preserved_dimensions.tables
                            or [],
                            intent_summary=intent_summary,
                            clarification=None,
                        )

                # Try ORDER BY change only when resolver indicates order_change
                if refinement_type == "order_change":
                    order_info = self._parse_order_clause(question)
                    if order_info:
                        logger.info(
                            "deterministic_order_refinement",
                            correlation_id=correlation_id,
                            order_column=order_info["column"],
                            order_direction=order_info["direction"],
                        )

                        new_sql = self._rewrite_order(prev_sql, order_info)

                        # Reuse previous intent but update ordering
                        intent_summary = (
                            dict(resolved_context.anchor_turn.intent_summary)
                            if resolved_context.anchor_turn.intent_summary
                            else {}
                        )
                        intent_summary["ordering"] = order_info

                        return SQLGenerationResult(
                            sql=new_sql,
                            confidence=0.99,
                            tables_used=resolved_context.preserved_dimensions.tables
                            or [],
                            intent_summary=intent_summary,
                            clarification=None,
                        )

        # Step 1: Skip clarification for continuation/refinement queries
        # Only ask clarification for NEW queries without clarification_answer
        should_check_clarification = not clarification_answer and (
            not resolved_context
            or resolved_context.continuation_type == ContinuationType.NEW
        )

        if should_check_clarification:
            clarification = self._detect_incomplete_intent(
                question, None, compiled_rules
            )

            if clarification.needs_clarification:
                log_clarification_request(
                    logger,
                    correlation_id,
                    question,
                    clarification.clarification_question,
                    clarification.partial_intent,
                )

                return SQLGenerationResult(
                    sql=None,
                    confidence=0.0,
                    tables_used=[],
                    intent_summary=clarification.partial_intent,
                    clarification=clarification,
                )

        # Step 2: Retrieve relevant KB context using RAG
        rag_start = time.time()
        try:
            # IMPORTANT FIX: Provide context hints to retrieval to reduce table drift.
            conversation_context = None
            if (
                resolved_context
                and resolved_context.is_related
                and resolved_context.preserved_dimensions
                and resolved_context.preserved_dimensions.tables
            ):
                conversation_context = resolved_context.preserved_dimensions.tables

            kb_context = retrieve_kb_context(
                question=question,
                compiled_rules=compiled_rules,
                conversation_context=conversation_context,  # Was None earlier; now passes preserved table hints when available
                partial_intent=partial_intent,
                clarification_answer=clarification_answer,
            )
            rag_duration_ms = (time.time() - rag_start) * 1000
            metrics.record_rag_request(success=True, duration_ms=rag_duration_ms)
        except Exception as e:
            rag_duration_ms = (time.time() - rag_start) * 1000
            metrics.record_rag_request(success=False, duration_ms=rag_duration_ms)
            logger.error(
                "rag_retrieval_error", error=str(e), correlation_id=correlation_id
            )
            # Fallback: kb_context will be minimal from retrieve_kb_context's internal fallback
            kb_context = retrieve_kb_context(
                question=question,
                compiled_rules=compiled_rules,
                conversation_context=None,
                partial_intent=None,
                clarification_answer=None,
            )

        # Step 3: Build prompt with schema grounding using ResolvedContext
        prompt = self._build_sql_prompt(
            question=question,
            kb_context=kb_context,
            resolved_context=resolved_context,
            clarification_answer=clarification_answer,
            partial_intent=partial_intent,
        )

        # Step 4: Generate SQL from LLM
        try:
            response = await llm_provider.generate_structured_completion(
                prompt=prompt, temperature=0.0
            )

            sql = response.get("sql", "").strip()
            confidence = response.get("confidence", 0.0)
            tables_used = response.get("tables_used", [])
            intent_summary = response.get("intent_summary", {})

            logger.info(
                "sql_generated",
                correlation_id=correlation_id,
                confidence=confidence,
                tables_used=tables_used,
                sql_length=len(sql),
                sql_empty=len(sql) == 0,
                response_keys=list(response.keys()),
                prompt_length=len(prompt),
            )

            # DEBUG: Log if SQL is empty
            if not sql:
                logger.warning(
                    "empty_sql_generated",
                    correlation_id=correlation_id,
                    response=response,
                    prompt_excerpt=prompt[:500],
                )

            return SQLGenerationResult(
                sql=sql,
                confidence=confidence,
                tables_used=tables_used,
                intent_summary=intent_summary,
                clarification=None,
            )

        except Exception as e:
            logger.error(
                "sql_generation_failed", correlation_id=correlation_id, error=str(e)
            )
            raise

    def _detect_incomplete_intent(
        self, question: str, _unused_context, compiled_rules: dict
    ) -> ClarificationRequest:
        """
        Detect if question has incomplete intent and needs clarification.

        IMPORTANT: Keeping the earlier logic (as-is) below for reference, but the actual
        executed logic is corrected and made deterministic + safe.
        """
        # --- LEGACY (kept, not deleted) ---
        # q = question.strip().lower()
        #
        # # If it's a continuation/refinement, NEVER clarify here
        # # (clarify handled elsewhere)
        #
        # # strongly vague
        # vague_phrases = {"show me data", "show data", "show details", "show info", "give me data", "tell me data"}
        # if q in vague_phrases:
        #     return ClarificationRequest(
        #         needs_clarification=True,
        #         clarification_question="What exactly do you want to see (e.g., loans, borrowers, branches, collections) and what should it be filtered/sorted by?",
        #         original_question=question,
        #         partial_intent={"vague": True}
        #     )
        #
        # # "top X branches" without metric
        # if "top" in q and "branch" in q and not any(k in q for k in ["collections", "repayments", "outstanding", "loans", "principal"]):
        #     return ClarificationRequest(
        #         needs_clarification=True,
        #         clarification_question="Top branches by what metric (total collections, total repayments, total outstanding balance, or number of loans)?",
        #         original_question=question,
        #         partial_intent={"entity": "branches", "needs_metric": True}
        #     )
        #
        # # "show loans" with no intent at all (limit/metric/time)
        # if q in {"show loans", "list loans", "show borrowers", "list borrowers", "show branches", "list branches"}:
        #     return ClarificationRequest(
        #         needs_clarification=True,
        #         clarification_question="Do you want a simple list (latest N) or a summary (totals/grouped)?",
        #         original_question=question,
        #         partial_intent={"entity": q.split()[-1]}
        #     )
        #
        # return ClarificationRequest(
        #     needs_clarification=False,
        #     clarification_question="",
        #     original_question=question,
        #     partial_intent={}
        # )
        # --- END LEGACY ---

        q = question.strip().lower()

        tables = compiled_rules.get("tables", {}) or {}
        table_tokens = set()
        for t in tables.keys():
            table_tokens.add(str(t).lower())
            table_tokens.add(str(t).split(".")[-1].lower())

        table_mentioned = any(tok in q for tok in table_tokens)

        # Strongly vague requests with no table/entity
        vague_phrases = {
            "show me data",
            "show data",
            "show details",
            "show info",
            "give me data",
            "tell me data",
        }
        if q in vague_phrases or (
            (
                q.startswith(("show", "list", "display", "give", "get"))
                and not table_mentioned
            )
            and len(q.split()) <= 4
        ):
            return ClarificationRequest(
                needs_clarification=True,
                clarification_question="Which table do you want (borrowers, loans, branches, collections, repayments, loan_documents, loan_status_history, field_officers)?",
                original_question=question,
                partial_intent={"vague": True, "needs_table": True},
            )

        # "top branches" without metric
        if ("top" in q and "branch" in q) and not any(
            k in q
            for k in [
                "collections",
                "repayments",
                "outstanding",
                "principal",
                "number of loans",
                "loan count",
            ]
        ):
            return ClarificationRequest(
                needs_clarification=True,
                clarification_question="Top branches by what metric: total collections, total repayments, total outstanding balance, total principal, or number of loans?",
                original_question=question,
                partial_intent={"entity": "branches", "needs_metric": True},
            )

        # Simple list requests with ambiguous intent
        if q in {
            "show loans",
            "list loans",
            "show borrowers",
            "list borrowers",
            "show branches",
            "list branches",
        }:
            return ClarificationRequest(
                needs_clarification=True,
                clarification_question="How many records do you want (e.g., 10, 20, 50) and should it be latest-first?",
                original_question=question,
                partial_intent={"entity": q.split()[-1], "needs_limit": True},
            )

        return ClarificationRequest(
            needs_clarification=False,
            clarification_question="",
            original_question=question,
            partial_intent={},
        )

    def _build_sql_prompt(
        self,
        question: str,
        kb_context: dict,
        resolved_context: Optional[ResolvedContext],
        clarification_answer: Optional[str],
        partial_intent: Optional[Dict],
    ) -> str:
        """
        Build schema-grounded SQL generation prompt with structured context.

        The LLM acts as SQL compiler, NOT decision maker.
        All context decisions are made by ContextResolver in code.
        """
        schema_name = kb_context.get("schema_name", "core")
        tables = kb_context.get("tables", {})
        fk_edges = kb_context.get("fk_edges", [])
        query_policies = kb_context.get("query_policies", {})

        # Build structured context block (NO LLM DECISIONS, JUST DATA)
        context_text = ""
        if resolved_context and resolved_context.is_related:
            dims = resolved_context.preserved_dimensions

            context_text = f"""
## RESOLVED CONTEXT (DO NOT CHANGE UNLESS USER EXPLICITLY ASKS)
Continuation type: {resolved_context.continuation_type.value}
"""

            if resolved_context.anchor_turn:
                context_text += f"""
Previous question: "{resolved_context.anchor_turn.question}"
Previous SQL: {resolved_context.anchor_turn.sql}

"""

            # Show preserved dimensions
            context_text += "Preserved dimensions:\n"
            if dims.subject:
                context_text += f"- Subject: {dims.subject}\n"
            if dims.metric:
                context_text += f"- Metric: {dims.metric}\n"
            if dims.time_window:
                context_text += f"- Time window: {dims.time_window}\n"
            if dims.grouping and len(dims.grouping) > 0:
                context_text += f"- Grouping: {', '.join(dims.grouping)}\n"
            if dims.ordering and isinstance(dims.ordering, dict):
                context_text += f"- Ordering: {dims.ordering.get('column', 'unknown')} {dims.ordering.get('direction', 'ASC')}\n"
            if dims.limit:
                context_text += f"- Limit: {dims.limit}\n"
            if dims.tables and len(dims.tables) > 0:
                context_text += f"- Tables: {', '.join(dims.tables)}\n"

            # Refinement instruction
            if resolved_context.refinement_instruction:
                context_text += f"""
Refinement: {resolved_context.refinement_instruction}
"""

            # CTE rules (only for DRILLDOWN with pronouns)
            if resolved_context.continuation_type == ContinuationType.DRILLDOWN:
                context_text += """
**CRITICAL: Use CTE (WITH clause) to preserve exact result scope**
Pattern:
```sql
WITH previous_results AS (
  -- Copy previous SQL here exactly
  {previous_sql}
)
SELECT pr.*, new_columns
FROM previous_results pr
JOIN {schema_name}.other_table ot ON pr.id = ot.entity_id
WHERE ...
"""
            elif resolved_context.continuation_type == ContinuationType.REFINE:
                context_text += f"""
**━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━**
CRITICAL INSTRUCTION - FOLLOW EXACTLY OR RESPONSE WILL BE REJECTED:
**━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━**

USER REQUEST: "{question}"
REFINEMENT TYPE: {resolved_context.refinement_instruction}

YOU MUST:
1. Take the "Previous SQL" shown above
2. Modify ONLY the element specified by refinement type
3. Preserve everything else EXACTLY (same tables, same aggregations, same JOINs)

REFINEMENT RULES:
• limit_change → Change ONLY the LIMIT number
• order_change → Change ONLY the ORDER BY clause  
• filter_change → Add/modify ONLY WHERE conditions

FORBIDDEN:
✗ DO NOT create a new query from scratch
✗ DO NOT change the subject/tables unless user explicitly asks
✗ DO NOT remove existing aggregations
✗ DO NOT switch metrics unless user explicitly asks

Your SQL MUST use the same tables and structure as Previous SQL.
"""

        # Build clarification section
        clarification_text = ""
        if clarification_answer and partial_intent:
            clarification_text = f"""
User clarification: "{clarification_answer}"

Partial intent: {json.dumps(partial_intent, indent=2)}

CRITICAL: You MUST incorporate this clarification answer into your SQL.
"""

        # Use enhanced prompt builder
        return build_enhanced_sql_prompt(
            question=question,
            schema_name=schema_name,
            tables=tables,
            fk_edges=fk_edges,
            query_policies=query_policies,
            context_text=context_text,
            clarification_text=clarification_text,
        )


# Global LLM SQL generator instance
llm_sql_generator = LLMSQLGenerator()
