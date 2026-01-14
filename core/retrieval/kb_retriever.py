"""
KB Retriever for Schema/KB RAG.
Deterministic retrieval of relevant schema/semantic metadata for SQL generation.
"""
import re
import time
from typing import Optional, List, Dict, Set, Tuple
from observability.logger import get_logger
from core.config import settings

logger = get_logger(__name__)


def tokenize_text(text: str) -> Set[str]:
    """
    Tokenize text into lowercase words/tokens.
    Handles underscores and camelCase.
    """
    # Replace underscores and special chars with spaces
    text = re.sub(r'[_\-]', ' ', text.lower())
    # Split on whitespace and non-alphanumeric
    tokens = re.findall(r'\b\w+\b', text)
    return set(tokens)


def score_table(
    table_metadata: dict,
    question_tokens: Set[str],
    context_tables: Set[str],
    partial_intent: Optional[Dict] = None
) -> float:
    """
    Score a table's relevance to the question.

    Scoring factors:
    - Table name match (high weight)
    - Semantic alias match (high weight)
    - Column name match (medium weight)
    - Context boost (recent conversation)
    - Partial intent boost
    """
    score = 0.0

    # Extract table info
    table_name = table_metadata.get('table', '')
    schema_qualified_name = table_metadata.get('schema_qualified_name', '')
    semantic = table_metadata.get('semantic', {})
    aliases = semantic.get('aliases', [])
    columns = table_metadata.get('columns', [])

    # 1. Table name match (weight: 10.0)
    table_tokens = tokenize_text(table_name)
    table_overlap = len(question_tokens & table_tokens)
    if table_overlap > 0:
        score += 10.0 * table_overlap

    # 2. Semantic alias match (weight: 8.0)
    for alias in aliases:
        alias_tokens = tokenize_text(alias)
        alias_overlap = len(question_tokens & alias_tokens)
        if alias_overlap > 0:
            score += 8.0 * alias_overlap

    # 3. Column name match (weight: 3.0)
    matched_columns = 0
    for col in columns:
        col_name = col.get('column_name', '')
        col_tokens = tokenize_text(col_name)
        if len(question_tokens & col_tokens) > 0:
            matched_columns += 1
    score += 3.0 * matched_columns

    # 4. Context boost (weight: 15.0) - recent conversation usage
    # IMPORTANT FIX: context_tables now actually contains preserved tables from ContextResolver/LLM generator.
    if schema_qualified_name in context_tables or table_name in context_tables:
        score += 15.0

    # 5. Partial intent boost (weight: 12.0)
    if partial_intent:
        intent_tables = partial_intent.get('tables', [])
        intent_metric = partial_intent.get('metric', '')

        # Check if this table is in partial intent
        if table_name in intent_tables or schema_qualified_name in intent_tables:
            score += 12.0

        # Check if metric mentions this table
        if intent_metric:
            metric_tokens = tokenize_text(intent_metric)
            if len(table_tokens & metric_tokens) > 0:
                score += 5.0

    return score


def select_top_columns(
    columns: List[Dict],
    question_tokens: Set[str],
    pk_columns: List[str],
    fk_columns: Set[str],
    max_columns: int
) -> List[Dict]:
    """
    Select most relevant columns, always including PK/FK.

    Strategy:
    1. Always include PK columns
    2. Always include FK columns
    3. Score remaining columns by name match
    4. Fill up to max_columns
    """
    # Separate PK/FK from regular columns
    pk_fk_cols = []
    regular_cols = []

    for col in columns:
        col_name = col.get('column_name', '')
        if col_name in pk_columns or col_name in fk_columns:
            pk_fk_cols.append(col)
        else:
            regular_cols.append(col)

    # Score regular columns
    scored_regular = []
    for col in regular_cols:
        col_name = col.get('column_name', '')
        col_tokens = tokenize_text(col_name)
        relevance = len(question_tokens & col_tokens)
        scored_regular.append((relevance, col))

    # Sort by relevance (descending)
    scored_regular.sort(key=lambda x: x[0], reverse=True)

    # Select columns: PK/FK first, then top scored
    selected = pk_fk_cols.copy()
    remaining_slots = max_columns - len(selected)

    for _, col in scored_regular[:remaining_slots]:
        selected.append(col)

    return selected


def filter_join_paths(
    all_join_paths: Dict,
    selected_tables: Set[str],
    max_paths: int
) -> Dict:
    """
    Filter join paths to only those connecting selected tables.
    """
    filtered_paths = {}

    for path_key, path_data in all_join_paths.items():
        from_table = path_data.get('from_table', '')
        to_table = path_data.get('to_table', '')

        # Include only if both tables are selected
        if from_table in selected_tables and to_table in selected_tables:
            filtered_paths[path_key] = path_data

            # Stop if we've reached max
            if len(filtered_paths) >= max_paths:
                break

    return filtered_paths


def retrieve_kb_context(
    question: str,
    compiled_rules: dict,
    conversation_context: Optional[object] = None,  # Was "deprecated" earlier; now used as table-hints when provided
    partial_intent: Optional[dict] = None,
    clarification_answer: Optional[str] = None
) -> dict:
    """
    Retrieve relevant KB context for the question using deterministic scoring.

    Args:
        question: User's natural language question
        compiled_rules: Full KB (schema + semantic + join paths)
        conversation_context: Table-hints from context (e.g., preserved tables list). If None, no boost.
        partial_intent: Partial intent from clarification request
        clarification_answer: User's answer to clarification question

    Returns:
        Filtered KB context with only relevant tables/columns/join paths
    """
    start_time = time.time()

    try:
        # Check if RAG is enabled
        if not settings.rag_enabled:
            logger.info("rag_disabled", reason="settings.rag_enabled=False")
            return _get_minimal_fallback_context(compiled_rules)

        # Combine question + clarification answer for tokenization
        combined_text = question
        if clarification_answer:
            combined_text = f"{question} {clarification_answer}"

        question_tokens = tokenize_text(combined_text)

        # IMPORTANT FIX: Use conversation_context as table-hints (if provided)
        context_tables: Set[str] = set()
        if conversation_context:
            if isinstance(conversation_context, (list, tuple, set)):
                for t in conversation_context:
                    if not t:
                        continue
                    t_str = str(t)
                    context_tables.add(t_str)
                    # Also add unqualified token (core.loans -> loans) for matching
                    context_tables.add(t_str.split('.')[-1])

        # Extract FK columns for each table (for always-include logic)
        fk_columns_by_table = {}
        all_tables = compiled_rules.get('tables', {})
        for schema_qual_name, table_meta in all_tables.items():
            fks = table_meta.get('foreign_keys', [])
            fk_cols = set(fk.get('column_name', '') for fk in fks)
            fk_columns_by_table[schema_qual_name] = fk_cols

        # Score all tables
        scored_tables = []
        for schema_qual_name, table_meta in all_tables.items():
            score = score_table(
                table_metadata=table_meta,
                question_tokens=question_tokens,
                context_tables=context_tables,
                partial_intent=partial_intent
            )
            scored_tables.append((score, schema_qual_name, table_meta))

        # Sort by score (descending) and select top N
        scored_tables.sort(key=lambda x: x[0], reverse=True)
        max_tables = settings.rag_max_tables
        top_tables = scored_tables[:max_tables]

        # Build filtered context
        selected_tables_dict = {}
        selected_table_names = set()          # schema-qualified + raw for join filtering
        selected_table_names_qualified = set()  # keep qualified for logging/metadata
        total_columns = 0

        for score, schema_qual_name, table_meta in top_tables:
            selected_table_names_qualified.add(schema_qual_name)
            selected_table_names.add(schema_qual_name)

            # IMPORTANT FIX: Add raw table name too (join_paths may use unqualified names)
            raw_table_name = table_meta.get('table', '')
            if raw_table_name:
                selected_table_names.add(raw_table_name)

            # Filter columns
            all_cols = table_meta.get('columns', [])
            pk_cols = table_meta.get('primary_keys', [])
            fk_cols = fk_columns_by_table.get(schema_qual_name, set())

            selected_cols = select_top_columns(
                columns=all_cols,
                question_tokens=question_tokens,
                pk_columns=pk_cols,
                fk_columns=fk_cols,
                max_columns=settings.rag_max_columns_per_table
            )

            total_columns += len(selected_cols)

            # Build filtered table metadata
            selected_tables_dict[schema_qual_name] = {
                'schema': table_meta.get('schema'),
                'table': table_meta.get('table'),
                'schema_qualified_name': schema_qual_name,
                'columns': selected_cols,
                'primary_keys': pk_cols,
                'foreign_keys': table_meta.get('foreign_keys', []),
                'domain': table_meta.get('domain', 'general'),
                'semantic': {
                    'purpose': table_meta.get('semantic', {}).get('purpose', 'unknown'),
                    'aliases': table_meta.get('semantic', {}).get('aliases', []),
                    'default_filters': table_meta.get('semantic', {}).get('default_filters', []),
                    'recommended_metrics': table_meta.get('semantic', {}).get('recommended_metrics', []),
                    'business_rules': table_meta.get('semantic', {}).get('business_rules', [])
                }
            }

        # Filter join paths
        all_join_paths = compiled_rules.get('join_paths', {})
        filtered_join_paths = filter_join_paths(
            all_join_paths=all_join_paths,
            selected_tables=selected_table_names,
            max_paths=settings.rag_max_join_paths
        )

        # Build minimal query policies (don't include full lists)
        query_policies = compiled_rules.get('query_policies', {})
        minimal_policies = {
            'default_limit': query_policies.get('default_limit', 200),
            'max_limit': query_policies.get('max_limit', 2000),
            'max_join_depth': query_policies.get('max_join_depth', 4),
            'statement_timeout_seconds': query_policies.get('statement_timeout_seconds', 30),
            'blocked_functions_count': len(query_policies.get('blocked_functions', [])),
            'blocked_patterns_count': len(query_policies.get('blocked_patterns', []))
        }

        # Build final context
        kb_context = {
            'schema_name': compiled_rules.get('schema_name'),
            'tables': selected_tables_dict,
            'join_paths': filtered_join_paths,
            'fk_edges': compiled_rules.get('fk_edges', []),  # Keep consistent with main path
            'query_policies': minimal_policies,
            'retrieval_metadata': {
                'total_tables_selected': len(selected_tables_dict),
                'total_columns_selected': total_columns,
                'total_join_paths': len(filtered_join_paths),
                'rag_enabled': True,
                'context_tables_hint_count': len(context_tables)
            }
        }

        duration_ms = (time.time() - start_time) * 1000

        logger.info(
            "rag_retrieval_completed",
            selected_tables=list(selected_table_names_qualified),
            columns_count_total=total_columns,
            join_paths_count=len(filtered_join_paths),
            duration_ms=round(duration_ms, 2)
        )

        return kb_context

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(
            "rag_retrieval_failed",
            error=str(e),
            duration_ms=round(duration_ms, 2)
        )
        # Fallback to minimal safe context
        return _get_minimal_fallback_context(compiled_rules)


def _get_minimal_fallback_context(compiled_rules: dict) -> dict:
    """
    Return minimal safe context when RAG fails or is disabled.
    Includes essential structure but limited content.
    """
    all_tables = compiled_rules.get('tables', {})

    # Include just table names and schema info (no columns)
    minimal_tables = {}
    for schema_qual_name, table_meta in list(all_tables.items())[:5]:  # Max 5 tables
        minimal_tables[schema_qual_name] = {
            'schema': table_meta.get('schema'),
            'table': table_meta.get('table'),
            'schema_qualified_name': schema_qual_name,
            'columns': table_meta.get('columns', [])[:10],  # Max 10 columns
            'primary_keys': table_meta.get('primary_keys', []),
            'foreign_keys': table_meta.get('foreign_keys', [])  # IMPORTANT: keep FK shape consistent
        }

    return {
        'schema_name': compiled_rules.get('schema_name'),
        'tables': minimal_tables,
        'join_paths': {},
        'fk_edges': compiled_rules.get('fk_edges', []),  # IMPORTANT FIX: keep consistent with main return
        'query_policies': {
            'default_limit': 200,
            'max_limit': 2000
        },
        'retrieval_metadata': {
            'total_tables_selected': len(minimal_tables),
            'rag_enabled': False,
            'fallback': True
        }
    }
