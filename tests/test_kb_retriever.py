"""
Tests for KB Retriever (Schema/KB RAG).
"""
import pytest
from core.retrieval.kb_retriever import (
    tokenize_text,
    score_table,
    select_top_columns,
    filter_join_paths,
    retrieve_kb_context
)
from core.context_resolver import Context, Turn
from core.config import settings


def test_tokenize_text():
    """Test text tokenization with underscores and case handling."""
    text = "Show me all_users from UserAccounts"
    tokens = tokenize_text(text)
    
    assert 'show' in tokens
    assert 'me' in tokens
    assert 'all' in tokens
    assert 'users' in tokens
    assert 'from' in tokens
    assert 'useraccounts' in tokens


def test_score_table_name_match():
    """Test table scoring with name match."""
    table_meta = {
        'table': 'borrowers',
        'schema_qualified_name': 'core.borrowers',
        'columns': [],
        'semantic': {'aliases': []}
    }
    
    question_tokens = {'borrowers', 'total', 'count'}
    score = score_table(table_meta, question_tokens, set(), None)
    
    # Should have high score due to table name match
    assert score >= 10.0


def test_score_table_alias_match():
    """Test table scoring with semantic alias match."""
    table_meta = {
        'table': 'borrowers',
        'schema_qualified_name': 'core.borrowers',
        'columns': [],
        'semantic': {'aliases': ['borrowers', 'borrower', 'clients']}
    }
    
    question_tokens = {'show', 'all', 'clients'}
    score = score_table(table_meta, question_tokens, set(), None)
    
    # Should have high score due to alias match
    assert score >= 8.0


def test_score_table_column_match():
    """Test table scoring with column match."""
    table_meta = {
        'table': 'loans',
        'schema_qualified_name': 'core.loans',
        'columns': [
            {'column_name': 'loan_number'},
            {'column_name': 'principal_amount'},
            {'column_name': 'status'}
        ],
        'semantic': {'aliases': []}
    }
    
    question_tokens = {'loan', 'number', 'principal'}
    score = score_table(table_meta, question_tokens, set(), None)
    
    # Should have score from column matches
    assert score >= 3.0


def test_score_table_context_boost():
    """Test table scoring with conversation context boost."""
    table_meta = {
        'table': 'loans',
        'schema_qualified_name': 'core.loans',
        'columns': [],
        'semantic': {'aliases': []}
    }
    
    question_tokens = {'total', 'count'}
    context_tables = {'core.loans'}
    
    score = score_table(table_meta, question_tokens, context_tables, None)
    
    # Should have high score due to context boost
    assert score >= 15.0


def test_score_table_partial_intent_boost():
    """Test table scoring with partial intent boost."""
    table_meta = {
        'table': 'collections',
        'schema_qualified_name': 'core.collections',
        'columns': [],
        'semantic': {'aliases': []}
    }
    
    question_tokens = {'amount'}
    partial_intent = {'tables': ['collections'], 'metric': 'sum'}
    
    score = score_table(table_meta, question_tokens, set(), partial_intent)
    
    # Should have score from partial intent
    assert score >= 12.0


def test_select_top_columns_includes_pk_fk():
    """Test that PK/FK columns are always included."""
    columns = [
        {'column_name': 'id'},
        {'column_name': 'borrower_id'},
        {'column_name': 'name'},
        {'column_name': 'address'},
        {'column_name': 'phone'}
    ]
    
    pk_columns = ['id']
    fk_columns = {'borrower_id'}
    question_tokens = {'name'}
    
    selected = select_top_columns(columns, question_tokens, pk_columns, fk_columns, max_columns=3)
    
    # Should include PK and FK
    col_names = [c['column_name'] for c in selected]
    assert 'id' in col_names
    assert 'borrower_id' in col_names
    
    # Should respect max_columns
    assert len(selected) <= 3


def test_select_top_columns_relevance_scoring():
    """Test column selection by relevance."""
    columns = [
        {'column_name': 'id'},
        {'column_name': 'name'},
        {'column_name': 'total_amount'},
        {'column_name': 'status'}
    ]
    
    question_tokens = {'total', 'amount', 'sum'}
    
    selected = select_top_columns(columns, question_tokens, ['id'], set(), max_columns=10)
    
    # Should include 'total_amount' due to relevance
    col_names = [c['column_name'] for c in selected]
    assert 'total_amount' in col_names


def test_filter_join_paths():
    """Test join path filtering to selected tables only."""
    all_join_paths = {
        'core.loans->core.borrowers': {
            'from_table': 'core.loans',
            'to_table': 'core.borrowers',
            'depth': 1
        },
        'core.loans->core.branches': {
            'from_table': 'core.loans',
            'to_table': 'core.branches',
            'depth': 1
        },
        'core.collections->core.loans': {
            'from_table': 'core.collections',
            'to_table': 'core.loans',
            'depth': 1
        }
    }
    
    selected_tables = {'core.loans', 'core.borrowers'}
    
    filtered = filter_join_paths(all_join_paths, selected_tables, max_paths=30)
    
    # Should only include path between selected tables
    assert 'core.loans->core.borrowers' in filtered
    assert 'core.loans->core.branches' not in filtered
    assert 'core.collections->core.loans' not in filtered


def test_retrieve_kb_context_respects_max_tables(monkeypatch):
    """Test that retrieval respects rag_max_tables setting."""
    monkeypatch.setattr(settings, 'rag_enabled', True)
    monkeypatch.setattr(settings, 'rag_max_tables', 2)
    monkeypatch.setattr(settings, 'rag_max_columns_per_table', 10)
    
    compiled_rules = {
        'schema_name': 'core',
        'tables': {
            'core.borrowers': {
                'table': 'borrowers',
                'schema_qualified_name': 'core.borrowers',
                'columns': [{'column_name': 'id'}, {'column_name': 'name'}],
                'primary_keys': ['id'],
                'foreign_keys': [],
                'semantic': {'aliases': ['borrowers']},
                'domain': 'microfinance'
            },
            'core.loans': {
                'table': 'loans',
                'schema_qualified_name': 'core.loans',
                'columns': [{'column_name': 'id'}, {'column_name': 'loan_number'}],
                'primary_keys': ['id'],
                'foreign_keys': [],
                'semantic': {'aliases': ['loans']},
                'domain': 'microfinance'
            },
            'core.branches': {
                'table': 'branches',
                'schema_qualified_name': 'core.branches',
                'columns': [{'column_name': 'id'}, {'column_name': 'branch_name'}],
                'primary_keys': ['id'],
                'foreign_keys': [],
                'semantic': {'aliases': ['branches']},
                'domain': 'microfinance'
            }
        },
        'join_paths': {},
        'query_policies': {}
    }
    
    question = "Show me borrowers and loans"
    
    kb_context = retrieve_kb_context(question, compiled_rules)
    
    # Should select at most 2 tables
    assert len(kb_context['tables']) <= 2
    assert kb_context['retrieval_metadata']['rag_enabled'] is True


def test_retrieve_kb_context_fallback_when_disabled(monkeypatch):
    """Test fallback when RAG is disabled."""
    monkeypatch.setattr(settings, 'rag_enabled', False)
    
    compiled_rules = {
        'schema_name': 'core',
        'tables': {
            'core.borrowers': {
                'table': 'borrowers',
                'schema_qualified_name': 'core.borrowers',
                'columns': [{'column_name': 'id'}],
                'primary_keys': [],
                'foreign_keys': []
            }
        },
        'join_paths': {},
        'query_policies': {}
    }
    
    question = "Show me all borrowers"
    
    kb_context = retrieve_kb_context(question, compiled_rules)
    
    # Should return fallback context
    assert kb_context['retrieval_metadata']['rag_enabled'] is False
    assert kb_context['retrieval_metadata']['fallback'] is True


def test_retrieve_kb_context_with_clarification_answer(monkeypatch):
    """Test that clarification answer is included in retrieval."""
    monkeypatch.setattr(settings, 'rag_enabled', True)
    monkeypatch.setattr(settings, 'rag_max_tables', 5)
    
    compiled_rules = {
        'schema_name': 'core',
        'tables': {
            'core.collections': {
                'table': 'collections',
                'schema_qualified_name': 'core.collections',
                'columns': [{'column_name': 'amount_collected'}],
                'primary_keys': [],
                'foreign_keys': [],
                'semantic': {'aliases': ['collections']},
                'domain': 'microfinance'
            }
        },
        'join_paths': {},
        'query_policies': {}
    }
    
    question = "Show me totals"
    clarification_answer = "collections"
    
    kb_context = retrieve_kb_context(
        question, 
        compiled_rules,
        clarification_answer=clarification_answer
    )
    
    # Should boost collections table due to clarification answer
    assert 'core.collections' in kb_context['tables']


def test_retrieve_kb_context_with_conversation_context(monkeypatch):
    """Test that conversation context boosts recent tables."""
    monkeypatch.setattr(settings, 'rag_enabled', True)
    monkeypatch.setattr(settings, 'rag_max_tables', 3)
    
    compiled_rules = {
        'schema_name': 'core',
        'tables': {
            'core.loans': {
                'table': 'loans',
                'schema_qualified_name': 'core.loans',
                'columns': [{'column_name': 'id'}],
                'primary_keys': [],
                'foreign_keys': [],
                'semantic': {'aliases': ['loans']},
                'domain': 'microfinance'
            },
            'core.borrowers': {
                'table': 'borrowers',
                'schema_qualified_name': 'core.borrowers',
                'columns': [{'column_name': 'id'}],
                'primary_keys': [],
                'foreign_keys': [],
                'semantic': {'aliases': ['borrowers']},
                'domain': 'microfinance'
            }
        },
        'join_paths': {},
        'query_policies': {}
    }
    
    # Create context with recent loans table usage
    context = Context(
        last_turns=[
            Turn(
                question="Show loans",
                sql="SELECT * FROM core.loans",
                intent_summary={'tables': ['core.loans']}
            )
        ],
        rolling_summary="User asked about loans"
    )
    
    question = "How many total?"
    
    kb_context = retrieve_kb_context(question, compiled_rules, conversation_context=context)
    
    # Should include loans due to context boost
    assert 'core.loans' in kb_context['tables']
