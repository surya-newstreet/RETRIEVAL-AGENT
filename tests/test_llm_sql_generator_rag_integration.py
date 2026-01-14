"""
Tests for LLM SQL Generator RAG Integration.
Ensures RAG retrieval is properly integrated into SQL generation flow.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from core.llm_sql_generator import llm_sql_generator, SQLGenerationResult
from core.context_resolver import Context, Turn


@pytest.fixture
def sample_compiled_rules():
    """Sample compiled rules for testing."""
    return {
        'schema_name': 'core',
        'tables': {
            'core.loans': {
                'table': 'loans',
                'schema_qualified_name': 'core.loans',
                'columns': [
                    {'column_name': 'id', 'data_type': 'bigint', 'is_nullable': False},
                    {'column_name': 'principal_amount', 'data_type': 'numeric', 'is_nullable': False}
                ],
                'primary_keys': ['id'],
                'foreign_keys': [],
                'semantic': {'aliases': ['loans'], 'purpose': 'Track all loans'},
                'domain': 'microfinance'
            }
        },
        'join_paths': {},
        'query_policies': {
            'default_limit': 200,
            'max_limit': 2000,
            'max_join_depth': 4,
            'blocked_functions': ['pg_sleep']
        }
    }


@pytest.fixture
def sample_kb_context():
    """Sample RAG-retrieved KB context."""
    return {
        'schema_name': 'core',
        'tables': {
            'core.loans': {
                'table': 'loans',
                'schema_qualified_name': 'core.loans',
                'columns': [
                    {'column_name': 'id', 'data_type': 'bigint', 'is_nullable': False},
                    {'column_name': 'principal_amount', 'data_type': 'numeric', 'is_nullable':  False}
                ],
                'primary_keys': ['id'],
                'foreign_keys': [],
                'semantic': {'aliases': ['loans'], 'purpose': 'Track all loans'},
                'domain': 'microfinance'
            }
        },
        'join_paths': {},
        'query_policies': {
            'default_limit': 200,
            'max_join_depth': 4
        },
        'retrieval_metadata': {
            'total_tables_selected': 1,
            'total_columns_selected': 2,
            'rag_enabled': True
        }
    }


@pytest.mark.asyncio
async def test_generate_sql_calls_rag_retriever(sample_compiled_rules, sample_kb_context):
    """Test that generate_sql calls the RAG retriever."""
    with patch('core.llm_sql_generator.retrieve_kb_context') as mock_retrieve, \
         patch('core.llm_sql_generator.llm_provider.generate_structured_completion') as mock_llm:
        
        # Setup mocks
        mock_retrieve.return_value = sample_kb_context
        mock_llm.return_value = {
            'sql': 'SELECT * FROM core.loans LIMIT 200',
            'confidence': 0.9,
            'tables_used': ['loans'],
            'intent_summary': {'tables': ['loans'], 'metric': 'all'}
        }
        
        question = "Show me all loans"
        correlation_id = "test-123"
        
        result = await llm_sql_generator.generate_sql(
            question=question,
            compiled_rules=sample_compiled_rules,
            correlation_id=correlation_id
        )
        
        # Verify RAG retriever was called
        mock_retrieve.assert_called_once()
        call_kwargs = mock_retrieve.call_args.kwargs
        assert call_kwargs['question'] == question
        assert call_kwargs['compiled_rules'] == sample_compiled_rules
        
        # Verify SQL was generated
        assert result.sql is not None
        assert 'loans' in result.sql.lower()


@pytest.mark.asyncio
async def test_generate_sql_passes_context_to_rag(sample_compiled_rules, sample_kb_context):
    """Test that conversation context is passed to RAG retriever."""
    with patch('core.llm_sql_generator.retrieve_kb_context') as mock_retrieve, \
         patch('core.llm_sql_generator.llm_provider.generate_structured_completion') as mock_llm:
        
        mock_retrieve.return_value = sample_kb_context
        mock_llm.return_value = {
            'sql': 'SELECT COUNT(*) FROM core.loans LIMIT 200',
            'confidence': 0.85,
            'tables_used': ['loans'],
            'intent_summary': {'tables': ['loans'], 'metric': 'count'}
        }
        
        # Create conversation context
        context = Context(
            last_turns=[
                Turn(
                    question="Show loans",
                    sql="SELECT * FROM core.loans",
                    intent_summary={'tables': ['loans']}
                )
            ],
            rolling_summary="User asked about loans"
        )
        
        question = "How many total?"
        correlation_id = "test-456"
        
        result = await llm_sql_generator.generate_sql(
            question=question,
            compiled_rules=sample_compiled_rules,
            correlation_id=correlation_id,
            context=context
        )
        
        # Verify context was passed to RAG
        mock_retrieve.assert_called_once()
        args, kwargs = mock_retrieve.call_args
        assert kwargs.get('conversation_context') == context or args[2] == context


@pytest.mark.asyncio
async def test_generate_sql_with_clarification_passes_to_rag(sample_compiled_rules, sample_kb_context):
    """Test that clarification_answer and partial_intent are passed to RAG."""
    with patch('core.llm_sql_generator.retrieve_kb_context') as mock_retrieve, \
         patch('core.llm_sql_generator.llm_provider.generate_structured_completion') as mock_llm:
        
        mock_retrieve.return_value = sample_kb_context
        mock_llm.return_value = {
            'sql': 'SELECT * FROM core.loans WHERE status = \'active\' LIMIT 200',
            'confidence': 0.9,
            'tables_used': ['loans'],
            'intent_summary': {'tables': ['loans'], 'metric': 'all', 'filter': 'active'}
        }
        
        question = "Show me totals"
        clarification_answer = "active loans"
        partial_intent = {'tables': ['loans'], 'metric': 'unknown'}
        correlation_id = "test-789"
        
        result = await llm_sql_generator.generate_sql(
            question=question,
            compiled_rules=sample_compiled_rules,
            correlation_id=correlation_id,
            clarification_answer=clarification_answer,
            partial_intent=partial_intent
        )
        
        # Verify clarification data passed to RAG
        mock_retrieve.assert_called_once()
        args, kwargs = mock_retrieve.call_args
        assert kwargs.get('clarification_answer') == clarification_answer or args[4] == clarification_answer
        assert kwargs.get('partial_intent') == partial_intent or args[3] == partial_intent


@pytest.mark.asyncio
async def test_generate_sql_uses_retrieved_context_in_prompt(sample_compiled_rules, sample_kb_context):
    """Test that the retrieved KB context is actually used in the prompt."""
    with patch('core.llm_sql_generator.retrieve_kb_context') as mock_retrieve, \
         patch('core.llm_sql_generator.llm_provider.generate_structured_completion') as mock_llm:
        
        mock_retrieve.return_value = sample_kb_context
        
        # Mock both clarification detection and SQL generation responses
        mock_llm.side_effect = [
            # First call: clarification detection (returns complete)
            {'is_complete': True, 'missing_elements': [], 'partial_intent': {}},
            # Second call: SQL generation
            {
                'sql': 'SELECT * FROM core.loans LIMIT 200',
                'confidence': 0.9,
                'tables_used': ['loans'],
                'intent_summary': {'tables': ['loans']}
            }
        ]
        
        question = "Show all loans"
        correlation_id = "test-prompt"
        
        await llm_sql_generator.generate_sql(
            question=question,
            compiled_rules=sample_compiled_rules,
            correlation_id=correlation_id
        )
        
        # Get the SECOND prompt (SQL generation, not clarification)
        assert mock_llm.call_count == 2
        sql_gen_call = mock_llm.call_args_list[1]
        prompt = sql_gen_call.kwargs.get('prompt') or sql_gen_call.args[0]
        
        # Verify prompt contains data from retrieved context (not full compiled_rules)
        assert 'core.loans' in prompt
        assert 'principal_amount' in prompt
        # Should NOT contain tables that weren't retrieved
        assert 'core.borrowers' not in prompt
        assert 'core.branches' not in prompt


@pytest.mark.asyncio
async def test_generate_sql_fallback_on_rag_failure(sample_compiled_rules):
    """Test graceful fallback when RAG retrieval fails."""
    with patch('core.llm_sql_generator.retrieve_kb_context') as mock_retrieve, \
         patch('core.llm_sql_generator.llm_provider.generate_structured_completion') as mock_llm:
        
        # First call raises exception, second call (fallback) succeeds
        minimal_context = {
            'schema_name': 'core',
            'tables': {},
            'join_paths': {},
            'query_policies': {'default_limit': 200},
            'retrieval_metadata': {'rag_enabled': False, 'fallback': True}
        }
        mock_retrieve.side_effect = [Exception("RAG failed"), minimal_context]
        
        mock_llm.return_value = {
            'sql': 'SELECT 1',
            'confidence': 0.5,
            'tables_used': [],
            'intent_summary': {}
        }
        
        question = "Show data"
        correlation_id = "test-fallback"
        
        # Should not raise exception
        result = await llm_sql_generator.generate_sql(
            question=question,
            compiled_rules=sample_compiled_rules,
            correlation_id=correlation_id
        )
        
        # Should have been called twice (initial + fallback)
        assert mock_retrieve.call_count == 2
        # SQL should still be generated
        assert result.sql is not None


@pytest.mark.asyncio
async def test_prompt_building_with_rag_context(sample_kb_context):
    """Test that _build_sql_prompt correctly uses kb_context."""
    question = "Count all loans"
    
    prompt = llm_sql_generator._build_sql_prompt(
        question=question,
        kb_context=sample_kb_context,
        context=None,
        clarification_answer=None,
        partial_intent=None
    )
    
    # Verify prompt structure
    assert 'SCHEMA:' in prompt
    assert 'RULES:' in prompt
    assert 'core.loans' in prompt
    assert 'principal_amount' in prompt
    assert question in prompt
    
    # Verify query policies from kb_context
    assert '200' in prompt  # default_limit
    assert '4' in prompt  # max_join_depth


@pytest.mark.asyncio
async def test_prompt_building_with_context(sample_kb_context):
    """Test prompt building with conversation context."""
    question = "How many?"
    context = Context(
        last_turns=[],
        rolling_summary="User previously asked about loans"
    )
    
    prompt = llm_sql_generator._build_sql_prompt(
        question=question,
        kb_context=sample_kb_context,
        context=context,
        clarification_answer=None,
        partial_intent=None
    )
    
    # Should include context
    assert 'Previous conversation context' in prompt
    assert 'loans' in prompt


@pytest.mark.asyncio
async def test_prompt_building_with_clarification(sample_kb_context):
    """Test prompt building with clarification answer."""
    question = "Show totals"
    clarification_answer = "active loans"
    partial_intent = {'tables': ['loans'], 'metric': 'count'}
    
    prompt = llm_sql_generator._build_sql_prompt(
        question=question,
        kb_context=sample_kb_context,
        context=None,
        clarification_answer=clarification_answer,
        partial_intent=partial_intent
    )
    
    # Should include clarification
    assert clarification_answer in prompt
    assert 'Partial intent' in prompt


@pytest.mark.asyncio
async def test_no_llm_calls_in_rag_retrieval(sample_compiled_rules):
    """Verify that RAG retrieval is deterministic (no LLM calls)."""
    from core.retrieval.kb_retriever import retrieve_kb_context
    
    with patch('llm.groq_client.llm_provider.generate_structured_completion') as mock_llm:
        # Call retriever directly
        kb_context = retrieve_kb_context(
            question="Show all loans",
            compiled_rules=sample_compiled_rules
        )
        
        # LLM should NOT have been called during retrieval
        mock_llm.assert_not_called()
        
        # Context should be returned
        assert 'tables' in kb_context
        assert 'retrieval_metadata' in kb_context
