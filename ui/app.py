"""
Production-Grade Natural Language to SQL Streamlit UI
=====================================================
A sophisticated, professional analytics dashboard interface with:
- Full-width glassmorphic design
- Real-time health monitoring
- Interactive query execution
- Clarification handling
- Metrics visualization
"""

import streamlit as st
import requests
import pandas as pd
import json
from datetime import datetime
from typing import Optional, Dict, List
import time

# ============================================================================
# CONFIGURATION
# ============================================================================

API_BASE_URL = "http://localhost:8000/api/v1"
st.set_page_config(
    page_title="NL to SQL Analytics",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CUSTOM CSS - PRODUCTION DESIGN SYSTEM
# ============================================================================

def load_custom_css():
    """Inject premium design system with glassmorphic aesthetics."""
    st.markdown("""
    <style>
        /* Import Modern Font */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
        
        /* Global Styles */
        * {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }
        
        /* Main Container - Full Width */
        .block-container {
            max-width: 100% !important;
            padding: 2rem 3rem !important;
        }
        
        /* Hide Streamlit Branding - keep sidebar toggle */
        #MainMenu {visibility: hidden !important;}
        footer {visibility: hidden !important;}
        
        /* Make header transparent but keep it functional */
        header[data-testid="stHeader"] {
            background: transparent !important;
        }
        
        /* Hide deploy button and settings, but keep sidebar toggle */
        header[data-testid="stHeader"] .stActionButton {
            display: none !important;
        }
        
        /* Ensure sidebar controls are ALWAYS visible and clickable */
        button[data-testid="baseButton-header"],
        button[data-testid="baseButton-headerNoPadding"],
        [data-testid="collapsedControl"],
        [data-testid="stSidebarCollapsedControl"] {
            display: flex !important;
            visibility: visible !important;
            opacity: 1 !important;
            pointer-events: auto !important;
            z-index: 999999 !important;
        }
        
        /* Custom Background with Gradient */
        .stApp {
            background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
            color: #ffffff;
        }
        
        /* Dashboard Header */
        .dashboard-header {
            background: linear-gradient(135deg, rgba(99, 102, 241, 0.15) 0%, rgba(168, 85, 247, 0.15) 100%);
            backdrop-filter: blur(20px);
            border-radius: 20px;
            padding: 2rem 2.5rem;
            margin-bottom: 2rem;
            border: 1px solid rgba(255, 255, 255, 0.1);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        }
        
        .dashboard-title {
            font-size: 2.5rem;
            font-weight: 700;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin: 0;
            padding: 0;
        }
        
        .dashboard-subtitle {
            font-size: 1rem;
            color: rgba(255, 255, 255, 0.7);
            margin-top: 0.5rem;
        }
        
        /* Glassmorphic Cards */
        .glass-card {
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(20px);
            border-radius: 16px;
            padding: 1.5rem;
            margin: 1rem 0;
            border: 1px solid rgba(255, 255, 255, 0.1);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
            transition: all 0.3s ease;
        }
        
        .glass-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 12px 40px rgba(0, 0, 0, 0.3);
            border-color: rgba(255, 255, 255, 0.2);
        }
        
        /* Metric Cards */
        .metric-card {
            background: linear-gradient(135deg, rgba(99, 102, 241, 0.1) 0%, rgba(168, 85, 247, 0.1) 100%);
            backdrop-filter: blur(15px);
            border-radius: 12px;
            padding: 1.25rem;
            text-align: center;
            border: 1px solid rgba(255, 255, 255, 0.1);
            transition: all 0.3s ease;
        }
        
        .metric-card:hover {
            transform: scale(1.05);
            box-shadow: 0 8px 24px rgba(99, 102, 241, 0.3);
        }
        
        .metric-value {
            font-size: 2rem;
            font-weight: 700;
            color: #ffffff;
            margin: 0.5rem 0;
        }
        
        .metric-label {
            font-size: 0.875rem;
            color: rgba(255, 255, 255, 0.6);
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        /* Status Badges */
        .status-badge {
            display: inline-block;
            padding: 0.4rem 1rem;
            border-radius: 20px;
            font-size: 0.875rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .status-healthy {
            background: rgba(16, 185, 129, 0.2);
            color: #10b981;
            border: 1px solid rgba(16, 185, 129, 0.5);
        }
        
        .status-degraded {
            background: rgba(245, 158, 11, 0.2);
            color: #f59e0b;
            border: 1px solid rgba(245, 158, 11, 0.5);
        }
        
        .status-failed {
            background: rgba(239, 68, 68, 0.2);
            color: #ef4444;
            border: 1px solid rgba(239, 68, 68, 0.5);
        }
        
        /* Query Input Area */
        .stTextArea textarea {
            background: rgba(255, 255, 255, 0.08) !important;
            border: 2px solid rgba(99, 102, 241, 0.3) !important;
            border-radius: 12px !important;
            color: #ffffff !important;
            font-size: 1rem !important;
            padding: 1rem !important;
            transition: all 0.3s ease !important;
        }
        
        .stTextArea textarea:focus {
            border-color: rgba(99, 102, 241, 0.6) !important;
            box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1) !important;
        }
        
        /* Buttons */
        .stButton > button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
            color: white !important;
            border: none !important;
            border-radius: 10px !important;
            padding: 0.75rem 2rem !important;
            font-weight: 600 !important;
            font-size: 1rem !important;
            transition: all 0.3s ease !important;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4) !important;
        }
        
        .stButton > button:hover {
            transform: translateY(-2px) !important;
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6) !important;
        }
        
        /* Results Table */
        .dataframe {
            background: rgba(255, 255, 255, 0.05) !important;
            border-radius: 12px !important;
            overflow: hidden !important;
        }
        
        .dataframe th {
            background: rgba(99, 102, 241, 0.2) !important;
            color: #ffffff !important;
            font-weight: 600 !important;
            padding: 1rem !important;
        }
        
        .dataframe td {
            background: rgba(255, 255, 255, 0.03) !important;
            color: rgba(255, 255, 255, 0.9) !important;
            padding: 0.875rem !important;
        }
        
        /* SQL Code Block */
        .sql-container {
            background: rgba(0, 0, 0, 0.3);
            border-left: 4px solid #667eea;
            border-radius: 8px;
            padding: 1rem 1.5rem;
            margin: 1rem 0;
            font-family: 'Courier New', monospace;
            color: #a5b4fc;
        }
        
        /* Expanders */
        .streamlit-expanderHeader {
            background: rgba(255, 255, 255, 0.05) !important;
            border-radius: 10px !important;
            color: #ffffff !important;
            font-weight: 600 !important;
        }
        
        /* Success/Warning/Error Messages */
        .stSuccess, .stWarning, .stError, .stInfo {
            border-radius: 12px !important;
            backdrop-filter: blur(10px) !important;
        }
        
        /* Sidebar Styling */
        .css-1d391kg, [data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(15, 12, 41, 0.95) 0%, rgba(36, 36, 62, 0.95) 100%) !important;
            backdrop-filter: blur(20px) !important;
        }
        
        /* Sidebar Headers */
        .sidebar-header {
            font-size: 1.25rem;
            font-weight: 700;
            color: #667eea;
            margin: 1.5rem 0 1rem 0;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid rgba(102, 126, 234, 0.3);
        }
        
        /* Animation for loading */
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        .loading {
            animation: pulse 2s ease-in-out infinite;
        }
        
        /* Scrollbar Styling */
        ::-webkit-scrollbar {
            width: 10px;
            height: 10px;
        }
        
        ::-webkit-scrollbar-track {
            background: rgba(255, 255, 255, 0.05);
        }
        
        ::-webkit-scrollbar-thumb {
            background: rgba(102, 126, 234, 0.5);
            border-radius: 5px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: rgba(102, 126, 234, 0.7);
        }
    </style>
    """, unsafe_allow_html=True)

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

def init_session_state():
    """Initialize session state variables."""
    # CRITICAL: Create session_id once and keep it stable for context continuity
    # FIX: if session_id ever becomes None/empty (e.g., API error payload), regenerate it
    if 'session_id' not in st.session_state or not st.session_state.session_id:
        import uuid
        st.session_state.session_id = str(uuid.uuid4())
    if 'sidebar_visible' not in st.session_state:
        st.session_state.sidebar_visible = True
    if 'query_history' not in st.session_state:
        st.session_state.query_history = []
    if 'query_hashes' not in st.session_state:  # NEW: For deduplication
        st.session_state.query_hashes = set()
    if 'pending_clarification' not in st.session_state:
        st.session_state.pending_clarification = None
    if 'last_result' not in st.session_state:
        st.session_state.last_result = None
    if 'selected_example' not in st.session_state:
        st.session_state.selected_example = None

# ============================================================================
# API FUNCTIONS
# ============================================================================

def check_api_health() -> Optional[Dict]:
    """Check API health status."""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        return response.json() if response.status_code == 200 else None
    except Exception:
        return None

def get_metrics() -> Optional[Dict]:
    """Fetch system metrics."""
    try:
        response = requests.get(f"{API_BASE_URL}/metrics", timeout=5)
        return response.json() if response.status_code == 200 else None
    except Exception:
        return None

def get_kb_status() -> Optional[Dict]:
    """Fetch knowledge base status."""
    try:
        response = requests.get(f"{API_BASE_URL}/kb-status", timeout=5)
        return response.json() if response.status_code == 200 else None
    except Exception:
        return None

def execute_query(question: str, session_id: Optional[str] = None) -> Dict:
    """Execute natural language query."""
    payload = {"question": question}
    if session_id:
        payload["session_id"] = session_id
    
    # DEBUG LOGGING (Step 1: Streamlit logging)
    print(f"\n{'='*80}")
    print(f"[STREAMLIT] Sending request to API")
    print(f"Question: {question}")
    print(f"Session ID: {session_id}")
    print(f"Full Payload: {json.dumps(payload, indent=2)}")
    print(f"{'='*80}\n")
    
    response = requests.post(f"{API_BASE_URL}/query", json=payload, timeout=60)
    # FIX: don't treat error payloads like normal QueryResponse
    response.raise_for_status()
    return response.json()

def send_clarification(original_question: str, clarification_answer: str, 
                      partial_intent: Dict, session_id: str) -> Dict:
    """Send clarification response."""
    payload = {
        "original_question": original_question,
        "clarification_answer": clarification_answer,
        "partial_intent": partial_intent,
        "session_id": session_id
    }
    
    response = requests.post(f"{API_BASE_URL}/clarify", json=payload, timeout=60)
    # FIX: don't treat error payloads like normal QueryResponse
    response.raise_for_status()
    return response.json()

# ============================================================================
# UI COMPONENTS
# ============================================================================

def render_header():
    """Render dashboard header."""
    st.markdown("""
    <div class="dashboard-header">
        <h1 class="dashboard-title">🔍 Natural Language to SQL Analytics</h1>
        <p class="dashboard-subtitle">
            Production-grade AI-powered SQL generation with schema grounding, 
            multi-stage validation, and defense-in-depth security
        </p>
    </div>
    """, unsafe_allow_html=True)

def render_status_badge(status: str) -> str:
    """Generate status badge HTML."""
    status_class = f"status-{status.lower()}"
    return f'<span class="status-badge {status_class}">{status}</span>'

def render_sidebar():
    """Render sidebar with system info and metrics."""
    with st.sidebar:
        st.markdown('<div class="sidebar-header">📊 System Status</div>', unsafe_allow_html=True)
        
        # Health Check
        health = check_api_health()
        if health:
            status_html = render_status_badge(health['status'])
            st.markdown(f"**API Health:** {status_html}", unsafe_allow_html=True)
            
            with st.expander("🔍 Detailed Health", expanded=False):
                st.json({
                    "Metadata Pool": health['db_metadata_pool'],
                    "Query Pool": health['db_query_pool'],
                    "KB Status": health['kb_status']
                })
        else:
            st.markdown(f"**API Health:** {render_status_badge('Failed')}", unsafe_allow_html=True)
            st.error("⚠️ API is unreachable")
        
        st.markdown("---")
        
        # Knowledge Base Status
        st.markdown('<div class="sidebar-header">📚 Knowledge Base</div>', unsafe_allow_html=True)
        kb_status = get_kb_status()
        if kb_status:
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Tables", kb_status.get('table_count', 0))
            with col2:
                status_color = "🟢" if kb_status['status'] == 'healthy' else "🟡"
                st.metric("Status", f"{status_color} {kb_status['status']}")
            
            if kb_status.get('last_refresh'):
                st.caption(f"Last refresh: {kb_status['last_refresh'][:19]}")
        
        st.markdown("---")
        
        # System Metrics
        st.markdown('<div class="sidebar-header">📈 Metrics</div>', unsafe_allow_html=True)
        metrics = get_metrics()
        if metrics:
            queries = metrics.get('queries', {})
            
            # Success Rate
            total = queries.get('total', 0)
            successful = queries.get('successful', 0)
            success_rate = (successful / total * 100) if total > 0 else 0
            
            st.metric("Total Queries", total)
            st.metric("Success Rate", f"{success_rate:.1f}%")
            
            # Avg Execution Time
            execution = metrics.get('execution', {})
            avg_time = execution.get('avg_time_ms', 0)
            st.metric("Avg Time (ms)", f"{avg_time:.2f}")
            
            with st.expander("📊 Detailed Metrics", expanded=False):
                st.json(metrics)
        
        st.markdown("---")
        
        # Query History
        st.markdown('<div class="sidebar-header">📝 Query History</div>', unsafe_allow_html=True)
        if st.session_state.query_history:
            for idx, query in enumerate(reversed(st.session_state.query_history[-5:])):
                with st.expander(f"Query {len(st.session_state.query_history) - idx}", expanded=False):
                    st.caption(query['question'][:100] + "..." if len(query['question']) > 100 else query['question'])
                    exec_time = query.get('execution_time_ms') or 0
                    st.caption(f"⏱️ {exec_time:.2f}ms")
        else:
            st.caption("No queries yet")
        
        st.markdown("---")
        
        # Session Info
        st.markdown('<div class="sidebar-header">🔐 Session</div>', unsafe_allow_html=True)
        if st.session_state.session_id:
            st.caption(f"ID: {st.session_state.session_id[:8]}...")
        else:
            st.caption("No active session")
        
        if st.button("🔄 New Session", use_container_width=True):
            import uuid
            st.session_state.session_id = str(uuid.uuid4())  # Create new session ID
            st.session_state.query_history = []
            st.session_state.query_hashes = set()
            st.session_state.pending_clarification = None
            st.rerun()

def render_query_interface():
    """Render main query interface."""
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    
    # Check for pending clarification
    if st.session_state.pending_clarification:
        st.warning("⚠️ Clarification Required")
        
        clarification = st.session_state.pending_clarification
        st.info(f"**Question:** {clarification['clarification_question']}")
        
        clarification_answer = st.text_input(
            "Your Answer:",
            key="clarification_input",
            placeholder="Type your clarification here..."
        )
        
        col1, col2 = st.columns([1, 5])
        with col1:
            if st.button("✅ Submit", use_container_width=True):
                if clarification_answer:
                    with st.spinner("🔄 Processing clarification..."):
                        try:
                            result = send_clarification(
                                clarification['original_question'],
                                clarification_answer,
                                clarification['partial_intent'],
                                clarification['session_id']
                            )
                            
                            st.session_state.pending_clarification = None
                            st.session_state.last_result = result
                            # FIX: never overwrite session_id with None
                            if result.get('session_id'):
                                st.session_state.session_id = result['session_id']
                            st.session_state.query_history.append({
                                'question': clarification['original_question'],
                                'clarification': clarification_answer,
                                'execution_time_ms': result.get('execution_time_ms', 0)
                            })
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                else:
                    st.warning("Please provide an answer")
        
        with col2:
            if st.button("❌ Cancel", use_container_width=True):
                st.session_state.pending_clarification = None
                st.rerun()
    
    else:
        # Normal query interface
        # Check if an example was selected and populate the input
        default_value = st.session_state.selected_example if st.session_state.selected_example else ""
        if st.session_state.selected_example:
            st.session_state.selected_example = None  # Reset after using
        
        question = st.text_area(
            "Ask a question in natural language:",
            height=120,
            placeholder="Example: How many users signed up last month?\nExample: Show me top 10 products by revenue\nExample: List all orders with customer names",
            key="question_input",
            value=default_value
        )
        
        col1, col2, col3 = st.columns([2, 2, 6])
        
        with col1:
            execute_btn = st.button("🚀 Execute Query", use_container_width=True, type="primary")
        
        with col2:
            clear_btn = st.button("🗑️ Clear", use_container_width=True)
        
        if clear_btn:
            st.session_state.last_result = None
            st.rerun()
        
        if execute_btn and question:
            with st.spinner("🔄 Processing your question..."):
                try:
                    result = execute_query(question, st.session_state.session_id)
                    
                    if result.get('needs_clarification'):
                        # Store clarification state
                        st.session_state.pending_clarification = {
                            'original_question': question,
                            'clarification_question': result['clarification_question'],
                            'partial_intent': result['partial_intent'],
                            'session_id': result['session_id']
                        }
                        st.rerun()
                    else:
                        # Store result
                        st.session_state.last_result = result
                        # FIX: never overwrite session_id with None
                        if result.get('session_id'):
                            st.session_state.session_id = result['session_id']
                        
                        # Deduplicate history (NEW)
                        import hashlib
                        query_hash = hashlib.sha256(
                            f"{question}|{result.get('sql', '')}|{result.get('correlation_id', '')}".encode()
                        ).hexdigest()
                        
                        if query_hash not in st.session_state.query_hashes:
                            st.session_state.query_history.append({
                                'question': question,
                                'execution_time_ms': result.get('execution_time_ms', 0)
                            })
                            st.session_state.query_hashes.add(query_hash)
                        
                        st.rerun()
                
                except Exception as e:
                    st.error(f"❌ Error executing query: {str(e)}")
    
    st.markdown('</div>', unsafe_allow_html=True)

def render_results():
    """Render query results."""
    if not st.session_state.last_result:
        return
    
    result = st.session_state.last_result
    
    # Check for refusal message (read-only enforcement) - NEW
    if result.get('refusal_message'):
        st.error(f"🚫 {result['refusal_message']}")
        return  # Don't show SQL/results
    
    # Metadata Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Rows Returned</div>
            <div class="metric-value">{result.get('row_count', 0):,}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Execution Time</div>
            <div class="metric-value">{result.get('execution_time_ms', 0):.2f}ms</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        confidence = result.get('confidence', 0)
        confidence_pct = confidence * 100 if confidence else 0
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Confidence</div>
            <div class="metric-value">{confidence_pct:.1f}%</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        correlation_id = result.get('correlation_id', 'N/A')
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Correlation ID</div>
            <div class="metric-value" style="font-size: 0.875rem;">{correlation_id[:8]}...</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # SQL Query
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown("### 📝 Generated SQL")
    st.code(result.get('sql', ''), language='sql')
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Results Table
    if result.get('rows'):
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown("### 📊 Query Results")
        df = pd.DataFrame(result['rows'])
        st.dataframe(df, use_container_width=True, height=400)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Warnings & Safety Info
    if result.get('warnings') or result.get('safety_explanation'):
        with st.expander("⚠️ Warnings & Safety Information", expanded=False):
            if result.get('warnings'):
                for warning in result['warnings']:
                    st.warning(warning)
            
            if result.get('safety_explanation'):
                st.info(result['safety_explanation'])
    
    # Provenance
    if result.get('provenance'):
        with st.expander("🔍 Query Provenance", expanded=False):
            st.json(result['provenance'])

def render_example_queries():
    """Render example queries section."""
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown("### 💡 Example Queries")
    
    examples = [
        "How many users do we have?",
        "Show me all products",
        "List orders with customer names",
        "Total revenue by product category",
        "Average order value per user",
        "Orders placed in the last 30 days",
        "New users this month",
        "Top 10 customers by total spend"
    ]
    
    cols = st.columns(4)
    for idx, example in enumerate(examples):
        with cols[idx % 4]:
            if st.button(example, key=f"example_{idx}", use_container_width=True):
                st.session_state.selected_example = example
                st.rerun()
    
    st.markdown('</div>', unsafe_allow_html=True)

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Main application entry point."""
    load_custom_css()
    init_session_state()
    
    render_header()
    render_sidebar()
    
    # Main Content Area
    render_query_interface()
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Results
    render_results()
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Examples
    render_example_queries()
    
    # Footer
    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown("""
    <div style="text-align: center; color: rgba(255, 255, 255, 0.4); font-size: 0.875rem;">
        <p>🔒 Production-Grade NL to SQL System | Schema Grounding • Multi-Stage Validation • Defense-in-Depth Security</p>
        <p>Powered by FastAPI, Groq LLM, PostgreSQL, and Streamlit</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
