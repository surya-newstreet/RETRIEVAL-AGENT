"""
Pytest configuration and fixtures.
"""
import pytest
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Setup test environment variables BEFORE importing any modules
os.environ.setdefault('DB_PASSWORD', 'test_password')
os.environ.setdefault('DATABASE_URL', 'postgresql://test:test@localhost/test')
os.environ.setdefault('GROQ_API_KEY', 'test_key')

