"""
Configuration management using Pydantic Settings.
Loads environment variables and provides typed configuration.
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database Configuration
    db_host: str = "localhost"
    db_port: int = 5432
    db_user: str = "postgres"
    db_password: str
    db_name: str = "rag_agent_v2"
    database_url: str
    
    # LLM Configuration
    groq_api_key: str
    groq_model: str = "meta-llama/llama-3-70b-8192"
    llm_temperature: float = 0.0
    llm_max_tokens: int = 2000
    llm_timeout_seconds: int = 10
    
    # Query Execution Policies
    default_limit: int = 200
    max_limit: int = 2000
    statement_timeout_seconds: int = 30
    max_join_depth: int = 4
    hard_cap_join_depth: int = 6
    
    # KB Refresh Configuration
    kb_refresh_interval_hours: int = 1
    schema_name: str = "core"
    
    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_cors_origins: list[str] = ["*"]
    
    # Observability
    log_level: str = "INFO"
    enable_metrics: bool = True
    
    # RAG Configuration
    rag_enabled: bool = True
    rag_max_tables: int = 8
    rag_max_columns_per_table: int = 25
    rag_max_join_paths: int = 30
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()
