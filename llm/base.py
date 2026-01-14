"""
LLM provider base class and factory.
"""
from abc import ABC, abstractmethod
from typing import Optional


class BaseLLMProvider(ABC):
    """Abstract base class for LLM providers."""
    
    @abstractmethod
    async def generate_completion(
        self,
        prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 2000,
        **kwargs
    ) -> str:
        """Generate completion from prompt."""
        pass
    
    @abstractmethod
    async def generate_structured_completion(
        self,
        prompt: str,
        response_format: dict,
        temperature: float = 0.0,
        max_tokens: int = 2000
    ) -> dict:
        """Generate structured JSON completion."""
        pass
