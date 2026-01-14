"""
Groq LLM provider implementation.
"""
import json
import time
from groq import AsyncGroq
from llm.base import BaseLLMProvider
from core.config import settings
from observability.logger import get_logger
from observability.metrics import metrics

logger = get_logger(__name__)


class GroqProvider(BaseLLMProvider):
    """Groq LLM provider using their API."""

    def __init__(self):
        self.client = AsyncGroq(api_key=settings.groq_api_key)
        self.model = settings.groq_model

    def _strip_code_fences(self, text: str) -> str:
        """Remove ``` / ```json fences if present."""
        if not text:
            return ""

        t = text.strip()

        if "```json" in t:
            t = t.split("```json", 1)[1]
            t = t.split("```", 1)[0]
            return t.strip()

        if "```" in t:
            # Generic fenced block
            parts = t.split("```", 2)
            if len(parts) >= 3:
                return parts[1].strip()
            return t.replace("```", "").strip()

        return t

    def _extract_json_object(self, text: str) -> str:
        """
        Extract the first top-level JSON object/array from a string.
        Minimal + robust for LLM responses that include extra text.
        """
        if not text:
            return ""

        t = text.strip()

        # Prefer object, else array
        obj_start = t.find("{")
        arr_start = t.find("[")

        # Choose earliest valid start
        candidates = [i for i in [obj_start, arr_start] if i != -1]
        if not candidates:
            return t

        start = min(candidates)
        open_ch = t[start]
        close_ch = "}" if open_ch == "{" else "]"

        end = t.rfind(close_ch)
        if end == -1 or end <= start:
            return t[start:]

        return t[start : end + 1].strip()

    def _sanitize_json_text(self, text: str) -> str:
        """
        Last-resort cleanup:
        - remove carriage returns
        - replace raw newlines/tabs with spaces
        (helps when LLM emits literal newlines inside JSON strings)
        """
        if not text:
            return ""
        return text.replace("\r", "").replace("\n", " ").replace("\t", " ").strip()

    async def generate_completion(
        self,
        prompt: str,
        temperature: float = None,
        max_tokens: int = None,
        **kwargs
    ) -> str:
        """Generate text completion."""
        temp = temperature if temperature is not None else settings.llm_temperature
        tokens = max_tokens if max_tokens is not None else settings.llm_max_tokens

        start_time = time.time()

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=temp,
                max_tokens=tokens,
                timeout=settings.llm_timeout_seconds
            )

            duration_ms = (time.time() - start_time) * 1000
            metrics.record_llm_request(success=True, duration_ms=duration_ms)

            return response.choices[0].message.content

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            metrics.record_llm_request(success=False, duration_ms=duration_ms)
            logger.error("llm_completion_failed", error=str(e))
            raise

    async def generate_structured_completion(
        self,
        prompt: str,
        response_format: dict = None,
        temperature: float = None,
        max_tokens: int = None
    ) -> dict:
        """
        Generate structured JSON completion.
        For models that don't support native JSON mode, we use prompt engineering.
        """
        temp = temperature if temperature is not None else settings.llm_temperature
        tokens = max_tokens if max_tokens is not None else settings.llm_max_tokens

        # Keep variable defined for error logging even if request fails early
        content = ""

        # Enhance prompt to request JSON
        json_prompt = f"""{prompt}

You MUST respond with valid JSON only. No explanations, no markdown, just the raw JSON object.
"""

        start_time = time.time()

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": json_prompt}],
                temperature=temp,
                max_tokens=tokens,
                timeout=settings.llm_timeout_seconds
            )

            duration_ms = (time.time() - start_time) * 1000
            metrics.record_llm_request(success=True, duration_ms=duration_ms)

            content = response.choices[0].message.content or ""

            # Strip markdown/code fences + extract the JSON object/array
            content = self._strip_code_fences(content)
            json_text = self._extract_json_object(content)

            # Parse JSON response
            try:
                return json.loads(json_text)
            except json.JSONDecodeError:
                # Last resort sanitize and retry
                cleaned = self._sanitize_json_text(json_text)
                return json.loads(cleaned)

        except json.JSONDecodeError as e:
            logger.error("llm_json_parse_failed", error=str(e), content=content[:500])
            duration_ms = (time.time() - start_time) * 1000
            metrics.record_llm_request(success=False, duration_ms=duration_ms)
            raise ValueError(f"LLM did not return valid JSON: {str(e)}")
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            metrics.record_llm_request(success=False, duration_ms=duration_ms)
            logger.error("llm_structured_completion_failed", error=str(e))
            raise


# Global provider instance
llm_provider = GroqProvider()
