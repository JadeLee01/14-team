import logging
import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

from app.core.errors import ExternalServiceRetryExhausted
from app.utils.retry import retry_async

logger = logging.getLogger(__name__)


class VLLMClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        base_url: str,
        api_key: str,
        model: str,
        retry_max_attempts: int = 3,
        retry_backoff_base_sec: float = 0.5,
    ):
        self.http = http
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.retry_max_attempts = max(1, retry_max_attempts)
        self.retry_backoff_base_sec = max(retry_backoff_base_sec, 0.0)

    async def chat(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.2,
        max_tokens: int = 1024,
        model: str | None = None,
    ) -> str:
        url = f"{self.base_url}/chat/completions"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        requested_model = self._resolve_model(model)
        last_retryable_error: Exception | None = None

        async def _call_once() -> str:
            payload: dict[str, Any] = {
                "model": requested_model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }

            try:
                res = await self.http.post(url, json=payload, headers=headers)
                res.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if not self._should_retry_with_base_model(exc, requested_model):
                    raise
                logger.warning(
                    "LoRA adapter model request failed; retrying with base model",
                    extra={"requested_model": requested_model, "base_model": self.model},
                )
                payload["model"] = self.model
                res = await self.http.post(url, json=payload, headers=headers)
                res.raise_for_status()

            data = res.json()
            return data["choices"][0]["message"]["content"]

        def _is_retryable(exc: Exception) -> bool:
            nonlocal last_retryable_error
            if isinstance(exc, httpx.TimeoutException | httpx.ConnectError | httpx.ReadError):
                last_retryable_error = exc
                return True
            if isinstance(exc, httpx.HTTPStatusError):
                status = exc.response.status_code if exc.response is not None else 0
                retryable = status == 429 or 500 <= status <= 599
                if retryable:
                    last_retryable_error = exc
                return retryable
            return False

        try:
            return await retry_async(
                _call_once,
                is_retryable=_is_retryable,
                max_attempts=self.retry_max_attempts,
                backoff_base_sec=self.retry_backoff_base_sec,
            )
        except Exception as exc:
            retry_source = last_retryable_error or exc
            if _is_retryable(retry_source):
                raise ExternalServiceRetryExhausted(
                    service="vllm",
                    attempts=self.retry_max_attempts,
                    detail=self._describe_error(retry_source),
                ) from exc
            raise

    async def chat_stream(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.2,
        max_tokens: int = 1024,
        model: str | None = None,
    ) -> AsyncIterator[str]:
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "text/event-stream",
        }
        payload: dict[str, Any] = {
            "model": self._resolve_model(model),
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": True,
        }

        async with self.http.stream("POST", url, json=payload, headers=headers) as res:
            res.raise_for_status()
            async for line in res.aiter_lines():
                if not line:
                    continue
                if not line.startswith("data:"):
                    continue

                raw = line[5:].strip()
                if not raw or raw == "[DONE]":
                    continue

                try:
                    data = json.loads(raw)
                except Exception:
                    continue

                choices = data.get("choices")
                if not isinstance(choices, list) or not choices:
                    continue

                item = choices[0] if isinstance(choices[0], dict) else {}
                delta = item.get("delta") if isinstance(item.get("delta"), dict) else {}
                token = delta.get("content")
                if not token:
                    message = item.get("message") if isinstance(item.get("message"), dict) else {}
                    token = message.get("content")
                if token:
                    yield str(token)

    def _resolve_model(self, model: str | None) -> str:
        if model is None:
            return self.model
        candidate = model.strip()
        if candidate.lower() in {"", "none", "null"}:
            return self.model
        return candidate

    def _should_retry_with_base_model(self, exc: httpx.HTTPStatusError, requested_model: str) -> bool:
        if requested_model == self.model:
            return False

        response = exc.response
        if response is None:
            return False
        if response.status_code not in {400, 404, 422}:
            return False

        try:
            body = response.json()
        except Exception:
            body = response.text

        text = str(body).lower()
        missing_model_hints = ("not found", "does not exist", "unknown", "lora", "adapter")
        return "model" in text and any(hint in text for hint in missing_model_hints)

    def _describe_error(self, exc: Exception) -> str:
        if isinstance(exc, httpx.TimeoutException):
            return "응답 시간이 초과되었습니다."
        if isinstance(exc, httpx.ConnectError):
            return "연결에 실패했습니다."
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code if exc.response is not None else "unknown"
            return f"HTTP {status} 오류가 발생했습니다."
        return str(exc) or exc.__class__.__name__
