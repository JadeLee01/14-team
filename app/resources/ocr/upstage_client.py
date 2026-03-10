import httpx
from aiolimiter import AsyncLimiter

from app.core.errors import ExternalServiceRetryExhausted
from app.utils.retry import retry_async


class UpstageDocumentParseClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        api_key: str,
        url: str,
        limiter: AsyncLimiter | None = None,
        retry_max_attempts: int = 3,
        retry_backoff_base_sec: float = 0.5,
    ):
        self.http = http
        self.api_key = api_key
        self.url = url
        self.limiter = limiter
        self.retry_max_attempts = max(1, retry_max_attempts)
        self.retry_backoff_base_sec = max(retry_backoff_base_sec, 0.0)

    async def parse_image(self, image_bytes: bytes, filename: str = "page.png") -> dict:
        headers = {"Authorization": f"Bearer {self.api_key}"}
        files = {"document": (filename, image_bytes, "image/png")}
        data = {"ocr": "force", "base64_encoding": "['table']", "model": "document-parse"}
        last_retryable_error: Exception | None = None

        async def _call_once() -> dict:
            if self.limiter is None:
                res = await self.http.post(self.url, headers=headers, files=files, data=data)
            else:
                async with self.limiter:
                    res = await self.http.post(self.url, headers=headers, files=files, data=data)
            res.raise_for_status()
            return res.json()

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
                    service="ocr",
                    attempts=self.retry_max_attempts,
                    detail=self._describe_error(retry_source),
                ) from exc
            raise

    def _describe_error(self, exc: Exception) -> str:
        if isinstance(exc, httpx.TimeoutException):
            return "응답 시간이 초과되었습니다."
        if isinstance(exc, httpx.ConnectError):
            return "연결에 실패했습니다."
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code if exc.response is not None else "unknown"
            return f"HTTP {status} 오류가 발생했습니다."
        return str(exc) or exc.__class__.__name__
