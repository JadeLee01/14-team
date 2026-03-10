from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    *,
    is_retryable: Callable[[Exception], bool],
    max_attempts: int,
    backoff_base_sec: float,
) -> T:
    if max_attempts < 1:
        raise ValueError("max_attempts는 1 이상이어야 합니다.")

    attempt = 0
    while True:
        attempt += 1
        try:
            return await operation()
        except Exception as exc:
            if attempt >= max_attempts or not is_retryable(exc):
                raise
            wait_sec = max(backoff_base_sec, 0.0) * (2 ** (attempt - 1))
            if wait_sec > 0:
                await asyncio.sleep(wait_sec)
