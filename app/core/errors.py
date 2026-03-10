from __future__ import annotations


class ExternalServiceRetryExhausted(RuntimeError):
    def __init__(self, *, service: str, attempts: int, detail: str) -> None:
        self.service = service
        self.attempts = attempts
        self.detail = detail
        super().__init__(f"{service} 호출 재시도 {attempts}회 실패: {detail}")
