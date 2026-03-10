from __future__ import annotations

import json

import httpx

_MAX_DETAIL_LEN = 180


def _normalize_text(text: str, *, limit: int = _MAX_DETAIL_LEN) -> str:
    normalized = " ".join((text or "").split())
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[: limit - 3]}..."


def _root_exception(exc: BaseException) -> BaseException:
    seen: set[int] = set()
    current: BaseException = exc
    while True:
        nxt = current.__cause__ or current.__context__
        if nxt is None or id(nxt) in seen:
            return current
        seen.add(id(nxt))
        current = nxt


def _classify_exception(exc: BaseException) -> str:
    if isinstance(exc, json.JSONDecodeError):
        return "요청 메시지가 올바른 JSON 형식이 아닙니다."
    if isinstance(exc, UnicodeDecodeError):
        return "요청 메시지 인코딩이 올바르지 않습니다."
    if isinstance(exc, KeyError):
        if exc.args:
            return f"필수 요청 필드({exc.args[0]})가 누락되었습니다."
        return "필수 요청 필드가 누락되었습니다."
    if isinstance(exc, TypeError):
        return "요청 데이터 타입이 올바르지 않습니다."
    if isinstance(exc, RuntimeError) and str(exc).strip() == "UNPROCESSABLE_DOCUMENT":
        return "문서를 처리할 수 없습니다. 파일 손상 또는 암호화 여부를 확인해주세요."
    if isinstance(exc, httpx.TimeoutException):
        return "외부 서비스 응답 시간이 초과되었습니다."
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code if exc.response is not None else "unknown"
        return f"외부 서비스가 오류 응답(HTTP {status})을 반환했습니다."
    if isinstance(exc, httpx.ConnectError):
        return "외부 서비스 연결에 실패했습니다."
    if isinstance(exc, httpx.RequestError):
        return "외부 서비스 통신 중 네트워크 오류가 발생했습니다."
    return "내부 처리 중 예기치 못한 오류가 발생했습니다."


def format_task_error(task_name: str, exc: Exception) -> str:
    if isinstance(exc, ValueError):
        detail = _normalize_text(str(exc))
        if detail:
            return detail

    root = _root_exception(exc)
    reason = _classify_exception(root)
    detail = _normalize_text(str(root))
    error_type = type(root).__name__

    message = f"{task_name}에 실패했습니다. 원인: {reason} (오류 유형: {error_type})"
    if detail and detail not in reason:
        message = f"{message} 상세: {detail}"
    return message
