from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def encode_json_message(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def decode_json_message(body: bytes) -> dict[str, Any]:
    raw = body.decode("utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("메시지 본문은 JSON 객체여야 합니다.")
    return data


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key}는 비어있지 않은 문자열이어야 합니다.")
    return value.strip()


def _require_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool):
        raise ValueError(f"{key}는 정수여야 합니다.")
    if not isinstance(value, int):
        raise ValueError(f"{key}는 정수여야 합니다.")
    return value


def parse_easy_contract_request(payload: dict[str, Any]) -> dict[str, Any]:
    correlation_id = _require_str(payload, "correlation_id")
    easy_contract_id = _require_int(payload, "easy_contract_id")
    member_id = _require_int(payload, "member_id")

    files = payload.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("files는 1개 이상의 객체 배열이어야 합니다.")

    normalized_files: list[dict[str, str]] = []
    for index, item in enumerate(files, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"files[{index}]는 객체여야 합니다.")

        doc_type = _require_str(item, "doc_type")
        filename = _require_str(item, "filename")
        url = _require_str(item, "url")

        normalized_files.append(
            {
                "doc_type": doc_type,
                "filename": filename,
                "url": url,
            }
        )

    return {
        "correlation_id": correlation_id,
        "easy_contract_id": easy_contract_id,
        "member_id": member_id,
        "files": normalized_files,
    }


def parse_checklist_request(payload: dict[str, Any]) -> dict[str, Any]:
    correlation_id = _require_str(payload, "correlation_id")
    template_id = _require_int(payload, "template_id")
    member_id = _require_int(payload, "member_id")

    keywords = payload.get("keywords")
    if not isinstance(keywords, list):
        raise ValueError("keywords는 문자열 배열이어야 합니다.")
    if any(not isinstance(item, str) for item in keywords):
        raise ValueError("keywords는 문자열 배열이어야 합니다.")

    return {
        "correlation_id": correlation_id,
        "template_id": template_id,
        "member_id": member_id,
        "keywords": keywords,
    }


def parse_easy_contract_cancel(payload: dict[str, Any]) -> dict[str, Any]:
    easy_contract_id = _require_int(payload, "easy_contract_id")
    return {"easy_contract_id": easy_contract_id}


def build_easy_contract_result_payload(
    *,
    correlation_id: str,
    easy_contract_id: int,
    member_id: int,
    success: bool,
    content: str | None,
    error_message: str | None,
) -> dict[str, Any]:
    return {
        "type": "easy-contract",
        "correlation_id": correlation_id,
        "easy_contract_id": easy_contract_id,
        "member_id": member_id,
        "success": success,
        "content": content,
        "error_message": error_message,
    }


def build_checklist_result_payload(
    *,
    correlation_id: str,
    template_id: int,
    member_id: int,
    success: bool,
    checklists: list[str],
    error_message: str | None,
) -> dict[str, Any]:
    return {
        "type": "checklist",
        "correlation_id": correlation_id,
        "member_id": member_id,
        "success": success,
        "error_message": error_message,
        "template_id": template_id,
        "checklists": checklists,
    }
