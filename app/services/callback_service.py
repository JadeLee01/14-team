import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class CallbackService:
    def __init__(self, http: httpx.AsyncClient, base_url: str, token: str):
        self.http = http
        self.base_url = base_url.rstrip("/")
        self.token = token

    async def post_checklist_complete(self, case_id: str, payload: dict[str, Any]) -> None:
        logger.info("체크리스트 콜백 시작", extra={"case_id": case_id})
        url = f"{self.base_url}/internal/callbacks/checklists/{case_id}/complete"
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        logger.info("체크리스트 콜백 전송", extra={"case_id": case_id, "payload": payload})

        res = await self.http.post(url, json=payload, headers=headers)
        res.raise_for_status()
        logger.info("체크리스트 콜백 성공", extra={"case_id": case_id})

    async def post_easy_contract_markdown(self, case_id: int, markdown: str) -> None:
        url = f"{self.base_url}/internal/callbacks/easy-contracts/{case_id}/complete"
        headers = {"Content-Type": "text/markdown; charset=utf-8"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        res = await self.http.post(url, content=markdown.encode("utf-8"), headers=headers)
        res.raise_for_status()

    async def post_easy_contract_error(self, case_id: int, code: str, message: str) -> None:
        url = f"{self.base_url}/internal/callbacks/easy-contracts/{case_id}/complete"
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        payload = {"error": {"code": code, "message": message}}
        res = await self.http.post(url, json=payload, headers=headers)
        res.raise_for_status()