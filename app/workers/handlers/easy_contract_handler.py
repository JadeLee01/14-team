from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from aio_pika.abc import AbstractIncomingMessage

from app.core.errors import ExternalServiceRetryExhausted
from app.resources.rabbitmq.codec import (
    decode_json_message,
    now_utc_iso,
    parse_easy_contract_request,
)
from app.resources.rabbitmq.result_publisher import RabbitMQResultPublisher
from app.services.cancel_registry import CancelRegistry
from app.services.easy_contract_service import (
    EasyContractCancelled,
    EasyContractService,
    NotLeaseContract,
)
from app.utils.error_messages import format_task_error

logger = logging.getLogger(__name__)


class EasyContractMessageHandler:
    def __init__(
        self,
        *,
        http: httpx.AsyncClient,
        easy_contract_service: EasyContractService,
        result_publisher: RabbitMQResultPublisher,
        cancel_registry: CancelRegistry,
    ) -> None:
        self.http = http
        self.easy_contract_service = easy_contract_service
        self.result_publisher = result_publisher
        self.cancel_registry = cancel_registry

    async def handle(self, message: AbstractIncomingMessage) -> None:
        correlation_id = self._fallback_correlation_id(message)
        easy_contract_id = -1
        member_id = -1
        success = False
        content: str | None = None
        error_message: str | None = "쉬운 계약서 생성에 실패했습니다."
        cancelled = False

        logger.info(
            "쉬운 계약서 요청 메시지 수신",
            extra={
                "correlation_id": correlation_id,
                "easy_contract_id": easy_contract_id,
                "member_id": member_id,
                "event_time": now_utc_iso(),
            },
        )

        try:
            payload = decode_json_message(message.body)
            correlation_id = self._extract_str_candidate(payload, "correlation_id", correlation_id)
            easy_contract_id = self._extract_int_candidate(payload, "easy_contract_id", easy_contract_id)
            member_id = self._extract_int_candidate(payload, "member_id", member_id)
            request = parse_easy_contract_request(payload)
            correlation_id = request["correlation_id"]
            easy_contract_id = request["easy_contract_id"]
            member_id = request["member_id"]
            docs = await self._extract_docs(request)

            if self.cancel_registry.is_cancelled(easy_contract_id):
                cancelled = True
                logger.info(
                    "쉬운 계약서 생성 시작 전 취소 감지",
                    extra={
                        "correlation_id": correlation_id,
                        "easy_contract_id": easy_contract_id,
                        "member_id": member_id,
                        "event_time": now_utc_iso(),
                    },
                )

            if not cancelled:
                markdown = await self.easy_contract_service.generate(
                    easy_contract_id=easy_contract_id,
                    docs=docs,
                    correlation_id=correlation_id,
                    is_cancelled=self.cancel_registry.is_cancelled,
                )
                if self.cancel_registry.is_cancelled(easy_contract_id):
                    cancelled = True
                    logger.info(
                        "쉬운 계약서 생성 완료 후 응답 발행 직전 취소 감지",
                        extra={
                            "correlation_id": correlation_id,
                            "easy_contract_id": easy_contract_id,
                            "member_id": member_id,
                            "event_time": now_utc_iso(),
                        },
                    )
                else:
                    success = True
                    content = markdown
                    error_message = None

        except EasyContractCancelled:
            cancelled = True
        except NotLeaseContract as exc:
            success = False
            content = None
            error_message = str(exc)
        except ValueError as exc:
            success = False
            content = None
            error_message = format_task_error("쉬운 계약서 생성", exc)
        except ExternalServiceRetryExhausted as exc:
            success = False
            content = None
            error_message = str(exc)
        except Exception:
            logger.exception("쉬운 계약서 메시지 처리 실패")
            raise

        if easy_contract_id >= 0 and self.cancel_registry.is_cancelled(easy_contract_id):
            cancelled = True

        if cancelled:
            logger.info(
                "취소된 쉬운 계약서 요청으로 응답 메시지 발행 생략",
                extra={
                    "correlation_id": correlation_id,
                    "easy_contract_id": easy_contract_id,
                    "member_id": member_id,
                    "event_time": now_utc_iso(),
                },
            )
            if not message.processed:
                await message.ack()
            return

        publish_ok = await self._publish_result(
            correlation_id=correlation_id,
            easy_contract_id=easy_contract_id,
            member_id=member_id,
            success=success,
            content=content,
            error_message=error_message,
            message=message,
        )
        if publish_ok and not message.processed:
            await message.ack()
        elif not publish_ok and not message.processed:
            await message.nack(requeue=False)

    async def _publish_result(
        self,
        *,
        correlation_id: str,
        easy_contract_id: int,
        member_id: int,
        success: bool,
        content: str | None,
        error_message: str | None,
        message: AbstractIncomingMessage,
    ) -> bool:
        try:
            publish_ok = await self.result_publisher.publish_easy_contract_result(
                correlation_id=correlation_id,
                easy_contract_id=easy_contract_id,
                member_id=member_id,
                success=success,
                content=content,
                error_message=error_message,
                message_id=message.message_id,
            )
            if not publish_ok:
                logger.error(
                    "쉬운 계약서 결과 발행 실패(재시도 소진)",
                    extra={
                        "correlation_id": correlation_id,
                        "easy_contract_id": easy_contract_id,
                        "member_id": member_id,
                        "success": success,
                        "event_time": now_utc_iso(),
                    },
                )
                return False
            logger.info(
                "쉬운 계약서 결과 메시지 발행 완료",
                extra={
                    "correlation_id": correlation_id,
                    "easy_contract_id": easy_contract_id,
                    "member_id": member_id,
                    "success": success,
                    "event_time": now_utc_iso(),
                },
            )
            return True
        except Exception:
            logger.exception(
                "쉬운 계약서 결과 발행 실패",
                extra={
                    "correlation_id": correlation_id,
                    "easy_contract_id": easy_contract_id,
                    "member_id": member_id,
                    "success": success,
                    "event_time": now_utc_iso(),
                },
            )
            return False

    def _fallback_correlation_id(self, message: AbstractIncomingMessage) -> str:
        return str(message.correlation_id or message.message_id or "unknown")

    def _extract_str_candidate(self, payload: dict[str, Any], key: str, default: str) -> str:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        return default

    def _extract_int_candidate(self, payload: dict[str, Any], key: str, default: int) -> int:
        value = payload.get(key)
        if isinstance(value, bool):
            return default
        if isinstance(value, int):
            return value
        return default

    async def _extract_docs(self, request: dict[str, Any]) -> list[dict[str, Any]]:
        normalized_files = request["files"]
        docs: list[dict[str, Any]] = []
        for idx, file_meta in enumerate(normalized_files, start=1):
            url = file_meta["url"]
            doc_type = file_meta["doc_type"]
            filename = file_meta["filename"] or self._filename_from_url(url) or f"file_{idx}"
            file_bytes = await self._download(url)
            if not file_bytes:
                raise ValueError("비어있는 파일은 처리할 수 없습니다.")
            docs.append({"filename": filename, "bytes": file_bytes, "doc_type": doc_type})

        return docs

    async def _download(self, url: str) -> bytes:
        try:
            res = await self.http.get(url)
            res.raise_for_status()
            return res.content
        except httpx.TimeoutException as exc:
            raise ValueError("파일 다운로드 시간이 초과되었습니다. 잠시 후 다시 시도해주세요.") from exc
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            raise ValueError(f"파일 다운로드 실패: 파일 서버가 HTTP {status}를 반환했습니다.") from exc
        except httpx.ConnectError as exc:
            raise ValueError("파일 다운로드 실패: 파일 서버에 연결할 수 없습니다.") from exc
        except httpx.RequestError as exc:
            raise ValueError("파일 다운로드 중 네트워크 오류가 발생했습니다.") from exc

    def _filename_from_url(self, url: str) -> str:
        return Path(urlparse(url).path).name
