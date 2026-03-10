from __future__ import annotations

import logging

from aio_pika.abc import AbstractIncomingMessage

from app.core.errors import ExternalServiceRetryExhausted
from app.resources.rabbitmq.codec import decode_json_message, now_utc_iso, parse_checklist_request
from app.resources.rabbitmq.result_publisher import RabbitMQResultPublisher
from app.services.checklist_service import ChecklistService
from app.utils.error_messages import format_task_error

logger = logging.getLogger(__name__)


class ChecklistMessageHandler:
    def __init__(
        self,
        *,
        checklist_service: ChecklistService,
        result_publisher: RabbitMQResultPublisher,
    ) -> None:
        self.checklist_service = checklist_service
        self.result_publisher = result_publisher

    async def handle(self, message: AbstractIncomingMessage) -> None:
        correlation_id = self._fallback_correlation_id(message)
        template_id = -1
        member_id = -1
        success = False
        checklists: list[str] = []
        error_message: str | None = "체크리스트 생성에 실패했습니다."

        logger.info(
            "체크리스트 요청 메시지 수신",
            extra={
                "correlation_id": correlation_id,
                "template_id": template_id,
                "member_id": member_id,
                "event_time": now_utc_iso(),
            },
        )

        try:
            payload = decode_json_message(message.body)
            correlation_id = self._extract_str_candidate(payload, "correlation_id", correlation_id)
            template_id = self._extract_int_candidate(payload, "template_id", template_id)
            member_id = self._extract_int_candidate(payload, "member_id", member_id)
            request = parse_checklist_request(payload)
            correlation_id = request["correlation_id"]
            template_id = request["template_id"]
            member_id = request["member_id"]
            keywords = request["keywords"]

            checklists = await self.checklist_service.generate(template_id=template_id, keywords=keywords)
            success = True
            error_message = None

        except ValueError as exc:
            success = False
            checklists = []
            error_message = format_task_error("체크리스트 생성", exc)
        except ExternalServiceRetryExhausted as exc:
            success = False
            checklists = []
            error_message = str(exc)
        except Exception:
            logger.exception("체크리스트 메시지 처리 실패")
            raise

        publish_ok = await self._publish_result(
            correlation_id=correlation_id,
            template_id=template_id,
            member_id=member_id,
            success=success,
            checklists=checklists,
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
        template_id: int,
        member_id: int,
        success: bool,
        checklists: list[str],
        error_message: str | None,
        message: AbstractIncomingMessage,
    ) -> bool:
        try:
            publish_ok = await self.result_publisher.publish_checklist_result(
                correlation_id=correlation_id,
                template_id=template_id,
                member_id=member_id,
                success=success,
                checklists=checklists,
                error_message=error_message,
                message_id=message.message_id,
            )
            if not publish_ok:
                logger.error(
                    "체크리스트 결과 발행 실패(재시도 소진)",
                    extra={
                        "correlation_id": correlation_id,
                        "template_id": template_id,
                        "member_id": member_id,
                        "success": success,
                        "event_time": now_utc_iso(),
                    },
                )
                return False
            logger.info(
                "체크리스트 결과 메시지 발행 완료",
                extra={
                    "correlation_id": correlation_id,
                    "template_id": template_id,
                    "member_id": member_id,
                    "success": success,
                    "event_time": now_utc_iso(),
                },
            )
            return True
        except Exception:
            logger.exception(
                "체크리스트 결과 발행 실패",
                extra={
                    "correlation_id": correlation_id,
                    "template_id": template_id,
                    "member_id": member_id,
                    "success": success,
                    "event_time": now_utc_iso(),
                },
            )
            return False

    def _fallback_correlation_id(self, message: AbstractIncomingMessage) -> str:
        return str(message.correlation_id or message.message_id or "unknown")

    def _extract_str_candidate(self, payload: dict[str, object], key: str, default: str) -> str:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        return default

    def _extract_int_candidate(self, payload: dict[str, object], key: str, default: int) -> int:
        value = payload.get(key)
        if isinstance(value, bool):
            return default
        if isinstance(value, int):
            return value
        return default
