from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from aio_pika.abc import AbstractIncomingMessage

from app.resources.rabbitmq.client import RabbitMQClient
from app.resources.rabbitmq.codec import decode_json_message
from app.resources.rabbitmq.result_publisher import RabbitMQResultPublisher
from app.utils.error_messages import format_task_error

logger = logging.getLogger(__name__)

MessageHandler = Callable[[AbstractIncomingMessage], Awaitable[None]]


class RabbitMQWorker:
    def __init__(
        self,
        *,
        client: RabbitMQClient,
        easy_contract_queue: str,
        checklist_queue: str,
        easy_contract_cancel_queue: str,
        easy_contract_handler: MessageHandler,
        checklist_handler: MessageHandler,
        easy_contract_cancel_handler: MessageHandler,
        result_publisher: RabbitMQResultPublisher | None = None,
        retry_max_attempts: int = 3,
        retry_backoff_base_sec: float = 0.5,
    ) -> None:
        self.client = client
        self.easy_contract_queue = easy_contract_queue
        self.checklist_queue = checklist_queue
        self.easy_contract_cancel_queue = easy_contract_cancel_queue
        self.easy_contract_handler = easy_contract_handler
        self.checklist_handler = checklist_handler
        self.easy_contract_cancel_handler = easy_contract_cancel_handler
        self.result_publisher = result_publisher
        self.retry_max_attempts = max(1, retry_max_attempts)
        self.retry_backoff_base_sec = max(retry_backoff_base_sec, 0.0)
        self._consumer_tags: dict[str, str] = {}
        self._started = False

    async def start(self) -> None:
        if self._started:
            return

        self._consumer_tags[self.easy_contract_queue] = await self.client.consume(
            self.easy_contract_queue,
            self._wrap_handler(self.easy_contract_handler, queue_name=self.easy_contract_queue),
        )
        self._consumer_tags[self.checklist_queue] = await self.client.consume(
            self.checklist_queue,
            self._wrap_handler(self.checklist_handler, queue_name=self.checklist_queue),
        )
        self._consumer_tags[self.easy_contract_cancel_queue] = await self.client.consume(
            self.easy_contract_cancel_queue,
            self._wrap_handler(self.easy_contract_cancel_handler, queue_name=self.easy_contract_cancel_queue),
        )
        self._started = True
        logger.info("래빗엠큐 워커 시작")

    async def stop(self) -> None:
        if not self._started:
            return
        for queue_name, consumer_tag in list(self._consumer_tags.items()):
            try:
                await self.client.cancel_consumer(queue_name, consumer_tag)
            except Exception:
                logger.exception(
                    "래빗엠큐 컨슈머 중지 실패",
                    extra={"queue": queue_name, "consumer_tag": consumer_tag},
                )
        self._consumer_tags.clear()
        self._started = False
        logger.info("래빗엠큐 워커 종료")

    def _wrap_handler(self, handler: MessageHandler, *, queue_name: str) -> MessageHandler:
        async def wrapped(message: AbstractIncomingMessage) -> None:
            last_exc: Exception | None = None
            for attempt in range(1, self.retry_max_attempts + 1):
                try:
                    await handler(message)
                    return
                except Exception as exc:
                    last_exc = exc
                    logger.exception(
                        "메시지 핸들러 처리 중 예외 발생",
                        extra={
                            "queue": queue_name,
                            "attempt": attempt,
                            "max_attempts": self.retry_max_attempts,
                        },
                    )
                    if message.processed:
                        return
                    if attempt < self.retry_max_attempts:
                        backoff_sec = self.retry_backoff_base_sec * (2 ** (attempt - 1))
                        if backoff_sec > 0:
                            await asyncio.sleep(backoff_sec)
                        continue

            terminal_exc = RuntimeError(
                f"내부 처리 오류 재시도 {self.retry_max_attempts}회 초과: {last_exc}"
            )
            publish_ok = await self._publish_fallback_error(
                queue_name=queue_name,
                message=message,
                exc=terminal_exc,
            )
            if message.processed:
                return
            try:
                if publish_ok:
                    await message.ack()
                else:
                    await message.nack(requeue=False)
            except Exception:
                logger.exception(
                    "메시지 ack/nack 처리 실패",
                    extra={"queue": queue_name, "fallback_publish_ok": publish_ok},
                )

        return wrapped

    async def _publish_fallback_error(
        self,
        *,
        queue_name: str,
        message: AbstractIncomingMessage,
        exc: Exception,
    ) -> bool:
        if self.result_publisher is None:
            return False
        if queue_name not in {self.easy_contract_queue, self.checklist_queue}:
            return False

        payload = self._safe_decode_payload(message.body)
        correlation_id = self._extract_str_candidate(
            payload,
            "correlation_id",
            str(message.correlation_id or message.message_id or "unknown"),
        )
        member_id = self._extract_int_candidate(payload, "member_id", -1)

        if queue_name == self.easy_contract_queue:
            easy_contract_id = self._extract_int_candidate(payload, "easy_contract_id", -1)
            return await self.result_publisher.publish_easy_contract_result(
                correlation_id=correlation_id,
                easy_contract_id=easy_contract_id,
                member_id=member_id,
                success=False,
                content=None,
                error_message=format_task_error("쉬운 계약서 생성", exc),
                message_id=message.message_id,
            )

        template_id = self._extract_int_candidate(payload, "template_id", -1)
        return await self.result_publisher.publish_checklist_result(
            correlation_id=correlation_id,
            template_id=template_id,
            member_id=member_id,
            success=False,
            checklists=[],
            error_message=format_task_error("체크리스트 생성", exc),
            message_id=message.message_id,
        )

    def _safe_decode_payload(self, body: bytes) -> dict[str, Any]:
        try:
            decoded = decode_json_message(body)
            return decoded if isinstance(decoded, dict) else {}
        except Exception:
            return {}

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
