from __future__ import annotations

import logging

from aio_pika.abc import AbstractIncomingMessage

from app.resources.rabbitmq.codec import decode_json_message, now_utc_iso, parse_easy_contract_cancel
from app.services.cancel_registry import CancelRegistry

logger = logging.getLogger(__name__)


class EasyContractCancelMessageHandler:
    def __init__(self, *, cancel_registry: CancelRegistry) -> None:
        self.cancel_registry = cancel_registry

    async def handle(self, message: AbstractIncomingMessage) -> None:
        try:
            payload = decode_json_message(message.body)
            request = parse_easy_contract_cancel(payload)
            easy_contract_id = request["easy_contract_id"]
            self.cancel_registry.mark_cancelled(easy_contract_id)
            logger.info(
                "쉬운 계약서 취소 요청 수신",
                extra={"easy_contract_id": easy_contract_id, "event_time": now_utc_iso()},
            )
        except Exception:
            logger.exception("쉬운 계약서 취소 메시지 처리 실패")
        finally:
            if not message.processed:
                await message.ack()
