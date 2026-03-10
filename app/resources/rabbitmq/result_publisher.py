from __future__ import annotations

from typing import Any

from app.resources.rabbitmq.client import RabbitMQClient
from app.resources.rabbitmq.codec import (
    build_checklist_result_payload,
    build_easy_contract_result_payload,
)


class RabbitMQResultPublisher:
    def __init__(self, *, client: RabbitMQClient, exchange_name: str, routing_key: str) -> None:
        self.client = client
        self.exchange_name = exchange_name
        self.routing_key = routing_key

    async def publish(
        self,
        payload: dict[str, Any],
        *,
        message_id: str | None = None,
        correlation_id: str | None = None,
        message_type: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> bool:
        return await self.client.publish_json(
            exchange_name=self.exchange_name,
            routing_key=self.routing_key,
            payload=payload,
            message_id=message_id,
            correlation_id=correlation_id,
            message_type=message_type,
            headers=headers,
        )

    async def publish_easy_contract_result(
        self,
        *,
        correlation_id: str,
        easy_contract_id: int,
        member_id: int,
        success: bool,
        content: str | None,
        error_message: str | None,
        message_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> bool:
        payload = build_easy_contract_result_payload(
            correlation_id=correlation_id,
            easy_contract_id=easy_contract_id,
            member_id=member_id,
            success=success,
            content=content,
            error_message=error_message,
        )
        return await self.publish(
            payload,
            message_id=message_id,
            correlation_id=correlation_id,
            message_type="easy-contract",
            headers=headers,
        )

    async def publish_checklist_result(
        self,
        *,
        correlation_id: str,
        template_id: int,
        member_id: int,
        success: bool,
        checklists: list[str],
        error_message: str | None,
        message_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> bool:
        payload = build_checklist_result_payload(
            correlation_id=correlation_id,
            template_id=template_id,
            member_id=member_id,
            success=success,
            checklists=checklists,
            error_message=error_message,
        )
        return await self.publish(
            payload,
            message_id=message_id,
            correlation_id=correlation_id,
            message_type="checklist",
            headers=headers,
        )
