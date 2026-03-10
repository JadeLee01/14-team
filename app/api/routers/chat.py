from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import StreamingResponse

from app.api.deps import get_container
from app.api.schemas.chat import ChatRequest

router = APIRouter(prefix="/api/chat", tags=["chat"])


def _sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


@router.post("/{easy_contract_id}/stream")
async def stream_chat(
    body: ChatRequest,
    easy_contract_id: int = Path(..., description="쉬운계약서 식별자(int)"),
    container=Depends(get_container),
):
    question = (body.question or "").strip()
    if not question:
        raise HTTPException(
            status_code=400,
            detail={"error": {"code": "INVALID_INPUT", "message": "question은 비어있을 수 없습니다."}},
        )

    try:
        context = container.chat_service.build_chat_context(
            easy_contract_id=easy_contract_id,
            question=question,
            include_corpus=body.include_corpus,
            top_k_contract=body.top_k_contract,
            top_k_corpus=body.top_k_corpus,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=404,
            detail={"error": {"code": "CONTEXT_NOT_FOUND", "message": str(exc)}},
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={"error": {"code": "CHAT_UNAVAILABLE", "message": str(exc)}},
        ) from exc

    async def event_stream():
        yield _sse_event(
            "meta",
            {
                "easy_contract_id": easy_contract_id,
                "contract_hits": context["contract_hits"],
                "corpus_hits": context["corpus_hits"],
            },
        )
        try:
            if not body.stream:
                text = await container.chat_service.answer(messages=context["messages"])
                yield _sse_event(
                    "done",
                    {
                        "ok": True,
                        "easy_contract_id": easy_contract_id,
                        "contract_hits": context["contract_hits"],
                        "corpus_hits": context["corpus_hits"],
                        "text": text,
                    },
                )
                return

            async for token in container.chat_service.stream_answer(messages=context["messages"]):
                yield _sse_event("token", {"text": token})
            yield _sse_event("done", {"ok": True})
        except Exception as exc:
            yield _sse_event("error", {"message": str(exc)})

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/{easy_contract_id}")
async def chat_once(
    body: ChatRequest,
    easy_contract_id: int = Path(..., description="쉬운계약서 식별자(int)"),
    container=Depends(get_container),
):
    question = (body.question or "").strip()
    if not question:
        raise HTTPException(
            status_code=400,
            detail={"error": {"code": "INVALID_INPUT", "message": "question은 비어있을 수 없습니다."}},
        )

    try:
        context = container.chat_service.build_chat_context(
            easy_contract_id=easy_contract_id,
            question=question,
            include_corpus=body.include_corpus,
            top_k_contract=body.top_k_contract,
            top_k_corpus=body.top_k_corpus,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=404,
            detail={"error": {"code": "CONTEXT_NOT_FOUND", "message": str(exc)}},
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={"error": {"code": "CHAT_UNAVAILABLE", "message": str(exc)}},
        ) from exc

    try:
        answer = await container.chat_service.answer(messages=context["messages"])
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail={"error": {"code": "LLM_FAILED", "message": str(exc)}},
        ) from exc

    return {
        "ok": True,
        "easy_contract_id": easy_contract_id,
        "contract_hits": context["contract_hits"],
        "corpus_hits": context["corpus_hits"],
        "text": answer,
    }
