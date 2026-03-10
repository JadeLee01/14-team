from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any

from app.resources.vectorstore import ChromaVectorStore
from app.resources.vllm.client import VLLMClient
from app.settings import settings

logger = logging.getLogger(__name__)


class ChatService:
    def __init__(
        self,
        *,
        vllm: VLLMClient,
        vector_store: ChromaVectorStore | None,
    ) -> None:
        self.vllm = vllm
        self.vector_store = vector_store

    def _build_messages(
        self,
        *,
        question: str,
        easy_contract_id: int,
        contract_chunks: list[dict[str, Any]],
        corpus_chunks: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        contract_lines: list[str] = []
        for idx, row in enumerate(contract_chunks, start=1):
            meta = row.get("metadata") or {}
            source = str(meta.get("source") or "contract")
            file_name = str(meta.get("file") or "")
            page = meta.get("page")
            prefix = f"[C{idx}][{source}]"
            if file_name:
                prefix += f"[{file_name}]"
            if page is not None:
                prefix += f"[p.{page}]"
            contract_lines.append(f"{prefix} {row.get('document', '')}")

        corpus_lines: list[str] = []
        for idx, row in enumerate(corpus_chunks, start=1):
            meta = row.get("metadata") or {}
            case_id = str(meta.get("case_id") or "unknown")
            section = str(meta.get("section") or "unknown")
            corpus_lines.append(f"[R{idx}][{case_id}/{section}] {row.get('document', '')}")

        system = (
            "너는 주택 임대차 계약 분석 도우미다.\n"
            "규칙:\n"
            "1) 반드시 제공된 근거를 우선 사용한다.\n"
            "2) 근거가 불충분하면 '확인 불가'를 명시한다.\n"
            "3) 사실과 의견을 구분해 작성한다.\n"
            "4) 마지막에 '근거' 섹션으로 사용한 근거 ID(C1, R1 등)를 나열한다.\n"
            "5) 한국어로 간결하게 답한다.\n"
        )
        user = (
            f"[easy_contract_id]\n{easy_contract_id}\n\n"
            "[계약서 기반 근거]\n"
            + ("\n".join(contract_lines) if contract_lines else "(없음)")
            + "\n\n[분쟁사례 근거]\n"
            + ("\n".join(corpus_lines) if corpus_lines else "(없음)")
            + f"\n\n[질문]\n{question}\n"
        )
        return [{"role": "system", "content": system}, {"role": "user", "content": user}]

    def build_chat_context(
        self,
        *,
        easy_contract_id: int,
        question: str,
        include_corpus: bool,
        top_k_contract: int,
        top_k_corpus: int,
    ) -> dict[str, Any]:
        if self.vector_store is None:
            raise RuntimeError("VECTOR_STORE_NOT_READY")

        contract_chunks = self.vector_store.query_contract_chunks(
            easy_contract_id=easy_contract_id,
            question=question,
            limit=top_k_contract,
        )
        corpus_chunks: list[dict[str, Any]] = []
        if include_corpus:
            corpus_chunks = self.vector_store.query_corpus_chunks(
                question=question,
                limit=top_k_corpus,
            )

        if not contract_chunks:
            raise ValueError("해당 계약서의 벡터 데이터가 없습니다.")

        messages = self._build_messages(
            question=question,
            easy_contract_id=easy_contract_id,
            contract_chunks=contract_chunks,
            corpus_chunks=corpus_chunks,
        )
        return {
            "messages": messages,
            "contract_hits": len(contract_chunks),
            "corpus_hits": len(corpus_chunks),
        }

    async def stream_answer(
        self,
        *,
        messages: list[dict[str, str]],
    ) -> AsyncIterator[str]:
        # Prefer true streaming if server supports it.
        emitted = False
        async for token in self.vllm.chat_stream(
            messages,
            temperature=0.2,
            max_tokens=1024,
            model=settings.VLLM_MODEL,
        ):
            emitted = True
            yield token

        # Some providers may not emit stream chunks; fallback to one-shot response.
        if not emitted:
            text = await self.vllm.chat(
                messages,
                temperature=0.2,
                max_tokens=1024,
                model=settings.VLLM_MODEL,
            )
            if text:
                yield text

    async def answer(
        self,
        *,
        messages: list[dict[str, str]],
    ) -> str:
        text = await self.vllm.chat(
            messages,
            temperature=0.2,
            max_tokens=1024,
            model=settings.VLLM_MODEL,
        )
        return text.strip()
