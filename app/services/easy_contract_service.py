from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Annotated, Any, TypedDict
from uuid import uuid4

from langgraph.graph import END, StateGraph

from app.resources.ocr.upstage_client import UpstageDocumentParseClient
from app.resources.rabbitmq.codec import now_utc_iso
from app.resources.vectorstore import ChromaVectorStore
from app.resources.vllm.client import VLLMClient
from app.settings import settings
from app.utils.lease_contract_guard import check_is_lease_contract
from app.utils.pii_redaction import redact_phone_and_account
from app.utils.upstage_html import extract_plain_text_from_upstage_json

logger = logging.getLogger(__name__)

OCR_PAGE_LIMITS_BY_DOC_TYPE: dict[str, int] = {
    "contract": 10,
    "registry": 5,
}


class EasyContractCancelled(Exception):
    pass


class NotLeaseContract(Exception):
    pass


def list_concat(left: Any, right: Any):
    if left is None:
        left = []
    if right is None:
        right = []
    return list(left) + list(right)


class EasyContractState(TypedDict, total=False):
    easy_contract_id: int
    correlation_id: str
    ingest_id: str
    is_cancelled: Callable[[int], bool]

    # 입력 파일들(각각 bytes + doc_type)
    docs: list[dict[str, Any]]  # {"filename": str, "bytes": bytes, "doc_type": str}

    # OCR 결과
    pages_text: list[dict[str, Any]]  # {"doc_type","file","page","text"}

    # 문서 종류별 요약 결과
    contract_page_summaries: Annotated[list[dict[str, Any]], list_concat]
    registry_summaries: Annotated[list[dict[str, Any]], list_concat]

    # 최종 합산 페이지 요약
    page_summaries: Annotated[list[dict[str, Any]], list_concat]  # {"doc_type","file","page","summary"}

    # 최종 쉬운계약서(마크다운)
    markdown: str


class EasyContractResult(TypedDict):
    markdown: str
    pages_text: list[dict[str, Any]]
    page_summaries: list[dict[str, Any]]


def _normalize_doc_type(doc_type: Any) -> str:
    normalized = str(doc_type or "").strip().lower()
    return normalized or "contract"


def _page_summary_prompt(doc_type: str, page_no: int, text: str) -> list[dict[str, str]]:
    system = (
        "너는 주택 임대차 문서를 페이지 단위로 읽고 핵심 정보를 추출하는 도우미다.\n"
        "규칙:\n"
        "1) 출력은 간결한 불릿 목록으로만 작성\n"
        "2) 아래 항목을 우선 추출: 계약일/임대차기간/보증금/월세/관리비/특약/위약금/해지조건/수리·하자/원상복구/당사자 정보\n"
        "3) 페이지에 없으면 '없음'으로 쓰지 말고 해당 항목은 생략\n"
        "4) 숫자/날짜는 원문 표현을 최대한 유지\n"
    )
    user = (
        f"[문서타입] {doc_type}\n"
        f"[페이지] {page_no}\n"
        f"[OCR 텍스트]\n{text}\n\n"
        "이 페이지에서 중요한 조항과 계약일/임대차기간/보증금/월세/관리비/특약/위약금/해지조건/수리·하자/원상복구만 불릿으로 정리해줘.\n"
        "반드시 OCR 텍스트에 제시된 내용을 정리하고 없는 정보를 만들어내지 마라.\n"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _registry_summary_prompt(filename: str, text: str) -> list[dict[str, str]]:
    system = (
        "너는 등기부등본 OCR 텍스트를 사실 기반으로 요약하는 도우미다.\n"
        "규칙:\n"
        "1) 출력은 간결한 불릿 목록으로만 작성\n"
        "2) 갑구의 현재 소유자 정보를 우선 요약\n"
        "3) 을구의 현재 소유권 이외 권리를 우선 요약\n"
        "4) 다음 단어가 나오면 반드시 요약에 포함: 가등기, 가처분, 예고등기, 가압류, 압류, 경매개시결정, 신탁, 근저당권\n"
        "5) 비용/금액 정보가 보이면 함께 요약\n"
        "6) OCR 텍스트에 없는 정보는 추측하거나 생성하지 말 것\n"
    )
    user = (
        "[문서타입] registry\n"
        f"[파일] {filename}\n"
        f"[OCR 텍스트 전체]\n{text}\n\n"
        "위 텍스트 전체를 한 번에 읽고 갑구 현재 소유자와 을구 현재 소유권 이외 권리를 중심으로 요약해줘.\n"
        "반드시 OCR 텍스트에 제시된 내용을 정리하고 없는 정보를 만들어내지 마라.\n"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _trim_registry_text(text: str, limit: int = 40000) -> str:
    if len(text) <= limit:
        return text
    head_len = int(limit * 0.65)
    tail_len = limit - head_len
    return text[:head_len] + "\n\n...(중간 일부 생략)...\n\n" + text[-tail_len:]


def _prepare_text_for_llm(text: str, *, max_chars: int) -> str:
    """Reduce OCR duplication noise before sending text to LLM."""
    raw = (text or "").strip()
    if not raw:
        return ""

    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if not lines:
        return raw[:max_chars]

    deduped: list[str] = []
    seen: set[str] = set()
    for line in lines:
        # Normalize for duplicate detection while keeping original line content.
        key = " ".join(line.split()).lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(line)

    cleaned = "\n".join(deduped).strip()
    if not cleaned:
        cleaned = raw
    return cleaned[:max_chars]


def _final_markdown_prompt(page_summaries: list[dict[str, Any]]) -> list[dict[str, str]]:
    system = (
        "너는 주택 임대차 계약서를 쉽게 풀어서 설명하고 분석해서 마크다운으로 출력하는 도우미다.\n"
        "규칙:\n"
        "1) 출력은 마크다운만 (설명문/코드블록 금지)\n"
        "2) 사실에 근거해 작성. 추측 금지\n"
        "3) 섹션 예시(필요한 것만 포함):\n"
        "   - 요약(다섯 문장 이하, 불릿)\n"
        "   - 핵심 조건(표)\n"
        "   - 중요 조항(불릿)\n"
        "   - 위험/주의 포인트(불릿)\n"
        "4) 금액/날짜/기간/당사자/주소 등은 가능한 한 원문 기반으로 명확히하고 월세와 관리비가 모두 있으면 실제 월 납부금액을 핵심조건(표)에 표시\n"
        "5) 등기부등본 해석은 등기부에 기재된 사실만 설명\n"
        "6) 시세/시장가/향후 경매 가능성은 추측하지 말 것\n"
        "7) 위험성은 단정하지 말고 '가능성' 표현으로 제한할 것\n"
        "8) 계약서 임대인과 등기부 갑구 소유자 비교 결과를 명시할 것\n"
        "9) 공동소유로 보이면 소유자 전원 계약 여부 확인 필요를 명시할 것\n"
        "10) 키워드(가등기/가처분/예고등기/가압류/압류/경매개시결정/신탁/근저당권)가 있으면 반드시 주의사항에 반영\n"
        "11) 근저당권이 있으면 금액을 원문 그대로 표기하고, 시세 비교 없이 '보증금 반환이 어려울 수 있다' 수준으로 표현\n"
    )

    contract_lines: list[str] = []
    registry_lines: list[str] = []
    for s in page_summaries:
        line = f"- ({s['doc_type']}/{s['file']} p.{s['page']}) {s['summary']}".strip()
        if _normalize_doc_type(s.get("doc_type")) == "registry":
            registry_lines.append(line)
        else:
            contract_lines.append(line)

    user = (
        "[계약서 페이지별 핵심 요약]\n"
        + ("\n".join(contract_lines) if contract_lines else "- 없음")
        + "\n\n"
        + "[등기부등본 요약]\n"
        + ("\n".join(registry_lines) if registry_lines else "- 없음")
        + "\n\n"
        + "위 요약들을 종합해 쉬운 계약서 결과를 마크다운으로 작성해줘.\n"
        + "특히 위험/주의 포인트 섹션에는 아래를 반드시 반영해줘.\n"
        + "- 과해석 금지: 등기부 기재 사실만 설명, 시세/시장/향후 경매 추측 금지, 위험은 가능성 표현\n"
        + "- 계약서 임대인과 등기부 갑구 소유자 비교(일치/불일치/확인불가)\n"
        + "- 불일치면 반드시 불일치로 표시\n"
        + "- 공동소유일 경우 전원 계약 여부 확인 필요를 명시\n"
        + "- 키워드(가등기/가처분/예고등기/가압류/압류/경매개시결정/신탁/근저당권) 존재 시 주의사항에 포함\n"
        + "- 근저당 설정 시 금액을 그대로 표기하고 시세 비교는 하지 말 것\n"
        + "반드시 제공된 요약만 근거로 작성하고 없는 정보를 만들어내지 마라.\n"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


class EasyContractService:
    def __init__(
        self,
        vllm: VLLMClient,
        ocr: UpstageDocumentParseClient,
        vector_store: ChromaVectorStore | None = None,
    ):
        self.vllm = vllm
        self.ocr = ocr
        self.vector_store = vector_store
        self.graph = self._build_graph()

    def _build_graph(self):
        g = StateGraph(EasyContractState)

        def _check_cancel(state: EasyContractState) -> None:
            easy_contract_id = state.get("easy_contract_id")
            if easy_contract_id is None:
                return
            checker = state.get("is_cancelled")
            if checker and checker(easy_contract_id):
                raise EasyContractCancelled(f"easy_contract_id={easy_contract_id}")

        def _log_extra(state: EasyContractState, **kwargs: Any) -> dict[str, Any]:
            extra = {
                "easy_contract_id": state.get("easy_contract_id"),
                "correlation_id": state.get("correlation_id"),
                "event_time": now_utc_iso(),
            }
            extra.update(kwargs)
            return extra

        def _persist_ocr_page_realtime(state: EasyContractState, page: dict[str, Any]) -> None:
            if self.vector_store is None:
                return
            try:
                inserted = self.vector_store.upsert_easy_contract_ocr(
                    easy_contract_id=state.get("easy_contract_id", -1),
                    pages_text=[page],
                    correlation_id=state.get("correlation_id"),
                    ingest_id=state.get("ingest_id", "unknown"),
                )
                logger.info(
                    "OCR 벡터 실시간 저장 완료",
                    extra=_log_extra(
                        state,
                        doc_filename=page.get("file"),
                        page=page.get("page"),
                        inserted=inserted,
                    ),
                )
            except Exception:
                logger.exception(
                    "OCR 벡터 실시간 저장 실패",
                    extra=_log_extra(
                        state,
                        doc_filename=page.get("file"),
                        page=page.get("page"),
                    ),
                )

        async def ocr_stage(state: EasyContractState) -> EasyContractState:
            logger.info("문서 문자 인식 단계 시작", extra=_log_extra(state))
            pages_text: list[dict[str, Any]] = []
            remaining_pages_by_doc_type = OCR_PAGE_LIMITS_BY_DOC_TYPE.copy()

            for doc in state["docs"]:
                _check_cancel(state)
                filename = doc["filename"]
                doc_type = _normalize_doc_type(doc["doc_type"])
                b = doc["bytes"]
                page_budget = remaining_pages_by_doc_type.get(doc_type)

                if page_budget is not None and page_budget <= 0:
                    logger.info(
                        "문서 타입 OCR 페이지 상한 도달로 문서 스킵",
                        extra=_log_extra(state, doc_filename=filename, doc_type=doc_type),
                    )
                    continue

                if filename.lower().endswith(".pdf"):
                    from app.utils.pdf_images import pdf_bytes_to_png_pages

                    try:
                        logger.info("pdf 이미지 변환 중", extra=_log_extra(state, doc_filename=filename))
                        page_images = pdf_bytes_to_png_pages(
                            b,
                            zoom=2.0,
                            max_pages=page_budget,
                        )
                        logger.info(
                            "pdf 이미지 변환 완료",
                            extra=_log_extra(
                                state,
                                doc_filename=filename,
                                page_count=len(page_images),
                                page_limit=page_budget,
                            ),
                        )
                    except Exception as e:
                        raise RuntimeError("UNPROCESSABLE_DOCUMENT") from e

                    for i, img in enumerate(page_images, start=1):
                        _check_cancel(state)
                        logger.info(
                            "이미지 변환 후 문자 인식 요청",
                            extra=_log_extra(state, doc_filename=filename, page=i),
                        )
                        data = await self.ocr.parse_image(img, filename=f"{filename}.p{i}.png")
                        logger.info(
                            "문자 인식 완료 후 텍스트 추출",
                            extra=_log_extra(state, doc_filename=filename, page=i),
                        )
                        text = extract_plain_text_from_upstage_json(data)
                        sanitized_text = redact_phone_and_account(text)
                        logger.debug(
                            "문자 인식 결과",
                            extra=_log_extra(state, doc_filename=filename, page=i, text_length=len(text)),
                        )
                        page_payload = {
                            "doc_type": doc_type,
                            "file": filename,
                            "page": i,
                            "text": sanitized_text,
                        }
                        pages_text.append(page_payload)
                        _persist_ocr_page_realtime(state, page_payload)
                        if doc_type in remaining_pages_by_doc_type:
                            remaining_pages_by_doc_type[doc_type] = max(0, remaining_pages_by_doc_type[doc_type] - 1)
                else:
                    _check_cancel(state)
                    logger.info("문자 인식 요청", extra=_log_extra(state, doc_filename=filename, page=1))
                    data = await self.ocr.parse_image(b, filename=filename)
                    logger.info(
                        "문자 인식 완료 후 텍스트 추출",
                        extra=_log_extra(state, doc_filename=filename, page=1),
                    )
                    text = extract_plain_text_from_upstage_json(data)
                    sanitized_text = redact_phone_and_account(text)
                    logger.debug(
                        "문자 인식 결과",
                        extra=_log_extra(state, doc_filename=filename, page=1, text_length=len(text)),
                    )
                    page_payload = {
                        "doc_type": doc_type,
                        "file": filename,
                        "page": 1,
                        "text": sanitized_text,
                    }
                    pages_text.append(page_payload)
                    _persist_ocr_page_realtime(state, page_payload)
                    if doc_type in remaining_pages_by_doc_type:
                        remaining_pages_by_doc_type[doc_type] = max(0, remaining_pages_by_doc_type[doc_type] - 1)

            return {"pages_text": pages_text}

        async def contract_page_summarize_stage(state: EasyContractState) -> EasyContractState:
            logger.info("계약서 페이지별 요약 시작", extra=_log_extra(state))
            summaries: list[dict[str, Any]] = []

            for p in state.get("pages_text", []):
                _check_cancel(state)
                if _normalize_doc_type(p.get("doc_type")) != "contract":
                    continue

                txt = (p.get("text") or "").strip()
                if not txt:
                    continue

                prepared_text = _prepare_text_for_llm(txt, max_chars=12000)
                msgs = _page_summary_prompt(p["doc_type"], p["page"], prepared_text)
                logger.info(
                    "계약서 페이지 요약 요청",
                    extra=_log_extra(state, doc_filename=p["file"], page=p["page"]),
                )
                summary = await self.vllm.chat(
                    msgs,
                    temperature=0.0,
                    max_tokens=1024,
                    model=settings.VLLM_LORA_ADAPTER_EASYCONTRACT,
                )
                logger.info(
                    "계약서 페이지 요약 완료",
                    extra=_log_extra(
                        state,
                        doc_filename=p["file"],
                        page=p["page"],
                        summary_length=len(summary),
                    ),
                )
                summaries.append(
                    {
                        "doc_type": p["doc_type"],
                        "file": p["file"],
                        "page": p["page"],
                        "summary": summary.strip(),
                    }
                )

            return {"contract_page_summaries": summaries}

        def sanitize_and_guard_stage(state: EasyContractState) -> EasyContractState:
            logger.info("OCR 텍스트 개인정보 마스킹 및 계약서 판별 시작", extra=_log_extra(state))
            pages_text = state.get("pages_text", [])
            sanitized_pages_text: list[dict[str, Any]] = []
            contract_texts: list[str] = []
            all_texts: list[str] = []

            for page in pages_text:
                raw_text = page.get("text") or ""
                sanitized_text = redact_phone_and_account(raw_text)
                sanitized_pages_text.append({**page, "text": sanitized_text})

                normalized_doc_type = _normalize_doc_type(page.get("doc_type"))
                text_for_check = sanitized_text.strip()
                if not text_for_check:
                    continue
                all_texts.append(text_for_check)
                if normalized_doc_type == "contract":
                    contract_texts.append(text_for_check)

            texts_for_guard = contract_texts if contract_texts else all_texts
            guard = check_is_lease_contract(texts_for_guard)
            logger.info(
                "계약서 판별 결과",
                extra=_log_extra(state, lease_guard_ok=guard.ok, lease_guard_score=guard.score),
            )
            if not guard.ok:
                raise NotLeaseContract("입력하신 문서가 계약서가 아닙니다. 문서를 다시 확인해주세요.")

            return {"pages_text": sanitized_pages_text}

        def persist_ocr_stage(state: EasyContractState) -> EasyContractState:
            pages_text = state.get("pages_text", [])
            if self.vector_store is None:
                # LangGraph requires every node to write at least one tracked key.
                return {"pages_text": pages_text}
            try:
                inserted = self.vector_store.upsert_easy_contract_ocr(
                    easy_contract_id=state.get("easy_contract_id", -1),
                    pages_text=pages_text,
                    correlation_id=state.get("correlation_id"),
                    ingest_id=state.get("ingest_id", "unknown"),
                )
                logger.info("OCR 벡터 저장 완료", extra=_log_extra(state, inserted=inserted))
            except Exception:
                logger.exception("OCR 벡터 저장 실패", extra=_log_extra(state))
            return {"pages_text": pages_text}

        async def registry_summarize_stage(state: EasyContractState) -> EasyContractState:
            logger.info("등기부등본 요약 시작", extra=_log_extra(state))
            summaries: list[dict[str, Any]] = []
            pages_by_file: dict[str, list[dict[str, Any]]] = {}

            for p in state.get("pages_text", []):
                if _normalize_doc_type(p.get("doc_type")) != "registry":
                    continue
                pages_by_file.setdefault(p["file"], []).append(p)

            for filename, pages in pages_by_file.items():
                _check_cancel(state)
                sorted_pages = sorted(pages, key=lambda item: int(item.get("page", 0)))
                merged_chunks: list[str] = []
                for p in sorted_pages:
                    txt = (p.get("text") or "").strip()
                    if not txt:
                        continue
                    merged_chunks.append(f"[페이지 {p['page']}]\n{txt}")

                if not merged_chunks:
                    continue

                merged_text = _trim_registry_text("\n\n".join(merged_chunks))
                merged_text = _prepare_text_for_llm(merged_text, max_chars=16000)
                msgs = _registry_summary_prompt(filename, merged_text)
                logger.info(
                    "등기부등본 요약 요청",
                    extra=_log_extra(state, doc_filename=filename, page_count=len(sorted_pages)),
                )
                summary = await self.vllm.chat(
                    msgs,
                    temperature=0.0,
                    max_tokens=1024,
                    model=settings.VLLM_LORA_ADAPTER_EASYCONTRACT,
                )
                logger.info(
                    "등기부등본 요약 완료",
                    extra=_log_extra(state, doc_filename=filename, summary_length=len(summary)),
                )
                summaries.append({"doc_type": "registry", "file": filename, "page": 1, "summary": summary.strip()})

            return {"registry_summaries": summaries}

        def merge_summaries_stage(state: EasyContractState) -> EasyContractState:
            contract_summaries = state.get("contract_page_summaries", [])
            registry_summaries = state.get("registry_summaries", [])
            return {"page_summaries": [*contract_summaries, *registry_summaries]}

        async def final_stage(state: EasyContractState) -> EasyContractState:
            _check_cancel(state)
            msgs = _final_markdown_prompt(state.get("page_summaries", []))
            logger.info("쉬운계약서 생성 요청", extra=_log_extra(state))
            md = await self.vllm.chat(
                msgs,
                temperature=0.0,
                max_tokens=2048,
                model=settings.VLLM_LORA_ADAPTER_EASYCONTRACT,
            )
            logger.info("쉬운계약서 생성 완료", extra=_log_extra(state))
            markdown = md.strip()
            logger.debug("마크다운 생성 완료", extra=_log_extra(state, length=len(markdown)))
            if self.vector_store is not None and markdown:
                try:
                    inserted = self.vector_store.upsert_easy_contract_markdown(
                        easy_contract_id=state.get("easy_contract_id", -1),
                        markdown=markdown,
                        correlation_id=state.get("correlation_id"),
                        ingest_id=state.get("ingest_id", "unknown"),
                    )
                    logger.info("쉬운계약서 벡터 저장 완료", extra=_log_extra(state, inserted=inserted))
                except Exception:
                    logger.exception("쉬운계약서 벡터 저장 실패", extra=_log_extra(state))
            return {"markdown": markdown}

        # ---- graph wiring ----
        g.add_node("ocr", ocr_stage)
        g.add_node("sanitize_and_guard", sanitize_and_guard_stage)
        g.add_node("persist_ocr", persist_ocr_stage)
        g.add_node("contract_page_summarize", contract_page_summarize_stage)
        g.add_node("registry_summarize", registry_summarize_stage)
        g.add_node("merge_summaries", merge_summaries_stage)
        g.add_node("final", final_stage)

        g.set_entry_point("ocr")

        # guard + sanitize 이후 fan-out (병렬)
        g.add_edge("ocr", "sanitize_and_guard")
        g.add_edge("sanitize_and_guard", "persist_ocr")
        g.add_edge("persist_ocr", "contract_page_summarize")
        g.add_edge("persist_ocr", "registry_summarize")

        # join
        g.add_edge(["contract_page_summarize", "registry_summarize"], "merge_summaries")
        g.add_edge("merge_summaries", "final")
        g.add_edge("final", END)

        return g.compile()

    async def generate(
        self,
        easy_contract_id: int,
        docs: list[dict[str, Any]],
        *,
        correlation_id: str | None = None,
        is_cancelled: Callable[[int], bool] | None = None,
    ) -> str:
        out = await self._run(
            easy_contract_id=easy_contract_id,
            docs=docs,
            correlation_id=correlation_id,
            is_cancelled=is_cancelled,
        )
        return out.get("markdown", "")

    async def generate_with_details(
        self,
        easy_contract_id: int,
        docs: list[dict[str, Any]],
        *,
        correlation_id: str | None = None,
        is_cancelled: Callable[[int], bool] | None = None,
    ) -> EasyContractResult:
        out = await self._run(
            easy_contract_id=easy_contract_id,
            docs=docs,
            correlation_id=correlation_id,
            is_cancelled=is_cancelled,
        )
        return {
            "markdown": out.get("markdown", ""),
            "pages_text": out.get("pages_text", []),
            "page_summaries": out.get("page_summaries", []),
        }

    async def _run(
        self,
        easy_contract_id: int,
        docs: list[dict[str, Any]],
        *,
        correlation_id: str | None = None,
        is_cancelled: Callable[[int], bool] | None = None,
    ) -> EasyContractState:
        logger.info(
            "쉬운 계약서 생성 시작",
            extra={
                "easy_contract_id": easy_contract_id,
                "correlation_id": correlation_id,
                "event_time": now_utc_iso(),
            },
        )

        normalized_docs = [{**doc, "doc_type": _normalize_doc_type(doc.get("doc_type"))} for doc in docs]

        state: EasyContractState = {
            "easy_contract_id": easy_contract_id,
            "ingest_id": correlation_id or f"{easy_contract_id}:{uuid4().hex}",
            "docs": normalized_docs,
            "pages_text": [],
            "contract_page_summaries": [],
            "registry_summaries": [],
            "page_summaries": [],
        }
        if correlation_id:
            state["correlation_id"] = correlation_id
        if is_cancelled:
            state["is_cancelled"] = is_cancelled

        out = await self.graph.ainvoke(state)

        logger.info(
            "쉬운 계약서 생성 완료",
            extra={
                "easy_contract_id": easy_contract_id,
                "correlation_id": correlation_id,
                "length": len(out.get("markdown", "")),
                "event_time": now_utc_iso(),
            },
        )
        return out
