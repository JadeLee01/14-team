from __future__ import annotations

import json
import logging
import re
from typing import TypedDict

from langgraph.graph import END, StateGraph

logger = logging.getLogger(__name__)

from app.resources.rabbitmq.codec import now_utc_iso
from app.resources.vllm.client import VLLMClient
from app.settings import settings


COMMON_CHECKLIST: list[str] = [
    "보증금이 주변 시세 대비 과도하지 않은지 확인하세요.",
    "등기부등본 소유자와 계약서 임대인이 동일한지 확인하세요.",
    "등기부등본에 압류·가압류·강제경매 등 권리침해가 없는지 확인하세요.",
    "근저당권 설정 금액이 과도하지 않은지 확인하세요.",
    "계약 직전 최신 등기부등본으로 변동사항이 없는지 다시 확인하세요.",
    "건축물대장에 위반건축물 표시가 없는지 확인하세요.",
    "건축물대장상 용도가 주택인지 확인하세요.",
    "주소/동·호수가 등기부등본·건축물대장·계약서와 모두 일치하는지 확인하세요.",
    "임대인의 신분을 확인하고 계약서 정보와 일치하는지 확인하세요.",
    "공동 소유 주택이면 소유자 전원과 계약하는지 확인하세요.",
    "대리인 계약이면 위임장 원본과 신분증을 확인하세요.",
    "위임장에 주택 주소·계약 조건·보증금 수령자가 명시됐는지 확인하세요.",
    "공인중개사 거래 시 개업 공인중개사 등록 여부를 확인하세요.",
    "중개대상물 확인·설명서를 교부받았는지 확인하세요.",
    "계약 기간(시작일/종료일)이 정확히 적혀있는지 확인하세요.",
    "보증금·월세 금액과 납부일이 계약서에 명시됐는지 확인하세요.",
    "보증금/월세 입금 계좌 예금주가 임대인(또는 적법 수령자)인지 확인하세요.",
    "관리비 포함 항목과 부담 주체가 계약서에 적혀있는지 확인하세요.",
    "구두로 약속한 내용이 있다면 특약에 반영됐는지 확인하세요.",
    "입주 전 집 상태가 계약 조건과 동일한지 확인하세요.",
]


class ChecklistState(TypedDict, total=False):
    template_id: int | str
    keywords: list[str]
    checklists: list[str]


def _normalize_keywords(keywords: list[str]) -> list[str]:
    cleaned = []
    for k in keywords:
        k2 = (k or "").strip()
        if k2:
            cleaned.append(k2)

    seen = set()
    out = []
    for k in cleaned:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _build_prompt(keywords: list[str], common: list[str]) -> list[dict[str, str]]:
    system = (
        "너는 주택임대차계약을 보조하는 전문 AI다.\n"
        "입력으로 공통 체크리스트와 사용자 라이프스타일 키워드가 함께 주어진다.\n\n"

        "공통 체크리스트는 '참고 자료'일 뿐이며 그대로 복사하지 마라.\n"
        "사용자 키워드에 맞춘 체크리스트와, 키워드와 무관한 공통 체크리스트를 함께 생성한다.\n"
        "최종 출력은 두 종류 항목이 섞인 하나의 리스트여야 한다.\n\n"

        "[출력 규칙]\n"
        '1. 출력은 JSON 배열(list) 형태로 ["문장1", "문장2"]만 출력한다.\n'
        "2. JSON 배열 외의 설명, 문장, 코드블록, ```json 표시는 절대 출력하지 마라.\n"
        "3. 각 항목은 하나의 완결된 문장으로 작성한다.\n"
        "4. 모든 문장은 반드시 '확인하세요.'로 끝나야 한다.\n"
        "5. 의미가 같은 항목은 하나로 통합하고 중복을 제거하라.\n\n"

        "[작성 방식]\n"
        "- 공통 체크리스트 문장을 그대로 복사하지 마라.\n"
        "- 사용자 키워드를 중심으로 내용을 확장하고 보강하라.\n"
        "- 공통과 키워드 항목이 자연스럽게 섞이도록 구성하라.\n"
        "- 형식적인 나열이 아니라 실제 계약 상황에서 유용한 점검 항목처럼 작성하라.\n\n"

        "[공통 체크리스트]\n"
        "- " + "\n- ".join(common) + "\n\n"
    )

    user = (
        "[사용자 키워드]\n"
        "- " + "\n- ".join(keywords) + "\n\n"
        "사용자 키워드에서 주택임대차계약과 관련되지 않은 단어는 제외해라.\n"
        "위 정보를 바탕으로 공통체크리스트와 중복되지 않도록 계약에 대한 체크리스트를 생성하라.\n"
        "하나의 키워드에 대해 최대 3개의 항목을 생성하라. 같은 키워드에 대한 항목은 연달아 나열한다.\n"
        "공통 항목은 약 10개 생성한다.\n"
        "공통 항목은 '모든 임대차 계약자에게 공통'인 내용만 작성한다.\n"
        "공통 항목에는 사용자 키워드의 단어(또는 유사어)를 포함하지 마라.\n"
        "체크리스트 전체 항목은 공통 항목과 키워드에 대한 항목을 합쳐서 약 20개이다.\n"
        "공통 항목은 키워드와 무관한 내용이어야한다. 공통체크리스트의 일부를 이용하여 일반적으로 주택 임대차 계약을 하는 모든 사람에게 해당하는 내용으로 생성한다."
    )

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]

_BAD_TOKENS = {"", "[", "]"}

def _clean_item(s: str) -> str:
    s = (s or "").strip()
    if s in _BAD_TOKENS:
        return ""
    return s


def _parse_model_output(text: str) -> list[str]:
    t = (text or "").strip()

    t = re.sub(r"^```(?:json)?\s*", "", t.strip(), flags=re.IGNORECASE)
    t = re.sub(r"\s*```$", "", t.strip())

    start = t.find("[")
    end = t.rfind("]")
    if start != -1 and end != -1 and end > start:
        candidate = t[start:end+1]
    else:
        candidate = t

    try:
        data = json.loads(candidate)
    except Exception:
        return []

    if not (isinstance(data, list) and all(isinstance(x, str) for x in data)):
        return []

    out: list[str] = []
    seen: set[str] = set()
    for x in data:
        item = _clean_item(x)
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


class ChecklistService:
    def __init__(self, vllm: VLLMClient):
        self.vllm = vllm
        self.graph = self._build_graph()

    def _build_graph(self):
        g = StateGraph(ChecklistState)

        def start(state: ChecklistState) -> ChecklistState:
            state["keywords"] = _normalize_keywords(state.get("keywords", []))
            return state

        def route(state: ChecklistState) -> str:
            return "no_keywords" if len(state.get("keywords", [])) == 0 else "with_keywords"

        def no_keywords(state: ChecklistState) -> ChecklistState:
            state["checklists"] = COMMON_CHECKLIST
            return state

        async def with_keywords(state: ChecklistState) -> ChecklistState:
            msgs = _build_prompt(state["keywords"], COMMON_CHECKLIST)
            logger.info(
                "체크리스트 생성 모델 요청",
                extra={"template_id": state.get("template_id"), "event_time": now_utc_iso()},
            )
            content = await self.vllm.chat(
                msgs,
                temperature=0.2,
                max_tokens=1024,
                model=settings.VLLM_LORA_ADAPTER_CHECKLIST,
            )
            logger.info(
                "체크리스트 생성 모델 응답",
                extra={
                    "template_id": state.get("template_id"),
                    "content_length": len(content),
                    "event_time": now_utc_iso(),
                },
            )
            items = _parse_model_output(content)

            merged = []
            seen = set()
            for x in items + COMMON_CHECKLIST:
                x2 = _clean_item(x)
                if x2 and x2 not in seen:
                    seen.add(x2)
                    merged.append(x2)

            if not merged:
                merged = COMMON_CHECKLIST[:]

            state["checklists"] = merged[:30]
            return state

        g.add_node("start", start)
        g.add_node("no_keywords", no_keywords)
        g.add_node("with_keywords", with_keywords)

        g.set_entry_point("start")
        g.add_conditional_edges(
            "start", route, {"no_keywords": "no_keywords", "with_keywords": "with_keywords"}
        )
        g.add_edge("no_keywords", END)
        g.add_edge("with_keywords", END)

        return g.compile()

    async def generate(self, template_id: int | str, keywords: list[str]) -> list[str]:
        out = await self.graph.ainvoke({"template_id": template_id, "keywords": keywords})
        logger.info(
            "체크리스트 생성 완료",
            extra={
                "template_id": template_id,
                "count": len(out.get("checklists", [])),
                "event_time": now_utc_iso(),
            },
        )
        return out.get("checklists", COMMON_CHECKLIST)
