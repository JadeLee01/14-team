from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.resources.vectorstore import ChromaVectorStore
from app.settings import settings

logger = logging.getLogger("seed_chat_demo_data")


def _bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="챗봇 로컬 테스트용 계약서 벡터 데이터를 주입합니다."
    )
    ap.add_argument("--easy-contract-id", type=int, default=999001)
    ap.add_argument("--load-corpus", type=str, default="true")
    ap.add_argument("--corpus-jsonl-path", default=settings.CORPUS_JSONL_PATH)
    return ap


def _resolve_corpus_jsonl_path(raw_path: str) -> str:
    given = Path(raw_path)
    if given.exists():
        return str(given)

    fallback = ROOT_DIR / "data" / "corpus.jsonl"
    if fallback.exists():
        logger.info(
            "기본 코퍼스 경로를 찾지 못해 data/corpus.jsonl을 사용합니다",
            extra={"requested_path": str(given), "fallback_path": str(fallback)},
        )
        return str(fallback)
    return str(given)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parser().parse_args()

    store = ChromaVectorStore(
        mode=settings.VECTOR_DB_MODE,
        persist_dir=settings.VECTOR_DB_DIR,
        host=settings.VECTOR_DB_HOST,
        port=settings.VECTOR_DB_PORT,
        contract_collection_name=settings.VECTOR_DB_COLLECTION_CONTRACT,
        corpus_collection_name=settings.VECTOR_DB_COLLECTION_CORPUS,
        embedding_model=settings.EMBEDDING_MODEL,
    )

    if _bool(args.load_corpus):
        corpus_jsonl_path = _resolve_corpus_jsonl_path(args.corpus_jsonl_path)
        inserted = store.ensure_corpus_loaded(corpus_jsonl_path=corpus_jsonl_path)
        logger.info("corpus loaded", extra={"inserted": inserted})

    easy_contract_id = args.easy_contract_id
    ingest_id = "demo-seed"
    correlation_id = "demo-seed-correlation"

    ocr_rows = [
        {
            "doc_type": "contract",
            "file": "lease_contract.pdf",
            "page": 1,
            "text": (
                "임대차기간은 2026-03-01부터 2028-02-28까지다. "
                "보증금은 20,000,000원, 월세는 650,000원이다. "
                "관리비는 월 80,000원으로 임차인이 부담한다."
            ),
        },
        {
            "doc_type": "registry",
            "file": "registry.pdf",
            "page": 1,
            "text": (
                "갑구 소유자: 홍길동. 을구 근저당권 설정금액 120,000,000원. "
                "가압류나 경매개시결정 기재는 확인되지 않는다."
            ),
        },
    ]
    md = """# 요약
- 보증금 2,000만원, 월세 65만원, 관리비 8만원입니다.
- 임대차기간은 2026-03-01 ~ 2028-02-28입니다.

# 위험/주의 포인트
- 근저당권 설정금액이 커서 보증금 반환이 어려울 가능성이 있습니다.
- 계약서 임대인과 등기부 소유자 일치 여부를 재확인하세요.
"""

    ocr_inserted = store.upsert_easy_contract_ocr(
        easy_contract_id=easy_contract_id,
        pages_text=ocr_rows,
        correlation_id=correlation_id,
        ingest_id=ingest_id,
    )
    md_inserted = store.upsert_easy_contract_markdown(
        easy_contract_id=easy_contract_id,
        markdown=md,
        correlation_id=correlation_id,
        ingest_id=ingest_id,
    )

    logger.info(
        "demo data seeded",
        extra={
            "easy_contract_id": easy_contract_id,
            "ocr_inserted": ocr_inserted,
            "markdown_inserted": md_inserted,
            "contract_collection_count": store.contract_collection.count(),
            "corpus_collection_count": store.corpus_collection.count(),
        },
    )
    print(f"seed complete: easy_contract_id={easy_contract_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
