from __future__ import annotations

import argparse
import logging
import sys

from app.resources.vectorstore import ChromaVectorStore
from app.settings import settings

logger = logging.getLogger("ingest_corpus_to_chroma")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="분쟁사례 corpus.jsonl을 Chroma 벡터DB(local/remote)에 적재합니다."
    )
    parser.add_argument(
        "--corpus-jsonl-path",
        default=settings.CORPUS_JSONL_PATH,
        help="분쟁사례 JSONL 파일 경로",
    )
    parser.add_argument(
        "--mode",
        default=settings.VECTOR_DB_MODE,
        choices=["local", "remote"],
        help="벡터DB 모드(local/remote)",
    )
    parser.add_argument(
        "--persist-dir",
        default=settings.VECTOR_DB_DIR,
        help="local 모드에서 사용할 Chroma 영속 저장 디렉터리",
    )
    parser.add_argument(
        "--host",
        default=settings.VECTOR_DB_HOST,
        help="remote 모드에서 사용할 Chroma host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=settings.VECTOR_DB_PORT,
        help="remote 모드에서 사용할 Chroma port",
    )
    parser.add_argument(
        "--contract-collection",
        default=settings.VECTOR_DB_COLLECTION_CONTRACT,
        help="계약서 컬렉션 이름(초기화 시 함께 생성됨)",
    )
    parser.add_argument(
        "--corpus-collection",
        default=settings.VECTOR_DB_COLLECTION_CORPUS,
        help="코퍼스 컬렉션 이름",
    )
    parser.add_argument(
        "--embedding-model",
        default=settings.EMBEDDING_MODEL,
        help="fastembed 임베딩 모델명",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="코퍼스 컬렉션을 삭제 후 전체 재적재",
    )
    return parser


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _build_parser().parse_args()

    store = ChromaVectorStore(
        mode=args.mode,
        persist_dir=args.persist_dir,
        host=args.host,
        port=args.port,
        contract_collection_name=args.contract_collection,
        corpus_collection_name=args.corpus_collection,
        embedding_model=args.embedding_model,
    )

    if args.force:
        logger.info("코퍼스 컬렉션 삭제 후 재생성", extra={"collection": args.corpus_collection})
        try:
            store.client.delete_collection(name=args.corpus_collection)
        except Exception:
            logger.info("삭제할 기존 코퍼스 컬렉션이 없거나 삭제 실패, 재생성 진행")
        store.corpus_collection = store.client.get_or_create_collection(name=args.corpus_collection)

    inserted = store.ensure_corpus_loaded(corpus_jsonl_path=args.corpus_jsonl_path)
    if inserted == 0 and not args.force:
        logger.info("적재할 신규 코퍼스가 없습니다. 기존 컬렉션을 그대로 사용합니다.")
    else:
        logger.info("코퍼스 적재 완료", extra={"inserted": inserted})
    return 0


if __name__ == "__main__":
    sys.exit(main())
