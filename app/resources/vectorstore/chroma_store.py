from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

import chromadb
from fastembed import TextEmbedding

from app.resources.rabbitmq.codec import now_utc_iso

logger = logging.getLogger(__name__)


def _chunk_text(text: str, *, chunk_size: int = 900, overlap: int = 150) -> list[str]:
    raw = (text or "").strip()
    if not raw:
        return []
    if chunk_size <= 0:
        return [raw]
    if overlap < 0:
        overlap = 0
    if overlap >= chunk_size:
        overlap = max(0, chunk_size // 4)

    out: list[str] = []
    start = 0
    while start < len(raw):
        end = min(len(raw), start + chunk_size)
        piece = raw[start:end].strip()
        if piece:
            out.append(piece)
        if end >= len(raw):
            break
        start = end - overlap
    return out


def _short_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


class ChromaVectorStore:
    def __init__(
        self,
        *,
        mode: str,
        persist_dir: str,
        host: str,
        port: int,
        contract_collection_name: str,
        corpus_collection_name: str,
        embedding_model: str,
    ) -> None:
        normalized_mode = (mode or "local").strip().lower()
        self.mode = normalized_mode
        self.persist_dir = Path(persist_dir)

        if normalized_mode == "remote":
            self.client = chromadb.HttpClient(host=host, port=port)
            logger.info("원격 Chroma 사용", extra={"host": host, "port": port})
        elif normalized_mode == "local":
            self.persist_dir.mkdir(parents=True, exist_ok=True)
            self.client = chromadb.PersistentClient(path=str(self.persist_dir))
            logger.info("로컬 Chroma 사용", extra={"persist_dir": str(self.persist_dir)})
        else:
            raise ValueError(f"VECTOR_DB_MODE는 'local' 또는 'remote'여야 합니다. 입력값: {mode}")

        self.contract_collection = self.client.get_or_create_collection(name=contract_collection_name)
        self.corpus_collection = self.client.get_or_create_collection(name=corpus_collection_name)
        self.embedder = TextEmbedding(model_name=embedding_model)

    def _embed_texts(self, texts: list[str]) -> list[list[float]]:
        vectors = list(self.embedder.embed(texts))
        return [vec.tolist() for vec in vectors]

    def _upsert(
        self,
        *,
        collection: Any,
        ids: list[str],
        documents: list[str],
        metadatas: list[dict[str, Any]],
    ) -> int:
        if not documents:
            return 0
        embeddings = self._embed_texts(documents)
        collection.upsert(
            ids=ids,
            documents=documents,
            metadatas=metadatas,
            embeddings=embeddings,
        )
        return len(documents)

    def upsert_easy_contract_ocr(
        self,
        *,
        easy_contract_id: int,
        pages_text: list[dict[str, Any]],
        correlation_id: str | None,
        ingest_id: str,
    ) -> int:
        ids: list[str] = []
        docs: list[str] = []
        metas: list[dict[str, Any]] = []
        now = now_utc_iso()

        for page in pages_text:
            text = str(page.get("text") or "").strip()
            if not text:
                continue
            file_name = str(page.get("file") or "")
            doc_type = str(page.get("doc_type") or "unknown")
            page_no = int(page.get("page") or 0)
            chunks = _chunk_text(text)
            for idx, chunk in enumerate(chunks, start=1):
                ids.append(
                    f"ec:{easy_contract_id}:ocr:{ingest_id}:{file_name}:{page_no}:{idx}:{_short_hash(chunk)}"
                )
                docs.append(chunk)
                metas.append(
                    {
                        "source": "ocr",
                        "easy_contract_id": easy_contract_id,
                        "ingest_id": ingest_id,
                        "doc_type": doc_type,
                        "file": file_name,
                        "page": page_no,
                        "chunk_index": idx,
                        "correlation_id": correlation_id or "",
                        "created_at": now,
                    }
                )

        return self._upsert(collection=self.contract_collection, ids=ids, documents=docs, metadatas=metas)

    def upsert_easy_contract_markdown(
        self,
        *,
        easy_contract_id: int,
        markdown: str,
        correlation_id: str | None,
        ingest_id: str,
    ) -> int:
        chunks = _chunk_text(markdown, chunk_size=1000, overlap=180)
        ids: list[str] = []
        docs: list[str] = []
        metas: list[dict[str, Any]] = []
        now = now_utc_iso()

        for idx, chunk in enumerate(chunks, start=1):
            ids.append(f"ec:{easy_contract_id}:easy:{ingest_id}:{idx}:{_short_hash(chunk)}")
            docs.append(chunk)
            metas.append(
                {
                    "source": "easy_contract",
                    "easy_contract_id": easy_contract_id,
                    "ingest_id": ingest_id,
                    "chunk_index": idx,
                    "correlation_id": correlation_id or "",
                    "created_at": now,
                }
            )

        return self._upsert(collection=self.contract_collection, ids=ids, documents=docs, metadatas=metas)

    def ensure_corpus_loaded(self, *, corpus_jsonl_path: str) -> int:
        # Keep startup cost low once corpus is built.
        if self.corpus_collection.count() > 0:
            return 0

        path = Path(corpus_jsonl_path)
        if not path.exists():
            logger.warning("분쟁사례 코퍼스 파일이 없어 적재를 건너뜁니다", extra={"path": str(path)})
            return 0

        ids: list[str] = []
        docs: list[str] = []
        metas: list[dict[str, Any]] = []
        now = now_utc_iso()

        with path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                row = line.strip()
                if not row:
                    continue
                try:
                    item = json.loads(row)
                except json.JSONDecodeError:
                    logger.warning("코퍼스 JSONL 파싱 실패", extra={"line_no": line_no})
                    continue
                if not isinstance(item, dict):
                    continue

                text = str(item.get("content") or "").strip()
                if not text:
                    continue

                case_id = str(item.get("case_id") or "unknown")
                section = str(item.get("section") or "unknown")
                title = str(item.get("title") or "")
                keywords = item.get("keyword")
                keyword_text = ",".join(keywords) if isinstance(keywords, list) else ""
                chunks = _chunk_text(text)

                for idx, chunk in enumerate(chunks, start=1):
                    ids.append(f"corpus:{case_id}:{section}:{idx}:{_short_hash(chunk)}")
                    docs.append(chunk)
                    metas.append(
                        {
                            "source": "corpus",
                            "case_id": case_id,
                            "section": section,
                            "title": title,
                            "keywords": keyword_text,
                            "line_no": line_no,
                            "chunk_index": idx,
                            "created_at": now,
                        }
                    )

        inserted = self._upsert(collection=self.corpus_collection, ids=ids, documents=docs, metadatas=metas)
        logger.info("분쟁사례 코퍼스 벡터 적재 완료", extra={"inserted": inserted, "path": str(path)})
        return inserted

    def _query(
        self,
        *,
        collection: Any,
        query_text: str,
        limit: int,
        where: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        text = (query_text or "").strip()
        if not text or limit <= 0:
            return []

        qvec = self._embed_texts([text])[0]
        kwargs: dict[str, Any] = {
            "query_embeddings": [qvec],
            "n_results": limit,
            "include": ["documents", "metadatas", "distances"],
        }
        if where is not None:
            kwargs["where"] = where

        result = collection.query(**kwargs)
        docs = (result.get("documents") or [[]])[0]
        metas = (result.get("metadatas") or [[]])[0]
        distances = (result.get("distances") or [[]])[0]

        out: list[dict[str, Any]] = []
        for doc, meta, dist in zip(docs, metas, distances):
            if not doc:
                continue
            out.append(
                {
                    "document": str(doc),
                    "metadata": meta if isinstance(meta, dict) else {},
                    "distance": float(dist) if dist is not None else None,
                }
            )
        return out

    def query_contract_chunks(
        self,
        *,
        easy_contract_id: int,
        question: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        return self._query(
            collection=self.contract_collection,
            query_text=question,
            limit=limit,
            where={"easy_contract_id": easy_contract_id},
        )

    def query_corpus_chunks(
        self,
        *,
        question: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        return self._query(
            collection=self.corpus_collection,
            query_text=question,
            limit=limit,
        )
