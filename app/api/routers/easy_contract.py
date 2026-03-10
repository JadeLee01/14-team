from __future__ import annotations

from pathlib import Path as FsPath
from urllib.parse import urlparse
from uuid import uuid4

import httpx
from fastapi import APIRouter, Depends, File, Form, HTTPException, Path, UploadFile
from fastapi.responses import PlainTextResponse

from app.api.schemas.easy_contract import EasyContractRequest
from app.api.deps import get_container

router = APIRouter(prefix="/api/easycontract", tags=["easycontract"])

ALLOWED_EXT = {".pdf", ".png", ".jpg", ".jpeg"}
ROOT_DIR = FsPath(__file__).resolve().parents[3]
S3_DIR = ROOT_DIR / "resources" / "s3"


def _ext_ok(filename: str) -> bool:
    return FsPath(filename or "").suffix.lower() in ALLOWED_EXT


def _filename_from_url(url: str) -> str:
    return FsPath(urlparse(url).path).name


def _safe_filename(name: str) -> str:
    return FsPath(name).name


def _resolve_filename_from_meta(meta, idx: int) -> str:
    url_name = _filename_from_url(meta.url or "")
    base = _safe_filename(meta.filename or url_name or f"file_{idx}")
    if not base:
        base = f"file_{idx}"
    ext = FsPath(base).suffix
    if not ext:
        url_ext = FsPath(url_name).suffix
        if url_ext:
            base = f"{base}{url_ext}"
    return base


def _validate_file_metas(files) -> list[str]:
    if not files or len(files) < 1 or len(files) > 5:
        raise HTTPException(
            status_code=400,
            detail={"error": {"code": "INVALID_INPUT", "message": "files는 1개 이상 5개 이하로 업로드해야 합니다."}},
        )

    resolved_names: list[str] = []
    for idx, meta in enumerate(files, start=1):
        if not meta.url:
            raise HTTPException(
                status_code=400,
                detail={"error": {"code": "INVALID_INPUT", "message": "url은 필수입니다."}},
            )
        if not meta.doc_type or not meta.doc_type.strip():
            raise HTTPException(
                status_code=400,
                detail={"error": {"code": "INVALID_INPUT", "message": "doc_type은 필수입니다."}},
            )

        name = _resolve_filename_from_meta(meta, idx)
        if not _ext_ok(name):
            raise HTTPException(
                status_code=400,
                detail={"error": {"code": "UNSUPPORTED_FILE_TYPE", "message": "pdf/png/jpg 파일만 업로드할 수 있습니다."}},
            )
        resolved_names.append(name)

    return resolved_names


def _validate_inputs(files: list[UploadFile], doc_types: list[str]) -> None:
    if not files or len(files) < 1 or len(files) > 5:
        raise HTTPException(
            status_code=400,
            detail={"error": {"code": "INVALID_INPUT", "message": "files는 1개 이상 5개 이하로 업로드해야 합니다."}},
        )

    if not doc_types or len(doc_types) != len(files):
        raise HTTPException(
            status_code=400,
            detail={"error": {"code": "DOC_TYPES_MISMATCH", "message": "doc_types 길이는 files 길이와 같아야 합니다."}},
        )

    for f in files:
        if not _ext_ok(f.filename or ""):
            raise HTTPException(
                status_code=400,
                detail={"error": {"code": "UNSUPPORTED_FILE_TYPE", "message": "pdf/png/jpg 파일만 업로드할 수 있습니다."}},
            )


async def _read_docs(files: list[UploadFile], doc_types: list[str]) -> list[dict]:
    docs: list[dict] = []
    for f, dt in zip(files, doc_types):
        b = await f.read()
        if not b:
            raise HTTPException(
                status_code=400,
                detail={"error": {"code": "INVALID_INPUT", "message": "비어있는 파일은 업로드할 수 없습니다."}},
            )
        docs.append({"filename": f.filename, "bytes": b, "doc_type": dt})
    return docs


async def _download_file(http: httpx.AsyncClient, url: str, dst_path: FsPath) -> bytes:
    try:
        res = await http.get(url)
        res.raise_for_status()
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=400,
            detail={"error": {"code": "DOWNLOAD_FAILED", "message": "파일 다운로드에 실패했습니다."}},
        ) from e

    try:
        dst_path.write_bytes(res.content)
    except OSError as e:
        raise HTTPException(
            status_code=500,
            detail={"error": {"code": "FAILED_TO_SAVE", "message": "파일 저장 중 오류가 발생하였습니다."}},
        ) from e

    return res.content


@router.post(
    "/sync",
    response_class=PlainTextResponse,
    responses={
        200: {"content": {"text/markdown": {}}},
        400: {"description": "입력값 오류"},
    },
)
async def create_easy_contract_sync(
    files: list[UploadFile] = File(..., description="pdf/png/jpg 최대 5개"),
    doc_types: list[str] = Form(..., description="files와 같은 순서의 타입"),
    container=Depends(get_container),
):
    _validate_inputs(files, doc_types)
    docs = await _read_docs(files, doc_types)

    try:
        md = await container.easy_contract_service.generate(easy_contract_id=-1, docs=docs)
        if not md.strip():
            raise HTTPException(
                status_code=500,
                detail={"error": {"code": "failed", "message": "쉬운 계약서 생성 중 오류가 발생하였습니다."}},
            )
        return PlainTextResponse(content=md, media_type="text/markdown; charset=utf-8")

    except RuntimeError as e:
        if str(e) == "UNPROCESSABLE_DOCUMENT":
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "code": "UNPROCESSABLE_DOCUMENT",
                        "message": "문서를 처리할 수 없습니다. 파일이 손상되었거나 암호화되어 있을 수 있습니다.",
                    }
                },
            )
        raise HTTPException(
            status_code=500,
            detail={"error": {"code": "failed", "message": "쉬운 계약서 생성 중 오류가 발생하였습니다."}},
        )

    except HTTPException:
        raise

    except Exception:
        raise HTTPException(
            status_code=500,
            detail={"error": {"code": "failed", "message": "쉬운 계약서 생성 중 오류가 발생하였습니다."}},
        )


@router.post(
    "/{id}",
    response_class=PlainTextResponse,
    responses={
        200: {"content": {"text/markdown": {}}},
        400: {"description": "입력값 오류"},
    },
)
async def create_easy_contract(
    body: EasyContractRequest,
    id: int = Path(..., description="쉬운계약서 식별자(int)"),
    container=Depends(get_container),
):
    files = body.files or []
    resolved_names = _validate_file_metas(files)

    S3_DIR.mkdir(parents=True, exist_ok=True)
    saved_paths: list[FsPath] = []
    docs: list[dict] = []

    try:
        for meta, filename in zip(files, resolved_names):
            ext = FsPath(filename).suffix
            stored_name = f"{id}_{uuid4().hex}{ext}"
            stored_path = S3_DIR / stored_name

            b = await _download_file(container.http, meta.url, stored_path)
            saved_paths.append(stored_path)
            docs.append({"filename": filename, "bytes": b, "doc_type": meta.doc_type})

        md = await container.easy_contract_service.generate(easy_contract_id=id, docs=docs)
        if not md.strip():
            raise HTTPException(
                status_code=500,
                detail={"error": {"code": "failed", "message": "쉬운 계약서 생성 중 오류가 발생하였습니다."}},
            )
        return PlainTextResponse(content=md, media_type="text/markdown; charset=utf-8")

    except RuntimeError as e:
        if str(e) == "UNPROCESSABLE_DOCUMENT":
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "code": "UNPROCESSABLE_DOCUMENT",
                        "message": "문서를 처리할 수 없습니다. 파일이 손상되었거나 암호화되어 있을 수 있습니다.",
                    }
                },
            )
        raise HTTPException(
            status_code=500,
            detail={"error": {"code": "failed", "message": "쉬운 계약서 생성 중 오류가 발생하였습니다."}},
        )

    except HTTPException:
        raise

    except Exception:
        raise HTTPException(
            status_code=500,
            detail={"error": {"code": "failed", "message": "쉬운 계약서 생성 중 오류가 발생하였습니다."}},
        )

    finally:
        for path in saved_paths:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except Exception:
                pass
