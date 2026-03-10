from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any

# 로컬 배치 실행에서는 RabbitMQ 연결을 비활성화한다.
os.environ["RABBITMQ_ENABLED"] = "false"

from app.bootstrap import create_container

ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = ROOT_DIR / "data"
DEFAULT_RESULTS_DIR = DEFAULT_DATA_DIR / "results"
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".pdf"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="data 폴더의 샘플 파일로 easy contract를 생성하고 결과를 저장합니다."
    )
    parser.add_argument(
        "--data-dir",
        default=str(DEFAULT_DATA_DIR),
        help="입력 파일 폴더 경로 (기본값: ./data)",
    )
    parser.add_argument(
        "--results-dir",
        default=str(DEFAULT_RESULTS_DIR),
        help="결과 저장 폴더 경로 (기본값: ./data/results)",
    )
    parser.add_argument(
        "--easy-contract-id",
        type=int,
        default=-1,
        help="내부 easy_contract_id (기본값: -1)",
    )
    parser.add_argument(
        "--doc-type",
        choices=["contract", "registry"],
        default=None,
        help="모든 입력 파일에 강제로 적용할 doc_type",
    )
    return parser.parse_args()


def _guess_doc_type(path: Path) -> str:
    name = path.name.lower()
    if "registry" in name or "등기" in name:
        return "registry"
    return "contract"


def _collect_input_files(data_dir: Path) -> list[Path]:
    files = [
        p for p in sorted(data_dir.iterdir())
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
    ]
    return files


def _natural_sort_key(value: str) -> list[Any]:
    parts = re.split(r"(\d+)", value)
    return [int(p) if p.isdigit() else p.lower() for p in parts]


def _extract_group_name(path: Path) -> str:
    stem = path.stem.strip()
    match = re.match(r"^(.*?)[-_](\d+)$", stem)
    if match:
        base = match.group(1).strip()
        if base:
            return base
    return stem


def _group_input_files(files: list[Path]) -> list[tuple[str, list[Path]]]:
    grouped: dict[str, list[Path]] = {}
    for path in sorted(files, key=lambda p: _natural_sort_key(p.name)):
        group_name = _extract_group_name(path)
        grouped.setdefault(group_name, []).append(path)

    ordered: list[tuple[str, list[Path]]] = []
    for group_name in sorted(grouped.keys(), key=_natural_sort_key):
        group_files = sorted(grouped[group_name], key=lambda p: _natural_sort_key(p.name))
        ordered.append((group_name, group_files))
    return ordered


def _safe_group_slug(group_name: str, index: int) -> str:
    slug = re.sub(r"[^0-9A-Za-z._-]+", "_", group_name.strip()).strip("._-").lower()
    if not slug:
        slug = "group"
    return f"{index:02d}_{slug}"


def _build_docs(files: list[Path], forced_doc_type: str | None) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for path in files:
        doc_type = forced_doc_type or _guess_doc_type(path)
        docs.append(
            {
                "filename": path.name,
                "bytes": path.read_bytes(),
                "doc_type": doc_type,
            }
        )
    return docs


async def _run_batch(
    *,
    data_dir: Path,
    results_dir: Path,
    easy_contract_id: int,
    forced_doc_type: str | None,
) -> None:
    if not data_dir.exists():
        raise FileNotFoundError(f"입력 폴더가 없습니다: {data_dir}")

    files = _collect_input_files(data_dir)
    if not files:
        raise FileNotFoundError(f"처리할 파일이 없습니다: {data_dir} ({', '.join(sorted(SUPPORTED_EXTENSIONS))})")

    grouped_files = _group_input_files(files)
    if not grouped_files:
        raise RuntimeError("입력 파일 그룹을 생성하지 못했습니다.")

    results_dir.mkdir(parents=True, exist_ok=True)
    container = await create_container()
    await container.startup()
    manifest: list[dict[str, Any]] = []
    try:
        for idx, (group_name, group_paths) in enumerate(grouped_files, start=1):
            docs = _build_docs(group_paths, forced_doc_type)
            group_slug = _safe_group_slug(group_name, idx)

            result = await container.easy_contract_service.generate_with_details(
                easy_contract_id=easy_contract_id,
                docs=docs,
                correlation_id=f"local-sample-{group_slug}",
            )

            ocr_output_path = results_dir / f"ocr_results_{group_slug}.json"
            markdown_output_path = results_dir / f"easy_contract_{group_slug}.md"

            ocr_payload = {
                "easy_contract_id": easy_contract_id,
                "group_name": group_name,
                "input_files": [path.name for path in group_paths],
                "pages_text": result.get("pages_text", []),
            }
            ocr_output_path.write_text(
                json.dumps(ocr_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            markdown_output_path.write_text(result.get("markdown", ""), encoding="utf-8")

            manifest.append(
                {
                    "group_name": group_name,
                    "input_files": [path.name for path in group_paths],
                    "ocr_result_path": str(ocr_output_path),
                    "easy_contract_path": str(markdown_output_path),
                }
            )

            print(f"[{idx}/{len(grouped_files)}] 그룹: {group_name} (파일 {len(group_paths)}개)")
            print(f"  OCR 결과 저장: {ocr_output_path}")
            print(f"  쉬운 계약서 저장: {markdown_output_path}")
    finally:
        await container.aclose()

    manifest_path = results_dir / "batch_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "total_groups": len(grouped_files),
                "total_files": len(files),
                "groups": manifest,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"그룹 처리 완료: {len(grouped_files)}개")
    print(f"배치 매니페스트 저장: {manifest_path}")


def main() -> None:
    args = _parse_args()
    asyncio.run(
        _run_batch(
            data_dir=Path(args.data_dir).resolve(),
            results_dir=Path(args.results_dir).resolve(),
            easy_contract_id=args.easy_contract_id,
            forced_doc_type=args.doc_type,
        )
    )


if __name__ == "__main__":
    main()
