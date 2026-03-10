from __future__ import annotations

import argparse
import json
import sys

import httpx


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="챗봇 SSE 엔드포인트를 로컬에서 테스트합니다.")
    ap.add_argument("--base-url", default="http://127.0.0.1:8000")
    ap.add_argument("--easy-contract-id", type=int, default=999001)
    ap.add_argument("--question", default="보증금 반환 관련해서 가장 위험한 포인트를 알려줘.")
    ap.add_argument("--top-k-contract", type=int, default=8)
    ap.add_argument("--top-k-corpus", type=int, default=5)
    ap.add_argument("--include-corpus", action="store_true", default=True)
    return ap


def main() -> int:
    args = _parser().parse_args()
    url = f"{args.base_url}/api/chat/{args.easy_contract_id}/stream"
    payload = {
        "question": args.question,
        "include_corpus": args.include_corpus,
        "top_k_contract": args.top_k_contract,
        "top_k_corpus": args.top_k_corpus,
    }

    got_done = False
    with httpx.Client(timeout=120.0) as client:
        with client.stream("POST", url, json=payload, headers={"Accept": "text/event-stream"}) as res:
            if res.status_code >= 400:
                print(f"request failed: status={res.status_code}, body={res.text}")
                return 1

            event_name = "message"
            for line in res.iter_lines():
                if not line:
                    continue
                if line.startswith("event:"):
                    event_name = line[len("event:") :].strip()
                    continue
                if not line.startswith("data:"):
                    continue

                raw = line[len("data:") :].strip()
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    data = {"raw": raw}

                print(f"[{event_name}] {data}")
                if event_name == "done":
                    got_done = True
                    break
                if event_name == "error":
                    return 1

    return 0 if got_done else 1


if __name__ == "__main__":
    sys.exit(main())
