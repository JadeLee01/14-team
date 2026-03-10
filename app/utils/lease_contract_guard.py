from __future__ import annotations
from dataclasses import dataclass

@dataclass
class LeaseGuardResult:
    ok: bool
    score: int
    matched: list[str]

_STRONG = ["임대차", "임대인", "임차인", "보증금", "월세", "전세", "차임", "임대차기간", "특약", "원상복구"]
_MED = ["부동산", "계약서", "해지", "위약", "관리비", "수선", "하자", "중개", "인도", "명도", "전입", "확정일자"]

def check_is_lease_contract(texts: list[str], threshold: int = 6) -> LeaseGuardResult:
    blob = "\n".join(t for t in texts if t).lower()
    score = 0
    matched: list[str] = []
    for kw in _STRONG:
        if kw.lower() in blob:
            score += 2
            matched.append(kw)
    for kw in _MED:
        if kw.lower() in blob:
            score += 1
            matched.append(kw)
    return LeaseGuardResult(ok=score >= threshold, score=score, matched=matched[:20])