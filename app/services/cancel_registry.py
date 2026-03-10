from __future__ import annotations

import time


class CancelRegistry:
    def __init__(self, ttl_sec: int) -> None:
        self._ttl_sec = ttl_sec
        self._expiry_by_easy_contract_id: dict[int, float] = {}

    def mark_cancelled(self, easy_contract_id: int) -> None:
        if easy_contract_id < 0:
            return
        self._expiry_by_easy_contract_id[easy_contract_id] = time.time() + self._ttl_sec

    def is_cancelled(self, easy_contract_id: int) -> bool:
        if easy_contract_id < 0:
            return False
        expiry = self._expiry_by_easy_contract_id.get(easy_contract_id)
        if expiry is None:
            return False
        if expiry < time.time():
            self._expiry_by_easy_contract_id.pop(easy_contract_id, None)
            return False
        return True

    def cleanup_expired(self) -> None:
        now = time.time()
        expired_keys = [
            easy_contract_id
            for easy_contract_id, expiry in self._expiry_by_easy_contract_id.items()
            if expiry < now
        ]
        for easy_contract_id in expired_keys:
            self._expiry_by_easy_contract_id.pop(easy_contract_id, None)
