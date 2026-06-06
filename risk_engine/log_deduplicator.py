from __future__ import annotations

import time
import threading
import logging
from typing import Dict, Any, Optional

class LogDeduplicator:
    _DEDUP_WINDOW_SEC = 5.0

    def __init__(self):
        self._recent: Dict[str, float] = {}
        self._lock = threading.Lock()

    def should_log(self, msg: str) -> bool:
        _now = time.time()
        with self._lock:
            _last = self._recent.get(msg, 0.0)
            if _now - _last < self._DEDUP_WINDOW_SEC:
                return False
            self._recent[msg] = _now
            if len(self._recent) > 500:
                _cutoff = _now - self._DEDUP_WINDOW_SEC
                self._recent = {k: v for k, v in self._recent.items() if v > _cutoff}
            return True
