"""Unified registry updater — read pre-computed results from DuckDB into memory dicts."""
from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple

from ali2026v3_trading.infra.logging_utils import get_logger

logger = get_logger(__name__)


class TTLCache:
    def __init__(self, max_size: int = 10000, default_ttl: float = 60.0):
        self._cache: OrderedDict[str, Tuple[float, Any]] = OrderedDict()
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._cache:
                ts, val = self._cache[key]
                if time.time() - ts < self._default_ttl:
                    self._cache.move_to_end(key)
                    return val
                else:
                    del self._cache[key]
            return None

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        with self._lock:
            self._cache[key] = (time.time(), value)
            self._cache.move_to_end(key)
            while len(self._cache) > self._max_size:
                self._cache.popitem(last=False)

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._cache.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()


class RegistryUpdater:
    _instance = None
    _lock = threading.Lock()

    def __init__(self, con_factory=None):
        self._con_factory = con_factory
        self._bar_cache = TTLCache(max_size=50000, default_ttl=60.0)
        self._cr_cache = TTLCache(max_size=10000, default_ttl=30.0)
        self._l0_cache = TTLCache(max_size=10000, default_ttl=30.0)
        self._pos_cache = TTLCache(max_size=10000, default_ttl=60.0)
        self._column_cache: Dict[str, List[str]] = {}
        self._column_cache_lock = threading.Lock()

    @classmethod
    def get_instance(cls, **kwargs) -> RegistryUpdater:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(**kwargs)
        return cls._instance

    def get_bar(
        self, symbol: str, minute: str
    ) -> Optional[Dict[str, Any]]:
        cache_key = f"{symbol}:{minute}"
        cached = self._bar_cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            con = self._con_factory()
            row = con.execute(
                "SELECT * FROM minute_data WHERE symbol = ? AND minute = ? LIMIT 1",
                [symbol, minute],
            ).fetchone()
            if row:
                with self._column_cache_lock:
                    cols = self._column_cache.get("minute_data")
                    if cols is None:
                        cols = [desc[0] for desc in con.execute("DESCRIBE minute_data").fetchall()]
                        self._column_cache["minute_data"] = cols
                result = dict(zip(cols, row))
                self._bar_cache.set(cache_key, result)
                return result
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("RegistryUpdater.get_bar failed: %s", e)
        return None

    def get_cycle_resonance(
        self, symbol: str, minute: str
    ) -> Optional[Dict[str, Any]]:
        cache_key = f"cr:{symbol}:{minute}"
        cached = self._cr_cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            con = self._con_factory()
            row = con.execute(
                "SELECT directional_bias, resonance_strength, cr_phase, state_entropy, "
                "hmm_state, hmm_posterior_low, hmm_posterior_normal, hmm_posterior_high "
                "FROM minute_data WHERE symbol = ? AND minute = ? LIMIT 1",
                [symbol, minute],
            ).fetchone()
            if row:
                result = {
                    "directional_bias": row[0],
                    "resonance_strength": row[1],
                    "cr_phase": row[2],
                    "state_entropy": row[3],
                    "hmm_state": row[4],
                    "hmm_posterior": (row[5], row[6], row[7]),
                }
                self._cr_cache.set(cache_key, result)
                return result
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("RegistryUpdater.get_cycle_resonance failed: %s", e)
        return None

    def get_l0_state(
        self, symbol: str, minute: str
    ) -> Optional[Dict[str, Any]]:
        cache_key = f"l0:{symbol}:{minute}"
        cached = self._l0_cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            con = self._con_factory()
            row = con.execute(
                "SELECT l0_raw_state, l0_smoothed_state, l0_state_entropy "
                "FROM minute_data WHERE symbol = ? AND minute = ? LIMIT 1",
                [symbol, minute],
            ).fetchone()
            if row:
                result = {
                    "raw_state": row[0],
                    "smoothed_state": row[1],
                    "state_entropy": row[2],
                }
                self._l0_cache.set(cache_key, result)
                return result
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("RegistryUpdater.get_l0_state failed: %s", e)
        return None