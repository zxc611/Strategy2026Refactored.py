# MODULE_ID: M1-236
"""
cooldown_manager.py - CooldownManager
Phase 2 (CC-P1-02): 从SignalService提取的冷却管理职责域

职责：
- 冷却键生成 (_make_cooldown_key)
- 冷却状态检查 (_is_in_cooldown)
- 设置/清除冷却 (set_cooldown, clear_cooldown)
"""
from __future__ import annotations

import logging
import time
import threading
from typing import Dict


class CooldownManager:
    """冷却管理 — 从SignalService提取的独立可测试组件"""

    def __init__(self):
        self._cooldown_times: Dict[str, float] = {}
        self._cooldown_durations: Dict[str, float] = {}
        self._lock = threading.Lock()

    def make_cooldown_key(self, instrument_id: str, signal_type: str = '') -> str:
        if signal_type:
            return f"{instrument_id}_{signal_type}"
        return f"{instrument_id}_default"

    def is_in_cooldown(self, cooldown_key: str, cooldown_seconds: float) -> bool:
        last_signal_time = self._cooldown_times.get(cooldown_key, 0)
        effective_cooldown = self._cooldown_durations.get(cooldown_key, cooldown_seconds)
        elapsed = time.time() - last_signal_time
        return elapsed < effective_cooldown

    def set_cooldown(self, instrument_id: str, cooldown_seconds: float,
                      signal_type: str = '') -> None:
        cooldown_key = self.make_cooldown_key(instrument_id, signal_type)
        with self._lock:
            self._cooldown_times[cooldown_key] = time.time()
            self._cooldown_durations[cooldown_key] = cooldown_seconds
            logging.info("[CooldownManager] Cooldown set for %s: %ss", cooldown_key, cooldown_seconds)

    def clear_cooldown(self, instrument_id: str) -> None:
        with self._lock:
            if instrument_id in self._cooldown_times:
                del self._cooldown_times[instrument_id]
                self._cooldown_durations.pop(instrument_id, None)
                logging.debug("[CooldownManager] Cooldown cleared for %s", instrument_id)

    @property
    def cooldown_times(self) -> Dict[str, float]:
        return self._cooldown_times

    @property
    def cooldown_durations(self) -> Dict[str, float]:
        return self._cooldown_durations

    def cleanup_expired(self, default_cooldown_seconds: float,
                        cleanup_interval: float,
                        last_cleanup: float) -> int:
        now = time.time()
        if now - last_cleanup <= cleanup_interval:
            return 0
        expired = [k for k, v in self._cooldown_times.items()
                   if now - v > default_cooldown_seconds]
        with self._lock:
            for k in expired:
                del self._cooldown_times[k]
                self._cooldown_durations.pop(k, None)
        if expired:
            logging.debug("[SignalService] 清理%d个过期冷却条目", len(expired))
        return len(expired)

    def count_active_cooldowns(self, default_cooldown_seconds: float) -> int:
        now = time.time()
        return len([k for k, v in self._cooldown_times.items()
                    if now - v < default_cooldown_seconds])