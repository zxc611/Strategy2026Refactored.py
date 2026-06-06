"""
signal_filter_chain.py - SignalFilterChain
Phase 2 (CC-P1-02): 从SignalService提取的信号过滤职责域

职责：
- HFT信号时序滤波 (enable_hft_filter, filter_with_hft)
- PLR过滤 (enable_plr_filter, disable_plr_filter)
- 信号状态转移 (transition_signal_state)
- 过期信号清理 (expire_stale_signals)
"""
from __future__ import annotations

import logging
import time
import threading
from typing import Any, Dict, List, Optional


class SignalFilterChain:
    """信号过滤链 — 从SignalService提取的独立可测试组件"""

    def __init__(self, signal_service=None):
        self._signal_service = signal_service
        self._hft_signal_filter = None
        self._hft_filter_enabled = False
        self._min_estimated_plr = 2.0
        self._plr_filter_enabled = False

    def enable_hft_filter(self, threshold: float = 0.6, use_kalman: bool = True) -> None:
        if self._signal_service is not None:
            from ali2026v3_trading.signal.signal_timing_filter import SignalTimingFilter
            self._hft_signal_filter = SignalTimingFilter(threshold=threshold, use_kalman=use_kalman)
        self._hft_filter_enabled = True
        logging.info("[SignalFilterChain] HFT信号时序滤波器已启用 threshold=%.2f", threshold)

    def filter_with_hft(self, instrument_id: str, resonance_strength: float) -> Optional[Dict[str, Any]]:
        if not self._hft_filter_enabled or not self._hft_signal_filter:
            return None
        return self._hft_signal_filter.filter_signal(instrument_id, resonance_strength)

    def enable_plr_filter(self, min_estimated_plr: float = 2.0) -> None:
        self._min_estimated_plr = min_estimated_plr
        self._plr_filter_enabled = True
        logging.info("[SignalFilterChain] PLR过滤已启用 min_estimated_plr=%.2f", min_estimated_plr)

    def disable_plr_filter(self) -> None:
        self._plr_filter_enabled = False
        logging.info("[SignalFilterChain] PLR过滤已禁用")

    @property
    def plr_filter_enabled(self) -> bool:
        return self._plr_filter_enabled

    @property
    def min_estimated_plr(self) -> float:
        return self._min_estimated_plr

    def transition_signal_state(self, signal_id: str, new_state: str,
                                 signal_states: Dict[str, str],
                                 valid_transitions: Dict[str, List[str]]) -> bool:
        current = signal_states.get(signal_id, 'GENERATED')
        valid_targets = valid_transitions.get(current, [])
        if new_state not in valid_targets:
            logging.warning("[R23-SM-02-FIX] 非法信号状态转移: %s -> %s, signal_id=%s", current, new_state, signal_id)
            return False
        signal_states[signal_id] = new_state
        return True

    def expire_stale_signals(self, signal_history: List[Dict],
                              signal_states: Dict[str, str],
                              signal_max_age_sec: float,
                              lock: threading.Lock = None) -> int:
        if signal_max_age_sec <= 0:
            return 0
        _now = time.time()
        if hasattr(self, '_last_expire_check_time') and (_now - self._last_expire_check_time) < 30.0:
            return 0
        self._last_expire_check_time = _now
        _expired_count = 0
        expired_states = ('COMPLETED', 'EXPIRED', 'REJECTED')
        if lock:
            with lock:
                for sig in signal_history:
                    sig_id = sig.get('signal_id', '')
                    sig_state = signal_states.get(sig_id, 'GENERATED')
                    if sig_state in expired_states:
                        continue
                    sig_time = sig.get('timestamp', 0)
                    if isinstance(sig_time, (int, float)) and (_now - sig_time) > signal_max_age_sec:
                        signal_states[sig_id] = 'EXPIRED'
                        _expired_count += 1
        else:
            for sig in signal_history:
                sig_id = sig.get('signal_id', '')
                sig_state = signal_states.get(sig_id, 'GENERATED')
                if sig_state in expired_states:
                    continue
                sig_time = sig.get('timestamp', 0)
                if isinstance(sig_time, (int, float)) and (_now - sig_time) > signal_max_age_sec:
                    signal_states[sig_id] = 'EXPIRED'
                    _expired_count += 1
        if _expired_count > 0:
            logging.info("[R23-FR-05-FIX] 过期信号清理: %d个信号已标记EXPIRED, max_age=%ds", _expired_count, int(signal_max_age_sec))
        return _expired_count