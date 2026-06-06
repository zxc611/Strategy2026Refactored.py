"""
analytics_manager.py - AnalyticsManager
Phase 2 (CC-02+CC-04): 从_LifecycleMixin提取的分析/预热/统计Manager

职责：
- Analytics初始化（_init_analytics_services, _build_instrument_groups）
- TType预加载（_init_t_type_service_and_preload）
- Analytics任务注册（_register_analytics_jobs）
- Analytics预热（_start_analytics_warmup_async, _ensure_analytics_ready）
- 性能统计记录（record_tick, record_trade, record_signal, record_error, get_stats）

Provider接口：
- provider.storage: 存储服务
- provider._scheduler_manager: 调度器
- provider._lock: 通用锁
"""

import logging
import time
import threading
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from ali2026v3_trading.shared_utils import CHINA_TZ


class AnalyticsManager:
    """分析/预热/统计Manager"""

    def __init__(self, provider: Any = None):
        self._provider = provider
        self._analytics_warmup_done: bool = False
        self._analytics_warmup_thread: Optional[threading.Thread] = None
        self._stats: Dict[str, Any] = {
            'start_time': None,
            'total_ticks': 0,
            'total_trades': 0,
            'total_signals': 0,
            'total_klines': 0,
            'errors_count': 0,
            'last_error_time': None,
            'last_error_message': None,
            'tick_by_type': {'future': 0, 'option': 0},
            'tick_by_exchange': {},
            'tick_by_instrument': {},
        }

    @property
    def stats(self) -> Dict[str, Any]:
        return self._stats

    @property
    def warmup_done(self) -> bool:
        return self._analytics_warmup_done

    @warmup_done.setter
    def warmup_done(self, value: bool):
        self._analytics_warmup_done = value

    @property
    def warmup_thread(self) -> Optional[threading.Thread]:
        return self._analytics_warmup_thread

    @warmup_thread.setter
    def warmup_thread(self, value: Optional[threading.Thread]):
        self._analytics_warmup_thread = value

    def reset_stats(self):
        self._stats = {
            'start_time': None,
            'total_ticks': 0,
            'total_trades': 0,
            'total_signals': 0,
            'total_klines': 0,
            'errors_count': 0,
            'last_error_time': None,
            'last_error_message': None,
            'tick_by_type': {'future': 0, 'option': 0},
            'tick_by_exchange': {},
            'tick_by_instrument': {},
        }

    def record_tick(self, lock: Optional[threading.RLock] = None) -> None:
        if lock:
            with lock:
                self._stats['total_ticks'] += 1
        else:
            self._stats['total_ticks'] += 1

    def record_trade(self, lock: Optional[threading.RLock] = None) -> None:
        if lock:
            with lock:
                self._stats['total_trades'] += 1
        else:
            self._stats['total_trades'] += 1

    def record_signal(self, lock: Optional[threading.RLock] = None) -> None:
        if lock:
            with lock:
                self._stats['total_signals'] += 1
        else:
            self._stats['total_signals'] += 1

    def record_error(self, error_message: str, lock: Optional[threading.RLock] = None) -> None:
        if lock:
            with lock:
                self._stats['errors_count'] += 1
                self._stats['last_error_time'] = datetime.now(CHINA_TZ)
                self._stats['last_error_message'] = error_message
        else:
            self._stats['errors_count'] += 1
            self._stats['last_error_time'] = datetime.now(CHINA_TZ)
            self._stats['last_error_message'] = error_message

    def get_stats(self, lock: Optional[threading.RLock] = None,
                  state_value: str = '', is_running: bool = False,
                  is_paused: bool = False) -> Dict[str, Any]:
        def _build():
            uptime = 0.0
            if self._stats['start_time']:
                uptime = (datetime.now(CHINA_TZ) - self._stats['start_time']).total_seconds()
            ticks_per_second = self._stats['total_ticks'] / uptime if uptime > 0 else 0
            return {
                'service_name': 'StrategyCoreService',
                **self._stats,
                'uptime_seconds': uptime,
                'ticks_per_second': ticks_per_second,
                'state': state_value,
                'is_running': is_running,
                'is_paused': is_paused,
            }
        if lock:
            with lock:
                return _build()
        return _build()

    def ensure_analytics_ready(self, timeout: float = 30.0) -> bool:
        if self._analytics_warmup_done:
            return True
        if self._analytics_warmup_thread and self._analytics_warmup_thread.is_alive():
            self._analytics_warmup_thread.join(timeout=timeout)
        return self._analytics_warmup_done
