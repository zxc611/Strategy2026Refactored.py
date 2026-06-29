"""
disk_monitor.py - 磁盘空间监控 (DiskSpaceMonitor)
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import time
from typing import Any, Dict, List, Optional

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads  # R4-4: 统一json_loads
from ali2026v3_trading.infra.shared_utils import atomic_replace_file
from ali2026v3_trading.infra._helpers import _CHINA_TZ
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5


logger = get_logger(__name__)  # R9-5


# ============================================================================
# R15-P0-RES-08修复: 磁盘空间监控
# ============================================================================
class DiskSpaceMonitor:
    """R15-P0-RES-08修复: 定期监控磁盘空间，空间不足时告警"""

    DEFAULT_CHECK_INTERVAL_SEC = 60.0
    DEFAULT_MIN_FREE_GB = 1.0

    def __init__(self, watch_paths: Optional[List[str]] = None,
                 min_free_gb: float = DEFAULT_MIN_FREE_GB,
                 check_interval_sec: float = DEFAULT_CHECK_INTERVAL_SEC,
                 persistence_file: Optional[str] = None):
        self._watch_paths = watch_paths or ['.']
        self._min_free_gb = min_free_gb
        self._check_interval_sec = check_interval_sec
        self._last_check_time = 0.0
        self._last_status: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._alert_callback: Optional[callable] = None
        # R16-P1-RES-17修复: 监控指标持久化文件路径
        self._persistence_file = persistence_file
        self._history: List[Dict[str, Any]] = []
        self._max_history_size = 1000
        self._load_history()

    def set_alert_callback(self, callback: callable) -> None:
        self._alert_callback = callback

    def check(self, force: bool = False) -> Dict[str, Any]:
        """检查磁盘空间，返回各路径状态"""
        now = time.time()
        if not force:
            with self._lock:
                if now - self._last_check_time < self._check_interval_sec:
                    return self._last_status

        status = {}
        for path in self._watch_paths:
            try:
                usage = shutil.disk_usage(path)
                free_gb = usage.free / (1024 ** 3)
                total_gb = usage.total / (1024 ** 3)
                used_pct = (usage.used / usage.total * 100) if usage.total > 0 else 0
                is_low = free_gb < self._min_free_gb
                status[path] = {
                    'free_gb': round(free_gb, 2),
                    'total_gb': round(total_gb, 2),
                    'used_pct': round(used_pct, 1),
                    'is_low': is_low,
                }
                if is_low:
                    logger.warning(
                        "[R15-P0-RES-08] 磁盘空间不足! path=%s free=%.2fGB < min=%.2fGB",
                        path, free_gb, self._min_free_gb
                    )
                    if self._alert_callback:
                        try:
                            self._alert_callback(path, free_gb, self._min_free_gb)
                        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                            logging.debug("[R3-L2] disk_monitor alert callback suppressed: %s", _r3_err)
                            pass
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
                status[path] = {'error': str(e)}

        with self._lock:
            self._last_check_time = now
            self._last_status = status
            # R16-P1-RES-17修复: 追加历史记录并持久化
            self._history.append({'timestamp': now, 'status': status})
            if len(self._history) > self._max_history_size:
                self._history = self._history[-self._max_history_size:]
            self._save_history()
        return status

    def _load_history(self) -> None:
        """R16-P1-RES-17修复: 启动时加载历史监控数据"""
        if not self._persistence_file:
            return
        try:
            if os.path.exists(self._persistence_file):
                with open(self._persistence_file, 'r', encoding='utf-8') as f:
                    self._history = json_loads(f.read())  # R4-4: 统一json_loads
                if len(self._history) > self._max_history_size:
                    self._history = self._history[-self._max_history_size:]
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.debug("[R16-P1-RES-17] 加载监控历史失败: %s", e)
            self._history = []

    def _save_history(self) -> None:
        """R16-P1-RES-17修复: 持久化历史监控数据"""
        if not self._persistence_file:
            return
        try:
            # R2-2修复: 使用 atomic_replace_file 替代手动临时文件+os.replace
            _result = atomic_replace_file(self._persistence_file, json_dumps(self._history[-self._max_history_size:]))
            if not _result.get('success'):
                logger.debug("[R16-P1-RES-17] 保存监控历史失败: %s", _result.get('error'))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.debug("[R16-P1-RES-17] 保存监控历史失败: %s", e)

    def get_history(self) -> List[Dict[str, Any]]:
        """R16-P1-RES-17修复: 获取历史监控数据"""
        with self._lock:
            return list(self._history)


# 全局单例
_disk_space_monitor: Optional[DiskSpaceMonitor] = None
_disk_space_monitor_lock = threading.Lock()


def get_disk_space_monitor() -> DiskSpaceMonitor:
    global _disk_space_monitor
    with _disk_space_monitor_lock:
        if _disk_space_monitor is None:
            _disk_space_monitor = DiskSpaceMonitor()
        return _disk_space_monitor

