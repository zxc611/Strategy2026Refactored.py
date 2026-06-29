"""health/统一健康检查模块 — 合并自4个子模块"""
from __future__ import annotations

import errno
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional

from ali2026v3_trading.infra.resilience import deterministic_round
from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from ali2026v3_trading.infra.serialization_utils import safe_jsonl_append_line
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
from ali2026v3_trading.infra.event_bus import RateLimiter

logger = get_logger(__name__)  # R9-5
"""diagnosis/统一诊断模块 — 合并自3个子模块"""
import functools
import importlib
import queue
import re
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Protocol, Set, Tuple, runtime_checkable
from ali2026v3_trading.infra.serialization_utils import json_dumps
from ali2026v3_trading.infra.shared_utils import normalize_instrument_id, CHINA_TZ
# ============================================================
# Section 1: 探针诊断 (原 diagnosis_probe.py)
# ============================================================




# ============================================================
# Section 1: 健康监控器 (原 health_monitor.py)
# ============================================================

class HealthMonitor:
    """DR-P1-07修复: 心跳检测 + 自动故障判定

    心跳检测：每30秒检查核心服务是否响应
    连续3次心跳失败 → 标记 DEGRADED
    连续5次心跳失败 → 标记 CRITICAL → 触发自动恢复
    故障日志写入 logs/health_events.jsonl
    """

    HEARTBEAT_INTERVAL_SEC = 30.0
    DEGRADED_THRESHOLD = 3
    CRITICAL_THRESHOLD = 5

    def __init__(self):
        self._last_heartbeat: float = 0.0
        self._consecutive_failures: int = 0
        self._status: str = "HEALTHY"
        self._lock = threading.Lock()
        self._health_events_path = os.path.join('logs', 'health_events.jsonl')
        self._core_services: Dict[str, Any] = {}
        os.makedirs(os.path.dirname(self._health_events_path), exist_ok=True)

    def register_service(self, name: str, service: Any) -> None:
        """注册需要心跳检测的核心服务"""
        self._core_services[name] = service

    def heartbeat(self) -> str:
        """执行一次心跳检测，返回当前状态

        Returns:
            'HEALTHY', 'DEGRADED', 或 'CRITICAL'
        """
        now = time.time()
        self._last_heartbeat = now
        all_ok = True

        if self._core_services:
            for name, svc in self._core_services.items():
                try:
                    if hasattr(svc, 'health_check'):
                        if not svc.health_check():
                            all_ok = False
                            logging.warning("[DR-P1-07] 服务 %s 健康检查失败", name)
                    elif hasattr(svc, 'is_alive'):
                        if not svc.is_alive():
                            all_ok = False
                            logging.warning("[DR-P1-07] 服务 %s 线程已死亡", name)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    all_ok = False
                    logging.warning("[DR-P1-07] 服务 %s 健康检查异常: %s", name, e)

        with self._lock:
            if all_ok:
                if self._consecutive_failures > 0:
                    logging.info("[DR-P1-07] 心跳恢复正常 (之前%d次连续失败)", self._consecutive_failures)
                self._consecutive_failures = 0
                self._status = "HEALTHY"
            else:
                self._consecutive_failures += 1
                if self._consecutive_failures >= self.CRITICAL_THRESHOLD:
                    new_status = "CRITICAL"
                elif self._consecutive_failures >= self.DEGRADED_THRESHOLD:
                    new_status = "DEGRADED"
                else:
                    new_status = "HEALTHY"

                if new_status != self._status:
                    self._status = new_status
                    self._log_health_event(
                        f"状态变更: {new_status}",
                        consecutive_failures=self._consecutive_failures,
                    )
                    logging.warning(
                        "[DR-P1-07] 健康状态变更: %s (连续失败=%d)",
                        new_status, self._consecutive_failures,
                    )

            return self._status

    def _log_health_event(self, message: str, **extra) -> None:
        """记录健康事件到JSONL日志"""
        try:
            record = {
                'timestamp': datetime.now(CHINA_TZ).isoformat(),
                'event_type': 'health_monitor',
                'status': self._status,
                'consecutive_failures': self._consecutive_failures,
                'message': message,
            }
            record.update(extra)
            with open(self._health_events_path, 'a', encoding='utf-8') as f:
                safe_jsonl_append_line(f, record)
                f.flush()
                os.fsync(f.fileno())
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logging.debug("[DR-P1-07] 健康事件日志写入失败: %s", e)

    def get_status(self) -> str:
        """获取当前健康状态"""
        with self._lock:
            return self._status

    def is_critical(self) -> bool:
        """是否为CRITICAL状态"""
        return self.get_status() == "CRITICAL"

    def is_degraded(self) -> bool:
        """是否为DEGRADED状态"""
        return self.get_status() == "DEGRADED"

    def reset(self) -> None:
        """重置故障计数"""
        with self._lock:
            self._consecutive_failures = 0
            self._status = "HEALTHY"


_health_monitor: Optional[HealthMonitor] = None
_health_monitor_lock = threading.Lock()


def get_health_monitor() -> HealthMonitor:
    global _health_monitor
    with _health_monitor_lock:
        if _health_monitor is None:
            _health_monitor = HealthMonitor()
        return _health_monitor


def check_disk_space(path: str = '.') -> Dict[str, Any]:
    """DR-P1-14: 磁盘空间预警检查

    使用 shutil.disk_usage 检查磁盘使用率
    磁盘 < 10% → WARNING
    磁盘 < 5%  → CRITICAL

    Args:
        path: 要检查的路径，默认当前目录

    Returns:
        dict: {total_gb, used_gb, free_gb, usage_percent, status}
    """
    import shutil
    try:
        usage = shutil.disk_usage(path)
        total_gb = usage.total / (1024 ** 3)
        used_gb = usage.used / (1024 ** 3)
        free_gb = usage.free / (1024 ** 3)
        usage_percent = (usage.used / usage.total) * 100 if usage.total > 0 else 0

        if usage_percent > 95:
            status = 'CRITICAL'
            logging.critical(
                "[DR-P1-14] CRITICAL: 磁盘空间仅剩%.1f%% (%.2f GB / %.2f GB)",
                usage_percent, free_gb, total_gb,
            )
        elif usage_percent > 90:
            status = 'WARNING'
            logging.warning(
                "[DR-P1-14] WARNING: 磁盘空间仅剩%.1f%% (%.2f GB / %.2f GB)",
                usage_percent, free_gb, total_gb,
            )
        else:
            status = 'HEALTHY'
            logging.debug(
                "[DR-P1-14] 磁盘空间正常: %.1f%% 已用 (%.2f GB / %.2f GB)",
                usage_percent, used_gb, total_gb,
            )

        return {
            'total_gb': deterministic_round(total_gb, 2),
            'used_gb': deterministic_round(used_gb, 2),
            'free_gb': deterministic_round(free_gb, 2),
            'usage_percent': deterministic_round(usage_percent, 2),
            'status': status,
        }
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.error("[DR-P1-14] 磁盘空间检查失败: %s", e)
        return {
            'total_gb': 0, 'used_gb': 0, 'free_gb': 0,
            'usage_percent': 0, 'status': 'UNKNOWN', 'error': str(e),
        }


# ============================================================
# Section 2: 结构化JSONL日志 (原 health_jsonl_logger.py)
# ============================================================

class StructuredJsonlLogger:
    _JSONL_SCHEMA_VERSION = "v1.0"
    _JSONL_MAX_BYTES = 50 * 1024 * 1024
    _JSONL_BACKUP_COUNT = 3

    def __init__(self, log_dir: str = "logs", strategy_id: Optional[str] = None):
        if strategy_id:
            log_dir = os.path.join(log_dir, str(strategy_id))
        os.makedirs(log_dir, exist_ok=True)
        self._log_dir = log_dir
        self._signal_log_path = os.path.join(log_dir, "signals.jsonl")
        self._order_log_path = os.path.join(log_dir, "orders.jsonl")
        self._rotate_if_needed(self._signal_log_path)
        self._rotate_if_needed(self._order_log_path)
        self._signal_log_f = open(self._signal_log_path, "a", encoding="utf-8")
        self._order_log_f = open(self._order_log_path, "a", encoding="utf-8")

    def _rotate_if_needed(self, path: str) -> None:
        """LG-P1-02修复: JSONL文件超限时rotate"""
        try:
            if os.path.exists(path) and os.path.getsize(path) > self._JSONL_MAX_BYTES:
                for i in range(self._JSONL_BACKUP_COUNT - 1, 0, -1):
                    src = f"{path}.{i}"
                    dst = f"{path}.{i+1}"
                    if os.path.exists(src):
                        os.replace(src, dst)
                os.replace(path, f"{path}.1")
        except OSError as _os_err:
            if getattr(_os_err, 'errno', None) == errno.ENOSPC:
                logging.critical("[P1-16] 磁盘空间不足，日志轮转失败: %s", _os_err)
            else:
                logging.debug("[P1-16] 日志轮转OSError(非ENOSPC): %s", _os_err)

    def close(self):
        """关闭文件句柄 — NEW-P1-01修复: 添加异常安全保护"""
        for attr_name in ('_signal_log_f', '_order_log_f'):
            fh = getattr(self, attr_name, None)
            if fh:
                try:
                    fh.flush()
                    fh.close()
                except (OSError, ValueError) as _err:
                    logging.debug("[health] I/O操作降级: %s", _err)
                finally:
                    setattr(self, attr_name, None)

    def __del__(self):
        try:
            self.close()
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] __del__ suppressed: %s", _r3_err)
            pass

    def log_signal(self, signal: Dict[str, Any], strategy_id: Optional[str] = None) -> None:
        _sid = strategy_id or signal.get("strategy_id", "unknown")
        if not _sid or _sid == "unknown":
            logging.warning("[P1-R11-17] strategy_id missing in log entry, may cause audit ambiguity")
        record = {
            "schema_version": self._JSONL_SCHEMA_VERSION,
            "timestamp": datetime.now(CHINA_TZ).isoformat(),
            "event_type": "signal",
            "strategy_id": _sid,
            "signal_id": signal.get("signal_id", ""),
            "instrument_id": signal.get("instrument_id", ""),
            "direction": signal.get("direction", ""),
            "strength": signal.get("strength", 0.0),
            "estimated_plr": signal.get("estimated_plr", 0.0),
            "decision_score": signal.get("decision_score", 0.0),
            "position_scale": signal.get("position_scale", 1.0),
            "filtered": signal.get("filtered", False),
            "filter_reason": signal.get("filter_reason", ""),
        }
        _sanitize_keys = {'api_key', 'access_key', 'access_secret', 'password', 'token', 'secret', 'credential'}
        record = {k: ('***' if k in _sanitize_keys else v) for k, v in record.items()}
        safe_jsonl_append_line(self._signal_log_f, record)

    def log_order(self, order: Dict[str, Any]) -> None:
        record = {
            "schema_version": self._JSONL_SCHEMA_VERSION,
            "timestamp": datetime.now(CHINA_TZ).isoformat(),
            "event_type": "order",
            "order_id": order.get("order_id", ""),
            "instrument_id": order.get("instrument_id", ""),
            "direction": order.get("direction", ""),
            "price": order.get("price", 0.0),
            "volume": order.get("volume", 0),
            "order_type": order.get("order_type", ""),
            "status": order.get("status", ""),
            "traded_volume": order.get("traded_volume", 0),
            "remaining_volume": order.get("remaining_volume", 0),
        }
        _sanitize_keys = {'api_key', 'access_key', 'access_secret', 'password', 'token', 'secret', 'credential'}
        record = {k: ('***' if k in _sanitize_keys else v) for k, v in record.items()}
        self._rotate_if_needed(self._order_log_path)
        safe_jsonl_append_line(self._order_log_f, record)

    def log_close(self, close_record: Dict[str, Any]) -> None:
        record = {
            "timestamp": datetime.now(CHINA_TZ).isoformat(),
            "event_type": "close",
            "position_id": close_record.get("position_id", ""),
            "instrument_id": close_record.get("instrument_id", ""),
            "pnl": close_record.get("pnl", 0.0),
            "close_reason": close_record.get("close_reason", ""),
            "hold_time_minutes": close_record.get("hold_time_minutes", 0.0),
        }
        with open(self._order_log_path, "a", encoding="utf-8") as f:
            safe_jsonl_append_line(f, record)
            f.flush()
            os.fsync(f.fileno())

    def log_risk_decision(self, decision: Dict[str, Any]) -> None:
        """R24-P1-TR-12修复: 记录风控决策事件"""
        record = {
            "timestamp": datetime.now(CHINA_TZ).isoformat(),
            "event_type": "risk_decision",
            "decision": decision.get("decision", ""),
            "detail": decision.get("detail", {}),
            "risk_level": decision.get("risk_level", ""),
            "threshold": decision.get("threshold"),
            "actual": decision.get("actual"),
        }
        _risk_log_path = os.path.join(self._log_dir, "risk_decisions.jsonl")
        with open(_risk_log_path, "a", encoding="utf-8") as f:
            safe_jsonl_append_line(f, record)
            f.flush()
            os.fsync(f.fileno())

    def log_strategy_switch(self, from_mode: str, to_mode: str, reason: str) -> None:
        """R24-P1-TR-12修复: 记录策略切换事件"""
        record = {
            "timestamp": datetime.now(CHINA_TZ).isoformat(),
            "event_type": "strategy_switch",
            "from_mode": from_mode,
            "to_mode": to_mode,
            "reason": reason,
        }
        _strategy_log_path = os.path.join(self._log_dir, "strategy_switches.jsonl")
        with open(_strategy_log_path, "a", encoding="utf-8") as f:
            safe_jsonl_append_line(f, record)
            f.flush()
            os.fsync(f.fileno())

    def log_diagnostic(self, component: str, message: str, data: Optional[Dict[str, Any]] = None) -> None:
        """R24-P1-TR-12修复: 记录诊断事件"""
        record = {
            "timestamp": datetime.now(CHINA_TZ).isoformat(),
            "event_type": "diagnostic",
            "component": component,
            "message": message,
            "data": data or {},
        }
        _diagnostic_log_path = os.path.join(self._log_dir, "diagnostics.jsonl")
        with open(_diagnostic_log_path, "a", encoding="utf-8") as f:
            safe_jsonl_append_line(f, record)
            f.flush()
            os.fsync(f.fileno())


# ============================================================
# Section 3: 健康检查API (原 health_check_service.py)
# ============================================================

class HealthCheckAPI:
    DEFAULT_SIGNAL_STALE_SECONDS = 300.0
    DEFAULT_ORDER_STALE_SECONDS = 600.0
    DEFAULT_DAILY_DRAWDOWN_THRESHOLD = 0.05
    DEFAULT_MAX_OPEN_POSITIONS = 10

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        cfg = config or {}
        self._start_time = datetime.now(CHINA_TZ)
        self._rate_limiter = RateLimiter(
            rate=cfg.get('rate_limit_max_calls', 60),
            capacity=cfg.get('rate_limit_max_calls', 60),
        )
        self._structured_logger: Optional[StructuredJsonlLogger] = None
        try:
            self._structured_logger = StructuredJsonlLogger(
                log_dir=cfg.get('structured_log_dir', 'logs'),
                strategy_id=cfg.get('strategy_id'),
            )
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] __init__ suppressed: %s", _r3_err)
            pass
        self._component_status: Dict[str, str] = {
            "data_feeds": "unknown",
            "greeks_calculator": "unknown",
            "state_diagnosis": "unknown",
            "signal_generator": "unknown",
            "risk_manager": "unknown",
            "order_executor": "unknown",
        }
        self._open_position_count: int = 0
        self._last_signal_time: Optional[float] = 0.0
        self._last_order_time: Optional[float] = 0.0

        self._signal_stale_seconds = self.DEFAULT_SIGNAL_STALE_SECONDS
        self._order_stale_seconds = self.DEFAULT_ORDER_STALE_SECONDS
        try:
            from ali2026v3_trading.config.config_service import ConfigService
            cs = ConfigService()
            self._signal_stale_seconds = cfg.get(
                'signal_stale_seconds',
                getattr(cs.trading, 'strategy_heartbeat_interval_seconds', self.DEFAULT_SIGNAL_STALE_SECONDS) * 10
            )
            self._order_stale_seconds = cfg.get(
                'order_stale_seconds',
                getattr(cs.trading, 'strategy_heartbeat_interval_seconds', self.DEFAULT_ORDER_STALE_SECONDS) * 20
            )
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] unknown suppressed: %s", _r3_err)
            pass

        self._alert_thresholds = {
            "l1_circuit_breaker": cfg.get('l1_circuit_breaker', True),
            "daily_drawdown": cfg.get('daily_drawdown_threshold', self.DEFAULT_DAILY_DRAWDOWN_THRESHOLD),
            "max_open_positions": cfg.get('max_open_positions', self.DEFAULT_MAX_OPEN_POSITIONS),
        }
        self._push_interval_sec: float = cfg.get('push_interval_sec', 30.0)
        self._push_thread = None
        self._push_stop = threading.Event()
        self._push_callback = None

        self._health_change_callbacks: list = []
        self._last_overall_status: str = "unknown"

        self._cached_greeks_dashboard: Optional[Dict[str, Any]] = None
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _bus.subscribe_weak('greeks.dashboard_updated', self._on_greeks_dashboard_updated)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] unknown suppressed: %s", _r3_err)
            pass

    def update_component_status(self, component: str, status: str) -> None:
        if component in self._component_status:
            self._component_status[component] = status

    def _probe_component_status(self, component: str) -> str:
        """R30-P0-03修复: 对unknown组件执行存活探测"""
        try:
            if component == 'greeks_calculator':
                from ali2026v3_trading.governance.greeks_calculator import get_greeks_calculator
                gc = get_greeks_calculator()
                return 'healthy' if gc is not None else 'degraded'
            elif component == 'state_diagnosis':
                from ali2026v3_trading.config.state_param import get_state_param_manager
                spm = get_state_param_manager()
                return 'healthy' if spm is not None else 'degraded'
            elif component == 'risk_manager':
                from ali2026v3_trading.risk.risk_service import RiskService
                return 'healthy' if RiskService is not None else 'degraded'
            elif component == 'order_executor':
                from ali2026v3_trading.order.order_service import get_order_service
                os = get_order_service()
                return 'healthy' if os is not None else 'degraded'
        except (ImportError, AttributeError) as e:
            logger.debug("[R30-P0-03] 组件%s探测失败: %s", component, e)
        return 'unknown'

    def _refresh_unknown_components(self) -> None:
        """R30-P0-03修复: 刷新所有unknown组件的状态"""
        for comp, status in list(self._component_status.items()):
            if status == 'unknown':
                probed = self._probe_component_status(comp)
                if probed != 'unknown':
                    self._component_status[comp] = probed

    def _on_greeks_dashboard_updated(self, event: Any) -> None:
        """DFG-P1-04修复: 消费Greeks仪表盘更新事件，缓存最新数据"""
        try:
            if isinstance(event, dict):
                self._cached_greeks_dashboard = event.get('data', event)
            elif hasattr(event, 'data'):
                self._cached_greeks_dashboard = event.data
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] _on_greeks_dashboard_updated suppressed: %s", _r3_err)
            pass

    def _push_health_status(self):
        """定时推送健康状态而非仅被动查询"""
        while not self._push_stop.is_set():
            self._push_stop.wait(timeout=self._push_interval_sec)
            if self._push_stop.is_set():
                break
            if self._push_callback:
                try:
                    self._push_callback(self.get_health_status())
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.debug("R15-P1-RES-10: push_health_status异常: %s", e)
            try:
                from ali2026v3_trading.infra.event_bus import get_global_event_bus, HealthStatusEvent
                bus = get_global_event_bus()
                status = self.get_health_status()
                event = HealthStatusEvent(
                    overall_status=status.get('overall_status', 'unknown'),
                    component_status=status.get('component_status', {}),
                    details=status,
                )
                bus.publish(event, async_mode=True)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                logging.debug("OPS-P1-02: EventBus健康状态推送异常: %s", e)

    def start_push(self, callback, interval_sec: float = None) -> None:
        """启动健康状态主动推送"""
        self._push_callback = callback
        if interval_sec:
            self._push_interval_sec = interval_sec
        self._push_stop.clear()
        self._push_thread = threading.Thread(target=self._push_health_status, daemon=True, name='health_push')
        self._push_thread.start()

    def stop_push(self) -> None:
        self._push_stop.set()

    def update_open_position_count(self, count: int) -> None:
        self._open_position_count = count

    def update_last_signal_time(self, timestamp: float = None) -> None:
        self._last_signal_time = timestamp if timestamp is not None else time.time()

    def update_last_order_time(self, timestamp: float = None) -> None:
        self._last_order_time = timestamp if timestamp is not None else time.time()

    def get_health_status(self) -> Dict[str, Any]:
        # P1-01修复: 优先委托HealthCheckAggregator获取核心健康状态
        _aggregator_result = self._delegate_to_aggregator()
        if _aggregator_result is not None:
            # 合并聚合器结果与API独有信息
            _aggregator_result.update({
                "open_position_count": self._open_position_count,
                "last_signal_time": self._last_signal_time,
                "last_order_time": self._last_order_time,
                "alert_thresholds": dict(self._alert_thresholds),
                "delegated_to": "HealthCheckAggregator",
            })
            return _aggregator_result

        self._refresh_unknown_components()
        if not self._rate_limiter.acquire():
            logger.warning("[HealthCheckAPI] Rate limit exceeded, request rejected")
            return {
                "overall_status": "rate_limited",
                "message": "Too many requests, please try again later",
                "timestamp": time.time(),
            }
        now = time.time()
        signal_stale = False
        if self._last_signal_time and self._last_signal_time > 0:
            signal_stale = (now - self._last_signal_time) > self._signal_stale_seconds

        order_stale = False
        if self._last_order_time and self._last_order_time > 0:
            order_stale = (now - self._last_order_time) > self._order_stale_seconds

        overall = "healthy"
        if any(s != "ok" for s in self._component_status.values()):
            overall = "degraded"
        if signal_stale or order_stale:
            overall = "degraded"
        if self._open_position_count > self._alert_thresholds["max_open_positions"]:
            overall = "warning"

        result = {
            "overall_status": overall,
            "component_status": dict(self._component_status),
            "open_position_count": self._open_position_count,
            "last_signal_time": self._last_signal_time,
            "last_order_time": self._last_order_time,
            "signal_stale": signal_stale,
            "order_stale": order_stale,
            "alert_thresholds": dict(self._alert_thresholds),
            "timestamp": now,
            "uptime_seconds": (datetime.now(CHINA_TZ) - self._start_time).total_seconds(),
            "daily_drawdown_pct": self._get_daily_drawdown_pct(),
            "circuit_breaker_status": self._get_circuit_breaker_status(),
        }
        try:
            from ali2026v3_trading.infra.service_container import get_service_container
            _sc = get_service_container()
            if _sc is not None:
                _cross_components = {}
                for _cname in ('RiskService', 'OrderService', 'ModeEngine', 'PositionService'):
                    _comp = _sc.get(_cname) if hasattr(_sc, 'get') else None
                    if _comp is not None and hasattr(_comp, 'get_health_status'):
                        try:
                            _cross_components[_cname] = _comp.get_health_status()
                        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                            logging.debug("[health] 数据解析降级: %s", _r3_err)
                            _cross_components[_cname] = {"status": "query_failed"}
                if _cross_components:
                    result["cross_components"] = _cross_components
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] unknown suppressed: %s", _r3_err)
            pass
        try:
            from ali2026v3_trading.infra.shared_utils import ThreadMgr
            _hm = get_health_monitor()
            _tm_candidates = [svc for svc in getattr(_hm, '_core_services', {}).values() if isinstance(svc, ThreadMgr)]
            if _tm_candidates:
                _zombie_result = _tm_candidates[0].detect_zombie_threads()
                if _zombie_result.get('zombie_count', 0) > 0:
                    result['zombie_threads'] = _zombie_result
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] unknown suppressed: %s", _r3_err)
            pass

        if self._structured_logger is not None:
            try:
                self._structured_logger.log_signal({
                    'event_type': 'health_check',
                    'overall_status': overall,
                    'signal_stale': signal_stale,
                    'order_stale': order_stale,
                    'open_position_count': self._open_position_count,
                })
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] unknown suppressed: %s", _r3_err)
                pass

        if overall != self._last_overall_status:
            self._fire_health_change_callbacks(self._last_overall_status, overall, result)
            self._last_overall_status = overall

        try:
            from ali2026v3_trading.config.ui_service import UIDiagnosisTool
            _ui_diag = UIDiagnosisTool.run_diagnosis()
            result['component_status']['ui'] = 'OK' if _ui_diag.get('tkinter', {}).get('available', False) else 'DEGRADED'
            result['ui_diagnostics'] = _ui_diag
        except ImportError:
            result['component_status']['ui'] = 'UNKNOWN'
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _ui_err:
            result['component_status']['ui'] = 'ERROR'
            result['ui_error'] = str(_ui_err)

        return result

    def _delegate_to_aggregator(self) -> Optional[Dict[str, Any]]:
        """P1-01修复: 委托HealthCheckAggregator获取核心健康状态。
        若聚合器可用且有strategy_service引用，返回聚合结果；
        否则返回None，回退到HealthCheckAPI原有逻辑。
        """
        try:
            from ali2026v3_trading.infra.service_container import get_service_container
            _sc = get_service_container()
            if _sc is not None:
                _svc = _sc.get('StrategyService') if hasattr(_sc, 'get') else None
                if _svc is not None:
                    agg = HealthCheckAggregator(_svc)
                    result = agg.aggregate()
                    # 转换聚合器结果格式为API兼容格式
                    overall = result.get('overall_status', 'unknown')
                    component_status = {}
                    for check_name, check_result in result.items():
                        if check_name == 'overall_status':
                            continue
                        if isinstance(check_result, dict):
                            component_status[check_name] = check_result.get('status', 'unknown')
                    return {
                        "overall_status": overall.lower() if isinstance(overall, str) else overall,
                        "component_status": component_status,
                        "timestamp": time.time(),
                        "uptime_seconds": (datetime.now(CHINA_TZ) - self._start_time).total_seconds(),
                    }
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            logging.debug("[P1-01] HealthCheckAggregator委托失败(回退到API逻辑): %s", e)
        return None

    def _get_daily_drawdown_pct(self) -> float:
        try:
            from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
            _sid = str(getattr(self, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
            if safety is not None:
                return float(getattr(safety, '_daily_drawdown', 0.0))
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] _get_daily_drawdown_pct suppressed: %s", _r3_err)
            pass
        return 0.0

    def _get_circuit_breaker_status(self) -> str:
        try:
            from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
            _sid = str(getattr(self, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
            if safety is not None:
                # [P0-29修复] 从 _risk_cb_half_open 派生暂停/冷静状态
                cb = getattr(safety, '_risk_cb_half_open', None)
                if cb is not None and cb.state == 'OPEN' and cb.opened_at > 0:
                    if time.time() < cb.opened_at + cb.open_duration:
                        return "triggered"
                calm_duration = getattr(safety, '_calm_period_duration', 0.0)
                if cb is not None and cb.opened_at > 0 and calm_duration > 0:
                    if time.time() < cb.opened_at + calm_duration:
                        return "calm_period"
                return "normal"
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] _get_circuit_breaker_status suppressed: %s", _r3_err)
            pass
        return "unknown"

    def check_l1_alerts(self, daily_drawdown_pct: float = 0.0) -> List[str]:
        alerts = []
        if daily_drawdown_pct > self._alert_thresholds["daily_drawdown"]:
            alerts.append(f"L1_ALERT: daily_drawdown={daily_drawdown_pct:.2%} exceeds threshold={self._alert_thresholds['daily_drawdown']:.2%}")
        if self._open_position_count > self._alert_thresholds["max_open_positions"]:
            alerts.append(f"L1_ALERT: open_positions={self._open_position_count} exceeds max={self._alert_thresholds['max_open_positions']}")
        return alerts

    def get_greeks_dashboard(self) -> Dict[str, Any]:
        """DFG-P1-04修复: 返回当前Greeks仪表盘数据"""
        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service
            rs = get_risk_service()
            if rs is not None and hasattr(rs, 'get_greeks_dashboard'):
                dashboard = rs.get_greeks_dashboard()
                return {
                    'status': 'OK',
                    'data': dashboard,
                    'timestamp': time.time(),
                }
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logger.warning("[DFG-P1-04] get_greeks_dashboard异常: %s", e)
        return {
            'status': 'UNAVAILABLE',
            'data': None,
            'timestamp': time.time(),
            'error': 'RiskService or greeks_dashboard not available',
        }

    def greeks_dashboard_data(self) -> Dict[str, Any]:
        """OPS-P1-01修复: 返回Greeks仪表盘结构化数据"""
        raw = self.get_greeks_dashboard()
        if raw.get('status') != 'OK' or raw.get('data') is None:
            return {
                'status': 'UNAVAILABLE',
                'portfolio_greeks': {},
                'risk_indicators': {},
                'timestamp': time.time(),
            }

        data = raw['data']
        portfolio = data.get('portfolio', {}) if isinstance(data, dict) else {}
        stress = data.get('stress', {}) if isinstance(data, dict) else {}
        safety = data.get('safety', {}) if isinstance(data, dict) else {}

        return {
            'status': 'OK',
            'portfolio_greeks': {
                'delta': portfolio.get('delta', 0.0),
                'gamma': portfolio.get('gamma', 0.0),
                'vega': portfolio.get('vega', 0.0),
                'theta': portfolio.get('theta', 0.0),
            },
            'risk_indicators': {
                'max_loss_scenario': stress.get('max_loss', 0.0),
                'var_95': safety.get('var_95', 0.0),
                'concentration_risk': safety.get('concentration_risk', 'unknown'),
            },
            'timestamp': time.time(),
        }

    def register_health_change_callback(self, callback) -> None:
        """OPS-P1-02修复/P1-05修复: 注册到EventBus单通道"""
        try:
            from ali2026v3_trading.infra.event_bus import EventBus
            event_bus = EventBus.get_instance()
            event_bus.subscribe("health_status_changed", callback)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[health] 属性访问降级: %s", _r3_err)
            if callback not in self._health_change_callbacks:
                self._health_change_callbacks.append(callback)

    def _fire_health_change_callbacks(self, old_status: str, new_status: str,
                                       details: Dict[str, Any]) -> None:
        """OPS-P1-02修复/P1-05修复: 统一通过EventBus发布"""
        event = {
            'old_status': old_status,
            'new_status': new_status,
            'timestamp': time.time(),
            'details': details,
        }
        try:
            from ali2026v3_trading.infra.event_bus import EventBus
            event_bus = EventBus.get_instance()
            event_bus.publish("health_status_changed", event)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[health] 属性访问降级: %s", _r3_err)
            for cb in self._health_change_callbacks:
                try:
                    cb(event)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.warning("OPS-P1-02: 健康变更回调异常: %s", e)

    def get_log_stats(self, root_logger_name: str = "") -> Dict[str, Any]:
        """返回最近日志统计信息，用于日志聚合与监控"""
        try:
            target_logger = logging.getLogger(root_logger_name)
            stats: Dict[str, Any] = {
                "logger_name": root_logger_name or "root",
                "level": logging.getLevelName(target_logger.level),
                "effective_level": logging.getLevelName(target_logger.getEffectiveLevel()),
                "handler_count": len(target_logger.handlers),
                "handler_types": [type(h).__name__ for h in target_logger.handlers],
            }
            for handler in target_logger.handlers:
                if hasattr(handler, 'baseFilename'):
                    try:
                        log_path = handler.baseFilename
                        if os.path.isfile(log_path):
                            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                                line_count = sum(1 for _ in f)
                            stats["log_file"] = log_path
                            stats["log_file_lines"] = line_count
                            stats["log_file_size_mb"] = deterministic_round(
                                os.path.getsize(log_path) / (1024 * 1024), 2
                            )
                    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                        logging.debug("[R3-L2] get_log_stats suppressed: %s", _r3_err)
                        pass
            return stats
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.error("[HealthCheckAPI.get_log_stats] Internal error: %s", e)
            return {"error": "Internal error occurred"}

    def check_capacity_limits(self) -> Dict[str, Any]:
        """OPS-11修复: 检查系统容量是否接近上限"""
        import shutil
        capacity_status = {
            'timestamp': time.time(),
            'checks': {},
            'overall': 'OK',
        }
        any_warning = False
        any_critical = False

        try:
            max_positions = self._alert_thresholds.get('max_open_positions', None)
            if max_positions is None:
                try:
                    from ali2026v3_trading.config.config_params import get_param
                    max_positions = int(get_param('max_open_positions', get_param('max_position_count', 3)))
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[health] 数据解析降级: %s", _r3_err)
                    max_positions = 3
            current_positions = self._open_position_count
            position_usage = current_positions / max_positions if max_positions > 0 else 0
            if position_usage >= 1.0:
                position_status = 'CRITICAL'
                any_critical = True
            elif position_usage >= 0.8:
                position_status = 'WARNING'
                any_warning = True
            else:
                position_status = 'OK'
            capacity_status['checks']['positions'] = {
                'current': current_positions,
                'limit': max_positions,
                'usage_pct': deterministic_round(position_usage * 100, 1),
                'status': position_status,
            }
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            capacity_status['checks']['positions'] = {'status': 'ERROR', 'error': str(e)}

        try:
            import resource as _resource
            try:
                usage = shutil.disk_usage('.')
                mem_status = 'OK'
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[health] I/O操作降级: %s", _r3_err)
                mem_status = 'UNKNOWN'
            capacity_status['checks']['memory'] = {'status': mem_status}
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[health] I/O操作降级: %s", _r3_err)
            capacity_status['checks']['memory'] = {'status': 'UNAVAILABLE'}

        try:
            disk_result = check_disk_space('.')
            disk_usage_pct = disk_result.get('usage_percent', 0)
            if disk_result.get('status') == 'CRITICAL':
                disk_status = 'CRITICAL'
                any_critical = True
            elif disk_result.get('status') == 'WARNING':
                disk_status = 'WARNING'
                any_warning = True
            else:
                disk_status = 'OK'
            capacity_status['checks']['disk'] = {
                'usage_pct': disk_usage_pct,
                'free_gb': disk_result.get('free_gb', 0),
                'status': disk_status,
            }
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            capacity_status['checks']['disk'] = {'status': 'ERROR', 'error': str(e)}

        try:
            rate_limiter = getattr(self, '_rate_limiter', None)
            if rate_limiter:
                max_calls = getattr(rate_limiter, '_capacity', 60)
                current_tokens = getattr(rate_limiter, '_tokens', float(max_calls))
                freq_usage = 1.0 - (current_tokens / max_calls) if max_calls > 0 else 0
                if freq_usage >= 0.9:
                    freq_status = 'WARNING'
                    any_warning = True
                else:
                    freq_status = 'OK'
                capacity_status['checks']['order_frequency'] = {
                    'recent_calls': recent_calls,
                    'limit': max_calls,
                    'usage_pct': deterministic_round(freq_usage * 100, 1),
                    'status': freq_status,
                }
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            capacity_status['checks']['order_frequency'] = {'status': 'ERROR', 'error': str(e)}

        if any_critical:
            capacity_status['overall'] = 'CRITICAL'
        elif any_warning:
            capacity_status['overall'] = 'WARNING'

        if any_critical or any_warning:
            try:
                from ali2026v3_trading.risk.risk_service import alert, AlertLevel
                level = AlertLevel.P0 if any_critical else AlertLevel.P1
                alert(level, 'capacity_limit_approaching',
                      f'容量接近上限: {capacity_status["overall"]}',
                      capacity_status['checks'])
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] unknown suppressed: %s", _r3_err)
                pass

        return capacity_status


# ============================================================
# Section 4: 健康检查聚合器 (原 health_check_aggregator.py)
# ============================================================

# P1-01: 本模块为唯一健康检查入口(权威源)，其他健康检查模块已deprecated
class HealthCheckAggregator:
    """健康检查聚合器

    _CHECKS定义6个独立检查项:
    1. _check_connection_state: 连接状态检查
    2. _check_heartbeat_status: 心跳状态检查
    3. _check_resource_usage: 资源使用检查
    4. _check_shadow_engine: 影子引擎检查
    5. _check_strategy_ecosystem: 策略生态系统检查
    6. _check_safety_meta_layer: 安全元层检查
    """

    _CHECKS = [
        '_check_connection_state',
        '_check_heartbeat_status',
        '_check_resource_usage',
        '_check_shadow_engine',
        '_check_strategy_ecosystem',
        '_check_safety_meta_layer',
    ]

    def __init__(self, strategy_service: Any):
        self._svc = strategy_service

    def aggregate(self) -> Dict[str, Any]:
        """编排器: 聚合所有检查项，≤20行，圈复杂度≤3"""
        results = {}
        for check_name in self._CHECKS:
            try:
                results[check_name] = getattr(self, check_name)()
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                results[check_name] = {'status': 'ERROR', 'error': str(e)}
                logging.warning("[HealthCheckAggregator] %s异常: %s", check_name, e)
        results['overall_status'] = self._determine_overall(results)
        return results

    def _check_connection_state(self) -> Dict[str, Any]:
        """检查1: 连接状态(平台/交易所连接)"""
        _connected = getattr(self._svc, '_platform_api_ready', False)
        return {
            'status': 'OK' if _connected else 'DEGRADED',
            'platform_connected': _connected,
        }

    def _check_heartbeat_status(self) -> Dict[str, Any]:
        """检查2: 心跳状态(最近心跳时间+延迟)
        P1-01修复: 优先委托 HealthMonitor.heartbeat() 获取心跳状态，失败则回退到原有逻辑
        """
        try:
            _hm = get_health_monitor()
            _hb_status = _hm.heartbeat()
            _status_map = {'HEALTHY': 'OK', 'DEGRADED': 'DEGRADED', 'CRITICAL': 'CRITICAL'}
            return {
                'status': _status_map.get(_hb_status, 'DEGRADED'),
                'last_heartbeat_delay_s': round(time.time() - _hm._last_heartbeat, 1) if _hm._last_heartbeat > 0 else float('inf'),
                'delegated_to': 'HealthMonitor.heartbeat',
            }
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[health] 数据解析降级: %s", _r3_err)  # 回退到原有逻辑
        _last_hb = getattr(self._svc, '_last_heartbeat_ts', 0.0)
        _delay = time.time() - _last_hb if _last_hb > 0 else float('inf')
        return {
            'status': 'OK' if _delay < 30 else 'DEGRADED',
            'last_heartbeat_delay_s': round(_delay, 1),
        }

    def _check_resource_usage(self) -> Dict[str, Any]:
        """检查3: 资源使用(线程池/内存/订单数)"""
        import threading
        _thread_count = threading.active_count()
        _orders = len(getattr(self._svc, '_orders_by_id', {})) if hasattr(self._svc, '_orders_by_id') else 0
        return {
            'status': 'OK' if _thread_count < 50 else 'WARNING',
            'active_threads': _thread_count,
            'tracked_orders': _orders,
        }

    def _check_shadow_engine(self) -> Dict[str, Any]:
        """检查4: 影子引擎状态(Alpha衰减/暂停)"""
        try:
            _shadow = getattr(self._svc, '_shadow_engine', None)
            if _shadow is None:
                return {'status': 'N/A', 'reason': 'shadow_engine_not_initialized'}
            _is_paused = getattr(_shadow, 'is_absolute_ev_paused', lambda: False)()
            return {
                'status': 'PAUSED' if _is_paused else 'OK',
                'ev_paused': _is_paused,
            }
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            return {'status': 'ERROR', 'error': str(e)}

    def _check_strategy_ecosystem(self) -> Dict[str, Any]:
        """检查5: 策略生态系统(策略活跃度/资金分配)
        P1-01修复: 优先委托 LifecycleMonitor.health_check() 获取策略状态，失败则回退到原有逻辑
        """
        try:
            _lifecycle_svc = getattr(self._svc, '_lifecycle_svc', None)
            if _lifecycle_svc is not None:
                _monitor = getattr(_lifecycle_svc, '_lc_monitor', None)
                if _monitor is not None:
                    _lm_result = _monitor.health_check()
                    _lm_status = _lm_result.get('status', 'unknown')
                    _status_map = {'healthy': 'OK', 'degraded': 'DEGRADED', 'warning': 'WARNING', 'unhealthy': 'CRITICAL'}
                    return {
                        'status': _status_map.get(_lm_status, 'DEGRADED'),
                        'strategy_state': _lm_result.get('state', 'unknown'),
                        'issues': _lm_result.get('issues', []),
                        'delegated_to': 'LifecycleMonitor.health_check',
                    }
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[health] 数据解析降级: %s", _r3_err)  # 回退到原有逻辑
        try:
            _ecosystem = getattr(self._svc, '_strategy_ecosystem', None)
            if _ecosystem is None:
                return {'status': 'N/A', 'reason': 'ecosystem_not_initialized'}
            _active = getattr(_ecosystem, 'get_active_strategies', lambda: [])()
            return {
                'status': 'OK' if len(_active) > 0 else 'WARNING',
                'active_strategies': len(_active),
            }
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            return {'status': 'ERROR', 'error': str(e)}

    def _check_safety_meta_layer(self) -> Dict[str, Any]:
        """检查6: 安全元层(熔断器/合规/资金充足性)
        P1-01修复: 优先委托 HealthCheckAPI._get_circuit_breaker_status() 获取熔断器状态，失败则回退到原有逻辑
        """
        try:
            _api = getattr(self._svc, '_health_check_api', None)
            if _api is None:
                _api = HealthCheckAPI()
            _cb_status = _api._get_circuit_breaker_status()
            _status_map = {'triggered': 'CRITICAL', 'calm_period': 'WARNING', 'normal': 'OK', 'unknown': 'DEGRADED'}
            return {
                'status': _status_map.get(_cb_status, 'DEGRADED'),
                'circuit_breaker_status': _cb_status,
                'delegated_to': 'HealthCheckAPI._get_circuit_breaker_status',
            }
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[health] 数据解析降级: %s", _r3_err)  # 回退到原有逻辑
        try:
            from ali2026v3_trading.risk.risk_circuit_breaker import get_safety_meta_layer
            _safety = get_safety_meta_layer()
            if _safety is None:
                return {'status': 'N/A', 'reason': 'safety_meta_layer_not_available'}
            _cb_open = getattr(_safety, '_circuit_breaker_open', False)
            return {
                'status': 'CRITICAL' if _cb_open else 'OK',
                'circuit_breaker_open': _cb_open,
            }
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            return {'status': 'ERROR', 'error': str(e)}

    def _determine_overall(self, results: Dict[str, Any]) -> str:
        """根据各检查项确定整体状态"""
        _statuses = [v.get('status', 'UNKNOWN') for v in results.values() if isinstance(v, dict)]
        if any(s == 'CRITICAL' for s in _statuses):
            return 'CRITICAL'
        if any(s == 'ERROR' for s in _statuses):
            return 'ERROR'
        if any(s == 'DEGRADED' for s in _statuses):
            return 'DEGRADED'
        if any(s == 'WARNING' for s in _statuses):
            return 'WARNING'
        return 'OK'


class CodeQualityMetrics:
    """运行时代码质量指标采集"""

    def collect(self) -> dict:
        return {
            "dead_code_imports": self._check_dead_imports(),
            "api_whitelist_violations": self._check_whitelist(),
            "component_failure_rate": self._get_component_failure_rate(),
            "try_except_suppression_count": self._count_suppressed_errors(),
        }

    def _check_dead_imports(self) -> int:
        """检查死代码导入数量"""
        import sys
        count = 0
        for mod_name, mod in list(sys.modules.items()):
            if mod_name.startswith('ali2026v3_trading'):
                try:
                    code = getattr(mod, '__loader__', None)
                    if code is None:
                        count += 1
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] _check_dead_imports suppressed: %s", _r3_err)
                    pass
        return count

    def _check_whitelist(self) -> int:
        """检查API白名单违规数量"""
        return 0  # 需要运行时数据才能检测

    def _get_component_failure_rate(self) -> float:
        """获取组件失败率"""
        try:
            from ali2026v3_trading.strategy_judgment._judgment_services import was_blocked
            return 1.0 if was_blocked() else 0.0
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[health] 属性访问降级: %s", _r3_err)
            return 0.0

    def _count_suppressed_errors(self) -> int:
        """统计try/except抑制的错误数量"""
        import logging
        logger = logging.getLogger('ali2026v3_trading')
        # 返回当前日志中的错误计数
        if hasattr(logger, 'error_count'):
            return logger.error_count
        return 0

_runtime_strategy_ref = None

def set_runtime_strategy_ref(strategy_obj):
    global _runtime_strategy_ref
    _runtime_strategy_ref = strategy_obj

_DEFAULT_MONITORED_CONTRACTS = {
    'IH2609': {'exchange': 'CFFEX', 'name': '上证50股指期货', 'type': 'future'},
    'IF2609': {'exchange': 'CFFEX', 'name': '沪深300股指期货', 'type': 'future'},
    'IM2609': {'exchange': 'CFFEX', 'name': '中证1000股指期货', 'type': 'future'},
    'cu2609': {'exchange': 'SHFE', 'name': '铜期货', 'type': 'future'},
    'au2609': {'exchange': 'SHFE', 'name': '黄金期货', 'type': 'future'},
    'al2609': {'exchange': 'SHFE', 'name': '铝期货', 'type': 'future'},
    'zn2609': {'exchange': 'SHFE', 'name': '锌期货', 'type': 'future'},
    'HO2609-C-2800': {'underlying': 'IH2609', 'exchange': 'CFFEX', 'name': 'IH看涨2800', 'type': 'option'},
    'IO2609-C-4200': {'underlying': 'IF2609', 'exchange': 'CFFEX', 'name': 'IF看涨4200', 'type': 'option'},
    'MO2609-C-7000': {'underlying': 'IM2609', 'exchange': 'CFFEX', 'name': 'IM看涨7000', 'type': 'option'},
    'cu2609C90000': {'underlying': 'cu2609', 'exchange': 'SHFE', 'name': '铜看涨90000', 'type': 'option'},
    'au2609C1000': {'underlying': 'au2609', 'exchange': 'SHFE', 'name': '黄金看涨1000', 'type': 'option'},
    'al2609C24000': {'underlying': 'al2609', 'exchange': 'SHFE', 'name': '铝看涨24000', 'type': 'option'},
    'zn2609C26000': {'underlying': 'zn2609', 'exchange': 'SHFE', 'name': '锌看涨26000', 'type': 'option'},
    'cu2609C104000': {'underlying': 'cu2609', 'exchange': 'SHFE', 'name': '铜看涨104000', 'type': 'option'},
    'au2609C1032': {'underlying': 'au2609', 'exchange': 'SHFE', 'name': '黄金看涨1032', 'type': 'option'},
    'al2609C22600': {'underlying': 'al2609', 'exchange': 'SHFE', 'name': '铝看涨22600', 'type': 'option'},
    'al2609C25200': {'underlying': 'al2609', 'exchange': 'SHFE', 'name': '铝看涨25200', 'type': 'option'},
    'al2609C26200': {'underlying': 'al2609', 'exchange': 'SHFE', 'name': '铝看涨26200', 'type': 'option'},
    'zn2609C20000': {'underlying': 'zn2609', 'exchange': 'SHFE', 'name': '锌看涨20000', 'type': 'option'},
}


def _find_config_dir() -> Optional[str]:
    _this_file = os.path.abspath(__file__)
    _candidates = [
        os.path.join(os.path.dirname(os.path.dirname(_this_file)), 'config'),
        os.path.join(os.path.dirname(_this_file), '..', 'config'),
        os.path.join(os.getcwd(), 'ali2026v3_trading', 'config'),
        os.path.join(os.getcwd(), 'pyStrategy', 'demo', 'ali2026v3_trading', 'config'),
    ]
    if 'INFINITRADER_SIMULATIONX64' in _this_file.upper() or 'InfiniTrader_SimulationX64' in _this_file:
        _base = _this_file.split('pyStrategy')[0] if 'pyStrategy' in _this_file else os.path.dirname(os.path.dirname(os.path.dirname(_this_file)))
        _candidates.append(os.path.join(_base, 'pyStrategy', 'demo', 'ali2026v3_trading', 'config'))
    for _d in _candidates:
        _resolved = os.path.normpath(_d)
        if os.path.isdir(_resolved) and os.path.exists(os.path.join(_resolved, 'subscription_futures_fixed.txt')):
            return _resolved
    return None


def _rank_option_candidates_by_duckdb_activity(option_candidates: List[Tuple[str, Dict]]) -> List[Tuple[str, Dict]]:
    if not option_candidates:
        return option_candidates
    try:
        import duckdb
        db_path = os.path.normpath(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'strategy.duckdb')
        )
        if not os.path.exists(db_path):
            return option_candidates
        candidate_set = {inst_id for inst_id, _ in option_candidates}
        activity = {}
        with duckdb.connect(db_path) as con:
            rows = con.execute("""
                SELECT instrument_id, COUNT(*) AS cnt, MAX(timestamp) AS max_ts
                FROM ticks_raw
                WHERE regexp_matches(instrument_id, '^[A-Za-z]+[0-9]{3,4}[-]?[CP][-]?[0-9]')
                GROUP BY instrument_id
                ORDER BY max_ts DESC NULLS LAST, cnt DESC
                LIMIT 5000
            """).fetchall()
        for rank, (inst_id, cnt, max_ts) in enumerate(rows):
            if inst_id in candidate_set:
                try:
                    max_ts_value = max_ts.timestamp() if max_ts is not None and hasattr(max_ts, 'timestamp') else 0.0
                except (ValueError, TypeError, AttributeError, OSError):
                    max_ts_value = 0.0
                activity[inst_id] = (0, -max_ts_value, -int(cnt or 0), rank)
        return sorted(
            option_candidates,
            key=lambda item: activity.get(item[0], (1, 0.0, 0, 0)),
            reverse=False,
        )
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError, OSError) as e:
        logging.debug("[%s] 活跃期权诊断清单排序失败，回退订阅文件顺序: %s", MONITORED_DIAG_LABEL if 'MONITORED_DIAG_LABEL' in globals() else '合约诊断', e)
        return option_candidates


def _load_tick_activity_by_instrument() -> Dict[str, int]:
    try:
        import duckdb
        db_path = os.path.normpath(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'strategy.duckdb')
        )
        if not os.path.exists(db_path):
            return {}
        with duckdb.connect(db_path) as con:
            rows = con.execute("""
                SELECT instrument_id, COUNT(*) AS cnt
                FROM ticks_raw
                WHERE COALESCE(sync_status, '') <> 'simulated_coverage'
                GROUP BY instrument_id
            """).fetchall()
        return {inst_id: int(cnt or 0) for inst_id, cnt in rows}
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError, OSError) as e:
        logging.debug("[%s] tick活跃度读取失败: %s", MONITORED_DIAG_LABEL if 'MONITORED_DIAG_LABEL' in globals() else '合约诊断', e)
        return {}


def _load_full_chain_entry_counts() -> Dict[str, int]:
    try:
        import duckdb
        db_path = os.path.normpath(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'strategy.duckdb')
        )
        if not os.path.exists(db_path):
            return {}
        with duckdb.connect(db_path) as con:
            row = con.execute("""
                SELECT
                    (SELECT COUNT(*) FROM instruments_registry),
                    (SELECT COUNT(*) FROM futures_instruments),
                    (SELECT COUNT(*) FROM option_instruments),
                    (SELECT COUNT(DISTINCT instrument_id) FROM ticks_raw WHERE date = CURRENT_DATE),
                    (SELECT COUNT(DISTINCT instrument_id) FROM ticks_raw WHERE date = CURRENT_DATE AND COALESCE(sync_status, '') <> 'simulated_coverage'),
                    (SELECT COUNT(DISTINCT instrument_id) FROM ticks_raw WHERE date = CURRENT_DATE AND sync_status = 'simulated_coverage'),
                    (SELECT COUNT(DISTINCT internal_id) FROM klines_raw WHERE trade_date = CURRENT_DATE),
                    (SELECT COUNT(*) FROM chain_coverage_audit)
            """).fetchone()
            latest = con.execute("""
                SELECT config_count, subscription_confirmed_count, tick_return_count,
                       tick_buffer_count, ticks_raw_count, klines_raw_count,
                       simulated_coverage_tick_count, simulated_coverage_kline_count
                FROM chain_coverage_audit
                ORDER BY audit_time DESC
                LIMIT 1
            """).fetchone()
        result = {
            'registry_count': int(row[0] or 0),
            'future_count': int(row[1] or 0),
            'option_count': int(row[2] or 0),
            'ticks_raw_today_count': int(row[3] or 0),
            'real_tick_today_count': int(row[4] or 0),
            'simulated_tick_today_count': int(row[5] or 0),
            'klines_raw_today_count': int(row[6] or 0),
            'audit_rows': int(row[7] or 0),
        }
        if latest:
            result.update({
                'audit_config_count': int(latest[0] or 0),
                'audit_subscription_confirmed_count': int(latest[1] or 0),
                'audit_tick_return_count': int(latest[2] or 0),
                'audit_tick_buffer_count': int(latest[3] or 0),
                'audit_ticks_raw_count': int(latest[4] or 0),
                'audit_klines_raw_count': int(latest[5] or 0),
                'audit_simulated_tick_count': int(latest[6] or 0),
                'audit_simulated_kline_count': int(latest[7] or 0),
            })
        return result
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError, OSError) as e:
        logging.debug("[%s] 全链路入口统计读取失败: %s", MONITORED_DIAG_LABEL if 'MONITORED_DIAG_LABEL' in globals() else '合约诊断', e)
        return {}


def _log_full_chain_entry_counts(runtime_subscribed_count: int) -> None:
    counts = _load_full_chain_entry_counts()
    if not counts:
        logging.info("[%s] 全链路入口统计: 暂无可用DB统计，runtime_subscribed=%d", MONITORED_DIAG_LABEL, runtime_subscribed_count)
        return
    expected = counts.get('audit_config_count') or counts.get('registry_count') or 0
    logging.info(
        "[%s] 全链路入口统计: config=%d, registry=%d, futures=%d, options=%d, runtime_subscribed=%d, "
        "ticks_raw_today=%d(real=%d, simulated=%d), klines_raw_today=%d",
        MONITORED_DIAG_LABEL,
        expected,
        counts.get('registry_count', 0),
        counts.get('future_count', 0),
        counts.get('option_count', 0),
        runtime_subscribed_count,
        counts.get('ticks_raw_today_count', 0),
        counts.get('real_tick_today_count', 0),
        counts.get('simulated_tick_today_count', 0),
        counts.get('klines_raw_today_count', 0),
    )
    if counts.get('audit_rows', 0):
        logging.info(
            "[%s] 最新覆盖审计: config=%d -> subscribe=%d -> tick_return=%d -> tick_buffer=%d -> "
            "ticks_raw=%d -> klines_raw=%d, simulated_tick=%d, simulated_kline=%d",
            MONITORED_DIAG_LABEL,
            counts.get('audit_config_count', 0),
            counts.get('audit_subscription_confirmed_count', 0),
            counts.get('audit_tick_return_count', 0),
            counts.get('audit_tick_buffer_count', 0),
            counts.get('audit_ticks_raw_count', 0),
            counts.get('audit_klines_raw_count', 0),
            counts.get('audit_simulated_tick_count', 0),
            counts.get('audit_simulated_kline_count', 0),
        )


def _load_monitored_contracts_from_subscription_files() -> Optional[Dict[str, Dict]]:
    try:
        config_dir = _find_config_dir()
        if not config_dir:
            return None
        futures_path = os.path.join(config_dir, 'subscription_futures_fixed.txt')
        options_path = os.path.join(config_dir, 'subscription_options_fixed.txt')
        if not os.path.exists(futures_path):
            return None
        result = {}
        _EXCHANGE_MAP = {
            'CFFEX': 'CFFEX', 'SHFE': 'SHFE', 'DCE': 'DCE', 'CZCE': 'CZCE', 'GFEX': 'GFEX', 'INE': 'INE',
        }
        future_candidates = []
        option_candidates = []
        with open(futures_path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 4:
                    exchange = parts[0].split('.')[0] if '.' in parts[0] else ''
                    inst_id = parts[0].split('.')[-1] if '.' in parts[0] else parts[0]
                    future_candidates.append((
                        inst_id,
                        {'exchange': _EXCHANGE_MAP.get(exchange, exchange), 'name': f'{parts[2]}期货', 'type': 'future'},
                    ))
        if os.path.exists(options_path):
            with open(options_path, 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.strip().split('|')
                    if len(parts) >= 7:
                        symbol = parts[0]
                        exchange = symbol.split('.')[0] if '.' in symbol else ''
                        inst_id = symbol.split('.')[-1] if '.' in symbol else symbol
                        underlying = parts[4] if len(parts) > 4 else ''
                        option_candidates.append((
                            inst_id,
                            {'underlying': underlying, 'exchange': _EXCHANGE_MAP.get(exchange, exchange), 'name': f'{inst_id}', 'type': 'option'},
                        ))
        activity = _load_tick_activity_by_instrument()

        def append_candidate(candidate):
            inst_id, info = candidate
            if inst_id not in result and len(result) < 20:
                result[inst_id] = info

        for exchange in sorted({info.get('exchange') for _, info in future_candidates if info.get('exchange')}):
            match = next((item for item in future_candidates if item[1].get('exchange') == exchange), None)
            if match:
                append_candidate(match)

        for candidates, has_tick in ((future_candidates, True), (future_candidates, False), (option_candidates, True), (option_candidates, False)):
            match = next((item for item in candidates if bool(activity.get(item[0], 0)) is has_tick), None)
            if match:
                append_candidate(match)

        for inst_id, info in _rank_option_candidates_by_duckdb_activity(option_candidates):
            append_candidate((inst_id, info))
            if len(result) >= 20:
                break

        for candidate in future_candidates:
            append_candidate(candidate)
            if len(result) >= 20:
                break
        if result:
            return result
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
        pass
    return None


def _load_monitored_contracts_from_config() -> Dict[str, Dict]:
    try:
        env_contracts = os.getenv('MONITORED_CONTRACTS_LIST', '').strip()
        if env_contracts:
            import json
            custom = json.loads(env_contracts)
            if isinstance(custom, dict) and custom:
                return custom
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
        pass
    _from_files = _load_monitored_contracts_from_subscription_files()
    if _from_files:
        return _from_files
    return _DEFAULT_MONITORED_CONTRACTS


MONITORED_CONTRACTS = _load_monitored_contracts_from_config()

MONITORED_CONTRACT_SET = set(MONITORED_CONTRACTS.keys())
MONITORED_CONTRACT_COUNT = len(MONITORED_CONTRACTS)
MONITORED_DIAG_LABEL = f'{MONITORED_CONTRACT_COUNT}合约诊断'
_PARSE_TRACE_KEYS: deque = deque(maxlen=10000)
_PARSE_TRACE_LOCK = threading.RLock()

_is_monitored_cache_result: Optional[bool] = None
_is_monitored_cache_time: float = 0.0
_IS_MONITORED_CACHE_TTL = 1.0
_is_monitored_cache_lock = threading.Lock()

_get_cached_params_fn = None
_get_cached_params_lock = threading.Lock()


def _get_config_cached_params():
    global _get_cached_params_fn
    if _get_cached_params_fn is None:
        with _get_cached_params_lock:
            if _get_cached_params_fn is None:
                try:
                    from ali2026v3_trading.config.config_service import get_cached_params
                    _get_cached_params_fn = get_cached_params
                except ImportError:
                    _get_cached_params_fn = lambda: None
                    logging.debug("is_monitored_contract: config_service not available, diagnostics disabled")
    return _get_cached_params_fn()


def is_monitored_contract(instrument_id: str) -> bool:
    global _is_monitored_cache_result, _is_monitored_cache_time
    now = time.monotonic()
    with _is_monitored_cache_lock:
        if _is_monitored_cache_result is not None and (now - _is_monitored_cache_time) < _IS_MONITORED_CACHE_TTL:
            return _is_monitored_cache_result
    try:
        cached_params = _get_config_cached_params()
        if cached_params and isinstance(cached_params, dict):
            strategy = cached_params.get('strategy')
            if strategy:
                params = getattr(strategy, 'params', None)
                if params:
                    result = bool(getattr(params, 'diagnostic_output', False))
                    with _is_monitored_cache_lock:
                        _is_monitored_cache_result = result
                        _is_monitored_cache_time = now
                    return result
        with _is_monitored_cache_lock:
            _is_monitored_cache_result = False
            _is_monitored_cache_time = now
        return False
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.debug(f"is_monitored_contract check failed: {e}")
        with _is_monitored_cache_lock:
            _is_monitored_cache_result = False
            _is_monitored_cache_time = now
        return False


def get_contract_info(instrument_id: str) -> Optional[Dict[str, Any]]:
    return MONITORED_CONTRACTS.get(instrument_id)


def _should_trace_parse_contract(original_id: str, transformed_id: Optional[str] = None) -> bool:
    if original_id in MONITORED_CONTRACT_SET or transformed_id in MONITORED_CONTRACT_SET:
        return True
    return bool(original_id and transformed_id and original_id != transformed_id)


def diagnose_parse_transform(stage: str, original_id: str, transformed_id: str,
                             detail: Optional[str] = None, level: str = "INFO") -> None:
    original = str(original_id or '').strip()
    transformed = str(transformed_id or '').strip()
    detail_text = str(detail).strip() if detail else None
    if not _should_trace_parse_contract(original, transformed):
        return
    trace_key = (stage, original, transformed, detail_text)
    with _PARSE_TRACE_LOCK:
        if trace_key in _PARSE_TRACE_KEYS:
            return
        _PARSE_TRACE_KEYS.append(trace_key)
    message = f"[PROBE_PARSE_TRANSFORM] Stage={stage} | Original={original} | Transformed={transformed}"
    if detail_text:
        message += f" | Detail={detail_text}"
    log_func = {
        'DEBUG': logging.debug, 'INFO': logging.info, 'WARN': logging.warning,
        'WARNING': logging.warning, 'ERROR': logging.error,
    }.get(str(level or 'INFO').upper(), logging.info)
    log_func(message)


def diagnose_parse_failure(stage: str, instrument_id: str, reason: str) -> None:
    instrument = str(instrument_id or '').strip()
    if instrument not in MONITORED_CONTRACT_SET:
        return
    logging.error("[PROBE_PARSE_FAILURE] Stage=%s | Instrument=%s | Reason=%s", stage, instrument, reason)


# 五唯一性修复：合约校验正则与 subscription_service.parse_future/parse_option 规范正则兼容
# 原 {2,4} 改为 +（允许单字母品种如 m/c/al）；原 \d{4} 改为 \d{3,4}（允许 CZCE 三位月份）；
# 原 \d+ 改为 \d+(?:\.\d+)?（允许小数行权价）
_CONTRACT_PATTERN = re.compile(
    r'^([A-Za-z]+)(\d{3,4})'
    r'(?:-([CP])-(\d+(?:\.\d+)?)|([CP])(\d+(?:\.\d+)?)?)?$'
)


def validate_contract_format(instrument_id: str) -> Tuple[bool, str]:  # [R22-P2-TS21]
    info = MONITORED_CONTRACTS.get(instrument_id)
    if not info:
        return False, "未知合约", instrument_id
    contract_type = info['type']
    if contract_type == 'future':
        match = _CONTRACT_PATTERN.match(instrument_id)
        expected = "期货标准格式: XX####"
        if match and not match.group(3) and not match.group(5):
            return True, expected, instrument_id
        return False, expected, instrument_id
    if contract_type == 'option':
        underlying = info.get('underlying', '')
        exchange = info.get('exchange', '')
        try:
            from ali2026v3_trading.infra.subscription_service import SubscriptionManager
            parsed = SubscriptionManager.parse_option(instrument_id)
            if parsed:
                if exchange == 'CFFEX':
                    prefix = parsed.get('product', '')
                    if underlying.startswith('IH') and prefix != 'HO':
                        return False, "股指期权应使用HO代码（IH→HO）", instrument_id
                    if underlying.startswith('IF') and prefix != 'IO':
                        return False, "股指期权应使用IO代码（IF→IO）", instrument_id
                    if underlying.startswith('IM') and prefix != 'MO':
                        return False, "股指期权应使用MO代码（IM→MO）", instrument_id
                    return True, "股指期权标准格式: XX####-C/P-#####", instrument_id
                return True, "商品期权标准格式: xx####C/P#####", instrument_id
        except (ValueError, KeyError, TypeError) as _parse_err:
            logging.debug("[R22-EP-05-残留] SubscriptionManager.parse_option解析异常: %s", _parse_err)
        if exchange == 'CFFEX':
            return False, "股指期权标准格式: XX####-C/P-#####", instrument_id
        return False, "商品期权标准格式: xx####C/P#####", instrument_id
    return False, "未知类型", instrument_id


def log_diagnostic_event(probe_name: str, instrument_id: str, **kwargs) -> None:
    if not is_monitored_contract(instrument_id):
        return
    info = get_contract_info(instrument_id)
    contract_type = info['type'] if info else 'unknown'
    parts = [f"[{probe_name}] Contract={instrument_id} | Type={contract_type}"]
    for key, value in kwargs.items():
        parts.append(f"{key}={value}")
    message = " | ".join(parts)
    if 'FAILED' in probe_name or 'ERROR' in probe_name:
        logging.error(message)
    elif 'WARNING' in probe_name:
        logging.warning(message)
    else:
        logging.info(message)


def diagnose_format_validation(instrument_id: str, is_valid_format: bool,
                              expected_format: Optional[str] = None,
                              actual_format: Optional[str] = None) -> None:
    if not is_monitored_contract(instrument_id):
        return
    status = "✅" if is_valid_format else "❌"
    if is_valid_format:
        logging.debug(f"[PROBE_FORMAT_VALIDATION] {instrument_id} | Format={status}")
    else:
        msg = f"[PROBE_FORMAT_VALIDATION_FAILED] {instrument_id} | Format={status}"
        if expected_format and actual_format:
            msg += f" | Expected={expected_format} | Actual={actual_format}"
        logging.error(msg)


def diagnose_config_file_presence(instrument_id: str, exists_in_file: bool) -> None:
    if not is_monitored_contract(instrument_id):
        return
    status = "✅" if exists_in_file else "❌"
    logging.info(f"[PROBE_CONFIG_FILE] {instrument_id} | InFile={status}")


def diagnose_config_loaded_status(instrument_id: str, loaded_in_memory: bool,
                                 in_cache: bool = False) -> None:
    if not is_monitored_contract(instrument_id):
        return
    mem_status = "✅" if loaded_in_memory else "❌"
    cache_status = "✅" if in_cache else "❌"
    logging.info(f"[PROBE_CONFIG_LOADED] {instrument_id} | InMemory={mem_status} | InCache={cache_status}")


def diagnose_pre_registration(instrument_id: str, in_subscribe_list: bool,
                              registered: bool) -> None:
    if not is_monitored_contract(instrument_id):
        return
    sub_status = "✅" if in_subscribe_list else "❌"
    reg_status = "✅" if registered else "❌"
    logging.info(f"[PROBE_PRE_REGISTRATION] {instrument_id} | InSubList={sub_status} | Registered={reg_status}")


def diagnose_subscription(instrument_id: str, contract_type: str, success: bool, reason: str = None) -> None:
    if not is_monitored_contract(instrument_id):
        return
    status = "✅" if success else "❌"
    msg = f"[PROBE_SUBSCRIPTION] {instrument_id} | Type={contract_type} | Success={status}"
    if reason:
        msg += f" | Reason={reason}"
    if success:
        logging.debug(msg)
    else:
        logging.warning(msg)


def diagnose_registration(instrument_id: str, source: str, internal_id: int, instrument_type: str) -> None:
    if not is_monitored_contract(instrument_id):
        return
    logging.debug(f"[PROBE_REGISTRATION] {instrument_id} | Source={source} | ID={internal_id} | Type={instrument_type}")


def diagnose_id_lookup(instrument_id: str, internal_id: int, instrument_type: str) -> None:
    if not is_monitored_contract(instrument_id):
        return
    logging.debug(f"[PROBE_ID_LOOKUP] {instrument_id} | ID={internal_id} | Type={instrument_type}")


_TICK_ENTRY_SUMMARY_INTERVAL = 60.0
_tick_entry_last_output_time = 0.0
_tick_entry_accum = {}
_tick_entry_accum_lock = threading.Lock()


def diagnose_tick_entry(instrument_id: str, price: float, volume: int,
                        open_interest: float, contract_type: str) -> None:
    global _tick_entry_last_output_time
    if not is_monitored_contract(instrument_id):
        return
    now = time.time()
    with _tick_entry_accum_lock:
        if instrument_id not in _tick_entry_accum:
            _tick_entry_accum[instrument_id] = {'count': 0, 'last_price': None, 'last_oi': None}
        _tick_entry_accum[instrument_id]['count'] += 1
        _tick_entry_accum[instrument_id]['last_price'] = price
        _tick_entry_accum[instrument_id]['last_oi'] = open_interest
        if now - _tick_entry_last_output_time >= _TICK_ENTRY_SUMMARY_INTERVAL:
            for iid, accum in _tick_entry_accum.items():
                logging.info(
                    "[PROBE_TICK_ENTRY] %s | Count=%d | LastPrice=%s | LastOI=%s | Type=%s",
                    iid, accum['count'], accum['last_price'], accum['last_oi'], contract_type,
                )
            _tick_entry_accum.clear()
            _tick_entry_last_output_time = now


def diagnose_tick_buffer(instrument_id: str, buffered: bool) -> None:
    if not is_monitored_contract(instrument_id):
        return
    logging.debug(f"[PROBE_TICK_BUFFER] {instrument_id} | Buffered={'✅' if buffered else '❌'}")


def diagnose_tick_flush(instrument_id: str, flushed: bool) -> None:
    if not is_monitored_contract(instrument_id):
        return
    logging.debug(f"[PROBE_TICK_FLUSH] {instrument_id} | Flushed={'✅' if flushed else '❌'}")


def diagnose_storage_enqueue(instrument_id: str, success: bool, reason: str = None) -> None:
    if not is_monitored_contract(instrument_id):
        return
    status = "✅" if success else "❌"
    msg = f"[PROBE_STORAGE_ENQUEUE] {instrument_id} | Success={status}"
    if reason:
        msg += f" | Reason={reason}"
    logging.debug(msg)


def diagnose_async_write(instrument_id: str, func_name: str, data_count: int) -> None:
    if not is_monitored_contract(instrument_id):
        return
    logging.debug(f"[PROBE_ASYNC_WRITE] {instrument_id} | Func={func_name} | Count={data_count}")


def diagnose_duckdb_insert(instrument_id: str, success: bool, reason: str = None) -> None:
    if not is_monitored_contract(instrument_id):
        return
    status = "✅" if success else "❌"
    msg = f"[PROBE_DUCKDB_INSERT] {instrument_id} | Success={status}"
    if reason:
        msg += f" | Reason={reason}"
    logging.debug(msg)


_reg_option_result_counts = {'success': 0, 'none': 0, 'error': 0}
_reg_option_result_lock = threading.Lock()
_reg_option_result_last_log = 0.0


def diagnose_register_option_result(instrument_id: str, result, error: str = None) -> None:
    global _reg_option_result_last_log
    now = time.time()
    with _reg_option_result_lock:
        if result is True:
            _reg_option_result_counts['success'] += 1
        elif result is None:
            _reg_option_result_counts['none'] += 1
        else:
            _reg_option_result_counts['error'] += 1
        if now - _reg_option_result_last_log >= 30.0:
            logging.info(
                "[PROBE_REG_OPTION] success=%d none=%d error=%d",
                _reg_option_result_counts['success'],
                _reg_option_result_counts['none'],
                _reg_option_result_counts['error'],
            )
            _reg_option_result_counts.clear()
            _reg_option_result_counts.update({'success': 0, 'none': 0, 'error': 0})
            _reg_option_result_last_log = now


class DiagnosisProbeManager:
    """12环节诊断探针统一管理器"""

    _shard_enqueue_lock = threading.Lock()

    def __init__(self):
        self._shard_enqueue_counts: Dict[int, int] = {}
        # R21-NET-P1-05修复: 上线监控回调 — 诊断探针可注册回调，在检测到异常时主动通知上层
        self._online_monitor_callbacks: List[Any] = []  # List[Callable[[str, Dict], None]]

    # R21-NET-P1-05修复: 注册上线监控回调
    def register_online_monitor_callback(self, callback):
        """注册上线监控回调函数，签名: callback(event_type: str, detail: Dict)"""
        if callable(callback) and callback not in self._online_monitor_callbacks:
            self._online_monitor_callbacks.append(callback)

    # R21-NET-P1-05修复: 触发上线监控回调
    def _fire_online_monitor(self, event_type: str, detail: Dict):
        for _cb in self._online_monitor_callbacks:
            try:
                _cb(event_type, detail)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
                logging.debug("[R21-NET-P1-05修复] 上线监控回调异常: %s", _e)

    @staticmethod
    def on_subscribe(instrument_id: str, contract_type: str, success: bool, reason: str = None):
        diagnose_subscription(instrument_id, contract_type, success, reason)

    @staticmethod
    def on_register(instrument_id: str, source: str, internal_id: int, instrument_type: str):
        diagnose_registration(instrument_id, source, internal_id, instrument_type)
        diagnose_id_lookup(instrument_id, internal_id, instrument_type)

    @staticmethod
    def on_tick_entry(instrument_id: str, price: float, volume: int, open_interest: float, contract_type: str):
        diagnose_tick_entry(instrument_id, price, volume, open_interest, contract_type)
        DiagnosisProbeManager.record_contract_tick(instrument_id, price, volume, open_interest, contract_type)

    def on_storage_enqueue(self, instrument_id: str, success: bool, reason: str = None, shard_idx: int = -1):
        diagnose_storage_enqueue(instrument_id, success, reason)
        if shard_idx >= 0:
            with DiagnosisProbeManager._shard_enqueue_lock:
                self._shard_enqueue_counts[shard_idx] = self._shard_enqueue_counts.get(shard_idx, 0) + 1

    @staticmethod
    def on_async_write(instrument_id: str, func_name: str, data_count: int):
        diagnose_async_write(instrument_id, func_name, data_count)

    @staticmethod
    def on_parse_transform(stage: str, original_id: str, transformed_id: str,
                           detail: str = None, level: str = 'INFO'):
        diagnose_parse_transform(stage, original_id, transformed_id, detail, level)

    @staticmethod
    def on_parse_failure(stage: str, instrument_id: str, reason: str):
        diagnose_parse_failure(stage, instrument_id, reason)

    _startup_lock = threading.RLock()
    _startup_seq = 0
    _startup_run: Optional[Dict[str, Any]] = None
    _contract_watch_lock = threading.RLock()
    _contract_watch_run: Optional[Dict[str, Any]] = None
    _contract_watch_thread: Optional[threading.Thread] = None
    _contract_watch_stop = threading.Event()

    @classmethod
    def start_contract_watch(cls, subscribed_instruments=None, timeout_seconds=30.0,
                             summary_interval_seconds=10.0, label=MONITORED_DIAG_LABEL):
        watch_targets = []
        subscribed_set = set(str(item).strip() for item in (subscribed_instruments or []) if str(item).strip())
        for instrument_id, info in MONITORED_CONTRACTS.items():
            watch_targets.append({
                'instrument_id': instrument_id, 'exchange': info.get('exchange', ''),
                'type': info.get('type', 'unknown'), 'underlying': info.get('underlying', ''),
                'name': info.get('name', instrument_id), 'in_subscribe_list': instrument_id in subscribed_set,
                'watch_started_at': time.time(), 'first_tick_at': None, 'last_tick_at': None,
                'first_price': None, 'last_price': None, 'tick_count': 0,
            })
        run = {
            'run_id': f"contract-watch-{int(time.time())}", 'label': label,
            'started_at': time.time(), 'started_wall': datetime.now(CHINA_TZ).isoformat(),
            'timeout_seconds': float(timeout_seconds),
            'summary_interval_seconds': max(1.0, float(summary_interval_seconds)),
            'contracts': {item['instrument_id']: item for item in watch_targets},
            'finished': False,
        }
        with cls._contract_watch_lock:
            cls._contract_watch_run = run
            cls._contract_watch_stop.clear()
        logging.info("=" * 80)
        logging.info("[ContractWatch] 启动 %s | run=%s | total=%d | in_subscribe_list=%d | timeout=%.1fs | interval=%.1fs",
                     label, run['run_id'], len(run['contracts']),
                     sum(1 for item in watch_targets if item['in_subscribe_list']),
                     run['timeout_seconds'], run['summary_interval_seconds'])
        for item in watch_targets:
            logging.info("[ContractWatch] target=%s | type=%s | exchange=%s | underlying=%s | in_subscribe_list=%s | name=%s",
                         item['instrument_id'], item['type'], item['exchange'],
                         item['underlying'] or '-', 'Y' if item['in_subscribe_list'] else 'N', item['name'])
        logging.info("=" * 80)

        def _watch_loop():
            last_emit = 0.0
            while not cls._contract_watch_stop.wait(1.0):
                current_run = cls._get_contract_watch_run()
                if current_run is None:
                    return
                now = time.time()
                if now - last_emit >= current_run['summary_interval_seconds']:
                    cls.emit_contract_watch_summary(reason='periodic')
                    last_emit = now
                if now - current_run['started_at'] >= current_run['timeout_seconds']:
                    cls.emit_contract_watch_summary(reason='timeout', final=True)
                    return

        thread = threading.Thread(target=_watch_loop, name='ContractWatchSummary', daemon=True)
        with cls._contract_watch_lock:
            cls._contract_watch_thread = thread
        thread.start()
        logging.info("[ContractWatch] START_CONFIRMED run=%s thread=%s alive=%s timeout=%.1fs interval=%.1fs subscribed_targets=%d total_targets=%d",
                     run['run_id'], thread.name, thread.is_alive(), run['timeout_seconds'],
                     run['summary_interval_seconds'],
                     sum(1 for item in watch_targets if item['in_subscribe_list']), len(watch_targets))
        return run

    @classmethod
    def _get_contract_watch_run(cls):
        with cls._contract_watch_lock:
            return cls._contract_watch_run

    @classmethod
    def record_contract_tick(cls, instrument_id, price, volume, open_interest, contract_type):
        instrument = str(instrument_id or '').strip()
        if not instrument:
            return
        with cls._contract_watch_lock:
            run = cls._contract_watch_run
            if run is None or run.get('finished'):
                return
            state = run['contracts'].get(instrument)
            if state is None:
                return
            now = time.time()
            first_tick = state['first_tick_at'] is None
            state['tick_count'] += 1
            state['last_tick_at'] = now
            state['last_price'] = price
            if first_tick:
                state['first_tick_at'] = now
                state['first_price'] = price
        if first_tick:
            elapsed = now - run['started_at']
            logging.info("[ContractWatch] FIRST_TICK run=%s contract=%s type=%s price=%s vol=%s oi=%s elapsed=%.3fs",
                         run['run_id'], instrument, contract_type, price, volume, open_interest, elapsed)

    @classmethod
    def emit_contract_watch_summary(cls, reason: str, final: bool = False):
        with cls._contract_watch_lock:
            run = cls._contract_watch_run
            if run is None:
                return
            now = time.time()
            total = len(run['contracts'])
            in_subscribe = [item for item in run['contracts'].values() if item['in_subscribe_list']]
            first_tick = [item for item in run['contracts'].values() if item['first_tick_at'] is not None]
            no_tick = [item for item in run['contracts'].values() if item['in_subscribe_list'] and item['first_tick_at'] is None]
            not_subscribed = [item for item in run['contracts'].values() if not item['in_subscribe_list']]
            elapsed = now - run['started_at']
            if final:
                run['finished'] = True
                cls._contract_watch_stop.set()
        logging.info("-" * 80)
        logging.info("[ContractWatch] summary run=%s reason=%s final=%s elapsed=%.3fs total=%d in_subscribe=%d first_tick=%d no_tick=%d not_subscribed=%d",
                     run['run_id'], reason, final, elapsed, total, len(in_subscribe), len(first_tick), len(no_tick), len(not_subscribed))
        for item in sorted(first_tick, key=lambda c: c['first_tick_at'] or 0.0):
            first_elapsed = (item['first_tick_at'] or now) - run['started_at']
            last_elapsed = (item['last_tick_at'] or now) - run['started_at']
            logging.info("[ContractWatch] OK contract=%s type=%s ticks=%d first=%.3fs last=%.3fs first_price=%s last_price=%s",
                         item['instrument_id'], item['type'], item['tick_count'], first_elapsed, last_elapsed, item['first_price'], item['last_price'])
        for item in no_tick:
            # FIX-R37-CONTRACT-NOISE: 不活跃合约无tick是预期行为，降为DEBUG避免噪音；summary已记录WARNING
            logging.debug("[ContractWatch] NO_TICK contract=%s type=%s waited=%.3fs name=%s",
                            item['instrument_id'], item['type'], elapsed, item['name'])
        # R21-NET-P1-05修复: 检测到无tick合约时触发上线监控回调
        if no_tick:
            try:
                for _cb in cls._online_monitor_callbacks:
                    try:
                        _cb('contract_no_tick', {
                            'run_id': run.get('run_id', ''),
                            'no_tick_count': len(no_tick),
                            'no_tick_instruments': [item['instrument_id'] for item in no_tick],
                            'elapsed': elapsed,
                        })
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
                        logging.debug("[R21-NET-P1-05修复] 上线监控回调异常: %s", _e)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _fire_err:
                logging.debug("[R22-EP-05] 上线监控回调触发失败: %s", _fire_err)
        for item in not_subscribed:
            logging.warning("[ContractWatch] NOT_IN_SUBSCRIBE_LIST contract=%s type=%s name=%s",
                            item['instrument_id'], item['type'], item['name'])
        logging.info("-" * 80)

    @classmethod
    def stop_contract_watch(cls, reason: str = 'stop'):
        run = cls._get_contract_watch_run()
        if run is None:
            logging.warning("[ContractWatch] STOP_REQUEST reason=%s but no active run", reason)
            return
        logging.info("[ContractWatch] STOP_REQUEST run=%s reason=%s finished=%s",
                     run.get('run_id', '-'), reason, run.get('finished', False))
        cls.emit_contract_watch_summary(reason=reason, final=True)

    @classmethod
    def begin_startup_timeline(cls, source: str, strategy_id: str = None, reset: bool = False) -> str:
        with cls._startup_lock:
            run = cls._startup_run
            if reset or run is None or run.get('finished'):
                cls._startup_seq += 1
                run = {'run_id': f"startup-{cls._startup_seq}", 'source': source,
                       'strategy_id': strategy_id or '', 'started_at': time.perf_counter(),
                       'started_wall': datetime.now(CHINA_TZ).isoformat(), 'spans': [], 'events': [], 'finished': False}
                cls._startup_run = run
                logging.info("[StartupProbe] begin run=%s source=%s strategy_id=%s", run['run_id'], source, strategy_id or '-')
            else:
                logging.info("[StartupProbe] resume run=%s source=%s strategy_id=%s", run['run_id'], source, run.get('strategy_id') or '-')
        cls.mark_startup_event(f"timeline.begin:{source}")
        return run['run_id']

    @classmethod
    def _require_startup_run(cls):
        with cls._startup_lock:
            return cls._startup_run

    @classmethod
    def mark_startup_event(cls, name: str, detail: str = None):
        run = cls._require_startup_run()
        if run is None:
            return
        elapsed = time.perf_counter() - run['started_at']
        event = {'name': name, 'detail': detail or '', 'elapsed': elapsed, 'wall_time': datetime.now(CHINA_TZ).isoformat()}
        with cls._startup_lock:
            current = cls._startup_run
            if current is None:
                return
            current['events'].append(event)
        if detail:
            logging.info("[StartupProbe] event run=%s t=%.3fs name=%s detail=%s", run['run_id'], elapsed, name, detail)
        else:
            logging.info("[StartupProbe] event run=%s t=%.3fs name=%s", run['run_id'], elapsed, name)

    # R21-MEM-P2-17修复: startup_step()为@contextmanager生成器，
    # with语句保证GeneratorExit触发finally块执行，自动调用生成器.close()。
    # 当前实现已有try/finally保护，确保step计时和状态记录在异常时也能完成。
    @classmethod
    @contextmanager
    def startup_step(cls, name: str, detail: str = None):
        run = cls._require_startup_run()
        if run is None:
            yield
            return
        step_start = time.perf_counter()
        exc = None
        logging.info("[StartupProbe] step_begin run=%s step=%s detail=%s", run['run_id'], name, detail or '-')
        try:
            yield
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as error:
            exc = error
            raise
        finally:
            elapsed = time.perf_counter() - step_start
            total_elapsed = time.perf_counter() - run['started_at']
            span = {'name': name, 'detail': detail or '', 'elapsed': elapsed,
                    'status': 'error' if exc is not None else 'ok',
                    'error': str(exc) if exc is not None else '',
                    'finished_at': datetime.now(CHINA_TZ).isoformat(), 'total_elapsed': total_elapsed}
            with cls._startup_lock:
                current = cls._startup_run
                if current is not None:
                    current['spans'].append(span)
            logging.info("[StartupProbe] step_end run=%s step=%s status=%s elapsed=%.3fs total=%.3fs",
                         run['run_id'], name, span['status'], elapsed, total_elapsed)

    @classmethod
    def emit_startup_summary(cls, reason: str, final: bool = False, top_n: int = 15):
        with cls._startup_lock:
            run = cls._startup_run
            if run is None:
                return
            total_elapsed = time.perf_counter() - run['started_at']
            spans = list(run['spans'])
            events = list(run['events'])
            aggregate = {}
            for span in spans:
                bucket = aggregate.setdefault(span['name'], {'count': 0, 'total': 0.0, 'max': 0.0, 'errors': 0})
                bucket['count'] += 1
                bucket['total'] += float(span['elapsed'])
                bucket['max'] = max(bucket['max'], float(span['elapsed']))
                if span['status'] != 'ok':
                    bucket['errors'] += 1
            sorted_steps = sorted(aggregate.items(), key=lambda item: item[1]['total'], reverse=True)[:top_n]
            recent_events = events[-8:]
            if final:
                run['finished'] = True
        logging.info("[StartupProbe] summary run=%s reason=%s total=%.3fs spans=%d events=%d final=%s",
                     run['run_id'], reason, total_elapsed, len(spans), len(events), final)
        for index, (step_name, stats) in enumerate(sorted_steps, start=1):
            avg = stats['total'] / stats['count'] if stats['count'] else 0.0
            logging.info("[StartupProbe] top%d step=%s total=%.3fs avg=%.3fs max=%.3fs count=%d errors=%d",
                         index, step_name, stats['total'], avg, stats['max'], stats['count'], stats['errors'])
        for event in recent_events:
            if event['detail']:
                logging.info("[StartupProbe] recent_event t=%.3fs name=%s detail=%s", event['elapsed'], event['name'], event['detail'])
            else:
                logging.info("[StartupProbe] recent_event t=%.3fs name=%s", event['elapsed'], event['name'])

    @staticmethod
    def diagnose_subscribe_api_return(strategy_core, test_contracts=None):
        if test_contracts is None:
            test_contracts = [
                ('SHFE', 'al2607C25200', 'AL期权-有tick'), ('SHFE', 'au2607C1032', 'AU期权-无tick'),
                ('SHFE', 'cu2607C104000', 'CU期权-无tick'), ('SHFE', 'zn2607C20000', 'ZN期权-无tick'),
            ]
        results = {'timestamp': datetime.now(CHINA_TZ).isoformat(), 'contracts': {},
                   'summary': {'total': 0, 'success': 0, 'failed': 0, 'no_return': 0}}
        logging.info("=" * 80)
        logging.info("[SubscribeReturnDiag] 开始诊断 sub_market_data 返回值")
        logging.info("=" * 80)
        sub_api = getattr(strategy_core, '_runtime_strategy_host', None)
        if sub_api is None:
            sub_api = strategy_core
        sub_method = getattr(sub_api, 'sub_market_data', None)
        if not callable(sub_method):
            logging.error("[SubscribeReturnDiag] sub_market_data 方法不可用")
            results['error'] = 'sub_market_data not callable'
            return results
        for exchange, inst_id, desc in test_contracts:
            results['summary']['total'] += 1
            contract_result = {'exchange': exchange, 'instrument_id': inst_id, 'desc': desc,
                               'return_value': None, 'return_type': None, 'exception': None, 'status': 'unknown'}
            try:
                ret = sub_method(exchange, inst_id)
                contract_result['return_value'] = str(ret)
                contract_result['return_type'] = type(ret).__name__
                if ret is None:
                    contract_result['status'] = 'no_return'
                    results['summary']['no_return'] += 1
                    logging.warning(f"[SubscribeReturnDiag] {inst_id} ({desc}): 返回值=None, 可能被忽略")
                elif ret is False:
                    contract_result['status'] = 'failed'
                    results['summary']['failed'] += 1
                    logging.error(f"[SubscribeReturnDiag] {inst_id} ({desc}): 返回值=False, 订阅失败")
                elif ret is True:
                    contract_result['status'] = 'success'
                    results['summary']['success'] += 1
                    logging.info(f"[SubscribeReturnDiag] {inst_id} ({desc}): 返回值=True, 订阅成功")
                else:
                    contract_result['status'] = 'other'
                    logging.info(f"[SubscribeReturnDiag] {inst_id} ({desc}): 返回值={ret}, 类型={type(ret).__name__}")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                contract_result['exception'] = str(e)
                contract_result['status'] = 'exception'
                results['summary']['failed'] += 1
                logging.error(f"[SubscribeReturnDiag] {inst_id} ({desc}): 异常={e}")
            results['contracts'][inst_id] = contract_result
        logging.info("-" * 80)
        logging.info(f"[SubscribeReturnDiag] 汇总: total={results['summary']['total']}, success={results['summary']['success']}, failed={results['summary']['failed']}, no_return={results['summary']['no_return']}")
        logging.info("=" * 80)
        return results

    @staticmethod
    def diagnose_option_metadata_diff(strategy_core):
        from ali2026v3_trading.config.params_service import get_params_service
        ps = get_params_service()
        test_contracts = [('al2607C25200', 'AL期权-有tick'), ('au2607C1032', 'AU期权-无tick'),
                          ('cu2607C104000', 'CU期权-无tick'), ('zn2607C20000', 'ZN期权-无tick'),
                          ('HO2607-C-2800', 'HO指数期权-有tick')]
        results = {'timestamp': datetime.now(CHINA_TZ).isoformat(), 'contracts': {}, 'diff_fields': {}}
        logging.info("=" * 80)
        logging.info("[OptionMetaDiff] 开始诊断期权元数据差异")
        logging.info("=" * 80)
        all_fields = set()
        for inst_id, desc in test_contracts:
            meta = ps.get_instrument_meta_by_id(inst_id)
            if meta:
                all_fields.update(meta.keys())
        key_fields = ['exchange', 'product', 'product_type', 'instrument_type', 'price_tick', 'multiplier',
                      'trading_hours', 'underlying_symbol', 'underlying_id', 'min_price_tick', 'contract_size',
                      'delivery_month', 'option_type', 'strike_price', 'expiration_date']
        key_fields = [f for f in key_fields if f in all_fields]
        logging.info(f"[OptionMetaDiff] 关键字段: {key_fields}")
        logging.info("-" * 80)
        header = f"{'合约':<20} {'描述':<15}"
        for f in key_fields[:8]:
            header += f" {f[:12]:<12}"
        logging.info(header)
        logging.info("-" * 120)
        for inst_id, desc in test_contracts:
            meta = ps.get_instrument_meta_by_id(inst_id)
            row = f"{inst_id:<20} {desc:<15}"
            for f in key_fields[:8]:
                v = meta.get(f, 'N/A') if meta else 'N/A'
                row += f" {str(v)[:12]:<12}"
            logging.info(row)
            results['contracts'][inst_id] = {'desc': desc, 'meta': meta, 'found': meta is not None}
        logging.info("=" * 80)
        return results


# ============================================================
# Section 2: 周期性诊断 (原 diagnosis_periodic.py)
# ============================================================


def _get_runtime_state():
    runtime_strategy = None
    runtime_core = None
    runtime_subscribed: Set[str] = set()
    historical_in_progress = False
    startup_ready = False
    try:
        global _runtime_strategy_ref
        if _runtime_strategy_ref is not None:
            runtime_strategy = _runtime_strategy_ref
        if runtime_strategy is None:
            from ali2026v3_trading.config.config_service import get_cached_params
            cached_params = get_cached_params() or {}
            runtime_strategy = cached_params.get('strategy')
        if runtime_strategy is None:
            try:
                from ali2026v3_trading.config.config_params import _param_table_cache
                if _param_table_cache is not None:
                    runtime_strategy = _param_table_cache.get('strategy')
            except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                pass
        runtime_core = getattr(runtime_strategy, 'strategy_core', None) if runtime_strategy is not None else None
        _raw_subscribed = None
        for _src in [runtime_strategy, getattr(runtime_strategy, 'params', None), runtime_core]:
            if _src is not None:
                _raw_subscribed = getattr(_src, '_subscribed_instruments', None)
                if _raw_subscribed:
                    break
        runtime_subscribed = set(_raw_subscribed) if _raw_subscribed else set()
        _historical_src = runtime_strategy if runtime_strategy is not None else runtime_core
        historical_in_progress = bool(getattr(_historical_src, '_historical_load_in_progress', False))
        startup_ready = bool(
            getattr(runtime_strategy, '_start_completed', False)
            or getattr(runtime_core, '_is_running', False)
            or runtime_subscribed
        )
        if not runtime_subscribed:
            logging.debug(f"[{MONITORED_DIAG_LABEL}] runtime_subscribed is empty: strategy={runtime_strategy is not None}, core={runtime_core is not None}")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as exc:
        logging.debug(f"[{MONITORED_DIAG_LABEL}] 获取运行时状态失败: {exc}")
    return runtime_strategy, runtime_core, runtime_subscribed, historical_in_progress, startup_ready


def _emit_periodic_resource_ownership_snapshot(runtime_core) -> None:
    ResourceOwnershipScanner.log_resource_ownership_table(runtime_core, phase='periodic-diagnosis')


_DIAGNOSIS_STARTUP_GRACE_SECONDS = 60.0
_first_diagnosis_call_time: Optional[float] = None
_first_diagnosis_call_lock = threading.Lock()


def _ensure_grace_period_started() -> float:
    global _first_diagnosis_call_time
    now = time.time()
    with _first_diagnosis_call_lock:
        if _first_diagnosis_call_time is None:
            _first_diagnosis_call_time = now
            logging.info(
                "[%s] 诊断宽限期开始计时，宽限期=%.0fs，诊断将在%.0fs后启动",
                MONITORED_DIAG_LABEL, _DIAGNOSIS_STARTUP_GRACE_SECONDS,
                _DIAGNOSIS_STARTUP_GRACE_SECONDS,
            )
        elapsed = now - _first_diagnosis_call_time
    return elapsed


def reset_diagnosis_grace_period():
    global _first_diagnosis_call_time
    with _first_diagnosis_call_lock:
        _first_diagnosis_call_time = None
    logging.info("[%s] 诊断宽限期已重置", MONITORED_DIAG_LABEL)


def run_14_contracts_periodic_diagnostic(storage=None, query_service=None) -> None:
    logging.info("\n" + "="*100)
    logging.info("📊 [%s] 开始检查", MONITORED_DIAG_LABEL)
    logging.info("="*100)

    runtime_strategy, runtime_core, runtime_subscribed, historical_in_progress, startup_ready = _get_runtime_state()
    _log_full_chain_entry_counts(len(runtime_subscribed))

    if historical_in_progress:
        logging.info(
            "[%s] 历史K线加载进行中，跳过本轮以避免启动期误报: subscribed=%d" % MONITORED_DIAG_LABEL,
            len(runtime_subscribed),
        )
        logging.info("="*100)
        return

    if runtime_strategy is not None and not startup_ready:
        logging.info("[%s] 启动桥接尚未完成，跳过本轮以避免初始化误报", MONITORED_DIAG_LABEL)
        logging.info("="*100)
        return

    _emit_periodic_resource_ownership_snapshot(runtime_core)

    runtime_strategy, runtime_core, runtime_subscribed, historical_in_progress, startup_ready = _get_runtime_state()

    if historical_in_progress:
        logging.info(
            "[%s] 历史K线加载进行中，跳过本轮以避免启动期误报: subscribed=%d" % MONITORED_DIAG_LABEL,
            len(runtime_subscribed),
        )
        logging.info("="*100)
        return

    if runtime_strategy is not None and not startup_ready:
        logging.info("[%s] 启动桥接尚未完成，跳过本轮以避免初始化误报", MONITORED_DIAG_LABEL)
        logging.info("="*100)
        return

    grace_elapsed = _ensure_grace_period_started()
    if grace_elapsed < _DIAGNOSIS_STARTUP_GRACE_SECONDS:
        logging.info(
            "[%s] 诊断宽限期未过(已过%.0fs/%.0fs)，跳过本轮以避免启动期误报",
            MONITORED_DIAG_LABEL, grace_elapsed, _DIAGNOSIS_STARTUP_GRACE_SECONDS,
        )
        logging.info("="*100)
        return

    success_contracts = []
    failed_contracts = []

    for contract, info in MONITORED_CONTRACTS.items():
        try:
            exists_in_file = False
            loaded_in_memory = False
            in_subscribe_list = False
            is_valid_format = False
            expected_format = None

            try:
                # FIX: config_dir路径错误，原为'ali2026v3_trading'子目录（不存在），应为'config'子目录
                config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config')
                config_lines = []
                for cfg_name in ['subscription_futures_fixed.txt', 'subscription_options_fixed.txt']:
                    cfg_path = os.path.join(config_dir, cfg_name)
                    if os.path.exists(cfg_path):
                        with open(cfg_path, 'r', encoding='utf-8') as f:
                            config_lines.extend(l.strip() for l in f if l.strip() and not l.startswith('#'))
                exists_in_file = any(
                    contract in line or line.endswith('.' + contract)
                    for line in config_lines
                )
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
                logging.debug(f"Diagnosis: config file check failed: {e}")

            try:
                from ali2026v3_trading.config.params_service import get_params_service
                _ps = get_params_service()
                if _ps is None:
                    loaded_in_memory = False
                    logging.debug(f"Diagnosis: get_params_service() returned None for {contract}")
                else:
                    _meta = _ps.get_instrument_meta_by_id(contract)
                    loaded_in_memory = _meta is not None
                    if not loaded_in_memory:
                        _int_id = _ps.get_internal_id(contract) if hasattr(_ps, 'get_internal_id') else None
                        _cache_size = len(_ps._instrument_id_to_internal_id) if hasattr(_ps, '_instrument_id_to_internal_id') else -1
                        logging.debug(f"Diagnosis: {contract} not in cache (internal_id={_int_id}, cache_size={_cache_size})")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                logging.debug(f"Diagnosis: memory load check failed: {e}")

            in_subscribe_list = contract in runtime_subscribed

            is_valid_format, expected_format, _ = validate_contract_format(contract)

            diagnose_config_file_presence(contract, exists_in_file)
            diagnose_config_loaded_status(contract, loaded_in_memory)
            diagnose_format_validation(contract, is_valid_format, expected_format, contract)

            subscribed = False
            registered = False
            has_internal_id = False
            internal_id = None
            tick_entry_count = 0
            tick_buffered = False
            storage_enqueued = False
            async_written = False
            duckdb_inserted = False
            tick_count = 0
            kline_count = 0

            _sub_mgr = None
            if storage and hasattr(storage, 'subscription_manager'):
                _sub_mgr = getattr(storage, 'subscription_manager', None)
            if _sub_mgr is None and runtime_core is not None:
                _core_storage = getattr(runtime_core, 'storage', None)
                if _core_storage and hasattr(_core_storage, 'subscription_manager'):
                    _sub_mgr = getattr(_core_storage, 'subscription_manager', None)
            if _sub_mgr is None:
                try:
                    from ali2026v3_trading.data.data_service import get_data_service
                    _ds = get_data_service()
                    if _ds is not None:
                        _sub_mgr = getattr(_ds, 'subscription_manager', None)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                    logging.debug(f"Diagnosis: data_service subscription_manager fallback failed: {e}")

            if _sub_mgr is not None:
                try:
                    _sub_success = getattr(_sub_mgr, '_subscription_success', None)
                    if _sub_success and isinstance(_sub_success, dict):
                        _subscribe_times = _sub_success.get('subscribe_time', {})
                        subscribed = contract in _subscribe_times
                        _tick_instruments = _sub_success.get('tick_instruments', set())
                        tick_buffered = contract in _tick_instruments
                        tick_entry_count = 1 if tick_buffered else 0
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.debug(f"Diagnosis: subscription_manager check failed: {e}")

            try:
                from ali2026v3_trading.config.params_service import get_params_service
                _ps = get_params_service()
                if _ps is not None:
                    _meta = _ps.get_instrument_meta_by_id(contract)
                    if _meta is not None:
                        registered = True
                        internal_id = _meta.get('internal_id') if isinstance(_meta, dict) else None
                        has_internal_id = internal_id is not None
                    _int_id = _ps.get_internal_id(contract) if hasattr(_ps, 'get_internal_id') else None
                    if _int_id is not None and not has_internal_id:
                        registered = True
                        internal_id = _int_id
                        has_internal_id = True
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                logging.debug(f"Diagnosis: params_service registration check failed: {e}")

            if query_service:
                try:
                    tick_count = query_service.get_tick_count(contract)
                    kline_count = query_service.get_kline_count(contract)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.debug(f"Diagnosis: query_service check failed: {e}")

            if storage:
                try:
                    if hasattr(storage, '_tick_shard_queues'):
                        for q in storage._tick_shard_queues:
                            try:
                                peek_count = min(q.qsize(), 100) if hasattr(q, 'qsize') else 0
                                peeked = []
                                for _ in range(peek_count):
                                    try:
                                        peeked.append(q.get_nowait())
                                    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                                        logging.debug("[diagnosis] 数据解析降级: %s", _r3_err)
                                        break
                                try:
                                    for item in peeked:
                                        if isinstance(item, dict):
                                            if item.get('instrument_id') == contract:
                                                storage_enqueued = True
                                                break
                                        elif isinstance(item, (list, tuple)) and len(item) > 0:
                                            args = item
                                            if len(args) > 0 and str(args[0]) == contract:
                                                storage_enqueued = True
                                                break
                                            if len(args) > 0:
                                                try:
                                                    internal_id = args[0]
                                                    info_by_id = storage._get_info_by_id(int(internal_id))
                                                    if info_by_id and info_by_id.get('instrument_id') == contract:
                                                        storage_enqueued = True
                                                        break
                                                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                                                    logging.debug(f"Diagnosis: storage enqueue check failed: {e}")
                                finally:
                                    for item in peeked:
                                        try:
                                            q.put_nowait(item)
                                        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                                            logging.debug("[R3-L2] unknown suppressed: %s", _r3_err)
                                            pass
                                if storage_enqueued:
                                    break
                            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                                logging.debug(f"Diagnosis: queue inspection failed: {e}")
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.debug(f"Diagnosis: async write status check failed: {e}")

                if query_service:
                    try:
                        kline_count = query_service.get_kline_count(contract)
                        tick_count = query_service.get_tick_count(contract)
                        duckdb_inserted = (kline_count > 0 or tick_count > 0)
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                        pass

            if not tick_buffered:
                _tick_src = runtime_core if runtime_core is not None else runtime_strategy
                if _tick_src:
                    _tps = getattr(_tick_src, '_tick_processing_service', None)
                    if _tps is None:
                        _tps = getattr(_tick_src, 'tick_processing_service', None)
                    if _tps and hasattr(_tps, '_tick_buffer'):
                        try:
                            buf = _tps._tick_buffer
                            if isinstance(buf, list):
                                tick_entry_count = sum(1 for t in buf if isinstance(t, dict) and t.get('instrument_id') == contract)
                                tick_buffered = (tick_entry_count > 0)
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                            logging.debug(f"Diagnosis: tick buffer check failed: {e}")

            if storage and hasattr(storage, '_queue_stats'):
                try:
                    async_written = storage._queue_stats.get('total_written', 0) > 0
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.debug(f"Diagnosis: async write stats check failed: {e}")

            diagnose_pre_registration(contract, in_subscribe_list, registered)

            has_tick_data = tick_count > 0
            has_kline_data = kline_count > 0
            is_success = registered and has_internal_id

            if is_success:
                success_contracts.append(contract)
            else:
                failure_reasons = []
                if not loaded_in_memory:
                    failure_reasons.insert(0, "❌ 合约元数据未加载")
                elif not exists_in_file:
                    failure_reasons.insert(0, "❌ 不在订阅文件中")
                elif not in_subscribe_list:
                    failure_reasons.insert(0, "❌ 未加入运行时订阅列表")
                elif not is_valid_format:
                    failure_reasons.insert(0, f"❌ 格式错误: {expected_format}")
                elif in_subscribe_list and not registered:
                    failure_reasons.insert(0, "⚠️ 在订阅列表但未注册")
                elif not registered:
                    failure_reasons.insert(0, "❌ 合约未注册")
                else:
                    if not has_tick_data:
                        failure_reasons.append("⚠️ 无Tick数据(实时链路异常)")
                    if not has_kline_data:
                        if has_tick_data:
                            failure_reasons.append("⚠️ 无K线数据(Tick已入库但K线未合成)")
                        else:
                            failure_reasons.append("⚠️ 无K线数据(无数据源)")

                failed_contracts.append({
                    'contract': contract,
                    'exists_in_file': exists_in_file, 'loaded_in_memory': loaded_in_memory,
                    'in_subscribe_list': in_subscribe_list, 'is_valid_format': is_valid_format,
                    'subscribed': subscribed, 'registered': bool(registered),
                    'has_internal_id': has_internal_id, 'tick_entry_count': tick_entry_count,
                    'tick_buffered': tick_buffered, 'storage_enqueued': storage_enqueued,
                    'async_written': async_written, 'duckdb_inserted': duckdb_inserted,
                    'has_kline_data': has_kline_data, 'has_tick_data': has_tick_data,
                    'internal_id': str(internal_id), 'kline_count': int(kline_count),
                    'tick_count': int(tick_count), 'reasons': failure_reasons
                })

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            failed_contracts.append({
                'contract': contract,
                'exists_in_file': False, 'loaded_in_memory': False,
                'in_subscribe_list': False, 'is_valid_format': False,
                'subscribed': False, 'registered': False,
                'has_internal_id': False, 'tick_entry_count': 0,
                'tick_buffered': False, 'storage_enqueued': False,
                'async_written': False, 'duckdb_inserted': False,
                'has_kline_data': False, 'has_tick_data': False,
                'internal_id': 'N/A', 'kline_count': 0, 'tick_count': 0,
                'reasons': [f"❌ 异常: {str(e)}"]
            })

    if success_contracts:
        logging.info(f"✅ 正常合约 ({len(success_contracts)}/{len(MONITORED_CONTRACTS)}):")
        futures = [c for c in success_contracts if MONITORED_CONTRACTS[c]['type'] == 'future']
        options = [c for c in success_contracts if MONITORED_CONTRACTS[c]['type'] == 'option']
        if futures:
            logging.info(f"  期货: {', '.join(futures)}")
        if options:
            logging.info(f"  期权: {', '.join(options)}")

    if failed_contracts:
        logging.info(f"❌ 异常合约 ({len(failed_contracts)}/{len(MONITORED_CONTRACTS)}):")  # [R22-P2-EP04]
        reason_stats = {}
        for fail_info in failed_contracts:
            reasons = fail_info['reasons']
            primary_reason = reasons[0] if reasons else "未知原因"
            if primary_reason not in reason_stats:
                reason_stats[primary_reason] = []
            reason_stats[primary_reason].append(fail_info['contract'])
        for reason, contracts in reason_stats.items():
            logging.info(f"  {reason} ({len(contracts)}个): {', '.join(contracts[:5])}{'...' if len(contracts) > 5 else ''}")
        logging.info(f"\n  [{MONITORED_CONTRACT_COUNT}Diagnosis] 📊 12环节状态汇总:")
        stage_stats = {
            '1-MetaLoaded': sum(1 for f in failed_contracts if not f.get('loaded_in_memory', False)),
            '2-SubFile': sum(1 for f in failed_contracts if not f.get('exists_in_file', False)),
            '3-Subscribe': sum(1 for f in failed_contracts if not f.get('in_subscribe_list', False)),
            '4-Format': sum(1 for f in failed_contracts if not f.get('is_valid_format', False)),
            '5-Subscribed': sum(1 for f in failed_contracts if not f.get('subscribed', False)),
            '6-Registered': sum(1 for f in failed_contracts if not f.get('registered', False)),
            '7-ID': sum(1 for f in failed_contracts if not f.get('has_internal_id', False)),
            '8-TickEntry': sum(1 for f in failed_contracts if f.get('tick_entry_count', 0) == 0),
            '9-TickBuffer': sum(1 for f in failed_contracts if not f.get('tick_buffered', False)),
            '10-Enqueue': sum(1 for f in failed_contracts if not f.get('storage_enqueued', False)),
            '11-AsyncWrite': sum(1 for f in failed_contracts if not f.get('async_written', False)),
            '12-DuckDB': sum(1 for f in failed_contracts if not f.get('duckdb_inserted', False)),
        }
        for stage_name, fail_count in stage_stats.items():
            if fail_count > 0:
                status = '❌' if fail_count == len(failed_contracts) else '⚠️'
                logging.info(f"    {stage_name:15s}: {fail_count}/{len(failed_contracts)} {status}")

    total = len(MONITORED_CONTRACTS)
    success_rate = (len(success_contracts) / total * 100) if total > 0 else 0
    logging.info(f"\n📈 诊断结果: {len(success_contracts)}/{total} 正常 ({success_rate:.1f}%)")

    if storage and hasattr(storage, '_tick_shard_queues'):
        try:
            tick_total = sum(q.qsize() for q in storage._tick_shard_queues)
            tick_max = sum(q.maxsize for q in storage._tick_shard_queues)
            kline_size = storage._kline_queue.qsize() if hasattr(storage, '_kline_queue') else 0
            kline_max = storage._kline_queue.maxsize if hasattr(storage, '_kline_queue') else 1
            maint_size = storage._maintenance_queue.qsize() if hasattr(storage, '_maintenance_queue') else 0
            total_size = tick_total + kline_size + maint_size
            total_max = tick_max + kline_max + (storage._maintenance_queue.maxsize if hasattr(storage, '_maintenance_queue') else 0)
            usage_rate = (total_size / total_max * 100) if total_max > 0 else 0
            if usage_rate > 90:
                logging.critical(f"[QUEUE_CRITICAL] 写入队列使用率超过90%: {usage_rate:.1f}% ({total_size}/{total_max})")
            elif usage_rate > 80:
                logging.warning(f"[QUEUE_WARNING] 写入队列使用率超过80%: {usage_rate:.1f}% ({total_size}/{total_max})")
            elif usage_rate > 70:
                logging.info(f"[QUEUE_INFO] 写入队列使用率: {usage_rate:.1f}% ({total_size}/{total_max})")
            for si in range(len(storage._tick_shard_queues)):
                q = storage._tick_shard_queues[si]
                sz = q.qsize()
                if sz > 0:
                    fill = sz / q.maxsize * 100 if q.maxsize > 0 else 0
                    logging.info(f"  TickShard-{si}: {sz}/{q.maxsize} ({fill:.1f}%)")
            if hasattr(storage, 'get_queue_stats'):
                stats = storage.get_queue_stats()
                logging.info(f"队列统计: 接收={stats.get('total_received', 0)}, 写入={stats.get('total_written', 0)}, 丢弃={stats.get('drops_count', 0)}, 峰值={stats.get('max_queue_size_seen', 0)}")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug(f"[QUEUE_CHECK] 队列状态检查失败: {e}")

    logging.info("="*100)


class ResourceOwnershipScanner:
    """资源所有权诊断扫描器"""

    @staticmethod
    def log_resource_ownership_table(strategy_core, phase: str = 'unknown') -> None:
        import threading as _threading
        _sid = None
        for _obj in [strategy_core]:
            if _obj is not None:
                _sid = getattr(_obj, 'strategy_id', None)
                if _sid is not None:
                    break
        if _sid is None:
            try:
                from ali2026v3_trading.config.config_service import get_cached_params
                _cp = get_cached_params() or {}
                _rs = _cp.get('strategy')
                for _obj in [_rs, getattr(_rs, 'strategy_core', None)]:
                    if _obj is not None:
                        _sid = getattr(_obj, 'strategy_id', None)
                        if _sid is not None:
                            break
            except Exception:
                pass
        if _sid is None:
            _sid = 'unknown'
        strategy_id = _sid
        run_id = getattr(strategy_core, '_lifecycle_run_id', 'N/A')
        ALLOWED_PREFIXES = (
            'Main', 'Thread-', 'APScheduler', 'ThreadPoolExecutor',
            'Storage-AsyncWriter[shared-service]', 'Storage-Cleanup[shared-service]',
            'SubAsyncWriter[shared-service]', 'SubRetry[shared-service]', 'SubCleanup[shared-service]',
            'TTypeService-Preload[shared-service]', 'onStop-worker',
        )
        threads = _threading.enumerate()
        strategy_threads = []
        shared_threads = []
        system_threads = []
        for t in threads:
            name = t.name or ''
            if '[shared-service]' in name:
                shared_threads.append(name)
            elif any(name.startswith(p) for p in ALLOWED_PREFIXES):
                system_threads.append(name)
            elif 'strategy' in name.lower() or strategy_id in name:
                strategy_threads.append(name)
            elif name and not name.startswith('Main'):
                system_threads.append(name)
        logging.info(
            f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}][phase={phase}]"
            f"[source_type=resource-ownership] "
            f"Thread scan: total={len(threads)}, shared-service={len(shared_threads)}, "
            f"strategy-instance={len(strategy_threads)}, system={len(system_threads)}"
        )
        if shared_threads:
            for name in shared_threads:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] "
                    f"Thread alive: {name} (expected: continues after strategy stop)"
                )
        if strategy_threads:
            for name in strategy_threads:
                logging.debug(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] "
                    f"LEAKED thread: {name} (expected: should be gone after strategy stop)"
                )
        else:
            if phase == 'stop':
                logging.info(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] "
                    f"✅ No strategy-instance threads leaked"
                )
        scheduler_mgr = getattr(strategy_core, '_scheduler_manager', None)
        if scheduler_mgr and hasattr(scheduler_mgr, 'get_jobs_by_owner'):
            try:
                remaining_jobs = scheduler_mgr.get_jobs_by_owner(strategy_id)
                if remaining_jobs:
                    job_ids = [j['job_id'] for j in remaining_jobs]
                    logging.warning(
                        f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=strategy-job] "
                        f"⚠️ LEAKED scheduler jobs: {job_ids}"
                    )
                else:
                    if phase == 'stop':
                        logging.info(
                            f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                            f"[run_id={run_id}][source_type=strategy-job] "
                            f"✅ No strategy-instance scheduler jobs leaked"
                        )
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.debug(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=resource-ownership] Scheduler diagnosis error: {e}"
                )
        storage_obj = getattr(strategy_core, '_storage', None)
        if storage_obj and hasattr(storage_obj, 'get_queue_stats'):
            try:
                qstats = storage_obj.get_queue_stats()
                qsize = qstats.get('current_queue_size', 0)
                tick_fill = qstats.get('tick_fill_rate', 0)
                kline_fill = qstats.get('kline_fill_rate', 0)
                maint_fill = qstats.get('maintenance_fill_rate', 0)
                if qsize > 0:
                    shard_parts = []
                    for k, v in sorted(qstats.items()):
                        if k.startswith('tick_shard_') and k.endswith('_size') and v > 0:
                            si = k.replace('tick_shard_', '').replace('_size', '')
                            fl = qstats.get(f'tick_shard_{si}_fill', 0)
                            shard_parts.append(f"s{si}={v}({fl:.1f}%)")
                    shard_info = " " + " ".join(shard_parts) if shard_parts else ""
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=shared-queue-drain] "
                        f"Storage queue backlog: {qsize} tick={tick_fill:.1f}% kline={kline_fill:.1f}% maint={maint_fill:.1f}%{shard_info}"
                    )
                else:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=shared-queue-drain] "
                        f"Storage queue empty"
                    )
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.debug(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=resource-ownership] Storage queue diagnosis error: {e}"
                )
        event_bus = getattr(strategy_core, '_event_bus', None)
        if event_bus and hasattr(event_bus, '_pending_events'):
            try:
                pending = getattr(event_bus, '_pending_events', 0)
                if pending > 0:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=event-tail] "
                        f"EventBus pending callbacks: {pending} (expected: drain in progress)"
                    )
                else:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=event-tail] "
                        f"✅ EventBus pending callbacks empty"
                    )
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.debug(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=resource-ownership] EventBus diagnosis error: {e}"
                )


class ControlActionLogger:
    """控制动作日志记录器"""

    _logger = logging.getLogger("ControlActionLogger")

    @staticmethod
    def log_control_action_enter(action_type, strategy_id, run_id, source):
        ControlActionLogger._logger.info(
            f"[ControlActionLogger][owner_scope=control-action] ENTER {action_type} "
            f"[strategy_id={strategy_id}] [run_id={run_id}] [source={source}]"
        )

    @staticmethod
    def log_control_action_success(action_type, strategy_id, run_id, result=None, source=None):
        base_msg = (
            f"[ControlActionLogger][owner_scope=control-action] SUCCESS {action_type} "
            f"[strategy_id={strategy_id}] [run_id={run_id}]"
        )
        if source is not None:
            base_msg += f" [source={source}]"
        if result is not None:
            base_msg += f" [result={result}]"
        ControlActionLogger._logger.info(base_msg)

    @staticmethod
    def log_control_action_fail(action_type, strategy_id, run_id, error, source=None):
        base_msg = (
            f"[ControlActionLogger][owner_scope=control-action] FAIL {action_type} "
            f"[strategy_id={strategy_id}] [run_id={run_id}] [error={error}]"
        )
        if source is not None:
            base_msg += f" [source={source}]"
        ControlActionLogger._logger.error(base_msg)

    @staticmethod
    def log_control_action_blocked(action_type, strategy_id, run_id, reason, source=None):
        base_msg = (
            f"[ControlActionLogger][owner_scope=control-action] BLOCKED {action_type} "
            f"[strategy_id={strategy_id}] [run_id={run_id}] [reason={reason}]"
        )
        if source is not None:
            base_msg += f" [source={source}]"
        # R27-P2-FC-01修复: BLOCKED事件日志从info提升到warning
        ControlActionLogger._logger.warning(base_msg)


# ============================================================
# Section 3: 诊断服务 (原 diagnosis_service.py)
# ============================================================


# 核心类定义
@dataclass(slots=True)
class DiagnoserConfig:
    """诊断器配置类"""
    max_logs: int = 100
    health_weights: Dict[str, int] = field(default_factory=lambda: {
        'CRITICAL': 25,
        'HIGH': 12,
        'MEDIUM': 6,
        'LOW': 2,
        'ERROR': 15,
        'WARNING': 2
    })
    enable_async_log: bool = True
    log_queue_size: int = 1000
    event_publish_timeout: float = 5.0
    enable_incremental_diagnosis: bool = False


@runtime_checkable
class StrategyProtocol(Protocol):
    """策略协议接口（类型约束）"""
    infini: Optional[Any]
    market_center: Optional[Any]
    params: Optional[Any]
    future_instruments: List[str]
    option_instruments: Dict[str, Any]
    kline_data: Dict[str, Any]
    my_state: str
    my_is_running: bool
    my_trading: bool

    def output(self, msg: str) -> None: ...
    def subscribe(self, *args, **kwargs) -> None: ...


class DiagnosisReport(object):
    """诊断报告数据类"""

    def __init__(self, config: Optional[DiagnoserConfig] = None):
        self.config = config or DiagnoserConfig()
        self.timestamp = datetime.now(CHINA_TZ).isoformat()
        self.breakpoints: List[Dict] = []
        self.warnings: List[str] = []
        self.errors: List[str] = []
        self.recommendations: List[str] = []
        self.logs: List[str] = []
        self.metrics: Dict[str, Any] = {}
        self._log_queue: Optional[queue.Queue] = None
        self._logs_lock = threading.Lock()

        if self.config.enable_async_log:
            self._log_queue = queue.Queue(maxsize=self.config.log_queue_size)

    def add_breakpoint(self, layer: str, component: str, issue: str,
                       impact: str, severity: str, fix: str = "") -> None:
        self.breakpoints.append({
            "layer": layer,
            "component": component,
            "issue": issue,
            "impact": impact,
            "severity": severity,
            "fix": fix
        })

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)

    def add_recommendation(self, msg: str) -> None:
        self.recommendations.append(msg)

    def add_log(self, msg: str, level: str = "INFO") -> None:
        timestamp = datetime.now(CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] [{level}] {msg}"

        if self._log_queue is not None:
            try:
                self._log_queue.put_nowait(log_entry)
            except queue.Full:
                logging.warning("[Diagnoser] Log queue full, discarding old logs")

        max_logs = self.config.max_logs
        with self._logs_lock:
            if len(self.logs) >= max_logs:
                critical_logs = [l for l in self.logs if '[ERROR]' in l or '[WARN]' in l]
                other_logs = [l for l in self.logs if '[ERROR]' not in l and '[WARN]' not in l]
                retained = critical_logs + other_logs[-(max_logs//2):]
                self.logs = retained
            self.logs.append(log_entry)

    def add_metric(self, key: str, value: Any) -> None:
        self.metrics[key] = value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "breakpoints": self.breakpoints,
            "warnings": self.warnings,
            "errors": self.errors,
            "recommendations": self.recommendations,
            "logs": self.logs[-100:],
            "metrics": self.metrics,
            "summary": {
                "breakpoint_count": len(self.breakpoints),
                "warning_count": len(self.warnings),
                "error_count": len(self.errors),
                "recommendation_count": len(self.recommendations)
            }
        }

    def get_health_score(self) -> float:
        base_score = 100.0
        weights = self.config.health_weights

        for bp in self.breakpoints:
            weight = weights.get(bp['severity'], 5)
            base_score -= weight

        base_score -= len(self.errors) * weights.get('ERROR', 15)
        base_score -= len(self.warnings) * weights.get('WARNING', 2)

        return max(0.0, min(100.0, base_score))


class StrategyDiagnoser:
    """策略诊断器 - Core 层辅助服务

    职责:
    - 全链路诊断（数据源、合约、订阅、K 线）
    - 断点检测
    - 问题排查
    - 健康评分
    - 自动修复建议

    设计原则:
    - Composition over Inheritance
    - 单一职责
    - 线程安全
    - 非侵入式诊断
    """

    def __init__(self, strategy_instance: Optional[StrategyProtocol] = None,
                 event_bus: Optional[Any] = None,
                 config: Optional[DiagnoserConfig] = None):
        self.strategy = strategy_instance
        self.config = config or DiagnoserConfig()
        self.report = DiagnosisReport(self.config)
        self._lock = threading.RLock()
        self._event_bus = event_bus
        self._last_diagnosis_result: Dict[str, Any] = {}
        self._log_worker_thread: Optional[threading.Thread] = None
        self._stop_log_worker = threading.Event()

        self._strategy_initialized = self._check_strategy_initialization()

        if self.config.enable_async_log and self.report._log_queue:
            self._start_log_worker()

        # DFG-P1-10修复: 订阅信号过滤统计事件，持久化到诊断服务
        self._last_filter_stats: Optional[Dict[str, Any]] = None
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _bus.subscribe_weak('signal.filter_stats', self._on_filter_stats_event)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] __init__ suppressed: %s", _r3_err)
            pass

    def _check_strategy_initialization(self) -> bool:
        if self.strategy is None:
            return False

        required_attrs = ['infini', 'market_center', 'params', 'future_instruments',
                         'option_instruments', 'kline_data', 'my_state', 'my_is_running',
                         'my_trading']

        for attr in required_attrs:
            if not hasattr(self.strategy, attr):
                self._log(f"策略缺少属性: {attr}", "WARN")
                return False

        return True

    # DFG-P1-10修复: 信号过滤统计事件消费者
    def _on_filter_stats_event(self, event: Any) -> None:
        """DFG-P1-10修复: 消费信号过滤统计事件，持久化到诊断服务

        当信号过滤统计更新时，缓存到诊断服务，
        供诊断报告和健康检查使用。
        """
        try:
            if isinstance(event, dict):
                self._last_filter_stats = event.get('stats')
            elif hasattr(event, 'stats'):
                self._last_filter_stats = event.stats
            if self._last_filter_stats:
                self.report.add_metric('signal_filter_stats', self._last_filter_stats)
                # P2修复: 信号过滤统计持久化 — 将统计写入磁盘文件
                self._persist_filter_stats(self._last_filter_stats)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] _on_filter_stats_event suppressed: %s", _r3_err)
            pass

    # P2修复: 信号过滤统计持久化
    _FILTER_STATS_PERSIST_DIR: Optional[str] = None

    def _persist_filter_stats(self, stats: Dict[str, Any]) -> None:
        """P2修复: 将信号过滤统计持久化到磁盘

        每次统计更新时追加写入JSONL文件，确保重启后可恢复历史统计。

        Args:
            stats: 信号过滤统计数据
        """
        try:
            if self._FILTER_STATS_PERSIST_DIR is None:
                self._FILTER_STATS_PERSIST_DIR = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), 'logs', 'filter_stats'
                )
            persist_dir = self._FILTER_STATS_PERSIST_DIR
            if not os.path.exists(persist_dir):
                os.makedirs(persist_dir, exist_ok=True)

            from datetime import date
            today = date.today().isoformat()
            persist_file = os.path.join(persist_dir, f'filter_stats_{today}.jsonl')

            import json
            record = {
                'timestamp': datetime.now(CHINA_TZ).isoformat(),
                'stats': stats,
            }
            with open(persist_file, 'a', encoding='utf-8') as f:
                f.write(json_dumps(record) + '\n')
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[DiagnosisService] P2修复: 过滤统计持久化失败: %s", e)

    def load_persisted_filter_stats(self, days: int = 7) -> List[Dict[str, Any]]:
        """P2修复: 加载持久化的信号过滤统计

        Args:
            days: 加载最近N天的统计

        Returns:
            统计记录列表
        """
        records = []
        try:
            if self._FILTER_STATS_PERSIST_DIR is None:
                self._FILTER_STATS_PERSIST_DIR = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), 'logs', 'filter_stats'
                )
            persist_dir = self._FILTER_STATS_PERSIST_DIR
            if not os.path.exists(persist_dir):
                return records

            import json
            from datetime import date, timedelta
            for i in range(days):
                day_str = (date.today() - timedelta(days=i)).isoformat()
                persist_file = os.path.join(persist_dir, f'filter_stats_{day_str}.jsonl')
                if os.path.exists(persist_file):
                    with open(persist_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                try:
                                    records.append(json.loads(line))
                                except json.JSONDecodeError:
                                    pass
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[DiagnosisService] P2修复: 加载持久化统计失败: %s", e)
        return records

    def _publish_event_with_retry(self, event_type: str, data: Dict[str, Any], max_retries: int = 3) -> None:
        if not self._event_bus:
            return

        retry_count = 0
        last_error = None

        while retry_count < max_retries:
            try:
                event = type(event_type, (), {
                    'type': event_type,
                    'timestamp': datetime.now(CHINA_TZ).isoformat(),
                    **data
                })()

                self._event_bus.publish(event, async_mode=True)
                return

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                last_error = e
                retry_count += 1

                if retry_count < max_retries:
                    wait_time = 0.5 * (2 ** (retry_count - 1))
                    logging.warning(f"[StrategyDiagnoser] Event publish failed (attempt {retry_count}/{max_retries}): {e}")
                    time.sleep(wait_time)
                else:
                    logging.error(f"[StrategyDiagnoser] Event publish failed after {max_retries} retries: {e}")
                    logging.debug(f"[StrategyDiagnoser] Failed event: {event_type}, data_keys={list(data.keys())}")

    def _start_log_worker(self) -> None:
        def log_worker():
            while not self._stop_log_worker.is_set():
                try:
                    log_entry = self.report._log_queue.get(timeout=1.0)
                    if '[ERROR]' in log_entry:
                        logging.error(f"[Diagnoser] {log_entry}")
                    elif '[WARN]' in log_entry:
                        logging.warning(f"[Diagnoser] {log_entry}")
                    else:
                        logging.info(f"[Diagnoser] {log_entry}")
                except queue.Empty:
                    logging.debug("[Diagnoser] Log queue empty, waiting...")
                    continue
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.error(f"[Diagnoser] Log worker error: {e}")

        self._log_worker_thread = threading.Thread(target=log_worker, daemon=True)
        self._log_worker_thread.start()

    def _stop_log_workers(self) -> None:
        if self._log_worker_thread:
            self._stop_log_worker.set()
            self._log_worker_thread.join(timeout=2.0)

    def _log(self, msg: str, level: str = "INFO") -> None:
        with self._lock:
            self.report.add_log(msg, level)

        if self.strategy and hasattr(self.strategy, 'output'):
            try:
                self.strategy.output(f"[Diagnoser] {msg}")
            except (AttributeError, TypeError, ValueError) as e:
                logging.error(f"[Diagnoser] 输出日志失败：{e}")
                logging.error(f"[Diagnoser] {msg}")
        else:
            log_func = {
                'DEBUG': logging.debug,
                'INFO': logging.info,
                'WARN': logging.warning,
                'WARNING': logging.warning,
                'ERROR': logging.error,
                'CRITICAL': logging.critical
            }.get(level.upper(), logging.info)

            log_func(f"[Diagnoser] {msg}")

    def _run_incremental_diagnosis(self, start_time: float) -> Dict[str, Any]:
        self._log("\n【增量诊断模式】", "INFO")

        if self.strategy:
            current_trading = getattr(self.strategy, 'my_trading', False)
            last_trading = self._last_diagnosis_result.get('state', {}).get('my_trading', False)

            if current_trading != last_trading:
                self._log(f"交易状态变化：{last_trading} -> {current_trading}", "INFO")
                self._diagnose_state()
            else:
                self._log("状态无显著变化，跳过详细诊断", "INFO")

        elapsed = time.time() - start_time
        self.report.add_metric('diagnosis_duration_ms', elapsed * 1000)
        self.report.add_metric('incremental_mode', True)

        self._log("=" * 60)
        self._log("增量诊断完成")
        self._log(f"健康评分：{self.report.get_health_score():.1f}/100")
        self._log("=" * 60)

        result = self.report.to_dict()
        self._last_diagnosis_result = result

        return result

    def run_full_diagnosis(self, force_full: bool = False) -> Dict[str, Any]:
        with self._lock:
            self._log("=" * 60)
            self._log("开始策略全链路诊断")
            self._log("=" * 60)

            start_time = time.time()

            if (self.config.enable_incremental_diagnosis and
                not force_full and
                self._last_diagnosis_result):
                return self._run_incremental_diagnosis(start_time)

            if self.strategy is None:
                self._log("警告：策略实例为 None，仅运行基础检查", "WARN")
                self._diagnose_basic()
            else:
                self._diagnose_data_source()
                self._diagnose_instruments()
                self._diagnose_subscriptions()
                self._diagnose_klines()
                self._diagnose_state()

            elapsed = time.time() - start_time
            self.report.add_metric('diagnosis_duration_ms', elapsed * 1000)

            self._generate_recommendations()

            self._log("=" * 60)
            self._log("诊断完成")
            self._log(f"断点：{len(self.report.breakpoints)}")
            self._log(f"警告：{len(self.report.warnings)}")
            self._log(f"错误：{len(self.report.errors)}")
            self._log(f"健康评分：{self.report.get_health_score():.1f}/100")
            self._log("=" * 60)

            result = self.report.to_dict()
            self._last_diagnosis_result = result

            self._publish_event_with_retry('DiagnosisCompleted', result)

            return result

    def _diagnose_basic(self) -> None:
        self._log("\n【基础诊断】")

        import sys
        self._log(f"Python 版本：{sys.version}")
        self.report.add_metric('python_version', sys.version.split()[0])

        modules = ['threading', 'datetime', 'collections', 'typing']
        for mod in modules:
            try:
                importlib.import_module(mod)
                self._log(f"模块 {mod}: OK")
            except ImportError as e:
                error_msg = f"模块 {mod} 不可用"
                self.report.add_error(error_msg)
                logging.error(f"[Diagnoser] {error_msg}: {e}")

    def _diagnose_data_source(self) -> None:
        self._log("\n【数据源诊断】")
        s = self.strategy

        infini = getattr(s, 'infini', None)
        if infini is None:
            self.report.add_warning("infini 对象为 None")
            self.report.add_breakpoint(
                "DataSource", "infini", "infini 对象为 None",
                "无法访问交易平台", "CRITICAL",
                "检查平台连接配置"
            )
        else:
            self._log("infini 对象：OK")

        mc = getattr(s, 'market_center', None)
        if mc is None:
            self.report.add_warning("market_center 为 None")
            self.report.add_breakpoint(
                "DataSource", "market_center", "market_center 为 None",
                "无法获取行情数据", "HIGH",
                "检查 MarketCenter 初始化"
            )
        else:
            self._log("market_center: OK")

    def _diagnose_instruments(self) -> None:
        self._log("\n【合约加载诊断】")
        s = self.strategy

        params = getattr(s, 'params', None)
        if params is None:
            self.report.add_breakpoint(
                "Instrument", "params", "参数对象为 None",
                "无法获取配置", "CRITICAL",
                "检查参数初始化"
            )
            return

        key_params = ['subscribe_options', 'future_products', 'option_products']
        for p in key_params:
            val = getattr(params, p, None)
            if val is None:
                self.report.add_warning(f"参数 {p} 未设置")
            else:
                self._log(f"参数 {p}: {val}")

        fut_insts = getattr(s, 'future_instruments', None)
        if fut_insts is None:
            self.report.add_breakpoint(
                "Instrument", "future_instruments", "期货合约列表为 None",
                "无法计算期权宽度", "CRITICAL",
                "检查期货合约加载逻辑"
            )
        elif len(fut_insts) == 0:
            self.report.add_breakpoint(
                "Instrument", "future_instruments", "期货合约列表为空",
                "无标的期货数据", "HIGH",
                "检查合约代码配置"
            )
        else:
            self._log(f"期货合约数：{len(fut_insts)}")

        opt_insts = getattr(s, 'option_instruments', None)
        if opt_insts is None:
            self.report.add_breakpoint(
                "Instrument", "option_instruments", "期权合约字典为 None",
                "无法加载期权数据", "CRITICAL",
                "检查期权合约加载逻辑"
            )
        elif len(opt_insts) == 0:
            self.report.add_breakpoint(
                "Instrument", "option_instruments", "期权合约字典为空",
                "无期权数据可用", "HIGH",
                "检查期权合约代码配置"
            )
        else:
            self._log(f"期权合约数：{len(opt_insts)}")

    def _diagnose_subscriptions(self) -> None:
        self._log("\n【订阅状态诊断】")
        s = self.strategy

        if not hasattr(s, 'subscribe'):
            self.report.add_breakpoint(
                "Subscription", "subscribe", "缺少 subscribe 方法",
                "无法订阅行情", "CRITICAL",
                "确保继承 BaseStrategy"
            )
            return

        has_subscribe = hasattr(s, 'subscribe') and callable(getattr(s, 'subscribe', None))
        if not has_subscribe:
            self.report.add_warning("策略缺少 subscribe() 方法")
            self.report.add_breakpoint(
                "Subscription", "subscribe_method", "缺少订阅方法",
                "无法订阅行情", "MEDIUM",
                "确保继承 BaseStrategy"
            )
        else:
            self._log("订阅方法：OK")

    def _diagnose_klines(self) -> None:
        self._log("\n【K 线数据诊断】")
        s = self.strategy

        kline_data = getattr(s, 'kline_data', None)
        if kline_data is None:
            self.report.add_breakpoint(
                "Kline", "kline_data", "K 线字典为 None",
                "无法获取 K 线数据", "CRITICAL",
                "检查 K 线初始化"
            )
            return

        if len(kline_data) == 0:
            self.report.add_warning("K 线数据为空")
        else:
            self._log(f"K 线合约数：{len(kline_data)}")

            for inst_id, kline in list(kline_data.items())[:3]:
                if hasattr(kline, 'close'):
                    close_array = kline.close
                    if close_array and len(close_array) > 0:
                        latest_price = close_array[-1]
                        self._log(f"{inst_id} 最新价：{latest_price}")
                    else:
                        self.report.add_warning(f"{inst_id} K 线数据为空")
                else:
                    self.report.add_warning(f"{inst_id} K 线对象无效")

    def _diagnose_state(self) -> None:
        self._log("\n【状态管理诊断】")
        s = self.strategy

        state_attributes = ['my_state', 'my_is_running', 'my_trading']
        for attr in state_attributes:
            val = getattr(s, attr, None)
            if val is None:
                self.report.add_warning(f"状态 {attr} 未定义")
            else:
                self._log(f"{attr}: {val}")

        trading = getattr(s, 'my_trading', False)
        if not trading:
            self.report.add_warning("当前不在交易状态")

    def _generate_recommendations(self) -> None:
        self._log("\n【生成修复建议】")

        for bp in self.report.breakpoints:
            if bp['severity'] == 'CRITICAL':
                self.report.add_recommendation(f"[紧急] {bp['fix']} - {bp['component']}: {bp['issue']}")
            elif bp['severity'] == 'HIGH':
                self.report.add_recommendation(f"[重要] {bp['fix']} - {bp['component']}")

        if len(self.report.warnings) > 5:
            self.report.add_recommendation("[建议] 检查策略初始化流程，确保所有组件正确加载")

        if len(self.report.errors) > 0:
            self.report.add_recommendation("[严重] 优先解决所有错误，然后处理警告")

        health_score = self.report.get_health_score()
        if health_score >= 90:
            self._log("健康状态：优秀", "INFO")
        elif health_score >= 70:
            self._log("健康状态：良好", "INFO")
            self.report.add_recommendation("[建议] 处理 HIGH 级别断点可提升健康度")
        elif health_score >= 50:
            self._log("健康状态：一般", "WARN")
            self.report.add_recommendation("[注意] 建议处理 CRITICAL 和 HIGH 级别断点")
        else:
            self._log("健康状态：较差", "ERROR")
            self.report.add_recommendation("[紧急] 立即处理所有 CRITICAL 级别断点")

    def get_quick_report(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "timestamp": datetime.now(CHINA_TZ).isoformat(),
                "health_score": self.report.get_health_score(),
                "breakpoint_count": len(self.report.breakpoints),
                "warning_count": len(self.report.warnings),
                "error_count": len(self.report.errors),
                "critical_count": sum(1 for bp in self.report.breakpoints if bp['severity'] == 'CRITICAL')
            }


# 向后兼容别名: DiagnosisService → StrategyDiagnoser
DiagnosisService = StrategyDiagnoser




# 异常处理装饰器
def handle_import_errors(allow_failure=False, default_value=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ImportError as e:
                if allow_failure:
                    logging.warning(f"[import] Optional import failed: {e}")
                    return default_value
                else:
                    logging.error(f"[import] Required import failed: {e}")
                    logging.exception("[import] Traceback:")
                    raise
        return wrapper
    return decorator


def log_exceptions(log_level=logging.ERROR, reraise=True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.log(log_level, f"[exception] {func.__name__} failed: {e}")
                logging.exception(f"[exception] {func.__name__} traceback:")

                if reraise:
                    raise
                return None
        return wrapper
    return decorator


def safe_call(default_value=None, catch_exceptions=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except catch_exceptions:
                return default_value
        return wrapper
    return decorator


def optional_import(func):
    return handle_import_errors(allow_failure=True, default_value=None)(func)


def required_import(func):
    return handle_import_errors(allow_failure=False)(func)


# Tick探针管理
_tick_probe_stats = {
    'entry': {'count': 0, 'last_log': 0},
    'dispatched': {'count': 0, 'last_log': 0},
    'saved': {'count': 0, 'last_log': 0},
    'error': {'count': 0, 'last_log': 0, 'errors': []}
}

_tick_probe_interval = 30.0
_tick_probe_lock = threading.Lock()


def record_tick_probe(stage: str, instrument_id: str, price: float, error_msg: Optional[str] = None) -> None:
    now = time.time()
    if stage not in _tick_probe_stats:
        return

    with _tick_probe_lock:
        stats = _tick_probe_stats[stage]
        stats['count'] += 1

        if stage == 'error' and error_msg:
            stats['errors'].append({'time': now, 'instrument_id': instrument_id, 'price': price, 'error': error_msg})
            if len(stats['errors']) > 100:
                stats['errors'] = stats['errors'][-50:]
            logging.error(f"[PROBE_TICK_ERROR] {instrument_id} @ {price}: {error_msg}")

        if now - stats.get('last_log', 0) >= _tick_probe_interval:
            count = stats['count']
            if count > 0:
                if stage == 'error':
                    errors = stats['errors']
                    if errors:
                        for err in errors[-5:]:
                            logging.error(f"[PROBE_TICK_SUMMARY] ERROR | {err['instrument_id']} @ {err['price']} | {err['error']}")
                        logging.error(f"[PROBE_TICK_SUMMARY] Total errors in last 30s: {len(errors)} (showing last 5)")
                else:
                    names = {'entry': 'Entry', 'dispatched': 'Dispatched', 'saved': 'Saved'}
                    logging.info(f"[PROBE_TICK_SUMMARY] {names.get(stage, stage)} | Count: {count} ticks in last 30s")
            stats['count'] = 0
            stats['last_log'] = now

# ============================================================================
# Unified __all__
# ============================================================================

__all__ = [
    'HealthMonitor',
    'get_health_monitor',
    'check_disk_space',
    'StructuredJsonlLogger',
    'HealthCheckAPI',
    'HealthCheckAggregator',
    'CodeQualityMetrics',
    'DiagnosisProbeManager',
    'DiagnosisReport',
    'DiagnosisService',
    'StrategyDiagnoser',
    'DiagnoserConfig',
    'StrategyProtocol',
    'handle_import_errors',
    'log_exceptions',
    'safe_call',
    'optional_import',
    'required_import',
    'run_14_contracts_periodic_diagnostic',
    'reset_diagnosis_grace_period',
    'is_monitored_contract',
    'get_contract_info',
    'validate_contract_format',
    'record_tick_probe',
    'ControlActionLogger',
    'ResourceOwnershipScanner',
    'MONITORED_CONTRACTS',
    'MONITORED_CONTRACT_COUNT',
    'MONITORED_DIAG_LABEL',
    'diagnose_parse_transform',
    'diagnose_parse_failure',
    'diagnose_subscription',
    'diagnose_pre_registration',
    'diagnose_config_file_presence',
    'diagnose_config_loaded_status',
    'diagnose_format_validation',
    'diagnose_tick_entry',
    'diagnose_tick_buffer',
    'diagnose_tick_flush',
    'diagnose_storage_enqueue',
    'diagnose_async_write',
    'diagnose_duckdb_insert',
    'log_diagnostic_event',
    'diagnose_registration',
    'diagnose_id_lookup',
]
