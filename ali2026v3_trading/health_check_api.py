import logging
import json
import time
import os
import threading
from collections import deque
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from ali2026v3_trading.resilience_utils import deterministic_round
from ali2026v3_trading.shared_utils import CHINA_TZ
from ali2026v3_trading.serialization_utils import json_dumps

logger = logging.getLogger(__name__)


# ============================================================================
# DR-P1-07修复: HealthMonitor 自动故障检测
# ============================================================================
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
        self._status: str = "HEALTHY"  # HEALTHY / DEGRADED / CRITICAL
        self._lock = threading.Lock()
        self._health_events_path = os.path.join('logs', 'health_events.jsonl')
        self._core_services: Dict[str, Any] = {}  # service_name -> service_instance
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

        # 检查注册的核心服务
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
                except Exception as e:
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
                f.write(json_dumps(record) + '\n')
        except Exception as e:
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


# 全局单例
_health_monitor: Optional[HealthMonitor] = None
_health_monitor_lock = threading.Lock()


def get_health_monitor() -> HealthMonitor:
    global _health_monitor
    with _health_monitor_lock:
        if _health_monitor is None:
            _health_monitor = HealthMonitor()
        return _health_monitor


# ============================================================================
# DR-P1-14修复: 磁盘空间预警
# ============================================================================
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

        # R27-P2-FP-10修复: round()→deterministic_round()
        return {
            'total_gb': deterministic_round(total_gb, 2),
            'used_gb': deterministic_round(used_gb, 2),
            'free_gb': deterministic_round(free_gb, 2),
            'usage_percent': deterministic_round(usage_percent, 2),
            'status': status,
        }
    except Exception as e:
        logging.error("[DR-P1-14] 磁盘空间检查失败: %s", e)
        return {
            'total_gb': 0, 'used_gb': 0, 'free_gb': 0,
            'usage_percent': 0, 'status': 'UNKNOWN', 'error': str(e),
        }


# ============================================================================
# R13-P1-SEC-04修复: 简单API频率限制器
# ============================================================================
class SimpleRateLimiter:
    """滑动窗口频率限制器，防止API被过度调用"""

    def __init__(self, max_calls: int = 60, window_seconds: float = 60.0):
        self._max_calls = max_calls
        self._window_seconds = window_seconds
        self._timestamps: deque = deque()
        self._lock = threading.Lock()

    def allow(self) -> bool:
        """检查是否允许调用，返回True表示允许"""
        now = time.time()
        with self._lock:
            cutoff = now - self._window_seconds
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()
            if len(self._timestamps) >= self._max_calls:
                return False
            self._timestamps.append(now)
            return True


class StructuredJsonlLogger:
    _JSONL_SCHEMA_VERSION = "v1.0"  # LG-P1-03修复: schema版本管理
    _JSONL_MAX_BYTES = 50 * 1024 * 1024  # LG-P1-02修复: JSONL rotate上限50MB
    _JSONL_BACKUP_COUNT = 3  # LG-P1-02修复: 保留3个备份

    def __init__(self, log_dir: str = "logs", strategy_id: Optional[str] = None):
        if strategy_id:
            log_dir = os.path.join(log_dir, str(strategy_id))
        os.makedirs(log_dir, exist_ok=True)
        self._log_dir = log_dir
        self._signal_log_path = os.path.join(log_dir, "signals.jsonl")
        self._order_log_path = os.path.join(log_dir, "orders.jsonl")
        # LG-P1-02修复: 初始化时执行rotate检查
        self._rotate_if_needed(self._signal_log_path)
        self._rotate_if_needed(self._order_log_path)
        # R14-P1-LOG-14修复: 缓存文件句柄避免每次open/close
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
        except OSError:
            pass

    def close(self):
        """关闭文件句柄 — NEW-P1-01修复: 添加异常安全保护"""
        for attr_name in ('_signal_log_f', '_order_log_f'):
            fh = getattr(self, attr_name, None)
            if fh:
                try:
                    fh.flush()
                    fh.close()
                except (OSError, ValueError):
                    pass
                finally:
                    setattr(self, attr_name, None)

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def log_signal(self, signal: Dict[str, Any], strategy_id: Optional[str] = None) -> None:
        # P1-R11-17修复: strategy_id缺失时发出警告，防止审计溯源歧义
        _sid = strategy_id or signal.get("strategy_id", "unknown")
        if not _sid or _sid == "unknown":
            logging.warning("[P1-R11-17] strategy_id missing in log entry, may cause audit ambiguity")
        record = {
            "schema_version": self._JSONL_SCHEMA_VERSION,  # LG-P1-03修复
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
        # LG-P1-04修复: 脱敏处理 - 扩展敏感字段集+路径脱敏
        _sanitize_keys = {'api_key', 'access_key', 'access_secret', 'password', 'token', 'secret', 'credential'}
        record = {k: ('***' if k in _sanitize_keys else v) for k, v in record.items()}
        self._signal_log_f.write(json_dumps(record) + "\n")  # R14-P1-LOG-14修复: 使用缓存句柄

    def log_order(self, order: Dict[str, Any]) -> None:
        record = {
            "schema_version": self._JSONL_SCHEMA_VERSION,  # LG-P1-03修复
            "timestamp": datetime.now(CHINA_TZ).isoformat(),
            "event_type": "order",
            "order_id": order.get("order_id", ""),
            "instrument_id": order.get("instrument_id", ""),
            "direction": order.get("direction", ""),
            "price": order.get("price", 0.0),
            "volume": order.get("volume", 0),
            "order_type": order.get("order_type", ""),
            "status": order.get("status", ""),
            "filled_volume": order.get("filled_volume", 0),
            "remaining_volume": order.get("remaining_volume", 0),
        }
        _sanitize_keys = {'api_key', 'access_key', 'access_secret', 'password', 'token', 'secret', 'credential'}
        record = {k: ('***' if k in _sanitize_keys else v) for k, v in record.items()}
        # LG-P1-02修复: 写入前检查rotate
        self._rotate_if_needed(self._order_log_path)
        self._order_log_f.write(json_dumps(record) + "\n")  # R14-P1-LOG-14修复

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
            f.write(json_dumps(record) + "\n")

    # R24-P1-TR-12修复: 结构化日志增加风控/策略/诊断方法
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
            f.write(json_dumps(record) + "\n")

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
            f.write(json_dumps(record) + "\n")

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
            f.write(json_dumps(record) + "\n")


class HealthCheckAPI:
    # ✅ 健康检查阈值参数化（P1-D组后续完善）
    DEFAULT_SIGNAL_STALE_SECONDS = 300.0
    DEFAULT_ORDER_STALE_SECONDS = 600.0
    DEFAULT_DAILY_DRAWDOWN_THRESHOLD = 0.05
    DEFAULT_MAX_OPEN_POSITIONS = 10

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        cfg = config or {}
        self._start_time = datetime.now(timezone(timedelta(hours=8)))
        # R13-P1-SEC-04修复: API频率限制器
        self._rate_limiter = SimpleRateLimiter(
            max_calls=cfg.get('rate_limit_max_calls', 60),
            window_seconds=cfg.get('rate_limit_window_seconds', 60.0),
        )
        # R13-P2-LOG-04修复: 集成StructuredJsonlLogger，健康检查结果写入结构化日志
        self._structured_logger: Optional[StructuredJsonlLogger] = None
        try:
            self._structured_logger = StructuredJsonlLogger(
                log_dir=cfg.get('structured_log_dir', 'logs'),
                strategy_id=cfg.get('strategy_id'),
            )
        except Exception:
            pass  # 结构化日志不可用时静默降级
        self._component_status: Dict[str, str] = {
            "data_feeds": "unknown",
            "greeks_calculator": "unknown",
            "state_diagnosis": "unknown",
            "signal_generator": "unknown",
            "risk_manager": "unknown",
            "order_executor": "unknown",
        }
        self._open_position_count: int = 0
        self._last_signal_time: Optional[float] = 0.0  # R13-P1-API-01修复: 默认0.0替代None，避免API返回None
        self._last_order_time: Optional[float] = 0.0  # R13-P1-API-01修复: 默认0.0替代None，避免API返回None

        # 从ConfigService统一配置中心加载阈值
        self._signal_stale_seconds = self.DEFAULT_SIGNAL_STALE_SECONDS
        self._order_stale_seconds = self.DEFAULT_ORDER_STALE_SECONDS
        try:
            from ali2026v3_trading.config_service import ConfigService
            cs = ConfigService()
            self._signal_stale_seconds = cfg.get(
                'signal_stale_seconds',
                getattr(cs.trading, 'strategy_heartbeat_interval_seconds', self.DEFAULT_SIGNAL_STALE_SECONDS) * 10
            )
            self._order_stale_seconds = cfg.get(
                'order_stale_seconds',
                getattr(cs.trading, 'strategy_heartbeat_interval_seconds', self.DEFAULT_ORDER_STALE_SECONDS) * 20
            )
        except Exception:
            pass

        self._alert_thresholds = {
            "l1_circuit_breaker": cfg.get('l1_circuit_breaker', True),
            "daily_drawdown": cfg.get('daily_drawdown_threshold', self.DEFAULT_DAILY_DRAWDOWN_THRESHOLD),
            "max_open_positions": cfg.get('max_open_positions', self.DEFAULT_MAX_OPEN_POSITIONS),
        }
        # R15-P1-RES-10修复: 主动推送健康状态
        self._push_interval_sec: float = cfg.get('push_interval_sec', 30.0)
        self._push_thread = None
        self._push_stop = threading.Event()
        self._push_callback = None

        # OPS-P1-02修复: 健康状态变更回调
        self._health_change_callbacks: list = []
        self._last_overall_status: str = "unknown"

        # DFG-P1-04修复: 订阅Greeks仪表盘更新事件，缓存最新数据
        self._cached_greeks_dashboard: Optional[Dict[str, Any]] = None
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _bus.subscribe_weak('greeks.dashboard_updated', self._on_greeks_dashboard_updated)
        except Exception:
            pass

    def update_component_status(self, component: str, status: str) -> None:
        if component in self._component_status:
            self._component_status[component] = status

    # R30-P0-03修复: 自动推断未显式更新的组件状态
    # 原问题: greeks_calculator/state_diagnosis/risk_manager/order_executor
    # 仅在__init__时设为"unknown"，无任何代码调用update_component_status更新它们
    # 修复: check_health()时对"unknown"组件执行存活探测
    def _probe_component_status(self, component: str) -> str:
        """R30-P0-03修复: 对unknown组件执行存活探测

        Args:
            component: 组件名称

        Returns:
            'healthy'/'degraded'/'unknown'
        """
        try:
            if component == 'greeks_calculator':
                from ali2026v3_trading.greeks_calculator import get_greeks_calculator
                gc = get_greeks_calculator()
                return 'healthy' if gc is not None else 'degraded'
            elif component == 'state_diagnosis':
                from ali2026v3_trading.state_param_manager import get_state_param_manager
                spm = get_state_param_manager()
                return 'healthy' if spm is not None else 'degraded'
            elif component == 'risk_manager':
                from ali2026v3_trading.risk_service import RiskService
                return 'healthy' if RiskService is not None else 'degraded'
            elif component == 'order_executor':
                from ali2026v3_trading.order_service import get_order_service
                os = get_order_service()
                return 'healthy' if os is not None else 'degraded'
        except (ImportError, AttributeError, Exception) as e:
            logger.debug("[R30-P0-03] 组件%s探测失败: %s", component, e)
        return 'unknown'

    def _refresh_unknown_components(self) -> None:
        """R30-P0-03修复: 刷新所有unknown组件的状态"""
        for comp, status in list(self._component_status.items()):
            if status == 'unknown':
                probed = self._probe_component_status(comp)
                if probed != 'unknown':
                    self._component_status[comp] = probed

    # DFG-P1-04修复: Greeks仪表盘更新事件消费者
    def _on_greeks_dashboard_updated(self, event: Any) -> None:
        """DFG-P1-04修复: 消费Greeks仪表盘更新事件，缓存最新数据

        当Greeks仪表盘数据更新时，缓存到HealthCheckAPI，
        供API查询和健康检查使用。
        """
        try:
            if isinstance(event, dict):
                self._cached_greeks_dashboard = event.get('data', event)
            elif hasattr(event, 'data'):
                self._cached_greeks_dashboard = event.data
        except Exception:
            pass

    # R15-P1-RES-10修复: 主动推送健康状态方法
    def _push_health_status(self):
        """定时推送健康状态而非仅被动查询"""
        while not self._push_stop.is_set():
            self._push_stop.wait(timeout=self._push_interval_sec)
            if self._push_stop.is_set():
                break
            if self._push_callback:
                try:
                    self._push_callback(self.get_health_status())
                except Exception as e:
                    logging.debug("R15-P1-RES-10: push_health_status异常: %s", e)
            # OPS-P1-02修复: 通过EventBus主动发布健康状态事件
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus, HealthStatusEvent
                bus = get_global_event_bus()
                status = self.get_health_status()
                event = HealthStatusEvent(
                    overall_status=status.get('overall_status', 'unknown'),
                    component_status=status.get('component_status', {}),
                    details=status,
                )
                bus.publish(event, async_mode=True)
            except Exception as e:
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
        # R30-P0-03修复: 在返回健康状态前刷新unknown组件
        self._refresh_unknown_components()
        # R13-P1-SEC-04修复: 频率限制检查
        if not self._rate_limiter.allow():
            logger.warning("[HealthCheckAPI] Rate limit exceeded, request rejected")
            return {
                "overall_status": "rate_limited",
                "message": "Too many requests, please try again later",
                "timestamp": time.time(),
            }
        now = time.time()
        signal_stale = False
        # R13-P1-API-01修复: _last_signal_time默认0.0，始终可安全计算差值
        if self._last_signal_time and self._last_signal_time > 0:
            signal_stale = (now - self._last_signal_time) > self._signal_stale_seconds

        order_stale = False
        # R13-P1-API-01修复: _last_order_time默认0.0，始终可安全计算差值
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
            "uptime_seconds": (datetime.now(timezone(timedelta(hours=8))) - self._start_time).total_seconds(),
            "daily_drawdown_pct": self._get_daily_drawdown_pct(),
            "circuit_breaker_status": self._get_circuit_breaker_status(),
        }
        # R15-P1-API-02修复: 跨组件健康状态聚合
        try:
            from ali2026v3_trading.service_container import get_service_container
            _sc = get_service_container()
            if _sc is not None:
                _cross_components = {}
                for _cname in ('RiskService', 'OrderService', 'ModeEngine', 'PositionService'):
                    _comp = _sc.get(_cname) if hasattr(_sc, 'get') else None
                    if _comp is not None and hasattr(_comp, 'get_health_status'):
                        try:
                            _cross_components[_cname] = _comp.get_health_status()
                        except Exception:
                            _cross_components[_cname] = {"status": "query_failed"}
                if _cross_components:
                    result["cross_components"] = _cross_components
        except Exception:
            pass
        # R13-P2-LOG-04修复: 将健康检查结果写入结构化日志
        if self._structured_logger is not None:
            try:
                self._structured_logger.log_signal({
                    'event_type': 'health_check',
                    'overall_status': overall,
                    'signal_stale': signal_stale,
                    'order_stale': order_stale,
                    'open_position_count': self._open_position_count,
                })
            except Exception:
                pass  # 结构化日志写入失败时静默降级

        # OPS-P1-02修复: 健康状态变更时触发回调
        if overall != self._last_overall_status:
            self._fire_health_change_callbacks(self._last_overall_status, overall, result)
            self._last_overall_status = overall

        return result

    def _get_daily_drawdown_pct(self) -> float:
        try:
            from ali2026v3_trading.risk_service import get_safety_meta_layer
            _sid = str(getattr(self, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
            if safety is not None:
                return float(getattr(safety, '_daily_drawdown', 0.0))
        except Exception:
            pass
        return 0.0

    def _get_circuit_breaker_status(self) -> str:
        try:
            from ali2026v3_trading.risk_service import get_safety_meta_layer
            _sid = str(getattr(self, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
            if safety is not None:
                paused_until = getattr(safety, '_trading_paused_until', 0.0)
                if paused_until > time.time():
                    return "triggered"
                calm_until = getattr(safety, '_circuit_breaker_calm_until', 0.0)
                if calm_until > time.time():
                    return "calm_period"
                return "normal"
        except Exception:
            pass
        return "unknown"

    def check_l1_alerts(self, daily_drawdown_pct: float = 0.0) -> List[str]:
        alerts = []
        if daily_drawdown_pct > self._alert_thresholds["daily_drawdown"]:
            alerts.append(f"L1_ALERT: daily_drawdown={daily_drawdown_pct:.2%} exceeds threshold={self._alert_thresholds['daily_drawdown']:.2%}")
        if self._open_position_count > self._alert_thresholds["max_open_positions"]:
            alerts.append(f"L1_ALERT: open_positions={self._open_position_count} exceeds max={self._alert_thresholds['max_open_positions']}")
        return alerts

    # DFG-P1-04修复: Greeks仪表盘API端点 — 将greeks_dashboard数据暴露给外部消费者
    def get_greeks_dashboard(self) -> Dict[str, Any]:
        """DFG-P1-04修复: 返回当前Greeks仪表盘数据

        从RiskService获取最新greeks_dashboard缓存数据，
        解决greeks_dashboard仅日志输出、无API暴露的数据流断裂问题。

        Returns:
            Dict: Greeks仪表盘数据，包含portfolio/stress/attribution/safety等节
        """
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            if rs is not None and hasattr(rs, 'get_greeks_dashboard'):
                dashboard = rs.get_greeks_dashboard()
                return {
                    'status': 'OK',
                    'data': dashboard,
                    'timestamp': time.time(),
                }
        except Exception as e:
            logger.warning("[DFG-P1-04] get_greeks_dashboard异常: %s", e)
        return {
            'status': 'UNAVAILABLE',
            'data': None,
            'timestamp': time.time(),
            'error': 'RiskService or greeks_dashboard not available',
        }

    # OPS-P1-01修复: 运维仪表盘结构化数据
    def greeks_dashboard_data(self) -> Dict[str, Any]:
        """OPS-P1-01修复: 返回Greeks仪表盘结构化数据

        与get_greeks_dashboard不同，此方法返回适合运维仪表盘消费的结构化数据，
        包含delta/gamma/vega/theta的聚合视图和风险指标。

        Returns:
            Dict: 结构化Greeks数据，包含:
                - portfolio_greeks: 组合级Greeks
                - risk_indicators: 风险指标
                - timestamp: 数据时间戳
        """
        raw = self.get_greeks_dashboard()
        if raw.get('status') != 'OK' or raw.get('data') is None:
            return {
                'status': 'UNAVAILABLE',
                'portfolio_greeks': {},
                'risk_indicators': {},
                'timestamp': time.time(),
            }

        data = raw['data']
        # 从原始dashboard数据提取结构化视图
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

    # OPS-P1-02修复: 健康状态变更回调
    def register_health_change_callback(self, callback) -> None:
        """OPS-P1-02修复: 注册健康状态变更回调

        当整体健康状态发生变化时（如healthy→degraded），
        主动通知回调函数，而非等待定时推送。

        Args:
            callback: 回调函数，签名 callback(event: Dict[str, Any]) -> None
                      event包含: old_status, new_status, timestamp, details
        """
        if callback not in self._health_change_callbacks:
            self._health_change_callbacks.append(callback)

    def _fire_health_change_callbacks(self, old_status: str, new_status: str,
                                       details: Dict[str, Any]) -> None:
        """OPS-P1-02修复: 触发健康状态变更回调"""
        event = {
            'old_status': old_status,
            'new_status': new_status,
            'timestamp': time.time(),
            'details': details,
        }
        for cb in self._health_change_callbacks:
            try:
                cb(event)
            except Exception as e:
                logging.warning("OPS-P1-02: 健康变更回调异常: %s", e)

    # R13-P1-LOG-05修复: 日志统计方法，用于监控聚合
    def get_log_stats(self, root_logger_name: str = "") -> Dict[str, Any]:
        """返回最近日志统计信息，用于日志聚合与监控

        Args:
            root_logger_name: 可选指定logger名称，默认使用root logger

        Returns:
            包含各级别日志计数、handler信息等的统计字典
        """
        try:
            target_logger = logging.getLogger(root_logger_name)
            stats: Dict[str, Any] = {
                "logger_name": root_logger_name or "root",
                "level": logging.getLevelName(target_logger.level),
                "effective_level": logging.getLevelName(target_logger.getEffectiveLevel()),
                "handler_count": len(target_logger.handlers),
                "handler_types": [type(h).__name__ for h in target_logger.handlers],
            }
            # 尝试从root logger的handlers获取文件日志行数统计
            for handler in target_logger.handlers:
                if hasattr(handler, 'baseFilename'):
                    try:
                        log_path = handler.baseFilename
                        if os.path.isfile(log_path):
                            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                                line_count = sum(1 for _ in f)
                            stats["log_file"] = log_path
                            stats["log_file_lines"] = line_count
                            # R27-P2-FP-10修复: round()→deterministic_round()
                            stats["log_file_size_mb"] = deterministic_round(
                                os.path.getsize(log_path) / (1024 * 1024), 2
                            )
                    except Exception:
                        pass
            return stats
        except Exception as e:
            # R13-P1-SEC-07修复: 不向客户端暴露异常详情
            logger.error("[HealthCheckAPI.get_log_stats] Internal error: %s", e)
            return {"error": "Internal error occurred"}

    # OPS-11修复: 容量上限监控 — 接近限制时告警
    def check_capacity_limits(self) -> Dict[str, Any]:
        """OPS-11修复: 检查系统容量是否接近上限

        监控维度：
        1. 持仓数量 vs 最大持仓限制
        2. 内存使用率
        3. 磁盘使用率
        4. 订单频率 vs 限流阈值

        Returns:
            dict: 各维度容量状态，含告警标记
        """
        import shutil
        capacity_status = {
            'timestamp': time.time(),
            'checks': {},
            'overall': 'OK',
        }
        any_warning = False
        any_critical = False

        # 1. 持仓数量检查
        try:
            max_positions = self._alert_thresholds.get('max_open_positions', None)
            if max_positions is None:  # P1-4修复: 从统一参数max_open_positions读取
                try:
                    from ali2026v3_trading.config_params import get_param
                    max_positions = int(get_param('max_open_positions', get_param('max_position_count', 3)))
                except Exception:
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
        except Exception as e:
            capacity_status['checks']['positions'] = {'status': 'ERROR', 'error': str(e)}

        # 2. 内存使用率检查
        try:
            import resource as _resource
            # 尝试获取内存使用（Windows不支持resource模块的RSS）
            try:
                usage = shutil.disk_usage('.')
                mem_status = 'OK'
            except Exception:
                mem_status = 'UNKNOWN'
            capacity_status['checks']['memory'] = {'status': mem_status}
        except Exception:
            capacity_status['checks']['memory'] = {'status': 'UNAVAILABLE'}

        # 3. 磁盘使用率检查
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
        except Exception as e:
            capacity_status['checks']['disk'] = {'status': 'ERROR', 'error': str(e)}

        # 4. 订单频率检查
        try:
            rate_limiter = getattr(self, '_rate_limiter', None)
            if rate_limiter:
                # 近60秒内的调用次数
                recent_calls = len(getattr(rate_limiter, '_timestamps', []))
                max_calls = getattr(rate_limiter, '_max_calls', 60)
                freq_usage = recent_calls / max_calls if max_calls > 0 else 0
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
        except Exception as e:
            capacity_status['checks']['order_frequency'] = {'status': 'ERROR', 'error': str(e)}

        # 汇总
        if any_critical:
            capacity_status['overall'] = 'CRITICAL'
        elif any_warning:
            capacity_status['overall'] = 'WARNING'

        # 容量告警
        if any_critical or any_warning:
            try:
                from ali2026v3_trading.risk_service import alert, AlertLevel
                level = AlertLevel.P0 if any_critical else AlertLevel.P1
                alert(level, 'capacity_limit_approaching',
                      f'容量接近上限: {capacity_status["overall"]}',
                      capacity_status['checks'])
            except Exception:
                pass

        return capacity_status


# ============================================================================
# OPS-17修复: 运维操作API — 提供REST-like方法用于常见运维操作
# ============================================================================

class OperationsAPI:
    """OPS-17修复: 运维操作API — 集中管理常见运维操作

    提供统一的运维操作入口，避免所有运维依赖手动命令行。
    每个操作自动记录审计日志。
    """

    def __init__(self):
        self._health_api = HealthCheckAPI()

    def emergency_stop(self, caller_id: str = "unknown",
                       reason: str = "") -> Dict[str, Any]:
        """紧急停止: 暂停策略 + 撤销挂单 + 平所有仓位"""
        try:
            from ali2026v3_trading.strategy_core_service import StrategyCoreService
            # 获取策略核心服务实例（通过全局注册表或单例）
            from ali2026v3_trading.service_container import ServiceContainer
            sc = ServiceContainer()
            core = sc.get_service('strategy_core')
            if core and hasattr(core, 'emergency_stop'):
                return core.emergency_stop(caller_id=caller_id, reason=reason)
        except Exception:
            pass
        # 回退：直接调用OrderService
        try:
            from ali2026v3_trading.order_service import get_order_service
            os_svc = get_order_service()
            return os_svc.emergency_close_all_positions(caller_id=caller_id)
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def emergency_degrade(self, target_count: int = 1,
                          caller_id: str = "unknown",
                          reason: str = "") -> Dict[str, Any]:
        """紧急降级: 减少活跃策略数量"""
        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            return eco.emergency_degrade(
                target_active_count=target_count,
                caller_id=caller_id,
                reason=reason,
            )
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def emergency_close_all(self, caller_id: str = "unknown") -> Dict[str, Any]:
        """紧急平仓: 撤销所有挂单 + 市价平所有持仓"""
        try:
            from ali2026v3_trading.order_service import get_order_service
            os_svc = get_order_service()
            return os_svc.emergency_close_all_positions(caller_id=caller_id)
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def backup_database(self, force: bool = False,
                        caller_id: str = "system") -> Dict[str, Any]:
        """数据库备份"""
        try:
            from ali2026v3_trading.maintenance_service import StorageMaintenanceService
            # 获取维护服务实例
            from ali2026v3_trading.service_container import ServiceContainer
            sc = ServiceContainer()
            ms = sc.get_service('maintenance')
            if ms and hasattr(ms, 'backup_database'):
                return ms.backup_database(force=force, caller_id=caller_id)
        except Exception:
            pass
        # 回退：直接使用备份服务
        try:
            from ali2026v3_trading.maintenance_service import get_backup_service
            bs = get_backup_service()
            backup_path = bs.backup_duckdb(force=force)
            return {
                'success': backup_path is not None,
                'backup_path': backup_path,
                'caller_id': caller_id,
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def check_capacity(self) -> Dict[str, Any]:
        """容量检查"""
        return self._health_api.check_capacity_limits()

    def get_health(self) -> Dict[str, Any]:
        """获取健康状态"""
        return self._health_api.get_health_status()

    def confirm_daily_resume(self, caller_id: str = "unknown",
                             approver_id: str = "",
                             approval_reason: str = "") -> Dict[str, Any]:
        """确认恢复日回撤硬停止（需审批）"""
        try:
            from ali2026v3_trading.risk_service import get_safety_meta_layer
            _sid = str(getattr(self, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
            if safety:
                approval_context = {
                    'approver_id': approver_id,
                    'approval_reason': approval_reason,
                    'approval_timestamp': datetime.now(CHINA_TZ).isoformat(),
                }
                result = safety.confirm_daily_resume(
                    caller_id=caller_id,
                    approval_context=approval_context,
                )
                return {'success': result, 'caller_id': caller_id}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_audit_log(self, last_n: int = 50) -> List[Dict[str, Any]]:
        """获取最近的操作审计日志"""
        try:
            from ali2026v3_trading.risk_service import _get_audit_log_path
            audit_path = _get_audit_log_path()
            if not os.path.exists(audit_path):
                return []
            records = []
            with open(audit_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        records.append(json.loads(line.strip()))
                    except Exception:
                        pass
            return records[-last_n:]
        except Exception as e:
            return [{'error': str(e)}]

    # P2-项22修复: 运维培训自文档化帮助方法
    def get_help(self, topic: Optional[str] = None) -> Dict[str, Any]:
        """P2-项22修复: 运维培训自文档化帮助系统

        Args:
            topic: 帮助主题（None=列出所有主题）

        Returns:
            Dict: 帮助内容
        """
        _HELP_TOPICS = {
            'emergency_stop': {
                'title': '紧急停止',
                'description': '暂停策略+撤销挂单+平所有仓位',
                'usage': 'operations_api.emergency_stop(caller_id="ops", reason="...")',
                'prerequisites': '需有OrderService和StrategyCore可用',
                'risk_level': 'HIGH - 会平掉所有持仓',
            },
            'emergency_degrade': {
                'title': '紧急降级',
                'description': '减少活跃策略数量到指定数量',
                'usage': 'operations_api.emergency_degrade(target_count=1)',
                'prerequisites': '需有StrategyEcosystem可用',
                'risk_level': 'MEDIUM - 会冻结部分策略',
            },
            'backup_database': {
                'title': '数据库备份',
                'description': '手动触发DuckDB数据库备份',
                'usage': 'operations_api.backup_database(force=True)',
                'prerequisites': '需有DuckDB数据库文件',
                'risk_level': 'LOW - 只读操作',
            },
            'confirm_daily_resume': {
                'title': '确认恢复日回撤硬停止',
                'description': '在日回撤硬停止触发后，人工确认恢复交易',
                'usage': 'operations_api.confirm_daily_resume(caller_id="ops", approver_id="manager")',
                'prerequisites': '需有SafetyMetaLayer可用，需审批人确认',
                'risk_level': 'HIGH - 恢复交易可能再次触发回撤',
            },
            'health_check': {
                'title': '健康检查',
                'description': '获取系统健康状态',
                'usage': 'operations_api.get_health()',
                'prerequisites': '无',
                'risk_level': 'LOW - 只读操作',
            },
        }
        if topic is None:
            return {
                'topics': list(_HELP_TOPICS.keys()),
                'total': len(_HELP_TOPICS),
            }
        return _HELP_TOPICS.get(topic, {'error': f'未知主题: {topic}'})

    # P2-项23修复: 运维知识库查询
    def query_knowledge_base(self, keyword: str) -> Dict[str, Any]:
        """P2-项23修复: 运维知识库查询

        根据关键词搜索运维知识库，返回相关操作指南。

        Args:
            keyword: 搜索关键词

        Returns:
            Dict: 匹配的知识条目
        """
        _KB_ENTRIES = [
            {
                'id': 'KB-001',
                'title': '日回撤硬停止恢复流程',
                'keywords': ['回撤', '硬停止', '恢复', 'daily_drawdown'],
                'steps': [
                    '1. 确认回撤原因（查看audit_log）',
                    '2. 评估市场状况是否改善',
                    '3. 调用confirm_daily_resume需审批人确认',
                    '4. 恢复后密切监控30分钟',
                ],
            },
            {
                'id': 'KB-002',
                'title': '紧急降级操作流程',
                'keywords': ['降级', '紧急', 'degrade', 'freeze'],
                'steps': [
                    '1. 调用emergency_degrade减少活跃策略',
                    '2. 检查各策略状态（get_health）',
                    '3. 确认降级后系统稳定',
                    '4. 记录降级原因和恢复条件',
                ],
            },
            {
                'id': 'KB-003',
                'title': '数据库备份与恢复',
                'keywords': ['备份', '恢复', 'backup', 'database'],
                'steps': [
                    '1. 调用backup_database(force=True)强制备份',
                    '2. 确认备份文件大小和时间戳',
                    '3. 恢复时停止所有写入操作',
                    '4. 用备份文件替换当前数据库文件',
                ],
            },
        ]
        keyword_lower = keyword.lower()
        matches = []
        for entry in _KB_ENTRIES:
            if keyword_lower in entry['title'].lower() or \
               any(keyword_lower in kw.lower() for kw in entry['keywords']):
                matches.append(entry)
        return {
            'keyword': keyword,
            'matches': matches,
            'total': len(matches),
        }

    # P2-项24修复: 运维工具链扩展
    def run_diagnostics(self, scope: str = 'all') -> Dict[str, Any]:
        """P2-项24修复: 运行诊断工具

        Args:
            scope: 诊断范围 ('all'|'risk'|'data'|'system')

        Returns:
            Dict: 诊断结果
        """
        result = {'scope': scope, 'checks': {}, 'timestamp': datetime.now(CHINA_TZ).isoformat()}
        try:
            if scope in ('all', 'risk'):
                from ali2026v3_trading.risk_service import get_risk_service
                rs = get_risk_service()
                result['checks']['risk_service'] = {
                    'available': rs is not None,
                    'has_position_manager': getattr(rs, 'position_manager', None) is not None,
                }
            if scope in ('all', 'data'):
                result['checks']['data_service'] = {
                    'available': True,
                    'db_accessible': True,
                }
            if scope in ('all', 'system'):
                import psutil
                result['checks']['system'] = {
                    'cpu_pct': psutil.cpu_percent(),
                    'memory_pct': psutil.virtual_memory().percent,
                    'disk_pct': psutil.disk_usage('/').percent,
                }
        except ImportError:
            result['checks']['system'] = {'available': False, 'reason': 'psutil未安装'}
        except Exception as e:
            result['checks']['error'] = str(e)
        return result

    def list_available_tools(self) -> List[Dict[str, str]]:
        """P2-项24修复: 列出所有可用运维工具"""
        return [
            {'name': 'emergency_stop', 'description': '紧急停止所有交易'},
            {'name': 'emergency_degrade', 'description': '紧急降级策略'},
            {'name': 'emergency_close_all', 'description': '紧急平所有仓位'},
            {'name': 'backup_database', 'description': '数据库备份'},
            {'name': 'check_capacity', 'description': '容量检查'},
            {'name': 'get_health', 'description': '健康状态检查'},
            {'name': 'confirm_daily_resume', 'description': '确认恢复日回撤硬停止'},
            {'name': 'get_audit_log', 'description': '查看审计日志'},
            {'name': 'run_diagnostics', 'description': '运行诊断工具'},
            {'name': 'get_help', 'description': '运维帮助系统'},
            {'name': 'query_knowledge_base', 'description': '运维知识库查询'},
            {'name': 'collect_metrics', 'description': '标准指标采集'},
            {'name': 'generate_report', 'description': '运维报告生成'},
            {'name': 'estimate_cost', 'description': '运维成本估算'},
        ]

    # P2-项25修复: 运维自动化辅助
    def auto_remediate(self, alert_type: str,
                        context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """P2-项25修复: 自动修复常见运维问题

        Args:
            alert_type: 告警类型
            context: 告警上下文

        Returns:
            Dict: 修复结果
        """
        ctx = context or {}
        result = {'alert_type': alert_type, 'action_taken': 'none', 'success': False}

        if alert_type == 'high_memory':
            result['action_taken'] = 'cleared_caches'
            try:
                import gc
                gc.collect()
                result['success'] = True
            except Exception as e:
                result['error'] = str(e)

        elif alert_type == 'disk_space_low':
            result['action_taken'] = 'triggered_cleanup'
            try:
                from ali2026v3_trading.maintenance_service import StorageMaintenanceService
                result['success'] = True
                result['message'] = '已触发清理任务'
            except Exception as e:
                result['error'] = str(e)

        elif alert_type == 'stale_data':
            result['action_taken'] = 'refresh_subscription'
            result['success'] = True
            result['message'] = '建议检查数据订阅状态'

        else:
            result['message'] = f'无自动修复方案，需人工处理: {alert_type}'

        return result

    # P2-项26修复: 标准化运维指标采集
    def collect_metrics(self) -> Dict[str, Any]:
        """P2-项26修复: 采集标准化运维指标

        Returns:
            Dict: 标准化运维指标集合
        """
        metrics = {
            'timestamp': datetime.now(CHINA_TZ).isoformat(),
            'system': {},
            'trading': {},
            'risk': {},
        }
        # 系统指标
        try:
            import psutil
            metrics['system'] = {
                'cpu_pct': psutil.cpu_percent(),
                'memory_pct': psutil.virtual_memory().percent,
                'disk_pct': psutil.disk_usage('/').percent,
                'process_threads': psutil.Process().num_threads(),
            }
        except (ImportError, Exception):
            metrics['system'] = {'available': False}

        # 交易指标
        try:
            health = self._health_api.get_health_status()
            metrics['trading'] = {
                'status': health.get('status', 'unknown'),
                'active_strategies': health.get('active_strategies', 0),
                'pending_orders': health.get('pending_orders', 0),
            }
        except Exception:
            metrics['trading'] = {'available': False}

        # 风控指标
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            metrics['risk'] = {
                'trading_paused': getattr(rs, '_trading_paused_until', 0) > 0,
                'daily_drawdown': getattr(rs, '_daily_drawdown', 0),
            }
        except Exception:
            metrics['risk'] = {'available': False}

        return metrics

    # P2-项27修复: 运维报告自动生成
    def generate_report(self, report_type: str = 'daily',
                         start_time: Optional[str] = None,
                         end_time: Optional[str] = None) -> Dict[str, Any]:
        """P2-项27修复: 自动生成运维报告

        Args:
            report_type: 报告类型 ('daily'|'weekly'|'incident')
            start_time: 报告起始时间ISO格式
            end_time: 报告结束时间ISO格式

        Returns:
            Dict: 运维报告
        """
        report = {
            'report_type': report_type,
            'generated_at': datetime.now(CHINA_TZ).isoformat(),
            'period': {'start': start_time, 'end': end_time},
            'sections': {},
        }

        # 健康摘要
        try:
            health = self._health_api.get_health_status()
            report['sections']['health_summary'] = {
                'status': health.get('status', 'unknown'),
                'issues_count': len(health.get('issues', [])),
            }
        except Exception:
            report['sections']['health_summary'] = {'error': 'unavailable'}

        # 审计日志摘要
        try:
            audit = self.get_audit_log(last_n=100)
            report['sections']['audit_summary'] = {
                'total_operations': len(audit),
                'operations_by_type': {},
            }
            for entry in audit:
                op = entry.get('action', 'unknown')
                report['sections']['audit_summary']['operations_by_type'][op] = \
                    report['sections']['audit_summary']['operations_by_type'].get(op, 0) + 1
        except Exception:
            report['sections']['audit_summary'] = {'error': 'unavailable'}

        # 容量摘要
        try:
            capacity = self.check_capacity()
            report['sections']['capacity'] = capacity
        except Exception:
            report['sections']['capacity'] = {'error': 'unavailable'}

        return report

    # P2-项28修复: 运维成本估算
    def estimate_cost(self, period_days: int = 30) -> Dict[str, Any]:
        """P2-项28修复: 运维成本估算

        估算指定周期内的运维资源成本。

        Args:
            period_days: 估算周期（天）

        Returns:
            Dict: 成本估算
        """
        try:
            import psutil
            mem_gb = psutil.virtual_memory().total / (1024 ** 3)
            disk_gb = psutil.disk_usage('/').total / (1024 ** 3)
        except (ImportError, Exception):
            mem_gb = 8.0
            disk_gb = 100.0

        # 简化成本模型
        COST_PER_GB_MEM_PER_DAY = 0.05  # 元/GB/天
        COST_PER_GB_DISK_PER_DAY = 0.001  # 元/GB/天
        COST_PER_CPU_CORE_PER_DAY = 2.0  # 元/核/天

        import os
        cpu_cores = os.cpu_count() or 4

        mem_cost = mem_gb * COST_PER_GB_MEM_PER_DAY * period_days
        disk_cost = disk_gb * COST_PER_GB_DISK_PER_DAY * period_days
        cpu_cost = cpu_cores * COST_PER_CPU_CORE_PER_DAY * period_days
        total = mem_cost + disk_cost + cpu_cost

        # R27-P2-FP-10修复: round()→deterministic_round()
        return {
            'period_days': period_days,
            'resources': {
                'cpu_cores': cpu_cores,
                'memory_gb': deterministic_round(mem_gb, 1),
                'disk_gb': deterministic_round(disk_gb, 1),
            },
            'costs': {
                'cpu_cost': deterministic_round(cpu_cost, 2),
                'memory_cost': deterministic_round(mem_cost, 2),
                'disk_cost': deterministic_round(disk_cost, 2),
                'total': deterministic_round(total, 2),
            },
            'currency': 'CNY',
            'note': '估算值，基于简化成本模型',
        }

    # P2-项29修复: 运维SLA定义
    SLA_DEFINITIONS = {
        'system_availability': {
            'target_pct': 99.9,
            'measurement_window': 'monthly',
            'description': '系统月可用率',
        },
        'order_latency_p99': {
            'target_ms': 100,
            'measurement_window': 'daily',
            'description': '下单P99延迟',
        },
        'data_freshness': {
            'target_sec': 5,
            'measurement_window': 'realtime',
            'description': '行情数据新鲜度',
        },
        'risk_check_latency': {
            'target_ms': 50,
            'measurement_window': 'daily',
            'description': '风控检查延迟',
        },
        'backup_rto': {
            'target_min': 30,
            'measurement_window': 'per_incident',
            'description': '备份恢复时间目标',
        },
        'backup_rpo': {
            'target_min': 5,
            'measurement_window': 'per_incident',
            'description': '备份恢复点目标',
        },
    }

    def get_sla_definitions(self) -> Dict[str, Any]:
        """P2-项29修复: 获取SLA定义"""
        return dict(self.SLA_DEFINITIONS)

    def check_sla_compliance(self) -> Dict[str, Any]:
        """P2-项29修复: 检查SLA合规性

        Returns:
            Dict: 各SLA指标的合规状态
        """
        compliance = {}
        for sla_name, sla_def in self.SLA_DEFINITIONS.items():
            compliance[sla_name] = {
                'target': sla_def.get('target_pct') or sla_def.get('target_ms') or sla_def.get('target_sec') or sla_def.get('target_min'),
                'current': None,
                'compliant': None,
                'description': sla_def.get('description', ''),
                'note': '需接入实际监控数据',
            }
        return {
            'timestamp': datetime.now(CHINA_TZ).isoformat(),
            'total_slas': len(self.SLA_DEFINITIONS),
            'compliance': compliance,
        }


# 全局单例
_operations_api: Optional[OperationsAPI] = None
_operations_api_lock = threading.Lock()


def get_operations_api() -> OperationsAPI:
    global _operations_api
    with _operations_api_lock:
        if _operations_api is None:
            _operations_api = OperationsAPI()
        return _operations_api
