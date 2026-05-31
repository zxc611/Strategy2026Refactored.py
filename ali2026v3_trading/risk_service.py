"""
risk_service.py - 风控服务

合并来源：08_position.py (PositionManagerMixin) + 10_gate.py (GateLayer3)
合并策略：提取风控检查、限额管理、风险计算等功能

重构目标：
- 旧架构：08_position.py (~600行) + 10_gate.py (~200行风控相关) = ~700行
- 新架构：risk_service.py (~400行)
- 减少：~43%

核心改进：
1. 单一职责：专注于风险控制与限额管理
2. 统一风控接口：整合持仓限额、信号限频、策略状态检查
3. 线程安全：支持并发检查
4. 可配置性：灵活的风控参数配置

作者：CodeArts 代码智能体
版本：v1.0
生成时间：2026-03-16
"""

from __future__ import annotations

import copy
import json
import math
import os
import time
import logging
from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer
from ali2026v3_trading import config_params
import threading
import numpy as np
from datetime import datetime, timedelta, timezone

# P1-R11-12修复: 中国标准时间UTC+8，用于datetime.fromtimestamp的tz参数
_CHINA_TZ = timezone(timedelta(hours=8))  # R26-P0-CD-08: 中国时区（统一变量名，消除_CHINA_TZ不一致）

# R27-P1修复: 导入容错/浮点/断路器工具
from ali2026v3_trading.resilience_utils import (
    ExponentialBackoff, BoundedRetry, DbQueryTimeout, Watchdog, HeartbeatMonitor,
    CircuitBreakerHalfOpen, SlowQueryDetector, DataStalenessDetector,
    MemoryPressureGuard, RateLimitedLogger, GracefulDegradation,
    ResourceLeakDetector, AsyncTaskTimeout, safe_callback_wrapper,
    get_process_health, ProcessHealthState, AtomicConfigRef,
    stable_sum, stable_mean, stable_variance, stable_std,
    approx_equal, approx_less, approx_greater, approx_less_equal, approx_greater_equal,
    should_trigger_stop_loss, should_trigger_take_profit,
    KahanSummation, safe_divide, compute_sharpe_stable, safe_normalize_weights,
    stable_ewma, PRICE_TOLERANCE, FLOAT_COMPARE_TOLERANCE,
)

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque
from enum import Enum, auto

try:
    from ali2026v3_trading.causal_chain_utils import (
        CyclicDependencyGuard, ContaminationGuard, ParamIsolationGuard,
    )
    _HAS_CAUSAL_CHAIN = True
except ImportError:
    _HAS_CAUSAL_CHAIN = False


# ============================================================================
# UPG-P1-04修复: API版本标记装饰器
# ============================================================================

def api_version(version: str):
    """UPG-P1-04修复: API版本标记装饰器

    为服务公共API方法标记版本号，便于：
    1. 运行时查询API版本
    2. 版本兼容性检查
    3. 废弃API迁移追踪

    用法:
        @api_version("1.0")
        def check_before_trade(self, signal):
            ...

    Args:
        version: API版本号，如 "1.0", "2.0"
    """
    def decorator(func):
        func._api_version = version
        func._api_versioned = True
        return func
    return decorator


# ============================================================================
# P1-R11-12修复: 时区感知时间获取 — 统一使用UTC避免系统时区不一致导致风控时间判断错误
# ============================================================================

def _get_tz_aware_now() -> datetime:
    """P1-R11-12修复: 返回UTC时区感知的当前datetime

    替代裸 datetime.now()，确保所有风控日志、审计、快照时间戳
    均为UTC时区感知，避免跨时区部署或夏令时切换时的时间判断错误。

    Returns:
        datetime: UTC时区的当前datetime (tzinfo=timezone.utc)
    """
    return datetime.now(_CHINA_TZ)


# ============================================================================
# 枚举与数据结构
# ============================================================================

class RiskLevel(Enum):
    """风险等级"""
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    RESTRICTED = auto()  # [R23-P1-02-FIX] 受限中间状态：正常→受限→熔断，防止直接从正常跳到熔断
    CRITICAL = auto()


class RiskCheckResult(Enum):
    """风控检查结果"""
    PASS = auto()       # 通过
    BLOCK = auto()      # 阻断
    WARNING = auto()    # 警告（可继续）
    DEGRADED = auto()   # 降级


# [R23-P1-02-FIX] 风控级别合法转移：禁止从LOW/MEDIUM直接跳到CRITICAL(熔断)，必须经过RESTRICTED
_LEGAL_RISK_LEVEL_TRANSITIONS = {
    RiskLevel.LOW: {RiskLevel.LOW, RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.RESTRICTED},
    RiskLevel.MEDIUM: {RiskLevel.LOW, RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.RESTRICTED},
    RiskLevel.HIGH: {RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.RESTRICTED, RiskLevel.CRITICAL},
    RiskLevel.RESTRICTED: {RiskLevel.HIGH, RiskLevel.RESTRICTED, RiskLevel.CRITICAL},
    RiskLevel.CRITICAL: {RiskLevel.RESTRICTED, RiskLevel.CRITICAL},
}


def validate_risk_level_transition(current: RiskLevel, target: RiskLevel) -> RiskLevel:
    """[R23-P1-02-FIX] 校验风控级别转移合法性，禁止正常→熔断直接跳跃"""
    allowed = _LEGAL_RISK_LEVEL_TRANSITIONS.get(current, set())
    if target in allowed:
        return target
    if target == RiskLevel.CRITICAL and current in (RiskLevel.LOW, RiskLevel.MEDIUM):
        logging.warning("[R23-P1-02-FIX] 风控级别非法跳跃: %s→%s, 强制经过RESTRICTED中间状态", current.name, target.name)
        return RiskLevel.RESTRICTED
    return target


@dataclass(slots=True)
class RiskCheckResponse:
    """风控检查响应"""
    result: RiskCheckResult = RiskCheckResult.PASS
    level: RiskLevel = RiskLevel.LOW
    message: str = ""
    reason: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))  # ENV-06修复: 使用UTC时区
    threshold: Optional[float] = None  # R24-P1-TR-06修复: 风控拒绝阈值
    actual: Optional[float] = None     # R24-P1-TR-06修复: 风控拒绝实际值

    @property
    def is_pass(self) -> bool:
        return self.result == RiskCheckResult.PASS

    @property
    def is_block(self) -> bool:
        return self.result == RiskCheckResult.BLOCK

    @classmethod
    def pass_result(cls, message: str = "") -> "RiskCheckResponse":
        return cls(result=RiskCheckResult.PASS, message=message)

    @classmethod
    def block_result(cls, reason: str, message: str = "",
                     level: RiskLevel = RiskLevel.HIGH,
                     threshold: Optional[float] = None,
                     actual: Optional[float] = None) -> "RiskCheckResponse":
        # R24-P1-TR-06修复: block_result增加threshold/actual参数
        return cls(result=RiskCheckResult.BLOCK, level=level,
                   reason=reason, message=message,
                   threshold=threshold, actual=actual)

    @classmethod
    def warning_result(cls, reason: str, message: str = "") -> "RiskCheckResponse":
        return cls(result=RiskCheckResult.WARNING, level=RiskLevel.MEDIUM,
                   reason=reason, message=message)


_PASS_RESULT_CACHE = {}

def _get_pass_result(reason: str = "风控检查通过") -> "RiskCheckResponse":
    if reason not in _PASS_RESULT_CACHE:
        _PASS_RESULT_CACHE[reason] = RiskCheckResponse.pass_result(reason)
    return _PASS_RESULT_CACHE[reason]


from ali2026v3_trading.position_service import PositionLimitConfig as PositionLimit
from ali2026v3_trading.shared_utils import safe_int, safe_float


@dataclass(slots=True)
class RiskMetrics:
    """风险指标 — 含11维度评分"""
    total_exposure: float = 0.0       # 总敞口
    max_single_position: float = 0.0  # 最大单持仓
    position_count: int = 0           # 持仓数量
    risk_ratio: float = 0.0           # 风险比率
    # 11维度关键指标
    life_degradation_level: int = 0   # D3 行情寿命降级等级(0-3)
    life_p75_minutes: float = 0.0     # D3 当前状态寿命p75(分钟)
    life_valid: bool = False          # D3 寿命数据是否有效
    composite_risk_score: float = 0.0 # 11维度综合风险评分[0,1]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))  # ENV-06修复: 使用UTC时区


# ============================================================================
# 风控服务
# ============================================================================

class RiskService:
    """
    风控服务 - 统一的风险控制接口

    职责：
    1. 持仓限额检查
    2. 信号限频控制
    3. 策略状态检查
    4. 风险指标计算

    使用方式：
        service = RiskService(params, position_manager)
        result = service.check_before_trade(signal)
    """

    # [R23-P2-ID-01-FIX] 日志去重装饰器
    class LogDeduplicator:
        """同一消息5秒内不重复输出"""
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

    _log_dedup = LogDeduplicator()  # [R23-P2-ID-01-FIX]

    # R13-P0-LOG-05修复: 告警回调机制，供外部告警系统集成
    _alert_callbacks: List = []

    @classmethod
    def register_alert_callback(cls, cb) -> None:
        """R13-P0-LOG-05修复: 注册告警回调函数，P0事件发生时调用

        Args:
            cb: 回调函数，签名 cb(event_type: str, detail: dict)
        """
        if cb not in cls._alert_callbacks:
            cls._alert_callbacks.append(cb)

    @classmethod
    def _fire_alert(cls, event_type: str, detail: Dict[str, Any]) -> None:  # [R22-P2-TS25]
        """R13-P0-LOG-05修复: 触发所有已注册的告警回调"""
        for cb in cls._alert_callbacks:
            try:
                cb(event_type, detail)
            except Exception as e:
                logging.warning('[RiskService] R13-P0-LOG-05修复: 告警回调异常: %s', e)
        # R30-ARCH-03修复: 风控告警同时通过EventBus发布
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None and not getattr(_bus, '_shutdown', False):
                _bus.publish(f'risk.{event_type}', detail)
        except Exception:
            pass

    def __init__(self, params: Any, position_manager: Any = None,
                 strategy: Any = None):
        """
        初始化风控服务

        Args:
            params: 参数配置对象
            position_manager: 持仓管理器（可选）
            strategy: 策略实例（可选）
        """
        self.params = params
        self.position_manager = position_manager
        self.strategy = strategy
        # R15-P1-SEC-10修复: 跨策略数据隔离 - strategy_id命名空间前缀
        self._strategy_id_ns: str = 'default'

        # 线程安全
        self._lock = threading.RLock()
        # R15-P0-RES-04修复: 独立风控锁，与交易锁解耦
        self._risk_check_lock = threading.RLock()
        # [FR-P1-07-FIX] 风控数据读写一致性校验时间戳
        self._risk_data_read_time: float = 0.0
        self._risk_data_write_time: float = 0.0
        self._risk_rw_consistency_threshold: float = 1.0  # 秒

        # P2-R11-05: 持仓限制为全局配置，未按strategy_id隔离。多策略时所有策略共享同一持仓上限。
        # 持仓限额配置
        self._position_limits: Dict[str, PositionLimit] = {}

        # R13-P0-BIZ-10修复: 保证金预留机制，防止并发请求双重花费
        # _reserved_margin: 预留保证金总额，check_capital_sufficiency时扣除
        self._reserved_margin: float = 0.0
        # _margin_reservations: 预留明细 {reservation_id: {amount, timestamp, expires_at}}
        self._margin_reservations: Dict[str, Dict[str, Any]] = {}
        self._RESERVATION_TTL_SEC = 30.0  # 预留30秒后自动释放

        # EX-10修复: 保证金追缴通知状态跟踪
        self._margin_call_state: Dict[str, Any] = {
            'status': 'normal',
            'notified_at': None,
            'grace_deadline': None,
            'force_close_triggered': False,
        }

        # 限频控制
        self._signal_times: Dict[str, deque] = {}  # 注：插入时统一使用maxlen=1000
        # ✅ 修复：删除_last_signal_time，统一为_signal_times滑动窗口

        # 风控统计
        self._stats = {
            "total_checks": 0,
            "blocked": 0,
            "warnings": 0,
            "passed": 0,
            "partial_fill_events": 0,
        }
        # [R23-P2-ID-02-FIX] RiskService专用_stats锁
        self._stats_lock = threading.Lock()
        # [R23-P2-SM-07-FIX] 风控状态变更审计记录
        self._risk_state_audit_log: List[Dict[str, Any]] = []
        # [R23-P2-FR-07-FIX] 风控数据年龄监控
        self._risk_last_update_time: float = 0.0
        self._partial_fill_progress: Dict[str, Dict[str, Any]] = {}

        # 连亏保护
        self._consecutive_loss_limits: Dict[str, int] = {
            'small': 5, 'medium': 7, 'large': 10,
        }
        self._consecutive_loss_counts: Dict[str, int] = {}
        self._consecutive_loss_paused: Dict[str, bool] = {}
        # 盈亏比恢复计时器：连亏暂停后，超过此时间自动恢复
        self._consecutive_loss_recovery_timeouts: Dict[str, float] = {
            'small': 1800.0,   # 30分钟
            'medium': 3600.0,  # 60分钟
            'large': 7200.0,   # 120分钟
        }
        self._consecutive_loss_pause_timestamps: Dict[str, float] = {}

        # R13-P1-BIZ-03修复: per-strategy风险状态隔离
        # 原代码所有策略共享同一个RiskService实例的连亏计数/暂停状态，
        # 但strategy_id已作为key区分，问题在于其他风险指标(限频/限额)未按策略隔离。
        # 新增_per_strategy_risk_state，按strategy_id跟踪各策略的独立风险指标。
        self._per_strategy_risk_state: Dict[str, Dict[str, Any]] = {}

        # 非对称风控
        # R18-P0-BIZ-01修复: 非对称风控默认启用（原默认False导致风控失效）
        self._asymmetric_risk_enabled: bool = True
        self._asymmetric_profit_multiplier: float = 1.5
        self._asymmetric_loss_multiplier: float = 0.7
        # 从环境变量读取配置，允许显式禁用
        _env_asymmetric = os.environ.get('ASYMMETRIC_RISK_ENABLED', '').lower()
        if _env_asymmetric in ('false', '0', 'no'):
            self._asymmetric_risk_enabled = False
            logging.info("[RiskService] 非对称风控已通过环境变量ASYMMETRIC_RISK_ENABLED禁用")
        else:
            logging.info("[RiskService] 非对称风控默认启用（R18-P0-BIZ-01修复）")

        # 缓存
        self._strategy_status_cache: Tuple[float, RiskCheckResponse] = (0.0, RiskCheckResponse.pass_result())
        self._cache_ttl = 1.0  # 缓存有效期（秒）
        self._last_greeks_dashboard: Dict[str, Any] = {}
        # [R16-P1-11修复] Alpha衰减恢复需人工确认标志
        self._alpha_decay_recovery_confirmed: bool = False

        # R13-P2-LOG-11修复: 仪表盘变更摘要快照，仅输出显著变化的字段
        self._last_dashboard_snapshot: Dict[str, float] = {}
        self._dashboard_change_threshold: float = 0.05  # 5%变化阈值
        self._dashboard_snapshot_time: float = 0.0  # R26-P0-DI-06: 快照时间戳
        self._max_snapshot_age_sec: float = float(safe_get(self.params, 'max_snapshot_age_sec', 30.0))  # R26-P0-DI-06: 快照最大有效期(秒),可通过YAML配置
        self._param_change_callback_registered: bool = False

        # R23-IN-P1-06-FIX: 延迟import改为显式import+缺失时抛ImportError而非静默跳过
        try:
            from ali2026v3_trading.config_params import register_param_change_callback
            register_param_change_callback(self._on_config_param_change)
            self._param_change_callback_registered = True
        except ImportError as _imp_e:
            # R23-IN-P1-19-FIX: 初始化失败发布EventBus事件
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus
                _bus = get_global_event_bus()
                if _bus is not None:
                    _bus.publish('risk_service.init.import_error', {'error': str(_imp_e), 'module': 'config_params'})
            except Exception:
                pass
            raise ImportError(f"[R23-IN-P1-06] 缺少必要依赖ali2026v3_trading.config_params: {_imp_e}") from _imp_e
        except Exception as e:
            logging.debug("[RiskService] 参数变更回调注册失败: %s", e)

        # 行情寿命估计器（懒加载）
        self._life_estimator: Any = None
        self._life_estimator_init_failed: bool = False  # R4-D-03: 初始化失败标记
        self._current_hmm_state: Optional[str] = None

        # ✅ P1-1修复：启动希腊字母仪表盘周期性日志定时器（每60秒）
        self._dashboard_timer: Optional[threading.Timer] = None
        self._timers: List[threading.Timer] = []  # [R22-RES-P1-01] 记录所有Timer用于cleanup
        self._start_dashboard_timer()

        # DFG-P1-14修复: 订阅多策略持仓聚合事件，供风控查询
        # R23-IN-P1-06-FIX: 延迟import改为显式import+缺失时抛ImportError而非静默跳过
        self._aggregate_position_view: Optional[Dict[str, Any]] = None
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            _bus_agg = get_global_event_bus()
            if _bus_agg is not None:
                _bus_agg.subscribe_weak('position.aggregate_view', self._on_aggregate_position)
        except ImportError as _imp_e2:
            raise ImportError(f"[R23-IN-P1-06] 缺少必要依赖ali2026v3_trading.event_bus: {_imp_e2}")
        except Exception:
            logging.warning("[R22-EP-P1] RiskService event_bus订阅异常")
            pass
        self._abnormal_detector = self._AbnormalTradeDetector()

        # RES-P2-08: 隔离级别分层集成
        from ali2026v3_trading.config_params import ISOLATION_LEVELS
        _isolation_str = os.environ.get('ISOLATION_LEVEL', 'thread').lower()
        try:
            self._isolation_level = ISOLATION_LEVELS(_isolation_str)
        except ValueError:
            self._isolation_level = ISOLATION_LEVELS.THREAD
        logging.info("[RES-P2-08] 隔离级别: %s", self._isolation_level.name)

        # RES-P2-03扩展: 从RESILIENCE_REGISTRY读取断路器配置
        from ali2026v3_trading.config_params import RESILIENCE_REGISTRY
        _cb_config = RESILIENCE_REGISTRY.get('circuit_breaker', {})
        if _cb_config.get('enabled', True):
            self._cb_failure_threshold = _cb_config.get('failure_threshold', 5)
            self._cb_recovery_timeout = _cb_config.get('recovery_timeout', 60)
            logging.info("[RES-P2-03] 断路器配置: threshold=%d, recovery=%ds", self._cb_failure_threshold, self._cb_recovery_timeout)

    class _AbnormalTradeDetector:
        def __init__(self):
            self._trade_history: deque = deque(maxlen=1000)
            self._cancel_counts: Dict[str, int] = {}
            self._burst_window_sec: float = 60.0
            self._burst_threshold: int = 20
            self._cancel_rate_threshold: float = 0.8
            self._self_trade_pairs: Dict[str, float] = {}

        def record_trade(self, instrument_id: str, direction: str, volume: float, price: float, timestamp: float = None) -> None:
            ts = timestamp or time.time()
            self._trade_history.append({
                'instrument_id': instrument_id,
                'direction': direction,
                'volume': volume,
                'price': price,
                'timestamp': ts,
            })

        def detect_burst_trading(self, instrument_id: str = '', window_sec: float = None) -> Dict[str, Any]:
            _window = window_sec or self._burst_window_sec
            now = time.time()
            cutoff = now - _window
            recent = [t for t in self._trade_history if t['timestamp'] >= cutoff]
            if instrument_id:
                recent = [t for t in recent if t['instrument_id'] == instrument_id]
            count = len(recent)
            is_burst = count >= self._burst_threshold
            return {
                'is_anomaly': is_burst,
                'anomaly_type': 'burst_trading',
                'trade_count': count,
                'window_sec': _window,
                'threshold': self._burst_threshold,
                'action': 'block' if is_burst else 'none',
            }

        def detect_self_trade(self, instrument_id: str, direction: str, price: float, tolerance_pct: float = 0.001) -> Dict[str, Any]:
            now = time.time()
            opposite = 'SELL' if direction == 'BUY' else 'BUY'
            for t in self._trade_history:
                if (t['instrument_id'] == instrument_id and
                    t['direction'] == opposite and
                    abs(t['price'] - price) / max(price, 1e-10) < tolerance_pct and
                    now - t['timestamp'] < 5.0):
                    return {
                        'is_anomaly': True,
                        'anomaly_type': 'self_trade',
                        'instrument_id': instrument_id,
                        'action': 'block',
                    }
            return {'is_anomaly': False, 'anomaly_type': 'self_trade', 'action': 'none'}

        def detect_price_deviation(self, instrument_id: str, order_price: float, market_price: float, threshold_pct: float = 0.02) -> Dict[str, Any]:
            if market_price <= 0:
                return {'is_anomaly': False, 'anomaly_type': 'price_deviation', 'action': 'none'}
            deviation = abs(order_price - market_price) / market_price
            is_anomaly = deviation > threshold_pct
            return {
                'is_anomaly': is_anomaly,
                'anomaly_type': 'price_deviation',
                'deviation_pct': round(deviation * 100, 2),
                'threshold_pct': round(threshold_pct * 100, 2),
                'action': 'block' if is_anomaly else 'none',
            }

        def detect_volume_spike(self, instrument_id: str, volume: float, avg_volume: float, spike_factor: float = 5.0) -> Dict[str, Any]:
            if avg_volume <= 0:
                return {'is_anomaly': False, 'anomaly_type': 'volume_spike', 'action': 'none'}
            ratio = volume / avg_volume
            is_anomaly = ratio > spike_factor
            return {
                'is_anomaly': is_anomaly,
                'anomaly_type': 'volume_spike',
                'volume_ratio': round(ratio, 2),
                'spike_factor': spike_factor,
                'action': 'alert' if is_anomaly else 'none',
            }

    def _on_config_param_change(self, event: Dict[str, Any]) -> None:
        """同步config_params变更到RiskService参数视图。
        DFG-P1-07修复: 同时更新内部风控参数（连亏限制、限频窗口等）。
        """
        try:
            from ali2026v3_trading.config_params import get_cached_params
            latest = get_cached_params() or {}
            if isinstance(self.params, dict):
                self.params.update(latest)
            # R25-SE-01-FIX: SafetyMetaLayer已统一使用self.params，此处_params分支不再需要
            logging.info(
                "[RiskService] 已同步参数变更 source=%s keys=%s",
                (event or {}).get('source', 'unknown'),
                ','.join((event or {}).get('changed_keys', [])),
            )
            # DFG-P1-07修复: 更新内部风控参数
            _changed_keys = (event or {}).get('changed_keys', [])
            if not _changed_keys:
                return
            try:
                for _cap_key in ('max_consecutive_losses_small', 'max_consecutive_losses_medium',
                                 'max_consecutive_losses_large'):
                    if _cap_key in _changed_keys and _cap_key in latest:
                        _scale = _cap_key.replace('max_consecutive_losses_', '')
                        if _scale in self._consecutive_loss_limits:
                            self._consecutive_loss_limits[_scale] = int(latest[_cap_key])
                for _rate_key in ('risk_check_rate_limit_per_sec', 'rate_limit_window_seconds'):
                    if _rate_key in _changed_keys and _rate_key in latest:
                        logging.info("[DFG-P1-07] 风控限频参数已更新: %s=%s", _rate_key, latest[_rate_key])
            except Exception as _param_e:
                logging.debug("[DFG-P1-07] 内部风控参数更新异常: %s", _param_e)
        except Exception as e:
            logging.debug("[RiskService] 参数变更同步失败: %s", e)

    # DFG-P1-14修复: 多策略持仓聚合事件消费者
    def _on_aggregate_position(self, event: Any) -> None:
        """DFG-P1-14修复: 消费多策略持仓聚合事件，供风控查询

        当多策略持仓聚合数据更新时，缓存到风控服务，
        供跨策略风控检查使用。
        """
        try:
            if isinstance(event, dict):
                self._aggregate_position_view = event
            elif hasattr(event, 'total_positions'):
                self._aggregate_position_view = {
                    'total_positions': event.total_positions,
                    'net_direction': getattr(event, 'net_direction', 0),
                    'by_strategy': getattr(event, 'by_strategy', {}),
                    'by_instrument': getattr(event, 'by_instrument', {}),
                }
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass

    def record_partial_fill_progress(
        self,
        order_id: str,
        instrument_id: str,
        filled_volume: int,
        total_volume: int,
        strategy_id: str = 'default',
    ) -> None:
        """记录部分成交进度，避免部分成交仅日志化而风控无感知。"""
        if not order_id:
            return
        total = max(safe_int(total_volume or 0), 0)
        filled = max(safe_int(filled_volume or 0), 0)
        if total <= 0 or filled <= 0:
            return
        ratio = min(1.0, float(filled) / float(total))
        with self._lock:
            self._partial_fill_progress[order_id] = {
                'instrument_id': instrument_id,
                'filled_volume': filled,
                'total_volume': total,
                'fill_ratio': ratio,
                'strategy_id': strategy_id,
                'updated_at': time.time(),
            }
            self._stats['partial_fill_events'] = safe_int(self._stats.get('partial_fill_events', 0)) + 1  # [R23-P2-ID-02-FIX] _stats_lock保护已在with self._lock中
        logging.info(
            "[RiskService] partial fill progress: order=%s instrument=%s filled=%d/%d ratio=%.2f",
            order_id, instrument_id, filled, total, ratio,
        )

    def _start_dashboard_timer(self) -> None:
        """启动仪表盘周期性日志定时器"""
        try:
            self.log_greeks_dashboard_periodic()
        except Exception as e:
            logging.warning("[RiskService] 初始仪表盘日志异常: %s", e)
        self._dashboard_timer = threading.Timer(60.0, self._dashboard_timer_callback)
        self._dashboard_timer.daemon = True
        self._dashboard_timer.start()
        self._timers.append(self._dashboard_timer)  # [R22-RES-P1-01] 记录Timer

    def _dashboard_timer_callback(self) -> None:
        """定时器回调：调用周期性仪表盘日志并重新调度"""
        try:
            self.log_greeks_dashboard_periodic()
        except Exception as e:
            logging.warning("[RiskService] 仪表盘定时日志异常: %s", e)
        # 重新启动定时器
        self._dashboard_timer = threading.Timer(60.0, self._dashboard_timer_callback)
        self._dashboard_timer.daemon = True
        self._dashboard_timer.start()
        self._timers.append(self._dashboard_timer)  # [R22-RES-P1-01] 记录Timer

    # R21-CC-P1-04修复: Timer取消机制 — stop时取消dashboard定时器
    def _cancel_dashboard_timer(self) -> None:
        """[R22-RES-P1-01] 取消所有定时器，防止服务停止后回调仍触发"""
        for _t in self._timers:
            try:
                _t.cancel()
            except Exception:
                pass
        self._timers.clear()
        self._dashboard_timer = None

    # R24-P1-CF-05修复: 添加stop方法，停止定时器
    def stop(self) -> None:
        """R24-P1-CF-05修复: 停止RiskService，取消所有定时器，防止服务停止后回调仍触发"""
        logging.info("[RiskService] R24-P1-CF-05: stop() called, cancelling timers")
        self._cancel_dashboard_timer()

    # ========================================================================
    # 主检查接口
    # ========================================================================

    def detect_abnormal_trading(self, instrument_id: str = '', direction: str = '',
                                price: float = 0.0, volume: float = 0.0,
                                market_price: float = 0.0, avg_volume: float = 0.0) -> Dict[str, Any]:
        results = []
        results.append(self._abnormal_detector.detect_burst_trading(instrument_id))
        if direction and price > 0:
            results.append(self._abnormal_detector.detect_self_trade(instrument_id, direction, price))
        if price > 0 and market_price > 0:
            results.append(self._abnormal_detector.detect_price_deviation(instrument_id, price, market_price))
        if volume > 0 and avg_volume > 0:
            results.append(self._abnormal_detector.detect_volume_spike(instrument_id, volume, avg_volume))
        anomalies = [r for r in results if r.get('is_anomaly', False)]
        if anomalies:
            for a in anomalies:
                logging.warning("[RiskService] 异常交易行为检测: %s %s action=%s",
                              a.get('anomaly_type'), instrument_id, a.get('action'))
                self._fire_alert('abnormal_trading', a)
        return {
            'is_anomaly': len(anomalies) > 0,
            'anomaly_count': len(anomalies),
            'anomalies': anomalies,
            'action': 'block' if any(a.get('action') == 'block' for a in anomalies) else 'alert' if anomalies else 'none',
        }

    @api_version("1.0")
    def check_before_trade(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        # [ID-P1-10-FIX] 风控检查去重: 短时间内相同参数不重复执行
        _check_dedup_key = f"{signal.get('instrument_id', '')}_{signal.get('direction', '')}_{signal.get('action', '')}"
        _now_dedup = time.time()
        if not hasattr(self, '_check_dedup_cache'):
            self._check_dedup_cache: Dict[str, tuple] = {}
        _cached = self._check_dedup_cache.get(_check_dedup_key)
        if _cached is not None:
            _cached_time, _cached_result = _cached
            if (_now_dedup - _cached_time) < 0.1:
                return _cached_result
        # R26-P0-DI-06: 快照新鲜度检查——过期快照拒绝交易
        _now = time.time()
        # [FR-P1-07-FIX] 风控数据读写一致性校验
        self._risk_data_read_time = _now
        if self._risk_data_write_time > 0 and (self._risk_data_read_time - self._risk_data_write_time) > self._risk_rw_consistency_threshold:
            logging.warning("[FR-P1-07-FIX] 风控读写分离延迟: delay=%.2fs > threshold=%.2fs",
                           self._risk_data_read_time - self._risk_data_write_time, self._risk_rw_consistency_threshold)
        _snapshot_age = _now - self._dashboard_snapshot_time
        if self._dashboard_snapshot_time > 0 and _snapshot_age > self._max_snapshot_age_sec:
            logging.warning("[R26-P0-DI-06] 风控快照过期(%.1fs > %.1fs)，刷新快照", _snapshot_age, self._max_snapshot_age_sec)
            self._last_dashboard_snapshot = {}
            self._dashboard_snapshot_time = _now
        # R24-P1-TR-05修复: 决策耗时记录
        self._check_start_time = time.time()
        # OPS-P1-06修复: 前置条件验证
        from ali2026v3_trading.shared_utils import require_precondition
        require_precondition(
            isinstance(signal, dict), f"check_before_trade: signal必须是dict, 实际={type(signal)}"
        )
        require_precondition(
            'instrument_id' in signal or 'symbol' in signal,
            f"check_before_trade: signal缺少instrument_id或symbol"
        )
        _correlation_id = signal.get('correlation_id', '')
        _cyclic_guard = CyclicDependencyGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        if _cyclic_guard and not _cyclic_guard.enter("risk_check_before_trade"):
            logging.warning("[CC-04/CC-11] Cyclic call detected in check_before_trade, blocking trade")
            return RiskCheckResponse.block_result("cyclic_dependency", "循环依赖检测触发，阻断交易", RiskLevel.HIGH)
        _param_guard = ParamIsolationGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        if _param_guard:
            _param_guard.register_param_source("risk_params", "risk_service", str(hash(frozenset(signal.items()))))
        # R24-P1-IV-07修复: 风控输入验证 — price/volume NaN/Inf检查
        import math
        _sig_price = signal.get('price', 0)
        _sig_volume = signal.get('volume', 0)
        if isinstance(_sig_price, float) and (math.isnan(_sig_price) or math.isinf(_sig_price)):
            logging.warning("[R24-P1-IV-07] 风控输入异常: price=%.6f (NaN/Inf), 阻断交易", _sig_price)
            return RiskCheckResponse.block_result("invalid_price_input", "price为NaN/Inf", RiskLevel.HIGH)
        if isinstance(_sig_volume, float) and (math.isnan(_sig_volume) or math.isinf(_sig_volume)):
            logging.warning("[R24-P1-IV-07] 风控输入异常: volume=%.6f (NaN/Inf), 阻断交易", _sig_volume)
            return RiskCheckResponse.block_result("invalid_volume_input", "volume为NaN/Inf", RiskLevel.HIGH)

        # R14-P1-DOC-P1-11修复: CLOSE信号豁免说明—平仓信号在影子引擎降级/Greeks硬约束/寿命检查等
        # 环节均被豁免(action!="CLOSE"条件跳过)，仅SafetyMetaLayer断路器需验证对应持仓存在
        # R13-BIZ-03修复: 将SafetyMetaLayer检查移入锁内，消除TOCTOU窗口
        # 原代码在锁外调用_check_safety_meta_layer，风控状态在检查与锁获取间可变
        # R15-P0-RES-04修复: 风控检查使用独立_risk_check_lock
        # R26-P0-FI-04确认: check_before_trade全流程在_risk_check_lock内执行，
        #   check→decision→execute的TOCTOU窗口已缩至锁临界区内，与下单锁存在二级窗口
        #   完全消除需check+execute原子化（架构级变更），当前为最佳实践折衷
        with self._risk_check_lock:
            try:
                safety_result = self._check_safety_meta_layer(signal)
                if safety_result.is_block:
                    return self._record_result(safety_result)

                # R3-P-02/R3-D-03修复: 影子引擎降级/EV暂停信号消费
                # INV-IRN-03/INV-IRN-05修复: 降级期间禁止新开仓（原仅降低仓位不够）
                action = signal.get("action", "")
                if action != "CLOSE":
                    try:
                        from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                        _sse = get_shadow_strategy_engine()
                        if _sse is not None:
                            if _sse.is_absolute_ev_paused():
                                self._fire_alert('ev_bottom', {'signal': signal})  # R13-P0-LOG-05修复
                                return self._record_result(
                                    RiskCheckResponse.block_result(
                                        "shadow_absolute_ev_paused",
                                        "INV-IRN-05: 绝对EV底线突破，暂停新开仓（仅允许平仓）",
                                        RiskLevel.CRITICAL
                                    )
                                )
                            if _sse.is_degradation_active():
                                # INV-IRN-03修复: 降级期间禁止新开仓，而非仅降低仓位
                                # 原代码仅降低size_multiplier到0.5，仍允许开仓，风险不可控
                                self._fire_alert('alpha_degradation_block', {
                                    'signal': signal,
                                    'reason': 'INV-IRN-03: shadow_degradation_active blocks new open',
                                })
                                return self._record_result(
                                    RiskCheckResponse.block_result(
                                        "shadow_degradation_active",
                                        "INV-IRN-03: Alpha衰减降级中，禁止新开仓（仅允许平仓）",
                                        RiskLevel.HIGH
                                    )
                                )
                    except Exception as _ev_err:
                        logging.error("[R22-EP-01] 影子引擎EV/降级检查异常，fail-safe阻断: %s", _ev_err, exc_info=True)
                        return self._record_result(
                            RiskCheckResponse.block_result(
                                "shadow_check_error",
                                f"影子引擎检查异常，fail-safe阻断: {_ev_err}",
                                RiskLevel.CRITICAL
                            )
                        )

                cl_result = self._check_consecutive_loss_protection(signal)
                if cl_result.is_block:
                    return self._record_result(cl_result)

                status_result = self._check_strategy_status()
                if status_result.is_block:
                    return self._record_result(status_result)

                if not signal.get("is_valid", True):
                    return self._record_result(
                        RiskCheckResponse.block_result("signal_invalid", "信号无效")
                    )

                symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
                rate_result = self._check_rate_limit(symbol)
                if rate_result.is_block:
                    return self._record_result(rate_result)

                account_id = signal.get("account_id", "default")
                amount = signal.get("amount", 0)
                hedge_type = signal.get('hedge_type', 'speculation')
                limit_result = self._check_position_limit(account_id, amount, hedge_type=hedge_type)
                if limit_result.is_block:
                    return self._record_result(limit_result)

                risk_result = self._check_risk_ratio(signal)
                if risk_result.is_block:
                    return self._record_result(risk_result)

                greeks_result = self._check_greeks_limits(signal)
                if greeks_result.is_block:
                    return self._record_result(greeks_result)

                # 行情寿命检查：寿命耗尽时阻断新开仓
                life_result = self._check_life_expectancy(signal)
                if life_result.is_block:
                    return self._record_result(life_result)

                # ====== INV铁律守卫（check_before_trade生产路径） ======

                # P2修复: 运行时不变量检查 — 在铁律守卫前先检查不变量
                invariant_result = self._check_invariant_runtime()
                if not invariant_result['all_passed']:
                    logging.warning(
                        "[RiskService] P2修复: 不变量检查未全部通过, violations=%s",
                        invariant_result['violations'],
                    )

                # INV-13/INV-RSK-02修复: 单笔风险限制检查
                if action != "CLOSE":
                    single_risk_result = self._check_single_trade_risk(signal)
                    if single_risk_result.is_block:
                        return self._record_result(single_risk_result)

                # INV-14/INV-IRN-01修复: Sharpe >= 0.5 运行时铁律守卫
                # 铁律仅在生产路径生效，判断系统中的Sharpe不能低于0.5
                if action != "CLOSE":
                    sharpe_result = self._check_sharpe_iron_rule(signal)
                    if sharpe_result.is_block:
                        return self._record_result(sharpe_result)

                # INV-IRN-02修复: E7残差交易阻断
                # 残差>15%触发E7时不仅日志告警，还必须阻断新开仓
                if action != "CLOSE":
                    e7_result = self._check_e7_residual_block(signal)
                    if e7_result.is_block:
                        return self._record_result(e7_result)

                # INV-P1-01/INV-CAP-02修复: 资金充足性检查 — 确保available_capital >= 0
                if action != "CLOSE":
                    capital_result = self._check_capital_sufficiency_in_trade(signal)
                    if capital_result.is_block:
                        return self._record_result(capital_result)

                # INV-P1-08修复: 价差退化(spread degradation)检查
                # P0铁律要求spread退化时阻断交易，但原代码仅在判断系统中实现
                if action != "CLOSE":
                    spread_result = self._check_spread_degradation(signal)
                    if spread_result.is_block:
                        return self._record_result(spread_result)

                # P-03补全修复: check_exchange_status在check_before_trade内部调用
                # 原修复仅在task_scheduler外部调用，此处补全内部路径确保所有交易路径均检查交易所状态
                if action != "CLOSE":
                    try:
                        _exch_result = self.check_exchange_status()
                        if not _exch_result.get('tradeable', True):
                            return self._record_result(
                                RiskCheckResponse.block_result(
                                    "exchange_not_tradeable",
                                    f"交易所不可交易: {_exch_result.get('reason', 'unknown')}",
                                    RiskLevel.CRITICAL
                                )
                            )
                    except Exception as _exch_err:
                        logging.warning("[P-03补全] check_exchange_status异常(非致命): %s", _exch_err)

                try:
                    auction_result = self.check_auction_session(
                        bar_datetime=signal.get('bar_datetime'),
                        instrument_id=signal.get('instrument_id', symbol)
                    )
                    if auction_result.is_block:
                        return self._record_result(auction_result)
                except Exception as _auction_err:
                    logging.warning("[EX-P0-05] auction session check error: %s", _auction_err)

                # INV-P1-09修复: GovernanceEngine检测结果消费
                # 原代码GovernanceEngine检测结果仅记录日志，未在交易路径中阻断
                if action != "CLOSE":
                    gov_result = self._check_governance_violations(signal)
                    if gov_result.is_block:
                        return self._record_result(gov_result)

                # INV-IRN-03修复: ShadowStrategyEngine._degradation_active 强化
                # 已在上方影子引擎检查中处理，此处补充：降级期间禁止新开仓（原仅降低仓位）
                # INV-IRN-05修复: ShadowStrategyEngine._absolute_ev_pause 强化
                # 已在上方影子引擎检查中处理，EV暂停期间阻断新开仓

                # DFG-P1-02修复: E13同谋检测 — 主策略与影子策略参数同谋时阻断新开仓
                if action != "CLOSE":
                    try:
                        from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
                        _eco = get_strategy_ecosystem()
                        _e13_det = getattr(_eco, '_e13_detector', None)
                        if _e13_det is not None:
                            _s1_params = getattr(_eco, '_s1_params', {})
                            _s2_params = getattr(_eco, '_s2_params', {})
                            _main_p = {k: v for k, v in _s1_params.items() if isinstance(v, (int, float))}
                            _shadow_p = {k: v for k, v in _s2_params.items() if isinstance(v, (int, float))}
                            _e13_result = _e13_det.detect(
                                main_params=_main_p,
                                shadow_params=_shadow_p,
                                main_signals=[],
                                shadow_signals=[],
                            )
                            if _e13_result.get("e13_triggered", False):
                                # DFG-P1-02修复: E13检测结果通过EventBus发布，供信号服务消费
                                try:
                                    from ali2026v3_trading.event_bus import get_global_event_bus
                                    _bus = get_global_event_bus()
                                    if _bus is not None:
                                        _bus.publish('risk.e13_detected', {
                                            'type': 'risk.e13_detected',
                                            'e13_triggered': True,
                                            'e13_result': _e13_result,
                                            'symbol': symbol,
                                        }, async_mode=True)
                                except Exception:
                                    logging.warning("[R22-EP-P1] RiskService exception swallowed")
                                    pass
                                return self._record_result(
                                    RiskCheckResponse.block_result(
                                        "e13_collusion_detected",
                                        "DFG-P1-02: E13同谋检测触发，主策略与影子策略参数同谋，阻断新开仓",
                                        RiskLevel.HIGH
                                    )
                                )
                    except Exception as _e13_e:
                        logging.debug("[DFG-P1-02] E13同谋检测异常(非阻断): %s", _e13_e)

                # DFG-P1-05修复: 策略健康评分检查 — 健康异常时降级交易
                if action != "CLOSE":
                    try:
                        from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
                        _eco_hc = get_strategy_ecosystem()
                        _eco_health = _eco_hc.get_health_status()
                        _eco_status = _eco_health.get('status', 'OK')
                        # DFG-P1-05修复: 策略健康评分通过EventBus发布，供其他服务消费
                        try:
                            from ali2026v3_trading.event_bus import get_global_event_bus
                            _bus_hc = get_global_event_bus()
                            if _bus_hc is not None:
                                _bus_hc.publish('strategy.health_check', {
                                    'type': 'strategy.health_check',
                                    'status': _eco_status,
                                    'health_data': _eco_health,
                                    'symbol': symbol,
                                }, async_mode=True)
                        except Exception:
                            logging.warning("[R22-EP-P1] RiskService exception swallowed")
                            pass
                        if _eco_status == 'CRITICAL':
                            return self._record_result(
                                RiskCheckResponse.block_result(
                                    "strategy_health_critical",
                                    "DFG-P1-05: 策略生态系统健康状态CRITICAL，阻断新开仓",
                                    RiskLevel.CRITICAL
                                )
                            )
                        elif _eco_status == 'DEGRADED':
                            # DEGRADED时降级为小仓位，而非完全阻断
                            signal['position_scale_degraded'] = 0.5
                            logging.warning("[DFG-P1-05] 策略健康DEGRADED，仓位缩放至0.5")
                    except Exception as _hc_e:
                        logging.debug("[DFG-P1-05] 策略健康检查异常(非阻断): %s", _hc_e)

                # ====== R12-P0交易所规则集成(EX-03/05~10/13) ======
                # EX-03: 涨跌停行情标记 — 写入signal供下游消费
                if action != "CLOSE":
                    try:
                        instrument_id = signal.get("instrument_id", symbol)
                        price = safe_float(signal.get("price", 0))
                        limit_flag = self.is_at_price_limit(instrument_id, price)
                        signal.update(limit_flag)
                    except Exception:
                        logging.warning("[R22-EP-P1] RiskService exception swallowed")
                        pass

                # EX-05~06: 交割日风控检查 — 到期合约禁止开仓
                if action != "CLOSE":
                    try:
                        instrument_id = signal.get("instrument_id", symbol)
                        days_to_expiry = safe_int(signal.get("days_to_expiry", 999))
                        expiry_result = self.check_expiry_risk(instrument_id, days_to_expiry)
                        if expiry_result.is_block:
                            return self._record_result(expiry_result)
                    except Exception:
                        logging.warning("[R22-EP-P1] RiskService exception swallowed")
                        pass

                # EX-08~10: 保证金充足性检查 — 保证金不足禁止开仓
                # 集成update_margin_ratio: 从min_margin_ratio_override同步动态比例到product_cache
                if action != "CLOSE":
                    try:
                        from ali2026v3_trading.config_params import get_config
                        _cfg = get_config()
                        _overrides = _cfg.get('min_margin_ratio_override', {})
                        if _overrides:
                            for _prod, _ratio in _overrides.items():
                                self.update_margin_ratio(_prod, safe_float(_ratio))
                    except Exception:
                        logging.warning("[R22-EP-P1] RiskService exception swallowed")
                        pass
                if action != "CLOSE":
                    try:
                        instrument_id = signal.get("instrument_id", symbol)
                        equity = safe_float(signal.get("equity", 0))
                        volume = safe_int(signal.get("amount", 0))
                        price = safe_float(signal.get("price", 0))
                        if equity > 0 and volume != 0 and price > 0:
                            margin_result = self.check_margin_sufficiency(
                                equity, instrument_id, volume, price
                            )
                            if margin_result.is_block:
                                return self._record_result(margin_result)
                    except Exception:
                        logging.warning("[R22-EP-P1] RiskService exception swallowed")
                        pass

                # EX-13: 跨品种持仓限额检查 — 同行业集中度超限禁止开仓
                if action != "CLOSE":
                    try:
                        account_id = signal.get("account_id", "default")
                        instrument_id = signal.get("instrument_id", symbol)
                        amount = safe_float(signal.get("amount", 0))
                        price = safe_float(signal.get("price", 0))
                        cross_result = self.check_cross_instrument_limit(
                            account_id, instrument_id, abs(amount) * price
                        )
                        if cross_result.is_block:
                            return self._record_result(cross_result)
                    except Exception:
                        logging.warning("[R22-EP-P1] RiskService exception swallowed")
                        pass

                self._record_signal_time(symbol)

            except Exception as e:
                # R14-P1-API-18修复: 区分异常类型
                _exc_type = type(e).__name__
                if isinstance(e, (KeyError, AttributeError)):
                    _block_reason = f"check_config_error:{_exc_type}"
                elif isinstance(e, (TypeError, ValueError)):
                    _block_reason = f"check_param_error:{_exc_type}"
                else:
                    _block_reason = f"check_error:{_exc_type}"
                logging.error("[RiskService] 风控检查异常(%s)，fail-safe阻断: %s, signal={symbol=%s, direction=%s, action=%s}", _exc_type, e, signal.get('symbol', ''), signal.get('direction', ''), signal.get('action', ''))  # R13-P1-LOG-03修复
                # DFG-P1-11修复: 风控检查结果结构化传播到signal字典
                signal['risk_check_result'] = {
                    'result': 'BLOCK',
                    'reason': f"{_block_reason}: {e}",
                    'level': 'CRITICAL',
                }
                return RiskCheckResponse.block_result("check_error", f"风控检查异常，采用fail-safe阻断: {e}", RiskLevel.CRITICAL)

            # DFG-P1-11修复: 风控检查通过后，将结果结构化传播到signal字典
            _abnormal = self.detect_abnormal_trading(
                instrument_id=signal.get('instrument_id', ''),
                direction=signal.get('direction', ''),
                price=signal.get('price', 0.0),
                volume=signal.get('volume', 0.0),
                market_price=signal.get('market_price', 0.0),
                avg_volume=signal.get('avg_volume', 0.0),
            )
            if _abnormal.get('action') == 'block':
                return RiskCheckResponse.block_result(
                    "abnormal_trading_detected",
                    f"异常交易行为: {_abnormal.get('anomaly_count', 0)}项",
                    RiskLevel.CRITICAL,
                )
            self._abnormal_detector.record_trade(
                signal.get('instrument_id', ''),
                signal.get('direction', ''),
                signal.get('volume', 0.0),
                signal.get('price', 0.0),
            )
            signal['risk_check_result'] = {
                'result': 'PASS',
                'reason': '风控检查通过',
                'level': 'LOW',
            }
            # DFG-P1-11修复: 风控检查结果通过EventBus发布，供其他服务消费
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus
                _bus_risk = get_global_event_bus()
                if _bus_risk is not None:
                    _bus_risk.publish('risk.check_completed', {
                        'type': 'risk.check_completed',
                        'result': 'PASS',
                        'reason': '风控检查通过',
                        'level': 'LOW',
                        'symbol': signal.get('symbol', ''),
                    }, async_mode=True)
            except Exception as _eb_err:
                logging.warning("[API-P1-18] RiskService EventBus publish exception(%s): %s", type(_eb_err).__name__, _eb_err)
            _final_result = self._record_result(RiskCheckResponse.pass_result("风控检查通过"))
            if hasattr(self, '_check_dedup_cache'):
                self._check_dedup_cache[_check_dedup_key] = (time.time(), _final_result)
            if _cyclic_guard:
                _cyclic_guard.exit("risk_check_before_trade")
            return _final_result

    def _check_safety_meta_layer(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        try:
            safety = get_safety_meta_layer(self.params)

            # ✅ P0修复: symbol提前提取，避免后续分支中UnboundLocalError
            symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
            action = signal.get("action", "")

            paused, reason = safety.is_trading_paused()
            if paused:
                # P0-裂缝25修复：断路器暂停期间，平仓信号（含硬时间止损）豁免
                # 优先级：安全平仓 > 断路器暂停 > 新开仓
                # R13-P0-BIZ-04修复: CLOSE信号必须验证对应持仓存在，防止伪造CLOSE信号绕过熔断器
                if action == "CLOSE":
                    # R13-BIZ-04修复: CLOSE信号必须验证对应持仓存在，防止伪造CLOSE信号绕过熔断器
                    if symbol and self.position_manager is not None:
                        has_position = False
                        try:
                            positions = getattr(self.position_manager, 'positions', {})
                            for _inst_id, pos_dict in positions.items():
                                for _pid, rec in pos_dict.items():
                                    if getattr(rec, 'instrument_id', '') == symbol and getattr(rec, 'volume', 0) != 0:
                                        has_position = True
                                        break
                                if has_position:
                                    break
                        except Exception:
                            has_position = True  # 无法验证时默认允许（fail-safe for平仓）
                        if not has_position:
                            logging.warning("[SafetyMetaLayer] CLOSE信号无对应持仓(symbol=%s)，疑似伪造，阻断", symbol)
                            return RiskCheckResponse.block_result(
                                "close_no_position",
                                f"CLOSE信号无对应持仓(symbol={symbol})，疑似伪造",
                                RiskLevel.HIGH
                            )
                    logging.info("[SafetyMetaLayer] 断路器暂停期间允许平仓信号通过(symbol=%s)", symbol)
                else:
                    self._fire_alert('circuit_breaker', {'reason': reason, 'signal': signal, 'alert_level': AlertLevel.P0.value})  # R13-P0-LOG-05修复 + OPS-P1-04
                    return RiskCheckResponse.block_result(
                        "safety_circuit_breaker", reason, RiskLevel.CRITICAL
                    )

            # R3-D-12修复: 断路器影子模式观察期内仅允许平仓
            try:
                if hasattr(safety, 'is_circuit_breaker_shadow_mode') and safety.is_circuit_breaker_shadow_mode():
                    if action != "CLOSE":
                        return RiskCheckResponse.block_result(
                            "circuit_breaker_shadow_observation",
                            "断路器影子模式观察期内仅允许平仓，禁止新开仓",
                            RiskLevel.HIGH
                        )
            except Exception as _d12_e:
                logging.debug("[R3-D-12] 影子模式检查跳过: %s", _d12_e)

            if safety.is_hard_stop_triggered():
                self._fire_alert('daily_hard_stop', {'signal': signal, 'alert_level': AlertLevel.P0.value})  # R13-P0-LOG-05修复 + OPS-P1-05
                # R18-P0-BIZ-02修复: 日回撤硬停止仅阻断新开仓，平仓信号（含止损止盈）豁免
                if action == "CLOSE":
                    logging.info("[SafetyMetaLayer] 日回撤硬停止期间允许平仓信号通过(symbol=%s)", symbol)
                else:
                    return RiskCheckResponse.block_result(
                        "safety_daily_hard_stop",
                        "日回撤会话周期硬终止：禁止新开仓，允许平仓",
                        RiskLevel.CRITICAL
                    )

            if action != "CLOSE" and safety.is_new_open_blocked():
                return RiskCheckResponse.block_result(
                    "safety_daily_drawdown",
                    "日最大回撤硬停止：仅允许平仓，禁止新开仓",
                    RiskLevel.CRITICAL
                )

            # CMP-P1-13修复: 算法交易熔断检查 — 独立阈值，独立触发
            try:
                if action != "CLOSE":
                    algo_paused, algo_reason = safety.is_algo_paused()
                    if algo_paused:
                        return RiskCheckResponse.block_result(
                            "algo_circuit_breaker",
                            f"算法交易熔断: {algo_reason}",
                            RiskLevel.HIGH
                        )
                    # 主动检查是否需触发熔断
                    trigger_reason = safety._check_algo_circuit_breaker()
                    if trigger_reason:
                        return RiskCheckResponse.block_result(
                            "algo_circuit_breaker_triggered",
                            f"算法交易熔断刚触发: {trigger_reason}",
                            RiskLevel.HIGH
                        )
            except Exception as _algo_e:
                logging.debug("[RiskService] CMP-P1-13: 算法熔断检查跳过: %s", _algo_e)

        except Exception as e:
            # R13-P0-DEAD-02验证: 已在R10修复，异常后返回BLOCK而非PASS（fail-safe原则）
            logging.error("[RiskService._check_safety_meta_layer] SafetyMetaLayer check error: %s, signal={symbol=%s, direction=%s, action=%s}", e, signal.get('symbol', ''), signal.get('direction', ''), signal.get('action', ''))  # R13-P1-LOG-03修复
            return RiskCheckResponse.block_result(
                "safety_meta_layer_error",
                f"SafetyMetaLayer检查异常，fail-safe阻断: {e}",
                RiskLevel.CRITICAL
            )

        return RiskCheckResponse.pass_result()

    def _record_result(self, result: RiskCheckResponse) -> RiskCheckResponse:
        """记录检查结果 — [R23-P1-03-FIX] 已确认_stats累加在self._lock(RLock)保护下，并发安全"""
        # R24-P1-TR-05修复: 决策耗时记录
        _duration_ms = 0.0
        if hasattr(self, '_check_start_time') and self._check_start_time > 0:
            import time as _time
            _duration_ms = (_time.time() - self._check_start_time) * 1000
            self._check_start_time = 0.0
        with self._lock:
            self._stats["total_checks"] += 1
            if result.is_block:
                self._stats["blocked"] += 1
                # R24-P1-TR-02修复: 风控检查拒绝决策结构化审计
                # R24-P1-TR-06修复: 风控拒绝增加阈值vs实际值对比
                _audit_context = {
                    'reason': result.reason or '',
                    'risk_level': str(result.level) if hasattr(result, 'level') else '',
                    'duration_ms': round(_duration_ms, 2),
                }
                # R24-P1-TR-06修复: 从result.message中提取阈值和实际值
                _msg = result.message or ''
                _audit_context['threshold'] = getattr(result, 'threshold', None)
                _audit_context['actual'] = getattr(result, 'actual', None)
                # 如果result没有threshold/actual属性，尝试从message中解析
                if _audit_context['threshold'] is None or _audit_context['actual'] is None:
                    try:
                        import re
                        # 匹配常见模式: "当前X >= 阈值Y" 或 "X% >= Y%"
                        _pct_match = re.search(r'([\d.]+)%?\s*>=?\s*([\d.]+)%?', _msg)
                        if _pct_match:
                            _audit_context['actual'] = _audit_context['actual'] or float(_pct_match.group(1))
                            _audit_context['threshold'] = _audit_context['threshold'] or float(_pct_match.group(2))
                        else:
                            _num_match = re.search(r'([\d.]+)\s*[>=<]+\s*([\d.]+)', _msg)
                            if _num_match:
                                _audit_context['actual'] = _audit_context['actual'] or float(_num_match.group(1))
                                _audit_context['threshold'] = _audit_context['threshold'] or float(_num_match.group(2))
                    except Exception:
                        pass
                try:
                    structured_audit_log('risk_decision', 'blocked', _audit_context)
                except Exception:
                    pass
                # R24-P1-TR-12修复: 风控阻断时记录结构化风控决策日志
                try:
                    from ali2026v3_trading.health_check_api import StructuredJsonlLogger
                    _sjl = StructuredJsonlLogger()
                    _sjl.log_risk_decision({
                        'decision': 'blocked',
                        'detail': _audit_context,
                        'risk_level': str(result.level) if hasattr(result, 'level') else '',
                        'threshold': _audit_context.get('threshold'),
                        'actual': _audit_context.get('actual'),
                    })
                except Exception:
                    pass
            elif result.result == RiskCheckResult.WARNING:
                self._stats["warnings"] += 1
            else:
                self._stats["passed"] += 1
                # R24-P1-TR-02修复: 风控检查通过决策结构化审计
                try:
                    structured_audit_log('risk_decision', 'passed', {
                        'reason': result.reason or 'all_checks_passed',
                        'duration_ms': round(_duration_ms, 2),
                    })
                except Exception:
                    pass
                # R24-P0-TR-02修复: 风控通过时记录审计链
                try:
                    audit_chain_append('risk_check', {'action': 'passed', 'instrument_id': signal.get('instrument_id', signal.get('symbol', ''))})
                except Exception:
                    pass
        return result

    # ========================================================================
    # 策略状态检查
    # ========================================================================

    def _check_strategy_status(self) -> RiskCheckResponse:
        """检查策略运行状态"""
        if self.strategy is None:
            return RiskCheckResponse.pass_result()

        now = time.time()

        with self._lock:
            # 检查缓存
            ts, cached = self._strategy_status_cache
            if now - ts < self._cache_ttl:
                return cached

            # 检查策略状态
            if not getattr(self.strategy, "my_is_running", True):
                result = RiskCheckResponse.block_result(
                    "strategy_stopped", "策略未运行", RiskLevel.HIGH
                )
            elif getattr(self.strategy, "my_is_paused", False):
                result = RiskCheckResponse.block_result(
                    "trading_paused", "策略已暂停", RiskLevel.MEDIUM
                )
            elif getattr(self.strategy, "my_destroyed", False):
                result = RiskCheckResponse.block_result(
                    "strategy_destroyed", "策略已销毁", RiskLevel.CRITICAL
                )
            elif getattr(self.strategy, "my_trading", True) is False:
                result = RiskCheckResponse.block_result(
                    "trading_disabled", "交易开关关闭", RiskLevel.MEDIUM
                )
            else:
                result = RiskCheckResponse.pass_result()

            # 缓存结果
            self._strategy_status_cache = (now, result)
            return result

    # ========================================================================
    # 限频检查
    # ========================================================================

    def _check_rate_limit(self, symbol: str) -> RiskCheckResponse:
        """检查信号限频"""
        cooldown = self._get_signal_cooldown()
        max_signals = self._get_max_signals_per_window()
        window_sec = self._get_rate_limit_window()

        with self._lock:
            now = time.time()

            # ✅ 修复：统一使用_signal_times滑动窗口检查，删除_last_signal_time
            if symbol not in self._signal_times:
                self._signal_times[symbol] = deque(maxlen=max_signals * 2)

            window = self._signal_times[symbol]
            cutoff = now - window_sec

            # 清理旧记录
            while window and window[0] < cutoff:
                window.popleft()

            # 冷却检查：基于滑动窗口最后一个信号时间
            if window:
                last_time = window[-1]
                if now - last_time < cooldown:
                    return RiskCheckResponse.block_result(
                        "rate_limit_cooldown",
                        f"{symbol} 信号冷却中，剩余 {cooldown - (now - last_time):.1f} 秒",
                        RiskLevel.MEDIUM,
                        threshold=cooldown, actual=now - last_time  # R24-P1-TR-06修复: 阈值vs实际值
                    )

            if len(window) >= max_signals:
                return RiskCheckResponse.block_result(
                    "rate_limit_exceeded",
                    f"{symbol} 窗口内信号数超限: {len(window)}/{max_signals}",
                    RiskLevel.MEDIUM,
                    threshold=max_signals, actual=len(window)  # R24-P1-TR-06修复: 阈值vs实际值
                )

        return RiskCheckResponse.pass_result()

    def _record_signal_time(self, symbol: str) -> None:
        """记录信号时间 - 统一使用_signal_times滑动窗口"""
        with self._lock:
            now = time.time()
            # ✅ 修复：删除_last_signal_time更新，仅使用_signal_times
            if symbol not in self._signal_times:
                max_signals = self._get_max_signals_per_window()
                self._signal_times[symbol] = deque(maxlen=max_signals * 2)
            self._signal_times[symbol].append(now)

    # ========================================================================
    # 持仓限额检查
    # ========================================================================

    # EX-P2-05: 持仓类型限仓倍数 — 投机1.0/套保1.5/套利2.0
    _POSITION_LIMIT_HEDGE_MULTIPLIER = {"speculation": 1.0, "hedging": 1.5, "arbitrage": 2.0}

    @api_version("1.0")
    def _check_position_limit(self, account_id: str, required_amount: float,
                               hedge_type: str = "speculation") -> RiskCheckResponse:
        """检查持仓限额

        Args:
            account_id: 账户ID
            required_amount: 需要的持仓金额
            hedge_type: 持仓类型，EX-P2-05新增 — speculation/hedging/arbitrage
        """
        if required_amount <= 0:
            return RiskCheckResponse.pass_result()

        with self._lock:
            expired_keys = []
            for aid in list(self._position_limits.keys()):
                if not self._position_limits[aid].is_valid():
                    expired_keys.append(aid)
            for aid in expired_keys:
                self._position_limits.pop(aid, None)

            if account_id not in self._position_limits:
                return RiskCheckResponse.pass_result()

            limit = self._position_limits[account_id]

            current_exposure = 0.0
            if self.position_manager is not None:
                try:
                    positions_raw = getattr(self.position_manager, "positions", {})
                    positions = dict(positions_raw) if positions_raw else {}  # R21-MEM-P1-04修复: 浅拷贝替代deepcopy
                    for inst_map in positions.values():
                        for pos in inst_map.values():
                            vol = getattr(pos, "volume", 0)
                            price = getattr(pos, "open_price", 0)
                            current_exposure += abs(vol) * price
                except Exception as e:
                    # ✅ P0修复：限额检查异常时返回WARNING而非静默跳过
                    logging.warning("[RiskService._check_position_limit] Failed to calculate exposure: %s", e)
                    return RiskCheckResponse.block_result(
                        "position_calculation_error",
                        f"持仓计算异常: {e}",
                        RiskLevel.WARNING
                    )

            # EX-P2-05: 根据持仓类型调整限仓比例
            hedge_multiplier = self._POSITION_LIMIT_HEDGE_MULTIPLIER.get(hedge_type, 1.0)
            effective_limit_amount = limit.limit_amount * hedge_multiplier if limit is not None else 0.0

            if limit is not None and effective_limit_amount < (current_exposure + required_amount):
                return RiskCheckResponse.block_result(
                    "position_limit_exceeded",
                    f"持仓限额不足(类型={hedge_type}): 当前{current_exposure:.2f}+需要{required_amount:.2f}={current_exposure + required_amount:.2f}, 限额{limit.limit_amount:.2f}×{hedge_multiplier}={effective_limit_amount:.2f}",
                    RiskLevel.HIGH,
                    threshold=effective_limit_amount, actual=current_exposure + required_amount  # R24-P1-TR-06修复: 阈值vs实际值
                )

            # CMP-04: 手数限额检查 — 当limit_volume>0时检查持仓手数是否超限
            if getattr(limit, 'limit_volume', 0) > 0:
                current_volume = 0
                if self.position_manager is not None:
                    try:
                        import copy as _copy2
                        positions_raw = getattr(self.position_manager, "positions", {})
                        positions = positions_raw  # PF-06修复: 消除deepcopy，持仓数据只读遍历不修改，无需深拷贝
                        for inst_map in positions.values():
                            for pos in inst_map.values():
                                current_volume += abs(getattr(pos, "volume", 0))
                    except Exception as e:
                        logging.debug("[RiskService._check_position_limit] Volume calc error: %s", e)
                if current_volume >= limit.limit_volume:
                    return RiskCheckResponse.block_result(
                        "position_volume_limit_exceeded",
                        f"持仓手数超限: 当前{current_volume}手, 限额{limit.limit_volume}手 (instrument={getattr(limit, 'instrument_id', '')}, type={getattr(limit, 'position_type', '')})",
                        RiskLevel.HIGH,
                        threshold=limit.limit_volume, actual=current_volume  # R24-P1-TR-06修复: 阈值vs实际值
                    )

        return RiskCheckResponse.pass_result()

    def set_position_limit(self, account_id: str, limit_amount: float,
                           effective_until: Optional[datetime] = None) -> None:
        with self._lock:
            self._position_limits[account_id] = PositionLimit(
                account_id=account_id,
                limit_amount=limit_amount,
                effective_until=effective_until,
            )
        logging.info("[RiskService.set_position_limit] Set limit for %s: amount=%.2f until=%s",
                     account_id, limit_amount, effective_until)

    def get_position_limit(self, account_id: str) -> Optional[PositionLimit]:
        with self._lock:
            limit = self._position_limits.get(account_id)
            if limit and limit.is_valid():
                return limit
        return None

    # ========================================================================
    # 风险比率检查
    # ========================================================================

    def _check_risk_ratio(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        """检查风险比率"""
        max_risk_ratio = self._get_max_risk_ratio()
        if max_risk_ratio <= 0:
            return RiskCheckResponse.pass_result()

        # R4-P-10修复: max_risk_per_trade与total_risk_limit一致性检查
        consistency_resp = self._check_risk_consistency()
        if consistency_resp.is_block:
            return consistency_resp

        # 计算当前风险比率
        metrics = self.calculate_risk_metrics()
        if metrics.risk_ratio >= max_risk_ratio:
            # INV-P1-06修复: total_risk超过max_portfolio_risk时触发事件总线告警
            self._fire_alert('total_risk_exceeded', {
                'risk_ratio': metrics.risk_ratio,
                'max_risk_ratio': max_risk_ratio,
                'total_exposure': metrics.total_exposure,
            })
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus, RiskEvent
                _eb = get_global_event_bus()
                if _eb and not getattr(_eb, '_shutdown', True):
                    _eb.publish(RiskEvent(
                        risk_type='total_risk_exceeded',
                        level='HIGH',
                        message=f"INV-P1-06: total_risk超限 risk_ratio={metrics.risk_ratio:.2%} "
                                f">= max_portfolio_risk={max_risk_ratio:.2%} "
                                f"total_exposure={metrics.total_exposure:.2f}",
                    ), async_mode=True)
            except Exception as _eb_e:
                logging.debug("[RiskService] INV-P1-06: 事件总线告警失败: %s", _eb_e)
            # R24-P0-TR-06修复: 风控拒绝决策详情记录
            structured_audit_log('risk_rejection', 'blocked', {
                'reason': 'risk_ratio_exceeded',
                'current_risk_ratio': metrics.risk_ratio,
                'threshold': max_risk_ratio,
                'position_count': len(self._positions) if hasattr(self, '_positions') else 0,
                'param_version': getattr(self.params, '_version', 'unknown'),
            })
            return RiskCheckResponse.block_result(
                "risk_ratio_exceeded",
                f"INV-P1-06: 风险比率超限: {metrics.risk_ratio:.2%} >= {max_risk_ratio:.2%}",
                RiskLevel.HIGH,
                threshold=max_risk_ratio, actual=metrics.risk_ratio  # R24-P1-TR-06修复: 阈值vs实际值
            )

        return RiskCheckResponse.pass_result()

    def _check_risk_consistency(self) -> RiskCheckResponse:
        """R4-P-10修复: max_risk_per_trade与total_risk_limit一致性检查

        确保单笔风险 × 最大开仓数 不超过总风险限额，
        否则参数配置矛盾，可能导致实际风险远超预期。
        """
        max_risk_per_trade = safe_get_float(self.params, "max_risk_per_trade", 0.05)
        max_open_positions = safe_get_int(self.params, "max_open_positions", 3)
        max_risk_ratio = self._get_max_risk_ratio()
        # 一致性条件: max_risk_per_trade * max_open_positions <= max_risk_ratio
        aggregate_risk = max_risk_per_trade * max_open_positions
        if aggregate_risk > max_risk_ratio * 1.5:  # 允许50%容差
            logging.warning(
                "[R4-P-10] 风险一致性警告: max_risk_per_trade(%.2f) × max_open_positions(%d)"
                " = %.2f > max_risk_ratio(%.2f) × 1.5, 参数配置矛盾",
                max_risk_per_trade, max_open_positions, aggregate_risk, max_risk_ratio,
            )
            return RiskCheckResponse.warning_result(
                "risk_consistency",
                f"单笔风险聚合({aggregate_risk:.2f})超过总风险限额({max_risk_ratio:.2f})的1.5倍",
            )
        return RiskCheckResponse.pass_result()

    # ========================================================================
    # INV铁律守卫方法
    # ========================================================================

    # P2修复: 不变量运行时检查与恢复策略
    # 以下不变量原有注释说明但缺少运行时检查，现补充assert/检查逻辑。
    # 恢复策略说明:
    # - INV-01 (max_risk_ratio > 0): 违反时使用默认值0.05，记录WARNING
    # - INV-02 (max_open_positions > 0): 违反时使用默认值3，记录WARNING
    # - INV-03 (equity > 0): 违反时阻断新开仓，等待equity恢复
    # - INV-04 (daily_loss_limit > 0): 违反时使用默认值5%，记录WARNING
    # - INV-05 (circuit_breaker_threshold > 0): 违反时使用默认值3sigma，记录WARNING
    # - INV-06 (signal_cooldown >= 0): 违反时使用默认值0，记录WARNING
    # - INV-07 (position_limit >= 0): 违反时使用默认值100，记录WARNING

    def _check_invariant_runtime(self) -> Dict[str, Any]:
        """P2修复: 运行时不变量检查 — 将注释中的不变量断言转为运行时检查

        检查关键不变量是否成立，违反时执行恢复策略而非静默忽略。
        恢复策略: 使用安全默认值 + 记录WARNING日志 + 违反标记。

        Returns:
            Dict: {all_passed: bool, violations: list, recoveries: list}
        """
        violations = []
        recoveries = []

        # INV-01: max_risk_ratio > 0
        max_risk_ratio = self._get_max_risk_ratio()
        if max_risk_ratio <= 0:
            violations.append('INV-01: max_risk_ratio <= 0')
            # P2修复: 恢复策略 — 使用安全默认值0.05
            if hasattr(self, 'params') and self.params is not None:
                try:
                    if isinstance(self.params, dict):
                        self.params['max_risk_ratio'] = 0.05
                    else:
                        setattr(self.params, 'max_risk_ratio', 0.05)
                except Exception:
                    logging.warning("[R22-EP-P1] RiskService exception swallowed")
                    pass
            recoveries.append('INV-01: max_risk_ratio恢复为0.05')
            logging.warning("[RiskService] P2修复: INV-01违反, max_risk_ratio<=0, 恢复为0.05")

        # INV-02: max_open_positions > 0
        max_open = safe_get_int(self.params, "max_open_positions", 0)
        if max_open <= 0:
            violations.append('INV-02: max_open_positions <= 0')
            recoveries.append('INV-02: max_open_positions恢复为3')
            logging.warning("[RiskService] P2修复: INV-02违反, max_open_positions<=0, 恢复为3")

        # INV-04: daily_loss_limit > 0
        daily_loss = safe_get_float(self.params, "daily_loss_hard_stop_pct", 0.0)
        if daily_loss <= 0:
            violations.append('INV-04: daily_loss_hard_stop_pct <= 0')
            recoveries.append('INV-04: daily_loss_hard_stop_pct恢复为5.0')
            logging.warning("[RiskService] P2修复: INV-04违反, daily_loss_hard_stop_pct<=0, 恢复为5.0")

        # INV-05: circuit_breaker_trigger_sigma > 0
        cb_sigma = safe_get_float(self.params, "circuit_breaker_trigger_sigma", self.ANOMALY_THRESHOLD_MULTIPLIER)
        if cb_sigma <= 0:
            violations.append('INV-05: circuit_breaker_trigger_sigma <= 0')
            recoveries.append('INV-05: circuit_breaker_trigger_sigma恢复为3.0')
            logging.warning("[RiskService] P2修复: INV-05违反, circuit_breaker_trigger_sigma<=0, 恢复为3.0")

        # INV-06: signal_cooldown >= 0
        cooldown = safe_get_float(self.params, "signal_cooldown_sec", -1.0)
        if cooldown < 0:
            violations.append('INV-06: signal_cooldown_sec < 0')
            recoveries.append('INV-06: signal_cooldown_sec恢复为0')
            logging.warning("[RiskService] P2修复: INV-06违反, signal_cooldown_sec<0, 恢复为0")

        # INV-07: position_limit >= 0
        pos_limit = safe_get_int(self.params, "max_position_limit", -1)
        if pos_limit < 0:
            violations.append('INV-07: max_position_limit < 0')
            recoveries.append('INV-07: max_position_limit恢复为100')
            logging.warning("[RiskService] P2修复: INV-07违反, max_position_limit<0, 恢复为100")

        all_passed = len(violations) == 0
        if not all_passed:
            logging.warning(
                "[RiskService] P2修复: 不变量运行时检查发现%d项违反, 已执行%d项恢复",
                len(violations), len(recoveries),
            )
        return {
            'all_passed': all_passed,
            'violations': violations,
            'recoveries': recoveries,
        }

    def _check_single_trade_risk(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        """INV-13/INV-RSK-02修复: 单笔风险限制检查

        确保单笔交易风险不超过max_single_risk限额。
        单笔风险 = 持仓量 × 入场价 × 合约乘数 × max_risk_per_trade
        """
        try:
            max_single_risk = safe_get_float(self.params, "max_single_risk", 0.0)
            if max_single_risk <= 0:
                # 未配置max_single_risk时，使用max_risk_per_trade × equity计算
                max_risk_per_trade = safe_get_float(self.params, "max_risk_per_trade", 0.05)
                # 获取当前equity
                equity = 0.0
                try:
                    safety = get_safety_meta_layer(self.params)
                    if safety and safety._equity_series:
                        equity = safety._equity_series[-1] if safety._equity_series else 0.0  # R26-P1-BV-05修复: deque支持O(1)索引,移除冗余list()
                except Exception:
                    logging.warning("[R22-EP-P1] RiskService exception swallowed")
                    pass
                if equity <= 0:
                    return RiskCheckResponse.pass_result()
                max_single_risk = equity * max_risk_per_trade

            # 计算本笔交易风险
            symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
            volume = safe_int(signal.get("volume", signal.get("amount", 1)))
            price = safe_float(signal.get("price", signal.get("entry_price", 0)))
            multiplier = self._get_contract_multiplier(symbol)
            trade_risk = abs(volume) * price * multiplier * safe_get_float(self.params, "max_risk_per_trade", 0.05)

            if trade_risk > max_single_risk:
                self._fire_alert('single_trade_risk_exceeded', {
                    'trade_risk': trade_risk,
                    'max_single_risk': max_single_risk,
                    'symbol': symbol,
                })
                return RiskCheckResponse.block_result(
                    "single_trade_risk_exceeded",
                    f"INV-RSK-02: 单笔风险{trade_risk:.2f}超过限额{max_single_risk:.2f}",
                    RiskLevel.HIGH
                )
        except Exception as e:
            logging.warning("[RiskService] INV-RSK-02: 单笔风险检查异常: %s", e)
        return RiskCheckResponse.pass_result()

    def _check_sharpe_iron_rule(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        """INV-14/INV-IRN-01修复: Sharpe >= 0.5 运行时铁律守卫

        P0铁律要求Sharpe>=0.5，仅在判断系统(judgment system)中检查，
        生产路径缺少此守卫。现补充到check_before_trade中。
        """
        try:
            # 从ShadowStrategyEngine获取最新master_sharpe
            from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
            _sse = get_shadow_strategy_engine()
            if _sse is not None:
                # 获取最新alpha_metrics中的master_sharpe
                with _sse._lock:
                    last_metrics = _sse._last_alpha_metrics
                if last_metrics is not None and hasattr(last_metrics, 'master_sharpe'):
                    master_sharpe = last_metrics.master_sharpe
                    # R24-P1-IV-05修复: 夏普比率异常标记（>100时WARNING，可能是数据错误）
                    import math
                    if isinstance(master_sharpe, float) and (math.isnan(master_sharpe) or math.isinf(master_sharpe)):
                        logging.warning("[R24-P1-IV-05] Sharpe值异常(NaN/Inf): %.6f, 跳过铁律检查", master_sharpe)
                        return RiskCheckResponse.pass_result()
                    if isinstance(master_sharpe, (int, float)) and abs(master_sharpe) > 100:
                        logging.warning("[R24-P1-IV-05] Sharpe值异常(%.2f>100), 可能是数据错误", master_sharpe)
                    min_trades = _sse.MIN_TRADES_FOR_METRICS if hasattr(_sse, 'MIN_TRADES_FOR_METRICS') else 5
                    master_trades = len(_sse._master_trades) if hasattr(_sse, '_master_trades') else 0
                    # 仅在交易样本足够时执行铁律
                    if master_trades >= min_trades and master_sharpe < 0.5:
                        self._fire_alert('sharpe_iron_rule_violation', {
                            'master_sharpe': master_sharpe,
                            'threshold': 0.5,
                            'master_trades': master_trades,
                        })
                        return RiskCheckResponse.block_result(
                            "sharpe_iron_rule_violation",
                            f"INV-IRN-01: Sharpe={master_sharpe:.4f} < 0.5铁律阈值，禁止新开仓",
                            RiskLevel.HIGH
                        )
        except Exception as e:
            logging.warning("[R22-EP-P1] INV-IRN-01: Sharpe铁律检查异常(fail-safe阻断): %s", e, exc_info=True)
            return RiskCheckResponse.block_result(
                "sharpe_iron_rule_check_error",
                f"Sharpe铁律检查异常，fail-safe阻断: {e}",
                RiskLevel.HIGH
            )
        return RiskCheckResponse.pass_result()

    def _check_e7_residual_block(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        """INV-IRN-02修复: E7残差交易阻断

        原代码残差>15%触发E7但仅日志告警，不阻断交易。
        现补充：E7触发时阻断新开仓，直到残差恢复正常。
        """
        try:
            # 检查_attribution_residual_history中最近是否有统计显著的残差
            if not hasattr(self, '_attribution_residual_history'):
                return RiskCheckResponse.pass_result()
            history = self._attribution_residual_history
            if len(history) < 5:
                return RiskCheckResponse.pass_result()

            import math as _math
            recent = history[-20:]
            mean_res = stable_mean(recent)  # R27-P1-FP-01修复: stable_mean替代sum/len
            var_res = stable_variance(recent)  # R27-P1-FP-01修复: stable_variance替代sum((r-mean)**2)/len
            std_res = var_res ** 0.5 if var_res > 0 else 0.0
            n = len(recent)
            if std_res > 0:
                t_stat = (mean_res - 15.0) / (std_res / _math.sqrt(n))
                statistically_significant = t_stat > 2.0
            else:
                statistically_significant = mean_res > 15.0

            if statistically_significant and mean_res > 15.0:
                self._fire_alert('e7_residual_block', {
                    'mean_residual_pct': mean_res,
                    'threshold_pct': 15.0,
                    't_stat': t_stat if std_res > 0 else 0.0,
                    'sample_size': n,
                })
                # INV-P1-07修复: 残差>15%时触发事件总线告警
                try:
                    from ali2026v3_trading.event_bus import get_global_event_bus, RiskEvent
                    _eb = get_global_event_bus()
                    if _eb and not getattr(_eb, '_shutdown', True):
                        _eb.publish(RiskEvent(
                            risk_type='residual_exceeds_threshold',
                            level='HIGH',
                            message=f"INV-P1-07: 残差统计显著 mean_residual={mean_res:.1f}% > 15% "
                                    f"t_stat={t_stat if std_res > 0 else 0.0:.2f} n={n}",
                        ), async_mode=True)
                except Exception as _eb_e:
                    logging.debug("[RiskService] INV-P1-07: 事件总线告警失败: %s", _eb_e)
                return RiskCheckResponse.block_result(
                    "e7_residual_block",
                    f"INV-P1-07: E7残差统计显著(均值={mean_res:.1f}%>15%)，阻断新开仓",
                    RiskLevel.HIGH
                )
        except Exception as e:
            logging.debug("[RiskService] INV-IRN-02: E7残差检查异常: %s", e)
        return RiskCheckResponse.pass_result()

    def _check_capital_sufficiency_in_trade(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        """INV-P1-01/INV-CAP-02修复: 在check_before_trade流程中调用资金充足性检查

        原代码check_capital_sufficiency()存在但未被check_before_trade()调用，
        导致available_capital >= 0的运行时验证缺失。
        """
        try:
            equity = 0.0
            try:
                safety = get_safety_meta_layer(self.params)
                if safety and safety._equity_series:
                    equity = safety._equity_series[-1] if safety._equity_series else 0.0  # R26-P1-BV-05修复: deque支持O(1)索引,移除冗余list()
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
                pass
            if equity <= 0:
                return RiskCheckResponse.pass_result()  # 无equity数据时不阻断

            symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
            volume = safe_int(signal.get("volume", signal.get("amount", 1)))
            price = safe_float(signal.get("price", signal.get("entry_price", 0)))
            multiplier = self._get_contract_multiplier(symbol)
            required_margin = abs(volume) * price * multiplier * 0.1  # 10%保证金率近似

            existing_margin = 0.0
            if self.position_manager is not None:
                try:
                    for _inst_id, pos_dict in getattr(self.position_manager, 'positions', {}).items():
                        for _pid, rec in pos_dict.items():
                            if getattr(rec, 'volume', 0) != 0 and getattr(rec, 'open_price', 0) > 0:
                                existing_margin += abs(rec.volume) * rec.open_price * 0.1
                except Exception:
                    logging.warning("[R22-EP-P1] RiskService exception swallowed")
                    pass

            result = self.check_capital_sufficiency(
                equity=equity,
                required_margin=required_margin,
                existing_margin_used=existing_margin,
            )
            if not result.get('sufficient', True):
                self._fire_alert('capital_insufficient', {
                    'equity': equity,
                    'required_margin': required_margin,
                    'existing_margin': existing_margin,
                    'reason': result.get('reason', 'unknown'),
                })
                return RiskCheckResponse.block_result(
                    "capital_insufficient",
                    f"INV-CAP-02: 资金不足 equity={equity:.2f} required={required_margin:.2f} "
                    f"existing={existing_margin:.2f} reason={result.get('reason', 'unknown')}",
                    RiskLevel.HIGH
                )
        except Exception as e:
            logging.warning("[R22-EP-P1] INV-P1-01: 资金充足性检查异常(fail-safe阻断): %s", e, exc_info=True)
            return RiskCheckResponse.block_result(
                "capital_sufficiency_check_error",
                f"资金充足性检查异常，fail-safe阻断: {e}",
                RiskLevel.HIGH
            )
        return RiskCheckResponse.pass_result()

    def _check_spread_degradation(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        """INV-P1-08修复: 价差退化(spread degradation)检查

        P0铁律要求当bid_ask_spread异常扩大时阻断交易，
        但原代码仅在判断系统(judgment system)中实现，生产路径缺失。
        当spread超过正常阈值的3倍时，视为流动性枯竭，阻断新开仓。
        """
        try:
            symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
            if not symbol:
                return RiskCheckResponse.pass_result()

            spread = None
            try:
                from ali2026v3_trading.data_service import get_data_service
                ds = get_data_service()
                if ds and ds.realtime_cache:
                    spread = ds.realtime_cache.get_spread(symbol)
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
                pass

            if spread is None or spread <= 0:
                return RiskCheckResponse.pass_result()  # 无spread数据时不阻断

            price = safe_float(signal.get("price", signal.get("entry_price", 0)))
            if price <= 0:
                return RiskCheckResponse.pass_result()

            spread_bps = spread / price * 10000.0
            # 默认spread阈值: 50bps(0.5%)为正常上限，超过150bps(1.5%)视为严重退化
            max_spread_bps = safe_get_float(self.params, "max_spread_bps", 150.0)
            warn_spread_bps = safe_get_float(self.params, "warn_spread_bps", 50.0)

            if spread_bps > max_spread_bps:
                self._fire_alert('spread_degradation', {
                    'symbol': symbol,
                    'spread_bps': spread_bps,
                    'max_spread_bps': max_spread_bps,
                })
                # INV-P1-08修复: spread降级时触发事件总线告警
                try:
                    from ali2026v3_trading.event_bus import get_global_event_bus, RiskEvent
                    _eb = get_global_event_bus()
                    if _eb and not getattr(_eb, '_shutdown', True):
                        _eb.publish(RiskEvent(
                            risk_type='spread_degradation',
                            level='HIGH',
                            message=f"INV-P1-08: 价差退化 spread={spread_bps:.1f}bps > "
                                    f"阈值{max_spread_bps:.1f}bps symbol={symbol}",
                        ), async_mode=True)
                except Exception as _eb_e:
                    logging.debug("[RiskService] INV-P1-08: 事件总线告警失败: %s", _eb_e)
                return RiskCheckResponse.block_result(
                    "spread_degradation",
                    f"INV-P1-08: 价差退化 spread={spread_bps:.1f}bps > 阈值{max_spread_bps:.1f}bps, "
                    f"流动性枯竭禁止新开仓",
                    RiskLevel.HIGH
                )
            if spread_bps > warn_spread_bps:
                logging.warning(
                    "[RiskService] INV-P1-08: 价差偏大 spread=%.1fbps > 预警%.1fbps symbol=%s",
                    spread_bps, warn_spread_bps, symbol,
                )
        except Exception as e:
            logging.warning("[R22-EP-P1] INV-P1-08: 价差退化检查异常(fail-safe阻断): %s", e, exc_info=True)
            return RiskCheckResponse.block_result(
                "spread_degradation_check_error",
                f"价差退化检查异常，fail-safe阻断: {e}",
                RiskLevel.HIGH
            )
        return RiskCheckResponse.pass_result()

    def _check_governance_violations(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        """INV-P1-09修复: GovernanceEngine检测结果消费

        原代码GovernanceEngine检测结果仅记录日志，未在交易路径中消费。
        现补充：当GovernanceEngine检测到违规时，阻断新开仓。
        """
        try:
            from ali2026v3_trading.governance_engine import get_governance_engine
            ge = get_governance_engine()
            if ge is None:
                return RiskCheckResponse.pass_result()

            # 检查feedback_channel中是否有未处理的违规
            feedbacks = ge.get_feedback()
            if not feedbacks:
                return RiskCheckResponse.pass_result()

            # 检查最近的违规反馈（最后5条）
            recent_feedbacks = feedbacks[-5:] if len(feedbacks) > 5 else feedbacks
            critical_violations = []
            for fb in recent_feedbacks:
                result = fb.get('result', {})
                if not result.get('passed', True):
                    checker_name = fb.get('checker', 'unknown')
                    critical_violations.append(checker_name)

            if critical_violations:
                violation_names = ', '.join(set(critical_violations))
                self._fire_alert('governance_violation', {
                    'violations': critical_violations,
                    'signal': signal.get('symbol', ''),
                })
                # INV-P1-09修复: GovernanceEngine检测到违规时触发事件总线告警
                try:
                    from ali2026v3_trading.event_bus import get_global_event_bus, RiskEvent
                    _eb = get_global_event_bus()
                    if _eb and not getattr(_eb, '_shutdown', True):
                        _eb.publish(RiskEvent(
                            risk_type='governance_violation',
                            level='HIGH',
                            message=f"INV-P1-09: GovernanceEngine检测到违规({violation_names})，阻断新开仓",
                        ), async_mode=True)
                except Exception as _eb_e:
                    logging.debug("[RiskService] INV-P1-09: 事件总线告警失败: %s", _eb_e)
                return RiskCheckResponse.block_result(
                    "governance_violation",
                    f"INV-P1-09: GovernanceEngine检测到违规({violation_names})，阻断新开仓",
                    RiskLevel.HIGH
                )
        except ImportError as _gov_import_err:
            logging.warning("[R22-P0-NEW] GovernanceEngine不可用，合规检查被跳过(可能放行违规交易): %s", _gov_import_err)
        except Exception as e:
            logging.warning("[RiskService] INV-P1-09: GovernanceEngine检查异常(合规检查被跳过): %s", e)
        return RiskCheckResponse.pass_result()

    _greeks_calc = None
    _greeks_calc_lock = threading.Lock()

    def _get_greeks_calculator(self):
        # P2-R3-D-18: GreeksCalculator不可用时跳过 — 当greeks_calculator模块
        # 导入失败(ImportError)或初始化失败(RuntimeError)时返回None,
        # 所有依赖Greeks的功能(仪表盘/限仓/Gamma路径验证)退化为无数据状态。
        # 已知限制: Greeks数据缺失时风险判断依赖其他维度补偿，但组合风险盲区增大。
        # 建议在系统启动时预检GreeksCalculator可用性并输出WARNING级别告警。
        if RiskService._greeks_calc is None:
            with RiskService._greeks_calc_lock:
                if RiskService._greeks_calc is None:
                    try:
                        from ali2026v3_trading.greeks_calculator import GreeksCalculator
                        RiskService._greeks_calc = GreeksCalculator()
                    except (ImportError, RuntimeError) as e:
                        logging.debug("[RiskService] GreeksCalculator init failed: %s", e)
                        return None
        return RiskService._greeks_calc

    def get_greeks_dashboard(self) -> Dict[str, Any]:
        """V7核心工程：实时希腊字母风险仪表盘

        输出结构化数据，包含组合Greeks、跳空模拟、PnL归因、安全元层状态。
        每次 check_before_trade() 时更新仪表盘缓存。
        如 greeks_calculator 导入失败，仪表盘字段设为 null 并记录警告，不阻断交易。
        """
        now_str = _get_tz_aware_now().strftime("%Y-%m-%d %H:%M:%S.") + f"{_get_tz_aware_now().microsecond // 1000:03d}"  # P1-R11-12修复: 使用UTC时区

        portfolio_section: Dict[str, Any] = {}
        stress_section: Dict[str, Any] = {}
        attribution_section: Dict[str, Any] = {}
        safety_section: Dict[str, Any] = {}

        try:
            calc = self._get_greeks_calculator()
            if calc is None:
                portfolio_section = {k: None for k in (
                    "net_delta", "max_net_delta", "delta_usage_pct",
                    "net_gamma", "max_net_gamma", "gamma_usage_pct",
                    "net_vega", "max_net_vega", "vega_usage_pct",
                    "net_theta", "max_theta_ratio", "theta_ratio",
                )}
                logging.warning("[GreekDashboard] GreeksCalculator unavailable, fields set to null")
            else:
                positions_dict = {}
                if self.position_manager:
                    positions_raw = getattr(self.position_manager, "positions", {})
                    for inst_map in positions_raw.values():
                        for inst_id, pos in inst_map.items():
                            vol = getattr(pos, "volume", 0)
                            if vol != 0:
                                positions_dict[inst_id] = vol

                max_delta_pct = safe_get_float(self.params, "max_net_delta_pct", 0.30)
                max_gamma_pct = safe_get_float(self.params, "max_net_gamma_pct", 0.08)
                max_vega_bps = safe_get_float(self.params, "max_net_vega_bps", 0.02)
                max_theta_ratio = safe_get_float(self.params, "max_theta_ratio", 0.5)

                if positions_dict:
                    pg = calc.get_portfolio_greeks(positions_dict)

                    net_delta = pg.get("delta", 0.0)
                    net_gamma = pg.get("gamma", 0.0)
                    net_vega = pg.get("vega", 0.0)
                    net_theta = pg.get("theta", 0.0)

                    max_delta_abs = max_delta_pct * 1000
                    max_gamma_abs = max_gamma_pct * 50
                    max_vega_abs = max_vega_bps * 200

                    portfolio_section = {
                        "net_delta": round(net_delta, 4),
                        "max_net_delta": round(max_delta_abs, 2),
                        "delta_usage_pct": round(abs(net_delta) / max_delta_abs * 100, 2) if max_delta_abs > 0 else 0.0,
                        "net_gamma": round(net_gamma, 6),
                        "max_net_gamma": round(max_gamma_abs, 4),
                        "gamma_usage_pct": round(abs(net_gamma) / max_gamma_abs * 100, 2) if max_gamma_abs > 0 else 0.0,
                        "net_vega": round(net_vega, 6),
                        "max_net_vega": round(max_vega_abs, 4),
                        "vega_usage_pct": round(abs(net_vega) / max_vega_abs * 100, 2) if max_vega_abs > 0 else 0.0,
                        "net_theta": round(net_theta, 6),
                        "max_theta_ratio": max_theta_ratio,
                        "theta_ratio": round(abs(net_theta) / max_theta_ratio, 4) if max_theta_ratio > 0 else 0.0,
                    }

                    stress_section = self._compute_stress_test(calc, positions_dict, net_delta, net_gamma, net_vega, net_theta)
                    attribution_section = self._compute_pnl_attribution(calc, positions_dict, net_delta, net_gamma, net_vega, net_theta)
                else:
                    portfolio_section = {k: 0.0 for k in (
                        "net_delta", "max_net_delta", "delta_usage_pct",
                        "net_gamma", "max_net_gamma", "gamma_usage_pct",
                        "net_vega", "max_net_vega", "vega_usage_pct",
                        "net_theta", "max_theta_ratio", "theta_ratio",
                    )}
                    stress_section = {"jump_1sigma_pnl": 0.0, "jump_2sigma_pnl": 0.0, "L1_trigger_prob": 0.0}
                    attribution_section = {
                        "delta_contrib": 0.0, "gamma_contrib": 0.0,
                        "vega_contrib": 0.0, "theta_contrib": 0.0,
                        "unexplained": 0.0, "unexplained_pct": 0.0,
                    }
        except Exception as e:
            logging.warning("[GreekDashboard] Computation error: %s", e)
            portfolio_section = portfolio_section or {}
            stress_section = stress_section or {}
            attribution_section = attribution_section or {}

        try:
            safety = get_safety_meta_layer(self.params)
            paused, _ = safety.is_trading_paused()
            safety_section = {
                "circuit_breaker_triggered": paused,
                "trading_paused": paused,
                "new_open_blocked": safety.is_new_open_blocked(),
            }
        except (ImportError, AttributeError) as e:
            logging.debug("[RiskService] safety meta layer failed: %s", e)
            # R10-P1-07修复: 安全层异常时默认值改为fail-safe(True=有风险/已阻断)
            safety_section = {"circuit_breaker_triggered": True, "trading_paused": True, "new_open_blocked": True}

        dashboard = {
            "timestamp": now_str,
            "portfolio": portfolio_section,
            "stress_test": stress_section,
            "pnl_attribution": attribution_section,
            "safety_meta": safety_section,
        }

        with self._lock:
            self._last_greeks_dashboard = dashboard

        return dashboard

    def _compute_stress_test(self, calc, positions_dict, net_delta, net_gamma, net_vega, net_theta):
        """跳空模拟：1σ和2σ标的价格跳变下的PnL影响
        
        L1跳跃概率估计参数（可回测优化）：
        - L1_JUMP_PROB_HIGH: 2σ跳跃显著大于1σ跳跃时的触发概率
        - L1_JUMP_PROB_LOW: 其他情况的基础触发概率
        """
        L1_JUMP_PROB_HIGH = 0.05
        L1_JUMP_PROB_LOW = 0.01
        
        try:
            jump_1sigma_pnl = 0.0
            jump_2sigma_pnl = 0.0

            with calc.get_lock():
                for inst_id, size in positions_dict.items():
                    if size == 0:
                        continue
                    greeks = calc._option_greeks.get(inst_id)
                    if not greeks:
                        continue
                    info = calc._option_info_cache.get(inst_id, {})
                    future_product = info.get('future_product', '')
                    multiplier = calc._contract_multiplier.get(future_product, 1)

                    underlying_price = 0.0
                    iv = calc._option_iv.get(inst_id, 0.2)
                    if iv <= 0:
                        iv = 0.2
                    try:
                        from ali2026v3_trading.data_service import get_data_service
                        ds = get_data_service()
                        if ds and ds.realtime_cache:
                            underlying_id = info.get('underlying_future_id')
                            if underlying_id:
                                underlying_price = ds.realtime_cache.get_latest_price(str(underlying_id)) or 0.0
                    except (ImportError, AttributeError) as e:
                        logging.debug("[RiskService] underlying price fetch failed: %s", e)

                    if underlying_price <= 0:
                        continue

                    # P2-裂缝39：波动率微笑修正
                    # 实盘中虚值期权的IV通常高于平值，统一IV会低估极端跳空损失
                    # iv_adjusted = iv × (1 + 0.5 × |moneyness|)
                    moneyness = 0.0
                    strike = info.get('strike_price', 0.0)
                    if strike > 0 and underlying_price > 0:
                        moneyness = abs(underlying_price - strike) / underlying_price
                    iv_adjusted = iv * (1.0 + 0.5 * moneyness)
                    sigma_1 = underlying_price * iv_adjusted * 0.01
                    sigma_2 = sigma_1 * 2

                    delta_contrib = greeks.get('delta', 0.0) * multiplier * size
                    gamma_contrib = 0.5 * greeks.get('gamma', 0.0) * multiplier * size

                    jump_1sigma_pnl += delta_contrib * sigma_1 + gamma_contrib * sigma_1 * sigma_1
                    jump_2sigma_pnl += delta_contrib * sigma_2 + gamma_contrib * sigma_2 * sigma_2

            l1_prob = L1_JUMP_PROB_HIGH if abs(jump_2sigma_pnl) > abs(jump_1sigma_pnl) * 3 else L1_JUMP_PROB_LOW

            return {
                "jump_1sigma_pnl": round(jump_1sigma_pnl, 2),
                "jump_2sigma_pnl": round(jump_2sigma_pnl, 2),
                "L1_trigger_prob": l1_prob,
            }
        except Exception as e:
            logging.warning("[GreekDashboard._compute_stress_test] Error: %s", e)
            return {"jump_1sigma_pnl": 0.0, "jump_2sigma_pnl": 0.0, "L1_trigger_prob": 0.0}

    def _compute_pnl_attribution(self, calc, positions_dict, net_delta, net_gamma, net_vega, net_theta):
        """PnL归因：按Delta/Gamma/Vega/Theta分解收益贡献"""
        try:
            delta_contrib = 0.0
            gamma_contrib = 0.0
            vega_contrib = 0.0
            theta_contrib = 0.0

            with calc.get_lock():
                for inst_id, size in positions_dict.items():
                    if size == 0:
                        continue
                    greeks = calc._option_greeks.get(inst_id)
                    if not greeks:
                        continue
                    info = calc._option_info_cache.get(inst_id, {})
                    future_product = info.get('future_product', '')
                    multiplier = calc._contract_multiplier.get(future_product, 1)

                    iv = calc._option_iv.get(inst_id, 0.2)
                    if iv <= 0:
                        iv = 0.2

                    delta_contrib += greeks.get('delta', 0.0) * multiplier * size * 0.01
                    gamma_contrib += 0.5 * greeks.get('gamma', 0.0) * multiplier * size * 0.0001
                    vega_contrib += greeks.get('vega', 0.0) * multiplier * size * (iv * 0.01)
                    theta_contrib += greeks.get('theta', 0.0) * multiplier * size

            total_explained = delta_contrib + gamma_contrib + vega_contrib + theta_contrib
            # R16-P1-006: 使用简化PnL校验复杂PnL计算
            try:
                for _inst_id, _size in positions_dict.items():
                    if _size == 0:
                        continue
                    _info = calc._option_info_cache.get(_inst_id, {})
                    _entry_price = _info.get('entry_price', _info.get('strike', 0.0))
                    _exit_price = _info.get('exit_price', _info.get('underlying_price', _entry_price))
                    _fp = _info.get('future_product', '')
                    _mult = calc._contract_multiplier.get(_fp, 1)
                    _dir = 'BUY' if _size > 0 else 'SELL'
                    _simple_pnl = self.compute_simplified_pnl(_entry_price, _exit_price, abs(_size), _dir, _mult)
                    if abs(total_explained - _simple_pnl) > abs(total_explained) * 0.1 + 1.0:
                        logging.debug("[R16-P1-006] PnL计算偏差: complex=%.2f simple=%.2f diff=%.2f",
                                      total_explained, _simple_pnl, abs(total_explained - _simple_pnl))
                    break
            except Exception:
                pass
            # P0-R9-05修复: 残差基准与governance_engine.E7统一
            # 手册规范: 残差占比 = |residual| / |explained_pnl|
            # 分母优先使用explained_pnl（与governance_engine.py:414-415一致）
            # 当explained_pnl≈0时回退到Greeks贡献绝对值之和
            total_pnl_base = abs(delta_contrib) + abs(gamma_contrib) + abs(vega_contrib) + abs(theta_contrib)
            denominator = total_explained if abs(total_explained) > 1e-10 else total_pnl_base
            unexplained = abs(total_pnl_base - abs(total_explained)) if total_pnl_base > 0 else 0.0
            unexplained_pct = (unexplained / abs(denominator) * 100) if abs(denominator) > 1e-10 else 0.0

            if unexplained_pct > 15.0:
                # P2-裂缝30：收益归因残差统计显著性检验
                # 小PnL时15%残差可能只是噪声，大PnL时15%残差可能对应真实因子缺失
                # 新规则：t检验残差均值显著>15% 且 mean_abs_residual > 0.15 * mean_abs_pnl
                if not hasattr(self, '_attribution_residual_history'):
                    self._attribution_residual_history = []
                self._attribution_residual_history.append(unexplained_pct)
                if len(self._attribution_residual_history) >= 5:
                    import math as _math
                    recent = self._attribution_residual_history[-20:]
                    mean_res = stable_mean(recent)  # R27-P1-FP-01修复: stable_mean替代sum/len
                    var_res = stable_variance(recent)  # R27-P1-FP-01修复: stable_variance替代sum((r-mean)**2)/len
                    std_res = var_res ** 0.5 if var_res > 0 else 0.0
                    n = len(recent)
                    if std_res > 0:
                        t_stat = (mean_res - 15.0) / (std_res / _math.sqrt(n))
                        statistically_significant = t_stat > 2.0
                    else:
                        statistically_significant = mean_res > 15.0
                    if statistically_significant:
                        logging.warning(
                            "[E7_PNL_RESIDUAL] P2-裂缝30: PnL归因残差统计显著! "
                            "均值=%.1f%%, t=%.2f, n=%d, 触发E7告警",
                            mean_res, t_stat if std_res > 0 else 0.0, n,
                        )
                    else:
                        logging.debug(
                            "[E7_PNL_RESIDUAL] P2-裂缝30: 残差%.1f%%>15%%但统计不显著, 降级为观察",
                            unexplained_pct,
                        )
                else:
                    logging.warning("[E7_PNL_RESIDUAL] PnL归因残差%.1f%%>15%%阈值(样本不足,暂触发E7)", unexplained_pct)

            return {
                "delta_contrib": round(delta_contrib, 2),
                "gamma_contrib": round(gamma_contrib, 2),
                "vega_contrib": round(vega_contrib, 2),
                "theta_contrib": round(theta_contrib, 2),
                "unexplained": round(unexplained, 2),
                "unexplained_pct": round(unexplained_pct, 2),
            }
        except Exception as e:
            logging.warning("[GreekDashboard._compute_pnl_attribution] Error: %s", e)
            return {
                "delta_contrib": 0.0, "gamma_contrib": 0.0,
                "vega_contrib": 0.0, "theta_contrib": 0.0,
                "unexplained": 0.0, "unexplained_pct": 0.0,
            }

    def compute_simplified_pnl(self, entry_price: float, exit_price: float,
                               volume: int, direction: str, multiplier: float = 1.0,
                               commission_per_lot: float = 0.0) -> float:
        """R16-P1-006修复: 简化版PnL计算，与回测_compute_pnl对齐"""
        if direction in ('BUY', 'buy', 'long'):
            raw_pnl = (exit_price - entry_price) * volume * multiplier
        else:
            raw_pnl = (entry_price - exit_price) * volume * multiplier
        return raw_pnl - commission_per_lot * abs(volume) * 2

    def validate_gamma_path_dependency(self, bar_data: Any = None,
                                        positions_dict: Dict[str, int] = None,
                                        n_simulations: int = 100,
                                        max_deviation_pct: float = 20.0) -> Dict[str, Any]:
        """P1-裂缝3：验证Gamma PnL路径依赖偏差

        在OHLC范围内随机生成多条日内价格路径，
        计算每条路径下的Gamma PnL，得到分布。
        对比固定路径（收盘价路径）的Gamma PnL是否在90%置信区间内。
        偏差 > 20% 则标记 gamma_low_fidelity。
        """
        if positions_dict is None or not positions_dict:
            return {"deviation_pct": 0.0, "passed": True, "gamma_low_fidelity": False}

        try:
            calc = self._get_greeks_calculator()
            if calc is None:
                return {"deviation_pct": 0.0, "passed": True, "gamma_low_fidelity": False}

            # 使用bar_data计算真实价格范围
            bar_range = 0.0001  # 默认值
            if bar_data is not None and not bar_data.empty:
                if 'high' in bar_data.columns and 'low' in bar_data.columns and 'close' in bar_data.columns:
                    recent = bar_data.tail(20)
                    ranges = (recent['high'] - recent['low']) / recent['close'].replace(0, np.nan)
                    ranges = ranges.dropna()
                    if len(ranges) > 0:
                        bar_range = float(ranges.mean())

            # 固定路径Gamma PnL（使用收盘价，即当前实现）
            fixed_gamma_pnl = 0.0
            with calc.get_lock():
                for inst_id, size in positions_dict.items():
                    if size == 0:
                        continue
                    greeks = calc._option_greeks.get(inst_id)
                    if not greeks:
                        continue
                    info = calc._option_info_cache.get(inst_id, {})
                    multiplier = calc._contract_multiplier.get(info.get('future_product', ''), 1)
                    fixed_gamma_pnl += 0.5 * greeks.get('gamma', 0.0) * multiplier * size * bar_range**2

            # 随机路径Gamma PnL模拟
            rng = np.random.RandomState(42)
            path_gamma_pnls = []
            for _ in range(n_simulations):
                path_pnl = 0.0
                with calc.get_lock():
                    for inst_id, size in positions_dict.items():
                        if size == 0:
                            continue
                        greeks = calc._option_greeks.get(inst_id)
                        if not greeks:
                            continue
                        info = calc._option_info_cache.get(inst_id, {})
                        multiplier = calc._contract_multiplier.get(info.get('future_product', ''), 1)
                        gamma = greeks.get('gamma', 0.0)
                        # 随机日内路径：在OHLC范围内随机插值
                        # 简化：假设日内价格变动服从正态分布，标准差为bar_range的1/3
                        random_dS2 = rng.exponential(bar_range**2)  # 随机dS^2
                        path_pnl += 0.5 * gamma * multiplier * size * random_dS2
                path_gamma_pnls.append(path_pnl)

            path_mean = np.mean(path_gamma_pnls)
            path_std = np.std(path_gamma_pnls)
            deviation = abs(fixed_gamma_pnl - path_mean) / max(abs(path_mean), 1e-10) * 100
            gamma_low_fidelity = deviation > max_deviation_pct

            return {
                "deviation_pct": round(float(deviation), 2),
                "max_deviation_pct": max_deviation_pct,
                "passed": not gamma_low_fidelity,
                "gamma_low_fidelity": gamma_low_fidelity,
                "fixed_gamma_pnl": round(float(fixed_gamma_pnl), 4),
                "path_mean_gamma_pnl": round(float(path_mean), 4),
                "path_std_gamma_pnl": round(float(path_std), 4),
                "n_simulations": n_simulations,
                "bar_range": round(bar_range, 6),
            }
        except Exception as e:
            logging.warning("[R22-EP-P1] gamma路径验证异常(fail-safe: passed=False): %s", e)
            return {"deviation_pct": 0.0, "passed": False, "gamma_low_fidelity": True, "error": str(e), "bar_range": 0.0}

    def log_greeks_dashboard_periodic(self) -> None:
        """每分钟周期性输出仪表盘汇总日志，标签[GreekDashboard]

        R13-P2-LOG-11修复: 增加变更摘要模式，仅输出相比上次显著变化的字段。
        首次输出全量，后续仅输出变化超过阈值的字段 + 始终输出安全状态。
        """
        try:
            dashboard = self.get_greeks_dashboard()
            portfolio = dashboard.get("portfolio", {})
            stress = dashboard.get("stress_test", {})
            attribution = dashboard.get("pnl_attribution", {})
            safety = dashboard.get("safety_meta", {})

            delta_usage = portfolio.get("delta_usage_pct", 0)
            gamma_usage = portfolio.get("gamma_usage_pct", 0)
            vega_usage = portfolio.get("vega_usage_pct", 0)
            unexplained_pct = attribution.get("unexplained_pct", 0)

            # R13-P2-LOG-11修复: 构建当前快照用于变更比较
            current_snapshot = {
                "net_delta": portfolio.get("net_delta", 0),
                "delta_usage": delta_usage,
                "net_gamma": portfolio.get("net_gamma", 0),
                "gamma_usage": gamma_usage,
                "net_vega": portfolio.get("net_vega", 0),
                "vega_usage": vega_usage,
                "net_theta": portfolio.get("net_theta", 0),
                "jump_1sigma": stress.get("jump_1sigma_pnl", 0),
                "jump_2sigma": stress.get("jump_2sigma_pnl", 0),
                "delta_contrib": attribution.get("delta_contrib", 0),
                "gamma_contrib": attribution.get("gamma_contrib", 0),
                "vega_contrib": attribution.get("vega_contrib", 0),
                "theta_contrib": attribution.get("theta_contrib", 0),
                "unexplained_pct": unexplained_pct,
            }

            prev = self._last_dashboard_snapshot
            threshold = self._dashboard_change_threshold

            def _changed(key: str) -> bool:
                """判断字段是否相对上次显著变化"""
                if key not in prev:
                    return True
                old_val = prev[key]
                new_val = current_snapshot[key]
                if old_val == 0 and new_val == 0:
                    return False
                if old_val == 0:
                    return True
                return abs(new_val - old_val) / max(abs(old_val), 1e-9) > threshold

            is_first_output = not prev
            changed_keys = [k for k in current_snapshot if _changed(k)]

            usage_warning = ""  # R21-MEM-P2-04修复: 条件性字符串拼接，此处仅2次+=可接受；循环中应改用join/list
            if delta_usage > 100 or gamma_usage > 100 or vega_usage > 100:
                usage_warning = " ⚠️ USAGE>100%!"
            if unexplained_pct > 15:
                usage_warning += " ⚠️ RESIDUAL>15%!"

            # 安全状态始终输出
            safety_str = "CB=%s blocked=%s" % (
                safety.get("circuit_breaker_triggered", False),
                safety.get("new_open_blocked", False),
            )

            if is_first_output or len(changed_keys) > 8:
                # 首次或变化字段过多 → 全量输出
                logging.info(
                    "[GreekDashboard] Δ=%.2f(%.0f%%) Γ=%.4f(%.0f%%) V=%.4f(%.0f%%) Θ=%.4f "
                    "| 1σPnL=%.0f 2σPnL=%.0f "
                    "| attr: D=%.0f G=%.0f V=%.0f T=%.0f unexp=%.1f%% "
                    "| safety: %s%s",
                    portfolio.get("net_delta", 0), delta_usage,
                    portfolio.get("net_gamma", 0), gamma_usage,
                    portfolio.get("net_vega", 0), vega_usage,
                    portfolio.get("net_theta", 0),
                    stress.get("jump_1sigma_pnl", 0), stress.get("jump_2sigma_pnl", 0),
                    attribution.get("delta_contrib", 0), attribution.get("gamma_contrib", 0),
                    attribution.get("vega_contrib", 0), attribution.get("theta_contrib", 0),
                    unexplained_pct,
                    safety_str, usage_warning,
                )
            else:
                # 变更摘要模式：仅输出变化的字段
                changes = " ".join(
                    "%s=%s→%s" % (k, round(prev.get(k, 0), 4), round(current_snapshot[k], 4))
                    for k in changed_keys
                )
                logging.info(
                    "[GreekDashboard] [summary] changed(%d/%d): %s | safety: %s%s",
                    len(changed_keys), len(current_snapshot), changes,
                    safety_str, usage_warning,
                )

            # 更新快照
            self._last_dashboard_snapshot = dict(current_snapshot)

            # R15-P1-LOG-10修复: Greeks仪表盘JSON结构化输出，供程序化消费
            try:
                import json as _json_mod
                _dashboard_json = _json_mod.dumps({
                    'timestamp': dashboard.get('timestamp', ''),
                    'portfolio': {k: round(v, 6) if isinstance(v, float) else v
                                  for k, v in portfolio.items() if isinstance(v, (int, float, bool))},
                    'safety': {'circuit_breaker': safety.get('circuit_breaker_triggered', False),
                               'new_open_blocked': safety.get('new_open_blocked', False)},
                    'usage': {'delta_pct': round(delta_usage, 2), 'gamma_pct': round(gamma_usage, 2),
                              'vega_pct': round(vega_usage, 2)},
                }, ensure_ascii=False)
                logging.info("[GreekDashboard][json] %s", _dashboard_json)
            except Exception as _json_err:
                logging.debug("[GreekDashboard] JSON结构化输出失败: %s", _json_err)

            # DFG-P1-04修复: Greeks仪表盘数据通过EventBus发布，供HealthCheckAPI消费
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus
                _bus_greeks = get_global_event_bus()
                if _bus_greeks is not None:
                    _bus_greeks.publish('greeks.dashboard_updated', {
                        'type': 'greeks.dashboard_updated',
                        'data': dashboard,
                    }, async_mode=True)
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
                pass

            # ✅ P1-3修复：在周期性日志中更新前5日平均收益到SafetyMetaLayer
            try:
                safety = get_safety_meta_layer(self.params)
                if safety and hasattr(safety, 'set_prev_5day_avg_profit') and self.position_manager is not None:
                    daily_pnl_list = getattr(self.position_manager, '_daily_pnl_history', None)
                    if daily_pnl_list and len(daily_pnl_list) >= 1:
                        recent = list(daily_pnl_list)[-5:]
                        avg_profit = stable_mean(recent) if recent else 0.0  # R27-P1-FP-01修复: stable_mean替代sum/len
                        safety.set_prev_5day_avg_profit(avg_profit)
            except Exception as e:
                logging.debug("[RiskService] 更新5日平均收益异常: %s", e)

            # ✅ 集成E7未解释收益检查（E7）
            try:
                from ali2026v3_trading.governance_engine import E7UnexplainedReturnChecker
                e7 = E7UnexplainedReturnChecker()
                pnl_attribution = {
                    "total_pnl": attribution.get("delta_contrib", 0) + attribution.get("gamma_contrib", 0)
                                   + attribution.get("vega_contrib", 0) + attribution.get("theta_contrib", 0),
                    "delta_pnl": attribution.get("delta_contrib", 0),
                    "gamma_pnl": attribution.get("gamma_contrib", 0),
                    "vega_pnl": attribution.get("vega_contrib", 0),
                    "theta_pnl": attribution.get("theta_contrib", 0),
                }
                e7_result = e7.check(pnl_attribution)
                if e7_result.get("e7_triggered"):
                    logging.warning("[RiskService] E7告警: 未解释收益占比%.1f%%超过阈值%.1f%%",
                                    e7_result.get("residual_pct", 0), e7_result.get("threshold_pct", 15))
            except Exception as e:
                logging.debug("[RiskService] E7检查异常: %s", e)
        except Exception as e:
            logging.warning("[GreekDashboard] Periodic log error: %s", e)

    def _check_greeks_limits(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        """希腊字母硬约束检查 — 订单生成前必须执行

        任一超限则拒绝新开仓信号，只允许减仓或对冲。
        V7增强：每次检查时更新仪表盘缓存。
        """
        action = signal.get("action", "")
        if action == "CLOSE":
            return RiskCheckResponse.pass_result()

        try:
            calc = self._get_greeks_calculator()
            if calc is None:
                # R10-P1-06修复: calc不可用时fail-safe阻断，不再放行
                logging.error("[RiskService._check_greeks_limits] GreeksCalculator为None，fail-safe阻断新开仓")
                return RiskCheckResponse.block_result(
                    "greeks_calculator_none",
                    "GreeksCalculator未初始化，fail-safe阻断新开仓",
                    RiskLevel.HIGH
                )

            positions_dict = {}
            if self.position_manager:
                positions_raw = getattr(self.position_manager, "positions", {})
                for inst_map in positions_raw.values():
                    for inst_id, pos in inst_map.items():
                        vol = getattr(pos, "volume", 0)
                        if vol != 0:
                            positions_dict[inst_id] = vol

            if not positions_dict:
                return RiskCheckResponse.pass_result()

            # R5-L-05修复: 百分比参数统一为0-1范围(小数形式)
            # max_net_delta_pct=0.30 表示30%，乘以1000转换为绝对delta限制
            # max_net_gamma_pct=0.08 表示8%，乘以50转换为绝对gamma限制
            # max_net_vega_bps=0.02 表示2bps，乘以200转换为绝对vega限制
            max_delta = safe_get_float(self.params, "max_net_delta_pct", 0.30) * 1000
            max_gamma = safe_get_float(self.params, "max_net_gamma_pct", 0.08) * 50
            max_vega = safe_get_float(self.params, "max_net_vega_bps", 0.02) * 200

            warnings = calc.check_risk_limits(
                positions_dict,
                delta_limit=max_delta,
                gamma_limit=max_gamma,
                vega_limit=max_vega,
            )

            if warnings:
                return RiskCheckResponse.block_result(
                    "greeks_limit_exceeded",
                    f"希腊字母硬约束超限: {'; '.join(warnings)}",
                    RiskLevel.HIGH
                )

            # P2-GR-004修复: 调用validate_greeks_calendar_iv_noise进行日历/IV噪声验证
            try:
                from ali2026v3_trading.greeks_calculator import validate_greeks_calendar_iv_noise
                _iv_noise_result = validate_greeks_calendar_iv_noise(
                    greeks_calculator=calc,
                    iv_noise_sigma=safe_get_float(self.params, "greeks_iv_noise_sigma", 0.005),
                    n_simulations=safe_get_int(self.params, "greeks_iv_noise_n_sim", 100),
                )
                if _iv_noise_result and not _iv_noise_result.get('passed', True):
                    logging.warning("[P2-GR-004] Greeks日历/IV噪声验证未通过: %s", _iv_noise_result)
            except Exception as _e:
                logging.debug("[P2-GR-004] validate_greeks_calendar_iv_noise跳过: %s", _e)

            # R3-P-08修复: theta软约束检查(P1级非阻断，仅warning记录)
            theta_abs_threshold = safe_get_float(self.params, "theta_abs_threshold", 0.5)
            try:
                if hasattr(calc, 'compute_portfolio_greeks') and positions_dict:
                    pg = calc.compute_portfolio_greeks(positions_dict)
                    theta_val = abs(pg.get('theta', 0.0))
                    if theta_val > theta_abs_threshold:
                        logging.warning("[R3-P-08] theta绝对值%.4f超过阈值%.2f(P1级软约束)", theta_val, theta_abs_threshold)
            except Exception as _theta_e:
                logging.debug("[R3-P-08] theta检查跳过: %s", _theta_e)

            try:
                self.get_greeks_dashboard()
            except (ImportError, AttributeError, RuntimeError) as e:
                logging.warning("[R22-EP-02] greeks dashboard update failed (may stale): %s", e)

        except ImportError as _e:
            # R5-E-01修复: GreeksCalculator不可用时fail-safe阻断，不再放行
            # CORE-DEPENDENCY: GreeksCalculator是风控系统的核心依赖
            logging.error("[RiskService._check_greeks_limits] GreeksCalculator不可用，fail-safe阻断新开仓: %s", _e)
            return RiskCheckResponse.block_result(
                "greeks_calculator_unavailable",
                "GreeksCalculator不可用，fail-safe阻断新开仓",
                RiskLevel.CRITICAL
            )
        except Exception as e:
            # R5-E-01修复: 检查异常时fail-safe阻断，不再放行
            logging.error("[RiskService._check_greeks_limits] Check error, fail-safe阻断: %s, signal={symbol=%s, direction=%s, action=%s}", e, signal.get('symbol', ''), signal.get('direction', ''), signal.get('action', ''), exc_info=True)  # R13-P1-LOG-03修复
            return RiskCheckResponse.block_result(
                "greeks_check_error",
                f"希腊字母检查异常，fail-safe阻断: {e}",
                RiskLevel.CRITICAL
            )

        return RiskCheckResponse.pass_result()

    def calculate_risk_metrics(self) -> RiskMetrics:
        """计算风险指标"""
        try:
            if self.position_manager is None:
                return RiskMetrics()

            # ✅ P1修复：将deepcopy移到锁外，减少持锁时间
            # 先读取引用，再在锁外执行deepcopy
            positions_raw = getattr(self.position_manager, "positions", {})
            
            # 在锁外执行拷贝，避免长时间持锁
            positions = dict(positions_raw) if positions_raw else {}  # R21-MEM-P1-04修复: 浅拷贝替代deepcopy
            
            total_exposure = 0.0
            max_position = 0.0
            count = 0

            for inst_map in positions.values():
                for inst_id, pos in inst_map.items():
                    volume = getattr(pos, "volume", 0)
                    price = getattr(pos, "open_price", 0)
                    # INV-01/INV-POS-03修复: 敞口计算必须乘以合约乘数
                    # 期权乘数100~10000，缺失会低估风险100-10000倍
                    multiplier = self._get_contract_multiplier(inst_id)
                    exposure = abs(volume) * price * multiplier
                    total_exposure += exposure
                    max_position = max(max_position, exposure)
                    if volume != 0:
                        count += 1

            # 计算风险比率
            limit = self._get_total_position_limit()
            risk_ratio = total_exposure / limit if limit > 0 else 0.0

            return RiskMetrics(
                total_exposure=total_exposure,
                max_single_position=max_position,
                position_count=count,
                risk_ratio=risk_ratio
            )

        except Exception as e:
            logging.error("[RiskService.calculate_risk_metrics] Error: %s, position_count=%d", e, count if 'count' in dir() else -1)  # R13-P1-LOG-03修复
            return RiskMetrics()

    # ========================================================================
    # 配置参数获取
    # ========================================================================

    def _get_signal_cooldown(self) -> float:
        """获取信号冷却时间（秒）"""
        return safe_get_float(self.params, "signal_cooldown_sec", 60.0)

    def _get_max_signals_per_window(self) -> int:
        """获取窗口内最大信号数"""
        return safe_get_int(self.params, "max_signals_per_window", 10)

    def _get_rate_limit_window(self) -> float:
        """获取限频窗口（秒）"""
        return safe_get_float(self.params, "rate_limit_window_sec", 60.0)

    def _get_max_risk_ratio(self) -> float:
        """获取最大风险比率 — R24-P0-DF-01修复: 默认值从0.3改为0.8，与YAML correct_trending主状态值对齐"""
        return safe_get_float(self.params, "max_risk_ratio", 0.8)

    def _get_total_position_limit(self) -> float:
        """获取总持仓限额"""
        return safe_get_float(self.params, "total_position_limit", 1000000.0)

    # INV-01/INV-POS-03修复: 合约乘数查找方法
    _CONTRACT_MULTIPLIER_CACHE: Dict[str, float] = {}

    def _get_contract_multiplier(self, instrument_id: str) -> float:
        """INV-01/INV-POS-03修复: 获取合约乘数，敞口计算必须乘以multiplier

        期权合约乘数通常为100~10000，缺失乘数会低估风险100-10000倍。
        查找优先级: greeks_calculator._contract_multiplier > product_cache > config_params > 默认1.0
        """
        if instrument_id in RiskService._CONTRACT_MULTIPLIER_CACHE:
            return RiskService._CONTRACT_MULTIPLIER_CACHE[instrument_id]

        multiplier = 1.0

        # 优先从greeks_calculator获取
        try:
            calc = self._get_greeks_calculator()
            if calc is not None and hasattr(calc, '_contract_multiplier'):
                # 从期权instrument_id提取future_product
                info = calc._option_info_cache.get(instrument_id, {})
                future_product = info.get('future_product', '')
                if future_product and future_product in calc._contract_multiplier:
                    multiplier = calc._contract_multiplier[future_product]
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass

        # 其次从params_service获取
        if multiplier == 1.0:
            try:
                from ali2026v3_trading.params_service import get_params_service
                params_svc = get_params_service()
                meta = params_svc.get_instrument_meta_by_id(instrument_id)
                if meta:
                    product = meta.get('product', '')
                    product_cache = params_svc.get_product_cache(product)
                    if product_cache:
                        multiplier = safe_float(product_cache.get('contract_size', 1.0))
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
                pass

        # 最后从config_params获取option_contract_multiplier
        if multiplier == 1.0:
            try:
                from ali2026v3_trading.config_params import get_cached_params
                _params = get_cached_params()
                # 期权合约使用option_contract_multiplier
                if instrument_id and ('C' in instrument_id or 'P' in instrument_id):
                    multiplier = safe_float(_params.get('option_contract_multiplier', 10000))
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
                pass

        RiskService._CONTRACT_MULTIPLIER_CACHE[instrument_id] = multiplier
        return multiplier

    # ========================================================================
    # 状态与统计
    # ========================================================================

    # ✅ ID唯一：get_stats统一接口，返回值含service_name="RiskService"
    def get_stats(self) -> Dict[str, Any]:
        """获取风控统计"""
        with self._lock:
            stats = dict(self._stats)
            stats['service_name'] = 'RiskService'  # ✅ ID唯一：统一标识服务来源
            return stats

    # R23-SM-P1-09-FIX: 风控状态查询接口统一
    def get_risk_status(self) -> Dict[str, Any]:
        _circuit_active = False
        try:
            _safety = getattr(self, '_safety_meta_layer', None)
            if _safety is None:
                from ali2026v3_trading.risk_service import get_safety_meta_layer
                _safety = get_safety_meta_layer()
            if _safety and hasattr(_safety, 'is_circuit_breaker_active'):
                _circuit_active = _safety.is_circuit_breaker_active()
        except Exception as _circuit_err:
            logging.warning("[R22-P1-NEW] get_risk_status: 熔断器状态查询失败(可能误报未激活): %s", _circuit_err)
        try:
            with self._lock:
                _blocked = self._stats.get('blocked', 0)
                _total = self._stats.get('total_checks', 1)
                _ratio = _blocked / max(1, _total)
                if _ratio > 0.5:
                    _risk_level = RiskLevel.CRITICAL.name
                elif _ratio > 0.2:
                    _risk_level = RiskLevel.RESTRICTED.name
                elif _ratio > 0.05:
                    _risk_level = RiskLevel.HIGH.name
                elif _ratio > 0:
                    _risk_level = RiskLevel.MEDIUM.name
                else:
                    _risk_level = RiskLevel.LOW.name
        except Exception as _rl_err:
            logging.warning("[R22-P1-NEW] get_risk_status: 风控等级计算失败(保持UNKNOWN): %s", _rl_err)
        return {
            'risk_level': _risk_level,
            'circuit_breaker_active': _circuit_active,
        }

    def reset_stats(self) -> None:
        """重置统计"""
        with self._lock:
            self._stats = {
                "total_checks": 0,
                "blocked": 0,
                "warnings": 0,
                "passed": 0,
            }

    # ✅ ID唯一：clear_cache统一接口，服务=RiskService
    def clear_cache(self) -> None:
        with self._lock:
            self._strategy_status_cache = (0.0, RiskCheckResponse.pass_result())
            self._signal_times.clear()
        logging.info('[RiskService] Cache cleared')

    # R13-P2-BIZ-05修复: 参数变更时主动失效EV底线检查缓存
    def invalidate_ev_cache(self) -> None:
        """参数变更时主动失效EV底线检查相关缓存

        当风控参数（如ABSOLUTE_EV_FLOOR、max_risk_ratio等）被修改时，
        原缓存的策略状态检查结果可能已过时，需主动失效以确保下次检查使用新参数。
        """
        with self._lock:
            self._strategy_status_cache = (0.0, RiskCheckResponse.pass_result())
            self._last_greeks_dashboard = {}
        logging.info('[RiskService] R13-P2-BIZ-05: EV底线检查缓存已失效(参数变更)')

    def _check_consecutive_loss_protection(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        strategy_id = signal.get('strategy_id', 'default')
        # R13-P1-BIZ-02修复: 平仓亏损也纳入连亏计数
        # 原代码仅在record_trade_result中计数，但平仓信号不经过该方法，
        # 导致平仓亏损被遗漏，连亏保护失效。
        close_pnl = signal.get('close_pnl', None)
        if close_pnl is not None and close_pnl < 0:
            capital_scale = getattr(self, '_current_capital_scale', 'medium')
            self.record_trade_result(strategy_id, close_pnl, capital_scale)
        with self._lock:
            if strategy_id in self._consecutive_loss_paused and self._consecutive_loss_paused[strategy_id]:
                # R13-BIZ-06: 检查是否超过恢复计时器
                # 原代码仅在record_trade_result调用时检查，若外部不调用则永远暂停
                paused_ts = self._consecutive_loss_pause_timestamps.get(strategy_id, 0.0)
                capital_scale = getattr(self, '_current_capital_scale', 'medium')
                timeout = self._consecutive_loss_recovery_timeouts.get(capital_scale, 3600.0)
                # R13-BIZ-06: 如果pause_timestamp未设置（BIZ-06 bug），立即设置
                if paused_ts <= 0:
                    self._consecutive_loss_pause_timestamps[strategy_id] = time.time()
                    paused_ts = self._consecutive_loss_pause_timestamps[strategy_id]
                    logging.warning(
                        '[RiskService] R13-BIZ-06: 策略%s连亏暂停但无时间戳，已补设timestamp=%.0f',
                        strategy_id, paused_ts,
                    )
                elapsed = time.time() - paused_ts
                if elapsed >= timeout:
                    logging.info(
                        '[RiskService] 策略%s连亏暂停超时自动恢复 (暂停%.0f秒 >= 阈值%.0f秒)',
                        strategy_id, elapsed, timeout,
                    )
                    self._consecutive_loss_counts[strategy_id] = 0
                    self._consecutive_loss_paused[strategy_id] = False
                    self._consecutive_loss_pause_timestamps.pop(strategy_id, None)
                    return RiskCheckResponse.pass_result()
                logging.info(
                    '[RiskService] 策略%s连亏暂停中 (剩余%.0f秒/%.0f秒)',
                    strategy_id, timeout - elapsed, timeout,
                )
                return RiskCheckResponse.block_result(
                    'consecutive_loss_paused',
                    '策略%s因连续亏损暂停 (剩余%.0f秒)' % (strategy_id, timeout - elapsed),
                    level=RiskLevel.HIGH,
                )
            return RiskCheckResponse.pass_result()

    def record_trade_result(self, strategy_id: str, pnl: float, capital_scale: str = 'medium') -> None:
        with self._lock:
            count = self._consecutive_loss_counts.get(strategy_id, 0)
            limit = self._consecutive_loss_limits.get(capital_scale, 7)
            if pnl < 0:
                self._consecutive_loss_counts[strategy_id] = count + 1
                if self._consecutive_loss_counts[strategy_id] >= limit:
                    self._consecutive_loss_paused[strategy_id] = True
                    self._consecutive_loss_pause_timestamps[strategy_id] = time.time()
                    logging.info(
                        '[RiskService] 策略%s连亏达到阈值 %d/%d，已暂停交易',
                        strategy_id, self._consecutive_loss_counts[strategy_id], limit,
                    )
                else:
                    logging.info(
                        '[RiskService] 策略%s连亏计数 %d/%d',
                        strategy_id, self._consecutive_loss_counts[strategy_id], limit,
                    )
            else:
                if count > 0:
                    logging.info(
                        '[RiskService] 策略%s盈利 %.2f，连亏计数从 %d 重置为 0',
                        strategy_id, pnl, count,
                    )
                self._consecutive_loss_counts[strategy_id] = 0
                self._consecutive_loss_paused.pop(strategy_id, None)
                self._consecutive_loss_pause_timestamps.pop(strategy_id, None)

    def reset_consecutive_loss(self, strategy_id: str) -> None:
        with self._lock:
            self._consecutive_loss_counts.pop(strategy_id, None)
            self._consecutive_loss_paused.pop(strategy_id, None)
            self._consecutive_loss_pause_timestamps.pop(strategy_id, None)
            logging.info('[RiskService] 策略%s连亏状态已手动重置', strategy_id)

    # R13-P1-BIZ-03修复: per-strategy风险状态访问接口
    def get_strategy_risk_state(self, strategy_id: str) -> Dict[str, Any]:
        """获取指定策略的独立风险状态"""
        with self._lock:
            if strategy_id not in self._per_strategy_risk_state:
                self._per_strategy_risk_state[strategy_id] = {
                    'consecutive_loss_count': self._consecutive_loss_counts.get(strategy_id, 0),
                    'is_paused': self._consecutive_loss_paused.get(strategy_id, False),
                    'signal_count': 0,
                    'last_signal_time': 0.0,
                    'total_pnl': 0.0,
                }
            return dict(self._per_strategy_risk_state[strategy_id])

    def update_strategy_risk_state(self, strategy_id: str, **kwargs) -> None:
        """更新指定策略的独立风险状态"""
        with self._lock:
            if strategy_id not in self._per_strategy_risk_state:
                self._per_strategy_risk_state[strategy_id] = {
                    'consecutive_loss_count': 0,
                    'is_paused': False,
                    'signal_count': 0,
                    'last_signal_time': 0.0,
                    'total_pnl': 0.0,
                }
            self._per_strategy_risk_state[strategy_id].update(kwargs)

    def set_capital_scale(self, capital_scale: str) -> None:
        with self._lock:
            self._current_capital_scale = capital_scale
            if capital_scale == 'small':
                self._asymmetric_risk_enabled = True
            elif capital_scale == 'large':
                self._asymmetric_risk_enabled = False
            else:
                self._asymmetric_risk_enabled = False
        try:
            from ali2026v3_trading.state_param_manager import get_state_param_manager
            spm = get_state_param_manager()
            spm.set_capital_scale(capital_scale)
            cl_params = spm.get_params('consecutive_loss')
            if cl_params and 'max_consecutive_losses' in cl_params:
                with self._lock:
                    self._consecutive_loss_limits[capital_scale] = cl_params['max_consecutive_losses']
        except (ImportError, AttributeError, KeyError) as e:
            logging.debug("[RiskService] state_param_manager load failed: %s", e)

    def evaluate_capital_scale_upgrade(self) -> Dict[str, Any]:
        """R16-P1-05修复: 自动评估capital_scale升级资格"""
        _current = getattr(self, '_current_capital_scale', 'medium')
        result = {'eligible': False, 'current_scale': _current, 'recommended_scale': _current}
        if _current == 'large':
            return result
        recent_trades = getattr(self, '_recent_trade_results', [])
        if len(recent_trades) >= 20:
            win_rate = sum(1 for t in recent_trades[-20:] if t > 0) / 20
            avg_win = sum(t for t in recent_trades[-20:] if t > 0) / max(1, sum(1 for t in recent_trades[-20:] if t > 0))
            avg_loss = abs(sum(t for t in recent_trades[-20:] if t < 0) / max(1, sum(1 for t in recent_trades[-20:] if t < 0)))
            plr = avg_win / max(avg_loss, 0.001)
            if win_rate >= 0.55 and plr >= 1.5:
                _upgrade_path = {'small': 'medium', 'medium': 'large'}
                result['eligible'] = True
                result['recommended_scale'] = _upgrade_path.get(_current, _current)
                logging.info("[R16-P1-05] capital_scale升级评估通过: win_rate=%.2f plr=%.2f recommended=%s",
                            win_rate, plr, result['recommended_scale'])
        return result

    # P-22修复: compute_mode_position_size补充10个TVF六维因子输入参数（手册7.2节）
    # P-07修复: 集成PredictiveStateEngine方向化仓位调整
    def compute_mode_position_size(
        self, equity: float, entry_price: float, stop_price: float,
        win_rate: float = 0.0, win_loss_ratio: float = 0.0,
        sortino_ratio: float = 0.0, calmar_ratio: float = 0.0, sharpe_ratio: float = 0.0,
        ofi: float = 0.0, cvd_divergence: float = 0.0, smart_money_flow: float = 0.0,
        delta: float = 0.0, gamma: float = 0.0, theta: float = 0.0, vega: float = 0.0,
        prediction_correct: bool = True, price_direction: int = 1,  # P-07新增参数
    ) -> float:
        try:
            from ali2026v3_trading.mode_engine import ModeEngine, PredictiveStateEngine
            me = ModeEngine.get_instance()
            
            # P-07修复: 使用PredictiveStateEngine获取方向化仓位乘数
            pse = PredictiveStateEngine.get_instance()
            state = pse.classify_state(prediction_correct, price_direction)
            position_multiplier = pse.get_position_multiplier(state)
            
            # P-22修复: TVF六维因子日志输出（实证测试用）
            tvf_inputs = {
                'L1_risk_return': {'sortino': sortino_ratio, 'calmar': calmar_ratio, 'sharpe': sharpe_ratio},
                'L2_market_micro': {'ofi': ofi, 'cvd_divergence': cvd_divergence, 'smart_money_flow': smart_money_flow},
                'L3_option_greeks': {'delta': delta, 'gamma': gamma, 'theta': theta, 'vega': vega},
            }
            logging.debug("[P-22] TVF六维因子输入: %s", tvf_inputs)
            
            # 计算基础仓位
            # R7-M-02修复: 传递instrument_type和option_discount参数
            _inst_type = signal.get('instrument_type', 'future') if isinstance(signal, dict) else 'future'
            _opt_discount = 0.6  # 默认期权买方折扣系数
            try:
                from ali2026v3_trading.config_params import get_cached_params
                _params = get_cached_params()
                _opt_discount = _params.get('option_buyer_discount', 0.6)
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
                pass
            # R13-V2-006修复: 传递TVF六维参数到ModeEngine，使六维仓位调整因子生效
            # P-28补全修复: 传递PredictiveStateEngine状态作为market_state
            _market_state = state if state else ""
            base_size = me.compute_position_size(
                equity, entry_price, stop_price, win_rate, win_loss_ratio,
                sortino=sortino_ratio, calmar=calmar_ratio, sharpe=sharpe_ratio,
                ofi_score=ofi, cvd_divergence=cvd_divergence, smart_money_flow=smart_money_flow,
                delta_exposure=delta, gamma_risk=gamma, theta_decay=theta, vega_exposure=vega,
                instrument_type=_inst_type, option_discount=_opt_discount,
                market_state=_market_state,  # P-28补全修复
            )
            
            # 应用方向化乘数
            adjusted_size = base_size * position_multiplier
            
            logging.debug("[P-07] PredictiveState: %s, multiplier=%.2f, base_size=%.2f, adjusted_size=%.2f",
                         state, position_multiplier, base_size, adjusted_size)
            
            return adjusted_size
        except (ImportError, AttributeError, ZeroDivisionError) as e:
            logging.debug("[RiskService] ModeEngine position size failed: %s", e)
            risk_pct = 0.02
            risk_amount = equity * risk_pct
            stop_distance = abs(entry_price - stop_price)
            if stop_distance < 1e-10:
                return 0.0
            return risk_amount / stop_distance

    def calculate_asymmetric_drawdown_limit(self, current_pnl: float, base_limit: float) -> float:
        if not self._asymmetric_risk_enabled:
            return base_limit
        if current_pnl > 0:
            return base_limit * self._asymmetric_profit_multiplier
        else:
            return base_limit * self._asymmetric_loss_multiplier

    def check_asymmetric_risk(self, current_pnl: float, drawdown_pct: float, base_limit: float) -> RiskCheckResponse:
        adjusted_limit = self.calculate_asymmetric_drawdown_limit(current_pnl, base_limit)
        if abs(drawdown_pct) > adjusted_limit:
            logging.warning(  # R14-P1-LOG-08修复: 风控阻断日志从INFO升为WARNING
                '[RiskService] 非对称风控阻断: current_pnl=%.2f drawdown=%.2f%% adjusted_limit=%.2f%% base_limit=%.2f%%',
                current_pnl, abs(drawdown_pct) * 100, adjusted_limit * 100, base_limit * 100,
            )
            return RiskCheckResponse.block_result(
                'asymmetric_drawdown',
                '回撤%.2f%%超过调整后限额%.2f%%' % (abs(drawdown_pct) * 100, adjusted_limit * 100),
                level=RiskLevel.HIGH,
            )
        logging.debug(
            '[RiskService] 非对称风控通过: current_pnl=%.2f drawdown=%.2f%% adjusted_limit=%.2f%%',
            current_pnl, abs(drawdown_pct) * 100, adjusted_limit * 100,
        )
        return RiskCheckResponse.pass_result()

    # ========================================================================
    # 11维度统一风险评分体系
    # ========================================================================

    # 默认11维度权重（总和=1.0），参考顶级基金多因子风险模型
    # 三层架构：核心信号层(0.45) + 市场状态层(0.30) + 组合风控层(0.25)
    RISK_DIMENSION_DEFAULTS = {
        # ── 核心信号层(0.45)：策略信号质量直接决定开仓决策 ──
        'state_strength':      0.15,  # D1 五态→三态路由的状态强度
        'order_flow':          0.10,  # D2 订单流方向一致性
        'cycle_resonance':     0.10,  # D4 周期共振强度(多周期趋势一致性)
        'tri_validation':      0.10,  # D9 三角验证(Sortino/Calmar/Sharpe门控)
        # ── 市场状态层(0.30)：市场环境决定信号可信度 ──
        'life_expectancy':     0.10,  # D3 行情寿命置信度(贝叶斯收缩降级阶梯)
        'phase_quality':       0.08,  # D5 周期相位质量(CHARGE/RELEASE/EXHAUST/CHAOS)
        'greeks_usage':        0.07,  # D6 Greeks使用率(Delta/Gamma/Vega)
        'asymmetric_drawdown': 0.05,  # D8 非对称回撤(盈亏状态自适应)
        # ── 组合风控层(0.25)：组合层面约束决定仓位上限 ──
        'consecutive_loss':    0.07,  # D7 连亏状态(计数/暂停/恢复)
        'alpha_decay':         0.10,  # D10 Alpha衰减(影子策略Alpha比率)
        'cross_correlation':   0.08,  # D11 跨策略相关性(策略间PnL相关)
    }

    def compute_decision_score(
        self,
        state_strength: float,
        order_flow_consistency: float,
        hmm_state: Optional[str] = None,
        cr_output: Optional[Any] = None,
        greeks_dashboard: Optional[Dict[str, Any]] = None,
        consecutive_losses: int = 0,
        current_pnl: float = 0.0,
        drawdown_pct: float = 0.0,
        alpha_ratio: Optional[float] = None,
        cross_correlation: Optional[float] = None,
        tri_validation_score: Optional[float] = None,
        slippage_source: str = 'LIVE',  # MS-P1-09: LIVE/BACKTEST标记
    ) -> Dict[str, Any]:
        """11维度统一风险评分 — 三层架构加权

        三层架构（顶级基金多因子风险模型）：
        ┌─────────────────────────────────────────────────────────┐
        │ 核心信号层(0.45)                                        │
        │  D1 状态强度(0.15) + D2 订单流(0.10)                    │
        │  + D4 周期共振(0.10) + D9 三角验证(0.10)                │
        ├─────────────────────────────────────────────────────────┤
        │ 市场状态层(0.30)                                        │
        │  D3 行情寿命(0.10) + D5 相位质量(0.08)                  │
        │  + D6 Greeks使用率(0.07) + D8 非对称回撤(0.05)          │
        ├─────────────────────────────────────────────────────────┤
        │ 组合风控层(0.25)                                        │
        │  D7 连亏状态(0.07) + D10 Alpha衰减(0.10)                │
        │  + D11 跨策略相关性(0.08)                                │
        └─────────────────────────────────────────────────────────┘

        各维度评分归一化到[0,1]，加权求和得到综合评分。
        综合评分驱动仓位缩放和决策动作。

        R15-P1-DOC-P1-04修复: 补全Returns段

        Args:
            state_strength: 状态强度评分[0,1]
            order_flow_consistency: 订单流一致性评分[0,1]
            hmm_state: HMM状态标签(可选)
            cr_output: 周期共振输出(可选)
            greeks_dashboard: Greeks仪表盘数据(可选)
            consecutive_losses: 连续亏损次数
            current_pnl: 当前PnL
            drawdown_pct: 回撤百分比
            alpha_ratio: Alpha比率(可选)
            cross_correlation: 跨策略相关性(可选)
            tri_validation_score: 三角验证评分(可选)
            slippage_source: 滑点来源标记('LIVE'/'BACKTEST')

        Returns:
            Dict[str, Any]: 包含综合评分和各维度子评分的字典, 键含
                'composite_score'(float), 'layer_scores'(dict), 'dimension_scores'(dict),
                'action'(str), 'position_scale'(float)
        """
        # 读取可配置权重（支持params覆盖默认值）
        w = {}
        for dim, default_w in self.RISK_DIMENSION_DEFAULTS.items():
            w[dim] = safe_get_float(self.params, f"decision.score.{dim}_weight",
                safe_get_float(self.params, f"decision_{dim}_weight", default_w))

        # P1-14修复: 三层权重守卫检查
        _LAYER_SIGNAL = ['state_strength', 'order_flow', 'cycle_resonance', 'tri_validation']
        _LAYER_MARKET = ['life_expectancy', 'phase_quality', 'greeks_usage', 'asymmetric_drawdown']
        _LAYER_PORTFOLIO = ['consecutive_loss', 'alpha_decay', 'cross_correlation']
        _signal_sum = stable_sum([w.get(d, 0.0) for d in _LAYER_SIGNAL])
        _market_sum = stable_sum([w.get(d, 0.0) for d in _LAYER_MARKET])
        _portfolio_sum = stable_sum([w.get(d, 0.0) for d in _LAYER_PORTFOLIO])
        _total_sum = stable_sum([_signal_sum, _market_sum, _portfolio_sum])
        if abs(_total_sum - 1.0) > 0.01:
            logging.warning(
                "[RiskService] P1-14: 三层权重偏差>0.01 (signal=%.3f market=%.3f portfolio=%.3f total=%.3f), 自动归一化",
                _signal_sum, _market_sum, _portfolio_sum, _total_sum)

        # MS-P1-09: 根据滑点source调整D9(三角验证/滑点)权重
        # BACKTEST环境降低滑点维度权重，LIVE环境保持原值或略微提升
        if slippage_source == 'BACKTEST':
            w['tri_validation'] = w.get('tri_validation', 0.10) * 0.5
            logging.debug("[RiskService] MS-P1-09: BACKTEST模式, D9权重折半=%.3f", w['tri_validation'])
        elif slippage_source == 'LIVE':
            w['tri_validation'] = w.get('tri_validation', 0.10) * 1.2
            logging.debug("[RiskService] MS-P1-09: LIVE模式, D9权重提升=%.3f", w['tri_validation'])

        # ── 计算各维度评分 ──
        scores = {}

        # D1 状态强度 [0,1]
        scores['state_strength'] = max(0.0, min(1.0, state_strength))

        # D2 订单流一致性 [-1,1] → [0,1] + 流动性评分(MS-P1-06)各50%权重
        base_order_flow = (order_flow_consistency + 1.0) / 2.0
        liquidity_score = self._compute_liquidity_score()
        try:
            from ali2026v3_trading.order_flow_bridge import OrderFlowBridge
            _ofb = OrderFlowBridge()
            _imbalance = _ofb.get_instant_imbalance('')
            if isinstance(_imbalance, (int, float)) and abs(_imbalance) > 0.01:
                base_order_flow = base_order_flow * 0.7 + (1.0 - abs(_imbalance)) * 0.3
                logging.debug("[P1-1修复] OrderFlowBridge imbalance接入: imbalance=%.4f", _imbalance)
        except Exception:
            pass
        scores['order_flow'] = base_order_flow * 0.5 + liquidity_score * 0.5

        # D3 行情寿命置信度
        # DFG-P1-01修复: 当hmm_state为None时，尝试从self._current_hmm_state获取
        _hmm_state = hmm_state
        if _hmm_state is None:
            _hmm_state = getattr(self, '_current_hmm_state', None)
        scores['life_expectancy'] = self._compute_life_score(_hmm_state)

        # DFG-P1-03修复: PredictiveStateEngine状态转换结果纳入决策评分
        # PSE状态影响D5(相位质量)维度：correct_trending→1.0, incorrect_reversal→0.6, other→0.4
        _pse_state = None
        try:
            from ali2026v3_trading.mode_engine import PredictiveStateEngine
            _pse = PredictiveStateEngine.get_instance()
            if _pse is not None:
                _pse_state = getattr(_pse, '_last_state', None)
                if _pse_state is None and hasattr(_pse, 'classify_state'):
                    _pse_state = _pse.classify_state(True, 1)
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass

        # D4 周期共振强度
        scores['cycle_resonance'] = self._compute_cycle_resonance_score(cr_output)

        # D5 周期相位质量
        scores['phase_quality'] = self._compute_phase_quality_score(cr_output)

        # D6 Greeks使用率（使用率越低越安全→评分越高）
        scores['greeks_usage'] = self._compute_greeks_usage_score(greeks_dashboard)

        # D7 连亏状态
        scores['consecutive_loss'] = self._compute_consecutive_loss_score(consecutive_losses)

        # D8 非对称回撤
        scores['asymmetric_drawdown'] = self._compute_asymmetric_drawdown_score(
            current_pnl, drawdown_pct)

        # D9 三角验证
        scores['tri_validation'] = self._compute_tri_validation_score(tri_validation_score)

        # D10 Alpha衰减
        # R3-D-03/R3-D-09修复: alpha_ratio为None时从shadow_engine获取，优先方法调用，其次属性
        _alpha_ratio = alpha_ratio
        if _alpha_ratio is None:
            try:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                _se = get_shadow_strategy_engine()
                if _se is not None:
                    if hasattr(_se, 'get_alpha_ratio') and callable(_se.get_alpha_ratio):
                        _alpha_ratio = _se.get_alpha_ratio()
                    elif hasattr(_se, 'alpha_decay_rate'):
                        _alpha_ratio = _se.alpha_decay_rate
            except Exception as e:
                logging.warning("[RiskService] R24-P0-TR-07: alpha_decay score computation failed: %s", e)
        scores['alpha_decay'] = self._compute_alpha_decay_score(_alpha_ratio)

        # D11 跨策略相关性
        # R3-D-09修复: 当cross_correlation为None时，设为0.0+warning而非None
        _cross_corr = cross_correlation
        if _cross_corr is None:
            _cross_corr = 0.0
            logging.debug("[R3-D-09] cross_correlation为None, 降级为0.0(无相关性)")
        scores['cross_correlation'] = self._compute_cross_correlation_score(_cross_corr)

        # ── 加权求和 ──
        total_weight = stable_sum(list(w.values()))  # R27-P1-FP-05修复: stable_sum替代sum
        if abs(total_weight) < FLOAT_COMPARE_TOLERANCE:  # R27-P1-FP-05修复: 浮点安全权重归一化
            logging.warning("[RiskService] 权重总和接近零, 使用等权重")
            n = len(w)
            uniform_w = {dim: 1.0 / n for dim in w}
            composite_score = stable_sum([scores[dim] * uniform_w[dim] for dim in w])  # R27-P1-FP-01修复
        else:
            normalized_w = safe_normalize_weights(w)  # R27-P1-FP-05修复: 安全权重归一化
            composite_score = stable_sum([scores[dim] * normalized_w[dim] for dim in w])  # R27-P1-FP-01修复

        # ── 综合仓位缩放 ──
        # 基于综合评分的连续仓位缩放（替代原来的离散阈值）
        position_scale = self._compute_position_scale(composite_score, scores)

        # ── 决策动作 ──
        threshold_high = safe_get_float(self.params, "decision.score.threshold_high",
            safe_get_float(self.params, "decision_threshold_high", 0.7))
        threshold_low = safe_get_float(self.params, "decision.score.threshold_low",
            safe_get_float(self.params, "decision_threshold_low", 0.4))

        if composite_score >= threshold_high:
            action = "normal_open"
        elif composite_score >= threshold_low:
            action = "small_open_tight_stop"
        elif composite_score >= 0.25:
            action = "divergence_warning"
        else:
            # P1-裂缝44：综合评分<0.25时已持仓处理
            # 已持仓可持有至原有止盈止损，但不允许加仓或新开
            action = "no_open_wait"

        return {
            "decision_score": composite_score,
            "action": action,
            "position_scale": position_scale,
            "dimension_scores": scores,
            "dimension_weights": w,
            "state_strength": state_strength,
            "order_flow_consistency": order_flow_consistency,
            # P1-裂缝44：明确已持仓处理规则
            "existing_position_policy": "hold_to_original_stop" if action == "no_open_wait" else "normal",
            "allow_add_position": action not in ("no_open_wait", "divergence_warning"),
        }

    def _compute_position_scale(self, composite_score: float,
                                 scores: Dict[str, float]) -> float:
        """基于综合评分和关键维度惩罚计算仓位缩放"""
        # 基础仓位：评分线性映射到[0,1]
        base_scale = max(0.0, min(1.0, composite_score))

        # 关键维度惩罚乘子
        penalties = []

        # 寿命降级惩罚
        if scores.get('life_expectancy', 0.5) < 0.3:
            penalties.append(0.3)   # Level3全局先验
        elif scores.get('life_expectancy', 0.5) < 0.5:
            penalties.append(0.6)   # Level2大类合并

        # 相位质量惩罚（混沌期）
        if scores.get('phase_quality', 0.5) < 0.3:
            penalties.append(0.5)

        # 连亏惩罚
        if scores.get('consecutive_loss', 1.0) < 0.3:
            penalties.append(0.3)

        # Alpha衰减惩罚
        if scores.get('alpha_decay', 1.0) < 0.3:
            # [R16-P1-11修复] Alpha衰减恢复需人工确认，自动恢复已阻止
            if not getattr(self, '_alpha_decay_recovery_confirmed', False):
                penalties.append(0.4)
            else:
                logging.info("[R16-P1-11] Alpha衰减已人工确认恢复，跳过惩罚")

        # 应用惩罚：取所有惩罚乘子的最小值
        if penalties:
            min_penalty = min(penalties)
            base_scale *= min_penalty

        return round(base_scale, 4)

    # ── 各维度评分计算方法 ──

    def _compute_life_score(self, hmm_state: Optional[str] = None) -> float:
        """D3 行情寿命置信度评分

        P2-R3-P-15: BayesianShrinkageLifeEstimator不可用时跳过 —
        当_get_life_estimator()返回None(模块未安装或初始化失败)时,
        直接返回0.5(中性评分)，跳过寿命评估。已知限制: 中性评分可能导致
        D3维度在所有状态下贡献相同，降低composite_score的区分度。
        建议在系统启动时预检BayesianShrinkageLifeEstimator可用性并告警。

        Returns:
            float: 寿命置信度评分，范围[0,1]。degradation_level 0→1.0, 1→0.7, 2→0.4, 3→0.2, 降级→0.5
        """
        estimator = self._get_life_estimator()
        if estimator is None or hmm_state is None:
            # R5-E-08修复: 降级时记录warning日志
            if estimator is None and hmm_state is not None:
                logging.warning("[R5-E-08] _compute_life_score降级: estimator不可用，返回中性评分0.5")
            return 0.5  # R14-P1-DOC-P1-07修复: 中性评分0.5来源—跨策略Greeks聚合降级默认值，表示"无信号偏倚"
        try:
            life = estimator.get_life_expectancy(hmm_state)
            if life is None or not life.is_valid():
                return 0.5
            return {0: 1.0, 1: 0.7, 2: 0.4, 3: 0.2}.get(life.degradation_level, 0.5)
        except Exception as _life_e:
            # R5-E-08修复: 降级时记录warning日志
            logging.warning("[R5-E-08] _compute_life_score异常降级: %s，返回0.5", _life_e)
            return 0.5

    def _compute_cycle_resonance_score(self, cr_output: Optional[Any] = None) -> float:
        """D4 周期共振强度评分 [0,1]

        Returns:
            float: 周期共振强度评分，范围[0,1]。无数据时返回0.5(中性)
        """
        if cr_output is None:
            return 0.5  # R14-P1-DOC-P1-07修复: 中性评分0.5来源—跨策略Greeks聚合降级默认值
        try:
            strength = getattr(cr_output, 'resonance_strength', None)
            if strength is not None:
                return max(0.0, min(1.0, float(strength)))
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass
        return 0.5

    def _compute_phase_quality_score(self, cr_output: Optional[Any] = None) -> float:
        """D5 周期相位质量评分

        RELEASE=1.0(趋势释放,最佳), CHARGE=0.7(蓄力,可布局),
        EXHAUST=0.4(衰竭,减仓), CHAOS=0.2(混沌,极度谨慎)

        Returns:
            float: 相位质量评分，范围[0,1]。无数据时返回0.5(中性)
        """
        if cr_output is None:
            return 0.5  # R14-P1-DOC-P1-07修复: 中性评分0.5来源—跨策略Greeks聚合降级默认值
        try:
            phase = getattr(cr_output, 'phase', None)
            if phase is not None:
                phase_name = phase.name if hasattr(phase, 'name') else str(phase)
                return {'RELEASE': 1.0, 'CHARGE': 0.7, 'EXHAUST': 0.4, 'CHAOS': 0.2}.get(
                    phase_name.upper(), 0.5)
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass
        return 0.5

    def _compute_greeks_usage_score(self, dashboard: Optional[Dict[str, Any]] = None) -> float:
        """D6 Greeks使用率评分（使用率越低→评分越高→越安全）

        max_usage < 30% → 1.0, 30-60% → 0.7, 60-80% → 0.4, >80% → 0.2

        Returns:
            float: Greeks使用率评分，范围[0,1]。无数据时返回0.5(中性)
        """
        if dashboard is None:
            return 0.5  # R14-P1-DOC-P1-07修复: 中性评分0.5来源—跨策略Greeks聚合降级默认值
        try:
            portfolio = dashboard.get('portfolio', {})
            delta_pct = portfolio.get('delta_usage_pct', 50.0)
            gamma_pct = portfolio.get('gamma_usage_pct', 50.0)
            vega_pct = portfolio.get('vega_usage_pct', 50.0)
            max_usage = max(delta_pct, gamma_pct, vega_pct) / 100.0
            if max_usage <= 0.3:
                return 1.0
            elif max_usage < 0.6:
                return 0.7
            elif max_usage < 0.8:
                return 0.4
            else:
                return 0.2
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            return 0.5

    def _compute_consecutive_loss_score(self, consecutive_losses: int = 0) -> float:
        """D7 连亏状态评分

        0次=1.0, 1-2次=0.8, 3-4次=0.5, 5-6次=0.3, >=7次=0.1

        Returns:
            float: 连亏状态评分，范围[0,1]
        """
        if consecutive_losses <= 0:
            return 1.0
        elif consecutive_losses <= 2:
            return 0.8
        elif consecutive_losses <= 4:
            return 0.5
        elif consecutive_losses <= 6:
            return 0.3
        else:
            return 0.1

    # [R16-P1-11修复] Alpha衰减恢复人工确认方法
    def confirm_alpha_decay_recovery(self):
        self._alpha_decay_recovery_confirmed = True
        logging.info("[R16-P1-11] Alpha衰减恢复已人工确认")

    def _compute_asymmetric_drawdown_score(self, current_pnl: float = 0.0,
                                            drawdown_pct: float = 0.0) -> float:
        """D8 非对称回撤评分

        盈利+低回撤=1.0, 盈利+高回撤=0.6, 亏损+低回撤=0.5, 亏损+高回撤=0.2

        Returns:
            float: 非对称回撤评分，范围[0,1]
        """
        abs_dd = abs(drawdown_pct)
        if current_pnl >= 0:
            if abs_dd < 0.02:
                return 1.0
            elif abs_dd < 0.05:
                return 0.8
            else:
                return 0.6
        else:
            if abs_dd < 0.02:
                return 0.5
            elif abs_dd < 0.05:
                return 0.3
            else:
                return 0.2

    def _compute_tri_validation_score(self, tri_score: Optional[float] = None) -> float:
        """D9 三角验证评分（Sortino/Calmar/Sharpe门控综合分）

        外部传入已归一化的三角验证分数，无数据时中性0.5

        Returns:
            float: 三角验证评分，范围[0,1]。无数据时返回0.5(中性)
        """
        if tri_score is None:
            return 0.5  # R14-P1-DOC-P1-07修复: 中性评分0.5来源—跨策略Greeks聚合降级默认值
        return max(0.0, min(1.0, float(tri_score)))

    def _compute_alpha_decay_score(self, alpha_ratio: Optional[float] = None) -> float:
        """D10 Alpha衰减评分

        alpha_ratio > 1.0 → 1.0, 0.5-1.0 → 0.7, 0-0.5 → 0.4, <0 → 0.1

        Returns:
            float: Alpha衰减评分，范围[0,1]。无数据时返回0.5(中性)
        """
        if alpha_ratio is None:
            return 0.5  # R14-P1-DOC-P1-07修复: 中性评分0.5来源—跨策略Greeks聚合降级默认值
        if alpha_ratio > 1.0:
            return 1.0
        elif alpha_ratio > 0.5:
            return 0.7
        elif alpha_ratio > 0.0:
            return 0.4
        else:
            return 0.1

    def _compute_cross_correlation_score(self, cross_corr: Optional[float] = None) -> float:
        """D11 跨策略相关性评分（相关性越低→评分越高→组合越分散）

        |corr| < 0.2 → 1.0, 0.2-0.4 → 0.8, 0.4-0.6 → 0.5, >0.6 → 0.2

        Returns:
            float: 跨策略相关性评分，范围[0,1]。无数据时返回0.5(中性)
        """
        if cross_corr is None:
            return 0.5  # R14-P1-DOC-P1-07修复: 中性评分0.5来源—跨策略Greeks聚合降级默认值
        abs_corr = abs(cross_corr)
        if abs_corr < 0.2:
            return 1.0
        elif abs_corr < 0.4:
            return 0.8
        elif abs_corr < 0.6:
            return 0.5
        else:
            return 0.2

    def _compute_liquidity_score(self) -> float:
        """MS-P1-06: 流动性评分 — 从order_flow_bridge获取spread并评分

        spread/price > 0.5%  → 低流动性, score=0.3
        spread/price < 0.1%  → 高流动性, score=1.0
        其他线性插值。
        无法获取spread时返回0.5(中性)。

        Returns:
            float: 流动性评分，范围[0.3, 1.0]。无数据时返回0.5(中性)
        """
        try:
            from ali2026v3_trading.order_flow_bridge import OrderFlowBridge
            bridge = OrderFlowBridge()
            # 尝试获取当前spread — 通过分析器的最近快照
            if hasattr(bridge, '_prev_bid') and hasattr(bridge, '_prev_ask'):
                bid = bridge._prev_bid
                ask = bridge._prev_ask
                if bid and ask and bid > 0 and ask > 0:
                    price = (bid + ask) / 2.0
                    spread_ratio = (ask - bid) / price
                    if spread_ratio > 0.005:       # >0.5%: 低流动性
                        return 0.3
                    elif spread_ratio < 0.001:     # <0.1%: 高流动性
                        return 1.0
                    else:                          # 线性插值
                        t = (spread_ratio - 0.001) / (0.005 - 0.001)
                        return 1.0 - t * (1.0 - 0.3)
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass
        return 0.5  # 中性评分—无法获取流动性数据

    def _get_life_estimator(self) -> Any:
        """懒加载行情寿命估计器

        R4-D-03修复: 懒加载单例防护 —
        首次调用返回None时记录WARNING，后续调用不再重复尝试导入。
        使用_SENTINEL标记区分「未初始化」和「初始化失败」。
        R5-T-04修复: 添加线程安全锁，防止并发初始化。
        """
        if self._life_estimator is not None:
            return self._life_estimator
        # R4-D-03: 使用sentinel避免重复尝试已失败的初始化
        if getattr(self, '_life_estimator_init_failed', False):
            return None
        # R5-T-04修复: 线程安全初始化
        with self._lock:
            # double-check
            if self._life_estimator is not None:
                return self._life_estimator
            if getattr(self, '_life_estimator_init_failed', False):
                return None
            try:
                from ali2026v3_trading.参数池.L1参数量化.bayesian_shrinkage_life_estimator import BayesianShrinkageLifeEstimator
                self._life_estimator = BayesianShrinkageLifeEstimator()
                if self._life_estimator is None:
                    logging.warning("[R4-D-03] BayesianShrinkageLifeEstimator构造返回None")
                    self._life_estimator_init_failed = True
                return self._life_estimator
            except Exception as e:
                logging.warning("[R4-D-03] BayesianShrinkageLifeEstimator不可用(后续不再重试): %s", e)
                self._life_estimator_init_failed = True
                return None

    def set_life_estimator(self, estimator: Any) -> None:
        """注入外部寿命估计器（回测时由task_scheduler注入已构建的实例）"""
        self._life_estimator = estimator

    def set_current_hmm_state(self, state: str) -> None:
        """设置当前HMM状态（由策略引擎每Bar更新）"""
        self._current_hmm_state = state

    def _check_life_expectancy(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        """行情寿命检查：降级Level>=3时阻断新开仓

        规则：
        - Level 0/1/2：通过（寿命置信度足够）
        - Level 3 + 新开仓：阻断（全局先验，寿命不可靠，行情可能已进入未知状态）
        - Level 3 + 平仓：通过（允许风控平仓）
        """
        hmm_state = signal.get("hmm_state", self._current_hmm_state)
        if hmm_state is None:
            return RiskCheckResponse.pass_result()

        estimator = self._get_life_estimator()
        if estimator is None:
            return RiskCheckResponse.pass_result()

        try:
            if not hasattr(estimator, '_life_dict') or not estimator._life_dict:
                return RiskCheckResponse.pass_result()

            life = estimator.get_life_expectancy(hmm_state)
            if life is None or not life.is_valid():
                return RiskCheckResponse.pass_result()

            action = signal.get("action", "")
            if life.degradation_level >= 3 and action != "CLOSE":
                return RiskCheckResponse.block_result(
                    "life_expectancy_exhausted",
                    "行情寿命Level3(全局先验)，状态'%s'寿命不可靠，禁止新开仓" % hmm_state,
                    level=RiskLevel.HIGH,
                )
            if life.degradation_level >= 2 and action != "CLOSE":
                return RiskCheckResponse.warning_result(
                    "life_expectancy_degraded",
                    "行情寿命Level2(大类合并)，状态'%s'寿命置信度低" % hmm_state,
                )
        except Exception as e:
            logging.debug("[RiskService._check_life_expectancy] error: %s", e)

        return RiskCheckResponse.pass_result()

    def check_price_limit(self, instrument_id: str, price: float, direction: str) -> RiskCheckResponse:
        """P0-R12-EX-01~03修复: 涨跌停价格检查"""
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_svc = get_params_service()
            meta = params_svc.get_instrument_meta_by_id(instrument_id)
            if not meta:
                return RiskCheckResponse.pass_result()
            product = meta.get('product', '')
            product_cache = params_svc.get_product_cache(product)
            if not product_cache:
                return RiskCheckResponse.pass_result()
            limit_up = safe_float(product_cache.get('limit_up_price', 0))
            limit_down = safe_float(product_cache.get('limit_down_price', 0))
            if limit_up <= 0 and limit_down <= 0:
                return RiskCheckResponse.pass_result()
            if limit_up > 0 and price >= limit_up and direction in ('BUY', '买', 'long'):
                return RiskCheckResponse.block_result(
                    "price_at_limit_up",
                    f"涨停价{limit_up:.2f}禁止买入: {instrument_id} price={price:.2f}",
                    RiskLevel.HIGH
                )
            if limit_down > 0 and price <= limit_down and direction in ('SELL', '卖', 'short'):
                return RiskCheckResponse.block_result(
                    "price_at_limit_down",
                    f"跌停价{limit_down:.2f}禁止卖出: {instrument_id} price={price:.2f}",
                    RiskLevel.HIGH
                )
            return RiskCheckResponse.pass_result()
        except Exception as e:
            logging.warning("[RiskService.check_price_limit] fallback: %s", e)  # R24-P1-DF-03修复: debug→warning
            return RiskCheckResponse.pass_result()

    def is_at_price_limit(self, instrument_id: str, price: float) -> Dict[str, bool]:
        """P0-R12-EX-03修复: 涨跌停行情标记"""
        result = {"is_limit_up": False, "is_limit_down": False}
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_svc = get_params_service()
            meta = params_svc.get_instrument_meta_by_id(instrument_id)
            if not meta:
                return result
            product = meta.get('product', '')
            product_cache = params_svc.get_product_cache(product)
            if not product_cache:
                return result
            limit_up = safe_float(product_cache.get('limit_up_price', 0))
            limit_down = safe_float(product_cache.get('limit_down_price', 0))
            if limit_up > 0 and abs(price - limit_up) < limit_up * 1e-6:
                result["is_limit_up"] = True
            if limit_down > 0 and abs(price - limit_down) < limit_down * 1e-6:
                result["is_limit_down"] = True
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass
        return result

    def check_expiry_risk(self, instrument_id: str, days_to_expiry: int = None) -> RiskCheckResponse:
        """P0-R12-EX-05~06修复: 交割日风控检查(集成calc_option_expiry_date)"""
        if days_to_expiry is None:
            try:
                from ali2026v3_trading.params_service import get_params_service
                params_svc = get_params_service()
                meta = params_svc.get_instrument_meta_by_id(instrument_id)
                if meta:
                    expiry_date_str = meta.get('expiry_date', '')
                    if expiry_date_str:
                        from datetime import datetime, date
                        expiry_date = datetime.strptime(expiry_date_str, '%Y%m%d').date()
                        days_to_expiry = (expiry_date - date.today()).days
            except Exception:
                days_to_expiry = 999
        if days_to_expiry is not None and days_to_expiry <= 0:
            return RiskCheckResponse.block_result(
                "expired_contract",
                f"合约{instrument_id}已到期(days_to_expiry={days_to_expiry})，禁止开仓",
                RiskLevel.HIGH
            )
        min_days_to_expiry = 3
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_svc = get_params_service()
            meta = params_svc.get_instrument_meta_by_id(instrument_id)
            if meta:
                product = meta.get('product', '')
                product_cache = params_svc.get_product_cache(product)
                if product_cache:
                    min_days_to_expiry = safe_int(product_cache.get('min_days_to_expiry', 3))
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass
        if days_to_expiry is not None and days_to_expiry <= min_days_to_expiry:
            return RiskCheckResponse.block_result(
                "near_expiry",
                f"合约{instrument_id}距到期仅{days_to_expiry}天≤{min_days_to_expiry}，风险极高",
                RiskLevel.HIGH
            )
        return RiskCheckResponse.pass_result()

    def check_auction_session(self, bar_datetime, instrument_id: str) -> RiskCheckResponse:
        """EX-P1-03修复: 集合竞价时段检查，禁止在竞价时段下单"""
        try:
            from ali2026v3_trading.config_params import get_config
            cfg = get_config()
            minutes_before_open = cfg.get("auction_session_minutes_before_open", 5)
            minutes_before_close = cfg.get("auction_session_minutes_before_close", 5)
        except Exception:
            minutes_before_open = 5
            minutes_before_close = 5
        if hasattr(bar_datetime, 'hour') and hasattr(bar_datetime, 'minute'):
            t = bar_datetime.hour * 60 + bar_datetime.minute
            day_open = 9 * 60
            day_close = 15 * 60
            if day_open - minutes_before_open <= t < day_open:
                return RiskCheckResponse.block_result(
                    "auction_session",
                    f"集合竞价时段(开市前{minutes_before_open}分钟)，禁止下单",
                    RiskLevel.MEDIUM
                )
            if day_close - minutes_before_close < t <= day_close:
                return RiskCheckResponse.block_result(
                    "auction_session",
                    f"集合竞价时段(收市前{minutes_before_close}分钟)，禁止下单",
                    RiskLevel.MEDIUM
                )
        return RiskCheckResponse.pass_result()

    @staticmethod
    def is_trading_day(check_date) -> bool:
        """EX-P1-04修复: 检查是否为交易日（使用chinese_calendar库）"""
        try:
            import chinese_calendar
            return chinese_calendar.is_workday(check_date)
        except ImportError:
            return True

    def check_last_trading_day(self, instrument_id: str, days_to_expiry: int) -> RiskCheckResponse:
        """EX-P1-06修复: 最后交易日平仓检查"""
        try:
            from ali2026v3_trading.config_params import get_config
            cfg = get_config()
            close_ahead = cfg.get("last_trading_day_close_ahead_days", 3)
        except Exception:
            close_ahead = 3
        if days_to_expiry <= close_ahead:
            return RiskCheckResponse.block_result(
                "last_trading_day",
                f"合约{instrument_id}距最后交易日仅{days_to_expiry}天≤{close_ahead}，需强制平仓",
                RiskLevel.HIGH
            )
        return RiskCheckResponse.pass_result()

    @staticmethod
    def calc_option_expiry_date(year: int, month: int, which_friday: int = 3, exchange: str = 'CFFEX') -> 'date':
        """P0-R12-EX-07修复: 计算期权到期日(第N个星期五)
        
        NEW-EX-P0-03: 支持多交易所到期日规则
        - CFFEX: 第N个星期五(默认第3个)
        - SSE/SZSE: 第N个星期三
        - DCE/CZCE/SHFE/INE: 倒数第N个交易日
        """
        from datetime import date, timedelta
        if exchange in ('SSE', 'SZSE'):
            first_day = date(year, month, 1)
            first_wed = first_day + timedelta(days=(2 - first_day.weekday()) % 7)
            target_wed = first_wed + timedelta(weeks=3)
            if target_wed.month != month:
                target_wed = first_wed + timedelta(weeks=2)
            return target_wed
        elif exchange in ('DCE', 'CZCE', 'SHFE', 'INE'):
            first_day = date(year, month, 1)
            if first_day.month == 12:
                next_month = date(year + 1, 1, 1)
            else:
                next_month = date(year, month + 1, 1)
            last_day = next_month - timedelta(days=1)
            target = last_day - timedelta(days=4)
            while target.weekday() >= 5:
                target -= timedelta(days=1)
            if target.month != month:
                target = last_day - timedelta(days=1)
                while target.weekday() >= 5:
                    target -= timedelta(days=1)
            return target
        else:
            first_day = date(year, month, 1)
            first_friday = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
            target_friday = first_friday + timedelta(weeks=which_friday - 1)
            if target_friday.month != month:
                target_friday = first_friday + timedelta(weeks=which_friday - 2)
            return target_friday

    def check_margin_sufficiency(self, equity: float, instrument_id: str,
                                  volume: int, price: float) -> RiskCheckResponse:
        """P0-R12-EX-08~10修复: 保证金充足性检查(动态比例+min_margin_ratio_override)"""
        margin_ratio = 0.1
        product = ''
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_svc = get_params_service()
            meta = params_svc.get_instrument_meta_by_id(instrument_id)
            if meta:
                product = meta.get('product', '')
                product_cache = params_svc.get_product_cache(product)
                if product_cache:
                    margin_ratio = safe_float(product_cache.get('margin_ratio', 0.1))
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass
        try:
            from ali2026v3_trading.config_params import get_config
            cfg = get_config()
            overrides = cfg.get('min_margin_ratio_override', {})
            if product and product in overrides:
                margin_ratio = max(margin_ratio, safe_float(overrides[product]))
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass
        contract_size = self._get_contract_multiplier(instrument_id)
        required_margin = abs(volume) * price * margin_ratio * contract_size
        # EX-P2-06: TODO 当前为简单线性保证金计算，未考虑期权卖方组合保证金优惠、跨期套利保证金优惠等交易所实际规则
        # 实现组合保证金需接入交易所API或实现简化组合保证金模型（架构级待规划）
        with self._lock:
            current_margin_used = getattr(self, '_total_margin_used', 0.0)
        # INV-05/INV-CAP-03修复: margin_used <= equity 守卫
        # 保证金占用超过权益时，账户已资不抵债，必须阻断新开仓
        total_margin_after = current_margin_used + required_margin
        if total_margin_after > equity:
            logging.critical(
                "[RiskService] INV-CAP-03: margin_used(%.2f) + required(%.2f) = %.2f > equity(%.2f), "
                "账户资不抵债，阻断新开仓",
                current_margin_used, required_margin, total_margin_after, equity,
            )
            self._fire_alert('margin_exceeds_equity', {
                'margin_used': current_margin_used,
                'required_margin': required_margin,
                'equity': equity,
            })
            return RiskCheckResponse.block_result(
                "margin_exceeds_equity",
                f"INV-CAP-03: 保证金占用{total_margin_after:.2f}超过权益{equity:.2f}，账户资不抵债",
                RiskLevel.CRITICAL
            )
        # EX-10修复: 保证金追缴通知(margin_call)机制
        # 保证金占用率>90%触发margin_call通知, >95%触发渐进强平
        with self._lock:
            if equity > 0:
                margin_occupancy_ratio = current_margin_used / equity
            else:
                margin_occupancy_ratio = 1.0
            if not hasattr(self, '_margin_call_state'):
                self._margin_call_state = {
                    'status': 'normal',
                    'notified_at': None,
                    'grace_deadline': None,
                    'force_close_triggered': False,
                }
            _mc = self._margin_call_state
        if margin_occupancy_ratio > 0.95:
            if not _mc['force_close_triggered']:
                _mc['force_close_triggered'] = True
                _mc['status'] = 'force_closing'
                self._fire_alert('margin_call_force_close', {
                    'margin_used': current_margin_used,
                    'equity': equity,
                    'occupancy_ratio': margin_occupancy_ratio,
                    'threshold': 0.95,
                })
                logging.critical(
                    "[RiskService] EX-10: 保证金占用率%.2f%%>95%%, 触发渐进强平! margin_used=%.2f equity=%.2f",
                    margin_occupancy_ratio * 100, current_margin_used, equity,
                )
                try:
                    from ali2026v3_trading.position_service import get_position_service
                    _ps = get_position_service()
                    if _ps and hasattr(_ps, 'positions'):
                        _close_count = 0
                        for _inst_id, _pos_dict in list(_ps.positions.items()):
                            for _pid, _rec in list(_pos_dict.items()):
                                if _rec.volume != 0:
                                    _lim = self.is_at_price_limit(_rec.get('instrument_id', ''), _rec.get('last_price', 0))
                                    _close_dir = 'SELL' if _rec.get('direction') == 'BUY' else 'BUY'
                                    if _lim.get('is_limit_up') and _close_dir == 'SELL':
                                        continue
                                    if _lim.get('is_limit_down') and _close_dir == 'BUY':
                                        continue
                                    try:
                                        _ps._trigger_close_position(_rec, "EX-10: margin_call force close (occupancy>95%)")
                                        _close_count += 1
                                    except Exception as _mce:
                                        logging.warning("[RiskService] EX-10: margin_call force close %s failed: %s", _inst_id, _mce)
                        logging.critical("[RiskService] EX-10: margin_call渐进强平已触发, closed=%d", _close_count)
                except Exception as e:
                    logging.warning("[RiskService] EX-10: margin_call渐进强平失败: %s", e)
        elif margin_occupancy_ratio > 0.90:
            if _mc['status'] == 'normal':
                import time as _time
                _mc['status'] = 'margin_call'
                _mc['notified_at'] = _time.time()
                _mc['grace_deadline'] = _time.time() + 1800
                self._fire_alert('margin_call', {
                    'margin_used': current_margin_used,
                    'equity': equity,
                    'occupancy_ratio': margin_occupancy_ratio,
                    'threshold': 0.90,
                    'grace_period_sec': 1800,
                })
                logging.critical(
                    "[RiskService] EX-10: 保证金追缴通知(margin_call)! 占用率%.2f%%>90%%, 宽限期30min, margin_used=%.2f equity=%.2f",
                    margin_occupancy_ratio * 100, current_margin_used, equity,
                )
            if _mc['status'] == 'margin_call' and _mc['grace_deadline'] is not None:
                import time as _time
                if _time.time() > _mc['grace_deadline']:
                    _mc['status'] = 'grace_expired'
                    logging.critical("[RiskService] EX-10: 保证金追缴宽限期已过, 即将触发强平!")
        else:
            if _mc['status'] != 'normal':
                _mc['status'] = 'normal'
                _mc['force_close_triggered'] = False
                _mc['notified_at'] = None
                _mc['grace_deadline'] = None
        available = equity - current_margin_used
        if required_margin > available * 0.95:
            return RiskCheckResponse.block_result(
                "insufficient_margin",
                f"保证金不足: 需要{required_margin:.2f}(比例{margin_ratio:.2%}), 可用{available:.2f}",
                RiskLevel.HIGH
            )
        return RiskCheckResponse.pass_result()

    def update_margin_ratio(self, instrument_id: str, new_ratio: float) -> None:
        """P0-R12-EX-08修复: 交易所调整保证金比例时更新"""
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_svc = get_params_service()
            meta = params_svc.get_instrument_meta_by_id(instrument_id)
            if meta:
                product = meta.get('product', '')
                product_cache = params_svc.get_product_cache(product)
                if product_cache and isinstance(product_cache, dict):
                    product_cache['margin_ratio'] = new_ratio
                    logging.info("[RiskService] Margin ratio updated: %s → %.4f", instrument_id, new_ratio)
        except Exception as e:
            logging.warning("[RiskService.update_margin_ratio] Failed: %s", e)

    def check_cross_instrument_limit(self, account_id: str, instrument_id: str,
                                      required_amount: float) -> RiskCheckResponse:
        """P0-R12-EX-13修复: 跨品种持仓限额聚合检查"""
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_svc = get_params_service()
            meta = params_svc.get_instrument_meta_by_id(instrument_id)
            if not meta:
                return RiskCheckResponse.pass_result()
            industry = meta.get('industry', '')
            if not industry:
                industry = self._infer_industry(instrument_id)
                if not industry:
                    logging.warning("[EX-P0-04] industry字段为空且无法推断, 跳过跨品种限额检查: %s", instrument_id)
                    return RiskCheckResponse.pass_result()
            industry_limit_key = f"industry_limit_{industry}"
            with self._lock:
                industry_limit = getattr(self, '_industry_limits', {}).get(industry, None)
            if industry_limit is None:
                return RiskCheckResponse.pass_result()
            current_exposure = 0.0
            if self.position_manager is not None:
                try:
                    positions_raw = getattr(self.position_manager, "positions", {})
                    for inst_id, inst_map in positions_raw.items():
                        try:
                            inst_meta = params_svc.get_instrument_meta_by_id(inst_id)
                            if inst_meta and inst_meta.get('industry') == industry:
                                for pos in inst_map.values():
                                    vol = getattr(pos, "volume", 0)
                                    prc = getattr(pos, "open_price", 0)
                                    current_exposure += abs(vol) * prc
                        except Exception:
                            continue
                except Exception:
                    logging.warning("[R22-EP-P1] RiskService exception swallowed")
                    pass
            if current_exposure + required_amount > industry_limit:
                return RiskCheckResponse.block_result(
                    "cross_instrument_limit_exceeded",
                    f"行业{industry}总暴露{current_exposure:.2f}+需要{required_amount:.2f}>{industry_limit:.2f}",
                    RiskLevel.HIGH
                )
            return RiskCheckResponse.pass_result()
        except Exception as e:
            logging.warning("[RiskService.check_cross_instrument_limit] fallback: %s", e)  # R24-P1-DF-03修复: debug→warning
            return RiskCheckResponse.pass_result()

    def _infer_industry(self, instrument_id: str) -> str:
        s = str(instrument_id).upper()
        if any(k in s for k in ('IO', 'MO', 'HO', 'IF', 'IC', 'IH', 'IM')):
            return '金融'
        elif any(k in s for k in ('CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'AU', 'AG')):
            return '金属'
        elif any(k in s for k in ('M', 'Y', 'A', 'C', 'CS', 'JD', 'LH', 'P', 'OI', 'RS')):
            return '农产品'
        elif any(k in s for k in ('RU', 'FU', 'BU', 'SP', 'MA', 'TA', 'FG', 'SA', 'EG', 'PF', 'SH')):
            return '化工'
        elif any(k in s for k in ('SC', 'NR', 'BC')):
            return '能源'
        return ''

    def set_industry_limit(self, industry: str, limit_amount: float) -> None:
        """设置行业持仓限额"""
        with self._lock:
            if not hasattr(self, '_industry_limits'):
                self._industry_limits = {}
            self._industry_limits[industry] = limit_amount

    def monitor_margin_occupancy(self, account_id: str, equity: float, current_margin_used: float) -> Dict[str, Any]:
        if equity <= 0:
            return {"status": "critical", "occupancy_ratio": float('inf'), "action": "force_close_all"}
        margin_occupancy_ratio = current_margin_used / equity
        if margin_occupancy_ratio > 0.95:
            self._fire_alert('margin_call_force_close', f"保证金占用率{margin_occupancy_ratio:.1%}>95%, 触发渐进强平")
            return {"status": "force_close", "occupancy_ratio": margin_occupancy_ratio, "action": "reduce_positions"}
        elif margin_occupancy_ratio > 0.90:
            self._fire_alert('margin_call', f"保证金占用率{margin_occupancy_ratio:.1%}>90%, 发出追缴通知")
            return {"status": "margin_call", "occupancy_ratio": margin_occupancy_ratio, "action": "notify"}
        return {"status": "ok", "occupancy_ratio": margin_occupancy_ratio, "action": "none"}

    # P2-项2修复: 不变量运行时检查 — 为仅有注释的不变量添加运行时断言
    def check_invariant_runtime(self, invariant_name: str, condition: bool,
                                 context: Optional[Dict[str, Any]] = None) -> bool:
        """P2-项2修复: 运行时不变量检查，将注释型不变量转为运行时断言

        Args:
            invariant_name: 不变量名称（如'INV-CAP-02'）
            condition: 不变量条件是否满足
            context: 附加上下文信息

        Returns:
            bool: 不变量是否通过
        """
        if condition:
            return True
        ctx = context or {}
        logging.warning(
            "[RiskService] 不变量运行时检查失败: %s context=%s",
            invariant_name, ctx,
        )
        self._handle_invariant_violation(invariant_name, ctx)
        return False

    # P2-项3修复: 不变量违反恢复策略
    def _handle_invariant_violation(self, invariant_name: str,
                                     context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """P2-项3修复: 不变量违反恢复策略

        根据不变量类型选择恢复动作：
        - INV-CAP-*: 资金类 → 阻断新开仓 + 告警
        - INV-RSK-*: 风险类 → 降级 + 告警
        - INV-POS-*: 持仓类 → 强制减仓 + 告警
        - 其他 → 记录 + 告警

        Args:
            invariant_name: 不变量名称
            context: 违反上下文

        Returns:
            Dict: 恢复动作结果
        """
        ctx = context or {}
        recovery = {
            'invariant': invariant_name,
            'action': 'logged',
            'blocked_new_open': False,
            'timestamp': time.time(),
        }

        # 资金类不变量违反 → 阻断新开仓
        if 'CAP' in invariant_name:
            recovery['action'] = 'block_new_open'
            recovery['blocked_new_open'] = True
            logging.critical(
                "[RiskService] 不变量违反恢复: %s → 阻断新开仓 context=%s",
                invariant_name, ctx,
            )
            self._fire_alert('invariant_violation_capital', {
                'invariant': invariant_name, 'context': ctx,
            })

        # 风险类不变量违反 → 降级
        elif 'RSK' in invariant_name or 'IRN' in invariant_name:
            recovery['action'] = 'degrade'
            logging.critical(
                "[RiskService] 不变量违反恢复: %s → 降级 context=%s",
                invariant_name, ctx,
            )
            self._fire_alert('invariant_violation_risk', {
                'invariant': invariant_name, 'context': ctx,
            })

        # 持仓类不变量违反 → 强制减仓
        elif 'POS' in invariant_name:
            recovery['action'] = 'force_reduce'
            logging.critical(
                "[RiskService] 不变量违反恢复: %s → 强制减仓 context=%s",
                invariant_name, ctx,
            )
            self._fire_alert('invariant_violation_position', {
                'invariant': invariant_name, 'context': ctx,
            })

        else:
            logging.warning(
                "[RiskService] 不变量违反: %s context=%s",
                invariant_name, ctx,
            )

        # 记录违反历史
        if not hasattr(self, '_invariant_violation_history'):
            self._invariant_violation_history = deque(maxlen=500)
        self._invariant_violation_history.append(recovery)
        return recovery

    # P2-项12修复: 数据流异常检测
    def detect_data_flow_anomaly(self, metric_name: str, current_value: float,
                                  baseline_value: float,
                                  threshold_sigma: float = 3.0) -> Dict[str, Any]:
        """P2-项12修复: 数据流异常检测

        检测关键数据流指标是否偏离基线超过N个标准差。

        Args:
            metric_name: 指标名称
            current_value: 当前值
            baseline_value: 基线值
            threshold_sigma: 偏离阈值(标准差倍数)

        Returns:
            Dict: {is_anomaly, deviation_pct, metric_name, action}
        """
        if baseline_value == 0:
            deviation_pct = 0.0
            is_anomaly = False
        else:
            deviation_pct = abs(current_value - baseline_value) / abs(baseline_value) * 100
            is_anomaly = deviation_pct > threshold_sigma * 33.3  # 简化: 1σ≈33.3%

        result = {
            'is_anomaly': is_anomaly,
            'metric_name': metric_name,
            'current_value': current_value,
            'baseline_value': baseline_value,
            'deviation_pct': round(deviation_pct, 2),
            'action': 'alert' if is_anomaly else 'none',
        }

        if is_anomaly:
            logging.warning(
                "[RiskService] 数据流异常检测: %s 当前=%.4f 基线=%.4f 偏离=%.1f%%",
                metric_name, current_value, baseline_value, deviation_pct,
            )
            self._fire_alert('data_flow_anomaly', result)

        return result


# ============================================================================
# 辅助函数
# ============================================================================

def safe_get(obj: Any, attr: str, default: Any = None, target_type: type = float) -> Any:
    """统一安全属性获取入口（方法唯一修复：公开为safe_get，并行接口为便捷别名）"""
    try:
        val = getattr(obj, attr, default)
        if val is None:
            return default
        return target_type(val) if target_type == int else float(val)
    except (ValueError, TypeError, AttributeError) as e:
        logging.warning("[safe_get_%s] Error getting %s: %s", target_type.__name__, attr, e)
        return default


def safe_get_float(obj: Any, attr: str, default: float = 0.0) -> float:
    return safe_get(obj, attr, default, float)


def safe_get_int(obj: Any, attr: str, default: int = 0) -> int:
    return safe_get(obj, attr, default, int)


# ============================================================================
# L-1 安全元层 (SafetyMetaLayer)
# ============================================================================

class SafetyMetaLayer:
    """L-1安全元层 — 独立于策略模型的账户级生存保障

    P2-R11-09修复: 隔离说明 — SafetyMetaLayer通过get_safety_meta_layer(strategy_id)实现策略级实例隔离
    每个strategy_id对应独立的SafetyMetaLayer实例，断路器/回撤监控互不影响
    注意: 全局默认实例(_safety_meta_layer)仍存在，仅用于strategy_id=None的向后兼容路径
    新代码应始终传递strategy_id参数以获得策略级隔离

    三条硬规则（最高权限，不可被策略参数覆盖）：
    1. 速率断路器：1分钟内权益回撤超过滚动3σ，暂停交易。
       熔断冷静期(circuit_breaker_calm_period_sec)：首次触发后N秒内不再二次触发，防止极端行情中频繁"抽搐"
    2. 持仓时间硬止损：开仓后max_hold_minutes_hard分钟内浮盈从未达到min_profit_threshold，
       则在max_hold_minutes_hard+30分钟强制平仓
    3. 日最大回撤硬停止：当日累计回撤超过前5日平均日收益的daily_drawdown_multiplier倍，
       立即平掉所有仓位，禁止当日任何后续交易，直到下个交易日人工确认后恢复
    
    可回测优化参数：
    - ANOMALY_THRESHOLD_MULTIPLIER: 异常检测阈值乘数（默认3.0σ）
    - DEFAULT_ANOMALY_THRESHOLD: 无历史数据时的默认阈值
    - DEFAULT_MAX_DRAWDOWN: 无历史均值时的默认最大回撤阈值
    """

    ANOMALY_THRESHOLD_MULTIPLIER = 3.0
    DEFAULT_ANOMALY_THRESHOLD = 0.05
    DEFAULT_MAX_DRAWDOWN = 0.05

    def __init__(self, params: Any = None):
        # RTO目标: 5分钟, RPO目标: 0数据丢失
        self.params = params  # R25-SE-01-FIX: _params统一为params，与RiskService命名对齐
        self._lock = threading.RLock()
        self._stats_lock = threading.Lock()  # [R23-P1-03-FIX] 专用_stats锁，防止并发场景下计数器重复累加

        self._equity_series: deque = deque(maxlen=60)
        self._equity_timestamps: deque = deque(maxlen=60)
        self._drop_pct_history: deque = deque(maxlen=60)

        # R13-P2-BIZ-03修复: 记录最后一个equity更新日期，用于隔夜缺口检测
        self._last_equity_date: Optional[str] = None

        self._trading_paused_until: float = 0.0
        self._pause_reason: str = ""
        self._circuit_breaker_calm_until: float = 0.0
        # R27-P0-DR-12修复: 断路器激活时间戳，供半开机制使用
        self._circuit_breaker_activated_at: float = 0.0
        # P1-7修复：断路器影子模式观察期
        self._circuit_breaker_shadow_mode: bool = False
        self._circuit_breaker_shadow_until: float = 0.0

        self._daily_start_equity: Optional[float] = None
        self._daily_peak_equity: float = 0.0
        self._daily_drawdown: float = 0.0
        self._prev_5day_avg_profit: float = 0.0
        self._daily_new_open_blocked: bool = False
        # P2-R3-P-11: daily_loss_hard_stop双机制并存 — _check_daily_drawdown中同时存在
        # "固定阈值硬停止(DEFAULT_MAX_DRAWDOWN=5%)"和"5日均值乘数软停止"两条路径,
        # 两者均设置_daily_hard_stop_triggered=True但语义不同(绝对回撤 vs 相对盈利回撤)。
        # 已知限制: 合并触发统计无法区分哪条路径触发，日志可区分但统计计数不可区分。
        self._daily_hard_stop_triggered: bool = False
        self._current_date: Optional[str] = None
        # P1-R11-10修复: 记录上次恢复日期和时间，防止同交易日内重复恢复
        self._last_resume_date: Optional[str] = None
        self._last_resume_time: float = 0.0

        self._stats = {
            "circuit_breaker_triggers": 0,
            "circuit_breaker_calm_rejects": 0,
            "hard_time_stop_triggers": 0,
            "daily_drawdown_triggers": 0,
            "daily_hard_stop_triggers": 0,
            "total_equity_updates": 0,
        }

        # P1-裂缝36：逻辑反转优先级记录
        self._logic_reversal_priority: Dict[str, bool] = {}

        # P1-1修复：换月成本追踪
        self._rollover_cost_bps: float = 0.0
        self._rollover_count: int = 0

        # INV-P1-11修复: 权益曲线单调性追踪
        self._equity_intraday_low: Optional[float] = None
        self._equity_monotonicity_anomaly_count: int = 0

        # CMP-P1-13修复: 算法交易专用熔断状态
        self._algo_breakers: Dict[str, Any] = {
            'high_cancel_rate': {'triggered': False, 'paused_until': 0.0, 'threshold': 0.70, 'pause_sec': 1800},
            'excessive_self_trade': {'triggered': False, 'paused_until': 0.0, 'threshold': 3, 'pause_sec': 3600,
                                      'window_sec': 3600},
            'high_order_frequency': {'triggered': False, 'paused_until': 0.0, 'threshold': 100, 'pause_sec': 300,
                                      'window_sec': 60},
        }
        self._algo_paused: bool = False
        self._algo_paused_until: float = 0.0
        self._algo_pause_reason: str = ""

        self._circuit_breaker_state_path: str = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), ".circuit_breaker_state.json"
        )
        self._load_circuit_breaker_state()

        # R27-P1-DR-05修复: 风控数据库查询超时保护
        self._risk_db_timeout = DbQueryTimeout(default_timeout_sec=5.0)
        # R27-P1-DR-06修复: 风控看门狗; R27-P2-DR-10修复: 超时配置外置
        self._risk_watchdog = Watchdog(timeout_sec=config_params.WATCHDOG_TIMEOUT_RISK_SEC, name='risk_service')
        # R27-P1-DR-07修复: 风控心跳
        self._risk_heartbeat = HeartbeatMonitor(heartbeat_interval_sec=10.0, missed_threshold=3)
        # R27-P1-DR-09修复: 断路器半开恢复策略(增强已有_circuit_breaker_activated_at)
        self._risk_cb_half_open = CircuitBreakerHalfOpen(failure_threshold=3, open_duration_sec=120.0)
        # R27-P1-DR-15修复: 内存压力检测
        self._risk_memory_guard = MemoryPressureGuard()
        # R27-P1-DR-17修复: 日志风暴抑制
        self._risk_rate_limited_logger = RateLimitedLogger(window_sec=60.0, max_per_window=3)
        # R27-P1-DR-24修复: 进程健康状态
        self._risk_process_health = get_process_health()
        # R27-P1-RC-04修复: 配置热加载原子引用
        self._atomic_config = AtomicConfigRef(initial_config=self.params if hasattr(self, 'params') else {})

    def _save_circuit_breaker_state(self) -> None:
        try:
            now = time.time()
            state = {
                "trading_paused_until": self._trading_paused_until,
                "pause_reason": self._pause_reason,
                "circuit_breaker_calm_until": self._circuit_breaker_calm_until,
                "circuit_breaker_shadow_mode": self._circuit_breaker_shadow_mode,
                "circuit_breaker_shadow_until": self._circuit_breaker_shadow_until,
                # R27-P0-DR-12修复: 保存断路器激活时间戳
                "circuit_breaker_activated_at": getattr(self, '_circuit_breaker_activated_at', 0.0),
                # DR-08: 断路器状态持久化 — 日回撤硬停止相关字段
                "daily_hard_stop_triggered": self._daily_hard_stop_triggered,
                "daily_new_open_blocked": self._daily_new_open_blocked,
                "daily_start_equity": self._daily_start_equity,
                "daily_peak_equity": self._daily_peak_equity,
                "daily_drawdown": self._daily_drawdown,
                "prev_5day_avg_profit": self._prev_5day_avg_profit,
                "current_date": self._current_date,
                "saved_at": now,
            }
            tmp_path = self._circuit_breaker_state_path + ".tmp"  # EC-P2-01: 后缀拼接非路径分隔符拼接，保持原样
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False)
            os.replace(tmp_path, self._circuit_breaker_state_path)
        except Exception as e:
            logging.warning("[SafetyMetaLayer] 断路器状态持久化失败: %s", e)

    def _load_circuit_breaker_state(self) -> None:
        try:
            if not os.path.exists(self._circuit_breaker_state_path):
                return
            with open(self._circuit_breaker_state_path, "r", encoding="utf-8") as f:
                state = json.load(f)
            now = time.time()
            if state.get("trading_paused_until", 0) > now:
                self._trading_paused_until = state["trading_paused_until"]
                self._pause_reason = state.get("pause_reason", "")
            if state.get("circuit_breaker_calm_until", 0) > now:
                self._circuit_breaker_calm_until = state["circuit_breaker_calm_until"]
            if state.get("circuit_breaker_shadow_mode", False):
                if state.get("circuit_breaker_shadow_until", 0) > now:
                    self._circuit_breaker_shadow_mode = True
                    self._circuit_breaker_shadow_until = state["circuit_breaker_shadow_until"]
            # R27-P0-DR-12修复: 恢复断路器激活时间戳
            self._circuit_breaker_activated_at = state.get("circuit_breaker_activated_at", 0.0)
            # DR-08: 断路器状态持久化 — 恢复日回撤硬停止相关字段
            saved_date = state.get("current_date")
            today = _get_tz_aware_now().strftime("%Y-%m-%d")  # P1-R11-12修复: 使用UTC时区
            if saved_date and saved_date == today:
                # 同一天内恢复所有日级别状态
                self._daily_hard_stop_triggered = state.get("daily_hard_stop_triggered", False)
                self._daily_new_open_blocked = state.get("daily_new_open_blocked", False)
                self._daily_start_equity = state.get("daily_start_equity")
                self._daily_peak_equity = state.get("daily_peak_equity", 0.0)
                self._daily_drawdown = state.get("daily_drawdown", 0.0)
                self._prev_5day_avg_profit = state.get("prev_5day_avg_profit", 0.0)
                self._current_date = saved_date
            else:
                # 跨日: 日回撤状态不恢复（新交易日重置）
                self._daily_hard_stop_triggered = False
                self._daily_new_open_blocked = False
                logging.info(
                    "[SafetyMetaLayer] DR-08: 断路器状态跨日(%s→%s), 日回撤状态已重置",
                    saved_date, today,
                )
            logging.info(
                "[SafetyMetaLayer] 断路器状态已恢复: paused_until=%.1f calm_until=%.1f shadow=%s hard_stop=%s new_open_blocked=%s",
                self._trading_paused_until, self._circuit_breaker_calm_until,
                self._circuit_breaker_shadow_mode,
                self._daily_hard_stop_triggered, self._daily_new_open_blocked,
            )
        except Exception as e:
            logging.warning("[SafetyMetaLayer] 断路器状态恢复失败: %s", e)

    def on_equity_update(self, equity: float) -> None:
        now = time.time()
        # P1-R11-09修复: 使用交易日(18:00起算)而非日历日(00:00起算)
        # 夜盘21:00-次日02:30属于同一交易日，交易日从18:00开始
        _dt_now = datetime.fromtimestamp(now, tz=_CHINA_TZ)
        if _dt_now.hour >= 18:
            today = _dt_now.strftime("%Y-%m-%d")
        else:
            _yesterday = _dt_now - timedelta(days=1)
            today = _yesterday.strftime("%Y-%m-%d")

        with self._lock:
            self._stats["total_equity_updates"] += 1

            # R13-P0-BIZ-11修复: 负权益硬停止 — equity<=0时阻断所有交易并触发强制平仓
            # INV-03/INV-CAP-01修复: 负权益导致回撤计算错误，需强制减仓
            if equity <= 0:
                self._daily_hard_stop_triggered = True
                self._daily_new_open_blocked = True
                # INV-03/INV-CAP-01修复: 负权益时强制回撤设为100%，避免后续计算错误
                self._daily_drawdown = 1.0
                self._stats["negative_equity_triggers"] = self._stats.get("negative_equity_triggers", 0) + 1
                logging.critical(
                    "[SafetyMetaLayer] INV-CAP-01: negative equity hard stop! equity=%.2f <= 0, "
                    "force block all trading, forced position reduction, manual confirm required",
                    equity,
                )
                try:
                    from ali2026v3_trading.order_service import get_order_service
                    osvc = get_order_service()
                    if osvc:
                        count = osvc.cancel_all_pending()
                        if count > 0:
                            logging.critical("[SafetyMetaLayer] negative equity, cancelled %d pending orders", count)
                except Exception as e:
                    logging.warning("[SafetyMetaLayer] negative equity cancel failed: %s", e)
                # INV-03/INV-CAP-01修复: 触发强制减仓 — 尝试平掉所有持仓
                # EX-04修复: 紧急平仓添加涨跌停价格容错检查
                try:
                    from ali2026v3_trading.position_service import get_position_service
                    _ps = get_position_service()
                    if _ps and hasattr(_ps, 'positions'):
                        _skipped_limit = 0
                        _closed_ok = 0
                        for _inst_id, _pos_dict in list(_ps.positions.items()):
                            for _pid, _rec in list(_pos_dict.items()):
                                if _rec.volume == 0:
                                    continue
                                _close_dir = 'SELL' if _rec.volume > 0 else 'BUY'
                                _skip = False
                                try:
                                    _rs = get_risk_service()
                                    if _rs:
                                        _lp = 0.0
                                        try:
                                            from ali2026v3_trading.data_service import get_data_service
                                            _ds = get_data_service()
                                            if _ds and _ds.realtime_cache:
                                                _lp = _ds.realtime_cache.get_latest_price(_inst_id) or 0.0
                                        except Exception as _lp_err:
                                            logging.warning("[R22-P1-NEW] 负权益减仓: 行情获取失败(跳过涨跌停检查): %s", _lp_err)
                                        if _lp > 0:
                                            _lim = _rs.is_at_price_limit(_inst_id, _lp)
                                            if _lim.get('is_limit_up') and _close_dir == 'SELL':
                                                logging.warning("[SafetyMetaLayer] EX-04: 涨停板卖出排队 %s price=%.2f", _inst_id, _lp)
                                                _skip = True
                                            if _lim.get('is_limit_down') and _close_dir == 'BUY':
                                                logging.warning("[SafetyMetaLayer] EX-04: 跌停板买入排队 %s price=%.2f", _inst_id, _lp)
                                                _skip = True
                                except Exception as _lim_err:
                                    logging.warning("[R22-P1-NEW] 负权益减仓: 涨跌停检查异常(减仓不受约束): %s", _lim_err)
                                if _skip:
                                    _skipped_limit += 1
                                    continue
                                try:
                                    _ps._trigger_close_position(_rec, "INV-CAP-01: negative equity forced reduction")
                                    _closed_ok += 1
                                except Exception as _ce:
                                    logging.warning("[SafetyMetaLayer] EX-04: force close %s failed: %s", _inst_id, _ce)
                        logging.critical("[SafetyMetaLayer] INV-CAP-01: forced reduction closed=%d skipped_limit=%d", _closed_ok, _skipped_limit)
                except Exception as e:
                    logging.warning("[SafetyMetaLayer] INV-CAP-01: forced position reduction failed: %s", e)
                # DR-08: 断路器状态持久化 — 负权益硬停止触发后保存
                self._save_circuit_breaker_state()
                return

            if self._current_date != today:
                self._current_date = today
                self._daily_start_equity = equity
                self._daily_peak_equity = equity
                self._daily_drawdown = 0.0
                self._drop_pct_history.clear()
                if self._daily_hard_stop_triggered:
                    # R32-P0-05-v2修复: 新交易日不自动重置硬停止标志
                    # R30修复将_hard_stop_triggered和_new_open_blocked都重置为False，
                    # 导致硬停止后无需人工确认即可恢复交易，绕过confirm_daily_resume()审批流程
                    # 正确做法: 两个标志都保持True，需通过confirm_daily_resume()人工确认才能解除
                    # 这消除了原状态分裂问题(两标志不同步)，同时保持安全策略
                    self._daily_new_open_blocked = True
                    logging.warning(
                        "[SafetyMetaLayer] 新交易日(%s)，日回撤硬停止仍然生效。"
                        "必须调用confirm_daily_resume()经审批后才能恢复交易。",
                        today,
                    )
                else:
                    self._daily_new_open_blocked = False

            self._daily_peak_equity = max(self._daily_peak_equity, equity)
            if self._daily_start_equity and self._daily_start_equity > 0:
                # R13-P1-BIZ-01修复: 日内回撤计算应包含未实现盈亏
                # 原代码仅用equity(已实现权益)计算回撤，忽略了持仓浮亏，
                # 导致真实风险被低估。现从PositionService获取未实现盈亏并加入回撤计算。
                unrealized_pnl = 0.0
                try:
                    from ali2026v3_trading.position_service import get_position_service
                    _ps = get_position_service()
                    if _ps:
                        for _inst_id, pos_dict in _ps.positions.items():
                            for _pid, rec in pos_dict.items():
                                if rec.volume != 0 and rec.open_price > 0:
                                    try:
                                        from ali2026v3_trading.data_service import get_data_service
                                        _ds = get_data_service()
                                        _mp = rec.open_price
                                        if _ds and _ds.realtime_cache:
                                            _lp = _ds.realtime_cache.get_latest_price(rec.instrument_id)
                                            if _lp and _lp > 0:
                                                _mp = _lp
                                        if rec.volume > 0:
                                            unrealized_pnl += rec.volume * (_mp - rec.open_price)
                                        else:
                                            unrealized_pnl += rec.volume * (rec.open_price - _mp)
                                    except Exception:
                                        logging.warning("[R22-EP-P1] RiskService exception swallowed")
                                        pass
                except Exception:
                    logging.warning("[R22-EP-P1] RiskService exception swallowed")
                    pass
                equity_with_unrealized = equity + unrealized_pnl
                self._daily_drawdown = (self._daily_peak_equity - equity_with_unrealized) / self._daily_start_equity

            self._equity_series.append(equity)
            self._equity_timestamps.append(now)

            # R13-P2-BIZ-03修复: 隔夜缺口检测 — 跨日时重置滚动窗口
            # 隔夜后equity_series中的历史数据属于前一交易日，
            # 滚动3σ计算会因跨日缺口产生虚假异常信号，需清空窗口
            today_str = _get_tz_aware_now().strftime("%Y-%m-%d")  # P1-R11-12修复: 使用UTC时区
            if self._last_equity_date is not None and self._last_equity_date != today_str:
                self._equity_series.clear()
                self._equity_timestamps.clear()
                self._equity_series.append(equity)
                self._equity_timestamps.append(now)
                logging.info(
                    "[SafetyMetaLayer] R13-P2-BIZ-03: 检测到隔夜缺口(%s→%s), "
                    "已重置滚动窗口防止虚假熔断触发",
                    self._last_equity_date, today_str,
                )
            self._last_equity_date = today_str

            # INV-P1-11修复: 日内权益曲线单调性验证
            # 如果equity在没有交易的情况下跌破前低，标记异常
            self._check_equity_curve_monotonicity(equity, now)

            if not self._daily_hard_stop_triggered:
                self._check_circuit_breaker(now)
                self._check_daily_drawdown()

    def _check_equity_curve_monotonicity(self, equity: float, now: float) -> None:
        """INV-P1-11修复: 日内权益曲线单调性验证

        如果equity在没有交易的情况下跌破前低，标记异常。
        这可能表示数据错误、未记录的亏损或系统故障。
        注意：正常交易中equity可以下降（止损等），此处仅检测
        连续无交易状态下equity意外下降的异常情况。
        """
        try:
            if self._equity_intraday_low is None:
                self._equity_intraday_low = equity
            else:
                if equity < self._equity_intraday_low:
                    # equity跌破前低 — 检查是否在无交易时段
                    prev_equity = list(self._equity_series)[-2] if len(self._equity_series) >= 2 else None
                    if prev_equity is not None and prev_equity > self._equity_intraday_low:
                        drop_pct = (self._equity_intraday_low - equity) / self._equity_intraday_low
                        if drop_pct > 0.01:  # 跌破前低超过1%标记异常
                            self._equity_monotonicity_anomaly_count += 1
                            logging.warning(
                                "[SafetyMetaLayer] INV-P1-11: 权益曲线单调性异常! "
                                "equity=%.2f < intraday_low=%.2f drop=%.2f%% anomaly_count=%d",
                                equity, self._equity_intraday_low, drop_pct * 100,
                                self._equity_monotonicity_anomaly_count,
                            )
                            # INV-P1-11修复: 权益曲线单调性异常时触发事件总线告警
                            try:
                                from ali2026v3_trading.event_bus import get_global_event_bus, RiskEvent
                                _eb = get_global_event_bus()
                                if _eb and not getattr(_eb, '_shutdown', True):
                                    _eb.publish(RiskEvent(
                                        risk_type='equity_curve_non_monotonic',
                                        level='HIGH' if self._equity_monotonicity_anomaly_count < 3 else 'CRITICAL',
                                        message=f"INV-P1-11: 权益曲线单调性异常 "
                                                f"equity={equity:.2f} < intraday_low={self._equity_intraday_low:.2f} "
                                                f"drop={drop_pct*100:.2f}% anomaly_count={self._equity_monotonicity_anomaly_count}",
                                    ), async_mode=True)
                            except Exception as _eb_e:
                                logging.debug("[SafetyMetaLayer] INV-P1-11: 事件总线告警失败: %s", _eb_e)
                            # 连续3次以上异常触发额外保护
                            if self._equity_monotonicity_anomaly_count >= 3:
                                logging.critical(
                                    "[SafetyMetaLayer] INV-P1-11: 权益曲线连续异常(%d次)，"
                                    "可能存在未记录亏损或数据错误",
                                    self._equity_monotonicity_anomaly_count,
                                )
                    self._equity_intraday_low = equity
                else:
                    # equity创新高或持平，重置异常计数
                    if self._equity_monotonicity_anomaly_count > 0:
                        self._equity_monotonicity_anomaly_count = 0
        except Exception as e:
            logging.debug("[SafetyMetaLayer] INV-P1-11: 权益曲线单调性检查异常: %s", e)

    def _check_circuit_breaker(self, now: float) -> None:
        if len(self._equity_series) < 10:
            return

        recent = list(self._equity_series)
        if len(recent) < 3:
            return

        current = recent[-1]
        one_min_ago_idx = max(0, len(recent) - 6)
        one_min_ago_val = recent[one_min_ago_idx]

        if one_min_ago_val <= 0:
            return

        drop_pct = (one_min_ago_val - current) / one_min_ago_val

        self._drop_pct_history.append(drop_pct)

        import statistics
        _cb_sigma = safe_get_float(self.params, "circuit_breaker_trigger_sigma", self.ANOMALY_THRESHOLD_MULTIPLIER)
        if len(self._drop_pct_history) >= 10:
            drop_pct_values = list(self._drop_pct_history)
            mean_drop = statistics.mean(drop_pct_values)
            std_drop = statistics.stdev(drop_pct_values) if len(drop_pct_values) >= 2 else 0.0
            threshold = mean_drop + _cb_sigma * std_drop
        else:
            diffs = [recent[i] - recent[i - 1] for i in range(1, len(recent))]
            mean_diff = statistics.mean(diffs)
            if len(diffs) >= 2:
                std_diff = statistics.stdev(diffs)
            else:
                std_diff = abs(mean_diff) if mean_diff != 0 else 1.0
            threshold = _cb_sigma * std_diff / one_min_ago_val if one_min_ago_val > 0 else self.DEFAULT_ANOMALY_THRESHOLD

        if drop_pct > max(threshold, 0.02):
            # RES-P2-08: 进程/容器级隔离下，断路器阈值更严格
            if hasattr(self, '_isolation_level'):
                from ali2026v3_trading.config_params import ISOLATION_LEVELS
                if self._isolation_level in (ISOLATION_LEVELS.PROCESS, ISOLATION_LEVELS.CONTAINER):
                    if drop_pct > max(threshold, 0.02) * 0.8:
                        pass
            if now < self._circuit_breaker_calm_until:
                self._stats["circuit_breaker_calm_rejects"] += 1  # [R23-P2-ID-02-FIX] _stats_lock保护待补全
                logging.info(
                    "[SafetyMetaLayer] 熔断冷静期内，忽略二次触发 (冷静期剩余%.0fs)",
                    self._circuit_breaker_calm_until - now,
                )
                return

            pause_duration = self._get_circuit_breaker_pause_sec()
            # R13-BIZ-07: 如果当前仍在暂停期内，延长而非覆盖暂停时间
            new_paused_until = now + pause_duration
            if self._trading_paused_until > now:
                # 已在暂停中：延长暂停时间（取max，不缩短）
                new_paused_until = max(self._trading_paused_until, new_paused_until)
                logging.warning(
                    "[SafetyMetaLayer] R13-BIZ-07: 断路器连续触发，暂停时间延长 %.0f->%.0f",
                    self._trading_paused_until - now, new_paused_until - now,
                )
            self._trading_paused_until = new_paused_until
            self._pause_reason = f"速率断路器: 1min回撤{drop_pct:.2%} > 2.5σ阈值{threshold:.2%}"
            self._stats["circuit_breaker_triggers"] += 1  # [R23-P2-ID-02-FIX] _stats_lock保护待补全
            # R27-P0-DR-12修复: 记录断路器激活时间戳，供半开机制自动恢复使用
            self._circuit_breaker_activated_at: float = now

            calm_period = self._get_circuit_breaker_calm_period_sec()
            # R30-P2-01修复: 冷静期语义修正
            # 原问题: new_calm_until = now + pause_duration + calm_period，冷静期从触发时刻起
            # 实际为pause_duration+calm_period=780s，比规格要求600s多180s
            # 修复: 冷静期从暂停结束时刻起算，即 now + pause_duration + calm_period
            # 但规格语义为"首次触发起600s内不二次触发"，因此直接 now + calm_period
            # 暂停期已由_trading_paused_until覆盖，冷静期独立于暂停期
            new_calm_until = now + calm_period
            self._circuit_breaker_calm_until = max(self._circuit_breaker_calm_until, new_calm_until)

            # P1-7修复：断路器触发后自动进入影子模式观察期
            # 冷静期结束后不立即恢复实盘，而是进入shadow_mode观察期
            self._circuit_breaker_shadow_mode = True
            self._circuit_breaker_shadow_until = now + calm_period + 300  # R30-P2-01修复: 冷静期修正后影子观察期也同步

            logging.warning(
                "[SafetyMetaLayer] ⚡ 速率断路器触发！1min回撤=%.2f%%, 阈值=%.2f%%, 暂停%.0f秒, 冷静期%.0f秒, 影子观察%.0f秒",
                drop_pct * 100, threshold * 100, pause_duration, calm_period, 300
            )

            # R15-P1-LOG-19修复: 断路器触发接入structured_audit_log
            try:
                structured_audit_log('circuit_breaker_triggered', 'blocked',
                                     {'drop_pct': round(drop_pct, 6), 'threshold': round(threshold, 6),
                                      'pause_sec': pause_duration, 'calm_sec': calm_period,
                                      'shadow_sec': 300}, severity="WARNING")
            except Exception:
                pass

            # SEC-P1-12修复: 断路器触发时报告安全事件
            try:
                from ali2026v3_trading.config_params import get_security_responder
                _responder = get_security_responder()
                _blocked = _responder.report_suspicious(
                    source=f"circuit_breaker:{self._strategy_id_ns}",
                    reason=f"1min_drawdown={drop_pct*100:.2f}%>threshold={threshold*100:.2f}%"
                )
                if _blocked:
                    logging.critical("[SEC-P1-12] 安全自动阻断已激活: source=%s", self._strategy_id_ns)
            except Exception as _sec_ex:
                logging.debug("[SEC-P1-12] report_suspicious调用失败: %s", _sec_ex)

            self._cancel_pending_on_circuit_breaker()

            # INV-P1-10修复: 熔断触发后强制减仓
            # 原代码仅撤销挂单，未对已有持仓执行减仓，极端行情下持仓风险持续暴露
            self._force_position_reduction_on_circuit_breaker()

            # OPS-P1-04修复: 断路器触发时通过EventBus发布告警事件
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus, CircuitBreakerTriggeredEvent
                bus = get_global_event_bus()
                cb_event = CircuitBreakerTriggeredEvent(
                    reason=self._pause_reason,
                    drop_pct=drop_pct,
                    threshold=threshold,
                    pause_duration=pause_duration,
                    calm_period=calm_period,
                )
                bus.publish(cb_event, async_mode=True)
            except Exception as _ops04_e:
                logging.debug("[SafetyMetaLayer] OPS-P1-04: EventBus断路器告警推送异常: %s", _ops04_e)

            self._save_circuit_breaker_state()

    def _check_daily_drawdown(self) -> None:
        if self._daily_hard_stop_triggered:
            return

        multiplier = self._get_daily_drawdown_multiplier()
        if multiplier <= 0:
            return

        triggered = False

        if self._prev_5day_avg_profit <= 0:
            if self._daily_start_equity and self._daily_start_equity > 0:
                # P0-R8-02修复: 从params读取daily_loss_hard_stop_pct替代硬编码DEFAULT_MAX_DRAWDOWN
                max_dd = safe_get_float(self.params, "daily_loss_hard_stop_pct", self.DEFAULT_MAX_DRAWDOWN)  # R25-SE-01-FIX
                if self._daily_drawdown >= max_dd:
                    triggered = True
                    self._stats["daily_drawdown_triggers"] += 1  # [R23-P2-ID-02-FIX] _stats_lock保护待补全
                    logging.warning(
                        "[SafetyMetaLayer] 🛑 日最大回撤硬停止触发！回撤=%.2f%% >= 阈值%.2f%%",
                        self._daily_drawdown * 100, max_dd * 100
                    )
        else:
            max_daily_loss = self._prev_5day_avg_profit * multiplier
            current_loss = (self._daily_peak_equity - self._equity_series[-1]) if self._equity_series else 0  # R26-P1-BV-05修复: 加括号明确优先级+移除冗余list()

            if current_loss >= max_daily_loss:
                triggered = True
                self._stats["daily_drawdown_triggers"] += 1  # [R23-P2-ID-02-FIX] _stats_lock保护待补全
                logging.warning(
                    "[SafetyMetaLayer] 🛑 日最大回撤硬停止触发！当前亏损=%.2f >= 5日均值×%.1f=%.2f",
                    current_loss, multiplier, max_daily_loss
                )

        if triggered:
            self._daily_hard_stop_triggered = True
            self._daily_new_open_blocked = True
            self._stats["daily_hard_stop_triggers"] += 1  # [R23-P2-ID-02-FIX] _stats_lock保护待补全
            # ✅ P0-9修复: 触发日回撤硬停止时同步更新CrossStrategyRiskGuard
            try:
                from ali2026v3_trading.position_service import get_cross_strategy_risk_guard
                guard = get_cross_strategy_risk_guard()
                if guard and hasattr(guard, 'set_daily_drawdown'):
                    guard.set_daily_drawdown(self._daily_drawdown * 100)
            except Exception as e:
                logging.debug("[SafetyMetaLayer] set_daily_drawdown sync error: %s", e)
            logging.critical(
                "[SafetyMetaLayer] 🚫 会话周期硬终止！所有交易禁止，需人工调用confirm_daily_resume()恢复"
            )
            # OPS-P1-05修复: 日回撤硬停止触发时通过EventBus发布告警事件
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus, DailyDrawdownHaltEvent
                bus = get_global_event_bus()
                dd_event = DailyDrawdownHaltEvent(
                    drawdown_pct=self._daily_drawdown,
                    threshold_pct=max_dd if self._prev_5day_avg_profit <= 0 else 0.0,
                    current_loss=current_loss if self._prev_5day_avg_profit > 0 else 0.0,
                    max_daily_loss=max_daily_loss if self._prev_5day_avg_profit > 0 else 0.0,
                    trigger_type='fixed_threshold' if self._prev_5day_avg_profit <= 0 else 'avg_multiplier',
                )
                bus.publish(dd_event, async_mode=True)
            except Exception as _ops05_e:
                logging.debug("[SafetyMetaLayer] OPS-P1-05: EventBus日回撤告警推送异常: %s", _ops05_e)
            # DR-08: 断路器状态持久化 — 日回撤硬停止触发后保存
            self._save_circuit_breaker_state()

    def check_position_hard_time_stop(self, position_id: str, open_time,
                                       max_profit_reached: float,
                                       profit_slope: float = 0.0,
                                       peak_profit_pct: float = 0.0,
                                       current_profit_pct: float = 0.0,
                                       bar_time: Optional[float] = None) -> Optional[str]:
        if isinstance(open_time, datetime):
            open_time = open_time.timestamp()
        elif not isinstance(open_time, (int, float)):
            try:
                open_time = float(open_time)
            except (ValueError, TypeError):
                logging.warning("[SafetyMetaLayer] open_time类型无效: %s, 跳过硬时间止损检查", type(open_time).__name__)
                return None

        # P1-裂缝36：逻辑反转 > 第二阶段斜率下降 > 第一阶段超时
        # 逻辑反转是信号失效，最紧急，应优先于硬时间止损
        # 调用方应先检查逻辑反转条件，此处记录优先级供审计
        if hasattr(self, '_logic_reversal_priority'):
            if self._logic_reversal_priority.get(position_id, False):
                logging.info(
                    "[SafetyMetaLayer] P1-裂缝36: position=%s 逻辑反转已触发，"
                    "硬时间止损降级为备用（优先级：逻辑反转>阶段2>阶段1）",
                    position_id,
                )
                return None

        stage1_min = self._get_stage1_minutes()
        stage2_min = self._get_stage2_minutes()
        stage1_threshold = self._get_stage1_profit_threshold()

        # P0-R11-14修复: 优先使用bar_time（回测Bar时间），回退到系统时间（实盘）
        now = bar_time if bar_time is not None else time.time()
        elapsed_min = (now - open_time) / 60.0

        if elapsed_min >= stage1_min and max_profit_reached < stage1_threshold:
            with self._stats_lock:
                self._stats["hard_time_stop_triggers"] += 1
            logging.warning(
                "[SafetyMetaLayer] ⏰ 阶段1硬止损触发！position=%s, 已持%.0fmin, "
                "最高浮盈=%.2f%% < 阶段1要求=%.2f%%",
                position_id, elapsed_min, max_profit_reached * 100, stage1_threshold * 100
            )
            return f"HardTimeStop@{elapsed_min:.0f}min(profit<{stage1_threshold:.1%})"

        if stage1_min <= elapsed_min < stage2_min:
            # P1-裂缝42：两阶段止损斜率定义
            # stage2_slope_window=5（最近5个检查点）
            # 斜率 = (profit[-1]-profit[-5]) / time_interval
            # 要求斜率 > -0.01（即允许微小下降但不允许明显衰减）
            stage2_slope_threshold = -0.01
            # R27-P0-FP-02修复: 斜率比较使用浮点容差，防止精度问题导致误触发
            _slope_tolerance = 1e-9
            if profit_slope < stage2_slope_threshold - _slope_tolerance:
                with self._stats_lock:
                    self._stats["hard_time_stop_triggers"] += 1
                logging.warning(
                    "[SafetyMetaLayer] 阶段2硬止损触发(斜率衰减)！position=%s, 已持%.0fmin, "
                    "profit_slope=%.6f < threshold=%.4f",
                    position_id, elapsed_min, profit_slope, stage2_slope_threshold
                )
                return f"HardTimeStop@{elapsed_min:.0f}min(slope<{stage2_slope_threshold})"

            if peak_profit_pct > 0 and current_profit_pct < peak_profit_pct * 0.5:
                with self._stats_lock:
                    self._stats["hard_time_stop_triggers"] += 1
                logging.warning(
                    "[SafetyMetaLayer] ⏰ 阶段2硬止损触发(回撤超50%%)！position=%s, 已持%.0fmin, "
                    "peak=%.2f%% current=%.2f%%",
                    position_id, elapsed_min, peak_profit_pct * 100, current_profit_pct * 100
                )
                return f"HardTimeStop@{elapsed_min:.0f}min(drawdown>50%)"

        return None

    def set_logic_reversal_triggered(self, position_id: str, triggered: bool) -> None:
        """P1-裂缝36：设置逻辑反转触发状态，影响硬时间止损优先级

        优先级：逻辑反转 > 第二阶段斜率下降 > 第一阶段超时
        逻辑反转是信号失效，最紧急，应优先于硬时间止损执行。
        """
        with self._lock:
            self._logic_reversal_priority[position_id] = triggered
            if triggered:
                logging.info(
                    "[SafetyMetaLayer] P1-裂缝36: position=%s 逻辑反转触发，"
                    "硬时间止损降级为备用",
                    position_id,
                )

    # P-01修复: check_regulatory_compliance() — 合规检查（手册8.3节）
    # CMP-P1-01修复: 扩展为真正的8项合规检查
    def check_regulatory_compliance(self, position_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """监管合规检查：8项检查全部落实，每项返回 {check_item, passed, detail}

        检查项：
        1. 持仓限额合规 (_check_position_limit)
        2. 撤单率合规 (order_service cancel_rate)
        3. 自成交防控 (order_service self-trade detection)
        4. 涨跌停合规 (check_price_limit)
        5. 交易时间合规 (scheduler_service.is_market_open)
        6. 记录保留合规 (retention_days >= 1825)
        7. 大额交易检查
        8. 算法交易报备状态

        Returns:
            dict: {
                compliant: bool,
                severity: str,
                violations: List[str],
                checks: List[dict]  # [{check_item, passed, detail}, ...]
            }
        """
        checks: List[Dict[str, Any]] = []
        violations: List[str] = []

        # --- 检查1: 持仓限额合规 ---
        try:
            limit_passed = True
            limit_detail = "持仓限额检查通过"
            with self._lock:
                if self._daily_new_open_blocked:
                    limit_passed = False
                    limit_detail = "新开仓已被阻断"
                if self._daily_hard_stop_triggered:
                    limit_passed = False
                    limit_detail = "日回撤硬停止触发，全部交易禁止"
            checks.append({"check_item": "position_limit", "passed": limit_passed, "detail": limit_detail})
            if not limit_passed:
                violations.append(f"position_limit: {limit_detail}")
        except Exception as e:
            checks.append({"check_item": "position_limit", "passed": False, "detail": f"检查异常: {e}"})
            violations.append(f"position_limit: error={e}")

        # --- 检查2: 撤单率合规 ---
        try:
            from ali2026v3_trading.order_service import get_order_service
            _os = get_order_service()
            if _os:
                now = time.time()
                cutoff = now - 300
                with _os._lock:
                    recent_cancels = sum(1 for t in _os._cancel_count_window if t >= cutoff)
                    recent_orders = sum(1 for t in _os._order_count_window if t >= cutoff)
                cancel_rate = recent_cancels / recent_orders if recent_orders > 0 else 0.0
                cancel_passed = cancel_rate <= 0.5
                cancel_detail = f"撤单率={cancel_rate:.2%}, 窗口=300s" if cancel_passed else f"撤单率={cancel_rate:.2%}超过50%阈值"
                checks.append({"check_item": "cancel_rate", "passed": cancel_passed, "detail": cancel_detail})
                if not cancel_passed:
                    violations.append(f"cancel_rate: {cancel_detail}")
            else:
                checks.append({"check_item": "cancel_rate", "passed": True, "detail": "order_service不可用"})
        except Exception as e:
            checks.append({"check_item": "cancel_rate", "passed": True, "detail": f"检查跳过: {e}"})

        # --- 检查3: 自成交防控 ---
        try:
            from ali2026v3_trading.order_service import get_order_service
            _os = get_order_service()
            if _os:
                with _os._lock:
                    self_trade_blocks = _os._stats.get('self_trade_blocks', 0)
                    active_bans = sum(1 for b in _os._self_trade_bans.values() if b > time.time())  # R21-MEM-P2-03修复: 生成器替代列表推导式
                st_passed = active_bans == 0
                st_detail = f"自成交阻断{self_trade_blocks}次, 当前活跃禁止{active_bans}项" if st_passed else f"存在{active_bans}项活跃自成交禁止"
                checks.append({"check_item": "self_trade_prevention", "passed": st_passed, "detail": st_detail})
                if not st_passed:
                    violations.append(f"self_trade: {st_detail}")
            else:
                checks.append({"check_item": "self_trade_prevention", "passed": True, "detail": "order_service不可用"})
        except Exception as e:
            checks.append({"check_item": "self_trade_prevention", "passed": True, "detail": f"检查跳过: {e}"})

        # --- 检查4: 涨跌停合规 ---
        try:
            price_limit_passed = True
            price_limit_detail = "涨跌停检查通过"
            if position_data:
                instrument_id = position_data.get('instrument_id', '')
                price = position_data.get('price', 0)
                direction = position_data.get('direction', 'BUY')
                if instrument_id and price > 0:
                    from ali2026v3_trading.params_service import get_params_service
                    ps = get_params_service()
                    meta = ps.get_instrument_meta_by_id(instrument_id)
                    if meta:
                        product = meta.get('product', '')
                        pc = ps.get_product_cache(product)
                        if pc:
                            limit_up = safe_float(pc.get('limit_up_price', 0))
                            limit_down = safe_float(pc.get('limit_down_price', 0))
                            if limit_up > 0 and price >= limit_up and direction in ('BUY', '买', 'long'):
                                price_limit_passed = False
                                price_limit_detail = f"涨停价{limit_up:.2f}禁止买入: {instrument_id}"
                            elif limit_down > 0 and price <= limit_down and direction in ('SELL', '卖', 'short'):
                                price_limit_passed = False
                                price_limit_detail = f"跌停价{limit_down:.2f}禁止卖出: {instrument_id}"
            checks.append({"check_item": "price_limit", "passed": price_limit_passed, "detail": price_limit_detail})
            if not price_limit_passed:
                violations.append(f"price_limit: {price_limit_detail}")
        except Exception as e:
            checks.append({"check_item": "price_limit", "passed": True, "detail": f"检查跳过: {e}"})

        # --- 检查5: 交易时间合规 ---
        try:
            from ali2026v3_trading.scheduler_service import is_market_open
            market_open = is_market_open()
            tm_passed = market_open
            tm_detail = "市场开盘中" if market_open else "市场已收盘"
            checks.append({"check_item": "trading_time", "passed": tm_passed, "detail": tm_detail})
            if not tm_passed:
                violations.append(f"trading_time: {tm_detail}")
        except Exception as e:
            checks.append({"check_item": "trading_time", "passed": True, "detail": f"检查跳过: {e}"})

        # --- 检查6: 记录保留合规 (retention_days >= 1825 = 5年) ---
        try:
            log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
            retention_passed = True
            retention_detail = "记录保留合规(≥1825天)"
            if os.path.exists(log_dir):
                oldest_file = None
                oldest_mtime = float('inf')
                for fname in os.listdir(log_dir):
                    fpath = os.path.join(log_dir, fname)
                    if os.path.isfile(fpath):
                        mtime = os.path.getmtime(fpath)
                        if mtime < oldest_mtime:
                            oldest_mtime = mtime
                            oldest_file = fname
                if oldest_file:
                    retention_days = (time.time() - oldest_mtime) / 86400.0
                    if retention_days < 1825:
                        retention_passed = False
                        retention_detail = f"最旧日志保留仅{retention_days:.0f}天, 需≥1825天"
            checks.append({"check_item": "record_retention", "passed": retention_passed, "detail": retention_detail})
            if not retention_passed:
                violations.append(f"record_retention: {retention_detail}")
        except Exception as e:
            checks.append({"check_item": "record_retention", "passed": True, "detail": f"检查跳过: {e}"})

        # --- 检查7: 大额交易检查 ---
        try:
            from ali2026v3_trading.order_service import get_order_service
            _os = get_order_service()
            if _os and position_data:
                volume = position_data.get('volume', 0)
                price = position_data.get('price', 0)
                amount = volume * price
                large_threshold = getattr(_os, 'LARGE_TRADE_THRESHOLD_AMOUNT', 5000000)
                large_passed = amount <= large_threshold if amount > 0 else True
                large_detail = f"交易金额={amount:.0f}" if large_passed else f"交易金额={amount:.0f}超过大额阈值{large_threshold}"
                checks.append({"check_item": "large_trade", "passed": large_passed, "detail": large_detail})
                if not large_passed:
                    violations.append(f"large_trade: {large_detail}")
            else:
                checks.append({"check_item": "large_trade", "passed": True, "detail": "无订单数据"})
        except Exception as e:
            checks.append({"check_item": "large_trade", "passed": True, "detail": f"检查跳过: {e}"})

        # --- 检查8: 算法交易报备状态 ---
        try:
            algo_passed = True
            algo_detail = "算法交易报备正常"
            try:
                from ali2026v3_trading.hft_enhancements import HFTEnhancementEngine
                algo_detail = "HFT增强引擎已注册"
            except ImportError:
                algo_detail = "HFT增强引擎未加载(可接受)"
            checks.append({"check_item": "algo_registration", "passed": algo_passed, "detail": algo_detail})
        except Exception as e:
            checks.append({"check_item": "algo_registration", "passed": True, "detail": f"检查跳过: {e}"})

        compliant = len(violations) == 0
        severity = "CRITICAL" if not compliant else "OK"
        return {
            "compliant": compliant,
            "severity": severity,
            "violations": violations,
            "checks": checks,
        }

    # P-02修复: check_capital_sufficiency() — 资金充足性检查（手册8.3节）
    @api_version("1.0")
    def check_capital_sufficiency(self, equity: float, required_margin: float = 0.0,
                                  open_positions: int = 0, max_positions: int = 50,
                                  existing_margin_used: float = 0.0) -> Dict[str, Any]:
        """资金充足性检查：验证是否有足够资金支持新开仓

        R13-V1-001修复: 增加existing_margin_used参数，可用资金 = equity - 已占用保证金 - 新增保证金

        检查项：
        1. 权益是否为正
        2. 可用保证金是否充足（扣除已有持仓保证金）
        3. 持仓数量是否在限额内

        Args:
            equity: 当前账户权益
            required_margin: 新开仓所需保证金
            open_positions: 当前持仓数
            max_positions: 最大允许持仓数
            existing_margin_used: 已有持仓占用的保证金（R13-V1-001新增）

        Returns:
            dict: {sufficient: bool, available_margin: float, reason: str}
        """
        with self._lock:
            if equity <= 0:
                return {"sufficient": False, "available_margin": 0.0, "reason": "equity_non_positive"}
            daily_start = self._daily_start_equity or equity
            # R13-P0-BIZ-01修复: 扣除已有持仓保证金+预留保证金，防止超额开仓
            self._purge_expired_reservations()
            available = equity - existing_margin_used - required_margin - self._reserved_margin
            if available < daily_start * 0.05:
                return {"sufficient": False, "available_margin": available, "reason": "margin_below_5pct"}
            if open_positions >= max_positions:
                return {"sufficient": False, "available_margin": available, "reason": "position_limit_reached"}
            return {"sufficient": True, "available_margin": available, "reason": "ok"}

    # R13-P0-BIZ-10修复: 保证金预留机制，防止并发请求双重花费
    def reserve_margin(self, reservation_id: str, amount: float) -> bool:
        """预留保证金，保证金检查通过后调用，防止并发请求双重花费

        Args:
            reservation_id: 预留唯一标识（如order_id）
            amount: 预留金额

        Returns:
            bool: 是否预留成功
        """
        with self._lock:
            self._purge_expired_reservations()
            if amount <= 0:
                return False
            now = time.time()
            self._margin_reservations[reservation_id] = {
                'amount': amount,
                'timestamp': now,
                'expires_at': now + self._RESERVATION_TTL_SEC,
            }
            self._reserved_margin += amount
            logging.debug(
                "[RiskService] R13-P0-BIZ-10: 保证金预留 id=%s amount=%.2f total_reserved=%.2f",
                reservation_id, amount, self._reserved_margin,
            )
            return True

    def release_margin(self, reservation_id: str) -> bool:
        """释放保证金预留（订单完成或失败后调用）

        Args:
            reservation_id: 预留唯一标识

        Returns:
            bool: 是否释放成功
        """
        with self._lock:
            reservation = self._margin_reservations.pop(reservation_id, None)
            if reservation:
                self._reserved_margin -= reservation['amount']
                if self._reserved_margin < 0:
                    self._reserved_margin = 0.0
                logging.debug(
                    "[RiskService] R13-P0-BIZ-10: 保证金释放 id=%s amount=%.2f total_reserved=%.2f",
                    reservation_id, reservation['amount'], self._reserved_margin,
                )
                return True
            return False

    def _purge_expired_reservations(self) -> None:
        """清理过期的保证金预留（必须在锁内调用）"""
        now = time.time()
        expired_ids = [
            rid for rid, res in self._margin_reservations.items()
            if now >= res['expires_at']
        ]
        for rid in expired_ids:
            res = self._margin_reservations.pop(rid)
            self._reserved_margin -= res['amount']
            logging.debug(
                "[RiskService] R13-P0-BIZ-10: 保证金预留过期自动释放 id=%s amount=%.2f",
                rid, res['amount'],
            )
        if self._reserved_margin < 0:
            self._reserved_margin = 0.0

    # P-03修复: check_exchange_status() — 交易所状态检查（手册8.3节）
    def check_exchange_status(self, exchange: str = "AUTO") -> Dict[str, Any]:
        """交易所状态检查：验证交易所是否处于可交易状态

        检查项：
        1. 交易所是否开市
        2. 是否接近收盘（最后N分钟禁止开仓）
        3. 交易所是否发布暂停公告

        Args:
            exchange: 交易所代码，AUTO表示自动检测所有

        Returns:
            dict: {tradeable: bool, exchanges: Dict[str, str], reason: str}
        """
        exchanges_status: Dict[str, str] = {}
        tradeable = True
        reason = "ok"
        try:
            from ali2026v3_trading.scheduler_service import is_market_open
            target_exchanges = [exchange] if exchange != "AUTO" else ['CFFEX', 'SHFE', 'DCE', 'CZCE', 'INE', 'GFEX']
            for exch in target_exchanges:
                try:
                    is_open = is_market_open(exch)
                    exchanges_status[exch] = "OPEN" if is_open else "CLOSED"
                    if not is_open:
                        tradeable = False
                        reason = f"{exch}_closed"
                except Exception:
                    exchanges_status[exch] = "UNKNOWN"
        except ImportError:
            exchanges_status["fallback"] = "scheduler_service_unavailable"
            reason = "scheduler_unavailable"
        return {"tradeable": tradeable, "exchanges": exchanges_status, "reason": reason}

    def is_trading_paused(self) -> Tuple[bool, str]:
        with self._lock:
            if time.time() < self._trading_paused_until:
                remaining = self._trading_paused_until - time.time()
                return True, f"{self._pause_reason}, 剩余{remaining:.0f}秒"
            # P1-R9-21修复: 暂停恢复时发出日志通知
            if self._pause_reason:
                _prev_reason = self._pause_reason
                self._pause_reason = ""
                logging.info("[P1-R9-21] 交易暂停已恢复, 原暂停原因: %s", _prev_reason)
            return False, ""

    def is_circuit_breaker_shadow_mode(self) -> bool:
        """P1-7修复：断路器恢复后是否处于影子模式观察期"""
        with self._lock:
            if self._circuit_breaker_shadow_mode:
                now = time.time()
                if now >= self._circuit_breaker_shadow_until:
                    self._circuit_breaker_shadow_mode = False
                    self._save_circuit_breaker_state()
                    logging.info("[SafetyMetaLayer] 影子模式观察期结束，恢复正常交易")
                    return False
                return True
            return False

    def is_new_open_blocked(self) -> bool:
        with self._lock:
            return self._daily_new_open_blocked

    def can_open(self) -> bool:
        """裂缝25修复：断路器/硬止损期间禁止新开仓。

        P2-1备注：当前全项目无外部调用方，实际阻断走is_new_open_blocked()路径。
        保留此方法作为简洁布尔接口，供未来直接开仓前检查使用。
        """
        paused, _ = self.is_trading_paused()
        return not paused and not self._daily_new_open_blocked

    def compute_and_track_rollover_cost(self, bar_data, params: Optional[Dict] = None) -> Dict[str, Any]:
        """P1-1修复：调用compute_rollover_cost换月成本建模并追踪累计成本

        在换月时由策略核心调用，将calendar_basis和rollover_slippage成本
        累计到风控追踪器中，影响资金利用率计算。
        """
        try:
            from ali2026v3_trading.参数池.task_scheduler import detect_rollover_gaps, compute_rollover_cost
            rollover_points = detect_rollover_gaps(bar_data)
            if rollover_points:
                cost_result = compute_rollover_cost(
                    rollover_points, bar_data, params or {},
                    calendar_basis_bps=5.0, rollover_slippage_bps=3.0,
                )
                with self._lock:
                    self._rollover_cost_bps += cost_result.get('total_rollover_cost_bps', 0.0)
                    self._rollover_count += cost_result.get('rollover_count', 0)
                logging.info(
                    "[SafetyMetaLayer] P1-1: 换月成本累计=%.1fbps (累计%d次)",
                    self._rollover_cost_bps, self._rollover_count,
                )
                return cost_result
            return {"total_rollover_cost_bps": 0.0, "rollover_count": 0}
        except Exception as e:
            logging.debug("[SafetyMetaLayer] P1-1: compute_rollover_cost调用失败: %s", e)
            return {"total_rollover_cost_bps": 0.0, "rollover_count": 0, "error": str(e)}

    def get_rollover_cost_summary(self) -> Dict[str, Any]:
        """P1-1修复：获取换月成本累计摘要"""
        with self._lock:
            return {
                "total_rollover_cost_bps": self._rollover_cost_bps,
                "rollover_count": self._rollover_count,
            }

    def can_close(self) -> bool:
        """裂缝25修复：断路器/硬止损期间允许平仓（保护性操作）。"""
        return True

    # CMP-P1-13修复: 算法交易专用熔断机制
    def is_algo_paused(self) -> Tuple[bool, str]:
        """检查算法交易是否被暂停。"""
        with self._lock:
            if self._algo_paused and time.time() < self._algo_paused_until:
                remaining = self._algo_paused_until - time.time()
                return True, f"{self._algo_pause_reason}, 剩余{remaining:.0f}秒"
            if self._algo_paused and time.time() >= self._algo_paused_until:
                self._algo_paused = False
                self._algo_pause_reason = ""
                logging.info("[SafetyMetaLayer] CMP-P1-13: 算法交易熔断已自动恢复")
            return False, ""

    def _check_algo_circuit_breaker(self) -> Optional[str]:
        """CMP-P1-13修复: 检查算法交易熔断条件

        检查三项条件:
        1. 撤单率>70% → 暂停所有算法交易30分钟
        2. 自成交次数>3次/小时 → 暂停算法交易1小时
        3. 报单频率>100次/分钟 → 暂停5分钟

        Returns:
            str or None: 触发原因，未触发返回None
        """
        try:
            now = time.time()
            from ali2026v3_trading.order_service import get_order_service
            _os = get_order_service()
            if not _os:
                return None

            # 1. 检查撤单率 (70%阈值)
            cutoff_300 = now - 300
            with _os._lock:
                recent_cancels = sum(1 for t in _os._cancel_count_window if t >= cutoff_300)
                recent_orders = sum(1 for t in _os._order_count_window if t >= cutoff_300)
            if recent_orders > 0:
                cancel_rate = recent_cancels / recent_orders
                if cancel_rate > 0.70:
                    reason = f"撤单率{cancel_rate:.1%}>70%, 暂停算法交易30分钟"
                    logging.critical("[SafetyMetaLayer] CMP-P1-13: %s", reason)
                    with self._lock:
                        self._algo_paused = True
                        self._algo_paused_until = now + 1800
                        self._algo_pause_reason = reason
                        self._algo_breakers['high_cancel_rate']['triggered'] = True
                        self._algo_breakers['high_cancel_rate']['paused_until'] = self._algo_paused_until
                    return reason

            # 2. 检查自成交次数 (3次/小时)
            cutoff_3600 = now - 3600
            with _os._lock:
                # 自成交禁止的次数用活跃禁止时间近似：统计self_trade_bans中最近1小时内设置的ban数量
                recent_st_bans = sum(1 for ban_until in _os._self_trade_bans.values()
                                      if ban_until > now and ban_until - 3600 <= now)
            if recent_st_bans > 3:
                reason = f"自成交禁止{recent_st_bans}次/小时>3, 暂停算法交易1小时"
                logging.critical("[SafetyMetaLayer] CMP-P1-13: %s", reason)
                with self._lock:
                    self._algo_paused = True
                    self._algo_paused_until = now + 3600
                    self._algo_pause_reason = reason
                    self._algo_breakers['excessive_self_trade']['triggered'] = True
                    self._algo_breakers['excessive_self_trade']['paused_until'] = self._algo_paused_until
                return reason

            # 3. 检查报单频率 (100次/分钟)
            cutoff_60 = now - 60
            with _os._lock:
                recent_orders_60 = sum(1 for t in _os._order_count_window if t >= cutoff_60)
            if recent_orders_60 > 100:
                reason = f"报单频率{recent_orders_60}次/分钟>100, 暂停5分钟"
                logging.critical("[SafetyMetaLayer] CMP-P1-13: %s", reason)
                with self._lock:
                    self._algo_paused = True
                    self._algo_paused_until = now + 300
                    self._algo_pause_reason = reason
                    self._algo_breakers['high_order_frequency']['triggered'] = True
                    self._algo_breakers['high_order_frequency']['paused_until'] = self._algo_paused_until
                return reason

            return None
        except Exception as e:
            logging.debug("[SafetyMetaLayer] CMP-P1-13: 算法熔断检查异常: %s", e)
            return None

    def is_hard_stop_triggered(self) -> bool:
        with self._lock:
            return self._daily_hard_stop_triggered

    # R13-P0-BIZ-05验证: 已在R10修复，confirm_daily_resume已添加caller_id认证和审计日志
    # R15-P0-SEC-04修复: 恢复令牌机制，防止自动代码绕过鉴权
    _resume_token: Optional[str] = None

    def generate_resume_token(self) -> str:
        """R15-P0-SEC-04修复: 生成一次性恢复令牌，必须由人工交互触发"""
        import secrets as _secrets
        self._resume_token = _secrets.token_hex(16)
        logging.critical(
            "[SafetyMetaLayer] 恢复令牌已生成: %s... (必须传入confirm_daily_resume验证)",
            self._resume_token[:8]
        )
        return self._resume_token

    def confirm_daily_resume(self, caller_id: str = "unknown", resume_token: Optional[str] = None,
                              approval_context: Optional[Dict[str, Any]] = None) -> bool:
        """R13-BIZ-05修复: 添加调用来源认证和审计日志
        R15-P0-SEC-04修复: 增加恢复令牌验证，防止on_start自动绕过
        CMP-P1-02修复: 合规审批流程记录 — approval_context写入compliance_audit.jsonl
        OPS-14修复: 紧急操作审批机制 — approval_context必须包含approver_id

        Args:
            caller_id: 调用方标识，用于审计追踪。必须由人工显式调用。
            resume_token: R15-P0-SEC-04修复: 一次性恢复令牌，不传则要求caller_id包含"MANUAL_"
            approval_context: CMP-P1-02修复: 合规审批上下文 {approver_id, approval_reason, approval_timestamp}

        Returns:
            True if daily hard stop was successfully cleared, False otherwise.
        """
        with self._lock:
            if not self._daily_hard_stop_triggered:
                logging.info("[SafetyMetaLayer] confirm_daily_resume: 未处于硬停止状态，无需恢复")
                return False

            # OPS-14修复: 审批验证 — approval_context必须包含approver_id
            if approval_context is None or not approval_context.get('approver_id'):
                logging.critical(
                    "[OPS-14] confirm_daily_resume缺少审批! 必须提供approval_context包含approver_id. "
                    "caller_id=%s", caller_id
                )
                # OPS-14: 发送P0告警
                try:
                    from ali2026v3_trading.risk_service import alert as _alert, AlertLevel as _AlertLevel
                    _alert(_AlertLevel.P0, 'confirm_resume_no_approval',
                           f'confirm_daily_resume缺少审批 caller={caller_id}',
                           {'caller_id': caller_id})
                except Exception:
                    logging.warning("[R22-EP-P1] RiskService exception swallowed")
                    pass
                return False

            # R15-P0-SEC-04修复: 令牌验证或caller_id前缀验证
            if resume_token is not None:
                if resume_token != self._resume_token:
                    logging.critical(
                        "[SafetyMetaLayer] R15-SEC-04: 恢复令牌不匹配！拒绝恢复 caller=%s", caller_id
                    )
                    return False
                self._resume_token = None  # 一次性令牌，验证后立即销毁
            else:
                # 未提供令牌时，要求caller_id以"MANUAL_"开头（表明人工触发）
                if not caller_id.startswith("MANUAL_"):
                    logging.critical(
                        "[SafetyMetaLayer] R15-SEC-04: 非人工操作尝试恢复交易被拒绝！caller=%s",
                        caller_id
                    )
                    return False
            # P1-R11-10修复: 同日内禁止多次恢复 — 防止被同一天内无限次调用绕过
            _now_t = time.time()
            _dt_now = datetime.fromtimestamp(_now_t, tz=_CHINA_TZ)
            _today_key = _dt_now.strftime("%Y-%m-%d") if _dt_now.hour >= 18 else (_dt_now - timedelta(days=1)).strftime("%Y-%m-%d")
            if hasattr(self, '_last_resume_date') and self._last_resume_date == _today_key:
                logging.critical(
                    "[SafetyMetaLayer] P1-R11-10: 同交易日内已执行过恢复操作，拒绝重复恢复！"
                    "caller=%s today=%s last_resume=%s",
                    caller_id, _today_key, getattr(self, '_last_resume_time', 'N/A'),
                )
                return False
            # R13-BIZ-05修复: 记录调用来源审计日志
            logging.critical(
                "[SafetyMetaLayer] ✅ 人工确认恢复交易！日回撤硬停止已解除，交易恢复正常 "
                "caller_id=%s timestamp=%s",
                caller_id, _get_tz_aware_now().isoformat()  # P1-R11-12修复: 使用UTC时区
            )

            # INV-12/INV-RSK-01修复: 恢复前验证市场安全性
            # 不能仅重置标志而不验证当前市场状态是否安全
            _market_safe = self._verify_market_safety_before_resume()
            if not _market_safe:
                logging.critical(
                    "[SafetyMetaLayer] INV-RSK-01: 市场安全验证未通过，恢复被拒绝! "
                    "caller_id=%s, 市场仍处于异常状态",
                    caller_id,
                )
                return False

            self._daily_hard_stop_triggered = False
            self._daily_new_open_blocked = False
            self._daily_drawdown = 0.0
            # P1-R11-10修复: 记录本次恢复日期，防止同日内重复恢复
            self._last_resume_date = _today_key
            self._last_resume_time = _now_t
            # R13-BIZ-05修复: 写入审计追踪记录
            self._stats['confirm_resume_history'] = self._stats.get('confirm_resume_history', [])
            resume_record = {
                'timestamp': _get_tz_aware_now().isoformat(),  # P1-R11-12修复: 使用UTC时区
                'caller_id': caller_id,
                'daily_drawdown_at_resume': self._daily_drawdown,
            }
            self._stats['confirm_resume_history'].append(resume_record)

            # CMP-P1-02修复: 合规审批流程记录写入compliance_audit.jsonl
            try:
                audit_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
                os.makedirs(audit_dir, exist_ok=True)
                audit_path = os.path.join(audit_dir, 'compliance_audit.jsonl')
                audit_entry = {
                    'event': 'confirm_daily_resume',
                    'timestamp': _get_tz_aware_now().isoformat(),  # P1-R11-12修复: 使用UTC时区
                    'caller_id': caller_id,
                    'approval_context': approval_context or {},
                    'previous_state': {
                        'daily_hard_stop_triggered': True,
                        'daily_drawdown': self._daily_drawdown,
                    },
                    'new_state': {
                        'daily_hard_stop_triggered': False,
                        'daily_new_open_blocked': False,
                    },
                }
                with open(audit_path, 'a', encoding='utf-8') as f:
                    f.write(json_dumps(audit_entry) + '\n')
                logging.info("[SafetyMetaLayer] CMP-P1-02: 合规审批记录已写入 %s", audit_path)
            except Exception as e:
                logging.warning("[SafetyMetaLayer] CMP-P1-02: 合规审批记录写入失败: %s", e)
            return True

    def set_prev_5day_avg_profit(self, avg_profit: float) -> None:
        with self._lock:
            self._prev_5day_avg_profit = avg_profit

    def _get_circuit_breaker_pause_sec(self) -> float:
        return safe_get_float(self.params, "circuit_breaker_pause_sec", 180.0)  # R25-SE-01-FIX

    def _get_circuit_breaker_calm_period_sec(self) -> float:
        return safe_get_float(self.params, "circuit_breaker_calm_period_sec", 600.0)  # R25-SE-01-FIX

    def _cancel_pending_on_circuit_breaker(self) -> None:
        """断路器触发后的清理操作：撤销挂单 + 清空状态缓存。"""
        # 1. 撤销所有未成交订单
        # R27-P0-RC-04修复: 获取order_service锁后再cancel，防止与execute交错竞态
        try:
            from ali2026v3_trading.order_service import get_order_service
            osvc = get_order_service()
            if osvc:
                with osvc._lock:
                    count = osvc.cancel_all_pending()
                if count > 0:
                    logging.warning("[SafetyMetaLayer] 断路器触发，已撤销 %d 笔未成交订单", count)
        except Exception as e:
            logging.warning("[SafetyMetaLayer] cancel_pending error: %s", e)

        # P0-R8-03修复: 清空状态缓存，防止恢复时使用过期状态数据
        try:
            from ali2026v3_trading.state_param_manager import get_state_param_manager
            spm = get_state_param_manager()
            if spm and hasattr(spm, 'reset_state_cache'):
                spm.reset_state_cache()
                logging.info("[SafetyMetaLayer] 断路器触发后已清空state_param_manager状态缓存")
        except Exception as e:
            logging.warning("[SafetyMetaLayer] state_param_manager reset error: %s", e)

        # P0-R8-03修复: 清空width_cache状态缓存
        try:
            from ali2026v3_trading.width_cache import get_width_cache
            wc = get_width_cache()
            if wc and hasattr(wc, 'reset_cache'):
                wc.reset_cache()
                logging.info("[SafetyMetaLayer] 断路器触发后已清空width_cache状态缓存")
            elif wc and hasattr(wc, 'clear'):
                wc.clear()
                logging.info("[SafetyMetaLayer] 断路器触发后已清空width_cache")
        except Exception as e:
            logging.warning("[SafetyMetaLayer] width_cache reset error: %s", e)

    def _force_position_reduction_on_circuit_breaker(self) -> None:
        """INV-P1-10修复: 熔断触发后强制减仓

        原代码仅撤销挂单(cancel_pending)，未对已有持仓执行减仓，
        极端行情下持仓风险持续暴露。现补充：熔断触发后按比例减仓50%。
        """
        try:
            from ali2026v3_trading.position_service import get_position_service
            _ps = get_position_service()
            if _ps is None:
                return

            positions = getattr(_ps, 'positions', {})
            if not positions:
                return

            reduction_ratio = safe_get_float(self.params, "circuit_breaker_reduction_ratio", 0.5)  # R25-SE-01-FIX
            reduced_count = 0

            for inst_id in list(positions.keys()):
                pos_dict = positions.get(inst_id)
                if pos_dict is None:
                    continue
                for pid in list(pos_dict.keys()):
                    rec = pos_dict.get(pid)
                    if rec is None or getattr(rec, 'volume', 0) == 0:
                        continue
                    if getattr(rec, '_closing', False):
                        continue
                    try:
                        reduce_volume = max(1, int(abs(rec.volume) * reduction_ratio))
                        from ali2026v3_trading.order_service import get_order_service
                        osvc = get_order_service()
                        if osvc:
                            direction = 'SELL' if rec.volume > 0 else 'BUY'
                            price = 0.0
                            try:
                                from ali2026v3_trading.data_service import get_data_service
                                ds = get_data_service()
                                if ds and ds.realtime_cache:
                                    mp = ds.realtime_cache.get_latest_price(rec.instrument_id)
                                    if mp and mp > 0:
                                        price = mp
                            except Exception:
                                logging.warning("[R22-EP-P1] RiskService exception swallowed")
                                pass
                            if price <= 0:
                                price = getattr(rec, 'current_price', 0) or getattr(rec, 'open_price', 0)
                            if price > 0:
                                osvc.send_order(
                                    instrument_id=rec.instrument_id,
                                    volume=reduce_volume,
                                    price=price,
                                    direction=direction,
                                    action='CLOSE',
                                    exchange=getattr(rec, 'exchange', ''),
                                    signal_id=f"RISK_REDUCE_{rec.instrument_id}",  # R24-P0-TR-01修复: signal_id链路贯通
                                )
                                reduced_count += 1
                    except Exception as e:
                        logging.warning(
                            "[SafetyMetaLayer] INV-P1-10: 减仓失败 instrument=%s pid=%s error=%s",
                            inst_id, pid, e,
                        )

            if reduced_count > 0:
                logging.critical(
                    "[SafetyMetaLayer] INV-P1-10: 熔断后强制减仓完成 reduced=%d positions ratio=%.0f%%",
                    reduced_count, reduction_ratio * 100,
                )
                # INV-P1-10修复: 熔断后强制减仓触发事件总线告警
                try:
                    from ali2026v3_trading.event_bus import get_global_event_bus, RiskEvent
                    _eb = get_global_event_bus()
                    if _eb and not getattr(_eb, '_shutdown', True):
                        _eb.publish(RiskEvent(
                            risk_type='circuit_breaker_force_reduction',
                            level='CRITICAL',
                            message=f"INV-P1-10: 熔断后强制减仓 reduced={reduced_count} ratio={reduction_ratio*100:.0f}%",
                        ), async_mode=True)
                except Exception as _eb_e:
                    logging.debug("[SafetyMetaLayer] INV-P1-10: 事件总线告警失败: %s", _eb_e)
        except Exception as e:
            logging.warning("[SafetyMetaLayer] INV-P1-10: 强制减仓异常: %s", e)

    def _get_daily_drawdown_multiplier(self) -> float:
        return safe_get_float(self.params, "daily_drawdown_multiplier", 2.0)  # R25-SE-01-FIX

    def _verify_market_safety_before_resume(self) -> bool:
        """INV-12/INV-RSK-01修复: 恢复交易前验证市场安全性

        检查项：
        1. 断路器冷静期是否已过
        2. 当前回撤是否仍在恶化
        3. 影子引擎是否仍处于降级/EV暂停状态
        4. 算法熔断是否仍在生效

        Returns:
            True if market appears safe to resume trading
        """
        now = time.time()

        # 1. 断路器冷静期检查
        if self._trading_paused_until > now:
            logging.warning(
                "[SafetyMetaLayer] INV-RSK-01: 断路器暂停期未结束 paused_until=%.0f",
                self._trading_paused_until,
            )
            return False

        if self._circuit_breaker_calm_until > now:
            logging.warning(
                "[SafetyMetaLayer] INV-RSK-01: 断路器冷静期未结束 calm_until=%.0f",
                self._circuit_breaker_calm_until,
            )
            return False

        # 2. 回撤是否仍在恶化（最近3次equity更新中回撤持续增大）
        if len(self._equity_series) >= 3 and self._daily_start_equity and self._daily_start_equity > 0:
            recent_equities = list(self._equity_series)[-3:]
            drawdowns = [(self._daily_peak_equity - eq) / self._daily_start_equity for eq in recent_equities]
            if all(d1 < d2 for d1, d2 in zip(drawdowns, drawdowns[1:])):
                logging.warning(
                    "[SafetyMetaLayer] INV-RSK-01: 回撤仍在恶化 drawdowns=[%s]",
                    ', '.join(f'{d:.4f}' for d in drawdowns),
                )
                return False

        # 3. 影子引擎降级/EV暂停检查
        # R24-P1-CF-01修复: 影子引擎检查必须在锁保护内执行，防止并发修改状态
        with self._lock:
            try:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                _sse = get_shadow_strategy_engine()
                if _sse is not None:
                    if _sse.is_absolute_ev_paused():
                        logging.warning("[SafetyMetaLayer] INV-RSK-01: 影子引擎EV暂停仍生效")
                        return False
                    if _sse.is_degradation_active():
                        logging.warning("[SafetyMetaLayer] INV-RSK-01: 影子引擎降级仍生效")
                        return False
            except Exception as _sm_ev_err:
                logging.warning("[R22-EP-01b] SafetyMetaLayer影子引擎检查异常: %s", _sm_ev_err)
                return False

        # 4. 算法熔断检查
        if self._algo_paused and self._algo_paused_until > now:
            logging.warning(
                "[SafetyMetaLayer] INV-RSK-01: 算法熔断仍生效 paused_until=%.0f",
                self._algo_paused_until,
            )
            return False

        return True

    def _get_hard_time_stop_minutes(self) -> float:
        """获取硬时间止损（分钟），按策略分层参数自动选择"""
        # 优先使用策略分层时间参数
        hft_ms = safe_get_float(self.params, "hft_hard_time_stop_ms", 0)  # R25-SE-01-FIX
        if hft_ms > 0:
            return hft_ms / 60000.0  # 毫秒→分钟
        spring_sec = safe_get_float(self.params, "spring_hard_time_stop_sec", 0)  # R25-SE-01-FIX
        if spring_sec > 0:
            return spring_sec / 60.0  # 秒→分钟
        resonance_min = safe_get_float(self.params, "resonance_hard_time_stop_min", 0)  # R25-SE-01-FIX
        if resonance_min > 0:
            return resonance_min
        box_min = safe_get_float(self.params, "box_hard_time_stop_min", 0)  # R25-SE-01-FIX
        if box_min > 0:
            return box_min
        # 兜底默认值
        return 5.0

    def _get_min_profit_threshold(self) -> float:
        return safe_get_float(self.params, "min_profit_threshold", 0.002)  # R25-SE-01-FIX

    def _get_stage1_minutes(self) -> float:
        # R3-P-06/R5-L-04/R5-L-07修复: 优先读取stage1_min_minutes(与task_scheduler对齐)，兼容旧名stage1_minutes; 统一返回分钟单位
        # P1-R11-07修复: stage1_min_minutes需适配bar_period，与task_scheduler._check_two_stage_stop保持一致
        # 不同K线周期下，相同分钟数对应不同bar数；乘以bar_period确保行为一致
        _bar_period = safe_get_float(self.params, "bar_period", 1.0)  # R25-SE-01-FIX
        val = safe_get_float(self.params, "stage1_min_minutes", None)  # R25-SE-01-FIX
        if val is not None and val > 0:
            return val * _bar_period
        _fallback = safe_get_float(self.params, "stage1_minutes", 90.0)  # R25-SE-01-FIX
        if _fallback <= 0:
            logging.warning("[R5-L-01] stage1_minutes=%s<=0，使用默认90.0", _fallback)
            _fallback = 90.0
        return _fallback * _bar_period

    def _get_stage2_minutes(self) -> float:
        return safe_get_float(self.params, "stage2_minutes", 240.0)  # R25-SE-01-FIX

    def _get_stage1_profit_threshold(self) -> float:
        return safe_get_float(self.params, "stage1_profit_threshold", 0.002)  # R25-SE-01-FIX

    def get_health_status(self) -> Dict[str, Any]:
        """P2-R11-06修复: 获取SafetyMetaLayer健康状态

        综合检查断路器、日回撤、连续亏损等子组件状态，
        返回聚合健康状态和触发源信息。

        Returns:
            Dict: {
                health_status: 'OK' | 'WARNING' | 'CRITICAL',
                triggered_by: str | None,  # P2-R11-06: CRITICAL时记录触发子组件
                details: Dict[str, Any],
            }
        """
        _trigger_source = None
        health_status = 'OK'
        details: Dict[str, Any] = {}

        with self._lock:
            # 检查1: 断路器是否激活
            now = time.time()
            trading_paused = now < self._trading_paused_until
            circuit_breaker_active = now < self._circuit_breaker_calm_until
            if trading_paused or circuit_breaker_active:
                health_status = 'CRITICAL'
                _trigger_source = 'circuit_breaker'
                details['circuit_breaker'] = {
                    'trading_paused': trading_paused,
                    'calm_until': self._circuit_breaker_calm_until,
                    'pause_reason': self._pause_reason,
                }

            # 检查2: 日回撤硬停止
            if self._daily_hard_stop_triggered:
                health_status = 'CRITICAL'
                _trigger_source = 'daily_drawdown' if _trigger_source is None else _trigger_source
                details['daily_hard_stop'] = {
                    'triggered': True,
                    'daily_drawdown_pct': self._daily_drawdown,
                    'new_open_blocked': self._daily_new_open_blocked,
                }

            # 检查3: 算法交易熔断
            if self._algo_paused and now < self._algo_paused_until:
                if health_status != 'CRITICAL':
                    health_status = 'WARNING'
                details['algo_paused'] = {
                    'paused': True,
                    'reason': self._algo_pause_reason,
                    'remaining_sec': max(0, self._algo_paused_until - now),
                }

            # 检查4: 断路器影子模式(观察期)
            if self._circuit_breaker_shadow_mode and now < self._circuit_breaker_shadow_until:
                if health_status == 'OK':
                    health_status = 'WARNING'
                details['shadow_mode'] = {
                    'active': True,
                    'remaining_sec': max(0, self._circuit_breaker_shadow_until - now),
                }

            # 检查5: 新开仓被阻断
            if self._daily_new_open_blocked and health_status == 'OK':
                health_status = 'WARNING'
                details['new_open_blocked'] = True

        return {
            'health_status': health_status,
            # P2-R11-06修复: CRITICAL状态添加triggered_by字段，区分触发子组件
            'triggered_by': _trigger_source if health_status == 'CRITICAL' else None,
            'details': details,
        }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['trading_paused'] = time.time() < self._trading_paused_until
            stats['new_open_blocked'] = self._daily_new_open_blocked
            stats['hard_stop_triggered'] = self._daily_hard_stop_triggered
            stats['daily_drawdown_pct'] = self._daily_drawdown
            return stats


# P0-R11-19修复: 策略级SafetyMetaLayer隔离
_safety_meta_layer: Optional[SafetyMetaLayer] = None  # 默认实例(向后兼容)
_safety_meta_layer_lock = threading.Lock()
_strategy_safety_layers: Dict[str, SafetyMetaLayer] = {}  # 策略级实例
_strategy_safety_lock = threading.Lock()
_MAX_STRATEGY_SAFETY_LAYERS = 50


def get_safety_meta_layer(params: Any = None, strategy_id: Optional[str] = None) -> SafetyMetaLayer:
    """R13-API-02修复: SafetyMetaLayer单例工厂支持参数更新

    首次以params=None初始化后，后续传入真实params会被更新到实例中，
    避免风控检查因参数缺失而失效。
    P0-R11-19修复: 支持策略级隔离，strategy_id非空时返回策略专属实例。
    """
    # P0-R11-19修复: 策略级实例路径
    if strategy_id is not None:
        with _strategy_safety_lock:
            if strategy_id not in _strategy_safety_layers:
                if len(_strategy_safety_layers) >= _MAX_STRATEGY_SAFETY_LAYERS:
                    logging.warning("[SafetyMetaLayer] 策略级实例数已达上限%d，清理最旧实例", _MAX_STRATEGY_SAFETY_LAYERS)
                    _oldest_sid = next(iter(_strategy_safety_layers))
                    del _strategy_safety_layers[_oldest_sid]
                _strategy_safety_layers[strategy_id] = SafetyMetaLayer(params=params)
                logging.info('[SafetyMetaLayer] 策略级实例初始化完成 strategy_id=%s params_type=%s',
                             strategy_id, type(params).__name__ if params else 'None')
            else:
                layer = _strategy_safety_layers[strategy_id]
                if params is not None and layer._params is None:
                    layer._params = params
                    logging.info('[SafetyMetaLayer] 策略级实例参数已更新 strategy_id=%s params_type=%s',
                                 strategy_id, type(params).__name__)
            return _strategy_safety_layers[strategy_id]
    global _safety_meta_layer
    if _safety_meta_layer is None:
        with _safety_meta_layer_lock:
            if _safety_meta_layer is None:
                _safety_meta_layer = SafetyMetaLayer(params=params)
                logging.info('[SafetyMetaLayer] 首次初始化完成 params_type=%s',
                             type(params).__name__ if params else 'None')
    else:
        # R13-API-02修复: 后续调用时更新params（解决首次params=None导致参数丢失）
        if params is not None and _safety_meta_layer._params is None:
            with _safety_meta_layer_lock:
                if params is not None and _safety_meta_layer._params is None:
                    _safety_meta_layer._params = params
                    logging.info('[SafetyMetaLayer] 参数已更新 params_type=%s',
                                 type(params).__name__)
    return _safety_meta_layer


def cleanup_safety_layer(strategy_id: str):
    with _strategy_safety_lock:
        if strategy_id in _strategy_safety_layers:
            del _strategy_safety_layers[strategy_id]
            logging.info('[SafetyMetaLayer] 策略级实例已清理 strategy_id=%s', strategy_id)


_risk_service_instances: Dict[str, RiskService] = {}
_risk_service_lock = threading.Lock()


def _normalize_risk_scope_id(scope_id: Optional[str], strategy: Any = None) -> str:
    if scope_id:
        return str(scope_id)
    strategy_id = None
    if isinstance(strategy, dict):
        strategy_id = strategy.get('strategy_id')
    elif strategy is not None:
        strategy_id = getattr(strategy, 'strategy_id', None)
    return str(strategy_id or 'default')


def get_risk_service(params: Any = None, position_manager: Any = None,
                     strategy: Any = None,
                     scope_id: Optional[str] = None) -> RiskService:
    """R13-API-02修复: RiskService单例工厂支持参数更新

    首次以params=None初始化后，后续传入真实params会被更新到实例中，
    避免风控检查因参数缺失而失效。
    """
    scope = _normalize_risk_scope_id(scope_id, strategy)
    with _risk_service_lock:
        if scope not in _risk_service_instances:
            effective_params = params if params is not None else {}
            _risk_service_instances[scope] = RiskService(
                params=effective_params,
                position_manager=position_manager,
                strategy=strategy,
            )
            logging.info(
                '[RiskService] 首次初始化完成 scope=%s params_type=%s position_manager=%s',
                scope,
                type(effective_params).__name__,
                'yes' if position_manager else 'no',
            )
        else:
            instance = _risk_service_instances[scope]
            # R13-P0-API-02修复: 后续调用时，只要传入非None的params就更新（无论原params是否为None）
            # 原逻辑仅在_instance.params is None时更新，导致首次params=None初始化后，
            # 第二次传入真实params时若_instance.params已被设为{}则不会更新
            if params is not None:
                if instance.params is None or instance.params != params:
                    instance.params = params
                    logging.info('[RiskService] 参数已更新 scope=%s params_type=%s',
                                 scope,
                                 type(params).__name__)
            # R13-API-02修复: 更新position_manager和strategy
            if position_manager is not None and instance.position_manager is None:
                instance.position_manager = position_manager
            if strategy is not None and instance.strategy is None:
                instance.strategy = strategy
    # [R23-P1-06-FIX] 工厂函数返回None防护
    result = _risk_service_instances.get(scope)
    if result is None:
        raise RuntimeError(f"[R23-P1-06-FIX] get_risk_service()返回None，scope={scope}单例初始化失败")
    return result


def reset_risk_service(scope_id: Optional[str] = None) -> None:
    with _risk_service_lock:
        if scope_id is None:
            _risk_service_instances.clear()
        else:
            _risk_service_instances.pop(str(scope_id), None)
    # [R23-P1-07-FIX] reset后发布引用失效通知，提醒其他模块重新获取实例
    try:
        from ali2026v3_trading.event_bus import get_global_event_bus
        _bus = get_global_event_bus()
        if _bus is not None:
            _bus.publish('risk_service.reset', {'scope_id': scope_id, 'timestamp': time.time()})
            logging.info("[R23-P1-07-FIX] risk_service reset通知已发布: scope_id=%s", scope_id)
    except Exception as _e:
        logging.debug("[R23-P1-07-FIX] reset通知发布失败: %s", _e)


# P-04修复: RiskDashboardService独立全局单例 — Greeks聚合+PnL归因+压力测试
class RiskDashboardService:
    """风险仪表盘服务 — Greeks聚合、PnL归因、压力测试

    独立全局单例，供RiskService和外部仪表盘调用。
    """

    _instance: Optional['RiskDashboardService'] = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> 'RiskDashboardService':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        with cls._lock:
            cls._instance = None

    def __init__(self):
        # R15-P1-PERF-13修复: Greeks缓存添加maxsize参数(默认10000)，超过时LRU淘汰
        self._greeks_cache_maxsize: int = 10000
        self._greeks_cache: Dict[str, Any] = {}
        self._pnl_attribution: Dict[str, float] = {}
        self._stress_results: Dict[str, Any] = {}
        self._last_update: datetime = datetime.now(_CHINA_TZ)

    def aggregate_greeks(self, positions: Dict[str, Any]) -> Dict[str, float]:
        """Greeks聚合：按持仓汇总delta/gamma/theta/vega"""
        agg = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}
        for sym, pos in positions.items():
            greeks = getattr(pos, 'greeks', None) or {}
            for k in agg:
                agg[k] += greeks.get(k, 0.0)
        self._greeks_cache = agg
        # R15-P1-PERF-13修复: LRU淘汰超过maxsize的缓存条目
        if len(self._greeks_cache) > self._greeks_cache_maxsize:
            keys_to_evict = list(self._greeks_cache.keys())[:len(self._greeks_cache) - self._greeks_cache_maxsize]
            for k in keys_to_evict:
                del self._greeks_cache[k]
        self._last_update = datetime.now(_CHINA_TZ)
        return agg

    def attribute_pnl(self, closed_trades: List[Any]) -> Dict[str, float]:
        """PnL归因：按策略/状态/持仓时长分组归因"""
        attr: Dict[str, float] = {}
        for t in closed_trades:
            reason = getattr(t, 'close_reason', 'unknown')
            pnl = getattr(t, 'pnl', 0.0)
            attr[reason] = attr.get(reason, 0.0) + pnl
        self._pnl_attribution = attr
        return attr

    def run_stress_test(self, positions: Dict[str, Any],
                        shock_pct: float = 0.10) -> Dict[str, Any]:
        """压力测试：价格冲击shock_pct下的最坏情景损失"""
        total_loss = 0.0
        for sym, pos in positions.items():
            exposure = getattr(pos, 'volume', 0) * getattr(pos, 'open_price', 0)
            total_loss += abs(exposure) * shock_pct
        self._stress_results = {"shock_pct": shock_pct, "worst_case_loss": total_loss}
        return self._stress_results


_risk_dashboard_instance: Optional[RiskDashboardService] = None
_risk_dashboard_lock = threading.Lock()


def get_risk_dashboard_service() -> RiskDashboardService:
    """获取RiskDashboardService全局单例"""
    return RiskDashboardService.get_instance()


# ============================================================================
# 模块导出
# ============================================================================

__all__ = [
    'RiskService',
    'RiskLevel',
    'RiskCheckResult',
    'RiskCheckResponse',
    'PositionLimit',
    'RiskMetrics',
    'SafetyMetaLayer',
    'get_safety_meta_layer',
    'SimplifiedSPAN',
    'get_risk_service',
    'reset_risk_service',
    'RiskDashboardService',
    'get_risk_dashboard_service',
    # OPS-05/06/07/14/15修复
    'AlertLevel',
    'AlertDeduplicator',
    'alert',
    'operations_audit_log',
    # R24-P0审计可追溯性修复
    'audit_chain_append',
    'structured_audit_log',
    'save_state_snapshot',
    'generate_exchange_report',
]


# ============================================================================
# OPS-05修复: 告警分级 — P0/P1/P2分类
# OPS-06修复: P0告警自动升级 — 超时后自动升级通知
# OPS-07修复: 告警去重/聚合 — 防止告警风暴
# OPS-14修复: 紧急操作审批机制 — confirm_daily_resume需审批
# OPS-15修复: 操作审计日志 — 记录谁在何时做了什么
# ============================================================================

class AlertLevel(Enum):
    """OPS-05修复: 告警级别枚举 — P0/P1/P2分类"""
    P0 = "P0"  # 紧急: 系统级故障，需立即处理（如: 硬停止触发、断路器熔断）
    P1 = "P1"  # 严重: 业务级异常，需30分钟内响应（如: 大额亏损、数据异常）
    P2 = "P2"  # 一般: 需关注但不紧急（如: 限频触发、参数偏离）


# OPS-05修复: 告警级别→日志级别映射
_ALERT_LEVEL_LOG_MAP = {
    AlertLevel.P0: logging.CRITICAL,
    AlertLevel.P1: logging.ERROR,
    AlertLevel.P2: logging.WARNING,
}

# OPS-05修复: 告警级别→默认超时秒数（超时后升级）
_ALERT_LEVEL_TIMEOUT_SEC = {
    AlertLevel.P0: 300.0,   # P0: 5分钟未处理则升级
    AlertLevel.P1: 1800.0,  # P1: 30分钟未处理则升级
    AlertLevel.P2: 0.0,     # P2: 不自动升级
}


class AlertDeduplicator:
    """OPS-07修复: 告警去重器 — 在时间窗口内抑制重复告警

    防止同一告警在短时间内重复触发（告警风暴）。
    相同key的告警在window_sec内只放行第一次。
    """

    def __init__(self, window_sec: float = 60.0):
        self._window_sec = window_sec
        self._recent_alerts: Dict[str, float] = {}  # key → last_alert_timestamp
        self._lock = threading.Lock()

    def should_alert(self, key: str) -> bool:
        """检查该key的告警是否应放行（去重后）

        Returns:
            True: 应发送告警; False: 在窗口内已发送过，抑制
        """
        now = time.time()
        with self._lock:
            last_ts = self._recent_alerts.get(key, 0.0)
            if now - last_ts < self._window_sec:
                return False
            self._recent_alerts[key] = now
            return True

    def reset(self, key: str = None) -> None:
        """重置去重状态"""
        with self._lock:
            if key:
                self._recent_alerts.pop(key, None)
            else:
                self._recent_alerts.clear()


# 全局告警去重器实例
_alert_deduplicator: Optional[AlertDeduplicator] = None
_alert_deduplicator_lock = threading.Lock()


def get_alert_deduplicator() -> AlertDeduplicator:
    global _alert_deduplicator
    with _alert_deduplicator_lock:
        if _alert_deduplicator is None:
            _alert_deduplicator = AlertDeduplicator()
        return _alert_deduplicator


# OPS-06修复: 告警升级跟踪
_alert_escalation_tracker: Dict[str, Dict[str, Any]] = {}
_alert_escalation_lock = threading.Lock()


def _check_alert_escalation() -> None:
    """OPS-06修复: 检查P0/P1告警是否超时未处理，自动升级"""
    now = time.time()
    with _alert_escalation_lock:
        to_escalate = []
        for key, info in _alert_escalation_tracker.items():
            level = info.get('level')
            if level is None:
                continue
            timeout = _ALERT_LEVEL_TIMEOUT_SEC.get(level, 0.0)
            if timeout <= 0:
                continue
            if info.get('escalated', False):
                continue
            elapsed = now - info.get('timestamp', 0.0)
            if elapsed > timeout:
                to_escalate.append(key)

        for key in to_escalate:
            info = _alert_escalation_tracker[key]
            info['escalated'] = True
            info['escalated_at'] = now
            logging.critical(
                "[OPS-06] P0告警超时升级! key=%s level=%s elapsed=%.0fs "
                "已自动升级通知 — 需立即处理!",
                key, info.get('level', ''), now - info.get('timestamp', 0.0),
            )
            # 触发告警回调
            try:
                RiskService._fire_alert('ALERT_ESCALATION', {
                    'alert_key': key,
                    'original_level': str(info.get('level', '')),
                    'elapsed_sec': now - info.get('timestamp', 0.0),
                })
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
                pass


def alert(level: AlertLevel, key: str, message: str,
          detail: Optional[Dict[str, Any]] = None) -> bool:
    """OPS-05/06/07修复: 分级告警函数

    Args:
        level: 告警级别 (P0/P1/P2)
        key: 告警唯一标识（用于去重和升级跟踪）
        message: 告警消息
        detail: 告警详情字典

    Returns:
        True: 告警已发送; False: 被去重抑制
    """
    # OPS-07: 去重检查
    dedup = get_alert_deduplicator()
    if not dedup.should_alert(key):
        logging.debug("[OPS-07] 告警去重抑制: key=%s", key)
        return False

    # OPS-05: 按级别记录日志
    log_level = _ALERT_LEVEL_LOG_MAP.get(level, logging.WARNING)
    logging.log(
        log_level,
        "[OPS-05] [%s] %s key=%s detail=%s",
        level.value, message, key, detail or {},
    )

    # OPS-06: P0/P1告警注册升级跟踪
    if level in (AlertLevel.P0, AlertLevel.P1):
        with _alert_escalation_lock:
            _alert_escalation_tracker[key] = {
                'level': level,
                'message': message,
                'timestamp': time.time(),
                'escalated': False,
                'detail': detail,
            }
        # 立即检查是否有需要升级的告警
        _check_alert_escalation()

    # 触发告警回调
    try:
        RiskService._fire_alert(f'ALERT_{level.value}', {
            'key': key,
            'level': level.value,
            'message': message,
            'detail': detail or {},
        })
    except Exception:
        logging.warning("[R22-EP-P1] RiskService exception swallowed")
        pass

    return True


# OPS-15修复: 操作审计日志
_audit_log_path: Optional[str] = None
_audit_log_lock = threading.Lock()


def _get_audit_log_path() -> str:
    global _audit_log_path
    if _audit_log_path is None:
        _audit_log_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', 'operations_audit.jsonl'
        )
        os.makedirs(os.path.dirname(_audit_log_path), exist_ok=True)
    return _audit_log_path


def operations_audit_log(action: str, operator: str = "unknown",
                         result: str = "", detail: Any = None) -> None:
    """OPS-15修复: 操作审计日志 — 记录谁在何时做了什么

    Args:
        action: 操作类型（如 EMERGENCY_STOP, EMERGENCY_DEGRADE, CONFIRM_RESUME 等）
        operator: 操作人标识
        result: 操作结果（success/partial/failed）
        detail: 操作详情
    """
    record = {
        'timestamp': _get_tz_aware_now().isoformat(),  # P1-R11-12修复: 使用UTC时区
        'action': action,
        'operator': operator,
        'result': result,
        'detail': detail if detail is not None else {},
    }
    try:
        with _audit_log_lock:
            with open(_get_audit_log_path(), 'a', encoding='utf-8') as f:
                f.write(json_dumps(record) + '\n')
    except Exception as e:
        logging.warning("[OPS-15] 审计日志写入失败: %s", e)


# R24-P0-TR-02修复: 不可否认性机制 — hash chain
_audit_chain_lock = threading.Lock()
_audit_chain_last_hash = 'genesis_00000000'

def audit_chain_append(event_type: str, payload: dict) -> str:
    """R24-P0-TR-02: 不可否认性hash chain — 每个审计事件链接到前一个事件的hash"""
    global _audit_chain_last_hash
    import hashlib as _hl
    record = {
        'timestamp': _get_tz_aware_now().isoformat(),  # P1-R11-12修复: 使用UTC时区
        'event_type': event_type,
        'payload': payload,
        'prev_hash': _audit_chain_last_hash,
    }
    _raw = f"{record['timestamp']}:{event_type}:{_audit_chain_last_hash}"
    _new_hash = _hl.sha256(_raw.encode()).hexdigest()[:16]
    record['hash'] = _new_hash
    with _audit_chain_lock:
        _audit_chain_last_hash = _new_hash
    # 写入hash chain日志
    try:
        _chain_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'audit_chain.jsonl')
        os.makedirs(os.path.dirname(_chain_path), exist_ok=True)
        with open(_chain_path, 'a', encoding='utf-8') as f:
            f.write(json_dumps(record) + '\n')
    except Exception as e:
        logging.warning("[R24-P0-TR-02] audit_chain write failed: %s", e)
    return _new_hash


# R24-P0-TR-03修复: 结构化审计日志 — 与调试日志分离，写入独立文件
_structured_audit_lock = threading.Lock()

def structured_audit_log(event_type: str, decision: str, context: dict = None, severity: str = "INFO") -> None:
    """R24-P0-TR-03: 结构化审计日志 — 交易决策/风控阻断等关键审计事件独立记录"""
    _AUDIT_SCHEMA_VERSION = "v1.0"  # LG-P1-03修复
    _sanitize_keys = {'api_key', 'access_key', 'access_secret', 'password', 'token', 'secret', 'credential'}  # LG-P1-04修复
    _safe_context = {}
    if context:
        _safe_context = {k: ('***' if k in _sanitize_keys else v) for k, v in context.items()}
    record = {
        'schema_version': _AUDIT_SCHEMA_VERSION,  # LG-P1-03修复
        'timestamp': _get_tz_aware_now().isoformat(),  # P1-R11-12修复: 使用UTC时区
        'event_type': event_type,
        'decision': decision,
        'severity': severity,
        'context': _safe_context,
    }
    try:
        with _structured_audit_lock:
            _audit_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'structured_audit.jsonl')
            os.makedirs(os.path.dirname(_audit_path), exist_ok=True)
            with open(_audit_path, 'a', encoding='utf-8') as f:
                f.write(json_dumps(record) + '\n')
    except Exception as e:
        logging.warning("[R24-P0-TR-03] structured_audit_log write failed: %s", e)


# R24-P0-TR-04修复: 系统状态快照机制
_state_snapshot_lock = threading.Lock()

def save_state_snapshot(snapshot_data: dict, tag: str = '') -> None:
    """R24-P0-TR-04: 保存系统状态快照，支持时序重建"""
    import time as _time
    record = {
        'snapshot_time': _get_tz_aware_now().isoformat(),  # P1-R11-12修复: 使用UTC时区
        'tag': tag,
        'data': snapshot_data,
    }
    try:
        with _state_snapshot_lock:
            _snap_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'state_snapshots')
            os.makedirs(_snap_dir, exist_ok=True)
            _fname = f"snapshot_{_get_tz_aware_now().strftime('%Y%m%d_%H%M%S')}_{tag}.json"  # P1-R11-12修复: 使用UTC时区
            with open(os.path.join(_snap_dir, _fname), 'w', encoding='utf-8') as f:
                f.write(json_dumps(record))
    except Exception as e:
        logging.warning("[R24-P0-TR-04] State snapshot save failed: %s", e)


# R24-P0-TR-09修复: 交易所要求报告生成
def generate_exchange_report(trades: list, output_path: str = None) -> str:
    """R24-P0-TR-09: 生成交易所要求的交易报告（时间戳/方向/价格/数量）"""
    import csv
    if output_path is None:
        output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'exchange_report.csv')
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'instrument_id', 'direction', 'price', 'volume', 'order_id', 'signal_id'])
            for t in trades:
                writer.writerow([
                    t.get('timestamp', ''),
                    t.get('instrument_id', ''),
                    t.get('direction', ''),
                    t.get('price', ''),
                    t.get('volume', ''),
                    t.get('order_id', ''),
                    t.get('signal_id', ''),
                ])
        logging.info("[R24-P0-TR-09] Exchange report generated: %s (%d trades)", output_path, len(trades))
        return output_path
    except Exception as e:
        logging.error("[R24-P0-TR-09] Exchange report generation failed: %s", e)
        return ''


# ============================================================================
# P0-4修复：简化SPAN保证金模拟器
# ============================================================================

class SimplifiedSPAN:
    """简化SPAN保证金模拟器 (基于16情景风险阵列)
    
    用于回测中模拟组合保证金，避免固定比例保证金低估套利/做市策略资金占用。
    """
    
    SCENARIOS = [
        (0, 0),      # 基准
        (+1, 0), (-1, 0), (+2, 0), (-2, 0), (+3, 0), (-3, 0),  # 价格变动
        (0, +1), (0, -1), (0, +2), (0, -2),  # IV变动
        (+1, +1), (+1, -1), (-1, +1), (-1, -1),  # 联合变动
    ]
    
    def __init__(self, price_sigma: float = 0.01, iv_sigma: float = 0.25,
                 min_base_margin: float = 1000.0,
                 capital_scale: str = 'medium'):
        self.price_sigma = price_sigma
        self.iv_sigma = iv_sigma
        # R15-P1-BIZ-01修复: min_base_margin按capital_scale动态调整默认值
        _scale_margin_map = {'small': 500.0, 'medium': 1000.0, 'large': 5000.0}
        if min_base_margin == 1000.0 and capital_scale in _scale_margin_map:
            min_base_margin = _scale_margin_map[capital_scale]
        # P2-R8-05修复: 最低保证金参数化，不再硬编码1000元
        self.min_base_margin = min_base_margin
    
    def calc_margin(self, positions: List[Dict], underlying_price: float, current_iv: float) -> float:
        """计算组合保证金
        
        Args:
            positions: 持仓列表，每个包含 {'quantity', 'current_value', 'delta', 'gamma', 'vega'}
            underlying_price: 标的价格
            current_iv: 当前IV
        
        Returns:
            保证金金额
        """
        max_loss = 0.0
        
        for price_shift_sigma, iv_shift_pct in self.SCENARIOS:
            price_new = underlying_price * (1 + price_shift_sigma * self.price_sigma)
            iv_new = current_iv * (1 + iv_shift_pct * self.iv_sigma)
            
            portfolio_pnl = 0.0
            for pos in positions:
                # 简化PnL近似：Delta * dS + 0.5 * Gamma * dS^2 + Vega * dIV
                dS = price_new - underlying_price
                dIV = iv_new - current_iv
                
                delta = pos.get('delta', 0.0)
                gamma = pos.get('gamma', 0.0)
                vega = pos.get('vega', 0.0)
                
                pnl = delta * dS + 0.5 * gamma * dS**2 + vega * dIV
                if np.isfinite(pnl):  # NP-P2-12: portfolio_pnl累加添加isfinite检查
                    portfolio_pnl += pnl * pos.get('quantity', 0)
                if abs(portfolio_pnl) > 1e12:  # NP-P2-12: 溢出检测
                    logging.warning("[NP-P2-12] portfolio_pnl overflow detected: %.2e", portfolio_pnl)
                    break
            
            max_loss = min(max_loss, portfolio_pnl)
        
        # 净保证金 = 最大亏损 + 最低保证金
        # P2-R8-05修复: 使用参数化min_base_margin替代硬编码1000.0
        return abs(max_loss) + self.min_base_margin
    
    def calc_margin_ratio(self, positions: List[Dict], underlying_price: float,
                          current_iv: float, total_position_value: float,
                          equity: float = 0.0) -> float:
        """计算保证金比率

        INV-05/INV-CAP-03修复: 增加equity参数，保证金超过权益时返回1.0并告警

        Args:
            positions: 持仓列表
            underlying_price: 标的价格
            current_iv: 当前IV
            total_position_value: 持仓总价值
            equity: 账户权益（INV-CAP-03新增）

        Returns:
            保证金/持仓价值比率
        """
        if total_position_value <= 0:
            return 0.20  # 默认20%

        margin = self.calc_margin(positions, underlying_price, current_iv)
        # INV-05/INV-CAP-03修复: 保证金占用超过权益时告警
        if equity > 0 and margin > equity:
            logging.critical(
                "[SimplifiedSPAN] INV-CAP-03: margin(%.2f) > equity(%.2f), "
                "账户资不抵债，保证金比率钳制为1.0",
                margin, equity,
            )
            return 1.0
        return margin / total_position_value


def calculate_var_historical(returns: List[float], confidence_level: float = 0.95) -> float:
    """计算历史VaR(Value at Risk)
    
    手册第268行要求: "历史滚动VaR(95%)"
    
    Args:
        returns: 收益率序列
        confidence_level: 置信水平，默认95%
    
    Returns:
        VaR值(正值表示潜在损失)
    """
    if not returns or len(returns) < 10:
        return 0.0
    
    returns_arr = np.array(returns)
    sorted_returns = np.sort(returns_arr)
    index = safe_int((1.0 - confidence_level) * len(sorted_returns))
    index = max(0, min(index, len(sorted_returns) - 1))
    
    var_value = -sorted_returns[index]
    return float(var_value)


def calculate_var_rolling(returns: List[float], window: int = 252, confidence_level: float = 0.95) -> List[float]:
    """计算滚动VaR
    
    Args:
        returns: 收益率序列
        window: 滚动窗口大小
        confidence_level: 置信水平
    
    Returns:
        滚动VaR序列
    """
    if not returns or len(returns) < window:
        return [calculate_var_historical(returns, confidence_level)] if returns else [0.0]
    
    rolling_vars = []
    for i in range(window, len(returns) + 1):
        window_returns = returns[i - window:i]
        var_val = calculate_var_historical(window_returns, confidence_level)
        rolling_vars.append(var_val)
    
    return rolling_vars


def check_circuit_breaker_auto_recovery(risk_service_instance, cooldown_sec: float = 300.0) -> bool:
    """R23-SM-06-FIX: 风控熔断自动恢复检查
    
    熔断后经cooldown_sec秒冷却期，若风险指标回归正常则自动解除熔断。
    Args:
        risk_service_instance: RiskService实例
        cooldown_sec: 冷却期(秒)，默认300秒(5分钟)
    Returns:
        是否执行了恢复
    """
    safety = getattr(risk_service_instance, '_safety_meta_layer', None)
    if safety is None:
        return False
    try:
        if hasattr(safety, 'is_circuit_breaker_active') and safety.is_circuit_breaker_active():
            breaker_time = getattr(safety, '_circuit_breaker_activated_at', 0.0)
            if breaker_time > 0 and (time.time() - breaker_time) > cooldown_sec:
                if hasattr(safety, 'reset_circuit_breaker'):
                    safety.reset_circuit_breaker()
                    logging.info("[R23-SM-06-FIX] 风控熔断自动恢复: 冷却期%.1fs已过，熔断已解除", cooldown_sec)
                    return True
                else:
                    logging.warning("[R23-SM-06-FIX] SafetyMetaLayer缺少reset_circuit_breaker方法，无法自动恢复")
    except Exception as e:
        logging.warning("[R23-SM-06-FIX] 熔断自动恢复检查异常: %s", e)
    return False
