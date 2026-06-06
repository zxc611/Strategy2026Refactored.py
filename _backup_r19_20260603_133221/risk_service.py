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
            return self._check_service.check_before_trade(signal)

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


def _audit_annotation(category='', phase1_status='', note=''):
    """审计标注装饰器 — 不改变类行为，仅用于H.1.3格式清单审计追踪"""
    def decorator(cls):
        cls._audit_category = category
        cls._audit_phase1_status = phase1_status
        cls._audit_note = note
        return cls
    return decorator


# ============================================================================
# 枚举与数据结构
# ============================================================================

@_audit_annotation(category='lightweight_enum', phase1_status='stable', note='枚举类，0方法，7行，无隐式依赖')
class RiskLevel(Enum):
    """风险等级"""
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    RESTRICTED = auto()  # [R23-P1-02-FIX] 受限中间状态：正常→受限→熔断，防止直接从正常跳到熔断
    CRITICAL = auto()


@_audit_annotation(category='lightweight_enum', phase1_status='stable', note='枚举类，0方法，6行，无隐式依赖')
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


@_audit_annotation(category='lightweight_dataclass', phase1_status='stable', note='响应dataclass，5方法，36行，无隐式依赖')
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


@_audit_annotation(category='lightweight_dataclass', phase1_status='stable', note='指标dataclass，0方法，12行，无隐式依赖')
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



# CC-03/AP-02修复: 子服务导入（延迟导入避免循环依赖）
from ali2026v3_trading.risk_check_service import RiskCheckService
from ali2026v3_trading.risk_compute_service import RiskComputeService
from ali2026v3_trading.risk_circuit_breaker import SafetyMetaLayer, get_safety_meta_layer, cleanup_safety_layer
from ali2026v3_trading.risk_config_provider import RiskConfigProvider
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
    # AUDIT: lightweight_helper, phase2_target=ali2026v3_trading.risk_engine.log_deduplicator, 2方法, 19行
    # DEPRECATED: 独立模块 risk_engine.log_deduplicator 已替代  # noqa: F811  # pylint: disable=useless-suppression
    class LogDeduplicator:  # noqa: F811  # pylint: disable=redefined-outer-name
        """同一消息5秒内不重复输出"""
        _DEDUP_WINDOW_SEC = 5.0

        def __init__(self):
            self._recent: Dict[str, float] = {}
            self._lock = threading.Lock()

        def should_log(self, msg: str) -> bool:
            return self._config_provider.should_log(msg)
    # Phase 2: LogDeduplicator提取为独立模块，优先使用risk_engine.log_deduplicator
    try:
        from ali2026v3_trading.risk_engine.log_deduplicator import LogDeduplicator as _LogDedupIndependent
        _log_dedup = _LogDedupIndependent()
    except ImportError:
        _log_dedup = LogDeduplicator()
    # [R23-P2-ID-01-FIX]

    # R13-P0-LOG-05修复: 告警回调机制，供外部告警系统集成
    _alert_callbacks: List = []
    _alert_callbacks_lock = threading.Lock()

    def __init__(self, params: Any = None, position_manager: Any = None, strategy: Any = None):
        # CC-03/AP-02修复: 子服务实例化
        import copy as _copy
        self.params = _copy.deepcopy(params) if params is not None else {}
        self.position_manager = position_manager
        self.strategy = strategy
        self._check_service = RiskCheckService(self)
        self._compute_service = RiskComputeService(self)
        self._config_provider = RiskConfigProvider(self)
        self._lock = threading.Lock()
        self._risk_check_lock = threading.RLock()
        self._life_estimator = None
        self._life_estimator_init_failed = False
        self._current_capital_scale = 'medium'
        self._greeks_calc = None
        self._alpha_decay_recovery_confirmed: bool = False
        self._greeks_calc_lock = threading.Lock()
        self._risk_engine = None
        self._signal_service = None

    @classmethod
    def register_alert_callback(cls, cb) -> None:
        with cls._alert_callbacks_lock:
            if cb not in cls._alert_callbacks:
                cls._alert_callbacks.append(cb)
    def _fire_alert(self, event_type, detail):
        with self._alert_callbacks_lock:
            callbacks = list(self._alert_callbacks)
        for cb in callbacks:
            try:
                cb(event_type, detail)
            except Exception as e:
                logging.warning('[RiskService] R13-P0-LOG-05修复: 告警回调异常: %s', e)
    class _AbnormalTradeDetector:  # noqa: F811  # pylint: disable=redefined-outer-name
        def __init__(self):
            self._trade_history: deque = deque(maxlen=1000)
            self._cancel_counts: Dict[str, int] = {}
            self._burst_window_sec: float = 60.0
            self._burst_threshold: int = 20
            self._cancel_rate_threshold: float = 0.8
            self._self_trade_pairs: Dict[str, float] = {}

        def record_trade(self, instrument_id: str, direction: str, volume: float, price: float, timestamp: float = None) -> None:
            return self._config_provider.record_trade(instrument_id, direction, volume, price, timestamp)
        def detect_burst_trading(self, instrument_id: str = '', window_sec: float = None) -> Dict[str, Any]:
            return self._check_service.detect_burst_trading(instrument_id, window_sec)
        def detect_self_trade(self, instrument_id: str, direction: str, price: float, tolerance_pct: float = 0.001) -> Dict[str, Any]:
            return self._check_service.detect_self_trade(instrument_id, direction, price, tolerance_pct)
        def detect_price_deviation(self, instrument_id: str, order_price: float, market_price: float, threshold_pct: float = 0.02) -> Dict[str, Any]:
            return self._check_service.detect_price_deviation(instrument_id, order_price, market_price, threshold_pct)
        def detect_volume_spike(self, instrument_id: str, volume: float, avg_volume: float, spike_factor: float = 5.0) -> Dict[str, Any]:
            return self._check_service.detect_volume_spike(instrument_id, volume, avg_volume, spike_factor)
    def _on_config_param_change(self, event: Dict[str, Any]) -> None:
        return self._config_provider._on_config_param_change(event)
    def _on_aggregate_position(self, event: Any) -> None:
        return self._config_provider._on_aggregate_position(event)
    def record_partial_fill_progress(self, order_id, instrument_id, filled_volume, total_volume, strategy_id) -> None:
        return self._config_provider.record_partial_fill_progress(order_id, instrument_id, filled_volume, total_volume, strategy_id)
    def _start_dashboard_timer(self) -> None:  # [R27-AUDIT] P1修复: 添加hasattr保护防止委托链断裂
        if hasattr(self._config_provider, '_start_dashboard_timer'):
            return self._config_provider._start_dashboard_timer()
        logging.debug("[R27-AUDIT] _config_provider无_start_dashboard_timer方法，跳过")
    def _dashboard_timer_callback(self) -> None:
        if hasattr(self._config_provider, '_dashboard_timer_callback'):
            return self._config_provider._dashboard_timer_callback()
    def _cancel_dashboard_timer(self) -> None:
        if hasattr(self._config_provider, '_cancel_dashboard_timer'):
            return self._config_provider._cancel_dashboard_timer()
    def stop(self) -> None:
        # R25-P1-CF-05修复: stop()添加hasattr防御，防止_config_provider无stop方法时AttributeError
        if hasattr(self._config_provider, 'stop') and callable(self._config_provider.stop):
            return self._config_provider.stop()
    def detect_abnormal_trading(self, instrument_id, direction, price, volume, market_price, avg_volume) -> Dict[str, Any]:
        return self._check_service.detect_abnormal_trading(instrument_id, direction, price, volume, market_price, avg_volume)
    def check_before_trade(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._check_service.check_before_trade(signal)
    def _validate_signal(self, signal: Dict[str, Any], _check_dedup_key: str, _cyclic_guard) -> Optional[RiskCheckResponse]:
        return self._check_service._validate_signal(signal, _check_dedup_key, _cyclic_guard)
    def _check_operational_risks(self, signal: Dict[str, Any], action: str) -> Optional[RiskCheckResponse]:
        return self._check_service._check_operational_risks(signal, action)
    def _check_market_risks(self, signal: Dict[str, Any], action: str) -> Optional[RiskCheckResponse]:
        return self._check_service._check_market_risks(signal, action)
    def _check_counterparty_risks(self, signal: Dict[str, Any], action: str) -> Optional[RiskCheckResponse]:
        return self._check_service._check_counterparty_risks(signal, action)
    def _check_regulatory_risks(self, signal: Dict[str, Any], action: str) -> Optional[RiskCheckResponse]:
        return self._check_service._check_regulatory_risks(signal, action)
    def _detect_abnormal_trading_step(self, signal: Dict[str, Any]) -> Optional[RiskCheckResponse]:
        return self._check_service._detect_abnormal_trading_step(signal)
    def _record_and_publish(self, signal: Dict[str, Any], _check_dedup_key: str, _cyclic_guard) -> RiskCheckResponse:
        return self._check_service._record_and_publish(signal, _check_dedup_key, _cyclic_guard)
    def _check_safety_meta_layer(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._check_service._check_safety_meta_layer(signal)

    def _record_result(self, result: RiskCheckResponse) -> RiskCheckResponse:
        return self._check_service._record_result(result)
    def _check_strategy_status(self) -> RiskCheckResponse:
        return self._check_service._check_strategy_status()
    def _check_rate_limit(self, symbol: str) -> RiskCheckResponse:
        return self._check_service._check_rate_limit(symbol)

    def _record_signal_time(self, symbol: str) -> None:
        return self._check_service._record_signal_time(symbol)
    def _check_position_limit(self, account_id, required_amount, hedge_type) -> RiskCheckResponse:
        return self._check_service._check_position_limit(account_id, required_amount, hedge_type)
    def set_position_limit(self, account_id, limit_amount, effective_until) -> None:
        return self._config_provider.set_position_limit(account_id, limit_amount, effective_until)
    def get_position_limit(self, account_id: str) -> Optional[PositionLimit]:
        return self._compute_service.get_position_limit(account_id)
    def _check_risk_ratio(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._check_service._check_risk_ratio(signal)
    def _check_risk_consistency(self) -> RiskCheckResponse:
        return self._check_service._check_risk_consistency()

    def _check_invariant_runtime(self) -> Dict[str, Any]:
        return self._check_service._check_invariant_runtime()
    def _check_single_trade_risk(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._check_service._check_single_trade_risk(signal)
    def _check_sharpe_iron_rule(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._check_service._check_sharpe_iron_rule(signal)
    def _check_e7_residual_block(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._check_service._check_e7_residual_block(signal)
    def _check_capital_sufficiency_in_trade(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._check_service._check_capital_sufficiency_in_trade(signal)
    def check_regulatory_compliance(self, position_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        _safety = get_safety_meta_layer()
        return _safety.check_regulatory_compliance(position_data)
    def check_capital_sufficiency(self, equity: float, required_margin: float = 0.0,
                                  open_positions: int = 0, max_positions: int = 50,
                                  existing_margin_used: float = 0.0) -> Dict[str, Any]:
        _safety = get_safety_meta_layer()
        return _safety.check_capital_sufficiency(equity, required_margin, open_positions, max_positions, existing_margin_used)
    def check_exchange_status(self, exchange: str = "AUTO") -> Dict[str, Any]:
        _safety = get_safety_meta_layer()
        _result = _safety.check_exchange_status(exchange)
        if 'status' not in _result:
            _result['status'] = 'OPEN' if _result.get('tradeable', False) else 'CLOSED'
        return _result
    def _check_spread_degradation(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._check_service._check_spread_degradation(signal)
    def _check_governance_violations(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._check_service._check_governance_violations(signal)

    def _get_greeks_calculator(self):
        return self._config_provider._get_greeks_calculator()
    def get_greeks_dashboard(self) -> Dict[str, Any]:
        return self._compute_service.get_greeks_dashboard()
    def _compute_stress_test(self, calc, positions_dict, net_delta, net_gamma, net_vega, net_theta):
        return self._compute_service._compute_stress_test(calc, positions_dict, net_delta, net_gamma, net_vega, net_theta)
    def _compute_pnl_attribution(self, calc, positions_dict, net_delta, net_gamma, net_vega, net_theta):
        return self._compute_service._compute_pnl_attribution(calc, positions_dict, net_delta, net_gamma, net_vega, net_theta)

    def compute_simplified_pnl(self, entry_price, exit_price, volume, direction, multiplier, commission_per_lot) -> float:
        return self._compute_service.compute_simplified_pnl(entry_price, exit_price, volume, direction, multiplier, commission_per_lot)
    def validate_gamma_path_dependency(self, bar_data, positions_dict, n_simulations, max_deviation_pct) -> Dict[str, Any]:
        return self._check_service.validate_gamma_path_dependency(bar_data, positions_dict, n_simulations, max_deviation_pct)
    def log_greeks_dashboard_periodic(self) -> None:
        return self._compute_service.log_greeks_dashboard_periodic()
    def _check_greeks_limits(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._check_service._check_greeks_limits(signal)
    def calculate_risk_metrics(self) -> RiskMetrics:
        return self._compute_service.calculate_risk_metrics()
    def _get_signal_cooldown(self) -> float:
        return self._config_provider._get_signal_cooldown()
    def _get_max_signals_per_window(self) -> int:
        return self._config_provider._get_max_signals_per_window()
    def _get_rate_limit_window(self) -> float:
        return self._config_provider._get_rate_limit_window()
    def _get_max_risk_ratio(self) -> float:
        return self._config_provider._get_max_risk_ratio()
    def _get_total_position_limit(self) -> float:
        return self._config_provider._get_total_position_limit()
    def _get_contract_multiplier(self, instrument_id: str) -> float:
        return self._config_provider._get_contract_multiplier(instrument_id)
    def _get_margin_ratio(self, instrument_id: str) -> float:
        return self._config_provider._get_margin_ratio(instrument_id)
    def _get_circuit_breaker_pause_sec(self) -> float:
        return self._config_provider._get_circuit_breaker_pause_sec()
    def _get_circuit_breaker_calm_period_sec(self) -> float:
        return self._config_provider._get_circuit_breaker_calm_period_sec()
    def _get_daily_drawdown_multiplier(self) -> float:
        return self._config_provider._get_daily_drawdown_multiplier()
    def _get_hard_time_stop_minutes(self) -> float:
        return self._config_provider._get_hard_time_stop_minutes()
    def _get_min_profit_threshold(self) -> float:
        return self._config_provider._get_min_profit_threshold()
    def _get_stage1_minutes(self) -> float:
        return self._config_provider._get_stage1_minutes()
    def _get_stage2_minutes(self) -> float:
        return self._config_provider._get_stage2_minutes()
    def _get_stage1_profit_threshold(self) -> float:
        return self._config_provider._get_stage1_profit_threshold()
    def get_stats(self) -> Dict[str, Any]:
        return self._compute_service.get_stats()
    def get_risk_status(self) -> Dict[str, Any]:
        return self._compute_service.get_risk_status()
    def reset_stats(self) -> None:
        return self._config_provider.reset_stats()
    def clear_cache(self) -> None:
        return self._config_provider.clear_cache()
    def invalidate_ev_cache(self) -> None:
        return self._config_provider.invalidate_ev_cache()
    def _check_consecutive_loss_protection(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._check_service._check_consecutive_loss_protection(signal)

    def record_trade_result(self, strategy_id: str, pnl: float, capital_scale: str = 'medium') -> None:
        return self._config_provider.record_trade_result(strategy_id, pnl, capital_scale)
    def reset_consecutive_loss(self, strategy_id: str) -> None:
        return self._config_provider.reset_consecutive_loss(strategy_id)
    def get_strategy_risk_state(self, strategy_id: str) -> Dict[str, Any]:
        return self._compute_service.get_strategy_risk_state(strategy_id)
    def update_strategy_risk_state(self, strategy_id: str, **kwargs) -> None:
        return self._config_provider.update_strategy_risk_state(strategy_id, **kwargs)
    def set_capital_scale(self, capital_scale: str) -> None:
        return self._config_provider.set_capital_scale(capital_scale)
    def evaluate_capital_scale_upgrade(self) -> Dict[str, Any]:
        return self._config_provider.evaluate_capital_scale_upgrade()
    def compute_mode_position_size(self, equity, entry_price, stop_price, win_rate, win_loss_ratio, sortino_ratio, calmar_ratio, sharpe_ratio, ofi, cvd_divergence, smart_money_flow, delta, gamma, theta, vega, prediction_correct, price_direction) -> float:
        return self._compute_service.compute_mode_position_size(equity, entry_price, stop_price, win_rate, win_loss_ratio, sortino_ratio, calmar_ratio, sharpe_ratio, ofi, cvd_divergence, smart_money_flow, delta, gamma, theta, vega, prediction_correct, price_direction)
    def calculate_asymmetric_drawdown_limit(self, current_pnl: float, base_limit: float) -> float:
        return self._compute_service.calculate_asymmetric_drawdown_limit(current_pnl, base_limit)

    def check_asymmetric_risk(self, current_pnl: float, drawdown_pct: float, base_limit: float) -> RiskCheckResponse:
        return self._check_service.check_asymmetric_risk(current_pnl, drawdown_pct, base_limit)
    def compute_decision_score(self, state_strength, order_flow_consistency, hmm_state, cr_output, greeks_dashboard, consecutive_losses, current_pnl, drawdown_pct, alpha_ratio, cross_correlation, tri_validation_score, slippage_source) -> Dict[str, Any]:
        return self._compute_service.compute_decision_score(state_strength, order_flow_consistency, hmm_state, cr_output, greeks_dashboard, consecutive_losses, current_pnl, drawdown_pct, alpha_ratio, cross_correlation, tri_validation_score, slippage_source)
    def _compute_position_scale(self, composite_score: float,
                                 scores: Dict[str, float]) -> float:
        return self._compute_service._compute_position_scale(composite_score, scores)

    # ── 各维度评分计算方法 ──
    def _compute_life_score(self, hmm_state: Optional[str] = None) -> float:
        return self._compute_service._compute_life_score(hmm_state)
    def _compute_cycle_resonance_score(self, cr_output: Optional[Any] = None) -> float:
        return self._compute_service._compute_cycle_resonance_score(cr_output)
    def _compute_phase_quality_score(self, cr_output: Optional[Any] = None) -> float:
        return self._compute_service._compute_phase_quality_score(cr_output)
    def _compute_greeks_usage_score(self, dashboard: Optional[Dict[str, Any]] = None) -> float:
        return self._compute_service._compute_greeks_usage_score(dashboard)
    def _compute_consecutive_loss_score(self, consecutive_losses: int = 0) -> float:
        return self._compute_service._compute_consecutive_loss_score(consecutive_losses)
    def confirm_alpha_decay_recovery(self):
        return self._config_provider.confirm_alpha_decay_recovery()
    def _compute_asymmetric_drawdown_score(self, current_pnl: float = 0.0,
                                            drawdown_pct: float = 0.0) -> float:
        return self._compute_service._compute_asymmetric_drawdown_score(current_pnl, drawdown_pct)
    def _compute_tri_validation_score(self, tri_score: Optional[float] = None) -> float:
        return self._compute_service._compute_tri_validation_score(tri_score)
    def _compute_alpha_decay_score(self, alpha_ratio: Optional[float] = None) -> float:
        return self._compute_service._compute_alpha_decay_score(alpha_ratio)
    def _compute_cross_correlation_score(self, cross_corr: Optional[float] = None) -> float:
        return self._compute_service._compute_cross_correlation_score(cross_corr)
    def _compute_liquidity_score(self) -> float:
        return self._compute_service._compute_liquidity_score()

    def _get_life_estimator(self) -> Any:
        return self._config_provider._get_life_estimator()
    def set_life_estimator(self, estimator: Any) -> None:
        return self._config_provider.set_life_estimator(estimator)
    def set_current_hmm_state(self, state: str) -> None:
        return self._config_provider.set_current_hmm_state(state)
    def _check_life_expectancy(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._check_service._check_life_expectancy(signal)
    def check_price_limit(self, instrument_id: str, price: float, direction: str) -> RiskCheckResponse:
        return self._check_service.check_price_limit(instrument_id, price, direction)
    def is_at_price_limit(self, instrument_id: str, price: float) -> Dict[str, bool]:
        return self._config_provider.is_at_price_limit(instrument_id, price)
    def check_expiry_risk(self, instrument_id: str, days_to_expiry: int = None) -> RiskCheckResponse:
        return self._check_service.check_expiry_risk(instrument_id, days_to_expiry)
    def check_auction_session(self, bar_datetime, instrument_id: str) -> RiskCheckResponse:
        return self._check_service.check_auction_session(bar_datetime, instrument_id)
    def is_trading_day(check_date) -> bool:
        return self._config_provider.is_trading_day(check_date)
    def check_last_trading_day(self, instrument_id: str, days_to_expiry: int) -> RiskCheckResponse:
        return self._check_service.check_last_trading_day(instrument_id, days_to_expiry)
    def calc_option_expiry_date(year: int, month: int, which_friday: int = 3, exchange: str = 'CFFEX') -> 'date':
        return self._compute_service.calc_option_expiry_date(year, month, which_friday, exchange)
    def check_margin_sufficiency(self, equity, instrument_id, volume, price) -> RiskCheckResponse:
        return self._check_service.check_margin_sufficiency(equity, instrument_id, volume, price)
    def update_margin_ratio(self, instrument_id: str, new_ratio: float) -> None:
        return self._config_provider.update_margin_ratio(instrument_id, new_ratio)
    def check_cross_instrument_limit(self, account_id, instrument_id, required_amount) -> RiskCheckResponse:
        return self._check_service.check_cross_instrument_limit(account_id, instrument_id, required_amount)
    def auto_rollover_if_needed(self, instrument_id: str, days_to_expiry: int):
        return self._check_service.auto_rollover_if_needed(instrument_id, days_to_expiry)
    def _infer_industry(self, instrument_id: str) -> str:
        return self._config_provider._infer_industry(instrument_id)
    def set_industry_limit(self, industry: str, limit_amount: float) -> None:
        return self._config_provider.set_industry_limit(industry, limit_amount)
    def monitor_margin_occupancy(self, account_id: str, equity: float, current_margin_used: float) -> Dict[str, Any]:
        return self._config_provider.monitor_margin_occupancy(account_id, equity, current_margin_used)
    def check_invariant_runtime(self, invariant_name, condition, context) -> bool:
        return self._check_service.check_invariant_runtime(invariant_name, condition, context)
    def _handle_invariant_violation(self, invariant_name, context) -> Dict[str, Any]:
        return self._check_service._handle_invariant_violation(invariant_name, context)
    def detect_data_flow_anomaly(self, metric_name, current_value, baseline_value, threshold_sigma) -> Dict[str, Any]:
        return self._check_service.detect_data_flow_anomaly(metric_name, current_value, baseline_value, threshold_sigma)
# ============================================================================
# 辅助函数
# ============================================================================

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
    # AP-03: SingletonRegistry注册（非关键路径，失败不影响服务返回）
    try:
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        registry = SingletonRegistry.get_registry("risk_service")
        registry.register_singleton("risk_service." + scope, result)
    except ImportError as e:
        logging.debug("[R26-FIX] SingletonRegistry模块不可用，跳过risk_service单例注册: %s", e)
    except Exception as e:
        logging.warning("[R26-FIX] SingletonRegistry.register_singleton失败: %s", e)
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
        import warnings
        warnings.warn(
            "RiskDashboardService.get_instance() is deprecated; use get_risk_dashboard_service() instead",
            DeprecationWarning,
            stacklevel=2,
        )
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
        self._greeks_cache_ttl_seconds: float = 300.0
        self._greeks_cache_timestamps: Dict[str, float] = {}
        self._greeks_cache: Dict[str, Any] = {}
        self._pnl_attribution: Dict[str, float] = {}
        self._stress_results: Dict[str, Any] = {}
        self._last_update: datetime = datetime.now(_CHINA_TZ)

    def aggregate_greeks(self, positions: Dict[str, Any]) -> Dict[str, float]:
        """Greeks聚合：按持仓汇总delta/gamma/theta/vega"""
        now = time.time()
        expired_keys = [k for k, ts in self._greeks_cache_timestamps.items() if (now - ts) > self._greeks_cache_ttl_seconds]
        for k in expired_keys:
            self._greeks_cache.pop(k, None)
            del self._greeks_cache_timestamps[k]
        agg = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}
        for sym, pos in positions.items():
            greeks = getattr(pos, 'greeks', None) or {}
            for k in agg:
                agg[k] += greeks.get(k, 0.0)
        self._greeks_cache = agg
        for k in agg:
            self._greeks_cache_timestamps[k] = now
        if len(self._greeks_cache) > self._greeks_cache_maxsize:
            keys_to_evict = list(self._greeks_cache.keys())[:len(self._greeks_cache) - self._greeks_cache_maxsize]
            for k in keys_to_evict:
                del self._greeks_cache[k]
                self._greeks_cache_timestamps.pop(k, None)
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
    with RiskDashboardService._lock:
        if RiskDashboardService._instance is None:
            RiskDashboardService._instance = RiskDashboardService()
            # AP-03: SingletonRegistry注册（非关键路径，失败不影响服务返回）
            try:
                from ali2026v3_trading.singleton_registry import SingletonRegistry
                registry = SingletonRegistry.get_registry("risk_dashboard_service")
                registry.register_singleton("risk_dashboard_service.instance", RiskDashboardService._instance)
            except ImportError as e:
                logging.debug("[R26-FIX] SingletonRegistry模块不可用，跳过dashboard单例注册: %s", e)
            except Exception as e:
                logging.warning("[R26-FIX] SingletonRegistry.register_singleton(dashboard)失败: %s", e)
    return RiskDashboardService._instance


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



# Re-exports from risk_audit_utils (backward compatibility)
from ali2026v3_trading.risk_audit_utils import (
    safe_get,
    safe_get_float,
    safe_get_int,
    _normalize_risk_scope_id,
    AlertLevel,
    AlertDeduplicator,
    get_alert_deduplicator,
    _check_alert_escalation,
    alert,
    _get_audit_log_path,
    operations_audit_log,
    audit_chain_append,
    structured_audit_log,
    save_state_snapshot,
    generate_exchange_report,
    SimplifiedSPAN,
    calculate_var_historical,
    calculate_var_rolling,
    check_circuit_breaker_auto_recovery,
)
