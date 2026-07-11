# [M1-27] 风控服务

# MODULE_ID: M1-218

"""
risk_service.py - 风控服务

合并来源：8_position.py (PositionManagerMixin) + 10_gate.py (GateLayer3)
合并策略：提取风控检查、限额管理、风险计算等功能

核心改进：
1. 单一职责：专注于风险控制与限额管理
2. 统一风控接口：整合持仓限额、信号限频、策略状态检查
3. 线程安全：支持并发检查
4. 可配置性：灵活的风控参数配置

作者：CodeArts 代码智能。
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
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple, TypedDict

import numpy as np

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, json_default_serializer
from ali2026v3_trading.config import config_params
from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ, api_version, safe_int, safe_float

from ali2026v3_trading.infra.resilience import (
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

from .risk_support import _get_tz_aware_now


class ComplianceResult(TypedDict, total=False):
    compliant: bool
    reason: str
    details: Dict[str, Any]


class SufficiencyResult(TypedDict, total=False):
    sufficient: bool
    reason: str
    margin_used: float
    details: Dict[str, Any]


class ExchangeStatusResult(TypedDict, total=False):
    status: str
    reason: str
    exchange: str
    details: Dict[str, Any]


def _audit_annotation(category='', phase1_status='', note=''):
    """审计标注装饰器，不改变类行为，仅用于H.1.3格式清单审计追踪"""
    def decorator(cls):
        cls._audit_category = category
        cls._audit_phase1_status = phase1_status
        cls._audit_note = note
        return cls
    return decorator


@_audit_annotation(category='lightweight_enum', phase1_status='stable', note='枚举类，0方法执行，无隐式依赖')
class RiskLevel(Enum):
    """风险等级"""
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    RESTRICTED = auto()  # [R23-P1-02-FIX] 受限中间状态：正常→受限→熔断，防止直接从正常跳到熔断
    CRITICAL = auto()


@_audit_annotation(category='lightweight_enum', phase1_status='stable', note='枚举类，0方法执行，无隐式依赖')
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
    """[R23-P1-02-FIX] 校验风控级别转移合法性，禁止正常→熔断直接跳过"""
    allowed = _LEGAL_RISK_LEVEL_TRANSITIONS.get(current, set())
    if target in allowed:
        return target
    if target == RiskLevel.CRITICAL and current in (RiskLevel.LOW, RiskLevel.MEDIUM):
        logging.warning("[R23-P1-02-FIX] 风控级别非法跳跃: %s, 强制经过RESTRICTED中间状态", target.name)
        return RiskLevel.RESTRICTED
    return target


@_audit_annotation(category='lightweight_dataclass', phase1_status='stable', note='响应dataclass，方法执行6行，无隐式依赖')
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


@_audit_annotation(category='lightweight_dataclass', phase1_status='stable', note='指标dataclass，方法执行2行，无隐式依赖')
@dataclass(slots=True)
class RiskMetrics:
    """风险指标 - 11维度评分"""
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


try:
    from ali2026v3_trading.strategy_judgment.causal_chain_utils import (
        CyclicDependencyGuard, ContaminationGuard, ParamIsolationGuard,
    )
    _HAS_CAUSAL_CHAIN = True
except ImportError:
    _HAS_CAUSAL_CHAIN = False

try:
    from ali2026v3_trading.position.position_service import PositionLimitConfig as PositionLimit
except ImportError:
    PositionLimit = None

from ali2026v3_trading.risk.risk_check_service import RiskCheckService
from ali2026v3_trading.risk.risk_compute_service import RiskComputeService
from ali2026v3_trading.risk.risk_circuit_breaker import SafetyMetaLayer, get_safety_meta_layer, cleanup_safety_layer
from ali2026v3_trading.risk.risk_config_provider import RiskConfigProvider


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

    _alert_callbacks: List = []
    _alert_callbacks_lock = threading.Lock()

    def __init__(self, params: Any = None, position_manager: Any = None, strategy: Any = None):
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

        self._dashboard_service = get_risk_dashboard_service()

    def _get_life_estimator(self) -> Any:
        """委托到RiskConfigProvider的懒加载life estimator"""
        return self._config_provider._get_life_estimator()

    def __getattr__(self, name: str) -> Any:
        """动态委托方法到子服务"""
        if name.startswith('_'):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        for service_attr in ('_check_service', '_compute_service', '_config_provider'):
            service = getattr(self, service_attr, None)
            if service is not None and hasattr(service, name):
                return getattr(service, name)

        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def check_regulatory_compliance(self, position_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._check_service.check_regulatory_compliance(position_data)

    def check_capital_sufficiency(self, equity: float, required_margin: float = 0.0,
                                   open_positions: int = 0, max_positions: int = 50,
                                   existing_margin_used: float = 0.0) -> Dict[str, Any]:
        return self._check_service.check_capital_sufficiency(
            equity, required_margin, open_positions, max_positions, existing_margin_used
        )

    def check_exchange_status(self, exchange: str = "AUTO") -> Dict[str, Any]:
        return self._check_service.check_exchange_status(exchange)

    def get_dashboard_service(self) -> 'RiskDashboardService':
        return self._dashboard_service

    @classmethod
    def register_alert_callback(cls, cb) -> None:
        with cls._alert_callbacks_lock:
            if cb not in cls._alert_callbacks:
                cls._alert_callbacks.append(cb)

        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            bus = get_global_event_bus()
            if bus is not None:
                def _eventbus_adapter(event):
                    try:
                        cb(event.alert_type, event.detail)
                    except (ValueError, KeyError, TypeError, AttributeError):
                        logging.debug("[R3-L2] suppressed exception", exc_info=True)

                bus.subscribe_weak('RiskAlertEvent', _eventbus_adapter)
        except (ValueError, KeyError, TypeError, AttributeError):
            logging.debug("[R3-L2] suppressed exception", exc_info=True)

    def _fire_alert(self, event_type, detail):
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus, RiskAlertEvent
            bus = get_global_event_bus()
            if bus is not None:
                bus.publish(RiskAlertEvent(alert_type=event_type, detail=detail))
        except (ValueError, KeyError, TypeError, AttributeError):
            logging.debug("[R3-L2] suppressed exception", exc_info=True)

        with self._alert_callbacks_lock:
            callbacks = list(self._alert_callbacks)

        for cb in callbacks:
            try:
                cb(event_type, detail)
            except (TypeError, AttributeError) as e:
                logging.error('[RiskService] R13-P0-LOG-05修复: 告警回调异常: %s', e)

    def check_before_trade(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        try:
            from ali2026v3_trading.risk.crack_validation import (
                check_safety_meta_layer,
                validate_circuit_breaker_vs_time_stop,
            )

            _cb_paused = getattr(self, '_is_circuit_breaker_paused', False)
            _hard_time_stop = getattr(self, '_hard_time_stop_triggered', False)
            _safety_ok, _time_stop_ok = check_safety_meta_layer(_cb_paused, _hard_time_stop)

            if not _safety_ok:
                return RiskCheckResponse.block_result(reason='crack_validation_safety_meta_failed')

            if not _time_stop_ok:
                logging.warning("[R33-P1-7] 断路器与时间止损冲突检查 cb_paused=%s, time_stop=%s", _cb_paused, _hard_time_stop)

        except ImportError:
            pass
        except (ImportError, AttributeError, TypeError) as _cv_err:
            logging.error("[R33-P1-7] crack_validation pre-check error: %s", _cv_err)

        # FIX-20260707-HARD-STOP: 补充日回撤硬停止检查
        # 根因: check_before_trade只检查_hard_time_stop_triggered(日内硬时间止损)，
        # 未检查_daily_hard_stop_triggered(日回撤硬停止)，导致hard_stop=True时
        # 仍放行开仓请求，83次无效下单被平台拒绝(result=-1)
        _daily_hard_stop = getattr(self, '_daily_hard_stop_triggered', None)
        if _daily_hard_stop is None:
            try:
                from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
                _sid = str(getattr(self, '_scope_id', '') or 'global')
                _safety = get_safety_meta_layer(None, scope_id=_sid)
                if _safety:
                    _daily_hard_stop = _safety.is_hard_stop_triggered()
            except Exception:
                pass
        if _daily_hard_stop:
            # FIX-20260710-DRY-RUN-PASS-V3: dry_run模式下不阻断开仓，在快照中记录风控状态
            # 根因V2: 2级fallback检测dry_run在某些时序下失败，导致dry_run=True仍被hard_stop阻断
            # 修复V3: 与strategy_business_layer.py对齐，使用3级fallback+诊断日志
            _dry_run = False
            _dr_source = 'none'
            try:
                from ali2026v3_trading.config.params_service import get_params_service
                _dry_run = get_params_service().get_bool('dry_run_mode', False) or False
                if _dry_run:
                    _dr_source = 'params_service'
            except Exception:
                pass
            if not _dry_run:
                try:
                    from ali2026v3_trading.order.order_base import get_order_service
                    _osvc = get_order_service()
                    if _osvc is not None:
                        _dry_run = bool(getattr(_osvc, '_dry_run_mode', False))
                        if _dry_run:
                            _dr_source = 'order_service'
                except Exception:
                    pass
            if not _dry_run:
                try:
                    from ali2026v3_trading.risk.risk_support import get_safety_meta_layer as _get_sml
                    _sml = _get_sml()
                    if _sml is not None:
                        _dry_run = bool(getattr(_sml, '_dry_run_active', False))
                        if _dry_run:
                            _dr_source = 'safety_meta_layer'
                except Exception:
                    pass
            logging.info(
                "[FIX-20260710-DRY-RUN-PASS-V3] risk_service hard_stop检查: "
                "dry_run=%s source=%s hard_stop=%s action=%s",
                _dry_run, _dr_source, _daily_hard_stop,
                signal.get('action', 'OPEN') if isinstance(signal, dict) else 'OPEN',
            )
            if _dry_run:
                if isinstance(signal, dict):
                    signal['_risk_block_reason'] = 'hard_stop_triggered: 日回撤硬停止已触发(dry_run跳过)'
                logging.info(
                    "[FIX-20260710-DRY-RUN-PASS-V3] dry_run模式跳过hard_stop阻断: action=%s instrument=%s",
                    signal.get('action', '') if isinstance(signal, dict) else '',
                    signal.get('symbol', '') if isinstance(signal, dict) else '',
                )
            else:
                _action = signal.get('action', 'OPEN') if isinstance(signal, dict) else 'OPEN'
                if _action in ('OPEN', 'open', ''):
                    return RiskCheckResponse.block_result(
                        reason='hard_stop_triggered: 日回撤硬停止已触发，禁止新开仓')

        return self._check_service.check_before_trade(signal)

    def _record_and_publish(self, signal: Dict[str, Any], _check_dedup_key: str, _cyclic_guard) -> RiskCheckResponse:
        result = self._validate_and_check(signal, _check_dedup_key, _cyclic_guard)
        self._record_result(result)
        self._publish_risk_event_via_eventbus(signal, result)
        return result

    def _validate_and_check(self, signal: Dict[str, Any], _check_dedup_key: str, _cyclic_guard) -> 'RiskCheckResponse':
        try:
            result = self._check_service.check_before_trade(signal)
            return result
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            return RiskCheckResponse.block_result("check_error", str(e), RiskLevel.CRITICAL)

    def _publish_risk_event_via_eventbus(self, signal: Dict[str, Any], result: 'RiskCheckResponse') -> None:
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus, RiskEvent
            bus = get_global_event_bus()
            if bus is not None:
                level = 'CRITICAL' if hasattr(result, 'level') and str(result.level) == 'CRITICAL' else 'INFO'
                bus.publish(RiskEvent(
                    risk_type='risk_check',
                    level=level,
                    message=f"RiskCheckResult: {getattr(result, 'result', 'unknown')}"
                ))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[P1-42] EventBus发布风控事件失败(非致命): %s", e)


_risk_service_lock = threading.Lock()
_risk_service_instances: Dict[str, RiskService] = {}


def get_risk_service(params: Any = None, position_manager: Any = None,
                     strategy: Any = None,
                     scope_id: Optional[str] = None) -> RiskService:
    """R13-API-02修复: RiskService单例工厂支持参数更新

    首次以params=None初始化后，后续传入真实params会被更新到实例中，
    避免风控检查因参数缺失而失效。
    """
    from ali2026v3_trading.infra.security_service import _normalize_risk_scope_id

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

            if params is not None:
                if instance.params is None or instance.params != params:
                    instance.params = params
                    logging.info('[RiskService] 参数已更新 scope=%s params_type=%s',
                                 scope,
                                 type(params).__name__)

            if position_manager is not None and instance.position_manager is None:
                instance.position_manager = position_manager

            if strategy is not None and instance.strategy is None:
                instance.strategy = strategy

    result = _risk_service_instances.get(scope)
    if result is None:
        raise RuntimeError(f"[R23-P1-06-FIX] get_risk_service()返回None，scope={scope}单例初始化失败")

    try:
        from ali2026v3_trading.infra.registry_service import SingletonRegistry
        SingletonRegistry.register_singleton("risk_service." + scope, result)
    except ImportError as e:
        logging.debug("[R26-FIX] SingletonRegistry模块不可用，跳过risk_service单例注册: %s", e)
    except (ImportError, AttributeError, TypeError) as e:
        logging.error("[R26-FIX] SingletonRegistry.register_singleton失败: %s", e)

    return result


def reset_risk_service(scope_id: Optional[str] = None) -> None:
    with _risk_service_lock:
        if scope_id is None:
            _risk_service_instances.clear()
        else:
            _risk_service_instances.pop(str(scope_id), None)

    try:
        from ali2026v3_trading.infra.event_bus import get_global_event_bus
        _bus = get_global_event_bus()
        if _bus is not None:
            _bus.publish('risk_service.reset', {'scope_id': scope_id, 'timestamp': time.time()})
            logging.info("[R23-P1-07-FIX] risk_service reset通知已发送 scope_id=%s", scope_id)
    except (ImportError, AttributeError) as _e:
        logging.error("[R23-P1-07-FIX] reset通知发布失败: %s", _e)


class RiskDashboardService:
    """风险仪表盘服务：Greeks聚合、PnL归因、压力测试

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
        self._greeks_cache_maxsize: int = 10000
        self._greeks_cache_ttl_seconds: float = 300.0
        self._greeks_cache_timestamps: Dict[str, float] = {}
        self._greeks_cache: Dict[str, Any] = {}
        self._pnl_attribution: Dict[str, float] = {}
        self._stress_results: Dict[str, Any] = {}
        self._last_update: datetime = datetime.now(_CHINA_TZ)

    def aggregate_greeks(self, positions: Dict[str, Any]) -> Dict[str, float]:
        """P1-61修复: Greeks聚合委托到position_greeks.aggregate_greeks_exposure（唯一权威实现）"""
        try:
            from ali2026v3_trading.position.position_greeks import aggregate_greeks_exposure
            exposure = aggregate_greeks_exposure(positions)
            return {
                'delta': exposure.total_delta,
                'gamma': exposure.total_gamma,
                'theta': getattr(exposure, 'total_theta', 0.0),
                'vega': exposure.total_vega,
            }
        except (ImportError, AttributeError, TypeError):
            agg = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}
            for sym, pos in positions.items():
                greeks = getattr(pos, 'greeks', None) or {}
                for k in agg:
                    agg[k] += greeks.get(k, 0.0)
            return agg

    def attribute_pnl(self, closed_trades: List[Any]) -> Dict[str, float]:
        """PnL归因：按策略/状态持仓时长分组归因"""
        attr: Dict[str, float] = {}
        for t in closed_trades:
            reason = getattr(t, 'close_reason', 'unknown')
            pnl = getattr(t, 'pnl', 0.0)
            attr[reason] = attr.get(reason, 0.0) + pnl
        self._pnl_attribution = attr
        return attr

    def run_stress_test(self, positions: Dict[str, Any],
                        shock_pct: float = 0.10) -> Dict[str, Any]:
        """压力测试：价格冲击shock_pct下的最坏情景损益"""
        total_loss = 0.0
        for sym, pos in positions.items():
            exposure = getattr(pos, 'volume', 0) * getattr(pos, 'open_price', 0)
            total_loss += abs(exposure) * shock_pct
        self._stress_results = {"shock_pct": shock_pct, "worst_case_loss": total_loss}
        return self._stress_results


def get_risk_dashboard_service() -> RiskDashboardService:
    """获取RiskDashboardService全局单例"""
    with RiskDashboardService._lock:
        if RiskDashboardService._instance is None:
            RiskDashboardService._instance = RiskDashboardService()
            try:
                from ali2026v3_trading.infra.registry_service import SingletonRegistry
                SingletonRegistry.register_singleton("risk_dashboard_service.instance", RiskDashboardService._instance)
            except ImportError as e:
                logging.debug("[R26-FIX] SingletonRegistry模块不可用，跳过dashboard单例注册: %s", e)
            except (ImportError, AttributeError, TypeError) as e:
                logging.error("[R26-FIX] SingletonRegistry.register_singleton(dashboard)失败: %s", e)
    return RiskDashboardService._instance


__all__ = [
    'RiskService',
    'RiskLevel',
    'RiskCheckResult',
    'RiskCheckResponse',
    'RiskMetrics',
    'get_risk_service',
    'reset_risk_service',
    'SafetyMetaLayer',
    'RiskDashboardService',
    'get_risk_dashboard_service',
]

from ali2026v3_trading.infra.security_service import (
    safe_get,
    safe_get_float,
    safe_get_int,
    AlertLevel,
    AlertDeduplicator,
    get_alert_deduplicator,
    _check_alert_escalation,
    alert,
    _get_audit_log_path,
    operations_audit_log,
    audit_chain_append,
    save_state_snapshot,
    generate_exchange_report,
    SimplifiedSPAN,
    calculate_var_historical,
    calculate_var_rolling,
    check_circuit_breaker_auto_recovery,
)

from ali2026v3_trading.infra.security_service import structured_audit_log
