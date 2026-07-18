# MODULE_ID: M1-224-235
"""
risk_engine.py - 风控引擎模块（合并12个文件）

合并来源：
- __init__.py (5行): 风控引擎入口
- snapshot.py (62行): 风控快照
- log_deduplicator.py (26行): 日志去重器
- abnormal_trade_detector.py (86行): 异常交易检测器
- input_builder.py (31行): 输入构建器
- result_handler.py (28行): 结果处理器
- regulatory_risk.py (14行): 合规风险检查
- shared_checks.py (91行): 共享检查函数
- counterparty_risk.py (101行): 交易对手风险
- operational_risk.py (123行): 运营风险检查
- market_risk.py (111行): 市场风险检查
- engine.py (110行): 风控引擎主循环

作者：CodeArts 代码智能体
版本：v2.1
"""

from __future__ import annotations

import copy
import time
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque

from risk.risk_service import RiskCheckResponse, RiskLevel

try:
    from infra.shared_utils import safe_int, safe_float
except ImportError:
    safe_int = lambda x, d=0: d if x is None else int(x)
    safe_float = lambda x, d=0.0: d if x is None else float(x)

try:
    from risk.risk_support import safe_get_float, safe_get_int
except ImportError:
    safe_get_float = lambda obj, k, d=0.0: d
    safe_get_int = lambda obj, k, d=0: d

try:
    from infra.security_service import structured_audit_log
except ImportError:
    structured_audit_log = None

try:
    from strategy_judgment.causal_chain_utils import CyclicDependencyGuard
    _HAS_CAUSAL_CHAIN = True
except ImportError:
    _HAS_CAUSAL_CHAIN = False


# =============================================================================
# Part 1: snapshot.py - 风控快照
# =============================================================================

@dataclass(frozen=True, slots=True)
class RiskSnapshot:
    snapshot_time: float
    signal: Dict[str, Any]
    action: str
    symbol: str
    instrument_id: str
    account_id: str
    amount: float
    price: float
    volume: float
    direction: str
    equity: float = 0.0
    market_price: float = 0.0
    avg_volume: float = 0.0
    days_to_expiry: int = 999
    hedge_type: str = 'speculation'
    bar_datetime: Optional[Any] = None
    is_valid: bool = True
    params: Optional[Any] = None
    strategy: Optional[Any] = None
    position_manager: Optional[Any] = None
    safety_meta_layer: Optional[Any] = None

    @classmethod
    def from_signal(cls, signal: Dict[str, Any], params: Any = None,
                    strategy: Any = None, position_manager: Any = None,
                    safety_meta_layer: Any = None) -> "RiskSnapshot":
        symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
        return cls(
            snapshot_time=time.time(),
            signal=signal,
            action=signal.get("action", ""),
            symbol=symbol,
            instrument_id=signal.get("instrument_id", symbol),
            account_id=signal.get("account_id", "default"),
            amount=float(signal.get("amount", 0)),
            price=float(signal.get("price", signal.get("entry_price", 0))),
            volume=float(signal.get("volume", signal.get("amount", 1))),
            direction=signal.get("direction", ""),
            equity=float(signal.get("equity", 0)),
            market_price=float(signal.get("market_price", 0)),
            avg_volume=float(signal.get("avg_volume", 0)),
            days_to_expiry=int(signal.get("days_to_expiry", 999)),
            hedge_type=signal.get('hedge_type', 'speculation'),
            bar_datetime=signal.get('bar_datetime'),
            is_valid=signal.get("is_valid", True),
            params=params,
            strategy=strategy,
            position_manager=position_manager,
            safety_meta_layer=safety_meta_layer,
        )


# =============================================================================
# Part 2: log_deduplicator.py - 日志去重器
# =============================================================================

class LogDeduplicator:
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


# =============================================================================
# Part 3: abnormal_trade_detector.py - 异常交易检测器
# =============================================================================

class AbnormalTradeDetector:
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


# =============================================================================
# Part 4: input_builder.py - 输入构建器
# =============================================================================

def build_snapshot(signal: Dict[str, Any], risk_service: Any) -> RiskSnapshot:
    params = getattr(risk_service, 'params', None)
    strategy = getattr(risk_service, 'strategy', None)
    position_manager = getattr(risk_service, 'position_manager', None)
    try:
        from risk.risk_service import get_safety_meta_layer
        safety_meta_layer = get_safety_meta_layer(params)
    except (ImportError, ValueError, TypeError, AttributeError, RuntimeError):
        safety_meta_layer = None
    return RiskSnapshot.from_signal(
        signal=signal,
        params=params,
        strategy=strategy,
        position_manager=position_manager,
        safety_meta_layer=safety_meta_layer,
    )


# =============================================================================
# Part 5: result_handler.py - 结果处理器
# =============================================================================

def record_result(result: RiskCheckResponse, risk_service: Any) -> RiskCheckResponse:
    return risk_service._record_result(result)


def record_and_publish(signal: Dict[str, Any], check_dedup_key: str, cyclic_guard: Any, risk_service: Any) -> RiskCheckResponse:
    return risk_service._record_and_publish(signal, check_dedup_key, cyclic_guard)


def record_block_with_context(result: RiskCheckResponse, risk_service: Any) -> RiskCheckResponse:
    _duration_ms = 0.0
    if hasattr(risk_service, '_check_start_time') and risk_service._check_start_time > 0:
        _duration_ms = (time.time() - risk_service._check_start_time) * 1000
        risk_service._check_start_time = 0.0
    return risk_service._record_result(result)


# =============================================================================
# Part 6: shared_checks.py - 共享检查函数
# =============================================================================

def check_position_limit(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    return risk_service._check_position_limit(
        snapshot.account_id, snapshot.amount, hedge_type=snapshot.hedge_type
    )


def check_governance_violations(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    return risk_service._check_governance_violations(snapshot.signal)


def check_margin_sufficiency(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    try:
        equity = safe_float(snapshot.equity)
        volume = safe_int(snapshot.amount)
        price = safe_float(snapshot.price)
        if equity > 0 and volume != 0 and price > 0:
            margin_result = risk_service.check_margin_sufficiency(
                equity, snapshot.instrument_id, volume, price
            )
            if margin_result.is_block:
                return margin_result
    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError):
        logging.warning("[R22-EP-P1] RiskService exception swallowed")
    return None


def check_cross_instrument_limit(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    try:
        amount_val = safe_float(snapshot.amount)
        price = safe_float(snapshot.price)
        cross_result = risk_service.check_cross_instrument_limit(
            snapshot.account_id, snapshot.instrument_id, abs(amount_val) * price
        )
        if cross_result.is_block:
            return cross_result
    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError):
        logging.warning("[R22-EP-P1] RiskService exception swallowed")
    return None


def update_margin_ratio_override(snapshot: RiskSnapshot, risk_service: Any) -> None:
    try:
        from config.config_service import get_config
    except ImportError:
        logging.warning("[P1-1-FIX] config_service.get_config导入失败，margin_ratio覆盖跳过")
        return
    try:
        _cfg = get_config()
        _overrides = _cfg.get('min_margin_ratio_override', {})
        if _overrides:
            for _prod, _ratio in _overrides.items():
                risk_service.update_margin_ratio(_prod, safe_float(_ratio))
    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError):
        logging.warning("[R22-EP-P1] RiskService exception swallowed")


def check_regulatory_risks(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    limit_result = check_position_limit(snapshot, risk_service)
    if limit_result is not None and limit_result.is_block:
        return limit_result
    if snapshot.action != "CLOSE":
        gov_result = check_governance_violations(snapshot, risk_service)
        if gov_result is not None and gov_result.is_block:
            return gov_result
    if snapshot.action != "CLOSE":
        update_margin_ratio_override(snapshot, risk_service)
    if snapshot.action != "CLOSE":
        margin_result = check_margin_sufficiency(snapshot, risk_service)
        if margin_result is not None and margin_result.is_block:
            return margin_result
    if snapshot.action != "CLOSE":
        cross_result = check_cross_instrument_limit(snapshot, risk_service)
        if cross_result is not None and cross_result.is_block:
            return cross_result
    risk_service._record_signal_time(snapshot.symbol)
    return None


# =============================================================================
# Part 7: market_risk.py - 市场风险检查
# =============================================================================

def check_risk_ratio(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    return risk_service._check_risk_ratio(snapshot.signal)


def check_greeks_limits(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    return risk_service._check_greeks_limits(snapshot.signal)


def check_life_expectancy(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    return risk_service._check_life_expectancy(snapshot.signal)


def check_spread_degradation(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    return risk_service._check_spread_degradation(snapshot.signal)


def check_exchange_status(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    try:
        _exch_result = risk_service.check_exchange_status()
        if not _exch_result.get('tradeable', True):
            return RiskCheckResponse.block_result(
                "exchange_not_tradeable",
                f"交易所不可交易: {_exch_result.get('reason', 'unknown')}",
                RiskLevel.CRITICAL
            )
    except (KeyError, AttributeError, RuntimeError) as _exch_err:
        logging.warning("[P-03补全] check_exchange_status异常(非致命): %s", _exch_err)
    return None


def check_price_limit(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    try:
        limit_flag = risk_service.is_at_price_limit(snapshot.instrument_id, snapshot.price)
        snapshot.signal.update(limit_flag)
    except (KeyError, AttributeError, TypeError, RuntimeError):
        logging.warning("[R22-EP-P1] RiskService exception swallowed")
    return None


def check_expiry_risk(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    try:
        expiry_result = risk_service.check_expiry_risk(snapshot.instrument_id, snapshot.days_to_expiry)
        if expiry_result.is_block:
            return expiry_result
    except (KeyError, AttributeError, RuntimeError):
        logging.warning("[R22-EP-P1] RiskService exception swallowed")
    return None


def check_auction_session(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    try:
        auction_result = risk_service.check_auction_session(
            bar_datetime=snapshot.bar_datetime,
            instrument_id=snapshot.instrument_id
        )
        if auction_result.is_block:
            return auction_result
    except (KeyError, AttributeError, RuntimeError) as _auction_err:
        logging.warning("[EX-P0-05] auction session check error: %s", _auction_err)
    return None


def check_market_risks(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    risk_result = check_risk_ratio(snapshot, risk_service)
    if risk_result is not None and risk_result.is_block:
        return risk_result
    greeks_result = check_greeks_limits(snapshot, risk_service)
    if greeks_result is not None and greeks_result.is_block:
        return greeks_result
    life_result = check_life_expectancy(snapshot, risk_service)
    if life_result is not None and life_result.is_block:
        return life_result
    if snapshot.action != "CLOSE":
        spread_result = check_spread_degradation(snapshot, risk_service)
        if spread_result is not None and spread_result.is_block:
            return spread_result
    if snapshot.action != "CLOSE":
        exch_result = check_exchange_status(snapshot, risk_service)
        if exch_result is not None:
            return exch_result
    auction_result = check_auction_session(snapshot, risk_service)
    if auction_result is not None:
        return auction_result
    if snapshot.action != "CLOSE":
        check_price_limit(snapshot, risk_service)
    if snapshot.action != "CLOSE":
        expiry_result = check_expiry_risk(snapshot, risk_service)
        if expiry_result is not None:
            return expiry_result
    return None


# =============================================================================
# Part 8: counterparty_risk.py - 交易对手风险
# =============================================================================

def check_e13_collusion(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    if snapshot.action != "CLOSE":
        try:
            from strategy.strategy_ecosystem import get_strategy_ecosystem
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
                    try:
                        from infra.event_bus import get_global_event_bus
                        _bus = get_global_event_bus()
                        if _bus is not None:
                            _bus.publish('risk.e13_detected', {
                                'type': 'risk.e13_detected',
                                'e13_triggered': True,
                                'e13_result': _e13_result,
                                'symbol': snapshot.symbol,
                            }, async_mode=True)
                    except (ImportError, AttributeError, RuntimeError):
                        logging.warning("[R22-EP-P1] RiskService exception swallowed")
                    return RiskCheckResponse.block_result(
                        "e13_collusion_detected",
                        "DFG-P1-02: E13同谋检测触发，主策略与影子策略参数同谋，阻断新开仓",
                        RiskLevel.HIGH
                    )
        except (ImportError, AttributeError, KeyError, TypeError, RuntimeError) as _e13_e:
            logging.debug("[DFG-P1-02] E13同谋检测异常(非阻断): %s", _e13_e)
    return None


def check_strategy_health(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    if snapshot.action != "CLOSE":
        try:
            from strategy.strategy_ecosystem import get_strategy_ecosystem
            _eco_hc = get_strategy_ecosystem()
            _eco_health = _eco_hc.get_health_status()
            _eco_status = _eco_health.get('status', 'OK')
            try:
                from infra.event_bus import get_global_event_bus
                _bus_hc = get_global_event_bus()
                if _bus_hc is not None:
                    _bus_hc.publish('strategy.health_check', {
                        'type': 'strategy.health_check',
                        'status': _eco_status,
                        'health_data': _eco_health,
                        'symbol': snapshot.symbol,
                    }, async_mode=True)
            except (ImportError, AttributeError, RuntimeError):
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
            if _eco_status == 'CRITICAL':
                return RiskCheckResponse.block_result(
                    "strategy_health_critical",
                    "DFG-P1-05: 策略生态系统健康状态CRITICAL，阻断新开仓",
                    RiskLevel.CRITICAL
                )
            elif _eco_status == 'DEGRADED':
                snapshot.signal['position_scale_degraded'] = 0.5
                logging.warning("[DFG-P1-05] 策略健康DEGRADED，仓位缩放至0.5")
        except (ImportError, AttributeError, KeyError, TypeError, RuntimeError) as _hc_e:
            logging.debug("[DFG-P1-05] 策略健康检查异常(非阻断): %s", _hc_e)
    return None


def check_counterparty_risks(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    limit_result = check_position_limit(snapshot, risk_service)
    if limit_result is not None and limit_result.is_block:
        return limit_result
    e13_result = check_e13_collusion(snapshot, risk_service)
    if e13_result is not None and e13_result.is_block:
        return e13_result
    health_result = check_strategy_health(snapshot, risk_service)
    if health_result is not None and health_result.is_block:
        return health_result
    return None


# =============================================================================
# Part 9: operational_risk.py - 运营风险检查
# =============================================================================

def check_strategy_status(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    return risk_service._check_strategy_status()


def check_rate_limit(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    return risk_service._check_rate_limit(snapshot.symbol)


def check_consecutive_loss_protection(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    result = risk_service._check_consecutive_loss_protection(snapshot.signal)
    if result.is_block:
        return result
    return None


def check_invariant_runtime(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    invariant_result = risk_service._check_invariant_runtime()
    if not invariant_result['all_passed']:
        logging.warning(
            "[RiskService] P2修复: 不变量检查未全部通过, violations=%s",
            invariant_result['violations'],
        )
    return None


def check_signal_validity(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    if not snapshot.is_valid:
        return RiskCheckResponse.block_result("signal_invalid", "信号无效")
    return None


def check_single_trade_risk(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    return risk_service._check_single_trade_risk(snapshot.signal)


def check_sharpe_iron_rule(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    return risk_service._check_sharpe_iron_rule(snapshot.signal)


def check_e7_residual_block(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    return risk_service._check_e7_residual_block(snapshot.signal)


def check_capital_sufficiency_in_trade(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    return risk_service._check_capital_sufficiency_in_trade(snapshot.signal)


def check_shadow_ev(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    if snapshot.action != "CLOSE":
        try:
            from strategy.shadow_strategy_facade import get_shadow_strategy_engine
            _sse = get_shadow_strategy_engine()
            if _sse is not None:
                if _sse.is_absolute_ev_paused():
                    risk_service._fire_alert('ev_bottom', {'signal': snapshot.signal})
                    return RiskCheckResponse.block_result(
                        "shadow_absolute_ev_paused",
                        "INV-IRN-05: 绝对EV底线突破，暂停新开仓（仅允许平仓）",
                        RiskLevel.CRITICAL
                    )
                if _sse.is_degradation_active():
                    risk_service._fire_alert('alpha_degradation_block', {
                        'signal': snapshot.signal,
                        'reason': 'INV-IRN-03: shadow_degradation_active blocks new open',
                    })
                    return RiskCheckResponse.block_result(
                        "shadow_degradation_active",
                        "INV-IRN-03: Alpha衰减降级中，禁止新开仓（仅允许平仓）",
                        RiskLevel.HIGH
                    )
        except (ImportError, AttributeError, KeyError, TypeError, RuntimeError) as _ev_err:
            logging.error("[R22-EP-01] 影子引擎EV/降级检查异常，fail-safe阻断: %s", _ev_err, exc_info=True)
            return RiskCheckResponse.block_result(
                "shadow_check_error",
                f"影子引擎检查异常，fail-safe阻断: {_ev_err}",
                RiskLevel.CRITICAL
            )
    return None


def check_operational_risks(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    ev_result = check_shadow_ev(snapshot, risk_service)
    if ev_result is not None:
        return ev_result
    cl_result = check_consecutive_loss_protection(snapshot, risk_service)
    if cl_result is not None:
        return cl_result
    status_result = check_strategy_status(snapshot, risk_service)
    if status_result is not None and status_result.is_block:
        return status_result
    valid_result = check_signal_validity(snapshot, risk_service)
    if valid_result is not None:
        return valid_result
    rate_result = check_rate_limit(snapshot, risk_service)
    if rate_result is not None and rate_result.is_block:
        return rate_result
    check_invariant_runtime(snapshot, risk_service)
    if snapshot.action != "CLOSE":
        single_risk_result = check_single_trade_risk(snapshot, risk_service)
        if single_risk_result is not None and single_risk_result.is_block:
            return single_risk_result
    if snapshot.action != "CLOSE":
        sharpe_result = check_sharpe_iron_rule(snapshot, risk_service)
        if sharpe_result is not None and sharpe_result.is_block:
            return sharpe_result
    if snapshot.action != "CLOSE":
        e7_result = check_e7_residual_block(snapshot, risk_service)
        if e7_result is not None and e7_result.is_block:
            return e7_result
    if snapshot.action != "CLOSE":
        capital_result = check_capital_sufficiency_in_trade(snapshot, risk_service)
        if capital_result is not None and capital_result.is_block:
            return capital_result
    return None


# =============================================================================
# Part 10: engine.py - 风控引擎主循环
# =============================================================================

class RiskEngine:
    def __init__(self, risk_service: Any):
        self._risk_service = risk_service
        self._log_dedup = LogDeduplicator()
        self._abnormal_detector = risk_service._abnormal_detector

    def check_before_trade(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        _check_dedup_key = f"{signal.get('instrument_id', '')}_{signal.get('direction', '')}_{signal.get('action', '')}"
        _cyclic_guard = CyclicDependencyGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        result = self._validate_signal(signal, _check_dedup_key, _cyclic_guard)
        if result is not None:
            return result
        snapshot = build_snapshot(signal, self._risk_service)
        with self._risk_service._risk_check_lock:
            try:
                safety_result = self._check_safety_meta_layer(signal)
                if safety_result.is_block:
                    return self._risk_service._record_result(safety_result)
                result = check_operational_risks(snapshot, self._risk_service)
                if result is not None:
                    return self._risk_service._record_result(result)
                result = check_market_risks(snapshot, self._risk_service)
                if result is not None:
                    return self._risk_service._record_result(result)
                result = check_counterparty_risks(snapshot, self._risk_service)
                if result is not None:
                    return self._risk_service._record_result(result)
                result = check_regulatory_risks(snapshot, self._risk_service)
                if result is not None:
                    return self._risk_service._record_result(result)
            except (KeyError, AttributeError, TypeError, ValueError, RuntimeError) as e:
                _exc_type = type(e).__name__
                if isinstance(e, (KeyError, AttributeError)):
                    _block_reason = f"check_config_error:{_exc_type}"
                elif isinstance(e, (TypeError, ValueError)):
                    _block_reason = f"check_param_error:{_exc_type}"
                else:
                    _block_reason = f"check_error:{_exc_type}"
                logging.error("[RiskEngine] 风控检查异常(%s)，fail-safe阻断: %s, signal={symbol=%s, direction=%s, action=%s}", _exc_type, e, signal.get('symbol', ''), signal.get('direction', ''), signal.get('action', ''))
                signal['risk_check_result'] = {
                    'result': 'BLOCK',
                    'reason': f"{_block_reason}: {e}",
                    'level': 'CRITICAL',
                }
                return RiskCheckResponse.block_result("check_error", f"风控检查异常，采用fail-safe阻断: {e}", RiskLevel.CRITICAL)
            result = self._detect_abnormal_trading_step(signal)
            if result is not None:
                return result
            return self._risk_service._record_and_publish(signal, _check_dedup_key, _cyclic_guard)

    def _validate_signal(self, signal: Dict[str, Any], check_dedup_key: str, cyclic_guard: Any) -> Optional[RiskCheckResponse]:
        return self._risk_service._validate_signal(signal, check_dedup_key, cyclic_guard)

    def _check_safety_meta_layer(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._risk_service._check_safety_meta_layer(signal)

    def _detect_abnormal_trading_step(self, signal: Dict[str, Any]) -> Optional[RiskCheckResponse]:
        _abnormal = self._risk_service.detect_abnormal_trading(
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
        return None

    @property
    def risk_service(self) -> Any:
        return self._risk_service

    @property
    def log_dedup(self) -> LogDeduplicator:
        return self._log_dedup

    @property
    def abnormal_detector(self) -> AbnormalTradeDetector:
        return self._abnormal_detector


# =============================================================================
# 导出
# =============================================================================

__all__ = [
    # snapshot
    "RiskSnapshot",
    # log_deduplicator
    "LogDeduplicator",
    # abnormal_trade_detector
    "AbnormalTradeDetector",
    # input_builder
    "build_snapshot",
    # result_handler
    "record_result",
    "record_and_publish",
    "record_block_with_context",
    # shared_checks
    "check_position_limit",
    "check_governance_violations",
    "check_margin_sufficiency",
    "check_cross_instrument_limit",
    "update_margin_ratio_override",
    "check_regulatory_risks",
    # market_risk
    "check_risk_ratio",
    "check_greeks_limits",
    "check_life_expectancy",
    "check_spread_degradation",
    "check_exchange_status",
    "check_price_limit",
    "check_expiry_risk",
    "check_auction_session",
    "check_market_risks",
    # counterparty_risk
    "check_e13_collusion",
    "check_strategy_health",
    "check_counterparty_risks",
    # operational_risk
    "check_strategy_status",
    "check_rate_limit",
    "check_consecutive_loss_protection",
    "check_invariant_runtime",
    "check_signal_validity",
    "check_single_trade_risk",
    "check_sharpe_iron_rule",
    "check_e7_residual_block",
    "check_capital_sufficiency_in_trade",
    "check_shadow_ev",
    "check_operational_risks",
    # engine
    "RiskEngine",
]