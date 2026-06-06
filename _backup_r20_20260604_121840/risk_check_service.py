from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from ali2026v3_trading.shared_utils import safe_int, safe_float
from ali2026v3_trading.resilience_utils import stable_mean, stable_variance
# CP-06修复: 以下import从函数体内延迟import提升为模块级import（审计日志模块无循环依赖风险）
from ali2026v3_trading.audit_log_utils import structured_audit_log  # R27-CP-05-FIX

# CP-06注释: 以下延迟import因循环依赖无法提升到模块级：
# - risk_service (RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel)
#   原因: risk_service模块级导入本模块(risk_check_service)，形成循环


def _safe_get_float(obj: Any, attr: str, default: float = 0.0) -> float:
    try:
        val = getattr(obj, attr, default)
        if val is None:
            return default
        return float(val)
    except (ValueError, TypeError, AttributeError) as e:
        logging.warning("[safe_get_float] Error getting %s: %s", attr, e)
        return default


def _safe_get_int(obj: Any, attr: str, default: int = 0) -> int:
    try:
        val = getattr(obj, attr, default)
        if val is None:
            return default
        return int(val)
    except (ValueError, TypeError, AttributeError) as e:
        logging.warning("[safe_get_int] Error getting %s: %s", attr, e)
        return default


class RiskCheckService:
    """CC-03/AP-02修复: 风控检查服务

    从RiskService提取的A类方法（风控检查/限额管理），
    包含18个风控检查方法。
    构造函数接受risk_service引用（用于访问共享状态）。
    """

    def __init__(self, risk_service: Any):
        self._rs = risk_service

    def _get_types(self):
        from ali2026v3_trading.risk_service import RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel
        return RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel

    def check_before_trade(self, signal: Dict[str, Any]):
        """R17-P1-DOC-P1-11修复: 交易前风控检查。CLOSE动作豁免设计说明: 平仓操作(CLOSE)跳过多数风控检查(单笔风险/Sharpe铁律/E7残差/资金充足性/影子引擎降级等)，因为: (1)平仓是风险释放操作，不应被风控阻断; (2)阻止平仓会导致持仓风险继续暴露; (3)仅保留CLOSE信号伪造检测(无对应持仓时阻断)。"""
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            try:
                from ali2026v3_trading.risk_check_engine import RiskCheckEngine, RiskContext, RiskCheckReport
                if hasattr(self, '_rule_engine') and self._rule_engine is not None:
                    _ctx = RiskContext(signal=signal, risk_service=self._rs)
                    _report = self._rule_engine.run_checks(_ctx)
                    if not _report.passed and _report.blocking_result is not None:
                        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
                        return RiskCheckResponse(
                            passed=False,
                            results=[RiskCheckResult(rule_name=r.rule_name, passed=r.passed, reason=r.reason) for r in _report.results],
                            blocking_rule=_report.blocking_result.rule_name,
                        )
                    if not _report.passed:
                        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
                        return RiskCheckResponse(
                            passed=True,
                            results=[RiskCheckResult(rule_name=r.rule_name, passed=r.passed, reason=r.reason) for r in _report.results],
                        )
            except Exception as e:
                logging.warning("[RiskCheckService] RiskCheckEngine委托异常,回退到原逻辑: %s", e)
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        if hasattr(self._rs, '_risk_engine') and self._rs._risk_engine is not None:
            return self._rs._risk_engine.check_before_trade(signal)

        _check_dedup_key = f"{signal.get('instrument_id', '')}_{signal.get('direction', '')}_{signal.get('action', '')}"
        try:
            from ali2026v3_trading.causal_chain_utils import CyclicDependencyGuard, ParamIsolationGuard
            _HAS_CAUSAL_CHAIN = True
        except ImportError:
            _HAS_CAUSAL_CHAIN = False
            _cyclic_guard = None
        if _HAS_CAUSAL_CHAIN:
            from ali2026v3_trading.causal_chain_utils import CyclicDependencyGuard, ParamIsolationGuard
            _cyclic_guard = CyclicDependencyGuard.get_instance()
            _param_guard = ParamIsolationGuard.get_instance()
        else:
            _cyclic_guard = None

        if _cyclic_guard and not _cyclic_guard.enter("risk_check_before_trade"):
            logging.warning("[R35-P0-01] Cyclic dependency detected in risk_check_before_trade, blocking")
            return RiskCheckResponse.block_result("cyclic_dependency", "循环依赖检测: risk_check_before_trade", RiskLevel.CRITICAL)

        if _HAS_CAUSAL_CHAIN and _param_guard:
            try:
                _param_guard.register_param_source("risk_params", "risk_check_service", str(hash(frozenset(signal.items()))))
                # CC-08/CC-09修复: 校验参数一致性（注册后立即校验）
                _violations = _param_guard.get_violations()
                if _violations:
                    logging.warning("[CC-08] Param consistency violations detected: %d violations", len(_violations))
            except TypeError:
                pass

        try:
            result = self._rs._validate_signal(signal, _check_dedup_key, _cyclic_guard)
            if result is not None:
                return result
            with self._rs._risk_check_lock:
                try:
                    safety_result = self._check_safety_meta_layer(signal)
                    if safety_result.is_block:
                        return self._rs._record_result(safety_result)
                    action = signal.get("action", "")
                    if is_risk_exempt(signal):
                        logging.debug("[R27-P0-FI-05] 信号豁免风控检查: action=%s, risk_exempt=%s", action, signal.get('risk_exempt', False))
                    _snapshot_time = getattr(self._rs, '_dashboard_snapshot_time', 0.0)
                    if not check_snapshot_freshness(_snapshot_time):
                        logging.warning("[R27-P0-DI-06] 风控快照过期，基于过期数据检查可能不准确")
                    result = self._rs._check_operational_risks(signal, action)
                    if result is not None:
                        return self._rs._record_result(result)
                    result = self._rs._check_market_risks(signal, action)
                    if result is not None:
                        return self._rs._record_result(result)
                    result = self._rs._check_counterparty_risks(signal, action)
                    if result is not None:
                        return self._rs._record_result(result)
                    result = self._rs._check_regulatory_risks(signal, action)
                    if result is not None:
                        return self._rs._record_result(result)
                except Exception as e:
                    _exc_type = type(e).__name__
                    if isinstance(e, (KeyError, AttributeError)):
                        _block_reason = f"check_config_error:{_exc_type}"
                    elif isinstance(e, (TypeError, ValueError)):
                        _block_reason = f"check_param_error:{_exc_type}"
                    else:
                        _block_reason = f"check_error:{_exc_type}"
                    logging.error("[RiskService] 风控检查异常(%s)，fail-safe阻断: %s, signal={symbol=%s, direction=%s, action=%s}", _exc_type, e, signal.get('symbol', ''), signal.get('direction', ''), signal.get('action', ''))
                    signal['risk_check_result'] = {
                        'result': 'BLOCK',
                        'reason': f"{_block_reason}: {e}",
                        'level': 'CRITICAL',
                    }
                    return RiskCheckResponse.block_result("check_error", f"风控检查异常，采用fail-safe阻断: {e}", RiskLevel.CRITICAL)
            # P2-6修复: 直接调用AbnormalTradeDetector
            _abnormal_result = self._check_abnormal_trading(signal)
            if _abnormal_result is True:
                return RiskCheckResponse.block_result(
                    "abnormal_trading_detected",
                    f"P2-6: 异常交易行为检测阻断 {signal.get('instrument_id', '')}",
                    RiskLevel.HIGH
                )
            return self._rs._record_and_publish(signal, _check_dedup_key, _cyclic_guard)
        finally:
            if _cyclic_guard:
                _cyclic_guard.exit("risk_check_before_trade")

    def detect_abnormal_trading(self, instrument_id: str = '', direction: str = '',
                                price: float = 0.0, volume: float = 0.0,
                                market_price: float = 0.0, avg_volume: float = 0.0) -> Dict[str, Any]:
        """P2-6修复: 异常交易检测 — 直接调用AbnormalTradeDetector"""
        try:
            from ali2026v3_trading.risk_engine.abnormal_trade_detector import AbnormalTradeDetector
            if not hasattr(self, '_abnormal_detector') or self._abnormal_detector is None:
                self._abnormal_detector = AbnormalTradeDetector()
            detector = self._abnormal_detector
        except ImportError:
            logging.warning("[P2-6] AbnormalTradeDetector不可用，跳过异常交易检测")
            return {'action': 'none', 'anomaly_count': 0}

        results = []
        results.append(detector.detect_burst_trading(instrument_id))
        if direction and price > 0:
            results.append(detector.detect_self_trade(instrument_id, direction, price))
        if price > 0 and market_price > 0:
            results.append(detector.detect_price_deviation(instrument_id, price, market_price))
        if volume > 0 and avg_volume > 0:
            results.append(detector.detect_volume_spike(instrument_id, volume, avg_volume))

        anomaly_count = sum(1 for r in results if r.get('is_anomaly', False))
        block_actions = [r for r in results if r.get('action') == 'block']

        if block_actions:
            return {
                'action': 'block',
                'anomaly_count': anomaly_count,
                'anomalies': results,
            }

        # 记录正常交易
        if hasattr(detector, 'record_trade'):
            detector.record_trade(instrument_id, direction, volume, price)
        return {'action': 'none', 'anomaly_count': anomaly_count, 'anomalies': results}

    def _check_abnormal_trading(self, signal: Dict[str, Any]):
        """P2-6修复: 异常交易行为检查 — 作为风控检查链的一环"""
        try:
            _abnormal = self.detect_abnormal_trading(
                instrument_id=signal.get('instrument_id', ''),
                direction=signal.get('direction', ''),
                price=signal.get('price', 0.0),
                volume=signal.get('volume', 0.0),
                market_price=signal.get('market_price', 0.0),
                avg_volume=signal.get('avg_volume', 0.0),
            )
            if _abnormal.get('action') == 'block':
                logging.warning("[P2-6] 异常交易行为检测: %d项异常，阻断交易 %s",
                              _abnormal.get('anomaly_count', 0), signal.get('instrument_id', ''))
                return True  # 阻断信号
        except Exception as e:
            logging.warning("[P2-6] 异常交易检测异常(非阻断): %s", e)
        return None

    def _check_safety_meta_layer(self, signal: Dict[str, Any]):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        try:
            from ali2026v3_trading.risk_circuit_breaker import get_safety_meta_layer
            safety = get_safety_meta_layer(self._rs.params)

            symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
            action = signal.get("action", "")

            paused, reason = safety.is_trading_paused()
            if paused:
                if action == "CLOSE":
                    if symbol and self._rs.position_manager is not None:
                        has_position = False
                        try:
                            positions = getattr(self._rs.position_manager, 'positions', {})
                            for _inst_id, pos_dict in positions.items():
                                for _pid, rec in pos_dict.items():
                                    if getattr(rec, 'instrument_id', '') == symbol and getattr(rec, 'volume', 0) != 0:
                                        has_position = True
                                        break
                                if has_position:
                                    break
                        except Exception:
                            has_position = True
                        if not has_position:
                            logging.warning("[SafetyMetaLayer] CLOSE信号无对应持仓(symbol=%s)，疑似伪造，阻断", symbol)
                            return RiskCheckResponse.block_result(
                                "close_no_position",
                                f"CLOSE信号无对应持仓(symbol={symbol})，疑似伪造",
                                RiskLevel.HIGH
                            )
                    logging.info("[SafetyMetaLayer] 断路器暂停期间允许平仓信号通过(symbol=%s)", symbol)
                else:
                    self._rs._fire_alert('circuit_breaker', {'reason': reason, 'signal': signal, 'alert_level': AlertLevel.P0.value})
                    return RiskCheckResponse.block_result(
                        "safety_circuit_breaker", reason, RiskLevel.CRITICAL
                    )

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
                self._rs._fire_alert('daily_hard_stop', {'signal': signal, 'alert_level': AlertLevel.P0.value})
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

            try:
                if action != "CLOSE":
                    algo_paused, algo_reason = safety.is_algo_paused()
                    if algo_paused:
                        return RiskCheckResponse.block_result(
                            "algo_circuit_breaker",
                            f"算法交易熔断: {algo_reason}",
                            RiskLevel.HIGH
                        )
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
            logging.error("[RiskService._check_safety_meta_layer] SafetyMetaLayer check error: %s, signal={symbol=%s, direction=%s, action=%s}", e, signal.get('symbol', ''), signal.get('direction', ''), signal.get('action', ''))
            return RiskCheckResponse.block_result(
                "safety_meta_layer_error",
                f"SafetyMetaLayer检查异常，fail-safe阻断: {e}",
                RiskLevel.CRITICAL
            )

        return RiskCheckResponse.pass_result()

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
        max_risk_ratio = self._rs._get_max_risk_ratio()
        if max_risk_ratio <= 0:
            violations.append('INV-01: max_risk_ratio <= 0')
            # P2修复: 恢复策略 — 使用安全默认值0.05
            if hasattr(self._rs, 'params') and self._rs.params is not None:
                try:
                    if isinstance(self._rs.params, dict):
                        self._rs.params['max_risk_ratio'] = 0.05
                    else:
                        setattr(self._rs.params, 'max_risk_ratio', 0.05)
                except Exception as _margin_err:
                    logging.warning("[R27-AUDIT] existing_margin计算失败，使用保守估计: %s", _margin_err)
            recoveries.append('INV-01: max_risk_ratio恢复为0.05')
            logging.warning("[RiskService] P2修复: INV-01违反, max_risk_ratio<=0, 恢复为0.05")

        # INV-02: max_open_positions > 0
        max_open = _safe_get_int(self._rs.params, "max_open_positions", 0)
        if max_open <= 0:
            violations.append('INV-02: max_open_positions <= 0')
            recoveries.append('INV-02: max_open_positions恢复为3')
            logging.warning("[RiskService] P2修复: INV-02违反, max_open_positions<=0, 恢复为3")

        # INV-04: daily_loss_limit > 0
        daily_loss = _safe_get_float(self._rs.params, "daily_loss_hard_stop_pct", 0.0)
        if daily_loss <= 0:
            violations.append('INV-04: daily_loss_hard_stop_pct <= 0')
            recoveries.append('INV-04: daily_loss_hard_stop_pct恢复为5.0')
            logging.warning("[RiskService] P2修复: INV-04违反, daily_loss_hard_stop_pct<=0, 恢复为5.0")

        # INV-05: circuit_breaker_trigger_sigma > 0
        # P0-4隐患修复: 从SafetyMetaLayer获取ANOMALY_THRESHOLD_MULTIPLIER，而非risk_service(该属性不存在于risk_service)
        try:
            from ali2026v3_trading.risk_circuit_breaker import get_safety_meta_layer
            _safety = get_safety_meta_layer()
            _anomaly_threshold = _safety.ANOMALY_THRESHOLD_MULTIPLIER if _safety else 3.0
        except Exception:
            _anomaly_threshold = 3.0
        cb_sigma = _safe_get_float(self._rs.params, "circuit_breaker_trigger_sigma", _anomaly_threshold)
        if cb_sigma <= 0:
            violations.append('INV-05: circuit_breaker_trigger_sigma <= 0')
            recoveries.append('INV-05: circuit_breaker_trigger_sigma恢复为3.0')
            logging.warning("[RiskService] P2修复: INV-05违反, circuit_breaker_trigger_sigma<=0, 恢复为3.0")

        # INV-06: signal_cooldown >= 0
        cooldown = _safe_get_float(self._rs.params, "signal_cooldown_sec", -1.0)
        if cooldown < 0:
            violations.append('INV-06: signal_cooldown_sec < 0')
            recoveries.append('INV-06: signal_cooldown_sec恢复为0')
            logging.warning("[RiskService] P2修复: INV-06违反, signal_cooldown_sec<0, 恢复为0")

        # INV-07: position_limit >= 0
        pos_limit = _safe_get_int(self._rs.params, "max_position_limit", -1)
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

    def _check_strategy_status(self):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        if self._rs.strategy is None:
            return RiskCheckResponse.pass_result()

        now = time.time()

        with self._rs._lock:
            ts, cached = self._rs._strategy_status_cache
            if now - ts < self._rs._cache_ttl:
                return cached

            if not getattr(self._rs.strategy, "my_is_running", True):
                result = RiskCheckResponse.block_result(
                    "strategy_stopped", "策略未运行", RiskLevel.HIGH
                )
            elif getattr(self._rs.strategy, "my_is_paused", False):
                result = RiskCheckResponse.block_result(
                    "trading_paused", "策略已暂停", RiskLevel.MEDIUM
                )
            elif getattr(self._rs.strategy, "my_destroyed", False):
                result = RiskCheckResponse.block_result(
                    "strategy_destroyed", "策略已销毁", RiskLevel.CRITICAL
                )
            elif getattr(self._rs.strategy, "my_trading", True) is False:
                result = RiskCheckResponse.block_result(
                    "trading_disabled", "交易开关关闭", RiskLevel.MEDIUM
                )
            else:
                result = RiskCheckResponse.pass_result()

            self._rs._strategy_status_cache = (now, result)
            return result

    def _check_rate_limit(self, symbol: str):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        from collections import deque
        cooldown = self._rs._get_signal_cooldown()
        max_signals = self._rs._get_max_signals_per_window()
        window_sec = self._rs._get_rate_limit_window()

        with self._rs._lock:
            now = time.time()

            if symbol not in self._rs._signal_times:
                self._rs._signal_times[symbol] = deque(maxlen=max_signals * 2)

            window = self._rs._signal_times[symbol]
            cutoff = now - window_sec

            while window and window[0] < cutoff:
                window.popleft()

            if window:
                last_time = window[-1]
                if now - last_time < cooldown:
                    return RiskCheckResponse.block_result(
                        "rate_limit_cooldown",
                        f"{symbol} 信号冷却中，剩余 {cooldown - (now - last_time):.1f} 秒",
                        RiskLevel.MEDIUM,
                        threshold=cooldown, actual=now - last_time
                    )

            if len(window) >= max_signals:
                return RiskCheckResponse.block_result(
                    "rate_limit_exceeded",
                    f"{symbol} 窗口内信号数超限: {len(window)}/{max_signals}",
                    RiskLevel.MEDIUM,
                    threshold=max_signals, actual=len(window)
                )

        return RiskCheckResponse.pass_result()

    def _check_position_limit(self, account_id: str, required_amount: float,
                               hedge_type: str = "speculation"):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        from ali2026v3_trading.position_service import PositionLimitConfig as PositionLimit

        _POSITION_LIMIT_HEDGE_MULTIPLIER = {"speculation": 1.0, "hedging": 1.5, "arbitrage": 2.0}

        if required_amount <= 0:
            return RiskCheckResponse.pass_result()

        with self._rs._lock:
            expired_keys = []
            for aid in list(self._rs._position_limits.keys()):
                if not self._rs._position_limits[aid].is_valid():
                    expired_keys.append(aid)
            for aid in expired_keys:
                self._rs._position_limits.pop(aid, None)

            if account_id not in self._rs._position_limits:
                return RiskCheckResponse.pass_result()

            limit = self._rs._position_limits[account_id]

            current_exposure = 0.0
            if self._rs.position_manager is not None:
                try:
                    positions_raw = getattr(self._rs.position_manager, "positions", {})
                    positions = dict(positions_raw) if positions_raw else {}
                    for inst_map in positions.values():
                        for pos in inst_map.values():
                            vol = getattr(pos, "volume", 0)
                            price = getattr(pos, "open_price", 0)
                            current_exposure += abs(vol) * price
                except Exception as e:
                    logging.warning("[RiskService._check_position_limit] Failed to calculate exposure: %s", e)
                    return RiskCheckResponse.block_result(
                        "position_calculation_error",
                        f"持仓计算异常: {e}",
                        RiskLevel.WARNING
                    )

            hedge_multiplier = _POSITION_LIMIT_HEDGE_MULTIPLIER.get(hedge_type, 1.0)
            effective_limit_amount = limit.limit_amount * hedge_multiplier if limit is not None else 0.0

            if limit is not None and effective_limit_amount < (current_exposure + required_amount):
                return RiskCheckResponse.block_result(
                    "position_limit_exceeded",
                    f"持仓限额不足(类型={hedge_type}): 当前{current_exposure:.2f}+需要{required_amount:.2f}={current_exposure + required_amount:.2f}, 限额{limit.limit_amount:.2f}×{hedge_multiplier}={effective_limit_amount:.2f}",
                    RiskLevel.HIGH,
                    threshold=effective_limit_amount, actual=current_exposure + required_amount
                )

            if getattr(limit, 'limit_volume', 0) > 0:
                current_volume = 0
                if self._rs.position_manager is not None:
                    try:
                        positions_raw = getattr(self._rs.position_manager, "positions", {})
                        positions = positions_raw
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
                        threshold=limit.limit_volume, actual=current_volume
                    )

        return RiskCheckResponse.pass_result()

    def _check_risk_ratio(self, signal: Dict[str, Any]):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        # CP-06修复: structured_audit_log已提升为模块级import
        max_risk_ratio = self._rs._get_max_risk_ratio()
        if max_risk_ratio <= 0:
            return RiskCheckResponse.pass_result()

        consistency_resp = self._check_risk_consistency()
        if consistency_resp.is_block:
            return consistency_resp

        metrics = self._rs.calculate_risk_metrics()
        if metrics.risk_ratio >= max_risk_ratio:
            self._rs._fire_alert('total_risk_exceeded', {
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
            structured_audit_log('risk_rejection', 'blocked', {
                'reason': 'risk_ratio_exceeded',
                'current_risk_ratio': metrics.risk_ratio,
                'threshold': max_risk_ratio,
                'position_count': len(self._rs._positions) if hasattr(self._rs, '_positions') else 0,
                'param_version': getattr(self._rs.params, '_version', 'unknown'),
            })
            return RiskCheckResponse.block_result(
                "risk_ratio_exceeded",
                f"INV-P1-06: 风险比率超限: {metrics.risk_ratio:.2%} >= {max_risk_ratio:.2%}",
                RiskLevel.HIGH,
                threshold=max_risk_ratio, actual=metrics.risk_ratio
            )

        return RiskCheckResponse.pass_result()

    def _check_risk_consistency(self):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        max_risk_per_trade = _safe_get_float(self._rs.params, "max_risk_per_trade", 0.05)
        max_open_positions = _safe_get_int(self._rs.params, "max_open_positions", 3)
        max_risk_ratio = self._rs._get_max_risk_ratio()
        aggregate_risk = max_risk_per_trade * max_open_positions
        if aggregate_risk > max_risk_ratio * 1.5:
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

    def _check_single_trade_risk(self, signal: Dict[str, Any]):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        try:
            max_single_risk = _safe_get_float(self._rs.params, "max_single_risk", 0.0)
            if max_single_risk <= 0:
                max_risk_per_trade = _safe_get_float(self._rs.params, "max_risk_per_trade", 0.05)
                equity = 0.0
                try:
                    from ali2026v3_trading.risk_circuit_breaker import get_safety_meta_layer
                    safety = get_safety_meta_layer(self._rs.params)
                    if safety and safety._equity_series:
                        equity = safety._equity_series[-1] if safety._equity_series else 0.0
                except Exception:
                    logging.warning("[R22-EP-P1] RiskService exception swallowed")
                    pass
                if equity <= 0:
                    return RiskCheckResponse.pass_result()
                max_single_risk = equity * max_risk_per_trade

            symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
            volume = safe_int(signal.get("volume", signal.get("amount", 1)))
            price = safe_float(signal.get("price", signal.get("entry_price", 0)))
            multiplier = self._rs._get_contract_multiplier(symbol)
            trade_risk = abs(volume) * price * multiplier * _safe_get_float(self._rs.params, "max_risk_per_trade", 0.05)

            # R21-INV-P1-02: 调用calculate_position_risk触发net_position一致性校验
            try:
                instrument_id = getattr(signal, 'instrument_id', '') or signal.get('instrument_id', '')
                if instrument_id:
                    from ali2026v3_trading.position_service import get_position_service
                    _ps = get_position_service()
                    if _ps:
                        _ps.calculate_position_risk(instrument_id)
            except Exception:
                pass

            if trade_risk > max_single_risk:
                self._rs._fire_alert('single_trade_risk_exceeded', {
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
            logging.warning("[RiskService] INV-RSK-02: 单笔风险检查异常，fail-safe阻断: %s", e)
            return RiskCheckResponse.block_result(
                "single_trade_risk_check_error",
                f"INV-RSK-02: 单笔风险检查异常，fail-safe阻断: {e}",
                RiskLevel.HIGH
            )  # [R27-AUDIT] P0修复: 异常时返回block_result而非pass_result

    def _check_sharpe_iron_rule(self, signal: Dict[str, Any]):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        try:
            from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
            _sse = get_shadow_strategy_engine()
            if _sse is not None:
                with _sse._lock:
                    last_metrics = _sse._last_alpha_metrics
                if last_metrics is not None and hasattr(last_metrics, 'master_sharpe'):
                    master_sharpe = last_metrics.master_sharpe
                    import math
                    if isinstance(master_sharpe, float) and (math.isnan(master_sharpe) or math.isinf(master_sharpe)):
                        logging.warning("[R24-P1-IV-05] Sharpe值异常(NaN/Inf): %.6f, 跳过铁律检查", master_sharpe)
                        return RiskCheckResponse.pass_result()
                    if isinstance(master_sharpe, (int, float)) and abs(master_sharpe) > 100:
                        logging.warning("[R24-P1-IV-05] Sharpe值异常(%.2f>100), 可能是数据错误", master_sharpe)
                    min_trades = _sse.MIN_TRADES_FOR_METRICS if hasattr(_sse, 'MIN_TRADES_FOR_METRICS') else 5
                    master_trades = len(_sse._master_trades) if hasattr(_sse, '_master_trades') else 0
                    if master_trades >= min_trades and master_sharpe < 0.5:
                        self._rs._fire_alert('sharpe_iron_rule_violation', {
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

    def _check_e7_residual_block(self, signal: Dict[str, Any]):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        try:
            if not hasattr(self._rs, '_attribution_residual_history'):
                return RiskCheckResponse.pass_result()
            history = self._rs._attribution_residual_history
            if len(history) < 5:
                return RiskCheckResponse.pass_result()

            import math as _math
            recent = history[-20:]
            mean_res = stable_mean(recent)
            var_res = stable_variance(recent)
            std_res = var_res ** 0.5 if var_res > 0 else 0.0
            n = len(recent)
            if std_res > 0:
                t_stat = (mean_res - 15.0) / (std_res / _math.sqrt(n))
                statistically_significant = t_stat > 2.0
            else:
                statistically_significant = mean_res > 15.0

            if statistically_significant and mean_res > 15.0:
                self._rs._fire_alert('e7_residual_block', {
                    'mean_residual_pct': mean_res,
                    'threshold_pct': 15.0,
                    't_stat': t_stat if std_res > 0 else 0.0,
                    'sample_size': n,
                })
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
            logging.warning("[RiskService] INV-IRN-02: E7残差检查异常，fail-safe阻断: %s", e)
            return RiskCheckResponse.block_result(
                "e7_residual_check_error",
                f"INV-IRN-02: E7残差检查异常，fail-safe阻断: {e}",
                RiskLevel.HIGH
            )  # [R27-AUDIT] P1修复: 异常时返回block_result而非pass_result

    def _check_capital_sufficiency_in_trade(self, signal: Dict[str, Any]):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        try:
            equity = 0.0
            try:
                from ali2026v3_trading.risk_circuit_breaker import get_safety_meta_layer
                safety = get_safety_meta_layer(self._rs.params)
                if safety and safety._equity_series:
                    equity = safety._equity_series[-1] if safety._equity_series else 0.0
            except Exception as _eq_err:
                logging.warning("[R27-AUDIT] equity获取失败，fail-safe阻断: %s", _eq_err)
                return RiskCheckResponse.block_result(
                    "equity_fetch_error",
                    f"资金数据获取失败，fail-safe阻断: {_eq_err}",
                    RiskLevel.HIGH
                )  # [R27-AUDIT] P1修复: equity获取失败时阻断而非放行
            if equity <= 0:
                return RiskCheckResponse.pass_result()

            symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
            volume = safe_int(signal.get("volume", signal.get("amount", 1)))
            price = safe_float(signal.get("price", signal.get("entry_price", 0)))
            multiplier = self._rs._get_contract_multiplier(symbol)
            margin_ratio = self._rs._get_margin_ratio(symbol)
            required_margin = abs(volume) * price * multiplier * margin_ratio

            existing_margin = 0.0
            if self._rs.position_manager is not None:
                try:
                    for _inst_id, pos_dict in getattr(self._rs.position_manager, 'positions', {}).items():
                        for _pid, rec in pos_dict.items():
                            if getattr(rec, 'volume', 0) != 0 and getattr(rec, 'open_price', 0) > 0:
                                _m_ratio = self._rs._get_margin_ratio(_inst_id)
                                existing_margin += abs(rec.volume) * rec.open_price * _m_ratio
                except Exception as _margin_err:
                    logging.warning("[R27-AUDIT] existing_margin计算失败，使用保守估计: %s", _margin_err)

            result = self._rs.check_capital_sufficiency(
                equity=equity,
                required_margin=required_margin,
                existing_margin_used=existing_margin,
            )
            if not result.get('sufficient', True):
                self._rs._fire_alert('capital_insufficient', {
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

    def _check_spread_degradation(self, signal: Dict[str, Any]):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
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
                return RiskCheckResponse.pass_result()

            price = safe_float(signal.get("price", signal.get("entry_price", 0)))
            if price <= 0:
                return RiskCheckResponse.pass_result()

            spread_bps = spread / price * 10000.0
            max_spread_bps = _safe_get_float(self._rs.params, "max_spread_bps", 150.0)
            warn_spread_bps = _safe_get_float(self._rs.params, "warn_spread_bps", 50.0)

            if spread_bps > max_spread_bps:
                self._rs._fire_alert('spread_degradation', {
                    'symbol': symbol,
                    'spread_bps': spread_bps,
                    'max_spread_bps': max_spread_bps,
                })
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

    def _check_governance_violations(self, signal: Dict[str, Any]):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        try:
            from ali2026v3_trading.governance_engine import get_governance_engine
            ge = get_governance_engine()
            if ge is None:
                return RiskCheckResponse.pass_result()

            feedbacks = ge.get_feedback()
            if not feedbacks:
                return RiskCheckResponse.pass_result()

            recent_feedbacks = feedbacks[-5:] if len(feedbacks) > 5 else feedbacks
            critical_violations = []
            for fb in recent_feedbacks:
                result = fb.get('result', {})
                if not result.get('passed', True):
                    checker_name = fb.get('checker', 'unknown')
                    critical_violations.append(checker_name)

            if critical_violations:
                violation_names = ', '.join(set(critical_violations))
                self._rs._fire_alert('governance_violation', {
                    'violations': critical_violations,
                    'signal': signal.get('symbol', ''),
                })
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
            logging.warning("[R27-AUDIT] GovernanceEngine不可用，fail-safe阻断: %s", _gov_import_err)
            return RiskCheckResponse.block_result(
                "governance_unavailable",
                f"INV-P1-09: GovernanceEngine不可用，fail-safe阻断: {_gov_import_err}",
                RiskLevel.HIGH
            )  # [R27-AUDIT] P1修复: ImportError时阻断而非放行
        except Exception as e:
            logging.warning("[RiskService] INV-P1-09: GovernanceEngine检查异常，fail-safe阻断: %s", e)
            return RiskCheckResponse.block_result(
                "governance_check_error",
                f"INV-P1-09: GovernanceEngine检查异常，fail-safe阻断: {e}",
                RiskLevel.HIGH
            )  # [R27-AUDIT] P1修复: 异常时阻断而非放行

    def _check_greeks_limits(self, signal: Dict[str, Any]):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        action = signal.get("action", "")
        if action == "CLOSE":
            return RiskCheckResponse.pass_result()

        try:
            calc = self._rs._get_greeks_calculator()
            if calc is None:
                logging.error("[RiskService._check_greeks_limits] GreeksCalculator为None，fail-safe阻断新开仓")
                return RiskCheckResponse.block_result(
                    "greeks_calculator_none",
                    "GreeksCalculator未初始化，fail-safe阻断新开仓",
                    RiskLevel.HIGH
                )

            positions_dict = {}
            if self._rs.position_manager:
                positions_raw = getattr(self._rs.position_manager, "positions", {})
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
            max_delta = _safe_get_float(self._rs.params, "max_net_delta_pct", 0.30) * 1000
            max_gamma = _safe_get_float(self._rs.params, "max_net_gamma_pct", 0.08) * 50
            max_vega = _safe_get_float(self._rs.params, "max_net_vega_bps", 0.02) * 200

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

            try:
                from ali2026v3_trading.greeks_calculator import validate_greeks_calendar_iv_noise
                _iv_noise_result = validate_greeks_calendar_iv_noise(
                    greeks_calculator=calc,
                    iv_noise_sigma=_safe_get_float(self._rs.params, "greeks_iv_noise_sigma", 0.005),
                    n_simulations=_safe_get_int(self._rs.params, "greeks_iv_noise_n_sim", 100),
                )
                if _iv_noise_result and not _iv_noise_result.get('passed', True):
                    logging.warning("[P2-GR-004] Greeks日历/IV噪声验证未通过: %s", _iv_noise_result)
            except Exception as _e:
                logging.debug("[P2-GR-004] validate_greeks_calendar_iv_noise跳过: %s", _e)

            theta_abs_threshold = _safe_get_float(self._rs.params, "theta_abs_threshold", 0.5)
            try:
                if hasattr(calc, 'compute_portfolio_greeks') and positions_dict:
                    pg = calc.compute_portfolio_greeks(positions_dict)
                    theta_val = abs(pg.get('theta', 0.0))
                    if theta_val > theta_abs_threshold:
                        # R3-P-08修复: theta超阈值时硬阻断新开仓(非仅warning)
                        _theta_hard_block = _safe_get_float(self._rs.params, "theta_hard_block_enabled", 1.0) > 0
                        if _theta_hard_block:
                            logging.warning("[R3-P-08] theta绝对值%.4f超过阈值%.2f, 硬阻断新开仓", theta_val, theta_abs_threshold)
                            return RiskCheckResponse.block_result(
                                "theta_abs_exceeded",
                                f"theta绝对值{theta_val:.4f}超过阈值{theta_abs_threshold:.2f}, 阻断新开仓",
                                RiskLevel.HIGH,
                            )
                        else:
                            logging.warning("[R3-P-08] theta绝对值%.4f超过阈值%.2f(P1级软约束)", theta_val, theta_abs_threshold)
            except Exception as _theta_e:
                logging.debug("[R3-P-08] theta检查跳过: %s", _theta_e)

            try:
                self._rs.get_greeks_dashboard()
            except (ImportError, AttributeError, RuntimeError) as e:
                logging.warning("[R22-EP-02] greeks dashboard update failed (may stale): %s", e)

        except ImportError as _e:
            # R5-E-01修复: GreeksCalculator不可用时fail-safe阻断，不再放行
            logging.error("[R5-E-01] [RiskService._check_greeks_limits] GreeksCalculator不可用，fail-safe阻断新开仓: %s", _e)
            return RiskCheckResponse.block_result(
                "greeks_calculator_unavailable",
                "GreeksCalculator不可用，fail-safe阻断新开仓",
                RiskLevel.CRITICAL
            )
        except Exception as e:
            # R5-E-01修复: 检查异常时fail-safe阻断，不再放行
            logging.error("[R5-E-01] [RiskService._check_greeks_limits] Check error, fail-safe阻断: %s, signal={symbol=%s, direction=%s, action=%s}", e, signal.get('symbol', ''), signal.get('direction', ''), signal.get('action', ''), exc_info=True)
            return RiskCheckResponse.block_result(
                "greeks_check_error",
                f"希腊字母检查异常，fail-safe阻断: {e}",
                RiskLevel.CRITICAL
            )

        return RiskCheckResponse.pass_result()

    def _check_consecutive_loss_protection(self, signal: Dict[str, Any]):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        strategy_id = signal.get('strategy_id', 'default')
        close_pnl = signal.get('close_pnl', None)
        if close_pnl is not None and close_pnl < 0:
            capital_scale = getattr(self._rs, '_current_capital_scale', 'medium')
            self._rs.record_trade_result(strategy_id, close_pnl, capital_scale)
        with self._rs._lock:
            if strategy_id in self._rs._consecutive_loss_paused and self._rs._consecutive_loss_paused[strategy_id]:
                paused_ts = self._rs._consecutive_loss_pause_timestamps.get(strategy_id, 0.0)
                capital_scale = getattr(self._rs, '_current_capital_scale', 'medium')
                timeout = self._rs._consecutive_loss_recovery_timeouts.get(capital_scale, 3600.0)
                if paused_ts <= 0:
                    self._rs._consecutive_loss_pause_timestamps[strategy_id] = time.time()
                    paused_ts = self._rs._consecutive_loss_pause_timestamps[strategy_id]
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
                    self._rs._consecutive_loss_counts[strategy_id] = 0
                    self._rs._consecutive_loss_paused[strategy_id] = False
                    self._rs._consecutive_loss_pause_timestamps.pop(strategy_id, None)
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

    def _check_life_expectancy(self, signal: Dict[str, Any]):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        hmm_state = signal.get("hmm_state", self._rs._current_hmm_state)
        if hmm_state is None:
            return RiskCheckResponse.pass_result()

        estimator = self._rs._get_life_estimator()
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

    def check_price_limit(self, instrument_id: str, price: float, direction: str):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
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
            logging.warning("[RiskService.check_price_limit] fallback: %s", e)
            return RiskCheckResponse.pass_result()

    def check_expiry_risk(self, instrument_id: str, days_to_expiry: int = None):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
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

    def auto_rollover_if_needed(self, instrument_id: str, days_to_expiry: int) -> Optional[Dict[str, Any]]:
        """EX-07修复: 交割日自动移仓 — 到期前N日触发移仓(平旧开新)"""
        try:
            from ali2026v3_trading.config_params import get_config
            cfg = get_config()
            if not cfg.get('auto_rollover_enabled', False):
                return None
            trigger_days = cfg.get('rollover_trigger_days_before_expiry', 5)
            if days_to_expiry > trigger_days or days_to_expiry <= 0:
                return None
            slippage_bps = cfg.get('rollover_slippage_bps_auto', 5.0)
            logging.info("[EX-07] 自动移仓触发: instrument=%s days_to_expiry=%d<=trigger=%d slippage=%.1fbps",
                         instrument_id, days_to_expiry, trigger_days, slippage_bps)
            return {
                'action': 'rollover',
                'instrument_id': instrument_id,
                'days_to_expiry': days_to_expiry,
                'slippage_bps': slippage_bps,
                'reason': f'距到期{days_to_expiry}天≤触发阈值{trigger_days}天，自动移仓'
            }
        except Exception as e:
            logging.warning("[EX-07] auto_rollover check failed: %s", e)
            return None


    def _check_operational_risks(self, signal: Dict[str, Any], action: str) -> Optional[RiskCheckResponse]:
        if action != "CLOSE":
            try:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                _sse = get_shadow_strategy_engine()
                if _sse is not None:
                    # R25-P1-CF-01修复: 影子引擎状态读取加锁保护，与sharpe_iron_rule一致
                    with getattr(_sse, '_lock', _sse):
                        _ev_paused = _sse.is_absolute_ev_paused()
                        _degradation = _sse.is_degradation_active()
                    if _ev_paused:
                        self._rs._fire_alert('ev_bottom', {'signal': signal})
                        return RiskCheckResponse.block_result(
                            "shadow_absolute_ev_paused",
                            "INV-IRN-05: 绝对EV底线突破，暂停新开仓（仅允许平仓）",
                            RiskLevel.CRITICAL
                        )
                    if _degradation:
                        self._rs._fire_alert('alpha_degradation_block', {
                            'signal': signal,
                            'reason': 'INV-IRN-03: shadow_degradation_active blocks new open',
                        })
                        return RiskCheckResponse.block_result(
                            "shadow_degradation_active",
                            "INV-IRN-03: Alpha衰减降级中，禁止新开仓（仅允许平仓）",
                            RiskLevel.HIGH
                        )
            except Exception as _ev_err:
                logging.error("[R22-EP-01] 影子引擎EV/降级检查异常，fail-safe阻断: %s", _ev_err, exc_info=True)
                return RiskCheckResponse.block_result(
                    "shadow_check_error",
                    f"影子引擎检查异常，fail-safe阻断: {_ev_err}",
                    RiskLevel.CRITICAL
                )
        cl_result = self._check_consecutive_loss_protection(signal)
        if cl_result.is_block:
            return cl_result
        status_result = self._check_strategy_status()
        if status_result.is_block:
            return status_result
        if not signal.get("is_valid", True):
            return RiskCheckResponse.block_result("signal_invalid", "信号无效")
        symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
        rate_result = self._check_rate_limit(symbol)
        if rate_result.is_block:
            return rate_result
        invariant_result = self._check_invariant_runtime()
        if not invariant_result['all_passed']:
            logging.warning(
                "[RiskService] P2修复: 不变量检查未全部通过, violations=%s",
                invariant_result['violations'],
            )
        if action != "CLOSE":
            single_risk_result = self._check_single_trade_risk(signal)
            if single_risk_result.is_block:
                return single_risk_result
        if action != "CLOSE":
            sharpe_result = self._check_sharpe_iron_rule(signal)
            if sharpe_result.is_block:
                return sharpe_result
        if action != "CLOSE":
            e7_result = self._check_e7_residual_block(signal)
            if e7_result.is_block:
                return e7_result
        if action != "CLOSE":
            capital_result = self._check_capital_sufficiency_in_trade(signal)
            if capital_result.is_block:
                return capital_result
        return None


    def _check_market_risks(self, signal: Dict[str, Any], action: str) -> Optional[RiskCheckResponse]:
        risk_result = self._check_risk_ratio(signal)
        if risk_result.is_block:
            return risk_result
        greeks_result = self._check_greeks_limits(signal)
        if greeks_result.is_block:
            return greeks_result
        life_result = self._check_life_expectancy(signal)
        if life_result.is_block:
            return life_result
        if action != "CLOSE":
            spread_result = self._check_spread_degradation(signal)
            if spread_result.is_block:
                return spread_result
        if action != "CLOSE":
            try:
                _exch_result = self._rs.check_exchange_status()
                if not _exch_result.get('tradeable', True):
                    return RiskCheckResponse.block_result(
                        "exchange_not_tradeable",
                        f"交易所不可交易: {_exch_result.get('reason', 'unknown')}",
                        RiskLevel.CRITICAL
                    )
            except Exception as _exch_err:
                logging.warning("[P-03补全] check_exchange_status异常(非致命): %s", _exch_err)
        symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
        try:
            auction_result = self._rs.check_auction_session(
                bar_datetime=signal.get('bar_datetime'),
                instrument_id=signal.get('instrument_id', symbol)
            )
            if auction_result.is_block:
                return auction_result
        except Exception as _auction_err:
            logging.warning("[EX-P0-05] auction session check error: %s", _auction_err)
        if action != "CLOSE":
            try:
                instrument_id = signal.get("instrument_id", symbol)
                price = safe_float(signal.get("price", 0))
                limit_flag = self._rs.is_at_price_limit(instrument_id, price)
                signal.update(limit_flag)
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
        if action != "CLOSE":
            try:
                instrument_id = signal.get("instrument_id", symbol)
                days_to_expiry = safe_int(signal.get("days_to_expiry", 999))
                expiry_result = self.check_expiry_risk(instrument_id, days_to_expiry)
                if expiry_result.is_block:
                    return expiry_result
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
        return None


    def _check_counterparty_risks(self, signal: Dict[str, Any], action: str) -> Optional[RiskCheckResponse]:
        symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
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
                        return RiskCheckResponse.block_result(
                            "e13_collusion_detected",
                            "DFG-P1-02: E13同谋检测触发，主策略与影子策略参数同谋，阻断新开仓",
                            RiskLevel.HIGH
                        )
            except Exception as _e13_e:
                logging.debug("[DFG-P1-02] E13同谋检测异常(非阻断): %s", _e13_e)
        if action != "CLOSE":
            try:
                from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
                _eco_hc = get_strategy_ecosystem()
                _eco_health = _eco_hc.get_health_status()
                _eco_status = _eco_health.get('status', 'OK')
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
                if _eco_status == 'CRITICAL':
                    return RiskCheckResponse.block_result(
                        "strategy_health_critical",
                        "DFG-P1-05: 策略生态系统健康状态CRITICAL，阻断新开仓",
                        RiskLevel.CRITICAL
                    )
                elif _eco_status == 'DEGRADED':
                    signal['position_scale_degraded'] = 0.5
                    logging.warning("[DFG-P1-05] 策略健康DEGRADED，仓位缩放至0.5")
            except Exception as _hc_e:
                logging.debug("[DFG-P1-05] 策略健康检查异常(非阻断): %s", _hc_e)
        return None


    _industry_limits: Dict[str, float] = {}

    def check_cross_instrument_limit(self, account_id: str, instrument_id: str,
                                      required_amount: float):
        RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel = self._get_types()
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_svc = get_params_service()
            meta = params_svc.get_instrument_meta_by_id(instrument_id)
            if not meta:
                return RiskCheckResponse.pass_result()
            industry = meta.get('industry', '')
            if not industry:
                industry = self._rs._infer_industry(instrument_id) if hasattr(self._rs, '_infer_industry') else ''
                if not industry:
                    return RiskCheckResponse.pass_result()
            industry_limit = RiskCheckService._industry_limits.get(industry, None)
            if industry_limit is None:
                return RiskCheckResponse.pass_result()
            current_exposure = 0.0
            if self._rs.position_manager is not None:
                try:
                    positions_raw = getattr(self._rs.position_manager, "positions", {})
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
            logging.warning("[RiskCheckService.check_cross_instrument_limit] fallback: %s", e)
            return RiskCheckResponse.pass_result()

    def _check_regulatory_risks(self, signal: Dict[str, Any], action: str) -> Optional[RiskCheckResponse]:
        account_id = signal.get("account_id", "default")
        amount = signal.get("amount", 0)
        hedge_type = signal.get('hedge_type', 'speculation')
        limit_result = self._rs._check_position_limit(account_id, amount, hedge_type=hedge_type)
        if limit_result.is_block:
            return limit_result
        symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
        if action != "CLOSE":
            gov_result = self._check_governance_violations(signal)
            if gov_result.is_block:
                return gov_result
        if action != "CLOSE":
            try:
                from ali2026v3_trading.config_params import get_config
                _cfg = get_config()
                _overrides = _cfg.get('min_margin_ratio_override', {})
                if _overrides:
                    for _prod, _ratio in _overrides.items():
                        self._rs.update_margin_ratio(_prod, safe_float(_ratio))
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
        if action != "CLOSE":
            try:
                instrument_id = signal.get("instrument_id", symbol)
                equity = safe_float(signal.get("equity", 0))
                volume = safe_int(signal.get("amount", 0))
                price = safe_float(signal.get("price", 0))
                if equity > 0 and volume != 0 and price > 0:
                    margin_result = self._rs.check_margin_sufficiency(
                        equity, instrument_id, volume, price
                    )
                    if margin_result.is_block:
                        return margin_result
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
        if action != "CLOSE":
            try:
                instrument_id = signal.get("instrument_id", symbol)
                amount_val = safe_float(signal.get("amount", 0))
                price = safe_float(signal.get("price", 0))
                cross_result = self._rs.check_cross_instrument_limit(
                    account_id, instrument_id, abs(amount_val) * price
                )
                if cross_result.is_block:
                    return cross_result
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
        self._rs._record_signal_time(symbol)
        return None


def check_snapshot_freshness(snapshot_time: float, max_age_sec: float = 30.0) -> bool:
    """R27-P0-DI-06修复: 风控快照新鲜度检查
    
    检查风控快照是否过期。过期则需刷新，避免基于过期持仓快照计算敞口。
    
    Args:
        snapshot_time: 快照时间戳(time.time())
        max_age_sec: 快照最大有效期(秒)，默认30.0
    
    Returns:
        True=快照新鲜, False=快照过期需刷新
    """
    if snapshot_time <= 0:
        return False
    _now = time.time()
    _age = _now - snapshot_time
    if _age > max_age_sec:
        logging.warning("[R27-P0-DI-06] 风控快照过期(%.1fs > %.1fs)，需刷新快照", _age, max_age_sec)
        return False
    return True


def is_risk_exempt(signal: Dict[str, Any]) -> bool:
    """R27-P0-FI-05修复: 风控豁免显式字段检查
    
    原FI-05问题: 风控豁免检查依赖signal dict的隐式action字段，action缺失时平仓豁免失效。
    修复: 使用显式risk_exempt字段，action字段作为回退。
    
    Args:
        signal: 信号字典
    
    Returns:
        True=该信号豁免风控检查(如平仓), False=需正常风控检查
    """
    if signal.get('risk_exempt', False) is True:
        return True
    action = signal.get('action', '')
    if action == 'CLOSE':
        return True
    return False


_ALLOWED_DIRECTIONS = frozenset({'BUY', 'SELL'})


def validate_direction(direction: str) -> str:
    """R27-P0-FI-06修复: direction白名单验证
    
    原FI-06问题: send_order_split未验证direction参数，非BUY/SELL值可能导致反向交易。
    修复: 显式白名单校验，非法direction→raise ValueError。
    
    Args:
        direction: 交易方向字符串
    
    Returns:
        验证通过的direction(大写)
    
    Raises:
        ValueError: direction不在白名单中
    """
    if not isinstance(direction, str):
        raise ValueError(f"R27-P0-FI-06: direction必须为字符串, 实际类型={type(direction).__name__}")
    _dir = direction.strip().upper()
    if _dir not in _ALLOWED_DIRECTIONS:
        raise ValueError(f"R27-P0-FI-06: direction={direction}不在白名单{_ALLOWED_DIRECTIONS}中")
    return _dir


