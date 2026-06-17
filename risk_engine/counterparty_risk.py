# MODULE_ID: M1-226
from __future__ import annotations

import logging
from typing import Any, Optional
from ali2026v3_trading.risk_engine.snapshot import RiskSnapshot
from ali2026v3_trading.risk.risk_service import RiskCheckResponse, RiskLevel
from ali2026v3_trading.risk_engine.shared_checks import (
    check_position_limit,
    check_governance_violations,
    check_margin_sufficiency,
    check_cross_instrument_limit,
    update_margin_ratio_override,
    check_regulatory_risks,
)


def check_e13_collusion(snapshot: RiskSnapshot, risk_service: Any) -> Optional[RiskCheckResponse]:
    if snapshot.action != "CLOSE":
        try:
            from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
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
                        from ali2026v3_trading.infra.event_bus import get_global_event_bus
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
            from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
            _eco_hc = get_strategy_ecosystem()
            _eco_health = _eco_hc.get_health_status()
            _eco_status = _eco_health.get('status', 'OK')
            try:
                from ali2026v3_trading.infra.event_bus import get_global_event_bus
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
