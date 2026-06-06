from __future__ import annotations

import logging
from typing import Dict, Any, Optional
from ali2026v3_trading.risk_engine.snapshot import RiskSnapshot
from ali2026v3_trading.risk_service import RiskCheckResponse, RiskLevel


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
            from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
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
        except Exception as _ev_err:
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
