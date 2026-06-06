from __future__ import annotations

import logging
from typing import Any, Optional
from ali2026v3_trading.risk_engine.snapshot import RiskSnapshot
from ali2026v3_trading.risk_service import RiskCheckResponse

try:
    from ali2026v3_trading.shared_utils import safe_int, safe_float
except ImportError:
    safe_int = lambda x, d=0: d if x is None else int(x)
    safe_float = lambda x, d=0.0: d if x is None else float(x)


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
    except Exception:
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
    except Exception:
        logging.warning("[R22-EP-P1] RiskService exception swallowed")
    return None


def update_margin_ratio_override(snapshot: RiskSnapshot, risk_service: Any) -> None:
    try:
        from ali2026v3_trading.config_params import get_config
    except ImportError:
        return
    try:
        _cfg = get_config()
        _overrides = _cfg.get('min_margin_ratio_override', {})
        if _overrides:
            for _prod, _ratio in _overrides.items():
                risk_service.update_margin_ratio(_prod, safe_float(_ratio))
    except Exception:
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
