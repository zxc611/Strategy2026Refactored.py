# MODULE_ID: M1-230
from __future__ import annotations

import logging
from typing import Dict, Any, Optional
from ali2026v3_trading.risk_engine.snapshot import RiskSnapshot
from ali2026v3_trading.risk.risk_service import RiskCheckResponse, RiskLevel

try:
    from ali2026v3_trading.infra.shared_utils import safe_int, safe_float
except ImportError:
    safe_int = lambda x, d=0: d if x is None else int(x)
    safe_float = lambda x, d=0.0: d if x is None else float(x)

try:
    from ali2026v3_trading.risk._utils import safe_get_float, safe_get_int
except ImportError:
    safe_get_float = lambda obj, k, d=0.0: d
    safe_get_int = lambda obj, k, d=0: d


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
