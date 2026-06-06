from __future__ import annotations

import logging
from typing import Dict, Any, Optional
from ali2026v3_trading.risk_engine.snapshot import RiskSnapshot
from ali2026v3_trading.risk_service import RiskCheckResponse, RiskLevel

try:
    from ali2026v3_trading.shared_utils import safe_int, safe_float
except ImportError:
    safe_int = lambda x, d=0: d if x is None else int(x)
    safe_float = lambda x, d=0.0: d if x is None else float(x)


def build_snapshot(signal: Dict[str, Any], risk_service: Any) -> RiskSnapshot:
    params = getattr(risk_service, 'params', None)
    strategy = getattr(risk_service, 'strategy', None)
    position_manager = getattr(risk_service, 'position_manager', None)
    try:
        from ali2026v3_trading.risk_service import get_safety_meta_layer
        safety_meta_layer = get_safety_meta_layer(params)
    except Exception:
        safety_meta_layer = None
    return RiskSnapshot.from_signal(
        signal=signal,
        params=params,
        strategy=strategy,
        position_manager=position_manager,
        safety_meta_layer=safety_meta_layer,
    )
