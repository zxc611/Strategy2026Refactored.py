from __future__ import annotations

from typing import Any, Optional
from ali2026v3_trading.risk_engine.snapshot import RiskSnapshot
from ali2026v3_trading.risk_service import RiskCheckResponse
from ali2026v3_trading.risk_engine.shared_checks import (
    check_position_limit,
    check_governance_violations,
    check_margin_sufficiency,
    check_cross_instrument_limit,
    update_margin_ratio_override,
    check_regulatory_risks,
)
