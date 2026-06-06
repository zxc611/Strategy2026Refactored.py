from __future__ import annotations

import time
import logging
from typing import Dict, Any, Optional
from ali2026v3_trading.risk_service import RiskCheckResponse, RiskLevel

try:
    from ali2026v3_trading.audit_log_utils import structured_audit_log  # R27-CP-05-FIX
except ImportError:
    structured_audit_log = None


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
