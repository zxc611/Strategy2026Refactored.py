"""Risk-Position Bridge Interface — Phase 1 architectural refactoring.

Breaks the circular dependency between risk_service.py and position_service.py
by introducing a protocol-based interface layer.

Before: position_service → risk_service (9 direct refs)
        risk_service → position_service (6 lazy imports)
After:  position_service → RiskPositionBridge (protocol)
        risk_service → PositionBridge (protocol)
        Both protocols defined here, no circular import.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from enum import Enum, auto
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


class BridgeRiskLevel(Enum):
    PASS = "PASS"
    BLOCK = "BLOCK"
    WARNING = "WARNING"


@dataclass(slots=True)
class BridgeRiskResponse:
    level: BridgeRiskLevel = BridgeRiskLevel.PASS
    reason: str = ""
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BridgePositionLimit:
    account_id: str = ""
    limit_amount: float = 0.0
    effective_until: Optional[float] = None


class RiskPositionBridge:
    """Interface that position_service uses to communicate with risk_service.

    This breaks the direct dependency on RiskService class.
    RiskService implements this interface; position_service depends only on this.
    """

    def check_position_limit(self, account_id: str, required_amount: float) -> BridgeRiskResponse:
        raise NotImplementedError

    def set_position_limit(self, account_id: str, limit_amount: float,
                           effective_until: Optional[float] = None) -> None:
        raise NotImplementedError

    def get_position_limit(self, account_id: str) -> Optional[BridgePositionLimit]:
        raise NotImplementedError

    def get_safety_meta_layer(self, params: Any = None, strategy_id: str = "") -> Any:
        raise NotImplementedError

    def check_cross_instrument_limit(self, account_id: str, instrument_id: str,
                                      requested_amount: float) -> BridgeRiskResponse:
        raise NotImplementedError


class PositionBridge:
    """Interface that risk_service uses to communicate with position_service.

    This breaks the lazy import dependency on PositionService class.
    PositionService implements this interface; risk_service depends only on this.
    """

    def get_position_service(self, scope_id: Optional[str] = None) -> Any:
        raise NotImplementedError

    def get_cross_strategy_risk_guard(self) -> Any:
        raise NotImplementedError

    def get_position_limit_config(self, account_id: str) -> Optional[Any]:
        raise NotImplementedError


class RiskBridgeAdapter(RiskPositionBridge):
    """Adapter wrapping an existing RiskService instance as a RiskPositionBridge."""

    def __init__(self, risk_service: Any):
        self._risk_service = risk_service

    def check_position_limit(self, account_id: str, required_amount: float) -> BridgeRiskResponse:
        if self._risk_service is None:
            return BridgeRiskResponse(level=BridgeRiskLevel.PASS, reason="no_risk_service")
        try:
            result = self._risk_service._check_position_limit(account_id, required_amount)
            if hasattr(result, 'is_pass') and result.is_pass:
                return BridgeRiskResponse(level=BridgeRiskLevel.PASS)
            elif hasattr(result, 'is_block') and result.is_block:
                return BridgeRiskResponse(
                    level=BridgeRiskLevel.BLOCK,
                    reason=getattr(result, 'reason', ''),
                    message=getattr(result, 'message', '')
                )
            return BridgeRiskResponse(
                level=BridgeRiskLevel.WARNING,
                reason=getattr(result, 'reason', ''),
                message=getattr(result, 'message', '')
            )
        except Exception as e:
            logger.warning("RiskBridgeAdapter.check_position_limit error: %s", e)
            return BridgeRiskResponse(level=BridgeRiskLevel.PASS, reason="adapter_error")

    def set_position_limit(self, account_id: str, limit_amount: float,
                           effective_until: Optional[float] = None) -> None:
        if self._risk_service is not None:
            self._risk_service.set_position_limit(account_id, limit_amount, effective_until)

    def get_position_limit(self, account_id: str) -> Optional[BridgePositionLimit]:
        if self._risk_service is None:
            return None
        try:
            pl = self._risk_service.get_position_limit(account_id)
            if pl is None:
                return None
            return BridgePositionLimit(
                account_id=getattr(pl, 'account_id', account_id),
                limit_amount=getattr(pl, 'limit_amount', 0.0),
                effective_until=getattr(pl, 'effective_until', None),
            )
        except Exception as e:
            logger.warning("RiskBridgeAdapter.get_position_limit error: %s", e)
            return None

    def get_safety_meta_layer(self, params: Any = None, strategy_id: str = "") -> Any:
        try:
            from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
            return get_safety_meta_layer(params=params, strategy_id=strategy_id)
        except ImportError:
            return None

    def check_cross_instrument_limit(self, account_id: str, instrument_id: str,
                                      requested_amount: float) -> BridgeRiskResponse:
        if self._risk_service is None:
            return BridgeRiskResponse(level=BridgeRiskLevel.PASS, reason="no_risk_service")
        try:
            result = self._risk_service.check_cross_instrument_limit(
                account_id, instrument_id, requested_amount
            )
            if hasattr(result, 'is_pass') and result.is_pass:
                return BridgeRiskResponse(level=BridgeRiskLevel.PASS)
            return BridgeRiskResponse(
                level=BridgeRiskLevel.BLOCK,
                reason=getattr(result, 'reason', ''),
            )
        except Exception as e:
            logger.warning("RiskBridgeAdapter.check_cross_instrument_limit error: %s", e)
            return BridgeRiskResponse(level=BridgeRiskLevel.PASS, reason="adapter_error")


class PositionBridgeAdapter(PositionBridge):
    """Adapter wrapping an existing PositionService instance as a PositionBridge."""

    def __init__(self, position_service: Any = None):
        self._position_service = position_service

    def get_position_service(self, scope_id: Optional[str] = None) -> Any:
        if self._position_service is not None:
            return self._position_service
        try:
            from ali2026v3_trading.position.position_service import get_position_service
            return get_position_service(scope_id=scope_id)
        except ImportError:
            return None

    def get_cross_strategy_risk_guard(self) -> Any:
        try:
            from ali2026v3_trading.position.position_service import get_cross_strategy_risk_guard
            return get_cross_strategy_risk_guard()
        except ImportError:
            return None

    def get_position_limit_config(self, account_id: str) -> Optional[Any]:
        if self._position_service is None:
            return None
        try:
            return getattr(self._position_service, '_position_limits', {}).get(account_id)
        except Exception:
            return None
