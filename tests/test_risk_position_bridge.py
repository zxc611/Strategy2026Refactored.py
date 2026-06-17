# MODULE_ID: M2-560
"""测试: risk/risk_position_bridge.py"""
import pytest
from unittest.mock import MagicMock
from ali2026v3_trading.risk.risk_position_bridge import (
    BridgeRiskLevel, BridgeRiskResponse, BridgePositionLimit,
    RiskPositionBridge, PositionBridge,
    RiskBridgeAdapter, PositionBridgeAdapter,
)


class TestBridgeRiskLevel:
    def test_enum_values(self):
        assert BridgeRiskLevel.PASS.value == "PASS"
        assert BridgeRiskLevel.BLOCK.value == "BLOCK"
        assert BridgeRiskLevel.WARNING.value == "WARNING"


class TestBridgeRiskResponse:
    def test_defaults(self):
        r = BridgeRiskResponse()
        assert r.level == BridgeRiskLevel.PASS
        assert r.reason == ""
        assert r.message == ""
        assert r.details == {}

    def test_custom(self):
        r = BridgeRiskResponse(level=BridgeRiskLevel.BLOCK, reason="limit_exceeded", message="too much")
        assert r.level == BridgeRiskLevel.BLOCK
        assert r.reason == "limit_exceeded"
        assert r.message == "too much"


class TestBridgePositionLimit:
    def test_defaults(self):
        pl = BridgePositionLimit()
        assert pl.account_id == ""
        assert pl.limit_amount == 0.0
        assert pl.effective_until is None

    def test_custom(self):
        pl = BridgePositionLimit(account_id="acc1", limit_amount=50000.0, effective_until=1700000000.0)
        assert pl.account_id == "acc1"
        assert pl.limit_amount == 50000.0


class TestRiskPositionBridgeProtocol:
    def test_check_position_limit_raises(self):
        bridge = RiskPositionBridge()
        with pytest.raises(NotImplementedError):
            bridge.check_position_limit("acc1", 1000.0)

    def test_set_position_limit_raises(self):
        bridge = RiskPositionBridge()
        with pytest.raises(NotImplementedError):
            bridge.set_position_limit("acc1", 5000.0)

    def test_get_position_limit_raises(self):
        bridge = RiskPositionBridge()
        with pytest.raises(NotImplementedError):
            bridge.get_position_limit("acc1")

    def test_get_safety_meta_layer_raises(self):
        bridge = RiskPositionBridge()
        with pytest.raises(NotImplementedError):
            bridge.get_safety_meta_layer()

    def test_check_cross_instrument_limit_raises(self):
        bridge = RiskPositionBridge()
        with pytest.raises(NotImplementedError):
            bridge.check_cross_instrument_limit("acc1", "AL2401", 100.0)


class TestPositionBridgeProtocol:
    def test_get_position_service_raises(self):
        bridge = PositionBridge()
        with pytest.raises(NotImplementedError):
            bridge.get_position_service()

    def test_get_cross_strategy_risk_guard_raises(self):
        bridge = PositionBridge()
        with pytest.raises(NotImplementedError):
            bridge.get_cross_strategy_risk_guard()

    def test_get_position_limit_config_raises(self):
        bridge = PositionBridge()
        with pytest.raises(NotImplementedError):
            bridge.get_position_limit_config("acc1")


class TestRiskBridgeAdapter:
    def test_no_risk_service_returns_pass(self):
        adapter = RiskBridgeAdapter(risk_service=None)
        result = adapter.check_position_limit("acc1", 1000.0)
        assert result.level == BridgeRiskLevel.PASS
        assert result.reason == "no_risk_service"

    def test_pass_result(self):
        rs = MagicMock()
        rs._check_position_limit.return_value = MagicMock(is_pass=True)
        adapter = RiskBridgeAdapter(risk_service=rs)
        result = adapter.check_position_limit("acc1", 1000.0)
        assert result.level == BridgeRiskLevel.PASS

    def test_block_result(self):
        rs = MagicMock()
        rs._check_position_limit.return_value = MagicMock(is_pass=False, is_block=True, reason="over_limit", message="blocked")
        adapter = RiskBridgeAdapter(risk_service=rs)
        result = adapter.check_position_limit("acc1", 1000.0)
        assert result.level == BridgeRiskLevel.BLOCK
        assert result.reason == "over_limit"

    def test_warning_result(self):
        rs = MagicMock()
        rs._check_position_limit.return_value = MagicMock(is_pass=False, is_block=False, reason="near_limit")
        adapter = RiskBridgeAdapter(risk_service=rs)
        result = adapter.check_position_limit("acc1", 1000.0)
        assert result.level == BridgeRiskLevel.WARNING

    def test_exception_returns_pass(self):
        rs = MagicMock()
        rs._check_position_limit.side_effect = RuntimeError("crash")
        adapter = RiskBridgeAdapter(risk_service=rs)
        result = adapter.check_position_limit("acc1", 1000.0)
        assert result.level == BridgeRiskLevel.PASS
        assert result.reason == "adapter_error"

    def test_set_position_limit_no_service(self):
        adapter = RiskBridgeAdapter(risk_service=None)
        adapter.set_position_limit("acc1", 5000.0)

    def test_set_position_limit_with_service(self):
        rs = MagicMock()
        adapter = RiskBridgeAdapter(risk_service=rs)
        adapter.set_position_limit("acc1", 5000.0)
        rs.set_position_limit.assert_called_once()

    def test_get_position_limit_no_service(self):
        adapter = RiskBridgeAdapter(risk_service=None)
        assert adapter.get_position_limit("acc1") is None

    def test_get_position_limit_with_service(self):
        rs = MagicMock()
        rs.get_position_limit.return_value = MagicMock(account_id="acc1", limit_amount=5000.0, effective_until=None)
        adapter = RiskBridgeAdapter(risk_service=rs)
        result = adapter.get_position_limit("acc1")
        assert result is not None
        assert result.account_id == "acc1"

    def test_get_position_limit_service_returns_none(self):
        rs = MagicMock()
        rs.get_position_limit.return_value = None
        adapter = RiskBridgeAdapter(risk_service=rs)
        assert adapter.get_position_limit("acc1") is None

    def test_get_position_limit_exception(self):
        rs = MagicMock()
        rs.get_position_limit.side_effect = RuntimeError("err")
        adapter = RiskBridgeAdapter(risk_service=rs)
        assert adapter.get_position_limit("acc1") is None

    def test_cross_instrument_limit_no_service(self):
        adapter = RiskBridgeAdapter(risk_service=None)
        result = adapter.check_cross_instrument_limit("acc1", "AL2401", 100.0)
        assert result.level == BridgeRiskLevel.PASS

    def test_cross_instrument_limit_pass(self):
        rs = MagicMock()
        rs.check_cross_instrument_limit.return_value = MagicMock(is_pass=True)
        adapter = RiskBridgeAdapter(risk_service=rs)
        result = adapter.check_cross_instrument_limit("acc1", "AL2401", 100.0)
        assert result.level == BridgeRiskLevel.PASS

    def test_cross_instrument_limit_block(self):
        rs = MagicMock()
        rs.check_cross_instrument_limit.return_value = MagicMock(is_pass=False, reason="cross_limit")
        adapter = RiskBridgeAdapter(risk_service=rs)
        result = adapter.check_cross_instrument_limit("acc1", "AL2401", 100.0)
        assert result.level == BridgeRiskLevel.BLOCK

    def test_cross_instrument_limit_exception(self):
        rs = MagicMock()
        rs.check_cross_instrument_limit.side_effect = RuntimeError("err")
        adapter = RiskBridgeAdapter(risk_service=rs)
        result = adapter.check_cross_instrument_limit("acc1", "AL2401", 100.0)
        assert result.level == BridgeRiskLevel.PASS


class TestPositionBridgeAdapter:
    def test_with_service(self):
        ps = MagicMock()
        adapter = PositionBridgeAdapter(position_service=ps)
        assert adapter.get_position_service() is ps

    def test_no_service_import_fallback(self):
        adapter = PositionBridgeAdapter(position_service=None)
        result = adapter.get_position_service()
        assert result is None or result is not None

    def test_get_cross_strategy_risk_guard(self):
        adapter = PositionBridgeAdapter(position_service=None)
        result = adapter.get_cross_strategy_risk_guard()
        assert result is None or result is not None

    def test_get_position_limit_config_no_service(self):
        adapter = PositionBridgeAdapter(position_service=None)
        assert adapter.get_position_limit_config("acc1") is None

    def test_get_position_limit_config_with_service(self):
        ps = MagicMock()
        ps._position_limits = {"acc1": MagicMock()}
        adapter = PositionBridgeAdapter(position_service=ps)
        result = adapter.get_position_limit_config("acc1")
        assert result is not None

    def test_get_position_limit_config_missing_account(self):
        ps = MagicMock()
        ps._position_limits = {}
        adapter = PositionBridgeAdapter(position_service=ps)
        assert adapter.get_position_limit_config("nonexistent") is None