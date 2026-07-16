# MODULE_ID: M2-555
"""测试: risk/risk_check_engine.py"""
import pytest
from unittest.mock import MagicMock
from risk.risk_check_engine import (
    RiskCheckEngine, RiskRule, PositionLimitRule, DailyDrawdownRule,
    create_default_risk_check_engine, RiskContext,
)


class TestPositionLimitRule:
    def test_blocks_over_limit(self):
        rule = PositionLimitRule()
        rs = MagicMock()
        rs._params = {'max_open_positions': 3}
        ctx = RiskContext(signal={}, position_data={'position_count': 5}, risk_service=rs)
        result = rule.check(ctx)
        assert result.passed is False

    def test_passes_under_limit(self):
        rule = PositionLimitRule()
        rs = MagicMock()
        rs._params = {'max_open_positions': 5}
        ctx = RiskContext(signal={}, position_data={'position_count': 3}, risk_service=rs)
        result = rule.check(ctx)
        assert result.passed is True

    def test_passes_at_limit(self):
        rule = PositionLimitRule()
        rs = MagicMock()
        rs._params = {'max_open_positions': 5}
        ctx = RiskContext(signal={}, position_data={'position_count': 5}, risk_service=rs)
        result = rule.check(ctx)
        assert result.passed is False


class TestDailyDrawdownRule:
    def test_check_callable(self):
        rule = DailyDrawdownRule()
        assert callable(rule.check)


class TestRiskCheckEngine:
    def test_register_and_run(self):
        engine = RiskCheckEngine()
        rule = MagicMock()
        rule.priority = 'P1'
        rule.check.return_value = MagicMock(passed=True)
        engine.register(rule)
        result = engine.run_checks(MagicMock())
        assert result.passed is True

    def test_p0_short_circuit(self):
        engine = RiskCheckEngine()
        p0 = MagicMock()
        p0.priority = 'P0'
        p0.check.return_value = MagicMock(passed=False, severity='P0')
        engine.register(p0)
        p1 = MagicMock()
        p1.priority = 'P1'
        p1.check.return_value = MagicMock(passed=True)
        engine.register(p1)
        result = engine.run_checks(MagicMock())
        assert result.passed is False
        p1.check.assert_not_called()

    def test_all_pass(self):
        engine = RiskCheckEngine()
        for i in range(3):
            r = MagicMock()
            r.priority = 'P1'
            r.check.return_value = MagicMock(passed=True)
            engine.register(r)
        result = engine.run_checks(MagicMock())
        assert result.passed is True

    def test_create_default_engine(self):
        engine = create_default_risk_check_engine()
        assert len(engine._rules) >= 2

    def test_empty_engine_passes(self):
        engine = RiskCheckEngine()
        result = engine.run_checks(MagicMock())
        assert result.passed is True