# MODULE_ID: M2-559
"""综合风控模块单测 — 覆盖 risk_check_engine / market_risk / operational_risk"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from ali2026v3_trading.risk.risk_check_engine import (
    RiskCheckEngine,
    RiskRule,
    RiskContext,
    RiskRuleResult,
    RiskCheckReport,
    PositionLimitRule,
    MarginSufficiencyRule,
    DailyDrawdownRule,
    NearExpiryRule,
    SignalCooldownRule,
    create_default_risk_check_engine,
)
from ali2026v3_trading.risk_engine.market_risk import (
    check_risk_ratio,
    check_greeks_limits,
    check_life_expectancy,
    check_spread_degradation,
    check_exchange_status,
    check_price_limit,
    check_expiry_risk,
    check_auction_session,
    check_market_risks,
)
from ali2026v3_trading.risk_engine.operational_risk import (
    check_strategy_status,
    check_rate_limit,
    check_consecutive_loss_protection,
    check_invariant_runtime,
    check_signal_validity,
    check_single_trade_risk,
    check_sharpe_iron_rule,
    check_e7_residual_block,
    check_capital_sufficiency_in_trade,
    check_shadow_ev,
    check_operational_risks,
)
from ali2026v3_trading.tests._stubs import RiskSnapshotStub


# ═══════════════════════════════════════════════════════════════
#  risk_check_engine 测试
# ═══════════════════════════════════════════════════════════════


class TestRiskContext:
    def test_risk_context_creation(self):
        ctx = RiskContext(signal={"symbol": "IF2606"})
        assert ctx.signal == {"symbol": "IF2606"}
        assert ctx.equity == 0.0
        assert ctx.position_data is None
        assert ctx.risk_service is None


class TestRiskRuleResult:
    def test_risk_rule_result_defaults(self):
        r = RiskRuleResult(rule_name="test", passed=True)
        assert r.severity == 'P2'
        assert r.details is None


class TestRiskCheckReport:
    def test_risk_check_report_failed_rules(self):
        r1 = RiskRuleResult(rule_name="a", passed=True)
        r2 = RiskRuleResult(rule_name="b", passed=False)
        r3 = RiskRuleResult(rule_name="c", passed=False)
        report = RiskCheckReport(passed=False, results=[r1, r2, r3])
        assert len(report.failed_rules) == 2
        assert all(not r.passed for r in report.failed_rules)

    def test_risk_check_report_all_passed(self):
        r1 = RiskRuleResult(rule_name="a", passed=True)
        r2 = RiskRuleResult(rule_name="b", passed=True)
        report = RiskCheckReport(passed=True, results=[r1, r2])
        assert report.failed_rules == []


class TestRiskCheckEngine:
    def test_engine_register_rule(self):
        engine = RiskCheckEngine()
        mock_rule = MagicMock(spec=RiskRule)
        mock_rule.name = "mock_rule"
        mock_rule.severity = "P2"
        engine.register(mock_rule)
        assert "mock_rule" in engine.rule_names

    def test_engine_rules_sorted_by_severity(self):
        engine = RiskCheckEngine()
        p2_rule = MagicMock(spec=RiskRule)
        p2_rule.name = "p2_rule"
        p2_rule.severity = "P2"
        p0_rule = MagicMock(spec=RiskRule)
        p0_rule.name = "p0_rule"
        p0_rule.severity = "P0"
        engine.register(p2_rule)
        engine.register(p0_rule)
        assert engine.rule_names[0] == "p0_rule"
        assert engine.rule_names[1] == "p2_rule"

    def test_engine_run_checks_all_pass(self):
        engine = RiskCheckEngine()
        rule = MagicMock(spec=RiskRule)
        rule.name = "ok_rule"
        rule.severity = "P2"
        rule.check.return_value = RiskRuleResult(rule_name="ok_rule", passed=True, severity="P2")
        engine.register(rule)
        ctx = RiskContext(signal={})
        report = engine.run_checks(ctx)
        assert report.passed is True

    def test_engine_run_checks_p0_blocks(self):
        engine = RiskCheckEngine()
        rule = MagicMock(spec=RiskRule)
        rule.name = "blocker"
        rule.severity = "P0"
        rule.check.return_value = RiskRuleResult(
            rule_name="blocker", passed=False, severity="P0", reason="blocked"
        )
        engine.register(rule)
        ctx = RiskContext(signal={})
        report = engine.run_checks(ctx)
        assert report.passed is False
        assert report.blocking_result is not None
        assert report.blocking_result.rule_name == "blocker"

    def test_engine_run_checks_p1_does_not_block(self):
        engine = RiskCheckEngine()
        rule = MagicMock(spec=RiskRule)
        rule.name = "warn_rule"
        rule.severity = "P1"
        rule.check.return_value = RiskRuleResult(
            rule_name="warn_rule", passed=False, severity="P1", reason="warn"
        )
        engine.register(rule)
        ctx = RiskContext(signal={})
        report = engine.run_checks(ctx)
        assert report.passed is False
        assert report.blocking_result is None

    def test_engine_run_checks_exception_handled(self):
        engine = RiskCheckEngine()
        rule = MagicMock(spec=RiskRule)
        rule.name = "broken_rule"
        rule.severity = "P2"
        rule.check.side_effect = ValueError("boom")
        engine.register(rule)
        ctx = RiskContext(signal={})
        report = engine.run_checks(ctx)
        assert len(report.results) == 1
        assert report.results[0].passed is False
        assert "boom" in report.results[0].reason


class TestPositionLimitRule:
    def test_position_limit_rule_pass(self):
        rule = PositionLimitRule()
        ctx = RiskContext(signal={}, position_data={"position_count": 1})
        result = rule.check(ctx)
        assert result.passed is True

    def test_position_limit_rule_fail(self):
        rule = PositionLimitRule()
        ctx = RiskContext(signal={}, position_data={"position_count": 3})
        result = rule.check(ctx)
        assert result.passed is False

    def test_position_limit_rule_custom_max(self):
        rule = PositionLimitRule()
        rs = MagicMock()
        rs._params = {"max_open_positions": 5}
        ctx = RiskContext(signal={}, position_data={"position_count": 4}, risk_service=rs)
        result = rule.check(ctx)
        assert result.passed is True


class TestMarginSufficiencyRule:
    def test_margin_sufficiency_rule_pass(self):
        rule = MarginSufficiencyRule()
        ctx = RiskContext(signal={}, equity=100.0)
        result = rule.check(ctx)
        assert result.passed is True

    def test_margin_sufficiency_rule_fail(self):
        rule = MarginSufficiencyRule()
        ctx = RiskContext(signal={}, equity=0.0)
        result = rule.check(ctx)
        assert result.passed is False


class TestDailyDrawdownRule:
    @patch("ali2026v3_trading.infra.risk_rules.resolve_and_check_daily_drawdown",
           return_value=(False, ''))
    def test_daily_drawdown_rule_pass(self, mock_dd):
        rule = DailyDrawdownRule()
        ctx = RiskContext(signal={}, position_data={"daily_drawdown_pct": 0.01})
        result = rule.check(ctx)
        assert result.passed is True

    @patch("ali2026v3_trading.infra.risk_rules.resolve_and_check_daily_drawdown",
           return_value=(True, 'drawdown exceeded'))
    def test_daily_drawdown_rule_fail(self, mock_dd):
        rule = DailyDrawdownRule()
        ctx = RiskContext(signal={}, position_data={"daily_drawdown_pct": 0.1})
        result = rule.check(ctx)
        assert result.passed is False
        assert "drawdown exceeded" in result.reason


class TestNearExpiryRule:
    def test_near_expiry_rule_pass(self):
        rule = NearExpiryRule()
        ctx = RiskContext(signal={}, position_data={"days_to_expiry": 10})
        result = rule.check(ctx)
        assert result.passed is True

    def test_near_expiry_rule_fail(self):
        rule = NearExpiryRule()
        ctx = RiskContext(signal={}, position_data={"days_to_expiry": 3})
        result = rule.check(ctx)
        assert result.passed is False


class TestSignalCooldownRule:
    def test_signal_cooldown_rule_always_passes(self):
        rule = SignalCooldownRule()
        ctx = RiskContext(signal={})
        result = rule.check(ctx)
        assert result.passed is True


class TestCreateDefaultEngine:
    def test_create_default_engine(self):
        engine = create_default_risk_check_engine()
        assert len(engine.rule_names) == 5


class TestCheckSharpeIronRule:
    def test_check_sharpe_iron_rule_pass(self):
        """signal sharpe >= threshold, passes"""
        engine = RiskCheckEngine()
        with patch(
            "ali2026v3_trading.config.config_params.DEFAULT_PARAM_TABLE",
            {"sharpe_iron_rule_threshold": 0.5},
        ):
            result = engine.check_sharpe_iron_rule({"sharpe": 1.0})
            assert result.passed is True

    def test_check_sharpe_iron_rule_fail(self):
        engine = RiskCheckEngine()
        with patch(
            "ali2026v3_trading.config.config_params.DEFAULT_PARAM_TABLE",
            {"sharpe_iron_rule_threshold": 0.5},
        ):
            result = engine.check_sharpe_iron_rule({"sharpe": 0.2})
            assert result.passed is False


# ═══════════════════════════════════════════════════════════════
#  market_risk 测试
# ═══════════════════════════════════════════════════════════════


def _make_risk_service_for_market():
    rs = MagicMock()
    rs._check_risk_ratio.return_value = None
    rs._check_greeks_limits.return_value = None
    rs._check_life_expectancy.return_value = None
    rs._check_spread_degradation.return_value = None
    rs.check_exchange_status.return_value = {"tradeable": True}
    rs.is_at_price_limit.return_value = {}
    rs.check_expiry_risk.return_value = MagicMock(is_block=False)
    rs.check_auction_session.return_value = MagicMock(is_block=False)
    return rs


class TestCheckRiskRatio:
    def test_check_risk_ratio_delegates(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        check_risk_ratio(snap, rs)
        rs._check_risk_ratio.assert_called_once_with(snap.signal)


class TestCheckGreeksLimits:
    def test_check_greeks_limits_delegates(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        check_greeks_limits(snap, rs)
        rs._check_greeks_limits.assert_called_once_with(snap.signal)


class TestCheckLifeExpectancy:
    def test_check_life_expectancy_delegates(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        check_life_expectancy(snap, rs)
        rs._check_life_expectancy.assert_called_once_with(snap.signal)


class TestCheckSpreadDegradation:
    def test_check_spread_degradation_delegates(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        check_spread_degradation(snap, rs)
        rs._check_spread_degradation.assert_called_once_with(snap.signal)


class TestCheckExchangeStatus:
    def test_check_exchange_status_tradeable(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        result = check_exchange_status(snap, rs)
        assert result is None

    def test_check_exchange_status_not_tradeable(self):
        from ali2026v3_trading.risk.risk_service import RiskLevel
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        rs.check_exchange_status.return_value = {"tradeable": False, "reason": "closed"}
        result = check_exchange_status(snap, rs)
        assert result is not None
        assert result.is_block

    def test_check_exchange_status_exception(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        rs.check_exchange_status.side_effect = RuntimeError("oops")
        result = check_exchange_status(snap, rs)
        assert result is None


class TestCheckPriceLimit:
    def test_check_price_limit_no_block(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        result = check_price_limit(snap, rs)
        assert result is None


class TestCheckExpiryRisk:
    def test_check_expiry_risk_no_block(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        rs.check_expiry_risk.return_value = MagicMock(is_block=False)
        result = check_expiry_risk(snap, rs)
        assert result is None

    def test_check_expiry_risk_blocked(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        block = MagicMock(is_block=True)
        rs.check_expiry_risk.return_value = block
        result = check_expiry_risk(snap, rs)
        assert result is block


class TestCheckAuctionSession:
    def test_check_auction_session_no_block(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        rs.check_auction_session.return_value = MagicMock(is_block=False)
        result = check_auction_session(snap, rs)
        assert result is None

    def test_check_auction_session_blocked(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        block = MagicMock(is_block=True)
        rs.check_auction_session.return_value = block
        result = check_auction_session(snap, rs)
        assert result is block


class TestCheckMarketRisks:
    def test_check_market_risks_all_pass(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        result = check_market_risks(snap, rs)
        assert result is None

    def test_check_market_risks_risk_ratio_blocks(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_market()
        rs._check_risk_ratio.return_value = MagicMock(is_block=True)
        result = check_market_risks(snap, rs)
        assert result is not None
        assert result.is_block

    def test_check_market_risks_close_skips_some(self):
        """action="CLOSE" skips spread/exchange/price/expiry checks"""
        snap = RiskSnapshotStub(action="CLOSE")
        rs = _make_risk_service_for_market()
        result = check_market_risks(snap, rs)
        assert result is None
        rs._check_spread_degradation.assert_not_called()
        rs.check_exchange_status.assert_not_called()
        rs.is_at_price_limit.assert_not_called()
        rs.check_expiry_risk.assert_not_called()


# ═══════════════════════════════════════════════════════════════
#  operational_risk 测试
# ═══════════════════════════════════════════════════════════════


def _make_risk_service_for_ops():
    rs = MagicMock()
    rs._check_strategy_status.return_value = None
    rs._check_rate_limit.return_value = None
    rs._check_consecutive_loss_protection.return_value = MagicMock(is_block=False)
    rs._check_invariant_runtime.return_value = {"all_passed": True, "violations": []}
    rs._check_single_trade_risk.return_value = None
    rs._check_sharpe_iron_rule.return_value = None
    rs._check_e7_residual_block.return_value = None
    rs._check_capital_sufficiency_in_trade.return_value = None
    rs._fire_alert.return_value = None
    return rs


class TestCheckStrategyStatus:
    def test_check_strategy_status_delegates(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        check_strategy_status(snap, rs)
        rs._check_strategy_status.assert_called_once()


class TestCheckRateLimit:
    def test_check_rate_limit_delegates(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        check_rate_limit(snap, rs)
        rs._check_rate_limit.assert_called_once_with(snap.symbol)


class TestCheckConsecutiveLossProtection:
    def test_check_consecutive_loss_protection_blocks(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        rs._check_consecutive_loss_protection.return_value = MagicMock(is_block=True)
        result = check_consecutive_loss_protection(snap, rs)
        assert result is not None
        assert result.is_block

    def test_check_consecutive_loss_protection_passes(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        rs._check_consecutive_loss_protection.return_value = MagicMock(is_block=False)
        result = check_consecutive_loss_protection(snap, rs)
        assert result is None


class TestCheckInvariantRuntime:
    def test_check_invariant_runtime_always_returns_none(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        rs._check_invariant_runtime.return_value = {"all_passed": False, "violations": ["v1"]}
        result = check_invariant_runtime(snap, rs)
        assert result is None


class TestCheckSignalValidity:
    def test_check_signal_validity_valid(self):
        snap = RiskSnapshotStub(is_valid=True)
        rs = _make_risk_service_for_ops()
        result = check_signal_validity(snap, rs)
        assert result is None

    def test_check_signal_validity_invalid(self):
        snap = RiskSnapshotStub(is_valid=False)
        rs = _make_risk_service_for_ops()
        result = check_signal_validity(snap, rs)
        assert result is not None
        assert result.is_block


class TestCheckSingleTradeRisk:
    def test_check_single_trade_risk_delegates(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        check_single_trade_risk(snap, rs)
        rs._check_single_trade_risk.assert_called_once_with(snap.signal)


class TestCheckSharpeIronRuleOps:
    def test_check_sharpe_iron_rule_delegates(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        check_sharpe_iron_rule(snap, rs)
        rs._check_sharpe_iron_rule.assert_called_once_with(snap.signal)


class TestCheckE7ResidualBlock:
    def test_check_e7_residual_block_delegates(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        check_e7_residual_block(snap, rs)
        rs._check_e7_residual_block.assert_called_once_with(snap.signal)


class TestCheckCapitalSufficiency:
    def test_check_capital_sufficiency_delegates(self):
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        check_capital_sufficiency_in_trade(snap, rs)
        rs._check_capital_sufficiency_in_trade.assert_called_once_with(snap.signal)


class TestCheckShadowEv:
    @patch('ali2026v3_trading.strategy.shadow_strategy_facade.get_shadow_strategy_engine')
    def test_check_shadow_ev_absolute_paused(self, mock_get_sse):
        mock_sse = MagicMock()
        mock_sse.is_absolute_ev_paused.return_value = True
        mock_get_sse.return_value = mock_sse
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        result = check_shadow_ev(snap, rs)
        assert result is not None
        assert result.is_block

    @patch('ali2026v3_trading.strategy.shadow_strategy_facade.get_shadow_strategy_engine')
    def test_check_shadow_ev_degradation_active(self, mock_get_sse):
        mock_sse = MagicMock()
        mock_sse.is_absolute_ev_paused.return_value = False
        mock_sse.is_degradation_active.return_value = True
        mock_get_sse.return_value = mock_sse
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        result = check_shadow_ev(snap, rs)
        assert result is not None
        assert result.is_block

    def test_check_shadow_ev_close_action_skips(self):
        snap = RiskSnapshotStub(action="CLOSE")
        rs = _make_risk_service_for_ops()
        result = check_shadow_ev(snap, rs)
        assert result is None

    def test_check_shadow_ev_import_error(self):
        """当shadow_strategy_facade导入失败时，fail-safe阻断"""
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        with patch.dict('sys.modules', {'ali2026v3_trading.strategy.shadow_strategy_facade': None}):
            result = check_shadow_ev(snap, rs)
            assert result is not None
            assert result.is_block


class TestCheckOperationalRisks:
    @patch('ali2026v3_trading.strategy.shadow_strategy_facade.get_shadow_strategy_engine')
    def test_check_operational_risks_all_pass(self, mock_get_sse):
        mock_sse = MagicMock()
        mock_sse.is_absolute_ev_paused.return_value = False
        mock_sse.is_degradation_active.return_value = False
        mock_get_sse.return_value = mock_sse
        snap = RiskSnapshotStub()
        rs = _make_risk_service_for_ops()
        result = check_operational_risks(snap, rs)
        assert result is None

    @patch('ali2026v3_trading.strategy.shadow_strategy_facade.get_shadow_strategy_engine')
    def test_check_operational_risks_signal_invalid(self, mock_get_sse):
        mock_sse = MagicMock()
        mock_sse.is_absolute_ev_paused.return_value = False
        mock_sse.is_degradation_active.return_value = False
        mock_get_sse.return_value = mock_sse
        snap = RiskSnapshotStub(is_valid=False)
        rs = _make_risk_service_for_ops()
        result = check_operational_risks(snap, rs)
        assert result is not None
        assert result.is_block

    @patch('ali2026v3_trading.strategy.shadow_strategy_facade.get_shadow_strategy_engine')
    def test_check_operational_risks_close_skips_some(self, mock_get_sse):
        """CLOSE action skips single_trade_risk, sharpe, e7, capital"""
        mock_sse = MagicMock()
        mock_sse.is_absolute_ev_paused.return_value = False
        mock_sse.is_degradation_active.return_value = False
        mock_get_sse.return_value = mock_sse
        snap = RiskSnapshotStub(action="CLOSE")
        rs = _make_risk_service_for_ops()
        result = check_operational_risks(snap, rs)
        assert result is None
        rs._check_single_trade_risk.assert_not_called()
        rs._check_sharpe_iron_rule.assert_not_called()
        rs._check_e7_residual_block.assert_not_called()
        rs._check_capital_sufficiency_in_trade.assert_not_called()
