# MODULE_ID: M2-469
"""strategy_judgment/pnl_attribution.py 覆盖率测试"""
import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from strategy_judgment.pnl_attribution import (
    PnLAttributor, AttributionEngine, TradeRecord, AttributionResult,
    CostAttribution, RiskAttribution, Direction, TimeSegment,
    classify_time_segment, classify_direction,
)


class TestClassifyHelpers:
    def test_classify_direction_positive(self):
        result = classify_direction(100)
        assert result is not None

    def test_classify_direction_negative(self):
        result = classify_direction(-100)
        assert result is not None

    def test_classify_direction_zero(self):
        result = classify_direction(0)
        assert result is not None

    def test_classify_time_segment(self):
        from datetime import datetime
        from infra.shared_utils import CHINA_TZ
        ts = datetime(2025, 1, 2, 10, 0, tzinfo=CHINA_TZ)
        result = classify_time_segment(ts)
        assert result is not None


class TestPnLAttributor:
    def test_init(self):
        attr = PnLAttributor()
        assert attr is not None

    def _make_trade(self, direction=Direction.LONG, pnl=10.0):
        return TradeRecord(
            strategy='test', instrument='AU2506', direction=direction,
            time_segment=TimeSegment.OPEN, pnl=pnl, commission=1.0, slippage=0.5,
        )

    def test_add_trade(self):
        attr = PnLAttributor()
        trade = self._make_trade()
        attr.add_trade(trade)

    def test_add_trades(self):
        attr = PnLAttributor()
        trades = [self._make_trade(), self._make_trade(direction=Direction.SHORT, pnl=-5.0)]
        attr.add_trades(trades)

    def test_compute_attribution(self):
        attr = PnLAttributor()
        attr.add_trade(self._make_trade())
        attr.add_trade(self._make_trade(direction=Direction.SHORT, pnl=-5.0))
        result = attr.compute_attribution()
        assert isinstance(result, AttributionResult)

    def test_compute_cost_attribution(self):
        attr = PnLAttributor()
        attr.add_trade(self._make_trade())
        result = attr.compute_cost_attribution()
        assert isinstance(result, CostAttribution)

    def test_compute_excess_return(self):
        attr = PnLAttributor()
        result = attr.compute_excess_return(strategy_pnl=10.0, benchmark_pnl=5.0)
        assert isinstance(result, float)

    def test_compute_residual(self):
        attr = PnLAttributor()
        attr.add_trade(self._make_trade())
        ar = attr.compute_attribution()
        result = attr.compute_residual(ar)
        assert isinstance(result, float)

    def test_compute_risk_attribution(self):
        attr = PnLAttributor()
        attr.add_trade(self._make_trade())
        result = attr.compute_risk_attribution()
        assert isinstance(result, RiskAttribution)

    def test_compute_win_loss_streaks(self):
        attr = PnLAttributor()
        for i in range(10):
            attr.add_trade(self._make_trade(pnl=5.0 if i % 2 == 0 else -3.0))
        result = attr.compute_win_loss_streaks()
        assert isinstance(result, dict)

    def test_format_attribution_report(self):
        attr = PnLAttributor()
        attr.add_trade(self._make_trade())
        ar = attr.compute_attribution()
        result = attr.format_attribution_report(ar)
        assert isinstance(result, str)

    def test_compute_attribution_confidence(self):
        attr = PnLAttributor()
        attr.add_trade(self._make_trade())
        ar = attr.compute_attribution()
        result = attr.compute_attribution_confidence(ar)
        assert isinstance(result, float)

    def test_compute_drawdown_duration(self):
        attr = PnLAttributor()
        result = attr.compute_drawdown_duration(equity_curve=[100, 95, 90, 95, 100])
        assert isinstance(result, dict)

    def test_compute_return_stability(self):
        attr = PnLAttributor()
        result = attr.compute_return_stability(returns=[0.01, -0.005, 0.02, -0.01, 0.015])
        assert isinstance(result, dict)

    def test_compute_interaction_effects(self):
        attr = PnLAttributor()
        attr.add_trade(self._make_trade())
        result = attr.compute_interaction_effects()
        assert isinstance(result, dict)

    def test_compute_attribution_frequency_match(self):
        attr = PnLAttributor()
        attr.add_trade(self._make_trade())
        result = attr.compute_attribution_frequency_match()
        assert isinstance(result, dict)

    def test_compute_alpha_decay_statistics(self):
        attr = PnLAttributor()
        result = attr.compute_alpha_decay_statistics(alpha_series=[0.1, 0.08, 0.06, 0.04, 0.02])
        assert isinstance(result, dict)

    def test_compute_residual_trend(self):
        attr = PnLAttributor()
        attr.add_trade(self._make_trade())
        ar = attr.compute_attribution()
        result = attr.compute_residual_trend(ar)
        assert isinstance(result, list)

    def test_detect_attribution_anomalies(self):
        attr = PnLAttributor()
        attr.add_trade(self._make_trade())
        ar = attr.compute_attribution()
        result = attr.detect_attribution_anomalies(ar)
        assert isinstance(result, list)

    def test_compute_factor_exposure_timeseries(self):
        attr = PnLAttributor()
        result = attr.compute_factor_exposure_timeseries(market_returns=[0.01, -0.005, 0.02])
        assert isinstance(result, dict)

    def test_compute_risk_return_map(self):
        attr = PnLAttributor()
        attr.add_trade(self._make_trade())
        ra = attr.compute_risk_attribution()
        result = attr.compute_risk_return_map(ra)
        assert isinstance(result, dict)

    def test_clear(self):
        attr = PnLAttributor()
        attr.add_trade(self._make_trade())
        attr.clear()


class TestAttributionEngine:
    def test_init(self):
        engine = AttributionEngine()
        assert engine is not None

    def test_add_trade_and_generate(self):
        engine = AttributionEngine()
        trade = TradeRecord(
            strategy='test', instrument='AU2506', direction=Direction.LONG,
            time_segment=TimeSegment.OPEN, pnl=10.0, commission=1.0,
        )
        engine.add_trade(trade)
        report = engine.generate_report(strategy='test')
        assert report is not None

    def test_get_history(self):
        engine = AttributionEngine()
        trade = TradeRecord(
            strategy='test', instrument='AU2506', direction=Direction.LONG,
            time_segment=TimeSegment.OPEN, pnl=10.0, commission=1.0,
        )
        engine.add_trade(trade)
        engine.generate_report(strategy='test')
        history = engine.get_history()
        assert isinstance(history, list)

    def test_clear(self):
        engine = AttributionEngine()
        engine.clear()

    def test_format_report_text(self):
        engine = AttributionEngine()
        trade = TradeRecord(
            strategy='test', instrument='AU2506', direction=Direction.LONG,
            time_segment=TimeSegment.OPEN, pnl=10.0, commission=1.0,
        )
        engine.add_trade(trade)
        report = engine.generate_report(strategy='test')
        text = engine.format_report_text(report)
        assert isinstance(text, str)
