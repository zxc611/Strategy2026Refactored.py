# MODULE_ID: M2-375
import sys
import os
import pytest
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestMultiPeriodTrendScorer:
    def test_init(self):
        from ali2026v3_trading.data.quant_trend_scorer import MultiPeriodTrendScorer
        scorer = MultiPeriodTrendScorer(periods=(5, 20, 60))
        assert scorer is not None

    def test_init_custom_periods(self):
        from ali2026v3_trading.data.quant_trend_scorer import MultiPeriodTrendScorer
        scorer = MultiPeriodTrendScorer(periods=(5, 10))
        assert scorer is not None

    def test_update_returns_dict(self):
        from ali2026v3_trading.data.quant_trend_scorer import MultiPeriodTrendScorer
        scorer = MultiPeriodTrendScorer(periods=(5, 10))
        for i in range(15):
            result = scorer.update(
                high=501.0 + np.random.randn(),
                low=499.0 + np.random.randn(),
                close=500.0 + np.random.randn()
            )
        assert isinstance(result, dict)

    def test_update_single_tick(self):
        from ali2026v3_trading.data.quant_trend_scorer import MultiPeriodTrendScorer
        scorer = MultiPeriodTrendScorer(periods=(5, 10))
        result = scorer.update(high=501.0, low=499.0, close=500.0)
        assert isinstance(result, dict)

    def test_update_with_adx_period(self):
        from ali2026v3_trading.data.quant_trend_scorer import MultiPeriodTrendScorer
        scorer = MultiPeriodTrendScorer(periods=(5, 20), adx_period=7)
        for i in range(30):
            result = scorer.update(high=501.0 + i * 0.1, low=499.0 + i * 0.05, close=500.0 + i * 0.08)
        assert isinstance(result, dict)