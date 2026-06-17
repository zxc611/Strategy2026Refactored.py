# MODULE_ID: M2-356
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestBacktestOrchestrator:
    def test_import(self):
        from ali2026v3_trading.param_pool import backtest_orchestrator
        assert backtest_orchestrator is not None

    def test_has_run_backtest_init(self):
        from ali2026v3_trading.param_pool.backtest_orchestrator import _run_backtest_init
        assert callable(_run_backtest_init)

    def test_has_run_backtest_finalize(self):
        from ali2026v3_trading.param_pool.backtest_orchestrator import _run_backtest_finalize
        assert callable(_run_backtest_finalize)