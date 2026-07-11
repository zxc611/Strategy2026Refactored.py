# MODULE_ID: M2-306
"""Tests for ali2026v3_trading.param_pool.backtest_runner_base shim."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest


def test_import_success():
    from ali2026v3_trading.param_pool import backtest_runner_base
    assert backtest_runner_base is not None


def test_dir_not_empty():
    from ali2026v3_trading.param_pool import backtest_runner_base
    names = dir(backtest_runner_base)
    assert isinstance(names, list)
    assert len(names) > 0


def test_access_BacktestStateEnum():
    from ali2026v3_trading.param_pool import backtest_runner_base
    enum_cls = backtest_runner_base.BacktestStateEnum
    assert enum_cls is not None


def test_access_run_backtest():
    from ali2026v3_trading.param_pool import backtest_runner_base
    func = backtest_runner_base.run_backtest
    assert callable(func)
