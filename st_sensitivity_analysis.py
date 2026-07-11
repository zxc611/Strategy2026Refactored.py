# MODULE_ID: M2-570
"""Tests for ali2026v3_trading.param_pool.sensitivity_analysis shim."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest


def test_import_success():
    from ali2026v3_trading.param_pool import sensitivity_analysis
    assert sensitivity_analysis is not None


def test_dir_not_empty():
    from ali2026v3_trading.param_pool import sensitivity_analysis
    names = dir(sensitivity_analysis)
    assert isinstance(names, list)
    assert len(names) > 0


def test_access_SensitivityAnalyzer():
    from ali2026v3_trading.param_pool import sensitivity_analysis
    cls = sensitivity_analysis.SensitivityAnalyzer
    assert cls is not None


def test_access_SensitivityResult():
    from ali2026v3_trading.param_pool import sensitivity_analysis
    cls = sensitivity_analysis.SensitivityResult
    assert cls is not None


def test_access_get_sensitivity_analyzer():
    from ali2026v3_trading.param_pool import sensitivity_analysis
    func = sensitivity_analysis.get_sensitivity_analyzer
    assert callable(func)
