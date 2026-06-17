# MODULE_ID: M2-328
"""Tests for ali2026v3_trading.param_pool.cycle_resonance_module shim."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest


def test_import_success():
    from ali2026v3_trading.param_pool import cycle_resonance_module
    assert cycle_resonance_module is not None


def test_dir_not_empty():
    from ali2026v3_trading.param_pool import cycle_resonance_module
    names = dir(cycle_resonance_module)
    assert isinstance(names, list)
    assert len(names) > 0


def test_access_CRParams():
    from ali2026v3_trading.param_pool import cycle_resonance_module
    cls = cycle_resonance_module.CRParams
    assert cls is not None
    instance = cls()
    assert instance is not None
    assert instance.hmm_entropy_window == 20


def test_access_Phase():
    from ali2026v3_trading.param_pool import cycle_resonance_module
    enum_cls = cycle_resonance_module.Phase
    assert enum_cls is not None


def test_access_CycleResonanceModule():
    from ali2026v3_trading.param_pool import cycle_resonance_module
    cls = cycle_resonance_module.CycleResonanceModule
    assert cls is not None
