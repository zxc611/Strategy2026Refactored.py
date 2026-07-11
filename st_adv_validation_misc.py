# MODULE_ID: M2-301
"""Tests for ali2026v3_trading.param_pool.adv_validation_misc shim."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest


def test_import_success():
    from ali2026v3_trading.param_pool import adv_validation_misc
    assert adv_validation_misc is not None


def test_dir_not_empty():
    from ali2026v3_trading.param_pool import adv_validation_misc
    names = dir(adv_validation_misc)
    assert isinstance(names, list)
    assert len(names) > 0


def test_access_OtherStateDefenseQuantifier():
    from ali2026v3_trading.param_pool import adv_validation_misc
    cls = adv_validation_misc.OtherStateDefenseQuantifier
    assert cls is not None
    instance = cls()
    result = instance.quantify()
    assert isinstance(result, dict)
    assert "defense_valuable" in result


def test_access_PARAM_TIERS():
    from ali2026v3_trading.param_pool import adv_validation_misc
    tiers = adv_validation_misc.PARAM_TIERS
    assert isinstance(tiers, dict)
    assert "must_calibrate_every_run" in tiers
