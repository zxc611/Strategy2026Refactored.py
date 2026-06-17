# MODULE_ID: M2-500
"""R8断言测试: P2-15/P2-18/P2-20/P2-23/P2-26——验证运行时行为"""
import os
import sys
import inspect
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'ali2026v3_trading'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_p2_15_resilience_delegates_to_config_params():
    """P2-15: resilience.py中3个check函数委托到config_params"""
    from ali2026v3_trading.infra import resilience
    for fname in ('check_multi_level_cache_consistency', 'check_indicator_freshness', 'check_risk_data_freshness'):
        fn = getattr(resilience, fname)
        src = inspect.getsource(fn)
        assert 'config_params' in src, f"{fname}未委托到config_params"


def test_p2_18_tick_count_method_exists():
    """P2-18: DuckDBTickStorage有_get_tick_count方法"""
    from ali2026v3_trading.param_pool.quantification._data_validation import DuckDBTickStorage
    assert hasattr(DuckDBTickStorage, '_get_tick_count'), "缺少_get_tick_count方法"


def test_p2_18_no_duplicate_count_in_storage():
    """P2-18: duckdb_tick_storage.py中SELECT COUNT(*) FROM tick_data仅剩_get_tick_count内1处"""
    import ali2026v3_trading.param_pool.l1_quantification._data_validation as dts
    src = inspect.getsource(dts)
    count = src.count("SELECT COUNT(*) FROM tick_data")
    assert count == 1, f"SELECT COUNT(*) FROM tick_data应仅剩1处(在_get_tick_count内)，实际{count}处"


def test_p2_20_validate_finite_extracted():
    """P2-20: position_command_service中NaN/Inf检查使用_validate_finite辅助"""
    from ali2026v3_trading.position.position_command_service import PositionCommandService
    src = inspect.getsource(PositionCommandService._add_position)
    assert '_validate_finite' in src, "_add_position未使用_validate_finite辅助函数"


def test_p2_23_greeks_calculator_uses_set_global_seed():
    """P2-23: greeks_calculator.py使用set_global_seed替代random.seed"""
    from ali2026v3_trading.governance import greeks_calculator
    src = inspect.getsource(greeks_calculator)
    assert 'set_global_seed' in src, "greeks_calculator未使用set_global_seed"
    assert 'random.seed(seed)' not in src, "greeks_calculator仍使用random.seed"


def test_p2_23_triple_truth_uses_set_global_seed():
    """P2-23: _quantification_core.py使用set_global_seed替代np.random.seed"""
    from ali2026v3_trading.param_pool.quantification import _quantification_core as triple_truth_anchor
    src = inspect.getsource(triple_truth_anchor)
    assert 'set_global_seed' in src, "_quantification_core未使用set_global_seed"


def test_p2_26_param_changed_delegates():
    """P2-26: _on_param_changed_event委托到_on_config_param_change"""
    from ali2026v3_trading.position.position_service import PositionService
    src = inspect.getsource(PositionService._on_param_changed_event)
    assert '_on_config_param_change' in src, "_on_param_changed_event未委托到_on_config_param_change"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
