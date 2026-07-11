# MODULE_ID: M2-510
"""R1-5: 验证 position_service 提取 _update_tp_sl_from_params 消除重复"""
import inspect
import pytest


def test_update_tp_sl_from_params_exists():
    """PositionService 应有 _update_tp_sl_from_params 方法"""
    from ali2026v3_trading.position.position_service import PositionService
    assert hasattr(PositionService, '_update_tp_sl_from_params'), \
        "PositionService 应有 _update_tp_sl_from_params 方法"


def test_on_config_param_change_delegates():
    """_on_config_param_change 应委托给 _update_tp_sl_from_params"""
    from ali2026v3_trading.position.position_service import PositionService
    source = inspect.getsource(PositionService._on_config_param_change)
    assert '_update_tp_sl_from_params' in source, \
        "_on_config_param_change 应调用 _update_tp_sl_from_params"
    assert 'get_cached_params' not in source, \
        "_on_config_param_change 不应直接调用 get_cached_params"


def test_on_param_changed_event_delegates():
    """_on_param_changed_event 应委托给 _update_tp_sl_from_params"""
    from ali2026v3_trading.position.position_service import PositionService
    source = inspect.getsource(PositionService._on_param_changed_event)
    assert '_update_tp_sl_from_params' in source, \
        "_on_param_changed_event 应调用 _update_tp_sl_from_params"
    assert 'get_cached_params' not in source, \
        "_on_param_changed_event 不应直接调用 get_cached_params"


def test_no_duplicate_get_cached_params_in_callbacks():
    """两个回调方法中不应有重复的 get_cached_params 调用"""
    from ali2026v3_trading.position.position_service import PositionService
    src1 = inspect.getsource(PositionService._on_config_param_change)
    src2 = inspect.getsource(PositionService._on_param_changed_event)
    total = src1.count('get_cached_params') + src2.count('get_cached_params')
    assert total == 0, \
        f"两个回调方法中共有 {total} 处 get_cached_params 调用，应全部委托到 _update_tp_sl_from_params"
