# MODULE_ID: M2-494
"""P1-55断言测试: Tick字段提取统一——验证运行时行为"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'ali2026v3_trading'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


class _MockTick:
    """模拟tick对象"""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def test_get_tick_field_shared_utils():
    """shared_utils.get_tick_field 是唯一权威实现，能正确提取字段"""
    from ali2026v3_trading.infra.shared_utils import get_tick_field

    tick = _MockTick(instrument_id='IF2506', LastPrice=3850.0, Volume=100)
    assert get_tick_field(tick, 'instrument_id') == 'IF2506'
    assert get_tick_field(tick, 'last_price') == 3850.0
    assert get_tick_field(tick, 'volume') == 100
    assert get_tick_field(tick, 'bid_price1', default=0) == 0  # 不存在的字段返回default


def test_get_tick_field_delegates_from_tick_processing_service():
    """tick_processing_service._get_tick_field 委托到 shared_utils"""
    from ali2026v3_trading.infra.shared_utils import get_tick_field
    import inspect

    # 验证tick_processing_service._get_tick_field源码中包含委托调用
    src = inspect.getsource(
        __import__('ali2026v3_trading.strategy.tick_processing_service',
                    fromlist=['tick_processing_service']).TickProcessingService._get_tick_field
    )
    assert 'shared_utils' in src, "_get_tick_field未委托到shared_utils"
    assert 'get_tick_field' in src, "_get_tick_field未调用get_tick_field"


def test_get_platform_attr_delegates_to_shared_utils():
    """position_service._get_platform_attr 委托到 shared_utils"""
    import inspect

    src = inspect.getsource(
        __import__('ali2026v3_trading.position.position_service',
                    fromlist=['position_service']).PositionService._get_platform_attr
    )
    assert 'shared_utils' in src, "_get_platform_attr未委托到shared_utils"
    assert 'get_tick_field' in src, "_get_platform_attr未调用get_tick_field"


def test_validate_out_of_order_ticks_delegates():
    """feature_engine.validate_out_of_order_ticks 委托到 preprocess_validation"""
    import inspect
    from ali2026v3_trading.param_pool import _preprocess as feature_engine

    src = inspect.getsource(feature_engine.validate_out_of_order_ticks)
    assert 'preprocess_validation' in src, "validate_out_of_order_ticks未委托到preprocess_validation"


def test_check_minute_boundary_delegates():
    """_preprocess.check_minute_boundary_integrity 是权威实现"""
    from ali2026v3_trading.param_pool import _preprocess
    assert callable(_preprocess.check_minute_boundary_integrity), \
        "check_minute_boundary_integrity should be callable"


def test_normalize_tick_shared_utils():
    """shared_utils.normalize_tick 能正确标准化tick对象"""
    from ali2026v3_trading.infra.shared_utils import normalize_tick

    tick = _MockTick(instrument_id='IF2506', LastPrice=3850.0, Volume=100)
    result = normalize_tick(tick)
    assert isinstance(result, dict)
    assert result['instrument_id'] == 'IF2506'
    assert result['last_price'] == 3850.0
    assert result['volume'] == 100


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
