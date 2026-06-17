# MODULE_ID: M2-488
"""R5-F2: P1-41 LRU缓存策略分散实现统一 断言测试

验证运行时行为:
1. CachedParamTableProvider使用functools.lru_cache
2. 缓存命中行为正确
3. invalidate_cache调用lru_cache的标准清除API
4. config_params._cached_get_param使用@lru_cache
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_cached_param_table_uses_lru_cache():
    """P1-41验证: CachedParamTableProvider使用functools.lru_cache"""
    from ali2026v3_trading.governance.param_table_provider import (
        CachedParamTableProvider, ParamTableProvider,
    )

    class MockProvider:
        def get_params(self, name):
            return {'key': 'value_' + name}
        def get_default(self, key):
            return None
        def list_strategies(self):
            return ['test']

    provider = MockProvider()
    cached = CachedParamTableProvider(provider, cache_size=16)
    # 验证有_cached_get_params属性（lru_cache包装的函数）
    assert hasattr(cached, '_cached_get_params'), \
        "CachedParamTableProvider应有_cached_get_params属性"
    # lru_cache包装的函数有cache_info方法
    assert hasattr(cached._cached_get_params, 'cache_info'), \
        "_cached_get_params应有cache_info方法（lru_cache特征）"


def test_cached_param_table_cache_hit():
    """P1-41验证: 缓存命中行为正确"""
    from ali2026v3_trading.governance.param_table_provider import (
        CachedParamTableProvider,
    )

    call_count = 0

    class MockProvider:
        def get_params(self, name):
            nonlocal call_count
            call_count += 1
            return {'key': 'value_' + name}
        def get_default(self, key):
            return None
        def list_strategies(self):
            return ['test']

    provider = MockProvider()
    cached = CachedParamTableProvider(provider, cache_size=16)

    # 第一次调用
    r1 = cached.get_params('strategy_a')
    assert call_count == 1, f"第一次调用应触发provider，实际调用{call_count}次"

    # 第二次调用（应命中缓存）
    r2 = cached.get_params('strategy_a')
    assert call_count == 1, f"第二次调用应命中缓存，实际调用{call_count}次"
    assert r1 == r2


def test_invalidate_cache_uses_lru_api():
    """P1-41验证: invalidate_cache调用lru_cache的标准清除API"""
    from ali2026v3_trading.governance.param_table_provider import (
        CachedParamTableProvider,
    )
    import inspect
    src = inspect.getsource(CachedParamTableProvider.invalidate_cache)
    assert 'cache_clear' in src, \
        "invalidate_cache应使用lru_cache的cache_clear API"


def test_config_params_uses_lru_cache():
    """P1-41验证: config_params._cached_get_param使用@lru_cache"""
    _path = os.path.join(_project_root, 'config', 'config_params.py')
    with open(_path, encoding='utf-8') as f:
        src = f.read()
    assert '@functools.lru_cache' in src or '@lru_cache' in src, \
        "config_params应使用@lru_cache"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
