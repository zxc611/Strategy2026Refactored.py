# MODULE_ID: M2-294
"""
pytest conftest: 全局单例重置，解决测试间状态污染
"""
import os
import random
import logging
import pytest
import sys

import numpy as np

logging.raiseExceptions = False

# 确保从ali2026v3_trading目录内运行测试时，父目录也在sys.path中
# 这样ali2026v3_trading包的根级模块（如strategy_core_service.py等重导出模块）可以被正确导入
_project_root = os.path.join(os.path.dirname(__file__), '..', '..')
_project_root = os.path.abspath(_project_root)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def _reset_all_singletons():
    """强制重置所有ali2026v3_trading模块的全局单例"""
    modules_to_reset = [
        k for k in sys.modules
        if k.startswith('ali2026v3_trading') and k != 'ali2026v3_trading.tests.conftest'
    ]
    singleton_attrs = [
        '_global_instance', '_instance', '_global_signal_service',
        '_global_risk_service', '_global_position_service',
        '_global_order_service', '_global_event_bus',
        '_singleton', '_instance_lock', '_init_lock',
        '_safety_meta_layer', '_strategy_safety_layers',
        '_config_service_instance',
        '_shadow_engine',
    ]
    for mod_name in modules_to_reset:
        mod = sys.modules.get(mod_name)
        if mod is None:
            continue
        for attr in singleton_attrs:
            if hasattr(mod, attr):
                val = getattr(mod, attr)
                if val is not None and not isinstance(val, type) and not callable(val):
                    try:
                        setattr(mod, attr, None)
                    except (ValueError, KeyError, TypeError, AttributeError):
                        pass


def _cleanup_circuit_breaker_state():
    """删除SafetyMetaLayer持久化状态文件"""
    import glob
    _risk_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              '..', '..', 'ali2026v3_trading', 'risk')
    _risk_dir = os.path.abspath(_risk_dir)
    for pattern in ['.circuit_breaker_state.json', '.circuit_breaker_state.json.tmp']:
        _path = os.path.join(_risk_dir, pattern)
        if os.path.exists(_path):
            try:
                os.remove(_path)
            except (ValueError, KeyError, TypeError, AttributeError):
                pass


@pytest.fixture(autouse=True)
def _isolate_global_state():
    # 清理SafetyMetaLayer持久化状态文件（必须在单例重置之前，防止load_state加载残留状态）
    _cleanup_circuit_breaker_state()
    _reset_all_singletons()
    # AP-03: SingletonRegistry全局重置
    try:
        from ali2026v3_trading.infra.singleton_registry import SingletonRegistry
        SingletonRegistry.reset_all()
    except (ValueError, KeyError, TypeError, AttributeError):
        pass
    yield
    _reset_all_singletons()
    try:
        from ali2026v3_trading.infra.singleton_registry import SingletonRegistry
        SingletonRegistry.reset_all()
    except (ValueError, KeyError, TypeError, AttributeError):
        pass
    _cleanup_circuit_breaker_state()


# P2-23修复: 统一测试随机种子fixture
_DEFAULT_TEST_SEED = 42


@pytest.fixture
def seeded_random():
    """统一设置random和numpy随机种子，确保测试可复现性

    用法: 在测试函数参数中声明 seeded_random 即可自动设置种子。
    返回值为种子值(int)，方便需要传递种子的场景。
    """
    from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import set_global_random_seed
    return set_global_random_seed(_DEFAULT_TEST_SEED)
