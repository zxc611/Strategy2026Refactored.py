# MODULE_ID: M2-481
"""R3-F5: P1-54 随机数种子设置逻辑统一 断言测试

验证运行时行为:
1. set_global_seed权威实现在shared_utils.py
2. backtest_runner_utils.set_global_random_seed委托到set_global_seed
3. set_global_seed同时设置random和np.random种子
4. set_global_seed返回seed值
"""
import os
import sys
import random
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_set_global_seed_in_shared_utils():
    """P1-54验证: set_global_seed权威实现在shared_utils.py"""
    from ali2026v3_trading.infra.shared_utils import set_global_seed
    assert callable(set_global_seed)


def test_set_global_seed_sets_both_random_and_numpy():
    """P1-54验证: set_global_seed同时设置random和np.random种子"""
    from ali2026v3_trading.infra.shared_utils import set_global_seed
    import numpy as np

    set_global_seed(12345)
    r1 = random.random()
    n1 = np.random.random()

    set_global_seed(12345)
    r2 = random.random()
    n2 = np.random.random()

    assert r1 == r2, f"random种子未正确设置: {r1} != {r2}"
    assert n1 == n2, f"np.random种子未正确设置: {n1} != {n2}"


def test_backtest_runner_delegates_to_set_global_seed():
    """P1-54验证: backtest_runner_utils.set_global_random_seed委托到set_global_seed"""
    # 直接验证backtest_runner_utils.py源码中调用了set_global_seed
    _path = os.path.join(_project_root, 'param_pool', 'backtest', 'backtest_runner_utils.py')
    with open(_path, encoding='utf-8') as f:
        src = f.read()
    assert 'from ali2026v3_trading.infra.shared_utils import set_global_seed' in src, \
        "backtest_runner_utils.py应导入shared_utils.set_global_seed"
    assert 'return set_global_seed(seed)' in src, \
        "backtest_runner_utils.py应委托到set_global_seed"


def test_set_global_seed_returns_seed():
    """P1-54验证: set_global_seed返回seed值"""
    from ali2026v3_trading.infra.shared_utils import set_global_seed
    result = set_global_seed(999)
    assert result == 999, f"应返回999，实际返回{result}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
