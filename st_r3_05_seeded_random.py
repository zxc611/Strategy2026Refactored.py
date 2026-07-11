# MODULE_ID: M2-531
"""R3-5 断言测试: P2-23 测试随机种子统一 — seeded_random fixture + set_global_random_seed

验证项:
1. set_global_random_seed函数存在于backtest_runner_utils
2. set_global_random_seed同时设置random和np.random种子
3. seeded_random fixture在conftest.py中定义
4. 测试文件中手动seed调用已替换为fixture或setUp
5. seeded_random返回种子值
"""
import sys
import os
import re
import random
import pytest
import numpy as np

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


class TestSeededRandom:
    """R3-5: 测试随机种子统一断言测试"""

    def test_set_global_random_seed_exists(self):
        """验证set_global_random_seed可从backtest_runner_utils导入"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import set_global_random_seed
        assert callable(set_global_random_seed)

    def test_set_global_random_seed_sets_both(self):
        """验证set_global_random_seed同时设置random和np.random"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import set_global_random_seed

        # 先用不同种子打乱
        random.seed(999)
        np.random.seed(999)

        # 调用统一函数
        result = set_global_random_seed(42)
        assert result == 42, "应返回seed值"

        # 验证random模块种子已设置
        val1 = random.random()
        random.seed(42)
        val2 = random.random()
        assert val1 == val2, "random种子应已设置"

        # 验证np.random种子已设置
        arr1 = np.random.randn(5).tolist()
        np.random.seed(42)
        arr2 = np.random.randn(5).tolist()
        assert arr1 == arr2, "np.random种子应已设置"

    def test_seeded_random_fixture_returns_seed(self, seeded_random):
        """验证seeded_random fixture返回种子值"""
        assert seeded_random == 42

    def test_seeded_random_fixture_sets_seed(self, seeded_random):
        """验证seeded_random fixture实际设置了种子"""
        # seeded_random已通过fixture设置种子
        val1 = random.random()
        random.seed(42)
        val2 = random.random()
        assert val1 == val2, "fixture应已设置random种子"

    def test_conftest_has_seeded_random_fixture(self):
        """验证conftest.py中定义了seeded_random fixture"""
        conftest_path = os.path.join(_project_root, "tests", "conftest.py")
        with open(conftest_path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'def seeded_random()' in source, "conftest.py应包含seeded_random fixture"
        assert 'set_global_random_seed' in source, "conftest.py应使用set_global_random_seed"

    def test_test_files_use_fixture_or_setup(self):
        """验证测试文件中手动np.random.seed(42)已替换为fixture或setUp"""
        test_files = [
            os.path.join(_project_root, "tests", "test_crack_validation.py"),
            os.path.join(_project_root, "tests", "test_kline_length_backtest.py"),
        ]
        for tf in test_files:
            if not os.path.exists(tf):
                continue
            with open(tf, 'r', encoding='utf-8') as f:
                source = f.read()
            # 不应有裸的 np.random.seed(42) 在方法体中（setUp除外）'
            # 检查是否有seeded_random参数或setUp
            if 'np.random.seed(42)' in source:
                # 如果仍有，必须只在setUp中
                lines = source.split('\n')
                for i, line in enumerate(lines):
                    if 'np.random.seed(42)' in line and 'def setUp' not in lines[max(0, i-3):i+1]:
                        # 允许在setUp方法内
                        in_setup = False
                        for j in range(max(0, i-10), i+1):
                            if 'def setUp' in lines[j]:
                                in_setup = True
                                break
                        assert in_setup, f"{os.path.basename(tf)} 第{i+1}行仍有裸np.random.seed(42)"
