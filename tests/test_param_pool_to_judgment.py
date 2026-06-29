#!/usr/bin/env python3
# MODULE_ID: M2-458
"""参数池→评判引擎全链路测试"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def test_parameter_pool_adapter_import():
    from ali2026v3_trading.strategy_judgment.parameter_pool_adapter import judge_backtest_result
    assert callable(judge_backtest_result)

def test_parameter_pool_adapter_sweep():
    from ali2026v3_trading.strategy_judgment.parameter_pool_adapter import judge_sweep_results
    assert callable(judge_sweep_results)

def test_result_key_map_exists():
    """验证验RESULT_KEY_MAP映射表存在"""
    from ali2026v3_trading.strategy_judgment.parameter_pool_adapter import _RESULT_KEY_MAP
    assert isinstance(_RESULT_KEY_MAP, dict)
    assert len(_RESULT_KEY_MAP) > 0

def test_judgment_engine_import():
    from ali2026v3_trading.strategy_judgment.strategy_judgment_facade import StrategyJudgmentEngine
    assert StrategyJudgmentEngine is not None

def test_judgment_report_type():
    from ali2026v3_trading.strategy_judgment.judgment_types import JudgmentReport
    assert JudgmentReport is not None

if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            try:
                fn()
                print(f'[PASS] {name}')
            except Exception as e:
                print(f'[FAIL] {name}: {e}')
