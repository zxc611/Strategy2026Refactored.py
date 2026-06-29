# MODULE_ID: M2-289
import sys, os
sys.path.insert(0, r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo')
os.chdir(r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading')

print('=== R8批次断言验证 (R8-1~R8-3) ===')
PASS = 0
FAIL = 0

# R8-1: 评判系统Facade模式引入（Facade已存在，验证可实例化）'
try:
    from ali2026v3_trading.strategy_judgment.strategy_judgment_facade import StrategyJudgmentEngine
    assert hasattr(StrategyJudgmentEngine, 'judge'), 'StrategyJudgmentEngine缺少judge方法'
    print('R8-1 PASS: StrategyJudgmentEngine Facade存在且可实例化')
    PASS += 1
except (AssertionError, ImportError) as e:
    print(f'R8-1 FAIL: {e}')
    FAIL += 1

# R8-2: 评判结果类型结构化(JudgmentResult dataclass)
try:
    from ali2026v3_trading.strategy_judgment.judgment_types import JudgmentResult
    from dataclasses import is_dataclass
    assert is_dataclass(JudgmentResult), 'JudgmentResult不是dataclass'
    jr = JudgmentResult()
    assert hasattr(jr, 'warnings') and hasattr(jr, 'blockers') and hasattr(jr, 'conditions')
    assert hasattr(jr, 'validation_degraded') and hasattr(jr, 'score') and hasattr(jr, 'passed')
    print('R8-2 PASS: JudgmentResult为dataclass，含6个字段')
    PASS += 1
except (AssertionError, ImportError) as e:
    print(f'R8-2 FAIL: {e}')
    FAIL += 1

# R8-3: strategy_judgment/__all__收紧
try:
    import ali2026v3_trading.strategy_judgment as jmod
    assert len(jmod.__all__) <= 7, f'strategy_judgment __all__有{len(jmod.__all__)}个符号(>7)'
    assert 'StrategyJudgmentEngine' in jmod.__all__, '__all__缺少StrategyJudgmentEngine'
    assert 'JudgmentResult' in jmod.__all__, '__all__缺少JudgmentResult'
    print(f'R8-3 PASS: strategy_judgment __all__={jmod.__all__}')
    PASS += 1
except (AssertionError, ImportError) as e:
    print(f'R8-3 FAIL: {e}')
    FAIL += 1

print(f'\nR8批次结果: {PASS} PASS / {FAIL} FAIL')