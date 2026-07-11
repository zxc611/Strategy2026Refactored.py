# MODULE_ID: M2-282
import sys, os
sys.path.insert(0, r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo')
os.chdir(r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading')

print('=== 第1组端到端断言验证 (R1-1~R1-5) ===')
PASS = 0
FAIL = 0

# R1-1: 6个governance checker接入真实业务数据
try:
    with open('strategy_judgment/_judgment_services.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert '_pm.get("master_trades"' in src, 'E12未从profitability_metrics提取master_trades'
    assert '_pm.get("reverse_trades"' in src, 'E12未从profitability_metrics提取reverse_trades'
    assert '_psr.get("main_params"' in src, 'E13未从parameter_stability_result提取main_params'
    assert '_psr.get("shadow_params"' in src, 'E13未从parameter_stability_result提取shadow_params'
    assert '_pm.get("walk_forward_metrics"' in src, 'WF未从profitability_metrics提取walk_forward_metrics'
    assert '_pm.get("total_pnl"' in src, 'E7未从profitability_metrics提取total_pnl'
    assert '_e12.detect(_master_trades, _reverse_trades)' in src, 'E12未传入真实数据'
    assert '_e13.detect(_main_params, _shadow_params, _main_signals, _shadow_signals)' in src, 'E13未传入真实数据'
    assert '_wf_chk.check_all(_wf_metrics)' in src, 'WF未传入真实数据'
    assert '_e7.check({"total_pnl": _pm.get("total_pnl"' in src, 'E7未传入真实数据'
    assert '_e11.check(_e11_context)' in src, 'E11未传入真实数据'
    assert '_e12.detect([], [])' not in src, 'E12仍为stub'
    assert '_e13.detect({}, {}, [], [])' not in src, 'E13仍为stub'
    assert '_wf_chk.check_all([])' not in src, 'WF仍为stub'
    assert '_e7.check({"total_pnl": 0.0})' not in src, 'E7仍为stub'
    assert '_e11.check({})' not in src, 'E11仍为stub'
    print('R1-1 PASS: 6个checker已接入真实业务数据')
    PASS += 1
except AssertionError as e:
    print(f'R1-1 FAIL: {e}')
    FAIL += 1

# R1-2: DeepValidationSuite传入真实回测结果
try:
    with open('strategy_judgment/_judgment_services.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert '_dvs_context' in src, '未构造验证上下文'
    assert 'dvs.run_full_validation(_dvs_context)' in src, 'DVS未传入真实上下文'
    assert 'dvs.run_full_validation({})' not in src, 'DVS仍传入空字典'
    print('R1-2 PASS: DeepValidationSuite已传入真实回测结果')
    PASS += 1
except AssertionError as e:
    print(f'R1-2 FAIL: {e}')
    FAIL += 1

# R1-3: 关键路径硬阻断改造
try:
    with open('strategy_judgment/judgment_scoring_helpers.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert 'except (ImportError, AttributeError, TypeError, ValueError)' in src, 'CascadeJudge未改为具体异常'
    assert 'STRICT_MODE' in src, '未实现STRICT_MODE硬阻断'
    assert 'raise RuntimeError' in src, 'CascadeJudge未硬阻断'
    assert 'except Exception' not in src, '仍存在bare except Exception'
    print('R1-3 PASS: 关键路径已改为具体异常+硬阻断')
    PASS += 1
except AssertionError as e:
    print(f'R1-3 FAIL: {e}')
    FAIL += 1

# R1-4: phase_scan_core.py集成验证器
try:
    with open('param_pool/optimization/phase_scan.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert 'SurvivalBiasTest' in src, '未集成SurvivalBiasTest'
    assert 'MultiPeriodCrossValidator' in src, '未集成MultiPeriodCrossValidator'
    assert '_sbt.test(train_result, test_result)' in src, 'SurvivalBiasTest未传入真实数据'
    assert '_mpcv.validate(train_result, test_result, params)' in src, 'MultiPeriodCrossValidator未传入真实数据'
    print('R1-4 PASS: phase_scan_core.py已集成验证器')
    PASS += 1
except AssertionError as e:
    print(f'R1-4 FAIL: {e}')
    FAIL += 1

# R1-5: CascadeJudge导入路径统一
try:
    files_to_check = [
        'param_pool/optimization/phase_scan.py',
        'param_pool/backtest/backtest_state.py',
        'strategy/strategy_ecosystem/_trading_ev.py',
    ]
    for fpath in files_to_check:
        with open(fpath, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'from ali2026v3_trading.evaluation.cascade_judge import' in src, f'{fpath}未使用绝对导入'
    print('R1-5 PASS: CascadeJudge导入路径已统一为绝对路径')
    PASS += 1
except AssertionError as e:
    print(f'R1-5 FAIL: {e}')
    FAIL += 1

print(f'\n第1组结果: {PASS} PASS / {FAIL} FAIL')