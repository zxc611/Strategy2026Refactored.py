# MODULE_ID: M2-287
import sys, os
sys.path.insert(0, r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo')
os.chdir(r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading')

print('=== R6批次断言验证 (R6-1~R6-5) ===')
PASS = 0
FAIL = 0

# R6-1: DeepValidationSuite上下文适配
try:
    with open('strategy_judgment/_judgment_services.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert 'bar_data=' in src, 'DVS调用未传入bar_data参数'
    assert '_dvs_params' in src or 'backtest_params' in src, 'DVS的params参数未改为回测参数'
    assert '_dvs_context' not in src, '仍使用旧的指dvs_context'
    print('R6-1 PASS: DVS上下文已适配为回测参数+bar_data')
    PASS += 1
except AssertionError as e:
    print(f'R6-1 FAIL: {e}')
    FAIL += 1

# R6-2: CascadeJudge gate方法与judge()逻辑同步
try:
    with open('evaluation/cascade_judge.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert 'def gate_l2_ml_model(self, metrics: BacktestMetrics) -> List[GateReport]:' in src, 'gate_l2未改为返回List[GateReport]'
    assert 'def gate_l3_shadow_stealth(self, metrics: BacktestMetrics) -> List[GateReport]:' in src, 'gate_l3未改为返回List[GateReport]'
    assert 'L2-索提诺验证' in src, 'gate_l2未拆分为索提诺子gate'
    assert 'L2-卡玛验证' in src, 'gate_l2未拆分为卡玛子gate'
    assert 'L2-夏普验证' in src, 'gate_l2未拆分为夏普子gate'
    assert 'L3-连续亏损约束' in src, 'gate_l3未扩展为连续亏损约束'
    assert 'L3-横盘天数约束' in src, 'gate_l3未扩展为横盘天数约束'
    assert 'L3-保证金约束' in src, 'gate_l3未扩展为保证金约束'
    print('R6-2 PASS: gate_l2/l3已与judge()逻辑同步')
    PASS += 1
except AssertionError as e:
    print(f'R6-2 FAIL: {e}')
    FAIL += 1

# R6-3: p0_gate_check返回类型结构化
try:
    with open('param_pool/optimization/phase_scan.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert 'class GateCheckResult(NamedTuple):' in src, 'GateCheckResult NamedTuple未定义'
    assert 'passed: bool' in src, 'GateCheckResult缺少passed字段'
    assert 'failures: List[str]' in src, 'GateCheckResult缺少failures字段'
    assert 'warnings: List[str]' in src, 'GateCheckResult缺少warnings字段'
    assert 'return GateCheckResult(' in src, 'p0_gate_check未返回GateCheckResult'
    assert 'failures + warnings' not in src.split('return GateCheckResult')[0].split('p0_gate_check')[-1], '仍合并failures+warnings'
    print('R6-3 PASS: p0_gate_check返回GateCheckResult NamedTuple')
    PASS += 1
except AssertionError as e:
    print(f'R6-3 FAIL: {e}')
    FAIL += 1

# R6-4: 风控检查结果类型结构化
try:
    with open('risk/risk_service.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert 'class ComplianceResult(TypedDict' in src, 'ComplianceResult TypedDict未定义'
    assert 'class SufficiencyResult(TypedDict' in src, 'SufficiencyResult TypedDict未定义'
    assert 'class ExchangeStatusResult(TypedDict' in src, 'ExchangeStatusResult TypedDict未定义'
    assert '-> ComplianceResult:' in src, 'check_regulatory_compliance未使用ComplianceResult返回类型'
    assert '-> SufficiencyResult:' in src, 'check_capital_sufficiency未使用SufficiencyResult返回类型'
    assert '-> ExchangeStatusResult:' in src, 'check_exchange_status未使用ExchangeStatusResult返回类型'
    print('R6-4 PASS: 风控3方法返回类型已结构化')
    PASS += 1
except AssertionError as e:
    print(f'R6-4 FAIL: {e}')
    FAIL += 1

# R6-5: __all__白名单收紧
try:
    import importlib
    rs_mod = importlib.import_module('ali2026v3_trading.risk.risk_service')
    assert len(rs_mod.__all__) <= 7, f'risk_service __all__有{len(rs_mod.__all__)}个符号(>7)'
    pp_mod = importlib.import_module('ali2026v3_trading.param_pool')
    assert len(pp_mod.__all__) <= 7, f'param_pool __all__有{len(pp_mod.__all__)}个符号(>7)'
    cj_mod = importlib.import_module('ali2026v3_trading.evaluation.cascade_judge')
    assert hasattr(cj_mod, '__all__'), 'cascade_judge缺少。_all__'
    assert len(cj_mod.__all__) <= 7, f'cascade_judge __all__有{len(cj_mod.__all__)}个符号(>7)'
    print(f'R6-5 PASS: __all__收紧完成 (risk={len(rs_mod.__all__)}, param_pool={len(pp_mod.__all__)}, cascade_judge={len(cj_mod.__all__)})')
    PASS += 1
except (AssertionError, ImportError) as e:
    print(f'R6-5 FAIL: {e}')
    FAIL += 1

print(f'\nR6批次结果: {PASS} PASS / {FAIL} FAIL')