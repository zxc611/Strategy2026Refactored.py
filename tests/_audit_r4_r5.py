# MODULE_ID: M2-285
import sys, os
sys.path.insert(0, r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo')
os.chdir(r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading')

print('=== 第4组端到端断言验证 (R4-1~R4-5) ===')
PASS = 0
FAIL = 0

# R4-1: infra/ 文件瘦身 (72→48, 瘦身33%)
try:
    py_files = [f for f in os.listdir('infra') if f.endswith('.py')]
    count = len(py_files)
    assert count <= 50, f'infra/仍有{count}个.py文件，超过50目标'
    print(f'R4-1 PASS: infra/当前{count}个.py文件（原72→{count}，瘦身{round((72-count)/72*100)}%）')
    PASS += 1
except AssertionError as e:
    print(f'R4-1 FAIL: {e}')
    FAIL += 1

# R4-2: config/ 文件瘦身 (35→19, 瘦身46%)
try:
    py_files = [f for f in os.listdir('config') if f.endswith('.py')]
    count = len(py_files)
    assert count <= 25, f'config/仍有{count}个.py文件，超过25目标'
    print(f'R4-2 PASS: config/当前{count}个.py文件（原35→{count}，瘦身{round((35-count)/35*100)}%）')
    PASS += 1
except AssertionError as e:
    print(f'R4-2 FAIL: {e}')
    FAIL += 1

# R4-3: strategy_judgment/ 工具脚本迁出
try:
    scripts = ['_split_v2.py', '_split_content.py', '_do_split.py', '_gen_rest.py', '_gen_all.py']
    found = []
    for s in scripts:
        if os.path.exists(os.path.join('strategy_judgment', s)):
            found.append(s)
    assert len(found) == 0, f'strategy_judgment/下仍有工具脚本: {found}'
    print('R4-3 PASS: strategy_judgment/工具脚本已全部迁出')
    PASS += 1
except AssertionError as e:
    print(f'R4-3 FAIL: {e}')
    FAIL += 1

# R4-4: RiskDashboardService决策（已从死代码变为被集成）'
try:
    with open('risk/risk_service.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert 'class RiskDashboardService' in src, 'RiskDashboardService类不存在'
    assert 'get_risk_dashboard_service()' in src, 'get_risk_dashboard_service()不存在'
    assert 'self._dashboard_service = get_risk_dashboard_service()' in src, 'RiskService.__init__未调用get_risk_dashboard_service()'
    assert 'def get_dashboard_service(self)' in src, 'RiskService未暴露get_dashboard_service方法'
    print('R4-4 PASS: RiskDashboardService已从死代码变为被集成（T1.2验收标准）')
    PASS += 1
except AssertionError as e:
    print(f'R4-4 FAIL: {e}')
    FAIL += 1

# R4-5: lifecycle_parallel合并
try:
    assert not os.path.exists('lifecycle/lifecycle_parallel_ops.py'), 'lifecycle_parallel_ops.py仍存在（未合并）'
    with open('lifecycle/lifecycle_parallel.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert 'lifecycle_parallel_ops' in src or 'Section 2' in src, 'lifecycle_parallel.py未包含合并内容'
    print('R4-5 PASS: lifecycle_parallel已合并')
    PASS += 1
except AssertionError as e:
    print(f'R4-5 FAIL: {e}')
    FAIL += 1

print(f'\n第4组结果: {PASS} PASS / {FAIL} FAIL')