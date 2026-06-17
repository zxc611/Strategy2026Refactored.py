# MODULE_ID: M2-283
import sys, os
sys.path.insert(0, r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo')
os.chdir(r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading')

print('=== 第2组端到端断言验证 (R2-1~R2-5) ===')
PASS = 0
FAIL = 0

# R2-1: ShadowStrategyFacade改为纯组合
try:
    with open('strategy/shadow_strategy_facade.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert 'class ShadowStrategyEngine' in src, 'ShadowStrategyEngine类不存在'
    assert 'ShadowStrategyCoreService' not in src.split('class ShadowStrategyEngine')[1].split(':')[0], '仍继承ShadowStrategyCoreService'
    assert 'self._core_service' in src, '未使用组合模式持有_core_service'
    assert 'self._signal_service' in src, '未使用组合模式持有_signal_service'
    assert 'self._pnl_service' in src, '未使用组合模式持有_pnl_service'
    lines = src.split('\n')
    class_line = None
    for i, line in enumerate(lines):
        if 'class ShadowStrategyEngine' in line:
            class_line = line
            break
    assert class_line is not None
    assert 'ShadowStrategyCoreService' not in class_line, 'Facade仍继承CoreService'
    print('R2-1 PASS: ShadowStrategyFacade已改为纯组合')
    PASS += 1
except AssertionError as e:
    print(f'R2-1 FAIL: {e}')
    FAIL += 1

# R2-2: CascadeJudge架构命名与手册V7.5对齐
try:
    with open('evaluation/cascade_judge.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert 'gate_l1_rule_engine' in src, '未添加gate_l1_rule_engine方法'
    assert 'gate_l2_ml_model' in src, '未添加gate_l2_ml_model方法'
    assert 'gate_l3_shadow_stealth' in src, '未添加gate_l3_shadow_stealth方法'
    print('R2-2 PASS: CascadeJudge架构命名已与手册V7.5对齐')
    PASS += 1
except AssertionError as e:
    print(f'R2-2 FAIL: {e}')
    FAIL += 1

# R2-3: 风控委托链统一
try:
    with open('risk/risk_service.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert 'self._check_service.check_regulatory_compliance' in src, 'check_regulatory_compliance未走_check_service委托'
    assert 'self._check_service.check_capital_sufficiency' in src, 'check_capital_sufficiency未走_check_service委托'
    assert 'self._check_service.check_exchange_status' in src, 'check_exchange_status未走_check_service委托'
    print('R2-3 PASS: 风控委托链已统一走_check_service')
    PASS += 1
except AssertionError as e:
    print(f'R2-3 FAIL: {e}')
    FAIL += 1

# R2-4: 参数三源一致性审计
try:
    with open('strategy/shadow_strategy_core.py', 'r', encoding='utf-8') as f:
        src = f.read()
    assert "os.path.join(\n            os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'params.yaml'\n        )" in src or "'..', 'config', 'params.yaml'" in src, 'YAML路径未修正为config/params.yaml'
    assert os.path.exists('param_pool/_audit_param_triple_source.py'), 'param_pool/_audit_param_triple_source.py审计脚本不存在'
    with open('param_pool/_audit_param_triple_source.py', 'r', encoding='utf-8') as f:
        audit_src = f.read()
    assert 'audit_triple_source' in audit_src, '审计脚本未导出audit_triple_source'
    print('R2-4 PASS: YAML路径已修正+审计脚本已创建')
    PASS += 1
except AssertionError as e:
    print(f'R2-4 FAIL: {e}')
    FAIL += 1

# R2-5: ShadowStrategyCore模块添加下划线前缀
try:
    assert os.path.exists('strategy/_shadow_strategy_core.py'), '_shadow_strategy_core.py不存在'
    assert os.path.exists('strategy/_shadow_strategy_signal.py'), '_shadow_strategy_signal.py不存在'
    assert os.path.exists('strategy/_shadow_strategy_pnl.py'), '_shadow_strategy_pnl.py不存在'
    with open('strategy/_shadow_strategy_core.py', 'r', encoding='utf-8') as f:
        core_shim = f.read()
    assert 'DEPRECATED' in core_shim or 'shadow_strategy_core' in core_shim, '_shadow_strategy_core.py不是有效shim'
    with open('strategy/shadow_strategy_facade.py', 'r', encoding='utf-8') as f:
        facade_src = f.read()
    assert '_shadow_strategy_signal' in facade_src, 'facade未导入_shadow_strategy_signal'
    print('R2-5 PASS: 下划线前缀shim已创建+facade已更新导入')
    PASS += 1
except AssertionError as e:
    print(f'R2-5 FAIL: {e}')
    FAIL += 1

print(f'\n第2组结果: {PASS} PASS / {FAIL} FAIL')