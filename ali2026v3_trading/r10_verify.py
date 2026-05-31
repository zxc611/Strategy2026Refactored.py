
import py_compile
import os

base = 'c:/Users/xu/AppData/Roaming/InfiniTrader_SimulationX64/pyStrategy/demo/ali2026v3_trading'
files = [
    'mode_engine.py',
    'config_params.py',
    'strategy_ecosystem.py',
    'strategy_core_service.py',
    'ds_schema_manager.py',
    'signal_service.py',
    'risk_service.py',
    'shadow_strategy_engine.py',
    'order_service.py',
    'state_param_manager.py',
    'width_cache.py',
    os.path.join('策略评判', 'strategy_judgment_engine.py'),
    os.path.join('参数池', 'task_scheduler.py'),
    os.path.join('参数池', 'cycle_resonance_module.py'),
]

all_pass = True
for f in files:
    path = os.path.join(base, f)
    try:
        py_compile.compile(path, doraise=True)
        print(f'PASS: {f}')
    except py_compile.PyCompileError as e:
        print(f'FAIL: {f} - {e}')
        all_pass = False

if all_pass:
    print('All py_compile checks PASSED')
else:
    print('Some files FAILED')
