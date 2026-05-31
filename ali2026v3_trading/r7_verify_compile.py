
import py_compile
import sys

files = [
    'mode_engine.py',
    'config_params.py',
    'strategy_ecosystem.py',
    'strategy_core_service.py',
    'ds_schema_manager.py',
    '策略评判/strategy_judgment_engine.py',
    '参数池/task_scheduler.py',
    'risk_service.py',
]

base = 'c:/Users/xu/AppData/Roaming/InfiniTrader_SimulationX64/pyStrategy/demo/ali2026v3_trading'
all_pass = True
for f in files:
    path = base + '/' + f
    try:
        py_compile.compile(path, doraise=True)
        print(f'PASS: {f}')
    except py_compile.PyCompileError as e:
        print(f'FAIL: {f} - {e}')
        all_pass = False

if all_pass:
    print('All py_compile checks PASSED')
else:
    print('Some files FAILED py_compile')
    sys.exit(1)
