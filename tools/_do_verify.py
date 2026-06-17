import py_compile, os
base = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading'
out = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\tools\_syntax_check_results.txt'
files = ['infra/_helpers.py','infra/_storage_checks_mixin.py','infra/_storage_catalog_mixin.py','infra/_storage_maintenance.py','infra/_ops_framework.py','infra/_disk_monitor.py','infra/_backup_restore.py','infra/_ops_knowledge_metrics.py','infra/_ops_automation.py','infra/maintenance_service.py']
results = []
for f in files:
    path = os.path.join(base, f)
    try:
        py_compile.compile(path, doraise=True)
        results.append(f'{f}: OK')
    except py_compile.PyCompileError as e:
        results.append(f'{f}: FAIL - {e}')
with open(out, 'w', encoding='utf-8') as f:
    f.write('\n'.join(results))
