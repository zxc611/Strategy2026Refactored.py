import os
base = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\demo'
files = ['infra/_helpers.py','infra/_storage_checks_mixin.py','infra/_storage_catalog_mixin.py','infra/_storage_maintenance.py','infra/_ops_framework.py','infra/_disk_monitor.py','infra/_backup_restore.py','infra/_ops_knowledge_metrics.py','infra/_ops_automation.py','infra/maintenance_service.py']
for f in files:
    path = os.path.join(base, f)
    with open(path, 'r', encoding='utf-8') as fh:
        count = len(fh.readlines())
    with open(os.path.join(base, 'tools', '_linecount.txt'), 'a', encoding='utf-8') as out:
        out.write(f'{f}: {count}\n')
