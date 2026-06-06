import re, os

base = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading'
files = [
    ('config_params.py', base),
    ('strategy_core_service.py', base),
    ('order_service.py', base),
    ('signal_service.py', base),
    ('backtest_runner_base.py', os.path.join(base, '参数池')),
]

for f, dirpath in files:
    fp = os.path.join(dirpath, f)
    with open(fp, 'r', encoding='utf-8') as fh:
        lines = fh.readlines()
    total = len(lines)
    methods = []
    for i, line in enumerate(lines, 1):
        m = re.match(r'^\s*def (\w+)\(', line)
        if m:
            methods.append((i, m.group(1)))
    method_sizes = []
    for idx in range(len(methods)):
        start = methods[idx][0]
        end = methods[idx + 1][0] - 1 if idx + 1 < len(methods) else total
        size = end - start + 1
        method_sizes.append((methods[idx][1], start, size))
    method_sizes.sort(key=lambda x: -x[2])
    print(f'\n=== {f} ({total} lines) ===')
    for name, start, size in method_sizes[:12]:
        print(f'  {name} L{start} {size} lines')