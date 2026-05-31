
import os

base = 'c:/Users/xu/AppData/Roaming/InfiniTrader_SimulationX64/pyStrategy/demo/ali2026v3_trading/backup_20260524'
total = 0
for f in sorted(os.listdir(base)):
    if f.endswith('.py'):
        path = os.path.join(base, f)
        with open(path, 'r', encoding='utf-8', errors='ignore') as fh:
            lines = sum(1 for _ in fh)
        total += lines
        print(f'{f}: {lines} lines')
print(f'Total: {total} lines')
