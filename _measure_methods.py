import re, sys
path = r"C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\参数池\backtest_runner_base.py"
lines = open(path, encoding='utf-8').readlines()
methods = []
for i, line in enumerate(lines, 1):
    m = re.match(r'^def (\w+)', line)
    if m:
        methods.append((m.group(1), i))
print(f"Total methods: {len(methods)}")
over80 = 0
for j, (name, start) in enumerate(methods):
    end = methods[j+1][1]-1 if j+1 < len(methods) else len(lines)
    length = end - start + 1
    if length >= 80:
        over80 += 1
        tag = 'OK' if length <= 80 else ('WARN' if length <= 120 else 'OVER')
        print(f"  {name}: {length} lines (L{start}-L{end}) [{tag}]")
print(f"\nMethods >= 80 lines: {over80}/{len(methods)}")
print(f"File total: {len(lines)} lines")