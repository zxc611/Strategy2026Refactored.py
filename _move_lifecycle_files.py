"""P1-R2: lifecycle/子系统目录重组PoC"""
import os
import shutil

root = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading'
lifecycle_dir = os.path.join(root, 'lifecycle')

moved = []
for f in os.listdir(root):
    if f.startswith('lifecycle_') and f.endswith('.py'):
        src = os.path.join(root, f)
        dst = os.path.join(lifecycle_dir, f)
        shutil.move(src, dst)
        moved.append(f)

print(f"Moved {len(moved)} lifecycle files to lifecycle/:")
for f in sorted(moved):
    print(f"  {f}")

lifecycle_modules = [f[:-3] for f in sorted(moved)]

init_lines = ['"""lifecycle/子系统 — 重导出模块(P1-R2)"""', '']
for mod in lifecycle_modules:
    init_lines.append(f'from ali2026v3_trading.lifecycle.{mod} import *')
init_path = os.path.join(lifecycle_dir, '__init__.py')
with open(init_path, 'w', encoding='utf-8') as fp:
    fp.write('\n'.join(init_lines) + '\n')
print(f"\nCreated lifecycle/__init__.py with {len(lifecycle_modules)} re-exports")

for mod in lifecycle_modules:
    filepath = os.path.join(root, f'{mod}.py')
    content = f'"""{mod}.py — 重导出模块(P1-R2目录重组后)"""\nfrom ali2026v3_trading.lifecycle.{mod} import *\n'
    with open(filepath, 'w', encoding='utf-8') as fp:
        fp.write(content)

print(f"Created {len(lifecycle_modules)} re-export modules in root")