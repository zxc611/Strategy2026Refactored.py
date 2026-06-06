"""P2-S0: import快照导出"""
import os
import re

root = os.path.dirname(os.path.abspath(__file__))
imports = []
for dirpath, dirnames, filenames in os.walk(root):
    rp = os.path.relpath(dirpath, root)
    if '_backup_' in rp or 'archive' in rp or '__pycache__' in rp or '.pytest_cache' in rp:
        continue
    for fn in filenames:
        if not fn.endswith('.py'):
            continue
        fp = os.path.join(dirpath, fn)
        try:
            with open(fp, 'r', encoding='utf-8', errors='ignore') as f:
                for i, line in enumerate(f, 1):
                    m = re.match(r'\s*(?:from|import)\s+ali2026v3_trading', line)
                    if m:
                        imports.append(f'{rp}/{fn}:{i}: {line.strip()}')
        except Exception:
            pass

out = os.path.join(root, 'archive', 'import_snapshot', 'all_imports.txt')
os.makedirs(os.path.dirname(out), exist_ok=True)
with open(out, 'w', encoding='utf-8') as f:
    f.write('\n'.join(imports))
print(f'Import snapshot: {len(imports)} import lines saved')