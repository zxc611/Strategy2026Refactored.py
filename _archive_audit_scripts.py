"""P0-S1: 归档审计脚本到 archive/audit_rounds/"""
import os
import shutil
import re

root = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading'
dest = os.path.join(root, 'archive', 'audit_rounds')

patterns = [
    r'^audit_',
    r'^_audit_',
    r'^_fix_',
    r'^_verify_',
    r'^r10_',
    r'^verify_',
    r'^fix_',
]

moved = []
for f in os.listdir(root):
    if not f.endswith('.py'):
        continue
    for pat in patterns:
        if re.match(pat, f):
            src = os.path.join(root, f)
            dst = os.path.join(dest, f)
            shutil.move(src, dst)
            moved.append(f)
            break

print(f"Moved {len(moved)} audit scripts to archive/audit_rounds/")
for f in sorted(moved):
    print(f"  {f}")

remaining = []
for f in os.listdir(root):
    if not f.endswith('.py'):
        continue
    for pat in patterns:
        if re.match(pat, f):
            remaining.append(f)
            break
print(f"\nRemaining audit scripts in root: {len(remaining)}")