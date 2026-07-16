#!/usr/bin/env python3
"""Count .py files by directory, excluding tests/tools/archive."""
import os

ROOT = r'c:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\demo'
EXCLUDE_TOP = {'archive', '__pycache__', 'tools', 'tests', '_verify_logs', '_wal_orders', '.git', '.ci', 'docs', 'benchmarks'}

details = {}
for dirpath, dirnames, filenames in os.walk(ROOT):
    # Skip excluded top-level dirs
    rel = os.path.relpath(dirpath, ROOT)
    top = rel.split(os.sep)[0] if rel != '.' else ''
    if top in EXCLUDE_TOP:
        dirnames.clear()
        continue
    # Skip __pycache__
    dirnames[:] = [d for d in dirnames if d != '__pycache__']
    py_count = len([f for f in filenames if f.endswith('.py')])
    if py_count > 0:
        details[rel] = py_count

total = sum(details.values())
print(f"Total .py files (excl tests/tools/archive): {total}")
print()
for k, v in sorted(details.items(), key=lambda x: -x[1]):
    print(f"  {k}: {v}")
