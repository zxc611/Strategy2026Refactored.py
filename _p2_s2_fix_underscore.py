"""P2-S2: 修复重导出模块中下划线前缀符号的导入"""
import os
import re
import importlib
import sys

root = os.path.dirname(os.path.abspath(__file__))

subsystem_dirs = ['order', 'signal', 'lifecycle', 'config', 'risk', 'position',
                  'data', 'strategy', 'governance', 'infra', 'execution']

fixed_count = 0
for subdir in subsystem_dirs:
    subdir_path = os.path.join(root, subdir)
    if not os.path.isdir(subdir_path):
        continue
    for fn in os.listdir(subdir_path):
        if not fn.endswith('.py') or fn == '__init__.py':
            continue
        mod_name = fn[:-3]
        src_path = os.path.join(subdir_path, fn)
        reexport_path = os.path.join(root, f'{mod_name}.py')

        if not os.path.exists(reexport_path):
            continue

        try:
            with open(src_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception:
            continue

        underscore_names = set()
        for m in re.finditer(r'^(_[A-Za-z_][A-Za-z0-9_]*)\s*[:=]', content, re.MULTILINE):
            underscore_names.add(m.group(1))
        for m in re.finditer(r'^def\s+(_[A-Za-z_][A-Za-z0-9_]*)\s*\(', content, re.MULTILINE):
            underscore_names.add(m.group(1))
        for m in re.finditer(r'^class\s+(_[A-Za-z_][A-Za-z0-9_]*)', content, re.MULTILINE):
            underscore_names.add(m.group(1))

        if not underscore_names:
            continue

        with open(reexport_path, 'r', encoding='utf-8') as f:
            reexport_content = f.read()

        existing_imports = set()
        for m in re.finditer(r'import\s+([\w_,\s]+)', reexport_content):
            for name in m.group(1).split(','):
                existing_imports.add(name.strip())

        new_names = sorted(underscore_names - existing_imports)
        if not new_names:
            continue

        try:
            import_line = f'from ali2026v3_trading.{subdir}.{mod_name} import {", ".join(new_names)}\n'
            with open(reexport_path, 'a', encoding='utf-8') as f:
                f.write(import_line)
            fixed_count += 1
        except Exception as e:
            print(f'  ERROR {mod_name}: {e}')

print(f'Fixed {fixed_count} re-export modules with underscore symbols')