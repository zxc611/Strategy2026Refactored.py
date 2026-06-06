"""P2-S2b: 从data/子模块源码提取所有下划线前缀的模块级名称"""
import os
import re

root = os.path.dirname(os.path.abspath(__file__))

subsystem_dirs = ['order', 'signal', 'lifecycle', 'config', 'risk', 'position',
                  'data', 'strategy', 'governance', 'infra', 'execution']

fixed = 0
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

        with open(src_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        all_underscore = set()
        for m in re.finditer(r'^(_[A-Za-z][A-Za-z0-9_]*)\s*[:=]', content, re.MULTILINE):
            all_underscore.add(m.group(1))
        for m in re.finditer(r'^def\s+(_[A-Za-z][A-Za-z0-9_]*)\s*\(', content, re.MULTILINE):
            all_underscore.add(m.group(1))
        for m in re.finditer(r'^class\s+(_[A-Za-z][A-Za-z0-9_]*)', content, re.MULTILINE):
            all_underscore.add(m.group(1))

        if not all_underscore:
            continue

        with open(reexport_path, 'r', encoding='utf-8') as f:
            reexport = f.read()

        already_imported = set()
        for m in re.finditer(r'import\s+([^\n]+)', reexport):
            for name in m.group(1).split(','):
                already_imported.add(name.strip())

        new_names = sorted(all_underscore - already_imported)
        if not new_names:
            continue

        import_line = f'from ali2026v3_trading.{subdir}.{mod_name} import {", ".join(new_names)}\n'
        with open(reexport_path, 'a', encoding='utf-8') as f:
            f.write(import_line)
        fixed += 1

print(f'Fixed {fixed} more re-export modules')