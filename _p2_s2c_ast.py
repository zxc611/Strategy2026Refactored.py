"""P2-S2c: 全面提取模块级下划线名称并添加到重导出"""
import os
import re
import ast

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

        underscore_names = set()
        try:
            tree = ast.parse(content)
            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name) and target.id.startswith('_'):
                            underscore_names.add(target.id)
                elif isinstance(node, ast.AnnAssign):
                    if isinstance(node.target, ast.Name) and node.target.id.startswith('_'):
                        underscore_names.add(node.target.id)
                elif isinstance(node, ast.FunctionDef) and node.name.startswith('_'):
                    underscore_names.add(node.name)
                elif isinstance(node, ast.ClassDef) and node.name.startswith('_'):
                    underscore_names.add(node.name)
        except SyntaxError:
            for m in re.finditer(r'^(\s*)(_[A-Za-z][A-Za-z0-9_]*)\s*[:=]', content, re.MULTILINE):
                if m.group(1) == '' or m.group(1).strip() == '':
                    underscore_names.add(m.group(2))

        if not underscore_names:
            continue

        with open(reexport_path, 'r', encoding='utf-8') as f:
            reexport = f.read()

        already = set()
        for m in re.finditer(r'import\s+([^\n]+)', reexport):
            for name in m.group(1).split(','):
                already.add(name.strip())

        new_names = sorted(underscore_names - already)
        if not new_names:
            continue

        import_line = f'from ali2026v3_trading.{subdir}.{mod_name} import {", ".join(new_names)}\n'
        with open(reexport_path, 'a', encoding='utf-8') as f:
            f.write(import_line)
        fixed += 1

print(f'Fixed {fixed} re-export modules with AST-extracted underscore names')