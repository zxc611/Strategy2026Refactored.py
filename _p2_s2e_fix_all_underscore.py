"""P2-S2e-v3: Fix ALL missing MODULE-LEVEL underscore symbols in re-export modules.
Handles multi-source re-export modules (multiple import * lines)."""
import ast, os, re, glob

pkg = os.path.dirname(os.path.abspath(__file__))

def get_module_level_underscore_names(filepath):
    try:
        tree = ast.parse(open(filepath, 'r', encoding='utf-8').read())
    except:
        return set()
    names = set()
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id.startswith('_') and not t.id.startswith('__'):
                    names.add(t.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id.startswith('_') and not node.target.id.startswith('__'):
                names.add(node.target.id)
        elif isinstance(node, ast.FunctionDef):
            if node.name.startswith('_') and not node.name.startswith('__'):
                names.add(node.name)
        elif isinstance(node, ast.ClassDef):
            if node.name.startswith('_') and not node.name.startswith('__'):
                names.add(node.name)
        elif isinstance(node, ast.ImportFrom) and node.names:
            for alias in node.names:
                if alias.name.startswith('_') and not alias.name.startswith('__'):
                    names.add(alias.name)
    return names

reexport_files = []
for f in glob.glob(os.path.join(pkg, '*.py')):
    content = open(f, 'r', encoding='utf-8').read()
    if 'import *' in content and '重导出' in content:
        reexport_files.append(f)

print(f'Found {len(reexport_files)} re-export modules')

total_added = 0
for f in reexport_files:
    content = open(f, 'r', encoding='utf-8').read()
    lines = content.split('\n')
    
    # Find all 'from ali2026v3_trading.X.Y import *' lines
    source_modules = []
    for i, line in enumerate(lines):
        m = re.match(r'from\s+(ali2026v3_trading\.\S+)\s+import\s+\*', line)
        if m:
            source_modules.append((i, m.group(1)))
    
    if not source_modules:
        continue
    
    # For each source module, find what underscore names it exports at module level
    new_lines = list(lines)
    offset = 0
    for idx, source_mod in source_modules:
        rel_path = source_mod.replace('.', os.sep) + '.py'
        source_file = os.path.normpath(os.path.join(pkg, '..', rel_path))
        if not os.path.exists(source_file):
            continue
        
        source_names = get_module_level_underscore_names(source_file)
        if not source_names:
            continue
        
        # Check if there's already an explicit import line for this source module
        insert_pos = idx + 1 + offset
        existing_import_idx = None
        for j, line in enumerate(new_lines):
            if line.startswith(f'from {source_mod} import _'):
                existing_import_idx = j
                break
        
        import_line = f'from {source_mod} import {", ".join(sorted(source_names))}'
        
        if existing_import_idx is not None:
            old_line = new_lines[existing_import_idx]
            if old_line != import_line:
                new_lines[existing_import_idx] = import_line
                total_added += len(source_names)
                print(f'  Updated {os.path.basename(f)} <- {source_mod}: {len(source_names)} symbols')
        else:
            new_lines.insert(insert_pos, import_line)
            offset += 1
            total_added += len(source_names)
            print(f'  Added {os.path.basename(f)} <- {source_mod}: {len(source_names)} symbols')
    
    content = '\n'.join(new_lines)
    with open(f, 'w', encoding='utf-8') as fout:
        fout.write(content)

print(f'\nTotal: {total_added} underscore symbol imports processed')
