"""P2-S2f: Find public symbols (non-underscore) that are used externally but not exported via import *.
These need explicit import in re-export modules."""
import ast, os, re, glob, subprocess

pkg = os.path.dirname(os.path.abspath(__file__))

# Step 1: For each re-export module, find what the source module's __all__ contains
# and what public symbols are defined but not in __all__
reexport_files = []
for f in glob.glob(os.path.join(pkg, '*.py')):
    content = open(f, 'r', encoding='utf-8').read()
    if 'import *' in content and '重导出' in content:
        reexport_files.append(f)

print(f'Found {len(reexport_files)} re-export modules')

total_added = 0
for f in reexport_files:
    content = open(f, 'r', encoding='utf-8').read()
    # Find all 'from ali2026v3_trading.X.Y import *' lines
    source_modules = []
    for m in re.finditer(r'from\s+(ali2026v3_trading\.\S+)\s+import\s+\*', content):
        source_modules.append(m.group(1))
    
    if not source_modules:
        continue
    
    # For each source module, find public symbols NOT in __all__
    missing_public = []
    for source_mod in source_modules:
        rel_path = source_mod.replace('.', os.sep) + '.py'
        source_file = os.path.normpath(os.path.join(pkg, '..', rel_path))
        if not os.path.exists(source_file):
            continue
        
        try:
            tree = ast.parse(open(source_file, 'r', encoding='utf-8').read())
        except:
            continue
        
        # Get __all__
        all_names = None
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name) and t.id == '__all__':
                        if isinstance(node.value, ast.List):
                            all_names = set(e.value for e in node.value.elts if isinstance(e, ast.Constant))
        
        if all_names is None:
            continue  # No __all__ means import * exports everything
        
        # Get all public module-level names
        public_names = set()
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name) and not t.id.startswith('_'):
                        public_names.add(t.id)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                if not node.target.id.startswith('_'):
                    public_names.add(node.target.id)
            elif isinstance(node, ast.FunctionDef) and not node.name.startswith('_'):
                public_names.add(node.name)
            elif isinstance(node, ast.ClassDef) and not node.name.startswith('_'):
                public_names.add(node.name)
            elif isinstance(node, ast.ImportFrom) and node.names:
                for alias in node.names:
                    if not alias.name.startswith('_'):
                        public_names.add(alias.name if alias.asname is None else alias.asname)
        
        # Public names not in __all__ - these won't be exported by import *
        not_exported = sorted(public_names - all_names)
        if not_exported:
            missing_public.extend([(source_mod, name) for name in not_exported])
    
    if not missing_public:
        continue
    
    # Add explicit imports for missing public symbols
    lines = content.split('\n')
    for source_mod, name in missing_public:
        # Check if already imported
        already = any(f'from {source_mod} import' in line and name in line for line in lines)
        if already:
            continue
        
        # Find the import * line for this source module
        insert_idx = None
        for i, line in enumerate(lines):
            if f'from {source_mod} import *' in line:
                insert_idx = i + 1
                break
        
        if insert_idx is not None:
            # Find existing explicit import line for this source module
            existing_idx = None
            for i, line in enumerate(lines):
                if line.startswith(f'from {source_mod} import ') and '*' not in line:
                    existing_idx = i
                    break
            
            if existing_idx is not None:
                # Append to existing import line
                old_line = lines[existing_idx]
                lines[existing_idx] = old_line.rstrip() + ', ' + name
            else:
                lines.insert(insert_idx, f'from {source_mod} import {name}')
    
    new_content = '\n'.join(lines)
    if new_content != content:
        with open(f, 'w', encoding='utf-8') as fout:
            fout.write(new_content)
        added = len(missing_public)
        total_added += added
        names = [n for _, n in missing_public]
        print(f'  Fixed {os.path.basename(f)}: +{added} public symbols: {names[:10]}...' if len(names)>10 else f'  Fixed {os.path.basename(f)}: +{added} public symbols: {names}')

print(f'\nTotal: {total_added} public symbol imports added')