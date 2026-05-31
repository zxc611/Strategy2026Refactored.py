import os, re, glob, ast

base = r'c:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading'
all_py = glob.glob(os.path.join(base, '**', '*.py'), recursive=True)

import_pattern = re.compile(r'^\s*(?:import\s+duckdb|from\s+duckdb)', re.MULTILINE)

def is_duckdb_api_call(node):
    if isinstance(node, ast.Attribute):
        if isinstance(node.value, ast.Name) and node.value.id.lower() == 'duckdb':
            return True
    elif isinstance(node, ast.Call):
        if isinstance(node.func, ast.Name) and node.func.id.lower() == 'duckdb':
            return True
        if isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name) and node.func.value.id.lower() == 'duckdb':
                return True
    elif isinstance(node, ast.Subscript):
        if isinstance(node.value, ast.Name) and node.value.id.lower() == 'duckdb':
            return True
    return False

import_count = 0
api_call_count = 0
details = []

for pyfile in all_py:
    try:
        with open(pyfile, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')
    except:
        continue
    
    basename = os.path.basename(pyfile)
    
    file_imports = import_pattern.findall(content)
    import_count += len(file_imports)
    for m in import_pattern.finditer(content):
        line_num = content[:m.start()].count('\n') + 1
        details.append(f'{basename}:{line_num} [IMPORT] {lines[line_num-1].strip()[:80]}')
    
    try:
        tree = ast.parse(content)
    except:
        continue
    
    for node in ast.walk(tree):
        if is_duckdb_api_call(node):
            api_call_count += 1
            line_num = getattr(node, 'lineno', 0)
            if line_num > 0 and line_num <= len(lines):
                details.append(f'{basename}:{line_num} [API] {lines[line_num-1].strip()[:80]}')

print(f'=== AST精确统计结果 ===')
print(f'Import语句数: {import_count}')
print(f'API调用数 (AST duckdb.xxx/duckdb()): {api_call_count}')
print(f'总计: {import_count + api_call_count}')
print()
print('=== 详细清单 ===')
for d in details:
    print(d)
