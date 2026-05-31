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
api_call_lines = set()  # 用set去重：同一行多个AST节点只计一次
import_lines = set()

for pyfile in all_py:
    try:
        with open(pyfile, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')
    except:
        continue
    
    basename = os.path.basename(pyfile)
    
    # 口径1: import语句
    for m in import_pattern.finditer(content):
        line_num = content[:m.start()].count('\n') + 1
        import_lines.add(f'{basename}:{line_num}')
    
    # 口径2: API调用 (AST分析)
    try:
        tree = ast.parse(content)
    except:
        continue
    
    for node in ast.walk(tree):
        if is_duckdb_api_call(node):
            line_num = getattr(node, 'lineno', 0)
            if line_num > 0:
                api_call_lines.add(f'{basename}:{line_num}')

import_count = len(import_lines)
api_call_count = len(api_call_lines)

print(f'=== AST精确统计结果 (去重后) ===')
print(f'Import语句数 (去重): {import_count}')
print(f'API调用行数 (去重): {api_call_count}')
print(f'总计: {import_count + api_call_count}')
print()
print('=== Import清单 ===')
for line in sorted(import_lines):
    print(line)
print()
print('=== API调用清单 ===')
for line in sorted(api_call_lines):
    print(line)
