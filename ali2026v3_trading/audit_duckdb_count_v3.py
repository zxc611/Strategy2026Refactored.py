import os, re, glob, ast

base = r'c:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading'
all_py = glob.glob(os.path.join(base, '**', '*.py'), recursive=True)

import_pattern = re.compile(r'^\s*(?:import\s+duckdb|from\s+duckdb)', re.MULTILINE)

def is_duckdb_api_call(node):
    """判断是否是实际的duckdb API调用（排除类型注解）"""
    if isinstance(node, ast.Call):
        # duckdb(...) 或 duckdb.xxx(...)
        if isinstance(node.func, ast.Name) and node.func.id.lower() == 'duckdb':
            return True
        if isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name) and node.func.value.id.lower() == 'duckdb':
                return True
    return False

import_lines = set()
api_call_lines = set()

for pyfile in all_py:
    try:
        with open(pyfile, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')
    except:
        continue
    
    basename = os.path.basename(pyfile)
    
    # 口径1: import语句（排除db_adapter自身和测试文件）
    for m in import_pattern.finditer(content):
        line_num = content[:m.start()].count('\n') + 1
        # 排除db_adapter自身（它是封装层）和测试文件
        if basename != 'db_adapter.py' and not basename.startswith('test_'):
            import_lines.add(f'{basename}:{line_num}')
    
    # 口径2: API调用（AST分析，排除类型注解）
    try:
        tree = ast.parse(content)
    except:
        continue
    
    for node in ast.walk(tree):
        if is_duckdb_api_call(node):
            line_num = getattr(node, 'lineno', 0)
            if line_num > 0:
                # 排除db_adapter自身和测试文件
                if basename != 'db_adapter.py' and not basename.startswith('test_'):
                    api_call_lines.add(f'{basename}:{line_num}')

import_count = len(import_lines)
api_call_count = len(api_call_lines)

print(f'=== 业务代码DuckDB引用统计 (排除db_adapter和测试文件) ===')
print(f'Import语句数: {import_count}')
print(f'API调用行数: {api_call_count}')
print(f'总计: {import_count + api_call_count}')
print()
print('=== Import清单 ===')
for line in sorted(import_lines):
    print(line)
print()
print('=== API调用清单 ===')
for line in sorted(api_call_lines):
    print(line)
