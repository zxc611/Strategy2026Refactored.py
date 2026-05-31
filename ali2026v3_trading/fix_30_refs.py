import re

filepath = r'c:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\重构方案_顶级基金架构视角_独立审计_v1.md'

with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 替换所有'30处'相关引用
replacements = [
    ('| import duckdb语句 | 32处 | **30处** | -2处 | 四审高估 |',
     '| import duckdb语句 | 32处 | **18处** | -14处 | 六审修正：业务代码import（排除db_adapter和测试） |'),
    ('2. **30处import duckdb**散布在18个文件中',
     '2. **46处DuckDB引用（18 import + 28 API调用）**散布在业务代码中'),
    ('- **真正问题**：30处import duckdb散布在18个文件中',
     '- **真正问题**：46处DuckDB引用（18 import + 28 API调用）散布在业务代码中'),
    ('db_adapter"空壳"→"薄封装"、30处import→精确计数、Mixin 5→4',
     'db_adapter"空壳"→"薄封装"、46处DuckDB引用→精确计数、Mixin 5→4'),
    ('扫描30处import duckdb时',
     '扫描46处DuckDB引用（18 import + 28 API调用）时'),
    ('消除30处DuckDB直引',
     '消除46处DuckDB直引（18 import + 28 API）'),
    ('│  │ 30处迁移→此处   │',
     '│  │ 46处迁移→此处   │'),
    ('| P0.1 | DuckDB引用审计 | 30处import + 调用点',
     '| P0.1 | DuckDB引用审计 | 46处DuckDB引用（18 import + 28 API）'),
    ('逐文件迁移+每步回归测试，30处分批进行',
     '逐文件迁移+每步回归测试，46处分批进行'),
    ('30处import duckdb迁移到data_access.py',
     '46处DuckDB引用（18 import + 28 API调用）迁移到data_access.py'),
    ('├─ 30处→data_access   ├─',
     '├─ 46处→data_access   ├─'),
    ('扫描全部30处import duckdb + 关联调用点',
     '扫描全部46处DuckDB引用（18 import + 28 API调用）+ 关联调用点'),
]

for old, new in replacements:
    content = content.replace(old, new)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print('替换完成')

# 验证
remaining = [m.start() for m in re.finditer(r'30处', content)]
print(f'剩余30处出现次数: {len(remaining)}')
for pos in remaining[:10]:
    start = max(0, pos-30)
    end = min(len(content), pos+30)
    print(f'  ...{content[start:end]}...')
