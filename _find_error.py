filepath = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\greeks_calculator.py'

with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
    content = f.read()

lines = content.split('\n')

for end_line in range(80, 90):
    snippet = '\n'.join(lines[:end_line])
    try:
        compile(snippet, '<string>', 'exec')
        status = 'OK'
    except SyntaxError as e:
        status = f'FAIL L{e.lineno}: {e.msg}'
    print(f'Lines 1-{end_line}: {status}')
