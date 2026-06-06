filepath = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\greeks_calculator.py'

with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
    content = f.read()

lines = content.split('\n')

for end_line in [74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85]:
    snippet = '\n'.join(lines[:end_line])
    try:
        compile(snippet, '<string>', 'exec')
        status = 'OK'
    except SyntaxError as e:
        status = f'FAIL L{e.lineno}: {e.msg}'
    print(f'Lines 1-{end_line}: {status}')
