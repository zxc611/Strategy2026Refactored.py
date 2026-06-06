import sys
filepath = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\greeks_calculator.py'
with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
    content = f.read()
lines = content.split('\n')
results = []
for end_line in [74, 75, 76, 80, 81, 82, 83, 84, 85, 86]:
    snippet = '\n'.join(lines[:end_line])
    try:
        compile(snippet, '<string>', 'exec')
        status = 'OK'
    except SyntaxError as e:
        status = 'FAIL L%d: %s' % (e.lineno, e.msg)
    results.append('Lines 1-%d: %s' % (end_line, status))
sys.stdout.write('\n'.join(results) + '\n')
sys.stdout.flush()
