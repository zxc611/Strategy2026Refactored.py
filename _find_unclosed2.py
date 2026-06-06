filepath = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\greeks_calculator.py'

with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
    content = f.read()

lines = content.split('\n')

unclosed = []
in_triple = False
triple_char = None
for i, line in enumerate(lines, 1):
    count3d = line.count('"""')
    count3s = line.count("'''")
    if not in_triple:
        if count3d % 2 == 1:
            in_triple = True
            triple_char = '"""'
            unclosed.append(i)
        elif count3s % 2 == 1:
            in_triple = True
            triple_char = "'''"
            unclosed.append(i)
    else:
        if triple_char == '"""' and count3d % 2 == 1:
            in_triple = False
            unclosed.pop()
        elif triple_char == "'''" and count3s % 2 == 1:
            in_triple = False
            unclosed.pop()

print(f'Unclosed triple-quote starting lines: {unclosed}')
for line_num in unclosed:
    print(f'  Line {line_num}: {lines[line_num-1][:80]}')

if not unclosed:
    print('No unclosed triple-quotes found')
