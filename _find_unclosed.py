import tokenize

filepath = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\greeks_calculator.py'

with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
    readline = f.readline

try:
    tokens = list(tokenize.generate_tokens(readline))
    print(f'Total tokens: {len(tokens)}')
except tokenize.TokenError as e:
    print(f'TokenError: {e}')
    print(f'Error at line {e.args[1][0]}, col {e.args[1][1]}')

strings = []
for i, t in enumerate(tokens):
    if t.type == 3:  # STRING token
        s = t.string
        if s.startswith('"""') or s.startswith("'''"):
            if not s.endswith('"""') and not s.endswith("'''"):
                strings.append((t.start[0], repr(s)[:80]))
            elif s.count('"""') < 2 and s.count("'''") < 2:
                strings.append((t.start[0], repr(s)[:80]))

print(f'Potentially unclosed strings: {len(strings)}')
for line, s in strings[:20]:
    print(f'  Line {line}: {s}')
