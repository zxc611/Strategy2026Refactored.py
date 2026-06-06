import re, os

DIR = r'c:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading'

lifecycle_defined = set()
with open(os.path.join(DIR, 'strategy_lifecycle_mixin.py'), 'r', encoding='utf-8') as f:
    content = f.read()
for m in re.finditer(r'self\.(_\w+)\s*=', content):
    lifecycle_defined.add(m.group(1))

tick_defined = set()
with open(os.path.join(DIR, 'strategy_tick_handler.py'), 'r', encoding='utf-8') as f:
    content = f.read()
for m in re.finditer(r'self\.(_\w+)\s*=', content):
    tick_defined.add(m.group(1))

results = []
total_harmful = 0
for fname, own_attrs in [
    ('strategy_lifecycle_mixin.py', lifecycle_defined),
    ('strategy_tick_handler.py', tick_defined),
]:
    with open(os.path.join(DIR, fname), 'r', encoding='utf-8') as f:
        lines = f.readlines()
    harmful = 0
    own = 0
    harmful_details = {}
    for i, line in enumerate(lines, 1):
        for m in re.finditer(r'self\.(_\w+)', line):
            attr = m.group(1)
            if attr in own_attrs:
                own += 1
            else:
                harmful += 1
                harmful_details.setdefault(attr, []).append(i)
    total_harmful += harmful
    results.append(f'{fname}: harmful={harmful}, own={own}')
    for attr, line_nums in sorted(harmful_details.items(), key=lambda x: -len(x[1])):
        results.append(f'  {attr}: {len(line_nums)} occurrences')

results.append(f'\nTOTAL harmful: {total_harmful}')

with open(os.path.join(DIR, '_ref_count_result.txt'), 'w') as out:
    out.write('\n'.join(results))
