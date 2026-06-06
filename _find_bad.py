import os
filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'greeks_calculator.py')
with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
    lines = f.readlines()

bad_lines = []
for i, line in enumerate(lines, 1):
    if '\ufffd' in line:
        bad_lines.append((i, line.rstrip()))

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '_bad_lines.txt'), 'w') as out:
    out.write('Total bad lines: %d\n' % len(bad_lines))
    for num, text in bad_lines:
        out.write('Line %d: %s\n' % (num, text[:120]))
