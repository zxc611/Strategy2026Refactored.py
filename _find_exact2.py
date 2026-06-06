import os

filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'greeks_calculator.py')

with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
    content = f.read()

lines = content.split('\n')

for end in range(500, 600):
    snippet = '\n'.join(lines[:end])
    try:
        compile(snippet, '<string>', 'exec')
    except SyntaxError as e:
        print('1-%d: ERR L%s: %s' % (end, e.lineno, e.msg))
        if end > 505:
            break
