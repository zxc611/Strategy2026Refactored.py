import os
filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'greeks_calculator.py')
with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
    content = f.read()

lines = content.split('\n')

for end in [36, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85]:
    snippet = '\n'.join(lines[:end])
    try:
        compile(snippet, '<string>', 'exec')
        status = 'OK'
    except SyntaxError as e:
        status = 'ERR L%s: %s' % (e.lineno, e.msg)
    print('1-%d: %s' % (end, status))
