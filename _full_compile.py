import py_compile, os

errors = []
base = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading'
skip_dirs = {'__pycache__', '.git', 'backup', 'tests'}
skip_prefixes = ('_',)

for root, dirs, files in os.walk(base):
    dirs[:] = [d for d in dirs if d not in skip_dirs]
    for f in files:
        if f.endswith('.py') and not f.startswith(skip_prefixes):
            path = os.path.join(root, f)
            try:
                py_compile.compile(path, doraise=True)
            except Exception as e:
                errors.append((path, str(e)[:150]))

outpath = os.path.join(base, '_compile_results.txt')
with open(outpath, 'w') as out:
    out.write('Total errors: %d\n' % len(errors))
    for path, err in errors:
        out.write('%s: %s\n' % (path, err))

print('Total errors: %d' % len(errors))
for path, err in errors[:10]:
    print('  %s: %s' % (os.path.basename(path), err[:80]))
