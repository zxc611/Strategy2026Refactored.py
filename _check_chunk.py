import os
filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'greeks_calculator.py')
with open(filepath, 'rb') as f:
    d = f.read()
idx = d.find(b'_bs_price')
chunk = d[idx:idx+500]
triple_count = chunk.count(b'"""')
outpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_chunk_info.txt')
with open(outpath, 'w') as out:
    out.write('Triple quotes in chunk: %d\n' % triple_count)
    out.write('Last 100 bytes: %s\n' % repr(chunk[-100:]))
    out.write('Full chunk: %s\n' % repr(chunk[:400]))
print('Done')
