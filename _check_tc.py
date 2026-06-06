import os
filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'greeks_calculator.py')
with open(filepath, 'rb') as f:
    data = f.read()
idx = data.find(b'TradingCalendar')
chunk = data[idx:idx+800]
outpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_tc_chunk.txt')
with open(outpath, 'wb') as out:
    out.write(b'TradingCalendar chunk:\n')
    out.write(repr(chunk).encode('utf-8'))
print('Done')
