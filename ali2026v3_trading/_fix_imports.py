import sys

f = r'c:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\param_pool\task_scheduler.py'
c = open(f, 'r', encoding='utf-8').read()
old = 'ali2026v3_trading.参数池.'
new = 'ali2026v3_trading.param_pool.'
count = c.count(old)
c = c.replace(old, new)
open(f, 'w', encoding='utf-8').write(c)
print(f'Replaced {count} occurrences')
