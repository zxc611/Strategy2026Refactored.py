import os

path = os.path.join(
    'c:', os.sep, 'Users', 'xu', 'AppData', 'Roaming',
    'InfiniTrader_SimulationX64', 'pyStrategy', 'demo',
    'ali2026v3_trading', '参数池', 'task_scheduler.py'
)

with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace run_backtest calls with _safe_backtest wrapper
replacements = [
    ('run_backtest_hft(hft_params, bar_data, task["train"], strategy_type="hft")',
     '_safe_backtest("S1_main", run_backtest_hft, hft_params, bar_data, task["train"], "hft")'),
    ('run_backtest_hft(hft_shadow_a_params, bar_data, task["train"], strategy_type="s1_hft_shadow_a")',
     '_safe_backtest("S1_shA", run_backtest_hft, hft_shadow_a_params, bar_data, task["train"], "s1_hft_shadow_a")'),
    ('run_backtest_hft(hft_shadow_b_params, bar_data, task["train"], strategy_type="s1_hft_shadow_b")',
     '_safe_backtest("S1_shB", run_backtest_hft, hft_shadow_b_params, bar_data, task["train"], "s1_hft_shadow_b")'),
]

count = 0
for old, new in replacements:
    if old in content:
        content = content.replace(old, new, 1)
        count += 1
        print("Replaced: " + old[:50])
    else:
        print("NOT FOUND: " + old[:50])

# Add helper function before the try block using chr(10) for newline
nl = chr(10)
marker = "    try:" + nl + "        # S1:"
idx = content.find(marker)
if idx == -1:
    print("ERROR: marker not found")
    exit(1)

helper = (
    "    # R10-P0-13: safe backtest wrapper" + nl +
    "    def _safe_backtest(name, fn, p, bd, train, st):" + nl +
    "        try:" + nl +
    "            return fn(p, bd, train, strategy_type=st)" + nl +
    "        except Exception as _e:" + nl +
    "            logger.error('[R10-P0-13] %s error: %s', name, _e)" + nl +
    "            return None" + nl +
    nl
)

content = content[:idx] + helper + content[idx:]

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print("R10-P0-13 fix done, replaced " + str(count) + " calls")
