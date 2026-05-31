
import os

path = os.path.join(
    'c:', os.sep, 'Users', 'xu', 'AppData', 'Roaming',
    'InfiniTrader_SimulationX64', 'pyStrategy', 'demo',
    'ali2026v3_trading', '参数池', 'task_scheduler.py'
)

with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

old_try = '    try:\n        # S1:'
idx = content.find(old_try)
if idx == -1:
    print("ERROR: old_try not found")
    exit(1)

helper_code = (
    '    # R10-P0-13: safe backtest wrapper\n'
    '    def _safe_backtest(name, fn, p, bd, train, st):\n'
    '        try:\n'
    '            return fn(p, bd, train, strategy_type=st)\n'
    '        except Exception as _e:\n'
    '            logger.error("[R10-P0-13] %s error: %s", name, _e)\n'
    '            return None\n'
    '\n'
)

content = content[:idx] + helper_code + content[idx:]

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

print(f"Replaced {count} run_backtest calls")

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print("R10-P0-13 fix done")
