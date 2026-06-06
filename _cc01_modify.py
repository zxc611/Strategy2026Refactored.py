#!/usr/bin/env python3
import re
import os

BASE = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading'

FUNC_NAMES = [
    'run_backtest_box_extreme',
    'run_backtest_box_spring',
    'run_backtest_hft',
    'run_backtest_arbitrage',
    'run_backtest_market_making',
    'run_backtest_hft_with_disturbance',
    'run_backtest_multiscale',
    'run_backtest_hft_tick_fidelity',
    'run_backtest_with_cycle_resonance',
]

DIRS = [
    ('param_pool', 'ali2026v3_trading.param_pool'),
    ('param_pool', 'ali2026v3_trading.param_pool'),
]


def find_top_level_boundaries(lines):
    boundaries = []
    for i, line in enumerate(lines):
        stripped = line.rstrip()
        if re.match(r'^(def |class |[A-Z_][A-Z_0-9]*\s*=)', stripped):
            boundaries.append(i)
        elif re.match(r'^@', stripped):
            boundaries.append(i)
    boundaries.append(len(lines))
    return boundaries


def modify_task_scheduler(dir_name, import_path):
    filepath = os.path.join(BASE, dir_name, 'task_scheduler.py')
    with open(filepath, encoding='utf-8') as f:
        lines = f.readlines()

    boundaries = find_top_level_boundaries(lines)

    func_ranges = []
    for fn_name in FUNC_NAMES:
        start = None
        for i, line in enumerate(lines):
            if line.startswith(f'def {fn_name}('):
                start = i
                break
        if start is None:
            print(f'  WARNING: {fn_name} not found in {dir_name}/task_scheduler.py')
            continue

        end = None
        for b in boundaries:
            if b > start:
                end = b
                break
        if end is None:
            end = len(lines)

        while end > start and lines[end - 1].strip() == '':
            end -= 1

        func_ranges.append((fn_name, start, end))

    func_ranges.sort(key=lambda x: x[1])

    import_line = f'from {import_path}.backtest_runner import (\n'
    for fn_name in FUNC_NAMES:
        import_line += f'    {fn_name},\n'
    import_line += ')\n'

    insert_pos = None
    for i, line in enumerate(lines):
        if line.strip().startswith('RISK_FREE_RATE'):
            insert_pos = i + 1
            break

    if insert_pos is None:
        print(f'  ERROR: Could not find RISK_FREE_RATE in {dir_name}/task_scheduler.py')
        return

    lines_to_delete = set()
    for fn_name, start, end in func_ranges:
        for i in range(start, end):
            lines_to_delete.add(i)

    new_lines = []
    for i, line in enumerate(lines):
        if i == insert_pos:
            new_lines.append('\n')
            new_lines.append(import_line)
        if i not in lines_to_delete:
            new_lines.append(line)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)

    total_lines = len(new_lines)
    deleted = len(lines_to_delete)
    print(f'{dir_name}/task_scheduler.py: {len(lines)} -> {total_lines} lines (deleted {deleted} lines)')


for dir_name, import_path in DIRS:
    print(f'Processing {dir_name}/task_scheduler.py...')
    modify_task_scheduler(dir_name, import_path)
