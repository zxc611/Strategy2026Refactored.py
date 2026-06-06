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


def extract_function_code(filepath, func_names):
    with open(filepath, encoding='utf-8') as f:
        lines = f.readlines()

    boundaries = find_top_level_boundaries(lines)

    results = {}
    for fn_name in func_names:
        start = None
        for i, line in enumerate(lines):
            if line.startswith(f'def {fn_name}('):
                start = i
                break
        if start is None:
            results[fn_name] = None
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

        func_code = ''.join(lines[start:end])
        results[fn_name] = func_code

    return results


def build_backtest_runner(func_code_dict, import_path):
    header_lines = [
        '#!/usr/bin/env python3',
        'from __future__ import annotations',
        '',
        'import logging',
        'from typing import Any, Dict, List, Optional, Tuple',
        '',
        'import numpy as np',
        'import pandas as pd',
        '',
        'from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_DAILY, ANNUALIZE_FACTOR_MINUTE',
        '',
        'logger = logging.getLogger(__name__)',
        '',
        '_ts_module = None',
        '',
        '',
        'def _ensure_ts():',
        '    global _ts_module',
        '    if _ts_module is None:',
        f'        from {import_path} import task_scheduler as _ts',
        '        _ts_module = _ts',
        '    return _ts_module',
        '',
        '',
        "def __getattr__(name):",
        '    ts = _ensure_ts()',
        '    try:',
        '        return getattr(ts, name)',
        '    except AttributeError:',
        '        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")',
        '',
        '',
        '',
    ]
    header = '\n'.join(header_lines)

    parts = [header]
    for fn_name in FUNC_NAMES:
        code = func_code_dict[fn_name]
        if code is None:
            raise ValueError(f'Function {fn_name} not found')
        parts.append(code)
        parts.append('\n\n\n')

    return ''.join(parts)


# Extract from 参数池
cn_path = os.path.join(BASE, 'param_pool', 'task_scheduler.py')
cn_funcs = extract_function_code(cn_path, FUNC_NAMES)
cn_runner = build_backtest_runner(cn_funcs, 'ali2026v3_trading.param_pool')
cn_out = os.path.join(BASE, 'param_pool', 'backtest_runner.py')
with open(cn_out, 'w', encoding='utf-8') as f:
    f.write(cn_runner)
cn_lines = cn_runner.count('\n') + 1
print(f'参数池/backtest_runner.py: {cn_lines} lines, {len(cn_runner)} bytes')

# Extract from param_pool
en_path = os.path.join(BASE, 'param_pool', 'task_scheduler.py')
en_funcs = extract_function_code(en_path, FUNC_NAMES)
en_runner = build_backtest_runner(en_funcs, 'ali2026v3_trading.param_pool')
en_out = os.path.join(BASE, 'param_pool', 'backtest_runner.py')
with open(en_out, 'w', encoding='utf-8') as f:
    f.write(en_runner)
en_lines = en_runner.count('\n') + 1
print(f'param_pool/backtest_runner.py: {en_lines} lines, {len(en_runner)} bytes')
