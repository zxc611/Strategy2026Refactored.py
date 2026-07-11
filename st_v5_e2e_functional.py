#!/usr/bin/env python3
# MODULE_ID: M2-601
"""端到端功能测试"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import numpy as np
import pandas as pd
import time

print('=== 端到端功能测试 ===')
print()

n = 1000
np.random.seed(42)
df = pd.DataFrame({
    'close': 100 + np.cumsum(np.random.randn(n) * 0.5),
    'high': 101 + np.cumsum(np.random.randn(n) * 0.5),
    'low': 99 + np.cumsum(np.random.randn(n) * 0.5),
    'volume': np.random.randint(1000, 10000, n),
})

try:
    print('1. OBOS向量化测试')
    from ali2026v3_trading.precompute._obos import compute_obos_vectorized
    start = time.time()
    obos = compute_obos_vectorized(df)
    elapsed = time.time() - start
    print(f'   OBOS计算{n}条数据耗时: {elapsed*1000:.2f}ms')
    print(f'   输出列: {list(obos.columns)[:5]}...')
    print('   ✓ 通过')
except Exception as e:
    print(f'   ✗ 失败: {e}')
print()

try:
    print('2. KL-RPD向量化测试')
    from ali2026v3_trading.precompute._kl_rpd import compute_kl_rpd_vectorized
    start = time.time()
    kl_rpd = compute_kl_rpd_vectorized(df)
    elapsed = time.time() - start
    print(f'   KL-RPD计算{n}条数据耗时: {elapsed*1000:.2f}ms')
    print(f'   输出列: {list(kl_rpd.columns)[:5]}...')
    print('   ✓ 通过')
except Exception as e:
    print(f'   ✗ 失败: {e}')
print()

try:
    print('3. Signals向量化测试')
    from ali2026v3_trading.precompute._signals import compute_signals_vectorized
    start = time.time()
    signals = compute_signals_vectorized(df)
    elapsed = time.time() - start
    print(f'   Signals计算{n}条数据耗时: {elapsed*1000:.2f}ms')
    print(f'   输出列: {list(signals.columns)[:5]}...')
    print('   ✓ 通过')
except Exception as e:
    print(f'   ✗ 失败: {e}')
print()

try:
    print('4. 信号衰减向量化测试')
    from ali2026v3_trading.precompute._signal_decay import compute_decay_and_linkage_vectorized
    df2 = df.copy()
    df2['signal_raw'] = np.random.randn(n)
    df2['signal_direction'] = np.random.choice([-1, 0, 1], n)
    start = time.time()
    decay = compute_decay_and_linkage_vectorized(df2)
    elapsed = time.time() - start
    print(f'   信号衰减计算{n}条数据耗时: {elapsed*1000:.2f}ms')
    print(f'   输出列: {list(decay.columns)[:5]}...')
    print('   ✓ 通过')
except Exception as e:
    print(f'   ✗ 失败: {e}')
print()

try:
    print('5. L0状态向量化测试')
    from ali2026v3_trading.precompute._l0_state import compute_l0_state_vectorized
    df3 = df.copy()
    df3['signal_raw'] = np.random.randn(n)
    start = time.time()
    l0 = compute_l0_state_vectorized(df3)
    elapsed = time.time() - start
    print(f'   L0状态计算{n}条数据耗时: {elapsed*1000:.2f}ms')
    print(f'   输出列: {list(l0.columns)[:5]}...')
    print('   ✓ 通过')
except Exception as e:
    print(f'   ✗ 失败: {e}')
print()

try:
    print('6. Position决策向量化测试')
    from ali2026v3_trading.precompute._position_decision import compute_position_decision_vectorized
    df4 = df.copy()
    df4['signal_strength'] = np.random.rand(n)
    df4['signal_direction'] = np.random.choice([-1, 0, 1], n)
    start = time.time()
    pos = compute_position_decision_vectorized(df4)
    elapsed = time.time() - start
    print(f'   Position决策计算{n}条数据耗时: {elapsed*1000:.2f}ms')
    print(f'   输出列: {list(pos.columns)[:5]}...')
    print('   ✓ 通过')
except Exception as e:
    print(f'   ✗ 失败: {e}')
print()

print('=== 端到端功能测试完成 ===')