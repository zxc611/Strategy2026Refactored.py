"""E2E-12: SignalService strength过滤修复验证"""
import sys, os, inspect
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

P = 0; F = 0
def ck(n, c, d=''):
    global P, F
    if c: P += 1; print(f'  PASS: {n}')
    else: F += 1; print(f'  FAIL: {n} {d}')

print('=== E2E-12: SignalService strength过滤修复验证 ===')

# ── 1. _filter_by_strength源码验证 ──
print('\n--- 1. _filter_by_strength拦截strength<=0.0 ---')
from ali2026v3_trading.signal.signal_service import SignalGenerator
src = inspect.getsource(SignalGenerator._filter_by_strength)
ck('拦截strength<=0.0', 'signal_strength <= 0.0' in src, '仍用<0.0')
ck('不再用<0.0', 'signal_strength < 0.0' not in src, '仍用<0.0而非<=0.0')

# ── 2. HFT调用点signal_strength参数验证 ──
print('\n--- 2. HFT调用点signal_strength参数验证 ---')
from ali2026v3_trading.strategy import tick_hft
from ali2026v3_trading.strategy import tick_hft as tick_hft_dispatch

def check_signal_strength_in_calls(source, filename):
    lines = source.split('\n')
    found = 0
    total = 0
    i = 0
    while i < len(lines):
        if 'sig_svc.generate_signal(' in lines[i]:
            total += 1
            block = ''
            paren_count = 0
            j = i
            while j < len(lines):
                block += lines[j] + '\n'
                paren_count += lines[j].count('(') - lines[j].count(')')
                if paren_count <= 0 and j > i:
                    break
                j += 1
            if 'signal_strength=' in block:
                found += 1
            else:
                print(f'    MISSING in {filename} near line {i+1}')
            i = j + 1
        else:
            i += 1
    return found, total

hft_src = inspect.getsource(tick_hft)
dispatch_src = inspect.getsource(tick_hft_dispatch)

hft_found, hft_total = check_signal_strength_in_calls(hft_src, 'tick_hft.py')
ck(f'tick_hft.py: {hft_found}/{hft_total}调用传signal_strength', hft_found == hft_total)

dispatch_found, dispatch_total = check_signal_strength_in_calls(dispatch_src, 'tick_hft_dispatch.py')
ck(f'tick_hft_dispatch.py: {dispatch_found}/{dispatch_total}调用传signal_strength', dispatch_found == dispatch_total)

# ── 3. SignalContext strength<=0.0被拦截 ──
print('\n--- 3. SignalContext strength<=0.0被拦截 ---')
from ali2026v3_trading.signal.signal_service import SignalService, SignalContext, SignalGenerator

svc = SignalService()
gen = SignalGenerator(svc)

ctx0 = SignalContext(instrument_id='IF2607', signal_type='BUY', price=4000.0, volume=1, signal_strength=0.0)
result0 = gen._filter_by_strength(ctx0)
ck('strength=0.0被拦截', result0.rejected is True, 'strength=0.0未被拦截')

ctx_neg = SignalContext(instrument_id='IF2607', signal_type='BUY', price=4000.0, volume=1, signal_strength=-0.1)
result_neg = gen._filter_by_strength(ctx_neg)
ck('strength=-0.1被拦截', result_neg.rejected is True)

ctx_pos = SignalContext(instrument_id='IF2607', signal_type='BUY', price=4000.0, volume=1, signal_strength=0.5)
result_pos = gen._filter_by_strength(ctx_pos)
ck('strength=0.5不被拦截', result_pos.rejected is False)

ctx_tiny = SignalContext(instrument_id='IF2607', signal_type='BUY', price=4000.0, volume=1, signal_strength=0.01)
result_tiny = gen._filter_by_strength(ctx_tiny)
ck('strength=0.01不被拦截', result_tiny.rejected is False)

ctx_close = SignalContext(instrument_id='IF2607', signal_type='CLOSE_LONG', price=4000.0, volume=1, signal_strength=0.0)
result_close = gen._filter_by_strength(ctx_close)
ck('CLOSE信号strength=0.0不被拦截', result_close.rejected is False)

# ── 4. 自适应阈值验证 ──
print('\n--- 4. 自适应阈值验证 ---')
from ali2026v3_trading.signal.signal_timing_filter import AdaptiveSignalThreshold

adaptive = AdaptiveSignalThreshold(min_threshold=0.15)
ck('初始阈值>=0.15', adaptive.threshold >= 0.15)

ctx_strong = SignalContext(instrument_id='IF2607', signal_type='BUY', price=4000.0, volume=1, signal_strength=0.5)
svc2 = SignalService()
svc2._adaptive_threshold = adaptive
gen2 = SignalGenerator(svc2)
result_strong = gen2._filter_by_adaptive(ctx_strong)
ck('strength=0.5>0.3通过自适应阈值', result_strong.rejected is False, f'rejected={result_strong.rejected}')

ctx_weak = SignalContext(instrument_id='IF2607', signal_type='BUY', price=4000.0, volume=1, signal_strength=0.1)
result_weak = gen2._filter_by_adaptive(ctx_weak)
ck('strength=0.1<0.15被自适应阈值拦截', result_weak.rejected is True, f'rejected={result_weak.rejected}')

# ── 5. generate_signal完整链路验证 ──
print('\n--- 5. generate_signal完整链路验证 ---')
svc3 = SignalService()
# strength=0.0 应在第1层被拦截，不会到达第7层自适应阈值
sig3 = svc3.generate_signal(instrument_id='IF2607', signal_type='BUY', price=4000.0, volume=1, signal_strength=0.0)
ck('strength=0.0的信号被generate_signal拒绝', sig3 is None)

sig4 = svc3.generate_signal(instrument_id='IF2607', signal_type='BUY', price=4000.0, volume=1, signal_strength=0.5)
ck('strength=0.5的信号通过generate_signal', sig4 is not None, f'sig4={sig4}')

print(f'\n=== E2E-12 结果: {P} PASS / {F} FAIL ===')
if F > 0:
    sys.exit(1)
