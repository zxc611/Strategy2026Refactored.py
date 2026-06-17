# MODULE_ID: M2-290
import sys
sys.path.insert(0, r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo')
from ali2026v3_trading.infra.resilience import BoundedRetry

# Test 1: basic retry success
c = [0]
def succeed_on_3rd():
    c[0] += 1
    if c[0] < 3:
        raise ValueError('not yet')
    return 'ok'
br = BoundedRetry(max_retries=5, base_delay=0.01, on_exhausted='raise')
result = br.execute(succeed_on_3rd)
assert result == 'ok', f"Expected 'ok', got {result}"
assert c[0] == 3, f"Expected 3 calls, got {c[0]}"
print("Test 1 PASSED: basic retry success")

# Test 2: retry exhausted raise
def always_fail():
    raise RuntimeError('fail')
br2 = BoundedRetry(max_retries=2, base_delay=0.01, on_exhausted='raise')
try:
    br2.execute(always_fail)
    assert False, "Should have raised"
except RuntimeError:
    print("Test 2 PASSED: retry exhausted raise")

# Test 3: retry exhausted return_none
br3 = BoundedRetry(max_retries=2, base_delay=0.01, on_exhausted='return_none')
result3 = br3.execute(always_fail)
assert result3 is None, f"Expected None, got {result3}"
print("Test 3 PASSED: retry exhausted return_none")

# Test 4: retry_on filter
c4 = [0]
def raise_value_error():
    c4[0] += 1
    raise ValueError('filtered out')
br4 = BoundedRetry(max_retries=3, base_delay=0.01, retry_on=(TypeError,), on_exhausted='return_none')
try:
    br4.execute(raise_value_error)
    assert False, "Should have raised ValueError"
except ValueError:
    assert c4[0] == 1, f"Expected 1 call (no retry), got {c4[0]}"
    print("Test 4 PASSED: retry_on filter")

# Test 5: cancel_event
import threading
cancel = threading.Event()
cancel.set()
c5 = [0]
def always_fail5():
    c5[0] += 1
    raise RuntimeError('fail')
br5 = BoundedRetry(max_retries=5, base_delay=0.5, cancel_event=cancel, on_exhausted='raise')
try:
    br5.execute(always_fail5)
    assert False, "Should have raised InterruptedError"
except InterruptedError:
    print("Test 5 PASSED: cancel_event interrupts")

# Test 6: linear backoff
br6 = BoundedRetry(max_retries=3, base_delay=1.0, backoff_strategy='linear')
assert br6._calc_delay(0) == 1.0, f"Expected 1.0, got {br6._calc_delay(0)}"
assert br6._calc_delay(1) == 2.0, f"Expected 2.0, got {br6._calc_delay(1)}"
assert br6._calc_delay(2) == 3.0, f"Expected 3.0, got {br6._calc_delay(2)}"
print("Test 6 PASSED: linear backoff")

# Test 7: retry_with_limit delegates
from ali2026v3_trading.infra.shared_utils import retry_with_limit
c7 = [0]
def succeed_on_2nd():
    c7[0] += 1
    if c7[0] < 2:
        raise ValueError('not yet')
    return 'ok'
result7 = retry_with_limit(succeed_on_2nd, max_retries=3, base_delay=0.01)
assert result7 == 'ok', f"Expected 'ok', got {result7}"
print("Test 7 PASSED: retry_with_limit delegates")

print("\nAll 7 runtime tests PASSED!")