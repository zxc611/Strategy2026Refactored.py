# MODULE_ID: M2-292
from pathlib import Path
P = Path('.')

src = (P / 'infra/resilience_retry.py').read_text(encoding='utf-8')
for kw in ['cancel_event', 'retry_on', 'on_exhausted', 'backoff_strategy']:
    assert kw in src, f'Missing {kw}'
print('Check 1 PASSED: BoundedRetry supports new params')

src2 = (P / 'infra/shared_utils.py').read_text(encoding='utf-8')
assert 'BoundedRetry' in src2
assert 'for attempt in range(max_retries' not in src2
print('Check 2 PASSED: retry_with_limit delegates')

src3 = (P / 'order/order_persistence.py').read_text(encoding='utf-8')
assert 'BoundedRetry' in src3
assert 'while self.should_retry' not in src3
print('Check 3 PASSED: NetworkRetryManager delegates')

src4 = (P / 'infra/_ops_framework.py').read_text(encoding='utf-8')
assert 'BoundedRetry' in src4
print('Check 4 PASSED: _ops_framework uses BoundedRetry')

src5 = (P / 'position/position_command_service.py').read_text(encoding='utf-8')
assert 'BoundedRetry' in src5
assert 'for _retry in range(1, self.CLOSE_RETRY_MAX_ATTEMPTS' not in src5
print('Check 5 PASSED: position_command uses BoundedRetry')

for f in ['infra/shared_utils.py', 'order/order_persistence.py', 'infra/_ops_framework.py', 'position/position_command_service.py']:
    s = (P / f).read_text(encoding='utf-8')
    assert 'TODO(P2-12)' not in s, f'{f} still has TODO(P2-12)'
print('Check 6 PASSED: No TODO(P2-12) remaining')

print('All 6 static checks PASSED!')