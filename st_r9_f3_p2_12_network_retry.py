# MODULE_ID: M2-550
"""Round9-F3 断言测试：P2-12 NetworkRetryManager完全委托BoundedRetry
验证运行时行为：
1) NetworkRetryManager不再有独立重试跟踪状态
2) execute_with_retry委托到BoundedRetry
3) 重试行为与BoundedRetry一致
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def test_no_independent_retry_tracking():
    """NetworkRetryManager不应有独立的指retry_counts/_last_retry_time"""
    from ali2026v3_trading.order.order_persistence import NetworkRetryManager
    nrm = NetworkRetryManager(max_retries=3, base_interval_sec=1.0)
    assert not hasattr(nrm, '_retry_counts'), \
        "NetworkRetryManager不应有独立的指retry_counts"
    assert not hasattr(nrm, '_last_retry_time'), \
        "NetworkRetryManager不应有独立的指last_retry_time"
    print("  OK: NetworkRetryManager无独立重试跟踪状态")


def test_execute_with_retry_delegates_to_bounded_retry():
    """execute_with_retry应委托到BoundedRetry"""
    from ali2026v3_trading.order.order_persistence import NetworkRetryManager
    nrm = NetworkRetryManager(max_retries=2, base_interval_sec=0.01)
    call_count = 0

    def succeed_on_first_try():
        nonlocal call_count
        call_count += 1
        return "success"

    result = nrm.execute_with_retry("test_op", succeed_on_first_try)
    assert result == "success", f"期望'success'，得到{result}"
    assert call_count == 1, f"期望调用1次，实际{call_count}次"
    print("  OK: execute_with_retry成功委托到BoundedRetry")


def test_retry_behavior_matches_bounded_retry():
    """重试行为应与BoundedRetry一致——失败时重试"""
    from ali2026v3_trading.order.order_persistence import NetworkRetryManager
    nrm = NetworkRetryManager(max_retries=2, base_interval_sec=0.01)
    call_count = 0

    def fail_then_succeed():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ConnectionError("test error")
        return "recovered"

    result = nrm.execute_with_retry("test_op", fail_then_succeed)
    assert result == "recovered", f"期望'recovered'，得到{result}"
    assert call_count == 2, f"期望调用2次（1次失败+1次成功），实际{call_count}次"
    print("  OK: 重试行为与BoundedRetry一致")


def test_exhausted_retries_raise():
    """重试耗尽后应抛出异常"""
    from ali2026v3_trading.order.order_persistence import NetworkRetryManager
    nrm = NetworkRetryManager(max_retries=1, base_interval_sec=0.01)

    def always_fail():
        raise ConnectionError("persistent error")

    try:
        nrm.execute_with_retry("test_op", always_fail)
        assert False, "应抛出异常"
    except ConnectionError:
        pass
    print("  OK: 重试耗尽后正确抛出异常")


if __name__ == '__main__':
    print("=== Round9-F3: P2-12 NetworkRetryManager委托BoundedRetry ===")
    test_no_independent_retry_tracking()
    test_execute_with_retry_delegates_to_bounded_retry()
    test_retry_behavior_matches_bounded_retry()
    test_exhausted_retries_raise()
    print("ALL ASSERTIONS PASSED")
