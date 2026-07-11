# MODULE_ID: M2-511
"""Round1-5 断言测试：P2-08 signal_service 委托 SignalExpiryManager
验证：1) expire_stale_signals 内部使用 SignalExpiryManager
      2) SignalExpiryManager 运行时行为正确
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def test_delegation():
    """expire_stale_signals 内部使用 SignalExpiryManager"""
    import inspect
    from ali2026v3_trading.signal.signal_service import SignalService
    source = inspect.getsource(SignalService.expire_stale_signals)
    assert 'SignalExpiryManager' in source, "expire_stale_signals 未委托 SignalExpiryManager"
    assert '_expiry_mgr' in source, "未创建 _expiry_mgr 实例"
    print("  OK: expire_stale_signals 委托 SignalExpiryManager")

def test_signal_expiry_manager_runtime():
    """SignalExpiryManager 运行时行为正确"""
    import time
    from ali2026v3_trading.infra.resilience import SignalExpiryManager
    mgr = SignalExpiryManager(default_ttl_sec=0.1)  # 100ms TTL
    # 添加信号
    mgr.put('sig_1', {'price': 100.0})
    mgr.put('sig_2', {'price': 200.0})
    assert mgr.size == 2, f"初始大小应为2, 实际={mgr.size}"
    # 立即获取应成功
    sig = mgr.get('sig_1')
    assert sig is not None, "未过期信号应可获取"
    assert sig['price'] == 100.0, "信号内容不正确"
    # 等待过期
    time.sleep(0.15)
    sig_expired = mgr.get('sig_1')
    assert sig_expired is None, "过期信号应返回None"
    # cleanup 应清理过期信号
    cleaned = mgr.cleanup()
    assert cleaned >= 1, f"cleanup应清理至少1个, 实际={cleaned}"
    print("  OK: SignalExpiryManager 运行时行为正确")

if __name__ == '__main__':
    print("=== Round1-5: P2-08 signal_service 委托 SignalExpiryManager ===")
    test_delegation()
    test_signal_expiry_manager_runtime()
    print("ALL ASSERTIONS PASSED")
