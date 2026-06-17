# MODULE_ID: M2-509
"""Round1-4 断言测试：P2-04 order_executor 委托 SmartOrderSplitter
验证：1) _plan_volume_split 委托 SmartOrderSplitter
      2) 运行时拆单行为正确
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def test_delegation():
    """_plan_volume_split 内部使用 SmartOrderSplitter"""
    import inspect
    from ali2026v3_trading.order.order_executor import OrderExecutor
    source = inspect.getsource(OrderExecutor._plan_volume_split)
    assert 'SmartOrderSplitter' in source, "_plan_volume_split 未委托 SmartOrderSplitter"
    # 确认不再有内联拆单逻辑
    assert 'remaining = volume' not in source, "_plan_volume_split 仍有内联拆单逻辑"
    print("  OK: _plan_volume_split 委托 SmartOrderSplitter")

def test_runtime_split():
    """运行时拆单行为正确"""
    from ali2026v3_trading.order.order_split_models import SmartOrderSplitter, OrderSplitStrategy
    splitter = SmartOrderSplitter(split_threshold=1, strategy=OrderSplitStrategy.AGGRESSIVE)
    # 构造盘口数据
    asks = [(100.0, 10), (101.0, 20), (102.0, 30)]
    result = splitter.plan_order_split(
        instrument_id='test', volume=25, direction='BUY',
        signal_strength=1.0, bids=None, asks=asks,
    )
    # 验证结果
    assert result is not None, "plan_order_split 返回 None"
    assert hasattr(result, 'child_orders'), "结果缺少 child_orders 属性"
    assert len(result.child_orders) >= 1, "应至少生成1个子单"
    # child_orders 是 dict 列表
    total_vol = sum(c.get('volume', 0) if isinstance(c, dict) else getattr(c, 'volume', 0)
                    for c in result.child_orders)
    assert total_vol > 0, f"子单总量应大于0, 实际={total_vol}"
    print(f"  OK: 运行时拆单正常, {len(result.child_orders)}笔子单, 总量={total_vol}")

if __name__ == '__main__':
    print("=== Round1-4: P2-04 order_executor 委托 SmartOrderSplitter ===")
    test_delegation()
    test_runtime_split()
    print("ALL ASSERTIONS PASSED")
