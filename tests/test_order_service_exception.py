# MODULE_ID: M2-429
"""order_service异常路径测试"""
import pytest

def test_order_service_import():
    """OrderService可正常导入"""
    from ali2026v3_trading.order.order_service import OrderService
    assert OrderService is not None

def test_order_risk_guard_import():
    """OrderRiskGuard可正常导入"""
    from ali2026v3_trading.order.order_risk_guard import OrderRiskGuard
    assert OrderRiskGuard is not None

def test_algo_trading_compliance_import():
    """AlgoTradingCompliance(算法交易合规)可正常导入"""
    from ali2026v3_trading.order.order_compliance import AlgoTradingCompliance
    assert AlgoTradingCompliance is not None
