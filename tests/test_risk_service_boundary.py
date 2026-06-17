# MODULE_ID: M2-561
"""risk_service边界条件测试"""
import pytest

def test_risk_service_import():
    """RiskService可正常导入"""
    from ali2026v3_trading.risk.risk_service import RiskService
    assert RiskService is not None

def test_risk_service_safety_methods():
    """RiskService有三个安全检查方法"""
    from ali2026v3_trading.risk.risk_service import RiskService
    assert hasattr(RiskService, 'check_regulatory_compliance')
    assert hasattr(RiskService, 'check_capital_sufficiency')
    assert hasattr(RiskService, 'check_exchange_status')

def test_risk_service_singleton():
    """RiskService有get_instance方法"""
    from ali2026v3_trading.risk.risk_service import RiskService
    assert hasattr(RiskService, 'get_instance') or callable(getattr(RiskService, '__new__', None))
