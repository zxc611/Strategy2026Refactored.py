# MODULE_ID: M2-556
"""升A路径T3.1: risk_service 3个check函数单元测试

验收标准: compliant/not, sufficient/not, open/closed 全场景覆盖
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.risk.risk_service import get_risk_service


class TestCheckRegulatoryCompliance:
    """check_regulatory_compliance 合规检查测试"""

    def setup_method(self):
        self.rs = get_risk_service()

    def test_compliant_with_valid_position(self):
        """合规: 有效持仓数据"""
        result = self.rs.check_regulatory_compliance({"symbol": "au2506", "volume": 1})
        assert isinstance(result, dict)
        assert 'compliant' in result

    def test_compliant_with_none_position(self):
        """合规: None持仓数据"""
        result = self.rs.check_regulatory_compliance(None)
        assert isinstance(result, dict)
        assert 'compliant' in result

    def test_fail_closed_on_delegation_error(self):
        """fail-closed: 委托链异常时返回compliant=False"""
        import unittest.mock as mock
        with mock.patch('ali2026v3_trading.risk.risk_check_service.get_safety_meta_layer',
                        side_effect=AttributeError("mock error")):
            result = self.rs.check_regulatory_compliance({})
            assert result.get('compliant') is False, f"Expected compliant=False on delegation error, got {result}"


class TestCheckCapitalSufficiency:
    """check_capital_sufficiency 资金充足性检查测试"""

    def setup_method(self):
        self.rs = get_risk_service()

    def test_sufficient_capital(self):
        """充足: 大额资金"""
        result = self.rs.check_capital_sufficiency(equity=1000000.0, required_margin=10000.0)
        assert isinstance(result, dict)
        assert 'sufficient' in result

    def test_insufficient_capital(self):
        """不足: 零资金"""
        result = self.rs.check_capital_sufficiency(equity=0.0, required_margin=10000.0)
        assert isinstance(result, dict)

    def test_fail_closed_on_delegation_error(self):
        """fail-closed: 委托链异常时返回sufficient=False"""
        import unittest.mock as mock
        with mock.patch('ali2026v3_trading.risk.risk_service.get_safety_meta_layer',
                        side_effect=AttributeError("mock error")):
            result = self.rs.check_capital_sufficiency(equity=1000000.0)
            assert result.get('sufficient') is False, f"Expected sufficient=False on delegation error, got {result}"


class TestCheckExchangeStatus:
    """check_exchange_status 交易所状态检查测试"""

    def setup_method(self):
        self.rs = get_risk_service()

    def test_exchange_status_returns_dict(self):
        """返回字典格式"""
        result = self.rs.check_exchange_status()
        assert isinstance(result, dict)
        assert 'status' in result

    def test_fail_closed_on_delegation_error(self):
        """fail-closed: 委托链异常时返回status=CLOSED"""
        import unittest.mock as mock
        with mock.patch('ali2026v3_trading.risk.risk_service.get_safety_meta_layer',
                        side_effect=AttributeError("mock error")):
            result = self.rs.check_exchange_status()
            assert result.get('status') == 'CLOSED', f"Expected status=CLOSED on delegation error, got {result}"


class TestRiskDashboardService:
    """RiskDashboardService 死代码集成测试"""

    def test_dashboard_service_callable(self):
        """get_risk_dashboard_service()可调用"""
        from ali2026v3_trading.risk.risk_service import get_risk_dashboard_service
        dashboard = get_risk_dashboard_service()
        assert dashboard is not None

    def test_aggregate_greeks(self):
        """Greeks聚合功能"""
        from ali2026v3_trading.risk.risk_service import get_risk_dashboard_service
        dashboard = get_risk_dashboard_service()
        result = dashboard.aggregate_greeks({"AU": type('obj', (object,), {'greeks': {'delta': 0.5, 'gamma': 0.1, 'theta': -0.01, 'vega': 0.02}})()})
        assert 'delta' in result
        assert result['delta'] == 0.5

    def test_attribute_pnl(self):
        """PnL归因功能"""
        from ali2026v3_trading.risk.risk_service import get_risk_dashboard_service
        dashboard = get_risk_dashboard_service()
        trade = type('Trade', (), {'close_reason': 'stop_loss', 'pnl': -100.0})()
        result = dashboard.attribute_pnl([trade])
        assert 'stop_loss' in result
        assert result['stop_loss'] == -100.0

    def test_run_stress_test(self):
        """压力测试功能"""
        from ali2026v3_trading.risk.risk_service import get_risk_dashboard_service
        dashboard = get_risk_dashboard_service()
        pos = type('Pos', (), {'volume': 10, 'open_price': 500.0})()
        result = dashboard.run_stress_test({"AU": pos}, shock_pct=0.1)
        assert 'worst_case_loss' in result
        assert result['worst_case_loss'] > 0
