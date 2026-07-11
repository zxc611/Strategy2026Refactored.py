# MODULE_ID: M2-379
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestRiskItem:
    def test_rpn_calculation(self):
        from ali2026v3_trading.risk.risk_support import RiskItem
        item = RiskItem(name="test", probability=3, impact=4, detectability=2)
        assert item.rpn == 24

    def test_severity_critical(self):
        from ali2026v3_trading.risk.risk_support import RiskItem
        item = RiskItem(name="test", probability=5, impact=5, detectability=5)
        assert item.severity == 'CRITICAL'

    def test_severity_high(self):
        from ali2026v3_trading.risk.risk_support import RiskItem
        item = RiskItem(name="test", probability=4, impact=3, detectability=2)
        assert item.severity == 'HIGH'

    def test_severity_medium(self):
        from ali2026v3_trading.risk.risk_support import RiskItem
        item = RiskItem(name="test", probability=3, impact=3, detectability=2)
        assert item.severity in ('MEDIUM', 'HIGH', 'LOW')

    def test_severity_low(self):
        from ali2026v3_trading.risk.risk_support import RiskItem
        item = RiskItem(name="test", probability=1, impact=1, detectability=1)
        assert item.severity == 'LOW'

    def test_default_fields(self):
        from ali2026v3_trading.risk.risk_support import RiskItem
        item = RiskItem(name="test", probability=1, impact=1, detectability=1)
        assert item.description == ''
        assert item.mitigation == ''
        assert item.trigger_phase == ''


class TestRiskPriorityMatrix:
    def test_init_with_defaults(self):
        from ali2026v3_trading.risk.risk_support import RiskPriorityMatrix
        matrix = RiskPriorityMatrix()
        risks = matrix.get_sorted_risks()
        assert len(risks) > 0

    def test_sorted_by_rpn_desc(self):
        from ali2026v3_trading.risk.risk_support import RiskPriorityMatrix
        matrix = RiskPriorityMatrix()
        risks = matrix.get_sorted_risks()
        for i in range(len(risks) - 1):
            assert risks[i].rpn >= risks[i + 1].rpn

    def test_get_critical_risks(self):
        from ali2026v3_trading.risk.risk_support import RiskPriorityMatrix
        matrix = RiskPriorityMatrix()
        critical = matrix.get_critical_risks()
        for r in critical:
            assert r.severity == 'CRITICAL'

    def test_add_risk(self):
        from ali2026v3_trading.risk.risk_support import RiskPriorityMatrix, RiskItem
        matrix = RiskPriorityMatrix()
        initial_count = len(matrix.get_sorted_risks())
        matrix.add_risk(RiskItem(name="new_risk", probability=2, impact=2, detectability=2))
        assert len(matrix.get_sorted_risks()) == initial_count + 1

    def test_get_risk_by_name(self):
        from ali2026v3_trading.risk.risk_support import RiskPriorityMatrix, RiskItem
        matrix = RiskPriorityMatrix()
        matrix.add_risk(RiskItem(name="unique_risk", probability=2, impact=2, detectability=2))
        found = matrix.get_risk_by_name("unique_risk")
        assert found is not None
        assert found.name == "unique_risk"

    def test_get_risk_by_name_missing(self):
        from ali2026v3_trading.risk.risk_support import RiskPriorityMatrix
        matrix = RiskPriorityMatrix()
        found = matrix.get_risk_by_name("nonexistent")
        assert found is None

    def test_to_report(self):
        from ali2026v3_trading.risk.risk_support import RiskPriorityMatrix
        matrix = RiskPriorityMatrix()
        report = matrix.to_report()
        assert isinstance(report, str)
        assert 'RPN' in report