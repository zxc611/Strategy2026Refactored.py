# MODULE_ID: M2-567
"""测试: risk/safety_meta_position.py (补充)"""
import pytest
from unittest.mock import MagicMock
from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService


class TestHardTimeStopAndComplianceService:
    def _make(self):
        return HardTimeStopAndComplianceService(MagicMock(), MagicMock())

    def test_confirm_daily_resume_no_approver(self):
        svc = self._make()
        result = svc.confirm_daily_resume(caller_id='')
        assert result is False

    def test_confirm_daily_resume_wrong_token(self):
        svc = self._make()
        result = svc.confirm_daily_resume(caller_id='admin', resume_token='wrong')
        assert result is False

    def test_check_position_hard_time_stop_callable(self):
        svc = self._make()
        assert callable(svc.check_position_hard_time_stop)

    def test_get_health_status_callable(self):
        svc = self._make()
        assert callable(svc.get_health_status)

    def test_check_capital_sufficiency_callable(self):
        svc = self._make()
        assert callable(svc.check_capital_sufficiency)

    def test_creation_sets_defaults(self):
        svc = self._make()
        assert hasattr(svc, '_logic_reversal_priority')
        assert hasattr(svc, '_rollover_cost_bps')