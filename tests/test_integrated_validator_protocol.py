# MODULE_ID: M2-390
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestStatisticalValidator:
    def test_import(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import StatisticalValidator
        assert StatisticalValidator is not None

    def test_has_test_method(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import StatisticalValidator
        assert hasattr(StatisticalValidator, 'test')

    def test_has_validate_method(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import StatisticalValidator
        assert hasattr(StatisticalValidator, 'validate')

    def test_runtime_checkable(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import StatisticalValidator
        class MyValidator:
            def test(self, *args, **kwargs): return {}
            def validate(self, *args, **kwargs): return {}
        assert isinstance(MyValidator(), StatisticalValidator)

    def test_not_instance_of_non_conforming(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import StatisticalValidator
        class NotAValidator:
            pass
        assert not isinstance(NotAValidator(), StatisticalValidator)


class TestGateValidator:
    def test_import(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import GateValidator
        assert GateValidator is not None

    def test_has_check_method(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import GateValidator
        assert hasattr(GateValidator, 'check')

    def test_runtime_checkable(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import GateValidator
        class MyGateValidator:
            def check(self, context): return {"passed": True}
        assert isinstance(MyGateValidator(), GateValidator)

    def test_not_instance_of_non_conforming(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import GateValidator
        class NotAGateValidator:
            pass
        assert not isinstance(NotAGateValidator(), GateValidator)