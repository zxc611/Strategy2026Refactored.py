# MODULE_ID: M2-378
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestResultHandlerFunctions:
    def test_record_result(self):
        from ali2026v3_trading.risk_engine.result_handler import record_result
        assert callable(record_result)

    def test_record_and_publish(self):
        from ali2026v3_trading.risk_engine.result_handler import record_and_publish
        assert callable(record_and_publish)

    def test_record_block_with_context(self):
        from ali2026v3_trading.risk_engine.result_handler import record_block_with_context
        assert callable(record_block_with_context)

    def test_record_result_with_mock(self):
        from ali2026v3_trading.risk_engine.result_handler import record_result
        from unittest.mock import MagicMock
        result = MagicMock()
        risk_service = MagicMock()
        returned = record_result(result, risk_service)
        assert returned is not None