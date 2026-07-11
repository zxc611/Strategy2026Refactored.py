# MODULE_ID: M2-389
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestTsResultExecutor:
    def test_import(self):
        from ali2026v3_trading.param_pool import ts_result_executor
        assert ts_result_executor is not None

    def test_has_execute_round(self):
        from ali2026v3_trading.param_pool.ts_result_executor import _execute_round
        assert callable(_execute_round)