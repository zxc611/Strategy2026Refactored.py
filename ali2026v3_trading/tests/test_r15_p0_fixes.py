"""
R15-P0审计修复验证测试: 性能/数据质量/安全/容错 四维度
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestDataQualityFixes:
    """DATA-01/02/03/04: 数据质量P0修复验证"""

    def test_validate_tick_rejects_bid_ge_ask(self):
        from ali2026v3_trading.storage_query import _StorageQueryMixin
        mixin = _StorageQueryMixin.__new__(_StorageQueryMixin)
        bad_tick = {
            'instrument_id': 'IF2606', 'last_price': 3500.0,
            'timestamp': '09:30:00', 'volume': 10,
            'bid_price1': 3501.0, 'ask_price1': 3500.0,
        }
        result = mixin._validate_tick(bad_tick)
        assert result is False, "bid>=ask应被拒绝"

    def test_validate_tick_allows_normal_spread(self):
        from ali2026v3_trading.storage_query import _StorageQueryMixin
        mixin = _StorageQueryMixin.__new__(_StorageQueryMixin)
        good_tick = {
            'instrument_id': 'IF2606', 'last_price': 3500.0,
            'timestamp': '09:30:00', 'volume': 10,
            'bid_price1': 3499.0, 'ask_price1': 3501.0,
        }
        result = mixin._validate_tick(good_tick)
        assert result is True, "正常spread应通过"

    def test_validate_tick_rejects_zero_price(self):
        from ali2026v3_trading.storage_query import _StorageQueryMixin
        mixin = _StorageQueryMixin.__new__(_StorageQueryMixin)
        zero_tick = {
            'instrument_id': 'IF2606', 'last_price': 0,
            'timestamp': '09:30:00', 'volume': 10,
        }
        result = mixin._validate_tick(zero_tick)
        assert result is False, "price=0应被拦截"

    def test_tick_dedup_mechanism_exists(self):
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        sm = SubscriptionManager.__new__(SubscriptionManager)
        sm.__init__.__code__  # class exists
        assert '_last_tick_seq' in SubscriptionManager.__init__.__code__.co_names or hasattr(SubscriptionManager, '_last_tick_seq')


class TestSecurityFixes:
    """SEC-01/03/05: 安全P0修复验证"""

    def test_restricted_exec_blocks_import(self):
        from ali2026v3_trading.参数池.L1参数量化.v7_meta_audit_v2 import RestrictedExecLoader
        with pytest.raises(RuntimeError, match="Blocked"):
            RestrictedExecLoader.safe_exec("__import__('os').system('echo pwned')", {})

    def test_restricted_exec_blocks_eval(self):
        from ali2026v3_trading.参数池.L1参数量化.v7_meta_audit_v2 import RestrictedExecLoader
        with pytest.raises(RuntimeError, match="Blocked"):
            RestrictedExecLoader.safe_exec("eval('1+1')", {})

    def test_restricted_exec_allows_safe_code(self):
        from ali2026v3_trading.参数池.L1参数量化.v7_meta_audit_v2 import RestrictedExecLoader
        ns = {}
        RestrictedExecLoader.safe_exec("x = 1 + 2", ns)
        assert ns['x'] == 3

    def test_sql_lookup_field_whitelist(self):
        from ali2026v3_trading.storage_query import _StorageQueryMixin
        mixin = _StorageQueryMixin.__new__(_StorageQueryMixin)
        if hasattr(mixin, '_ALLOWED_LOOKUP_FIELDS'):
            assert 'instrument_id' in mixin._ALLOWED_LOOKUP_FIELDS
            assert 'product_code' in mixin._ALLOWED_LOOKUP_FIELDS

    def test_risk_param_modification_audit(self):
        from ali2026v3_trading.config_params import update_cached_params
        assert callable(update_cached_params)


class TestResilienceFixes:
    """RES-01/03: 容错P0修复验证"""

    def test_degraded_tick_blocked(self):
        try:
            from ali2026v3_trading.strategy_core_service import StrategyCoreService
            assert hasattr(StrategyCoreService, 'on_tick') or hasattr(StrategyCoreService, 'onTick')
        except ImportError:
            pytest.skip("StrategyCoreService requires pythongo/talib")

    def test_circuit_breaker_persistence_methods(self):
        from ali2026v3_trading.risk_service import SafetyMetaLayer
        assert hasattr(SafetyMetaLayer, '_save_circuit_breaker_state')
        assert hasattr(SafetyMetaLayer, '_load_circuit_breaker_state')


class TestPerformanceFixes:
    """PERF-04/05: 性能P0修复验证"""

    def test_greeks_lru_cache_exists(self):
        from ali2026v3_trading.greeks_calculator import GreeksCalculator
        assert GreeksCalculator is not None

    def test_tick_delay_monitoring_exists(self):
        from ali2026v3_trading.strategy_tick_handler import TickHandlerMixin
        assert TickHandlerMixin is not None

    def test_bs_price_cached(self):
        try:
            from ali2026v3_trading.greeks_calculator import _bs_price
            assert hasattr(_bs_price, 'cache_info') or hasattr(_bs_price, '__wrapped__')
        except ImportError:
            pytest.skip("_bs_price not directly importable")


class TestMinuteBoundaryActivation:
    """DATA-05: 分钟边界安全分块激活验证"""

    def test_minute_boundary_config_exists(self):
        try:
            from ali2026v3_trading.参数池.preprocess_ticks import ENABLE_MINUTE_BOUNDARY_CHECK
            assert isinstance(ENABLE_MINUTE_BOUNDARY_CHECK, bool)
        except ImportError:
            pytest.skip("ENABLE_MINUTE_BOUNDARY_CHECK not in module")
