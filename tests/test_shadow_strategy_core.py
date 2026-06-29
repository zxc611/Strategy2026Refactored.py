# MODULE_ID: M2-575
"""测试 strategy/shadow_strategy_core.py — 参数独立性验证"""
import pytest


# ============================================================
# _validate_shadow_param_independence
# ============================================================

class TestValidateShadowParamIndependence:
    """_validate_shadow_param_independence: 参数独立性验证"""

    def _make_service(self):
        """创建一个可测试的ShadowStrategyCoreService实例(不调用例例init__)"""
        from ali2026v3_trading.strategy.shadow_strategy_core import ShadowStrategyCoreService
        svc = object.__new__(ShadowStrategyCoreService)
        return svc

    def test_independent_params_pass(self):
        """参数差异足够大时通过"""
        svc = self._make_service()
        shadow_a = {
            'close_take_profit_ratio': 2.0,
            'close_stop_loss_ratio': 0.8,
            'max_risk_ratio': 0.3,
            'max_hold_minutes': 60,
        }
        shadow_b = {
            'close_take_profit_ratio': 3.0,
            'close_stop_loss_ratio': 1.2,
            'max_risk_ratio': 0.5,
            'max_hold_minutes': 120,
        }
        assert svc._validate_shadow_param_independence(shadow_a, shadow_b) is True

    def test_identical_params_fail(self):
        """参数完全相同时不通过"""
        svc = self._make_service()
        shadow_a = {
            'close_take_profit_ratio': 2.0,
            'close_stop_loss_ratio': 0.8,
            'max_risk_ratio': 0.3,
            'max_hold_minutes': 60,
        }
        shadow_b = dict(shadow_a)
        assert svc._validate_shadow_param_independence(shadow_a, shadow_b) is False

    def test_similar_params_fail(self):
        """参数差异不足20%时不通过"""
        svc = self._make_service()
        shadow_a = {
            'close_take_profit_ratio': 2.0,
            'close_stop_loss_ratio': 0.8,
            'max_risk_ratio': 0.3,
            'max_hold_minutes': 60,
        }
        shadow_b = {
            'close_take_profit_ratio': 2.1,
            'close_stop_loss_ratio': 0.84,
            'max_risk_ratio': 0.315,
            'max_hold_minutes': 63,
        }
        assert svc._validate_shadow_param_independence(shadow_a, shadow_b) is False

    def test_no_comparable_params_passes(self):
        """无可比较参数时通过"""
        svc = self._make_service()
        shadow_a = {'other_param': 1.0}
        shadow_b = {'other_param': 2.0}
        assert svc._validate_shadow_param_independence(shadow_a, shadow_b) is True

    def test_zero_base_value_excluded(self):
        """基值为0的参数不参与比较"""
        svc = self._make_service()
        shadow_a = {
            'close_take_profit_ratio': 0,
            'close_stop_loss_ratio': 0.8,
            'max_risk_ratio': 0.3,
            'max_hold_minutes': 60,
        }
        shadow_b = {
            'close_take_profit_ratio': 0,
            'close_stop_loss_ratio': 1.2,
            'max_risk_ratio': 0.5,
            'max_hold_minutes': 120,
        }
        assert svc._validate_shadow_param_independence(shadow_a, shadow_b) is True

    def test_partial_params_some_independent(self):
        """部分参数独立、部分不存在"""
        svc = self._make_service()
        shadow_a = {
            'close_take_profit_ratio': 2.0,
            'close_stop_loss_ratio': 0.8,
        }
        shadow_b = {
            'close_take_profit_ratio': 3.0,
            'close_stop_loss_ratio': 1.2,
        }
        assert svc._validate_shadow_param_independence(shadow_a, shadow_b) is True

    def test_one_param_missing_in_b(self):
        """b中缺少某个参数时跳过该参数"""
        svc = self._make_service()
        shadow_a = {
            'close_take_profit_ratio': 2.0,
            'close_stop_loss_ratio': 0.8,
            'max_risk_ratio': 0.3,
        }
        shadow_b = {
            'close_take_profit_ratio': 2.1,
            'close_stop_loss_ratio': 0.84,
            # max_risk_ratio缺失
        }
        # 只有2个可比较参数，差异5%，不通过
        assert svc._validate_shadow_param_independence(shadow_a, shadow_b) is False

    def test_boundary_exactly_20_pct(self):
        """恰好20%差异"""
        svc = self._make_service()
        shadow_a = {
            'close_take_profit_ratio': 1.0,
            'close_stop_loss_ratio': 1.0,
            'max_risk_ratio': 1.0,
            'max_hold_minutes': 100,
        }
        shadow_b = {
            'close_take_profit_ratio': 1.2,
            'close_stop_loss_ratio': 1.2,
            'max_risk_ratio': 1.2,
            'max_hold_minutes': 120,
        }
        # 恰好20%，avg_diff=0.2，min_diff_pct=0.20，0.2 < 0.20 为False
        result = svc._validate_shadow_param_independence(shadow_a, shadow_b)
        assert result is False  # 严格小于

    def test_slightly_above_20_pct(self):
        """略超20%差异"""
        svc = self._make_service()
        shadow_a = {
            'close_take_profit_ratio': 1.0,
            'close_stop_loss_ratio': 1.0,
            'max_risk_ratio': 1.0,
            'max_hold_minutes': 100,
        }
        shadow_b = {
            'close_take_profit_ratio': 1.21,
            'close_stop_loss_ratio': 1.21,
            'max_risk_ratio': 1.21,
            'max_hold_minutes': 121,
        }
        result = svc._validate_shadow_param_independence(shadow_a, shadow_b)
        assert result is True

    def test_empty_dicts_pass(self):
        """空字典通过"""
        svc = self._make_service()
        assert svc._validate_shadow_param_independence({}, {}) is True


# ============================================================
# ShadowTradeRecord
# ============================================================

class TestShadowTradeRecord:
    """ShadowTradeRecord: 数据结构"""

    def test_to_dict(self):
        """to_dict返回完整字典"""
        from ali2026v3_trading.strategy.shadow_strategy_core import ShadowTradeRecord
        record = ShadowTradeRecord(
            trade_id="test_001", shadow_type="shadow_a", timestamp="2026-01-01T00:00:00"
        )
        d = record.to_dict()
        assert d['trade_id'] == "test_001"
        assert d['shadow_type'] == "shadow_a"
        assert 'instrument_id' in d

    def test_default_values(self):
        """默认值"""
        from ali2026v3_trading.strategy.shadow_strategy_core import ShadowTradeRecord
        record = ShadowTradeRecord(
            trade_id="test_001", shadow_type="shadow_a", timestamp="2026-01-01"
        )
        assert record.instrument_id == ""
        assert record.direction == ""
        assert record.price == 0.0
        assert record.is_open is True


# ============================================================
# AlphaMetrics
# ============================================================

class TestAlphaMetrics:
    """AlphaMetrics: 数据结构"""

    def test_to_dict(self):
        """to_dict返回完整字典"""
        from ali2026v3_trading.strategy.shadow_strategy_core import AlphaMetrics
        metrics = AlphaMetrics(timestamp="2026-01-01")
        d = metrics.to_dict()
        assert d['timestamp'] == "2026-01-01"
        assert 's1_master_sharpe' in d
        assert 'alpha_ratio' in d

    def test_default_values(self):
        """默认值"""
        from ali2026v3_trading.strategy.shadow_strategy_core import AlphaMetrics
        metrics = AlphaMetrics(timestamp="2026-01-01")
        assert metrics.alpha_ratio == 0.0
        assert metrics.degradation_triggered is False
        assert metrics.master_sharpe_eliminate is False


# ============================================================
# ShadowParamsSnapshot
# ============================================================

class TestShadowParamsSnapshot:
    """ShadowParamsSnapshot: 参数快照"""

    def test_to_dict(self):
        """to_dict返回完整字典"""
        from ali2026v3_trading.strategy.shadow_strategy_core import ShadowParamsSnapshot
        snapshot = ShadowParamsSnapshot(
            shadow_type='shadow_a', locked_at='2026-01-01',
            param_set={'key': 'value'}, param_hash='abc123'
        )
        d = snapshot.to_dict()
        assert d['shadow_type'] == 'shadow_a'
        assert d['param_set'] == {'key': 'value'}
        assert d['param_hash'] == 'abc123'

    def test_default_values(self):
        """默认值"""
        from ali2026v3_trading.strategy.shadow_strategy_core import ShadowParamsSnapshot
        snapshot = ShadowParamsSnapshot(shadow_type='shadow_a', locked_at='2026-01-01')
        assert snapshot.param_set == {}
        assert snapshot.param_yaml_path == ""
        assert snapshot.param_hash == ""


# ============================================================
# ConfigError
# ============================================================

class TestConfigError:
    """ConfigError: 配置错误异常"""

    def test_is_runtime_error(self):
        """ConfigError是RuntimeError子类"""
        from ali2026v3_trading.strategy.shadow_strategy_core import ConfigError
        assert issubclass(ConfigError, RuntimeError)

    def test_can_be_raised(self):
        """可以抛出和捕获"""
        from ali2026v3_trading.strategy.shadow_strategy_core import ConfigError
        with pytest.raises(ConfigError, match="test error"):
            raise ConfigError("test error")
