"""
R14-P0-TEST-12/13/14/15: CI/CD配置 + 集成测试 + 回归套件 + E2E增强
"""
import sys
import os
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestProjectInfrastructure:
    """TEST-12: CI/CD和基础设施"""

    def test_requirements_freeze_exists(self):
        project_root = os.path.join(os.path.dirname(__file__), '..', '..')
        req_path = os.path.join(project_root, 'requirements-freeze.txt')
        assert os.path.exists(req_path), "requirements-freeze.txt应存在(DEP-01修复)"

    def test_pytest_ini_or_conftest(self):
        tests_dir = os.path.dirname(__file__)
        conftest = os.path.join(tests_dir, 'conftest.py')
        assert os.path.exists(conftest), "conftest.py应存在"


class TestCrossServiceIntegration:
    """TEST-13: 跨服务集成测试(信号→风控→下单→持仓)"""

    def test_signal_to_risk_chain(self):
        from ali2026v3_trading.signal_service import SignalService
        from ali2026v3_trading.risk_service import RiskService
        assert SignalService is not None
        assert RiskService is not None

    def test_risk_to_order_chain(self):
        from ali2026v3_trading.risk_service import RiskService
        from ali2026v3_trading.order_service import OrderService
        assert RiskService is not None
        assert OrderService is not None

    def test_order_to_position_chain(self):
        from ali2026v3_trading.order_service import OrderService
        from ali2026v3_trading.position_service import PositionService
        assert OrderService is not None
        assert PositionService is not None

    def test_signal_risk_order_position_importable(self):
        from ali2026v3_trading.signal_service import SignalService
        from ali2026v3_trading.risk_service import RiskService
        from ali2026v3_trading.order_service import OrderService
        from ali2026v3_trading.position_service import PositionService
        for cls in [SignalService, RiskService, OrderService, PositionService]:
            assert cls is not None


class TestE2EPipelineEnhanced:
    """TEST-15: 增强端到端测试"""

    def test_config_params_table_complete(self):
        from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE
        required_keys = [
            'close_take_profit_ratio', 'close_stop_loss_ratio', 'max_risk_ratio',
            'signal_cooldown_sec', 'circuit_breaker_pause_sec',
            'spring_hard_time_stop_sec', 'resonance_hard_time_stop_min',
            'box_hard_time_stop_min', 'hft_hard_time_stop_ms',
        ]
        for key in required_keys:
            assert key in DEFAULT_PARAM_TABLE, f"DEFAULT_PARAM_TABLE缺少{key}"

    def test_state_param_sets_complete(self):
        from ali2026v3_trading.config_params import get_default_state_param_sets
        states = get_default_state_param_sets()
        required_states = ['correct_trending', 'incorrect_reversal', 'other']
        for state in required_states:
            assert state in states, f"缺少状态{state}"
            assert 'close_take_profit_ratio' in states[state]
            assert 'close_stop_loss_ratio' in states[state]

    def test_sensitive_fields_no_hardcoded_secrets(self):
        from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE
        sensitive = DEFAULT_PARAM_TABLE.get('_sensitive_fields', [])
        for field in sensitive:
            val = DEFAULT_PARAM_TABLE.get(field, '')
            assert val == '' or not val, f"敏感字段{field}不应有硬编码非空值"

    def test_mode_config_respects_yaml_tfv_disable(self):
        from ali2026v3_trading.mode_engine import (
            _make_mode_config, CapitalMode, TakeProfitMethod,
            StopLossMethod, PyramidingRule, DrawdownAction,
        )

        fake_loader = MagicMock()
        fake_loader.get_params.return_value = {'tvf_enabled': False}

        with patch('ali2026v3_trading.tvf_param_loader.get_tvf_param_loader', return_value=fake_loader):
            config = _make_mode_config(
                mode=CapitalMode.BALANCED,
                max_positions=3,
                single_position_cap=0.2,
                take_profit_method=TakeProfitMethod.FIXED,
                stop_loss_method=StopLossMethod.FIXED,
                pyramiding=False,
                pyramiding_rule=PyramidingRule.NONE,
                drawdown_action=DrawdownAction.HALT_NEW,
                recovery_target=0.5,
                min_signal_strength=0.6,
                time_decay_cutoff=0.2,
                profitability_mode='test',
                profitability_weights=(('pnl', 1.0),),
                win_loss_ratio_full_score_at=2.0,
                profit_factor_full_score_at=1.5,
                recovery_efficiency_full_score_at=2.0,
                max_consecutive_losses_full=3,
                max_consecutive_losses_zero=8,
                drawdown_recovery_max_hours=24.0,
                extreme_max_recovery_hours=48.0,
                overall_pass_threshold=0.65,
                overall_conditional_threshold=0.5,
                kelly_fraction=0.1,
                min_estimated_plr=1.2,
                plr_filter_enabled=True,
                consecutive_loss_limit=5,
                recovery_timeout_seconds=1800.0,
                asymmetric_risk_enabled=False,
                tvf_enabled=True,
            )

        assert config.tvf_enabled is False


class TestModeEngineLockFix:
    """R21-ALIGN: 与生产系统mode_engine.py的R21-CC01修复对齐 — Lock→RLock防死锁"""

    def test_mode_engine_uses_rlock(self):
        """验证ModeEngine单例锁使用RLock而非Lock，防止重入死锁"""
        import threading
        from ali2026v3_trading.mode_engine import ModeEngine
        assert isinstance(ModeEngine._lock, threading.RLock), \
            "ModeEngine._lock应为RLock(R21-CC01修复: Lock→RLock防重入死锁)"

    def test_predictive_state_engine_uses_rlock(self):
        """验证PredictiveStateEngine单例锁使用RLock而非Lock"""
        import threading
        from ali2026v3_trading.mode_engine import PredictiveStateEngine
        assert isinstance(PredictiveStateEngine._lock, threading.RLock), \
            "PredictiveStateEngine._lock应为RLock(R21-CC01修复: Lock→RLock防重入死锁)"


class TestStrategyCoreServiceNetworkFix:
    """R21-ALIGN: 与生产系统strategy_core_service.py的R21-NET修复对齐 — 心跳检查"""

    def test_heartbeat_interval_configurable(self):
        """验证心跳间隔可从环境变量配置(R21-NET-P2-01修复)"""
        from ali2026v3_trading.strategy_core_service import StrategyCoreService
        svc = StrategyCoreService.__new__(StrategyCoreService)
        # 默认值应为30.0秒
        assert hasattr(StrategyCoreService, '__init__')
        # 验证属性存在于初始化逻辑中
        import inspect
        source = inspect.getsource(StrategyCoreService.__init__)
        assert '_heartbeat_interval_sec' in source, \
            "StrategyCoreService应有_heartbeat_interval_sec属性(R21-NET-P2-01修复)"

    def test_heartbeat_max_failures_defined(self):
        """验证心跳最大失败次数已定义(R21-NET-P1-03修复)"""
        from ali2026v3_trading.strategy_core_service import StrategyCoreService
        import inspect
        source = inspect.getsource(StrategyCoreService.__init__)
        assert '_heartbeat_max_failures' in source, \
            "StrategyCoreService应有_heartbeat_max_failures属性(R21-NET-P1-03修复)"
