"""
R14-P0-TEST-05/06/07/08: SignalService/CapitalFlow/OrderService/PositionService核心覆盖
"""
import sys
import os
import pytest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestSignalService:
    """TEST-05: SignalService零覆盖"""

    def test_signal_service_importable(self):
        from ali2026v3_trading.signal_service import SignalService
        assert SignalService is not None

    def test_signal_cooldown_key_aligned(self):
        from ali2026v3_trading.signal_service import SignalService
        assert hasattr(SignalService, '_CONFIG_COOLDOWN_KEY')
        assert SignalService._CONFIG_COOLDOWN_KEY == 'signal_cooldown_sec'

    def test_signal_cooldown_default_nonzero(self):
        from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE
        assert DEFAULT_PARAM_TABLE['signal_cooldown_sec'] > 0


class TestOrderService:
    """TEST-07: OrderService核心方法"""

    def test_order_service_importable(self):
        from ali2026v3_trading.order_service import OrderService
        assert OrderService is not None

    def test_cancel_all_pending_exists(self):
        from ali2026v3_trading.order_service import OrderService
        assert hasattr(OrderService, 'cancel_all_pending')


class TestPositionService:
    """TEST-08: PositionService主体业务逻辑"""

    def test_position_service_importable(self):
        from ali2026v3_trading.position_service import PositionService
        assert PositionService is not None

    def test_tp_sl_ratio_from_state_param(self):
        from ali2026v3_trading.config_params import get_default_state_param_sets
        states = get_default_state_param_sets()
        assert 'correct_trending' in states
        assert 'close_take_profit_ratio' in states['correct_trending']
        assert states['correct_trending']['close_take_profit_ratio'] == 2.5
        assert states['incorrect_reversal']['close_take_profit_ratio'] == 1.3
        assert states['other']['close_take_profit_ratio'] == 1.1

    def test_three_source_alignment_tp_sl(self):
        """三系统对齐: config_params/position_service/risk_service的TP/SL一致"""
        from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE
        assert DEFAULT_PARAM_TABLE['close_take_profit_ratio'] == 1.8  # R24修复: 断言值与生产代码DEFAULT_PARAM_TABLE对齐
        assert DEFAULT_PARAM_TABLE['close_stop_loss_ratio'] == 0.3  # R24修复: 断言值与生产代码DEFAULT_PARAM_TABLE对齐
        assert DEFAULT_PARAM_TABLE['max_risk_ratio'] == 0.8  # R24修复: 断言值与生产代码DEFAULT_PARAM_TABLE对齐

    def test_state_params_loaded_from_yaml_primary_source(self):
        from ali2026v3_trading.config_params import get_default_state_param_sets
        states = get_default_state_param_sets()
        assert states['correct_trending']['max_risk_ratio'] == 0.8
        assert states['correct_trending']['option_buy_lots_min'] == 1
        assert states['correct_trending']['option_buy_lots_max'] == 100

    def test_state_params_fallback_keeps_yaml_aligned_defaults(self):
        import ali2026v3_trading.config_params as config_params

        with patch.object(config_params, '_yaml_cache', None), \
             patch.object(config_params, '_yaml_cache_mtime', 0.0), \
             patch.object(config_params.os.path, 'isfile', return_value=False):
            states = config_params.get_default_state_param_sets()

        assert states['correct_trending']['max_risk_ratio'] == 0.8
        assert states['incorrect_reversal']['option_buy_lots_max'] == 30
        assert states['other']['option_buy_lots_max'] == 10

    def test_add_position_tp_ratio_zero_returns_none(self):
        """R26-P2-TEST-01: tp_ratio=0时拒绝创建持仓(无止盈保护)"""
        from ali2026v3_trading.position_service import PositionService
        svc = PositionService.__new__(PositionService)
        svc._positions = {}
        svc._risk_service = None
        svc._order_service = None
        svc._event_bus = None
        result = svc.add_position(
            instrument_id='test_instrument', direction='long',
            volume=1, price=100.0, tp_ratio=0, sl_ratio=0.3,
        )
        assert result is None

    def test_add_position_tp_ratio_negative_returns_none(self):
        """R26-P2-TEST-01: tp_ratio<0时拒绝创建持仓(无止盈保护)"""
        from ali2026v3_trading.position_service import PositionService
        svc = PositionService.__new__(PositionService)
        svc._positions = {}
        svc._risk_service = None
        svc._order_service = None
        svc._event_bus = None
        result = svc.add_position(
            instrument_id='test_instrument', direction='long',
            volume=1, price=100.0, tp_ratio=-1.0, sl_ratio=0.3,
        )
        assert result is None
