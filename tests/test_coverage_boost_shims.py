# MODULE_ID: M2-324
"""
补充测试：DEPRECATED re-export shim + 验证已达80%覆盖
覆盖模块: _box_spring_types, shadow_strategy_types, strategy_core_service_builder
"""
import pytest


class TestBoxSpringTypesReexport:
    def test_spring_state_importable(self):
        from ali2026v3_trading.strategy._box_spring_types import SpringState
        assert SpringState is not None

    def test_box_range_importable(self):
        from ali2026v3_trading.strategy._box_spring_types import BoxRange
        assert BoxRange is not None

    def test_spring_signal_importable(self):
        from ali2026v3_trading.strategy._box_spring_types import SpringSignal
        assert SpringSignal is not None

    def test_spring_position_importable(self):
        from ali2026v3_trading.strategy._box_spring_types import SpringPosition
        assert SpringPosition is not None

    def test_pending_pullback_importable(self):
        from ali2026v3_trading.strategy._box_spring_types import PendingPullback
        assert PendingPullback is not None

    def test_get_box_spring_strategy_importable(self):
        from ali2026v3_trading.strategy._box_spring_types import get_box_spring_strategy
        assert callable(get_box_spring_strategy)

    def test_reexport_matches_source(self):
        from ali2026v3_trading.strategy._box_spring_types import SpringState as SS_shim
        from ali2026v3_trading.strategy.box_spring_detector import SpringState as SS_src
        assert SS_shim is SS_src


class TestShadowStrategyTypesReexport:
    def test_shadow_trade_record_importable(self):
        from ali2026v3_trading.strategy.shadow_strategy_types import ShadowTradeRecord
        assert ShadowTradeRecord is not None

    def test_alpha_metrics_importable(self):
        from ali2026v3_trading.strategy.shadow_strategy_types import AlphaMetrics
        assert AlphaMetrics is not None

    def test_shadow_params_snapshot_importable(self):
        from ali2026v3_trading.strategy.shadow_strategy_types import ShadowParamsSnapshot
        assert ShadowParamsSnapshot is not None

    def test_reexport_matches_source(self):
        from ali2026v3_trading.strategy.shadow_strategy_types import ShadowTradeRecord as STR_shim
        from ali2026v3_trading.strategy.shadow_strategy_core import ShadowTradeRecord as STR_src
        assert STR_shim is STR_src

    def test_shadow_trade_record_via_shim(self):
        from ali2026v3_trading.strategy.shadow_strategy_types import ShadowTradeRecord
        rec = ShadowTradeRecord(trade_id='t1', shadow_type='shadow_a', timestamp='2026-01-01')
        assert rec.trade_id == 't1'
        assert rec.is_open is True


class TestStrategyCoreServiceBuilderReexport:
    def test_builder_importable(self):
        from ali2026v3_trading.strategy.strategy_core_service_builder import StrategyCoreServiceBuilder
        assert StrategyCoreServiceBuilder is not None

    def test_reexport_matches_source(self):
        from ali2026v3_trading.strategy.strategy_core_service_builder import StrategyCoreServiceBuilder as B_shim
        from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreServiceBuilder as B_src
        assert B_shim is B_src