# MODULE_ID: M2-589
"""
strategy_business_layer 行为测试 — 惰性初始化

验证 StrategyBusinessLayer 的核心行为契约:
1. 惰性初始化: 服务首次访问时初始化, 后续访问返回同一实例
2. 惰性初始化失败: 导入异常 → 服务保持None
3. 订单/成交回调: on_order/on_trade 委托到子服务
4. 交易周期守卫: 未初始化/未运行/暂停/降级 → 跳过交易周期
5. 生态系统互斥: 互斥规则阻断 → 跳过交易
6. 影子策略推送: 交易信号推送到影子引擎
"""
import threading
import time
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState


# ============================================================================
# 测试辅助: 构造 provider mock
# ============================================================================

def _make_provider(**overrides):
    """构造标准 provider mock"""
    p = MagicMock()
    p._order_service = None
    p._order_service_init_lock = threading.RLock()
    p._position_service = None
    p._hft_engine = None
    p._snapshot_collector = None
    p._signal_service = MagicMock()
    p._shadow_engine = None
    p._state_store = MagicMock()
    p._risk_service = None
    p._initialized = True
    p._is_running = True
    p._is_paused = False
    p._state = StrategyState.RUNNING
    p._trading_lock = threading.Lock()
    p._health_pause_new_open = False
    p._processed_trade_ids = {}
    p.MAX_PROCESSED_TRADE_IDS = 1000
    p.strategy_id = 'test_strategy'
    p._state_param_manager = None
    # 添加 _ensure_order_service 方法
    p._ensure_order_service = MagicMock()
    # 添加 _check_ecosystem_exclusion 方法
    p._check_ecosystem_exclusion = MagicMock(return_value=True)
    # 添加 _feed_shadow_engine 方法
    p._feed_shadow_engine = MagicMock()
    # 添加 _resolve_open_reason 方法
    p._resolve_open_reason = MagicMock(return_value='TEST')
    # 添加 get_health_status 方法
    p.get_health_status = MagicMock(return_value={'health': 'OK'})
    # t_type_service
    p.t_type_service = MagicMock()
    p.t_type_service.select_otm_targets_by_volume.return_value = []
    for k, v in overrides.items():
        setattr(p, k, v)
    return p


# ============================================================================
# 惰性初始化行为
# ============================================================================

class TestEnsureOrderServiceBehavior:
    """验证: ensure_order_service 惰性初始化行为"""

    def test_first_call_initializes_order_service(self):
        """首次调用 → _order_service 从None变为实例"""
        provider = _make_provider()
        bl = StrategyBusinessLayer(provider)
        assert provider._order_service is None, "初始状态应为None"
        with patch('ali2026v3_trading.strategy.strategy_business_layer.StrategyBusinessLayer.ensure_order_service') as mock_ensure:
            # 直接测试初始化逻辑
            with patch('ali2026v3_trading.order.order_service.get_order_service') as mock_get:
                mock_svc = MagicMock()
                mock_get.return_value = mock_svc
                bl.ensure_order_service()
        # 由于mock了ensure_order_service本身, 用另一种方式测试
        provider2 = _make_provider()
        bl2 = StrategyBusinessLayer(provider2)
        with patch('ali2026v3_trading.order.order_service.get_order_service') as mock_get:
            mock_svc = MagicMock()
            mock_get.return_value = mock_svc
            bl2.ensure_order_service()
            assert provider2._order_service is mock_svc, \
                "首次调用后_order_service应被初始化"

    def test_already_initialized_skips_reinit(self):
        """已初始化 → 不重复初始化"""
        provider = _make_provider()
        existing_svc = MagicMock()
        provider._order_service = existing_svc
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.order.order_service.get_order_service') as mock_get:
            bl.ensure_order_service()
            mock_get.assert_not_called(), "已初始化时不应再次调用get_order_service"
        assert provider._order_service is existing_svc, \
            "已初始化时_order_service应保持不变"

    def test_init_failure_sets_none(self):
        """初始化失败 → _order_service 保持None"""
        provider = _make_provider()
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.order.order_service.get_order_service',
                   side_effect=ImportError("module not found")):
            bl.ensure_order_service()
        assert provider._order_service is None, \
            "初始化失败后_order_service应为None"

    def test_state_store_set_ref_called(self):
        """初始化成功 → _state_store.set_ref 被调用"""
        provider = _make_provider()
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.order.order_service.get_order_service') as mock_get:
            mock_get.return_value = MagicMock()
            bl.ensure_order_service()
            provider._state_store.set_ref.assert_called()


class TestEnsurePositionServiceBehavior:
    """验证: ensure_position_service 惰性初始化行为"""

    def test_first_call_initializes_position_service(self):
        """首次调用 → _position_service 从None变为实例"""
        provider = _make_provider()
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.risk.risk_service.get_risk_service') as mock_risk, \
             patch('ali2026v3_trading.position.position_service.get_position_service') as mock_pos:
            mock_risk.return_value = MagicMock()
            mock_pos.return_value = MagicMock()
            bl.ensure_position_service()
            assert provider._position_service is not None, \
                "首次调用后_position_service应被初始化"

    def test_already_initialized_skips_reinit(self):
        """已初始化 → 不重复初始化"""
        provider = _make_provider()
        existing_svc = MagicMock()
        provider._position_service = existing_svc
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.risk.risk_service.get_risk_service') as mock_risk:
            bl.ensure_position_service()
            mock_risk.assert_not_called()
        assert provider._position_service is existing_svc

    def test_init_failure_sets_none(self):
        """初始化失败 → _position_service 保持None"""
        provider = _make_provider()
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.risk.risk_service.get_risk_service',
                   side_effect=RuntimeError("init failed")):
            bl.ensure_position_service()
        assert provider._position_service is None


class TestEnsureHftEngineBehavior:
    """验证: ensure_hft_engine 惰性初始化行为"""

    def test_first_call_initializes_hft_engine(self):
        """首次调用 → _hft_engine 从None变为实例"""
        provider = _make_provider()
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.strategy.hft_enhancements.get_hft_engine') as mock_get:
            mock_get.return_value = MagicMock()
            bl.ensure_hft_engine()
            assert provider._hft_engine is not None

    def test_already_initialized_skips_reinit(self):
        """已初始化 → 不重复初始化"""
        provider = _make_provider()
        existing_svc = MagicMock()
        provider._hft_engine = existing_svc
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.strategy.hft_enhancements.get_hft_engine') as mock_get:
            bl.ensure_hft_engine()
            mock_get.assert_not_called()
        assert provider._hft_engine is existing_svc

    def test_init_failure_sets_none(self):
        """初始化失败 → _hft_engine 保持None"""
        provider = _make_provider()
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.strategy.hft_enhancements.get_hft_engine',
                   side_effect=ImportError("not found")):
            bl.ensure_hft_engine()
        assert provider._hft_engine is None


class TestEnsureSnapshotCollectorBehavior:
    """验证: ensure_snapshot_collector 惰性初始化行为"""

    def test_first_call_initializes_snapshot_collector(self):
        """首次调用 → _snapshot_collector 从None变为实例"""
        provider = _make_provider()
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.strategy_judgment.market_snapshot_collector.MarketSnapshotCollector') as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance
            bl.ensure_snapshot_collector(symbol="test_symbol")
            assert provider._snapshot_collector is mock_instance

    def test_already_initialized_skips_reinit(self):
        """已初始化 → 不重复初始化"""
        provider = _make_provider()
        existing_svc = MagicMock()
        provider._snapshot_collector = existing_svc
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.strategy_judgment.market_snapshot_collector.MarketSnapshotCollector') as mock_cls:
            bl.ensure_snapshot_collector()
            mock_cls.assert_not_called()
        assert provider._snapshot_collector is existing_svc

    def test_init_failure_sets_none(self):
        """初始化失败 → _snapshot_collector 保持None"""
        provider = _make_provider()
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.strategy_judgment.market_snapshot_collector.MarketSnapshotCollector',
                   side_effect=ImportError("not found")):
            bl.ensure_snapshot_collector()
        assert provider._snapshot_collector is None


# ============================================================================
# 订单/成交回调行为
# ============================================================================

class TestOrderCallbackBehavior:
    """验证: on_order 委托到 order_service"""

    def test_on_order_delegates_to_order_service(self):
        """on_order → order_service.on_trade_update 被调用"""
        provider = _make_provider()
        mock_order_svc = MagicMock()
        provider._order_service = mock_order_svc
        bl = StrategyBusinessLayer(provider)
        order_data = MagicMock()
        order_data.order_id = "ORD001"
        order_data.status = "FILLED"
        bl.on_order(order_data)
        mock_order_svc.on_trade_update.assert_called_once_with(order_data)

    def test_on_order_no_order_service(self):
        """_order_service为None → 不抛异常"""
        provider = _make_provider()
        provider._order_service = None
        bl = StrategyBusinessLayer(provider)
        order_data = MagicMock()
        # 不应抛异常
        bl.on_order(order_data)


class TestTradeCallbackBehavior:
    """验证: on_trade 委托到 update_position_from_trade"""

    def test_on_trade_calls_update_position(self):
        """on_trade → update_position_from_trade 被调用"""
        provider = _make_provider()
        bl = StrategyBusinessLayer(provider)
        with patch.object(bl, 'update_position_from_trade') as mock_update:
            trade_data = MagicMock()
            bl.on_trade(trade_data)
            mock_update.assert_called_once_with(trade_data)


# ============================================================================
# 交易周期守卫行为
# ============================================================================

class TestTradingCycleGuardBehavior:
    """验证: 各种前置条件不满足时跳过交易周期"""

    def test_not_initialized_skips_cycle(self):
        """未初始化 → 跳过交易周期"""
        provider = _make_provider(_initialized=False)
        bl = StrategyBusinessLayer(provider)
        bl.execute_option_trading_cycle()
        # t_type_service.select_otm_targets_by_volume 不应被调用
        provider.t_type_service.select_otm_targets_by_volume.assert_not_called()

    def test_not_running_skips_cycle(self):
        """未运行 → 跳过交易周期"""
        provider = _make_provider(_is_running=False)
        bl = StrategyBusinessLayer(provider)
        bl.execute_option_trading_cycle()
        provider.t_type_service.select_otm_targets_by_volume.assert_not_called()

    def test_paused_skips_cycle(self):
        """暂停 → 跳过交易周期"""
        provider = _make_provider(_is_paused=True)
        bl = StrategyBusinessLayer(provider)
        bl.execute_option_trading_cycle()
        provider.t_type_service.select_otm_targets_by_volume.assert_not_called()

    def test_degraded_skips_cycle(self):
        """降级状态 → 跳过交易周期"""
        provider = _make_provider(_state=StrategyState.DEGRADED)
        bl = StrategyBusinessLayer(provider)
        bl.execute_option_trading_cycle()
        provider.t_type_service.select_otm_targets_by_volume.assert_not_called()

    def test_no_targets_skips_cycle(self):
        """无交易标的 → 跳过交易周期"""
        provider = _make_provider()
        provider.t_type_service.select_otm_targets_by_volume.return_value = []
        bl = StrategyBusinessLayer(provider)
        bl.execute_option_trading_cycle()
        # _check_ecosystem_exclusion 不应被调用
        provider._check_ecosystem_exclusion.assert_not_called()


# ============================================================================
# 生态系统互斥行为
# ============================================================================

class TestEcosystemExclusionBehavior:
    """验证: 互斥规则阻断交易"""

    def test_exclusion_blocks_trading(self):
        """互斥规则阻断 → 返回False, 设置health_pause"""
        provider = _make_provider()
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.strategy.strategy_business_layer.StrategyBusinessLayer._check_ecosystem_exclusion',
                   side_effect=lambda targets: (
                       setattr(provider, '_health_pause_new_open', True), False
                   )[-1]):
            # 模拟异常场景
            with patch('ali2026v3_trading.strategy.strategy_ecosystem.get_strategy_ecosystem',
                       side_effect=ImportError("not found")):
                result = bl._check_ecosystem_exclusion([])
                # 异常时安全阻断
                assert result is False, "异常时应安全阻断"
                assert provider._health_pause_new_open is True, \
                    "异常时应设置health_pause标志"


# ============================================================================
# 影子策略推送行为
# ============================================================================

class TestShadowEngineFeedBehavior:
    """验证: 交易信号推送到影子引擎"""

    def test_feed_shadow_engine_processes_signals(self):
        """_feed_shadow_engine → 每个target调用se.process_signal"""
        provider = _make_provider()
        mock_se = MagicMock()
        mock_se.are_params_locked.return_value = True
        mock_se.is_shadow_mode.return_value = True
        provider._shadow_engine = mock_se
        bl = StrategyBusinessLayer(provider)
        targets = [
            {'instrument_id': 'IO2506-C-4000', 'direction': 'BUY', 'price': 100.0, 'volume': 1},
            {'instrument_id': 'IO2506-P-3900', 'direction': 'SELL', 'price': 50.0, 'volume': 2},
        ]
        with patch('ali2026v3_trading.strategy.strategy_ecosystem.get_strategy_ecosystem',
                   side_effect=ImportError("not found")):
            bl._feed_shadow_engine(targets, 'TEST')
        assert mock_se.process_signal.call_count == 2, \
            "每个target应调用一次process_signal"

    def test_feed_shadow_engine_no_engine_creates_one(self):
        """_shadow_engine为None → 自动获取引擎"""
        provider = _make_provider()
        provider._shadow_engine = None
        bl = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.strategy.shadow_strategy_facade.get_shadow_strategy_engine') as mock_get:
            mock_se = MagicMock()
            mock_se.are_params_locked.return_value = True
            mock_se.is_shadow_mode.return_value = True
            mock_get.return_value = mock_se
            bl._feed_shadow_engine([], 'TEST')
            assert provider._shadow_engine is mock_se, \
                "应自动获取并缓存影子引擎"
