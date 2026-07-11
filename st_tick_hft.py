# MODULE_ID: M2-596
"""测试 strategy/tick_hft.py — DynamicPursuitEngine"""
import time

import pytest


# ============================================================
# DynamicPursuitEngine — 开仓
# ============================================================

class TestDynamicPursuitEngineOpen:
    """DynamicPursuitEngine: 开仓"""

    def test_surge_detection_open_buy(self):
        """BUY方向强度突增开仓"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        result = engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        assert result is not None
        assert result['action'] == 'OPEN_POSITION'
        assert result['direction'] == 'BUY'
        assert result['instrument_id'] == 'test_inst'

    def test_surge_detection_open_sell(self):
        """SELL方向强度突增开仓"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        result = engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'SELL')
        assert result is not None
        assert result['action'] == 'OPEN_POSITION'
        assert result['direction'] == 'SELL'

    def test_no_surge_below_threshold(self):
        """强度增量低于阈值不触发"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        result = engine.evaluate_surge('test_inst', 0.4, 0.3, 100.0, 'BUY')
        assert result is None

    def test_invalid_direction_rejected(self):
        """无效方向被拒绝"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        result = engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'INVALID')
        assert result is None

    def test_zero_price_rejected(self):
        """价格为0被拒绝"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        result = engine.evaluate_surge('test_inst', 0.8, 0.3, 0.0, 'BUY')
        assert result is None

    def test_negative_price_rejected(self):
        """负价格被拒绝"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        result = engine.evaluate_surge('test_inst', 0.8, 0.3, -1.0, 'BUY')
        assert result is None

    def test_open_has_stop_profit(self):
        """开仓结果包含止盈价"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        result = engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        assert 'stop_profit' in result

    def test_open_has_strength_delta(self):
        """开仓结果包含强度增量"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        result = engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        assert 'strength_delta' in result
        assert abs(result['strength_delta'] - 0.5) < 1e-6

    def test_open_volume_is_one(self):
        """初始开仓数量为1"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        result = engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        assert result['volume'] == 1


# ============================================================
# DynamicPursuitEngine — 追仓
# ============================================================

class TestDynamicPursuitEngineAdd:
    """DynamicPursuitEngine: 追仓"""

    def test_add_position_on_surge(self):
        """已有仓位时追仓"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, max_add_positions=3, add_volume_ratio=0.5)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        result = engine.evaluate_surge('test_inst', 1.2, 0.8, 105.0, 'BUY')
        assert result is not None
        assert result['action'] == 'ADD_POSITION'
        assert result['volume'] >= 1

    def test_max_add_positions_limit(self):
        """追仓次数达到上限"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, max_add_positions=1, add_volume_ratio=0.5)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        engine.evaluate_surge('test_inst', 1.2, 0.8, 105.0, 'BUY')
        # 第二次追仓应被拒绝
        result = engine.evaluate_surge('test_inst', 1.6, 1.2, 110.0, 'BUY')
        assert result is None

    def test_opposite_direction_rejected(self):
        """已有仓位方向不同时拒绝"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        result = engine.evaluate_surge('test_inst', 1.2, 0.8, 105.0, 'SELL')
        assert result is None

    def test_add_updates_total_volume(self):
        """追仓更新总仓位"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, max_add_positions=3, add_volume_ratio=1.0)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        result = engine.evaluate_surge('test_inst', 1.2, 0.8, 105.0, 'BUY')
        assert result['total_volume'] > 1

    def test_add_updates_avg_price(self):
        """追仓更新加权平均价"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, max_add_positions=3, add_volume_ratio=1.0)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        result = engine.evaluate_surge('test_inst', 1.2, 0.8, 110.0, 'BUY')
        assert result['avg_price'] > 100.0
        assert result['avg_price'] < 110.0

    def test_add_has_new_stop_profit(self):
        """追仓结果包含新止盈价"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, max_add_positions=3, add_volume_ratio=0.5)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        result = engine.evaluate_surge('test_inst', 1.2, 0.8, 105.0, 'BUY')
        assert 'new_stop_profit' in result


# ============================================================
# DynamicPursuitEngine — 退出
# ============================================================

class TestDynamicPursuitEngineExit:
    """DynamicPursuitEngine: 退出"""

    def test_check_exit_take_profit_buy(self):
        """BUY仓位止盈退出"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, stop_profit_trail_ratio=0.3)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        pos = engine._positions['test_inst']
        pos.platform_confirmed = True
        pos.current_stop_profit = 100.5
        result = engine.check_exit('test_inst', 101.0)
        assert result is not None
        assert result['reason'] == 'pursuit_take_profit'

    def test_check_exit_stop_loss_buy(self):
        """BUY仓位止损退出"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, tight_stop_loss_pct=0.15)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        pos = engine._positions['test_inst']
        pos.platform_confirmed = True
        result = engine.check_exit('test_inst', 80.0)
        assert result is not None
        assert result['reason'] == 'pursuit_stop_loss'

    def test_check_exit_take_profit_sell(self):
        """SELL仓位止盈退出"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, stop_profit_trail_ratio=0.3)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'SELL')
        pos = engine._positions['test_inst']
        pos.platform_confirmed = True
        pos.current_stop_profit = 99.5
        result = engine.check_exit('test_inst', 99.0)
        assert result is not None
        assert result['reason'] == 'pursuit_take_profit'

    def test_check_exit_stop_loss_sell(self):
        """SELL仓位止损退出"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, tight_stop_loss_pct=0.15)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'SELL')
        pos = engine._positions['test_inst']
        pos.platform_confirmed = True
        result = engine.check_exit('test_inst', 120.0)
        assert result is not None
        assert result['reason'] == 'pursuit_stop_loss'

    def test_check_exit_no_position(self):
        """无仓位时返回None"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine()
        result = engine.check_exit('nonexistent', 100.0)
        assert result is None

    def test_check_exit_unconfirmed_short_wait(self):
        """未确认仓位短时间内不退出"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        # 未确认仓位，刚创建
        result = engine.check_exit('test_inst', 50.0)
        assert result is None

    def test_check_exit_result_has_pnl(self):
        """退出结果包含PnL"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, stop_profit_trail_ratio=0.3)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        pos = engine._positions['test_inst']
        pos.platform_confirmed = True
        pos.current_stop_profit = 100.5
        result = engine.check_exit('test_inst', 101.0)
        assert 'pnl' in result

    def test_check_exit_result_has_close_direction(self):
        """退出结果包含平仓方向"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, stop_profit_trail_ratio=0.3)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        pos = engine._positions['test_inst']
        pos.platform_confirmed = True
        pos.current_stop_profit = 100.5
        result = engine.check_exit('test_inst', 101.0)
        assert result['direction'] == 'SELL'  # BUY仓位的平仓方向是SELL


# ============================================================
# DynamicPursuitEngine — 追踪止盈
# ============================================================

class TestDynamicPursuitEngineTrailingStop:
    """DynamicPursuitEngine: 追踪止盈"""

    def test_update_trailing_stop_buy(self):
        """BUY仓位追踪止盈提高"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, stop_profit_trail_ratio=0.3)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        pos = engine._positions['test_inst']
        old_sp = pos.current_stop_profit
        new_sp = engine.update_trailing_stop('test_inst', 110.0)
        if new_sp is not None:
            assert new_sp > old_sp

    def test_update_trailing_stop_no_position(self):
        """无仓位时返回None"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine()
        result = engine.update_trailing_stop('nonexistent', 100.0)
        assert result is None

    def test_update_trailing_stop_buy_price_below_avg(self):
        """BUY仓位价格低于均价时不更新"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        result = engine.update_trailing_stop('test_inst', 99.0)
        assert result is None


# ============================================================
# DynamicPursuitEngine — 确认
# ============================================================

class TestDynamicPursuitEngineConfirm:
    """DynamicPursuitEngine: 确认"""

    def test_confirm_position_on_platform(self):
        """确认平台仓位"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        result = engine.confirm_position_on_platform('test_inst', 'order_123')
        assert result is True
        pos = engine._positions['test_inst']
        assert pos.platform_confirmed is True
        assert 'order_123' in pos.platform_order_ids

    def test_confirm_nonexistent_position(self):
        """确认不存在的仓位返回False"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine()
        result = engine.confirm_position_on_platform('nonexistent', 'order_123')
        assert result is False

    def test_add_platform_order_id(self):
        """添加平台订单ID"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        result = engine.add_platform_order_id('test_inst', 'order_456')
        assert result is True
        pos = engine._positions['test_inst']
        assert 'order_456' in pos.platform_order_ids


# ============================================================
# DynamicPursuitEngine — 统计
# ============================================================

class TestDynamicPursuitEngineStats:
    """DynamicPursuitEngine: 统计"""

    def test_get_stats_structure(self):
        """统计信息结构"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        stats = engine.get_stats()
        assert stats['service_name'] == 'DynamicPursuitEngine'
        assert stats['total_pursuit_entries'] == 1
        assert stats['surge_detected'] == 1
        assert stats['active_positions'] == 1

    def test_stats_after_close(self):
        """关闭仓位后统计更新"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine(surge_threshold=0.3, stop_profit_trail_ratio=0.3)
        engine.evaluate_surge('test_inst', 0.8, 0.3, 100.0, 'BUY')
        pos = engine._positions['test_inst']
        pos.platform_confirmed = True
        pos.current_stop_profit = 100.5
        engine.check_exit('test_inst', 101.0)
        stats = engine.get_stats()
        assert stats['positions_closed'] == 1
        assert stats['active_positions'] == 0

    def test_stats_no_entries(self):
        """无入场时统计"""
        from ali2026v3_trading.strategy.tick_hft import DynamicPursuitEngine
        engine = DynamicPursuitEngine()
        stats = engine.get_stats()
        assert stats['total_pursuit_entries'] == 0
        assert stats['surge_detected'] == 0
        assert stats['active_positions'] == 0


# ============================================================
# PursuitPosition
# ============================================================

class TestPursuitPosition:
    """PursuitPosition: 数据结构"""

    def test_creation(self):
        """创建PursuitPosition"""
        from ali2026v3_trading.strategy.tick_hft import PursuitPosition
        pos = PursuitPosition(
            position_id="test_001", instrument_id="test_inst",
            direction="BUY", entries=[], total_volume=1,
            weighted_avg_price=100.0, current_stop_profit=105.0,
            current_stop_loss=95.0, peak_strength=0.8,
        )
        assert pos.position_id == "test_001"
        assert pos.is_open is True
        assert pos.platform_confirmed is False
        assert pos.platform_order_ids == []

    def test_default_values(self):
        """默认值"""
        from ali2026v3_trading.strategy.tick_hft import PursuitPosition
        pos = PursuitPosition(
            position_id="test", instrument_id="test", direction="BUY",
            entries=[], total_volume=0, weighted_avg_price=0.0,
            current_stop_profit=0.0, current_stop_loss=0.0, peak_strength=0.0,
        )
        assert pos.is_open is True
        assert pos.platform_confirmed is False


# ============================================================
# PyramidAddPositionEngine
# ============================================================

class TestPyramidAddPositionEngine:
    """PyramidAddPositionEngine: 金字塔加仓"""

    def test_calc_add_volume_level0(self):
        """第0级加仓量"""
        from ali2026v3_trading.strategy.tick_hft import PyramidAddPositionEngine
        engine = PyramidAddPositionEngine(max_levels=4, pyramid_ratio=0.5)
        vol = engine.calc_add_volume('inst', 10, 0)
        assert vol == 10  # 10 * 0.5^0 = 10

    def test_calc_add_volume_level1(self):
        """第1级加仓量"""
        from ali2026v3_trading.strategy.tick_hft import PyramidAddPositionEngine
        engine = PyramidAddPositionEngine(max_levels=4, pyramid_ratio=0.5)
        vol = engine.calc_add_volume('inst', 10, 1)
        assert vol == 5  # 10 * 0.5^1 = 5

    def test_calc_add_volume_level2(self):
        """第2级加仓量"""
        from ali2026v3_trading.strategy.tick_hft import PyramidAddPositionEngine
        engine = PyramidAddPositionEngine(max_levels=4, pyramid_ratio=0.5)
        vol = engine.calc_add_volume('inst', 10, 2)
        assert vol == 2  # int(10 * 0.25) = 2

    def test_calc_add_volume_max_level(self):
        """超过最大级别返回0"""
        from ali2026v3_trading.strategy.tick_hft import PyramidAddPositionEngine
        engine = PyramidAddPositionEngine(max_levels=4, pyramid_ratio=0.5)
        vol = engine.calc_add_volume('inst', 10, 4)
        assert vol == 0

    def test_calc_add_volume_plr_blocked(self):
        """PLR过低阻止加仓"""
        from ali2026v3_trading.strategy.tick_hft import PyramidAddPositionEngine
        engine = PyramidAddPositionEngine(min_plr_for_add=1.5)
        vol = engine.calc_add_volume('inst', 10, 0, current_plr=1.0)
        assert vol == 0

    def test_get_stats(self):
        """统计信息"""
        from ali2026v3_trading.strategy.tick_hft import PyramidAddPositionEngine
        engine = PyramidAddPositionEngine()
        stats = engine.get_stats()
        assert stats['service_name'] == 'PyramidAddPositionEngine'
        assert 'total_adds' in stats
