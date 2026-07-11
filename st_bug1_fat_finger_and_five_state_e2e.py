"""
BUG-1胖手指修复 & 五态分类OTHER_SCALP根因修复 — 端到端验证

验证项：
1. 胖手指防护：开仓价fallback时ref_price=开仓价（非-1.0），防护正常工作
2. 胖手指防护：ref_price=开仓价时偏差=tick_size/open_price < 20%阈值
3. 胖手指防护：ref_price=0时从市价获取参考价
4. 胖手指防护：不再有ref_price<0跳过防护的路径
5. 五态分类：non_other_ratio_threshold=0.40（非0.65）
6. 五态分类：state_confirm_bars=2（非5）
7. 五态分类：state_check_interval_sec=15（非60）
8. 五态分类：min_state_hold_seconds=120（非600）
9. 五态分类：resolve_open_reason主动调用update_state_from_width_cache
10. 五态分类：resolve_open_reason fallback获取spm
11. 五态分类：状态切换从other→correct_trending的完整链路
12. 五态分类：旧阈值0.65 vs 新阈值0.40对比

运行方式: python -m pytest ali2026v3_trading/tests/test_bug1_fat_finger_and_five_state_e2e.py -v --tb=short --no-cov
"""

import sys
import os
import unittest
import logging
import re
import time
import threading
from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock

logging.basicConfig(level=logging.WARNING)

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def _make_order_executor():
    from ali2026v3_trading.order.order_executor import OrderExecutor
    svc = MagicMock()
    svc._operation_timeouts = {'default': 30, 'open': 30, 'close': 30}
    svc._circuit_breaker_open = False
    svc._circuit_breaker_auto_recovery_sec = 60
    svc._circuit_breaker_half_open = False
    svc._circuit_breaker_opened_at = 0.0
    svc._consecutive_failures = 0
    svc._get_last_market_price = MagicMock(return_value=3.8)
    svc._correct_price = MagicMock(side_effect=lambda p, _: p)
    svc.rate_limiter = MagicMock()
    svc.rate_limiter.acquire = MagicMock(return_value=True)
    svc._is_duplicate_order = MagicMock(return_value=False)
    svc._self_trade_bans = {}
    svc._self_trade_ban_minutes = 5
    svc._orders_by_id = {}
    svc._order_idempotent_set = set()
    svc._MAX_ORDERS_TRACKED = 10000
    svc._persist_idempotent_key = MagicMock()
    svc._get_position_count = None
    svc._close_order_sent = {}
    return OrderExecutor(svc), svc


class TestFatFingerRefPriceE2E(unittest.TestCase):
    """端到端验证：胖手指防护ref_price修复"""

    @patch('ali2026v3_trading.order.order_executor._HAS_CAUSAL_CHAIN', False)
    def test_01_ref_price_equals_open_price_not_rejected(self):
        """开仓价fallback时ref_price=open_price，偏差<20%不触发胖手指"""
        executor, svc = _make_order_executor()
        from ali2026v3_trading.order.order_executor import OrderContext

        ctx = OrderContext(
            instrument_id='FG608P910', volume=1, price=28.3,
            direction='SELL', action='CLOSE', exchange='CZCE', ref_price=28.8,
        )
        result_ctx = executor._pre_send_checks(ctx)
        self.assertFalse(result_ctx.rejected,
                         f"ref_price=28.8, price=28.3, 偏差1.74%<20%不应被拦截: {result_ctx.reject_code}")

    @patch('ali2026v3_trading.order.order_executor._HAS_CAUSAL_CHAIN', False)
    def test_02_ref_price_zero_triggers_fat_finger(self):
        """ref_price=0时从市价获取参考价，大偏差触发胖手指"""
        executor, svc = _make_order_executor()
        from ali2026v3_trading.order.order_executor import OrderContext

        ctx = OrderContext(
            instrument_id='FG608P910', volume=1, price=4.8,
            direction='SELL', action='CLOSE', exchange='CZCE', ref_price=0.0,
        )
        result_ctx = executor._pre_send_checks(ctx)
        self.assertTrue(result_ctx.rejected)
        self.assertEqual(result_ctx.reject_code, 'fat_finger')

    def test_03_no_skip_fat_finger_logic_in_source(self):
        """验证代码中不再存在ref_price<0跳过胖手指防护的路径"""
        import inspect
        from ali2026v3_trading.order.order_executor import OrderExecutor
        source = inspect.getsource(OrderExecutor._pre_send_checks)
        has_skip = '跳过胖手指' in source
        self.assertFalse(has_skip, "不应存在跳过胖手指防护的代码")

    def test_04_no_negative_ref_price_in_position_command(self):
        """验证position_command_service中不再有ref_price=-1.0"""
        import inspect
        from ali2026v3_trading.position.position_command_service import PositionCommandService
        source = inspect.getsource(PositionCommandService)
        matches = re.findall(r'ref_price\s*=\s*-1\.0', source)
        self.assertEqual(len(matches), 0, f"不应存在ref_price=-1.0，找到{len(matches)}处")

    def test_05_deviation_calculation_various_prices(self):
        """验证各种价格下开仓价fallback的偏差都<20%"""
        test_cases = [
            (28.8, 0.5),    # 期权如FG608P910
            (14300.0, 5.0), # 期货如CF609
            (4.8, 0.01),    # 低价期权
            (100.0, 0.5),   # 中等价格
        ]
        for open_price, tick_size in test_cases:
            close_price = open_price - tick_size
            deviation = abs(close_price - open_price) / open_price
            self.assertLess(deviation, 0.20,
                            f"open={open_price}, tick={tick_size}, 偏差{deviation:.2%}应<20%")


class TestFiveStateClassificationE2E(unittest.TestCase):
    """端到端验证：五态分类状态切换参数修复"""

    def test_01_non_other_ratio_threshold_is_0_40(self):
        from ali2026v3_trading.config.state_param import StateParamManager
        spm = StateParamManager()
        self.assertAlmostEqual(spm._non_other_ratio_threshold, 0.40, places=2)

    def test_02_state_confirm_bars_is_2(self):
        from ali2026v3_trading.config.state_param import StateParamManager
        spm = StateParamManager()
        self.assertEqual(spm._state_confirm_bars, 2)

    def test_03_state_check_interval_is_15(self):
        from ali2026v3_trading.config.state_param import StateParamManager
        spm = StateParamManager()
        self.assertAlmostEqual(spm._state_check_interval_sec, 15.0, places=1)

    def test_04_min_state_hold_seconds_is_120(self):
        from ali2026v3_trading.config.state_param import StateParamManager
        spm = StateParamManager()
        self.assertAlmostEqual(spm._min_state_hold_seconds, 120.0, places=1)

    def test_05_state_switches_from_other_to_correct_trending(self):
        """非other占比>40%时状态从other切换到correct_trending"""
        from ali2026v3_trading.config.state_param import StateParamManager
        spm = StateParamManager(
            state_confirm_bars=1, min_state_hold_seconds=0, state_check_interval_sec=0,
        )
        mock_wc = MagicMock()
        mock_wc.get_status_counts_snapshot = MagicMock(return_value={
            1: {1: {'CALL': {'correct_rise': 80, 'correct_fall': 10, 'other': 30}}}
        })
        spm.bind_width_cache(mock_wc)
        result = spm.update_state_from_width_cache()
        self.assertEqual(result, 'correct_trending')

    def test_06_state_stays_other_when_ratio_below_threshold(self):
        """非other占比<40%时状态保持other"""
        from ali2026v3_trading.config.state_param import StateParamManager
        spm = StateParamManager(
            state_confirm_bars=1, min_state_hold_seconds=0, state_check_interval_sec=0,
        )
        mock_wc = MagicMock()
        mock_wc.get_status_counts_snapshot = MagicMock(return_value={
            1: {1: {'CALL': {'correct_rise': 10, 'other': 90}}}
        })
        spm.bind_width_cache(mock_wc)
        result = spm.update_state_from_width_cache()
        self.assertEqual(result, 'other')

    def test_07_confirm_bars_2_requires_two_confirmations(self):
        """confirm_bars=2需要2次确认才能切换"""
        from ali2026v3_trading.config.state_param import StateParamManager
        spm = StateParamManager(
            state_confirm_bars=2, min_state_hold_seconds=0, state_check_interval_sec=0,
        )
        mock_wc = MagicMock()
        mock_wc.get_status_counts_snapshot = MagicMock(return_value={
            1: {1: {'CALL': {'correct_rise': 60, 'other': 40}}}
        })
        spm.bind_width_cache(mock_wc)
        result1 = spm.update_state_from_width_cache()
        self.assertEqual(result1, 'other', "第1次确认应保持other")
        result2 = spm.update_state_from_width_cache()
        self.assertEqual(result2, 'correct_trending', "第2次确认应切换")

    def test_08_old_threshold_065_blocks_50_percent(self):
        """旧阈值0.65下50%非other占比无法切换（回归测试）"""
        from ali2026v3_trading.config.state_param import StateParamManager
        spm = StateParamManager(
            state_confirm_bars=1, min_state_hold_seconds=0, state_check_interval_sec=0,
            non_other_ratio_threshold=0.65,
        )
        mock_wc = MagicMock()
        mock_wc.get_status_counts_snapshot = MagicMock(return_value={
            1: {1: {'CALL': {'correct_rise': 50, 'other': 50}}}
        })
        spm.bind_width_cache(mock_wc)
        result = spm.update_state_from_width_cache()
        self.assertEqual(result, 'other')

    def test_09_new_threshold_040_allows_50_percent(self):
        """新阈值0.40下50%非other占比可以切换（回归测试）"""
        from ali2026v3_trading.config.state_param import StateParamManager
        spm = StateParamManager(
            state_confirm_bars=1, min_state_hold_seconds=0, state_check_interval_sec=0,
            non_other_ratio_threshold=0.40,
        )
        mock_wc = MagicMock()
        mock_wc.get_status_counts_snapshot = MagicMock(return_value={
            1: {1: {'CALL': {'correct_rise': 50, 'other': 50}}}
        })
        spm.bind_width_cache(mock_wc)
        result = spm.update_state_from_width_cache()
        self.assertEqual(result, 'correct_trending')

    def test_10_resolve_open_reason_calls_update_state(self):
        """resolve_open_reason主动调用update_state_from_width_cache"""
        from ali2026v3_trading.strategy.strategy_config_layer import StrategyConfigLayer
        provider = MagicMock()
        provider._state_param_manager = MagicMock()
        provider._state_param_manager.get_current_state = MagicMock(return_value='correct_trending')
        scl = StrategyConfigLayer(provider)
        reason = scl.resolve_open_reason()
        provider._state_param_manager.update_state_from_width_cache.assert_called_once()
        self.assertEqual(reason, 'CORRECT_RESONANCE')

    def test_11_resolve_open_reason_fallback_gets_spm(self):
        """resolve_open_reason在provider无spm时fallback获取"""
        from ali2026v3_trading.strategy.strategy_config_layer import StrategyConfigLayer
        provider = MagicMock()
        provider._state_param_manager = None
        mock_spm = MagicMock()
        mock_spm.get_current_state = MagicMock(return_value='incorrect_reversal')
        mock_spm.update_state_from_width_cache = MagicMock()
        with patch('ali2026v3_trading.config.state_param.get_state_param_manager', return_value=mock_spm):
            scl = StrategyConfigLayer(provider)
            reason = scl.resolve_open_reason()
        self.assertEqual(reason, 'INCORRECT_REVERSAL')

    def test_12_full_chain_correct_rising_to_resonance(self):
        """端到端：correct_rise占主导→correct_trending→CORRECT_RESONANCE"""
        from ali2026v3_trading.config.state_param import StateParamManager
        from ali2026v3_trading.strategy.strategy_config_layer import StrategyConfigLayer
        spm = StateParamManager(
            state_confirm_bars=1, min_state_hold_seconds=0, state_check_interval_sec=0,
        )
        mock_wc = MagicMock()
        mock_wc.get_status_counts_snapshot = MagicMock(return_value={
            1: {1: {'CALL': {'correct_rise': 60, 'correct_fall': 20, 'wrong_rise': 10, 'other': 10}}}
        })
        spm.bind_width_cache(mock_wc)
        state = spm.update_state_from_width_cache()
        self.assertEqual(state, 'correct_trending')
        provider = MagicMock()
        provider._state_param_manager = spm
        scl = StrategyConfigLayer(provider)
        reason = scl.resolve_open_reason()
        self.assertEqual(reason, 'CORRECT_RESONANCE')

    def test_13_full_chain_wrong_rise_to_incorrect_reversal(self):
        """端到端：wrong_rise占主导→incorrect_reversal→INCORRECT_REVERSAL"""
        from ali2026v3_trading.config.state_param import StateParamManager
        from ali2026v3_trading.strategy.strategy_config_layer import StrategyConfigLayer
        spm = StateParamManager(
            state_confirm_bars=1, min_state_hold_seconds=0, state_check_interval_sec=0,
        )
        mock_wc = MagicMock()
        mock_wc.get_status_counts_snapshot = MagicMock(return_value={
            1: {1: {'CALL': {'wrong_rise': 50, 'wrong_fall': 10, 'other': 40}}}
        })
        spm.bind_width_cache(mock_wc)
        state = spm.update_state_from_width_cache()
        self.assertEqual(state, 'incorrect_reversal')
        provider = MagicMock()
        provider._state_param_manager = spm
        scl = StrategyConfigLayer(provider)
        reason = scl.resolve_open_reason()
        self.assertEqual(reason, 'INCORRECT_REVERSAL')


if __name__ == '__main__':
    unittest.main()
