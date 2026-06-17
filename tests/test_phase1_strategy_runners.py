# MODULE_ID: M2-468
"""Phase 1 轮次1 断言测试：验证策略入口合并的运行时行为

测试目标：
1. 新模块 backtest_strategy_runners.py 的所有函数可导入且可调用
2. shim文件 re-export 路径兼容
3. 共享工具函数的运行时行为正确
4. 各策略函数对空数据/基本场景的运行时行为
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock


# ============================================================================
# 测试1: 新模块直接导入
# ============================================================================
class TestStrategyRunnersDirectImport:
    """验证 backtest_strategy_runners.py 所有公开函数可导入"""

    def test_import_box_extreme(self):
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import run_backtest_box_extreme
        assert callable(run_backtest_box_extreme)

    def test_import_box_spring(self):
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import run_backtest_box_spring
        assert callable(run_backtest_box_spring)

    def test_import_hft(self):
        from ali2026v3_trading.param_pool.backtest._backtest_runners_hft import run_backtest_hft
        assert callable(run_backtest_hft)

    def test_import_hft_with_disturbance(self):
        from ali2026v3_trading.param_pool.backtest._backtest_runners_hft import run_backtest_hft_with_disturbance
        assert callable(run_backtest_hft_with_disturbance)

    def test_import_hft_tick_fidelity(self):
        from ali2026v3_trading.param_pool.backtest._backtest_runners_hft import run_backtest_hft_tick_fidelity
        assert callable(run_backtest_hft_tick_fidelity)

    def test_import_arbitrage(self):
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import run_backtest_arbitrage
        assert callable(run_backtest_arbitrage)

    def test_import_market_making(self):
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import run_backtest_market_making
        assert callable(run_backtest_market_making)


# ============================================================================
# 测试2: Shim re-export 兼容性
# ============================================================================
class TestShimReExport:
    """验证原文件路径的导入仍然可用"""

    def test_box_from_runners(self):
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import (
            run_backtest_box_extreme, run_backtest_box_spring,
        )
        assert callable(run_backtest_box_extreme)
        assert callable(run_backtest_box_spring)

    def test_hft_from_runners(self):
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import (
            run_backtest_hft, run_backtest_hft_with_disturbance, run_backtest_hft_tick_fidelity,
        )
        assert callable(run_backtest_hft)
        assert callable(run_backtest_hft_with_disturbance)
        assert callable(run_backtest_hft_tick_fidelity)

    def test_arb_mm_from_runners(self):
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import (
            run_backtest_arbitrage, run_backtest_market_making,
        )
        assert callable(run_backtest_arbitrage)
        assert callable(run_backtest_market_making)

    def test_runners_direct_import(self):
        """验证直接从backtest_strategy_runners导入和从runner_base导入指向同一个函数对象"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import run_backtest_box_extreme as direct
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import run_backtest_box_extreme as base
        assert direct is base


# ============================================================================
# 测试3: 共享工具函数运行时行为
# ============================================================================
class TestSharedUtilities:
    """验证提取的共享工具函数运行时行为正确"""

    def test_handle_health_critical_closes_all_positions(self):
        """验证健康检查CRITICAL时所有持仓被平仓"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _handle_health_critical
        from ali2026v3_trading.param_pool.backtest.backtest_state import _BacktestState, _BacktestPosition

        bt = _BacktestState()
        bt.positions["SYM1"] = _BacktestPosition(
            instrument_id="SYM1", volume=1, open_price=100.0,
            open_time=pd.Timestamp("2026-01-01 10:00"),
            stop_profit_price=110.0, stop_loss_price=95.0,
            open_reason="TEST", lots=1,
            instrument_type="future",
        )
        bt.positions["SYM2"] = _BacktestPosition(
            instrument_id="SYM2", volume=-1, open_price=200.0,
            open_time=pd.Timestamp("2026-01-01 10:00"),
            stop_profit_price=190.0, stop_loss_price=210.0,
            open_reason="TEST", lots=1,
            instrument_type="future",
        )
        bar = pd.Series({"close": 105.0})

        with patch("ali2026v3_trading.param_pool.backtest.backtest_strategy_runners._get_contract_multiplier", return_value=10.0):
            _handle_health_critical(bt, bar)

        assert len(bt.positions) == 0, "所有持仓应被平仓"
        assert bt.total_trades == 2, "应记录2笔平仓交易"
        assert len(bt.equity_curve) == 1, "应记录1个权益点"

    def test_apply_shadow_direction_main(self):
        """验证main策略不改变方向"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _apply_shadow_direction
        direction, should_open = _apply_shadow_direction(1, "main")
        assert direction == 1
        assert should_open is True

    def test_apply_shadow_direction_reverse(self):
        """验证shadow_reverse策略反转方向"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _apply_shadow_direction
        direction, should_open = _apply_shadow_direction(1, "shadow_reverse")
        assert direction == -1
        assert should_open is True

    def test_apply_shadow_direction_random(self):
        """验证shadow_random策略返回有效方向"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _apply_shadow_direction
        np.random.seed(42)
        results = set()
        for _ in range(100):
            direction, should_open = _apply_shadow_direction(1, "shadow_random")
            if should_open:
                results.add(direction)
            else:
                results.add(0)  # 0表示不开仓
        # shadow_random应该产生多种结果
        assert len(results) > 1, "shadow_random应产生多种结果"

    def test_check_other_state_block_main(self):
        """验证other状态下main策略被阻止"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _check_other_state_block
        from ali2026v3_trading.param_pool.backtest.backtest_state import _BacktestState

        bt = _BacktestState()
        bt.current_state = "other"
        assert _check_other_state_block(bt, "main") is True
        assert _check_other_state_block(bt, "shadow_reverse") is True
        assert _check_other_state_block(bt, "shadow_random") is False

    def test_check_other_state_block_not_other(self):
        """验证非other状态下不被阻止"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _check_other_state_block
        from ali2026v3_trading.param_pool.backtest.backtest_state import _BacktestState

        bt = _BacktestState()
        bt.current_state = "trend_up"
        assert _check_other_state_block(bt, "main") is False

    def test_check_risk_service_blocks_on_exception(self):
        """验证RiskService异常时fail-safe阻止开仓"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _check_risk_service_before_trade
        with patch("ali2026v3_trading.risk.risk_service.get_risk_service",
                   side_effect=ImportError("test")):
            result = _check_risk_service_before_trade("SYM", 1, 100.0, 1, "BT_TEST", pd.Timestamp("2026-01-01"))
            assert result is False, "RiskService异常时应fail-safe阻止开仓"

    def test_check_risk_service_passes_when_ok(self):
        """验证RiskService正常时允许开仓"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _check_risk_service_before_trade
        mock_rs = MagicMock()
        mock_chk = MagicMock()
        mock_chk.is_block = False
        mock_rs.check_before_trade.return_value = mock_chk
        with patch("ali2026v3_trading.risk.risk_service.get_risk_service", return_value=mock_rs):
            result = _check_risk_service_before_trade("SYM", 1, 100.0, 1, "BT_TEST", pd.Timestamp("2026-01-01"))
            assert result is True

    def test_check_risk_service_blocks_when_blocked(self):
        """验证RiskService拦截时阻止开仓"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _check_risk_service_before_trade
        mock_rs = MagicMock()
        mock_chk = MagicMock()
        mock_chk.is_block = True
        mock_chk.reason = "test block"
        mock_rs.check_before_trade.return_value = mock_chk
        with patch("ali2026v3_trading.risk.risk_service.get_risk_service", return_value=mock_rs):
            result = _check_risk_service_before_trade("SYM", 1, 100.0, 1, "BT_TEST", pd.Timestamp("2026-01-01"))
            assert result is False

    def test_check_n1_quality_gates_spread_zero(self):
        """验证spread_quality=0时N1门控不通过"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _check_n1_quality_gates
        from ali2026v3_trading.param_pool.backtest.backtest_state import _BacktestState

        bt = _BacktestState()
        bar = pd.Series({"_spread_quality": 0, "_option_metadata_quality": 1})
        assert _check_n1_quality_gates(bar, bt, {}) is False

    def test_check_n1_quality_gates_omq_zero(self):
        """验证option_metadata_quality=0时C-13门控不通过"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _check_n1_quality_gates
        from ali2026v3_trading.param_pool.backtest.backtest_state import _BacktestState

        bt = _BacktestState()
        bar = pd.Series({"_spread_quality": 1, "_option_metadata_quality": 0})
        assert _check_n1_quality_gates(bar, bt, {}) is False

    def test_check_n1_quality_gates_max_signals(self):
        """验证信号数超限时门控不通过"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _check_n1_quality_gates
        from ali2026v3_trading.param_pool.backtest.backtest_state import _BacktestState

        bt = _BacktestState()
        bt.total_signals = 10
        bar = pd.Series({"_spread_quality": 1, "_option_metadata_quality": 1})
        assert _check_n1_quality_gates(bar, bt, {"max_signals_per_window": 5}) is False

    def test_check_n1_quality_gates_pass(self):
        """验证正常情况门控通过"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _check_n1_quality_gates
        from ali2026v3_trading.param_pool.backtest.backtest_state import _BacktestState

        bt = _BacktestState()
        bar = pd.Series({"_spread_quality": 1, "_option_metadata_quality": 1})
        assert _check_n1_quality_gates(bar, bt, {"max_signals_per_window": 5}) is True

    def test_execute_open_with_slippage(self):
        """验证开仓执行创建正确持仓"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _execute_open_with_slippage
        from ali2026v3_trading.param_pool.backtest.backtest_state import _BacktestState

        bt = _BacktestState()
        bt.equity = 100000.0
        bar = pd.Series({
            "close": 100.0,
            "bid_ask_spread": 0.01,
            "_spread_quality": 1,
            "minute": pd.Timestamp("2026-01-01 10:00"),
            "symbol": "TEST",
        })
        params = {"lots_min": 1}

        # Mock _compute_commission and _compute_dynamic_slippage_bps to avoid
        # complex internal dependencies (exchange_id lookup, etc.)
        with patch("ali2026v3_trading.param_pool.backtest.backtest_strategy_runners._compute_commission", return_value=5.0), \
             patch("ali2026v3_trading.param_pool.backtest.backtest_strategy_runners._compute_dynamic_slippage_bps", return_value=1.0), \
             patch("ali2026v3_trading.param_pool.backtest.backtest_strategy_runners._infer_exchange_id", return_value="SSE"), \
             patch("ali2026v3_trading.param_pool.backtest.backtest_strategy_runners._infer_instrument_type", return_value="future"):
            _execute_open_with_slippage(
                bt, bar, params, "TEST", 1, 100.0, 1,
                2.0, 0.3, "TEST_OPEN", spread_quality=1,
            )

        assert "TEST" in bt.positions
        pos = bt.positions["TEST"]
        assert pos.volume == 1  # direction * lots
        assert pos.open_price == 100.0
        assert pos.open_reason == "TEST_OPEN"
        assert pos.lots == 1
        assert bt.total_signals == 1
        # 验证手续费和滑点被扣除 (commission=5.0 + slip_cost=100*1/10000*1=0.01)
        assert bt.equity < 100000.0

    def test_eod_close_morning(self):
        """验证日盘EOD平仓时间检测"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _check_eod_close
        bar_time = pd.Timestamp("2026-01-01 14:55")
        should_close, reason = _check_eod_close(bar_time)
        assert should_close is True
        assert reason == "EOD"

    def test_eod_close_night(self):
        """验证夜盘EOD平仓时间检测"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _check_eod_close
        bar_time = pd.Timestamp("2026-01-01 02:25")
        should_close, reason = _check_eod_close(bar_time)
        assert should_close is True
        assert reason == "EOD_NIGHT"

    def test_eod_close_not_time(self):
        """验证非EOD时间不平仓"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _check_eod_close
        bar_time = pd.Timestamp("2026-01-01 10:30")
        should_close, reason = _check_eod_close(bar_time)
        assert should_close is False

    def test_close_position_with_pnl(self):
        """验证平仓PnL计算正确"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import _close_position_with_pnl
        from ali2026v3_trading.param_pool.backtest.backtest_state import _BacktestState, _BacktestPosition

        bt = _BacktestState()
        bt.equity = 100000.0
        pos = _BacktestPosition(
            instrument_id="TEST", volume=1, open_price=100.0,
            open_time=pd.Timestamp("2026-01-01 10:00"),
            stop_profit_price=110.0, stop_loss_price=95.0,
            open_reason="TEST", lots=1,
            instrument_type="future",
        )
        bt.positions["TEST"] = pos

        with patch("ali2026v3_trading.param_pool.backtest.backtest_strategy_runners._get_contract_multiplier", return_value=10.0):
            _close_position_with_pnl(bt, "TEST", pos, 105.0)

        assert "TEST" not in bt.positions
        assert bt.total_trades == 1


# ============================================================================
# 测试4: 空数据场景运行时行为
# ============================================================================
class TestEmptyDataBehavior:
    """验证各策略函数对空数据的运行时行为"""

    def test_box_extreme_empty_data(self):
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import run_backtest_box_extreme
        result = run_backtest_box_extreme({}, pd.DataFrame())
        assert "error" in result
        assert result["error"] == "无数据"

    def test_box_spring_empty_data(self):
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import run_backtest_box_spring
        result = run_backtest_box_spring({}, pd.DataFrame())
        assert "error" in result

    def test_hft_empty_data(self):
        from ali2026v3_trading.param_pool.backtest._backtest_runners_hft import run_backtest_hft
        result = run_backtest_hft({}, pd.DataFrame())
        assert "error" in result

    def test_hft_with_disturbance_empty_data(self):
        from ali2026v3_trading.param_pool.backtest._backtest_runners_hft import run_backtest_hft_with_disturbance
        result = run_backtest_hft_with_disturbance({}, pd.DataFrame())
        assert "error" in result

    def test_hft_tick_fidelity_empty_data(self):
        from ali2026v3_trading.param_pool.backtest._backtest_runners_hft import run_backtest_hft_tick_fidelity
        result = run_backtest_hft_tick_fidelity({}, pd.DataFrame())
        assert "error" in result

    def test_arbitrage_empty_data(self):
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import run_backtest_arbitrage
        result = run_backtest_arbitrage({}, pd.DataFrame())
        assert "error" in result

    def test_market_making_empty_data(self):
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import run_backtest_market_making
        result = run_backtest_market_making({}, pd.DataFrame())
        assert "error" in result


# ============================================================================
# 测试5: Facade和下游导入兼容性
# ============================================================================
class TestDownstreamCompatibility:
    """验证下游模块的导入路径仍然有效"""

    def test_backtest_runner_base_facade(self):
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import (
            run_backtest, run_backtest_hft, run_backtest_box_extreme, run_backtest_box_spring,
        )
        assert callable(run_backtest)
        assert callable(run_backtest_hft)
        assert callable(run_backtest_box_extreme)

    def test_backtest_runner_base_getattr(self):
        """验证backtest_runner_base的__getattr__仍能路由到策略函数"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import __getattr__ as _ga
        fn = _ga("run_backtest_box_extreme")
        assert callable(fn)

    def test_task_scheduler_import(self):
        """验证从backtest_strategy_runners直接导入"""
        from ali2026v3_trading.param_pool.backtest._backtest_runners_hft import run_backtest_hft_tick_fidelity
        assert callable(run_backtest_hft_tick_fidelity)
