# MODULE_ID: M2-597
"""测试 infra/trading_utils.py — compute_commission + classify_deviation + ThreadSafeCounter"""
import math
import threading
import time
from datetime import datetime

import pytest


# ============================================================
# compute_commission
# ============================================================

class TestComputeCommission:
    """compute_commission: 佣金计算"""

    # --- 基础合约类型 ---

    def test_50etf_option_open(self):
        """50ETF期权开仓"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("50ETF购6月2700", 10, is_open=True) == 3.0 * 10

    def test_50etf_option_close_overnight(self):
        """50ETF期权隔夜平仓"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("50ETF购6月2700", 10, is_open=False) == 3.0 * 10

    def test_300etf_option_open(self):
        """300ETF期权开仓"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("300ETF沽6月4000", 5, is_open=True) == 3.0 * 5

    def test_io_index_option(self):
        """IO指数期权"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("IO2506-C-4000", 2, is_open=True) == 15.0 * 2

    def test_mo_index_option(self):
        """MO指数期权"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("MO2506-P-6000", 3, is_open=True) == 15.0 * 3

    def test_commodity_option(self):
        """商品期权"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("M2609-C-3000", 3, is_open=True) == 5.0 * 3

    def test_default_contract_type(self):
        """未知合约类型使用DEFAULT费率"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("UNKNOWN_SYMBOL", 1, is_open=True) == 15.0

    # --- V2费率(带exchange参数) ---

    def test_v2_sse_50etf_taker_open(self):
        """V2: SSE 50ETF taker开仓"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("50ETF购6月2700", 10, is_open=True, exchange="SSE", order_type="taker") == 3.0 * 10

    def test_v2_sse_50etf_maker_open(self):
        """V2: SSE 50ETF maker开仓"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("50ETF购6月2700", 10, is_open=True, exchange="SSE", order_type="maker") == 1.5 * 10

    def test_v2_sse_close_today_zero(self):
        """V2: SSE close_today为0"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        now = datetime.now()
        assert compute_commission(
            "50ETF购6月2700", 10, is_open=False,
            open_time=now, close_time=now,
            exchange="SSE", order_type="taker"
        ) == 0.0

    def test_v2_cffex_io_taker_open(self):
        """V2: CFFEX IO taker开仓"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("IO2506-C-4000", 2, is_open=True, exchange="CFFEX", order_type="taker") == 15.0 * 2

    def test_v2_cffex_io_maker_open(self):
        """V2: CFFEX IO maker开仓"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("IO2506-C-4000", 2, is_open=True, exchange="CFFEX", order_type="maker") == 10.0 * 2

    def test_v2_dce_commodity_taker_open(self):
        """V2: DCE 商品期权 taker开仓"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("M2609-C-3000", 3, is_open=True, exchange="DCE", order_type="taker") == 5.0 * 3

    def test_v2_dce_commodity_maker_open(self):
        """V2: DCE 商品期权 maker开仓"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("M2609-C-3000", 3, is_open=True, exchange="DCE", order_type="maker") == 3.0 * 3

    def test_v2_default_exchange(self):
        """V2: 未知交易所使用DEFAULT费率"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        assert compute_commission("UNKNOWN", 1, is_open=True, exchange="UNKNOWN_EXCHANGE", order_type="taker") == 15.0

    # --- V1平今/隔夜逻辑 ---

    def test_v1_close_today_short_hold(self):
        """V1: 持仓<4小时按close_today"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        open_t = datetime(2026, 1, 1, 9, 0, 0)
        close_t = datetime(2026, 1, 1, 10, 0, 0)  # 1小时
        fee = compute_commission("50ETF购6月2700", 10, is_open=False, open_time=open_t, close_time=close_t)
        assert fee == 0.0 * 10  # close_today=0.0

    def test_v1_close_overnight_long_hold(self):
        """V1: 持仓>=4小时按close_overnight"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        open_t = datetime(2026, 1, 1, 9, 0, 0)
        close_t = datetime(2026, 1, 1, 14, 0, 0)  # 5小时
        fee = compute_commission("50ETF购6月2700", 10, is_open=False, open_time=open_t, close_time=close_t)
        assert fee == 3.0 * 10  # close_overnight=3.0

    def test_v1_close_no_times_defaults_overnight(self):
        """V1: 无时间参数默认按close_overnight"""
        from ali2026v3_trading.infra.trading_utils import compute_commission
        fee = compute_commission("50ETF购6月2700", 10, is_open=False)
        assert fee == 3.0 * 10

    # --- 辅助函数 ---

    def test_get_commission_per_lot(self):
        """get_commission_per_lot返回每手开仓费"""
        from ali2026v3_trading.infra.trading_utils import get_commission_per_lot
        assert get_commission_per_lot("50ETF购6月2700") == 3.0
        assert get_commission_per_lot("IO2506-C-4000") == 15.0

    def test_estimate_commission_simple(self):
        """estimate_commission_simple按比率估算"""
        from ali2026v3_trading.infra.trading_utils import estimate_commission_simple
        fee = estimate_commission_simple(100.0, 10, rate=0.00003)
        assert abs(fee - 0.03) < 1e-6

    def test_calc_trade_fee(self):
        """calc_trade_fee计算开+平总费用"""
        from ali2026v3_trading.infra.trading_utils import calc_trade_fee
        open_t = datetime(2026, 1, 1, 9, 0, 0)
        close_t = datetime(2026, 1, 1, 14, 0, 0)
        fee = calc_trade_fee("50ETF_OPTION", open_t, close_t, 10)
        assert fee == 3.0 * 10 + 3.0 * 10  # open + close_overnight


# ============================================================
# classify_deviation
# ============================================================

class TestClassifyDeviation:
    """classify_deviation: 偏差分类"""

    def test_both_none_is_match(self):
        """两侧均无信号 => MATCH"""
        from ali2026v3_trading.infra.trading_utils import classify_deviation
        result = classify_deviation(None, None)
        assert result.category == 'MATCH'

    def test_identical_signals_match(self):
        """完全一致 => MATCH"""
        from ali2026v3_trading.infra.trading_utils import classify_deviation, Signal
        s = Signal(timestamp=1.0, direction='BUY', price=100.0)
        result = classify_deviation(s, s)
        assert result.category == 'MATCH'

    def test_direction_diff_is_critical(self):
        """方向不同 => CRITICAL_DEVIATION"""
        from ali2026v3_trading.infra.trading_utils import classify_deviation, Signal
        b = Signal(timestamp=1.0, direction='BUY', price=100.0)
        a = Signal(timestamp=1.0, direction='SELL', price=100.0)
        result = classify_deviation(b, a)
        assert result.category == 'CRITICAL_DEVIATION'
        assert '方向性差异' in result.description

    def test_float_noise_is_numerical_noise(self):
        """极小价格差(浮点噪声) => NUMERICAL_NOISE"""
        from ali2026v3_trading.infra.trading_utils import classify_deviation, Signal
        b = Signal(timestamp=1.0, direction='BUY', price=100.0)
        a = Signal(timestamp=1.0, direction='BUY', price=100.0 + 1e-5)
        result = classify_deviation(b, a, noise_tol=1e-6, acceptable_diff=0.01)
        assert result.category == 'NUMERICAL_NOISE'

    def test_small_price_diff_is_numerical_noise(self):
        """微小价格差(可接受) => NUMERICAL_NOISE"""
        from ali2026v3_trading.infra.trading_utils import classify_deviation, Signal
        b = Signal(timestamp=1.0, direction='BUY', price=100.0)
        a = Signal(timestamp=1.0, direction='BUY', price=100.005)
        result = classify_deviation(b, a, acceptable_diff=0.01)
        assert result.category == 'NUMERICAL_NOISE'

    def test_large_price_diff_needs_review(self):
        """较大价格差 => NEEDS_REVIEW"""
        from ali2026v3_trading.infra.trading_utils import classify_deviation, Signal
        b = Signal(timestamp=1.0, direction='BUY', price=100.0)
        a = Signal(timestamp=1.0, direction='BUY', price=105.0)
        result = classify_deviation(b, a, acceptable_diff=0.01)
        assert result.category == 'NEEDS_REVIEW'

    def test_baseline_only_is_correct_deviation(self):
        """baseline有actual无 => CORRECT_DEVIATION"""
        from ali2026v3_trading.infra.trading_utils import classify_deviation, Signal
        b = Signal(timestamp=1.0, direction='BUY', price=100.0)
        result = classify_deviation(b, None)
        assert result.category == 'CORRECT_DEVIATION'

    def test_actual_only_needs_review(self):
        """baseline无actual有 => NEEDS_REVIEW"""
        from ali2026v3_trading.infra.trading_utils import classify_deviation, Signal
        a = Signal(timestamp=1.0, direction='BUY', price=100.0)
        result = classify_deviation(None, a)
        assert result.category == 'NEEDS_REVIEW'

    def test_result_has_baseline_and_actual(self):
        """结果包含baseline和actual引用"""
        from ali2026v3_trading.infra.trading_utils import classify_deviation, Signal
        b = Signal(timestamp=1.0, direction='BUY', price=100.0)
        a = Signal(timestamp=1.0, direction='SELL', price=100.0)
        result = classify_deviation(b, a)
        assert result.baseline is b
        assert result.actual is a


class TestClassifySignalSequence:
    """classify_signal_sequence: 批量分类"""

    def test_length_mismatch_returns_error(self):
        """序列长度不一致 => 错误"""
        from ali2026v3_trading.infra.trading_utils import classify_signal_sequence, Signal
        result = classify_signal_sequence(
            [Signal(timestamp=1.0, direction='BUY', price=100.0)],
            [],
        )
        assert result['total'] == -1
        assert result['pass_pa06'] is False

    def test_all_match_passes_pa06(self):
        """全部MATCH => 通过PA-06"""
        from ali2026v3_trading.infra.trading_utils import classify_signal_sequence, Signal
        signals = [Signal(timestamp=1.0, direction='BUY', price=100.0)]
        result = classify_signal_sequence(signals, signals)
        assert result['pass_pa06'] is True
        assert result['category_counts']['MATCH'] == 1

    def test_critical_deviation_fails_pa06(self):
        """有CRITICAL_DEVIATION => 不通过PA-06"""
        from ali2026v3_trading.infra.trading_utils import classify_signal_sequence, Signal
        baseline = [Signal(timestamp=1.0, direction='BUY', price=100.0)]
        actual = [Signal(timestamp=1.0, direction='SELL', price=100.0)]
        result = classify_signal_sequence(baseline, actual)
        assert result['pass_pa06'] is False
        assert len(result['critical_deviations']) == 1


# ============================================================
# ThreadSafeCounter
# ============================================================

class TestThreadSafeCounter:
    """ThreadSafeCounter: 线程安全计数器"""

    def test_initial_value(self):
        """初始值"""
        from ali2026v3_trading.infra.trading_utils import ThreadSafeCounter
        c = ThreadSafeCounter(42.0)
        assert c.get() == 42.0

    def test_default_initial_value(self):
        """默认初始值为0"""
        from ali2026v3_trading.infra.trading_utils import ThreadSafeCounter
        c = ThreadSafeCounter()
        assert c.get() == 0.0

    def test_add_returns_new_value(self):
        """add返回新值"""
        from ali2026v3_trading.infra.trading_utils import ThreadSafeCounter
        c = ThreadSafeCounter(0.0)
        result = c.add(5.0)
        assert result == 5.0
        assert c.get() == 5.0

    def test_add_negative(self):
        """add负数"""
        from ali2026v3_trading.infra.trading_utils import ThreadSafeCounter
        c = ThreadSafeCounter(10.0)
        c.add(-3.0)
        assert c.get() == 7.0

    def test_set_value(self):
        """set设置值"""
        from ali2026v3_trading.infra.trading_utils import ThreadSafeCounter
        c = ThreadSafeCounter(0.0)
        c.set(42.0)
        assert c.get() == 42.0

    def test_concurrent_adds_no_loss(self):
        """并发add不丢失"""
        from ali2026v3_trading.infra.trading_utils import ThreadSafeCounter
        c = ThreadSafeCounter(0.0)
        n = 100
        barrier = threading.Barrier(n)

        def add_one():
            barrier.wait()
            c.add(1.0)

        threads = [threading.Thread(target=add_one) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert c.get() == float(n)

    def test_concurrent_set_and_add(self):
        """并发set和add不崩溃"""
        from ali2026v3_trading.infra.trading_utils import ThreadSafeCounter
        c = ThreadSafeCounter(0.0)

        def do_ops():
            for _ in range(100):
                c.add(1.0)
                c.set(0.0)

        threads = [threading.Thread(target=do_ops) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 只要没崩溃就算通过


# ============================================================
# ThreadSafeDict
# ============================================================

class TestThreadSafeDict:
    """ThreadSafeDict: 线程安全字典"""

    def test_set_and_get(self):
        """set+get"""
        from ali2026v3_trading.infra.trading_utils import ThreadSafeDict
        d = ThreadSafeDict()
        d.set('key', 'value')
        assert d.get('key') == 'value'

    def test_get_default(self):
        """get不存在的key返回默认值"""
        from ali2026v3_trading.infra.trading_utils import ThreadSafeDict
        d = ThreadSafeDict()
        assert d.get('missing') is None
        assert d.get('missing', 42) == 42

    def test_delete(self):
        """delete删除key"""
        from ali2026v3_trading.infra.trading_utils import ThreadSafeDict
        d = ThreadSafeDict()
        d.set('key', 'value')
        d.delete('key')
        assert d.get('key') is None

    def test_items_and_keys(self):
        """items和keys返回快照"""
        from ali2026v3_trading.infra.trading_utils import ThreadSafeDict
        d = ThreadSafeDict()
        d.set('a', 1)
        d.set('b', 2)
        assert sorted(d.keys()) == ['a', 'b']
        assert sorted(d.items()) == [('a', 1), ('b', 2)]


# ============================================================
# 其他工具函数
# ============================================================

class TestOtherUtilities:
    """其他工具函数"""

    def test_safe_equity_update_normal(self):
        """safe_equity_update正常更新"""
        from ali2026v3_trading.infra.trading_utils import safe_equity_update
        assert safe_equity_update(1000.0, 50.0) == 1050.0

    def test_safe_equity_update_nan_returns_current(self):
        """safe_equity_update NaN返回当前值"""
        from ali2026v3_trading.infra.trading_utils import safe_equity_update
        result = safe_equity_update(1000.0, float('nan'))
        assert result == 1000.0

    def test_safe_equity_update_inf_returns_current(self):
        """safe_equity_update inf返回当前值"""
        from ali2026v3_trading.infra.trading_utils import safe_equity_update
        result = safe_equity_update(1000.0, float('inf'))
        assert result == 1000.0

    def test_safe_equity_update_tiny_pnl_ignored(self):
        """safe_equity_update极小PnL忽略"""
        from ali2026v3_trading.infra.trading_utils import safe_equity_update
        result = safe_equity_update(1e8, 1e-12)
        assert result == 1e8

    def test_unified_slippage_model_buy(self):
        """UnifiedSlippageModel BUY方向滑点"""
        from ali2026v3_trading.infra.trading_utils import UnifiedSlippageModel
        model = UnifiedSlippageModel(base_bps=2.0, impact_factor=0.05)
        price = model.estimate_price(100.0, 100, 'BUY')
        assert price > 100.0

    def test_unified_slippage_model_sell(self):
        """UnifiedSlippageModel SELL方向滑点"""
        from ali2026v3_trading.infra.trading_utils import UnifiedSlippageModel
        model = UnifiedSlippageModel(base_bps=2.0, impact_factor=0.05)
        price = model.estimate_price(100.0, 100, 'SELL')
        assert price < 100.0

    def test_unified_slippage_model_zero_price(self):
        """UnifiedSlippageModel零价格返回0"""
        from ali2026v3_trading.infra.trading_utils import UnifiedSlippageModel
        model = UnifiedSlippageModel()
        assert model.estimate_price(0.0, 100, 'BUY') == 0.0

    def test_unified_position_sizer(self):
        """UnifiedPositionSizer仓位计算"""
        from ali2026v3_trading.infra.trading_utils import UnifiedPositionSizer
        sizer = UnifiedPositionSizer(max_risk_ratio=0.2)
        lots = sizer.size(equity=100000.0, risk_per_lot=1000.0)
        assert lots == 20  # 100000*0.2/1000

    def test_unified_position_sizer_zero_equity(self):
        """UnifiedPositionSizer零权益返回最小"""
        from ali2026v3_trading.infra.trading_utils import UnifiedPositionSizer
        sizer = UnifiedPositionSizer()
        assert sizer.size(0.0, 100.0) == 1

    def test_backtest_order_splitter(self):
        """BacktestOrderSplitter拆单"""
        from ali2026v3_trading.infra.trading_utils import BacktestOrderSplitter
        splitter = BacktestOrderSplitter(max_lots_per_child=10)
        chunks = splitter.split(25)
        assert chunks == [10, 10, 5]

    def test_backtest_order_splitter_zero(self):
        """BacktestOrderSplitter零拆单"""
        from ali2026v3_trading.infra.trading_utils import BacktestOrderSplitter
        splitter = BacktestOrderSplitter()
        assert splitter.split(0) == [0]

    def test_perf_monitor(self):
        """perf_monitor上下文管理器"""
        from ali2026v3_trading.infra.trading_utils import perf_monitor, get_perf_stats
        with perf_monitor('test_op', warn_threshold_ms=10000):
            x = 1 + 1
        stats = get_perf_stats()
        assert 'test_op' in stats
        assert stats['test_op']['count'] == 1

    def test_safe_execute_normal(self):
        """safe_execute正常执行"""
        from ali2026v3_trading.infra.trading_utils import safe_execute
        result = safe_execute(lambda: 42)
        assert result == 42

    def test_safe_execute_exception_returns_default(self):
        """safe_execute异常返回默认值"""
        from ali2026v3_trading.infra.trading_utils import safe_execute
        result = safe_execute(lambda: int('abc'), default=-1)
        assert result == -1
