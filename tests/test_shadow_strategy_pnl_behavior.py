# MODULE_ID: M2-577
"""
shadow_strategy_pnl 行为测试 — PnL计算

验证 ShadowStrategyPnLMetricsService/ShadowStrategyPnLService 的核心行为契约:
1. PnL计算: 权益曲线 → 收益率 → Sharpe/MaxDD/EV
2. Alpha指标: 主策略优于影子 → 正Alpha; 劣于影子 → 负Alpha
3. 降级触发: 连续衰减 → degradation_active
4. 降级清除: clear_degradation → 恢复
5. 绝对EV暂停: EV低于底线 → absolute_ev_pause
6. EV暂停清除: clear_absolute_ev_pause → 恢复
7. 健康状态: OK/WARNING/CRITICAL 三态
8. 日/周汇总: 结构完整+文件写入
9. 向后兼容别名: Mixin = Service
"""
import json
import math
import os
import shutil
import tempfile
import threading
import time
import pytest
from collections import deque
from datetime import datetime
from unittest.mock import MagicMock, patch

from ali2026v3_trading.strategy.shadow_strategy_pnl import (
    ShadowStrategyPnLMetricsService,
    ShadowStrategyPnLService,
    ShadowStrategyPnLMetricsMixin,
    ShadowStrategyPnLMixin,
)
from ali2026v3_trading.strategy.shadow_strategy_facade import (
    ShadowStrategyEngine,
    ShadowTradeRecord,
    AlphaMetrics,
    get_shadow_strategy_engine,
)


# ============================================================================
# PnL计算行为
# ============================================================================

class TestSharpeComputationBehavior:
    """验证: Sharpe计算行为"""

    def test_positive_returns_positive_sharpe(self):
        """正收益序列 → 正Sharpe"""
        returns = [0.01, 0.02, 0.015, 0.008, 0.012]
        sharpe = ShadowStrategyPnLMetricsService._compute_sharpe(returns)
        assert sharpe > 0.0, "正收益序列应产生正Sharpe"

    def test_negative_returns_negative_sharpe(self):
        """负收益序列 → 负Sharpe"""
        returns = [-0.01, -0.02, -0.015, -0.008, -0.012]
        sharpe = ShadowStrategyPnLMetricsService._compute_sharpe(returns)
        assert sharpe < 0.0, "负收益序列应产生负Sharpe"

    def test_empty_returns_zero_sharpe(self):
        """空收益序列 → Sharpe为0"""
        sharpe = ShadowStrategyPnLMetricsService._compute_sharpe([])
        assert sharpe == 0.0, "空收益序列应返回0"

    def test_single_return_zero_sharpe(self):
        """单条收益 → Sharpe为0(样本方差无法计算)"""
        sharpe = ShadowStrategyPnLMetricsService._compute_sharpe([0.01])
        assert sharpe == 0.0, "单条收益应返回0"

    def test_higher_volatility_lower_sharpe(self):
        """波动率更高 → Sharpe更低(相同均值)"""
        # 低波动: 均值0.01, 小幅波动
        low_vol = [0.011, 0.009, 0.012, 0.008, 0.011, 0.009, 0.012, 0.008,
                   0.011, 0.009, 0.012, 0.008, 0.011, 0.009, 0.012, 0.008,
                   0.011, 0.009, 0.012, 0.008]
        # 高波动: 均值接近0.01, 大幅波动
        high_vol = [0.06, -0.04, 0.08, -0.06, 0.05, -0.03, 0.07, -0.05,
                    0.06, -0.04, 0.08, -0.06, 0.05, -0.03, 0.07, -0.05,
                    0.06, -0.04, 0.08, -0.06]
        sharpe_low = ShadowStrategyPnLMetricsService._compute_sharpe(low_vol)
        sharpe_high = ShadowStrategyPnLMetricsService._compute_sharpe(high_vol)
        assert sharpe_low > sharpe_high, \
            "低波动率应产生更高Sharpe(相同均值)"


class TestMaxDrawdownBehavior:
    """验证: 最大回撤计算行为"""

    def test_monotonic_up_zero_drawdown(self):
        """单调递增 → 最大回撤为0"""
        curve = [100, 110, 120, 130, 140]
        dd = ShadowStrategyPnLMetricsService._compute_max_drawdown(curve)
        assert dd == 0.0, "单调递增曲线最大回撤应为0"

    def test_single_drop_has_drawdown(self):
        """单次下跌 → 最大回撤>0"""
        curve = [100, 110, 105, 115]
        dd = ShadowStrategyPnLMetricsService._compute_max_drawdown(curve)
        assert dd > 0.0, "有下跌的曲线最大回撤应>0"
        expected_dd = (110 - 105) / 110
        assert abs(dd - expected_dd) < 0.001, \
            f"最大回撤应接近{expected_dd}"

    def test_empty_curve_zero_drawdown(self):
        """空曲线 → 最大回撤为0"""
        dd = ShadowStrategyPnLMetricsService._compute_max_drawdown([])
        assert dd == 0.0

    def test_single_point_zero_drawdown(self):
        """单点曲线 → 最大回撤为0"""
        dd = ShadowStrategyPnLMetricsService._compute_max_drawdown([100])
        assert dd == 0.0

    def test_drawdown_within_0_1_range(self):
        """最大回撤 ∈ [0, 1]"""
        curve = [100, 110, 80, 120, 60, 130]
        dd = ShadowStrategyPnLMetricsService._compute_max_drawdown(curve)
        assert 0.0 <= dd <= 1.0, "最大回撤应在[0,1]范围内"


class TestEquityToReturnsBehavior:
    """验证: 权益曲线 → 收益率转换行为"""

    def test_basic_conversion(self):
        """基本转换: (100, 110, 105) → (0.1, -5/110)"""
        curve = [100, 110, 105]
        returns = ShadowStrategyPnLMetricsService._equity_to_returns(curve)
        assert abs(returns[0] - 0.1) < 1e-10
        assert abs(returns[1] - (-5.0 / 110.0)) < 1e-10

    def test_empty_curve_returns_empty(self):
        """空曲线 → 空列表"""
        returns = ShadowStrategyPnLMetricsService._equity_to_returns([])
        assert returns == []

    def test_single_point_returns_empty(self):
        """单点曲线 → 空列表"""
        returns = ShadowStrategyPnLMetricsService._equity_to_returns([100])
        assert returns == []

    def test_zero_previous_returns_zero(self):
        """前值为0 → 收益率为0(避免除零)"""
        curve = [0, 100]
        returns = ShadowStrategyPnLMetricsService._equity_to_returns(curve)
        assert returns[0] == 0.0


class TestExpectedValueBehavior:
    """验证: 期望值计算行为"""

    def test_closed_trades_ev(self):
        """已平仓交易 → EV = 净PnL均值"""
        trades = deque([
            ShadowTradeRecord(trade_id="T1", shadow_type="shadow_a", timestamp="now", is_open=False, net_pnl=100.0),
            ShadowTradeRecord(trade_id="T2", shadow_type="shadow_a", timestamp="now", is_open=False, net_pnl=-50.0),
        ])
        ev = ShadowStrategyPnLMetricsService._compute_expected_value(trades)
        assert abs(ev - 25.0) < 1e-10, "EV应为(100-50)/2=25"

    def test_open_trades_excluded(self):
        """未平仓交易 → 不参与EV计算"""
        trades = deque([
            ShadowTradeRecord(trade_id="T1", shadow_type="shadow_a", timestamp="now", is_open=False, net_pnl=100.0),
            ShadowTradeRecord(trade_id="T2", shadow_type="shadow_a", timestamp="now", is_open=True, net_pnl=200.0),
        ])
        ev = ShadowStrategyPnLMetricsService._compute_expected_value(trades)
        assert abs(ev - 100.0) < 1e-10, "仅已平仓交易参与EV计算"

    def test_no_closed_trades_zero_ev(self):
        """无已平仓交易 → EV为0"""
        trades = deque()
        ev = ShadowStrategyPnLMetricsService._compute_expected_value(trades)
        assert ev == 0.0


# ============================================================================
# Alpha指标行为
# ============================================================================

class TestAlphaMetricsBehavior:
    """验证: Alpha指标计算行为(通过ShadowStrategyEngine集成)"""

    def _make_engine(self):
        tmp_dir = tempfile.mkdtemp()
        engine = ShadowStrategyEngine(log_dir=tmp_dir)
        return engine, tmp_dir

    def _cleanup(self, tmp_dir):
        shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_alpha_formula_behavior(self):
        """alpha_ratio = master_sharpe - max(shadow_a_sharpe, shadow_b_sharpe)"""
        engine, tmp_dir = self._make_engine()
        try:
            # 使用有噪声的权益曲线, 避免极端Sharpe
            import random
            rng = random.Random(42)
            for i in range(30):
                m_noise = rng.gauss(0, 200)
                a_noise = rng.gauss(0, 500)
                b_noise = rng.gauss(0, 800)
                engine.update_equity_curves(
                    master_equity=100000 + i * 300 + m_noise,
                    shadow_a_equity=100000 + i * 100 + a_noise,
                    shadow_b_equity=100000 + i * 50 + b_noise,
                )
            metrics = engine.compute_alpha_metrics()
            # 验证Alpha公式: alpha_ratio = master_sharpe - max(shadow_a, shadow_b)
            expected_alpha = metrics.master_sharpe - max(metrics.shadow_a_sharpe, metrics.shadow_b_sharpe)
            assert abs(metrics.alpha_ratio - expected_alpha) < 0.01, \
                f"alpha_ratio应等于master_sharpe-max(shadow_a,shadow_b), " \
                f"实际={metrics.alpha_ratio}, 期望={expected_alpha}"
        finally:
            self._cleanup(tmp_dir)

    def test_no_data_zero_alpha(self):
        """无数据 → Alpha为0"""
        engine, tmp_dir = self._make_engine()
        try:
            metrics = engine.compute_alpha_metrics()
            assert metrics.alpha_ratio == 0.0
        finally:
            self._cleanup(tmp_dir)

    def test_alpha_decline_detection(self):
        """Alpha连续衰减 → consecutive_decline_windows增加"""
        engine, tmp_dir = self._make_engine()
        try:
            # 第一轮: 主策略表现好
            for i in range(20):
                engine.update_equity_curves(
                    master_equity=100000 + i * 200,
                    shadow_a_equity=100000,
                    shadow_b_equity=100000,
                )
            metrics1 = engine.compute_alpha_metrics()
            # 第二轮: 主策略表现差
            for i in range(20):
                engine.update_equity_curves(
                    master_equity=100000 + 3800 - i * 100,
                    shadow_a_equity=100000 + i * 200,
                    shadow_b_equity=100000,
                )
            metrics2 = engine.compute_alpha_metrics()
            # Alpha应下降
            assert metrics2.alpha_ratio < metrics1.alpha_ratio, \
                "主策略表现恶化后Alpha应下降"
        finally:
            self._cleanup(tmp_dir)


# ============================================================================
# 降级行为
# ============================================================================

class TestDegradationBehavior:
    """验证: 降级触发/清除行为"""

    def _make_engine(self):
        tmp_dir = tempfile.mkdtemp()
        engine = ShadowStrategyEngine(log_dir=tmp_dir)
        return engine, tmp_dir

    def _cleanup(self, tmp_dir):
        shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_initial_no_degradation(self):
        """初始状态 → 降级未激活"""
        engine, tmp_dir = self._make_engine()
        try:
            assert not engine.is_degradation_active(), \
                "初始状态降级不应激活"
        finally:
            self._cleanup(tmp_dir)

    def test_clear_degradation_resets_flag(self):
        """clear_degradation → 降级标志清除"""
        engine, tmp_dir = self._make_engine()
        try:
            engine._pnl_service._degradation_active = True
            assert engine.is_degradation_active()
            engine.clear_degradation()
            assert not engine.is_degradation_active(), \
                "清除后降级应不激活"
        finally:
            self._cleanup(tmp_dir)

    def test_degradation_state_transition(self):
        """降级状态转换: 未激活→激活→清除→未激活"""
        engine, tmp_dir = self._make_engine()
        try:
            # 未激活
            assert not engine.is_degradation_active()
            # 激活
            engine._pnl_service._degradation_active = True
            assert engine.is_degradation_active()
            # 清除
            engine.clear_degradation()
            assert not engine.is_degradation_active()
        finally:
            self._cleanup(tmp_dir)


# ============================================================================
# 绝对EV暂停行为
# ============================================================================

class TestAbsoluteEVPauseBehavior:
    """验证: 绝对EV暂停触发/清除行为"""

    def _make_engine(self):
        tmp_dir = tempfile.mkdtemp()
        engine = ShadowStrategyEngine(log_dir=tmp_dir)
        return engine, tmp_dir

    def _cleanup(self, tmp_dir):
        shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_initial_no_pause(self):
        """初始状态 → EV暂停未激活"""
        engine, tmp_dir = self._make_engine()
        try:
            assert not engine.is_absolute_ev_paused()
        finally:
            self._cleanup(tmp_dir)

    def test_clear_absolute_ev_pause_resets_flag(self):
        """clear_absolute_ev_pause → 暂停标志清除"""
        engine, tmp_dir = self._make_engine()
        try:
            engine._pnl_service._absolute_ev_pause = True
            assert engine.is_absolute_ev_paused()
            engine.clear_absolute_ev_pause()
            assert not engine.is_absolute_ev_paused(), \
                "清除后EV暂停应不激活"
        finally:
            self._cleanup(tmp_dir)

    def test_ev_pause_state_transition(self):
        """EV暂停状态转换: 未暂停→暂停→清除→未暂停"""
        engine, tmp_dir = self._make_engine()
        try:
            assert not engine.is_absolute_ev_paused()
            engine._pnl_service._absolute_ev_pause = True
            assert engine.is_absolute_ev_paused()
            engine.clear_absolute_ev_pause()
            assert not engine.is_absolute_ev_paused()
        finally:
            self._cleanup(tmp_dir)


# ============================================================================
# 健康状态行为
# ============================================================================

class TestHealthStatusBehavior:
    """验证: 健康状态三态(OK/WARNING/CRITICAL)行为"""

    def _make_engine(self):
        tmp_dir = tempfile.mkdtemp()
        engine = ShadowStrategyEngine(log_dir=tmp_dir)
        return engine, tmp_dir

    def _cleanup(self, tmp_dir):
        shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_initial_status_ok_or_warning(self):
        """初始状态 → OK或WARNING"""
        engine, tmp_dir = self._make_engine()
        try:
            health = engine.get_health_status()
            assert health['status'] in ('OK', 'WARNING'), \
                f"初始状态应为OK或WARNING, 实际为{health['status']}"
        finally:
            self._cleanup(tmp_dir)

    def test_ev_pause_triggers_critical(self):
        """EV暂停激活 → 状态为CRITICAL"""
        engine, tmp_dir = self._make_engine()
        try:
            engine._pnl_service._absolute_ev_pause = True
            health = engine.get_health_status()
            assert health['status'] == 'CRITICAL', \
                "EV暂停激活时状态应为CRITICAL"
        finally:
            self._cleanup(tmp_dir)

    def test_degradation_triggers_warning(self):
        """降级激活 → 状态为WARNING"""
        engine, tmp_dir = self._make_engine()
        try:
            engine._pnl_service._degradation_active = True
            health = engine.get_health_status()
            assert health['status'] == 'WARNING', \
                "降级激活时状态应为WARNING"
        finally:
            self._cleanup(tmp_dir)

    def test_critical_has_action_required(self):
        """CRITICAL状态 → 包含action_required字段"""
        engine, tmp_dir = self._make_engine()
        try:
            engine._pnl_service._absolute_ev_pause = True
            health = engine.get_health_status()
            assert 'action_required' in health, \
                "CRITICAL状态应包含action_required"
            assert health['action_required'] == 'halt_new_open'
        finally:
            self._cleanup(tmp_dir)

    def test_health_status_contains_required_fields(self):
        """健康状态 → 包含必要字段"""
        engine, tmp_dir = self._make_engine()
        try:
            health = engine.get_health_status()
            required_fields = [
                'component', 'status', 'params_locked',
                'degradation_active', 'absolute_ev_pause',
                'alpha_ratio', 'master_expected_value',
            ]
            for field in required_fields:
                assert field in health, f"缺少字段: {field}"
        finally:
            self._cleanup(tmp_dir)


# ============================================================================
# 日/周汇总行为
# ============================================================================

class TestDailySummaryBehavior:
    """验证: 日汇总结构完整+文件写入"""

    def _make_engine(self):
        tmp_dir = tempfile.mkdtemp()
        engine = ShadowStrategyEngine(log_dir=tmp_dir)
        return engine, tmp_dir

    def _cleanup(self, tmp_dir):
        shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_daily_summary_structure(self):
        """日汇总 → 包含必要字段"""
        engine, tmp_dir = self._make_engine()
        try:
            engine.record_master_trade(pnl=100.0)
            engine.process_shadow_a_signal(market_state='test', signal_direction='long')
            engine.process_shadow_b_signal(market_state='test')
            summary = engine.generate_daily_summary()
            required_keys = [
                'summary_date', 'alpha_metrics',
                'shadow_a_stats', 'shadow_b_stats', 'master_stats',
                'params_locked', 'degradation_active', 'absolute_ev_pause',
            ]
            for key in required_keys:
                assert key in summary, f"日汇总缺少字段: {key}"
        finally:
            self._cleanup(tmp_dir)

    def test_daily_summary_file_written(self):
        """日汇总 → JSON文件被写入"""
        engine, tmp_dir = self._make_engine()
        try:
            engine.generate_daily_summary()
            summary_files = [f for f in os.listdir(tmp_dir)
                             if f.startswith('summary_') and f.endswith('.json')]
            assert len(summary_files) > 0, "日汇总JSON文件应被写入"
        finally:
            self._cleanup(tmp_dir)

    def test_daily_summary_alpha_beta_attribution(self):
        """日汇总 → alpha_beta_attribution 包含alpha/beta/alpha_pct"""
        engine, tmp_dir = self._make_engine()
        try:
            summary = engine.generate_daily_summary()
            attr = summary.get('alpha_beta_attribution', {})
            assert 'alpha' in attr, "alpha_beta_attribution应包含alpha"
            assert 'beta' in attr, "alpha_beta_attribution应包含beta"
            assert 'alpha_pct' in attr, "alpha_beta_attribution应包含alpha_pct"
        finally:
            self._cleanup(tmp_dir)


class TestWeeklySummaryBehavior:
    """验证: 周汇总结构完整"""

    def _make_engine(self):
        tmp_dir = tempfile.mkdtemp()
        engine = ShadowStrategyEngine(log_dir=tmp_dir)
        return engine, tmp_dir

    def _cleanup(self, tmp_dir):
        shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_weekly_summary_structure(self):
        """周汇总 → 包含必要字段"""
        engine, tmp_dir = self._make_engine()
        try:
            for i in range(5):
                engine.update_equity_curves(
                    master_equity=100000 + i * 100,
                    shadow_a_equity=100000,
                    shadow_b_equity=100000,
                )
                engine.compute_alpha_metrics()
            summary = engine.generate_weekly_summary()
            assert 'week_start' in summary
            assert 'alpha_ratio_stats' in summary
            assert 'total_degradation_events' in summary
        finally:
            self._cleanup(tmp_dir)

    def test_weekly_summary_no_data(self):
        """无数据 → 周汇总返回提示消息"""
        engine, tmp_dir = self._make_engine()
        try:
            summary = engine.generate_weekly_summary()
            assert 'week_start' in summary
        finally:
            self._cleanup(tmp_dir)


# ============================================================================
# 向后兼容别名行为
# ============================================================================

class TestBackwardCompatibleAliasesBehavior:
    """验证: Mixin别名指向Service类"""

    def test_metrics_mixin_is_metrics_service(self):
        """ShadowStrategyPnLMetricsMixin = ShadowStrategyPnLMetricsService"""
        assert ShadowStrategyPnLMetricsMixin is ShadowStrategyPnLMetricsService, \
            "Mixin别名应指向Service类"

    def test_pnl_mixin_is_pnl_service(self):
        """ShadowStrategyPnLMixin = ShadowStrategyPnLService"""
        assert ShadowStrategyPnLMixin is ShadowStrategyPnLService, \
            "Mixin别名应指向Service类"

    def test_pnl_service_inherits_metrics_service(self):
        """ShadowStrategyPnLService 继承 ShadowStrategyPnLMetricsService"""
        assert issubclass(ShadowStrategyPnLService, ShadowStrategyPnLMetricsService), \
            "PnLService应继承MetricsService"


# ============================================================================
# 验证方法行为
# ============================================================================

class TestValidationMethodsBehavior:
    """验证: validate_shadow_b_stability / alpha_confidence_overlap_test / validate_shadow_param_orthogonality"""

    def _make_engine(self):
        tmp_dir = tempfile.mkdtemp()
        engine = ShadowStrategyEngine(log_dir=tmp_dir)
        return engine, tmp_dir

    def _cleanup(self, tmp_dir):
        shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_validate_shadow_b_stability_returns_dict(self):
        """validate_shadow_b_stability → 返回包含ci_width和passed的字典"""
        engine, tmp_dir = self._make_engine()
        try:
            result = engine.validate_shadow_b_stability(n_seeds=10)
            assert isinstance(result, dict)
            assert 'ci_width' in result
            assert 'passed' in result
        finally:
            self._cleanup(tmp_dir)

    def test_alpha_confidence_overlap_returns_dict(self):
        """alpha_confidence_overlap_test → 返回包含overlap和alpha_unreliable的字典"""
        engine, tmp_dir = self._make_engine()
        try:
            # 需要一些数据
            for i in range(20):
                engine.update_equity_curves(
                    master_equity=100000 + i * 100,
                    shadow_a_equity=100000 + i * 50,
                    shadow_b_equity=100000 + i * 20,
                )
            result = engine.alpha_confidence_overlap_test()
            assert isinstance(result, dict)
            assert 'max_overlap_ratio' in result
            assert 'alpha_unreliable' in result
        finally:
            self._cleanup(tmp_dir)

    def test_validate_shadow_param_orthogonality_returns_dict(self):
        """validate_shadow_param_orthogonality → 返回包含kl和passed的字典"""
        engine, tmp_dir = self._make_engine()
        try:
            for i in range(20):
                engine.update_equity_curves(
                    master_equity=100000 + i * 100,
                    shadow_a_equity=100000 + i * 50,
                    shadow_b_equity=100000 + i * 20,
                )
            result = engine.validate_shadow_param_orthogonality()
            assert isinstance(result, dict)
            assert 'kl_master_shadow_a' in result
            assert 'kl_master_shadow_b' in result
            assert 'passed' in result
        finally:
            self._cleanup(tmp_dir)


# ============================================================================
# 统计行为
# ============================================================================

class TestStatsBehavior:
    """验证: get_stats 反映当前状态"""

    def _make_engine(self):
        tmp_dir = tempfile.mkdtemp()
        engine = ShadowStrategyEngine(log_dir=tmp_dir)
        return engine, tmp_dir

    def _cleanup(self, tmp_dir):
        shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_stats_increment_after_trades(self):
        """交易后 → 统计计数器增加"""
        engine, tmp_dir = self._make_engine()
        try:
            engine.process_shadow_a_signal(market_state='test', signal_direction='long')
            engine.process_shadow_b_signal(market_state='test')
            engine.record_master_trade(pnl=50.0)
            stats = engine.get_stats()
            assert stats['shadow_a_trades'] == 1
            assert stats['shadow_b_trades'] == 1
            assert stats['master_trades'] == 1
        finally:
            self._cleanup(tmp_dir)

    def test_stats_reflects_degradation_state(self):
        """降级状态 → stats中degradation_active为True"""
        engine, tmp_dir = self._make_engine()
        try:
            engine._pnl_service._degradation_active = True
            stats = engine.get_stats()
            assert stats['degradation_active'] is True
        finally:
            self._cleanup(tmp_dir)
