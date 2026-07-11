# MODULE_ID: M2-554
"""
test_resonance_order.py - 共振策略下单测试脚本

测试链路：
  CycleResonanceModule.update()
    → _compute_directional_bias() 方向偏置
    → _compute_resonance_strength() 共振强度
    → _compute_phase() 相位判定
    → get_risk_surface('resonance') 风险曲面调节
    → SignalService.generate_signal() 信号生成
    → OrderService.send_order() 实际下单

验证目标：
  1. 周期共振模块正确计算四变量输出
  2. 释放期+高共振强度 → 大仓位下单
  3. 混沌期 → 降级保护，小仓位下单
  4. 风险曲面参数正确传递到下单逻辑
  5. 组合约束和熔断机制正常工作
  6. 信号→下单全链路通畅
"""
import os
import sys
import time
import unittest
from datetime import datetime
from typing import Any, Dict, Tuple
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.param_pool.optimization.cycle_sharpe import (
    CycleResonanceModule,
    CycleResonanceOutput,
    RiskSurfaceAdjustment,
    Phase,
    get_cycle_resonance_module,
    reset_cycle_resonance_module,
)
from ali2026v3_trading.signal.signal_service import SignalService
from ali2026v3_trading.order.order_service import OrderService, get_order_service


def _reset_order_service_singleton():
    import ali2026v3_trading.order.order_service as mod
    mod._order_service_instance = None


def _reset_global_singletons():
    _reset_order_service_singleton()
    reset_cycle_resonance_module()
    import ali2026v3_trading.position.position_service as ps_mod
    ps_mod._cross_strategy_risk_guard = None
    ps_mod._position_service_instance = None
    from ali2026v3_trading.governance.mode_engine import ModeEngine
    ModeEngine.reset_instance()


class TestCycleResonanceOutput(unittest.TestCase):
    """共振策略：四变量输出计算"""

    def setUp(self):
        self.crm = CycleResonanceModule(
            n_periods=3,
            hmm_entropy_window=20,
            phase_transition_threshold=0.3,
            chaos_entropy_threshold=0.7,
        )

    def test_release_phase_with_strong_resonance(self):
        output = self.crm.update(
            hmm_state='NORMAL',
            hmm_posterior=(0.1, 0.8, 0.1),
            trend_scores=(0.7, 0.8, 0.6),
            trend_directions=(1.0, 1.0, 1.0),
            strength=0.85,
            imbalance=0.3,
        )
        self.assertGreater(output.resonance_strength, 0.3)
        self.assertGreater(abs(output.directional_bias), 0.0)
        self.assertIn(output.phase, [Phase.RELEASE, Phase.CHARGE])

    def test_chaos_phase_with_high_entropy(self):
        for i in range(25):
            state = 'LOW_VOL' if i % 2 == 0 else 'HIGH_VOL'
            self.crm.update(
                hmm_state=state,
                hmm_posterior=(0.4, 0.2, 0.4),
                trend_scores=(0.1, -0.1, 0.05),
                trend_directions=(1.0, -1.0, 0.0),
                strength=0.2,
                imbalance=0.0,
            )
        output = self.crm.update(
            hmm_state='HIGH_VOL',
            hmm_posterior=(0.1, 0.1, 0.8),
            trend_scores=(0.1, -0.1, 0.05),
            trend_directions=(1.0, -1.0, 0.0),
            strength=0.15,
            imbalance=0.0,
        )
        self.assertGreater(output.state_entropy, 0.5)

    def test_charge_phase_low_vol(self):
        output = self.crm.update(
            hmm_state='LOW_VOL',
            hmm_posterior=(0.7, 0.2, 0.1),
            trend_scores=(0.1, 0.05, 0.02),
            trend_directions=(0.0, 0.0, 0.0),
            strength=0.15,
            imbalance=0.0,
        )
        self.assertIn(output.phase, [Phase.CHARGE, Phase.EXHAUST])

    def test_directional_bias_with_trend_alignment(self):
        output = self.crm.update(
            hmm_state='NORMAL',
            hmm_posterior=(0.1, 0.8, 0.1),
            trend_scores=(0.8, 0.9, 0.7),
            trend_directions=(1.0, 1.0, 1.0),
            strength=0.8,
            imbalance=0.5,
        )
        self.assertGreater(output.directional_bias, 0.3)

    def test_directional_bias_with_divergent_trends(self):
        output = self.crm.update(
            hmm_state='NORMAL',
            hmm_posterior=(0.3, 0.4, 0.3),
            trend_scores=(0.8, -0.5, 0.3),
            trend_directions=(1.0, -1.0, 1.0),
            strength=0.3,
            imbalance=0.0,
        )
        self.assertLess(abs(output.directional_bias), 0.8)

    def test_output_to_dict(self):
        output = self.crm.update(
            hmm_state='NORMAL',
            hmm_posterior=(0.2, 0.6, 0.2),
            trend_scores=(0.5, 0.5, 0.5),
            trend_directions=(1.0, 1.0, 1.0),
            strength=0.6,
            imbalance=0.2,
        )
        d = output.to_dict()
        self.assertIn('directional_bias', d)
        self.assertIn('resonance_strength', d)
        self.assertIn('phase', d)
        self.assertIn('state_entropy', d)


class TestResonanceRiskSurface(unittest.TestCase):
    """共振策略：风险曲面调节"""

    def setUp(self):
        self.crm = CycleResonanceModule()

    def test_resonance_risk_surface_release_high_strength(self):
        output = CycleResonanceOutput(
            directional_bias=0.6,
            resonance_strength=0.8,
            phase=Phase.RELEASE,
            state_entropy=0.2,
            hmm_state='NORMAL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('resonance')
        self.assertGreaterEqual(rs.size_multiplier, 0.6)
        self.assertGreater(rs.max_hold_seconds, 300)

    def test_resonance_risk_surface_chaos(self):
        output = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.3,
            phase=Phase.CHAOS,
            state_entropy=0.8,
            hmm_state='HIGH_VOL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('resonance')
        self.assertLessEqual(rs.size_multiplier, 0.4)

    def test_resonance_risk_surface_moderate_strength(self):
        output = CycleResonanceOutput(
            directional_bias=0.3,
            resonance_strength=0.5,
            phase=Phase.CHARGE,
            state_entropy=0.3,
            hmm_state='NORMAL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('resonance')
        self.assertAlmostEqual(rs.size_multiplier, 0.6)

    def test_resonance_risk_surface_low_strength(self):
        output = CycleResonanceOutput(
            directional_bias=0.1,
            resonance_strength=0.2,
            phase=Phase.EXHAUST,
            state_entropy=0.4,
            hmm_state='NORMAL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('resonance')
        self.assertAlmostEqual(rs.size_multiplier, 0.3)


class TestResonancePortfolioConstraints(unittest.TestCase):
    """共振策略：组合约束检查"""

    def setUp(self):
        self.crm = CycleResonanceModule()

    def test_no_violation_with_small_positions(self):
        positions = {
            'IF2606': {'direction': 1, 'size': 0.3},
            'IC2606': {'direction': -1, 'size': 0.2},
        }
        result = self.crm.check_portfolio_constraints(positions)
        self.assertTrue(result['ok'])

    def test_violation_with_large_directional_exposure(self):
        positions = {
            'IF2606': {'direction': 1, 'size': 1.0},
            'IC2606': {'direction': 1, 'size': 0.8},
        }
        output = CycleResonanceOutput(
            directional_bias=0.5,
            resonance_strength=0.6,
            phase=Phase.RELEASE,
            state_entropy=0.2,
        )
        self.crm._last_output = output
        result = self.crm.check_portfolio_constraints(positions)
        self.assertFalse(result['ok'])


class TestResonanceCircuitBreaker(unittest.TestCase):
    """共振策略：熔断机制"""

    def setUp(self):
        self.crm = CycleResonanceModule()

    def test_no_circuit_breaker_normal(self):
        output = CycleResonanceOutput(
            directional_bias=0.3,
            resonance_strength=0.5,
            phase=Phase.RELEASE,
            state_entropy=0.3,
        )
        self.crm._last_output = output
        result = self.crm.check_circuit_breaker()
        self.assertFalse(result['triggered'])

    def test_circuit_breaker_sustained_chaos(self):
        output = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.1,
            phase=Phase.CHAOS,
            state_entropy=0.95,
        )
        self.crm._last_output = output
        result = self.crm.check_circuit_breaker(entropy_sustained_minutes=20)
        self.assertTrue(result['triggered'])
        self.assertEqual(result['action'], 'emergency_degrade')

    def test_circuit_breaker_daily_drawdown(self):
        output = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.3,
            phase=Phase.EXHAUST,
            state_entropy=0.3,
        )
        self.crm._last_output = output
        result = self.crm.check_circuit_breaker(daily_drawdown_pct=4.0)
        self.assertTrue(result['triggered'])
        self.assertEqual(result['action'], 'conservative_mode')


class TestResonanceSignalPriority(unittest.TestCase):
    """共振策略：策略优先级"""

    def setUp(self):
        self.crm = CycleResonanceModule()

    def test_release_phase_prioritizes_resonance(self):
        priority = self.crm.get_signal_priority(Phase.RELEASE)
        self.assertEqual(priority[0], 'resonance')

    def test_charge_phase_prioritizes_spring(self):
        priority = self.crm.get_signal_priority(Phase.CHARGE)
        self.assertEqual(priority[0], 'spring')

    def test_exhaust_phase_prioritizes_hft(self):
        priority = self.crm.get_signal_priority(Phase.EXHAUST)
        self.assertEqual(priority[0], 'high_freq')

    def test_chaos_phase_prioritizes_hft(self):
        priority = self.crm.get_signal_priority(Phase.CHAOS)
        self.assertEqual(priority[0], 'high_freq')


class TestResonanceOrderPlacement(unittest.TestCase):
    """共振策略：完整下单流程"""

    def setUp(self):
        _reset_global_singletons()
        self.crm = CycleResonanceModule()
        self.signal_svc = SignalService()
        self.signal_svc._default_cooldown_seconds = 0.0
        self.signal_svc._adaptive_threshold = None
        self.order_svc = get_order_service()
        self.order_svc._order_idempotent_set = set()
        self.placed_orders = []
        self.order_svc.bind_platform_apis(
            insert_order_func=self._mock_insert_order,
            cancel_order_func=lambda x: None,
        )

    def _mock_insert_order(self, **kwargs):
        self.placed_orders.append(kwargs)
        mock_result = MagicMock()
        mock_result.OrderRef = f"MOCK_{int(time.time()*1000)}"
        mock_result.order_id = f"MOCK_{int(time.time()*1000)}"
        mock_result.OrderStatus = 'SUBMITTED'
        mock_result.VolumeTraded = 0
        return mock_result

    def tearDown(self):
        _reset_global_singletons()

    def test_resonance_buy_order_release_phase(self):
        output = self.crm.update(
            hmm_state='NORMAL',
            hmm_posterior=(0.1, 0.8, 0.1),
            trend_scores=(0.8, 0.9, 0.7),
            trend_directions=(1.0, 1.0, 1.0),
            strength=0.85,
            imbalance=0.5,
        )
        rs = self.crm.get_risk_surface('resonance', output)
        volume = max(1, int(rs.size_multiplier * 3))
        signal = self.signal_svc.generate_signal(
            instrument_id='IF2606',
            signal_type='BUY',
            price=4000.0,
            volume=volume,
            reason='RESONANCE_RELEASE_BUY',
            signal_strength=0.8,
        )
        self.assertIsNotNone(signal)
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=volume,
            price=4000.2,
            direction='BUY',
            action='OPEN',
            open_reason='CORRECT_RESONANCE',
        )
        self.assertIsNotNone(order_id)
        self.assertGreater(len(self.placed_orders), 0)

    def test_resonance_sell_order_release_phase(self):
        output = self.crm.update(
            hmm_state='NORMAL',
            hmm_posterior=(0.1, 0.8, 0.1),
            trend_scores=(-0.8, -0.9, -0.7),
            trend_directions=(-1.0, -1.0, -1.0),
            strength=0.85,
            imbalance=-0.5,
        )
        rs = self.crm.get_risk_surface('resonance', output)
        volume = max(1, int(rs.size_multiplier * 3))
        signal = self.signal_svc.generate_signal(
            instrument_id='IF2606',
            signal_type='SELL',
            price=3998.0,
            volume=volume,
            reason='RESONANCE_RELEASE_SELL',
            signal_strength=0.8,
        )
        self.assertIsNotNone(signal)
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=volume,
            price=3997.8,
            direction='SELL',
            action='OPEN',
            open_reason='CORRECT_RESONANCE',
        )
        self.assertIsNotNone(order_id)

    def test_resonance_reduced_size_in_chaos(self):
        for i in range(25):
            state = 'LOW_VOL' if i % 2 == 0 else 'HIGH_VOL'
            self.crm.update(
                hmm_state=state,
                hmm_posterior=(0.4, 0.2, 0.4),
                trend_scores=(0.1, -0.1, 0.05),
                trend_directions=(1.0, -1.0, 0.0),
                strength=0.15,
                imbalance=0.0,
            )
        output = self.crm.update(
            hmm_state='HIGH_VOL',
            hmm_posterior=(0.1, 0.1, 0.8),
            trend_scores=(0.1, -0.1, 0.05),
            trend_directions=(1.0, -1.0, 0.0),
            strength=0.15,
            imbalance=0.0,
        )
        rs = self.crm.get_risk_surface('resonance', output)
        self.assertLessEqual(rs.size_multiplier, 0.4)
        volume = max(1, int(rs.size_multiplier * 3))
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=volume,
            price=4000.0,
            direction='BUY',
            action='OPEN',
            open_reason='CORRECT_RESONANCE',
        )
        self.assertIsNotNone(order_id)

    def test_resonance_close_position_order(self):
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=4010.0,
            direction='SELL',
            action='CLOSE',
        )
        self.assertIsNotNone(order_id)

    def test_resonance_hft_floor_adjustment(self):
        output = CycleResonanceOutput(
            directional_bias=0.6,
            resonance_strength=0.8,
            phase=Phase.RELEASE,
            state_entropy=0.2,
        )
        result = self.crm.get_hft_floor_adjustment(True, output)
        self.assertEqual(result['size_floor'], 0.7)

    def test_resonance_spring_threshold_adjustment(self):
        output = CycleResonanceOutput(
            directional_bias=0.7,
            resonance_strength=0.6,
            phase=Phase.RELEASE,
            state_entropy=0.2,
        )
        self.crm._last_output = output
        adj = self.crm.get_spring_threshold_adjustment(output)
        self.assertLess(adj['long_threshold_mult'], 1.0)
        self.assertGreater(adj['short_threshold_mult'], 1.0)


class TestHighFreqRiskSurface(unittest.TestCase):
    """高频策略：风险曲面调节"""

    def setUp(self):
        self.crm = CycleResonanceModule()

    def test_hf_co_aligned_release(self):
        output = CycleResonanceOutput(
            directional_bias=0.6,
            resonance_strength=0.8,
            phase=Phase.RELEASE,
            state_entropy=0.2,
            hmm_state='NORMAL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('high_freq')
        self.assertGreaterEqual(rs.size_multiplier, self.crm._p.hf_co_size * 0.5)
        self.assertAlmostEqual(rs.stop_loss_multiplier, self.crm._p.hf_co_sl)
        self.assertAlmostEqual(rs.max_hold_seconds, self.crm._p.hf_co_hold)
        self.assertTrue(rs.allow_overnight)

    def test_hf_counter_aligned(self):
        output = CycleResonanceOutput(
            directional_bias=-0.6,
            resonance_strength=0.4,
            phase=Phase.CHARGE,
            state_entropy=0.3,
            hmm_state='NORMAL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('high_freq')
        self.assertLessEqual(rs.size_multiplier, self.crm._p.hf_counter_size)
        self.assertFalse(rs.allow_overnight)

    def test_hf_chaos_phase(self):
        output = CycleResonanceOutput(
            directional_bias=-0.6,
            resonance_strength=0.1,
            phase=Phase.CHAOS,
            state_entropy=0.9,
            hmm_state='HIGH_VOL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('high_freq')
        entropy_penalty = 1.0 - self.crm._p.hf_entropy_penalty_coeff * output.state_entropy
        expected_max = self.crm._p.hf_chaos_size * entropy_penalty
        self.assertLessEqual(rs.size_multiplier, expected_max + 0.01)
        self.assertFalse(rs.allow_overnight)

    def test_hf_entropy_penalty_reduces_size(self):
        output_low_entropy = CycleResonanceOutput(
            directional_bias=0.5,
            resonance_strength=0.7,
            phase=Phase.RELEASE,
            state_entropy=0.1,
            hmm_state='NORMAL',
        )
        output_high_entropy = CycleResonanceOutput(
            directional_bias=0.5,
            resonance_strength=0.7,
            phase=Phase.RELEASE,
            state_entropy=0.9,
            hmm_state='NORMAL',
        )
        self.crm._last_output = output_low_entropy
        rs_low = self.crm.get_risk_surface('high_freq')
        self.crm._last_output = output_high_entropy
        rs_high = self.crm.get_risk_surface('high_freq')
        self.assertLess(rs_high.size_multiplier, rs_low.size_multiplier)

    def test_hf_never_zero_size(self):
        for phase in Phase:
            for bias in [-0.8, 0.0, 0.8]:
                output = CycleResonanceOutput(
                    directional_bias=bias,
                    resonance_strength=0.5,
                    phase=phase,
                    state_entropy=0.5,
                    hmm_state='NORMAL',
                )
                self.crm._last_output = output
                rs = self.crm.get_risk_surface('high_freq')
                self.assertGreater(rs.size_multiplier, 0.0)
                self.assertGreater(rs.min_size, 0.0)


class TestBoxRiskSurface(unittest.TestCase):
    """箱形策略：风险曲面调节"""

    def setUp(self):
        self.crm = CycleResonanceModule()

    def test_box_low_vol(self):
        output = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.3,
            phase=Phase.CHARGE,
            state_entropy=0.2,
            hmm_state='LOW_VOL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('box')
        self.assertAlmostEqual(rs.size_multiplier, self.crm._p.box_low_vol_size)
        self.assertTrue(rs.allow_overnight)

    def test_box_high_vol_release(self):
        output = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.7,
            phase=Phase.RELEASE,
            state_entropy=0.2,
            hmm_state='HIGH_VOL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('box')
        self.assertAlmostEqual(rs.size_multiplier, self.crm._p.box_high_vol_release_size)

    def test_box_normal_vol(self):
        output = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.4,
            phase=Phase.CHARGE,
            state_entropy=0.3,
            hmm_state='NORMAL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('box')
        self.assertAlmostEqual(rs.size_multiplier, self.crm._p.box_normal_size)

    def test_box_default_case(self):
        output = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.2,
            phase=Phase.EXHAUST,
            state_entropy=0.5,
            hmm_state='HIGH_VOL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('box')
        self.assertAlmostEqual(rs.size_multiplier, self.crm._p.box_default_size)

    def test_box_bias_up_adjustment(self):
        output = CycleResonanceOutput(
            directional_bias=0.8,
            resonance_strength=0.4,
            phase=Phase.CHARGE,
            state_entropy=0.2,
            hmm_state='NORMAL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('box')
        self.assertGreater(rs.size_multiplier, self.crm._p.box_normal_size)

    def test_box_bias_down_adjustment(self):
        output = CycleResonanceOutput(
            directional_bias=-0.8,
            resonance_strength=0.4,
            phase=Phase.CHARGE,
            state_entropy=0.2,
            hmm_state='NORMAL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('box')
        self.assertLess(rs.size_multiplier, self.crm._p.box_normal_size)


class TestSpringRiskSurface(unittest.TestCase):
    """弹簧策略：风险曲面调节"""

    def setUp(self):
        self.crm = CycleResonanceModule()

    def test_spring_charge_phase(self):
        output = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.2,
            phase=Phase.CHARGE,
            state_entropy=0.0,
            hmm_state='LOW_VOL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('spring')
        self.assertAlmostEqual(rs.size_multiplier, self.crm._p.sp_charge_size)
        self.assertTrue(rs.allow_overnight)

    def test_spring_release_phase(self):
        output = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.7,
            phase=Phase.RELEASE,
            state_entropy=0.0,
            hmm_state='NORMAL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('spring')
        self.assertAlmostEqual(rs.size_multiplier, self.crm._p.sp_release_size)
        self.assertFalse(rs.allow_overnight)

    def test_spring_default_phase(self):
        output = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.3,
            phase=Phase.EXHAUST,
            state_entropy=0.0,
            hmm_state='NORMAL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('spring')
        self.assertAlmostEqual(rs.size_multiplier, self.crm._p.sp_default_size)

    def test_spring_bias_boost(self):
        output_strong_bias = CycleResonanceOutput(
            directional_bias=0.9,
            resonance_strength=0.5,
            phase=Phase.RELEASE,
            state_entropy=0.1,
            hmm_state='NORMAL',
        )
        output_weak_bias = CycleResonanceOutput(
            directional_bias=0.1,
            resonance_strength=0.5,
            phase=Phase.RELEASE,
            state_entropy=0.1,
            hmm_state='NORMAL',
        )
        self.crm._last_output = output_strong_bias
        rs_strong = self.crm.get_risk_surface('spring')
        self.crm._last_output = output_weak_bias
        rs_weak = self.crm.get_risk_surface('spring')
        self.assertGreater(rs_strong.size_multiplier, rs_weak.size_multiplier)

    def test_spring_entropy_penalty(self):
        output_low_entropy = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.4,
            phase=Phase.CHARGE,
            state_entropy=0.1,
            hmm_state='LOW_VOL',
        )
        output_high_entropy = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.4,
            phase=Phase.CHARGE,
            state_entropy=0.9,
            hmm_state='LOW_VOL',
        )
        self.crm._last_output = output_low_entropy
        rs_low = self.crm.get_risk_surface('spring')
        self.crm._last_output = output_high_entropy
        rs_high = self.crm.get_risk_surface('spring')
        self.assertLess(rs_high.size_multiplier, rs_low.size_multiplier)

    def test_spring_chaos_overnight_false(self):
        output = CycleResonanceOutput(
            directional_bias=0.0,
            resonance_strength=0.1,
            phase=Phase.CHAOS,
            state_entropy=0.9,
            hmm_state='HIGH_VOL',
        )
        self.crm._last_output = output
        rs = self.crm.get_risk_surface('spring')
        self.assertFalse(rs.allow_overnight)


class TestResonanceSingletonFactory(unittest.TestCase):
    """共振策略：单例工厂"""

    def test_singleton_creation(self):
        reset_cycle_resonance_module()
        crm1 = get_cycle_resonance_module()
        crm2 = get_cycle_resonance_module()
        self.assertIs(crm1, crm2)
        reset_cycle_resonance_module()


if __name__ == '__main__':
    unittest.main()
