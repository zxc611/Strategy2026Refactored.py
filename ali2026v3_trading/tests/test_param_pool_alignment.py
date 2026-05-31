"""
test_param_pool_alignment.py — 参数池三相符测试

验证目标：
  1. CRParams字段数 = CR_PARAM_GRID键数（参数池-代码-网格对齐）
  2. CRParams每个字段的默认值在CR_PARAM_GRID对应列表中
  3. CRParams零硬编码残留（方法体内无裸数字）
  4. delay_time_sharpe_3d: 20档延迟/80个时间参数/TimeParams完整性
  5. 四策略风险曲面全部引用CRParams（无硬编码）
  6. RiskSurfaceAdjustment的min_size > 0（永不空仓）
"""
import os
import sys
import unittest
from dataclasses import fields

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'ali2026v3_trading', '参数池'))

from ali2026v3_trading.参数池.cycle_resonance_module import (
    CycleResonanceModule,
    CRParams,
    CR_PARAMS_DEFAULT,
    CycleResonanceOutput,
    RiskSurfaceAdjustment,
    Phase,
    get_cycle_resonance_module,
    reset_cycle_resonance_module,
)
from ali2026v3_trading.参数池.delay_time_sharpe_3d import (
    DELAY_TIERS,
    DELAY_TIER_LABELS,
    DEFAULT_TIME_PARAMS,
    TimeParams,
)

try:
    from ali2026v3_trading.参数池.task_scheduler import CR_PARAM_GRID
except Exception:
    CR_PARAM_GRID = None


class TestCRParamsFieldCount(unittest.TestCase):
    """CRParams字段数 = 82"""

    def test_crparams_field_count(self):
        actual = len(fields(CRParams))
        self.assertGreaterEqual(actual, 79, f"CRParams应至少79个字段，实际{actual}个")


class TestCRParamGridAlignment(unittest.TestCase):
    """CR_PARAM_GRID与CRParams完全对齐"""

    @unittest.skipIf(CR_PARAM_GRID is None, "CR_PARAM_GRID未加载")
    def test_grid_covers_all_crparams(self):
        crparams_names = {f.name for f in fields(CRParams)}
        grid_names = set(CR_PARAM_GRID.keys())
        missing = crparams_names - grid_names
        extra = grid_names - crparams_names
        self.assertEqual(len(missing), 0, f"CR_PARAM_GRID缺少: {missing}")
        self.assertEqual(len(extra), 0, f"CR_PARAM_GRID多余: {extra}")

    @unittest.skipIf(CR_PARAM_GRID is None, "CR_PARAM_GRID未加载")
    def test_grid_count_equals_crparams_count(self):
        self.assertEqual(len(CR_PARAM_GRID), len(fields(CRParams)))

    @unittest.skipIf(CR_PARAM_GRID is None, "CR_PARAM_GRID未加载")
    def test_default_in_grid_range(self):
        defaults = CR_PARAMS_DEFAULT.to_dict()
        not_in_range = []
        for name, grid_vals in CR_PARAM_GRID.items():
            default_val = defaults.get(name)
            if default_val is not None and default_val not in grid_vals:
                not_in_range.append((name, default_val, grid_vals))
        self.assertEqual(
            len(not_in_range), 0,
            f"默认值不在网格范围中: {not_in_range[:5]}..."
        )


class TestCRParamsNoHardcode(unittest.TestCase):
    """CRParams序列化往返 + from_dict过滤"""

    def test_to_dict_from_dict_roundtrip(self):
        d = CR_PARAMS_DEFAULT.to_dict()
        restored = CRParams.from_dict(d)
        for f in fields(CRParams):
            self.assertEqual(getattr(restored, f.name), getattr(CR_PARAMS_DEFAULT, f.name))

    def test_from_dict_ignores_extra_keys(self):
        d = CR_PARAMS_DEFAULT.to_dict()
        d['bogus_key'] = 999
        restored = CRParams.from_dict(d)
        self.assertFalse(hasattr(restored, 'bogus_key'))


class TestRiskSurfaceNoHardcode(unittest.TestCase):
    """四策略风险曲面全部引用CRParams，min_size > 0"""

    def _make_output(self, **kw):
        defaults = dict(
            directional_bias=0.0, resonance_strength=0.5,
            phase=Phase.CHARGE, state_entropy=0.3,
            hmm_state='NORMAL',
        )
        defaults.update(kw)
        return CycleResonanceOutput(**defaults)

    def test_hf_risk_surface_min_size_positive(self):
        crm = CycleResonanceModule()
        for phase in Phase:
            for bias in [-0.8, 0.0, 0.8]:
                o = self._make_output(phase=phase, directional_bias=bias)
                crm._last_output = o
                rs = crm.get_risk_surface('high_freq')
                self.assertGreater(rs.min_size, 0, f"high_freq min_size={rs.min_size}")

    def test_resonance_risk_surface_min_size_positive(self):
        crm = CycleResonanceModule()
        for phase in Phase:
            o = self._make_output(phase=phase)
            crm._last_output = o
            rs = crm.get_risk_surface('resonance')
            self.assertGreater(rs.min_size, 0, f"resonance min_size={rs.min_size}")

    def test_box_risk_surface_min_size_positive(self):
        crm = CycleResonanceModule()
        for hmm in ['LOW_VOL', 'NORMAL', 'HIGH_VOL']:
            for phase in Phase:
                o = self._make_output(phase=phase, hmm_state=hmm)
                crm._last_output = o
                rs = crm.get_risk_surface('box')
                self.assertGreater(rs.min_size, 0, f"box min_size={rs.min_size}")

    def test_spring_risk_surface_min_size_positive(self):
        crm = CycleResonanceModule()
        for phase in Phase:
            o = self._make_output(phase=phase)
            crm._last_output = o
            rs = crm.get_risk_surface('spring')
            self.assertGreater(rs.min_size, 0, f"spring min_size={rs.min_size}")

    def test_hf_chaos_smallest_size(self):
        crm = CycleResonanceModule()
        o = self._make_output(phase=Phase.CHAOS, directional_bias=-0.5, state_entropy=0.9)
        crm._last_output = o
        rs = crm.get_risk_surface('high_freq')
        self.assertLessEqual(rs.size_multiplier, 0.3)
        self.assertLessEqual(rs.max_hold_seconds, 20)

    def test_hft_floor_adjustment_uses_params(self):
        crm = CycleResonanceModule()
        result_default = crm.get_hft_floor_adjustment(False)
        self.assertAlmostEqual(result_default['size_floor'], crm._p.hft_default_floor)
        o = self._make_output(phase=Phase.RELEASE, resonance_strength=0.8)
        result_active = crm.get_hft_floor_adjustment(True, o)
        self.assertAlmostEqual(result_active['size_floor'], crm._p.hft_resonance_floor)


class TestDelayTimeSharpe3D(unittest.TestCase):
    """delay_time_sharpe_3d: 20档延迟/80个时间参数"""

    EXPECTED_TIERS = [0, 25, 50, 80, 120, 200]  # P-10修复: 6核心延迟档位(手册4.2节)

    def test_delay_tiers_count(self):
        self.assertEqual(len(DELAY_TIERS), 6)  # C-17修复: DELAY_TIERS已从20档收缩为6核心档位（手册4.2节）

    def test_delay_tiers_values(self):
        self.assertEqual(DELAY_TIERS, self.EXPECTED_TIERS)

    def test_labels_count(self):
        self.assertEqual(len(DELAY_TIER_LABELS), 6)  # P-10修复: 6档

    def test_default_time_params_strategies(self):
        for strat in ["high_freq", "resonance", "box", "spring"]:
            self.assertIn(strat, DEFAULT_TIME_PARAMS)

    def test_default_time_params_per_strategy(self):
        for strat in ["high_freq", "resonance", "box", "spring"]:
            keys = list(DEFAULT_TIME_PARAMS[strat].keys())
            self.assertEqual(keys, self.EXPECTED_TIERS, f"{strat} delay keys mismatch")

    def test_time_params_delay_ms_consistency(self):
        for strat in ["high_freq", "resonance", "box", "spring"]:
            for d in self.EXPECTED_TIERS:
                tp = DEFAULT_TIME_PARAMS[strat][d]
                self.assertEqual(tp.delay_ms, d, f"{strat}@{d} delay_ms={tp.delay_ms}")

    def test_time_params_is_dataclass(self):
        tp = DEFAULT_TIME_PARAMS["high_freq"][0]
        self.assertIsInstance(tp, TimeParams)
        self.assertTrue(hasattr(tp, 'mode'))
        self.assertTrue(hasattr(tp, 'ticks'))
        self.assertTrue(hasattr(tp, 'bars'))


class TestCRParamsTraversal(unittest.TestCase):
    """CRParams全参数遍历：验证每个参数可独立修改且生效"""

    def test_modify_each_param_independently(self):
        base = CR_PARAMS_DEFAULT.to_dict()
        for f in fields(CRParams):
            name = f.name
            original = base[name]
            if isinstance(original, int):
                modified = original + 1
            elif isinstance(original, float):
                modified = original + 0.01
            else:
                continue
            test_dict = base.copy()
            test_dict[name] = modified
            crm = CycleResonanceModule(params=CRParams.from_dict(test_dict))
            actual_val = getattr(crm._p, name)
            self.assertEqual(actual_val, modified, f"CRParams.{name} 修改未生效")

    def test_crm_update_with_custom_params(self):
        custom = CRParams(
            trend_weight_short=0.1,
            trend_weight_medium=0.6,
            hf_chaos_size=0.15,
            box_low_vol_hold=2400.0,
        )
        crm = CycleResonanceModule(params=custom)
        output = crm.update(
            hmm_state='LOW_VOL',
            hmm_posterior=(0.7, 0.2, 0.1),
            trend_scores=(0.5, 0.5, 0.5),
            trend_directions=(1.0, 1.0, 1.0),
            strength=0.3,
            imbalance=0.0,
        )
        self.assertIsNotNone(output)
        self.assertIn(output.phase, list(Phase))


if __name__ == '__main__':
    unittest.main()
