# MODULE_ID: M2-564
"""
test_safety_governance_pqs.py — 综合测试覆盖四个模块:
1. ali2026v3_trading.risk.safety_meta_circuit.CircuitBreakerService
2. ali2026v3_trading.risk.safety_meta_position.HardTimeStopAndComplianceService
3. ali2026v3_trading.governance.mode_exit_rules (ExitRuleEngine, DrawdownManager, DefensiveDrawdownChecker)
4. ali2026v3_trading.ProductionQuantSystem.ProductionQuantSystem
"""
from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# 安全导入 — 如果模块不可用则跳过整个测试类
# ---------------------------------------------------------------------------
try:
    from ali2026v3_trading.risk.safety_meta_circuit import CircuitBreakerService
    _HAS_CIRCUIT = True
except Exception:
    _HAS_CIRCUIT = False

try:
    from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService
    _HAS_HARD_STOP = True
except Exception:
    _HAS_HARD_STOP = False

try:
    from ali2026v3_trading.governance.mode_exit_rules import (
        ExitRuleEngine, DrawdownManager, DefensiveDrawdownChecker,
    )
    from ali2026v3_trading.governance.mode_config import (
        TakeProfitMethod, StopLossMethod, DrawdownAction,
    )
    _HAS_EXIT_RULES = True
except Exception:
    _HAS_EXIT_RULES = False

# ---------------------------------------------------------------------------
# ProductionQuantSystem — 通过预注入 mock 模块解决重依赖导入问题
# ---------------------------------------------------------------------------
_PQS_MOCK_MODULES = {}

def _ensure_pqs_importable():
    """在 sys.modules 中注入 quant_infra/quant_core/quant_platform/quant_services/serialization_utils 的 mock，
    使 ProductionQuantSystem 可以成功导入"""
    import types
    import sys

    pkg = "ali2026v3_trading"

    # 需要mock的子模块及其导出类/函数
    _mock_specs = {
        f"{pkg}.quant_infra": ["NumpyRingBuffer"],
        f"{pkg}.quant_core": [
            "MultiPeriodTrendScorer", "IVSurfacePCA", "AdaptiveHMM",
            "VolatilityRegimeFilter", "CointegrationScanner", "SurvivalAnalyzer",
        ],
        f"{pkg}.quant_platform": [
            "ExchangeTime", "TickAggregator", "AtomicSystemState", "SystemHealthMonitor",
        ],
        f"{pkg}.quant_services": [
            "LightweightPersistence", "HotConfigManager", "numba_helper", "HAS_NUMBA",
        ],
        f"{pkg}.serialization_utils": [
            "json_dumps", "json_loads", "json_default_serializer",
        ],
    }

    injected = {}
    for mod_name, exports in _mock_specs.items():
        if mod_name not in sys.modules:
            mod = types.ModuleType(mod_name)
            for name in exports:
                setattr(mod, name, MagicMock())
            mod.__path__ = []
            sys.modules[mod_name] = mod
            injected[mod_name] = mod
        else:
            # 已存在，检查是否缺少属性
            mod = sys.modules[mod_name]
            for name in exports:
                if not hasattr(mod, name):
                    setattr(mod, name, MagicMock())

    # 确保 HAS_NUMBA 是 bool
    if f"{pkg}.quant_services" in sys.modules:
        sys.modules[f"{pkg}.quant_services"].HAS_NUMBA = False

    return injected


def _cleanup_pqs_mock_modules(injected):
    """清理注入的 mock 模块"""
    import sys
    for mod_name in injected:
        sys.modules.pop(mod_name, None)


# 尝试导入 ProductionQuantSystem
_PQS_INJECTED = _ensure_pqs_importable()
try:
    from ali2026v3_trading.ProductionQuantSystem import ProductionQuantSystem
    _HAS_PQS = True
except Exception:
    _HAS_PQS = False


# ===================================================================
# 1. CircuitBreakerService 测试
# ===================================================================

def _make_circuit_breaker_service():
    """创建 CircuitBreakerService 实例，绕过 __init__ 中的重量级依赖"""
    if not _HAS_CIRCUIT:
        pytest.skip("CircuitBreakerService 不可导入")
    svc = CircuitBreakerService.__new__(CircuitBreakerService)
    svc.params = MagicMock()
    svc.owner = MagicMock()
    svc.owner._isolation_level = None
    svc._lock = threading.RLock()
    svc._calm_period_duration = 0.0
    svc._pause_reason = ""
    svc._circuit_breaker_activated_at = 0.0
    svc._circuit_breaker_shadow_mode = False
    svc._circuit_breaker_shadow_until = 0.0
    svc._algo_breakers = {
        'high_cancel_rate': {'triggered': False, 'paused_until': 0.0, 'threshold': 0.70, 'pause_sec': 1800},
        'excessive_self_trade': {'triggered': False, 'paused_until': 0.0, 'threshold': 3, 'pause_sec': 3600, 'window_sec': 3600},
        'high_order_frequency': {'triggered': False, 'paused_until': 0.0, 'threshold': 100, 'pause_sec': 300, 'window_sec': 60},
    }
    svc._algo_paused = False
    svc._algo_paused_until = 0.0
    svc._algo_pause_reason = ""
    svc._circuit_breaker_state_path = "/tmp/test_cb_state.json"
    # Mock _risk_cb_half_open
    svc._risk_cb_half_open = MagicMock()
    svc._risk_cb_half_open.state = 'CLOSED'
    svc._risk_cb_half_open.opened_at = 0.0
    svc._risk_cb_half_open.open_duration = 0.0
    svc._risk_cb_half_open.allow_request.return_value = True
    return svc


@pytest.mark.skipif(not _HAS_CIRCUIT, reason="CircuitBreakerService 不可导入")
class TestCircuitBreakerService:
    """CircuitBreakerService 核心方法测试"""

    def test_init_defaults(self):
        svc = _make_circuit_breaker_service()
        assert svc._pause_reason == ""
        assert svc._circuit_breaker_shadow_mode is False

    def test_is_trading_paused_initially_false(self):
        svc = _make_circuit_breaker_service()
        paused, reason = svc.is_trading_paused()
        assert paused is False
        assert reason == ""

    def test_is_trading_paused_when_cb_open(self):
        svc = _make_circuit_breaker_service()
        now = time.time()
        svc._risk_cb_half_open.state = 'OPEN'
        svc._risk_cb_half_open.opened_at = now
        svc._risk_cb_half_open.open_duration = 120.0
        svc._pause_reason = "速率断路器触发"
        paused, reason = svc.is_trading_paused()
        assert paused is True
        assert "速率断路器触发" in reason

    def test_is_circuit_breaker_shadow_mode_false_initially(self):
        svc = _make_circuit_breaker_service()
        assert svc.is_circuit_breaker_shadow_mode() is False

    def test_is_circuit_breaker_shadow_mode_true_when_active(self):
        svc = _make_circuit_breaker_service()
        svc._circuit_breaker_shadow_mode = True
        svc._circuit_breaker_shadow_until = time.time() + 300
        assert svc.is_circuit_breaker_shadow_mode() is True

    def test_is_circuit_breaker_shadow_mode_expires(self):
        svc = _make_circuit_breaker_service()
        svc._circuit_breaker_shadow_mode = True
        svc._circuit_breaker_shadow_until = time.time() - 10  # 过去时间
        assert svc.is_circuit_breaker_shadow_mode() is False
        # 过期后应重置
        assert svc._circuit_breaker_shadow_mode is False

    def test_is_new_open_blocked_true(self):
        svc = _make_circuit_breaker_service()
        assert svc.is_new_open_blocked(daily_new_open_blocked=True) is True

    def test_is_new_open_blocked_false(self):
        svc = _make_circuit_breaker_service()
        assert svc.is_new_open_blocked(daily_new_open_blocked=False) is False

    def test_can_open_not_paused(self):
        svc = _make_circuit_breaker_service()
        # 未暂停且未阻断
        assert svc.can_open(daily_new_open_blocked=False) is True

    def test_can_open_paused(self):
        svc = _make_circuit_breaker_service()
        now = time.time()
        svc._risk_cb_half_open.state = 'OPEN'
        svc._risk_cb_half_open.opened_at = now
        svc._risk_cb_half_open.open_duration = 120.0
        svc._pause_reason = "test"
        assert svc.can_open(daily_new_open_blocked=False) is False

    def test_is_algo_paused_false_initially(self):
        svc = _make_circuit_breaker_service()
        paused, reason = svc.is_algo_paused()
        assert paused is False
        assert reason == ""

    def test_is_hard_stop_triggered_true(self):
        svc = _make_circuit_breaker_service()
        assert svc.is_hard_stop_triggered(daily_hard_stop_triggered=True) is True

    def test_is_hard_stop_triggered_false(self):
        svc = _make_circuit_breaker_service()
        assert svc.is_hard_stop_triggered(daily_hard_stop_triggered=False) is False

    def test_generate_resume_token(self):
        svc = _make_circuit_breaker_service()
        token = svc.generate_resume_token()
        assert isinstance(token, str)
        assert len(token) == 32  # secrets.token_hex(16) = 32 hex chars
        # 验证是有效的十六进制字符串
        int(token, 16)


# ===================================================================
# 2. HardTimeStopAndComplianceService 测试
# ===================================================================

def _make_hard_time_stop_service():
    """创建 HardTimeStopAndComplianceService 实例，绕过 __init__ 中的重量级依赖"""
    if not _HAS_HARD_STOP:
        pytest.skip("HardTimeStopAndComplianceService 不可导入")
    svc = HardTimeStopAndComplianceService.__new__(HardTimeStopAndComplianceService)
    svc.params = {}
    svc.owner = MagicMock()
    svc._lock = threading.RLock()
    svc._stats_lock = threading.Lock()
    svc._logic_reversal_priority = {}
    svc._rollover_cost_bps = 0.0
    svc._rollover_count = 0
    svc._margin_manager = MagicMock()
    svc._compliance_checker = MagicMock()
    # Mock _get_stage 方法
    svc._get_stage1_minutes = MagicMock(return_value=30)
    svc._get_stage2_minutes = MagicMock(return_value=60)
    svc._get_stage1_profit_threshold = MagicMock(return_value=0.005)
    return svc


@pytest.mark.skipif(not _HAS_HARD_STOP, reason="HardTimeStopAndComplianceService 不可导入")
class TestHardTimeStopAndComplianceService:
    """HardTimeStopAndComplianceService 核心方法测试"""

    def test_check_position_hard_time_stop_stage1(self):
        svc = _make_hard_time_stop_service()
        now = time.time()
        open_time = now - 35 * 60  # 35分钟前开仓，超过stage1的30分钟
        result = svc.check_position_hard_time_stop(
            position_id="pos_001",
            open_time=open_time,
            max_profit_reached=0.001,  # 低于阈值0.005
        )
        assert result is not None
        assert "HardTimeStop" in result

    def test_check_position_hard_time_stop_no_trigger(self):
        svc = _make_hard_time_stop_service()
        now = time.time()
        open_time = now - 10 * 60  # 10分钟前开仓，未到stage1的30分钟
        result = svc.check_position_hard_time_stop(
            position_id="pos_002",
            open_time=open_time,
            max_profit_reached=0.001,
        )
        assert result is None

    def test_check_position_hard_time_stop_logic_reversal(self):
        """逻辑反转已触发时，硬时间止损降级返回None"""
        svc = _make_hard_time_stop_service()
        now = time.time()
        open_time = now - 35 * 60  # 超过stage1
        svc._logic_reversal_priority["pos_003"] = True
        result = svc.check_position_hard_time_stop(
            position_id="pos_003",
            open_time=open_time,
            max_profit_reached=0.001,
        )
        assert result is None

    def test_set_logic_reversal_triggered(self):
        svc = _make_hard_time_stop_service()
        svc.set_logic_reversal_triggered("pos_004", True)
        assert svc._logic_reversal_priority.get("pos_004") is True
        svc.set_logic_reversal_triggered("pos_004", False)
        assert svc._logic_reversal_priority.get("pos_004") is False

    def test_check_capital_sufficiency_sufficient(self):
        svc = _make_hard_time_stop_service()
        svc._margin_manager.check_capital_sufficiency.return_value = {'sufficient': True}
        result = svc.check_capital_sufficiency(equity=100000.0, required_margin=5000.0)
        assert result['sufficient'] is True

    def test_check_capital_sufficiency_insufficient(self):
        svc = _make_hard_time_stop_service()
        svc._margin_manager.check_capital_sufficiency.return_value = {'sufficient': False}
        result = svc.check_capital_sufficiency(equity=1000.0, required_margin=50000.0)
        assert result['sufficient'] is False


# ===================================================================
# 3. ExitRuleEngine / DrawdownManager / DefensiveDrawdownChecker 测试
# ===================================================================

@pytest.mark.skipif(not _HAS_EXIT_RULES, reason="mode_exit_rules 不可导入")
class TestExitRuleEngine:
    """ExitRuleEngine 止盈止损计算测试"""

    def test_compute_take_profit_tiered_buy(self):
        engine = ExitRuleEngine(TakeProfitMethod.TIERED, StopLossMethod.FIXED)
        levels = engine.compute_take_profit_levels(
            entry_price=100.0, volatility_1d=5.0, size=1.0, direction='BUY',
        )
        assert len(levels) == 3
        # 第一级: entry + 1*vol = 105
        assert levels[0]['price'] == 105.0
        assert levels[0]['volume_ratio'] == 0.50
        # 第二级: entry + 2*vol = 110
        assert levels[1]['price'] == 110.0
        assert levels[1]['volume_ratio'] == 0.30
        # 第三级: 无价格(追踪止损)
        assert levels[2]['price'] is None
        assert levels[2]['volume_ratio'] == 0.20

    def test_compute_take_profit_tiered_sell(self):
        engine = ExitRuleEngine(TakeProfitMethod.TIERED, StopLossMethod.FIXED)
        levels = engine.compute_take_profit_levels(
            entry_price=100.0, volatility_1d=5.0, size=1.0, direction='SELL',
        )
        assert len(levels) == 3
        # SELL方向: 价格应下降
        assert levels[0]['price'] == 95.0   # 100 - 5
        assert levels[1]['price'] == 90.0   # 100 - 10
        assert levels[2]['price'] is None

    def test_compute_take_profit_trailing(self):
        engine = ExitRuleEngine(TakeProfitMethod.TRAILING, StopLossMethod.FIXED)
        levels = engine.compute_take_profit_levels(
            entry_price=100.0, volatility_1d=5.0, size=1.0, direction='BUY',
        )
        assert len(levels) == 1
        assert 'activation' in levels[0]
        # BUY: activation = entry * 1.05 = 105
        assert levels[0]['activation'] == pytest.approx(105.0)
        assert levels[0]['trail_pct'] == 0.10

    def test_compute_take_profit_fixed(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.FIXED)
        levels = engine.compute_take_profit_levels(
            entry_price=100.0, volatility_1d=5.0, size=1.0, direction='BUY',
        )
        assert len(levels) == 1
        # FIXED BUY: price = entry * 1.10 = 110
        assert levels[0]['price'] == pytest.approx(110.0)
        assert levels[0]['volume_ratio'] == 1.0

    def test_compute_stop_loss_volatility(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.VOLATILITY)
        result = engine.compute_stop_loss(
            entry_price=100.0, stop_distance=3.0, volatility_20d=0.02, direction='BUY',
        )
        assert result['method'] == 'volatility'
        # sl_distance = max(3.0, 100 * 0.02 * 1.5) = max(3.0, 3.0) = 3.0
        assert result['distance'] == pytest.approx(3.0)
        # price = 100 - 3 = 97
        assert result['price'] == pytest.approx(97.0)

    def test_compute_stop_loss_time_decay(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.TIME_DECAY)
        result = engine.compute_stop_loss(
            entry_price=100.0, stop_distance=5.0, direction='BUY',
        )
        assert result['method'] == 'time_decay'
        assert result['distance'] == 5.0
        assert result['price'] == pytest.approx(95.0)

    def test_compute_stop_loss_fixed(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.FIXED)
        result = engine.compute_stop_loss(
            entry_price=100.0, stop_distance=4.0, direction='BUY',
        )
        assert result['method'] == 'fixed'
        assert result['distance'] == 4.0
        assert result['price'] == pytest.approx(96.0)

    def test_take_profit_method_property(self):
        engine = ExitRuleEngine(TakeProfitMethod.TRAILING, StopLossMethod.VOLATILITY)
        assert engine.take_profit_method == TakeProfitMethod.TRAILING

    def test_stop_loss_method_property(self):
        engine = ExitRuleEngine(TakeProfitMethod.TRAILING, StopLossMethod.VOLATILITY)
        assert engine.stop_loss_method == StopLossMethod.VOLATILITY


@pytest.mark.skipif(not _HAS_EXIT_RULES, reason="mode_exit_rules 不可导入")
class TestDrawdownManager:
    """DrawdownManager 回撤管理测试"""

    @patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event')
    def _make_dm(self, action=DrawdownAction.REDUCE_SIZE, **kwargs):
        return DrawdownManager(action=action, **kwargs)

    def test_drawdown_manager_init(self, ):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE)
        assert dm._current_drawdown == 0.0
        assert dm._drawdown_low == 0.0
        assert dm._var_upgrade_active is False
        assert dm._atr_upgrade_active is False
        assert dm._daily_hard_stop_triggered is False

    def test_update_var_baseline_activates_upgrade(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE, var_upgrade_threshold=0.02)
        # 构造超过阈值的收益率序列 (VaR > 0.02)
        returns = [-0.05] * 5 + [0.01] * 15  # 20个数据点
        var_value = dm.update_var_baseline(returns, confidence=0.95)
        assert var_value > 0
        assert dm._var_upgrade_active is True

    def test_update_var_baseline_no_upgrade(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE, var_upgrade_threshold=0.02)
        # 构造低于阈值的收益率序列 (VaR < 0.02)
        returns = [-0.001] * 5 + [0.001] * 15  # 20个数据点，VaR很小
        var_value = dm.update_var_baseline(returns, confidence=0.95)
        assert dm._var_upgrade_active is False

    def test_update_var_baseline_insufficient_data(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE)
        # 少于20个数据点
        returns = [0.01, -0.02, 0.005]
        var_value = dm.update_var_baseline(returns)
        assert var_value == 0.0

    def test_update_atr_baseline_activates_upgrade(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE, atr_upgrade_threshold=0.015)
        dm.update_atr_baseline(0.025)  # ATR > 0.015
        assert dm._atr_upgrade_active is True

    def test_update_atr_baseline_no_upgrade(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE, atr_upgrade_threshold=0.015)
        dm.update_atr_baseline(0.010)  # ATR < 0.015
        assert dm._atr_upgrade_active is False

    def test_update_drawdown(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE)
        dm.update_drawdown(-0.03)
        assert dm._current_drawdown == -0.03
        assert dm._drawdown_low == -0.03
        # 更深的回撤
        dm.update_drawdown(-0.05)
        assert dm._current_drawdown == -0.05
        assert dm._drawdown_low == -0.05
        # 回撤恢复
        dm.update_drawdown(-0.02)
        assert dm._current_drawdown == -0.02
        assert dm._drawdown_low == -0.05  # low不变

    def test_should_reduce_size_hard_stop(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE)
        dm._daily_hard_stop_triggered = True
        assert dm.should_reduce_size() is True

    def test_should_reduce_size_with_var_upgrade(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE)
        dm._var_upgrade_active = True
        dm.update_drawdown(-0.01)
        assert dm.should_reduce_size() is True

    def test_should_reduce_size_normal(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE)
        dm.update_drawdown(-0.01)
        assert dm.should_reduce_size() is True

    def test_should_reduce_size_no_drawdown(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE)
        dm.update_drawdown(0.0)
        assert dm.should_reduce_size() is False

    def test_should_halt_new_hard_stop(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.HALT_NEW)
        dm._daily_hard_stop_triggered = True
        assert dm.should_halt_new() is True

    def test_should_halt_new_with_upgrade(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE)
        dm._var_upgrade_active = True
        dm.update_drawdown(-0.01)
        assert dm.should_halt_new() is True

    def test_should_full_stop_hard_stop(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.FULL_STOP)
        dm._daily_hard_stop_triggered = True
        assert dm.should_full_stop() is True

    def test_should_full_stop_upgrade_threshold(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.FULL_STOP)
        dm._var_upgrade_active = True
        dm.update_drawdown(-0.03)  # < -0.02
        assert dm.should_full_stop() is True

    def test_should_full_stop_default(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.FULL_STOP)
        dm.update_drawdown(-0.06)  # < -0.05
        assert dm.should_full_stop() is True

    def test_should_full_stop_not_enough(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.FULL_STOP)
        dm.update_drawdown(-0.03)  # > -0.05
        assert dm.should_full_stop() is False

    def test_is_recovered_no_drawdown(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE)
        dm._drawdown_low = 0.0
        assert dm.is_recovered(current_equity=100.0, peak_equity=100.0) is True

    def test_is_recovered_success(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE, recovery_target=1.0)
        dm._drawdown_low = -10.0
        # peak=100, low=-10 => trough=90, need recovery=10
        # current=100, actual_recovery = 100 - (100 + (-10)) = 100 - 90 = 10 >= 10
        assert dm.is_recovered(current_equity=100.0, peak_equity=100.0) is True

    def test_is_recovered_not_enough(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE, recovery_target=1.0)
        dm._drawdown_low = -10.0
        # peak=100, low=-10 => trough=90, need recovery=10
        # current=92, actual_recovery = 92 - 90 = 2 < 10
        assert dm.is_recovered(current_equity=92.0, peak_equity=100.0) is False

    def test_set_daily_hard_stop(self):
        with patch('ali2026v3_trading.governance.mode_exit_rules.DrawdownManager._subscribe_daily_hard_stop_event'):
            dm = DrawdownManager(action=DrawdownAction.REDUCE_SIZE)
        dm.set_daily_hard_stop(True)
        assert dm._daily_hard_stop_triggered is True
        dm.set_daily_hard_stop(False)
        assert dm._daily_hard_stop_triggered is False


@pytest.mark.skipif(not _HAS_EXIT_RULES, reason="mode_exit_rules 不可导入")
class TestDefensiveDrawdownChecker:
    """DefensiveDrawdownChecker 防御性减仓检查测试"""

    def test_check_no_decay(self):
        checker = DefensiveDrawdownChecker()
        # 当前指标 >= 入场指标，无衰减
        result = checker.check(
            current_sortino=2.0, current_calmar=1.5,
            entry_sortino=1.5, entry_calmar=1.0,
        )
        assert result == 1.0

    def test_check_sortino_decay(self):
        checker = DefensiveDrawdownChecker()
        # sortino衰减: 0.5/1.5 ≈ 0.333 < 0.5 阈值
        result = checker.check(
            current_sortino=0.5, current_calmar=1.5,
            entry_sortino=1.5, entry_calmar=1.0,
            decay_threshold=0.5,
        )
        # min(0.5/1.5, 1.5/1.0) = min(0.333, 1.5) = 0.333
        assert result == pytest.approx(0.5 / 1.5)

    def test_check_calmar_decay(self):
        checker = DefensiveDrawdownChecker()
        # calmar衰减: 0.3/1.0 = 0.3 < 0.5 阈值
        result = checker.check(
            current_sortino=2.0, current_calmar=0.3,
            entry_sortino=1.5, entry_calmar=1.0,
            decay_threshold=0.5,
        )
        # min(2.0/1.5, 0.3/1.0) = min(1.333, 0.3) = 0.3
        assert result == pytest.approx(0.3)

    def test_check_zero_entry(self):
        checker = DefensiveDrawdownChecker()
        # entry_sortino <= 0，不触发减仓
        result = checker.check(
            current_sortino=1.0, current_calmar=1.0,
            entry_sortino=0.0, entry_calmar=1.0,
        )
        assert result == 1.0


# ===================================================================
# 4. ProductionQuantSystem 测试
# ===================================================================

def _make_pqs():
    """创建 ProductionQuantSystem 实例，绕过 __init__ 中的重量级依赖"""
    if not _HAS_PQS:
        pytest.skip("ProductionQuantSystem 不可导入")
    pqs = ProductionQuantSystem.__new__(ProductionQuantSystem)
    pqs._shutdown_requested = False
    pqs._initialized = False
    pqs._symbols = []
    pqs._crm = None
    pqs._last_close = None
    pqs._last_cum_volume = 0
    from collections import deque
    pqs._signed_volume_history = deque(maxlen=20)
    return pqs


@pytest.mark.skipif(not _HAS_PQS, reason="ProductionQuantSystem 不可导入")
class TestProductionQuantSystem:
    """ProductionQuantSystem 门面类基础测试"""

    def test_class_exists(self):
        assert ProductionQuantSystem is not None
        assert callable(ProductionQuantSystem)

    def test_shutdown_flag_default(self):
        pqs = _make_pqs()
        assert pqs._shutdown_requested is False

    def test_get_system_status_returns_dict(self):
        pqs = _make_pqs()
        # Mock 所有 get_system_status 依赖的属性
        pqs._initialized = False
        pqs._symbols = []
        pqs.atomic_state = MagicMock()
        pqs.atomic_state.version = 0
        pqs.atomic_state.age_ms = 0
        pqs.health_monitor = MagicMock()
        pqs.health_monitor.get_health_report.return_value = {}
        pqs.hot_config = MagicMock()
        pqs.hot_config.get_all.return_value = {}
        pqs.health_check_api = None
        # HAS_NUMBA 在模块级 mock 中已设为 False
        status = pqs.get_system_status()
        assert isinstance(status, dict)
        assert 'initialized' in status
        assert 'symbols' in status
        assert status['initialized'] is False
        assert status['symbols'] == []

    def test_shutdown_sets_flag(self):
        pqs = _make_pqs()
        pqs._initialized = True
        # Mock shutdown 所需的子服务
        pqs.hot_config = MagicMock()
        pqs.persistence = MagicMock()
        pqs.hmm = MagicMock()
        pqs.hmm._em_running = False
        pqs.hmm._em_thread = None
        pqs.tick_aggregator = MagicMock()
        pqs.health_monitor = MagicMock()
        pqs.atomic_state = MagicMock()
        pqs.health_check_api = None
        pqs.structured_logger = None
        pqs.shutdown()
        assert pqs._shutdown_requested is True or pqs._initialized is False
