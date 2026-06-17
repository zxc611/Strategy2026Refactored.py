# MODULE_ID: M2-557
"""
risk_circuit_breaker 行为测试 — 熔断触发/恢复

验证 SafetyMetaLayer 的核心行为契约:
1. 熔断触发: 权益急跌超过阈值 → 交易暂停
2. 熔断冷静期: 冷静期内拒绝重触发
3. 熔断恢复: 冷静期后可再次触发
4. 日最大回撤硬止损: 回撤超限 → 禁止开仓+强制平仓
5. 日硬止损恢复: confirm_daily_resume → 恢复交易
6. 硬止损平仓豁免: 硬止损期间平仓仍被允许
7. 跨日状态保持: 新交易日不自动重置硬止损
8. 属性委托: __getattr__/__setattr__ 代理到子服务
"""
import time
import threading
import pytest
from unittest.mock import patch, MagicMock

from ali2026v3_trading.risk.risk_circuit_breaker import (
    SafetyMetaLayer, get_safety_meta_layer, get_risk_circuit_breaker,
    cleanup_safety_layer,
)
from ali2026v3_trading.risk.risk_service import RiskService, RiskCheckResponse


# ============================================================================
# 熔断触发行为
# ============================================================================

class TestCircuitBreakerTriggerBehavior:
    """验证: 权益急跌 → 熔断触发 → 交易暂停"""

    def test_equity_drop_triggers_circuit_breaker(self):
        """权益急跌超过3σ阈值 → circuit_breaker_triggers计数增加"""
        s = SafetyMetaLayer()
        s._prev_5day_avg_profit = 100.0
        s._daily_start_equity = 10000.0
        s._current_date = "2026-06-15"
        now = time.time()
        # 构造急跌序列: 前10个点平稳, 后2个点暴跌
        for i in range(10):
            s._equity_series.append(10000.0)
            s._equity_timestamps.append(now - (12 - i) * 10)
        s._equity_series.append(9400.0)
        s._equity_timestamps.append(now - 20)
        s._equity_series.append(9200.0)
        s._equity_timestamps.append(now - 10)
        old_triggers = s._stats["circuit_breaker_triggers"]
        s._check_circuit_breaker(now)
        assert s._stats["circuit_breaker_triggers"] > old_triggers, \
            "权益急跌后熔断器应触发"

    def test_circuit_breaker_triggers_trading_pause(self):
        """熔断触发后 → is_trading_paused 返回 (True, reason)"""
        s = SafetyMetaLayer()
        s._prev_5day_avg_profit = 100.0
        s._daily_start_equity = 10000.0
        s._current_date = "2026-06-15"
        now = time.time()
        for i in range(10):
            s._equity_series.append(10000.0)
            s._equity_timestamps.append(now - (12 - i) * 10)
        s._equity_series.append(9400.0)
        s._equity_timestamps.append(now - 20)
        s._equity_series.append(9200.0)
        s._equity_timestamps.append(now - 10)
        s._check_circuit_breaker(now)
        paused, reason = s.is_trading_paused()
        assert paused, "熔断触发后交易应暂停"

    def test_stable_equity_no_trigger(self):
        """权益平稳 → 熔断器不触发"""
        s = SafetyMetaLayer()
        s._prev_5day_avg_profit = 100.0
        s._daily_start_equity = 10000.0
        s._current_date = "2026-06-15"
        now = time.time()
        for i in range(12):
            s._equity_series.append(10000.0 + i * 10)
            s._equity_timestamps.append(now - (12 - i) * 10)
        old_triggers = s._stats["circuit_breaker_triggers"]
        s._check_circuit_breaker(now)
        assert s._stats["circuit_breaker_triggers"] == old_triggers, \
            "权益平稳时熔断器不应触发"


# ============================================================================
# 熔断冷静期行为
# ============================================================================

class TestCircuitBreakerCalmPeriodBehavior:
    """验证: 冷静期内拒绝重触发, 冷静期后可再次触发"""

    def test_calm_period_rejects_retrigger(self):
        """冷静期内 → 再次触发被拒绝, calm_rejects计数增加"""
        s = SafetyMetaLayer()
        s._prev_5day_avg_profit = 100.0
        s._daily_start_equity = 10000.0
        s._current_date = "2026-06-15"
        now = time.time()
        # 设置冷静期: 600秒
        s._circuit_breaker_svc._risk_cb_half_open.force_open(
            open_duration_sec=10.0, opened_at=now
        )
        s._circuit_breaker_svc._calm_period_duration = 600.0
        for i in range(12):
            s._equity_series.append(10000.0 if i < 10 else 9500.0)
            s._equity_timestamps.append(now - (12 - i) * 10)
        old_triggers = s._stats["circuit_breaker_triggers"]
        s._check_circuit_breaker(now)
        assert s._stats["circuit_breaker_calm_rejects"] >= 1, \
            "冷静期内应拒绝重触发"
        assert s._stats["circuit_breaker_triggers"] == old_triggers, \
            "冷静期内不应增加触发计数"

    def test_after_calm_period_can_trigger(self):
        """冷静期过后 → 可以再次触发熔断"""
        s = SafetyMetaLayer()
        s._prev_5day_avg_profit = 100.0
        s._daily_start_equity = 10000.0
        s._current_date = "2026-06-15"
        now = time.time()
        # 设置已过期的冷静期
        s._circuit_breaker_svc._risk_cb_half_open.force_open(
            open_duration_sec=10.0, opened_at=now - 700
        )
        s._circuit_breaker_svc._calm_period_duration = 600.0
        for i in range(12):
            val = 10000.0 if i < 10 else 9500.0
            s._equity_series.append(val)
            s._equity_timestamps.append(now - (12 - i) * 10)
        s._check_circuit_breaker(now)
        assert s._stats["circuit_breaker_triggers"] >= 1, \
            "冷静期后应可再次触发"


# ============================================================================
# 日最大回撤硬止损行为
# ============================================================================

class TestDailyHardStopBehavior:
    """验证: 日回撤超限 → 硬止损触发 → 禁止开仓"""

    def test_drawdown_exceed_triggers_hard_stop(self):
        """日回撤超过5日均值2倍 → 硬止损触发+开仓被阻断"""
        s = SafetyMetaLayer()
        s._prev_5day_avg_profit = 100.0
        s._daily_start_equity = 10000.0
        s._daily_peak_equity = 10000.0
        s._current_date = "2026-06-15"
        for _ in range(12):
            s._equity_series.append(10000.0)
            s._equity_timestamps.append(time.time())
        s._equity_series.append(9750.0)
        s._equity_timestamps.append(time.time())
        s._daily_drawdown = (10000.0 - 9750.0) / 10000.0
        # 行为: 回撤超限 → 硬止损触发
        assert not s.is_hard_stop_triggered(), "触发前硬止损应为False"
        s._check_daily_drawdown()
        assert s.is_hard_stop_triggered(), "回撤超限后硬止损应触发"
        assert s.is_new_open_blocked(), "硬止损触发后开仓应被阻断"

    def test_5pct_drawdown_triggers_hard_stop(self):
        """日回撤达到5% → 硬止损触发(即使5日均值为0)"""
        s = SafetyMetaLayer()
        s._prev_5day_avg_profit = 0
        s._daily_start_equity = 10000.0
        s._daily_peak_equity = 10000.0
        s._current_date = "2026-06-15"
        s._daily_drawdown = 0.05
        for _ in range(12):
            s._equity_series.append(9500.0)
            s._equity_timestamps.append(time.time())
        s._check_daily_drawdown()
        assert s.is_hard_stop_triggered(), "5%回撤应触发硬止损"

    def test_hard_stop_no_retrigger(self):
        """硬止损已触发 → 不会重复触发(计数器不变)"""
        s = SafetyMetaLayer()
        s._daily_hard_stop_triggered = True
        s._stats["daily_drawdown_triggers"] = 1
        old_triggers = s._stats["daily_drawdown_triggers"]
        s._check_daily_drawdown()
        assert s._stats["daily_drawdown_triggers"] == old_triggers, \
            "已触发的硬止损不应重复计数"

    def test_hard_stop_not_triggered_initially(self):
        """初始状态 → 硬止损未触发, 开仓未被阻断"""
        s = SafetyMetaLayer()
        assert not s.is_hard_stop_triggered()
        assert not s.is_new_open_blocked()


# ============================================================================
# 日硬止损恢复行为
# ============================================================================

class TestDailyResumeBehavior:
    """验证: confirm_daily_resume → 恢复交易权限"""

    def test_resume_clears_hard_stop(self):
        """确认恢复 → 硬止损解除, 开仓恢复"""
        s = SafetyMetaLayer()
        s._daily_hard_stop_triggered = True
        s._daily_new_open_blocked = True
        # 行为: 恢复前 → 硬止损激活
        assert s.is_hard_stop_triggered()
        assert s.is_new_open_blocked()
        # 行为: 恢复后 → 硬止损解除
        result = s.confirm_daily_resume(
            caller_id="MANUAL_test",
            approval_context={"approver_id": "test_approver"},
        )
        assert result is True, "恢复操作应成功"
        assert not s.is_hard_stop_triggered(), "恢复后硬止损应解除"
        assert not s.is_new_open_blocked(), "恢复后开仓应恢复"

    def test_resume_clears_drawdown(self):
        """确认恢复 → 日回撤值归零"""
        s = SafetyMetaLayer()
        s._daily_hard_stop_triggered = True
        s._daily_new_open_blocked = True
        s._daily_drawdown = 0.06
        s.confirm_daily_resume(
            caller_id="MANUAL_test",
            approval_context={"approver_id": "test_approver"},
        )
        assert s._daily_drawdown == 0.0, "恢复后日回撤应归零"

    def test_resume_noop_when_not_triggered(self):
        """未触发硬止损时恢复 → 返回False, 状态不变"""
        s = SafetyMetaLayer()
        result = s.confirm_daily_resume()
        assert result is False, "未触发时恢复应返回False"
        assert not s.is_hard_stop_triggered()


# ============================================================================
# 硬止损平仓豁免行为
# ============================================================================

class TestHardStopCloseExemptionBehavior:
    """验证: 硬止损期间平仓仍被允许(保护性操作豁免)"""

    def _make_risk_service(self):
        return RiskService(params={})

    def test_hard_stop_blocks_open(self):
        """硬止损触发 → 开仓信号被阻断"""
        svc = self._make_risk_service()
        safety = get_safety_meta_layer(svc.params)
        safety._daily_hard_stop_triggered = True
        safety._daily_new_open_blocked = True
        open_signal = {"action": "OPEN", "symbol": "test", "amount": 1}
        result = svc._check_safety_meta_layer(open_signal)
        assert result.is_block, "硬止损期间开仓应被阻断"

    def test_hard_stop_allows_close(self):
        """硬止损触发 → 平仓信号仍被允许"""
        svc = self._make_risk_service()
        safety = get_safety_meta_layer(svc.params)
        safety._daily_hard_stop_triggered = True
        close_signal = {"action": "CLOSE", "symbol": "test", "amount": 1}
        result = svc._check_safety_meta_layer(close_signal)
        assert not result.is_block, "硬止损期间平仓应被允许"

    def test_new_open_blocked_still_allows_close(self):
        """开仓被阻断(非硬止损) → 平仓仍被允许"""
        svc = self._make_risk_service()
        safety = get_safety_meta_layer(svc.params)
        safety._daily_hard_stop_triggered = False
        safety._daily_new_open_blocked = True
        close_signal = {"action": "CLOSE", "symbol": "test", "amount": 1}
        result = svc._check_safety_meta_layer(close_signal)
        assert not result.is_block, "开仓阻断时平仓应被允许"

    def test_can_close_always_true(self):
        """SafetyMetaLayer.can_close() 始终返回True"""
        s = SafetyMetaLayer()
        assert s.can_close() is True
        s._daily_hard_stop_triggered = True
        assert s.can_close() is True


# ============================================================================
# 跨日状态保持行为
# ============================================================================

class TestCrossDayStateBehavior:
    """验证: 新交易日不自动重置硬止损状态"""

    def test_new_day_keeps_hard_stop(self):
        """新交易日on_equity_update → 硬止损状态保持"""
        s = SafetyMetaLayer()
        s._daily_hard_stop_triggered = True
        s._daily_new_open_blocked = True
        s._current_date = "2026-06-14"
        s.on_equity_update(10000.0)
        assert s.is_hard_stop_triggered(), "新交易日不应自动重置硬止损"

    def test_hard_stop_skips_further_checks(self):
        """硬止损激活后 → 后续权益更新跳过熔断和回撤检查"""
        s = SafetyMetaLayer()
        s._daily_hard_stop_triggered = True
        s._daily_start_equity = 10000.0
        s._current_date = "2026-06-15"
        old_cb_triggers = s._stats["circuit_breaker_triggers"]
        old_dd_triggers = s._stats["daily_drawdown_triggers"]
        for _ in range(15):
            s.on_equity_update(9500.0)
        assert s._stats["circuit_breaker_triggers"] == old_cb_triggers, \
            "硬止损期间不应触发熔断"
        assert s._stats["daily_drawdown_triggers"] == old_dd_triggers, \
            "硬止损期间不应触发回撤检查"


# ============================================================================
# 属性委托行为
# ============================================================================

class TestDelegationBehavior:
    """验证: __getattr__/__setattr__ 正确代理到子服务"""

    def test_getattr_delegates_to_drawdown_monitor(self):
        """读取委托属性 → 从 DrawdownMonitorService 获取"""
        s = SafetyMetaLayer()
        s._drawdown_monitor_svc._daily_hard_stop_triggered = True
        assert s._daily_hard_stop_triggered is True, \
            "__getattr__应代理到_drawdown_monitor_svc"

    def test_setattr_delegates_to_drawdown_monitor(self):
        """写入委托属性 → 写入 DrawdownMonitorService"""
        s = SafetyMetaLayer()
        s._daily_drawdown = 0.05
        assert s._drawdown_monitor_svc._daily_drawdown == 0.05, \
            "__setattr__应代理到_drawdown_monitor_svc"

    def test_getattr_unknown_raises(self):
        """读取非委托属性 → 抛出AttributeError"""
        s = SafetyMetaLayer()
        with pytest.raises(AttributeError, match="no attribute"):
            _ = s._nonexistent_delegated_attr


# ============================================================================
# 统计和健康状态行为
# ============================================================================

class TestStatsAndHealthBehavior:
    """验证: get_stats/get_health_status 反映当前状态"""

    def test_stats_reflect_hard_stop(self):
        """硬止损触发 → stats中hard_stop_triggered为True"""
        s = SafetyMetaLayer()
        s._daily_hard_stop_triggered = True
        stats = s.get_stats()
        assert stats["hard_stop_triggered"] is True, \
            "stats应反映硬止损状态"

    def test_stats_initial_values(self):
        """初始状态 → stats中所有触发计数为0"""
        s = SafetyMetaLayer()
        stats = s.get_stats()
        assert stats["hard_stop_triggered"] is False
        assert stats["daily_drawdown_triggers"] == 0

    def test_health_status_structure(self):
        """get_health_status → 包含必要字段"""
        s = SafetyMetaLayer()
        health = s.get_health_status()
        # health_status 是一个dict, 至少包含 health_status 字段
        assert isinstance(health, dict)
        assert "health_status" in health


# ============================================================================
# 全局单例工厂行为
# ============================================================================

class TestSingletonFactoryBehavior:
    """验证: get_safety_meta_layer 单例行为"""

    def _ensure_strategy_layers_dict(self):
        """确保 _strategy_safety_layers 是 dict(conftest可能将其重置为None)"""
        import ali2026v3_trading.risk.risk_circuit_breaker as _cb_mod
        if _cb_mod._strategy_safety_layers is None:
            _cb_mod._strategy_safety_layers = {}

    def test_same_params_returns_same_instance(self):
        """相同参数 → 返回同一实例"""
        s1 = get_safety_meta_layer(params={})
        s2 = get_safety_meta_layer(params={})
        assert s1 is s2, "全局单例应返回同一实例"

    def test_strategy_id_creates_separate_instance(self):
        """不同strategy_id → 返回不同实例"""
        self._ensure_strategy_layers_dict()
        s1 = get_safety_meta_layer(params={}, strategy_id="strategy_A")
        s2 = get_safety_meta_layer(params={}, strategy_id="strategy_B")
        assert s1 is not s2, "不同策略ID应返回不同实例"

    def test_get_risk_circuit_breaker_calls_same_logic(self):
        """get_risk_circuit_breaker 调用 get_safety_meta_layer 相同逻辑"""
        # get_risk_circuit_breaker 是独立函数, 内部调用 get_safety_meta_layer
        result = get_risk_circuit_breaker(params={})
        assert result is not None, "get_risk_circuit_breaker应返回实例"

    def test_cleanup_removes_strategy_instance(self):
        """cleanup_safety_layer → 策略级实例被移除"""
        self._ensure_strategy_layers_dict()
        get_safety_meta_layer(params={}, strategy_id="to_cleanup")
        cleanup_safety_layer("to_cleanup")
        # 再次获取应创建新实例
        s_new = get_safety_meta_layer(params={}, strategy_id="to_cleanup")
        assert s_new is not None
