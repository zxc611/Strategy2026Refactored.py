# MODULE_ID: M2-342
"""测试 strategy/strategy_ecosystem/services.py — regression_check + emergency_degrade"""
import threading
import time

import pytest


def _make_ops_service():
    """创建一个可测试的OpsService实例"""
    from ali2026v3_trading.strategy.strategy_ecosystem.services import OpsService
    from ali2026v3_trading.strategy.strategy_ecosystem._models import SlotState, StrategySlot

    svc = OpsService()
    svc._lock = threading.RLock()
    svc._master = StrategySlot(strategy_id='master', strategy_type='resonance', state=SlotState.ACTIVE, capital_allocation=0.4)
    svc._reverse = StrategySlot(strategy_id='reverse', strategy_type='reversal', state=SlotState.ACTIVE, capital_allocation=0.2)
    svc._other = StrategySlot(strategy_id='other', strategy_type='scalp', state=SlotState.ACTIVE, capital_allocation=0.15)
    svc._spring = StrategySlot(strategy_id='spring', strategy_type='spring', state=SlotState.ACTIVE, capital_allocation=0.1)
    svc._arbitrage = StrategySlot(strategy_id='arbitrage', strategy_type='arbitrage', state=SlotState.STANDBY, capital_allocation=0.1)
    svc._market_making = StrategySlot(strategy_id='market_making', strategy_type='market_making', state=SlotState.STANDBY, capital_allocation=0.05)
    return svc


# ============================================================
# regression_check
# ============================================================

class TestRegressionCheck:
    """OpsService.regression_check: 回归检查"""

    def test_passes_with_valid_state(self):
        """合法状态通过回归检查"""
        svc = _make_ops_service()
        result = svc.regression_check()
        assert result['passed'] is True
        assert len(result['violations']) == 0

    def test_fails_on_bad_allocation(self):
        """资金分配总和偏差>5%时失败"""
        svc = _make_ops_service()
        svc._master.capital_allocation = 0.9
        result = svc.regression_check()
        assert result['passed'] is False
        assert any('资金分配总和' in v for v in result['violations'])

    def test_fails_on_no_active(self):
        """无活跃策略时失败"""
        from ali2026v3_trading.strategy.strategy_ecosystem._models import SlotState
        svc = _make_ops_service()
        for slot in [svc._master, svc._reverse, svc._other,
                     svc._spring, svc._arbitrage, svc._market_making]:
            slot.state = SlotState.STANDBY
            slot.paused = True
        result = svc.regression_check()
        assert result['passed'] is False
        assert any('无活跃策略' in v for v in result['violations'])

    def test_fails_on_negative_ev_with_trades(self):
        """活跃策略EV<0且有交易时失败"""
        svc = _make_ops_service()
        svc._master.expected_value = -1.0
        svc._master.total_pnl = -100
        svc._master.consecutive_losses = 5
        result = svc.regression_check()
        assert result['passed'] is False
        assert any('期望值' in v for v in result['violations'])

    def test_negative_ev_no_trades_passes(self):
        """活跃策略EV<0但无交易时不触发"""
        svc = _make_ops_service()
        svc._master.expected_value = -1.0
        svc._master.total_pnl = 0
        svc._master.consecutive_losses = 0
        result = svc.regression_check()
        # total_pnl==0 and consecutive_losses<=3，不触发
        assert not any('期望值' in v for v in result['violations'])

    def test_checked_invariants_count(self):
        """checked_invariants计数"""
        svc = _make_ops_service()
        result = svc.regression_check()
        assert result['checked_invariants'] > 0

    def test_result_structure(self):
        """结果结构完整"""
        svc = _make_ops_service()
        result = svc.regression_check()
        assert 'passed' in result
        assert 'violations' in result
        assert 'checked_invariants' in result

    def test_allocation_within_tolerance_passes(self):
        """资金分配偏差在5%以内通过"""
        svc = _make_ops_service()
        svc._master.capital_allocation = 0.42
        svc._reverse.capital_allocation = 0.20
        svc._other.capital_allocation = 0.15
        svc._spring.capital_allocation = 0.10
        svc._arbitrage.capital_allocation = 0.10
        svc._market_making.capital_allocation = 0.05
        result = svc.regression_check()
        # 总和=1.02，偏差0.02 < 0.05
        assert not any('资金分配总和' in v for v in result['violations'])


# ============================================================
# emergency_degrade
# ============================================================

class TestEmergencyDegrade:
    """OpsService.emergency_degrade: 紧急降级"""

    def _make_active_service(self):
        """创建所有策略都活跃的service"""
        from ali2026v3_trading.strategy.strategy_ecosystem.services import OpsService
        from ali2026v3_trading.strategy.strategy_ecosystem._models import SlotState, StrategySlot

        svc = OpsService()
        svc._lock = threading.RLock()
        svc._master = StrategySlot(strategy_id='master', strategy_type='resonance', state=SlotState.ACTIVE, capital_allocation=0.4)
        svc._reverse = StrategySlot(strategy_id='reverse', strategy_type='reversal', state=SlotState.ACTIVE, capital_allocation=0.2)
        svc._other = StrategySlot(strategy_id='other', strategy_type='scalp', state=SlotState.ACTIVE, capital_allocation=0.15)
        svc._spring = StrategySlot(strategy_id='spring', strategy_type='spring', state=SlotState.ACTIVE, capital_allocation=0.1)
        svc._arbitrage = StrategySlot(strategy_id='arbitrage', strategy_type='arbitrage', state=SlotState.ACTIVE, capital_allocation=0.1)
        svc._market_making = StrategySlot(strategy_id='market_making', strategy_type='market_making', state=SlotState.ACTIVE, capital_allocation=0.05)
        svc._get_slot = lambda sid: {
            'master': svc._master, 'reverse': svc._reverse, 'other': svc._other,
            'spring': svc._spring, 'arbitrage': svc._arbitrage, 'market_making': svc._market_making,
        }.get(sid)
        return svc

    def test_degrade_reduces_active_count(self):
        """降级减少活跃策略数量"""
        svc = self._make_active_service()
        result = svc.emergency_degrade(target_active_count=2, caller_id='test')
        assert len(result['degraded_strategies']) > 0

    def test_degrade_sets_paused(self):
        """被降级策略标记为paused"""
        from ali2026v3_trading.strategy.strategy_ecosystem._models import SlotState
        svc = self._make_active_service()
        svc.emergency_degrade(target_active_count=1, caller_id='test')
        paused_count = sum(1 for s in [svc._reverse, svc._other, svc._spring, svc._arbitrage, svc._market_making]
                          if s.paused)
        assert paused_count > 0

    def test_degrade_sets_standby_state(self):
        """被降级策略状态变为STANDBY"""
        from ali2026v3_trading.strategy.strategy_ecosystem._models import SlotState
        svc = self._make_active_service()
        svc.emergency_degrade(target_active_count=1, caller_id='test')
        standby_count = sum(1 for s in [svc._reverse, svc._other, svc._spring, svc._arbitrage, svc._market_making]
                          if s.state == SlotState.STANDBY)
        assert standby_count > 0

    def test_degrade_result_has_timestamp(self):
        """结果包含时间戳"""
        svc = self._make_active_service()
        result = svc.emergency_degrade(target_active_count=1, caller_id='test')
        assert 'timestamp' in result
        assert 'caller_id' in result
        assert result['caller_id'] == 'test'

    def test_degrade_no_need_when_within_target(self):
        """活跃数<=目标时不需要降级"""
        svc = self._make_active_service()
        result = svc.emergency_degrade(target_active_count=6, caller_id='test')
        assert len(result['degraded_strategies']) == 0

    def test_degrade_result_per_strategy(self):
        """结果包含每个策略的处理结果"""
        svc = self._make_active_service()
        result = svc.emergency_degrade(target_active_count=2, caller_id='test')
        assert 'result_per_strategy' in result

    def test_degrade_sets_pause_reason(self):
        """被降级策略有暂停原因"""
        svc = self._make_active_service()
        svc.emergency_degrade(target_active_count=1, caller_id='test', reason='test_reason')
        for s in [svc._reverse, svc._other, svc._spring, svc._arbitrage, svc._market_making]:
            if s.paused:
                assert 'EMERGENCY_DEGRADE' in s.pause_reason

    def test_degrade_active_strategies_remaining(self):
        """降级后剩余活跃策略数"""
        svc = self._make_active_service()
        result = svc.emergency_degrade(target_active_count=2, caller_id='test')
        assert len(result['active_strategies']) >= 1


# ============================================================
# compute_defense_net_benefit
# ============================================================

class TestComputeDefenseNetBenefit:
    """compute_defense_net_benefit: 防御净收益量化"""

    def test_positive_net_benefit(self):
        """净收益为正时should_defend=True"""
        from ali2026v3_trading.strategy.strategy_ecosystem.services import compute_defense_net_benefit
        result = compute_defense_net_benefit(
            defense_mode_loss=100, ignore_mode_loss=500,
            ignore_mode_win_trades=10, ignore_mode_avg_win=50,
            other_state_ratio=0.2,
        )
        assert result['should_defend'] is True
        assert result['net_benefit'] > 0

    def test_negative_net_benefit(self):
        """净收益为负时should_defend=False"""
        from ali2026v3_trading.strategy.strategy_ecosystem.services import compute_defense_net_benefit
        result = compute_defense_net_benefit(
            defense_mode_loss=400, ignore_mode_loss=500,
            ignore_mode_win_trades=100, ignore_mode_avg_win=50,
            other_state_ratio=0.2,
        )
        assert result['should_defend'] is False

    def test_result_structure(self):
        """结果结构完整"""
        from ali2026v3_trading.strategy.strategy_ecosystem.services import compute_defense_net_benefit
        result = compute_defense_net_benefit(100, 200, 10, 50, 0.2)
        assert 'defense_mode_loss' in result
        assert 'ignore_mode_loss' in result
        assert 'defense_opportunity_cost' in result
        assert 'net_benefit' in result
        assert 'should_defend' in result
        assert 'defense_savings' in result

    def test_zero_opportunity_cost(self):
        """零机会成本"""
        from ali2026v3_trading.strategy.strategy_ecosystem.services import compute_defense_net_benefit
        result = compute_defense_net_benefit(100, 200, 0, 50, 0.2)
        assert result['defense_opportunity_cost'] == 0.0
        assert result['should_defend'] is True
