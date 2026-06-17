# MODULE_ID: M1-074
"""
mode_exit_rules - ExitRuleEngine + DrawdownManager + DefensiveDrawdownChecker

退出规则与回撤管理 - 文档12.3.5拆分方案

Split from mode_engine.py - P2 split
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from ali2026v3_trading.governance.mode_config import TakeProfitMethod, StopLossMethod, DrawdownAction

class ExitRuleEngine:
    def __init__(self, method: TakeProfitMethod, stop_method: StopLossMethod):
        self._tp_method = method
        self._sl_method = stop_method

    @property
    def take_profit_method(self) -> TakeProfitMethod:
        return self._tp_method

    @property
    def stop_loss_method(self) -> StopLossMethod:
        return self._sl_method

    def compute_take_profit_levels(
        self, entry_price: float, volatility_1d: float, size: float,
        direction: str = 'BUY',
    ) -> List[Dict[str, Any]]:
        """计算止盈价位，支持BUY和SELL两个方向"""
        sign = 1.0 if direction == 'BUY' else -1.0
        if self._tp_method == TakeProfitMethod.TIERED:
            return [
                {'pct': 0.50, 'price': entry_price + sign * volatility_1d, 'volume_ratio': 0.50},
                {'pct': 0.30, 'price': entry_price + sign * 2 * volatility_1d, 'volume_ratio': 0.30},
                {'pct': 0.20, 'price': None, 'volume_ratio': 0.20},
            ]
        elif self._tp_method == TakeProfitMethod.TRAILING:
            return [
                {'activation': entry_price * (1.05 if direction == 'BUY' else 0.95), 'trail_pct': 0.10},
            ]
        else:
            return [
                {'price': entry_price * (1.10 if direction == 'BUY' else 0.90), 'volume_ratio': 1.0},
            ]

    def compute_stop_loss(
        self, entry_price: float, stop_distance: float, volatility_20d: float = 0.0,
        direction: str = 'BUY',
    ) -> Dict[str, Any]:
        """计算止损价位，支持BUY和SELL两个方向"""
        sign = 1.0 if direction == 'BUY' else -1.0
        if self._sl_method == StopLossMethod.VOLATILITY and volatility_20d > 0:
            sl_distance = max(stop_distance, entry_price * volatility_20d * 1.5)
            return {'method': 'volatility', 'price': entry_price - sign * sl_distance, 'distance': sl_distance}
        elif self._sl_method == StopLossMethod.TIME_DECAY:
            sl_distance = stop_distance
            return {'method': 'time_decay', 'price': entry_price - sign * sl_distance, 'distance': sl_distance}
        else:
            return {'method': 'fixed', 'price': entry_price - sign * stop_distance, 'distance': stop_distance}

# [P2-07-DM] 职责: 退出规则驱动的回撤决策(VaR/ATR升级+仓位缩减/暂停/全停阈值)
# 与risk/safety_meta_equity.py的DrawdownMonitorService分工: DM=规则决策, DMS=权益监控+日回撤检测
class DrawdownManager:
    """回撤管理器 — 含VaR/ATR动态升级机制（R7-M-01修复）

    手册第268行要求："动态升级采用历史滚动VaR(95%)或ATR为基准"
    当VaR或ATR超过阈值时，自动收紧仓位缩减/暂停/停止条件。

    P2-12 优先级规则:
        风控层硬停止（DrawdownMonitorService）优先级高于治理层策略性回撤管理（DrawdownManager）。
        DrawdownManager 的决策应先检查 DrawdownMonitorService 的硬停止状态，
        若硬停止已触发，DrawdownManager 的所有决策方法应直接返回最保守结果。
    """

    def __init__(self, action: DrawdownAction, recovery_target: float = 1.0,
                 var_confidence: float = 0.95, var_upgrade_threshold: float = 0.02,
                 atr_upgrade_threshold: float = 0.015):
        self._action = action
        self._recovery_target = recovery_target
        self._current_drawdown: float = 0.0
        self._drawdown_low: float = 0.0
        self._recovery_start_equity: float = 0.0
        # R7-M-01修复: VaR/ATR动态升级标志
        self._var_upgrade_active: bool = False
        self._atr_upgrade_active: bool = False
        self._var_confidence: float = var_confidence
        self._var_upgrade_threshold: float = var_upgrade_threshold
        self._atr_upgrade_threshold: float = atr_upgrade_threshold
        self._last_var_value: float = 0.0
        self._last_atr_value: float = 0.0
        # P2-12: 风控层硬停止状态引用
        self._daily_hard_stop_triggered: bool = False
        # P2-07修复: 订阅EventBus日回撤硬停止事件，自动同步DrawdownMonitorService状态
        self._subscribe_daily_hard_stop_event()

    def set_daily_hard_stop(self, triggered: bool) -> None:
        """P2-12: 设置风控层硬停止状态，由外部DrawdownMonitorService同步"""
        self._daily_hard_stop_triggered = triggered

    def _subscribe_daily_hard_stop_event(self) -> None:
        """P2-07修复: 订阅EventBus DailyDrawdownHaltEvent，自动同步硬停止状态。
        当DrawdownMonitorService触发日回撤硬停止时，DrawdownManager自动收到通知，
        确保治理层决策与风控层硬停止状态一致。
        """
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            bus = get_global_event_bus()
            if bus is not None:
                bus.subscribe_weak('DailyDrawdownHaltEvent', self._on_daily_hard_stop_event)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[P2-07] DrawdownManager EventBus订阅失败(非致命): %s", e)

    def _on_daily_hard_stop_event(self, event: Any) -> None:
        """P2-07修复: EventBus回调——日回撤硬停止事件处理"""
        self._daily_hard_stop_triggered = True
        logging.warning("[P2-07] DrawdownManager收到日回撤硬停止事件，所有决策将返回最保守结果")

    @property
    def action(self) -> DrawdownAction:
        return self._action

    @property
    def recovery_target(self) -> float:
        return self._recovery_target

    @property
    def var_upgrade_active(self) -> bool:
        """R7-M-01修复: VaR动态升级是否激活"""
        return self._var_upgrade_active

    @property
    def atr_upgrade_active(self) -> bool:
        """R7-M-01修复: ATR动态升级是否激活"""
        return self._atr_upgrade_active

    @property
    def last_var_value(self) -> float:
        """最近一次VaR计算值"""
        return self._last_var_value

    @property
    def last_atr_value(self) -> float:
        """最近一次ATR值"""
        return self._last_atr_value

    # [P2-03] 计划统一到risk/的VaR计算函数
    def update_var_baseline(self, returns: List[float], confidence: float = 0.95) -> float:
        """R7-M-01修复: 计算历史滚动VaR并更新升级标志

        手册第268行: "动态升级采用历史滚动VaR(95%)为基准"
        当VaR超过阈值时，激活动态升级 → 收紧should_reduce_size/halt_new/full_stop的触发阈值

        Args:
            returns: 收益率序列
            confidence: VaR置信水平 (默认0.95=95%)

        Returns:
            float: VaR值(正值=潜在损失百分比)
        """
        if len(returns) < 20:
            return 0.0
        sorted_returns = sorted(returns)
        idx = int(len(sorted_returns) * (1.0 - confidence))
        var_value = abs(sorted_returns[idx]) if idx < len(sorted_returns) else abs(sorted_returns[-1])
        self._last_var_value = var_value
        self._var_confidence = confidence
        # VaR超过升级阈值时激活动态升级
        if var_value > self._var_upgrade_threshold:
            self._var_upgrade_active = True
            logging.warning(
                "[R7-M-01] VaR动态升级激活: VaR=%.4f > threshold=%.4f (confidence=%.0f%%)",
                var_value, self._var_upgrade_threshold, confidence * 100)
        else:
            self._var_upgrade_active = False
        return var_value

    def update_atr_baseline(self, atr_value: float) -> None:
        """R7-M-01修复: 更新ATR基准并检查是否激活动态升级

        手册第268行: "或ATR为基准"

        Args:
            atr_value: 当前ATR值(相对于价格的比例，如0.015=1.5%)
        """
        self._last_atr_value = atr_value
        if atr_value > self._atr_upgrade_threshold:
            self._atr_upgrade_active = True
            logging.warning(
                "[R7-M-01] ATR动态升级激活: ATR=%.4f > threshold=%.4f",
                atr_value, self._atr_upgrade_threshold)
        else:
            self._atr_upgrade_active = False

    def update_drawdown(self, current_drawdown: float) -> None:
        self._current_drawdown = current_drawdown
        if current_drawdown < self._drawdown_low:
            self._drawdown_low = current_drawdown

    def should_reduce_size(self) -> bool:
        """R7-M-01修复: VaR/ATR升级时阈值更敏感

        P2-12: 若风控层硬停止已触发，直接返回True（最保守）
        """
        if self._daily_hard_stop_triggered:
            return True
        _dd_active = self._current_drawdown < 0
        if self._var_upgrade_active or self._atr_upgrade_active:
            # VaR/ATR升级: 更早介入减仓 (任何负回撤即触发)
            if _dd_active:
                logging.info("[R7-M-01] VaR/ATR升级模式: should_reduce_size提前触发")
                return True
        return self._action == DrawdownAction.REDUCE_SIZE and _dd_active

    def should_halt_new(self) -> bool:
        """R7-M-01修复: VaR/ATR升级时暂停新开仓阈值收紧

        P2-12: 若风控层硬停止已触发，直接返回True（最保守）
        """
        if self._daily_hard_stop_triggered:
            return True
        _dd_active = self._current_drawdown < 0
        if self._var_upgrade_active or self._atr_upgrade_active:
            # VaR/ATR升级: 更早暂停新开仓 (任何负回撤即触发)
            return _dd_active
        return self._action == DrawdownAction.HALT_NEW and _dd_active

    def should_full_stop(self) -> bool:
        """R7-M-01修复: VaR/ATR升级时全停阈值从-5%收紧到-2%

        P2-12: 若风控层硬停止已触发，直接返回True（最保守）
        """
        if self._daily_hard_stop_triggered:
            return True
        if self._var_upgrade_active or self._atr_upgrade_active:
            # VaR/ATR升级: 全停阈值从-5%收紧到-2%
            if self._current_drawdown < -0.02:
                logging.warning("[R7-M-01] VaR/ATR升级模式: should_full_stop阈值收紧(-5%%→-2%%)")
                return True
            return False
        return self._action == DrawdownAction.FULL_STOP and self._current_drawdown < -0.05

    def is_recovered(self, current_equity: float, peak_equity: float) -> bool:
        if self._drawdown_low >= 0:
            return True
        required_recovery = abs(self._drawdown_low) * self._recovery_target
        actual_recovery = current_equity - (peak_equity + self._drawdown_low)
        return actual_recovery >= required_recovery


class DefensiveDrawdownChecker:
    """防御性减仓检查器 — 当实时风险指标显著低于入场时触发减仓"""

    def check(
        self,
        current_sortino: float,
        current_calmar: float,
        entry_sortino: float,
        entry_calmar: float,
        decay_threshold: float = 0.5,
    ) -> float:
        """
        返回减仓因子 [0, 1]
        1.0 = 不减仓, 0.5 = 减仓50%, 0.0 = 清仓
        """
        if entry_sortino <= 0 or entry_calmar <= 0:
            return 1.0

        sortino_decay = current_sortino / entry_sortino
        calmar_decay = current_calmar / entry_calmar

        if sortino_decay < decay_threshold or calmar_decay < decay_threshold:
            return min(sortino_decay, calmar_decay)

        return 1.0

