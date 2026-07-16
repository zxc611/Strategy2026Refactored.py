# MODULE_ID: M1-075
"""
mode_position_sizing - SixDimPositionAdjustmentFactor + kelly_fraction + kelly_position_size + PredictiveStateEngine

仓位计算与预测状态 - 文档12.3.5拆分方案

Split from mode_engine.py - P2 split
"""
from __future__ import annotations

import logging
import math
import threading
import time
from typing import Any, Dict, List, Optional

from governance.mode_config import ModeConfig
from config.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_OTHER,
)

class SixDimPositionAdjustmentFactor:
    """六维仓位调整因子 — 将风险指标、订单流、希腊字母内化为仓位乘数

    六维体系:
    L1 风险收益层 (三角验证): Sortino + Calmar + Sharpe
    L2 市场微观层 (订单流): OFI + CVD偏离 + 智能资金流向
    L3 期权风险层 (希腊字母): Delta暴露 + Gamma风险 + Theta衰减 + Vega波动
    """

    def compute_adjustment(
        self,
        config: ModeConfig,
        sortino: float = 0.0,
        calmar: float = 0.0,
        sharpe: float = 0.0,
        ofi_score: float = 0.0,
        cvd_divergence: float = 0.0,
        smart_money_flow: float = 0.0,
        delta_exposure: float = 0.0,
        gamma_risk: float = 0.0,
        theta_decay: float = 0.0,
        vega_exposure: float = 0.0,
        market_state: str = "",  # P-28补全修复: 传递市场状态到。sigmoid_adjust
    ) -> float:
        """计算六维仓位调整因子 [0, 1]"""
        if not config.tvf_enabled:
            return 1.0

        # === L1: 三角验证因子 ===
        sortino_factor = self._sigmoid_adjust(
            sortino, config.tvf_sortino_threshold, config.tvf_sortino_scale,
            market_state=market_state  # P-28补全修复
        )
        calmar_factor = self._sigmoid_adjust(
            calmar, config.tvf_calmar_threshold, config.tvf_calmar_scale,
            market_state=market_state  # P-28补全修复
        )
        sharpe_factor = self._sigmoid_adjust(
            sharpe, config.tvf_sharpe_threshold, config.tvf_sharpe_scale,
            market_state=market_state  # P-28补全修复
        )
        l1_tvf = (
            config.tvf_l1_inner_sortino_weight * sortino_factor
            + config.tvf_l1_inner_calmar_weight * calmar_factor
            + config.tvf_l1_inner_sharpe_weight * sharpe_factor
        )

        # === L2: 订单流因子 ===
        ofi_norm = 1.0 / (1.0 + math.exp(-ofi_score / config.tvf_ofi_scale))
        cvd_factor = min(1.0, max(0.0, 0.5 + cvd_divergence * 0.5))
        smf_norm = 1.0 / (1.0 + math.exp(-smart_money_flow / config.tvf_smf_scale))
        l2_tvf = (
            config.tvf_l2_inner_ofi_weight * ofi_norm
            + config.tvf_l2_inner_cvd_weight * cvd_factor
            + config.tvf_l2_inner_smf_weight * smf_norm
        )

        # === L3: 希腊字母因子 ===
        delta_factor = 1.0 - abs(delta_exposure)
        gamma_factor = 1.0 / (
            1.0 + math.exp((gamma_risk - config.tvf_gamma_threshold) / config.tvf_gamma_scale)
        )
        theta_factor = 1.0 / (
            1.0 + math.exp((theta_decay - config.tvf_theta_threshold) / config.tvf_theta_scale)
        )
        vega_factor = 1.0 / (
            1.0 + math.exp((vega_exposure - config.tvf_vega_threshold) / config.tvf_vega_scale)
        )
        l3_tvf = (
            config.tvf_l3_inner_delta_weight * delta_factor
            + config.tvf_l3_inner_gamma_weight * gamma_factor
            + config.tvf_l3_inner_theta_weight * theta_factor
            + config.tvf_l3_inner_vega_weight * vega_factor
        )

        # === 六维综合 ===
        final_tvf = (
            config.tvf_l1_weight * l1_tvf
            + config.tvf_l2_weight * l2_tvf
            + config.tvf_l3_weight * l3_tvf
        )
        return min(1.0, max(0.0, final_tvf))

    @staticmethod
    def _sigmoid_adjust(value: float, threshold: float, scale: float,
                        market_state: str = "") -> float:
        """Sigmoid调整: 阈值处=0.5, 2倍阈值处≈0.88
        
        P-28修复: 市场状态自适应scale — 高波动市场scale放大(更保守)，低波动市场scale缩小(更激进)
        R21-MATH-P2-04修复: 添加输入裁剪防止exp溢出
        """
        # P-28修复: 市场状态自适应scale调整
        _adaptive_scale = scale
        if market_state == STRATEGY_MODE_CORRECT_TRENDING:  # R25-SE-P1-02-FIX
            _adaptive_scale = scale * 0.8  # 趋势市场更激进
        elif market_state == STRATEGY_MODE_INCORRECT_REVERSAL:  # R25-SE-P1-02-FIX
            _adaptive_scale = scale * 1.2  # 反转市场更保守
        elif market_state == STRATEGY_MODE_OTHER:  # R25-SE-P1-02-FIX
            _adaptive_scale = scale * 1.5  # 震荡市场最保守
        # R21-MATH-P2-04修复: 裁剪指数参数防止exp溢出
        _exp_arg = -(value - threshold) / _adaptive_scale if abs(_adaptive_scale) > 1e-10 else (-500.0 if value < threshold else 500.0)
        _exp_arg = max(-500.0, min(500.0, _exp_arg))
        return 1.0 / (1.0 + math.exp(_exp_arg))


def kelly_fraction(win_rate: float, win_loss_ratio: float, fraction: float = 1.0,
                   cost_ratio: float = 0.0) -> float:
    """Kelly公式（含交易成本调整）

    R17-07修复: 交易成本从赢利中扣除，公式:
      kelly = (win_rate * (win_loss_ratio - cost_ratio) - (1 - win_rate)) / (win_loss_ratio - cost_ratio)

    Args:
        win_rate: 胜率 (0~1)
        win_loss_ratio: 盈亏比
        fraction: Kelly比例系数 (0~1)
        cost_ratio: 单位交易成本占赌注比例 (默认0)

    Returns:
        float: Kelly仓位比例 [0, 1]
    """
    if win_rate <= 0 or win_rate >= 1 or win_loss_ratio <= 0:
        return 0.0
    # 扣除交易成本后的有效盈亏比
    effective_odds = win_loss_ratio - cost_ratio
    if effective_odds <= 0:
        return 0.0
    kelly = (win_rate * effective_odds - (1 - win_rate)) / effective_odds
    return max(0.0, kelly * fraction)

def kelly_position_size(
    equity: float, win_rate: float, win_loss_ratio: float,
    entry_price: float, stop_price: float,
    kelly_fraction_param: float = 0.33, max_cap: float = 0.08,
    cost_ratio: float = 0.0,
    instrument_type: str = "future", option_discount: float = 0.6,
) -> float:
    """Kelly仓位计算（含交易成本调整 + 期权买方折扣）

    R17-07修复: cost_ratio传递给kelly_fraction进行成本扣除
    R7-M-02修复: instrument_type + option_discount 期权买方仓位打折(手册第854行要求0.5~0.7)
    """
    if equity <= 0 or kelly_fraction_param <= 0 or entry_price <= 0 or stop_price <= 0:
        return 0.0
    kf = kelly_fraction(win_rate, win_loss_ratio, kelly_fraction_param, cost_ratio=cost_ratio)
    if kf <= 0:
        return 0.0
    # 期权买方应用折扣系数（手册第854行: 半凯利×折扣(0.5~0.7)）
    if instrument_type == "option_buyer" and option_discount > 0:
        kf *= option_discount
    risk_per_share = abs(entry_price - stop_price)
    if risk_per_share < 1e-10:
        return 0.0
    risk_amount = equity * kf
    shares = risk_amount / risk_per_share
    max_shares = equity * max_cap / entry_price
    return min(shares, max_shares)

class PredictiveStateEngine:
    """方向化细分预测状态引擎

    将传统二元状态(correct/incorrect)细分为四方向:
    - correct_rise: 正确预判+价格上涨 → 满仓进攻
    - correct_fall: 正确预判+价格下跌 → 满仓进攻
    - wrong_rise: 错误预判+价格上涨 → 减仓防守
    - wrong_fall: 错误预判+价格下跌 → 减仓防守

    用于ModeEngine仓位调整的方向化因子计算。
    """

    _instance: Optional['PredictiveStateEngine'] = None
    # R21-CC01修复: Lock→RLock，防止单例get_instance()重入死锁
    _lock = threading.RLock()

    @classmethod
    def get_instance(cls) -> 'PredictiveStateEngine':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        with cls._lock:
            cls._instance = None

    def __init__(self):
        self._state_counts: Dict[str, int] = {
            "correct_rise": 0, "correct_fall": 0,
            "wrong_rise": 0, "wrong_fall": 0,
        }
        self._position_multipliers: Dict[str, float] = {
            "correct_rise": 1.0, "correct_fall": 1.0,
            "wrong_rise": 0.5, "wrong_fall": 0.5,
        }
        self._transition_history: List[Dict[str, Any]] = []
        self._max_history: int = 1000

    def classify_state(self, prediction_correct: bool, price_direction: int) -> str:
        """将预测正确性+价格方向分类为四方向状态

        Args:
            prediction_correct: 预测是否正确
            price_direction: 价格变化方向 (1=上涨, -1=下跌)

        Returns:
            str: correct_rise/correct_fall/wrong_rise/wrong_fall
        """
        if prediction_correct:
            return "correct_rise" if price_direction > 0 else "correct_fall"
        else:
            return "wrong_rise" if price_direction > 0 else "wrong_fall"

    def get_position_multiplier(self, state: str) -> float:
        """获取对应方向的仓位调整乘数

        Args:
            state: 四方向状态名

        Returns:
            float: 仓位乘数 (0.5~1.0)
        """
        return self._position_multipliers.get(state, 0.75)

    def record_transition(self, from_state: str, to_state: str, pnl: float = 0.0) -> None:
        """记录状态转换事件

        Args:
            from_state: 源状态
            to_state: 目标状态
            pnl: 本次转换对应的盈亏
        """
        self._state_counts[to_state] = self._state_counts.get(to_state, 0) + 1
        logging.info("[PSE] LOG-P1-02: 状态转换 %s→%s, pnl=%.2f", from_state, to_state, pnl)
        self._transition_history.append({
            "from": from_state, "to": to_state, "pnl": pnl,
            "timestamp": time.time(),
        })
        if len(self._transition_history) > self._max_history:
            self._transition_history = self._transition_history[-self._max_history:]
        # DFG-P1-03修复: PredictiveStateEngine状态转换结果通过EventBus传播
        try:
            from infra.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _bus.publish('pse.state_transition', {
                    'type': 'pse.state_transition',
                    'from_state': from_state,
                    'to_state': to_state,
                    'pnl': pnl,
                    'position_multiplier': self._position_multipliers.get(to_state, 0.75),
                }, async_mode=True)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _bus_err:
            logging.warning('[R22-EP-06] state_transition事件发布失败: %s', _bus_err)

    def get_state_stats(self) -> Dict[str, Any]:
        """获取四方向状态统计"""
        total = max(sum(self._state_counts.values()), 1)
        return {
            "counts": dict(self._state_counts),
            "ratios": {k: v / total for k, v in self._state_counts.items()},
            "correct_ratio": (self._state_counts["correct_rise"] + self._state_counts["correct_fall"]) / total,
            "wrong_ratio": (self._state_counts["wrong_rise"] + self._state_counts["wrong_fall"]) / total,
        }
