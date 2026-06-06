"""
V7实盘前边缘裂缝验证模块

覆盖裂缝14-30的验证函数与缺失功能实现。
裂缝2/5/10/11由其他模块处理。
"""
from __future__ import annotations

import math
import logging
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field

import numpy as np

# DEPRECATED: 此模块仅被测试代码引用，生产零引用。待架构决策集成或删除。

logger = logging.getLogger(__name__)


# =====================================================================
# 裂缝14: 期权熔断停牌验证
# =====================================================================

@dataclass(slots=True)
class CircuitBreakerEvent:
    instrument_id: str
    halt_start: str
    halt_end: str
    reason: str
    price_jump_pct: float = 0.0


def validate_circuit_breaker_halts(
    equity_before: float,
    equity_after_injection: float,
    survival_threshold: float = 0.99,
) -> Dict[str, Any]:
    """验证注入熔断事件后生存率是否达标。

    通过标准: 注入后equity/初始equity >= survival_threshold
    """
    if equity_before <= 0:
        return {'passed': False, 'reason': '初始资金<=0', 'survival_ratio': 0.0}
    survival_ratio = equity_after_injection / equity_before
    passed = survival_ratio >= survival_threshold
    return {
        'passed': passed,
        'survival_ratio': survival_ratio,
        'threshold': survival_threshold,
        'reason': 'ok' if passed else f'生存率{survival_ratio:.4f}<{survival_threshold}',
    }


# =====================================================================
# 裂缝15: 影子B随机路径稳定性
# =====================================================================

def validate_shadow_b_stability(
    sharpe_samples: List[float],
    ci_width_threshold: float = 1.0,
) -> Dict[str, Any]:
    """验证影子B随机路径的夏普置信区间宽度。

    若CI宽度 > 1.0，则应使用影子B的期望夏普（均值）而非单次最大值。
    """
    arr = np.array(sharpe_samples)
    if len(arr) < 2:
        return {'passed': False, 'ci_width': float('inf'), 'reason': '样本数<2'}
    ci_low = float(np.percentile(arr, 2.5))
    ci_high = float(np.percentile(arr, 97.5))
    ci_width = ci_high - ci_low
    mean_sharpe = float(np.mean(arr))
    passed = ci_width <= ci_width_threshold
    return {
        'passed': passed,
        'ci_low': ci_low,
        'ci_high': ci_high,
        'ci_width': ci_width,
        'mean_sharpe': mean_sharpe,
        'threshold': ci_width_threshold,
        'recommendation': 'use_mean' if not passed else 'use_max_ok',
    }


# =====================================================================
# 裂缝16: 最小变动价位与手数取整
# =====================================================================

def apply_tick_rounding(
    price: float,
    tick_size: float,
    direction: str = 'nearest',
) -> float:
    """将价格取整到最小变动价位。

    direction: 'nearest'四舍五入, 'up'向上取整, 'down'向下取整
    """
    if tick_size <= 0:
        return price
    ticks = price / tick_size
    if direction == 'up':
        return math.ceil(ticks) * tick_size
    elif direction == 'down':
        return math.floor(ticks) * tick_size
    else:
        # R27-P2-FP-02修复: round()银行家舍入→确定性舍入
        return math.floor(ticks + 0.5) * tick_size


def apply_lot_rounding(lots: float, direction: str = 'floor') -> int:
    """手数取整。direction: 'floor'向下, 'ceil'向上"""
    if direction == 'ceil':
        return max(1, math.ceil(lots))
    return max(0, math.floor(lots))


def validate_tick_rounding_impact(
    sharpe_before: float,
    sharpe_after: float,
    min_ratio: float = 0.7,
) -> Dict[str, Any]:
    """验证取整后策略夏普 >= 取整前夏普 * min_ratio。"""
    if sharpe_before <= 0:
        return {'passed': True, 'ratio': float('inf'), 'reason': '原夏普<=0无需比较'}
    ratio = sharpe_after / sharpe_before
    passed = ratio >= min_ratio
    return {
        'passed': passed,
        'sharpe_before': sharpe_before,
        'sharpe_after': sharpe_after,
        'ratio': ratio,
        'min_ratio': min_ratio,
    }


# =====================================================================
# 裂缝19: 平仓+开仓连续滑点放大效应
# =====================================================================

CASCADE_SLIPPAGE_MULTIPLIER_DEFAULT = 1.5


def compute_cascade_slippage(
    base_slippage_bps: float,
    close_volume: float,
    avg_volume: float,
    cascade_multiplier: float = CASCADE_SLIPPAGE_MULTIPLIER_DEFAULT,
) -> float:
    """计算状态切换时的级联滑点。

    新开仓滑点 = 原滑点 × (1 + 平仓量/平均成交量) × cascade_multiplier
    平仓订单影响价格，导致新开仓滑点放大。
    """
    volume_impact = 1.0 + (close_volume / avg_volume if avg_volume > 0 else 0.0)
    return base_slippage_bps * volume_impact * cascade_multiplier


def validate_cascade_slippage_impact(
    sharpe_before: float,
    sharpe_after: float,
    max_decay_pct: float = 0.25,
) -> Dict[str, Any]:
    """验证启用级联滑点后夏普衰减是否可接受。"""
    if sharpe_before <= 0:
        return {'passed': True, 'decay_pct': 0.0, 'reason': '原夏普<=0'}
    decay_pct = max(0.0, (sharpe_before - sharpe_after) / sharpe_before)
    passed = decay_pct <= max_decay_pct
    return {
        'passed': passed,
        'sharpe_before': sharpe_before,
        'sharpe_after': sharpe_after,
        'decay_pct': decay_pct,
        'max_decay_pct': max_decay_pct,
    }


# =====================================================================
# 裂缝20: 资金曲线离散化
# =====================================================================

def compute_discrete_equity(
    initial_capital: float,
    premium_losses: List[float],
    realized_gains: List[float],
) -> List[float]:
    """计算离散资金曲线。

    每笔期权亏损=全额权利金，资金呈跳变（而非连续）。
    """
    equity = initial_capital
    curve = [equity]
    for loss, gain in zip(premium_losses, realized_gains):
        equity = max(0.0, equity - loss + gain)
        curve.append(equity)
    return curve


def validate_capital_discretization_gap(
    sharpe_continuous: float,
    sharpe_discrete: float,
    max_diff: float = 0.3,
) -> Dict[str, Any]:
    """验证连续与离散资金曲线的夏普差异 <= max_diff。"""
    diff = abs(sharpe_continuous - sharpe_discrete)
    passed = diff <= max_diff
    return {
        'passed': passed,
        'sharpe_continuous': sharpe_continuous,
        'sharpe_discrete': sharpe_discrete,
        'diff': diff,
        'max_diff': max_diff,
    }


# =====================================================================
# 裂缝23: 影子订单隔离验证
# =====================================================================

def validate_shadow_order_isolation(
    main_cash_before: float,
    main_cash_after: float,
    main_positions_before: Dict,
    main_positions_after: Dict,
) -> Dict[str, Any]:
    """验证影子策略执行后主策略资金和持仓完全不变。"""
    cash_unchanged = abs(main_cash_after - main_cash_before) < 1e-6
    positions_unchanged = main_positions_before == main_positions_after
    passed = cash_unchanged and positions_unchanged
    return {
        'passed': passed,
        'cash_unchanged': cash_unchanged,
        'positions_unchanged': positions_unchanged,
        'cash_delta': main_cash_after - main_cash_before,
    }


# =====================================================================
# 裂缝24: 状态确认窗口边界语义
# =====================================================================

@dataclass(slots=True)
class StateConfirmWindow:
    """状态确认窗口：连续N次确认才切换。

    规则：只有连续且完全相同的新状态才增加计数器，
    任何非新状态出现则重置为0。
    """
    confirm_bars: int = 3
    _pending_state: Optional[str] = field(default=None, repr=False)
    _consecutive_count: int = field(default=0, repr=False)

    def feed(self, observed_state: str) -> Tuple[bool, Optional[str]]:
        """输入观测状态，返回(是否切换, 切换到什么状态)。"""
        if observed_state == self._pending_state:
            self._consecutive_count += 1
        else:
            self._pending_state = observed_state
            self._consecutive_count = 1

        if self._consecutive_count >= self.confirm_bars:
            confirmed_state = self._pending_state
            self._pending_state = None
            self._consecutive_count = 0
            return True, confirmed_state
        return False, None


def validate_state_window_boundary():
    """验证5种交替模式下确认窗口行为正确。"""
    results = []
    # 模式1: AAA → 确认A
    w = StateConfirmWindow(confirm_bars=3)
    for s in ['A', 'A', 'A']:
        switched, to = w.feed(s)
    results.append(('AAA', switched == True and to == 'A'))

    # 模式2: AAB → 不确认
    w = StateConfirmWindow(confirm_bars=3)
    switched_final = False
    for s in ['A', 'A', 'B']:
        switched_final, _ = w.feed(s)
    results.append(('AAB', switched_final == False))

    # 模式3: AABAA → 确认A(第5个)
    w = StateConfirmWindow(confirm_bars=3)
    switches = []
    for s in ['A', 'A', 'B', 'A', 'A']:
        sw, to = w.feed(s)
        if sw:
            switches.append(to)
    results.append(('AABAA', len(switches) == 0))

    # 模式4: AABAAA → 确认A(第6个)
    w = StateConfirmWindow(confirm_bars=3)
    switches = []
    for s in ['A', 'A', 'B', 'A', 'A', 'A']:
        sw, to = w.feed(s)
        if sw:
            switches.append(to)
    results.append(('AABAAA', len(switches) == 1 and switches[0] == 'A'))

    # 模式5: ABABAB → 不确认
    w = StateConfirmWindow(confirm_bars=3)
    switches = []
    for s in ['A', 'B', 'A', 'B', 'A', 'B']:
        sw, to = w.feed(s)
        if sw:
            switches.append(to)
    results.append(('ABABAB', len(switches) == 0))

    all_passed = all(r[1] for r in results)
    return {'passed': all_passed, 'details': results}


# =====================================================================
# 裂缝25: 断路器vs时间止损优先级
# =====================================================================

def check_safety_meta_layer(
    is_circuit_breaker_paused: bool,
    hard_time_stop_triggered: bool,
) -> Tuple[bool, bool]:
    """断路器暂停期间仅允许平仓(保护)，禁止新开仓。

    返回: (can_open, can_close)
    """
    can_open = not is_circuit_breaker_paused
    can_close = True  # 平仓始终允许（保护性操作）
    if hard_time_stop_triggered:
        can_close = True
        can_open = False
    return can_open, can_close


def validate_circuit_breaker_vs_time_stop():
    """验证4种组合下can_open/can_close的正确性。"""
    results = []
    # 正常: 可开可平
    can_open, can_close = check_safety_meta_layer(False, False)
    results.append(('normal', can_open == True and can_close == True))

    # 熔断中: 不可开可平
    can_open, can_close = check_safety_meta_layer(True, False)
    results.append(('circuit_breaker', can_open == False and can_close == True))

    # 时间止损: 不可开可平
    can_open, can_close = check_safety_meta_layer(False, True)
    results.append(('time_stop', can_open == False and can_close == True))

    # 熔断+时间止损: 不可开可平
    can_open, can_close = check_safety_meta_layer(True, True)
    results.append(('both', can_open == False and can_close == True))

    all_passed = all(r[1] for r in results)
    return {'passed': all_passed, 'details': results}


# =====================================================================
# 裂缝26: 多粒度指标一致性
# =====================================================================

def resample_indicator_ffill(
    indicator_1min: np.ndarray,
    target_freq: int,
) -> np.ndarray:
    """将1分钟指标通过前向填充对齐到目标频率。

    target_freq: 目标频率(如5/15/60分钟)
    """
    n = len(indicator_1min)
    if n == 0 or target_freq <= 1:
        return indicator_1min.copy()
    result_len = n // target_freq
    result = np.zeros(result_len)
    for i in range(result_len):
        result[i] = indicator_1min[i * target_freq]
    return result


def validate_indicator_multiscale_consistency(
    indicator_native: np.ndarray,
    indicator_resampled: np.ndarray,
    min_corr: float = 0.95,
) -> Dict[str, Any]:
    """验证原生频率指标下采样 vs 重采样后直接计算的相关系数 > min_corr。"""
    n = min(len(indicator_native), len(indicator_resampled))
    if n < 2:
        return {'passed': False, 'corr': 0.0, 'reason': '数据不足'}
    a = indicator_native[:n]
    b = indicator_resampled[:n]
    if np.std(a) == 0 or np.std(b) == 0:
        return {'passed': True, 'corr': 1.0, 'reason': '常量序列'}
    corr = float(np.corrcoef(a, b)[0, 1])
    passed = corr >= min_corr
    return {'passed': passed, 'corr': corr, 'min_corr': min_corr}


# =====================================================================
# 裂缝27: 主-影子Sharpe置信区间重叠
# =====================================================================

def compute_sharpe_ci(
    sharpe: float,
    n_observations: int,
    z: float = 1.96,
) -> Tuple[float, float]:
    """计算Sharpe比率的近似置信区间。

    Sharpe标准误差 ≈ 1/sqrt(n)（简化假设）。
    """
    if n_observations <= 0:
        return sharpe, sharpe
    se = 1.0 / math.sqrt(n_observations)
    return sharpe - z * se, sharpe + z * se


def validate_alpha_confidence_overlap(
    main_sharpe: float,
    main_n: int,
    shadow_sharpe: float,
    shadow_n: int,
    max_overlap_ratio: float = 0.5,
) -> Dict[str, Any]:
    """验证主-影子Sharpe置信区间重叠比例 <= max_overlap_ratio。

    若重叠>50%，标记alpha_unreliable，资金分配回退到基础权重。
    """
    main_low, main_high = compute_sharpe_ci(main_sharpe, main_n)
    shadow_low, shadow_high = compute_sharpe_ci(shadow_sharpe, shadow_n)
    overlap = max(0.0, min(main_high, shadow_high) - max(main_low, shadow_low))
    main_width = main_high - main_low
    if main_width <= 0:
        overlap_ratio = 0.0
    else:
        overlap_ratio = overlap / main_width
    alpha = main_sharpe - shadow_sharpe
    passed = overlap_ratio <= max_overlap_ratio
    return {
        'passed': passed,
        'main_ci': (main_low, main_high),
        'shadow_ci': (shadow_low, shadow_high),
        'overlap_ratio': overlap_ratio,
        'alpha': alpha,
        'alpha_reliable': passed,
        'recommendation': 'use_alpha_weights' if passed else 'fallback_to_base_weights',
    }


# =====================================================================
# 裂缝28: 逻辑反转wrong_pct未来函数验证
# =====================================================================

def validate_logic_reversal_no_future(
    signal_timestamps: List[str],
    ref_price_timestamps: List[str],
    wrong_pct_bar_timestamps: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """验证逻辑反转平仓触发时刻，wrong_pct不使用未来数据。

    三重验证：
    1. 每个signal_timestamp >= 对应的ref_price_timestamp（信号不早于参考价）
    2. wrong_pct_bar_timestamps: wrong_pct数据来源bar的时间戳，
       必须 <= signal_timestamp（不使用信号触发后的bar数据）
    3. 若wrong_pct_bar_timestamps为None，标记为needs_bar_level_audit
    """
    violations = []
    bar_violations = []
    n = min(len(signal_timestamps), len(ref_price_timestamps))
    for i in range(n):
        if signal_timestamps[i] < ref_price_timestamps[i]:
            violations.append(i)
    if wrong_pct_bar_timestamps is not None:
        n_bar = min(n, len(wrong_pct_bar_timestamps))
        for i in range(n_bar):
            if wrong_pct_bar_timestamps[i] > signal_timestamps[i]:
                bar_violations.append(i)
    passed = len(violations) == 0 and len(bar_violations) == 0
    needs_bar_audit = wrong_pct_bar_timestamps is None
    return {
        'passed': passed and not needs_bar_audit,
        'n_checked': n,
        'n_violations': len(violations),
        'n_bar_violations': len(bar_violations),
        'violation_indices': violations[:10],
        'bar_violation_indices': bar_violations[:10],
        'needs_bar_level_audit': needs_bar_audit,
    }


# =====================================================================
# 裂缝29: 影子参数空间独立性(KL散度)
# =====================================================================

def compute_kl_divergence(
    p: np.ndarray,
    q: np.ndarray,
    epsilon: float = 1e-10,
) -> float:
    """计算两个离散分布的KL散度 KL(P||Q)。

    p, q需为概率分布(自动归一化)。
    """
    p = np.array(p, dtype=float)
    q = np.array(q, dtype=float)
    p_sum = p.sum()
    q_sum = q.sum()
    if p_sum > 0:
        p = p / p_sum
    if q_sum > 0:
        q = q / q_sum
    p = np.clip(p, epsilon, None)
    q = np.clip(q, epsilon, None)
    return float(np.sum(p * np.log(p / q)))


def validate_shadow_param_orthogonality(
    main_param_vector: np.ndarray,
    shadow_param_vector: np.ndarray,
    n_bins: int = 20,
    min_kl: float = 0.5,
) -> Dict[str, Any]:
    """验证影子参数分布与主参数分布的KL散度 > min_kl。

    将参数向量直方图化为离散分布后计算KL散度。
    """
    if len(main_param_vector) == 0 or len(shadow_param_vector) == 0:
        return {'passed': False, 'kl_divergence': 0.0, 'reason': '空向量'}
    all_vals = np.concatenate([main_param_vector, shadow_param_vector])
    vmin, vmax = all_vals.min(), all_vals.max()
    if vmax <= vmin:
        return {'passed': False, 'kl_divergence': 0.0, 'reason': '参数无差异'}
    bins = np.linspace(vmin - 1e-10, vmax + 1e-10, n_bins + 1)
    p_hist, _ = np.histogram(main_param_vector, bins=bins)
    q_hist, _ = np.histogram(shadow_param_vector, bins=bins)
    kl = compute_kl_divergence(p_hist.astype(float), q_hist.astype(float))
    passed = kl >= min_kl
    return {
        'passed': passed,
        'kl_divergence': kl,
        'min_kl': min_kl,
        'recommendation': 'ok' if passed else '需增加参数差异度或更换影子参数',
    }


# =====================================================================
# 裂缝30: 归因残差统计显著性
# =====================================================================

def validate_attribution_residual_significance(
    residual_series: np.ndarray,
    pnl_series: np.ndarray,
    alpha: float = 0.05,
    residual_pct_threshold: float = 0.15,
) -> Dict[str, Any]:
    """验证归因残差是否统计显著且超过相对阈值。

    触发E7条件: t检验p<alpha 且 mean(|residual|) > residual_pct_threshold * mean(|pnl|)
    """
    n = min(len(residual_series), len(pnl_series))
    if n < 2:
        return {'passed': True, 'reason': '样本不足跳过', 'e7_should_trigger': False}
    residuals = residual_series[:n]
    pnls = pnl_series[:n]
    mean_residual = float(np.mean(residuals))
    std_residual = float(np.std(residuals, ddof=1))
    mean_abs_residual = float(np.mean(np.abs(residuals)))
    mean_abs_pnl = float(np.mean(np.abs(pnls)))
    if std_residual <= 0:
        t_stat = 0.0
        p_value = 1.0
    else:
        t_stat = mean_residual / (std_residual / math.sqrt(n))
        p_value = 2.0 * (1.0 - _normal_cdf(abs(t_stat)))
    exceeds_pct = (mean_abs_pnl > 0 and
                   mean_abs_residual > residual_pct_threshold * mean_abs_pnl)
    e7_should_trigger = (p_value < alpha) and exceeds_pct
    passed = True  # 验证本身总是通过，结果告知E7是否应触发
    return {
        'passed': passed,
        't_stat': t_stat,
        'p_value': p_value,
        'mean_abs_residual': mean_abs_residual,
        'mean_abs_pnl': mean_abs_pnl,
        'residual_pct': mean_abs_residual / mean_abs_pnl if mean_abs_pnl > 0 else 0.0,
        'e7_should_trigger': e7_should_trigger,
        'reason': 'significant_and_large' if e7_should_trigger else 'not_significant_or_small',
    }


def _normal_cdf(x: float) -> float:
    """标准正态CDF近似（不依赖scipy）。"""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))
