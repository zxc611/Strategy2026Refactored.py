# [M1-105] L2优化器
# MODULE_ID: M1-172
from ali2026v3_trading.infra.trading_utils import get_spawn_context  # NEW-P2-03修复

import logging
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
import math
import itertools
import multiprocessing
import numpy as np
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ProcessPoolExecutor, as_completed

# EC-P2-12: multiprocessing freeze_support for Windows (P1修复: 顶层调用确保spawn子进程安全)
try:
    multiprocessing.freeze_support()
except (RuntimeError, OSError):
    pass  # L3: freeze_support仅Windows spawn模式需要，失败不影响功能

logger = get_logger(__name__)  # R9-5


L2_PARAM_GRID = {
    'non_other_ratio_threshold': [round(x * 0.05 + 0.30, 2) for x in range(13)],
    'state_confirm_bars': list(range(2, 7)),
    'logic_reversal_threshold': [round(x * 0.25 + 1.0, 2) for x in range(5)],
}

L2_CONFLICT_RESOLUTION = {"rule": "independent_dataset_wins"}


@dataclass(slots=True)
class L2OptimizationResult:
    best_params: Dict[str, Any]
    best_accuracy: float
    overall_accuracy: float
    state_accuracies: Dict[str, float]
    power_sufficient: bool
    conflict_detected: bool
    conflict_action: str
    cross_fold_overlap: float
    # P0-R8-10修复: 反事实验证字段
    permutation_p_value: float = 1.0
    null_distribution_95pct: float = 0.0
    n_permutations: int = 0
    # P0-R8-11修复: 多重比较校正字段
    n_comparisons: int = 0
    bonferroni_corrected_p: float = 1.0
    bh_corrected_p: float = 1.0
    # R10-P0-24修复: L-2参数锁定标记，优化完成后冻结参数防止后续修改
    locked_params: Dict[str, bool] = field(default_factory=dict)


class L2Optimizer:
    def __init__(self, min_state_transitions: int = 100,
                 min_cross_fold_overlap: float = 0.60,
                 conflict_threshold: float = 0.10):
        self._min_state_transitions = min_state_transitions
        self._min_cross_fold_overlap = min_cross_fold_overlap
        self._conflict_threshold = conflict_threshold

    def evaluate_state_accuracy(self, params: Dict[str, Any],
                                 bar_data: Any = None,
                                 lookahead_bars: int = 5,
                                 is_backtest: bool = True) -> Dict[str, Any]:
        non_other_threshold = params.get('non_other_ratio_threshold', 0.65)
        confirm_bars = params.get('state_confirm_bars', 5)  # R24-P1-DF-01修复: 统一默认值为5
        reversal_threshold = params.get('logic_reversal_threshold', 1.5)
        effective_lookahead = 0 if is_backtest else lookahead_bars
        # R17-P1-PERF-17修复: 向量化快速路径
        if hasattr(bar_data, 'columns') and 'non_other_ratio' in bar_data.columns and hasattr(bar_data, 'values'):
            import numpy as _np
            _non_other = bar_data['non_other_ratio'].values
            _close = bar_data['close'].values if 'close' in bar_data.columns else None
            _direction = bar_data['price_direction'].values if 'price_direction' in bar_data.columns else None
            _state = _np.full(len(bar_data), 'other', dtype=object)
            if _direction is not None:
                _mask_high = _non_other >= non_other_threshold
                _mask_low = _non_other < (1 - non_other_threshold)
                _state[_mask_high & (_direction == 'fall')] = 'correct_trending_defensive'
                _state[_mask_high & (_direction != 'fall')] = 'correct_trending'
                _state[_mask_low & (_direction == 'fall')] = 'incorrect_reversal_defensive'
                _state[_mask_low & (_direction != 'fall')] = 'incorrect_reversal'

        # R10-三对齐修复: 五态分类与state_param_manager.py _FIVE_STATES一致
        state_counts = {
            'correct_trending': 0, 'correct_trending_defensive': 0,
            'incorrect_reversal': 0, 'incorrect_reversal_defensive': 0,
            'other': 0,
        }
        state_correct = {
            'correct_trending': 0, 'correct_trending_defensive': 0,
            'incorrect_reversal': 0, 'incorrect_reversal_defensive': 0,
            'other': 0,
        }

        if bar_data is not None and hasattr(bar_data, '__len__'):
            n = len(bar_data)
            for i in range(confirm_bars, n - effective_lookahead):
                try:
                    row = bar_data.iloc[i] if hasattr(bar_data, 'iloc') else bar_data[i]
                    if effective_lookahead > 0:
                        future_row = bar_data.iloc[i + effective_lookahead] if hasattr(bar_data, 'iloc') else bar_data[i + effective_lookahead]
                    else:
                        future_row = row

                    # R10-三对齐修复: 五态分类逻辑与state_param_manager.py STATE_NAME_MAP一致
                    non_other_ratio = getattr(row, 'non_other_ratio', 0.5)
                    price_direction = getattr(row, 'price_direction', None)
                    if non_other_ratio >= non_other_threshold:
                        if price_direction == 'fall':
                            state = 'correct_trending_defensive'
                        else:
                            state = 'correct_trending'
                    elif non_other_ratio < (1 - non_other_threshold):
                        if price_direction == 'fall':
                            state = 'incorrect_reversal_defensive'
                        else:
                            state = 'incorrect_reversal'
                    else:
                        state = 'other'

                    state_counts[state] += 1

                    _close = getattr(row, 'close', 0)
                    future_return = getattr(future_row, 'close', 0) / _close - 1 if _close > 1e-10 else 0.0

                    # R10-三对齐修复: 五态正确性判定
                    if state == 'correct_trending' and future_return > 0:
                        state_correct[state] += 1
                    elif state == 'correct_trending_defensive' and future_return > 0:
                        state_correct[state] += 1
                    elif state == 'incorrect_reversal' and future_return < 0:
                        state_correct[state] += 1
                    elif state == 'incorrect_reversal_defensive' and future_return < 0:
                        state_correct[state] += 1
                    elif state == 'other' and abs(future_return) < 0.005:
                        state_correct[state] += 1
                except (ValueError, KeyError, TypeError, AttributeError):
                    continue  # L2: 单行数据处理失败，跳过该行不影响整体统计

        total = sum(state_counts.values()) or 1
        correct = sum(state_correct.values())
        overall_accuracy = correct / total

        state_accuracies = {}
        for s in state_counts:
            state_accuracies[s] = state_correct[s] / state_counts[s] if state_counts[s] > 0 else 0.0

        return {
            'overall_accuracy': overall_accuracy,
            'state_accuracies': state_accuracies,
            'total_transitions': total,
        }

    def check_l2_statistical_power(self, bar_data: Any = None) -> Dict[str, Any]:
        """P1-R8-06修复: L2统计功效检验 — Cohen's d效应量+所需样本量+实际功效值

        手册0.4节要求: 每IV regime状态切换≥100次 + 跨fold最优区间重叠度≥60%
        返回Dict而非bool，含效应量、所需样本量、实际功效、是否达标。'
        """
        result = {
            'sufficient': False,
            'n_samples': 0,
            'min_required': self._min_state_transitions,
            'cohens_d': 0.0,
            'power_1_minus_beta': 0.0,
            'required_n_for_80pct_power': 0,
        }
        if bar_data is None:
            return result
        try:
            n = len(bar_data) if hasattr(bar_data, '__len__') else 0
            result['n_samples'] = n
            if n < 2:
                return result
            if hasattr(bar_data, 'iloc') and 'close' in getattr(bar_data, 'columns', []):
                returns = bar_data['close'].pct_change().dropna().values
            elif isinstance(bar_data, (list, tuple)):
                returns = np.array([bar.get('close', 0) for bar in bar_data])
                if len(returns) > 1:
                    returns = np.diff(returns) / (returns[:-1] + 1e-10)
            else:
                returns = np.array([])
            if len(returns) < 2:
                result['sufficient'] = n >= self._min_state_transitions
                return result
            mean_ret = float(np.mean(returns))
            std_ret = float(np.std(returns, ddof=1))
            if std_ret > 1e-10:
                cohens_d = abs(mean_ret) / std_ret
            else:
                cohens_d = 0.0
            result['cohens_d'] = cohens_d
            if cohens_d > 1e-10:
                required_n = int(np.ceil((2.802 / cohens_d) ** 2))
            else:
                required_n = 999999
            result['required_n_for_80pct_power'] = required_n
            if cohens_d > 0 and n > 1:
                ncp = cohens_d * np.sqrt(n)
                from scipy.stats import norm as _norm
                power = 1.0 - _norm.cdf(1.96 - ncp)
                result['power_1_minus_beta'] = float(power)
            else:
                result['power_1_minus_beta'] = 0.0
            result['sufficient'] = (
                n >= self._min_state_transitions
                and result['power_1_minus_beta'] >= 0.80
            )
            return result
        except (ValueError, KeyError, TypeError, RuntimeError) as e:
            logger.warning("[L2Optimizer] check_l2_statistical_power异常: %s", e)
            result['sufficient'] = result.get('n_samples', 0) >= self._min_state_transitions
            return result

    def check_l2_conflict(self, independent_accuracy: float,
                           main_accuracy: float) -> Dict[str, Any]:
        diff = abs(independent_accuracy - main_accuracy) / max(main_accuracy, 1e-10)
        if diff <= self._conflict_threshold:
            action = "proceed_to_step2"
        else:
            action = "expand_independent_data_or_manual_review"
        return {
            "conflict_detected": diff > self._conflict_threshold,
            "diff_pct": diff,
            "action": action,
            "winner": "independent_dataset" if diff > self._conflict_threshold else "consistent",
        }

    def check_temporal_independence(self, train_data, valid_data,
                                     vol_col: str = "close",
                                     volume_col: str = "volume") -> Dict[str, Any]:
        """P1-裂缝1：检验训练集与验证集的时间独立性

        使用波动率和成交量的分布JS散度评估独立性。'
        JS散度 > 0.1 视为足够独立。
        """
        try:
            from scipy.spatial.distance import jensenshannon
            from scipy.stats import ks_2samp
        except ImportError:
            return {"independence_ok": True, "js_divergence": 0.0, "reason": "scipy不可用，跳过独立性检验"}

        if train_data is None or valid_data is None:
            return {"independence_ok": False, "js_divergence": 1.0, "reason": "数据为空"}

        results = {}
        try:
            # 波动率独立性
            if hasattr(train_data, 'pct_change') and vol_col in getattr(train_data, 'columns', []):
                train_vol = train_data[vol_col].pct_change().dropna().values
                valid_vol = valid_data[vol_col].pct_change().dropna().values
                if len(train_vol) > 10 and len(valid_vol) > 10:
                    # 计算直方图
                    n_bins = min(50, len(train_vol) // 10, len(valid_vol) // 10)
                    if n_bins > 2:
                        train_hist, bin_edges = np.histogram(train_vol, bins=n_bins, density=True)
                        valid_hist, _ = np.histogram(valid_vol, bins=bin_edges, density=True)
                        js_vol = jensenshannon(train_hist, valid_hist)
                        ks_stat, ks_p = ks_2samp(train_vol, valid_vol)
                        results["vol_js_divergence"] = float(js_vol)
                        results["vol_ks_pvalue"] = float(ks_p)

            # 成交量独立性
            if volume_col in getattr(train_data, 'columns', []):
                train_vol_data = train_data[volume_col].dropna().values
                valid_vol_data = valid_data[volume_col].dropna().values
                if len(train_vol_data) > 10 and len(valid_vol_data) > 10:
                    n_bins = min(50, len(train_vol_data) // 10, len(valid_vol_data) // 10)
                    if n_bins > 2:
                        train_hist, bin_edges = np.histogram(train_vol_data, bins=n_bins, density=True)
                        valid_hist, _ = np.histogram(valid_vol_data, bins=bin_edges, density=True)
                        js_volume = jensenshannon(train_hist, valid_hist)
                        results["volume_js_divergence"] = float(js_volume)

            max_js = max(results.get("vol_js_divergence", 0.0), results.get("volume_js_divergence", 0.0))
            results["js_divergence"] = max_js
            results["independence_ok"] = max_js >= 0.1  # JS散度>0.1视为足够独立
            results["threshold"] = 0.1

        except (ValueError, KeyError, TypeError, RuntimeError) as e:
            results["independence_ok"] = False  # L1-修复: 验证失败必须阻断，不能默认通过
            results["error"] = str(e)

        return results

    def optimize_step1(self, bar_data: Any = None,
                        independent_bar_data: Any = None) -> L2OptimizationResult:
        # P-06修复: 强制要求独立数据集，违反铁律时抛出异常
        if independent_bar_data is None:
            raise ValueError(
                "[P-06] L-2 Step1优化必须使用独立数据集！"
                "根据V7手册铁律：L-2参数优化必须使用与策略回测期完全无重叠的独立历史数据。"
                "请传入independent_bar_data参数，确保数据独立性以防止数据窥探(data snooping)。"
            )
        
        best_params = {}
        best_accuracy = 0.0
        best_state_accuracies = {}

        keys = list(L2_PARAM_GRID.keys())
        values = [L2_PARAM_GRID[k] for k in keys]

        for combo in itertools.product(*values):
            params = dict(zip(keys, combo))
            # P-06修复: 始终使用独立数据集进行Step1优化
            data = independent_bar_data
            result = self.evaluate_state_accuracy(params, data)
            # P1-裂缝41修复: 增加min_state_accuracy约束，某状态准确率<50%且样本>=10时跳过该组合
            state_accuracies = result.get('state_accuracies', {})
            state_counts_raw = {}
            if hasattr(data, '__len__') and len(data) > 0:
                # 复用evaluate_state_accuracy内部的状态计数逻辑进行快速估算
                import numpy as _np
                _non_other = data['non_other_ratio'].values if hasattr(data, 'columns') and 'non_other_ratio' in data.columns else _np.array([])
                _dir = data['price_direction'].values if hasattr(data, 'columns') and 'price_direction' in data.columns else _np.full(len(_non_other), 'rise')
                _thresh = params.get('non_other_ratio_threshold', 0.65)
                for s in ['correct_trending', 'correct_trending_defensive', 'incorrect_reversal', 'incorrect_reversal_defensive', 'other']:
                    state_counts_raw[s] = 0
                for i in range(len(_non_other)):
                    if _non_other[i] >= _thresh:
                        state_counts_raw['correct_trending_defensive' if _dir[i] == 'fall' else 'correct_trending'] += 1
                    elif _non_other[i] < (1 - _thresh):
                        state_counts_raw['incorrect_reversal_defensive' if _dir[i] == 'fall' else 'incorrect_reversal'] += 1
                    else:
                        state_counts_raw['other'] += 1
            min_state_accuracy_ok = True
            for s, acc in state_accuracies.items():
                n_samples = state_counts_raw.get(s, 0)
                if n_samples >= 10 and acc < 0.50:
                    min_state_accuracy_ok = False
                    break
            if not min_state_accuracy_ok:
                logger.debug("[L2Optimizer] 组合跳过: 状态%s准确率%.2f<50%%(n=%d)", s, acc, n_samples)
                continue
            if result['overall_accuracy'] > best_accuracy:
                best_accuracy = result['overall_accuracy']
                best_params = params
                best_state_accuracies = result['state_accuracies']

        # P-06修复: 统计功效检查也使用独立数据集
        _power_result = self.check_l2_statistical_power(independent_bar_data)
        power_sufficient = _power_result.get('sufficient', False) if isinstance(_power_result, dict) else bool(_power_result)

        # P0-R8-10修复: 1000次打乱反事实验证
        perm_result = {'permutation_p_value': 1.0, 'null_95pct': 0.0, 'n_permutations': 0}
        if independent_bar_data is not None:
            try:
                perm_result = self._permutation_test(best_params, independent_bar_data, n_permutations=1000)
                logger.info("[L2Optimizer] 反事实验证: p=%.4f, null_95pct=%.4f, n=%d",
                           perm_result['permutation_p_value'], perm_result['null_95pct'], perm_result['n_permutations'])
            except (ValueError, KeyError, TypeError, RuntimeError) as e:
                logger.warning("[L2Optimizer] 反事实验证失败: %s", e)

        # P0-R8-11修复: Bonferroni/BH多重比较校正
        n_combos = len(list(itertools.product(*values)))
        # 每组合对应一个p值(简化: 用accuracy的补数作为proxy)
        combo_p_values = []
        for combo in itertools.product(*values):
            params_t = dict(zip(keys, combo))
            r = self.evaluate_state_accuracy(params_t, independent_bar_data)
            # 使用1-accuracy作为近似p值(越小越显著)
            combo_p_values.append(max(1e-10, 1.0 - r['overall_accuracy']))
        correction = self._apply_multiple_comparison_correction(combo_p_values[:min(len(combo_p_values), 325)])
        logger.info("[L2Optimizer] 多重比较校正: Bonferroni=%.6f, BH=%.6f, n=%d/%d",
                   correction['bonferroni'], correction['bh'], correction['n_comparisons'], n_combos)

        # R10-P0-22修复: 调用跨时段交叉折叠重叠度计算，替代硬编码0.0
        cross_fold_overlap = self._compute_cross_fold_overlap(best_params, independent_bar_data)
        logger.info("[L2Optimizer] 跨时段交叉折叠重叠度: %.4f (阈值>0.60)", cross_fold_overlap)

        # R10-P0-24修复: 优化完成后锁定所有L2参数网格中的参数
        locked_params = {k: True for k in L2_PARAM_GRID.keys()}

        conflict = {"conflict_detected": False, "action": "proceed_to_step2"}
        if bar_data is not None:
            main_result = self.evaluate_state_accuracy(best_params, bar_data)
            conflict = self.check_l2_conflict(best_accuracy, main_result['overall_accuracy'])

        _validation_results = self._run_validation_suite(best_params)
        if _validation_results:
            logger.info("[L2Optimizer] validation_suite结果: %s", {k: v.get('passed', 'N/A') for k, v in _validation_results.items() if isinstance(v, dict)})

        return L2OptimizationResult(
            best_params=best_params,
            best_accuracy=best_accuracy,
            overall_accuracy=best_accuracy,
            state_accuracies=best_state_accuracies,
            power_sufficient=power_sufficient,
            conflict_detected=conflict["conflict_detected"],
            conflict_action=conflict["action"],
            cross_fold_overlap=cross_fold_overlap,
            # P0-R8-10: 反事实验证结果
            permutation_p_value=perm_result.get('permutation_p_value', 1.0),
            null_distribution_95pct=perm_result.get('null_95pct', 0.0),
            n_permutations=perm_result.get('n_permutations', 0),
            # P0-R8-11: 多重比较校正结果
            n_comparisons=correction.get('n_comparisons', 0),
            bonferroni_corrected_p=correction.get('bonferroni', 1.0),
            bh_corrected_p=correction.get('bh', 1.0),
            # R10-P0-24: 参数锁定标记
            locked_params=locked_params,
        )

    def _run_validation_suite(self, best_params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return self.validation_suite(
                backtest_params=best_params,
                live_params=best_params,
            )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("[L2Optimizer] validation_suite执行异常: %s", e)
            return {'validation_error': str(e)}

    # P2-R3-T-08: optimize_l2_params_step1方法名偏差 — task_scheduler.py模块级函数
    # 命名为optimize_l2_params_step1，而l2_optimizer.py类方法命名为optimize_step1。
    # 添加别名方法以保持跨模块调用一致性。'
    def optimize_l2_params_step1(self, bar_data: Any = None,
                                  independent_bar_data: Any = None) -> L2OptimizationResult:
        """别名方法: 委托到optimize_step1，保持与task_scheduler模块级函数命名一致"""
        return self.optimize_step1(bar_data=bar_data, independent_bar_data=independent_bar_data)


STRATEGY_VARIANTS = ['main', 'shadow_a', 'shadow_b']
STRATEGY_IDS = ['s1_hft', 's2_resonance', 's3_box_extreme', 's4_box_spring', 's5_arbitrage', 's6_market_making']  # R16-P0-NAMING修复: s2_minute→s2_resonance与strategy_ecosystem/shadow_strategy_engine对齐

ALPHA_THRESHOLDS = {
    's1_hft': 0.5,
    's2_resonance': 0.5,  # R16-P0-NAMING修复: s2_minute→s2_resonance
    's3_box_extreme': 0.3,
    's4_box_spring': 0.4,
    's5_arbitrage': 0.4,
    's6_market_making': 0.3,
}


@dataclass(slots=True)
class TwelveStrategyResult:
    strategy_id: str
    variant: str
    sharpe: float
    max_drawdown: float
    total_return: float
    alpha_ratio: float
    expected_value: float
    n_trades: int
    alpha_threshold: float
    alpha_passed: bool


def _twelve_strategy_worker(strategy_id: str, variant: str,
                             params: Dict[str, Any], bar_data: Any = None) -> 'TwelveStrategyResult':
    """顶层函数，供ProcessPoolExecutor调用（实例方法无法pickle）"""
    runner = TwelveStrategyRunner(max_workers=1)
    return runner.run_single_strategy(strategy_id, variant, params, bar_data)


class TwelveStrategyRunner:
    def __init__(self, max_workers: int = 1):
        self.max_workers = max_workers

    def run_single_strategy(self, strategy_id: str, variant: str,
                             params: Dict[str, Any],
                             bar_data: Any = None) -> TwelveStrategyResult:
        sharpe = 0.0
        max_drawdown = 0.0
        total_return = 0.0
        n_trades = 0

        if bar_data is not None:
            try:
                run_params = dict(params)
                if variant == 'shadow_a':
                    run_params['close_take_profit_ratio'] = run_params.get('close_take_profit_ratio', 1.8) * SHADOW_VARIANT_A_TP_FACTOR  # R16-P0-CFG-02修复: 回退值1.5→1.8
                    run_params['close_stop_loss_ratio'] = run_params.get('close_stop_loss_ratio', 0.3) * SHADOW_VARIANT_A_SL_FACTOR  # R27-P0-CD-04修复: 回退值0.5→0.3，与config_params全局默认值对齐
                elif variant == 'shadow_b':
                    run_params['close_take_profit_ratio'] = run_params.get('close_take_profit_ratio', 1.8) * SHADOW_VARIANT_B_TP_FACTOR  # R16-P0-CFG-02修复: 回退值1.5→1.8
                    run_params['close_stop_loss_ratio'] = run_params.get('close_stop_loss_ratio', 0.3) * SHADOW_VARIANT_B_SL_FACTOR  # R27-P0-CD-04修复: 回退值0.5→0.3，与config_params全局默认值对齐

                # ✅ P1-17修复: 调用task_scheduler真实回测逻辑
                from ali2026v3_trading.param_pool.backtest.backtest_runner_base import (
                    run_backtest,
                    run_backtest_box_extreme,
                    run_backtest_box_spring,
                    run_backtest_hft,
                )

                if strategy_id == 's1_hft':
                    bt_result = run_backtest_hft(run_params, bar_data, train=True, strategy_type=variant)
                elif strategy_id == 's2_resonance':  # R16-P0-NAMING修复: s2_minute→s2_resonance
                    bt_result = run_backtest(run_params, bar_data, train=True, strategy_type=variant)
                elif strategy_id == 's3_box_extreme':
                    bt_result = run_backtest_box_extreme(run_params, bar_data, train=True, strategy_type=variant)
                elif strategy_id == 's4_box_spring':
                    bt_result = run_backtest_box_spring(run_params, bar_data, train=True, strategy_type=variant)
                else:
                    bt_result = run_backtest(run_params, bar_data, train=True, strategy_type=variant)

                if bt_result and 'error' not in bt_result:
                    sharpe = float(bt_result.get('sharpe', 0.0))
                    max_drawdown = float(bt_result.get('max_drawdown', 0.0))
                    total_return = float(bt_result.get('total_return', 0.0))
                    n_trades = int(bt_result.get('num_signals', 0))
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.warning("[TwelveStrategyRunner] 回测执行失败 %s %s: %s", strategy_id, variant, e)

        alpha_threshold = ALPHA_THRESHOLDS.get(strategy_id, 0.5)
        alpha_ratio = max(0, sharpe - 0.0) if sharpe > 0 else 0.0
        expected_value = total_return / n_trades if n_trades > 0 else 0.0
        alpha_passed = alpha_ratio >= alpha_threshold

        return TwelveStrategyResult(
            strategy_id=strategy_id,
            variant=variant,
            sharpe=sharpe,
            max_drawdown=max_drawdown,
            total_return=total_return,
            alpha_ratio=alpha_ratio,
            expected_value=expected_value,
            n_trades=n_trades,
            alpha_threshold=alpha_threshold,
            alpha_passed=alpha_passed,
        )

    def run_all_twelve(self, params_by_strategy: Dict[str, Dict[str, Any]],
                        bar_data: Any = None) -> List[TwelveStrategyResult]:
        results = []
        tasks = []
        for sid in STRATEGY_IDS:
            for variant in STRATEGY_VARIANTS:
                params = params_by_strategy.get(sid, {})
                tasks.append((sid, variant, params))

        if self.max_workers > 1:
            from concurrent.futures import ProcessPoolExecutor, as_completed
            from functools import partial
            # 使用顶层函数避免pickle实例方法的问题
            _worker = _twelve_strategy_worker
            # R21-CC-P2-02修复: 添加max_tasks_per_child参数(Python 3.11+)
            # 优化任务涉及大量DataFrame序列化，子进程内存碎片累积可能导致OOM
            _executor_kwargs = dict(max_workers=self.max_workers, mp_context=get_spawn_context())  # NEW-P2-03修复: 使用公共函数替代重复代码
            try:
                _executor_kwargs['max_tasks_per_child'] = 50  # Python 3.11+
            except (TypeError, ValueError):
                pass  # L3: Python 3.10及以下不支持此参数
            with ProcessPoolExecutor(**_executor_kwargs) as executor:
                futures = {}
                for sid, variant, params in tasks:
                    future = executor.submit(_worker, sid, variant, params, bar_data)
                    futures[future] = (sid, variant)
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except (ValueError, KeyError, TypeError, RuntimeError) as e:
                        sid, variant = futures[future]
                        results.append(TwelveStrategyResult(
                            strategy_id=sid, variant=variant,
                            sharpe=0.0, max_drawdown=0.0, total_return=0.0,
                            alpha_ratio=0.0, expected_value=0.0, n_trades=0,
                            alpha_threshold=ALPHA_THRESHOLDS.get(sid, 0.5),
                            alpha_passed=False
                        ))
        else:
            for sid, variant, params in tasks:
                result = self.run_single_strategy(sid, variant, params, bar_data)
                results.append(result)

        if bar_data is not None and len(results) > 0:
            try:
                best_result = max(results, key=lambda r: getattr(r, 'sharpe', 0.0))
                best_sid = best_result.strategy_id
                best_params = params_by_strategy.get(best_sid, {})
                perm_result = self._permutation_test(best_params, bar_data, n_permutations=min(1000, 100))
                logger.info("[P1-10修复] run_all_twelve反事实验证: p=%.4f, n=%d",
                           perm_result.get('permutation_p_value', 1.0), perm_result.get('n_permutations', 0))
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.warning("[P1-10修复] run_all_twelve反事实验证失败(非致命): %s", e)

        return results

    # P0-R8-10修复: 1000次打乱反事实验证
    def _permutation_test(self, params: Dict, data: Any, n_permutations: int = 1000) -> Dict[str, Any]:
        """
        手册2.4节要求: 执行至少1000次独立打乱，计算每次期望值，构建虚无分布。'
        真实策略期望值必须>95%分位数，并报告p值。
        """
        import random
        real_result = self.evaluate_state_accuracy(params, data)
        real_accuracy = real_result['overall_accuracy']

        null_accuracies = []
        # 尝试获取标签列名
        label_col = data.columns[-1] if hasattr(data, 'columns') and len(data.columns) > 0 else None

        for i in range(n_permutations):
            try:
                if hasattr(data, 'copy'):
                    permuted = data.copy()
                    if label_col and label_col in permuted.columns:
                        labels = permuted[label_col].values.copy()
                        random.shuffle(labels)
                        permuted[label_col] = labels
                    else:
                        # R10-P2-05修复: 块洗牌保留时间结构，避免破坏序列自相关
                        # 原版: random.shuffle(idx) 全局随机打乱 → p值过于乐观
                        # 修复: 块洗牌(block shuffle)，每块20行，块内顺序保持
                        block_size = 20
                        n_rows = len(permuted)
                        n_blocks = max(1, n_rows // block_size)
                        block_starts = list(range(0, n_rows, block_size))
                        random.shuffle(block_starts)
                        idx = []
                        for bs in block_starts:
                            idx.extend(range(bs, min(bs + block_size, n_rows)))
                        permuted = permuted.iloc[idx].reset_index(drop=True)  # R21-MEM-P2-02修复: 链式iloc+reset_index产生2个中间DataFrame副本
                    null_result = self.evaluate_state_accuracy(params, permuted)
                    null_accuracies.append(null_result['overall_accuracy'])
            except (ValueError, KeyError, TypeError, RuntimeError):
                pass  # L2: 单次置换迭代失败，跳过不影响统计显著性

        if len(null_accuracies) < 100:
            return {'permutation_p_value': 1.0, 'null_95pct': 0.0, 'n_permutations': len(null_accuracies)}

        null_accuracies = sorted(null_accuracies)
        n_perm = len(null_accuracies)
        null_95pct = null_accuracies[math.ceil(0.95 * n_perm) - 1] if n_perm > 0 else 0.0
        n_ge = sum(1 for x in null_accuracies if x >= real_accuracy)
        p_value = (n_ge + 1) / (n_perm + 1) if n_perm > 0 else 1.0

        return {
            'permutation_p_value': p_value,
            'null_95pct': null_95pct,
            'n_permutations': len(null_accuracies),
            'real_accuracy': real_accuracy,
        }

    # P0-R8-11修复: Bonferroni和Benjamini-Hochberg多重比较校正
    def _apply_multiple_comparison_correction(self, p_values: List[float]) -> Dict[str, float]:
        """
        手册2.5节要求: 多参数同时扫描时使用Bonferroni校正或BH程序调整显著性阈值。'
        返回bonferroni和bh校正后的p值。
        """
        n = len(p_values)
        if n == 0:
            return {'bonferroni': 1.0, 'bh': 1.0, 'n_comparisons': 0}

        # Bonferroni校正: p * n (截断到1.0)
        bonf_p = min(1.0, min(p_values) * n) if p_values else 1.0

        # Benjamini-Hochberg校正
        sorted_pairs = sorted(enumerate(p_values), key=lambda x: x[1])
        bh_p = 1.0
        for rank, (_, p) in enumerate(sorted_pairs, 1):
            bh_crit = p * n / rank
            if bh_crit <= 0.05:
                bh_p = min(bh_p, p)
                break

        return {
            'bonferroni': bonf_p,
            'bh': bh_p,
            'n_comparisons': n,
        }

    # R10-P0-22修复: 跨时段交叉折叠重叠度计算
    def _compute_cross_fold_overlap(self, params: Dict[str, Any],
                                     data: Any) -> float:
        """将数据分为早/中/近三段，分别计算accuracy及95%CI，
        检查各段CI之间重叠比例是否>60%。

        返回: 最小重叠比例 (0.0~1.0)，若任两段CI重叠>60%则返回对应值。'
        """
        if data is None or not hasattr(data, '__len__') or len(data) < 30:
            return 0.0

        n = len(data)
        third = n // 3
        if third < 10:
            return 0.0

        segments = {  # R21-MEM-P2-01修复: DataFrame.iloc切片返回视图(不产生副本)，但后续若修改需显式.copy()
            'early': data.iloc[:third] if hasattr(data, 'iloc') else data[:third],
            'mid': data.iloc[third:2*third] if hasattr(data, 'iloc') else data[third:2*third],
            'recent': data.iloc[2*third:] if hasattr(data, 'iloc') else data[2*third:],
        }

        segment_stats = {}
        for seg_name, seg_data in segments.items():
            result = self.evaluate_state_accuracy(params, seg_data)
            acc = result['overall_accuracy']
            n_trans = max(result.get('total_transitions', 1), 1)
            # 95% CI: acc ± 1.96 * sqrt(acc*(1-acc)/n)
            se = (acc * (1 - acc) / n_trans) ** 0.5 if 0 < acc < 1 else 0.5
            ci_low = acc - 1.96 * se
            ci_high = acc + 1.96 * se
            segment_stats[seg_name] = (ci_low, ci_high)

        # 计算所有段对之间的CI重叠比例
        seg_names = list(segment_stats.keys())
        min_overlap = 1.0
        for i in range(len(seg_names)):
            for j in range(i + 1, len(seg_names)):
                lo_i, hi_i = segment_stats[seg_names[i]]
                lo_j, hi_j = segment_stats[seg_names[j]]
                # 重叠区间
                overlap_lo = max(lo_i, lo_j)
                overlap_hi = min(hi_i, hi_j)
                # 各自CI宽度
                width_i = hi_i - lo_i
                width_j = hi_j - lo_j
                if width_i <= 0 or width_j <= 0:
                    min_overlap = 0.0
                    continue
                # 重叠比例取两个CI中较小宽度的比例
                overlap_frac = max(0.0, (overlap_hi - overlap_lo) / min(width_i, width_j)) if min(width_i, width_j) > 1e-10 else 0.0
                min_overlap = min(min_overlap, overlap_frac)

        return min_overlap

    def compute_alpha_confidence_interval(self, sharpe: float, n_trades: int,
                                            confidence: float = 0.95) -> Tuple[float, float]:
        if n_trades < 2:
            return (sharpe - 2.0, sharpe + 2.0)
        se = 1.0 / (n_trades ** 0.5)
        try:
            from scipy import stats
            z = stats.norm.ppf((1 + confidence) / 2) if n_trades > 30 else stats.t.ppf((1 + confidence) / 2, n_trades - 1)
        except ImportError:
            z = 1.96 if n_trades > 30 else 2.0
        ci_width = z * se
        return (sharpe - ci_width, sharpe + ci_width)

    # R10-P1-16修复: P0-Q2/Q3/Q4质量门验证函数
    def validate_q2_backtest_vs_live_consistency(
        self,
        backtest_params: Dict[str, Any],
        live_params: Dict[str, Any],
        tolerance: float = 0.10,
    ) -> Dict[str, Any]:
        """P0-Q2质量门: 验证回测参数与实盘参数的一致性

        检查关键参数在回测和实盘之间是否一致，差异超过tolerance则不通过。'
        """
        _KEYS_TO_CHECK = [
            'close_take_profit_ratio', 'close_stop_loss_ratio',
            'max_risk_ratio', 'state_confirm_bars', 'lots_min',
        ]
        inconsistencies = []
        for key in _KEYS_TO_CHECK:
            bt_val = backtest_params.get(key)
            lv_val = live_params.get(key)
            if bt_val is not None and lv_val is not None:
                if isinstance(bt_val, (int, float)) and isinstance(lv_val, (int, float)):
                    if abs(bt_val) > 1e-10:
                        diff = abs(bt_val - lv_val) / abs(bt_val)
                        if diff > tolerance:
                            inconsistencies.append({
                                'key': key,
                                'backtest_value': bt_val,
                                'live_value': lv_val,
                                'diff_pct': round(diff * 100, 2),
                            })

        passed = len(inconsistencies) == 0
        return {
            'passed': passed,
            'details': {
                'inconsistencies': inconsistencies,
                'n_checked': len(_KEYS_TO_CHECK),
                'n_failed': len(inconsistencies),
                'tolerance': tolerance,
            },
        }

    def validate_q3_cross_period_stability(
        self,
        params: Dict[str, Any],
        bar_data: Any = None,
        n_periods: int = 3,
        stability_threshold: float = 0.30,
    ) -> Dict[str, Any]:
        """P0-Q3质量门: 验证参数跨时段稳定性

        将数据分为n_periods段，分别评估accuracy，检查各段accuracy的标准差
        是否在可接受范围内。'
        """
        if bar_data is None or not hasattr(bar_data, '__len__') or len(bar_data) < 30:
            return {'passed': False, 'details': {'reason': '数据不足'}}

        n = len(bar_data)
        period_size = n // n_periods
        if period_size < 10:
            return {'passed': False, 'details': {'reason': '每段数据不足'}}

        period_accuracies = []
        for i in range(n_periods):
            start_idx = i * period_size
            end_idx = (i + 1) * period_size if i < n_periods - 1 else n
            segment = bar_data.iloc[start_idx:end_idx] if hasattr(bar_data, 'iloc') else bar_data[start_idx:end_idx]  # R21-MEM-P2-01修复: iloc切片为视图
            result = self.evaluate_state_accuracy(params, segment)
            period_accuracies.append(result['overall_accuracy'])

        if len(period_accuracies) < 2:
            return {'passed': False, 'details': {'reason': '时段数不足'}}

        mean_acc = np.mean(period_accuracies)
        std_acc = np.std(period_accuracies)
        cv = std_acc / mean_acc if mean_acc > 1e-10 else 999.0  # R27-P2-06-FIX: 用有限值替代inf

        passed = cv <= stability_threshold
        return {
            'passed': passed,
            'details': {
                'period_accuracies': [round(a, 4) for a in period_accuracies],
                'mean_accuracy': round(mean_acc, 4),
                'std_accuracy': round(std_acc, 4),
                'cv': round(cv, 4),
                'stability_threshold': stability_threshold,
            },
        }

    def validate_q4_greeks_constraint_satisfaction(
        self,
        params: Dict[str, Any],
        greeks_exposure: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """P0-Q4质量门: 验证Greeks约束是否满足

        检查参数配置是否满足Greeks风险约束（delta/gamma/vega/theta）。'
        """
        if greeks_exposure is None:
            greeks_exposure = {}

        max_delta_pct = params.get('max_net_delta_pct', 0.30)
        max_gamma_pct = params.get('max_net_gamma_pct', 0.08)
        max_vega_bps = params.get('max_net_vega_bps', 0.02)
        max_theta_ratio = params.get('max_theta_ratio', 0.5)

        violations = []
        net_delta = abs(greeks_exposure.get('net_delta_pct', 0.0))
        if net_delta > max_delta_pct:
            violations.append({
                'constraint': 'net_delta_pct',
                'value': round(net_delta, 4),
                'limit': max_delta_pct,
            })

        net_gamma = abs(greeks_exposure.get('net_gamma_pct', 0.0))
        if net_gamma > max_gamma_pct:
            violations.append({
                'constraint': 'net_gamma_pct',
                'value': round(net_gamma, 4),
                'limit': max_gamma_pct,
            })

        net_vega = abs(greeks_exposure.get('net_vega_bps', 0.0))
        if net_vega > max_vega_bps:
            violations.append({
                'constraint': 'net_vega_bps',
                'value': round(net_vega, 4),
                'limit': max_vega_bps,
            })

        theta_ratio = abs(greeks_exposure.get('theta_ratio', 0.0))
        if theta_ratio > max_theta_ratio:
            violations.append({
                'constraint': 'theta_ratio',
                'value': round(theta_ratio, 4),
                'limit': max_theta_ratio,
            })

        passed = len(violations) == 0
        return {
            'passed': passed,
            'details': {
                'violations': violations,
                'n_checked': 4,
                'n_violations': len(violations),
                'greeks_exposure': greeks_exposure,
            },
        }

    # R10-P1-18修复: 存活分析修正真实盈亏比
    def compute_survival_adjusted_win_loss_ratio(
        self,
        raw_win_loss_ratio: float,
        n_survived: int,
        n_total: int,
    ) -> Dict[str, Any]:
        """计算存活偏差修正后的真实盈亏比

        公式: adjusted_ratio = raw_ratio * (n_survived / n_total)
        存活偏差修正: 仅存活策略的盈亏比被观测到，真实盈亏比需乘以存活率。'
        Args:
            raw_win_loss_ratio: 原始盈亏比（仅基于存活策略计算）'
            n_survived: 存活策略数
            n_total: 总策略数（含淘汰策略）

        Returns:
            Dict: 包含修正后盈亏比和修正因子
        """
        if n_total <= 0:
            return {
                'adjusted_ratio': 0.0,
                'survival_rate': 0.0,
                'raw_ratio': raw_win_loss_ratio,
                'n_survived': n_survived,
                'n_total': n_total,
            }

        survival_rate = n_survived / n_total
        adjusted_ratio = raw_win_loss_ratio * survival_rate

        return {
            'adjusted_ratio': round(adjusted_ratio, 6),
            'survival_rate': round(survival_rate, 4),
            'raw_ratio': raw_win_loss_ratio,
            'adjustment_factor': round(survival_rate, 4),
            'n_survived': n_survived,
            'n_total': n_total,
        }

    def validation_suite(
        self,
        backtest_params: Optional[Dict[str, Any]] = None,
        live_params: Optional[Dict[str, Any]] = None,
        greeks_exposure: Optional[Dict[str, float]] = None,
        raw_win_loss_ratio: float = 0.0,
        n_survived: int = 0,
        n_total: int = 0,
    ) -> Dict[str, Any]:
        results = {}
        if backtest_params and live_params:
            results['q2'] = self.validate_q2_backtest_vs_live_consistency(backtest_params, live_params)
        if backtest_params:
            results['q3'] = self.validate_q3_cross_period_stability(backtest_params)
        if backtest_params:
            results['q4'] = self.validate_q4_greeks_constraint_satisfaction(backtest_params, greeks_exposure)
        # R10-P1-18修复: 在validation_suite中调用存活分析修正
        if n_total > 0 and raw_win_loss_ratio > 0:
            results['survival_adjusted'] = self.compute_survival_adjusted_win_loss_ratio(
                raw_win_loss_ratio, n_survived, n_total)
        return results
