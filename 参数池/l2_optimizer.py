import logging
import itertools
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ProcessPoolExecutor, as_completed

logger = logging.getLogger(__name__)


L2_PARAM_GRID = {
    'non_other_ratio_threshold': [round(x * 0.05 + 0.30, 2) for x in range(13)],
    'state_confirm_bars': list(range(2, 7)),
    'logic_reversal_threshold': [round(x * 0.25 + 1.0, 2) for x in range(5)],
}

L2_CONFLICT_RESOLUTION = {"rule": "independent_dataset_wins"}


@dataclass
class L2OptimizationResult:
    best_params: Dict[str, Any]
    best_accuracy: float
    overall_accuracy: float
    state_accuracies: Dict[str, float]
    power_sufficient: bool
    conflict_detected: bool
    conflict_action: str
    cross_fold_overlap: float


class L2Optimizer:
    def __init__(self, min_state_transitions: int = 100,
                 min_cross_fold_overlap: float = 0.60,
                 conflict_threshold: float = 0.10):
        self._min_state_transitions = min_state_transitions
        self._min_cross_fold_overlap = min_cross_fold_overlap
        self._conflict_threshold = conflict_threshold

    def evaluate_state_accuracy(self, params: Dict[str, Any],
                                 bar_data: Any = None,
                                 lookahead_bars: int = 5) -> Dict[str, Any]:
        non_other_threshold = params.get('non_other_ratio_threshold', 0.65)
        confirm_bars = params.get('state_confirm_bars', 3)
        reversal_threshold = params.get('logic_reversal_threshold', 1.5)

        state_counts = {'correct_trending': 0, 'incorrect_reversal': 0, 'other': 0}
        state_correct = {'correct_trending': 0, 'incorrect_reversal': 0, 'other': 0}

        if bar_data is not None and hasattr(bar_data, '__len__'):
            n = len(bar_data)
            for i in range(confirm_bars, n - lookahead_bars):
                try:
                    row = bar_data.iloc[i] if hasattr(bar_data, 'iloc') else bar_data[i]
                    future_row = bar_data.iloc[i + lookahead_bars] if hasattr(bar_data, 'iloc') else bar_data[i + lookahead_bars]

                    non_other_ratio = getattr(row, 'non_other_ratio', 0.5)
                    if non_other_ratio >= non_other_threshold:
                        state = 'correct_trending'
                    elif non_other_ratio < (1 - non_other_threshold):
                        state = 'incorrect_reversal'
                    else:
                        state = 'other'

                    state_counts[state] += 1

                    future_return = getattr(future_row, 'close', 0) / getattr(row, 'close', 1) - 1 if hasattr(row, 'close') else 0

                    if state == 'correct_trending' and future_return > 0:
                        state_correct[state] += 1
                    elif state == 'incorrect_reversal' and future_return < 0:
                        state_correct[state] += 1
                    elif state == 'other' and abs(future_return) < 0.005:
                        state_correct[state] += 1
                except Exception:
                    continue

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

    def check_l2_statistical_power(self, bar_data: Any = None) -> bool:
        if bar_data is None:
            return False
        try:
            n = len(bar_data) if hasattr(bar_data, '__len__') else 0
            return n >= self._min_state_transitions
        except Exception:
            return False

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

    def optimize_step1(self, bar_data: Any = None,
                        independent_bar_data: Any = None) -> L2OptimizationResult:
        best_params = {}
        best_accuracy = 0.0
        best_state_accuracies = {}

        keys = list(L2_PARAM_GRID.keys())
        values = [L2_PARAM_GRID[k] for k in keys]

        for combo in itertools.product(*values):
            params = dict(zip(keys, combo))
            data = independent_bar_data if independent_bar_data is not None else bar_data
            result = self.evaluate_state_accuracy(params, data)
            if result['overall_accuracy'] > best_accuracy:
                best_accuracy = result['overall_accuracy']
                best_params = params
                best_state_accuracies = result['state_accuracies']

        power_sufficient = self.check_l2_statistical_power(
            independent_bar_data if independent_bar_data is not None else bar_data
        )

        conflict = {"conflict_detected": False, "action": "proceed_to_step2"}
        if independent_bar_data is not None and bar_data is not None:
            main_result = self.evaluate_state_accuracy(best_params, bar_data)
            conflict = self.check_l2_conflict(best_accuracy, main_result['overall_accuracy'])

        return L2OptimizationResult(
            best_params=best_params,
            best_accuracy=best_accuracy,
            overall_accuracy=best_accuracy,
            state_accuracies=best_state_accuracies,
            power_sufficient=power_sufficient,
            conflict_detected=conflict["conflict_detected"],
            conflict_action=conflict["action"],
            cross_fold_overlap=0.0,
        )


STRATEGY_VARIANTS = ['main', 'shadow_a', 'shadow_b']
STRATEGY_IDS = ['s1_hft', 's2_minute', 's3_box_extreme', 's4_box_spring']

ALPHA_THRESHOLDS = {
    's1_hft': 0.5,
    's2_minute': 0.5,
    's3_box_extreme': 0.3,
    's4_box_spring': 0.4,
}


@dataclass
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


class TwelveStrategyRunner:
    def __init__(self, max_workers: int = 1):
        self._max_workers = max_workers

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
                    run_params['close_take_profit_ratio'] = run_params.get('close_take_profit_ratio', 1.5) * 0.8
                    run_params['close_stop_loss_ratio'] = run_params.get('close_stop_loss_ratio', 0.5) * 1.2
                elif variant == 'shadow_b':
                    run_params['close_take_profit_ratio'] = run_params.get('close_take_profit_ratio', 1.5) * 0.73
                    run_params['close_stop_loss_ratio'] = run_params.get('close_stop_loss_ratio', 0.5) * 0.6

                # ✅ P1-17修复: 调用task_scheduler真实回测逻辑
                from ali2026v3_trading.参数池.task_scheduler import (
                    run_backtest,
                    run_backtest_box_extreme,
                    run_backtest_box_spring,
                    run_backtest_hft,
                )

                if strategy_id == 's1_hft':
                    bt_result = run_backtest_hft(run_params, bar_data, train=True, strategy_type=variant)
                elif strategy_id == 's2_minute':
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
            except Exception as e:
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
        for strategy_id in STRATEGY_IDS:
            params = params_by_strategy.get(strategy_id, {})
            for variant in STRATEGY_VARIANTS:
                result = self.run_single_strategy(strategy_id, variant, params, bar_data)
                results.append(result)
        return results

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
