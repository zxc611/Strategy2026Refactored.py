"""P0-AD-001/P1-AD-002修复: 假信号注入对抗测试模块

手册7.2节: 在回测中以随机间隔向期权报价注入恰好跨过判定阈值的虚假跳动
(订单簿深度为零)。观察策略是否被诱导交易。若误触发率>5%，
调整阈值或增加订单流确认延迟。
"""
import logging
import random
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

_DEFAULT_MISSTRIGGER_THRESHOLD = 0.05
_DEFAULT_N_INJECTION_ROUNDS = 1000
_DEFAULT_PRICE_PERTURBATION_BPS = 5.0


class AdversarialSignalInjector:
    """假信号注入器 — 手册7.2节对抗测试"""

    def __init__(
        self,
        n_rounds: int = _DEFAULT_N_INJECTION_ROUNDS,
        misstrigger_threshold: float = _DEFAULT_MISSTRIGGER_THRESHOLD,
        price_perturbation_bps: float = _DEFAULT_PRICE_PERTURBATION_BPS,
        seed: Optional[int] = 42,
    ):
        self._n_rounds = n_rounds
        self._misstrigger_threshold = misstrigger_threshold
        self._price_perturbation_bps = price_perturbation_bps
        self._rng = random.Random(seed)
        self._results: List[Dict[str, Any]] = []

    def generate_fake_ticks(
        self,
        base_price: float,
        threshold_price: float,
        n_ticks: int = 1,
    ) -> List[Dict[str, Any]]:
        """生成恰好跨过判定阈值的虚假跳动

        Args:
            base_price: 基准价格
            threshold_price: 判定阈值价格
            n_ticks: 生成tick数量

        Returns:
            虚假tick列表, 每个tick含price/volume/is_fake/depth等字段
        """
        fake_ticks = []
        for _ in range(n_ticks):
            direction = self._rng.choice(['above', 'below'])
            perturbation = threshold_price * self._price_perturbation_bps / 10000.0
            if direction == 'above':
                fake_price = threshold_price + perturbation * self._rng.uniform(0.5, 1.5)
            else:
                fake_price = threshold_price - perturbation * self._rng.uniform(0.5, 1.5)
            fake_ticks.append({
                'price': fake_price,
                'volume': 0,
                'is_fake': True,
                'depth': 0,
                'base_price': base_price,
                'threshold': threshold_price,
                'direction': direction,
            })
        return fake_ticks

    def run_adversarial_test(
        self,
        strategy_signal_func,
        base_price: float,
        threshold_price: float,
    ) -> Dict[str, Any]:
        """执行对抗测试 — 注入虚假信号并统计误触发率

        Args:
            strategy_signal_func: 策略信号生成函数, 接受tick dict, 返回signal或None
            base_price: 基准价格
            threshold_price: 判定阈值价格

        Returns:
            测试结果dict, 含misstrigger_rate/pass/details等字段
        """
        triggered_count = 0
        total_injections = 0
        details = []

        for round_idx in range(self._n_rounds):
            n_ticks = self._rng.randint(1, 3)
            fake_ticks = self.generate_fake_ticks(base_price, threshold_price, n_ticks)
            for tick in fake_ticks:
                total_injections += 1
                try:
                    signal = strategy_signal_func(tick)
                    if signal is not None:
                        triggered_count += 1
                        details.append({
                            'round': round_idx,
                            'tick': tick,
                            'signal': signal,
                        })
                except Exception as e:
                    details.append({
                        'round': round_idx,
                        'tick': tick,
                        'error': str(e),
                    })

        misstrigger_rate = triggered_count / total_injections if total_injections > 0 else 0.0
        passed = misstrigger_rate <= self._misstrigger_threshold

        result = {
            'misstrigger_rate': misstrigger_rate,
            'misstrigger_threshold': self._misstrigger_threshold,
            'triggered_count': triggered_count,
            'total_injections': total_injections,
            'n_rounds': self._n_rounds,
            'passed': passed,
            'details': details[:10],
        }
        self._results.append(result)

        if not passed:
            logger.warning(
                "[P0-AD-001] 假信号对抗测试未通过: 误触发率=%.2f%% > %.2f%%阈值",
                misstrigger_rate * 100, self._misstrigger_threshold * 100)
        else:
            logger.info(
                "[P0-AD-001] 假信号对抗测试通过: 误触发率=%.2f%% <= %.2f%%阈值",
                misstrigger_rate * 100, self._misstrigger_threshold * 100)

        return result

    @property
    def results(self) -> List[Dict[str, Any]]:
        return list(self._results)


def run_adversarial_validation(
    strategy_signal_func,
    base_price: float,
    threshold_price: float,
    n_rounds: int = _DEFAULT_N_INJECTION_ROUNDS,
    misstrigger_threshold: float = _DEFAULT_MISSTRIGGER_THRESHOLD,
) -> Dict[str, Any]:
    """模块级入口: 执行假信号对抗测试并返回结果

    Args:
        strategy_signal_func: 策略信号生成函数
        base_price: 基准价格
        threshold_price: 判定阈值价格
        n_rounds: 注入轮次
        misstrigger_threshold: 误触发率阈值(默认5%)

    Returns:
        对抗测试结果dict
    """
    injector = AdversarialSignalInjector(
        n_rounds=n_rounds,
        misstrigger_threshold=misstrigger_threshold,
    )
    return injector.run_adversarial_test(strategy_signal_func, base_price, threshold_price)
