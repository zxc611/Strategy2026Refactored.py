"""P0-GD-001/P2-GD-002修复: 金发姑娘测试模块

手册1.1节: 使用极端行情库(真实闪崩日)和正常行情库(盘整日)。
扫描参数组合，评估生存率、误杀率、恢复参与率。
金发姑娘原则: 参数不能太紧(误杀太多)也不能太松(在闪崩中全亏)。
"""
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

_DEFAULT_SURVIVAL_THRESHOLD = 0.95
_DEFAULT_FALSE_KILL_THRESHOLD = 0.30
_DEFAULT_RECOVERY_THRESHOLD = 0.60


@dataclass(slots=True)
class GoldilocksResult:
    """金发姑娘测试结果"""
    param_set_id: str
    survival_rate: float = 0.0
    false_kill_rate: float = 0.0
    recovery_rate: float = 0.0
    passed: bool = False
    reject_reason: str = ''


class GoldilocksValidator:
    """金发姑娘验证器 — 手册1.1节量化金发姑娘测试"""

    def __init__(
        self,
        survival_threshold: float = _DEFAULT_SURVIVAL_THRESHOLD,
        false_kill_threshold: float = _DEFAULT_FALSE_KILL_THRESHOLD,
        recovery_threshold: float = _DEFAULT_RECOVERY_THRESHOLD,
    ):
        self._survival_threshold = survival_threshold
        self._false_kill_threshold = false_kill_threshold
        self._recovery_threshold = recovery_threshold
        self._results: List[GoldilocksResult] = []

    def evaluate_param_set(
        self,
        param_set_id: str,
        extreme_results: List[Dict[str, Any]],
        normal_results: List[Dict[str, Any]],
    ) -> GoldilocksResult:
        """评估一组参数在极端行情和正常行情下的表现

        Args:
            param_set_id: 参数集标识
            extreme_results: 极端行情回测结果列表, 每项含survived(bool)/recovered(bool)等字段
            normal_results: 正常行情回测结果列表, 每项含survived(bool)/killed(bool)等字段

        Returns:
            GoldilocksResult含生存率/误杀率/恢复参与率及是否通过
        """
        n_extreme = len(extreme_results)
        n_normal = len(normal_results)

        if n_extreme == 0 or n_normal == 0:
            result = GoldilocksResult(
                param_set_id=param_set_id,
                passed=False,
                reject_reason='insufficient_data',
            )
            self._results.append(result)
            return result

        survival_count = sum(1 for r in extreme_results if r.get('survived', False))
        survival_rate = survival_count / n_extreme

        killed_count = sum(1 for r in normal_results if r.get('killed', False))
        false_kill_rate = killed_count / n_normal

        recovered_count = sum(1 for r in extreme_results if r.get('recovered', False))
        recoverable_count = sum(1 for r in extreme_results if not r.get('survived', True))
        recovery_rate = recovered_count / recoverable_count if recoverable_count > 0 else 1.0

        reject_reasons = []
        if survival_rate < self._survival_threshold:
            reject_reasons.append(
                f'survival={survival_rate:.2f}<{self._survival_threshold:.2f}')
        if false_kill_rate > self._false_kill_threshold:
            reject_reasons.append(
                f'false_kill={false_kill_rate:.2f}>{self._false_kill_threshold:.2f}')
        if recovery_rate < self._recovery_threshold:
            reject_reasons.append(
                f'recovery={recovery_rate:.2f}<{self._recovery_threshold:.2f}')

        passed = len(reject_reasons) == 0
        result = GoldilocksResult(
            param_set_id=param_set_id,
            survival_rate=survival_rate,
            false_kill_rate=false_kill_rate,
            recovery_rate=recovery_rate,
            passed=passed,
            reject_reason='; '.join(reject_reasons) if reject_reasons else '',
        )
        self._results.append(result)

        if not passed:
            logger.warning(
                "[P0-GD-001] 金发姑娘测试未通过: param=%s, %s",
                param_set_id, result.reject_reason)
        else:
            logger.info(
                "[P0-GD-001] 金发姑娘测试通过: param=%s, survival=%.2f, false_kill=%.2f, recovery=%.2f",
                param_set_id, survival_rate, false_kill_rate, recovery_rate)

        return result

    def run_goldilocks_scan(
        self,
        param_sets: Dict[str, Dict[str, Any]],
        extreme_data: Any,
        normal_data: Any,
        backtest_func,
    ) -> Dict[str, GoldilocksResult]:
        """批量扫描参数组合的金发姑娘测试

        Args:
            param_sets: 参数集字典 {param_id: params_dict}
            extreme_data: 极端行情数据
            normal_data: 正常行情数据
            backtest_func: 回测函数(params, data) -> List[result_dict]

        Returns:
            {param_id: GoldilocksResult}
        """
        scan_results = {}
        for pid, params in param_sets.items():
            extreme_results = backtest_func(params, extreme_data)
            normal_results = backtest_func(params, normal_data)
            scan_results[pid] = self.evaluate_param_set(pid, extreme_results, normal_results)
        n_pass = sum(1 for r in scan_results.values() if r.passed)
        logger.info(
            "[P0-GD-001] 金发姑娘扫描完成: %d/%d 参数集通过",
            n_pass, len(param_sets))
        return scan_results

    @property
    def results(self) -> List[GoldilocksResult]:
        return list(self._results)


def run_goldilocks_test(
    param_sets: Dict[str, Dict[str, Any]],
    extreme_data: Any,
    normal_data: Any,
    backtest_func,
    survival_threshold: float = _DEFAULT_SURVIVAL_THRESHOLD,
    false_kill_threshold: float = _DEFAULT_FALSE_KILL_THRESHOLD,
    recovery_threshold: float = _DEFAULT_RECOVERY_THRESHOLD,
) -> Dict[str, GoldilocksResult]:
    """模块级入口: 执行金发姑娘测试

    Args:
        param_sets: 参数集字典
        extreme_data: 极端行情数据
        normal_data: 正常行情数据
        backtest_func: 回测函数
        survival_threshold: 生存率阈值(默认95%)
        false_kill_threshold: 误杀率阈值(默认30%)
        recovery_threshold: 恢复参与率阈值(默认60%)

    Returns:
        {param_id: GoldilocksResult}
    """
    validator = GoldilocksValidator(
        survival_threshold=survival_threshold,
        false_kill_threshold=false_kill_threshold,
        recovery_threshold=recovery_threshold,
    )
    return validator.run_goldilocks_scan(param_sets, extreme_data, normal_data, backtest_func)
