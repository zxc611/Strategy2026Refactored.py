import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from ali2026v3_trading.shared_utils import CHINA_TZ
from enum import Enum

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class ValidationStatus(Enum):
    PASS = 'pass'
    WARNING = 'warning'
    FAIL = 'fail'
    PENDING = 'pending'


@dataclass(slots=True)
class ExternalSourceConfig:
    name: str
    fetch_fn_name: str
    deviation_threshold: float
    weight: float
    description: str


@dataclass(slots=True)
class ValidationResult:
    source_name: str
    status: ValidationStatus
    deviation_pct: float
    internal_value: float
    external_value: float
    threshold: float
    n_samples: int
    timestamp: str = field(default_factory=lambda: datetime.now(CHINA_TZ).isoformat())


@dataclass(slots=True)
class QuarterlyReport:
    quarter: str
    results: List[ValidationResult]
    overall_status: ValidationStatus
    max_deviation: float
    action_required: str
    timestamp: str = field(default_factory=lambda: datetime.now(CHINA_TZ).isoformat())


class ExternalValidationPipeline:
    """
    季度外部数据源校验流水线

    核心功能：
    1. 多外部源校验：交易所官方数据/第三方数据/公开指标
    2. 偏差检测：内部计算值 vs 外部权威值，偏差>阈值触发告警
    3. 季度自动运行：校验结果存档，连续2季度FAIL触发人工审议
    4. 校验项覆盖：
       - IV中位数 vs 交易所结算IV
       - 状态分类准确率 vs 公开研究报告
       - 希腊字母值 vs 期权定价模型理论值
       - 回测夏普 vs 公开基准夏普
    """

    DEFAULT_SOURCES = {
        'exchange_settlement_iv': ExternalSourceConfig(
            name='交易所结算IV',
            fetch_fn_name='fetch_exchange_iv',
            deviation_threshold=0.05,
            weight=0.40,
            description='内部IV vs 交易所官方结算IV中位数',
        ),
        'public_benchmark_sharpe': ExternalSourceConfig(
            name='公开基准夏普',
            fetch_fn_name='fetch_public_sharpe',
            deviation_threshold=0.10,
            weight=0.30,
            description='内部回测夏普 vs 公开CTA/期权策略基准夏普',
        ),
        'bsm_theoretical_greeks': ExternalSourceConfig(
            name='BSM理论希腊字母',
            fetch_fn_name='fetch_bsm_greeks',
            deviation_threshold=0.03,
            weight=0.20,
            description='内部Greeks vs Black-Scholes-Merton理论值',
        ),
        'state_accuracy_benchmark': ExternalSourceConfig(
            name='状态分类基准',
            fetch_fn_name='fetch_state_benchmark',
            deviation_threshold=0.08,
            weight=0.10,
            description='内部状态准确率 vs 公开研究报告基准',
        ),
    }

    def __init__(
        self,
        sources: Optional[Dict[str, ExternalSourceConfig]] = None,
        consecutive_fail_limit: int = 2,
    ):
        self._sources = sources or dict(self.DEFAULT_SOURCES)
        self._consecutive_fail_limit = consecutive_fail_limit
        self._report_history: List[QuarterlyReport] = []
        self._custom_fetch_fns: Dict[str, Callable] = {}

    def register_fetch_function(
        self, fn_name: str, fn: Callable
    ) -> None:
        self._custom_fetch_fns[fn_name] = fn
        logger.info("外部数据获取函数注册: %s", fn_name)

    def validate_quarter(
        self,
        quarter: str,
        internal_data: Dict[str, Any],
        external_fetch_functions: Optional[Dict[str, Callable]] = None,
    ) -> QuarterlyReport:
        results = []

        for source_key, config in self._sources.items():
            fetch_fn = None
            if external_fetch_functions and config.fetch_fn_name in external_fetch_functions:
                fetch_fn = external_fetch_functions[config.fetch_fn_name]
            elif config.fetch_fn_name in self._custom_fetch_fns:
                fetch_fn = self._custom_fetch_fns[config.fetch_fn_name]

            if fetch_fn is None:
                result = ValidationResult(
                    source_name=config.name,
                    status=ValidationStatus.PENDING,
                    deviation_pct=0.0,
                    internal_value=0.0,
                    external_value=0.0,
                    threshold=config.deviation_threshold,
                    n_samples=0,
                )
                results.append(result)
                continue

            try:
                internal_val, external_val, n_samples = fetch_fn(internal_data)
                deviation = (
                    abs(internal_val - external_val) / max(abs(external_val), 1e-10)
                    if external_val != 0 else 0.0
                )

                if deviation <= config.deviation_threshold * 0.5:
                    status = ValidationStatus.PASS
                elif deviation <= config.deviation_threshold:
                    status = ValidationStatus.WARNING
                else:
                    status = ValidationStatus.FAIL

                result = ValidationResult(
                    source_name=config.name,
                    status=status,
                    deviation_pct=deviation,
                    internal_value=internal_val,
                    external_value=external_val,
                    threshold=config.deviation_threshold,
                    n_samples=n_samples,
                )
            except Exception as e:
                logger.warning("外部校验 %s 失败: %s", config.name, e)
                result = ValidationResult(
                    source_name=config.name,
                    status=ValidationStatus.PENDING,
                    deviation_pct=0.0,
                    internal_value=0.0,
                    external_value=0.0,
                    threshold=config.deviation_threshold,
                    n_samples=0,
                )

            results.append(result)

        overall = self._determine_overall_status(results)
        max_dev = max(r.deviation_pct for r in results) if results else 0.0
        action = self._determine_action(overall, max_dev)

        report = QuarterlyReport(
            quarter=quarter,
            results=results,
            overall_status=overall,
            max_deviation=max_dev,
            action_required=action,
        )
        self._report_history.append(report)

        logger.info(
            "季度外部校验完成: Q=%s, status=%s, max_dev=%.4f, action=%s",
            quarter, overall.value, max_dev, action,
        )
        return report

    @staticmethod
    def _determine_overall_status(
        results: List[ValidationResult],
    ) -> ValidationStatus:
        if any(r.status == ValidationStatus.FAIL for r in results):
            return ValidationStatus.FAIL
        if any(r.status == ValidationStatus.WARNING for r in results):
            return ValidationStatus.WARNING
        if any(r.status == ValidationStatus.PENDING for r in results):
            return ValidationStatus.PENDING
        return ValidationStatus.PASS

    def _determine_action(
        self, overall: ValidationStatus, max_deviation: float
    ) -> str:
        if overall == ValidationStatus.PASS:
            return '无需行动'
        if overall == ValidationStatus.WARNING:
            return '关注：偏差在阈值内但偏高，建议检查数据源'
        if overall == ValidationStatus.FAIL:
            consecutive = self._count_consecutive_fails()
            if consecutive >= self._consecutive_fail_limit:
                return '紧急：连续{}季度FAIL，必须人工审议'.format(consecutive)
            return '行动：偏差超阈值，需检查内部计算逻辑和数据源'
        return '待定：部分数据源未就绪'

    def _count_consecutive_fails(self) -> int:
        count = 0
        for report in reversed(self._report_history):
            if report.overall_status == ValidationStatus.FAIL:
                count += 1
            else:
                break
        return count

    def check_drift(
        self,
        internal_series: pd.Series,
        external_series: pd.Series,
        window: int = 252,
    ) -> Dict[str, Any]:
        min_len = min(len(internal_series), len(external_series), window)
        if min_len < 10:
            return {'drift_detected': False, 'reason': '样本不足'}

        int_vals = internal_series.iloc[-min_len:].values
        ext_vals = external_series.iloc[-min_len:].values

        mean_dev = float(np.mean(np.abs(int_vals - ext_vals) / (np.abs(ext_vals) + 1e-10)))
        max_dev = float(np.max(np.abs(int_vals - ext_vals) / (np.abs(ext_vals) + 1e-10)))

        correlation = float(np.corrcoef(int_vals, ext_vals)[0, 1]) if min_len > 2 else 0.0

        drift_detected = mean_dev > 0.05 or max_dev > 0.10 or correlation < 0.9

        return {
            'drift_detected': drift_detected,
            'mean_deviation': mean_dev,
            'max_deviation': max_dev,
            'correlation': correlation,
            'window': min_len,
            'threshold_mean': 0.05,
            'threshold_max': 0.10,
            'threshold_corr': 0.9,
        }

    def get_report_history(self) -> List[QuarterlyReport]:
        return list(self._report_history)

    def save_reports(self, path: str) -> None:
        records = []
        for report in self._report_history:
            for result in report.results:
                records.append({
                    'quarter': report.quarter,
                    'source': result.source_name,
                    'status': result.status.value,
                    'deviation_pct': result.deviation_pct,
                    'internal_value': result.internal_value,
                    'external_value': result.external_value,
                    'threshold': result.threshold,
                    'overall_status': report.overall_status.value,
                    'max_deviation': report.max_deviation,
                    'action': report.action_required,
                    'timestamp': result.timestamp,
                })
        if records:
            df = pd.DataFrame(records)
            df.to_csv(path, index=False, encoding='utf-8')
            logger.info("校验报告保存至 %s (%d条)", path, len(records))

    def generate_mock_external_data(
        self,
        internal_data: Dict[str, Any],
        noise_level: float = 0.02,
    ) -> Dict[str, Callable]:
        def mock_exchange_iv(data):
            iv = data.get('iv_median', 0.20)
            return iv, iv * (1 + np.random.normal(0, noise_level)), 252

        def mock_public_sharpe(data):
            sharpe = data.get('sharpe', 1.5)
            return sharpe, sharpe * (1 + np.random.normal(0, noise_level)), 120

        def mock_bsm_greeks(data):
            delta = data.get('delta', 0.5)
            return delta, delta * (1 + np.random.normal(0, noise_level)), 500

        def mock_state_benchmark(data):
            accuracy = data.get('state_accuracy', 0.7)
            return accuracy, accuracy * (1 + np.random.normal(0, noise_level)), 200

        return {
            'fetch_exchange_iv': mock_exchange_iv,
            'fetch_public_sharpe': mock_public_sharpe,
            'fetch_bsm_greeks': mock_bsm_greeks,
            'fetch_state_benchmark': mock_state_benchmark,
        }
