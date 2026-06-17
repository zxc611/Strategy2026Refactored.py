# [M1-72] 可观测性指标注册
"""
Phase0增强: MetricsRegistry — 可观测性设计(Prometheus风格指标)
SLI/SLO/SLA定义 + 结构化指标 + 告警规则
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field


@dataclass(slots=True)
class SLIDefinition:
    service: str
    metric: str
    slo_target: str
    sla_commitment: str
    unit: str = 'ms'


SLI_DEFINITIONS = [
    SLIDefinition('OrderExecutor', '下单成功延迟 P99', '≤ 50ms', '月达标率 ≥ 99.9%', 'ms'),
    SLIDefinition('SignalGenerator', '信号生成延迟 P99', '≤ 30ms', '月达标率 ≥ 99.9%', 'ms'),
    SLIDefinition('OrderPersistence', 'WAL写入延迟 P99', '≤ 10ms', '月达标率 ≥ 99.99%', 'ms'),
    SLIDefinition('HealthCheckAggregator', '健康检查完成时间 P99', '≤ 5s', '月达标率 ≥ 99.9%', 's'),
    SLIDefinition('StrategyCoreService', '启动时间 P99', '≤ 30s', '月达标率 ≥ 99.9%', 's'),
]


class MetricsRegistry:
    """可观测性指标注册中心

    Prometheus风格指标:
    - Counter: 单调递增计数器(请求量/错误数/WAL写入量)
    - Histogram: 延迟分布(请求延迟)
    - Gauge: 瞬时值(断路器状态/队列深度)
    """

    def __init__(self):
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = {}
        self._lock = threading.RLock()

    def inc_counter(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        _key = self._make_key(name, labels)
        with self._lock:
            self._counters[_key] = self._counters.get(_key, 0.0) + value

    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        _key = self._make_key(name, labels)
        with self._lock:
            self._gauges[_key] = value

    def observe_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        _key = self._make_key(name, labels)
        with self._lock:
            if _key not in self._histograms:
                self._histograms[_key] = []
            self._histograms[_key].append(value)
            if len(self._histograms[_key]) > 10000:
                self._histograms[_key] = self._histograms[_key][-5000:]

    def get_counter(self, name: str, labels: Optional[Dict[str, str]] = None) -> float:
        _key = self._make_key(name, labels)
        with self._lock:
            return self._counters.get(_key, 0.0)

    def get_gauge(self, name: str, labels: Optional[Dict[str, str]] = None) -> float:
        _key = self._make_key(name, labels)
        with self._lock:
            return self._gauges.get(_key, 0.0)

    def get_histogram_percentile(self, name: str, percentile: float, labels: Optional[Dict[str, str]] = None) -> float:
        _key = self._make_key(name, labels)
        with self._lock:
            values = self._histograms.get(_key, [])
            if not values:
                return 0.0
            _sorted = sorted(values)
            _idx = int(len(_sorted) * percentile)
            return _sorted[min(_idx, len(_sorted) - 1)]

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'histogram_counts': {k: len(v) for k, v in self._histograms.items()},
            }

    @staticmethod
    def _make_key(name: str, labels: Optional[Dict[str, str]] = None) -> str:
        if not labels:
            return name
        _label_str = ','.join(f'{k}={v}' for k, v in sorted(labels.items()))
        return f'{name}{{{_label_str}}}'


_metrics_instance: Optional[MetricsRegistry] = None
_metrics_lock = threading.Lock()


def get_metrics_registry() -> MetricsRegistry:
    global _metrics_instance
    if _metrics_instance is None:
        with _metrics_lock:
            if _metrics_instance is None:
                _metrics_instance = MetricsRegistry()
    return _metrics_instance


class AlertRule:
    """告警规则"""

    def __init__(self, name: str, metric_name: str, threshold: float,
                 duration_seconds: float = 300.0, severity: str = 'P1'):
        self.name = name
        self.metric_name = metric_name
        self.threshold = threshold
        self.duration_seconds = duration_seconds
        self.severity = severity

    def evaluate(self, metrics: MetricsRegistry) -> bool:
        _value = metrics.get_gauge(self.metric_name)
        return _value > self.threshold


DEFAULT_ALERT_RULES = [
    AlertRule('下单延迟过高', 'order_submit_latency_p99_ms', 100.0, 300, 'P1'),
    AlertRule('信号生成失败率', 'signal_generate_error_rate', 0.01, 300, 'P1'),
    AlertRule('WAL写入失败', 'wal_write_error_rate', 0.001, 60, 'P0'),
    AlertRule('断路器打开', 'circuit_breaker_open', 0.5, 0, 'P0'),
    AlertRule('双写不一致', 'dual_write_diff_count', 0.5, 600, 'P1'),
    AlertRule('健康检查失败', 'health_check_fail_count', 3.0, 0, 'P0'),
    AlertRule('测试覆盖率下降', 'test_coverage_drop_pct', 5.0, 86400, 'P2'),
    AlertRule('圈复杂度超标', 'max_cyclomatic_complexity', 10.0, 86400, 'P2'),
]