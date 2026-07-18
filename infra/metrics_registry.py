"""
infra/metrics_registry.py — 可观测性+模块状态+特性开关+性能监控 合并模块

合并自:
  - metrics_registry.py (可观测性指标注册)
  - module_load_status.py (模块加载状态管理 NEW-P1-06)
  - phase_feature_flag.py (阶段特性开关)
  - performance_monitor.py (性能监控 PathCounter/count_call/ChaosFaultInjector)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from functools import wraps

from infra._helpers import get_logger

logger = get_logger(__name__)


# ============================================================
# Section 1: 可观测性指标注册 (原 metrics_registry.py)
# ============================================================

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


# ============================================================
# Section 2: 模块加载状态管理 (原 module_load_status.py)
# ============================================================

_STATUS_LOCK = threading.Lock()

_MODULE_LOAD_STATUS = {
    'evaluation_parameter_drift_detector': False,
    'evaluation_chicory_eviction': False,
    'evaluation_marquee_threshold': False,
    'evaluation_violation_tracker': False,
    'evaluation_state_density_decay': False,
    'evaluation_activity_weighted_scorer': False,
    'governance_engine_evaluation_classes': False,
    'risk_service_greeks_calculator': False,
    'service_container_query_service': False,
    'service_container_greeks_calculator': False,
    'service_container_risk_service': False,
    'service_container_t-type_service': False,
    'service_container_diagnosis_service': False,
    'service_container_ui_service': False,
    'service_container_strategy_core_service': False,
    'strategy_core_service_live_strategy_selector': False,
    'strategy_ecosystem_live_strategy_selector': False,
    'strategy_lifecycle_mixin_instrument_data_manager': False,
}


def mark_module_loaded(module_key: str) -> None:
    with _STATUS_LOCK:
        if module_key in _MODULE_LOAD_STATUS:
            _MODULE_LOAD_STATUS[module_key] = True
        else:
            logger.warning("[ModuleLoadStatus] 未知的模块键: %s", module_key)


def mark_module_failed(module_key: str, error: Exception) -> None:
    with _STATUS_LOCK:
        if module_key in _MODULE_LOAD_STATUS:
            _MODULE_LOAD_STATUS[module_key] = False
            logger.error("[ModuleLoadStatus] 核心依赖加载失败: %s, 错误: %s", module_key, error)
        else:
            logger.warning("[ModuleLoadStatus] 未知的模块键: %s, 错误: %s", module_key, error)


def is_module_loaded(module_key: str) -> bool:
    with _STATUS_LOCK:
        return _MODULE_LOAD_STATUS.get(module_key, False)


def get_all_status() -> dict:
    with _STATUS_LOCK:
        return dict(_MODULE_LOAD_STATUS)


def get_degraded_modules() -> list:
    return [k for k, v in _MODULE_LOAD_STATUS.items() if not v]


# ============================================================
# Section 3: 阶段特性开关 (原 phase_feature_flag.py)
# ============================================================

class PhaseFeatureFlag:
    _flags: Dict[str, bool] = {
        'USE_PIPELINED_SEND_ORDER': True,
        'USE_FILTER_CHAIN_SIGNAL': True,
        'USE_BUILDER_INIT': True,
        'USE_HEALTH_AGGREGATOR': True,
        'USE_PARAM_TABLE_PROVIDER': True,
        'USE_RISK_CHECK_ENGINE': True,
        'USE_VALIDATION_REGISTRY': True,
    }
    _lock = threading.RLock()
    _change_log: list = []

    @classmethod
    def is_enabled(cls, flag_name: str) -> bool:
        with cls._lock:
            return cls._flags.get(flag_name, False)

    @classmethod
    def enable(cls, flag_name: str) -> None:
        with cls._lock:
            old = cls._flags.get(flag_name, False)
            cls._flags[flag_name] = True
            cls._change_log.append((flag_name, old, True))
            logging.info("[FeatureFlag] %s: %s -> True", flag_name, old)

    @classmethod
    def disable(cls, flag_name: str) -> None:
        with cls._lock:
            old = cls._flags.get(flag_name, False)
            cls._flags[flag_name] = False
            cls._change_log.append((flag_name, old, False))
            logging.info("[FeatureFlag] %s: %s -> False", flag_name, old)

    @classmethod
    def disable_all(cls) -> None:
        with cls._lock:
            for k in cls._flags:
                cls._flags[k] = False
            logging.critical("[FeatureFlag] 全局紧急回滚: 所有Feature Flag已关闭")

    @classmethod
    def get_all(cls) -> Dict[str, bool]:
        with cls._lock:
            return dict(cls._flags)

    @classmethod
    def get_change_log(cls) -> list:
        with cls._lock:
            return list(cls._change_log)


# ============================================================
# Section 4: 性能监控 (原 performance_monitor.py)
# ============================================================

_path_counter_lock = threading.Lock()


class PathCounter:

    _ALERT_THRESHOLDS: Dict[str, Dict[str, float]] = {
        'avg_time_ms': 5000.0,
        'error_rate': 0.1,
        'total_time_sec': 300.0,
    }

    _persist_filepath: Optional[str] = None
    _persist_thread: Optional[threading.Thread] = None
    _persist_stop_event = threading.Event()
    _PERSIST_INTERVAL_SEC = 300.0

    def __new__(cls):
        from infra.registry_service import SingletonRegistry
        _registry = SingletonRegistry.get_registry('performance_monitor')
        _inst = _registry.get('instance')
        if _inst is None:
            with _path_counter_lock:
                _inst = _registry.get('instance')
                if _inst is None:
                    _inst = super().__new__(cls)
                    _inst._initialized = False
                    _registry.set('instance', _inst)
        return _inst

    def __init__(self):
        if self._initialized:
            return
        self._counters: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._enabled = True
        self._start_time = time.time()
        self._initialized = True

    @classmethod
    def enable(cls) -> None:
        counter = cls()
        counter._enabled = True
        logging.info("[PathCounter] Performance monitoring enabled")

    @classmethod
    def disable(cls) -> None:
        counter = cls()
        counter._enabled = False
        logging.info("[PathCounter] Performance monitoring disabled")

    @classmethod
    def is_enabled(cls) -> bool:
        counter = cls()
        return counter._enabled

    @classmethod
    def start_auto_persist(cls, filepath: str = "performance_report.json", interval_sec: float = 300.0) -> None:
        cls._PERSIST_INTERVAL_SEC = interval_sec
        cls._persist_filepath = filepath
        if cls._persist_thread is not None and cls._persist_thread.is_alive():
            return
        cls._persist_stop_event.clear()
        def _persist_loop():
            while not cls._persist_stop_event.wait(timeout=cls._PERSIST_INTERVAL_SEC):
                try:
                    cls.get_stats(output_format='json', filepath=cls._persist_filepath)
                except Exception as e:
                    logging.error("[PathCounter] Auto-persist failed: %s", e)
        cls._persist_thread = threading.Thread(target=_persist_loop, daemon=True, name="PathCounterAutoPersist")
        cls._persist_thread.start()
        logging.info("[RES-P1-17] 性能指标自动持久化已启动: interval=%.0fs filepath=%s", interval_sec, filepath)

    @classmethod
    def stop_auto_persist(cls) -> None:
        cls._persist_stop_event.set()
        if cls._persist_thread is not None:
            cls._persist_thread.join(timeout=5.0)
        if cls._persist_filepath:
            try:
                cls.get_stats(output_format='json', filepath=cls._persist_filepath)
            except Exception:
                pass

    @classmethod
    def record_call(cls, path: str, execution_time: float = 0.0, exception: Optional[Exception] = None) -> None:
        counter = cls()
        if not counter._enabled:
            return
        with counter._lock:
            if path not in counter._counters:
                counter._counters[path] = {
                    'count': 0,
                    'total_time': 0.0,
                    'errors': 0,
                    'last_error': None
                }
            data = counter._counters[path]
            data['count'] += 1
            data['total_time'] += execution_time
            if exception is not None:
                data['errors'] += 1
                data['last_error'] = str(exception)
            thresholds = cls._ALERT_THRESHOLDS
            count = data['count']
            total_time = data['total_time']
            errors = data['errors']
            avg_time_ms = (total_time / count * 1000) if count > 0 else 0
            error_rate = errors / count if count > 0 else 0
            alerts = []
            if avg_time_ms > thresholds['avg_time_ms']:
                alerts.append("avg_time=%.1fms>%.1fms" % (avg_time_ms, thresholds['avg_time_ms']))
            if error_rate > thresholds['error_rate']:
                alerts.append("error_rate=%.1f%%>%.1f%%" % (error_rate * 100, thresholds['error_rate'] * 100))
            if total_time > thresholds['total_time_sec']:
                alerts.append("total_time=%.1fs>%.1fs" % (total_time, thresholds['total_time_sec']))
            if alerts:
                logging.warning("[PerformanceMonitor] %s: %s", path, "; ".join(alerts))

    @classmethod
    def get_stats(cls, output_format: str = 'dict', **kwargs) -> Any:
        counter = cls()
        with counter._lock:
            from datetime import datetime, timezone as _tz
            result = {
                'service_name': 'PerformanceMonitor',
                'start_time': datetime.fromtimestamp(counter._start_time, tz=_tz.utc).isoformat(),
                'uptime_seconds': time.time() - counter._start_time,
                'total_paths': len(counter._counters),
                'counters': {}
            }
            for path, stats in counter._counters.items():
                avg_time = (stats['total_time'] / stats['count']) if stats['count'] > 0 else 0
                result['counters'][path] = {
                    'count': stats['count'],
                    'total_time': round(stats['total_time'], 6),
                    'avg_time': round(avg_time * 1000, 3),
                    'errors': stats['errors'],
                    'last_error': stats['last_error']
                }
            if output_format == 'json':
                import json as _json
                from infra.serialization_utils import json_dumps
                filepath = kwargs.get('filepath', 'performance_report.json')
                try:
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(json_dumps(result, indent=2))
                    logging.info(f"[PathCounter] Performance report exported to {filepath}")
                except Exception as e:
                    logging.error(f"[PathCounter] Failed to export report: {e}")
                return None
            elif output_format == 'console':
                top_n = kwargs.get('top_n', 20)
                print(f"\n{'='*80}")
                print(f"Path Counter Summary (Uptime: {result['uptime_seconds']:.1f}s)")
                print(f"{'='*80}")
                sorted_paths = sorted(
                    result['counters'].items(),
                    key=lambda x: x[1]['count'],
                    reverse=True
                )[:top_n]
                if not sorted_paths:
                    print("No calls recorded yet.")
                else:
                    for path, stats in sorted_paths:
                        error_flag = f" ({stats['errors']} errors)" if stats['errors'] > 0 else ""
                        print(f"{stats['count']:6d} calls | {stats['avg_time']:8.3f}ms avg | {path}{error_flag}")
                print(f"{'='*80}\n")
                return None
            else:
                return result

    @classmethod
    def reset(cls) -> None:
        from infra.registry_service import SingletonRegistry
        _registry = SingletonRegistry.get_registry('performance_monitor')
        _registry.remove('instance')
        counter = cls()
        with counter._lock:
            counter._counters.clear()
            counter._start_time = time.time()
            logging.info("[PathCounter] All counters reset")


def count_call(path: Optional[str] = None):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not PathCounter.is_enabled():
                return func(*args, **kwargs)
            call_path = path
            if call_path is None:
                is_bound_method = False
                is_classmethod = False
                if args:
                    first_arg = args[0]
                    if hasattr(first_arg, '__class__'):
                        arg_class = first_arg.__class__
                        if isinstance(first_arg, type):
                            is_classmethod = True
                            call_path = f"{first_arg.__name__}.{func.__name__}"
                        elif arg_class.__module__ != 'builtins' and not isinstance(first_arg, (int, float, str, list, dict, tuple, set, bytes)):
                            is_bound_method = True
                            call_path = f"{arg_class.__name__}.{func.__name__}"
                if not is_bound_method and not is_classmethod:
                    call_path = f"{func.__module__}.{func.__name__}"
            start_time = time.time()
            exception = None
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                exception = e
                raise
            finally:
                execution_time = time.time() - start_time
                PathCounter.record_call(call_path, execution_time, exception)
        return wrapper
    return decorator


class ChaosFaultInjector:

    _FAULT_TYPES = frozenset({
        'duckdb_connection', 'duckdb_write', 'duckdb_read',
        'platform_api_timeout', 'platform_api_error',
        'disk_full', 'memory_pressure',
        'network_timeout', 'network_disconnect',
        'thread_pool_reject', 'event_bus_overload',
    })

    def __init__(self):
        self._fault_handlers: Dict[str, callable] = {}
        self._injected_faults: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._enabled = os.environ.get("CHAOS_FAULT_INJECTION", "false").lower() in ("true", "1", "yes")

    def is_enabled(self) -> bool:
        return self._enabled

    def register(self, fault_type: str, handler: callable) -> None:
        if fault_type not in self._FAULT_TYPES:
            raise ValueError(f"Unknown fault type: {fault_type}, allowed: {self._FAULT_TYPES}")
        self._fault_handlers[fault_type] = handler

    def inject(self, fault_type: str, **kwargs) -> Any:
        if not self._enabled:
            return None
        handler = self._fault_handlers.get(fault_type)
        if handler is None:
            logging.warning("[ChaosFaultInjector] No handler for fault_type=%s", fault_type)
            return None
        with self._lock:
            self._injected_faults[fault_type] = {
                'timestamp': time.time(),
                'kwargs': kwargs,
            }
        logging.warning("[ChaosFaultInjector] Injecting fault: %s kwargs=%s", fault_type, kwargs)
        return handler()

    def get_injected_faults(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return dict(self._injected_faults)

    def clear(self) -> None:
        with self._lock:
            self._injected_faults.clear()


_chaos_injector: Optional[ChaosFaultInjector] = None
_chaos_injector_lock = threading.Lock()


def get_chaos_injector() -> ChaosFaultInjector:
    global _chaos_injector
    with _chaos_injector_lock:
        if _chaos_injector is None:
            _chaos_injector = ChaosFaultInjector()
        return _chaos_injector
