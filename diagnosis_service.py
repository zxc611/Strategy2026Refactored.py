"""
策略诊断服务 - CQRS 架构 Diagnosis 层
来源：分散的诊断逻辑（strategy_lifecycle_mixin.py 中的诊断函数）
功能：策略运行诊断、断点检测、问题排查、健康报告

重构 v2.0 (2026-05-07):
- ✅ DiagnosisProbeManager + 12环节诊断函数 → diagnosis_probe.py
- ✅ 周期性诊断 + 宽限期 + 资源快照 → diagnosis_periodic.py
- ✅ 本模块保留：核心类 + 装饰器 + tick探针 + 重新导出
- ✅ 向后兼容：所有 from ali2026v3_trading.diagnosis_service import xxx 不变
"""
from __future__ import annotations

import functools
import logging
import os
import queue
import threading
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Any, Optional, Protocol, Set, Tuple, runtime_checkable
from collections import deque

from ali2026v3_trading.shared_utils import normalize_instrument_id

from ali2026v3_trading.diagnosis_probe import (
    MONITORED_CONTRACTS, MONITORED_CONTRACT_COUNT, MONITORED_DIAG_LABEL,
    MONITORED_CONTRACT_SET,
    DiagnosisProbeManager,
    is_monitored_contract, get_contract_info,
    diagnose_parse_transform, diagnose_parse_failure,
    validate_contract_format, log_diagnostic_event,
    diagnose_format_validation, diagnose_config_file_presence,
    diagnose_config_loaded_status, diagnose_pre_registration,
    diagnose_subscription, diagnose_registration, diagnose_id_lookup,
    diagnose_tick_entry, diagnose_tick_buffer, diagnose_tick_flush,
    diagnose_storage_enqueue, diagnose_async_write, diagnose_duckdb_insert,
)

from ali2026v3_trading.diagnosis_periodic import (
    run_14_contracts_periodic_diagnostic,
    reset_diagnosis_grace_period,
    ResourceOwnershipScanner,
    ControlActionLogger,
    _get_runtime_state,
    _emit_periodic_resource_ownership_snapshot,
)


# ============================================================================
# 核心类定义
# ============================================================================

@dataclass
class DiagnoserConfig:
    """诊断器配置类"""
    max_logs: int = 100
    health_weights: Dict[str, int] = field(default_factory=lambda: {
        'CRITICAL': 25,
        'HIGH': 12,
        'MEDIUM': 6,
        'LOW': 2,
        'ERROR': 15,
        'WARNING': 2
    })
    enable_async_log: bool = True
    log_queue_size: int = 1000
    event_publish_timeout: float = 5.0
    enable_incremental_diagnosis: bool = False


class StrategyProtocol(Protocol):
    """策略协议接口（类型约束）"""
    infini: Optional[Any]
    market_center: Optional[Any]
    params: Optional[Any]
    future_instruments: List[str]
    option_instruments: Dict[str, Any]
    kline_data: Dict[str, Any]
    my_state: str
    my_is_running: bool
    my_trading: bool

    def output(self, msg: str) -> None: ...
    def subscribe(self, *args, **kwargs) -> None: ...


class DiagnosisReport(object):
    """诊断报告数据类"""

    def __init__(self, config: Optional[DiagnoserConfig] = None):
        self.config = config or DiagnoserConfig()
        self.timestamp = datetime.now().isoformat()
        self.breakpoints: List[Dict] = []
        self.warnings: List[str] = []
        self.errors: List[str] = []
        self.recommendations: List[str] = []
        self.logs: List[str] = []
        self.metrics: Dict[str, Any] = {}
        self._log_queue: Optional[queue.Queue] = None
        self._logs_lock = threading.Lock()

        if self.config.enable_async_log:
            self._log_queue = queue.Queue(maxsize=self.config.log_queue_size)

    def add_breakpoint(self, layer: str, component: str, issue: str,
                       impact: str, severity: str, fix: str = "") -> None:
        self.breakpoints.append({
            "layer": layer,
            "component": component,
            "issue": issue,
            "impact": impact,
            "severity": severity,
            "fix": fix
        })

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)

    def add_recommendation(self, msg: str) -> None:
        self.recommendations.append(msg)

    def add_log(self, msg: str, level: str = "INFO") -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] [{level}] {msg}"

        if self._log_queue is not None:
            try:
                self._log_queue.put_nowait(log_entry)
            except queue.Full:
                logging.warning("[Diagnoser] Log queue full, discarding old logs")

        with self._logs_lock:
            max_logs = self.config.max_logs
            if len(self.logs) >= max_logs:
                critical_logs = [l for l in self.logs if '[ERROR]' in l or '[WARN]' in l]
                other_logs = [l for l in self.logs if '[ERROR]' not in l and '[WARN]' not in l]
                retained = critical_logs + other_logs[-(max_logs//2):]
                self.logs = retained

            self.logs.append(log_entry)

    def add_metric(self, key: str, value: Any) -> None:
        self.metrics[key] = value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "breakpoints": self.breakpoints,
            "warnings": self.warnings,
            "errors": self.errors,
            "recommendations": self.recommendations,
            "logs": self.logs[-100:],
            "metrics": self.metrics,
            "summary": {
                "breakpoint_count": len(self.breakpoints),
                "warning_count": len(self.warnings),
                "error_count": len(self.errors),
                "recommendation_count": len(self.recommendations)
            }
        }

    def get_health_score(self) -> float:
        base_score = 100.0
        weights = self.config.health_weights

        for bp in self.breakpoints:
            weight = weights.get(bp['severity'], 5)
            base_score -= weight

        base_score -= len(self.errors) * weights.get('ERROR', 15)
        base_score -= len(self.warnings) * weights.get('WARNING', 2)

        return max(0.0, min(100.0, base_score))


class StrategyDiagnoser:
    """策略诊断器 - Core 层辅助服务

    职责:
    - 全链路诊断（数据源、合约、订阅、K 线）
    - 断点检测
    - 问题排查
    - 健康评分
    - 自动修复建议

    设计原则:
    - Composition over Inheritance
    - 单一职责
    - 线程安全
    - 非侵入式诊断
    """

    def __init__(self, strategy_instance: Optional[StrategyProtocol] = None,
                 event_bus: Optional[Any] = None,
                 config: Optional[DiagnoserConfig] = None):
        self.strategy = strategy_instance
        self.config = config or DiagnoserConfig()
        self.report = DiagnosisReport(self.config)
        self._lock = threading.RLock()
        self._event_bus = event_bus
        self._last_diagnosis_result: Dict[str, Any] = {}
        self._log_worker_thread: Optional[threading.Thread] = None
        self._stop_log_worker = threading.Event()

        self._strategy_initialized = self._check_strategy_initialization()

        if self.config.enable_async_log and self.report._log_queue:
            self._start_log_worker()

    def _check_strategy_initialization(self) -> bool:
        if self.strategy is None:
            return False

        required_attrs = ['infini', 'market_center', 'params', 'future_instruments',
                         'option_instruments', 'kline_data', 'my_state', 'my_is_running',
                         'my_trading']

        for attr in required_attrs:
            if not hasattr(self.strategy, attr):
                self._log(f"策略缺少属性: {attr}", "WARN")
                return False

        return True

    def _publish_event_with_retry(self, event_type: str, data: Dict[str, Any], max_retries: int = 3) -> None:
        if not self._event_bus:
            return

        retry_count = 0
        last_error = None

        while retry_count < max_retries:
            try:
                event = type(event_type, (), {
                    'type': event_type,
                    'timestamp': datetime.now().isoformat(),
                    **data
                })()

                self._event_bus.publish(event, async_mode=True)
                return

            except Exception as e:
                last_error = e
                retry_count += 1

                if retry_count < max_retries:
                    wait_time = 0.5 * (2 ** (retry_count - 1))
                    logging.warning(f"[StrategyDiagnoser] Event publish failed (attempt {retry_count}/{max_retries}): {e}")
                    time.sleep(wait_time)
                else:
                    logging.error(f"[StrategyDiagnoser] Event publish failed after {max_retries} retries: {e}")
                    logging.debug(f"[StrategyDiagnoser] Failed event: {event_type}, data_keys={list(data.keys())}")

    def _start_log_worker(self) -> None:
        def log_worker():
            while not self._stop_log_worker.is_set():
                try:
                    log_entry = self.report._log_queue.get(timeout=1.0)
                    if '[ERROR]' in log_entry:
                        logging.error(f"[Diagnoser] {log_entry}")
                    elif '[WARN]' in log_entry:
                        logging.warning(f"[Diagnoser] {log_entry}")
                    else:
                        logging.info(f"[Diagnoser] {log_entry}")
                except queue.Empty:
                    logging.debug("[Diagnoser] Log queue empty, waiting...")
                    continue
                except Exception as e:
                    logging.error(f"[Diagnoser] Log worker error: {e}")

        self._log_worker_thread = threading.Thread(target=log_worker, daemon=True)
        self._log_worker_thread.start()

    def _stop_log_workers(self) -> None:
        if self._log_worker_thread:
            self._stop_log_worker.set()
            self._log_worker_thread.join(timeout=2.0)

    def _log(self, msg: str, level: str = "INFO") -> None:
        with self._lock:
            self.report.add_log(msg, level)

        if self.strategy and hasattr(self.strategy, 'output'):
            try:
                self.strategy.output(f"[Diagnoser] {msg}")
            except (AttributeError, TypeError, ValueError) as e:
                logging.error(f"[Diagnoser] 输出日志失败：{e}")
                logging.error(f"[Diagnoser] {msg}")
        else:
            log_func = {
                'DEBUG': logging.debug,
                'INFO': logging.info,
                'WARN': logging.warning,
                'WARNING': logging.warning,
                'ERROR': logging.error,
                'CRITICAL': logging.critical
            }.get(level.upper(), logging.info)

            log_func(f"[Diagnoser] {msg}")

    def _run_incremental_diagnosis(self, start_time: float) -> Dict[str, Any]:
        self._log("\n【增量诊断模式】", "INFO")

        if self.strategy:
            current_trading = getattr(self.strategy, 'my_trading', False)
            last_trading = self._last_diagnosis_result.get('state', {}).get('my_trading', False)

            if current_trading != last_trading:
                self._log(f"交易状态变化：{last_trading} -> {current_trading}", "INFO")
                self._diagnose_state()
            else:
                self._log("状态无显著变化，跳过详细诊断", "INFO")

        elapsed = time.time() - start_time
        self.report.add_metric('diagnosis_duration_ms', elapsed * 1000)
        self.report.add_metric('incremental_mode', True)

        self._log("=" * 60)
        self._log("增量诊断完成")
        self._log(f"健康评分：{self.report.get_health_score():.1f}/100")
        self._log("=" * 60)

        result = self.report.to_dict()
        self._last_diagnosis_result = result

        return result

    def run_full_diagnosis(self, force_full: bool = False) -> Dict[str, Any]:
        with self._lock:
            self._log("=" * 60)
            self._log("开始策略全链路诊断")
            self._log("=" * 60)

            start_time = time.time()

            if (self.config.enable_incremental_diagnosis and
                not force_full and
                self._last_diagnosis_result):
                return self._run_incremental_diagnosis(start_time)

            if self.strategy is None:
                self._log("警告：策略实例为 None，仅运行基础检查", "WARN")
                self._diagnose_basic()
            else:
                self._diagnose_data_source()
                self._diagnose_instruments()
                self._diagnose_subscriptions()
                self._diagnose_klines()
                self._diagnose_state()

            elapsed = time.time() - start_time
            self.report.add_metric('diagnosis_duration_ms', elapsed * 1000)

            self._generate_recommendations()

            self._log("=" * 60)
            self._log("诊断完成")
            self._log(f"断点：{len(self.report.breakpoints)}")
            self._log(f"警告：{len(self.report.warnings)}")
            self._log(f"错误：{len(self.report.errors)}")
            self._log(f"健康评分：{self.report.get_health_score():.1f}/100")
            self._log("=" * 60)

            result = self.report.to_dict()
            self._last_diagnosis_result = result

            self._publish_event_with_retry('DiagnosisCompleted', result)

            return result

    def _diagnose_basic(self) -> None:
        self._log("\n【基础诊断】")

        import sys
        self._log(f"Python 版本：{sys.version}")
        self.report.add_metric('python_version', sys.version.split()[0])

        modules = ['threading', 'datetime', 'collections', 'typing']
        for mod in modules:
            try:
                __import__(mod)
                self._log(f"模块 {mod}: OK")
            except ImportError as e:
                error_msg = f"模块 {mod} 不可用"
                self.report.add_error(error_msg)
                logging.error(f"[Diagnoser] {error_msg}: {e}")

    def _diagnose_data_source(self) -> None:
        self._log("\n【数据源诊断】")
        s = self.strategy

        infini = getattr(s, 'infini', None)
        if infini is None:
            self.report.add_warning("infini 对象为 None")
            self.report.add_breakpoint(
                "DataSource", "infini", "infini 对象为 None",
                "无法访问交易平台", "CRITICAL",
                "检查平台连接配置"
            )
        else:
            self._log("infini 对象：OK")

        mc = getattr(s, 'market_center', None)
        if mc is None:
            self.report.add_warning("market_center 为 None")
            self.report.add_breakpoint(
                "DataSource", "market_center", "market_center 为 None",
                "无法获取行情数据", "HIGH",
                "检查 MarketCenter 初始化"
            )
        else:
            self._log("market_center: OK")

    def _diagnose_instruments(self) -> None:
        self._log("\n【合约加载诊断】")
        s = self.strategy

        params = getattr(s, 'params', None)
        if params is None:
            self.report.add_breakpoint(
                "Instrument", "params", "参数对象为 None",
                "无法获取配置", "CRITICAL",
                "检查参数初始化"
            )
            return

        key_params = ['subscribe_options', 'future_products', 'option_products']
        for p in key_params:
            val = getattr(params, p, None)
            if val is None:
                self.report.add_warning(f"参数 {p} 未设置")
            else:
                self._log(f"参数 {p}: {val}")

        fut_insts = getattr(s, 'future_instruments', None)
        if fut_insts is None:
            self.report.add_breakpoint(
                "Instrument", "future_instruments", "期货合约列表为 None",
                "无法计算期权宽度", "CRITICAL",
                "检查期货合约加载逻辑"
            )
        elif len(fut_insts) == 0:
            self.report.add_breakpoint(
                "Instrument", "future_instruments", "期货合约列表为空",
                "无标的期货数据", "HIGH",
                "检查合约代码配置"
            )
        else:
            self._log(f"期货合约数：{len(fut_insts)}")

        opt_insts = getattr(s, 'option_instruments', None)
        if opt_insts is None:
            self.report.add_breakpoint(
                "Instrument", "option_instruments", "期权合约字典为 None",
                "无法加载期权数据", "CRITICAL",
                "检查期权合约加载逻辑"
            )
        elif len(opt_insts) == 0:
            self.report.add_breakpoint(
                "Instrument", "option_instruments", "期权合约字典为空",
                "无期权数据可用", "HIGH",
                "检查期权合约代码配置"
            )
        else:
            self._log(f"期权合约数：{len(opt_insts)}")

    def _diagnose_subscriptions(self) -> None:
        self._log("\n【订阅状态诊断】")
        s = self.strategy

        if not hasattr(s, 'subscribe'):
            self.report.add_breakpoint(
                "Subscription", "subscribe", "缺少 subscribe 方法",
                "无法订阅行情", "CRITICAL",
                "确保继承 BaseStrategy"
            )
            return

        has_subscribe = hasattr(s, 'subscribe') and callable(getattr(s, 'subscribe', None))
        if not has_subscribe:
            self.report.add_warning("策略缺少 subscribe() 方法")
            self.report.add_breakpoint(
                "Subscription", "subscribe_method", "缺少订阅方法",
                "无法订阅行情", "MEDIUM",
                "确保继承 BaseStrategy"
            )
        else:
            self._log("订阅方法：OK")

    def _diagnose_klines(self) -> None:
        self._log("\n【K 线数据诊断】")
        s = self.strategy

        kline_data = getattr(s, 'kline_data', None)
        if kline_data is None:
            self.report.add_breakpoint(
                "Kline", "kline_data", "K 线字典为 None",
                "无法获取 K 线数据", "CRITICAL",
                "检查 K 线初始化"
            )
            return

        if len(kline_data) == 0:
            self.report.add_warning("K 线数据为空")
        else:
            self._log(f"K 线合约数：{len(kline_data)}")

            for inst_id, kline in list(kline_data.items())[:3]:
                if hasattr(kline, 'close'):
                    close_array = kline.close
                    if close_array and len(close_array) > 0:
                        latest_price = close_array[-1]
                        self._log(f"{inst_id} 最新价：{latest_price}")
                    else:
                        self.report.add_warning(f"{inst_id} K 线数据为空")
                else:
                    self.report.add_warning(f"{inst_id} K 线对象无效")

    def _diagnose_state(self) -> None:
        self._log("\n【状态管理诊断】")
        s = self.strategy

        state_attributes = ['my_state', 'my_is_running', 'my_trading']
        for attr in state_attributes:
            val = getattr(s, attr, None)
            if val is None:
                self.report.add_warning(f"状态 {attr} 未定义")
            else:
                self._log(f"{attr}: {val}")

        trading = getattr(s, 'my_trading', False)
        if not trading:
            self.report.add_warning("当前不在交易状态")

    def _generate_recommendations(self) -> None:
        self._log("\n【生成修复建议】")

        for bp in self.report.breakpoints:
            if bp['severity'] == 'CRITICAL':
                self.report.add_recommendation(f"[紧急] {bp['fix']} - {bp['component']}: {bp['issue']}")
            elif bp['severity'] == 'HIGH':
                self.report.add_recommendation(f"[重要] {bp['fix']} - {bp['component']}")

        if len(self.report.warnings) > 5:
            self.report.add_recommendation("[建议] 检查策略初始化流程，确保所有组件正确加载")

        if len(self.report.errors) > 0:
            self.report.add_recommendation("[严重] 优先解决所有错误，然后处理警告")

        health_score = self.report.get_health_score()
        if health_score >= 90:
            self._log("健康状态：优秀", "INFO")
        elif health_score >= 70:
            self._log("健康状态：良好", "INFO")
            self.report.add_recommendation("[建议] 处理 HIGH 级别断点可提升健康度")
        elif health_score >= 50:
            self._log("健康状态：一般", "WARN")
            self.report.add_recommendation("[注意] 建议处理 CRITICAL 和 HIGH 级别断点")
        else:
            self._log("健康状态：较差", "ERROR")
            self.report.add_recommendation("[紧急] 立即处理所有 CRITICAL 级别断点")

    def get_quick_report(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "timestamp": datetime.now().isoformat(),
                "health_score": self.report.get_health_score(),
                "breakpoint_count": len(self.report.breakpoints),
                "warning_count": len(self.report.warnings),
                "error_count": len(self.report.errors),
                "critical_count": sum(1 for bp in self.report.breakpoints if bp['severity'] == 'CRITICAL')
            }


# ============================================================================
# __all__ 导出
# ============================================================================

__all__ = [
    'DiagnosisReport',
    'StrategyDiagnoser',
    'DiagnosisProbeManager',
    'handle_import_errors',
    'log_exceptions',
    'safe_call',
    'optional_import',
    'required_import',
    'run_14_contracts_periodic_diagnostic',
    'reset_diagnosis_grace_period',
    'is_monitored_contract',
    'record_tick_probe',
    'ControlActionLogger',
    'ResourceOwnershipScanner',
    'MONITORED_CONTRACTS',
    'MONITORED_CONTRACT_COUNT',
    'MONITORED_DIAG_LABEL',
]


# ============================================================================
# 异常处理装饰器
# ============================================================================

def handle_import_errors(allow_failure=False, default_value=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ImportError as e:
                if allow_failure:
                    logging.warning(f"[import] Optional import failed: {e}")
                    return default_value
                else:
                    logging.error(f"[import] Required import failed: {e}")
                    logging.exception("[import] Traceback:")
                    raise
        return wrapper
    return decorator


def log_exceptions(log_level=logging.ERROR, reraise=True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.log(log_level, f"[exception] {func.__name__} failed: {e}")
                logging.exception(f"[exception] {func.__name__} traceback:")

                if reraise:
                    raise
                return None
        return wrapper
    return decorator


def safe_call(default_value=None, catch_exceptions=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except catch_exceptions:
                return default_value
        return wrapper
    return decorator


def optional_import(func):
    return handle_import_errors(allow_failure=True, default_value=None)(func)


def required_import(func):
    return handle_import_errors(allow_failure=False)(func)


# ============================================================================
# Tick探针管理
# ============================================================================

_tick_probe_stats = {
    'entry': {'count': 0, 'last_log': 0},
    'dispatched': {'count': 0, 'last_log': 0},
    'saved': {'count': 0, 'last_log': 0},
    'error': {'count': 0, 'last_log': 0, 'errors': []}
}

_tick_probe_interval = 30.0
_tick_probe_lock = threading.Lock()


def record_tick_probe(stage: str, instrument_id: str, price: float, error_msg: Optional[str] = None) -> None:
    now = time.time()
    if stage not in _tick_probe_stats:
        return

    with _tick_probe_lock:
        stats = _tick_probe_stats[stage]
        stats['count'] += 1

        if stage == 'error' and error_msg:
            stats['errors'].append({'time': now, 'instrument_id': instrument_id, 'price': price, 'error': error_msg})
            if len(stats['errors']) > 100:
                stats['errors'] = stats['errors'][-50:]
            logging.error(f"[PROBE_TICK_ERROR] {instrument_id} @ {price}: {error_msg}")

        if now - stats.get('last_log', 0) >= _tick_probe_interval:
            count = stats['count']
            if count > 0:
                if stage == 'error':
                    errors = stats['errors']
                    if errors:
                        for err in errors[-5:]:
                            logging.error(f"[PROBE_TICK_SUMMARY] ERROR | {err['instrument_id']} @ {err['price']} | {err['error']}")
                        logging.error(f"[PROBE_TICK_SUMMARY] Total errors in last 30s: {len(errors)} (showing last 5)")
                else:
                    names = {'entry': 'Entry', 'dispatched': 'Dispatched', 'saved': 'Saved'}
                    logging.info(f"[PROBE_TICK_SUMMARY] {names.get(stage, stage)} | Count: {count} ticks in last 30s")
            stats['count'] = 0
            stats['last_log'] = now
