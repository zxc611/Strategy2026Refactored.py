"""
infra/_helpers.py — 运维基础设施合并模块

合并自:
  - _helpers.py (辅助函数和常量)
  - logging_utils.py (统一日志工厂 R9-5)
  - _disk_monitor.py (磁盘空间监控 R15-P0-RES-08)
  - _ops_knowledge_metrics.py (知识库/指标/SLA)
"""

from __future__ import annotations

import copy
import json
import logging
import os
import shutil
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional


# ============================================================
# Section 1: 统一日志工厂 (原 logging_utils.py)
# NOTE: get_logger 必须在 shared_utils import 之前定义，
#       因为 shared_utils 导入了 get_logger
# ============================================================

_default_handler_installed = False


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if level is not None:
        logger.setLevel(level)
    return logger


logger = get_logger(__name__)


# ============================================================
# 以下 import 放在 get_logger 定义之后，避免循环导入
# ============================================================

# FIX-20260702: 移除 serialization_utils 的顶层导入，改为函数内延迟导入，
# 切断 serialization_utils <-> _helpers 循环导入。

try:
    from lifecycle.product_initializer import get_product_params as _get_product_params
    _HAS_PRODUCT_PARAMS = True
except ImportError:
    _HAS_PRODUCT_PARAMS = False


# ============================================================
# Section 2: 辅助函数和常量 (原 _helpers.py)
# ============================================================

OPTION_PRODUCT_SPECS = {
    'IO': {'tick_size': 0.2, 'contract_size': 100},
    'HO': {'tick_size': 0.2, 'contract_size': 100},
    'MO': {'tick_size': 0.2, 'contract_size': 100},
    'CU': {'tick_size': 10.0, 'contract_size': 5},
    'AL': {'tick_size': 5.0, 'contract_size': 5},
    'ZN': {'tick_size': 5.0, 'contract_size': 5},
    'AU': {'tick_size': 0.02, 'contract_size': 1000},
    'AG': {'tick_size': 1.0, 'contract_size': 15},
    'RB': {'tick_size': 1.0, 'contract_size': 10},
    'RU': {'tick_size': 5.0, 'contract_size': 10},
    'MA': {'tick_size': 1.0, 'contract_size': 10},
    'TA': {'tick_size': 2.0, 'contract_size': 5},
    'OI': {'tick_size': 1.0, 'contract_size': 10},
    'RM': {'tick_size': 1.0, 'contract_size': 10},
    'SA': {'tick_size': 1.0, 'contract_size': 20},
    'FG': {'tick_size': 1.0, 'contract_size': 20},
    'SR': {'tick_size': 1.0, 'contract_size': 10},
    'CF': {'tick_size': 5.0, 'contract_size': 5},
    'AP': {'tick_size': 1.0, 'contract_size': 10},
    'CJ': {'tick_size': 5.0, 'contract_size': 5},
    'SF': {'tick_size': 2.0, 'contract_size': 5},
    'SM': {'tick_size': 2.0, 'contract_size': 5},
    'UR': {'tick_size': 1.0, 'contract_size': 5},
    'M': {'tick_size': 1.0, 'contract_size': 10},
    'Y': {'tick_size': 2.0, 'contract_size': 10},
    'P': {'tick_size': 2.0, 'contract_size': 10},
    'A': {'tick_size': 1.0, 'contract_size': 10},
    'L': {'tick_size': 1.0, 'contract_size': 5},
    'V': {'tick_size': 5.0, 'contract_size': 5},
    'PP': {'tick_size': 1.0, 'contract_size': 5},
    'EB': {'tick_size': 1.0, 'contract_size': 5},
    'I': {'tick_size': 0.5, 'contract_size': 100},
    'EG': {'tick_size': 1.0, 'contract_size': 5},
    'C': {'tick_size': 1.0, 'contract_size': 10},
    'CS': {'tick_size': 1.0, 'contract_size': 10},
}

DEFAULT_OPTION_SPEC = {'tick_size': 1.0, 'contract_size': 1.0}


def _get_option_spec(product: str) -> Dict[str, Any]:
    if _HAS_PRODUCT_PARAMS:
        return _get_product_params(product)
    return OPTION_PRODUCT_SPECS.get(product.upper(), DEFAULT_OPTION_SPEC)


def _normalize_code(code: str) -> str:
    if not code:
        return ''
    return str(code).strip()


def _get_data_service():
    """延迟导入get_data_service以打破循环导入。"""
    from data.data_service import get_data_service
    return _get_data_service()


def _has_data_service() -> bool:
    """检查data_service是否可用(延迟导入)。"""
    try:
        from data.data_service import get_data_service
        return True
    except ImportError:
        return False

# 兼容旧代码的DataService类型引用
DataService = None


def _result_to_pylist(result: Any) -> List[Dict[str, Any]]:
    if result is None:
        return []
    if hasattr(result, 'to_pylist'):
        return result.to_pylist()
    if hasattr(result, 'read_all'):
        materialized = result.read_all()
        if hasattr(materialized, 'to_pylist'):
            return materialized.to_pylist()
    if hasattr(result, 'to_dict'):
        return result.to_dict('records')
    return []


def _table_exists(ds: Any, table_name: str) -> bool:
    rows = _result_to_pylist(ds.query(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ))
    return bool(rows and int(rows[0].get('cnt', 0) or 0) > 0)


def create_archive_table(conn: Any, table_name: str, archive_table: str) -> None:
    # FIX-20260702: 延迟导入，切断 _helpers <-> shared_utils 循环
    from infra.shared_utils import sanitize_sql_identifier
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {sanitize_sql_identifier(archive_table)} AS "
        f"SELECT * FROM {sanitize_sql_identifier(table_name)} WHERE 1=0"
    )


# ============================================================
# Section 3: 磁盘空间监控 (原 _disk_monitor.py)
# ============================================================

class DiskSpaceMonitor:
    DEFAULT_CHECK_INTERVAL_SEC = 60.0
    DEFAULT_MIN_FREE_GB = 1.0

    def __init__(self, watch_paths: Optional[List[str]] = None,
                 min_free_gb: float = DEFAULT_MIN_FREE_GB,
                 check_interval_sec: float = DEFAULT_CHECK_INTERVAL_SEC,
                 persistence_file: Optional[str] = None):
        self._watch_paths = watch_paths or ['.']
        self._min_free_gb = min_free_gb
        self._check_interval_sec = check_interval_sec
        self._last_check_time = 0.0
        self._last_status: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._alert_callback: Optional[callable] = None
        self._persistence_file = persistence_file
        self._history: List[Dict[str, Any]] = []
        self._max_history_size = 1000
        self._load_history()

    def set_alert_callback(self, callback: callable) -> None:
        self._alert_callback = callback

    _last_summary_time = 0.0
    _SUMMARY_INTERVAL_SEC = 3600.0

    def _maybe_report_summary(self, status: Dict[str, Any]) -> None:
        now = time.time()
        if now - self._last_summary_time < self._SUMMARY_INTERVAL_SEC:
            return
        self.__class__._last_summary_time = now
        summary_parts = []
        for path, info in status.items():
            if 'error' in info:
                summary_parts.append(f"{path}=ERROR({info['error']})")
            else:
                summary_parts.append(f"{path}={info.get('free_gb', '?')}GB free({info.get('used_pct', '?')}% used)")
        logger.info("[Observability] DiskSpaceMonitor每小时摘要: %s", "; ".join(summary_parts))

    def check(self, force: bool = False) -> Dict[str, Any]:
        now = time.time()
        if not force:
            with self._lock:
                if now - self._last_check_time < self._check_interval_sec:
                    return self._last_status

        status = {}
        for path in self._watch_paths:
            try:
                usage = shutil.disk_usage(path)
                free_gb = usage.free / (1024 ** 3)
                total_gb = usage.total / (1024 ** 3)
                used_pct = (usage.used / usage.total * 100) if usage.total > 0 else 0
                is_low = free_gb < self._min_free_gb
                status[path] = {
                    'free_gb': round(free_gb, 2),
                    'total_gb': round(total_gb, 2),
                    'used_pct': round(used_pct, 1),
                    'is_low': is_low,
                }
                if is_low:
                    logger.warning(
                        "[R15-P0-RES-08] 磁盘空间不足! path=%s free=%.2fGB < min=%.2fGB",
                        path, free_gb, self._min_free_gb
                    )
                    if self._alert_callback:
                        try:
                            self._alert_callback(path, free_gb, self._min_free_gb)
                        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                            logging.debug("[R3-L2] disk_monitor alert callback suppressed: %s", _r3_err)
                            pass
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
                status[path] = {'error': str(e)}
                logger.warning("[Observability] DiskSpaceMonitor检测异常: path=%s error=%s", path, e)

        with self._lock:
            self._last_check_time = now
            self._last_status = status
            self._history.append({'timestamp': now, 'status': status})
            if len(self._history) > self._max_history_size:
                self._history = self._history[-self._max_history_size:]
            self._save_history()
        self._maybe_report_summary(status)
        return status

    def _load_history(self) -> None:
        if not self._persistence_file:
            return
        try:
            if os.path.exists(self._persistence_file):
                from infra.serialization_utils import json_loads
                with open(self._persistence_file, 'r', encoding='utf-8') as f:
                    self._history = json_loads(f.read())
                if len(self._history) > self._max_history_size:
                    self._history = self._history[-self._max_history_size:]
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.debug("[R16-P1-RES-17] 加载监控历史失败: %s", e)
            self._history = []

    def _save_history(self) -> None:
        if not self._persistence_file:
            return
        try:
            from infra.serialization_utils import json_dumps
            from infra.shared_utils import atomic_replace_file
            _result = atomic_replace_file(self._persistence_file, json_dumps(self._history[-self._max_history_size:]))
            if not _result.get('success'):
                logger.debug("[R16-P1-RES-17] 保存监控历史失败: %s", _result.get('error'))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.debug("[R16-P1-RES-17] 保存监控历史失败: %s", e)

    def get_history(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._history)


_disk_space_monitor: Optional[DiskSpaceMonitor] = None
_disk_space_monitor_lock = threading.Lock()


def get_disk_space_monitor() -> DiskSpaceMonitor:
    global _disk_space_monitor
    with _disk_space_monitor_lock:
        if _disk_space_monitor is None:
            _disk_space_monitor = DiskSpaceMonitor()
        return _disk_space_monitor


# ============================================================
# Section 4: 知识库/指标/SLA (原 _ops_knowledge_metrics.py)
# ============================================================

_OPS_KNOWLEDGE_BASE: Dict[str, Dict[str, Any]] = {
    'startup_failure': {
        'symptoms': ['策略无法启动', '初始化超时', '数据库连接失败'],
        'causes': ['数据库文件损坏', '配置文件缺失', '端口被占用'],
        'solutions': ['检查数据库文件完整性', '恢复默认配置', '释放端口占用'],
        'severity': 'HIGH',
    },
    'high_memory_usage': {
        'symptoms': ['内存使用率>80%', '系统响应变慢', 'OOM告警'],
        'causes': ['数据缓存过大', '内存泄漏', '并发请求过多'],
        'solutions': ['清理数据缓存', '重启策略进程', '降低并发数'],
        'severity': 'MEDIUM',
    },
    'disk_space_low': {
        'symptoms': ['磁盘空间不足告警', '写入失败', '日志无法写入'],
        'causes': ['日志文件过大', '备份数据过多', '临时文件未清理'],
        'solutions': ['清理旧日志', '删除过期备份', '清理临时文件'],
        'severity': 'HIGH',
    },
    'data_inconsistency': {
        'symptoms': ['持仓数据与实际不符', '信号计算异常', '诊断报告异常'],
        'causes': ['数据写入中断', '并发写入冲突', 'Schema版本不匹配'],
        'solutions': ['运行数据自愈', '恢复备份', '升级Schema版本'],
        'severity': 'CRITICAL',
    },
    'signal_loss': {
        'symptoms': ['信号未触发', '交易信号丢失', '过滤统计异常'],
        'causes': ['EventBus订阅失败', '回调异常', '速率限制触发'],
        'solutions': ['检查EventBus订阅状态', '查看错误日志', '调整速率限制'],
        'severity': 'HIGH',
    },
    'circuit_breaker_triggered': {
        'symptoms': ['交易暂停', '断路器告警', '策略降级运行'],
        'causes': ['回撤超限', '波动率异常', '连续亏损'],
        'solutions': ['检查回撤指标', '评估市场状况', '调整断路器参数'],
        'severity': 'CRITICAL',
    },
}


def search_ops_knowledge(keyword: str) -> List[Dict[str, Any]]:
    keyword_lower = keyword.lower()
    results = []
    for key, entry in _OPS_KNOWLEDGE_BASE.items():
        searchable = ' '.join([
            key, ' '.join(entry.get('symptoms', [])),
            ' '.join(entry.get('causes', [])),
            ' '.join(entry.get('solutions', [])),
        ]).lower()
        if keyword_lower in searchable:
            results.append({'id': key, **entry})
    return results


def get_ops_knowledge(issue_id: str) -> Optional[Dict[str, Any]]:
    entry = _OPS_KNOWLEDGE_BASE.get(issue_id)
    if entry:
        return {'id': issue_id, **entry}
    return None


class OpsMetrics:
    METRIC_CPU_USAGE_PCT = 'cpu_usage_pct'
    METRIC_MEMORY_USAGE_PCT = 'memory_usage_pct'
    METRIC_DISK_USAGE_PCT = 'disk_usage_pct'
    METRIC_DISK_FREE_GB = 'disk_free_gb'
    METRIC_STRATEGY_UPTIME_SEC = 'strategy_uptime_sec'
    METRIC_SIGNAL_COUNT = 'signal_count'
    METRIC_TRADE_COUNT = 'trade_count'
    METRIC_ORDER_SUCCESS_RATE = 'order_success_rate'
    METRIC_MAX_DRAWDOWN_PCT = 'max_drawdown_pct'
    METRIC_SHARPE_RATIO = 'sharpe_ratio'
    METRIC_WIN_RATE = 'win_rate'
    METRIC_CIRCUIT_BREAKER_COUNT = 'circuit_breaker_count'
    METRIC_DB_SIZE_MB = 'db_size_mb'
    METRIC_TICK_LATENCY_MS = 'tick_latency_ms'
    METRIC_EVENT_BUS_QUEUE_SIZE = 'event_bus_queue_size'
    METRIC_OPS_OPERATION_COUNT = 'ops_operation_count'
    METRIC_BACKUP_LAST_TIME = 'backup_last_time'
    METRIC_HEALTH_CHECK_STATUS = 'health_check_status'

    UNITS = {
        'cpu_usage_pct': '%',
        'memory_usage_pct': '%',
        'disk_usage_pct': '%',
        'disk_free_gb': 'GB',
        'strategy_uptime_sec': 's',
        'signal_count': '个',
        'trade_count': '笔',
        'order_success_rate': '%',
        'max_drawdown_pct': '%',
        'sharpe_ratio': '',
        'win_rate': '%',
        'circuit_breaker_count': '次',
        'db_size_mb': 'MB',
        'tick_latency_ms': 'ms',
        'event_bus_queue_size': '个',
        'ops_operation_count': '次',
    }


class OpsSLA:
    AVAILABILITY_TARGET_PCT = 99.9
    AVAILABILITY_MAX_DOWNTIME_MIN_PER_MONTH = 43.8
    INCIDENT_RESPONSE_TIME_MIN = 15
    INCIDENT_RESOLUTION_TIME_MIN = 60
    CRITICAL_INCIDENT_RESPONSE_TIME_MIN = 5
    DATA_BACKUP_INTERVAL_HOURS = 1
    DATA_RECOVERY_TIME_OBJECTIVE_MIN = 30
    DATA_RECOVERY_POINT_OBJECTIVE_MIN = 60
    TICK_PROCESSING_LATENCY_MS = 100
    SIGNAL_GENERATION_LATENCY_MS = 500
    ORDER_EXECUTION_LATENCY_MS = 1000
    HEALTH_CHECK_INTERVAL_SEC = 60
    ALERT_NOTIFICATION_DELAY_SEC = 30
    PARAM_CHANGE_AUDIT_RETENTION_DAYS = 90
    OPS_LOG_RETENTION_DAYS = 30
    BACKUP_RETENTION_DAYS = 30

    @classmethod
    def check_sla_compliance(cls, metric_name: str, actual_value: float) -> Dict[str, Any]:
        sla_targets = {
            'tick_latency_ms': cls.TICK_PROCESSING_LATENCY_MS,
            'signal_latency_ms': cls.SIGNAL_GENERATION_LATENCY_MS,
            'order_latency_ms': cls.ORDER_EXECUTION_LATENCY_MS,
            'health_check_interval_sec': cls.HEALTH_CHECK_INTERVAL_SEC,
            'alert_delay_sec': cls.ALERT_NOTIFICATION_DELAY_SEC,
        }
        target = sla_targets.get(metric_name)
        if target is None:
            return {
                'compliant': True,
                'metric': metric_name,
                'actual': actual_value,
                'target': None,
                'note': '未定义SLA目标',
            }
        compliant = actual_value <= target
        return {
            'compliant': compliant,
            'metric': metric_name,
            'actual': actual_value,
            'target': target,
        }
