"""
ops_knowledge_metrics.py - 知识库/指标/SLA
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5


logger = get_logger(__name__)  # R9-5


# ============================================================================
# P2修复: 运维就绪度 — 知识库、指标定义、SLA
# ============================================================================

# P2修复: 运维操作指南注释
# 运维操作指南:
# 1. 启动检查: 调用 StorageMaintenanceService.run_startup_checks() 确认数据完整性
# 2. 日常巡检: 调用 StorageMaintenanceService.run_daily_maintenance() 执行日常维护
# 3. 磁盘监控: 调用 get_disk_space_monitor().check() 检查磁盘空间
# 4. 数据备份: 调用 get_backup_service().backup_duckdb() 执行数据备份
# 5. 孤儿清理: 调用 StorageMaintenanceService.clean_orphaned_data() 清理孤儿数据
# 6. 参数变更: 通过 OpsOperationManager 记录运维操作审计
# 7. 健康检查: 调用 StorageMaintenanceService.get_health_status() 获取健康状态
# 8. 紧急停止: 设置 strategy.my_is_running = False 停止策略
# 9. 日志排查: 检查 logs/strategy.log 中的ERROR级别日志
# 10. 版本确认: 调用 ConfigService.check_version_alignment() 确认版本一致

# P2修复: 运维知识库数据结构
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
    """P2修复: 搜索运维知识库

    Args:
        keyword: 搜索关键词

    Returns:
        匹配的知识条目列表
    """
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
    """P2修复: 获取指定运维知识条目

    Args:
        issue_id: 问题ID

    Returns:
        知识条目字典，不存在返回None
    """
    entry = _OPS_KNOWLEDGE_BASE.get(issue_id)
    if entry:
        return {'id': issue_id, **entry}
    return None





# P2修复: 运维指标标准化常量
class OpsMetrics:
    """P2修复: 运维指标标准化定义"""

    # 系统级指标
    METRIC_CPU_USAGE_PCT = 'cpu_usage_pct'
    METRIC_MEMORY_USAGE_PCT = 'memory_usage_pct'
    METRIC_DISK_USAGE_PCT = 'disk_usage_pct'
    METRIC_DISK_FREE_GB = 'disk_free_gb'

    # 策略级指标
    METRIC_STRATEGY_UPTIME_SEC = 'strategy_uptime_sec'
    METRIC_SIGNAL_COUNT = 'signal_count'
    METRIC_TRADE_COUNT = 'trade_count'
    METRIC_ORDER_SUCCESS_RATE = 'order_success_rate'

    # 风控指标
    METRIC_MAX_DRAWDOWN_PCT = 'max_drawdown_pct'
    METRIC_SHARPE_RATIO = 'sharpe_ratio'
    METRIC_WIN_RATE = 'win_rate'
    METRIC_CIRCUIT_BREAKER_COUNT = 'circuit_breaker_count'

    # 数据指标
    METRIC_DB_SIZE_MB = 'db_size_mb'
    METRIC_TICK_LATENCY_MS = 'tick_latency_ms'
    METRIC_EVENT_BUS_QUEUE_SIZE = 'event_bus_queue_size'

    # 运维指标
    METRIC_OPS_OPERATION_COUNT = 'ops_operation_count'
    METRIC_BACKUP_LAST_TIME = 'backup_last_time'
    METRIC_HEALTH_CHECK_STATUS = 'health_check_status'

    # 指标单位
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





# P2修复: SLA定义
class OpsSLA:
    """P2修复: 运维SLA常量定义"""

    # 可用性SLA
    AVAILABILITY_TARGET_PCT = 99.9           # 年度可用性目标99.9%
    AVAILABILITY_MAX_DOWNTIME_MIN_PER_MONTH = 43.8  # 每月最大停机时间(分钟)

    # 响应时间SLA
    INCIDENT_RESPONSE_TIME_MIN = 15          # 事件响应时间15分钟
    INCIDENT_RESOLUTION_TIME_MIN = 60        # 事件解决时间60分钟
    CRITICAL_INCIDENT_RESPONSE_TIME_MIN = 5  # 严重事件响应时间5分钟

    # 数据SLA
    DATA_BACKUP_INTERVAL_HOURS = 1           # 数据备份间隔1小时
    DATA_RECOVERY_TIME_OBJECTIVE_MIN = 30    # 数据恢复时间目标30分钟
    DATA_RECOVERY_POINT_OBJECTIVE_MIN = 60   # 数据恢复点目标60分钟

    # 性能SLA
    TICK_PROCESSING_LATENCY_MS = 100         # Tick处理延迟<100ms
    SIGNAL_GENERATION_LATENCY_MS = 500       # 信号生成延迟<500ms
    ORDER_EXECUTION_LATENCY_MS = 1000        # 订单执行延迟<1000ms

    # 监控SLA
    HEALTH_CHECK_INTERVAL_SEC = 60           # 健康检查间隔60秒
    ALERT_NOTIFICATION_DELAY_SEC = 30        # 告警通知延迟<30秒

    # 运维操作SLA
    PARAM_CHANGE_AUDIT_RETENTION_DAYS = 90   # 参数变更审计保留90天
    OPS_LOG_RETENTION_DAYS = 30              # 运维日志保留30天
    BACKUP_RETENTION_DAYS = 30               # 备份保留30天

    @classmethod
    def check_sla_compliance(cls, metric_name: str, actual_value: float) -> Dict[str, Any]:
        """P2修复: 检查SLA合规性

        Args:
            metric_name: 指标名称
            actual_value: 实际值

        Returns:
            Dict: {compliant: bool, metric: str, actual: float, target: float}
        """
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
        # 延迟类指标: 实际值应小于目标值
        compliant = actual_value <= target
        return {
            'compliant': compliant,
            'metric': metric_name,
            'actual': actual_value,
            'target': target,
        }


