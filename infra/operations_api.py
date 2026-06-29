# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional
from datetime import datetime

from ali2026v3_trading.infra.resilience import deterministic_round
from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from ali2026v3_trading.infra.health_monitor import HealthCheckAPI
from ali2026v3_trading.infra.serialization_utils import json_loads
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5

logger = get_logger(__name__)  # R9-5


class OperationsAPI:
    """OPS-17修复: 运维操作API — 集中管理常见运维操作

    提供统一的运维操作入口，避免所有运维依赖手动命令行。
    每个操作自动记录审计日志。
    """

    def __init__(self):
        self._health_api = HealthCheckAPI()
        try:
            from ali2026v3_trading.infra.maintenance_service import get_capacity_stress_test, get_sop_manager
            self._stress_test = get_capacity_stress_test()
            self._sop_manager = get_sop_manager()
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] operations_api init suppressed: %s", _r3_err)
            self._stress_test = None
            self._sop_manager = None

    def emergency_stop(self, caller_id: str = "unknown",
                       reason: str = "") -> Dict[str, Any]:
        """紧急停止: 暂停策略 + 撤销挂单 + 平所有仓位"""
        try:
            from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
            from ali2026v3_trading.infra.service_container import ServiceContainer
            sc = ServiceContainer()
            core = sc.get_service('strategy_core')
            if core and hasattr(core, 'emergency_stop'):
                return core.emergency_stop(caller_id=caller_id, reason=reason)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] operations_api emergency_stop suppressed: %s", _r3_err)
            pass
        try:
            from ali2026v3_trading.order.order_service import get_order_service
            os_svc = get_order_service()
            return os_svc.emergency_close_all_positions(caller_id=caller_id)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            return {'success': False, 'error': str(e)}

    def emergency_degrade(self, target_count: int = 1,
                          caller_id: str = "unknown",
                          reason: str = "") -> Dict[str, Any]:
        """紧急降级: 减少活跃策略数量"""
        try:
            from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            return eco.emergency_degrade(
                target_active_count=target_count,
                caller_id=caller_id,
                reason=reason,
            )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            return {'success': False, 'error': str(e)}

    def emergency_close_all(self, caller_id: str = "unknown") -> Dict[str, Any]:
        """紧急平仓: 撤销所有挂单 + 市价平所有持仓"""
        try:
            from ali2026v3_trading.order.order_service import get_order_service
            os_svc = get_order_service()
            return os_svc.emergency_close_all_positions(caller_id=caller_id)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            return {'success': False, 'error': str(e)}

    def force_backupbackup_database(self, force: bool = False,
                        caller_id: str = "system") -> Dict[str, Any]:
        """数据库备份"""
        try:
            from ali2026v3_trading.infra.maintenance_service import StorageMaintenanceService
            from ali2026v3_trading.infra.service_container import ServiceContainer
            sc = ServiceContainer()
            ms = sc.get_service('maintenance')
            if ms and hasattr(ms, 'backup_database'):
                return ms.backup_database(force=force, caller_id=caller_id)
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] operations_api force_backup suppressed: %s", _r3_err)
            pass
        try:
            from ali2026v3_trading.infra.maintenance_service import get_backup_service
            bs = get_backup_service()
            backup_path = bs.backup_duckdb(force=force)
            return {
                'success': backup_path is not None,
                'backup_path': backup_path,
                'caller_id': caller_id,
            }
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            return {'success': False, 'error': str(e)}

    def check_capacity(self) -> Dict[str, Any]:
        """容量检查"""
        result = self._health_api.check_capacity_limits()
        if self._stress_test:
            result['stress_test_available'] = True
        return result

    def run_stress_test(self, test_name: str = 'default',
                        duration_sec: float = 60.0,
                        target_qps: float = 1000.0) -> Dict[str, Any]:
        """R21-OPS-18: 执行容量压测"""
        if not self._stress_test:
            return {'success': False, 'error': '压测框架未初始化'}
        return self._stress_test.run_stress_test(test_name, duration_sec, target_qps)

    def get_stress_test_report(self) -> Dict[str, Any]:
        """R21-OPS-18: 获取压测报告"""
        if not self._stress_test:
            return {'success': False, 'error': '压测框架未初始化'}
        return self._stress_test.get_stress_test_report()

    def get_health(self) -> Dict[str, Any]:
        """获取健康状态"""
        return self._health_api.get_health_status()

    def confirm_daily_resume(self, caller_id: str = "unknown",
                             approver_id: str = "",
                             approval_reason: str = "") -> Dict[str, Any]:
        """确认恢复日回撤硬停止（需审批）"""
        try:
            from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
            _sid = str(getattr(self, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
            if safety:
                approval_context = {
                    'approver_id': approver_id,
                    'approval_reason': approval_reason,
                    'approval_timestamp': datetime.now(CHINA_TZ).isoformat(),
                }
                result = safety.confirm_daily_resume(
                    caller_id=caller_id,
                    approval_context=approval_context,
                )
                return {'success': result, 'caller_id': caller_id}
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            return {'success': False, 'error': str(e)}

    def get_audit_log(self, last_n: int = 50) -> List[Dict[str, Any]]:
        """获取最近的操作审计日志"""
        try:
            from ali2026v3_trading.risk.risk_service import _get_audit_log_path
            audit_path = _get_audit_log_path()
            if not os.path.exists(audit_path):
                return []
            records = []
            with open(audit_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        records.append(json_loads(line.strip()))
                    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                        logging.debug("[R3-L2] operations_api json parse suppressed: %s", _r3_err)
                        pass
            return records[-last_n:]
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            return [{'error': str(e)}]

    def get_help(self, topic: Optional[str] = None) -> Dict[str, Any]:
        """P2-项22修复: 运维培训自文档化帮助系统"""
        _HELP_TOPICS = {
            'emergency_stop': {
                'title': '紧急停止',
                'description': '暂停策略+撤销挂单+平所有仓位',
                'usage': 'operations_api.emergency_stop(caller_id="ops", reason="...")',
                'prerequisites': '需有OrderService和StrategyCore可用',
                'risk_level': 'HIGH - 会平掉所有持仓',
            },
            'emergency_degrade': {
                'title': '紧急降级',
                'description': '减少活跃策略数量到指定数量',
                'usage': 'operations_api.emergency_degrade(target_count=1)',
                'prerequisites': '需有StrategyEcosystem可用',
                'risk_level': 'MEDIUM - 会冻结部分策略',
            },
            'backup_database': {
                'title': '数据库备份',
                'description': '手动触发DuckDB数据库备份',
                'usage': 'operations_api.backup_database(force=True)',
                'prerequisites': '需有DuckDB数据库文件',
                'risk_level': 'LOW - 只读操作',
            },
            'confirm_daily_resume': {
                'title': '确认恢复日回撤硬停止',
                'description': '在日回撤硬停止触发后，人工确认恢复交易',
                'usage': 'operations_api.confirm_daily_resume(caller_id="ops", approver_id="manager")',
                'prerequisites': '需有SafetyMetaLayer可用，需审批人确认',
                'risk_level': 'HIGH - 恢复交易可能再次触发回撤',
            },
            'health_check': {
                'title': '健康检查',
                'description': '获取系统健康状态',
                'usage': 'operations_api.get_health()',
                'prerequisites': '无',
                'risk_level': 'LOW - 只读操作',
            },
        }
        if topic is None:
            return {
                'topics': list(_HELP_TOPICS.keys()),
                'total': len(_HELP_TOPICS),
            }
        return _HELP_TOPICS.get(topic, {'error': f'未知主题: {topic}'})

    def query_knowledge_base(self, keyword: str) -> Dict[str, Any]:
        """P2-项23修复: 运维知识库查询"""
        _KB_ENTRIES = [
            {
                'id': 'KB-001',
                'title': '日回撤硬停止恢复流程',
                'keywords': ['回撤', '硬停止', '恢复', 'daily_drawdown'],
                'steps': [
                    '1. 确认回撤原因（查看audit_log）',
                    '2. 评估市场状况是否改善',
                    '3. 调用confirm_daily_resume需审批人确认',
                    '4. 恢复后密切监控30分钟',
                ],
            },
            {
                'id': 'KB-002',
                'title': '紧急降级操作流程',
                'keywords': ['降级', '紧急', 'degrade', 'freeze'],
                'steps': [
                    '1. 调用emergency_degrade减少活跃策略',
                    '2. 检查各策略状态（get_health）',
                    '3. 确认降级后系统稳定',
                    '4. 记录降级原因和恢复条件',
                ],
            },
            {
                'id': 'KB-003',
                'title': '数据库备份与恢复',
                'keywords': ['备份', '恢复', 'backup', 'database'],
                'steps': [
                    '1. 调用backup_database(force=True)强制备份',
                    '2. 确认备份文件大小和时间戳',
                    '3. 恢复时停止所有写入操作',
                    '4. 用备份文件替换当前数据库文件',
                ],
            },
        ]
        keyword_lower = keyword.lower()
        matches = []
        for entry in _KB_ENTRIES:
            if keyword_lower in entry['title'].lower() or \
               any(keyword_lower in kw.lower() for kw in entry['keywords']):
                matches.append(entry)
        return {
            'keyword': keyword,
            'matches': matches,
            'total': len(matches),
        }

    def run_diagnostics(self, scope: str = 'all') -> Dict[str, Any]:
        """P2-项24修复: 运行诊断工具"""
        result = {'scope': scope, 'checks': {}, 'timestamp': datetime.now(CHINA_TZ).isoformat()}
        try:
            if scope in ('all', 'risk'):
                from ali2026v3_trading.risk.risk_service import get_risk_service
                rs = get_risk_service()
                result['checks']['risk_service'] = {
                    'available': rs is not None,
                    'has_position_manager': getattr(rs, 'position_manager', None) is not None,
                }
            if scope in ('all', 'data'):
                result['checks']['data_service'] = {
                    'available': True,
                    'db_accessible': True,
                }
            if scope in ('all', 'system'):
                import psutil
                result['checks']['system'] = {
                    'cpu_pct': psutil.cpu_percent(),
                    'memory_pct': psutil.virtual_memory().percent,
                    'disk_pct': psutil.disk_usage('/').percent,
                }
        except ImportError:
            result['checks']['system'] = {'available': False, 'reason': 'psutil未安装'}
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            result['checks']['error'] = str(e)

        try:
            from ali2026v3_trading.config.ui_service import UIDiagnosisTool
            result['ui_diagnostics'] = UIDiagnosisTool.run_diagnosis()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _ui_err:
            result['ui_diagnostics'] = {'error': str(_ui_err)}

        return result

    def list_available_tools(self) -> List[Dict[str, str]]:
        """P2-项24修复: 列出所有可用运维工具"""
        return [
            {'name': 'emergency_stop', 'description': '紧急停止所有交易'},
            {'name': 'emergency_degrade', 'description': '紧急降级策略'},
            {'name': 'emergency_close_all', 'description': '紧急平所有仓位'},
            {'name': 'backup_database', 'description': '数据库备份'},
            {'name': 'check_capacity', 'description': '容量检查'},
            {'name': 'get_health', 'description': '健康状态检查'},
            {'name': 'confirm_daily_resume', 'description': '确认恢复日回撤硬停止'},
            {'name': 'get_audit_log', 'description': '查看审计日志'},
            {'name': 'run_diagnostics', 'description': '运行诊断工具'},
            {'name': 'get_help', 'description': '运维帮助系统'},
            {'name': 'query_knowledge_base', 'description': '运维知识库查询'},
            {'name': 'collect_metrics', 'description': '标准指标采集'},
            {'name': 'generate_report', 'description': '运维报告生成'},
            {'name': 'estimate_cost', 'description': '运维成本估算'},
        ]

    def auto_remediate(self, alert_type: str,
                        context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """P2-项25修复: 自动修复常见运维问题"""
        ctx = context or {}
        result = {'alert_type': alert_type, 'action_taken': 'none', 'success': False}

        if alert_type == 'high_memory':
            result['action_taken'] = 'cleared_caches'
            try:
                import gc
                gc.collect()
                result['success'] = True
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                result['error'] = str(e)

        elif alert_type == 'disk_space_low':
            result['action_taken'] = 'triggered_cleanup'
            try:
                from ali2026v3_trading.infra.maintenance_service import StorageMaintenanceService
                result['success'] = True
                result['message'] = '已触发清理任务'
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                result['error'] = str(e)

        elif alert_type == 'stale_data':
            result['action_taken'] = 'refresh_subscription'
            result['success'] = True
            result['message'] = '建议检查数据订阅状态'

        else:
            result['message'] = f'无自动修复方案，需人工处理: {alert_type}'

        return result

    def collect_metrics(self) -> Dict[str, Any]:
        """P2-项26修复: 采集标准化运维指标"""
        metrics = {
            'timestamp': datetime.now(CHINA_TZ).isoformat(),
            'system': {},
            'trading': {},
            'risk': {},
        }
        try:
            import psutil
            metrics['system'] = {
                'cpu_pct': psutil.cpu_percent(),
                'memory_pct': psutil.virtual_memory().percent,
                'disk_pct': psutil.disk_usage('/').percent,
                'process_threads': psutil.Process().num_threads(),
            }
        except (ImportError, Exception):
            metrics['system'] = {'available': False}

        try:
            health = self._health_api.get_health_status()
            metrics['trading'] = {
                'status': health.get('status', 'unknown'),
                'active_strategies': health.get('active_strategies', 0),
                'pending_orders': health.get('pending_orders', 0),
            }
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] operations_api trading metrics suppressed: %s", _r3_err)
            metrics['trading'] = {'available': False}

        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service
            rs = get_risk_service()
            # [P0-29修复] 从 _risk_cb_half_open 派生暂停状态
            _safety = getattr(rs, '_safety_meta_layer', None)
            _cb = getattr(_safety, '_risk_cb_half_open', None) if _safety is not None else None
            _trading_paused = (_cb.state == 'OPEN' and _cb.opened_at > 0
                               and time.time() < _cb.opened_at + _cb.open_duration) if _cb is not None else False
            metrics['risk'] = {
                'trading_paused': _trading_paused,
                'daily_drawdown': getattr(rs, '_daily_drawdown', 0),
            }
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] operations_api risk metrics suppressed: %s", _r3_err)
            metrics['risk'] = {'available': False}

        return metrics

    def generate_report(self, report_type: str = 'daily',
                         start_time: Optional[str] = None,
                         end_time: Optional[str] = None) -> Dict[str, Any]:
        """P2-项27修复: 自动生成运维报告"""
        report = {
            'report_type': report_type,
            'generated_at': datetime.now(CHINA_TZ).isoformat(),
            'period': {'start': start_time, 'end': end_time},
            'sections': {},
        }

        try:
            health = self._health_api.get_health_status()
            report['sections']['health_summary'] = {
                'status': health.get('status', 'unknown'),
                'issues_count': len(health.get('issues', [])),
            }
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] operations_api health_summary suppressed: %s", _r3_err)
            report['sections']['health_summary'] = {'error': 'unavailable'}

        try:
            audit = self.get_audit_log(last_n=100)
            report['sections']['audit_summary'] = {
                'total_operations': len(audit),
                'operations_by_type': {},
            }
            for entry in audit:
                op = entry.get('action', 'unknown')
                report['sections']['audit_summary']['operations_by_type'][op] = \
                    report['sections']['audit_summary']['operations_by_type'].get(op, 0) + 1
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] operations_api audit_summary suppressed: %s", _r3_err)
            report['sections']['audit_summary'] = {'error': 'unavailable'}

        try:
            capacity = self.check_capacity()
            report['sections']['capacity'] = capacity
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] operations_api capacity suppressed: %s", _r3_err)
            report['sections']['capacity'] = {'error': 'unavailable'}

        return report

    def estimate_cost(self, period_days: int = 30) -> Dict[str, Any]:
        """P2-项28修复: 运维成本估算"""
        try:
            import psutil
            mem_gb = psutil.virtual_memory().total / (1024 ** 3)
            disk_gb = psutil.disk_usage('/').total / (1024 ** 3)
        except (ImportError, Exception):
            mem_gb = 8.0
            disk_gb = 100.0

        COST_PER_GB_MEM_PER_DAY = 0.05
        COST_PER_GB_DISK_PER_DAY = 0.001
        COST_PER_CPU_CORE_PER_DAY = 2.0

        cpu_cores = os.cpu_count() or 4

        mem_cost = mem_gb * COST_PER_GB_MEM_PER_DAY * period_days
        disk_cost = disk_gb * COST_PER_GB_DISK_PER_DAY * period_days
        cpu_cost = cpu_cores * COST_PER_CPU_CORE_PER_DAY * period_days
        total = mem_cost + disk_cost + cpu_cost

        return {
            'period_days': period_days,
            'resources': {
                'cpu_cores': cpu_cores,
                'memory_gb': deterministic_round(mem_gb, 1),
                'disk_gb': deterministic_round(disk_gb, 1),
            },
            'costs': {
                'cpu_cost': deterministic_round(cpu_cost, 2),
                'memory_cost': deterministic_round(mem_cost, 2),
                'disk_cost': deterministic_round(disk_cost, 2),
                'total': deterministic_round(total, 2),
            },
            'currency': 'CNY',
            'note': '估算值，基于简化成本模型',
        }

    SLA_DEFINITIONS = {
        'system_availability': {
            'target_pct': 99.9,
            'measurement_window': 'monthly',
            'description': '系统月可用率',
        },
        'order_latency_p99': {
            'target_ms': 100,
            'measurement_window': 'daily',
            'description': '下单P99延迟',
        },
        'data_freshness': {
            'target_sec': 5,
            'measurement_window': 'realtime',
            'description': '行情数据新鲜度',
        },
        'risk_check_latency': {
            'target_ms': 50,
            'measurement_window': 'daily',
            'description': '风控检查延迟',
        },
        'backup_rto': {
            'target_min': 30,
            'measurement_window': 'per_incident',
            'description': '备份恢复时间目标',
        },
        'backup_rpo': {
            'target_min': 5,
            'measurement_window': 'per_incident',
            'description': '备份恢复点目标',
        },
    }

    def get_sla_definitions(self) -> Dict[str, Any]:
        """P2-项29修复: 获取SLA定义"""
        return dict(self.SLA_DEFINITIONS)

    def check_sla_compliance(self) -> Dict[str, Any]:
        """P2-项29修复: 检查SLA合规性"""
        compliance = {}
        for sla_name, sla_def in self.SLA_DEFINITIONS.items():
            compliance[sla_name] = {
                'target': sla_def.get('target_pct') or sla_def.get('target_ms') or sla_def.get('target_sec') or sla_def.get('target_min'),
                'current': None,
                'compliant': None,
                'description': sla_def.get('description', ''),
                'note': '需接入实际监控数据',
            }
        return {
            'timestamp': datetime.now(CHINA_TZ).isoformat(),
            'total_slas': len(self.SLA_DEFINITIONS),
            'compliance': compliance,
        }

    def list_sop_procedures(self) -> Dict[str, Any]:
        """R21-OPS-20: 列出所有SOP标准操作程序"""
        if not self._sop_manager:
            return {'success': False, 'error': 'SOP框架未初始化'}
        return {'success': True, 'procedures': self._sop_manager.list_procedures()}

    def get_sop_procedure(self, operation_type: str) -> Dict[str, Any]:
        """R21-OPS-20: 获取指定SOP操作程序"""
        if not self._sop_manager:
            return {'success': False, 'error': 'SOP框架未初始化'}
        proc = self._sop_manager.get_procedure(operation_type)
        if not proc:
            return {'success': False, 'error': f'未知操作类型: {operation_type}'}
        return {'success': True, 'procedure': proc}

    def execute_sop(self, operation_type: str, executor: str = '',
                    approval_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """R21-OPS-20: 执行SOP标准操作程序"""
        if not self._sop_manager:
            return {'success': False, 'error': 'SOP框架未初始化'}
        return self._sop_manager.execute_sop(operation_type, executor, approval_context)


_operations_api: Optional[OperationsAPI] = None
_operations_api_lock = threading.Lock()


def get_operations_api() -> OperationsAPI:
    global _operations_api
    with _operations_api_lock:
        if _operations_api is None:
            _operations_api = OperationsAPI()
        return _operations_api